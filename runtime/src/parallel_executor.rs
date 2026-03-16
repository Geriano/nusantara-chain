//! Parallel slot execution using Sealevel-style transaction scheduling.
//!
//! Transactions are grouped into non-conflicting batches by the
//! [`TransactionScheduler`], and each batch is executed in parallel using
//! [`rayon`]. The key invariant is that `execute_slot_parallel()` produces an
//! **identical** `account_delta_hash` as the sequential [`execute_slot()`] for
//! the same input, because deltas are committed and hashed in **original
//! transaction order** regardless of which thread executed them.
//!
//! # Concurrency model
//!
//! Within each batch, transactions touch disjoint account sets (guaranteed by
//! the scheduler). Each transaction independently loads its accounts from
//! storage, executes, and produces a `TransactionResult`. After the batch
//! completes, results are sorted by original transaction index and committed
//! to storage sequentially. This sequential commit phase ensures determinism.
//!
//! # Deadlock prevention
//!
//! No locks are held across the rayon parallel scope. Storage reads inside the
//! parallel section use point-get operations on RocksDB which are inherently
//! lock-free from the caller's perspective. The `ProgramCache` uses a
//! `parking_lot::Mutex` with very short critical sections (no `.await`).

use std::collections::HashMap;

use nusantara_core::{Account, FeeCalculator, Transaction};
use nusantara_crypto::{Hash, Hasher};
use nusantara_storage::{Storage, TransactionStatus, TransactionStatusMeta};
use nusantara_vm::ProgramCache;
use rayon::prelude::*;
use tracing::instrument;

use crate::batch_executor::SlotExecutionResult;
use crate::error::RuntimeError;
use crate::scheduler::TransactionScheduler;
use crate::sysvar_cache::SysvarCache;
use crate::transaction_executor::{TransactionResult, execute_transaction};

/// Execute a slot's transactions in parallel batches.
///
/// # Determinism guarantee
///
/// The `account_delta_hash` is computed by feeding account deltas in the
/// **original transaction order** (not execution order). Within each batch,
/// rayon may execute transactions in any order, but results are collected and
/// sorted by their original index before being committed and hashed.
///
/// This means the output is byte-identical to [`crate::batch_executor::execute_slot()`]
/// for the same input.
///
/// # Algorithm
///
/// 1. Schedule transactions into non-conflicting parallel batches.
/// 2. For each batch:
///    a. Execute all transactions in parallel via rayon.
///    b. Sort results by original transaction index.
///    c. Commit deltas to storage in original order.
///    d. Feed delta hasher in original order.
/// 3. Return aggregated results.
#[instrument(skip_all, fields(slot = slot, tx_count = transactions.len()))]
pub fn execute_slot_parallel(
    slot: u64,
    transactions: &[Transaction],
    storage: &Storage,
    sysvars: &SysvarCache,
    fee_calculator: &FeeCalculator,
    program_cache: &ProgramCache,
) -> Result<SlotExecutionResult, RuntimeError> {
    let mut transactions_executed = 0u64;
    let mut transactions_succeeded = 0u64;
    let mut transactions_failed = 0u64;
    let mut total_fees = 0u64;
    let mut total_compute_consumed = 0u64;
    let mut delta_hasher = Hasher::default();
    // Collect all account deltas; later writes for the same address override earlier ones.
    let mut merged_deltas: HashMap<Hash, Account> = HashMap::new();

    // Step 1: Schedule transactions into parallel batches
    let batches = TransactionScheduler::schedule(transactions);

    // Step 2: Execute each batch
    for batch in &batches {
        // Execute transactions within this batch in parallel.
        // Safety: the scheduler guarantees no two transactions in the same
        // batch touch the same writable account, so parallel execution with
        // independent snapshots is safe.
        let results: Vec<(usize, TransactionResult)> = batch
            .tx_indices
            .par_iter()
            .map(|&tx_idx| {
                let tx = &transactions[tx_idx];
                let result =
                    execute_transaction(tx, storage, sysvars, fee_calculator, slot, program_cache);
                (tx_idx, result)
            })
            .collect();

        // Sort by original transaction index for deterministic commit order
        let mut sorted_results = results;
        sorted_results.sort_by_key(|(idx, _)| *idx);

        // Commit results in original order (deterministic delta hash)
        for (tx_idx, result) in sorted_results {
            transactions_executed += 1;
            total_fees += result.fee;
            total_compute_consumed += result.compute_units_consumed;

            let status = match &result.status {
                Ok(()) => {
                    transactions_succeeded += 1;
                    TransactionStatus::Success
                }
                Err(e) => {
                    transactions_failed += 1;
                    tracing::warn!(
                        slot,
                        tx_idx,
                        error = %e,
                        fee = result.fee,
                        "transaction failed"
                    );
                    TransactionStatus::Failed(e.to_string())
                }
            };

            // Commit account deltas to storage
            for (address, account) in &result.account_deltas {
                storage.put_account(address, slot, account)?;

                // Feed into delta hash (identical to sequential path)
                let account_bytes = borsh::to_vec(account)
                    .map_err(|e| RuntimeError::InvalidAccountData(e.to_string()))?;
                delta_hasher.update(address.as_bytes());
                delta_hasher.update(&account_bytes);

                // Track final state for each address (last write wins)
                merged_deltas.insert(*address, account.clone());
            }

            // Record transaction status
            let meta = TransactionStatusMeta {
                slot,
                status,
                fee: result.fee,
                pre_balances: result.pre_balances,
                post_balances: result.post_balances,
                compute_units_consumed: result.compute_units_consumed,
            };
            storage.put_transaction_status(&result.tx_hash, &meta)?;

            // Record address signatures
            for (address, _) in &result.account_deltas {
                storage.put_address_signature(address, slot, tx_idx as u32, &result.tx_hash)?;
            }
        }
    }

    let account_delta_hash = delta_hasher.finalize();

    // Sort deltas by address for deterministic state tree updates
    let mut account_deltas: Vec<(Hash, Account)> = merged_deltas.into_iter().collect();
    account_deltas.sort_by_key(|(addr, _)| *addr);

    metrics::counter!("runtime_parallel_slot_transactions_total").increment(transactions_executed);
    metrics::counter!("runtime_parallel_slot_fees_collected_total").increment(total_fees);
    metrics::counter!("runtime_parallel_slot_compute_consumed").increment(total_compute_consumed);
    metrics::counter!("runtime_parallel_batches_total").increment(batches.len() as u64);

    Ok(SlotExecutionResult {
        slot,
        transactions_executed,
        transactions_succeeded,
        transactions_failed,
        total_fees,
        total_compute_consumed,
        account_delta_hash,
        account_deltas,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use nusantara_core::program::SYSTEM_PROGRAM_ID;
    use nusantara_core::{Account, EpochSchedule, Message};
    use nusantara_crypto::{Hash, Keypair, hash};
    use nusantara_rent_program::Rent;
    use nusantara_sysvar_program::{Clock, RecentBlockhashes, SlotHashes, StakeHistory};
    use tempfile::tempdir;

    fn test_sysvars() -> SysvarCache {
        SysvarCache::new(
            Clock::default(),
            Rent::default(),
            EpochSchedule::default(),
            SlotHashes::default(),
            StakeHistory::default(),
            RecentBlockhashes::default(),
        )
    }

    fn test_storage() -> (Storage, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let storage = Storage::open(dir.path()).unwrap();
        (storage, dir)
    }

    fn transfer_tx(from_kp: &Keypair, to: Hash, amount: u64) -> Transaction {
        let from = from_kp.address();
        let ix = nusantara_system_program::transfer(&from, &to, amount);
        let msg = Message::new(&[ix], &from).unwrap();
        let mut tx = Transaction::new(msg);
        tx.sign(&[from_kp]);
        tx
    }

    // ---------------------------------------------------------------
    // Determinism: parallel must produce identical delta hash as sequential
    // ---------------------------------------------------------------

    #[test]
    fn determinism_single_tx() {
        let alice_kp = Keypair::generate();
        let alice = alice_kp.address();
        let bob = hash(b"bob");

        let tx = transfer_tx(&alice_kp, bob, 100_000);
        let sysvars = test_sysvars();
        let fee_calc = FeeCalculator::default();
        let cache = ProgramCache::new(16);

        // Sequential
        let (storage_seq, _d1) = test_storage();
        storage_seq
            .put_account(&alice, 0, &Account::new(1_000_000, *SYSTEM_PROGRAM_ID))
            .unwrap();
        let seq_result = crate::batch_executor::execute_slot(
            1,
            std::slice::from_ref(&tx),
            &storage_seq,
            &sysvars,
            &fee_calc,
            &cache,
        )
        .unwrap();

        // Parallel
        let (storage_par, _d2) = test_storage();
        storage_par
            .put_account(&alice, 0, &Account::new(1_000_000, *SYSTEM_PROGRAM_ID))
            .unwrap();
        let par_result = execute_slot_parallel(
            1,
            std::slice::from_ref(&tx),
            &storage_par,
            &sysvars,
            &fee_calc,
            &cache,
        )
        .unwrap();

        assert_eq!(seq_result.account_delta_hash, par_result.account_delta_hash);
        assert_eq!(
            seq_result.transactions_executed,
            par_result.transactions_executed
        );
        assert_eq!(
            seq_result.transactions_succeeded,
            par_result.transactions_succeeded
        );
        assert_eq!(seq_result.total_fees, par_result.total_fees);
    }

    #[test]
    fn determinism_multiple_conflicting_txs() {
        // Same payer sends two transfers -> must be sequential batches
        let alice_kp = Keypair::generate();
        let alice = alice_kp.address();
        let bob = hash(b"bob");
        let carol = hash(b"carol");

        let tx1 = transfer_tx(&alice_kp, bob, 100_000);
        let tx2 = transfer_tx(&alice_kp, carol, 50_000);
        let txs = [tx1, tx2];
        let sysvars = test_sysvars();
        let fee_calc = FeeCalculator::default();
        let cache = ProgramCache::new(16);

        // Sequential
        let (storage_seq, _d1) = test_storage();
        storage_seq
            .put_account(&alice, 0, &Account::new(2_000_000, *SYSTEM_PROGRAM_ID))
            .unwrap();
        let seq_result =
            crate::batch_executor::execute_slot(1, &txs, &storage_seq, &sysvars, &fee_calc, &cache)
                .unwrap();

        // Parallel
        let (storage_par, _d2) = test_storage();
        storage_par
            .put_account(&alice, 0, &Account::new(2_000_000, *SYSTEM_PROGRAM_ID))
            .unwrap();
        let par_result =
            execute_slot_parallel(1, &txs, &storage_par, &sysvars, &fee_calc, &cache).unwrap();

        assert_eq!(seq_result.account_delta_hash, par_result.account_delta_hash);
        assert_eq!(
            seq_result.transactions_executed,
            par_result.transactions_executed
        );
        assert_eq!(
            seq_result.transactions_succeeded,
            par_result.transactions_succeeded
        );
        assert_eq!(
            seq_result.transactions_failed,
            par_result.transactions_failed
        );
        assert_eq!(seq_result.total_fees, par_result.total_fees);
    }

    #[test]
    fn determinism_independent_transfers() {
        // Independent payers -> can run in parallel batch
        let alice_kp = Keypair::generate();
        let carol_kp = Keypair::generate();
        let alice = alice_kp.address();
        let carol = carol_kp.address();
        let bob = hash(b"bob");
        let dave = hash(b"dave");

        let tx1 = transfer_tx(&alice_kp, bob, 100_000);
        let tx2 = transfer_tx(&carol_kp, dave, 200_000);
        let txs = [tx1, tx2];
        let sysvars = test_sysvars();
        let fee_calc = FeeCalculator::default();
        let cache = ProgramCache::new(16);

        // Sequential
        let (storage_seq, _d1) = test_storage();
        storage_seq
            .put_account(&alice, 0, &Account::new(1_000_000, *SYSTEM_PROGRAM_ID))
            .unwrap();
        storage_seq
            .put_account(&carol, 0, &Account::new(1_000_000, *SYSTEM_PROGRAM_ID))
            .unwrap();
        let seq_result =
            crate::batch_executor::execute_slot(1, &txs, &storage_seq, &sysvars, &fee_calc, &cache)
                .unwrap();

        // Parallel
        let (storage_par, _d2) = test_storage();
        storage_par
            .put_account(&alice, 0, &Account::new(1_000_000, *SYSTEM_PROGRAM_ID))
            .unwrap();
        storage_par
            .put_account(&carol, 0, &Account::new(1_000_000, *SYSTEM_PROGRAM_ID))
            .unwrap();
        let par_result =
            execute_slot_parallel(1, &txs, &storage_par, &sysvars, &fee_calc, &cache).unwrap();

        assert_eq!(seq_result.account_delta_hash, par_result.account_delta_hash);
        assert_eq!(
            seq_result.transactions_executed,
            par_result.transactions_executed
        );
        assert_eq!(
            seq_result.transactions_succeeded,
            par_result.transactions_succeeded
        );
        assert_eq!(seq_result.total_fees, par_result.total_fees);
    }

    // ---------------------------------------------------------------
    // Edge cases
    // ---------------------------------------------------------------

    #[test]
    fn empty_slot() {
        let (storage, _dir) = test_storage();
        let sysvars = test_sysvars();
        let fee_calc = FeeCalculator::default();
        let cache = ProgramCache::new(16);

        let result = execute_slot_parallel(1, &[], &storage, &sysvars, &fee_calc, &cache).unwrap();
        assert_eq!(result.slot, 1);
        assert_eq!(result.transactions_executed, 0);
        assert_eq!(result.transactions_succeeded, 0);
        assert_eq!(result.transactions_failed, 0);
        assert_eq!(result.total_fees, 0);
    }

    #[test]
    fn mixed_success_failure() {
        let (storage, _dir) = test_storage();
        let alice_kp = Keypair::generate();
        let alice = alice_kp.address();
        let poor_kp = Keypair::generate();
        let poor = poor_kp.address();
        let bob = hash(b"bob");

        storage
            .put_account(&alice, 0, &Account::new(1_000_000, *SYSTEM_PROGRAM_ID))
            .unwrap();
        storage
            .put_account(&poor, 0, &Account::new(10_000, *SYSTEM_PROGRAM_ID))
            .unwrap();

        let tx1 = transfer_tx(&alice_kp, bob, 100_000); // success
        let tx2 = transfer_tx(&poor_kp, bob, 1_000_000); // fail: insufficient

        // These conflict on bob (both write) so they will be in separate batches
        let txs = [tx1, tx2];
        let sysvars = test_sysvars();
        let fee_calc = FeeCalculator::default();
        let cache = ProgramCache::new(16);

        let result = execute_slot_parallel(1, &txs, &storage, &sysvars, &fee_calc, &cache).unwrap();
        assert_eq!(result.transactions_executed, 2);
        assert_eq!(result.transactions_succeeded, 1);
        assert_eq!(result.transactions_failed, 1);
    }

    #[test]
    fn fee_collection() {
        let (storage, _dir) = test_storage();
        let alice_kp = Keypair::generate();
        let alice = alice_kp.address();
        let bob = hash(b"bob");

        storage
            .put_account(&alice, 0, &Account::new(1_000_000, *SYSTEM_PROGRAM_ID))
            .unwrap();

        let tx = transfer_tx(&alice_kp, bob, 100);
        let sysvars = test_sysvars();
        let fee_calc = FeeCalculator::new(10_000);
        let cache = ProgramCache::new(16);

        let result = execute_slot_parallel(
            1,
            std::slice::from_ref(&tx),
            &storage,
            &sysvars,
            &fee_calc,
            &cache,
        )
        .unwrap();
        assert_eq!(result.total_fees, 10_000);
    }

    #[test]
    fn determinism_repeated_runs() {
        // Run the same parallel execution 5 times and verify identical hashes
        let alice_kp = Keypair::generate();
        let carol_kp = Keypair::generate();
        let alice = alice_kp.address();
        let carol = carol_kp.address();
        let bob = hash(b"bob");
        let dave = hash(b"dave");

        let tx1 = transfer_tx(&alice_kp, bob, 50_000);
        let tx2 = transfer_tx(&carol_kp, dave, 75_000);
        let txs = [tx1, tx2];
        let sysvars = test_sysvars();
        let fee_calc = FeeCalculator::default();
        let cache = ProgramCache::new(16);

        let mut hashes = Vec::new();
        for _ in 0..5 {
            let (storage, _dir) = test_storage();
            storage
                .put_account(&alice, 0, &Account::new(1_000_000, *SYSTEM_PROGRAM_ID))
                .unwrap();
            storage
                .put_account(&carol, 0, &Account::new(1_000_000, *SYSTEM_PROGRAM_ID))
                .unwrap();

            let result =
                execute_slot_parallel(1, &txs, &storage, &sysvars, &fee_calc, &cache).unwrap();
            hashes.push(result.account_delta_hash);
        }

        // All hashes must be identical
        for h in &hashes[1..] {
            assert_eq!(hashes[0], *h, "parallel execution must be deterministic");
        }
    }

    #[test]
    fn determinism_many_independent_txs() {
        // 10 independent transfers from different payers
        let keypairs: Vec<Keypair> = (0..10).map(|_| Keypair::generate()).collect();
        let targets: Vec<Hash> = (0..10)
            .map(|i| hash(format!("target_{i}").as_bytes()))
            .collect();

        let txs: Vec<Transaction> = keypairs
            .iter()
            .zip(targets.iter())
            .map(|(kp, target)| transfer_tx(kp, *target, 50_000))
            .collect();

        let sysvars = test_sysvars();
        let fee_calc = FeeCalculator::default();
        let cache = ProgramCache::new(16);

        // Sequential baseline
        let (storage_seq, _d1) = test_storage();
        for kp in &keypairs {
            storage_seq
                .put_account(
                    &kp.address(),
                    0,
                    &Account::new(1_000_000, *SYSTEM_PROGRAM_ID),
                )
                .unwrap();
        }
        let seq_result =
            crate::batch_executor::execute_slot(1, &txs, &storage_seq, &sysvars, &fee_calc, &cache)
                .unwrap();

        // Parallel
        let (storage_par, _d2) = test_storage();
        for kp in &keypairs {
            storage_par
                .put_account(
                    &kp.address(),
                    0,
                    &Account::new(1_000_000, *SYSTEM_PROGRAM_ID),
                )
                .unwrap();
        }
        let par_result =
            execute_slot_parallel(1, &txs, &storage_par, &sysvars, &fee_calc, &cache).unwrap();

        assert_eq!(seq_result.account_delta_hash, par_result.account_delta_hash);
        assert_eq!(
            seq_result.transactions_executed,
            par_result.transactions_executed
        );
        assert_eq!(seq_result.total_fees, par_result.total_fees);
    }
}
