use std::collections::HashMap;

use nusantara_core::{Account, FeeCalculator, Transaction};
use nusantara_crypto::{Hash, Hasher};
use nusantara_storage::{Storage, TransactionStatus, TransactionStatusMeta};
use nusantara_vm::ProgramCache;
use tracing::instrument;

use crate::error::RuntimeError;
use crate::sysvar_cache::SysvarCache;
use crate::transaction_executor::execute_transaction;

pub struct SlotExecutionResult {
    pub slot: u64,
    pub transactions_executed: u64,
    pub transactions_succeeded: u64,
    pub transactions_failed: u64,
    pub total_fees: u64,
    pub total_compute_consumed: u64,
    pub account_delta_hash: Hash,
    /// Aggregated account deltas from all transactions in the slot.
    /// For accounts modified by multiple transactions, the final state is kept.
    /// Sorted by address for deterministic state tree updates.
    pub account_deltas: Vec<(Hash, Account)>,
}

#[instrument(skip_all, fields(slot = slot, tx_count = transactions.len()))]
pub fn execute_slot(
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

    for (tx_index, tx) in transactions.iter().enumerate() {
        let result = execute_transaction(tx, storage, sysvars, fee_calculator, slot, program_cache);

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
                TransactionStatus::Failed(e.to_string())
            }
        };

        // Commit account deltas to storage
        for (address, account) in &result.account_deltas {
            storage.put_account(address, slot, account)?;

            // Feed into delta hash
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
            storage.put_address_signature(address, slot, tx_index as u32, &result.tx_hash)?;
        }
    }

    let account_delta_hash = delta_hasher.finalize();

    // Sort deltas by address for deterministic state tree updates
    let mut account_deltas: Vec<(Hash, Account)> = merged_deltas.into_iter().collect();
    account_deltas.sort_by_key(|(addr, _)| *addr);

    metrics::counter!("runtime_slot_transactions_total").increment(transactions_executed);
    metrics::counter!("runtime_slot_fees_collected_total").increment(total_fees);
    metrics::counter!("runtime_slot_compute_consumed").increment(total_compute_consumed);

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
    use nusantara_crypto::{Keypair, hash};
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

    #[test]
    fn empty_slot() {
        let (storage, _dir) = test_storage();
        let sysvars = test_sysvars();
        let fee_calc = FeeCalculator::default();

        let cache = ProgramCache::new(16);
        let result = execute_slot(1, &[], &storage, &sysvars, &fee_calc, &cache).unwrap();
        assert_eq!(result.slot, 1);
        assert_eq!(result.transactions_executed, 0);
        assert_eq!(result.transactions_succeeded, 0);
        assert_eq!(result.transactions_failed, 0);
        assert_eq!(result.total_fees, 0);
    }

    #[test]
    fn single_tx() {
        let (storage, _dir) = test_storage();
        let alice_kp = Keypair::generate();
        let alice = alice_kp.address();
        let bob = hash(b"bob");

        storage
            .put_account(&alice, 0, &Account::new(1_000_000, *SYSTEM_PROGRAM_ID))
            .unwrap();

        let tx = transfer_tx(&alice_kp, bob, 100_000);
        let sysvars = test_sysvars();
        let fee_calc = FeeCalculator::default();

        let cache = ProgramCache::new(16);
        let result = execute_slot(1, &[tx], &storage, &sysvars, &fee_calc, &cache).unwrap();
        assert_eq!(result.transactions_executed, 1);
        assert_eq!(result.transactions_succeeded, 1);
        assert_eq!(result.total_fees, 5000);
    }

    #[test]
    fn multiple_tx() {
        let (storage, _dir) = test_storage();
        let alice_kp = Keypair::generate();
        let alice = alice_kp.address();
        let bob = hash(b"bob");
        let carol = hash(b"carol");

        storage
            .put_account(&alice, 0, &Account::new(2_000_000, *SYSTEM_PROGRAM_ID))
            .unwrap();

        let tx1 = transfer_tx(&alice_kp, bob, 100_000);
        let tx2 = transfer_tx(&alice_kp, carol, 50_000);
        let sysvars = test_sysvars();
        let fee_calc = FeeCalculator::default();

        let cache = ProgramCache::new(16);
        let result = execute_slot(1, &[tx1, tx2], &storage, &sysvars, &fee_calc, &cache).unwrap();
        assert_eq!(result.transactions_executed, 2);
        assert_eq!(result.transactions_succeeded, 2);
        assert_eq!(result.total_fees, 10000);
    }

    #[test]
    fn mixed_success_failure() {
        let (storage, _dir) = test_storage();
        let alice_kp = Keypair::generate();
        let alice = alice_kp.address();
        let bob = hash(b"bob");
        let poor_kp = Keypair::generate();
        let poor = poor_kp.address();

        storage
            .put_account(&alice, 0, &Account::new(1_000_000, *SYSTEM_PROGRAM_ID))
            .unwrap();
        storage
            .put_account(&poor, 0, &Account::new(10_000, *SYSTEM_PROGRAM_ID))
            .unwrap();

        let tx1 = transfer_tx(&alice_kp, bob, 100_000); // should succeed
        let tx2 = transfer_tx(&poor_kp, bob, 1_000_000); // should fail (insufficient)
        let sysvars = test_sysvars();
        let fee_calc = FeeCalculator::default();

        let cache = ProgramCache::new(16);
        let result = execute_slot(1, &[tx1, tx2], &storage, &sysvars, &fee_calc, &cache).unwrap();
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
        let result = execute_slot(1, &[tx], &storage, &sysvars, &fee_calc, &cache).unwrap();
        assert_eq!(result.total_fees, 10_000);
    }

    #[test]
    fn delta_hash_deterministic() {
        let (storage1, _dir1) = test_storage();
        let (storage2, _dir2) = test_storage();
        let alice_kp = Keypair::generate();
        let alice = alice_kp.address();
        let bob = hash(b"bob");

        for storage in [&storage1, &storage2] {
            storage
                .put_account(&alice, 0, &Account::new(1_000_000, *SYSTEM_PROGRAM_ID))
                .unwrap();
        }

        let tx = transfer_tx(&alice_kp, bob, 100_000);
        let sysvars = test_sysvars();
        let fee_calc = FeeCalculator::default();

        let cache = ProgramCache::new(16);
        let result1 = execute_slot(
            1,
            std::slice::from_ref(&tx),
            &storage1,
            &sysvars,
            &fee_calc,
            &cache,
        )
        .unwrap();
        let result2 = execute_slot(
            1,
            std::slice::from_ref(&tx),
            &storage2,
            &sysvars,
            &fee_calc,
            &cache,
        )
        .unwrap();

        assert_eq!(result1.account_delta_hash, result2.account_delta_hash);
    }
}
