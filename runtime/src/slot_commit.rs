//! Shared slot-level commit logic used by both sequential and parallel executors.
//!
//! The [`SlotCommitter`] encapsulates the per-transaction delta commit, hash
//! accumulation, status recording, and final result assembly that was previously
//! duplicated between `batch_executor` and `parallel_executor`.

use std::collections::HashMap;

use nusantara_core::Account;
use nusantara_crypto::{Hash, Hasher};
use nusantara_storage::{Storage, TransactionStatus, TransactionStatusMeta};

use crate::batch_executor::SlotExecutionResult;
use crate::error::RuntimeError;
use crate::transaction_executor::TransactionResult;

/// Accumulates transaction results within a slot and commits them to storage.
///
/// # Determinism invariant
///
/// `commit_result()` must be called in **original transaction index order** so
/// that the delta hasher produces a deterministic hash identical across
/// sequential and parallel execution paths.
pub(crate) struct SlotCommitter {
    transactions_executed: u64,
    transactions_succeeded: u64,
    transactions_failed: u64,
    total_fees: u64,
    total_compute_consumed: u64,
    delta_hasher: Hasher,
    merged_deltas: HashMap<Hash, Account>,
}

impl SlotCommitter {
    pub fn new() -> Self {
        Self {
            transactions_executed: 0,
            transactions_succeeded: 0,
            transactions_failed: 0,
            total_fees: 0,
            total_compute_consumed: 0,
            delta_hasher: Hasher::default(),
            merged_deltas: HashMap::new(),
        }
    }

    /// Commit a single transaction result to storage and accumulate its deltas.
    ///
    /// Must be called in original transaction index order for deterministic hashing.
    pub fn commit_result(
        &mut self,
        tx_idx: usize,
        result: TransactionResult,
        slot: u64,
        storage: &Storage,
    ) -> Result<(), RuntimeError> {
        self.transactions_executed += 1;
        self.total_fees += result.fee;
        self.total_compute_consumed += result.compute_units_consumed;

        let status = match &result.status {
            Ok(()) => {
                self.transactions_succeeded += 1;
                TransactionStatus::Success
            }
            Err(e) => {
                self.transactions_failed += 1;
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

            // Feed into delta hash
            let account_bytes = borsh::to_vec(account)
                .map_err(|e| RuntimeError::InvalidAccountData(e.to_string()))?;
            self.delta_hasher.update(address.as_bytes());
            self.delta_hasher.update(&account_bytes);

            // Track final state for each address (last write wins)
            self.merged_deltas.insert(*address, account.clone());
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

        Ok(())
    }

    /// Finalize the slot: compute delta hash, sort deltas, and return the result.
    pub fn finalize(self, slot: u64) -> SlotExecutionResult {
        let account_delta_hash = self.delta_hasher.finalize();

        // Sort deltas by address for deterministic state tree updates
        let mut account_deltas: Vec<(Hash, Account)> = self.merged_deltas.into_iter().collect();
        account_deltas.sort_by_key(|(addr, _)| *addr);

        SlotExecutionResult {
            slot,
            transactions_executed: self.transactions_executed,
            transactions_succeeded: self.transactions_succeeded,
            transactions_failed: self.transactions_failed,
            total_fees: self.total_fees,
            total_compute_consumed: self.total_compute_consumed,
            account_delta_hash,
            account_deltas,
        }
    }
}
