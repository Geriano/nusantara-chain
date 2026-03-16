use std::collections::HashSet;
use std::sync::Arc;

use nusantara_consensus::bank::ConsensusBank;
use nusantara_consensus::replay_stage::{ReplayResult, ReplayStage};
use nusantara_core::block::Block;
use nusantara_core::{EpochSchedule, FeeCalculator};
use nusantara_crypto::{Hash, MerkleTree, hashv};
use nusantara_rent_program::Rent;
use nusantara_runtime::{ProgramCache, SysvarCache, execute_slot_parallel};
use nusantara_storage::Storage;
use nusantara_sysvar_program::{RecentBlockhashes, SlotHashes};
use tracing::instrument;

use crate::error::ValidatorError;

/// Full block replay with state verification.
///
/// Re-executes all transactions and verifies:
/// 1. `account_delta_hash` → `bank_hash` matches header
/// 2. `merkle_root` matches recomputed MerkleTree
/// 3. `block_hash` matches `hashv(parent_hash, slot, poh_hash)`
///
/// If all pass, feeds the block through `ReplayStage` for fork tree/Tower processing.
#[allow(clippy::too_many_arguments)]
#[instrument(skip_all, fields(slot = block.header.slot))]
pub fn replay_block_full(
    block: &Block,
    storage: &Arc<Storage>,
    bank: &Arc<ConsensusBank>,
    replay_stage: &mut ReplayStage,
    fee_calculator: &FeeCalculator,
    rent: &Rent,
    epoch_schedule: &EpochSchedule,
    program_cache: &ProgramCache,
) -> Result<ReplayResult, ValidatorError> {
    let slot = block.header.slot;
    let parent_slot = block.header.parent_slot;

    // 1. Verify parent exists in fork tree BEFORE modifying any state.
    //
    // This must be checked first because the subsequent steps (rollback,
    // set_slot_hashes, execute_slot) mutate bank and storage state. If the
    // parent is missing, the caller buffers this block as an orphan — but
    // state mutations would already have corrupted the bank's slot_hashes
    // and written account deltas to storage.
    let parent_bank_hash = replay_stage
        .fork_tree()
        .get_node(parent_slot)
        .map(|n| n.bank_hash)
        .ok_or(ValidatorError::MissingParentBlock {
            slot,
            parent_slot,
        })?;

    // 2. Rollback bank state to parent_slot.
    bank.rollback_to_slot(parent_slot, storage)?;

    // 3. Rebuild slot_hashes from the fork tree's ancestry chain.
    //
    // After rollback, the bank's slot_hashes reflects THIS validator's own
    // chain history, not the block producer's. When validators ran on separate
    // forks, these diverge: e.g. V1 has slot_hashes for [7,4,1,0] while V2's
    // block was produced with [8,5,2,0]. Transactions in V2's block reference
    // blockhashes from V2's chain — if V1 uses its own slot_hashes, those
    // transactions fail with BlockhashNotFound, producing a different
    // account_delta_hash and causing bank_hash mismatch.
    //
    // The fork tree tracks every successfully replayed slot with its block_hash,
    // so walking the ancestry from parent_slot to root gives us exactly the
    // slot_hashes the block producer had when it produced this block.
    let ancestry = replay_stage.fork_tree().get_ancestry(parent_slot);
    let fork_slot_hashes: Vec<(u64, Hash)> = ancestry
        .iter()
        .filter_map(|&s| {
            replay_stage
                .fork_tree()
                .get_node(s)
                .map(|n| (s, n.block_hash))
        })
        .collect();
    bank.set_slot_hashes(SlotHashes(fork_slot_hashes));

    // 4. Advance bank to current slot (updates Clock sysvar)
    bank.advance_slot(slot, block.header.timestamp);

    // 5. Build SysvarCache from reconstructed bank state
    let clock = bank.clock();
    let slot_hashes = bank.slot_hashes();
    let stake_history = bank.stake_history();
    let recent_blockhashes = RecentBlockhashes::new(
        slot_hashes.0.iter().take(300).map(|(_, h)| *h).collect(),
    );
    let sysvars = SysvarCache::new(
        clock,
        rent.clone(),
        epoch_schedule.clone(),
        slot_hashes,
        stake_history,
        recent_blockhashes,
    );

    // 6. Fork-aware rewind of account index before execution.
    //
    // execute_slot() loads accounts via get_account() which reads the account
    // index. If this validator replayed blocks on a different fork, the index
    // may point to account versions from those foreign slots. A simple
    // slot-number rewind (rewind_account_index_to_slot) is insufficient
    // because foreign-fork slots with numbers ≤ parent_slot would NOT be
    // rewound, causing the execution to load wrong account data.
    //
    // Instead, we build the exact set of ancestor slots from the fork tree
    // and ensure every account index entry points to a version from this set.
    let ancestor_set: HashSet<u64> = ancestry.iter().copied().collect();
    let rewound = storage.rewind_account_index_for_ancestry(&ancestor_set)?;
    if rewound > 0 {
        tracing::info!(parent_slot, rewound, "rewound account index (fork-aware) before replay");
    }

    // 7. Execute slot via runtime (parallel, same as produce_block path)
    let exec_result = execute_slot_parallel(
        slot,
        &block.transactions,
        storage,
        &sysvars,
        fee_calculator,
        program_cache,
    )?;

    // 8. Update state tree with account deltas (matches produce_block path)
    bank.update_state_tree(&exec_result.account_deltas);

    // Collect addresses modified by execution (for cleanup on failure).
    // execute_slot_parallel already wrote these to CF_ACCOUNTS during execution.
    // If verification fails, we must delete them to avoid contaminating future
    // replays — otherwise the second attempt loads the wrong account state.
    let modified_addresses: Vec<Hash> = exec_result
        .account_deltas
        .iter()
        .map(|(addr, _)| *addr)
        .collect();

    // 9. Verify bank_hash (parent_bank_hash was obtained in step 1)
    let expected_bank_hash = ConsensusBank::compute_bank_hash(
        &parent_bank_hash,
        &exec_result.account_delta_hash,
    );
    if expected_bank_hash != block.header.bank_hash {
        tracing::warn!(
            slot,
            parent_slot,
            tx_count = block.transactions.len(),
            expected = %expected_bank_hash.to_base64(),
            got = %block.header.bank_hash.to_base64(),
            parent_bank_hash = %parent_bank_hash.to_base64(),
            account_delta_hash = %exec_result.account_delta_hash.to_base64(),
            "bank_hash mismatch diagnostic"
        );
        cleanup_failed_replay(storage, slot, &modified_addresses, &ancestor_set);
        return Err(ValidatorError::BankHashMismatch { slot });
    }

    // 10. Verify merkle_root
    let expected_merkle = if block.transactions.is_empty() {
        Hash::zero()
    } else {
        let tx_hashes: Vec<Hash> = block.transactions.iter().map(|tx| tx.hash()).collect();
        MerkleTree::new(&tx_hashes).root()
    };
    if expected_merkle != block.header.merkle_root {
        cleanup_failed_replay(storage, slot, &modified_addresses, &ancestor_set);
        return Err(ValidatorError::MerkleRootMismatch { slot });
    }

    // 11. Verify block_hash = hashv(parent_hash, slot_le_bytes, poh_hash)
    let expected_block_hash = hashv(&[
        block.header.parent_hash.as_bytes(),
        &slot.to_le_bytes(),
        block.header.poh_hash.as_bytes(),
    ]);
    if expected_block_hash != block.header.block_hash {
        cleanup_failed_replay(storage, slot, &modified_addresses, &ancestor_set);
        return Err(ValidatorError::BlockHashMismatch { slot });
    }

    // 12. Record slot hash in bank
    bank.record_slot_hash(slot, block.header.block_hash);

    // 13. Feed through ReplayStage for fork tree / Tower / commitment processing
    let result = replay_stage.replay_block(block, &[])?;

    Ok(result)
}

/// Clean up storage pollution from a failed `replay_block_full`.
///
/// `execute_slot_parallel` writes account deltas to `CF_ACCOUNTS` and updates
/// `CF_ACCOUNT_INDEX` DURING execution (before verification). If verification
/// fails, these writes must be undone — otherwise the contaminated data is
/// loaded on the next replay attempt, producing a different
/// `account_delta_hash` and causing cascading mismatches.
///
/// Uses fork-aware cleanup: the account index is restored to the latest
/// version from the parent's ancestry set, not just any version ≤ parent_slot.
fn cleanup_failed_replay(
    storage: &Arc<Storage>,
    slot: u64,
    modified_addresses: &[Hash],
    ancestor_set: &HashSet<u64>,
) {
    match storage.cleanup_failed_slot_for_ancestry(slot, modified_addresses, ancestor_set) {
        Ok(count) => {
            tracing::debug!(
                slot,
                cleaned = count,
                "cleaned up failed replay storage entries (fork-aware)"
            );
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                slot,
                "failed to clean up storage after replay failure"
            );
        }
    }
}
