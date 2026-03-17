use std::time::{SystemTime, UNIX_EPOCH};

use nusantara_consensus::bank::ConsensusBank;
use nusantara_core::EpochSchedule;
use nusantara_crypto::{Hash, MerkleTree};
use nusantara_core::Transaction;
use nusantara_rent_program::Rent;
use nusantara_runtime::SysvarCache;
use nusantara_sysvar_program::RecentBlockhashes;

use crate::constants::RECENT_BLOCKHASHES_COUNT;

/// Current Unix timestamp in seconds (i64).
pub(crate) fn unix_timestamp_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(std::time::Duration::ZERO)
        .as_secs() as i64
}

/// Current Unix timestamp in milliseconds (u64).
#[allow(dead_code)]
pub(crate) fn unix_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(std::time::Duration::ZERO)
        .as_millis() as u64
}

/// Build a SysvarCache from the current bank state.
pub(crate) fn build_sysvar_cache(
    bank: &ConsensusBank,
    rent: &Rent,
    epoch_schedule: &EpochSchedule,
) -> SysvarCache {
    let clock = bank.clock();
    let slot_hashes = bank.slot_hashes();
    let stake_history = bank.stake_history();
    let recent_blockhashes = RecentBlockhashes::new(
        slot_hashes
            .0
            .iter()
            .take(RECENT_BLOCKHASHES_COUNT)
            .map(|(_, h)| *h)
            .collect(),
    );
    SysvarCache::new(
        clock,
        rent.clone(),
        epoch_schedule.clone(),
        slot_hashes,
        stake_history,
        recent_blockhashes,
    )
}

/// Compute the Merkle root of a list of transactions.
pub(crate) fn compute_merkle_root(transactions: &[Transaction]) -> Hash {
    if transactions.is_empty() {
        Hash::zero()
    } else {
        let tx_hashes: Vec<Hash> = transactions.iter().map(|tx| tx.hash()).collect();
        MerkleTree::new(&tx_hashes).root()
    }
}
