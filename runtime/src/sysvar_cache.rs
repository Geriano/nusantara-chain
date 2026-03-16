use nusantara_core::EpochSchedule;
use nusantara_rent_program::Rent;
use nusantara_sysvar_program::{Clock, RecentBlockhashes, SlotHashes, StakeHistory};

pub struct SysvarCache {
    clock: Clock,
    rent: Rent,
    epoch_schedule: EpochSchedule,
    slot_hashes: SlotHashes,
    stake_history: StakeHistory,
    recent_blockhashes: RecentBlockhashes,
}

impl SysvarCache {
    pub fn new(
        clock: Clock,
        rent: Rent,
        epoch_schedule: EpochSchedule,
        slot_hashes: SlotHashes,
        stake_history: StakeHistory,
        recent_blockhashes: RecentBlockhashes,
    ) -> Self {
        Self {
            clock,
            rent,
            epoch_schedule,
            slot_hashes,
            stake_history,
            recent_blockhashes,
        }
    }

    pub fn clock(&self) -> &Clock {
        &self.clock
    }

    pub fn rent(&self) -> &Rent {
        &self.rent
    }

    pub fn epoch_schedule(&self) -> &EpochSchedule {
        &self.epoch_schedule
    }

    pub fn slot_hashes(&self) -> &SlotHashes {
        &self.slot_hashes
    }

    pub fn stake_history(&self) -> &StakeHistory {
        &self.stake_history
    }

    pub fn recent_blockhashes(&self) -> &RecentBlockhashes {
        &self.recent_blockhashes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nusantara_crypto::hash;

    fn test_cache() -> SysvarCache {
        SysvarCache::new(
            Clock::default(),
            Rent::default(),
            EpochSchedule::default(),
            SlotHashes::default(),
            StakeHistory::default(),
            RecentBlockhashes::new(vec![hash(b"blockhash1")]),
        )
    }

    #[test]
    fn construction() {
        let cache = test_cache();
        assert_eq!(cache.clock().slot, 0);
        assert_eq!(cache.rent().lamports_per_byte_year, 3480);
    }

    #[test]
    fn rent_minimum() {
        let cache = test_cache();
        let min = cache.rent().minimum_balance(0);
        assert_eq!(min, 890_880);
    }

    #[test]
    fn recent_blockhashes_contains() {
        let cache = test_cache();
        let h = hash(b"blockhash1");
        assert!(cache.recent_blockhashes().contains(&h));
        assert!(!cache.recent_blockhashes().contains(&hash(b"other")));
    }
}
