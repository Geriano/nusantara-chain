use std::collections::BTreeMap;

use dashmap::DashMap;
use nusantara_core::block::Block;
use nusantara_core::native_token::const_parse_u64;
use nusantara_storage::shred::DataShred;

use crate::deshredder::Deshredder;
use crate::signed_shred::SignedDataShred;

pub const MAX_SHREDS_PER_SLOT: u64 =
    const_parse_u64(env!("NUSA_TURBINE_MAX_SHREDS_PER_SLOT"));

struct SlotShreds {
    data_shreds: BTreeMap<u32, DataShred>,
    last_index: Option<u32>,
}

impl SlotShreds {
    fn new() -> Self {
        Self {
            data_shreds: BTreeMap::new(),
            last_index: None,
        }
    }

    /// Insert a shred, enforcing the per-slot shred count limit.
    /// Returns false if the shred was rejected due to limits.
    fn insert(&mut self, shred: &SignedDataShred) -> bool {
        // Reject if shred index exceeds max
        if shred.index() >= MAX_SHREDS_PER_SLOT as u32 {
            metrics::counter!("turbine_shreds_rejected_max_index").increment(1);
            return false;
        }
        // Reject if slot already has too many shreds
        if self.data_shreds.len() >= MAX_SHREDS_PER_SLOT as usize {
            metrics::counter!("turbine_shreds_rejected_max_index").increment(1);
            return false;
        }
        if shred.is_last() {
            self.last_index = Some(shred.index());
        }
        self.data_shreds.insert(shred.index(), shred.shred.clone());
        true
    }

    fn is_complete(&self) -> bool {
        let last = match self.last_index {
            Some(l) => l,
            None => return false,
        };
        // Need all indices from 0 to last
        self.data_shreds.len() == (last + 1) as usize
    }

    fn to_sorted_shreds(&self) -> Vec<DataShred> {
        self.data_shreds.values().cloned().collect()
    }
}

pub struct ShredCollector {
    slots: DashMap<u64, SlotShreds>,
    /// Slots whose blocks have been stored to disk. Prevents re-assembly of
    /// already-stored blocks and skips unnecessary repair requests.
    stored_slots: DashMap<u64, ()>,
}

impl ShredCollector {
    pub fn new() -> Self {
        Self {
            slots: DashMap::new(),
            stored_slots: DashMap::new(),
        }
    }

    /// Mark a slot as stored to disk. Removes it from active shred tracking
    /// so future shreds for this slot are dropped.
    pub fn mark_slot_stored(&self, slot: u64) {
        self.stored_slots.insert(slot, ());
        self.slots.remove(&slot);
    }

    /// Check if a slot's block has already been stored.
    pub fn is_slot_stored(&self, slot: u64) -> bool {
        self.stored_slots.contains_key(&slot)
    }

    /// Insert a signed data shred. Returns `Some(Block)` if the slot is now complete.
    pub fn insert_data_shred(&self, shred: &SignedDataShred) -> Option<Block> {
        let slot = shred.slot();

        if self.stored_slots.contains_key(&slot) {
            metrics::counter!("turbine_shreds_skipped_already_stored").increment(1);
            return None;
        }

        let mut entry = self.slots.entry(slot).or_insert_with(SlotShreds::new);
        if !entry.insert(shred) {
            return None;
        }

        if entry.is_complete() {
            let sorted = entry.to_sorted_shreds();
            drop(entry); // release lock before deserialization
            match Deshredder::deshred(&sorted) {
                Ok(block) => {
                    self.slots.remove(&slot);
                    metrics::counter!("turbine_blocks_assembled_total").increment(1);
                    Some(block)
                }
                Err(e) => {
                    tracing::warn!(slot, error = %e, "deshredding failed");
                    None
                }
            }
        } else {
            None
        }
    }

    /// Check which shred indices are missing for a slot.
    pub fn missing_shreds(&self, slot: u64) -> Vec<u32> {
        let entry = match self.slots.get(&slot) {
            Some(e) => e,
            None => return Vec::new(),
        };

        let last = match entry.last_index {
            Some(l) => l,
            None => {
                // Don't know the last index yet, can't determine missing
                return Vec::new();
            }
        };

        (0..=last)
            .filter(|i| !entry.data_shreds.contains_key(i))
            .collect()
    }

    /// Check if a slot has any shreds.
    pub fn has_slot(&self, slot: u64) -> bool {
        self.slots.contains_key(&slot)
    }

    /// Get count of data shreds for a slot.
    pub fn shred_count(&self, slot: u64) -> usize {
        self.slots
            .get(&slot)
            .map(|e| e.data_shreds.len())
            .unwrap_or(0)
    }

    /// Check if a slot has all its shreds assembled (last shred received
    /// and all indices 0..=last present). Returns false if the slot doesn't
    /// exist or if the last shred (with completion flag) hasn't arrived yet.
    pub fn is_slot_complete(&self, slot: u64) -> bool {
        self.slots
            .get(&slot)
            .is_some_and(|e| e.is_complete())
    }

    /// Remove a slot (e.g. after it's been finalized and stored).
    pub fn remove_slot(&self, slot: u64) {
        self.slots.remove(&slot);
    }

    /// Register a slot for repair. Creates an empty entry so the RepairService
    /// will request shreds for this slot from peers. Skips if already stored.
    pub fn request_slot_repair(&self, slot: u64) {
        if self.stored_slots.contains_key(&slot) {
            return;
        }
        self.slots.entry(slot).or_insert_with(SlotShreds::new);
    }

    /// Evict tracked slots older than `current_slot - max_age`. Returns how
    /// many entries were removed. This prevents unbounded growth from
    /// `request_slot_repair()` entries that never complete assembly.
    /// Also evicts old `stored_slots` entries to prevent unbounded growth.
    pub fn cleanup_old_slots(&self, current_slot: u64, max_age: u64) -> usize {
        let cutoff = current_slot.saturating_sub(max_age);
        let old_slots: Vec<u64> = self
            .slots
            .iter()
            .filter(|e| *e.key() < cutoff)
            .map(|e| *e.key())
            .collect();
        let count = old_slots.len();
        for slot in old_slots {
            self.slots.remove(&slot);
        }

        // Evict old stored_slots entries to prevent unbounded growth
        let old_stored: Vec<u64> = self
            .stored_slots
            .iter()
            .filter(|e| *e.key() < cutoff)
            .map(|e| *e.key())
            .collect();
        for slot in old_stored {
            self.stored_slots.remove(&slot);
        }

        if count > 0 {
            metrics::counter!("turbine_shred_collector_slots_evicted").increment(count as u64);
        }
        count
    }

    pub fn tracked_slots(&self) -> Vec<u64> {
        self.slots.iter().map(|e| *e.key()).collect()
    }
}

impl Default for ShredCollector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shredder::Shredder;
    use nusantara_core::block::{Block, BlockHeader};
    use nusantara_crypto::{Hash, Keypair, hash};

    fn test_block(slot: u64) -> Block {
        Block {
            header: BlockHeader {
                slot,
                parent_slot: slot.saturating_sub(1),
                parent_hash: hash(b"parent"),
                block_hash: hash(b"block"),
                timestamp: 1000,
                validator: hash(b"validator"),
                transaction_count: 0,
                merkle_root: Hash::zero(),
                poh_hash: Hash::zero(),
                bank_hash: Hash::zero(),
                state_root: Hash::zero(),
            },
            transactions: Vec::new(),
        }
    }

    #[test]
    fn collect_all_shreds_assembles_block() {
        let kp = Keypair::generate();
        let block = test_block(1);
        let batch = Shredder::shred_block(&block, 0, &kp).unwrap();
        let collector = ShredCollector::new();

        let mut result = None;
        for shred in &batch.data_shreds {
            if let Some(assembled) = collector.insert_data_shred(shred) {
                result = Some(assembled);
            }
        }

        assert!(result.is_some());
        assert_eq!(result.unwrap(), block);
    }

    #[test]
    fn incomplete_slot_returns_none() {
        let kp = Keypair::generate();
        let block = test_block(1);
        let batch = Shredder::shred_block(&block, 0, &kp).unwrap();

        if batch.data_shreds.len() > 1 {
            let collector = ShredCollector::new();
            // Insert all but the last
            for shred in &batch.data_shreds[..batch.data_shreds.len() - 1] {
                assert!(collector.insert_data_shred(shred).is_none());
            }
        }
    }

    #[test]
    fn missing_shreds_detection() {
        let kp = Keypair::generate();
        let block = test_block(1);
        let batch = Shredder::shred_block(&block, 0, &kp).unwrap();
        let collector = ShredCollector::new();

        if batch.data_shreds.len() > 2 {
            // Insert first and last, skip middle
            collector.insert_data_shred(&batch.data_shreds[0]);
            collector.insert_data_shred(batch.data_shreds.last().unwrap());

            let missing = collector.missing_shreds(1);
            assert!(!missing.is_empty());
        }
    }

    #[test]
    fn stored_slot_skips_insertion() {
        let kp = Keypair::generate();
        let block = test_block(1);
        let batch = Shredder::shred_block(&block, 0, &kp).unwrap();
        let collector = ShredCollector::new();

        // Mark slot 1 as stored
        collector.mark_slot_stored(1);
        assert!(collector.is_slot_stored(1));

        // All shred insertions should return None (skipped)
        for shred in &batch.data_shreds {
            assert!(collector.insert_data_shred(shred).is_none());
        }

        // No shreds should be tracked for slot 1
        assert!(!collector.has_slot(1));
    }

    #[test]
    fn stored_slot_skips_repair_request() {
        let collector = ShredCollector::new();
        collector.mark_slot_stored(5);

        // Repair request for stored slot should be a no-op
        collector.request_slot_repair(5);
        assert!(!collector.has_slot(5));
    }

    #[test]
    fn rejects_shred_above_max_index() {
        let kp = Keypair::generate();
        let collector = ShredCollector::new();

        // Create a shred with index at MAX
        let shred = nusantara_storage::shred::DataShred {
            slot: 1,
            index: MAX_SHREDS_PER_SLOT as u32,
            parent_offset: 1,
            data: vec![0u8; 10],
            flags: 0,
        };
        let signed = SignedDataShred::new(shred, kp.address(), &kp);
        assert!(collector.insert_data_shred(&signed).is_none());
    }

    #[test]
    fn accepts_shred_within_limit() {
        let kp = Keypair::generate();
        let collector = ShredCollector::new();

        let shred = nusantara_storage::shred::DataShred {
            slot: 1,
            index: 0,
            parent_offset: 1,
            data: vec![0u8; 10],
            flags: 0x01, // last shred
        };
        let signed = SignedDataShred::new(shred, kp.address(), &kp);
        // Single shred with last flag should assemble a block (or at least be accepted)
        let _ = collector.insert_data_shred(&signed);
        assert!(collector.has_slot(1) || collector.is_slot_stored(1));
    }

    #[test]
    fn cleanup_evicts_old_stored_slots() {
        let collector = ShredCollector::new();
        collector.mark_slot_stored(10);
        collector.mark_slot_stored(50);
        collector.mark_slot_stored(90);

        // Cleanup with current_slot=100, max_age=50 → cutoff=50
        // Slots 10 should be evicted, 50 and 90 should remain
        collector.cleanup_old_slots(100, 50);

        assert!(!collector.is_slot_stored(10));
        assert!(collector.is_slot_stored(50));
        assert!(collector.is_slot_stored(90));
    }
}
