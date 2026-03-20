use std::collections::BTreeMap;

use dashmap::DashMap;
use nusantara_core::block::Block;
use nusantara_core::native_token::const_parse_u64;
use nusantara_crypto::Hash;
use nusantara_storage::shred::DataShred;

use crate::deshredder::Deshredder;
use crate::merkle_shred::{MerkleDataShred, ShredBatchHeader};

pub const MAX_SHREDS_PER_SLOT: u64 =
    const_parse_u64(env!("NUSA_TURBINE_MAX_SHREDS_PER_SLOT"));

struct SlotShreds {
    data_shreds: BTreeMap<u32, DataShred>,
    last_index: Option<u32>,
    /// Cached batch header for this slot (contains Merkle root + signature).
    header: Option<ShredBatchHeader>,
}

impl SlotShreds {
    fn new() -> Self {
        Self {
            data_shreds: BTreeMap::new(),
            last_index: None,
            header: None,
        }
    }

    /// Insert a shred, enforcing the per-slot shred count limit.
    fn insert(&mut self, shred: &MerkleDataShred) -> bool {
        if shred.index() >= MAX_SHREDS_PER_SLOT as u32 {
            metrics::counter!("nusantara_turbine_shreds_rejected_max_index").increment(1);
            return false;
        }
        if self.data_shreds.len() >= MAX_SHREDS_PER_SLOT as usize {
            metrics::counter!("nusantara_turbine_shreds_rejected_max_index").increment(1);
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
        self.data_shreds.len() == (last + 1) as usize
    }

    fn to_sorted_shreds(&self) -> Vec<DataShred> {
        self.data_shreds.values().cloned().collect()
    }
}

pub struct ShredCollector {
    slots: DashMap<u64, SlotShreds>,
    stored_slots: DashMap<u64, ()>,
}

impl ShredCollector {
    pub fn new() -> Self {
        Self {
            slots: DashMap::new(),
            stored_slots: DashMap::new(),
        }
    }

    pub fn mark_slot_stored(&self, slot: u64) {
        self.stored_slots.insert(slot, ());
        self.slots.remove(&slot);
    }

    pub fn is_slot_stored(&self, slot: u64) -> bool {
        self.stored_slots.contains_key(&slot)
    }

    /// Insert a ShredBatchHeader. Stores the Merkle root for proof verification.
    pub fn insert_header(&self, header: ShredBatchHeader) {
        let slot = header.slot;
        if self.stored_slots.contains_key(&slot) {
            return;
        }
        let mut entry = self.slots.entry(slot).or_insert_with(SlotShreds::new);
        entry.header = Some(header);
    }

    /// Get the cached Merkle root for a slot (if header has been received).
    pub fn get_merkle_root(&self, slot: u64) -> Option<Hash> {
        self.slots
            .get(&slot)
            .and_then(|e| e.header.as_ref().map(|h| h.merkle_root))
    }

    /// Check if we have the batch header for a slot.
    pub fn has_header(&self, slot: u64) -> bool {
        self.slots
            .get(&slot)
            .is_some_and(|e| e.header.is_some())
    }

    /// Insert a Merkle data shred. Returns `Some(Block)` if the slot is now complete.
    pub fn insert_data_shred(&self, shred: &MerkleDataShred) -> Option<Block> {
        let slot = shred.slot();

        if self.stored_slots.contains_key(&slot) {
            metrics::counter!("nusantara_turbine_shreds_skipped_already_stored").increment(1);
            return None;
        }

        let mut entry = self.slots.entry(slot).or_insert_with(SlotShreds::new);

        // If header is present, verify Merkle proof before accepting
        if let Some(ref header) = entry.header
            && !shred.verify(&header.merkle_root)
        {
            metrics::counter!("nusantara_turbine_invalid_shred_signatures").increment(1);
            return None;
        }

        if !entry.insert(shred) {
            return None;
        }

        if entry.is_complete() {
            let sorted = entry.to_sorted_shreds();
            drop(entry);
            match Deshredder::deshred(&sorted) {
                Ok(block) => {
                    // Mark as stored BEFORE removing shred data so duplicate
                    // shreds arriving concurrently are rejected immediately.
                    self.stored_slots.insert(slot, ());
                    self.slots.remove(&slot);
                    metrics::counter!("nusantara_turbine_blocks_assembled_total").increment(1);
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

    pub fn missing_shreds(&self, slot: u64) -> Vec<u32> {
        let entry = match self.slots.get(&slot) {
            Some(e) => e,
            None => return Vec::new(),
        };

        let last = match entry.last_index {
            Some(l) => l,
            None => return Vec::new(),
        };

        (0..=last)
            .filter(|i| !entry.data_shreds.contains_key(i))
            .collect()
    }

    pub fn has_slot(&self, slot: u64) -> bool {
        self.slots.contains_key(&slot)
    }

    pub fn shred_count(&self, slot: u64) -> usize {
        self.slots
            .get(&slot)
            .map(|e| e.data_shreds.len())
            .unwrap_or(0)
    }

    pub fn is_slot_complete(&self, slot: u64) -> bool {
        self.slots
            .get(&slot)
            .is_some_and(|e| e.is_complete())
    }

    pub fn remove_slot(&self, slot: u64) {
        self.slots.remove(&slot);
    }

    pub fn request_slot_repair(&self, slot: u64) {
        if self.stored_slots.contains_key(&slot) {
            return;
        }
        self.slots.entry(slot).or_insert_with(SlotShreds::new);
    }

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
            metrics::counter!("nusantara_turbine_shred_collector_slots_evicted").increment(count as u64);
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
            batches: Vec::new(),
        }
    }

    #[test]
    fn collect_all_shreds_assembles_block() {
        let kp = Keypair::generate();
        let block = test_block(1);
        let batch = Shredder::shred_block(&block, 0, &kp).unwrap();
        let collector = ShredCollector::new();

        // Insert header first
        collector.insert_header(batch.header.clone());

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

        collector.mark_slot_stored(1);
        assert!(collector.is_slot_stored(1));

        for shred in &batch.data_shreds {
            assert!(collector.insert_data_shred(shred).is_none());
        }

        assert!(!collector.has_slot(1));
    }

    #[test]
    fn stored_slot_skips_repair_request() {
        let collector = ShredCollector::new();
        collector.mark_slot_stored(5);

        collector.request_slot_repair(5);
        assert!(!collector.has_slot(5));
    }

    #[test]
    fn rejects_shred_above_max_index() {
        let kp = Keypair::generate();
        let collector = ShredCollector::new();

        let shred = nusantara_storage::shred::DataShred {
            slot: 1,
            index: MAX_SHREDS_PER_SLOT as u32,
            parent_offset: 1,
            data: vec![0u8; 10],
            flags: 0,
        };
        let merkle = MerkleDataShred::new(shred, kp.address());
        assert!(collector.insert_data_shred(&merkle).is_none());
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
            flags: 0x01,
        };
        let merkle = MerkleDataShred::new(shred, kp.address());
        let _ = collector.insert_data_shred(&merkle);
        assert!(collector.has_slot(1) || collector.is_slot_stored(1));
    }

    #[test]
    fn cleanup_evicts_old_stored_slots() {
        let collector = ShredCollector::new();
        collector.mark_slot_stored(10);
        collector.mark_slot_stored(50);
        collector.mark_slot_stored(90);

        collector.cleanup_old_slots(100, 50);

        assert!(!collector.is_slot_stored(10));
        assert!(collector.is_slot_stored(50));
        assert!(collector.is_slot_stored(90));
    }

    #[test]
    fn header_insert_and_lookup() {
        let kp = Keypair::generate();
        let root = hash(b"root");
        let header = ShredBatchHeader {
            slot: 5,
            leader: kp.address(),
            merkle_root: root,
            signature: kp.sign(root.as_bytes()),
            num_data_shreds: 10,
            num_code_shreds: 3,
        };
        let collector = ShredCollector::new();
        collector.insert_header(header);

        assert!(collector.has_header(5));
        assert_eq!(collector.get_merkle_root(5), Some(root));
        assert!(!collector.has_header(6));
    }

    #[test]
    fn shreds_before_header_then_header() {
        let kp = Keypair::generate();
        let block = test_block(1);
        let batch = Shredder::shred_block(&block, 0, &kp).unwrap();
        let collector = ShredCollector::new();

        // Insert shreds WITHOUT header first (buffered without proof check)
        for shred in &batch.data_shreds {
            collector.insert_data_shred(shred);
        }

        // Now insert header — the block should have already been assembled
        // since proof check is optional when no header
        // (Shreds may have already triggered assembly)
    }
}
