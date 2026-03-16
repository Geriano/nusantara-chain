use std::sync::Arc;

use borsh::{BorshDeserialize, BorshSerialize};
use dashmap::DashMap;
use nusantara_core::Account;
use nusantara_core::epoch::EpochSchedule;
use nusantara_crypto::{hashv, Hash};
use nusantara_stake_program::Delegation;
use nusantara_storage::Storage;
use nusantara_sysvar_program::{Clock, SlotHashes, StakeHistory, StakeHistoryEntry};
use nusantara_vote_program::VoteState;
use parking_lot::{Mutex, RwLock};
use tracing::instrument;

use crate::error::ConsensusError;
use crate::state_tree::{StateMerkleProof, StateTree};

#[derive(Clone, Debug, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct FrozenBankState {
    pub slot: u64,
    pub parent_slot: u64,
    pub block_hash: Hash,
    pub bank_hash: Hash,
    pub epoch: u64,
    pub transaction_count: u64,
}

pub struct ConsensusBank {
    storage: Arc<Storage>,
    epoch_schedule: EpochSchedule,
    vote_accounts: DashMap<Hash, VoteState>,
    stake_delegations: DashMap<Hash, Delegation>,
    epoch_stakes: DashMap<Hash, u64>,
    total_active_stake: RwLock<u64>,
    total_supply: RwLock<u64>,
    clock: RwLock<Clock>,
    slot_hashes: RwLock<SlotHashes>,
    stake_history: RwLock<StakeHistory>,
    current_slot: RwLock<u64>,
    /// Validator identity -> total slashed lamports. Reduces effective stake
    /// without modifying the Delegation structs (avoids serialization breakage).
    slash_registry: DashMap<Hash, u64>,
    /// Incremental Merkle tree over all account state.
    /// Protected by a `Mutex` (not held across `.await` points).
    state_tree: Mutex<StateTree>,
}

impl ConsensusBank {
    pub fn new(storage: Arc<Storage>, epoch_schedule: EpochSchedule) -> Self {
        Self {
            storage,
            epoch_schedule,
            vote_accounts: DashMap::new(),
            stake_delegations: DashMap::new(),
            epoch_stakes: DashMap::new(),
            total_active_stake: RwLock::new(0),
            total_supply: RwLock::new(0),
            clock: RwLock::new(Clock::default()),
            slot_hashes: RwLock::new(SlotHashes::default()),
            stake_history: RwLock::new(StakeHistory::default()),
            current_slot: RwLock::new(0),
            slash_registry: DashMap::new(),
            state_tree: Mutex::new(StateTree::new()),
        }
    }

    pub fn storage(&self) -> &Arc<Storage> {
        &self.storage
    }

    pub fn epoch_schedule(&self) -> &EpochSchedule {
        &self.epoch_schedule
    }

    pub fn current_slot(&self) -> u64 {
        *self.current_slot.read()
    }

    pub fn current_epoch(&self) -> u64 {
        self.epoch_schedule.get_epoch(self.current_slot())
    }

    /// Register a vote account.
    pub fn set_vote_state(&self, vote_account: Hash, state: VoteState) {
        self.vote_accounts.insert(vote_account, state);
    }

    /// Get vote state for a vote account.
    pub fn get_vote_state(&self, vote_account: &Hash) -> Option<VoteState> {
        self.vote_accounts.get(vote_account).map(|v| v.clone())
    }

    /// Update vote state after processing a vote.
    pub fn update_vote_state(&self, vote_account: &Hash, vote_state: VoteState) {
        self.vote_accounts.insert(*vote_account, vote_state);
    }

    /// Register a stake delegation.
    pub fn set_stake_delegation(&self, stake_account: Hash, delegation: Delegation) {
        self.stake_delegations.insert(stake_account, delegation);
    }

    /// Get validator effective stake.
    pub fn get_validator_stake(&self, validator: &Hash) -> u64 {
        self.epoch_stakes
            .get(validator)
            .map(|v| *v)
            .unwrap_or(0)
    }

    /// Get the total active stake.
    pub fn total_active_stake(&self) -> u64 {
        *self.total_active_stake.read()
    }

    /// Get the total token supply.
    pub fn total_supply(&self) -> u64 {
        *self.total_supply.read()
    }

    /// Set the total supply (initialized from genesis sum of all accounts).
    pub fn set_total_supply(&self, supply: u64) {
        *self.total_supply.write() = supply;
        metrics::gauge!("bank_total_supply").set(supply as f64);
    }

    /// Deduct burned fees from total supply.
    pub fn burn_fees(&self, amount: u64) {
        let mut supply = self.total_supply.write();
        *supply = supply.saturating_sub(amount);
    }

    /// Update a stake delegation's effective stake in-memory.
    pub fn update_delegation_stake(&self, stake_account: &Hash, new_stake: u64) {
        if let Some(mut entry) = self.stake_delegations.get_mut(stake_account) {
            entry.stake = new_stake;
        }
    }

    /// Remove a fully-cooled-down stake delegation.
    pub fn remove_stake_delegation(&self, stake_account: &Hash) {
        self.stake_delegations.remove(stake_account);
    }

    /// Get all vote states.
    pub fn get_all_vote_states(&self) -> Vec<(Hash, VoteState)> {
        self.vote_accounts
            .iter()
            .map(|entry| (*entry.key(), entry.value().clone()))
            .collect()
    }

    /// Get all stake delegations.
    pub fn get_all_delegations(&self) -> Vec<(Hash, Delegation)> {
        self.stake_delegations
            .iter()
            .map(|entry| (*entry.key(), entry.value().clone()))
            .collect()
    }

    /// Get the stake distribution: (validator_identity, effective_stake).
    pub fn get_stake_distribution(&self) -> Vec<(Hash, u64)> {
        self.epoch_stakes
            .iter()
            .map(|entry| (*entry.key(), *entry.value()))
            .collect()
    }

    /// Recalculate effective stakes for a new epoch.
    #[instrument(skip(self), level = "info")]
    pub fn recalculate_epoch_stakes(&self, epoch: u64) {
        let mut new_stakes: HashMap<Hash, u64> = HashMap::new();
        let mut total: u64 = 0;

        for entry in self.stake_delegations.iter() {
            let delegation = entry.value();

            // Check if delegation is active in this epoch
            if delegation.activation_epoch > epoch {
                continue;
            }
            if delegation.deactivation_epoch < epoch {
                continue;
            }

            let effective_stake = if delegation.activation_epoch == epoch {
                // Still warming up - apply warmup rate
                let warmup_rate = delegation.warmup_cooldown_rate_bps as f64 / 10_000.0;
                (delegation.stake as f64 * warmup_rate) as u64
            } else if delegation.deactivation_epoch == epoch {
                // Cooling down
                let cooldown_rate = delegation.warmup_cooldown_rate_bps as f64 / 10_000.0;
                (delegation.stake as f64 * (1.0 - cooldown_rate)) as u64
            } else {
                delegation.stake
            };

            // Map vote account -> validator identity via VoteState
            let identity = self
                .vote_accounts
                .get(&delegation.voter_pubkey)
                .map(|vs| vs.node_pubkey)
                .unwrap_or(delegation.voter_pubkey);
            *new_stakes.entry(identity).or_default() += effective_stake;
            total += effective_stake;
        }

        // Apply slash penalties before committing epoch stakes
        for (validator, stake) in &mut new_stakes {
            let slashed = self.get_slashed_amount(validator);
            if slashed > 0 {
                let before = *stake;
                *stake = stake.saturating_sub(slashed);
                total = total.saturating_sub(before - *stake);
            }
        }

        // Update epoch_stakes
        self.epoch_stakes.clear();
        for (validator, stake) in new_stakes {
            self.epoch_stakes.insert(validator, stake);
        }
        *self.total_active_stake.write() = total;

        metrics::gauge!("bank_total_active_stake").set(total as f64);
        metrics::gauge!("bank_epoch_stake_validators").set(self.epoch_stakes.len() as f64);
    }

    /// Advance to a new slot, updating the Clock sysvar.
    #[instrument(skip(self), level = "debug")]
    pub fn advance_slot(&self, slot: u64, timestamp: i64) {
        *self.current_slot.write() = slot;

        let epoch = self.epoch_schedule.get_epoch(slot);
        let mut clock = self.clock.write();
        clock.slot = slot;
        clock.unix_timestamp = timestamp;
        clock.epoch = epoch;
        clock.leader_schedule_epoch = epoch + 1;

        metrics::gauge!("bank_current_slot").set(slot as f64);
    }

    /// Update slot hashes sysvar.
    /// Replaces any existing entry for the same slot (e.g. when an orphan block
    /// arrives and replaces a previously recorded skip Hash::zero()).
    pub fn record_slot_hash(&self, slot: u64, hash: Hash) {
        let mut slot_hashes = self.slot_hashes.write();
        slot_hashes.0.retain(|(s, _)| *s != slot);
        slot_hashes.0.insert(0, (slot, hash));
        slot_hashes.0.truncate(512);
    }

    /// Update stake history sysvar.
    pub fn update_stake_history(&self, epoch: u64, entry: StakeHistoryEntry) {
        let mut history = self.stake_history.write();
        history.0.insert(0, (epoch, entry));
        // Keep max 512 entries
        history.0.truncate(512);
    }

    /// Get the Clock sysvar.
    pub fn clock(&self) -> Clock {
        self.clock.read().clone()
    }

    /// Get the SlotHashes sysvar.
    pub fn slot_hashes(&self) -> SlotHashes {
        self.slot_hashes.read().clone()
    }

    /// Replace slot_hashes entirely.
    ///
    /// Used during cross-fork block replay to match the block producer's
    /// `RecentBlockhashes` sysvar. Without this, the bank's slot_hashes
    /// reflects only this validator's own chain history, which diverges
    /// from the producer's chain when validators ran on separate forks.
    pub fn set_slot_hashes(&self, slot_hashes: SlotHashes) {
        *self.slot_hashes.write() = slot_hashes;
    }

    /// Get the StakeHistory sysvar.
    pub fn stake_history(&self) -> StakeHistory {
        self.stake_history.read().clone()
    }

    /// Compute the bank hash from parent hash and account delta hash.
    pub fn compute_bank_hash(parent_bank_hash: &Hash, account_delta_hash: &Hash) -> Hash {
        hashv(&[parent_bank_hash.as_bytes(), account_delta_hash.as_bytes()])
    }

    /// Freeze the bank state for the current slot.
    #[instrument(skip(self), level = "info")]
    pub fn freeze(
        &self,
        slot: u64,
        parent_slot: u64,
        block_hash: Hash,
        parent_bank_hash: &Hash,
        account_delta_hash: &Hash,
        transaction_count: u64,
    ) -> FrozenBankState {
        let bank_hash = Self::compute_bank_hash(parent_bank_hash, account_delta_hash);
        let epoch = self.epoch_schedule.get_epoch(slot);

        metrics::counter!("bank_slots_frozen_total").increment(1);

        FrozenBankState {
            slot,
            parent_slot,
            block_hash,
            bank_hash,
            epoch,
            transaction_count,
        }
    }

    /// Persist critical state to storage.
    #[instrument(skip(self), level = "info")]
    pub fn flush_to_storage(&self, frozen: &FrozenBankState) -> Result<(), ConsensusError> {
        self.storage
            .put_bank_hash(frozen.slot, &frozen.bank_hash)?;
        self.storage
            .put_slot_hash(frozen.slot, &frozen.block_hash)?;
        Ok(())
    }

    /// Mark a slot as a finalized root in storage.
    pub fn set_root(&self, slot: u64) -> Result<(), ConsensusError> {
        self.storage.set_root(slot)?;
        Ok(())
    }

    /// Record a skipped slot in slot_hashes with Hash::zero().
    pub fn record_skipped_slot(&self, slot: u64) {
        self.record_slot_hash(slot, nusantara_crypto::Hash::zero());
    }

    /// Record a slash penalty for a validator. Accumulates into the slash registry.
    pub fn apply_slash(&self, validator: &Hash, amount: u64) {
        self.slash_registry
            .entry(*validator)
            .and_modify(|total| *total += amount)
            .or_insert(amount);
        metrics::counter!("slashing_penalties_applied").increment(1);
        metrics::counter!("slashing_total_slashed_lamports").increment(amount);
    }

    /// Get total slashed amount for a validator.
    pub fn get_slashed_amount(&self, validator: &Hash) -> u64 {
        self.slash_registry.get(validator).map(|v| *v).unwrap_or(0)
    }

    /// Get all slash entries: (validator_identity, total_slashed_lamports).
    pub fn get_all_slashes(&self) -> Vec<(Hash, u64)> {
        self.slash_registry
            .iter()
            .map(|e| (*e.key(), *e.value()))
            .collect()
    }

    /// Update the state Merkle tree with account deltas from a slot execution.
    ///
    /// This should be called after committing account deltas to storage
    /// so the state root reflects the latest on-chain state.
    #[instrument(skip_all, fields(delta_count = deltas.len()), level = "debug")]
    pub fn update_state_tree(&self, deltas: &[(Hash, Account)]) {
        let mut tree = self.state_tree.lock();
        tree.update(deltas);
        metrics::gauge!("state_tree_leaf_count").set(tree.len() as f64);
    }

    /// Compute the current state Merkle root.
    pub fn state_root(&self) -> Hash {
        self.state_tree.lock().root()
    }

    /// Number of accounts tracked in the state tree.
    pub fn state_tree_len(&self) -> usize {
        self.state_tree.lock().len()
    }

    /// Generate a state Merkle proof for a specific account.
    pub fn state_proof(&self, address: &Hash) -> Option<StateMerkleProof> {
        self.state_tree.lock().proof(address)
    }

    /// Replace the state tree (e.g., after loading from storage at boot).
    pub fn set_state_tree(&self, tree: StateTree) {
        *self.state_tree.lock() = tree;
    }

    /// Rollback bank state to a given ancestor slot.
    /// Resets the clock and current_slot to the ancestor.
    pub fn rollback_to_slot(&self, slot: u64, storage: &Storage) -> Result<(), ConsensusError> {
        // Reset current_slot
        *self.current_slot.write() = slot;

        // Try to get block header to restore timestamp
        if let Some(header) = storage.get_block_header(slot)? {
            let epoch = self.epoch_schedule.get_epoch(slot);
            let mut clock = self.clock.write();
            clock.slot = slot;
            clock.unix_timestamp = header.timestamp;
            clock.epoch = epoch;
            clock.leader_schedule_epoch = epoch + 1;
        }

        // Rebuild slot_hashes: keep only entries at or before the target slot
        let mut slot_hashes = self.slot_hashes.write();
        slot_hashes.0.retain(|(s, _)| *s <= slot);

        Ok(())
    }
}

use std::collections::HashMap;

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_bank() -> (ConsensusBank, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(Storage::open(dir.path()).unwrap());
        let bank = ConsensusBank::new(storage, EpochSchedule::new(100));
        (bank, dir)
    }

    #[test]
    fn new_bank() {
        let (bank, _dir) = temp_bank();
        assert_eq!(bank.current_slot(), 0);
        assert_eq!(bank.current_epoch(), 0);
        assert_eq!(bank.total_active_stake(), 0);
    }

    #[test]
    fn vote_state_crud() {
        let (bank, _dir) = temp_bank();
        let addr = nusantara_crypto::hash(b"vote_acc");
        let vs = VoteState::new(&nusantara_vote_program::VoteInit {
            node_pubkey: nusantara_crypto::hash(b"node"),
            authorized_voter: nusantara_crypto::hash(b"voter"),
            authorized_withdrawer: nusantara_crypto::hash(b"wd"),
            commission: 10,
        });

        assert!(bank.get_vote_state(&addr).is_none());
        bank.set_vote_state(addr, vs.clone());
        assert_eq!(bank.get_vote_state(&addr).unwrap(), vs);
    }

    #[test]
    fn stake_delegation_and_recalculate() {
        let (bank, _dir) = temp_bank();
        let voter = nusantara_crypto::hash(b"voter");

        for i in 0..5u64 {
            let acc = nusantara_crypto::hash(&i.to_le_bytes());
            bank.set_stake_delegation(
                acc,
                Delegation {
                    voter_pubkey: voter,
                    stake: 1_000_000,
                    activation_epoch: 0,
                    deactivation_epoch: u64::MAX,
                    warmup_cooldown_rate_bps: 2500,
                },
            );
        }

        bank.recalculate_epoch_stakes(1);
        assert_eq!(bank.get_validator_stake(&voter), 5_000_000);
        assert_eq!(bank.total_active_stake(), 5_000_000);
    }

    #[test]
    fn advance_slot_updates_clock() {
        let (bank, _dir) = temp_bank();
        bank.advance_slot(42, 1234567890);
        let clock = bank.clock();
        assert_eq!(clock.slot, 42);
        assert_eq!(clock.unix_timestamp, 1234567890);
        assert_eq!(clock.epoch, 0); // 42 < 100 (slots_per_epoch)
    }

    #[test]
    fn record_slot_hash() {
        let (bank, _dir) = temp_bank();
        let h = nusantara_crypto::hash(b"block1");
        bank.record_slot_hash(1, h);
        let sh = bank.slot_hashes();
        assert_eq!(sh.get(1), Some(&h));
    }

    #[test]
    fn freeze_and_flush() {
        let (bank, _dir) = temp_bank();
        let block_hash = nusantara_crypto::hash(b"block");
        let parent_bank = nusantara_crypto::hash(b"parent_bank");
        let delta = nusantara_crypto::hash(b"delta");

        let frozen = bank.freeze(1, 0, block_hash, &parent_bank, &delta, 10);
        assert_eq!(frozen.slot, 1);
        assert_eq!(frozen.transaction_count, 10);

        bank.flush_to_storage(&frozen).unwrap();
        let stored_bank_hash = bank.storage().get_bank_hash(1).unwrap();
        assert_eq!(stored_bank_hash, Some(frozen.bank_hash));
    }

    #[test]
    fn compute_bank_hash_deterministic() {
        let p = nusantara_crypto::hash(b"parent");
        let d = nusantara_crypto::hash(b"delta");
        let h1 = ConsensusBank::compute_bank_hash(&p, &d);
        let h2 = ConsensusBank::compute_bank_hash(&p, &d);
        assert_eq!(h1, h2);
    }

    #[test]
    fn epoch_boundary_detection() {
        let (bank, _dir) = temp_bank();
        bank.advance_slot(99, 1000);
        assert_eq!(bank.current_epoch(), 0);
        bank.advance_slot(100, 1001);
        assert_eq!(bank.current_epoch(), 1);
    }

    #[test]
    fn slash_reduces_effective_stake() {
        let (bank, _dir) = temp_bank();
        let voter = nusantara_crypto::hash(b"voter");

        // Set up vote state so identity resolves to voter itself
        bank.set_stake_delegation(
            nusantara_crypto::hash(b"stake_acc"),
            Delegation {
                voter_pubkey: voter,
                stake: 10_000_000,
                activation_epoch: 0,
                deactivation_epoch: u64::MAX,
                warmup_cooldown_rate_bps: 2500,
            },
        );

        // Without slash: full stake
        bank.recalculate_epoch_stakes(1);
        assert_eq!(bank.get_validator_stake(&voter), 10_000_000);

        // Apply a 1M lamport slash
        bank.apply_slash(&voter, 1_000_000);
        assert_eq!(bank.get_slashed_amount(&voter), 1_000_000);

        // Recalculate: effective stake reduced by slash
        bank.recalculate_epoch_stakes(2);
        assert_eq!(bank.get_validator_stake(&voter), 9_000_000);
    }

    #[test]
    fn slash_cannot_exceed_stake() {
        let (bank, _dir) = temp_bank();
        let voter = nusantara_crypto::hash(b"voter");

        bank.set_stake_delegation(
            nusantara_crypto::hash(b"stake_acc"),
            Delegation {
                voter_pubkey: voter,
                stake: 1_000,
                activation_epoch: 0,
                deactivation_epoch: u64::MAX,
                warmup_cooldown_rate_bps: 2500,
            },
        );

        // Slash more than the stake
        bank.apply_slash(&voter, 999_999_999);
        bank.recalculate_epoch_stakes(1);
        // saturating_sub: effective stake = 0, not negative
        assert_eq!(bank.get_validator_stake(&voter), 0);
    }
}
