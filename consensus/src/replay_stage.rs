use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use nusantara_core::block::Block;
use nusantara_crypto::Hash;
use nusantara_vote_program::{Vote, VoteInstruction};
use tokio::sync::watch;
use tracing::instrument;

use crate::bank::{ConsensusBank, FrozenBankState};
use crate::commitment::CommitmentTracker;
use crate::error::ConsensusError;
use crate::fork_choice::ForkTree;
use crate::gpu::GpuPohVerifier;
use crate::leader_schedule::{LeaderSchedule, LeaderScheduleGenerator};
use crate::poh::{PohEntry, verify_poh_entries};
use crate::tower::Tower;

#[derive(Clone, Debug)]
pub struct ReplayResult {
    pub slot: u64,
    pub block_hash: Hash,
    pub bank_hash: Hash,
    pub parent_slot: u64,
    pub transaction_count: u64,
    pub vote_count: u64,
    pub new_root: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct ForkSwitchPlan {
    pub common_ancestor: u64,
    pub rollback_from: u64,
    pub replay_slots: Vec<u64>,
}

pub struct ReplayStage {
    bank: Arc<ConsensusBank>,
    tower: Tower,
    fork_tree: ForkTree,
    commitment_tracker: CommitmentTracker,
    leader_schedule_cache: HashMap<u64, LeaderSchedule>,
    leader_schedule_generator: LeaderScheduleGenerator,
    gpu_verifier: Option<GpuPohVerifier>,
    current_tip: u64,
}

impl ReplayStage {
    pub fn new(
        bank: Arc<ConsensusBank>,
        tower: Tower,
        fork_tree: ForkTree,
        commitment_tracker: CommitmentTracker,
        gpu_verifier: Option<GpuPohVerifier>,
    ) -> Self {
        let epoch_schedule = bank.epoch_schedule().clone();
        let initial_tip = fork_tree.root_slot();
        Self {
            bank,
            tower,
            fork_tree,
            commitment_tracker,
            leader_schedule_cache: HashMap::new(),
            leader_schedule_generator: LeaderScheduleGenerator::new(epoch_schedule),
            gpu_verifier,
            current_tip: initial_tip,
        }
    }

    pub fn tower(&self) -> &Tower {
        &self.tower
    }

    pub fn fork_tree(&self) -> &ForkTree {
        &self.fork_tree
    }

    pub fn commitment_tracker(&self) -> &CommitmentTracker {
        &self.commitment_tracker
    }

    pub fn bank(&self) -> &Arc<ConsensusBank> {
        &self.bank
    }

    /// Replay a block through the consensus pipeline.
    #[instrument(skip(self, block, poh_entries), fields(slot = block.header.slot), level = "info")]
    pub fn replay_block(
        &mut self,
        block: &Block,
        poh_entries: &[PohEntry],
    ) -> Result<ReplayResult, ConsensusError> {
        let slot = block.header.slot;
        let parent_slot = block.header.parent_slot;
        let block_hash = block.header.block_hash;

        tracing::debug!(slot, parent_slot, "Replaying block");

        // 1. Verify leader matches schedule (if schedule is cached)
        let epoch = self.bank.epoch_schedule().get_epoch(slot);
        if let Some(schedule) = self.leader_schedule_cache.get(&epoch)
            && let Some(expected_leader) = schedule.get_leader(slot, self.bank.epoch_schedule())
            && *expected_leader != block.header.validator
        {
            return Err(ConsensusError::WrongLeader {
                slot,
                expected: format!("{expected_leader:?}"),
                got: format!("{:?}", block.header.validator),
            });
        }

        // 2. Verify PoH entries (GPU batch if available, else CPU)
        if !poh_entries.is_empty() {
            let poh_valid = if let Some(ref gpu) = self.gpu_verifier {
                let entries: Vec<(Hash, u64, Hash)> = poh_entries
                    .windows(2)
                    .map(|w| (w[0].hash, w[1].num_hashes - w[0].num_hashes, w[1].hash))
                    .collect();
                if entries.is_empty() {
                    true
                } else {
                    match gpu.verify_batch(&entries) {
                        Ok(results) => results.iter().all(|&r| r),
                        Err(_) => {
                            tracing::warn!("GPU verification failed, falling back to CPU");
                            verify_poh_entries(
                                &poh_entries[0].hash,
                                &poh_entries[1..],
                            )
                        }
                    }
                }
            } else {
                verify_poh_entries(&poh_entries[0].hash, &poh_entries[1..])
            };

            if !poh_valid {
                return Err(ConsensusError::PohVerificationFailed { index: 0 });
            }
        }

        // 3. Add slot to fork tree
        // Use the block header's bank_hash directly. For leader-produced blocks,
        // BlockProducer computed it from real account deltas. For observer-replayed
        // blocks, replay_block_full() verified it via re-execution.
        let frozen = FrozenBankState {
            slot,
            parent_slot,
            block_hash,
            bank_hash: block.header.bank_hash,
            epoch: self.bank.epoch_schedule().get_epoch(slot),
            transaction_count: block.header.transaction_count,
        };

        self.fork_tree
            .add_slot(slot, parent_slot, block_hash, frozen.bank_hash)?;

        // 4. Extract vote transactions and process through Tower
        let mut vote_count = 0u64;
        let mut new_root = None;
        let root_slot = self.tower.root_slot().unwrap_or(0);

        for tx in &block.transactions {
            if let Some(vote) = extract_vote_from_transaction(tx) {
                // Pre-filter: skip votes whose highest slot is at or below our root.
                // This avoids a burst of VoteTooOld errors when replaying blocks
                // that contain stale vote history from other validators.
                let highest_vote_slot = vote.slots.last().copied().unwrap_or(0);
                if highest_vote_slot <= root_slot {
                    continue;
                }

                // Process through tower
                match self.tower.process_vote(&vote) {
                    Ok(result) => {
                        // Update fork tree with vote
                        if let Some(&voted_slot) = vote.slots.last() {
                            let stake = self.bank.get_validator_stake(&block.header.validator);
                            self.fork_tree.add_vote(voted_slot, stake);

                            // Update commitment tracker
                            let voted_block_hash = self
                                .fork_tree
                                .get_node(voted_slot)
                                .map(|n| n.block_hash)
                                .unwrap_or(vote.hash);
                            self.commitment_tracker
                                .record_vote(voted_slot, voted_block_hash, stake);
                        }

                        // Check for root advancement
                        if let Some(root) = result.new_root_slot {
                            new_root = Some(root);
                            self.commitment_tracker.mark_finalized(root);
                        }

                        vote_count += 1;
                    }
                    Err(e) => {
                        tracing::debug!(?e, "Vote processing failed, skipping");
                    }
                }
            }
        }

        // 5. Advance bank slot
        self.bank.advance_slot(slot, block.header.timestamp);
        self.bank.record_slot_hash(slot, block_hash);

        // 6. Root advancement is deferred to the caller via `advance_root()`.
        //
        // Previously this was done inline, but the caller (ValidatorNode) needs
        // to gate root advancement on whether orphan blocks would be pruned.
        // Premature root advancement permanently prevents replay of cross-
        // validator blocks whose parents have been pruned from the fork tree.

        // 7. Compute best fork
        self.fork_tree.compute_best_fork();

        // 8. Freeze bank -> persist
        self.bank.flush_to_storage(&frozen)?;

        // Track commitment for this slot
        self.commitment_tracker.track_slot(slot, block_hash);

        // Update current tip
        self.current_tip = slot;

        metrics::counter!("replay_blocks_processed_total").increment(1);
        metrics::counter!("replay_votes_processed_total").increment(vote_count);

        Ok(ReplayResult {
            slot,
            block_hash,
            bank_hash: frozen.bank_hash,
            parent_slot,
            transaction_count: block.header.transaction_count,
            vote_count,
            new_root,
        })
    }

    pub fn current_tip(&self) -> u64 {
        self.current_tip
    }

    /// Advance the fork tree root to the given slot.
    ///
    /// This prunes all fork tree nodes below `root` and marks the slot as
    /// finalized in storage. The caller is responsible for deciding WHEN to
    /// advance — typically gated on whether pending orphan blocks would lose
    /// their parents.
    pub fn advance_root(&mut self, root: u64) -> Result<(), ConsensusError> {
        if !self.fork_tree.contains(root) {
            tracing::debug!(
                root,
                "skipping root advancement — slot not in fork tree"
            );
            return Ok(());
        }
        let pruned = self.fork_tree.set_root(root);
        self.commitment_tracker.prune_below(root);
        self.bank.set_root(root)?;
        tracing::info!(root, pruned_count = pruned.len(), "Root advanced");
        Ok(())
    }

    /// Check if we should switch to a different fork.
    ///
    /// Returns `Some(ForkSwitchPlan)` if the best fork diverges from our current tip
    /// and Tower lockout rules allow switching.
    pub fn check_fork_switch(&self) -> Option<ForkSwitchPlan> {
        let best = self.fork_tree.best_slot();
        if best == self.current_tip {
            return None;
        }

        let best_ancestry = self.fork_tree.get_ancestry(best);
        let tip_ancestry = self.fork_tree.get_ancestry(self.current_tip);

        // Find common ancestor
        let tip_set: HashSet<u64> = tip_ancestry.iter().copied().collect();
        let common = *best_ancestry.iter().find(|s| tip_set.contains(s))?;

        // If common ancestor == current_tip, best is a descendant — no switch needed
        if common == self.current_tip {
            return None;
        }

        // Check Tower lockout allows switching
        if self.tower.check_vote_lockout(best).is_err() {
            // Check switch threshold (38%) — need enough stake on alternative fork
            let alt_stake = self.fork_tree.get_node(best)?.subtree_stake;
            let total = self.fork_tree.total_active_stake();
            if total == 0 || alt_stake * 100 / total < crate::tower::SWITCH_THRESHOLD_PERCENTAGE {
                return None;
            }
        }

        // Build replay path: common_ancestor → ... → best
        let mut replay_slots: Vec<u64> = best_ancestry
            .into_iter()
            .take_while(|s| *s != common)
            .collect();
        replay_slots.reverse();

        Some(ForkSwitchPlan {
            common_ancestor: common,
            rollback_from: self.current_tip,
            replay_slots,
        })
    }

    /// Process a gossip vote from a peer validator.
    pub fn process_gossip_vote(&mut self, _voter: Hash, slot: u64, hash: Hash, stake: u64) {
        self.fork_tree.add_vote(slot, stake);
        if self.fork_tree.contains(slot) {
            self.commitment_tracker.record_vote(slot, hash, stake);
        }
    }

    /// Cache a leader schedule for the given epoch.
    pub fn cache_leader_schedule(&mut self, epoch: u64, schedule: LeaderSchedule) {
        self.leader_schedule_cache.insert(epoch, schedule);
    }

    /// Get or compute leader schedule for the given epoch.
    pub fn get_leader_schedule(
        &mut self,
        epoch: u64,
        epoch_seed: &Hash,
    ) -> Result<&LeaderSchedule, ConsensusError> {
        if !self.leader_schedule_cache.contains_key(&epoch) {
            let stakes = self.bank.get_stake_distribution();
            let schedule =
                self.leader_schedule_generator
                    .compute_schedule(epoch, &stakes, epoch_seed)?;
            self.leader_schedule_cache.insert(epoch, schedule);
        }
        Ok(self.leader_schedule_cache.get(&epoch).unwrap())
    }

    /// Main async replay loop.
    #[instrument(skip(self, block_receiver, shutdown), level = "info")]
    pub async fn run(
        &mut self,
        mut block_receiver: tokio::sync::mpsc::Receiver<(Block, Vec<PohEntry>)>,
        mut shutdown: watch::Receiver<bool>,
    ) {
        tracing::info!("ReplayStage started");

        loop {
            // Bias toward processing blocks over shutdown
            tokio::select! {
                biased;
                Some((block, poh_entries)) = block_receiver.recv() => {
                    let slot = block.header.slot;
                    match self.replay_block(&block, &poh_entries) {
                        Ok(result) => {
                            // In standalone run() mode, always advance root
                            if let Some(root) = result.new_root
                                && let Err(e) = self.advance_root(root)
                            {
                                tracing::warn!(?e, root, "root advancement failed");
                            }
                            tracing::info!(
                                slot = result.slot,
                                votes = result.vote_count,
                                root = ?result.new_root,
                                "Block replayed successfully"
                            );
                        }
                        Err(e) => {
                            tracing::error!(slot, ?e, "Block replay failed");
                        }
                    }
                }
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        tracing::info!("ReplayStage shutting down");
                        break;
                    }
                }
            }
        }
    }
}

/// Extract a Vote from a transaction by looking for vote program instructions.
fn extract_vote_from_transaction(
    tx: &nusantara_core::transaction::Transaction,
) -> Option<Vote> {
    use nusantara_core::program::VOTE_PROGRAM_ID;

    for ix in &tx.message.instructions {
        let program_id = tx
            .message
            .account_keys
            .get(ix.program_id_index as usize)?;
        if *program_id == *VOTE_PROGRAM_ID
            && let Ok(vote_ix) = borsh::from_slice::<VoteInstruction>(&ix.data)
        {
            match vote_ix {
                VoteInstruction::Vote(vote) => return Some(vote),
                VoteInstruction::SwitchVote(vote, _) => return Some(vote),
                _ => {}
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use nusantara_core::block::{Block, BlockHeader};
    use nusantara_core::epoch::EpochSchedule;
    use nusantara_crypto::hash;
    use nusantara_storage::Storage;
    use nusantara_vote_program::{VoteInit, VoteState};

    fn make_replay_stage() -> (ReplayStage, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(Storage::open(dir.path()).unwrap());
        let epoch_schedule = EpochSchedule::new(100);
        let bank = Arc::new(ConsensusBank::new(storage, epoch_schedule));

        let init = VoteInit {
            node_pubkey: hash(b"node"),
            authorized_voter: hash(b"voter"),
            authorized_withdrawer: hash(b"wd"),
            commission: 10,
        };
        let tower = Tower::new(VoteState::new(&init));
        let fork_tree = ForkTree::new(0, hash(b"genesis"), hash(b"genesis_bank"));
        let commitment = CommitmentTracker::new(1000);

        let stage = ReplayStage::new(bank, tower, fork_tree, commitment, None);
        (stage, dir)
    }

    fn make_block(slot: u64, parent_slot: u64) -> Block {
        Block {
            header: BlockHeader {
                slot,
                parent_slot,
                parent_hash: hash(format!("parent_{parent_slot}").as_bytes()),
                block_hash: hash(format!("block_{slot}").as_bytes()),
                timestamp: 1000 + slot as i64,
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
    fn replay_empty_block() {
        let (mut stage, _dir) = make_replay_stage();
        let block = make_block(1, 0);
        let result = stage.replay_block(&block, &[]).unwrap();
        assert_eq!(result.slot, 1);
        assert_eq!(result.vote_count, 0);
        assert!(result.new_root.is_none());
    }

    #[test]
    fn replay_sequential_blocks() {
        let (mut stage, _dir) = make_replay_stage();
        for slot in 1..=5 {
            let block = make_block(slot, slot - 1);
            let result = stage.replay_block(&block, &[]).unwrap();
            assert_eq!(result.slot, slot);
        }
        assert_eq!(stage.fork_tree().node_count(), 6); // root + 5 blocks
    }

    #[test]
    fn replay_fork() {
        let (mut stage, _dir) = make_replay_stage();
        // Linear: 0 -> 1 -> 2
        stage.replay_block(&make_block(1, 0), &[]).unwrap();
        stage.replay_block(&make_block(2, 1), &[]).unwrap();
        // Fork: 0 -> 3
        stage.replay_block(&make_block(3, 0), &[]).unwrap();

        assert_eq!(stage.fork_tree().node_count(), 4);
    }

    #[test]
    fn replay_duplicate_slot_fails() {
        let (mut stage, _dir) = make_replay_stage();
        stage.replay_block(&make_block(1, 0), &[]).unwrap();
        let result = stage.replay_block(&make_block(1, 0), &[]);
        assert!(result.is_err());
    }
}
