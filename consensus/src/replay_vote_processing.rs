use nusantara_crypto::Hash;
use nusantara_vote_program::{Vote, VoteInstruction};

use crate::replay_stage::ReplayStage;

impl ReplayStage {
    /// Process a gossip vote from a peer validator.
    pub fn process_gossip_vote(&mut self, _voter: Hash, slot: u64, hash: Hash, stake: u64) {
        self.fork_tree.add_vote(slot, stake);
        if self.fork_tree.contains(slot) {
            self.commitment_tracker.record_vote(slot, hash, stake);
        }
    }
}

/// Extract a Vote from a transaction by looking for vote program instructions.
pub(crate) fn extract_vote_from_transaction(
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
