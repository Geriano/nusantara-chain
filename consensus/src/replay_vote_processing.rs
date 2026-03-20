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

/// Extract a Vote and the voter's identity from a transaction.
///
/// The voter identity is `account_keys[0]` (fee payer), which is the validator's
/// identity address for vote transactions built by `build_vote_transaction`.
pub(crate) fn extract_vote_from_transaction(
    tx: &nusantara_core::transaction::Transaction,
) -> Option<(Hash, Vote)> {
    use nusantara_core::program::VOTE_PROGRAM_ID;

    let voter = *tx.message.account_keys.first()?;

    for ix in &tx.message.instructions {
        let program_id = tx.message.account_keys.get(ix.program_id_index as usize)?;
        if *program_id == *VOTE_PROGRAM_ID
            && let Ok(vote_ix) = borsh::from_slice::<VoteInstruction>(&ix.data)
        {
            match vote_ix {
                VoteInstruction::Vote(vote) => return Some((voter, vote)),
                VoteInstruction::SwitchVote(vote, _) => return Some((voter, vote)),
                _ => {}
            }
        }
    }
    None
}
