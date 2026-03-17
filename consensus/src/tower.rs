use nusantara_core::native_token::const_parse_u64;
use nusantara_crypto::Hash;
use nusantara_vote_program::{Lockout, Vote, VoteState};
use tracing::instrument;

use crate::error::ConsensusError;

pub const VOTE_THRESHOLD_DEPTH: u64 = const_parse_u64(env!("NUSA_TOWER_VOTE_THRESHOLD_DEPTH"));
pub const VOTE_THRESHOLD_PERCENTAGE: u64 =
    const_parse_u64(env!("NUSA_TOWER_VOTE_THRESHOLD_PERCENTAGE"));
pub const SWITCH_THRESHOLD_PERCENTAGE: u64 =
    const_parse_u64(env!("NUSA_TOWER_SWITCH_THRESHOLD_PERCENTAGE"));
pub const MAX_LOCKOUT_HISTORY: u64 = const_parse_u64(env!("NUSA_TOWER_MAX_LOCKOUT_HISTORY"));

pub struct TowerVoteResult {
    pub new_root_slot: Option<u64>,
    pub expired_lockouts: Vec<Lockout>,
    pub updated_vote_state: VoteState,
}

pub struct Tower {
    vote_state: VoteState,
}

impl Tower {
    pub fn new(vote_state: VoteState) -> Self {
        Self { vote_state }
    }

    pub fn vote_state(&self) -> &VoteState {
        &self.vote_state
    }

    pub fn root_slot(&self) -> Option<u64> {
        self.vote_state.root_slot
    }

    /// Process a vote, enforcing Tower BFT lockout rules.
    ///
    /// 1. Expire lockouts where slot + 2^confirmation_count <= vote.slot
    /// 2. Push new lockout at vote slot with confirmation_count=1
    /// 3. Increment confirmation_count on votes that voted for an ancestor
    /// 4. If bottom vote reaches MAX_LOCKOUT_HISTORY confirmations -> becomes root
    #[instrument(skip(self, vote), level = "debug")]
    pub fn process_vote(&mut self, vote: &Vote) -> Result<TowerVoteResult, ConsensusError> {
        let vote_slot = *vote
            .slots
            .last()
            .ok_or(ConsensusError::VoteTooOld {
                vote_slot: 0,
                root_slot: self.vote_state.root_slot.unwrap_or(0),
            })?;

        // Check vote is not at or before root
        if let Some(root) = self.vote_state.root_slot
            && vote_slot <= root
        {
            return Err(ConsensusError::VoteTooOld {
                vote_slot,
                root_slot: root,
            });
        }

        // Check lockout constraints
        self.check_vote_lockout(vote_slot)?;

        // 1. Expire old lockouts
        let mut expired = Vec::new();
        self.vote_state.votes.retain(|lockout| {
            if !lockout.is_locked_out_at_slot(vote_slot) {
                expired.push(lockout.clone());
                false
            } else {
                true
            }
        });

        // 2. Push new lockout
        self.vote_state.votes.push(Lockout {
            slot: vote_slot,
            confirmation_count: 1,
        });

        // 3. Increment confirmation_count on remaining votes
        // (All existing votes that are still active get their confirmation bumped)
        let len = self.vote_state.votes.len();
        if len > 1 {
            for i in 0..len - 1 {
                self.vote_state.votes[i].confirmation_count += 1;
            }
        }

        // 4. Check if bottom vote reached MAX_LOCKOUT_HISTORY -> becomes root
        let mut new_root = None;
        let root_count = self.vote_state.votes.iter()
            .take_while(|v| v.confirmation_count >= MAX_LOCKOUT_HISTORY as u32)
            .count();
        if root_count > 0 {
            let last_rooted = self.vote_state.votes.drain(..root_count).next_back().unwrap();
            new_root = Some(last_rooted.slot);
            self.vote_state.root_slot = Some(last_rooted.slot);
        }

        metrics::counter!("nusantara_tower_votes_processed_total").increment(1);
        if new_root.is_some() {
            metrics::counter!("nusantara_tower_roots_advanced_total").increment(1);
        }

        Ok(TowerVoteResult {
            new_root_slot: new_root,
            expired_lockouts: expired,
            updated_vote_state: self.vote_state.clone(),
        })
    }

    /// Verify no lockout is violated for the given vote slot.
    pub fn check_vote_lockout(&self, slot: u64) -> Result<(), ConsensusError> {
        for lockout in &self.vote_state.votes {
            if lockout.is_locked_out_at_slot(slot) && slot < lockout.slot {
                return Err(ConsensusError::LockoutViolation {
                    vote_slot: slot,
                    locked_slot: lockout.slot,
                });
            }
        }
        Ok(())
    }

    /// Check switch threshold: is there enough stake on the alternative fork?
    /// Returns true if the alternative fork has >= SWITCH_THRESHOLD_PERCENTAGE of total stake.
    pub fn check_switch_threshold(
        &self,
        _switch_slot: u64,
        voted_stakes: &[(Hash, u64)],
        total_stake: u64,
    ) -> bool {
        if total_stake == 0 {
            return false;
        }

        // Sum all stake on the alternative fork
        let alternative_stake: u64 = voted_stakes.iter().map(|(_, s)| *s).sum();
        let pct = alternative_stake * 100 / total_stake;
        pct >= SWITCH_THRESHOLD_PERCENTAGE
    }

    /// Get the depth of the current tower (number of active lockouts).
    pub fn depth(&self) -> usize {
        self.vote_state.votes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nusantara_crypto::hash;
    use nusantara_vote_program::VoteInit;

    fn make_tower() -> Tower {
        let init = VoteInit {
            node_pubkey: hash(b"node"),
            authorized_voter: hash(b"voter"),
            authorized_withdrawer: hash(b"withdrawer"),
            commission: 10,
        };
        Tower::new(VoteState::new(&init))
    }

    fn make_vote(slot: u64) -> Vote {
        Vote {
            slots: vec![slot],
            hash: hash(slot.to_le_bytes().as_ref()),
            timestamp: None,
        }
    }

    #[test]
    fn config_values() {
        assert_eq!(VOTE_THRESHOLD_DEPTH, 8);
        assert_eq!(VOTE_THRESHOLD_PERCENTAGE, 66);
        assert_eq!(SWITCH_THRESHOLD_PERCENTAGE, 38);
        assert_eq!(MAX_LOCKOUT_HISTORY, 31);
    }

    #[test]
    fn single_vote() {
        let mut tower = make_tower();
        let result = tower.process_vote(&make_vote(1)).unwrap();
        assert!(result.new_root_slot.is_none());
        assert_eq!(tower.depth(), 1);
        assert_eq!(tower.vote_state().votes[0].slot, 1);
    }

    #[test]
    fn sequential_votes_build_tower() {
        let mut tower = make_tower();
        for slot in 1..=10 {
            tower.process_vote(&make_vote(slot)).unwrap();
        }
        assert_eq!(tower.depth(), 10);
        // First vote should have confirmation_count = 10
        assert_eq!(tower.vote_state().votes[0].confirmation_count, 10);
    }

    #[test]
    fn root_advancement() {
        let mut tower = make_tower();
        let mut last_result = None;

        // Need MAX_LOCKOUT_HISTORY sequential votes for root to advance
        for slot in 1..=MAX_LOCKOUT_HISTORY {
            let result = tower.process_vote(&make_vote(slot)).unwrap();
            last_result = Some(result);
        }

        // After MAX_LOCKOUT_HISTORY votes, the first vote should become root
        let result = last_result.unwrap();
        assert_eq!(result.new_root_slot, Some(1));
        assert_eq!(tower.root_slot(), Some(1));
    }

    #[test]
    fn vote_too_old() {
        let mut tower = make_tower();
        // Set root slot to 10
        for slot in 1..=MAX_LOCKOUT_HISTORY + 10 {
            tower.process_vote(&make_vote(slot)).unwrap();
        }

        // Try to vote on a slot before root
        let result = tower.process_vote(&make_vote(1));
        assert!(result.is_err());
    }

    #[test]
    fn expired_lockouts() {
        let mut tower = make_tower();
        tower.process_vote(&make_vote(1)).unwrap();

        // Vote at a far future slot -> lockout at slot 1 (lockout = 2^1 = 2) expires
        // Slot 1 locked until slot 1+2 = 3, so voting at 4 should expire it
        let result = tower.process_vote(&make_vote(100)).unwrap();
        assert!(!result.expired_lockouts.is_empty());
        assert_eq!(result.expired_lockouts[0].slot, 1);
    }

    #[test]
    fn switch_threshold() {
        let tower = make_tower();
        let stakes = vec![(hash(b"v1"), 40)];
        assert!(tower.check_switch_threshold(10, &stakes, 100));

        let stakes = vec![(hash(b"v1"), 30)];
        assert!(!tower.check_switch_threshold(10, &stakes, 100));
    }

    #[test]
    fn lockout_violation() {
        let mut tower = make_tower();
        tower.process_vote(&make_vote(10)).unwrap();

        // Trying to vote on slot 5 while locked out at 10 should fail
        let result = tower.process_vote(&make_vote(5));
        assert!(result.is_err());
    }
}
