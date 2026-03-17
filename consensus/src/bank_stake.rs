use std::collections::HashMap;

use nusantara_crypto::Hash;
use nusantara_stake_program::Delegation;
use tracing::instrument;

use crate::bank::ConsensusBank;

impl ConsensusBank {
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
                // Still warming up — integer BPS arithmetic (deterministic)
                delegation.stake * delegation.warmup_cooldown_rate_bps / 10_000
            } else if delegation.deactivation_epoch == epoch {
                // Cooling down — integer BPS arithmetic (deterministic)
                delegation.stake * (10_000 - delegation.warmup_cooldown_rate_bps) / 10_000
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
}

#[cfg(test)]
mod tests {
    use crate::test_utils::test_helpers::temp_bank;

    #[test]
    fn stake_delegation_and_recalculate() {
        let (bank, _storage, _dir) = temp_bank();

        let voter = nusantara_crypto::hash(b"voter");

        for i in 0..5u64 {
            let acc = nusantara_crypto::hash(&i.to_le_bytes());
            bank.set_stake_delegation(
                acc,
                nusantara_stake_program::Delegation {
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
}
