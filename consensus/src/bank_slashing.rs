use nusantara_crypto::Hash;

use crate::bank::ConsensusBank;

impl ConsensusBank {
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
}

#[cfg(test)]
mod tests {
    use nusantara_stake_program::Delegation;

    use crate::test_utils::test_helpers::temp_bank;

    #[test]
    fn slash_reduces_effective_stake() {
        let (bank, _storage, _dir) = temp_bank();

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
        let (bank, _storage, _dir) = temp_bank();

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
