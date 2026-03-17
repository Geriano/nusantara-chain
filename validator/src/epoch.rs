use std::sync::Arc;

use nusantara_consensus::bank::ConsensusBank;
use nusantara_core::EpochSchedule;
use nusantara_crypto::Hash;
use nusantara_rent_program::{Rent, RentDue};
use nusantara_storage::Storage;
use tracing::{info, warn};

use crate::constants::{MS_PER_YEAR, RENT_PARTITIONS};
use crate::helpers;
use crate::node::ValidatorNode;

impl ValidatorNode {
    pub(crate) async fn check_epoch_boundary(&mut self, snapshot_interval: u64) {
        let current_epoch = self.epoch_schedule.get_epoch(self.current_slot);
        let next_epoch = self.epoch_schedule.get_epoch(self.current_slot + 1);

        if next_epoch > current_epoch {
            // 0. Collect rent from accounts (blocking I/O — offloaded)
            {
                let storage = Arc::clone(&self.storage);
                let bank = Arc::clone(&self.bank);
                let rent = self.rent.clone();
                let epoch_schedule = self.epoch_schedule.clone();
                let current_slot = self.current_slot;
                let epoch = current_epoch;
                let _ = tokio::task::spawn_blocking(move || {
                    collect_rent_blocking(
                        &storage,
                        &bank,
                        &rent,
                        &epoch_schedule,
                        epoch,
                        current_slot,
                    );
                })
                .await;
            }

            // 1. Calculate and distribute rewards (blocking I/O — offloaded)
            {
                let storage = Arc::clone(&self.storage);
                let bank = Arc::clone(&self.bank);
                let current_slot = self.current_slot;
                let epoch = current_epoch;
                let _ = tokio::task::spawn_blocking(move || {
                    distribute_epoch_rewards_blocking(&storage, &bank, epoch, current_slot);
                })
                .await;
            }

            // 2. Process stake transitions (multi-epoch warmup/cooldown)
            self.process_stake_transitions(next_epoch);

            // 3. Update stake history sysvar
            let total_stake = self.bank.total_active_stake();
            self.bank.update_stake_history(
                current_epoch,
                nusantara_sysvar_program::StakeHistoryEntry {
                    effective: total_stake,
                    activating: 0,
                    deactivating: 0,
                },
            );

            // 4. Recalculate epoch stakes for next epoch
            self.bank.recalculate_epoch_stakes(next_epoch);

            // 5. Compute leader schedule for next epoch
            let stakes = self.bank.get_stake_distribution();
            if let Ok(schedule) = self.leader_schedule_generator.compute_schedule(
                next_epoch,
                &stakes,
                &self.genesis_hash,
            ) {
                self.replay_stage
                    .cache_leader_schedule(next_epoch, schedule.clone());
                self.leader_cache.write().insert(next_epoch, schedule);
            }

            info!(
                epoch = next_epoch,
                total_stake = self.bank.total_active_stake(),
                "epoch boundary crossed"
            );

            // 6. Create snapshot at epoch boundary if configured
            if snapshot_interval > 0 && next_epoch.is_multiple_of(snapshot_interval) {
                self.create_snapshot();
            }
        }
    }

    fn create_snapshot(&self) {
        use nusantara_storage::snapshot_archive;

        let bank_hash = self
            .bank
            .slot_hashes()
            .0
            .first()
            .map(|(_, h)| *h)
            .unwrap_or(Hash::zero());

        let timestamp = helpers::unix_timestamp_secs();
        let storage = Arc::clone(&self.storage);
        let current_slot = self.current_slot;
        let snapshot_dir = self.snapshot_dir.clone();

        // Snapshot creation reads from RocksDB (blocking I/O) — offload to
        // a blocking thread to avoid stalling the async slot loop.
        tokio::task::spawn_blocking(move || {
            match snapshot_archive::create_snapshot(&storage, current_slot, bank_hash, timestamp) {
                Ok(archive) => {
                    if std::fs::create_dir_all(&snapshot_dir).is_ok() {
                        let path = snapshot_dir.join(format!("snapshot-{current_slot}.bin"));
                        if let Err(e) = snapshot_archive::save_to_file(&archive, &path) {
                            tracing::warn!(error = %e, "failed to save snapshot file");
                        } else {
                            tracing::info!(
                                slot = current_slot,
                                accounts = archive.manifest.account_count,
                                path = %path.display(),
                                "snapshot created"
                            );
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "failed to create snapshot");
                    metrics::counter!("nusantara_snapshot_failures").increment(1);
                }
            }
        });
    }

    fn process_stake_transitions(&self, epoch: u64) {
        let delegations = self.bank.get_all_delegations();
        let rate_bps = nusantara_stake_program::DEFAULT_WARMUP_COOLDOWN_RATE_BPS;

        for (stake_account, delegation) in &delegations {
            // Remove fully cooled-down delegations (integer BPS arithmetic)
            if delegation.deactivation_epoch != u64::MAX {
                let epochs_deactivating = epoch.saturating_sub(delegation.deactivation_epoch);
                let cooldown_bps = epochs_deactivating.saturating_mul(rate_bps);
                if cooldown_bps >= 10_000 {
                    // Fully cooled down — remove delegation from bank
                    // The stake has been returned to the stake account via withdraw
                    self.bank.remove_stake_delegation(stake_account);
                }
            }
        }
    }
}

/// Freestanding rent collection to run in a blocking thread.
fn collect_rent_blocking(
    storage: &Storage,
    bank: &ConsensusBank,
    rent: &Rent,
    epoch_schedule: &EpochSchedule,
    epoch: u64,
    current_slot: u64,
) {
    let partition = epoch % RENT_PARTITIONS;
    let mut rent_collected = 0u64;
    let mut accounts_closed = 0u64;

    let ms_per_epoch =
        epoch_schedule.slots_per_epoch * nusantara_core::DEFAULT_SLOT_DURATION_MS;

    if let Ok(accounts) = storage.get_accounts_in_partition(partition, RENT_PARTITIONS) {
        for (address, mut account) in accounts {
            let rent_due = match rent.due_epoch(
                account.lamports,
                account.data.len(),
                ms_per_epoch,
                MS_PER_YEAR,
            ) {
                RentDue::Exempt => continue,
                RentDue::Paying(amount) => amount,
            };

            if rent_due == 0 {
                continue;
            }

            if account.lamports <= rent_due {
                rent_collected += account.lamports;
                account.lamports = 0;
                account.data.clear();
                accounts_closed += 1;
            } else {
                account.lamports -= rent_due;
                rent_collected += rent_due;
            }

            let _ = storage.put_account(&address, current_slot, &account);
        }
    }

    if rent_collected > 0 {
        bank.burn_fees(rent_collected);
        info!(
            epoch,
            partition, rent_collected, accounts_closed, "rent collected"
        );
    }
}

/// Freestanding reward distribution to run in a blocking thread.
fn distribute_epoch_rewards_blocking(
    storage: &Storage,
    bank: &ConsensusBank,
    epoch: u64,
    current_slot: u64,
) {
    use nusantara_consensus::rewards::RewardsCalculator;

    let vote_states = bank.get_all_vote_states();
    let delegations = bank.get_all_delegations();

    if delegations.is_empty() {
        return;
    }

    let total_supply = bank.total_supply();
    let inflation_rewards = RewardsCalculator::epoch_inflation_rewards(epoch, total_supply);

    match RewardsCalculator::calculate_epoch_rewards(
        epoch,
        inflation_rewards,
        &vote_states,
        &delegations,
    ) {
        Ok(rewards) => {
            let mut total_distributed = 0u64;
            for partition in &rewards.partitions {
                for entry in partition {
                    if let Ok(Some(mut account)) =
                        storage.get_account(&entry.stake_account)
                    {
                        account.lamports = account.lamports.saturating_add(entry.lamports);
                        if let Err(e) = storage.put_account(
                            &entry.stake_account,
                            current_slot,
                            &account,
                        ) {
                            warn!(error = %e, "failed to credit staker reward");
                        }
                        bank.update_delegation_stake(&entry.stake_account, account.lamports);
                    }
                    total_distributed += entry.lamports;

                    if entry.commission_lamports > 0 {
                        if let Ok(Some(mut vote_account)) =
                            storage.get_account(&entry.vote_account)
                        {
                            vote_account.lamports = vote_account
                                .lamports
                                .saturating_add(entry.commission_lamports);
                            if let Err(e) = storage.put_account(
                                &entry.vote_account,
                                current_slot,
                                &vote_account,
                            ) {
                                warn!(error = %e, "failed to credit commission");
                            }
                        }
                        total_distributed += entry.commission_lamports;
                    }
                }
            }

            bank.set_total_supply(total_supply.saturating_add(total_distributed));

            info!(
                epoch,
                total_rewards = total_distributed,
                "epoch rewards distributed"
            );
        }
        Err(e) => {
            warn!(epoch, error = %e, "failed to calculate epoch rewards");
        }
    }
}
