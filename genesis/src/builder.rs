use borsh::{BorshDeserialize, BorshSerialize};
use nusantara_core::program::{LOADER_PROGRAM_ID, STAKE_PROGRAM_ID, SYSTEM_PROGRAM_ID, VOTE_PROGRAM_ID};
use nusantara_core::{Account, Block, BlockHeader, EpochSchedule};
use nusantara_crypto::{Hash, Keypair, hashv};
use nusantara_rent_program::Rent;
use nusantara_stake_program::{
    Authorized, Delegation, Lockup, Meta, Stake, StakeStateV2,
    DEFAULT_WARMUP_COOLDOWN_RATE_BPS,
};
use nusantara_storage::cf::CF_DEFAULT;
use nusantara_storage::{SlotMeta, Storage};
use nusantara_sysvar_program::{
    Clock, EpochScheduleSysvar, RecentBlockhashes, RentSysvar, SlotHashes, StakeHistory,
};
use nusantara_vote_program::{VoteInit, VoteState};
use tracing::{info, instrument};

use crate::config::GenesisConfig;
use crate::error::GenesisError;

pub const GENESIS_HASH_KEY: &[u8] = b"genesis_hash";
pub const VALIDATORS_KEY: &[u8] = b"genesis_validators";
pub const FAUCET_KEYPAIR_KEY: &[u8] = b"genesis_faucet_keypair";

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct GenesisValidatorInfo {
    pub identity: Hash,
    pub vote_account: Hash,
    pub stake_account: Hash,
    pub stake_lamports: u64,
    pub commission: u8,
}

#[derive(Debug)]
pub struct GenesisResult {
    pub genesis_hash: Hash,
    pub cluster_name: String,
    pub creation_time: i64,
    pub validator_count: usize,
    pub total_stake: u64,
    pub total_supply: u64,
    pub epoch_schedule_slots: u64,
}

pub struct GenesisBuilder<'a> {
    config: &'a GenesisConfig,
    storage: &'a Storage,
}

impl<'a> GenesisBuilder<'a> {
    pub fn new(config: &'a GenesisConfig, storage: &'a Storage) -> Self {
        Self { config, storage }
    }

    #[instrument(skip_all)]
    pub fn build(&self) -> Result<GenesisResult, GenesisError> {
        // Step 1: Check idempotency
        if self.storage.get_cf(CF_DEFAULT, GENESIS_HASH_KEY)?.is_some() {
            return Err(GenesisError::AlreadyInitialized(
                "genesis already initialized".to_string(),
            ));
        }

        if self.config.validators.is_empty() {
            return Err(GenesisError::NoValidators);
        }

        let creation_time = self.config.cluster.creation_time;
        let rent = Rent {
            lamports_per_byte_year: self.config.rent.lamports_per_byte_year,
            exemption_threshold: self.config.rent.exemption_threshold,
            burn_percent: self.config.rent.burn_percent,
        };
        let mut total_supply: u128 = 0;
        let mut total_stake: u64 = 0;
        let mut validator_infos = Vec::new();

        // Step 2: Fund initial accounts
        for acc_cfg in &self.config.accounts {
            let addr = GenesisConfig::resolve_address(&acc_cfg.address, b"")?;
            let account = Account::new(acc_cfg.lamports, *SYSTEM_PROGRAM_ID);
            self.storage.put_account(&addr, 0, &account)?;
            total_supply = total_supply
                .checked_add(acc_cfg.lamports as u128)
                .ok_or(GenesisError::SupplyOverflow)?;
            info!(address = %addr.to_base64(), lamports = acc_cfg.lamports, "funded account");
        }

        // Step 3: Fund faucet
        if let Some(faucet) = &self.config.faucet {
            let addr = if faucet.address == "generate" {
                let keypair = Keypair::generate();
                let addr = keypair.address();
                // Persist faucet keypair so validators can load it at boot
                let mut keypair_bytes = Vec::with_capacity(
                    keypair.public_key().as_bytes().len()
                        + keypair.secret_key().as_bytes().len(),
                );
                keypair_bytes.extend_from_slice(keypair.public_key().as_bytes());
                keypair_bytes.extend_from_slice(keypair.secret_key().as_bytes());
                self.storage
                    .put_cf(CF_DEFAULT, FAUCET_KEYPAIR_KEY, &keypair_bytes)?;
                info!("persisted faucet keypair to storage");
                addr
            } else {
                GenesisConfig::resolve_address(&faucet.address, b"")?
            };
            let account = Account::new(faucet.lamports, *SYSTEM_PROGRAM_ID);
            self.storage.put_account(&addr, 0, &account)?;
            total_supply = total_supply
                .checked_add(faucet.lamports as u128)
                .ok_or(GenesisError::SupplyOverflow)?;
            info!(address = %addr.to_base64(), lamports = faucet.lamports, "funded faucet");
        }

        // Step 4: Create validators
        for v_cfg in &self.config.validators {
            let identity = GenesisConfig::resolve_address(&v_cfg.identity, b"")?;

            let vote_seed = [identity.as_bytes().as_slice(), b"vote"].concat();
            let vote_addr =
                GenesisConfig::resolve_address(&v_cfg.vote_account, &vote_seed)?;

            let stake_addr = hashv(&[identity.as_bytes(), b"stake"]);

            // Create vote account
            let vote_init = VoteInit {
                node_pubkey: identity,
                authorized_voter: identity,
                authorized_withdrawer: identity,
                commission: v_cfg.commission,
            };
            let vote_state = VoteState::new(&vote_init);
            let vote_data = borsh::to_vec(&vote_state)
                .map_err(|e| GenesisError::Serialization(e.to_string()))?;
            let vote_rent = rent.minimum_balance(vote_data.len());
            let mut vote_account = Account::new(vote_rent, *VOTE_PROGRAM_ID);
            vote_account.data = vote_data;
            self.storage.put_account(&vote_addr, 0, &vote_account)?;

            // Create stake account — serialize placeholder to get data size
            let placeholder_state = StakeStateV2::Stake(
                Meta {
                    rent_exempt_reserve: 0,
                    authorized: Authorized {
                        staker: identity,
                        withdrawer: identity,
                    },
                    lockup: Lockup {
                        unix_timestamp: 0,
                        epoch: 0,
                        custodian: Hash::zero(),
                    },
                },
                Stake {
                    delegation: Delegation {
                        voter_pubkey: vote_addr,
                        stake: v_cfg.stake_lamports,
                        activation_epoch: 0,
                        deactivation_epoch: u64::MAX,
                        warmup_cooldown_rate_bps: DEFAULT_WARMUP_COOLDOWN_RATE_BPS,
                    },
                    credits_observed: 0,
                },
            );
            let placeholder_data = borsh::to_vec(&placeholder_state)
                .map_err(|e| GenesisError::Serialization(e.to_string()))?;
            let stake_rent = rent.minimum_balance(placeholder_data.len());

            // Re-serialize with correct rent_exempt_reserve
            let stake_state = StakeStateV2::Stake(
                Meta {
                    rent_exempt_reserve: stake_rent,
                    authorized: Authorized {
                        staker: identity,
                        withdrawer: identity,
                    },
                    lockup: Lockup {
                        unix_timestamp: 0,
                        epoch: 0,
                        custodian: Hash::zero(),
                    },
                },
                Stake {
                    delegation: Delegation {
                        voter_pubkey: vote_addr,
                        stake: v_cfg.stake_lamports,
                        activation_epoch: 0,
                        deactivation_epoch: u64::MAX,
                        warmup_cooldown_rate_bps: DEFAULT_WARMUP_COOLDOWN_RATE_BPS,
                    },
                    credits_observed: 0,
                },
            );
            let stake_data = borsh::to_vec(&stake_state)
                .map_err(|e| GenesisError::Serialization(e.to_string()))?;
            let mut stake_account =
                Account::new(v_cfg.stake_lamports + stake_rent, *STAKE_PROGRAM_ID);
            stake_account.data = stake_data;
            self.storage
                .put_account(&stake_addr, 0, &stake_account)?;

            // Fund identity account: rent-exempt minimum + 10 NUSA for vote tx fees.
            // Each vote costs lamports_per_signature (5000) per slot.
            // 10 NUSA = 10_000_000_000 lamports ≈ 2M vote fees.
            //
            // If the identity address was already funded (e.g. as the faucet), add to
            // the existing balance instead of overwriting it.
            let identity_lamports = rent.minimum_balance(0) + 10_000_000_000;
            let identity_account = if let Some(existing) = self.storage.get_account(&identity)? {
                Account::new(existing.lamports + identity_lamports, *SYSTEM_PROGRAM_ID)
            } else {
                Account::new(identity_lamports, *SYSTEM_PROGRAM_ID)
            };
            self.storage.put_account(&identity, 0, &identity_account)?;

            let validator_lamports =
                v_cfg.stake_lamports as u128 + stake_rent as u128 + vote_rent as u128 + identity_lamports as u128;
            total_stake += v_cfg.stake_lamports;
            total_supply = total_supply
                .checked_add(validator_lamports)
                .ok_or(GenesisError::SupplyOverflow)?;

            validator_infos.push(GenesisValidatorInfo {
                identity,
                vote_account: vote_addr,
                stake_account: stake_addr,
                stake_lamports: v_cfg.stake_lamports,
                commission: v_cfg.commission,
            });

            info!(
                identity = %identity.to_base64(),
                vote = %vote_addr.to_base64(),
                stake = %stake_addr.to_base64(),
                stake_lamports = v_cfg.stake_lamports,
                "created validator"
            );
        }

        // Store validator info for validator boot
        let validators_data = borsh::to_vec(&validator_infos)
            .map_err(|e| GenesisError::Serialization(e.to_string()))?;
        self.storage
            .put_cf(CF_DEFAULT, VALIDATORS_KEY, &validators_data)?;

        // Step 5: Write sysvars
        let epoch_schedule = EpochSchedule::new(self.config.epoch.slots_per_epoch);

        let clock = Clock {
            slot: 0,
            epoch: 0,
            epoch_start_timestamp: creation_time,
            leader_schedule_epoch: 1,
            unix_timestamp: creation_time,
        };
        self.storage.put_sysvar(&clock)?;
        self.storage.put_sysvar(&RentSysvar(rent))?;
        self.storage
            .put_sysvar(&EpochScheduleSysvar(epoch_schedule.clone()))?;
        self.storage.put_sysvar(&SlotHashes::default())?;
        self.storage.put_sysvar(&StakeHistory::default())?;
        self.storage.put_sysvar(&RecentBlockhashes::default())?;

        // Step 6: Register native program accounts
        {
            let loader_id = *LOADER_PROGRAM_ID;
            let mut loader_account = Account::new(1, loader_id);
            loader_account.executable = true;
            self.storage.put_account(&loader_id, 0, &loader_account)?;
            info!(address = %loader_id.to_base64(), "registered loader program");
        }

        // Step 7: Create genesis block
        let genesis_hash = hashv(&[
            b"nusantara_genesis",
            self.config.cluster.name.as_bytes(),
            &creation_time.to_le_bytes(),
        ]);

        let bank_hash = hashv(&[Hash::zero().as_bytes(), genesis_hash.as_bytes()]);
        let block = Block {
            header: BlockHeader {
                slot: 0,
                parent_slot: 0,
                parent_hash: Hash::zero(),
                block_hash: genesis_hash,
                timestamp: creation_time,
                validator: Hash::zero(),
                transaction_count: 0,
                merkle_root: Hash::zero(),
                poh_hash: Hash::zero(),
                bank_hash,
                state_root: Hash::zero(),
            },
            transactions: Vec::new(),
            batches: Vec::new(),
        };
        self.storage.put_block(&block)?;

        // Step 8: Store slot metadata
        let slot_meta = SlotMeta {
            slot: 0,
            parent_slot: 0,
            block_time: Some(creation_time),
            num_data_shreds: 0,
            num_code_shreds: 0,
            is_connected: true,
            completed: true,
        };
        self.storage.put_slot_meta(&slot_meta)?;

        // Step 9: Set root and hashes
        let bank_hash = hashv(&[Hash::zero().as_bytes(), genesis_hash.as_bytes()]);
        self.storage.set_root(0)?;
        self.storage.put_bank_hash(0, &bank_hash)?;
        self.storage.put_slot_hash(0, &genesis_hash)?;

        // Step 10: Store genesis marker
        self.storage
            .put_cf(CF_DEFAULT, GENESIS_HASH_KEY, genesis_hash.as_bytes())?;

        let total_supply_u64 =
            u64::try_from(total_supply).map_err(|_| GenesisError::SupplyOverflow)?;

        metrics::counter!("nusantara_genesis_initialized").increment(1);

        info!(
            genesis_hash = %genesis_hash.to_base64(),
            cluster = %self.config.cluster.name,
            validators = self.config.validators.len(),
            total_stake,
            total_supply = total_supply_u64,
            "genesis initialized"
        );

        Ok(GenesisResult {
            genesis_hash,
            cluster_name: self.config.cluster.name.clone(),
            creation_time,
            validator_count: self.config.validators.len(),
            total_stake,
            total_supply: total_supply_u64,
            epoch_schedule_slots: self.config.epoch.slots_per_epoch,
        })
    }
}
