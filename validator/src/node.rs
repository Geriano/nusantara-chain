use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use borsh::BorshDeserialize;
use nusantara_consensus::bank::ConsensusBank;
use nusantara_consensus::commitment::CommitmentTracker;
use nusantara_consensus::fork_choice::ForkTree;
use nusantara_consensus::gpu::GpuPohVerifier;
use nusantara_consensus::leader_schedule::{LeaderSchedule, LeaderScheduleGenerator};
use nusantara_consensus::replay_stage::ReplayStage;
use nusantara_consensus::tower::Tower;
use nusantara_core::block::Block;
use nusantara_core::epoch::EpochSchedule;
use nusantara_core::{DEFAULT_SLOT_DURATION_MS, FeeCalculator, Transaction};
use nusantara_crypto::{Hash, Keypair};
use nusantara_genesis::{
    GENESIS_HASH_KEY, GenesisBuilder, GenesisConfig, GenesisValidatorInfo, VALIDATORS_KEY,
};
use nusantara_gossip::{ClusterInfo, ContactInfo, GossipService};
use nusantara_mempool::Mempool;
use nusantara_rpc::{PubsubEvent, RpcServer, RpcState, RpcTlsConfig};
use nusantara_runtime::ProgramCache;
use nusantara_stake_program::Delegation;
use nusantara_storage::Storage;
use nusantara_storage::cf::CF_DEFAULT;
use nusantara_sysvar_program::{Clock, EpochScheduleSysvar, RentSysvar, SlotHashes};
use nusantara_tpu_forward::TpuService;
use nusantara_turbine::turbine_tree::TURBINE_FANOUT;
use nusantara_turbine::protocol::{RepairRequest, MAX_UDP_PACKET};
use nusantara_turbine::{
    BatchRepairResponse, BroadcastStage, RepairService, RetransmitStage, ShredCollector,
    ShredReceiver, Shredder, SignedShred, TurbineMessage, TurbineTree,
};
use nusantara_vote_program::{Vote, VoteInit, VoteState};
use tokio::net::UdpSocket;
use tokio::sync::{broadcast, mpsc, watch};
use tracing::{debug, info, warn};

use crate::block_producer::BlockProducer;
use crate::cli::Cli;
use crate::error::ValidatorError;
use crate::slot_clock::SlotClock;
use crate::vote_tx::build_vote_transaction;

const KEYPAIR_SIZE: usize = 1952 + 4032; // pubkey + secret

/// Maximum age (in slots) for orphan blocks and fork branches before they are
/// considered stale. Orphans older than this are evicted and no longer block
/// root advancement. 32 slots = 28.8s at 900ms/slot.
const ORPHAN_HORIZON: u64 = 32;

type SharedLeaderCache = Arc<parking_lot::RwLock<HashMap<u64, LeaderSchedule>>>;

pub struct ValidatorNode {
    // Identity
    keypair: Arc<Keypair>,
    identity: Hash,

    // Storage & Consensus
    storage: Arc<Storage>,
    bank: Arc<ConsensusBank>,
    block_producer: BlockProducer,

    // Transactions
    mempool: Arc<Mempool>,

    // Timing
    slot_clock: SlotClock,
    current_slot: u64,

    // Networking
    cluster_info: Arc<ClusterInfo>,

    // Consensus engine
    replay_stage: ReplayStage,

    // Leader schedule
    leader_cache: SharedLeaderCache,
    leader_schedule_generator: LeaderScheduleGenerator,
    epoch_schedule: EpochSchedule,
    genesis_hash: Hash,

    // Vote account
    my_vote_account: Option<Hash>,

    // Network addresses
    gossip_addr: SocketAddr,
    turbine_addr: SocketAddr,
    repair_addr: SocketAddr,
    tpu_addr: SocketAddr,
    #[allow(dead_code)]
    tpu_forward_addr: SocketAddr,

    // Skip tracking (F1/F5)
    consecutive_skips: Arc<AtomicU64>,
    total_skips: u64,

    // Gossip vote cursor (F4)
    gossip_vote_cursor: u64,

    // Slash detection (F3)
    slash_detector: nusantara_consensus::SlashDetector,

    // Fee/rent for block replay (F2)
    fee_calculator: FeeCalculator,
    rent: nusantara_rent_program::Rent,

    // WASM program cache
    program_cache: Arc<ProgramCache>,

    // WebSocket pubsub broadcast channel
    pubsub_tx: broadcast::Sender<PubsubEvent>,

    // Orphan block buffer (blocks whose parents haven't arrived yet)
    orphan_blocks: BTreeMap<u64, Block>,

    // Shared shred collector for requesting repair
    shred_collector: Arc<ShredCollector>,

    // Track fork switch targets that have failed to prevent infinite retry.
    // Cleared when root advances (fork landscape changes).
    failed_fork_targets: HashSet<u64>,

    // Last slot we submitted a vote for (used to batch unvoted slots)
    last_voted_slot: u64,
}

impl ValidatorNode {
    pub fn boot(cli: &Cli) -> Result<Self, ValidatorError> {
        // 1. Open storage
        let storage_path = Path::new(&cli.ledger_path);
        let storage = Arc::new(Storage::open(storage_path)?);
        info!(path = %cli.ledger_path, "storage opened");

        // 2. Load or generate identity keypair
        let keypair = Arc::new(Self::load_or_generate_keypair(cli)?);
        let identity_address = keypair.address();
        info!(identity = %identity_address.to_base64(), "identity loaded");

        // 2b. Attempt snapshot restore before genesis
        //     If ledger has no genesis marker but a snapshot file exists,
        //     restore state from the snapshot to skip full replay.
        let snapshot_dir = Path::new(&cli.ledger_path).join("snapshots");
        if storage.get_cf(CF_DEFAULT, GENESIS_HASH_KEY)?.is_none()
            && let Some(snapshot_path) =
                nusantara_storage::snapshot_archive::find_latest_snapshot_file(&snapshot_dir)
        {
            info!(
                path = %snapshot_path.display(),
                "found snapshot file, restoring state"
            );
            let archive = nusantara_storage::snapshot_archive::load_from_file(&snapshot_path)?;
            nusantara_storage::snapshot_archive::restore_snapshot(&storage, &archive)?;
            info!(
                slot = archive.manifest.slot,
                accounts = archive.manifest.account_count,
                "state restored from snapshot"
            );
        }

        // 3. Ensure genesis is applied
        if storage.get_cf(CF_DEFAULT, GENESIS_HASH_KEY)?.is_none() {
            let genesis_path = cli
                .genesis_config
                .as_deref()
                .ok_or(ValidatorError::NoGenesis)?;
            info!(path = genesis_path, "applying genesis config");
            let mut config = GenesisConfig::load(genesis_path)?;

            // Bind genesis validators to keypairs:
            // - First "generate" validator ??? this node's keypair
            // - Subsequent "generate" validators ??? extra keypair files (--extra-validator-keys)
            let mut extra_idx = 0;
            for (i, validator) in config.validators.iter_mut().enumerate() {
                if validator.identity == "generate" {
                    if i == 0 {
                        validator.identity = identity_address.to_base64();
                        info!("bound genesis validator[0] identity to this node's keypair");
                    } else if extra_idx < cli.extra_validator_keys.len() {
                        let extra_kp =
                            Self::load_keypair_from_path(&cli.extra_validator_keys[extra_idx])?;
                        validator.identity = extra_kp.address().to_base64();
                        info!(
                            validator_index = i,
                            path = %cli.extra_validator_keys[extra_idx],
                            "bound genesis validator identity from extra keypair"
                        );
                        extra_idx += 1;
                    } else {
                        // Auto-generate a keypair for this validator
                        let auto_kp = Keypair::generate();
                        validator.identity = auto_kp.address().to_base64();
                        info!(
                            validator_index = i,
                            "auto-generated keypair for genesis validator"
                        );
                    }
                }
            }

            let builder = GenesisBuilder::new(&config, &storage);
            let result = builder.build()?;
            info!(
                genesis_hash = %result.genesis_hash.to_base64(),
                validators = result.validator_count,
                total_stake = result.total_stake,
                total_supply = result.total_supply,
                "genesis applied"
            );
        } else {
            info!("existing genesis found in storage");
        }

        // 4. Load sysvars from storage
        let clock: Clock = storage
            .get_sysvar::<Clock>()?
            .ok_or(ValidatorError::NoGenesis)?;
        let rent_sysvar: RentSysvar = storage
            .get_sysvar::<RentSysvar>()?
            .ok_or(ValidatorError::NoGenesis)?;
        let epoch_sysvar: EpochScheduleSysvar = storage
            .get_sysvar::<EpochScheduleSysvar>()?
            .ok_or(ValidatorError::NoGenesis)?;

        let epoch_schedule = epoch_sysvar.0;
        let rent = rent_sysvar.0;
        let fee_calculator = FeeCalculator::default();

        // 5. Determine last root slot and hashes
        let last_root = storage.get_latest_root()?.unwrap_or(0);
        let genesis_hash_bytes = storage
            .get_cf(CF_DEFAULT, GENESIS_HASH_KEY)?
            .ok_or(ValidatorError::NoGenesis)?;
        let genesis_hash = Hash::new(
            genesis_hash_bytes
                .try_into()
                .map_err(|_| ValidatorError::Keypair("invalid genesis hash".to_string()))?,
        );

        let parent_hash = storage.get_slot_hash(last_root)?.unwrap_or(genesis_hash);
        let parent_bank_hash = storage
            .get_bank_hash(last_root)?
            .unwrap_or_else(|| hashv_bank_genesis(&genesis_hash));

        info!(
            last_root,
            parent_hash = %parent_hash.to_base64(),
            "loaded chain state"
        );

        // 6. Create ConsensusBank
        let bank = Arc::new(ConsensusBank::new(
            Arc::clone(&storage),
            epoch_schedule.clone(),
        ));

        // Advance bank to last root state
        bank.advance_slot(last_root, clock.unix_timestamp);
        // Seed slot hashes with genesis hash
        bank.record_slot_hash(0, genesis_hash);
        if last_root > 0 {
            bank.record_slot_hash(last_root, parent_hash);
        }

        // 7-8. Load genesis validators and register in bank
        let mut validators: Vec<GenesisValidatorInfo> = Vec::new();
        if let Some(validators_data) = storage.get_cf(CF_DEFAULT, VALIDATORS_KEY)? {
            validators = BorshDeserialize::deserialize(&mut validators_data.as_slice())
                .map_err(|e| ValidatorError::Keypair(format!("failed to load validators: {e}")))?;

            for v in &validators {
                // Load vote account from storage
                if let Some(vote_account) = storage.get_account(&v.vote_account)? {
                    let vote_state: VoteState = BorshDeserialize::deserialize(
                        &mut vote_account.data.as_slice(),
                    )
                    .map_err(|e| {
                        ValidatorError::Keypair(format!("failed to deserialize vote state: {e}"))
                    })?;
                    bank.set_vote_state(v.vote_account, vote_state);
                }

                // Register stake delegation
                let delegation = Delegation {
                    voter_pubkey: v.vote_account,
                    stake: v.stake_lamports,
                    activation_epoch: 0,
                    deactivation_epoch: u64::MAX,
                    warmup_cooldown_rate_bps:
                        nusantara_stake_program::DEFAULT_WARMUP_COOLDOWN_RATE_BPS,
                };
                bank.set_stake_delegation(v.stake_account, delegation);
            }

            info!(
                count = validators.len(),
                "loaded genesis validators into bank"
            );
        } else {
            warn!("no validator info found in storage");
        }

        // 9. Recalculate epoch stakes
        let current_epoch = epoch_schedule.get_epoch(last_root);
        bank.recalculate_epoch_stakes(current_epoch);
        info!(
            epoch = current_epoch,
            total_stake = bank.total_active_stake(),
            "epoch stakes calculated"
        );

        // 9b. Initialize state Merkle tree from all accounts in storage
        let state_tree = nusantara_consensus::StateTree::init_from_storage(&storage)?;
        info!(
            leaves = state_tree.len(),
            "state tree initialized from storage"
        );
        bank.set_state_tree(state_tree);

        // 10. Create SlotClock
        let slot_clock = SlotClock::new(clock.epoch_start_timestamp, DEFAULT_SLOT_DURATION_MS);
        let current_slot = slot_clock.current_slot().max(last_root + 1);

        // 11. Create ProgramCache
        let program_cache = Arc::new(ProgramCache::new(256));

        // 12. Create BlockProducer
        let block_producer = BlockProducer::new(
            identity_address,
            Arc::clone(&storage),
            Arc::clone(&bank),
            parent_hash,
            epoch_schedule.clone(),
            fee_calculator.clone(),
            rent.clone(),
            last_root,
            parent_hash,
            parent_bank_hash,
            Arc::clone(&program_cache),
        );

        // 12. Create mempool
        let mempool = Arc::new(Mempool::new(
            nusantara_mempool::pool::DEFAULT_MAX_SIZE as usize,
        ));

        // 13. Parse network addresses
        let gossip_addr: SocketAddr = cli
            .gossip_addr
            .parse()
            .map_err(|e| ValidatorError::NetworkInit(format!("invalid gossip addr: {e}")))?;
        let turbine_addr: SocketAddr = cli
            .turbine_addr
            .parse()
            .map_err(|e| ValidatorError::NetworkInit(format!("invalid turbine addr: {e}")))?;
        let repair_addr: SocketAddr = cli
            .repair_addr
            .parse()
            .map_err(|e| ValidatorError::NetworkInit(format!("invalid repair addr: {e}")))?;
        let tpu_addr: SocketAddr = cli
            .tpu_addr
            .parse()
            .map_err(|e| ValidatorError::NetworkInit(format!("invalid tpu addr: {e}")))?;
        let tpu_forward_addr: SocketAddr = cli
            .tpu_forward_addr
            .parse()
            .map_err(|e| ValidatorError::NetworkInit(format!("invalid tpu forward addr: {e}")))?;

        // 14. Build ContactInfo
        // When --public-host is set, replace 0.0.0.0 bind IPs with the resolved public IP
        let (adv_gossip, adv_tpu, adv_tpu_fwd, adv_turbine, adv_repair) =
            if let Some(ref host) = cli.public_host {
                let public_ip = Self::resolve_public_host(host)?;
                info!(host, ip = %public_ip, "resolved public host");
                (
                    SocketAddr::new(public_ip, gossip_addr.port()),
                    SocketAddr::new(public_ip, tpu_addr.port()),
                    SocketAddr::new(public_ip, tpu_forward_addr.port()),
                    SocketAddr::new(public_ip, turbine_addr.port()),
                    SocketAddr::new(public_ip, repair_addr.port()),
                )
            } else {
                (
                    gossip_addr,
                    tpu_addr,
                    tpu_forward_addr,
                    turbine_addr,
                    repair_addr,
                )
            };

        let wallclock = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time went backwards")
            .as_millis() as u64;

        let contact_info = ContactInfo::new(
            keypair.public_key().clone(),
            adv_gossip,
            adv_tpu,
            adv_tpu_fwd,
            adv_turbine,
            adv_repair,
            cli.shred_version,
            wallclock,
        );

        // 15. Create ClusterInfo
        let entrypoints: Vec<SocketAddr> = cli
            .entrypoints
            .iter()
            .filter_map(|ep| {
                // Try direct parse first, then DNS resolution for hostnames
                if let Ok(addr) = ep.parse() {
                    return Some(addr);
                }
                // Resolve hostname (e.g. "validator-2:8000" in Docker)
                match std::net::ToSocketAddrs::to_socket_addrs(&ep.as_str()) {
                    Ok(mut addrs) => {
                        if let Some(addr) = addrs.next() {
                            info!(entrypoint = ep, resolved = %addr, "resolved entrypoint hostname");
                            Some(addr)
                        } else {
                            warn!(entrypoint = ep, "hostname resolved to no addresses, skipping");
                            None
                        }
                    }
                    Err(e) => {
                        warn!(entrypoint = ep, error = %e, "failed to resolve entrypoint, skipping");
                        None
                    }
                }
            })
            .collect();

        let cluster_info = Arc::new(ClusterInfo::new(
            Arc::clone(&keypair),
            contact_info,
            entrypoints,
            60_000, // ping_cache_ttl_ms
        ));

        // 16. Build ReplayStage
        let tower = Tower::new(VoteState::new(&VoteInit {
            node_pubkey: identity_address,
            authorized_voter: identity_address,
            authorized_withdrawer: identity_address,
            commission: 0,
        }));
        let fork_tree = ForkTree::new(last_root, parent_hash, parent_bank_hash);
        let commitment_tracker = CommitmentTracker::new(bank.total_active_stake());
        let gpu_verifier = GpuPohVerifier::new().ok().flatten();
        let mut replay_stage = ReplayStage::new(
            Arc::clone(&bank),
            tower,
            fork_tree,
            commitment_tracker,
            gpu_verifier,
        );

        // 17. Compute initial leader schedule
        let leader_cache: SharedLeaderCache = Arc::new(parking_lot::RwLock::new(HashMap::new()));
        let leader_schedule_generator = LeaderScheduleGenerator::new(epoch_schedule.clone());

        let stakes = bank.get_stake_distribution();
        if let Ok(schedule) =
            leader_schedule_generator.compute_schedule(current_epoch, &stakes, &genesis_hash)
        {
            replay_stage.cache_leader_schedule(current_epoch, schedule.clone());
            leader_cache.write().insert(current_epoch, schedule);
            info!(epoch = current_epoch, "initial leader schedule computed");
        }

        // 18. Look up own vote account from genesis validators
        let my_vote_account = validators
            .iter()
            .find(|v| v.identity == identity_address)
            .map(|v| v.vote_account);

        if let Some(va) = my_vote_account {
            info!(vote_account = %va.to_base64(), "found own vote account");
        } else {
            warn!("no vote account found for this identity ??? votes will not be submitted");
        }

        info!(
            start_slot = current_slot,
            identity = %identity_address.to_base64(),
            gossip = %gossip_addr,
            turbine = %turbine_addr,
            tpu = %tpu_addr,
            peers = cluster_info.entrypoints().len(),
            "validator ready"
        );

        let gossip_vote_cursor = cluster_info.crds().current_cursor();

        Ok(Self {
            keypair,
            identity: identity_address,
            storage,
            bank,
            block_producer,
            mempool,
            slot_clock,
            current_slot,
            cluster_info,
            replay_stage,
            leader_cache,
            leader_schedule_generator,
            epoch_schedule,
            genesis_hash,
            my_vote_account,
            gossip_addr,
            turbine_addr,
            repair_addr,
            tpu_addr,
            tpu_forward_addr,
            consecutive_skips: Arc::new(AtomicU64::new(0)),
            total_skips: 0,
            gossip_vote_cursor,
            slash_detector: nusantara_consensus::SlashDetector::new(),
            fee_calculator,
            rent,
            program_cache,
            pubsub_tx: RpcState::new_pubsub_channel(),
            orphan_blocks: BTreeMap::new(),
            shred_collector: Arc::new(ShredCollector::new()),
            failed_fork_targets: HashSet::new(),
            last_voted_slot: current_slot,
        })
    }

    pub async fn run(&mut self, cli: &Cli) -> Result<(), ValidatorError> {
        info!(start_slot = self.current_slot, "starting validator");

        // 1. Shutdown channel
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        // 2. Spawn GossipService
        let gossip_service =
            GossipService::new(Arc::clone(&self.cluster_info), self.gossip_addr).await?;
        let gossip_shutdown = shutdown_rx.clone();
        tokio::spawn(async move {
            gossip_service.run(gossip_shutdown).await;
        });
        info!(addr = %self.gossip_addr, "gossip service started");

        // 3. Spawn Turbine pipeline
        let turbine_socket = Arc::new(UdpSocket::bind(self.turbine_addr).await?);
        let repair_socket = Arc::new(UdpSocket::bind(self.repair_addr).await?);
        info!(
            turbine = %turbine_socket.local_addr()?,
            repair = %repair_socket.local_addr()?,
            "turbine sockets bound"
        );

        let shred_collector = Arc::clone(&self.shred_collector);

        // Shared current-slot counter (used by RepairService and TPU leader lookup)
        let current_slot_shared = Arc::new(AtomicU64::new(self.current_slot));

        // Channels
        let (shred_tx, shred_rx) = mpsc::channel(10_000);
        let repair_shred_tx = shred_tx.clone(); // Clone before shred_tx moves into ShredReceiver
        let (repair_msg_tx, _repair_msg_rx) = mpsc::channel(1_000);
        let (block_tx, block_rx) = mpsc::channel(100);

        // 3a. ShredReceiver
        let shred_receiver = ShredReceiver::new(Arc::clone(&turbine_socket));
        let shred_shutdown = shutdown_rx.clone();
        tokio::spawn(async move {
            shred_receiver
                .run(shred_tx, repair_msg_tx, shred_shutdown)
                .await;
        });

        // 3b. RetransmitStage
        let retransmit = RetransmitStage::new(
            self.identity,
            Arc::clone(&turbine_socket),
            Arc::clone(&shred_collector),
        );
        let retransmit_shutdown = shutdown_rx.clone();

        // tree_provider closure
        let tree_leader_cache = Arc::clone(&self.leader_cache);
        let tree_cluster_info = Arc::clone(&self.cluster_info);
        let tree_bank = Arc::clone(&self.bank);
        let tree_epoch_schedule = self.epoch_schedule.clone();
        let tree_identity = self.identity;
        let tree_provider = move |slot: u64| -> Option<TurbineTree> {
            let epoch = tree_epoch_schedule.get_epoch(slot);
            let cache = tree_leader_cache.read();
            let leader = *cache.get(&epoch)?.get_leader(slot, &tree_epoch_schedule)?;
            let mut peers: Vec<Hash> = tree_cluster_info
                .all_peers()
                .iter()
                .map(|ci| ci.identity)
                .collect();
            if !peers.contains(&tree_identity) {
                peers.push(tree_identity);
            }
            let stakes = tree_bank.get_stake_distribution();
            Some(TurbineTree::new(
                leader,
                &peers,
                &stakes,
                slot,
                TURBINE_FANOUT as usize,
            ))
        };

        // addr_lookup closure
        let retransmit_ci = Arc::clone(&self.cluster_info);
        let addr_lookup = move |id: &Hash| -> Option<SocketAddr> {
            retransmit_ci
                .get_contact_info(id)
                .map(|ci| ci.turbine_addr.0)
        };

        // pubkey_lookup closure ??? resolve leader identity to PublicKey for shred verification
        let pubkey_ci = Arc::clone(&self.cluster_info);
        let pubkey_lookup =
            move |id: &Hash| -> Option<nusantara_crypto::PublicKey> { pubkey_ci.get_pubkey(id) };

        tokio::spawn(async move {
            retransmit
                .run(
                    shred_rx,
                    block_tx,
                    tree_provider,
                    addr_lookup,
                    pubkey_lookup,
                    retransmit_shutdown,
                )
                .await;
        });

        // 3c. RepairService
        let repair_service = RepairService::new(
            Arc::clone(&repair_socket),
            Arc::clone(&shred_collector),
            Arc::clone(&current_slot_shared),
        );
        let repair_shutdown = shutdown_rx.clone();
        let repair_ci = Arc::clone(&self.cluster_info);
        let my_identity = self.identity;
        let repair_peers_fn = move || -> Vec<SocketAddr> {
            repair_ci
                .all_peers()
                .iter()
                .filter(|ci| ci.identity != my_identity)
                .map(|ci| ci.repair_addr.0)
                .collect()
        };
        tokio::spawn(async move {
            repair_service.run(repair_peers_fn, repair_shutdown).await;
        });

        // 3d. Repair responder — handles incoming repair requests and responses
        // on the repair socket. Without this, repair is non-functional:
        // - Incoming RepairRequest: look up block from storage, re-shred, send back
        // - Incoming RepairResponse: feed shreds into normal pipeline for assembly
        let responder_socket = Arc::clone(&repair_socket);
        let responder_storage = Arc::clone(&self.storage);
        let responder_keypair = Arc::clone(&self.keypair);
        let mut responder_shutdown = shutdown_rx.clone();
        tokio::spawn(async move {
            let mut buf = vec![0u8; MAX_UDP_PACKET];
            loop {
                tokio::select! {
                    biased;
                    result = responder_socket.recv_from(&mut buf) => {
                        match result {
                            Ok((len, src)) => {
                                let data = &buf[..len];
                                match TurbineMessage::deserialize_from_bytes(data) {
                                    Ok(TurbineMessage::RepairRequest(request)) => {
                                        metrics::counter!("turbine_repair_requests_received")
                                            .increment(1);
                                        let slot = match &request {
                                            RepairRequest::Shred { slot, .. }
                                            | RepairRequest::ShredBatch { slot, .. }
                                            | RepairRequest::HighestShred { slot }
                                            | RepairRequest::Orphan { slot } => *slot,
                                        };
                                        tracing::debug!(slot, ?request, %src, "received repair request");
                                        let block = match responder_storage.get_block(slot) {
                                            Ok(Some(b)) => b,
                                            _ => continue,
                                        };
                                        let shred_batch = match Shredder::shred_block(
                                            &block,
                                            block.header.parent_slot,
                                            &responder_keypair,
                                        ) {
                                            Ok(b) => b,
                                            Err(_) => continue,
                                        };
                                        match request {
                                            RepairRequest::Shred { index, .. } => {
                                                if let Some(shred) =
                                                    shred_batch.data_shreds.get(index as usize)
                                                {
                                                    let msg = TurbineMessage::RepairResponse(
                                                        SignedShred::Data(shred.clone()),
                                                    );
                                                    if let Ok(bytes) = msg.serialize_to_bytes() {
                                                        let _ = responder_socket
                                                            .send_to(&bytes, src)
                                                            .await;
                                                    }
                                                }
                                            }
                                            RepairRequest::ShredBatch { indices, .. } => {
                                                let shreds: Vec<SignedShred> = indices
                                                    .iter()
                                                    .filter_map(|&i| {
                                                        shred_batch
                                                            .data_shreds
                                                            .get(i as usize)
                                                            .map(|s| SignedShred::Data(s.clone()))
                                                    })
                                                    .collect();
                                                let batches = BatchRepairResponse::pack(
                                                    slot,
                                                    shreds,
                                                    MAX_UDP_PACKET,
                                                );
                                                for batch in batches {
                                                    let msg = TurbineMessage::BatchRepairResponse(
                                                        batch,
                                                    );
                                                    if let Ok(bytes) = msg.serialize_to_bytes() {
                                                        let _ = responder_socket
                                                            .send_to(&bytes, src)
                                                            .await;
                                                    }
                                                }
                                            }
                                            RepairRequest::HighestShred { .. }
                                            | RepairRequest::Orphan { .. } => {
                                                // Send the LAST data shred first as a single
                                                // RepairResponse (1 shred = 1 UDP packet). This
                                                // ensures the receiver learns `last_index` from the
                                                // very first packet, so even if subsequent batch
                                                // packets are lost, `missing_shreds()` can return
                                                // specific indices for targeted ShredBatch repair.
                                                if let Some(last_shred) =
                                                    shred_batch.data_shreds.last()
                                                {
                                                    let msg = TurbineMessage::RepairResponse(
                                                        SignedShred::Data(last_shred.clone()),
                                                    );
                                                    if let Ok(bytes) = msg.serialize_to_bytes() {
                                                        let _ = responder_socket
                                                            .send_to(&bytes, src)
                                                            .await;
                                                    }
                                                }
                                                // Then send all shreds as batch (idempotent
                                                // insert via BTreeMap in ShredCollector)
                                                let shreds: Vec<SignedShred> = shred_batch
                                                    .data_shreds
                                                    .iter()
                                                    .map(|s| SignedShred::Data(s.clone()))
                                                    .collect();
                                                let batches = BatchRepairResponse::pack(
                                                    slot,
                                                    shreds,
                                                    MAX_UDP_PACKET,
                                                );
                                                for batch in batches {
                                                    let msg = TurbineMessage::BatchRepairResponse(
                                                        batch,
                                                    );
                                                    if let Ok(bytes) = msg.serialize_to_bytes() {
                                                        let _ = responder_socket
                                                            .send_to(&bytes, src)
                                                            .await;
                                                    }
                                                }
                                            }
                                        }
                                        metrics::counter!("turbine_repair_responses_sent")
                                            .increment(1);
                                    }
                                    Ok(TurbineMessage::RepairResponse(shred)) => {
                                        tracing::debug!(
                                            slot = shred.slot(),
                                            index = shred.index(),
                                            %src,
                                            "received repair response shred"
                                        );
                                        metrics::counter!("turbine_repair_shreds_received")
                                            .increment(1);
                                        let _ = repair_shred_tx.send((shred, src)).await;
                                    }
                                    Ok(TurbineMessage::BatchRepairResponse(batch)) => {
                                        tracing::debug!(
                                            slot = batch.slot,
                                            shred_count = batch.shreds.len(),
                                            %src,
                                            "received batch repair response"
                                        );
                                        let count = batch.shreds.len() as u64;
                                        metrics::counter!("turbine_repair_shreds_received")
                                            .increment(count);
                                        for shred in batch.shreds {
                                            let _ = repair_shred_tx.send((shred, src)).await;
                                        }
                                    }
                                    _ => {}
                                }
                            }
                            Err(e) => {
                                tracing::error!(error = %e, "repair socket recv error");
                            }
                        }
                    }
                    _ = responder_shutdown.changed() => {
                        break;
                    }
                }
            }
            info!("repair responder stopped");
        });
        info!("turbine pipeline started");

        // 4. Spawn TpuService
        let server_config = TpuService::create_server_config()?;
        let client_config = TpuService::create_client_config()?;

        let server_endpoint = quinn::Endpoint::server(server_config, self.tpu_addr)?;
        let mut client_endpoint =
            quinn::Endpoint::client("0.0.0.0:0".parse::<SocketAddr>().unwrap())?;
        client_endpoint.set_default_client_config(client_config);

        let tpu_identity = self.identity;
        let tpu_shutdown = shutdown_rx.clone();

        // Bridge channel: TPU writes to mpsc, background task drains into mempool.
        // This avoids modifying the TPU-forward crate's mpsc-based API.
        let (tpu_tx_sender, mut tpu_tx_receiver) = mpsc::channel::<Transaction>(10_000);
        let rpc_tx_forward_sender = tpu_tx_sender.clone();
        let tpu_mempool = Arc::clone(&self.mempool);
        let mut tpu_bridge_shutdown = shutdown_rx.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    result = tpu_bridge_shutdown.changed() => {
                        if result.is_ok() {
                            // Drain remaining before stopping
                            while let Ok(tx) = tpu_tx_receiver.try_recv() {
                                let _ = tpu_mempool.insert(tx);
                            }
                        }
                        break;
                    }
                    Some(tx) = tpu_tx_receiver.recv() => {
                        if let Err(e) = tpu_mempool.insert(tx) {
                            tracing::debug!(error = %e, "TPU bridge: mempool rejected transaction");
                        }
                    }
                }
            }
            info!("TPU-mempool bridge stopped");
        });

        // leader_lookup closure for TPU
        let tpu_leader_cache = Arc::clone(&self.leader_cache);
        let tpu_cluster_info = Arc::clone(&self.cluster_info);
        let tpu_epoch_schedule = self.epoch_schedule.clone();
        let tpu_current_slot = Arc::clone(&current_slot_shared);

        let leader_lookup = move || -> Option<(Hash, SocketAddr)> {
            let slot = tpu_current_slot.load(Ordering::Relaxed);
            let epoch = tpu_epoch_schedule.get_epoch(slot);
            let cache = tpu_leader_cache.read();
            let leader = cache.get(&epoch)?.get_leader(slot, &tpu_epoch_schedule)?;
            let addr = tpu_cluster_info
                .get_contact_info(leader)?
                .tpu_forward_addr
                .0;
            Some((*leader, addr))
        };

        tokio::spawn(async move {
            TpuService::run(
                server_endpoint,
                client_endpoint,
                tpu_identity,
                tpu_tx_sender,
                leader_lookup,
                tpu_shutdown,
            )
            .await;
        });
        info!(addr = %self.tpu_addr, "TPU service started");

        // 5. Spawn RPC server
        let rpc_addr: SocketAddr = cli
            .rpc_addr
            .parse()
            .map_err(|e| ValidatorError::NetworkInit(format!("invalid rpc addr: {e}")))?;

        let faucet_keypair = if cli.enable_faucet {
            nusantara_genesis::load_faucet_keypair(&self.storage)
                .map(Arc::new)
                .or_else(|| {
                    tracing::warn!("no faucet keypair in genesis, falling back to validator identity");
                    Some(Arc::clone(&self.keypair))
                })
        } else {
            None
        };

        let rpc_state = Arc::new(RpcState {
            storage: Arc::clone(&self.storage),
            bank: Arc::clone(&self.bank),
            mempool: Arc::clone(&self.mempool),
            leader_cache: Arc::clone(&self.leader_cache),
            leader_schedule_generator: LeaderScheduleGenerator::new(self.epoch_schedule.clone()),
            epoch_schedule: self.epoch_schedule.clone(),
            genesis_hash: self.genesis_hash,
            faucet_keypair,
            identity: self.identity,
            cluster_info: Arc::clone(&self.cluster_info),
            consecutive_skips: Arc::clone(&self.consecutive_skips),
            tx_forward_sender: Some(rpc_tx_forward_sender),
            pubsub_tx: self.pubsub_tx.clone(),
            snapshot_dir: Path::new(&cli.ledger_path).join("snapshots"),
        });
        // Build optional TLS config from CLI flags
        let rpc_tls = match (&cli.rpc_tls_cert, &cli.rpc_tls_key) {
            (Some(cert_path), Some(key_path)) => {
                let tls = RpcTlsConfig::from_pem_files(Path::new(cert_path), Path::new(key_path))
                    .map_err(|e| ValidatorError::NetworkInit(format!("RPC TLS init: {e}")))?;
                info!(cert = cert_path, key = key_path, "RPC TLS enabled");
                Some(tls)
            }
            (Some(_), None) | (None, Some(_)) => {
                return Err(ValidatorError::NetworkInit(
                    "both --rpc-tls-cert and --rpc-tls-key must be provided".to_string(),
                ));
            }
            _ => None,
        };

        let rpc_shutdown = shutdown_rx.clone();
        tokio::spawn(async move {
            RpcServer::serve(rpc_addr, rpc_state, rpc_tls, rpc_shutdown).await;
        });
        info!(addr = %rpc_addr, "RPC server started");

        // 6. Create BroadcastStage (called on-demand by leader path)
        let broadcast_stage =
            BroadcastStage::new(Arc::clone(&self.keypair), Arc::clone(&turbine_socket));

        // 6. Main slot loop
        let mut block_rx = block_rx;
        loop {
            tokio::select! {
                biased;
                _ = tokio::signal::ctrl_c() => {
                    info!("received shutdown signal");
                    let _ = shutdown_tx.send(true);
                    break;
                }
                _ = self.slot_clock.wait_for_slot(self.current_slot) => {
                    // Update shared current_slot for TPU closure
                    current_slot_shared.store(self.current_slot, Ordering::Relaxed);

                    if self.am_i_leader(self.current_slot) {
                        self.leader_slot(&broadcast_stage, &mut block_rx).await?;
                    } else {
                        self.non_leader_slot(&mut block_rx, cli.leader_timeout_ms).await?;
                    }

                    self.process_gossip_votes();

                    // Check for fork switch (F3)
                    if let Some(plan) = self.replay_stage.check_fork_switch() {
                        let target = plan.replay_slots.last().copied()
                            .unwrap_or(plan.common_ancestor);
                        if self.failed_fork_targets.contains(&target) {
                            tracing::trace!(target, "skipping fork switch ??? already failed");
                        } else {
                            self.handle_fork_switch(plan);
                        }
                    }

                    self.submit_vote(self.current_slot);
                    self.process_orphan_queue()?;
                    self.check_epoch_boundary(cli.snapshot_interval);

                    // Periodically report gossip peer count
                    if self.current_slot.is_multiple_of(10) {
                        let peer_count = self.cluster_info.peer_count();
                        metrics::gauge!("nusantara_gossip_peers").set(peer_count as f64);
                    }

                    // Periodic ledger pruning every 100 slots
                    if cli.max_ledger_slots > 0
                        && self.current_slot.is_multiple_of(100)
                    {
                        let min_slot =
                            self.current_slot.saturating_sub(cli.max_ledger_slots);
                        if min_slot > 0
                            && let Err(e) = self.storage.purge_slots_below(min_slot)
                        {
                            warn!(error = %e, min_slot, "ledger pruning failed");
                        }
                    }

                    self.current_slot += 1;
                }
            }
        }

        info!("validator shutdown complete");
        Ok(())
    }

    fn am_i_leader(&self, slot: u64) -> bool {
        let epoch = self.epoch_schedule.get_epoch(slot);

        // Ensure schedule is cached
        if !self.leader_cache.read().contains_key(&epoch) {
            let stakes = self.bank.get_stake_distribution();
            if let Ok(schedule) =
                self.leader_schedule_generator
                    .compute_schedule(epoch, &stakes, &self.genesis_hash)
            {
                self.leader_cache.write().insert(epoch, schedule);
            }
        }

        self.leader_cache
            .read()
            .get(&epoch)
            .and_then(|s| s.get_leader(slot, &self.epoch_schedule))
            .map(|leader| *leader == self.identity)
            .unwrap_or(false)
    }

    /// Restore the bank's slot_hashes from the fork tree ancestry of the
    /// current chain tip. Called after a failed replay to undo the corruption
    /// caused by `set_slot_hashes()` in `replay_block_full()`.
    fn restore_bank_slot_hashes(&self) {
        let tip = self.replay_stage.current_tip();
        let ancestry = self.replay_stage.fork_tree().get_ancestry(tip);
        let entries: Vec<(u64, Hash)> = ancestry
            .iter()
            .filter_map(|&s| {
                self.replay_stage
                    .fork_tree()
                    .get_node(s)
                    .map(|n| (s, n.block_hash))
            })
            .collect();
        self.bank.set_slot_hashes(SlotHashes(entries));
    }

    /// Replay a received block or buffer it if parent is missing.
    /// On verification mismatch, rewinds storage and discards the block.
    fn replay_or_buffer_block(&mut self, block: Block) -> Result<(), ValidatorError> {
        let slot = block.header.slot;
        let parent_slot = block.header.parent_slot;

        // Skip if already in fork tree (already replayed)
        if self.replay_stage.fork_tree().contains(slot) {
            tracing::debug!(slot, "block already replayed, skipping");
            return Ok(());
        }

        // Skip if already buffered as orphan (avoid duplicate replay attempts)
        if self.orphan_blocks.contains_key(&slot) {
            tracing::debug!(slot, "block already buffered as orphan, skipping");
            return Ok(());
        }

        // Store block early (before replay) so RPC can serve it regardless
        // of fork-tree state. put_block is pure storage with no consensus
        // side effects.
        let already_stored = self.storage.has_block_header(slot).unwrap_or(false);
        if !already_stored {
            self.storage.put_block(&block)?;
            self.shred_collector.mark_slot_stored(slot);
            metrics::counter!("nusantara_blocks_stored_early").increment(1);
        }

        match crate::block_replayer::replay_block_full(
            &block,
            &self.storage,
            &self.bank,
            &mut self.replay_stage,
            &self.fee_calculator,
            &self.rent,
            &self.epoch_schedule,
            &self.program_cache,
        ) {
            Ok(result) => {
                self.block_producer.set_parent(
                    slot,
                    block.header.block_hash,
                    result.bank_hash,
                );

                // Defer root advancement ??? try_advance_root() gates on orphan state
                if let Some(root) = result.new_root {
                    self.try_advance_root(root)?;
                }

                self.consecutive_skips.store(0, Ordering::Relaxed);

                // Publish pubsub events
                let root = self
                    .storage
                    .get_latest_root()
                    .unwrap_or(None)
                    .unwrap_or(0);
                let _ = self.pubsub_tx.send(PubsubEvent::SlotUpdate {
                    slot,
                    parent: parent_slot,
                    root,
                });
                let _ = self.pubsub_tx.send(PubsubEvent::BlockNotification {
                    slot,
                    block_hash: block.header.block_hash.to_base64(),
                    tx_count: block.header.transaction_count,
                });

                metrics::counter!("nusantara_blocks_replayed").increment(1);
                info!(
                    slot,
                    parent_slot,
                    fork_tree_nodes = self.replay_stage.fork_tree().node_count(),
                    fork_tree_root = self.replay_stage.fork_tree().root_slot(),
                    "block replayed successfully"
                );
                Ok(())
            }
            Err(ValidatorError::MissingParentBlock { slot, parent_slot }) => {
                let root = self.replay_stage.fork_tree().root_slot();
                if parent_slot < root {
                    // Parent already finalized and pruned ??? block can never be replayed
                    debug!(
                        slot,
                        parent_slot,
                        root,
                        "discarding block ??? parent already finalized and pruned"
                    );
                    metrics::counter!("nusantara_blocks_discarded_parent_pruned").increment(1);
                    return Ok(());
                }

                warn!(
                    slot,
                    parent_slot,
                    fork_tree_root = root,
                    fork_tree_nodes = self.replay_stage.fork_tree().node_count(),
                    "buffering orphan block ??? parent not in fork tree"
                );
                self.orphan_blocks.insert(slot, block);
                self.request_missing_slots(parent_slot);
                metrics::counter!("nusantara_orphan_blocks_buffered").increment(1);
                metrics::gauge!("nusantara_orphan_queue_size")
                    .set(self.orphan_blocks.len() as f64);
                Ok(())
            }
            // replay_block_full now cleans up storage internally on verification
            // failure (deletes contaminated CF_ACCOUNTS entries and restores the
            // account index to parent_slot). We just need to restore the bank's
            // slot_hashes to the current fork tip's ancestry.
            Err(ValidatorError::BankHashMismatch { slot })
            | Err(ValidatorError::MerkleRootMismatch { slot })
            | Err(ValidatorError::BlockHashMismatch { slot }) => {
                warn!(slot, "block verification mismatch ??? discarding");
                // Delete the pre-stored invalid block (only if we stored it
                // this call ??? don't delete a previously-verified valid block)
                if !already_stored {
                    let _ = self.storage.delete_block(slot);
                    metrics::counter!("nusantara_blocks_deleted_verification_failure")
                        .increment(1);
                }
                self.restore_bank_slot_hashes();
                metrics::counter!("nusantara_blocks_discarded_mismatch").increment(1);
                Ok(())
            }
            Err(e) => {
                // Check for SlotAlreadyProcessed (from replay_stage)
                let msg = e.to_string();
                if msg.contains("already processed") || msg.contains("already exists") {
                    tracing::debug!(slot, "block already in fork tree, skipping");
                    Ok(())
                } else {
                    Err(e)
                }
            }
        }
    }

    async fn leader_slot(
        &mut self,
        broadcast: &BroadcastStage,
        block_rx: &mut mpsc::Receiver<Block>,
    ) -> Result<(), ValidatorError> {
        // 1. Catch up on pending blocks from previous leader
        let mut pending = Vec::new();
        while let Ok(block) = block_rx.try_recv() {
            pending.push(block);
        }
        if !pending.is_empty() {
            pending.sort_by_key(|b| b.header.slot);
            info!(count = pending.len(), "catching up on pending blocks before leader slot");
            for block in pending {
                self.replay_or_buffer_block(block)?;
            }
            self.process_orphan_queue()?;
        }

        // 2. Wait for the previous slot's block if it's missing.
        //
        // If the immediately preceding slot is not in the fork tree and we're
        // not its leader, wait up to half a slot duration for it to arrive via
        // turbine. Without this, we produce with a stale parent (e.g. parent=22
        // when slot 23 hasn't arrived yet), creating a fork that can never be
        // reconciled with other validators' chains.
        let prev_slot = self.current_slot.saturating_sub(1);
        if prev_slot > 0
            && !self.replay_stage.fork_tree().contains(prev_slot)
            && !self.am_i_leader(prev_slot)
        {
            let wait_ms = DEFAULT_SLOT_DURATION_MS / 2;
            tracing::debug!(
                slot = self.current_slot,
                prev_slot,
                wait_ms,
                "waiting for previous slot's block before producing"
            );
            match tokio::time::timeout(
                Duration::from_millis(wait_ms),
                block_rx.recv(),
            )
            .await
            {
                Ok(Some(block)) => {
                    self.replay_or_buffer_block(block)?;
                    // Drain any additional blocks that arrived
                    while let Ok(extra) = block_rx.try_recv() {
                        self.replay_or_buffer_block(extra)?;
                    }
                    self.process_orphan_queue()?;
                }
                Ok(None) => return Err(ValidatorError::Shutdown),
                Err(_) => {
                    tracing::debug!(
                        slot = self.current_slot,
                        prev_slot,
                        "previous slot block didn't arrive, producing anyway"
                    );
                }
            }
        }

        // 3. Skip production if this slot was already processed (e.g. from orphan replay)
        if self.replay_stage.fork_tree().contains(self.current_slot) {
            info!(slot = self.current_slot, "slot already in fork tree, skipping production");
            return Ok(());
        }

        // 3b. NOTE: We must NOT skip production when we're the leader, even if
        // orphan blocks exist. Skipping creates a deadlock: our produced blocks
        // fill the gap that orphans need to resolve. If we skip, nobody produces
        // these slots, and orphans remain stuck forever.

        // 3a. CRITICAL: Set parent to the fork-choice best fork before producing.
        //
        // Without this, each validator produces on its own fork ??? the parent
        // follows the last replayed block, which may be from any fork. Root
        // advancement then prunes other validators' forks, causing permanent
        // chain divergence. By using the fork-choice best slot (heaviest
        // subtree), all validators converge on the same chain.
        let best = self.replay_stage.fork_tree().best_slot();
        if let Some(node) = self.replay_stage.fork_tree().get_node(best) {
            let prev_parent = self.block_producer.parent_slot();
            if prev_parent != best {
                tracing::info!(
                    prev_parent,
                    best_fork = best,
                    "switching parent to fork-choice best fork"
                );
            }
            self.block_producer
                .set_parent(best, node.block_hash, node.bank_hash);
        }

        // 3c. Rebuild slot_hashes and rewind account index from fork tree
        // ancestry before production.
        //
        // The bank's slot_hashes accumulates entries from ALL forks. If this
        // validator replayed blocks on a different fork, slot_hashes contains
        // entries that other validators' fork trees won't have. When a replayer
        // reconstructs slot_hashes from its fork tree ancestry, it gets a
        // different set ??? different RecentBlockhashes ??? transactions that
        // reference blockhashes from non-ancestor slots succeed here but fail
        // on replayers ??? different account_delta_hash ??? bank_hash mismatch.
        //
        // Similarly, the account index may point to versions from foreign
        // forks. A fork-aware rewind ensures the producer loads the same
        // account state that a replayer would reconstruct.
        let parent_slot = self.block_producer.parent_slot();
        let ancestry = self.replay_stage.fork_tree().get_ancestry(parent_slot);
        let fork_slot_hashes: Vec<(u64, Hash)> = ancestry
            .iter()
            .filter_map(|&s| {
                self.replay_stage
                    .fork_tree()
                    .get_node(s)
                    .map(|n| (s, n.block_hash))
            })
            .collect();
        self.bank.set_slot_hashes(SlotHashes(fork_slot_hashes));

        // Fork-aware account index rewind
        let ancestor_set: HashSet<u64> = ancestry.iter().copied().collect();
        let rewound = self
            .storage
            .rewind_account_index_for_ancestry(&ancestor_set)?;
        if rewound > 0 {
            tracing::info!(
                parent_slot,
                rewound,
                "rewound account index (fork-aware) before production"
            );
        }

        // 3b. Drain pending transactions from the priority mempool
        let transactions = self.mempool.drain_by_priority(2048);

        // 4. Produce block
        let block = self
            .block_producer
            .produce_block(self.current_slot, transactions)?;

        // Mark our own block as stored so retransmit stage doesn't re-assemble it
        self.shred_collector.mark_slot_stored(self.current_slot);

        // 5. Feed into ReplayStage for fork tree tracking
        let result = self.replay_stage.replay_block(&block, &[])?;

        // Defer root advancement ??? try_advance_root() gates on orphan state
        if let Some(root) = result.new_root {
            self.try_advance_root(root)?;
        }

        // 4. Build TurbineTree and broadcast
        let mut peers: Vec<Hash> = self
            .cluster_info
            .all_peers()
            .iter()
            .map(|ci| ci.identity)
            .collect();
        if !peers.contains(&self.identity) {
            peers.push(self.identity);
        }
        let stakes = self.bank.get_stake_distribution();
        let tree = TurbineTree::new(
            self.identity,
            &peers,
            &stakes,
            self.current_slot,
            TURBINE_FANOUT as usize,
        );
        let ci = Arc::clone(&self.cluster_info);
        broadcast
            .broadcast_block(&block, &tree, |id| {
                ci.get_contact_info(id).map(|c| c.turbine_addr.0)
            })
            .await?;

        // 5. Publish pubsub events for WebSocket subscribers
        let root = self.storage.get_latest_root().unwrap_or(None).unwrap_or(0);
        let _ = self.pubsub_tx.send(PubsubEvent::SlotUpdate {
            slot: self.current_slot,
            parent: block.header.parent_slot,
            root,
        });
        let _ = self.pubsub_tx.send(PubsubEvent::BlockNotification {
            slot: self.current_slot,
            block_hash: block.header.block_hash.to_base64(),
            tx_count: block.header.transaction_count,
        });

        metrics::counter!("nusantara_leader_slots").increment(1);
        info!(
            slot = self.current_slot,
            fork_tree_nodes = self.replay_stage.fork_tree().node_count(),
            fork_tree_root = self.replay_stage.fork_tree().root_slot(),
            "leader slot completed"
        );
        Ok(())
    }

    async fn non_leader_slot(
        &mut self,
        block_rx: &mut mpsc::Receiver<Block>,
        leader_timeout_ms: u64,
    ) -> Result<(), ValidatorError> {
        let timeout = Duration::from_millis(leader_timeout_ms);
        let mut blocks = Vec::new();

        // Wait for at least one block with timeout
        match tokio::time::timeout(timeout, block_rx.recv()).await {
            Ok(Some(block)) => blocks.push(block),
            Ok(None) => return Err(ValidatorError::Shutdown),
            Err(_) => {} // timeout ??? no block arrived
        }

        // Drain additional available blocks (non-blocking)
        while let Ok(block) = block_rx.try_recv() {
            blocks.push(block);
        }

        if blocks.is_empty() {
            // No blocks ??? skip this slot
            // NOTE: Do NOT record Hash::zero() in slot_hashes here. If the block
            // arrives late (via orphan queue), replay_block_full will build its
            // SysvarCache from slot_hashes. A stale zero entry would cause
            // RecentBlockhashes divergence vs the producing validator, leading to
            // bank_hash mismatch on replay.
            let skips = self.consecutive_skips.fetch_add(1, Ordering::Relaxed) + 1;
            self.total_skips += 1;
            warn!(
                slot = self.current_slot,
                consecutive_skips = skips,
                "no block received (leader skip)"
            );
            if skips > 10 {
                warn!(
                    consecutive_skips = skips,
                    "possible network partition ??? many consecutive leader skips"
                );
            }
            metrics::counter!("nusantara_leader_skips").increment(1);
            metrics::counter!("nusantara_non_leader_slots").increment(1);
            metrics::gauge!("nusantara_consecutive_skips").set(skips as f64);
            return Ok(());
        }

        // Sort by slot for correct replay order
        blocks.sort_by_key(|b| b.header.slot);
        metrics::gauge!("nusantara_blocks_drained_per_slot").set(blocks.len() as f64);

        // Process each block via replay_or_buffer_block.
        // Do NOT skip future-slot blocks ??? their parents may already be in the
        // fork tree, and replay_or_buffer_block will properly buffer orphans and
        // request repair for missing parents.
        for block in blocks {
            self.replay_or_buffer_block(block)?;
        }

        self.process_orphan_queue()?;
        metrics::counter!("nusantara_non_leader_slots").increment(1);
        Ok(())
    }

    /// Replay buffered orphan blocks whose parents are now in the fork tree.
    /// Loops until no more orphans can be replayed.
    fn process_orphan_queue(&mut self) -> Result<(), ValidatorError> {
        // Evict orphans older than ORPHAN_HORIZON slots to prevent unbounded
        // growth. Matches the horizon used in try_advance_root so stale orphans
        // are cleaned up promptly instead of lingering and blocking finalization.
        let cutoff = self.current_slot.saturating_sub(ORPHAN_HORIZON);
        self.orphan_blocks.retain(|slot, _| *slot > cutoff);

        // Evict orphans whose parent is below the fork tree root ??? these parents
        // have been finalized and pruned, so the orphans can never be replayed.
        let root = self.replay_stage.fork_tree().root_slot();
        let before = self.orphan_blocks.len();
        self.orphan_blocks
            .retain(|_slot, block| block.header.parent_slot >= root);
        let pruned = before - self.orphan_blocks.len();
        if pruned > 0 {
            debug!(
                pruned_count = pruned,
                root,
                remaining = self.orphan_blocks.len(),
                "discarded irrecoverable orphans (parent below root)"
            );
            metrics::counter!("nusantara_orphan_blocks_pruned_below_root")
                .increment(pruned as u64);
            metrics::gauge!("nusantara_orphan_queue_size")
                .set(self.orphan_blocks.len() as f64);
        }

        loop {
            let ready_slot = self.orphan_blocks.iter().find_map(|(slot, block)| {
                if self
                    .replay_stage
                    .fork_tree()
                    .get_node(block.header.parent_slot)
                    .is_some()
                {
                    Some(*slot)
                } else {
                    None
                }
            });

            let Some(slot) = ready_slot else { break };
            let block = self.orphan_blocks.remove(&slot).unwrap();

            info!(slot, parent_slot = block.header.parent_slot, "replaying buffered orphan block");

            self.replay_or_buffer_block(block)?;

            metrics::counter!("nusantara_orphan_blocks_replayed").increment(1);
            metrics::gauge!("nusantara_orphan_queue_size")
                .set(self.orphan_blocks.len() as f64);
        }
        Ok(())
    }

    /// Request repair for missing ancestors across ALL orphan chains.
    ///
    /// Scans every orphan block's parent_slot and requests repair for any
    /// parent that is NOT in the fork tree AND NOT already buffered as an
    /// orphan. This finds ALL gap roots simultaneously, enabling parallel
    /// repair of multiple gaps instead of one-at-a-time sequential repair.
    fn request_missing_slots(&self, _needed_slot: u64) {
        let root = self.replay_stage.fork_tree().root_slot();
        let mut to_repair = Vec::new();

        // Scan ALL orphan blocks for missing parents
        for block in self.orphan_blocks.values() {
            let parent = block.header.parent_slot;
            if parent >= root
                && self.replay_stage.fork_tree().get_node(parent).is_none()
                && !self.orphan_blocks.contains_key(&parent)
            {
                to_repair.push(parent);
            }
        }

        // Deduplicate and limit
        to_repair.sort_unstable();
        to_repair.dedup();
        let count = to_repair.len().min(32);
        for &slot in &to_repair[..count] {
            self.shred_collector.request_slot_repair(slot);
        }

        if count > 0 {
            debug!(
                gap_roots = count,
                fork_tree_root = root,
                orphan_count = self.orphan_blocks.len(),
                "requesting repair for missing ancestor slots"
            );
        }
    }

    /// Advance the fork tree root, but only if doing so won't:
    /// 1. Prune parents that pending orphan blocks depend on
    /// 2. Disconnect fork branches that other validators may be building on
    ///
    /// Without these gates, root advancement prunes other validators' forks,
    /// causing permanent chain divergence. For example: if V2's fork branches
    /// from slot 95 (95???100???101???...) and root advances to 96, V2's fork is
    /// disconnected and all subsequent V2 blocks become unreplayable orphans.
    fn try_advance_root(&mut self, proposed_root: u64) -> Result<(), ValidatorError> {
        let current_root = self.replay_stage.fork_tree().root_slot();
        if proposed_root <= current_root {
            return Ok(());
        }

        // Safety valve: if the gap between proposed and current root exceeds
        // MAX_ROOT_GAP, force advance bypassing both gates. Tower BFT already
        // determined correctness via lockout voting — the gates are short-term
        // transient protection, not permanent blocks. This prevents the
        // deadlock where shred loss → orphan accumulation → root stall.
        const MAX_ROOT_GAP: u64 = 64;
        if proposed_root > current_root + MAX_ROOT_GAP {
            tracing::warn!(
                proposed_root,
                current_root,
                gap = proposed_root - current_root,
                orphan_count = self.orphan_blocks.len(),
                "forcing root advancement — gap exceeds {MAX_ROOT_GAP} slots"
            );
            metrics::counter!("nusantara_root_safety_valve_activated").increment(1);
            self.replay_stage.advance_root(proposed_root)?;
            metrics::gauge!("nusantara_root_slot").set(proposed_root as f64);
            self.failed_fork_targets.clear();
            return Ok(());
        }

        let mut safe_root = proposed_root;

        // Gate 1: Don't prune parents of *recent* orphan blocks.
        //
        // Only orphans whose parent is within ORPHAN_HORIZON slots of the
        // proposed root are considered "recent" enough to preserve.  Stale
        // orphans (> ORPHAN_HORIZON behind) represent dead forks that will
        // never be resolved — letting them block root advancement causes
        // the deadlock observed under high-throughput benchmarks where
        // large blocks cause shred loss and the repair loop can't recover.
        //
        // 32 slots (28.8s at 900ms/slot) is aggressive but necessary in
        // environments with continuous shred loss (e.g. Docker UDP). In
        // production networks with reliable connectivity a larger horizon
        // (e.g. 200) would be appropriate.
        if !self.orphan_blocks.is_empty() {
            let horizon = proposed_root.saturating_sub(ORPHAN_HORIZON);
            let min_recent_orphan_parent = self
                .orphan_blocks
                .values()
                .map(|b| b.header.parent_slot)
                .filter(|&p| p >= horizon)
                .min();
            if let Some(min_parent) = min_recent_orphan_parent {
                safe_root = safe_root.min(min_parent.saturating_sub(1));
            }
        }

        // Gate 2: Don't advance past *recent* fork points that have branches
        // outside the proposed root's ancestry chain.
        //
        // Walk the ancestry from proposed_root back to current_root. For each
        // slot in the ancestry, check if it has children NOT on the ancestry
        // path. If so, advancing the root past that fork point would disconnect
        // those children (other validators' forks), making them unreplayable.
        //
        // Only consider fork points within ORPHAN_HORIZON of the proposed
        // root. Stale fork points (> 200 slots behind) represent dead forks
        // whose branches will never grow — holding root for them causes the
        // same deadlock as stale orphans.
        let fork_horizon = proposed_root.saturating_sub(ORPHAN_HORIZON);
        let ancestry = self.replay_stage.fork_tree().get_ancestry(proposed_root);
        let ancestry_set: HashSet<u64> = ancestry.iter().copied().collect();
        for &slot in &ancestry {
            if slot < current_root || slot < fork_horizon {
                break;
            }
            if let Some(node) = self.replay_stage.fork_tree().get_node(slot) {
                let has_branch = node
                    .children
                    .iter()
                    .any(|child| !ancestry_set.contains(child));
                if has_branch {
                    safe_root = safe_root.min(slot);
                    tracing::debug!(
                        fork_point = slot,
                        proposed_root,
                        "limiting root to preserve fork branch"
                    );
                }
            }
        }

        if safe_root > current_root {
            self.replay_stage.advance_root(safe_root)?;
            metrics::gauge!("nusantara_root_slot").set(safe_root as f64);
            self.failed_fork_targets.clear();
            if safe_root < proposed_root {
                tracing::debug!(
                    proposed_root,
                    safe_root,
                    orphan_count = self.orphan_blocks.len(),
                    "root advancement limited to preserve forks/orphans"
                );
            }
        } else {
            if proposed_root > current_root + 10 {
                tracing::debug!(
                    proposed_root,
                    current_root,
                    orphan_count = self.orphan_blocks.len(),
                    "root advancement suppressed ??? preserving forks/orphan parents"
                );
            }
            metrics::counter!("nusantara_root_advancement_deferred").increment(1);
        }
        Ok(())
    }

    /// Submit a single vote transaction covering all unvoted slots since the
    /// last vote. This batches `[last_voted_slot+1 ..= slot]` into one Vote
    /// instead of emitting one tx per slot, which eliminates the burst of
    /// VoteTooOld / LockoutViolation errors on replaying validators.
    fn submit_vote(&mut self, slot: u64) {
        let Some(vote_account) = self.my_vote_account else {
            return;
        };

        if slot <= self.last_voted_slot {
            return;
        }

        // Collect all unvoted slots in range (last_voted+1 ..= slot)
        let vote_slots: Vec<u64> = (self.last_voted_slot + 1..=slot).collect();

        let block_hash = self
            .bank
            .slot_hashes()
            .0
            .iter()
            .find(|(s, _)| *s == slot)
            .map(|(_, h)| *h)
            .unwrap_or(Hash::zero());

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time went backwards")
            .as_secs() as i64;

        let vote = Vote {
            slots: vote_slots,
            hash: block_hash,
            timestamp: Some(timestamp),
        };

        let tx = build_vote_transaction(&self.keypair, &vote_account, vote, block_hash);
        let _ = self.mempool.insert(tx); // best-effort

        self.last_voted_slot = slot;

        // Also publish vote via gossip for fast propagation (F4)
        self.cluster_info.push_vote(slot, block_hash);

        metrics::counter!("nusantara_votes_submitted").increment(1);
    }

    /// Handle a fork switch by rolling back to common ancestor and replaying.
    ///
    /// On failure, restores bank slot_hashes and records the target in
    /// `failed_fork_targets` to prevent infinite retry.  `replay_block_full`
    /// already cleans up storage (CF_ACCOUNTS) on verification failure, so
    /// we only need to restore bank-level state here.
    fn handle_fork_switch(
        &mut self,
        plan: nusantara_consensus::replay_stage::ForkSwitchPlan,
    ) {
        let target = plan
            .replay_slots
            .last()
            .copied()
            .unwrap_or(plan.common_ancestor);

        info!(
            common_ancestor = plan.common_ancestor,
            rollback_from = plan.rollback_from,
            replay_count = plan.replay_slots.len(),
            target,
            "switching forks"
        );

        // 1. Rollback bank to common ancestor
        if let Err(e) = self.bank.rollback_to_slot(plan.common_ancestor, &self.storage) {
            warn!(error = %e, "fork switch: bank rollback failed");
            self.failed_fork_targets.insert(target);
            return;
        }

        // 2. Rewind account index
        match self.storage.rewind_account_index_to_slot(plan.common_ancestor) {
            Ok(rewound) => {
                if rewound > 0 {
                    info!(rewound, "account index rewound for fork switch");
                }
            }
            Err(e) => {
                warn!(error = %e, "fork switch: account index rewind failed");
                self.restore_bank_slot_hashes();
                self.failed_fork_targets.insert(target);
                return;
            }
        }

        // 3. Replay blocks on the new fork (skip slots already in fork tree)
        for slot in &plan.replay_slots {
            if self.replay_stage.fork_tree().contains(*slot) {
                tracing::debug!(slot, "slot already in fork tree, skipping fork-switch replay");
                continue;
            }
            match self.storage.get_block(*slot) {
                Ok(Some(block)) => {
                    if let Err(e) = crate::block_replayer::replay_block_full(
                        &block,
                        &self.storage,
                        &self.bank,
                        &mut self.replay_stage,
                        &self.fee_calculator,
                        &self.rent,
                        &self.epoch_schedule,
                        &self.program_cache,
                    ) {
                        warn!(
                            slot,
                            error = %e,
                            "fork switch replay failed ??? aborting switch"
                        );
                        self.restore_bank_slot_hashes();
                        self.failed_fork_targets.insert(target);
                        metrics::counter!("nusantara_fork_switch_failures").increment(1);
                        return;
                    }
                }
                Ok(None) => {
                    warn!(slot, "block not found for fork replay ??? aborting switch");
                    self.restore_bank_slot_hashes();
                    self.failed_fork_targets.insert(target);
                    return;
                }
                Err(e) => {
                    warn!(slot, error = %e, "failed to load block for fork replay");
                    self.restore_bank_slot_hashes();
                    self.failed_fork_targets.insert(target);
                    return;
                }
            }
        }

        // 4. Update block producer parent to new fork tip
        if let Some(node) = self.replay_stage.fork_tree().get_node(target) {
            self.block_producer
                .set_parent(target, node.block_hash, node.bank_hash);
        }

        // Success ??? clear failed targets since fork landscape changed
        self.failed_fork_targets.clear();
        info!(new_tip = target, "fork switch complete");
        metrics::counter!("nusantara_fork_switches_completed").increment(1);
    }

    /// Drain gossip votes and feed them into the consensus engine.
    ///
    /// Before normal processing, each vote is checked for equivocation (double-voting).
    /// If a validator voted for two different blocks at the same slot, a `SlashProof`
    /// is persisted and a 5% stake penalty is applied to the slash registry.
    fn process_gossip_votes(&mut self) {
        let (votes, new_cursor) = self.cluster_info.get_votes_since(self.gossip_vote_cursor);
        for vote in &votes {
            // Check for equivocation before processing the vote normally
            if let Some(proof) = self.slash_detector.check_vote(
                &vote.from,
                vote.slot,
                &vote.hash,
                &self.identity,
            ) {
                // Persist slash proof to storage
                if let Err(e) = self.storage.put_slash_proof(&proof) {
                    warn!(error = %e, "failed to store slash proof");
                }

                // Calculate penalty: 5% of validator's current effective stake
                let validator_stake = self.bank.get_validator_stake(&proof.validator);
                let penalty =
                    validator_stake * nusantara_consensus::SLASH_PENALTY_BPS / 10_000;
                if penalty > 0 {
                    self.bank.apply_slash(&proof.validator, penalty);
                    info!(
                        validator = %proof.validator.to_base64(),
                        slot = proof.slot,
                        penalty,
                        "slash penalty applied for double vote"
                    );
                }
            }

            // Normal vote processing
            let stake = self.bank.get_validator_stake(&vote.from);
            if stake > 0 {
                self.replay_stage
                    .process_gossip_vote(vote.from, vote.slot, vote.hash, stake);
            }
        }

        // Periodic purge of old slash detector entries to bound memory
        if self.current_slot.is_multiple_of(100) {
            self.slash_detector
                .purge_below(self.current_slot.saturating_sub(1000));
        }

        if !votes.is_empty() {
            metrics::counter!("nusantara_gossip_votes_processed").increment(votes.len() as u64);
        }
        self.gossip_vote_cursor = new_cursor;
    }

    fn check_epoch_boundary(&mut self, snapshot_interval: u64) {
        let current_epoch = self.epoch_schedule.get_epoch(self.current_slot);
        let next_epoch = self.epoch_schedule.get_epoch(self.current_slot + 1);

        if next_epoch > current_epoch {
            // 0. Collect rent from accounts
            self.collect_rent(current_epoch);

            // 1. Calculate and distribute rewards
            self.distribute_epoch_rewards(current_epoch);

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

    /// Create a snapshot of the current state at current slot.
    fn create_snapshot(&self) {
        use nusantara_storage::snapshot_archive;

        let bank_hash = self
            .bank
            .slot_hashes()
            .0
            .first()
            .map(|(_, h)| *h)
            .unwrap_or(Hash::zero());

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time went backwards")
            .as_secs() as i64;

        match snapshot_archive::create_snapshot(
            &self.storage,
            self.current_slot,
            bank_hash,
            timestamp,
        ) {
            Ok(archive) => {
                // Save to ledger/snapshots/ directory
                let snapshot_dir = std::path::Path::new("ledger").join("snapshots");
                if std::fs::create_dir_all(&snapshot_dir).is_ok() {
                    let path = snapshot_dir.join(format!("snapshot-{}.bin", self.current_slot));
                    if let Err(e) = snapshot_archive::save_to_file(&archive, &path) {
                        warn!(error = %e, "failed to save snapshot file");
                    } else {
                        info!(
                            slot = self.current_slot,
                            accounts = archive.manifest.account_count,
                            path = %path.display(),
                            "snapshot created"
                        );
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, "failed to create snapshot");
            }
        }
    }

    /// Collect rent from non-exempt accounts at epoch boundary.
    /// Uses partition scheme (4096 partitions) to spread rent collection over epochs.
    fn collect_rent(&self, epoch: u64) {
        let partition = epoch % 4096;
        let mut rent_collected = 0u64;
        let mut accounts_closed = 0u64;

        let rent = &self.rent;
        // Approximate: epochs per year at 900ms slots, 432000 slots/epoch
        let epochs_per_year: f64 = 365.0 * 24.0 * 3600.0 * 1000.0 / (432_000.0 * 900.0);

        // Iterate accounts in this partition
        if let Ok(accounts) = self.storage.get_accounts_in_partition(partition, 4096) {
            for (address, mut account) in accounts {
                // Skip rent-exempt accounts
                if account.lamports >= rent.minimum_balance(account.data.len()) {
                    continue;
                }

                // Calculate rent due
                let rent_due = (rent.lamports_per_byte_year as f64 * account.data.len() as f64
                    / epochs_per_year) as u64;

                if rent_due == 0 {
                    continue;
                }

                if account.lamports <= rent_due {
                    // Account can't pay rent ??? close it
                    rent_collected += account.lamports;
                    account.lamports = 0;
                    account.data.clear();
                    accounts_closed += 1;
                } else {
                    account.lamports -= rent_due;
                    rent_collected += rent_due;
                }

                let _ = self
                    .storage
                    .put_account(&address, self.current_slot, &account);
            }
        }

        if rent_collected > 0 {
            // Burn collected rent (reduces total supply)
            self.bank.burn_fees(rent_collected);
            info!(
                epoch,
                partition, rent_collected, accounts_closed, "rent collected"
            );
        }
    }

    /// Calculate and distribute epoch inflation rewards (F7).
    fn distribute_epoch_rewards(&mut self, epoch: u64) {
        use nusantara_consensus::rewards::RewardsCalculator;

        let vote_states = self.bank.get_all_vote_states();
        let delegations = self.bank.get_all_delegations();

        if delegations.is_empty() {
            return;
        }

        // Use tracked total supply (initialized from genesis sum)
        let total_supply = self.bank.total_supply();
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
                        // Credit staker reward to stake account in storage
                        if let Ok(Some(mut account)) =
                            self.storage.get_account(&entry.stake_account)
                        {
                            account.lamports = account.lamports.saturating_add(entry.lamports);
                            if let Err(e) = self.storage.put_account(
                                &entry.stake_account,
                                self.current_slot,
                                &account,
                            ) {
                                warn!(error = %e, "failed to credit staker reward");
                            }
                            // Also update in-memory delegation stake
                            self.bank
                                .update_delegation_stake(&entry.stake_account, account.lamports);
                        }
                        total_distributed += entry.lamports;

                        // Credit validator commission to vote account
                        if entry.commission_lamports > 0 {
                            if let Ok(Some(mut vote_account)) =
                                self.storage.get_account(&entry.vote_account)
                            {
                                vote_account.lamports = vote_account
                                    .lamports
                                    .saturating_add(entry.commission_lamports);
                                if let Err(e) = self.storage.put_account(
                                    &entry.vote_account,
                                    self.current_slot,
                                    &vote_account,
                                ) {
                                    warn!(error = %e, "failed to credit commission");
                                }
                            }
                            total_distributed += entry.commission_lamports;
                        }
                    }
                }

                // Inflation increases total supply
                self.bank
                    .set_total_supply(total_supply.saturating_add(total_distributed));

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

    /// Process stake transitions at epoch boundary.
    /// Only cleans up fully-cooled-down delegations.
    /// Effective stake calculation (warmup/cooldown) is handled correctly
    /// in `bank.recalculate_epoch_stakes()` without mutating the base stake amount.
    fn process_stake_transitions(&self, epoch: u64) {
        let delegations = self.bank.get_all_delegations();
        let warmup_cooldown_rate =
            nusantara_stake_program::DEFAULT_WARMUP_COOLDOWN_RATE_BPS as f64 / 10_000.0;

        for (stake_account, delegation) in &delegations {
            // Remove fully cooled-down delegations
            if delegation.deactivation_epoch != u64::MAX {
                let epochs_deactivating = epoch.saturating_sub(delegation.deactivation_epoch);
                let effective_rate =
                    (1.0 - epochs_deactivating as f64 * warmup_cooldown_rate).max(0.0);
                if effective_rate == 0.0 {
                    // Fully cooled down ??? remove delegation from bank
                    // The stake has been returned to the stake account via withdraw
                    self.bank.remove_stake_delegation(stake_account);
                }
            }
        }
    }

    /// Flush storage to disk (memtables + WAL ??? SST files).
    pub fn flush_storage(&self) -> Result<(), ValidatorError> {
        self.storage.flush_all()?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn mempool(&self) -> Arc<Mempool> {
        Arc::clone(&self.mempool)
    }

    fn load_or_generate_keypair(cli: &Cli) -> Result<Keypair, ValidatorError> {
        if let Some(path) = &cli.identity {
            // --identity flag: load from explicit path
            info!(path, "loading identity keypair from explicit path");
            Self::load_keypair_from_path(path)
        } else {
            let keypair_path = Path::new(&cli.ledger_path).join("identity.key");
            if keypair_path.exists() {
                // Restart: load previously saved keypair
                info!(path = %keypair_path.display(), "loading existing identity keypair");
                Self::load_keypair_from_path(&keypair_path.to_string_lossy())
            } else {
                // First boot: generate and save
                let keypair = Keypair::generate();
                let mut bytes = Vec::with_capacity(KEYPAIR_SIZE);
                bytes.extend_from_slice(keypair.public_key().as_bytes());
                bytes.extend_from_slice(keypair.secret_key().as_bytes());
                if let Some(parent) = keypair_path.parent() {
                    let _ = std::fs::create_dir_all(parent);
                }
                match std::fs::write(&keypair_path, &bytes) {
                    Ok(()) => info!(path = %keypair_path.display(), "saved identity keypair"),
                    Err(e) => warn!(error = %e, "failed to save identity keypair"),
                }
                Ok(keypair)
            }
        }
    }

    fn resolve_public_host(host: &str) -> Result<std::net::IpAddr, ValidatorError> {
        // Try parsing as IP first
        if let Ok(ip) = host.parse::<std::net::IpAddr>() {
            return Ok(ip);
        }
        // Resolve hostname (e.g. Docker container name)
        let addrs: Vec<_> = std::net::ToSocketAddrs::to_socket_addrs(&(host, 0u16))
            .map_err(|e| {
                ValidatorError::NetworkInit(format!("failed to resolve public host '{host}': {e}"))
            })?
            .collect();
        addrs.first().map(|a| a.ip()).ok_or_else(|| {
            ValidatorError::NetworkInit(format!("public host '{host}' resolved to no addresses"))
        })
    }

    fn load_keypair_from_path(path: &str) -> Result<Keypair, ValidatorError> {
        let bytes = std::fs::read(path)?;
        if bytes.len() != KEYPAIR_SIZE {
            return Err(ValidatorError::Keypair(format!(
                "invalid keypair file size: expected {KEYPAIR_SIZE}, got {}",
                bytes.len()
            )));
        }
        Keypair::from_bytes(&bytes[..1952], &bytes[1952..])
            .map_err(|e| ValidatorError::Keypair(format!("invalid keypair: {e}")))
    }
}

fn hashv_bank_genesis(genesis_hash: &Hash) -> Hash {
    nusantara_crypto::hashv(&[Hash::zero().as_bytes(), genesis_hash.as_bytes()])
}
