use serde::{Deserialize, Serialize};

// ── Health ──

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
    pub slot: u64,
    pub identity: String,
    pub root_slot: u64,
    pub behind_slots: u64,
    pub peer_count: usize,
    pub epoch: u64,
    pub epoch_progress_pct: f64,
    pub consecutive_skips: u64,
    pub total_active_stake: u64,
}

// ── Slot ──

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlotResponse {
    pub slot: u64,
    pub latest_stored_slot: Option<u64>,
    pub latest_root: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockhashResponse {
    pub blockhash: String,
    pub slot: u64,
}

// ── Block ──

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockResponse {
    pub slot: u64,
    pub parent_slot: u64,
    pub parent_hash: String,
    pub block_hash: String,
    pub timestamp: i64,
    pub validator: String,
    pub transaction_count: u64,
    pub merkle_root: String,
}

// ── Account ──

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountResponse {
    pub address: String,
    pub lamports: u64,
    pub nusa: f64,
    pub owner: String,
    pub executable: bool,
    pub rent_epoch: u64,
    pub data_len: usize,
}

// ── Transaction ──

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionStatusResponse {
    pub signature: String,
    pub slot: u64,
    pub status: String,
    pub fee: u64,
    pub pre_balances: Vec<u64>,
    pub post_balances: Vec<u64>,
    pub compute_units_consumed: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendTransactionRequest {
    pub transaction: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendTransactionResponse {
    pub signature: String,
    #[serde(default)]
    pub status: String,
}

// ── Airdrop ──

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AirdropRequest {
    pub address: String,
    pub lamports: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AirdropResponse {
    pub signature: String,
}

// ── Validators ──

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidatorsResponse {
    pub total_active_stake: u64,
    pub validators: Vec<ValidatorEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidatorEntry {
    pub identity: String,
    pub vote_account: String,
    pub commission: u8,
    pub active_stake: u64,
    pub last_vote: Option<u64>,
    pub root_slot: Option<u64>,
}

// ── Epoch ──

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EpochInfoResponse {
    pub epoch: u64,
    pub slot_index: u64,
    pub slots_in_epoch: u64,
    pub absolute_slot: u64,
    pub timestamp: i64,
    pub leader_schedule_epoch: u64,
}

// ── Leader Schedule ──

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaderScheduleResponse {
    pub epoch: u64,
    pub schedule: Vec<LeaderSlotEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaderSlotEntry {
    pub slot: u64,
    pub leader: String,
}
