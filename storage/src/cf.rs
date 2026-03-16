use rocksdb::{ColumnFamilyDescriptor, Options, SliceTransform};

pub const CF_DEFAULT: &str = "default";
pub const CF_ACCOUNTS: &str = "accounts";
pub const CF_ACCOUNT_INDEX: &str = "account_index";
pub const CF_BLOCKS: &str = "blocks";
pub const CF_TRANSACTIONS: &str = "transactions";
pub const CF_ADDRESS_SIGNATURES: &str = "address_signatures";
pub const CF_SLOT_META: &str = "slot_meta";
pub const CF_DATA_SHREDS: &str = "data_shreds";
pub const CF_CODE_SHREDS: &str = "code_shreds";
pub const CF_BANK_HASHES: &str = "bank_hashes";
pub const CF_ROOTS: &str = "roots";
pub const CF_SLOT_HASHES: &str = "slot_hashes";
pub const CF_SYSVARS: &str = "sysvars";
pub const CF_SNAPSHOTS: &str = "snapshots";
pub const CF_OWNER_INDEX: &str = "owner_index";
pub const CF_PROGRAM_INDEX: &str = "program_index";
pub const CF_SLASHES: &str = "slashes";

/// Hash size in bytes (SHA3-512 = 64 bytes).
const HASH_BYTES: usize = 64;

/// Slot size in bytes (u64 big-endian = 8 bytes).
const SLOT_BYTES: usize = 8;

pub const ALL_CF_NAMES: &[&str] = &[
    CF_DEFAULT,
    CF_ACCOUNTS,
    CF_ACCOUNT_INDEX,
    CF_BLOCKS,
    CF_TRANSACTIONS,
    CF_ADDRESS_SIGNATURES,
    CF_SLOT_META,
    CF_DATA_SHREDS,
    CF_CODE_SHREDS,
    CF_BANK_HASHES,
    CF_ROOTS,
    CF_SLOT_HASHES,
    CF_SYSVARS,
    CF_SNAPSHOTS,
    CF_OWNER_INDEX,
    CF_PROGRAM_INDEX,
    CF_SLASHES,
];

pub fn cf_descriptors() -> Vec<ColumnFamilyDescriptor> {
    ALL_CF_NAMES
        .iter()
        .map(|name| {
            let mut opts = Options::default();
            match *name {
                CF_ACCOUNTS => {
                    // Key: Hash(64) ++ slot(8) — prefix by address for iteration
                    opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(HASH_BYTES));
                }
                CF_ADDRESS_SIGNATURES => {
                    // Key: Hash(64) ++ slot(8) ++ tx_index(4) — prefix by address
                    opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(HASH_BYTES));
                }
                CF_DATA_SHREDS | CF_CODE_SHREDS => {
                    // Key: slot(8) ++ shred_index(4) — prefix by slot
                    opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(SLOT_BYTES));
                }
                CF_OWNER_INDEX | CF_PROGRAM_INDEX => {
                    // Key: owner/program_hash(64) ++ account_address(64) — prefix by owner/program
                    opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(HASH_BYTES));
                }
                CF_SLASHES => {
                    // Key: validator_hash(64) ++ slot(8 BE) — prefix by validator
                    opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(HASH_BYTES));
                }
                _ => {}
            }
            ColumnFamilyDescriptor::new(*name, opts)
        })
        .collect()
}
