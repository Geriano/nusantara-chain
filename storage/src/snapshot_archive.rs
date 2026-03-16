use std::io::{Read, Write};
use std::path::Path;

use borsh::{BorshDeserialize, BorshSerialize};
use nusantara_core::Account;
use nusantara_crypto::Hash;
use rocksdb::IteratorMode;

use crate::cf::CF_ACCOUNT_INDEX;
use crate::error::StorageError;
use crate::snapshot::SnapshotManifest;
use crate::storage::Storage;

/// A snapshot archive containing all state needed to bootstrap a validator.
#[derive(Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct SnapshotArchive {
    pub manifest: SnapshotManifest,
    pub accounts: Vec<(Hash, Account)>,
}

/// Create a snapshot of the current state.
pub fn create_snapshot(
    storage: &Storage,
    slot: u64,
    bank_hash: Hash,
    timestamp: i64,
) -> Result<SnapshotArchive, StorageError> {
    // Collect all current accounts from the account index
    let cf_index = storage
        .db
        .cf_handle(CF_ACCOUNT_INDEX)
        .ok_or(StorageError::CfNotFound(CF_ACCOUNT_INDEX))?;

    let mut accounts = Vec::new();

    let iter = storage.db.iterator_cf(cf_index, IteratorMode::Start);
    for item in iter {
        let (key, _value) = item.map_err(StorageError::RocksDb)?;
        if key.len() != 64 {
            continue;
        }
        let address = Hash::new(
            key[..64]
                .try_into()
                .map_err(|_| StorageError::Corruption("invalid address".into()))?,
        );

        if let Some(account) = storage.get_account(&address)? {
            accounts.push((address, account));
        }
    }

    let manifest = SnapshotManifest {
        slot,
        bank_hash,
        account_count: accounts.len() as u64,
        timestamp,
    };

    // Store manifest in storage
    storage.put_snapshot(&manifest)?;

    Ok(SnapshotArchive { manifest, accounts })
}

/// Save a snapshot archive to a file (borsh-serialized).
pub fn save_to_file(archive: &SnapshotArchive, path: &Path) -> Result<(), StorageError> {
    let bytes = borsh::to_vec(archive).map_err(|e| StorageError::Serialization(e.to_string()))?;
    let mut file = std::fs::File::create(path).map_err(|e| StorageError::Io(e.to_string()))?;
    file.write_all(&bytes)
        .map_err(|e| StorageError::Io(e.to_string()))?;
    Ok(())
}

/// Load a snapshot archive from a file.
pub fn load_from_file(path: &Path) -> Result<SnapshotArchive, StorageError> {
    let mut file = std::fs::File::open(path).map_err(|e| StorageError::Io(e.to_string()))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|e| StorageError::Io(e.to_string()))?;
    let archive = SnapshotArchive::try_from_slice(&bytes)
        .map_err(|e| StorageError::Deserialization(e.to_string()))?;
    Ok(archive)
}

/// Bootstrap storage from a snapshot archive.
pub fn bootstrap_from_snapshot(
    storage: &Storage,
    archive: &SnapshotArchive,
) -> Result<(), StorageError> {
    // Write all accounts to storage at the snapshot slot
    let slot = archive.manifest.slot;
    for (address, account) in &archive.accounts {
        storage.put_account(address, slot, account)?;
    }

    // Store the snapshot manifest
    storage.put_snapshot(&archive.manifest)?;

    Ok(())
}

/// Restore state from a snapshot archive.
///
/// This performs a full state restore: writes all accounts, stores the manifest,
/// sets the snapshot slot as a finalized root, and records the bank hash.
/// After calling this, the validator can resume from the snapshot slot
/// without needing to replay from genesis.
pub fn restore_snapshot(storage: &Storage, archive: &SnapshotArchive) -> Result<(), StorageError> {
    let slot = archive.manifest.slot;

    // 1. Write all accounts to storage at the snapshot slot
    for (address, account) in &archive.accounts {
        storage.put_account(address, slot, account)?;
    }

    // 2. Store the snapshot manifest
    storage.put_snapshot(&archive.manifest)?;

    // 3. Mark the snapshot slot as a finalized root
    storage.set_root(slot)?;

    // 4. Store the bank hash so the validator can reconstruct parent state
    storage.put_bank_hash(slot, &archive.manifest.bank_hash)?;

    Ok(())
}

/// Find the latest snapshot file in the given directory.
///
/// Scans for files matching the pattern `snapshot-{slot}.bin` and returns
/// the path to the one with the highest slot number.
pub fn find_latest_snapshot_file(dir: &Path) -> Option<std::path::PathBuf> {
    let read_dir = std::fs::read_dir(dir).ok()?;
    let mut best: Option<(u64, std::path::PathBuf)> = None;

    for entry in read_dir.flatten() {
        let path = entry.path();
        if let Some(name) = path.file_name().and_then(|n| n.to_str())
            && let Some(slot_str) = name
                .strip_prefix("snapshot-")
                .and_then(|s| s.strip_suffix(".bin"))
            && let Ok(slot) = slot_str.parse::<u64>()
        {
            match &best {
                Some((best_slot, _)) if slot > *best_slot => {
                    best = Some((slot, path));
                }
                None => {
                    best = Some((slot, path));
                }
                _ => {}
            }
        }
    }

    best.map(|(_, path)| path)
}
