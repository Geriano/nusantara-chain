use std::cell::Cell;
use std::collections::BTreeMap;

use nusantara_core::Account;
use nusantara_crypto::{hashv, Hash};
use nusantara_storage::Storage;
use tracing::instrument;

use crate::error::ConsensusError;

/// Merkle proof for a single account in the state tree.
///
/// Contains the sibling hashes from the leaf level up to the root,
/// along with a path indicating whether the current node was the
/// right child at each level.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StateMerkleProof {
    /// Sibling hashes from leaf level up to the root.
    pub siblings: Vec<Hash>,
    /// For each level, `true` if the current node was the right child.
    pub path: Vec<bool>,
    /// Leaf index in the sorted leaf array.
    pub leaf_index: usize,
    /// Total number of leaves when the proof was generated.
    pub total_leaves: usize,
}

/// Incremental state Merkle tree over all accounts.
///
/// Maintains a sorted mapping of account addresses to their leaf hashes.
/// The tree is a standard binary Merkle tree using power-of-two padding
/// (matching the existing `MerkleTree` in the crypto crate) but operates
/// over account state rather than transaction hashes.
///
/// Leaf hash = hashv(&[b"state_leaf", address_bytes, borsh(account)])
/// Internal nodes use a 0x01 domain separator to prevent second-preimage attacks.
pub struct StateTree {
    /// Sorted by address for deterministic ordering.
    /// address -> leaf_hash
    leaves: BTreeMap<Hash, Hash>,
    /// Cached root hash; invalidated on update/remove.
    cached_root: Cell<Option<Hash>>,
}

/// Hash a leaf node with a domain separator to avoid second-preimage attacks.
fn hash_leaf(data: &Hash) -> Hash {
    hashv(&[&[0x00], data.as_bytes()])
}

/// Hash two child nodes into a parent with a domain separator.
fn hash_internal(left: &Hash, right: &Hash) -> Hash {
    hashv(&[&[0x01], left.as_bytes(), right.as_bytes()])
}

/// Compute the leaf hash for an account at the given address.
///
/// The hash commits to both the address and the full account state,
/// ensuring that any change to any field (lamports, data, owner, etc.)
/// produces a different leaf hash.
fn account_leaf_hash(address: &Hash, account: &Account) -> Hash {
    let account_bytes = borsh::to_vec(account).expect("account serialization cannot fail");
    hashv(&[b"state_leaf", address.as_bytes(), &account_bytes])
}

impl Default for StateTree {
    fn default() -> Self {
        Self::new()
    }
}

impl StateTree {
    /// Create an empty state tree.
    pub fn new() -> Self {
        Self {
            leaves: BTreeMap::new(),
            cached_root: Cell::new(None),
        }
    }

    /// Number of accounts tracked in the tree.
    pub fn len(&self) -> usize {
        self.leaves.len()
    }

    /// Returns true if the tree has no accounts.
    pub fn is_empty(&self) -> bool {
        self.leaves.is_empty()
    }

    /// Update leaf hashes for changed accounts.
    ///
    /// Each delta replaces the leaf hash for the given address.
    /// New addresses are inserted; existing addresses are updated.
    #[instrument(skip_all, fields(delta_count = deltas.len()), level = "debug")]
    pub fn update(&mut self, deltas: &[(Hash, Account)]) {
        for (address, account) in deltas {
            let leaf = account_leaf_hash(address, account);
            self.leaves.insert(*address, leaf);
        }
        self.cached_root.set(None);
    }

    /// Remove an account from the tree.
    pub fn remove(&mut self, address: &Hash) {
        self.leaves.remove(address);
        self.cached_root.set(None);
    }

    /// Compute the Merkle root of all current leaves.
    ///
    /// Returns `Hash::zero()` for an empty tree.
    pub fn root(&self) -> Hash {
        if let Some(cached) = self.cached_root.get() {
            return cached;
        }

        if self.leaves.is_empty() {
            return Hash::zero();
        }

        let leaf_hashes: Vec<Hash> = self.leaves.values().copied().collect();
        let result = compute_root(&leaf_hashes);
        self.cached_root.set(Some(result));
        result
    }

    /// Generate a Merkle proof for a specific account address.
    ///
    /// Returns `None` if the address is not in the tree.
    pub fn proof(&self, address: &Hash) -> Option<StateMerkleProof> {
        if self.leaves.is_empty() {
            return None;
        }

        // Find the leaf index in sorted order.
        let leaf_index = self.leaves.keys().position(|k| k == address)?;
        let leaf_hashes: Vec<Hash> = self.leaves.values().copied().collect();
        let total_leaves = leaf_hashes.len();

        let padded_count = total_leaves.next_power_of_two();
        let total_nodes = 2 * padded_count - 1;
        let mut nodes = vec![Hash::zero(); total_nodes];

        // Fill leaf layer
        for (i, lh) in leaf_hashes.iter().enumerate() {
            nodes[padded_count - 1 + i] = hash_leaf(lh);
        }
        // Pad remaining with zero-hashed leaves
        for i in leaf_hashes.len()..padded_count {
            nodes[padded_count - 1 + i] = hash_leaf(&Hash::zero());
        }

        // Build internal nodes bottom-up
        for i in (0..padded_count - 1).rev() {
            let left = &nodes[2 * i + 1];
            let right = &nodes[2 * i + 2];
            nodes[i] = hash_internal(left, right);
        }

        // Walk from leaf to root collecting siblings
        let mut pos = padded_count - 1 + leaf_index;
        let mut siblings = Vec::new();
        let mut path = Vec::new();

        while pos > 0 {
            let sibling = if pos % 2 == 1 { pos + 1 } else { pos - 1 };
            siblings.push(nodes[sibling]);
            path.push(pos.is_multiple_of(2)); // true if current node is right child
            pos = (pos - 1) / 2;
        }

        Some(StateMerkleProof {
            siblings,
            path,
            leaf_index,
            total_leaves,
        })
    }

    /// Verify a proof against a known root.
    ///
    /// Recomputes the leaf hash from the address and account, then walks
    /// up the proof path to see if the final hash matches the root.
    pub fn verify_proof(
        root: &Hash,
        address: &Hash,
        account: &Account,
        proof: &StateMerkleProof,
    ) -> bool {
        let leaf = account_leaf_hash(address, account);
        let mut current = hash_leaf(&leaf);

        for (sibling, is_right) in proof.siblings.iter().zip(proof.path.iter()) {
            current = if *is_right {
                hash_internal(sibling, &current)
            } else {
                hash_internal(&current, sibling)
            };
        }

        current == *root
    }

    /// Initialize the state tree from all accounts currently in storage.
    ///
    /// Loads every account via the storage public API and builds the leaf map.
    #[instrument(skip_all, level = "info")]
    pub fn init_from_storage(storage: &Storage) -> Result<Self, ConsensusError> {
        let all_accounts = storage.get_all_accounts()?;

        let mut leaves = BTreeMap::new();
        for (address, account) in &all_accounts {
            let leaf = account_leaf_hash(address, account);
            leaves.insert(*address, leaf);
        }

        tracing::info!(
            account_count = leaves.len(),
            "state tree initialized from storage"
        );
        metrics::gauge!("nusantara_state_tree_leaf_count").set(leaves.len() as f64);

        Ok(Self {
            leaves,
            cached_root: Cell::new(None),
        })
    }
}

/// Compute the root hash from a slice of leaf hashes.
///
/// Uses the same power-of-two padding and hash domain separators
/// as the crypto crate's MerkleTree.
fn compute_root(leaf_hashes: &[Hash]) -> Hash {
    if leaf_hashes.is_empty() {
        return Hash::zero();
    }

    let padded_count = leaf_hashes.len().next_power_of_two();
    let total_nodes = 2 * padded_count - 1;
    let mut nodes = vec![Hash::zero(); total_nodes];

    // Fill leaf layer
    for (i, lh) in leaf_hashes.iter().enumerate() {
        nodes[padded_count - 1 + i] = hash_leaf(lh);
    }
    // Pad remaining with zero-hashed leaves
    for i in leaf_hashes.len()..padded_count {
        nodes[padded_count - 1 + i] = hash_leaf(&Hash::zero());
    }

    // Build internal nodes bottom-up
    for i in (0..padded_count - 1).rev() {
        let left = &nodes[2 * i + 1];
        let right = &nodes[2 * i + 2];
        nodes[i] = hash_internal(left, right);
    }

    nodes[0]
}

#[cfg(test)]
mod tests {
    use super::*;
    use nusantara_core::Account;
    use nusantara_crypto::hash;

    fn make_account(lamports: u64) -> Account {
        Account::new(lamports, hash(b"system"))
    }

    #[test]
    fn empty_tree_root_is_zero() {
        let tree = StateTree::new();
        assert_eq!(tree.root(), Hash::zero());
        assert!(tree.is_empty());
        assert_eq!(tree.len(), 0);
    }

    #[test]
    fn single_account_tree() {
        let mut tree = StateTree::new();
        let addr = hash(b"alice");
        let account = make_account(1000);
        tree.update(&[(addr, account.clone())]);

        assert_eq!(tree.len(), 1);
        let root = tree.root();
        assert_ne!(root, Hash::zero());

        // Proof should verify
        let proof = tree.proof(&addr).unwrap();
        assert!(StateTree::verify_proof(&root, &addr, &account, &proof));
    }

    #[test]
    fn deterministic_root() {
        let addr_a = hash(b"alice");
        let addr_b = hash(b"bob");
        let acc_a = make_account(1000);
        let acc_b = make_account(2000);

        let mut tree1 = StateTree::new();
        tree1.update(&[(addr_a, acc_a.clone()), (addr_b, acc_b.clone())]);

        let mut tree2 = StateTree::new();
        tree2.update(&[(addr_b, acc_b), (addr_a, acc_a)]);

        // Same accounts in different insertion order produce the same root
        // because BTreeMap sorts by key.
        assert_eq!(tree1.root(), tree2.root());
    }

    #[test]
    fn proof_verifies_for_all_accounts() {
        let addrs: Vec<Hash> = (0..10u8).map(|i| hash(&[i])).collect();
        let accounts: Vec<Account> = (0..10u64).map(|i| make_account(i * 100)).collect();

        let mut tree = StateTree::new();
        let deltas: Vec<(Hash, Account)> = addrs
            .iter()
            .zip(accounts.iter())
            .map(|(a, acc)| (*a, acc.clone()))
            .collect();
        tree.update(&deltas);

        let root = tree.root();

        for (addr, account) in addrs.iter().zip(accounts.iter()) {
            let proof = tree.proof(addr).unwrap();
            assert_eq!(proof.total_leaves, 10);
            assert!(
                StateTree::verify_proof(&root, addr, account, &proof),
                "proof failed for addr index in sorted order"
            );
        }
    }

    #[test]
    fn tampered_account_fails_verification() {
        let addr = hash(b"alice");
        let real_account = make_account(1000);
        let fake_account = make_account(9999);

        let mut tree = StateTree::new();
        tree.update(&[(addr, real_account)]);
        let root = tree.root();
        let proof = tree.proof(&addr).unwrap();

        assert!(!StateTree::verify_proof(
            &root,
            &addr,
            &fake_account,
            &proof
        ));
    }

    #[test]
    fn wrong_address_fails_verification() {
        let addr = hash(b"alice");
        let wrong_addr = hash(b"bob");
        let account = make_account(1000);

        let mut tree = StateTree::new();
        tree.update(&[(addr, account.clone())]);
        let root = tree.root();
        let proof = tree.proof(&addr).unwrap();

        assert!(!StateTree::verify_proof(
            &root,
            &wrong_addr,
            &account,
            &proof
        ));
    }

    #[test]
    fn incremental_update_matches_full_rebuild() {
        let addr_a = hash(b"alice");
        let addr_b = hash(b"bob");
        let addr_c = hash(b"carol");

        let acc_a = make_account(1000);
        let acc_b = make_account(2000);
        let acc_c = make_account(3000);

        // Build incrementally
        let mut incremental = StateTree::new();
        incremental.update(&[(addr_a, acc_a.clone()), (addr_b, acc_b.clone())]);
        incremental.update(&[(addr_c, acc_c.clone())]);

        // Build from scratch
        let mut full = StateTree::new();
        full.update(&[(addr_a, acc_a), (addr_b, acc_b), (addr_c, acc_c)]);

        assert_eq!(incremental.root(), full.root());
    }

    #[test]
    fn update_existing_account_changes_root() {
        let addr = hash(b"alice");
        let acc_v1 = make_account(1000);
        let acc_v2 = make_account(2000);

        let mut tree = StateTree::new();
        tree.update(&[(addr, acc_v1)]);
        let root1 = tree.root();

        tree.update(&[(addr, acc_v2)]);
        let root2 = tree.root();

        assert_ne!(root1, root2);
    }

    #[test]
    fn remove_account_changes_root() {
        let addr_a = hash(b"alice");
        let addr_b = hash(b"bob");
        let acc_a = make_account(1000);
        let acc_b = make_account(2000);

        let mut tree = StateTree::new();
        tree.update(&[(addr_a, acc_a.clone()), (addr_b, acc_b)]);
        let root_both = tree.root();

        tree.remove(&addr_b);
        let root_one = tree.root();

        assert_ne!(root_both, root_one);
        assert_eq!(tree.len(), 1);

        // Remaining account's proof still verifies
        let proof = tree.proof(&addr_a).unwrap();
        assert!(StateTree::verify_proof(&root_one, &addr_a, &acc_a, &proof));
    }

    #[test]
    fn remove_all_returns_to_zero_root() {
        let addr = hash(b"alice");
        let acc = make_account(1000);

        let mut tree = StateTree::new();
        tree.update(&[(addr, acc)]);
        assert_ne!(tree.root(), Hash::zero());

        tree.remove(&addr);
        assert_eq!(tree.root(), Hash::zero());
        assert!(tree.is_empty());
    }

    #[test]
    fn proof_for_missing_address_returns_none() {
        let addr = hash(b"alice");
        let missing = hash(b"bob");
        let acc = make_account(1000);

        let mut tree = StateTree::new();
        tree.update(&[(addr, acc)]);

        assert!(tree.proof(&missing).is_none());
    }

    #[test]
    fn proof_on_empty_tree_returns_none() {
        let tree = StateTree::new();
        assert!(tree.proof(&hash(b"alice")).is_none());
    }

    #[test]
    fn large_tree_proofs_verify() {
        let mut tree = StateTree::new();
        let mut deltas = Vec::new();
        for i in 0..100u64 {
            let addr = hash(&i.to_le_bytes());
            let acc = make_account(i * 1000);
            deltas.push((addr, acc));
        }
        tree.update(&deltas);

        let root = tree.root();
        for (addr, acc) in &deltas {
            let proof = tree.proof(addr).unwrap();
            assert!(StateTree::verify_proof(&root, addr, acc, &proof));
        }
    }

    #[test]
    fn non_power_of_two_leaf_count() {
        // 7 leaves -- not a power of two, tests padding behavior
        let mut tree = StateTree::new();
        let mut deltas = Vec::new();
        for i in 0..7u64 {
            let addr = hash(&i.to_le_bytes());
            let acc = make_account(i * 100);
            deltas.push((addr, acc));
        }
        tree.update(&deltas);

        let root = tree.root();
        for (addr, acc) in &deltas {
            let proof = tree.proof(addr).unwrap();
            assert!(StateTree::verify_proof(&root, addr, acc, &proof));
        }
    }
}
