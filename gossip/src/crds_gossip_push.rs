use std::collections::HashSet;
use std::net::SocketAddr;

use nusantara_core::native_token::const_parse_u64;
use nusantara_crypto::Hash;
use parking_lot::RwLock;

use crate::contact_info::ContactInfo;
use crate::crds::CrdsTable;
use crate::crds_value::CrdsValue;
use crate::weighted_shuffle::weighted_shuffle;

pub const PUSH_FANOUT: u64 = const_parse_u64(env!("NUSA_GOSSIP_PUSH_FANOUT"));
pub const MAX_CRDS_VALUES_PER_PUSH: u64 =
    const_parse_u64(env!("NUSA_GOSSIP_MAX_CRDS_VALUES_PER_PUSH"));

pub struct CrdsGossipPush {
    my_identity: Hash,
    push_cursor: RwLock<u64>,
    prune_set: RwLock<HashSet<(Hash, Hash)>>,
}

impl CrdsGossipPush {
    pub fn new(my_identity: Hash) -> Self {
        Self {
            my_identity,
            push_cursor: RwLock::new(0),
            prune_set: RwLock::new(HashSet::new()),
        }
    }

    /// Collect new CRDS values since last push and select target peers.
    /// Returns (target_addr, values_to_push) pairs.
    pub fn new_push_messages(
        &self,
        crds: &CrdsTable,
        peers: &[(ContactInfo, u64)],
        seed: &Hash,
    ) -> Vec<(SocketAddr, Vec<CrdsValue>)> {
        if peers.is_empty() {
            return Vec::new();
        }

        let cursor = *self.push_cursor.read();
        let new_values = crds.values_since(cursor);
        *self.push_cursor.write() = crds.current_cursor();

        if new_values.is_empty() {
            return Vec::new();
        }

        // Limit values per push
        let values: Vec<CrdsValue> = new_values
            .into_iter()
            .take(MAX_CRDS_VALUES_PER_PUSH as usize)
            .collect();

        // Select push targets via stake-weighted shuffle
        let stake_peers: Vec<(Hash, u64)> = peers
            .iter()
            .map(|(ci, stake)| (ci.identity, *stake))
            .collect();
        let indices = weighted_shuffle(&stake_peers, seed);
        let prune_set = self.prune_set.read();

        let targets: Vec<SocketAddr> = indices
            .into_iter()
            .filter(|&i| {
                let peer_id = peers[i].0.identity;
                peer_id != self.my_identity
                    && !values.iter().any(|v| {
                        prune_set.contains(&(peer_id, v.origin()))
                    })
            })
            .take(PUSH_FANOUT as usize)
            .map(|i| peers[i].0.gossip_addr.0)
            .collect();

        targets
            .into_iter()
            .map(|addr| (addr, values.clone()))
            .collect()
    }

    /// Record a prune: peer `pruner` doesn't want values from `origin`.
    pub fn process_prune(&self, pruner: Hash, origins: &[Hash]) {
        let mut prune_set = self.prune_set.write();
        for origin in origins {
            prune_set.insert((pruner, *origin));
        }
        metrics::counter!("nusantara_gossip_prune_messages_total").increment(1);
    }

    /// Reset prune state (called periodically).
    pub fn clear_prunes(&self) {
        self.prune_set.write().clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nusantara_crypto::{Keypair, hash};
    use crate::crds_value::{CrdsData, CrdsValue};

    fn make_peer(i: i32) -> (ContactInfo, u64) {
        let kp = Keypair::generate();
        let ci = ContactInfo::new(
            kp.public_key().clone(),
            format!("127.0.0.1:{}", 8000 + i).parse().unwrap(),
            format!("127.0.0.1:{}", 9000 + i).parse().unwrap(),
            format!("127.0.0.1:{}", 9100 + i).parse().unwrap(),
            format!("127.0.0.1:{}", 9200 + i).parse().unwrap(),
            format!("127.0.0.1:{}", 9300 + i).parse().unwrap(),
            1,
            1000,
        );
        (ci, 1000)
    }

    #[test]
    fn config_values() {
        assert_eq!(PUSH_FANOUT, 6);
        assert_eq!(MAX_CRDS_VALUES_PER_PUSH, 10);
    }

    #[test]
    fn push_empty_peers() {
        let push = CrdsGossipPush::new(hash(b"me"));
        let crds = CrdsTable::new();
        let msgs = push.new_push_messages(&crds, &[], &hash(b"seed"));
        assert!(msgs.is_empty());
    }

    #[test]
    fn push_with_new_values() {
        let kp = Keypair::generate();
        let my_identity = kp.address();
        let push = CrdsGossipPush::new(my_identity);
        let crds = CrdsTable::new();

        // Insert a value into CRDS
        let ci = ContactInfo::new(
            kp.public_key().clone(),
            "127.0.0.1:8000".parse().unwrap(),
            "127.0.0.1:8003".parse().unwrap(),
            "127.0.0.1:8004".parse().unwrap(),
            "127.0.0.1:8001".parse().unwrap(),
            "127.0.0.1:8002".parse().unwrap(),
            1,
            1000,
        );
        crds.insert(CrdsValue::new_signed(CrdsData::ContactInfo(ci), &kp))
            .unwrap();

        let peers: Vec<(ContactInfo, u64)> = (0..10).map(make_peer).collect();
        let msgs = push.new_push_messages(&crds, &peers, &hash(b"seed"));
        assert!(!msgs.is_empty());
        assert!(msgs.len() <= PUSH_FANOUT as usize);
    }

    #[test]
    fn prune_excludes_peer() {
        let kp = Keypair::generate();
        let my_identity = kp.address();
        let push = CrdsGossipPush::new(my_identity);

        let peer = make_peer(0);
        let peer_id = peer.0.identity;

        push.process_prune(peer_id, &[my_identity]);

        let prune_set = push.prune_set.read();
        assert!(prune_set.contains(&(peer_id, my_identity)));
    }
}
