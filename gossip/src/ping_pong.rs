use std::time::Instant;

use dashmap::DashMap;
use nusantara_crypto::{Hash, Keypair, hash as crypto_hash, hashv};

use crate::protocol::{PingMessage, PongMessage};

pub struct PingCache {
    verified: DashMap<Hash, Instant>,
    ttl: std::time::Duration,
}

impl PingCache {
    pub fn new(ttl_ms: u64) -> Self {
        Self {
            verified: DashMap::new(),
            ttl: std::time::Duration::from_millis(ttl_ms),
        }
    }

    pub fn is_verified(&self, identity: &Hash) -> bool {
        if let Some(entry) = self.verified.get(identity) {
            entry.elapsed() < self.ttl
        } else {
            false
        }
    }

    pub fn mark_verified(&self, identity: Hash) {
        self.verified.insert(identity, Instant::now());
    }

    pub fn create_ping(&self, keypair: &Keypair) -> PingMessage {
        let token = crypto_hash(&rand::random::<[u8; 32]>());
        let sig = keypair.sign(token.as_bytes());
        PingMessage {
            from: keypair.address(),
            token,
            signature: sig,
        }
    }

    pub fn create_pong(keypair: &Keypair, ping: &PingMessage) -> PongMessage {
        let token_hash = crypto_hash(ping.token.as_bytes());
        let sign_data = hashv(&[b"pong", token_hash.as_bytes()]);
        let sig = keypair.sign(sign_data.as_bytes());
        PongMessage {
            from: keypair.address(),
            token_hash,
            signature: sig,
        }
    }

    pub fn verify_pong(&self, pong: &PongMessage, ping_token: &Hash) -> bool {
        let expected_hash = crypto_hash(ping_token.as_bytes());
        if pong.token_hash != expected_hash {
            return false;
        }
        // Signature verification would require the public key from CRDS
        // For now, just check the token hash matches
        true
    }

    pub fn purge_expired(&self) {
        self.verified.retain(|_, instant| instant.elapsed() < self.ttl);
    }

    pub fn len(&self) -> usize {
        self.verified.len()
    }

    pub fn is_empty(&self) -> bool {
        self.verified.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_ping_and_pong() {
        let kp1 = Keypair::generate();
        let kp2 = Keypair::generate();
        let cache = PingCache::new(60_000);

        let ping = cache.create_ping(&kp1);
        assert_eq!(ping.from, kp1.address());

        let pong = PingCache::create_pong(&kp2, &ping);
        assert_eq!(pong.from, kp2.address());

        assert!(cache.verify_pong(&pong, &ping.token));
    }

    #[test]
    fn wrong_token_fails() {
        let kp1 = Keypair::generate();
        let kp2 = Keypair::generate();
        let cache = PingCache::new(60_000);

        let ping = cache.create_ping(&kp1);
        let pong = PingCache::create_pong(&kp2, &ping);

        let wrong_token = crypto_hash(b"wrong");
        assert!(!cache.verify_pong(&pong, &wrong_token));
    }

    #[test]
    fn verified_cache() {
        let cache = PingCache::new(60_000);
        let identity = crypto_hash(b"node");

        assert!(!cache.is_verified(&identity));
        cache.mark_verified(identity);
        assert!(cache.is_verified(&identity));
    }

    #[test]
    fn expired_entry_not_verified() {
        let cache = PingCache::new(0); // 0ms TTL
        let identity = crypto_hash(b"node");

        cache.mark_verified(identity);
        std::thread::sleep(std::time::Duration::from_millis(1));
        assert!(!cache.is_verified(&identity));
    }
}
