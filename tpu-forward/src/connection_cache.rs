use std::net::SocketAddr;
use std::sync::Arc;

use dashmap::DashMap;
use quinn::Connection;

pub struct ConnectionCache {
    connections: DashMap<SocketAddr, Arc<Connection>>,
}

impl ConnectionCache {
    pub fn new() -> Self {
        Self {
            connections: DashMap::new(),
        }
    }

    /// Get a cached connection or return None.
    pub fn get(&self, addr: &SocketAddr) -> Option<Arc<Connection>> {
        self.connections.get(addr).map(|c| Arc::clone(c.value()))
    }

    /// Cache a connection.
    pub fn insert(&self, addr: SocketAddr, conn: Arc<Connection>) {
        self.connections.insert(addr, conn);
    }

    /// Remove a connection (e.g. on error).
    pub fn remove(&self, addr: &SocketAddr) {
        self.connections.remove(addr);
    }

    /// Remove stale connections.
    pub fn prune_closed(&self) {
        self.connections.retain(|_, conn| {
            conn.close_reason().is_none()
        });
    }

    pub fn len(&self) -> usize {
        self.connections.len()
    }

    pub fn is_empty(&self) -> bool {
        self.connections.is_empty()
    }
}

impl Default for ConnectionCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ConnectionCache basic tests (no real QUIC connections in unit tests)

    #[test]
    fn empty_cache() {
        let cache = ConnectionCache::new();
        let addr: SocketAddr = "127.0.0.1:8000".parse().unwrap();
        assert!(cache.get(&addr).is_none());
        assert!(cache.is_empty());
    }
}
