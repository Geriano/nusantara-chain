//! LRU cache for compiled WASM modules.
//!
//! Compilation (parsing + validation) of a WASM module is expensive relative to
//! execution. The [`ProgramCache`] keeps the most-recently-used compiled
//! [`Module`]s in memory so that repeated invocations of the same program skip
//! the compilation step entirely.
//!
//! Thread safety is provided by a [`parking_lot::Mutex`] around the inner LRU
//! map. The lock is held only for the duration of a single get/put operation --
//! never across an `.await` point -- so there is no risk of async deadlocks.

use std::num::NonZeroUsize;

use lru::LruCache;
use parking_lot::Mutex;
use wasmi::Module;

use nusantara_crypto::Hash;

use crate::config::PROGRAM_CACHE_CAPACITY;

/// An LRU cache mapping program account hashes to compiled wasmi [`Module`]s.
pub struct ProgramCache {
    cache: Mutex<LruCache<Hash, Module>>,
}

impl ProgramCache {
    /// Create a new cache with the given capacity.
    ///
    /// # Panics
    ///
    /// Panics if `capacity` is zero (guarded by `NonZeroUsize`).
    pub fn new(capacity: usize) -> Self {
        let cap = NonZeroUsize::new(capacity)
            .unwrap_or_else(|| NonZeroUsize::new(1).expect("1 is non-zero"));
        Self {
            cache: Mutex::new(LruCache::new(cap)),
        }
    }

    /// Create a new cache using the default capacity from `config.toml`.
    pub fn with_default_capacity() -> Self {
        Self::new(PROGRAM_CACHE_CAPACITY)
    }

    /// Retrieve a compiled module by its program hash.
    ///
    /// Promotes the entry to the head of the LRU list if present.
    pub fn get(&self, key: &Hash) -> Option<Module> {
        self.cache.lock().get(key).cloned()
    }

    /// Insert a compiled module into the cache.
    ///
    /// If the cache is at capacity the least-recently-used entry is evicted.
    pub fn insert(&self, key: Hash, module: Module) {
        self.cache.lock().put(key, module);
    }

    /// Remove a specific module from the cache (e.g. after a program upgrade).
    pub fn invalidate(&self, key: &Hash) {
        self.cache.lock().pop(key);
    }

    /// Remove all entries from the cache.
    pub fn clear(&self) {
        self.cache.lock().clear();
    }

    /// Return the number of modules currently cached.
    pub fn len(&self) -> usize {
        self.cache.lock().len()
    }

    /// Return `true` if the cache contains no entries.
    pub fn is_empty(&self) -> bool {
        self.cache.lock().is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nusantara_crypto::hash;

    #[test]
    fn new_cache_is_empty() {
        let cache = ProgramCache::new(10);
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn invalidate_missing_key_is_noop() {
        let cache = ProgramCache::new(10);
        let key = hash(b"nonexistent");
        cache.invalidate(&key); // should not panic
        assert!(cache.is_empty());
    }

    #[test]
    fn clear_on_empty_is_noop() {
        let cache = ProgramCache::new(10);
        cache.clear();
        assert!(cache.is_empty());
    }

    #[test]
    fn default_capacity() {
        let cache = ProgramCache::with_default_capacity();
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn get_missing_returns_none() {
        let cache = ProgramCache::new(10);
        let key = hash(b"missing");
        assert!(cache.get(&key).is_none());
    }
}
