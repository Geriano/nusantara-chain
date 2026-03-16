use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
use nusantara_crypto::Hash;

use crate::contact_info::ContactInfo;
use crate::crds_value::{CrdsData, CrdsValue, CrdsValueLabel};
use crate::error::GossipError;

pub struct CrdsEntry {
    pub value: CrdsValue,
    pub insert_order: u64,
}

pub struct CrdsTable {
    entries: DashMap<CrdsValueLabel, CrdsEntry>,
    cursor: AtomicU64,
}

impl CrdsTable {
    pub fn new() -> Self {
        Self {
            entries: DashMap::new(),
            cursor: AtomicU64::new(0),
        }
    }

    /// Insert a CRDS value. Returns Ok(Some(old)) if replaced, Ok(None) if new,
    /// or Err if the new value is stale (older wallclock).
    pub fn insert(&self, value: CrdsValue) -> Result<Option<CrdsValue>, GossipError> {
        let label = value.label();
        let wallclock = value.wallclock();

        if let Some(existing) = self.entries.get(&label)
            && existing.value.wallclock() >= wallclock
        {
            return Err(GossipError::StaleValue {
                value_wallclock: wallclock,
                existing_wallclock: existing.value.wallclock(),
            });
        }

        let order = self.cursor.fetch_add(1, Ordering::Relaxed);
        let old = self.entries.insert(
            label,
            CrdsEntry {
                value,
                insert_order: order,
            },
        );

        metrics::counter!("gossip_crds_inserts_total").increment(1);
        Ok(old.map(|e| e.value))
    }

    pub fn get(&self, label: &CrdsValueLabel) -> Option<CrdsValue> {
        self.entries.get(label).map(|e| e.value.clone())
    }

    pub fn all_contact_infos(&self) -> Vec<ContactInfo> {
        self.entries
            .iter()
            .filter_map(|entry| {
                if let CrdsData::ContactInfo(ci) = &entry.value().value.data {
                    Some(ci.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn values_since(&self, cursor: u64) -> Vec<CrdsValue> {
        self.entries
            .iter()
            .filter(|e| e.value().insert_order >= cursor)
            .map(|e| e.value().value.clone())
            .collect()
    }

    pub fn current_cursor(&self) -> u64 {
        self.cursor.load(Ordering::Relaxed)
    }

    pub fn purge_old(&self, min_wallclock: u64) -> usize {
        let stale_labels: Vec<CrdsValueLabel> = self
            .entries
            .iter()
            .filter(|e| e.value().value.wallclock() < min_wallclock)
            .map(|e| e.key().clone())
            .collect();

        let count = stale_labels.len();
        for label in stale_labels {
            self.entries.remove(&label);
        }

        if count > 0 {
            metrics::counter!("gossip_crds_purged_total").increment(count as u64);
        }
        count
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn get_contact_info(&self, identity: &Hash) -> Option<ContactInfo> {
        let label = CrdsValueLabel::ContactInfo(*identity);
        self.entries.get(&label).and_then(|e| {
            if let CrdsData::ContactInfo(ci) = &e.value.data {
                Some(ci.clone())
            } else {
                None
            }
        })
    }

    pub fn all_labels(&self) -> Vec<CrdsValueLabel> {
        self.entries.iter().map(|e| e.key().clone()).collect()
    }
}

impl Default for CrdsTable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nusantara_crypto::Keypair;

    fn make_contact_value(kp: &Keypair, wallclock: u64) -> CrdsValue {
        let ci = ContactInfo::new(
            kp.public_key().clone(),
            "127.0.0.1:8000".parse().unwrap(),
            "127.0.0.1:8003".parse().unwrap(),
            "127.0.0.1:8004".parse().unwrap(),
            "127.0.0.1:8001".parse().unwrap(),
            "127.0.0.1:8002".parse().unwrap(),
            1,
            wallclock,
        );
        CrdsValue::new_signed(CrdsData::ContactInfo(ci), kp)
    }

    #[test]
    fn insert_and_get() {
        let table = CrdsTable::new();
        let kp = Keypair::generate();
        let value = make_contact_value(&kp, 1000);
        let label = value.label();

        assert!(table.insert(value.clone()).unwrap().is_none());
        assert_eq!(table.get(&label).unwrap(), value);
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn update_newer_wallclock() {
        let table = CrdsTable::new();
        let kp = Keypair::generate();
        let v1 = make_contact_value(&kp, 1000);
        let v2 = make_contact_value(&kp, 2000);

        table.insert(v1.clone()).unwrap();
        let old = table.insert(v2.clone()).unwrap();
        assert_eq!(old.unwrap(), v1);
        assert_eq!(table.get(&v2.label()).unwrap(), v2);
    }

    #[test]
    fn reject_stale_value() {
        let table = CrdsTable::new();
        let kp = Keypair::generate();
        let v1 = make_contact_value(&kp, 2000);
        let v2 = make_contact_value(&kp, 1000);

        table.insert(v1).unwrap();
        let result = table.insert(v2);
        assert!(result.is_err());
    }

    #[test]
    fn all_contact_infos() {
        let table = CrdsTable::new();
        for _ in 0..5 {
            let kp = Keypair::generate();
            table.insert(make_contact_value(&kp, 1000)).unwrap();
        }
        assert_eq!(table.all_contact_infos().len(), 5);
    }

    #[test]
    fn values_since_cursor() {
        let table = CrdsTable::new();
        let kp1 = Keypair::generate();
        let kp2 = Keypair::generate();

        table.insert(make_contact_value(&kp1, 1000)).unwrap();
        let cursor = table.current_cursor();
        table.insert(make_contact_value(&kp2, 1001)).unwrap();

        let new_values = table.values_since(cursor);
        assert_eq!(new_values.len(), 1);
    }

    #[test]
    fn purge_old() {
        let table = CrdsTable::new();
        let kp1 = Keypair::generate();
        let kp2 = Keypair::generate();

        table.insert(make_contact_value(&kp1, 100)).unwrap();
        table.insert(make_contact_value(&kp2, 2000)).unwrap();

        let purged = table.purge_old(1000);
        assert_eq!(purged, 1);
        assert_eq!(table.len(), 1);
    }
}
