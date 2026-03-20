use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use nusantara_core::native_token::const_parse_u64;
use tokio::net::UdpSocket;
use tokio::sync::watch;
use tracing::{debug, info, instrument};

use crate::protocol::{RepairRequest, TurbineMessage};
use crate::shred_collector::ShredCollector;

pub const REPAIR_INTERVAL_MS: u64 = const_parse_u64(env!("NUSA_TURBINE_REPAIR_INTERVAL_MS"));
pub const MAX_REPAIR_BATCH_REQUEST: u64 =
    const_parse_u64(env!("NUSA_TURBINE_MAX_REPAIR_BATCH_REQUEST"));

/// Slots older than this relative to current_slot are evicted from the
/// ShredCollector on each repair tick.
const MAX_REPAIR_SLOT_AGE: u64 = 64;

/// Maximum number of slots to request repairs for per tick.
/// With 200ms tick interval, this paces repair to 20 slots/second.
const MAX_REPAIR_SLOTS_PER_TICK: usize = 16;

pub struct RepairService {
    socket: Arc<UdpSocket>,
    collector: Arc<ShredCollector>,
    current_slot: Arc<AtomicU64>,
}

impl RepairService {
    pub fn new(
        socket: Arc<UdpSocket>,
        collector: Arc<ShredCollector>,
        current_slot: Arc<AtomicU64>,
    ) -> Self {
        Self {
            socket,
            collector,
            current_slot,
        }
    }

    #[instrument(skip(self, repair_peers_fn, shutdown), name = "repair_service")]
    pub async fn run<F>(
        self,
        repair_peers_fn: F,
        mut shutdown: watch::Receiver<bool>,
    ) where
        F: Fn() -> Vec<SocketAddr>,
    {
        let interval = tokio::time::Duration::from_millis(REPAIR_INTERVAL_MS);
        let mut tick = tokio::time::interval(interval);

        loop {
            tokio::select! {
                biased;
                _ = tick.tick() => {
                    let current = self.current_slot.load(Ordering::Relaxed);
                    let evicted = self.collector.cleanup_old_slots(current, MAX_REPAIR_SLOT_AGE);
                    if evicted > 0 {
                        debug!(evicted, current, "evicted stale slots from shred collector");
                    }

                    let mut slots = self.collector.tracked_slots();
                    // Prioritize most recent slots, cap per tick to prevent burst
                    slots.sort_unstable_by(|a, b| b.cmp(a));
                    let deferred = slots.len().saturating_sub(MAX_REPAIR_SLOTS_PER_TICK);
                    if deferred > 0 {
                        metrics::counter!("nusantara_turbine_repair_slots_deferred").increment(deferred as u64);
                    }
                    slots.truncate(MAX_REPAIR_SLOTS_PER_TICK);

                    let peers = repair_peers_fn();

                    if peers.is_empty() {
                        continue;
                    }

                    if !slots.is_empty() {
                        info!(tracked_slots = slots.len(), peers = peers.len(), "repair tick");
                    }

                    for slot in &slots {
                        // Request batch header if missing
                        if !self.collector.has_header(*slot) && self.collector.shred_count(*slot) > 0 {
                            let req = TurbineMessage::RepairRequest(
                                RepairRequest::BatchHeader { slot: *slot },
                            );
                            if let Ok(bytes) = req.serialize_to_bytes() {
                                for peer in &peers {
                                    let _ = self.socket.send_to(&bytes, peer).await;
                                }
                            }
                            debug!(slot = *slot, "requesting missing batch header");
                            metrics::counter!("nusantara_turbine_repair_requests_total").increment(1);
                        }

                        let missing = self.collector.missing_shreds(*slot);

                        if missing.is_empty() && self.collector.shred_count(*slot) == 0 {
                            let req = TurbineMessage::RepairRequest(
                                RepairRequest::HighestShred { slot: *slot },
                            );
                            if let Ok(bytes) = req.serialize_to_bytes() {
                                for peer in &peers {
                                    let _ = self.socket.send_to(&bytes, peer).await;
                                }
                                debug!(slot = *slot, peers = peers.len(), "broadcast HighestShred repair request");
                            }
                            metrics::counter!("nusantara_turbine_repair_requests_total").increment(1);
                            continue;
                        }

                        if missing.is_empty() {
                            if self.collector.is_slot_complete(*slot) {
                                continue;
                            }
                            let req = TurbineMessage::RepairRequest(
                                RepairRequest::HighestShred { slot: *slot },
                            );
                            if let Ok(bytes) = req.serialize_to_bytes() {
                                for peer in &peers {
                                    let _ = self.socket.send_to(&bytes, peer).await;
                                }
                            }
                            debug!(
                                slot = *slot,
                                shred_count = self.collector.shred_count(*slot),
                                "requesting HighestShred — have shreds but missing last index"
                            );
                            metrics::counter!("nusantara_turbine_repair_requests_total").increment(1);
                            continue;
                        }

                        debug!(slot, missing_count = missing.len(), "requesting batch repair shreds");

                        for chunk in missing.chunks(MAX_REPAIR_BATCH_REQUEST as usize) {
                            let req = TurbineMessage::RepairRequest(
                                RepairRequest::ShredBatch {
                                    slot: *slot,
                                    indices: chunk.to_vec(),
                                },
                            );
                            if let Ok(bytes) = req.serialize_to_bytes() {
                                for peer in &peers {
                                    let _ = self.socket.send_to(&bytes, peer).await;
                                }
                            }
                        }

                        let chunk_count =
                            missing.len().div_ceil(MAX_REPAIR_BATCH_REQUEST as usize);
                        metrics::counter!("nusantara_turbine_repair_requests_total")
                            .increment(chunk_count as u64);
                    }
                }
                _ = shutdown.changed() => {
                    break;
                }
            }
        }
    }
}
