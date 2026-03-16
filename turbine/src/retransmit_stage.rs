use std::net::SocketAddr;
use std::sync::Arc;

use nusantara_core::block::Block;
use nusantara_crypto::{Hash, PublicKey};
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, watch};
use tracing::{debug, info, instrument, warn};

use crate::protocol::TurbineMessage;
use crate::shred_collector::ShredCollector;
use crate::signed_shred::SignedShred;
use crate::turbine_tree::TurbineTree;

pub struct RetransmitStage {
    my_identity: Hash,
    socket: Arc<UdpSocket>,
    collector: Arc<ShredCollector>,
}

impl RetransmitStage {
    pub fn new(
        my_identity: Hash,
        socket: Arc<UdpSocket>,
        collector: Arc<ShredCollector>,
    ) -> Self {
        Self {
            my_identity,
            socket,
            collector,
        }
    }

    /// Run the retransmit loop.
    /// `tree_provider` returns the turbine tree for a given slot.
    /// `addr_lookup` maps identity Hash to turbine SocketAddr.
    /// `pubkey_lookup` maps identity Hash to PublicKey for shred verification.
    #[instrument(skip_all, name = "retransmit")]
    pub async fn run<T, A, P>(
        self,
        mut shred_receiver: mpsc::Receiver<(SignedShred, SocketAddr)>,
        block_sender: mpsc::Sender<Block>,
        tree_provider: T,
        addr_lookup: A,
        pubkey_lookup: P,
        mut shutdown: watch::Receiver<bool>,
    ) where
        T: Fn(u64) -> Option<TurbineTree>,
        A: Fn(&Hash) -> Option<SocketAddr>,
        P: Fn(&Hash) -> Option<PublicKey>,
    {
        loop {
            tokio::select! {
                biased;
                Some((shred, _src)) = shred_receiver.recv() => {
                    let slot = shred.slot();
                    let leader = shred.leader();

                    // Verify shred signature before retransmitting
                    if let Some(pubkey) = pubkey_lookup(&leader) && !shred.verify(&pubkey) {
                        warn!(
                            slot,
                            leader = ?leader,
                            "dropping shred with invalid signature"
                        );
                        metrics::counter!("turbine_invalid_shred_signatures").increment(1);
                        continue;
                    }

                    // Retransmit to downstream peers
                    if let Some(tree) = tree_provider(slot) {
                        let peer_ids = tree.retransmit_peers(&self.my_identity);
                        let peer_addrs: Vec<SocketAddr> = peer_ids
                            .iter()
                            .filter_map(&addr_lookup)
                            .collect();
                        if !peer_addrs.is_empty() {
                            self.retransmit_shred(&shred, &peer_addrs).await;
                        }
                    }

                    // Feed data shreds to collector
                    if let SignedShred::Data(ref data_shred) = shred
                        && let Some(block) = self.collector.insert_data_shred(data_shred)
                    {
                        info!(
                            slot = block.header.slot,
                            txs = block.header.transaction_count,
                            "block assembled from shreds"
                        );
                        if block_sender.send(block).await.is_err() {
                            debug!("block channel closed");
                            break;
                        }
                    }

                    metrics::counter!("turbine_retransmit_total").increment(1);
                }
                _ = shutdown.changed() => {
                    break;
                }
            }
        }
    }

    async fn retransmit_shred(&self, shred: &SignedShred, peer_addrs: &[SocketAddr]) {
        let msg = TurbineMessage::Shred(shred.clone());
        let bytes = match msg.serialize_to_bytes() {
            Ok(b) => b,
            Err(e) => {
                debug!(error = %e, "failed to serialize retransmit message");
                return;
            }
        };

        for addr in peer_addrs {
            if let Err(e) = self.socket.send_to(&bytes, addr).await {
                debug!(%addr, error = %e, "retransmit send failed");
            }
        }
    }
}
