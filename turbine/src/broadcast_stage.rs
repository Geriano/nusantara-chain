use std::net::SocketAddr;
use std::sync::Arc;

use nusantara_core::block::Block;
use nusantara_crypto::{Hash, Keypair};
use tokio::net::UdpSocket;
use tracing::{debug, info, instrument};

use crate::error::TurbineError;
use crate::protocol::TurbineMessage;
use crate::shredder::Shredder;
use crate::signed_shred::SignedShred;
use crate::turbine_tree::TurbineTree;

pub struct BroadcastStage {
    keypair: Arc<Keypair>,
    socket: Arc<UdpSocket>,
}

impl BroadcastStage {
    pub fn new(keypair: Arc<Keypair>, socket: Arc<UdpSocket>) -> Self {
        Self { keypair, socket }
    }

    /// Shred a block and broadcast to layer-0 turbine peers.
    /// `addr_lookup` maps identity Hash to turbine SocketAddr.
    #[instrument(skip(self, block, tree, addr_lookup), fields(slot = block.header.slot))]
    pub async fn broadcast_block<F>(
        &self,
        block: &Block,
        tree: &TurbineTree,
        addr_lookup: F,
    ) -> Result<(), TurbineError>
    where
        F: Fn(&Hash) -> Option<SocketAddr>,
    {
        let slot = block.header.slot;
        let parent_slot = block.header.parent_slot;

        let batch = Shredder::shred_block(block, parent_slot, &self.keypair)?;
        let peer_ids = tree.retransmit_peers(&self.keypair.address());
        let peer_addrs: Vec<SocketAddr> = peer_ids
            .iter()
            .filter_map(&addr_lookup)
            .collect();

        info!(
            slot,
            data_shreds = batch.data_shreds.len(),
            code_shreds = batch.code_shreds.len(),
            layer_0_peers = peer_addrs.len(),
            "broadcasting block shreds"
        );

        // Send all data shreds to layer-0 peers
        for shred in &batch.data_shreds {
            let signed = SignedShred::Data(shred.clone());
            self.send_shred_to_peers(&signed, &peer_addrs).await;
        }

        // Send code shreds too
        for shred in &batch.code_shreds {
            let signed = SignedShred::Code(shred.clone());
            self.send_shred_to_peers(&signed, &peer_addrs).await;
        }

        metrics::counter!("turbine_broadcast_total").increment(1);
        metrics::histogram!("turbine_shreds_per_broadcast")
            .record((batch.data_shreds.len() + batch.code_shreds.len()) as f64);

        Ok(())
    }

    async fn send_shred_to_peers(&self, shred: &SignedShred, peer_addrs: &[SocketAddr]) {
        let msg = TurbineMessage::Shred(shred.clone());
        let bytes = match msg.serialize_to_bytes() {
            Ok(b) => b,
            Err(e) => {
                debug!(error = %e, "failed to serialize shred message");
                return;
            }
        };

        for addr in peer_addrs {
            if let Err(e) = self.socket.send_to(&bytes, addr).await {
                debug!(%addr, error = %e, "failed to send shred");
            }
        }
    }
}
