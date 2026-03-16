use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::UdpSocket;
use tokio::sync::{mpsc, watch};
use tracing::{debug, error, instrument};

use crate::protocol::{TurbineMessage, MAX_UDP_PACKET};
use crate::signed_shred::SignedShred;

pub struct ShredReceiver {
    socket: Arc<UdpSocket>,
}

impl ShredReceiver {
    pub fn new(socket: Arc<UdpSocket>) -> Self {
        Self { socket }
    }

    /// Receive shreds from the UDP socket and forward to the given channel.
    #[instrument(skip(self, shred_sender, repair_sender, shutdown), name = "shred_receiver")]
    pub async fn run(
        self,
        shred_sender: mpsc::Sender<(SignedShred, SocketAddr)>,
        repair_sender: mpsc::Sender<(TurbineMessage, SocketAddr)>,
        mut shutdown: watch::Receiver<bool>,
    ) {
        let mut buf = vec![0u8; MAX_UDP_PACKET];

        loop {
            tokio::select! {
                biased;
                result = self.socket.recv_from(&mut buf) => {
                    match result {
                        Ok((len, src)) => {
                            let data = &buf[..len];
                            match TurbineMessage::deserialize_from_bytes(data) {
                                Ok(TurbineMessage::Shred(shred)) => {
                                    metrics::counter!("turbine_shreds_received_total").increment(1);
                                    if shred_sender.send((shred, src)).await.is_err() {
                                        debug!("shred channel closed");
                                        break;
                                    }
                                }
                                Ok(TurbineMessage::RepairResponse(shred)) => {
                                    metrics::counter!("turbine_repair_shreds_received").increment(1);
                                    if shred_sender.send((shred, src)).await.is_err() {
                                        debug!("shred channel closed");
                                        break;
                                    }
                                }
                                Ok(TurbineMessage::BatchRepairResponse(batch)) => {
                                    let count = batch.shreds.len() as u64;
                                    metrics::counter!("turbine_repair_shreds_received").increment(count);
                                    for shred in batch.shreds {
                                        if shred_sender.send((shred, src)).await.is_err() {
                                            debug!("shred channel closed");
                                            return;
                                        }
                                    }
                                }
                                Ok(msg @ TurbineMessage::RepairRequest(_)) => {
                                    let _ = repair_sender.send((msg, src)).await;
                                }
                                Err(e) => {
                                    debug!(%src, error = %e, "failed to deserialize turbine message");
                                }
                            }
                        }
                        Err(e) => {
                            error!(error = %e, "turbine recv error");
                        }
                    }
                }
                _ = shutdown.changed() => {
                    break;
                }
            }
        }
    }
}
