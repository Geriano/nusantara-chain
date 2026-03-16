use std::sync::Arc;

use nusantara_core::transaction::Transaction;
use quinn::Endpoint;
use tokio::sync::{mpsc, watch};
use tracing::{debug, info, instrument, warn};

use crate::protocol::TpuMessage;
use crate::rate_limiter::RateLimiter;
use crate::tx_validator::TxValidator;

pub struct TpuQuicServer {
    endpoint: Endpoint,
    rate_limiter: Arc<RateLimiter>,
}

impl TpuQuicServer {
    pub fn new(endpoint: Endpoint, rate_limiter: Arc<RateLimiter>) -> Self {
        Self {
            endpoint,
            rate_limiter,
        }
    }

    /// Run the QUIC server, accepting connections and forwarding valid transactions.
    #[instrument(skip(self, tx_sender, shutdown), name = "tpu_quic_server")]
    pub async fn run(
        self,
        tx_sender: mpsc::Sender<Transaction>,
        mut shutdown: watch::Receiver<bool>,
    ) {
        info!(
            addr = %self.endpoint.local_addr().unwrap_or_else(|_| "unknown".parse().unwrap()),
            "TPU QUIC server started"
        );

        loop {
            tokio::select! {
                biased;
                incoming = self.endpoint.accept() => {
                    let Some(incoming) = incoming else {
                        info!("QUIC endpoint closed");
                        break;
                    };

                    let remote = incoming.remote_address();
                    let ip = remote.ip();

                    // Check connection limit
                    if let Err(e) = self.rate_limiter.check_connection_limit(ip) {
                        debug!(%remote, error = %e, "connection rejected");
                        continue;
                    }

                    let rate_limiter = Arc::clone(&self.rate_limiter);
                    let tx_sender = tx_sender.clone();

                    tokio::spawn(async move {
                        rate_limiter.add_connection(ip);
                        match incoming.await {
                            Ok(conn) => {
                                handle_connection(conn, ip, &rate_limiter, &tx_sender).await;
                            }
                            Err(e) => {
                                debug!(%remote, error = %e, "incoming connection failed");
                            }
                        }
                        rate_limiter.remove_connection(ip);
                    });
                }
                _ = shutdown.changed() => {
                    break;
                }
            }
        }

        self.endpoint.close(0u32.into(), b"shutdown");
        info!("TPU QUIC server stopped");
    }
}

async fn handle_connection(
    conn: quinn::Connection,
    ip: std::net::IpAddr,
    rate_limiter: &RateLimiter,
    tx_sender: &mpsc::Sender<Transaction>,
) {
    loop {
        let mut stream = match conn.accept_uni().await {
            Ok(stream) => stream,
            Err(quinn::ConnectionError::ApplicationClosed(_)) => break,
            Err(e) => {
                debug!(%ip, error = %e, "stream accept error");
                break;
            }
        };

        match stream.read_to_end(crate::tx_validator::MAX_TRANSACTION_SIZE as usize).await {
            Ok(data) => {
                match TpuMessage::deserialize_from_bytes(&data) {
                    Ok(msg) => {
                        for tx in msg.transactions() {
                            if let Err(e) = rate_limiter.check_rate_limit(ip) {
                                debug!(%ip, error = %e, "rate limited");
                                return;
                            }

                            if let Err(e) = TxValidator::validate(&tx) {
                                debug!(%ip, error = %e, "invalid transaction");
                                metrics::counter!("tpu_invalid_transactions_total").increment(1);
                                continue;
                            }

                            metrics::counter!("tpu_transactions_received_total").increment(1);

                            if tx_sender.send(tx).await.is_err() {
                                warn!("tx channel closed");
                                return;
                            }
                        }
                    }
                    Err(e) => {
                        debug!(%ip, error = %e, "failed to deserialize TPU message");
                    }
                }
            }
            Err(e) => {
                debug!(%ip, error = %e, "stream read error");
            }
        }
    }
}
