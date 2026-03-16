use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::task::JoinSet;
use tracing::{debug, warn};

use crate::client::NusantaraClient;
use crate::error::E2eError;
use crate::types::TransactionStatusResponse;

use super::sender::Submission;

/// A confirmed transaction record.
#[derive(Debug, Clone)]
pub struct Confirmation {
    pub signature: String,
    pub submit_time: Instant,
    pub confirm_time: Instant,
    pub latency: Duration,
    pub status: String,
}

/// Result of tracking: confirmed, failed, or timed-out transactions.
#[derive(Debug)]
pub struct TrackingResult {
    pub confirmed: Vec<Confirmation>,
    pub failed: Vec<Confirmation>,
    pub timed_out: Vec<String>,
}

/// Track confirmation of submitted transactions by polling the RPC concurrently.
///
/// Polls up to 256 signatures in parallel per batch using `JoinSet`.
pub async fn track(
    client: Arc<NusantaraClient>,
    submissions: &[Submission],
    timeout: Duration,
) -> TrackingResult {
    let poll_interval = Duration::from_millis(500);
    let batch_size = 256;
    let start = Instant::now();

    // Work with owned copies so we can move data into spawned tasks
    let mut pending: Vec<Submission> = submissions.to_vec();
    let mut confirmed = Vec::new();
    let mut failed = Vec::new();

    while !pending.is_empty() && start.elapsed() < timeout {
        let mut still_pending = Vec::new();

        for chunk in pending.chunks(batch_size) {
            let mut join_set = JoinSet::new();

            for sub in chunk {
                let sig = sub.signature.clone();
                let submit_time = sub.submit_time;
                let client = client.clone();

                join_set.spawn(async move {
                    let path = format!("/v1/transaction/{sig}");
                    let result = client.get::<TransactionStatusResponse>(&path).await;
                    (sig, submit_time, result)
                });
            }

            while let Some(join_result) = join_set.join_next().await {
                match join_result {
                    Ok((sig, submit_time, Ok(status))) => {
                        let confirm_time = Instant::now();
                        let latency = confirm_time.duration_since(submit_time);
                        if status.status != "success" {
                            warn!(
                                %sig,
                                status = %status.status,
                                slot = status.slot,
                                fee = status.fee,
                                "transaction failed"
                            );
                        }
                        let confirmation = Confirmation {
                            signature: sig,
                            submit_time,
                            confirm_time,
                            latency,
                            status: status.status.clone(),
                        };
                        if status.status == "success" {
                            confirmed.push(confirmation);
                        } else {
                            failed.push(confirmation);
                        }
                    }
                    Ok((sig, submit_time, Err(E2eError::Rpc { status: 404, .. }))) => {
                        still_pending.push(Submission {
                            signature: sig,
                            submit_time,
                        });
                    }
                    Ok((sig, submit_time, Err(e))) => {
                        warn!(%sig, %e, "error polling tx status");
                        still_pending.push(Submission {
                            signature: sig,
                            submit_time,
                        });
                    }
                    Err(e) => {
                        warn!(%e, "tracker poll task panicked");
                    }
                }
            }
        }

        pending = still_pending;

        if !pending.is_empty() {
            debug!(remaining = pending.len(), "waiting for confirmations");
            tokio::time::sleep(poll_interval).await;
        }
    }

    let timed_out: Vec<String> = pending.iter().map(|s| s.signature.clone()).collect();

    TrackingResult {
        confirmed,
        failed,
        timed_out,
    }
}
