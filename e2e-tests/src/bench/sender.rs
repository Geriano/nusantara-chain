use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use nusantara_crypto::{Hash, Keypair};
use rand::Rng;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::client::NusantaraClient;
use crate::tx_builder;

/// A submitted transaction record.
#[derive(Debug, Clone)]
pub struct Submission {
    pub signature: String,
    pub submit_time: Instant,
}

pub struct TransactionSender {
    client: Arc<NusantaraClient>,
    keypairs: Vec<Keypair>,
    addresses: Vec<Hash>,
    tx_count: usize,
    num_senders: usize,
    target_tps: u64,
    lamports_per_tx: u64,
}

impl TransactionSender {
    pub fn new(
        client: Arc<NusantaraClient>,
        keypairs: Vec<Keypair>,
        tx_count: usize,
        num_senders: usize,
        target_tps: u64,
        lamports_per_tx: u64,
    ) -> Self {
        let addresses: Vec<Hash> = keypairs.iter().map(|kp| kp.address()).collect();
        Self {
            client,
            keypairs,
            addresses,
            tx_count,
            num_senders,
            target_tps,
            lamports_per_tx,
        }
    }

    /// Send all transactions across multiple tokio tasks.
    /// Returns the list of submissions.
    pub async fn send_all(self) -> Vec<Submission> {
        let num_accounts = self.keypairs.len();
        let txs_per_sender = self.tx_count / self.num_senders;
        let remainder = self.tx_count % self.num_senders;

        // Global nonce counter to ensure unique tx hashes
        let nonce_counter: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));

        // Shared state: keypairs + addresses wrapped in Arc
        let keypairs: Arc<Vec<Keypair>> = Arc::new(self.keypairs);
        let addresses: Arc<Vec<Hash>> = Arc::new(self.addresses);
        let submissions: Arc<Mutex<Vec<Submission>>> =
            Arc::new(Mutex::new(Vec::with_capacity(self.tx_count)));

        // Rate limiter: interval between sends (per sender)
        let send_interval = if self.target_tps > 0 {
            let interval_us = 1_000_000 * self.num_senders as u64 / self.target_tps;
            Some(Duration::from_micros(interval_us))
        } else {
            None
        };

        // Fetch initial blockhash
        let blockhash = Arc::new(Mutex::new(
            tx_builder::fetch_blockhash(&self.client)
                .await
                .expect("failed to fetch blockhash"),
        ));
        let blockhash_fetched = Arc::new(Mutex::new(Instant::now()));

        let mut handles = Vec::with_capacity(self.num_senders);

        for sender_id in 0..self.num_senders {
            let count = txs_per_sender + if sender_id < remainder { 1 } else { 0 };
            let client = self.client.clone();
            let keypairs = keypairs.clone();
            let addresses = addresses.clone();
            let submissions = submissions.clone();
            let blockhash = blockhash.clone();
            let blockhash_fetched = blockhash_fetched.clone();
            let lamports = self.lamports_per_tx;
            let nonce_counter = nonce_counter.clone();

            handles.push(tokio::spawn(async move {
                let mut local_subs = Vec::with_capacity(count);

                for i in 0..count {
                    // Pick sender account round-robin across all accounts
                    let sender_idx = (sender_id + i * self.num_senders) % num_accounts;
                    // Pick random recipient (different from sender)
                    let recipient_idx = loop {
                        let idx = rand::rng().random_range(0..num_accounts);
                        if idx != sender_idx {
                            break idx;
                        }
                    };

                    // Refresh blockhash if stale (>50s)
                    {
                        let fetched = *blockhash_fetched.lock().await;
                        if fetched.elapsed() > Duration::from_secs(50) {
                            match tx_builder::fetch_blockhash(&client).await {
                                Ok(new_bh) => {
                                    *blockhash.lock().await = new_bh;
                                    *blockhash_fetched.lock().await = Instant::now();
                                    debug!(sender_id, "refreshed blockhash");
                                }
                                Err(e) => {
                                    warn!(sender_id, %e, "failed to refresh blockhash");
                                }
                            }
                        }
                    }

                    let bh = *blockhash.lock().await;
                    let nonce = nonce_counter.fetch_add(1, Ordering::Relaxed);
                    let encoded = match tx_builder::build_transfer_with_nonce(
                        &keypairs[sender_idx],
                        &addresses[recipient_idx],
                        lamports,
                        &bh,
                        nonce,
                    ) {
                        Ok(enc) => enc,
                        Err(e) => {
                            warn!(sender_id, tx = i, %e, "failed to build tx");
                            continue;
                        }
                    };

                    let submit_time = Instant::now();
                    let req = crate::types::SendTransactionRequest {
                        transaction: encoded,
                    };
                    match client
                        .post::<crate::types::SendTransactionResponse, _>(
                            "/v1/transaction/send",
                            &req,
                        )
                        .await
                    {
                        Ok(resp) => {
                            local_subs.push(Submission {
                                signature: resp.signature,
                                submit_time,
                            });
                        }
                        Err(e) => {
                            warn!(sender_id, tx = i, %e, "failed to send tx");
                        }
                    }

                    // Rate limiting
                    if let Some(interval) = send_interval {
                        tokio::time::sleep(interval).await;
                    }
                }

                submissions.lock().await.extend(local_subs);
            }));
        }

        for handle in handles {
            if let Err(e) = handle.await {
                warn!(%e, "sender task panicked");
            }
        }

        let subs = Arc::try_unwrap(submissions)
            .expect("all tasks done")
            .into_inner();
        info!(submitted = subs.len(), "all transactions sent");
        subs
    }
}

/// Generate `n` fresh keypairs.
pub fn generate_keypairs(n: usize) -> Vec<Keypair> {
    (0..n).map(|_| Keypair::generate()).collect()
}
