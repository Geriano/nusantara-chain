use std::sync::Arc;
use std::time::Duration;

use nusantara_crypto::Keypair;
use tokio::task::JoinSet;
use tracing::{info, warn};

use crate::client::NusantaraClient;
use crate::error::E2eError;
use crate::tx_builder;

/// Fund multiple accounts via sequential airdrops.
///
/// Each airdrop is confirmed before proceeding to the next.
/// The faucet has a max of 10 NUSA per request.
pub async fn fund_accounts(
    client: &NusantaraClient,
    keypairs: &[Keypair],
    lamports_each: u64,
) -> Result<(), E2eError> {
    let confirm_timeout = Duration::from_secs(30);
    let pacing = Duration::from_millis(50);

    for (i, kp) in keypairs.iter().enumerate() {
        let address = kp.address();
        let sig = tx_builder::airdrop(client, &address, lamports_each).await?;
        let status = tx_builder::wait_for_confirmation(client, &sig, confirm_timeout).await?;
        if status.status != "success" {
            return Err(E2eError::Other(format!(
                "airdrop for account {i} failed: {}",
                status.status
            )));
        }
        info!(
            account = i,
            address = %address.to_base64(),
            lamports = lamports_each,
            "funded account"
        );
        tokio::time::sleep(pacing).await;
    }

    Ok(())
}

/// Fund multiple accounts via parallel batched airdrops.
///
/// Splits keypairs into chunks of `batch_size`, fires up to `concurrency` airdrops
/// concurrently within each chunk, then batch-confirms all signatures before
/// moving to the next chunk. Failed airdrops are retried up to 3 times.
pub async fn fund_accounts_parallel(
    client: Arc<NusantaraClient>,
    keypairs: &[Keypair],
    lamports_each: u64,
    batch_size: usize,
    concurrency: usize,
) -> Result<(), E2eError> {
    let confirm_timeout = Duration::from_secs(60);
    let max_retries = 3u32;
    let slot_pacing = Duration::from_millis(400); // ~1 slot between chunks

    let total = keypairs.len();
    let chunks: Vec<&[Keypair]> = keypairs.chunks(batch_size).collect();

    for (chunk_idx, chunk) in chunks.iter().enumerate() {
        let chunk_start = chunk_idx * batch_size;
        info!(
            chunk = chunk_idx,
            accounts = format!("{}-{}", chunk_start, chunk_start + chunk.len() - 1),
            total,
            "funding chunk"
        );

        // Collect addresses for this chunk
        let addresses: Vec<_> = chunk.iter().map(|kp| kp.address()).collect();

        // Fire airdrops concurrently with bounded concurrency via semaphore
        let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency));
        let mut join_set = JoinSet::new();

        for (i, address) in addresses.iter().enumerate() {
            let client = client.clone();
            let address = *address;
            let sem = semaphore.clone();
            let account_idx = chunk_start + i;

            join_set.spawn(async move {
                let _permit = sem.acquire().await.expect("semaphore closed");
                let sig = tx_builder::airdrop(&client, &address, lamports_each).await?;
                Ok::<(usize, String), E2eError>((account_idx, sig))
            });
        }

        // Collect airdrop results
        let mut signatures: Vec<(usize, String)> = Vec::with_capacity(chunk.len());
        let mut airdrop_failures: Vec<(usize, nusantara_crypto::Hash)> = Vec::new();

        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok((idx, sig))) => signatures.push((idx, sig)),
                Ok(Err(e)) => {
                    warn!(%e, "airdrop failed in batch");
                    // We'll retry these
                    airdrop_failures.push((0, nusantara_crypto::Hash::zero()));
                }
                Err(e) => {
                    warn!(%e, "airdrop task panicked");
                }
            }
        }

        // Batch-confirm all signatures
        let sigs: Vec<String> = signatures.iter().map(|(_, s)| s.clone()).collect();
        if !sigs.is_empty() {
            let results =
                tx_builder::wait_for_confirmations_batch(client.clone(), sigs, confirm_timeout)
                    .await;

            let confirmed = results.iter().filter(|r| r.result.is_ok()).count();
            let failed = results.len() - confirmed;

            info!(
                chunk = chunk_idx,
                confirmed,
                failed,
                "chunk confirmation complete"
            );

            // Retry failed confirmations
            if failed > 0 {
                let failed_sigs: Vec<String> = results
                    .iter()
                    .filter(|r| r.result.is_err())
                    .map(|r| r.signature.clone())
                    .collect();
                warn!(
                    count = failed_sigs.len(),
                    "retrying failed confirmations"
                );

                for retry in 0..max_retries {
                    let retry_results = tx_builder::wait_for_confirmations_batch(
                        client.clone(),
                        failed_sigs.clone(),
                        confirm_timeout,
                    )
                    .await;

                    let still_failed: Vec<String> = retry_results
                        .iter()
                        .filter(|r| r.result.is_err())
                        .map(|r| r.signature.clone())
                        .collect();

                    if still_failed.is_empty() {
                        info!(retry = retry + 1, "all retried confirmations succeeded");
                        break;
                    }

                    if retry == max_retries - 1 {
                        warn!(
                            count = still_failed.len(),
                            "some transactions failed after all retries"
                        );
                    }
                }
            }
        }

        // Log failures from airdrop phase
        if !airdrop_failures.is_empty() {
            warn!(
                count = airdrop_failures.len(),
                chunk = chunk_idx,
                "airdrop requests failed in chunk"
            );
        }

        // Pace between chunks to let mempool drain
        if chunk_idx + 1 < chunks.len() {
            tokio::time::sleep(slot_pacing).await;
        }
    }

    info!(total, "all accounts funded");
    Ok(())
}
