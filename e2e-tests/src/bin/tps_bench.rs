use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use tracing::info;

use nusantara_e2e_tests::bench::config::{BenchConfig, OutputFormat};
use nusantara_e2e_tests::bench::report::BenchReport;
use nusantara_e2e_tests::bench::sender::{TransactionSender, generate_keypairs};
use nusantara_e2e_tests::bench::tracker;
use nusantara_e2e_tests::client::{ClientConfig, NusantaraClient};
use nusantara_e2e_tests::cluster::wait_for_cluster_ready;
use nusantara_e2e_tests::funding;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let config = BenchConfig::parse();
    let num_senders = config.effective_num_senders();

    info!(?config, num_senders, "starting TPS benchmark");

    // Scale connection pool based on sender count
    let pool_max_idle = num_senders + 8;
    let client = Arc::new(NusantaraClient::new(
        config.rpc_urls.clone(),
        ClientConfig {
            timeout: Duration::from_secs(15),
            max_retries: 3,
            retry_backoff: Duration::from_millis(500),
            pool_max_idle_per_host: pool_max_idle,
        },
    ));

    // Wait for cluster
    info!("waiting for cluster to be ready...");
    wait_for_cluster_ready(&client, 1, Duration::from_secs(60)).await?;

    // Generate and fund accounts
    info!(num_accounts = config.num_accounts, "generating keypairs");
    let keypairs = generate_keypairs(config.num_accounts);

    info!(
        fund_amount = config.fund_amount,
        "funding {} accounts",
        config.num_accounts
    );

    // Use parallel funding for >50 accounts, sequential otherwise
    if config.num_accounts > 50 {
        info!(
            batch_size = config.funding_batch_size,
            concurrency = config.funding_concurrency,
            "using parallel funding"
        );
        funding::fund_accounts_parallel(
            client.clone(),
            &keypairs,
            config.fund_amount,
            config.funding_batch_size,
            config.funding_concurrency,
        )
        .await?;
    } else {
        funding::fund_accounts(&client, &keypairs, config.fund_amount).await?;
    }

    // Prepare and send
    let sender = TransactionSender::new(
        client.clone(),
        keypairs,
        config.tx_count,
        num_senders,
        config.target_tps,
        config.lamports_per_tx,
    );

    info!(tx_count = config.tx_count, num_senders, "sending transactions...");
    let submit_start = Instant::now();
    let submissions = sender.send_all().await;
    let submit_end = Instant::now();

    // Track confirmations
    info!(
        submitted = submissions.len(),
        "tracking confirmations..."
    );
    let tracking = tracker::track(client.clone(), &submissions, config.confirm_timeout()).await;

    // Report
    let report = BenchReport::compute(submissions.len(), submit_start, submit_end, &tracking);

    match config.output {
        OutputFormat::Human => report.print_human(),
        OutputFormat::Json => report.print_json(),
    }

    Ok(())
}
