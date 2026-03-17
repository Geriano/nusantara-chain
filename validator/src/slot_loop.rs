use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use nusantara_core::block::Block;
use nusantara_core::DEFAULT_SLOT_DURATION_MS;
use nusantara_crypto::Hash;
use nusantara_rpc::PubsubEvent;
use nusantara_sysvar_program::SlotHashes;
use nusantara_turbine::turbine_tree::TURBINE_FANOUT;
use nusantara_turbine::{BroadcastStage, TurbineTree};
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::cli::Cli;
use crate::constants::{GOSSIP_REPORT_INTERVAL, LEDGER_PRUNE_INTERVAL};
use crate::error::ValidatorError;
use crate::node::ValidatorNode;

impl ValidatorNode {
    pub async fn run(&mut self, cli: &Cli) -> Result<(), ValidatorError> {
        info!(start_slot = self.current_slot, "starting validator");

        let services = self.spawn_services(cli).await?;
        let mut block_rx = services.block_rx;
        let broadcast_stage = services.broadcast_stage;
        let current_slot_shared = services.current_slot_shared;
        let shutdown_tx = services.shutdown_tx;

        loop {
            tokio::select! {
                biased;
                _ = tokio::signal::ctrl_c() => {
                    info!("received shutdown signal");
                    let _ = shutdown_tx.send(true);
                    break;
                }
                _ = self.slot_clock.wait_for_slot(self.current_slot) => {
                    // Update shared current_slot for TPU closure
                    current_slot_shared.store(self.current_slot, Ordering::Relaxed);

                    if self.am_i_leader(self.current_slot) {
                        self.leader_slot(&broadcast_stage, &mut block_rx).await?;
                    } else {
                        self.non_leader_slot(&mut block_rx, cli.leader_timeout_ms).await?;
                    }

                    self.process_gossip_votes();

                    // Check for fork switch (F3)
                    if let Some(plan) = self.replay_stage.check_fork_switch() {
                        let target = plan.replay_slots.last().copied()
                            .unwrap_or(plan.common_ancestor);
                        if self.failed_fork_targets.contains(&target) {
                            tracing::trace!(target, "skipping fork switch — already failed");
                        } else {
                            self.handle_fork_switch(plan);
                        }
                    }

                    self.submit_vote(self.current_slot);
                    self.process_orphan_queue()?;
                    self.check_epoch_boundary(cli.snapshot_interval);

                    // Periodically report gossip peer count
                    if self.current_slot.is_multiple_of(GOSSIP_REPORT_INTERVAL) {
                        let peer_count = self.cluster_info.peer_count();
                        metrics::gauge!("nusantara_gossip_peers").set(peer_count as f64);
                    }

                    // Periodic ledger pruning
                    if cli.max_ledger_slots > 0
                        && self.current_slot.is_multiple_of(LEDGER_PRUNE_INTERVAL)
                    {
                        let min_slot =
                            self.current_slot.saturating_sub(cli.max_ledger_slots);
                        if min_slot > 0
                            && let Err(e) = self.storage.purge_slots_below(min_slot)
                        {
                            warn!(error = %e, min_slot, "ledger pruning failed");
                        }
                    }

                    self.current_slot += 1;
                }
            }
        }

        info!("validator shutdown complete");
        Ok(())
    }

    pub(crate) fn am_i_leader(&self, slot: u64) -> bool {
        let epoch = self.epoch_schedule.get_epoch(slot);

        // Ensure schedule is cached
        if !self.leader_cache.read().contains_key(&epoch) {
            let stakes = self.bank.get_stake_distribution();
            if let Ok(schedule) =
                self.leader_schedule_generator
                    .compute_schedule(epoch, &stakes, &self.genesis_hash)
            {
                self.leader_cache.write().insert(epoch, schedule);
            }
        }

        self.leader_cache
            .read()
            .get(&epoch)
            .and_then(|s| s.get_leader(slot, &self.epoch_schedule))
            .map(|leader| *leader == self.identity)
            .unwrap_or(false)
    }

    async fn leader_slot(
        &mut self,
        broadcast: &BroadcastStage,
        block_rx: &mut mpsc::Receiver<Block>,
    ) -> Result<(), ValidatorError> {
        // 1. Catch up on pending blocks from previous leader
        let mut pending = Vec::new();
        while let Ok(block) = block_rx.try_recv() {
            pending.push(block);
        }
        if !pending.is_empty() {
            pending.sort_by_key(|b| b.header.slot);
            info!(count = pending.len(), "catching up on pending blocks before leader slot");
            for block in pending {
                self.replay_or_buffer_block(block)?;
            }
            self.process_orphan_queue()?;
        }

        // 2. Wait for the previous slot's block if it's missing.
        let prev_slot = self.current_slot.saturating_sub(1);
        if prev_slot > 0
            && !self.replay_stage.fork_tree().contains(prev_slot)
            && !self.am_i_leader(prev_slot)
        {
            let wait_ms = DEFAULT_SLOT_DURATION_MS / 2;
            tracing::debug!(
                slot = self.current_slot,
                prev_slot,
                wait_ms,
                "waiting for previous slot's block before producing"
            );
            match tokio::time::timeout(
                Duration::from_millis(wait_ms),
                block_rx.recv(),
            )
            .await
            {
                Ok(Some(block)) => {
                    self.replay_or_buffer_block(block)?;
                    // Drain any additional blocks that arrived
                    while let Ok(extra) = block_rx.try_recv() {
                        self.replay_or_buffer_block(extra)?;
                    }
                    self.process_orphan_queue()?;
                }
                Ok(None) => return Err(ValidatorError::Shutdown),
                Err(_) => {
                    tracing::debug!(
                        slot = self.current_slot,
                        prev_slot,
                        "previous slot block didn't arrive, producing anyway"
                    );
                }
            }
        }

        // 3. Skip production if this slot was already processed
        if self.replay_stage.fork_tree().contains(self.current_slot) {
            info!(slot = self.current_slot, "slot already in fork tree, skipping production");
            return Ok(());
        }

        // 3a. Set parent to the fork-choice best fork before producing.
        let best = self.replay_stage.fork_tree().best_slot();
        if let Some(node) = self.replay_stage.fork_tree().get_node(best) {
            let prev_parent = self.block_producer.parent_slot();
            if prev_parent != best {
                tracing::info!(
                    prev_parent,
                    best_fork = best,
                    "switching parent to fork-choice best fork"
                );
            }
            self.block_producer
                .set_parent(best, node.block_hash, node.bank_hash);
        }

        // 3c. Rebuild slot_hashes and rewind account index from fork tree ancestry.
        let parent_slot = self.block_producer.parent_slot();
        let ancestry = self.replay_stage.fork_tree().get_ancestry(parent_slot);
        let fork_slot_hashes: Vec<(u64, Hash)> = ancestry
            .iter()
            .filter_map(|&s| {
                self.replay_stage
                    .fork_tree()
                    .get_node(s)
                    .map(|n| (s, n.block_hash))
            })
            .collect();
        self.bank.set_slot_hashes(SlotHashes(fork_slot_hashes));

        // Fork-aware account index rewind
        let ancestor_set: HashSet<u64> = ancestry.iter().copied().collect();
        let rewound = self
            .storage
            .rewind_account_index_for_ancestry(&ancestor_set)?;
        if rewound > 0 {
            tracing::info!(
                parent_slot,
                rewound,
                "rewound account index (fork-aware) before production"
            );
        }

        // 3b. Drain pending transactions from the priority mempool
        let transactions = self.mempool.drain_by_priority(2048);

        // 4. Produce block
        let block = self
            .block_producer
            .produce_block(self.current_slot, transactions)?;

        // Mark our own block as stored
        self.shred_collector.mark_slot_stored(self.current_slot);

        // 5. Feed into ReplayStage for fork tree tracking
        let result = self.replay_stage.replay_block(&block, &[])?;

        // Defer root advancement
        if let Some(root) = result.new_root {
            self.try_advance_root(root)?;
        }

        // 4. Build TurbineTree and broadcast
        let mut peers: Vec<Hash> = self
            .cluster_info
            .all_peers()
            .iter()
            .map(|ci| ci.identity)
            .collect();
        if !peers.contains(&self.identity) {
            peers.push(self.identity);
        }
        let stakes = self.bank.get_stake_distribution();
        let tree = TurbineTree::new(
            self.identity,
            &peers,
            &stakes,
            self.current_slot,
            TURBINE_FANOUT as usize,
        );
        let ci = Arc::clone(&self.cluster_info);
        broadcast
            .broadcast_block(&block, &tree, |id| {
                ci.get_contact_info(id).map(|c| c.turbine_addr.0)
            })
            .await?;

        // 5. Publish pubsub events for WebSocket subscribers
        let root = self.storage.get_latest_root().unwrap_or(None).unwrap_or(0);
        let _ = self.pubsub_tx.send(PubsubEvent::SlotUpdate {
            slot: self.current_slot,
            parent: block.header.parent_slot,
            root,
        });
        let _ = self.pubsub_tx.send(PubsubEvent::BlockNotification {
            slot: self.current_slot,
            block_hash: block.header.block_hash.to_base64(),
            tx_count: block.header.transaction_count,
        });

        metrics::counter!("nusantara_leader_slots").increment(1);
        info!(
            slot = self.current_slot,
            fork_tree_nodes = self.replay_stage.fork_tree().node_count(),
            fork_tree_root = self.replay_stage.fork_tree().root_slot(),
            "leader slot completed"
        );
        Ok(())
    }

    async fn non_leader_slot(
        &mut self,
        block_rx: &mut mpsc::Receiver<Block>,
        leader_timeout_ms: u64,
    ) -> Result<(), ValidatorError> {
        let timeout = Duration::from_millis(leader_timeout_ms);
        let mut blocks = Vec::new();

        // Wait for at least one block with timeout
        match tokio::time::timeout(timeout, block_rx.recv()).await {
            Ok(Some(block)) => blocks.push(block),
            Ok(None) => return Err(ValidatorError::Shutdown),
            Err(_) => {} // timeout — no block arrived
        }

        // Drain additional available blocks (non-blocking)
        while let Ok(block) = block_rx.try_recv() {
            blocks.push(block);
        }

        if blocks.is_empty() {
            let skips = self.consecutive_skips.fetch_add(1, Ordering::Relaxed) + 1;
            self.total_skips += 1;
            warn!(
                slot = self.current_slot,
                consecutive_skips = skips,
                "no block received (leader skip)"
            );
            if skips > 10 {
                warn!(
                    consecutive_skips = skips,
                    "possible network partition — many consecutive leader skips"
                );
            }
            metrics::counter!("nusantara_leader_skips").increment(1);
            metrics::counter!("nusantara_non_leader_slots").increment(1);
            metrics::gauge!("nusantara_consecutive_skips").set(skips as f64);
            return Ok(());
        }

        // Sort by slot for correct replay order
        blocks.sort_by_key(|b| b.header.slot);
        metrics::gauge!("nusantara_blocks_drained_per_slot").set(blocks.len() as f64);

        for block in blocks {
            self.replay_or_buffer_block(block)?;
        }

        self.process_orphan_queue()?;
        metrics::counter!("nusantara_non_leader_slots").increment(1);
        Ok(())
    }
}
