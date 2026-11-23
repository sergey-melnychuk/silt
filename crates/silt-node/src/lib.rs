//! silt-node: the silt capabilities as a library any reth node installs
//! (PLAN D7):
//!
//! - the **state-diff feed** — an ExEx mapping every committed block into a
//!   node-agnostic [`silt_wire::BlockDiff`] pushed over TCP ([`feed`], D1/D5/D6),
//!   or, when the host forwards the engine's events, every block as soon as
//!   it is executed and validated ([`early`], D15);
//! - the **lookup fast path** — binary, pipelined tip-state reads for cache
//!   misses and trie proofs on their own listener ([`lookup`], D9/D10),
//!   started by the same ExEx;
//! - **`silt_multiproof`** — path-directed multiproofs on reth's own RPC
//!   transports ([`rpc`], D8).
//!
//! Installing it in a node is two builder calls, plus, for read touches
//! (D13/D16) and early publishing (D15), the stock EVM config wrapped by
//! [`evm::SiltExecutorBuilder`] and the engine's events forwarded after
//! launch. `silt-exex`'s `main` is the whole of it; a node with its own
//! executor reports read touches from that executor instead
//! ([`reads::ReadTouchSink`]):
//!
//! ```ignore
//! let cfg = silt_node::SiltConfig::load()?; // ./silt.toml, defaults otherwise
//! let reads = cfg.read_touches.then(|| Arc::new(ReadTouchSink::new(256)));
//! let exex_reads = reads.clone().map(|r| r as Arc<dyn ReadTouches>);
//! let (early_tx, early_rx) = silt_node::early::channel();
//! let handle = builder
//!     .with_types::<EthereumNode>()
//!     .with_components(EthereumNode::components().executor(SiltExecutorBuilder::new(reads)))
//!     .with_add_ons(EthereumAddOns::default())
//!     .install_exex("silt", async move |ctx| {
//!         Ok(silt_node::run_exex_with(ctx, cfg, exex_reads, Some(early_rx)))
//!     })
//!     .extend_rpc_modules(|ctx| {
//!         ctx.modules.merge_configured(silt_node::rpc_module(ctx.provider().clone()))?;
//!         Ok(())
//!     })
//!     .launch_with_debug_capabilities()
//!     .await?;
//! let events = handle.node.add_ons_handle.engine_events.new_listener();
//! tokio::spawn(silt_node::early::forward_engine_events(events, early_tx));
//! ```
//!
//! The host must build against the same reth as silt-node (the workspace
//! pin in the root `Cargo.toml`): one binary cannot mix reth versions.

pub mod config;
pub mod early;
pub mod evm;
pub mod feed;
pub mod lookup;
pub mod mapping;
pub mod pending;
pub mod reads;
pub mod rpc;

pub use config::SiltConfig;
pub use rpc::{SiltApiServer, SiltRpc};

use futures::TryStreamExt;
use reth_ethereum::pool::{PoolTransaction, TransactionPool};
use reth_ethereum::{EthPrimitives, TransactionSigned};
use reth_ethereum::chainspec::{ChainSpec, EthChainSpec};
use reth_ethereum::exex::{ExExContext, ExExEvent, ExExNotification};
use reth_ethereum::node::api::{FullNodeComponents, NodeTypes};
use reth_ethereum::provider::StateProviderFactory;

/// `silt_multiproof` (D8) as a jsonrpsee module, for `extend_rpc_modules`:
/// `ctx.modules.merge_configured(rpc_module(ctx.provider().clone()))`. It is
/// served on whatever `--http`/`--ws`/`--ipc` enable; the feed stays
/// push-only.
pub fn rpc_module<P>(provider: P) -> jsonrpsee::RpcModule<SiltRpc<P>>
where
    P: StateProviderFactory + Clone + Send + Sync + 'static,
{
    SiltRpc::new(provider).into_rpc()
}

/// The silt ExEx, for `install_exex("silt", async move |ctx| Ok(run_exex(ctx, cfg)))`:
/// binds the feed (`cfg.bind`) and, unless `cfg.lookup_bind` is empty, the
/// lookup server; then maps every notification into a diff, spools and
/// fans it out, and acks `FinishedHeight` once spooled (D5). Diffs carry no
/// read touches and go out once canonical; see [`run_exex_with`].
pub async fn run_exex<Node>(ctx: ExExContext<Node>, cfg: SiltConfig) -> eyre::Result<()>
where
    Node: FullNodeComponents<Types: NodeTypes<Primitives = EthPrimitives, ChainSpec = ChainSpec>>,
    Node::Pool: TransactionPool<Transaction: PoolTransaction<Consensus = TransactionSigned>>,
{
    run_exex_with(ctx, cfg, None, None).await
}

/// [`run_exex`] with the host's optional hooks:
/// - `reads`: diffs also carry read touches (PLAN D13/D15/D16), the values
///   the host's executor reports; with `cfg.pending_reads`, also what the
///   pool's pending transactions read on the head (D14, [`pending`]);
/// - `early`: the engine's executed blocks, from
///   [`early::forward_engine_events`]. With `cfg.early_publish`, each block
///   is published as soon as the engine has executed and validated it
///   (D15); the ExEx's own notifications then publish only what that path
///   missed, and reorg away early blocks that didn't become canonical.
pub async fn run_exex_with<Node>(
    mut ctx: ExExContext<Node>,
    cfg: SiltConfig,
    reads: Option<std::sync::Arc<dyn reads::ReadTouches>>,
    early: Option<early::EarlyReceiver>,
) -> eyre::Result<()>
where
    Node: FullNodeComponents<Types: NodeTypes<Primitives = EthPrimitives, ChainSpec = ChainSpec>>,
    Node::Pool: TransactionPool<Transaction: PoolTransaction<Consensus = TransactionSigned>>,
{
    let chain_id = ctx.config.chain.chain().id();
    let genesis_hash = ctx.config.chain.genesis_hash().0;
    let publisher = feed::Publisher::new(chain_id, genesis_hash, cfg.spool);

    let listener = tokio::net::TcpListener::bind(&cfg.bind).await?;
    tracing::info!(bind = %cfg.bind, chain_id, "silt feed listening");
    tokio::spawn(publisher.clone().serve(listener));

    // D9: binary lookups on their own listener. Reads run on the server's
    // own threads; this loop only spawns it and never waits on it (D5).
    if !cfg.lookup_bind.is_empty() {
        let listener = tokio::net::TcpListener::bind(&cfg.lookup_bind).await?;
        let lookup_cfg = lookup::LookupConfig {
            workers: cfg.lookup_workers,
            proof_workers: cfg.lookup_proof_workers,
            proof_max_depth: cfg.lookup_proof_max_depth,
            ..Default::default()
        };
        let source = lookup::NodeSource::new(ctx.provider().clone());
        let server = lookup::LookupServer::new(source, chain_id, genesis_hash, lookup_cfg)?;
        tracing::info!(
            bind = %cfg.lookup_bind,
            workers = cfg.lookup_workers,
            proof_workers = cfg.lookup_proof_workers,
            "silt lookup listening"
        );
        tokio::spawn(server.serve(listener));
    }

    {
        let publisher = publisher.clone();
        let period = std::time::Duration::from_secs(cfg.heartbeat_secs.max(1));
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(period).await;
                if let Err(err) = publisher.publish_heartbeat().await {
                    tracing::error!(%err, "silt heartbeat failed");
                }
            }
        });
    }

    // D14: the pool worker, fed each canonical head below.
    let pending = match (&reads, cfg.pending_reads) {
        (Some(sink), true) => match pending::spawn(
            ctx.provider().clone(),
            ctx.pool().clone(),
            ctx.evm_config().clone(),
            sink.clone(),
        ) {
            Ok(heads) => {
                tracing::info!("silt: pending read touches on (pool worker started)");
                Some(heads)
            }
            Err(err) => {
                tracing::warn!(%err, "silt: pool worker not started; no pending read touches");
                None
            }
        },
        (None, true) => {
            tracing::warn!("silt: pending_reads needs read_touches; off");
            None
        }
        _ => None,
    };

    let mut early = early.filter(|_| cfg.early_publish);
    tracing::info!(early = early.is_some(), "silt: publishing blocks");
    // The chain published so far: early blocks continue it; committed
    // diffs already in it are skipped (this also dedupes reth's replay of
    // unacked notifications after a restart); a committed block that
    // replaces an early one reorgs it away first.
    let mut published = early::Published::new(early::PUBLISHED_WINDOW);
    let mut stats = PublishStats::default();

    loop {
        tokio::select! {
            // Early blocks first: they're the latency-critical ones.
            biased;
            head = next_early(&mut early) => {
                match head {
                    Some(head) => {
                        publish_early(&publisher, &mut published, reads.as_deref(), &mut stats, head)
                            .await?;
                    }
                    None => {
                        tracing::warn!("silt: engine events ended; publishing after commit only");
                        early = None;
                    }
                }
            }
            notification = ctx.notifications.try_next() => {
                let Some(notification) = notification? else { break };
                // D14: the new canonical head, for the pool worker.
                let tip = pending
                    .as_ref()
                    .and_then(|_| notification.committed_chain())
                    .map(|chain| chain.tip().clone_sealed_header());
                on_notification(&ctx, &cfg, &publisher, &mut published, reads.as_deref(), &mut stats, notification)
                    .await?;
                if let (Some(heads), Some(tip)) = (&pending, tip) {
                    // The worker may be gone (it logs why): nothing to do then.
                    let _ = heads.send(tip);
                }
            }
        }
    }
    Ok(())
}

async fn next_early(early: &mut Option<early::EarlyReceiver>) -> Option<early::ExecutedHead> {
    match early {
        Some(rx) => rx.recv().await,
        None => std::future::pending().await,
    }
}

/// D15: publishes an executed block that continues the published chain.
async fn publish_early(
    publisher: &feed::Publisher,
    published: &mut early::Published,
    reads: Option<&dyn reads::ReadTouches>,
    stats: &mut PublishStats,
    head: early::ExecutedHead,
) -> eyre::Result<()> {
    use reth_ethereum::primitives::AlloyBlockHeader;
    let block = &head.block;
    let (number, hash, parent) = (block.number(), block.hash().0, block.parent_hash().0);
    if published.holds(number, &hash) {
        return Ok(());
    }
    if !published.continues(number, &parent) {
        stats.early_not_continuing += 1;
        tracing::debug!(
            number,
            "silt: executed block doesn't continue the feed; publishing after commit"
        );
        return Ok(());
    }
    let mut diff = match mapping::diff_from_executed(block, &head.output) {
        Ok(diff) => diff,
        Err(err) => {
            tracing::warn!(number, %err, "silt: executed block not mapped; publishing after commit");
            return Ok(());
        }
    };
    if let Some(reads) = reads {
        add_reads(reads, &mut diff, [(number, hash)]);
    }
    log_publish(&diff, "early");
    publisher.publish_diff(&diff).await?;
    published.record(&diff);
    stats.early += 1;
    let lag_us = head.received.elapsed().as_micros() as u64;
    stats.lag_us_sum += lag_us;
    stats.lag_us_max = stats.lag_us_max.max(lag_us);
    if number.is_multiple_of(300) {
        log_stats(stats, reads);
    }
    Ok(())
}

/// The ExEx path: D12 reorgs, then committed blocks the early path didn't
/// publish, then the `FinishedHeight` ack.
async fn on_notification<Node>(
    ctx: &ExExContext<Node>,
    cfg: &SiltConfig,
    publisher: &feed::Publisher,
    published: &mut early::Published,
    reads: Option<&dyn reads::ReadTouches>,
    stats: &mut PublishStats,
    notification: ExExNotification<EthPrimitives>,
) -> eyre::Result<()>
where
    Node: FullNodeComponents<Types: NodeTypes<Primitives = EthPrimitives, ChainSpec = ChainSpec>>,
{
    use reth_ethereum::primitives::AlloyBlockHeader;
    // D12: consumers roll back to the fork; the new side (if any) follows
    // as ordinary diffs.
    let replaced = match &notification {
        ExExNotification::ChainReorged { old, new } => {
            tracing::warn!(from = ?old.range(), to = ?new.range(), "silt: reorg");
            Some((old, new.fork_block()))
        }
        ExExNotification::ChainReverted { old } => {
            tracing::warn!(range = ?old.range(), "silt: revert");
            Some((old, old.fork_block()))
        }
        ExExNotification::ChainCommitted { .. } => None,
    };
    if let Some((old, fork)) = replaced {
        let fork = (fork.number, fork.hash.0);
        // The old canonical blocks, and any early ones published above them.
        let mut dropped = published.above(fork.0);
        for header in old.headers() {
            let block = (header.number(), header.hash().0);
            if !dropped.contains(&block) {
                dropped.push(block);
            }
        }
        dropped.sort_unstable();
        publisher.publish_reorg(fork, &dropped).await?;
        published.rewind(fork);
    }

    if let Some(chain) = notification.committed_chain() {
        // Long catch-up segments come out as several diffs of at most
        // `max_span` blocks; all are spooled before the ack.
        for mut diff in mapping::diffs_from_chain(&chain, cfg.max_span)? {
            match published.plan(&diff) {
                early::Plan::Skip => {
                    stats.skipped += 1;
                    tracing::debug!(number = diff.number, "silt: diff already published");
                    continue;
                }
                early::Plan::ReorgFirst { fork, dropped } => {
                    tracing::warn!(
                        fork = fork.0,
                        dropped = dropped.len(),
                        "silt: early block(s) not canonical; reorg"
                    );
                    stats.early_dropped += dropped.len() as u64;
                    publisher.publish_reorg(fork, &dropped).await?;
                    published.rewind(fork);
                }
                early::Plan::Publish => {}
            }
            if let Some(reads) = reads {
                let blocks = chain
                    .blocks_iter()
                    .filter(|b| (diff.span_start..=diff.number).contains(&b.number()))
                    .map(|b| (b.number(), b.hash().0))
                    .collect::<Vec<_>>();
                add_reads(reads, &mut diff, blocks);
            }
            log_publish(&diff, "exex");
            publisher.publish_diff(&diff).await?;
            published.record(&diff);
            stats.exex += 1;
            if diff.number.is_multiple_of(300) {
                log_stats(stats, reads);
            }
        }
        // D5: the frames are spooled — safe to let reth prune.
        ctx.events
            .send(ExExEvent::FinishedHeight(chain.tip().num_hash()))?;
    }
    Ok(())
}

fn log_publish(diff: &silt_wire::BlockDiff, path: &'static str) {
    tracing::info!(
        span = ?(diff.span_start..=diff.number),
        path,
        accounts = diff.accounts.len(),
        slots = diff.storage.len(),
        read_accounts = diff.read_accounts.len(),
        read_slots = diff.read_storage.len(),
        "silt: publishing diff"
    );
}

/// D13/D15: the read touches the host's executor reported for the span's
/// `blocks` (number, hash), valued at each block; then (D14) the pool's
/// pending touches read on the diff's parent. Both skip what the span
/// wrote. No reads from the node's state.
fn add_reads(
    reads: &dyn reads::ReadTouches,
    diff: &mut silt_wire::BlockDiff,
    blocks: impl IntoIterator<Item = (u64, silt_wire::Word)>,
) {
    for (number, hash) in blocks {
        if let Some(values) = reads.take(number, &hash) {
            reads::merge_reads(diff, values);
        }
    }
    let pending = reads.take_pending(diff.number, &diff.parent_hash);
    if !pending.is_empty() {
        let added = reads::merge_reads(diff, pending) as u64;
        PENDING_ADDED.fetch_add(added, std::sync::atomic::Ordering::Relaxed);
    }
}

/// D14 pending touches published since the last stats line.
static PENDING_ADDED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Diffs published by path since the last stats line (D15).
#[derive(Debug, Default)]
struct PublishStats {
    /// Published at execution.
    early: u64,
    /// Published after commit, by the ExEx.
    exex: u64,
    /// Committed diffs skipped as already published.
    skipped: u64,
    /// Executed blocks that didn't continue the published chain (left to
    /// the ExEx).
    early_not_continuing: u64,
    /// Early blocks a later commit reorged away.
    early_dropped: u64,
    /// Engine event → published, for early diffs.
    lag_us_sum: u64,
    lag_us_max: u64,
}

/// Logs the "silt: publish" line (and, with read touches, the "silt: read
/// touches" one) and resets the counters.
fn log_stats(stats: &mut PublishStats, reads: Option<&dyn reads::ReadTouches>) {
    use std::sync::atomic::Ordering::Relaxed;
    let s = std::mem::take(stats);
    tracing::info!(
        early = s.early,
        exex = s.exex,
        skipped = s.skipped,
        early_not_continuing = s.early_not_continuing,
        early_dropped = s.early_dropped,
        early_queue_dropped = early::FORWARD_DROPPED.swap(0, Relaxed),
        lag_ms_avg = s.lag_us_sum as f64 / 1e3 / s.early.max(1) as f64,
        lag_ms_max = s.lag_us_max as f64 / 1e3,
        "silt: publish"
    );
    if let Some(reads) = reads {
        tracing::info!(
            source = reads.stats().unwrap_or_default(),
            pending_published = PENDING_ADDED.swap(0, Relaxed),
            "silt: read touches"
        );
    }
}
