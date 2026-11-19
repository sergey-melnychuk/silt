//! silt-node: the silt capabilities as a library any reth node installs
//! (PLAN D7):
//!
//! - the **state-diff feed** — an ExEx mapping every committed block into a
//!   node-agnostic [`silt_wire::BlockDiff`] pushed over TCP ([`feed`], D1/D5/D6);
//! - the **lookup fast path** — binary, pipelined tip-state reads for cache
//!   misses and trie proofs on their own listener ([`lookup`], D9/D10),
//!   started by the same ExEx;
//! - **`silt_multiproof`** — path-directed multiproofs on reth's own RPC
//!   transports ([`rpc`], D8).
//!
//! Installing it in a node is two builder calls; this is all of
//! `silt-exex`'s `main` (stock executor), and a custom node (e.g. one with
//! its own executor) adds the same two calls to its own builder chain:
//!
//! ```ignore
//! let cfg = silt_node::SiltConfig::load()?; // ./silt.toml, defaults otherwise
//! let handle = builder
//!     .node(EthereumNode::default())
//!     .install_exex("silt", async move |ctx| Ok(silt_node::run_exex(ctx, cfg)))
//!     .extend_rpc_modules(|ctx| {
//!         ctx.modules.merge_configured(silt_node::rpc_module(ctx.provider().clone()))?;
//!         Ok(())
//!     })
//!     .launch_with_debug_capabilities()
//!     .await?;
//! ```
//!
//! The host must build against the same reth as silt-node (the workspace
//! pin in the root `Cargo.toml`): one binary cannot mix reth versions.

pub mod config;
pub mod feed;
pub mod lookup;
pub mod mapping;
pub mod reads;
pub mod rpc;

pub use config::SiltConfig;
pub use rpc::{SiltApiServer, SiltRpc};

use futures::TryStreamExt;
use reth_ethereum::EthPrimitives;
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
/// read touches; see [`run_exex_with_reads`].
pub async fn run_exex<Node>(ctx: ExExContext<Node>, cfg: SiltConfig) -> eyre::Result<()>
where
    Node: FullNodeComponents<Types: NodeTypes<Primitives = EthPrimitives, ChainSpec = ChainSpec>>,
{
    run_exex_with_reads(ctx, cfg, None).await
}

/// [`run_exex`] whose diffs also carry read touches (PLAN D13): the keys
/// the host's executor reports through `reads`, valued at each diff's block
/// ([`reads::fill_reads`]). A block with no reported keys, or a failed
/// fill, publishes without them.
pub async fn run_exex_with_reads<Node>(
    mut ctx: ExExContext<Node>,
    cfg: SiltConfig,
    reads: Option<std::sync::Arc<dyn reads::ReadTouches>>,
) -> eyre::Result<()>
where
    Node: FullNodeComponents<Types: NodeTypes<Primitives = EthPrimitives, ChainSpec = ChainSpec>>,
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

    // Restart dedupe (docs/reth-notes.md): reth replays notifications not
    // yet acked; linear re-deliveries are skipped by height, reorgs always
    // pass (they arrive as ChainReorged and reset the stream).
    let mut last_published: Option<(u64, silt_wire::Word)> = None;

    while let Some(notification) = ctx.notifications.try_next().await? {
        // D12: consumers roll back to the fork; the new side (if any)
        // follows as ordinary diffs.
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
            let dropped: Vec<(u64, silt_wire::Word)> = old
                .headers()
                .map(|h| {
                    let nh = h.num_hash();
                    (nh.number, nh.hash.0)
                })
                .collect();
            publisher
                .publish_reorg((fork.number, fork.hash.0), &dropped)
                .await?;
            last_published = None; // the new side must always publish
        }

        if let Some(chain) = notification.committed_chain() {
            // Long catch-up segments come out as several diffs of at most
            // `max_span` blocks; all are spooled before the ack.
            for mut diff in mapping::diffs_from_chain(&chain, cfg.max_span)? {
                let duplicate =
                    matches!(last_published, Some((n, h)) if diff.number <= n && diff.hash == h);
                if duplicate {
                    tracing::info!(number = diff.number, "silt: skipping re-delivered diff");
                    continue;
                }
                if let Some(reads) = &reads {
                    diff = with_reads(ctx.provider().clone(), reads.as_ref(), &chain, diff).await;
                    if diff.number.is_multiple_of(300) {
                        log_read_stats(reads.as_ref());
                    }
                }
                tracing::info!(
                    span = ?(diff.span_start..=diff.number),
                    accounts = diff.accounts.len(),
                    slots = diff.storage.len(),
                    read_accounts = diff.read_accounts.len(),
                    read_slots = diff.read_storage.len(),
                    "silt: publishing diff"
                );
                publisher.publish_diff(&diff).await?;
                last_published = Some((diff.number, diff.hash));
            }
            // D5: the frames are spooled — safe to let reth prune.
            ctx.events
                .send(ExExEvent::FinishedHeight(chain.tip().num_hash()))?;
        }
    }
    Ok(())
}

/// Read-touch fills since the last summary: count, total and max µs (the
/// time each adds before its diff is published).
static FILLS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static FILL_US_SUM: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static FILL_US_MAX: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Logs one INFO line about read touches: the source's counters (keys per
/// block, collect time on the import path, drops, ignored executions) and
/// the fill time this ExEx added before publishing.
fn log_read_stats(reads: &dyn reads::ReadTouches) {
    use std::sync::atomic::Ordering::Relaxed;
    let fills = FILLS.swap(0, Relaxed);
    let sum = FILL_US_SUM.swap(0, Relaxed);
    let max = FILL_US_MAX.swap(0, Relaxed);
    tracing::info!(
        source = reads.stats().unwrap_or_default(),
        fills,
        fill_ms_avg = sum as f64 / 1e3 / fills.max(1) as f64,
        fill_ms_max = max as f64 / 1e3,
        "silt: read touches"
    );
}

/// D13: the span's reported read keys, valued at the diff's block on a
/// blocking thread. Optional data: on any failure the diff goes out
/// without them.
async fn with_reads<P>(
    provider: P,
    reads: &dyn reads::ReadTouches,
    chain: &reth_ethereum::provider::Chain<EthPrimitives>,
    mut diff: silt_wire::BlockDiff,
) -> silt_wire::BlockDiff
where
    P: StateProviderFactory + Send + Sync + 'static,
{
    use reth_ethereum::primitives::AlloyBlockHeader;
    let mut keys = reads::ReadKeys::default();
    for block in chain.blocks_iter() {
        let number = block.header().number();
        if !(diff.span_start..=diff.number).contains(&number) {
            continue;
        }
        let hashes: Vec<silt_wire::Word> = block
            .body()
            .transactions
            .iter()
            .map(|tx| tx.tx_hash().0)
            .collect();
        let txs = reads::tx_digest(&hashes);
        if let Some(k) = reads.take(number, &block.header().parent_hash().0, &txs) {
            keys.extend(k);
        }
    }
    if keys.is_empty() {
        return diff;
    }
    let started = std::time::Instant::now();
    let number = diff.number;
    // The task gets a copy: whatever happens to it, `diff` still goes out.
    let mut copy = diff.clone();
    let filled = tokio::task::spawn_blocking(move || {
        reads::fill_reads(&provider, &mut copy, keys).map(|()| copy)
    })
    .await;
    match filled {
        Ok(Ok(filled)) => {
            let us = started.elapsed().as_micros() as u64;
            FILL_US_SUM.fetch_add(us, std::sync::atomic::Ordering::Relaxed);
            FILL_US_MAX.fetch_max(us, std::sync::atomic::Ordering::Relaxed);
            FILLS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            tracing::debug!(
                number,
                accounts = filled.read_accounts.len(),
                slots = filled.read_storage.len(),
                ms = us as f64 / 1e3,
                "silt: read touches filled"
            );
            diff.read_accounts = filled.read_accounts;
            diff.read_storage = filled.read_storage;
        }
        Ok(Err(err)) => {
            tracing::warn!(number, %err, "silt: read touches not filled; publishing without");
        }
        Err(err) => {
            tracing::error!(number, %err, "silt: read-touch fill panicked; publishing without");
        }
    }
    diff
}
