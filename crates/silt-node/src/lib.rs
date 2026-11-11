//! silt-node: the silt capabilities as a library any reth node installs
//! (PLAN D7):
//!
//! - the **state-diff feed** — an ExEx mapping every committed block into a
//!   node-agnostic [`silt_wire::BlockDiff`] pushed over TCP ([`feed`], D1/D5/D6);
//! - the **lookup fast path** — binary, pipelined tip-state reads for cache
//!   misses on their own listener ([`lookup`], D9), started by the same ExEx;
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
/// fans it out, and acks `FinishedHeight` once spooled (D5).
pub async fn run_exex<Node>(mut ctx: ExExContext<Node>, cfg: SiltConfig) -> eyre::Result<()>
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
            ..Default::default()
        };
        let source = lookup::NodeSource::new(ctx.provider().clone());
        let server = lookup::LookupServer::new(source, chain_id, genesis_hash, lookup_cfg)?;
        tracing::info!(bind = %cfg.lookup_bind, workers = cfg.lookup_workers, "silt lookup listening");
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
        if let ExExNotification::ChainReorged { old, new } = &notification {
            tracing::warn!(from = ?old.range(), to = ?new.range(), "silt: reorg");
            publisher
                .publish_reorg(new.tip().header().number, new.tip().hash().0)
                .await?;
            last_published = None; // the new side must always publish
        }

        if let Some(chain) = notification.committed_chain() {
            let diff = mapping::diff_from_chain(&chain)?;
            let duplicate =
                matches!(last_published, Some((n, h)) if diff.number <= n && diff.hash == h);
            if duplicate {
                tracing::info!(number = diff.number, "silt: skipping re-delivered diff");
            } else {
                tracing::info!(
                    span = ?(diff.span_start..=diff.number),
                    accounts = diff.accounts.len(),
                    slots = diff.storage.len(),
                    "silt: publishing diff"
                );
                publisher.publish_diff(&diff).await?;
                last_published = Some((diff.number, diff.hash));
            }
            // D5: the frame is spooled — safe to let reth prune.
            ctx.events
                .send(ExExEvent::FinishedHeight(chain.tip().num_hash()))?;
        }
    }
    Ok(())
}
