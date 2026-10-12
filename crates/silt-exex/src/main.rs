//! silt-exex: stock reth + the silt state-diff feed (PLAN M1, D7).
//!
//! Same CLI and datadir semantics as the stock `reth` binary; the only
//! addition is the "silt" ExEx publishing `silt_wire` frames over TCP.
//! Rollback = run stock reth on the same datadir.

mod config;
mod mapping;
mod server;

use futures::TryStreamExt;
use reth_ethereum::EthPrimitives;
use reth_ethereum::chainspec::{ChainSpec, EthChainSpec};
use reth_ethereum::exex::{ExExContext, ExExEvent, ExExNotification};
use reth_ethereum::node::EthereumNode;
use reth_ethereum::node::api::{FullNodeComponents, NodeTypes};

fn main() -> eyre::Result<()> {
    reth_ethereum::cli::Cli::parse_args().run(async move |builder, _| {
        let cfg = config::SiltConfig::load()?;
        // launch_with_debug_capabilities, not launch(): it is what the
        // stock reth binary uses (bin/reth/src/main.rs) and what wires
        // --dev interval mining among other debug features — D7 means
        // matching the stock binary exactly.
        let handle = builder
            .node(EthereumNode::default())
            .install_exex("silt", async move |ctx| Ok(run(ctx, cfg)))
            .launch_with_debug_capabilities()
            .await?;
        handle.wait_for_node_exit().await
    })
}

async fn run<Node>(mut ctx: ExExContext<Node>, cfg: config::SiltConfig) -> eyre::Result<()>
where
    Node: FullNodeComponents<Types: NodeTypes<Primitives = EthPrimitives, ChainSpec = ChainSpec>>,
{
    let chain_id = ctx.config.chain.chain().id();
    let genesis_hash = ctx.config.chain.genesis_hash().0;
    let publisher = server::Publisher::new(chain_id, genesis_hash, cfg.spool);

    let listener = tokio::net::TcpListener::bind(&cfg.bind).await?;
    tracing::info!(bind = %cfg.bind, chain_id, "silt feed listening");
    tokio::spawn(publisher.clone().serve(listener));

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
