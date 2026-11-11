//! silt-exex: stock reth + silt (PLAN M1, D7).
//!
//! Same CLI and datadir semantics as the stock `reth` binary; the only
//! additions come from `silt-node`: the "silt" ExEx (state-diff feed + the
//! lookup fast path, D9) and `silt_multiproof` (D8). A node with a custom
//! executor installs silt the same way. Rollback = run stock reth on the
//! same datadir.

use reth_ethereum::node::EthereumNode;

fn main() -> eyre::Result<()> {
    reth_ethereum::cli::Cli::parse_args().run(async move |builder, _| {
        let cfg = silt_node::SiltConfig::load()?;
        // launch_with_debug_capabilities, not launch(): it is what the
        // stock reth binary uses (bin/reth/src/main.rs) and what wires
        // --dev interval mining among other debug features — D7 means
        // matching the stock binary exactly.
        let handle = builder
            .node(EthereumNode::default())
            .install_exex("silt", async move |ctx| Ok(silt_node::run_exex(ctx, cfg)))
            .extend_rpc_modules(|ctx| {
                // D8: silt_multiproof rides reth's own RPC transports —
                // whatever --http/--ws/--ipc enable; no flags, no
                // endpoint. The feed socket stays push-only.
                ctx.modules
                    .merge_configured(silt_node::rpc_module(ctx.provider().clone()))?;
                tracing::info!("silt_multiproof registered on configured rpc transports");
                Ok(())
            })
            .launch_with_debug_capabilities()
            .await?;
        handle.wait_for_node_exit().await
    })
}
