//! silt-exex: stock reth + silt (PLAN M1, D7, D16).
//!
//! Same CLI, datadir and execution as the stock `reth` binary (its `main`,
//! bin/reth/src/main.rs at the pinned rev, is mirrored line by line: the
//! allocator, the account-extension guard, the SIGSEGV handler, backfill;
//! not the JIT helper: the `jit` feature isn't built, see Cargo.toml).
//! The additions all come from `silt-node`:
//!
//! - the "silt" ExEx: the state-diff feed and the lookup fast path (D9);
//! - `silt_multiproof` on reth's RPC transports (D8);
//! - the stock EVM config wrapped by [`SiltExecutorBuilder`]: the engine's
//!   EVM for each payload records what the block reads, for read touches
//!   (D13/D16); every other EVM is reth's, unwrapped;
//! - the engine's events, forwarded so each block is published as soon as
//!   it is executed and validated (D15).
//!
//! Rollback = run stock reth on the same datadir.

#[global_allocator]
static ALLOC: reth_ethereum::cli::allocator::Allocator =
    reth_ethereum::cli::allocator::new_allocator();

// Required for "override_allocator_on_supported_platforms" (as reth's main).
#[cfg(all(feature = "jemalloc", unix))]
use reth_ethereum::cli::allocator::tikv_jemalloc_sys as _;

use std::sync::Arc;

use reth_ethereum::node::snap::EthereumBackfill;
use reth_ethereum::node::{EthereumAddOns, EthereumNode};
use silt_node::evm::SiltExecutorBuilder;
use silt_node::reads::{ReadTouchSink, ReadTouches};

/// Blocks the read-touch sink holds: the engine runs at most a few ahead of
/// what the ExEx publishes; those of blocks that never commit age out.
const READ_SINK_BLOCKS: usize = 256;

fn main() {
    // Ethereum does not use account extensions (as reth's main).
    if let Err(err) = reth_ethereum::primitives::ensure_no_account_extensions() {
        eprintln!("Error: {err}: this binary was built with the `account-ext` feature");
        std::process::exit(1);
    }

    reth_ethereum::cli::sigsegv_handler::install();

    // Enable backtraces unless a RUST_BACKTRACE value has already been explicitly provided.
    if std::env::var_os("RUST_BACKTRACE").is_none() {
        unsafe { std::env::set_var("RUST_BACKTRACE", "1") };
    }

    if let Err(err) = reth_ethereum::cli::Cli::parse_args().run(async move |builder, _| {
        tracing::info!(target: "reth::cli", "Launching node (stock reth + silt)");
        let cfg = silt_node::SiltConfig::load()?;
        let reads = cfg
            .read_touches
            .then(|| Arc::new(ReadTouchSink::new(READ_SINK_BLOCKS)));
        let exex_reads = reads.clone().map(|sink| sink as Arc<dyn ReadTouches>);
        let (early_tx, early_rx) = silt_node::early::channel();
        let early_publish = cfg.early_publish;
        let backfill = EthereumBackfill::new(builder.config());
        let handle = builder
            .with_types::<EthereumNode>()
            .with_components(EthereumNode::components().executor(SiltExecutorBuilder::new(reads)))
            .with_add_ons(EthereumAddOns::default())
            .install_exex("silt", async move |ctx| {
                Ok(silt_node::run_exex_with(
                    ctx,
                    cfg,
                    exex_reads,
                    Some(early_rx),
                ))
            })
            .extend_rpc_modules(|ctx| {
                // D8: silt_multiproof rides reth's own RPC transports —
                // whatever --http/--ws/--ipc enable; no flags, no
                // endpoint. The feed socket stays push-only.
                ctx.modules
                    .merge_configured(silt_node::rpc_module(ctx.provider().clone()))?;
                tracing::info!("silt_multiproof registered on configured rpc transports");
                Ok(())
            })
            // As reth's main: debug capabilities (--dev mining among them)
            // and backfill.
            .launch_with_debug_capabilities_and_backfill(backfill)
            .await?;

        // D15: the engine's executed blocks, to the ExEx (which ignores them
        // when `early_publish` is off).
        if early_publish {
            let events = handle.node.add_ons_handle.engine_events.new_listener();
            tokio::spawn(silt_node::early::forward_engine_events(events, early_tx));
        }
        handle.wait_for_node_exit().await
    }) {
        eprintln!("Error: {err:?}");
        std::process::exit(1);
    }
}
