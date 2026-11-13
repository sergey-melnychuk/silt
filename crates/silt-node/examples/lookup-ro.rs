//! Read-only lookup harness (PLAN T4.5): serves the D9 lookup protocol from
//! a live reth datadir opened **read-only**, through the same server code
//! the ExEx runs, so the fast path can be benchmarked and verified against
//! real mainnet state without touching the running node.
//!
//! Usage:
//!   lookup-ro --datadir <path> [--bind 127.0.0.1:8587] [--workers 16]
//!             [--scratch <dir>]
//!
//! - "latest" (all-zero pin) is the **state frontier** (see
//!   `common/ro.rs`); pins above it answer `UnknownBlock`.
//! - Every request opens a fresh provider (one MDBX read transaction), so
//!   new blocks become visible and no reader outlives a request.
//! - The datadir stays untouched: RocksDB's secondary scratch lands in
//!   `<scratch>` (removed on exit; `common/ro.rs`).

#[path = "common/ro.rs"]
mod ro;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use reth_ethereum::chainspec::{EthChainSpec, MAINNET};
use reth_ethereum::provider::BlockNumReader;
use silt_node::lookup::{LookupConfig, LookupServer};

use ro::state_frontier;

struct Args {
    datadir: PathBuf,
    bind: String,
    workers: usize,
    scratch: PathBuf,
}

fn args() -> eyre::Result<Args> {
    let mut a = Args {
        datadir: PathBuf::new(),
        bind: "127.0.0.1:8587".into(),
        workers: LookupConfig::default().workers,
        scratch: std::env::temp_dir().join(format!("silt-lookup-ro-{}", std::process::id())),
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut value = || it.next().ok_or_else(|| eyre::eyre!("{flag} needs a value"));
        match flag.as_str() {
            "--datadir" => a.datadir = value()?.into(),
            "--bind" => a.bind = value()?,
            "--workers" => a.workers = value()?.parse()?,
            "--scratch" => a.scratch = value()?.into(),
            other => eyre::bail!("unknown flag {other}"),
        }
    }
    eyre::ensure!(!a.datadir.as_os_str().is_empty(), "--datadir is required");
    Ok(a)
}

fn main() -> eyre::Result<()> {
    let args = args()?;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()?;
    let rocksdb = ro::prepare_scratch(&args.scratch, &args.datadir)?;
    let result = rt.block_on(serve(&args, rocksdb));
    // Drop every connection task (and with them the provider factory, whose
    // RocksDB secondary removes its own temp dir), then the scratch dir.
    rt.shutdown_timeout(Duration::from_secs(2));
    ro::remove_scratch(&args.scratch, "lookup-ro");
    result
}

async fn serve(args: &Args, rocksdb: PathBuf) -> eyre::Result<()> {
    let source = ro::open(&args.datadir, rocksdb)?;
    let (frontier, persisted) = {
        let p = source.factory.provider()?;
        (
            state_frontier(&p).map_err(|e| eyre::eyre!("{e}"))?,
            p.best_block_number()?,
        )
    };
    let server = LookupServer::new(
        source,
        MAINNET.chain().id(),
        MAINNET.genesis_hash().0,
        LookupConfig {
            workers: args.workers,
            ..Default::default()
        },
    )?;
    let listener = tokio::net::TcpListener::bind(&args.bind).await?;
    println!(
        "lookup-ro: {} on {} ({} workers): state frontier {frontier}, persisted {persisted}",
        args.datadir.display(),
        args.bind,
        args.workers
    );
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        _ = Arc::clone(&server).serve(listener) => {}
        _ = tokio::signal::ctrl_c() => println!("lookup-ro: interrupted"),
        _ = term.recv() => println!("lookup-ro: terminated"),
    }
    Ok(())
}
