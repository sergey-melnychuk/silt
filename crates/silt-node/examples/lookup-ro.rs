//! Read-only lookup harness (PLAN T4.5): serves the D9 lookup protocol from
//! a live reth datadir opened **read-only**, through the same server code
//! the ExEx runs, so the fast path can be benchmarked and verified against
//! real mainnet state without touching the running node.
//!
//! Usage:
//!   lookup-ro --datadir <path> [--bind 127.0.0.1:8587] [--workers 16]
//!             [--scratch <dir>]
//!
//! - "latest" (all-zero pin) is the **state frontier**: since reth 2.10 a
//!   live node defers hashed-state writes for its newest persisted blocks
//!   (state masking), so from disk alone state is exact only up to
//!   `FinishCheckpoint::partial_state_trie`. Pins above it answer
//!   `UnknownBlock`.
//! - Every request opens a fresh provider (one MDBX read transaction), so
//!   new blocks become visible and no reader outlives a request.
//! - reth's read-only RocksDB is a *secondary* instance that writes a
//!   `rocksdb-secondary-tmp-<pid>` directory next to `rocksdb/`. To keep
//!   the node's datadir untouched, the harness points reth at a symlink
//!   `<scratch>/rocksdb -> <datadir>/rocksdb`, so that directory lands in
//!   `<scratch>` (removed on exit).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::B256;
use reth_ethereum::chainspec::{EthChainSpec, MAINNET};
use reth_ethereum::node::EthereumNode;
use reth_ethereum::node::api::NodeTypesWithDBAdapter;
use reth_ethereum::provider::db::DatabaseEnv;
use reth_ethereum::provider::providers::{BlockchainProvider, ProviderFactory, ReadOnlyConfig};
use reth_ethereum::provider::{BlockHashReader, BlockNumReader, StageCheckpointReader};
use reth_ethereum::tasks::{RayonConfig, Runtime, RuntimeBuilder, RuntimeConfig, TokioConfig};
use reth_stages_types::StageId;
use silt_node::lookup::{
    LookupConfig, LookupServer, Opened, ReadError, RethView, StateSource, classify,
};
use silt_wire::Word;

type Node = NodeTypesWithDBAdapter<EthereumNode, DatabaseEnv>;

/// reth's `Runtime` with every rayon pool at one thread: point lookups use
/// none of them (sizing verified at the pinned tag, docs/reth-notes.md).
fn minimal_runtime(handle: tokio::runtime::Handle) -> eyre::Result<Runtime> {
    let config = RuntimeConfig::default()
        .with_tokio(TokioConfig::existing_handle(handle))
        .with_rayon(RayonConfig {
            cpu_threads: Some(1),
            reserved_cpu_cores: 0,
            rpc_threads: Some(1),
            storage_threads: Some(1),
            max_blocking_tasks: 16,
            proof_storage_worker_threads: Some(1),
            proof_account_worker_threads: Some(1),
            prewarming_threads: Some(1),
            bal_streaming_threads: Some(1),
            state_trie_overlay_worker_threads: Some(1),
        });
    Ok(RuntimeBuilder::new(config).build()?)
}

/// Newest block whose post-state is exactly readable from disk.
fn state_frontier(provider: &impl StageCheckpointReader) -> Result<u64, ReadError> {
    let finish = provider
        .get_stage_checkpoint(StageId::Finish)
        .map_err(classify)?
        .ok_or_else(|| ReadError::Other("datadir has no Finish checkpoint".into()))?;
    Ok(finish
        .finish_stage_checkpoint()
        .and_then(|f| f.partial_state_trie())
        .unwrap_or(finish.block_number))
}

struct ReadOnlySource {
    factory: ProviderFactory<Node>,
    /// Only for `state_provider_from_database` (shares the factory's
    /// overlay manager); its own in-memory view stays empty.
    chain: BlockchainProvider<Node>,
}

impl StateSource for ReadOnlySource {
    type View = RethView;

    fn open(&self, pin: Option<&Word>) -> Result<Opened<RethView>, ReadError> {
        // One read transaction serves the frontier check, the hash
        // resolution and the state reads.
        let provider = self.factory.provider().map_err(classify)?;
        let frontier = state_frontier(&provider)?;
        let (number, hash) = match pin {
            None => {
                let hash = provider
                    .block_hash(frontier)
                    .map_err(classify)?
                    .ok_or_else(|| ReadError::Other(format!("no hash for frontier {frontier}")))?;
                (frontier, hash)
            }
            Some(pin) => {
                let hash = B256::from(*pin);
                let number = provider
                    .block_number(hash)
                    .map_err(classify)?
                    .ok_or_else(|| ReadError::UnknownBlock(format!("{hash} not canonical")))?;
                if provider.block_hash(number).map_err(classify)? != Some(hash) {
                    return Err(ReadError::UnknownBlock(format!("{hash} not canonical")));
                }
                if number > frontier {
                    return Err(ReadError::UnknownBlock(format!(
                        "block {number} is past the state frontier {frontier}"
                    )));
                }
                (number, hash)
            }
        };
        let state = self.chain.state_provider_from_database(provider, hash);
        Ok(Opened {
            number,
            hash: hash.0,
            view: RethView(state),
        })
    }
}

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

/// `<scratch>/rocksdb -> <datadir>/rocksdb`; see module docs.
fn prepare_scratch(scratch: &Path, datadir: &Path) -> eyre::Result<PathBuf> {
    std::fs::create_dir_all(scratch)?;
    let link = scratch.join("rocksdb");
    if !link.exists() {
        std::os::unix::fs::symlink(datadir.join("rocksdb").canonicalize()?, &link)?;
    }
    Ok(link)
}

fn main() -> eyre::Result<()> {
    let args = args()?;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()?;
    let rocksdb = prepare_scratch(&args.scratch, &args.datadir)?;
    let result = rt.block_on(serve(&args, rocksdb));
    // Drop every connection task (and with them the provider factory, whose
    // RocksDB secondary removes its own temp dir), then the scratch dir.
    rt.shutdown_timeout(Duration::from_secs(2));
    // `remove_dir_all` removes the symlink itself, never its target.
    if let Err(err) = std::fs::remove_dir_all(&args.scratch) {
        eprintln!(
            "lookup-ro: could not remove {}: {err}",
            args.scratch.display()
        );
    }
    result
}

async fn serve(args: &Args, rocksdb: PathBuf) -> eyre::Result<()> {
    let runtime = minimal_runtime(tokio::runtime::Handle::current())?;
    let mut ro = ReadOnlyConfig::from_datadir(&args.datadir);
    ro.rocksdb_dir = rocksdb;
    let factory =
        EthereumNode::provider_factory_builder().open_read_only(MAINNET.clone(), ro, runtime)?;
    let chain = BlockchainProvider::new(factory.clone())?;
    let (frontier, persisted) = {
        let p = factory.provider()?;
        (
            state_frontier(&p).map_err(|e| eyre::eyre!("{e}"))?,
            p.best_block_number()?,
        )
    };
    let server = LookupServer::new(
        ReadOnlySource { factory, chain },
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
