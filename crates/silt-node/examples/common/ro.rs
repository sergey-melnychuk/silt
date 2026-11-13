//! Opening a live reth datadir **read-only**, shared by the harness
//! examples (`lookup-ro`, `proof-bench`), and serving it as a lookup
//! [`StateSource`] pinned at or below the state frontier.
//!
//! - State is exact from disk only up to the **state frontier**
//!   (`FinishCheckpoint::partial_state_trie`): since reth 2.10 a live node
//!   defers hashed-state writes for its newest persisted blocks (state
//!   masking), so readers pin at or below it.
//! - reth's read-only RocksDB is a *secondary* instance that writes a
//!   `rocksdb-secondary-tmp-<pid>` directory next to `rocksdb/`. To keep
//!   the node's datadir untouched, reth is pointed at a symlink
//!   `<scratch>/rocksdb -> <datadir>/rocksdb`, so that directory lands in
//!   `<scratch>`, which [`remove_scratch`] deletes on exit.

// Each example uses a different subset.
#![allow(dead_code)]

use std::path::{Path, PathBuf};

use alloy_primitives::B256;
use reth_ethereum::chainspec::MAINNET;
use reth_ethereum::node::EthereumNode;
use reth_ethereum::node::api::NodeTypesWithDBAdapter;
use reth_ethereum::provider::db::DatabaseEnv;
use reth_ethereum::provider::providers::{BlockchainProvider, ProviderFactory, ReadOnlyConfig};
use reth_ethereum::provider::{BlockHashReader, BlockNumReader, StageCheckpointReader};
use reth_ethereum::tasks::{RayonConfig, Runtime, RuntimeBuilder, RuntimeConfig, TokioConfig};
use reth_stages_types::StageId;
use silt_node::lookup::{Opened, ReadError, RethView, StateSource, classify};
use silt_wire::Word;

pub type Node = NodeTypesWithDBAdapter<EthereumNode, DatabaseEnv>;

/// The read-only provider pair: `factory` opens one MDBX read transaction
/// per `provider()`; `chain` is only for `state_provider_from_database`
/// (it shares the factory's overlay manager) and for code that expects the
/// node's provider type (its in-memory view stays empty). Clones share
/// both.
#[derive(Clone)]
pub struct ReadOnly {
    pub factory: ProviderFactory<Node>,
    pub chain: BlockchainProvider<Node>,
}

/// reth's `Runtime` with every rayon pool at one thread: point lookups and
/// the sequential v1 multiproof walk use none of them (sizing verified at
/// the pinned rev, docs/reth-notes.md).
pub fn minimal_runtime(handle: tokio::runtime::Handle) -> eyre::Result<Runtime> {
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
pub fn state_frontier(provider: &impl StageCheckpointReader) -> Result<u64, ReadError> {
    let finish = provider
        .get_stage_checkpoint(StageId::Finish)
        .map_err(classify)?
        .ok_or_else(|| ReadError::Other("datadir has no Finish checkpoint".into()))?;
    Ok(finish
        .finish_stage_checkpoint()
        .and_then(|f| f.partial_state_trie())
        .unwrap_or(finish.block_number))
}

/// `<scratch>/rocksdb -> <datadir>/rocksdb`; see module docs.
pub fn prepare_scratch(scratch: &Path, datadir: &Path) -> eyre::Result<PathBuf> {
    std::fs::create_dir_all(scratch)?;
    let link = scratch.join("rocksdb");
    if !link.exists() {
        std::os::unix::fs::symlink(datadir.join("rocksdb").canonicalize()?, &link)?;
    }
    Ok(link)
}

/// Opens `datadir` read-only with RocksDB redirected to `rocksdb` (from
/// [`prepare_scratch`]). Must run inside a tokio runtime: reth's `Runtime`
/// borrows the current handle.
pub fn open(datadir: &Path, rocksdb: PathBuf) -> eyre::Result<ReadOnly> {
    let runtime = minimal_runtime(tokio::runtime::Handle::current())?;
    let mut ro = ReadOnlyConfig::from_datadir(datadir);
    ro.rocksdb_dir = rocksdb;
    let factory =
        EthereumNode::provider_factory_builder().open_read_only(MAINNET.clone(), ro, runtime)?;
    let chain = BlockchainProvider::new(factory.clone())?;
    Ok(ReadOnly { factory, chain })
}

/// Deletes the scratch dir. Call after every provider is dropped (the
/// RocksDB secondary removes its own temp dir on drop); `remove_dir_all`
/// removes the symlink itself, never its target.
pub fn remove_scratch(scratch: &Path, tag: &str) {
    if let Err(err) = std::fs::remove_dir_all(scratch) {
        eprintln!("{tag}: could not remove {}: {err}", scratch.display());
    }
}

/// "latest" is the state frontier; a non-zero pin must be canonical and at
/// or below it, else `UnknownBlock`.
impl StateSource for ReadOnly {
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

    /// The frontier: "latest" for this source.
    fn head(&self) -> Result<u64, ReadError> {
        state_frontier(&self.factory.provider().map_err(classify)?)
    }
}
