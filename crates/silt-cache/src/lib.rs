//! silt-cache: a coherent chain-state cache fed by the silt diff stream,
//! with standard `eth_*` RPC as the always-available fallback (PLAN M2).
//!
//! Chain-shaped API (PLAN T2.4): whole-account reads, storage-slot reads,
//! heads, raw blocks — the same semantic surface yevm's `Chain` trait
//! wants; the optional `yevm` feature ships the 1:1 adapter plus an
//! `overlay()` helper (`Chain` & `State` walk together).
//!
//! Misses can take the binary lookup fast path (PLAN D9,
//! [`CacheConfig::with_lookup`]); JSON-RPC stays the fallback for every
//! lookup failure.

pub mod cache;
pub mod client;
pub mod fallback;
pub mod lookup;
pub mod store;

#[cfg(feature = "yevm")]
pub mod adapter;

#[cfg(feature = "trie")]
pub mod trie;

use std::sync::Arc;

pub use cache::{Cache, CacheConfig, DEFAULT_SOFT_LIMIT_BYTES};
pub use fallback::{Fallback, HttpFallback, RpcAccount};
pub use lookup::{LookupClient, LookupClientConfig, LookupError, LookupFallback};
pub use silt_wire::{Addr, HeadLite, Word};
pub use store::MemUsage;

/// What the consumer loop reacts to (PLAN T2.4): heads to process, breaks
/// to cancel in-flight work on.
#[derive(Clone, Debug)]
pub enum HeadEvent {
    NewHead(HeadLite),
    /// The carry was dropped (gap, feed anomaly, or a reorg it could not
    /// roll back) — state rebuilds lazily; in-flight work pinned to
    /// pre-break heads should be cancelled.
    ChainBreak {
        reason: String,
    },
    /// A reorg was rolled back (PLAN D12): the cache is coherent at the
    /// fork block now and follows the new side from there. Work pinned to
    /// heads above the fork should be cancelled.
    Reorg {
        fork_number: u64,
        fork_hash: silt_wire::Word,
    },
}

/// Whole-account read result (code always resolved).
#[derive(Clone, Debug)]
pub struct AccountView {
    pub balance: Word,
    pub nonce: u64,
    pub code_hash: Word,
    pub code: Arc<Vec<u8>>,
}
