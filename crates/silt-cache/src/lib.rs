//! silt-cache: a coherent chain-state cache fed by the silt diff stream,
//! with standard `eth_*` RPC as the always-available fallback (PLAN M2).
//!
//! Chain-shaped API (PLAN T2.4): whole-account reads, storage-slot reads,
//! heads, raw blocks — the same semantic surface yevm's `Chain` trait
//! wants; the optional `yevm` feature ships the 1:1 adapter plus an
//! `overlay()` helper (`Chain` & `State` walk together).

pub mod cache;
pub mod client;
pub mod fallback;
pub mod store;

#[cfg(feature = "yevm")]
pub mod adapter;

use std::sync::Arc;

pub use cache::{Cache, CacheConfig};
pub use fallback::{Fallback, HttpFallback, RpcAccount};
pub use silt_wire::{Addr, HeadLite, Word};

/// What the consumer loop reacts to (PLAN T2.4): heads to process, breaks
/// to cancel in-flight work on.
#[derive(Clone, Debug)]
pub enum HeadEvent {
    NewHead(HeadLite),
    /// The carry was dropped (reorg, gap, feed anomaly) — state rebuilds
    /// lazily; in-flight work pinned to pre-break heads should be cancelled.
    ChainBreak {
        reason: String,
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
