//! The `silt_multiproof` JSON-RPC endpoint (PLAN D8).
//!
//! Resolving a partial trie's stubs needs *path-directed* node fetches:
//! probe keys chosen so their proof walk passes through a target nibble
//! path. That requires submitting **already-hashed** trie keys, and
//! `eth_getProof` structurally cannot do it — it keccaks the raw key
//! server-side, so steering to a chosen path would mean inverting keccak.
//! `StateProofProvider::multiproof` takes hashed keys, but only
//! in-process; this module exposes it on reth's own RPC server (via
//! `extend_rpc_modules`), taking `hashed_address -> [hashed slot probe]`
//! targets and returning the path-keyed RLP node walks.

use std::collections::BTreeMap;

use alloy_primitives::{B256, Bytes};
use async_trait::async_trait;
use jsonrpsee::core::RpcResult;
use jsonrpsee::proc_macros::rpc;
use jsonrpsee::types::error::INTERNAL_ERROR_CODE;
use jsonrpsee::types::{ErrorObject, ErrorObjectOwned};
use reth_ethereum::provider::{StateProofProvider, StateProviderFactory};
use reth_ethereum::trie::{MultiProofTargets, Nibbles};
use serde::Serialize;

/// Storage-trie half of a [`MultiproofResponse`] entry.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StorageProof {
    /// Storage root of the account (the empty-trie root for accounts
    /// without storage).
    pub root: B256,
    /// Nibble path (one hex char per nibble, `""` = trie root) -> RLP node.
    pub subtree: BTreeMap<String, Bytes>,
}

/// Wire shape of a `silt_multiproof` result — deliberately reth-free JSON.
/// Consumers verify every node by hash before use (the response is
/// untrusted input to a partial-trie resolver), so path -> RLP maps are
/// the whole contract.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MultiproofResponse {
    /// Account (state) trie walk covering all requested hashed addresses.
    pub account_subtree: BTreeMap<String, Bytes>,
    /// Per hashed address: storage root + storage trie walk.
    pub storages: BTreeMap<B256, StorageProof>,
}

#[rpc(server, namespace = "silt")]
pub trait SiltApi {
    /// `targets`: `[hashedAddress, [hashedSlotProbe, ..]]` pairs with
    /// **pre-hashed** (keccak256) keys, so the caller steers the walk; an
    /// empty slot list probes only the account trie. `block`: block hash
    /// pinning the state (`null`/absent = latest). Historical blocks are
    /// served through reth's revert overlay and get costlier with
    /// distance from tip.
    #[method(name = "multiproof")]
    async fn multiproof(
        &self,
        targets: Vec<(B256, Vec<B256>)>,
        block: Option<B256>,
    ) -> RpcResult<MultiproofResponse>;
}

/// Server state: just a cloned node provider; one instance serves all
/// configured transports.
pub struct SiltRpc<P> {
    provider: P,
}

impl<P> SiltRpc<P> {
    pub const fn new(provider: P) -> Self {
        Self { provider }
    }
}

#[async_trait]
impl<P> SiltApiServer for SiltRpc<P>
where
    P: StateProviderFactory + Clone + Send + Sync + 'static,
{
    async fn multiproof(
        &self,
        targets: Vec<(B256, Vec<B256>)>,
        block: Option<B256>,
    ) -> RpcResult<MultiproofResponse> {
        let provider = self.provider.clone();
        // The proof walk reads MDBX synchronously — keep it off the RPC
        // worker threads.
        let mp = tokio::task::spawn_blocking(move || {
            let state = match block {
                Some(hash) => provider.state_by_block_hash(hash),
                None => provider.latest(),
            }
            .map_err(internal)?;

            let mut wanted = MultiProofTargets::default();
            for (hashed_address, hashed_slots) in targets {
                wanted
                    .entry(hashed_address)
                    .or_default()
                    .extend(hashed_slots);
            }
            state
                .multiproof(Default::default(), wanted)
                .map_err(internal)
        })
        .await
        .map_err(internal)??;

        Ok(MultiproofResponse {
            account_subtree: subtree(mp.account_subtree.iter()),
            storages: mp
                .storages
                .into_iter()
                .map(|(hashed_address, sp)| {
                    let storage = StorageProof {
                        root: sp.root,
                        subtree: subtree(sp.subtree.iter()),
                    };
                    (hashed_address, storage)
                })
                .collect(),
        })
    }
}

fn subtree<'a>(nodes: impl Iterator<Item = (&'a Nibbles, &'a Bytes)>) -> BTreeMap<String, Bytes> {
    nodes
        .map(|(path, rlp)| (path_hex(path), rlp.clone()))
        .collect()
}

/// One lowercase hex char per nibble; the empty string is the trie root.
fn path_hex(path: &Nibbles) -> String {
    path.to_vec().iter().map(|n| format!("{n:x}")).collect()
}

fn internal(err: impl std::fmt::Display) -> ErrorObjectOwned {
    ErrorObject::owned(INTERNAL_ERROR_CODE, err.to_string(), None::<()>)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_hex_one_char_per_nibble_empty_is_root() {
        assert_eq!(path_hex(&Nibbles::default()), "");
        let path = Nibbles::from_nibbles([0x0, 0xa, 0x3, 0xf]);
        assert_eq!(path_hex(&path), "0a3f");
    }

    #[test]
    fn response_serializes_to_plain_hex_json() {
        let resp = MultiproofResponse {
            account_subtree: BTreeMap::from([(String::new(), Bytes::from(vec![0x80]))]),
            storages: BTreeMap::from([(
                B256::ZERO,
                StorageProof {
                    root: B256::ZERO,
                    subtree: BTreeMap::new(),
                },
            )]),
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["accountSubtree"][""], "0x80");
        let zero = format!("0x{}", "0".repeat(64));
        assert_eq!(json["storages"][&zero]["root"], zero);
    }
}
