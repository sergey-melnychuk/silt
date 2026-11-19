//! Read touches (PLAN D13): the keys a block's execution read, reported by
//! the host's executor, published in the diff with their values so that
//! consumer caches hold what the chain is reading before a simulation asks.
//!
//! The executor supplies keys only, keyed by (number, parent hash, digest
//! of the block's transactions), through a [`ReadTouches`] hook
//! ([`ReadTouchSink`] is a ready-made bounded one that keeps only blocks the
//! engine announced).
//! Values always come from the node's committed state at the published
//! block ([`fill_reads`]), so any execution on the same parent, even a
//! simulation with state overrides, can only add keys, never wrong values.

use std::collections::{BTreeSet, VecDeque};
use std::sync::Mutex;

use alloy_primitives::{Address, B256};
use reth_ethereum::provider::StateProviderFactory;
use silt_wire::{AccountChange, Addr, BlockDiff, KECCAK_EMPTY, Word};

/// Keys one block's execution read (written ones included; the diff drops
/// those).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReadKeys {
    pub accounts: Vec<Addr>,
    pub slots: Vec<(Addr, Word)>,
}

impl ReadKeys {
    pub fn is_empty(&self) -> bool {
        self.accounts.is_empty() && self.slots.is_empty()
    }

    pub fn extend(&mut self, other: ReadKeys) {
        self.accounts.extend(other.accounts);
        self.slots.extend(other.slots);
    }
}

/// Where the ExEx gets read keys from (see the module docs).
pub trait ReadTouches: Send + Sync + 'static {
    /// The keys the block `number`, built on `parent_hash` with
    /// transactions `txs` ([`tx_digest`]), read; each block's keys are
    /// handed out once.
    fn take(&self, number: u64, parent_hash: &Word, txs: &Word) -> Option<ReadKeys>;

    /// One summary line's worth of counters since the last call (logged by
    /// the ExEx every few hundred blocks); `None` if the source keeps none.
    fn stats(&self) -> Option<String> {
        None
    }
}

/// Identifies a block's transaction list: keccak of its transaction hashes
/// in order. The engine path and the executor compute it independently, so
/// only an execution of exactly the block's transactions on its parent can
/// report its keys.
pub fn tx_digest<'a>(hashes: impl IntoIterator<Item = &'a Word>) -> Word {
    let mut hasher = alloy_primitives::Keccak256::new();
    for h in hashes {
        hasher.update(h);
    }
    hasher.finalize().0
}

/// Most keys (accounts + slots) taken from one block; a block that reads
/// more keeps the first ones and counts the rest as dropped. Mainnet blocks
/// read ~2,000.
pub const MAX_KEYS_PER_BLOCK: usize = 20_000;

/// One expected block: what the engine is about to execute.
#[derive(Debug)]
struct Expected {
    number: u64,
    parent_hash: Word,
    txs: Word,
    keys: Option<ReadKeys>,
}

/// Counters for [`ReadTouches::stats`].
#[derive(Debug, Default)]
struct Counters {
    blocks: u64,
    keys: u64,
    dropped_keys: u64,
    /// Executions with no matching expected block (RPC simulations,
    /// executions other than block import): ignored.
    unexpected: u64,
    collect_us_sum: u64,
    collect_us_max: u64,
}

/// A bounded sink between the engine ([`ReadTouchSink::expect`]), the
/// executor ([`ReadTouchSink::put`]) and the ExEx ([`ReadTouches::take`]).
///
/// Only executions the engine announced are kept: the node's RPC can run
/// the same executor (`eth_simulateV1`) on the same parent, and its keys,
/// though never its values, would otherwise land in the feed. Keeps the
/// newest `cap` expected blocks; a block that never commits ages out.
#[derive(Debug)]
pub struct ReadTouchSink {
    blocks: Mutex<VecDeque<Expected>>,
    counters: Mutex<Counters>,
    cap: usize,
}

impl ReadTouchSink {
    pub fn new(cap: usize) -> Self {
        Self {
            blocks: Mutex::new(VecDeque::with_capacity(cap)),
            counters: Mutex::default(),
            cap: cap.max(1),
        }
    }

    /// The engine is about to execute block `number` on `parent_hash` with
    /// transactions `txs` ([`tx_digest`]).
    pub fn expect(&self, number: u64, parent_hash: Word, txs: Word) {
        let mut blocks = self.blocks.lock().expect("read sink lock");
        if blocks
            .iter()
            .any(|e| (e.number, e.parent_hash, e.txs) == (number, parent_hash, txs))
        {
            return;
        }
        blocks.push_back(Expected {
            number,
            parent_hash,
            txs,
            keys: None,
        });
        while blocks.len() > self.cap {
            blocks.pop_front();
        }
    }

    /// An execution finished: block `number` on `parent_hash` with
    /// transactions `txs` loaded `keys`, collected in `collect_us`. Kept
    /// only for an expected block not yet filled, at most
    /// [`MAX_KEYS_PER_BLOCK`] keys.
    pub fn put(
        &self,
        number: u64,
        parent_hash: Word,
        txs: Word,
        mut keys: ReadKeys,
        collect_us: u64,
    ) {
        let mut blocks = self.blocks.lock().expect("read sink lock");
        let slot = blocks.iter_mut().find(|e| {
            (e.number, e.parent_hash, e.txs) == (number, parent_hash, txs) && e.keys.is_none()
        });
        let mut counters = self.counters.lock().expect("read sink counters lock");
        let Some(slot) = slot else {
            counters.unexpected += 1;
            return;
        };
        let total = keys.accounts.len() + keys.slots.len();
        if total > MAX_KEYS_PER_BLOCK {
            keys.accounts.truncate(MAX_KEYS_PER_BLOCK);
            keys.slots
                .truncate(MAX_KEYS_PER_BLOCK - keys.accounts.len());
            counters.dropped_keys += (total - MAX_KEYS_PER_BLOCK) as u64;
        }
        counters.blocks += 1;
        counters.keys += (keys.accounts.len() + keys.slots.len()) as u64;
        counters.collect_us_sum += collect_us;
        counters.collect_us_max = counters.collect_us_max.max(collect_us);
        slot.keys = Some(keys);
    }
}

impl ReadTouches for ReadTouchSink {
    fn take(&self, number: u64, parent_hash: &Word, txs: &Word) -> Option<ReadKeys> {
        let mut blocks = self.blocks.lock().expect("read sink lock");
        let at = blocks.iter().position(|e| {
            (e.number, e.parent_hash, e.txs) == (number, *parent_hash, *txs) && e.keys.is_some()
        })?;
        blocks.remove(at).and_then(|e| e.keys)
    }

    fn stats(&self) -> Option<String> {
        let c = std::mem::take(&mut *self.counters.lock().expect("read sink counters lock"));
        Some(format!(
            "blocks {} keys/block {} collect µs avg {} max {} dropped keys {} unexpected executions {}",
            c.blocks,
            c.keys / c.blocks.max(1),
            c.collect_us_sum / c.blocks.max(1),
            c.collect_us_max,
            c.dropped_keys,
            c.unexpected
        ))
    }
}

/// Fills `diff.read_accounts` / `diff.read_storage` from `keys`: every key
/// the span did not write (nor a slot of an account it destroyed), valued
/// at the diff's own block from `provider`. Sorted, deduplicated. Blocking:
/// run it off the async runtime.
pub fn fill_reads<P: StateProviderFactory>(
    provider: &P,
    diff: &mut BlockDiff,
    keys: ReadKeys,
) -> eyre::Result<()> {
    let written: BTreeSet<Addr> = diff.accounts.iter().map(|(a, _)| *a).collect();
    let destroyed: BTreeSet<Addr> = diff
        .accounts
        .iter()
        .filter(|(_, c)| c.is_none())
        .map(|(a, _)| *a)
        .collect();
    let written_slots: BTreeSet<(Addr, Word)> =
        diff.storage.iter().map(|(a, s, _)| (*a, *s)).collect();
    let accounts: BTreeSet<Addr> = keys
        .accounts
        .into_iter()
        .filter(|a| !written.contains(a))
        .collect();
    let slots: BTreeSet<(Addr, Word)> = keys
        .slots
        .into_iter()
        .filter(|k| !destroyed.contains(&k.0) && !written_slots.contains(k))
        .collect();
    if accounts.is_empty() && slots.is_empty() {
        return Ok(());
    }

    let state = provider.state_by_block_hash(B256::from(diff.hash))?;
    diff.read_accounts = accounts
        .into_iter()
        .map(|addr| {
            let account = state.basic_account(&Address::from(addr))?;
            Ok((
                addr,
                match account {
                    Some(a) => AccountChange {
                        balance: a.balance.to_be_bytes::<32>(),
                        nonce: a.nonce,
                        code_hash: a.bytecode_hash.map_or(KECCAK_EMPTY, |h| h.0),
                    },
                    // Absent reads as the empty account, as `eth_*` does.
                    None => AccountChange {
                        balance: [0; 32],
                        nonce: 0,
                        code_hash: KECCAK_EMPTY,
                    },
                },
            ))
        })
        .collect::<eyre::Result<_>>()?;
    diff.read_storage = slots
        .into_iter()
        .map(|(addr, slot)| {
            let value = state
                .storage(Address::from(addr), B256::from(slot))?
                .unwrap_or_default();
            Ok((addr, slot, value.to_be_bytes::<32>()))
        })
        .collect::<eyre::Result<_>>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(accounts: &[u8], slots: &[(u8, u8)]) -> ReadKeys {
        ReadKeys {
            accounts: accounts.iter().map(|a| [*a; 20]).collect(),
            slots: slots.iter().map(|(a, s)| ([*a; 20], [*s; 32])).collect(),
        }
    }

    #[test]
    fn sink_keeps_only_expected_blocks_once_and_stays_bounded() {
        let sink = ReadTouchSink::new(2);
        let txs = tx_digest(&[[0xAB; 32]]);
        // An execution nobody announced (an RPC simulation) is ignored.
        sink.put(10, [1; 32], txs, keys(&[9], &[]), 5);
        assert_eq!(sink.take(10, &[1; 32], &txs), None);

        sink.expect(10, [1; 32], txs);
        assert_eq!(sink.take(10, &[1; 32], &txs), None, "nothing put yet");
        sink.put(10, [1; 32], txs, keys(&[1], &[(2, 2)]), 7);
        // A second execution of the same block doesn't add keys.
        sink.put(10, [1; 32], txs, keys(&[8], &[]), 7);
        // Same block number and parent, other transactions: not this block.
        assert_eq!(sink.take(10, &[1; 32], &tx_digest(&[])), None);
        assert_eq!(sink.take(10, &[1; 32], &txs), Some(keys(&[1], &[(2, 2)])));
        assert_eq!(sink.take(10, &[1; 32], &txs), None, "taken once");

        for (n, p) in [(11, 2), (12, 3), (13, 4)] {
            sink.expect(n, [p; 32], txs);
            sink.put(n, [p; 32], txs, keys(&[p], &[]), 1);
        }
        assert_eq!(sink.take(11, &[2; 32], &txs), None, "aged out past cap");
        assert_eq!(sink.take(13, &[4; 32], &txs), Some(keys(&[4], &[])));
        let stats = sink.stats().unwrap();
        assert!(stats.contains("unexpected executions 2"), "{stats}");
    }

    #[test]
    fn sink_caps_keys_per_block() {
        let sink = ReadTouchSink::new(4);
        let txs = tx_digest(&[]);
        sink.expect(1, [0; 32], txs);
        let many = ReadKeys {
            accounts: vec![[1; 20]; MAX_KEYS_PER_BLOCK],
            slots: vec![([1; 20], [2; 32]); 10],
        };
        sink.put(1, [0; 32], txs, many, 1);
        let got = sink.take(1, &[0; 32], &txs).unwrap();
        assert_eq!(got.accounts.len() + got.slots.len(), MAX_KEYS_PER_BLOCK);
        assert!(sink.stats().unwrap().contains("dropped keys 10"));
    }
}
