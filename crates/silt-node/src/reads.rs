//! Read touches (PLAN D13): the accounts and slots a block's execution
//! read, published in the diff with their values so that consumer caches
//! hold what the chain is reading before a simulation asks.
//!
//! The host's executor reports them through a [`ReadTouches`] hook
//! ([`ReadTouchSink`] is a ready-made bounded one), keyed by (number, parent
//! hash, digest of the block's transactions), *with* their values (D15):
//! after the block, the executor's cache holds its post-state, which for a
//! key the block didn't write is also the value before it. Nothing is read
//! back from the node's state.
//!
//! Only the engine's own execution of a block may supply values: an RPC
//! simulation of the same transactions on the same parent, with state
//! overrides, must not. The sink enforces it with a per-thread ticket
//! ([`ReadTouchSink::expect`] leaves it where the engine announces the
//! payload; the executor it creates next on that thread claims it with
//! [`ReadTouchSink::claim_ticket`]; [`ReadTouchSink::put`] accepts values
//! only for the block the ticket names). RPC executions never hold one.
//!
//! Pending read touches (PLAN D14): the host may also report what its
//! pool's pending transactions read on the current head
//! ([`ReadTouchSink::add_pending`]). The diff of the next block takes those
//! read on its parent ([`ReadTouches::take_pending`]) and publishes the ones
//! it didn't write: unchanged, so their values hold at its block too. These
//! values come from the node's own state, never from a caller.

use std::cell::Cell;
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::Mutex;

use silt_wire::{AccountChange, Addr, BlockDiff, Word};

/// Keys with their values at one block: a block's read touches (D13/D15)
/// or a pool pass's (D14).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReadValues {
    pub accounts: Vec<(Addr, AccountChange)>,
    pub slots: Vec<(Addr, Word, Word)>,
}

impl ReadValues {
    pub fn len(&self) -> usize {
        self.accounts.len() + self.slots.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Where the ExEx gets read touches from (see the module docs).
pub trait ReadTouches: Send + Sync + 'static {
    /// What the block `number`, built on `parent_hash` with transactions
    /// `txs` ([`tx_digest`]), read, valued at that block (written keys
    /// included; the diff drops those). Each block's are handed out once.
    fn take(&self, number: u64, parent_hash: &Word, txs: &Word) -> Option<ReadValues>;

    /// Pending read touches (D14) for the diff of block `number` on
    /// `parent_hash`: what pool transactions read on that parent, with its
    /// values there, less what was published within the last
    /// [`PENDING_RESEND_BLOCKS`]. Anything read on another head is dropped.
    /// The default has none.
    fn take_pending(&self, _number: u64, _parent_hash: &Word) -> ReadValues {
        ReadValues::default()
    }

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

/// Most pending keys (D14) held between two diffs; more are counted as
/// dropped until the next diff drains them.
pub const MAX_PENDING_KEYS: usize = 20_000;

/// A pending key published within this many blocks isn't published again:
/// consumers hold it, and the diffs keep its value current. After that it
/// is, which also keeps it from aging out of a consumer's cache while the
/// pool keeps reading it.
pub const PENDING_RESEND_BLOCKS: u64 = 64;

/// A block the engine announced: (number, parent hash, transactions
/// digest). See the module docs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Ticket {
    pub number: u64,
    pub parent_hash: Word,
    pub txs: Word,
}

thread_local! {
    /// The ticket [`ReadTouchSink::expect`] left on this thread, until the
    /// next executor created here claims it.
    static TICKET: Cell<Option<Ticket>> = const { Cell::new(None) };
}

/// Pending read touches waiting for the next diff: read on `head`, with
/// their values there; and when each key was last published.
#[derive(Debug, Default)]
struct Pending {
    head: Option<(u64, Word)>,
    accounts: HashMap<Addr, AccountChange>,
    slots: HashMap<(Addr, Word), Word>,
    sent_accounts: HashMap<Addr, u64>,
    sent_slots: HashMap<(Addr, Word), u64>,
}

/// One expected block: what the engine is about to execute.
#[derive(Debug)]
struct Expected {
    ticket: Ticket,
    values: Option<ReadValues>,
}

/// Counters for [`ReadTouches::stats`].
#[derive(Debug, Default)]
struct Counters {
    blocks: u64,
    keys: u64,
    dropped_keys: u64,
    /// Executions without the engine's ticket for the block they ran (RPC
    /// simulations, pending blocks, blocks synced from the network):
    /// ignored.
    unannounced: u64,
    /// Further executions of an announced block already filled: ignored.
    refilled: u64,
    collect_us_sum: u64,
    collect_us_max: u64,
    /// D14: pool transactions the host executed, the passes it took, the
    /// time they took, keys collected and dropped at the cap, keys skipped
    /// as recently published, and keys handed to a diff.
    pending_txs: u64,
    pending_passes: u64,
    pending_us_sum: u64,
    pending_us_max: u64,
    pending_keys: u64,
    pending_dropped: u64,
    pending_skipped: u64,
    pending_taken: u64,
    /// Pending touches dropped for having been read on another head than
    /// the diff's parent (a late pass, or a block that arrived first).
    pending_stale: u64,
}

/// A bounded sink between the engine ([`ReadTouchSink::expect`]), the
/// executor ([`ReadTouchSink::claim_ticket`], [`ReadTouchSink::put`]) and
/// the ExEx ([`ReadTouches::take`]).
///
/// Only the engine's own executions are kept (the module docs): the node's
/// RPC can run the same executor (`eth_simulateV1`) on the same parent,
/// with overrides. Keeps the newest `cap` expected blocks; a block that
/// never commits ages out.
#[derive(Debug)]
pub struct ReadTouchSink {
    blocks: Mutex<VecDeque<Expected>>,
    pending: Mutex<Pending>,
    counters: Mutex<Counters>,
    cap: usize,
}

impl ReadTouchSink {
    pub fn new(cap: usize) -> Self {
        Self {
            blocks: Mutex::new(VecDeque::with_capacity(cap)),
            pending: Mutex::default(),
            counters: Mutex::default(),
            cap: cap.max(1),
        }
    }

    /// The engine is about to execute block `number` on `parent_hash` with
    /// transactions `txs` ([`tx_digest`]). Call it where the engine
    /// announces the payload (reth: `context_for_payload`): it also leaves
    /// this thread's ticket for the executor created next.
    pub fn expect(&self, number: u64, parent_hash: Word, txs: Word) {
        let ticket = Ticket {
            number,
            parent_hash,
            txs,
        };
        TICKET.set(Some(ticket));
        let mut blocks = self.blocks.lock().expect("read sink lock");
        if blocks.iter().any(|e| e.ticket == ticket) {
            return;
        }
        blocks.push_back(Expected {
            ticket,
            values: None,
        });
        while blocks.len() > self.cap {
            blocks.pop_front();
        }
    }

    /// Takes this thread's ticket, if [`Self::expect`] left one. Call it
    /// where an executor is created, and hand the ticket to
    /// [`Self::put`]; every executor claims, so a ticket never outlives the
    /// executor it was left for.
    pub fn claim_ticket() -> Option<Ticket> {
        TICKET.take()
    }

    /// An execution finished: it ran `executed` (number, parent hash and
    /// the digest of the transactions it actually executed) and read
    /// `values` at its end, collected in `collect_us`. Kept only when
    /// `claimed` (its executor's ticket) names exactly that block, the
    /// block is still expected and not yet filled; at most
    /// [`MAX_KEYS_PER_BLOCK`] keys.
    pub fn put(
        &self,
        claimed: Option<Ticket>,
        executed: Ticket,
        mut values: ReadValues,
        collect_us: u64,
    ) {
        let mut counters = self.counters.lock().expect("read sink counters lock");
        if claimed != Some(executed) {
            counters.unannounced += 1;
            return;
        }
        let mut blocks = self.blocks.lock().expect("read sink lock");
        let slot = match blocks.iter_mut().find(|e| e.ticket == executed) {
            None => {
                counters.unannounced += 1;
                return;
            }
            Some(slot) if slot.values.is_some() => {
                counters.refilled += 1;
                return;
            }
            Some(slot) => slot,
        };
        let total = values.len();
        if total > MAX_KEYS_PER_BLOCK {
            values.accounts.truncate(MAX_KEYS_PER_BLOCK);
            values
                .slots
                .truncate(MAX_KEYS_PER_BLOCK - values.accounts.len());
            counters.dropped_keys += (total - MAX_KEYS_PER_BLOCK) as u64;
        }
        counters.blocks += 1;
        counters.keys += values.len() as u64;
        counters.collect_us_sum += collect_us;
        counters.collect_us_max = counters.collect_us_max.max(collect_us);
        slot.values = Some(values);
    }

    /// D14: one pass of the host's pool worker on head `(number, hash)`:
    /// `txs` pool transactions executed in `pass_us`, reading `values` (at
    /// that head). Held, deduplicated, until the next diff takes them; at
    /// most [`MAX_PENDING_KEYS`]. A pass on a newer head replaces what an
    /// older one left; a pass on an older head is dropped.
    pub fn add_pending(&self, head: (u64, Word), values: ReadValues, txs: u64, pass_us: u64) {
        let mut pending = self.pending.lock().expect("read sink pending lock");
        let (mut added, mut dropped, mut stale) = (0u64, 0u64, 0u64);
        match pending.head {
            Some(held) if held.0 > head.0 => stale = values.len() as u64,
            Some(held) if held == head => {}
            _ => {
                stale = (pending.accounts.len() + pending.slots.len()) as u64;
                pending.accounts.clear();
                pending.slots.clear();
                pending.head = Some(head);
            }
        }
        if pending.head == Some(head) {
            for (addr, change) in values.accounts {
                if pending.accounts.len() + pending.slots.len() >= MAX_PENDING_KEYS {
                    dropped += 1;
                } else if pending.accounts.insert(addr, change).is_none() {
                    added += 1;
                }
            }
            for (addr, slot, value) in values.slots {
                if pending.accounts.len() + pending.slots.len() >= MAX_PENDING_KEYS {
                    dropped += 1;
                } else if pending.slots.insert((addr, slot), value).is_none() {
                    added += 1;
                }
            }
        }
        drop(pending);
        let mut c = self.counters.lock().expect("read sink counters lock");
        c.pending_txs += txs;
        c.pending_passes += 1;
        c.pending_us_sum += pass_us;
        c.pending_us_max = c.pending_us_max.max(pass_us);
        c.pending_keys += added;
        c.pending_dropped += dropped;
        c.pending_stale += stale;
    }
}

impl ReadTouches for ReadTouchSink {
    fn take(&self, number: u64, parent_hash: &Word, txs: &Word) -> Option<ReadValues> {
        let ticket = Ticket {
            number,
            parent_hash: *parent_hash,
            txs: *txs,
        };
        let mut blocks = self.blocks.lock().expect("read sink lock");
        let at = blocks
            .iter()
            .position(|e| e.ticket == ticket && e.values.is_some())?;
        blocks.remove(at).and_then(|e| e.values)
    }

    fn take_pending(&self, number: u64, parent_hash: &Word) -> ReadValues {
        let mut pending = self.pending.lock().expect("read sink pending lock");
        let Pending {
            head,
            accounts,
            slots,
            sent_accounts,
            sent_slots,
        } = &mut *pending;
        let mut out = ReadValues::default();
        let (mut skipped, mut stale) = (0u64, 0u64);
        if *head != Some((number.saturating_sub(1), *parent_hash)) {
            stale = (accounts.len() + slots.len()) as u64;
            accounts.clear();
            slots.clear();
        }
        let fresh = |sent: Option<&u64>| {
            sent.is_none_or(|&at| number.saturating_sub(at) >= PENDING_RESEND_BLOCKS)
        };
        for (addr, change) in accounts.drain() {
            if fresh(sent_accounts.get(&addr)) {
                sent_accounts.insert(addr, number);
                out.accounts.push((addr, change));
            } else {
                skipped += 1;
            }
        }
        for ((addr, slot), value) in slots.drain() {
            if fresh(sent_slots.get(&(addr, slot))) {
                sent_slots.insert((addr, slot), number);
                out.slots.push((addr, slot, value));
            } else {
                skipped += 1;
            }
        }
        // Forget publications older than the window (bounded: at most
        // ~PENDING_RESEND_BLOCKS diffs' worth of keys).
        if number.is_multiple_of(16) {
            let live = |&at: &u64| number.saturating_sub(at) < PENDING_RESEND_BLOCKS;
            sent_accounts.retain(|_, at| live(at));
            sent_slots.retain(|_, at| live(at));
        }
        drop(pending);
        let mut c = self.counters.lock().expect("read sink counters lock");
        c.pending_skipped += skipped;
        c.pending_stale += stale;
        c.pending_taken += out.len() as u64;
        out
    }

    fn stats(&self) -> Option<String> {
        let c = std::mem::take(&mut *self.counters.lock().expect("read sink counters lock"));
        Some(format!(
            "blocks {} keys/block {} collect µs avg {} max {} dropped keys {} unannounced executions {} refilled {} | \
             pending: passes {} txs {} pass ms avg {:.1} max {:.1} keys {} dropped {} stale {} skipped (recent) {} to diffs {}",
            c.blocks,
            c.keys / c.blocks.max(1),
            c.collect_us_sum / c.blocks.max(1),
            c.collect_us_max,
            c.dropped_keys,
            c.unannounced,
            c.refilled,
            c.pending_passes,
            c.pending_txs,
            c.pending_us_sum as f64 / 1e3 / c.pending_passes.max(1) as f64,
            c.pending_us_max as f64 / 1e3,
            c.pending_keys,
            c.pending_dropped,
            c.pending_stale,
            c.pending_skipped,
            c.pending_taken
        ))
    }
}

/// What the diff's span wrote: keys a read touch must not carry (the diff
/// carries their values), slots of destroyed accounts included.
struct Written {
    accounts: BTreeSet<Addr>,
    destroyed: BTreeSet<Addr>,
    slots: BTreeSet<(Addr, Word)>,
}

impl Written {
    fn of(diff: &BlockDiff) -> Self {
        Self {
            accounts: diff.accounts.iter().map(|(a, _)| *a).collect(),
            destroyed: diff
                .accounts
                .iter()
                .filter(|(_, c)| c.is_none())
                .map(|(a, _)| *a)
                .collect(),
            slots: diff.storage.iter().map(|(a, s, _)| (*a, *s)).collect(),
        }
    }

    fn account(&self, addr: &Addr) -> bool {
        self.accounts.contains(addr)
    }

    fn slot(&self, key: &(Addr, Word)) -> bool {
        self.destroyed.contains(&key.0) || self.slots.contains(key)
    }
}

/// Adds read touches to `diff`: those the span didn't write (nor slots of an
/// account it destroyed) and the diff doesn't already carry. A block's own
/// (D15) were read at that block, a pool pass's (D14) on the diff's parent;
/// either way, unwritten, they hold at the diff's block. Sorted. Returns
/// how many it added.
pub fn merge_reads(diff: &mut BlockDiff, pending: ReadValues) -> usize {
    let written = Written::of(diff);
    let held_accounts: BTreeSet<Addr> = diff.read_accounts.iter().map(|(a, _)| *a).collect();
    let held_slots: BTreeSet<(Addr, Word)> =
        diff.read_storage.iter().map(|(a, s, _)| (*a, *s)).collect();
    let before = diff.read_accounts.len() + diff.read_storage.len();
    diff.read_accounts.extend(
        pending
            .accounts
            .into_iter()
            .filter(|(a, _)| !written.account(a) && !held_accounts.contains(a)),
    );
    diff.read_storage
        .extend(pending.slots.into_iter().filter(|(a, s, _)| {
            let key = (*a, *s);
            !written.slot(&key) && !held_slots.contains(&key)
        }));
    diff.read_accounts.sort_unstable_by_key(|(a, _)| *a);
    diff.read_storage.sort_unstable_by_key(|(a, s, _)| (*a, *s));
    diff.read_accounts.len() + diff.read_storage.len() - before
}

#[cfg(test)]
mod tests {
    use super::*;
    use silt_wire::KECCAK_EMPTY;

    fn change(n: u8) -> AccountChange {
        AccountChange {
            balance: [n; 32],
            nonce: n as u64,
            code_hash: KECCAK_EMPTY,
        }
    }

    fn values(accounts: &[u8], slots: &[(u8, u8, u8)]) -> ReadValues {
        ReadValues {
            accounts: accounts.iter().map(|a| ([*a; 20], change(*a))).collect(),
            slots: slots
                .iter()
                .map(|(a, s, v)| ([*a; 20], [*s; 32], [*v; 32]))
                .collect(),
        }
    }

    fn sorted(mut v: ReadValues) -> ReadValues {
        v.accounts.sort_unstable_by_key(|(a, _)| *a);
        v.slots.sort_unstable_by_key(|(a, s, _)| (*a, *s));
        v
    }

    fn ticket(number: u64, parent: u8, txs: Word) -> Ticket {
        Ticket {
            number,
            parent_hash: [parent; 32],
            txs,
        }
    }

    /// D15: values are kept only from an executor holding the engine's
    /// ticket for exactly the block it ran, once per block; the sink stays
    /// bounded.
    #[test]
    fn sink_keeps_only_ticketed_executions_once_and_stays_bounded() {
        let sink = ReadTouchSink::new(2);
        let txs = tx_digest(&[[0xAB; 32]]);
        let t10 = ticket(10, 1, txs);
        // An execution nobody announced (an RPC simulation) is ignored.
        sink.put(None, t10, values(&[9], &[]), 5);
        assert_eq!(sink.take(10, &[1; 32], &txs), None);

        sink.expect(10, [1; 32], txs);
        let claimed = ReadTouchSink::claim_ticket();
        assert_eq!(claimed, Some(t10));
        assert_eq!(ReadTouchSink::claim_ticket(), None, "claimed once");
        assert_eq!(sink.take(10, &[1; 32], &txs), None, "nothing put yet");
        // The same block without the ticket (an RPC simulation of the same
        // transactions on the same parent, overrides and all): ignored.
        sink.put(None, t10, values(&[8], &[]), 7);
        // The ticketed executor ran other transactions: ignored.
        sink.put(claimed, ticket(10, 1, tx_digest(&[])), values(&[7], &[]), 7);
        sink.put(claimed, t10, values(&[1], &[(2, 2, 2)]), 7);
        // A second engine execution of the same block doesn't replace them.
        sink.put(claimed, t10, values(&[6], &[]), 7);
        // Same block number and parent, other transactions: not this block.
        assert_eq!(sink.take(10, &[1; 32], &tx_digest(&[])), None);
        assert_eq!(
            sink.take(10, &[1; 32], &txs),
            Some(values(&[1], &[(2, 2, 2)]))
        );
        assert_eq!(sink.take(10, &[1; 32], &txs), None, "taken once");

        for (n, p) in [(11, 2), (12, 3), (13, 4)] {
            sink.expect(n, [p; 32], txs);
            let claimed = ReadTouchSink::claim_ticket();
            sink.put(claimed, ticket(n, p, txs), values(&[p], &[]), 1);
        }
        assert_eq!(sink.take(11, &[2; 32], &txs), None, "aged out past cap");
        assert_eq!(sink.take(13, &[4; 32], &txs), Some(values(&[4], &[])));
        let stats = sink.stats().unwrap();
        assert!(
            stats.contains("unannounced executions 3 refilled 1"),
            "{stats}"
        );
    }

    /// The ticket stays on the thread that announced the payload.
    #[test]
    fn ticket_is_per_thread() {
        let sink = ReadTouchSink::new(2);
        sink.expect(5, [5; 32], [0; 32]);
        let elsewhere = std::thread::spawn(ReadTouchSink::claim_ticket)
            .join()
            .unwrap();
        assert_eq!(elsewhere, None);
        assert_eq!(ReadTouchSink::claim_ticket(), Some(ticket(5, 5, [0; 32])));
    }

    /// D14: pending touches go to the diff built on the head they were read
    /// on, once; anything read on another head is dropped; a key published
    /// within the resend window isn't published again.
    #[test]
    fn pending_follows_the_parent_head_and_resend_window() {
        let sink = ReadTouchSink::new(4);
        let (h9, h10) = ([9; 32], [10; 32]);
        sink.add_pending((9, h9), values(&[1], &[(1, 1, 7)]), 2, 100);
        sink.add_pending((9, h9), values(&[2, 1], &[]), 1, 50);
        // A late pass on an older head adds nothing.
        sink.add_pending((8, [8; 32]), values(&[3], &[]), 1, 10);
        assert_eq!(
            sorted(sink.take_pending(10, &h9)),
            values(&[1, 2], &[(1, 1, 7)])
        );
        assert!(sink.take_pending(10, &h9).is_empty(), "drained");

        // Read on h10, but the next diff is on a fork's other block: stale.
        sink.add_pending((10, h10), values(&[4], &[]), 1, 10);
        assert!(sink.take_pending(11, &[0xEE; 32]).is_empty());

        // Published at 10: skipped until 10 + PENDING_RESEND_BLOCKS.
        sink.add_pending((10, h10), values(&[1, 5], &[]), 1, 10);
        assert_eq!(sink.take_pending(11, &h10), values(&[5], &[]));
        let later = 10 + PENDING_RESEND_BLOCKS;
        sink.add_pending((later - 1, [1; 32]), values(&[1], &[]), 1, 10);
        assert_eq!(sink.take_pending(later, &[1; 32]), values(&[1], &[]));

        let stats = sink.stats().unwrap();
        assert!(stats.contains("passes 6 txs 7"), "{stats}");
        assert!(stats.contains("skipped (recent) 1"), "{stats}");
    }

    /// Read touches the diff wrote, slots of accounts it destroyed and keys
    /// it already carries stay out.
    #[test]
    fn merge_reads_skips_written_and_held_keys() {
        let mut diff = BlockDiff {
            span_start: 5,
            number: 5,
            hash: [5; 32],
            parent_hash: [4; 32],
            heads: vec![],
            accounts: vec![([1; 20], Some(change(1))), ([2; 20], None)],
            storage: vec![([3; 20], [3; 32], [33; 32])],
            code: vec![],
            receipts: vec![],
            read_accounts: vec![([4; 20], change(4))],
            read_storage: vec![],
        };
        let added = merge_reads(
            &mut diff,
            values(&[1, 4, 6], &[(2, 1, 1), (3, 3, 9), (3, 4, 9), (7, 7, 7)]),
        );
        assert_eq!(added, 3);
        assert_eq!(
            diff.read_accounts,
            vec![([4; 20], change(4)), ([6; 20], change(6))]
        );
        assert_eq!(
            diff.read_storage,
            vec![([3; 20], [4; 32], [9; 32]), ([7; 20], [7; 32], [7; 32])]
        );
    }

    #[test]
    fn sink_caps_keys_per_block() {
        let sink = ReadTouchSink::new(4);
        let txs = tx_digest(&[]);
        sink.expect(1, [0; 32], txs);
        let claimed = ReadTouchSink::claim_ticket();
        let many = ReadValues {
            accounts: vec![([1; 20], change(1)); MAX_KEYS_PER_BLOCK],
            slots: vec![([1; 20], [2; 32], [3; 32]); 10],
        };
        sink.put(claimed, ticket(1, 0, txs), many, 1);
        let got = sink.take(1, &[0; 32], &txs).unwrap();
        assert_eq!(got.len(), MAX_KEYS_PER_BLOCK);
        assert!(sink.stats().unwrap().contains("dropped keys 10"));
    }
}
