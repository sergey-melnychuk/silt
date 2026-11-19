//! The state store: concurrent maps + the apply path (PLAN T2.2).
//!
//! Coherence discipline (PLAN §E, D3):
//! - diff-derived writes always clobber (`insert_diff`) — they are the
//!   authoritative post-state;
//! - lazily-fetched values never clobber (`insert_lazy` = insert-if-absent) —
//!   if a diff touched the key first, the diff's value wins; this closes the
//!   fetch/apply race without locks.
//! - the provenance bit is recorded per entry (reserved for the layered v1).
//!
//! Reorgs (PLAN D12): every applied diff journals the entries it replaced,
//! so [`Store::rollback`] can undo the blocks a reorg dropped instead of
//! dropping the whole cache. Lazy fills carry the store's epoch from when
//! their read was pinned; a rollback or clear bumps it, and a fill from an
//! older epoch is discarded (it may have read a dropped block).

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
use silt_wire::{Addr, BlockDiff, HeadLite, Word};

pub use silt_wire::KECCAK_EMPTY;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Provenance {
    Diff,
    Lazy,
    /// A read touch from the feed (PLAN D13): a value the chain read at a
    /// block, current at that block.
    Read,
}

#[derive(Clone, Debug)]
pub struct AccountEntry {
    pub balance: Word,
    pub nonce: u64,
    /// Code hash; bytes live in the code map (or come lazily via RPC).
    pub code_hash: Word,
    pub provenance: Provenance,
    pub last_read: Arc<AtomicU64>,
}

#[derive(Clone, Debug)]
pub struct SlotEntry {
    pub value: Word,
    pub provenance: Provenance,
    pub last_read: Arc<AtomicU64>,
}

#[derive(Default)]
pub struct Stats {
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    /// Entries a prefetch fetched ahead of a read (batched; not misses).
    pub prefetched: AtomicU64,
    pub applied_blocks: AtomicU64,
    pub cold_drops: AtomicU64,
    /// Reorgs undone by [`Store::rollback`], and the blocks they undid.
    pub rollbacks: AtomicU64,
    pub rolled_back_blocks: AtomicU64,
}

/// Blocks of undo journal kept by default: about mainnet finality, far
/// beyond the one- or two-block reorgs it sees (PLAN D12).
pub const DEFAULT_REORG_DEPTH: usize = 64;

/// One entry an applied diff (or a lazy fill) replaced.
#[derive(Debug)]
enum Undo {
    /// The account entry before; `None` = not held.
    Account(Addr, Option<AccountEntry>),
    /// The slot entry before; `None` = not held.
    Slot(Addr, Word, Option<SlotEntry>),
}

#[derive(Debug)]
struct BlockUndo {
    /// The tip the diff was applied on; `None` = it seeded a cold store.
    prev_tip: Option<(u64, Word)>,
    number: u64,
    /// In the order applied; undone in reverse.
    ops: Vec<Undo>,
}

#[derive(Debug, Default)]
struct Journal {
    /// Bumped by every rollback and clear.
    epoch: u64,
    /// Oldest first; at most `reorg_depth` blocks.
    blocks: VecDeque<BlockUndo>,
}

pub struct Store {
    accounts: DashMap<Addr, AccountEntry>,
    storage: DashMap<(Addr, Word), SlotEntry>,
    code: DashMap<Word, Arc<Vec<u8>>>,
    /// Which slots we hold per owner — destroyed accounts drop all of them.
    slots_by_acc: DashMap<Addr, HashSet<Word>>,
    /// Recent headers, newest at the back (bounded ring).
    heads: parking_lot_free::Mutex<VecDeque<HeadLite>>,
    tip: parking_lot_free::Mutex<Option<(u64, Word)>>,
    pub stats: Stats,
    heads_cap: usize,
    /// Taken by `apply`, `rollback`, `clear` and lazy fills, before any
    /// other lock in this struct.
    journal: std::sync::Mutex<Journal>,
    reorg_depth: usize,
}

/// std Mutex with a short alias (no parking_lot dependency).
mod parking_lot_free {
    pub type Mutex<T> = std::sync::Mutex<T>;
}

impl Store {
    pub fn new(heads_cap: usize) -> Self {
        Self::with_reorg_depth(heads_cap, DEFAULT_REORG_DEPTH)
    }

    /// A store that can roll back reorgs up to `reorg_depth` blocks deep;
    /// `0` keeps no journal (every reorg drops to cold, as in D3).
    pub fn with_reorg_depth(heads_cap: usize, reorg_depth: usize) -> Self {
        Self {
            accounts: DashMap::new(),
            storage: DashMap::new(),
            code: DashMap::new(),
            slots_by_acc: DashMap::new(),
            heads: parking_lot_free::Mutex::new(VecDeque::new()),
            tip: parking_lot_free::Mutex::new(None),
            stats: Stats::default(),
            heads_cap,
            journal: std::sync::Mutex::new(Journal::default()),
            reorg_depth,
        }
    }

    /// The epoch a lazy read must be pinned in for its fill to land.
    pub fn epoch(&self) -> u64 {
        self.journal.lock().expect("journal lock").epoch
    }

    /// The tip and the epoch, read together: what a lazy read pins to.
    pub fn tip_and_epoch(&self) -> (Option<(u64, Word)>, u64) {
        let journal = self.journal.lock().expect("journal lock");
        (self.tip(), journal.epoch)
    }

    pub fn tip(&self) -> Option<(u64, Word)> {
        *self.tip.lock().expect("tip lock")
    }

    pub fn tip_head(&self) -> Option<HeadLite> {
        self.heads.lock().expect("heads lock").back().cloned()
    }

    pub fn head(&self, number: u64) -> Option<HeadLite> {
        self.heads
            .lock()
            .expect("heads lock")
            .iter()
            .rev()
            .find(|h| h.number == number)
            .cloned()
    }

    /// Applies one validated diff. Caller has already checked chain
    /// continuity; this is pure upsert (PLAN §E apply semantics).
    pub fn apply(&self, diff: &BlockDiff) {
        let mut journal = self.journal.lock().expect("journal lock");
        let mut ops = Vec::new();
        for (hash, bytes) in &diff.code {
            // Content-addressed: immune to reorgs, never journaled.
            self.code
                .entry(*hash)
                .or_insert_with(|| Arc::new(bytes.clone()));
        }
        for (addr, change) in &diff.accounts {
            match change {
                None => {
                    // Destroyed: the account and every slot we hold for it.
                    if let Some((_, prev)) = self.accounts.remove(addr) {
                        ops.push(Undo::Account(*addr, Some(prev)));
                    }
                    if let Some((_, slots)) = self.slots_by_acc.remove(addr) {
                        for slot in slots {
                            if let Some((_, prev)) = self.storage.remove(&(*addr, slot)) {
                                ops.push(Undo::Slot(*addr, slot, Some(prev)));
                            }
                        }
                    }
                }
                Some(c) => {
                    let prev = self.accounts.insert(
                        *addr,
                        AccountEntry {
                            balance: c.balance,
                            nonce: c.nonce,
                            code_hash: c.code_hash,
                            provenance: Provenance::Diff,
                            last_read: Arc::new(AtomicU64::new(diff.number)),
                        },
                    );
                    ops.push(Undo::Account(*addr, prev));
                }
            }
        }
        for (addr, slot, value) in &diff.storage {
            let prev = self.storage.insert(
                (*addr, *slot),
                SlotEntry {
                    value: *value,
                    provenance: Provenance::Diff,
                    last_read: Arc::new(AtomicU64::new(diff.number)),
                },
            );
            self.slots_by_acc.entry(*addr).or_default().insert(*slot);
            ops.push(Undo::Slot(*addr, *slot, prev));
        }
        // D13 read touches: values at `diff.number` that the span left
        // alone. Like lazy fills they never clobber (an entry held is
        // current: the feed keeps it so) and a rollback removes them; an
        // entry already held is marked read now, so keys the chain keeps
        // reading are never evicted.
        for (addr, c) in &diff.read_accounts {
            match self.accounts.entry(*addr) {
                dashmap::Entry::Occupied(e) => {
                    e.get().last_read.fetch_max(diff.number, Ordering::Relaxed);
                }
                dashmap::Entry::Vacant(v) => {
                    v.insert(AccountEntry {
                        balance: c.balance,
                        nonce: c.nonce,
                        code_hash: c.code_hash,
                        provenance: Provenance::Read,
                        last_read: Arc::new(AtomicU64::new(diff.number)),
                    });
                    ops.push(Undo::Account(*addr, None));
                }
            }
        }
        for (addr, slot, value) in &diff.read_storage {
            match self.storage.entry((*addr, *slot)) {
                dashmap::Entry::Occupied(e) => {
                    e.get().last_read.fetch_max(diff.number, Ordering::Relaxed);
                }
                dashmap::Entry::Vacant(v) => {
                    v.insert(SlotEntry {
                        value: *value,
                        provenance: Provenance::Read,
                        last_read: Arc::new(AtomicU64::new(diff.number)),
                    });
                    self.slots_by_acc.entry(*addr).or_default().insert(*slot);
                    ops.push(Undo::Slot(*addr, *slot, None));
                }
            }
        }
        let prev_tip = self.tip();
        {
            // A span overlapping the tip (client::Continuity::Overlaps)
            // repeats heads the ring already holds.
            let tip = self.tip();
            let mut heads = self.heads.lock().expect("heads lock");
            for head in diff
                .heads
                .iter()
                .filter(|h| tip.is_none_or(|(t, _)| h.number > t))
            {
                heads.push_back(head.clone());
                while heads.len() > self.heads_cap {
                    heads.pop_front();
                }
            }
        }
        *self.tip.lock().expect("tip lock") = Some((diff.number, diff.hash));
        if self.reorg_depth > 0 {
            journal.blocks.push_back(BlockUndo {
                prev_tip,
                number: diff.number,
                ops,
            });
            while journal.blocks.len() > self.reorg_depth {
                journal.blocks.pop_front();
            }
        }
        self.stats.applied_blocks.fetch_add(1, Ordering::Relaxed);
    }

    /// Rolls the store back to the fork block `(fork_number, fork_hash)`
    /// (PLAN D12): undoes, newest first, every diff applied above it and
    /// drops the heads above it. Succeeds only if the oldest diff above the
    /// fork was applied right on that block (within the journal, on the
    /// same chain); otherwise changes nothing and returns `false`, and the
    /// caller drops to cold. A tip already at the fork is a no-op `true`.
    pub fn rollback(&self, fork_number: u64, fork_hash: Word) -> bool {
        let mut journal = self.journal.lock().expect("journal lock");
        let Some((tip, tip_hash)) = self.tip() else {
            return false;
        };
        if (tip, tip_hash) == (fork_number, fork_hash) {
            return true;
        }
        if tip <= fork_number {
            return false;
        }
        let above = journal
            .blocks
            .iter()
            .rev()
            .take_while(|b| b.number > fork_number)
            .count();
        let oldest = journal.blocks.len() - above;
        match journal.blocks.get(oldest) {
            Some(b) if b.prev_tip == Some((fork_number, fork_hash)) => {}
            _ => return false,
        }
        let undone: Vec<BlockUndo> = journal.blocks.drain(oldest..).collect();
        for block in undone.into_iter().rev() {
            for op in block.ops.into_iter().rev() {
                self.undo(op);
            }
        }
        {
            let mut heads = self.heads.lock().expect("heads lock");
            while heads.back().is_some_and(|h| h.number > fork_number) {
                heads.pop_back();
            }
        }
        *self.tip.lock().expect("tip lock") = Some((fork_number, fork_hash));
        journal.epoch += 1;
        self.stats.rollbacks.fetch_add(1, Ordering::Relaxed);
        self.stats
            .rolled_back_blocks
            .fetch_add(tip - fork_number, Ordering::Relaxed);
        true
    }

    fn undo(&self, op: Undo) {
        match op {
            Undo::Account(addr, Some(prev)) => {
                self.accounts.insert(addr, prev);
            }
            Undo::Account(addr, None) => {
                self.accounts.remove(&addr);
            }
            Undo::Slot(addr, slot, Some(prev)) => {
                self.storage.insert((addr, slot), prev);
                self.slots_by_acc.entry(addr).or_default().insert(slot);
            }
            Undo::Slot(addr, slot, None) => {
                self.storage.remove(&(addr, slot));
                if let Some(mut set) = self.slots_by_acc.get_mut(&addr) {
                    set.remove(&slot);
                }
            }
        }
    }

    /// Lazy fills never clobber (see module docs), and land only in the
    /// `epoch` their read was pinned in (from [`Store::tip_and_epoch`]).
    /// Returns whether the entry was inserted.
    pub fn insert_lazy_account(
        &self,
        addr: Addr,
        balance: Word,
        nonce: u64,
        code_hash: Word,
        at_block: u64,
        epoch: u64,
    ) -> bool {
        let mut journal = self.journal.lock().expect("journal lock");
        if journal.epoch != epoch {
            return false;
        }
        let mut inserted = false;
        self.accounts.entry(addr).or_insert_with(|| {
            inserted = true;
            AccountEntry {
                balance,
                nonce,
                code_hash,
                provenance: Provenance::Lazy,
                last_read: Arc::new(AtomicU64::new(at_block)),
            }
        });
        // A rollback past the current tip removes it: it may have been
        // read at a block the rollback drops.
        if inserted && let Some(block) = journal.blocks.back_mut() {
            block.ops.push(Undo::Account(addr, None));
        }
        inserted
    }

    pub fn insert_lazy_slot(
        &self,
        addr: Addr,
        slot: Word,
        value: Word,
        at_block: u64,
        epoch: u64,
    ) -> bool {
        let mut journal = self.journal.lock().expect("journal lock");
        if journal.epoch != epoch {
            return false;
        }
        let mut inserted = false;
        self.storage.entry((addr, slot)).or_insert_with(|| {
            inserted = true;
            SlotEntry {
                value,
                provenance: Provenance::Lazy,
                last_read: Arc::new(AtomicU64::new(at_block)),
            }
        });
        if inserted {
            self.slots_by_acc.entry(addr).or_default().insert(slot);
            if let Some(block) = journal.blocks.back_mut() {
                block.ops.push(Undo::Slot(addr, slot, None));
            }
        }
        inserted
    }

    pub fn insert_code(&self, hash: Word, bytes: Arc<Vec<u8>>) {
        self.code.entry(hash).or_insert(bytes);
    }

    pub fn account(&self, addr: &Addr, now: u64) -> Option<AccountEntry> {
        let entry = self.accounts.get(addr)?;
        entry.last_read.store(now, Ordering::Relaxed);
        self.stats.hits.fetch_add(1, Ordering::Relaxed);
        Some(entry.clone())
    }

    pub fn slot(&self, addr: &Addr, slot: &Word, now: u64) -> Option<Word> {
        let entry = self.storage.get(&(*addr, *slot))?;
        entry.last_read.store(now, Ordering::Relaxed);
        self.stats.hits.fetch_add(1, Ordering::Relaxed);
        Some(entry.value)
    }

    /// Whether `addr` is held, without counting a hit or marking a read
    /// (prefetch decides what to fetch with it).
    pub fn has_account(&self, addr: &Addr) -> bool {
        self.accounts.contains_key(addr)
    }

    /// Whether `(addr, slot)` is held; see [`Store::has_account`].
    pub fn has_slot(&self, addr: &Addr, slot: &Word) -> bool {
        self.storage.contains_key(&(*addr, *slot))
    }

    /// Whether the bytecode for `hash` is held (empty code always is).
    pub fn has_code(&self, hash: &Word) -> bool {
        *hash == KECCAK_EMPTY || *hash == [0u8; 32] || self.code.contains_key(hash)
    }

    pub fn code(&self, hash: &Word) -> Option<Arc<Vec<u8>>> {
        if *hash == KECCAK_EMPTY || *hash == [0u8; 32] {
            return Some(Arc::new(Vec::new()));
        }
        self.code.get(hash).map(|c| c.clone())
    }

    /// Drop-to-cold (D3): forget everything except code — the next diff
    /// or lazy read rebuilds.
    pub fn clear(&self) {
        let mut journal = self.journal.lock().expect("journal lock");
        journal.blocks.clear();
        journal.epoch += 1;
        self.accounts.clear();
        self.storage.clear();
        self.slots_by_acc.clear();
        // Code is content-addressed by hash: immune to reorgs, keep it.
        self.heads.lock().expect("heads lock").clear();
        *self.tip.lock().expect("tip lock") = None;
        self.stats.cold_drops.fetch_add(1, Ordering::Relaxed);
    }

    /// Memory bound (PLAN T2.5): drop entries unread for `max_age` blocks,
    /// and the slot sets of accounts left with no slots. Code stays (see
    /// [`Store::clear`]).
    pub fn evict(&self, now: u64, max_age: u64) -> usize {
        let cutoff = now.saturating_sub(max_age);
        let before = self.accounts.len() + self.storage.len();
        self.accounts
            .retain(|_, e| e.last_read.load(Ordering::Relaxed) >= cutoff);
        self.storage.retain(|(addr, slot), e| {
            let keep = e.last_read.load(Ordering::Relaxed) >= cutoff;
            if !keep && let Some(mut set) = self.slots_by_acc.get_mut(addr) {
                set.remove(slot);
            }
            keep
        });
        // An emptied set keeps its allocation, and every account that ever
        // had a slot would keep one: the store grew without bound.
        self.slots_by_acc.retain(|_, set| !set.is_empty());
        before - (self.accounts.len() + self.storage.len())
    }

    pub fn len(&self) -> (usize, usize) {
        (self.accounts.len(), self.storage.len())
    }

    /// Approximate heap the store holds, from table capacities and sizes
    /// rather than the allocator; it leaves out allocator overhead, so the
    /// process RSS is higher. Walks the slot sets and the code map: call it
    /// per sweep, not per read.
    pub fn mem_usage(&self) -> MemUsage {
        use std::mem::size_of;
        // hashbrown: about 8/7 buckets per unit of capacity, plus one
        // control byte per bucket.
        fn table<T>(capacity: usize) -> usize {
            capacity * 8 / 7 * (size_of::<T>() + 1)
        }
        // `last_read`: an `Arc<AtomicU64>` per entry, two counts + the value.
        const LAST_READ: usize = 24;
        let sets: usize = self
            .slots_by_acc
            .iter()
            .map(|e| table::<Word>(e.value().capacity()))
            .sum();
        let entries = table::<(Addr, AccountEntry)>(self.accounts.capacity())
            + table::<((Addr, Word), SlotEntry)>(self.storage.capacity())
            + table::<(Addr, HashSet<Word>)>(self.slots_by_acc.capacity())
            + sets
            + (self.accounts.len() + self.storage.len()) * LAST_READ;
        // Each code entry: an `Arc<Vec<u8>>` (two counts + the Vec header).
        let code = table::<(Word, Arc<Vec<u8>>)>(self.code.capacity())
            + self
                .code
                .iter()
                .map(|e| 16 + size_of::<Vec<u8>>() + e.value().capacity())
                .sum::<usize>();
        let journal = {
            let journal = self.journal.lock().expect("journal lock");
            journal
                .blocks
                .iter()
                .map(|b| size_of::<BlockUndo>() + b.ops.capacity() * size_of::<Undo>())
                .sum::<usize>()
        };
        let heads = self.heads.lock().expect("heads lock").capacity() * size_of::<HeadLite>();
        MemUsage {
            entries,
            code,
            journal: journal + heads,
        }
    }
}

/// [`Store::mem_usage`], in bytes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MemUsage {
    /// Accounts and slots, with their indexes. Bounded by eviction.
    pub entries: usize,
    /// Bytecode. Never evicted: grows with every contract the store sees.
    pub code: usize,
    /// The undo journal and the head ring. Bounded by the reorg depth.
    pub journal: usize,
}

impl MemUsage {
    pub fn total(&self) -> usize {
        self.entries + self.code + self.journal
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use silt_wire::AccountChange;

    fn head(number: u64, hash: u8, parent: u8) -> HeadLite {
        HeadLite {
            number,
            hash: [hash; 32],
            parent_hash: [parent; 32],
            timestamp: number * 12,
            basefee: [0; 32],
            gas_limit: 30_000_000,
            coinbase: [0; 20],
            prevrandao: [0; 32],
        }
    }

    fn diff(number: u64, hash: u8, parent: u8) -> BlockDiff {
        BlockDiff {
            span_start: number,
            number,
            hash: [hash; 32],
            parent_hash: [parent; 32],
            heads: vec![head(number, hash, parent)],
            accounts: vec![],
            storage: vec![],
            code: vec![],
            receipts: vec![],
            read_accounts: Vec::new(),
            read_storage: Vec::new(),
        }
    }

    #[test]
    fn destroyed_account_drops_its_slots() {
        let store = Store::new(64);
        let addr = [0xAA; 20];
        let mut d1 = diff(1, 1, 0);
        d1.accounts = vec![(
            addr,
            Some(AccountChange {
                balance: [1; 32],
                nonce: 1,
                code_hash: [0; 32],
            }),
        )];
        d1.storage = vec![(addr, [5; 32], [9; 32]), (addr, [6; 32], [8; 32])];
        store.apply(&d1);
        assert!(store.slot(&addr, &[5; 32], 1).is_some());

        let mut d2 = diff(2, 2, 1);
        d2.accounts = vec![(addr, None)]; // destroyed
        store.apply(&d2);
        assert!(store.account(&addr, 2).is_none());
        assert!(store.slot(&addr, &[5; 32], 2).is_none());
        assert!(store.slot(&addr, &[6; 32], 2).is_none());
    }

    #[test]
    fn zero_write_is_a_real_resolved_value() {
        let store = Store::new(64);
        let addr = [0xBB; 20];
        let mut d = diff(1, 1, 0);
        d.storage = vec![(addr, [5; 32], [0; 32])];
        store.apply(&d);
        assert_eq!(
            store.slot(&addr, &[5; 32], 1),
            Some([0; 32]),
            "cleared != unknown"
        );
    }

    #[test]
    fn lazy_never_clobbers_diff() {
        let store = Store::new(64);
        let addr = [0xCC; 20];
        let mut d = diff(1, 1, 0);
        d.storage = vec![(addr, [5; 32], [7; 32])];
        store.apply(&d);
        // A racing lazy fetch that resolved before the diff landed must not
        // overwrite the authoritative value.
        assert!(!store.insert_lazy_slot(addr, [5; 32], [6; 32], 0, store.epoch()));
        assert_eq!(store.slot(&addr, &[5; 32], 1), Some([7; 32]));
        // And diff always clobbers lazy:
        assert!(store.insert_lazy_slot(addr, [9; 32], [1; 32], 1, store.epoch()));
        let mut d2 = diff(2, 2, 1);
        d2.storage = vec![(addr, [9; 32], [2; 32])];
        store.apply(&d2);
        assert_eq!(store.slot(&addr, &[9; 32], 2), Some([2; 32]));
    }

    #[test]
    fn span_heads_and_tip() {
        let store = Store::new(64);
        let mut d = diff(5, 5, 3);
        d.span_start = 4;
        d.heads = vec![head(4, 4, 3), head(5, 5, 4)];
        store.apply(&d);
        assert_eq!(store.tip(), Some((5, [5; 32])));
        assert_eq!(store.head(4).unwrap().hash, [4; 32]);
        assert_eq!(store.tip_head().unwrap().number, 5);
    }

    #[test]
    fn clear_then_eviction() {
        let store = Store::new(64);
        let addr = [0xDD; 20];
        let mut d = diff(1, 1, 0);
        d.storage = vec![(addr, [1; 32], [1; 32])];
        store.apply(&d);
        store.clear();
        assert_eq!(store.len(), (0, 0));
        assert!(store.tip().is_none());

        // Eviction: unread entries age out, read ones survive.
        let mut d2 = diff(2, 2, 1);
        d2.storage = vec![(addr, [1; 32], [1; 32]), (addr, [2; 32], [2; 32])];
        store.apply(&d2);
        let _ = store.slot(&addr, &[1; 32], 100); // touch
        let evicted = store.evict(100, 64);
        assert_eq!(evicted, 1, "untouched slot evicted");
        assert!(store.slot(&addr, &[1; 32], 100).is_some());
    }

    /// D13: read touches fill what the store lacks, never clobber what it
    /// holds, keep held entries from aging out, and roll back like lazy
    /// fills.
    #[test]
    fn read_touches_fill_refresh_and_roll_back() {
        let s = Store::new(256);
        let (a, b) = ([0xAA; 20], [0xBB; 20]);
        let change = |n: u8| AccountChange {
            balance: [n; 32],
            nonce: n as u64,
            code_hash: KECCAK_EMPTY,
        };
        let mut d1 = diff(1, 1, 0);
        d1.accounts = vec![(a, Some(change(1)))];
        d1.storage = vec![(a, [1; 32], [11; 32])];
        s.apply(&d1);

        let mut d2 = diff(2, 2, 1);
        // `a` and its slot are held: the touch must not overwrite them.
        d2.read_accounts = vec![(a, change(9)), (b, change(2))];
        d2.read_storage = vec![(a, [1; 32], [99; 32]), (b, [2; 32], [22; 32])];
        s.apply(&d2);
        assert_eq!(
            s.account(&a, 2).unwrap().balance,
            [1; 32],
            "held: not clobbered"
        );
        assert_eq!(s.slot(&a, &[1; 32], 2), Some([11; 32]));
        let fresh = s.account(&b, 2).unwrap();
        assert_eq!(
            (fresh.balance, fresh.provenance),
            ([2; 32], Provenance::Read)
        );
        assert_eq!(s.slot(&b, &[2; 32], 2), Some([22; 32]));

        // A held key the chain keeps reading doesn't age out.
        let mut d3 = diff(3, 3, 2);
        d3.read_accounts = vec![(a, change(1))];
        s.apply(&d3);
        s.evict(3 + 64, 64);
        assert!(
            s.has_account(&a),
            "touched at 3: kept at 67 with max age 64"
        );
        assert!(!s.has_account(&b), "last read at 2: evicted");

        // Rolling back d3 and d2 removes what the touches inserted.
        let s = Store::new(256);
        s.apply(&d1);
        s.apply(&d2);
        assert!(s.rollback(1, [1; 32]));
        assert!(!s.has_account(&b) && !s.has_slot(&b, &[2; 32]));
        assert_eq!(s.account(&a, 1).unwrap().balance, [1; 32]);
    }

    #[test]
    fn eviction_drops_emptied_slot_sets() {
        let store = Store::new(64);
        let (a, b) = ([0xA1; 20], [0xB2; 20]);
        let mut d = diff(1, 1, 0);
        d.storage = (0..50u8)
            .map(|i| (a, [i; 32], [1; 32]))
            .chain([(b, [1; 32], [1; 32])])
            .collect();
        store.apply(&d);
        let full = store.mem_usage();
        let _ = store.slot(&b, &[1; 32], 100); // touch
        assert_eq!(store.evict(100, 64), 50);
        assert!(
            !store.slots_by_acc.contains_key(&a),
            "a's emptied set dropped"
        );
        assert_eq!(store.slots_by_acc.get(&b).unwrap().len(), 1);
        let after = store.mem_usage();
        assert!(after.entries < full.entries, "{after:?} vs {full:?}");
        assert_eq!(after.code, full.code);

        // A later write recreates the set, so a destroy still finds it.
        let mut d2 = diff(101, 101, 100);
        d2.storage = vec![(a, [7; 32], [7; 32])];
        store.apply(&d2);
        let mut d3 = diff(102, 102, 101);
        d3.accounts = vec![(a, None)];
        store.apply(&d3);
        assert!(store.slot(&a, &[7; 32], 102).is_none());
    }

    /// Everything the store holds, for comparing two stores.
    type Snapshot = (
        std::collections::BTreeMap<Addr, (Word, u64, Word)>,
        std::collections::BTreeMap<(Addr, Word), Word>,
        Option<(u64, Word)>,
        Vec<u64>,
    );

    fn snapshot(store: &Store) -> Snapshot {
        let accounts = store
            .accounts
            .iter()
            .map(|e| (*e.key(), (e.balance, e.nonce, e.code_hash)))
            .collect();
        let storage = store.storage.iter().map(|e| (*e.key(), e.value)).collect();
        let heads = store
            .heads
            .lock()
            .unwrap()
            .iter()
            .map(|h| h.number)
            .collect();
        // The slot index must name exactly the slots held.
        for e in store.storage.iter() {
            let (addr, slot) = e.key();
            assert!(store.slots_by_acc.get(addr).unwrap().contains(slot));
        }
        (accounts, storage, store.tip(), heads)
    }

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    /// Block `number` of chain `side`: a few account writes, destroys and
    /// slot writes over a small key space, so blocks collide on keys.
    fn random_block(rng: &mut Rng, number: u64, side: u8, parent: Word) -> BlockDiff {
        let hash = {
            let mut h = [number as u8; 32];
            h[1] = side;
            h
        };
        let mut accounts = std::collections::BTreeMap::new();
        for _ in 0..rng.below(6) {
            let addr = [rng.below(8) as u8; 20];
            let change = (rng.below(5) != 0).then(|| AccountChange {
                balance: [rng.below(250) as u8; 32],
                nonce: rng.below(100),
                code_hash: [0; 32],
            });
            accounts.insert(addr, change);
        }
        let mut storage = std::collections::BTreeMap::new();
        for _ in 0..rng.below(8) {
            let addr = [rng.below(8) as u8; 20];
            if matches!(accounts.get(&addr), Some(None)) {
                continue; // a destroyed account's diff carries no slots
            }
            storage.insert((addr, [rng.below(6) as u8; 32]), [rng.below(250) as u8; 32]);
        }
        let mut head = head(number, 0, 0);
        head.hash = hash;
        head.parent_hash = parent;
        BlockDiff {
            span_start: number,
            number,
            hash,
            parent_hash: parent,
            heads: vec![head],
            accounts: accounts.into_iter().collect(),
            storage: storage.into_iter().map(|((a, k), v)| (a, k, v)).collect(),
            code: vec![],
            receipts: vec![],
            read_accounts: Vec::new(),
            read_storage: Vec::new(),
        }
    }

    /// D12: rolling back chain A to the fork and applying chain B leaves
    /// the store exactly as if it had followed B, lazy fills included.
    #[test]
    fn rollback_then_new_side_equals_following_the_new_side() {
        let mut rng = Rng(0x5117_d12d);
        for trial in 0..300 {
            let prefix = 1 + rng.below(4);
            let (a_len, b_len) = (1 + rng.below(4), 1 + rng.below(4));
            let mut chain = Vec::new();
            let mut parent = [0; 32];
            for n in 1..=prefix {
                let d = random_block(&mut rng, n, 0, parent);
                parent = d.hash;
                chain.push(d);
            }
            let fork = (prefix, parent);
            let side = |rng: &mut Rng, len: u64, tag: u8| {
                let mut p = fork.1;
                (prefix + 1..=prefix + len)
                    .map(|n| {
                        let d = random_block(rng, n, tag, p);
                        p = d.hash;
                        d
                    })
                    .collect::<Vec<_>>()
            };
            let (a, b) = (side(&mut rng, a_len, 1), side(&mut rng, b_len, 2));
            // Lazy fills of keys the prefix never wrote, on both stores.
            let fills: Vec<(Addr, Word, Word)> = (0..rng.below(4))
                .map(|i| ([0xF0 + i as u8; 20], [i as u8; 32], [7; 32]))
                .collect();

            let rolled = Store::new(16);
            let direct = Store::new(16);
            for d in &chain {
                rolled.apply(d);
                direct.apply(d);
            }
            for &(addr, slot, v) in &fills {
                assert!(rolled.insert_lazy_slot(addr, slot, v, prefix, rolled.epoch()));
                assert!(direct.insert_lazy_slot(addr, slot, v, prefix, direct.epoch()));
            }
            for (i, d) in a.iter().enumerate() {
                rolled.apply(d);
                // A fill read on chain A must not survive the rollback.
                let epoch = rolled.epoch();
                rolled.insert_lazy_slot([0xEE; 20], [i as u8; 32], [1; 32], d.number, epoch);
            }
            let stale_epoch = rolled.epoch();
            assert!(rolled.rollback(fork.0, fork.1), "trial {trial}");
            assert_eq!(
                snapshot(&rolled),
                snapshot(&direct),
                "trial {trial} at fork"
            );
            assert!(
                !rolled.insert_lazy_slot([0xEF; 20], [0; 32], [1; 32], 0, stale_epoch),
                "a fill pinned before the rollback is discarded"
            );
            for d in &b {
                rolled.apply(d);
                direct.apply(d);
            }
            assert_eq!(
                snapshot(&rolled),
                snapshot(&direct),
                "trial {trial} after B"
            );
            assert_eq!(rolled.stats.rollbacks.load(Ordering::Relaxed), 1);
        }
    }

    #[test]
    fn rollback_refuses_what_it_cannot_undo_exactly() {
        let chain = |store: &Store, n: u64| {
            for i in 1..=n {
                store.apply(&diff(i, i as u8, i as u8 - 1));
            }
        };
        // Deeper than the journal: unchanged, caller drops to cold.
        let store = Store::with_reorg_depth(16, 2);
        chain(&store, 4);
        let before = snapshot(&store);
        assert!(!store.rollback(1, [1; 32]));
        assert_eq!(snapshot(&store), before);
        assert!(store.rollback(2, [2; 32]), "within the journal");
        assert_eq!(store.tip(), Some((2, [2; 32])));
        assert!(store.head(3).is_none());

        // Wrong fork hash, a fork above the tip, no journal at all.
        let store = Store::new(16);
        chain(&store, 3);
        assert!(!store.rollback(1, [0x99; 32]));
        assert!(!store.rollback(5, [5; 32]));
        assert!(store.rollback(3, [3; 32]), "tip at the fork: no-op");
        assert_eq!(store.stats.rollbacks.load(Ordering::Relaxed), 0);
        let none = Store::with_reorg_depth(16, 0);
        chain(&none, 3);
        assert!(!none.rollback(2, [2; 32]));

        // A folded span that straddles the fork can't be split.
        let store = Store::new(16);
        chain(&store, 1);
        let mut span = diff(3, 3, 1);
        span.span_start = 2;
        span.heads = vec![head(2, 2, 1), head(3, 3, 2)];
        store.apply(&span);
        assert!(!store.rollback(2, [2; 32]));
        assert!(store.rollback(1, [1; 32]));

        // After a clear the journal is gone.
        let store = Store::new(16);
        chain(&store, 3);
        store.clear();
        store.apply(&diff(3, 3, 2));
        assert!(!store.rollback(2, [2; 32]));
    }
}
