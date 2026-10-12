//! The state store: concurrent maps + the apply path (PLAN T2.2).
//!
//! Coherence discipline (PLAN §E, D3):
//! - diff-derived writes always clobber (`insert_diff`) — they are the
//!   authoritative post-state;
//! - lazily-fetched values never clobber (`insert_lazy` = insert-if-absent) —
//!   if a diff touched the key first, the diff's value wins; this closes the
//!   fetch/apply race without locks.
//! - the provenance bit is recorded per entry (reserved for the layered v1).

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
use silt_wire::{Addr, BlockDiff, HeadLite, Word};

pub const KECCAK_EMPTY: Word = [
    0xc5, 0xd2, 0x46, 0x01, 0x86, 0xf7, 0x23, 0x3c, 0x92, 0x7e, 0x7d, 0xb2, 0xdc, 0xc7, 0x03, 0xc0,
    0xe5, 0x00, 0xb6, 0x53, 0xca, 0x82, 0x27, 0x3b, 0x7b, 0xfa, 0xd8, 0x04, 0x5d, 0x85, 0xa4, 0x70,
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Provenance {
    Diff,
    Lazy,
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
    pub applied_blocks: AtomicU64,
    pub cold_drops: AtomicU64,
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
}

/// std Mutex with a short alias (no parking_lot dependency).
mod parking_lot_free {
    pub type Mutex<T> = std::sync::Mutex<T>;
}

impl Store {
    pub fn new(heads_cap: usize) -> Self {
        Self {
            accounts: DashMap::new(),
            storage: DashMap::new(),
            code: DashMap::new(),
            slots_by_acc: DashMap::new(),
            heads: parking_lot_free::Mutex::new(VecDeque::new()),
            tip: parking_lot_free::Mutex::new(None),
            stats: Stats::default(),
            heads_cap,
        }
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
        for (hash, bytes) in &diff.code {
            self.code
                .entry(*hash)
                .or_insert_with(|| Arc::new(bytes.clone()));
        }
        for (addr, change) in &diff.accounts {
            match change {
                None => {
                    // Destroyed: the account and every slot we hold for it.
                    self.accounts.remove(addr);
                    if let Some((_, slots)) = self.slots_by_acc.remove(addr) {
                        for slot in slots {
                            self.storage.remove(&(*addr, slot));
                        }
                    }
                }
                Some(c) => {
                    self.accounts.insert(
                        *addr,
                        AccountEntry {
                            balance: c.balance,
                            nonce: c.nonce,
                            code_hash: c.code_hash,
                            provenance: Provenance::Diff,
                            last_read: Arc::new(AtomicU64::new(diff.number)),
                        },
                    );
                }
            }
        }
        for (addr, slot, value) in &diff.storage {
            self.storage.insert(
                (*addr, *slot),
                SlotEntry {
                    value: *value,
                    provenance: Provenance::Diff,
                    last_read: Arc::new(AtomicU64::new(diff.number)),
                },
            );
            self.slots_by_acc.entry(*addr).or_default().insert(*slot);
        }
        {
            let mut heads = self.heads.lock().expect("heads lock");
            for head in &diff.heads {
                heads.push_back(head.clone());
                while heads.len() > self.heads_cap {
                    heads.pop_front();
                }
            }
        }
        *self.tip.lock().expect("tip lock") = Some((diff.number, diff.hash));
        self.stats.applied_blocks.fetch_add(1, Ordering::Relaxed);
    }

    /// Lazy fills never clobber (see module docs).
    pub fn insert_lazy_account(
        &self,
        addr: Addr,
        balance: Word,
        nonce: u64,
        code_hash: Word,
        at_block: u64,
    ) {
        self.accounts.entry(addr).or_insert_with(|| AccountEntry {
            balance,
            nonce,
            code_hash,
            provenance: Provenance::Lazy,
            last_read: Arc::new(AtomicU64::new(at_block)),
        });
    }

    pub fn insert_lazy_slot(&self, addr: Addr, slot: Word, value: Word, at_block: u64) {
        self.storage
            .entry((addr, slot))
            .or_insert_with(|| SlotEntry {
                value,
                provenance: Provenance::Lazy,
                last_read: Arc::new(AtomicU64::new(at_block)),
            });
        self.slots_by_acc.entry(addr).or_default().insert(slot);
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

    pub fn code(&self, hash: &Word) -> Option<Arc<Vec<u8>>> {
        if *hash == KECCAK_EMPTY || *hash == [0u8; 32] {
            return Some(Arc::new(Vec::new()));
        }
        self.code.get(hash).map(|c| c.clone())
    }

    /// Drop-to-cold (D3): forget everything except nothing — the next diff
    /// or lazy read rebuilds.
    pub fn clear(&self) {
        self.accounts.clear();
        self.storage.clear();
        self.slots_by_acc.clear();
        // Code is content-addressed by hash: immune to reorgs, keep it.
        self.heads.lock().expect("heads lock").clear();
        *self.tip.lock().expect("tip lock") = None;
        self.stats.cold_drops.fetch_add(1, Ordering::Relaxed);
    }

    /// Memory bound (PLAN T2.5): drop entries unread for `max_age` blocks.
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
        before - (self.accounts.len() + self.storage.len())
    }

    pub fn len(&self) -> (usize, usize) {
        (self.accounts.len(), self.storage.len())
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
        store.insert_lazy_slot(addr, [5; 32], [6; 32], 0);
        assert_eq!(store.slot(&addr, &[5; 32], 1), Some([7; 32]));
        // And diff always clobbers lazy:
        store.insert_lazy_slot(addr, [9; 32], [1; 32], 1);
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
}
