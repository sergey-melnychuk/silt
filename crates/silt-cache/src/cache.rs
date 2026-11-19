//! The public cache: Chain-shaped reads over store + RPC fallback
//! (PLAN T2.3/T2.4/T2.7).

use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use silt_wire::{Addr, HeadLite, Word};
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};

use crate::client::{ClientState, run_tcp};
use crate::fallback::Fallback;
use crate::store::{MemUsage, Store};
use crate::{AccountView, HeadEvent};

#[derive(Clone, Debug)]
pub struct CacheConfig {
    /// Feed address (`host:port`). `None` = RPC-only mode (every read is a
    /// pinned lazy fetch; still correct, just cold).
    pub feed_addr: Option<String>,
    pub rpc_url: String,
    /// Evict entries unread for this many blocks (PLAN T2.5).
    pub evict_after_blocks: u64,
    /// Reconnect if the feed is silent this long (heartbeats come ~10 s).
    pub liveness: Duration,
    /// Lookup fast path (`host:port`, PLAN D9). `None` = misses go
    /// straight to JSON-RPC; `Some` = account/storage misses go over the
    /// lookup protocol, with JSON-RPC as the fallback on any failure (D2).
    pub lookup_addr: Option<String>,
    /// Per-request lookup deadline before falling back to RPC.
    pub lookup_timeout: Duration,
    /// Reorgs up to this many blocks deep roll back; deeper ones drop to
    /// cold (PLAN D12). `0` = always drop to cold.
    pub reorg_depth: usize,
    /// Soft memory limit on the store ([`Store::mem_usage`]), in bytes.
    /// While the store is above it, the eviction sweep logs a warning: on
    /// the first sweep over it, then once an hour. It never evicts or
    /// blocks; keeping the cache in budget is the consumer's job
    /// (`docs/runbook.md` §11). `None` = no check.
    pub soft_limit_bytes: Option<usize>,
}

/// The default [`CacheConfig::soft_limit_bytes`]: 512 MiB.
pub const DEFAULT_SOFT_LIMIT_BYTES: usize = 512 << 20;

/// The default [`CacheConfig::evict_after_blocks`]: 2048 blocks (~7 h).
/// Entries fetched on a miss stay until unread this long; the feed keeps
/// them correct meanwhile, so a longer life only costs memory
/// (`docs/runbook.md` §11), and every entry kept is a miss a simulation
/// doesn't pay (PLAN M6).
pub const DEFAULT_EVICT_AFTER_BLOCKS: u64 = 2048;

impl CacheConfig {
    pub fn new(rpc_url: impl Into<String>) -> Self {
        Self {
            feed_addr: None,
            rpc_url: rpc_url.into(),
            evict_after_blocks: DEFAULT_EVICT_AFTER_BLOCKS,
            liveness: Duration::from_secs(30),
            lookup_addr: None,
            lookup_timeout: Duration::from_millis(500),
            reorg_depth: crate::store::DEFAULT_REORG_DEPTH,
            soft_limit_bytes: Some(DEFAULT_SOFT_LIMIT_BYTES),
        }
    }

    pub fn with_feed(mut self, addr: impl Into<String>) -> Self {
        self.feed_addr = Some(addr.into());
        self
    }

    /// Sends account/storage misses over the lookup fast path (D9), e.g.
    /// `127.0.0.1:8586`; JSON-RPC answers whenever it can't.
    pub fn with_lookup(mut self, addr: impl Into<String>) -> Self {
        self.lookup_addr = Some(addr.into());
        self
    }

    pub fn with_lookup_timeout(mut self, timeout: Duration) -> Self {
        self.lookup_timeout = timeout;
        self
    }
}

#[derive(Hash, PartialEq, Eq, Clone)]
enum FlightKey {
    Account(Addr),
    Slot(Addr, Word),
}

pub struct Cache {
    pub(crate) store: Arc<Store>,
    pub(crate) fallback: Arc<dyn Fallback>,
    flights: DashMap<FlightKey, Arc<tokio::sync::Mutex<()>>>,
}

impl Cache {
    /// Builds the cache, spawns the feed client (if configured) and the
    /// eviction sweep; returns the handle plus the head-event stream.
    pub fn connect(cfg: CacheConfig) -> (Arc<Self>, UnboundedReceiver<HeadEvent>) {
        let rpc: Arc<dyn Fallback> =
            Arc::new(crate::fallback::HttpFallback::new(cfg.rpc_url.clone()));
        let fallback = match &cfg.lookup_addr {
            Some(addr) => {
                let lookup = crate::lookup::LookupClientConfig::new(addr.clone())
                    .with_timeout(cfg.lookup_timeout);
                Arc::new(crate::lookup::LookupFallback::new(lookup, rpc)) as Arc<dyn Fallback>
            }
            None => rpc,
        };
        Self::with_fallback(cfg, fallback)
    }

    /// Dependency-injected variant (tests, embedders with their own RPC).
    pub fn with_fallback(
        cfg: CacheConfig,
        fallback: Arc<dyn Fallback>,
    ) -> (Arc<Self>, UnboundedReceiver<HeadEvent>) {
        let store = Arc::new(Store::with_reorg_depth(256, cfg.reorg_depth));
        let (events_tx, events_rx) = unbounded_channel();
        let cache = Arc::new(Self {
            store: store.clone(),
            fallback,
            flights: DashMap::new(),
        });

        if let Some(addr) = cfg.feed_addr.clone() {
            let state = Arc::new(ClientState {
                store,
                events: events_tx,
                identity: std::sync::Mutex::new(None),
            });
            tokio::spawn(run_tcp(addr, state, cfg.liveness));
        }
        {
            let cache = cache.clone();
            let max_age = cfg.evict_after_blocks;
            let mut soft_limit = cfg.soft_limit_bytes.map(SoftLimit::new);
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    if let Some((tip, _)) = cache.store.tip() {
                        let evicted = cache.store.evict(tip, max_age);
                        let (accounts, slots) = cache.store.len();
                        let mem = cache.store.mem_usage();
                        tracing::debug!(
                            evicted,
                            accounts,
                            slots,
                            bytes = mem.total(),
                            "silt-cache eviction sweep"
                        );
                        if let Some(soft_limit) = soft_limit.as_mut() {
                            soft_limit.check(&mem, accounts, slots);
                        }
                    }
                }
            });
        }
        (cache, events_rx)
    }

    /// A cache over a store the caller feeds itself (`Store::apply`): no
    /// feed task and no eviction sweep. For consumers that must control
    /// when a diff lands, e.g. to run block N on state N-1 before applying
    /// diff N. Misses go to `fallback`, pinned at the store's tip.
    pub fn from_store(store: Arc<Store>, fallback: Arc<dyn Fallback>) -> Arc<Self> {
        Arc::new(Self {
            store,
            fallback,
            flights: DashMap::new(),
        })
    }

    pub fn tip(&self) -> Option<(u64, Word)> {
        self.store.tip()
    }

    /// The store's approximate heap ([`Store::mem_usage`]), e.g. for a
    /// metric. Walks the store's maps: call it every few seconds at most.
    pub fn mem_usage(&self) -> MemUsage {
        self.store.mem_usage()
    }

    /// The block lazy reads pin to, and the store epoch their fills must
    /// land in (D12); before the first diff we pin to the RPC's latest
    /// block (RPC-only / cold operation).
    async fn pin(&self) -> eyre::Result<(u64, Word, u64)> {
        let (tip, epoch) = self.store.tip_and_epoch();
        if let Some((number, hash)) = tip {
            return Ok((number, hash, epoch));
        }
        let head = self.fallback.head_by_number(None).await?;
        Ok((head.number, head.hash, epoch))
    }

    pub async fn account(&self, addr: &Addr) -> eyre::Result<AccountView> {
        let (now, pin_hash, epoch) = self.pin().await?;
        if let Some(entry) = self.store.account(addr, now) {
            let code = self.code_for(addr, &entry.code_hash, &pin_hash).await?;
            return Ok(AccountView {
                balance: entry.balance,
                nonce: entry.nonce,
                code_hash: entry.code_hash,
                code,
            });
        }
        self.store
            .stats
            .misses
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Single-flight: N concurrent misses on one key -> one fetch.
        let flight = self
            .flights
            .entry(FlightKey::Account(*addr))
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        let _guard = flight.lock().await;
        if let Some(entry) = self.store.account(addr, now) {
            let code = self.code_for(addr, &entry.code_hash, &pin_hash).await?;
            return Ok(AccountView {
                balance: entry.balance,
                nonce: entry.nonce,
                code_hash: entry.code_hash,
                code,
            });
        }
        // One round trip: balance, nonce, code hash and (from the lookup
        // path) the code itself, kept by hash for good.
        let meta = self
            .fallback
            .accounts_meta_at(&[*addr], &pin_hash)
            .await?
            .pop()
            .ok_or_else(|| eyre::eyre!("fallback: no answer for account"))?;
        if let Some(code) = meta.code
            && !self.store.has_code(&meta.code_hash)
        {
            self.store.insert_code(meta.code_hash, Arc::new(code));
        }
        let code = self.code_for(addr, &meta.code_hash, &pin_hash).await?;
        self.store
            .insert_lazy_account(*addr, meta.balance, meta.nonce, meta.code_hash, now, epoch);
        self.flights.remove(&FlightKey::Account(*addr));
        Ok(AccountView {
            balance: meta.balance,
            nonce: meta.nonce,
            code_hash: meta.code_hash,
            code,
        })
    }

    /// The bytecode for `hash` (of `addr`'s account): from the store, else
    /// by hash from the fallback, else the account read whole.
    async fn code_for(
        &self,
        addr: &Addr,
        hash: &Word,
        pin_hash: &Word,
    ) -> eyre::Result<Arc<Vec<u8>>> {
        if let Some(code) = self.store.code(hash) {
            return Ok(code);
        }
        let by_hash = self
            .fallback
            .codes_by_hash(&[*hash], pin_hash)
            .await?
            .pop()
            .flatten();
        let bytes = match by_hash {
            Some(bytes) => bytes,
            None => self.fallback.account_at(addr, pin_hash).await?.code,
        };
        let code = Arc::new(bytes);
        self.store.insert_code(*hash, code.clone());
        Ok(code)
    }

    /// Fetches whatever of `accounts` (with their code) and `slots` the
    /// store doesn't hold, batched: one round trip for the accounts and
    /// the slots together, one more only for code never seen before. They
    /// land as lazy fills pinned at the tip, like misses, so a simulation
    /// about to read them finds them (PLAN M6). Counted in
    /// `stats.prefetched`, not as misses.
    pub async fn prefetch(&self, accounts: &[Addr], slots: &[(Addr, Word)]) -> eyre::Result<()> {
        let (now, pin_hash, epoch) = self.pin().await?;
        let mut accs: Vec<Addr> = accounts
            .iter()
            .filter(|a| !self.store.has_account(a))
            .copied()
            .collect();
        accs.sort_unstable();
        accs.dedup();
        let mut keys: Vec<(Addr, Word)> = slots
            .iter()
            .filter(|(a, s)| !self.store.has_slot(a, s))
            .copied()
            .collect();
        keys.sort_unstable();
        keys.dedup();
        if accs.is_empty() && keys.is_empty() {
            return Ok(());
        }
        let (metas, values) = tokio::join!(
            async {
                if accs.is_empty() {
                    Ok(Vec::new())
                } else {
                    self.fallback.accounts_meta_at(&accs, &pin_hash).await
                }
            },
            async {
                if keys.is_empty() {
                    Ok(Vec::new())
                } else {
                    self.fallback.storages_at(&keys, &pin_hash).await
                }
            }
        );
        let (metas, values) = (metas?, values?);

        let mut unknown: Vec<Word> = Vec::new();
        for meta in &metas {
            match &meta.code {
                _ if self.store.has_code(&meta.code_hash) => {}
                Some(code) => self
                    .store
                    .insert_code(meta.code_hash, Arc::new(code.clone())),
                None => unknown.push(meta.code_hash),
            }
        }
        unknown.sort_unstable();
        unknown.dedup();
        let mut no_code = std::collections::HashSet::new();
        if !unknown.is_empty() {
            let codes = self.fallback.codes_by_hash(&unknown, &pin_hash).await?;
            for (hash, code) in unknown.iter().zip(codes) {
                match code {
                    Some(code) => self.store.insert_code(*hash, Arc::new(code)),
                    // Left to the read's own miss path (account read whole).
                    None => {
                        no_code.insert(*hash);
                    }
                }
            }
        }

        let mut filled = 0u64;
        for (addr, meta) in accs.iter().zip(metas) {
            if no_code.contains(&meta.code_hash) {
                continue;
            }
            filled += self.store.insert_lazy_account(
                *addr,
                meta.balance,
                meta.nonce,
                meta.code_hash,
                now,
                epoch,
            ) as u64;
        }
        for ((addr, slot), value) in keys.iter().zip(values) {
            filled += self.store.insert_lazy_slot(*addr, *slot, value, now, epoch) as u64;
        }
        self.store
            .stats
            .prefetched
            .fetch_add(filled, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    pub async fn storage(&self, addr: &Addr, slot: &Word) -> eyre::Result<Word> {
        let (now, pin_hash, epoch) = self.pin().await?;
        if let Some(value) = self.store.slot(addr, slot, now) {
            return Ok(value);
        }
        self.store
            .stats
            .misses
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let flight = self
            .flights
            .entry(FlightKey::Slot(*addr, *slot))
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        let _guard = flight.lock().await;
        if let Some(value) = self.store.slot(addr, slot, now) {
            return Ok(value);
        }
        let value = self.fallback.storage_at(addr, slot, &pin_hash).await?;
        self.store.insert_lazy_slot(*addr, *slot, value, now, epoch);
        self.flights.remove(&FlightKey::Slot(*addr, *slot));
        Ok(value)
    }

    pub async fn head(&self, number: u64) -> eyre::Result<HeadLite> {
        if let Some(head) = self.store.head(number) {
            return Ok(head);
        }
        self.fallback.head_by_number(Some(number)).await
    }

    pub fn tip_head(&self) -> Option<HeadLite> {
        self.store.tip_head()
    }

    /// Raw RPC block passthrough (replay/adapter path; never cached).
    pub async fn block_json(&self, number: u64, full: bool) -> eyre::Result<serde_json::Value> {
        self.fallback.block_json(number, full).await
    }

    pub async fn chain_id(&self) -> eyre::Result<u64> {
        self.fallback.chain_id().await
    }
}

/// Logs the sweep's soft-limit warnings ([`CacheConfig::soft_limit_bytes`]).
struct SoftLimit {
    limit: usize,
    /// Consecutive sweeps over the limit.
    over: u64,
}

impl SoftLimit {
    /// Sweeps run every minute: warn on the first one over, then hourly.
    const WARN_EVERY: u64 = 60;

    fn new(limit: usize) -> Self {
        Self { limit, over: 0 }
    }

    /// Whether this sweep warned.
    fn check(&mut self, mem: &MemUsage, accounts: usize, slots: usize) -> bool {
        let mib = |b: usize| b >> 20;
        if mem.total() <= self.limit {
            if self.over > 0 {
                tracing::info!(
                    mib = mib(mem.total()),
                    limit_mib = mib(self.limit),
                    "silt-cache back under its soft memory limit"
                );
            }
            self.over = 0;
            return false;
        }
        let warn = self.over.is_multiple_of(Self::WARN_EVERY);
        if warn {
            tracing::warn!(
                mib = mib(mem.total()),
                limit_mib = mib(self.limit),
                entries_mib = mib(mem.entries),
                code_mib = mib(mem.code),
                journal_mib = mib(mem.journal),
                accounts,
                slots,
                "silt-cache over its soft memory limit (see docs/runbook.md §11)"
            );
        }
        self.over += 1;
        warn
    }
}

pub(crate) fn keccak(bytes: &[u8]) -> Word {
    use tiny_keccak::Hasher;
    let mut out = [0u8; 32];
    let mut k = tiny_keccak::Keccak::v256();
    k.update(bytes);
    k.finalize(&mut out);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fallback::RpcAccount;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct MockChain {
        calls: AtomicU64,
    }

    #[async_trait::async_trait]
    impl Fallback for MockChain {
        async fn account_at(&self, _addr: &Addr, _h: &Word) -> eyre::Result<RpcAccount> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(20)).await;
            Ok(RpcAccount {
                balance: [3; 32],
                nonce: 7,
                code: vec![0x60, 0x00],
            })
        }
        async fn storage_at(&self, _a: &Addr, _s: &Word, _h: &Word) -> eyre::Result<Word> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(20)).await;
            Ok([42; 32])
        }
        async fn head_by_number(&self, _n: Option<u64>) -> eyre::Result<HeadLite> {
            Ok(HeadLite {
                number: 100,
                hash: [1; 32],
                parent_hash: [0; 32],
                timestamp: 0,
                basefee: [0; 32],
                gas_limit: 0,
                coinbase: [0; 20],
                prevrandao: [0; 32],
            })
        }
        async fn block_json(&self, _n: u64, _f: bool) -> eyre::Result<serde_json::Value> {
            Ok(serde_json::Value::Null)
        }
        async fn chain_id(&self) -> eyre::Result<u64> {
            Ok(1)
        }
    }

    /// PLAN G0.3 analog: concurrent misses on one key = one fallback call.
    #[tokio::test]
    async fn single_flight_dedupes_concurrent_misses() {
        let mock = Arc::new(MockChain { calls: 0.into() });
        let (cache, _events) = Cache::with_fallback(
            CacheConfig::new("unused"),
            mock.clone() as Arc<dyn Fallback>,
        );
        let addr = [1u8; 20];
        let slot = [2u8; 32];
        let mut tasks = Vec::new();
        for _ in 0..32 {
            let cache = cache.clone();
            tasks.push(tokio::spawn(async move {
                cache.storage(&addr, &slot).await.unwrap()
            }));
        }
        for t in tasks {
            assert_eq!(t.await.unwrap(), [42; 32]);
        }
        // 1 storage fetch; the pin() lookup goes through head_by_number which
        // is uncounted. Allow the race of the very first entry: all waiters
        // re-check the store under the key lock.
        assert_eq!(mock.calls.load(Ordering::SeqCst), 1);

        // Account path: one fetch, code hashed + cached, whole account view.
        let view = cache.account(&addr).await.unwrap();
        assert_eq!(view.nonce, 7);
        assert_eq!(view.code.as_slice(), &[0x60, 0x00]);
        assert_eq!(mock.calls.load(Ordering::SeqCst), 2);
        let again = cache.account(&addr).await.unwrap();
        assert_eq!(again.code_hash, view.code_hash);
        assert_eq!(
            mock.calls.load(Ordering::SeqCst),
            2,
            "hit serves from store"
        );
    }

    /// A fallback that answers batches and code by hash (like the lookup
    /// path), counting each kind of call.
    #[derive(Default)]
    struct BatchChain {
        singles: AtomicU64,
        batches: AtomicU64,
        code_calls: AtomicU64,
    }

    const CODE: [u8; 2] = [0x60, 0x00];

    #[async_trait::async_trait]
    impl Fallback for BatchChain {
        async fn account_at(&self, _addr: &Addr, _h: &Word) -> eyre::Result<RpcAccount> {
            self.singles.fetch_add(1, Ordering::SeqCst);
            Ok(RpcAccount {
                balance: [3; 32],
                nonce: 7,
                code: CODE.to_vec(),
            })
        }
        async fn storage_at(&self, _a: &Addr, _s: &Word, _h: &Word) -> eyre::Result<Word> {
            self.singles.fetch_add(1, Ordering::SeqCst);
            Ok([42; 32])
        }
        async fn head_by_number(&self, _n: Option<u64>) -> eyre::Result<HeadLite> {
            MockChain { calls: 0.into() }.head_by_number(None).await
        }
        async fn block_json(&self, _n: u64, _f: bool) -> eyre::Result<serde_json::Value> {
            Ok(serde_json::Value::Null)
        }
        async fn chain_id(&self) -> eyre::Result<u64> {
            Ok(1)
        }
        async fn accounts_meta_at(
            &self,
            addrs: &[Addr],
            _h: &Word,
        ) -> eyre::Result<Vec<crate::fallback::AccountMeta>> {
            self.batches.fetch_add(1, Ordering::SeqCst);
            Ok(addrs
                .iter()
                .map(|a| crate::fallback::AccountMeta {
                    balance: [a[0]; 32],
                    nonce: a[0] as u64,
                    code_hash: keccak(&CODE),
                    code: None,
                })
                .collect())
        }
        async fn storages_at(&self, keys: &[(Addr, Word)], _h: &Word) -> eyre::Result<Vec<Word>> {
            self.batches.fetch_add(1, Ordering::SeqCst);
            Ok(keys.iter().map(|(_, s)| *s).collect())
        }
        async fn codes_by_hash(
            &self,
            hashes: &[Word],
            _h: &Word,
        ) -> eyre::Result<Vec<Option<Vec<u8>>>> {
            self.code_calls.fetch_add(1, Ordering::SeqCst);
            Ok(hashes.iter().map(|_| Some(CODE.to_vec())).collect())
        }
    }

    /// PLAN M6: a prefetch takes one batch for accounts, one for slots and
    /// one for code never seen; the reads after it all hit, and a second
    /// prefetch of the same keys costs nothing.
    #[tokio::test]
    async fn prefetch_batches_then_reads_hit() {
        let mock = Arc::new(BatchChain::default());
        let (cache, _events) = Cache::with_fallback(
            CacheConfig::new("unused"),
            mock.clone() as Arc<dyn Fallback>,
        );
        let (a1, a2) = ([1u8; 20], [2u8; 20]);
        let (s1, s2) = ([0x11u8; 32], [0x22u8; 32]);
        cache
            .prefetch(&[a1, a2, a1], &[(a1, s1), (a2, s2), (a1, s1)])
            .await
            .unwrap();
        assert_eq!(mock.batches.load(Ordering::SeqCst), 2, "accounts + slots");
        assert_eq!(
            mock.code_calls.load(Ordering::SeqCst),
            1,
            "one unknown code hash"
        );
        assert_eq!(cache.store.stats.prefetched.load(Ordering::SeqCst), 4);

        let view = cache.account(&a2).await.unwrap();
        assert_eq!((view.nonce, view.balance), (2, [2; 32]));
        assert_eq!(view.code.as_slice(), &CODE);
        assert_eq!(cache.storage(&a1, &s1).await.unwrap(), s1);
        assert_eq!(cache.store.stats.misses.load(Ordering::SeqCst), 0);
        assert_eq!(mock.singles.load(Ordering::SeqCst), 0, "no per-key fetches");

        cache.prefetch(&[a1, a2], &[(a2, s2)]).await.unwrap();
        assert_eq!(mock.batches.load(Ordering::SeqCst), 2, "all held: no calls");
    }

    /// A miss fetches the account without code, then the code by hash
    /// once; a second account with the same code costs no code fetch.
    #[tokio::test]
    async fn miss_fetches_code_by_hash_once() {
        let mock = Arc::new(BatchChain::default());
        let (cache, _events) = Cache::with_fallback(
            CacheConfig::new("unused"),
            mock.clone() as Arc<dyn Fallback>,
        );
        let view = cache.account(&[5u8; 20]).await.unwrap();
        assert_eq!(view.code.as_slice(), &CODE);
        assert_eq!(view.code_hash, keccak(&CODE));
        assert_eq!(mock.code_calls.load(Ordering::SeqCst), 1);
        cache.account(&[6u8; 20]).await.unwrap();
        assert_eq!(
            mock.code_calls.load(Ordering::SeqCst),
            1,
            "code cached by hash"
        );
        assert_eq!(mock.singles.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn soft_limit_warns_first_then_hourly() {
        let mut soft = SoftLimit::new(100);
        let at = |total| MemUsage {
            entries: total,
            ..MemUsage::default()
        };
        assert!(!soft.check(&at(100), 0, 0), "at the limit is fine");
        assert!(soft.check(&at(101), 0, 0), "first sweep over warns");
        let warned = (1..=120).filter(|_| soft.check(&at(150), 0, 0)).count();
        assert_eq!(warned, 2, "then once every 60 sweeps");
        assert!(!soft.check(&at(50), 0, 0));
        assert!(soft.check(&at(101), 0, 0), "a new crossing warns again");
    }

    #[test]
    fn keccak_empty_matches_constant() {
        assert_eq!(keccak(&[]), crate::store::KECCAK_EMPTY);
    }
}
