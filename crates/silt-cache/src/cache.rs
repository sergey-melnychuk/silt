//! The public cache: Chain-shaped reads over store + RPC fallback
//! (PLAN T2.3/T2.4/T2.7).

use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use silt_wire::{Addr, HeadLite, Word};
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};

use crate::client::{ClientState, run_tcp};
use crate::fallback::Fallback;
use crate::store::{KECCAK_EMPTY, Store};
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
}

impl CacheConfig {
    pub fn new(rpc_url: impl Into<String>) -> Self {
        Self {
            feed_addr: None,
            rpc_url: rpc_url.into(),
            evict_after_blocks: 64,
            liveness: Duration::from_secs(30),
        }
    }

    pub fn with_feed(mut self, addr: impl Into<String>) -> Self {
        self.feed_addr = Some(addr.into());
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
        Self::with_fallback(
            cfg.clone(),
            Arc::new(crate::fallback::HttpFallback::new(cfg.rpc_url)),
        )
    }

    /// Dependency-injected variant (tests, embedders with their own RPC).
    pub fn with_fallback(
        cfg: CacheConfig,
        fallback: Arc<dyn Fallback>,
    ) -> (Arc<Self>, UnboundedReceiver<HeadEvent>) {
        let store = Arc::new(Store::new(256));
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
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    if let Some((tip, _)) = cache.store.tip() {
                        let evicted = cache.store.evict(tip, max_age);
                        let (accounts, slots) = cache.store.len();
                        tracing::debug!(evicted, accounts, slots, "silt-cache eviction sweep");
                    }
                }
            });
        }
        (cache, events_rx)
    }

    pub fn tip(&self) -> Option<(u64, Word)> {
        self.store.tip()
    }

    /// The block hash lazy reads pin to; before the first diff we pin to
    /// the RPC's latest block (RPC-only / cold operation).
    async fn pin(&self) -> eyre::Result<(u64, Word)> {
        if let Some(tip) = self.store.tip() {
            return Ok(tip);
        }
        let head = self.fallback.head_by_number(None).await?;
        Ok((head.number, head.hash))
    }

    pub async fn account(&self, addr: &Addr) -> eyre::Result<AccountView> {
        let (now, pin_hash) = self.pin().await?;
        if let Some(entry) = self.store.account(addr, now) {
            let code = match self.store.code(&entry.code_hash) {
                Some(code) => code,
                None => {
                    // Diff told us the hash but the code predates the feed.
                    let fetched = self.fallback.account_at(addr, &pin_hash).await?;
                    let code = Arc::new(fetched.code);
                    self.store.insert_code(entry.code_hash, code.clone());
                    code
                }
            };
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

        // Single-flight: N concurrent misses on one key -> one RPC fetch.
        let flight = self
            .flights
            .entry(FlightKey::Account(*addr))
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        let _guard = flight.lock().await;
        if let Some(entry) = self.store.account(addr, now) {
            let code = self
                .store
                .code(&entry.code_hash)
                .unwrap_or_else(|| Arc::new(Vec::new()));
            return Ok(AccountView {
                balance: entry.balance,
                nonce: entry.nonce,
                code_hash: entry.code_hash,
                code,
            });
        }
        let fetched = self.fallback.account_at(addr, &pin_hash).await?;
        let code_hash = if fetched.code.is_empty() {
            KECCAK_EMPTY
        } else {
            keccak(&fetched.code)
        };
        let code = Arc::new(fetched.code);
        self.store.insert_code(code_hash, code.clone());
        self.store
            .insert_lazy_account(*addr, fetched.balance, fetched.nonce, code_hash, now);
        self.flights.remove(&FlightKey::Account(*addr));
        Ok(AccountView {
            balance: fetched.balance,
            nonce: fetched.nonce,
            code_hash,
            code,
        })
    }

    pub async fn storage(&self, addr: &Addr, slot: &Word) -> eyre::Result<Word> {
        let (now, pin_hash) = self.pin().await?;
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
        self.store.insert_lazy_slot(*addr, *slot, value, now);
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

    #[test]
    fn keccak_empty_matches_constant() {
        assert_eq!(keccak(&[]), KECCAK_EMPTY);
    }
}
