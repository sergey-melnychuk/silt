//! Restart recovery (PLAN G3.2), offline: a feed server killed and restarted
//! on the same port while a `Cache` consumes it under load and readers hit
//! it concurrently.
//!
//! The server mirrors silt-node's publisher (`feed.rs`): Hello, client Hello,
//! `Subscribe{from}`; spooled diffs replayed when their tip is `>= from`;
//! `GapTooOld` when `from` is older than the first spooled span's start,
//! then live; live fan-out skips what was replayed. Killing it drops the listener
//! and every connection, as a node restart does. Each restart scenario is
//! one of the shapes a real node restart produces:
//! - the new process re-publishes blocks the client already holds (reth
//!   replays from the last acked height);
//! - it re-publishes them as one folded span that straddles the client's tip;
//! - the client lagged so far that its next block left the spool;
//! - the spool restarts empty and its first block skips past the client.
//!
//! The first two must recover with no break and no missed or repeated head;
//! the last two must drop to cold (`ChainBreak`) rather than apply silently.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use silt_cache::fallback::RpcAccount;
use silt_cache::{Cache, CacheConfig, Fallback, HeadEvent};
use silt_wire::{
    AccountChange, Addr, BlockDiff, Frame, HeadLite, KECCAK_EMPTY, Word, read_frame, write_frame,
};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, broadcast, mpsc::UnboundedReceiver};
use tokio::task::{JoinHandle, JoinSet};

const GENESIS: Word = [0x99; 32];
/// Written by every block: balance = block number.
const HOT: Addr = [0xEE; 20];
/// Keys per diff, besides HOT: accounts and slots (the "load").
const ACCOUNTS: usize = 200;
const SLOTS: usize = 400;

fn hash(n: u64) -> Word {
    let mut h = [0u8; 32];
    h[..8].copy_from_slice(&n.to_be_bytes());
    h[31] = 0x5A;
    h
}

fn head(n: u64) -> HeadLite {
    HeadLite {
        number: n,
        hash: hash(n),
        parent_hash: hash(n - 1),
        timestamp: n * 12,
        basefee: [0; 32],
        gas_limit: 30_000_000,
        coinbase: [0; 20],
        prevrandao: [0; 32],
    }
}

fn word(n: u64) -> Word {
    let mut w = [0u8; 32];
    w[24..].copy_from_slice(&n.to_be_bytes());
    w
}

fn account(n: u64) -> AccountChange {
    AccountChange {
        balance: word(n),
        nonce: n,
        code_hash: KECCAK_EMPTY,
    }
}

/// Block `n` alone, or the folded span `start..=n`: every key a block in
/// the span touched, with its value at `n` (last write wins). Block b
/// writes HOT, account `[b as u8; 20]`, and `ACCOUNTS`/`SLOTS` load keys.
fn span(start: u64, n: u64) -> BlockDiff {
    let mut accounts = vec![(HOT, Some(account(n)))];
    let mut storage = Vec::new();
    for b in start..=n {
        accounts.push(([b as u8; 20], Some(account(b))));
    }
    for i in 0..ACCOUNTS {
        let mut a = [0x11; 20];
        a[..8].copy_from_slice(&(i as u64).to_be_bytes());
        accounts.push((a, Some(account(n))));
    }
    for i in 0..SLOTS {
        storage.push((HOT, word(i as u64), word(n)));
    }
    BlockDiff {
        span_start: start,
        number: n,
        hash: hash(n),
        parent_hash: hash(start - 1),
        heads: (start..=n).map(head).collect(),
        accounts,
        storage,
        code: vec![],
        receipts: vec![],
        read_accounts: Vec::new(),
        read_storage: Vec::new(),
    }
}

// ---- the mock feed server ------------------------------------------------

struct Feed {
    /// (span start, tip, frame).
    spool: Mutex<VecDeque<(u64, u64, Frame)>>,
    tx: broadcast::Sender<(u64, Frame)>,
    subscribers: AtomicUsize,
}

impl Feed {
    fn new(spooled: impl IntoIterator<Item = BlockDiff>) -> Arc<Self> {
        let spool = spooled
            .into_iter()
            .map(|d| (d.span_start, d.number, Frame::Diff(d)))
            .collect();
        Arc::new(Self {
            spool: Mutex::new(spool),
            tx: broadcast::channel(4096).0,
            subscribers: AtomicUsize::new(0),
        })
    }

    async fn publish(&self, diff: BlockDiff) {
        let (start, n) = (diff.span_start, diff.number);
        let frame = Frame::Diff(diff);
        let mut spool = self.spool.lock().await;
        spool.push_back((start, n, frame.clone()));
        while spool.len() > 128 {
            spool.pop_front();
        }
        drop(spool);
        let _ = self.tx.send((n, frame));
    }

    async fn handle(&self, mut s: tokio::net::TcpStream) -> eyre::Result<()> {
        let tip = self.spool.lock().await.back().map_or(0, |(_, n, _)| *n);
        write_frame(&mut s, &silt_wire::hello(1, GENESIS, tip)).await?;
        let _hello = read_frame(&mut s).await?;
        let Some(Frame::Subscribe { from, .. }) = read_frame(&mut s).await? else {
            eyre::bail!("expected Subscribe");
        };
        let mut rx = self.tx.subscribe();
        let (gap, replay) = {
            let spool = self.spool.lock().await;
            match (from, spool.front()) {
                (None, _) => (None, Vec::new()),
                (Some(f), Some(&(oldest, _, _))) if f < oldest => (Some(oldest), Vec::new()),
                (Some(f), _) => (
                    None,
                    spool
                        .iter()
                        .filter(|(_, n, _)| *n >= f)
                        .map(|(_, n, frame)| (*n, frame.clone()))
                        .collect(),
                ),
            }
        };
        self.subscribers.fetch_add(1, Ordering::SeqCst);
        if let Some(oldest_available) = gap {
            write_frame(&mut s, &Frame::GapTooOld { oldest_available }).await?;
        }
        let mut last_sent = 0;
        for (n, frame) in replay {
            write_frame(&mut s, &frame).await?;
            last_sent = n;
        }
        loop {
            let (n, frame) = rx.recv().await?;
            if n > last_sent {
                write_frame(&mut s, &frame).await?;
                last_sent = n;
            }
        }
    }
}

/// Serves `feed` on `port` (0 = any) until the handle is aborted; aborting
/// drops the listener and every connection (the JoinSet aborts on drop).
async fn serve(feed: Arc<Feed>, port: u16) -> (u16, JoinHandle<()>) {
    let listener = TcpListener::bind(("127.0.0.1", port)).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let task = tokio::spawn(async move {
        let mut conns = JoinSet::new();
        loop {
            let (s, _) = listener.accept().await.unwrap();
            let feed = feed.clone();
            conns.spawn(async move {
                let _ = feed.handle(s).await;
            });
        }
    });
    (port, task)
}

async fn kill(task: JoinHandle<()>) {
    task.abort();
    let _ = task.await;
}

async fn wait_subscribed(feed: &Feed) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while feed.subscribers.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("client never subscribed");
}

// ---- the consumer ------------------------------------------------------

/// Misses must not happen in these tests: every read key is diff-applied.
struct NoRpc;

#[async_trait::async_trait]
impl Fallback for NoRpc {
    async fn account_at(&self, _: &Addr, _: &Word) -> eyre::Result<RpcAccount> {
        eyre::bail!("unexpected fallback read")
    }
    async fn storage_at(&self, _: &Addr, _: &Word, _: &Word) -> eyre::Result<Word> {
        eyre::bail!("unexpected fallback read")
    }
    async fn head_by_number(&self, _: Option<u64>) -> eyre::Result<HeadLite> {
        eyre::bail!("unexpected fallback read")
    }
    async fn block_json(&self, _: u64, _: bool) -> eyre::Result<serde_json::Value> {
        eyre::bail!("unexpected fallback read")
    }
    async fn chain_id(&self) -> eyre::Result<u64> {
        Ok(1)
    }
}

fn consumer(port: u16) -> (Arc<Cache>, UnboundedReceiver<HeadEvent>) {
    let mut cfg = CacheConfig::new("unused").with_feed(format!("127.0.0.1:{port}"));
    cfg.liveness = Duration::from_secs(5);
    Cache::with_fallback(cfg, Arc::new(NoRpc))
}

/// Reads HOT in a loop while the feed restarts: every read must succeed
/// from the store and never go backwards (a break would show as a miss).
fn reader(cache: Arc<Cache>, stop: Arc<AtomicBool>) -> JoinHandle<(u64, usize)> {
    tokio::spawn(async move {
        let (mut last, mut reads) = (0u64, 0usize);
        while !stop.load(Ordering::Relaxed) {
            if cache.tip().is_some() {
                let a = cache
                    .account(&HOT)
                    .await
                    .expect("HOT served from the store");
                assert!(
                    a.nonce >= last,
                    "HOT went backwards: {} after {last}",
                    a.nonce
                );
                last = a.nonce;
                reads += 1;
            }
            tokio::task::yield_now().await;
        }
        (last, reads)
    })
}

/// Events until the cache reaches `until` (or a timeout): new-head numbers
/// and break count.
async fn drain(events: &mut UnboundedReceiver<HeadEvent>, until: u64) -> (Vec<u64>, usize) {
    let (mut heads, mut breaks) = (Vec::new(), 0);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while heads.last().is_none_or(|&n| n < until) {
        match tokio::time::timeout_at(deadline, events.recv()).await {
            Ok(Some(HeadEvent::NewHead(h))) => heads.push(h.number),
            Ok(Some(HeadEvent::ChainBreak { .. })) => breaks += 1,
            Ok(Some(HeadEvent::Reorg { fork_number, .. })) => {
                panic!("no reorg in these tests, got one to {fork_number}")
            }
            Ok(None) | Err(_) => panic!("stalled at {heads:?} (breaks {breaks})"),
        }
    }
    (heads, breaks)
}

/// Common start: a client following blocks 1..=40 live, a reader running.
type Life = (
    u16,
    Arc<Feed>,
    Arc<Cache>,
    UnboundedReceiver<HeadEvent>,
    JoinHandle<()>,
);

async fn first_life() -> Life {
    let feed = Feed::new([]);
    let (port, task) = serve(feed.clone(), 0).await;
    let (cache, mut events) = consumer(port);
    wait_subscribed(&feed).await;
    for n in 1..=40 {
        feed.publish(span(n, n)).await;
    }
    let (heads, breaks) = drain(&mut events, 40).await;
    assert_eq!(heads, (1..=40).collect::<Vec<_>>());
    assert_eq!(breaks, 0);
    (port, feed, cache, events, task)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_republishing_held_blocks_resumes_without_a_break() {
    let (port, _, cache, mut events, task) = first_life().await;
    let stop = Arc::new(AtomicBool::new(false));
    let reads = reader(cache.clone(), stop.clone());
    kill(task).await;
    tokio::time::sleep(Duration::from_millis(300)).await; // the node is down

    // The new process replays from its last acked height (31): the client
    // already holds 31..=40; it asks from 41.
    let feed = Feed::new((31..=60).map(|n| span(n, n)));
    let (_, task) = serve(feed.clone(), port).await;
    wait_subscribed(&feed).await;
    for n in 61..=100 {
        feed.publish(span(n, n)).await;
    }
    let (heads, breaks) = drain(&mut events, 100).await;
    stop.store(true, Ordering::Relaxed);
    let (last_read, n_reads) = reads.await.unwrap();

    assert_eq!(
        heads,
        (41..=100).collect::<Vec<_>>(),
        "every head once, in order"
    );
    assert_eq!(breaks, 0);
    assert!(n_reads > 0 && last_read <= 100);
    assert_eq!(cache.account(&HOT).await.unwrap().nonce, 100);
    assert_eq!(cache.storage(&HOT, &word(7)).await.unwrap(), word(100));
    assert_eq!(
        cache.account(&[3; 20]).await.unwrap().nonce,
        3,
        "pre-restart key kept"
    );
    kill(task).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_republishing_a_straddling_span_applies_it() {
    let (port, _, cache, mut events, task) = first_life().await;
    let stop = Arc::new(AtomicBool::new(false));
    let reads = reader(cache.clone(), stop.clone());
    kill(task).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // The replay comes back as one folded span 31..=50 (spooled under 50 >=
    // 41), through the client's tip at 40.
    let feed = Feed::new([span(31, 50)]);
    let (_, task) = serve(feed.clone(), port).await;
    wait_subscribed(&feed).await;
    for n in 51..=60 {
        feed.publish(span(n, n)).await;
    }
    let (heads, breaks) = drain(&mut events, 60).await;
    stop.store(true, Ordering::Relaxed);
    reads.await.unwrap();

    assert_eq!(breaks, 0, "a span through our tip is not a break");
    assert_eq!(
        heads,
        std::iter::once(50).chain(51..=60).collect::<Vec<_>>()
    );
    assert_eq!(
        cache.account(&[45; 20]).await.unwrap().nonce,
        45,
        "inside the span"
    );
    assert_eq!(
        cache.account(&[3; 20]).await.unwrap().nonce,
        3,
        "pre-restart key kept"
    );
    assert_eq!(cache.account(&HOT).await.unwrap().nonce, 60);
    assert_eq!(cache.head(45).await.unwrap().hash, hash(45));
    kill(task).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_past_the_spool_drops_to_cold() {
    let (port, _, cache, mut events, task) = first_life().await;
    kill(task).await;

    // The client's next block (41) already left the new spool (50..=60).
    let feed = Feed::new((50..=60).map(|n| span(n, n)));
    let (_, task) = serve(feed.clone(), port).await;
    wait_subscribed(&feed).await;
    for n in 61..=70 {
        feed.publish(span(n, n)).await;
    }
    let (heads, breaks) = drain(&mut events, 70).await;
    assert_eq!(breaks, 1, "GapTooOld -> cold");
    assert_eq!(heads, (61..=70).collect::<Vec<_>>(), "live after the gap");
    assert!(
        cache.account(&[3; 20]).await.is_err(),
        "pre-gap state dropped (the read falls back, and this test has no RPC)"
    );
    assert_eq!(cache.account(&HOT).await.unwrap().nonce, 70);
    kill(task).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_with_an_empty_spool_and_a_skip_is_a_break() {
    let (port, _, cache, mut events, task) = first_life().await;
    kill(task).await;

    // Empty spool: the server can't tell the client it missed 41..=44; the
    // client's own continuity check must.
    let feed = Feed::new([]);
    let (_, task) = serve(feed.clone(), port).await;
    wait_subscribed(&feed).await;
    for n in 45..=50 {
        feed.publish(span(n, n)).await;
    }
    let (heads, breaks) = drain(&mut events, 50).await;
    assert_eq!(breaks, 1, "45 does not extend 40");
    assert_eq!(heads, (45..=50).collect::<Vec<_>>());
    assert!(
        cache.account(&[3; 20]).await.is_err(),
        "pre-break state dropped"
    );
    kill(task).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_consumer_follows_live_from_the_tip() {
    let (port, feed, _old, mut old_events, task) = first_life().await;
    // A consumer restart: the fresh cache is cold and subscribes from the
    // tip (None); nothing is replayed to it, it follows live.
    let before = feed.subscribers.load(Ordering::SeqCst);
    let (fresh, mut events) = consumer(port);
    tokio::time::timeout(Duration::from_secs(10), async {
        while feed.subscribers.load(Ordering::SeqCst) == before {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("fresh consumer never subscribed");
    assert!(
        fresh.tip().is_none(),
        "nothing replayed to a cold subscriber"
    );
    for n in 41..=45 {
        feed.publish(span(n, n)).await;
    }
    let (heads, breaks) = drain(&mut events, 45).await;
    assert_eq!((heads, breaks), ((41..=45).collect::<Vec<_>>(), 0));
    let (heads, _) = drain(&mut old_events, 45).await;
    assert_eq!(
        heads,
        (41..=45).collect::<Vec<_>>(),
        "the old consumer is unaffected"
    );
    assert_eq!(fresh.account(&HOT).await.unwrap().nonce, 45);
    kill(task).await;
}
