//! Lookup fast-path client (PLAN D9) and [`LookupFallback`], the
//! [`Fallback`] that sends cache misses over it.
//!
//! One TCP connection carries up to [`MAX_INFLIGHT`] requests; answers are
//! matched to callers by id, in any order. A supervisor task owns the
//! connection: it (re)connects with backoff and pings every
//! [`LookupClientConfig::ping_every`]. Callers never wait for a connect —
//! with no live connection a call fails at once with
//! [`LookupError::Unavailable`], and [`LookupFallback`] answers it from
//! JSON-RPC instead (D2).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::{Duration, Instant};

use silt_wire::lookup::{
    self, FLAG_INLINE_CODE, FrameWriter, ItemStatus, LATEST, MAX_INFLIGHT, MAX_RESPONSE_BYTES, Op,
    RequestBuilder, ResponseRef, ResultRef, Status,
};
use silt_wire::{Addr, Frame, Word, hello, hello_mismatch, read_frame, write_frame};
use tokio::io::{AsyncRead, BufReader};
use tokio::net::TcpStream;
use tokio::sync::{Semaphore, oneshot, watch};

use crate::fallback::{Fallback, RpcAccount, hex_word};

#[derive(Clone, Debug)]
pub struct LookupClientConfig {
    /// Server address (`host:port`), e.g. `127.0.0.1:8586`.
    pub addr: String,
    /// Per-request deadline (slot wait + round trip).
    pub timeout: Duration,
    pub connect_timeout: Duration,
    pub backoff_min: Duration,
    pub backoff_max: Duration,
    /// Keep-alive / liveness probe period (the server drops connections
    /// idle for 120 s).
    pub ping_every: Duration,
}

impl LookupClientConfig {
    pub fn new(addr: impl Into<String>) -> Self {
        Self {
            addr: addr.into(),
            timeout: Duration::from_millis(500),
            connect_timeout: Duration::from_secs(1),
            backoff_min: Duration::from_millis(100),
            backoff_max: Duration::from_secs(5),
            ping_every: Duration::from_secs(30),
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum LookupError {
    /// No live connection right now (connecting or backing off).
    Unavailable,
    Timeout,
    /// The connection died with the request in flight.
    ConnectionLost,
    /// Non-`Ok` frame status (`UnknownBlock`, `Busy`, `Internal`, ...).
    Status(Status),
    /// Malformed or mismatched response.
    Protocol(String),
    /// More items than the op allows in one frame.
    TooManyItems,
}

impl std::fmt::Display for LookupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unavailable => write!(f, "lookup server unavailable"),
            Self::Timeout => write!(f, "lookup timed out"),
            Self::ConnectionLost => write!(f, "lookup connection lost"),
            Self::Status(s) => write!(f, "lookup status {s:?}"),
            Self::Protocol(m) => write!(f, "lookup protocol error: {m}"),
            Self::TooManyItems => write!(f, "too many items for one lookup frame"),
        }
    }
}

impl std::error::Error for LookupError {}

/// A typed answer: what was read, and where.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Reply<T> {
    pub read_number: u64,
    pub read_hash: Word,
    /// Server time decode → encode (round trip minus this = transport).
    pub server_us: u32,
    pub items: Vec<T>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccountRead {
    pub status: ItemStatus,
    pub nonce: u64,
    pub balance: Word,
    /// `KECCAK_EMPTY` for no code.
    pub code_hash: Word,
    /// `Some` iff requested inline.
    pub code: Option<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageRead {
    pub status: ItemStatus,
    pub value: Word,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CodeRead {
    pub status: ItemStatus,
    pub code: Vec<u8>,
}

/// Counters (relaxed; for logs and benches).
#[derive(Default, Debug)]
pub struct LookupStats {
    pub requests: AtomicU64,
    pub connects: AtomicU64,
    /// Calls [`LookupFallback`] answered from JSON-RPC instead.
    pub fallbacks: AtomicU64,
}

/// One live connection.
struct Conn {
    /// Callers write their own frames (no task hop); see [`FrameWriter`].
    writer: Arc<FrameWriter<()>>,
    pending: Mutex<HashMap<u32, oneshot::Sender<Vec<u8>>>>,
    slots: Arc<Semaphore>,
    /// When the last frame arrived (liveness for timeouts).
    last_rx: Mutex<Instant>,
    dead: watch::Sender<bool>,
}

impl Conn {
    fn kill(&self, why: &str) {
        if !self.dead.send_replace(true) {
            tracing::debug!(why, "silt lookup connection down");
        }
        self.writer.close();
        // Dropping the senders wakes every waiter with ConnectionLost.
        self.pending.lock().expect("pending lock").clear();
    }

    fn is_dead(&self) -> bool {
        *self.dead.borrow()
    }
}

struct Shared {
    cfg: LookupClientConfig,
    conn: RwLock<Option<Arc<Conn>>>,
    next_id: AtomicU32,
    /// `(chain_id, genesis)` pinned at first contact.
    identity: Mutex<Option<(u64, Word)>>,
    stats: LookupStats,
}

/// Pipelined lookup client. Cheap to clone; dropping the last clone stops
/// its supervisor and closes the connection. Must be created inside a
/// tokio runtime.
#[derive(Clone)]
pub struct LookupClient {
    shared: Arc<Shared>,
}

impl LookupClient {
    pub fn new(cfg: LookupClientConfig) -> Self {
        let shared = Arc::new(Shared {
            cfg,
            conn: RwLock::new(None),
            next_id: AtomicU32::new(1),
            identity: Mutex::new(None),
            stats: LookupStats::default(),
        });
        tokio::spawn(supervise(Arc::downgrade(&shared)));
        Self { shared }
    }

    pub fn stats(&self) -> &LookupStats {
        &self.shared.stats
    }

    pub fn is_connected(&self) -> bool {
        self.live().is_some()
    }

    /// Waits up to `limit` for a live connection (benches, tests, startup).
    pub async fn wait_connected(&self, limit: Duration) -> bool {
        let deadline = Instant::now() + limit;
        while Instant::now() < deadline {
            if self.is_connected() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        self.is_connected()
    }

    fn live(&self) -> Option<Arc<Conn>> {
        let conn = self.shared.conn.read().expect("conn lock").clone()?;
        (!conn.is_dead()).then_some(conn)
    }

    /// Sends one frame built for the allocated id; returns the raw body.
    async fn round_trip(&self, build: impl FnOnce(u32) -> Vec<u8>) -> Result<Vec<u8>, LookupError> {
        let conn = self.live().ok_or(LookupError::Unavailable)?;
        let timeout = self.shared.cfg.timeout;
        let deadline = tokio::time::Instant::now() + timeout;
        let _slot = tokio::time::timeout_at(deadline, conn.slots.acquire())
            .await
            .map_err(|_| LookupError::Timeout)?
            .map_err(|_| LookupError::ConnectionLost)?;
        let id = self.shared.next_id.fetch_add(1, Ordering::Relaxed);
        let (done, answer) = oneshot::channel();
        conn.pending.lock().expect("pending lock").insert(id, done);
        if conn.is_dead() || conn.writer.send(build(id), ()).is_err() {
            conn.pending.lock().expect("pending lock").remove(&id);
            return Err(LookupError::ConnectionLost);
        }
        self.shared.stats.requests.fetch_add(1, Ordering::Relaxed);
        let sent = Instant::now();
        match tokio::time::timeout_at(deadline, answer).await {
            Ok(Ok(body)) => Ok(body),
            Ok(Err(_)) => Err(LookupError::ConnectionLost),
            Err(_) => {
                conn.pending.lock().expect("pending lock").remove(&id);
                // Nothing at all came back since we sent: the connection is
                // wedged, not just slow. Reconnect.
                if *conn.last_rx.lock().expect("last_rx lock") < sent {
                    conn.kill("request timed out on a silent connection");
                }
                Err(LookupError::Timeout)
            }
        }
    }

    /// One request frame; checks status, op, count and the pin.
    async fn call(
        &self,
        op: Op,
        flags: u8,
        pin: &Word,
        count: usize,
        push: impl FnOnce(&mut RequestBuilder),
    ) -> Result<Vec<u8>, LookupError> {
        if count > op.max_items(flags) {
            return Err(LookupError::TooManyItems);
        }
        let body = self
            .round_trip(|id| {
                let mut b = RequestBuilder::with_capacity(op, flags, id, pin, count);
                push(&mut b);
                b.finish()
            })
            .await?;
        let r = ResponseRef::decode(&body).map_err(|e| LookupError::Protocol(e.to_string()))?;
        if r.status != Status::Ok {
            return Err(LookupError::Status(r.status));
        }
        if r.op != op as u8 || r.flags != flags || r.count() != count {
            return Err(LookupError::Protocol(
                "response does not match request".into(),
            ));
        }
        if *pin != LATEST && r.read_hash != pin {
            return Err(LookupError::Protocol(
                "response read at a different block".into(),
            ));
        }
        Ok(body)
    }

    fn reply<T>(body: &[u8], map: impl FnMut(ResultRef<'_>) -> T) -> Reply<T> {
        let r = ResponseRef::decode(body).expect("validated in call()");
        Reply {
            read_number: r.read_number,
            read_hash: *r.read_hash,
            server_us: r.server_us,
            items: r.results().map(map).collect(),
        }
    }

    /// Resolves `pin` without reading anything (`count = 0`).
    pub async fn ping(&self, pin: &Word) -> Result<Reply<()>, LookupError> {
        let body = self.call(Op::Storage, 0, pin, 0, |_| {}).await?;
        Ok(Self::reply(&body, |_| ()))
    }

    pub async fn accounts(
        &self,
        pin: &Word,
        addrs: &[Addr],
        inline_code: bool,
    ) -> Result<Reply<AccountRead>, LookupError> {
        let flags = if inline_code { FLAG_INLINE_CODE } else { 0 };
        let body = self
            .call(Op::Account, flags, pin, addrs.len(), |b| {
                for a in addrs {
                    b.push_account(a);
                }
            })
            .await?;
        Ok(Self::reply(&body, |r| match r {
            ResultRef::Account {
                status,
                nonce,
                balance,
                code_hash,
                code,
            } => AccountRead {
                status,
                nonce,
                balance: *balance,
                code_hash: *code_hash,
                code: code.map(<[u8]>::to_vec),
            },
            _ => unreachable!("op checked in call()"),
        }))
    }

    pub async fn storage(
        &self,
        pin: &Word,
        keys: &[(Addr, Word)],
    ) -> Result<Reply<StorageRead>, LookupError> {
        let body = self
            .call(Op::Storage, 0, pin, keys.len(), |b| {
                for (a, s) in keys {
                    b.push_storage(a, s);
                }
            })
            .await?;
        Ok(Self::reply(&body, |r| match r {
            ResultRef::Storage { status, value } => StorageRead {
                status,
                value: *value,
            },
            _ => unreachable!("op checked in call()"),
        }))
    }

    /// Bytecode by hash (immutable: safe to cache forever).
    pub async fn code(&self, pin: &Word, hashes: &[Word]) -> Result<Reply<CodeRead>, LookupError> {
        let body = self
            .call(Op::Code, 0, pin, hashes.len(), |b| {
                for h in hashes {
                    b.push_code(h);
                }
            })
            .await?;
        Ok(Self::reply(&body, |r| match r {
            ResultRef::Code { status, code } => CodeRead {
                status,
                code: code.to_vec(),
            },
            _ => unreachable!("op checked in call()"),
        }))
    }
}

/// Resolves once the connection is marked dead (the guard `wait_for`
/// returns is not `Send`, so it must not live across awaits).
async fn died(dead: &mut watch::Receiver<bool>) {
    let _ = dead.wait_for(|d| *d).await;
}

/// Owns the connection: connect, publish, wait for death (pinging), back
/// off, repeat. Exits once the client is dropped.
async fn supervise(shared: Weak<Shared>) {
    let mut backoff = match shared.upgrade() {
        Some(s) => s.cfg.backoff_min,
        None => return,
    };
    loop {
        let Some(s) = shared.upgrade() else { return };
        let cfg = s.cfg.clone();
        match connect(&s).await {
            Ok(conn) => {
                backoff = cfg.backoff_min;
                s.stats.connects.fetch_add(1, Ordering::Relaxed);
                *s.conn.write().expect("conn lock") = Some(conn.clone());
                drop(s);
                let mut dead = conn.dead.subscribe();
                loop {
                    tokio::select! {
                        _ = died(&mut dead) => break,
                        _ = tokio::time::sleep(cfg.ping_every) => {
                            let Some(s) = shared.upgrade() else {
                                conn.kill("client dropped");
                                return;
                            };
                            let client = LookupClient { shared: s };
                            if let Err(err) = client.ping(&LATEST).await
                                && err != LookupError::Status(Status::Busy)
                            {
                                conn.kill(&format!("ping failed: {err}"));
                            }
                        }
                    }
                }
                if let Some(s) = shared.upgrade() {
                    let mut slot = s.conn.write().expect("conn lock");
                    if slot.as_ref().is_some_and(|c| Arc::ptr_eq(c, &conn)) {
                        *slot = None;
                    }
                }
            }
            Err(err) => {
                tracing::debug!(%err, addr = cfg.addr, "silt lookup connect failed");
                drop(s);
            }
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(cfg.backoff_max);
    }
}

async fn connect(s: &Shared) -> eyre::Result<Arc<Conn>> {
    let stream = tokio::time::timeout(
        s.cfg.connect_timeout,
        tokio::net::TcpStream::connect(&s.cfg.addr),
    )
    .await
    .map_err(|_| eyre::eyre!("connect timed out"))??;
    stream.set_nodelay(true)?;
    start(stream, s).await
}

/// Handshake, then the reader task and the write-backlog flusher.
async fn start(mut stream: TcpStream, s: &Shared) -> eyre::Result<Arc<Conn>> {
    let handshake = async {
        let theirs = read_frame(&mut stream)
            .await?
            .ok_or_else(|| eyre::eyre!("eof before Hello"))?;
        let Frame::Hello {
            chain_id,
            genesis_hash,
            ..
        } = theirs
        else {
            eyre::bail!("expected server Hello");
        };
        let ours = hello(chain_id, genesis_hash, 0);
        // Exact version match is checked on our side too: a mismatched
        // server must never see a lookup frame.
        if let Some(reason) = hello_mismatch(&ours, &theirs) {
            eyre::bail!("refusing lookup server: {reason}");
        }
        {
            let mut pinned = s.identity.lock().expect("identity lock");
            match *pinned {
                None => *pinned = Some((chain_id, genesis_hash)),
                Some(p) => eyre::ensure!(
                    p == (chain_id, genesis_hash),
                    "lookup server identity changed (chain {} -> {chain_id})",
                    p.0
                ),
            }
        }
        write_frame(&mut stream, &ours).await
    };
    tokio::time::timeout(s.cfg.connect_timeout, handshake)
        .await
        .map_err(|_| eyre::eyre!("handshake timed out"))??;

    let (rd, wr) = stream.into_split();
    let (dead, _) = watch::channel(false);
    let conn = Arc::new(Conn {
        writer: Arc::new(FrameWriter::new(wr)),
        pending: Mutex::new(HashMap::new()),
        slots: Arc::new(Semaphore::new(MAX_INFLIGHT)),
        last_rx: Mutex::new(Instant::now()),
        dead,
    });
    tokio::spawn(read_loop(rd, conn.clone()));
    {
        let conn = conn.clone();
        let deadline = s.cfg.timeout.max(Duration::from_secs(1));
        tokio::spawn(async move {
            if let Err(err) = conn.writer.run(deadline).await {
                conn.kill(&format!("write: {err}"));
            }
        });
    }
    Ok(conn)
}

async fn read_loop<R: AsyncRead + Unpin>(rd: R, conn: Arc<Conn>) {
    let mut rd = BufReader::with_capacity(64 * 1024, rd);
    let mut dead = conn.dead.subscribe();
    loop {
        let mut buf = Vec::new();
        let read = tokio::select! {
            _ = died(&mut dead) => return,
            read = lookup::read_body(&mut rd, &mut buf, MAX_RESPONSE_BYTES) => read,
        };
        match read {
            Ok(true) => {}
            Ok(false) => return conn.kill("server closed"),
            Err(err) => return conn.kill(&format!("read: {err}")),
        }
        *conn.last_rx.lock().expect("last_rx lock") = Instant::now();
        let Some((_, _, id)) = lookup::peek_prefix(&buf) else {
            return conn.kill("short frame");
        };
        // Unknown id = an answer to a request that already timed out.
        if let Some(waiter) = conn.pending.lock().expect("pending lock").remove(&id) {
            let _ = waiter.send(buf);
        }
    }
}

/// [`Fallback`] whose account and storage misses take the lookup fast path
/// (D9); heads, blocks, chain id and **every lookup failure** go to the
/// inner fallback (JSON-RPC, D2). `UnknownBlock` is an error, as the RPC
/// would answer for that pin.
pub struct LookupFallback {
    client: LookupClient,
    inner: Arc<dyn Fallback>,
}

impl LookupFallback {
    pub fn new(cfg: LookupClientConfig, inner: Arc<dyn Fallback>) -> Self {
        Self {
            client: LookupClient::new(cfg),
            inner,
        }
    }

    pub fn client(&self) -> &LookupClient {
        &self.client
    }

    fn fell_back(&self, what: &str, err: &dyn std::fmt::Display) {
        self.client
            .stats()
            .fallbacks
            .fetch_add(1, Ordering::Relaxed);
        tracing::debug!(what, %err, "silt lookup -> rpc fallback");
    }
}

fn unknown_block(block_hash: &Word) -> eyre::Report {
    eyre::eyre!(
        "lookup: block {} is unknown, non-canonical or pruned",
        hex_word(block_hash)
    )
}

#[async_trait::async_trait]
impl Fallback for LookupFallback {
    async fn account_at(&self, addr: &Addr, block_hash: &Word) -> eyre::Result<RpcAccount> {
        match self.client.accounts(block_hash, &[*addr], true).await {
            Ok(mut reply) => {
                let item = reply.items.pop().expect("count checked");
                if item.status == ItemStatus::Error {
                    self.fell_back("account", &"item error");
                    return self.inner.account_at(addr, block_hash).await;
                }
                // Ok and Absent alike: Absent carries zeros + no code,
                // exactly what eth_* reports for a missing account.
                Ok(RpcAccount {
                    balance: item.balance,
                    nonce: item.nonce,
                    code: item.code.unwrap_or_default(),
                })
            }
            Err(LookupError::Status(Status::UnknownBlock)) => Err(unknown_block(block_hash)),
            Err(err) => {
                self.fell_back("account", &err);
                self.inner.account_at(addr, block_hash).await
            }
        }
    }

    async fn storage_at(&self, addr: &Addr, slot: &Word, block_hash: &Word) -> eyre::Result<Word> {
        match self.client.storage(block_hash, &[(*addr, *slot)]).await {
            Ok(reply) if reply.items[0].status != ItemStatus::Error => Ok(reply.items[0].value),
            Ok(_) => {
                self.fell_back("storage", &"item error");
                self.inner.storage_at(addr, slot, block_hash).await
            }
            Err(LookupError::Status(Status::UnknownBlock)) => Err(unknown_block(block_hash)),
            Err(err) => {
                self.fell_back("storage", &err);
                self.inner.storage_at(addr, slot, block_hash).await
            }
        }
    }

    async fn head_by_number(&self, number: Option<u64>) -> eyre::Result<silt_wire::HeadLite> {
        self.inner.head_by_number(number).await
    }

    async fn block_json(&self, number: u64, full: bool) -> eyre::Result<serde_json::Value> {
        self.inner.block_json(number, full).await
    }

    async fn chain_id(&self) -> eyre::Result<u64> {
        self.inner.chain_id().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Cache, CacheConfig};
    use silt_wire::KECCAK_EMPTY;
    use silt_wire::lookup::{Item, RequestRef, ResponseBuilder, read_body};
    use tokio::io::AsyncWriteExt;
    use tokio::net::{TcpListener, TcpStream};

    const CHAIN_ID: u64 = 1;
    const GENESIS: Word = [7; 32];
    const HEAD: (u64, Word) = (100, [1; 32]);
    const CONTRACT: Addr = [0xC0; 20];
    const RPC_WORD: Word = [0x77; 32];

    async fn accept(listener: &TcpListener) -> TcpStream {
        let (mut s, _) = listener.accept().await.unwrap();
        write_frame(&mut s, &hello(CHAIN_ID, GENESIS, HEAD.0))
            .await
            .unwrap();
        let theirs = read_frame(&mut s).await.unwrap().unwrap();
        assert!(hello_mismatch(&hello(CHAIN_ID, GENESIS, 0), &theirs).is_none());
        s
    }

    async fn next_request(s: &mut TcpStream) -> Option<Vec<u8>> {
        let mut buf = Vec::new();
        read_body(s, &mut buf, lookup::MAX_REQUEST_BYTES)
            .await
            .ok()
            .filter(|&got| got)
            .map(|_| buf)
    }

    /// Deterministic answers: storage value = slot; account nonce/balance
    /// from the address; `[0; 20]` is absent; CONTRACT has code.
    fn answer(body: &[u8]) -> Vec<u8> {
        let req = RequestRef::decode(body).unwrap();
        let read = if req.is_latest() { HEAD.1 } else { *req.pin };
        let mut out = ResponseBuilder::new(req.op, req.flags, req.id, HEAD.0, &read, req.count());
        for item in req.items() {
            match item {
                Item::Storage(_, slot) => out.push_storage(ItemStatus::Ok, slot),
                Item::Account(a) if *a == [0; 20] => out.push_account_absent(),
                Item::Account(a) => {
                    let code: &[u8] = if *a == CONTRACT { &[0x60, 0x00] } else { &[] };
                    let hash = if code.is_empty() {
                        KECCAK_EMPTY
                    } else {
                        [0xC1; 32]
                    };
                    out.push_account(ItemStatus::Ok, a[0] as u64, &[a[0]; 32], &hash, code)
                }
                Item::Code(_) => out.push_code(ItemStatus::Absent, &[]),
            }
        }
        out.finish(3)
    }

    fn cfg(addr: std::net::SocketAddr) -> LookupClientConfig {
        LookupClientConfig {
            backoff_min: Duration::from_millis(20),
            backoff_max: Duration::from_millis(50),
            ..LookupClientConfig::new(addr.to_string())
        }
        .with_timeout(Duration::from_millis(300))
    }

    async fn listener() -> (TcpListener, std::net::SocketAddr) {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        (l, addr)
    }

    #[tokio::test]
    async fn pipelined_requests_match_out_of_order_answers() {
        let (l, addr) = listener().await;
        tokio::spawn(async move {
            let mut s = accept(&l).await;
            let first = next_request(&mut s).await.unwrap();
            let second = next_request(&mut s).await.unwrap();
            s.write_all(&answer(&second)).await.unwrap(); // reversed
            s.write_all(&answer(&first)).await.unwrap();
            while let Some(req) = next_request(&mut s).await {
                s.write_all(&answer(&req)).await.unwrap();
            }
        });
        let client = LookupClient::new(cfg(addr));
        assert!(client.wait_connected(Duration::from_secs(5)).await);
        let (a, b) = tokio::join!(
            client.storage(&HEAD.1, &[([1; 20], [0xA1; 32])]),
            client.storage(&HEAD.1, &[([1; 20], [0xB2; 32]), ([2; 20], [0xB3; 32])]),
        );
        let (a, b) = (a.unwrap(), b.unwrap());
        assert_eq!(a.items[0].value, [0xA1; 32]);
        assert_eq!(
            b.items.iter().map(|r| r.value).collect::<Vec<_>>(),
            [[0xB2; 32], [0xB3; 32]]
        );
        assert_eq!(
            (a.read_number, a.read_hash, a.server_us),
            (HEAD.0, HEAD.1, 3)
        );

        // Many more in flight on the same connection.
        let calls = (0..200u8).map(|i| {
            let client = client.clone();
            async move { client.storage(&LATEST, &[([i; 20], [i; 32])]).await }
        });
        for (i, r) in futures::future::join_all(calls)
            .await
            .into_iter()
            .enumerate()
        {
            assert_eq!(r.unwrap().items[0].value, [i as u8; 32]);
        }
        let accts = client
            .accounts(&LATEST, &[CONTRACT, [0; 20]], true)
            .await
            .unwrap();
        assert_eq!(accts.items[0].code.as_deref(), Some(&[0x60, 0x00][..]));
        assert_eq!(accts.items[1].status, ItemStatus::Absent);
        assert_eq!(accts.items[1].code_hash, KECCAK_EMPTY);
        assert_eq!(client.stats().connects.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn reconnects_after_the_server_drops_the_connection() {
        let (l, addr) = listener().await;
        tokio::spawn(async move {
            let mut s = accept(&l).await; // first connection: one answer, then gone
            let req = next_request(&mut s).await.unwrap();
            s.write_all(&answer(&req)).await.unwrap();
            drop(s);
            let mut s = accept(&l).await;
            while let Some(req) = next_request(&mut s).await {
                s.write_all(&answer(&req)).await.unwrap();
            }
        });
        let client = LookupClient::new(cfg(addr));
        assert!(client.wait_connected(Duration::from_secs(5)).await);
        client
            .storage(&LATEST, &[([1; 20], [1; 32])])
            .await
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while client.stats().connects.load(Ordering::Relaxed) < 2 {
            assert!(Instant::now() < deadline, "no reconnect");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(client.wait_connected(Duration::from_secs(5)).await);
        let r = client
            .storage(&LATEST, &[([1; 20], [9; 32])])
            .await
            .unwrap();
        assert_eq!(r.items[0].value, [9; 32]);
    }

    /// In-memory RPC stand-in that counts the calls it serves.
    struct Rpc(AtomicU64);

    #[async_trait::async_trait]
    impl Fallback for Rpc {
        async fn account_at(&self, _a: &Addr, _h: &Word) -> eyre::Result<RpcAccount> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(RpcAccount {
                balance: RPC_WORD,
                nonce: 77,
                code: vec![],
            })
        }
        async fn storage_at(&self, _a: &Addr, _s: &Word, _h: &Word) -> eyre::Result<Word> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(RPC_WORD)
        }
        async fn head_by_number(&self, _n: Option<u64>) -> eyre::Result<silt_wire::HeadLite> {
            Ok(silt_wire::HeadLite {
                number: HEAD.0,
                hash: HEAD.1,
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
            Ok(CHAIN_ID)
        }
    }

    /// The server's behaviour keyed by `slot[0]`.
    fn misbehaving(body: &[u8]) -> Option<Vec<u8>> {
        let req = RequestRef::decode(body).unwrap();
        let first = req.items().next();
        let key = match first {
            Some(Item::Storage(_, slot)) => slot[0],
            _ => 0,
        };
        let status = |s| {
            Some(lookup::error_response(
                req.op as u8,
                req.flags,
                req.id,
                s,
                1,
            ))
        };
        match key {
            1 => status(Status::Busy),
            2 => status(Status::Internal),
            3 => {
                let mut out = ResponseBuilder::new(req.op, req.flags, req.id, HEAD.0, &HEAD.1, 1);
                out.push_storage(ItemStatus::Error, &[0; 32]);
                Some(out.finish(1))
            }
            4 => status(Status::UnknownBlock),
            5 => None, // never answers
            6 => status(Status::BadRequest),
            _ => Some(answer(body)),
        }
    }

    #[tokio::test]
    async fn every_failure_mode_falls_back_to_rpc_except_unknown_block() {
        let (l, addr) = listener().await;
        let server = tokio::spawn(async move {
            loop {
                let mut s = accept(&l).await;
                while let Some(req) = next_request(&mut s).await {
                    if let Some(out) = misbehaving(&req) {
                        s.write_all(&out).await.unwrap();
                    }
                }
            }
        });
        let rpc = Arc::new(Rpc(AtomicU64::new(0)));
        let fb = LookupFallback::new(cfg(addr), rpc.clone());
        assert!(fb.client().wait_connected(Duration::from_secs(5)).await);
        let a = [1; 20];
        let slot = |k: u8| {
            let mut s = [0x5A; 32];
            s[0] = k;
            s
        };

        // The fast path proper: no RPC call.
        assert_eq!(fb.storage_at(&a, &slot(0), &HEAD.1).await.unwrap(), slot(0));
        let acct = fb.account_at(&CONTRACT, &HEAD.1).await.unwrap();
        assert_eq!(
            (acct.nonce, acct.code.as_slice()),
            (0xC0, &[0x60, 0x00][..])
        );
        let absent = fb.account_at(&[0; 20], &HEAD.1).await.unwrap();
        assert_eq!(
            absent,
            RpcAccount {
                balance: [0; 32],
                nonce: 0,
                code: vec![]
            }
        );
        assert_eq!(rpc.0.load(Ordering::SeqCst), 0);

        // Busy, Internal, item Error, BadRequest: answered by RPC.
        for (n, k) in [1u8, 2, 3, 6].into_iter().enumerate() {
            assert_eq!(
                fb.storage_at(&a, &slot(k), &HEAD.1).await.unwrap(),
                RPC_WORD,
                "key {k}"
            );
            assert_eq!(rpc.0.load(Ordering::SeqCst), n as u64 + 1);
        }
        // UnknownBlock: an error, like RPC with that pin; no RPC call.
        let err = fb.storage_at(&a, &slot(4), &HEAD.1).await.unwrap_err();
        assert!(err.to_string().contains("unknown"), "{err}");
        assert_eq!(rpc.0.load(Ordering::SeqCst), 4);
        // Timeout: RPC after the per-request deadline.
        let t = Instant::now();
        assert_eq!(
            fb.storage_at(&a, &slot(5), &HEAD.1).await.unwrap(),
            RPC_WORD
        );
        assert!(t.elapsed() >= Duration::from_millis(300));
        assert_eq!(rpc.0.load(Ordering::SeqCst), 5);

        // Server gone: RPC at once, no waiting for a connect.
        server.abort();
        let _ = server.await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        let t = Instant::now();
        assert_eq!(
            fb.storage_at(&a, &slot(0), &HEAD.1).await.unwrap(),
            RPC_WORD
        );
        assert!(
            t.elapsed() < Duration::from_millis(100),
            "{:?}",
            t.elapsed()
        );
        assert_eq!(fb.client().stats().fallbacks.load(Ordering::Relaxed), 6);
    }

    #[tokio::test]
    async fn cache_misses_ride_lookup_with_single_flight_intact() {
        let (l, addr) = listener().await;
        let requests = Arc::new(AtomicU64::new(0));
        let seen = requests.clone();
        tokio::spawn(async move {
            let mut s = accept(&l).await;
            while let Some(req) = next_request(&mut s).await {
                seen.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(20)).await; // let misses pile up
                s.write_all(&answer(&req)).await.unwrap();
            }
        });
        let rpc = Arc::new(Rpc(AtomicU64::new(0)));
        let fb = Arc::new(LookupFallback::new(cfg(addr), rpc.clone()));
        assert!(fb.client().wait_connected(Duration::from_secs(5)).await);
        let (cache, _events) = Cache::with_fallback(CacheConfig::new("unused"), fb);
        let tasks: Vec<_> = (0..32)
            .map(|_| {
                let cache = cache.clone();
                tokio::spawn(async move { cache.storage(&[3; 20], &[0x42; 32]).await.unwrap() })
            })
            .collect();
        for t in tasks {
            assert_eq!(t.await.unwrap(), [0x42; 32]);
        }
        assert_eq!(
            requests.load(Ordering::SeqCst),
            1,
            "single-flight: one lookup"
        );
        let view = cache.account(&CONTRACT).await.unwrap();
        assert_eq!(view.code.as_slice(), &[0x60, 0x00]);
        assert_eq!(rpc.0.load(Ordering::SeqCst), 0, "nothing went to RPC");
    }
}
