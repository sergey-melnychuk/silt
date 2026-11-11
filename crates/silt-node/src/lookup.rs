//! Lookup fast path server (PLAN D9): binary request/response tip-state
//! reads on their own listener, next to the push-only feed (D8).
//!
//! The server is generic over a small [`StateSource`], so the same code is
//! driven by the node's provider ([`NodeSource`], from the ExEx), by the
//! read-only harness (`examples/lookup-ro.rs`) and by in-memory tests.
//!
//! Threading (D9): per connection, a reader task (frame reads with
//! deadlines, header validation, `Busy` decisions, dispatch) and a writer
//! task (drains the response queue, one deadline per write, flushes when
//! the queue is empty). Database reads run on a dedicated pool of OS
//! threads — never on async workers, the ExEx notification loop, or tokio's
//! blocking pool (which reth's RPC shares). A job opens one state view per
//! request frame, reads every item, encodes the response and drops the
//! view: no provider/MDBX read transaction outlives a request.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{SyncSender, TrySendError, sync_channel};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use alloy_primitives::{Address, B256, Bytes};
use reth_ethereum::provider::{ProviderError, StateProviderBox, StateProviderFactory};
use silt_wire::lookup::{
    self, FrameWriter, Item, ItemStatus, MAX_INFLIGHT, MAX_REQUEST_BYTES, MAX_RESPONSE_BYTES,
    RequestRef, ResponseBuilder, Status,
};
use silt_wire::{Addr, Frame, KECCAK_EMPTY, Word, hello, hello_mismatch, read_frame, write_frame};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, BufReader};
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

// ------------------------------------------------------------ state access

/// Account fields as the wire carries them; `code_hash` is `KECCAK_EMPTY`
/// for "no code", never zero.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AccountInfo {
    pub nonce: u64,
    pub balance: Word,
    pub code_hash: Word,
}

#[derive(Debug)]
pub enum ReadError {
    /// Unknown, non-canonical, pruned or not-yet-readable block: the whole
    /// frame answers `UnknownBlock`, never partial or stale data.
    UnknownBlock(String),
    Other(String),
}

impl std::fmt::Display for ReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownBlock(m) => write!(f, "unknown block: {m}"),
            Self::Other(m) => f.write_str(m),
        }
    }
}

/// One consistent state snapshot at one block (one request frame).
pub trait StateView {
    /// `None` = no such account.
    fn account(&self, addr: &Addr) -> Result<Option<AccountInfo>, ReadError>;
    /// A missing slot reads as zero.
    fn storage(&self, addr: &Addr, slot: &Word) -> Result<Word, ReadError>;
    /// `None` = no bytecode under that hash.
    fn code(&self, code_hash: &Word) -> Result<Option<Bytes>, ReadError>;
}

/// A view plus the block it reads at.
pub struct Opened<V> {
    pub number: u64,
    pub hash: Word,
    pub view: V,
}

/// Where views come from. `open` runs on a pool thread and may block.
pub trait StateSource: Send + Sync + 'static {
    type View: StateView;
    /// `pin = None` means this source's "latest".
    fn open(&self, pin: Option<&Word>) -> Result<Opened<Self::View>, ReadError>;
}

/// Maps reth's errors: everything that means "this block's state is not
/// (or no longer, or not yet) readable" becomes `UnknownBlock`.
pub fn classify(err: ProviderError) -> ReadError {
    match err {
        ProviderError::BlockHashNotFound(_)
        | ProviderError::HeaderNotFound(_)
        | ProviderError::UnknownBlockHash(_)
        | ProviderError::StateForHashNotFound(_)
        | ProviderError::StateForNumberNotFound(_)
        | ProviderError::StateAtBlockPruned(_)
        | ProviderError::InsufficientChangesets { .. }
        | ProviderError::BlockNotExecuted { .. } => ReadError::UnknownBlock(err.to_string()),
        other => ReadError::Other(other.to_string()),
    }
}

/// A reth state provider as a [`StateView`] (node and read-only harness).
pub struct RethView(pub StateProviderBox);

impl StateView for RethView {
    fn account(&self, addr: &Addr) -> Result<Option<AccountInfo>, ReadError> {
        let account = self
            .0
            .basic_account(&Address::from(*addr))
            .map_err(classify)?;
        Ok(account.map(|a| AccountInfo {
            nonce: a.nonce,
            balance: a.balance.to_be_bytes(),
            // reth stores `None` for accounts without code (D9: normalize).
            code_hash: a.bytecode_hash.map_or(KECCAK_EMPTY, |h| h.0),
        }))
    }

    fn storage(&self, addr: &Addr, slot: &Word) -> Result<Word, ReadError> {
        let value = self
            .0
            .storage(Address::from(*addr), B256::from(*slot))
            .map_err(classify)?;
        Ok(value.unwrap_or_default().to_be_bytes())
    }

    fn code(&self, code_hash: &Word) -> Result<Option<Bytes>, ReadError> {
        let code = self
            .0
            .bytecode_by_hash(&B256::from(*code_hash))
            .map_err(classify)?;
        Ok(code.map(|c| c.original_bytes()))
    }
}

/// The node's own provider (`ctx.provider()`): "latest" is the canonical
/// head, including blocks executed but not yet persisted.
pub struct NodeSource<P> {
    provider: P,
}

impl<P> NodeSource<P> {
    pub const fn new(provider: P) -> Self {
        Self { provider }
    }
}

impl<P> StateSource for NodeSource<P>
where
    P: StateProviderFactory + Send + Sync + 'static,
{
    type View = RethView;

    fn open(&self, pin: Option<&Word>) -> Result<Opened<RethView>, ReadError> {
        let Some(pin) = pin else {
            // Resolve latest to a hash first, so the response names exactly
            // what was read. A reorg between the two calls can orphan that
            // hash; one retry picks up the new head.
            let mut last = None;
            for _ in 0..2 {
                let info = self.provider.chain_info().map_err(classify)?;
                match self.provider.state_by_block_hash(info.best_hash) {
                    Ok(state) => {
                        return Ok(Opened {
                            number: info.best_number,
                            hash: info.best_hash.0,
                            view: RethView(state),
                        });
                    }
                    Err(err) => last = Some(err),
                }
            }
            return Err(classify(last.expect("loop ran")));
        };
        let hash = B256::from(*pin);
        // Canonical only: `state_by_block_hash` alone would also serve the
        // pending block (docs/reth-notes.md).
        let number = self
            .provider
            .block_number(hash)
            .map_err(classify)?
            .ok_or_else(|| ReadError::UnknownBlock(format!("{hash} is not canonical")))?;
        if self.provider.block_hash(number).map_err(classify)? != Some(hash) {
            return Err(ReadError::UnknownBlock(format!("{hash} is not canonical")));
        }
        let state = self.provider.state_by_block_hash(hash).map_err(classify)?;
        Ok(Opened {
            number,
            hash: hash.0,
            view: RethView(state),
        })
    }
}

// --------------------------------------------------------------- the server

#[derive(Clone, Debug)]
pub struct LookupConfig {
    /// Dedicated read threads.
    pub workers: usize,
    /// Server-wide queued jobs; a request arriving when it is full answers
    /// `Busy`.
    pub queue: usize,
    pub handshake_timeout: Duration,
    /// Rest of a frame once its length arrived.
    pub frame_timeout: Duration,
    pub write_timeout: Duration,
    /// Silence between frames before the connection is dropped (clients
    /// ping every 30 s).
    pub idle_timeout: Duration,
}

impl Default for LookupConfig {
    fn default() -> Self {
        Self {
            workers: 16,
            queue: 4096,
            handshake_timeout: Duration::from_secs(10),
            frame_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(120),
        }
    }
}

type Job = Box<dyn FnOnce() + Send>;

/// Fixed pool of named OS threads behind a bounded queue.
struct Pool {
    tx: SyncSender<Job>,
}

impl Pool {
    fn new(workers: usize, queue: usize) -> std::io::Result<Self> {
        let (tx, rx) = sync_channel::<Job>(queue.max(1));
        let rx = Arc::new(Mutex::new(rx));
        for i in 0..workers.max(1) {
            let rx = rx.clone();
            std::thread::Builder::new()
                .name(format!("silt-lookup-{i}"))
                .spawn(move || {
                    loop {
                        // The lock is held only while waiting for one job.
                        let job = match rx.lock() {
                            Ok(rx) => rx.recv(),
                            Err(_) => return,
                        };
                        let Ok(job) = job else { return }; // server dropped
                        // A panicking read must not shrink the pool; the job
                        // itself turns the panic into an `Internal` answer.
                        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(job));
                    }
                })?;
        }
        Ok(Self { tx })
    }

    fn try_submit(&self, job: Job) -> bool {
        match self.tx.try_send(job) {
            Ok(()) => true,
            Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) => false,
        }
    }
}

pub struct LookupServer<S> {
    source: Arc<S>,
    pool: Pool,
    chain_id: u64,
    genesis_hash: Word,
    /// Newest block any request read at (the `tip` in our Hello).
    tip: Arc<AtomicU64>,
    cfg: LookupConfig,
}

impl<S: StateSource> LookupServer<S> {
    pub fn new(
        source: S,
        chain_id: u64,
        genesis_hash: Word,
        cfg: LookupConfig,
    ) -> std::io::Result<Arc<Self>> {
        Ok(Arc::new(Self {
            source: Arc::new(source),
            pool: Pool::new(cfg.workers, cfg.queue)?,
            chain_id,
            genesis_hash,
            tip: Arc::new(AtomicU64::new(0)),
            cfg,
        }))
    }

    /// Accept loop; one reader task + one flusher task per connection.
    pub async fn serve(self: Arc<Self>, listener: TcpListener) {
        loop {
            match listener.accept().await {
                Ok((stream, peer)) => {
                    // Small frames, latency-bound: never wait for Nagle.
                    let _ = stream.set_nodelay(true);
                    let this = self.clone();
                    tokio::spawn(async move {
                        if let Err(err) = this.handle(stream).await {
                            tracing::debug!(%peer, %err, "silt lookup connection closed");
                        }
                    });
                }
                Err(err) => {
                    tracing::error!(%err, "silt lookup accept failed");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    /// Serves one connection until EOF, a protocol error or a deadline.
    ///
    /// Slot accounting (D9): `jobs` (`MAX_INFLIGHT`) admits requests and is
    /// released as soon as a response is *built* — before it can reach the
    /// client — so a client that keeps at most `MAX_INFLIGHT` requests
    /// outstanding never sees `Busy`. `outputs` bounds every frame not yet
    /// on the socket (job answers and reader-originated `Busy`/`BadRequest`
    /// alike): the reader waits for one before taking a request, so a
    /// client that stops reading stalls the reader (TCP pushes back) until
    /// the write deadline drops it.
    pub async fn handle(self: Arc<Self>, mut stream: TcpStream) -> eyre::Result<()> {
        tokio::time::timeout(self.cfg.handshake_timeout, self.handshake(&mut stream))
            .await
            .map_err(|_| {
                eyre::eyre!("handshake timed out after {:?}", self.cfg.handshake_timeout)
            })??;

        let (rd, wr) = stream.into_split();
        let writer = Arc::new(FrameWriter::new(wr));
        let jobs = Arc::new(Semaphore::new(MAX_INFLIGHT));
        let outputs = Arc::new(Semaphore::new(MAX_INFLIGHT + OUTPUT_SLACK));
        let flusher = writer.clone();
        let write_timeout = self.cfg.write_timeout;
        let mut flusher = tokio::spawn(async move { flusher.run(write_timeout).await });
        let read = tokio::select! {
            read = self.read_loop(rd, &writer, &jobs, &outputs) => read,
            // The flusher only ends early on failure (e.g. its deadline):
            // the connection is dead, don't wait for the idle timeout.
            flushed = &mut flusher => {
                return match flushed {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(err)) => Err(err.into()),
                    Err(err) => Err(eyre::eyre!("flusher task: {err}")),
                };
            }
        };
        if read.is_err() {
            writer.close();
            flusher.abort();
            return read;
        }
        // Clean EOF: answers still in flight go out, then the write side
        // closes once the last job has handed its frame over.
        let _ = outputs
            .acquire_many((MAX_INFLIGHT + OUTPUT_SLACK) as u32)
            .await;
        writer.close();
        flusher
            .await
            .map_err(|e| eyre::eyre!("flusher task: {e}"))??;
        Ok(())
    }

    async fn handshake<T>(&self, stream: &mut T) -> eyre::Result<()>
    where
        T: AsyncRead + AsyncWrite + Unpin,
    {
        let ours = hello(
            self.chain_id,
            self.genesis_hash,
            self.tip.load(Ordering::Relaxed),
        );
        write_frame(stream, &ours).await?;
        let theirs = read_frame(stream)
            .await?
            .ok_or_else(|| eyre::eyre!("eof in handshake"))?;
        if let Some(reason) = hello_mismatch(&ours, &theirs) {
            write_frame(
                stream,
                &Frame::Refused {
                    reason: reason.clone(),
                },
            )
            .await?;
            eyre::bail!("refused: {reason}");
        }
        Ok(())
    }

    async fn read_loop(
        &self,
        rd: OwnedReadHalf,
        writer: &Arc<FrameWriter<OwnedSemaphorePermit>>,
        jobs: &Arc<Semaphore>,
        outputs: &Arc<Semaphore>,
    ) -> eyre::Result<()> {
        let mut rd = BufReader::with_capacity(64 * 1024, rd);
        loop {
            let mut len = [0u8; 4];
            match tokio::time::timeout(self.cfg.idle_timeout, rd.read_exact(&mut len)).await {
                Err(_) => eyre::bail!("idle for {:?}", self.cfg.idle_timeout),
                Ok(Err(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
                Ok(Err(e)) => return Err(e.into()),
                Ok(Ok(_)) => {}
            }
            // A length we can't trust means a stream we can't trust: close.
            let len = lookup::check_len(u32::from_le_bytes(len), MAX_REQUEST_BYTES)?;
            let mut body = vec![0u8; len];
            tokio::time::timeout(self.cfg.frame_timeout, rd.read_exact(&mut body))
                .await
                .map_err(|_| {
                    eyre::eyre!(
                        "frame body not received within {:?}",
                        self.cfg.frame_timeout
                    )
                })??;
            let started = Instant::now();
            // Every frame read produces exactly one output frame.
            let output = outputs.clone().acquire_owned().await?;

            let req = match RequestRef::decode(&body) {
                Ok(req) => req,
                Err(err) => {
                    let (op, flags, id) = lookup::peek_prefix(&body).expect("len >= PREFIX");
                    tracing::debug!(%err, id, "silt lookup: bad request");
                    let frame =
                        lookup::error_response(op, flags, id, Status::BadRequest, micros(started));
                    writer.send(frame, output)?;
                    continue;
                }
            };
            let (op, flags, id) = (req.op as u8, req.flags, req.id);
            let busy = || lookup::error_response(op, flags, id, Status::Busy, micros(started));
            let Ok(admitted) = jobs.clone().try_acquire_owned() else {
                writer.send(busy(), output)?;
                continue;
            };
            let job = {
                let source = self.source.clone();
                let tip = self.tip.clone();
                let writer = writer.clone();
                Box::new(move || {
                    let frame = answer(&*source, &body, started, &tip);
                    drop(admitted); // built: no longer in flight (see `handle`)
                    // Straight to the socket from this thread when it can
                    // take it (no task hop); fails only once closed.
                    let _ = writer.send(frame, output);
                }) as Job
            };
            if !self.pool.try_submit(job) {
                // Server-wide queue full: the rejected job dropped its
                // slots; take a fresh output slot for the Busy answer.
                let output = outputs.clone().acquire_owned().await?;
                writer.send(busy(), output)?;
            }
        }
    }
}

/// Output slots beyond `MAX_INFLIGHT`, for `Busy`/`BadRequest` answers.
const OUTPUT_SLACK: usize = 64;

fn micros(since: Instant) -> u32 {
    since.elapsed().as_micros().min(u32::MAX as u128) as u32
}

/// One request frame, start to finish, on a pool thread: open one view,
/// read every item in order, drop the view, encode.
fn answer<S: StateSource>(source: &S, body: &[u8], started: Instant, tip: &AtomicU64) -> Vec<u8> {
    let req = RequestRef::decode(body).expect("validated by the reader");
    let fail =
        |status| lookup::error_response(req.op as u8, req.flags, req.id, status, micros(started));
    let run = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| read_all(source, &req)));
    let out = match run {
        Ok(Ok(out)) => out,
        Ok(Err(ReadError::UnknownBlock(msg))) => {
            tracing::debug!(id = req.id, msg, "silt lookup: unknown block");
            return fail(Status::UnknownBlock);
        }
        Ok(Err(ReadError::Other(msg))) => {
            tracing::debug!(id = req.id, msg, "silt lookup: internal error");
            return fail(Status::Internal);
        }
        Err(_) => {
            tracing::error!(id = req.id, "silt lookup: read panicked");
            return fail(Status::Internal);
        }
    };
    if out.0.body_len() > MAX_RESPONSE_BYTES {
        return fail(Status::Internal);
    }
    tip.fetch_max(out.1, Ordering::Relaxed);
    out.0.finish(micros(started))
}

/// Reads all items into a response; a frame-level failure (state not
/// readable at the pin) aborts the whole frame.
fn read_all<S: StateSource>(
    source: &S,
    req: &RequestRef<'_>,
) -> Result<(ResponseBuilder, u64), ReadError> {
    let pin = (!req.is_latest()).then_some(req.pin);
    let Opened { number, hash, view } = source.open(pin)?;
    let mut out = ResponseBuilder::new(req.op, req.flags, req.id, number, &hash, req.count());
    let inline = req.inline_code();
    for item in req.items() {
        match item {
            Item::Account(addr) => match view.account(addr) {
                Ok(None) => out.push_account_absent(),
                Ok(Some(acct)) => {
                    let code = if inline && acct.code_hash != KECCAK_EMPTY {
                        match view.code(&acct.code_hash) {
                            Ok(Some(code)) => Some(code),
                            // A code hash with no code is a broken
                            // invariant: answer this key with Error.
                            Ok(None) => None,
                            Err(ReadError::UnknownBlock(m)) => {
                                return Err(ReadError::UnknownBlock(m));
                            }
                            Err(ReadError::Other(_)) => None,
                        }
                    } else {
                        Some(Bytes::new())
                    };
                    match code {
                        Some(code) => out.push_account(
                            ItemStatus::Ok,
                            acct.nonce,
                            &acct.balance,
                            &acct.code_hash,
                            &code,
                        ),
                        None => out.push_account_error(),
                    }
                }
                Err(ReadError::UnknownBlock(m)) => return Err(ReadError::UnknownBlock(m)),
                Err(ReadError::Other(_)) => out.push_account_error(),
            },
            Item::Storage(addr, slot) => match view.storage(addr, slot) {
                Ok(value) => out.push_storage(ItemStatus::Ok, &value),
                Err(ReadError::UnknownBlock(m)) => return Err(ReadError::UnknownBlock(m)),
                Err(ReadError::Other(_)) => out.push_storage(ItemStatus::Error, &[0; 32]),
            },
            Item::Code(code_hash) if *code_hash == KECCAK_EMPTY => {
                out.push_code(ItemStatus::Ok, &[]);
            }
            Item::Code(code_hash) => match view.code(code_hash) {
                Ok(Some(code)) => out.push_code(ItemStatus::Ok, &code),
                Ok(None) => out.push_code(ItemStatus::Absent, &[]),
                Err(ReadError::UnknownBlock(m)) => return Err(ReadError::UnknownBlock(m)),
                Err(ReadError::Other(_)) => out.push_code(ItemStatus::Error, &[]),
            },
        }
        if out.body_len() > MAX_RESPONSE_BYTES {
            break; // answered `Internal` by the caller
        }
    }
    // The read transaction ends here, before the response is finished.
    drop(view);
    Ok((out, number))
}

#[cfg(test)]
mod tests {
    use super::*;
    use silt_wire::lookup::{
        FLAG_INLINE_CODE, LATEST, Op, RequestBuilder, ResponseRef, ResultRef, read_body,
    };
    use std::collections::HashMap;
    use std::sync::Condvar;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpSocket;
    use tokio::task::JoinHandle;

    const CHAIN_ID: u64 = 1;
    const GENESIS: Word = [7; 32];
    const HEAD: (u64, Word) = (100, [0x10; 32]);
    const PARENT: (u64, Word) = (99, [0x09; 32]);
    /// Pin whose open fails with a non-block error.
    const BROKEN_PIN: Word = [0xDE; 32];

    const CONTRACT: Addr = [0xC0; 20];
    const EOA: Addr = [0xE0; 20];
    const MISSING: Addr = [0x00; 20];
    /// Account read fails (item `Error`).
    const FAILING: Addr = [0xFA; 20];
    /// Account read panics.
    const PANICS: Addr = [0x99; 20];
    /// Account read blocks until the gate opens.
    const SLOW: Addr = [0x51; 20];
    /// Storage key whose value depends on the block read.
    const PER_BLOCK: (Addr, Word) = ([0xAB; 20], [1; 32]);
    const CODE: &[u8] = &[0x60, 0x00, 0x60, 0x00, 0xf3];
    const CODE_HASH: Word = [0xC1; 32];
    const HUGE_HASH: Word = [0x4B; 32];

    #[derive(Default)]
    struct Gate {
        open: Mutex<bool>,
        cv: Condvar,
        entered: AtomicU64,
    }

    impl Gate {
        fn wait(&self) {
            self.entered.fetch_add(1, Ordering::SeqCst);
            let mut open = self.open.lock().unwrap();
            while !*open {
                open = self.cv.wait(open).unwrap();
            }
        }
        fn release(&self) {
            *self.open.lock().unwrap() = true;
            self.cv.notify_all();
        }
    }

    struct Data {
        storage: HashMap<(Addr, Word), Word>,
        gate: Gate,
    }

    struct MemSource(Arc<Data>);
    struct MemView(Arc<Data>, u64);

    impl StateView for MemView {
        fn account(&self, addr: &Addr) -> Result<Option<AccountInfo>, ReadError> {
            match *addr {
                CONTRACT => Ok(Some(AccountInfo {
                    nonce: 1,
                    balance: [0; 32],
                    code_hash: CODE_HASH,
                })),
                EOA | SLOW => {
                    if *addr == SLOW {
                        self.0.gate.wait();
                    }
                    let mut balance = [0; 32];
                    balance[31] = self.1 as u8; // block-dependent
                    Ok(Some(AccountInfo {
                        nonce: 5,
                        balance,
                        code_hash: KECCAK_EMPTY,
                    }))
                }
                FAILING => Err(ReadError::Other("disk on fire".into())),
                PANICS => panic!("read panicked"),
                _ => Ok(None),
            }
        }
        fn storage(&self, addr: &Addr, slot: &Word) -> Result<Word, ReadError> {
            if (*addr, *slot) == PER_BLOCK {
                return Ok([self.1 as u8; 32]);
            }
            Ok(self
                .0
                .storage
                .get(&(*addr, *slot))
                .copied()
                .unwrap_or([0; 32]))
        }
        fn code(&self, code_hash: &Word) -> Result<Option<Bytes>, ReadError> {
            Ok(match *code_hash {
                CODE_HASH => Some(Bytes::from_static(CODE)),
                HUGE_HASH => Some(Bytes::from(vec![0x5b; 300 * 1024])),
                _ => None,
            })
        }
    }

    impl StateSource for MemSource {
        type View = MemView;
        fn open(&self, pin: Option<&Word>) -> Result<Opened<MemView>, ReadError> {
            let (number, hash) = match pin {
                None => HEAD,
                Some(h) if *h == HEAD.1 => HEAD,
                Some(h) if *h == PARENT.1 => PARENT,
                Some(h) if *h == BROKEN_PIN => return Err(ReadError::Other("io".into())),
                Some(_) => return Err(ReadError::UnknownBlock("not canonical".into())),
            };
            Ok(Opened {
                number,
                hash,
                view: MemView(self.0.clone(), number),
            })
        }
    }

    fn server(cfg: LookupConfig) -> (Arc<LookupServer<MemSource>>, Arc<Data>) {
        let data = Arc::new(Data {
            storage: HashMap::from([((EOA, [2; 32]), [0x2A; 32])]),
            gate: Gate::default(),
        });
        let s = LookupServer::new(MemSource(data.clone()), CHAIN_ID, GENESIS, cfg).unwrap();
        (s, data)
    }

    fn short() -> LookupConfig {
        LookupConfig {
            workers: 4,
            handshake_timeout: Duration::from_millis(300),
            frame_timeout: Duration::from_millis(300),
            write_timeout: Duration::from_millis(300),
            idle_timeout: Duration::from_millis(300),
            ..Default::default()
        }
    }

    /// A loopback TCP pair; `rcvbuf` shrinks the client's receive buffer.
    async fn pair(rcvbuf: Option<u32>) -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let socket = TcpSocket::new_v4().unwrap();
        if let Some(size) = rcvbuf {
            socket.set_recv_buffer_size(size).unwrap();
        }
        let (client, server) = tokio::join!(
            socket.connect(listener.local_addr().unwrap()),
            listener.accept()
        );
        (client.unwrap(), server.unwrap().0)
    }

    async fn connect_with(
        s: &Arc<LookupServer<MemSource>>,
        rcvbuf: Option<u32>,
        chain_id: u64,
    ) -> (TcpStream, JoinHandle<eyre::Result<()>>) {
        let (mut client, srv) = pair(rcvbuf).await;
        let task = tokio::spawn(s.clone().handle(srv));
        let theirs = read_frame(&mut client).await.unwrap().unwrap();
        assert!(matches!(theirs, Frame::Hello { .. }), "{theirs:?}");
        write_frame(&mut client, &hello(chain_id, GENESIS, 0))
            .await
            .unwrap();
        (client, task)
    }

    async fn connect(
        s: &Arc<LookupServer<MemSource>>,
    ) -> (TcpStream, JoinHandle<eyre::Result<()>>) {
        connect_with(s, None, CHAIN_ID).await
    }

    async fn send(c: &mut TcpStream, frame: Vec<u8>) {
        c.write_all(&frame).await.unwrap();
    }

    async fn recv(c: &mut TcpStream) -> Vec<u8> {
        let mut buf = Vec::new();
        let got = tokio::time::timeout(
            Duration::from_secs(5),
            read_body(c, &mut buf, MAX_RESPONSE_BYTES),
        )
        .await
        .expect("no response within 5s")
        .unwrap();
        assert!(got, "eof");
        buf
    }

    fn storage_req(id: u32, pin: &Word, keys: &[(Addr, Word)]) -> Vec<u8> {
        let mut b = RequestBuilder::new(Op::Storage, 0, id, pin);
        for (a, s) in keys {
            b.push_storage(a, s);
        }
        b.finish()
    }

    fn account_req(id: u32, flags: u8, addrs: &[Addr]) -> Vec<u8> {
        let mut b = RequestBuilder::new(Op::Account, flags, id, &LATEST);
        for a in addrs {
            b.push_account(a);
        }
        b.finish()
    }

    async fn server_error(task: JoinHandle<eyre::Result<()>>) -> String {
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("connection task still running after 5s")
            .unwrap()
            .unwrap_err()
            .to_string()
    }

    #[tokio::test]
    async fn reads_every_op_with_d9_encodings() {
        let (s, _) = server(LookupConfig::default());
        let (mut c, _task) = connect(&s).await;

        send(
            &mut c,
            account_req(1, FLAG_INLINE_CODE, &[CONTRACT, EOA, MISSING, FAILING]),
        )
        .await;
        let body = recv(&mut c).await;
        let r = ResponseRef::decode(&body).unwrap();
        assert_eq!(
            (r.id, r.status, r.read_number, *r.read_hash),
            (1, Status::Ok, HEAD.0, HEAD.1)
        );
        let res: Vec<_> = r.results().collect();
        assert!(matches!(
            res[0],
            ResultRef::Account {
                status: ItemStatus::Ok,
                nonce: 1,
                code_hash: &CODE_HASH,
                code: Some(CODE),
                ..
            }
        ));
        // EOA: no code is KECCAK_EMPTY with empty inline code.
        assert!(matches!(
            res[1],
            ResultRef::Account {
                status: ItemStatus::Ok,
                nonce: 5,
                code_hash: &KECCAK_EMPTY,
                code: Some(&[]),
                ..
            }
        ));
        // Missing account: Absent, zeroed, KECCAK_EMPTY.
        let ResultRef::Account {
            status,
            nonce,
            balance,
            code_hash,
            code,
        } = res[2]
        else {
            panic!("{:?}", res[2]);
        };
        assert_eq!(
            (status, nonce, *balance, *code_hash, code),
            (ItemStatus::Absent, 0, [0; 32], KECCAK_EMPTY, Some(&[][..]))
        );
        // One failing key doesn't fail the frame.
        assert!(matches!(
            res[3],
            ResultRef::Account {
                status: ItemStatus::Error,
                ..
            }
        ));

        send(
            &mut c,
            storage_req(2, &LATEST, &[(EOA, [2; 32]), (EOA, [3; 32])]),
        )
        .await;
        let body = recv(&mut c).await;
        let r = ResponseRef::decode(&body).unwrap();
        let res: Vec<_> = r.results().collect();
        assert_eq!(
            res[0],
            ResultRef::Storage {
                status: ItemStatus::Ok,
                value: &[0x2A; 32]
            }
        );
        assert_eq!(
            res[1],
            ResultRef::Storage {
                status: ItemStatus::Ok,
                value: &[0; 32]
            },
            "missing slot = zero"
        );

        let mut b = RequestBuilder::new(Op::Code, 0, 3, &LATEST);
        b.push_code(&CODE_HASH)
            .push_code(&[0x77; 32])
            .push_code(&KECCAK_EMPTY);
        send(&mut c, b.finish()).await;
        let body = recv(&mut c).await;
        let res: Vec<_> = ResponseRef::decode(&body).unwrap().results().collect();
        assert_eq!(
            res[0],
            ResultRef::Code {
                status: ItemStatus::Ok,
                code: CODE
            }
        );
        assert_eq!(
            res[1],
            ResultRef::Code {
                status: ItemStatus::Absent,
                code: &[]
            }
        );
        assert_eq!(
            res[2],
            ResultRef::Code {
                status: ItemStatus::Ok,
                code: &[]
            }
        );
    }

    #[tokio::test]
    async fn pinning_ping_unknown_block_and_internal() {
        let (s, _) = server(LookupConfig::default());
        let (mut c, _task) = connect(&s).await;
        // Pinned one block back: values come from that block, and the
        // response names it.
        send(&mut c, storage_req(1, &PARENT.1, &[PER_BLOCK])).await;
        let body = recv(&mut c).await;
        let r = ResponseRef::decode(&body).unwrap();
        assert_eq!(
            (r.status, r.read_number, *r.read_hash),
            (Status::Ok, PARENT.0, PARENT.1)
        );
        assert_eq!(
            r.results().next(),
            Some(ResultRef::Storage {
                status: ItemStatus::Ok,
                value: &[PARENT.0 as u8; 32]
            })
        );
        // count = 0 is a ping: resolves the pin.
        send(&mut c, storage_req(2, &LATEST, &[])).await;
        let body = recv(&mut c).await;
        let r = ResponseRef::decode(&body).unwrap();
        assert_eq!(
            (r.status, r.count(), r.read_number, *r.read_hash),
            (Status::Ok, 0, HEAD.0, HEAD.1)
        );
        // Unknown pin: explicit status, nothing stale.
        send(&mut c, storage_req(3, &[0x33; 32], &[PER_BLOCK])).await;
        let body = recv(&mut c).await;
        let r = ResponseRef::decode(&body).unwrap();
        assert_eq!(
            (r.id, r.status, r.count(), r.read_number, *r.read_hash),
            (3, Status::UnknownBlock, 0, 0, [0; 32])
        );
        send(&mut c, storage_req(4, &BROKEN_PIN, &[PER_BLOCK])).await;
        let r = recv(&mut c).await;
        assert_eq!(ResponseRef::decode(&r).unwrap().status, Status::Internal);
    }

    #[tokio::test]
    async fn bad_request_echoes_id_and_untrusted_length_closes() {
        let (s, _) = server(LookupConfig::default());
        let (mut c, task) = connect(&s).await;
        let mut frame = storage_req(41, &LATEST, &[PER_BLOCK]);
        frame[4] = 9; // op
        send(&mut c, frame).await;
        let body = recv(&mut c).await;
        let r = ResponseRef::decode(&body).unwrap();
        assert_eq!((r.op, r.id, r.status), (9, 41, Status::BadRequest));
        // Still usable afterwards.
        send(&mut c, storage_req(42, &LATEST, &[PER_BLOCK])).await;
        assert_eq!(
            ResponseRef::decode(&recv(&mut c).await).unwrap().status,
            Status::Ok
        );
        // A length below the shared prefix can't be answered: close.
        send(&mut c, vec![4, 0, 0, 0, 1, 2, 3, 4]).await;
        let err = server_error(task).await;
        assert!(err.contains("out of bounds"), "{err}");
    }

    #[tokio::test]
    async fn pipelined_answers_return_out_of_order() {
        let (s, data) = server(LookupConfig::default());
        let (mut c, _task) = connect(&s).await;
        send(&mut c, account_req(1, 0, &[SLOW])).await;
        send(&mut c, storage_req(2, &LATEST, &[PER_BLOCK])).await;
        send(&mut c, storage_req(3, &LATEST, &[PER_BLOCK])).await;
        let first = recv(&mut c).await;
        let second = recv(&mut c).await;
        let ids = [
            ResponseRef::decode(&first).unwrap().id,
            ResponseRef::decode(&second).unwrap().id,
        ];
        assert!(
            !ids.contains(&1),
            "the slow request answered first: {ids:?}"
        );
        data.gate.release();
        let last = recv(&mut c).await;
        let r = ResponseRef::decode(&last).unwrap();
        assert_eq!((r.id, r.status), (1, Status::Ok));
    }

    #[tokio::test]
    async fn busy_beyond_the_inflight_cap() {
        let (s, data) = server(LookupConfig {
            workers: 1,
            queue: MAX_INFLIGHT,
            ..Default::default()
        });
        let (mut c, _task) = connect(&s).await;
        send(&mut c, account_req(0, 0, &[SLOW])).await; // occupies the worker
        for id in 1..MAX_INFLIGHT as u32 {
            send(&mut c, storage_req(id, &LATEST, &[PER_BLOCK])).await;
        }
        send(&mut c, storage_req(9999, &LATEST, &[PER_BLOCK])).await;
        // Answered at once, without a read, while the others wait.
        let body = recv(&mut c).await;
        let r = ResponseRef::decode(&body).unwrap();
        assert_eq!((r.id, r.status, r.count()), (9999, Status::Busy, 0));
        data.gate.release();
        let mut ok = 0;
        for _ in 0..MAX_INFLIGHT {
            let body = recv(&mut c).await;
            assert_eq!(ResponseRef::decode(&body).unwrap().status, Status::Ok);
            ok += 1;
        }
        assert_eq!(ok, MAX_INFLIGHT);
    }

    /// Regression: the in-flight slot used to be released only after the
    /// answer was flushed, so a client that refilled its window the moment
    /// an answer arrived could find all slots still taken and get `Busy`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn client_at_the_inflight_cap_never_sees_busy() {
        let (s, _) = server(LookupConfig::default());
        let (c, _task) = connect(&s).await;
        let (mut rd, mut wr) = c.into_split();
        let total = 5000u32;
        // Keep exactly MAX_INFLIGHT outstanding: one new request per answer.
        let mut sent = 0u32;
        for _ in 0..MAX_INFLIGHT {
            wr.write_all(&storage_req(sent, &LATEST, &[PER_BLOCK]))
                .await
                .unwrap();
            sent += 1;
        }
        let mut busy = 0;
        let mut buf = Vec::new();
        for _ in 0..total {
            assert!(
                read_body(&mut rd, &mut buf, MAX_RESPONSE_BYTES)
                    .await
                    .unwrap()
            );
            if ResponseRef::decode(&buf).unwrap().status == Status::Busy {
                busy += 1;
            }
            if sent < total {
                wr.write_all(&storage_req(sent, &LATEST, &[PER_BLOCK]))
                    .await
                    .unwrap();
                sent += 1;
            }
        }
        assert_eq!(busy, 0);
    }

    #[tokio::test]
    async fn busy_when_the_server_queue_is_full() {
        let (s, data) = server(LookupConfig {
            workers: 1,
            queue: 1,
            ..Default::default()
        });
        let (mut c, _task) = connect(&s).await;
        send(&mut c, account_req(1, 0, &[SLOW])).await;
        while data.gate.entered.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        send(&mut c, storage_req(2, &LATEST, &[PER_BLOCK])).await; // queued
        send(&mut c, storage_req(3, &LATEST, &[PER_BLOCK])).await; // no room
        let r = recv(&mut c).await;
        assert_eq!(
            (
                ResponseRef::decode(&r).unwrap().id,
                ResponseRef::decode(&r).unwrap().status
            ),
            (3, Status::Busy)
        );
        data.gate.release();
        let mut ids: Vec<u32> = Vec::new();
        for _ in 0..2 {
            ids.push(ResponseRef::decode(&recv(&mut c).await).unwrap().id);
        }
        ids.sort();
        assert_eq!(ids, [1, 2]);
    }

    #[tokio::test]
    async fn panicking_read_answers_internal_and_pool_survives() {
        let (s, _) = server(LookupConfig {
            workers: 1,
            ..Default::default()
        });
        let (mut c, _task) = connect(&s).await;
        send(&mut c, account_req(1, 0, &[PANICS])).await;
        let r = recv(&mut c).await;
        assert_eq!(ResponseRef::decode(&r).unwrap().status, Status::Internal);
        send(&mut c, account_req(2, 0, &[EOA])).await;
        let r = recv(&mut c).await;
        assert_eq!(ResponseRef::decode(&r).unwrap().status, Status::Ok);
    }

    #[tokio::test]
    async fn oversized_response_answers_internal() {
        let (s, _) = server(LookupConfig::default());
        let (mut c, _task) = connect(&s).await;
        let mut b = RequestBuilder::new(Op::Code, 0, 7, &LATEST);
        for _ in 0..64 {
            b.push_code(&HUGE_HASH); // 64 × 300 KiB > 16 MiB
        }
        send(&mut c, b.finish()).await;
        let r = recv(&mut c).await;
        assert_eq!(ResponseRef::decode(&r).unwrap().status, Status::Internal);
    }

    #[tokio::test]
    async fn handshake_refuses_wrong_chain() {
        let (s, _) = server(short());
        let (mut c, srv) = pair(None).await;
        let task = tokio::spawn(s.clone().handle(srv));
        read_frame(&mut c).await.unwrap().unwrap();
        write_frame(&mut c, &hello(2, GENESIS, 0)).await.unwrap();
        let refused = read_frame(&mut c).await.unwrap().unwrap();
        assert!(matches!(refused, Frame::Refused { .. }), "{refused:?}");
        let err = server_error(task).await;
        assert!(err.contains("chain id mismatch"), "{err}");
    }

    #[tokio::test]
    async fn idle_torn_and_undrained_connections_are_dropped() {
        let (s, _) = server(short());
        // Silent after the handshake.
        let (_c, task) = connect_with(&s, None, CHAIN_ID).await;
        assert!(server_error(task).await.contains("idle"));
        // A length, then half a body.
        let (mut c, task) = connect_with(&s, None, CHAIN_ID).await;
        let frame = storage_req(1, &LATEST, &[PER_BLOCK]);
        send(&mut c, frame[..10].to_vec()).await;
        assert!(server_error(task).await.contains("frame body"));
        // Never reads its answers: the write deadline drops it (idle
        // deadline pushed out so only the writer can fire).
        let (s, _) = server(LookupConfig {
            idle_timeout: Duration::from_secs(30),
            ..short()
        });
        let (mut c, task) = connect_with(&s, Some(4096), CHAIN_ID).await;
        for id in 0..48 {
            // 48 × 300 KiB answers: far past loopback socket buffers.
            let mut b = RequestBuilder::new(Op::Code, 0, id, &LATEST);
            b.push_code(&HUGE_HASH);
            send(&mut c, b.finish()).await;
        }
        let err = server_error(task).await;
        assert!(err.contains("timed out"), "{err}");
        drop(c);
    }
}
