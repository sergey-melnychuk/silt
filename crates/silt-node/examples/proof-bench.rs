//! Proof path benchmark and check (PLAN D8, D10): for realistic target
//! sets, what a proof costs as reth's trie walk alone, over `silt_multiproof`
//! (JSON-RPC) and over the binary lookup op `Proof = 4`; and whether op 4
//! answers exactly what reth's multiproof says.
//!
//! Opens a live datadir read-only (`common/ro.rs`) and, per request,
//! re-pins to the **current** state frontier (untimed) — a builder proves
//! at the tip; a fixed pin falls behind a live node and every walk then
//! rebuilds a revert overlay that grows with the distance (minutes after a
//! couple of hours) — and measures:
//! - **walk**: open a state provider + `multiproof`, in-process — what any
//!   transport pays;
//! - **build + json**: the RPC's own `MultiproofResponse::from_multiproof`
//!   and its `serde_json` encoding;
//! - **rpc**: the real `silt_multiproof` module served by jsonrpsee over
//!   HTTP on loopback (reth's default size limits): request encode, round
//!   trip, typed client-side parse;
//! - **op 4**: the real lookup server (`LookupServer` on the same read-only
//!   source, default config) over loopback TCP after the D9 handshake:
//!   request encode, round trip (`server_us` separates the server's share),
//!   and a zero-copy decode that touches every node.
//!
//! Checks (any failure exits 1): keccak(root node) equals the pinned
//! header's state root, in-process and over RPC; every op-4 answer is
//! **byte-identical** (header, every node, every storage trie) to
//! `write_multiproof` of the in-process walk at the same pin.
//!
//! Workloads (pre-hashed targets from the last `--blocks` blocks'
//! changesets, each proved at the frontier current at the time):
//! - `block`: every account and slot one block changed — the key set a
//!   state-root computation for a block of that shape needs;
//! - `acct-1`, `slot-1`, `acct-64`: one account, one account + one slot,
//!   64 accounts — the shape of fetches for a sparse trie kept warm by the
//!   feed.
//!
//! Then a **depth sweep** (`--depths`, in-process walks only): the same
//! block-sized and 64-account requests pinned `d` blocks below the frontier,
//! to size the server's `proof_max_depth`.
//!
//! Servers and client run in one process on separate runtimes (servers on
//! 4 tokio workers plus the lookup server's own threads, the client on the
//! main thread).
//!
//! Usage:
//!   proof-bench --datadir <path> [--blocks 40] [--small 300]
//!               [--depths 0,1,4,16,32,64] [--rpc-bind 127.0.0.1:8588]
//!               [--lookup-bind 127.0.0.1:8589] [--scratch <dir>]

#[path = "common/ro.rs"]
mod ro;

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use alloy_primitives::{B256, Bytes, keccak256};
use reth_ethereum::chainspec::{EthChainSpec, MAINNET};
use reth_ethereum::provider::{
    BlockHashReader, BlockNumReader, ChangeSetReader, HeaderProvider, StateProofProvider,
    StorageChangeSetReader,
};
use reth_ethereum::trie::{MultiProof, MultiProofTargets};
use serde::Deserialize;
use silt_node::lookup::{LookupConfig, LookupServer, write_multiproof};
use silt_node::rpc::MultiproofResponse;
use silt_wire::lookup::{
    MAX_RESPONSE_BYTES, Op, ProofBuilder, RESPONSE_HEADER, RequestBuilder, ResponseRef, Status,
    read_body,
};
use silt_wire::{Word, hello, read_frame, write_frame};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

type Targets = Vec<(B256, Vec<B256>)>;

struct Args {
    datadir: PathBuf,
    blocks: u64,
    small: usize,
    depths: Vec<u64>,
    rpc_bind: String,
    lookup_bind: String,
    scratch: PathBuf,
}

fn args() -> eyre::Result<Args> {
    let mut a = Args {
        datadir: PathBuf::new(),
        blocks: 40,
        small: 300,
        depths: vec![0, 1, 4, 16, 32, 64],
        rpc_bind: "127.0.0.1:8588".into(),
        lookup_bind: "127.0.0.1:8589".into(),
        scratch: std::env::temp_dir().join(format!("silt-proof-bench-{}", std::process::id())),
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut value = || it.next().ok_or_else(|| eyre::eyre!("{flag} needs a value"));
        match flag.as_str() {
            "--datadir" => a.datadir = value()?.into(),
            "--blocks" => a.blocks = value()?.parse()?,
            "--small" => a.small = value()?.parse()?,
            "--depths" => {
                let v = value()?;
                a.depths = if v.is_empty() {
                    vec![]
                } else {
                    v.split(',').map(str::parse).collect::<Result<_, _>>()?
                };
            }
            "--rpc-bind" => a.rpc_bind = value()?,
            "--lookup-bind" => a.lookup_bind = value()?,
            "--scratch" => a.scratch = value()?.into(),
            other => eyre::bail!("unknown flag {other}"),
        }
    }
    eyre::ensure!(!a.datadir.as_os_str().is_empty(), "--datadir is required");
    eyre::ensure!(
        a.blocks > 0 && a.small > 0,
        "--blocks and --small must be > 0"
    );
    Ok(a)
}

fn main() -> eyre::Result<()> {
    let args = args()?;
    let servers = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()?;
    let client = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let rocksdb = ro::prepare_scratch(&args.scratch, &args.datadir)?;
    let result = run(&args, rocksdb, &servers, &client);
    drop(client);
    servers.shutdown_timeout(Duration::from_secs(2));
    ro::remove_scratch(&args.scratch, "proof-bench");
    result
}

/// What one request costs, end to end, per path.
#[derive(Default)]
struct Sample {
    keys: usize,
    nodes: usize,
    json: usize,
    op4: usize,
    walk: f64,
    build_json: f64,
    rpc_enc: f64,
    rpc_rtt: f64,
    rpc_parse: f64,
    op4_enc: f64,
    op4_rtt: f64,
    op4_server: f64,
    op4_decode: f64,
}

impl Sample {
    fn rpc_total(&self) -> f64 {
        self.rpc_enc + self.rpc_rtt + self.rpc_parse
    }
    fn op4_total(&self) -> f64 {
        self.op4_enc + self.op4_rtt + self.op4_decode
    }
}

struct Ctx<'a> {
    ro: &'a ro::ReadOnly,
    /// Current frontier block hash and its state root (see `repin`).
    pin: B256,
    state_root: B256,
    frontier: u64,
    repins: usize,
    rpc: Http,
    lookup: Wire,
    client: &'a tokio::runtime::Runtime,
    root_failures: usize,
    op4_mismatches: usize,
}

fn run(
    args: &Args,
    rocksdb: PathBuf,
    servers: &tokio::runtime::Runtime,
    client: &tokio::runtime::Runtime,
) -> eyre::Result<()> {
    let ro = {
        let _enter = servers.enter();
        ro::open(&args.datadir, rocksdb)?
    };
    let p = ro.factory.provider()?;
    let frontier = ro::state_frontier(&p).map_err(|e| eyre::eyre!("{e}"))?;
    let persisted = p.best_block_number()?;
    let first = frontier + 1 - args.blocks.min(frontier);
    let mut block_reqs = Vec::new();
    for n in first..=frontier {
        block_reqs.push(block_targets(&p, n)?);
    }
    drop(p);

    let rpc_handle = servers.block_on(async {
        // reth's RPC defaults: --rpc.max-request-size 15 MB,
        // --rpc.max-response-size 160 MB.
        let cfg = jsonrpsee::server::ServerConfig::builder()
            .max_request_body_size(15 * 1024 * 1024)
            .max_response_body_size(160 * 1024 * 1024)
            .build();
        let server = jsonrpsee::server::Server::builder()
            .set_config(cfg)
            .build(&args.rpc_bind)
            .await?;
        Ok::<_, eyre::Report>(server.start(silt_node::rpc_module(ro.chain.clone())))
    })?;
    let lookup_listener = servers.block_on(TcpListener::bind(&args.lookup_bind))?;
    let lookup = LookupServer::new(
        ro.clone(),
        MAINNET.chain().id(),
        MAINNET.genesis_hash().0,
        LookupConfig::default(),
    )?;
    servers.spawn(lookup.serve(lookup_listener));

    let (rpc, lookup) = client.block_on(async {
        Ok::<_, eyre::Report>((
            Http::connect(&args.rpc_bind).await?,
            Wire::connect(&args.lookup_bind).await?,
        ))
    })?;
    println!(
        "proof-bench: {} frontier {frontier}, persisted {persisted}; \
         block workload from blocks {first}..={frontier}; proofs at the current frontier",
        args.datadir.display()
    );

    let mut ctx = Ctx {
        ro: &ro,
        pin: B256::ZERO,
        state_root: B256::ZERO,
        frontier: 0,
        repins: 0,
        rpc,
        lookup,
        client,
        root_failures: 0,
        op4_mismatches: 0,
    };
    let workloads = small_workloads(&block_reqs, args.small);
    let acct64: Vec<Targets> = workloads
        .iter()
        .find(|(name, _)| *name == "acct-64")
        .map(|(_, reqs)| reqs.clone())
        .unwrap_or_default();
    let mut rows = Vec::new();
    let started = Instant::now();
    for (name, reqs) in std::iter::once(("block", block_reqs.clone())).chain(workloads) {
        // First pass: in-process walk only (first touch for this process;
        // the node itself may still have these pages in the OS cache).
        let mut first_walk = Vec::with_capacity(reqs.len());
        for t in &reqs {
            ctx.repin()?;
            let (mp, walk) = walk_at(ctx.ro, ctx.pin, t)?;
            ctx.root_failures += !root_ok(&mp, ctx.state_root) as usize;
            first_walk.push(walk);
        }
        // Fresh connections per measured loop: hyper (jsonrpsee) closes a
        // connection idle for 30 s, e.g. across the first pass above.
        ctx.client.block_on(ctx.rpc.reconnect())?;
        let mut samples = Vec::with_capacity(reqs.len());
        for t in &reqs {
            ctx.repin()?;
            samples.push(measure(&mut ctx, t)?);
        }
        let worst = samples.iter().map(|s| s.walk).fold(0.0, f64::max);
        eprintln!(
            "proof-bench: {name}: {} requests done at {:.0} s (frontier {}, slowest walk {:.0} ms)",
            reqs.len(),
            started.elapsed().as_secs_f64(),
            ctx.frontier,
            worst / 1e3
        );
        rows.push((name, reqs.len(), first_walk, samples));
    }
    rpc_handle.stop()?;
    let sweep = depth_sweep(&mut ctx, &block_reqs, &acct64, &args.depths)?;

    report(&rows);
    report_depths(&sweep);
    println!(
        "rpc reconnects inside measured loops: {}; frontier moved {} times during the run",
        ctx.rpc.reconnects, ctx.repins
    );
    println!(
        "root checks (keccak(root node) == state root, in-process and over RPC): {} failures",
        ctx.root_failures
    );
    println!(
        "op 4 vs in-process multiproof (byte-identical answers): {} mismatches",
        ctx.op4_mismatches
    );
    eyre::ensure!(ctx.root_failures == 0, "proof root mismatch");
    eyre::ensure!(ctx.op4_mismatches == 0, "op 4 answer differs from reth");
    Ok(())
}

impl Ctx<'_> {
    /// Pins the current state frontier (untimed): one read transaction for
    /// the frontier, its hash and its header's state root.
    fn repin(&mut self) -> eyre::Result<()> {
        let p = self.ro.factory.provider()?;
        let frontier = ro::state_frontier(&p).map_err(|e| eyre::eyre!("{e}"))?;
        if frontier != self.frontier {
            self.pin = p
                .block_hash(frontier)?
                .ok_or_else(|| eyre::eyre!("no hash for frontier {frontier}"))?;
            self.state_root = p
                .header(self.pin)?
                .ok_or_else(|| eyre::eyre!("no header for {}", self.pin))?
                .state_root;
            self.repins += (self.frontier != 0) as usize;
            self.frontier = frontier;
        }
        Ok(())
    }
}

/// A walk this slow means the pin fell behind the node (see module docs);
/// stop rather than hold read transactions on a live database.
const MAX_WALK: Duration = Duration::from_secs(30);

/// Hashed targets for everything block `n` changed.
fn block_targets(
    p: &(impl ChangeSetReader + StorageChangeSetReader),
    n: u64,
) -> eyre::Result<Targets> {
    let mut m: BTreeMap<B256, BTreeSet<B256>> = BTreeMap::new();
    for a in p.account_block_changeset(n)? {
        m.entry(keccak256(a.address)).or_default();
    }
    for (bna, entry) in p.storage_changeset(n)? {
        m.entry(keccak256(bna.address()))
            .or_default()
            .insert(keccak256(entry.key));
    }
    Ok(m.into_iter()
        .map(|(a, s)| (a, s.into_iter().collect()))
        .collect())
}

fn small_workloads(blocks: &[Targets], n: usize) -> Vec<(&'static str, Vec<Targets>)> {
    let accounts: Vec<B256> = blocks
        .iter()
        .flatten()
        .map(|(a, _)| *a)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let slots: Vec<(B256, B256)> = blocks
        .iter()
        .flatten()
        .flat_map(|(a, s)| s.iter().map(move |s| (*a, *s)))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let mut rng = Rng(0x5157_7072_6f6f_6621);
    let mut pick = |len: usize| rng.below(len);
    let acct1 = (0..n)
        .map(|_| vec![(accounts[pick(accounts.len())], vec![])])
        .collect();
    let slot1 = (0..n)
        .map(|_| {
            let (a, s) = slots[pick(slots.len())];
            vec![(a, vec![s])]
        })
        .collect();
    let acct64 = (0..(n / 10).max(1))
        .map(|_| {
            let set: BTreeSet<B256> = (0..64).map(|_| accounts[pick(accounts.len())]).collect();
            set.into_iter().map(|a| (a, vec![])).collect()
        })
        .collect();
    vec![("acct-1", acct1), ("slot-1", slot1), ("acct-64", acct64)]
}

/// Open + multiproof in-process at `pin`: exactly the servers' blocking
/// section, on the harness's read-only provider.
fn walk_at(ro: &ro::ReadOnly, pin: B256, targets: &Targets) -> eyre::Result<(MultiProof, f64)> {
    let start = Instant::now();
    let provider = ro.factory.provider()?;
    let state = ro.chain.state_provider_from_database(provider, pin);
    let mut wanted = MultiProofTargets::default();
    for (a, s) in targets {
        wanted.entry(*a).or_default().extend(s.iter().copied());
    }
    let mp = state.multiproof(Default::default(), wanted)?;
    eyre::ensure!(
        start.elapsed() < MAX_WALK,
        "walk took {:?} at {pin}: aborting",
        start.elapsed()
    );
    Ok((mp, us(start)))
}

fn measure(ctx: &mut Ctx<'_>, targets: &Targets) -> eyre::Result<Sample> {
    let mut s = Sample {
        keys: targets.iter().map(|(_, s)| 1 + s.len()).sum(),
        ..Default::default()
    };

    let (mp, walk) = walk_at(ctx.ro, ctx.pin, targets)?;
    s.walk = walk;
    ctx.root_failures += !root_ok(&mp, ctx.state_root) as usize;
    s.nodes = mp.account_subtree.iter().count()
        + mp.storages
            .values()
            .map(|sp| sp.subtree.iter().count())
            .sum::<usize>();
    // What op 4 must answer (untimed): reth's walk in D10 order.
    let mut expect = ProofBuilder::new(0, 0, ctx.frontier, &ctx.pin.0, 0);
    write_multiproof(&mp, &mut expect);
    let expect = expect.finish(0);

    let start = Instant::now();
    let json = serde_json::to_vec(&MultiproofResponse::from_multiproof(mp))?;
    s.build_json = us(start);
    s.json = json.len();

    // JSON-RPC: silt_multiproof.
    let start = Instant::now();
    let body = serde_json::to_vec(&serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "silt_multiproof",
        "params": [targets, ctx.pin],
    }))?;
    s.rpc_enc = us(start);
    let start = Instant::now();
    let reply = ctx.client.block_on(ctx.rpc.call(&body))?;
    s.rpc_rtt = us(start);
    let start = Instant::now();
    let reply: RpcReply = serde_json::from_slice(&reply)?;
    let proof = match (reply.result, reply.error) {
        (Some(p), _) => p,
        (None, err) => eyre::bail!("silt_multiproof failed: {err:?}"),
    };
    s.rpc_parse = us(start);
    let rpc_root = proof.account_subtree.get("").map(keccak256);
    ctx.root_failures += (rpc_root != Some(ctx.state_root)) as usize;

    // Op 4 on the lookup listener.
    let start = Instant::now();
    let mut b = RequestBuilder::with_capacity(Op::Proof, 0, 1, &ctx.pin.0, targets.len());
    for (addr, slots) in targets {
        let slots: Vec<Word> = slots.iter().map(|s| s.0).collect();
        b.push_proof_target(&addr.0, &slots);
    }
    let frame = b.finish();
    s.op4_enc = us(start);
    let start = Instant::now();
    let answer = ctx.client.block_on(ctx.lookup.call(&frame))?;
    s.op4_rtt = us(start);
    let start = Instant::now();
    let r = ResponseRef::decode(&answer)?;
    eyre::ensure!(r.status == Status::Ok, "op 4 answered {:?}", r.status);
    let p = r.proof().expect("Ok proof");
    let mut touched = 0usize;
    for n in p.account_nodes() {
        touched += n.rlp.len() + n.path_len as usize;
    }
    for st in p.storages() {
        touched += st.storage_root[0] as usize;
        for n in st.nodes {
            touched += n.rlp.len() + n.path_len as usize;
        }
    }
    s.op4_decode = us(start);
    s.op4_server = r.server_us as f64;
    s.op4 = answer.len();
    std::hint::black_box(touched);
    // Identical but for the id and server_us (header bytes 4..8, 12..16).
    let same = answer.len() == expect.len() - 4
        && answer[..4] == expect[4..8]
        && answer[8..12] == expect[12..16]
        && answer[16..] == expect[20..];
    if !same {
        eprintln!(
            "op 4 mismatch at {}: {} vs {} bytes ({} vs {} storage tries)",
            ctx.frontier,
            answer.len(),
            expect.len() - 4,
            r.count(),
            u16::from_le_bytes([expect[6], expect[7]])
        );
    }
    ctx.op4_mismatches += !same as usize;
    debug_assert!(answer.len() >= RESPONSE_HEADER);
    Ok(s)
}

fn root_ok(mp: &MultiProof, state_root: B256) -> bool {
    mp.account_subtree
        .iter()
        .find(|(path, _)| path.is_empty())
        .is_some_and(|(_, rlp)| keccak256(rlp) == state_root)
}

/// Per depth: (depth, block-sized walks µs, 64-account walks µs).
type DepthRow = (u64, Vec<f64>, Vec<f64>);

/// In-process walks pinned `d` blocks below the current frontier: the first
/// eight block-sized and 64-account requests per depth.
fn depth_sweep(
    ctx: &mut Ctx<'_>,
    blocks: &[Targets],
    acct64: &[Targets],
    depths: &[u64],
) -> eyre::Result<Vec<DepthRow>> {
    let mut rows = Vec::new();
    for &d in depths {
        ctx.repin()?;
        let number = ctx.frontier.saturating_sub(d);
        let pin = ctx
            .ro
            .factory
            .provider()?
            .block_hash(number)?
            .ok_or_else(|| eyre::eyre!("no hash for {number}"))?;
        let started = Instant::now();
        let walks = |reqs: &[Targets]| -> eyre::Result<Vec<f64>> {
            reqs.iter()
                .take(8)
                .map(|t| walk_at(ctx.ro, pin, t).map(|(_, us)| us))
                .collect()
        };
        let (b, a) = (walks(blocks)?, walks(acct64)?);
        eprintln!(
            "proof-bench: depth {d}: done in {:.1} s",
            started.elapsed().as_secs_f64()
        );
        rows.push((d, b, a));
    }
    Ok(rows)
}

/// Typed client view of a `silt_multiproof` reply: hex decoded into bytes,
/// as a consumer would.
#[derive(Deserialize)]
struct RpcReply {
    result: Option<WireProof>,
    error: Option<serde_json::Value>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireProof {
    account_subtree: BTreeMap<String, Bytes>,
    #[allow(dead_code)]
    storages: BTreeMap<B256, WireStorage>,
}

#[derive(Deserialize)]
#[allow(dead_code)]
struct WireStorage {
    root: B256,
    subtree: BTreeMap<String, Bytes>,
}

/// One lookup connection after the D9 handshake; one request in flight.
struct Wire {
    stream: TcpStream,
    buf: Vec<u8>,
}

impl Wire {
    async fn connect(addr: &str) -> eyre::Result<Self> {
        let mut stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        read_frame(&mut stream)
            .await?
            .ok_or_else(|| eyre::eyre!("lookup server closed during the handshake"))?;
        write_frame(
            &mut stream,
            &hello(MAINNET.chain().id(), MAINNET.genesis_hash().0, 0),
        )
        .await?;
        Ok(Self {
            stream,
            buf: Vec::with_capacity(1 << 20),
        })
    }

    /// Sends one frame; returns the response body (a copy: the decode is
    /// timed separately).
    async fn call(&mut self, frame: &[u8]) -> eyre::Result<Vec<u8>> {
        self.stream.write_all(frame).await?;
        eyre::ensure!(
            read_body(&mut self.stream, &mut self.buf, MAX_RESPONSE_BYTES).await?,
            "lookup server closed the connection"
        );
        Ok(std::mem::take(&mut self.buf))
    }
}

/// Minimal keep-alive HTTP/1.1 JSON client: one request in flight, no
/// extra dependencies (jsonrpsee answers with Content-Length).
struct Http {
    host: String,
    stream: TcpStream,
    buf: Vec<u8>,
    /// Retries after the server closed the connection before answering.
    reconnects: usize,
}

impl Http {
    async fn connect(addr: &str) -> eyre::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        Ok(Self {
            host: addr.to_string(),
            stream,
            buf: Vec::with_capacity(1 << 20),
            reconnects: 0,
        })
    }

    async fn reconnect(&mut self) -> eyre::Result<()> {
        self.stream = TcpStream::connect(&self.host).await?;
        self.stream.set_nodelay(true)?;
        Ok(())
    }

    /// One request; if the server closed the connection before answering,
    /// reconnects and retries once (counted in `reconnects`).
    async fn call(&mut self, body: &[u8]) -> eyre::Result<Vec<u8>> {
        match self.try_call(body).await? {
            Some(reply) => Ok(reply),
            None => {
                self.reconnects += 1;
                self.reconnect().await?;
                self.try_call(body)
                    .await?
                    .ok_or_else(|| eyre::eyre!("rpc closed the connection twice"))
            }
        }
    }

    /// `None`: closed before any reply byte.
    async fn try_call(&mut self, body: &[u8]) -> eyre::Result<Option<Vec<u8>>> {
        let mut req = format!(
            "POST / HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
            self.host,
            body.len()
        )
        .into_bytes();
        req.extend_from_slice(body);
        if self.stream.write_all(&req).await.is_err() {
            return Ok(None);
        }

        self.buf.clear();
        let mut chunk = vec![0u8; 64 * 1024];
        let header_end = loop {
            let n = match self.stream.read(&mut chunk).await {
                Ok(0) | Err(_) if self.buf.is_empty() => return Ok(None),
                Ok(n) => n,
                Err(err) => return Err(err.into()),
            };
            eyre::ensure!(n > 0, "rpc connection closed mid-reply");
            self.buf.extend_from_slice(&chunk[..n]);
            if let Some(i) = self.buf.windows(4).position(|w| w == b"\r\n\r\n") {
                break i + 4;
            }
        };
        let head = std::str::from_utf8(&self.buf[..header_end])?;
        eyre::ensure!(
            head.starts_with("HTTP/1.1 200"),
            "rpc: {}",
            head.lines().next().unwrap_or("")
        );
        let len: usize = head
            .lines()
            .find_map(|l| {
                let (k, v) = l.split_once(':')?;
                k.eq_ignore_ascii_case("content-length")
                    .then(|| v.trim().parse().ok())?
            })
            .ok_or_else(|| eyre::eyre!("rpc reply without Content-Length"))?;
        while self.buf.len() < header_end + len {
            let n = self.stream.read(&mut chunk).await?;
            eyre::ensure!(n > 0, "rpc connection closed");
            self.buf.extend_from_slice(&chunk[..n]);
        }
        Ok(Some(self.buf[header_end..header_end + len].to_vec()))
    }
}

fn report(rows: &[(&str, usize, Vec<f64>, Vec<Sample>)]) {
    println!();
    println!(
        "| workload | reqs | keys/req | nodes/req | op 4 KiB | JSON KiB | walk µs (first pass) p50/p90/max | walk µs p50/p90/max | build+json µs p50 |"
    );
    println!("|---|---:|---:|---:|---:|---:|---|---|---:|");
    for (name, n, first, samples) in rows {
        let avg =
            |f: &dyn Fn(&Sample) -> f64| samples.iter().map(f).sum::<f64>() / samples.len() as f64;
        let col = |f: &dyn Fn(&Sample) -> f64| samples.iter().map(f).collect::<Vec<_>>();
        println!(
            "| {name} | {n} | {:.0} | {:.0} | {:.1} | {:.1} | {} | {} | {:.0} |",
            avg(&|s| s.keys as f64),
            avg(&|s| s.nodes as f64),
            avg(&|s| s.op4 as f64) / 1024.0,
            avg(&|s| s.json as f64) / 1024.0,
            p3(first),
            p3(&col(&|s| s.walk)),
            pct(&col(&|s| s.build_json), 0.5),
        );
    }
    println!();
    println!(
        "| workload | JSON-RPC µs p50/p90/max (enc / rtt / parse p50) | op 4 µs p50/p90/max (enc / rtt / decode p50) | op 4 server_us p50 | op 4 transport µs p50 | op 4 − walk µs p50 | saved vs JSON-RPC p50 |"
    );
    println!("|---|---|---|---:|---:|---:|---|");
    for (name, _, _, samples) in rows {
        let col = |f: &dyn Fn(&Sample) -> f64| samples.iter().map(f).collect::<Vec<_>>();
        let rpc = pct(&col(&|s| s.rpc_total()), 0.5);
        let op4 = pct(&col(&|s| s.op4_total()), 0.5);
        println!(
            "| {name} | {} ({:.0} / {:.0} / {:.0}) | {} ({:.0} / {:.0} / {:.0}) | {:.0} | {:.0} | {:.0} | {:.0} µs ({:.0}%) |",
            p3(&col(&|s| s.rpc_total())),
            pct(&col(&|s| s.rpc_enc), 0.5),
            pct(&col(&|s| s.rpc_rtt), 0.5),
            pct(&col(&|s| s.rpc_parse), 0.5),
            p3(&col(&|s| s.op4_total())),
            pct(&col(&|s| s.op4_enc), 0.5),
            pct(&col(&|s| s.op4_rtt), 0.5),
            pct(&col(&|s| s.op4_decode), 0.5),
            pct(&col(&|s| s.op4_server), 0.5),
            pct(&col(&|s| s.op4_rtt - s.op4_server), 0.5),
            pct(&col(&|s| s.op4_total() - s.walk), 0.5),
            rpc - op4,
            (rpc - op4) / rpc * 100.0,
        );
    }
}

fn report_depths(rows: &[DepthRow]) {
    if rows.is_empty() {
        return;
    }
    println!();
    println!(
        "| pin depth (blocks below frontier) | block-sized walk µs first / p50 / max | 64-account walk µs first / p50 / max |"
    );
    println!("|---:|---|---|");
    let fmt = |v: &[f64]| match v.first() {
        Some(first) => format!("{first:.0} / {:.0} / {:.0}", pct(v, 0.5), pct(v, 1.0)),
        None => "-".into(),
    };
    for (d, b, a) in rows {
        println!("| {d} | {} | {} |", fmt(b), fmt(a));
    }
}

fn p3(v: &[f64]) -> String {
    format!(
        "{:.0} / {:.0} / {:.0}",
        pct(v, 0.5),
        pct(v, 0.9),
        pct(v, 1.0)
    )
}

fn pct(v: &[f64], q: f64) -> f64 {
    let mut v = v.to_vec();
    v.sort_by(f64::total_cmp);
    let i = ((v.len() as f64 - 1.0) * q).round() as usize;
    v[i.min(v.len() - 1)]
}

fn us(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1e6
}

/// xorshift64*: deterministic key sampling.
struct Rng(u64);

impl Rng {
    fn below(&mut self, n: usize) -> usize {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        (self.0.wrapping_mul(0x2545_F491_4F6C_DD1D) % n as u64) as usize
    }
}
