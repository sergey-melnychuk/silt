//! In-node proof benchmark and check (PLAN D10, M5): op `Proof = 4` on a
//! running node's lookup listener next to `silt_multiproof` on its RPC, for
//! target sets taken from the node's own feed. Reth-free client.
//!
//! The read-only `proof-bench` (silt-node example) measures the same paths
//! against a datadir opened next to the node. This one measures what a
//! client of the running node sees: tip reads go through reth's in-memory
//! overlay, and proofs run on the node's proof workers, next to block
//! import.
//!
//! Target sets come from the feed's spool: the newest `--blocks`
//! single-block diffs, each giving every account and slot that block
//! changed (hashed) — `block`; and from the union of their keys, disjoint
//! slices: `acct-1`, `slot-1` (one account + one of its changed slots),
//! `acct-64`. Each is proved at the node's head.
//!
//! Per request, in this order: op 4 at `LATEST` (first touch), then
//! `silt_multiproof` pinned to the block op 4 answered at, then op 4 again
//! at that block (warm). Measured: round trip (client clock), `server_us`
//! and a zero-copy decode that touches every node (op 4); request encode,
//! round trip and typed parse (JSON-RPC).
//!
//! Checks (any failure exits 1): keccak(op-4 root node) equals the answered
//! block's `stateRoot`; the JSON-RPC answer at the same block has exactly
//! the same account nodes, storage roots and storage nodes as op 4.
//!
//! Then a depth sweep: the first `--sweep` block-sized and 64-account
//! requests pinned `d` blocks below the node's head (`--depths`, within the
//! server's `proof_max_depth`, default 8).
//!
//! Usage:
//!   node-proof-bench [--feed 127.0.0.1:8585] [--lookup 127.0.0.1:8586]
//!                    [--rpc http://127.0.0.1:8545] [--blocks 20]
//!                    [--small 200] [--depths 0,1,2,4,8] [--sweep 8]

#[path = "common/keys.rs"]
mod keys;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::{Duration, Instant};

use silt_cache::fallback::{hex_word, parse_bytes, parse_u64, parse_word};
use silt_cache::lookup::{LookupClient, LookupClientConfig, LookupError, ProofFrame};
use silt_wire::lookup::{LATEST, MAX_ITEMS, MAX_PROOF_SLOTS, Status};
use silt_wire::{Addr, BlockDiff, Frame, Word, read_frame, write_frame};

type Targets = Vec<(Word, Vec<Word>)>;

struct Args {
    feed: String,
    lookup: String,
    rpc: String,
    blocks: usize,
    small: usize,
    depths: Vec<u64>,
    sweep: usize,
}

fn args() -> eyre::Result<Args> {
    let mut a = Args {
        feed: "127.0.0.1:8585".into(),
        lookup: "127.0.0.1:8586".into(),
        rpc: "http://127.0.0.1:8545".into(),
        blocks: 20,
        small: 200,
        depths: vec![0, 1, 2, 4, 8],
        sweep: 8,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut v = || it.next().ok_or_else(|| eyre::eyre!("{flag} needs a value"));
        match flag.as_str() {
            "--feed" => a.feed = v()?,
            "--lookup" => a.lookup = v()?,
            "--rpc" => a.rpc = v()?,
            "--blocks" => a.blocks = v()?.parse()?,
            "--small" => a.small = v()?.parse()?,
            "--depths" => {
                a.depths = v()?
                    .split(',')
                    .map(|d| Ok(d.trim().parse()?))
                    .collect::<eyre::Result<_>>()?
            }
            "--sweep" => a.sweep = v()?.parse()?,
            other => eyre::bail!("unknown flag {other}"),
        }
    }
    Ok(a)
}

/// The newest `n` single-block diffs from the feed's spool.
async fn recent_diffs(feed: &str, n: usize) -> eyre::Result<Vec<BlockDiff>> {
    let mut stream = tokio::net::TcpStream::connect(feed).await?;
    let Some(Frame::Hello {
        chain_id,
        genesis_hash,
        tip,
        ..
    }) = read_frame(&mut stream).await?
    else {
        eyre::bail!("expected Hello from {feed}");
    };
    write_frame(&mut stream, &silt_wire::hello(chain_id, genesis_hash, 0)).await?;
    let from = tip.saturating_sub(n as u64 - 1);
    write_frame(&mut stream, &Frame::Subscribe { from: Some(from) }).await?;
    let mut diffs = Vec::new();
    let mut folded = 0;
    while diffs.len() < n {
        match read_frame(&mut stream).await? {
            Some(Frame::Diff(d)) if d.span_start == d.number => diffs.push(d),
            Some(Frame::Diff(_)) => folded += 1,
            Some(Frame::Heartbeat { .. }) => {}
            Some(Frame::GapTooOld { oldest_available }) => {
                eyre::bail!("spool starts at {oldest_available}, after {from}")
            }
            Some(other) => eyre::bail!("unexpected frame {other:?}"),
            None => eyre::bail!("feed closed"),
        }
    }
    println!(
        "feed {feed}: {} single-block diffs {}..={} (skipped {folded} folded spans)",
        diffs.len(),
        diffs[0].number,
        diffs[diffs.len() - 1].number
    );
    Ok(diffs)
}

/// Every account and slot one block changed, hashed, within the request
/// limits (sorted by hashed address; the tail is dropped past the caps).
fn block_targets(d: &BlockDiff) -> (Targets, bool) {
    let mut m: BTreeMap<Word, BTreeSet<Word>> = BTreeMap::new();
    for (a, _) in &d.accounts {
        m.entry(keys::keccak(a)).or_default();
    }
    for (a, s, _) in &d.storage {
        m.entry(keys::keccak(a))
            .or_default()
            .insert(keys::keccak(s));
    }
    let (mut out, mut slots, mut capped) = (Vec::new(), 0, false);
    for (a, s) in m {
        if out.len() == MAX_ITEMS || slots + s.len() > MAX_PROOF_SLOTS {
            capped = true;
            break;
        }
        slots += s.len();
        out.push((a, s.into_iter().collect()));
    }
    (out, capped)
}

/// Trie nodes by path (one nibble per byte) -> RLP.
type Subtree = BTreeMap<Vec<u8>, Vec<u8>>;

/// A proof as comparable owned maps.
#[derive(PartialEq, Eq, Debug)]
struct Proof {
    account: Subtree,
    /// hashed address -> (storage root, storage trie nodes).
    storages: BTreeMap<Word, (Word, Subtree)>,
}

fn from_op4(frame: &ProofFrame) -> Proof {
    let p = frame.to_read();
    Proof {
        account: p
            .account_nodes
            .into_iter()
            .map(|n| (n.path, n.rlp))
            .collect(),
        storages: p
            .storages
            .into_iter()
            .map(|s| {
                let nodes = s.nodes.into_iter().map(|n| (n.path, n.rlp)).collect();
                (s.hashed_address, (s.storage_root, nodes))
            })
            .collect(),
    }
}

fn nibbles(path: &str) -> eyre::Result<Vec<u8>> {
    path.chars()
        .map(|c| {
            c.to_digit(16)
                .map(|d| d as u8)
                .ok_or_else(|| eyre::eyre!("bad nibble {c:?}"))
        })
        .collect()
}

fn subtree(v: &serde_json::Value) -> eyre::Result<Subtree> {
    v.as_object()
        .ok_or_else(|| eyre::eyre!("expected a path -> node map"))?
        .iter()
        .map(|(path, node)| Ok((nibbles(path)?, parse_bytes(node)?)))
        .collect()
}

/// `silt_multiproof`'s JSON (`MultiproofResponse`) as typed maps.
fn from_json(body: &[u8]) -> eyre::Result<Proof> {
    let v: serde_json::Value = serde_json::from_slice(body)?;
    if let Some(err) = v.get("error") {
        eyre::bail!("silt_multiproof: {err}");
    }
    let r = &v["result"];
    let mut storages = BTreeMap::new();
    for (addr, st) in r["storages"]
        .as_object()
        .ok_or_else(|| eyre::eyre!("no storages"))?
    {
        let addr = parse_word(&serde_json::Value::String(addr.clone()))?;
        storages.insert(addr, (parse_word(&st["root"])?, subtree(&st["subtree"])?));
    }
    Ok(Proof {
        account: subtree(&r["accountSubtree"])?,
        storages,
    })
}

struct Rpc {
    url: String,
    http: reqwest::Client,
    roots: HashMap<Word, Word>,
}

impl Rpc {
    async fn call(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> eyre::Result<serde_json::Value> {
        let body =
            serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params});
        let v: serde_json::Value = self
            .http
            .post(&self.url)
            .json(&body)
            .send()
            .await?
            .json()
            .await?;
        if let Some(err) = v.get("error") {
            eyre::bail!("{method}: {err}");
        }
        Ok(v["result"].clone())
    }

    async fn state_root(&mut self, hash: &Word) -> eyre::Result<Word> {
        if let Some(r) = self.roots.get(hash) {
            return Ok(*r);
        }
        let b = self
            .call(
                "eth_getBlockByHash",
                serde_json::json!([hex_word(hash), false]),
            )
            .await?;
        let root = parse_word(&b["stateRoot"])?;
        self.roots.insert(*hash, root);
        Ok(root)
    }

    async fn hash_at(&self, number: u64) -> eyre::Result<Word> {
        let b = self
            .call(
                "eth_getBlockByNumber",
                serde_json::json!([format!("0x{number:x}"), false]),
            )
            .await?;
        parse_word(&b["hash"])
    }

    /// (encode, round trip, parse, response bytes, typed answer).
    async fn multiproof(
        &self,
        targets: &Targets,
        pin: &Word,
    ) -> eyre::Result<(Duration, Duration, Duration, usize, Proof)> {
        let t = Instant::now();
        let targets: Vec<serde_json::Value> = targets
            .iter()
            .map(|(a, s)| {
                serde_json::json!([hex_word(a), s.iter().map(hex_word).collect::<Vec<_>>()])
            })
            .collect();
        let body = serde_json::to_vec(&serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "silt_multiproof",
            "params": [targets, hex_word(pin)],
        }))?;
        let enc = t.elapsed();
        let t = Instant::now();
        let resp = self
            .http
            .post(&self.url)
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await?
            .bytes()
            .await?;
        let rtt = t.elapsed();
        let t = Instant::now();
        let proof = from_json(&resp)?;
        Ok((enc, rtt, t.elapsed(), resp.len(), proof))
    }
}

/// Zero-copy pass over every node (what a consumer does before trusting
/// the answer); returns the node count so the walk isn't optimized away.
fn touch(frame: &ProofFrame) -> usize {
    let p = frame.proof();
    let mut n = 0usize;
    let mut sum = 0usize;
    for node in p.account_nodes() {
        n += 1;
        sum += node.rlp.len() + node.nibbles().len();
    }
    for st in p.storages() {
        sum += st.storage_root[0] as usize;
        for node in st.nodes {
            n += 1;
            sum += node.rlp.len() + node.nibbles().len();
        }
    }
    std::hint::black_box(sum);
    n
}

#[derive(Default)]
struct Row {
    reqs: usize,
    keys: usize,
    nodes: usize,
    op4_bytes: usize,
    json_bytes: usize,
    capped: usize,
    first_rtt: Vec<f64>,
    first_server: Vec<f64>,
    warm_rtt: Vec<f64>,
    warm_server: Vec<f64>,
    warm_transport: Vec<f64>,
    decode: Vec<f64>,
    json_enc: Vec<f64>,
    json_rtt: Vec<f64>,
    json_parse: Vec<f64>,
    json_total: Vec<f64>,
}

fn us(d: Duration) -> f64 {
    d.as_secs_f64() * 1e6
}

fn pct(v: &[f64], p: f64) -> f64 {
    if v.is_empty() {
        return f64::NAN;
    }
    let mut v = v.to_vec();
    v.sort_by(|a, b| a.total_cmp(b));
    v[((v.len() as f64 - 1.0) * p).round() as usize]
}

fn p50_90_max(v: &[f64]) -> String {
    format!(
        "{:.0} / {:.0} / {:.0}",
        pct(v, 0.5),
        pct(v, 0.9),
        pct(v, 1.0)
    )
}

struct Checks {
    answers: usize,
    differ: usize,
    root_failures: usize,
}

async fn op4(
    client: &LookupClient,
    pin: &Word,
    targets: &Targets,
) -> Result<(Duration, ProofFrame), LookupError> {
    let t = Instant::now();
    let frame = client.proof(pin, targets).await?;
    Ok((t.elapsed(), frame))
}

async fn run(
    name: &str,
    requests: &[Targets],
    capped: &[bool],
    client: &LookupClient,
    rpc: &mut Rpc,
    checks: &mut Checks,
) -> eyre::Result<Row> {
    let mut row = Row::default();
    for (targets, &cap) in requests.iter().zip(capped) {
        let (rtt, first) = op4(client, &LATEST, targets).await?;
        row.first_rtt.push(us(rtt));
        row.first_server.push(first.server_us as f64);
        let pin = first.read_hash;

        let (enc, jrtt, parse, jbytes, json) = rpc.multiproof(targets, &pin).await?;
        row.json_enc.push(us(enc));
        row.json_rtt.push(us(jrtt));
        row.json_parse.push(us(parse));
        row.json_total.push(us(enc + jrtt + parse));

        let (rtt, warm) = op4(client, &pin, targets).await?;
        row.warm_rtt.push(us(rtt));
        row.warm_server.push(warm.server_us as f64);
        row.warm_transport
            .push((us(rtt) - warm.server_us as f64).max(0.0));
        let t = Instant::now();
        let nodes = touch(&warm);
        row.decode.push(us(t.elapsed()));

        let ours = from_op4(&warm);
        let root = rpc.state_root(&pin).await?;
        let root_ok = ours
            .account
            .get(&Vec::new())
            .is_some_and(|rlp| keys::keccak(rlp) == root);
        checks.answers += 1;
        if !root_ok {
            checks.root_failures += 1;
            println!(
                "ROOT {name} at {}: op-4 root node does not hash to stateRoot",
                warm.read_number
            );
        }
        // Both op-4 answers are at `pin` too: they must agree with each other.
        if ours != json || from_op4(&first) != ours {
            checks.differ += 1;
            println!(
                "DIFFER {name} at {}: op 4 {} account nodes / {} storages, JSON-RPC {} / {}",
                warm.read_number,
                ours.account.len(),
                ours.storages.len(),
                json.account.len(),
                json.storages.len()
            );
        }

        row.reqs += 1;
        row.keys += targets.len() + targets.iter().map(|(_, s)| s.len()).sum::<usize>();
        row.nodes += nodes;
        row.op4_bytes += warm.wire_len();
        row.json_bytes += jbytes;
        row.capped += cap as usize;
    }
    Ok(row)
}

/// Pins `d` blocks below the node's head, re-resolved per request
/// (untimed); one retry if the head moved past the server's depth bound.
async fn sweep(
    client: &LookupClient,
    rpc: &Rpc,
    d: u64,
    requests: &[Targets],
) -> eyre::Result<(Vec<f64>, Vec<f64>)> {
    let (mut rtt, mut server) = (Vec::new(), Vec::new());
    for targets in requests {
        for attempt in 0..2 {
            let head = client.ping(&LATEST).await?.read_number;
            let pin = rpc.hash_at(head - d).await?;
            match op4(client, &pin, targets).await {
                Ok((t, frame)) => {
                    rtt.push(us(t));
                    server.push(frame.server_us as f64);
                    break;
                }
                Err(LookupError::Status(Status::UnknownBlock)) if attempt == 0 => {}
                Err(e) => return Err(e.into()),
            }
        }
    }
    Ok((rtt, server))
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> eyre::Result<()> {
    let args = args()?;
    let mut cfg = LookupClientConfig::new(&args.lookup);
    cfg.proof_timeout = Duration::from_secs(30);
    let client = LookupClient::new(cfg);
    eyre::ensure!(
        client.wait_connected(Duration::from_secs(10)).await,
        "no lookup server at {}",
        args.lookup
    );
    let mut rpc = Rpc {
        url: args.rpc.clone(),
        http: reqwest::Client::new(),
        roots: HashMap::new(),
    };
    let head = parse_u64(&rpc.call("eth_blockNumber", serde_json::json!([])).await?)?;
    println!("node head {head}, lookup {}, rpc {}", args.lookup, args.rpc);

    let diffs = recent_diffs(&args.feed, args.blocks).await?;
    let (block, block_capped): (Vec<Targets>, Vec<bool>) = diffs.iter().map(block_targets).unzip();

    let mut accounts: Vec<Addr> = diffs
        .iter()
        .flat_map(|d| d.accounts.iter().map(|(a, _)| *a))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let mut slots: Vec<(Addr, Word)> = diffs
        .iter()
        .flat_map(|d| d.storage.iter().map(|(a, s, _)| (*a, *s)))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let mut rng = keys::Rng(0x5117_0004);
    rng.shuffle(&mut accounts);
    rng.shuffle(&mut slots);
    let hashed = |a: &Addr| (keys::keccak(a), Vec::new());
    let acct1: Vec<Targets> = accounts
        .iter()
        .take(args.small)
        .map(|a| vec![hashed(a)])
        .collect();
    let slot1: Vec<Targets> = slots
        .iter()
        .take(args.small)
        .map(|(a, s)| vec![(keys::keccak(a), vec![keys::keccak(s)])])
        .collect();
    let acct64: Vec<Targets> = accounts[args.small.min(accounts.len())..]
        .as_chunks::<64>()
        .0
        .iter()
        .take(args.blocks)
        .map(|c| c.iter().map(hashed).collect())
        .collect();

    let mut checks = Checks {
        answers: 0,
        differ: 0,
        root_failures: 0,
    };
    let mut rows = Vec::new();
    for (name, reqs) in [
        ("block", &block),
        ("acct-1", &acct1),
        ("slot-1", &slot1),
        ("acct-64", &acct64),
    ] {
        let capped: Vec<bool> = if name == "block" {
            block_capped.clone()
        } else {
            vec![false; reqs.len()]
        };
        let row = run(name, reqs, &capped, &client, &mut rpc, &mut checks).await?;
        println!("{name}: {} requests done", row.reqs);
        rows.push((name, row));
    }

    println!();
    println!(
        "| workload | reqs | keys/req | nodes/req | op 4 KiB | JSON KiB | op 4 first touch p50/p90/max (server p50) | op 4 warm p50/p90/max (server / transport / decode p50) | JSON-RPC p50/p90/max (enc / rtt / parse p50) | saved vs JSON-RPC p50 |"
    );
    println!("|---|---:|---:|---:|---:|---:|---|---|---|---|");
    for (name, r) in &rows {
        let n = r.reqs.max(1) as f64;
        let (warm, json) = (pct(&r.warm_rtt, 0.5), pct(&r.json_total, 0.5));
        println!(
            "| {name}{} | {} | {:.0} | {:.0} | {:.1} | {:.1} | {} ({:.0}) | {} ({:.0} / {:.0} / {:.0}) | {} ({:.0} / {:.0} / {:.0}) | {:.0} ({:.0}%) |",
            if r.capped > 0 {
                format!(" ({} capped)", r.capped)
            } else {
                String::new()
            },
            r.reqs,
            r.keys as f64 / n,
            r.nodes as f64 / n,
            r.op4_bytes as f64 / n / 1024.0,
            r.json_bytes as f64 / n / 1024.0,
            p50_90_max(&r.first_rtt),
            pct(&r.first_server, 0.5),
            p50_90_max(&r.warm_rtt),
            pct(&r.warm_server, 0.5),
            pct(&r.warm_transport, 0.5),
            pct(&r.decode, 0.5),
            p50_90_max(&r.json_total),
            pct(&r.json_enc, 0.5),
            pct(&r.json_rtt, 0.5),
            pct(&r.json_parse, 0.5),
            json - warm,
            (json - warm) / json * 100.0,
        );
    }

    println!();
    println!(
        "| depth | block-sized: first / p50 / max server µs | 64-account: first / p50 / max server µs |"
    );
    println!("|---:|---|---|");
    let k = args.sweep;
    for &d in &args.depths {
        let (_, b) = sweep(&client, &rpc, d, &block[..k.min(block.len())]).await?;
        let (_, a) = sweep(&client, &rpc, d, &acct64[..k.min(acct64.len())]).await?;
        let f = |v: &[f64]| {
            format!(
                "{:.0} / {:.0} / {:.0}",
                v.first().copied().unwrap_or(f64::NAN),
                pct(v, 0.5),
                pct(v, 1.0)
            )
        };
        println!("| {d} | {} | {} |", f(&b), f(&a));
    }

    println!();
    println!(
        "done: {} op-4 answers, {} differ from silt_multiproof at the same block, {} root failures",
        checks.answers, checks.differ, checks.root_failures
    );
    if checks.differ > 0 || checks.root_failures > 0 {
        std::process::exit(1);
    }
    Ok(())
}
