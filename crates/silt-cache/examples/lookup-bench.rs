//! Lookup fast-path benchmark (PLAN T4.6): real mainnet keys against a
//! lookup server (the ExEx, or the read-only harness `lookup-ro`).
//!
//! Usage:
//!   lookup-bench [--lookup 127.0.0.1:8587] [--rpc http://127.0.0.1:8545]
//!                [--server-pid PID] [--threads 0] [--scale 1.0]
//!                [--pin latest|fixed] [--modes seq,if16,if256,b16,b256]
//!                [--old-offset 5000]
//!
//! `--pin latest` (default) reads at the server's tip on every request —
//! the path a builder pinned to its cache head takes in the ExEx. `--pin
//! fixed` pins every request to the hash the server called latest at start;
//! once the tip moves past it, that is a read *behind* the tip (through
//! reth's history path in the read-only harness). Two key pools:
//! - `recent`: keys touched by the newest blocks (senders, recipients, log
//!   emitters, access-list slots) — what a builder misses on;
//! - `old`: the same kinds of keys from blocks `--old-offset` back — likely cold in
//!   the OS page cache (the page cache is shared with the live node and is
//!   not dropped; `--server-pid` reports the server's major page faults per
//!   pass as evidence).
//!
//! Each mode runs on its own disjoint slice of a pool twice: first touch,
//! then warm. Per request: round trip (client clock, from call to typed
//! result), `server_us` (decode → encode on the server) and transport =
//! round trip − `server_us`.

#[path = "common/keys.rs"]
mod keys;

use std::time::{Duration, Instant};

use futures::StreamExt;
use silt_cache::lookup::{LookupClient, LookupClientConfig};
use silt_cache::{Fallback, HttpFallback};
use silt_wire::lookup::LATEST;
use silt_wire::{Addr, Word};

#[derive(Clone, Copy)]
enum Key {
    Account(Addr),
    Slot(Addr, Word),
}

#[derive(Clone, Copy, Debug)]
enum Mode {
    Sequential,
    InFlight(usize),
    Batch(usize),
}

impl Mode {
    fn name(self) -> String {
        match self {
            Self::Sequential => "single, sequential".into(),
            Self::InFlight(n) => format!("single, {n} in flight"),
            Self::Batch(n) => format!("batch of {n}"),
        }
    }
}

#[derive(Default)]
struct Samples {
    rtt: Vec<f64>,
    server: Vec<f64>,
    transport: Vec<f64>,
    keys: usize,
    errors: usize,
}

impl Samples {
    fn push(&mut self, rtt: Duration, server_us: u32, keys: usize) {
        let rtt = rtt.as_secs_f64() * 1e6;
        self.rtt.push(rtt);
        self.server.push(server_us as f64);
        self.transport.push((rtt - server_us as f64).max(0.0));
        self.keys += keys;
    }
}

fn pct(v: &mut [f64], p: f64) -> f64 {
    if v.is_empty() {
        return f64::NAN;
    }
    v.sort_by(|a, b| a.total_cmp(b));
    let i = ((v.len() as f64 - 1.0) * p).round() as usize;
    v[i]
}

fn fmt3(v: &mut [f64]) -> String {
    format!(
        "{:.0} / {:.0} / {:.0}",
        pct(v, 0.50),
        pct(v, 0.99),
        pct(v, 0.999)
    )
}

/// (major faults, bytes read from storage) of the server process — mmap
/// faults and `pread`s (RocksDB) alike show up in `read_bytes`.
fn disk(pid: Option<u32>) -> Option<(u64, u64)> {
    let pid = pid?;
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // Fields after the parenthesized comm: state is field 3, majflt 12.
    let rest = &stat[stat.rfind(')')? + 2..];
    let majflt = rest.split_whitespace().nth(9)?.parse().ok()?;
    let io = std::fs::read_to_string(format!("/proc/{pid}/io")).ok()?;
    let read = io
        .lines()
        .find_map(|l| l.strip_prefix("read_bytes: "))?
        .trim()
        .parse()
        .ok()?;
    Some((majflt, read))
}

async fn single(client: &LookupClient, pin: &Word, key: Key) -> Option<(Duration, u32)> {
    let t = Instant::now();
    let server_us = match key {
        // Inline code, exactly what `LookupFallback::account_at` asks.
        Key::Account(a) => client.accounts(pin, &[a], true).await.ok()?.server_us,
        Key::Slot(a, s) => client.storage(pin, &[(a, s)]).await.ok()?.server_us,
    };
    Some((t.elapsed(), server_us))
}

async fn run(client: &LookupClient, pin: &Word, mode: Mode, keys: &[Key]) -> (Samples, Duration) {
    let mut s = Samples::default();
    let start = Instant::now();
    match mode {
        Mode::Sequential => {
            for &k in keys {
                match single(client, pin, k).await {
                    Some((rtt, us)) => s.push(rtt, us, 1),
                    None => s.errors += 1,
                }
            }
        }
        Mode::InFlight(n) => {
            let results: Vec<_> = futures::stream::iter(keys.iter().copied())
                .map(|k| single(client, pin, k))
                .buffer_unordered(n)
                .collect()
                .await;
            for r in results {
                match r {
                    Some((rtt, us)) => s.push(rtt, us, 1),
                    None => s.errors += 1,
                }
            }
        }
        Mode::Batch(n) => {
            // One op per frame: accounts (no inline code — 256 exceeds the
            // 64-item cap for code-carrying frames) and slots separately.
            let accounts: Vec<Addr> = keys
                .iter()
                .filter_map(|k| match k {
                    Key::Account(a) => Some(*a),
                    _ => None,
                })
                .collect();
            let slots: Vec<(Addr, Word)> = keys
                .iter()
                .filter_map(|k| match k {
                    Key::Slot(a, sl) => Some((*a, *sl)),
                    _ => None,
                })
                .collect();
            for chunk in accounts.chunks(n) {
                let t = Instant::now();
                match client.accounts(pin, chunk, false).await {
                    Ok(r) => s.push(t.elapsed(), r.server_us, chunk.len()),
                    Err(_) => s.errors += 1,
                }
            }
            for chunk in slots.chunks(n) {
                let t = Instant::now();
                match client.storage(pin, chunk).await {
                    Ok(r) => s.push(t.elapsed(), r.server_us, chunk.len()),
                    Err(_) => s.errors += 1,
                }
            }
        }
    }
    (s, start.elapsed())
}

struct Args {
    lookup: String,
    rpc: String,
    server_pid: Option<u32>,
    threads: usize,
    scale: f64,
    fixed_pin: bool,
    modes: Vec<Mode>,
    old_offset: u64,
}

fn args() -> eyre::Result<Args> {
    let mut a = Args {
        lookup: "127.0.0.1:8587".into(),
        rpc: "http://127.0.0.1:8545".into(),
        server_pid: None,
        threads: 0,
        scale: 1.0,
        fixed_pin: false,
        modes: MODES.to_vec(),
        old_offset: 5000,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut v = || it.next().ok_or_else(|| eyre::eyre!("{flag} needs a value"));
        match flag.as_str() {
            "--lookup" => a.lookup = v()?,
            "--rpc" => a.rpc = v()?,
            "--server-pid" => a.server_pid = Some(v()?.parse()?),
            "--threads" => a.threads = v()?.parse()?,
            "--scale" => a.scale = v()?.parse()?,
            "--old-offset" => a.old_offset = v()?.parse()?,
            "--modes" => {
                a.modes = v()?
                    .split(',')
                    .map(|m| match m {
                        "seq" => Ok(Mode::Sequential),
                        "if16" => Ok(Mode::InFlight(16)),
                        "if256" => Ok(Mode::InFlight(256)),
                        "b16" => Ok(Mode::Batch(16)),
                        "b256" => Ok(Mode::Batch(256)),
                        other => Err(eyre::eyre!("unknown mode {other}")),
                    })
                    .collect::<eyre::Result<_>>()?
            }
            "--pin" => {
                a.fixed_pin = match v()?.as_str() {
                    "latest" => false,
                    "fixed" => true,
                    other => eyre::bail!("--pin latest|fixed, not {other}"),
                }
            }
            other => eyre::bail!("unknown flag {other}"),
        }
    }
    Ok(a)
}

fn main() -> eyre::Result<()> {
    let args = args()?;
    // 0 = current-thread runtime: no cross-thread wakeups on the client.
    let rt = if args.threads == 0 {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
    } else {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(args.threads)
            .enable_all()
            .build()?
    };
    rt.block_on(bench(args))
}

const MODES: [Mode; 5] = [
    Mode::Sequential,
    Mode::InFlight(16),
    Mode::InFlight(256),
    Mode::Batch(16),
    Mode::Batch(256),
];

/// Lookups per mode slice (scaled by `--scale`).
fn slice_len(mode: Mode) -> usize {
    match mode {
        Mode::Sequential => 1000,
        Mode::InFlight(16) => 2000,
        Mode::InFlight(_) => 4000,
        Mode::Batch(16) => 2048,
        Mode::Batch(_) => 4096,
    }
}

async fn bench(args: Args) -> eyre::Result<()> {
    let client = LookupClient::new(
        LookupClientConfig::new(&args.lookup).with_timeout(Duration::from_secs(5)),
    );
    eyre::ensure!(
        client.wait_connected(Duration::from_secs(10)).await,
        "no lookup server at {}",
        args.lookup
    );
    let head = client.ping(&LATEST).await?;
    let (pin_number, start_hash) = (head.read_number, head.read_hash);
    let pin = if args.fixed_pin { start_hash } else { LATEST };
    println!(
        "lookup server {} | {} {} 0x{} | client runtime: {}",
        args.lookup,
        if args.fixed_pin {
            "pinned at"
        } else {
            "pin: latest; tip at start"
        },
        pin_number,
        hex(&start_hash),
        if args.threads == 0 {
            "current-thread".to_string()
        } else {
            format!("{} threads", args.threads)
        }
    );

    let rpc = keys::Rpc::new(&args.rpc);
    let need: usize = args
        .modes
        .iter()
        .map(|&m| (slice_len(m) as f64 * args.scale) as usize)
        .sum();
    let mut pools = Vec::new();
    for (name, top) in [
        ("recent", pin_number),
        ("old", pin_number.saturating_sub(args.old_offset)),
    ] {
        let k = keys::collect(&rpc, top, 400, need * 3 / 4, need / 4).await?;
        println!(
            "pool {name}: blocks {}..={} -> {} accounts ({} senders/recipients, {} log emitters), {} access-list slots",
            k.first_block,
            k.last_block,
            k.accounts.len(),
            k.source.senders_recipients,
            k.source.log_emitters,
            k.slots.len()
        );
        let mut all: Vec<Key> = k.accounts.iter().map(|&a| Key::Account(a)).collect();
        all.extend(k.slots.iter().map(|&(a, s)| Key::Slot(a, s)));
        keys::Rng(0x5117_BE4C).shuffle(&mut all);
        pools.push((name, all));
    }

    println!();
    println!(
        "| pool | pass | mode | requests | keys | keys/s | round trip µs p50/p99/p999 | server µs p50/p99/p999 | transport µs p50/p99/p999 | server disk reads (MiB / major faults) | errors |"
    );
    println!("|---|---|---|---:|---:|---:|---|---|---|---:|---:|");
    for (name, all) in &pools {
        let mut at = 0;
        for &mode in &args.modes {
            let len =
                ((slice_len(mode) as f64 * args.scale) as usize).min(all.len().saturating_sub(at));
            let slice = &all[at..at + len];
            at += len;
            if slice.is_empty() {
                println!("| {name} | - | {} | pool exhausted |", mode.name());
                continue;
            }
            for pass in ["first touch", "warm"] {
                let d0 = disk(args.server_pid);
                let (mut s, wall) = run(&client, &pin, mode, slice).await;
                let faults = match (d0, disk(args.server_pid)) {
                    (Some((f0, r0)), Some((f1, r1))) => {
                        format!("{:.1} / {}", (r1 - r0) as f64 / (1 << 20) as f64, f1 - f0)
                    }
                    _ => "-".into(),
                };
                println!(
                    "| {name} | {pass} | {} | {} | {} | {:.0} | {} | {} | {} | {} | {} |",
                    mode.name(),
                    s.rtt.len(),
                    s.keys,
                    s.keys as f64 / wall.as_secs_f64(),
                    fmt3(&mut s.rtt),
                    fmt3(&mut s.server),
                    fmt3(&mut s.transport),
                    faults,
                    s.errors
                );
            }
        }
    }

    // Context: the same single reads over pinned eth_* JSON-RPC (what a
    // miss cost before D9), on warm keys of the recent pool.
    let http = HttpFallback::new(&args.rpc);
    let sample: Vec<Key> = pools[0]
        .1
        .iter()
        .take((300.0 * args.scale) as usize)
        .copied()
        .collect();
    let mut rtt = Vec::new();
    for k in &sample {
        let t = Instant::now();
        let ok = match k {
            Key::Account(a) => http.account_at(a, &start_hash).await.is_ok(),
            Key::Slot(a, s) => http.storage_at(a, s, &start_hash).await.is_ok(),
        };
        if ok {
            rtt.push(t.elapsed().as_secs_f64() * 1e6);
        }
    }
    println!();
    println!(
        "baseline: pinned eth_* JSON-RPC, sequential, same recent keys (account = 3 calls): {} reads, µs p50/p99/p999 {}",
        rtt.len(),
        fmt3(&mut rtt)
    );
    Ok(())
}

fn hex(w: &Word) -> String {
    w.iter().map(|b| format!("{b:02x}")).collect()
}
