//! Live equivalence verifier and soak (PLAN T3.1/T3.2): subscribe to a silt
//! feed, apply every diff to a silt-cache [`Store`] (with the cache's
//! eviction), and check every diff-applied key against RPC reads pinned at
//! that block. Zero mismatches or it exits 1.
//!
//! Per diff, pinned at the diff's block hash (EIP-1898):
//! - every storage slot (`eth_getStorageAt`);
//! - every live account's balance, nonce and code hash (keccak of
//!   `eth_getCode`; `KECCAK_EMPTY` for none);
//! - every destroyed account reads as empty (zero balance and nonce, no
//!   code);
//! - every new code blob hashes to its key (local);
//! - the store answers every applied key with the diff's value (local).
//!
//! A reader task stamps each frame on arrival (so slow RPC checks don't
//! skew latency) and reconnects on EOF, error or 30 s of silence,
//! resubscribing after the last block it received. The checker also
//! classifies continuity like the cache's client
//! ([`silt_cache::client::continuity`]): a span overlapping the tip after a
//! node restart is applied, a re-delivered diff is skipped, a `Reorg` rolls
//! the store back to its fork (PLAN D12), and a jump, a fork, a `GapTooOld`
//! or a reorg it can't roll back drops the store to cold; all counted.
//! After a rollback, every key the dropped blocks touched that the store
//! still holds is checked against RPC pinned at the fork block.
//!
//! RPC calls are retried (3 attempts over ~3 s); a key whose call still
//! fails — the node restarting, or the block pruned or reorged away —
//! counts as unverifiable, never as a mismatch.
//!
//! Soak metrics: block timestamp → diff received (ms; includes block
//! propagation and import, the node exposes no commit time), frame bytes,
//! store apply time, per-block check time, store size and this process's
//! RSS. A summary line every `--summary-mins` and a final report, also
//! when stopped early by SIGTERM or Ctrl-C.
//!
//! Usage: verify [feed-addr] [rpc-url] [--blocks N] [--hours H]
//!               [--summary-mins 60] [--evict-after 64] [--concurrency 16]

use std::collections::{BTreeSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures::StreamExt;
use silt_cache::client::{Continuity, continuity};
use silt_cache::fallback::{Fallback, HttpFallback, hex_addr, hex_word};
use silt_cache::store::Store;
use silt_wire::{Addr, BlockDiff, Frame, KECCAK_EMPTY, MAX_FRAME_BYTES, Word, write_frame};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};

struct Args {
    feed: String,
    rpc: String,
    blocks: u64,
    hours: f64,
    summary_mins: u64,
    evict_after: u64,
    concurrency: usize,
}

fn args() -> eyre::Result<Args> {
    let mut a = Args {
        feed: "127.0.0.1:8585".into(),
        rpc: "http://127.0.0.1:8545".into(),
        blocks: u64::MAX,
        hours: f64::INFINITY,
        summary_mins: 60,
        evict_after: 64,
        concurrency: 16,
    };
    let mut positional = 0;
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        let mut v = || it.next().ok_or_else(|| eyre::eyre!("{arg} needs a value"));
        match arg.as_str() {
            "--blocks" => a.blocks = v()?.parse()?,
            "--hours" => a.hours = v()?.parse()?,
            "--summary-mins" => a.summary_mins = v()?.parse()?,
            "--evict-after" => a.evict_after = v()?.parse()?,
            "--concurrency" => a.concurrency = v()?.parse()?,
            flag if flag.starts_with("--") => eyre::bail!("unknown flag {flag}"),
            value => {
                match positional {
                    0 => a.feed = value.into(),
                    1 => a.rpc = value.into(),
                    _ => eyre::bail!("unexpected argument {value}"),
                }
                positional += 1;
            }
        }
    }
    Ok(a)
}

fn keccak(bytes: &[u8]) -> Word {
    use tiny_keccak::Hasher;
    let mut out = [0u8; 32];
    let mut k = tiny_keccak::Keccak::v256();
    k.update(bytes);
    k.finalize(&mut out);
    out
}

fn now_ms() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64() * 1e3)
        .unwrap_or(0.0)
}

fn rss_mib() -> f64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find_map(|l| l.strip_prefix("VmRSS:"))
                .and_then(|v| v.split_whitespace().next()?.parse::<f64>().ok())
        })
        .map_or(f64::NAN, |kb| kb / 1024.0)
}

// ---- reader: frames off the wire, stamped on arrival -------------------

enum Event {
    Diff {
        diff: BlockDiff,
        bytes: usize,
        received_ms: f64,
        /// At or below the server's tip at connect: a spool replay, not a
        /// live publish (excluded from latency).
        replayed: bool,
    },
    Gap(u64),
    Reorg {
        fork_number: u64,
        fork_hash: Word,
    },
    Disconnected(String),
}

/// `read_frame` that also returns the frame's wire size (length prefix
/// included).
async fn read_sized<R: AsyncRead + Unpin>(r: &mut R) -> eyre::Result<Option<(Frame, usize)>> {
    let mut len = [0u8; 4];
    match r.read_exact(&mut len).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let len = u32::from_le_bytes(len);
    eyre::ensure!(len <= MAX_FRAME_BYTES, "frame length {len} exceeds cap");
    let mut buf = vec![0u8; len as usize];
    r.read_exact(&mut buf).await?;
    Ok(Some((Frame::decode(&buf)?, 4 + len as usize)))
}

/// A diff's block number and the accounts and slots it wrote.
type Touched = (u64, Vec<Addr>, Vec<(Addr, Word)>);

/// The newest block the stream is on: the last diff received, or the fork
/// a reorg rolled back to. `None` = nothing yet.
type Last = Mutex<Option<(u64, Word)>>;

/// One connection: handshake, subscribe after `last` (with its hash, so a
/// tip a reorg dropped meanwhile is answered with the fork, D12), forward
/// frames until EOF/error/silence.
async fn stream_once(feed: &str, last: &Last, tx: &UnboundedSender<Event>) -> eyre::Result<()> {
    let mut stream = tokio::net::TcpStream::connect(feed).await?;
    let Some((
        Frame::Hello {
            chain_id,
            genesis_hash,
            tip,
            ..
        },
        _,
    )) = read_sized(&mut stream).await?
    else {
        eyre::bail!("expected Hello");
    };
    write_frame(&mut stream, &silt_wire::hello(chain_id, genesis_hash, 0)).await?;
    let after = *last.lock().expect("last lock");
    let from = after.map(|(n, _)| n + 1);
    let subscribe = Frame::Subscribe {
        from,
        parent_hash: after.map(|(_, h)| h),
    };
    write_frame(&mut stream, &subscribe).await?;
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(30), read_sized(&mut stream))
            .await
            .map_err(|_| eyre::eyre!("feed silent for 30 s"))??;
        let Some((frame, bytes)) = frame else {
            return Ok(());
        };
        let received_ms = now_ms();
        let event = match frame {
            Frame::Diff(diff) => {
                *last.lock().expect("last lock") = Some((diff.number, diff.hash));
                Event::Diff {
                    replayed: from.is_some() && diff.number <= tip,
                    diff,
                    bytes,
                    received_ms,
                }
            }
            Frame::GapTooOld { oldest_available } => Event::Gap(oldest_available),
            Frame::Reorg {
                fork_number,
                fork_hash,
            } => {
                *last.lock().expect("last lock") = Some((fork_number, fork_hash));
                Event::Reorg {
                    fork_number,
                    fork_hash,
                }
            }
            Frame::Heartbeat { .. } => continue,
            other => eyre::bail!("unexpected frame: {other:?}"),
        };
        if tx.send(event).is_err() {
            return Ok(());
        }
    }
}

async fn reader(feed: String, tx: UnboundedSender<Event>) {
    let last: Last = Mutex::new(None);
    loop {
        let why = match stream_once(&feed, &last, &tx).await {
            Ok(()) => "eof".to_string(),
            Err(err) => err.to_string(),
        };
        if tx.send(Event::Disconnected(why)).is_err() {
            return;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

// ---- checks ------------------------------------------------------------

enum Check {
    Ok,
    Mismatch(String),
    Unverifiable(String),
}

async fn retry<T, F, Fut>(mut call: F) -> Result<T, String>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = eyre::Result<T>>,
{
    let mut err = String::new();
    for attempt in 0..3u64 {
        match call().await {
            Ok(v) => return Ok(v),
            Err(e) => err = e.to_string(),
        }
        tokio::time::sleep(Duration::from_millis(500 * (attempt + 1))).await;
    }
    Err(err)
}

async fn check_slot(
    rpc: &HttpFallback,
    n: u64,
    pin: &Word,
    addr: Addr,
    slot: Word,
    want: Word,
) -> Check {
    match retry(|| rpc.storage_at(&addr, &slot, pin)).await {
        Err(e) => Check::Unverifiable(e),
        Ok(got) if got == want => Check::Ok,
        Ok(got) => Check::Mismatch(format!(
            "block {n} {} slot {}: feed {} rpc {}",
            hex_addr(&addr),
            hex_word(&slot),
            hex_word(&want),
            hex_word(&got)
        )),
    }
}

async fn check_account(
    rpc: &HttpFallback,
    n: u64,
    pin: &Word,
    addr: Addr,
    want: Option<(Word, u64, Word)>,
) -> Check {
    let got = match retry(|| rpc.account_at(&addr, pin)).await {
        Err(e) => return Check::Unverifiable(e),
        Ok(a) => a,
    };
    let code_hash = if got.code.is_empty() {
        KECCAK_EMPTY
    } else {
        keccak(&got.code)
    };
    let (balance, nonce, hash) = want.unwrap_or(([0; 32], 0, KECCAK_EMPTY));
    if got.balance == balance && got.nonce == nonce && code_hash == hash {
        return Check::Ok;
    }
    Check::Mismatch(format!(
        "block {n} account {}{}: feed bal {} nonce {} code {} | rpc bal {} nonce {} code {}",
        hex_addr(&addr),
        if want.is_none() { " (destroyed)" } else { "" },
        hex_word(&balance),
        nonce,
        hex_word(&hash),
        hex_word(&got.balance),
        got.nonce,
        hex_word(&code_hash)
    ))
}

/// Local checks: new code hashes to its key; the store answers every
/// applied key with the diff's value.
fn check_local(store: &Store, diff: &BlockDiff) -> Vec<String> {
    let n = diff.number;
    let mut bad = Vec::new();
    for (hash, bytes) in &diff.code {
        if keccak(bytes) != *hash {
            bad.push(format!(
                "block {n} code {}: bytes hash elsewhere",
                hex_word(hash)
            ));
        }
    }
    for (addr, change) in &diff.accounts {
        let entry = store.account(addr, n);
        let ok = match (change, &entry) {
            (None, None) => true,
            (Some(c), Some(e)) => {
                e.balance == c.balance && e.nonce == c.nonce && e.code_hash == c.code_hash
            }
            _ => false,
        };
        if !ok {
            bad.push(format!("block {n} store account {}", hex_addr(addr)));
        }
    }
    for (addr, slot, value) in &diff.storage {
        if store.slot(addr, slot, n) != Some(*value) {
            bad.push(format!(
                "block {n} store slot {} {}",
                hex_addr(addr),
                hex_word(slot)
            ));
        }
    }
    bad
}

// ---- metrics -----------------------------------------------------------

#[derive(Default)]
struct Window {
    blocks: u64,
    diffs: u64,
    keys: u64,
    mismatches: u64,
    unverifiable: u64,
    reconnects: u64,
    gaps: u64,
    reorgs: u64,
    /// Reorgs rolled back (the rest dropped to cold), and the keys of
    /// their dropped blocks checked at the fork.
    rollbacks: u64,
    rollback_keys: u64,
    breaks: u64,
    overlaps: u64,
    stale: u64,
    bytes_total: u64,
    latency_ms: Vec<f64>,
    bytes_per_block: Vec<f64>,
    apply_us: Vec<f64>,
    check_ms: Vec<f64>,
}

fn pct(v: &[f64], p: f64) -> f64 {
    if v.is_empty() {
        return f64::NAN;
    }
    let mut v = v.to_vec();
    v.sort_by(|a, b| a.total_cmp(b));
    v[((v.len() as f64 - 1.0) * p).round() as usize]
}

fn p3(v: &[f64]) -> String {
    format!(
        "{:.0} / {:.0} / {:.0}",
        pct(v, 0.5),
        pct(v, 0.99),
        pct(v, 1.0)
    )
}

impl Window {
    fn merge(&mut self, o: &Window) {
        self.blocks += o.blocks;
        self.diffs += o.diffs;
        self.keys += o.keys;
        self.mismatches += o.mismatches;
        self.unverifiable += o.unverifiable;
        self.reconnects += o.reconnects;
        self.gaps += o.gaps;
        self.reorgs += o.reorgs;
        self.rollbacks += o.rollbacks;
        self.rollback_keys += o.rollback_keys;
        self.breaks += o.breaks;
        self.overlaps += o.overlaps;
        self.stale += o.stale;
        self.bytes_total += o.bytes_total;
        self.latency_ms.extend(&o.latency_ms);
        self.bytes_per_block.extend(&o.bytes_per_block);
        self.apply_us.extend(&o.apply_us);
        self.check_ms.extend(&o.check_ms);
    }

    fn line(&self, label: &str, store: &Store, elapsed: Duration) -> String {
        let (accounts, slots) = store.len();
        let hours = elapsed.as_secs_f64() / 3600.0;
        format!(
            "SUMMARY {label} (+{hours:.2} h): blocks {} in {} diffs, keys {}, mismatches {}, unverifiable {}, \
             reconnects {}, gaps {}, reorgs {} (rolled back {}, {} keys checked), breaks {}, overlaps {}, stale {} | latency ms p50/p99/max {} | bytes/block p50/p99/max {} \
             ({:.1} MiB) | apply µs p50/p99/max {} | check ms p50/p99/max {} | store {} accounts {} slots | rss {:.0} MiB",
            self.blocks,
            self.diffs,
            self.keys,
            self.mismatches,
            self.unverifiable,
            self.reconnects,
            self.gaps,
            self.reorgs,
            self.rollbacks,
            self.rollback_keys,
            self.breaks,
            self.overlaps,
            self.stale,
            p3(&self.latency_ms),
            p3(&self.bytes_per_block),
            self.bytes_total as f64 / (1 << 20) as f64,
            p3(&self.apply_us),
            p3(&self.check_ms),
            accounts,
            slots,
            rss_mib()
        )
    }
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let args = args()?;
    let rpc = HttpFallback::new(args.rpc.clone());
    let store = Store::new(256);
    // Keys of the last diffs applied, for checking a rollback.
    let mut recent: VecDeque<Touched> = VecDeque::new();
    let (tx, mut rx) = unbounded_channel();
    tokio::spawn(reader(args.feed.clone(), tx));
    println!(
        "verify: feed {} rpc {} (blocks {}, hours {}, evict after {} blocks, {} RPC calls in flight)",
        args.feed, args.rpc, args.blocks, args.hours, args.evict_after, args.concurrency
    );

    // SIGTERM (systemctl stop) or Ctrl-C ends the run with the report.
    let stop = Arc::new(tokio::sync::Notify::new());
    {
        let stop = stop.clone();
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::spawn(async move {
            tokio::select! {
                _ = term.recv() => {}
                _ = tokio::signal::ctrl_c() => {}
            }
            stop.notify_one();
        });
    }
    let start = Instant::now();
    let deadline = Duration::from_secs_f64((args.hours * 3600.0).min(1e9));
    let summary_every = Duration::from_secs(args.summary_mins.max(1) * 60);
    let (mut total, mut window) = (Window::default(), Window::default());
    let (mut next_summary, mut hour) = (summary_every, 0u64);
    let mut rss_max = 0f64;

    while total.blocks + window.blocks < args.blocks && start.elapsed() < deadline {
        let left = deadline.saturating_sub(start.elapsed());
        let event = tokio::select! {
            event = rx.recv() => match event {
                Some(event) => Some(event),
                None => break,
            },
            // No frames for a minute: still print summaries.
            _ = tokio::time::sleep(left.min(Duration::from_secs(60))) => None,
            _ = stop.notified() => {
                println!("stopping on signal");
                break;
            }
        };
        match event {
            None => {}
            Some(Event::Disconnected(why)) => {
                window.reconnects += 1;
                println!("feed disconnected ({why}); reconnecting");
            }
            Some(Event::Gap(oldest)) => {
                window.gaps += 1;
                store.clear();
                recent.clear();
                println!("GAP: spool starts at {oldest}; store dropped to cold");
            }
            Some(Event::Reorg {
                fork_number,
                fork_hash,
            }) => {
                window.reorgs += 1;
                let tip = store.tip().map_or(0, |(n, _)| n);
                if !store.rollback(fork_number, fork_hash) {
                    store.clear();
                    recent.clear();
                    println!(
                        "REORG to fork {fork_number} from {tip}: can't roll back; store dropped to cold"
                    );
                    continue;
                }
                window.rollbacks += 1;
                // Whatever the store still holds for a key the dropped
                // blocks touched must be the fork's value.
                let dropped: Vec<Touched> = recent
                    .iter()
                    .filter(|(n, _, _)| *n > fork_number)
                    .cloned()
                    .collect();
                recent.retain(|(n, _, _)| *n <= fork_number);
                let mut accounts = BTreeSet::new();
                let mut slots = BTreeSet::new();
                for (_, a, s) in &dropped {
                    accounts.extend(a.iter().copied());
                    slots.extend(s.iter().copied());
                }
                let held_accounts: Vec<(Addr, (Word, u64, Word))> = accounts
                    .into_iter()
                    .filter_map(|a| {
                        let e = store.account(&a, fork_number)?;
                        Some((a, (e.balance, e.nonce, e.code_hash)))
                    })
                    .collect();
                let held_slots: Vec<(Addr, Word, Word)> = slots
                    .into_iter()
                    .filter_map(|(a, s)| Some((a, s, store.slot(&a, &s, fork_number)?)))
                    .collect();
                let checks = held_slots
                    .iter()
                    .map(|&(a, s, v)| {
                        futures::future::Either::Left(check_slot(
                            &rpc,
                            fork_number,
                            &fork_hash,
                            a,
                            s,
                            v,
                        ))
                    })
                    .chain(held_accounts.iter().map(|&(a, want)| {
                        futures::future::Either::Right(check_account(
                            &rpc,
                            fork_number,
                            &fork_hash,
                            a,
                            Some(want),
                        ))
                    }));
                let results: Vec<Check> = futures::stream::iter(checks)
                    .buffer_unordered(args.concurrency)
                    .collect()
                    .await;
                let mut bad = 0u64;
                for r in &results {
                    match r {
                        Check::Ok => {}
                        Check::Mismatch(line) => {
                            bad += 1;
                            println!("MISMATCH after rollback: {line}");
                        }
                        Check::Unverifiable(_) => window.unverifiable += 1,
                    }
                }
                window.mismatches += bad;
                window.rollback_keys += results.len() as u64;
                println!(
                    "REORG: rolled back {} block(s) from {tip} to fork {fork_number}; {} held keys of the dropped blocks checked at the fork, {bad} mismatched",
                    tip - fork_number,
                    results.len()
                );
            }
            Some(Event::Diff {
                diff,
                bytes,
                received_ms,
                replayed,
            }) => {
                match continuity(&store, &diff) {
                    Continuity::Extends => {}
                    Continuity::Overlaps => {
                        window.overlaps += 1;
                        println!(
                            "OVERLAP: span {}..{} passes through our tip (node restart); applied",
                            diff.span_start, diff.number
                        );
                    }
                    Continuity::Stale => {
                        window.stale += 1;
                        println!("STALE: diff {} already applied; skipped", diff.number);
                        continue;
                    }
                    Continuity::Break => {
                        window.breaks += 1;
                        let tip = store.tip().map_or(0, |(n, _)| n);
                        store.clear();
                        recent.clear();
                        println!(
                            "BREAK: diff {}..{} does not extend {tip}; store dropped to cold",
                            diff.span_start, diff.number
                        );
                    }
                }
                let t = Instant::now();
                store.apply(&diff);
                let apply_us = t.elapsed().as_secs_f64() * 1e6;
                recent.push_back((
                    diff.number,
                    diff.accounts.iter().map(|(a, _)| *a).collect(),
                    diff.storage.iter().map(|(a, s, _)| (*a, *s)).collect(),
                ));
                while recent.len() > silt_cache::store::DEFAULT_REORG_DEPTH {
                    recent.pop_front();
                }
                if diff.number.is_multiple_of(8) {
                    store.evict(diff.number, args.evict_after);
                }

                let t = Instant::now();
                let n = diff.number;
                let pin = diff.hash;
                let mut local = check_local(&store, &diff);
                let slots = diff.storage.iter().map(|&(a, s, v)| {
                    futures::future::Either::Left(check_slot(&rpc, n, &pin, a, s, v))
                });
                let accounts = diff.accounts.iter().map(|(a, c)| {
                    let want = c.as_ref().map(|c| (c.balance, c.nonce, c.code_hash));
                    futures::future::Either::Right(check_account(&rpc, n, &pin, *a, want))
                });
                let results: Vec<Check> = futures::stream::iter(slots.chain(accounts))
                    .buffer_unordered(args.concurrency)
                    .collect()
                    .await;
                let check_ms = t.elapsed().as_secs_f64() * 1e3;

                let (mut mismatches, mut unverifiable) = (local.len() as u64, 0u64);
                for line in local.drain(..) {
                    println!("MISMATCH {line}");
                }
                for r in &results {
                    match r {
                        Check::Ok => {}
                        Check::Mismatch(line) => {
                            mismatches += 1;
                            println!("MISMATCH {line}");
                        }
                        Check::Unverifiable(err) => {
                            if unverifiable == 0 {
                                println!("UNVERIFIABLE block {n}: {err}");
                            }
                            unverifiable += 1;
                        }
                    }
                }
                let span = diff.number - diff.span_start + 1;
                let block_ts = diff.heads.last().map_or(0, |h| h.timestamp);
                let latency_ms = received_ms - block_ts as f64 * 1e3;
                window.blocks += span;
                window.diffs += 1;
                window.keys += results.len() as u64;
                window.mismatches += mismatches;
                window.unverifiable += unverifiable;
                window.bytes_total += bytes as u64;
                if span == 1 {
                    // Folded catch-up spans would skew both distributions;
                    // spool replays arrive late by design.
                    window.bytes_per_block.push(bytes as f64);
                    if !replayed {
                        window.latency_ms.push(latency_ms);
                    }
                }
                window.apply_us.push(apply_us);
                window.check_ms.push(check_ms);
                rss_max = rss_max.max(rss_mib());
                println!(
                    "block {n} span {}..{n}{}: accounts {} slots {} code {} | {bytes} B, +{latency_ms:.0} ms, apply {apply_us:.0} µs, \
                     check {check_ms:.0} ms | keys {} (total {}, mismatches {}, unverifiable {})",
                    diff.span_start,
                    if replayed { " (replay)" } else { "" },
                    diff.accounts.len(),
                    diff.storage.len(),
                    diff.code.len(),
                    results.len(),
                    total.keys + window.keys,
                    total.mismatches + window.mismatches,
                    total.unverifiable + window.unverifiable,
                );
            }
        }
        if start.elapsed() >= next_summary {
            hour += 1;
            println!(
                "{}",
                window.line(&format!("window {hour}"), &store, start.elapsed())
            );
            total.merge(&window);
            window = Window::default();
            next_summary += summary_every;
        }
    }
    total.merge(&window);
    println!("{}", total.line("total", &store, start.elapsed()));
    println!();
    println!("| metric | value |");
    println!("|---|---|");
    println!(
        "| duration | {:.2} h |",
        start.elapsed().as_secs_f64() / 3600.0
    );
    println!("| blocks (diffs) | {} ({}) |", total.blocks, total.diffs);
    println!("| keys checked | {} |", total.keys);
    println!("| mismatches | {} |", total.mismatches);
    println!("| unverifiable keys | {} |", total.unverifiable);
    println!(
        "| reconnects / gaps / reorgs / breaks | {} / {} / {} / {} |",
        total.reconnects, total.gaps, total.reorgs, total.breaks
    );
    println!(
        "| reorgs rolled back (dropped-block keys checked at the fork) | {} ({}) |",
        total.rollbacks, total.rollback_keys
    );
    println!(
        "| re-deliveries after a node restart: overlapping spans / stale diffs | {} / {} |",
        total.overlaps, total.stale
    );
    println!(
        "| block timestamp → diff received, ms p50/p99/max | {} |",
        p3(&total.latency_ms)
    );
    println!(
        "| frame bytes per block p50/p99/max | {} |",
        p3(&total.bytes_per_block)
    );
    println!(
        "| feed volume | {:.1} MiB ({:.0} MiB/day) |",
        total.bytes_total as f64 / (1 << 20) as f64,
        total.bytes_total as f64 / (1 << 20) as f64 / start.elapsed().as_secs_f64().max(1.0)
            * 86400.0
    );
    println!("| store apply µs p50/p99/max | {} |", p3(&total.apply_us));
    println!(
        "| check ms per block p50/p99/max | {} |",
        p3(&total.check_ms)
    );
    let (accounts, slots) = store.len();
    println!("| store at end | {accounts} accounts, {slots} slots |");
    println!("| rss at end / max | {:.0} / {rss_max:.0} MiB |", rss_mib());
    println!();
    println!(
        "done: blocks {}, keys checked {}, mismatches {}",
        total.blocks, total.keys, total.mismatches
    );
    if total.mismatches > 0 {
        std::process::exit(1);
    }
    Ok(())
}
