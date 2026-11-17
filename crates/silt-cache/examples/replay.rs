//! Replay check for the yevm adapter (PLAN M2 gate): run every transaction
//! of each new block through yevm's `Executor` on a silt-cache `Cache` that
//! holds the parent state, and compare with the node's receipts: status, gas
//! used, and the created address for deployments.
//!
//! The tool feeds its own store from the feed, so it can hold diff N back
//! while block N executes on state N-1. Misses take the lookup fast path
//! pinned at N-1's hash, with JSON-RPC as the fallback. Then diff N is
//! applied and the next block replays. Folded spans (catch-up after a node
//! restart) are applied without a replay. A reorg rolls the store back to
//! its fork (PLAN D12) and the new side replays from there; a break, or a
//! reorg it can't roll back, drops the store to cold and the next diff
//! reseeds it. Read-only against the node.
//!
//! Usage: replay [--feed 127.0.0.1:8585] [--lookup 127.0.0.1:8586]
//!               [--rpc http://127.0.0.1:8545] [--blocks 20]

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

use silt_cache::Cache;
use silt_cache::client::{Continuity, continuity};
use silt_cache::fallback::{Fallback, HttpFallback, hex_word, parse_u64};
use silt_cache::lookup::{LookupClientConfig, LookupFallback};
use silt_cache::store::Store;
use silt_wire::{Frame, read_frame, write_frame};
use yevm::core::call::{Block, TxFull};
use yevm::core::exe::{post_block, pre_block};
use yevm::{CallResult, Chain, Executor, State};

struct Args {
    feed: String,
    lookup: String,
    rpc: String,
    blocks: u64,
}

fn args() -> eyre::Result<Args> {
    let mut a = Args {
        feed: "127.0.0.1:8585".into(),
        lookup: "127.0.0.1:8586".into(),
        rpc: "http://127.0.0.1:8545".into(),
        blocks: 20,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut v = || it.next().ok_or_else(|| eyre::eyre!("{flag} needs a value"));
        match flag.as_str() {
            "--feed" => a.feed = v()?,
            "--lookup" => a.lookup = v()?,
            "--rpc" => a.rpc = v()?,
            "--blocks" => a.blocks = v()?.parse()?,
            other => eyre::bail!("unknown flag {other}"),
        }
    }
    Ok(a)
}

async fn rpc_call(
    http: &reqwest::Client,
    url: &str,
    method: &str,
    params: serde_json::Value,
) -> eyre::Result<serde_json::Value> {
    let body = serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params});
    let v: serde_json::Value = http.post(url).json(&body).send().await?.json().await?;
    if let Some(err) = v.get("error") {
        eyre::bail!("{method}: {err}");
    }
    Ok(v["result"].clone())
}

#[derive(Default)]
struct Tally {
    blocks: u64,
    txs: u64,
    mismatched: u64,
    errors: u64,
    hits: u64,
    misses: u64,
    exec_ms: Vec<f64>,
}

/// Runs block `n` on the store's state (its parent) and checks every
/// transaction against the node's receipts. Returns mismatched + failed txs.
async fn replay_block(
    n: u64,
    cache: &Cache,
    store: &Store,
    http: &reqwest::Client,
    rpc: &str,
    tally: &mut Tally,
) -> eyre::Result<u64> {
    let block: Block = Chain::block(cache, n).await?;
    let head = block.head.clone();
    let (tip, tip_hash) = store.tip().ok_or_else(|| eyre::eyre!("cold store"))?;
    eyre::ensure!(
        tip + 1 == n
            && head
                .parent_hash
                .to_string()
                .eq_ignore_ascii_case(&hex_word(&tip_hash)),
        "block {n} does not build on the store's tip {tip}"
    );
    let receipts = rpc_call(
        http,
        rpc,
        "eth_getBlockReceipts",
        serde_json::json!([format!("0x{n:x}")]),
    )
    .await?;
    let receipts = receipts.as_array().cloned().unwrap_or_default();
    eyre::ensure!(
        receipts.len() == block.txs.len(),
        "block {n}: {} receipts for {} transactions",
        receipts.len(),
        block.txs.len()
    );

    let (hits0, misses0) = (
        store.stats.hits.load(Ordering::Relaxed),
        store.stats.misses.load(Ordering::Relaxed),
    );
    let t = Instant::now();
    let mut state = yevm::Cache::new();
    state.set_chain_id(Chain::chain_id(cache).await?);
    pre_block(&head, &mut state, cache).await?;
    let mut bad = 0u64;
    for (i, (TxFull { tx, call }, receipt)) in block.txs.into_iter().zip(&receipts).enumerate() {
        let hash = tx.hash.to_string();
        let want_hash = receipt["transactionHash"].as_str().unwrap_or_default();
        eyre::ensure!(
            hash.eq_ignore_ascii_case(want_hash),
            "block {n} tx {i}: receipt order"
        );
        let want_status = parse_u64(&receipt["status"])?;
        let want_gas = parse_u64(&receipt["gasUsed"])? as i64;
        let want_created = receipt["contractAddress"].as_str().map(str::to_lowercase);

        let mut exe = Executor::new(call.into());
        state.reset();
        let problem = match exe.run(&tx, &head, &mut state, cache).await {
            Err(e) => {
                tally.errors += 1;
                Some(format!("execution failed: {e}"))
            }
            Ok(CallResult::Done { status, gas, .. }) => {
                let status = status.as_u8() as u64;
                (status != want_status || gas.finalized != want_gas).then(|| {
                    format!(
                        "status {status} gas {} | receipt status {want_status} gas {want_gas}",
                        gas.finalized
                    )
                })
            }
            Ok(CallResult::Created { acc, gas, .. }) => {
                let acc = acc.to_string().to_lowercase();
                (want_status != 1 || gas.finalized != want_gas || Some(&acc) != want_created.as_ref())
                    .then(|| {
                        format!(
                            "created {acc} gas {} | receipt status {want_status} created {want_created:?} gas {want_gas}",
                            gas.finalized
                        )
                    })
            }
        };
        if let Some(problem) = problem {
            bad += 1;
            println!("MISMATCH block {n} tx {i} {hash}: {problem}");
        }
        tally.txs += 1;
    }
    post_block(&block.withdrawals, &mut state, cache).await?;
    let ms = t.elapsed().as_secs_f64() * 1e3;
    let hits = store.stats.hits.load(Ordering::Relaxed) - hits0;
    let misses = store.stats.misses.load(Ordering::Relaxed) - misses0;
    println!(
        "block {n}: {} txs, {} ok, {bad} mismatched | {ms:.0} ms, store hits {hits}, misses {misses}",
        receipts.len(),
        receipts.len() as u64 - bad
    );
    tally.blocks += 1;
    tally.mismatched += bad;
    tally.hits += hits;
    tally.misses += misses;
    tally.exec_ms.push(ms);
    Ok(bad)
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let args = args()?;
    let rpc: Arc<dyn Fallback> = Arc::new(HttpFallback::new(args.rpc.clone()));
    let fallback: Arc<dyn Fallback> = Arc::new(LookupFallback::new(
        LookupClientConfig::new(&args.lookup),
        rpc,
    ));
    let store = Arc::new(Store::new(256));
    let cache = Cache::from_store(store.clone(), fallback);
    let http = reqwest::Client::new();

    let mut feed = tokio::net::TcpStream::connect(&args.feed).await?;
    let Some(Frame::Hello {
        chain_id,
        genesis_hash,
        ..
    }) = read_frame(&mut feed).await?
    else {
        eyre::bail!("expected Hello");
    };
    write_frame(&mut feed, &silt_wire::hello(chain_id, genesis_hash, 0)).await?;
    let subscribe = Frame::Subscribe {
        from: None,
        parent_hash: None,
    };
    write_frame(&mut feed, &subscribe).await?;
    println!(
        "replay: feed {} lookup {} rpc {}, {} blocks",
        args.feed, args.lookup, args.rpc, args.blocks
    );

    let mut tally = Tally::default();
    while tally.blocks < args.blocks {
        let diff = match read_frame(&mut feed).await? {
            Some(Frame::Diff(diff)) => diff,
            Some(Frame::Heartbeat { .. }) => continue,
            Some(Frame::Reorg {
                fork_number,
                fork_hash,
            }) => {
                if store.rollback(fork_number, fork_hash) {
                    println!("reorg: rolled back to {fork_number}");
                } else {
                    println!("reorg to {fork_number}: can't roll back; store dropped to cold");
                    store.clear();
                }
                continue;
            }
            Some(Frame::GapTooOld { .. }) => {
                println!("gap: store dropped to cold");
                store.clear();
                continue;
            }
            Some(other) => eyre::bail!("unexpected frame {other:?}"),
            None => eyre::bail!("feed closed"),
        };
        let seeded = store.tip().is_some();
        match continuity(&store, &diff) {
            Continuity::Stale => continue,
            Continuity::Break => {
                println!("break at {}: store dropped to cold", diff.number);
                store.clear();
            }
            Continuity::Extends if seeded && diff.span_start == diff.number => {
                if let Err(e) =
                    replay_block(diff.number, &cache, &store, &http, &args.rpc, &mut tally).await
                {
                    tally.errors += 1;
                    println!("ERROR block {}: {e}", diff.number);
                }
            }
            Continuity::Extends | Continuity::Overlaps => {
                println!(
                    "applying {}..{} without replay",
                    diff.span_start, diff.number
                );
            }
        }
        store.apply(&diff);
    }

    let mut ms = tally.exec_ms.clone();
    ms.sort_by(f64::total_cmp);
    let p50 = ms.get(ms.len() / 2).copied().unwrap_or(f64::NAN);
    let max = ms.last().copied().unwrap_or(f64::NAN);
    println!(
        "done: {} blocks, {} txs, {} mismatched, {} errors | block replay ms p50 {p50:.0} max {max:.0} | \
         store hits {} misses {}",
        tally.blocks, tally.txs, tally.mismatched, tally.errors, tally.hits, tally.misses
    );
    if tally.mismatched > 0 || tally.errors > 0 {
        std::process::exit(1);
    }
    Ok(())
}
