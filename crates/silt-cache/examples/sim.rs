//! Simulation latency on silt (PLAN M6, side A, standalone): simulate each
//! new block's transactions on the parent state held in a silt-cache
//! `Cache`, at the head, and write one JSON line per simulation.
//!
//! It pairs with the in-node baseline (T6.4), which simulates the same
//! blocks inside the node at the same moments; the two outputs join on
//! (block, tx). Like `replay`, the tool feeds its own store and applies
//! diff N+1 only after block N+1's simulations ran on state N. Misses take
//! the lookup fast path pinned at N, with JSON-RPC as the fallback.
//!
//! Per block: every transaction alone (`sim_tx`), then the block as one
//! bundle (`sim_bundle`). Fetching the block's transactions happens before
//! the clocks start. Read-only against the node.
//!
//! Usage: sim [--feed 127.0.0.1:8585] [--lookup 127.0.0.1:8586]
//!            [--rpc http://127.0.0.1:8545] [--blocks 300] [--out sim-a.jsonl]

use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use silt_cache::Cache;
use silt_cache::client::{Continuity, continuity};
use silt_cache::fallback::{Fallback, HttpFallback};
use silt_cache::lookup::{LookupClientConfig, LookupFallback};
use silt_cache::sim::{Counted, Outcome, Reads, sim_bundle, sim_tx};
use silt_cache::store::Store;
use silt_wire::{Frame, read_frame, write_frame};
use yevm::Chain;
use yevm::core::call::Block;

struct Args {
    feed: String,
    lookup: String,
    rpc: String,
    blocks: u64,
    out: String,
}

fn args() -> eyre::Result<Args> {
    let mut a = Args {
        feed: "127.0.0.1:8585".into(),
        lookup: "127.0.0.1:8586".into(),
        rpc: "http://127.0.0.1:8545".into(),
        blocks: 300,
        out: "sim-a.jsonl".into(),
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut v = || it.next().ok_or_else(|| eyre::eyre!("{flag} needs a value"));
        match flag.as_str() {
            "--feed" => a.feed = v()?,
            "--lookup" => a.lookup = v()?,
            "--rpc" => a.rpc = v()?,
            "--blocks" => a.blocks = v()?.parse()?,
            "--out" => a.out = v()?,
            other => eyre::bail!("unknown flag {other}"),
        }
    }
    Ok(a)
}

fn reads_json(r: &Reads) -> serde_json::Value {
    serde_json::json!({"acc": r.accounts, "slot": r.slots, "other": r.other, "us": r.nanos as f64 / 1e3})
}

/// Store hits, misses and prefetched entries so far.
fn store_counts(store: &Store) -> (u64, u64, u64) {
    (
        store.stats.hits.load(Ordering::Relaxed),
        store.stats.misses.load(Ordering::Relaxed),
        store.stats.prefetched.load(Ordering::Relaxed),
    )
}

/// Simulates block `n` on the store's state (its parent): every transaction
/// alone, then the bundle. Returns the number of transactions simulated.
async fn sim_block(
    n: u64,
    seq: u64,
    chain: &Counted<Arc<Cache>>,
    store: &Store,
    out: &mut impl Write,
) -> eyre::Result<usize> {
    let block: Block = Chain::block(&*chain.inner, n).await?;
    let (tip, _) = store.tip().ok_or_else(|| eyre::eyre!("cold store"))?;
    eyre::ensure!(
        tip + 1 == n,
        "block {n} does not build on the store's tip {tip}"
    );
    let chain_id = Chain::chain_id(&*chain.inner).await?;
    let head = &block.head;
    let started = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis() as u64;

    for (i, tx) in block.txs.iter().enumerate() {
        let (h0, m0, p0) = store_counts(store);
        let sim = sim_tx(tx, head, chain_id, chain).await?;
        let (h1, m1, p1) = store_counts(store);
        let line = serde_json::json!({
            "side": "silt", "kind": "tx", "block": n, "seq": seq, "i": i,
            "hash": tx.tx.hash.to_string(), "us": sim.elapsed.as_secs_f64() * 1e6,
            "outcome": sim.outcome.to_string(), "failed": matches!(sim.outcome, Outcome::Failed(_)),
            "reads": reads_json(&sim.reads), "hits": h1 - h0, "misses": m1 - m0, "prefetched": p1 - p0,
        });
        writeln!(out, "{line}")?;
    }

    let (h0, m0, p0) = store_counts(store);
    let (outcomes, elapsed, reads) = sim_bundle(&block.txs, head, chain_id, chain).await?;
    let (h1, m1, p1) = store_counts(store);
    let failed = outcomes
        .iter()
        .filter(|o| matches!(o, Outcome::Failed(_)))
        .count();
    let digest = silt_cache::sim::fnv1a(
        outcomes
            .iter()
            .map(|o| o.to_string())
            .collect::<Vec<_>>()
            .join("|")
            .as_bytes(),
    );
    let line = serde_json::json!({
        "side": "silt", "kind": "bundle", "block": n, "seq": seq, "txs": block.txs.len(),
        "us": elapsed.as_secs_f64() * 1e6, "failed": failed, "digest": format!("{digest:016x}"),
        "reads": reads_json(&reads), "hits": h1 - h0, "misses": m1 - m0, "prefetched": p1 - p0, "started_ms": started,
    });
    writeln!(out, "{line}")?;
    out.flush()?;
    println!(
        "block {n} (#{seq}): {} txs, bundle {:.1} ms, {failed} failed, reads {} ({:.1} ms), store hits {} misses {}",
        block.txs.len(),
        elapsed.as_secs_f64() * 1e3,
        reads.total(),
        reads.nanos as f64 / 1e6,
        h1 - h0,
        m1 - m0
    );
    Ok(block.txs.len())
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
    let chain = Counted::new(Cache::from_store(store.clone(), fallback));
    let mut out = std::io::BufWriter::new(std::fs::File::create(&args.out)?);

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
    write_frame(
        &mut feed,
        &Frame::Subscribe {
            from: None,
            parent_hash: None,
        },
    )
    .await?;
    println!(
        "sim (silt): feed {} lookup {} rpc {}, {} blocks, out {}",
        args.feed, args.lookup, args.rpc, args.blocks, args.out
    );

    let (mut seq, mut txs, mut errors) = (0u64, 0usize, 0u64);
    while seq < args.blocks {
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
                seq += 1;
                match sim_block(diff.number, seq, &chain, &store, &mut out).await {
                    Ok(n) => txs += n,
                    Err(e) => {
                        errors += 1;
                        println!("ERROR block {}: {e}", diff.number);
                    }
                }
            }
            Continuity::Extends | Continuity::Overlaps => {
                println!("applying {}..{} without sims", diff.span_start, diff.number);
            }
        }
        store.apply(&diff);
        if diff.number.is_multiple_of(8) {
            store.evict(diff.number, silt_cache::DEFAULT_EVICT_AFTER_BLOCKS);
        }
    }
    println!("done: {seq} blocks, {txs} txs simulated, {errors} block errors");
    Ok(())
}
