//! Live check of the warm trie (PLAN D11): follow the feed, apply every diff
//! to a `WarmTrie` that fetches the nodes it lacks over op 4, and compare
//! each recomputed state root with the block header's `stateRoot`.
//!
//! Seeds cold from the first diff's parent header, so the first blocks fetch
//! most of what they touch; later blocks fetch only nodes the trie has not
//! seen. Per block: root match, nodes and bytes fetched, batches and probes,
//! apply time, and the trie's revealed size. A root mismatch or a failed
//! update reseeds from the next diff's parent and is counted. Read-only.
//!
//! Usage: trie-follow [--feed 127.0.0.1:8585] [--lookup 127.0.0.1:8586]
//!                    [--rpc http://127.0.0.1:8545] [--blocks 50]
//!                    [--max-nodes 250000] [--keep-depth 4]

use std::sync::Arc;
use std::time::{Duration, Instant};

use silt_cache::fallback::{hex_word, parse_word};
use silt_cache::lookup::{LookupClient, LookupClientConfig};
use silt_cache::trie::{WarmTrie, WarmTrieConfig};
use silt_wire::{Frame, Word, read_frame, write_frame};

struct Args {
    feed: String,
    lookup: String,
    rpc: String,
    blocks: u64,
    cfg: WarmTrieConfig,
}

fn args() -> eyre::Result<Args> {
    let mut a = Args {
        feed: "127.0.0.1:8585".into(),
        lookup: "127.0.0.1:8586".into(),
        rpc: "http://127.0.0.1:8545".into(),
        blocks: 50,
        cfg: WarmTrieConfig::default(),
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut v = || it.next().ok_or_else(|| eyre::eyre!("{flag} needs a value"));
        match flag.as_str() {
            "--feed" => a.feed = v()?,
            "--lookup" => a.lookup = v()?,
            "--rpc" => a.rpc = v()?,
            "--blocks" => a.blocks = v()?.parse()?,
            "--max-nodes" => a.cfg.max_nodes = v()?.parse()?,
            "--keep-depth" => a.cfg.keep_depth = v()?.parse()?,
            other => eyre::bail!("unknown flag {other}"),
        }
    }
    Ok(a)
}

/// A header's `stateRoot`. Right at the tip the header can lag the feed by
/// a beat, so retry briefly.
async fn state_root(http: &reqwest::Client, rpc: &str, hash: &Word) -> eyre::Result<Word> {
    let body = serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "eth_getBlockByHash",
        "params": [hex_word(hash), false],
    });
    for _ in 0..20 {
        let v: serde_json::Value = http.post(rpc).json(&body).send().await?.json().await?;
        if v["result"]["stateRoot"].is_string() {
            return parse_word(&v["result"]["stateRoot"]);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    eyre::bail!("no header for {}", hex_word(hash))
}

#[tokio::main]
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
    let http = reqwest::Client::new();
    let mut warm = WarmTrie::new(Arc::new(client), args.cfg.clone());

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
    write_frame(&mut feed, &Frame::Subscribe { from: None }).await?;
    println!(
        "trie-follow: feed {} lookup {} rpc {}, {} blocks, {:?}",
        args.feed, args.lookup, args.rpc, args.blocks, args.cfg
    );

    let (mut checked, mut matched, mut failed, mut reseeds) = (0u64, 0u64, 0u64, 0u64);
    let mut ms = Vec::new();
    let mut reseed = true;
    while checked < args.blocks {
        let diff = match read_frame(&mut feed).await? {
            Some(Frame::Diff(diff)) => diff,
            // A reorg or gap shows up as the next diff not continuing.
            Some(Frame::Heartbeat { .. } | Frame::Reorg { .. } | Frame::GapTooOld { .. }) => {
                continue;
            }
            Some(other) => eyre::bail!("unexpected frame {other:?}"),
            None => eyre::bail!("feed closed"),
        };
        if let Some((at, _)) = warm.at()
            && diff.number <= at
        {
            continue; // re-delivered
        }
        let continues = warm.at().is_some_and(|(at, hash)| {
            (diff.span_start == at + 1 && diff.parent_hash == hash)
                || (diff.span_start <= at
                    && diff.heads.iter().any(|h| h.number == at && h.hash == hash))
        });
        if reseed || !continues {
            let root = state_root(&http, &args.rpc, &diff.parent_hash).await?;
            warm.seed(diff.span_start - 1, diff.parent_hash, root);
            reseeds += 1;
            reseed = false;
            println!(
                "seeded cold at {} (root {})",
                diff.span_start - 1,
                hex_word(&root)
            );
        }

        let before = warm.stats().clone();
        let t = Instant::now();
        let got = match warm.apply(&diff).await {
            Ok(root) => root,
            Err(e) => {
                failed += 1;
                checked += 1;
                reseed = true;
                println!("FAILED {}..{}: {e}", diff.span_start, diff.number);
                continue;
            }
        };
        let elapsed = t.elapsed().as_secs_f64() * 1e3;
        let want = state_root(&http, &args.rpc, &diff.hash).await?;
        checked += 1;
        let s = warm.stats();
        let ok = got == want;
        if ok {
            matched += 1;
            ms.push(elapsed);
        } else {
            reseed = true;
        }
        println!(
            "{} {}..{}: accounts {} slots {} | fetched {} nodes {:.1} KiB in {} batch + {} probes | {elapsed:.0} ms | trie {} nodes, {} storage tries{}",
            if ok { "MATCH" } else { "MISMATCH" },
            diff.span_start,
            diff.number,
            diff.accounts.len(),
            diff.storage.len(),
            s.fetched_nodes - before.fetched_nodes,
            (s.fetched_bytes - before.fetched_bytes) as f64 / 1024.0,
            s.batches - before.batches,
            s.probes - before.probes,
            s.nodes,
            s.storage_tries,
            if s.prunes > before.prunes {
                ", pruned"
            } else {
                ""
            },
        );
        if !ok {
            println!("  header {}\n  warm   {}", hex_word(&want), hex_word(&got));
        }
    }
    ms.sort_by(f64::total_cmp);
    let p50 = ms.get(ms.len() / 2).copied().unwrap_or(f64::NAN);
    let s = warm.stats();
    println!(
        "done: {checked} blocks, {matched} roots match, {failed} failed, {reseeds} seeds | apply ms p50 {p50:.0} | \
         last trie: {} nodes, {} storage tries, {} prunes",
        s.nodes, s.storage_tries, s.prunes
    );
    if matched != checked {
        std::process::exit(1);
    }
    Ok(())
}
