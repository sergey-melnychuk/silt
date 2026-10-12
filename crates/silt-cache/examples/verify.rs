//! Live equivalence verifier (PLAN T3.1): subscribe to a silt feed, apply
//! every diff, and check EVERY diff-applied storage slot and account
//! balance/nonce against pinned RPC reads at that block. Zero mismatches
//! or it prints them.
//!
//! Usage: verify <feed-addr> <rpc-url> [--blocks N]

use silt_cache::fallback::{Fallback, HttpFallback, hex_addr, hex_word};
use silt_wire::{Frame, read_frame, write_frame};

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let mut args = std::env::args().skip(1);
    let feed = args.next().unwrap_or_else(|| "127.0.0.1:8585".into());
    let rpc_url = args
        .next()
        .unwrap_or_else(|| "http://127.0.0.1:8545".into());
    let max_blocks: u64 = match (args.next().as_deref(), args.next()) {
        (Some("--blocks"), Some(n)) => n.parse()?,
        _ => u64::MAX,
    };
    let rpc = HttpFallback::new(rpc_url);

    let mut stream = tokio::net::TcpStream::connect(&feed).await?;
    let hello = read_frame(&mut stream)
        .await?
        .ok_or_else(|| eyre::eyre!("eof"))?;
    let Frame::Hello {
        chain_id,
        genesis_hash,
        tip,
        ..
    } = hello
    else {
        eyre::bail!("expected Hello");
    };
    println!("feed: chain_id={chain_id} tip={tip}");
    write_frame(&mut stream, &silt_wire::hello(chain_id, genesis_hash, 0)).await?;
    write_frame(&mut stream, &Frame::Subscribe { from: None }).await?;

    let (mut seen, mut checked, mut mismatches) = (0u64, 0u64, 0u64);
    while seen < max_blocks {
        let Some(frame) = read_frame(&mut stream).await? else {
            break;
        };
        match frame {
            Frame::Diff(diff) => {
                seen += 1;
                let pin = &diff.hash;
                let mut block_checked = 0u64;
                for (addr, slot, value) in &diff.storage {
                    let rpc_value = rpc.storage_at(addr, slot, pin).await?;
                    block_checked += 1;
                    if rpc_value != *value {
                        mismatches += 1;
                        println!(
                            "MISMATCH block {} {} slot {}: feed {} rpc {}",
                            diff.number,
                            hex_addr(addr),
                            hex_word(slot),
                            hex_word(value),
                            hex_word(&rpc_value)
                        );
                    }
                }
                for (addr, change) in &diff.accounts {
                    let Some(change) = change else { continue };
                    let acct = rpc.account_at(addr, pin).await?;
                    block_checked += 1;
                    if acct.balance != change.balance || acct.nonce != change.nonce {
                        mismatches += 1;
                        println!(
                            "MISMATCH block {} account {}: feed bal {} nonce {} rpc bal {} nonce {}",
                            diff.number,
                            hex_addr(addr),
                            hex_word(&change.balance),
                            change.nonce,
                            hex_word(&acct.balance),
                            acct.nonce
                        );
                    }
                }
                checked += block_checked;
                println!(
                    "block {} span {}..{}: accounts {} slots {} receipts {} | checked {} (total {}, mismatches {})",
                    diff.number,
                    diff.span_start,
                    diff.number,
                    diff.accounts.len(),
                    diff.storage.len(),
                    diff.receipts.iter().map(|(_, _, r)| r.len()).sum::<usize>(),
                    block_checked,
                    checked,
                    mismatches
                );
            }
            Frame::Reorg { new_tip_number, .. } => println!("REORG -> {new_tip_number}"),
            Frame::Heartbeat {
                height,
                spool_depth,
            } => {
                println!("heartbeat height={height} spool={spool_depth}")
            }
            Frame::GapTooOld { oldest_available } => println!("gap: oldest {oldest_available}"),
            other => eyre::bail!("unexpected frame: {other:?}"),
        }
    }
    println!("done: blocks {seen}, keys checked {checked}, mismatches {mismatches}");
    if mismatches > 0 {
        std::process::exit(1);
    }
    Ok(())
}
