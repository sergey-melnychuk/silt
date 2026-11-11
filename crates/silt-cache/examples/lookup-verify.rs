//! Lookup equivalence check (PLAN T4.6, G4.4): every key read over the
//! lookup protocol must equal the node's pinned `eth_*` answer at the same
//! block hash. Zero mismatches or it exits 1.
//!
//! Usage:
//!   lookup-verify [--lookup 127.0.0.1:8587] [--rpc http://127.0.0.1:8545]
//!                 [--accounts 6000] [--slots 3000]
//!
//! Pins one block — the server's "latest" (the read-only harness's state
//! frontier) at start — and checks keys from recent blocks and from ~5000
//! blocks back: balance, nonce and inline code per account, `code_hash`
//! against keccak(code) (`KECCAK_EMPTY` for none), `Absent` accounts against
//! RPC zeros, `Code(code_hash)` against the inline code, and every storage
//! slot. Also checks that an unknown hash answers `UnknownBlock`.

#[path = "common/keys.rs"]
mod keys;

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use silt_cache::fallback::{hex_addr, hex_word};
use silt_cache::lookup::{LookupClient, LookupClientConfig, LookupError};
use silt_cache::{Fallback, HttpFallback};
use silt_wire::lookup::{ItemStatus, LATEST, Status};
use silt_wire::{Addr, KECCAK_EMPTY, Word};

struct Args {
    lookup: String,
    rpc: String,
    accounts: usize,
    slots: usize,
}

fn args() -> eyre::Result<Args> {
    let mut a = Args {
        lookup: "127.0.0.1:8587".into(),
        rpc: "http://127.0.0.1:8545".into(),
        accounts: 6000,
        slots: 3000,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut v = || it.next().ok_or_else(|| eyre::eyre!("{flag} needs a value"));
        match flag.as_str() {
            "--lookup" => a.lookup = v()?,
            "--rpc" => a.rpc = v()?,
            "--accounts" => a.accounts = v()?.parse()?,
            "--slots" => a.slots = v()?.parse()?,
            other => eyre::bail!("unknown flag {other}"),
        }
    }
    Ok(a)
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let args = args()?;
    let client = LookupClient::new(
        LookupClientConfig::new(&args.lookup).with_timeout(Duration::from_secs(10)),
    );
    eyre::ensure!(
        client.wait_connected(Duration::from_secs(10)).await,
        "no lookup server at {}",
        args.lookup
    );
    let head = client.ping(&LATEST).await?;
    let (number, pin) = (head.read_number, head.read_hash);
    println!("pinned at {number} {}", hex_word(&pin));

    let rpc = keys::Rpc::new(&args.rpc);
    let mut accounts: Vec<Addr> = Vec::new();
    let mut slots: Vec<(Addr, Word)> = Vec::new();
    for top in [number, number.saturating_sub(5000)] {
        let k = keys::collect(&rpc, top, 400, args.accounts / 2, args.slots / 2).await?;
        println!(
            "keys from blocks {}..={}: {} accounts, {} slots",
            k.first_block,
            k.last_block,
            k.accounts.len(),
            k.slots.len()
        );
        accounts.extend(k.accounts.into_iter().take(args.accounts / 2));
        slots.extend(k.slots.into_iter().take(args.slots / 2));
    }
    accounts.sort();
    accounts.dedup();
    slots.sort();
    slots.dedup();

    let http = Arc::new(HttpFallback::new(&args.rpc));
    let mut mismatches = 0u64;
    let mut checked = 0u64;

    // Accounts: lookup in frames of 64 with inline code; RPC per account.
    let mut lookup_accounts = Vec::with_capacity(accounts.len());
    for chunk in accounts.chunks(64) {
        let reply = client.accounts(&pin, chunk, true).await?;
        eyre::ensure!(
            reply.read_hash == pin && reply.read_number == number,
            "read at another block"
        );
        lookup_accounts.extend(chunk.iter().copied().zip(reply.items));
    }
    let rpc_accounts: Vec<_> = futures::stream::iter(accounts.iter().copied())
        .map(|a| {
            let http = http.clone();
            async move { (a, http.account_at(&a, &pin).await) }
        })
        .buffered(32)
        .collect()
        .await;
    let (mut absent, mut contracts, mut item_errors) = (0, 0, 0);
    let mut code_hashes: Vec<(Word, Vec<u8>)> = Vec::new();
    for ((addr, got), (_, want)) in lookup_accounts.iter().zip(&rpc_accounts) {
        let want = want
            .as_ref()
            .map_err(|e| eyre::eyre!("rpc account {}: {e}", hex_addr(addr)))?;
        checked += 1;
        let code = got.code.clone().unwrap_or_default();
        let expect_hash = if want.code.is_empty() {
            KECCAK_EMPTY
        } else {
            keys::keccak(&want.code)
        };
        let mut bad = Vec::new();
        match got.status {
            ItemStatus::Error => {
                item_errors += 1;
                bad.push("item Error");
            }
            ItemStatus::Absent => {
                absent += 1;
                if want.balance != [0; 32] || want.nonce != 0 || !want.code.is_empty() {
                    bad.push("Absent but RPC has state");
                }
            }
            ItemStatus::Ok => {}
        }
        if got.balance != want.balance {
            bad.push("balance");
        }
        if got.nonce != want.nonce {
            bad.push("nonce");
        }
        if code != want.code {
            bad.push("code");
        }
        if got.code_hash != expect_hash {
            bad.push("code_hash");
        }
        if !bad.is_empty() {
            mismatches += 1;
            println!(
                "MISMATCH account {} {bad:?}: lookup bal {} nonce {} code {}B | rpc bal {} nonce {} code {}B",
                hex_addr(addr),
                hex_word(&got.balance),
                got.nonce,
                code.len(),
                hex_word(&want.balance),
                want.nonce,
                want.code.len()
            );
        }
        if got.code_hash != KECCAK_EMPTY {
            contracts += 1;
            code_hashes.push((got.code_hash, code));
        }
    }
    code_hashes.sort();
    code_hashes.dedup_by(|a, b| a.0 == b.0);

    // Code by hash must equal the inline code.
    let mut code_checked = 0;
    for chunk in code_hashes.chunks(64) {
        let hashes: Vec<Word> = chunk.iter().map(|(h, _)| *h).collect();
        let reply = client.code(&pin, &hashes).await?;
        for ((hash, inline), got) in chunk.iter().zip(&reply.items) {
            code_checked += 1;
            if got.status != ItemStatus::Ok || got.code != *inline {
                mismatches += 1;
                println!(
                    "MISMATCH code {}: {:?}, {}B vs inline {}B",
                    hex_word(hash),
                    got.status,
                    got.code.len(),
                    inline.len()
                );
            }
        }
    }

    // Storage: lookup in frames of 1024; RPC per slot.
    let mut lookup_slots = Vec::with_capacity(slots.len());
    for chunk in slots.chunks(1024) {
        let reply = client.storage(&pin, chunk).await?;
        lookup_slots.extend(reply.items);
    }
    let rpc_slots: Vec<_> = futures::stream::iter(slots.iter().copied())
        .map(|(a, s)| {
            let http = http.clone();
            async move { http.storage_at(&a, &s, &pin).await }
        })
        .buffered(32)
        .collect()
        .await;
    let mut nonzero = 0;
    for (((addr, slot), got), want) in slots.iter().zip(&lookup_slots).zip(&rpc_slots) {
        let want = want.as_ref().map_err(|e| eyre::eyre!("rpc storage: {e}"))?;
        checked += 1;
        nonzero += (got.value != [0; 32]) as u64;
        if got.status != ItemStatus::Ok || got.value != *want {
            mismatches += 1;
            println!(
                "MISMATCH storage {} {}: lookup {:?} {} | rpc {}",
                hex_addr(addr),
                hex_word(slot),
                got.status,
                hex_word(&got.value),
                hex_word(want)
            );
        }
    }

    // An unknown hash must answer UnknownBlock, never data.
    let unknown = client
        .storage(&[0x5A; 32], &slots[..1.min(slots.len())])
        .await;
    let unknown_ok = unknown == Err(LookupError::Status(Status::UnknownBlock));
    if !unknown_ok {
        mismatches += 1;
        println!("MISMATCH unknown pin answered {unknown:?}");
    }

    println!(
        "done at block {number}: {} accounts ({contracts} with code, {absent} absent, {item_errors} item errors), \
         {code_checked} code hashes, {} slots ({nonzero} non-zero); keys checked {checked}; \
         unknown pin -> UnknownBlock: {unknown_ok}; mismatches {mismatches}",
        lookup_accounts.len(),
        lookup_slots.len()
    );
    if mismatches > 0 {
        std::process::exit(1);
    }
    Ok(())
}
