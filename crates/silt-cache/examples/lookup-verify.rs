//! Lookup equivalence check (PLAN T4.6, G4.4): every key read over the
//! lookup protocol must equal the node's pinned `eth_*` answer at the same
//! block hash. Zero mismatches or it exits 1.
//!
//! Usage:
//!   lookup-verify [--lookup 127.0.0.1:8587] [--rpc http://127.0.0.1:8545]
//!                 [--accounts 6000] [--slots 3000] [--proofs 120]
//!
//! Pins one block — the server's "latest" (the read-only harness's state
//! frontier) at start — and checks keys from recent blocks and from ~5000
//! blocks back: balance, nonce and inline code per account, `code_hash`
//! against keccak(code) (`KECCAK_EMPTY` for none), `Absent` accounts against
//! RPC zeros, `Code(code_hash)` against the inline code, and every storage
//! slot. Also checks that an unknown hash answers `UnknownBlock`.
//!
//! Proofs (D10): op-4 proofs for `--proofs` accounts (with their sampled
//! slots) at the node's **head** — reth answers `eth_getProof` only there
//! (default proof window 0), so it waits for the lookup server to reach
//! the head and retries when a new block lands mid-round. Every node of
//! every `accountProof` and `storageProof` must appear in the op-4 answer,
//! each `storageHash` must equal its op-4 storage root, and keccak(op-4
//! root node) must equal the header's `stateRoot`.

#[path = "common/keys.rs"]
mod keys;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use silt_cache::fallback::{hex_addr, hex_word, parse_bytes, parse_u64, parse_word};
use silt_cache::lookup::{LookupClient, LookupClientConfig, LookupError};
use silt_cache::{Fallback, HttpFallback};
use silt_wire::lookup::{ItemStatus, LATEST, Status};
use silt_wire::{Addr, KECCAK_EMPTY, Word};

struct Args {
    lookup: String,
    rpc: String,
    accounts: usize,
    slots: usize,
    proofs: usize,
}

fn args() -> eyre::Result<Args> {
    let mut a = Args {
        lookup: "127.0.0.1:8587".into(),
        rpc: "http://127.0.0.1:8545".into(),
        accounts: 6000,
        slots: 3000,
        proofs: 120,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut v = || it.next().ok_or_else(|| eyre::eyre!("{flag} needs a value"));
        match flag.as_str() {
            "--lookup" => a.lookup = v()?,
            "--rpc" => a.rpc = v()?,
            "--accounts" => a.accounts = v()?.parse()?,
            "--slots" => a.slots = v()?.parse()?,
            "--proofs" => a.proofs = v()?.parse()?,
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

    let proofs = if args.proofs > 0 {
        let p = verify_proofs(&client, &rpc, &accounts, &slots, args.proofs).await?;
        mismatches += p.mismatches;
        format!(
            "; proofs at head {}: {} accounts, {} slots, {} account-proof nodes and {} storage-proof nodes found in op 4, root ok: {}",
            p.head, p.accounts, p.slots, p.account_nodes, p.storage_nodes, p.root_ok
        )
    } else {
        String::new()
    };

    println!(
        "done at block {number}: {} accounts ({contracts} with code, {absent} absent, {item_errors} item errors), \
         {code_checked} code hashes, {} slots ({nonzero} non-zero); keys checked {checked}; \
         unknown pin -> UnknownBlock: {unknown_ok}{proofs}; mismatches {mismatches}",
        lookup_accounts.len(),
        lookup_slots.len()
    );
    if mismatches > 0 {
        std::process::exit(1);
    }
    Ok(())
}

struct ProofCheck {
    head: u64,
    accounts: usize,
    slots: usize,
    account_nodes: usize,
    storage_nodes: usize,
    root_ok: bool,
    mismatches: u64,
}

/// See the module docs: op 4 vs `eth_getProof`, both at the node's head.
async fn verify_proofs(
    client: &LookupClient,
    rpc: &keys::Rpc,
    accounts: &[Addr],
    slots: &[(Addr, Word)],
    n: usize,
) -> eyre::Result<ProofCheck> {
    // n accounts, plus up to n slots spread over contracts (≤ 2 each; the
    // slot list is sorted, so its head would cluster on a few contracts).
    let mut sample: BTreeMap<Addr, Vec<Word>> = BTreeMap::new();
    let mut taken = 0;
    for (a, s) in slots {
        let v = sample.entry(*a).or_default();
        if v.len() < 2 {
            v.push(*s);
            taken += 1;
            if taken == n {
                break;
            }
        }
    }
    for a in accounts.iter().take(n) {
        sample.entry(*a).or_default();
    }
    let sample: Vec<(Addr, Vec<Word>)> = sample.into_iter().collect();
    let targets: Vec<(Word, Vec<Word>)> = sample
        .iter()
        .map(|(a, s)| (keys::keccak(a), s.iter().map(|s| keys::keccak(s)).collect()))
        .collect();

    for attempt in 1..=20 {
        let head = rpc
            .call("eth_getBlockByNumber", serde_json::json!(["latest", false]))
            .await?;
        let hash = parse_word(&head["hash"])?;
        let number = parse_u64(&head["number"])?;
        let state_root = parse_word(&head["stateRoot"])?;
        let frame = match client.proof(&hash, &targets).await {
            Ok(frame) => frame,
            // The read-only server's frontier trails the node's head by a
            // persist; wait for it.
            Err(LookupError::Status(Status::UnknownBlock)) => {
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
            Err(err) => eyre::bail!("op 4 at head {number}: {err}"),
        };
        let answers: Vec<eyre::Result<serde_json::Value>> = futures::stream::iter(&sample)
            .map(|(a, s)| {
                let keys: Vec<String> = s.iter().map(hex_word).collect();
                let at = serde_json::json!({ "blockHash": hex_word(&hash) });
                rpc.call("eth_getProof", serde_json::json!([hex_addr(a), keys, at]))
            })
            .buffered(16)
            .collect()
            .await;
        if let Some(Err(err)) = answers.iter().find(|r| r.is_err()) {
            // Typically a new block landed: the node proves only its head.
            println!("proof round {attempt} at {number}: eth_getProof failed ({err}); retrying");
            continue;
        }

        let p = frame.to_read();
        let account_nodes: HashSet<&[u8]> =
            p.account_nodes.iter().map(|n| n.rlp.as_slice()).collect();
        let root_ok = p
            .account_nodes
            .iter()
            .find(|n| n.path.is_empty())
            .is_some_and(|n| keys::keccak(&n.rlp) == state_root);
        let storages: HashMap<Word, (Word, HashSet<&[u8]>)> = p
            .storages
            .iter()
            .map(|st| {
                let nodes = st.nodes.iter().map(|n| n.rlp.as_slice()).collect();
                (st.hashed_address, (st.storage_root, nodes))
            })
            .collect();
        let mut check = ProofCheck {
            head: number,
            accounts: sample.len(),
            slots: sample.iter().map(|(_, s)| s.len()).sum(),
            account_nodes: 0,
            storage_nodes: 0,
            root_ok,
            mismatches: (!root_ok) as u64,
        };
        for ((addr, slots), answer) in sample.iter().zip(answers) {
            let answer = answer?;
            let mut bad = Vec::new();
            for node in answer["accountProof"].as_array().into_iter().flatten() {
                check.account_nodes += 1;
                if !account_nodes.contains(parse_bytes(node)?.as_slice()) {
                    bad.push("account node missing");
                }
            }
            if !slots.is_empty() {
                match storages.get(&keys::keccak(addr)) {
                    None => bad.push("no storage trie"),
                    Some((root, nodes)) => {
                        if parse_word(&answer["storageHash"])? != *root {
                            bad.push("storage root");
                        }
                        for sp in answer["storageProof"].as_array().into_iter().flatten() {
                            for node in sp["proof"].as_array().into_iter().flatten() {
                                check.storage_nodes += 1;
                                if !nodes.contains(parse_bytes(node)?.as_slice()) {
                                    bad.push("storage node missing");
                                }
                            }
                        }
                    }
                }
            }
            if !bad.is_empty() {
                check.mismatches += 1;
                println!("MISMATCH proof {} {bad:?}", hex_addr(addr));
            }
        }
        return Ok(check);
    }
    eyre::bail!("no proof round completed: the lookup server never reached the node's head")
}
