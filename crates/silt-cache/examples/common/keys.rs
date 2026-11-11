//! Real keys from recent mainnet blocks via the node's RPC (shared by the
//! `lookup-bench` and `lookup-verify` examples): transaction senders and
//! recipients, log emitters (where receipts are still retained) and
//! access-list storage keys.

#![allow(dead_code)]

use std::collections::BTreeSet;

use silt_cache::fallback::{parse_u64, parse_word};
use silt_wire::{Addr, Word};

pub struct Rpc {
    url: String,
    http: reqwest::Client,
}

impl Rpc {
    pub fn new(url: &str) -> Self {
        Self {
            url: url.into(),
            http: reqwest::Client::new(),
        }
    }

    pub async fn call(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> eyre::Result<serde_json::Value> {
        let body =
            serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params});
        let resp: serde_json::Value = self
            .http
            .post(&self.url)
            .json(&body)
            .send()
            .await?
            .json()
            .await?;
        if let Some(err) = resp.get("error") {
            eyre::bail!("{method}: {err}");
        }
        Ok(resp
            .get("result")
            .cloned()
            .unwrap_or(serde_json::Value::Null))
    }

    pub async fn block_number(&self) -> eyre::Result<u64> {
        parse_u64(&self.call("eth_blockNumber", serde_json::json!([])).await?)
    }

    pub async fn block_hash(&self, number: u64) -> eyre::Result<Word> {
        let b = self
            .call(
                "eth_getBlockByNumber",
                serde_json::json!([format!("0x{number:x}"), false]),
            )
            .await?;
        parse_word(&b["hash"])
    }
}

fn addr(v: &serde_json::Value) -> Option<Addr> {
    let w = parse_word(v).ok()?;
    Some(w[12..].try_into().expect("20 bytes"))
}

#[derive(Default, Clone, Copy, Debug)]
pub struct Yield {
    pub blocks: u64,
    pub senders_recipients: usize,
    pub log_emitters: usize,
    pub access_list_slots: usize,
}

pub struct Keys {
    pub accounts: Vec<Addr>,
    pub slots: Vec<(Addr, Word)>,
    pub first_block: u64,
    pub last_block: u64,
    pub source: Yield,
}

/// Scans blocks downward from `top` until `want_accounts` and `want_slots`
/// are met (or `max_blocks` scanned). Deterministic order (sorted), so a
/// run is repeatable for a given block range.
pub async fn collect(
    rpc: &Rpc,
    top: u64,
    max_blocks: u64,
    want_accounts: usize,
    want_slots: usize,
) -> eyre::Result<Keys> {
    let mut accounts = BTreeSet::new();
    let mut slots = BTreeSet::new();
    let mut source = Yield::default();
    let mut n = top;
    while source.blocks < max_blocks && (accounts.len() < want_accounts || slots.len() < want_slots)
    {
        let block = rpc
            .call(
                "eth_getBlockByNumber",
                serde_json::json!([format!("0x{n:x}"), true]),
            )
            .await?;
        for tx in block["transactions"].as_array().into_iter().flatten() {
            for a in [addr(&tx["from"]), addr(&tx["to"])].into_iter().flatten() {
                source.senders_recipients += accounts.insert(a) as usize;
            }
            for entry in tx["accessList"].as_array().into_iter().flatten() {
                let Some(a) = addr(&entry["address"]) else {
                    continue;
                };
                accounts.insert(a);
                for key in entry["storageKeys"].as_array().into_iter().flatten() {
                    if let Ok(slot) = parse_word(key) {
                        source.access_list_slots += slots.insert((a, slot)) as usize;
                    }
                }
            }
        }
        // Receipts are pruned past ~64 blocks on a --minimal node.
        if let Ok(receipts) = rpc
            .call(
                "eth_getBlockReceipts",
                serde_json::json!([format!("0x{n:x}")]),
            )
            .await
        {
            for r in receipts.as_array().into_iter().flatten() {
                for log in r["logs"].as_array().into_iter().flatten() {
                    if let Some(a) = addr(&log["address"]) {
                        source.log_emitters += accounts.insert(a) as usize;
                    }
                }
            }
        }
        source.blocks += 1;
        if n == 0 {
            break;
        }
        n -= 1;
    }
    Ok(Keys {
        accounts: accounts.into_iter().collect(),
        slots: slots.into_iter().collect(),
        first_block: n + 1,
        last_block: top,
        source,
    })
}

/// xorshift64* — deterministic shuffles without a rand dependency.
pub struct Rng(pub u64);

impl Rng {
    pub fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }

    pub fn shuffle<T>(&mut self, v: &mut [T]) {
        for i in (1..v.len()).rev() {
            v.swap(i, (self.next() % (i as u64 + 1)) as usize);
        }
    }
}

pub fn keccak(bytes: &[u8]) -> Word {
    use tiny_keccak::Hasher;
    let mut out = [0u8; 32];
    let mut k = tiny_keccak::Keccak::v256();
    k.update(bytes);
    k.finalize(&mut out);
    out
}
