//! yevm compatibility adapter (feature `yevm`, PLAN T2.4/D8).
//!
//! `Chain` and `State` walk together in yevm: this module provides both —
//! the `Chain` impl over [`Cache`] and [`overlay()`] for the per-sim
//! mutable `State`. Canonical usage:
//!
//! ```ignore
//! let (cache, mut heads) = Cache::connect(cfg);
//! let result = Executor::new().run(&tx, &head, &mut overlay(), &*cache)?;
//! ```
//!
//! Numeric bridging goes through yevm's serde (RPC-shaped JSON): slower
//! than a native conversion but immune to wrapper-type churn; the hot path
//! stays inside the core crate either way.

use crate::cache::Cache;
use yevm::core::call::{Block, Head};
use yevm::core::state::Account;
use yevm::{Acc, Buf, Int};

/// Fresh per-sim overlay; misses fall through to the `Chain` impl below.
pub fn overlay() -> yevm::Cache {
    yevm::Cache::new()
}

fn hex32(word: &crate::Word) -> String {
    crate::fallback::hex_word(word)
}

fn int(word: &crate::Word) -> eyre::Result<Int> {
    Ok(serde_json::from_value(serde_json::Value::String(hex32(
        word,
    )))?)
}

fn acc_bytes(acc: &Acc) -> crate::Addr {
    let hex = acc.to_string();
    let hex = hex.strip_prefix("0x").unwrap_or(&hex);
    let mut out = [0u8; 20];
    for (i, chunk) in hex.as_bytes().chunks(2).take(20).enumerate() {
        out[i] = u8::from_str_radix(std::str::from_utf8(chunk).unwrap_or("00"), 16).unwrap_or(0);
    }
    out
}

fn head_json(h: &silt_wire::HeadLite) -> serde_json::Value {
    serde_json::json!({
        "number": format!("0x{:x}", h.number),
        "hash": hex32(&h.hash),
        "parentHash": hex32(&h.parent_hash),
        "timestamp": format!("0x{:x}", h.timestamp),
        "baseFeePerGas": hex32(&h.basefee),
        "gasLimit": format!("0x{:x}", h.gas_limit),
        "miner": crate::fallback::hex_addr(&h.coinbase),
        "mixHash": hex32(&h.prevrandao),
    })
}

#[async_trait::async_trait]
impl yevm::Chain for Cache {
    async fn get(&self, acc: &Acc, key: &Int) -> eyre::Result<Int> {
        let slot: crate::Word =
            crate::fallback::parse_word(&serde_json::Value::String(key.to_string()))?;
        let value = self.storage(&acc_bytes(acc), &slot).await?;
        int(&value)
    }

    async fn acc(&self, acc: &Acc) -> eyre::Result<Account> {
        let view = self.account(&acc_bytes(acc)).await?;
        Ok(Account {
            value: int(&view.balance)?,
            nonce: serde_json::from_value(serde_json::json!(format!("0x{:x}", view.nonce)))?,
            code: (Buf::from(view.code.to_vec()), int(&view.code_hash)?),
        })
    }

    async fn code(&self, acc: &Acc) -> eyre::Result<(Buf, Int)> {
        Ok(yevm::Chain::acc(self, acc).await?.code)
    }

    async fn nonce(&self, acc: &Acc) -> eyre::Result<u64> {
        let view = self.account(&acc_bytes(acc)).await?;
        Ok(view.nonce)
    }

    async fn balance(&self, acc: &Acc) -> eyre::Result<Int> {
        let view = self.account(&acc_bytes(acc)).await?;
        int(&view.balance)
    }

    async fn head(&self, number: u64) -> eyre::Result<Head> {
        let head = Cache::head(self, number).await?;
        Ok(serde_json::from_value(head_json(&head))?)
    }

    async fn block(&self, number: u64) -> eyre::Result<Block> {
        Ok(serde_json::from_value(
            self.block_json(number, true).await?,
        )?)
    }

    async fn chain_id(&self) -> eyre::Result<u64> {
        Cache::chain_id(self).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn head_json_deserializes_into_yevm_head() {
        let lite = silt_wire::HeadLite {
            number: 16,
            hash: [0x11; 32],
            parent_hash: [0x22; 32],
            timestamp: 100,
            basefee: {
                let mut w = [0u8; 32];
                w[28..].copy_from_slice(&1_000_000_000u32.to_be_bytes());
                w
            },
            gas_limit: 30_000_000,
            coinbase: [0x33; 20],
            prevrandao: [0x44; 32],
        };
        let head: Head = serde_json::from_value(head_json(&lite)).unwrap();
        assert_eq!(head.hash.to_string(), format!("0x{}", "11".repeat(32)));
    }

    #[test]
    fn int_bridge_roundtrips() {
        let mut w = [0u8; 32];
        w[31] = 0x2a;
        let i = int(&w).unwrap();
        assert!(i.to_string().ends_with("2a"));
    }
}
