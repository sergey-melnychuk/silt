//! yevm compatibility adapter (feature `yevm`, PLAN T2.4).
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
//! Words and addresses convert natively (yevm's `Hex<N>` is a big-endian
//! byte array, like silt's `Word`/`Addr`): these conversions sit on every
//! state read a simulation makes (PLAN M6). Headers, read only for
//! `BLOCKHASH`, still go through yevm's serde.

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

fn int(word: &crate::Word) -> Int {
    Int::from(&word[..])
}

fn word(int: &Int) -> crate::Word {
    let mut out = [0u8; 32];
    out.copy_from_slice(int.as_ref());
    out
}

fn acc_bytes(acc: &Acc) -> crate::Addr {
    let mut out = [0u8; 20];
    out.copy_from_slice(acc.as_ref());
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
        let value = self.storage(&acc_bytes(acc), &word(key)).await?;
        Ok(int(&value))
    }

    async fn acc(&self, acc: &Acc) -> eyre::Result<Account> {
        let view = self.account(&acc_bytes(acc)).await?;
        Ok(Account {
            value: int(&view.balance),
            nonce: Int::from(view.nonce),
            code: (Buf::from(view.code.to_vec()), int(&view.code_hash)),
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
        Ok(int(&view.balance))
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

/// PLAN M6: before a simulation, fetch in one batch what its transactions
/// are known to touch (sender, recipient and its code, the coinbase,
/// access-list accounts and slots), instead of one round trip per miss as
/// execution reaches them.
#[async_trait::async_trait]
impl crate::sim::Prefetch for Cache {
    async fn prefetch(&self, txs: &[yevm::core::call::TxFull], head: &Head) -> eyre::Result<()> {
        let mut accounts = vec![acc_bytes(&head.coinbase)];
        let mut slots = Vec::new();
        for tx in txs {
            accounts.push(acc_bytes(&tx.call.from));
            if let Some(to) = &tx.call.to {
                accounts.push(acc_bytes(to));
            }
            for item in &tx.tx.access_list {
                let addr = acc_bytes(&item.address);
                accounts.push(addr);
                slots.extend(item.storage_keys.iter().map(|k| (addr, word(k))));
            }
        }
        Cache::prefetch(self, &accounts, &slots).await
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
        let i = int(&w);
        assert!(i.to_string().ends_with("2a"));
        assert_eq!(word(&i), w);
    }

    #[test]
    fn native_conversions_match_the_serde_path() {
        // The serde (RPC-shaped hex) path the adapter used before: same values.
        let mut w = [0u8; 32];
        for (i, b) in w.iter_mut().enumerate() {
            *b = (i as u8).wrapping_mul(37).wrapping_add(1);
        }
        let via_serde: Int = serde_json::from_value(serde_json::Value::String(hex32(&w))).unwrap();
        assert_eq!(int(&w), via_serde);
        assert_eq!(word(&via_serde), w);

        let addr: crate::Addr = std::array::from_fn(|i| 0xa0 ^ i as u8);
        let acc: Acc =
            serde_json::from_value(serde_json::json!(crate::fallback::hex_addr(&addr))).unwrap();
        assert_eq!(acc_bytes(&acc), addr);
        assert_eq!(
            Int::from(0x1234u64),
            serde_json::from_value::<Int>(serde_json::json!("0x1234")).unwrap()
        );
    }
}
