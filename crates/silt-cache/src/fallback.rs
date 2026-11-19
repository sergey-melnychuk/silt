//! The RPC fallback (PLAN D2): `eth_*` reads pinned to a block hash.
//! Behind a trait so tests can inject an in-memory chain and embedders
//! can bring their own RPC client.

use silt_wire::{Addr, Word};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RpcAccount {
    pub balance: Word,
    pub nonce: u64,
    pub code: Vec<u8>,
}

/// An account without its code: what a cache that already holds the code
/// (by hash, forever) needs on a miss. `code` is set when the source had to
/// fetch it anyway (JSON-RPC returns code, not its hash).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccountMeta {
    pub balance: Word,
    pub nonce: u64,
    pub code_hash: Word,
    pub code: Option<Vec<u8>>,
}

impl AccountMeta {
    fn from_rpc(a: RpcAccount) -> Self {
        let code_hash = if a.code.is_empty() {
            crate::store::KECCAK_EMPTY
        } else {
            crate::cache::keccak(&a.code)
        };
        Self {
            balance: a.balance,
            nonce: a.nonce,
            code_hash,
            code: Some(a.code),
        }
    }
}

#[async_trait::async_trait]
pub trait Fallback: Send + Sync + 'static {
    /// Whole account at the given block hash (balance + nonce + code: the
    /// yevm-notes finding — account-ish misses always want all of it).
    async fn account_at(&self, addr: &Addr, block_hash: &Word) -> eyre::Result<RpcAccount>;
    async fn storage_at(&self, addr: &Addr, slot: &Word, block_hash: &Word) -> eyre::Result<Word>;
    /// Header by number (fallback for heads outside the ring); `None` block
    /// param means latest.
    async fn head_by_number(&self, number: Option<u64>) -> eyre::Result<silt_wire::HeadLite>;
    /// Raw RPC block JSON (adapter/replay passthrough).
    async fn block_json(&self, number: u64, full: bool) -> eyre::Result<serde_json::Value>;
    async fn chain_id(&self) -> eyre::Result<u64>;

    /// Accounts without their code, in `addrs` order (PLAN M6: the cache
    /// asks for code by hash only when it doesn't hold it). The default
    /// reads whole accounts one by one and hashes their code.
    async fn accounts_meta_at(
        &self,
        addrs: &[Addr],
        block_hash: &Word,
    ) -> eyre::Result<Vec<AccountMeta>> {
        let mut out = Vec::with_capacity(addrs.len());
        for addr in addrs {
            out.push(AccountMeta::from_rpc(
                self.account_at(addr, block_hash).await?,
            ));
        }
        Ok(out)
    }

    /// Many slots, in `keys` order. The default reads them one by one.
    async fn storages_at(
        &self,
        keys: &[(Addr, Word)],
        block_hash: &Word,
    ) -> eyre::Result<Vec<Word>> {
        let mut out = Vec::with_capacity(keys.len());
        for (addr, slot) in keys {
            out.push(self.storage_at(addr, slot, block_hash).await?);
        }
        Ok(out)
    }

    /// Bytecode by hash, in `hashes` order; `None` where the source can't
    /// answer by hash (the caller then reads the account whole). The
    /// default can't (JSON-RPC has no code-by-hash call).
    async fn codes_by_hash(
        &self,
        hashes: &[Word],
        _block_hash: &Word,
    ) -> eyre::Result<Vec<Option<Vec<u8>>>> {
        Ok(vec![None; hashes.len()])
    }
}

pub struct HttpFallback {
    url: String,
    http: reqwest::Client,
    id: std::sync::atomic::AtomicU64,
}

impl HttpFallback {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            http: reqwest::Client::new(),
            id: 1.into(),
        }
    }

    async fn call(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> eyre::Result<serde_json::Value> {
        let id = self.id.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let body =
            serde_json::json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
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
        resp.get("result")
            .cloned()
            .ok_or_else(|| eyre::eyre!("{method}: no result"))
    }
}

pub fn hex_word(word: &Word) -> String {
    let mut s = String::with_capacity(66);
    s.push_str("0x");
    for b in word {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

pub fn hex_addr(addr: &Addr) -> String {
    let mut s = String::with_capacity(42);
    s.push_str("0x");
    for b in addr {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

pub fn parse_word(value: &serde_json::Value) -> eyre::Result<Word> {
    let s = value
        .as_str()
        .ok_or_else(|| eyre::eyre!("expected hex string"))?;
    let s = s.strip_prefix("0x").unwrap_or(s);
    eyre::ensure!(s.len() <= 64, "hex too long for a word");
    let mut out = [0u8; 32];
    let padded = format!("{s:0>64}");
    for (i, chunk) in padded.as_bytes().chunks(2).enumerate() {
        out[i] = u8::from_str_radix(std::str::from_utf8(chunk)?, 16)?;
    }
    Ok(out)
}

pub fn parse_u64(value: &serde_json::Value) -> eyre::Result<u64> {
    let s = value
        .as_str()
        .ok_or_else(|| eyre::eyre!("expected hex string"))?;
    Ok(u64::from_str_radix(s.strip_prefix("0x").unwrap_or(s), 16)?)
}

pub fn parse_bytes(value: &serde_json::Value) -> eyre::Result<Vec<u8>> {
    let s = value
        .as_str()
        .ok_or_else(|| eyre::eyre!("expected hex string"))?;
    let s = s.strip_prefix("0x").unwrap_or(s);
    eyre::ensure!(s.len().is_multiple_of(2), "odd hex length");
    (0..s.len())
        .step_by(2)
        .map(|i| Ok(u8::from_str_radix(&s[i..i + 2], 16)?))
        .collect()
}

fn head_from_json(block: &serde_json::Value) -> eyre::Result<silt_wire::HeadLite> {
    let get = |k: &str| block.get(k).cloned().unwrap_or(serde_json::Value::Null);
    Ok(silt_wire::HeadLite {
        number: parse_u64(&get("number"))?,
        hash: parse_word(&get("hash"))?,
        parent_hash: parse_word(&get("parentHash"))?,
        timestamp: parse_u64(&get("timestamp"))?,
        basefee: parse_word(&get("baseFeePerGas")).unwrap_or([0; 32]),
        gas_limit: parse_u64(&get("gasLimit"))?,
        coinbase: {
            let w = parse_word(&get("miner"))?;
            w[12..32].try_into().expect("20 bytes")
        },
        prevrandao: parse_word(&get("mixHash")).unwrap_or([0; 32]),
    })
}

#[async_trait::async_trait]
impl Fallback for HttpFallback {
    async fn account_at(&self, addr: &Addr, block_hash: &Word) -> eyre::Result<RpcAccount> {
        // EIP-1898 pinned reads: same state no matter when they land.
        let a = hex_addr(addr);
        let at = serde_json::json!({ "blockHash": hex_word(block_hash) });
        let balance = self
            .call("eth_getBalance", serde_json::json!([a, at]))
            .await?;
        let nonce = self
            .call("eth_getTransactionCount", serde_json::json!([a, at]))
            .await?;
        let code = self.call("eth_getCode", serde_json::json!([a, at])).await?;
        Ok(RpcAccount {
            balance: parse_word(&balance)?,
            nonce: parse_u64(&nonce)?,
            code: parse_bytes(&code)?,
        })
    }

    async fn storage_at(&self, addr: &Addr, slot: &Word, block_hash: &Word) -> eyre::Result<Word> {
        let at = serde_json::json!({ "blockHash": hex_word(block_hash) });
        let value = self
            .call(
                "eth_getStorageAt",
                serde_json::json!([hex_addr(addr), hex_word(slot), at]),
            )
            .await?;
        parse_word(&value)
    }

    async fn head_by_number(&self, number: Option<u64>) -> eyre::Result<silt_wire::HeadLite> {
        let tag = match number {
            Some(n) => format!("0x{n:x}"),
            None => "latest".into(),
        };
        let block = self
            .call("eth_getBlockByNumber", serde_json::json!([tag, false]))
            .await?;
        eyre::ensure!(!block.is_null(), "no such block");
        head_from_json(&block)
    }

    async fn block_json(&self, number: u64, full: bool) -> eyre::Result<serde_json::Value> {
        self.call(
            "eth_getBlockByNumber",
            serde_json::json!([format!("0x{number:x}"), full]),
        )
        .await
    }

    async fn chain_id(&self) -> eyre::Result<u64> {
        parse_u64(&self.call("eth_chainId", serde_json::json!([])).await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_roundtrips() {
        let mut w = [0u8; 32];
        w[31] = 0x2a;
        assert_eq!(hex_word(&w), format!("0x{}2a", "00".repeat(31)));
        assert_eq!(parse_word(&serde_json::json!("0x2a")).unwrap(), w);
        assert_eq!(parse_u64(&serde_json::json!("0x2a")).unwrap(), 42);
    }

    #[test]
    fn head_parses_rpc_block_shape() {
        let block = serde_json::json!({
            "number": "0x10", "hash": format!("0x{}", "11".repeat(32)),
            "parentHash": format!("0x{}", "22".repeat(32)),
            "timestamp": "0x64", "baseFeePerGas": "0x3b9aca00",
            "gasLimit": "0x1c9c380", "miner": format!("0x{}", "33".repeat(20)),
            "mixHash": format!("0x{}", "44".repeat(32)),
        });
        let head = head_from_json(&block).unwrap();
        assert_eq!(head.number, 16);
        assert_eq!(head.coinbase, [0x33; 20]);
        assert_eq!(head.gas_limit, 30_000_000);
    }
}
