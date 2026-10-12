# reth v2.5.1 ExEx API notes (T1.0 VERIFY)

Verified against the pinned tag source (`v2.5.1` = `6dec1b96`, the exact
version the target node runs). File references are into that tag.

- **Dependency**: `reth-ethereum = { git, tag = "v2.5.1", features = ["full", "cli"] }`
  — the in-repo `examples/exex-subscription` uses exactly this and exercises
  both the CLI wrapper and the ExEx (its `Cargo.toml:13`).
- **Wrapper pattern** (`examples/exex-subscription/src/main.rs:163`):
  `reth_ethereum::cli::Cli::parse_args().run(async |builder, _| { builder
  .node(EthereumNode::default()).install_exex("name", async |ctx|
  Ok(fut)).launch().await?; handle.wait_for_node_exit().await })`.
  **Caveat found hands-on:** the examples' plain `.launch()` does NOT wire
  dev interval mining — the stock binary (`bin/reth/src/main.rs:40`) uses
  `.launch_with_debug_capabilities()` (`crates/node/builder/src/builder/mod.rs:713`).
  silt-exex uses the latter so `--dev` behaves exactly like stock reth.
- **`ExExContext`** (`crates/exex/exex/src/context.rs:15`): `head:
  BlockNumHash`, `config: NodeConfig<ChainSpec>` (→ `config.chain:
  Arc<ChainSpec>`, `node_config.rs:101` — chain id + genesis hash for the
  Hello frame), `events: UnboundedSender<ExExEvent>`, `notifications`
  (TryStream of `ExExNotification`).
- **`ExExNotification`** (`crates/exex/types/src/notification.rs:10`):
  `ChainCommitted{new}`, `ChainReorged{old,new}`, `ChainReverted{old}`;
  `notification.committed_chain()` returns the NEW side for both Committed
  and Reorged (`:33`) — so Reorged maps to `Reorg` marker + `Diff` of the
  new segment in one place.
- **`Chain`** (`crates/evm/execution-types/src/chain.rs`): `headers()`
  (`:108`), `blocks_and_receipts()` (`:234`), `tip()` (`:283`,
  `RecoveredBlock`), `range()` (`:297`), `execution_outcome()`.
- **`ExecutionOutcome`** (`crates/evm/execution-types/src/execution_outcome.rs:49`):
  `bundle: BundleState`, `receipts: Vec<Vec<Receipt>>` (outer = per block,
  in order), `first_block`.
- **`BundleState`** (revm): `state: HashMap<Address, BundleAccount>`
  (`info: Option<AccountInfo>` — `None`/destroyed status = account gone;
  `storage: HashMap<U256, StorageSlot>` with `present_value()`), and
  `contracts: HashMap<B256, Bytecode>` — **new bytecode is reachable from
  the bundle**, no extra lookup needed. Storage iteration pattern confirmed
  by the example (`main.rs:118-131`).
- **Receipt** (`crates/ethereum/primitives/src/receipt.rs`): `success`,
  `cumulative_gas_used`, `logs` (alloy `Log { address, data: { topics,
  data } }`). Receipts do NOT carry tx hashes — zip with the block body's
  transactions for `ReceiptLite.tx_hash`.
- **Ack** (`context.rs:26`): emit `ExExEvent::FinishedHeight(
  chain.tip().num_hash())` once a notification is durably ours (for silt:
  after the spool write, D5). reth retains un-acked notifications and
  replays them on restart — the publisher dedupes by `(number, hash)`.

## silt_multiproof facts (T1.6 VERIFY, D8)

- **Custom RPC namespaces**: `extend_rpc_modules` on the builder
  (`crates/node/builder/src/builder/mod.rs:631`, chains after
  `install_exex` and before `launch_with_debug_capabilities` — the
  in-repo `examples/node-custom-rpc/src/main.rs` shows the exact
  pattern). The hook's `RpcContext` exposes the node provider
  (`crates/node/builder/src/rpc.rs:316`) and
  `ctx.modules.merge_configured(ext.into_rpc())` merges a jsonrpsee
  namespace into every configured transport — no `--http.api` listing
  needed, but no transport flags means the endpoint is simply not served.
- **jsonrpsee is `0.26.0`** at the pin (workspace `Cargo.toml:582`);
  generated `...ApiServer` traits are implemented with `#[async_trait]`
  (reth's own pattern, e.g. `crates/rpc/rpc/src/txpool.rs:78`).
- **`StateProofProvider::multiproof(TrieInput, MultiProofTargets) ->
  MultiProof`** (`crates/storage/storage-api/src/trie.rs:159`), reachable
  from any `StateProviderBox` minted by `StateProviderFactory::latest()` /
  `state_by_block_hash()` (`crates/storage/storage-api/src/state.rs:150,185`).
  `MultiProofTargets` is hashed-address → hashed-slot set and derefs to a
  map (`crates/trie/common/src/proofs.rs:22,43,51`).
- **Historical blocks work**: `HistoricalStateProviderRef::multiproof`
  builds a revert overlay before walking
  (`crates/storage/provider/src/providers/state/historical.rs:580`) —
  cost grows with distance from tip.
- **`MultiProof` has no serde** (`proofs.rs:179` derives only
  Clone/Default/Debug/PartialEq/Eq) — hence silt's own response types.
  `account_subtree`/`StorageMultiProof::subtree` are `ProofNodes`
  (path-keyed RLP; `Nibbles::to_vec()` yields one `u8` per nibble,
  nybbles 0.4.8).
