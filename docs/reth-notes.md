# reth v2.10.0 ExEx API notes (T1.0 VERIFY)

Verified against the pinned tag source (`v2.10.0` = `f5192ac`, the exact
version the target node runs). File references are into that tag.
(Originally verified at v2.5.1; re-verified line-by-line after the 2.8.1
and 2.10.0 bumps.)

- **Dependency**: `reth-ethereum = { git, tag = "v2.10.0", features = ["full", "cli"] }`
  — the in-repo `examples/exex-subscription` uses exactly this and exercises
  both the CLI wrapper and the ExEx (its `Cargo.toml:13`).
- **Patches (new in 2.10)**: reth's root `Cargo.toml:672-686` `[patch]`es
  evm2 (to its pre-transfer `DaniPopes/evm2` URL, same rev) and the
  `reth-core` crates (`reth-codecs{,-derive}`, `reth-primitives-traits`,
  `reth-rpc-traits`, `reth-zstd-compressors` → `paradigmxyz/reth-core` git
  rev). Cargo ignores a dependency's patches, so silt's root `Cargo.toml`
  mirrors them; without them reth-core resolves from crates.io (0.8.2,
  still on revm) and `reth-trie-common` fails with `Account:
  From<&evm2::AccountInfo>` not satisfied. The `inkwell` patch is skipped
  (evm2-jit only, not in silt's tree).
- **alloy-trie must stay at 0.9.8** (reth's lock): 0.9.9 adds a pub
  `HashBuilder::all_branch_nodes` field that reth-trie-common's struct
  literal (`crates/trie/common/src/hash_builder/state.rs:32`) doesn't set.
  A bare `cargo update` re-breaks this; re-pin with
  `cargo update -p alloy-trie --precise 0.9.8`.
- **Wrapper pattern** (`examples/exex-subscription/src/main.rs:171`):
  `reth_ethereum::cli::Cli::parse_args().run(async |builder, _| { builder
  .node(EthereumNode::default()).install_exex("name", async |ctx|
  Ok(fut)).launch().await?; handle.wait_for_node_exit().await })`.
  **Caveat found hands-on:** the examples' plain `.launch()` does NOT wire
  dev interval mining — the stock binary (`bin/reth/src/main.rs:52`) uses
  the debug launcher; since 2.8 it calls
  `.launch_with_debug_capabilities_and_backfill(backfill)` with a
  CLI-built backfill, and plain `launch_with_debug_capabilities()`
  (`crates/node/builder/src/builder/mod.rs:741`) is the same launcher with
  the default `PipelineBackfill` (`:749`). silt-exex keeps the plain call,
  so `--dev` behaves exactly like stock reth.
- **`ExExContext`** (`crates/exex/exex/src/context.rs:14`): `head:
  BlockNumHash`, `config: NodeConfig<ChainSpec>` (→ `config.chain:
  Arc<ChainSpec>`, `node_config.rs:101` — chain id + genesis hash for the
  Hello frame), `events: UnboundedSender<ExExEvent>` (`:28`),
  `notifications: ExExNotifications<Node::Provider, Node::Evm>` (`:35`).
- **`ExExNotification`** (`crates/exex/types/src/notification.rs:10`):
  `ChainCommitted{new}`, `ChainReorged{old,new}`, `ChainReverted{old}`;
  `notification.committed_chain()` returns the NEW side for both Committed
  and Reorged (`:33`) — so Reorged maps to `Reorg` marker + `Diff` of the
  new segment in one place.
- **`Chain`** (`crates/evm/execution-types/src/chain.rs`): `headers()`
  (`:118`), `execution_outcome()` (`:161`), `blocks_and_receipts()`
  (`:267`), `tip()` (`:328`, `RecoveredBlock`), `range()` (`:342`).
- **`ExecutionOutcome`** (`crates/evm/execution-types/src/execution_outcome.rs:49`):
  now generic `ExecutionOutcome<T = reth_ethereum_primitives::Receipt>`;
  `bundle: BundleState` (`:51`), `receipts: Vec<Vec<T>>` (`:55`, outer =
  per block, in order), `first_block` (`:57`).
- **`BundleState`**: revm is gone in 2.10 — execution runs on `evm2`, and
  the bundle types are reth's own port of `revm-database` in
  `reth-trie-common` (`crates/trie/common/src/bundle_state/`), reached as
  `reth_ethereum::trie::bundle_state::{BundleState, BundleAccount,
  AccountStatus, StorageSlot}` (`reth_evm` also re-exports `BundleState`,
  `crates/evm/evm/src/lib.rs:51`). Shapes are unchanged:
  `state: AddressMap<BundleAccount>` (`bundle_state.rs:389`;
  `info: Option<AccountInfo>` — `None`/destroyed status = account gone;
  `storage: U256Map<StorageSlot>` with `present_value()`,
  `plain_account.rs:64,72`), and `contracts: B256Map<Bytecode>` (`:391`)
  — **new bytecode is reachable from the bundle**, no extra lookup needed.
  `AccountInfo`/`Bytecode` are evm2's (`balance`/`nonce`/`code_hash`,
  `original_bytes()`), re-exported only via
  `reth_ethereum::evm::primitives::cached` (`crates/evm/evm/src/cached.rs:6`).
  Storage iteration pattern confirmed by the example (`main.rs:123-137`).
- **Receipt**: moved upstream in 2.8 — `reth_ethereum_primitives::Receipt`
  is now an alias for `alloy_consensus::EthereumReceipt<TxType>`
  (`crates/ethereum/primitives/src/receipt.rs:5`). Fields are unchanged:
  `success`, `cumulative_gas_used`, `logs` (alloy `Log { address, data:
  { topics, data } }`). Receipts still do NOT carry tx hashes — zip with
  the block body's transactions for `ReceiptLite.tx_hash`.
- **Ack** (`context.rs:28`): emit `ExExEvent::FinishedHeight(
  chain.tip().num_hash())` once a notification is durably ours (for silt:
  after the spool write, D5). `ExExEvent` lives in
  `crates/exex/exex/src/event.rs:5` (variant takes `BlockNumHash`, `:12`).
  reth retains un-acked notifications and replays them on restart — the
  publisher dedupes by `(number, hash)`.

## silt_multiproof facts (T1.6 VERIFY, D8)

- **Custom RPC namespaces**: `extend_rpc_modules` on the builder
  (`crates/node/builder/src/builder/mod.rs:659`, chains after
  `install_exex` and before `launch_with_debug_capabilities` — the
  in-repo `examples/node-custom-rpc/src/main.rs:38` shows the exact
  pattern). The hook's `RpcContext` exposes the node provider
  (`crates/node/builder/src/rpc.rs:328`) and
  `ctx.modules.merge_configured(ext.into_rpc())` merges a jsonrpsee
  namespace into every configured transport — no `--http.api` listing
  needed, but no transport flags means the endpoint is simply not served.
- **jsonrpsee is `0.26.0`** at the pin (workspace `Cargo.toml:567`);
  generated `...ApiServer` traits are implemented with `#[async_trait]`
  (reth's own pattern, e.g. `crates/rpc/rpc/src/txpool.rs:89`).
- **`StateProofProvider::multiproof(TrieInput, MultiProofTargets) ->
  MultiProof`** (`crates/storage/storage-api/src/trie.rs:159`, signature
  unchanged), reachable from any `StateProviderBox` minted by
  `StateProviderFactory::latest()` / `state_by_block_hash()`
  (`crates/storage/storage-api/src/state.rs:156,199`). 2.8 adds a
  `multiproof_v2` (`trie.rs:174`, `MultiProofTargetsV2` →
  `DecodedMultiProofV2`) — silt stays on v1. `MultiProofTargets` is a
  newtype over `B256Map<B256Set>` (hashed-address → hashed-slot set) and
  derefs to the map via derive_more (`crates/trie/common/src/proofs.rs:23`).
- **Historical blocks work**: reworked in 2.8 —
  `HistoricalStateProviderRef` is gone; historical (and latest) state is
  served by `OverlayStateProvider` from the new `reth-storage-overlay`
  crate, wired via `OverlayStateProviderFactory` in
  `crates/storage/provider/src/providers/blockchain_provider.rs`
  (`state_by_block_hash`, `:813`). Its `multiproof`
  (`crates/storage/storage-overlay/src/provider.rs:752`) builds a revert
  overlay (`build_overlay`, `:352`) before walking — cost still grows
  with distance from tip.
- **`MultiProof` has no serde** (`proofs.rs:183` derives only
  Clone/Default/Debug/PartialEq/Eq) — hence silt's own response types.
  2.8 adds a `branch_node_masks: BranchNodeMasksMap` field alongside
  `account_subtree`/`storages` (silt's response types are unaffected).
  `account_subtree`/`StorageMultiProof::subtree` (`:186,693`) are
  `ProofNodes` (path-keyed RLP; `Nibbles::to_vec()` yields one `u8` per
  nibble, nybbles 0.4.8).
