# reth 46731e9 (2.10.1-dev) ExEx API notes (T1.0 VERIFY)

Verified against the pinned rev source: reth main `46731e9` (crates say
2.10.1), the exact rev yevm-node pins and the target node runs. It is 35
commits past where the `v2.10.1` tag (`58ebce1`) branched off. File
references are into that rev. (Originally verified at v2.5.1; re-verified
line-by-line after the 2.8.1, 2.10.0, 2.10.1 and `46731e9` bumps. For
`46731e9` every reth file cited below was diffed against `v2.10.1`: only
`blockchain_provider.rs` moved (lines re-cited), and the bundle-state
types moved into evm2 behind the same re-export path — see `BundleState`.)

- **Dependency**: `reth-ethereum = { git, rev = "46731e9…", features = ["full", "cli"] }`
  — the in-repo `examples/exex-subscription` uses exactly this and exercises
  both the CLI wrapper and the ExEx (its `Cargo.toml:13`).
- **Patches (new in 2.10)**: reth's root `Cargo.toml:672-683` `[patch]`es
  evm2 (2.10.0: the pre-transfer `DaniPopes/evm2` URL; v2.10.1:
  `www.github.com/alloy-rs/evm2` at rev `01df8f6`; `46731e9`: back to
  `DaniPopes/evm2` at rev `37fb326`, the same patches yevm's root
  `Cargo.toml` carries) and the
  `reth-core` crates (`reth-codecs{,-derive}`, `reth-primitives-traits`,
  `reth-rpc-traits`, `reth-zstd-compressors` → `paradigmxyz/reth-core` git
  rev). Cargo ignores a dependency's patches, so silt's root `Cargo.toml`
  mirrors them; without them reth-core resolves from crates.io (0.8.2,
  still on revm) and `reth-trie-common` fails with `Account:
  From<&evm2::AccountInfo>` not satisfied. The `evm2-jit` patch (and, at
  v2.10.1, `inkwell`) is skipped: evm2-jit is not in silt's tree.
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
- **`BundleState`**: revm is gone in 2.10 — execution runs on `evm2`. The
  bundle types (a port of `revm-database`) lived in `reth-trie-common` up
  to v2.10.1; at `46731e9` they moved into evm2
  (`crates/evm2/src/block/bundle_state/` at rev `37fb326`) and
  `reth-trie-common` re-exports them under the old name
  (`crates/trie/common/src/lib.rs:42`, `pub use evm2::block::bundle_state`;
  the code is identical bar imports/formatting). Reached as
  `reth_ethereum::trie::bundle_state::{BundleState, BundleAccount,
  AccountStatus, StorageSlot}` (`reth_evm` also re-exports `BundleState`,
  `crates/evm/evm/src/lib.rs:51`). Shapes are unchanged:
  `state: AddressMap<BundleAccount>` (`bundle_state.rs:389`;
  `info: Option<AccountInfo>` — `None`/destroyed status = account gone;
  `storage: U256Map<StorageSlot>` with `present_value()`,
  `plain_account.rs:64,72`; line numbers are evm2's and unchanged by the
  move), and `contracts: B256Map<Bytecode>` (`:391`)
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
  (`state_by_block_hash`, `:819`). Its `multiproof`
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

## Lookup fast path facts (T4.2 VERIFY, D9)

Verified at `v2.10.0` (`f5192ac`) for `silt-node/src/lookup.rs` and the
read-only harness `silt-node/examples/lookup-ro.rs`; unchanged at `v2.10.1`
(`58ebce1`) and re-cited at `46731e9`: only `blockchain_provider.rs` moved,
by a new `get_prune_checkpoints_for_availability` at `:725` (`latest()`
`:739`→`:745`, `state_by_block_hash()` `:813`→`:819`), and evm2's
`Bytecode::original_bytes` moved `:352`→`:386` with an identical body.

- **Provider from the ExEx**: `ExExContext::provider() -> &Node::Provider`
  (`crates/exex/exex/src/context.rs:82`); `Node::Provider: FullProvider`
  (`crates/node/api/src/node.rs:30`), which includes `StateProviderFactory`
  and `Clone`. `NodeSource` clones it once at startup and only ever mints
  short-lived state providers from it.
- **`StateProviderFactory`** (`crates/storage/storage-api/src/state.rs:151`):
  `latest()` (`:156`), `history_by_block_hash()` (`:194`, "only looks at
  historical blocks") and `state_by_block_hash()` (`:199`, "either a
  historical **or pending** block"). It extends `BlockIdReader:
  BlockNumReader + Sync` (`block_id.rs:59`), so one bound also gives
  `chain_info()` / `block_number(hash)` (`block_id.rs:14,28`) and
  `BlockHashReader::block_hash(number)` (`block_hash.rs:11`).
- **`BlockchainProvider` (the node's provider) resolution order**
  (`crates/storage/provider/src/providers/blockchain_provider.rs`):
  `latest()` (`:745`) uses `canonical_in_memory_state.head_state()` — the
  canonical head **including executed-but-unpersisted blocks** — else the
  DB; `chain_info()` (`:470`) reads the same in-memory head.
  `state_by_block_hash()` (`:819`) tries canonical in-memory blocks, then
  `history_by_block_hash()` (DB; canonical only), then
  `pending_state_by_hash()`, else `StateForHashNotFound`. The pending block
  is set by the engine on `newPayload` before it is canonical
  (`crates/engine/tree/src/tree/mod.rs:1639,3287`), so `state_by_block_hash`
  alone would serve a **non-canonical** block. `NodeSource` therefore
  requires `block_number(hash) == Some(n)` and `block_hash(n) == hash`
  before opening state, and resolves "latest" to `chain_info().best_hash`
  first so the response names exactly what it read.
- **Point reads** on any `StateProviderBox`: `AccountReader::basic_account`
  (`storage-api/src/account.rs:18`) → `Option<Account>`; `Account {
  nonce, balance, bytecode_hash: Option<B256> }` (reth-core
  `crates/primitives-traits/src/account.rs:33`) with `None` = no code (the
  server normalizes to `KECCAK_EMPTY`). `StateProvider::storage`
  (`state.rs:45`) → `Option<U256>` (missing = zero).
  `BytecodeReader::bytecode_by_hash` (`state.rs:124`) → reth's `Bytecode`
  wrapping evm2's; `original_bytes()` (evm2
  `crates/evm2/src/bytecode/mod.rs:386` at rev `37fb326`) is the unpadded code as a cheap
  `Bytes` clone. The overlay's `bytecode_by_hash` is not pinned by block
  (`crates/storage/storage-overlay/src/provider.rs:580`): code is global
  per hash.
- **How unknown / pruned / not-readable blocks surface**
  (`crates/storage/errors/src/provider.rs`): `BlockHashNotFound` (`:37`),
  `HeaderNotFound` (`:66`), `UnknownBlockHash` (`:84`),
  `StateForHashNotFound` (`:87`), `StateForNumberNotFound` (`:90`),
  `StateAtBlockPruned` (`:102`), `BlockNotExecuted` (`:105`),
  `InsufficientChangesets` (`:170`) — all mapped to `UnknownBlock`. Two
  traps: (1) the overlay is built **lazily on the first read**
  (`execution_overlay`, `storage-overlay/src/provider.rs:386`), so
  `StateAtBlockPruned` (`:419,428`) can come from `basic_account`/`storage`,
  not from opening — the server classifies item-read errors the same way
  and turns any block-level error into a frame-level `UnknownBlock`;
  (2) asking for state above the partial-state-trie frontier fails with an
  **untyped** `ProviderError::other("overlay anchor … is after partial
  state trie frontier …")` (`storage-overlay/src/builder.rs:319`), so the
  read-only source checks `number <= frontier` itself.
- **Read-only open** (harness): `EthereumNode::provider_factory_builder()`
  (`crates/ethereum/node/src/node.rs:131`) →
  `open_read_only(chainspec, ReadOnlyConfig, Runtime)`
  (`crates/storage/provider/src/providers/database/builder.rs:95`);
  `ReadOnlyConfig` (`:130`) has pub `db_dir`/`static_files_dir`/
  `rocksdb_dir`/`watch`, `from_datadir` (`:157`) sets `watch: true` (the
  monitored mode). `RayonConfig` (`crates/tasks/src/runtime.rs:101`) has
  exactly the ten fields yevm-reth's `minimal_runtime()` sets;
  `RuntimeConfig::with_tokio` (`:248`). v2.10.0 and v2.10.1 readers opened
  the `46731e9` datadir without complaint; silt now pins that exact rev.
- **RocksDB secondary writes into the datadir**: a read-only RocksDB is a
  *secondary* instance whose scratch dir is
  `rocksdb_dir.parent()/rocksdb-secondary-tmp-<pid>`
  (`crates/storage/provider/src/providers/rocksdb/provider.rs:461-466`),
  removed only on a clean drop (`:796`). The live datadir already holds 604
  such leftovers from killed readers. The harness points `rocksdb_dir` at
  a symlink in its own scratch dir, so the secondary dir lands there.
  Opening replays the primary's WAL (~8 GB read on first open here, ~1 min).
- **Fresh view per request**: `ProviderFactory::provider()`
  (`database/mod.rs:382`) opens one MDBX read transaction and calls
  `sync_providers_if_needed()` (`:293`), which catches the RocksDB
  secondary and static-file index up when the MDBX txn id moved (slow path
  under a lock, once per persisted block). `BlockchainProvider::new`
  (`blockchain_provider.rs:89`) is built once; its pub
  `state_provider_from_database(provider, hash)` (`:168`) turns that same
  `DatabaseProviderRO` into the overlay state provider, so one request =
  one read transaction.
- **State frontier**: `FinishCheckpoint::partial_state_trie()`
  (`crates/stages/types/src/checkpoints.rs:491,498`) on
  `StageId::Finish` (`crates/stages/types/src/id.rs:33`), falling back to
  the Finish block — the same rule the overlay uses
  (`database_state_frontiers`, `storage-overlay/src/builder.rs:740`).
  `StageId` is not re-exported by `reth-ethereum`, hence the direct
  `reth-stages-types` dependency (same tag, already in the tree).
- **Cost note (harness only)**: a pin below the persisted tip resolves to
  `AnchorForParent::RevertsRequired` (`builder.rs:260`) and reads go
  through the historical fallback — a history-index lookup plus changeset
  reads per key (`provider.rs:484-505`) — so the harness's fixed-pin reads
  cost more than the node's tip reads, which take the in-memory overlay +
  hashed-state path.

## Proof op facts (T5 VERIFY, D10)

Verified at `46731e9` for op `Proof = 4` (`silt-node/src/lookup.rs`:
`RethView::proof`, `write_multiproof`) and `examples/proof-bench.rs`.

- **The read**: `StateProofProvider::multiproof(&self, TrieInput,
  MultiProofTargets) -> ProviderResult<MultiProof>`
  (`crates/storage/storage-api/src/trie.rs:159`), the same call as
  `silt_multiproof` (D8). `MultiProof { account_subtree: ProofNodes,
  branch_node_masks, storages: B256Map<StorageMultiProof> }`
  (`crates/trie/common/src/proofs.rs:184`); `StorageMultiProof { root,
  subtree, branch_node_masks }` (`:689`). Op 4 carries `account_subtree`
  and each storage trie's `root` + `subtree`; the masks are dropped, as in
  D8. `multiproof_v2` (`trie.rs:174`) is left for later.
- **On the overlay provider** (`crates/storage/storage-overlay/src/provider.rs:752`)
  it builds the revert overlay (`build_overlay`, `:352`), then runs one
  sequential `DbProof::overlay_multiproof` on the view's own read
  transaction. It uses no rayon pool, so the harness's one-thread
  runtime doesn't slow it, and one frame stays one MDBX read transaction.
- **Order**: `ProofNodes` and `storages` are hash maps, so the server
  sorts them. `Nibbles: Ord` (nybbles 0.4.9 `src/nibbles.rs:193`)
  compares the left-aligned value, then the length: lexicographic nibble
  order, with a prefix before its extensions. `Nibbles::pack()` (`:539`)
  packs high nibble first and leaves an odd path's last low nibble 0,
  into a `SmallVec<[u8; 32]>` (no allocation). That is exactly the op-4
  path encoding.
- **Pin depth**: a pin below the persisted tip pays the revert overlay.
  Measured: +0.1 s one block back, about 25 ms per further block, 0.8 s
  at 32 (`docs/proof-bench.md`). Hence `proof_max_depth` = 8.
- **MDBX reader limit**: a read transaction older than
  `DEFAULT_MAX_READ_TRANSACTION_DURATION` (5 min,
  `crates/storage/libmdbx-rs/src/environment.rs:27`) is aborted, and the
  next read fails with "read transaction has been timed out (-96000)".
  `ReadOnlyConfig::disable_long_read_transaction_safety`
  (`crates/storage/provider/src/providers/database/builder.rs:172`)
  lifts that, but must never be used against a live node: a long reader
  stops MDBX reusing freed pages.
- **`eth_getProof`** on a default node answers only the latest block:
  `--rpc.eth-proof-window` defaults to `DEFAULT_ETH_PROOF_WINDOW = 0`
  (`crates/rpc/rpc-server-types/src/constants.rs:62`). So the verify
  waits for the lookup server to reach the node's head.
- **Changesets keep plain keys** in both storage layouts (v1 MDBX and v2
  static files; reth's own tests assert it at
  `crates/storage/provider/src/providers/database/provider.rs:5826,6022`).
  proof-bench keccaks them into targets.
