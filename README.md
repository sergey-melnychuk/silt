# silt

State-diff feed for Ethereum nodes + a coherent chain-state cache.

A reth node commits a block → the **silt ExEx** maps the execution outcome
into a node-agnostic `BlockDiff` (accounts, storage, new code, headers,
receipts) and pushes it over TCP → **silt-cache** applies it to a carried
state cache that is coherent by construction, with standard `eth_*` RPC as
the always-available fallback for anything not yet resolved. Reorg handling
is drop-to-cold (v0). See `PLAN.md` for the full design and milestones,
`docs/reth-notes.md` for the verified reth API facts.

| Crate | What | reth? |
|---|---|---|
| `silt-wire` | wire types, postcard codec, framing, handshake; the binary lookup + proof layout (`lookup`) | no |
| `silt-node` | library any reth node installs: the feed ExEx + TCP publisher, lookup + proof server, `silt_multiproof` RPC | pinned v2.11.0 (`b9abbcc`) |
| `silt-exex` | thin binary: stock reth (its evm2 executor, features and profiles) with silt-node installed | pinned v2.11.0 (`b9abbcc`) |
| `silt-cache` | subscriber, cache, lookup client, RPC fallback, Chain-shaped API (+ optional `yevm` adapter) | no |

## Run

```sh
cargo build --release                       # wire + cache (reth-free)
cargo build --release -p silt-exex          # the node wrapper (compiles reth)
cargo build --profile maxperf -p silt-exex  # ... as reth's fastest build (fat LTO)

# the wrapper IS reth — same CLI, same datadir, same evm2 executor; it and
# stock reth can take turns on one datadir. silt.toml (optional, in the
# working directory) sets bind/spool/heartbeat_secs/lookup_bind/lookup_workers/
# lookup_proof_workers/lookup_proof_max_depth/max_span/read_touches/pending_reads/early_publish;
# defaults: feed on 127.0.0.1:8585, lookup fast path on 127.0.0.1:8586
./target/release/silt-exex node <your usual reth flags>

# watch + verify the feed against RPC (checks every applied key, pinned):
cargo run -p silt-cache --example verify -- 127.0.0.1:8585 http://127.0.0.1:8545

# 24 h soak (PLAN T3.2): the same verifier in a memory-capped user unit,
# with latency/bytes/apply/RSS summaries hourly and a final report:
scripts/soak.sh 24       # log in target/soak/; systemctl --user stop silt-soak
```

Operating silt on a node (build, deploy, checks, restarts, rollback,
upgrades, binding): `docs/runbook.md`.

Lookup fast path (PLAN D9) — binary, pipelined tip-state reads for cache
misses on their own listener (`lookup_bind`; `""` disables it). Format in
PLAN §2.1; numbers and the live-datadir verification in
`docs/lookup-bench.md` (single lookups: warm p50 ~55–80 µs, first touch
with SSD reads p50 0.3–0.8 ms, 0 mismatches vs pinned `eth_*`):

```toml
# silt.toml
lookup_bind = "127.0.0.1:8586"   # never 0.0.0.0: no auth (D1)
lookup_workers = 16              # dedicated read threads
lookup_proof_workers = 4         # proofs (op 4) on their own threads
lookup_proof_max_depth = 8       # deeper proof pins answer UnknownBlock
```

Trie proofs ride the same listener as op `Proof = 4` (PLAN D10): the read
`silt_multiproof` serves (pre-hashed, path-directed targets), without
JSON, hex or HTTP. `LookupClient::proof(pin, targets)` returns a
`ProofFrame` you can walk without copying (`frame.proof()`) or copy out
(`frame.to_read()`). Every answer is byte-identical to reth's in-process
multiproof, and every `eth_getProof` node is found in op 4
(`docs/proof-bench.md`). Op 4 adds 36–61 µs to the trie walk for small
proofs, 11–24% less than JSON-RPC at p50. Prove at the head: every block
of pin depth costs reth a revert overlay, about 0.1 s at one block and
0.8 s at 32. Protocol `VERSION` is 1 since D10, so v0 peers are refused
at the handshake.

```sh
# serve the same protocol from a live datadir, read-only, without touching
# the node (benchmarks/verification): state frontier = "latest"
cargo run --release -p silt-node --example lookup-ro -- \
    --datadir /path/to/datadir --bind 127.0.0.1:8587
cargo run --release -p silt-cache --example lookup-bench -- --server-pid $(pgrep -x lookup-ro)
cargo run --release -p silt-cache --example lookup-verify   # + op 4 vs eth_getProof at the head
cargo run --release -p silt-node --example proof-bench -- --datadir /path/to/datadir
```

Next to a live node, run these harnesses under a memory cap. They pull
GBs of database pages and once pushed the machine into the memory
pressure that got the node killed: `systemd-run --user --scope -p
MemoryMax=6G -p MemorySwapMax=0 …`, plus a pressure watchdog
(`docs/proof-bench.md`, Reproduce).

Path-directed multiproofs (PLAN D8) — proofs for **pre-hashed** trie keys,
the read `eth_getProof` cannot express; served on reth's own RPC
transports whenever `--http`/`--ws`/`--ipc` are enabled:

```sh
# params: [[hashedAddress, [hashedSlotProbe, ...]], ...], blockHash|null
curl -s http://127.0.0.1:8545 -H 'content-type: application/json' -d '{
  "jsonrpc":"2.0","id":1,"method":"silt_multiproof",
  "params":[[["0x<keccak(addr)>",["0x<probe>"]]], null]
}'
# -> { "accountSubtree": {"<nibble path hex>": "0x<rlp>", ...},
#      "storages": {"0x<hashedAddr>": {"root": "0x..", "subtree": {...}}} }
```

Embedding the cache:

```rust
let cfg = CacheConfig::new("http://node:8545")
    .with_feed("node:8585")
    .with_lookup("node:8586"); // misses over D9; eth_* RPC on any failure
let (cache, mut heads) = silt_cache::Cache::connect(cfg);
// cache.account(&addr) / cache.storage(&addr, &slot) / cache.head(n)
// feature "yevm": Executor::run(&tx, &head, &mut overlay(), &*cache)
```

## Installing silt in another reth node

`silt-node` is a library; any reth node installs it with two builder calls:

```rust
let cfg = silt_node::SiltConfig::load()?; // ./silt.toml, defaults otherwise
let handle = builder
    .with_types::<EthereumNode>()            // or .node(EthereumNode::default())
    .with_components(/* your components, e.g. a custom executor */)
    .with_add_ons(EthereumAddOns::default())
    .install_exex("silt", async move |ctx| Ok(silt_node::run_exex(ctx, cfg)))
    .extend_rpc_modules(|ctx| {
        ctx.modules.merge_configured(silt_node::rpc_module(ctx.provider().clone()))?;
        Ok(())
    })
    .launch_with_debug_capabilities()
    .await?;
```

That gives the feed (published once blocks are canonical), lookups and
`silt_multiproof`. Two more steps give what `silt-exex` has (its `main` is
the full example):

- **Read touches (PLAN D13/D16):** on the stock executor, use
  `silt_node::evm::SiltExecutorBuilder` as the components' executor (reth's
  own EVM config, recording what each payload's block reads). A node with
  its own executor reports through `silt_node::reads::ReadTouchSink`
  instead. Either way, pass the sink to `run_exex_with`.
- **Publishing at execution (D15):** pass `early::channel()`'s receiver to
  `run_exex_with` and, after launch, forward
  `handle.node.add_ons_handle.engine_events.new_listener()` with
  `early::forward_engine_events`.

The host must build against the same reth as silt (the workspace pin): one
binary can't link two reth versions. silt pins reth v2.11.0 (`b9abbcc`)
and mirrors its `[patch]` set; yevm-node pins the same, so it can depend on
silt-node as-is. `run_exex` needs Ethereum primitives
and `ChainSpec` (`NodeTypes<Primitives = EthPrimitives, ChainSpec =
ChainSpec>`); the lookup server only needs the node's provider.

## Status

- M0 (wire) and M2 (cache) complete: unit/property/mock-feed tests green;
  dependency boundaries CI-enforced (`scripts/ci.sh`; `FULL=1` adds the
  wrapper).
- M1 (ExEx) implemented and proven on a `--dev` chain end to end: blocks
  mined → diffs published → every applied account/slot verified against
  pinned RPC with zero mismatches (transfers + a storage-writing deploy).
- M4 (lookup fast path) implemented and verified against the live mainnet
  datadir through the read-only harness: 0 mismatches vs pinned `eth_*`;
  latency report in `docs/lookup-bench.md`. Measured again inside the
  running node once it installed silt-node: warm single lookups p50
  118 µs, recent-key first touch p50 149 µs, 0 mismatches.
- M5 (proofs as op 4, D10) implemented and verified the same way: every
  answer byte-identical to reth's multiproof, every `eth_getProof` node
  present (`docs/proof-bench.md`). In the node, all 440 op-4 answers equal
  `silt_multiproof` at the same block (`node-proof-bench`).
- M3 soak tooling: `verify` applies every diff to a silt-cache `Store`
  (with eviction), checks every applied key, code hash and destroyed
  account against pinned RPC, reconnects through the spool, and reports
  latency, frame bytes, apply time and RSS (`scripts/soak.sh`). A 10-block
  smoke run: 12,510 keys, 0 mismatches, diff received p50 2.8 s after the
  block's timestamp, ~280 KB per block.
- M3 restart recovery (G3.2), offline: `crates/silt-cache/tests/restart.rs`
  kills and restarts a feed server under a consuming, concurrently read
  `Cache`. A node restart that re-publishes held blocks, or a folded span
  through the consumer's tip, resumes with no break and every head once;
  a spool that no longer covers the consumer, or an empty spool that skips
  ahead, drops it to cold. Two fixes came out of it: the publisher judged
  `GapTooOld` by a span's end rather than its start, and the client now
  applies an overlapping span (`client::continuity`) instead of dropping
  to cold.
- M2 replay check (yevm adapter): `replay` (silt-cache example, feature
  `yevm`) runs each new block's transactions through yevm's `Executor` on
  a `Cache` that holds the parent state (`Cache::from_store`: the tool
  feeds the store itself and applies diff N only after block N ran), and
  compares status, gas and created address with the node's receipts.
  Live (2026-11-17): 20 mainnet blocks, 6,690 transactions, 0 mismatches,
  0 errors; block replay p50 1.0 s with the store starting cold (max 14 s,
  while `trie-follow` ran alongside).
- D11 warm state trie (silt-cache feature `trie`): `WarmTrie` follows the
  diffs, recomputes the state root per block and fetches only unseen nodes
  over op 4 (a batch per diff plus path-directed probes); offline tests
  check every root against full tries over 40 random blocks, with and
  without pruning. `trie-follow` checks it against live headers. Live
  (2026-11-17), seeded cold: 50 of 50 roots equal the mainnet headers;
  apply p50 2.2 s (first block 11.6 s), still fetching 1–4 MB of nodes per
  block at 170k revealed nodes, below the cap.
- D6 cap: catch-up segments longer than `max_span` (default 32) blocks are
  published as several diffs, each with only its own blocks' keys. In the
  node (2026-11-16): catching up ~45 blocks after a restart took spans of
  2–17 blocks, and the diff stream ran on across the restart with no gap
  or overlap; over the next 24 h, 7,241 diffs, the longest 17 blocks.
- D12 reorg rollback: a `Reorg` frame names the fork; the store and the
  warm trie undo, from a per-block journal (64 blocks), only what the
  dropped blocks wrote, then follow the new side, instead of dropping to
  cold. The feed drops the dropped blocks' diffs from its spool and tells a
  consumer that resubscribes from a dropped tip where the fork is. Tests:
  300 random reorgs where rolling back A and applying B equals following
  B; trie roots exact after rollback. The frames changed without a
  protocol version bump: every consumer is ours and is rebuilt with the
  node.
- M3 soak (G3.1, `docs/soak.md`): 24 h against the live node
  (2026-11-17/18), 7,190 blocks, 9,236,637 keys checked, 0 mismatches, 0
  gaps; 4 one-block reorgs rolled back with 973 dropped-block keys checked
  at the fork. Diff received p50 2.3 s / p99 7.0 s after the block's
  timestamp, ~260 KB per block. A node restart (2026-11-18) kept the diff
  stream continuous.
- M6 simulation latency vs reth's database (in progress): the same yevm
  executor and transactions on silt's cache and on reth's read-only
  datadir (`sim-bench`, yevm-reth). All 6,443 transactions so far have
  identical outcomes. Whole blocks are faster on silt (p50 75 vs 79 ms,
  worst 136 vs 231 ms). A transaction served from the cache takes 11 µs
  against reth's 69 µs, but each miss costs a ~150 µs round trip to the
  node, so per-transaction p50 still trails (255 vs 158 µs). D13 read
  touches and lookup view reuse target the misses.
- Pending live runs (PLAN M3 gates): a consumer connected through a node
  restart under load (G3.2), and soak 3 (the RSS curve with `882d5c9`).

Feed and lookup security model (PLAN D1): no auth/TLS — bind to localhost
or a private network only.

## Upgrades

silt's reth pin follows yevm-node's (`yevm-reth/Cargo.toml`): a host must
build against the same reth as silt-node, and yevm only moves forward. The
pin is the `reth-ethereum`/`reth-stages-types` rev in the root
`Cargo.toml`'s `[workspace.dependencies]`, shared by silt-node and
silt-exex. Bumping:

1. Change the rev (or tag) to yevm's.
2. Diff the `[patch]` section of reth's root `Cargo.toml` at the new rev
   against silt's root `Cargo.toml` and mirror it — Cargo does not apply a
   dependency's patches.
3. If the reth tree itself fails to compile, compare `Cargo.lock` with
   reth's lockfile at the rev and `cargo update -p <crate> --precise <ver>`
   the offenders.
4. `FULL=1 scripts/ci.sh`, then re-verify `docs/reth-notes.md`.

Per-version notes:

- 2.5.1 -> 2.8.1: `cargo update alloy-hardforks alloy-evm` to resolve
  incompatible dependency versions.
- 2.8.1 -> 2.10.0: reth now patches evm2 and the reth-core crates (mirrored
  in the root `Cargo.toml`). `alloy-trie` is pinned to 0.9.8 — 0.9.9 breaks
  reth-trie-common, and a bare `cargo update` undoes the pin (until
  v2.11.0, which requires 0.9.9). revm is gone: bundle types moved to
  `reth_ethereum::trie::bundle_state`.
- 2.10.0 -> 2.10.1: tag bump only. reth's evm2 patch moved from
  `DaniPopes/evm2` to `www.github.com/alloy-rs/evm2` at rev `01df8f6`
  (mirrored); reth-core patches unchanged; no crates.io version moved in
  `Cargo.lock`; `alloy-trie` stays 0.9.8 (reth's lock agrees).
- v2.10.1 -> `46731e9` (reth main, 2.10.1-dev; yevm-node's pin and the
  target node's build): rev bump only, no source changes. reth's evm2 patch
  is back on `DaniPopes/evm2` at rev `37fb326` (mirrored; same as yevm's);
  reth-core patches unchanged. `Cargo.lock` picks up reth's own new
  dependency versions (discv5 0.12, enr 0.14, ed25519-dalek 3, aes-gcm
  0.11); `alloy-trie` stays 0.9.8. Bundle types moved into evm2 but reth
  still re-exports them as `reth_ethereum::trie::bundle_state`.
- `46731e9` -> v2.11.0 (`b9abbcc`, 59 commits on; yevm-node's pin): rev
  bump. reth's evm2 patch moves to rev `66687c1` (mirrored); reth-core
  patches unchanged. `alloy-trie` must now be 0.9.9 (reth sets its new
  field): `cargo update -p alloy-trie --precise 0.9.9`; `schnellru` gives
  way to `similar`. No storage-format change (`DB_VERSION` 2, tables,
  `NIPPY_JAR_VERSION` 1 and the codecs pin unchanged), so 2.10 binaries can
  still open the datadir. reth's `eth_getProof` now caps storage keys at
  1,024; silt's op 4 and `silt_multiproof` keep their own limits.
