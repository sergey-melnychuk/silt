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
| `silt-node` | library any reth node installs: the feed ExEx + TCP publisher, lookup + proof server, `silt_multiproof` RPC | pinned `46731e9` (2.10.1-dev) |
| `silt-exex` | thin binary: stock reth with silt-node installed | pinned `46731e9` (2.10.1-dev) |
| `silt-cache` | subscriber, cache, lookup client, RPC fallback, Chain-shaped API (+ optional `yevm` adapter) | no |

## Run

```sh
cargo build --release                       # wire + cache (reth-free)
cargo build --release -p silt-exex          # the node wrapper (compiles reth)

# the wrapper IS reth — same CLI, same datadir; silt.toml (optional, in the
# working directory) sets bind/spool/heartbeat_secs/lookup_bind/lookup_workers/
# lookup_proof_workers/lookup_proof_max_depth;
# defaults: feed on 127.0.0.1:8585, lookup fast path on 127.0.0.1:8586
./target/release/silt-exex node <your usual reth flags>

# watch + verify the feed against RPC (checks every applied key, pinned):
cargo run -p silt-cache --example verify -- 127.0.0.1:8585 http://127.0.0.1:8545
```

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

`silt-node` is a library; any reth node installs it with two builder calls
(this is all of `silt-exex`'s `main`):

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

The host must build against the same reth as silt (the workspace pin): one
binary can't link two reth versions. silt pins yevm-node's reth rev
(`46731e9`) and its `[patch]` set, so yevm-node can depend on silt-node
as-is. `run_exex` needs Ethereum primitives
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
  latency report in `docs/lookup-bench.md`. In-node measurement waits for
  the node to run silt (silt-exex, or a node that installs silt-node).
- M5 (proofs as op 4, D10) implemented and verified the same way: every
  answer byte-identical to reth's multiproof, every `eth_getProof` node
  present (`docs/proof-bench.md`).
- Pending (PLAN M2/M3 gates): yevm `Executor::run` replay smoke, wrapper
  kill/catch-up test under load, and the 24 h live-node equivalence soak.

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
  reth-trie-common, and a bare `cargo update` undoes the pin. revm is gone:
  bundle types moved to `reth_ethereum::trie::bundle_state`.
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
