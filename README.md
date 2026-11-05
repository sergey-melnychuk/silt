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
| `silt-wire` | wire types, postcard codec, framing, handshake | no |
| `silt-exex` | stock reth + the feed ExEx + TCP publisher + `silt_multiproof` RPC | pinned `v2.5.1` |
| `silt-cache` | subscriber, cache, RPC fallback, Chain-shaped API (+ optional `yevm` adapter) | no |

## Run

```sh
cargo build --release                       # wire + cache (reth-free)
cargo build --release -p silt-exex          # the node wrapper (compiles reth)

# the wrapper IS reth — same CLI, same datadir; silt.toml (optional) sets
# [bind/spool/heartbeat_secs], default feed on 127.0.0.1:8585
./target/release/silt-exex node <your usual reth flags>

# watch + verify the feed against RPC (checks every applied key, pinned):
cargo run -p silt-cache --example verify -- 127.0.0.1:8585 http://127.0.0.1:8545
```

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
let cfg = CacheConfig::new("http://node:8545").with_feed("node:8585");
let (cache, mut heads) = silt_cache::Cache::connect(cfg);
// cache.account(&addr) / cache.storage(&addr, &slot) / cache.head(n)
// feature "yevm": Executor::run(&tx, &head, &mut overlay(), &*cache)
```

## Status

- M0 (wire) and M2 (cache) complete: unit/property/mock-feed tests green;
  dependency boundaries CI-enforced (`scripts/ci.sh`; `FULL=1` adds the
  wrapper).
- M1 (ExEx) implemented and proven on a `--dev` chain end to end: blocks
  mined → diffs published → every applied account/slot verified against
  pinned RPC with zero mismatches (transfers + a storage-writing deploy).
- Pending (PLAN M2/M3 gates): yevm `Executor::run` replay smoke, wrapper
  kill/catch-up test under load, and the 24 h live-node equivalence soak.

Feed security model (PLAN D1): no auth/TLS — bind to localhost or a private
network only.

## Upgrades

Bumping reth (the `reth-ethereum` tag in `crates/silt-exex/Cargo.toml`):

1. Change the tag.
2. Diff the `[patch]` section of reth's root `Cargo.toml` at the new tag
   against silt's root `Cargo.toml` and mirror it — Cargo does not apply a
   dependency's patches.
3. If the reth tree itself fails to compile, compare `Cargo.lock` with
   reth's lockfile at the tag and `cargo update -p <crate> --precise <ver>`
   the offenders.
4. `FULL=1 scripts/ci.sh`, then re-verify `docs/reth-notes.md`.

Per-version notes:

- 2.5.1 -> 2.8.1: `cargo update alloy-hardforks alloy-evm` to resolve
  incompatible dependency versions.
- 2.8.1 -> 2.10.0: reth now patches evm2 and the reth-core crates (mirrored
  in the root `Cargo.toml`). `alloy-trie` is pinned to 0.9.8 — 0.9.9 breaks
  reth-trie-common, and a bare `cargo update` undoes the pin. revm is gone:
  bundle types moved to `reth_ethereum::trie::bundle_state`.
