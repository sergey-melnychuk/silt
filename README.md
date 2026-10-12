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
| `silt-exex` | stock reth + the feed ExEx + TCP publisher | pinned `v2.5.1` |
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
