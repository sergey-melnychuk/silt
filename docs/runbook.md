# Operating silt on a reth node (PLAN T3.3)

How to build, deploy, check, roll back and upgrade a reth node that runs
silt. Written from the first production deployment (a mainnet node,
November 2026). Gate G3.3: the node's operator reviews this before silt
first runs against a production node.

## 1. What runs where

silt runs **inside** a reth node; there is no separate silt process.

| Piece | Where | Default | Exposure |
|---|---|---|---|
| Diff feed (push, D1) | silt ExEx in the node | `127.0.0.1:8585` (`bind`) | no auth, no TLS |
| Lookups and proofs (D9/D10) | the node, own threads | `127.0.0.1:8586` (`lookup_bind`) | no auth, no TLS |
| `silt_multiproof` (D8) | reth's RPC server | wherever `--http`/`--ws`/`--ipc` listen | reth's RPC rules |
| Consumers (`silt-cache`) | anywhere that can reach the above | — | — |

Two ways to get a node with silt:

- **`silt-exex`** (this repo): stock reth plus silt. Same CLI, same
  datadir, same flags as the `reth` it replaces.
- **Any other reth-based node** adds silt-node with two calls on reth's
  node launcher: `install_exex("silt", |ctx| Ok(silt_node::run_exex(ctx, cfg)))`
  and, in `extend_rpc_modules`,
  `ctx.modules.merge_configured(silt_node::rpc_module(ctx.provider().clone()))`
  (see `crates/silt-node/src/lib.rs`).

silt never writes to the node's database. The ExEx acks each block to
reth only after the diff is in its in-memory spool (D5), so a slow or dead
consumer can never hold the node back.

## 2. Configuration (`silt.toml`)

Read from the node's **working directory** at startup. Optional; a
malformed file stops the node (refusing beats silently ignoring).

```toml
bind = "127.0.0.1:8585"        # feed
spool = 128                    # diffs kept for reconnecting consumers (~25 min of blocks)
heartbeat_secs = 10            # feed heartbeat; consumers reconnect after 30 s of silence
lookup_bind = "127.0.0.1:8586" # lookups + proofs; "" disables the listener
lookup_workers = 16            # OS threads for point reads
lookup_proof_workers = 4       # OS threads for proofs (never queue ahead of reads)
lookup_proof_max_depth = 8     # proofs pinned deeper below the head answer UnknownBlock
max_span = 32                  # most blocks per diff while catching up; 0 = one diff per segment
```

**Binding (D1).** Neither listener authenticates or encrypts.
- Keep `lookup_bind` on loopback. It serves arbitrary state and proof reads,
  and a proof can cost the node a second of trie walking.
- The feed is push-only and read-only. If consumers live on other hosts,
  bind it to a private interface, or to `0.0.0.0` behind a firewall that
  admits only the consumers. With `ufw`, for example:

  ```sh
  ufw allow from 10.0.0.0/24 to any port 8585 proto tcp
  ufw deny 8585/tcp
  ```

- **Check before deploying:** `grep bind silt.toml`. A leftover
  `bind = "0.0.0.0:8585"` on a host with a public interface publishes the
  feed to the internet.

## 3. Build

The host binary and silt-node must use the **same reth revision**: one
binary cannot link two reths. silt pins one reth rev in the workspace
`Cargo.toml` and mirrors that rev's `[patch]` section. A host that depends
on silt-node pins the same rev.

```sh
CARGO_BUILD_JOBS=4 cargo build --release -p silt-exex          # stock reth + silt
# or, in the host repo: cargo build --release -p <host> --bin <node>
target/release/silt-exex --version                             # reth version + commit
strings target/release/silt-exex | grep -c "silt feed listening"   # 1: silt is in
```

A release build of reth needs several GB per job. **Next to a live node**,
cap it: `CARGO_BUILD_JOBS=4`, one build at a time, ideally inside a
memory-capped scope. On a 30 GB machine a 16-job build pushed the node into
systemd-oomd's kill range.

## 4. Deploy (swap the binary)

1. Keep the running binary: `cp bin/node bin/node.prev`.
2. Copy the new binary in.
3. Stop the node cleanly with SIGTERM, and wait for the process to exit;
   reth flushes on shutdown. The consensus client can stay up: it
   reconnects to the engine API on its own.
4. Start the node with the same flags, from the directory that holds
   `silt.toml`.

Downtime is the stop plus the start, a few seconds. The node then catches
up on the blocks it missed. While it is behind, reth imports in batches,
and silt publishes **folded spans** of at most `max_span` blocks (32 by
default; before the cap, one catch-up span was 415 blocks and 78k
accounts in one frame). Once the node is at the tip, silt is back to one
diff per block.

## 5. Check after every start

```sh
grep -E "silt (feed|lookup) listening" reth.log         # both, at startup
ss -ltn | grep -E ":(8585|8586) "                       # both listening
grep "publishing diff" reth.log | tail -3               # span=N..=N, one per block at the tip
grep -E " ERROR |panicked" reth.log                     # nothing from silt
```

Then, read-only against the node (reth-free tools from `silt-cache`):

```sh
target/release/examples/verify 127.0.0.1:8585 http://127.0.0.1:8545 --blocks 20
target/release/examples/lookup-verify --lookup 127.0.0.1:8586 --rpc http://127.0.0.1:8545
```

Both must end with `mismatches 0`. `verify` checks every key of every
diff against `eth_*` pinned at that block; `lookup-verify` checks lookups
and op-4 proofs against `eth_getProof` at the head.

**Continuity across the restart:** the first diff after the restart should
start right after the last one the old process published. `start.sh`-style
scripts that truncate `reth.log` lose the old line, so note the last
`publishing diff` span before stopping.

## 6. What consumers see when the node restarts

A consumer (`silt-cache`, or `verify`) reconnects with backoff and asks
for the block after the last one it holds. Then, depending on what the new
process has spooled (all covered by `crates/silt-cache/tests/restart.rs`):

| Restarted node's spool | Consumer sees | Result |
|---|---|---|
| Re-publishes blocks the consumer holds, then newer ones | the blocks after its tip | resumes, no break |
| A folded span that started below the consumer's tip and runs through it | that span | applied as an overlap, no break |
| Its first span starts after the consumer's next block | `GapTooOld` | drops to cold, rebuilds lazily |
| Empty, and the first new block skips ahead | a diff that does not extend its tip | drops to cold |

Cold costs time, not correctness. Reads fall back to the lookup fast path
or JSON-RPC until the feed has warmed the cache again. A consumer down
longer than the spool (128 blocks, about 25 minutes) always comes back
cold.

**Reorgs (D12).** The node sends `Reorg` with the fork block, then the new
chain's diffs. A consumer rolls its cache back to the fork (undoing only
what the dropped blocks wrote) and follows the new side; it keeps
everything else. A consumer that was disconnected through a reorg is told
the fork when it resubscribes. It drops to cold only if the reorg is
deeper than its journal (`reorg_depth`, 64 blocks) or cuts through a
folded catch-up span. `verify` counts rollbacks and checks every key of
the dropped blocks it still holds against the node at the fork.

## 7. Roll back

- **To the previous binary:** swap `bin/node.prev` back in, then stop and
  start as in §4.
- **To a node without silt:** start stock reth (same version, same
  datadir, same flags). silt keeps no state of its own in the datadir;
  reth keeps its own ExEx notification log there, and a node without the
  silt ExEx no longer uses it. Consumers lose the feed and the lookup
  listener. With RPC configured, they keep working through JSON-RPC,
  slower.

## 8. Upgrade (bump reth)

Order matters, because the host and silt must agree on reth:

1. Bump the host's reth rev.
2. Bump silt's reth pin to the same rev, re-copy reth's `[patch]` section,
   and fix any API drift (silt-node is the only crate that sees reth).
3. Point the host's `silt-node` dependency at the new silt commit.

**Wire compatibility.** The protocol isn't versioned yet: every consumer
is ours, so frames change without a version bump (PLAN D12). Build the
node and its consumers (silt-cache, `verify`, the other tools) from the
same silt commit. A consumer from an older commit can fail to decode the
newer frames rather than be refused at the handshake.

Gates before deploying:
- `FULL=1 scripts/ci.sh`;
- a deploy to a non-critical node, or a short window on the target, with
  §5's checks;
- `node-proof-bench` (0 differences, 0 root failures);
- for a large reth jump, a soak: `scripts/soak.sh 24`.

## 9. Monitoring

- **Node log:** one `silt: publishing diff span=…` line per block. A
  silence longer than a few blocks while the node imports means the ExEx
  is stuck: an incident.
- **Feed heartbeats** carry the height and spool depth every
  `heartbeat_secs`.
- **Canary:** run `verify` continuously from the consumer's host
  (`scripts/soak.sh`). Its hourly `SUMMARY` lines report mismatches,
  reconnects, gaps, breaks, latency (block timestamp to diff received,
  about 3 s p50 on mainnet), bytes per block (about 280 KB, about 2 GB a
  day), and its own memory.
- **Consumers:** silt-cache's `over its soft memory limit` WARN (§11).

## 10. Known limits

- No auth, no TLS (D1): network placement is the only access control.
- A reorg rolls back to its fork (D12). One deeper than 64 blocks, or one
  that cuts through a folded catch-up span, drops to cold (D3); consumers
  rebuild lazily. The warm trie (D11) also reseeds if a prune trimmed a
  node the undo needs.
- **First-touch block-sized proofs can be slow:** 0.95 s p50 in the node,
  one in 20 took 16 s (`docs/proof-bench.md`). The client's default
  `proof_timeout` is 5 s. Prove warm, in smaller sets, or raise it.
- Proofs deeper than `lookup_proof_max_depth` (8) below the head answer
  `UnknownBlock`. Each block of depth costs reth about 25–30 ms of revert
  overlay.

## 11. Consumer cache memory

A silt-cache store holds three things (`Store::mem_usage`, also
`Cache::mem_usage`). Sizes below were measured following the mainnet feed
with a 64-block eviction and no reads of the store's own (`verify`'s
pattern), in November 2026:

| Part (`MemUsage` field) | What bounds it | Measured |
|---|---|---|
| Accounts and slots, with their indexes (`entries`) | eviction: entries unread for `evict_after_blocks` | ~13 MiB, flat at 64 blocks (~13k accounts, ~31k slots, ~290 B each) |
| Undo journal and head ring (`journal`) | `reorg_depth` blocks (64), ~0.2 MiB each | 12–14 MiB, flat |
| Bytecode (`code`) | nothing: never evicted | 0.3–0.8 MiB/h from new deployments alone |

**Since PLAN M6 the default eviction age is 2048 blocks (~7 h,
`DEFAULT_EVICT_AFTER_BLOCKS`), and diffs carry read touches (D13).** Both
trade memory for misses: every entry kept is a simulation read that
doesn't go to the node (~135 µs each, against ~11 µs for a whole
transaction served from the cache). `entries` then holds every key the
chain wrote or read in the last 2048 blocks, plus the consumer's own hot
set. A block writes ~400 accounts and ~600 slots and reads ~600 and
~1,450, mostly the same popular contracts block after block. The
estimate, until it's measured on the feed with read touches, is **150–350
MiB of `entries`** at ~290 B per key. That's under the 512 MiB soft
limit, with the journal and code on top. If it runs over, lower
`evict_after_blocks`: the cost is misses, not correctness.

A consumer that reads adds its own hot set to `entries`, and the code of
every contract it reads to `code`. Code is content-addressed and never goes
stale, so the store keeps it even through a drop to cold (`Store::clear`).
Only a new `Cache` (or a process restart) releases it. **Code is the one
part that grows for as long as the process runs:** a consumer that runs
for weeks has to budget for it.

**Soft limit.** `CacheConfig::soft_limit_bytes` (default 512 MiB;
`None` turns it off) makes the eviction sweep log a WARN `silt-cache over
its soft memory limit` with the total and the `entries`, `code` and
`journal` parts. It logs on the first sweep over the limit, then hourly,
and an INFO line once the store is back under. It never evicts or blocks:
staying within budget is the consumer's job.

When it fires, look at which part grew:

- **`entries`:** the hot set. Lower `evict_after_blocks` (more misses,
  each a lookup or RPC read), or read less.
- **`journal`:** lower `reorg_depth` (deeper reorgs drop to cold instead
  of rolling back; `0` keeps no journal).
- **`code`:** restart the consumer on a schedule that suits it, or build a
  new `Cache`. Either starts cold: reads go to the lookup path or RPC
  until the feed warms the cache again (§6).

**Estimate vs RSS.** `mem_usage` counts table capacities and sizes, not
the allocator: it came to about 90% of the exact heap. The process RSS is
higher still, by allocator overhead and fragmentation (7–10 MiB over a 30
MiB store in a single-task probe; more with glibc's per-thread arenas).
Set the limit with headroom below the memory you can give the cache, and
watch RSS as well. `mem_usage` walks the store's maps; call it at most
every few seconds.

**`Cache::from_store`** consumers get no sweep: call `Store::evict(tip,
max_age)` regularly (`verify` does every 8 blocks) and check
`Store::mem_usage()` yourself.
