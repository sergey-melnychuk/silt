# Live-node soak (PLAN M3, T3.2 / G3.1)

The `verify` example (silt-cache) runs against the live mainnet node in its
own memory-capped user unit (`scripts/soak.sh`). It subscribes to the feed,
applies every diff to a silt-cache `Store` (evicting entries unread for 64
blocks, as a consumer would), and checks every key the diff wrote, every
code hash and every destroyed account against pinned `eth_*` reads at that
block (16 calls in flight). On a `Reorg` it rolls the store back to the
fork and checks the dropped blocks' keys there. The node kept importing
blocks throughout; latencies carry that noise, and any build on the same
disk shows up in them (soak 1, below).

Latency is *block timestamp → diff received*: it includes the block's
propagation to the node and its import, not only silt's publish.

## Soak 2: the G3.1 window (24 h, 0 mismatches)

2026-11-17 18:11:12 UTC → 2026-11-18 18:11 UTC. Node: yevm-node `aaae0322`
(reth v2.11.0 `b9abbcc`, silt `f701f02` with D12), started 18:07:32 UTC,
`--engine.persistence-threshold 0`. `verify` from silt `558f449`.

| metric | value |
|---|---|
| duration | 24.00 h |
| blocks (diffs) | 7,190 (7,189) |
| keys checked | 9,236,637 |
| mismatches | **0** |
| unverifiable keys | 0 |
| reconnects / gaps / reorgs / breaks | 0 / 0 / 4 / 0 |
| reorgs rolled back (dropped-block keys checked at the fork) | 4 (973), 0 mismatched |
| block timestamp → diff received, ms p50 / p99 / max | 2,285 / 7,003 / 19,100 |
| frame bytes per block p50 / p99 / max | 261,360 / 656,465 / 2,055,652 |
| feed volume | 1,889 MiB/day |
| store apply µs p50 / p99 / max | 785 / 2,398 / 23,723 |
| check ms per block p50 / p99 / max | 62 / 555 / 4,167 |
| store at end | 14,733 accounts, 32,648 slots |
| RSS at end / max | 153 / 156 MiB |

Every hourly window had 0 mismatches. The 4 reorgs were each one block deep
(forks at 26438677, 26438797, 26440769 and 26441868). This was the first
live run of D12: each one rolled back from the journal and followed the new
side, with no drop to cold.

**Memory.** RSS by hour (MiB): 55, 68, 77, 82, 84, 88, 91, 106, 114, 117,
121, 123, 123, 128, 128, 129, 129, 136, 137, 139, 144, 154, 153, 153. The
store's entries are bounded by eviction, but `Store::evict` left an empty
slot set behind for every account whose slots it dropped, so the
per-account index grew by ~4,700 sets an hour, about half of `verify`'s
growth. Fixed in `882d5c9`, measured offline by replaying 350 recorded
blocks through both stores: the index stays flat at ~2,300 sets and the
heap grows 0.8 MiB instead of 2.3 MiB over 45 minutes of mainnet. The rest
of the growth is in `verify` itself (RPC client, tasks, allocator) and is
unmeasured. Soak 3 repeats this window with the fix to show the RSS curve;
consumer guidance is in `docs/runbook.md` §11.

## Soak 1: stopped at 4.16 h (0 mismatches)

2026-11-17 13:53:04 UTC → ~18:05 UTC, against the node before its reth
2.11 upgrade. It was stopped for that upgrade: the new node speaks wire v2
(D12), which this `verify` could not follow.

| metric | value |
|---|---|
| duration | 4.16 h |
| blocks (diffs) | 1,041 (1,041) |
| keys checked | 1,466,763 |
| mismatches | **0** |
| unverifiable keys | 0 |
| reconnects / gaps / reorgs / breaks | 2,434 / 0 / 0 / 0 |
| block timestamp → diff received, ms p50 / p99 / max | 2,543 / 14,511 / 27,927 |
| frame bytes per block p50 / p99 / max | 264,120 / 562,841 / 804,616 |
| feed volume | 1,570 MiB/day |
| store apply µs p50 / p99 / max | 754 / 6,935 / 42,215 |
| check ms per block p50 / p99 / max | 79 / 2,586 / 4,819 |
| RSS at end / max | 50 / 50 MiB |

The node was stopped after block 26436226 (17:21:47 UTC) for the upgrade
and stayed down for the rest of the run; all 2,434 reconnects are the
backoff retries against the closed port. Hour 3's p99 latency of 18 s came
from 4-job builds on the node's disk (16:05–16:33 UTC), which slowed its
block import; soak 2's 7 s p99 is the cleaner figure.

## Restarts (G3.2)

- Offline: `crates/silt-cache/tests/restart.rs` (runbook §6).
- Live, feed side: the node restarted at 2026-11-18 18:44:29–18:44:35 UTC
  (SIGTERM) to change its persistence flags. Diffs ran on from 26443824 to
  26443825 with no gap, the feed listened again by 18:45:00, and the new
  spool started at the first block after the restart, so a fresh
  subscriber asking for older blocks got `GapTooOld` (runbook §6, row 3).
  A 15-block `verify` right after (18:46–18:49 UTC) checked 19,522 keys,
  0 mismatches: the first check of reads at the newest block under
  `--engine.persistence-threshold 50`.
- Still to show live: a consumer connected *through* a node restart under
  load. No consumer was connected across the 11-18 restart, and soak 1
  ended before its node came back.
