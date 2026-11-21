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

## Soak 3: memory after the eviction fix, through three node restarts (24 h, 0 mismatches)

2026-11-18 19:30:10 UTC → 2026-11-19 19:30 UTC. `verify` from silt
`49f5a75`, which includes the eviction fix `882d5c9`. Eviction after 64
blocks, 16 calls in flight. The node was replaced under it three times
(restarts, below):

| from (UTC) | node | silt in the node |
|---|---|---|
| start | yevm-node `aaae0322`, persistence threshold 50 | `f701f02` (D12) |
| 11-19 08:03 | `d2530e51`, with the in-node simulation task | `9df61c2` (D13 read touches) |
| 10:21 | the same build, without that task | `9df61c2` |
| 11:20 | `8653babd` | `673bfbe` (D14 pending read touches) |

This `verify` predates D13. It decodes the read-touch fields away and
checks only the keys each diff wrote. The read touches themselves are
checked by the run that follows.

| metric | value |
|---|---|
| duration | 24.00 h |
| blocks (diffs) | 7,214 (7,015) |
| keys checked | 8,988,443 |
| mismatches | **0** |
| unverifiable keys | 0 |
| reconnects / gaps / reorgs / breaks | 1,844 / 0 / 3 / 0 |
| reorgs rolled back (dropped-block keys checked at the fork) | 3 (620), 0 mismatched |
| re-deliveries after a node restart: overlapping spans / stale diffs | 3 / 5 |
| block timestamp → diff received, ms p50 / p99 / max | 2,199 / 6,100 / 39,980 |
| frame bytes per block p50 / p99 / max | 312,048 / 753,284 / 1,005,169 |
| feed volume | 2,233 MiB/day |
| store apply µs p50 / p99 / max | 722 / 1,938 / 9,211 |
| check ms per block p50 / p99 / max | 72 / 157 / 12,816 |
| store at end | 12,913 accounts, 27,078 slots |
| RSS at end / max | 181 / 181 MiB |

Every hourly window had 0 mismatches. The 3 reorgs were each one block
deep (forks at 26444491, 26446374 and 26447417). All 1,844 reconnects fell
inside the three restart windows.

- **Latency outliers.** The two worst latencies (40 s and 26 s) were blocks
  whose import in the node took 35 s (03:02 UTC) and 13 s (04:17 UTC).
- **Check maximum.** The 12.8 s check was the 31-block span re-delivered
  after the third restart.

**Feed size with read touches.** Before D13, diffs ran 237–266 KB per block
(hourly p50s), or 72–78 MiB an hour. From the D13 node on, they ran
349–433 KB per block, or 99–121 MiB an hour. That is about 40% more, close
to 2.7 GB a day; soak 2 averaged 79 MiB an hour without read touches.

**Memory.**
- RSS by hour (MiB): 55, 60, 67, 63, 73, 72, 81, 82, 89, 82, 88, 89, 107,
  102, 122, 140, 153, 156, 166, 171, 177, 180, 181, 181.
- **Hours 1–12** match soak 2's window with the eviction fix: 89 MiB at
  12 h against 123.
- **Hours 13–24.** RSS rose again from hour 13 and stopped at 180–181 MiB
  for the last three hours.
  - The store did not grow: it held 9.5k–16.1k accounts and 26k–41k slots
    in every hour of the run. The growth is in `verify`'s own heap.
  - It began with the first restart and continued through hours 17–22,
    which had no restart.
  - Two things changed at hour 13: catch-up spans of 7–9 MB after each
    restart, and diffs about 40% larger, carrying read touches this
    `verify` decodes away.
  - Neither was measured as the cause.
- The run after this one (the current `verify`, eviction after 2,048
  blocks) shows whether it repeats.

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
unmeasured. Soak 3 (above) repeats this window with the fix; consumer
guidance is in `docs/runbook.md` §11.

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
- **Live, a consumer connected through a restart (G3.2), 2026-11-19.** Soak
  3's `verify` stayed connected while the node was stopped and replaced by
  a new build (silt `9df61c2`, D13).
  - The node stopped at 07:55:10 UTC. The feed was down from the last old
    diff (26447764, 07:55:04) to the first new one (08:08:30), about
    13.5 min; `verify` retried with backoff meanwhile.
  - At startup the node rolled back to 26447728. It was the first restart
    under state masking, and the persisted state lagged the blocks. It
    re-executed from there, so the new process re-published blocks
    `verify` already held. The consumer side handled them as designed:
    `STALE: diff 26447743 already applied; skipped`, then
    `OVERLAP: span 26447744..26447775 passes through our tip (node
    restart); applied`. That span's 20,680 keys were checked against RPC,
    0 mismatches, with no break and no drop to cold.
  - This `verify` predates D13. From 26447832 on, the new node's diffs
    carry read touches (183 accounts and 812 slots in that first block),
    and the old decoder kept reading them unchanged.
  - A second restart the same morning, 10:15–10:21 UTC (the same build
    without the in-node simulation task), went the same way. Two stale
    spans (to 26448445 and 26448461) were skipped, then `OVERLAP: span
    26448462..26448493` was applied: 22,925 keys checked, 0 mismatches.
  - A third, 11:11–11:26 UTC, brought in D14 (node `8653babd`, silt
    `673bfbe`). The feed was down from the last old diff (26448741,
    11:11:14) to the first new one (11:25:56), about 15 min. The new node
    re-published from 26448699. `verify` skipped two stale spans (to
    26448700 and 26448732) and applied `OVERLAP: span 26448733..26448763`:
    21,386 keys checked, 0 mismatches.
  - Over the whole of soak 3: 3 restarts, 5 stale diffs skipped, 3
    overlapping spans applied, 0 gaps, 0 breaks, no drop to cold.
