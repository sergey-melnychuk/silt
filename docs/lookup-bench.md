# Lookup fast path — benchmark and verification (PLAN M4, G4.4/G4.5)

Measured 2026-11-10 on the target machine (16 cores, SATA SSD), against
the **live mainnet datadir** of the running node, opened read-only by
`lookup-ro` (silt-node example; the same `LookupServer` code the ExEx runs,
16 read workers), with the reth-free `lookup-bench` / `lookup-verify`
examples in silt-cache as the client. Loopback TCP, `TCP_NODELAY` both
sides. The node kept importing blocks throughout (its block writes are what
stall cold reads), so absolute numbers carry that noise.

## Method

- **Keys**: real keys from the node's RPC — transaction senders and
  recipients, log emitters (receipts are retained ~64 blocks on this
  `--minimal` node, so the old pool has none), access-list storage keys.
  `recent` = the ~40 newest blocks at or below the server's tip (the
  read-only harness's tip is its state frontier, ~0–30 blocks behind the
  node); `old` = the same kinds of keys from blocks 8000 back (never
  touched by earlier runs, so cold in the OS page cache, which is shared
  with the live node and was **not** dropped).
- **Pin**: `latest` — every request reads at the server's tip, the path a
  builder pinned to its cache head takes. A `--pin fixed` run is below.
- **Modes**, each on its own disjoint slice of a pool, run twice: *first
  touch*, then *warm* (same keys again). Singles ask exactly what
  `LookupFallback` asks (`Account` + inline code, or one `Storage`);
  batches are one op per frame (accounts without inline code — 256 exceeds
  the 64-item cap for code-carrying frames).
- **Columns**: *round trip* = client clock around the call (encode → typed
  result); *server* = `server_us` (decode → encode on the server: pool
  queue wait + provider open + reads + encode); *transport* = round trip −
  server (both kernels' loopback, wakeups, client/server framing).
  *Server disk reads* = `read_bytes` / major faults of the server process
  over the pass (`/proc/<pid>/io`, `/proc/<pid>/stat`): non-zero = real SSD
  reads.

## Results (final build, client on a current-thread runtime)

| pool | pass | mode | requests | keys | keys/s | round trip µs p50/p99/p999 | server µs p50/p99/p999 | transport µs p50/p99/p999 | server disk reads (MiB / major faults) | errors |
|---|---|---|---:|---:|---:|---|---|---|---:|---:|
| recent | first touch | single, sequential | 1000 | 1000 | 1944 | 303 / 2448 / 3246 | 125 / 2218 / 2894 | 148 / 323 / 369 | 3.3 / 750 | 0 |
| recent | warm | single, sequential | 1000 | 1000 | 16470 | 54 / 154 / 235 | 21 / 87 / 147 | 33 / 86 / 140 | 0.0 / 0 | 0 |
| recent | first touch | single, 16 in flight | 2000 | 2000 | 37760 | 86 / 2993 / 3663 | 27 / 2944 / 3628 | 44 / 145 / 187 | 6.1 / 1353 | 0 |
| recent | warm | single, 16 in flight | 2000 | 2000 | 114973 | 108 / 161 / 184 | 18 / 38 / 50 | 88 / 141 / 157 | 0.0 / 0 | 0 |
| recent | first touch | single, 256 in flight | 4000 | 4000 | 44870 | 5316 / 8597 / 9660 | 5253 / 8548 / 9544 | 54 / 1455 / 1715 | 11.3 / 2580 | 0 |
| recent | warm | single, 256 in flight | 4000 | 4000 | 150677 | 1255 / 3222 / 3974 | 24 / 275 / 296 | 1228 / 3200 / 3702 | 0.0 / 0 | 0 |
| recent | first touch | batch of 16 | 128 | 2048 | 5571 | 2936 / 5664 / 7094 | 2755 / 5428 / 6784 | 162 / 383 / 438 | 4.4 / 1135 | 0 |
| recent | warm | batch of 16 | 128 | 2048 | 55352 | 261 / 495 / 510 | 140 / 237 / 239 | 117 / 283 / 319 | 0.0 / 0 | 0 |
| recent | first touch | batch of 256 | 17 | 4096 | 6391 | 38087 / 52099 / 52099 | 37808 / 51620 / 51620 | 331 / 504 / 504 | 8.6 / 2190 | 0 |
| recent | warm | batch of 256 | 17 | 4096 | 263070 | 904 / 1651 / 1651 | 802 / 1534 / 1534 | 113 / 168 / 168 | 0.0 / 0 | 0 |
| old | first touch | single, sequential | 1000 | 1000 | 1216 | 772 / 2316 / 2935 | 613 / 2140 / 2686 | 166 / 387 / 452 | 7.1 / 1710 | 0 |
| old | warm | single, sequential | 1000 | 1000 | 12637 | 79 / 168 / 247 | 27 / 67 / 95 | 49 / 135 / 158 | 0.0 / 0 | 0 |
| old | first touch | single, 16 in flight | 2000 | 2000 | 21770 | 757 / 2652 / 3078 | 716 / 2614 / 3023 | 44 / 147 / 188 | 11.8 / 2845 | 0 |
| old | warm | single, 16 in flight | 2000 | 2000 | 114655 | 105 / 199 / 525 | 19 / 78 / 123 | 85 / 157 / 449 | 0.0 / 0 | 0 |
| old | first touch | single, 256 in flight | 4000 | 4000 | 23218 | 10763 / 13659 / 14811 | 10717 / 13598 / 14778 | 45 / 427 / 910 | 22.9 / 5425 | 0 |
| old | warm | single, 256 in flight | 4000 | 4000 | 140328 | 1613 / 3282 / 3306 | 21 / 53 / 77 | 1586 / 3261 / 3285 | 0.0 / 0 | 0 |
| old | first touch | batch of 16 | 129 | 2048 | 2386 | 6811 / 11314 / 12216 | 6581 / 10903 / 11763 | 264 / 488 / 489 | 9.6 / 2461 | 0 |
| old | warm | batch of 16 | 129 | 2048 | 64187 | 190 / 567 / 575 | 123 / 270 / 308 | 67 / 336 / 342 | 0.0 / 0 | 0 |
| old | first touch | batch of 256 | 17 | 4096 | 2863 | 104627 / 119869 / 119869 | 104106 / 119374 / 119374 | 419 / 617 / 617 | 18.7 / 4792 | 0 |
| old | warm | batch of 256 | 17 | 4096 | 153515 | 1541 / 2051 / 2051 | 1224 / 1761 / 1761 | 267 / 380 / 380 | 0.0 / 0 | 0 |

baseline: pinned eth_* JSON-RPC, sequential, same recent keys (account = 3 calls): 300 reads, µs p50/p99/p999 175 / 2077 / 2601

### Supplementary runs

Both were measured on the build *before* the combining `FrameWriter`
(see Findings), so their 256-in-flight server times still show the old
socket-lock queueing; the single and batch rows are comparable.

`--pin fixed` (every request pinned to the hash that was latest at start;
once the node persists past it, the harness serves it through reth's
history path: history index + changesets per key). The first-touch
columns here are warmer — earlier runs had touched these blocks:

| pool | pass | mode | requests | keys | keys/s | round trip µs p50/p99/p999 | server µs p50/p99/p999 | transport µs p50/p99/p999 | server disk reads (MiB / major faults) | errors |
|---|---|---|---:|---:|---:|---|---|---|---:|---:|
| recent | first touch | single, sequential | 250 | 250 | 8714 | 66 / 1156 / 1657 | 25 / 1108 / 1621 | 37 / 166 / 185 | 0.1 / 28 | 0 |
| recent | warm | single, sequential | 250 | 250 | 11796 | 76 / 200 / 220 | 24 / 98 / 110 | 49 / 139 / 143 | 0.0 / 0 | 0 |
| recent | first touch | single, 16 in flight | 500 | 500 | 42779 | 319 / 1556 / 2261 | 42 / 1197 / 1833 | 270 / 758 / 967 | 0.2 / 34 | 0 |
| recent | warm | single, 16 in flight | 500 | 500 | 68103 | 209 / 582 / 667 | 25 / 83 / 88 | 182 / 545 / 638 | 0.0 / 0 | 0 |
| recent | first touch | single, 256 in flight | 1000 | 1000 | 84561 | 2387 / 3962 / 4701 | 2156 / 3803 / 4527 | 176 / 1063 / 1174 | 0.5 / 71 | 0 |
| recent | warm | single, 256 in flight | 1000 | 1000 | 104673 | 2212 / 2757 / 3065 | 2004 / 2317 / 2348 | 218 / 1478 / 1535 | 0.0 / 0 | 0 |
| recent | first touch | batch of 16 | 33 | 512 | 117735 | 109 / 666 / 666 | 76 / 617 / 617 | 33 / 79 / 79 | 0.0 / 2 | 0 |
| recent | warm | batch of 16 | 33 | 512 | 178898 | 88 / 159 / 159 | 46 / 90 / 90 | 35 / 72 / 72 | 0.0 / 0 | 0 |
| recent | first touch | batch of 256 | 5 | 1024 | 119609 | 1431 / 4040 / 4040 | 1345 / 3969 / 3969 | 86 / 132 / 132 | 0.1 / 16 | 0 |
| recent | warm | batch of 256 | 5 | 1024 | 334785 | 621 / 879 / 879 | 585 / 758 / 758 | 73 / 121 / 121 | 0.0 / 0 | 0 |
| old | first touch | single, sequential | 250 | 250 | 4690 | 97 / 1622 / 2078 | 36 / 1543 / 1970 | 53 / 163 / 187 | 0.4 / 80 | 0 |
| old | warm | single, sequential | 250 | 250 | 10614 | 80 / 256 / 348 | 29 / 126 / 149 | 50 / 148 / 247 | 0.0 / 0 | 0 |
| old | first touch | single, 16 in flight | 500 | 500 | 43813 | 199 / 2034 / 2982 | 35 / 1848 / 2781 | 139 / 602 / 1085 | 0.6 / 132 | 0 |
| old | warm | single, 16 in flight | 500 | 500 | 66593 | 212 / 689 / 948 | 24 / 77 / 120 | 185 / 672 / 916 | 0.0 / 0 | 0 |
| old | first touch | single, 256 in flight | 1000 | 1000 | 69297 | 2891 / 4575 / 5431 | 2732 / 4537 / 5214 | 84 / 1436 / 1499 | 1.5 / 325 | 0 |
| old | warm | single, 256 in flight | 1000 | 1000 | 94811 | 2175 / 3055 / 4029 | 1597 / 2467 / 2480 | 482 / 1869 / 2522 | 0.0 / 0 | 0 |
| old | first touch | batch of 16 | 33 | 512 | 13304 | 1174 / 2642 / 2642 | 1093 / 2547 / 2547 | 89 / 195 / 195 | 0.5 / 119 | 0 |
| old | warm | batch of 16 | 33 | 512 | 109027 | 141 / 259 / 259 | 71 / 116 / 116 | 58 / 156 / 156 | 0.0 / 0 | 0 |
| old | first touch | batch of 256 | 5 | 1024 | 18456 | 14766 / 17591 / 17591 | 14658 / 17462 / 17462 | 164 / 259 / 259 | 0.8 / 192 | 0 |
| old | warm | batch of 256 | 5 | 1024 | 246289 | 916 / 1021 / 1021 | 760 / 864 / 864 | 164 / 196 / 196 | 0.0 / 0 | 0 |

Client on a 4-thread runtime (`--threads 4`), pin latest:

| pool | pass | mode | requests | keys | keys/s | round trip µs p50/p99/p999 | server µs p50/p99/p999 | transport µs p50/p99/p999 | server disk reads (MiB / major faults) | errors |
|---|---|---|---:|---:|---:|---|---|---|---:|---:|
| recent | first touch | single, sequential | 500 | 500 | 14047 | 51 / 573 / 1363 | 22 / 536 / 1324 | 28 / 139 / 335 | 0.1 / 17 | 0 |
| recent | warm | single, sequential | 500 | 500 | 12490 | 77 / 144 / 249 | 31 / 53 / 144 | 47 / 101 / 172 | 0.0 / 0 | 0 |
| recent | first touch | single, 16 in flight | 1000 | 1000 | 55709 | 190 / 1248 / 2470 | 27 / 942 / 2105 | 160 / 710 / 863 | 0.4 / 44 | 0 |
| recent | warm | single, 16 in flight | 1000 | 1000 | 70705 | 161 / 792 / 1341 | 20 / 84 / 101 | 140 / 759 / 1286 | 0.0 / 0 | 0 |
| recent | first touch | single, 256 in flight | 2000 | 2000 | 97215 | 1826 / 2720 / 3372 | 434 / 2256 / 3205 | 698 / 2558 / 3257 | 0.3 / 45 | 0 |
| recent | warm | single, 256 in flight | 2000 | 2000 | 94624 | 2509 / 3195 / 3472 | 2317 / 2738 / 2752 | 211 / 1920 / 2183 | 0.0 / 0 | 0 |
| recent | first touch | batch of 16 | 64 | 1024 | 121939 | 105 / 458 / 715 | 74 / 409 / 650 | 33 / 87 / 247 | 0.0 / 4 | 0 |
| recent | warm | batch of 16 | 64 | 1024 | 168408 | 92 / 135 / 141 | 64 / 102 / 103 | 29 / 38 / 43 | 0.0 / 0 | 0 |
| recent | first touch | batch of 256 | 9 | 2048 | 159206 | 1216 / 2742 / 2742 | 1146 / 2619 / 2619 | 101 / 150 / 150 | 0.1 / 15 | 0 |
| recent | warm | batch of 256 | 9 | 2048 | 277168 | 772 / 1266 / 1266 | 674 / 1119 / 1119 | 102 / 168 / 168 | 0.0 / 0 | 0 |
| old | first touch | single, sequential | 500 | 500 | 8918 | 72 / 856 / 2239 | 28 / 797 / 2111 | 43 / 111 / 155 | 0.3 / 61 | 0 |
| old | warm | single, sequential | 500 | 500 | 11492 | 68 / 627 / 1700 | 26 / 66 / 1403 | 42 / 136 / 1676 | 0.0 / 0 | 0 |
| old | first touch | single, 16 in flight | 1000 | 1000 | 61147 | 216 / 1256 / 2123 | 26 / 1158 / 2069 | 186 / 527 / 695 | 0.5 / 88 | 0 |
| old | warm | single, 16 in flight | 1000 | 1000 | 76404 | 167 / 523 / 735 | 21 / 57 / 76 | 147 / 498 / 688 | 0.0 / 0 | 0 |
| old | first touch | single, 256 in flight | 2000 | 2000 | 79513 | 2654 / 4079 / 4690 | 2421 / 3792 / 4589 | 280 / 2222 / 2368 | 0.8 / 152 | 0 |
| old | warm | single, 256 in flight | 2000 | 2000 | 90139 | 2688 / 3273 / 3503 | 2497 / 2782 / 2820 | 222 / 1238 / 1350 | 0.0 / 0 | 0 |
| old | first touch | batch of 16 | 65 | 1024 | 53113 | 136 / 1317 / 1359 | 84 / 1280 / 1304 | 36 / 106 / 108 | 0.2 / 47 | 0 |
| old | warm | batch of 16 | 65 | 1024 | 145168 | 102 / 203 / 209 | 68 / 119 / 152 | 33 / 84 / 87 | 0.0 / 0 | 0 |
| old | first touch | batch of 256 | 9 | 2048 | 64363 | 3845 / 5725 / 5725 | 3712 / 5590 / 5590 | 134 / 150 / 150 | 0.3 / 81 | 0 |
| old | warm | batch of 256 | 9 | 2048 | 186472 | 1259 / 1894 / 1894 | 1065 / 1703 / 1703 | 207 / 246 / 246 | 0.0 / 0 | 0 |

### Re-run after the reth v2.10.1 bump

Same harness and client after the `v2.10.0 -> v2.10.1` dependency bump
(no source changes), single and batch-of-16 modes, `--old-offset 12000`
for fresh cold keys. Matches the v2.10.0 run within this machine's noise:

| pool | pass | mode | requests | keys | keys/s | round trip µs p50/p99/p999 | server µs p50/p99/p999 | transport µs p50/p99/p999 | server disk reads (MiB / major faults) | errors |
|---|---|---|---:|---:|---:|---|---|---|---:|---:|
| recent | first touch | single, sequential | 1000 | 1000 | 4100 | 137 / 1732 / 2493 | 50 / 1600 / 2451 | 79 / 243 / 317 | 1.6 / 281 | 0 |
| recent | warm | single, sequential | 1000 | 1000 | 10447 | 91 / 223 / 298 | 34 / 108 / 122 | 56 / 159 / 198 | 0.0 / 0 | 0 |
| recent | first touch | single, 16 in flight | 2000 | 2000 | 65283 | 82 / 2546 / 4595 | 23 / 2485 / 4529 | 56 / 167 / 305 | 2.7 / 503 | 0 |
| recent | warm | single, 16 in flight | 2000 | 2000 | 121935 | 94 / 165 / 194 | 19 / 40 / 50 | 75 / 146 / 172 | 0.0 / 0 | 0 |
| recent | first touch | batch of 16 | 129 | 2048 | 15023 | 1013 / 2605 / 2629 | 839 / 2369 / 2442 | 192 / 449 / 850 | 1.1 / 270 | 0 |
| recent | warm | batch of 16 | 129 | 2048 | 68104 | 206 / 493 / 532 | 109 / 246 / 251 | 96 / 314 / 327 | 0.0 / 0 | 0 |
| old | first touch | single, sequential | 1000 | 1000 | 1060 | 876 / 2836 / 3467 | 656 / 2578 / 3231 | 217 / 415 / 485 | 7.0 / 1642 | 0 |
| old | warm | single, sequential | 1000 | 1000 | 9885 | 82 / 262 / 325 | 27 / 120 / 188 | 51 / 171 / 215 | 0.0 / 0 | 0 |
| old | first touch | single, 16 in flight | 2000 | 2000 | 22687 | 623 / 2890 / 3358 | 574 / 2848 / 3303 | 42 / 129 / 179 | 11.3 / 2625 | 0 |
| old | warm | single, 16 in flight | 2000 | 2000 | 113996 | 106 / 160 / 173 | 19 / 37 / 44 | 86 / 140 / 149 | 0.0 / 0 | 0 |
| old | first touch | batch of 16 | 129 | 2048 | 2625 | 6037 / 10388 / 11897 | 5761 / 10109 / 11658 | 288 / 491 / 496 | 8.6 / 2212 | 0 |
| old | warm | batch of 16 | 129 | 2048 | 100510 | 128 / 482 / 630 | 78 / 215 / 229 | 43 / 318 / 401 | 0.0 / 0 | 0 |

baseline: pinned eth_* JSON-RPC, sequential, same recent keys (account = 3 calls): 300 reads, µs p50/p99/p999 431 / 938 / 1263

## Verification (`lookup-verify`, G4.4)

Every key read over the lookup protocol vs the node's `eth_getBalance` /
`eth_getTransactionCount` / `eth_getCode` / `eth_getStorageAt` at the
**same block hash** (`{"blockHash": …}`), plus `code_hash` vs
keccak(code) (`KECCAK_EMPTY` for none), `Absent` accounts vs RPC zeros,
`Code(hash)` vs the inline code, and an unknown pin vs `UnknownBlock`:

```
done at block 26385193: 5538 accounts (1530 with code, 19 absent, 0 item errors), 1045 code hashes, 2773 slots (2338 non-zero); keys checked 8311; unknown pin -> UnknownBlock: true; mismatches 0
done at block 26385213: 5532 accounts (1599 with code, 22 absent, 0 item errors), 1101 code hashes, 2726 slots (2317 non-zero); keys checked 8258; unknown pin -> UnknownBlock: true; mismatches 0
done at block 26385554: 5698 accounts (1597 with code, 13 absent, 0 item errors), 1094 code hashes, 2958 slots (2330 non-zero); keys checked 8656; unknown pin -> UnknownBlock: true; mismatches 0   # reth v2.10.1
done at block 26386303: 5661 accounts (1470 with code, 13 absent, 0 item errors), 1047 code hashes, 2732 slots (2230 non-zero); keys checked 8393; unknown pin -> UnknownBlock: true; mismatches 0   # reth 46731e9
```

**0 mismatches** in all four runs (two on v2.10.0, one on v2.10.1, one on
`46731e9`, the rev yevm-node pins and the node runs).

## Findings

- **Single lookups are sub-millisecond end to end.** Warm: p50 54–79 µs,
  p99 154–168 µs, of which transport is p50 33–49 µs (two cross-process
  wakeups over loopback; about the floor without busy-polling). First
  touch with real SSD reads: p50 0.30 ms (recent keys) / 0.77 ms (cold
  keys 8000 blocks back); the p99 of 2.3–2.4 ms is the SSD under the
  node's write load (the task's own measurement: 12–78 ms stalls while the
  disk is busy; single reads up to ~1 s were seen in earlier smoke runs).
  The pinned `eth_*` baseline on the same keys: p50 175 µs, p99 2.1 ms.
- **Keep ~16 requests in flight.** Warm p50 105–108 µs, p99 161–199 µs at
  ~115k keys/s on one connection; cold first touch p50 86 µs / 757 µs.
  256 in flight adds no throughput for single keys and only queues: the
  ~1.3–1.6 ms round trips there are Little's law at ~140–150k req/s, spent
  in the single-threaded client (server time stays ~20 µs).
- **Batches amortize the per-frame cost but serialize the disk.** Warm
  batches of 256 reach 150–260k keys/s; cold ones take 38–105 ms because
  a frame's reads run one after another on one worker (one snapshot per
  frame, D9). For cold key sets, several small frames in flight beat one
  big frame by an order of magnitude.
- **Two fixes came out of measuring.** (1) The server used to release a
  request's in-flight slot only after its answer was flushed, so a client
  refilling its window at exactly 256 got `Busy` (11 of 400 in one smoke
  run); the slot is now released when the answer is built
  (regression test `client_at_the_inflight_cap_never_sees_busy`). (2) Each
  answer used to hop from the read worker to a writer task, and under load
  workers then queued on the socket lock one syscall each (single-client
  ceiling ~100k req/s, `server_us` ~2.2 ms at 256 in flight). The
  combining `FrameWriter` writes from the worker thread and coalesces
  concurrent answers into one vectored write: `server_us` at 256 in flight
  fell to ~25 µs, 16-in-flight p99 from ~0.4–0.8 ms to <0.2 ms.
- **Harness vs in-node.** In the ExEx, tip reads go through reth's
  in-memory overlay on top of the hashed state; the harness reads the
  datadir at its state frontier, and anything behind the persisted tip
  goes through reth's history path (see `--pin fixed`). The in-node
  server time should be at or below the harness numbers; it can only be
  measured once the node runs silt.

## Reproduce

```sh
cargo build --release -p silt-node --example lookup-ro
cargo build --release -p silt-cache --example lookup-bench --example lookup-verify
./target/release/examples/lookup-ro --datadir /home/work/Temp/reth-node/datadir \
    --bind 127.0.0.1:8587 --workers 16 &          # ready after ~1 min (WAL replay)
./target/release/examples/lookup-bench --server-pid $(pgrep -x lookup-ro) --old-offset 8000
./target/release/examples/lookup-verify
kill -TERM $(pgrep -x lookup-ro)                  # removes its scratch dir
```
