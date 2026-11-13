# Proof path: where the time goes (`proof-bench`)

Question: what does a trie proof cost as reth's walk alone, over
`silt_multiproof` (D8, JSON-RPC), and over the binary lookup op
`Proof = 4` (D10)? Does op 4 answer exactly what reth says? The first run
(2026-11-12, "Before op 4" below) measured only the walk and JSON-RPC,
and it led to D10. The current bench measures the real op 4.

## Op 4 (D10), 2026-11-13

`crates/silt-node/examples/proof-bench.rs` opens the live node's datadir
read-only (reth `46731e9`, mainnet, the same harness as `lookup-ro`).
Before each request it re-pins to the current state frontier, untimed: a
client proves at the tip. It runs every request three ways:

- **walk**: `StateProofProvider::multiproof` in-process, the floor for
  any transport;
- **JSON-RPC**: the real `silt_multiproof` module (jsonrpsee, HTTP,
  loopback, reth's default size limits): encode, round trip, typed parse;
- **op 4**: the real `LookupServer` (default config) on the same
  read-only source, after the D9 handshake: encode, round trip
  (`server_us` splits out the server's share), and a zero-copy decode
  that touches every node.

Checks (any failure exits 1):
- keccak(root node) must equal the header's state root, in-process and
  over RPC;
- every op-4 answer must be **byte-identical** (header, every node, every
  storage trie) to `write_multiproof` of the in-process walk at the same
  pin.

Run with `--blocks 20 --small 200 --depths 0,1,4,16,32`, under a 6 GB
memory cap and a pressure guard (see Reproduce). Frontier
26404794–26404795. **0 of 440 op-4 answers differ from reth; 0 root
failures.** Times are in µs.

| workload | reqs | keys/req | nodes/req | op 4 KiB | JSON KiB | walk, first pass p50/p90/max | walk p50/p90/max | build+json p50 |
|---|---:|---:|---:|---:|---:|---|---|---:|
| block | 20 | 1397 | 7484 | 2566.5 | 5125.9 | 759085 / 1389428 / 1799370 | 56844 / 78333 / 95951 | 5873 |
| acct-1 | 200 | 1 | 10 | 4.0 | 8.0 | 122 / 207 / 347 | 144 / 239 / 404 | 8 |
| slot-1 | 200 | 2 | 15 | 6.1 | 12.1 | 215 / 262 / 321 | 264 / 387 / 511 | 13 |
| acct-64 | 20 | 64 | 511 | 191.5 | 383.2 | 4343 / 4771 / 5315 | 4390 / 5109 / 5329 | 400 |

| workload | JSON-RPC p50/p90/max (enc / rtt / parse p50) | op 4 p50/p90/max (enc / rtt / decode p50) | op 4 server_us p50 | op 4 transport p50 | op 4 − walk p50 | saved vs JSON-RPC p50 |
|---|---|---|---:|---:|---:|---|
| block | 67826 / 92607 / 118953 (160 / 65334 / 2170) | 60224 / 83962 / 104405 (14 / 60040 / 171) | 59226 | 863 | 4071 | 7602 (11%) |
| acct-1 | 288 / 425 / 775 (1 / 279 / 7) | 225 / 329 / 536 (0 / 224 / 0) | 133 | 67 | 61 | 63 (22%) |
| slot-1 | 404 / 591 / 892 (2 / 394 / 7) | 309 / 480 / 685 (0 / 308 / 1) | 249 | 52 | 36 | 96 (24%) |
| acct-64 | 5461 / 6025 / 6639 (28 / 5216 / 193) | 4256 / 4948 / 5218 (1 / 4249 / 7) | 4018 | 335 | -147 | 1205 (22%) |

"op 4 − walk" compares two separate walks of the same request, so for the
64-account case it lands inside the noise (-147). The decode is a full
validating walk plus a pass over every node, with no allocation: 0–7 µs
for small proofs and 171 µs for a block-sized one. JSON parsing of the
same answers takes 7 µs and 2.2 ms.

### Pin depth (sets `proof_max_depth`)

The same requests pinned `d` blocks below the frontier: in-process walks,
the first 8 block-sized and 8 64-account requests. Times are in µs.

| depth | block-sized walk first / p50 / max | 64-account walk first / p50 / max |
|---:|---|---|
| 0 | 48677 / 57981 / 77946 | 5240 / 5823 / 7516 |
| 1 | 825366 / 163730 / 825366 | 103039 / 103678 / 107912 |
| 4 | 257597 / 192072 / 257597 | 149632 / 150055 / 152012 |
| 16 | 540547 / 538920 / 542576 | 485062 / 517535 / 1205288 |
| 32 | 4166156 / 809127 / 4166156 | 1059742 / 785237 / 1059742 |

Any pin below the frontier pays for reth's revert overlay, about 0.1 s
for one block and roughly 25 ms per further block. A 64-account proof
costs 6 ms at the frontier and 0.79 s 32 blocks back. The server default
`proof_max_depth` is therefore **8**, about a quarter second
(interpolated between the measured 4 and 16), instead of the 32 first
written into D10. This is the read-only, persisted-state path; inside the
node, pins in its in-memory canonical blocks are cheaper at one or two
blocks of depth and reach about 0.3 s at 8 (see "In the node" below).
Either way, clients should prove at the head.

### Verification against the node (`lookup-verify`)

The node answers `eth_getProof` only at its head (reth's default proof
window is 0). So `lookup-verify` waits until `lookup-ro`'s frontier
reaches the head, sends one op-4 request and the matching `eth_getProof`
calls, and retries the round if a new block lands in between. Every node
of every `accountProof` and `storageProof` must appear in the op-4 answer,
every `storageHash` must equal the op-4 storage root, and keccak(op-4
root) must equal the header's `stateRoot`:

```
done at block 26404803: 5598 accounts (1742 with code, 15 absent, 0 item errors), 1184 code hashes, 2824 slots (2435 non-zero); keys checked 8422; unknown pin -> UnknownBlock: true; proofs at head 26404804: 120 accounts, 8 slots, 1062 account-proof nodes and 46 storage-proof nodes found in op 4, root ok: true; mismatches 0
done at block 26404805: 1884 accounts (568 with code, 12 absent, 0 item errors), 434 code hashes, 2795 slots (2417 non-zero); keys checked 4679; unknown pin -> UnknownBlock: true; proofs at head 26404805: 175 accounts, 120 slots, 1549 account-proof nodes and 517 storage-proof nodes found in op 4, root ok: true; mismatches 0
```

The second run spreads the slot sample across contracts (at most 2
each). The first took its slots from the head of a sorted list, which
landed on only a few contracts.

## In the node (2026-11-13)

The node now runs silt-node (yevm-node `b73b0f6`, reth `46731e9`, 4 proof
workers), so op 4 and `silt_multiproof` can be measured where clients use
them. `crates/silt-cache/examples/node-proof-bench.rs` is a reth-free
client; it opens nothing next to the node. It takes its target sets from
the node's own feed: the newest 20 single-block diffs (26408698–26408717),
each giving every account and slot that block changed (`block`), plus
disjoint slices of their keys for `acct-1`, `slot-1` (an account and one
of its changed slots) and `acct-64`. Every request is proved at the head,
in this order:

1. op 4 at `LATEST`: first touch;
2. `silt_multiproof` over HTTP, pinned to the block op 4 answered at:
   request encode, round trip, and a typed parse (hex to path → bytes
   maps);
3. op 4 again at that block: warm, then a zero-copy pass over every node.

Checks (any failure exits 1): keccak(op-4 root node) equals that block's
`stateRoot`, and both op-4 answers and the JSON-RPC answer have the same
account nodes, storage roots and storage nodes. **0 of 440 answers
differ; 0 root failures.** Times are in µs.

| workload | reqs | keys/req | nodes/req | op 4 KiB | JSON KiB | op 4 first touch p50/p90/max (server p50) | op 4 warm p50/p90/max (server / transport / decode p50) | JSON-RPC p50/p90/max (enc / rtt / parse p50) | saved vs JSON-RPC p50 |
|---|---:|---:|---:|---:|---:|---|---|---|---|
| block | 20 | 1387 | 7449 | 2556.8 | 5107.0 | 947404 / 1709098 / 16040528 (946283) | 57202 / 74911 / 140810 (56158 / 1042 / 154) | 89246 / 115427 / 1577431 (2636 / 72059 / 13777) | 32045 (36%) |
| acct-1 | 200 | 1 | 10 | 4.0 | 8.0 | 388 / 670 / 2190 (287) | 363 / 642 / 1604 (275 / 85 / 0) | 550 / 900 / 3527 (8 / 489 / 53) | 187 (34%) |
| slot-1 | 200 | 2 | 14 | 5.9 | 11.8 | 370 / 572 / 2118 (289) | 395 / 704 / 1575 (310 / 84 / 0) | 551 / 1115 / 2487 (7 / 478 / 69) | 156 (28%) |
| acct-64 | 20 | 64 | 513 | 192.2 | 384.6 | 4711 / 5015 / 6557 (4471) | 4496 / 5108 / 5505 (4196 / 347 / 6) | 6639 / 7548 / 9100 (74 / 5430 / 989) | 2142 (32%) |

Pin depth, measured in the node (op 4, server time): the first 8
block-sized and 8 64-account requests, pinned `d` blocks below the head.

| depth | block-sized: first / p50 / max server µs | 64-account: first / p50 / max server µs |
|---:|---|---|
| 0 | 73460 / 57129 / 76970 | 4502 / 4639 / 5425 |
| 1 | 733725 / 93958 / 733725 | 53264 / 56975 / 59429 |
| 2 | 136811 / 119373 / 138056 | 79492 / 80776 / 81656 |
| 4 | 204764 / 189988 / 204764 | 145343 / 259488 / 676766 |
| 8 | 339360 / 295217 / 339360 | 237631 / 238280 / 242053 |

- **Warm proofs cost what the harness measured.** A block-sized proof
  takes 57 ms warm (server 56 ms), the same as the harness's walk; op 4
  adds about 1 ms of transport and 0.15 ms of decode.
- **Small proofs cost about twice as much server time.** One account:
  275 µs in the node against 133 µs through the harness. As with lookups
  (`docs/lookup-bench.md`), the server shares the cores with block import,
  and a tip read resolves reth's in-memory blocks first.
- **First touch of a block-sized set is slow and has a long tail.** p50
  0.95 s (harness: 0.76 s), and one of 20 took 16 s: proving a just-changed
  set walks storage tries the node has not read since, while it imports
  and persists blocks. That exceeds the client's default `proof_timeout`
  (5 s); the bench raises it to 30 s. Request block-sized proofs warm, or
  in smaller pieces, or raise the timeout.
- **Op 4 saves 28–36% over JSON-RPC at p50** in the node (11–24% through
  the harness): the typed parse of a block-sized JSON answer alone takes
  14 ms, against 0.15 ms to walk the op-4 frame.
- **Shallow pins are cheaper in the node.** One block below the head: 94 ms
  block-sized and 57 ms for 64 accounts, against 164 ms and 104 ms through
  the harness; each further block adds roughly 25–30 ms, about 0.3 s at
  depth 8. The default `proof_max_depth` of 8 stands.

## Before op 4 (2026-11-12): JSON-RPC vs the walk

### Method

These are the results of the first version of the bench. Its `bin`
column was a transport floor estimated before op 4 existed; the current
bench measures op 4 itself (above).

`crates/silt-node/examples/proof-bench.rs` opens the live node's datadir
read-only (reth `46731e9`, mainnet, the same harness as `lookup-ro`).
Before each request it re-pins to the current state frontier, untimed: a
client always proves at the tip. Per request it measures:

- **walk**: opening a state provider plus `StateProofProvider::multiproof`,
  in-process. Every transport pays this.
- **build + json**: the RPC's own `MultiproofResponse::from_multiproof`
  plus `serde_json` encoding.
- **rpc**: the real `silt_multiproof` module, served by jsonrpsee over
  HTTP on loopback with reth's default size limits. Measured as request
  encode, round trip, and a typed client-side parse (hex decoded to bytes).
- **bin**: a loopback TCP round trip carrying the request's and the
  proof's packed binary sizes: 32 B per key; path plus RLP per node. This
  is the transport floor a binary op would add on top of the walk. It is
  not an implemented op.

Every proof is checked: keccak(root node) must equal the pinned header's
state root, both in-process and over RPC.

Targets are pre-hashed and come from the changesets of the 40 most recent
blocks:

- **block**: every account and slot one block changed. This is the key set
  a state-root computation for a block of that shape needs.
- **acct-1**, **slot-1**, **acct-64**: one account, one account plus one
  slot, and 64 accounts. These are the shapes of the fetches a sparse trie
  kept warm by the feed would make.

Server and client run in one process on separate runtimes. The RPC server
is standalone jsonrpsee rather than reth's full RPC stack, which adds
middleware and is not faster.

### Results

Frontier 26397670–26397674 (it moved 4 times during the run). Root checks:
0 failures. Times are in µs.

| workload | reqs | keys/req | nodes/req | packed KiB | JSON KiB | walk, first pass p50/p90/max | walk p50/p90/max | build+json p50 | rpc p50/p90/max (enc / rtt / parse p50) | rpc − walk p50 (share) | bin rtt p50 | walk+bin p50/p90/max |
|---|---:|---:|---:|---:|---:|---|---|---:|---|---|---:|---|
| block | 40 | 1316 | 7110 | 2450.1 | 4893.8 | 909800 / 1432486 / 4531607 | 56998 / 77189 / 111353 | 5917 | 69773 / 94976 / 135905 (171 / 67400 / 2283) | 11698 (17%) | 697 | 57755 / 78455 / 114815 |
| acct-1 | 300 | 1 | 10 | 4.0 | 7.9 | 130 / 217 / 335 | 324 / 440 / 902 | 22 | 434 / 694 / 987 (3 / 424 / 10) | 129 (30%) | 45 | 383 / 542 / 1018 |
| slot-1 | 300 | 2 | 15 | 6.1 | 12.3 | 238 / 290 / 593 | 230 / 275 / 340 | 10 | 476 / 584 / 787 (1 / 465 / 5) | 248 (52%) | 19 | 250 / 307 / 420 |
| acct-64 | 30 | 64 | 515 | 190.3 | 380.9 | 4503 / 4831 / 4934 | 4387 / 4784 / 5159 | 390 | 5580 / 6131 / 6343 (33 / 5289 / 232) | 1184 (21%) | 196 | 4573 / 4975 / 5321 |

The "first pass" walks each request once before the measured pass. Its
block-sized walks pay first touch plus the overlay that reth builds once
per new frontier. Sub-millisecond p50s on the small workloads are noisy:
the server's own walks run on other threads in between.

### Findings

- **The walk dominates large proofs.** A block-sized proof (about 1300
  keys, 7100 nodes, 2.4 MiB of RLP) walks in 57 ms warm and about 0.9 s on
  first touch. JSON-RPC adds 11.7 ms on top (17%): 4.9 MiB of hex, about
  6 ms server encode and 2.3 ms client parse. A binary op would land at
  about 58 ms. No transport fixes the cold walk.
- **JSON's share grows as requests shrink.** Fixed per-call costs make up
  30% of a single-account fetch (about 0.13 ms) and 52% of a single-slot
  fetch (about 0.25 ms). A binary op would bring those to walk plus about
  20–45 µs, roughly a 1.2–2x saving per small fetch.
- **The bigger lever is not walking at all.** A client that keeps its
  sparse trie warm from the diff feed (D1) only fetches nodes it hasn't
  revealed yet (D8's path-directed probes). Those are exactly the small
  requests where the binary op saves the most, and batching a block's
  reveals into one request amortizes the rest.
- **Pin at the tip.** A fixed pin falls behind a live node, and every walk
  then rebuilds a revert overlay that grows with the distance. An earlier
  version of this bench pinned once. About 600 blocks (2 hours) later,
  single walks took minutes, and MDBX's 5-minute read-transaction safety
  aborted one. Proof servers must serve tip or near-tip pins only, as the
  lookup protocol already does.

## Reproduce

Next to a live node, run harnesses only under a memory cap and a pressure
guard. The read-only harness reached 5.2 GB within a minute on
2026-11-13: database pages it reads compete with the node's. Uncapped, it
pushed the machine into the memory pressure that let systemd-oomd kill
the node. The guard used for the runs above:

```sh
# guard.sh NAME CMD...: systemd-run --user --scope -p MemoryMax=6G -p MemorySwapMax=0 CMD,
# polled every second; SIGTERM if /proc/pressure/memory "some avg10" >= 10,
# MemAvailable < 3 GiB, or the node's RPC (8545) disappears.
CARGO_BUILD_JOBS=4 cargo build --release -p silt-node --example proof-bench --example lookup-ro
CARGO_BUILD_JOBS=4 cargo build --release -p silt-cache --example lookup-verify
guard.sh proofbench ./target/release/examples/proof-bench \
    --datadir /home/work/Temp/reth-node/datadir --blocks 20 --small 200 --depths 0,1,4,16,32
guard.sh lookupro ./target/release/examples/lookup-ro \
    --datadir /home/work/Temp/reth-node/datadir --bind 127.0.0.1:8587 &
./target/release/examples/lookup-verify --accounts 2000      # includes --proofs 120
kill -TERM $(pgrep -x lookup-ro)                              # removes its scratch dir
```

In the node, nothing opens the datadir; the client needs only the node's
feed, lookup listener and RPC:

```sh
CARGO_BUILD_JOBS=4 cargo build --release -p silt-cache --example node-proof-bench
./target/release/examples/node-proof-bench --blocks 20 --small 200 --depths 0,1,2,4,8
```

proof-bench binds 127.0.0.1:8588 (JSON-RPC) and 8589 (lookup), and exits
and cleans up on its own. A proof-bench killed by the guard leaves
`/tmp/silt-proof-bench-<pid>`: a symlink into the datadir's `rocksdb/`
plus the secondary's temp dir. Remove the symlink with `rm` (never
`rm -r` through it), then the directory.
