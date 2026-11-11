# silt — ExEx state-diff feed + chain-state cache

A four-crate workspace (reth enters nobody's tree but silt-node's and its
host binary's):

| Crate | What | Depends on |
|---|---|---|
| `silt-wire` | Diff/head/receipt wire types, framing, handshake | serde + postcard only |
| `silt-node` | Library any reth node installs: the feed ExEx + TCP publisher, the TCP lookup server (D9), `silt_multiproof` (D8) | reth (pinned), wire |
| `silt-exex` | Thin host binary: stock reth (`EthereumNode`, stock CLI) with silt-node installed | silt-node, reth (pinned) |
| `silt-cache` | Consumer: subscribes, maintains a coherent chain-state cache; misses go to the lookup fast path (D9), falling back to `eth_*` RPC | wire, HTTP JSON-RPC client; **no reth** |

The cache exposes its own Chain-*shaped* async API (whole-account read,
storage-slot read, head/block by number — the same semantic surface yevm's
`Chain` trait wants); an optional feature-gated adapter maps it 1:1 onto
yevm's trait. The core crate has **no yevm dependency**.

## 0. Fixed design decisions

- **D1 — Transport is a custom RPC over TCP** (not a Unix socket: consumer
  may live on another machine). Length-prefixed (`u32` LE) postcard frames,
  versioned handshake, exact-version match in v0. Push stream + one request
  (`Subscribe { from }`). No TLS/auth in v0: bind to a private interface /
  WireGuard; documented as an ops constraint, revisit if that ever changes.
- **D2 — Standard JSON-RPC is always the fallback** (`eth_getStorageAt`,
  `eth_getBalance`, `eth_getCode`, `eth_getTransactionCount`,
  `eth_getBlockByNumber`), pinned to a block hash. The feed is a coherence
  and latency upgrade, never the only source. Cold start, gaps, and
  never-touched keys all resolve via RPC.
- **D3 — Reorg = drop-to-cold.** On any chain break (reorg frame, parent
  mismatch, gap, decode error): discard carried state, resubscribe at tip,
  refill lazily. A provenance bit (diff-applied vs lazily-fetched) is
  reserved on every cache entry **now** so a future layered/versioned
  design stays cheap; no other reorg machinery in v0.
- **D4 — Wire types are dependency-free primitives** (`[u8;20]`, `[u8;32]`,
  `u64`, `Vec<u8>`). Each side adapts to its native types (alloy in the
  wrapper, yevm/whatever in consumers). CI enforces the dependency
  boundaries (tree check: `reth*` only under `silt-node`/`silt-exex`; `yevm*`
  only behind the cache's optional adapter feature).
- **D5 — Ack discipline:** the ExEx emits `FinishedHeight` only after the
  frame is in its local bounded spool (ring, default 128 blocks), never
  gated on subscriber liveness — reth retains un-acked notifications in
  its WAL, so a slow consumer must never become a node-disk incident.
  Subscribers behind the spool get `GapTooOld` and cold-rebuild.
- **D6 — Multi-block `ChainCommitted` segments fold** into one cumulative
  diff (`span_start..number`, last-write-wins); per-block `HeadLite` and
  receipts ride inside the frame so historical heads/receipts survive the
  fold.
- **D7 — The wrapper is a thin pin, not a fork:** stock reth node builder
  + `install_exex`, same CLI/datadir as stock reth; target ≤200 LoC for
  main + glue. Upgrading reth = bump the pin, fix compile, rerun gates.
  All silt logic lives in the **`silt-node` library**, installed with two
  builder calls (`install_exex("silt", … run_exex(ctx, cfg))` +
  `extend_rpc_modules(… rpc_module(provider))`), so any reth node — the
  stock-executor `silt-exex`, or a node with its own executor — carries
  the same capabilities. The reth pin is one workspace dependency; a host
  must build against that same reth (one binary can't mix reth versions),
  so the pin follows yevm-node's reth rev (currently `46731e9`) and only
  moves forward with it.
- **D8 — Path-directed multiproofs ride reth's RPC server, not the feed.**
  Resolving a partial trie's stubs (mpt-lab PTRIE.md §8) needs proofs for
  **pre-hashed** trie keys so the caller can steer the walk to a chosen
  nibble path; `eth_getProof` keccaks raw keys server-side and so cannot
  express that read. The wrapper registers `silt_multiproof(targets:
  [hashedAddress, [hashedSlotProbe]], block?: hash)` via the node
  builder's `extend_rpc_modules`, backed by in-process
  `StateProofProvider::multiproof`, returning path-keyed RLP nodes in a
  reth-free JSON shape (consumers hash-verify every node anyway). It is
  served on whatever transports reth's own `--http/--ws/--ipc` flags
  enable — no flags, no endpoint — and the silt feed (D1) stays
  push-only. State is pinned by block hash (`null` = latest); historical
  blocks resolve through reth's revert overlay and get costlier with
  distance from tip, same constraint as `eth_getProof`.
- **D9 — Cache misses take a binary lookup fast path, on its own
  socket.** A builder reads state only at the tip; the feed keeps its
  cache current, so only misses leave the process, and each one used to
  pay HTTP + JSON + hex per key. The ExEx serves a request/response
  lookup protocol on a **separate listener** (`lookup_bind`, default
  `127.0.0.1:8586`; empty string disables it; never defaults to
  `0.0.0.0`). It is a fast path only: JSON-RPC stays the fallback for
  every failure mode (D2). Wire rules:
  - **Handshake** = the feed's: `Frame::Hello` both ways (postcard,
    exact `VERSION` match, server first), `Refused` + close on mismatch;
    then the connection switches to lookup frames. The lookup layout is
    covered by the same `VERSION`; it stays `0` (the feed wire is
    unchanged) and any change to either protocol bumps it.
  - **Lookup frames**: the same `u32` LE length prefix, then a fixed
    little-endian layout (`silt_wire::lookup`, hand-written, reth-free):
    no postcard/serde/varints on the hot path, and request decoding is
    zero-copy and allocation-free (fixed offsets, borrowed slices).
    Integers are LE; 32-byte words are opaque arrays exactly as in
    `BlockDiff` (U256 values big-endian). Refinements over the first
    sketch: the response opens with the request's own 8-byte prefix
    (`op, flags, count, id`), so it is self-describing (decoding Account
    results needs `INLINE_CODE`) and both directions route on the same
    bytes; `status` moves to byte 8, followed by 3 reserved zero bytes so
    `server_us`/`read_number` sit naturally aligned (56 B vs 52 B); and
    an account's existence is folded into `item_status` (`Ok`/`Absent`)
    instead of a separate `exists` byte.

    ```
    request  (40 B header): u8 op | u8 flags | u16 count | u32 id | [32] pin_hash | count × item
      items: Account [20] addr | Storage [20] addr,[32] slot | Code [32] code_hash
    response (56 B header): u8 op | u8 flags | u16 count | u32 id      (the request's own prefix)
                            | u8 status | [3] reserved=0 | u32 server_us
                            | u64 read_number | [32] read_hash | count × result
      results (u8 item_status first, then fixed-size payload):
        Account: status, u64 nonce, [32] balance, [32] code_hash   (73 B)
                 [+ u32 code_len, code   iff flags.INLINE_CODE]
        Storage: status, [32] value                                (33 B)
        Code:    status, u32 len, bytes
    ```
  - **Ops** `Account = 1`, `Storage = 2`, `Code = 3` (0 is never valid,
    so a zeroed frame is a `BadRequest`). **Flags**: bit 0 =
    `INLINE_CODE` (Account only); any other bit, or `INLINE_CODE` on
    another op, is a `BadRequest` — unknown flags are never ignored.
    `count = 0` is a valid ping: it resolves the pin and returns the
    `(read_number, read_hash)` it would read at.
  - **No code / missing account**: the code hash on the wire is
    **always `KECCAK_EMPTY`** for "no code" (reth's `bytecode_hash: None`
    is normalized), never zero — the same convention as the cache's store
    and the account trie. A missing account answers item status `Absent`
    with nonce 0, balance 0, `KECCAK_EMPTY`, `code_len` 0, which is what
    `eth_get{Balance,TransactionCount,Code}` return; `Ok` means the
    account exists, so callers can tell empty from nonexistent (RPC
    can't). Storage: a missing slot reads as zero with status `Ok`
    (`Absent` is never sent for storage). Code: `KECCAK_EMPTY` answers
    `Ok` + empty without a read; an unknown hash answers `Absent`. Code is
    immutable per hash, so clients may cache `Code` results forever.
  - **Statuses.** Frame: `Ok 0`, `UnknownBlock 1`, `BadRequest 2`,
    `Internal 3`, `Busy 4`. Any non-`Ok` frame carries `count = 0`, zero
    `read_number`/`read_hash`, and no results. Item: `Ok 0`, `Absent 1`,
    `Error 2`. `Ok` and `Absent` both mean the payload is valid; on
    `Error` (one key's read failed, the others still answer) the payload
    is zeroed and must be ignored. `Busy` means "not attempted": retry
    later or use the fallback.
  - **Limits** (wire constants, both sides): `MAX_ITEMS = 1024` per frame
    (Account, Storage); `MAX_CODE_ITEMS = 64` for `Code` and
    `Account + INLINE_CODE`; `MAX_REQUEST_BYTES = 40 + 1024 × 52 = 53 288`;
    `MAX_RESPONSE_BYTES = 16 MiB` (64 × a 256 KiB contract still fits,
    10× headroom over EIP-170; a response that would exceed it answers
    `Internal`); `MAX_INFLIGHT = 256` requests per connection. A length
    prefix below 8 or above `MAX_REQUEST_BYTES` closes the connection
    (the stream can't be trusted); a frame with a readable `op`+`id` that
    fails any other check answers `BadRequest` with the id echoed.
  - **Pinning.** Every request carries a block hash; all zeros = the
    source's "latest" (the node: its canonical head, including blocks
    executed but not yet persisted; the read-only harness: the state
    frontier). A non-zero pin must be canonical (`block_number(hash)` and
    `block_hash(number) == hash`); unknown, non-canonical (incl. the
    pending block), pruned (`StateAtBlockPruned`,
    `InsufficientChangesets`) or not-yet-readable hashes answer
    `UnknownBlock` — never stale data. Every `Ok` response names the
    `(read_number, read_hash)` it read at; all items in one frame come
    from one state view at that block. A builder pins to its cache head;
    when reth is a block ahead, the read goes through reth's in-memory
    overlay one block back — expected. A pin that loses canonicality
    mid-request may still return that block's post-state; the consumer's
    D3 drop-to-cold discards it.
  - **In flight / backpressure.** Requests carry a client-chosen `u32`
    id; responses may come back in any order. A request is in flight from
    decode until its response is *built*; at most `MAX_INFLIGHT` per
    connection. One beyond that is answered `Busy` at once, without a
    read, and so is any request arriving while the server-wide work queue
    (4096 jobs) is full. Because the slot frees before the answer can
    reach the client, a client that keeps at most `MAX_INFLIGHT`
    outstanding never sees `Busy` from the cap; `Busy` means misuse or
    overload. Frames not yet on the socket are bounded separately
    (`MAX_INFLIGHT + 64` per connection, answers and `Busy`/`BadRequest`
    alike): when a client stops reading, the server stops reading its
    requests (TCP pushes back) and the write deadline then drops the
    connection.
  - **`server_us`** (u32, saturating) = server time from request decode
    to response encode (queue wait + provider open + reads + encode), so
    a client separates transport (round trip − `server_us`) from database
    time.
  - **Threading.** One accept task; per connection a reader task (frame
    read with a deadline, header validation, `Busy` decisions, dispatch)
    and a flusher task that only handles backpressure. Reads run on a
    **dedicated pool** of `lookup_workers` OS threads (default 16) fed by
    a bounded queue — never on async workers, never on the ExEx
    notification loop, and not on tokio's blocking pool, which reth's own
    RPC shares (D5). The worker that built an answer writes it to the
    socket itself with a non-blocking write — no hop to a writer task —
    and concurrent answers are *combined*: a sender that finds the
    socket busy leaves its frame in an inbox that the active writer
    drains into one vectored write (`silt_wire::lookup::FrameWriter`,
    used by the client too). Only bytes the socket can't take now wait
    for the flusher, which waits for writability under the write
    deadline. (Measured: removing the hop and the per-answer lock convoy
    took `server_us` at 256 in flight from ~2.2 ms to ~25 µs;
    `docs/lookup-bench.md`.)
    A job opens **one state provider per request frame** (one MDBX read
    transaction + reth's overlay), reads every item in order, encodes the
    response and drops the provider: no provider or read transaction ever
    outlives a request, because a long-lived MDBX reader stops reth from
    reclaiming pages. The frame is the unit of consistency (one snapshot,
    one block) and of parallelism (frames run concurrently, items within
    a frame sequentially) — a client wanting I/O parallelism for cold keys
    keeps several smaller frames in flight rather than one huge frame.
    Deadlines: handshake 10 s, rest-of-frame 10 s, a full socket must
    drain within 10 s, idle 120 s between frames (clients ping every
    30 s). One INFO line at
    startup with the bind address; nothing per request above DEBUG.
  - **Why the feed stays push-only (D8).** Different shapes: bulk
    ordered push vs latency-critical small request/response. A 400 KB
    diff frame on a shared socket would put head-of-line delay in front
    of misses; the feed drops slow subscribers while lookups answer
    `Busy`; and a separate port can carry a separate bind/firewall policy
    (lookups on loopback only, the feed on a private interface).
  - **Reads go through reth's providers, never MDBX directly**: the
    node source uses `ctx.provider()` (`StateProviderFactory`:
    `chain_info` → `state_by_block_hash`, so canonical in-memory blocks
    are visible); the read-only harness uses `open_read_only` + the
    overlay pinned at the state frontier. Both implement one small
    `StateSource` trait in `silt-node/src/lookup.rs`, so the server code
    is shared and testable without reth.

## 1. Architecture

```
            reth (library, pinned)                     other machine OK
 ┌───────────────────────────────────────┐           ┌──────────────────────────┐
 │ silt-exex / any node + silt-node │           │ consumer                 │
 │  node builder ── install_exex ──┐     │           │ ┌──────────────────────┐ │
 │                                 ▼     │    TCP    │ │ silt-cache      │ │
 │  ChainCommitted/Reorged → BlockDiff ──┼─ frames ──┼▶│ client → validate →  │ │
 │        │ (fold span, receipts, heads) │  (push +  │ │ apply → state maps   │ │
 │        ▼                              │ Subscribe)│ │   │ miss              │ │
 │  spool(128) ──ack──▶ FinishedHeight   │           │ │   ▼                  │ │
 │  lookup server (D9) ◀─────────────────┼─ req/resp ┼─┤ lookup (pinned) ─┐   │ │
 └───────────────────────────────────────┘  :8586    │ │ eth_* RPC fallback ◀┘│ │
                                                     │ └──────────┬───────────┘ │
                                                     │   Chain-shaped API       │
                                                     │   (+ optional yevm impl) │
                                                     └──────────────────────────┘
```

## 2. Wire protocol (v0)

Frames (postcard-encoded enum, `u32` LE length prefix):

- `Hello { magic, version, chain_id, genesis_hash, tip }` — both directions
  first; any mismatch → close with `Refused { reason }`.
- `Subscribe { from: Option<u64> }` — client→server; `None` = from tip.
- `Diff(BlockDiff)` — server→client.
- `Reorg { new_tip_number, new_tip_hash }` — server→client marker; the new
  side's `Diff` (whose `parent_hash` points below the fork) follows.
- `Heartbeat { height, spool_depth }` — server→client, ~10 s.
- `GapTooOld { oldest_available }` — server→client answer to an
  unserviceable `Subscribe`.

```rust
pub struct HeadLite {            // enough to build a sim env for N+1
    pub number: u64, pub hash: [u8;32], pub parent_hash: [u8;32],
    pub timestamp: u64, pub basefee: [u8;32], pub gas_limit: u64,
    pub coinbase: [u8;20], pub prevrandao: [u8;32],
}
pub struct BlockDiff {
    pub span_start: u64,                      // == number for single blocks
    pub number: u64,                          // tip of span
    pub hash: [u8;32], pub parent_hash: [u8;32], // parent of span_start
    pub heads: Vec<HeadLite>,                 // one per block in span
    pub accounts: Vec<([u8;20], Option<AccountChange>)>, // None = destroyed
    pub storage: Vec<([u8;20], [u8;32], [u8;32])>, // presence = written (0 incl.)
    pub code: Vec<([u8;32], Vec<u8>)>,        // new bytecode by code hash
    pub receipts: Vec<(u64, [u8;32], Vec<ReceiptLite>)>, // per block in span
}
pub struct AccountChange { pub balance: [u8;32], pub nonce: u64, pub code_hash: [u8;32] }
```

### 2.1 Lookup protocol (D9, `silt_wire::lookup`)

Separate listener (`lookup_bind`, default `127.0.0.1:8586`). Connection
setup is the feed's `Hello` exchange (server first, postcard, exact
version); after it, both directions carry only lookup frames: `u32` LE
length + a fixed little-endian body. Byte offsets:

| Request field | Offset | Size | Notes |
|---|---|---|---|
| `op` | 0 | 1 | 1 Account, 2 Storage, 3 Code |
| `flags` | 1 | 1 | bit 0 `INLINE_CODE` (Account only); other bits must be 0 |
| `count` | 2 | 2 | items; 0 = ping (resolve the pin only) |
| `id` | 4 | 4 | client-chosen, echoed |
| `pin_hash` | 8 | 32 | block hash; all zeros = latest |
| items | 40 | count × 20 / 52 / 32 | `addr` / `addr,slot` / `code_hash` |

| Response field | Offset | Size | Notes |
|---|---|---|---|
| `op` | 0 | 1 | echoed |
| `flags` | 1 | 1 | echoed (results are self-describing) |
| `count` | 2 | 2 | = request count if Ok, else 0 |
| `id` | 4 | 4 | echoed |
| `status` | 8 | 1 | 0 Ok, 1 UnknownBlock, 2 BadRequest, 3 Internal, 4 Busy |
| reserved | 9 | 3 | zero |
| `server_us` | 12 | 4 | decode → encode, server side |
| `read_number` | 16 | 8 | block actually read (0 unless Ok) |
| `read_hash` | 24 | 32 | its hash (zeros unless Ok) |
| results | 56 | … | in request order |

Results start with `item_status` (0 Ok, 1 Absent, 2 Error):
Account `status, nonce u64, balance [32], code_hash [32]` (73 B), plus
`code_len u32, code` when `INLINE_CODE`; Storage `status, value [32]`
(33 B); Code `status, len u32, bytes`. "No code" is always `KECCAK_EMPTY`;
a missing account is `Absent` with zeroed fields; a missing slot is `Ok`
with zero. Limits: 1024 items (64 for code-carrying ops), 53 288-byte
requests, 16 MiB responses, 256 requests in flight per connection.

## 3. Milestones

### M0 — Scaffolding and wire

- **T0.1** `git init`; workspace; `scripts/ci.sh` (fmt, `clippy -D
  warnings`, test, dependency-tree boundary check per D4).
- **T0.2** `silt-wire`: types above, postcard codec, framing
  read/write helpers (tokio `AsyncRead/Write`), handshake constants.
- **T0.3** Fixture + property tests: round-trip 10k random frames;
  handcrafted fixtures for the three sharp corners — destroyed account,
  zero-value storage write, folded 2-block span.

**Gates:** G0.1 CI green. G0.2 round-trip property test passes. G0.3 tree
check enforced and failing-by-construction test documented (put a `reth`
dep in `wire` on a branch once, watch CI fail, record it).

### M1 — ExEx wrapper (`silt-exex`)

- **T1.0 VERIFY (blocking):** pin reth to the exact version the deployed
  node runs (v2.5.1 at time of writing). Against that tag confirm: node
  builder + `install_exex` signature; `ExExNotification` variants and
  whether `ChainReorged` carries the new chain; `ExecutionOutcome` /
  `BundleState` field shapes (present-side account info, storage slots,
  new bytecode, receipts); `FinishedHeight` + WAL retention semantics.
  Record findings in `docs/reth-notes.md` (yevm-notes style: every claim
  with a file/line reference into the pinned tag).
- **T1.1** Wrapper main: stock node builder + `.install_exex("silt",
  …)`; identical CLI/datadir semantics to stock reth.
- **T1.2** Mapping `ChainCommitted → BlockDiff`: fold span last-write-wins;
  accounts incl. deletions; storage incl. zero-writes; code table;
  `HeadLite` + receipts per block. `ChainReorged`/`ChainReverted` →
  `Reorg` frame (+ the new segment's `Diff` for Reorged).
- **T1.3** Spool ring (default 128) + D5 ack discipline; dedupe re-delivered
  notifications after wrapper restart by `(number, hash)` (reth replays
  un-acked WAL entries). Heartbeat every 10 s.
- **T1.4** TCP server: handshake, multi-subscriber fan-out (frames are
  `Arc`'d), `Subscribe{from}` replayed from spool, `GapTooOld` past it;
  per-subscriber bounded send queue — overflow disconnects that subscriber
  (it reconnects and catches up from the spool). Graceful shutdown.
- **T1.5** Config: one `silt.toml` section for the wrapper (bind
  address, spool depth, heartbeat period) read next to reth's own flags;
  env-free, secret-free.
- **T1.6** `silt_multiproof` endpoint (D8): jsonrpsee namespace `silt`
  merged through `extend_rpc_modules`; the handler clones the node
  provider and runs the MDBX-walking `multiproof` under
  `spawn_blocking`, mapping the result to nibble-path-hex → RLP-hex maps.

**Gates:** G1.1 mapping unit tests green on synthetic
notifications (incl. selfdestruct + span fixtures). G1.2 end-to-end on a
`--dev` chain: mined txs produce frames; bytes/block and commit→frame
latency recorded. G1.3 WAL safety: run N blocks with no subscriber —
`FinishedHeight` advances, node disk/WAL bounded, spool capped at 128.
G1.4 wrapper kill/restart mid-stream: no duplicate or missing heights
observed by a subscriber (dedupe works).

### M2 — Consumer cache (`silt-cache`)

- **T2.1** Client: connect/handshake, auto-reconnect with backoff,
  `Subscribe{from: last+1}`; liveness = no frame for 30 s **and** RPC tip
  advanced → force reconnect. Validated stream: monotonic heights,
  `parent_hash == carried.hash`, else chain-break signal.
- **T2.2** Store: concurrent maps — whole accounts (balance/nonce/code),
  storage cells, code by hash, recent `HeadLite` ring — plus a per-owner
  slot index (destroyed accounts drop account **and** held slots) and the
  D3 provenance bit per entry. Apply = upsert before the new head is
  published to the consumer's head stream.
- **T2.3** Lazy fallback: JSON-RPC reads pinned at the current block hash;
  whole-account resolution on any account-ish miss (the yevm-notes
  finding: balance/nonce/code misses all want the full account — cache
  whole accounts); single-flight dedupe of concurrent misses.
- **T2.4** Public API, Chain-shaped: `async fn account(&Acc20) -> Account`,
  `async fn storage(&Acc20, &B256) -> B256`, `async fn head(u64) ->
  HeadLite`, `async fn block(u64) -> BlockLite (RPC passthrough)`, plus
  `fn heads(&self) -> impl Stream<Item = HeadEvent>` (NewHead / ChainBreak).
  **Optional `yevm` cargo feature** ships the pair, because in yevm `Chain`
  and `State` always walk together: `impl yevm::Chain for Cache`
  (1:1 over the four reads) **and** an overlay helper `fn overlay(&self) ->
  yevm::Cache` — a fresh per-sim `State` whose misses read through the
  cache, so `Executor::run(tx, head, &mut cache.overlay(), &cache)`
  works out of the box. Core crate
  still compiles without yevm.
- **T2.5** Drop-to-cold: on Reorg frame / parent mismatch / `GapTooOld` /
  decode error → clear store, resubscribe at tip, emit `ChainBreak` so the
  consumer can cancel in-flight work. Feed-down semantics: no head events
  are emitted while disconnected; reads stay valid for the last coherent
  block (RPC pinned to its hash). Eviction: background sweep of entries
  unread for K blocks (default 64) — the RSS bound.
- **T2.6** Observability: one INFO line per applied block (height, span,
  accounts/slots applied, apply µs), ERROR on every chain break with cause.
- **T2.7** Config: builder-style `CacheConfig { feed_addr, rpc_url,
  evict_after_blocks, liveness_timeout }` — a plain struct, no file format
  imposed on embedders.

**Gates:** G2.1 unit fixtures: destroyed account, span fold, zero-write,
reorg-drop, eviction. G2.2 integration vs `--dev` node: ≥200 mined blocks,
after each block **every** diff-applied key equals the RPC answer at that
block — 0 mismatches. G2.3 compatibility smoke (behind the `yevm`
feature): replay one real tx through `Executor::run(tx, head, &mut
cache.overlay(), &cache)` — the paired usage; status/gas match the
receipt. G2.4 kill-tests: kill the wrapper
mid-stream → consumer reconnects or cold-rebuilds; no panic, no stale
read served for a new head.

### M3 — Live-node equivalence + soak

- **T3.1** Verifier (example bin in `silt-cache`): subscribe to the
  live node; per block, compare all diff-applied keys against RPC at that
  block (rate-limited sweep); record mismatches, commit→frame→applied
  latency, frame bytes, RSS.
- **T3.2** 24 h window on the live node.
- **T3.3** `docs/runbook.md`: build, run the wrapper **in place of** stock
  reth (same datadir/flags), firewall/bind guidance (D1), rollback = stop
  wrapper / start stock reth, upgrade procedure (bump pin → gates).

**Gates:** G3.1 24 h, **0 mismatches**, report with p50/p99 latencies and
bytes/block committed to `docs/`. G3.2 mid-window kill/restart of either
side → degrades and recovers cleanly (no missed-head accounting). G3.3
runbook reviewed by operator before first run against the production node.

### M4 — Lookup fast path (D9)

- **T4.1** `silt_wire::lookup`: ops/flags/status constants and limits;
  request builder + zero-copy, allocation-free request decoder; response
  builder (patches `count`/`server_us`/length at finish) + zero-copy
  response decoder; length-prefixed read helpers with explicit caps.
  Unit tests per layout corner plus property tests (T0.3 style): 10k
  random request/response round-trips; every truncation of a valid frame
  and every oversized count/length rejected.
- **T4.2 VERIFY** at the pinned reth tag (`docs/reth-notes.md`):
  `ExExContext::provider`, `StateProviderFactory::{latest,
  state_by_block_hash}`, `BlockNumReader::{chain_info, block_number}`,
  `BlockHashReader::block_hash`, `AccountReader::basic_account`,
  `StateProvider::storage`, `BytecodeReader::bytecode_by_hash`; how
  unknown/non-canonical/pending/pruned hashes surface; the read-only
  open (`open_read_only`, `ReadOnlyConfig`, runtime sizing, state
  frontier, RocksDB secondary directory).
- **T4.3** `silt-node/src/lookup.rs` (was `silt-exex`, see T4.7): `StateSource`/`StateView` traits; the
  node source over `ctx.provider()`; the server (handshake, reader/writer
  tasks, dedicated worker pool, deadlines, `Busy`), spawned from `run()`
  next to the publisher. Config `lookup_bind` (+ `lookup_workers`).
  Tests drive the shared server with an in-memory source: pipelining and
  out-of-order answers, `Busy` at the in-flight cap, `UnknownBlock`,
  `BadRequest`, handshake refusal, deadlines.
- **T4.4** `silt-cache`: `LookupClient` (one connection, ≤256 in flight,
  supervisor reconnect with backoff, per-request timeout, 30 s ping) and
  `LookupFallback: Fallback` — `account_at` (inline code) and `storage_at`
  over lookup, everything else and every failure via the inner
  `HttpFallback` (D2); `UnknownBlock` errors like RPC would.
  `CacheConfig::with_lookup(addr)`. Mock-server tests: pipelining with
  out-of-order answers, reconnect, fallback on refusal/timeout/`Busy`/
  `Internal`/item `Error`.
- **T4.5** Read-only harness `lookup-ro` (silt-node example): opens a live
  datadir read-only (1-thread reth pools, monitored `ReadOnlyConfig`,
  RocksDB secondary kept out of the datadir), serves the same protocol
  with the same server code, "latest" = state frontier, a fresh provider
  per request.
- **T4.6** `lookup-bench` (sequential, 16/256 in flight, batches of
  16/256; p50/p99/p999 + throughput; transport vs `server_us`; first-touch
  vs warm) and `lookup-verify` (every key vs `eth_*` at the same hash),
  both reth-free silt-cache examples fed with real keys from recent blocks.
- **T4.7** Extract `silt-node`: config, diff mapping, feed publisher,
  lookup server, `silt_multiproof` and the ExEx loop (`run_exex`) move out
  of the wrapper into a library; `rpc_module(provider)` for
  `extend_rpc_modules`; the reth pin becomes one workspace dependency;
  `silt-exex` keeps only `main` (D7). No behaviour change.

**Gates:** G4.1 wire round-trip/truncation property tests green. G4.2
server tests green with the in-memory source; `FULL=1 scripts/ci.sh`
green. G4.3 client mock-server tests green (D2 fallback on every failure
mode). G4.4 `lookup-verify` against the live datadir via `lookup-ro`:
**0 mismatches**. G4.5 latency report committed to `docs/lookup-bench.md`
(single warm lookup p50 under 1 ms end to end).

**Status (2026-11-10):** T4.1–T4.6 done; G4.1–G4.5 met against the live
datadir through `lookup-ro` (verify: 0 mismatches over ~8.3k keys, twice;
single lookups warm p50 54–79 µs / p99 <170 µs, first touch with SSD reads
p50 0.30–0.77 ms); T4.7 done (2026-11-10, same tests, now in silt-node).
Still open: the same run in-node, once the node runs silt (silt-exex, or
a node that installs silt-node) — the harness reads at its state
frontier, not the node's tip.

## 4. Feature → milestone matrix

| # | Feature | Milestone |
|---|---|---|
| F1 | Wire types + codec + framing | M0 |
| F2 | Notification → diff mapping (fold, deletes, code, receipts, heads) | M1 |
| F3 | Spool + ack discipline (WAL safety) | M1 |
| F4 | TCP server, subscribe/catch-up, fan-out | M1 |
| F5 | Client, reconnect, chain validation | M2 |
| F6 | Store + apply path + provenance bit | M2 |
| F7 | RPC lazy fallback, whole-account, single-flight | M2 |
| F8 | Chain-shaped API + optional yevm adapter | M2 |
| F9 | Drop-to-cold + eviction | M2 |
| F10 | Observability (heartbeats, per-block line, break ERRORs) | M1/M2 |
| F11 | Equivalence verifier + soak | M3 |
| F12 | Path-directed multiproof RPC (`silt_multiproof`, D8) | M1 |
| F13 | Binary lookup protocol: layout, codec, limits (D9) | M4 |
| F14 | Lookup server in the ExEx (pool, pinning, backpressure) | M4 |
| F15 | Lookup client + `LookupFallback` (pipelined, D2 fallback) | M4 |
| F16 | Read-only harness, lookup bench + verifier vs live datadir | M4 |
| F17 | `silt-node` library: capabilities installable in any reth node | M4 |

## 5. VERIFY before coding (facts, not assumptions)

1. Deployed reth version; the pinned tag's `install_exex` / node-builder
   API (expect churn vs. docs).
2. `ExExNotification::ChainReorged` payload: does the new chain arrive in
   the same notification (fold into Reorg+Diff) or separately?
3. `ExecutionOutcome`: receipts shape; whether new bytecode is reachable
   from the bundle (vs. needing a code lookup); destroyed-account
   representation.
4. WAL/`FinishedHeight`: exact semantics at the pinned tag (what is
   retained, what is replayed on restart).
5. yevm `main` SHA to pin for the adapter feature; `Chain` trait exact
   signatures (known today: `get/acc/code/nonce/balance/head/block`,
   async, whole-`Account` resolution).
6. postcard vs bincode final call (postcard assumed: compact, no_std-clean,
   deterministic).

## 6. Risks

- **reth-as-library compile weight**: the wrapper pulls the full node —
  long builds, rustc version coupling. Contained: separate repo, CI cache,
  nobody else depends on it.
- **API churn on reth upgrades**: the wrapper is ≤200 LoC of glue by
  design (D7); gates G1.x are the re-certification suite after any bump.
- **`--dev` chains don't reorg**: the reorg path is covered by synthetic
  notification tests (G1.1/G2.1) + live observation in M3; accepted v0 risk,
  consistent with drop-to-cold being the only reorg behavior.
- **Span folding bugs**: the sharpest correctness corner (ordering,
  delete-then-recreate). Dedicated fixtures (G1.1) + the full-key live
  verifier (G3.1) both target it.
- **Consumer memory growth**: eviction (T2.5) + RSS tracked in the soak.

## 7. Out of scope (v0)

Layered/versioned reorg recovery (provenance bit reserved; design exists),
TLS/auth on the feed, compression, receipt storage/consumption (receipts
ride the wire; the cache exposes them as a raw stream and drops them),
multi-node fan-in, historical backfill beyond the spool.
