# silt — ExEx state-diff feed + chain-state cache

A three-crate workspace (reth enters nobody's tree but the wrapper's):

| Crate | What | Depends on |
|---|---|---|
| `silt-wire` | Diff/head/receipt wire types, framing, handshake | serde + postcard only |
| `silt-exex` | reth wrapper binary with the ExEx installed + TCP server | reth (pinned), wire |
| `silt-cache` | Consumer: subscribes, maintains a coherent chain-state cache, falls back to `eth_*` RPC | wire, HTTP JSON-RPC client; **no reth** |

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
  boundaries (tree check: `reth*` only under `silt-exex`; `yevm*`
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

## 1. Architecture

```
            reth (library, pinned)                     other machine OK
 ┌───────────────────────────────────────┐           ┌──────────────────────────┐
 │ silt-exex (wrapper binary)       │           │ consumer                 │
 │  node builder ── install_exex ──┐     │           │ ┌──────────────────────┐ │
 │                                 ▼     │    TCP    │ │ silt-cache      │ │
 │  ChainCommitted/Reorged → BlockDiff ──┼─ frames ──┼▶│ client → validate →  │ │
 │        │ (fold span, receipts, heads) │  (push +  │ │ apply → state maps   │ │
 │        ▼                              │ Subscribe)│ │   │ miss              │ │
 │  spool(128) ──ack──▶ FinishedHeight   │           │ │   ▼                  │ │
 └───────────────────────────────────────┘           │ │ eth_* RPC (pinned)   │ │
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
