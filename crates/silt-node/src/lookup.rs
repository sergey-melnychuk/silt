//! Lookup fast path server (PLAN D9, D10): binary request/response
//! tip-state reads and trie proofs on their own listener, next to the
//! push-only feed (D8).
//!
//! The server is generic over a small [`StateSource`], so the same code is
//! driven by the node's provider ([`NodeSource`], from the ExEx), by the
//! read-only harness (`examples/lookup-ro.rs`) and by in-memory tests.
//!
//! Threading (D9): per connection, a reader task (frame reads with
//! deadlines, header validation, `Busy` decisions, dispatch) and a writer
//! task (drains the response queue, one deadline per write, flushes when
//! the queue is empty). Database reads run on a dedicated pool of OS
//! threads — never on async workers, the ExEx notification loop, or tokio's
//! blocking pool (which reth's RPC shares). A job takes one state view per
//! request frame, reads every item and encodes the response. In the node a
//! view of a block is reused by later requests at that same block for up
//! to [`VIEW_REUSE`] (1 s), then dropped with its read transaction. Proofs
//! (op 4, D10) run the same way on a second, smaller pool, so a proof walk
//! (ms to s) never queues ahead of point reads (µs). Headers (op 5) and
//! block transactions (op 6) are chain reads, not state reads: they run on
//! the point-read pool, open no view and ignore the pin.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{SyncSender, TrySendError, sync_channel};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use alloy_eips::eip2718::Encodable2718;
use alloy_primitives::{Address, B256, Bytes, keccak256};
use alloy_rlp::{EMPTY_STRING_CODE, Encodable};
use reth_ethereum::provider::{
    BlockReader, ProviderError, StateProviderBox, StateProviderFactory, TransactionVariant,
};
use reth_ethereum::trie::{
    BranchNodeRef, DecodedMultiProofV2, EMPTY_ROOT_HASH, MultiProof, MultiProofTargetsV2, Nibbles,
    ProofTrieNodeV2, ProofV2Target, ProofV2TargetParent, TrieNodeV2,
};
use silt_wire::lookup::{
    self, FINALIZED, FrameWriter, Item, ItemStatus, MAX_INFLIGHT, MAX_REQUEST_BYTES,
    MAX_RESPONSE_BYTES, Op, ProofBuilder, ProofTarget, RequestRef, ResponseBuilder, Status,
};
use silt_wire::{Addr, Frame, KECCAK_EMPTY, Word, hello, hello_mismatch, read_frame, write_frame};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, BufReader};
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

// ------------------------------------------------------------ state access

/// Account fields as the wire carries them; `code_hash` is `KECCAK_EMPTY`
/// for "no code", never zero.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AccountInfo {
    pub nonce: u64,
    pub balance: Word,
    pub code_hash: Word,
}

#[derive(Debug)]
pub enum ReadError {
    /// Unknown, non-canonical, pruned or not-yet-readable block: the whole
    /// frame answers `UnknownBlock`, never partial or stale data.
    UnknownBlock(String),
    Other(String),
}

impl std::fmt::Display for ReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownBlock(m) => write!(f, "unknown block: {m}"),
            Self::Other(m) => f.write_str(m),
        }
    }
}

/// One block's transactions as `(sender, EIP-2718 bytes)`, in block order.
pub type BlockTxs = Vec<(Addr, Vec<u8>)>;

/// One consistent state snapshot at one block (one request frame).
pub trait StateView {
    /// `None` = no such account.
    fn account(&self, addr: &Addr) -> Result<Option<AccountInfo>, ReadError>;
    /// A missing slot reads as zero.
    fn storage(&self, addr: &Addr, slot: &Word) -> Result<Word, ReadError>;
    /// `None` = no bytecode under that hash.
    fn code(&self, code_hash: &Word) -> Result<Option<Bytes>, ReadError>;
    /// Multiproof for pre-hashed targets (D10), written into `out`: account
    /// trie nodes sorted by path, then storage tries sorted by hashed
    /// address, each sorted by path ([`write_multiproof_v2`] does this for
    /// reth's v2 proofs, [`write_multiproof`] for v1's `MultiProof`).
    /// Sources without proofs answer `Internal`.
    fn proof<'a>(
        &self,
        targets: impl Iterator<Item = ProofTarget<'a>>,
        out: &mut ProofBuilder,
    ) -> Result<(), ReadError> {
        let _ = (targets, out);
        Err(ReadError::Other("this source serves no proofs".into()))
    }
}

/// A view plus the block it reads at.
pub struct Opened<V> {
    pub number: u64,
    pub hash: Word,
    pub view: V,
}

/// Where views come from. `open` runs on a pool thread and may block.
pub trait StateSource: Send + Sync + 'static {
    type View: StateView;
    /// `pin = None` means this source's "latest".
    fn open(&self, pin: Option<&Word>) -> Result<Opened<Self::View>, ReadError>;
    /// The block number "latest" reads at now (the proof depth bound, D10).
    fn head(&self) -> Result<u64, ReadError>;
    /// RLP of a consensus header (op 5). `hash = Some`: the header with
    /// that hash (`number` not checked). `None`: the canonical header at
    /// `number`, or the finalized one if `number` is [`FINALIZED`].
    /// `Ok(None)` = no such header (or no finalized block yet).
    fn header(&self, number: u64, hash: Option<&Word>) -> Result<Option<Vec<u8>>, ReadError> {
        let _ = (number, hash);
        Err(ReadError::Other("this source serves no headers".into()))
    }
    /// The transactions of the block with this hash (op 6) as `(sender,
    /// EIP-2718 bytes)` in block order. `Ok(None)` = no such block.
    fn transactions(&self, block_hash: &Word) -> Result<Option<BlockTxs>, ReadError> {
        let _ = block_hash;
        Err(ReadError::Other(
            "this source serves no transactions".into(),
        ))
    }
}

/// Maps reth's errors: everything that means "this block's state is not
/// (or no longer, or not yet) readable" becomes `UnknownBlock`.
pub fn classify(err: ProviderError) -> ReadError {
    match err {
        ProviderError::BlockHashNotFound(_)
        | ProviderError::HeaderNotFound(_)
        | ProviderError::UnknownBlockHash(_)
        | ProviderError::StateForHashNotFound(_)
        | ProviderError::StateForNumberNotFound(_)
        | ProviderError::StateAtBlockPruned(_)
        | ProviderError::InsufficientChangesets { .. }
        | ProviderError::BlockNotExecuted { .. } => ReadError::UnknownBlock(err.to_string()),
        other => ReadError::Other(other.to_string()),
    }
}

/// A reth state provider as a [`StateView`] (node and read-only harness).
pub struct RethView(pub StateProviderBox);

impl StateView for RethView {
    fn account(&self, addr: &Addr) -> Result<Option<AccountInfo>, ReadError> {
        let account = self
            .0
            .basic_account(&Address::from(*addr))
            .map_err(classify)?;
        Ok(account.map(|a| AccountInfo {
            nonce: a.nonce,
            balance: a.balance.to_be_bytes(),
            // reth stores `None` for accounts without code (D9: normalize).
            code_hash: a.bytecode_hash.map_or(KECCAK_EMPTY, |h| h.0),
        }))
    }

    fn storage(&self, addr: &Addr, slot: &Word) -> Result<Word, ReadError> {
        let value = self
            .0
            .storage(Address::from(*addr), B256::from(*slot))
            .map_err(classify)?;
        Ok(value.unwrap_or_default().to_be_bytes())
    }

    fn code(&self, code_hash: &Word) -> Result<Option<Bytes>, ReadError> {
        let code = self
            .0
            .bytecode_by_hash(&B256::from(*code_hash))
            .map_err(classify)?;
        Ok(code.map(|c| c.original_bytes()))
    }

    /// reth's `multiproof_v2`, written exactly as v1's `multiproof` (the
    /// read `silt_multiproof` serves, D8) would write it: see
    /// [`RethView::prove`].
    fn proof<'a>(
        &self,
        targets: impl Iterator<Item = ProofTarget<'a>>,
        out: &mut ProofBuilder,
    ) -> Result<(), ReadError> {
        let targets = ProofTargets::new(targets.map(|t| {
            let slots: Vec<_> = t.slots().map(|s| B256::from(*s)).collect();
            (B256::from(*t.hashed_address), slots)
        }));
        self.prove(&targets, out)
    }
}

impl RethView {
    /// One op-4 proof through reth's `multiproof_v2`. On a view with
    /// executed but unpersisted blocks, v1 `multiproof` clones and re-sorts
    /// the whole in-memory overlay on every call; v2 reads the overlay
    /// reth's engine already built (shared per anchor and tip) through a
    /// calculator this provider keeps, so a reused view (D9) pays that
    /// once. The answer is byte-identical to [`write_multiproof`] of v1
    /// for the same state and targets ([`write_multiproof_v2`]).
    ///
    /// `requires_masks = false`: only node RLP is written, never masks, and
    /// `true` would build a second overlay (with trie changesets) that v1
    /// never reads.
    pub fn prove(&self, targets: &ProofTargets, out: &mut ProofBuilder) -> Result<(), ReadError> {
        let mp = self
            .0
            .multiproof_v2(targets.request(), false)
            .map_err(classify)?;
        write_multiproof_v2(&mp, targets, out)
    }
}

/// Proof targets as v1's `MultiProofTargets` holds them: each hashed
/// address once (repeats merged), its hashed slots without repeats, all
/// sorted.
#[derive(Debug, Default)]
pub struct ProofTargets {
    /// Hashed addresses with their slots as nibble paths.
    accounts: Vec<(B256, Vec<Nibbles>)>,
    /// The addresses as nibble paths, in the same (sorted) order.
    account_paths: Vec<Nibbles>,
}

impl ProofTargets {
    /// `(hashed address, hashed slots)` pairs, in any order, repeats allowed.
    pub fn new<S: IntoIterator<Item = B256>>(targets: impl IntoIterator<Item = (B256, S)>) -> Self {
        let mut merged: BTreeMap<B256, Vec<Nibbles>> = BTreeMap::new();
        for (hashed_address, slots) in targets {
            merged
                .entry(hashed_address)
                .or_default()
                .extend(slots.into_iter().map(Nibbles::unpack));
        }
        let accounts: Vec<_> = merged
            .into_iter()
            .map(|(hashed_address, mut slots)| {
                slots.sort_unstable();
                slots.dedup();
                (hashed_address, slots)
            })
            .collect();
        let account_paths = accounts.iter().map(|(a, _)| Nibbles::unpack(a)).collect();
        Self {
            accounts,
            account_paths,
        }
    }

    /// The v2 request: every address is an account target and a storage
    /// target (no slots asks for the storage root node), as v1 proves the
    /// storage trie of every target account that exists. v1 proves the
    /// account trie's root even with no targets, which v2 answers with
    /// nothing; one probe target makes v2 walk it, and the writer keeps
    /// only the root of that walk.
    fn request(&self) -> MultiProofTargetsV2 {
        let target = |key_nibbles: Nibbles| ProofV2Target {
            key_nibbles,
            parent: ProofV2TargetParent::NONE,
        };
        let mut account_targets: Vec<_> = self.account_paths.iter().copied().map(target).collect();
        if account_targets.is_empty() {
            account_targets.push(ProofV2Target::new(B256::ZERO));
        }
        let storage_targets = self
            .accounts
            .iter()
            .map(|(hashed_address, slots)| {
                (*hashed_address, slots.iter().copied().map(target).collect())
            })
            .collect();
        MultiProofTargetsV2 {
            account_targets,
            storage_targets,
        }
    }
}

/// Writes a reth `MultiProof` into an op-4 answer in the D10 order:
/// account nodes by path, storage tries by hashed address, their nodes by
/// path (reth keeps both in hash maps).
pub fn write_multiproof(mp: &MultiProof, out: &mut ProofBuilder) {
    let mut nodes: Vec<_> = mp.account_subtree.iter().collect();
    nodes.sort_unstable_by(|a, b| a.0.cmp(b.0));
    for (path, rlp) in nodes {
        out.push_account_node(path.len(), &path.pack(), rlp);
    }
    let mut storages: Vec<_> = mp.storages.iter().collect();
    storages.sort_unstable_by_key(|(hashed_address, _)| **hashed_address);
    for (hashed_address, sp) in storages {
        out.begin_storage(&hashed_address.0, &sp.root.0);
        let mut nodes: Vec<_> = sp.subtree.iter().collect();
        nodes.sort_unstable_by(|a, b| a.0.cmp(b.0));
        for (path, rlp) in nodes {
            out.push_storage_node(path.len(), &path.pack(), rlp);
        }
    }
}

/// Writes reth's `multiproof_v2` answer for `targets` into an op-4 answer,
/// byte-identical to [`write_multiproof`] of v1's `multiproof` for the same
/// state and targets. Where the two differ:
///
/// - **Which nodes.** v1 keeps every node it builds whose path is the root
///   or a prefix of a target. v2 merges an extension with its child branch
///   into one node at the extension's path; v1 has both, the branch only
///   when its own path is a prefix of a target. Every v2 node is checked
///   against the same rule ([`v1_retains`]), which also drops the probe
///   walk of an empty request ([`ProofTargets::request`]).
/// - **Order.** v2 lists nodes children first; D10 sorts them by path.
/// - **Storage roots.** v2 carries none: the root is the keccak of the
///   root node's RLP (`0x80` for an empty trie hashes to the empty root).
/// - **Missing accounts.** v1 answers an account that does not exist with
///   an empty storage trie (`[0x80]` at the root) without reading its
///   storage; here the account exists iff its leaf is in the account proof.
pub fn write_multiproof_v2(
    mp: &DecodedMultiProofV2,
    targets: &ProofTargets,
    out: &mut ProofBuilder,
) -> Result<(), ReadError> {
    let mut buf = Vec::with_capacity(1024);
    let mut existing = Vec::new();
    write_trie(
        &mp.account_proofs,
        &targets.account_paths,
        &mut buf,
        |path, rlp| out.push_account_node(path.len(), &path.pack(), rlp),
    )?;
    for node in &mp.account_proofs {
        if let TrieNodeV2::Leaf(leaf) = &node.node {
            let key = node.path.join(&leaf.key);
            if key.len() == 64 {
                existing.push(B256::from_slice(&key.pack()));
            }
        }
    }
    existing.sort_unstable();
    for (hashed_address, slots) in &targets.accounts {
        let nodes = match mp.storage_proofs.get(hashed_address) {
            Some(nodes) if existing.binary_search(hashed_address).is_ok() => nodes,
            Some(_) => {
                out.begin_storage(&hashed_address.0, &EMPTY_ROOT_HASH.0);
                out.push_storage_node(0, &[], &[EMPTY_STRING_CODE]);
                continue;
            }
            None => {
                return Err(ReadError::Other(format!(
                    "multiproof_v2 has no storage proof for {hashed_address}"
                )));
            }
        };
        let root = nodes.iter().find(|n| n.path.is_empty()).ok_or_else(|| {
            ReadError::Other(format!(
                "multiproof_v2 has no storage root node for {hashed_address}"
            ))
        })?;
        buf.clear();
        root.node.encode(&mut buf);
        out.begin_storage(&hashed_address.0, &keccak256(&buf).0);
        write_trie(nodes, slots, &mut buf, |path, rlp| {
            out.push_storage_node(path.len(), &path.pack(), rlp)
        })?;
    }
    Ok(())
}

/// Writes one trie's v2 nodes as v1 keeps them, sorted by path: a merged
/// extension + branch is written as the extension, then (if v1 keeps it)
/// the branch, which sorts right after it, since no other node's path
/// lies between the two.
fn write_trie(
    nodes: &[ProofTrieNodeV2],
    targets: &[Nibbles],
    buf: &mut Vec<u8>,
    mut push: impl FnMut(&Nibbles, &[u8]),
) -> Result<(), ReadError> {
    let mut kept: Vec<_> = nodes
        .iter()
        .filter(|n| v1_retains(targets, &n.path))
        .collect();
    kept.sort_unstable_by_key(|n| n.path);
    for n in kept {
        buf.clear();
        n.node.encode(buf);
        push(&n.path, buf);
        match &n.node {
            TrieNodeV2::Branch(branch) if !branch.key.is_empty() => {
                let path = n.path.join(&branch.key);
                if v1_retains(targets, &path) {
                    buf.clear();
                    BranchNodeRef::new(&branch.stack, branch.state_mask).encode(buf);
                    push(&path, buf);
                }
            }
            // An extension whose branch v2 left hashed: v1 never keeps that
            // branch either, unless it leads to a target, which v2 always
            // reveals. Refuse rather than answer short.
            TrieNodeV2::Extension(ext) if v1_retains(targets, &n.path.join(&ext.key)) => {
                return Err(ReadError::Other(format!(
                    "multiproof_v2 left the branch below {:?} hashed",
                    n.path
                )));
            }
            _ => {}
        }
    }
    Ok(())
}

/// v1's rule for keeping a node (alloy-trie's `ProofRetainer::matches`):
/// the root, or a prefix of a target. `targets` sorted.
fn v1_retains(targets: &[Nibbles], path: &Nibbles) -> bool {
    path.is_empty()
        || targets
            .get(targets.partition_point(|t| t < path))
            .is_some_and(|t| t.starts_with(path))
}

/// How long an opened view of one block is reused (PLAN M6). A simulation
/// on a consumer misses one key at a time, ~100 µs apart, and opening a
/// provider per request was a large part of each miss. A view is only
/// reused for the same block hash, so every answer stays exact; a janitor
/// drops views older than this, so an idle read transaction lives at most
/// 1.5× this long.
pub const VIEW_REUSE: Duration = Duration::from_secs(1);

/// Idle views kept for reuse, at most this many (one per busy worker).
const MAX_IDLE_VIEWS: usize = 32;

/// An opened view waiting for the next request at its block.
struct IdleView {
    hash: Word,
    opened: Instant,
    view: RethView,
}

/// Parked views; `enabled` is off when there's no janitor to expire them.
#[derive(Default)]
struct Idle {
    enabled: bool,
    views: Vec<IdleView>,
}

type IdleViews = Arc<Mutex<Idle>>;

/// The node's own provider (`ctx.provider()`): "latest" is the canonical
/// head, including blocks executed but not yet persisted. Views are reused
/// across requests for the same block for up to [`VIEW_REUSE`].
pub struct NodeSource<P> {
    provider: P,
    idle: IdleViews,
}

impl<P> NodeSource<P> {
    pub fn new(provider: P) -> Self {
        let idle: IdleViews = Arc::default();
        let weak = Arc::downgrade(&idle);
        let janitor = std::thread::Builder::new()
            .name("silt-lookup-views".into())
            .spawn(move || {
                loop {
                    std::thread::sleep(VIEW_REUSE / 2);
                    let Some(idle) = weak.upgrade() else { return };
                    idle.lock()
                        .expect("idle views lock")
                        .views
                        .retain(|v| v.opened.elapsed() < VIEW_REUSE);
                }
            });
        match janitor {
            Ok(_) => idle.lock().expect("idle views lock").enabled = true,
            // Without a janitor, parked views could hold read transactions
            // indefinitely: park none.
            Err(err) => tracing::warn!(%err, "silt lookup: no view janitor; views not reused"),
        }
        Self { provider, idle }
    }

    /// An idle view of block `hash` young enough to reuse, if any.
    fn reuse(&self, hash: &Word) -> Option<IdleView> {
        let mut idle = self.idle.lock().expect("idle views lock");
        let at = idle
            .views
            .iter()
            .position(|v| v.hash == *hash && v.opened.elapsed() < VIEW_REUSE)?;
        Some(idle.views.swap_remove(at))
    }

    fn view(&self, hash: Word, opened: Instant, view: RethView) -> NodeView {
        NodeView {
            view: Some(view),
            hash,
            opened,
            home: self.idle.clone(),
        }
    }
}

/// A view from [`NodeSource`]: parked for reuse when the request is done,
/// if it is still young.
pub struct NodeView {
    view: Option<RethView>,
    hash: Word,
    opened: Instant,
    home: IdleViews,
}

impl NodeView {
    fn inner(&self) -> &RethView {
        self.view.as_ref().expect("view present until drop")
    }
}

impl Drop for NodeView {
    fn drop(&mut self) {
        let Some(view) = self.view.take() else { return };
        if self.opened.elapsed() >= VIEW_REUSE {
            return;
        }
        let mut idle = self.home.lock().expect("idle views lock");
        if idle.enabled && idle.views.len() < MAX_IDLE_VIEWS {
            idle.views.push(IdleView {
                hash: self.hash,
                opened: self.opened,
                view,
            });
        }
    }
}

impl StateView for NodeView {
    fn account(&self, addr: &Addr) -> Result<Option<AccountInfo>, ReadError> {
        self.inner().account(addr)
    }

    fn storage(&self, addr: &Addr, slot: &Word) -> Result<Word, ReadError> {
        self.inner().storage(addr, slot)
    }

    fn code(&self, code_hash: &Word) -> Result<Option<Bytes>, ReadError> {
        self.inner().code(code_hash)
    }

    fn proof<'a>(
        &self,
        targets: impl Iterator<Item = ProofTarget<'a>>,
        out: &mut ProofBuilder,
    ) -> Result<(), ReadError> {
        self.inner().proof(targets, out)
    }
}

impl<P> StateSource for NodeSource<P>
where
    P: StateProviderFactory + BlockReader + Send + Sync + 'static,
{
    type View = NodeView;

    fn open(&self, pin: Option<&Word>) -> Result<Opened<NodeView>, ReadError> {
        let Some(pin) = pin else {
            // Resolve latest to a hash first, so the response names exactly
            // what was read. A reorg between the two calls can orphan that
            // hash; one retry picks up the new head.
            let mut last = None;
            for _ in 0..2 {
                let info = self.provider.chain_info().map_err(classify)?;
                let hash = info.best_hash.0;
                if let Some(idle) = self.reuse(&hash) {
                    return Ok(Opened {
                        number: info.best_number,
                        hash,
                        view: self.view(hash, idle.opened, idle.view),
                    });
                }
                match self.provider.state_by_block_hash(info.best_hash) {
                    Ok(state) => {
                        return Ok(Opened {
                            number: info.best_number,
                            hash,
                            view: self.view(hash, Instant::now(), RethView(state)),
                        });
                    }
                    Err(err) => last = Some(err),
                }
            }
            return Err(classify(last.expect("loop ran")));
        };
        let hash = B256::from(*pin);
        // Canonical only: `state_by_block_hash` alone would also serve the
        // pending block (docs/reth-notes.md). Checked on every request, so
        // a reused view is never served for a block a reorg dropped.
        let number = self
            .provider
            .block_number(hash)
            .map_err(classify)?
            .ok_or_else(|| ReadError::UnknownBlock(format!("{hash} is not canonical")))?;
        if self.provider.block_hash(number).map_err(classify)? != Some(hash) {
            return Err(ReadError::UnknownBlock(format!("{hash} is not canonical")));
        }
        if let Some(idle) = self.reuse(pin) {
            return Ok(Opened {
                number,
                hash: *pin,
                view: self.view(*pin, idle.opened, idle.view),
            });
        }
        let state = self.provider.state_by_block_hash(hash).map_err(classify)?;
        Ok(Opened {
            number,
            hash: hash.0,
            view: self.view(*pin, Instant::now(), RethView(state)),
        })
    }

    fn head(&self) -> Result<u64, ReadError> {
        Ok(self.provider.chain_info().map_err(classify)?.best_number)
    }

    fn header(&self, number: u64, hash: Option<&Word>) -> Result<Option<Vec<u8>>, ReadError> {
        let header = match hash {
            Some(hash) => self.provider.header(B256::from(*hash)),
            None if number == FINALIZED => {
                let Some(finalized) = self.provider.finalized_block_num_hash().map_err(classify)?
                else {
                    return Ok(None);
                };
                self.provider.header(finalized.hash)
            }
            None => self.provider.header_by_number(number),
        }
        .map_err(classify)?;
        Ok(header.map(|h| {
            let mut rlp = Vec::with_capacity(h.length());
            h.encode(&mut rlp);
            rlp
        }))
    }

    fn transactions(&self, block_hash: &Word) -> Result<Option<BlockTxs>, ReadError> {
        // Senders come from the database (or are recovered); tx hashes
        // aren't needed to encode.
        let block = self
            .provider
            .recovered_block(B256::from(*block_hash).into(), TransactionVariant::NoHash)
            .map_err(classify)?;
        Ok(block.map(|block| {
            block
                .transactions_with_sender()
                .map(|(sender, tx)| (sender.0.0, tx.encoded_2718()))
                .collect()
        }))
    }
}

// --------------------------------------------------------------- the server

#[derive(Clone, Debug)]
pub struct LookupConfig {
    /// Dedicated read threads.
    pub workers: usize,
    /// Server-wide queued jobs; a request arriving when it is full answers
    /// `Busy`.
    pub queue: usize,
    pub handshake_timeout: Duration,
    /// Rest of a frame once its length arrived.
    pub frame_timeout: Duration,
    pub write_timeout: Duration,
    /// Silence between frames before the connection is dropped (clients
    /// ping every 30 s).
    pub idle_timeout: Duration,
    /// Dedicated proof threads (D10), apart from `workers`.
    pub proof_workers: usize,
    /// Server-wide queued proof jobs; `Busy` beyond.
    pub proof_queue: usize,
    /// Pinned proofs more than this many blocks below the source's head
    /// answer `UnknownBlock`: reth's revert overlay grows with the
    /// distance, about 25 ms per block on the persisted-state path
    /// (D10, docs/proof-bench.md).
    pub proof_max_depth: u64,
}

impl Default for LookupConfig {
    fn default() -> Self {
        Self {
            workers: 16,
            queue: 4096,
            proof_workers: 4,
            proof_queue: 64,
            proof_max_depth: 8,
            handshake_timeout: Duration::from_secs(10),
            frame_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(120),
        }
    }
}

type Job = Box<dyn FnOnce() + Send>;

/// Fixed pool of named OS threads behind a bounded queue.
struct Pool {
    tx: SyncSender<Job>,
}

impl Pool {
    fn new(name: &str, workers: usize, queue: usize) -> std::io::Result<Self> {
        let (tx, rx) = sync_channel::<Job>(queue.max(1));
        let rx = Arc::new(Mutex::new(rx));
        for i in 0..workers.max(1) {
            let rx = rx.clone();
            std::thread::Builder::new()
                .name(format!("{name}-{i}"))
                .spawn(move || {
                    loop {
                        // The lock is held only while waiting for one job.
                        let job = match rx.lock() {
                            Ok(rx) => rx.recv(),
                            Err(_) => return,
                        };
                        let Ok(job) = job else { return }; // server dropped
                        // A panicking read must not shrink the pool; the job
                        // itself turns the panic into an `Internal` answer.
                        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(job));
                    }
                })?;
        }
        Ok(Self { tx })
    }

    fn try_submit(&self, job: Job) -> bool {
        match self.tx.try_send(job) {
            Ok(()) => true,
            Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) => false,
        }
    }
}

pub struct LookupServer<S> {
    source: Arc<S>,
    pool: Pool,
    proof_pool: Pool,
    chain_id: u64,
    genesis_hash: Word,
    /// Newest block any request read at (the `tip` in our Hello).
    tip: Arc<AtomicU64>,
    cfg: LookupConfig,
}

impl<S: StateSource> LookupServer<S> {
    pub fn new(
        source: S,
        chain_id: u64,
        genesis_hash: Word,
        cfg: LookupConfig,
    ) -> std::io::Result<Arc<Self>> {
        Ok(Arc::new(Self {
            source: Arc::new(source),
            pool: Pool::new("silt-lookup", cfg.workers, cfg.queue)?,
            proof_pool: Pool::new("silt-proof", cfg.proof_workers, cfg.proof_queue)?,
            chain_id,
            genesis_hash,
            tip: Arc::new(AtomicU64::new(0)),
            cfg,
        }))
    }

    /// Accept loop; one reader task + one flusher task per connection.
    pub async fn serve(self: Arc<Self>, listener: TcpListener) {
        loop {
            match listener.accept().await {
                Ok((stream, peer)) => {
                    // Small frames, latency-bound: never wait for Nagle.
                    let _ = stream.set_nodelay(true);
                    let this = self.clone();
                    tokio::spawn(async move {
                        if let Err(err) = this.handle(stream).await {
                            tracing::debug!(%peer, %err, "silt lookup connection closed");
                        }
                    });
                }
                Err(err) => {
                    tracing::error!(%err, "silt lookup accept failed");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    /// Serves one connection until EOF, a protocol error or a deadline.
    ///
    /// Slot accounting (D9): `jobs` (`MAX_INFLIGHT`) admits requests and is
    /// released as soon as a response is *built* — before it can reach the
    /// client — so a client that keeps at most `MAX_INFLIGHT` requests
    /// outstanding never sees `Busy`. `outputs` bounds every frame not yet
    /// on the socket (job answers and reader-originated `Busy`/`BadRequest`
    /// alike): the reader waits for one before taking a request, so a
    /// client that stops reading stalls the reader (TCP pushes back) until
    /// the write deadline drops it.
    pub async fn handle(self: Arc<Self>, mut stream: TcpStream) -> eyre::Result<()> {
        tokio::time::timeout(self.cfg.handshake_timeout, self.handshake(&mut stream))
            .await
            .map_err(|_| {
                eyre::eyre!("handshake timed out after {:?}", self.cfg.handshake_timeout)
            })??;

        let (rd, wr) = stream.into_split();
        let writer = Arc::new(FrameWriter::new(wr));
        let jobs = Arc::new(Semaphore::new(MAX_INFLIGHT));
        let outputs = Arc::new(Semaphore::new(MAX_INFLIGHT + OUTPUT_SLACK));
        let flusher = writer.clone();
        let write_timeout = self.cfg.write_timeout;
        let mut flusher = tokio::spawn(async move { flusher.run(write_timeout).await });
        let read = tokio::select! {
            read = self.read_loop(rd, &writer, &jobs, &outputs) => read,
            // The flusher only ends early on failure (e.g. its deadline):
            // the connection is dead, don't wait for the idle timeout.
            flushed = &mut flusher => {
                return match flushed {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(err)) => Err(err.into()),
                    Err(err) => Err(eyre::eyre!("flusher task: {err}")),
                };
            }
        };
        if read.is_err() {
            writer.close();
            flusher.abort();
            return read;
        }
        // Clean EOF: answers still in flight go out, then the write side
        // closes once the last job has handed its frame over.
        let _ = outputs
            .acquire_many((MAX_INFLIGHT + OUTPUT_SLACK) as u32)
            .await;
        writer.close();
        flusher
            .await
            .map_err(|e| eyre::eyre!("flusher task: {e}"))??;
        Ok(())
    }

    async fn handshake<T>(&self, stream: &mut T) -> eyre::Result<()>
    where
        T: AsyncRead + AsyncWrite + Unpin,
    {
        let ours = hello(
            self.chain_id,
            self.genesis_hash,
            self.tip.load(Ordering::Relaxed),
        );
        write_frame(stream, &ours).await?;
        let theirs = read_frame(stream)
            .await?
            .ok_or_else(|| eyre::eyre!("eof in handshake"))?;
        if let Some(reason) = hello_mismatch(&ours, &theirs) {
            write_frame(
                stream,
                &Frame::Refused {
                    reason: reason.clone(),
                },
            )
            .await?;
            eyre::bail!("refused: {reason}");
        }
        Ok(())
    }

    async fn read_loop(
        &self,
        rd: OwnedReadHalf,
        writer: &Arc<FrameWriter<OwnedSemaphorePermit>>,
        jobs: &Arc<Semaphore>,
        outputs: &Arc<Semaphore>,
    ) -> eyre::Result<()> {
        let mut rd = BufReader::with_capacity(64 * 1024, rd);
        loop {
            let mut len = [0u8; 4];
            match tokio::time::timeout(self.cfg.idle_timeout, rd.read_exact(&mut len)).await {
                Err(_) => eyre::bail!("idle for {:?}", self.cfg.idle_timeout),
                Ok(Err(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
                Ok(Err(e)) => return Err(e.into()),
                Ok(Ok(_)) => {}
            }
            // A length we can't trust means a stream we can't trust: close.
            let len = lookup::check_len(u32::from_le_bytes(len), MAX_REQUEST_BYTES)?;
            let mut body = vec![0u8; len];
            tokio::time::timeout(self.cfg.frame_timeout, rd.read_exact(&mut body))
                .await
                .map_err(|_| {
                    eyre::eyre!(
                        "frame body not received within {:?}",
                        self.cfg.frame_timeout
                    )
                })??;
            let started = Instant::now();
            // Every frame read produces exactly one output frame.
            let output = outputs.clone().acquire_owned().await?;

            let req = match RequestRef::decode(&body) {
                Ok(req) => req,
                Err(err) => {
                    let (op, flags, id) = lookup::peek_prefix(&body).expect("len >= PREFIX");
                    tracing::debug!(%err, id, "silt lookup: bad request");
                    let frame =
                        lookup::error_response(op, flags, id, Status::BadRequest, micros(started));
                    writer.send(frame, output)?;
                    continue;
                }
            };
            let (op, flags, id) = (req.op as u8, req.flags, req.id);
            // Proofs (D10) get their own threads; read before `body` moves.
            let pool = if req.op == Op::Proof {
                &self.proof_pool
            } else {
                &self.pool
            };
            let busy = || lookup::error_response(op, flags, id, Status::Busy, micros(started));
            let Ok(admitted) = jobs.clone().try_acquire_owned() else {
                writer.send(busy(), output)?;
                continue;
            };
            let job = {
                let source = self.source.clone();
                let tip = self.tip.clone();
                let writer = writer.clone();
                let max_depth = self.cfg.proof_max_depth;
                Box::new(move || {
                    let frame = answer(&*source, &body, started, &tip, max_depth);
                    drop(admitted); // built: no longer in flight (see `handle`)
                    // Straight to the socket from this thread when it can
                    // take it (no task hop); fails only once closed.
                    let _ = writer.send(frame, output);
                }) as Job
            };
            if !pool.try_submit(job) {
                // Server-wide queue full: the rejected job dropped its
                // slots; take a fresh output slot for the Busy answer.
                let output = outputs.clone().acquire_owned().await?;
                writer.send(busy(), output)?;
            }
        }
    }
}

/// Output slots beyond `MAX_INFLIGHT`, for `Busy`/`BadRequest` answers.
const OUTPUT_SLACK: usize = 64;

fn micros(since: Instant) -> u32 {
    since.elapsed().as_micros().min(u32::MAX as u128) as u32
}

/// An answer being built: lookups (ops 1–3) or a proof (op 4).
enum Built {
    Lookup(ResponseBuilder),
    Proof(ProofBuilder),
}

impl Built {
    fn body_len(&self) -> usize {
        match self {
            Self::Lookup(b) => b.body_len(),
            Self::Proof(b) => b.body_len(),
        }
    }

    fn finish(self, server_us: u32) -> Vec<u8> {
        match self {
            Self::Lookup(b) => b.finish(server_us),
            Self::Proof(b) => b.finish(server_us),
        }
    }
}

/// One request frame, start to finish, on a pool thread: open one view,
/// read every item in order (or walk the proof), drop the view, encode.
fn answer<S: StateSource>(
    source: &S,
    body: &[u8],
    started: Instant,
    tip: &AtomicU64,
    proof_max_depth: u64,
) -> Vec<u8> {
    let req = RequestRef::decode(body).expect("validated by the reader");
    let fail =
        |status| lookup::error_response(req.op as u8, req.flags, req.id, status, micros(started));
    let run = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| match req.op {
        Op::Proof => prove(source, &req, proof_max_depth).map(|(b, n)| (Built::Proof(b), n)),
        Op::Header | Op::Transactions => {
            read_chain(source, &req).map(|(b, n)| (Built::Lookup(b), n))
        }
        Op::Account | Op::Storage | Op::Code => {
            read_all(source, &req).map(|(b, n)| (Built::Lookup(b), n))
        }
    }));
    let out = match run {
        Ok(Ok(out)) => out,
        Ok(Err(ReadError::UnknownBlock(msg))) => {
            tracing::debug!(id = req.id, msg, "silt lookup: unknown block");
            return fail(Status::UnknownBlock);
        }
        Ok(Err(ReadError::Other(msg))) => {
            tracing::debug!(id = req.id, msg, "silt lookup: internal error");
            return fail(Status::Internal);
        }
        Err(_) => {
            tracing::error!(id = req.id, "silt lookup: read panicked");
            return fail(Status::Internal);
        }
    };
    if out.0.body_len() > MAX_RESPONSE_BYTES {
        return fail(Status::TooLarge);
    }
    tip.fetch_max(out.1, Ordering::Relaxed);
    out.0.finish(micros(started))
}

/// One Proof frame (D10): resolve the pin, refuse pins deeper than
/// `max_depth` below the head, walk, drop the view.
fn prove<S: StateSource>(
    source: &S,
    req: &RequestRef<'_>,
    max_depth: u64,
) -> Result<(ProofBuilder, u64), ReadError> {
    let pin = (!req.is_latest()).then_some(req.pin);
    let Opened { number, hash, view } = source.open(pin)?;
    if pin.is_some() {
        let head = source.head()?;
        if head.saturating_sub(number) > max_depth {
            return Err(ReadError::UnknownBlock(format!(
                "block {number} is {} below the head {head}; proofs serve the last {max_depth}",
                head - number
            )));
        }
    }
    let mut out = ProofBuilder::new(req.flags, req.id, number, &hash, 0);
    if req.count() > 0 {
        let targets = req.items().map(|item| match item {
            Item::Proof(t) => t,
            _ => unreachable!("op validated as Proof"),
        });
        view.proof(targets, &mut out)?;
    }
    // The read transaction ends here, before the response is finished.
    drop(view);
    Ok((out, number))
}

/// Reads all items into a response; a frame-level failure (state not
/// readable at the pin) aborts the whole frame.
fn read_all<S: StateSource>(
    source: &S,
    req: &RequestRef<'_>,
) -> Result<(ResponseBuilder, u64), ReadError> {
    let pin = (!req.is_latest()).then_some(req.pin);
    let Opened { number, hash, view } = source.open(pin)?;
    let mut out = ResponseBuilder::new(req.op, req.flags, req.id, number, &hash, req.count());
    let inline = req.inline_code();
    for item in req.items() {
        match item {
            Item::Account(addr) => match view.account(addr) {
                Ok(None) => out.push_account_absent(),
                Ok(Some(acct)) => {
                    let code = if inline && acct.code_hash != KECCAK_EMPTY {
                        match view.code(&acct.code_hash) {
                            Ok(Some(code)) => Some(code),
                            // A code hash with no code is a broken
                            // invariant: answer this key with Error.
                            Ok(None) => None,
                            Err(ReadError::UnknownBlock(m)) => {
                                return Err(ReadError::UnknownBlock(m));
                            }
                            Err(ReadError::Other(_)) => None,
                        }
                    } else {
                        Some(Bytes::new())
                    };
                    match code {
                        Some(code) => out.push_account(
                            ItemStatus::Ok,
                            acct.nonce,
                            &acct.balance,
                            &acct.code_hash,
                            &code,
                        ),
                        None => out.push_account_error(),
                    }
                }
                Err(ReadError::UnknownBlock(m)) => return Err(ReadError::UnknownBlock(m)),
                Err(ReadError::Other(_)) => out.push_account_error(),
            },
            Item::Storage(addr, slot) => match view.storage(addr, slot) {
                Ok(value) => out.push_storage(ItemStatus::Ok, &value),
                Err(ReadError::UnknownBlock(m)) => return Err(ReadError::UnknownBlock(m)),
                Err(ReadError::Other(_)) => out.push_storage(ItemStatus::Error, &[0; 32]),
            },
            Item::Code(code_hash) if *code_hash == KECCAK_EMPTY => {
                out.push_code(ItemStatus::Ok, &[]);
            }
            Item::Code(code_hash) => match view.code(code_hash) {
                Ok(Some(code)) => out.push_code(ItemStatus::Ok, &code),
                Ok(None) => out.push_code(ItemStatus::Absent, &[]),
                Err(ReadError::UnknownBlock(m)) => return Err(ReadError::UnknownBlock(m)),
                Err(ReadError::Other(_)) => out.push_code(ItemStatus::Error, &[]),
            },
            Item::Proof(_) => unreachable!("Proof frames go to prove()"),
            Item::Header { .. } | Item::Transactions { .. } => {
                unreachable!("chain reads go to read_chain()")
            }
        }
        if out.body_len() > MAX_RESPONSE_BYTES {
            break; // answered `TooLarge` by the caller
        }
    }
    // The read transaction ends here, before the response is finished.
    drop(view);
    Ok((out, number))
}

/// One Header or Transactions frame: no state view, the pin ignored. The
/// response names the head at the time of the read (`read_hash` zero);
/// each item's failure is that item's `Error` only.
fn read_chain<S: StateSource>(
    source: &S,
    req: &RequestRef<'_>,
) -> Result<(ResponseBuilder, u64), ReadError> {
    let head = source.head()?;
    let mut out = ResponseBuilder::new(req.op, req.flags, req.id, head, &[0; 32], req.count());
    for item in req.items() {
        match item {
            Item::Header { number, hash } => {
                let hash = (*hash != [0; 32]).then_some(hash);
                match source.header(number, hash) {
                    Ok(Some(rlp)) => out.push_header(ItemStatus::Ok, &rlp),
                    Ok(None) => out.push_header(ItemStatus::Absent, &[]),
                    Err(err) => {
                        tracing::debug!(id = req.id, number, %err, "silt lookup: header read failed");
                        out.push_header(ItemStatus::Error, &[]);
                    }
                }
            }
            Item::Transactions { block_hash } => match source.transactions(block_hash) {
                Ok(Some(txs)) => out.push_transactions(
                    ItemStatus::Ok,
                    txs.iter().map(|(sender, tx)| (sender, tx.as_slice())),
                ),
                Ok(None) => out.push_transactions(ItemStatus::Absent, std::iter::empty()),
                Err(err) => {
                    tracing::debug!(id = req.id, %err, "silt lookup: transactions read failed");
                    out.push_transactions(ItemStatus::Error, std::iter::empty());
                }
            },
            _ => unreachable!("op validated as Header or Transactions"),
        }
        if out.body_len() > MAX_RESPONSE_BYTES {
            break; // answered `TooLarge` by the caller
        }
    }
    Ok((out, head))
}

#[cfg(test)]
mod tests {
    use super::*;
    use silt_wire::lookup::{
        FLAG_INLINE_CODE, LATEST, RequestBuilder, ResponseRef, ResultRef, pack_path, read_body,
    };
    use std::collections::HashMap;
    use std::sync::Condvar;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpSocket;
    use tokio::task::JoinHandle;

    const CHAIN_ID: u64 = 1;
    const GENESIS: Word = [7; 32];
    const HEAD: (u64, Word) = (100, [0x10; 32]);
    const PARENT: (u64, Word) = (99, [0x09; 32]);
    /// Pin whose open fails with a non-block error.
    const BROKEN_PIN: Word = [0xDE; 32];

    const CONTRACT: Addr = [0xC0; 20];
    const EOA: Addr = [0xE0; 20];
    const MISSING: Addr = [0x00; 20];
    /// Account read fails (item `Error`).
    const FAILING: Addr = [0xFA; 20];
    /// Account read panics.
    const PANICS: Addr = [0x99; 20];
    /// Account read blocks until the gate opens.
    const SLOW: Addr = [0x51; 20];
    /// Storage key whose value depends on the block read.
    const PER_BLOCK: (Addr, Word) = ([0xAB; 20], [1; 32]);
    const CODE: &[u8] = &[0x60, 0x00, 0x60, 0x00, 0xf3];
    const CODE_HASH: Word = [0xC1; 32];
    const HUGE_HASH: Word = [0x4B; 32];
    /// Proof targets with scripted behavior.
    const PROOF_SLOW: Word = [0x51; 32];
    const PROOF_HUGE: Word = [0x4B; 32];
    const PROOF_FAILS: Word = [0xFA; 32];
    /// Header number whose read fails (item `Error`).
    const FAILING_HEADER: u64 = 66;
    /// Block hash whose transactions read fails (item `Error`).
    const FAILING_BLOCK: Word = [0xFB; 32];
    const TX_SENDERS: [Addr; 2] = [[0x5E; 20], [0x5F; 20]];

    /// The header RLP the source serves for block `number`.
    fn header_rlp(number: u64) -> Vec<u8> {
        vec![0xF9, 0x02, number as u8]
    }

    #[derive(Default)]
    struct Gate {
        open: Mutex<bool>,
        cv: Condvar,
        entered: AtomicU64,
    }

    impl Gate {
        fn wait(&self) {
            self.entered.fetch_add(1, Ordering::SeqCst);
            let mut open = self.open.lock().unwrap();
            while !*open {
                open = self.cv.wait(open).unwrap();
            }
        }
        fn release(&self) {
            *self.open.lock().unwrap() = true;
            self.cv.notify_all();
        }
    }

    struct Data {
        storage: HashMap<(Addr, Word), Word>,
        /// Canonical headers: (number, hash, RLP).
        headers: Vec<(u64, Word, Vec<u8>)>,
        /// The finalized block number, if any.
        finalized: Mutex<Option<u64>>,
        /// Block hash -> (sender, EIP-2718 bytes).
        blocks: HashMap<Word, Vec<(Addr, Vec<u8>)>>,
        gate: Gate,
    }

    struct MemSource(Arc<Data>);
    struct MemView(Arc<Data>, u64);

    impl StateView for MemView {
        fn account(&self, addr: &Addr) -> Result<Option<AccountInfo>, ReadError> {
            match *addr {
                CONTRACT => Ok(Some(AccountInfo {
                    nonce: 1,
                    balance: [0; 32],
                    code_hash: CODE_HASH,
                })),
                EOA | SLOW => {
                    if *addr == SLOW {
                        self.0.gate.wait();
                    }
                    let mut balance = [0; 32];
                    balance[31] = self.1 as u8; // block-dependent
                    Ok(Some(AccountInfo {
                        nonce: 5,
                        balance,
                        code_hash: KECCAK_EMPTY,
                    }))
                }
                FAILING => Err(ReadError::Other("disk on fire".into())),
                PANICS => panic!("read panicked"),
                _ => Ok(None),
            }
        }
        fn storage(&self, addr: &Addr, slot: &Word) -> Result<Word, ReadError> {
            if (*addr, *slot) == PER_BLOCK {
                return Ok([self.1 as u8; 32]);
            }
            Ok(self
                .0
                .storage
                .get(&(*addr, *slot))
                .copied()
                .unwrap_or([0; 32]))
        }
        fn code(&self, code_hash: &Word) -> Result<Option<Bytes>, ReadError> {
            Ok(match *code_hash {
                CODE_HASH => Some(Bytes::from_static(CODE)),
                HUGE_HASH => Some(Bytes::from(vec![0x5b; 300 * 1024])),
                _ => None,
            })
        }
        /// Root RLP `[0xF8, block]`; one account node per target at its
        /// first two nibbles; one storage trie (root `[0x5A; 32]`) per
        /// target with slots, one node per slot. Sorted, as the trait asks.
        fn proof<'a>(
            &self,
            targets: impl Iterator<Item = ProofTarget<'a>>,
            out: &mut ProofBuilder,
        ) -> Result<(), ReadError> {
            let mut targets: Vec<_> = targets.collect();
            targets.sort_by_key(|t| *t.hashed_address);
            let has = |w: Word| targets.iter().any(|t| *t.hashed_address == w);
            if has(PROOF_SLOW) {
                self.0.gate.wait();
            }
            if has(PROOF_FAILS) {
                return Err(ReadError::Other("trie on fire".into()));
            }
            if has(PROOF_HUGE) {
                for i in 0..17u8 {
                    out.push_account_node(2, &pack_path(&[i >> 4, i & 15]), &vec![0xB9; 1 << 20]);
                }
                return Ok(());
            }
            out.push_account_node(0, &[], &[0xF8, self.1 as u8]);
            for t in &targets {
                let a = t.hashed_address[0];
                out.push_account_node(2, &pack_path(&[a >> 4, a & 15]), &[a]);
            }
            for t in targets.iter().filter(|t| t.slot_count() > 0) {
                out.begin_storage(t.hashed_address, &[0x5A; 32]);
                for slot in t.slots() {
                    out.push_storage_node(1, &pack_path(&[slot[0] >> 4]), &[slot[0]]);
                }
            }
            Ok(())
        }
    }

    impl StateSource for MemSource {
        type View = MemView;
        fn open(&self, pin: Option<&Word>) -> Result<Opened<MemView>, ReadError> {
            let (number, hash) = match pin {
                None => HEAD,
                Some(h) if *h == HEAD.1 => HEAD,
                Some(h) if *h == PARENT.1 => PARENT,
                Some(h) if *h == BROKEN_PIN => return Err(ReadError::Other("io".into())),
                Some(_) => return Err(ReadError::UnknownBlock("not canonical".into())),
            };
            Ok(Opened {
                number,
                hash,
                view: MemView(self.0.clone(), number),
            })
        }
        fn head(&self) -> Result<u64, ReadError> {
            Ok(HEAD.0)
        }
        fn header(&self, number: u64, hash: Option<&Word>) -> Result<Option<Vec<u8>>, ReadError> {
            let number = match hash {
                Some(h) => match self.0.headers.iter().find(|(_, hh, _)| hh == h) {
                    Some((n, ..)) => *n,
                    None => return Ok(None),
                },
                None if number == FINALIZED => match *self.0.finalized.lock().unwrap() {
                    Some(n) => n,
                    None => return Ok(None),
                },
                None if number == FAILING_HEADER => {
                    return Err(ReadError::Other("header on fire".into()));
                }
                None => number,
            };
            Ok(self
                .0
                .headers
                .iter()
                .find(|(n, ..)| *n == number)
                .map(|(.., rlp)| rlp.clone()))
        }
        fn transactions(&self, block_hash: &Word) -> Result<Option<BlockTxs>, ReadError> {
            if *block_hash == FAILING_BLOCK {
                return Err(ReadError::Other("block on fire".into()));
            }
            Ok(self.0.blocks.get(block_hash).cloned())
        }
    }

    fn server(cfg: LookupConfig) -> (Arc<LookupServer<MemSource>>, Arc<Data>) {
        let data = Arc::new(Data {
            storage: HashMap::from([((EOA, [2; 32]), [0x2A; 32])]),
            headers: [PARENT, HEAD]
                .into_iter()
                .map(|(n, h)| (n, h, header_rlp(n)))
                .collect(),
            finalized: Mutex::new(Some(PARENT.0)),
            blocks: HashMap::from([
                (
                    HEAD.1,
                    vec![
                        (TX_SENDERS[0], vec![0x02, 0xF8, 0x01]),
                        (TX_SENDERS[1], vec![0xF8, 0x02]),
                    ],
                ),
                (PARENT.1, vec![]),
            ]),
            gate: Gate::default(),
        });
        let s = LookupServer::new(MemSource(data.clone()), CHAIN_ID, GENESIS, cfg).unwrap();
        (s, data)
    }

    fn short() -> LookupConfig {
        LookupConfig {
            workers: 4,
            handshake_timeout: Duration::from_millis(300),
            frame_timeout: Duration::from_millis(300),
            write_timeout: Duration::from_millis(300),
            idle_timeout: Duration::from_millis(300),
            ..Default::default()
        }
    }

    /// A loopback TCP pair; `rcvbuf` shrinks the client's receive buffer.
    async fn pair(rcvbuf: Option<u32>) -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let socket = TcpSocket::new_v4().unwrap();
        if let Some(size) = rcvbuf {
            socket.set_recv_buffer_size(size).unwrap();
        }
        let (client, server) = tokio::join!(
            socket.connect(listener.local_addr().unwrap()),
            listener.accept()
        );
        (client.unwrap(), server.unwrap().0)
    }

    async fn connect_with(
        s: &Arc<LookupServer<MemSource>>,
        rcvbuf: Option<u32>,
        chain_id: u64,
    ) -> (TcpStream, JoinHandle<eyre::Result<()>>) {
        let (mut client, srv) = pair(rcvbuf).await;
        let task = tokio::spawn(s.clone().handle(srv));
        let theirs = read_frame(&mut client).await.unwrap().unwrap();
        assert!(matches!(theirs, Frame::Hello { .. }), "{theirs:?}");
        write_frame(&mut client, &hello(chain_id, GENESIS, 0))
            .await
            .unwrap();
        (client, task)
    }

    async fn connect(
        s: &Arc<LookupServer<MemSource>>,
    ) -> (TcpStream, JoinHandle<eyre::Result<()>>) {
        connect_with(s, None, CHAIN_ID).await
    }

    async fn send(c: &mut TcpStream, frame: Vec<u8>) {
        c.write_all(&frame).await.unwrap();
    }

    async fn recv(c: &mut TcpStream) -> Vec<u8> {
        let mut buf = Vec::new();
        let got = tokio::time::timeout(
            Duration::from_secs(5),
            read_body(c, &mut buf, MAX_RESPONSE_BYTES),
        )
        .await
        .expect("no response within 5s")
        .unwrap();
        assert!(got, "eof");
        buf
    }

    fn storage_req(id: u32, pin: &Word, keys: &[(Addr, Word)]) -> Vec<u8> {
        let mut b = RequestBuilder::new(Op::Storage, 0, id, pin);
        for (a, s) in keys {
            b.push_storage(a, s);
        }
        b.finish()
    }

    fn account_req(id: u32, flags: u8, addrs: &[Addr]) -> Vec<u8> {
        let mut b = RequestBuilder::new(Op::Account, flags, id, &LATEST);
        for a in addrs {
            b.push_account(a);
        }
        b.finish()
    }

    async fn server_error(task: JoinHandle<eyre::Result<()>>) -> String {
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("connection task still running after 5s")
            .unwrap()
            .unwrap_err()
            .to_string()
    }

    #[tokio::test]
    async fn reads_every_op_with_d9_encodings() {
        let (s, _) = server(LookupConfig::default());
        let (mut c, _task) = connect(&s).await;

        send(
            &mut c,
            account_req(1, FLAG_INLINE_CODE, &[CONTRACT, EOA, MISSING, FAILING]),
        )
        .await;
        let body = recv(&mut c).await;
        let r = ResponseRef::decode(&body).unwrap();
        assert_eq!(
            (r.id, r.status, r.read_number, *r.read_hash),
            (1, Status::Ok, HEAD.0, HEAD.1)
        );
        let res: Vec<_> = r.results().collect();
        assert!(matches!(
            res[0],
            ResultRef::Account {
                status: ItemStatus::Ok,
                nonce: 1,
                code_hash: &CODE_HASH,
                code: Some(CODE),
                ..
            }
        ));
        // EOA: no code is KECCAK_EMPTY with empty inline code.
        assert!(matches!(
            res[1],
            ResultRef::Account {
                status: ItemStatus::Ok,
                nonce: 5,
                code_hash: &KECCAK_EMPTY,
                code: Some(&[]),
                ..
            }
        ));
        // Missing account: Absent, zeroed, KECCAK_EMPTY.
        let ResultRef::Account {
            status,
            nonce,
            balance,
            code_hash,
            code,
        } = res[2]
        else {
            panic!("{:?}", res[2]);
        };
        assert_eq!(
            (status, nonce, *balance, *code_hash, code),
            (ItemStatus::Absent, 0, [0; 32], KECCAK_EMPTY, Some(&[][..]))
        );
        // One failing key doesn't fail the frame.
        assert!(matches!(
            res[3],
            ResultRef::Account {
                status: ItemStatus::Error,
                ..
            }
        ));

        send(
            &mut c,
            storage_req(2, &LATEST, &[(EOA, [2; 32]), (EOA, [3; 32])]),
        )
        .await;
        let body = recv(&mut c).await;
        let r = ResponseRef::decode(&body).unwrap();
        let res: Vec<_> = r.results().collect();
        assert_eq!(
            res[0],
            ResultRef::Storage {
                status: ItemStatus::Ok,
                value: &[0x2A; 32]
            }
        );
        assert_eq!(
            res[1],
            ResultRef::Storage {
                status: ItemStatus::Ok,
                value: &[0; 32]
            },
            "missing slot = zero"
        );

        let mut b = RequestBuilder::new(Op::Code, 0, 3, &LATEST);
        b.push_code(&CODE_HASH)
            .push_code(&[0x77; 32])
            .push_code(&KECCAK_EMPTY);
        send(&mut c, b.finish()).await;
        let body = recv(&mut c).await;
        let res: Vec<_> = ResponseRef::decode(&body).unwrap().results().collect();
        assert_eq!(
            res[0],
            ResultRef::Code {
                status: ItemStatus::Ok,
                code: CODE
            }
        );
        assert_eq!(
            res[1],
            ResultRef::Code {
                status: ItemStatus::Absent,
                code: &[]
            }
        );
        assert_eq!(
            res[2],
            ResultRef::Code {
                status: ItemStatus::Ok,
                code: &[]
            }
        );
    }

    #[tokio::test]
    async fn pinning_ping_unknown_block_and_internal() {
        let (s, _) = server(LookupConfig::default());
        let (mut c, _task) = connect(&s).await;
        // Pinned one block back: values come from that block, and the
        // response names it.
        send(&mut c, storage_req(1, &PARENT.1, &[PER_BLOCK])).await;
        let body = recv(&mut c).await;
        let r = ResponseRef::decode(&body).unwrap();
        assert_eq!(
            (r.status, r.read_number, *r.read_hash),
            (Status::Ok, PARENT.0, PARENT.1)
        );
        assert_eq!(
            r.results().next(),
            Some(ResultRef::Storage {
                status: ItemStatus::Ok,
                value: &[PARENT.0 as u8; 32]
            })
        );
        // count = 0 is a ping: resolves the pin.
        send(&mut c, storage_req(2, &LATEST, &[])).await;
        let body = recv(&mut c).await;
        let r = ResponseRef::decode(&body).unwrap();
        assert_eq!(
            (r.status, r.count(), r.read_number, *r.read_hash),
            (Status::Ok, 0, HEAD.0, HEAD.1)
        );
        // Unknown pin: explicit status, nothing stale.
        send(&mut c, storage_req(3, &[0x33; 32], &[PER_BLOCK])).await;
        let body = recv(&mut c).await;
        let r = ResponseRef::decode(&body).unwrap();
        assert_eq!(
            (r.id, r.status, r.count(), r.read_number, *r.read_hash),
            (3, Status::UnknownBlock, 0, 0, [0; 32])
        );
        send(&mut c, storage_req(4, &BROKEN_PIN, &[PER_BLOCK])).await;
        let r = recv(&mut c).await;
        assert_eq!(ResponseRef::decode(&r).unwrap().status, Status::Internal);
    }

    #[tokio::test]
    async fn bad_request_echoes_id_and_untrusted_length_closes() {
        let (s, _) = server(LookupConfig::default());
        let (mut c, task) = connect(&s).await;
        let mut frame = storage_req(41, &LATEST, &[PER_BLOCK]);
        frame[4] = 9; // op
        send(&mut c, frame).await;
        let body = recv(&mut c).await;
        let r = ResponseRef::decode(&body).unwrap();
        assert_eq!((r.op, r.id, r.status), (9, 41, Status::BadRequest));
        // Still usable afterwards.
        send(&mut c, storage_req(42, &LATEST, &[PER_BLOCK])).await;
        assert_eq!(
            ResponseRef::decode(&recv(&mut c).await).unwrap().status,
            Status::Ok
        );
        // A length below the shared prefix can't be answered: close.
        send(&mut c, vec![4, 0, 0, 0, 1, 2, 3, 4]).await;
        let err = server_error(task).await;
        assert!(err.contains("out of bounds"), "{err}");
    }

    #[tokio::test]
    async fn pipelined_answers_return_out_of_order() {
        let (s, data) = server(LookupConfig::default());
        let (mut c, _task) = connect(&s).await;
        send(&mut c, account_req(1, 0, &[SLOW])).await;
        send(&mut c, storage_req(2, &LATEST, &[PER_BLOCK])).await;
        send(&mut c, storage_req(3, &LATEST, &[PER_BLOCK])).await;
        let first = recv(&mut c).await;
        let second = recv(&mut c).await;
        let ids = [
            ResponseRef::decode(&first).unwrap().id,
            ResponseRef::decode(&second).unwrap().id,
        ];
        assert!(
            !ids.contains(&1),
            "the slow request answered first: {ids:?}"
        );
        data.gate.release();
        let last = recv(&mut c).await;
        let r = ResponseRef::decode(&last).unwrap();
        assert_eq!((r.id, r.status), (1, Status::Ok));
    }

    #[tokio::test]
    async fn busy_beyond_the_inflight_cap() {
        let (s, data) = server(LookupConfig {
            workers: 1,
            queue: MAX_INFLIGHT,
            ..Default::default()
        });
        let (mut c, _task) = connect(&s).await;
        send(&mut c, account_req(0, 0, &[SLOW])).await; // occupies the worker
        for id in 1..MAX_INFLIGHT as u32 {
            send(&mut c, storage_req(id, &LATEST, &[PER_BLOCK])).await;
        }
        send(&mut c, storage_req(9999, &LATEST, &[PER_BLOCK])).await;
        // Answered at once, without a read, while the others wait.
        let body = recv(&mut c).await;
        let r = ResponseRef::decode(&body).unwrap();
        assert_eq!((r.id, r.status, r.count()), (9999, Status::Busy, 0));
        data.gate.release();
        let mut ok = 0;
        for _ in 0..MAX_INFLIGHT {
            let body = recv(&mut c).await;
            assert_eq!(ResponseRef::decode(&body).unwrap().status, Status::Ok);
            ok += 1;
        }
        assert_eq!(ok, MAX_INFLIGHT);
    }

    /// Regression: the in-flight slot used to be released only after the
    /// answer was flushed, so a client that refilled its window the moment
    /// an answer arrived could find all slots still taken and get `Busy`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn client_at_the_inflight_cap_never_sees_busy() {
        let (s, _) = server(LookupConfig::default());
        let (c, _task) = connect(&s).await;
        let (mut rd, mut wr) = c.into_split();
        let total = 5000u32;
        // Keep exactly MAX_INFLIGHT outstanding: one new request per answer.
        let mut sent = 0u32;
        for _ in 0..MAX_INFLIGHT {
            wr.write_all(&storage_req(sent, &LATEST, &[PER_BLOCK]))
                .await
                .unwrap();
            sent += 1;
        }
        let mut busy = 0;
        let mut buf = Vec::new();
        for _ in 0..total {
            assert!(
                read_body(&mut rd, &mut buf, MAX_RESPONSE_BYTES)
                    .await
                    .unwrap()
            );
            if ResponseRef::decode(&buf).unwrap().status == Status::Busy {
                busy += 1;
            }
            if sent < total {
                wr.write_all(&storage_req(sent, &LATEST, &[PER_BLOCK]))
                    .await
                    .unwrap();
                sent += 1;
            }
        }
        assert_eq!(busy, 0);
    }

    #[tokio::test]
    async fn busy_when_the_server_queue_is_full() {
        let (s, data) = server(LookupConfig {
            workers: 1,
            queue: 1,
            ..Default::default()
        });
        let (mut c, _task) = connect(&s).await;
        send(&mut c, account_req(1, 0, &[SLOW])).await;
        while data.gate.entered.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        send(&mut c, storage_req(2, &LATEST, &[PER_BLOCK])).await; // queued
        send(&mut c, storage_req(3, &LATEST, &[PER_BLOCK])).await; // no room
        let r = recv(&mut c).await;
        assert_eq!(
            (
                ResponseRef::decode(&r).unwrap().id,
                ResponseRef::decode(&r).unwrap().status
            ),
            (3, Status::Busy)
        );
        data.gate.release();
        let mut ids: Vec<u32> = Vec::new();
        for _ in 0..2 {
            ids.push(ResponseRef::decode(&recv(&mut c).await).unwrap().id);
        }
        ids.sort();
        assert_eq!(ids, [1, 2]);
    }

    #[tokio::test]
    async fn panicking_read_answers_internal_and_pool_survives() {
        let (s, _) = server(LookupConfig {
            workers: 1,
            ..Default::default()
        });
        let (mut c, _task) = connect(&s).await;
        send(&mut c, account_req(1, 0, &[PANICS])).await;
        let r = recv(&mut c).await;
        assert_eq!(ResponseRef::decode(&r).unwrap().status, Status::Internal);
        send(&mut c, account_req(2, 0, &[EOA])).await;
        let r = recv(&mut c).await;
        assert_eq!(ResponseRef::decode(&r).unwrap().status, Status::Ok);
    }

    fn proof_req(id: u32, pin: &Word, targets: &[(Word, Vec<Word>)]) -> Vec<u8> {
        let mut b = RequestBuilder::new(Op::Proof, 0, id, pin);
        for (a, slots) in targets {
            b.push_proof_target(a, slots);
        }
        b.finish()
    }

    /// (status, read_number, account nodes as (nibbles, rlp), storage tries
    /// as (hashed address, root, node count)).
    type ProofSummary = (
        Status,
        u64,
        Vec<(Vec<u8>, Vec<u8>)>,
        Vec<(Word, Word, usize)>,
    );

    fn summarize(body: &[u8]) -> ProofSummary {
        let r = ResponseRef::decode(body).unwrap();
        let Some(p) = r.proof() else {
            return (r.status, r.read_number, vec![], vec![]);
        };
        let nodes = p
            .account_nodes()
            .map(|n| (n.nibbles().collect(), n.rlp.to_vec()))
            .collect();
        let storages = p
            .storages()
            .map(|st| (*st.hashed_address, *st.storage_root, st.nodes.count()))
            .collect();
        (r.status, r.read_number, nodes, storages)
    }

    #[tokio::test]
    async fn proofs_answer_in_order_with_pins_pings_and_depth_bound() {
        let (s, _) = server(LookupConfig::default());
        let (mut c, _task) = connect(&s).await;
        let targets = vec![([0x56; 32], vec![]), ([0x12; 32], vec![[0x34; 32]])];
        send(&mut c, proof_req(1, &LATEST, &targets)).await;
        let (status, number, nodes, storages) = summarize(&recv(&mut c).await);
        assert_eq!((status, number), (Status::Ok, HEAD.0));
        assert_eq!(
            nodes,
            [
                (vec![], vec![0xF8, HEAD.0 as u8]),
                (vec![1, 2], vec![0x12]),
                (vec![5, 6], vec![0x56]),
            ]
        );
        assert_eq!(storages, [([0x12; 32], [0x5A; 32], 1)]);

        // One block back is within the default depth (32).
        send(&mut c, proof_req(2, &PARENT.1, &targets)).await;
        let (status, number, nodes, _) = summarize(&recv(&mut c).await);
        assert_eq!((status, number), (Status::Ok, PARENT.0));
        assert_eq!(nodes[0].1, [0xF8, PARENT.0 as u8], "read at the pin");

        // count = 0: a ping, no walk.
        send(&mut c, proof_req(3, &LATEST, &[])).await;
        assert_eq!(
            summarize(&recv(&mut c).await),
            (Status::Ok, HEAD.0, vec![], vec![])
        );

        // Unknown pins, failing walks.
        send(&mut c, proof_req(4, &[0x33; 32], &targets)).await;
        assert_eq!(summarize(&recv(&mut c).await).0, Status::UnknownBlock);
        send(&mut c, proof_req(5, &BROKEN_PIN, &targets)).await;
        assert_eq!(summarize(&recv(&mut c).await).0, Status::Internal);
        send(&mut c, proof_req(6, &LATEST, &[(PROOF_FAILS, vec![])])).await;
        assert_eq!(summarize(&recv(&mut c).await).0, Status::Internal);
        send(&mut c, proof_req(7, &LATEST, &[(PROOF_HUGE, vec![])])).await;
        assert_eq!(summarize(&recv(&mut c).await).0, Status::TooLarge);

        // Depth 0: only the head itself is provable; lookups are unaffected.
        let (s, _) = server(LookupConfig {
            proof_max_depth: 0,
            ..Default::default()
        });
        let (mut c, _task) = connect(&s).await;
        send(&mut c, proof_req(1, &PARENT.1, &targets)).await;
        assert_eq!(summarize(&recv(&mut c).await).0, Status::UnknownBlock);
        send(&mut c, proof_req(2, &HEAD.1, &targets)).await;
        assert_eq!(summarize(&recv(&mut c).await).0, Status::Ok);
        send(&mut c, storage_req(3, &PARENT.1, &[PER_BLOCK])).await;
        let body = recv(&mut c).await;
        assert_eq!(ResponseRef::decode(&body).unwrap().status, Status::Ok);
    }

    #[tokio::test]
    async fn slow_proofs_never_delay_lookups_and_their_queue_answers_busy() {
        let (s, data) = server(LookupConfig {
            workers: 1,
            proof_workers: 1,
            proof_queue: 1,
            ..Default::default()
        });
        let (mut c, _task) = connect(&s).await;
        let slow = vec![(PROOF_SLOW, vec![])];
        send(&mut c, proof_req(1, &LATEST, &slow)).await;
        let deadline = Instant::now() + Duration::from_secs(5);
        while data.gate.entered.load(Ordering::SeqCst) < 1 {
            assert!(Instant::now() < deadline, "proof never started");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        // The only proof thread is stuck: lookups still flow.
        for id in 10..20 {
            send(&mut c, storage_req(id, &LATEST, &[PER_BLOCK])).await;
            let r = recv(&mut c).await;
            let r = ResponseRef::decode(&r).unwrap();
            assert_eq!((r.id, r.status), (id, Status::Ok));
        }
        // One proof fits the queue; the next is Busy at once.
        send(&mut c, proof_req(2, &LATEST, &[([1; 32], vec![])])).await;
        send(&mut c, proof_req(3, &LATEST, &[([1; 32], vec![])])).await;
        let r = recv(&mut c).await;
        let r = ResponseRef::decode(&r).unwrap();
        assert_eq!((r.id, r.status), (3, Status::Busy));
        data.gate.release();
        let mut done: Vec<_> = Vec::new();
        for _ in 0..2 {
            let r = recv(&mut c).await;
            let r = ResponseRef::decode(&r).unwrap();
            done.push((r.id, r.status));
        }
        done.sort_by_key(|d| d.0);
        assert_eq!(done, [(1, Status::Ok), (2, Status::Ok)]);
    }

    #[tokio::test]
    async fn oversized_response_answers_too_large() {
        let (s, _) = server(LookupConfig::default());
        let (mut c, _task) = connect(&s).await;
        let mut b = RequestBuilder::new(Op::Code, 0, 7, &LATEST);
        for _ in 0..64 {
            b.push_code(&HUGE_HASH); // 64 × 300 KiB > 16 MiB
        }
        send(&mut c, b.finish()).await;
        let r = recv(&mut c).await;
        assert_eq!(ResponseRef::decode(&r).unwrap().status, Status::TooLarge);
    }

    fn header_req(id: u32, pin: &Word, items: &[(u64, Word)]) -> Vec<u8> {
        let mut b = RequestBuilder::new(Op::Header, 0, id, pin);
        for (number, hash) in items {
            b.push_header(*number, hash);
        }
        b.finish()
    }

    fn header_results(body: &[u8]) -> Vec<(ItemStatus, Vec<u8>)> {
        let r = ResponseRef::decode(body).unwrap();
        assert_eq!(
            (r.status, r.read_number, *r.read_hash),
            (Status::Ok, HEAD.0, [0; 32])
        );
        r.results()
            .map(|res| match res {
                ResultRef::Header { status, rlp } => (status, rlp.to_vec()),
                other => panic!("{other:?}"),
            })
            .collect()
    }

    #[tokio::test]
    async fn headers_by_number_hash_and_finalized_ignore_the_pin() {
        let (s, data) = server(LookupConfig::default());
        let (mut c, _task) = connect(&s).await;
        let items = [
            (HEAD.0, [0; 32]),         // canonical by number
            (12345, PARENT.1),         // by hash: number not checked
            (FINALIZED, [0; 32]),      // finalized = PARENT
            (5000, [0; 32]),           // no such number
            (HEAD.0, [0x77; 32]),      // no such hash
            (FAILING_HEADER, [0; 32]), // read fails: this item only
        ];
        let want = [
            (ItemStatus::Ok, header_rlp(HEAD.0)),
            (ItemStatus::Ok, header_rlp(PARENT.0)),
            (ItemStatus::Ok, header_rlp(PARENT.0)),
            (ItemStatus::Absent, vec![]),
            (ItemStatus::Absent, vec![]),
            (ItemStatus::Error, vec![]),
        ];
        // The pin is ignored: latest, an unknown block and a pin whose
        // open fails all read the same headers.
        for (id, pin) in [(1, LATEST), (2, [0x33; 32]), (3, BROKEN_PIN)] {
            send(&mut c, header_req(id, &pin, &items)).await;
            let body = recv(&mut c).await;
            assert_eq!(ResponseRef::decode(&body).unwrap().id, id);
            assert_eq!(header_results(&body), want, "pin {pin:?}");
        }
        // No finalized block yet: Absent.
        *data.finalized.lock().unwrap() = None;
        send(&mut c, header_req(4, &LATEST, &[(FINALIZED, [0; 32])])).await;
        assert_eq!(
            header_results(&recv(&mut c).await),
            [(ItemStatus::Absent, vec![])]
        );
    }

    #[tokio::test]
    async fn transactions_by_block_hash_with_senders_ignore_the_pin() {
        let (s, _) = server(LookupConfig::default());
        let (mut c, _task) = connect(&s).await;
        for (id, pin) in [(1, LATEST), (2, [0x33; 32])] {
            let mut b = RequestBuilder::new(Op::Transactions, 0, id, &pin);
            b.push_transactions(&HEAD.1)
                .push_transactions(&PARENT.1)
                .push_transactions(&[0x77; 32])
                .push_transactions(&FAILING_BLOCK);
            send(&mut c, b.finish()).await;
            let body = recv(&mut c).await;
            let r = ResponseRef::decode(&body).unwrap();
            assert_eq!(
                (r.id, r.status, r.read_number, *r.read_hash),
                (id, Status::Ok, HEAD.0, [0; 32])
            );
            let got: Vec<_> = r
                .results()
                .map(|res| match res {
                    ResultRef::Transactions { status, txs } => (
                        status,
                        txs.map(|(a, tx)| (*a, tx.to_vec())).collect::<Vec<_>>(),
                    ),
                    other => panic!("{other:?}"),
                })
                .collect();
            assert_eq!(
                got,
                [
                    (
                        ItemStatus::Ok,
                        vec![
                            (TX_SENDERS[0], vec![0x02, 0xF8, 0x01]),
                            (TX_SENDERS[1], vec![0xF8, 0x02]),
                        ]
                    ),
                    (ItemStatus::Ok, vec![]), // a block without transactions
                    (ItemStatus::Absent, vec![]), // no such block
                    (ItemStatus::Error, vec![]), // read fails: this item only
                ]
            );
        }
    }

    #[tokio::test]
    async fn handshake_refuses_wrong_chain() {
        let (s, _) = server(short());
        let (mut c, srv) = pair(None).await;
        let task = tokio::spawn(s.clone().handle(srv));
        read_frame(&mut c).await.unwrap().unwrap();
        write_frame(&mut c, &hello(2, GENESIS, 0)).await.unwrap();
        let refused = read_frame(&mut c).await.unwrap().unwrap();
        assert!(matches!(refused, Frame::Refused { .. }), "{refused:?}");
        let err = server_error(task).await;
        assert!(err.contains("chain id mismatch"), "{err}");
    }

    #[tokio::test]
    async fn idle_torn_and_undrained_connections_are_dropped() {
        let (s, _) = server(short());
        // Silent after the handshake.
        let (_c, task) = connect_with(&s, None, CHAIN_ID).await;
        assert!(server_error(task).await.contains("idle"));
        // A length, then half a body.
        let (mut c, task) = connect_with(&s, None, CHAIN_ID).await;
        let frame = storage_req(1, &LATEST, &[PER_BLOCK]);
        send(&mut c, frame[..10].to_vec()).await;
        assert!(server_error(task).await.contains("frame body"));
        // Never reads its answers: the write deadline drops it (idle
        // deadline pushed out so only the writer can fire).
        let (s, _) = server(LookupConfig {
            idle_timeout: Duration::from_secs(30),
            ..short()
        });
        let (mut c, task) = connect_with(&s, Some(4096), CHAIN_ID).await;
        for id in 0..48 {
            // 48 × 300 KiB answers: far past loopback socket buffers.
            let mut b = RequestBuilder::new(Op::Code, 0, id, &LATEST);
            b.push_code(&HUGE_HASH);
            send(&mut c, b.finish()).await;
        }
        let err = server_error(task).await;
        assert!(err.contains("timed out"), "{err}");
        drop(c);
    }
}

/// v2 proofs (op 4) against v1 on a real reth provider: tries in MDBX plus
/// blocks executed but not persisted, served through `BlockchainProvider`
/// exactly as the node serves them.
#[cfg(test)]
mod proof_tests {
    use super::*;
    use alloy_primitives::U256;
    use reth_chain_state::{ExecutedBlock, NewCanonicalChain};
    use reth_ethereum::Block;
    use reth_ethereum::chainspec::MAINNET;
    use reth_ethereum::node::EthereumNode;
    use reth_ethereum::node::api::NodeTypesWithDBAdapter;
    use reth_ethereum::primitives::{Account, Header, RecoveredBlock, SealedHeader};
    use reth_ethereum::provider::db::mdbx::DatabaseArguments;
    use reth_ethereum::provider::db::{ClientVersion, DatabaseEnv, init_db};
    use reth_ethereum::provider::providers::{
        BlockchainProvider, ProviderFactory, RocksDBBuilder, StaticFileProvider,
    };
    use reth_ethereum::provider::{
        BlockWriter, StageCheckpointWriter, StateRootProvider, StateWriter, TrieWriter,
    };
    use reth_ethereum::tasks::Runtime;
    use reth_ethereum::trie::{HashedPostState, MultiProofTargets, SortedTrieData};
    use reth_stages_types::{StageCheckpoint, StageId};
    use std::path::PathBuf;

    type Node = NodeTypesWithDBAdapter<EthereumNode, DatabaseEnv>;

    /// One proof target: a hashed address and its hashed slots.
    type Target = (B256, Vec<B256>);

    /// A temp datadir, removed on drop (after the providers using it).
    struct TempDir(PathBuf);

    impl TempDir {
        fn new(tag: &str) -> Self {
            static N: AtomicU64 = AtomicU64::new(0);
            let n = N.fetch_add(1, Ordering::Relaxed);
            let dir = std::env::temp_dir().join(format!("silt-{tag}-{}-{n}", std::process::id()));
            std::fs::create_dir_all(dir.join("static_files")).unwrap();
            Self(dir)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// Deterministic xorshift64*.
    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 >> 12;
            self.0 ^= self.0 << 25;
            self.0 ^= self.0 >> 27;
            self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
        }
        fn below(&mut self, n: usize) -> usize {
            (self.next() % n as u64) as usize
        }
        fn word(&mut self) -> B256 {
            keccak256(self.next().to_be_bytes())
        }
    }

    fn key(seed: u64) -> B256 {
        keccak256(seed.to_be_bytes())
    }

    /// `prefix` bytes of `base`, then the rest of `tail`.
    fn sharing(base: B256, prefix: usize, tail: B256) -> B256 {
        let mut k = tail;
        k.0[..prefix].copy_from_slice(&base.0[..prefix]);
        k
    }

    fn account(nonce: u64, balance: u64, contract: bool) -> Account {
        Account {
            nonce,
            balance: U256::from(balance),
            bytecode_hash: contract.then(|| key(nonce ^ 0xc0de)),
        }
    }

    /// Hashed keys picked for trie shapes v1 and v2 encode differently.
    struct Shapes {
        /// Two accounts sharing 9 bytes: an extension + branch deep in the
        /// account trie.
        ext_pair: [B256; 2],
        /// Storage with two slots sharing 31 bytes (leaves and their
        /// branch short enough to be inlined) plus 20 others.
        inline: B256,
        inline_slots: [B256; 2],
        /// Storage of exactly two slots sharing 5 bytes: the root is an
        /// extension over a branch.
        root_ext: B256,
        root_ext_slots: [B256; 2],
        /// Storage of exactly one slot: the root is a leaf.
        root_leaf: B256,
        /// A contract destroyed in memory, one recreated with fresh
        /// storage, one created in memory, an EOA deleted in memory.
        destroyed: B256,
        recreated: B256,
        created: B256,
        deleted: B256,
        /// Never written anywhere; a slot never written anywhere.
        missing: B256,
        missing_slot: B256,
    }

    struct Chain {
        provider: BlockchainProvider<Node>,
        /// Block hashes by number: 0 persisted, the rest in memory.
        hashes: Vec<B256>,
        roots: Vec<B256>,
        /// Every account ever written, with every slot ever written.
        accounts: Vec<Target>,
        shapes: Shapes,
        _dir: TempDir,
    }

    /// Block 0 persisted (hashed state + tries in MDBX, Finish at 0), then
    /// `in_memory` executed blocks with exact trie updates, committed to
    /// the canonical in-memory state the way the engine does.
    fn chain(in_memory: u64, db_accounts: u64, per_block: usize) -> Chain {
        let dir = TempDir::new("proof");
        let db = init_db(
            dir.0.join("db"),
            DatabaseArguments::new(ClientVersion::default()),
        )
        .unwrap();
        let static_files = StaticFileProvider::read_write(dir.0.join("static_files")).unwrap();
        let rocksdb = RocksDBBuilder::new(dir.0.join("rocksdb"))
            .with_default_tables()
            .build()
            .unwrap();
        let factory = ProviderFactory::<Node>::new(
            db,
            MAINNET.clone(),
            static_files,
            rocksdb,
            Runtime::test(),
        )
        .unwrap();

        let mut rng = Rng(0x5eed);
        let base = key(1_000_000);
        let shapes = Shapes {
            ext_pair: [base, sharing(base, 9, key(1_000_001))],
            inline: key(1_000_002),
            inline_slots: {
                let s = key(1_000_003);
                let mut t = s;
                t.0[31] ^= 0x01;
                [s, t]
            },
            root_ext: key(1_000_004),
            root_ext_slots: [key(1_000_005), sharing(key(1_000_005), 5, key(1_000_006))],
            root_leaf: key(1_000_007),
            destroyed: key(3),
            recreated: key(5),
            created: key(2_000_000),
            deleted: key(2),
            missing: key(3_000_000),
            missing_slot: key(3_000_001),
        };
        let mut accounts: BTreeMap<B256, Vec<B256>> = BTreeMap::new();

        // Block 0: plain accounts, every 16th a contract with 30 slots.
        let mut state = HashedPostState::default();
        let mut set =
            |state: &mut HashedPostState, a: B256, acct: Option<Account>, slots: &[(B256, u64)]| {
                state.accounts.insert(a, acct);
                let written = accounts.entry(a).or_default();
                // reth 2.11's hashed state has no "wiped" flag: destroying an
                // account (or recreating `shapes.recreated` with one new slot)
                // zeroes every slot it ever had.
                let wipe: Vec<_> = if acct.is_none() || a == shapes.recreated && slots.len() == 1 {
                    written.iter().map(|s| (*s, 0)).collect()
                } else {
                    vec![]
                };
                if !slots.is_empty() || !wipe.is_empty() {
                    let storage = state.storages.entry(a).or_default();
                    for (slot, value) in wipe.iter().chain(slots) {
                        storage.storage.insert(*slot, U256::from(*value));
                        written.push(*slot);
                    }
                }
            };
        for i in 0..db_accounts {
            let contract = i % 16 == 3 || i == 5;
            let slots: Vec<_> = if contract {
                (0..30).map(|j| (key(i << 32 | j), 1 + j)).collect()
            } else {
                vec![]
            };
            set(
                &mut state,
                key(i),
                Some(account(i, 1 + i, contract)),
                &slots,
            );
        }
        for a in shapes.ext_pair {
            set(&mut state, a, Some(account(7, 7, false)), &[]);
        }
        let mut inline_slots: Vec<_> = (0..20).map(|j| (key(4_000_000 + j), 1 + j)).collect();
        // Value 1: each leaf is 3 bytes, their branch about 23.
        inline_slots.extend(shapes.inline_slots.map(|s| (s, 1)));
        set(
            &mut state,
            shapes.inline,
            Some(account(1, 1, true)),
            &inline_slots,
        );
        let root_ext_slots = shapes.root_ext_slots.map(|s| (s, 0xffff));
        set(
            &mut state,
            shapes.root_ext,
            Some(account(1, 1, true)),
            &root_ext_slots,
        );
        set(
            &mut state,
            shapes.root_leaf,
            Some(account(1, 1, true)),
            &[(key(5_000_000), 9)],
        );

        let rw = factory.provider_rw().unwrap();
        rw.write_hashed_state(&state.into_sorted()).unwrap();
        rw.commit().unwrap();
        // No trie nodes yet: the walk hashes every leaf and returns every
        // branch node as an update.
        let (root, updates) = factory
            .latest()
            .unwrap()
            .state_root_with_updates(HashedPostState::default())
            .unwrap();
        let genesis = Block {
            header: Header {
                number: 0,
                state_root: root,
                ..Default::default()
            },
            ..Default::default()
        };
        let genesis = RecoveredBlock::new_unhashed(genesis, vec![]);
        let rw = factory.provider_rw().unwrap();
        rw.insert_block(&genesis).unwrap();
        rw.write_trie_updates(updates).unwrap();
        rw.save_stage_checkpoint(StageId::Finish, StageCheckpoint::new(0))
            .unwrap();
        rw.commit().unwrap();

        let provider = BlockchainProvider::new(factory).unwrap();
        let in_mem = provider.canonical_in_memory_state();
        let mut hashes = vec![genesis.hash()];
        let mut roots = vec![root];
        let contracts: Vec<u64> = (4..db_accounts).filter(|i| i % 16 == 3).collect();
        for number in 1..=in_memory {
            let mut state = HashedPostState::default();
            for _ in 0..per_block {
                let i = rng.below(db_accounts as usize) as u64;
                // Keys 2, 3, 5 are the deleted/destroyed/recreated shapes.
                if matches!(i, 2 | 3 | 5) {
                    continue;
                }
                let contract = i % 16 == 3 || i == 5;
                set(
                    &mut state,
                    key(i),
                    Some(account(i + number, rng.next() >> 8, contract)),
                    &[],
                );
            }
            for j in 0..per_block / 10 {
                set(
                    &mut state,
                    key(10_000_000 + number * 1000 + j as u64),
                    Some(account(0, 1, false)),
                    &[],
                );
            }
            for _ in 0..per_block / 10 {
                let i = contracts[rng.below(contracts.len())];
                let mut slots: Vec<_> = (0..5).map(|_| (rng.word(), rng.next() >> 1)).collect();
                // Rewrite two existing slots, zero (delete) one.
                slots.push((key(i << 32 | rng.below(30) as u64), number));
                slots.push((key(i << 32 | rng.below(30) as u64), 0));
                set(
                    &mut state,
                    key(i),
                    Some(account(i + number, 1, true)),
                    &slots,
                );
            }
            match number {
                2 => {
                    set(&mut state, shapes.destroyed, None, &[]);
                }
                3 => {
                    let slots: Vec<_> = (0..12).map(|j| (key(6_000_000 + j), 1 + j)).collect();
                    set(
                        &mut state,
                        shapes.created,
                        Some(account(1, 1, true)),
                        &slots,
                    );
                    set(&mut state, shapes.deleted, None, &[]);
                }
                4 => {
                    // Wiped, then written: only the new slot remains.
                    let acct = Some(account(0, 1, true));
                    set(&mut state, shapes.recreated, acct, &[(key(7_000_000), 3)]);
                }
                5 => {
                    // A slot in the inline region changes value.
                    set(
                        &mut state,
                        shapes.inline,
                        Some(account(2, 1, true)),
                        &[(shapes.inline_slots[0], 2)],
                    );
                }
                _ => {}
            }
            let parent = *hashes.last().unwrap();
            let (root, updates) = provider
                .state_by_block_hash(parent)
                .unwrap()
                .state_root_with_updates(state.clone())
                .unwrap();
            let block = Block {
                header: Header {
                    number,
                    parent_hash: parent,
                    state_root: root,
                    ..Default::default()
                },
                ..Default::default()
            };
            let block = RecoveredBlock::new_unhashed(block, vec![]);
            hashes.push(block.hash());
            roots.push(root);
            let header = SealedHeader::new(block.header().clone(), block.hash());
            let executed = ExecutedBlock::new(
                Arc::new(block),
                ExecutedBlock::<reth_ethereum::EthPrimitives>::default().execution_output,
                SortedTrieData::new(
                    Arc::new(state.into_sorted()),
                    Arc::new(updates.into_sorted()),
                ),
            );
            in_mem.update_chain(NewCanonicalChain::Commit {
                new: vec![executed],
            });
            in_mem.set_canonical_head(header);
        }
        let accounts = accounts.into_iter().collect();
        Chain {
            provider,
            hashes,
            roots,
            accounts,
            shapes,
            _dir: dir,
        }
    }

    fn view(chain: &Chain, number: usize) -> RethView {
        RethView(
            chain
                .provider
                .state_by_block_hash(chain.hashes[number])
                .unwrap(),
        )
    }

    fn v1(view: &RethView, targets: &[Target]) -> (MultiProof, Vec<u8>) {
        let mut wanted = MultiProofTargets::default();
        for (a, slots) in targets {
            wanted.entry(*a).or_default().extend(slots.iter().copied());
        }
        let mp = view.0.multiproof(Default::default(), wanted).unwrap();
        let mut out = ProofBuilder::new(0, 1, 0, &[0; 32], 0);
        write_multiproof(&mp, &mut out);
        (mp, out.finish(0))
    }

    fn v2(view: &RethView, targets: &[Target]) -> Vec<u8> {
        let targets = ProofTargets::new(targets.iter().cloned());
        let mut out = ProofBuilder::new(0, 1, 0, &[0; 32], 0);
        view.prove(&targets, &mut out).unwrap();
        out.finish(0)
    }

    /// Target sets covering every case the two writers could disagree on.
    fn target_sets(chain: &Chain) -> Vec<(&'static str, Vec<Target>)> {
        let s = &chain.shapes;
        let all = &chain.accounts;
        let mut rng = Rng(0xfeed);
        let with_slots = |a: B256| -> Vec<B256> {
            all.iter()
                .find(|(k, _)| *k == a)
                .map(|(_, slots)| slots.clone())
                .unwrap_or_default()
        };
        let mut mixed: Vec<Target> = (0..40)
            .map(|_| {
                let (a, slots) = &all[rng.below(all.len())];
                let take = rng.below(4);
                (*a, slots.iter().take(take).copied().collect())
            })
            .collect();
        mixed.push((s.missing, vec![s.missing_slot]));
        mixed.push((s.ext_pair[0], vec![]));
        let mut inline_slots = s.inline_slots.to_vec();
        // Absent, but inside the inline branch / beside the root extension.
        let mut near = s.inline_slots[0];
        near.0[31] ^= 0x02;
        inline_slots.push(near);
        let mut beside = s.root_ext_slots[0];
        beside.0[3] ^= 0x10;
        vec![
            ("empty request", vec![]),
            ("one account, no slots", vec![(all[0].0, vec![])]),
            (
                "account trie extension pair",
                s.ext_pair.iter().map(|a| (*a, vec![])).collect(),
            ),
            ("one of the extension pair", vec![(s.ext_pair[1], vec![])]),
            (
                "beside the extension pair",
                vec![(sharing(s.ext_pair[0], 6, key(9)), vec![])],
            ),
            (
                "missing account, missing slot",
                vec![(s.missing, vec![s.missing_slot])],
            ),
            (
                "existing account, missing slot",
                vec![(all[1].0, vec![s.missing_slot])],
            ),
            ("contract, no slots", vec![(key(3 + 16), vec![])]),
            (
                "contract, all slots",
                vec![(key(3 + 16), with_slots(key(3 + 16)))],
            ),
            ("inline nodes", vec![(s.inline, inline_slots)]),
            ("inline storage, no slots", vec![(s.inline, vec![])]),
            (
                "storage root extension, no slots",
                vec![(s.root_ext, vec![])],
            ),
            (
                "storage root extension, one slot",
                vec![(s.root_ext, vec![s.root_ext_slots[1]])],
            ),
            (
                "beside the storage root extension",
                vec![(s.root_ext, vec![beside])],
            ),
            (
                "storage root leaf",
                vec![(s.root_leaf, vec![key(5_000_000), s.missing_slot])],
            ),
            (
                "destroyed in memory",
                vec![(s.destroyed, with_slots(s.destroyed))],
            ),
            (
                "recreated in memory",
                vec![(s.recreated, with_slots(s.recreated))],
            ),
            (
                "created in memory",
                vec![(s.created, with_slots(s.created))],
            ),
            ("deleted in memory", vec![(s.deleted, vec![s.missing_slot])]),
            (
                "repeats",
                vec![
                    (s.inline, vec![s.inline_slots[0]]),
                    (s.inline, vec![s.inline_slots[0], s.inline_slots[1]]),
                    (s.missing, vec![]),
                    (s.missing, vec![]),
                ],
            ),
            ("mixed", mixed),
        ]
    }

    /// v2 node shapes the writer re-encodes, counted so the test is known
    /// to cover each.
    #[derive(Debug, Default)]
    struct Seen {
        /// Extension + branch merged into one v2 node.
        merged: usize,
        /// Nodes under 32 bytes of RLP (inlined in their parent).
        inline: usize,
        /// Storage tries whose root is an extension.
        root_extension: usize,
        /// Empty storage tries.
        empty_storage: usize,
    }

    impl Seen {
        fn count(&mut self, view: &RethView, targets: &[Target]) {
            let targets = ProofTargets::new(targets.iter().cloned());
            let mp = view.0.multiproof_v2(targets.request(), false).unwrap();
            let storage = mp.storage_proofs.values().flatten();
            for n in mp.account_proofs.iter().chain(storage) {
                match &n.node {
                    TrieNodeV2::Branch(b) if !b.key.is_empty() => self.merged += 1,
                    _ => {}
                }
                if !n.path.is_empty() && alloy_rlp::encode(&n.node).len() < 32 {
                    self.inline += 1;
                }
            }
            for nodes in mp.storage_proofs.values() {
                match &nodes.iter().find(|n| n.path.is_empty()).unwrap().node {
                    TrieNodeV2::Branch(b) if !b.key.is_empty() => self.root_extension += 1,
                    TrieNodeV2::EmptyRoot => self.empty_storage += 1,
                    _ => {}
                }
            }
        }
    }

    #[test]
    fn v2_proofs_are_byte_identical_to_v1_over_unpersisted_blocks() {
        let chain = chain(6, 600, 60);
        let mut seen = Seen::default();
        for number in [0, 3, 6] {
            let view = view(&chain, number);
            for (name, targets) in target_sets(&chain) {
                let (mp, want) = v1(&view, &targets);
                // The view reads block `number`, in-memory blocks included.
                let root = mp.account_subtree.get(&Nibbles::default()).unwrap();
                assert_eq!(
                    keccak256(root),
                    chain.roots[number],
                    "block {number}, {name}"
                );
                let got = v2(&view, &targets);
                assert!(
                    want == got,
                    "block {number}, {name}: v2 answer differs from v1"
                );
                seen.count(&view, &targets);
            }
        }
        assert!(
            seen.merged > 0 && seen.inline > 0 && seen.root_extension > 0 && seen.empty_storage > 0,
            "{seen:?}"
        );
    }

    /// Not an assertion on time: per-call proof time, v1 vs v2, with 15
    /// blocks of 2000 account writes in memory. About 20 s unoptimized:
    /// `cargo test -p silt-node proof_time -- --ignored --nocapture`.
    #[test]
    #[ignore = "timing printout; run explicitly"]
    fn proof_time_v1_vs_v2_with_15_blocks_in_memory() {
        let chain = chain(15, 20_000, 2000);
        let mut rng = Rng(0xbeef);
        let all = &chain.accounts;
        let requests: Vec<Vec<Target>> = (0..50)
            .map(|_| {
                (0..8)
                    .map(|_| {
                        let (a, slots) = &all[rng.below(all.len())];
                        (*a, slots.iter().take(2).copied().collect())
                    })
                    .collect()
            })
            .collect();
        let tip = chain.hashes.len() - 1;
        let view = view(&chain, tip);
        for r in &requests {
            assert!(v1(&view, r).1 == v2(&view, r), "v2 answer differs from v1");
        }
        let time = |f: &dyn Fn(&[Target])| {
            f(&requests[0]);
            let started = Instant::now();
            for r in &requests {
                f(r);
            }
            started.elapsed() / requests.len() as u32
        };
        let t1 = time(&|r| {
            v1(&view, r);
        });
        let t2 = time(&|r| {
            v2(&view, r);
        });
        let t2_fresh = time(&|r| {
            v2(&self::view(&chain, tip), r);
        });
        println!(
            "proof per call, 8 accounts x 2 slots, 15 blocks in memory: \
             v1 {t1:?}, v2 (reused view) {t2:?}, v2 (fresh view) {t2_fresh:?}"
        );
    }
}
