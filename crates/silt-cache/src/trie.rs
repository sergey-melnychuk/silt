//! Warm partial state trie, kept current by the diff stream (PLAN D11).
//!
//! [`WarmTrie`] holds a partial account trie, plus a partial storage trie
//! per account it has touched, across blocks. Each diff is applied to it and
//! the state root recomputed, so a consumer can check the state it follows
//! against block headers and compute roots for state it derives. Only nodes
//! the trie has never seen are fetched:
//!
//! 1. one batched proof request (op `Proof = 4`, D10) for the diff's keys
//!    whose paths end in a stub, pinned at the trie's block;
//! 2. path-directed probes for any stub an update still runs into, e.g. the
//!    sibling a deletion's collapse needs: a key padded from the stub's
//!    nibble path, whose proof must pass through that node (D8).
//!
//! The trie itself is mpt-lab's `mpt-core` partial MPT (byte-exact roots;
//! verified against mainnet there). Every fetched node is checked against
//! the hash its parent holds before use.
//!
//! **Memory.** Revealed nodes are bounded by [`WarmTrieConfig::max_nodes`]:
//! past it, subtrees below `keep_depth` nibbles turn back into stubs. Storage
//! tries untouched for `storage_ttl` blocks are dropped; they rebuild from
//! their account's storage root on the next touch.
//!
//! **Runtime.** Probes run synchronously inside `mpt-core`'s traversal
//! (`tokio::task::block_in_place`), so [`WarmTrie::apply`] needs a
//! multi-threaded tokio runtime.
//!
//! **Failure.** A fetch that fails during an update leaves the trie
//! unusable (the traversal takes the root apart as it goes); the trie then
//! drops to cold and [`WarmTrie::apply`] returns the error. Seed it again
//! from a header's state root ([`WarmTrie::seed`]).
//!
//! **Reorgs (PLAN D12).** Every diff journals the leaves it replaced and
//! the root it started from; [`WarmTrie::rollback`] re-applies the old
//! leaves newest-first and checks the root it lands on. Nodes the undo
//! needs and doesn't hold are fetched pinned at the fork block: they are
//! content-addressed, so any node unchanged since the fork verifies.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::Arc;

use mpt_core::Keccak256;
use mpt_core::hasher::keccak;
use mpt_core::partial::NodeProvider;
use mpt_core::trie::{Node, Trie, build_partial, node_rlp, node_root};
use silt_wire::lookup::{MAX_ITEMS, MAX_PROOF_SLOTS};
use silt_wire::{AccountChange, Addr, BlockDiff, KECCAK_EMPTY, Word};

use crate::lookup::LookupClient;

type K = Keccak256;

/// Pre-hashed proof targets: `(hashed address, [hashed slot])`.
pub type Targets = Vec<(Word, Vec<Word>)>;

/// keccak(rlp("")): the root of an empty trie.
pub fn empty_root() -> Word {
    keccak(&[0x80])
}

/// Where trie nodes come from.
#[async_trait::async_trait]
pub trait ProofSource: Send + Sync {
    /// The RLP of every node (account and storage tries) on the paths of
    /// `targets` at block `pin`.
    async fn nodes(&self, pin: &Word, targets: &[(Word, Vec<Word>)]) -> eyre::Result<Vec<Vec<u8>>>;
}

/// Op 4 over the node's lookup listener, split into requests within the
/// protocol's limits.
#[async_trait::async_trait]
impl ProofSource for LookupClient {
    async fn nodes(&self, pin: &Word, targets: &[(Word, Vec<Word>)]) -> eyre::Result<Vec<Vec<u8>>> {
        let mut out = Vec::new();
        for chunk in split_targets(targets) {
            let frame = self
                .proof(pin, &chunk)
                .await
                .map_err(|e| eyre::eyre!("op 4 at 0x{}: {e}", hex(pin)))?;
            let p = frame.proof();
            out.extend(p.account_nodes().map(|n| n.rlp.to_vec()));
            for s in p.storages() {
                out.extend(s.nodes.map(|n| n.rlp.to_vec()));
            }
        }
        Ok(out)
    }
}

fn hex(w: &Word) -> String {
    w.iter().map(|b| format!("{b:02x}")).collect()
}

/// Splits targets into requests of at most `MAX_ITEMS` targets and
/// `MAX_PROOF_SLOTS` slots (one account's slots may span requests).
pub fn split_targets(targets: &[(Word, Vec<Word>)]) -> Vec<Targets> {
    let (mut out, mut cur, mut slots) = (Vec::new(), Vec::new(), 0usize);
    for (addr, ss) in targets {
        let mut rest = ss.as_slice();
        loop {
            if cur.len() == MAX_ITEMS || (slots == MAX_PROOF_SLOTS && !rest.is_empty()) {
                out.push(std::mem::take(&mut cur));
                slots = 0;
            }
            let n = rest.len().min(MAX_PROOF_SLOTS - slots);
            cur.push((*addr, rest[..n].to_vec()));
            slots += n;
            rest = &rest[n..];
            if rest.is_empty() {
                break;
            }
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

/// Pack a nibble prefix into a 32-byte key, zero-padded on the right: any
/// key with this prefix walks through the node at that path, so its proof
/// contains that node.
pub fn key_with_prefix(prefix: &[u8]) -> Word {
    let mut out = [0u8; 32];
    for (i, n) in prefix.iter().take(64).enumerate() {
        out[i / 2] |= if i % 2 == 0 { n << 4 } else { n & 0x0f };
    }
    out
}

#[derive(Clone, Debug)]
pub struct WarmTrieConfig {
    /// Revealed nodes (account trie plus storage tries) before pruning.
    pub max_nodes: usize,
    /// Pruning keeps this many nibbles of each trie; deeper subtrees become
    /// stubs again.
    pub keep_depth: usize,
    /// Storage tries untouched for this many blocks are dropped.
    pub storage_ttl: u64,
    /// Blocks of undo journal for [`WarmTrie::rollback`] (PLAN D12),
    /// capped at `storage_ttl` so an undo never needs a dropped storage
    /// trie. `0` = no journal.
    pub reorg_depth: usize,
}

impl Default for WarmTrieConfig {
    fn default() -> Self {
        Self {
            max_nodes: 250_000,
            keep_depth: 4,
            storage_ttl: 64,
            reorg_depth: crate::store::DEFAULT_REORG_DEPTH,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct TrieStats {
    /// Diffs applied.
    pub applied: u64,
    /// Batched proof requests (step 1) and path-directed probes (step 2).
    pub batches: u64,
    pub probes: u64,
    pub fetched_nodes: u64,
    pub fetched_bytes: u64,
    pub prunes: u64,
    /// Drops to cold after a failed update or rollback.
    pub resets: u64,
    /// Reorgs undone by [`WarmTrie::rollback`].
    pub rollbacks: u64,
    /// Revealed nodes and storage tries after the last apply.
    pub nodes: usize,
    pub storage_tries: usize,
}

pub struct WarmTrie {
    source: Arc<dyn ProofSource>,
    cfg: WarmTrieConfig,
    state: Trie<K>,
    /// hashed address -> (storage trie, last block that touched it).
    storages: HashMap<Word, (Trie<K>, u64)>,
    /// The block the trie is at: (number, hash).
    at: Option<(u64, Word)>,
    /// The state root at `at` (the last apply's, or the seed's).
    root_at: Word,
    /// Undo records of the last diffs applied, oldest first (D12).
    journal: VecDeque<TrieUndoBlock>,
    stats: TrieStats,
}

/// A leaf (or storage trie) one diff replaced.
enum TrieUndo {
    /// The account leaf before; `None` = absent.
    Account(Word, Option<Vec<u8>>),
    /// The slot's RLP value before, in the storage trie of the hashed
    /// address; `None` = absent.
    Slot(Word, Word, Option<Vec<u8>>),
    /// A storage trie dropped with its destroyed account.
    Storage(Word, (Trie<K>, u64)),
}

struct TrieUndoBlock {
    /// The block and root the diff was applied on.
    prev: (u64, Word),
    prev_root: Word,
    number: u64,
    /// In the order applied; undone in reverse.
    ops: Vec<TrieUndo>,
}

struct Leaf {
    nonce: u64,
    balance: Vec<u8>,
    storage_root: Word,
    code_hash: Word,
}

fn decode_leaf(bytes: &[u8]) -> eyre::Result<Leaf> {
    let r = rlp::Rlp::new(bytes);
    let word = |i: usize| -> eyre::Result<Word> {
        let v: Vec<u8> = r.val_at(i).map_err(|e| eyre::eyre!("account leaf: {e}"))?;
        Ok(v.as_slice().try_into()?)
    };
    Ok(Leaf {
        nonce: r.val_at(0).map_err(|e| eyre::eyre!("account leaf: {e}"))?,
        balance: r.val_at(1).map_err(|e| eyre::eyre!("account leaf: {e}"))?,
        storage_root: word(2)?,
        code_hash: word(3)?,
    })
}

/// `rlp([nonce, balance, storage_root, code_hash])`: an account leaf.
pub fn encode_leaf(nonce: u64, balance: &[u8], storage_root: &Word, code_hash: &Word) -> Vec<u8> {
    let mut s = rlp::RlpStream::new_list(4);
    s.append(&nonce);
    s.append(&balance);
    s.append(&storage_root.as_slice());
    s.append(&code_hash.as_slice());
    s.out().to_vec()
}

/// Minimal big-endian bytes: the RLP integer form (`[]` for zero).
pub fn trim(bytes: &[u8]) -> &[u8] {
    let i = bytes.iter().position(|&b| b != 0).unwrap_or(bytes.len());
    &bytes[i..]
}

fn trie_at(root: &Word) -> Trie<K> {
    Trie::from_node(build_partial(&BTreeMap::new(), root))
}

/// Nodes fetched while applying one diff, shared by the account trie and
/// every storage trie (content-addressed, so they can't collide).
struct Fetcher<'a> {
    source: &'a dyn ProofSource,
    pin: Word,
    handle: tokio::runtime::Handle,
    nodes: RefCell<HashMap<Word, Vec<u8>>>,
    probes: Cell<u64>,
    fetched: Cell<(u64, u64)>,
}

impl Fetcher<'_> {
    fn add(&self, nodes: Vec<Vec<u8>>) {
        let mut map = self.nodes.borrow_mut();
        let (mut n, mut bytes) = self.fetched.get();
        for rlp in nodes {
            if rlp.as_slice() == [0x80] {
                continue;
            }
            n += 1;
            bytes += rlp.len() as u64;
            map.insert(keccak(&rlp), rlp);
        }
        self.fetched.set((n, bytes));
    }
}

/// A [`Fetcher`] seen from one trie: `owner` is the hashed address for a
/// storage trie, `None` for the account trie.
struct View<'f, 'a> {
    f: &'f Fetcher<'a>,
    owner: Option<Word>,
}

impl NodeProvider<K> for View<'_, '_> {
    fn get(&self, path: &[u8], hash: &Word) -> Option<Vec<u8>> {
        if let Some(bytes) = self.f.nodes.borrow().get(hash) {
            return Some(bytes.clone());
        }
        let probe = key_with_prefix(path);
        let targets = match self.owner {
            None => vec![(probe, Vec::new())],
            Some(owner) => vec![(owner, vec![probe])],
        };
        let f = self.f;
        let nodes =
            tokio::task::block_in_place(|| f.handle.block_on(f.source.nodes(&f.pin, &targets)));
        f.probes.set(f.probes.get() + 1);
        f.add(nodes.ok()?);
        f.nodes.borrow().get(hash).cloned()
    }
}

fn trie_err(what: &str, e: mpt_core::error::TrieError<K>) -> eyre::Report {
    eyre::eyre!("{what}: {e:?}")
}

/// Revealed (non-stub) nodes.
fn count(node: &Node<K>) -> usize {
    match node {
        Node::Null | Node::Stub(_) => 0,
        Node::Leaf { .. } => 1,
        Node::Skip { child, .. } => 1 + count(child),
        Node::Fork { children, .. } => {
            1 + children.iter().flatten().map(|c| count(c)).sum::<usize>()
        }
    }
}

/// Turns every subtree at `keep` nibbles or deeper back into a stub. Nodes
/// whose RLP is under 32 bytes stay: their parent inlines them rather than
/// referencing a hash, so a stub would change the parent's encoding.
fn prune(node: Node<K>, depth: usize, keep: usize) -> Node<K> {
    match node {
        Node::Null | Node::Stub(_) => node,
        _ if depth >= keep => {
            let enc = node_rlp(&node);
            if enc.len() >= 32 {
                Node::Stub(keccak(&enc))
            } else {
                node
            }
        }
        Node::Fork { children, value } => Node::Fork {
            children: children.map(|c| c.map(|b| Box::new(prune(*b, depth + 1, keep)))),
            value,
        },
        Node::Skip { path, child } => {
            let d = depth + path.len();
            Node::Skip {
                path,
                child: Box::new(prune(*child, d, keep)),
            }
        }
        leaf @ Node::Leaf { .. } => leaf,
    }
}

impl WarmTrie {
    pub fn new(source: Arc<dyn ProofSource>, cfg: WarmTrieConfig) -> Self {
        Self {
            source,
            cfg,
            state: Trie::new(),
            storages: HashMap::new(),
            at: None,
            root_at: empty_root(),
            journal: VecDeque::new(),
            stats: TrieStats::default(),
        }
    }

    /// Cold start at a block whose state root is all we know (e.g. from
    /// its header). Everything is fetched on first touch.
    pub fn seed(&mut self, number: u64, hash: Word, state_root: Word) {
        self.state = trie_at(&state_root);
        self.storages.clear();
        self.at = Some((number, hash));
        self.root_at = state_root;
        self.journal.clear();
    }

    /// The block the trie is at, or `None` when cold.
    pub fn at(&self) -> Option<(u64, Word)> {
        self.at
    }

    pub fn root(&self) -> Word {
        node_root(self.state.root())
    }

    pub fn stats(&self) -> &TrieStats {
        &self.stats
    }

    fn reset(&mut self) {
        self.state = Trie::new();
        self.storages.clear();
        self.at = None;
        self.journal.clear();
        self.stats.resets += 1;
    }

    fn journal_depth(&self) -> usize {
        self.cfg
            .reorg_depth
            .min(usize::try_from(self.cfg.storage_ttl).unwrap_or(usize::MAX))
    }

    /// Rolls the trie back to the fork block (PLAN D12): re-applies, newest
    /// first, the leaves every diff above it replaced, and returns the root
    /// it lands on, which must equal the root the trie had at the fork.
    /// Errs, leaving the trie as it was, if the journal doesn't reach back
    /// to a diff applied right on the fork; errs and drops to cold if the
    /// undo needs a node it can't get (e.g. pruned since) or lands on
    /// another root. Either way, reseed from the fork's header. A trie
    /// already at the fork returns its root. Needs a multi-threaded tokio
    /// runtime, like [`WarmTrie::apply`].
    pub async fn rollback(&mut self, fork_number: u64, fork_hash: Word) -> eyre::Result<Word> {
        let (at, _) = self
            .at
            .ok_or_else(|| eyre::eyre!("cold: nothing to roll back"))?;
        if self.at == Some((fork_number, fork_hash)) {
            return Ok(self.root_at);
        }
        let above = self
            .journal
            .iter()
            .rev()
            .take_while(|b| b.number > fork_number)
            .count();
        let oldest = self.journal.len() - above;
        let want = match self.journal.get(oldest) {
            Some(b) if at > fork_number && b.prev == (fork_number, fork_hash) => b.prev_root,
            _ => eyre::bail!("the journal doesn't reach back to block {fork_number}"),
        };
        let undone: Vec<TrieUndoBlock> = self.journal.drain(oldest..).collect();
        let source = self.source.clone();
        let fetcher = Fetcher {
            source: &*source,
            pin: fork_hash,
            handle: tokio::runtime::Handle::current(),
            nodes: RefCell::new(HashMap::new()),
            probes: Cell::new(0),
            fetched: Cell::new((0, 0)),
        };
        let result = self.undo(&fetcher, undone);
        let (n, bytes) = fetcher.fetched.get();
        self.stats.fetched_nodes += n;
        self.stats.fetched_bytes += bytes;
        self.stats.probes += fetcher.probes.get();
        drop(fetcher);
        let root = match result {
            Ok(()) => self.root(),
            Err(e) => {
                self.reset();
                return Err(e);
            }
        };
        if root != want {
            self.reset();
            eyre::bail!(
                "rollback to {fork_number} landed on {}, not {}",
                hex(&root),
                hex(&want)
            );
        }
        self.at = Some((fork_number, fork_hash));
        self.root_at = root;
        self.stats.rollbacks += 1;
        Ok(root)
    }

    fn undo(&mut self, f: &Fetcher<'_>, blocks: Vec<TrieUndoBlock>) -> eyre::Result<()> {
        let accounts = View { f, owner: None };
        for block in blocks.into_iter().rev() {
            for op in block.ops.into_iter().rev() {
                match op {
                    TrieUndo::Account(ha, Some(leaf)) => self
                        .state
                        .insert_with(&accounts, &ha, leaf)
                        .map_err(|e| trie_err("undo account", e))?,
                    TrieUndo::Account(ha, None) => {
                        self.state
                            .remove_with(&accounts, &ha)
                            .map_err(|e| trie_err("undo account", e))?;
                    }
                    TrieUndo::Slot(ha, hs, value) => {
                        let (storage, _) = self
                            .storages
                            .get_mut(&ha)
                            .ok_or_else(|| eyre::eyre!("undo: storage trie {} gone", hex(&ha)))?;
                        let view = View { f, owner: Some(ha) };
                        match value {
                            Some(v) => storage.insert_with(&view, &hs, v),
                            None => storage.remove_with(&view, &hs).map(|_| ()),
                        }
                        .map_err(|e| trie_err("undo slot", e))?;
                    }
                    TrieUndo::Storage(ha, trie) => {
                        self.storages.insert(ha, trie);
                    }
                }
            }
        }
        Ok(())
    }

    /// Applies `diff` and returns the new state root. The diff must
    /// continue the trie's block: start right after it on its hash, or (a
    /// span re-delivered after a node restart) start at or below it and
    /// pass through it.
    pub async fn apply(&mut self, diff: &BlockDiff) -> eyre::Result<Word> {
        let (at, pin) = self
            .at
            .ok_or_else(|| eyre::eyre!("cold: seed the trie first"))?;
        let continues = if diff.span_start == at + 1 {
            diff.parent_hash == pin
        } else {
            diff.span_start <= at
                && diff.number > at
                && diff.heads.iter().any(|h| h.number == at && h.hash == pin)
        };
        eyre::ensure!(
            continues,
            "diff {}..{} does not continue block {at}",
            diff.span_start,
            diff.number
        );

        let mut writes: BTreeMap<Addr, Vec<(Word, Word)>> = BTreeMap::new();
        for (a, s, v) in &diff.storage {
            writes.entry(*a).or_default().push((*s, *v));
        }
        let changes: BTreeMap<Addr, Option<AccountChange>> =
            diff.accounts.iter().cloned().collect();
        let touched: BTreeSet<Addr> = changes.keys().chain(writes.keys()).copied().collect();

        let targets = self.missing(&touched, &changes, &writes, diff.number)?;
        let batch = if targets.is_empty() {
            Vec::new()
        } else {
            // Before any update: a failure here leaves the trie intact.
            self.stats.batches += 1;
            self.source.nodes(&pin, &targets).await?
        };
        let source = self.source.clone();
        let fetcher = Fetcher {
            source: &*source,
            pin,
            handle: tokio::runtime::Handle::current(),
            nodes: RefCell::new(HashMap::new()),
            probes: Cell::new(0),
            fetched: Cell::new((0, 0)),
        };
        fetcher.add(batch);
        let mut ops = Vec::new();
        let result = self.update(diff.number, &fetcher, &touched, &changes, &writes, &mut ops);
        let (n, bytes) = fetcher.fetched.get();
        self.stats.fetched_nodes += n;
        self.stats.fetched_bytes += bytes;
        self.stats.probes += fetcher.probes.get();
        drop(fetcher);
        if let Err(e) = result {
            self.reset();
            return Err(e);
        }
        if self.journal_depth() > 0 {
            self.journal.push_back(TrieUndoBlock {
                prev: (at, pin),
                prev_root: self.root_at,
                number: diff.number,
                ops,
            });
            while self.journal.len() > self.journal_depth() {
                self.journal.pop_front();
            }
        }
        self.at = Some((diff.number, diff.hash));
        self.stats.applied += 1;
        self.maybe_prune(diff.number);
        self.root_at = self.root();
        Ok(self.root_at)
    }

    /// The diff's keys whose paths end in a stub: accounts (with all their
    /// written slots) and, for accounts we hold, slots.
    fn missing(
        &mut self,
        touched: &BTreeSet<Addr>,
        changes: &BTreeMap<Addr, Option<AccountChange>>,
        writes: &BTreeMap<Addr, Vec<(Word, Word)>>,
        now: u64,
    ) -> eyre::Result<Targets> {
        let mut targets = Vec::new();
        for addr in touched {
            let ha = keccak(addr);
            let slots: Vec<Word> = writes
                .get(addr)
                .map(|w| w.iter().map(|(s, _)| keccak(s)).collect())
                .unwrap_or_default();
            let leaf = match self.state.get(&ha) {
                Ok(leaf) => leaf.map(<[u8]>::to_vec),
                Err(_) => {
                    targets.push((ha, slots));
                    continue;
                }
            };
            if slots.is_empty() || matches!(changes.get(addr), Some(None)) {
                continue;
            }
            let storage = self.storage_trie(ha, leaf.as_deref(), now)?;
            let need: Vec<Word> = slots
                .into_iter()
                .filter(|s| storage.get(s).is_err())
                .collect();
            if !need.is_empty() {
                targets.push((ha, need));
            }
        }
        Ok(targets)
    }

    /// The warm storage trie of `ha`, or a fresh one at the storage root its
    /// account leaf holds.
    fn storage_trie(
        &mut self,
        ha: Word,
        leaf: Option<&[u8]>,
        now: u64,
    ) -> eyre::Result<&mut Trie<K>> {
        let root = match leaf {
            Some(l) if !self.storages.contains_key(&ha) => decode_leaf(l)?.storage_root,
            _ => empty_root(),
        };
        Ok(&mut self
            .storages
            .entry(ha)
            .or_insert_with(|| (trie_at(&root), now))
            .0)
    }

    fn update(
        &mut self,
        now: u64,
        f: &Fetcher<'_>,
        touched: &BTreeSet<Addr>,
        changes: &BTreeMap<Addr, Option<AccountChange>>,
        writes: &BTreeMap<Addr, Vec<(Word, Word)>>,
        ops: &mut Vec<TrieUndo>,
    ) -> eyre::Result<()> {
        let accounts = View { f, owner: None };
        for (addr, change) in changes {
            if change.is_none() {
                let ha = keccak(addr);
                let prior = self
                    .state
                    .get_with(&accounts, &ha)
                    .map_err(|e| trie_err("read account", e))?
                    .map(<[u8]>::to_vec);
                self.state
                    .remove_with(&accounts, &ha)
                    .map_err(|e| trie_err("remove account", e))?;
                ops.push(TrieUndo::Account(ha, prior));
                if let Some(storage) = self.storages.remove(&ha) {
                    ops.push(TrieUndo::Storage(ha, storage));
                }
            }
        }
        for addr in touched {
            let change = changes.get(addr);
            if matches!(change, Some(None)) {
                continue;
            }
            let ha = keccak(addr);
            let prior_leaf = self
                .state
                .get_with(&accounts, &ha)
                .map_err(|e| trie_err("read account", e))?
                .map(<[u8]>::to_vec);
            let prior = prior_leaf.as_deref().map(decode_leaf).transpose()?;

            // Storage first: the leaf needs the new root.
            let storage_root = match writes.get(addr) {
                Some(w) => {
                    let mut storage = match self.storages.remove(&ha) {
                        Some((t, _)) => t,
                        None => {
                            trie_at(&prior.as_ref().map_or_else(empty_root, |l| l.storage_root))
                        }
                    };
                    let view = View { f, owner: Some(ha) };
                    for (slot, value) in w {
                        let hs = keccak(slot);
                        let before = storage
                            .get_with(&view, &hs)
                            .map_err(|e| trie_err("read slot", e))?
                            .map(<[u8]>::to_vec);
                        ops.push(TrieUndo::Slot(ha, hs, before));
                        let value = trim(value);
                        if value.is_empty() {
                            storage.remove_with(&view, &hs)
                        } else {
                            storage
                                .insert_with(&view, &hs, rlp::encode(&value).to_vec())
                                .map(|()| true)
                        }
                        .map_err(|e| trie_err("write slot", e))?;
                    }
                    let root = node_root(storage.root());
                    self.storages.insert(ha, (storage, now));
                    root
                }
                None => prior.as_ref().map_or_else(empty_root, |l| l.storage_root),
            };

            let (nonce, balance, code_hash) = match (change, &prior) {
                (Some(Some(c)), _) => (c.nonce, trim(&c.balance).to_vec(), c.code_hash),
                (_, Some(l)) => (l.nonce, l.balance.clone(), l.code_hash),
                _ => (0, Vec::new(), KECCAK_EMPTY),
            };
            // EIP-161: an untouched-before, empty account is never created.
            if prior.is_none()
                && nonce == 0
                && balance.is_empty()
                && code_hash == KECCAK_EMPTY
                && storage_root == empty_root()
            {
                continue;
            }
            self.state
                .insert_with(
                    &accounts,
                    &ha,
                    encode_leaf(nonce, &balance, &storage_root, &code_hash),
                )
                .map_err(|e| trie_err("write account", e))?;
            ops.push(TrieUndo::Account(ha, prior_leaf));
        }
        Ok(())
    }

    fn maybe_prune(&mut self, now: u64) {
        let ttl = self.cfg.storage_ttl;
        self.storages
            .retain(|_, (_, last)| now.saturating_sub(*last) <= ttl);
        let total = |s: &Self| {
            count(s.state.root())
                + s.storages
                    .values()
                    .map(|(t, _)| count(t.root()))
                    .sum::<usize>()
        };
        let mut nodes = total(self);
        if nodes > self.cfg.max_nodes {
            let keep = self.cfg.keep_depth;
            self.state = Trie::from_node(prune(self.state.root().clone(), 0, keep));
            for (t, _) in self.storages.values_mut() {
                *t = Trie::from_node(prune(t.root().clone(), 0, keep));
            }
            self.stats.prunes += 1;
            nodes = total(self);
        }
        self.stats.nodes = nodes;
        self.stats.storage_tries = self.storages.len();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use silt_wire::HeadLite;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[derive(Clone, Default)]
    struct Acct {
        nonce: u64,
        balance: Word,
        code_hash: Word,
        storage: BTreeMap<Word, Word>,
    }

    type World = BTreeMap<Addr, Acct>;

    /// The full tries of a world: the "node" a warm trie fetches from.
    struct Full {
        state: Trie<K>,
        storages: HashMap<Word, Trie<K>>,
    }

    fn full(world: &World) -> Full {
        let (mut state, mut storages) = (Trie::new(), HashMap::new());
        for (addr, a) in world {
            let mut st = Trie::<K>::new();
            for (slot, value) in &a.storage {
                st.insert(&keccak(slot), rlp::encode(&trim(value)).to_vec())
                    .unwrap();
            }
            let root = if a.storage.is_empty() {
                empty_root()
            } else {
                node_root(st.root())
            };
            state
                .insert(
                    &keccak(addr),
                    encode_leaf(a.nonce, trim(&a.balance), &root, &a.code_hash),
                )
                .unwrap();
            storages.insert(keccak(addr), st);
        }
        Full { state, storages }
    }

    /// Serves proofs from full tries snapshotted per block hash.
    #[derive(Default)]
    struct Node {
        snapshots: Mutex<HashMap<Word, Full>>,
        calls: AtomicU64,
        fail: Mutex<Option<Word>>,
    }

    #[async_trait::async_trait]
    impl ProofSource for Node {
        async fn nodes(
            &self,
            pin: &Word,
            targets: &[(Word, Vec<Word>)],
        ) -> eyre::Result<Vec<Vec<u8>>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            eyre::ensure!(*self.fail.lock().unwrap() != Some(*pin), "node unavailable");
            let mut snaps = self.snapshots.lock().unwrap();
            let full = snaps
                .get_mut(pin)
                .ok_or_else(|| eyre::eyre!("unknown pin"))?;
            let mut out = Vec::new();
            for (ha, slots) in targets {
                out.extend(full.state.prove(ha));
                if let Some(st) = full.storages.get_mut(ha) {
                    for s in slots {
                        out.extend(st.prove(s));
                    }
                }
            }
            Ok(out)
        }
    }

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
        fn word(&mut self) -> Word {
            let mut w = [0u8; 32];
            let v = self.next();
            w[24..].copy_from_slice(&v.to_be_bytes());
            w
        }
    }

    fn addr(i: u64) -> Addr {
        let mut a = [0u8; 20];
        a[12..].copy_from_slice(&i.to_be_bytes());
        a
    }

    fn block_hash(n: u64) -> Word {
        let mut h = [0xB0; 32];
        h[24..].copy_from_slice(&n.to_be_bytes());
        h
    }

    fn head(n: u64) -> HeadLite {
        HeadLite {
            number: n,
            hash: block_hash(n),
            parent_hash: block_hash(n - 1),
            timestamp: n * 12,
            basefee: [0; 32],
            gas_limit: 30_000_000,
            coinbase: [0; 20],
            prevrandao: [0; 32],
        }
    }

    fn genesis(rng: &mut Rng) -> World {
        let mut w = World::new();
        for i in 0..300u64 {
            let mut a = Acct {
                nonce: rng.below(100),
                balance: rng.word(),
                code_hash: KECCAK_EMPTY,
                storage: BTreeMap::new(),
            };
            if i % 7 == 0 {
                a.code_hash = keccak(&i.to_be_bytes());
                for _ in 0..rng.below(60) {
                    a.storage.insert(rng.word(), rng.word());
                }
            }
            w.insert(addr(i), a);
        }
        w
    }

    /// One block of random changes: balance/nonce updates, new accounts,
    /// slot writes (new, updated, and cleared, which collapse nodes), and
    /// destroyed accounts. Returns the diff and the next world.
    fn step(world: &World, n: u64, rng: &mut Rng) -> (BlockDiff, World) {
        let mut next = world.clone();
        let mut accounts = BTreeMap::new();
        let mut storage = Vec::new();
        let keys: Vec<Addr> = world.keys().copied().collect();
        for _ in 0..20 {
            let a = keys[rng.below(keys.len() as u64) as usize];
            let acct = next.get_mut(&a).unwrap();
            acct.nonce += 1;
            acct.balance = rng.word();
            accounts.insert(a, Some(acct.clone()));
        }
        for i in 0..3 {
            let a = addr(10_000 + n * 10 + i);
            let acct = Acct {
                nonce: 1,
                balance: rng.word(),
                code_hash: KECCAK_EMPTY,
                storage: BTreeMap::new(),
            };
            next.insert(a, acct.clone());
            accounts.insert(a, Some(acct));
        }
        let contracts: Vec<Addr> = world
            .iter()
            .filter(|(_, a)| a.code_hash != KECCAK_EMPTY)
            .map(|(k, _)| *k)
            .collect();
        for _ in 0..4 {
            let a = contracts[rng.below(contracts.len() as u64) as usize];
            let acct = next.get_mut(&a).unwrap();
            let existing: Vec<Word> = acct.storage.keys().copied().collect();
            for _ in 0..8 {
                let (slot, value) = match rng.below(3) {
                    0 if !existing.is_empty() => (
                        existing[rng.below(existing.len() as u64) as usize],
                        [0u8; 32],
                    ),
                    1 if !existing.is_empty() => (
                        existing[rng.below(existing.len() as u64) as usize],
                        rng.word(),
                    ),
                    _ => (rng.word(), rng.word()),
                };
                if value == [0u8; 32] {
                    acct.storage.remove(&slot);
                } else {
                    acct.storage.insert(slot, value);
                }
                storage.push((a, slot, value));
            }
            accounts.insert(a, Some(acct.clone()));
        }
        if n.is_multiple_of(5) {
            let victim = keys[rng.below(keys.len() as u64) as usize];
            if let std::collections::btree_map::Entry::Vacant(e) = accounts.entry(victim) {
                next.remove(&victim);
                e.insert(None);
            }
        }
        let diff = BlockDiff {
            span_start: n,
            number: n,
            hash: block_hash(n),
            parent_hash: block_hash(n - 1),
            heads: vec![head(n)],
            accounts: accounts
                .into_iter()
                .map(|(a, acct)| {
                    let change = acct.map(|acct| AccountChange {
                        balance: acct.balance,
                        nonce: acct.nonce,
                        code_hash: acct.code_hash,
                    });
                    (a, change)
                })
                .collect(),
            storage,
            code: vec![],
            receipts: vec![],
        };
        (diff, next)
    }

    /// Runs `blocks` random blocks through a warm trie; every root must equal
    /// the full trie's. Returns the warm trie and the node.
    async fn follow(cfg: WarmTrieConfig, blocks: u64) -> (WarmTrie, Arc<Node>) {
        let mut rng = Rng(0x0511_7d11);
        let mut world = genesis(&mut rng);
        let node = Arc::new(Node::default());
        let root0 = node_root(full(&world).state.root());
        node.snapshots
            .lock()
            .unwrap()
            .insert(block_hash(0), full(&world));
        let mut warm = WarmTrie::new(node.clone(), cfg);
        warm.seed(0, block_hash(0), root0);
        for n in 1..=blocks {
            let (diff, next) = step(&world, n, &mut rng);
            let want = node_root(full(&next).state.root());
            let got = warm.apply(&diff).await.unwrap();
            assert_eq!(hex(&got), hex(&want), "root after block {n}");
            node.snapshots
                .lock()
                .unwrap()
                .insert(block_hash(n), full(&next));
            world = next;
        }
        (warm, node)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn follows_random_blocks_with_exact_roots() {
        let (warm, node) = follow(WarmTrieConfig::default(), 40).await;
        let s = warm.stats();
        eprintln!("{s:?}");
        assert_eq!(s.applied, 40);
        assert_eq!(s.resets, 0);
        assert!(s.batches <= 40 && s.fetched_nodes > 0);
        assert!(
            node.calls.load(Ordering::SeqCst) < 40 * 10,
            "fetches stay batched: {} calls",
            node.calls.load(Ordering::SeqCst)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pruning_keeps_roots_exact() {
        let cfg = WarmTrieConfig {
            max_nodes: 150,
            keep_depth: 1,
            storage_ttl: 3,
            reorg_depth: 3,
        };
        let (warm, _) = follow(cfg, 30).await;
        assert!(warm.stats().prunes > 0, "the cap was hit");
        assert_eq!(warm.stats().resets, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_failed_batch_leaves_the_trie_intact() {
        let mut rng = Rng(7);
        let world = genesis(&mut rng);
        let node = Arc::new(Node::default());
        node.snapshots
            .lock()
            .unwrap()
            .insert(block_hash(0), full(&world));
        *node.fail.lock().unwrap() = Some(block_hash(0));
        let mut warm = WarmTrie::new(node.clone(), WarmTrieConfig::default());
        warm.seed(0, block_hash(0), node_root(full(&world).state.root()));
        let (diff, _) = step(&world, 1, &mut rng);
        assert!(warm.apply(&diff).await.is_err());
        // The batch failed before any update: still seeded, not reset.
        assert_eq!(warm.at(), Some((0, block_hash(0))));
        assert!(
            warm.apply(&step(&world, 2, &mut rng).0).await.is_err(),
            "2 does not continue 0"
        );
    }

    /// Re-chains a generated diff onto `parent`, as block hash `hash`.
    fn rechain(diff: &mut BlockDiff, hash: Word, parent: Word) {
        diff.hash = hash;
        diff.parent_hash = parent;
        diff.heads[0].hash = hash;
        diff.heads[0].parent_hash = parent;
    }

    /// Generates `len` blocks of side `tag` on top of block `fork` (hash
    /// `parent`) of `world`, registering each block's snapshot with the
    /// node. Returns the diffs with the full-trie root after each, and the
    /// world at the side's tip.
    fn side(
        node: &Node,
        world: &World,
        (fork, parent): (u64, Word),
        len: u64,
        tag: u8,
        rng: &mut Rng,
    ) -> (Vec<(BlockDiff, Word)>, World) {
        let (mut world, mut parent) = (world.clone(), parent);
        let blocks = (fork + 1..=fork + len)
            .map(|n| {
                let (mut diff, next) = step(&world, n, rng);
                let mut hash = block_hash(n);
                hash[0] = tag;
                rechain(&mut diff, hash, parent);
                let full_next = full(&next);
                let root = node_root(full_next.state.root());
                node.snapshots.lock().unwrap().insert(hash, full_next);
                (world, parent) = (next, hash);
                (diff, root)
            })
            .collect();
        (blocks, world)
    }

    /// D12: following side A, rolling back to the fork, then following side
    /// B gives B's exact roots; B then becomes the chain the next trial
    /// extends. With heavy pruning a rollback may fail (the undo needs a
    /// node changed and pruned since the fork), but it must never land on
    /// a wrong root; a failed one reseeds at the fork.
    async fn reorgs(cfg: WarmTrieConfig, trials: u64) -> TrieStats {
        let mut rng = Rng(0x0511_d12d);
        let mut world = genesis(&mut rng);
        let node = Arc::new(Node::default());
        let mut root = node_root(full(&world).state.root());
        let mut tip = (0u64, block_hash(0));
        node.snapshots.lock().unwrap().insert(tip.1, full(&world));
        let mut warm = WarmTrie::new(node.clone(), cfg);
        warm.seed(tip.0, tip.1, root);
        for trial in 0..trials {
            let (common, next) = side(&node, &world, tip, 1 + rng.below(3), 0xC0, &mut rng);
            for (diff, want) in &common {
                assert_eq!(
                    warm.apply(diff).await.unwrap(),
                    *want,
                    "block {}",
                    diff.number
                );
                (tip, root) = ((diff.number, diff.hash), *want);
            }
            world = next;
            let (a, _) = side(&node, &world, tip, 1 + rng.below(3), 0xA0, &mut rng);
            let (b, b_world) = side(&node, &world, tip, 1 + rng.below(3), 0xB0, &mut rng);
            for (diff, want) in &a {
                assert_eq!(
                    warm.apply(diff).await.unwrap(),
                    *want,
                    "side A {}",
                    diff.number
                );
            }
            match warm.rollback(tip.0, tip.1).await {
                Ok(got) => assert_eq!(got, root, "trial {trial}: root at the fork"),
                Err(_) => warm.seed(tip.0, tip.1, root),
            }
            assert_eq!(warm.at(), Some(tip));
            for (diff, want) in &b {
                assert_eq!(
                    warm.apply(diff).await.unwrap(),
                    *want,
                    "side B {}",
                    diff.number
                );
                (tip, root) = ((diff.number, diff.hash), *want);
            }
            world = b_world;
        }
        warm.stats().clone()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rollback_then_the_new_side_gives_exact_roots() {
        let s = reorgs(WarmTrieConfig::default(), 12).await;
        eprintln!("{s:?}");
        assert_eq!(s.rollbacks, 12, "every rollback succeeds without pruning");
        assert_eq!(s.resets, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rollback_under_heavy_pruning_is_exact_or_refuses() {
        let cfg = WarmTrieConfig {
            max_nodes: 150,
            keep_depth: 1,
            storage_ttl: 8,
            reorg_depth: 8,
        };
        let s = reorgs(cfg, 12).await;
        eprintln!("{s:?}");
        assert!(s.prunes > 0, "the cap was hit");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rollback_refuses_beyond_the_journal() {
        let cfg = WarmTrieConfig {
            reorg_depth: 2,
            ..WarmTrieConfig::default()
        };
        let (mut warm, _) = follow(cfg, 5).await;
        let root5 = warm.root();
        assert!(warm.rollback(2, block_hash(2)).await.is_err());
        assert_eq!(warm.at(), Some((5, block_hash(5))), "left as it was");
        assert_eq!(warm.root(), root5);
        assert!(
            warm.rollback(3, [0x99; 32]).await.is_err(),
            "wrong fork hash"
        );
        assert_eq!(
            warm.rollback(5, block_hash(5)).await.unwrap(),
            root5,
            "at the fork"
        );
        assert!(warm.rollback(3, block_hash(3)).await.is_ok());
        assert_eq!(warm.at(), Some((3, block_hash(3))));
    }

    #[test]
    fn split_targets_respects_limits() {
        let many: Targets = (0..2500u64)
            .map(|i| (key_with_prefix(&[(i % 16) as u8]), vec![[1; 32]; 3]))
            .collect();
        let parts = split_targets(&many);
        assert!(parts.iter().all(|p| p.len() <= MAX_ITEMS));
        assert!(
            parts
                .iter()
                .all(|p| p.iter().map(|(_, s)| s.len()).sum::<usize>() <= MAX_PROOF_SLOTS)
        );
        let flat: usize = parts.iter().flatten().map(|(_, s)| s.len()).sum();
        assert_eq!(flat, 7500);
        let big: Targets = vec![([2; 32], vec![[3; 32]; MAX_PROOF_SLOTS + 10])];
        let parts = split_targets(&big);
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[1][0].1.len(), 10);
    }

    #[test]
    fn key_with_prefix_packs_high_nibble_first() {
        let k = key_with_prefix(&[0xa, 0xb, 0xc]);
        assert_eq!(&k[..2], &[0xab, 0xc0]);
        assert!(k[2..].iter().all(|&b| b == 0));
    }
}
