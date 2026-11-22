//! Publishing at execution (PLAN D15): each block goes out as soon as the
//! engine has executed and validated it, before `forkchoiceUpdated` makes it
//! canonical and before reth's ExEx WAL writes it to disk.
//!
//! The host forwards the engine's `CanonicalBlockAdded` events into a
//! channel ([`forward_engine_events`]); the ExEx publishes each block that
//! continues the chain it has published, and keeps publishing whatever
//! that path missed from its own notifications. [`Published`] is the chain
//! it has published, and decides, for each committed diff, whether it is
//! already out, needs publishing, or first needs a `Reorg` because an early
//! block it published did not become canonical.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Instant;

use futures::{Stream, StreamExt};
use reth_ethereum::node::api::ConsensusEngineEvent;
use reth_ethereum::primitives::RecoveredBlock;
use reth_ethereum::provider::BlockExecutionOutput;
use reth_ethereum::{Block, EthPrimitives, Receipt};
use silt_wire::{BlockDiff, Word};
use tokio::sync::mpsc;

/// Executed blocks waiting for the ExEx. A full queue drops the newest (the
/// ExEx publishes them after commit instead); it only fills if the ExEx is
/// stuck.
pub const EARLY_QUEUE: usize = 64;

/// Most recent published blocks [`Published`] remembers: deeper than any
/// reorg the engine reports and than the feed's spool.
pub const PUBLISHED_WINDOW: usize = 256;

/// One block the engine executed and validated above the canonical head,
/// with its outcome, still in memory.
pub struct ExecutedHead {
    pub block: Arc<RecoveredBlock<Block>>,
    pub output: Arc<BlockExecutionOutput<Receipt>>,
    /// When the forwarder received the engine's event.
    pub received: Instant,
}

pub type EarlySender = mpsc::Sender<ExecutedHead>;
pub type EarlyReceiver = mpsc::Receiver<ExecutedHead>;

/// The channel from [`forward_engine_events`] to the ExEx.
pub fn channel() -> (EarlySender, EarlyReceiver) {
    mpsc::channel(EARLY_QUEUE)
}

/// Executed blocks dropped on a full queue (published after commit).
pub static FORWARD_DROPPED: AtomicU64 = AtomicU64::new(0);

/// Forwards every `CanonicalBlockAdded` the engine emits to the ExEx, until
/// the events end or the ExEx is gone. The host spawns it once the node is
/// launched, with a listener on the engine's events (reth:
/// `add_ons_handle.engine_events.new_listener()`). Never blocks the engine:
/// the events come off a broadcast channel, and a full queue drops.
pub async fn forward_engine_events<S>(events: S, tx: EarlySender)
where
    S: Stream<Item = ConsensusEngineEvent<EthPrimitives>>,
{
    let mut events = std::pin::pin!(events);
    while let Some(event) = events.next().await {
        let ConsensusEngineEvent::CanonicalBlockAdded(executed, _) = event else {
            continue;
        };
        let head = ExecutedHead {
            block: executed.recovered_block.clone(),
            output: executed.execution_output.clone(),
            received: Instant::now(),
        };
        match tx.try_send(head) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                FORWARD_DROPPED.fetch_add(1, Relaxed);
            }
            Err(mpsc::error::TrySendError::Closed(_)) => return,
        }
    }
}

/// What to do with a committed diff (see [`Published::plan`]).
#[derive(Debug, PartialEq, Eq)]
pub enum Plan {
    /// Already published: every block of it, same hashes.
    Skip,
    /// Publish it.
    Publish,
    /// Blocks published early at its heights didn't become canonical:
    /// publish a `Reorg` to `fork` dropping `dropped`, then the diff.
    ReorgFirst {
        fork: (u64, Word),
        dropped: Vec<(u64, Word)>,
    },
}

/// The chain of blocks the feed has published, (number, hash), oldest
/// first, contiguous; at most `cap`.
#[derive(Debug)]
pub struct Published {
    chain: VecDeque<(u64, Word)>,
    cap: usize,
}

impl Published {
    pub fn new(cap: usize) -> Self {
        Self {
            chain: VecDeque::new(),
            cap: cap.max(1),
        }
    }

    /// The last block published.
    pub fn tip(&self) -> Option<(u64, Word)> {
        self.chain.back().copied()
    }

    fn hash_at(&self, number: u64) -> Option<Word> {
        let first = self.chain.front()?.0;
        let at = usize::try_from(number.checked_sub(first)?).ok()?;
        self.chain.get(at).map(|(_, hash)| *hash)
    }

    /// Whether block (`number`, `hash`) is published.
    pub fn holds(&self, number: u64, hash: &Word) -> bool {
        self.hash_at(number) == Some(*hash)
    }

    /// Whether a block `number` on `parent_hash` continues the published
    /// chain: the only early blocks published. Nothing does before the
    /// first diff.
    pub fn continues(&self, number: u64, parent_hash: &Word) -> bool {
        number
            .checked_sub(1)
            .is_some_and(|parent| self.tip() == Some((parent, *parent_hash)))
    }

    /// Published blocks above `number`, oldest first.
    pub fn above(&self, number: u64) -> Vec<(u64, Word)> {
        self.chain
            .iter()
            .filter(|(n, _)| *n > number)
            .copied()
            .collect()
    }

    /// What to do with a diff of committed (canonical) blocks:
    /// - its last block is published already: [`Plan::Skip`];
    /// - a published block at one of its heights has another hash: an
    ///   early block that didn't become canonical, so
    ///   [`Plan::ReorgFirst`], to the block below the first such height;
    /// - otherwise [`Plan::Publish`]: it extends the chain, overlaps its
    ///   tip (consumers apply an overlapping span), or doesn't connect at
    ///   all (consumers see the break, as before D15).
    pub fn plan(&self, diff: &BlockDiff) -> Plan {
        if self.holds(diff.number, &diff.hash) {
            return Plan::Skip;
        }
        let Some(below) = diff.span_start.checked_sub(1) else {
            return Plan::Publish;
        };
        let mut prev = (below, diff.parent_hash);
        if self.hash_at(prev.0).is_some_and(|h| h != prev.1) {
            // Diverged below the span: no fork to name from it.
            return Plan::Publish;
        }
        for head in &diff.heads {
            if self.hash_at(head.number).is_some_and(|h| h != head.hash) {
                return Plan::ReorgFirst {
                    fork: prev,
                    dropped: self.above(prev.0),
                };
            }
            prev = (head.number, head.hash);
        }
        Plan::Publish
    }

    /// A `Reorg` to `fork` was published: forget everything above it.
    pub fn rewind(&mut self, fork: (u64, Word)) {
        while self.chain.back().is_some_and(|(n, _)| *n > fork.0) {
            self.chain.pop_back();
        }
        if self.tip() != Some(fork) {
            self.chain.clear();
            self.chain.push_back(fork);
        }
    }

    /// `diff` was published.
    pub fn record(&mut self, diff: &BlockDiff) {
        // An overlapping span re-covers the tip.
        while self
            .chain
            .back()
            .is_some_and(|(n, _)| *n >= diff.span_start)
        {
            self.chain.pop_back();
        }
        let below = diff
            .span_start
            .checked_sub(1)
            .map(|n| (n, diff.parent_hash));
        if self.tip().is_some() && self.tip() != below {
            // A diff that doesn't connect starts the chain over.
            self.chain.clear();
        }
        self.chain
            .extend(diff.heads.iter().map(|h| (h.number, h.hash)));
        while self.chain.len() > self.cap {
            self.chain.pop_front();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use silt_wire::HeadLite;

    fn h(number: u64, fork: u8) -> Word {
        let mut w = [fork; 32];
        w[..8].copy_from_slice(&number.to_be_bytes());
        w
    }

    /// A diff of blocks `start..=end` on fork `fork`, whose parent is on
    /// fork `parent_fork`.
    fn diff(start: u64, end: u64, fork: u8, parent_fork: u8) -> BlockDiff {
        let heads = (start..=end)
            .map(|n| HeadLite {
                number: n,
                hash: h(n, fork),
                parent_hash: if n == start {
                    h(n - 1, parent_fork)
                } else {
                    h(n - 1, fork)
                },
                timestamp: 0,
                basefee: [0; 32],
                gas_limit: 0,
                coinbase: [0; 20],
                prevrandao: [0; 32],
            })
            .collect();
        BlockDiff {
            span_start: start,
            number: end,
            hash: h(end, fork),
            parent_hash: h(start - 1, parent_fork),
            heads,
            accounts: vec![],
            storage: vec![],
            code: vec![],
            receipts: vec![],
            read_accounts: vec![],
            read_storage: vec![],
        }
    }

    #[test]
    fn early_blocks_continue_the_chain_and_commits_skip_them() {
        let mut p = Published::new(16);
        assert!(!p.continues(10, &h(9, 0)), "nothing before the first diff");
        // The first diff comes from the ExEx.
        assert_eq!(p.plan(&diff(10, 10, 0, 0)), Plan::Publish);
        p.record(&diff(10, 10, 0, 0));
        // Early: 11 on 10 continues; 12 on a block not published doesn't.
        assert!(p.continues(11, &h(10, 0)));
        assert!(!p.continues(12, &h(11, 0)));
        assert!(!p.continues(11, &h(10, 7)), "a sibling of 10 doesn't");
        p.record(&diff(11, 11, 0, 0));
        assert!(p.holds(11, &h(11, 0)));
        // Their commits are skipped, alone or folded into a span.
        assert_eq!(p.plan(&diff(11, 11, 0, 0)), Plan::Skip);
        assert_eq!(p.plan(&diff(10, 11, 0, 0)), Plan::Skip);
        // A commit past the early tip overlaps it: published, then recorded
        // over the overlap.
        assert_eq!(p.plan(&diff(11, 13, 0, 0)), Plan::Publish);
        p.record(&diff(11, 13, 0, 0));
        assert_eq!(p.tip(), Some((13, h(13, 0))));
        assert!(p.holds(12, &h(12, 0)) && p.holds(10, &h(10, 0)));
    }

    /// The engine validated 11a above the head and it went out early; the
    /// consensus client chose 11b. Its commit reorgs to 10 first, dropping
    /// 11a and the 12a built on it.
    #[test]
    fn a_commit_of_a_sibling_reorgs_the_early_blocks_first() {
        let mut p = Published::new(16);
        p.record(&diff(10, 10, 0, 0));
        p.record(&diff(11, 11, 1, 0));
        p.record(&diff(12, 12, 1, 1));
        let commit = diff(11, 11, 2, 0);
        assert_eq!(
            p.plan(&commit),
            Plan::ReorgFirst {
                fork: (10, h(10, 0)),
                dropped: vec![(11, h(11, 1)), (12, h(12, 1))],
            }
        );
        p.rewind((10, h(10, 0)));
        assert_eq!(p.tip(), Some((10, h(10, 0))));
        assert_eq!(p.plan(&commit), Plan::Publish);
        p.record(&commit);
        assert!(p.continues(12, &h(11, 2)));

        // Divergence inside a folded span: the fork is the block below it.
        let mut p = Published::new(16);
        p.record(&diff(10, 10, 0, 0));
        p.record(&diff(11, 11, 0, 0));
        p.record(&diff(12, 12, 1, 0));
        assert_eq!(
            p.plan(&diff(11, 13, 0, 0)),
            Plan::ReorgFirst {
                fork: (11, h(11, 0)),
                dropped: vec![(12, h(12, 1))],
            }
        );
    }

    #[test]
    fn rewind_record_and_the_window() {
        let mut p = Published::new(4);
        for n in 10..=15 {
            p.record(&diff(n, n, 0, 0));
        }
        let window: Vec<_> = (12..=15).map(|n| (n, h(n, 0))).collect();
        assert_eq!(p.above(0), window);
        assert!(!p.holds(11, &h(11, 0)), "beyond the window");
        // A reorg to a fork the window no longer holds starts over there.
        p.rewind((5, h(5, 3)));
        assert_eq!(p.tip(), Some((5, h(5, 3))));
        assert!(p.continues(6, &h(5, 3)));
        // A diff that doesn't connect starts the chain over too.
        p.record(&diff(20, 21, 4, 4));
        assert_eq!(p.above(0), vec![(20, h(20, 4)), (21, h(21, 4))]);
        // Diverged below a span: nothing to name as the fork; publish.
        assert_eq!(p.plan(&diff(22, 22, 5, 6)), Plan::Publish);
    }
}
