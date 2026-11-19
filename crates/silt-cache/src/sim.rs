//! Simulation timing (feature `yevm`, PLAN M6): the one workload every side
//! of the simulation-latency comparison runs, whatever its state source.
//!
//! A simulation runs yevm's `Executor::run` for transactions of block N+1
//! on state N:
//! - [`sim_tx`]: one transaction alone, on a fresh overlay;
//! - [`sim_bundle`]: the whole block in order, on one overlay.
//!
//! The overlay is prepared (`pre_block`) before the clock starts, and the
//! clock stops when the last `run` returns, so a result covers the state
//! source's [`Prefetch`], execution and the state reads it makes, nothing
//! else. [`Counted`] wraps any
//! `Chain` to count those reads and the time spent in them, so every side
//! is measured by the same code.

use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use yevm::core::call::{Block, Head, TxFull};
use yevm::core::exe::pre_block;
use yevm::core::state::Account;
use yevm::{Acc, Buf, CallResult, Chain, Executor, Int, State};

/// Reads a simulation made through its `Chain` (overlay misses): counts and
/// the time spent inside them.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Reads {
    /// `acc`, `code`, `nonce` and `balance` calls.
    pub accounts: u64,
    /// `get` calls.
    pub slots: u64,
    /// `head`, `block` and `chain_id` calls.
    pub other: u64,
    pub nanos: u64,
}

impl Reads {
    pub fn total(&self) -> u64 {
        self.accounts + self.slots + self.other
    }

    /// `self - before`, for counters read around one simulation.
    pub fn since(&self, before: &Reads) -> Reads {
        Reads {
            accounts: self.accounts - before.accounts,
            slots: self.slots - before.slots,
            other: self.other - before.other,
            nanos: self.nanos - before.nanos,
        }
    }
}

/// Counts every read through the `Chain` that `C` points at (`Arc<Cache>`,
/// `&RethDb`, ...) and the time spent in it.
pub struct Counted<C> {
    pub inner: C,
    accounts: AtomicU64,
    slots: AtomicU64,
    other: AtomicU64,
    nanos: AtomicU64,
}

impl<C> Counted<C> {
    pub fn new(inner: C) -> Self {
        Self {
            inner,
            accounts: AtomicU64::new(0),
            slots: AtomicU64::new(0),
            other: AtomicU64::new(0),
            nanos: AtomicU64::new(0),
        }
    }

    pub fn reads(&self) -> Reads {
        Reads {
            accounts: self.accounts.load(Ordering::Relaxed),
            slots: self.slots.load(Ordering::Relaxed),
            other: self.other.load(Ordering::Relaxed),
            nanos: self.nanos.load(Ordering::Relaxed),
        }
    }

    fn done(&self, counter: &AtomicU64, t: Instant) {
        counter.fetch_add(1, Ordering::Relaxed);
        self.nanos
            .fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }
}

#[async_trait::async_trait]
impl<C> Chain for Counted<C>
where
    C: Deref + Send + Sync,
    C::Target: Chain + Send + Sync,
{
    async fn get(&self, acc: &Acc, key: &Int) -> eyre::Result<Int> {
        let t = Instant::now();
        let r = Chain::get(&*self.inner, acc, key).await;
        self.done(&self.slots, t);
        r
    }

    async fn acc(&self, acc: &Acc) -> eyre::Result<Account> {
        let t = Instant::now();
        let r = Chain::acc(&*self.inner, acc).await;
        self.done(&self.accounts, t);
        r
    }

    async fn code(&self, acc: &Acc) -> eyre::Result<(Buf, Int)> {
        let t = Instant::now();
        let r = Chain::code(&*self.inner, acc).await;
        self.done(&self.accounts, t);
        r
    }

    async fn nonce(&self, acc: &Acc) -> eyre::Result<u64> {
        let t = Instant::now();
        let r = Chain::nonce(&*self.inner, acc).await;
        self.done(&self.accounts, t);
        r
    }

    async fn balance(&self, acc: &Acc) -> eyre::Result<Int> {
        let t = Instant::now();
        let r = Chain::balance(&*self.inner, acc).await;
        self.done(&self.accounts, t);
        r
    }

    async fn head(&self, number: u64) -> eyre::Result<Head> {
        let t = Instant::now();
        let r = Chain::head(&*self.inner, number).await;
        self.done(&self.other, t);
        r
    }

    async fn block(&self, number: u64) -> eyre::Result<Block> {
        let t = Instant::now();
        let r = Chain::block(&*self.inner, number).await;
        self.done(&self.other, t);
        r
    }

    async fn chain_id(&self) -> eyre::Result<u64> {
        let t = Instant::now();
        let r = Chain::chain_id(&*self.inner).await;
        self.done(&self.other, t);
        r
    }
}

/// What a state source does before a simulation reads (PLAN M6). silt's
/// cache fetches, in one batched round trip, what the transactions are
/// known to touch: senders, recipients, the coinbase, access lists. A
/// local database has nothing to do (the default). The time it takes
/// counts toward the simulation.
#[async_trait::async_trait]
pub trait Prefetch {
    async fn prefetch(&self, _txs: &[TxFull], _head: &Head) -> eyre::Result<()> {
        Ok(())
    }
}

/// What a transaction did, compact enough to compare across sides and
/// processes (PLAN G6.1): equal outcomes on both sides, or the timing of
/// that transaction does not count.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    Done {
        status: u8,
        gas: i64,
        /// FNV-1a 64 of the return data.
        ret: u64,
        logs: usize,
    },
    Created {
        acc: String,
        gas: i64,
        logs: usize,
    },
    Failed(String),
}

impl std::fmt::Display for Outcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Outcome::Done {
                status,
                gas,
                ret,
                logs,
            } => write!(
                f,
                "done status {status} gas {gas} ret {ret:016x} logs {logs}"
            ),
            Outcome::Created { acc, gas, logs } => {
                write!(f, "created {acc} gas {gas} logs {logs}")
            }
            Outcome::Failed(e) => write!(f, "failed: {e}"),
        }
    }
}

/// FNV-1a 64: stable across builds and processes, unlike std's hasher.
pub fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325u64, |h, b| {
        (h ^ *b as u64).wrapping_mul(0x0100_0000_01b3)
    })
}

/// One simulation: what happened, how long it took, and the reads made
/// while the clock ran.
#[derive(Clone, Debug)]
pub struct Sim {
    pub outcome: Outcome,
    pub elapsed: Duration,
    pub reads: Reads,
}

/// A fresh overlay for `head` (block N+1) on the chain's state N, with the
/// block's system calls applied.
async fn prepared(head: &Head, chain_id: u64, chain: &impl Chain) -> eyre::Result<yevm::Cache> {
    let mut state = yevm::Cache::new();
    state.set_chain_id(chain_id);
    pre_block(head, &mut state, chain).await?;
    Ok(state)
}

async fn run_one(tx: &TxFull, head: &Head, state: &mut yevm::Cache, chain: &impl Chain) -> Outcome {
    let mut exe = Executor::new(tx.call.clone().into());
    match exe.run(&tx.tx, head, state, chain).await {
        Ok(CallResult::Done { status, ret, gas }) => Outcome::Done {
            status: status.as_u8(),
            gas: gas.finalized,
            ret: fnv1a(ret.as_slice()),
            logs: state.logs.len(),
        },
        Ok(CallResult::Created { acc, gas, .. }) => Outcome::Created {
            acc: acc.to_string().to_lowercase(),
            gas: gas.finalized,
            logs: state.logs.len(),
        },
        Err(e) => Outcome::Failed(e.to_string()),
    }
}

/// `tx` alone on state N (`head` is block N+1's header). The clock and
/// the read counts cover `Executor::run` only.
pub async fn sim_tx<C>(
    tx: &TxFull,
    head: &Head,
    chain_id: u64,
    chain: &Counted<C>,
) -> eyre::Result<Sim>
where
    C: Deref + Send + Sync,
    C::Target: Chain + Prefetch + Send + Sync,
{
    let mut state = prepared(head, chain_id, chain).await?;
    let before = chain.reads();
    let t = Instant::now();
    // Best effort: whatever it didn't fetch is read on demand.
    let _ = chain.inner.prefetch(std::slice::from_ref(tx), head).await;
    let outcome = run_one(tx, head, &mut state, chain).await;
    let elapsed = t.elapsed();
    Ok(Sim {
        outcome,
        elapsed,
        reads: chain.reads().since(&before),
    })
}

/// The whole block in order on state N: one overlay, each transaction
/// seeing the ones before it. Returns every transaction's outcome, the
/// time for all of them together and the reads they made.
pub async fn sim_bundle<C>(
    txs: &[TxFull],
    head: &Head,
    chain_id: u64,
    chain: &Counted<C>,
) -> eyre::Result<(Vec<Outcome>, Duration, Reads)>
where
    C: Deref + Send + Sync,
    C::Target: Chain + Prefetch + Send + Sync,
{
    let mut state = prepared(head, chain_id, chain).await?;
    let mut outcomes = Vec::with_capacity(txs.len());
    let before = chain.reads();
    let t = Instant::now();
    let _ = chain.inner.prefetch(txs, head).await;
    for tx in txs {
        state.reset();
        outcomes.push(run_one(tx, head, &mut state, chain).await);
    }
    let elapsed = t.elapsed();
    Ok((outcomes, elapsed, chain.reads().since(&before)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fnv1a_matches_reference_vectors() {
        assert_eq!(fnv1a(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(fnv1a(b"a"), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(fnv1a(b"foobar"), 0x8594_4171_f739_67e8);
    }

    #[test]
    fn reads_since_subtracts_fieldwise() {
        let a = Reads {
            accounts: 5,
            slots: 7,
            other: 1,
            nanos: 900,
        };
        let b = Reads {
            accounts: 2,
            slots: 3,
            other: 1,
            nanos: 400,
        };
        let d = a.since(&b);
        assert_eq!((d.accounts, d.slots, d.other, d.nanos), (3, 4, 0, 500));
        assert_eq!(d.total(), 7);
    }
}
