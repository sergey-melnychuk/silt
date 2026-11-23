//! Pending read touches (PLAN D14, opt-in: `pending_reads`): what the pool's
//! pending transactions read on the head, with the node's own EVM config.
//!
//! One worker thread, off the import path. On each new canonical head it
//! opens that block's state once and runs the pool's best transactions on
//! it, each on the head's state (nothing committed), in reth's predicted
//! environment for the next block (`speculative_next_evm_env`, the one
//! reth's own pool prewarming uses), with nonce and balance checks off so a
//! transaction queued behind another still runs. Every account and slot
//! they read is recorded with its value there; the pass goes to the
//! [`ReadTouches`] sink, and the next diff publishes the keys that block
//! didn't write.
//!
//! The thread runs at nice 10 with the idle I/O class (its reads are mostly
//! first touches, page faults on the node's disk, which must not queue
//! ahead of block import). A pass stops after [`MAX_TXS_PER_PASS`]
//! transactions or [`PASS_BUDGET`], whichever comes first; the budget is
//! enforced inside the reads too, so a transaction stuck behind import I/O
//! is abandoned at its next read.

use std::collections::HashMap;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use alloy_primitives::{Address, B256, U256};
use reth_ethereum::EthPrimitives;
use reth_ethereum::TransactionSigned;
use reth_ethereum::evm::primitives::cached::{AccountInfo, Bytecode};
use reth_ethereum::evm::primitives::database::StateProviderDatabase;
use reth_ethereum::evm::primitives::{ConfigureEvm, Database, Evm, EvmEnv};
use reth_ethereum::pool::{PoolTransaction, TransactionPool};
use reth_ethereum::primitives::SealedHeader;
use reth_ethereum::provider::{StateProvider, StateProviderFactory};

use crate::evm::{ReadAccount, read_account, read_values};
use crate::reads::ReadTouches;

/// Most pool transactions run per head.
pub const MAX_TXS_PER_PASS: usize = 2_000;

/// Most time one pass spends on one head.
pub const PASS_BUDGET: Duration = Duration::from_millis(500);

/// Hands new canonical heads to the worker; the newest one waiting wins.
pub type HeadSender = Sender<SealedHeader>;

/// Starts the worker thread. It runs until the returned sender is dropped.
pub fn spawn<P, Pool, C>(
    provider: P,
    pool: Pool,
    evm_config: C,
    sink: Arc<dyn ReadTouches>,
) -> std::io::Result<HeadSender>
where
    P: StateProviderFactory + Send + 'static,
    Pool: TransactionPool<Transaction: PoolTransaction<Consensus = TransactionSigned>> + 'static,
    C: ConfigureEvm<Primitives = EthPrimitives> + 'static,
{
    let (heads, rx) = std::sync::mpsc::channel();
    std::thread::Builder::new()
        .name("silt-pending".into())
        .spawn(move || run_worker(rx, provider, pool, evm_config, sink))?;
    Ok(heads)
}

/// Lowers the calling thread's priority: CPU nice 10, idle I/O class
/// (Linux: per thread).
fn lower_priority() {
    // SAFETY: plain syscalls on the calling thread's own id.
    unsafe {
        let tid = libc::syscall(libc::SYS_gettid) as libc::id_t;
        libc::setpriority(libc::PRIO_PROCESS, tid, 10);
        const IOPRIO_WHO_PROCESS: libc::c_long = 1;
        const IOPRIO_CLASS_IDLE: libc::c_long = 3;
        libc::syscall(
            libc::SYS_ioprio_set,
            IOPRIO_WHO_PROCESS,
            tid as libc::c_long,
            IOPRIO_CLASS_IDLE << 13,
        );
    }
}

fn run_worker<P, Pool, C>(
    heads: Receiver<SealedHeader>,
    provider: P,
    pool: Pool,
    evm_config: C,
    sink: Arc<dyn ReadTouches>,
) where
    P: StateProviderFactory,
    Pool: TransactionPool<Transaction: PoolTransaction<Consensus = TransactionSigned>>,
    C: ConfigureEvm<Primitives = EthPrimitives>,
{
    lower_priority();
    while let Ok(mut head) = heads.recv() {
        while let Ok(newer) = heads.try_recv() {
            head = newer;
        }
        let started = Instant::now();
        let (number, hash) = (head.number, head.hash());
        let Some(evm_env) = evm_config.speculative_next_evm_env(&head, Address::ZERO) else {
            tracing::debug!(number, "silt-pending: no next-block environment");
            continue;
        };
        let evm_env = evm_env
            .with_nonce_check_disabled()
            .with_balance_check_disabled();
        let state = match provider.state_by_block_hash(hash) {
            Ok(state) => state,
            Err(err) => {
                tracing::debug!(number, %err, "silt-pending: no state for head");
                continue;
            }
        };
        let log = Arc::new(Mutex::new(PassLog::default()));
        let db = PassDb {
            db: StateProviderDatabase::new(state.into_evm_state_provider()),
            log: log.clone(),
            deadline: started + PASS_BUDGET,
        };
        let mut evm = evm_config.evm_with_env(db, evm_env);
        let mut txs = 0u64;
        for pooled in pool.best_transactions().take(MAX_TXS_PER_PASS) {
            if started.elapsed() >= PASS_BUDGET {
                break;
            }
            let tx = evm_config.tx_env(pooled.transaction.clone_into_consensus());
            // Each on the head's state; failures still read what they read.
            let _ = evm.transact_result(&tx);
            txs += 1;
        }
        // The state view (a read transaction) ends here.
        drop(evm);
        let log = std::mem::take(&mut *log.lock().expect("pending log lock"));
        let values = read_values(
            log.accounts,
            log.slots.into_iter().map(|((a, k), v)| (a, k, v)),
        );
        sink.add_pending(
            (number, hash.0),
            values,
            txs,
            started.elapsed().as_micros() as u64,
        );
    }
}

/// What one pass read, each key once.
#[derive(Default)]
struct PassLog {
    accounts: HashMap<Address, ReadAccount>,
    slots: HashMap<(Address, U256), U256>,
}

/// The head's state, recording into `log`, refusing reads after `deadline`.
struct PassDb<DB> {
    db: DB,
    log: Arc<Mutex<PassLog>>,
    deadline: Instant,
}

/// A read refused: the database's own error, or the pass budget spent.
#[derive(Debug)]
enum PassError<E> {
    Db(E),
    Budget,
}

impl<E: std::fmt::Display> std::fmt::Display for PassError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Db(err) => err.fmt(f),
            Self::Budget => f.write_str("pending pass budget spent"),
        }
    }
}

impl<E: std::error::Error> std::error::Error for PassError<E> {}

impl<DB: Database> PassDb<DB> {
    fn check(&self) -> Result<(), PassError<DB::Error>> {
        if Instant::now() < self.deadline {
            Ok(())
        } else {
            Err(PassError::Budget)
        }
    }
}

impl<DB: Database> Database for PassDb<DB> {
    type Error = PassError<DB::Error>;

    fn get_account(&mut self, address: &Address) -> Result<Option<AccountInfo>, Self::Error> {
        self.check()?;
        let info = self.db.get_account(address).map_err(PassError::Db)?;
        self.log
            .lock()
            .expect("pending log lock")
            .accounts
            .insert(*address, read_account(info.as_ref()));
        Ok(info)
    }

    fn get_code_by_hash(&mut self, code_hash: &B256) -> Result<Bytecode, Self::Error> {
        self.check()?;
        self.db.get_code_by_hash(code_hash).map_err(PassError::Db)
    }

    fn get_storage(&mut self, address: &Address, key: &U256) -> Result<U256, Self::Error> {
        self.check()?;
        let value = self.db.get_storage(address, key).map_err(PassError::Db)?;
        self.log
            .lock()
            .expect("pending log lock")
            .slots
            .insert((*address, *key), value);
        Ok(value)
    }

    fn get_block_hash(&mut self, number: &U256) -> Result<B256, Self::Error> {
        self.check()?;
        self.db.get_block_hash(number).map_err(PassError::Db)
    }
}
