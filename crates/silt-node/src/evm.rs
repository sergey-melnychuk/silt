//! Read touches on reth's stock executor (PLAN D16).
//!
//! [`SiltEvmConfig`] is reth's own Ethereum EVM configuration
//! ([`EthEvmConfig`], evm2) with one addition: the EVM the engine executes a
//! payload with reads its state through a [`Recording`] database, which
//! notes every account and slot the block loads from below evm2's own
//! state, with its value, and files them in the [`ReadTouchSink`] when the
//! EVM is dropped (the end of the block's execution). Everything else —
//! environments, contexts, executors, block building, prewarming and its
//! result reuse, the precompile cache, the JIT — is the inner config's.
//!
//! Which EVM records: [`ConfigureEngineEvm::context_for_payload`] (only the
//! engine's payload validator calls it) leaves a ticket for the payload's
//! (number, hash) on its thread, and the next EVM created on that thread —
//! the block's own, right after, in the same function — claims it. Every
//! other EVM (RPC, prewarm workers, the payload builder) holds none and
//! runs on the database it was given.
//!
//! Every value the block loads from below its own writes is the parent
//! state's; for a key the block doesn't write, that is also its value at
//! the block, and the ExEx drops written keys ([`crate::reads::merge_reads`]).
//! Prewarm results the engine reuses are covered too: reuse first re-reads
//! each key the speculative run read through this same database.
//! Not covered: blocks with a block-level access list (EIP-7928) run on
//! reth's parallel BAL path, which spreads a block over several EVMs; only
//! the first records, so those blocks carry fewer read touches, never
//! wrong ones.

use std::sync::Arc;
use std::time::Instant;

use alloy_eips::Decodable2718;
use alloy_primitives::{Address, B256, Bytes, U256};
use reth_ethereum::chainspec::ChainSpec;
use reth_ethereum::evm::primitives::cached::{AccountInfo, Bytecode};
use reth_ethereum::evm::primitives::precompile_cache::PrecompileCacheMap;
use reth_ethereum::evm::primitives::{
    ConfigureEngineEvm, ConfigureEvm, Database, EvmEnvFor, EvmFor, ExecutableTxIterator,
    ExecutionCtxFor, JitBackend, NextBlockEnvAttributes, SpecFor, TxExecutionResultFor,
    TxResultWithStateFor,
};
use reth_ethereum::evm::{EthEvmConfig, ExecutableRecoveredTx};
use reth_ethereum::node::EthereumExecutorBuilder;
use reth_ethereum::node::api::{FullNodeTypes, NodeTypes};
use reth_ethereum::node::builder::{BuilderContext, components::ExecutorBuilder};
use reth_ethereum::primitives::{Header, SealedBlock, SealedHeader, SignedTransaction};
use reth_ethereum::rpc::types::engine::ExecutionData;
use reth_ethereum::storage::errors::any::AnyError;
use reth_ethereum::{Block, EthPrimitives, TransactionSigned};
use silt_wire::{AccountChange, KECCAK_EMPTY};

use crate::reads::{ReadTouchSink, ReadValues, Ticket};

/// Reth's stock Ethereum EVM configuration, recording the engine's read
/// touches into `reads` (see the module docs). Without a sink it is the
/// inner config, unchanged.
#[derive(Debug, Clone)]
pub struct SiltEvmConfig {
    /// Everything but the recording.
    pub inner: EthEvmConfig,
    reads: Option<Arc<ReadTouchSink>>,
}

impl SiltEvmConfig {
    pub fn new(inner: EthEvmConfig, reads: Option<Arc<ReadTouchSink>>) -> Self {
        Self { inner, reads }
    }

    /// The sink and this thread's ticket, if the engine left one for this
    /// EVM's block.
    fn claim(&self, evm_env: &EvmEnvFor<Self>) -> Option<(Arc<ReadTouchSink>, Ticket)> {
        let sink = self.reads.as_ref()?;
        let ticket = ReadTouchSink::claim_ticket()?;
        if evm_env.block.number != U256::from(ticket.number) {
            sink.mismatched();
            return None;
        }
        Some((sink.clone(), ticket))
    }
}

/// Builds the stock Ethereum EVM config exactly as reth's
/// [`EthereumExecutorBuilder`] does (JIT settings, sender recovery cache),
/// wrapped in a [`SiltEvmConfig`]: for
/// `EthereumNode::components().executor(..)`.
#[derive(Debug, Default, Clone)]
pub struct SiltExecutorBuilder {
    reads: Option<Arc<ReadTouchSink>>,
}

impl SiltExecutorBuilder {
    pub fn new(reads: Option<Arc<ReadTouchSink>>) -> Self {
        Self { reads }
    }
}

impl<Node> ExecutorBuilder<Node> for SiltExecutorBuilder
where
    Node: FullNodeTypes<Types: NodeTypes<ChainSpec = ChainSpec, Primitives = EthPrimitives>>,
{
    type EVM = SiltEvmConfig;

    async fn build_evm(self, ctx: &BuilderContext<Node>) -> eyre::Result<Self::EVM> {
        let inner = EthereumExecutorBuilder::default().build_evm(ctx).await?;
        Ok(SiltEvmConfig::new(inner, self.reads))
    }
}

impl ConfigureEvm for SiltEvmConfig {
    type Primitives = EthPrimitives;
    type Error = <EthEvmConfig as ConfigureEvm>::Error;
    type NextBlockEnvCtx = NextBlockEnvAttributes;
    type BlockExecutorFactory = <EthEvmConfig as ConfigureEvm>::BlockExecutorFactory;
    type BlockAssembler = <EthEvmConfig as ConfigureEvm>::BlockAssembler;

    fn block_executor_factory(&self) -> &Self::BlockExecutorFactory {
        self.inner.block_executor_factory()
    }

    fn block_assembler(&self) -> &Self::BlockAssembler {
        self.inner.block_assembler()
    }

    fn evm_env(&self, header: &Header) -> Result<EvmEnvFor<Self>, Self::Error> {
        self.inner.evm_env(header)
    }

    fn next_evm_env(
        &self,
        parent: &Header,
        attributes: &NextBlockEnvAttributes,
    ) -> Result<EvmEnvFor<Self>, Self::Error> {
        self.inner.next_evm_env(parent, attributes)
    }

    fn context_for_block<'a>(
        &self,
        block: &'a SealedBlock<Block>,
    ) -> Result<ExecutionCtxFor<'a, Self>, Self::Error>
    where
        Self: 'a,
    {
        // A block the engine didn't get as a payload (synced from the
        // network, replayed): no read touches, and no ticket left over.
        ReadTouchSink::clear_ticket();
        self.inner.context_for_block(block)
    }

    fn context_for_next_block(
        &self,
        parent: &SealedHeader,
        attributes: NextBlockEnvAttributes,
    ) -> Result<ExecutionCtxFor<'_, Self>, Self::Error> {
        self.inner.context_for_next_block(parent, attributes)
    }

    fn with_jit_support_enabled(self, enabled: bool) -> Self
    where
        Self: Sized,
    {
        Self {
            inner: self.inner.with_jit_support_enabled(enabled),
            reads: self.reads,
        }
    }

    fn evm_with_precompile_cache<'a, DB>(
        &self,
        db: DB,
        evm_env: EvmEnvFor<Self>,
        cache_map: PrecompileCacheMap<SpecFor<Self>>,
        metrics_enabled: bool,
    ) -> EvmFor<'a, Self>
    where
        DB: Database + 'a,
    {
        match self.claim(&evm_env) {
            Some((sink, ticket)) => self.inner.evm_with_precompile_cache(
                Recording::new(db, sink, ticket),
                evm_env,
                cache_map,
                metrics_enabled,
            ),
            None => self
                .inner
                .evm_with_precompile_cache(db, evm_env, cache_map, metrics_enabled),
        }
    }

    fn jit_backend(&self) -> Option<&dyn JitBackend> {
        self.inner.jit_backend()
    }

    fn evm_with_env<'a, DB>(&self, db: DB, evm_env: EvmEnvFor<Self>) -> EvmFor<'a, Self>
    where
        DB: Database + 'a,
    {
        match self.claim(&evm_env) {
            Some((sink, ticket)) => self
                .inner
                .evm_with_env(Recording::new(db, sink, ticket), evm_env),
            None => self.inner.evm_with_env(db, evm_env),
        }
    }

    fn reuses_prewarm_results(&self) -> bool {
        self.inner.reuses_prewarm_results()
    }

    fn prewarm_tx_result<'a>(
        &self,
        tx: &TransactionSigned,
        result: TxResultWithStateFor<Self>,
    ) -> Option<TxExecutionResultFor<'a, Self>>
    where
        Self: 'a,
    {
        self.inner.prewarm_tx_result(tx, result)
    }

    fn speculative_next_evm_env(
        &self,
        parent: &SealedHeader,
        beneficiary: Address,
    ) -> Option<EvmEnvFor<Self>> {
        self.inner.speculative_next_evm_env(parent, beneficiary)
    }
}

impl ConfigureEngineEvm<ExecutionData> for SiltEvmConfig {
    fn evm_env_for_payload(&self, payload: &ExecutionData) -> Result<EvmEnvFor<Self>, Self::Error> {
        self.inner.evm_env_for_payload(payload)
    }

    fn context_for_payload<'a>(
        &self,
        payload: &'a ExecutionData,
    ) -> Result<ExecutionCtxFor<'a, Self>, Self::Error> {
        // Only the engine's payload validator gets here; the EVM it creates
        // next on this thread executes this block and claims the ticket.
        if let Some(sink) = &self.reads {
            sink.expect(payload.block_number(), payload.block_hash().0);
        }
        self.inner.context_for_payload(payload)
    }

    fn tx_iterator_for_payload(
        &self,
        payload: &ExecutionData,
    ) -> Result<impl ExecutableTxIterator<Self>, Self::Error> {
        // The inner config's, verbatim: its iterator type is opaque and
        // typed for `EthEvmConfig`, so it can't be handed through.
        let txs = payload.payload.transactions().clone();
        let sender_recovery_cache = self.inner.sender_recovery_cache.clone();
        let convert = move |tx: Bytes| {
            let tx = TransactionSigned::decode_2718_exact(tx.as_ref()).map_err(AnyError::new)?;
            let signer = if let Some(cache) = &sender_recovery_cache {
                cache.recover(&tx)
            } else {
                tx.try_recover()
            }
            .map_err(AnyError::new)?;
            Ok::<_, AnyError>(ExecutableRecoveredTx::new(tx.with_signer(signer)))
        };
        Ok((txs, convert))
    }
}

/// A database that notes every account and slot read through it, with its
/// value, and files them under `ticket` when dropped: the end of the
/// block's execution. A failed read means a failed execution: nothing is
/// filed then.
pub struct Recording<DB> {
    db: DB,
    sink: Arc<ReadTouchSink>,
    ticket: Ticket,
    accounts: Vec<(Address, ReadAccount)>,
    slots: Vec<(Address, U256, U256)>,
    failed: bool,
}

impl<DB> Recording<DB> {
    pub fn new(db: DB, sink: Arc<ReadTouchSink>, ticket: Ticket) -> Self {
        Self {
            db,
            sink,
            ticket,
            accounts: Vec::with_capacity(1024),
            slots: Vec::with_capacity(4096),
            failed: false,
        }
    }

    fn note<T, E>(&mut self, result: &Result<T, E>) {
        self.failed |= result.is_err();
    }
}

impl<DB: Database> Database for Recording<DB> {
    type Error = DB::Error;

    fn is_fatal(error: &Self::Error) -> bool {
        DB::is_fatal(error)
    }

    fn get_account(&mut self, address: &Address) -> Result<Option<AccountInfo>, Self::Error> {
        let result = self.db.get_account(address);
        self.note(&result);
        if let Ok(info) = &result {
            self.accounts.push((*address, read_account(info.as_ref())));
        }
        result
    }

    fn get_code_by_hash(&mut self, code_hash: &B256) -> Result<Bytecode, Self::Error> {
        let result = self.db.get_code_by_hash(code_hash);
        self.note(&result);
        result
    }

    fn get_storage(&mut self, address: &Address, key: &U256) -> Result<U256, Self::Error> {
        let result = self.db.get_storage(address, key);
        self.note(&result);
        if let Ok(value) = &result {
            self.slots.push((*address, *key, *value));
        }
        result
    }

    fn get_block_hash(&mut self, number: &U256) -> Result<B256, Self::Error> {
        let result = self.db.get_block_hash(number);
        self.note(&result);
        result
    }
}

impl<DB> Drop for Recording<DB> {
    fn drop(&mut self) {
        if self.failed || std::thread::panicking() {
            return;
        }
        // On the engine's thread: only move what was recorded; the ExEx
        // converts it when it takes the block's values.
        let start = Instant::now();
        let accounts = std::mem::take(&mut self.accounts);
        let slots = std::mem::take(&mut self.slots);
        self.sink.put_deferred(
            self.ticket,
            move || read_values(accounts, slots),
            start.elapsed().as_micros() as u64,
        );
    }
}

/// An account as read from the state: (balance, nonce, code hash), `None`
/// if it doesn't exist.
pub(crate) type ReadAccount = Option<(U256, u64, B256)>;

pub(crate) fn read_account(info: Option<&AccountInfo>) -> ReadAccount {
    info.map(|i| (i.balance, i.nonce, i.code_hash))
}

/// Recorded reads as silt-wire values. An account that doesn't exist
/// reads as empty (EIP-161).
pub(crate) fn read_values(
    accounts: impl IntoIterator<Item = (Address, ReadAccount)>,
    slots: impl IntoIterator<Item = (Address, U256, U256)>,
) -> ReadValues {
    ReadValues {
        accounts: accounts
            .into_iter()
            .map(|(address, info)| {
                let change = match info {
                    Some((balance, nonce, code_hash)) => AccountChange {
                        balance: balance.to_be_bytes(),
                        nonce,
                        code_hash: code_hash.0,
                    },
                    None => AccountChange {
                        balance: [0; 32],
                        nonce: 0,
                        code_hash: KECCAK_EMPTY,
                    },
                };
                (address.0.0, change)
            })
            .collect(),
        slots: slots
            .into_iter()
            .map(|(address, key, value)| (address.0.0, key.to_be_bytes(), value.to_be_bytes()))
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reads::ReadTouches;
    use reth_ethereum::evm::primitives::DynDatabase;
    use reth_ethereum::rpc::types::engine::{
        ExecutionPayload, ExecutionPayloadSidecar, ExecutionPayloadV1,
    };
    use std::collections::HashMap;

    /// A plain in-memory state; `broken` fails every read of that account.
    #[derive(Default)]
    struct TestDb {
        accounts: HashMap<Address, AccountInfo>,
        slots: HashMap<(Address, U256), U256>,
        broken: Option<Address>,
    }

    impl Database for TestDb {
        type Error = std::io::Error;

        fn get_account(&mut self, address: &Address) -> Result<Option<AccountInfo>, Self::Error> {
            if self.broken == Some(*address) {
                return Err(std::io::Error::other("broken"));
            }
            Ok(self.accounts.get(address).cloned())
        }

        fn get_code_by_hash(&mut self, _: &B256) -> Result<Bytecode, Self::Error> {
            Ok(Bytecode::default())
        }

        fn get_storage(&mut self, address: &Address, key: &U256) -> Result<U256, Self::Error> {
            Ok(self
                .slots
                .get(&(*address, *key))
                .copied()
                .unwrap_or_default())
        }

        fn get_block_hash(&mut self, _: &U256) -> Result<B256, Self::Error> {
            Ok(B256::ZERO)
        }
    }

    fn state() -> TestDb {
        let mut db = TestDb::default();
        let info = AccountInfo {
            balance: U256::from(1000),
            nonce: 7,
            code_hash: B256::repeat_byte(0xC0),
            ..Default::default()
        };
        db.accounts.insert(Address::repeat_byte(1), info);
        db.slots
            .insert((Address::repeat_byte(1), U256::from(5)), U256::from(55));
        db
    }

    fn expected() -> ReadValues {
        ReadValues {
            accounts: vec![
                (
                    [1; 20],
                    AccountChange {
                        balance: U256::from(1000).to_be_bytes(),
                        nonce: 7,
                        code_hash: [0xC0; 32],
                    },
                ),
                (
                    [2; 20],
                    AccountChange {
                        balance: [0; 32],
                        nonce: 0,
                        code_hash: KECCAK_EMPTY,
                    },
                ),
            ],
            slots: vec![(
                [1; 20],
                U256::from(5).to_be_bytes(),
                U256::from(55).to_be_bytes(),
            )],
        }
    }

    fn reads(db: &mut impl Database) {
        db.get_account(&Address::repeat_byte(1)).unwrap();
        db.get_account(&Address::repeat_byte(2)).unwrap();
        db.get_storage(&Address::repeat_byte(1), &U256::from(5))
            .unwrap();
        db.get_code_by_hash(&B256::repeat_byte(0xC0)).unwrap();
    }

    /// [`reads`] through an EVM's own (type-erased) database.
    fn reads_dyn(db: &mut dyn DynDatabase) {
        db.get_account(&Address::repeat_byte(1)).unwrap();
        db.get_account(&Address::repeat_byte(2)).unwrap();
        db.get_storage(&Address::repeat_byte(1), &U256::from(5))
            .unwrap();
        db.get_code_by_hash(&B256::repeat_byte(0xC0)).unwrap();
    }

    /// What the block loaded is filed under its ticket when the EVM goes;
    /// a failed read files nothing.
    #[test]
    fn recording_files_reads_on_drop_unless_a_read_failed() {
        let sink = Arc::new(ReadTouchSink::new(4));
        sink.expect(7, [7; 32]);
        let ticket = ReadTouchSink::claim_ticket().unwrap();
        let mut db = Recording::new(state(), sink.clone(), ticket);
        reads(&mut db);
        drop(db);
        assert_eq!(sink.take(7, &[7; 32]), Some(expected()));

        sink.expect(8, [8; 32]);
        let ticket = ReadTouchSink::claim_ticket().unwrap();
        let mut broken = state();
        broken.broken = Some(Address::repeat_byte(3));
        let mut db = Recording::new(broken, sink.clone(), ticket);
        reads(&mut db);
        assert!(db.get_account(&Address::repeat_byte(3)).is_err());
        drop(db);
        assert_eq!(sink.take(8, &[8; 32]), None);
    }

    fn payload(number: u64, hash: B256) -> ExecutionData {
        ExecutionData::new(
            ExecutionPayload::V1(ExecutionPayloadV1 {
                parent_hash: B256::repeat_byte(0xAA),
                fee_recipient: Address::ZERO,
                state_root: B256::ZERO,
                receipts_root: B256::ZERO,
                logs_bloom: Default::default(),
                prev_randao: B256::ZERO,
                block_number: number,
                gas_limit: 30_000_000,
                gas_used: 0,
                timestamp: 0,
                extra_data: Bytes::new(),
                base_fee_per_gas: U256::from(7),
                block_hash: hash,
                transactions: vec![],
            }),
            ExecutionPayloadSidecar::none(),
        )
    }

    fn env_at(config: &SiltEvmConfig, number: u64) -> EvmEnvFor<SiltEvmConfig> {
        config
            .evm_env(&Header {
                number,
                ..Default::default()
            })
            .unwrap()
    }

    /// The engine's sequence: `context_for_payload`, then the block's EVM on
    /// the same thread records; EVMs without the ticket, or for another
    /// block, don't.
    #[test]
    fn only_the_announced_blocks_evm_records() {
        let sink = Arc::new(ReadTouchSink::new(8));
        let config = SiltEvmConfig::new(EthEvmConfig::mainnet(), Some(sink.clone()));
        let hash = B256::repeat_byte(0x10);

        let data = payload(10, hash);
        config.context_for_payload(&data).unwrap();
        let mut evm = config.evm_with_env(state(), env_at(&config, 10));
        reads_dyn(evm.database_mut());
        drop(evm);
        assert_eq!(sink.take(10, &hash.0), Some(expected()));

        // No ticket: an RPC call, a prewarm worker.
        let mut evm = config.evm_with_env(state(), env_at(&config, 10));
        reads_dyn(evm.database_mut());
        drop(evm);

        // A ticket for block 11 claimed by an EVM for block 12: not filed.
        let data = payload(11, B256::repeat_byte(0x11));
        config.context_for_payload(&data).unwrap();
        let mut evm = config.evm_with_env(state(), env_at(&config, 12));
        reads_dyn(evm.database_mut());
        drop(evm);
        assert_eq!(sink.take(11, &[0x11; 32]), None);
        assert!(sink.stats().unwrap().contains("mismatched 1"));

        // Without a sink it's the stock config: nothing expected or kept.
        let plain = SiltEvmConfig::new(EthEvmConfig::mainnet(), None);
        plain
            .context_for_payload(&payload(13, B256::repeat_byte(0x13)))
            .unwrap();
        assert_eq!(ReadTouchSink::claim_ticket(), None);
    }
}
