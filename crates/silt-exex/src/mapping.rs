//! `Chain` -> `silt_wire::BlockDiff` (PLAN T1.2, docs/reth-notes.md).
//!
//! Multi-block segments fold into one cumulative diff (D6): the bundle
//! state reth hands us is already cumulative over the segment, receipts
//! and headers stay per block inside the frame.

use alloy_primitives::{B256, U256};
use reth_ethereum::EthPrimitives;
use reth_ethereum::evm::revm::db::BundleState;
use reth_ethereum::primitives::AlloyBlockHeader;
use reth_ethereum::provider::Chain;
use silt_wire::{AccountChange, BlockDiff, HeadLite, LogLite, ReceiptLite, Word};

fn word(v: &B256) -> Word {
    v.0
}

fn word_u256(v: &U256) -> Word {
    v.to_be_bytes()
}

type Accounts = Vec<(silt_wire::Addr, Option<AccountChange>)>;
type Slots = Vec<(silt_wire::Addr, Word, Word)>;
type Codes = Vec<(Word, Vec<u8>)>;

/// Folds the bundle's account/storage/code changes (cumulative over the
/// span already, last-write-wins by construction).
pub fn fold_bundle(bundle: &BundleState) -> (Accounts, Slots, Codes) {
    let mut accounts = Vec::with_capacity(bundle.state.len());
    let mut storage = Vec::new();
    for (addr, acct) in &bundle.state {
        match &acct.info {
            None => accounts.push((addr.0.0, None)), // destroyed (reth-notes)
            Some(info) => {
                accounts.push((
                    addr.0.0,
                    Some(AccountChange {
                        balance: word_u256(&info.balance),
                        nonce: info.nonce,
                        code_hash: word(&info.code_hash),
                    }),
                ));
                // Present values, including zero writes ("cleared"). An
                // unchanged value upserts harmlessly; coherence only needs
                // the post-state to be right.
                for (slot, value) in &acct.storage {
                    storage.push((addr.0.0, word_u256(slot), word_u256(&value.present_value())));
                }
            }
        }
    }
    let code = bundle
        .contracts
        .iter()
        .map(|(hash, bytecode)| (word(hash), bytecode.original_bytes().to_vec()))
        .collect();
    (accounts, storage, code)
}

/// Maps a committed chain segment into one wire diff.
pub fn diff_from_chain(chain: &Chain<EthPrimitives>) -> eyre::Result<BlockDiff> {
    let outcome = chain.execution_outcome();
    let (accounts, storage, code) = fold_bundle(&outcome.bundle);

    let mut heads = Vec::new();
    for header in chain.headers() {
        heads.push(HeadLite {
            number: header.number(),
            hash: word(&header.hash()),
            parent_hash: word(&header.parent_hash()),
            timestamp: header.timestamp(),
            basefee: word_u256(&U256::from(header.base_fee_per_gas().unwrap_or(0))),
            gas_limit: header.gas_limit(),
            coinbase: header.beneficiary().0.0,
            prevrandao: header.mix_hash().map(|h| word(&h)).unwrap_or_default(),
        });
    }
    eyre::ensure!(!heads.is_empty(), "committed chain with no headers");

    let mut receipts = Vec::new();
    for (block, block_receipts) in chain.blocks_and_receipts() {
        let lite = block
            .body()
            .transactions
            .iter()
            .zip(block_receipts.iter())
            .map(|(tx, r)| ReceiptLite {
                tx_hash: word(tx.tx_hash()),
                success: r.success,
                cumulative_gas_used: r.cumulative_gas_used,
                logs: r
                    .logs
                    .iter()
                    .map(|log| LogLite {
                        address: log.address.0.0,
                        topics: log.topics().iter().map(word).collect(),
                        data: log.data.data.to_vec(),
                    })
                    .collect(),
            })
            .collect();
        receipts.push((block.number(), word(&block.hash()), lite));
    }

    let first = heads.first().expect("checked non-empty");
    let last = heads.last().expect("checked non-empty");
    Ok(BlockDiff {
        span_start: first.number,
        number: last.number,
        hash: last.hash,
        parent_hash: first.parent_hash,
        heads,
        accounts,
        storage,
        code,
        receipts,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::Address;
    use reth_ethereum::evm::revm::db::states::StorageSlot;
    use reth_ethereum::evm::revm::db::{AccountStatus, BundleAccount};
    use reth_ethereum::evm::revm::state::AccountInfo;

    fn acct(balance: u64, nonce: u64, slots: &[(u64, u64)]) -> BundleAccount {
        BundleAccount {
            info: Some(AccountInfo {
                balance: U256::from(balance),
                nonce,
                ..Default::default()
            }),
            original_info: None,
            storage: slots
                .iter()
                .map(|(k, v)| {
                    (
                        U256::from(*k),
                        StorageSlot {
                            previous_or_original_value: U256::ZERO,
                            present_value: U256::from(*v),
                        },
                    )
                })
                .collect(),
            status: AccountStatus::Changed,
        }
    }

    /// PLAN G1.1: destroyed account, zero-value write, and recreated state
    /// all fold into the shapes the consumer expects.
    #[test]
    fn fold_destroyed_zero_write_and_change() {
        let mut bundle = BundleState::default();
        let alive = Address::with_last_byte(1);
        let dead = Address::with_last_byte(2);
        bundle.state.insert(alive, acct(7, 3, &[(5, 0), (6, 42)])); // slot 5 cleared
        bundle.state.insert(
            dead,
            BundleAccount {
                info: None,
                original_info: None,
                storage: Default::default(),
                status: AccountStatus::Destroyed,
            },
        );

        let (accounts, storage, code) = fold_bundle(&bundle);
        assert!(code.is_empty());
        assert_eq!(accounts.len(), 2);
        let dead_entry = accounts.iter().find(|(a, _)| a[19] == 2).unwrap();
        assert!(dead_entry.1.is_none(), "destroyed account maps to None");
        let alive_entry = accounts.iter().find(|(a, _)| a[19] == 1).unwrap();
        let change = alive_entry.1.as_ref().unwrap();
        assert_eq!(change.nonce, 3);
        assert_eq!(U256::from_be_bytes(change.balance), U256::from(7));

        assert_eq!(storage.len(), 2, "zero write travels as a real write");
        let cleared = storage
            .iter()
            .find(|(_, k, _)| U256::from_be_bytes(*k) == U256::from(5))
            .unwrap();
        assert_eq!(cleared.2, [0u8; 32]);
        let written = storage
            .iter()
            .find(|(_, k, _)| U256::from_be_bytes(*k) == U256::from(6))
            .unwrap();
        assert_eq!(U256::from_be_bytes(written.2), U256::from(42));
    }
}
