//! `Chain` -> `silt_wire::BlockDiff` (PLAN T1.2, docs/reth-notes.md).
//!
//! Multi-block segments fold into one cumulative diff (D6): the bundle
//! state reth hands us is already cumulative over the segment, receipts
//! and headers stay per block inside the frame.
//!
//! Long segments (a node catching up after a restart imports hundreds of
//! blocks per notification) are capped: [`diffs_from_chain`] splits them
//! into diffs of at most `max_span` blocks, each carrying only the keys its
//! own blocks touched (from the bundle's per-block reverts), with values at
//! its last block (the bundle reverted to it).

use std::collections::{BTreeMap, BTreeSet, HashSet};

use alloy_primitives::{Address, B256, U256};
use reth_ethereum::EthPrimitives;
use reth_ethereum::primitives::AlloyBlockHeader;
use reth_ethereum::provider::{Chain, ExecutionOutcome};
use reth_ethereum::trie::bundle_state::BundleState;
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
    let (accounts, storage, code) = fold_bundle(&chain.execution_outcome().bundle);
    let (heads, receipts) = heads_and_receipts(chain)?;
    assemble(heads, receipts, accounts, storage, code)
}

/// Maps a committed segment into diffs of at most `max_span` blocks
/// (`0` = no cap: one diff, D6). Falls back to one diff when the segment
/// fits, or when its reverts don't account for every changed account.
pub fn diffs_from_chain(
    chain: &Chain<EthPrimitives>,
    max_span: u64,
) -> eyre::Result<Vec<BlockDiff>> {
    let Some(chunks) = fold_chunks(chain.execution_outcome(), max_span) else {
        return Ok(vec![diff_from_chain(chain)?]);
    };
    let (heads, receipts) = heads_and_receipts(chain)?;
    chunks
        .into_iter()
        .map(|c| {
            let range = c.start..=c.end;
            assemble(
                heads
                    .iter()
                    .filter(|h| range.contains(&h.number))
                    .cloned()
                    .collect(),
                receipts
                    .iter()
                    .filter(|(n, _, _)| range.contains(n))
                    .cloned()
                    .collect(),
                c.accounts,
                c.storage,
                c.code,
            )
        })
        .collect()
}

/// Blocks `start..=end` of a capped segment: the keys they touched, with
/// their values at `end`.
#[derive(Debug)]
pub struct Chunk {
    pub start: u64,
    pub end: u64,
    pub accounts: Accounts,
    pub storage: Slots,
    pub code: Codes,
}

/// Splits a segment's outcome into chunks of at most `max_span` blocks,
/// last chunk first (reverting the bundle as it goes), returned in block
/// order; the first chunk holds the remainder. `None` when there is
/// nothing to split, or when the per-block reverts don't cover every
/// changed account (splitting would then drop keys; the caller folds the
/// whole segment instead).
pub fn fold_chunks<T: Clone>(outcome: &ExecutionOutcome<T>, max_span: u64) -> Option<Vec<Chunk>> {
    let blocks = outcome.len() as u64;
    if max_span == 0 || blocks <= max_span || outcome.bundle.reverts.len() as u64 != blocks {
        return None;
    }
    let covered: HashSet<Address> = outcome
        .bundle
        .reverts
        .iter()
        .flatten()
        .map(|(a, _)| *a)
        .collect();
    if outcome
        .bundle
        .state
        .iter()
        .any(|(a, acct)| !acct.status.is_not_modified() && !covered.contains(a))
    {
        return None;
    }

    let first = outcome.first_block();
    let mut outcome = outcome.clone();
    let mut end = outcome.last_block();
    let mut chunks = Vec::new();
    loop {
        let start = end.saturating_sub(max_span - 1).max(first);
        chunks.push(chunk(&outcome.bundle, first, start, end));
        if start == first {
            break;
        }
        if !outcome.revert_to(start - 1) {
            return None;
        }
        end = start - 1;
    }
    chunks.reverse();
    Some(chunks)
}

/// One chunk from a bundle whose plain state is at `end`.
fn chunk(bundle: &BundleState, first: u64, start: u64, end: u64) -> Chunk {
    let mut touched: BTreeMap<Address, BTreeSet<U256>> = BTreeMap::new();
    for b in start..=end {
        for (addr, revert) in &bundle.reverts[(b - first) as usize] {
            touched
                .entry(*addr)
                .or_default()
                .extend(revert.storage.keys().copied());
        }
    }
    let (mut accounts, mut storage, mut code_hashes) = (Vec::new(), Vec::new(), BTreeSet::new());
    for (addr, slots) in touched {
        let acct = bundle.state.get(&addr);
        match acct.and_then(|a| a.info.as_ref().map(|info| (a, info))) {
            None => accounts.push((addr.0.0, None)), // destroyed (or never lived)
            Some((acct, info)) => {
                accounts.push((
                    addr.0.0,
                    Some(AccountChange {
                        balance: word_u256(&info.balance),
                        nonce: info.nonce,
                        code_hash: word(&info.code_hash),
                    }),
                ));
                code_hashes.insert(info.code_hash);
                for slot in slots {
                    // A slot the reverts name but the state no longer holds
                    // was removed: it reads as zero.
                    let value = acct
                        .storage
                        .get(&slot)
                        .map_or(U256::ZERO, |s| s.present_value());
                    storage.push((addr.0.0, word_u256(&slot), word_u256(&value)));
                }
            }
        }
    }
    let code = code_hashes
        .iter()
        .filter_map(|h| {
            bundle
                .contracts
                .get(h)
                .map(|b| (word(h), b.original_bytes().to_vec()))
        })
        .collect();
    Chunk {
        start,
        end,
        accounts,
        storage,
        code,
    }
}

type Receipts = Vec<(u64, Word, Vec<ReceiptLite>)>;

fn heads_and_receipts(chain: &Chain<EthPrimitives>) -> eyre::Result<(Vec<HeadLite>, Receipts)> {
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

    Ok((heads, receipts))
}

fn assemble(
    heads: Vec<HeadLite>,
    receipts: Receipts,
    accounts: Accounts,
    storage: Slots,
    code: Codes,
) -> eyre::Result<BlockDiff> {
    eyre::ensure!(!heads.is_empty(), "diff with no headers");
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
    // evm2's `AccountInfo`; reth re-exports it only from `reth_evm::cached`.
    use reth_ethereum::evm::primitives::cached::AccountInfo;
    use reth_ethereum::trie::bundle_state::{AccountStatus, BundleAccount, StorageSlot};

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

    fn info(balance: u64, nonce: u64) -> AccountInfo {
        AccountInfo {
            balance: U256::from(balance),
            nonce,
            ..Default::default()
        }
    }

    /// Blocks 100..=102:
    /// - 100: A balance 1 -> 2, slot 1 = 5;
    /// - 101: B created (balance 7, nonce 1), A slot 2 = 9;
    /// - 102: A balance 2 -> 3, slot 1 cleared.
    fn three_blocks() -> ExecutionOutcome<()> {
        let (a, b) = (Address::with_last_byte(0xA), Address::with_last_byte(0xB));
        let s = U256::from;
        let bundle = BundleState::new(
            [
                (
                    a,
                    Some(info(1, 0)),
                    Some(info(3, 0)),
                    [(s(1), (s(0), s(0))), (s(2), (s(0), s(9)))]
                        .into_iter()
                        .collect(),
                ),
                (b, None, Some(info(7, 1)), Default::default()),
            ],
            vec![
                vec![(a, Some(Some(info(1, 0))), vec![(s(1), s(0))])],
                vec![(b, Some(None), vec![]), (a, None, vec![(s(2), s(0))])],
                vec![(a, Some(Some(info(2, 0))), vec![(s(1), s(5))])],
            ],
            [],
        );
        ExecutionOutcome::new(bundle, vec![vec![]; 3], 100, vec![])
    }

    type Keys = BTreeMap<(Word, Option<Word>), Option<(u64, Word)>>;

    /// Accounts as `(addr, None) -> Some((nonce, balance))` (None =
    /// destroyed), slots as `(addr, Some(slot)) -> Some((0, value))`.
    fn keys(accounts: &Accounts, storage: &Slots) -> Keys {
        let mut m = BTreeMap::new();
        for (a, c) in accounts {
            let mut k = [0u8; 32];
            k[12..].copy_from_slice(a);
            m.insert((k, None), c.as_ref().map(|c| (c.nonce, c.balance)));
        }
        for (a, slot, v) in storage {
            let mut k = [0u8; 32];
            k[12..].copy_from_slice(a);
            m.insert((k, Some(*slot)), Some((0, *v)));
        }
        m
    }

    fn balance(v: u64) -> Word {
        U256::from(v).to_be_bytes()
    }

    #[test]
    fn chunks_carry_their_own_keys_at_their_last_block() {
        let chunks = fold_chunks(&three_blocks(), 1).expect("3 blocks > 1");
        let ranges: Vec<_> = chunks.iter().map(|c| (c.start, c.end)).collect();
        assert_eq!(ranges, vec![(100, 100), (101, 101), (102, 102)]);

        let (a, b) = (0xA, 0xB);
        let acct = |c: &Chunk, last: u8| c.accounts.iter().find(|(x, _)| x[19] == last).cloned();
        let slot = |c: &Chunk, n: u64| {
            c.storage
                .iter()
                .find(|(_, k, _)| U256::from_be_bytes(*k) == U256::from(n))
                .map(|(_, _, v)| U256::from_be_bytes(*v))
        };
        // 100: A at balance 2, slot 1 = 5; nothing of B or slot 2.
        assert_eq!(acct(&chunks[0], a).unwrap().1.unwrap().balance, balance(2));
        assert!(acct(&chunks[0], b).is_none());
        assert_eq!(slot(&chunks[0], 1), Some(U256::from(5)));
        assert_eq!(slot(&chunks[0], 2), None);
        // 101: B created; A only for its slot 2 write (balance still 2).
        assert_eq!(acct(&chunks[1], b).unwrap().1.unwrap().nonce, 1);
        assert_eq!(acct(&chunks[1], a).unwrap().1.unwrap().balance, balance(2));
        assert_eq!(slot(&chunks[1], 2), Some(U256::from(9)));
        assert_eq!(slot(&chunks[1], 1), None);
        // 102: A at balance 3, slot 1 cleared (a real zero write).
        assert_eq!(acct(&chunks[2], a).unwrap().1.unwrap().balance, balance(3));
        assert_eq!(slot(&chunks[2], 1), Some(U256::ZERO));
        assert!(acct(&chunks[2], b).is_none());
    }

    #[test]
    fn chunks_applied_in_order_equal_the_whole_fold() {
        let outcome = three_blocks();
        let (accounts, storage, _) = fold_bundle(&outcome.bundle);
        let whole = keys(&accounts, &storage);
        for max_span in [1, 2] {
            let chunks = fold_chunks(&outcome, max_span).unwrap();
            assert!(chunks.iter().all(|c| c.end - c.start < max_span));
            assert_eq!(chunks.first().unwrap().start, 100);
            assert_eq!(chunks.last().unwrap().end, 102);
            let mut applied = Keys::new();
            for c in &chunks {
                applied.extend(keys(&c.accounts, &c.storage));
            }
            assert_eq!(applied, whole, "max_span {max_span}");
        }
        let two = fold_chunks(&outcome, 2).unwrap();
        let ranges: Vec<_> = two.iter().map(|c| (c.start, c.end)).collect();
        assert_eq!(
            ranges,
            vec![(100, 100), (101, 102)],
            "the remainder comes first"
        );
    }

    #[test]
    fn no_split_when_it_fits_is_off_or_reverts_fall_short() {
        let outcome = three_blocks();
        assert!(fold_chunks(&outcome, 3).is_none(), "fits");
        assert!(fold_chunks(&outcome, 0).is_none(), "cap off");
        let mut missing = three_blocks();
        missing.bundle.state.insert(
            Address::with_last_byte(0xC),
            acct(1, 1, &[]), // changed, but no block's reverts name it
        );
        assert!(fold_chunks(&missing, 1).is_none(), "would drop 0xC");
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
