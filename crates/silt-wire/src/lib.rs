//! silt wire protocol: node-agnostic state-diff frames + codec + framing.
//!
//! Dependency-free types (PLAN.md D4): raw byte arrays and integers only.
//! Adapters on each side map to native types (alloy in the wrapper, yevm or
//! anything else in consumers). Frames are postcard-encoded, length-prefixed
//! (u32 LE). Version negotiation is exact-match (v0, D1). The lookup fast
//! path (D9) shares the handshake and length prefix but uses a fixed
//! little-endian layout instead of postcard: see [`lookup`].

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub mod lookup;

pub const MAGIC: [u8; 4] = *b"SILT";
/// Covers the feed and the lookup protocol (PLAN D9): `0` at D9, `1` since
/// D10 added op `Proof` and status `TooLarge`, `2` since D12 changed
/// `Subscribe` and `Reorg`. Not bumped any more for now: every consumer is
/// ours and ships from the same commit as the node, so frames change
/// freely. Versioning resumes once a consumer we don't deploy exists.
pub const VERSION: u16 = 2;
/// Upper bound on a single frame; a mainnet block diff is ~100-400 KB, so
/// this is generous headroom while still rejecting garbage lengths.
pub const MAX_FRAME_BYTES: u32 = 64 * 1024 * 1024;

pub type Addr = [u8; 20];
pub type Word = [u8; 32];

/// keccak256 of the empty string: the code hash of every account without
/// code. Both protocols use it for "no code", never zero.
pub const KECCAK_EMPTY: Word = [
    0xc5, 0xd2, 0x46, 0x01, 0x86, 0xf7, 0x23, 0x3c, 0x92, 0x7e, 0x7d, 0xb2, 0xdc, 0xc7, 0x03, 0xc0,
    0xe5, 0x00, 0xb6, 0x53, 0xca, 0x82, 0x27, 0x3b, 0x7b, 0xfa, 0xd8, 0x04, 0x5d, 0x85, 0xa4, 0x70,
];

/// Enough header to pin RPC reads and build a next-block sim environment.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeadLite {
    pub number: u64,
    pub hash: Word,
    pub parent_hash: Word,
    pub timestamp: u64,
    pub basefee: Word,
    pub gas_limit: u64,
    pub coinbase: Addr,
    pub prevrandao: Word,
}

/// Post-state of a changed account. `code_hash` refers into
/// [`BlockDiff::code`] when the code is new this block.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountChange {
    pub balance: Word,
    pub nonce: u64,
    pub code_hash: Word,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogLite {
    pub address: Addr,
    pub topics: Vec<Word>,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReceiptLite {
    pub tx_hash: Word,
    pub success: bool,
    pub cumulative_gas_used: u64,
    pub logs: Vec<LogLite>,
}

/// One block's (or folded span's) state increment, node-agnostic (PLAN §2).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockDiff {
    /// First block of the span; == `number` for the single-block common case.
    pub span_start: u64,
    /// Tip of the span.
    pub number: u64,
    pub hash: Word,
    /// Parent of `span_start` — the chain-continuity check (PLAN §E).
    pub parent_hash: Word,
    /// One per block in the span, ascending.
    pub heads: Vec<HeadLite>,
    /// Account-level post-states; `None` = destroyed (remove account AND
    /// all its storage).
    pub accounts: Vec<(Addr, Option<AccountChange>)>,
    /// (owner, slot, new value). Presence means written; zero values are
    /// real writes ("cleared"), absence means untouched.
    pub storage: Vec<(Addr, Word, Word)>,
    /// New bytecode deployed in the span, keyed by code hash (deduped).
    pub code: Vec<(Word, Vec<u8>)>,
    /// Receipts per block in the span: (number, block hash, receipts).
    pub receipts: Vec<(u64, Word, Vec<ReceiptLite>)>,
    /// Read touches (PLAN D13): accounts the span's blocks read and did not
    /// write, at their value at `number` (unchanged by the span). Empty when
    /// the node's host doesn't report reads.
    pub read_accounts: Vec<(Addr, AccountChange)>,
    /// Slots the span read and did not write: (owner, slot, value at
    /// `number`). Never a slot of an account the span destroyed.
    pub read_storage: Vec<(Addr, Word, Word)>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Frame {
    /// First frame in both directions; any mismatch => `Refused` + close.
    Hello {
        magic: [u8; 4],
        version: u16,
        chain_id: u64,
        genesis_hash: Word,
        tip: u64,
    },
    Refused {
        reason: String,
    },
    /// Client -> server. `from: None` = from the current tip.
    /// `parent_hash` is the hash of block `from - 1` (the client's tip): a
    /// tip that a reorg dropped is answered with `Reorg` first (PLAN D12).
    Subscribe {
        from: Option<u64>,
        parent_hash: Option<Word>,
    },
    Diff(BlockDiff),
    /// Everything above `fork_number` was replaced (or reverted); the new
    /// side's diffs follow from `fork_number + 1`. Consumers roll back to
    /// the fork, or drop to cold if they can't (PLAN D12).
    Reorg {
        fork_number: u64,
        fork_hash: Word,
    },
    Heartbeat {
        height: u64,
        spool_depth: u32,
    },
    /// Answer to an unserviceable `Subscribe{from}`.
    GapTooOld {
        oldest_available: u64,
    },
}

impl Frame {
    pub fn encode(&self) -> eyre::Result<Vec<u8>> {
        Ok(postcard::to_allocvec(self)?)
    }

    /// Decodes a frame. D13 appended the read touches to `BlockDiff`, the
    /// last thing in a `Diff` frame: an older decoder ignores them (postcard
    /// doesn't check for trailing bytes), and this decoder reads a `Diff`
    /// from an older node through [`legacy`], with no read touches.
    pub fn decode(bytes: &[u8]) -> eyre::Result<Self> {
        match postcard::from_bytes(bytes) {
            Ok(frame) => Ok(frame),
            Err(err) => match postcard::from_bytes::<legacy::Frame>(bytes) {
                Ok(frame) => Ok(frame.into()),
                Err(_) => Err(err.into()),
            },
        }
    }
}

/// The frame layout before D13 (2026-11-18): `BlockDiff` without the read
/// touches. Kept so consumers built after D13 still read a node built
/// before it; drop once every node has D13.
pub mod legacy {
    use super::*;

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct BlockDiff {
        pub span_start: u64,
        pub number: u64,
        pub hash: Word,
        pub parent_hash: Word,
        pub heads: Vec<HeadLite>,
        pub accounts: Vec<(Addr, Option<AccountChange>)>,
        pub storage: Vec<(Addr, Word, Word)>,
        pub code: Vec<(Word, Vec<u8>)>,
        pub receipts: Vec<(u64, Word, Vec<ReceiptLite>)>,
    }

    /// Variants in the same order as [`super::Frame`] (postcard encodes
    /// the variant index).
    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub enum Frame {
        Hello {
            magic: [u8; 4],
            version: u16,
            chain_id: u64,
            genesis_hash: Word,
            tip: u64,
        },
        Refused {
            reason: String,
        },
        Subscribe {
            from: Option<u64>,
            parent_hash: Option<Word>,
        },
        Diff(BlockDiff),
        Reorg {
            fork_number: u64,
            fork_hash: Word,
        },
        Heartbeat {
            height: u64,
            spool_depth: u32,
        },
        GapTooOld {
            oldest_available: u64,
        },
    }

    impl From<BlockDiff> for super::BlockDiff {
        fn from(d: BlockDiff) -> Self {
            Self {
                span_start: d.span_start,
                number: d.number,
                hash: d.hash,
                parent_hash: d.parent_hash,
                heads: d.heads,
                accounts: d.accounts,
                storage: d.storage,
                code: d.code,
                receipts: d.receipts,
                read_accounts: Vec::new(),
                read_storage: Vec::new(),
            }
        }
    }

    impl From<Frame> for super::Frame {
        fn from(f: Frame) -> Self {
            match f {
                Frame::Hello {
                    magic,
                    version,
                    chain_id,
                    genesis_hash,
                    tip,
                } => Self::Hello {
                    magic,
                    version,
                    chain_id,
                    genesis_hash,
                    tip,
                },
                Frame::Refused { reason } => Self::Refused { reason },
                Frame::Subscribe { from, parent_hash } => Self::Subscribe { from, parent_hash },
                Frame::Diff(d) => Self::Diff(d.into()),
                Frame::Reorg {
                    fork_number,
                    fork_hash,
                } => Self::Reorg {
                    fork_number,
                    fork_hash,
                },
                Frame::Heartbeat {
                    height,
                    spool_depth,
                } => Self::Heartbeat {
                    height,
                    spool_depth,
                },
                Frame::GapTooOld { oldest_available } => Self::GapTooOld { oldest_available },
            }
        }
    }
}

/// Writes one length-prefixed frame.
pub async fn write_frame<W: AsyncWrite + Unpin>(w: &mut W, frame: &Frame) -> eyre::Result<()> {
    let bytes = frame.encode()?;
    eyre::ensure!(
        bytes.len() as u64 <= MAX_FRAME_BYTES as u64,
        "frame too large"
    );
    w.write_all(&(bytes.len() as u32).to_le_bytes()).await?;
    w.write_all(&bytes).await?;
    w.flush().await?;
    Ok(())
}

/// Reads one length-prefixed frame. `Ok(None)` on clean EOF at a frame
/// boundary; errors on torn frames, oversized lengths, or decode failures.
pub async fn read_frame<R: AsyncRead + Unpin>(r: &mut R) -> eyre::Result<Option<Frame>> {
    let mut len = [0u8; 4];
    match r.read_exact(&mut len).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let len = u32::from_le_bytes(len);
    eyre::ensure!(len <= MAX_FRAME_BYTES, "frame length {len} exceeds cap");
    let mut buf = vec![0u8; len as usize];
    r.read_exact(&mut buf).await?;
    Ok(Some(Frame::decode(&buf)?))
}

pub fn hello(chain_id: u64, genesis_hash: Word, tip: u64) -> Frame {
    Frame::Hello {
        magic: MAGIC,
        version: VERSION,
        chain_id,
        genesis_hash,
        tip,
    }
}

/// Validates a peer's `Hello` against ours; returns the refusal reason if
/// the peers must not talk (exact-version match in v0).
pub fn hello_mismatch(ours: &Frame, theirs: &Frame) -> Option<String> {
    let (
        Frame::Hello {
            magic: m1,
            version: v1,
            chain_id: c1,
            genesis_hash: g1,
            ..
        },
        Frame::Hello {
            magic: m2,
            version: v2,
            chain_id: c2,
            genesis_hash: g2,
            ..
        },
    ) = (ours, theirs)
    else {
        return Some("expected Hello".into());
    };
    if m1 != m2 {
        return Some("bad magic".into());
    }
    if v1 != v2 {
        return Some(format!("version mismatch: ours {v1}, theirs {v2}"));
    }
    if c1 != c2 {
        return Some(format!("chain id mismatch: ours {c1}, theirs {c2}"));
    }
    if g1 != g2 {
        return Some("genesis hash mismatch".into());
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tiny deterministic PRNG (xorshift64*) — no rand dependency.
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x >> 12;
            x ^= x << 25;
            x ^= x >> 27;
            self.0 = x;
            x.wrapping_mul(0x2545F4914F6CDD1D)
        }
        fn word(&mut self) -> Word {
            let mut w = [0u8; 32];
            for c in w.chunks_mut(8) {
                c.copy_from_slice(&self.next().to_le_bytes());
            }
            w
        }
        fn addr(&mut self) -> Addr {
            let w = self.word();
            w[..20].try_into().unwrap()
        }
        fn bytes(&mut self, max: usize) -> Vec<u8> {
            let n = (self.next() as usize) % (max + 1);
            (0..n).map(|_| self.next() as u8).collect()
        }
    }

    fn random_diff(rng: &mut Rng) -> BlockDiff {
        let number = rng.next() % 1_000_000;
        let span = rng.next() % 3;
        let heads = (0..=span)
            .map(|i| HeadLite {
                number: number - span + i,
                hash: rng.word(),
                parent_hash: rng.word(),
                timestamp: rng.next(),
                basefee: rng.word(),
                gas_limit: rng.next(),
                coinbase: rng.addr(),
                prevrandao: rng.word(),
            })
            .collect();
        BlockDiff {
            span_start: number - span,
            number,
            hash: rng.word(),
            parent_hash: rng.word(),
            heads,
            accounts: (0..rng.next() % 8)
                .map(|i| {
                    let change = (i % 3 != 0).then(|| AccountChange {
                        balance: rng.word(),
                        nonce: rng.next(),
                        code_hash: rng.word(),
                    });
                    (rng.addr(), change)
                })
                .collect(),
            storage: (0..rng.next() % 16)
                .map(|_| (rng.addr(), rng.word(), rng.word()))
                .collect(),
            code: (0..rng.next() % 2)
                .map(|_| (rng.word(), rng.bytes(512)))
                .collect(),
            receipts: (0..rng.next() % 3)
                .map(|_| {
                    let logs = (0..rng.next() % 4)
                        .map(|_| LogLite {
                            address: rng.addr(),
                            topics: (0..rng.next() % 4).map(|_| rng.word()).collect(),
                            data: rng.bytes(64),
                        })
                        .collect();
                    (
                        rng.next(),
                        rng.word(),
                        vec![ReceiptLite {
                            tx_hash: rng.word(),
                            success: rng.next().is_multiple_of(2),
                            cumulative_gas_used: rng.next(),
                            logs,
                        }],
                    )
                })
                .collect(),
            read_accounts: (0..rng.next() % 6)
                .map(|_| {
                    (
                        rng.addr(),
                        AccountChange {
                            balance: rng.word(),
                            nonce: rng.next(),
                            code_hash: rng.word(),
                        },
                    )
                })
                .collect(),
            read_storage: (0..rng.next() % 10)
                .map(|_| (rng.addr(), rng.word(), rng.word()))
                .collect(),
        }
    }

    #[test]
    fn roundtrip_10k_random_frames() {
        let mut rng = Rng(0x5117_5117_5117_5117);
        for i in 0..10_000u64 {
            let frame = match i % 6 {
                0 => hello(rng.next(), rng.word(), rng.next()),
                1 => Frame::Subscribe {
                    from: (i.is_multiple_of(2)).then(|| rng.next()),
                    parent_hash: (i.is_multiple_of(4)).then(|| rng.word()),
                },
                2 => Frame::Diff(random_diff(&mut rng)),
                3 => Frame::Reorg {
                    fork_number: rng.next(),
                    fork_hash: rng.word(),
                },
                4 => Frame::Heartbeat {
                    height: rng.next(),
                    spool_depth: rng.next() as u32,
                },
                _ => Frame::GapTooOld {
                    oldest_available: rng.next(),
                },
            };
            let decoded = Frame::decode(&frame.encode().unwrap()).unwrap();
            assert_eq!(frame, decoded, "iteration {i}");
        }
    }

    /// D13 compatibility, both ways, without a version bump: a decoder
    /// that predates the read touches reads a new `Diff` frame (trailing
    /// bytes ignored), and the new decoder reads an old one (no touches).
    #[test]
    fn read_touches_are_compatible_both_ways() {
        let mut rng = Rng(0xD13);
        let mut with_touches = 0;
        for _ in 0..2_000 {
            let new = random_diff(&mut rng);
            with_touches +=
                usize::from(!new.read_accounts.is_empty() || !new.read_storage.is_empty());
            let bytes = Frame::Diff(new.clone()).encode().unwrap();

            // Old consumer, new node.
            let old: legacy::Frame = postcard::from_bytes(&bytes).unwrap();
            let legacy::Frame::Diff(old) = old else {
                panic!("decoded as another variant")
            };
            let mut expect = new.clone();
            expect.read_accounts.clear();
            expect.read_storage.clear();
            assert_eq!(BlockDiff::from(old.clone()), expect);

            // New consumer, old node.
            let old_bytes = postcard::to_allocvec(&legacy::Frame::Diff(old)).unwrap();
            assert_eq!(Frame::decode(&old_bytes).unwrap(), Frame::Diff(expect));
        }
        assert!(with_touches > 1_000, "the property saw read touches");

        // Frames other than Diff are byte-identical in both layouts.
        for frame in [
            hello(1, [7; 32], 42),
            Frame::Heartbeat {
                height: 9,
                spool_depth: 3,
            },
            Frame::GapTooOld {
                oldest_available: 5,
            },
        ] {
            let bytes = frame.encode().unwrap();
            let old: legacy::Frame = postcard::from_bytes(&bytes).unwrap();
            assert_eq!(postcard::to_allocvec(&old).unwrap(), bytes);
        }
    }

    /// Fixture: the three sharp corners travel intact (PLAN T0.3).
    #[test]
    fn fixture_destroyed_account_zero_write_span() {
        let diff = BlockDiff {
            span_start: 100,
            number: 101, // 2-block folded span
            hash: [1; 32],
            parent_hash: [0; 32],
            heads: vec![
                HeadLite {
                    number: 100,
                    hash: [2; 32],
                    parent_hash: [0; 32],
                    timestamp: 1,
                    basefee: [0; 32],
                    gas_limit: 30_000_000,
                    coinbase: [9; 20],
                    prevrandao: [3; 32],
                },
                HeadLite {
                    number: 101,
                    hash: [1; 32],
                    parent_hash: [2; 32],
                    timestamp: 13,
                    basefee: [0; 32],
                    gas_limit: 30_000_000,
                    coinbase: [9; 20],
                    prevrandao: [4; 32],
                },
            ],
            accounts: vec![([0xAA; 20], None)], // destroyed
            storage: vec![([0xBB; 20], [5; 32], [0; 32])], // zero write = cleared
            code: vec![],
            receipts: vec![(100, [2; 32], vec![]), (101, [1; 32], vec![])],
            read_accounts: Vec::new(),
            read_storage: Vec::new(),
        };
        let back = Frame::decode(&Frame::Diff(diff.clone()).encode().unwrap()).unwrap();
        assert_eq!(back, Frame::Diff(diff));
    }

    #[tokio::test]
    async fn framing_roundtrip_and_eof() {
        let (mut a, mut b) = tokio::io::duplex(1 << 20);
        let f1 = hello(1, [7; 32], 42);
        let f2 = Frame::Heartbeat {
            height: 42,
            spool_depth: 3,
        };
        write_frame(&mut a, &f1).await.unwrap();
        write_frame(&mut a, &f2).await.unwrap();
        drop(a);
        assert_eq!(read_frame(&mut b).await.unwrap(), Some(f1));
        assert_eq!(read_frame(&mut b).await.unwrap(), Some(f2));
        assert_eq!(read_frame(&mut b).await.unwrap(), None); // clean EOF
    }

    #[tokio::test]
    async fn framing_rejects_oversized_length() {
        let (mut a, mut b) = tokio::io::duplex(64);
        tokio::spawn(async move {
            let _ = tokio::io::AsyncWriteExt::write_all(&mut a, &u32::MAX.to_le_bytes()).await;
        });
        assert!(read_frame(&mut b).await.is_err());
    }

    #[test]
    fn hello_mismatch_matrix() {
        let ours = hello(1, [7; 32], 10);
        assert!(hello_mismatch(&ours, &hello(1, [7; 32], 99)).is_none()); // tip may differ
        assert!(hello_mismatch(&ours, &hello(2, [7; 32], 10)).is_some());
        assert!(hello_mismatch(&ours, &hello(1, [8; 32], 10)).is_some());
        let subscribe = Frame::Subscribe {
            from: None,
            parent_hash: None,
        };
        assert!(hello_mismatch(&ours, &subscribe).is_some());
        // Any other version is refused: v0 (D9) since D10, v1 since D12.
        for version in [VERSION + 1, 1, 0] {
            let bad_version = Frame::Hello {
                magic: MAGIC,
                version,
                chain_id: 1,
                genesis_hash: [7; 32],
                tip: 10,
            };
            assert!(hello_mismatch(&ours, &bad_version).is_some(), "v{version}");
        }
    }
}
