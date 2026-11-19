//! Feed client: connect, validate, apply, emit head events (PLAN T2.1/T2.5).
//!
//! Generic over the stream so tests drive it with `tokio::io::duplex`.
//! A `Reorg` frame rolls the store back to the fork and emits `Reorg`
//! (PLAN D12); the new side then arrives as ordinary diffs. Any other chain
//! break, and a reorg the store can't roll back, drops to cold (D3): the
//! store is cleared, `ChainBreak` is emitted, and consuming continues (the
//! next diff seeds a fresh carry — with `Subscribe{None}` semantics the
//! first diff is accepted without a parent check because the store is
//! empty).
//!
//! A node restart is not a break: the new process doesn't know what the old
//! one published, so reth's replay can re-deliver a diff the client already
//! holds, or a folded span that straddles its tip ([`continuity`]).

use std::sync::Arc;
use std::time::Duration;

use silt_wire::{BlockDiff, Frame, read_frame, write_frame};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc::UnboundedSender;

use crate::HeadEvent;
use crate::store::Store;

/// How a diff relates to the carried tip.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Continuity {
    /// Starts right after the tip, on its hash — or the store is cold.
    Extends,
    /// A folded span that starts at or below the tip and passes through it
    /// on the same chain: its cumulative post-state applies cleanly.
    Overlaps,
    /// Ends at or below the tip and matches the block held there: a
    /// re-delivery, skip it.
    Stale,
    /// Anything else: a gap, a fork, or an unknown history.
    Break,
}

/// Classifies `diff` against the store's tip and recent heads. After a
/// node restart the new process republishes from reth's last acked height,
/// so a subscriber resuming at `tip + 1` can receive a span that began
/// earlier (the server spools by span end). Applying it whole is correct:
/// it carries every key the span touched with its value at the span's end.
pub fn continuity(store: &Store, diff: &BlockDiff) -> Continuity {
    let Some((tip, tip_hash)) = store.tip() else {
        return Continuity::Extends;
    };
    if diff.span_start == tip + 1 {
        return if diff.parent_hash == tip_hash {
            Continuity::Extends
        } else {
            Continuity::Break
        };
    }
    if diff.span_start > tip + 1 {
        return Continuity::Break;
    }
    if diff.number <= tip {
        return match store.head(diff.number) {
            Some(h) if h.hash == diff.hash => Continuity::Stale,
            _ => Continuity::Break,
        };
    }
    if diff
        .heads
        .iter()
        .any(|h| h.number == tip && h.hash == tip_hash)
    {
        Continuity::Overlaps
    } else {
        Continuity::Break
    }
}

/// Pinned peer identity after the first successful handshake.
#[derive(Clone, Copy, Debug)]
pub struct FeedIdentity {
    pub chain_id: u64,
    pub genesis_hash: silt_wire::Word,
}

pub struct ClientState {
    pub store: Arc<Store>,
    pub events: UnboundedSender<HeadEvent>,
    pub identity: std::sync::Mutex<Option<FeedIdentity>>,
}

impl ClientState {
    fn cold(&self, reason: &str) {
        tracing::error!(reason, "silt-cache: chain break -> cold");
        self.store.clear();
        let _ = self.events.send(HeadEvent::ChainBreak {
            reason: reason.to_string(),
        });
    }
}

/// Runs one connection to completion. Returns `Ok(())` on clean EOF; the
/// caller reconnects with backoff. `liveness` bounds the silence between
/// frames (heartbeats arrive every ~10 s, so 30 s of silence means the
/// feed is gone).
pub async fn run_stream<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    state: &ClientState,
    liveness: Duration,
) -> eyre::Result<()> {
    // Handshake: server speaks first; we pin its identity on first contact
    // and refuse a different chain/genesis forever after.
    let server_hello = read_frame(&mut stream)
        .await?
        .ok_or_else(|| eyre::eyre!("eof before Hello"))?;
    let Frame::Hello {
        chain_id,
        genesis_hash,
        ..
    } = server_hello
    else {
        eyre::bail!("expected server Hello");
    };
    {
        let mut pinned = state.identity.lock().expect("identity lock");
        match *pinned {
            None => {
                *pinned = Some(FeedIdentity {
                    chain_id,
                    genesis_hash,
                })
            }
            Some(p) => {
                eyre::ensure!(
                    p.chain_id == chain_id && p.genesis_hash == genesis_hash,
                    "feed identity changed (chain {} -> {})",
                    p.chain_id,
                    chain_id
                );
            }
        }
    }
    write_frame(&mut stream, &silt_wire::hello(chain_id, genesis_hash, 0)).await?;
    // Our tip's hash lets the server spot a tip a reorg dropped while we
    // were away and send the fork to roll back to first (D12).
    let tip = state.store.tip();
    let subscribe = Frame::Subscribe {
        from: tip.map(|(n, _)| n + 1),
        parent_hash: tip.map(|(_, h)| h),
    };
    write_frame(&mut stream, &subscribe).await?;

    loop {
        let frame = tokio::time::timeout(liveness, read_frame(&mut stream)).await;
        let frame = match frame {
            Err(_) => eyre::bail!("feed silent for {liveness:?}"),
            Ok(frame) => frame?,
        };
        let Some(frame) = frame else { return Ok(()) }; // clean EOF
        match frame {
            Frame::Diff(diff) => {
                // Cold store (tip None): the first diff seeds the carry with
                // no parent check. Otherwise the diff must extend the tip
                // (or overlap it on the same chain, after a node restart) —
                // any break drops to cold, and the offending diff then seeds
                // the fresh carry (its own content is internally coherent).
                match continuity(&state.store, &diff) {
                    Continuity::Extends | Continuity::Overlaps => {}
                    Continuity::Stale => {
                        tracing::debug!(number = diff.number, "silt-cache: re-delivered diff");
                        continue;
                    }
                    Continuity::Break => {
                        let tip = state.store.tip().map_or(0, |(n, _)| n);
                        state.cold(&format!(
                            "diff {}..{} does not extend tip {tip}",
                            diff.span_start, diff.number
                        ));
                    }
                }
                state.store.apply(&diff);
                if let Some(head) = state.store.tip_head() {
                    let _ = state.events.send(HeadEvent::NewHead(head));
                }
            }
            Frame::Reorg {
                fork_number,
                fork_hash,
            } => {
                let tip = state.store.tip().map_or(0, |(n, _)| n);
                if state.store.rollback(fork_number, fork_hash) {
                    tracing::warn!(tip, fork_number, "silt-cache: reorg rolled back");
                    let _ = state.events.send(HeadEvent::Reorg {
                        fork_number,
                        fork_hash,
                    });
                } else {
                    state.cold(&format!(
                        "reorg to fork {fork_number} from tip {tip}: can't roll back"
                    ));
                }
            }
            Frame::GapTooOld { oldest_available } => {
                state.cold(&format!("gap: spool starts at {oldest_available}"));
            }
            Frame::Heartbeat { .. } => {}
            Frame::Refused { reason } => eyre::bail!("refused: {reason}"),
            Frame::Hello { .. } | Frame::Subscribe { .. } => {
                eyre::bail!("unexpected frame from server")
            }
        }
    }
}

/// Reconnect loop over TCP (PLAN T2.1).
pub async fn run_tcp(addr: String, state: Arc<ClientState>, liveness: Duration) {
    let mut backoff = Duration::from_millis(250);
    loop {
        match tokio::net::TcpStream::connect(&addr).await {
            Ok(stream) => {
                backoff = Duration::from_millis(250);
                match run_stream(stream, &state, liveness).await {
                    Ok(()) => tracing::info!("silt feed closed; reconnecting"),
                    Err(err) => tracing::error!(%err, "silt feed error; reconnecting"),
                }
            }
            Err(err) => tracing::info!(%err, addr, "silt feed connect failed"),
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(10));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use silt_wire::{AccountChange, BlockDiff, HeadLite};
    use tokio::sync::mpsc::unbounded_channel;

    fn head(number: u64, hash: u8, parent: u8) -> HeadLite {
        HeadLite {
            number,
            hash: [hash; 32],
            parent_hash: [parent; 32],
            timestamp: number * 12,
            basefee: [0; 32],
            gas_limit: 30_000_000,
            coinbase: [0; 20],
            prevrandao: [0; 32],
        }
    }

    fn diff(number: u64, hash: u8, parent: u8) -> BlockDiff {
        BlockDiff {
            span_start: number,
            number,
            hash: [hash; 32],
            parent_hash: [parent; 32],
            heads: vec![head(number, hash, parent)],
            accounts: vec![(
                [number as u8; 20],
                Some(AccountChange {
                    balance: [1; 32],
                    nonce: number,
                    code_hash: [0; 32],
                }),
            )],
            storage: vec![],
            code: vec![],
            receipts: vec![],
            read_accounts: Vec::new(),
            read_storage: Vec::new(),
        }
    }

    /// Mock feed end-to-end over a duplex pipe: handshake, two chained
    /// diffs, then a parent mismatch -> ChainBreak + cold, then a fresh
    /// seed diff (PLAN G2.1/G2.4 unit shape).
    #[tokio::test]
    async fn applies_validates_and_drops_to_cold() {
        let (client_io, mut server_io) = tokio::io::duplex(1 << 20);
        let (tx, mut rx) = unbounded_channel();
        let state = ClientState {
            store: Arc::new(Store::new(16)),
            events: tx,
            identity: std::sync::Mutex::new(None),
        };

        let server = tokio::spawn(async move {
            write_frame(&mut server_io, &silt_wire::hello(1, [9; 32], 10))
                .await
                .unwrap();
            let hello = read_frame(&mut server_io).await.unwrap().unwrap();
            assert!(matches!(hello, Frame::Hello { chain_id: 1, .. }));
            let sub = read_frame(&mut server_io).await.unwrap().unwrap();
            assert_eq!(
                sub,
                Frame::Subscribe {
                    from: None,
                    parent_hash: None
                },
                "cold store subscribes from tip"
            );

            write_frame(&mut server_io, &Frame::Diff(diff(10, 10, 9)))
                .await
                .unwrap();
            write_frame(&mut server_io, &Frame::Diff(diff(11, 11, 10)))
                .await
                .unwrap();
            write_frame(
                &mut server_io,
                &Frame::Heartbeat {
                    height: 11,
                    spool_depth: 2,
                },
            )
            .await
            .unwrap();
            // Parent mismatch: pretends 13 extends an unknown 12.
            write_frame(&mut server_io, &Frame::Diff(diff(13, 13, 12)))
                .await
                .unwrap();
            server_io // keep alive until client finishes assertions
        });

        let client = {
            let store = state.store.clone();
            tokio::spawn(async move {
                let _ = run_stream(client_io, &state, Duration::from_secs(5)).await;
                store
            })
        };

        assert!(matches!(rx.recv().await, Some(HeadEvent::NewHead(h)) if h.number == 10));
        assert!(matches!(rx.recv().await, Some(HeadEvent::NewHead(h)) if h.number == 11));
        // Mismatch: break first, then the offending diff seeds the fresh carry.
        assert!(matches!(
            rx.recv().await,
            Some(HeadEvent::ChainBreak { .. })
        ));
        assert!(matches!(rx.recv().await, Some(HeadEvent::NewHead(h)) if h.number == 13));

        let server_io = server.await.unwrap();
        drop(server_io); // EOF -> client returns Ok
        let store = client.await.unwrap();
        assert_eq!(store.tip(), Some((13, [13; 32])));
        // Pre-break state was dropped (cold), post-break seed is present.
        assert!(store.account(&[13; 20], 13).is_some());
        assert!(store.account(&[10; 20], 13).is_none());
    }

    #[tokio::test]
    async fn resubscribes_from_tip_plus_one() {
        let (client_io, mut server_io) = tokio::io::duplex(1 << 16);
        let (tx, _rx) = unbounded_channel();
        let state = ClientState {
            store: Arc::new(Store::new(16)),
            events: tx,
            identity: std::sync::Mutex::new(None),
        };
        // Pre-warm the store as if a previous connection applied block 7.
        state.store.apply(&diff(7, 7, 6));

        tokio::spawn(async move {
            write_frame(&mut server_io, &silt_wire::hello(1, [9; 32], 7))
                .await
                .unwrap();
            let _ = read_frame(&mut server_io).await.unwrap().unwrap();
            let sub = read_frame(&mut server_io).await.unwrap().unwrap();
            assert_eq!(
                sub,
                Frame::Subscribe {
                    from: Some(8),
                    parent_hash: Some([7; 32])
                }
            );
            drop(server_io);
        });
        let _ = run_stream(client_io, &state, Duration::from_secs(5)).await;
    }

    /// A folded span `start..=end` on the chain where block n's hash is
    /// `[n; 32]`; `fork_at` gives that block (and later ones) another hash.
    fn folded(start: u64, end: u64, fork_at: Option<u64>) -> BlockDiff {
        let h = |n: u64| match fork_at {
            Some(f) if n >= f => n as u8 ^ 0x80,
            _ => n as u8,
        };
        BlockDiff {
            span_start: start,
            number: end,
            hash: [h(end); 32],
            parent_hash: [h(start - 1); 32],
            heads: (start..=end).map(|n| head(n, h(n), h(n - 1))).collect(),
            accounts: vec![(
                [0xAA; 20],
                Some(AccountChange {
                    balance: [end as u8; 32],
                    nonce: end,
                    code_hash: [0; 32],
                }),
            )],
            storage: vec![],
            code: vec![],
            receipts: vec![],
            read_accounts: Vec::new(),
            read_storage: Vec::new(),
        }
    }

    /// Node-restart re-deliveries (G3.2): a duplicate is skipped, a folded
    /// span through the tip applies, anything off our chain is a break.
    #[test]
    fn continuity_classifies_restart_redeliveries() {
        let store = Store::new(16);
        assert_eq!(
            continuity(&store, &diff(5, 5, 4)),
            Continuity::Extends,
            "cold seeds"
        );
        for n in 5..=8u8 {
            store.apply(&diff(n as u64, n, n - 1));
        }
        assert_eq!(continuity(&store, &diff(9, 9, 8)), Continuity::Extends);
        assert_eq!(
            continuity(&store, &diff(9, 9, 7)),
            Continuity::Break,
            "wrong parent"
        );
        assert_eq!(
            continuity(&store, &diff(10, 10, 9)),
            Continuity::Break,
            "gap"
        );
        assert_eq!(
            continuity(&store, &diff(8, 8, 7)),
            Continuity::Stale,
            "tip again"
        );
        assert_eq!(
            continuity(&store, &diff(6, 6, 5)),
            Continuity::Stale,
            "older block"
        );
        assert_eq!(
            continuity(&store, &diff(7, 0x77, 6)),
            Continuity::Break,
            "other fork"
        );
        assert_eq!(
            continuity(&store, &folded(6, 10, None)),
            Continuity::Overlaps
        );
        assert_eq!(continuity(&store, &folded(6, 8, None)), Continuity::Stale);
        assert_eq!(
            continuity(&store, &folded(6, 10, Some(8))),
            Continuity::Break,
            "forks at tip"
        );
        assert_eq!(
            continuity(&store, &folded(9, 12, None)),
            Continuity::Extends
        );
    }

    /// After a node restart the spool can start with a folded span that
    /// began below our tip: it applies without a break, and the heads ring
    /// doesn't repeat the blocks we already had.
    #[tokio::test]
    async fn overlapping_span_after_restart_applies_without_a_break() {
        let (client_io, mut server_io) = tokio::io::duplex(1 << 16);
        let (tx, mut rx) = unbounded_channel();
        let state = ClientState {
            store: Arc::new(Store::new(16)),
            events: tx,
            identity: std::sync::Mutex::new(None),
        };
        for n in 5..=8u8 {
            state.store.apply(&diff(n as u64, n, n - 1));
        }
        tokio::spawn(async move {
            write_frame(&mut server_io, &silt_wire::hello(1, [9; 32], 12))
                .await
                .unwrap();
            let _ = read_frame(&mut server_io).await.unwrap().unwrap();
            let sub = read_frame(&mut server_io).await.unwrap().unwrap();
            assert_eq!(
                sub,
                Frame::Subscribe {
                    from: Some(9),
                    parent_hash: Some([8; 32])
                }
            );
            for frame in [folded(6, 8, None), folded(6, 11, None), diff(12, 12, 11)] {
                write_frame(&mut server_io, &Frame::Diff(frame))
                    .await
                    .unwrap();
            }
        });
        let store = state.store.clone();
        let _ = run_stream(client_io, &state, Duration::from_secs(5)).await;
        assert!(matches!(rx.recv().await, Some(HeadEvent::NewHead(h)) if h.number == 11));
        assert!(matches!(rx.recv().await, Some(HeadEvent::NewHead(h)) if h.number == 12));
        assert!(
            rx.try_recv().is_err(),
            "no break, no event for the stale span"
        );
        assert_eq!(store.tip(), Some((12, [12; 32])));
        assert_eq!(store.account(&[0xAA; 20], 12).unwrap().nonce, 11);
        assert!(
            store.account(&[5; 20], 12).is_some(),
            "pre-restart state kept"
        );
        let heads: Vec<u64> = (5..=12)
            .filter_map(|n| store.head(n).map(|h| h.number))
            .collect();
        assert_eq!(heads, (5..=12).collect::<Vec<_>>());
    }

    /// D12: a one-block reorg rolls back to the fork (state of the
    /// dropped block undone, the rest kept), then the replacement applies;
    /// a reorg deeper than the journal drops to cold.
    #[tokio::test]
    async fn a_reorg_rolls_back_and_follows_the_new_side() {
        let (client_io, mut server_io) = tokio::io::duplex(1 << 16);
        let (tx, mut rx) = unbounded_channel();
        let state = ClientState {
            store: Arc::new(Store::with_reorg_depth(16, 3)),
            events: tx,
            identity: std::sync::Mutex::new(None),
        };
        tokio::spawn(async move {
            write_frame(&mut server_io, &silt_wire::hello(1, [9; 32], 12))
                .await
                .unwrap();
            let _ = read_frame(&mut server_io).await.unwrap().unwrap();
            let _ = read_frame(&mut server_io).await.unwrap().unwrap();
            let mut replacement = diff(12, 0x8c, 11);
            replacement.accounts[0].0 = [0x8c; 20];
            let frames = [
                Frame::Diff(diff(10, 10, 9)),
                Frame::Diff(diff(11, 11, 10)),
                Frame::Diff(diff(12, 12, 11)),
                Frame::Reorg {
                    fork_number: 11,
                    fork_hash: [11; 32],
                },
                Frame::Diff(replacement),
                Frame::Diff(diff(13, 13, 0x8c)),
                Frame::Diff(diff(14, 14, 13)),
                Frame::Diff(diff(15, 15, 14)),
                // Four blocks above the fork, journal of three: cold.
                Frame::Reorg {
                    fork_number: 11,
                    fork_hash: [11; 32],
                },
            ];
            for frame in frames {
                write_frame(&mut server_io, &frame).await.unwrap();
            }
        });
        let store = state.store.clone();
        let _ = run_stream(client_io, &state, Duration::from_secs(5)).await;
        let mut events = Vec::new();
        while let Ok(e) = rx.try_recv() {
            events.push(match e {
                HeadEvent::NewHead(h) => format!("head {}", h.number),
                HeadEvent::Reorg { fork_number, .. } => format!("reorg {fork_number}"),
                HeadEvent::ChainBreak { .. } => "break".to_string(),
            });
        }
        assert_eq!(
            events,
            [
                "head 10", "head 11", "head 12", "reorg 11", "head 12", "head 13", "head 14",
                "head 15", "break"
            ]
        );
        assert_eq!(
            store
                .stats
                .rollbacks
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
    }

    /// The rolled-back store holds what it held at the fork: the dropped
    /// block's account is gone, older ones stay, the new side's is there.
    #[tokio::test]
    async fn a_rolled_back_store_keeps_its_pre_fork_state() {
        let (client_io, mut server_io) = tokio::io::duplex(1 << 16);
        let (tx, _rx) = unbounded_channel();
        let state = ClientState {
            store: Arc::new(Store::new(16)),
            events: tx,
            identity: std::sync::Mutex::new(None),
        };
        tokio::spawn(async move {
            write_frame(&mut server_io, &silt_wire::hello(1, [9; 32], 12))
                .await
                .unwrap();
            let _ = read_frame(&mut server_io).await.unwrap().unwrap();
            let _ = read_frame(&mut server_io).await.unwrap().unwrap();
            let mut replacement = diff(12, 0x8c, 11);
            replacement.accounts[0].0 = [0x8c; 20];
            for frame in [
                Frame::Diff(diff(10, 10, 9)),
                Frame::Diff(diff(11, 11, 10)),
                Frame::Diff(diff(12, 12, 11)),
                Frame::Reorg {
                    fork_number: 11,
                    fork_hash: [11; 32],
                },
                Frame::Diff(replacement),
            ] {
                write_frame(&mut server_io, &frame).await.unwrap();
            }
        });
        let store = state.store.clone();
        let _ = run_stream(client_io, &state, Duration::from_secs(5)).await;
        assert_eq!(store.tip(), Some((12, [0x8c; 32])));
        assert!(store.account(&[10; 20], 12).is_some());
        assert!(store.account(&[11; 20], 12).is_some());
        assert!(
            store.account(&[12; 20], 12).is_none(),
            "dropped block undone"
        );
        assert!(store.account(&[0x8c; 20], 12).is_some());
        assert_eq!(store.head(12).unwrap().hash, [0x8c; 32]);
    }

    #[tokio::test]
    async fn identity_change_is_refused() {
        let (tx, _rx) = unbounded_channel();
        let state = ClientState {
            store: Arc::new(Store::new(16)),
            events: tx,
            identity: std::sync::Mutex::new(Some(FeedIdentity {
                chain_id: 1,
                genesis_hash: [9; 32],
            })),
        };
        let (client_io, mut server_io) = tokio::io::duplex(1 << 16);
        tokio::spawn(async move {
            write_frame(&mut server_io, &silt_wire::hello(2, [9; 32], 7))
                .await
                .unwrap();
            let _ = read_frame(&mut server_io).await; // client may bail before writing
        });
        let err = run_stream(client_io, &state, Duration::from_secs(5))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("identity changed"), "{err}");
    }
}
