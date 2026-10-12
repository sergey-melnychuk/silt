//! Feed client: connect, validate, apply, emit head events (PLAN T2.1/T2.5).
//!
//! Generic over the stream so tests drive it with `tokio::io::duplex`.
//! Drop-to-cold is the only reorg/gap behavior (D3): any chain break clears
//! the store, emits `ChainBreak`, and keeps consuming (the next diff seeds
//! a fresh carry — with `Subscribe{None}` semantics the first diff is
//! accepted without a parent check because the store is empty).

use std::sync::Arc;
use std::time::Duration;

use silt_wire::{Frame, read_frame, write_frame};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc::UnboundedSender;

use crate::HeadEvent;
use crate::store::Store;

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
    let from = state.store.tip().map(|(n, _)| n + 1);
    write_frame(&mut stream, &Frame::Subscribe { from }).await?;

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
                // no parent check. Otherwise the diff must extend the tip —
                // any break drops to cold, and the offending diff then seeds
                // the fresh carry (its own content is internally coherent).
                if let Some((tip_number, tip_hash)) = state.store.tip()
                    && (diff.parent_hash != tip_hash || diff.span_start != tip_number + 1)
                {
                    state.cold(&format!(
                        "diff {}..{} does not extend tip {tip_number}",
                        diff.span_start, diff.number
                    ));
                }
                state.store.apply(&diff);
                if let Some(head) = state.store.tip_head() {
                    let _ = state.events.send(HeadEvent::NewHead(head));
                }
            }
            Frame::Reorg { new_tip_number, .. } => {
                state.cold(&format!("reorg to {new_tip_number}"));
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
                Frame::Subscribe { from: None },
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
            assert_eq!(sub, Frame::Subscribe { from: Some(8) });
            drop(server_io);
        });
        let _ = run_stream(client_io, &state, Duration::from_secs(5)).await;
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
