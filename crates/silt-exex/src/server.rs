//! Feed publisher: bounded spool + broadcast fan-out + TCP server
//! (PLAN T1.3/T1.4, D5).
//!
//! The ExEx task calls [`Publisher::publish_diff`] / [`publish_reorg`];
//! `FinishedHeight` is acked by the caller right after `publish_diff`
//! returns (the frame is then in the spool — D5: node health never depends
//! on subscriber liveness). Subscribers catch up from the spool; anything
//! older answers `GapTooOld`. A slow subscriber that lags the broadcast
//! channel is disconnected and reconnects through the spool.
//!
//! No socket I/O happens while the spool lock is held, and every subscriber
//! read/write is deadline-bounded: a client that stops draining can neither
//! stall `publish_diff` (D5) nor park its task forever.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use silt_wire::{Frame, Word, hello, hello_mismatch, read_frame, write_frame};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{RwLock, broadcast};

/// Hello exchange + `Subscribe` must complete within this.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// Per-frame write deadline. A subscriber that can't take a whole frame
/// within this is dropped and reconnects through the spool. Generous on
/// purpose: a large folded diff over a slow link must still fit.
const WRITE_TIMEOUT: Duration = Duration::from_secs(30);

/// A spooled diff frame: (tip number, encoded frame).
type Spooled = (u64, Arc<Vec<u8>>);

#[derive(Clone)]
enum Out {
    /// A diff frame, tagged with its tip number for replay bookkeeping.
    Diff(u64, Arc<Vec<u8>>),
    /// Reorg marker / heartbeat — forwarded as-is.
    Raw(Arc<Vec<u8>>),
}

pub struct Publisher {
    spool: RwLock<VecDeque<Spooled>>,
    spool_cap: usize,
    tx: broadcast::Sender<Out>,
    /// Identity for the Hello frame.
    chain_id: u64,
    genesis_hash: Word,
    height: std::sync::atomic::AtomicU64,
    /// Fields (not just consts) so tests can shorten them.
    handshake_timeout: Duration,
    write_timeout: Duration,
}

impl Publisher {
    pub fn new(chain_id: u64, genesis_hash: Word, spool_cap: usize) -> Arc<Self> {
        let (tx, _) = broadcast::channel(256);
        Arc::new(Self {
            spool: RwLock::new(VecDeque::new()),
            spool_cap,
            tx,
            chain_id,
            genesis_hash,
            height: 0.into(),
            handshake_timeout: HANDSHAKE_TIMEOUT,
            write_timeout: WRITE_TIMEOUT,
        })
    }

    pub fn height(&self) -> u64 {
        self.height.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub async fn spool_depth(&self) -> u32 {
        self.spool.read().await.len() as u32
    }

    /// Spools + fans out one diff. After this returns the frame is durable
    /// enough to ack (D5).
    pub async fn publish_diff(&self, diff: &silt_wire::BlockDiff) -> eyre::Result<()> {
        let number = diff.number;
        let bytes = Arc::new(Frame::Diff(diff.clone()).encode()?);
        {
            let mut spool = self.spool.write().await;
            spool.push_back((number, bytes.clone()));
            while spool.len() > self.spool_cap {
                spool.pop_front();
            }
        }
        self.height
            .store(number, std::sync::atomic::Ordering::Relaxed);
        let _ = self.tx.send(Out::Diff(number, bytes)); // no subscribers is fine
        Ok(())
    }

    pub async fn publish_reorg(&self, new_tip_number: u64, new_tip_hash: Word) -> eyre::Result<()> {
        let frame = Frame::Reorg {
            new_tip_number,
            new_tip_hash,
        };
        let _ = self.tx.send(Out::Raw(Arc::new(frame.encode()?)));
        Ok(())
    }

    pub async fn publish_heartbeat(&self) -> eyre::Result<()> {
        let frame = Frame::Heartbeat {
            height: self.height(),
            spool_depth: self.spool_depth().await,
        };
        let _ = self.tx.send(Out::Raw(Arc::new(frame.encode()?)));
        Ok(())
    }

    /// Accept loop; one task per subscriber.
    pub async fn serve(self: Arc<Self>, listener: TcpListener) {
        loop {
            match listener.accept().await {
                Ok((stream, peer)) => {
                    let this = self.clone();
                    tokio::spawn(async move {
                        if let Err(err) = this.handle(stream).await {
                            tracing::info!(%peer, %err, "silt subscriber closed");
                        }
                    });
                }
                Err(err) => {
                    tracing::error!(%err, "silt accept failed");
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            }
        }
    }

    async fn handle<S>(&self, mut stream: S) -> eyre::Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let from = tokio::time::timeout(self.handshake_timeout, self.handshake(&mut stream))
            .await
            .map_err(|_| eyre::eyre!("handshake timed out after {:?}", self.handshake_timeout))??;

        // Subscribe to live fan-out BEFORE snapshotting the spool so nothing
        // falls between replay and live; duplicates are skipped by number.
        let mut rx = self.tx.subscribe();
        // Decide under the read lock, write only after releasing it: a
        // subscriber that stops draining mid-write must never hold the spool
        // lock, or publish_diff's write lock queues behind it and the ExEx
        // stops acking FinishedHeight (D5).
        let (gap, replay): (Option<u64>, Vec<Spooled>) = {
            let spool = self.spool.read().await;
            match (from, spool.front()) {
                (None, _) => (None, Vec::new()),
                (Some(from), Some(&(oldest, _))) if from < oldest => (Some(oldest), Vec::new()),
                (Some(from), _) => (
                    None,
                    spool.iter().filter(|(n, _)| *n >= from).cloned().collect(),
                ),
            }
        };
        if let Some(oldest_available) = gap {
            // Beyond the spool: tell the client to cold-rebuild, then
            // continue live from now.
            let frame = Frame::GapTooOld { oldest_available }.encode()?;
            stream_write(&mut stream, &frame, self.write_timeout).await?;
        }
        let mut last_sent = 0u64;
        for (number, bytes) in replay {
            stream_write(&mut stream, &bytes, self.write_timeout).await?;
            last_sent = number;
        }

        loop {
            match rx.recv().await {
                Ok(Out::Diff(number, bytes)) => {
                    if number <= last_sent {
                        continue; // already replayed from the spool
                    }
                    stream_write(&mut stream, &bytes, self.write_timeout).await?;
                    last_sent = number;
                }
                Ok(Out::Raw(bytes)) => {
                    stream_write(&mut stream, &bytes, self.write_timeout).await?
                }
                // Lagged: this subscriber is too slow for live fan-out; drop
                // it — it reconnects and catches up through the spool.
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    eyre::bail!("subscriber lagged by {n} frames")
                }
                Err(broadcast::error::RecvError::Closed) => return Ok(()),
            }
        }
    }

    /// Hello exchange, then the client's `Subscribe`; returns its `from`.
    async fn handshake<S>(&self, stream: &mut S) -> eyre::Result<Option<u64>>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let ours = hello(self.chain_id, self.genesis_hash, self.height());
        write_frame(stream, &ours).await?;
        let theirs = read_frame(stream)
            .await?
            .ok_or_else(|| eyre::eyre!("eof in handshake"))?;
        if let Some(reason) = hello_mismatch(&ours, &theirs) {
            write_frame(
                stream,
                &Frame::Refused {
                    reason: reason.clone(),
                },
            )
            .await?;
            eyre::bail!("refused: {reason}");
        }
        let Some(Frame::Subscribe { from }) = read_frame(stream).await? else {
            eyre::bail!("expected Subscribe");
        };
        Ok(from)
    }
}

/// Writes one pre-encoded frame (length-prefixed); fails if the whole
/// frame isn't written within `deadline`.
async fn stream_write<S>(stream: &mut S, encoded: &[u8], deadline: Duration) -> eyre::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let write = async {
        stream
            .write_all(&(encoded.len() as u32).to_le_bytes())
            .await?;
        stream.write_all(encoded).await?;
        stream.flush().await
    };
    tokio::time::timeout(deadline, write)
        .await
        .map_err(|_| eyre::eyre!("subscriber write timed out after {deadline:?}"))??;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use silt_wire::BlockDiff;
    use tokio::io::{AsyncReadExt, DuplexStream};
    use tokio::task::JoinHandle;

    const CHAIN_ID: u64 = 1;
    const GENESIS: Word = [7; 32];
    const SHORT: Duration = Duration::from_millis(200);

    fn diff(number: u64) -> BlockDiff {
        BlockDiff {
            span_start: number,
            number,
            hash: [number as u8; 32],
            parent_hash: [0; 32],
            heads: vec![],
            accounts: vec![],
            storage: vec![],
            code: vec![],
            receipts: vec![],
        }
    }

    /// Publisher with `timeout` for both deadlines and diffs `1..=n`
    /// published into a spool of `cap`.
    async fn publisher(cap: usize, timeout: Duration, n: u64) -> Arc<Publisher> {
        let mut p = Publisher::new(CHAIN_ID, GENESIS, cap);
        let inner = Arc::get_mut(&mut p).unwrap();
        inner.handshake_timeout = timeout;
        inner.write_timeout = timeout;
        for i in 1..=n {
            p.publish_diff(&diff(i)).await.unwrap();
        }
        p
    }

    /// Runs `handle` on one end of a duplex pipe with `buf`-byte buffers.
    fn connect(p: &Arc<Publisher>, buf: usize) -> (DuplexStream, JoinHandle<eyre::Result<()>>) {
        let (client, server) = tokio::io::duplex(buf);
        let p = p.clone();
        (client, tokio::spawn(async move { p.handle(server).await }))
    }

    /// Client half of the handshake.
    async fn subscribe(client: &mut DuplexStream, from: Option<u64>) {
        let theirs = read_frame(client).await.unwrap().unwrap();
        assert!(matches!(theirs, Frame::Hello { .. }), "{theirs:?}");
        write_frame(client, &hello(CHAIN_ID, GENESIS, 0))
            .await
            .unwrap();
        write_frame(client, &Frame::Subscribe { from })
            .await
            .unwrap();
    }

    async fn next(client: &mut DuplexStream) -> Frame {
        tokio::time::timeout(Duration::from_secs(5), read_frame(client))
            .await
            .expect("no frame within 5s")
            .unwrap()
            .unwrap()
    }

    async fn server_error(server: JoinHandle<eyre::Result<()>>) -> String {
        tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .expect("server task still running after 5s")
            .unwrap()
            .unwrap_err()
            .to_string()
    }

    #[tokio::test]
    async fn replays_spool_from_requested_number_then_goes_live() {
        let p = publisher(2, WRITE_TIMEOUT, 3).await; // spool: 2, 3
        let (mut c, _server) = connect(&p, 1 << 16);
        subscribe(&mut c, Some(2)).await;
        assert_eq!(next(&mut c).await, Frame::Diff(diff(2)));
        assert_eq!(next(&mut c).await, Frame::Diff(diff(3)));
        p.publish_diff(&diff(4)).await.unwrap();
        assert_eq!(next(&mut c).await, Frame::Diff(diff(4)));
    }

    #[tokio::test]
    async fn gap_too_old_then_goes_live() {
        let p = publisher(2, WRITE_TIMEOUT, 3).await; // spool: 2, 3
        let (mut c, _server) = connect(&p, 1 << 16);
        subscribe(&mut c, Some(1)).await;
        assert_eq!(
            next(&mut c).await,
            Frame::GapTooOld {
                oldest_available: 2
            }
        );
        p.publish_diff(&diff(4)).await.unwrap();
        assert_eq!(next(&mut c).await, Frame::Diff(diff(4)));
    }

    /// Regression: `GapTooOld` used to be written while holding the spool
    /// read lock, so a subscriber that stopped reading at that moment
    /// blocked `publish_diff` and with it the ExEx's `FinishedHeight` (D5).
    #[tokio::test]
    async fn stalled_gap_write_does_not_block_publish() {
        let p = publisher(2, WRITE_TIMEOUT, 3).await; // spool: 2, 3
        // 1-byte pipe: every server write blocks until the client reads.
        let (mut c, _server) = connect(&p, 1);
        subscribe(&mut c, Some(1)).await;
        // First byte of GapTooOld: the server is now mid-write. Stop reading.
        c.read_exact(&mut [0u8; 1]).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), p.publish_diff(&diff(4)))
            .await
            .expect("publish_diff blocked behind a stalled subscriber")
            .unwrap();
        drop(c);
    }

    #[tokio::test]
    async fn stalled_subscriber_is_dropped_after_write_timeout() {
        let p = publisher(2, SHORT, 3).await;
        let (mut c, server) = connect(&p, 1);
        subscribe(&mut c, Some(2)).await; // replay starts; never read it
        let err = server_error(server).await;
        assert!(err.contains("write timed out"), "{err}");
        drop(c);
    }

    #[tokio::test]
    async fn silent_client_is_dropped_after_handshake_timeout() {
        let p = publisher(2, SHORT, 0).await;
        let (c, server) = connect(&p, 1 << 16);
        let err = server_error(server).await;
        assert!(err.contains("handshake timed out"), "{err}");
        drop(c);
    }
}
