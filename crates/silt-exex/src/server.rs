//! Feed publisher: bounded spool + broadcast fan-out + TCP server
//! (PLAN T1.3/T1.4, D5).
//!
//! The ExEx task calls [`Publisher::publish_diff`] / [`publish_reorg`];
//! `FinishedHeight` is acked by the caller right after `publish_diff`
//! returns (the frame is then in the spool — D5: node health never depends
//! on subscriber liveness). Subscribers catch up from the spool; anything
//! older answers `GapTooOld`. A slow subscriber that lags the broadcast
//! channel is disconnected and reconnects through the spool.

use std::collections::VecDeque;
use std::sync::Arc;

use silt_wire::{Frame, Word, hello, hello_mismatch, read_frame, write_frame};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{RwLock, broadcast};

#[derive(Clone)]
enum Out {
    /// A diff frame, tagged with its tip number for replay bookkeeping.
    Diff(u64, Arc<Vec<u8>>),
    /// Reorg marker / heartbeat — forwarded as-is.
    Raw(Arc<Vec<u8>>),
}

pub struct Publisher {
    spool: RwLock<VecDeque<(u64, Arc<Vec<u8>>)>>,
    spool_cap: usize,
    tx: broadcast::Sender<Out>,
    /// Identity for the Hello frame.
    chain_id: u64,
    genesis_hash: Word,
    height: std::sync::atomic::AtomicU64,
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

    async fn handle(&self, mut stream: TcpStream) -> eyre::Result<()> {
        let ours = hello(self.chain_id, self.genesis_hash, self.height());
        write_frame(&mut stream, &ours).await?;
        let theirs = read_frame(&mut stream)
            .await?
            .ok_or_else(|| eyre::eyre!("eof in handshake"))?;
        if let Some(reason) = hello_mismatch(&ours, &theirs) {
            write_frame(
                &mut stream,
                &Frame::Refused {
                    reason: reason.clone(),
                },
            )
            .await?;
            eyre::bail!("refused: {reason}");
        }
        let Some(Frame::Subscribe { from }) = read_frame(&mut stream).await? else {
            eyre::bail!("expected Subscribe");
        };

        // Subscribe to live fan-out BEFORE snapshotting the spool so nothing
        // falls between replay and live; duplicates are skipped by number.
        let mut rx = self.tx.subscribe();
        let replay: Vec<(u64, Arc<Vec<u8>>)> = {
            let spool = self.spool.read().await;
            match from {
                None => Vec::new(),
                Some(from) => {
                    if let Some((oldest, _)) = spool.front()
                        && from < *oldest
                    {
                        // Beyond the spool: tell the client to cold-rebuild,
                        // then continue live from now.
                        write_frame(
                            &mut stream,
                            &Frame::GapTooOld {
                                oldest_available: *oldest,
                            },
                        )
                        .await?;
                        Vec::new()
                    } else {
                        spool.iter().filter(|(n, _)| *n >= from).cloned().collect()
                    }
                }
            }
        };
        let mut last_sent = 0u64;
        for (number, bytes) in replay {
            stream_write(&mut stream, &bytes).await?;
            last_sent = number;
        }

        loop {
            match rx.recv().await {
                Ok(Out::Diff(number, bytes)) => {
                    if number <= last_sent {
                        continue; // already replayed from the spool
                    }
                    stream_write(&mut stream, &bytes).await?;
                    last_sent = number;
                }
                Ok(Out::Raw(bytes)) => stream_write(&mut stream, &bytes).await?,
                // Lagged: this subscriber is too slow for live fan-out; drop
                // it — it reconnects and catches up through the spool.
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    eyre::bail!("subscriber lagged by {n} frames")
                }
                Err(broadcast::error::RecvError::Closed) => return Ok(()),
            }
        }
    }
}

async fn stream_write(stream: &mut TcpStream, encoded: &[u8]) -> eyre::Result<()> {
    use tokio::io::AsyncWriteExt;
    stream
        .write_all(&(encoded.len() as u32).to_le_bytes())
        .await?;
    stream.write_all(encoded).await?;
    stream.flush().await?;
    Ok(())
}
