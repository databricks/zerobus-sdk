//! Outbound gRPC sender task.
//!
//! Transport-specific: reads from the landing zone (transport-agnostic) and
//! writes ingest messages through the `OutboundSink`, which hides whether the
//! underlying RPC is `EphemeralStream` or `PersistentStream`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[cfg(feature = "test-hooks")]
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::error;

use super::transport::OutboundSink;
use super::types::RecordLandingZone;
use super::ZerobusStream;
use crate::ZerobusResult;

/// Test-only pause point reached after a sender has half-closed its request on cancellation.
#[derive(Default)]
pub(crate) struct SenderExitHook {
    #[cfg(feature = "test-hooks")]
    barrier: tokio::sync::Mutex<Option<(Arc<Notify>, Arc<Notify>)>>,
}

impl SenderExitHook {
    #[cfg(feature = "test-hooks")]
    pub(crate) async fn arm(&self) -> (Arc<Notify>, Arc<Notify>) {
        let reached = Arc::new(Notify::new());
        let proceed = Arc::new(Notify::new());
        *self.barrier.lock().await = Some((Arc::clone(&reached), Arc::clone(&proceed)));
        (reached, proceed)
    }

    async fn park(&self) {
        #[cfg(feature = "test-hooks")]
        {
            let barrier = self.barrier.lock().await.take();
            if let Some((reached, proceed)) = barrier {
                reached.notify_one();
                proceed.notified().await;
            }
        }
    }
}

impl ZerobusStream {
    /// Spawns a task that continuously sends records to the Zerobus API by observing the landing zone
    /// to get records and sending them through the outbound stream to the gRPC stream.
    pub(super) fn spawn_sender_task(
        sink: OutboundSink,
        landing_zone: RecordLandingZone,
        is_paused: Arc<AtomicBool>,
        server_error_tx: crate::stream_stats::ServerErrorSender,
        cancellation_token: CancellationToken,
        exit_hook: Arc<SenderExitHook>,
    ) -> tokio::task::JoinHandle<ZerobusResult<()>> {
        tokio::spawn(async move {
            loop {
                let item = tokio::select! {
                    biased;
                    _ = cancellation_token.cancelled() => {
                        // Dropping the sink half-closes the request, so the server's EOF follows.
                        drop(sink);
                        exit_hook.park().await;
                        return Ok(());
                    }
                    item = async {
                        if is_paused.load(Ordering::Relaxed) {
                            std::future::pending().await // Wait until supervisor task aborts this task.
                        } else {
                            landing_zone.observe().await
                        }
                    } => item.clone(),
                };
                let send_result = sink.send_ingest(item.payload, item.offset_id).await;

                if let Err(err) = send_result {
                    error!("Failed to send record: {}", err);
                    let _ = server_error_tx.send(Some(err.clone()));
                    return Err(err);
                }
            }
        })
    }
}
