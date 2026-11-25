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

/// Barrier pair `(reached, proceed)` for a teardown pause point.
#[cfg(feature = "test-hooks")]
type TeardownBarrierGate = tokio::sync::Mutex<Option<(Arc<Notify>, Arc<Notify>)>>;

/// Test-only pause points in teardown: around the sender's half-close on cancellation, and
/// before the receiver drains its final acks.
#[cfg(feature = "test-hooks")]
#[derive(Default)]
pub(crate) struct TeardownPauses {
    pub(super) before_half_close: TeardownBarrierGate,
    pub(super) after_half_close: TeardownBarrierGate,
    pub(super) before_drain: TeardownBarrierGate,
}

/// The pause points the teardown tasks receive. Zero-sized without `test-hooks`.
#[cfg(feature = "test-hooks")]
pub(crate) type TeardownHooks = Arc<TeardownPauses>;
#[cfg(not(feature = "test-hooks"))]
#[derive(Default, Clone, Copy)]
pub(crate) struct TeardownHooks;

#[cfg(feature = "test-hooks")]
impl TeardownPauses {
    pub(crate) async fn arm_before_half_close(&self) -> (Arc<Notify>, Arc<Notify>) {
        Self::arm(&self.before_half_close).await
    }

    pub(crate) async fn arm_after_half_close(&self) -> (Arc<Notify>, Arc<Notify>) {
        Self::arm(&self.after_half_close).await
    }

    pub(crate) async fn arm_before_drain(&self) -> (Arc<Notify>, Arc<Notify>) {
        Self::arm(&self.before_drain).await
    }

    async fn arm(gate: &TeardownBarrierGate) -> (Arc<Notify>, Arc<Notify>) {
        let reached = Arc::new(Notify::new());
        let proceed = Arc::new(Notify::new());
        *gate.lock().await = Some((Arc::clone(&reached), Arc::clone(&proceed)));
        (reached, proceed)
    }

    pub(crate) async fn park(gate: &TeardownBarrierGate) {
        let barrier = gate.lock().await.take();
        if let Some((reached, proceed)) = barrier {
            reached.notify_one();
            proceed.notified().await;
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
        request_half_closed: Arc<AtomicBool>,
        #[cfg_attr(not(feature = "test-hooks"), allow(unused_variables))] hooks: TeardownHooks,
    ) -> tokio::task::JoinHandle<ZerobusResult<()>> {
        tokio::spawn(async move {
            loop {
                let item = tokio::select! {
                    biased;
                    _ = cancellation_token.cancelled() => {
                        #[cfg(feature = "test-hooks")]
                        TeardownPauses::park(&hooks.before_half_close).await;
                        // Dropping the sink half-closes the request, so the server's EOF follows.
                        request_half_closed.store(true, Ordering::Release);
                        drop(sink);
                        #[cfg(feature = "test-hooks")]
                        TeardownPauses::park(&hooks.after_half_close).await;
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
