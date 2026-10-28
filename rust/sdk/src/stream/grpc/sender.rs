//! Outbound gRPC sender task.
//!
//! Transport-specific: reads from the landing zone (transport-agnostic) and
//! writes ingest messages through the `OutboundSink`, which hides whether the
//! underlying RPC is `EphemeralStream` or `PersistentStream`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use tracing::error;

use super::transport::OutboundSink;
use super::types::RecordLandingZone;
use super::ZerobusStream;
use crate::{ZerobusError, ZerobusResult};

impl ZerobusStream {
    /// Spawns a task that continuously sends records to the Zerobus API by observing the landing zone
    /// to get records and sending them through the outbound stream to the gRPC stream.
    pub(super) fn spawn_sender_task(
        sink: OutboundSink,
        landing_zone: RecordLandingZone,
        is_paused: Arc<AtomicBool>,
        server_error_tx: tokio::sync::watch::Sender<Option<ZerobusError>>,
        cancellation_token: CancellationToken,
    ) -> tokio::task::JoinHandle<ZerobusResult<()>> {
        tokio::spawn(async move {
            loop {
                let item = tokio::select! {
                    biased;
                    _ = cancellation_token.cancelled() => return Ok(()),
                    item = async {
                        if is_paused.load(Ordering::Relaxed) {
                            std::future::pending().await // Wait until supervisor task aborts this task.
                        } else {
                            landing_zone.observe().await
                        }
                    } => item.clone(),
                };
                let send_result = tokio::select! {
                    biased;
                    send_result = sink.send_ingest(item.payload, item.offset_id) => send_result,
                    // A full request channel can block the send; teardown must still run.
                    _ = cancellation_token.cancelled() => return Ok(()),
                };

                if let Err(err) = send_result {
                    error!("Failed to send record: {}", err);
                    let _ = server_error_tx.send(Some(err.clone()));
                    return Err(err);
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use smallvec::smallvec;
    use tokio::time::Duration;

    use super::super::types::IngestRequest;
    use super::*;
    use crate::databricks::zerobus::EphemeralStreamRequest;
    use crate::landing_zone::LandingZone;
    use crate::{EncodedBatch, OffsetIdGenerator};

    #[tokio::test(start_paused = true)]
    async fn cancellation_stops_send_blocked_on_full_request_channel() {
        // The receiver stays alive, so the send blocks instead of failing.
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        tx.try_send(EphemeralStreamRequest::default()).unwrap();
        let sink = OutboundSink::Ephemeral {
            tx,
            wire_offsets: OffsetIdGenerator::default(),
        };
        let landing_zone = Arc::new(LandingZone::new(1));
        landing_zone
            .add(Box::new(IngestRequest {
                payload: EncodedBatch::Proto(smallvec![b"record".to_vec()]),
                offset_id: 0,
            }))
            .await;
        let (server_error_tx, _) = tokio::sync::watch::channel(None);
        let cancellation_token = CancellationToken::new();
        let sender = ZerobusStream::spawn_sender_task(
            sink,
            landing_zone,
            Arc::new(AtomicBool::new(false)),
            server_error_tx,
            cancellation_token.clone(),
        );

        // The sender takes the record and blocks on the full channel.
        tokio::task::yield_now().await;
        cancellation_token.cancel();

        let result = tokio::time::timeout(Duration::from_secs(1), sender)
            .await
            .expect("the cancelled sender must stop");
        assert!(matches!(result, Ok(Ok(()))));
    }
}
