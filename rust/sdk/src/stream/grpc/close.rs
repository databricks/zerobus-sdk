//! Stream teardown: closing the stream and shutting down its tasks.
//!
//! Transport-agnostic: `close` flushes via the ack path, flips the closed
//! flag, and cancels the supervisor and callback tasks. The IO tasks observe
//! the cancellation and unwind on their own.

use std::sync::atomic::Ordering;

use tokio::time::Duration;
use tracing::{debug, error, info, warn};

use super::ZerobusStream;
use crate::{ZerobusError, ZerobusResult};

/// Maximum time to wait for the supervisor task to finish during stream
/// teardown.
const SHUTDOWN_TIMEOUT_SECS: u64 = 1;
/// Give cooperative tasks a chance to acknowledge abort, without waiting on
/// synchronous user code that Tokio cannot interrupt.
const ABORT_WAIT_TIMEOUT_MS: u64 = 100;

impl ZerobusStream {
    /// Returns whether the stream has been closed.
    pub fn is_closed(&self) -> bool {
        self.is_closed.load(Ordering::Relaxed)
    }

    /// Closes the stream after a flush attempt.
    ///
    /// If already closed, returns immediately without flushing or awaiting cleanup.
    /// Otherwise, calls `flush()`, then requests task shutdown even if flushing fails.
    /// Supervisor shutdown waits up to one second, then at most 100 ms after abort.
    /// Callback draining uses `callback_max_wait_time_ms` (`None` waits indefinitely).
    /// These waits are separate from the flush timeout.
    ///
    /// # Returns
    ///
    /// `Ok(())` if the stream was already closed or flushing succeeded. Task shutdown
    /// errors are not returned. Blocked synchronous user code can outlive the teardown
    /// waits, so `Ok(())` does not guarantee all tasks have exited or resources are released.
    ///
    /// # Errors
    ///
    /// Returns any errors from the flush operation. If flush fails, some records
    /// may not have been acknowledged. Use `get_unacked_records()` to retrieve them.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use databricks_zerobus_ingest_sdk::*;
    /// # async fn example(mut stream: ZerobusStream) -> Result<(), ZerobusError> {
    /// // After ingesting records...
    /// stream.close().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn close(&mut self) -> ZerobusResult<()> {
        if self.is_closed.load(Ordering::Relaxed) {
            return Ok(());
        }
        if let Some(stream_id) = self.stream_id.as_deref() {
            info!(stream_id = %stream_id, "Closing stream");
        } else {
            error!("Stream ID is None during closing");
        }
        let flush_result = self.flush().await;
        self.is_closed.store(true, Ordering::Relaxed);
        self.terminal_token.cancel();
        let _ = self.shutdown_supervisor().await;
        self.shutdown_callbacks().await;
        flush_result
    }

    /// Stops a mux lane after its flush attempt. Cache the outcome and close
    /// the lane before the caller starts cancellable callback draining.
    pub(crate) async fn close_after_flush(&mut self) -> Option<ZerobusError> {
        if let Some(result) = &self.supervisor_shutdown_result {
            return result.as_ref().err().cloned();
        }
        let task_error = self.shutdown_supervisor().await;
        // The supervisor publishes the final error before cancelling this
        // token. A clean shutdown's transient watch error must not be promoted
        // to a terminal cause.
        let lane_error = if self.terminal_token.is_cancelled() {
            self.server_error_rx.borrow().clone()
        } else {
            None
        };
        let result = lane_error.or(task_error).map_or(Ok(()), Err);
        self.supervisor_shutdown_result = Some(result.clone());
        self.is_closed.store(true, Ordering::Relaxed);
        self.terminal_token.cancel();
        result.err()
    }

    /// Waits up to one second for cooperative shutdown, then at most 100ms
    /// after abort. Synchronous user code may outlive that budget; the retained
    /// handle is aborted and the timeout outcome is cached for retries.
    async fn shutdown_supervisor(&mut self) -> Option<ZerobusError> {
        if let Some(result) = &self.supervisor_shutdown_result {
            return result.as_ref().err().cloned();
        }
        self.cancellation_token.cancel();
        let joined = match tokio::time::timeout(
            Duration::from_secs(SHUTDOWN_TIMEOUT_SECS),
            &mut self.supervisor_task,
        )
        .await
        {
            Ok(result) => Some(result),
            Err(_) => {
                warn!("Supervisor task did not exit within timeout, aborting");
                self.supervisor_task.abort();
                tokio::time::timeout(
                    Duration::from_millis(ABORT_WAIT_TIMEOUT_MS),
                    &mut self.supervisor_task,
                )
                .await
                .ok()
            }
        };
        let result = match joined {
            Some(Ok(result)) => result,
            Some(Err(join_error)) if join_error.is_cancelled() => Ok(()),
            Some(Err(join_error)) => Err(ZerobusError::UnexpectedStreamResponseError(format!(
                "Supervisor task failed during shutdown: {join_error}"
            ))),
            None => Err(ZerobusError::ConnectionTimeout(
                "Supervisor task did not stop after abort".into(),
            )),
        };
        self.supervisor_shutdown_result = Some(result.clone());
        result.err()
    }

    /// Retain the handle while awaiting callbacks so cancellation can be resumed.
    pub(crate) async fn shutdown_callbacks(&mut self) {
        if let Some(task) = self.callback_handler_task.as_mut() {
            Self::shutdown_callback_task(task, self.options.callback_max_wait_time_ms).await;
            self.callback_handler_task.take();
        }
    }

    /// Drains the callback handler task during teardown. The caller must have
    /// already cancelled the `cancellation_token`. With `Some(ms)`, waits up to
    /// that long then aborts; with `None`, waits indefinitely.
    ///
    /// Split out so the teardown can be exercised in isolation by tests
    /// (`CallbackHandlerHarness`, `testing` feature).
    pub(super) async fn shutdown_callback_task(
        task: &mut tokio::task::JoinHandle<()>,
        callback_max_wait_time_ms: Option<u64>,
    ) {
        if let Some(callback_max_wait_time_ms) = callback_max_wait_time_ms {
            match tokio::time::timeout(Duration::from_millis(callback_max_wait_time_ms), &mut *task)
                .await
            {
                Ok(_) => {
                    debug!("Callback handler task exited gracefully");
                }
                Err(_) => {
                    debug!("Callback handler task did not exit within timeout, aborting");
                    task.abort();
                }
            }
        } else {
            debug!("Callback max wait time is not set, waiting indefinitely");
            let _ = task.await;
        }
    }

    // Signal the stream to stop accepting work and tear down its background
    // tasks. Unlike `close`, this only needs `&self` — it relies on the
    // cancellation token and `is_closed` flag, both of which are already
    // interior-mutable. The `JoinHandle`s aren't reaped here; that happens in
    // `close` or `Drop`.
    pub(crate) fn signal_shutdown(&self) {
        self.is_closed.store(true, Ordering::Relaxed);
        self.terminal_token.cancel();
        self.cancellation_token.cancel();
    }
}

#[cfg(all(test, feature = "testing"))]
mod tests {
    use std::sync::{atomic::AtomicBool, Arc};

    use tokio::sync::{oneshot, watch, Mutex, RwLock};
    use tokio::task::JoinHandle;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::{
        landing_zone::LandingZone, MultiplexedStream, NoAuthHeadersProvider, StreamType,
        TableProperties,
    };

    // Substitute only background tasks; tests exercise public close, flush,
    // and recovery, with explicit gates and no gRPC server scheduling.
    fn test_stream(
        supervisor: JoinHandle<ZerobusResult<()>>,
        callback: Option<JoinHandle<()>>,
    ) -> (ZerobusStream, watch::Sender<Option<ZerobusError>>) {
        let (ack_tx, ack_rx) = watch::channel(None);
        let (error_tx, error_rx) = watch::channel(None);
        let stream = ZerobusStream {
            stream_id: Some("cancelled-close".into()),
            stream_type: StreamType::Ephemeral,
            headers_provider: Arc::new(NoAuthHeadersProvider),
            options: crate::StreamConfigurationOptions {
                callback_max_wait_time_ms: None,
                flush_timeout_ms: 1_000,
                ..Default::default()
            },
            table_properties: TableProperties {
                table_name: "catalog.schema.table".into(),
                descriptor_proto: None,
                message_descriptor: None,
                #[cfg(feature = "avro")]
                avro_schema: None,
            },
            landing_zone: Arc::new(LandingZone::new(1)),
            oneshot_map: Arc::new(Mutex::new(Default::default())),
            supervisor_task: supervisor,
            supervisor_shutdown_result: None,
            logical_offset_id_generator: Default::default(),
            logical_last_received_offset_id_tx: ack_tx,
            _logical_last_received_offset_id_rx: ack_rx,
            failed_records: Arc::new(RwLock::new(Vec::new())),
            is_closed: Arc::new(AtomicBool::new(false)),
            sync_mutex: Arc::new(Mutex::new(())),
            terminal_token: CancellationToken::new(),
            server_error_rx: error_rx,
            cancellation_token: CancellationToken::new(),
            callback_handler_task: callback,
            dynamic_message_descriptor: None,
        };
        (stream, error_tx)
    }

    #[tokio::test(start_paused = true)]
    async fn cancelled_mux_close_skips_flush_and_resumes_callbacks() {
        let (release_callback, callback_gate) = oneshot::channel();
        let callback = tokio::spawn(async { callback_gate.await.unwrap() });
        let (stream, _error_tx) = test_stream(tokio::spawn(async { Ok(()) }), Some(callback));
        stream
            .ingest_record_offset(b"unacked".to_vec())
            .await
            .unwrap();
        let lane_closed = Arc::clone(&stream.is_closed);
        let mut mux = MultiplexedStream::new(vec![stream]);

        // First flush times out after 1s, then close parks on callback draining.
        assert!(
            tokio::time::timeout(Duration::from_millis(1_100), mux.close())
                .await
                .is_err()
        );
        assert!(lane_closed.load(Ordering::Relaxed));
        for _ in 0..2 {
            assert!(tokio::time::timeout(Duration::from_millis(20), mux.close())
                .await
                .is_err());
        }
        release_callback.send(()).unwrap();
        let result = tokio::time::timeout(Duration::from_millis(20), mux.close())
            .await
            .expect("retry must not flush the stopped lane for another second");
        assert!(
            matches!(result, Err(ZerobusError::StreamClosedError(status)) if status.code() == tonic::Code::DeadlineExceeded)
        );
        let records: Vec<_> = mux.get_unacked_records().await.unwrap().collect();
        assert!(
            matches!(records.as_slice(), [crate::EncodedRecord::Proto(bytes)] if bytes == b"unacked")
        );
    }

    #[tokio::test(start_paused = true)]
    async fn cancelled_mux_close_retains_terminal_failure_after_all_acks() {
        let (release_callback, callback_gate) = oneshot::channel();
        let callback = tokio::spawn(async { callback_gate.await.unwrap() });
        let (mut stream, error_tx) = test_stream(tokio::spawn(async { Ok(()) }), Some(callback));
        let offset = stream
            .ingest_record_offset(b"acked".to_vec())
            .await
            .unwrap();
        stream
            .logical_last_received_offset_id_tx
            .send(Some(offset))
            .unwrap();
        stream.landing_zone.remove_all();
        let cancelled = stream.cancellation_token.clone();
        let terminal = stream.terminal_token.clone();
        let closed = Arc::clone(&stream.is_closed);
        // Publish the terminal failure during close, after its all-acked flush.
        stream.supervisor_task = tokio::spawn(async move {
            cancelled.cancelled().await;
            let error =
                ZerobusError::StreamClosedError(tonic::Status::permission_denied("terminal close"));
            closed.store(true, Ordering::Relaxed);
            error_tx.send(Some(error.clone())).unwrap();
            terminal.cancel();
            Err(error)
        });
        let mut mux = MultiplexedStream::new(vec![stream]);
        assert!(tokio::time::timeout(Duration::from_millis(20), mux.close())
            .await
            .is_err());
        release_callback.send(()).unwrap();
        for _ in 0..2 {
            let result = tokio::time::timeout(Duration::from_millis(20), mux.close())
                .await
                .unwrap();
            assert!(
                matches!(result, Err(ZerobusError::StreamClosedError(status)) if status.code() == tonic::Code::PermissionDenied)
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ordinary_close_bounds_post_abort_wait_on_synchronous_code() {
        // Ensure an assertion failure also releases the blocking task, so the
        // test runtime can shut down even when the close deadline regresses.
        struct ReleaseOnDrop(std::sync::mpsc::Sender<()>);
        impl Drop for ReleaseOnDrop {
            fn drop(&mut self) {
                let _ = self.0.send(());
            }
        }
        let (release, blocked) = std::sync::mpsc::channel();
        let release_guard = ReleaseOnDrop(release);
        let (entered_tx, entered_rx) = oneshot::channel();
        let supervisor = tokio::spawn(async move {
            entered_tx.send(()).unwrap();
            blocked.recv().unwrap(); // Models a synchronous credentials callback.
            Ok(())
        });
        let (mut stream, _error_tx) = test_stream(supervisor, None);
        entered_rx.await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), stream.close())
            .await
            .expect("close must remain bounded after abort")
            .unwrap();
        assert!(matches!(
            stream.supervisor_shutdown_result,
            Some(Err(ZerobusError::ConnectionTimeout(_)))
        ));
        assert!(!stream.supervisor_task.is_finished());
        drop(release_guard);
    }
}
