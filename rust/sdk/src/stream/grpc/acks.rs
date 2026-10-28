//! Acknowledgment tracking: waiting for offsets and inspecting unacked records.
//!
//! Transport-agnostic: these only touch the offset generator, watch channels,
//! and the failed-records buffer. They don't move bytes; they observe progress
//! the IO tasks report back. A wait timeout also closes the stream: it cancels
//! the supervisor and sets the closed state.

use std::sync::atomic::Ordering;

use tokio::time::Duration;
use tracing::{debug, error, instrument, trace, warn};

use super::close::SHUTDOWN_TIMEOUT_SECS;
use super::ZerobusStream;
use crate::{EncodedBatch, EncodedRecord, OffsetId, ZerobusError, ZerobusResult};

impl ZerobusStream {
    /// Once a stream has opened, a later stream-creation failure belongs to a
    /// recovery attempt. Preserve its status while presenting it through the
    /// established stream-operation error variant. The raw terminal cause stays
    /// in `server_error_rx` for mux poisoning and internal diagnostics.
    pub(super) fn normalize_wait_error(error: ZerobusError) -> ZerobusError {
        match error {
            ZerobusError::CreateStreamError(status) => ZerobusError::StreamClosedError(status),
            error => error,
        }
    }

    /// Internal method to wait for a specific offset to be acknowledged.
    /// Used by both `flush()` and `wait_for_offset()`.
    async fn wait_for_offset_internal(
        &self,
        offset_to_wait: OffsetId,
        operation_name: &str,
    ) -> ZerobusResult<()> {
        let wait_operation = async {
            let mut offset_receiver = self.logical_last_received_offset_id_tx.subscribe();
            let mut error_rx = self.server_error_rx.clone();

            loop {
                let offset = *offset_receiver.borrow_and_update();

                let stream_id = match self.stream_id.as_deref() {
                    Some(stream_id) => stream_id,
                    None => {
                        error!("Stream ID is None during {}", operation_name.to_lowercase());
                        "None"
                    }
                };
                if let Some(offset) = offset {
                    if offset >= offset_to_wait {
                        debug!(stream_id = %stream_id, "Stream is caught up to the given offset. {} completed.", operation_name);
                        return Ok(());
                    } else {
                        trace!(
                            stream_id = %stream_id,
                            "Stream is caught up to offset {}. Waiting for offset {}.",
                            offset, offset_to_wait
                        );
                    }
                } else {
                    trace!(
                        stream_id = %stream_id,
                        "Stream is not caught up to any offset yet. Waiting for the first offset."
                    );
                }
                if self.is_closed.load(Ordering::Relaxed) {
                    // Re-check offset before failing, it might have been updated.
                    let offset = *offset_receiver.borrow_and_update();
                    if let Some(offset) = offset {
                        if offset >= offset_to_wait {
                            return Ok(());
                        }
                    }
                    // fail_stream sets is_closed, publishes the error, then cancels
                    // terminal_token. This path can observe closure before the final
                    // watch value; the mux terminal-cause hook waits on the token before reading it.
                    if let Some(server_error) = error_rx.borrow().clone() {
                        return Err(Self::normalize_wait_error(server_error));
                    }
                    return Err(ZerobusError::StreamClosedError(tonic::Status::internal(
                        format!("Stream closed during {}", operation_name.to_lowercase()),
                    )));
                }
                // Race between offset updates and server errors.
                tokio::select! {
                    result = offset_receiver.changed() => {
                        // If offset_receiver channel is closed, break the loop.
                        if result.is_err() {
                            break;
                        }
                        // Loop continues to check new offset value.
                    }
                    _ = error_rx.changed() => {
                        // Server error occurred, return it immediately if stream is closed.
                        if let Some(server_error) = error_rx.borrow().clone() {
                            if self.is_closed.load(Ordering::Relaxed) {
                                // Re-check offset before failing, it might have been updated.
                                let offset = *offset_receiver.borrow_and_update();
                                if let Some(offset) = offset {
                                    if offset >= offset_to_wait {
                                        return Ok(());
                                    }
                                }
                                return Err(Self::normalize_wait_error(server_error));
                            }
                        }
                    }
                }
            }

            if let Some(server_error) = error_rx.borrow().clone() {
                if self.is_closed.load(Ordering::Relaxed) {
                    return Err(Self::normalize_wait_error(server_error));
                }
            }

            Err(ZerobusError::StreamClosedError(tonic::Status::internal(
                format!("Stream closed during {}", operation_name.to_lowercase()),
            )))
        };

        match tokio::time::timeout(
            Duration::from_millis(self.options.flush_timeout_ms),
            wait_operation,
        )
        .await
        {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(_) => {
                if let Some(stream_id) = self.stream_id.as_deref() {
                    error!(stream_id = %stream_id, table_name = %self.table_properties.table_name, "{} timed out", operation_name);
                } else {
                    error!(table_name = %self.table_properties.table_name, "{} timed out", operation_name);
                }
                let error = ZerobusError::StreamClosedError(tonic::Status::deadline_exceeded(
                    format!("{} timed out", operation_name),
                ));
                self.close_after_wait_timeout(offset_to_wait, error).await
            }
        }
    }

    /// Makes the stream terminal after a wait timeout. A caller then resends
    /// `get_unacked_batches()` on a new stream, so this stream must not send them again.
    async fn close_after_wait_timeout(
        &self,
        offset_to_wait: OffsetId,
        error: ZerobusError,
    ) -> ZerobusResult<()> {
        let is_acknowledged = || {
            self.logical_last_received_offset_id_tx
                .borrow()
                .is_some_and(|offset| offset >= offset_to_wait)
        };
        // An acknowledgement published at the deadline wins over the timeout.
        if is_acknowledged() {
            return Ok(());
        }
        let terminal = self.terminal_error.get_or_init(|| error).clone();
        self.cancellation_token.cancel();
        // The supervisor drains the connection (reading late acks on an ephemeral stream),
        // retains the remaining records, and closes the stream.
        if !self.supervisor_task.is_finished() {
            let closed = tokio::time::timeout(
                Duration::from_secs(SHUTDOWN_TIMEOUT_SECS),
                self.terminal_token.cancelled(),
            )
            .await;
            if closed.is_err() {
                warn!("Supervisor task did not close the stream within timeout, closing it here");
            }
        }
        // If the supervisor has exited or stalls, this closes the stream; otherwise the
        // lock waits for `fail_stream` to finish.
        // The ingest lock keeps the enqueue barrier that `fail_stream` relies on.
        // Unlike `fail_stream`, this does not drain the landing zone, fail oneshots, call
        // `on_error()`, or update `server_error_tx`; `get_unacked_records()` still drains it.
        {
            let _ingest_guard = self.sync_mutex.lock().await;
            self.is_closed.store(true, Ordering::Relaxed);
        }
        self.terminal_token.cancel();
        // A late acknowledgement completes the wait, but the stream stays closed.
        if is_acknowledged() {
            return Ok(());
        }
        Err(Self::normalize_wait_error(terminal))
    }

    /// Flushes all currently pending records and waits for their acknowledgments.
    ///
    /// This method captures the current highest offset and waits until all records up to
    /// that offset have been acknowledged by the server. Records ingested during the flush
    /// operation are not included in this flush.
    ///
    /// # Returns
    ///
    /// `Ok(())` when all pending records at the time of the call have been acknowledged.
    ///
    /// # Errors
    ///
    /// * `StreamClosedError` - If the stream is closed or times out
    ///
    /// A timeout closes the stream, so this stream does not send its unacknowledged
    /// records again; see [`ZerobusError::StreamClosedError`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use databricks_zerobus_ingest_sdk::*;
    /// # async fn example(stream: ZerobusStream) -> Result<(), ZerobusError> {
    /// // Ingest many records
    /// for i in 0..1000 {
    ///     let _offset = stream.ingest_record_offset(vec![i as u8]).await?;
    /// }
    ///
    /// // Wait for all to be acknowledged
    /// stream.flush().await?;
    /// println!("All 1000 records have been acknowledged");
    /// # Ok(())
    /// # }
    /// ```
    #[instrument(level = "debug", skip_all, fields(table_name = %self.table_properties.table_name))]
    pub async fn flush(&self) -> ZerobusResult<()> {
        let offset_to_wait = match self.logical_offset_id_generator.last() {
            Some(offset) => offset,
            None => return Ok(()), // Nothing to flush.
        };
        self.wait_for_offset_internal(offset_to_wait, "Flush").await
    }

    /// Waits for server acknowledgment of a specific logical offset.
    ///
    /// This method blocks until the server has acknowledged the record or batch at the
    /// specified offset. Use this with offsets returned from `ingest_record_offset()` or
    /// `ingest_records_offset()` to explicitly control when to wait for acknowledgments.
    ///
    /// # Arguments
    ///
    /// * `offset` - The logical offset ID to wait for (returned from `ingest_record_offset()` or `ingest_records_offset()`)
    ///
    /// # Returns
    ///
    /// `Ok(())` when the record/batch at the specified offset has been acknowledged.
    ///
    /// # Errors
    ///
    /// * `StreamClosedError` - If the stream is closed or times out while waiting. A
    ///   timeout closes the stream, the same as a `flush()` timeout; see
    ///   [`ZerobusError::StreamClosedError`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use databricks_zerobus_ingest_sdk::*;
    /// # async fn example(stream: ZerobusStream) -> Result<(), ZerobusError> {
    /// # let my_record = vec![1, 2, 3];
    /// // Queue records, then wait once on the last offset (or call flush()).
    /// let mut last_offset = None;
    /// for i in 0..100 {
    ///     last_offset = Some(stream.ingest_record_offset(vec![i as u8]).await?);
    /// }
    /// if let Some(offset) = last_offset {
    ///     stream.wait_for_offset(offset).await?;
    /// }
    /// println!("All records acknowledged");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn wait_for_offset(&self, offset: OffsetId) -> ZerobusResult<()> {
        self.wait_for_offset_internal(offset, "Waiting for acknowledgement")
            .await
    }

    /// Returns all records that were ingested but not acknowledged by the server.
    ///
    /// This method should only be called after a stream has failed or been closed.
    /// It's useful for implementing custom retry logic or persisting failed records.
    ///
    /// After a `flush()` or `wait_for_offset()` timeout, this returns exactly the records
    /// that the stream did not get acknowledged; see [`ZerobusError::StreamClosedError`].
    ///
    /// **Note:** This method flattens all unacknowledged records into a single iterator,
    /// losing the original batch grouping.
    /// If you want to preserve the batch grouping, use `ZerobusStream::get_unacked_batches()` instead.
    /// If you want to re-ingest unacknowledged records while preserving their batch
    /// structure, use `ZerobusSdk::recreate_stream()` instead.
    ///
    ///
    /// # Returns
    ///
    /// An iterator over individual `EncodedRecord` items. All unacknowledged records are
    /// flattened into a single sequence, regardless of how they were originally ingested
    /// (via `ingest_record_offset()` or `ingest_records_offset()`).
    ///
    /// # Errors
    ///
    /// * `InvalidStateError` - If called on an active (not closed) stream
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use databricks_zerobus_ingest_sdk::*;
    /// # async fn example(sdk: ZerobusSdk, mut stream: ZerobusStream) -> Result<(), ZerobusError> {
    /// match stream.close().await {
    ///     Err(e) => {
    ///         // Stream failed, get unacked records
    ///         let unacked = stream.get_unacked_records().await?;
    ///         let total_records = unacked.into_iter().count();
    ///         println!("Failed to acknowledge {} records", total_records);
    ///
    ///         // For re-ingestion with preserved batch structure, use recreate_stream
    ///         let new_stream = sdk.recreate_stream(&stream).await?;
    ///     }
    ///     Ok(_) => println!("All records acknowledged"),
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_unacked_records(&self) -> ZerobusResult<impl Iterator<Item = EncodedRecord>> {
        Ok(self
            .get_unacked_batches()
            .await?
            .into_iter()
            .flat_map(|batch| batch.into_iter()))
    }

    /// Returns all records that were ingested but not acknowledged by the server, grouped by batch.
    ///
    /// This method should only be called after a stream has failed or been closed.
    /// It's useful for implementing custom retry logic or persisting failed records.
    ///
    /// After a `flush()` or `wait_for_offset()` timeout, this returns exactly the batches
    /// that the stream did not get acknowledged; see [`ZerobusError::StreamClosedError`].
    ///
    /// **Note:** This method returns the unacknowledged records as a vector of `EncodedBatch` items,
    /// where each batch corresponds to how records were ingested:
    /// - Each `ingest_record_offset()` call creates a single batch containing one record
    /// - Each `ingest_records_offset()` call creates a single batch containing multiple records
    ///
    /// For alternatives, see `ZerobusStream::get_unacked_records()` and `ZerobusSdk::recreate_stream()`.
    ///
    /// # Returns
    ///
    /// A vector of `EncodedBatch` items. Records are grouped by their original ingestion call.
    pub async fn get_unacked_batches(&self) -> ZerobusResult<Vec<EncodedBatch>> {
        if self.is_closed.load(Ordering::Relaxed) {
            // The supervisor only moves landing-zone records into
            // `failed_records` on a stream failure. After a wait timeout, unacked
            // records stay in the landing zone only if the supervisor has exited or
            // stalls past the 1 s bound; drain them here too and retain them so
            // repeat calls return the same result.
            let mut failed = self.failed_records.write().await;
            failed.extend(
                self.landing_zone
                    .remove_all()
                    .into_iter()
                    .map(|request| request.payload),
            );
            return Ok(failed.clone());
        }
        if let Some(stream_id) = self.stream_id.as_deref() {
            error!(stream_id = %stream_id, "Cannot get unacked records from an active stream. Stream must be closed first.");
        } else {
            error!(
                "Cannot get unacked records from an active stream. Stream must be closed first."
            );
        }
        Err(ZerobusError::InvalidStateError(
            "Cannot get unacked records from an active stream. Stream must be closed first."
                .to_string(),
        ))
    }
}
