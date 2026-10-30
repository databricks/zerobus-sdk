//! Unary ingestion client (Beta): one `InsertRecords` call per batch, no stream.
//!
//! [`UnaryClient::ingest_records`] converts the records, checks that each one is
//! exactly one valid JSON object, joins them into one JSON array (the request's
//! `records_json` body), and sends it with the same retry policy as initial
//! stream creation. Each attempt is bounded by `server_lack_of_ack_timeout_ms`.
//! The server answers only once the batch is durable, so a successful call has
//! nothing left to flush or acknowledge.

use std::sync::Arc;
use std::time::Duration;

use tokio_retry::strategy::FixedInterval;
use tokio_retry::RetryIf;
use tonic::transport::Channel;
use tracing::{debug, instrument, warn};

use crate::databricks::zerobus::zerobus_client::ZerobusClient;
use crate::databricks::zerobus::InsertRecordsRequest;
use crate::errors::should_retry_initial_connection;
use crate::grpc_status::GrpcStatus;
use crate::headers_provider::grpc_metadata_from_headers;
use crate::{EncodedRecord, HeadersProvider, PreparedInput, ZerobusError, ZerobusResult};

/// The builder settings a unary client reads.
// Constructed by `UnaryClientBuilder::build()`, which a later commit adds.
#[allow(dead_code)]
pub(crate) struct UnaryOptions {
    pub(crate) recovery: bool,
    pub(crate) recovery_retries: u32,
    pub(crate) recovery_backoff_ms: u64,
    pub(crate) server_lack_of_ack_timeout_ms: u64,
    pub(crate) max_ingest_payload_bytes: usize,
}

/// A client that ingests JSON records into one table without opening a stream (Beta).
///
/// Each [`ingest_records`](Self::ingest_records) call sends its batch in one
/// unary `InsertRecords` request and returns once the server confirms the
/// batch is durable. Nothing is buffered client-side: there are no offsets, no
/// flush, and no close. Use it for sparse traffic, such as a few batches per
/// minute, when keeping a stream open is not worth it.
///
/// Each call waits for the server, so sending one record per call is slow. For
/// continuous ingestion, use a [`ZerobusStream`](crate::ZerobusStream) with
/// the ingest-then-`flush()` pattern, which keeps many records in flight
/// instead of waiting on each batch.
///
/// Build one with
/// [`ZerobusSdk::unary_client_builder`](crate::ZerobusSdk::unary_client_builder).
/// All unary clients of one SDK share one connection. The client is
/// `Send + Sync`: share it across tasks with an `Arc`, and concurrent calls run
/// in parallel. It runs no background tasks and has no `close()`; dropping it
/// is enough.
///
/// Available behind the `unary` feature. The API is in development.
///
/// # Examples
///
/// ```no_run
/// # use databricks_zerobus_ingest_sdk::*;
/// # async fn example(client: UnaryClient) -> Result<(), ZerobusError> {
/// // One call per batch; it returns once both records are durable.
/// client
///     .ingest_records([
///         JsonString(r#"{"id":1,"status":"pending"}"#.to_string()),
///         JsonString(r#"{"id":2,"status":"shipped"}"#.to_string()),
///     ])
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct UnaryClient {
    channel: ZerobusClient<Channel>,
    table_name: String,
    headers_provider: Arc<dyn HeadersProvider>,
    options: UnaryOptions,
}

impl std::fmt::Debug for UnaryClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnaryClient")
            .field("table_name", &self.table_name)
            .finish_non_exhaustive()
    }
}

impl UnaryClient {
    // Called by `UnaryClientBuilder::build()`, which a later commit adds.
    #[allow(dead_code)]
    pub(crate) fn new(
        channel: ZerobusClient<Channel>,
        table_name: String,
        headers_provider: Arc<dyn HeadersProvider>,
        options: UnaryOptions,
    ) -> Self {
        Self {
            channel,
            table_name,
            headers_provider,
            options,
        }
    }

    /// Sends a single record; the same as [`ingest_records`](Self::ingest_records)
    /// with one record.
    ///
    /// Each call waits until the record is durable. For anything but very sparse
    /// traffic, collect records and send them with `ingest_records` instead.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use databricks_zerobus_ingest_sdk::*;
    /// # async fn example(client: UnaryClient) -> Result<(), ZerobusError> {
    /// // Very sparse traffic only: this call returns once the record is durable.
    /// client
    ///     .ingest_record(JsonString(r#"{"id":3,"status":"delivered"}"#.to_string()))
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn ingest_record(&self, record: impl Into<PreparedInput>) -> ZerobusResult<()> {
        self.ingest_records([record]).await
    }

    /// Sends all records in one `InsertRecords` call and returns once the server
    /// confirms they are durable.
    ///
    /// All-or-nothing: one invalid record rejects the whole batch. Retryable
    /// failures are retried per the builder's retry settings. A retried batch
    /// can be written more than once if an earlier attempt already reached the
    /// server (at-least-once, like ephemeral stream recovery). With
    /// `recovery(false)` nothing is retried, so the SDK never writes a batch
    /// twice. In both modes, an error returned after the request was sent leaves
    /// it unknown whether the batch was written; only the client-side
    /// `InvalidArgument` errors below guarantee nothing was written.
    ///
    /// An empty batch returns `Ok(())` without sending anything, like stream
    /// ingest.
    ///
    /// # Arguments
    ///
    /// * `records` - The records to ingest, accepted as the same record types as
    ///   stream ingest (`JsonValue`, `JsonString`, or `String`).
    ///
    /// # Errors
    ///
    /// * `InvalidArgument` - A record that is not JSON (e.g. `ProtoMessage`), a
    ///   record that is not exactly one valid JSON object, or a batch whose
    ///   request body is larger than `max_ingest_payload_bytes`. The message names
    ///   the 0-based index of the first bad record. Nothing is sent.
    /// * `UnaryIngestError` - The server rejected the batch, the connection failed,
    ///   or an attempt timed out (`DeadlineExceeded`), after retries are exhausted.
    /// * Errors from the headers provider, such as `TokenFetchError`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use databricks_zerobus_ingest_sdk::*;
    /// # use serde::Serialize;
    /// # async fn example(client: UnaryClient) -> Result<(), ZerobusError> {
    /// #[derive(Serialize)]
    /// struct Event {
    ///     id: i64,
    ///     status: String,
    /// }
    ///
    /// let batch: Vec<JsonValue<Event>> = (0..100)
    ///     .map(|id| JsonValue(Event { id, status: "pending".to_string() }))
    ///     .collect();
    /// // One call for the whole batch; it returns once all 100 records are durable.
    /// client.ingest_records(batch).await?;
    /// # Ok(())
    /// # }
    /// ```
    #[instrument(level = "debug", skip_all, fields(table_name = %self.table_name))]
    pub async fn ingest_records<I, T>(&self, records: I) -> ZerobusResult<()>
    where
        I: IntoIterator<Item = T>,
        T: Into<PreparedInput>,
    {
        let Some(records_json) =
            build_request_body(records, self.options.max_ingest_payload_bytes)?
        else {
            return Ok(());
        };
        debug!(body_bytes = records_json.len(), "Sending InsertRecords");
        let request = InsertRecordsRequest {
            table_name: Some(self.table_name.clone()),
            records_json: Some(records_json),
        };
        self.send_with_retries(&request).await
    }

    /// Sends the request with the retry policy of initial stream creation:
    /// retryable errors are retried `recovery_backoff_ms` apart, up to
    /// `recovery_retries` times, and one auth rejection may spend a retry from the
    /// same budget so the invalidated credential is re-minted.
    async fn send_with_retries(&self, request: &InsertRecordsRequest) -> ZerobusResult<()> {
        let options = &self.options;
        let strategy = FixedInterval::from_millis(options.recovery_backoff_ms)
            .take(options.recovery_retries as usize);
        // Created per call; a stream creates it once, for its initial setup.
        let mut auth_retry_available = options.recovery_retries > 0;
        let mut attempts = 0u32;
        let attempt = || {
            attempts += 1;
            let attempt_no = attempts;
            async move {
                let result = self.send_attempt(request).await;
                if let Err(e) = &result {
                    warn!(
                        attempt = attempt_no,
                        max_attempts = options.recovery_retries.saturating_add(1),
                        retryable = e.is_retryable(),
                        backoff_ms = options.recovery_backoff_ms,
                        "InsertRecords attempt failed: {}",
                        e
                    );
                }
                result
            }
        };
        let should_retry = |e: &ZerobusError| {
            should_retry_initial_connection(e, options.recovery, &mut auth_retry_available)
        };
        RetryIf::spawn(strategy, attempt, should_retry).await
    }

    /// One attempt, bounded by `server_lack_of_ack_timeout_ms`: fetching the
    /// headers, connecting if the channel has no live connection, sending, and
    /// waiting for the response.
    ///
    /// After an auth rejection it invalidates the credential within the rest of
    /// the same deadline and keeps the original rejection if the provider stalls,
    /// as `create_initial_stream_connection` does for streams. A stalled provider
    /// therefore cannot turn a known auth rejection into a retryable timeout.
    async fn send_attempt(&self, request: &InsertRecordsRequest) -> ZerobusResult<()> {
        let timeout_ms = self.options.server_lack_of_ack_timeout_ms;
        let attempt_timeout = Duration::from_millis(timeout_ms);
        let attempt_started = tokio::time::Instant::now();
        let result = tokio::time::timeout(attempt_timeout, self.send_once(request.clone()))
            .await
            .unwrap_or_else(|_| {
                Err(ZerobusError::UnaryIngestError(
                    GrpcStatus::deadline_exceeded(format!(
                        "InsertRecords attempt timed out after {timeout_ms} ms"
                    )),
                ))
            });

        if let Err(err) = &result {
            let invalidate_timeout = attempt_timeout.saturating_sub(attempt_started.elapsed());
            if err.is_auth_rejection()
                && tokio::time::timeout(invalidate_timeout, self.headers_provider.invalidate())
                    .await
                    .is_err()
            {
                warn!(
                    timeout_ms,
                    "Headers provider invalidation timed out; preserving auth rejection"
                );
            }
        }
        result
    }

    async fn send_once(&self, request: InsertRecordsRequest) -> ZerobusResult<()> {
        let headers = self.headers_provider.get_headers().await?;
        let mut request = tonic::Request::new(request);
        *request.metadata_mut() = grpc_metadata_from_headers(headers, &self.table_name)?;
        // Generated client methods take `&mut self`; a clone shares the channel.
        let mut channel = self.channel.clone();
        channel
            .insert_records(request)
            .await
            .map_err(|status| ZerobusError::UnaryIngestError(GrpcStatus::from_tonic(&status)))?;
        Ok(())
    }
}

/// Builds the `records_json` body: the records joined into one JSON array.
/// Returns `None` for an empty batch, which is not sent.
///
/// Runs before any network I/O, in this order: convert the records (a
/// non-JSON record fails), skip an empty batch, validate each record, check
/// the body size, then join.
#[allow(clippy::result_large_err)]
fn build_request_body<I, T>(records: I, max_payload_bytes: usize) -> ZerobusResult<Option<Vec<u8>>>
where
    I: IntoIterator<Item = T>,
    T: Into<PreparedInput>,
{
    let records = records
        .into_iter()
        .enumerate()
        .map(|(index, record)| json_record(index, record.into()))
        .collect::<ZerobusResult<Vec<String>>>()?;
    if records.is_empty() {
        return Ok(None);
    }

    for (index, record) in records.iter().enumerate() {
        validate_json_object(record).map_err(|problem| {
            ZerobusError::InvalidArgument(format!(
                "record {index} is not a single JSON object: {problem}"
            ))
        })?;
    }

    // The record bytes, n - 1 commas, and 2 brackets. Unlike on streams, the
    // whole body counts, because the server limits the whole request.
    let body_len = records.iter().map(String::len).sum::<usize>() + records.len() + 1;
    if body_len > max_payload_bytes {
        return Err(ZerobusError::InvalidArgument(format!(
            "Ingest payload too large: {body_len} bytes (the records joined into a JSON array) exceeds the configured limit of {max_payload_bytes} bytes"
        )));
    }

    let mut body = Vec::with_capacity(body_len);
    body.push(b'[');
    for (index, record) in records.iter().enumerate() {
        if index > 0 {
            body.push(b',');
        }
        body.extend_from_slice(record.as_bytes());
    }
    body.push(b']');
    Ok(Some(body))
}

/// Extracts the JSON text of record `index`, or fails if it is not a JSON record.
#[allow(clippy::result_large_err)]
fn json_record(index: usize, record: PreparedInput) -> ZerobusResult<String> {
    match record {
        PreparedInput::Ready(EncodedRecord::Json(json)) => Ok(json),
        _ => Err(ZerobusError::InvalidArgument(format!(
            "record {index}: unary clients accept only JSON records"
        ))),
    }
}

/// Checks that `record` is exactly one valid JSON object.
///
/// `InsertRecords` carries the whole batch in one field, and joining the
/// records removes their boundaries: `{"a":1},{"a":2}` would silently become
/// two rows. Full parsing also stops a request the server would reject from
/// being sent at all. `IgnoredAny` checks the syntax without building values,
/// and `from_str` rejects anything after the object. Field names and types are
/// still checked by the server against the table schema.
fn validate_json_object(record: &str) -> Result<(), String> {
    match record
        .trim_start_matches([' ', '\t', '\n', '\r'])
        .as_bytes()
        .first()
    {
        None => return Err("it is empty".to_string()),
        Some(b'{') => {}
        Some(_) => return Err("it does not start with '{'".to_string()),
    }
    serde_json::from_str::<serde::de::IgnoredAny>(record)
        .map(|_| ())
        .map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{JsonString, JsonValue, ProtoBytes};

    fn invalid_argument(result: ZerobusResult<Option<Vec<u8>>>) -> String {
        match result {
            Err(ZerobusError::InvalidArgument(msg)) => msg,
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    fn body(records: Vec<PreparedInput>) -> ZerobusResult<Option<Vec<u8>>> {
        build_request_body(records, usize::MAX)
    }

    #[test]
    fn joins_records_into_one_json_array() {
        let records: Vec<PreparedInput> = vec![
            JsonString(r#"{"id":1}"#.to_string()).into(),
            r#" {"id":2} "#.to_string().into(),
            JsonValue(serde_json::json!({"id": 3})).into(),
        ];
        let body = body(records).unwrap().unwrap();
        assert_eq!(body, br#"[{"id":1}, {"id":2} ,{"id":3}]"#);
        let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed.as_array().unwrap().len(), 3);
    }

    #[test]
    fn empty_batch_builds_no_body() {
        assert!(body(Vec::new()).unwrap().is_none());
    }

    #[test]
    fn non_json_record_is_rejected_with_its_index() {
        let records: Vec<PreparedInput> = vec![
            JsonString("{}".to_string()).into(),
            ProtoBytes(vec![1, 2]).into(),
        ];
        assert_eq!(
            invalid_argument(body(records)),
            "record 1: unary clients accept only JSON records"
        );
    }

    #[test]
    fn invalid_records_are_rejected_with_their_index() {
        for (record, problem) in [
            ("", "it is empty"),
            ("[1]", "it does not start with '{'"),
            (r#"{"a":}"#, "expected value"),
            (r#"{"a":1},{"a":2}"#, "trailing characters"),
        ] {
            let records: Vec<PreparedInput> = vec![
                JsonString("{}".to_string()).into(),
                JsonString(record.to_string()).into(),
            ];
            let msg = invalid_argument(body(records));
            assert!(
                msg.starts_with("record 1 is not a single JSON object: ") && msg.contains(problem),
                "{record:?}: {msg}"
            );
        }
    }

    #[test]
    fn size_limit_counts_the_whole_body() {
        // `[{},{}]` is 7 bytes: 4 record bytes, 1 comma, 2 brackets.
        let records = || -> Vec<PreparedInput> {
            vec![
                JsonString("{}".to_string()).into(),
                JsonString("{}".to_string()).into(),
            ]
        };
        assert!(build_request_body(records(), 7).unwrap().is_some());
        let msg = invalid_argument(build_request_body(records(), 6));
        assert!(
            msg.contains("7 bytes") && msg.contains("limit of 6 bytes"),
            "{msg}"
        );
    }

    #[test]
    fn client_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<UnaryClient>();
    }
}
