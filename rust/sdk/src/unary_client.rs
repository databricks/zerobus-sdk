//! Unary ingestion client: one `InsertRecords` call per batch, no stream.
//!
//! [`UnaryClient::ingest_records`] converts the records, checks that each one is
//! exactly one valid JSON object, joins them into one JSON array or into JSON
//! Lines (the request's `records_json` body), and sends it, retrying retryable
//! errors. Each attempt is bounded by `server_lack_of_ack_timeout_ms`.
//! The server answers only once the batch is durable, so a successful call has
//! nothing left to flush or acknowledge.

use std::fmt;
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
pub(crate) struct UnaryOptions {
    pub(crate) recovery: bool,
    pub(crate) recovery_retries: u32,
    pub(crate) recovery_backoff_ms: u64,
    pub(crate) server_lack_of_ack_timeout_ms: u64,
    pub(crate) max_ingest_payload_bytes: usize,
    pub(crate) body: UnaryBody,
}

/// How the records are joined into the request body, chosen with the
/// builder's format setter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum UnaryBody {
    /// `[r0,r1,...]`, from `.json()`.
    JsonArray,
    /// `r0\nr1\n...` without a trailing newline, from `.json_lines()`.
    JsonLines,
}

/// A client that ingests JSON records into one table without opening a stream.
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

impl fmt::Debug for UnaryClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnaryClient")
            .field("table_name", &self.table_name)
            .finish_non_exhaustive()
    }
}

impl UnaryClient {
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
    ///   request body is larger than `max_ingest_payload_bytes`. For a bad record,
    ///   the message names its 0-based index. Nothing is sent.
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
        let Some(records_json) = build_request_body(
            records,
            self.options.max_ingest_payload_bytes,
            self.options.body,
        )?
        else {
            return Ok(());
        };
        debug!(body_bytes = records_json.len(), "Sending InsertRecords");
        let request = InsertRecordsRequest {
            table_name: Some(self.table_name.clone()),
            records_json: Some(records_json.into()),
        };
        self.send_with_retries(&request).await
    }

    /// Sends the request, retrying retryable errors `recovery_backoff_ms` apart up
    /// to `recovery_retries` times. One auth rejection may spend a retry so the
    /// invalidated credential is re-minted.
    async fn send_with_retries(&self, request: &InsertRecordsRequest) -> ZerobusResult<()> {
        let options = &self.options;
        let strategy = FixedInterval::from_millis(options.recovery_backoff_ms)
            .take(options.recovery_retries as usize);
        let mut auth_retry_available = options.recovery_retries > 0;
        let max_attempts = if options.recovery {
            options.recovery_retries.saturating_add(1)
        } else {
            1
        };
        let mut attempts = 0u32;
        let attempt = || {
            attempts += 1;
            let attempt_no = attempts;
            async move {
                let result = self.send_attempt(request).await;
                if let Err(e) = &result {
                    warn!(
                        attempt = attempt_no,
                        max_attempts,
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

    /// One attempt, bounded by `server_lack_of_ack_timeout_ms`. After an auth
    /// rejection it invalidates the credential within the rest of that deadline,
    /// so a stalled provider cannot turn the rejection into a retryable timeout.
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

/// Builds the `records_json` body: the records joined into one JSON array, or
/// into JSON Lines. Returns `None` for an empty batch, which is not sent.
#[allow(clippy::result_large_err)]
fn build_request_body<I, T>(
    records: I,
    max_payload_bytes: usize,
    body_kind: UnaryBody,
) -> ZerobusResult<Option<Vec<u8>>>
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

    // The server limits the whole body, so the separators and brackets count.
    // Checked before validation, which parses every record.
    let record_bytes = records.iter().map(String::len).sum::<usize>();
    let (body_len, shape) = match body_kind {
        UnaryBody::JsonArray => (record_bytes + records.len() + 1, "a JSON array"),
        UnaryBody::JsonLines => (record_bytes + records.len() - 1, "JSON Lines"),
    };
    if body_len > max_payload_bytes {
        return Err(ZerobusError::InvalidArgument(format!(
            "Ingest payload too large: {body_len} bytes (the records joined into {shape}) exceeds the configured limit of {max_payload_bytes} bytes"
        )));
    }

    records.iter().enumerate().try_for_each(|(index, record)| {
        validate_json_object(record).map_err(|problem| {
            ZerobusError::InvalidArgument(format!(
                "record {index} is not a single JSON object: {problem}"
            ))
        })
    })?;

    let (open, separator, close): (&[u8], u8, &[u8]) = match body_kind {
        UnaryBody::JsonArray => (b"[", b',', b"]"),
        UnaryBody::JsonLines => (b"", b'\n', b""),
    };
    let mut body = Vec::with_capacity(body_len);
    body.extend_from_slice(open);
    records.iter().enumerate().for_each(|(index, record)| {
        if index > 0 {
            body.push(separator);
        }
        match body_kind {
            UnaryBody::JsonArray => body.extend_from_slice(record.as_bytes()),
            // A validated record has raw line breaks only between tokens, where
            // a space means the same, so this keeps its value and keeps it on
            // one line.
            UnaryBody::JsonLines => body.extend(record.bytes().map(|byte| match byte {
                b'\n' | b'\r' => b' ',
                other => other,
            })),
        }
    });
    body.extend_from_slice(close);
    debug_assert_eq!(body.len(), body_len);
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

/// Checks that `record` is exactly one valid JSON object. Joining records
/// removes their boundaries, so `{"a":1},{"a":2}` would otherwise become two
/// rows. Field names and types are checked by the server.
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
        build_request_body(records, usize::MAX, UnaryBody::JsonArray)
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

    fn lines(records: Vec<PreparedInput>) -> ZerobusResult<Option<Vec<u8>>> {
        build_request_body(records, usize::MAX, UnaryBody::JsonLines)
    }

    #[test]
    fn json_lines_joins_records_with_newlines() {
        let records: Vec<PreparedInput> = vec![
            JsonString(r#"{"a":1}"#.to_string()).into(),
            JsonString(r#"{"a":2}"#.to_string()).into(),
        ];
        assert_eq!(lines(records).unwrap().unwrap(), b"{\"a\":1}\n{\"a\":2}");
    }

    #[test]
    fn json_lines_turns_raw_line_breaks_into_spaces_and_keeps_escapes() {
        let pretty = "{\r\n  \"msg\": \"a\\nb\"\n}";
        let records: Vec<PreparedInput> = vec![
            JsonString(pretty.to_string()).into(),
            JsonString(r#"{"a":2}"#.to_string()).into(),
        ];
        let body = lines(records).unwrap().unwrap();
        assert_eq!(body, b"{    \"msg\": \"a\\nb\" }\n{\"a\":2}");

        // Same value as the record the caller passed, on a single line.
        let first_line = body.split(|&byte| byte == b'\n').next().unwrap();
        let sent: serde_json::Value = serde_json::from_slice(first_line).unwrap();
        let passed: serde_json::Value = serde_json::from_str(pretty).unwrap();
        assert_eq!(sent, passed);
        assert_eq!(sent["msg"], "a\nb");
    }

    #[test]
    fn json_lines_size_limit_counts_one_byte_per_separator() {
        // `{}\n{}` is 5 bytes: 4 record bytes and 1 newline.
        let records = || -> Vec<PreparedInput> {
            vec![
                JsonString("{}".to_string()).into(),
                JsonString("{}".to_string()).into(),
            ]
        };
        assert!(build_request_body(records(), 5, UnaryBody::JsonLines)
            .unwrap()
            .is_some());
        let msg = invalid_argument(build_request_body(records(), 4, UnaryBody::JsonLines));
        assert!(
            msg.contains("5 bytes") && msg.contains("JSON Lines"),
            "{msg}"
        );
    }

    #[test]
    fn size_limit_is_checked_before_records_are_parsed() {
        let records: Vec<PreparedInput> = vec![JsonString("{not json".to_string()).into()];
        let msg = invalid_argument(build_request_body(records, 5, UnaryBody::JsonArray));
        assert!(msg.contains("payload too large"), "{msg}");
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
        assert!(build_request_body(records(), 7, UnaryBody::JsonArray)
            .unwrap()
            .is_some());
        let msg = invalid_argument(build_request_body(records(), 6, UnaryBody::JsonArray));
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
