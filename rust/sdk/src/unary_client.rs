//! Unary ingestion client: one `InsertRecords` call per batch, no stream.
//!
//! [`UnaryClient::ingest_records`] converts the records, checks that each one is
//! exactly one valid JSON object, joins them into one JSON array (the
//! request's `records_json` body), and sends it, retrying retryable errors.
//! Each attempt is bounded by `server_lack_of_ack_timeout_ms`.
//! The server answers only once the batch is durable, so a successful call has
//! nothing left to flush or acknowledge.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use tokio_retry::strategy::FixedInterval;
use tokio_retry::RetryIf;
use tracing::{debug, instrument, warn};

use crate::builder::HeadersProviderFactory;
use crate::databricks::zerobus::InsertRecordsRequest;
use crate::errors::should_retry_initial_connection;
use crate::grpc_channel::{GrpcChannelSource, GrpcClient, RequestSent};
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
}

/// The result of a successful unary ingest call.
///
/// It has no fields: the server returns no data for `InsertRecords`. It is
/// `#[non_exhaustive]`, so fields can be added if the server starts returning
/// data, without a breaking change.
#[derive(Debug, Default)]
#[non_exhaustive]
pub struct UnaryIngestResult {}

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
    channel_source: Arc<GrpcChannelSource>,
    table_name: String,
    /// Gives each call its own provider, so a call's auth rejection invalidates
    /// only the token that call was served.
    headers_provider_factory: HeadersProviderFactory,
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
        channel_source: Arc<GrpcChannelSource>,
        table_name: String,
        headers_provider_factory: HeadersProviderFactory,
        options: UnaryOptions,
    ) -> Self {
        Self {
            channel_source,
            table_name,
            headers_provider_factory,
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
    pub async fn ingest_record(
        &self,
        record: impl Into<PreparedInput>,
    ) -> ZerobusResult<UnaryIngestResult> {
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
    /// An empty batch returns at once without sending anything, like stream
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
    ///   the message names its 0-based index. It is also returned for settings
    ///   the SDK can't use: on the first call, a TLS file that is missing or
    ///   can't be read, TLS material passed as PEM data that the SDK can't use, or
    ///   a proxy URL that can't be parsed; on any call, a custom header from the
    ///   headers provider with an invalid name or value. Not retried, and nothing
    ///   is sent.
    /// * `UnaryIngestError` - The server rejected the batch, the server could not
    ///   be reached (`Unavailable`, for example a DNS, connection or TLS handshake
    ///   failure), or an attempt timed out (`DeadlineExceeded`), after retries are
    ///   exhausted. The attempt timeout also covers getting the headers, so a
    ///   headers provider that takes longer than `server_lack_of_ack_timeout_ms`
    ///   gives `DeadlineExceeded` although that attempt sent nothing.
    /// * `ChannelCreationError` - The SDK could not set up the channel: the
    ///   content of a TLS file can't be used (for example a certificate that
    ///   doesn't match the key while a rotation is in progress), or the proxy
    ///   connector can't load its root certificates. Retried.
    /// * `InvalidTableName` or `InvalidUCTokenError` - The headers provider returned
    ///   a table name or `authorization` value that isn't a valid HTTP header
    ///   value. Not retried.
    /// * Other errors from the headers provider, such as `TokenFetchError`.
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
    pub async fn ingest_records<I, T>(&self, records: I) -> ZerobusResult<UnaryIngestResult>
    where
        I: IntoIterator<Item = T>,
        T: Into<PreparedInput>,
    {
        let Some(records_json) =
            build_request_body(records, self.options.max_ingest_payload_bytes)?
        else {
            return Ok(UnaryIngestResult::default());
        };
        debug!(body_bytes = records_json.len(), "Sending InsertRecords");
        let request = InsertRecordsRequest {
            table_name: Some(self.table_name.clone()),
            records_json: Some(records_json.into()),
        };
        self.send_with_retries(&request).await?;
        Ok(UnaryIngestResult::default())
    }

    /// Sends the request, retrying retryable errors `recovery_backoff_ms` apart up
    /// to `recovery_retries` times. One auth rejection may spend a retry so the
    /// invalidated credential is re-minted, unless the headers provider cannot
    /// refresh its credential.
    async fn send_with_retries(&self, request: &InsertRecordsRequest) -> ZerobusResult<()> {
        let options = &self.options;
        let headers_provider = (self.headers_provider_factory)();
        let headers_provider = headers_provider.as_ref();
        let strategy = FixedInterval::from_millis(options.recovery_backoff_ms)
            .take(options.recovery_retries as usize);
        let mut auth_retry_available =
            options.recovery_retries > 0 && headers_provider.can_refresh();
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
                let result = self.send_attempt(request, headers_provider).await;
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

    /// One attempt, bounded by `server_lack_of_ack_timeout_ms`. The channel lookup,
    /// headers and call share that deadline. After an auth rejection the credential
    /// is invalidated within the rest of it, so a stalled provider cannot turn the
    /// rejection into a retryable timeout.
    async fn send_attempt(
        &self,
        request: &InsertRecordsRequest,
        headers_provider: &dyn HeadersProvider,
    ) -> ZerobusResult<()> {
        let timeout_ms = self.options.server_lack_of_ack_timeout_ms;
        let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
        let timed_out = || {
            ZerobusError::UnaryIngestError(GrpcStatus::deadline_exceeded(format!(
                "InsertRecords attempt timed out after {timeout_ms} ms"
            )))
        };
        let sent = RequestSent::default();
        // Asking the source on every attempt picks up rotated TLS certificates.
        let result = match tokio::time::timeout_at(deadline, self.channel_source.client()).await {
            Err(_) => Err(timed_out()),
            Ok(Err(err)) => Err(err),
            Ok(Ok((channel, channel_timeout))) => {
                match tokio::time::timeout_at(
                    deadline,
                    self.send_once(channel, request.clone(), headers_provider, &sent),
                )
                .await
                {
                    Ok(result) => result,
                    Err(_) => {
                        channel_timeout.record_timeout(&sent);
                        Err(timed_out())
                    }
                }
            }
        };

        if let Err(err) = &result {
            let invalidate_timeout =
                deadline.saturating_duration_since(tokio::time::Instant::now());
            if err.is_auth_rejection()
                && tokio::time::timeout(invalidate_timeout, headers_provider.invalidate())
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

    async fn send_once(
        &self,
        mut channel: GrpcClient,
        request: InsertRecordsRequest,
        headers_provider: &dyn HeadersProvider,
        sent: &RequestSent,
    ) -> ZerobusResult<()> {
        let headers = headers_provider.get_headers().await?;
        let mut request = tonic::Request::new(request);
        *request.metadata_mut() = grpc_metadata_from_headers(headers, &self.table_name)?;
        sent.mark();
        channel
            .insert_records(request)
            .await
            .map_err(|status| ZerobusError::UnaryIngestError(GrpcStatus::from_tonic(&status)))?;
        Ok(())
    }
}

/// Builds the `records_json` body: the records joined into one JSON array.
/// Returns `None` for an empty batch, which is not sent.
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

    // The server limits the whole body, so the commas and brackets count.
    // Checked before validation, which parses every record.
    let body_len = records.iter().map(String::len).sum::<usize>() + records.len() + 1;
    if body_len > max_payload_bytes {
        return Err(ZerobusError::InvalidArgument(format!(
            "Ingest payload too large: {body_len} bytes (the records joined into a JSON array) exceeds the configured limit of {max_payload_bytes} bytes"
        )));
    }

    records.iter().enumerate().try_for_each(|(index, record)| {
        validate_json_object(record).map_err(|problem| {
            ZerobusError::InvalidArgument(format!(
                "record {index} is not a single JSON object: {problem}"
            ))
        })
    })?;

    let mut body = Vec::with_capacity(body_len);
    body.push(b'[');
    records.iter().enumerate().for_each(|(index, record)| {
        if index > 0 {
            body.push(b',');
        }
        body.extend_from_slice(record.as_bytes());
    });
    body.push(b']');
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
    use crate::headers_provider::tests::MockTokenEndpoint;
    use crate::tls_config::TlsFingerprint;
    use crate::{JsonString, JsonValue, ProtoBytes, TlsConfig, TlsSettings};

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
    fn size_limit_is_checked_before_records_are_parsed() {
        let records: Vec<PreparedInput> = vec![JsonString("{not json".to_string()).into()];
        let msg = invalid_argument(build_request_body(records, 5));
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
        assert!(build_request_body(records(), 7).unwrap().is_some());
        let msg = invalid_argument(build_request_body(records(), 6));
        assert!(
            msg.contains("7 bytes") && msg.contains("limit of 6 bytes"),
            "{msg}"
        );
    }

    /// Two calls rejected with the same token: each call has its own provider,
    /// so the second call's rejection doesn't drop the token that the first
    /// call's retry already minted. One shared provider would reject that newer
    /// token too and mint a third.
    #[tokio::test]
    async fn concurrent_rejections_of_one_token_mint_one_new_token() {
        let mock = MockTokenEndpoint::start();
        let sdk = crate::ZerobusSdk::new_with_config(
            "http://localhost:1234".to_string(),
            mock.base_url.clone(),
            "12345".to_string(),
            Arc::new(crate::tls_config::SecureTlsConfig::new()),
            None,
            Arc::from(crate::DEFAULT_SDK_IDENTIFIER),
            true,
            crate::token_cache::DEFAULT_REFRESH_BUFFER,
            true,
        );
        let client = sdk
            .unary_client_builder()
            .table("cat.sch.tbl")
            .oauth("id", "secret")
            .json()
            .build()
            .await
            .unwrap();
        let (first, second) = (
            (client.headers_provider_factory)(),
            (client.headers_provider_factory)(),
        );

        let sent = first.get_headers().await.unwrap();
        assert_eq!(second.get_headers().await.unwrap(), sent);
        first.invalidate().await;
        let retried = first.get_headers().await.unwrap();
        second.invalidate().await;

        assert_eq!(second.get_headers().await.unwrap(), retried);
        assert_ne!(retried, sent);
        assert_eq!(mock.mint_count(), 2);
    }

    /// TLS settings that a test can replace, to rotate the channel.
    struct RotatingTls(std::sync::Mutex<TlsSettings>);

    impl TlsConfig for RotatingTls {
        fn tls_settings(&self) -> ZerobusResult<TlsSettings> {
            Ok(self.0.lock().unwrap().clone())
        }
    }

    struct ReadyHeaders;

    #[async_trait::async_trait]
    impl HeadersProvider for ReadyHeaders {
        async fn get_headers(
            &self,
        ) -> ZerobusResult<std::collections::HashMap<&'static str, String>> {
            Ok(std::collections::HashMap::new())
        }
    }

    struct StalledHeaders;

    #[async_trait::async_trait]
    impl HeadersProvider for StalledHeaders {
        async fn get_headers(
            &self,
        ) -> ZerobusResult<std::collections::HashMap<&'static str, String>> {
            std::future::pending().await
        }
    }

    /// A source whose previous channel reached the server, rotated to a channel
    /// that has not. Returns the source, the settings, and the two fingerprints.
    async fn rotated_source(
        endpoint: &str,
    ) -> (
        Arc<GrpcChannelSource>,
        Arc<RotatingTls>,
        TlsFingerprint,
        TlsFingerprint,
    ) {
        let tls = Arc::new(RotatingTls(std::sync::Mutex::new(
            TlsSettings::new().with_domain_name("a.example.com"),
        )));
        let source = Arc::new(GrpcChannelSource::new(
            endpoint.to_string(),
            Arc::from(crate::DEFAULT_SDK_IDENTIFIER),
            Arc::clone(&tls) as Arc<dyn TlsConfig>,
            None,
        ));
        let previous = source.next_channel_fingerprint_for_test().await;
        source.mark_current_reached_for_test().await;
        *tls.0.lock().unwrap() = TlsSettings::new().with_domain_name("b.example.com");
        let rotated = source.next_channel_fingerprint_for_test().await;
        assert_ne!(previous, rotated);
        (source, tls, previous, rotated)
    }

    fn unary_client(source: Arc<GrpcChannelSource>) -> UnaryClient {
        UnaryClient::new(
            source,
            "cat.sch.tbl".to_string(),
            Arc::new(|| Arc::new(ReadyHeaders) as Arc<dyn HeadersProvider>),
            UnaryOptions {
                recovery: false,
                recovery_retries: 0,
                recovery_backoff_ms: 0,
                server_lack_of_ack_timeout_ms: 200,
                max_ingest_payload_bytes: 1024,
            },
        )
    }

    fn is_deadline(err: &ZerobusError) -> bool {
        matches!(err, ZerobusError::UnaryIngestError(status)
            if status.code() == crate::GrpcCode::DeadlineExceeded)
    }

    /// The RPC is sent on the rotated channel and never answers, so the attempt
    /// times out after the request was sent, and the source falls back.
    #[tokio::test]
    async fn a_stalled_call_on_a_rotated_channel_falls_back_to_the_previous_channel() {
        // Bound but never accepted: the connection stalls in the TLS handshake.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!(
            "https://127.0.0.1:{}",
            listener.local_addr().unwrap().port()
        );
        let (source, _, previous, rotated) = rotated_source(&endpoint).await;
        let client = unary_client(Arc::clone(&source));

        let err = client
            .send_attempt(&InsertRecordsRequest::default(), &ReadyHeaders)
            .await
            .unwrap_err();

        assert!(is_deadline(&err), "{err:?}");
        assert_eq!(source.next_channel_fingerprint_for_test().await, previous);
        assert_ne!(previous, rotated);
        drop(listener);
    }

    /// A headers provider that stalls never sends a request, so the timeout says
    /// nothing about the rotated channel and the next call still uses it.
    #[tokio::test]
    async fn a_stalled_headers_provider_does_not_fail_the_rotated_channel() {
        let (source, _, _, rotated) = rotated_source("https://localhost:443").await;
        let client = unary_client(Arc::clone(&source));

        let err = client
            .send_attempt(&InsertRecordsRequest::default(), &StalledHeaders)
            .await
            .unwrap_err();

        assert!(is_deadline(&err), "{err:?}");
        assert_eq!(source.next_channel_fingerprint_for_test().await, rotated);
    }

    #[test]
    fn client_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<UnaryClient>();
    }
}
