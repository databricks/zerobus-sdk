//! Configuration options for Arrow Flight streams.
//!
use crate::stream_options::defaults;
use arrow_ipc::CompressionType;

/// Configuration options for Arrow Flight stream creation and operation.
///
/// These options control the behavior of Arrow Flight ingestion streams, including
/// backpressure limits, timeout settings, and recovery policies.
///
/// **Do not construct this directly.** Configure Arrow streams via the builder API:
///
/// ```rust,ignore
/// let stream = sdk
///     .stream_builder()
///     .table("catalog.schema.table")
///     .oauth("client-id", "client-secret")
///     .arrow(schema)
///     .max_inflight_batches(100)
///     .server_lack_of_ack_timeout_ms(30_000)
///     .recovery(true)
///     .build_arrow()
///     .await?;
/// ```
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ArrowStreamConfigurationOptions {
    /// Maximum number of batches that can be in-flight (sent but not acknowledged).
    ///
    /// This limit controls memory usage and backpressure. When this limit is reached,
    /// `ingest_batch()` calls will block until acknowledgments free up space.
    ///
    /// Default: 1,000
    pub max_inflight_batches: usize,

    /// Whether to enable automatic stream recovery on failure.
    ///
    /// When enabled, the SDK will automatically attempt to reconnect and recover
    /// the stream when encountering retryable errors.
    ///
    /// Default: `true`
    pub recovery: bool,

    /// Timeout in milliseconds for each stream recovery attempt.
    ///
    /// If a recovery attempt takes longer than this, it will be retried.
    /// Values whose absolute deadline cannot be represented by the platform's
    /// monotonic clock are rejected when the stream is built.
    ///
    /// For OAuth-authenticated streams this also caps a proactive token refresh at
    /// half its value, but only when the cached token has more life left than that
    /// cap, so a stalled endpoint falls back to the cached token before the attempt
    /// deadline. When too little of the token's life remains, the refresh runs
    /// unbounded, like a cold miss.
    ///
    /// Default: 15,000 (15 seconds)
    pub recovery_timeout_ms: u64,

    /// Base delay in milliseconds of the exponential backoff before stream recovery
    /// attempts.
    ///
    /// After the n-th consecutive failure, the delay is `recovery_backoff_ms * 2^(n-1)`,
    /// capped at `recovery_backoff_max_ms`. The SDK waits the delay plus a random jitter of
    /// up to half of the delay. If the server rejects the stream with a retry delay
    /// (`google.rpc.RetryInfo`), the SDK waits at least that long, up to the same cap. The
    /// jitter does not apply to the server retry delay. With `recovery_backoff_ms = 0`, a
    /// server retry delay is still the minimum wait.
    ///
    /// A failure is a failed connection attempt, or an active connection that ends because
    /// of an error or a server-requested rotation. A connection that stayed up for at least
    /// 30 seconds and either received an acknowledgement or had no sent batches left
    /// unacknowledged when it ended resets the count first, so the wait after it is
    /// `recovery_backoff_ms` plus jitter again. A successful reconnect does not reset the
    /// count.
    ///
    /// Initial stream creation uses the same backoff. With the defaults, `build_arrow()`
    /// against an unavailable server waits 30 to 45 seconds in total (2, 4, 8, and 16
    /// seconds, plus jitter) before it returns the error.
    ///
    /// Default: 2,000 (2 seconds)
    pub recovery_backoff_ms: u64,

    /// Maximum delay in milliseconds of the recovery backoff, before jitter.
    ///
    /// A value no larger than `recovery_backoff_ms` keeps the delay fixed at `recovery_backoff_ms`.
    ///
    /// Default: 30,000 (30 seconds)
    pub recovery_backoff_max_ms: u64,

    /// Maximum number of recovery retry attempts before giving up.
    ///
    /// After this many failed attempts, the stream will close and return an error.
    ///
    /// Default: 4
    pub recovery_retries: u32,

    /// Maximum time in milliseconds that a batch may remain pending during normal stream
    /// operation without being fully acknowledged on the active connection.
    ///
    /// No timer runs while there are no pending batches. A batch's absolute deadline starts
    /// when it becomes pending; responses and partial acknowledgments do not refresh it.
    /// Configure this timeout together with `max_inflight_batches` so the server can
    /// acknowledge a full allowed backlog in time. Replayed batches receive a fresh deadline
    /// after the full replay has completed and ACK processing can resume on the recovered
    /// connection. Expiry fails the stream and triggers recovery when recovery is enabled.
    /// Values whose absolute deadline cannot be represented by the platform's
    /// monotonic clock are rejected when the stream is built.
    ///
    /// Default: 60,000 (60 seconds)
    pub server_lack_of_ack_timeout_ms: u64,

    /// Timeout in milliseconds for flush operations.
    ///
    /// If a `flush()` call cannot complete within this time, it will return a timeout error.
    /// Values whose absolute deadline cannot be represented by the platform's
    /// monotonic clock are rejected when the stream is built.
    ///
    /// Default: 300,000 (5 minutes)
    pub flush_timeout_ms: u64,

    /// Timeout in milliseconds for stream connection establishment.
    ///
    /// If the Arrow Flight stream cannot be established within this time,
    /// stream creation will fail.
    ///
    /// Default: 30,000 (30 seconds)
    pub connection_timeout_ms: u64,

    /// Arrow IPC compression for Flight payloads.
    ///
    /// Supported compression types from `arrow_ipc::CompressionType`:
    /// - `CompressionType::LZ4_FRAME` - LZ4 frame compression
    /// - `CompressionType::ZSTD` - Zstandard compression
    ///
    /// Set to `None` to send uncompressed payloads.
    ///
    /// Default: `Some(CompressionType::ZSTD)`
    pub ipc_compression: Option<CompressionType>,

    /// Maximum time in milliseconds to wait for acknowledgments during server-initiated
    /// graceful stream rotation.
    ///
    /// When the server sends a close stream signal indicating it will close the stream,
    /// the SDK enters a "paused" state where it:
    /// - Continues accepting and buffering new `ingest_batch()` calls
    /// - Stops sending buffered batches to the server
    /// - Continues processing acknowledgments for in-flight batches
    /// - Waits for batches already sent at the time of the signal to be acknowledged,
    ///   or for the configured grace period to expire
    /// - Half-closes the request, then shares up to 500ms between observing request EOF
    ///   and draining the response so the peer can send `END_STREAM` before recovery
    ///
    /// Configuration values:
    /// - `None`: Use the available server grace period for acknowledgments
    /// - `Some(0)`: Do not wait for acknowledgments
    /// - `Some(x)`: Wait up to `x` milliseconds for acknowledgments, further capped by
    ///   the server grace period after reserving transport-cleanup time
    ///
    /// The SDK reserves time inside the server grace period for bounded request/response
    /// transport cleanup. This cleanup still runs when the configured ACK wait is zero.
    /// If the server advertises less than 500ms (including zero), the SDK skips the ACK
    /// wait and makes a best-effort local cleanup attempt for up to 500ms; the server may
    /// already have hard-closed, so clean peer shutdown cannot be guaranteed in that case.
    /// Server-advertised grace periods longer than one year are capped at one year.
    /// Close signals are honored even when recovery is disabled: the SDK performs transport
    /// cleanup and terminates without reconnecting. Batches accepted while paused remain
    /// available through `get_unacked_batches()`.
    ///
    /// The clean half-close guarantee applies to the active connection. Explicit close
    /// during an already-active rotation or recovery retains that attempt's trigger even if
    /// the explicit close target is already acknowledged. Any uncommitted replacement request
    /// is dropped best-effort.
    ///
    /// Default: `None` (use the available server grace period)
    pub stream_paused_max_wait_time_ms: Option<u64>,
}

impl Default for ArrowStreamConfigurationOptions {
    fn default() -> Self {
        Self {
            max_inflight_batches: 1_000,
            recovery: defaults::RECOVERY,
            recovery_timeout_ms: defaults::RECOVERY_TIMEOUT_MS,
            recovery_backoff_ms: defaults::RECOVERY_BACKOFF_MS,
            recovery_backoff_max_ms: defaults::RECOVERY_BACKOFF_MAX_MS,
            recovery_retries: defaults::RECOVERY_RETRIES,
            server_lack_of_ack_timeout_ms: defaults::SERVER_LACK_OF_ACK_TIMEOUT_MS,
            flush_timeout_ms: defaults::FLUSH_TIMEOUT_MS,
            connection_timeout_ms: defaults::CONNECTION_TIMEOUT_MS,
            ipc_compression: Some(CompressionType::ZSTD),
            stream_paused_max_wait_time_ms: None,
        }
    }
}
