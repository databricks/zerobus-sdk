//! Fluent builder for creating Zerobus ingestion streams.
//!
//! All setters can be called in any order. The builder validates at
//! `build()` time that table name, authentication, and format have been
//! configured. You can also call `validate()` to check the builder state
//! without opening a stream.
//!
//! # Examples
//!
//! ```rust,ignore
//! let stream = sdk
//!     .stream_builder()
//!     .table("catalog.schema.table")
//!     .oauth("client-id", "client-secret")
//!     .json()
//!     .max_inflight_requests(500_000)
//!     .build()
//!     .await?;
//! ```

use std::fmt;
use std::sync::Arc;

#[cfg(test)]
use std::time::Duration;

use crate::callbacks::AckCallback;
use crate::databricks::zerobus::RecordType;
#[cfg(feature = "testing")]
use crate::headers_provider::NoAuthHeadersProvider;
use crate::headers_provider::{HeadersProvider, OAuthHeadersProvider};
use crate::stream_configuration::StreamConfigurationOptions;
#[cfg(feature = "avro")]
use crate::AvroSchema;
use crate::{
    MessageDescriptor, MessageId, TableProperties, ZerobusError, ZerobusResult, ZerobusSdk,
    ZerobusStream,
};

mod multiplexed;

pub use multiplexed::MultiplexedStreamBuilder;
#[cfg(test)]
use multiplexed::MAX_MULTIPLEXED_JITTER_MS;

#[cfg(feature = "arrow-flight")]
use crate::stream::{
    ArrowSchema, ArrowStreamConfigurationOptions, ArrowTableProperties, ZerobusArrowStream,
};

/// Internal representation of the authentication configuration.
enum AuthConfig {
    OAuth {
        client_id: String,
        client_secret: String,
    },
    HeadersProvider(Arc<dyn HeadersProvider>),
    #[cfg(feature = "testing")]
    NoAuth,
}

/// Which record format was selected.
enum FormatConfig {
    Json,
    CompiledProto(Box<prost_types::DescriptorProto>),
    DynamicProto(MessageDescriptor),
    #[cfg(feature = "arrow-flight")]
    Arrow(Arc<ArrowSchema>),
    #[cfg(feature = "avro")]
    Avro(String),
}

type GrpcFormat = (
    RecordType,
    Option<prost_types::DescriptorProto>,
    Option<MessageDescriptor>,
);

impl FormatConfig {
    fn into_grpc(self) -> ZerobusResult<GrpcFormat> {
        match self {
            Self::Json => Ok((RecordType::Json, None, None)),
            Self::CompiledProto(descriptor) => Ok((RecordType::Proto, Some(*descriptor), None)),
            Self::DynamicProto(descriptor) => Ok((
                RecordType::Proto,
                Some(descriptor.descriptor_proto().clone()),
                Some(descriptor),
            )),
            #[cfg(feature = "arrow-flight")]
            Self::Arrow(_) => Err(ZerobusError::InvalidArgument(
                "Arrow format requires .build_arrow() instead of .build()".into(),
            )),
            #[cfg(feature = "avro")]
            Self::Avro(_) => Ok((RecordType::Avro, None, None)),
        }
    }
}

/// A fluent builder for creating Zerobus ingestion streams.
///
/// All setters can be called in any order. The builder validates at
/// `build()` time that table name, authentication, and format have been
/// configured. Use [`validate()`](Self::validate) to check the builder
/// state without opening a stream.
///
/// # Examples
///
/// ```rust,ignore
/// // JSON stream with OAuth
/// let stream = sdk
///     .stream_builder()
///     .table("catalog.schema.table")
///     .oauth("client-id", "client-secret")
///     .json()
///     .build()
///     .await?;
///
/// // Proto stream with custom headers
/// let stream = sdk
///     .stream_builder()
///     .table("catalog.schema.table")
///     .headers_provider(my_provider)
///     .compiled_proto(descriptor)
///     .max_inflight_requests(500_000)
///     .build()
///     .await?;
///
/// // Validate without opening a stream
/// let builder = sdk
///     .stream_builder()
///     .table("catalog.schema.table")
///     .oauth("client-id", "client-secret")
///     .json();
/// builder.validate()?;
/// let stream = builder.build().await?;
/// ```
#[must_use = "a StreamBuilder does nothing until `.build()` is called"]
pub struct StreamBuilder<'a> {
    sdk: &'a ZerobusSdk,
    table_name: String,
    auth: Option<AuthConfig>,
    format: Option<FormatConfig>,
    grpc_config: StreamConfigurationOptions,
    multiplexed_callback: Option<Arc<dyn AckCallback<MessageId>>>,
    #[cfg(feature = "arrow-flight")]
    arrow_config: ArrowStreamConfigurationOptions,
    #[cfg(feature = "arrow-flight")]
    stats_exporter: Option<Arc<dyn crate::stats::StatsExporter>>,
}

impl fmt::Debug for StreamBuilder<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let auth_kind = match &self.auth {
            Some(AuthConfig::OAuth { .. }) => "OAuth",
            Some(AuthConfig::HeadersProvider(_)) => "HeadersProvider",
            #[cfg(feature = "testing")]
            Some(AuthConfig::NoAuth) => "NoAuth",
            None => "None",
        };
        let format_kind = match &self.format {
            Some(FormatConfig::Json) => "Json",
            Some(FormatConfig::CompiledProto(_)) => "CompiledProto",
            Some(FormatConfig::DynamicProto(_)) => "DynamicProto",
            #[cfg(feature = "arrow-flight")]
            Some(FormatConfig::Arrow(_)) => "Arrow",
            #[cfg(feature = "avro")]
            Some(FormatConfig::Avro(_)) => "Avro",
            None => "None",
        };
        f.debug_struct("StreamBuilder")
            .field("table_name", &self.table_name)
            .field("auth", &auth_kind)
            .field("format", &format_kind)
            .field("ack_callback", &self.grpc_config.ack_callback.is_some())
            .field(
                "multiplexed_ack_callback",
                &self.multiplexed_callback.is_some(),
            )
            .finish_non_exhaustive()
    }
}

const fn missing_auth_error() -> &'static str {
    #[cfg(feature = "testing")]
    {
        "authentication is required: call .oauth(), .headers_provider(), or .no_auth()"
    }
    #[cfg(not(feature = "testing"))]
    {
        "authentication is required: call .oauth() or .headers_provider()"
    }
}

#[allow(clippy::result_large_err)]
impl<'a> StreamBuilder<'a> {
    pub(crate) fn new(sdk: &'a ZerobusSdk) -> Self {
        Self {
            sdk,
            table_name: String::new(),
            auth: None,
            format: None,
            grpc_config: StreamConfigurationOptions::default(),
            multiplexed_callback: None,
            #[cfg(feature = "arrow-flight")]
            arrow_config: ArrowStreamConfigurationOptions::default(),
            #[cfg(feature = "arrow-flight")]
            stats_exporter: None,
        }
    }

    /// Set the fully-qualified Unity Catalog table name (e.g., `"catalog.schema.table"`).
    pub fn table(mut self, table_name: impl Into<String>) -> Self {
        self.table_name = table_name.into();
        self
    }

    /// Authenticate with OAuth client credentials.
    pub fn oauth(mut self, client_id: impl Into<String>, client_secret: impl Into<String>) -> Self {
        self.auth = Some(AuthConfig::OAuth {
            client_id: client_id.into(),
            client_secret: client_secret.into(),
        });
        self
    }

    /// Authenticate with a custom headers provider.
    pub fn headers_provider(mut self, provider: Arc<dyn HeadersProvider>) -> Self {
        self.auth = Some(AuthConfig::HeadersProvider(provider));
        self
    }

    /// Use a no-op headers provider that sends no authentication credentials.
    ///
    /// Intended only for local testing against a Zerobus endpoint that does not
    /// enforce authentication. Available behind the `testing` feature flag.
    #[cfg(feature = "testing")]
    pub fn no_auth(mut self) -> Self {
        self.auth = Some(AuthConfig::NoAuth);
        self
    }

    /// Select JSON record format.
    pub fn json(mut self) -> Self {
        self.format = Some(FormatConfig::Json);
        self
    }

    /// Select compiled protobuf record format.
    pub fn compiled_proto(mut self, descriptor: prost_types::DescriptorProto) -> Self {
        self.format = Some(FormatConfig::CompiledProto(Box::new(descriptor)));
        self
    }

    /// Select dynamic protobuf record format, for a schema known only at runtime.
    ///
    /// Takes a resolved [`MessageDescriptor`](crate::MessageDescriptor). Get one
    /// from [`message_descriptor`](crate::message_descriptor), which resolves a
    /// [`prost_types::DescriptorProto`] (built with
    /// [`crate::schema::descriptor_from_uc_columns`] or fetched from Unity
    /// Catalog), or from your own [`prost_reflect::DescriptorPool`].
    ///
    /// Fill records with [`DynamicRecord`](crate::DynamicRecord). Wire-identical to
    /// [`compiled_proto`](Self::compiled_proto).
    pub fn dynamic_proto(mut self, descriptor: MessageDescriptor) -> Self {
        self.format = Some(FormatConfig::DynamicProto(descriptor));
        self
    }

    /// Select Arrow Flight record format.
    #[cfg(feature = "arrow-flight")]
    pub fn arrow(mut self, schema: Arc<ArrowSchema>) -> Self {
        self.format = Some(FormatConfig::Arrow(schema));
        self
    }

    /// Select Avro record format with the writer schema as JSON (Beta).
    ///
    /// Records are then ingested either as objects the stream encodes against this
    /// schema ([`AvroRecord`](crate::AvroRecord)) or as pre-encoded datums
    /// ([`AvroBytes`](crate::AvroBytes)).
    #[cfg(feature = "avro")]
    pub fn avro(mut self, schema_json: impl Into<String>) -> Self {
        self.format = Some(FormatConfig::Avro(schema_json.into()));
        self
    }

    /// Enable or disable automatic stream recovery.
    pub fn recovery(mut self, enabled: bool) -> Self {
        self.grpc_config.recovery = enabled;
        #[cfg(feature = "arrow-flight")]
        {
            self.arrow_config.recovery = enabled;
        }
        self
    }

    /// Set the timeout in milliseconds for each recovery attempt.
    ///
    /// For streams authenticated with [`oauth`](Self::oauth), this also caps a
    /// proactive OAuth token refresh at half this value, but only when the cached
    /// token has more life left than that cap, so a stalled endpoint falls back
    /// before the attempt deadline. Very near expiry the refresh runs unbounded.
    pub fn recovery_timeout_ms(mut self, ms: u64) -> Self {
        self.grpc_config.recovery_timeout_ms = ms;
        #[cfg(feature = "arrow-flight")]
        {
            self.arrow_config.recovery_timeout_ms = ms;
        }
        self
    }

    /// Set the backoff time in milliseconds between recovery retries.
    pub fn recovery_backoff_ms(mut self, ms: u64) -> Self {
        self.grpc_config.recovery_backoff_ms = ms;
        #[cfg(feature = "arrow-flight")]
        {
            self.arrow_config.recovery_backoff_ms = ms;
        }
        self
    }

    /// Set the maximum number of recovery retry attempts.
    pub fn recovery_retries(mut self, n: u32) -> Self {
        self.grpc_config.recovery_retries = n;
        #[cfg(feature = "arrow-flight")]
        {
            self.arrow_config.recovery_retries = n;
        }
        self
    }

    /// Set the timeout in milliseconds for server acknowledgement.
    pub fn server_lack_of_ack_timeout_ms(mut self, ms: u64) -> Self {
        self.grpc_config.server_lack_of_ack_timeout_ms = ms;
        #[cfg(feature = "arrow-flight")]
        {
            self.arrow_config.server_lack_of_ack_timeout_ms = ms;
        }
        self
    }

    /// Set the timeout in milliseconds for flush operations.
    pub fn flush_timeout_ms(mut self, ms: u64) -> Self {
        self.grpc_config.flush_timeout_ms = ms;
        #[cfg(feature = "arrow-flight")]
        {
            self.arrow_config.flush_timeout_ms = ms;
        }
        self
    }

    /// Set the maximum number of in-flight requests (JSON and Protocol Buffer streams only).
    ///
    /// For a multiplexed stream, this is a mux-wide budget. Each sub-stream
    /// receives `n / stream_count` capacity; any remainder is unused. The
    /// budget must be at least the number of sub-streams.
    pub fn max_inflight_requests(mut self, n: usize) -> Self {
        self.grpc_config.max_inflight_requests = n;
        self
    }

    /// Set the maximum total encoded record byte size allowed per ingest call
    /// (JSON and Protocol Buffer streams only).
    ///
    /// This is the sum of all record bytes passed to a single ingest call.
    /// Calls exceeding this limit fail fast with
    /// [`ZerobusError::InvalidArgument`] before any network I/O.
    ///
    /// Defaults to slightly below the 10 MiB server limit.
    ///
    /// This setting applies to gRPC streams, including multiplexed streams.
    /// Arrow Flight streams (built with `build_arrow()`) do not read this value
    /// and have no client-side payload-size enforcement.
    pub fn max_ingest_payload_bytes(mut self, bytes: usize) -> Self {
        self.grpc_config.max_ingest_payload_bytes = bytes;
        self
    }

    /// Set the maximum wait time during graceful stream pause (JSON/proto and Arrow streams).
    pub fn stream_paused_max_wait_time_ms(mut self, ms: Option<u64>) -> Self {
        self.grpc_config.stream_paused_max_wait_time_ms = ms;
        #[cfg(feature = "arrow-flight")]
        {
            self.arrow_config.stream_paused_max_wait_time_ms = ms;
        }
        self
    }

    /// Set the acknowledgment callback for an ordinary JSON or Protocol Buffer gRPC stream.
    ///
    /// Calling this setter again replaces the previous ordinary callback.
    /// [`MultiplexedStreamBuilder::validate`] and `build_arrow()` reject it.
    pub fn ack_callback(mut self, callback: Arc<dyn AckCallback>) -> Self {
        self.grpc_config.ack_callback = Some(callback);
        self
    }

    /// Set the acknowledgment callback for a multiplexed gRPC stream.
    ///
    /// The callback receives the [`MessageId`] returned by multiplexed ingest
    /// methods. Calling this setter again replaces the previous multiplexed
    /// callback. Ordinary `build()` and `build_arrow()` reject it.
    pub fn multiplexed_ack_callback(mut self, callback: Arc<dyn AckCallback<MessageId>>) -> Self {
        self.multiplexed_callback = Some(callback);
        self
    }

    /// Set the maximum wait time for callbacks after stream close
    /// (JSON and Protocol Buffer gRPC streams only).
    pub fn callback_max_wait_time_ms(mut self, ms: Option<u64>) -> Self {
        self.grpc_config.callback_max_wait_time_ms = ms;
        self
    }

    /// Register a telemetry exporter (Arrow streams only).
    ///
    /// The exporter receives [`StreamStat`](crate::StreamStat) events (batch
    /// sends with byte sizes, acknowledgements, reconnects). Its `record` runs
    /// inline on the stream's IO tasks, so keep it lightweight — use
    /// [`channel_exporter`](crate::channel_exporter) to offload heavier work.
    ///
    /// Takes the exporter by value (`.stats_exporter(MyExporter)`); the SDK wraps
    /// it in an `Arc` internally to share across its IO tasks. The `'static` bound
    /// is required because those tasks outlive this call. Pass an `Arc` clone when
    /// you need to keep a handle (e.g. the [`channel_exporter`](crate::channel_exporter) pair).
    ///
    /// Supported only by Arrow Flight streams: configuring it and then calling
    /// [`build`](Self::build) (a non-Arrow stream) is rejected. Use
    /// [`build_arrow`](Self::build_arrow).
    #[cfg(feature = "arrow-flight")]
    pub fn stats_exporter<E>(mut self, exporter: E) -> Self
    where
        E: crate::stats::StatsExporter + 'static,
    {
        self.stats_exporter = Some(Arc::new(exporter));
        self
    }

    /// Set the maximum number of in-flight Arrow batches (Arrow streams only).
    #[cfg(feature = "arrow-flight")]
    pub fn max_inflight_batches(mut self, n: usize) -> Self {
        self.arrow_config.max_inflight_batches = n;
        self
    }

    /// Set the connection timeout in milliseconds for Arrow Flight (Arrow streams only).
    #[cfg(feature = "arrow-flight")]
    pub fn connection_timeout_ms(mut self, ms: u64) -> Self {
        self.arrow_config.connection_timeout_ms = ms;
        self
    }

    /// Set the Arrow IPC compression type (Arrow streams only).
    #[cfg(feature = "arrow-flight")]
    pub fn ipc_compression(mut self, compression: Option<arrow_ipc::CompressionType>) -> Self {
        self.arrow_config.ipc_compression = compression;
        self
    }

    /// Select a multiplexed gRPC stream composed of `stream_count` homogeneous
    /// sub-streams.
    ///
    /// This is a terminal mode selection: configure table, authentication,
    /// format, stream options, and an optional
    /// [`multiplexed_ack_callback`](Self::multiplexed_ack_callback) before
    /// calling it. The returned builder only supports validation and
    /// construction.
    ///
    /// Use multiplexing when one gRPC stream is the throughput bottleneck
    /// and global ordering is not required. Records retain ordering within
    /// each sub-stream, but there is no global record, message-ID, or callback
    /// order. Different sub-stream callback workers may invoke the shared
    /// callback concurrently.
    ///
    /// For a JSON stream, first migrate to compiled Protocol Buffers and
    /// measure throughput again before considering multiplexing.
    ///
    /// # Beta
    ///
    /// Multiplexed streams are a Beta API.
    ///
    /// `stream_count` must be in `1..=64` and cannot exceed the configured
    /// `max_inflight_requests`. The mux-wide in-flight budget is divided evenly
    /// across sub-streams using integer division. JSON, compiled protobuf, and
    /// dynamic protobuf are supported; Arrow Flight and Avro are not.
    pub fn multiplexed(self, stream_count: usize) -> MultiplexedStreamBuilder<'a> {
        MultiplexedStreamBuilder::new(self, stream_count)
    }

    /// Validate that the builder has all required fields configured.
    ///
    /// Returns `Ok(())` if table name, authentication, and format are all set — useful
    /// for fail-fast validation during startup or config parsing. This is a required-fields
    /// check, not a guarantee that `build()` succeeds: the Avro writer schema is parsed in
    /// `build()` (before any connection), so a malformed schema surfaces there, not here.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let builder = sdk
    ///     .stream_builder()
    ///     .table("catalog.schema.table")
    ///     .oauth("client-id", "client-secret")
    ///     .json();
    ///
    /// // Check configuration before opening the stream
    /// builder.validate()?;
    /// let stream = builder.build().await?;
    /// ```
    pub fn validate(&self) -> ZerobusResult<()> {
        self.validate_common()?;
        if self.multiplexed_callback.is_some() {
            return Err(ZerobusError::InvalidArgument(
                "multiplexed_ack_callback is only valid with .multiplexed(...)".into(),
            ));
        }
        Ok(())
    }

    fn validate_common(&self) -> ZerobusResult<()> {
        if self.table_name.is_empty() {
            return Err(ZerobusError::InvalidArgument(
                "table name is required: call .table()".into(),
            ));
        }
        if self.auth.is_none() {
            return Err(ZerobusError::InvalidArgument(missing_auth_error().into()));
        }
        if self.format.is_none() {
            return Err(ZerobusError::InvalidArgument(
                "record format is required: call .json(), .compiled_proto(), .dynamic_proto(), .avro(), or .arrow()".into(),
            ));
        }
        #[cfg(feature = "arrow-flight")]
        if self.stats_exporter.is_some() && !matches!(self.format, Some(FormatConfig::Arrow(_))) {
            return Err(ZerobusError::InvalidArgument(
                "stats_exporter is only supported for Arrow Flight streams; use .arrow()".into(),
            ));
        }
        Ok(())
    }

    /// Resolve the headers provider from the stored auth config. For OAuth, a
    /// proactive token refresh is bounded by half the stream's recovery timeout.
    fn resolve_headers_provider(&self) -> ZerobusResult<Arc<dyn HeadersProvider>> {
        match self.auth.as_ref() {
            Some(AuthConfig::OAuth {
                client_id,
                client_secret,
            }) => {
                // Give a proactive refresh half the recovery timeout so a stalled
                // refresh falls back to the cached token before the setup deadline
                // cancels the request. Both recovery-timeout setters write the gRPC
                // and Arrow configs in lockstep, so grpc_config is a valid single
                // source even for an Arrow stream; revisit if a per-transport
                // recovery timeout is ever added.
                let refresh_timeout =
                    std::time::Duration::from_millis(self.grpc_config.recovery_timeout_ms) / 2;
                Ok(Arc::new(OAuthHeadersProvider::with_cache(
                    client_id.clone(),
                    client_secret.clone(),
                    self.table_name.clone(),
                    self.sdk.workspace_id.clone(),
                    self.sdk.unity_catalog_url.clone(),
                    Arc::clone(&self.sdk.token_cache),
                    Some(refresh_timeout),
                )))
            }
            Some(AuthConfig::HeadersProvider(p)) => Ok(Arc::clone(p)),
            #[cfg(feature = "testing")]
            Some(AuthConfig::NoAuth) => Ok(Arc::new(NoAuthHeadersProvider)),
            None => Err(ZerobusError::InvalidArgument(missing_auth_error().into())),
        }
    }

    /// Build and open the configured stream (any record format except Arrow Flight).
    ///
    /// Returns an error if table name, authentication, or format has not been set,
    /// or if an Arrow format was selected (use `build_arrow()` instead).
    pub async fn build(mut self) -> ZerobusResult<ZerobusStream> {
        // `validate()` already rejects `stats_exporter` on a non-Arrow format, and the
        // format match below rejects an Arrow format outright.
        self.validate()?;

        // Parse the writer schema once, before any connection, so malformed JSON fails at
        // build(). Keep the raw JSON (sent to the server on stream creation) and the parsed
        // schema (used to encode records) together in TableProperties for reuse on ingest.
        #[cfg(feature = "avro")]
        let avro_schema = match &self.format {
            Some(FormatConfig::Avro(json)) => {
                let parsed = apache_avro::Schema::parse_str(json).map_err(|e| {
                    ZerobusError::AvroSchemaParseError(format!("Failed to parse Avro schema: {e}"))
                })?;
                Some(AvroSchema {
                    json: json.clone(),
                    parsed,
                })
            }
            _ => None,
        };

        let (record_type, descriptor_proto, message_descriptor) = self
            .format
            .take()
            .expect("format was validated")
            .into_grpc()?;
        let headers_provider = self.resolve_headers_provider()?;

        self.grpc_config.record_type = record_type;
        let table_properties = TableProperties {
            table_name: self.table_name,
            descriptor_proto,
            message_descriptor,
            #[cfg(feature = "avro")]
            avro_schema,
        };

        let channel = self.sdk.get_or_create_channel_zerobus_client().await?;
        let stream = ZerobusStream::new_stream(
            channel,
            table_properties,
            headers_provider,
            self.grpc_config,
        )
        .await?;
        crate::client_warnings::record_stream_creation(stream.table_properties.table_name.as_str());
        Ok(stream)
    }

    /// Build and open an Arrow Flight ingestion stream.
    ///
    /// Returns an error if table name, authentication, or format has not been set,
    /// if a non-Arrow format was selected (use `build()` instead), or if
    /// [`ack_callback`](Self::ack_callback) or
    /// [`multiplexed_ack_callback`](Self::multiplexed_ack_callback) was
    /// configured (callbacks are unsupported for Arrow Flight streams).
    #[cfg(feature = "arrow-flight")]
    pub async fn build_arrow(self) -> ZerobusResult<ZerobusArrowStream> {
        self.validate_common()?;

        let schema = match self.format.as_ref() {
            Some(FormatConfig::Arrow(schema)) => Arc::clone(schema),
            Some(_) => {
                return Err(ZerobusError::InvalidArgument(
                    "non-Arrow format requires .build() instead of .build_arrow()".into(),
                ));
            }
            None => {
                return Err(ZerobusError::InvalidArgument(
                    "record format is required: call .arrow() before .build_arrow()".into(),
                ));
            }
        };

        if self.grpc_config.ack_callback.is_some() || self.multiplexed_callback.is_some() {
            return Err(ZerobusError::InvalidArgument(
                "ack_callback and multiplexed_ack_callback are not supported for Arrow Flight streams"
                    .into(),
            ));
        }

        // Arrow-only: a zero bound deadlocks ingest / panics the channel. Not in the
        // shared validate() since it's irrelevant to JSON/proto build().
        if self.arrow_config.max_inflight_batches == 0 {
            return Err(ZerobusError::InvalidArgument(
                "max_inflight_batches must be greater than 0".into(),
            ));
        }

        // `max_ingest_payload_bytes` only applies to JSON and Protocol Buffer streams; warn if the
        // user changed it from the default before building an Arrow stream.
        if self.grpc_config.max_ingest_payload_bytes
            != StreamConfigurationOptions::default().max_ingest_payload_bytes
        {
            crate::client_warnings::warn_payload_limit_ignored_for_arrow();
        }

        let headers_provider = self.resolve_headers_provider()?;

        let table_properties = ArrowTableProperties {
            table_name: self.table_name,
            schema,
        };

        let table_name = table_properties.table_name.clone();
        let stream = ZerobusArrowStream::new(
            &self.sdk.zerobus_endpoint,
            Arc::clone(&self.sdk.tls_config),
            self.sdk.connector_factory.clone(),
            table_properties,
            headers_provider,
            self.arrow_config,
            Arc::clone(&self.sdk.sdk_identifier),
            self.stats_exporter,
        )
        .await?;
        crate::client_warnings::record_stream_creation(&table_name);
        Ok(stream)
    }
}

#[cfg(test)]
mod tests;
