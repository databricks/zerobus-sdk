//! Fluent builder for creating Zerobus ingestion streams.
//!
//! All setters can be called in any order. The builder validates at
//! `build()` time that table name, authentication, and format have been
//! configured. You can also call `validate()` to check the builder state
//! without opening a stream.
//!
//! This file holds the builder state and the setters that the stream kinds
//! share. `PersistentStreamBuilder` (behind the `eos` feature) uses only these
//! shared setters. The ephemeral terminals live in their own files:
//! `ephemeral.rs` holds `build()`, and `multiplexed.rs` holds `multiplexed()`
//! and `multiplexed_ack_callback()`. Persistent streams do not support
//! multiplexing or Arrow Flight. The Arrow Flight setters and `build_arrow()`
//! are still in this file. The table, authentication, record format, recovery,
//! retry count and backoff, acknowledgement timeout, and payload limit live in
//! [`ClientBuilderCore`], which the public setters write.
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
use std::time::Duration;

use crate::callbacks::AckCallback;
#[cfg(feature = "arrow-flight")]
use crate::client_warnings::{record_stream_creation, warn_payload_limit_ignored_for_arrow};
use crate::grpc_channel::GrpcChannelSource;
use crate::headers_provider::{HeadersProvider, IdpTokenSupplier};
use crate::stream_configuration::{RecordType, StreamConfigurationOptions};
use crate::{
    MessageDescriptor, MessageId, TableProperties, ZerobusError, ZerobusResult, ZerobusSdk,
};

use super::client_builder_core::{ClientBuilderCore, FormatConfig};

mod ephemeral;
mod multiplexed;

pub use multiplexed::MultiplexedStreamBuilder;
#[cfg(test)]
use multiplexed::MAX_MULTIPLEXED_JITTER_MS;

#[cfg(feature = "arrow-flight")]
use crate::stream::{
    ArrowDictionaryHandling, ArrowSchema, ArrowStreamConfigurationOptions, ArrowTableProperties,
    ZerobusArrowStream,
};

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
    core: ClientBuilderCore<'a>,
    settings: StreamSettings,
    multiplexed_callback: Option<Arc<dyn AckCallback<MessageId>>>,
    #[cfg(feature = "arrow-flight")]
    stats_exporter: Option<Arc<dyn crate::stats::StatsExporter>>,
}

/// Settings only streams have. The settings the core owns are not repeated
/// here; [`grpc_options`](StreamBuilder::grpc_options) and
/// [`arrow_options`](StreamBuilder::arrow_options) combine the two at build time.
struct StreamSettings {
    // gRPC and Arrow Flight streams.
    recovery_timeout_ms: u64,
    flush_timeout_ms: u64,
    stream_paused_max_wait_time_ms: Option<u64>,
    // gRPC streams only.
    max_inflight_requests: usize,
    ack_callback: Option<Arc<dyn AckCallback>>,
    callback_max_wait_time_ms: Option<u64>,
    // Arrow Flight streams only.
    #[cfg(feature = "arrow-flight")]
    max_inflight_batches: usize,
    #[cfg(feature = "arrow-flight")]
    connection_timeout_ms: u64,
    #[cfg(feature = "arrow-flight")]
    ipc_compression: Option<arrow_ipc::CompressionType>,
    #[cfg(feature = "arrow-flight")]
    compression: crate::ArrowCompression,
    #[cfg(feature = "arrow-flight")]
    dictionary_handling: ArrowDictionaryHandling,
}

impl Default for StreamSettings {
    /// Takes the defaults from the public options types, so they stay defined in
    /// one place.
    #[allow(deprecated)]
    fn default() -> Self {
        let grpc = StreamConfigurationOptions::default();
        #[cfg(feature = "arrow-flight")]
        let arrow = ArrowStreamConfigurationOptions::default();
        Self {
            recovery_timeout_ms: grpc.recovery_timeout_ms,
            flush_timeout_ms: grpc.flush_timeout_ms,
            stream_paused_max_wait_time_ms: grpc.stream_paused_max_wait_time_ms,
            max_inflight_requests: grpc.max_inflight_requests,
            ack_callback: grpc.ack_callback,
            callback_max_wait_time_ms: grpc.callback_max_wait_time_ms,
            #[cfg(feature = "arrow-flight")]
            max_inflight_batches: arrow.max_inflight_batches,
            #[cfg(feature = "arrow-flight")]
            connection_timeout_ms: arrow.connection_timeout_ms,
            #[cfg(feature = "arrow-flight")]
            ipc_compression: arrow.ipc_compression,
            #[cfg(feature = "arrow-flight")]
            compression: arrow.compression,
            #[cfg(feature = "arrow-flight")]
            dictionary_handling: arrow.dictionary_handling,
        }
    }
}

/// Resolved inputs shared by the gRPC stream terminals: `StreamBuilder::build`
/// and `PersistentStreamBuilder::build` / `resume`. Produced by
/// [`StreamBuilder::prepare_grpc`].
pub(crate) struct GrpcStreamParts {
    pub(crate) channel: Arc<GrpcChannelSource>,
    pub(crate) table_properties: TableProperties,
    pub(crate) headers_provider: Arc<dyn HeadersProvider>,
    /// Stream options with the record type already set from the chosen format.
    pub(crate) config: StreamConfigurationOptions,
}

impl fmt::Debug for StreamBuilder<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamBuilder")
            .field("table_name", &self.core.table_name)
            .field("auth", &self.core.auth_kind())
            .field("format", &self.core.format_kind())
            .field("ack_callback", &self.settings.ack_callback.is_some())
            .field(
                "multiplexed_ack_callback",
                &self.multiplexed_callback.is_some(),
            )
            .finish_non_exhaustive()
    }
}

#[allow(clippy::result_large_err)]
impl<'a> StreamBuilder<'a> {
    pub(crate) fn new(sdk: &'a ZerobusSdk) -> Self {
        Self {
            core: ClientBuilderCore::new(sdk),
            settings: StreamSettings::default(),
            multiplexed_callback: None,
            #[cfg(feature = "arrow-flight")]
            stats_exporter: None,
        }
    }

    /// Set the fully-qualified Unity Catalog table name (e.g., `"catalog.schema.table"`).
    pub fn table(mut self, table_name: impl Into<String>) -> Self {
        self.core.table_name = table_name.into();
        self
    }

    /// Authenticate with OAuth client credentials.
    pub fn oauth(mut self, client_id: impl Into<String>, client_secret: impl Into<String>) -> Self {
        self.core.set_oauth(client_id.into(), client_secret.into());
        self
    }

    /// Authenticate with external-IdP federation (RFC 8693).
    ///
    /// `client_id`: `None` for account-level federation; a Databricks service
    /// principal id for workload identity federation (accepts `None`, a
    /// `String`, or `Some(..)`).
    ///
    /// The shared token cache is partitioned automatically so distinct
    /// identities used from one SDK instance never serve each other's token:
    /// workload keys by `client_id`, and account-level keys by the
    /// `idp_token_supplier`'s stable identity. Clone one supplier across streams
    /// to share its cached token; pass a distinct supplier to isolate.
    pub fn federated_auth(
        mut self,
        idp_token_supplier: IdpTokenSupplier,
        client_id: impl Into<Option<String>>,
    ) -> Self {
        self.core
            .set_federated_auth(idp_token_supplier, client_id.into());
        self
    }

    /// Authenticate with a custom headers provider.
    pub fn headers_provider(mut self, provider: Arc<dyn HeadersProvider>) -> Self {
        self.core.set_headers_provider(provider);
        self
    }

    /// Use a no-op headers provider that sends no authentication credentials.
    ///
    /// Intended only for local testing against a Zerobus endpoint that does not
    /// enforce authentication. Available behind the `testing` feature flag.
    #[cfg(feature = "testing")]
    pub fn no_auth(mut self) -> Self {
        self.core.set_no_auth();
        self
    }

    /// Select JSON record format.
    pub fn json(mut self) -> Self {
        self.core.format = Some(FormatConfig::Json);
        self
    }

    /// Select compiled protobuf record format.
    pub fn compiled_proto(mut self, descriptor: prost_types::DescriptorProto) -> Self {
        self.core.format = Some(FormatConfig::CompiledProto(Box::new(descriptor)));
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
        self.core.format = Some(FormatConfig::DynamicProto(descriptor));
        self
    }

    /// Select Arrow Flight record format.
    #[cfg(feature = "arrow-flight")]
    pub fn arrow(mut self, schema: Arc<ArrowSchema>) -> Self {
        self.core.format = Some(FormatConfig::Arrow(schema));
        self
    }

    /// Select Avro record format with the writer schema as JSON (Beta).
    ///
    /// Records are then ingested either as objects the stream encodes against this
    /// schema ([`AvroRecord`](crate::AvroRecord)) or as pre-encoded datums
    /// ([`AvroBytes`](crate::AvroBytes)).
    #[cfg(feature = "avro")]
    pub fn avro(mut self, schema_json: impl Into<String>) -> Self {
        self.core.format = Some(FormatConfig::Avro(schema_json.into()));
        self
    }

    /// Enable or disable automatic stream recovery.
    pub fn recovery(mut self, enabled: bool) -> Self {
        self.core.recovery = enabled;
        self
    }

    /// Set the timeout in milliseconds for each recovery attempt.
    ///
    /// For streams authenticated with [`oauth`](Self::oauth) or
    /// [`federated_auth`](Self::federated_auth), this also caps a proactive
    /// token refresh at half this value, but only when the cached token has more
    /// life left than that cap, so a stalled endpoint falls back before the
    /// attempt deadline. Very near expiry the refresh runs unbounded.
    pub fn recovery_timeout_ms(mut self, ms: u64) -> Self {
        self.settings.recovery_timeout_ms = ms;
        self
    }

    /// Set the backoff time in milliseconds between recovery retries.
    pub fn recovery_backoff_ms(mut self, ms: u64) -> Self {
        self.core.recovery_backoff_ms = ms;
        self
    }

    /// Set the maximum number of recovery retry attempts.
    pub fn recovery_retries(mut self, n: u32) -> Self {
        self.core.recovery_retries = n;
        self
    }

    /// Set the timeout in milliseconds for server acknowledgement.
    pub fn server_lack_of_ack_timeout_ms(mut self, ms: u64) -> Self {
        self.core.server_lack_of_ack_timeout_ms = ms;
        self
    }

    /// Set the timeout in milliseconds for flush operations.
    pub fn flush_timeout_ms(mut self, ms: u64) -> Self {
        self.settings.flush_timeout_ms = ms;
        self
    }

    /// Set the maximum number of in-flight requests (JSON and Protocol Buffer streams only).
    ///
    /// For a multiplexed stream, this is a mux-wide budget. Each sub-stream
    /// receives `n / stream_count` capacity; any remainder is unused. The
    /// budget must be at least the number of sub-streams.
    pub fn max_inflight_requests(mut self, n: usize) -> Self {
        self.settings.max_inflight_requests = n;
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
        self.core.max_ingest_payload_bytes = bytes;
        self
    }

    /// Set the maximum wait time during graceful stream pause (JSON/proto and Arrow streams).
    pub fn stream_paused_max_wait_time_ms(mut self, ms: Option<u64>) -> Self {
        self.settings.stream_paused_max_wait_time_ms = ms;
        self
    }

    /// Set the acknowledgment callback for an ordinary JSON or Protocol Buffer gRPC stream.
    ///
    /// Calling this setter again replaces the previous ordinary callback.
    /// [`MultiplexedStreamBuilder::validate`] and `build_arrow()` reject it.
    pub fn ack_callback(mut self, callback: Arc<dyn AckCallback>) -> Self {
        self.settings.ack_callback = Some(callback);
        self
    }

    /// Set the maximum wait time for callbacks after stream close
    /// (JSON and Protocol Buffer gRPC streams only).
    pub fn callback_max_wait_time_ms(mut self, ms: Option<u64>) -> Self {
        self.settings.callback_max_wait_time_ms = ms;
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
    /// Supported only by standalone Arrow Flight streams. Non-Arrow streams and
    /// multiplexed Arrow streams reject this option. Use
    /// [`build_arrow`](Self::build_arrow) without `.multiplexed(...)`.
    #[cfg(feature = "arrow-flight")]
    pub fn stats_exporter<E>(mut self, exporter: E) -> Self
    where
        E: crate::stats::StatsExporter + 'static,
    {
        self.stats_exporter = Some(Arc::new(exporter));
        self
    }

    /// Set the maximum number of in-flight Arrow batches (Arrow streams only).
    /// For a mux, each lane receives `n / stream_count` slots; any remainder is
    /// unused. The budget must be at least the lane count.
    #[cfg(feature = "arrow-flight")]
    pub fn max_inflight_batches(mut self, n: usize) -> Self {
        self.settings.max_inflight_batches = n;
        self
    }

    /// Set the connection timeout in milliseconds for Arrow Flight (Arrow streams only).
    #[cfg(feature = "arrow-flight")]
    pub fn connection_timeout_ms(mut self, ms: u64) -> Self {
        self.settings.connection_timeout_ms = ms;
        self
    }

    /// Set the Arrow IPC compression type (Arrow streams only).
    ///
    /// Defaults to `Some(CompressionType::ZSTD)`. Pass `None` to send uncompressed payloads.
    /// Of this and [`compression`](Self::compression), the last call wins.
    #[cfg(feature = "arrow-flight")]
    #[deprecated(since = "3.0.0", note = "Use `compression` instead")]
    #[allow(deprecated)]
    pub fn ipc_compression(mut self, compression: Option<arrow_ipc::CompressionType>) -> Self {
        self.settings.ipc_compression = compression;
        self.settings.compression = crate::ArrowCompression::FallbackToLegacy;
        self
    }

    /// Set the compression for Arrow Flight payloads (Arrow streams only).
    ///
    /// Defaults to [`ArrowCompression::FallbackToLegacy`](crate::ArrowCompression::FallbackToLegacy),
    /// which applies the deprecated `ipc_compression` setting: IPC zstd unless changed.
    /// Of this and `ipc_compression`, the last call wins. See
    /// [`ArrowCompression`](crate::ArrowCompression) for the options.
    #[cfg(feature = "arrow-flight")]
    pub fn compression(mut self, compression: crate::ArrowCompression) -> Self {
        self.settings.compression = compression;
        self
    }

    /// Set how dictionary-encoded columns are sent (Arrow streams only).
    ///
    /// Zerobus does not support Arrow dictionaries yet, so keep the default,
    /// `Hydrate`. Until it does, `Resend` or `Delta` with dictionary-encoded columns
    /// makes stream creation fail with
    /// [`ZerobusError::InvalidSchema`](crate::ZerobusError::InvalidSchema). See
    /// [`ArrowDictionaryHandling`] for the modes.
    #[cfg(feature = "arrow-flight")]
    pub fn dictionary_handling(mut self, handling: ArrowDictionaryHandling) -> Self {
        self.settings.dictionary_handling = handling;
        self
    }

    /// The gRPC stream options, built from the core and the stream settings. The
    /// literal names every field, so a new options field does not compile until
    /// it is given a source.
    fn grpc_options(&self, record_type: RecordType) -> StreamConfigurationOptions {
        let (core, s) = (&self.core, &self.settings);
        StreamConfigurationOptions {
            record_type,
            recovery: core.recovery,
            recovery_retries: core.recovery_retries,
            recovery_backoff_ms: core.recovery_backoff_ms,
            server_lack_of_ack_timeout_ms: core.server_lack_of_ack_timeout_ms,
            max_ingest_payload_bytes: core.max_ingest_payload_bytes,
            recovery_timeout_ms: s.recovery_timeout_ms,
            flush_timeout_ms: s.flush_timeout_ms,
            stream_paused_max_wait_time_ms: s.stream_paused_max_wait_time_ms,
            max_inflight_requests: s.max_inflight_requests,
            ack_callback: s.ack_callback.clone(),
            callback_max_wait_time_ms: s.callback_max_wait_time_ms,
        }
    }

    /// The Arrow stream options, built from the core and the stream settings.
    /// Arrow has no payload limit, so `max_ingest_payload_bytes` is not used.
    #[cfg(feature = "arrow-flight")]
    #[allow(deprecated)]
    fn arrow_options(&self) -> ArrowStreamConfigurationOptions {
        let (core, s) = (&self.core, &self.settings);
        ArrowStreamConfigurationOptions {
            recovery: core.recovery,
            recovery_retries: core.recovery_retries,
            recovery_backoff_ms: core.recovery_backoff_ms,
            server_lack_of_ack_timeout_ms: core.server_lack_of_ack_timeout_ms,
            recovery_timeout_ms: s.recovery_timeout_ms,
            flush_timeout_ms: s.flush_timeout_ms,
            stream_paused_max_wait_time_ms: s.stream_paused_max_wait_time_ms,
            max_inflight_batches: s.max_inflight_batches,
            connection_timeout_ms: s.connection_timeout_ms,
            ipc_compression: s.ipc_compression,
            compression: s.compression,
            dictionary_handling: s.dictionary_handling,
        }
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
        self.core.validate_target()?;
        if self.core.format.is_none() {
            return Err(ZerobusError::InvalidArgument(
                "record format is required: call .json(), .compiled_proto(), .dynamic_proto(), .avro(), or .arrow()".into(),
            ));
        }
        #[cfg(feature = "arrow-flight")]
        if self.stats_exporter.is_some()
            && !matches!(self.core.format, Some(FormatConfig::Arrow(_)))
        {
            return Err(ZerobusError::InvalidArgument(
                "stats_exporter is only supported for Arrow Flight streams; use .arrow()".into(),
            ));
        }
        Ok(())
    }

    /// Resolve the headers provider from the stored auth config. For OAuth and
    /// federation, a proactive token refresh is bounded by half the stream's
    /// recovery timeout.
    fn resolve_headers_provider(&self) -> ZerobusResult<Arc<dyn HeadersProvider>> {
        // Give a proactive refresh half the recovery timeout so a stalled
        // refresh falls back to the cached token before the setup deadline
        // cancels the request.
        let refresh_timeout = Duration::from_millis(self.settings.recovery_timeout_ms) / 2;
        self.core.resolve_headers_provider(refresh_timeout)
    }

    /// Validates the builder and resolves the pieces shared by the gRPC
    /// terminals (`build`, and the persistent builder's `build` / `resume`):
    /// the channel, table properties, headers provider, and finalized config
    /// with the record type set. Rejects the Arrow format (which needs
    /// `build_arrow()`).
    pub(crate) async fn prepare_grpc(mut self) -> ZerobusResult<GrpcStreamParts> {
        // `validate()` already rejects `stats_exporter` on a non-Arrow format, and the
        // format match below rejects an Arrow format outright.
        self.validate()?;

        // Avro schemas are parsed here, before any connection is opened. The raw JSON and
        // parsed schema remain together in TableProperties for stream creation and ingest.
        let format = self
            .core
            .format
            .take()
            .expect("format was validated")
            .into_grpc()?;
        let headers_provider = self.resolve_headers_provider()?;

        let config = self.grpc_options(format.record_type);
        let table_properties = TableProperties {
            table_name: self.core.table_name,
            descriptor_proto: format.descriptor_proto,
            message_descriptor: format.message_descriptor,
            #[cfg(feature = "avro")]
            avro_schema: format.avro_schema,
        };

        let channel = self.core.sdk.grpc_channel_source();
        Ok(GrpcStreamParts {
            channel,
            table_properties,
            headers_provider,
            config,
        })
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
        let schema = self.validate_arrow()?;
        self.warn_arrow_ignored_options();

        let headers_provider = self.resolve_headers_provider()?;
        let options = self.arrow_options();

        let table_properties = ArrowTableProperties {
            table_name: self.core.table_name,
            schema,
        };

        let table_name = table_properties.table_name.clone();
        let stream = ZerobusArrowStream::new(
            &self.core.sdk.zerobus_endpoint,
            Arc::clone(&self.core.sdk.tls_config),
            self.core.sdk.connector_factory.clone(),
            table_properties,
            headers_provider,
            options,
            Arc::clone(&self.core.sdk.sdk_identifier),
            self.stats_exporter,
        )
        .await?;
        record_stream_creation(&table_name);
        Ok(stream)
    }

    #[cfg(feature = "arrow-flight")]
    #[allow(clippy::result_large_err)]
    fn arrow_schema(&self) -> ZerobusResult<&Arc<ArrowSchema>> {
        match self.core.format.as_ref() {
            Some(FormatConfig::Arrow(schema)) => Ok(schema),
            Some(_) => Err(ZerobusError::InvalidArgument(
                "non-Arrow format requires .build() instead of .build_arrow()".into(),
            )),
            None => Err(ZerobusError::InvalidArgument(
                "record format is required: call .arrow() before .build_arrow()".into(),
            )),
        }
    }

    #[cfg(feature = "arrow-flight")]
    fn validate_arrow(&self) -> ZerobusResult<Arc<ArrowSchema>> {
        let schema = Arc::clone(self.arrow_schema()?);

        if self.settings.ack_callback.is_some() || self.multiplexed_callback.is_some() {
            return Err(ZerobusError::InvalidArgument(
                "ack_callback and multiplexed_ack_callback are not supported for Arrow Flight streams"
                    .into(),
            ));
        }

        // Arrow-only: a zero bound deadlocks ingest / panics the channel. Not in the
        // shared validate() since it's irrelevant to JSON/proto build().
        if self.settings.max_inflight_batches == 0 {
            return Err(ZerobusError::InvalidArgument(
                "max_inflight_batches must be greater than 0".into(),
            ));
        }

        Ok(schema)
    }

    #[cfg(feature = "arrow-flight")]
    fn warn_arrow_ignored_options(&self) {
        // `max_ingest_payload_bytes` only applies to JSON and Protocol Buffer streams; warn if the
        // user changed it from the default before building an Arrow stream.
        if self.core.max_ingest_payload_bytes
            != crate::stream_options::defaults::MAX_INGEST_PAYLOAD_BYTES
        {
            warn_payload_limit_ignored_for_arrow();
        }
    }
}

#[cfg(test)]
mod tests;
