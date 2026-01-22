//! Builder for creating [`ZerobusArrowStream`] instances with typestate pattern.
//!
//! **Experimental/Unsupported**: Arrow Flight ingestion is experimental and not yet
//! supported for production use. The API may change in future releases.

use std::marker::PhantomData;
use std::sync::Arc;

use arrow_ipc::CompressionType;
use arrow_schema::Schema as ArrowSchema;

use crate::arrow_config::ArrowStreamConfigurationOptions;
use crate::arrow_stream::ArrowTableProperties;
use crate::headers_provider::{OAuthHeadersProvider, DEFAULT_USER_AGENT};
use crate::{HeadersProvider, ZerobusArrowStream, ZerobusResult, ZerobusSdk};

use super::stages::ReadyArrow;
use super::{AuthMethod, BuilderConfig, ClientCredentials};

/// Builder for creating [`ZerobusArrowStream`] instances with compile-time safety.
///
/// **Experimental/Unsupported**: Arrow Flight ingestion is experimental and not yet
/// supported for production use. The API may change in future releases.
///
/// This builder is typically created by calling `.arrow(schema)` on a
/// [`StreamBuilder`](super::StreamBuilder) in the `Configured` stage.
///
/// # Examples
///
/// ```no_run
/// # use databricks_zerobus_ingest_sdk::{ZerobusSdk, ZerobusError};
/// # use arrow_schema::{Schema as ArrowSchema, Field, DataType};
/// # use std::sync::Arc;
/// # use arrow_ipc::CompressionType;
/// # async fn example(sdk: &ZerobusSdk) -> Result<(), ZerobusError> {
/// let schema = Arc::new(ArrowSchema::new(vec![
///     Field::new("id", DataType::Int64, false),
///     Field::new("name", DataType::Utf8, true),
/// ]));
///
/// let stream = sdk.stream_builder("catalog.schema.table")
///     .client_credentials("client-id", "client-secret")
///     .max_inflight_requests(1_000)
///     .arrow(schema)
///     .ipc_compression(CompressionType::ZSTD)
///     .connection_timeout_ms(45_000)
///     .build()
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct ArrowStreamBuilder<'sdk, Stage> {
    sdk: &'sdk ZerobusSdk,
    table_name: String,
    auth: Option<AuthMethod>,
    config: BuilderConfig,
    schema: Arc<ArrowSchema>,
    _stage: PhantomData<Stage>,
}

impl<'sdk> ArrowStreamBuilder<'sdk, ReadyArrow> {
    /// Creates an ArrowStreamBuilder from a configured StreamBuilder.
    ///
    /// This is called internally when `.arrow(schema)` is invoked on a StreamBuilder.
    pub(crate) fn from_configured(
        sdk: &'sdk ZerobusSdk,
        table_name: String,
        auth: Option<AuthMethod>,
        config: BuilderConfig,
        schema: Arc<ArrowSchema>,
    ) -> Self {
        Self {
            sdk,
            table_name,
            auth,
            config,
            schema,
            _stage: PhantomData,
        }
    }

    /// Sets the maximum number of inflight batches.
    ///
    /// This limit controls memory usage and backpressure. When this limit is reached,
    /// `ingest_batch()` calls will block until acknowledgments free up space.
    ///
    /// Default: 1,000
    ///
    /// Note: This overrides any value set via `max_inflight_requests()` on the
    /// gRPC builder before calling `.arrow()`.
    pub fn max_inflight_batches(mut self, max: usize) -> Self {
        self.config.max_inflight_requests = Some(max);
        self
    }

    /// Sets the timeout in milliseconds for stream connection establishment.
    ///
    /// If the Arrow Flight stream cannot be established within this time,
    /// stream creation will fail.
    ///
    /// Default: 30,000 (30 seconds)
    pub fn connection_timeout_ms(mut self, ms: u64) -> Self {
        self.config.connection_timeout_ms = ms;
        self
    }

    /// Sets Arrow IPC compression for Flight payloads.
    ///
    /// Supported compression types:
    /// - `CompressionType::LZ4_FRAME` - LZ4 frame compression
    /// - `CompressionType::ZSTD` - Zstandard compression
    ///
    /// Default: `None` (no compression)
    pub fn ipc_compression(mut self, compression: CompressionType) -> Self {
        self.config.ipc_compression = Some(compression);
        self
    }

    /// Enables or disables automatic stream recovery on failure.
    ///
    /// When enabled, the SDK will automatically attempt to reconnect and recover
    /// the stream when encountering retryable errors.
    ///
    /// Default: `true`
    pub fn recovery(mut self, enabled: bool) -> Self {
        self.config.recovery = enabled;
        self
    }

    /// Sets the timeout in milliseconds for each stream recovery attempt.
    ///
    /// Default: 15,000 (15 seconds)
    pub fn recovery_timeout_ms(mut self, ms: u64) -> Self {
        self.config.recovery_timeout_ms = ms;
        self
    }

    /// Sets the backoff time in milliseconds between stream recovery retry attempts.
    ///
    /// Default: 2,000 (2 seconds)
    pub fn recovery_backoff_ms(mut self, ms: u64) -> Self {
        self.config.recovery_backoff_ms = ms;
        self
    }

    /// Sets the maximum number of recovery retry attempts before giving up.
    ///
    /// Default: 4
    pub fn recovery_retries(mut self, retries: u32) -> Self {
        self.config.recovery_retries = retries;
        self
    }

    /// Sets the timeout in milliseconds for waiting for server acknowledgements.
    ///
    /// If no acknowledgement is received within this time (and there are pending batches),
    /// the stream will be considered failed and recovery will be triggered.
    ///
    /// Default: 60,000 (60 seconds)
    pub fn server_ack_timeout_ms(mut self, ms: u64) -> Self {
        self.config.server_lack_of_ack_timeout_ms = ms;
        self
    }

    /// Sets the timeout in milliseconds for flush operations.
    ///
    /// Default: 300,000 (5 minutes)
    pub fn flush_timeout_ms(mut self, ms: u64) -> Self {
        self.config.flush_timeout_ms = ms;
        self
    }

    /// Builds and connects the Arrow Flight stream.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Stream creation fails
    /// - OAuth authentication fails
    /// - The table doesn't exist or is inaccessible
    /// - Connection timeout is exceeded
    pub async fn build(self) -> ZerobusResult<ZerobusArrowStream> {
        let table_properties = ArrowTableProperties {
            table_name: self.table_name.clone(),
            schema: self.schema,
        };

        let options = ArrowStreamConfigurationOptions {
            max_inflight_batches: self.config.max_inflight_requests.unwrap_or(1_000),
            recovery: self.config.recovery,
            recovery_timeout_ms: self.config.recovery_timeout_ms,
            recovery_backoff_ms: self.config.recovery_backoff_ms,
            recovery_retries: self.config.recovery_retries,
            server_lack_of_ack_timeout_ms: self.config.server_lack_of_ack_timeout_ms,
            flush_timeout_ms: self.config.flush_timeout_ms,
            connection_timeout_ms: self.config.connection_timeout_ms,
            ipc_compression: self.config.ipc_compression,
        };

        // Determine the headers provider to use
        let headers_provider: Arc<dyn HeadersProvider> =
            if let Some(custom_provider) = self.config.headers_provider {
                // Custom provider overrides everything
                custom_provider
            } else {
                // Use default based on auth method
                match self.auth.expect("auth must be set") {
                    AuthMethod::OAuth(ClientCredentials {
                        client_id,
                        client_secret,
                    }) => Arc::new(OAuthHeadersProvider::new(
                        client_id,
                        client_secret,
                        self.table_name.clone(),
                        self.sdk.workspace_id().to_string(),
                        self.sdk.unity_catalog_url.clone(),
                        DEFAULT_USER_AGENT.to_string(),
                    )),
                    AuthMethod::Unauthenticated => {
                        // This should never happen due to typestate enforcement
                        unreachable!(
                            "Unauthenticated requires headers_provider, enforced at compile time"
                        )
                    }
                }
            };

        self.sdk
            .create_arrow_stream_with_headers_provider(table_properties, headers_provider, Some(options))
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_sdk() -> ZerobusSdk {
        ZerobusSdk::new_with_config(
            "https://test-workspace.zerobus.databricks.com".to_string(),
            "https://test-workspace.cloud.databricks.com".to_string(),
            true,
            "test-workspace".to_string(),
        )
    }

    #[test]
    fn test_arrow_builder_configuration() {
        use arrow_schema::{DataType, Field};

        let sdk = create_test_sdk();
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]));

        let builder = sdk
            .stream_builder("test.table")
            .client_credentials("id", "secret")
            .arrow(schema)
            .max_inflight_batches(500)
            .connection_timeout_ms(45_000)
            .ipc_compression(CompressionType::ZSTD)
            .recovery(false);

        assert_eq!(builder.config.max_inflight_requests, Some(500));
        assert_eq!(builder.config.connection_timeout_ms, 45_000);
        assert_eq!(builder.config.ipc_compression, Some(CompressionType::ZSTD));
        assert!(!builder.config.recovery);
    }
}
