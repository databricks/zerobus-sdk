//! Builder for creating [`ZerobusStream`] instances with typestate pattern.

use std::marker::PhantomData;
use std::sync::Arc;

use crate::databricks::zerobus::RecordType;
use crate::headers_provider::{OAuthHeadersProvider, DEFAULT_USER_AGENT};
use crate::{
    HeadersProvider, StreamConfigurationOptions, TableProperties, ZerobusResult, ZerobusSdk,
    ZerobusStream,
};

use super::record_type_markers::{Json, Proto};
use super::stages::{Configured, NeedsAuth, NeedsProvider, ReadyJson, ReadyProto};
use super::{AuthMethod, BuilderConfig, ClientCredentials, SchemaConfig};

#[cfg(feature = "arrow-flight")]
use super::arrow_stream_builder::ArrowStreamBuilder;
#[cfg(feature = "arrow-flight")]
use super::stages::ReadyArrow;

/// Builder for creating [`ZerobusStream`] instances with compile-time safety.
///
/// This builder uses the typestate pattern to ensure that required configuration
/// steps are completed before building the stream. The stages are:
///
/// 1. **NeedsAuth**: Initial stage - must call `client_credentials()` or `unauthenticated()`
/// 2. **NeedsProvider**: Called `unauthenticated()` - must call `headers_provider()`
/// 3. **Configured**: Authentication set - can configure options and select schema
/// 4. **ReadyProto/ReadyJson**: Schema selected - can build the stream
///
/// # Examples
///
/// ## OAuth Authentication (most common)
///
/// ```no_run
/// # use databricks_zerobus_ingest_sdk::{ZerobusSdk, ZerobusError};
/// # async fn example(sdk: &ZerobusSdk, descriptor: prost_types::DescriptorProto) -> Result<(), ZerobusError> {
/// let stream = sdk.stream_builder("catalog.schema.table")
///     .client_credentials("client-id", "client-secret")
///     .max_inflight_requests(100_000)
///     .recovery(true)
///     .proto(descriptor)
///     .build()
///     .await?;
/// # Ok(())
/// # }
/// ```
///
/// ## Unauthenticated with Custom Headers Provider
///
/// ```no_run
/// # use databricks_zerobus_ingest_sdk::{ZerobusSdk, ZerobusError, HeadersProvider, ZerobusResult};
/// # use std::sync::Arc;
/// # use std::collections::HashMap;
/// # struct MyProvider;
/// # #[async_trait::async_trait]
/// # impl HeadersProvider for MyProvider {
/// #     async fn get_headers(&self) -> ZerobusResult<HashMap<&'static str, String>> { Ok(HashMap::new()) }
/// # }
/// # async fn example(sdk: &ZerobusSdk) -> Result<(), ZerobusError> {
/// let stream = sdk.stream_builder("catalog.schema.table")
///     .unauthenticated()
///     .headers_provider(Arc::new(MyProvider))
///     .json()
///     .build()
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct StreamBuilder<'sdk, Stage> {
    sdk: &'sdk ZerobusSdk,
    table_name: String,
    auth: Option<AuthMethod>,
    config: BuilderConfig,
    schema: Option<SchemaConfig>,
    _stage: PhantomData<Stage>,
}

// ============================================================================
// NeedsAuth stage - must choose authentication method
// ============================================================================

impl<'sdk> StreamBuilder<'sdk, NeedsAuth> {
    /// Creates a new stream builder for the given table.
    ///
    /// This is typically called via [`ZerobusSdk::stream_builder`].
    pub(crate) fn new(sdk: &'sdk ZerobusSdk, table_name: String) -> Self {
        Self {
            sdk,
            table_name,
            auth: None,
            config: BuilderConfig::default(),
            schema: None,
            _stage: PhantomData,
        }
    }

    /// Configures OAuth2 client credentials authentication.
    ///
    /// This is the standard authentication method for production use.
    /// The SDK will use these credentials to obtain OAuth tokens automatically.
    ///
    /// # Arguments
    ///
    /// * `client_id` - OAuth client ID
    /// * `client_secret` - OAuth client secret
    pub fn client_credentials(
        self,
        client_id: impl Into<String>,
        client_secret: impl Into<String>,
    ) -> StreamBuilder<'sdk, Configured> {
        StreamBuilder {
            sdk: self.sdk,
            table_name: self.table_name,
            auth: Some(AuthMethod::OAuth(ClientCredentials {
                client_id: client_id.into(),
                client_secret: client_secret.into(),
            })),
            config: self.config,
            schema: None,
            _stage: PhantomData,
        }
    }

    /// Configures the stream for unauthenticated access.
    ///
    /// This is intended for local testing against a server that doesn't require
    /// authentication. You **must** call `headers_provider()` after this to
    /// provide a custom headers provider.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use databricks_zerobus_ingest_sdk::{ZerobusSdk, ZerobusError, HeadersProvider, ZerobusResult};
    /// # use std::sync::Arc;
    /// # use std::collections::HashMap;
    /// # struct NoOpProvider;
    /// # #[async_trait::async_trait]
    /// # impl HeadersProvider for NoOpProvider {
    /// #     async fn get_headers(&self) -> ZerobusResult<HashMap<&'static str, String>> { Ok(HashMap::new()) }
    /// # }
    /// # async fn example(sdk: &ZerobusSdk) -> Result<(), ZerobusError> {
    /// let stream = sdk.stream_builder("table")
    ///     .unauthenticated()
    ///     .headers_provider(Arc::new(NoOpProvider))
    ///     .json()
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn unauthenticated(self) -> StreamBuilder<'sdk, NeedsProvider> {
        StreamBuilder {
            sdk: self.sdk,
            table_name: self.table_name,
            auth: Some(AuthMethod::Unauthenticated),
            config: self.config,
            schema: None,
            _stage: PhantomData,
        }
    }
}

// ============================================================================
// NeedsProvider stage - must provide headers provider after unauthenticated()
// ============================================================================

impl<'sdk> StreamBuilder<'sdk, NeedsProvider> {
    /// Provides a custom headers provider.
    ///
    /// This is **required** after calling `unauthenticated()` since there's no
    /// default OAuth provider available.
    ///
    /// # Arguments
    ///
    /// * `provider` - An `Arc` holding your custom [`HeadersProvider`] implementation
    pub fn headers_provider(
        mut self,
        provider: Arc<dyn HeadersProvider>,
    ) -> StreamBuilder<'sdk, Configured> {
        self.config.headers_provider = Some(provider);
        StreamBuilder {
            sdk: self.sdk,
            table_name: self.table_name,
            auth: self.auth,
            config: self.config,
            schema: None,
            _stage: PhantomData,
        }
    }
}

// ============================================================================
// Configured stage - can set options and select schema
// ============================================================================

impl<'sdk> StreamBuilder<'sdk, Configured> {
    /// Provides a custom headers provider, overriding the default OAuth provider.
    ///
    /// This is optional when using `client_credentials()` - the default OAuth
    /// provider will be used if not specified.
    ///
    /// # Arguments
    ///
    /// * `provider` - An `Arc` holding your custom [`HeadersProvider`] implementation
    pub fn headers_provider(mut self, provider: Arc<dyn HeadersProvider>) -> Self {
        self.config.headers_provider = Some(provider);
        self
    }

    /// Sets the maximum number of inflight requests.
    ///
    /// This limit controls memory usage and backpressure. When this limit is reached,
    /// `ingest_record()` calls will block until acknowledgments free up space.
    ///
    /// Default: 1,000,000
    pub fn max_inflight_requests(mut self, max: usize) -> Self {
        self.config.max_inflight_requests = Some(max);
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
    /// If no acknowledgement is received within this time (and there are pending records),
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

    /// Sets the maximum time in milliseconds to wait during graceful stream close.
    ///
    /// Configuration values:
    /// - `None`: Wait for the full server-specified duration (most graceful)
    /// - `Some(0)`: Immediate recovery, close stream right away
    /// - `Some(x)`: Wait up to min(x, server_duration) milliseconds
    ///
    /// Default: `None`
    pub fn stream_paused_max_wait_time_ms(mut self, ms: Option<u64>) -> Self {
        self.config.stream_paused_max_wait_time_ms = ms;
        self
    }

    /// Configures the stream for protobuf-encoded records with the given descriptor.
    ///
    /// # Arguments
    ///
    /// * `descriptor` - The protobuf descriptor for the record schema
    pub fn proto(self, descriptor: prost_types::DescriptorProto) -> StreamBuilder<'sdk, ReadyProto> {
        StreamBuilder {
            sdk: self.sdk,
            table_name: self.table_name,
            auth: self.auth,
            config: self.config,
            schema: Some(SchemaConfig::Proto(Box::new(descriptor))),
            _stage: PhantomData,
        }
    }

    /// Configures the stream for JSON-encoded records.
    ///
    /// JSON streams do not require a schema descriptor.
    pub fn json(self) -> StreamBuilder<'sdk, ReadyJson> {
        StreamBuilder {
            sdk: self.sdk,
            table_name: self.table_name,
            auth: self.auth,
            config: self.config,
            schema: Some(SchemaConfig::Json),
            _stage: PhantomData,
        }
    }

    /// Configures the stream for Arrow Flight ingestion with the given schema.
    ///
    /// **Experimental/Unsupported**: Arrow Flight ingestion is experimental and not yet
    /// supported for production use. The API may change in future releases.
    ///
    /// # Arguments
    ///
    /// * `schema` - The Arrow schema for the record batches
    #[cfg(feature = "arrow-flight")]
    pub fn arrow(
        self,
        schema: std::sync::Arc<arrow_schema::Schema>,
    ) -> ArrowStreamBuilder<'sdk, ReadyArrow> {
        ArrowStreamBuilder::from_configured(
            self.sdk,
            self.table_name,
            self.auth,
            self.config,
            schema,
        )
    }
}

// ============================================================================
// ReadyProto stage - build Proto stream
// ============================================================================

impl<'sdk> StreamBuilder<'sdk, ReadyProto> {
    /// Builds and connects the Proto stream.
    ///
    /// # Returns
    ///
    /// A [`ZerobusStream<Proto>`] for type-safe Proto record ingestion.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Stream creation fails
    /// - OAuth authentication fails
    /// - The table doesn't exist or is inaccessible
    pub async fn build(self) -> ZerobusResult<ZerobusStream<Proto>> {
        let stream = self.build_grpc_stream(RecordType::Proto).await?;
        Ok(stream.into_typed())
    }
}

// ============================================================================
// ReadyJson stage - build JSON stream
// ============================================================================

impl<'sdk> StreamBuilder<'sdk, ReadyJson> {
    /// Builds and connects the JSON stream.
    ///
    /// # Returns
    ///
    /// A [`ZerobusStream<Json>`] for type-safe JSON record ingestion.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Stream creation fails
    /// - OAuth authentication fails
    /// - The table doesn't exist or is inaccessible
    pub async fn build(self) -> ZerobusResult<ZerobusStream<Json>> {
        let stream = self.build_grpc_stream(RecordType::Json).await?;
        Ok(stream.into_typed())
    }
}

// ============================================================================
// Shared implementation for building gRPC streams
// ============================================================================

impl<'sdk, Stage> StreamBuilder<'sdk, Stage> {
    async fn build_grpc_stream(self, record_type: RecordType) -> ZerobusResult<ZerobusStream> {
        let descriptor_proto = match &self.schema {
            Some(SchemaConfig::Proto(desc)) => Some((**desc).clone()),
            _ => None,
        };

        let table_properties = TableProperties {
            table_name: self.table_name.clone(),
            descriptor_proto,
        };

        let options = StreamConfigurationOptions {
            max_inflight_requests: self.config.max_inflight_requests.unwrap_or(1_000_000),
            recovery: self.config.recovery,
            recovery_timeout_ms: self.config.recovery_timeout_ms,
            recovery_backoff_ms: self.config.recovery_backoff_ms,
            recovery_retries: self.config.recovery_retries,
            server_lack_of_ack_timeout_ms: self.config.server_lack_of_ack_timeout_ms,
            flush_timeout_ms: self.config.flush_timeout_ms,
            record_type,
            stream_paused_max_wait_time_ms: self.config.stream_paused_max_wait_time_ms,
        };

        // Determine the headers provider to use
        let headers_provider: Arc<dyn HeadersProvider> =
            if let Some(custom_provider) = self.config.headers_provider {
                // Custom provider overrides everything
                custom_provider
            } else {
                // Use default based on auth method
                match self.auth.expect("auth must be set") {
                    AuthMethod::OAuth(creds) => Arc::new(OAuthHeadersProvider::new(
                        creds.client_id,
                        creds.client_secret,
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
            .create_stream_with_headers_provider(table_properties, headers_provider, Some(options))
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
    fn test_builder_typestate_client_credentials() {
        let sdk = create_test_sdk();

        // NeedsAuth -> Configured via client_credentials
        let _configured: StreamBuilder<'_, Configured> = sdk
            .stream_builder("test.table")
            .client_credentials("id", "secret");
    }

    #[test]
    fn test_builder_typestate_unauthenticated() {
        let sdk = create_test_sdk();

        // NeedsAuth -> NeedsProvider via unauthenticated
        let _needs_provider: StreamBuilder<'_, NeedsProvider> =
            sdk.stream_builder("test.table").unauthenticated();

        // NeedsProvider -> Configured via headers_provider
        struct DummyProvider;
        #[async_trait::async_trait]
        impl HeadersProvider for DummyProvider {
            async fn get_headers(
                &self,
            ) -> ZerobusResult<std::collections::HashMap<&'static str, String>> {
                Ok(std::collections::HashMap::new())
            }
        }
        let _configured: StreamBuilder<'_, Configured> = sdk
            .stream_builder("test.table")
            .unauthenticated()
            .headers_provider(Arc::new(DummyProvider));
    }

    #[test]
    fn test_builder_configuration_methods() {
        let sdk = create_test_sdk();

        let builder = sdk
            .stream_builder("test.table")
            .client_credentials("id", "secret")
            .max_inflight_requests(50_000)
            .recovery(false)
            .recovery_timeout_ms(20_000)
            .recovery_backoff_ms(3_000)
            .recovery_retries(5)
            .server_ack_timeout_ms(90_000)
            .flush_timeout_ms(600_000)
            .stream_paused_max_wait_time_ms(Some(5_000));

        assert_eq!(builder.config.max_inflight_requests, Some(50_000));
        assert!(!builder.config.recovery);
        assert_eq!(builder.config.recovery_timeout_ms, 20_000);
        assert_eq!(builder.config.recovery_backoff_ms, 3_000);
        assert_eq!(builder.config.recovery_retries, 5);
        assert_eq!(builder.config.server_lack_of_ack_timeout_ms, 90_000);
        assert_eq!(builder.config.flush_timeout_ms, 600_000);
        assert_eq!(builder.config.stream_paused_max_wait_time_ms, Some(5_000));
    }

    #[test]
    fn test_builder_proto_schema() {
        let sdk = create_test_sdk();

        let descriptor = prost_types::DescriptorProto::default();
        let builder: StreamBuilder<'_, ReadyProto> = sdk
            .stream_builder("test.table")
            .client_credentials("id", "secret")
            .proto(descriptor);

        assert!(matches!(builder.schema, Some(SchemaConfig::Proto(_))));
    }

    #[test]
    fn test_builder_json_schema() {
        let sdk = create_test_sdk();

        let builder: StreamBuilder<'_, ReadyJson> = sdk
            .stream_builder("test.table")
            .client_credentials("id", "secret")
            .json();

        assert!(matches!(builder.schema, Some(SchemaConfig::Json)));
    }
}
