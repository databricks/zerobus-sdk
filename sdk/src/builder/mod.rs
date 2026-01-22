//! Builder API for creating Zerobus SDK instances and streams.
//!
//! This module provides a fluent builder pattern for configuring and creating
//! SDK instances and ingestion streams with compile-time safety for required
//! configuration steps.
//!
//! # Examples
//!
//! ## SDK Builder
//!
//! ```no_run
//! use databricks_zerobus_ingest_sdk::ZerobusSdkBuilder;
//!
//! let sdk = ZerobusSdkBuilder::new()
//!     .endpoint("https://workspace.zerobus.databricks.com")
//!     .unity_catalog_url("https://workspace.cloud.databricks.com")
//!     .build()?;
//! # Ok::<(), databricks_zerobus_ingest_sdk::ZerobusError>(())
//! ```
//!
//! ## Stream Builder (Proto with OAuth)
//!
//! ```no_run
//! # use databricks_zerobus_ingest_sdk::{ZerobusSdk, ZerobusError};
//! # async fn example(sdk: ZerobusSdk, descriptor: prost_types::DescriptorProto) -> Result<(), ZerobusError> {
//! let stream = sdk.stream_builder("catalog.schema.table")
//!     .client_credentials("client-id", "client-secret")
//!     .max_inflight_requests(100_000)
//!     .recovery(true)
//!     .proto(descriptor)
//!     .build()
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Stream Builder (JSON)
//!
//! ```no_run
//! # use databricks_zerobus_ingest_sdk::{ZerobusSdk, ZerobusError};
//! # async fn example(sdk: ZerobusSdk) -> Result<(), ZerobusError> {
//! let stream = sdk.stream_builder("catalog.schema.table")
//!     .client_credentials("client-id", "client-secret")
//!     .json()
//!     .build()
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Stream Builder (Unauthenticated with custom headers provider)
//!
//! ```no_run
//! # use databricks_zerobus_ingest_sdk::{ZerobusSdk, ZerobusError, HeadersProvider, ZerobusResult};
//! # use std::sync::Arc;
//! # use std::collections::HashMap;
//! # struct MyHeadersProvider;
//! # #[async_trait::async_trait]
//! # impl HeadersProvider for MyHeadersProvider {
//! #     async fn get_headers(&self) -> ZerobusResult<HashMap<&'static str, String>> {
//! #         Ok(HashMap::new())
//! #     }
//! # }
//! # async fn example(sdk: ZerobusSdk) -> Result<(), ZerobusError> {
//! let stream = sdk.stream_builder("catalog.schema.table")
//!     .unauthenticated()
//!     .headers_provider(Arc::new(MyHeadersProvider))
//!     .json()
//!     .build()
//!     .await?;
//! # Ok(())
//! # }
//! ```

#[cfg(feature = "arrow-flight")]
mod arrow_stream_builder;
mod sdk_builder;
mod stream_builder;

#[cfg(feature = "arrow-flight")]
pub use arrow_stream_builder::ArrowStreamBuilder;
pub use sdk_builder::ZerobusSdkBuilder;
pub use stream_builder::StreamBuilder;

/// Marker types for stream record types.
///
/// These are used as generic parameters on [`ZerobusStream`](crate::ZerobusStream)
/// to enable compile-time checking of record types passed to ingestion methods.
pub mod record_type_markers {
    use crate::{JsonString, JsonValue, ProtoBytes, ProtoMessage};

    /// Marker for Proto-encoded streams.
    #[derive(Debug, Clone, Copy)]
    pub struct Proto;

    /// Marker for JSON-encoded streams.
    #[derive(Debug, Clone, Copy)]
    pub struct Json;

    /// Marker for dynamically-typed streams (runtime checking).
    ///
    /// This is used by the legacy `create_stream()` API for backward compatibility.
    #[deprecated(
        since = "0.5.0",
        note = "Use `ZerobusStream<Proto>` or `ZerobusStream<Json>` via stream_builder() instead"
    )]
    #[derive(Debug, Clone, Copy)]
    pub struct Dynamic;

    /// Trait that marks a payload type as acceptable for a given stream record type.
    ///
    /// This trait enables compile-time checking that the record type being ingested
    /// matches the stream's expected record type.
    ///
    /// # Examples
    ///
    /// With a `ZerobusStream<Proto>`, only Proto record types are accepted:
    ///
    /// ```ignore
    /// let proto_stream: ZerobusStream<Proto> = /* ... */;
    /// proto_stream.ingest_record_offset(vec![1, 2, 3]).await?;  // OK: Vec<u8> is Proto
    /// proto_stream.ingest_record_offset("json".to_string()).await?;  // Compile error!
    /// ```
    ///
    /// With a `ZerobusStream<Dynamic>` (legacy), all record types are accepted:
    ///
    /// ```ignore
    /// let dynamic_stream: ZerobusStream<Dynamic> = /* ... */;
    /// dynamic_stream.ingest_record_offset(vec![1, 2, 3]).await?;  // OK
    /// dynamic_stream.ingest_record_offset("json".to_string()).await?;  // OK
    /// ```
    pub trait AcceptsRecord<Payload> {}

    // Proto streams accept Proto record types
    impl AcceptsRecord<Vec<u8>> for Proto {}
    impl AcceptsRecord<ProtoBytes> for Proto {}
    impl<T: prost::Message> AcceptsRecord<ProtoMessage<T>> for Proto {}

    // Json streams accept Json record types
    impl AcceptsRecord<String> for Json {}
    impl AcceptsRecord<JsonString> for Json {}
    impl<T: serde::Serialize> AcceptsRecord<JsonValue<T>> for Json {}

    // Dynamic streams accept all record types (for backward compatibility)
    #[allow(deprecated)]
    impl AcceptsRecord<Vec<u8>> for Dynamic {}
    #[allow(deprecated)]
    impl AcceptsRecord<ProtoBytes> for Dynamic {}
    #[allow(deprecated)]
    impl<T: prost::Message> AcceptsRecord<ProtoMessage<T>> for Dynamic {}
    #[allow(deprecated)]
    impl AcceptsRecord<String> for Dynamic {}
    #[allow(deprecated)]
    impl AcceptsRecord<JsonString> for Dynamic {}
    #[allow(deprecated)]
    impl<T: serde::Serialize> AcceptsRecord<JsonValue<T>> for Dynamic {}
}

/// Typestate markers for builder stages.
///
/// These zero-sized types are used to enforce compile-time constraints
/// on the builder API, ensuring that required configuration steps are
/// completed before building.
pub mod stages {
    /// Initial stage - authentication method is required before proceeding.
    #[derive(Debug, Clone, Copy)]
    pub struct NeedsAuth;

    /// Called `.unauthenticated()` - must provide a custom headers provider.
    #[derive(Debug, Clone, Copy)]
    pub struct NeedsProvider;

    /// Authenticated stage - can configure options and select schema type.
    #[derive(Debug, Clone, Copy)]
    pub struct Configured;

    /// Ready to build a Proto-encoded gRPC stream.
    #[derive(Debug, Clone, Copy)]
    pub struct ReadyProto;

    /// Ready to build a JSON-encoded gRPC stream.
    #[derive(Debug, Clone, Copy)]
    pub struct ReadyJson;

    /// Ready to build an Arrow Flight stream.
    #[cfg(feature = "arrow-flight")]
    #[derive(Debug, Clone, Copy)]
    pub struct ReadyArrow;
}

use std::sync::Arc;

use crate::HeadersProvider;

/// Client credentials for OAuth authentication.
#[derive(Clone)]
pub(crate) struct ClientCredentials {
    pub client_id: String,
    pub client_secret: String,
}

/// Internal authentication configuration.
#[derive(Clone)]
pub(crate) enum AuthMethod {
    /// OAuth2 client credentials flow.
    OAuth(ClientCredentials),
    /// Unauthenticated (requires custom headers provider).
    Unauthenticated,
}

/// Internal builder configuration with defaults.
#[derive(Clone)]
pub(crate) struct BuilderConfig {
    pub max_inflight_requests: Option<usize>,
    pub recovery: bool,
    pub recovery_timeout_ms: u64,
    pub recovery_backoff_ms: u64,
    pub recovery_retries: u32,
    pub server_lack_of_ack_timeout_ms: u64,
    pub flush_timeout_ms: u64,
    pub stream_paused_max_wait_time_ms: Option<u64>,
    /// Custom headers provider (overrides default OAuth provider if set).
    pub headers_provider: Option<Arc<dyn HeadersProvider>>,
    // Arrow-specific fields
    #[cfg(feature = "arrow-flight")]
    pub connection_timeout_ms: u64,
    #[cfg(feature = "arrow-flight")]
    pub ipc_compression: Option<arrow_ipc::CompressionType>,
}

impl Default for BuilderConfig {
    fn default() -> Self {
        use crate::stream_options::defaults;

        Self {
            max_inflight_requests: None, // Will use type-specific default
            recovery: defaults::RECOVERY,
            recovery_timeout_ms: defaults::RECOVERY_TIMEOUT_MS,
            recovery_backoff_ms: defaults::RECOVERY_BACKOFF_MS,
            recovery_retries: defaults::RECOVERY_RETRIES,
            server_lack_of_ack_timeout_ms: defaults::SERVER_LACK_OF_ACK_TIMEOUT_MS,
            flush_timeout_ms: defaults::FLUSH_TIMEOUT_MS,
            stream_paused_max_wait_time_ms: None,
            headers_provider: None,
            #[cfg(feature = "arrow-flight")]
            connection_timeout_ms: defaults::CONNECTION_TIMEOUT_MS,
            #[cfg(feature = "arrow-flight")]
            ipc_compression: None,
        }
    }
}

/// Internal schema configuration for gRPC streams.
#[derive(Clone)]
pub(crate) enum SchemaConfig {
    /// Protobuf-encoded records with descriptor.
    Proto(Box<prost_types::DescriptorProto>),
    /// JSON-encoded records (no schema required).
    Json,
}
