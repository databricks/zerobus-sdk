//! Builder API for creating Zerobus SDK instances and ingestion streams.
//!
//! This module provides fluent builder patterns for configuring and creating
//! SDK instances and streams.
//!
//! # Examples
//!
//! ## SDK Builder
//!
//! ```no_run
//! use databricks_zerobus_ingest_sdk::ZerobusSdkBuilder;
//! let server_endpoint = std::env::var("ZEROBUS_SERVER_ENDPOINT").expect("endpoint must be set");
//! let workspace_url = std::env::var("DATABRICKS_WORKSPACE_URL").expect("workspace URL must be set");
//!
//! let sdk = ZerobusSdkBuilder::new()
//!     .endpoint(server_endpoint)
//!     .unity_catalog_url(workspace_url)
//!     .build()?;
//! # Ok::<(), databricks_zerobus_ingest_sdk::ZerobusError>(())
//! ```
//!
//! ## Stream Builder
//!
//! ```rust,ignore
//! let stream = sdk
//!     .stream_builder()
//!     .table("catalog.schema.table")
//!     .oauth("client-id", "client-secret")
//!     .json()
//!     .build()
//!     .await?;
//! ```

mod client_builder_core;
#[cfg(feature = "eos")]
mod persistent_stream_builder;
mod sdk_builder;
mod stream_builder;
#[cfg(feature = "unary")]
mod unary_client_builder;

#[cfg(feature = "eos")]
pub use persistent_stream_builder::PersistentStreamBuilder;
pub use sdk_builder::ZerobusSdkBuilder;
pub use stream_builder::{MultiplexedStreamBuilder, StreamBuilder};
#[cfg(feature = "unary")]
pub use unary_client_builder::UnaryClientBuilder;
