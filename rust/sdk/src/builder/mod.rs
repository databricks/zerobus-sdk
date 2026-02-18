//! Builder API for creating Zerobus SDK instances.
//!
//! This module provides a fluent builder pattern for configuring and creating
//! SDK instances.
//!
//! # Examples
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

mod sdk_builder;

pub use sdk_builder::ZerobusSdkBuilder;
