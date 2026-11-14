//! Test-only support for this repository: a mock Zerobus gRPC server and the
//! server-side bindings it is built on.
//!
//! The bindings are generated here from the SDK's `zerobus_service.proto`
//! rather than taken from the SDK, so the SDK's own generated types stay private.

// tonic's generated server trait goes through async-trait, which Clippy >= 1.99 flags as double_must_use.
#[allow(clippy::double_must_use, clippy::enum_variant_names)]
pub mod databricks {
    pub mod zerobus {
        tonic::include_proto!("databricks.zerobus");
    }
}

pub mod mock_grpc;
