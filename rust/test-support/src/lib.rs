//! Test-only support for this repository: a mock Zerobus gRPC server and the
//! server-side bindings it is built on.
//!
//! The bindings are generated here from the SDK's `zerobus_service.proto`
//! rather than taken from the SDK, so the SDK's own generated types stay private.

#[allow(clippy::enum_variant_names)]
pub mod databricks {
    pub mod zerobus {
        tonic::include_proto!("databricks.zerobus");
    }
}

pub mod mock_grpc;
