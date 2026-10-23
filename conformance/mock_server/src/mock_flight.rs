//! Arrow Flight data plane (`arrow.flight.protocol.FlightService`, `DoPut`).
//!
//! TODO: not implemented in this slice. Not registered in `serve_with_shutdown`.
//! When added, implement `FlightService` here (the existing rust/tests
//! `mock_arrow_flight.rs` is the reference) and register it on the same port.
//!
//! Dependency note: do not add `arrow-flight` from crates.io blindly. `rust/` uses a
//! vendored, patched `arrow-flight` (`rust/third_party/arrow-flight`), and a second copy
//! here would produce two incompatible versions once `rust/tests` imports this crate.
//! Pick the source before adding it.
