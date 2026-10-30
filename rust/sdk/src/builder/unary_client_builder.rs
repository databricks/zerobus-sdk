//! Fluent builder for unary ingestion clients (Beta).
//!
//! Reached via [`ZerobusSdk::unary_client_builder`](crate::ZerobusSdk::unary_client_builder).
//! Like [`StreamBuilder`](super::StreamBuilder), it wraps a
//! [`ClientBuilderCore`], which holds the table, auth, format, retry, and
//! payload-limit settings. It offers only the setters that apply to a unary
//! call.
//!
//! # Examples
//!
//! ```rust,ignore
//! let client = sdk
//!     .unary_client_builder()
//!     .table("catalog.schema.events")
//!     .oauth("client-id", "client-secret")
//!     .json()
//!     .build()
//!     .await?;
//! ```

use std::fmt;
use std::sync::Arc;

use super::client_builder_core::{ClientBuilderCore, FormatConfig};
use crate::headers_provider::{HeadersProvider, IdpTokenSupplier};
use crate::{ZerobusError, ZerobusResult, ZerobusSdk};

/// A fluent builder for a unary ingestion client (Beta).
///
/// Configures a client bound to one table. The setters have the same names
/// and defaults as the matching [`StreamBuilder`](crate::StreamBuilder)
/// setters; settings that only apply to streams are not offered. The builder
/// checks at `build()` time that table name, authentication, and format have
/// been configured. Use [`validate`](Self::validate) to check the builder
/// state without building the client.
///
/// Available behind the `unary` feature. The API is in development.
#[must_use = "a UnaryClientBuilder does nothing until `.build()` is called"]
pub struct UnaryClientBuilder<'a> {
    core: ClientBuilderCore<'a>,
}

impl fmt::Debug for UnaryClientBuilder<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnaryClientBuilder")
            .field("table_name", &self.core.table_name)
            .field("auth", &self.core.auth_kind())
            .field("format", &self.core.format_kind())
            .finish_non_exhaustive()
    }
}

#[allow(clippy::result_large_err)]
impl<'a> UnaryClientBuilder<'a> {
    pub(crate) fn new(sdk: &'a ZerobusSdk) -> Self {
        Self {
            core: ClientBuilderCore::new(sdk),
        }
    }

    /// Set the fully-qualified Unity Catalog table name (e.g., `"catalog.schema.table"`).
    pub fn table(mut self, table_name: impl Into<String>) -> Self {
        self.core.set_table(table_name.into());
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
    /// and unary clients to share its cached token; pass a distinct supplier to
    /// isolate.
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
    ///
    /// The client calls the provider before every call attempt, while a stream
    /// calls it only when the stream is opened or recovered. It should be cheap
    /// or cache its result. The built-in OAuth and federated providers cache
    /// tokens.
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

    /// Select JSON record format. Unary calls support only JSON.
    pub fn json(mut self) -> Self {
        self.core.set_format(FormatConfig::Json);
        self
    }

    /// Enable or disable automatic retries of a failed call.
    ///
    /// With retries on, a call that failed with a retryable error is sent
    /// again, so a batch may be written more than once (at-least-once). With
    /// retries off, a batch is never sent twice.
    pub fn recovery(mut self, enabled: bool) -> Self {
        self.core.set_recovery(enabled);
        self
    }

    /// Set the backoff time in milliseconds between retries of a failed call.
    pub fn recovery_backoff_ms(mut self, ms: u64) -> Self {
        self.core.set_recovery_backoff_ms(ms);
        self
    }

    /// Set the maximum number of retries of a failed call.
    pub fn recovery_retries(mut self, n: u32) -> Self {
        self.core.set_recovery_retries(n);
        self
    }

    /// Set the timeout in milliseconds for each call attempt.
    ///
    /// The attempt covers getting the headers, connecting if needed, and
    /// waiting for the server to confirm the batch is durable. For clients
    /// authenticated with [`oauth`](Self::oauth) or
    /// [`federated_auth`](Self::federated_auth), this also caps a proactive
    /// token refresh at half this value.
    pub fn server_lack_of_ack_timeout_ms(mut self, ms: u64) -> Self {
        self.core.set_server_lack_of_ack_timeout_ms(ms);
        self
    }

    /// Set the maximum request body size in bytes allowed per call.
    ///
    /// Calls exceeding this limit fail fast with
    /// [`ZerobusError::InvalidArgument`] before any network I/O.
    ///
    /// Defaults to slightly below the 10 MiB server limit.
    pub fn max_ingest_payload_bytes(mut self, bytes: usize) -> Self {
        self.core.set_max_ingest_payload_bytes(bytes);
        self
    }

    /// Validate the builder configuration without building the client.
    ///
    /// Checks that table name, authentication, and format have been set, in
    /// that order, like [`StreamBuilder::validate`](crate::StreamBuilder::validate).
    /// Performs no network I/O.
    pub fn validate(&self) -> ZerobusResult<()> {
        self.core.validate_target()?;
        if self.core.format.is_none() {
            return Err(ZerobusError::InvalidArgument(
                "record format is required: call .json()".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_sdk() -> ZerobusSdk {
        ZerobusSdk::new_with_config(
            "http://localhost:1234".to_string(),
            "http://localhost:5678".to_string(),
            "test-workspace".to_string(),
            Arc::new(crate::tls_config::SecureTlsConfig::new()),
            None,
            Arc::from(crate::DEFAULT_SDK_IDENTIFIER),
            true,
            crate::token_cache::DEFAULT_REFRESH_BUFFER,
            true,
        )
    }

    fn invalid_argument(result: ZerobusResult<()>) -> String {
        match result {
            Err(ZerobusError::InvalidArgument(msg)) => msg,
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    #[test]
    fn validate_accepts_complete_configuration() {
        let sdk = test_sdk();
        let builder = sdk.unary_client_builder().table("t").oauth("a", "b").json();
        assert!(builder.validate().is_ok());
    }

    #[test]
    fn validate_reports_table_then_auth_then_format() {
        let sdk = test_sdk();

        let msg = invalid_argument(sdk.unary_client_builder().validate());
        assert!(msg.contains(".table()"), "{msg}");

        let msg = invalid_argument(sdk.unary_client_builder().table("t").json().validate());
        assert!(msg.contains(".oauth()"), "{msg}");

        let msg = invalid_argument(
            sdk.unary_client_builder()
                .table("t")
                .oauth("a", "b")
                .validate(),
        );
        assert_eq!(msg, "record format is required: call .json()");
    }

    #[test]
    fn debug_shows_kinds_without_secrets() {
        let sdk = test_sdk();
        let builder = sdk
            .unary_client_builder()
            .table("t")
            .oauth("id", "super-secret")
            .json();
        let debug = format!("{builder:?}");
        assert!(debug.contains("OAuth") && debug.contains("Json"), "{debug}");
        assert!(!debug.contains("super-secret"), "{debug}");
    }
}
