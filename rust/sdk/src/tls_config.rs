//! TLS configuration for Zerobus connections.
//!
//! This module provides a strategy pattern for TLS configuration,
//! allowing different TLS setups (secure, custom CA, or no TLS for testing).

use std::fmt;

use crate::errors::ZerobusError;
use crate::ZerobusResult;
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity};

/// Trait for TLS configuration strategies.
///
/// Implementations return the [`TlsSettings`] for a gRPC channel. The SDK calls
/// [`TlsConfig::tls_settings`] every time it opens a channel, including on
/// reconnects, so an implementation can rotate certificates between connections.
/// The SDK supports these configurations:
/// - `SecureTlsConfig`: Production TLS with system CA certificates (default)
/// - `NoTlsConfig`: No TLS, for testing with local `http://` endpoints (requires `testing` feature)
/// - `TlsSettings`: Custom CA certificates, a client identity for mutual TLS, or a domain override
/// - Custom implementations that build `TlsSettings` per connection
///
/// # Examples
///
/// ```rust
/// use databricks_zerobus_ingest_sdk::{SecureTlsConfig, TlsConfig, TlsSettings};
/// use std::sync::Arc;
///
/// // Secure TLS with system CAs (default)
/// let tls: Arc<dyn TlsConfig> = Arc::new(SecureTlsConfig::new());
///
/// // Trust an additional CA certificate
/// let tls: Arc<dyn TlsConfig> = Arc::new(TlsSettings::new().with_ca_certificate_pem(
///     "-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----\n",
/// ));
/// ```
#[allow(clippy::result_large_err)]
pub trait TlsConfig: Send + Sync {
    /// Returns the TLS settings for a new gRPC channel.
    ///
    /// # Errors
    ///
    /// Returns an error if the settings can't be produced, for example when
    /// certificates fail to load.
    fn tls_settings(&self) -> ZerobusResult<TlsSettings>;
}

/// TLS settings for a gRPC channel.
///
/// TLS applies to `https://` endpoints. Plaintext settings leave the endpoint unchanged.
///
/// # Examples
///
/// ```rust
/// use databricks_zerobus_ingest_sdk::TlsSettings;
///
/// let ca_pem = std::fs::read("ca.pem").unwrap_or_default();
/// let settings = TlsSettings::new()
///     .with_native_roots(false)
///     .with_ca_certificate_pem(ca_pem)
///     .with_domain_name("zerobus.internal");
/// ```
#[derive(Clone)]
pub struct TlsSettings {
    enabled: bool,
    native_roots: bool,
    ca_certificates: Vec<Vec<u8>>,
    client_identity: Option<(Vec<u8>, Vec<u8>)>,
    domain_name: Option<String>,
}

impl TlsSettings {
    /// TLS that trusts the operating system's CA certificates.
    pub fn new() -> Self {
        Self {
            enabled: true,
            native_roots: true,
            ca_certificates: Vec::new(),
            client_identity: None,
            domain_name: None,
        }
    }

    /// A plaintext connection, for testing with local `http://` endpoints.
    #[cfg(any(test, feature = "testing"))]
    pub fn plaintext() -> Self {
        Self {
            enabled: false,
            ..Self::new()
        }
    }

    /// Sets whether the operating system's CA certificates are trusted. Default: `true`.
    pub fn with_native_roots(mut self, enabled: bool) -> Self {
        self.native_roots = enabled;
        self
    }

    /// Adds a trusted CA certificate in PEM format.
    pub fn with_ca_certificate_pem(mut self, pem: impl Into<Vec<u8>>) -> Self {
        self.ca_certificates.push(pem.into());
        self
    }

    /// Sets the client certificate and private key, both in PEM format, for mutual TLS.
    pub fn with_client_identity_pem(
        mut self,
        cert_pem: impl Into<Vec<u8>>,
        key_pem: impl Into<Vec<u8>>,
    ) -> Self {
        self.client_identity = Some((cert_pem.into(), key_pem.into()));
        self
    }

    /// Overrides the server name verified against the server's certificate.
    pub fn with_domain_name(mut self, domain_name: impl Into<String>) -> Self {
        self.domain_name = Some(domain_name.into());
        self
    }

    #[allow(clippy::result_large_err)]
    pub(crate) fn apply(self, endpoint: Endpoint) -> ZerobusResult<Endpoint> {
        if !self.enabled {
            return Ok(endpoint);
        }
        let mut tls = ClientTlsConfig::new();
        if self.native_roots {
            tls = tls.with_native_roots();
        }
        for pem in self.ca_certificates {
            tls = tls.ca_certificate(Certificate::from_pem(pem));
        }
        if let Some((cert_pem, key_pem)) = self.client_identity {
            tls = tls.identity(Identity::from_pem(cert_pem, key_pem));
        }
        if let Some(domain_name) = self.domain_name {
            tls = tls.domain_name(domain_name);
        }
        endpoint
            .tls_config(tls)
            .map_err(|_| ZerobusError::FailedToEstablishTlsConnectionError)
    }
}

impl Default for TlsSettings {
    fn default() -> Self {
        Self::new()
    }
}

// Never prints certificate or key material.
impl fmt::Debug for TlsSettings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TlsSettings")
            .field("enabled", &self.enabled)
            .field("native_roots", &self.native_roots)
            .field("ca_certificates", &self.ca_certificates.len())
            .field("client_identity", &self.client_identity.is_some())
            .field("domain_name", &self.domain_name)
            .finish()
    }
}

impl TlsConfig for TlsSettings {
    fn tls_settings(&self) -> ZerobusResult<TlsSettings> {
        Ok(self.clone())
    }
}

/// Secure TLS configuration using system CA certificates.
///
/// This is the default and recommended configuration for production use.
/// It enables TLS encryption using the operating system's trusted CA certificates.
///
/// # Examples
///
/// ```rust
/// use databricks_zerobus_ingest_sdk::SecureTlsConfig;
///
/// let tls = SecureTlsConfig::new();
/// ```
#[derive(Clone, Debug, Default)]
pub struct SecureTlsConfig;

impl SecureTlsConfig {
    /// Create a new secure TLS configuration.
    pub fn new() -> Self {
        Self
    }
}

impl TlsConfig for SecureTlsConfig {
    fn tls_settings(&self) -> ZerobusResult<TlsSettings> {
        Ok(TlsSettings::new())
    }
}

/// No-op TLS configuration for testing with plaintext `http://` endpoints.
///
/// This passes the endpoint through without any TLS configuration.
/// Only available when the `testing` feature is enabled.
///
/// # Examples
///
/// ```rust
/// use databricks_zerobus_ingest_sdk::{NoTlsConfig, TlsConfig};
/// use std::sync::Arc;
///
/// let tls: Arc<dyn TlsConfig> = Arc::new(NoTlsConfig);
/// ```
#[cfg(feature = "testing")]
#[derive(Clone, Debug, Default)]
pub struct NoTlsConfig;

#[cfg(feature = "testing")]
impl TlsConfig for NoTlsConfig {
    fn tls_settings(&self) -> ZerobusResult<TlsSettings> {
        Ok(TlsSettings::plaintext())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_omits_certificate_and_key_material() {
        let settings = TlsSettings::new()
            .with_ca_certificate_pem("CA-PEM-BYTES")
            .with_client_identity_pem("CERT-PEM-BYTES", "SECRET-KEY-PEM");
        let debug = format!("{settings:?}");
        assert!(!debug.contains("SECRET-KEY-PEM"));
        assert!(!debug.contains("CA-PEM-BYTES"));
        assert!(debug.contains("client_identity: true"));
    }

    #[test]
    fn plaintext_leaves_endpoint_unchanged() {
        let applied = TlsSettings::plaintext()
            .apply(Endpoint::from_static("http://127.0.0.1:50051"))
            .unwrap();
        assert_eq!(applied.uri(), "http://127.0.0.1:50051");
    }

    #[test]
    fn invalid_client_identity_is_rejected() {
        let result = TlsSettings::new()
            .with_client_identity_pem("not a certificate", "not a key")
            .apply(Endpoint::from_static("https://localhost:443"));
        assert!(matches!(
            result,
            Err(ZerobusError::FailedToEstablishTlsConnectionError)
        ));
    }
}
