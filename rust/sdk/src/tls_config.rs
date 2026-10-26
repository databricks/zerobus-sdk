//! TLS configuration for Zerobus connections.
//!
//! This module provides a strategy pattern for TLS configuration,
//! allowing different TLS setups (secure, custom CA, or no TLS for testing).
//!
//! A [`TlsConfig`] describes the TLS setup of a channel with a [`TlsSettings`]
//! value. The SDK applies the settings to the gRPC endpoint itself, so the
//! public API does not name any `tonic` type.

use crate::errors::ZerobusError;
use crate::ZerobusResult;
use std::fmt;
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity};

/// Trait for TLS configuration strategies.
///
/// Implementations describe the TLS settings of the gRPC channel. This allows
/// the SDK to support different TLS configurations:
/// - `SecureTlsConfig`: Production TLS with system CA certificates (default)
/// - `NoTlsConfig`: No TLS, for testing with local `http://` endpoints (requires `testing` feature)
/// - Custom implementations for special certificate requirements
///
/// The SDK calls [`tls_settings`](TlsConfig::tls_settings) every time it opens a
/// new channel. An implementation can return different settings on each call,
/// for example to rotate certificates.
///
/// # Examples
///
/// ```rust
/// use databricks_zerobus_ingest_sdk::{SecureTlsConfig, TlsConfig};
/// use std::sync::Arc;
///
/// // Secure TLS with system CAs (default)
/// let tls: Arc<dyn TlsConfig> = Arc::new(SecureTlsConfig::new());
/// ```
///
/// A custom implementation that trusts a private CA:
///
/// ```rust
/// use databricks_zerobus_ingest_sdk::{TlsConfig, TlsSettings, ZerobusResult};
///
/// struct PrivateCaTlsConfig {
///     ca_pem: Vec<u8>,
/// }
///
/// impl TlsConfig for PrivateCaTlsConfig {
///     fn tls_settings(&self) -> ZerobusResult<TlsSettings> {
///         Ok(TlsSettings::new()
///             .without_native_roots()
///             .with_ca_certificate_pem(self.ca_pem.clone()))
///     }
/// }
/// ```
pub trait TlsConfig: Send + Sync {
    /// Returns the TLS settings for a new gRPC channel.
    ///
    /// # Errors
    ///
    /// Returns an error if the implementation cannot produce the settings,
    /// for example because it cannot read a certificate file.
    fn tls_settings(&self) -> ZerobusResult<TlsSettings>;
}

/// Describes the TLS setup of a gRPC channel.
///
/// A new value uses the operating system's trusted CA certificates. Add more
/// trusted CA certificates, a client identity for mutual TLS, or a domain-name
/// override with the `with_*` methods.
///
/// `TlsSettings` implements [`TlsConfig`], so a value can be passed directly to
/// [`ZerobusSdkBuilder::tls_config`](crate::ZerobusSdkBuilder::tls_config) inside an `Arc`.
///
/// # Security
///
/// The `Debug` output shows the number of CA certificates and whether a client
/// identity is set. It never prints certificate or key material.
///
/// # Examples
///
/// ```rust
/// use databricks_zerobus_ingest_sdk::TlsSettings;
///
/// # let (ca_pem, cert_pem, key_pem) = (Vec::<u8>::new(), Vec::<u8>::new(), Vec::<u8>::new());
/// // Trust the system CAs and one extra CA, and present a client certificate.
/// let settings = TlsSettings::new()
///     .with_ca_certificate_pem(ca_pem)
///     .with_client_identity_pem(cert_pem, key_pem);
/// ```
#[derive(Clone)]
pub struct TlsSettings {
    mode: TlsMode,
    native_roots: bool,
    ca_certificates: Vec<Vec<u8>>,
    client_identity: Option<ClientIdentity>,
    domain_name: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TlsMode {
    Tls,
    #[cfg(feature = "testing")]
    Plaintext,
}

/// PEM-encoded client certificate chain and private key for mutual TLS.
#[derive(Clone)]
struct ClientIdentity {
    cert_pem: Vec<u8>,
    key_pem: Vec<u8>,
}

impl Default for TlsSettings {
    fn default() -> Self {
        Self::new()
    }
}

impl TlsSettings {
    /// Creates settings that use TLS and trust the operating system's CA certificates.
    ///
    /// The native certificate store works on Windows, macOS, and Linux.
    pub fn new() -> Self {
        Self {
            mode: TlsMode::Tls,
            native_roots: true,
            ca_certificates: Vec::new(),
            client_identity: None,
            domain_name: None,
        }
    }

    /// Creates settings for a plaintext `http://` endpoint.
    ///
    /// The SDK applies no TLS configuration to the channel. The other settings
    /// have no effect on a plaintext channel.
    /// Only available when the `testing` feature is enabled.
    #[cfg(feature = "testing")]
    pub fn plaintext() -> Self {
        Self {
            mode: TlsMode::Plaintext,
            ..Self::new()
        }
    }

    /// Stops the channel from trusting the operating system's CA certificates.
    ///
    /// Add at least one CA certificate with
    /// [`with_ca_certificate_pem`](Self::with_ca_certificate_pem). Without one,
    /// the channel trusts no server and the SDK rejects the settings.
    pub fn without_native_roots(mut self) -> Self {
        self.native_roots = false;
        self
    }

    /// Trusts one more CA certificate, in PEM format.
    ///
    /// Call this method more than once to trust more than one CA certificate.
    pub fn with_ca_certificate_pem(mut self, pem: impl Into<Vec<u8>>) -> Self {
        self.ca_certificates.push(pem.into());
        self
    }

    /// Sets the client certificate chain and private key for mutual TLS, both in PEM format.
    pub fn with_client_identity_pem(
        mut self,
        cert_pem: impl Into<Vec<u8>>,
        key_pem: impl Into<Vec<u8>>,
    ) -> Self {
        self.client_identity = Some(ClientIdentity {
            cert_pem: cert_pem.into(),
            key_pem: key_pem.into(),
        });
        self
    }

    /// Sets the domain name the SDK checks the server certificate against.
    ///
    /// By default the SDK uses the host name of the endpoint.
    pub fn with_domain_name(mut self, domain_name: impl Into<String>) -> Self {
        self.domain_name = Some(domain_name.into());
        self
    }

    /// Applies the settings to a gRPC endpoint.
    pub(crate) fn apply(&self, endpoint: Endpoint) -> ZerobusResult<Endpoint> {
        match self.mode {
            TlsMode::Tls => {}
            #[cfg(feature = "testing")]
            TlsMode::Plaintext => return Ok(endpoint),
        }

        if !self.native_roots && self.ca_certificates.is_empty() {
            return Err(ZerobusError::InvalidArgument(
                "TLS settings trust no CA certificates: add one with \
                 `with_ca_certificate_pem` or keep the native roots"
                    .to_string(),
            ));
        }

        let mut tls_config = ClientTlsConfig::new();
        if self.native_roots {
            tls_config = tls_config.with_native_roots();
        }
        for pem in &self.ca_certificates {
            tls_config = tls_config.ca_certificate(Certificate::from_pem(pem));
        }
        if let Some(identity) = &self.client_identity {
            tls_config =
                tls_config.identity(Identity::from_pem(&identity.cert_pem, &identity.key_pem));
        }
        if let Some(domain_name) = &self.domain_name {
            tls_config = tls_config.domain_name(domain_name.clone());
        }

        endpoint
            .tls_config(tls_config)
            .map_err(|_| ZerobusError::FailedToEstablishTlsConnectionError)
    }
}

impl fmt::Debug for TlsSettings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TlsSettings")
            .field("mode", &self.mode)
            .field("native_roots", &self.native_roots)
            .field("ca_certificates", &self.ca_certificates.len())
            .field(
                "client_identity",
                &self.client_identity.as_ref().map(|_| "<redacted>"),
            )
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

    const KEY_MARKER: &str = "PRIVATE-KEY-MARKER";
    const CERT_MARKER: &str = "CLIENT-CERT-MARKER";
    const CA_MARKER: &str = "CA-CERT-MARKER";

    fn endpoint() -> Endpoint {
        Endpoint::from_static("https://localhost:443")
    }

    /// `Debug` must not leak certificate or key material.
    #[test]
    fn debug_does_not_print_key_or_certificate_material() {
        let settings = TlsSettings::new()
            .with_ca_certificate_pem(CA_MARKER)
            .with_client_identity_pem(CERT_MARKER, KEY_MARKER)
            .with_domain_name("zerobus.example.com");

        let debug = format!("{settings:?}");

        assert!(!debug.contains(KEY_MARKER), "key leaked: {debug}");
        assert!(!debug.contains(CERT_MARKER), "client cert leaked: {debug}");
        assert!(!debug.contains(CA_MARKER), "CA cert leaked: {debug}");
        assert!(debug.contains("<redacted>"), "identity not marked: {debug}");
        assert!(debug.contains("zerobus.example.com"));
    }

    #[test]
    fn debug_shows_that_no_identity_is_set() {
        let debug = format!("{:?}", TlsSettings::new());

        assert!(debug.contains("client_identity: None"), "{debug}");
        assert!(debug.contains("native_roots: true"), "{debug}");
    }

    #[test]
    fn secure_tls_config_returns_native_root_settings() {
        let settings = SecureTlsConfig::new().tls_settings().unwrap();

        assert_eq!(settings.mode, TlsMode::Tls);
        assert!(settings.native_roots);
        assert!(settings.ca_certificates.is_empty());
        assert!(settings.client_identity.is_none());
        assert!(settings.domain_name.is_none());
    }

    #[test]
    fn tls_settings_implements_tls_config_by_returning_itself() {
        let settings = TlsSettings::new()
            .without_native_roots()
            .with_ca_certificate_pem("a")
            .with_ca_certificate_pem("b")
            .with_domain_name("host.example.com");

        let returned = settings.tls_settings().unwrap();

        assert!(!returned.native_roots);
        assert_eq!(returned.ca_certificates, vec![b"a".to_vec(), b"b".to_vec()]);
        assert_eq!(returned.domain_name.as_deref(), Some("host.example.com"));
    }

    #[test]
    fn apply_with_native_roots_succeeds() {
        assert!(TlsSettings::new().apply(endpoint()).is_ok());
    }

    #[test]
    fn apply_with_custom_ca_and_domain_name_succeeds() {
        let settings = TlsSettings::new()
            .without_native_roots()
            .with_ca_certificate_pem("not-checked-until-connect")
            .with_domain_name("localhost");

        assert!(settings.apply(endpoint()).is_ok());
    }

    #[test]
    fn apply_rejects_settings_that_trust_no_ca() {
        let err = TlsSettings::new()
            .without_native_roots()
            .apply(endpoint())
            .unwrap_err();

        assert!(matches!(err, ZerobusError::InvalidArgument(_)), "{err:?}");
        assert!(!err.is_retryable());
    }

    #[cfg(feature = "testing")]
    #[test]
    fn plaintext_settings_leave_the_endpoint_unchanged() {
        let plaintext = Endpoint::from_static("http://localhost:50051");

        let applied = TlsSettings::plaintext().apply(plaintext.clone()).unwrap();

        assert_eq!(applied.uri(), plaintext.uri());
    }

    #[cfg(feature = "testing")]
    #[test]
    fn no_tls_config_returns_plaintext_settings() {
        assert_eq!(NoTlsConfig.tls_settings().unwrap().mode, TlsMode::Plaintext);
    }
}
