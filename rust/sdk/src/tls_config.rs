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
use rustls_pki_types::{pem::PemObject, CertificateDer};
use std::borrow::Cow;
use std::collections::hash_map::DefaultHasher;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::io::Read;
use std::path::{Path, PathBuf};
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
/// new channel, and before it reuses a gRPC channel for a new stream or a
/// recovery attempt. When the settings differ from the ones the channel was
/// opened with, the SDK opens a new channel. It keeps the gRPC channel it
/// replaced until a call on the new one reaches the server, and goes back to it
/// if the new one fails first. An implementation can return different settings
/// on each call, for example to rotate certificates.
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
    /// Returns the TLS settings to open a channel with, or to compare with the
    /// settings of a channel the SDK is about to reuse.
    ///
    /// The SDK calls this before every new stream and every recovery attempt, so
    /// it should return quickly and not block, for example on a network call.
    /// Cache settings that are slow to produce.
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
/// # Certificate files
///
/// The `*_file` methods take a path instead of PEM data. The SDK reads the files
/// each time it opens a channel, and before it reuses a gRPC channel. When the
/// files changed, it opens a new channel, so replaced files take effect without
/// a restart:
///
/// - A gRPC stream gets its channel when it is created and before each recovery
///   attempt. This holds for a dedicated channel per stream (the
///   [`connection_per_stream`](crate::ZerobusSdkBuilder::connection_per_stream)
///   default) and for a shared channel. A stream that doesn't reconnect keeps its
///   connection, because certificates are only checked when a connection is set up.
/// - An Arrow Flight stream opens a channel on every connection and reconnection.
///
/// When the SDK can't read or use the files before it reuses a gRPC channel, it
/// keeps the channel it has and logs a warning.
///
/// It also keeps the gRPC channel it replaced until a call on the new channel
/// reaches the server. If the new channel fails first, for example because the
/// server rejects the new client certificate or the new CA doesn't trust the
/// server, the SDK goes back to the replaced channel and logs a warning. It tries
/// the new channel again when the files change again or when the replaced
/// channel fails too. The first channel of a stream and every Arrow Flight
/// connection have no channel to go back to, so they use the files as they are.
///
/// The SDK reads the files on a blocking thread, so a slow volume doesn't stall
/// the async runtime.
///
/// Replace the certificate and the key together: write both into a new
/// directory, then switch a symbolic link to that directory, as Kubernetes
/// secret volumes do. The SDK reads the certificate again after the key, and
/// reads the pair again if the certificate changed in between. Use absolute
/// paths, because the SDK resolves a relative path when it opens the channel.
///
/// A path that is missing, unreadable, not a regular file, or larger than 1 MiB
/// gives [`ZerobusError::InvalidArgument`], which is not retryable. Content read
/// from a file that the SDK can't use, such as a file with no certificate or a
/// certificate that doesn't match the key, gives a retryable
/// [`ZerobusError::ChannelCreationError`], because a rotation may be in progress.
/// The same content passed as PEM data gives `InvalidArgument`. When the
/// settings mix files and PEM data and the SDK can't tell which input it can't
/// use, the error is retryable.
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
///
/// // Trust only a private CA and present a client certificate, all read from
/// // files that the SDK reads again for each new channel.
/// let settings = TlsSettings::new()
///     .without_native_roots()
///     .with_ca_certificate_file("/etc/zerobus/ca.pem")
///     .with_client_identity_files("/etc/zerobus/tls.crt", "/etc/zerobus/tls.key");
/// ```
#[derive(Clone)]
pub struct TlsSettings {
    mode: TlsMode,
    native_roots: bool,
    ca_certificates: Vec<PemSource>,
    client_identity: Option<ClientIdentity>,
    domain_name: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum TlsMode {
    Tls,
    #[cfg(feature = "testing")]
    Plaintext,
}

/// PEM data, held in memory or read from a file when the settings are applied.
#[derive(Clone, PartialEq, Eq)]
enum PemSource {
    Bytes(Vec<u8>),
    File(PathBuf),
}

// A private key can be a `PemSource`, so `Debug` never prints the bytes.
impl fmt::Debug for PemSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bytes(pem) => write!(f, "Bytes(<{} bytes>)", pem.len()),
            Self::File(path) => f.debug_tuple("File").field(path).finish(),
        }
    }
}

impl PemSource {
    fn is_file(&self) -> bool {
        matches!(self, Self::File(_))
    }

    fn read(&self) -> ZerobusResult<Cow<'_, [u8]>> {
        match self {
            Self::Bytes(pem) => Ok(Cow::Borrowed(pem)),
            Self::File(path) => read_pem_file(path).map(Cow::Owned),
        }
    }
}

/// The largest PEM file the SDK reads. Certificate chains and keys are a few KB.
const MAX_PEM_FILE_BYTES: u64 = 1024 * 1024;

fn read_pem_file(path: &Path) -> ZerobusResult<Vec<u8>> {
    let unreadable = |reason: &dyn fmt::Display| {
        ZerobusError::InvalidArgument(format!("cannot read TLS file {}: {reason}", path.display()))
    };

    // Check the type before opening the path: opening a FIFO blocks until a
    // writer appears, and a device such as `/dev/zero` never ends.
    let metadata = std::fs::metadata(path).map_err(|err| unreadable(&err))?;
    if !metadata.is_file() {
        return Err(unreadable(&"not a regular file"));
    }

    let mut pem = Vec::new();
    std::fs::File::open(path)
        .and_then(|file| file.take(MAX_PEM_FILE_BYTES + 1).read_to_end(&mut pem))
        .map_err(|err| unreadable(&err))?;
    if pem.len() as u64 > MAX_PEM_FILE_BYTES {
        return Err(unreadable(&"larger than 1 MiB"));
    }
    Ok(pem)
}

/// PEM-encoded client certificate chain and private key for mutual TLS.
#[derive(Clone)]
struct ClientIdentity {
    cert_pem: PemSource,
    key_pem: PemSource,
}

/// A PEM certificate chain and its private key, in that order.
type PemPair<'a> = (Cow<'a, [u8]>, Cow<'a, [u8]>);

impl ClientIdentity {
    fn is_file(&self) -> bool {
        self.cert_pem.is_file() || self.key_pem.is_file()
    }

    fn read(&self) -> ZerobusResult<PemPair<'_>> {
        read_consistent_pair(|| self.cert_pem.read(), || self.key_pem.read())
    }
}

/// How many times the SDK reads a client identity before it uses the last pair.
const MAX_IDENTITY_READS: usize = 3;

/// Reads the certificate, the key, and the certificate again.
///
/// The two files are opened separately, so a rotation between the reads can pair
/// a new certificate with an old key. When the certificate changed in between,
/// the pair is read again. If it keeps changing, the last pair is returned and
/// the TLS library's key check reports a mismatch.
fn read_consistent_pair<'a>(
    mut read_cert: impl FnMut() -> ZerobusResult<Cow<'a, [u8]>>,
    mut read_key: impl FnMut() -> ZerobusResult<Cow<'a, [u8]>>,
) -> ZerobusResult<PemPair<'a>> {
    let mut cert = read_cert()?;
    let mut key = read_key()?;
    for _ in 1..MAX_IDENTITY_READS {
        let cert_again = read_cert()?;
        if cert_again == cert {
            return Ok((cert, key));
        }
        cert = cert_again;
        key = read_key()?;
    }
    Ok((cert, key))
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
    /// [`with_ca_certificate_pem`](Self::with_ca_certificate_pem) or
    /// [`with_ca_certificate_file`](Self::with_ca_certificate_file). Without a CA
    /// certificate that the SDK can read as PEM, the channel trusts no server
    /// and the SDK rejects the settings.
    pub fn without_native_roots(mut self) -> Self {
        self.native_roots = false;
        self
    }

    /// Trusts one more CA certificate, in PEM format.
    ///
    /// Call this method more than once to trust more than one CA certificate.
    pub fn with_ca_certificate_pem(mut self, pem: impl Into<Vec<u8>>) -> Self {
        self.ca_certificates.push(PemSource::Bytes(pem.into()));
        self
    }

    /// Trusts the CA certificates in a PEM file.
    ///
    /// The SDK reads the file each time it opens a channel. See
    /// [Certificate files](Self#certificate-files). Call this method more than
    /// once to trust certificates from more than one file.
    pub fn with_ca_certificate_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.ca_certificates.push(PemSource::File(path.into()));
        self
    }

    /// Sets the client certificate chain and private key for mutual TLS, both in PEM format.
    pub fn with_client_identity_pem(
        mut self,
        cert_pem: impl Into<Vec<u8>>,
        key_pem: impl Into<Vec<u8>>,
    ) -> Self {
        self.client_identity = Some(ClientIdentity {
            cert_pem: PemSource::Bytes(cert_pem.into()),
            key_pem: PemSource::Bytes(key_pem.into()),
        });
        self
    }

    /// Sets the client certificate chain and private key for mutual TLS from
    /// two PEM files.
    ///
    /// The SDK reads both files each time it opens a channel. See
    /// [Certificate files](Self#certificate-files).
    pub fn with_client_identity_files(
        mut self,
        cert_path: impl Into<PathBuf>,
        key_path: impl Into<PathBuf>,
    ) -> Self {
        self.client_identity = Some(ClientIdentity {
            cert_pem: PemSource::File(cert_path.into()),
            key_pem: PemSource::File(key_path.into()),
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
    #[cfg(test)]
    pub(crate) fn apply(&self, endpoint: Endpoint) -> ZerobusResult<Endpoint> {
        self.resolve()?.apply(endpoint)
    }

    /// Like [`resolve`](Self::resolve), but reads the files on a blocking thread,
    /// so a slow certificate volume doesn't stall the async runtime.
    pub(crate) async fn resolve_off_runtime(&self) -> ZerobusResult<ResolvedTls<'_>> {
        if !self.reads_files() {
            return self.resolve();
        }
        let settings = self.clone();
        tokio::task::spawn_blocking(move || settings.resolve().map(ResolvedTls::into_owned))
            .await
            .unwrap_or_else(|err| match err.try_into_panic() {
                Ok(panic) => std::panic::resume_unwind(panic),
                Err(err) => Err(ZerobusError::ChannelCreationError(format!(
                    "reading the TLS files did not finish: {err}"
                ))),
            })
    }

    /// Whether [`resolve`](Self::resolve) reads any file.
    fn reads_files(&self) -> bool {
        self.mode == TlsMode::Tls
            && (self.ca_certificates.iter().any(PemSource::is_file)
                || self
                    .client_identity
                    .as_ref()
                    .is_some_and(ClientIdentity::is_file))
    }

    /// Reads every file the settings name, once. The result can be applied to an
    /// endpoint and compared with the material an existing channel was opened with.
    pub(crate) fn resolve(&self) -> ZerobusResult<ResolvedTls<'_>> {
        let mut resolved = ResolvedTls {
            mode: self.mode,
            native_roots: self.native_roots,
            ca_certificates: Vec::new(),
            client_identity: None,
            domain_name: self.domain_name.as_deref().map(Cow::Borrowed),
            ca_from_files: self.ca_certificates.iter().any(PemSource::is_file),
            identity_from_files: self
                .client_identity
                .as_ref()
                .is_some_and(ClientIdentity::is_file),
            custom_material: !self.ca_certificates.is_empty() || self.client_identity.is_some(),
        };
        // A plaintext channel applies no TLS configuration, so it reads no files.
        if self.mode == TlsMode::Tls {
            resolved.ca_certificates = self
                .ca_certificates
                .iter()
                .map(PemSource::read)
                .collect::<ZerobusResult<_>>()?;
            resolved.client_identity = self
                .client_identity
                .as_ref()
                .map(ClientIdentity::read)
                .transpose()?;
        }
        Ok(resolved)
    }
}

/// [`TlsSettings`] with every file read, ready to apply to a gRPC endpoint.
pub(crate) struct ResolvedTls<'a> {
    mode: TlsMode,
    native_roots: bool,
    ca_certificates: Vec<Cow<'a, [u8]>>,
    client_identity: Option<PemPair<'a>>,
    domain_name: Option<Cow<'a, str>>,
    /// Whether any CA certificate came from a file.
    ca_from_files: bool,
    /// Whether the client identity came from files.
    identity_from_files: bool,
    /// Whether the caller supplied any CA certificate or a client identity.
    custom_material: bool,
}

/// Identifies the TLS material a channel was opened with.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TlsFingerprint(u64);

impl ResolvedTls<'_> {
    fn into_owned(self) -> ResolvedTls<'static> {
        let owned = |pem: Cow<'_, [u8]>| Cow::Owned(pem.into_owned());
        ResolvedTls {
            mode: self.mode,
            native_roots: self.native_roots,
            ca_certificates: self.ca_certificates.into_iter().map(owned).collect(),
            client_identity: self
                .client_identity
                .map(|(cert, key)| (owned(cert), owned(key))),
            domain_name: self.domain_name.map(|name| Cow::Owned(name.into_owned())),
            ca_from_files: self.ca_from_files,
            identity_from_files: self.identity_from_files,
            custom_material: self.custom_material,
        }
    }

    pub(crate) fn fingerprint(&self) -> TlsFingerprint {
        let mut hasher = DefaultHasher::new();
        self.mode.hash(&mut hasher);
        self.native_roots.hash(&mut hasher);
        self.ca_certificates.hash(&mut hasher);
        self.client_identity.hash(&mut hasher);
        self.domain_name.hash(&mut hasher);
        TlsFingerprint(hasher.finish())
    }

    pub(crate) fn apply(&self, endpoint: Endpoint) -> ZerobusResult<Endpoint> {
        match self.mode {
            TlsMode::Tls => {}
            #[cfg(feature = "testing")]
            TlsMode::Plaintext => return Ok(endpoint),
        }

        // Material passed as PEM data fails the same way on every attempt, so it is
        // reported as `InvalidArgument`, which recovery does not retry. Content read
        // from a file can change between attempts, for example while a rotation is
        // in progress, so it is reported as a retryable `ChannelCreationError`.
        let unusable = |from_files: bool, reason: String| {
            if from_files {
                ZerobusError::ChannelCreationError(reason)
            } else {
                ZerobusError::InvalidArgument(reason)
            }
        };

        // tonic skips PEM data that holds no certificate, for example a private
        // key or a DER file. Without this check, such a CA gives an empty root
        // store and the channel trusts no server.
        if !self.native_roots && !self.ca_certificates.iter().any(|pem| has_certificate(pem)) {
            return Err(unusable(
                self.ca_from_files,
                "TLS settings trust no CA certificates: add a PEM certificate with \
                 `with_ca_certificate_pem` or `with_ca_certificate_file`, or keep the \
                 native roots"
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
        if let Some((cert_pem, key_pem)) = &self.client_identity {
            tls_config = tls_config.identity(Identity::from_pem(cert_pem, key_pem));
        }
        if let Some(domain_name) = &self.domain_name {
            tls_config = tls_config.domain_name(domain_name.as_ref());
        }

        endpoint.tls_config(tls_config).map_err(|err| {
            if self.custom_material {
                // The `Display` of the tonic error only says "transport error".
                // The reason is in `source()`.
                let reason = std::error::Error::source(&err)
                    .map_or_else(|| err.to_string(), |source| source.to_string());
                // tonic doesn't say which input it rejected, so any file counts.
                unusable(
                    self.ca_from_files || self.identity_from_files,
                    format!("invalid TLS settings: {reason}"),
                )
            } else {
                ZerobusError::FailedToEstablishTlsConnectionError
            }
        })
    }
}

/// Reports whether the PEM data holds at least one well-formed certificate section.
fn has_certificate(pem: &[u8]) -> bool {
    CertificateDer::pem_slice_iter(pem).any(|cert| cert.is_ok())
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

    /// A self-signed CA certificate (CN=zerobus-test-ca). It holds no private key.
    const TEST_CA_PEM: &str = "\
-----BEGIN CERTIFICATE-----
MIIBizCCATGgAwIBAgIUQKpKxM/JogrsAiBjTBW9zZNn7aAwCgYIKoZIzj0EAwIw
GjEYMBYGA1UEAwwPemVyb2J1cy10ZXN0LWNhMCAXDTI2MTAyODExMzYxMFoYDzIx
MjYxMDA0MTEzNjEwWjAaMRgwFgYDVQQDDA96ZXJvYnVzLXRlc3QtY2EwWTATBgcq
hkjOPQIBBggqhkjOPQMBBwNCAASKmMpvY8kCrxbut0kDNV0tJckwqGi7meFlyKcp
GXVgp0QtxYsP6A8qw3cImCxu+PRYIQVEGexsQE3gQiL+eQMQo1MwUTAdBgNVHQ4E
FgQUGxeLAn9I200zjQ0xgH9K72mu440wHwYDVR0jBBgwFoAUGxeLAn9I200zjQ0x
gH9K72mu440wDwYDVR0TAQH/BAUwAwEB/zAKBggqhkjOPQQDAgNIADBFAiA/XPyA
2ppG9BVyn/QaRXweTlxsrMoD6v5+2a6337qDNQIhAJSkTaw0wxqIOYyzluxwa9hv
OXAXnIE7l91w/88HcoR0
-----END CERTIFICATE-----
";

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
        assert_eq!(
            returned.ca_certificates,
            vec![
                PemSource::Bytes(b"a".to_vec()),
                PemSource::Bytes(b"b".to_vec())
            ]
        );
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
            .with_ca_certificate_pem(TEST_CA_PEM)
            .with_domain_name("localhost");

        assert!(settings.apply(endpoint()).is_ok());
    }

    #[test]
    fn apply_with_native_roots_and_custom_ca_succeeds() {
        let settings = TlsSettings::new().with_ca_certificate_pem(TEST_CA_PEM);

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

    /// tonic skips PEM data that holds no certificate. A CA entry like that
    /// leaves an empty root store, so the SDK must reject it.
    #[test]
    fn apply_rejects_a_ca_that_holds_no_certificate_when_native_roots_are_off() {
        let not_certificates: [&[u8]; 4] = [
            b"not a certificate",
            b"",
            // A PEM section of another type, such as a public key file.
            b"-----BEGIN PUBLIC KEY-----\nAAAA\n-----END PUBLIC KEY-----\n",
            // Binary data, such as a DER file.
            &[0x30, 0x82, 0x01, 0x0a, 0x02, 0x82, 0x01, 0x01, 0x00],
        ];

        for pem in not_certificates {
            let err = TlsSettings::new()
                .without_native_roots()
                .with_ca_certificate_pem(pem)
                .apply(endpoint())
                .unwrap_err();

            assert!(
                matches!(err, ZerobusError::InvalidArgument(_)),
                "{pem:?}: {err:?}"
            );
            assert!(!err.is_retryable(), "{pem:?}");
        }
    }

    #[test]
    fn apply_accepts_one_certificate_among_unusable_ca_entries() {
        let settings = TlsSettings::new()
            .without_native_roots()
            .with_ca_certificate_pem("not a certificate")
            .with_ca_certificate_pem(TEST_CA_PEM);

        assert!(settings.apply(endpoint()).is_ok());
    }

    /// A client identity passed as PEM data that tonic rejects fails the same way
    /// on every attempt, so it must not be retryable.
    #[test]
    fn apply_reports_an_unusable_client_identity_as_not_retryable() {
        let err = TlsSettings::new()
            .with_client_identity_pem("not a certificate", "not a key")
            .apply(endpoint())
            .unwrap_err();

        assert!(matches!(err, ZerobusError::InvalidArgument(_)), "{err:?}");
        assert!(!err.is_retryable());
        // The message carries the reason from tonic's `source()`.
        assert!(err.to_string().contains("private key"), "{err}");
    }

    #[test]
    fn apply_reads_a_ca_certificate_file() {
        let dir = tempfile::tempdir().unwrap();
        let ca_path = dir.path().join("ca.pem");
        std::fs::write(&ca_path, TEST_CA_PEM).unwrap();

        let settings = TlsSettings::new()
            .without_native_roots()
            .with_ca_certificate_file(&ca_path);

        assert!(settings.apply(endpoint()).is_ok());
    }

    #[test]
    fn only_settings_that_name_a_file_read_files() {
        assert!(!TlsSettings::new().reads_files());
        assert!(!TlsSettings::new()
            .with_ca_certificate_pem(TEST_CA_PEM)
            .with_client_identity_pem(CERT_MARKER, KEY_MARKER)
            .reads_files());
        assert!(TlsSettings::new()
            .with_ca_certificate_file("/etc/zerobus/ca.pem")
            .reads_files());
        assert!(TlsSettings::new()
            .with_client_identity_files("/etc/zerobus/tls.crt", "/etc/zerobus/tls.key")
            .reads_files());
    }

    /// Reading the files on a blocking thread must give the same material as
    /// reading them in place.
    #[tokio::test(flavor = "current_thread")]
    async fn resolve_off_runtime_reads_the_same_material_as_resolve() {
        let dir = tempfile::tempdir().unwrap();
        let ca_path = dir.path().join("ca.pem");
        std::fs::write(&ca_path, TEST_CA_PEM).unwrap();
        let settings = TlsSettings::new()
            .without_native_roots()
            .with_ca_certificate_file(&ca_path)
            .with_ca_certificate_pem(TEST_CA_PEM)
            .with_domain_name("localhost");

        let off_runtime = settings.resolve_off_runtime().await.unwrap();

        assert_eq!(
            off_runtime.fingerprint(),
            settings.resolve().unwrap().fingerprint()
        );
        assert!(off_runtime.apply(endpoint()).is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn resolve_off_runtime_reports_an_unreadable_file_with_its_path() {
        let settings = TlsSettings::new().with_ca_certificate_file("/nonexistent/ca.pem");

        let Err(err) = settings.resolve_off_runtime().await else {
            panic!("a missing CA file must fail");
        };

        assert!(matches!(err, ZerobusError::InvalidArgument(_)), "{err:?}");
        assert!(err.to_string().contains("/nonexistent/ca.pem"), "{err}");
    }

    /// The SDK applies the same settings to every new channel, so each call must
    /// read the files again to pick up a replaced certificate.
    #[test]
    fn apply_reads_a_replaced_ca_file_again() {
        let dir = tempfile::tempdir().unwrap();
        let ca_path = dir.path().join("ca.pem");
        std::fs::write(&ca_path, "not a certificate").unwrap();
        let settings = TlsSettings::new()
            .without_native_roots()
            .with_ca_certificate_file(&ca_path);

        let err = settings.apply(endpoint()).unwrap_err();
        assert!(
            matches!(err, ZerobusError::ChannelCreationError(_)),
            "{err:?}"
        );
        assert!(err.is_retryable());

        std::fs::write(&ca_path, TEST_CA_PEM).unwrap();

        assert!(settings.apply(endpoint()).is_ok());
    }

    /// Content read from a file may be mid-rotation, so a CA or a client identity
    /// from files that the SDK can't use must be retryable.
    #[test]
    fn apply_reports_unusable_content_from_files_as_retryable() {
        let dir = tempfile::tempdir().unwrap();
        let garbage = dir.path().join("garbage.pem");
        std::fs::write(&garbage, "not a certificate").unwrap();

        let cases = [
            TlsSettings::new()
                .without_native_roots()
                .with_ca_certificate_file(&garbage),
            TlsSettings::new().with_client_identity_files(&garbage, &garbage),
        ];

        for settings in cases {
            let err = settings.apply(endpoint()).unwrap_err();

            assert!(
                matches!(err, ZerobusError::ChannelCreationError(_)),
                "{err:?}"
            );
            assert!(err.is_retryable(), "{err:?}");
        }
    }

    /// Identity files can't add a CA, so they don't make a missing one retryable.
    #[test]
    fn apply_reports_no_usable_ca_as_not_retryable_when_only_the_identity_is_from_files() {
        let dir = tempfile::tempdir().unwrap();
        let garbage = dir.path().join("garbage.pem");
        std::fs::write(&garbage, "not a certificate").unwrap();

        let err = TlsSettings::new()
            .without_native_roots()
            .with_ca_certificate_pem("not a certificate")
            .with_client_identity_files(&garbage, &garbage)
            .apply(endpoint())
            .unwrap_err();

        assert!(matches!(err, ZerobusError::InvalidArgument(_)), "{err:?}");
        assert!(!err.is_retryable());
    }

    #[test]
    fn apply_reports_an_unreadable_file_as_not_retryable_with_its_path() {
        let dir = tempfile::tempdir().unwrap();
        let cert_path = dir.path().join("tls.crt");
        std::fs::write(&cert_path, TEST_CA_PEM).unwrap();
        let missing = dir.path().join("missing.pem");
        let missing_key = dir.path().join("missing.key");
        let large = dir.path().join("large.pem");
        std::fs::write(&large, vec![b'a'; MAX_PEM_FILE_BYTES as usize + 1]).unwrap();

        let cases = [
            (
                TlsSettings::new().with_ca_certificate_file(&missing),
                missing.clone(),
                "cannot read TLS file",
            ),
            (
                TlsSettings::new().with_client_identity_files(&missing, &missing),
                missing.clone(),
                "cannot read TLS file",
            ),
            // The certificate is readable, so the error comes from the key.
            (
                TlsSettings::new().with_client_identity_files(&cert_path, &missing_key),
                missing_key.clone(),
                "cannot read TLS file",
            ),
            (
                TlsSettings::new().with_ca_certificate_file(dir.path()),
                dir.path().to_path_buf(),
                "not a regular file",
            ),
            (
                TlsSettings::new().with_ca_certificate_file(&large),
                large.clone(),
                "larger than 1 MiB",
            ),
        ];

        for (settings, path, reason) in cases {
            let err = settings.apply(endpoint()).unwrap_err();

            assert!(matches!(err, ZerobusError::InvalidArgument(_)), "{err:?}");
            assert!(!err.is_retryable());
            let message = err.to_string();
            assert!(message.contains(&path.display().to_string()), "{message}");
            assert!(message.contains(reason), "{message}");
        }
    }

    #[test]
    fn read_consistent_pair_reads_a_stable_pair_once() {
        let (mut cert_reads, mut key_reads) = (0, 0);

        let (cert, key) = read_consistent_pair(
            || {
                cert_reads += 1;
                Ok(Cow::Borrowed(b"cert".as_slice()))
            },
            || {
                key_reads += 1;
                Ok(Cow::Borrowed(b"key".as_slice()))
            },
        )
        .unwrap();

        assert_eq!((&*cert, &*key), (b"cert".as_slice(), b"key".as_slice()));
        assert_eq!((cert_reads, key_reads), (2, 1));
    }

    /// A rotation between the certificate read and the key read pairs the old
    /// certificate with the new key. The certificate differs on the next read, so
    /// the pair is read again.
    #[test]
    fn read_consistent_pair_reads_again_when_the_certificate_changes() {
        let certs: [&[u8]; 3] = [b"cert-1", b"cert-2", b"cert-2"];
        let keys: [&[u8]; 2] = [b"key-2", b"key-2"];
        let (mut certs, mut keys) = (certs.into_iter(), keys.into_iter());

        let (cert, key) = read_consistent_pair(
            || Ok(Cow::Borrowed(certs.next().unwrap())),
            || Ok(Cow::Borrowed(keys.next().unwrap())),
        )
        .unwrap();

        assert_eq!((&*cert, &*key), (b"cert-2".as_slice(), b"key-2".as_slice()));
    }

    #[test]
    fn read_consistent_pair_returns_the_last_pair_when_the_files_keep_changing() {
        let certs: [&[u8]; 3] = [b"cert-1", b"cert-2", b"cert-3"];
        let keys: [&[u8]; 3] = [b"key-1", b"key-2", b"key-3"];
        let (mut certs, mut keys) = (certs.into_iter(), keys.into_iter());

        let (cert, key) = read_consistent_pair(
            || Ok(Cow::Borrowed(certs.next().unwrap())),
            || Ok(Cow::Borrowed(keys.next().unwrap())),
        )
        .unwrap();

        assert_eq!((&*cert, &*key), (b"cert-3".as_slice(), b"key-3".as_slice()));
    }

    #[test]
    fn pem_source_debug_does_not_print_bytes() {
        let debug = format!("{:?}", PemSource::Bytes(KEY_MARKER.into()));

        assert!(!debug.contains(KEY_MARKER), "key leaked: {debug}");
    }

    #[cfg(feature = "testing")]
    #[test]
    fn plaintext_settings_do_not_read_files() {
        let settings = TlsSettings::plaintext()
            .with_ca_certificate_file("/nonexistent/ca.pem")
            .with_client_identity_files("/nonexistent/tls.crt", "/nonexistent/tls.key");

        assert!(!settings.reads_files());
        assert!(settings
            .apply(Endpoint::from_static("http://localhost:50051"))
            .is_ok());
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
