//! gRPC channels that follow changes to the TLS settings.
//!
//! A [`GrpcChannelSource`] hands out the channel for gRPC streams. Before it
//! reuses a channel, it reads the TLS settings again and opens a new channel if
//! they changed, so rotated certificates reach a shared channel and recovery
//! attempts, not only new dedicated channels. Until a call on the new channel
//! reaches the server, the source keeps the channel it replaced, and goes back
//! to it when the new channel fails first.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll};

use tonic::body::Body;
use tonic::codegen::{http, BoxFuture, Service};
use tonic::transport::{Channel, Endpoint};
use tracing::{info, warn};

use crate::databricks::zerobus::zerobus_client::ZerobusClient;
use crate::proxy::{self, ConnectorFactory};
use crate::tls_config::{ResolvedTls, TlsFingerprint};
use crate::{TlsConfig, ZerobusError, ZerobusResult};

/// The gRPC client that streams use.
pub(crate) type GrpcClient = ZerobusClient<TrackedChannel>;

pub(crate) struct GrpcChannelSource {
    endpoint: String,
    sdk_identifier: Arc<str>,
    tls_config: Arc<dyn TlsConfig>,
    connector_factory: Option<ConnectorFactory>,
    /// Counts failed calls across this source's channels, to order the failures.
    failures: Arc<AtomicU64>,
    channels: Mutex<Option<Channels>>,
}

/// The open channels of a source.
struct Channels {
    /// The channel opened with the newest TLS settings.
    current: OpenChannel,
    /// A channel that reached the server, kept until `current` does too.
    previous: Option<OpenChannel>,
    /// Whether the switch back to `previous` was logged.
    fallback_logged: bool,
}

#[derive(Clone)]
struct OpenChannel {
    client: GrpcClient,
    tls: TlsFingerprint,
    health: Arc<ChannelHealth>,
}

/// What the calls on one channel showed.
#[derive(Debug)]
struct ChannelHealth {
    failures: Arc<AtomicU64>,
    /// Whether any call got a response from the server.
    reached_server: AtomicBool,
    /// The number of the latest finished call in the source's count of failed
    /// calls, if it failed without a response. 0 if it got one, or before any
    /// call finished.
    last_failure: AtomicU64,
}

impl ChannelHealth {
    fn new(failures: Arc<AtomicU64>) -> Self {
        Self {
            failures,
            reached_server: AtomicBool::new(false),
            last_failure: AtomicU64::new(0),
        }
    }

    fn record(&self, reached_server: bool) {
        if reached_server {
            self.reached_server.store(true, Ordering::Relaxed);
            self.last_failure.store(0, Ordering::Relaxed);
        } else {
            let failure = self.failures.fetch_add(1, Ordering::Relaxed) + 1;
            self.last_failure.store(failure, Ordering::Relaxed);
        }
    }

    fn reached_server(&self) -> bool {
        self.reached_server.load(Ordering::Relaxed)
    }

    fn last_failure(&self) -> u64 {
        self.last_failure.load(Ordering::Relaxed)
    }
}

/// A channel that records whether each call got a response from the server.
///
/// A response with an error status still counts: the server answered, so the
/// TLS handshake worked. Under TLS 1.3 the server checks the client certificate
/// after the client considers the handshake done, so connecting alone doesn't
/// show that the server accepts the certificate. The first call does.
#[derive(Clone, Debug)]
pub(crate) struct TrackedChannel {
    channel: Channel,
    health: Arc<ChannelHealth>,
}

impl Service<http::Request<Body>> for TrackedChannel {
    type Response = http::Response<Body>;
    type Error = tonic::transport::Error;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.channel.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        let response = self.channel.call(request);
        let health = Arc::clone(&self.health);
        Box::pin(async move {
            let response = response.await;
            health.record(response.is_ok());
            response
        })
    }
}

impl Channels {
    /// Returns the current channel, unless it failed more recently than the
    /// previous one. While both fail, the two take turns.
    fn choose(&mut self) -> OpenChannel {
        if let Some(previous) = &self.previous {
            if self.current.health.reached_server() {
                info!("The gRPC channel with the new TLS settings reached the server; dropping the previous channel");
                self.previous = None;
            } else if self.current.health.last_failure() > previous.health.last_failure() {
                if !self.fallback_logged {
                    warn!("The gRPC channel with the new TLS settings failed before reaching the server; using the previous channel until the TLS settings change or the previous channel fails too");
                    self.fallback_logged = true;
                }
                return previous.clone();
            }
        }
        self.current.clone()
    }

    /// Returns a channel for the TLS material `tls`, if one is open.
    fn reuse(&mut self, tls: TlsFingerprint) -> Option<OpenChannel> {
        if self.current.tls != tls {
            if self.previous.as_ref()?.tls != tls {
                return None;
            }
            // The settings went back to the material of the channel that reached the server.
            self.current = self.previous.take()?;
        }
        Some(self.choose())
    }

    /// Makes `opened` the current channel, unless another call already opened a
    /// channel with the same material.
    fn replace(&mut self, opened: OpenChannel) -> OpenChannel {
        if self.current.tls != opened.tls {
            info!("TLS settings changed; opened a new gRPC channel");
            let replaced = std::mem::replace(&mut self.current, opened);
            // A channel that never reached the server is no better than the new one.
            if replaced.health.reached_server() {
                self.previous = Some(replaced);
            }
            self.fallback_logged = false;
        }
        self.choose()
    }
}

impl GrpcChannelSource {
    pub(crate) fn new(
        endpoint: String,
        sdk_identifier: Arc<str>,
        tls_config: Arc<dyn TlsConfig>,
        connector_factory: Option<ConnectorFactory>,
    ) -> Self {
        Self {
            endpoint,
            sdk_identifier,
            tls_config,
            connector_factory,
            failures: Arc::new(AtomicU64::new(0)),
            channels: Mutex::new(None),
        }
    }

    /// Returns the channel for the next stream or recovery attempt.
    ///
    /// The first call opens the channel and returns any error. Later calls read
    /// the TLS settings again and open a new channel if they changed. If the
    /// settings can't be read or applied at that point, the current channel is
    /// kept, so a broken rotation doesn't take down a channel that still works.
    /// A new channel replaces one that reached the server only once a call on
    /// it reaches the server too.
    pub(crate) async fn client(&self) -> ZerobusResult<GrpcClient> {
        self.acquire().await.map(|open| open.client)
    }

    async fn acquire(&self) -> ZerobusResult<OpenChannel> {
        match self.open_if_changed().await {
            Ok(open) => Ok(open),
            Err(err) => match self.lock().as_mut() {
                Some(channels) => {
                    warn!(error = %err, "Keeping the current gRPC channel: cannot use the current TLS settings");
                    Ok(channels.choose())
                }
                None => Err(err),
            },
        }
    }

    // The settings are read before taking the lock, so a slow certificate
    // volume doesn't hold up other streams that share this source.
    async fn open_if_changed(&self) -> ZerobusResult<OpenChannel> {
        let settings = self.tls_config.tls_settings()?;
        let resolved = settings.resolve_off_runtime().await?;
        let tls = resolved.fingerprint();
        if let Some(open) = self
            .lock()
            .as_mut()
            .and_then(|channels| channels.reuse(tls))
        {
            return Ok(open);
        }

        let opened = self.open(&resolved, tls)?;
        let mut channels = self.lock();
        Ok(match channels.as_mut() {
            Some(channels) => channels.replace(opened),
            None => {
                *channels = Some(Channels {
                    current: opened.clone(),
                    previous: None,
                    fallback_logged: false,
                });
                opened
            }
        })
    }

    fn lock(&self) -> MutexGuard<'_, Option<Channels>> {
        self.channels.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn open(&self, resolved: &ResolvedTls<'_>, tls: TlsFingerprint) -> ZerobusResult<OpenChannel> {
        let endpoint = resolved.apply(base_endpoint(&self.endpoint, &self.sdk_identifier)?)?;
        let host = endpoint.uri().host().unwrap_or_default().to_string();
        let proxy_connector = proxy::resolve_connector(&host, self.connector_factory.as_ref())?;
        let channel = match proxy_connector {
            Some(pc) => endpoint.connect_with_connector_lazy(pc),
            None => endpoint.connect_lazy(),
        };

        let health = Arc::new(ChannelHealth::new(Arc::clone(&self.failures)));
        let tracked = TrackedChannel {
            channel,
            health: Arc::clone(&health),
        };
        Ok(OpenChannel {
            client: ZerobusClient::new(tracked)
                .max_decoding_message_size(usize::MAX)
                .max_encoding_message_size(usize::MAX),
            tls,
            health,
        })
    }
}

/// Parses `endpoint` and sets the `user-agent` header. Both fail the same way on
/// every attempt, so the errors are not retryable.
pub(crate) fn base_endpoint(endpoint: &str, sdk_identifier: &str) -> ZerobusResult<Endpoint> {
    Endpoint::from_shared(endpoint.to_string())
        .map_err(|err| ZerobusError::InvalidZerobusEndpointError(format!("{endpoint}: {err}")))?
        .user_agent(sdk_identifier)
        .map_err(|err| ZerobusError::InvalidArgument(err.to_string()))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use super::*;
    use crate::TlsSettings;

    /// Returns whatever settings the test last set, and counts the calls.
    struct SwitchableTlsConfig {
        settings: Mutex<ZerobusResult<TlsSettings>>,
        calls: AtomicUsize,
    }

    impl SwitchableTlsConfig {
        fn new(settings: TlsSettings) -> Arc<Self> {
            Arc::new(Self {
                settings: Mutex::new(Ok(settings)),
                calls: AtomicUsize::new(0),
            })
        }

        fn set(&self, settings: ZerobusResult<TlsSettings>) {
            *self.settings.lock().unwrap() = settings;
        }
    }

    impl TlsConfig for SwitchableTlsConfig {
        fn tls_settings(&self) -> ZerobusResult<TlsSettings> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.settings.lock().unwrap().clone()
        }
    }

    // The domain name is part of the fingerprint, so changing it stands in for
    // rotated certificates without needing real ones.
    fn settings_for(domain_name: &str) -> TlsSettings {
        TlsSettings::new().with_domain_name(domain_name)
    }

    fn source(tls_config: Arc<SwitchableTlsConfig>) -> GrpcChannelSource {
        source_for("https://localhost:443", "zerobus-test", tls_config)
    }

    fn source_for(
        endpoint: &str,
        sdk_identifier: &str,
        tls_config: Arc<SwitchableTlsConfig>,
    ) -> GrpcChannelSource {
        GrpcChannelSource::new(
            endpoint.to_string(),
            Arc::from(sdk_identifier),
            tls_config,
            None,
        )
    }

    fn current_fingerprint(source: &GrpcChannelSource) -> TlsFingerprint {
        source.lock().as_ref().unwrap().current.tls
    }

    fn has_previous(source: &GrpcChannelSource) -> bool {
        source.lock().as_ref().unwrap().previous.is_some()
    }

    #[tokio::test]
    async fn client_reuses_the_channel_while_the_settings_are_unchanged() {
        let tls = SwitchableTlsConfig::new(settings_for("a.example.com"));
        let source = source(Arc::clone(&tls));

        source.client().await.unwrap();
        let first = current_fingerprint(&source);
        source.client().await.unwrap();

        assert_eq!(current_fingerprint(&source), first);
        // Each call reads the settings, so a change would be noticed.
        assert_eq!(tls.calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn client_opens_a_new_channel_when_the_settings_change() {
        let tls = SwitchableTlsConfig::new(settings_for("a.example.com"));
        let source = source(Arc::clone(&tls));
        source.client().await.unwrap();
        let first = current_fingerprint(&source);

        tls.set(Ok(settings_for("b.example.com")));
        source.client().await.unwrap();

        assert_ne!(current_fingerprint(&source), first);
    }

    #[tokio::test]
    async fn client_keeps_the_channel_when_the_settings_can_no_longer_be_read() {
        let tls = SwitchableTlsConfig::new(settings_for("a.example.com"));
        let source = source(Arc::clone(&tls));
        source.client().await.unwrap();
        let first = current_fingerprint(&source);

        tls.set(Err(ZerobusError::InvalidArgument(
            "cannot read TLS file".to_string(),
        )));

        assert!(source.client().await.is_ok());
        assert_eq!(current_fingerprint(&source), first);
    }

    #[tokio::test]
    async fn client_keeps_the_channel_when_new_settings_cannot_be_applied() {
        let tls = SwitchableTlsConfig::new(settings_for("a.example.com"));
        let source = source(Arc::clone(&tls));
        source.client().await.unwrap();
        let first = current_fingerprint(&source);

        // Trusts no CA, so applying the settings fails.
        tls.set(Ok(TlsSettings::new().without_native_roots()));

        assert!(source.client().await.is_ok());
        assert_eq!(current_fingerprint(&source), first);
    }

    /// Rotates from a channel that reached the server to one that has not been
    /// used yet, and returns the two.
    async fn rotate_from_a_working_channel(
        tls: &SwitchableTlsConfig,
        source: &GrpcChannelSource,
    ) -> (OpenChannel, OpenChannel) {
        let working = source.acquire().await.unwrap();
        working.health.record(true);

        tls.set(Ok(settings_for("b.example.com")));
        let rotated = source.acquire().await.unwrap();
        assert_ne!(
            rotated.tls, working.tls,
            "the first call after a change must try the new channel"
        );
        (working, rotated)
    }

    #[tokio::test]
    async fn client_goes_back_to_the_previous_channel_when_the_new_one_fails_first() {
        let tls = SwitchableTlsConfig::new(settings_for("a.example.com"));
        let source = source(Arc::clone(&tls));
        let (working, rotated) = rotate_from_a_working_channel(&tls, &source).await;

        rotated.health.record(false);

        // The files haven't changed again, so later calls keep using the channel that works.
        for _ in 0..2 {
            let next = source.acquire().await.unwrap();
            assert_eq!(next.tls, working.tls);
            next.health.record(true);
        }
        assert_eq!(current_fingerprint(&source), rotated.tls);
    }

    #[tokio::test]
    async fn client_drops_the_previous_channel_once_the_new_one_reaches_the_server() {
        let tls = SwitchableTlsConfig::new(settings_for("a.example.com"));
        let source = source(Arc::clone(&tls));
        let (_, rotated) = rotate_from_a_working_channel(&tls, &source).await;

        rotated.health.record(true);

        assert_eq!(source.acquire().await.unwrap().tls, rotated.tls);
        assert!(!has_previous(&source));
    }

    #[tokio::test]
    async fn client_tries_the_new_channel_again_when_the_previous_one_fails_too() {
        let tls = SwitchableTlsConfig::new(settings_for("a.example.com"));
        let source = source(Arc::clone(&tls));
        let (working, rotated) = rotate_from_a_working_channel(&tls, &source).await;

        rotated.health.record(false);
        let fallback = source.acquire().await.unwrap();
        assert_eq!(fallback.tls, working.tls);

        // While both fail, the channel that failed longer ago goes next.
        fallback.health.record(false);
        let retried = source.acquire().await.unwrap();
        assert_eq!(retried.tls, rotated.tls);
        retried.health.record(false);
        assert_eq!(source.acquire().await.unwrap().tls, working.tls);
    }

    #[tokio::test]
    async fn client_reuses_the_previous_channel_when_the_settings_change_back() {
        let tls = SwitchableTlsConfig::new(settings_for("a.example.com"));
        let source = source(Arc::clone(&tls));
        let (working, _) = rotate_from_a_working_channel(&tls, &source).await;

        tls.set(Ok(settings_for("a.example.com")));

        assert_eq!(source.acquire().await.unwrap().tls, working.tls);
        assert_eq!(current_fingerprint(&source), working.tls);
        assert!(!has_previous(&source));
    }

    #[tokio::test]
    async fn client_keeps_the_working_channel_across_more_than_one_failed_rotation() {
        let tls = SwitchableTlsConfig::new(settings_for("a.example.com"));
        let source = source(Arc::clone(&tls));
        let (working, rotated) = rotate_from_a_working_channel(&tls, &source).await;
        rotated.health.record(false);

        tls.set(Ok(settings_for("c.example.com")));
        let rotated_again = source.acquire().await.unwrap();
        assert_ne!(rotated_again.tls, working.tls);
        rotated_again.health.record(false);

        assert_eq!(source.acquire().await.unwrap().tls, working.tls);
    }

    #[tokio::test]
    async fn client_keeps_no_previous_channel_that_never_reached_the_server() {
        let tls = SwitchableTlsConfig::new(settings_for("a.example.com"));
        let source = source(Arc::clone(&tls));
        let never_worked = source.acquire().await.unwrap();
        never_worked.health.record(false);

        tls.set(Ok(settings_for("b.example.com")));
        let rotated = source.acquire().await.unwrap();
        rotated.health.record(false);

        assert_eq!(source.acquire().await.unwrap().tls, rotated.tls);
        assert!(!has_previous(&source));
    }

    #[tokio::test]
    async fn first_client_call_reports_setup_errors() {
        let tls = SwitchableTlsConfig::new(TlsSettings::new().without_native_roots());
        let source = source(tls);

        let err = source.client().await.unwrap_err();

        assert!(matches!(err, ZerobusError::InvalidArgument(_)), "{err:?}");
        assert!(source.lock().is_none());
    }

    /// The first open runs inside the stream's retry loop, so an endpoint or a
    /// `user-agent` that can never work must not be retryable.
    #[tokio::test]
    async fn first_client_call_reports_an_invalid_endpoint_or_user_agent_as_not_retryable() {
        let tls = || SwitchableTlsConfig::new(TlsSettings::new());

        let err = source_for("https://bad host.example.com", "zerobus-test", tls())
            .client()
            .await
            .unwrap_err();
        assert!(
            matches!(err, ZerobusError::InvalidZerobusEndpointError(_)),
            "{err:?}"
        );
        assert!(!err.is_retryable());
        assert!(err.to_string().contains("bad host.example.com"), "{err}");

        let err = source_for("https://localhost:443", "zerobus-test\n", tls())
            .client()
            .await
            .unwrap_err();
        assert!(matches!(err, ZerobusError::InvalidArgument(_)), "{err:?}");
        assert!(!err.is_retryable());
    }
}
