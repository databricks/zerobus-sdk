//! gRPC channels that follow changes to the TLS settings.
//!
//! A [`GrpcChannelSource`] hands out the channel for gRPC streams and unary
//! clients, and opens a new one when the TLS settings change. The fallback rule
//! for a replaced channel is in the TLS Configuration section of the Rust README.

use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll};
use std::time::Duration;

use tonic::body::Body;
use tonic::codegen::{http, BoxFuture, Service};
use tonic::transport::{Channel, Endpoint};
use tracing::{info, warn};

use crate::databricks::zerobus::zerobus_client::ZerobusClient;
use crate::grpc_status::GrpcStatus;
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
    /// HTTP/2 keepalive interval and timeout of every channel opened, if set.
    keepalive: Option<(Duration, Duration)>,
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
    /// Counts the changes to `current` and `previous`, so a TLS snapshot read
    /// before a change is not applied after it.
    generation: u64,
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
/// A gRPC response with an error status still counts: the server answered, so
/// the TLS handshake worked. Under TLS 1.3 the server checks the client
/// certificate after the client considers the handshake done, so connecting
/// alone doesn't show that the server accepts the certificate. The first call does.
#[derive(Clone, Debug)]
pub(crate) struct TrackedChannel {
    channel: Channel,
    health: Arc<ChannelHealth>,
}

/// How far one attempt's request got. The attempt attaches a shared copy to its
/// request. `TrackedChannel` sets `sent` when the call is submitted and `answered`
/// when a gRPC response arrives.
#[derive(Debug, Default)]
pub(crate) struct AttemptProgress {
    sent: AtomicBool,
    answered: AtomicBool,
}

impl AttemptProgress {
    pub(crate) fn sent(&self) -> bool {
        self.sent.load(Ordering::Relaxed)
    }

    pub(crate) fn answered(&self) -> bool {
        self.answered.load(Ordering::Relaxed)
    }

    fn is_unanswered(&self) -> bool {
        self.sent() && !self.answered()
    }
}

/// Reports that an attempt on a channel timed out before the server answered.
/// Only an attempt's own timeout calls this: a call dropped by its caller says
/// nothing about the channel.
#[derive(Debug)]
pub(crate) struct ChannelTimeout(Arc<ChannelHealth>);

impl ChannelTimeout {
    /// Records a failed attempt on the channel if its request was sent and no
    /// gRPC response came back.
    pub(crate) fn record_timeout(&self, progress: &AttemptProgress) {
        if progress.is_unanswered() {
            self.0.record(false);
        }
    }

    /// Runs one attempt on the channel, bounded by `timeout_ms`. If the attempt
    /// times out, the channel records a failure only when the request was sent
    /// and unanswered, and the attempt fails with a deadline error.
    pub(crate) async fn bounded<T>(
        &self,
        timeout_ms: u64,
        progress: &AttemptProgress,
        attempt: impl Future<Output = ZerobusResult<T>>,
    ) -> ZerobusResult<T> {
        match tokio::time::timeout(Duration::from_millis(timeout_ms), attempt).await {
            Ok(result) => result,
            Err(_) => {
                self.record_timeout(progress);
                Err(ZerobusError::CreateStreamError(
                    GrpcStatus::deadline_exceeded("Stream creation timed out"),
                ))
            }
        }
    }
}

/// Whether `response` came from the gRPC server, rather than from a proxy or an
/// error page in front of it.
fn is_grpc_response(response: &http::Response<Body>) -> bool {
    response
        .headers()
        .get(http::header::CONTENT_TYPE)
        .is_some_and(|value| value.as_bytes().starts_with(b"application/grpc"))
}

impl Service<http::Request<Body>> for TrackedChannel {
    type Response = http::Response<Body>;
    type Error = tonic::transport::Error;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.channel.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        // Tonic calls this only once the channel has a free slot, so a request that
        // waits for one is not marked sent.
        let progress = request.extensions().get::<Arc<AttemptProgress>>().cloned();
        if let Some(progress) = &progress {
            progress.sent.store(true, Ordering::Relaxed);
        }
        let response = self.channel.call(request);
        let health = Arc::clone(&self.health);
        Box::pin(async move {
            let response = response.await;
            let answered = response.as_ref().is_ok_and(is_grpc_response);
            if let (Some(progress), true) = (&progress, answered) {
                progress.answered.store(true, Ordering::Relaxed);
            }
            health.record(answered);
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
                self.generation += 1;
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
            self.generation += 1;
        }
        Some(self.choose())
    }

    /// Makes `opened` the current channel, unless the current one already has
    /// the same material.
    fn replace(&mut self, opened: OpenChannel) -> OpenChannel {
        if self.current.tls != opened.tls {
            info!("TLS settings changed; opened a new gRPC channel");
            let replaced = std::mem::replace(&mut self.current, opened);
            if replaced.health.reached_server() {
                self.previous = Some(replaced);
            }
            self.generation += 1;
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
            keepalive: None,
            failures: Arc::new(AtomicU64::new(0)),
            channels: Mutex::new(None),
        }
    }

    /// Makes every channel this source opens send an HTTP/2 PING after
    /// `interval` without a received frame, only while a call is in flight, and
    /// close the connection if the reply takes longer than `timeout`.
    pub(crate) fn with_keepalive(mut self, interval: Duration, timeout: Duration) -> Self {
        self.keepalive = Some((interval, timeout));
        self
    }

    /// Returns the channel for the next stream, recovery or unary attempt, and
    /// the handle that reports the attempt's own timeout on it. Report a timeout
    /// only for the attempt's own deadline, not when the caller cancels.
    ///
    /// The first call opens the channel and returns any error. Later calls read
    /// the TLS settings again and open a new channel if they changed. If the
    /// settings can't be read or applied at that point, the current channel is
    /// kept, so a broken rotation doesn't take down a channel that still works.
    pub(crate) async fn client(&self) -> ZerobusResult<(GrpcClient, ChannelTimeout)> {
        self.acquire()
            .await
            .map(|open| (open.client, ChannelTimeout(open.health)))
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
    // A call that publishes a channel while this one reads the files makes the
    // snapshot stale, so the files are read again.
    async fn open_if_changed(&self) -> ZerobusResult<OpenChannel> {
        loop {
            let seen = self.lock().as_ref().map(|channels| channels.generation);
            let settings = self.tls_config.tls_settings()?;
            let resolved = settings.resolve_off_runtime().await?;
            let tls = resolved.fingerprint();
            {
                let mut channels = self.lock();
                if let Some(channels) = channels.as_mut() {
                    if Some(channels.generation) != seen {
                        continue;
                    }
                    if let Some(open) = channels.reuse(tls) {
                        return Ok(open);
                    }
                }
            }

            let opened = self.open(&resolved, tls)?;
            let mut channels = self.lock();
            if let Some(channels) = channels.as_mut() {
                if Some(channels.generation) != seen {
                    continue;
                }
                return Ok(channels.replace(opened));
            }
            *channels = Some(Channels {
                current: opened.clone(),
                previous: None,
                fallback_logged: false,
                generation: 0,
            });
            return Ok(opened);
        }
    }

    fn lock(&self) -> MutexGuard<'_, Option<Channels>> {
        self.channels.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn open(&self, resolved: &ResolvedTls, tls: TlsFingerprint) -> ZerobusResult<OpenChannel> {
        let mut endpoint = base_endpoint(&self.endpoint, &self.sdk_identifier)?;
        if let Some((interval, timeout)) = self.keepalive {
            endpoint = endpoint
                .http2_keep_alive_interval(interval)
                .keep_alive_timeout(timeout)
                .keep_alive_while_idle(false);
        }
        let endpoint = resolved.apply(endpoint)?;
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
impl GrpcChannelSource {
    /// Records that the current channel reached the server, without a call.
    pub(crate) async fn mark_current_reached_for_test(&self) {
        let open = self.acquire().await.unwrap();
        open.health.record(true);
    }

    /// The TLS fingerprint of the channel a call would get now.
    pub(crate) async fn next_channel_fingerprint_for_test(&self) -> TlsFingerprint {
        self.acquire().await.unwrap().tls
    }
}

/// Helpers for tests that rotate TLS settings and stall calls.
#[cfg(test)]
pub(crate) mod test_support {
    use std::collections::HashMap;

    use super::*;
    use crate::{HeadersProvider, TlsSettings};

    /// TLS settings that a test can replace, to rotate the channel.
    pub(crate) struct RotatingTls(pub(crate) std::sync::Mutex<TlsSettings>);

    impl TlsConfig for RotatingTls {
        fn tls_settings(&self) -> ZerobusResult<TlsSettings> {
            Ok(self.0.lock().unwrap().clone())
        }
    }

    pub(crate) struct ReadyHeaders;

    #[async_trait::async_trait]
    impl HeadersProvider for ReadyHeaders {
        async fn get_headers(&self) -> ZerobusResult<HashMap<&'static str, String>> {
            Ok(HashMap::new())
        }
    }

    pub(crate) struct StalledHeaders;

    #[async_trait::async_trait]
    impl HeadersProvider for StalledHeaders {
        async fn get_headers(&self) -> ZerobusResult<HashMap<&'static str, String>> {
            std::future::pending().await
        }
    }

    /// A source whose previous channel reached the server, rotated to a channel
    /// that has not. Returns the source, the settings, and the two fingerprints.
    pub(crate) async fn rotated_source(
        endpoint: &str,
    ) -> (
        Arc<GrpcChannelSource>,
        Arc<RotatingTls>,
        TlsFingerprint,
        TlsFingerprint,
    ) {
        let tls = Arc::new(RotatingTls(std::sync::Mutex::new(
            TlsSettings::new().with_domain_name("a.example.com"),
        )));
        let source = Arc::new(GrpcChannelSource::new(
            endpoint.to_string(),
            Arc::from(crate::DEFAULT_SDK_IDENTIFIER),
            Arc::clone(&tls) as Arc<dyn TlsConfig>,
            None,
        ));
        let previous = source.next_channel_fingerprint_for_test().await;
        source.mark_current_reached_for_test().await;
        *tls.0.lock().unwrap() = TlsSettings::new().with_domain_name("b.example.com");
        let rotated = source.next_channel_fingerprint_for_test().await;
        assert_ne!(previous, rotated);
        (source, tls, previous, rotated)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    use super::*;
    use crate::TlsSettings;

    type OnRead = Box<dyn FnOnce() + Send>;

    /// Returns whatever settings the test last set, and counts the calls. A
    /// hook set with `on_next_read` runs right after the next read.
    struct SwitchableTlsConfig {
        settings: Mutex<ZerobusResult<TlsSettings>>,
        calls: AtomicUsize,
        on_read: Mutex<Option<OnRead>>,
    }

    impl SwitchableTlsConfig {
        fn new(settings: TlsSettings) -> Arc<Self> {
            Arc::new(Self {
                settings: Mutex::new(Ok(settings)),
                calls: AtomicUsize::new(0),
                on_read: Mutex::new(None),
            })
        }

        fn set(&self, settings: ZerobusResult<TlsSettings>) {
            *self.settings.lock().unwrap() = settings;
        }

        fn on_next_read(&self, hook: impl FnOnce() + Send + 'static) {
            *self.on_read.lock().unwrap() = Some(Box::new(hook));
        }
    }

    impl TlsConfig for SwitchableTlsConfig {
        fn tls_settings(&self) -> ZerobusResult<TlsSettings> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let settings = self.settings.lock().unwrap().clone();
            let hook = self.on_read.lock().unwrap().take();
            if let Some(hook) = hook {
                hook();
            }
            settings
        }
    }

    /// Answers every call with a gRPC error status, as the server does when it
    /// rejects a request.
    #[derive(Clone)]
    struct RejectingServer;

    impl tonic::server::NamedService for RejectingServer {
        const NAME: &'static str = "zerobus.test.Rejecting";
    }

    impl Service<http::Request<Body>> for RejectingServer {
        type Response = http::Response<Body>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _request: http::Request<Body>) -> Self::Future {
            Box::pin(async {
                Ok(tonic::Status::permission_denied("rejected by the test server").into_http())
            })
        }
    }

    /// Starts a plaintext gRPC server on a free local port and returns its URL.
    async fn serve_rejecting_server() -> String {
        let addr = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap();
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(RejectingServer)
                .serve(addr),
        );
        while std::net::TcpStream::connect(addr).is_err() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        format!("http://{addr}")
    }

    /// A local URL with nothing listening on it, so a connection to it is refused.
    fn unused_local_url() -> String {
        let addr = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap();
        format!("http://{addr}")
    }

    /// Makes one call on `channel`, waiting until it is ready first.
    async fn call_channel(
        channel: &mut TrackedChannel,
        uri: String,
    ) -> Result<http::Response<Body>, tonic::transport::Error> {
        std::future::poll_fn(|cx| channel.poll_ready(cx)).await?;
        channel
            .call(http::Request::post(uri).body(Body::empty()).unwrap())
            .await
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
    async fn an_attempt_that_finishes_in_time_records_nothing() {
        let tls = SwitchableTlsConfig::new(settings_for("a.example.com"));
        let source = source(Arc::clone(&tls));
        let (_, rotated) = rotate_from_a_working_channel(&tls, &source).await;

        let (_, timeout) = source.client().await.unwrap();
        let attempt = timeout
            .bounded(1_000, &AttemptProgress::default(), async { Ok(()) })
            .await;

        assert!(attempt.is_ok());
        assert_eq!(rotated.health.last_failure(), 0);
    }

    #[tokio::test]
    async fn a_call_dropped_before_a_response_leaves_the_channel_unchanged() {
        let tls = SwitchableTlsConfig::new(settings_for("a.example.com"));
        let source = source(Arc::clone(&tls));
        let (_, rotated) = rotate_from_a_working_channel(&tls, &source).await;

        // Dropped without a response, as when the caller cancels stream creation.
        let mut channel = TrackedChannel {
            channel: base_endpoint(&unused_local_url(), "zerobus-test")
                .unwrap()
                .connect_lazy(),
            health: Arc::clone(&rotated.health),
        };
        std::future::poll_fn(|cx| channel.poll_ready(cx))
            .await
            .unwrap();
        drop(channel.call(http::Request::new(Body::empty())));

        assert_eq!(rotated.health.last_failure(), 0);
        assert_eq!(source.acquire().await.unwrap().tls, rotated.tls);
    }

    #[tokio::test]
    async fn client_rereads_the_settings_when_a_channel_was_published_while_they_were_read() {
        let tls = SwitchableTlsConfig::new(settings_for("a.example.com"));
        let source = Arc::new(source(Arc::clone(&tls)));
        let working = source.acquire().await.unwrap();
        working.health.record(true);

        let hook_source = Arc::clone(&source);
        let hook_tls = Arc::clone(&tls);
        tls.on_next_read(move || {
            // Another call publishes the rotated channel after this call read the old settings.
            hook_tls.set(Ok(settings_for("b.example.com")));
            let settings = settings_for("b.example.com");
            let resolved = settings.resolve().unwrap();
            let opened = hook_source.open(&resolved, resolved.fingerprint()).unwrap();
            hook_source.lock().as_mut().unwrap().replace(opened);
        });

        let rotated_tls = settings_for("b.example.com")
            .resolve()
            .unwrap()
            .fingerprint();
        let stale_read = source.acquire().await.unwrap();

        assert_eq!(stale_read.tls, rotated_tls);
        assert_eq!(current_fingerprint(&source), rotated_tls);
        assert!(has_previous(&source));
    }

    /// After a gRPC response the attempt is answered, so a later timeout in the
    /// same attempt, such as a stalled credential invalidation, is not blamed on
    /// the channel.
    #[tokio::test]
    async fn a_timeout_after_a_grpc_response_records_nothing() {
        let health = Arc::new(ChannelHealth::new(Arc::new(AtomicU64::new(0))));
        let timeout = ChannelTimeout(Arc::clone(&health));
        let progress = AttemptProgress::default();
        progress.sent.store(true, Ordering::Relaxed);
        progress.answered.store(true, Ordering::Relaxed);

        timeout.record_timeout(&progress);

        assert_eq!(health.last_failure(), 0);
    }

    #[tokio::test]
    async fn a_request_is_marked_sent_when_called_and_answered_by_a_grpc_response() {
        let url = serve_rejecting_server().await;
        let health = Arc::new(ChannelHealth::new(Arc::new(AtomicU64::new(0))));
        let mut channel = TrackedChannel {
            channel: base_endpoint(&url, "zerobus-test").unwrap().connect_lazy(),
            health,
        };
        let progress = Arc::new(AttemptProgress::default());
        let mut request = http::Request::post(format!("{url}/zerobus.test.Rejecting/Call"))
            .body(Body::empty())
            .unwrap();
        request.extensions_mut().insert(Arc::clone(&progress));
        std::future::poll_fn(|cx| channel.poll_ready(cx))
            .await
            .unwrap();
        assert!(
            !progress.sent(),
            "readiness alone must not mark the request sent"
        );

        let response = channel.call(request);

        assert!(progress.sent());
        assert!(!progress.answered());
        response.await.unwrap();
        assert!(progress.answered());
    }

    #[tokio::test]
    async fn a_grpc_error_status_counts_as_reaching_the_server() {
        let url = serve_rejecting_server().await;
        let health = Arc::new(ChannelHealth::new(Arc::new(AtomicU64::new(0))));
        let mut channel = TrackedChannel {
            channel: base_endpoint(&url, "zerobus-test").unwrap().connect_lazy(),
            health: Arc::clone(&health),
        };

        let response = call_channel(&mut channel, format!("{url}/zerobus.test.Rejecting/Call"))
            .await
            .unwrap();

        assert_eq!(response.headers().get("grpc-status").unwrap(), "7");
        assert!(health.reached_server());
        assert_eq!(health.last_failure(), 0);
    }

    #[tokio::test]
    async fn a_failed_transport_call_does_not_count_as_reaching_the_server() {
        let url = unused_local_url();
        let health = Arc::new(ChannelHealth::new(Arc::new(AtomicU64::new(0))));
        let mut channel = TrackedChannel {
            channel: base_endpoint(&url, "zerobus-test").unwrap().connect_lazy(),
            health: Arc::clone(&health),
        };

        let result = call_channel(&mut channel, format!("{url}/zerobus.test.Rejecting/Call")).await;

        assert!(result.is_err());
        assert!(!health.reached_server());
        assert!(health.last_failure() > 0);
    }

    #[test]
    fn only_a_grpc_response_counts_as_reaching_the_server() {
        let grpc = http::Response::builder()
            .header(http::header::CONTENT_TYPE, "application/grpc")
            .body(Body::empty())
            .unwrap();
        let proxy_error = http::Response::builder()
            .status(503)
            .header(http::header::CONTENT_TYPE, "text/html")
            .body(Body::empty())
            .unwrap();
        let bare_error = http::Response::builder()
            .status(503)
            .body(Body::empty())
            .unwrap();

        assert!(is_grpc_response(&grpc));
        assert!(!is_grpc_response(&proxy_error));
        assert!(!is_grpc_response(&bare_error));
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
