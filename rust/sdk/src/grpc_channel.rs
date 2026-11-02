//! gRPC channels that follow changes to the TLS settings.
//!
//! A [`GrpcChannelSource`] hands out the channel for gRPC streams. Before it
//! reuses a channel, it reads the TLS settings again and opens a new channel if
//! they changed, so rotated certificates reach a shared channel and recovery
//! attempts, not only new dedicated channels.

use std::sync::Arc;

use tonic::transport::{Channel, Endpoint};
use tracing::{info, warn};

use crate::databricks::zerobus::zerobus_client::ZerobusClient;
use crate::proxy::{self, ConnectorFactory};
use crate::tls_config::{ResolvedTls, TlsFingerprint};
use crate::{TlsConfig, ZerobusError, ZerobusResult};

pub(crate) struct GrpcChannelSource {
    endpoint: String,
    sdk_identifier: Arc<str>,
    tls_config: Arc<dyn TlsConfig>,
    connector_factory: Option<ConnectorFactory>,
    current: tokio::sync::Mutex<Option<OpenChannel>>,
}

struct OpenChannel {
    client: ZerobusClient<Channel>,
    tls: TlsFingerprint,
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
            current: tokio::sync::Mutex::new(None),
        }
    }

    /// Returns the channel for the next stream or recovery attempt.
    ///
    /// The first call opens the channel and returns any error. Later calls read
    /// the TLS settings again and open a new channel if they changed. If the
    /// settings can't be read or applied at that point, the current channel is
    /// kept, so a broken rotation doesn't take down a channel that still works.
    pub(crate) async fn client(&self) -> ZerobusResult<ZerobusClient<Channel>> {
        let mut current = self.current.lock().await;
        if let Some(open) = current.as_ref() {
            let (tls, client) = (open.tls, open.client.clone());
            return match self.reopen_if_changed(tls) {
                Ok(None) => Ok(client),
                Ok(Some(reopened)) => {
                    info!("TLS settings changed; opening a new gRPC channel");
                    let client = reopened.client.clone();
                    *current = Some(reopened);
                    Ok(client)
                }
                Err(err) => {
                    warn!(error = %err, "Keeping the current gRPC channel: cannot use the current TLS settings");
                    Ok(client)
                }
            };
        }

        let settings = self.tls_config.tls_settings()?;
        let opened = self.open(&settings.resolve()?)?;
        let client = opened.client.clone();
        *current = Some(opened);
        Ok(client)
    }

    fn reopen_if_changed(&self, tls: TlsFingerprint) -> ZerobusResult<Option<OpenChannel>> {
        let settings = self.tls_config.tls_settings()?;
        let resolved = settings.resolve()?;
        if resolved.fingerprint() == tls {
            return Ok(None);
        }
        self.open(&resolved).map(Some)
    }

    fn open(&self, tls: &ResolvedTls<'_>) -> ZerobusResult<OpenChannel> {
        let endpoint = Endpoint::from_shared(self.endpoint.clone())
            .map_err(|err| ZerobusError::ChannelCreationError(err.to_string()))?
            .user_agent(self.sdk_identifier.as_ref())
            .map_err(|err| ZerobusError::ChannelCreationError(err.to_string()))?;

        let endpoint = tls.apply(endpoint)?;
        let host = endpoint.uri().host().unwrap_or_default().to_string();
        let proxy_connector = proxy::resolve_connector(&host, self.connector_factory.as_ref())?;
        let channel = match proxy_connector {
            Some(pc) => endpoint.connect_with_connector_lazy(pc),
            None => endpoint.connect_lazy(),
        };

        Ok(OpenChannel {
            client: ZerobusClient::new(channel)
                .max_decoding_message_size(usize::MAX)
                .max_encoding_message_size(usize::MAX),
            tls: tls.fingerprint(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

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
        GrpcChannelSource::new(
            "https://localhost:443".to_string(),
            Arc::from("zerobus-test"),
            tls_config,
            None,
        )
    }

    async fn current_fingerprint(source: &GrpcChannelSource) -> TlsFingerprint {
        source.current.lock().await.as_ref().unwrap().tls
    }

    #[tokio::test]
    async fn client_reuses_the_channel_while_the_settings_are_unchanged() {
        let tls = SwitchableTlsConfig::new(settings_for("a.example.com"));
        let source = source(Arc::clone(&tls));

        source.client().await.unwrap();
        let first = current_fingerprint(&source).await;
        source.client().await.unwrap();

        assert_eq!(current_fingerprint(&source).await, first);
        // Each call reads the settings, so a change would be noticed.
        assert_eq!(tls.calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn client_opens_a_new_channel_when_the_settings_change() {
        let tls = SwitchableTlsConfig::new(settings_for("a.example.com"));
        let source = source(Arc::clone(&tls));
        source.client().await.unwrap();
        let first = current_fingerprint(&source).await;

        tls.set(Ok(settings_for("b.example.com")));
        source.client().await.unwrap();

        assert_ne!(current_fingerprint(&source).await, first);
    }

    #[tokio::test]
    async fn client_keeps_the_channel_when_the_settings_can_no_longer_be_read() {
        let tls = SwitchableTlsConfig::new(settings_for("a.example.com"));
        let source = source(Arc::clone(&tls));
        source.client().await.unwrap();
        let first = current_fingerprint(&source).await;

        tls.set(Err(ZerobusError::InvalidArgument(
            "cannot read TLS file".to_string(),
        )));

        assert!(source.client().await.is_ok());
        assert_eq!(current_fingerprint(&source).await, first);
    }

    #[tokio::test]
    async fn client_keeps_the_channel_when_new_settings_cannot_be_applied() {
        let tls = SwitchableTlsConfig::new(settings_for("a.example.com"));
        let source = source(Arc::clone(&tls));
        source.client().await.unwrap();
        let first = current_fingerprint(&source).await;

        // Trusts no CA, so applying the settings fails.
        tls.set(Ok(TlsSettings::new().without_native_roots()));

        assert!(source.client().await.is_ok());
        assert_eq!(current_fingerprint(&source).await, first);
    }

    #[tokio::test]
    async fn first_client_call_reports_setup_errors() {
        let tls = SwitchableTlsConfig::new(TlsSettings::new().without_native_roots());
        let source = source(tls);

        let err = source.client().await.unwrap_err();

        assert!(matches!(err, ZerobusError::InvalidArgument(_)), "{err:?}");
        assert!(source.current.lock().await.is_none());
    }
}
