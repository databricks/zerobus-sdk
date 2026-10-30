//! Builder state shared by every client kind that targets one table.
//!
//! [`StreamBuilder`](super::StreamBuilder) wraps a [`ClientBuilderCore`] and
//! forwards its public setters to it. The core holds only what is independent
//! of the transport: the SDK, the table name, and the authentication choice.

use std::sync::Arc;
use std::time::Duration;

#[cfg(feature = "testing")]
use crate::headers_provider::NoAuthHeadersProvider;
use crate::headers_provider::{
    FederatedTokenProvider, HeadersProvider, IdpTokenSupplier, OAuthHeadersProvider,
};
use crate::{ZerobusError, ZerobusResult, ZerobusSdk};

/// Internal representation of the authentication configuration.
enum AuthConfig {
    OAuth {
        client_id: String,
        client_secret: String,
    },
    /// External-IdP federation via RFC 8693 token exchange. `client_id` is
    /// `Some` for workload identity federation and `None` for account-level
    /// federation. The shared token cache is partitioned by the identity the
    /// caller already holds — the service principal `client_id` for workload,
    /// or the `idp_token_supplier`'s own stable identity (not a reusable pointer)
    /// for account-level — so distinct identities never collide, without the
    /// caller passing a partition key.
    FederatedAuth {
        idp_token_supplier: IdpTokenSupplier,
        client_id: Option<String>,
    },
    HeadersProvider(Arc<dyn HeadersProvider>),
    #[cfg(feature = "testing")]
    NoAuth,
}

const fn missing_auth_error() -> &'static str {
    #[cfg(feature = "testing")]
    {
        "authentication is required: call .oauth(), .federated_auth(), .headers_provider(), or .no_auth()"
    }
    #[cfg(not(feature = "testing"))]
    {
        "authentication is required: call .oauth(), .federated_auth(), or .headers_provider()"
    }
}

/// Table and authentication settings shared by the client builders.
pub(crate) struct ClientBuilderCore<'a> {
    pub(super) sdk: &'a ZerobusSdk,
    pub(super) table_name: String,
    auth: Option<AuthConfig>,
}

#[allow(clippy::result_large_err)]
impl<'a> ClientBuilderCore<'a> {
    pub(super) fn new(sdk: &'a ZerobusSdk) -> Self {
        Self {
            sdk,
            table_name: String::new(),
            auth: None,
        }
    }

    pub(super) fn set_table(&mut self, table_name: String) {
        self.table_name = table_name;
    }

    pub(super) fn set_oauth(&mut self, client_id: String, client_secret: String) {
        self.auth = Some(AuthConfig::OAuth {
            client_id,
            client_secret,
        });
    }

    pub(super) fn set_federated_auth(
        &mut self,
        idp_token_supplier: IdpTokenSupplier,
        client_id: Option<String>,
    ) {
        self.auth = Some(AuthConfig::FederatedAuth {
            idp_token_supplier,
            client_id,
        });
    }

    pub(super) fn set_headers_provider(&mut self, provider: Arc<dyn HeadersProvider>) {
        self.auth = Some(AuthConfig::HeadersProvider(provider));
    }

    #[cfg(feature = "testing")]
    pub(super) fn set_no_auth(&mut self) {
        self.auth = Some(AuthConfig::NoAuth);
    }

    /// Name of the configured auth kind, for `Debug` output.
    pub(super) fn auth_kind(&self) -> &'static str {
        match &self.auth {
            Some(AuthConfig::OAuth { .. }) => "OAuth",
            Some(AuthConfig::FederatedAuth { .. }) => "FederatedAuth",
            Some(AuthConfig::HeadersProvider(_)) => "HeadersProvider",
            #[cfg(feature = "testing")]
            Some(AuthConfig::NoAuth) => "NoAuth",
            None => "None",
        }
    }

    /// Checks that the table name and authentication are set, in that order.
    pub(super) fn validate_target(&self) -> ZerobusResult<()> {
        if self.table_name.is_empty() {
            return Err(ZerobusError::InvalidArgument(
                "table name is required: call .table()".into(),
            ));
        }
        if self.auth.is_none() {
            return Err(ZerobusError::InvalidArgument(missing_auth_error().into()));
        }
        Ok(())
    }

    /// Resolve the headers provider from the stored auth config. For OAuth and
    /// federation, a proactive token refresh is bounded by `refresh_timeout`.
    pub(super) fn resolve_headers_provider(
        &self,
        refresh_timeout: Duration,
    ) -> ZerobusResult<Arc<dyn HeadersProvider>> {
        match self.auth.as_ref() {
            Some(AuthConfig::OAuth {
                client_id,
                client_secret,
            }) => Ok(Arc::new(OAuthHeadersProvider::with_cache(
                client_id.clone(),
                client_secret.clone(),
                self.table_name.clone(),
                self.sdk.workspace_id.clone(),
                self.sdk.unity_catalog_url.clone(),
                Arc::clone(&self.sdk.token_cache),
                Some(refresh_timeout),
            ))),
            Some(AuthConfig::FederatedAuth {
                idp_token_supplier,
                client_id,
            }) => {
                // Federation mirrors OAuth's proactive-refresh bound so a stalled
                // token exchange falls back to the cached token before the setup
                // deadline, keeping client-credentials and token-exchange at parity.
                Ok(Arc::new(FederatedTokenProvider::with_cache(
                    client_id.clone(),
                    idp_token_supplier.clone(),
                    self.table_name.clone(),
                    self.sdk.workspace_id.clone(),
                    self.sdk.unity_catalog_url.clone(),
                    Arc::clone(&self.sdk.token_cache),
                    Some(refresh_timeout),
                )))
            }
            Some(AuthConfig::HeadersProvider(p)) => Ok(Arc::clone(p)),
            #[cfg(feature = "testing")]
            Some(AuthConfig::NoAuth) => Ok(Arc::new(NoAuthHeadersProvider)),
            None => Err(ZerobusError::InvalidArgument(missing_auth_error().into())),
        }
    }
}
