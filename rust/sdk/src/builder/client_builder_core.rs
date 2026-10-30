//! Builder state shared by every client kind that targets one table.
//!
//! [`StreamBuilder`](super::StreamBuilder) wraps a [`ClientBuilderCore`] and
//! forwards its public setters to it. The core holds only what is independent
//! of the transport: the SDK, the table name, the authentication choice, and
//! the record format. Each wrapper decides which formats it exposes and how it
//! reports a missing one.

use std::sync::Arc;
use std::time::Duration;

#[cfg(feature = "testing")]
use crate::headers_provider::NoAuthHeadersProvider;
use crate::headers_provider::{
    FederatedTokenProvider, HeadersProvider, IdpTokenSupplier, OAuthHeadersProvider,
};
#[cfg(feature = "arrow-flight")]
use crate::stream::ArrowSchema;
#[cfg(feature = "avro")]
use crate::AvroSchema;
use crate::{MessageDescriptor, RecordType, ZerobusError, ZerobusResult, ZerobusSdk};

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

/// Which record format was selected.
pub(super) enum FormatConfig {
    Json,
    CompiledProto(Box<prost_types::DescriptorProto>),
    DynamicProto(MessageDescriptor),
    #[cfg(feature = "arrow-flight")]
    Arrow(Arc<ArrowSchema>),
    #[cfg(feature = "avro")]
    Avro(String),
}

/// A non-Arrow format resolved into the pieces a gRPC request needs.
pub(super) struct GrpcFormat {
    pub(super) record_type: RecordType,
    pub(super) descriptor_proto: Option<prost_types::DescriptorProto>,
    pub(super) message_descriptor: Option<MessageDescriptor>,
    #[cfg(feature = "avro")]
    pub(super) avro_schema: Option<Arc<AvroSchema>>,
}

impl FormatConfig {
    pub(super) fn into_grpc(self) -> ZerobusResult<GrpcFormat> {
        let (record_type, descriptor_proto, message_descriptor) = match self {
            Self::Json => (RecordType::Json, None, None),
            Self::CompiledProto(descriptor) => (RecordType::Proto, Some(*descriptor), None),
            Self::DynamicProto(descriptor) => (
                RecordType::Proto,
                Some(descriptor.descriptor_proto().clone()),
                Some(descriptor),
            ),
            #[cfg(feature = "arrow-flight")]
            Self::Arrow(_) => {
                return Err(ZerobusError::InvalidArgument(
                    "Arrow format requires .build_arrow() instead of .build()".into(),
                ));
            }
            #[cfg(feature = "avro")]
            Self::Avro(json) => {
                let parsed = apache_avro::Schema::parse_str(&json).map_err(|e| {
                    ZerobusError::AvroSchemaParseError(format!("Failed to parse Avro schema: {e}"))
                })?;
                return Ok(GrpcFormat {
                    record_type: RecordType::Avro,
                    descriptor_proto: None,
                    message_descriptor: None,
                    avro_schema: Some(Arc::new(AvroSchema { json, parsed })),
                });
            }
        };

        Ok(GrpcFormat {
            record_type,
            descriptor_proto,
            message_descriptor,
            #[cfg(feature = "avro")]
            avro_schema: None,
        })
    }
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

/// Table, authentication, and format settings shared by the client builders.
pub(crate) struct ClientBuilderCore<'a> {
    pub(super) sdk: &'a ZerobusSdk,
    pub(super) table_name: String,
    auth: Option<AuthConfig>,
    pub(super) format: Option<FormatConfig>,
}

#[allow(clippy::result_large_err)]
impl<'a> ClientBuilderCore<'a> {
    pub(super) fn new(sdk: &'a ZerobusSdk) -> Self {
        Self {
            sdk,
            table_name: String::new(),
            auth: None,
            format: None,
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

    pub(super) fn set_format(&mut self, format: FormatConfig) {
        self.format = Some(format);
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

    /// Name of the configured format, for `Debug` output.
    pub(super) fn format_kind(&self) -> &'static str {
        match &self.format {
            Some(FormatConfig::Json) => "Json",
            Some(FormatConfig::CompiledProto(_)) => "CompiledProto",
            Some(FormatConfig::DynamicProto(_)) => "DynamicProto",
            #[cfg(feature = "arrow-flight")]
            Some(FormatConfig::Arrow(_)) => "Arrow",
            #[cfg(feature = "avro")]
            Some(FormatConfig::Avro(_)) => "Avro",
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
