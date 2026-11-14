//! Shared plumbing for persistent (Eos) streams, used by both the sync and async wrappers.
//!
//! The Python `PersistentStreamBuilder` collects settings in pure Python and hands them to
//! the native layer as one `PersistentStreamSettings`. Every setting is optional so the Rust
//! core keeps ownership of the defaults: an unset value is never forwarded to the builder.

use std::sync::Arc;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use databricks_zerobus_ingest_sdk::{
    AckCallback as RustAckCallback, HeadersProvider as RustHeadersProvider, PersistentStream,
    PersistentStreamBuilder, ZerobusSdk as RustSdk,
};

use crate::auth::HeadersProviderWrapper;
use crate::common::{AckCallback, AckCallbackWrapper, RecordFormat, TableProperties};

/// Settings collected by the Python persistent stream builder.
///
/// Not part of the public Python API: users configure streams through
/// `PersistentStreamBuilder`.
#[pyclass(skip_from_py_object)]
pub struct PersistentStreamSettings {
    client_id: Option<String>,
    client_secret: Option<String>,
    headers_provider: Option<Py<PyAny>>,
    recovery: Option<bool>,
    recovery_timeout_ms: Option<u64>,
    recovery_backoff_ms: Option<u64>,
    recovery_retries: Option<u32>,
    server_lack_of_ack_timeout_ms: Option<u64>,
    flush_timeout_ms: Option<u64>,
    max_inflight_requests: Option<usize>,
    max_ingest_payload_bytes: Option<usize>,
    ack_callback: Option<Py<AckCallback>>,
    /// `callback_max_wait_time_ms` accepts `None` (wait without limit), so "unset" needs its
    /// own flag.
    callback_max_wait_time_ms_set: bool,
    callback_max_wait_time_ms: Option<u64>,
}

#[pymethods]
impl PersistentStreamSettings {
    #[new]
    #[pyo3(signature = (
        client_id = None,
        client_secret = None,
        headers_provider = None,
        recovery = None,
        recovery_timeout_ms = None,
        recovery_backoff_ms = None,
        recovery_retries = None,
        server_lack_of_ack_timeout_ms = None,
        flush_timeout_ms = None,
        max_inflight_requests = None,
        max_ingest_payload_bytes = None,
        ack_callback = None,
        callback_max_wait_time_ms_set = false,
        callback_max_wait_time_ms = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        client_id: Option<String>,
        client_secret: Option<String>,
        headers_provider: Option<Py<PyAny>>,
        recovery: Option<bool>,
        recovery_timeout_ms: Option<u64>,
        recovery_backoff_ms: Option<u64>,
        recovery_retries: Option<u32>,
        server_lack_of_ack_timeout_ms: Option<u64>,
        flush_timeout_ms: Option<u64>,
        max_inflight_requests: Option<usize>,
        max_ingest_payload_bytes: Option<usize>,
        ack_callback: Option<Py<AckCallback>>,
        callback_max_wait_time_ms_set: bool,
        callback_max_wait_time_ms: Option<u64>,
    ) -> Self {
        Self {
            client_id,
            client_secret,
            headers_provider,
            recovery,
            recovery_timeout_ms,
            recovery_backoff_ms,
            recovery_retries,
            server_lack_of_ack_timeout_ms,
            flush_timeout_ms,
            max_inflight_requests,
            max_ingest_payload_bytes,
            ack_callback,
            callback_max_wait_time_ms_set,
            callback_max_wait_time_ms,
        }
    }
}

enum PersistentAuth {
    OAuth {
        client_id: String,
        client_secret: String,
    },
    Headers(Arc<dyn RustHeadersProvider>),
}

/// `PersistentStreamSettings` resolved into owned Rust values, so the async work does not
/// need the GIL to read them.
pub(crate) struct PersistentConfig {
    auth: PersistentAuth,
    table_properties: TableProperties,
    format: RecordFormat,
    recovery: Option<bool>,
    recovery_timeout_ms: Option<u64>,
    recovery_backoff_ms: Option<u64>,
    recovery_retries: Option<u32>,
    server_lack_of_ack_timeout_ms: Option<u64>,
    flush_timeout_ms: Option<u64>,
    max_inflight_requests: Option<usize>,
    max_ingest_payload_bytes: Option<usize>,
    ack_callback: Option<Arc<dyn RustAckCallback>>,
    callback_max_wait_time_ms: Option<Option<u64>>,
}

impl PersistentConfig {
    /// Validate the settings and the table properties and resolve them for use off the GIL.
    pub(crate) fn resolve(
        py: Python<'_>,
        settings: &PersistentStreamSettings,
        table_properties: &TableProperties,
    ) -> PyResult<Self> {
        if table_properties.avro_schema.is_some() {
            return Err(PyValueError::new_err(
                "persistent streams do not support the Avro record format; use JSON or protobuf",
            ));
        }
        let format = table_properties.resolve_format(Default::default())?;

        let auth = match (
            &settings.client_id,
            &settings.client_secret,
            &settings.headers_provider,
        ) {
            (Some(client_id), Some(client_secret), None) => PersistentAuth::OAuth {
                client_id: client_id.clone(),
                client_secret: client_secret.clone(),
            },
            (None, None, Some(provider)) => PersistentAuth::Headers(Arc::new(
                HeadersProviderWrapper::new(provider.clone_ref(py)),
            )),
            _ => {
                return Err(PyValueError::new_err(
                    "set exactly one authentication method: oauth(client_id, client_secret) \
                     or headers_provider(provider)",
                ))
            }
        };

        Ok(Self {
            auth,
            table_properties: table_properties.clone(),
            format,
            recovery: settings.recovery,
            recovery_timeout_ms: settings.recovery_timeout_ms,
            recovery_backoff_ms: settings.recovery_backoff_ms,
            recovery_retries: settings.recovery_retries,
            server_lack_of_ack_timeout_ms: settings.server_lack_of_ack_timeout_ms,
            flush_timeout_ms: settings.flush_timeout_ms,
            max_inflight_requests: settings.max_inflight_requests,
            max_ingest_payload_bytes: settings.max_ingest_payload_bytes,
            ack_callback: settings.ack_callback.as_ref().map(|cb| {
                Arc::new(AckCallbackWrapper::new(cb.clone_ref(py))) as Arc<dyn RustAckCallback>
            }),
            callback_max_wait_time_ms: settings
                .callback_max_wait_time_ms_set
                .then_some(settings.callback_max_wait_time_ms),
        })
    }

    pub(crate) fn format(&self) -> RecordFormat {
        self.format
    }

    fn apply<'a>(self, sdk: &'a RustSdk) -> PersistentStreamBuilder<'a> {
        let mut b = sdk
            .persistent_stream_builder()
            .table(self.table_properties.table_name.clone());
        b = match self.auth {
            PersistentAuth::OAuth {
                client_id,
                client_secret,
            } => b.oauth(client_id, client_secret),
            PersistentAuth::Headers(provider) => b.headers_provider(provider),
        };
        b = match self.table_properties.descriptor_proto.clone() {
            Some(descriptor) => b.compiled_proto(descriptor),
            None => b.json(),
        };
        if let Some(v) = self.recovery {
            b = b.recovery(v);
        }
        if let Some(v) = self.recovery_timeout_ms {
            b = b.recovery_timeout_ms(v);
        }
        if let Some(v) = self.recovery_backoff_ms {
            b = b.recovery_backoff_ms(v);
        }
        if let Some(v) = self.recovery_retries {
            b = b.recovery_retries(v);
        }
        if let Some(v) = self.server_lack_of_ack_timeout_ms {
            b = b.server_lack_of_ack_timeout_ms(v);
        }
        if let Some(v) = self.flush_timeout_ms {
            b = b.flush_timeout_ms(v);
        }
        if let Some(v) = self.max_inflight_requests {
            b = b.max_inflight_requests(v);
        }
        if let Some(v) = self.max_ingest_payload_bytes {
            b = b.max_ingest_payload_bytes(v);
        }
        if let Some(cb) = self.ack_callback {
            b = b.ack_callback(cb);
        }
        if let Some(v) = self.callback_max_wait_time_ms {
            b = b.callback_max_wait_time_ms(v);
        }
        b
    }

    /// Open a new persistent stream, or resume one when `resume_stream_id` is set.
    pub(crate) async fn open(
        self,
        sdk: &RustSdk,
        resume_stream_id: Option<String>,
    ) -> databricks_zerobus_ingest_sdk::ZerobusResult<PersistentStream> {
        let builder = self.apply(sdk);
        match resume_stream_id {
            Some(id) => builder.resume(id).await,
            None => builder.build().await,
        }
    }
}
