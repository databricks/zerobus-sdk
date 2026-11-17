//! Shared plumbing for `stream_builder()` streams, used by both the sync and async wrappers.
//!
//! The Python `StreamBuilder` collects settings in pure Python and hands them to the native
//! layer as one `StreamSettings`. Every setting is optional so the Rust core keeps ownership
//! of the defaults: an unset value is never forwarded to the builder. One configuration opens
//! either an ordinary stream or, after `multiplexed(n)`, a multiplexed stream (Beta).
//!
//! Multiplexed ingest calls return a message ID: an opaque `int` that packs the sub-stream
//! index and that sub-stream's offset. Message IDs of sub-streams 32-63 are negative, so no
//! return value is reserved as an error sentinel; failures are always raised.

use std::sync::Arc;

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;

use databricks_zerobus_ingest_sdk::{
    AckCallback as RustAckCallback, HeadersProvider as RustHeadersProvider, MessageId,
    MultiplexedStream, StreamBuilder, ZerobusResult, ZerobusSdk as RustSdk, ZerobusStream,
};

use crate::auth::HeadersProviderWrapper;
use tokio::sync::RwLock;

use crate::common::{map_error, AckCallback, AckCallbackWrapper, RecordFormat, TableProperties};

/// Settings collected by the Python stream builder.
///
/// Not part of the public Python API: users configure streams through `StreamBuilder`.
#[pyclass(skip_from_py_object)]
pub struct StreamSettings {
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
    /// `callback_max_wait_time_ms` and `stream_paused_max_wait_time_ms` accept `None`
    /// (wait without limit, or the full server-specified time), so "unset" needs its own flag.
    callback_max_wait_time_ms_set: bool,
    callback_max_wait_time_ms: Option<u64>,
    stream_paused_max_wait_time_ms_set: bool,
    stream_paused_max_wait_time_ms: Option<u64>,
}

#[pymethods]
impl StreamSettings {
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
        stream_paused_max_wait_time_ms_set = false,
        stream_paused_max_wait_time_ms = None,
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
        stream_paused_max_wait_time_ms_set: bool,
        stream_paused_max_wait_time_ms: Option<u64>,
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
            stream_paused_max_wait_time_ms_set,
            stream_paused_max_wait_time_ms,
        }
    }
}

/// Bridges a Python `AckCallback` to the mux's `AckCallback<MessageId>`; the Python
/// callback's `offset` argument receives `MessageId::raw()`.
struct MultiplexedAckCallbackWrapper(AckCallbackWrapper);

impl RustAckCallback<MessageId> for MultiplexedAckCallbackWrapper {
    fn on_ack(&self, message_id: MessageId) {
        self.0.on_ack(message_id.raw());
    }

    fn on_error(&self, message_id: MessageId, error_message: &str) {
        self.0.on_error(message_id.raw(), error_message);
    }
}

enum StreamAuth {
    OAuth {
        client_id: String,
        client_secret: String,
    },
    Headers(Arc<dyn RustHeadersProvider>),
}

/// `StreamSettings` resolved into owned Rust values, so the async work does not need the GIL
/// to read them.
pub(crate) struct StreamConfig {
    auth: StreamAuth,
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
    stream_paused_max_wait_time_ms: Option<Option<u64>>,
}

impl StreamConfig {
    /// Validate the settings and the table properties and resolve them for use off the GIL.
    pub(crate) fn resolve(
        py: Python<'_>,
        settings: &StreamSettings,
        table_properties: &TableProperties,
    ) -> PyResult<Self> {
        let format = table_properties.resolve_format(Default::default())?;

        let auth = match (
            &settings.client_id,
            &settings.client_secret,
            &settings.headers_provider,
        ) {
            (Some(client_id), Some(client_secret), None) => StreamAuth::OAuth {
                client_id: client_id.clone(),
                client_secret: client_secret.clone(),
            },
            (None, None, Some(provider)) => StreamAuth::Headers(Arc::new(
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
            stream_paused_max_wait_time_ms: settings
                .stream_paused_max_wait_time_ms_set
                .then_some(settings.stream_paused_max_wait_time_ms),
        })
    }

    pub(crate) fn format(&self) -> RecordFormat {
        self.format
    }

    fn apply(self, sdk: &RustSdk) -> StreamBuilder<'_> {
        let mut b = sdk
            .stream_builder()
            .table(self.table_properties.table_name.clone());
        b = match self.auth {
            StreamAuth::OAuth {
                client_id,
                client_secret,
            } => b.oauth(client_id, client_secret),
            StreamAuth::Headers(provider) => b.headers_provider(provider),
        };
        b = match (
            self.table_properties.descriptor_proto.clone(),
            self.table_properties.avro_schema.clone(),
        ) {
            (Some(descriptor), _) => b.compiled_proto(descriptor),
            (None, Some(schema)) => b.avro(schema),
            (None, None) => b.json(),
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
        if let Some(v) = self.stream_paused_max_wait_time_ms {
            b = b.stream_paused_max_wait_time_ms(v);
        }
        b
    }

    /// Open an ordinary stream.
    pub(crate) async fn open(self, sdk: &RustSdk) -> ZerobusResult<ZerobusStream> {
        self.apply(sdk).build().await
    }

    /// Open every sub-stream of a multiplexed stream (Beta).
    ///
    /// `ack_callback` is the multiplexed callback, which receives message IDs. The core
    /// validates the stream count, the in-flight budget, and a misplaced ordinary callback;
    /// the Python builder checks the first and last before this call, so the core's check
    /// is the only one for the budget.
    pub(crate) async fn open_multiplexed(
        self,
        sdk: &RustSdk,
        stream_count: usize,
        ack_callback: Option<Py<AckCallback>>,
    ) -> ZerobusResult<MultiplexedStream> {
        self.apply_multiplexed_callback(sdk, ack_callback)
            .multiplexed(stream_count)
            .build()
            .await
    }

    /// Run the Rust core's configuration checks without opening anything.
    ///
    /// With `stream_count`, this is `MultiplexedStreamBuilder::validate()`; without it,
    /// `StreamBuilder::validate()`, which rejects a multiplexed `ack_callback`. Like the
    /// core, it neither resolves authentication nor parses the Avro writer schema.
    pub(crate) fn validate(
        self,
        sdk: &RustSdk,
        stream_count: Option<usize>,
        ack_callback: Option<Py<AckCallback>>,
    ) -> ZerobusResult<()> {
        let b = self.apply_multiplexed_callback(sdk, ack_callback);
        match stream_count {
            Some(n) => b.multiplexed(n).validate(),
            None => b.validate(),
        }
    }

    fn apply_multiplexed_callback(
        self,
        sdk: &RustSdk,
        ack_callback: Option<Py<AckCallback>>,
    ) -> StreamBuilder<'_> {
        let b = self.apply(sdk);
        match ack_callback {
            Some(cb) => b.multiplexed_ack_callback(Arc::new(MultiplexedAckCallbackWrapper(
                AckCallbackWrapper::new(cb),
            ))),
            None => b,
        }
    }
}

/// `validate_stream` for the sync and async SDKs: resolve the settings, then run the Rust
/// core's checks. Synchronous in both, as Rust's `validate()` is, so it never needs a
/// runtime. Nothing takes the SDK's write lock, so `try_read` does not fail in practice.
pub(crate) fn validate_stream(
    py: Python<'_>,
    sdk: &RwLock<RustSdk>,
    table_properties: &TableProperties,
    settings: &StreamSettings,
    stream_count: Option<usize>,
    ack_callback: Option<Py<AckCallback>>,
) -> PyResult<()> {
    let config = StreamConfig::resolve(py, settings, table_properties)?;
    let sdk = sdk
        .try_read()
        .map_err(|_| PyRuntimeError::new_err("the SDK is in use; try again"))?;
    config
        .validate(&sdk, stream_count, ack_callback)
        .map_err(map_error)
}
