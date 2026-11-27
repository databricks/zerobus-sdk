//! Shared plumbing for multiplexed streams (Beta), used by the sync and async wrappers.
//!
//! `create_multiplexed_stream*` opens several sub-streams to one table and distributes records
//! round-robin across them. Ingest calls return a message ID: an opaque `int` that packs the
//! sub-stream index and that sub-stream's offset. Message IDs of sub-streams 32-63 are
//! negative, so no return value is reserved as an error sentinel; failures are always raised.

use std::sync::Arc;

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};

use databricks_zerobus_ingest_sdk::{
    AckCallback as RustAckCallback, IdpTokenSupplier, MessageId,
    MultiplexedStream as RustMultiplexedStream, ZerobusSdk as RustSdk,
};

use crate::auth::{HeadersProviderWrapper, IdpCallbackHolder, IdpSupplier};
use crate::common::{
    apply_grpc_options, map_error, AckCallback, AckCallbackWrapper, RecordFormat,
    StreamConfigurationOptions, TableProperties,
};
use crate::sync_wrapper::apply_table_and_format;

/// Options for a multiplexed stream (Beta): the `StreamConfigurationOptions` fields, where
/// `ack_callback` is shared by every sub-stream and its `offset` argument receives the
/// message ID returned by the ingest call. Sub-streams may call it concurrently.
/// `max_inflight_records` is a budget for the whole stream: each sub-stream gets
/// `max_inflight_records // stream_count`, so it must be at least `stream_count`; the core's
/// error calls it `max_inflight_requests`. Passed to `create_stream`, it acts as a plain
/// `StreamConfigurationOptions`, and the callback receives ordinary offsets.
#[pyclass(extends = StreamConfigurationOptions, subclass)]
pub struct MultiplexedStreamOptions {}

#[pymethods]
impl MultiplexedStreamOptions {
    /// Accept and ignore any arguments, as the base class does; the inherited `__init__`
    /// applies the options.
    #[new]
    #[pyo3(signature = (*_args, **_kwargs))]
    fn new(
        _args: &Bound<'_, PyTuple>,
        _kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyClassInitializer<Self> {
        PyClassInitializer::from(StreamConfigurationOptions::default()).add_subclass(Self {})
    }

    fn __repr__(slf: PyRef<'_, Self>) -> String {
        let base = slf.as_super().__repr__();
        match base.strip_prefix("StreamConfigurationOptions") {
            Some(fields) => format!("MultiplexedStreamOptions{fields}"),
            None => base,
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

/// How a multiplexed stream authenticates; one variant per `create_multiplexed_stream*`.
pub(crate) enum MuxAuth {
    OAuth {
        client_id: String,
        client_secret: String,
    },
    Headers(Arc<HeadersProviderWrapper>),
    PersonalAccessToken(String),
    /// `databricks_client_id` is `Some` for workload identity federation and `None` for
    /// account-level federation.
    Federated {
        supplier: IdpTokenSupplier,
        databricks_client_id: Option<String>,
    },
}

/// The arguments of one multiplexed open, read from Python while attached, so the open can
/// run off the GIL.
pub(crate) struct MuxConfig {
    auth: MuxAuth,
    table_properties: TableProperties,
    stream_count: usize,
    /// The options without `ack_callback`: the ordinary callback is never set on a mux.
    options: StreamConfigurationOptions,
    ack_callback: Option<Py<AckCallback>>,
    format: RecordFormat,
}

impl MuxConfig {
    /// Validate the options and split off the multiplexed callback.
    ///
    /// A plain `StreamConfigurationOptions` with `ack_callback` set is rejected: its callback
    /// would receive message IDs, not the offsets its type promises.
    pub(crate) fn new(
        auth: MuxAuth,
        table_properties: TableProperties,
        stream_count: usize,
        options: Option<&Bound<'_, StreamConfigurationOptions>>,
    ) -> PyResult<Self> {
        let (mut options, is_multiplexed) = match options {
            Some(opts) => (
                opts.borrow().clone(),
                opts.is_instance_of::<MultiplexedStreamOptions>(),
            ),
            None => (StreamConfigurationOptions::default(), true),
        };
        if !is_multiplexed && options.ack_callback.is_some() {
            return Err(PyValueError::new_err(
                "ack_callback on StreamConfigurationOptions is not supported for multiplexed \
                 streams; pass it in MultiplexedStreamOptions",
            ));
        }
        options.validate()?;
        let ack_callback = options.ack_callback.take();
        let format = table_properties.resolve_format(options.record_type)?;
        Ok(Self {
            auth,
            table_properties,
            stream_count,
            options,
            ack_callback,
            format,
        })
    }

    pub(crate) fn format(&self) -> RecordFormat {
        self.format
    }

    /// Open every sub-stream. Opening is atomic: if any sub-stream fails, the others are
    /// closed and the error is returned. The core validates the stream count and the
    /// in-flight budget.
    pub(crate) async fn open(self, sdk: &RustSdk) -> PyResult<RustMultiplexedStream> {
        let builder = sdk.stream_builder();
        let builder = match self.auth {
            MuxAuth::OAuth {
                client_id,
                client_secret,
            } => builder.oauth(client_id, client_secret),
            MuxAuth::Headers(provider) => builder.headers_provider(provider),
            MuxAuth::PersonalAccessToken(token) => builder.personal_access_token(token),
            MuxAuth::Federated {
                supplier,
                databricks_client_id,
            } => builder.federated_auth(supplier, databricks_client_id),
        };
        let builder = apply_table_and_format(builder, &self.table_properties);
        let mut builder = apply_grpc_options(builder, &self.options)?;
        if let Some(cb) = self.ack_callback {
            builder = builder.multiplexed_ack_callback(Arc::new(MultiplexedAckCallbackWrapper(
                AckCallbackWrapper::new(cb),
            )));
        }
        builder
            .multiplexed(self.stream_count)
            .build()
            .await
            .map_err(map_error)
    }
}

/// The strong reference to a federated supplier's callback holder, for the returned stream
/// to keep (the native supplier holds only a weak one), as `create_stream_federated` does.
pub(crate) fn idp_holder(
    py: Python<'_>,
    idp_supplier: &IdpSupplier,
) -> PyResult<Py<IdpCallbackHolder>> {
    idp_supplier
        .holder
        .as_ref()
        .map(|holder| holder.clone_ref(py))
        .ok_or_else(|| {
            PyRuntimeError::new_err("federated IdP supplier handle has already been cleared")
        })
}
