use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use prost::Message;
use pyo3::exceptions::{PyException, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};

use databricks_zerobus_ingest_sdk::{
    AckCallback as RustAckCallback, EncodedRecord, OffsetId, StreamBuilder, ZerobusError,
};

/// User-agent prefix emitted by this wrapper SDK. Combined with the wrapper
/// crate version via `env!("CARGO_PKG_VERSION")` at the call site.
pub(crate) const SDK_IDENTIFIER_PREFIX: &str = "zerobus-sdk-py";

/// Type of records to ingest into the stream. Defaults to `UNSPECIFIED` (0).
#[pyclass(from_py_object)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RecordType {
    #[pyo3(get)]
    pub value: i32,
}

#[pymethods]
impl RecordType {
    #[classattr]
    #[allow(non_snake_case)]
    fn UNSPECIFIED() -> Self {
        RecordType { value: 0 }
    }

    #[classattr]
    #[allow(non_snake_case)]
    fn PROTO() -> Self {
        RecordType { value: 1 }
    }

    #[classattr]
    #[allow(non_snake_case)]
    fn JSON() -> Self {
        RecordType { value: 2 }
    }

    #[classattr]
    #[allow(non_snake_case)]
    fn AVRO() -> Self {
        RecordType { value: 4 }
    }

    fn __int__(&self) -> i32 {
        self.value
    }

    fn __eq__(&self, other: &Self) -> bool {
        self.value == other.value
    }

    fn __repr__(&self) -> String {
        match self.value {
            0 => "RecordType.UNSPECIFIED".to_string(),
            1 => "RecordType.PROTO".to_string(),
            2 => "RecordType.JSON".to_string(),
            4 => "RecordType.AVRO".to_string(),
            _ => format!("RecordType({})", self.value),
        }
    }
}

/// Table properties for the stream
#[pyclass(from_py_object)]
#[derive(Debug, Clone)]
pub struct TableProperties {
    #[pyo3(get)]
    pub table_name: String,

    // Internal field - stores the parsed DescriptorProto
    pub(crate) descriptor_proto: Option<prost_types::DescriptorProto>,

    #[cfg(feature = "avro")]
    #[pyo3(get)]
    pub avro_schema: Option<String>,
}

#[pymethods]
impl TableProperties {
    #[new]
    #[pyo3(signature = (table_name, descriptor_proto=None, avro_schema=None))]
    fn new(
        table_name: String,
        descriptor_proto: Option<&Bound<'_, PyAny>>,
        avro_schema: Option<String>,
    ) -> PyResult<Self> {
        #[cfg(not(feature = "avro"))]
        if avro_schema.is_some() {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "avro_schema parameter requires the 'avro' feature (install: pip install databricks-zerobus-ingest-sdk[avro])",
            ));
        }
        let rust_descriptor = if let Some(obj) = descriptor_proto {
            if obj.is_none() {
                None
            } else {
                // If a `google.protobuf.descriptor.Descriptor` was passed in,
                // remember its simple `name` so we can pick the correct
                // message out of the FileDescriptorProto below (instead of
                // blindly taking the first one — that mis-routes schemas in
                // proto files containing multiple messages).
                let descriptor_message_name: Option<String> = obj
                    .getattr("name")
                    .ok()
                    .and_then(|n| n.extract::<String>().ok());

                // Try to extract as bytes first
                let descriptor_bytes = if let Ok(bytes) = obj.extract::<Vec<u8>>() {
                    bytes
                } else if obj.hasattr("file")? {
                    let file = obj.getattr("file")?;
                    if file.hasattr("serialized_pb")? {
                        let serialized_pb = file.getattr("serialized_pb")?;
                        serialized_pb.extract::<Vec<u8>>().map_err(|_| {
                            PyValueError::new_err("Descriptor.file.serialized_pb must be bytes")
                        })?
                    } else {
                        return Err(PyValueError::new_err(
                            "Descriptor.file does not have serialized_pb attribute",
                        ));
                    }
                } else {
                    return Err(PyValueError::new_err(
                        "Descriptor must be bytes or a Descriptor object with file.serialized_pb",
                    ));
                };

                let file_descriptor_proto = prost_types::FileDescriptorProto::decode(
                    &descriptor_bytes[..],
                )
                .map_err(|e| {
                    PyValueError::new_err(format!("Invalid FileDescriptorProto bytes: {}", e))
                })?;

                if file_descriptor_proto.message_type.is_empty() {
                    return Err(PyValueError::new_err(
                        "No message types found in the provided file descriptor",
                    ));
                }

                // Prefer match-by-name when we got the descriptor object;
                // fall back to the first message for raw-bytes input.
                let descriptor = match descriptor_message_name {
                    Some(name) => file_descriptor_proto
                        .message_type
                        .into_iter()
                        .find(|m| m.name.as_deref() == Some(name.as_str()))
                        .ok_or_else(|| {
                            PyValueError::new_err(format!(
                                "Message '{}' not found in the provided file descriptor",
                                name
                            ))
                        })?,
                    None => file_descriptor_proto
                        .message_type
                        .into_iter()
                        .next()
                        .unwrap(),
                };

                Some(descriptor)
            }
        } else {
            None
        };

        Ok(Self {
            table_name,
            descriptor_proto: rust_descriptor,
            #[cfg(feature = "avro")]
            avro_schema,
        })
    }

    fn __repr__(&self) -> String {
        #[cfg(feature = "avro")]
        {
            format!(
                "TableProperties(table_name='{}', descriptor_proto={}, avro_schema={})",
                self.table_name,
                if self.descriptor_proto.is_some() {
                    "Some(...)"
                } else {
                    "None"
                },
                if self.avro_schema.is_some() {
                    "Some(...)"
                } else {
                    "None"
                }
            )
        }
        #[cfg(not(feature = "avro"))]
        {
            format!(
                "TableProperties(table_name='{}', descriptor_proto={})",
                self.table_name,
                if self.descriptor_proto.is_some() {
                    "Some(...)"
                } else {
                    "None"
                }
            )
        }
    }

    /// Record format implied by the schema: "proto", "json", or "avro".
    #[getter]
    #[pyo3(name = "record_format")]
    fn record_format_str(&self) -> &'static str {
        self.inferred_format().as_str()
    }
}

/// The record encoding of a stream, fixed at creation.
#[derive(Clone, Copy, Debug)]
pub(crate) enum RecordFormat {
    Proto,
    Json,
    #[cfg(feature = "avro")]
    Avro,
}

impl RecordFormat {
    fn as_str(self) -> &'static str {
        match self {
            RecordFormat::Proto => "proto",
            RecordFormat::Json => "json",
            #[cfg(feature = "avro")]
            RecordFormat::Avro => "avro",
        }
    }

    /// The `RecordType` value a client sets to request this format.
    fn record_type_value(self) -> i32 {
        match self {
            RecordFormat::Proto => 1,
            RecordFormat::Json => 2,
            #[cfg(feature = "avro")]
            RecordFormat::Avro => 4,
        }
    }
}

impl TableProperties {
    /// The record format implied by the supplied schema.
    fn inferred_format(&self) -> RecordFormat {
        if self.descriptor_proto.is_some() {
            return RecordFormat::Proto;
        }
        #[cfg(feature = "avro")]
        if self.avro_schema.is_some() {
            return RecordFormat::Avro;
        }
        RecordFormat::Json
    }

    /// The stream's record format. `UNSPECIFIED` takes the schema's implied
    /// format; any other `record_type` must match it.
    pub(crate) fn resolve_format(&self, record_type: RecordType) -> PyResult<RecordFormat> {
        let inferred = self.inferred_format();
        if record_type.value == 0 || record_type.value == inferred.record_type_value() {
            Ok(inferred)
        } else {
            Err(PyValueError::new_err(format!(
                "record_type {} conflicts with the schema, which implies {}",
                record_type.__repr__(),
                inferred.as_str(),
            )))
        }
    }
}

/// Base class for logical ingest submission acknowledgment callbacks.
///
/// A batch ingest is one logical submission and produces one callback, not one
/// callback per record in the batch.
#[pyclass(subclass, skip_from_py_object)]
#[derive(Clone)]
pub struct AckCallback {
    _phantom: std::marker::PhantomData<()>,
}

#[pymethods]
impl AckCallback {
    #[new]
    #[pyo3(signature = (*_args, **_kwargs))]
    fn new(_args: &Bound<'_, PyTuple>, _kwargs: Option<&Bound<'_, PyDict>>) -> Self {
        // Accept and ignore any arguments to allow subclasses to use __init__
        Self {
            _phantom: std::marker::PhantomData,
        }
    }

    /// Called when a logical ingest submission is acknowledged by the server.
    fn on_ack(&self, py: Python, offset: i64) -> PyResult<()> {
        let _ = (py, offset);
        Ok(())
    }

    /// Called when a logical ingest submission encounters an error during ingestion.
    fn on_error(&self, py: Python, offset: i64, error_message: &str) -> PyResult<()> {
        let _ = (py, offset, error_message);
        Ok(())
    }
}

/// Bridges Python `AckCallback` subclasses to Rust's `AckCallback` trait.
pub(crate) struct AckCallbackWrapper {
    py_callback: Py<AckCallback>,
}

impl AckCallbackWrapper {
    pub fn new(py_callback: Py<AckCallback>) -> Self {
        Self { py_callback }
    }
}

impl RustAckCallback for AckCallbackWrapper {
    fn on_ack(&self, offset_id: OffsetId) {
        Python::attach(|py| {
            if let Err(e) = self.py_callback.call_method1(py, "on_ack", (offset_id,)) {
                eprintln!("Error invoking Python AckCallback.on_ack: {:?}", e);
            }
        });
    }

    fn on_error(&self, offset_id: OffsetId, error_message: &str) {
        Python::attach(|py| {
            if let Err(e) =
                self.py_callback
                    .call_method1(py, "on_error", (offset_id, error_message))
            {
                eprintln!("Error invoking Python AckCallback.on_error: {:?}", e);
            }
        });
    }
}

/// Configuration options for the stream
#[pyclass(from_py_object)]
pub struct StreamConfigurationOptions {
    #[pyo3(get, set)]
    pub max_inflight_records: i32,

    #[pyo3(get, set)]
    pub recovery: bool,

    #[pyo3(get, set)]
    pub recovery_timeout_ms: i32,

    #[pyo3(get, set)]
    pub recovery_backoff_ms: i32,

    #[pyo3(get, set)]
    pub recovery_retries: i32,

    #[pyo3(get, set)]
    pub server_lack_of_ack_timeout_ms: i32,

    #[pyo3(get, set)]
    pub flush_timeout_ms: i32,

    #[pyo3(get, set)]
    pub record_type: RecordType,

    #[pyo3(get, set)]
    pub stream_paused_max_wait_time_ms: Option<i32>,

    #[pyo3(get, set)]
    pub callback_max_wait_time_ms: Option<i32>,

    #[pyo3(get, set)]
    pub ack_callback: Option<Py<AckCallback>>,
}

// `Py<T>` is not unconditionally `Clone` in PyO3 0.29 — cloning a `Py` handle
// needs the interpreter attached to bump the refcount. Implement `Clone` by
// hand via `clone_ref` so the refcount is incremented under an attached
// interpreter, rather than enabling PyO3's `py-clone` feature (which panics at
// runtime when the interpreter is detached — exactly the case in the async
// stream-builder paths that clone these options).
impl Clone for StreamConfigurationOptions {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            max_inflight_records: self.max_inflight_records,
            recovery: self.recovery,
            recovery_timeout_ms: self.recovery_timeout_ms,
            recovery_backoff_ms: self.recovery_backoff_ms,
            recovery_retries: self.recovery_retries,
            server_lack_of_ack_timeout_ms: self.server_lack_of_ack_timeout_ms,
            flush_timeout_ms: self.flush_timeout_ms,
            record_type: self.record_type,
            stream_paused_max_wait_time_ms: self.stream_paused_max_wait_time_ms,
            callback_max_wait_time_ms: self.callback_max_wait_time_ms,
            ack_callback: self.ack_callback.as_ref().map(|cb| cb.clone_ref(py)),
        })
    }
}

impl StreamConfigurationOptions {
    /// Validate that all numeric fields are non-negative before casting to unsigned types.
    pub fn validate(&self) -> PyResult<()> {
        if self.max_inflight_records < 0 {
            return Err(PyValueError::new_err(
                "max_inflight_records must be non-negative",
            ));
        }
        if self.recovery_timeout_ms < 0 {
            return Err(PyValueError::new_err(
                "recovery_timeout_ms must be non-negative",
            ));
        }
        if self.recovery_backoff_ms < 0 {
            return Err(PyValueError::new_err(
                "recovery_backoff_ms must be non-negative",
            ));
        }
        if self.recovery_retries < 0 {
            return Err(PyValueError::new_err(
                "recovery_retries must be non-negative",
            ));
        }
        if self.server_lack_of_ack_timeout_ms < 0 {
            return Err(PyValueError::new_err(
                "server_lack_of_ack_timeout_ms must be non-negative",
            ));
        }
        if self.flush_timeout_ms < 0 {
            return Err(PyValueError::new_err(
                "flush_timeout_ms must be non-negative",
            ));
        }
        if let Some(v) = self.stream_paused_max_wait_time_ms {
            if v < 0 {
                return Err(PyValueError::new_err(
                    "stream_paused_max_wait_time_ms must be non-negative",
                ));
            }
        }
        if let Some(v) = self.callback_max_wait_time_ms {
            if v < 0 {
                return Err(PyValueError::new_err(
                    "callback_max_wait_time_ms must be non-negative",
                ));
            }
        }
        Ok(())
    }
}

impl Default for StreamConfigurationOptions {
    fn default() -> Self {
        Self {
            max_inflight_records: 1_000_000,
            recovery: true,
            recovery_timeout_ms: 15_000,
            recovery_backoff_ms: 2_000,
            recovery_retries: 4,
            server_lack_of_ack_timeout_ms: 60_000,
            flush_timeout_ms: 300_000,
            record_type: RecordType::default(), // UNSPECIFIED
            stream_paused_max_wait_time_ms: None,
            callback_max_wait_time_ms: Some(5_000),
            ack_callback: None,
        }
    }
}

#[pymethods]
impl StreamConfigurationOptions {
    #[new]
    #[pyo3(signature = (**kwargs))]
    fn new(kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<Self> {
        let mut options = Self::default();

        if let Some(kwargs) = kwargs {
            for (key, value) in kwargs.iter() {
                let key_str: String = key.extract()?;
                let key_str = key_str.as_str();
                match key_str {
                    "max_inflight_records" => options.max_inflight_records = value.extract()?,
                    "recovery" => options.recovery = value.extract()?,
                    "recovery_timeout_ms" => options.recovery_timeout_ms = value.extract()?,
                    "recovery_backoff_ms" => options.recovery_backoff_ms = value.extract()?,
                    "recovery_retries" => options.recovery_retries = value.extract()?,
                    "server_lack_of_ack_timeout_ms" => {
                        options.server_lack_of_ack_timeout_ms = value.extract()?
                    }
                    "flush_timeout_ms" => options.flush_timeout_ms = value.extract()?,
                    "record_type" => options.record_type = value.extract()?,
                    "stream_paused_max_wait_time_ms" => {
                        options.stream_paused_max_wait_time_ms = if value.is_none() {
                            None
                        } else {
                            Some(value.extract()?)
                        };
                    }
                    "callback_max_wait_time_ms" => {
                        options.callback_max_wait_time_ms = if value.is_none() {
                            None
                        } else {
                            Some(value.extract()?)
                        };
                    }
                    "ack_callback" => {
                        options.ack_callback = if value.is_none() {
                            None
                        } else {
                            Some(value.extract()?)
                        };
                    }
                    _ => {
                        return Err(PyValueError::new_err(format!(
                            "Unknown configuration option: {}",
                            key_str
                        )));
                    }
                }
            }
        }

        Ok(options)
    }

    fn __repr__(&self) -> String {
        format!(
            "StreamConfigurationOptions(max_inflight_records={}, recovery={}, recovery_timeout_ms={}, \
             recovery_backoff_ms={}, recovery_retries={}, server_lack_of_ack_timeout_ms={}, \
             flush_timeout_ms={}, record_type={:?}, ack_callback={})",
            self.max_inflight_records,
            self.recovery,
            self.recovery_timeout_ms,
            self.recovery_backoff_ms,
            self.recovery_retries,
            self.server_lack_of_ack_timeout_ms,
            self.flush_timeout_ms,
            self.record_type,
            if self.ack_callback.is_some() { "Some(...)" } else { "None" }
        )
    }
}

// =============================================================================
// SHARED HELPERS (used by sync_wrapper and async_wrapper)
// =============================================================================

/// Coerce a Python record payload into an `EncodedRecord` according to the
/// stream's `RecordFormat`. Each format accepts its pre-encoded bytes form or
/// its native Python object; any other type is a `TypeError`.
pub(crate) fn extract_record_payload(
    payload: &Bound<'_, PyAny>,
    format: RecordFormat,
) -> PyResult<EncodedRecord> {
    match format {
        RecordFormat::Proto => Ok(EncodedRecord::Proto(extract_proto_bytes(payload)?)),
        RecordFormat::Json => Ok(EncodedRecord::Json(extract_json_string(payload)?)),
        #[cfg(feature = "avro")]
        RecordFormat::Avro => Ok(EncodedRecord::Avro(extract_avro_bytes(payload)?)),
    }
}

/// Proto: a protobuf message (serialized via `SerializeToString`) or bytes.
fn extract_proto_bytes(payload: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    if let Ok(bytes) = payload.cast::<PyBytes>() {
        Ok(bytes.as_bytes().to_vec())
    } else if payload.hasattr("SerializeToString")? {
        let serialized: Vec<u8> = payload.getattr("SerializeToString")?.call0()?.extract()?;
        Ok(serialized)
    } else if let Ok(bytes) = payload.extract::<Vec<u8>>() {
        Ok(bytes)
    } else {
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "proto stream expects a protobuf message or bytes",
        ))
    }
}

/// JSON: a JSON string, or any JSON-serializable object encoded via `json.dumps`.
fn extract_json_string(payload: &Bound<'_, PyAny>) -> PyResult<String> {
    if let Ok(json_str) = payload.extract::<String>() {
        Ok(json_str)
    } else {
        let py = payload.py();
        let json_dumps = py.import("json")?.getattr("dumps")?;
        json_dumps.call1((payload,))?.extract()
    }
}

/// Avro: bytes pre-encoded by the Python layer (via fastavro against the schema).
#[cfg(feature = "avro")]
fn extract_avro_bytes(payload: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    if let Ok(bytes) = payload.cast::<PyBytes>() {
        Ok(bytes.as_bytes().to_vec())
    } else if let Ok(bytes) = payload.extract::<Vec<u8>>() {
        Ok(bytes)
    } else {
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "Avro payload must be bytes (pre-encoded by the Python layer)",
        ))
    }
}

pub(crate) fn extract_record_payloads(
    payloads: &Bound<'_, PyAny>,
    format: RecordFormat,
) -> PyResult<Vec<EncodedRecord>> {
    let mut out = Vec::new();

    if let Ok(list) = payloads.cast::<PyList>() {
        out.reserve(list.len());
        for item in list.iter() {
            out.push(extract_record_payload(&item, format)?);
        }
    } else {
        // Any other sequence/iterable of records; each item is coerced by format.
        for item in payloads.try_iter()? {
            out.push(extract_record_payload(&item?, format)?);
        }
    }

    Ok(out)
}

pub(crate) fn encoded_record_to_pybytes(py: Python, record: EncodedRecord) -> Py<PyAny> {
    match record {
        EncodedRecord::Proto(bytes) => PyBytes::new(py, &bytes).into_any().unbind(),
        EncodedRecord::Json(json_str) => PyBytes::new(py, json_str.as_bytes()).into_any().unbind(),
        #[cfg(feature = "avro")]
        EncodedRecord::Avro(bytes) => PyBytes::new(py, &bytes).into_any().unbind(),
    }
}

/// Apply a Python `StreamConfigurationOptions` to a `StreamBuilder` via the
/// builder's setters. Record format is applied separately by `apply_table_and_format`.
pub(crate) fn apply_grpc_options<'a>(
    builder: StreamBuilder<'a>,
    opts: &StreamConfigurationOptions,
) -> PyResult<StreamBuilder<'a>> {
    opts.validate()?;

    let mut b = builder
        .max_inflight_requests(opts.max_inflight_records as usize)
        .recovery(opts.recovery)
        .recovery_timeout_ms(opts.recovery_timeout_ms as u64)
        .recovery_backoff_ms(opts.recovery_backoff_ms as u64)
        .recovery_retries(opts.recovery_retries as u32)
        .server_lack_of_ack_timeout_ms(opts.server_lack_of_ack_timeout_ms as u64)
        .flush_timeout_ms(opts.flush_timeout_ms as u64)
        .stream_paused_max_wait_time_ms(opts.stream_paused_max_wait_time_ms.map(|v| v as u64))
        .callback_max_wait_time_ms(opts.callback_max_wait_time_ms.map(|v| v as u64));

    if let Some(cb) = opts
        .ack_callback
        .as_ref()
        .map(|cb| Python::attach(|py| cb.clone_ref(py)))
    {
        b = b.ack_callback(Arc::new(AckCallbackWrapper::new(cb)) as Arc<dyn RustAckCallback>);
    }

    Ok(b)
}

// =============================================================================
// HEADER KEY INTERNER (shared by HeadersProviderWrapper in auth.rs)
// =============================================================================

/// Intern a header name string into a `&'static str`.
///
/// The Rust SDK's `HeadersProvider` trait requires header keys to have a
/// `'static` lifetime. To avoid leaking memory on every `get_headers()` call,
/// we maintain a process-wide interner that leaks each distinct header name
/// exactly once. Header values are not interned — they change per request
/// (e.g. OAuth tokens) and are passed through as `String`.
pub(crate) fn intern_header_name(name: String) -> &'static str {
    static INTERNER: OnceLock<Mutex<HashMap<String, &'static str>>> = OnceLock::new();
    let interner = INTERNER.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = interner.lock().expect("header interner mutex poisoned");
    if let Some(&existing) = guard.get(&name) {
        return existing;
    }
    let leaked: &'static str = Box::leak(name.clone().into_boxed_str());
    guard.insert(name, leaked);
    leaked
}

// Custom exception types
pyo3::create_exception!(
    _zerobus_core,
    ZerobusException,
    PyException,
    "Base class for all exceptions in the Zerobus SDK"
);
pyo3::create_exception!(
    _zerobus_core,
    NonRetriableException,
    ZerobusException,
    "Indicates a non-retriable error has occurred"
);

/// Map Rust SDK errors to Python exceptions using `ZerobusError::is_retryable()`.
pub fn map_error(err: ZerobusError) -> PyErr {
    let msg = err.to_string();
    if err.is_retryable() {
        ZerobusException::new_err(msg)
    } else {
        NonRetriableException::new_err(msg)
    }
}
