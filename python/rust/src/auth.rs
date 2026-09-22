use async_trait::async_trait;
use pyo3::exceptions::PyNotImplementedError;
use pyo3::prelude::*;
use pyo3_async_runtimes::TaskLocals;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use databricks_zerobus_ingest_sdk::{
    HeadersProvider as RustHeadersProvider, IdpTokenCallback, IdpTokenSupplier,
    ZerobusError as RustError, ZerobusResult as RustResult,
};

use crate::common::intern_header_name;

/// Builds a **retryable** error for a transient failure while obtaining the IdP
/// token — the callback raised, or its awaitable rejected (e.g. the external IdP
/// was briefly unavailable). Classified as `TokenFetchError` so it is retryable,
/// exactly like an OAuth client-credentials mint failure, and the stream's
/// setup/recovery path can retry rather than failing permanently.
fn idp_token_fetch_error(context: &str, err: PyErr) -> RustError {
    RustError::TokenFetchError(format!("{}: {}", context, err))
}

/// Builds a **non-retryable** error for caller misuse of the IdP token callback
/// (it returned a non-string, or an async callback was used without a running
/// event loop). Retrying cannot fix these, so they surface as `InvalidArgument`.
fn idp_supplier_misuse(msg: impl Into<String>) -> RustError {
    RustError::InvalidArgument(msg.into())
}

/// Builds an error for the case where the native supplier is invoked but its
/// weakly-held Python callback has already been collected. The only strong,
/// GC-visible reference lives on the Python `ZerobusStream`, so this is reachable
/// only once that stream is unreachable (being torn down) — e.g. a detached
/// `ingest_record_nowait` task racing GC. Classified retryable so a stray
/// occurrence degrades gracefully rather than hard-failing.
fn idp_callback_dropped_error() -> RustError {
    RustError::TokenFetchError(
        "federated IdP token callback was dropped before mint (stream released)".to_string(),
    )
}

/// Base class for custom authentication headers (subclassable from Python)
///
/// The Rust SDK handles OAuth authentication internally by default.
/// Only implement a custom HeadersProvider if you need non-standard authentication.
///
/// Example:
///     class CustomHeadersProvider(HeadersProvider):
///         def get_headers(self):
///             return [
///                 ("authorization", "Bearer my-custom-token"),
///                 ("x-custom-header", "value"),
///             ]
#[pyclass(subclass, skip_from_py_object)]
#[derive(Clone)]
pub struct HeadersProvider {}

#[pymethods]
impl HeadersProvider {
    #[new]
    #[pyo3(signature = (*_args, **_kwargs))]
    fn new(
        _args: &Bound<'_, pyo3::types::PyTuple>,
        _kwargs: Option<&Bound<'_, pyo3::types::PyDict>>,
    ) -> Self {
        // Accept and ignore any arguments to allow Python subclasses to pass their own arguments
        Self {}
    }

    /// Returns headers for gRPC metadata
    ///
    /// Subclasses must implement this method.
    ///
    /// Returns:
    ///     List of (header_name, header_value) tuples
    fn get_headers(&self, _py: Python) -> PyResult<Py<PyAny>> {
        Err(PyNotImplementedError::new_err(
            "Subclasses must implement get_headers()",
        ))
    }

    /// Optional hook to drop cached auth state after the server rejects a token.
    ///
    /// The default is a no-op. Override it in a subclass that caches credentials
    /// so a rejected token is re-fetched on the next `get_headers()` call.
    fn invalidate(&self) {}
}

// =============================================================================
// HEADERS PROVIDER WRAPPER
// =============================================================================

/// Wrapper that bridges Python HeadersProvider to Rust SDK's HeadersProvider trait
pub struct HeadersProviderWrapper {
    py_obj: Py<PyAny>,
}

impl HeadersProviderWrapper {
    pub fn new(py_obj: Py<PyAny>) -> Self {
        Self { py_obj }
    }
}

#[async_trait]
impl RustHeadersProvider for HeadersProviderWrapper {
    async fn get_headers(&self) -> RustResult<HashMap<&'static str, String>> {
        // Call into Python to get headers
        let headers_vec: Vec<(String, String)> = Python::attach(|py| {
            let method = self.py_obj.getattr(py, "get_headers")?;
            let result = method.call0(py)?;
            let headers: Vec<(String, String)> = result.extract(py)?;
            Ok::<_, PyErr>(headers)
        })
        .map_err(|e: PyErr| {
            let msg = format!("Python HeadersProvider error: {}", e);
            RustError::CreateStreamError(tonic::Status::new(tonic::Code::InvalidArgument, msg))
        })?;

        // Convert Vec<(String, String)> to HashMap<&'static str, String>.
        // The Rust SDK's HeadersProvider trait requires &'static str keys.
        // We intern each distinct header name in a process-wide table so the
        // leak is bounded to the set of names ever used (typically <10), not
        // proportional to the number of get_headers() invocations.
        let mut map = HashMap::with_capacity(headers_vec.len());
        for (key, value) in headers_vec {
            map.insert(intern_header_name(key), value);
        }
        Ok(map)
    }

    async fn invalidate(&self) {
        // Forward to the Python provider's optional `invalidate()` hook so a
        // custom provider can drop cached auth state when the server rejects a
        // token. Providers that do not define one (the common case) are a
        // no-op. Best-effort: any error is swallowed because `invalidate` is a
        // cache-drop hint with no return channel.
        let _ = Python::attach(|py| -> PyResult<()> {
            let obj = self.py_obj.bind(py);
            if let Ok(method) = obj.getattr("invalidate") {
                method.call0()?;
            }
            Ok(())
        });
    }
}

// =============================================================================
// FEDERATED IDP TOKEN SUPPLIER BRIDGE
// =============================================================================

/// The outcome of invoking the Python IdP-token callback: a token returned
/// directly (sync callback), an awaitable to drive to completion (async
/// callback), or a classified failure (retryable vs non-retryable already
/// decided at the point it occurred).
enum TokenOutcome {
    Ready(String),
    Awaitable(Pin<Box<dyn Future<Output = PyResult<Py<PyAny>>> + Send>>),
    Failed(RustError),
}

/// A short-lived, opaque handle to one constructed [`IdpTokenSupplier`].
///
/// The Python `FederatedToken` rebuilds this per `create_stream`, so it binds to
/// the SDK creating the stream: `allow_async` matches sync vs async, and an async
/// callback captures the current event loop's task-locals. It must not be memoized
/// on the `FederatedToken`, or a cached handle would freeze the first loop's locals
/// and `allow_async` and break a later `asyncio.run()` reusing that token.
///
/// Cross-stream cache sharing is preserved instead by `cache_identity`: the Rust
/// SDK keys the account-level cache on it, so reusing one `FederatedToken` shares
/// its Databricks token and a distinct one isolates, independent of the `Arc`.
#[pyclass]
pub struct IdpSupplier {
    pub(crate) supplier: IdpTokenSupplier,
}

#[pymethods]
impl IdpSupplier {
    /// `allow_async` must be `true` only for the async SDK. The sync SDK passes
    /// `false` so that an `async def` supplier is rejected as misuse rather than
    /// scheduled on a loop the sync `block_on` will never drive (which would
    /// hang whenever the caller — e.g. a notebook — already has a running loop).
    ///
    /// `cache_identity` is the owning `FederatedToken`'s stable per-instance id;
    /// it partitions the account-level token cache so streams sharing one token
    /// share its cached exchanged token even though the handle (and its `Arc`) is
    /// rebuilt for each stream.
    ///
    /// The supplier holds the callback only *weakly* (see [`CallbackRef`]); the
    /// strong, GC-visible reference is passed separately to `create_stream_federated`
    /// and lives on the `ZerobusStream`, so a self-referential owner/stream/callback
    /// cycle stays collectable and this handle never pins the callback.
    #[new]
    fn new(py_callable: Py<PyAny>, allow_async: bool, cache_identity: String) -> Self {
        Self {
            supplier: make_idp_token_supplier(py_callable, allow_async, cache_identity),
        }
    }
}

/// How the native supplier closure holds the Python IdP callback. Weak by
/// default, so the supplier never keeps the callback (or any object cycle running
/// through it) alive; the Python `ZerobusStream` holds the sole strong,
/// GC-visible reference for the stream's lifetime, so the collector can reclaim an
/// `owner -> stream -> callback -> owner` cycle. A callback that cannot be weakly
/// referenced (e.g. a builtin function) falls back to a strong reference, which
/// only preserves today's keep-alive behavior for that exotic case.
enum CallbackRef {
    /// A `weakref.ref` object; calling it yields the callback or `None` if dropped.
    Weak(Py<PyAny>),
    Strong(Py<PyAny>),
}

impl CallbackRef {
    /// Builds a weak reference to `callable`, falling back to strong when the
    /// object is not weakly referenceable (`weakref.ref` raises `TypeError`).
    fn new(py: Python<'_>, callable: &Py<PyAny>) -> Self {
        match py
            .import("weakref")
            .and_then(|m| m.call_method1("ref", (callable.bind(py),)))
        {
            Ok(wref) => CallbackRef::Weak(wref.unbind()),
            Err(_) => CallbackRef::Strong(callable.clone_ref(py)),
        }
    }

    /// Resolves the live callback under the GIL, or `None` if a weak reference has
    /// been collected (the owning stream is gone).
    fn resolve<'py>(&self, py: Python<'py>) -> Option<Bound<'py, PyAny>> {
        match self {
            CallbackRef::Strong(cb) => Some(cb.bind(py).clone()),
            CallbackRef::Weak(wref) => match wref.bind(py).call0() {
                Ok(obj) if !obj.is_none() => Some(obj),
                _ => None,
            },
        }
    }
}

/// Bridges a Python IdP-token callback to the Rust SDK's [`IdpTokenSupplier`].
///
/// The callback is invoked only when a fresh Databricks token must be minted
/// (a cache miss or refresh), and must return the current external IdP token as
/// a string. Both sync callbacks (return the string directly) and async
/// callbacks (return an awaitable) are supported. Async callbacks are driven
/// via the running event loop, so they require the async SDK.
pub fn make_idp_token_supplier(
    py_callable: Py<PyAny>,
    allow_async: bool,
    cache_identity: String,
) -> IdpTokenSupplier {
    // Async (awaitable) callbacks are driven later on a Rust worker thread with
    // no running event loop, so capture the current loop's task-locals here, up
    // front on the Python thread. Only for the async SDK (`allow_async`); the
    // sync SDK leaves it `None` so an async callback is rejected as misuse below.
    // A sync caller can still be inside a running loop (a notebook, `asyncio.run`),
    // and capturing it would schedule the awaitable on a loop `block_on` never
    // drives, an indefinite hang.
    let task_locals: Option<TaskLocals> = if allow_async {
        Python::attach(|py| pyo3_async_runtimes::tokio::get_current_locals(py).ok())
    } else {
        None
    };

    // Hold the callback weakly (see `CallbackRef`): the `ZerobusStream` owns the
    // sole strong, GC-visible reference for the stream's lifetime, so a
    // self-referential owner -> stream -> callback cycle stays collectable. The
    // strong `py_callable` is released when this function returns.
    let callback_ref = Python::attach(|py| CallbackRef::new(py, &py_callable));

    let call: IdpTokenCallback = Arc::new(move || {
        // Invoke the callback under the GIL. If it returned an awaitable,
        // convert it to a Rust future here (GIL held) using the captured
        // event-loop locals, then await it below without holding the GIL. Each
        // failure is classified where it happens: invoking/awaiting the callback
        // is a retryable token-fetch failure, while a non-string return or an
        // async callback without an event loop is non-retryable misuse.
        let outcome = Python::attach(|py| -> TokenOutcome {
            // Upgrade the weak reference. `None` means the callback was collected,
            // which can only happen once the owning stream is unreachable.
            let callable = match callback_ref.resolve(py) {
                Some(callable) => callable,
                None => return TokenOutcome::Failed(idp_callback_dropped_error()),
            };
            let result = match callable.call0() {
                Ok(result) => result,
                Err(e) => {
                    // A non-callable supplier (e.g. a string) can never succeed, so
                    // it is non-retryable misuse; a callable that raised is a
                    // transient token-fetch failure, retryable like an OAuth mint.
                    return if callable.is_callable() {
                        TokenOutcome::Failed(idp_token_fetch_error(
                            "federated IdP token callback failed",
                            e,
                        ))
                    } else {
                        TokenOutcome::Failed(idp_supplier_misuse(format!(
                            "federated idp_token_supplier is not callable: {}",
                            e
                        )))
                    };
                }
            };
            match result.hasattr("__await__") {
                Ok(true) => match task_locals.as_ref() {
                    Some(locals) => {
                        match pyo3_async_runtimes::into_future_with_locals(locals, result) {
                            Ok(fut) => TokenOutcome::Awaitable(Box::pin(fut)),
                            Err(e) => TokenOutcome::Failed(idp_token_fetch_error(
                                "federated IdP token callback failed",
                                e,
                            )),
                        }
                    }
                    None => {
                        // Reject the async callback as misuse (the sync SDK cannot
                        // drive it). Close the coroutine first (best-effort): we
                        // called it but will never await it, and an un-closed
                        // coroutine makes CPython warn "coroutine ... was never
                        // awaited".
                        let _ = result.call_method0("close");
                        TokenOutcome::Failed(idp_supplier_misuse(
                            "async idp_token_supplier requires the async SDK (a running event loop); \
                             use a synchronous callback with the sync SDK",
                        ))
                    }
                },
                Ok(false) => match result.extract::<String>() {
                    Ok(token) => TokenOutcome::Ready(token),
                    Err(e) => TokenOutcome::Failed(idp_supplier_misuse(format!(
                        "federated IdP token callback returned a non-string: {}",
                        e
                    ))),
                },
                Err(e) => TokenOutcome::Failed(idp_token_fetch_error(
                    "federated IdP token callback failed",
                    e,
                )),
            }
        });

        Box::pin(async move {
            match outcome {
                TokenOutcome::Ready(token) => Ok(token),
                TokenOutcome::Awaitable(fut) => {
                    let awaited = fut.await.map_err(|e| {
                        idp_token_fetch_error("federated IdP token callback failed", e)
                    })?;
                    Python::attach(|py| awaited.bind(py).extract::<String>()).map_err(|e| {
                        idp_supplier_misuse(format!(
                            "federated IdP token callback returned a non-string: {}",
                            e
                        ))
                    })
                }
                TokenOutcome::Failed(err) => Err(err),
            }
        })
    });

    IdpTokenSupplier::with_identity(call, cache_identity)
}
