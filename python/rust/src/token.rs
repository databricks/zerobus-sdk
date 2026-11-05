use std::time::UNIX_EPOCH;

use databricks_zerobus_ingest_sdk::{DefaultTokenFactory, OAuthToken};
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::{future_into_py, get_runtime};

use crate::common::map_error;

/// `(access_token, expires_at_unix_ms)`. The Python layer builds the public
/// `OAuthToken`; abi3 has no native `datetime` constructor.
type RawToken = (String, Option<u64>);

fn to_raw(token: OAuthToken) -> RawToken {
    let expires_at_ms = token
        .expires_at
        .and_then(|at| at.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as u64);
    (token.access_token, expires_at_ms)
}

/// Blocking token fetch backing `zerobus.get_token`.
#[pyfunction]
pub fn get_token(
    py: Python<'_>,
    workspace_url: String,
    table_name: String,
    client_id: String,
    client_secret: String,
    workspace_id: String,
) -> PyResult<RawToken> {
    py.detach(|| {
        get_runtime().block_on(DefaultTokenFactory::get_token_with_expiry(
            &workspace_url,
            &table_name,
            &client_id,
            &client_secret,
            &workspace_id,
        ))
    })
    .map(to_raw)
    .map_err(map_error)
}

/// Async token fetch backing `zerobus.sdk.aio.get_token`.
#[pyfunction(name = "get_token")]
pub fn get_token_async(
    py: Python<'_>,
    workspace_url: String,
    table_name: String,
    client_id: String,
    client_secret: String,
    workspace_id: String,
) -> PyResult<Bound<'_, PyAny>> {
    future_into_py(py, async move {
        DefaultTokenFactory::get_token_with_expiry(
            &workspace_url,
            &table_name,
            &client_id,
            &client_secret,
            &workspace_id,
        )
        .await
        .map(to_raw)
        .map_err(map_error)
    })
}
