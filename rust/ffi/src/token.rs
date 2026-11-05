//! Standalone OAuth token FFI surface.

use crate::common::*;
use databricks_zerobus_ingest_sdk::{DefaultTokenFactory, ZerobusError, ZerobusResult};
use std::ffi::CString;
use std::os::raw::c_char;
use std::ptr;
use std::time::UNIX_EPOCH;

/// A Zerobus-scoped OAuth access token returned by `zerobus_get_token`.
/// Release it with `zerobus_free_token`.
#[repr(C)]
pub struct CZerobusToken {
    /// NUL-terminated OAuth access token, sent as `Bearer <access_token>`.
    pub access_token: *mut c_char,
    /// Expiry as Unix epoch milliseconds, or 0 if the server reported no
    /// lifetime. Measured from when the request was sent.
    pub expires_at_unix_ms: u64,
}

/// Fetch a Zerobus-scoped OAuth access token with the client credentials flow.
///
/// Use it to authenticate any client that takes an OAuth token callback, such
/// as a Kafka client's SASL/OAUTHBEARER token provider. Every call mints a new
/// token; nothing is cached.
///
/// `workspace_url` is the Databricks workspace URL, `table_name` the full
/// `catalog.schema.table` name the token is scoped to, and `workspace_id` the
/// Databricks workspace ID. Blocks until the token endpoint responds, for at
/// most 30 seconds.
///
/// On success returns true and fills `out_token`; release it with
/// `zerobus_free_token`. On failure returns false, leaves `out_token` zeroed,
/// and fills `result` (free its message with `zerobus_free_error_message`).
#[no_mangle]
pub extern "C" fn zerobus_get_token(
    workspace_url: *const c_char,
    table_name: *const c_char,
    client_id: *const c_char,
    client_secret: *const c_char,
    workspace_id: *const c_char,
    out_token: *mut CZerobusToken,
    result: *mut CResult,
) -> bool {
    ffi_guard(result, false, move || {
        if out_token.is_null() {
            write_error_result(result, "out_token is null", false);
            return false;
        }
        unsafe {
            *out_token = CZerobusToken::empty();
        }

        match unsafe {
            get_token(
                workspace_url,
                table_name,
                client_id,
                client_secret,
                workspace_id,
            )
        } {
            Ok(token) => {
                unsafe {
                    *out_token = token;
                }
                write_success_result(result);
                true
            }
            Err(err) => {
                if !result.is_null() {
                    unsafe {
                        *result = CResult::error(err);
                    }
                }
                false
            }
        }
    })
}

unsafe fn get_token(
    workspace_url: *const c_char,
    table_name: *const c_char,
    client_id: *const c_char,
    client_secret: *const c_char,
    workspace_id: *const c_char,
) -> ZerobusResult<CZerobusToken> {
    let arg = |value: *const c_char, name: &str| {
        c_str_to_string(value).map_err(|e| ZerobusError::InvalidArgument(format!("{name}: {e}")))
    };
    let workspace_url = arg(workspace_url, "workspace_url")?;
    let table_name = arg(table_name, "table_name")?;
    let client_id = arg(client_id, "client_id")?;
    let client_secret = arg(client_secret, "client_secret")?;
    let workspace_id = arg(workspace_id, "workspace_id")?;

    let token = RUNTIME.block_on(DefaultTokenFactory::get_token_with_expiry(
        &workspace_url,
        &table_name,
        &client_id,
        &client_secret,
        &workspace_id,
    ))?;

    let access_token = CString::new(token.access_token).map_err(|_| {
        ZerobusError::InvalidUCTokenError("access token contains a NUL byte".to_string())
    })?;
    let expires_at_unix_ms = token
        .expires_at
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0);
    Ok(CZerobusToken {
        access_token: access_token.into_raw(),
        expires_at_unix_ms,
    })
}

impl CZerobusToken {
    fn empty() -> Self {
        Self {
            access_token: ptr::null_mut(),
            expires_at_unix_ms: 0,
        }
    }
}

/// Free a token filled by `zerobus_get_token` and reset it to empty. Safe to
/// call on a null pointer or an already-freed (zeroed) token.
#[no_mangle]
pub extern "C" fn zerobus_free_token(token: *mut CZerobusToken) {
    ffi_guard(ptr::null_mut(), (), move || {
        if token.is_null() {
            return;
        }
        unsafe {
            let token = &mut *token;
            if !token.access_token.is_null() {
                drop(CString::from_raw(token.access_token));
            }
            *token = CZerobusToken::empty();
        }
    })
}

#[cfg(test)]
mod token_tests {
    use super::*;
    use crate::zerobus_free_error_message;
    use std::ffi::CStr;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::time::{Duration, SystemTime};

    /// Serves one HTTP response on loopback and returns the base URL.
    fn serve_once(status: &str, body: &'static str) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let response = format!(
            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            let mut request = Vec::new();
            let mut buf = [0u8; 4096];
            while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                let n = socket.read(&mut buf).unwrap();
                if n == 0 {
                    return;
                }
                request.extend_from_slice(&buf[..n]);
            }
            socket.write_all(response.as_bytes()).unwrap();
        });
        url
    }

    fn get_token(url: &str, table: &str) -> (bool, CZerobusToken, CResult) {
        let c = |s: &str| CString::new(s).unwrap();
        let (url, table) = (c(url), c(table));
        let (id, secret, ws) = (c("id"), c("secret"), c("ws"));
        let mut token = CZerobusToken {
            access_token: ptr::null_mut(),
            expires_at_unix_ms: 42,
        };
        let mut result = CResult::success();
        let ok = zerobus_get_token(
            url.as_ptr(),
            table.as_ptr(),
            id.as_ptr(),
            secret.as_ptr(),
            ws.as_ptr(),
            &mut token,
            &mut result,
        );
        (ok, token, result)
    }

    fn error_message(result: &CResult) -> String {
        unsafe { CStr::from_ptr(result.error_message) }
            .to_string_lossy()
            .into_owned()
    }

    #[test]
    fn returns_token_and_expiry() {
        let url = serve_once(
            "200 OK",
            r#"{"access_token":"tok","token_type":"Bearer","expires_in":3600}"#,
        );
        let before = SystemTime::now();
        let (ok, mut token, result) = get_token(&url, "c.s.t");
        let after = SystemTime::now();

        assert!(ok && result.success);
        assert_eq!(
            unsafe { CStr::from_ptr(token.access_token) }.to_str(),
            Ok("tok")
        );
        let ms = |t: SystemTime| {
            (t + Duration::from_secs(3600))
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64
        };
        assert!(token.expires_at_unix_ms >= ms(before) && token.expires_at_unix_ms <= ms(after));

        zerobus_free_token(&mut token);
        assert!(token.access_token.is_null());
        zerobus_free_token(&mut token);
    }

    #[test]
    fn missing_expires_in_yields_zero_expiry() {
        let url = serve_once("200 OK", r#"{"access_token":"tok"}"#);
        let (ok, mut token, _) = get_token(&url, "c.s.t");

        assert!(ok);
        assert_eq!(token.expires_at_unix_ms, 0);
        zerobus_free_token(&mut token);
    }

    #[test]
    fn rejected_request_is_not_retryable() {
        let url = serve_once("401 Unauthorized", r#"{"error":"invalid_client"}"#);
        let (ok, token, result) = get_token(&url, "c.s.t");

        assert!(!ok && !result.success && !result.is_retryable);
        assert!(error_message(&result).contains("401"));
        assert!(token.access_token.is_null());
        assert_eq!(token.expires_at_unix_ms, 0);
        zerobus_free_error_message(result.error_message);
    }

    #[test]
    fn invalid_table_name_is_rejected() {
        let (ok, token, result) = get_token("http://127.0.0.1:1", "not-a-table");

        assert!(!ok && !result.is_retryable);
        assert!(error_message(&result).contains("table name"));
        assert!(token.access_token.is_null());
        zerobus_free_error_message(result.error_message);
    }

    #[test]
    fn null_argument_is_rejected() {
        let mut token = CZerobusToken {
            access_token: ptr::null_mut(),
            expires_at_unix_ms: 0,
        };
        let mut result = CResult::success();
        let ok = zerobus_get_token(
            ptr::null(),
            ptr::null(),
            ptr::null(),
            ptr::null(),
            ptr::null(),
            &mut token,
            &mut result,
        );

        assert!(!ok && !result.is_retryable);
        assert!(error_message(&result).contains("workspace_url"));
        zerobus_free_error_message(result.error_message);

        let mut result = CResult::success();
        let ok = zerobus_get_token(
            ptr::null(),
            ptr::null(),
            ptr::null(),
            ptr::null(),
            ptr::null(),
            ptr::null_mut(),
            &mut result,
        );
        assert!(!ok);
        assert!(error_message(&result).contains("out_token"));
        zerobus_free_error_message(result.error_message);
    }
}
