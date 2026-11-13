//! C callback bridge for external-IdP token federation.
//!
//! The Rust core's `FederatedTokenProvider` does the RFC 8693 exchange, caching,
//! and refresh. This module only adapts a C callback that yields the current
//! external IdP token (for example a Microsoft Entra ID JWT) into the core's
//! [`IdpTokenSupplier`].

use databricks_zerobus_ingest_sdk::{
    IdpTokenCallback, IdpTokenSupplier, ZerobusError, ZerobusResult,
};
use std::ffi::{c_void, CStr, CString};
use std::os::raw::c_char;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;

use crate::common::c_str_to_string;

/// The result of an IdP token callback.
///
/// On success set `token` and leave `error_message` null. On failure set
/// `error_message` (and `is_retryable`) and leave `token` null. Both strings
/// must be NUL-terminated and allocated with `zerobus_alloc_cstring` or
/// `malloc`; the FFI takes ownership and frees them. If both are set, the error
/// wins and both are freed.
#[repr(C)]
pub struct CIdpToken {
    /// The current external IdP token, or null on failure.
    pub token: *mut c_char,
    /// A description of the failure, or null on success.
    pub error_message: *mut c_char,
    /// Whether the failure is transient. A retryable failure is retried like a
    /// failed OAuth token request; a non-retryable one stops stream recovery.
    /// Ignored on success.
    pub is_retryable: bool,
}

/// Function pointer type for the IdP token callback.
///
/// Called only when the SDK must mint or refresh the exchanged Databricks token
/// (a cache miss or a proactive refresh), never per request. It runs on an
/// internal blocking thread, may run concurrently for streams on different
/// tables, and may perform a network call, so it must be thread-safe. It must
/// not unwind across the FFI boundary.
pub type IdpTokenSupplierCallback = extern "C" fn(user_data: *mut c_void) -> CIdpToken;

/// Owns a C IdP token callback and its `user_data`.
///
/// When `free_user_data` is set it fires from `Drop`, i.e. once the last
/// reference is gone. Every in-flight callback holds its own `Arc` clone, so
/// that happens only after any running callback has returned.
pub(crate) struct CallbackIdpTokenSupplier {
    callback: IdpTokenSupplierCallback,
    user_data: *mut c_void,
    free_user_data: Option<extern "C" fn(user_data: *mut c_void)>,
}

// Safety: the callback contract requires thread safety, and `user_data` is only
// passed back to that callback and to `free_user_data`.
unsafe impl Send for CallbackIdpTokenSupplier {}
unsafe impl Sync for CallbackIdpTokenSupplier {}

impl CallbackIdpTokenSupplier {
    /// Takes ownership of `user_data`. Construct this before any fallible work
    /// in an FFI entry point so that its `Drop` frees `user_data` on every path.
    pub(crate) fn new(
        callback: IdpTokenSupplierCallback,
        user_data: *mut c_void,
        free_user_data: Option<extern "C" fn(user_data: *mut c_void)>,
    ) -> Arc<Self> {
        Arc::new(Self {
            callback,
            user_data,
            free_user_data,
        })
    }

    /// Calls the C callback on the current thread and takes ownership of the
    /// strings it returns.
    pub(crate) fn get_token(&self) -> ZerobusResult<String> {
        let raw = (self.callback)(self.user_data);
        // Take both strings first so that each is freed exactly once on every path.
        let token = unsafe { take_c_string(raw.token) };
        let error_message = unsafe { take_c_string(raw.error_message) };

        if let Some(message) = error_message {
            return Err(if raw.is_retryable {
                supplier_retryable_error(message)
            } else {
                supplier_invalid_argument(message)
            });
        }
        match token {
            Some(token) if !token.is_empty() => Ok(token),
            Some(_) => Err(supplier_invalid_argument(
                "callback returned an empty token",
            )),
            None => Err(supplier_invalid_argument(
                "callback returned neither a token nor an error",
            )),
        }
    }
}

impl Drop for CallbackIdpTokenSupplier {
    fn drop(&mut self) {
        if let Some(free) = self.free_user_data {
            let user_data = self.user_data;
            // Contain panics: unwinding across the FFI boundary is UB.
            if catch_unwind(AssertUnwindSafe(|| free(user_data))).is_err() {
                tracing::error!(
                    "IdP token supplier free callback panicked; contained at FFI boundary"
                );
            }
        }
    }
}

/// Validated federation arguments for `StreamBuilder::federated_auth`.
pub(crate) struct FederatedAuth {
    pub(crate) idp_token_supplier: IdpTokenSupplier,
    /// The Databricks service principal for workload identity federation, or
    /// `None` for account-level federation.
    pub(crate) client_id: Option<String>,
}

/// Wraps `owner` as the core's [`IdpTokenSupplier`] and validates the C
/// arguments.
///
/// `cache_identity` is the wrapper's stable id for one logical federated
/// identity (for example a random id fixed when a Go `FederatedToken` is
/// built). It partitions the account-level token cache: streams created with
/// the same id share one exchanged token, and two ids never share a token. It
/// must never be derived from a pointer, which the allocator can reuse.
///
/// A null `client_id` selects account-level federation. On error `owner` is
/// dropped, which frees `user_data`.
///
/// # Safety
///
/// `client_id` and `cache_identity` must each be null or a valid NUL-terminated
/// string.
pub(crate) unsafe fn federated_auth(
    owner: Arc<CallbackIdpTokenSupplier>,
    client_id: *const c_char,
    cache_identity: *const c_char,
) -> Result<FederatedAuth, ZerobusError> {
    let cache_identity = c_str_to_string(cache_identity).map_err(|_| {
        ZerobusError::InvalidArgument("cache_identity must be a non-null UTF-8 string".to_string())
    })?;
    if cache_identity.trim().is_empty() {
        return Err(ZerobusError::InvalidArgument(
            "cache_identity must not be empty".to_string(),
        ));
    }

    let client_id = if client_id.is_null() {
        None
    } else {
        let client_id = c_str_to_string(client_id).map_err(|_| {
            ZerobusError::InvalidArgument("client_id must be a UTF-8 string".to_string())
        })?;
        if client_id.trim().is_empty() {
            return Err(ZerobusError::InvalidArgument(
                "client_id must not be empty; pass null for account-level federation".to_string(),
            ));
        }
        Some(client_id)
    };

    Ok(FederatedAuth {
        idp_token_supplier: idp_token_supplier(owner, cache_identity),
        client_id,
    })
}

/// Adapts the C callback to the core's async [`IdpTokenSupplier`].
///
/// The callback runs on a blocking thread, so a slow IdP round-trip does not
/// stall the runtime's worker threads.
fn idp_token_supplier(owner: Arc<CallbackIdpTokenSupplier>, identity: String) -> IdpTokenSupplier {
    let call: IdpTokenCallback = Arc::new(move || {
        let owner = Arc::clone(&owner);
        Box::pin(async move {
            tokio::task::spawn_blocking(move || owner.get_token())
                .await
                .map_err(|error| {
                    supplier_retryable_error(format!("callback task failed: {error}"))
                })?
        })
    });
    IdpTokenSupplier::with_identity(call, identity)
}

/// Copies and frees a C string returned by the callback. Null yields `None`.
///
/// # Safety
///
/// `ptr` must be null or a NUL-terminated string allocated with
/// `zerobus_alloc_cstring` or `malloc` that is not used after this call.
unsafe fn take_c_string(ptr: *mut c_char) -> Option<String> {
    if ptr.is_null() {
        return None;
    }
    let value = CStr::from_ptr(ptr).to_string_lossy().into_owned();
    // Same ownership convention as `zerobus_free_headers`.
    drop(CString::from_raw(ptr));
    Some(value)
}

fn supplier_invalid_argument(message: impl Into<String>) -> ZerobusError {
    ZerobusError::InvalidArgument(format!("IdP token supplier error: {}", message.into()))
}

fn supplier_retryable_error(message: impl Into<String>) -> ZerobusError {
    ZerobusError::TokenFetchError(format!("IdP token supplier error: {}", message.into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        zerobus_free_error_message, zerobus_get_default_config, zerobus_sdk_builder_build,
        zerobus_sdk_builder_endpoint, zerobus_sdk_builder_new,
        zerobus_sdk_builder_unity_catalog_url,
        zerobus_sdk_create_arrow_stream_with_federated_token,
        zerobus_sdk_create_stream_with_federated_token, zerobus_sdk_free, CResult, CZerobusSdk,
    };
    use std::collections::VecDeque;
    use std::ptr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    enum Reply {
        Token(&'static str),
        Error(&'static str, bool),
        Both,
        Neither,
    }

    /// Per-test callback state, passed as `user_data` so parallel tests never
    /// share counters.
    #[derive(Default)]
    struct State {
        replies: Mutex<VecDeque<Reply>>,
        calls: AtomicUsize,
        frees: Arc<AtomicUsize>,
    }

    fn c_string(value: &str) -> *mut c_char {
        CString::new(value).unwrap().into_raw()
    }

    extern "C" fn scripted(user_data: *mut c_void) -> CIdpToken {
        let state = unsafe { &*(user_data as *const State) };
        state.calls.fetch_add(1, Ordering::SeqCst);
        let reply = state.replies.lock().unwrap().pop_front();
        let (token, error_message, is_retryable) = match reply {
            Some(Reply::Token(token)) => (c_string(token), ptr::null_mut(), false),
            Some(Reply::Error(message, retryable)) => {
                (ptr::null_mut(), c_string(message), retryable)
            }
            Some(Reply::Both) => (c_string("tok"), c_string("both set"), true),
            Some(Reply::Neither) | None => (ptr::null_mut(), ptr::null_mut(), false),
        };
        CIdpToken {
            token,
            error_message,
            is_retryable,
        }
    }

    extern "C" fn free_state(user_data: *mut c_void) {
        let state = unsafe { Arc::from_raw(user_data as *const State) };
        state.frees.fetch_add(1, Ordering::SeqCst);
    }

    /// Hands a new `State` to the FFI. Returns the `user_data` pointer and the
    /// free counter, which outlives the state.
    fn hand_over(replies: Vec<Reply>) -> (*mut c_void, Arc<State>, Arc<AtomicUsize>) {
        let state = Arc::new(State {
            replies: Mutex::new(replies.into()),
            ..Default::default()
        });
        let frees = Arc::clone(&state.frees);
        let user_data = Arc::into_raw(Arc::clone(&state)) as *mut c_void;
        (user_data, state, frees)
    }

    fn sdk(unity_catalog_url: &str) -> *mut CZerobusSdk {
        let endpoint = CString::new("https://1234567890.zerobus.databricks.com").unwrap();
        let uc = CString::new(unity_catalog_url).unwrap();
        let builder = zerobus_sdk_builder_new();
        zerobus_sdk_builder_endpoint(builder, endpoint.as_ptr());
        zerobus_sdk_builder_unity_catalog_url(builder, uc.as_ptr());
        let mut result = result();
        let sdk = zerobus_sdk_builder_build(builder, &mut result);
        assert!(result.success && !sdk.is_null());
        sdk
    }

    fn result() -> CResult {
        CResult {
            success: true,
            error_message: ptr::null_mut(),
            is_retryable: false,
        }
    }

    fn take_message(result: &mut CResult) -> String {
        assert!(!result.success);
        let message = unsafe { CStr::from_ptr(result.error_message) }
            .to_string_lossy()
            .into_owned();
        zerobus_free_error_message(result.error_message);
        result.error_message = ptr::null_mut();
        message
    }

    #[test]
    fn callback_replies_map_to_tokens_and_errors() {
        let (user_data, state, frees) = hand_over(vec![
            Reply::Token("idp-token"),
            Reply::Error("transient", true),
            Reply::Error("permanent", false),
            Reply::Token(""),
            Reply::Neither,
            Reply::Both,
        ]);
        let owner = CallbackIdpTokenSupplier::new(scripted, user_data, Some(free_state));

        assert_eq!(owner.get_token().unwrap(), "idp-token");

        let err = owner.get_token().unwrap_err();
        assert!(matches!(err, ZerobusError::TokenFetchError(_)), "{err:?}");
        assert!(err.is_retryable());
        assert!(err
            .to_string()
            .contains("IdP token supplier error: transient"));

        let err = owner.get_token().unwrap_err();
        assert!(matches!(err, ZerobusError::InvalidArgument(_)), "{err:?}");
        assert!(!err.is_retryable());
        assert!(err.to_string().contains("permanent"));

        let err = owner.get_token().unwrap_err();
        assert!(err.to_string().contains("empty token"), "{err}");
        let err = owner.get_token().unwrap_err();
        assert!(
            err.to_string().contains("neither a token nor an error"),
            "{err}"
        );
        // The error wins when both are set (and both strings are freed).
        let err = owner.get_token().unwrap_err();
        assert!(
            err.is_retryable() && err.to_string().contains("both set"),
            "{err}"
        );

        assert_eq!(state.calls.load(Ordering::SeqCst), 6);
        assert_eq!(frees.load(Ordering::SeqCst), 0);
        drop(owner);
        assert_eq!(
            frees.load(Ordering::SeqCst),
            1,
            "free_user_data must run once on drop"
        );
    }

    #[test]
    fn federated_auth_validates_arguments() {
        let identity = CString::new("go:abc").unwrap();
        let blank = CString::new("  ").unwrap();
        let sp = CString::new("sp-client-id").unwrap();

        let cases: [(*const c_char, *const c_char, Option<&str>); 4] = [
            (ptr::null(), identity.as_ptr(), None),
            (
                sp.as_ptr(),
                ptr::null(),
                Some("cache_identity must be a non-null"),
            ),
            (
                sp.as_ptr(),
                blank.as_ptr(),
                Some("cache_identity must not be empty"),
            ),
            (
                blank.as_ptr(),
                identity.as_ptr(),
                Some("client_id must not be empty"),
            ),
        ];
        for (client_id, cache_identity, expected_error) in cases {
            let (user_data, _state, frees) = hand_over(vec![]);
            let owner = CallbackIdpTokenSupplier::new(scripted, user_data, Some(free_state));
            match (
                unsafe { federated_auth(owner, client_id, cache_identity) },
                expected_error,
            ) {
                (Ok(auth), None) => {
                    assert_eq!(auth.client_id, None);
                    drop(auth);
                }
                (Err(err), Some(expected)) => {
                    assert!(!err.is_retryable());
                    assert!(err.to_string().contains(expected), "{err}");
                }
                (other, _) => panic!("unexpected outcome: {:?}", other.map(|a| a.client_id)),
            }
            assert_eq!(
                frees.load(Ordering::SeqCst),
                1,
                "free_user_data must run once"
            );
        }

        let (user_data, _state, frees) = hand_over(vec![]);
        let owner = CallbackIdpTokenSupplier::new(scripted, user_data, Some(free_state));
        let auth = unsafe { federated_auth(owner, sp.as_ptr(), identity.as_ptr()) }.unwrap();
        assert_eq!(auth.client_id.as_deref(), Some("sp-client-id"));
        drop(auth);
        assert_eq!(frees.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn create_frees_user_data_once_on_failure() {
        let identity = CString::new("go:abc").unwrap();
        let table = CString::new("catalog.schema.table").unwrap();
        let sdk = sdk("http://127.0.0.1:9");
        let options = zerobus_get_default_config();

        // A null SDK, then a missing cache identity, on both entry points.
        for (sdk_ptr, cache_identity) in [(ptr::null_mut(), identity.as_ptr()), (sdk, ptr::null())]
        {
            let (user_data, state, frees) = hand_over(vec![]);
            let mut res = result();
            let stream = zerobus_sdk_create_stream_with_federated_token(
                sdk_ptr,
                table.as_ptr(),
                ptr::null(),
                0,
                scripted,
                user_data,
                Some(free_state),
                ptr::null(),
                cache_identity,
                &options,
                &mut res,
            );
            assert!(stream.is_null());
            take_message(&mut res);
            assert_eq!(frees.load(Ordering::SeqCst), 1);
            assert_eq!(state.calls.load(Ordering::SeqCst), 0);

            let (user_data, _state, frees) = hand_over(vec![]);
            let mut res = result();
            let stream = zerobus_sdk_create_arrow_stream_with_federated_token(
                sdk_ptr,
                table.as_ptr(),
                ptr::null(),
                0,
                scripted,
                user_data,
                Some(free_state),
                ptr::null(),
                cache_identity,
                ptr::null(),
                &mut res,
            );
            assert!(stream.is_null());
            take_message(&mut res);
            assert_eq!(frees.load(Ordering::SeqCst), 1);
        }
        zerobus_sdk_free(sdk);
    }

    #[test]
    fn create_stream_surfaces_supplier_error() {
        let identity = CString::new("go:abc").unwrap();
        let sp = CString::new("sp-client-id").unwrap();
        let table = CString::new("catalog.schema.table").unwrap();
        let sdk = sdk("http://127.0.0.1:9");
        let mut options = zerobus_get_default_config();
        options.recovery = false;
        options.record_type = 2; // JSON

        for client_id in [ptr::null(), sp.as_ptr()] {
            let (user_data, state, frees) = hand_over(vec![Reply::Error("idp unavailable", false)]);
            let mut res = result();
            let stream = zerobus_sdk_create_stream_with_federated_token(
                sdk,
                table.as_ptr(),
                ptr::null(),
                0,
                scripted,
                user_data,
                Some(free_state),
                client_id,
                identity.as_ptr(),
                &options,
                &mut res,
            );
            assert!(stream.is_null());
            assert!(!res.is_retryable);
            let message = take_message(&mut res);
            assert!(message.contains("idp unavailable"), "{message}");
            assert_eq!(state.calls.load(Ordering::SeqCst), 1);
            // This failure happens inside the stream's supervisor task, which drops
            // its reference to the supplier as it shuts down, possibly just after
            // create returns. Wait for that single free.
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            while frees.load(Ordering::SeqCst) == 0 && std::time::Instant::now() < deadline {
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
            assert_eq!(
                frees.load(Ordering::SeqCst),
                1,
                "free_user_data must run exactly once"
            );
        }
        zerobus_sdk_free(sdk);
    }
}
