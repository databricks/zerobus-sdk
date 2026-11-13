//! JNI bridge for the Java `IdpTokenSupplier` interface (external-IdP federation).
//!
//! The Rust core's `FederatedTokenProvider` does the RFC 8693 exchange, caching,
//! refresh, and `invalidate()`. This module only adapts the Java callback that
//! yields the current external IdP token into the core's [`IdpTokenSupplier`].

use crate::headers_provider::take_java_exception;
use crate::runtime::get_jvm;
use databricks_zerobus_ingest_sdk::{
    IdpTokenCallback, IdpTokenSupplier, ZerobusError, ZerobusResult,
};
use jni::errors::Error as JniError;
use jni::objects::{GlobalRef, JString};
use jni::JNIEnv;
use std::sync::Arc;

struct LocalFrameError(ZerobusError);

impl From<JniError> for LocalFrameError {
    fn from(error: JniError) -> Self {
        Self(supplier_retryable_error(format!(
            "JNI local frame failed: {error}"
        )))
    }
}

impl From<ZerobusError> for LocalFrameError {
    fn from(error: ZerobusError) -> Self {
        Self(error)
    }
}

/// Wraps a Java `IdpTokenSupplier` as the core's [`IdpTokenSupplier`].
///
/// `cache_identity` is the stable id of the Java `FederatedToken` (a random
/// uuid fixed at construction). It partitions the account-level token cache, so
/// streams sharing one `FederatedToken` share its exchanged token, and two
/// distinct `FederatedToken`s never serve each other's token. It is never
/// derived from the JNI reference, whose address can be reused.
///
/// The Java callback runs on a blocking thread, so a slow IdP round-trip does
/// not stall the runtime's worker threads.
pub(crate) fn java_idp_token_supplier(
    supplier_ref: GlobalRef,
    cache_identity: String,
) -> IdpTokenSupplier {
    let call: IdpTokenCallback = Arc::new(move || {
        let supplier_ref = supplier_ref.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || get_token_blocking(supplier_ref))
                .await
                .map_err(|error| {
                    supplier_retryable_error(format!("callback task failed: {error}"))
                })?
        })
    });
    IdpTokenSupplier::with_identity(call, cache_identity)
}

/// Calls `IdpTokenSupplier.getToken()` on the current (blocking) thread.
///
/// An exception is a retryable token-fetch failure, like an OAuth mint failure,
/// unless it is a `NonRetriableException` or a `java.lang.Error`. A `null` or
/// empty return is caller misuse and is not retryable.
pub(crate) fn get_token_blocking(supplier_ref: GlobalRef) -> ZerobusResult<String> {
    let jvm = get_jvm();
    let mut env = jvm
        .attach_current_thread_as_daemon()
        .map_err(|error| supplier_retryable_error(format!("failed to attach to JVM: {error}")))?;

    env.with_local_frame(8, |env| -> Result<_, LocalFrameError> {
        let token = env
            .call_method(
                supplier_ref.as_obj(),
                "getToken",
                "()Ljava/lang/String;",
                &[],
            )
            .and_then(|value| value.l())
            .map_err(|error| java_call_error(env, error))?;

        if token.is_null() {
            return Err(supplier_invalid_argument("getToken returned null").into());
        }

        let token = JString::from(token);
        let token: String = env
            .get_string(&token)
            .map_err(|error| java_call_error(env, error))?
            .into();
        if token.is_empty() {
            // Never send an empty subject_token to the token endpoint.
            return Err(supplier_invalid_argument("getToken returned an empty token").into());
        }
        Ok(token)
    })
    .map_err(|error| error.0)
}

fn java_call_error(env: &mut JNIEnv<'_>, error: JniError) -> ZerobusError {
    if matches!(error, JniError::JavaException) {
        let exception = take_java_exception(env);
        let detail = exception
            .as_ref()
            .and_then(|exception| exception.message.as_deref())
            .unwrap_or("Java exception was thrown");
        let message = format!("getToken failed: {detail}");
        if exception.is_some_and(|exception| exception.non_retryable) {
            supplier_invalid_argument(message)
        } else {
            supplier_retryable_error(message)
        }
    } else {
        supplier_retryable_error(format!("getToken failed: {error}"))
    }
}

fn supplier_invalid_argument(message: impl Into<String>) -> ZerobusError {
    ZerobusError::InvalidArgument(format!("Java IdpTokenSupplier error: {}", message.into()))
}

fn supplier_retryable_error(message: impl Into<String>) -> ZerobusError {
    ZerobusError::TokenFetchError(format!("Java IdpTokenSupplier error: {}", message.into()))
}
