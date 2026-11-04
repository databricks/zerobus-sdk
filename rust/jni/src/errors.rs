//! Error handling and conversion between Rust and Java exceptions.
//!
//! This module provides utilities for converting Zerobus SDK errors to
//! appropriate Java exceptions.

use crate::class_cache::{as_jclass, get_class_cache};
use crate::runtime::{BlockingCallInCallback, BLOCKING_CALL_IN_CALLBACK_MESSAGE};
use databricks_zerobus_ingest_sdk::ZerobusError;
use jni::objects::{GlobalRef, JObject, JString, JThrowable, JValue};
use jni::JNIEnv;
use std::any::Any;
use std::panic::{catch_unwind, AssertUnwindSafe};

/// Runs the body of a JNI entry point, converting a panic into a Java exception.
///
/// A panic must not reach the `extern "system"` boundary: Rust aborts the
/// process there, taking the whole JVM down. Each entry point is a thin wrapper
/// that passes its arguments to the original function through this guard. On
/// panic it throws a Java exception (unless the body already left one pending)
/// and returns `R::default()`, which Java ignores while an exception is pending.
pub(crate) fn guard<'local, R: Default>(
    env: JNIEnv<'local>,
    body: impl FnOnce(JNIEnv<'local>) -> R,
) -> R {
    guard_or(env, R::default(), body)
}

/// Like [`guard`], but returns `on_panic`: pass the entry point's usual error
/// value (such as `-1` for offsets) for the case where throwing itself fails.
pub(crate) fn guard_or<'local, R>(
    mut env: JNIEnv<'local>,
    on_panic: R,
    body: impl FnOnce(JNIEnv<'local>) -> R,
) -> R {
    // SAFETY: the clone is used only by `body`, on this thread, within this
    // native call; `env` is used again only after `body` has unwound.
    let body_env = unsafe { env.unsafe_clone() };
    match catch_unwind(AssertUnwindSafe(|| body(body_env))) {
        Ok(value) => value,
        Err(payload) => {
            throw_from_panic(&mut env, payload.as_ref());
            on_panic
        }
    }
}

/// Throws the Java exception for a panic caught by [`guard_or`].
///
/// A blocking call made from inside a callback becomes an
/// `IllegalStateException`; any other panic becomes a `NonRetriableException`.
pub(crate) fn throw_from_panic(env: &mut JNIEnv, payload: &(dyn Any + Send)) {
    if payload.is::<BlockingCallInCallback>() {
        if env.exception_check().unwrap_or(false) {
            return;
        }
        let class = &get_class_cache().illegal_state_exception_class;
        match create_exception(env, class, BLOCKING_CALL_IN_CALLBACK_MESSAGE) {
            Some(exception) => {
                if let Err(e) = env.throw(exception) {
                    tracing::error!("Failed to throw IllegalStateException: {}", e);
                }
            }
            None => tracing::error!("Failed to create IllegalStateException"),
        }
        return;
    }
    let detail = payload
        .downcast_ref::<&str>()
        .map(|message| message.to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic".to_string());
    tracing::error!("Panic in Zerobus native code: {detail}");
    if env.exception_check().unwrap_or(false) {
        return;
    }
    throw_non_retriable_exception(
        env,
        &format!("Internal error in Zerobus native code: {detail}"),
    );
}

/// A Java exception taken off the current thread by [`take_java_exception`].
pub(crate) struct JavaException<'local> {
    pub(crate) throwable: JThrowable<'local>,
    pub(crate) message: Option<String>,
    pub(crate) non_retryable: bool,
}

/// Takes the pending Java exception off the current thread and describes it.
///
/// Call this after any JNI call into Java code fails with
/// `jni::errors::Error::JavaException`, before making another JNI call.
/// Every intermediate exception is cleared, so the thread is left with none
/// pending even if describing the exception fails.
pub(crate) fn take_java_exception<'local>(
    env: &mut JNIEnv<'local>,
) -> Option<JavaException<'local>> {
    let throwable = match env.exception_occurred() {
        Ok(throwable) => throwable,
        Err(_) => {
            let _ = env.exception_clear();
            return None;
        }
    };
    let _ = env.exception_clear();

    let explicitly_non_retryable = env
        .is_instance_of(
            &throwable,
            as_jclass(&get_class_cache().non_retriable_exception_class),
        )
        .unwrap_or(false);
    let _ = env.exception_clear();
    let fatal_error = env
        .is_instance_of(&throwable, as_jclass(&get_class_cache().error_class))
        .unwrap_or(false);
    let _ = env.exception_clear();
    let message = extract_throwable_string(env, &throwable);
    let _ = env.exception_clear();

    Some(JavaException {
        throwable,
        message,
        non_retryable: explicitly_non_retryable || fatal_error,
    })
}

fn extract_throwable_string(env: &mut JNIEnv<'_>, throwable: &JObject<'_>) -> Option<String> {
    let message = env
        .call_method(throwable, "toString", "()Ljava/lang/String;", &[])
        .ok()?
        .l()
        .ok()?;
    let message = JString::from(message);
    env.get_string(&message).ok().map(Into::into)
}

/// Throw a ZerobusException in Java.
///
/// This function creates and throws a Java ZerobusException with the given message.
pub fn throw_zerobus_exception(env: &mut JNIEnv, message: &str) {
    let cache = get_class_cache();
    match create_exception(env, &cache.zerobus_exception_class, message) {
        Some(exc) => {
            if let Err(e) = env.throw(exc) {
                tracing::error!("Failed to throw ZerobusException: {}", e);
            }
        }
        None => {
            tracing::error!("Failed to create ZerobusException");
        }
    }
}

/// Throw a NonRetriableException in Java.
///
/// This function creates and throws a Java NonRetriableException with the given message.
pub fn throw_non_retriable_exception(env: &mut JNIEnv, message: &str) {
    let cache = get_class_cache();
    match create_exception(env, &cache.non_retriable_exception_class, message) {
        Some(exc) => {
            if let Err(e) = env.throw(exc) {
                tracing::error!("Failed to throw NonRetriableException: {}", e);
            }
        }
        None => {
            tracing::error!("Failed to create NonRetriableException");
        }
    }
}

/// Convert a ZerobusError to a Java exception and throw it.
///
/// This function maps Rust error types to appropriate Java exception types
/// based on whether the error is retryable or not.
pub fn throw_from_zerobus_error(env: &mut JNIEnv, error: &ZerobusError) {
    let message = error.to_string();

    if error.is_retryable() {
        throw_zerobus_exception(env, &message);
    } else {
        throw_non_retriable_exception(env, &message);
    }
}

/// Create a Java exception object from a ZerobusError.
///
/// This function creates but does not throw the exception, useful for
/// completing CompletableFutures exceptionally.
pub fn create_exception_from_error<'local>(
    env: &mut JNIEnv<'local>,
    error: &ZerobusError,
) -> Option<JThrowable<'local>> {
    let cache = get_class_cache();
    let message = error.to_string();
    let class_ref = if error.is_retryable() {
        &cache.zerobus_exception_class
    } else {
        &cache.non_retriable_exception_class
    };

    create_exception(env, class_ref, &message)
}

/// Create a Java exception object with the given cached class reference and message.
pub fn create_exception<'local>(
    env: &mut JNIEnv<'local>,
    class_ref: &GlobalRef,
    message: &str,
) -> Option<JThrowable<'local>> {
    let class = as_jclass(class_ref);

    // Create the message string
    let j_message = match env.new_string(message) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!("Failed to create message string: {}", e);
            return None;
        }
    };

    // Create the exception instance
    match env.new_object(
        class,
        "(Ljava/lang/String;)V",
        &[JValue::Object(&j_message.into())],
    ) {
        Ok(obj) => Some(JThrowable::from(obj)),
        Err(e) => {
            tracing::error!("Failed to create exception instance: {}", e);
            None
        }
    }
}

/// Create a ZerobusException Java object.
pub fn create_zerobus_exception<'local>(
    env: &mut JNIEnv<'local>,
    message: &str,
) -> Option<JThrowable<'local>> {
    let cache = get_class_cache();
    create_exception(env, &cache.zerobus_exception_class, message)
}

/// Create a NonRetriableException Java object.
pub fn create_non_retriable_exception<'local>(
    env: &mut JNIEnv<'local>,
    message: &str,
) -> Option<JThrowable<'local>> {
    let cache = get_class_cache();
    create_exception(env, &cache.non_retriable_exception_class, message)
}

/// Check if a Java exception is pending and clear it.
///
/// Returns the exception message if one was pending.
pub fn check_and_clear_exception(env: &mut JNIEnv) -> Option<String> {
    if env.exception_check().unwrap_or(false) {
        if let Ok(exc) = env.exception_occurred() {
            let _ = env.exception_clear();

            // Try to get the exception message
            if let Ok(message_obj) =
                env.call_method(&exc, "getMessage", "()Ljava/lang/String;", &[])
            {
                if let Ok(msg) = message_obj.l() {
                    if !msg.is_null() {
                        let jstr: &JString = msg.as_ref().into();
                        if let Ok(s) = env.get_string(jstr) {
                            return Some(s.into());
                        }
                    }
                }
            }
            return Some("Unknown exception".to_string());
        }
    }
    None
}

/// A result type alias for JNI operations that may throw Java exceptions.
pub type JniResult<T> = Result<T, JniError>;

/// Error type for JNI operations.
#[derive(Debug, thiserror::Error)]
pub enum JniError {
    #[error("JNI error: {0}")]
    Jni(#[from] jni::errors::Error),

    #[error("Zerobus error: {0}")]
    Zerobus(#[from] ZerobusError),

    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    #[error("Null pointer")]
    NullPointer,
}
