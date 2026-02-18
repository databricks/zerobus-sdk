//! Error handling and conversion between Rust and Java exceptions.
//!
//! This module provides utilities for converting Zerobus SDK errors to
//! appropriate Java exceptions.

use databricks_zerobus_ingest_sdk::ZerobusError;
use jni::objects::{JString, JThrowable, JValue};
use jni::JNIEnv;

/// Java class names for exceptions.
const ZEROBUS_EXCEPTION_CLASS: &str = "com/databricks/zerobus/ZerobusException";
const NON_RETRIABLE_EXCEPTION_CLASS: &str = "com/databricks/zerobus/NonRetriableException";

/// Throw a ZerobusException in Java.
///
/// This function creates and throws a Java ZerobusException with the given message.
pub fn throw_zerobus_exception(env: &mut JNIEnv, message: &str) {
    if let Err(e) = env.throw_new(ZEROBUS_EXCEPTION_CLASS, message) {
        tracing::error!("Failed to throw ZerobusException: {}", e);
    }
}

/// Throw a NonRetriableException in Java.
///
/// This function creates and throws a Java NonRetriableException with the given message.
pub fn throw_non_retriable_exception(env: &mut JNIEnv, message: &str) {
    if let Err(e) = env.throw_new(NON_RETRIABLE_EXCEPTION_CLASS, message) {
        tracing::error!("Failed to throw NonRetriableException: {}", e);
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
    let message = error.to_string();
    let class_name = if error.is_retryable() {
        ZEROBUS_EXCEPTION_CLASS
    } else {
        NON_RETRIABLE_EXCEPTION_CLASS
    };

    create_exception(env, class_name, &message)
}

/// Create a Java exception object with the given class and message.
pub fn create_exception<'local>(
    env: &mut JNIEnv<'local>,
    class_name: &str,
    message: &str,
) -> Option<JThrowable<'local>> {
    // Find the exception class
    let class = match env.find_class(class_name) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("Failed to find exception class {}: {}", class_name, e);
            return None;
        }
    };

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
    create_exception(env, ZEROBUS_EXCEPTION_CLASS, message)
}

/// Create a NonRetriableException Java object.
pub fn create_non_retriable_exception<'local>(
    env: &mut JNIEnv<'local>,
    message: &str,
) -> Option<JThrowable<'local>> {
    create_exception(env, NON_RETRIABLE_EXCEPTION_CLASS, message)
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
