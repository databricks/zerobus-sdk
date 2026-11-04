//! JNI bridge for AckCallback interface.
//!
//! This module provides a Rust implementation of the SDK's AckCallback trait
//! that delegates to a Java AckCallback object.

use crate::class_cache::{as_jclass, get_class_cache};
use crate::errors::take_java_exception;
use crate::runtime::{get_jvm, CallbackScope};
use databricks_zerobus_ingest_sdk::{AckCallback, OffsetId};
use jni::errors::Error as JniError;
use jni::objects::{GlobalRef, JThrowable, JValue};
use jni::JNIEnv;
use std::sync::Arc;

/// A JNI bridge that implements the Rust AckCallback trait by delegating
/// to a Java AckCallback object.
pub struct JavaAckCallback {
    /// Global reference to the Java AckCallback object.
    callback_ref: GlobalRef,
}

impl JavaAckCallback {
    /// Create a new JavaAckCallback from a GlobalRef to a Java AckCallback object.
    pub fn new(callback_ref: GlobalRef) -> Self {
        Self { callback_ref }
    }

    /// Create an Arc-wrapped JavaAckCallback suitable for use with the SDK.
    pub fn into_arc(self) -> Arc<dyn AckCallback> {
        Arc::new(self)
    }
}

impl AckCallback for JavaAckCallback {
    fn on_ack(&self, offset_id: OffsetId) {
        let jvm = get_jvm();
        let mut env = match jvm.attach_current_thread_as_daemon() {
            Ok(env) => env,
            Err(e) => {
                tracing::error!("Failed to attach to JVM for onAck callback: {}", e);
                return;
            }
        };

        run_in_local_frame(&mut env, "onAck", |env| {
            let callback = self.callback_ref.as_obj();
            let result = {
                let _scope = CallbackScope::enter();
                env.call_method(callback, "onAck", "(J)V", &[JValue::Long(offset_id)])
            };
            if let Err(e) = result {
                report_callback_failure(env, "onAck", e);
            }
        });
    }

    fn on_error(&self, offset_id: OffsetId, error_message: &str) {
        let jvm = get_jvm();
        let mut env = match jvm.attach_current_thread_as_daemon() {
            Ok(env) => env,
            Err(e) => {
                tracing::error!("Failed to attach to JVM for onError callback: {}", e);
                return;
            }
        };

        run_in_local_frame(&mut env, "onError", |env| {
            let callback = self.callback_ref.as_obj();
            let j_message = match env.new_string(error_message) {
                Ok(s) => s,
                Err(e) => {
                    report_callback_failure(env, "onError", e);
                    return;
                }
            };
            let result = {
                let _scope = CallbackScope::enter();
                env.call_method(
                    callback,
                    "onError",
                    "(JLjava/lang/String;)V",
                    &[JValue::Long(offset_id), JValue::Object(&j_message.into())],
                )
            };
            if let Err(e) = result {
                report_callback_failure(env, "onError", e);
            }
        });
    }
}

/// Local references one callback creates, including failure reporting.
const CALLBACK_LOCAL_FRAME_CAPACITY: i32 = 16;

/// Runs a callback's JNI work inside a local frame that frees its local references.
///
/// Callbacks run on SDK threads that never return to Java. Without a frame,
/// every local reference created here (the error message, a thrown exception
/// and its description) would stay alive, keeping those objects reachable,
/// until the thread exits.
fn run_in_local_frame(env: &mut JNIEnv, method: &str, f: impl FnOnce(&mut JNIEnv)) {
    let result = env.with_local_frame(
        CALLBACK_LOCAL_FRAME_CAPACITY,
        |env| -> jni::errors::Result<()> {
            f(env);
            Ok(())
        },
    );
    if let Err(error) = result {
        // Pushing a frame fails only when the JVM is out of memory, which also throws.
        let _ = env.exception_clear();
        tracing::error!("Failed to call AckCallback.{method}: {error}");
    }
}

/// Clears the Java exception a failed callback left pending and logs it through SLF4J.
///
/// Callbacks run on SDK threads that never return to Java, so nothing else
/// clears the exception. Left pending, it makes every later JNI call on that
/// thread fail, including callbacks of other streams that reuse it.
fn report_callback_failure(env: &mut JNIEnv, method: &str, error: JniError) {
    if !matches!(error, JniError::JavaException) {
        tracing::error!("Failed to call AckCallback.{method}: {error}");
        return;
    }
    let Some(exception) = take_java_exception(env) else {
        tracing::error!("AckCallback.{method} threw an exception that could not be read");
        return;
    };
    if let Err(log_error) = log_callback_exception(env, method, &exception.throwable) {
        let _ = env.exception_clear();
        tracing::error!(
            "AckCallback.{method} threw: {}; logging it failed: {log_error}",
            exception.message.as_deref().unwrap_or("unknown exception"),
        );
    }
}

/// Logs a callback's exception, with its stack trace, through the Java SDK's SLF4J logger.
fn log_callback_exception(
    env: &mut JNIEnv,
    method: &str,
    throwable: &JThrowable,
) -> jni::errors::Result<()> {
    let callback = env.new_string(format!("AckCallback.{method}"))?;
    env.call_static_method(
        as_jclass(&get_class_cache().callback_exception_logger_class),
        "log",
        "(Ljava/lang/String;Ljava/lang/Throwable;)V",
        &[JValue::Object(&callback), JValue::Object(throwable)],
    )?;
    Ok(())
}

// Safety: The GlobalRef is thread-safe and the JVM can be accessed from any thread
unsafe impl Send for JavaAckCallback {}
unsafe impl Sync for JavaAckCallback {}
