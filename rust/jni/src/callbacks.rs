//! JNI bridge for AckCallback interface.
//!
//! This module provides a Rust implementation of the SDK's AckCallback trait
//! that delegates to a Java AckCallback object.

use crate::errors::take_java_exception;
use crate::runtime::get_jvm;
use databricks_zerobus_ingest_sdk::{AckCallback, OffsetId};
use jni::errors::Error as JniError;
use jni::objects::{GlobalRef, JValue};
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

        let callback = self.callback_ref.as_obj();

        if let Err(e) = env.call_method(callback, "onAck", "(J)V", &[JValue::Long(offset_id)]) {
            report_callback_failure(&mut env, "onAck", e);
        }
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

        let callback = self.callback_ref.as_obj();

        let j_message = match env.new_string(error_message) {
            Ok(s) => s,
            Err(e) => {
                report_callback_failure(&mut env, "onError", e);
                return;
            }
        };

        if let Err(e) = env.call_method(
            callback,
            "onError",
            "(JLjava/lang/String;)V",
            &[JValue::Long(offset_id), JValue::Object(&j_message.into())],
        ) {
            report_callback_failure(&mut env, "onError", e);
        }
    }
}

/// Logs a failed callback and clears any Java exception it left pending.
///
/// Callbacks run on SDK threads that never return to Java, so nothing else
/// clears the exception. Left pending, it makes every later JNI call on that
/// thread fail, including callbacks of other streams that reuse it.
fn report_callback_failure(env: &mut JNIEnv, method: &str, error: JniError) {
    if matches!(error, JniError::JavaException) {
        let detail = take_java_exception(env)
            .and_then(|exception| exception.message)
            .unwrap_or_else(|| "Java exception was thrown".to_string());
        tracing::error!("AckCallback.{method} threw: {detail}");
    } else {
        tracing::error!("Failed to call AckCallback.{method}: {error}");
    }
}

// Safety: The GlobalRef is thread-safe and the JVM can be accessed from any thread
unsafe impl Send for JavaAckCallback {}
unsafe impl Sync for JavaAckCallback {}
