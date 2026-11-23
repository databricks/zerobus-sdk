//! Native test helpers for JNI integration testing.
//!
//! These JNI functions are used by `ClassLoaderIsolationTest` to verify that
//! class references work correctly from daemon threads, even when the system
//! classloader cannot see the SDK classes, and by the callback native tests to
//! drive Java callbacks from SDK-owned threads.

use crate::callbacks::JavaAckCallback;
use crate::class_cache::{as_jclass, get_class_cache};
use crate::errors::guard;
use crate::headers_provider::{get_headers_blocking, invalidate_blocking, JavaHeadersProvider};
use crate::runtime::{block_on, get_jvm, get_runtime};
use databricks_zerobus_ingest_sdk::{AckCallback, HeadersProvider, ZerobusError};
use jni::objects::{JClass, JObject, JString};
use jni::sys::jint;
use jni::JNIEnv;
use std::sync::Arc;
use std::time::Duration;

/// Test finding a class from a Tokio daemon thread using direct `find_class`.
///
/// Returns `"OK"` if the class is found, or an error message string.
/// In an isolated classloader environment, this is expected to FAIL because
/// daemon threads use the system classloader which cannot see SDK classes.
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_NativeTestHelper_nativeTestFindClassFromDaemonThread<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    class_name: JString<'local>,
) -> JObject<'local> {
    guard(env, |env| {
        native_test_find_class_from_daemon_thread(env, _class, class_name)
    })
}

fn native_test_find_class_from_daemon_thread<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    class_name: JString<'local>,
) -> JObject<'local> {
    let class_name_str: String = match env.get_string(&class_name) {
        Ok(s) => s.into(),
        Err(_) => return JObject::null(),
    };

    let result = get_runtime().block_on(async {
        tokio::task::spawn_blocking(move || {
            let jvm = get_jvm();
            let mut env = match jvm.attach_current_thread_as_daemon() {
                Ok(env) => env,
                Err(e) => return format!("ERROR: Failed to attach thread: {}", e),
            };
            match env.find_class(&class_name_str) {
                Ok(_) => "OK".to_string(),
                Err(e) => {
                    // Capture the actual Java exception details before clearing
                    let exception_detail = if env.exception_check().unwrap_or(false) {
                        if let Ok(exc) = env.exception_occurred() {
                            let _ = env.exception_clear();
                            // Get exception class name
                            let class_name = env
                                .get_object_class(&exc)
                                .ok()
                                .and_then(|cls| {
                                    env.call_method(cls, "getName", "()Ljava/lang/String;", &[])
                                        .ok()
                                })
                                .and_then(|v| v.l().ok())
                                .filter(|o| !o.is_null())
                                .and_then(|o| {
                                    let jstr: jni::objects::JString = o.into();
                                    env.get_string(&jstr).ok().map(String::from)
                                })
                                .unwrap_or_default();
                            // Get exception message
                            let message = env
                                .call_method(&exc, "getMessage", "()Ljava/lang/String;", &[])
                                .ok()
                                .and_then(|v| v.l().ok())
                                .filter(|o| !o.is_null())
                                .and_then(|o| {
                                    let jstr: jni::objects::JString = o.into();
                                    env.get_string(&jstr).ok().map(String::from)
                                })
                                .unwrap_or_default();
                            format!("{}: {}", class_name, message)
                        } else {
                            let _ = env.exception_clear();
                            String::new()
                        }
                    } else {
                        String::new()
                    };
                    format!("ERROR: {} [{}]", e, exception_detail)
                }
            }
        })
        .await
        .unwrap_or_else(|e| format!("ERROR: Task panicked: {}", e))
    });

    match env.new_string(&result) {
        Ok(s) => s.into(),
        Err(_) => JObject::null(),
    }
}

/// Test finding a class from a Tokio daemon thread using the cached `GlobalRef`.
///
/// Returns `"OK"` if the cached class reference is valid, or an error message.
/// This is expected to SUCCEED even in isolated classloader environments because
/// the `GlobalRef` was populated during `JNI_OnLoad` with the correct classloader.
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_NativeTestHelper_nativeTestFindClassFromDaemonThreadCached<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    class_name: JString<'local>,
) -> JObject<'local> {
    guard(env, |env| {
        native_test_find_class_from_daemon_thread_cached(env, _class, class_name)
    })
}

fn native_test_find_class_from_daemon_thread_cached<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    class_name: JString<'local>,
) -> JObject<'local> {
    let class_name_str: String = match env.get_string(&class_name) {
        Ok(s) => s.into(),
        Err(_) => return JObject::null(),
    };

    let result = get_runtime().block_on(async {
        tokio::task::spawn_blocking(move || {
            let jvm = get_jvm();
            let _env = match jvm.attach_current_thread_as_daemon() {
                Ok(env) => env,
                Err(e) => return format!("ERROR: Failed to attach thread: {}", e),
            };

            let cache = get_class_cache();
            let cached_ref = match class_name_str.as_str() {
                "com/databricks/zerobus/ZerobusException" => Some(&cache.zerobus_exception_class),
                "com/databricks/zerobus/NonRetriableException" => {
                    Some(&cache.non_retriable_exception_class)
                }
                "java/lang/Long" => Some(&cache.long_class),
                "java/util/concurrent/CompletableFuture" => Some(&cache.completable_future_class),
                "java/util/ArrayList" => Some(&cache.array_list_class),
                "com/databricks/zerobus/EncodedBatch" => Some(&cache.encoded_batch_class),
                _ => None,
            };

            match cached_ref {
                Some(global_ref) => {
                    let jclass = as_jclass(global_ref);
                    if jclass.is_null() {
                        return "ERROR: cached class reference is null".to_string();
                    }
                    // Actually instantiate the class to verify the ref is usable,
                    // not just non-null. Use ArrayList since it has a no-arg ctor.
                    let mut env = match jvm.attach_current_thread_as_daemon() {
                        Ok(env) => env,
                        Err(e) => return format!("ERROR: Failed to re-attach thread: {}", e),
                    };
                    let test_class = as_jclass(&cache.array_list_class);
                    match env.new_object(test_class, "()V", &[]) {
                        Ok(_) => "OK".to_string(),
                        Err(e) => {
                            let _ = env.exception_clear();
                            format!("ERROR: cached ref is non-null but unusable: {}", e)
                        }
                    }
                }
                None => format!("ERROR: class {} not found in cache", class_name_str),
            }
        })
        .await
        .unwrap_or_else(|e| format!("ERROR: Task panicked: {}", e))
    });

    match env.new_string(&result) {
        Ok(s) => s.into(),
        Err(_) => JObject::null(),
    }
}

#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_NativeTestHelper_nativeTestHeadersProviderCallbacks<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    provider: JObject<'local>,
) -> JObject<'local> {
    guard(env, |env| {
        native_test_headers_provider_callbacks(env, _class, provider)
    })
}

fn native_test_headers_provider_callbacks<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    provider: JObject<'local>,
) -> JObject<'local> {
    let provider_ref = match env.new_global_ref(provider) {
        Ok(provider_ref) => provider_ref,
        Err(error) => return test_result(&mut env, format!("new_global_ref failed: {error}")),
    };

    let result = get_runtime().block_on(async {
        tokio::task::spawn_blocking(move || {
            let first = get_headers_blocking(provider_ref.clone());
            if !first.is_err_and(|error| error.is_retryable()) {
                return "first provider exception was not retryable".to_string();
            }

            let second = get_headers_blocking(provider_ref.clone());
            if !second.is_ok_and(|headers| {
                headers.get("authorization").map(String::as_str) == Some("Bearer token")
            }) {
                return "second provider call did not return headers".to_string();
            }

            invalidate_blocking(provider_ref.clone());

            let third = get_headers_blocking(provider_ref.clone());
            if !third.is_err_and(|error| !error.is_retryable()) {
                return "NonRetriableException was retryable".to_string();
            }

            let fourth = get_headers_blocking(provider_ref.clone());
            if !fourth.is_err_and(|error| !error.is_retryable()) {
                return "Java Error was retryable".to_string();
            }

            let fifth = get_headers_blocking(provider_ref.clone());
            if !fifth.is_err_and(|error| {
                matches!(error, ZerobusError::InvalidArgument(message) if message.contains("java.lang.String"))
            }) {
                return "non-String map entry was not rejected clearly".to_string();
            }

            let sixth = get_headers_blocking(provider_ref.clone());
            if !sixth.is_err_and(|error| {
                matches!(error, ZerobusError::InvalidArgument(message) if message.contains("invalid gRPC metadata header name"))
            }) {
                return "invalid header name was not rejected clearly".to_string();
            }

            let seventh = get_headers_blocking(provider_ref);
            if !seventh.is_err_and(|error| {
                matches!(error, ZerobusError::InvalidArgument(message) if message.contains("maximum length"))
            }) {
                return "oversized header name was not rejected clearly".to_string();
            }

            "OK".to_string()
        })
        .await
        .unwrap_or_else(|error| format!("callback task failed: {error}"))
    });

    test_result(&mut env, result)
}

#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_NativeTestHelper_nativeTestHeadersProviderSerialization<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    provider: JObject<'local>,
) -> JObject<'local> {
    guard(env, |env| {
        native_test_headers_provider_serialization(env, _class, provider)
    })
}

fn native_test_headers_provider_serialization<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    provider: JObject<'local>,
) -> JObject<'local> {
    let provider_ref = match env.new_global_ref(provider) {
        Ok(provider_ref) => provider_ref,
        Err(error) => return test_result(&mut env, format!("new_global_ref failed: {error}")),
    };

    let result = get_runtime().block_on(async {
        let provider = Arc::new(JavaHeadersProvider::new(provider_ref));
        if tokio::time::timeout(Duration::from_millis(100), provider.get_headers())
            .await
            .is_ok()
        {
            return "first provider call did not time out".to_string();
        }

        match tokio::time::timeout(Duration::from_secs(2), provider.get_headers()).await {
            Ok(Ok(headers))
                if headers.get("authorization").map(String::as_str) == Some("Bearer token") =>
            {
                "OK".to_string()
            }
            Ok(Ok(_)) => "second provider call returned unexpected headers".to_string(),
            Ok(Err(error)) => format!("second provider call failed: {error}"),
            Err(_) => "second provider call timed out".to_string(),
        }
    });

    test_result(&mut env, result)
}

/// Panics inside a JNI entry point.
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_NativeTestHelper_nativeTestPanic<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
) {
    guard(env, |_env| panic!("test panic"))
}

/// Blocks on the SDK runtime, as the blocking stream methods (`flush`, `close`, ...) do.
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_NativeTestHelper_nativeTestBlockOn<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
) {
    guard(env, |_env| block_on(async {}))
}

/// Runs `task.run()` on a Tokio worker thread, where the SDK invokes ack callbacks.
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_NativeTestHelper_nativeTestRunOnRuntimeWorker<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    task: JObject<'local>,
) -> JObject<'local> {
    guard(env, |env| {
        native_test_run_on_runtime_worker(env, _class, task)
    })
}

fn native_test_run_on_runtime_worker<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    task: JObject<'local>,
) -> JObject<'local> {
    let task_ref = match env.new_global_ref(task) {
        Ok(task_ref) => task_ref,
        Err(error) => return test_result(&mut env, format!("new_global_ref failed: {error}")),
    };

    let result = get_runtime().block_on(async {
        get_runtime()
            .spawn(async move {
                let mut env = match get_jvm().attach_current_thread_as_daemon() {
                    Ok(env) => env,
                    Err(error) => return format!("failed to attach thread: {error}"),
                };
                match env.call_method(task_ref.as_obj(), "run", "()V", &[]) {
                    Ok(_) => "OK".to_string(),
                    Err(error) => {
                        let _ = env.exception_clear();
                        format!("task failed: {error}")
                    }
                }
            })
            .await
            .unwrap_or_else(|error| format!("worker task failed: {error}"))
    });

    test_result(&mut env, result)
}

/// Drives `JavaAckCallback` from one SDK-owned thread, as the SDK's callback worker does.
///
/// Every Java callback may throw; each later call must still reach Java, and no
/// exception may be left pending on the thread.
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_NativeTestHelper_nativeTestAckCallbackExceptions<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    callback: JObject<'local>,
    acks: jint,
) -> JObject<'local> {
    guard(env, |env| {
        native_test_ack_callback_exceptions(env, _class, callback, acks)
    })
}

fn native_test_ack_callback_exceptions<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    callback: JObject<'local>,
    acks: jint,
) -> JObject<'local> {
    let callback_ref = match env.new_global_ref(callback) {
        Ok(callback_ref) => callback_ref,
        Err(error) => return test_result(&mut env, format!("new_global_ref failed: {error}")),
    };

    let result = get_runtime().block_on(async {
        tokio::task::spawn_blocking(move || {
            let callback = JavaAckCallback::new(callback_ref);
            for offset in 0..i64::from(acks) {
                callback.on_ack(offset);
            }
            callback.on_error(i64::from(acks), "ack failed");

            let env = match get_jvm().attach_current_thread_as_daemon() {
                Ok(env) => env,
                Err(error) => return format!("failed to attach thread: {error}"),
            };
            if env.exception_check().unwrap_or(true) {
                return "a Java exception was left pending on the callback thread".to_string();
            }
            "OK".to_string()
        })
        .await
        .unwrap_or_else(|error| format!("callback task failed: {error}"))
    });

    test_result(&mut env, result)
}

/// Invokes `callback.onAck` and then `callback.onError` through `JavaAckCallback`
/// on a Tokio worker thread, exactly as the SDK delivers acknowledgments.
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_NativeTestHelper_nativeTestAckCallbackOnRuntimeWorker<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    callback: JObject<'local>,
) -> JObject<'local> {
    guard(env, |env| {
        native_test_ack_callback_on_runtime_worker(env, _class, callback)
    })
}

fn native_test_ack_callback_on_runtime_worker<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    callback: JObject<'local>,
) -> JObject<'local> {
    let callback_ref = match env.new_global_ref(callback) {
        Ok(callback_ref) => callback_ref,
        Err(error) => return test_result(&mut env, format!("new_global_ref failed: {error}")),
    };

    let result = get_runtime().block_on(async {
        get_runtime()
            .spawn(async move {
                let callback = JavaAckCallback::new(callback_ref);
                callback.on_ack(0);
                callback.on_error(1, "ack failed");
                "OK".to_string()
            })
            .await
            .unwrap_or_else(|error| format!("worker task failed: {error}"))
    });

    test_result(&mut env, result)
}

fn test_result<'local>(env: &mut JNIEnv<'local>, result: String) -> JObject<'local> {
    env.new_string(result)
        .map(Into::into)
        .unwrap_or_else(|_| JObject::null())
}
