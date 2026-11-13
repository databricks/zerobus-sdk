//! Native test helpers for classloader isolation testing.
//!
//! These JNI functions are used by `ClassLoaderIsolationTest` to verify that
//! class references work correctly from daemon threads, even when the system
//! classloader cannot see the SDK classes.

use crate::class_cache::{as_jclass, get_class_cache};
use crate::federated_auth::{get_token_blocking, java_idp_token_supplier};
use crate::headers_provider::{get_headers_blocking, invalidate_blocking, JavaHeadersProvider};
use crate::runtime::{get_jvm, get_runtime};
use databricks_zerobus_ingest_sdk::{FederatedTokenProvider, HeadersProvider, ZerobusError};
use jni::objects::{JClass, JObject, JString};
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

/// Exercises `IdpTokenSupplier.getToken()` error classification on one native
/// blocking thread. The Java supplier must, in order: throw a plain exception,
/// return `"idp-token"`, throw `NonRetriableException`, throw a `java.lang.Error`,
/// and return `null`.
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_NativeTestHelper_nativeTestIdpTokenSupplierCallbacks<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    supplier: JObject<'local>,
) -> JObject<'local> {
    let supplier_ref = match env.new_global_ref(supplier) {
        Ok(supplier_ref) => supplier_ref,
        Err(error) => return test_result(&mut env, format!("new_global_ref failed: {error}")),
    };

    let result = get_runtime().block_on(async {
        tokio::task::spawn_blocking(move || {
            let first = get_token_blocking(supplier_ref.clone());
            if !first.is_err_and(|error| {
                error.is_retryable()
                    && matches!(&error, ZerobusError::TokenFetchError(message) if message.contains("Java IdpTokenSupplier error"))
            }) {
                return "supplier exception was not a retryable token-fetch error".to_string();
            }

            let second = get_token_blocking(supplier_ref.clone());
            if !second.is_ok_and(|token| token == "idp-token") {
                return "supplier token was not returned".to_string();
            }

            let third = get_token_blocking(supplier_ref.clone());
            if !third.is_err_and(|error| !error.is_retryable()) {
                return "NonRetriableException was retryable".to_string();
            }

            let fourth = get_token_blocking(supplier_ref.clone());
            if !fourth.is_err_and(|error| !error.is_retryable()) {
                return "Java Error was retryable".to_string();
            }

            let fifth = get_token_blocking(supplier_ref);
            if !fifth.is_err_and(|error| {
                matches!(error, ZerobusError::InvalidArgument(message) if message.contains("getToken returned null"))
            }) {
                return "null token was not rejected clearly".to_string();
            }

            "OK".to_string()
        })
        .await
        .unwrap_or_else(|error| format!("callback task failed: {error}"))
    });

    test_result(&mut env, result)
}

/// Drives a Java `IdpTokenSupplier` through the core's `FederatedTokenProvider`
/// (the async bridge, not `get_token_blocking` directly). The supplier must throw
/// `NonRetriableException("idp unavailable")`; the provider must surface that as a
/// non-retryable error before attempting any token exchange.
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_NativeTestHelper_nativeTestFederatedProviderSupplierError<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    supplier: JObject<'local>,
) -> JObject<'local> {
    let supplier_ref = match env.new_global_ref(supplier) {
        Ok(supplier_ref) => supplier_ref,
        Err(error) => return test_result(&mut env, format!("new_global_ref failed: {error}")),
    };

    let result = get_runtime().block_on(async {
        let provider = FederatedTokenProvider::new(
            None,
            java_idp_token_supplier(supplier_ref, "java:native-test".to_string()),
            "catalog.schema.table".to_string(),
            "1234567890".to_string(),
            // Never contacted: the supplier fails before the exchange.
            "http://127.0.0.1:9".to_string(),
        );
        match provider.get_headers().await {
            Ok(_) => "provider returned headers despite supplier failure".to_string(),
            Err(error) if error.is_retryable() => {
                format!("supplier failure became retryable: {error}")
            }
            Err(error) if !error.to_string().contains("idp unavailable") => {
                format!("supplier failure detail was lost: {error}")
            }
            Err(_) => "OK".to_string(),
        }
    });

    test_result(&mut env, result)
}

fn test_result<'local>(env: &mut JNIEnv<'local>, result: String) -> JObject<'local> {
    env.new_string(result)
        .map(Into::into)
        .unwrap_or_else(|_| JObject::null())
}
