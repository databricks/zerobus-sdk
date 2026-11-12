//! JNI bindings for ZerobusSdk.
//!
//! This module provides JNI functions for creating and managing ZerobusSdk instances.

use crate::arrow_stream::NativeArrowStreamHandle;
use crate::async_bridge::{create_completable_future, spawn_and_complete};
use crate::errors::{throw_from_zerobus_error, throw_zerobus_exception};
use crate::federated_auth::java_idp_token_supplier;
use crate::headers_provider::JavaHeadersProvider;
use crate::options::{
    apply_arrow_stream_options, apply_stream_options, extract_arrow_stream_options,
    extract_stream_options,
};
use crate::runtime::block_on;
use crate::stream::NativeStreamHandle;
use databricks_zerobus_ingest_sdk::{
    HeadersProvider, IdpTokenSupplier, RecordType, StreamBuilder, ZerobusSdk,
};
use jni::objects::{JByteArray, JClass, JObject, JString};
use jni::sys::{jboolean, jint, jlong, JNI_FALSE};
use jni::JNIEnv;
use std::sync::Arc;

/// Native ABI version; bump in lockstep with Java's `NativeLoader.EXPECTED_NATIVE_ABI` whenever
/// native method signatures change, so a stale library is rejected at load.
const NATIVE_ABI_VERSION: jint = 2;

/// # JNI Signature
/// ```java
/// private static native int nativeAbiVersion();
/// ```
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_NativeLoader_nativeAbiVersion<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jint {
    NATIVE_ABI_VERSION
}

/// Native SDK handle stored in Java.
pub struct NativeSdkHandle {
    pub sdk: Arc<ZerobusSdk>,
}

impl NativeSdkHandle {
    pub fn new(sdk: ZerobusSdk) -> Self {
        Self { sdk: Arc::new(sdk) }
    }

    pub fn into_raw(self) -> jlong {
        Box::into_raw(Box::new(self)) as jlong
    }

    /// # Safety
    ///
    /// The pointer must have been created by `into_raw` and not freed.
    pub unsafe fn from_raw(ptr: jlong) -> Box<Self> {
        Box::from_raw(ptr as *mut Self)
    }

    /// # Safety
    ///
    /// The pointer must have been created by `into_raw` and not freed.
    pub unsafe fn borrow_from_raw(ptr: jlong) -> &'static Self {
        &*(ptr as *const Self)
    }
}

/// How a stream created over JNI authenticates.
enum JniStreamAuth {
    /// OAuth client credentials.
    OAuth {
        client_id: String,
        client_secret: String,
    },
    /// A Java `HeadersProvider`.
    HeadersProvider(Arc<dyn HeadersProvider>),
    /// External-IdP federation. `client_id` is `None` for account-level
    /// federation, or the service principal client_id for workload identity
    /// federation.
    Federated {
        idp_token_supplier: IdpTokenSupplier,
        client_id: Option<String>,
    },
}

impl JniStreamAuth {
    /// The OAuth credentials kept on the native stream handle. Empty for the
    /// other modes; recreation reuses the stream's headers provider instead.
    fn stored_credentials(&self) -> (String, String) {
        match self {
            Self::OAuth {
                client_id,
                client_secret,
            } => (client_id.clone(), client_secret.clone()),
            Self::HeadersProvider(_) | Self::Federated { .. } => (String::new(), String::new()),
        }
    }

    fn apply(self, builder: StreamBuilder<'_>) -> StreamBuilder<'_> {
        match self {
            Self::OAuth {
                client_id,
                client_secret,
            } => builder.oauth(client_id, client_secret),
            Self::HeadersProvider(provider) => builder.headers_provider(provider),
            Self::Federated {
                idp_token_supplier,
                client_id,
            } => builder.federated_auth(idp_token_supplier, client_id),
        }
    }
}

/// Reads OAuth credentials, or a `HeadersProvider` when one is passed (the
/// credentials are then empty and ignored).
fn extract_oauth_or_headers_provider_auth(
    env: &mut JNIEnv<'_>,
    client_id: &JString<'_>,
    client_secret: &JString<'_>,
    headers_provider: &JObject<'_>,
) -> Result<JniStreamAuth, String> {
    let client_id: String = env
        .get_string(client_id)
        .map_err(|e| format!("Invalid client ID: {}", e))?
        .into();
    let client_secret: String = env
        .get_string(client_secret)
        .map_err(|e| format!("Invalid client secret: {}", e))?
        .into();

    if headers_provider.is_null() {
        return Ok(JniStreamAuth::OAuth {
            client_id,
            client_secret,
        });
    }
    let provider_ref = env
        .new_global_ref(headers_provider)
        .map_err(|e| format!("Failed to retain headers provider: {}", e))?;
    Ok(JniStreamAuth::HeadersProvider(
        JavaHeadersProvider::new(provider_ref).into_arc(),
    ))
}

/// Reads the federation parameters passed by the Java `FederatedToken`.
fn extract_federated_auth(
    env: &mut JNIEnv<'_>,
    idp_token_supplier: &JObject<'_>,
    federated_client_id: &JString<'_>,
    cache_identity: &JString<'_>,
) -> Result<JniStreamAuth, String> {
    if idp_token_supplier.is_null() {
        return Err("IdP token supplier must not be null".to_string());
    }
    let client_id: Option<String> = if federated_client_id.is_null() {
        None
    } else {
        Some(
            env.get_string(federated_client_id)
                .map_err(|e| format!("Invalid federated client ID: {}", e))?
                .into(),
        )
    };
    if cache_identity.is_null() {
        return Err("Federated cache identity must not be null".to_string());
    }
    let cache_identity: String = env
        .get_string(cache_identity)
        .map_err(|e| format!("Invalid federated cache identity: {}", e))?
        .into();
    let supplier_ref = env
        .new_global_ref(idp_token_supplier)
        .map_err(|e| format!("Failed to retain IdP token supplier: {}", e))?;
    Ok(JniStreamAuth::Federated {
        idp_token_supplier: java_idp_token_supplier(supplier_ref, cache_identity),
        client_id,
    })
}

fn create_sdk<'local>(
    env: &mut JNIEnv<'local>,
    server_endpoint: JString<'local>,
    unity_catalog_endpoint: JString<'local>,
    application_name: JString<'local>,
    connection_per_stream: bool,
) -> jlong {
    // Extract the endpoint strings
    let server_endpoint: String = match env.get_string(&server_endpoint) {
        Ok(s) => s.into(),
        Err(e) => {
            throw_zerobus_exception(env, &format!("Invalid server endpoint: {}", e));
            return 0;
        }
    };

    let unity_catalog_endpoint: String = match env.get_string(&unity_catalog_endpoint) {
        Ok(s) => s.into(),
        Err(e) => {
            throw_zerobus_exception(env, &format!("Invalid Unity Catalog endpoint: {}", e));
            return 0;
        }
    };

    let application_name: Option<String> = if application_name.is_null() {
        None
    } else {
        match env.get_string(&application_name) {
            Ok(s) => Some(s.into()),
            Err(e) => {
                throw_zerobus_exception(env, &format!("Invalid application name: {}", e));
                return 0;
            }
        }
    };

    // Create the SDK
    let sdk_identifier = format!("zerobus-sdk-java/{}", env!("CARGO_PKG_VERSION"));
    let builder = ZerobusSdk::builder()
        .endpoint(server_endpoint)
        .unity_catalog_url(unity_catalog_endpoint)
        .sdk_identifier(sdk_identifier)
        .connection_per_stream(connection_per_stream);
    let builder = match application_name {
        Some(name) => builder.application_name(name),
        None => builder,
    };
    match builder.build() {
        Ok(sdk) => NativeSdkHandle::new(sdk).into_raw(),
        Err(e) => {
            throw_from_zerobus_error(env, &e);
            0
        }
    }
}

/// Create a new ZerobusSdk instance using the default connection ownership.
///
/// # JNI Signature
/// ```java
/// private static native long nativeCreate(String serverEndpoint, String unityCatalogEndpoint, String applicationName);
/// ```
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_ZerobusSdk_nativeCreate<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    server_endpoint: JString<'local>,
    unity_catalog_endpoint: JString<'local>,
    application_name: JString<'local>,
) -> jlong {
    create_sdk(
        &mut env,
        server_endpoint,
        unity_catalog_endpoint,
        application_name,
        true,
    )
}

/// Create a new ZerobusSdk instance with configurable connection ownership.
///
/// # JNI Signature
/// ```java
/// private static native long nativeCreateWithConnectionPerStream(String serverEndpoint, String unityCatalogEndpoint, String applicationName, boolean connectionPerStream);
/// ```
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_ZerobusSdk_nativeCreateWithConnectionPerStream<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    server_endpoint: JString<'local>,
    unity_catalog_endpoint: JString<'local>,
    application_name: JString<'local>,
    connection_per_stream: jboolean,
) -> jlong {
    create_sdk(
        &mut env,
        server_endpoint,
        unity_catalog_endpoint,
        application_name,
        connection_per_stream != JNI_FALSE,
    )
}

/// Destroy a ZerobusSdk instance.
///
/// # JNI Signature
/// ```java
/// private static native void nativeDestroy(long handle);
/// ```
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_ZerobusSdk_nativeDestroy<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    if handle != 0 {
        // Safety: handle was created by nativeCreate
        unsafe {
            let _ = NativeSdkHandle::from_raw(handle);
        }
    }
}

/// Create a new stream.
///
/// # JNI Signature
/// ```java
/// private native CompletableFuture<Long> nativeCreateStream(
///     long sdkHandle,
///     String tableName,
///     byte[] descriptorProto,
///     String avroSchemaJson,
///     String clientId,
///     String clientSecret,
///     Object options,
///     int formatCode
/// );
/// ```
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_ZerobusSdk_nativeCreateStream<'local>(
    env: JNIEnv<'local>,
    _obj: JObject<'local>,
    sdk_handle: jlong,
    table_name: JString<'local>,
    descriptor_proto: JByteArray<'local>,
    avro_schema: JString<'local>,
    client_id: JString<'local>,
    client_secret: JString<'local>,
    options: JObject<'local>,
    format_code: jint,
) -> JObject<'local> {
    Java_com_databricks_zerobus_ZerobusSdk_nativeCreateStreamWithHeadersProvider(
        env,
        _obj,
        sdk_handle,
        table_name,
        descriptor_proto,
        avro_schema,
        client_id,
        client_secret,
        JObject::null(),
        options,
        format_code,
    )
}

#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_ZerobusSdk_nativeCreateStreamWithHeadersProvider<
    'local,
>(
    mut env: JNIEnv<'local>,
    _obj: JObject<'local>,
    sdk_handle: jlong,
    table_name: JString<'local>,
    descriptor_proto: JByteArray<'local>,
    avro_schema: JString<'local>,
    client_id: JString<'local>,
    client_secret: JString<'local>,
    headers_provider: JObject<'local>,
    options: JObject<'local>,
    format_code: jint,
) -> JObject<'local> {
    let auth = match extract_oauth_or_headers_provider_auth(
        &mut env,
        &client_id,
        &client_secret,
        &headers_provider,
    ) {
        Ok(auth) => auth,
        Err(message) => {
            throw_zerobus_exception(&mut env, &message);
            return JObject::null();
        }
    };
    create_stream(
        env,
        sdk_handle,
        table_name,
        descriptor_proto,
        avro_schema,
        auth,
        options,
        format_code,
    )
}

/// Create a new stream authenticated with external-IdP federation (RFC 8693).
///
/// `federated_client_id` is `null` for account-level federation, or the
/// Databricks service principal client_id for workload identity federation.
/// `cache_identity` is the Java `FederatedToken`'s stable id.
///
/// # JNI Signature
/// ```java
/// private native CompletableFuture<Long> nativeCreateStreamWithFederatedAuth(
///     long sdkHandle,
///     String tableName,
///     byte[] descriptorProto,
///     String avroSchemaJson,
///     IdpTokenSupplier idpTokenSupplier,
///     String federatedClientId,
///     String cacheIdentity,
///     Object options,
///     int formatCode
/// );
/// ```
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_ZerobusSdk_nativeCreateStreamWithFederatedAuth<
    'local,
>(
    mut env: JNIEnv<'local>,
    _obj: JObject<'local>,
    sdk_handle: jlong,
    table_name: JString<'local>,
    descriptor_proto: JByteArray<'local>,
    avro_schema: JString<'local>,
    idp_token_supplier: JObject<'local>,
    federated_client_id: JString<'local>,
    cache_identity: JString<'local>,
    options: JObject<'local>,
    format_code: jint,
) -> JObject<'local> {
    let auth = match extract_federated_auth(
        &mut env,
        &idp_token_supplier,
        &federated_client_id,
        &cache_identity,
    ) {
        Ok(auth) => auth,
        Err(message) => {
            throw_zerobus_exception(&mut env, &message);
            return JObject::null();
        }
    };
    create_stream(
        env,
        sdk_handle,
        table_name,
        descriptor_proto,
        avro_schema,
        auth,
        options,
        format_code,
    )
}

#[allow(clippy::too_many_arguments)]
fn create_stream<'local>(
    mut env: JNIEnv<'local>,
    sdk_handle: jlong,
    table_name: JString<'local>,
    descriptor_proto: JByteArray<'local>,
    avro_schema: JString<'local>,
    auth: JniStreamAuth,
    options: JObject<'local>,
    format_code: jint,
) -> JObject<'local> {
    // Create the CompletableFuture to return
    let future = match create_completable_future(&mut env) {
        Ok(f) => f,
        Err(e) => {
            throw_zerobus_exception(&mut env, &format!("Failed to create future: {}", e));
            return JObject::null();
        }
    };

    // Create a global reference to the future for use in async code
    let future_ref = match env.new_global_ref(&future) {
        Ok(r) => r,
        Err(e) => {
            throw_zerobus_exception(&mut env, &format!("Failed to create global ref: {}", e));
            return JObject::null();
        }
    };

    // Extract parameters
    let table_name: String = match env.get_string(&table_name) {
        Ok(s) => s.into(),
        Err(e) => {
            throw_zerobus_exception(&mut env, &format!("Invalid table name: {}", e));
            return JObject::null();
        }
    };

    // Extract descriptor proto bytes (may be null for JSON)
    let descriptor_proto_bytes: Option<Vec<u8>> = if descriptor_proto.is_null() {
        None
    } else {
        match env.convert_byte_array(descriptor_proto) {
            Ok(bytes) => Some(bytes),
            Err(e) => {
                throw_zerobus_exception(&mut env, &format!("Invalid descriptor proto: {}", e));
                return JObject::null();
            }
        }
    };

    // Avro writer schema (JSON); only present for Avro streams.
    #[cfg(feature = "avro")]
    let avro_schema_json: Option<String> = if avro_schema.is_null() {
        None
    } else {
        match env.get_string(&avro_schema) {
            Ok(s) => Some(s.into()),
            Err(e) => {
                throw_zerobus_exception(&mut env, &format!("Invalid Avro schema: {}", e));
                return JObject::null();
            }
        }
    };
    #[cfg(not(feature = "avro"))]
    let _ = &avro_schema; // unused without the feature, but kept to match the Java signature

    let extracted_options = if options.is_null() {
        None
    } else {
        match extract_stream_options(&mut env, &options) {
            Ok(opts) => Some(opts),
            Err(e) => {
                throw_zerobus_exception(&mut env, &format!("Invalid options: {}", e));
                return JObject::null();
            }
        }
    };

    // Get SDK handle
    let sdk = unsafe { NativeSdkHandle::borrow_from_raw(sdk_handle) };
    let sdk_arc = sdk.sdk.clone();

    // Store credentials for later use in stream recreation
    let credentials = auth.stored_credentials();

    // Spawn async operation to create stream
    spawn_and_complete(
        future_ref,
        async move {
            // Parse descriptor proto if provided
            let descriptor_proto = if let Some(bytes) = descriptor_proto_bytes {
                Some(prost::Message::decode(bytes.as_slice()).map_err(|e| {
                    databricks_zerobus_ingest_sdk::ZerobusError::InvalidArgument(format!(
                        "Failed to parse descriptor proto: {}",
                        e
                    ))
                })?)
            } else {
                None
            };

            let base = auth.apply(sdk_arc.stream_builder().table(table_name));
            // Map the wire format code to the builder, mirroring the FFI layer's integer
            // dispatch. `RecordType` no longer derives `TryFrom<i32>`, and its `Avro` variant
            // only exists with the `avro` feature, so match on the code (its discriminant).
            let (record_type, mut builder) = match format_code {
                code if code == RecordType::Proto as jint => {
                    let desc = descriptor_proto.ok_or_else(|| {
                        databricks_zerobus_ingest_sdk::ZerobusError::InvalidArgument(
                            "Proto descriptor is required for Proto record type".to_string(),
                        )
                    })?;
                    (RecordType::Proto, base.compiled_proto(desc))
                }
                code if code == RecordType::Json as jint => (RecordType::Json, base.json()),
                #[cfg(feature = "avro")]
                code if code == RecordType::Avro as jint => {
                    let schema = avro_schema_json.ok_or_else(|| {
                        databricks_zerobus_ingest_sdk::ZerobusError::InvalidArgument(
                            "Avro schema is required for Avro record type".to_string(),
                        )
                    })?;
                    (RecordType::Avro, base.avro(schema))
                }
                _ => {
                    return Err(
                        databricks_zerobus_ingest_sdk::ZerobusError::InvalidArgument(format!(
                            "Unsupported record format code: {}",
                            format_code
                        )),
                    );
                }
            };
            if let Some(opts) = extracted_options {
                builder = apply_stream_options(builder, opts);
            }

            let stream = builder.build().await?;

            Ok(NativeStreamHandle::new(
                stream,
                record_type,
                credentials.0,
                credentials.1,
            ))
        },
        |handle| handle.into_raw(),
    );

    future
}

/// Recreate a stream from a failed stream.
///
/// # JNI Signature
/// ```java
/// private native CompletableFuture<Long> nativeRecreateStream(long sdkHandle, long streamHandle);
/// ```
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_ZerobusSdk_nativeRecreateStream<'local>(
    mut env: JNIEnv<'local>,
    _obj: JObject<'local>,
    sdk_handle: jlong,
    stream_handle: jlong,
) -> JObject<'local> {
    // Create the CompletableFuture to return
    let future = match create_completable_future(&mut env) {
        Ok(f) => f,
        Err(e) => {
            throw_zerobus_exception(&mut env, &format!("Failed to create future: {}", e));
            return JObject::null();
        }
    };

    // Create a global reference to the future
    let future_ref = match env.new_global_ref(&future) {
        Ok(r) => r,
        Err(e) => {
            throw_zerobus_exception(&mut env, &format!("Failed to create global ref: {}", e));
            return JObject::null();
        }
    };

    // Get SDK and stream handles
    let sdk = unsafe { NativeSdkHandle::borrow_from_raw(sdk_handle) };
    let sdk_arc = sdk.sdk.clone();

    let stream = unsafe { NativeStreamHandle::borrow_from_raw(stream_handle) };
    let client_id = stream.client_id.clone();
    let client_secret = stream.client_secret.clone();
    let record_type = stream.record_type;

    // Take the stream out of the handle for recreation
    let old_stream = {
        let mut guard = block_on(stream.stream.lock());
        guard.take()
    };

    let old_stream = match old_stream {
        Some(s) => s,
        None => {
            throw_zerobus_exception(&mut env, "Stream already closed or recreated");
            return JObject::null();
        }
    };

    // Spawn async operation
    spawn_and_complete(
        future_ref,
        async move {
            let new_stream = sdk_arc.recreate_stream(&old_stream).await?;
            Ok(NativeStreamHandle::new(
                new_stream,
                record_type,
                client_id,
                client_secret,
            ))
        },
        |handle| handle.into_raw(),
    );

    future
}

/// Create a new Arrow stream.
///
/// # JNI Signature
/// ```java
/// private native CompletableFuture<Long> nativeCreateArrowStream(
///     long sdkHandle,
///     String tableName,
///     byte[] arrowSchema,
///     String clientId,
///     String clientSecret,
///     Object options
/// );
/// ```
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_ZerobusSdk_nativeCreateArrowStream<'local>(
    env: JNIEnv<'local>,
    _obj: JObject<'local>,
    sdk_handle: jlong,
    table_name: JString<'local>,
    arrow_schema: JByteArray<'local>,
    client_id: JString<'local>,
    client_secret: JString<'local>,
    options: JObject<'local>,
) -> JObject<'local> {
    Java_com_databricks_zerobus_ZerobusSdk_nativeCreateArrowStreamWithHeadersProvider(
        env,
        _obj,
        sdk_handle,
        table_name,
        arrow_schema,
        client_id,
        client_secret,
        JObject::null(),
        options,
    )
}

#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_ZerobusSdk_nativeCreateArrowStreamWithHeadersProvider<
    'local,
>(
    mut env: JNIEnv<'local>,
    _obj: JObject<'local>,
    sdk_handle: jlong,
    table_name: JString<'local>,
    arrow_schema: JByteArray<'local>,
    client_id: JString<'local>,
    client_secret: JString<'local>,
    headers_provider: JObject<'local>,
    options: JObject<'local>,
) -> JObject<'local> {
    let auth = match extract_oauth_or_headers_provider_auth(
        &mut env,
        &client_id,
        &client_secret,
        &headers_provider,
    ) {
        Ok(auth) => auth,
        Err(message) => {
            throw_zerobus_exception(&mut env, &message);
            return JObject::null();
        }
    };
    create_arrow_stream(env, sdk_handle, table_name, arrow_schema, auth, options)
}

/// Create a new Arrow stream authenticated with external-IdP federation (RFC 8693).
///
/// See `nativeCreateStreamWithFederatedAuth` for the federation parameters.
///
/// # JNI Signature
/// ```java
/// private native CompletableFuture<Long> nativeCreateArrowStreamWithFederatedAuth(
///     long sdkHandle,
///     String tableName,
///     byte[] arrowSchema,
///     IdpTokenSupplier idpTokenSupplier,
///     String federatedClientId,
///     String cacheIdentity,
///     Object options
/// );
/// ```
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_ZerobusSdk_nativeCreateArrowStreamWithFederatedAuth<
    'local,
>(
    mut env: JNIEnv<'local>,
    _obj: JObject<'local>,
    sdk_handle: jlong,
    table_name: JString<'local>,
    arrow_schema: JByteArray<'local>,
    idp_token_supplier: JObject<'local>,
    federated_client_id: JString<'local>,
    cache_identity: JString<'local>,
    options: JObject<'local>,
) -> JObject<'local> {
    let auth = match extract_federated_auth(
        &mut env,
        &idp_token_supplier,
        &federated_client_id,
        &cache_identity,
    ) {
        Ok(auth) => auth,
        Err(message) => {
            throw_zerobus_exception(&mut env, &message);
            return JObject::null();
        }
    };
    create_arrow_stream(env, sdk_handle, table_name, arrow_schema, auth, options)
}

fn create_arrow_stream<'local>(
    mut env: JNIEnv<'local>,
    sdk_handle: jlong,
    table_name: JString<'local>,
    arrow_schema: JByteArray<'local>,
    auth: JniStreamAuth,
    options: JObject<'local>,
) -> JObject<'local> {
    // Create the CompletableFuture to return
    let future = match create_completable_future(&mut env) {
        Ok(f) => f,
        Err(e) => {
            throw_zerobus_exception(&mut env, &format!("Failed to create future: {}", e));
            return JObject::null();
        }
    };

    // Create a global reference to the future
    let future_ref = match env.new_global_ref(&future) {
        Ok(r) => r,
        Err(e) => {
            throw_zerobus_exception(&mut env, &format!("Failed to create global ref: {}", e));
            return JObject::null();
        }
    };

    // Extract parameters
    let table_name: String = match env.get_string(&table_name) {
        Ok(s) => s.into(),
        Err(e) => {
            throw_zerobus_exception(&mut env, &format!("Invalid table name: {}", e));
            return JObject::null();
        }
    };

    // Extract Arrow schema bytes
    let schema_bytes: Vec<u8> = match env.convert_byte_array(arrow_schema) {
        Ok(bytes) => bytes,
        Err(e) => {
            throw_zerobus_exception(&mut env, &format!("Invalid Arrow schema: {}", e));
            return JObject::null();
        }
    };

    let extracted_options = if options.is_null() {
        None
    } else {
        match extract_arrow_stream_options(&mut env, &options) {
            Ok(opts) => Some(opts),
            Err(e) => {
                throw_zerobus_exception(&mut env, &format!("Invalid options: {}", e));
                return JObject::null();
            }
        }
    };

    // Get SDK handle
    let sdk = unsafe { NativeSdkHandle::borrow_from_raw(sdk_handle) };
    let sdk_arc = sdk.sdk.clone();

    let credentials = auth.stored_credentials();

    // Spawn async operation
    spawn_and_complete(
        future_ref,
        async move {
            // Deserialize Arrow schema from IPC format using StreamReader
            let reader = arrow_ipc::reader::StreamReader::try_new(&schema_bytes[..], None)
                .map_err(|e| {
                    databricks_zerobus_ingest_sdk::ZerobusError::InvalidArgument(format!(
                        "Failed to parse Arrow schema: {}",
                        e
                    ))
                })?;

            let schema = reader.schema();

            let builder = auth.apply(sdk_arc.stream_builder().table(table_name));
            let mut builder = builder.arrow(schema);
            if let Some(opts) = extracted_options {
                builder = apply_arrow_stream_options(builder, opts);
            }

            let stream = builder.build_arrow().await?;

            Ok(NativeArrowStreamHandle::new(
                stream,
                credentials.0,
                credentials.1,
            ))
        },
        |handle| handle.into_raw(),
    );

    future
}

/// Recreate an Arrow stream from a failed stream.
///
/// # JNI Signature
/// ```java
/// private native CompletableFuture<Long> nativeRecreateArrowStream(long sdkHandle, long streamHandle);
/// ```
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_ZerobusSdk_nativeRecreateArrowStream<'local>(
    mut env: JNIEnv<'local>,
    _obj: JObject<'local>,
    sdk_handle: jlong,
    stream_handle: jlong,
) -> JObject<'local> {
    // Create the CompletableFuture to return
    let future = match create_completable_future(&mut env) {
        Ok(f) => f,
        Err(e) => {
            throw_zerobus_exception(&mut env, &format!("Failed to create future: {}", e));
            return JObject::null();
        }
    };

    // Create a global reference to the future
    let future_ref = match env.new_global_ref(&future) {
        Ok(r) => r,
        Err(e) => {
            throw_zerobus_exception(&mut env, &format!("Failed to create global ref: {}", e));
            return JObject::null();
        }
    };

    // Get SDK and stream handles
    let sdk = unsafe { NativeSdkHandle::borrow_from_raw(sdk_handle) };
    let sdk_arc = sdk.sdk.clone();

    let stream = unsafe { NativeArrowStreamHandle::borrow_from_raw(stream_handle) };
    let client_id = stream.client_id.clone();
    let client_secret = stream.client_secret.clone();

    // Take the stream out of the handle for recreation
    let old_stream = {
        let mut guard = block_on(stream.stream.lock());
        guard.take()
    };

    let old_stream = match old_stream {
        Some(s) => s,
        None => {
            throw_zerobus_exception(&mut env, "Arrow stream already closed or recreated");
            return JObject::null();
        }
    };

    // Spawn async operation
    spawn_and_complete(
        future_ref,
        async move {
            let new_stream = sdk_arc.recreate_arrow_stream(&old_stream).await?;
            Ok(NativeArrowStreamHandle::new(
                new_stream,
                client_id,
                client_secret,
            ))
        },
        |handle| handle.into_raw(),
    );

    future
}
