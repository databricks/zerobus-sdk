//! JNI bindings for multiplexed streams (Beta).
//!
//! A multiplexed stream distributes ingestion round-robin across `streamCount`
//! gRPC sub-streams to raise aggregate throughput. Ingest calls return a message
//! ID: an opaque `long` that packs the sub-stream index and that sub-stream's
//! offset. Pass it back to `waitForMessageId`; do not compare or order message
//! IDs, and only use them with the stream that produced them. Message IDs of
//! sub-streams 32-63 are negative, so no ingest return value is reserved as an
//! error sentinel; failures are always thrown.

use crate::async_bridge::{create_completable_future, spawn_and_complete};
use crate::callbacks::JavaAckCallback;
use crate::errors::{guard, throw_from_zerobus_error, throw_zerobus_exception};
use crate::runtime::block_on;
use crate::sdk::{
    extract_federated_auth, extract_oauth_or_headers_provider_auth, JniStreamAuth, NativeSdkHandle,
    StreamCreateArgs,
};
use crate::stream::{batches_to_java_list, encoded_record_from, encoded_records_from_list};
use databricks_zerobus_ingest_sdk::{AckCallback, MessageId, MultiplexedStream, RecordType};
use jni::objects::{JByteArray, JClass, JObject, JString};
use jni::sys::{jboolean, jint, jlong, JNI_FALSE, JNI_TRUE};
use jni::JNIEnv;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Native multiplexed stream handle stored in Java.
///
/// Ingest, flush, and wait need only `&MultiplexedStream` and may run
/// concurrently, while `close` and the unacked getters need `&mut`. The lock
/// lets the former share the mux and makes the latter wait for them.
///
/// The Java stream is thread-safe, so these calls do arrive from several threads
/// at once. Java keeps the handle alive during every native call (a read lock in
/// `BaseZerobusMultiplexedStream`) and calls `nativeDestroy` only under its write
/// lock, so no call can use a freed handle.
pub struct NativeMultiplexedStreamHandle {
    mux: RwLock<MultiplexedStream>,
    /// Record format, fixed at creation; the ingest paths read it instead of taking it per call.
    record_type: RecordType,
}

impl NativeMultiplexedStreamHandle {
    fn new(mux: MultiplexedStream, record_type: RecordType) -> Self {
        Self {
            mux: RwLock::new(mux),
            record_type,
        }
    }

    fn into_raw(self) -> jlong {
        Box::into_raw(Box::new(self)) as jlong
    }

    /// # Safety
    ///
    /// The pointer must have been created by `into_raw` and not freed.
    unsafe fn from_raw(ptr: jlong) -> Box<Self> {
        Box::from_raw(ptr as *mut Self)
    }

    /// # Safety
    ///
    /// The pointer must have been created by `into_raw` and not freed.
    unsafe fn borrow_from_raw(ptr: jlong) -> &'static Self {
        &*(ptr as *const Self)
    }
}

/// Bridges a Java `AckCallback` to the mux's `AckCallback<MessageId>`; the Java
/// callback's `long` argument receives `MessageId::raw()`.
struct JavaMultiplexedAckCallback(JavaAckCallback);

impl AckCallback<MessageId> for JavaMultiplexedAckCallback {
    fn on_ack(&self, message_id: MessageId) {
        self.0.on_ack(message_id.raw());
    }

    fn on_error(&self, message_id: MessageId, error_message: &str) {
        self.0.on_error(message_id.raw(), error_message);
    }
}

/// Create a multiplexed JSON, protobuf, or Avro stream (Beta) authenticated with OAuth or a
/// `HeadersProvider`.
///
/// # JNI Signature
/// ```java
/// private native CompletableFuture<Long> nativeCreateMultiplexedStream(
///     long sdkHandle,
///     String tableName,
///     byte[] descriptorProto,
///     String avroSchemaJson,
///     String clientId,
///     String clientSecret,
///     HeadersProvider headersProvider,
///     Object options,
///     int formatCode,
///     AckCallback ackCallback,
///     int streamCount
/// );
/// ```
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_ZerobusSdk_nativeCreateMultiplexedStream<
    'local,
>(
    env: JNIEnv<'local>,
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
    ack_callback: JObject<'local>,
    stream_count: jint,
) -> JObject<'local> {
    guard(env, |mut env| {
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
        create_multiplexed_stream(
            env,
            sdk_handle,
            table_name,
            descriptor_proto,
            avro_schema,
            auth,
            options,
            format_code,
            ack_callback,
            stream_count,
        )
    })
}

/// Create a multiplexed JSON, protobuf, or Avro stream (Beta) authenticated with external-IdP
/// federation.
///
/// # JNI Signature
/// ```java
/// private native CompletableFuture<Long> nativeCreateMultiplexedStreamWithFederatedAuth(
///     long sdkHandle,
///     String tableName,
///     byte[] descriptorProto,
///     String avroSchemaJson,
///     IdpTokenSupplier idpTokenSupplier,
///     String federatedClientId,
///     String cacheIdentity,
///     Object options,
///     int formatCode,
///     AckCallback ackCallback,
///     int streamCount
/// );
/// ```
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_ZerobusSdk_nativeCreateMultiplexedStreamWithFederatedAuth<
    'local,
>(
    env: JNIEnv<'local>,
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
    ack_callback: JObject<'local>,
    stream_count: jint,
) -> JObject<'local> {
    guard(env, |mut env| {
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
        create_multiplexed_stream(
            env,
            sdk_handle,
            table_name,
            descriptor_proto,
            avro_schema,
            auth,
            options,
            format_code,
            ack_callback,
            stream_count,
        )
    })
}

// Takes the create natives' remaining Java arguments one-to-one.
#[allow(clippy::too_many_arguments)]
fn create_multiplexed_stream<'local>(
    mut env: JNIEnv<'local>,
    sdk_handle: jlong,
    table_name: JString<'local>,
    descriptor_proto: JByteArray<'local>,
    avro_schema: JString<'local>,
    auth: JniStreamAuth,
    options: JObject<'local>,
    format_code: jint,
    ack_callback: JObject<'local>,
    stream_count: jint,
) -> JObject<'local> {
    let future = match create_completable_future(&mut env) {
        Ok(f) => f,
        Err(e) => {
            throw_zerobus_exception(&mut env, &format!("Failed to create future: {}", e));
            return JObject::null();
        }
    };

    let future_ref = match env.new_global_ref(&future) {
        Ok(r) => r,
        Err(e) => {
            throw_zerobus_exception(&mut env, &format!("Failed to create global ref: {}", e));
            return JObject::null();
        }
    };

    let args = match StreamCreateArgs::extract(
        &mut env,
        table_name,
        descriptor_proto,
        avro_schema,
        auth,
        options,
        format_code,
    ) {
        Ok(args) => args,
        Err(msg) => {
            throw_zerobus_exception(&mut env, &msg);
            return JObject::null();
        }
    };

    let ack_callback = if ack_callback.is_null() {
        None
    } else {
        match env.new_global_ref(&ack_callback) {
            Ok(callback_ref) => Some(JavaMultiplexedAckCallback(JavaAckCallback::new(
                callback_ref,
            ))),
            Err(e) => {
                throw_zerobus_exception(&mut env, &format!("Failed to retain ack callback: {}", e));
                return JObject::null();
            }
        }
    };

    // A negative count maps to 0, which the SDK rejects as out of range.
    let stream_count = usize::try_from(stream_count).unwrap_or(0);

    let sdk = unsafe { NativeSdkHandle::borrow_from_raw(sdk_handle) };
    let sdk_arc = sdk.sdk.clone();

    spawn_and_complete(
        future_ref,
        async move {
            let (record_type, mut builder) = args.configure(&sdk_arc)?;
            if let Some(callback) = ack_callback {
                builder = builder.multiplexed_ack_callback(Arc::new(callback));
            }
            let mux = builder.multiplexed(stream_count).build().await?;
            Ok(NativeMultiplexedStreamHandle::new(mux, record_type))
        },
        |handle| handle.into_raw(),
    );

    future
}

/// Destroy a multiplexed stream handle.
///
/// # JNI Signature
/// ```java
/// private static native void nativeDestroy(long handle);
/// ```
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_BaseZerobusMultiplexedStream_nativeDestroy<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    guard(env, |env| native_destroy(env, _class, handle))
}

fn native_destroy<'local>(_env: JNIEnv<'local>, _class: JClass<'local>, handle: jlong) {
    if handle != 0 {
        // Safety: handle was created by nativeCreateMultiplexedStream
        unsafe {
            let _ = NativeMultiplexedStreamHandle::from_raw(handle);
        }
    }
}

/// Ingest a record and return its message ID once it is queued (blocking until enqueued).
///
/// # JNI Signature
/// ```java
/// private native long nativeIngestRecord(long handle, byte[] payload);
/// ```
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_BaseZerobusMultiplexedStream_nativeIngestRecord<
    'local,
>(
    env: JNIEnv<'local>,
    _obj: JObject<'local>,
    handle: jlong,
    payload: JByteArray<'local>,
) -> jlong {
    guard(env, |env| native_ingest_record(env, _obj, handle, payload))
}

fn native_ingest_record<'local>(
    mut env: JNIEnv<'local>,
    _obj: JObject<'local>,
    handle: jlong,
    payload: JByteArray<'local>,
) -> jlong {
    let bytes: Vec<u8> = match env.convert_byte_array(payload) {
        Ok(b) => b,
        Err(e) => {
            throw_zerobus_exception(&mut env, &format!("Invalid payload: {}", e));
            return 0;
        }
    };

    let stream_handle = unsafe { NativeMultiplexedStreamHandle::borrow_from_raw(handle) };

    let record = match encoded_record_from(stream_handle.record_type, bytes) {
        Ok(r) => r,
        Err(msg) => {
            throw_zerobus_exception(&mut env, &msg);
            return 0;
        }
    };

    let result = block_on(async {
        let mux = stream_handle.mux.read().await;
        mux.ingest_record(record).await
    });

    match result {
        Ok(message_id) => message_id.raw(),
        Err(e) => {
            throw_from_zerobus_error(&mut env, &e);
            0
        }
    }
}

/// Ingest a batch of records into one sub-stream and return the batch's message ID, or 0 for an
/// empty batch, which sends nothing but still fails on a closed or failed stream.
///
/// # JNI Signature
/// ```java
/// private native long nativeIngestRecords(long handle, List<byte[]> payloads);
/// ```
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_BaseZerobusMultiplexedStream_nativeIngestRecords<
    'local,
>(
    env: JNIEnv<'local>,
    _obj: JObject<'local>,
    handle: jlong,
    payloads: JObject<'local>,
) -> jlong {
    guard(env, |env| {
        native_ingest_records(env, _obj, handle, payloads)
    })
}

fn native_ingest_records<'local>(
    mut env: JNIEnv<'local>,
    _obj: JObject<'local>,
    handle: jlong,
    payloads: JObject<'local>,
) -> jlong {
    let stream_handle = unsafe { NativeMultiplexedStreamHandle::borrow_from_raw(handle) };

    let records = match encoded_records_from_list(&mut env, &payloads, stream_handle.record_type) {
        Ok(r) => r,
        Err(msg) => {
            throw_zerobus_exception(&mut env, &msg);
            return 0;
        }
    };

    let result = block_on(async {
        let mux = stream_handle.mux.read().await;
        mux.ingest_records(records).await
    });

    match result {
        Ok(Some(message_id)) => message_id.raw(),
        // An empty batch on an open stream; Java knows the list was empty and ignores the value.
        Ok(None) => 0,
        Err(e) => {
            throw_from_zerobus_error(&mut env, &e);
            0
        }
    }
}

/// Wait for the record or batch behind a message ID to be acknowledged (blocking).
///
/// # JNI Signature
/// ```java
/// private native void nativeWaitForMessageId(long handle, long messageId);
/// ```
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_BaseZerobusMultiplexedStream_nativeWaitForMessageId<
    'local,
>(
    env: JNIEnv<'local>,
    _obj: JObject<'local>,
    handle: jlong,
    message_id: jlong,
) {
    guard(env, |env| {
        native_wait_for_message_id(env, _obj, handle, message_id)
    })
}

fn native_wait_for_message_id<'local>(
    mut env: JNIEnv<'local>,
    _obj: JObject<'local>,
    handle: jlong,
    message_id: jlong,
) {
    let stream_handle = unsafe { NativeMultiplexedStreamHandle::borrow_from_raw(handle) };

    let result = block_on(async {
        let mux = stream_handle.mux.read().await;
        mux.wait_for_message_id(MessageId::from_raw(message_id))
            .await
    });

    if let Err(e) = result {
        throw_from_zerobus_error(&mut env, &e);
    }
}

/// Flush all pending records on every sub-stream (blocking).
///
/// # JNI Signature
/// ```java
/// private native void nativeFlush(long handle);
/// ```
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_BaseZerobusMultiplexedStream_nativeFlush<
    'local,
>(
    env: JNIEnv<'local>,
    _obj: JObject<'local>,
    handle: jlong,
) {
    guard(env, |env| native_flush(env, _obj, handle))
}

fn native_flush<'local>(mut env: JNIEnv<'local>, _obj: JObject<'local>, handle: jlong) {
    let stream_handle = unsafe { NativeMultiplexedStreamHandle::borrow_from_raw(handle) };

    let result = block_on(async {
        let mux = stream_handle.mux.read().await;
        mux.flush().await
    });

    if let Err(e) = result {
        throw_from_zerobus_error(&mut env, &e);
    }
}

/// Flush and close every sub-stream (blocking).
///
/// # JNI Signature
/// ```java
/// private native void nativeClose(long handle);
/// ```
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_BaseZerobusMultiplexedStream_nativeClose<
    'local,
>(
    env: JNIEnv<'local>,
    _obj: JObject<'local>,
    handle: jlong,
) {
    guard(env, |env| native_close(env, _obj, handle))
}

fn native_close<'local>(mut env: JNIEnv<'local>, _obj: JObject<'local>, handle: jlong) {
    let stream_handle = unsafe { NativeMultiplexedStreamHandle::borrow_from_raw(handle) };

    let result = block_on(async {
        let mut mux = stream_handle.mux.write().await;
        mux.close().await
    });

    if let Err(e) = result {
        throw_from_zerobus_error(&mut env, &e);
    }
}

/// Check if the multiplexed stream is closed, by `close` or by a terminal sub-stream failure.
///
/// # JNI Signature
/// ```java
/// private native boolean nativeIsClosed(long handle);
/// ```
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_BaseZerobusMultiplexedStream_nativeIsClosed<
    'local,
>(
    env: JNIEnv<'local>,
    _obj: JObject<'local>,
    handle: jlong,
) -> jboolean {
    guard(env, |env| native_is_closed(env, _obj, handle))
}

fn native_is_closed<'local>(
    _env: JNIEnv<'local>,
    _obj: JObject<'local>,
    handle: jlong,
) -> jboolean {
    let stream_handle = unsafe { NativeMultiplexedStreamHandle::borrow_from_raw(handle) };

    let is_closed = block_on(async { stream_handle.mux.read().await.is_closed() });

    if is_closed {
        JNI_TRUE
    } else {
        JNI_FALSE
    }
}

/// Get unacked batches from every sub-stream as a list of EncodedBatch objects.
///
/// Closes the mux first so the result is complete.
///
/// # JNI Signature
/// ```java
/// private native List<EncodedBatch> nativeGetUnackedBatches(long handle);
/// ```
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_BaseZerobusMultiplexedStream_nativeGetUnackedBatches<
    'local,
>(
    env: JNIEnv<'local>,
    _obj: JObject<'local>,
    handle: jlong,
) -> JObject<'local> {
    guard(env, |env| native_get_unacked_batches(env, _obj, handle))
}

fn native_get_unacked_batches<'local>(
    mut env: JNIEnv<'local>,
    _obj: JObject<'local>,
    handle: jlong,
) -> JObject<'local> {
    let stream_handle = unsafe { NativeMultiplexedStreamHandle::borrow_from_raw(handle) };

    let result = block_on(async {
        let mut mux = stream_handle.mux.write().await;
        mux.get_unacked_batches().await
    });

    match result {
        Ok(batches) => batches_to_java_list(&mut env, batches),
        Err(e) => {
            throw_from_zerobus_error(&mut env, &e);
            JObject::null()
        }
    }
}
