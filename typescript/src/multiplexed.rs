// Multiplexed (mux) stream bindings (Beta).
//
// A multiplexed stream spreads records round-robin over N ordinary gRPC sub-streams. The
// public entry point is the JS `streamBuilder()...multiplexed(n).build()` in wrapper.js;
// it calls the internal `ZerobusSdk._createMultiplexedStream`, which lands in `create`.
//
// Ingest calls return a message ID: an opaque bigint that packs the sub-stream index and
// that sub-stream's offset. IDs of sub-streams 32-63 are negative, and IDs are not ordered.

use std::sync::{Arc, Mutex as StdMutex, PoisonError};

use napi::bindgen_prelude::*;
use napi::threadsafe_function::{
    ErrorStrategy, ThreadSafeCallContext, ThreadsafeFunction, ThreadsafeFunctionCallMode,
};
use napi::{Env, JsFunction, JsObject, JsUnknown};
use napi_derive::napi;
use tokio::sync::RwLock;

use databricks_zerobus_ingest_sdk::{
    AckCallback as RustAckCallback, EncodedBatch, MessageId,
    MultiplexedStream as RustMultiplexedStream,
};

use crate::{
    apply_format, apply_options, convert_js_to_record_payload, create_headers_tsfn,
    record_payload_to_buffer, resolve_format, static_headers_provider, JsHeadersProvider,
    RecordType, StreamConfigurationOptions, TableProperties, ZerobusSdk,
};

/// An event for the JS ack dispatcher in wrapper.js, called as
/// `(kind: 'ack' | 'error' | 'drained', messageId?: bigint, message?: string)`.
enum AckEvent {
    Ack(i64),
    Error(i64, String),
    /// Sent after a completed core close: every ack event is already queued ahead of it.
    Drained,
}

type AckTsfn = ThreadsafeFunction<AckEvent, ErrorStrategy::Fatal>;

/// Owns the ack TSFN in a releasable slot.
///
/// The core keeps the ack callback alive inside every sub-stream, so the TSFN must not live
/// as long as the `MultiplexedStream`: a close is final for callbacks even if the stream
/// object stays around (a failed close is cached by the core and may never succeed).
/// The slot is the only owner of the TSFN (napi releases it when the last clone drops), and
/// an empty slot makes every call a no-op.
///
/// The TSFN is unref'd while the stream is open, so an open, never-closed stream does not keep
/// Node alive. `close()` / `getUnacked*()` re-ref it until the drained marker is delivered.
struct AckAdapter {
    slot: StdMutex<Option<AckTsfn>>,
}

impl AckAdapter {
    fn new(env: &Env, dispatcher: JsFunction) -> Result<Self> {
        let mut tsfn: AckTsfn = dispatcher.create_threadsafe_function(
            0,
            |ctx: ThreadSafeCallContext<AckEvent>| -> Result<Vec<JsUnknown>> {
                let env = ctx.env;
                Ok(match ctx.value {
                    AckEvent::Ack(id) => vec![
                        env.create_string("ack")?.into_unknown(),
                        env.create_bigint_from_i64(id)?.into_unknown()?,
                    ],
                    AckEvent::Error(id, message) => vec![
                        env.create_string("error")?.into_unknown(),
                        env.create_bigint_from_i64(id)?.into_unknown()?,
                        env.create_string(&message)?.into_unknown(),
                    ],
                    AckEvent::Drained => vec![env.create_string("drained")?.into_unknown()],
                })
            },
        )?;
        tsfn.unref(env)?;
        Ok(Self {
            slot: StdMutex::new(Some(tsfn)),
        })
    }

    /// Queues an event without blocking the calling sub-stream worker (the queue is unbounded).
    fn send(&self, event: AckEvent) {
        let slot = self.slot.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(tsfn) = slot.as_ref() {
            tsfn.call(event, ThreadsafeFunctionCallMode::NonBlocking);
        }
    }

    /// Keeps Node alive until the drained marker is delivered. JS thread only.
    fn refer(&self, env: &Env) -> Result<()> {
        let mut slot = self.slot.lock().unwrap_or_else(PoisonError::into_inner);
        match slot.as_mut() {
            Some(tsfn) => tsfn.refer(env),
            None => Ok(()),
        }
    }

    /// Undoes `refer` when no close was started. JS thread only.
    fn unref(&self, env: &Env) -> Result<()> {
        let mut slot = self.slot.lock().unwrap_or_else(PoisonError::into_inner);
        match slot.as_mut() {
            Some(tsfn) => tsfn.unref(env),
            None => Ok(()),
        }
    }

    /// Sends the drained marker and releases the TSFN. Idempotent: a no-op once released.
    /// Call only after a completed core close, which has drained every ack callback.
    fn drain_and_release(&self) {
        // Take under a temporary guard; the marker call and the release (on drop) run unlocked.
        let tsfn = self
            .slot
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(tsfn) = tsfn {
            tsfn.call(AckEvent::Drained, ThreadsafeFunctionCallMode::NonBlocking);
        }
    }
}

/// The core's ack callback for one mux; called concurrently from every sub-stream worker.
struct MuxAckCallback(Arc<AckAdapter>);

impl RustAckCallback<MessageId> for MuxAckCallback {
    fn on_ack(&self, message_id: MessageId) {
        self.0.send(AckEvent::Ack(message_id.raw()));
    }

    fn on_error(&self, message_id: MessageId, error_message: &str) {
        self.0
            .send(AckEvent::Error(message_id.raw(), error_message.to_string()));
    }
}

/// Opens a multiplexed stream. Backs `ZerobusSdk._createMultiplexedStream`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn create(
    sdk: &ZerobusSdk,
    env: Env,
    table_properties: TableProperties,
    client_id: String,
    client_secret: String,
    stream_count: f64,
    options: Option<StreamConfigurationOptions>,
    headers_provider: Option<JsHeadersProvider>,
    ack_dispatcher: Option<JsFunction>,
) -> Result<JsObject> {
    let opts = options.unwrap_or_default();
    let format = resolve_format(&table_properties, opts.record_type)?;
    let record_type = format.record_type;

    let headers_tsfn = match headers_provider {
        Some(JsHeadersProvider {
            get_headers_callback,
        }) => Some(create_headers_tsfn(get_headers_callback)?),
        None => None,
    };
    let ack = match ack_dispatcher {
        Some(dispatcher) => Some(Arc::new(AckAdapter::new(&env, dispatcher)?)),
        None => None,
    };
    // Taken as f64 so no JS number wraps or truncates into a valid count; anything that is not
    // an integer in 1..=64 becomes 0, which the core's range check rejects.
    let stream_count = if stream_count.fract() == 0.0 && (1.0..=64.0).contains(&stream_count) {
        stream_count as usize
    } else {
        0
    };

    let sdk = sdk.inner.clone();
    let table_name = table_properties.table_name;

    env.execute_tokio_future(
        async move {
            let builder = sdk.stream_builder().table(table_name);
            // OAuth goes through the core so all sub-streams share the SDK's token cache.
            let builder = match headers_tsfn {
                Some(tsfn) => builder.headers_provider(static_headers_provider(tsfn).await?),
                None => builder.oauth(client_id, client_secret),
            };
            let mut builder = apply_format(apply_options(builder, &opts), format)?;
            if let Some(ack) = &ack {
                // The callback only queues to the JS thread, so close can wait for all of
                // them; the default cutoff would cut the drain short before the marker.
                builder = builder
                    .callback_max_wait_time_ms(None)
                    .multiplexed_ack_callback(Arc::new(MuxAckCallback(ack.clone())));
            }

            let mux = builder
                .multiplexed(stream_count)
                .build()
                .await
                .map_err(|e| {
                    Error::from_reason(format!("Failed to create multiplexed stream: {}", e))
                })?;

            Ok(ZerobusMultiplexedStream {
                inner: Arc::new(RwLock::new(Some(mux))),
                record_type,
                ack,
            })
        },
        |_env, stream| Ok(stream),
    )
}

/// A multiplexed stream (Beta): N sub-streams to one table, fed round-robin.
///
/// Records keep their order within a sub-stream, but there is no order across sub-streams:
/// message IDs and acknowledgments arrive in no global order. Created through
/// `sdk.streamBuilder()...multiplexed(n).build()`.
///
/// # Example
///
/// ```typescript
/// const stream = await sdk.streamBuilder()
///   .table(tableName)
///   .oauth(clientId, clientSecret)
///   .compiledProto(descriptorBase64)
///   .multiplexed(4)
///   .build();
/// for (const record of records) {
///   await stream.ingestRecord(record); // resolves once queued
/// }
/// await stream.flush(); // confirms everything ingested so far
/// await stream.close();
/// ```
#[napi]
pub struct ZerobusMultiplexedStream {
    /// Read lock for ingest, wait and flush (they run concurrently across sub-streams);
    /// write lock for close and the unacked getters, which need `&mut`. `None` after a
    /// successful close.
    inner: Arc<RwLock<Option<RustMultiplexedStream>>>,
    /// Record format, used to coerce ingest payloads.
    record_type: RecordType,
    ack: Option<Arc<AckAdapter>>,
}

#[napi]
impl ZerobusMultiplexedStream {
    /// Ingests a single record into the next sub-stream.
    ///
    /// Resolves to the record's message ID as soon as the record is queued; it does not wait
    /// for acknowledgment. Ingest in a loop and call `flush()` once to confirm.
    #[napi(ts_return_type = "Promise<bigint>")]
    pub fn ingest_record(&self, env: Env, payload: Unknown) -> Result<JsObject> {
        let record_payload = convert_js_to_record_payload(&env, payload, self.record_type)?;
        let stream = self.inner.clone();

        env.execute_tokio_future(
            async move {
                let guard = stream.read().await;
                let mux = guard
                    .as_ref()
                    .ok_or_else(|| Error::from_reason("Stream has been closed"))?;
                let message_id = mux
                    .ingest_record(record_payload)
                    .await
                    .map_err(|e| Error::from_reason(format!("Failed to ingest record: {}", e)))?;
                Ok(message_id.raw())
            },
            |_env, message_id| Ok(BigInt::from(message_id)),
        )
    }

    /// Ingests records as one batch on a single sub-stream.
    ///
    /// Resolves to the batch's message ID once queued, or `null` for an empty batch.
    #[napi(ts_return_type = "Promise<bigint | null>")]
    pub fn ingest_records(&self, env: Env, records: Vec<Unknown>) -> Result<JsObject> {
        let record_type = self.record_type;
        let record_payloads = records
            .into_iter()
            .map(|payload| convert_js_to_record_payload(&env, payload, record_type))
            .collect::<Result<Vec<_>>>()?;
        let stream = self.inner.clone();

        env.execute_tokio_future(
            async move {
                let guard = stream.read().await;
                let mux = guard
                    .as_ref()
                    .ok_or_else(|| Error::from_reason("Stream has been closed"))?;
                let message_id = mux
                    .ingest_records(record_payloads)
                    .await
                    .map_err(|e| Error::from_reason(format!("Failed to ingest batch: {}", e)))?;
                Ok(message_id.map(|id| id.raw()))
            },
            |_env, message_id| Ok(message_id.map(BigInt::from)),
        )
    }

    /// Waits until the record or batch behind `messageId` is acknowledged.
    ///
    /// IDs are not ordered, so this confirms only that record or batch, not earlier ones.
    /// Prefer `flush()` to confirm everything; pass only IDs returned by this stream.
    #[napi(ts_return_type = "Promise<void>")]
    pub fn wait_for_message_id(&self, env: Env, message_id: BigInt) -> Result<JsObject> {
        let (raw, lossless) = message_id.get_i64();
        if !lossless {
            return Err(Error::from_reason(
                "messageId exceeds i64 range; cannot be represented without loss",
            ));
        }
        let stream = self.inner.clone();

        env.execute_tokio_future(
            async move {
                let guard = stream.read().await;
                let mux = guard
                    .as_ref()
                    .ok_or_else(|| Error::from_reason("Stream has been closed"))?;
                mux.wait_for_message_id(MessageId::from_raw(raw))
                    .await
                    .map_err(|e| {
                        Error::from_reason(format!("Failed to wait for message ID: {}", e))
                    })
            },
            |_env, _| Ok(()),
        )
    }

    /// Waits until every record queued so far, on every sub-stream, is acknowledged.
    #[napi]
    pub async fn flush(&self) -> Result<()> {
        let guard = self.inner.read().await;
        let mux = guard
            .as_ref()
            .ok_or_else(|| Error::from_reason("Stream has been closed"))?;
        mux.flush()
            .await
            .map_err(|e| Error::from_reason(format!("Failed to flush stream: {}", e)))
    }

    /// Flushes and closes every sub-stream.
    ///
    /// Waits for in-flight ingest, wait and flush calls first. On error the stream keeps its
    /// records: use `getUnackedRecords()` / `getUnackedBatches()` to recover them.
    #[napi(ts_return_type = "Promise<void>")]
    pub fn close(&self, env: Env) -> Result<JsObject> {
        let stream = self.inner.clone();
        let ack = self.ack.clone();

        // Ref before spawning: a close that finishes first must not leave queued callbacks
        // and the drained marker behind an unref'd TSFN, or Node can exit before they run.
        self.refer_ack(&env)?;
        let promise = env.execute_tokio_future(
            async move {
                let mut guard = stream.write().await;
                let result = match guard.as_mut() {
                    Some(mux) => mux.close().await,
                    None => Ok(()),
                };
                if result.is_ok() {
                    guard.take();
                }
                drop(guard);
                release_ack(ack);
                result.map_err(|e| Error::from_reason(format!("Failed to close stream: {}", e)))
            },
            |_env, _| Ok(()),
        );
        if promise.is_err() {
            self.unref_ack(&env);
        }
        promise
    }

    /// Whether the stream is closed: by `close()`, by a sub-stream failure, or while a
    /// close or unacked getter is in progress.
    #[napi(getter)]
    pub fn is_closed(&self) -> bool {
        match self.inner.try_read() {
            Ok(guard) => match guard.as_ref() {
                Some(mux) => mux.is_closed(),
                None => true,
            },
            Err(_) => true,
        }
    }

    /// Returns the records that were ingested but not acknowledged. Closes the stream first.
    #[napi(ts_return_type = "Promise<Buffer[]>")]
    pub fn get_unacked_records(&self, env: Env) -> Result<JsObject> {
        let stream = self.inner.clone();
        let ack = self.ack.clone();

        // Ref before spawning: a close that finishes first must not leave queued callbacks
        // and the drained marker behind an unref'd TSFN, or Node can exit before they run.
        self.refer_ack(&env)?;
        let promise = env.execute_tokio_future(
            async move {
                let batches = unacked_batches(&stream, "Failed to get unacked records").await;
                release_ack(ack);
                batches?
                    .into_iter()
                    .flatten()
                    .map(record_payload_to_buffer)
                    .collect::<Result<Vec<Buffer>>>()
            },
            |_env, records| Ok(records),
        );
        if promise.is_err() {
            self.unref_ack(&env);
        }
        promise
    }

    /// Returns the unacknowledged records grouped by their original batches (a single-record
    /// ingest is a 1-element batch). Closes the stream first.
    #[napi(ts_return_type = "Promise<Buffer[][]>")]
    pub fn get_unacked_batches(&self, env: Env) -> Result<JsObject> {
        let stream = self.inner.clone();
        let ack = self.ack.clone();

        // Ref before spawning: a close that finishes first must not leave queued callbacks
        // and the drained marker behind an unref'd TSFN, or Node can exit before they run.
        self.refer_ack(&env)?;
        let promise = env.execute_tokio_future(
            async move {
                let batches = unacked_batches(&stream, "Failed to get unacked batches").await;
                release_ack(ack);
                batches?
                    .into_iter()
                    .map(|batch| {
                        batch
                            .into_iter()
                            .map(record_payload_to_buffer)
                            .collect::<Result<Vec<Buffer>>>()
                    })
                    .collect::<Result<Vec<_>>>()
            },
            |_env, batches| Ok(batches),
        );
        if promise.is_err() {
            self.unref_ack(&env);
        }
        promise
    }
}

impl ZerobusMultiplexedStream {
    fn refer_ack(&self, env: &Env) -> Result<()> {
        match &self.ack {
            Some(ack) => ack.refer(env),
            None => Ok(()),
        }
    }

    /// Best effort: the spawn error is what the caller needs to see.
    fn unref_ack(&self, env: &Env) {
        if let Some(ack) = &self.ack {
            let _ = ack.unref(env);
        }
    }
}

/// Closes the mux (the core ignores close errors here) and takes its unacked batches.
async fn unacked_batches(
    stream: &RwLock<Option<RustMultiplexedStream>>,
    error_prefix: &str,
) -> Result<Vec<EncodedBatch>> {
    let mut guard = stream.write().await;
    let mux = guard
        .as_mut()
        .ok_or_else(|| Error::from_reason("Stream has been closed"))?;
    mux.get_unacked_batches()
        .await
        .map_err(|e| Error::from_reason(format!("{}: {}", error_prefix, e)))
}

/// Runs after every completed core close, successful or not: the core has drained all ack
/// callbacks on both paths.
fn release_ack(ack: Option<Arc<AckAdapter>>) {
    if let Some(ack) = ack {
        ack.drain_and_release();
    }
}
