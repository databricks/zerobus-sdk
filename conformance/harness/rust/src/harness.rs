//! `TestHarness` service: maps each contract RPC onto the Rust SDK.
//!
//! Supported: `GRPC` with `PROTO`/`AVRO`, and `ARROW_FLIGHT` with `ARROW`. `GRPC` with
//! `JSON` is not implemented by this harness yet (see [`check_slice_supported`]).
//!
//! Pure pass-through: each RPC calls the SDK method of the same meaning and reports its
//! result. The harness adds no retries, timeouts, sleeps, or error rewriting.
//!
//! Errors are split by origin:
//! - An SDK rejection (`ZerobusError`) is an OK response carrying an [`SdkError`] with the
//!   SDK's `to_string()` and `is_retryable()`.
//! - Anything else is a harness fault and a non-OK gRPC `Status`: an unknown `stream_id`, an
//!   unimplemented or invalid protocol/format pairing, input the harness cannot convert (a
//!   malformed record or schema from the orchestrator), or an empty batch.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use apache_avro::Schema as AvroSchema;
use databricks_zerobus_ingest_sdk::{
    ArrowSchema, MessageDescriptor, OffsetId, StreamBuilder, ZerobusArrowStream, ZerobusError,
    ZerobusSdk, ZerobusStream,
};
use tokio::sync::{watch, RwLock};
use tonic::{Request, Response, Status};
use tracing::info;

use crate::pb::test_harness_server::TestHarness;
use crate::pb::{
    ActionResponse, AwaitRequest, AwaitResponse, CloseRequest, CreateStreamRequest,
    CreateStreamResponse, Empty, FlushRequest, IngestRecordRequest, IngestRecordResponse,
    IngestRecordsRequest, IngestRecordsResponse, Protocol, RecordFormat, SdkError,
};
use crate::records;

/// The writer schema a gRPC stream's records are built against, by format.
enum RecordSchema {
    Proto(MessageDescriptor),
    Avro(AvroSchema),
}

/// An open SDK stream. The gRPC (row) and Arrow Flight (columnar) streams are distinct
/// SDK types, so each variant also carries the schema its records are built against.
enum StreamKind {
    Grpc {
        stream: ZerobusStream,
        schema: RecordSchema,
    },
    Arrow {
        stream: ZerobusArrowStream,
        schema: Arc<ArrowSchema>,
    },
}

impl StreamKind {
    /// Queues one record. The outer `Err` is a harness fault (conversion); the inner
    /// `Err` is the SDK's rejection.
    async fn ingest_one(
        &self,
        record_json: &str,
    ) -> Result<Result<OffsetId, ZerobusError>, Status> {
        Ok(match self {
            StreamKind::Grpc {
                stream,
                schema: RecordSchema::Proto(descriptor),
            } => {
                let message = records::record_from_json(descriptor, record_json)
                    .map_err(Status::invalid_argument)?;
                stream
                    .ingest_record_offset(records::sdk_input(message))
                    .await
            }
            StreamKind::Grpc {
                stream,
                schema: RecordSchema::Avro(schema),
            } => {
                let value = records::avro_value_from_json(schema, record_json)
                    .map_err(Status::invalid_argument)?;
                stream
                    .ingest_record_offset(records::avro_sdk_input(value))
                    .await
            }
            StreamKind::Arrow { stream, schema } => {
                let batch =
                    records::record_batch_from_json(schema, std::slice::from_ref(&record_json))
                        .map_err(Status::invalid_argument)?;
                stream.ingest_batch(batch).await
            }
        })
    }

    /// Queues a batch in one SDK call. `Ok(None)` is the SDK's answer for an empty batch.
    async fn ingest_all(
        &self,
        record_json: &[String],
    ) -> Result<Result<Option<OffsetId>, ZerobusError>, Status> {
        Ok(match self {
            StreamKind::Grpc {
                stream,
                schema: RecordSchema::Proto(descriptor),
            } => {
                let batch = record_json
                    .iter()
                    .map(|json| records::record_from_json(descriptor, json).map(records::sdk_input))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(Status::invalid_argument)?;
                stream.ingest_records_offset(batch).await
            }
            StreamKind::Grpc {
                stream,
                schema: RecordSchema::Avro(schema),
            } => {
                let batch = record_json
                    .iter()
                    .map(|json| {
                        records::avro_value_from_json(schema, json).map(records::avro_sdk_input)
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(Status::invalid_argument)?;
                stream.ingest_records_offset(batch).await
            }
            StreamKind::Arrow { stream, schema } => {
                let batch = records::record_batch_from_json(schema, record_json)
                    .map_err(Status::invalid_argument)?;
                stream.ingest_batch(batch).await.map(Some)
            }
        })
    }

    async fn wait_for_offset(&self, offset: OffsetId) -> Result<(), ZerobusError> {
        match self {
            StreamKind::Grpc { stream, .. } => stream.wait_for_offset(offset).await,
            StreamKind::Arrow { stream, .. } => stream.wait_for_offset(offset).await,
        }
    }

    async fn flush(&self) -> Result<(), ZerobusError> {
        match self {
            StreamKind::Grpc { stream, .. } => stream.flush().await,
            StreamKind::Arrow { stream, .. } => stream.flush().await,
        }
    }

    /// The SDK's close takes `&mut self`, so this needs the entry's write guard.
    async fn close(&mut self) -> Result<(), ZerobusError> {
        match self {
            StreamKind::Grpc { stream, .. } => stream.close().await,
            StreamKind::Arrow { stream, .. } => stream.close().await,
        }
    }
}

/// One SDK stream plus the SDK that created it (kept alive alongside the stream).
struct StreamEntry {
    _sdk: ZerobusSdk,
    kind: StreamKind,
}

type StreamMap = HashMap<String, Arc<RwLock<StreamEntry>>>;

pub struct HarnessService {
    /// `stream_id -> entry`. The std mutex guards only the map and is never held across
    /// an await. Each entry has its own async RwLock: ingest, wait, and flush take a read
    /// guard, since the SDK takes `&self` for them; close takes the write guard, since the
    /// SDK takes `&mut self` for it.
    streams: Mutex<StreamMap>,
    next_stream_id: AtomicU64,
    shutdown: watch::Sender<bool>,
}

impl HarnessService {
    pub fn new(shutdown: watch::Sender<bool>) -> Self {
        Self {
            streams: Mutex::new(HashMap::new()),
            next_stream_id: AtomicU64::new(0),
            shutdown,
        }
    }

    fn streams(&self) -> Result<MutexGuard<'_, StreamMap>, Status> {
        self.streams
            .lock()
            .map_err(|_| Status::internal("stream map poisoned"))
    }

    /// The only place that reports an unknown `stream_id`.
    fn entry(&self, stream_id: &str) -> Result<Arc<RwLock<StreamEntry>>, Status> {
        self.streams()?
            .get(stream_id)
            .cloned()
            .ok_or_else(|| Status::not_found(format!("unknown stream_id '{stream_id}'")))
    }
}

/// Reject the protocol/format combinations this harness does not accept. Only the GRPC +
/// JSON arm is unimplemented harness work; the others are malformed orchestrator input.
#[allow(clippy::result_large_err)]
fn check_slice_supported(protocol: Protocol, format: RecordFormat) -> Result<(), Status> {
    match (protocol, format) {
        (Protocol::Grpc, RecordFormat::Proto) => Ok(()),
        (Protocol::Grpc, RecordFormat::Avro) => Ok(()),
        (Protocol::ArrowFlight, RecordFormat::Arrow) => Ok(()),
        (Protocol::Grpc, RecordFormat::Json) => Err(Status::unimplemented(
            "TODO(conformance): GRPC + JSON not implemented in the Rust harness yet",
        )),
        (Protocol::Grpc, RecordFormat::Arrow) => Err(Status::invalid_argument(
            "GRPC + ARROW is not a valid pairing; ARROW rides ARROW_FLIGHT",
        )),
        (Protocol::ArrowFlight, _) => Err(Status::invalid_argument(
            "ARROW_FLIGHT carries only the ARROW format",
        )),
        _ => Err(Status::invalid_argument(
            "protocol and format must be specified",
        )),
    }
}

/// Convert an SDK rejection into the contract's `SdkError`, keeping the SDK's own
/// retry classification.
fn sdk_error(err: &ZerobusError) -> SdkError {
    SdkError {
        message: err.to_string(),
        retryable: err.is_retryable(),
    }
}

fn sdk_error_opt(result: Result<(), ZerobusError>) -> Option<SdkError> {
    result.err().map(|err| sdk_error(&err))
}

/// The `(offset, error)` pair both ingest responses carry. A rejected record has no offset.
fn offset_parts(result: Result<OffsetId, ZerobusError>) -> (OffsetId, Option<SdkError>) {
    match result {
        Ok(offset) => (offset, None),
        Err(err) => (0, Some(sdk_error(&err))),
    }
}

/// A `CreateStreamResponse` carrying an SDK failure. `stream_id` is unset on error.
fn create_failed(err: &ZerobusError) -> CreateStreamResponse {
    CreateStreamResponse {
        stream_id: String::new(),
        error: Some(sdk_error(err)),
    }
}

/// The SDK stream builder for `table`, with no auth (the mock checks no credentials).
fn stream_builder<'a>(sdk: &'a ZerobusSdk, table: &str) -> StreamBuilder<'a> {
    sdk.stream_builder().table(table).no_auth()
}

/// Open the SDK stream described by `req`. The outer `Err` is a harness fault; the inner
/// `Err` is the SDK's rejection of the stream.
async fn open_stream(
    sdk: &ZerobusSdk,
    req: &CreateStreamRequest,
) -> Result<Result<StreamKind, ZerobusError>, Status> {
    if req.protocol() == Protocol::ArrowFlight {
        // Arrow Flight (columnar): the SDK sends RecordBatches over DoPut.
        let schema = records::arrow_schema_from_ipc(&req.arrow_schema_ipc)
            .map_err(Status::invalid_argument)?;
        return Ok(stream_builder(sdk, &req.table_path)
            .arrow(Arc::clone(&schema))
            .build_arrow()
            .await
            .map(|stream| StreamKind::Arrow { stream, schema }));
    }

    if req.format() == RecordFormat::Avro {
        // The SDK validates the writer schema on build, so its rejection is reported as-is.
        // The harness parses the schema only after that, to convert records. If that parse
        // fails, the open SDK stream is closed before the fault is returned.
        let mut stream = match stream_builder(sdk, &req.table_path)
            .avro(req.avro_schema_json.clone())
            .build()
            .await
        {
            Ok(stream) => stream,
            Err(err) => return Ok(Err(err)),
        };
        return match records::avro_schema_from_json(&req.avro_schema_json) {
            Ok(schema) => Ok(Ok(StreamKind::Grpc {
                stream,
                schema: RecordSchema::Avro(schema),
            })),
            Err(fault) => {
                let _ = stream.close().await;
                Err(Status::invalid_argument(fault))
            }
        };
    }

    // gRPC (row) PROTO: the SDK needs the parsed descriptor to build the stream, so the
    // harness parse comes first and no SDK stream is open yet.
    let descriptor = records::descriptor_from_bytes(&req.descriptor_proto_bytes)
        .map_err(Status::invalid_argument)?;
    Ok(stream_builder(sdk, &req.table_path)
        .dynamic_proto(descriptor.clone())
        .build()
        .await
        .map(|stream| StreamKind::Grpc {
            stream,
            schema: RecordSchema::Proto(descriptor),
        }))
}

#[tonic::async_trait]
impl TestHarness for HarnessService {
    async fn create_stream(
        &self,
        request: Request<CreateStreamRequest>,
    ) -> Result<Response<CreateStreamResponse>, Status> {
        let req = request.into_inner();
        check_slice_supported(req.protocol(), req.format())?;

        info!(endpoint = %req.server_endpoint, table = %req.table_path, "CreateStream");

        // The mock is a local plaintext server that checks no credentials, so the
        // SDK runs with no TLS (`http://`) and no auth headers.
        let sdk = match ZerobusSdk::builder()
            .endpoint(format!("http://{}", req.server_endpoint))
            .no_tls()
            .build()
        {
            Ok(sdk) => sdk,
            Err(err) => return Ok(Response::new(create_failed(&err))),
        };

        let kind = match open_stream(&sdk, &req).await? {
            Ok(kind) => kind,
            Err(err) => return Ok(Response::new(create_failed(&err))),
        };

        let stream_id = format!(
            "stream-{}",
            self.next_stream_id.fetch_add(1, Ordering::Relaxed)
        );
        self.streams()?.insert(
            stream_id.clone(),
            Arc::new(RwLock::new(StreamEntry { _sdk: sdk, kind })),
        );

        Ok(Response::new(CreateStreamResponse {
            stream_id,
            error: None,
        }))
    }

    async fn ingest_record(
        &self,
        request: Request<IngestRecordRequest>,
    ) -> Result<Response<IngestRecordResponse>, Status> {
        let req = request.into_inner();
        let entry = self.entry(&req.stream_id)?;
        let guard = entry.read().await;

        // Queue only: the SDK call returns once the record is enqueued, not acknowledged.
        let (offset, error) = offset_parts(guard.kind.ingest_one(&req.record_json).await?);
        Ok(Response::new(IngestRecordResponse { offset, error }))
    }

    async fn ingest_records(
        &self,
        request: Request<IngestRecordsRequest>,
    ) -> Result<Response<IngestRecordsResponse>, Status> {
        let req = request.into_inner();
        let entry = self.entry(&req.stream_id)?;
        let guard = entry.read().await;

        // One SDK batch call, which returns the single offset for the whole batch.
        // Queue only: no acks are awaited here.
        let outcome = match guard.kind.ingest_all(&req.record_json).await? {
            Ok(Some(offset)) => Ok(offset),
            // The SDK returns `None` only for an empty batch, which has no offset to report.
            Ok(None) => return Err(Status::invalid_argument("empty batch")),
            Err(err) => Err(err),
        };
        let (offset, error) = offset_parts(outcome);
        Ok(Response::new(IngestRecordsResponse { offset, error }))
    }

    async fn await_write_completed(
        &self,
        request: Request<AwaitRequest>,
    ) -> Result<Response<AwaitResponse>, Status> {
        let req = request.into_inner();
        let entry = self.entry(&req.stream_id)?;
        let guard = entry.read().await;

        // No harness-side timeout: the wait is bounded by the SDK's own ack/flush timeout.
        let result = guard.kind.wait_for_offset(req.offset).await;
        Ok(Response::new(AwaitResponse {
            error: sdk_error_opt(result),
        }))
    }

    async fn flush(
        &self,
        request: Request<FlushRequest>,
    ) -> Result<Response<ActionResponse>, Status> {
        let req = request.into_inner();
        let entry = self.entry(&req.stream_id)?;
        let guard = entry.read().await;

        let result = guard.kind.flush().await;
        Ok(Response::new(ActionResponse {
            error: sdk_error_opt(result),
        }))
    }

    async fn close(
        &self,
        request: Request<CloseRequest>,
    ) -> Result<Response<ActionResponse>, Status> {
        let req = request.into_inner();
        let entry = self.entry(&req.stream_id)?;

        // The write guard waits for in-flight RPCs on this stream to finish first. The
        // entry stays registered, so later RPCs reach the SDK and its own closed-stream
        // error surfaces. Entries are freed on Shutdown.
        let mut guard = entry.write().await;
        let result = guard.kind.close().await;
        info!(stream_id = %req.stream_id, "stream closed");
        Ok(Response::new(ActionResponse {
            error: sdk_error_opt(result),
        }))
    }

    async fn shutdown(&self, _request: Request<Empty>) -> Result<Response<Empty>, Status> {
        info!("Shutdown RPC received");
        // Free every remaining stream entry, then let the server finish this response and exit.
        self.streams()?.clear();
        let _ = self.shutdown.send_replace(true);
        Ok(Response::new(Empty {}))
    }
}
