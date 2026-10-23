//! `TestHarness` service: maps each contract RPC onto the Rust SDK.
//!
//! First slice: `protocol=GRPC`, `format=PROTO` only. Every other combination
//! returns `UNIMPLEMENTED` with a TODO (see [`check_slice_supported`]).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use databricks_zerobus_ingest_sdk::{MessageDescriptor, ZerobusError, ZerobusSdk, ZerobusStream};
use tokio::sync::{watch, Mutex as AsyncMutex};
use tonic::{Request, Response, Status};
use tracing::info;

use crate::pb::test_harness_server::TestHarness;
use crate::pb::{
    AwaitRequest, AwaitResponse, CloseRequest, CreateStreamRequest, CreateStreamResponse, Empty,
    FlushRequest, IngestRecordRequest, IngestRecordResponse, Protocol, RecordFormat,
};
use crate::records;

/// One open SDK stream plus the descriptor its records are built against.
struct StreamEntry {
    /// Kept alive for as long as the stream it created.
    _sdk: ZerobusSdk,
    stream: ZerobusStream,
    descriptor: MessageDescriptor,
}

pub struct HarnessService {
    /// `stream_id -> entry`. The std mutex guards only the map and is never held
    /// across an await. Each entry has its own async mutex, which serializes
    /// per-stream RPCs (ingest / await / flush / close) regardless of whether the
    /// SDK stream type is `Sync`.
    streams: Mutex<HashMap<String, Arc<AsyncMutex<StreamEntry>>>>,
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

    fn entry(&self, stream_id: &str) -> Result<Arc<AsyncMutex<StreamEntry>>, Status> {
        self.streams
            .lock()
            .expect("stream map poisoned")
            .get(stream_id)
            .cloned()
            .ok_or_else(|| Status::not_found(format!("unknown stream_id '{stream_id}'")))
    }
}

/// Reject every protocol/format combination this slice does not implement.
/// Each TODO names the work that remains.
#[allow(clippy::result_large_err)]
fn check_slice_supported(protocol: Protocol, format: RecordFormat) -> Result<(), Status> {
    match (protocol, format) {
        (Protocol::Grpc, RecordFormat::Proto) => Ok(()),
        (Protocol::Grpc, RecordFormat::Json) => Err(Status::unimplemented(
            "TODO(conformance): GRPC + JSON not implemented in the Rust harness yet",
        )),
        (Protocol::Grpc, RecordFormat::Avro) => Err(Status::unimplemented(
            "TODO(conformance): GRPC + AVRO not implemented in the Rust harness yet (needs the `avro` feature)",
        )),
        (Protocol::Grpc, RecordFormat::Arrow) => Err(Status::unimplemented(
            "TODO(conformance): GRPC + ARROW is not a valid pairing; ARROW rides ARROW_FLIGHT",
        )),
        (Protocol::ArrowFlight, _) => Err(Status::unimplemented(
            "TODO(conformance): ARROW_FLIGHT protocol not implemented in the Rust harness yet",
        )),
        _ => Err(Status::invalid_argument(
            "protocol and format must be specified",
        )),
    }
}

/// Map an SDK error onto a gRPC status. Argument problems stay `INVALID_ARGUMENT`;
/// everything else is `INTERNAL` with the SDK message attached.
fn to_status(err: ZerobusError) -> Status {
    match err {
        ZerobusError::InvalidArgument(msg) => Status::invalid_argument(msg),
        ZerobusError::StreamClosedError(status) => {
            Status::failed_precondition(format!("stream closed: {status}"))
        }
        other => Status::internal(other.to_string()),
    }
}

#[tonic::async_trait]
impl TestHarness for HarnessService {
    async fn create_stream(
        &self,
        request: Request<CreateStreamRequest>,
    ) -> Result<Response<CreateStreamResponse>, Status> {
        let req = request.into_inner();
        check_slice_supported(req.protocol(), req.format())?;

        let descriptor = records::descriptor_from_bytes(&req.descriptor_proto_bytes)
            .map_err(Status::invalid_argument)?;

        info!(endpoint = %req.server_endpoint, table = %req.table_path, "CreateStream");

        // The mock is a local plaintext server that checks no credentials, so the
        // SDK runs with no TLS (`http://`) and no auth headers.
        let sdk = ZerobusSdk::builder()
            .endpoint(format!("http://{}", req.server_endpoint))
            .no_tls()
            .build()
            .map_err(to_status)?;

        let stream = sdk
            .stream_builder()
            .table(req.table_path)
            .no_auth()
            .dynamic_proto(descriptor.clone())
            .build()
            .await
            .map_err(to_status)?;

        let stream_id = format!(
            "stream-{}",
            self.next_stream_id.fetch_add(1, Ordering::Relaxed)
        );
        let entry = StreamEntry {
            _sdk: sdk,
            stream,
            descriptor,
        };
        self.streams
            .lock()
            .expect("stream map poisoned")
            .insert(stream_id.clone(), Arc::new(AsyncMutex::new(entry)));

        Ok(Response::new(CreateStreamResponse { stream_id }))
    }

    async fn ingest_record(
        &self,
        request: Request<IngestRecordRequest>,
    ) -> Result<Response<IngestRecordResponse>, Status> {
        let req = request.into_inner();
        let entry = self.entry(&req.stream_id)?;
        let guard = entry.lock().await;

        let message = records::record_from_json(&guard.descriptor, &req.record_json)
            .map_err(Status::invalid_argument)?;

        // Queue only. This returns once the SDK has enqueued the record, not once
        // it is acknowledged. The SDK encodes the message itself.
        let offset = guard
            .stream
            .ingest_record_offset(records::sdk_input(message))
            .await
            .map_err(to_status)?;

        Ok(Response::new(IngestRecordResponse { offset }))
    }

    async fn await_write_completed(
        &self,
        request: Request<AwaitRequest>,
    ) -> Result<Response<AwaitResponse>, Status> {
        let req = request.into_inner();
        let entry = self.entry(&req.stream_id)?;
        let guard = entry.lock().await;

        // `wait_for_offset` has no timeout of its own, so the contract's
        // `timeout_ms` is applied here. A value <= 0 waits with no deadline.
        let wait = guard.stream.wait_for_offset(req.offset);
        let outcome = if req.timeout_ms > 0 {
            match tokio::time::timeout(Duration::from_millis(req.timeout_ms as u64), wait).await {
                Ok(result) => result,
                Err(_) => {
                    return Ok(Response::new(AwaitResponse {
                        error_message: format!(
                            "timed out after {} ms waiting for offset {}",
                            req.timeout_ms, req.offset
                        ),
                    }));
                }
            }
        } else {
            wait.await
        };

        // Per the contract, a failed wait is reported in the response body, not as
        // a gRPC error. Empty `error_message` means success.
        Ok(Response::new(AwaitResponse {
            error_message: outcome.err().map(|e| e.to_string()).unwrap_or_default(),
        }))
    }

    async fn flush(&self, request: Request<FlushRequest>) -> Result<Response<Empty>, Status> {
        let req = request.into_inner();
        let entry = self.entry(&req.stream_id)?;
        let guard = entry.lock().await;
        guard.stream.flush().await.map_err(to_status)?;
        Ok(Response::new(Empty {}))
    }

    async fn close(&self, request: Request<CloseRequest>) -> Result<Response<Empty>, Status> {
        let req = request.into_inner();
        // Remove first so new RPCs on this id see NOT_FOUND. The entry is dropped
        // even if the SDK close fails.
        let entry = self
            .streams
            .lock()
            .expect("stream map poisoned")
            .remove(&req.stream_id)
            .ok_or_else(|| Status::not_found(format!("unknown stream_id '{}'", req.stream_id)))?;
        let mut guard = entry.lock().await;
        guard.stream.close().await.map_err(to_status)?;
        info!(stream_id = %req.stream_id, "stream closed");
        Ok(Response::new(Empty {}))
    }

    async fn shutdown(&self, _request: Request<Empty>) -> Result<Response<Empty>, Status> {
        info!("Shutdown RPC received");
        // The server finishes this response before exiting (graceful shutdown).
        let _ = self.shutdown.send_replace(true);
        Ok(Response::new(Empty {}))
    }
}
