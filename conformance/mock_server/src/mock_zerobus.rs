//! `databricks.zerobus.Zerobus` data plane.
//!
//! `EphemeralStream` is implemented: create-stream handshake, per-record cumulative
//! acks, raw-byte capture, and graceful-vs-cancel accounting. PersistentStream and
//! RetireStream return `Unimplemented`.

use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, info, warn};

use crate::proto::zerobus::{
    ephemeral_stream_request::Payload, ephemeral_stream_response::Payload as ResponsePayload,
    ingest_record_request::Record, zerobus_server::Zerobus, CreateIngestStreamResponse,
    EphemeralStreamRequest, EphemeralStreamResponse, IngestRecordResponse, PersistentStreamRequest,
    PersistentStreamResponse, RetireStreamRequest, RetireStreamResponse,
};
use crate::MockState;

type ResponseTx = mpsc::Sender<Result<EphemeralStreamResponse, Status>>;
type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

/// Zerobus gRPC service backed by shared [`MockState`].
pub struct MockZerobus {
    state: Arc<MockState>,
}

impl MockZerobus {
    pub fn new(state: Arc<MockState>) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl Zerobus for MockZerobus {
    type EphemeralStreamStream = BoxStream<EphemeralStreamResponse>;

    async fn ephemeral_stream(
        &self,
        request: Request<Streaming<EphemeralStreamRequest>>,
    ) -> Result<Response<Self::EphemeralStreamStream>, Status> {
        let inbound = request.into_inner();
        let (tx, rx) = mpsc::channel(128);
        let state = Arc::clone(&self.state);
        tokio::spawn(async move { run_ephemeral_stream(state, inbound, tx).await });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    type PersistentStreamStream = BoxStream<PersistentStreamResponse>;

    // TODO: PersistentStream (IN DEVELOPMENT in the proto). Implement with the
    // resume/commit semantics in rust/tests/src/mock_grpc.rs when needed.
    async fn persistent_stream(
        &self,
        _request: Request<Streaming<PersistentStreamRequest>>,
    ) -> Result<Response<Self::PersistentStreamStream>, Status> {
        Err(Status::unimplemented(
            "PersistentStream is not implemented in the conformance mock",
        ))
    }

    // TODO: RetireStream (IN DEVELOPMENT in the proto).
    async fn retire_stream(
        &self,
        _request: Request<RetireStreamRequest>,
    ) -> Result<Response<RetireStreamResponse>, Status> {
        Err(Status::unimplemented(
            "RetireStream is not implemented in the conformance mock",
        ))
    }
}

/// Drives one EphemeralStream: handshake, then ingest until the client half-closes
/// (graceful) or cancels. Each ingest is acked cumulatively as it is received.
async fn run_ephemeral_stream(
    state: Arc<MockState>,
    mut inbound: Streaming<EphemeralStreamRequest>,
    tx: ResponseTx,
) {
    let mut stream_id: Option<String> = None;

    loop {
        let request = match inbound.message().await {
            Ok(Some(request)) => request,
            Ok(None) => {
                // Client half-close: graceful completion.
                state.inc_on_completed();
                debug!("EphemeralStream half-closed by client");
                return;
            }
            Err(status) => {
                if status.code() == tonic::Code::Cancelled {
                    state.inc_cancel();
                    debug!("EphemeralStream cancelled by client");
                } else {
                    warn!("EphemeralStream inbound error: {status}");
                }
                return;
            }
        };
        state.inc_message();

        match request.payload {
            Some(Payload::CreateStream(_create)) => {
                if stream_id.is_some() {
                    let _ = tx
                        .send(Err(Status::invalid_argument("create_stream already sent")))
                        .await;
                    return;
                }
                // TODO: record table_name / descriptor / record_type per stream
                // (the orchestrator will want these for per-stream assertions).
                state.inc_call();
                let id = state.next_stream_id();
                info!("EphemeralStream created stream {id}");
                let response = EphemeralStreamResponse {
                    payload: Some(ResponsePayload::CreateStreamResponse(
                        CreateIngestStreamResponse {
                            stream_id: Some(id.clone()),
                        },
                    )),
                };
                if tx.send(Ok(response)).await.is_err() {
                    state.inc_cancel();
                    return;
                }
                stream_id = Some(id);
            }
            Some(Payload::IngestRecord(record)) => {
                if stream_id.is_none() {
                    let _ = tx
                        .send(Err(Status::failed_precondition(
                            "ingest_record before create_stream",
                        )))
                        .await;
                    return;
                }
                let Some(offset) = record.offset_id else {
                    let _ = tx
                        .send(Err(Status::invalid_argument(
                            "ingest_record missing offset_id",
                        )))
                        .await;
                    return;
                };
                let bytes = match record.record {
                    Some(Record::ProtoEncodedRecord(bytes)) => bytes,
                    Some(Record::AvroEncodedRecord(bytes)) => bytes,
                    // Raw UTF-8 of the JSON text; the mock never parses it.
                    Some(Record::JsonRecord(json)) => json.into_bytes(),
                    None => {
                        let _ = tx
                            .send(Err(Status::invalid_argument(
                                "ingest_record has no record payload",
                            )))
                            .await;
                        return;
                    }
                };
                state.capture_record(offset, bytes);

                // One-shot injected abort: fire before acking so this record (and
                // everything after) stays unacked for a recovering SDK to resend.
                if let Some(retryable) = state.take_inject(offset) {
                    let status = if retryable {
                        Status::unavailable("injected transient stream error")
                    } else {
                        Status::failed_precondition("injected non-retryable stream error")
                    };
                    warn!("injecting stream abort at offset {offset} (retryable={retryable})");
                    let _ = tx.send(Err(status)).await;
                    return;
                }

                // Cumulative ack: everything up to and including `offset` is durable.
                state.await_ack_gate().await;
                state.note_ack(offset);
                let ack = EphemeralStreamResponse {
                    payload: Some(ResponsePayload::IngestRecordResponse(
                        IngestRecordResponse {
                            durability_ack_up_to_offset: Some(offset),
                        },
                    )),
                };
                if tx.send(Ok(ack)).await.is_err() {
                    state.inc_cancel();
                    return;
                }
            }
            Some(Payload::IngestRecordBatch(batch)) => {
                // TODO(capture): batch records are not captured yet. Only the
                // cumulative ack is emitted, keyed on the batch offset_id.
                if stream_id.is_none() {
                    let _ = tx
                        .send(Err(Status::failed_precondition(
                            "ingest_record_batch before create_stream",
                        )))
                        .await;
                    return;
                }
                let Some(offset) = batch.offset_id else {
                    let _ = tx
                        .send(Err(Status::invalid_argument(
                            "ingest_record_batch missing offset_id",
                        )))
                        .await;
                    return;
                };
                state.await_ack_gate().await;
                state.note_ack(offset);
                let ack = EphemeralStreamResponse {
                    payload: Some(ResponsePayload::IngestRecordResponse(
                        IngestRecordResponse {
                            durability_ack_up_to_offset: Some(offset),
                        },
                    )),
                };
                if tx.send(Ok(ack)).await.is_err() {
                    state.inc_cancel();
                    return;
                }
            }
            None => {
                debug!("EphemeralStream received request with empty payload; ignored");
            }
        }
    }
}
