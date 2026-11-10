//! `databricks.zerobus.Zerobus` data plane.
//!
//! `EphemeralStream` is implemented: create-stream handshake, per-record cumulative
//! acks, raw-byte capture, and graceful-vs-cancel accounting. PersistentStream,
//! RetireStream and InsertRecords return `Unimplemented`.

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
    EphemeralStreamRequest, EphemeralStreamResponse, IngestRecordResponse, InsertRecordsRequest,
    InsertRecordsResponse, PersistentStreamRequest, PersistentStreamResponse, RetireStreamRequest,
    RetireStreamResponse,
};
use crate::MockState;

type ResponseTx = mpsc::Sender<Result<EphemeralStreamResponse, Status>>;
type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

/// Outcome of a guarded send on the response channel.
enum SendOutcome {
    Sent,
    ClientGone,
    ShuttingDown,
}

/// Sends, but gives up if shutdown starts, so a stalled client can't wedge the drain.
async fn guarded_send(
    tx: &ResponseTx,
    state: &MockState,
    msg: Result<EphemeralStreamResponse, Status>,
) -> SendOutcome {
    tokio::select! {
        _ = state.shutdown_requested() => SendOutcome::ShuttingDown,
        res = tx.send(msg) => {
            if res.is_ok() {
                SendOutcome::Sent
            } else {
                SendOutcome::ClientGone
            }
        }
    }
}

/// Zerobus gRPC service backed by shared [`MockState`].
pub struct MockGrpc {
    state: Arc<MockState>,
}

impl MockGrpc {
    pub fn new(state: Arc<MockState>) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl Zerobus for MockGrpc {
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

    async fn insert_records(
        &self,
        _request: Request<InsertRecordsRequest>,
    ) -> Result<Response<InsertRecordsResponse>, Status> {
        Err(Status::unimplemented(
            "InsertRecords is not implemented in the conformance mock",
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
    let mut next_expected: i64 = 0; // wire offsets: 0, +1 per record, per connection
    let my_gen = state.generation(); // a Reset bumps this; stop serving if it changes

    loop {
        if state.generation() != my_gen {
            return;
        }
        let request = tokio::select! {
            // End open streams on shutdown, else tonic's graceful drain hangs.
            _ = state.shutdown_requested() => return,
            msg = inbound.message() => msg,
        };
        // A Reset may have landed while we were idle on the await; re-check before recording.
        if state.generation() != my_gen {
            return;
        }
        let request = match request {
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
                    // Not a graceful close or a cancel — count aborts separately.
                    state.inc_abort();
                    warn!("EphemeralStream inbound error: {status}");
                }
                return;
            }
        };
        state.inc_message();

        match request.payload {
            Some(Payload::CreateStream(_create)) => {
                if stream_id.is_some() {
                    let _ = guarded_send(
                        &tx,
                        &state,
                        Err(Status::invalid_argument("create_stream already sent")),
                    )
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
                match guarded_send(&tx, &state, Ok(response)).await {
                    SendOutcome::Sent => {}
                    SendOutcome::ClientGone => {
                        state.inc_cancel();
                        return;
                    }
                    SendOutcome::ShuttingDown => return,
                }
                stream_id = Some(id);
            }
            Some(Payload::IngestRecord(record)) => {
                if stream_id.is_none() {
                    // InvalidArgument is unretriable, so the client fails fast.
                    let _ = guarded_send(
                        &tx,
                        &state,
                        Err(Status::invalid_argument(
                            "ingest_record before create_stream",
                        )),
                    )
                    .await;
                    return;
                }
                let Some(offset) = record.offset_id else {
                    let _ = guarded_send(
                        &tx,
                        &state,
                        Err(Status::invalid_argument("ingest_record missing offset_id")),
                    )
                    .await;
                    return;
                };
                // Offsets must be dense and in order on a connection.
                if offset != next_expected {
                    let _ = guarded_send(
                        &tx,
                        &state,
                        Err(Status::invalid_argument(format!(
                            "expected offset_id {next_expected}, got {offset}"
                        ))),
                    )
                    .await;
                    return;
                }
                next_expected += 1;
                let bytes = match record.record {
                    Some(Record::ProtoEncodedRecord(bytes)) => bytes,
                    Some(Record::AvroEncodedRecord(bytes)) => bytes,
                    // Raw UTF-8 of the JSON text; the mock never parses it.
                    Some(Record::JsonRecord(json)) => json.into_bytes(),
                    None => {
                        let _ = guarded_send(
                            &tx,
                            &state,
                            Err(Status::invalid_argument(
                                "ingest_record has no record payload",
                            )),
                        )
                        .await;
                        return;
                    }
                };
                state.capture_record(offset, bytes);

                // Fire before the ack, so this record and the tail stay unacked for a resend.
                if let Some(retryable) = state.take_inject(offset) {
                    let status = if retryable {
                        Status::unavailable("injected transient stream error")
                    } else {
                        // Unretriable in the SDK (unlike FailedPrecondition), so it terminates.
                        Status::invalid_argument("injected non-retryable stream error")
                    };
                    warn!("injecting stream abort at offset {offset} (retryable={retryable})");
                    let _ = guarded_send(&tx, &state, Err(status)).await;
                    return;
                }

                // Cumulative ack: everything up to and including `offset` is durable.
                state.note_ack(offset);
                let ack = EphemeralStreamResponse {
                    payload: Some(ResponsePayload::IngestRecordResponse(
                        IngestRecordResponse {
                            durability_ack_up_to_offset: Some(offset),
                        },
                    )),
                };
                match guarded_send(&tx, &state, Ok(ack)).await {
                    SendOutcome::Sent => {}
                    SendOutcome::ClientGone => {
                        state.inc_cancel();
                        return;
                    }
                    SendOutcome::ShuttingDown => return,
                }
            }
            Some(Payload::IngestRecordBatch(_batch)) => {
                // Not supported yet — reject loudly rather than pass without capture or
                // fault injection. Full support comes with the shared-engine refactor.
                let _ = guarded_send(
                    &tx,
                    &state,
                    Err(Status::unimplemented(
                        "ingest_record_batch is not supported by the mock yet",
                    )),
                )
                .await;
                return;
            }
            None => {
                let _ = guarded_send(
                    &tx,
                    &state,
                    Err(Status::invalid_argument("request has no payload")),
                )
                .await;
                return;
            }
        }
    }
}
