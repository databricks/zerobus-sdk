//! Reactive Arrow Flight data plane (`arrow.flight.protocol.FlightService`, `DoPut`).
//!
//! The Flight analog of [`MockGrpc`](crate::MockGrpc): no script. On a `DoPut` it accepts
//! the schema message, replies with the ready signal, then acks every batch cumulatively,
//! updating the shared [`MockState`] so `MockAdmin` reports Arrow traffic (records, ack
//! offset, completion/cancel) the same way it reports gRPC.

use std::pin::Pin;
use std::sync::Arc;

use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PutResult, SchemaResult, Ticket,
};
use futures::Stream;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::warn;

use crate::MockState;

/// Per-batch metadata the client sends with each FlightData batch; must match the SDK's
/// `FlightBatchMetadata`.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct FlightBatchMetadata {
    offset_id: i64,
}

/// Ack metadata returned to the client; must match the SDK's `FlightAckMetadata`.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct FlightAckMetadata {
    ack_up_to_offset: i64,
    ack_up_to_records: u64,
}

/// Sentinel offset: stream setup is complete but no batches are acked yet.
const STREAM_READY_OFFSET: i64 = -1;

type BoxedFlightStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

/// Outcome of a guarded send on the response channel.
enum SendOutcome {
    Sent,
    ClientGone,
    ShuttingDown,
}

/// Sends, but gives up if shutdown starts, so a stalled client can't wedge the drain.
async fn guarded_send(
    tx: &mpsc::Sender<Result<PutResult, Status>>,
    state: &MockState,
    msg: Result<PutResult, Status>,
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

/// Reactive Flight service backed by shared [`MockState`].
pub struct MockFlight {
    state: Arc<MockState>,
}

impl MockFlight {
    pub fn new(state: Arc<MockState>) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl FlightService for MockFlight {
    type HandshakeStream = BoxedFlightStream<HandshakeResponse>;
    type ListFlightsStream = BoxedFlightStream<FlightInfo>;
    type DoGetStream = BoxedFlightStream<FlightData>;
    type DoPutStream = BoxedFlightStream<PutResult>;
    type DoExchangeStream = BoxedFlightStream<FlightData>;
    type DoActionStream = BoxedFlightStream<arrow_flight::Result>;
    type ListActionsStream = BoxedFlightStream<ActionType>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Handshake not implemented"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("ListFlights not implemented"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("GetFlightInfo not implemented"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<arrow_flight::PollInfo>, Status> {
        Err(Status::unimplemented("PollFlightInfo not implemented"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("GetSchema not implemented"))
    }

    async fn do_get(
        &self,
        _request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        Err(Status::unimplemented("DoGet not implemented"))
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let mut inbound = request.into_inner();
        let (tx, rx) = mpsc::channel(128);
        let state = Arc::clone(&self.state);
        // A DoPut connection opened — analogous to a CreateStream on the gRPC plane.
        state.inc_call();

        tokio::spawn(async move {
            let mut is_first = true;
            let mut expected_offset: i64 = 0;
            let mut connection_records: u64 = 0;
            let my_gen = state.generation(); // a Reset bumps this; stop serving if it changes

            loop {
                if state.generation() != my_gen {
                    return;
                }
                let flight_data = tokio::select! {
                    // End open streams on shutdown, else tonic's graceful drain hangs.
                    _ = state.shutdown_requested() => return,
                    msg = inbound.message() => msg,
                };
                // A Reset may have landed while we were idle on the await; re-check before recording.
                if state.generation() != my_gen {
                    return;
                }
                let flight_data = match flight_data {
                    Ok(Some(fd)) => fd,
                    Ok(None) => {
                        state.inc_on_completed();
                        return;
                    }
                    Err(status) => {
                        if status.code() == tonic::Code::Cancelled {
                            state.inc_cancel();
                        } else {
                            // Not a graceful close or a cancel — count aborts separately.
                            state.inc_abort();
                            warn!("DoPut inbound error: {status}");
                        }
                        return;
                    }
                };
                state.inc_message();

                // First message must be the Arrow IPC schema; reply with the ready signal.
                if is_first {
                    is_first = false;
                    let is_schema = arrow_ipc::root_as_message(&flight_data.data_header)
                        .ok()
                        .and_then(|m| m.header_as_schema())
                        .is_some();
                    if !is_schema {
                        let _ = guarded_send(
                            &tx,
                            &state,
                            Err(Status::invalid_argument(
                                "first DoPut frame is not an Arrow IPC schema",
                            )),
                        )
                        .await;
                        return;
                    }
                    let ready = FlightAckMetadata {
                        ack_up_to_offset: STREAM_READY_OFFSET,
                        ack_up_to_records: 0,
                    };
                    let msg = PutResult {
                        app_metadata: serde_json::to_vec(&ready).unwrap().into(),
                    };
                    match guarded_send(&tx, &state, Ok(msg)).await {
                        SendOutcome::Sent => {}
                        SendOutcome::ClientGone => {
                            state.inc_cancel();
                            return;
                        }
                        SendOutcome::ShuttingDown => return,
                    }
                    continue;
                }

                // Post-schema frames always carry batch metadata (Hydrate, no dictionary side
                // messages). A frame without it is a client bug; reject rather than hang flush().
                let Some(metadata) =
                    serde_json::from_slice::<FlightBatchMetadata>(&flight_data.app_metadata).ok()
                else {
                    let _ = guarded_send(
                        &tx,
                        &state,
                        Err(Status::invalid_argument(
                            "DoPut batch frame missing FlightBatchMetadata",
                        )),
                    )
                    .await;
                    return;
                };
                if metadata.offset_id != expected_offset {
                    let _ = guarded_send(
                        &tx,
                        &state,
                        Err(Status::invalid_argument(format!(
                            "Non-incremental offset: expected {expected_offset}, actual {}",
                            metadata.offset_id
                        ))),
                    )
                    .await;
                    return;
                }
                expected_offset = metadata.offset_id + 1;

                // A batch frame must be a non-empty record batch whose body covers the header's
                // bodyLength. Count rows from the header; never decode (SDK sends zstd IPC).
                let Ok(ipc_msg) = arrow_ipc::root_as_message(&flight_data.data_header) else {
                    let _ = guarded_send(
                        &tx,
                        &state,
                        Err(Status::invalid_argument(
                            "DoPut batch frame has an unparseable IPC header",
                        )),
                    )
                    .await;
                    return;
                };
                let Some(record_batch) = ipc_msg.header_as_record_batch() else {
                    let _ = guarded_send(
                        &tx,
                        &state,
                        Err(Status::invalid_argument(
                            "DoPut batch frame is not an Arrow record batch",
                        )),
                    )
                    .await;
                    return;
                };
                let rows = record_batch.length();
                if rows <= 0 || (flight_data.data_body.len() as i64) < ipc_msg.bodyLength() {
                    let _ = guarded_send(
                        &tx,
                        &state,
                        Err(Status::invalid_argument(
                            "DoPut batch frame is empty or its body is truncated",
                        )),
                    )
                    .await;
                    return;
                }
                state.add_captured_records(rows);
                connection_records += rows as u64;

                // One-shot injected abort (fault-injection parity with the gRPC plane): fire
                // before the ack so the batch stays unacked for a recovering SDK to resend.
                if let Some(retryable) = state.take_inject(metadata.offset_id) {
                    let status = if retryable {
                        Status::unavailable("injected transient stream error")
                    } else {
                        Status::invalid_argument("injected non-retryable stream error")
                    };
                    let _ = guarded_send(&tx, &state, Err(status)).await;
                    return;
                }
                state.note_ack(metadata.offset_id);

                let ack = FlightAckMetadata {
                    ack_up_to_offset: metadata.offset_id,
                    ack_up_to_records: connection_records,
                };
                let msg = PutResult {
                    app_metadata: serde_json::to_vec(&ack).unwrap().into(),
                };
                match guarded_send(&tx, &state, Ok(msg)).await {
                    SendOutcome::Sent => {}
                    SendOutcome::ClientGone => {
                        state.inc_cancel();
                        return;
                    }
                    SendOutcome::ShuttingDown => return,
                }
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("DoExchange not implemented"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("DoAction not implemented"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("ListActions not implemented"))
    }
}
