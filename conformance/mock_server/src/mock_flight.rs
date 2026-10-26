//! Reactive Arrow Flight data plane (`arrow.flight.protocol.FlightService`, `DoPut`).
//!
//! The Flight analog of [`MockGrpc`](crate::MockGrpc): no script. On a `DoPut` it accepts
//! the schema message, replies with the ready signal, then acks every batch cumulatively,
//! updating the shared [`MockState`] so `MockAdmin` reports Arrow traffic (records, ack
//! offset, completion/cancel) the same way it reports gRPC.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PutResult, SchemaResult, Ticket,
};
use arrow_schema::Schema;
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
            let mut wire_schema: Option<Arc<Schema>> = None;

            loop {
                let flight_data = match inbound.message().await {
                    Ok(Some(fd)) => fd,
                    Ok(None) => {
                        state.inc_on_completed();
                        return;
                    }
                    Err(status) => {
                        if status.code() == tonic::Code::Cancelled {
                            state.inc_cancel();
                        } else {
                            warn!("DoPut inbound error: {status}");
                        }
                        return;
                    }
                };
                state.inc_message();

                // First message carries the Arrow IPC schema (no app_metadata).
                if is_first {
                    is_first = false;
                    if flight_data.app_metadata.is_empty() {
                        wire_schema = Schema::try_from(&flight_data).ok().map(Arc::new);
                        let ready = FlightAckMetadata {
                            ack_up_to_offset: STREAM_READY_OFFSET,
                            ack_up_to_records: 0,
                        };
                        let msg = PutResult {
                            app_metadata: serde_json::to_vec(&ready).unwrap().into(),
                        };
                        if tx.send(Ok(msg)).await.is_err() {
                            return;
                        }
                        continue;
                    }
                }

                // Every subsequent batch carries FlightBatchMetadata with its offset.
                let Some(metadata) = serde_json::from_slice::<FlightBatchMetadata>(&flight_data.app_metadata).ok()
                else {
                    // Non-batch message (e.g. a dictionary batch): ignore.
                    continue;
                };
                if metadata.offset_id != expected_offset {
                    let _ = tx
                        .send(Err(Status::invalid_argument(format!(
                            "Non-incremental offset: expected {expected_offset}, actual {}",
                            metadata.offset_id
                        ))))
                        .await;
                    return;
                }
                expected_offset = metadata.offset_id + 1;

                // Count the rows in the batch (decode against the wire schema).
                let mut rows: i64 = 0;
                if let Some(schema) = &wire_schema {
                    match arrow_flight::utils::flight_data_to_arrow_batch(
                        &flight_data,
                        Arc::clone(schema),
                        &HashMap::new(),
                    ) {
                        Ok(batch) => rows = batch.num_rows() as i64,
                        Err(error) => warn!("failed to decode Arrow batch: {error}"),
                    }
                }
                if rows > 0 {
                    state.add_captured_records(rows);
                    connection_records += rows as u64;
                }
                state.note_ack(metadata.offset_id);

                let ack = FlightAckMetadata {
                    ack_up_to_offset: metadata.offset_id,
                    ack_up_to_records: connection_records,
                };
                let msg = PutResult {
                    app_metadata: serde_json::to_vec(&ack).unwrap().into(),
                };
                if tx.send(Ok(msg)).await.is_err() {
                    return;
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
