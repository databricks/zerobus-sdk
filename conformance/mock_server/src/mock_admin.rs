//! `zerobus.conformance.mock.MockAdmin` control plane (out-of-band, not Zerobus protocol).
//!
//! Counters are global: `StreamQuery.stream_id` is reserved and ignored.

use std::sync::Arc;

use tonic::{Request, Response, Status};
use tracing::info;

use crate::proto::mock::{
    mock_admin_server::MockAdmin as MockAdminRpc, BytesResponse, CountResponse, Empty,
    InjectStreamErrorRequest, OffsetResponse, RecordQuery, StreamQuery,
};
use crate::MockState;

/// MockAdmin service backed by shared [`MockState`].
pub struct MockAdmin {
    state: Arc<MockState>,
}

impl MockAdmin {
    pub fn new(state: Arc<MockState>) -> Self {
        Self { state }
    }
}

fn count(value: i64) -> Result<Response<CountResponse>, Status> {
    Ok(Response::new(CountResponse { count: value }))
}

#[tonic::async_trait]
impl MockAdminRpc for MockAdmin {
    async fn get_call_count(
        &self,
        _request: Request<StreamQuery>,
    ) -> Result<Response<CountResponse>, Status> {
        count(self.state.call_count())
    }

    async fn get_captured_message_count(
        &self,
        _request: Request<StreamQuery>,
    ) -> Result<Response<CountResponse>, Status> {
        count(self.state.captured_message_count())
    }

    async fn get_on_completed_count(
        &self,
        _request: Request<StreamQuery>,
    ) -> Result<Response<CountResponse>, Status> {
        count(self.state.on_completed_count())
    }

    async fn get_cancel_count(
        &self,
        _request: Request<StreamQuery>,
    ) -> Result<Response<CountResponse>, Status> {
        count(self.state.cancel_count())
    }

    async fn get_captured_record_count(
        &self,
        _request: Request<StreamQuery>,
    ) -> Result<Response<CountResponse>, Status> {
        count(self.state.captured_record_count())
    }

    async fn get_ack_offset(
        &self,
        _request: Request<StreamQuery>,
    ) -> Result<Response<OffsetResponse>, Status> {
        // -1 when no ack has been sent yet.
        Ok(Response::new(OffsetResponse {
            offset: self.state.ack_offset(),
        }))
    }

    async fn get_captured_record_bytes(
        &self,
        request: Request<RecordQuery>,
    ) -> Result<Response<BytesResponse>, Status> {
        let offset = request.into_inner().offset;
        match self.state.record_bytes(offset) {
            Some(bytes) => Ok(Response::new(BytesResponse {
                record_bytes: bytes,
            })),
            None => Err(Status::not_found(format!(
                "no captured record at offset {offset}"
            ))),
        }
    }

    async fn inject_stream_error(
        &self,
        request: Request<InjectStreamErrorRequest>,
    ) -> Result<Response<Empty>, Status> {
        let req = request.into_inner();
        info!(
            "MockAdmin.InjectStreamError after_offset={} retryable={}",
            req.after_offset, req.retryable
        );
        self.state.arm_inject(req.after_offset, req.retryable);
        Ok(Response::new(Empty {}))
    }

    async fn reset(&self, _request: Request<Empty>) -> Result<Response<Empty>, Status> {
        info!("MockAdmin.Reset");
        self.state.reset();
        Ok(Response::new(Empty {}))
    }

    async fn shutdown(&self, _request: Request<Empty>) -> Result<Response<Empty>, Status> {
        info!("MockAdmin.Shutdown requested");
        self.state.request_shutdown();
        Ok(Response::new(Empty {}))
    }
}
