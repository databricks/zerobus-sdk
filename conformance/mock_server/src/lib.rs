//! Conformance mock server: the single source of truth for the Zerobus mock.
//!
//! Hosts the Zerobus gRPC data plane (`databricks.zerobus.Zerobus`) and the
//! out-of-band `MockAdmin` control plane on one port. `rust/tests` embeds it via
//! [`start_server`]; the `zerobus-mock-server` binary wraps the same helpers.
//!
//! Counters, captured bytes and the ack gate live in [`MockState`], shared by
//! both services behind an `Arc`. Counters are global per mock instance
//! (first-slice rule: one stream per scenario, `Reset` between runs).

use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::net::TcpListener;
use tokio::sync::{Notify, Semaphore};
use tokio::task::JoinHandle;

pub mod mock_admin;
pub mod mock_flight;
pub mod mock_zerobus;

pub use mock_admin::MockAdmin;
pub use mock_zerobus::MockZerobus;

/// Generated protobuf/gRPC code.
pub mod proto {
    /// `databricks.zerobus` — the real Zerobus data-plane protocol.
    pub mod zerobus {
        tonic::include_proto!("databricks.zerobus");
    }
    /// `zerobus.conformance.mock` — the MockAdmin control plane.
    pub mod mock {
        tonic::include_proto!("zerobus.conformance.mock");
    }
}

/// Shared mock state. Cheap to clone via `Arc`; all methods take `&self`.
pub struct MockState {
    /// CreateStream messages seen (`GetCallCount`).
    call_count: AtomicI64,
    /// Every inbound EphemeralStream request message (`GetCapturedMessageCount`).
    captured_message_count: AtomicI64,
    /// Graceful client half-closes (`GetOnCompletedCount`).
    on_completed_count: AtomicI64,
    /// Client cancels, as opposed to half-closes (`GetCancelCount`).
    cancel_count: AtomicI64,
    /// Ingest records captured with their raw bytes (`GetCapturedRecordCount`).
    captured_record_count: AtomicI64,
    /// Highest cumulative ack sent; -1 when nothing has been acked yet.
    ack_offset: AtomicI64,
    /// Source of unique stream ids.
    stream_counter: AtomicU64,
    /// Raw wire bytes of each ingested record, keyed by `offset_id`.
    /// Stores bytes only; never decodes. Last write wins on duplicate offsets.
    records: Mutex<HashMap<i64, Vec<u8>>>,
    /// When true, each ack waits for a permit from `ack_gate` (see `hold_acks`).
    ack_gate_enabled: AtomicBool,
    ack_gate: Semaphore,
    /// One-shot stream-abort injection, armed via `MockAdmin.InjectStreamError`.
    inject_armed: AtomicBool,
    inject_fired: AtomicBool,
    inject_after_offset: AtomicI64,
    inject_retryable: AtomicBool,
    /// Signalled by `MockAdmin.Shutdown` (`request_shutdown`).
    shutdown: Notify,
}

impl Default for MockState {
    fn default() -> Self {
        Self::new()
    }
}

impl MockState {
    pub fn new() -> Self {
        Self {
            call_count: AtomicI64::new(0),
            captured_message_count: AtomicI64::new(0),
            on_completed_count: AtomicI64::new(0),
            cancel_count: AtomicI64::new(0),
            captured_record_count: AtomicI64::new(0),
            ack_offset: AtomicI64::new(-1),
            stream_counter: AtomicU64::new(0),
            records: Mutex::new(HashMap::new()),
            ack_gate_enabled: AtomicBool::new(false),
            ack_gate: Semaphore::new(0),
            inject_armed: AtomicBool::new(false),
            inject_fired: AtomicBool::new(false),
            inject_after_offset: AtomicI64::new(-1),
            inject_retryable: AtomicBool::new(true),
            shutdown: Notify::new(),
        }
    }

    // ----- counters (read) -------------------------------------------------

    pub fn call_count(&self) -> i64 {
        self.call_count.load(Ordering::SeqCst)
    }
    pub fn captured_message_count(&self) -> i64 {
        self.captured_message_count.load(Ordering::SeqCst)
    }
    pub fn on_completed_count(&self) -> i64 {
        self.on_completed_count.load(Ordering::SeqCst)
    }
    pub fn cancel_count(&self) -> i64 {
        self.cancel_count.load(Ordering::SeqCst)
    }
    pub fn captured_record_count(&self) -> i64 {
        self.captured_record_count.load(Ordering::SeqCst)
    }
    /// Highest cumulative ack sent, or -1 if none.
    pub fn ack_offset(&self) -> i64 {
        self.ack_offset.load(Ordering::SeqCst)
    }
    /// Raw captured bytes for `offset`, if a record was captured there.
    pub fn record_bytes(&self, offset: i64) -> Option<Vec<u8>> {
        self.records
            .lock()
            .expect("records lock")
            .get(&offset)
            .cloned()
    }

    // ----- ack gating ------------------------------------------------------

    /// Holds every subsequent cumulative ack until [`release_acks`](Self::release_acks)
    /// grants a permit. Lets tests observe in-flight (unacked) state deterministically.
    pub fn hold_acks(&self) {
        self.ack_gate_enabled.store(true, Ordering::SeqCst);
    }

    /// Grants `n` ack permits. Only meaningful while acks are held.
    pub fn release_acks(&self, n: usize) {
        self.ack_gate.add_permits(n);
    }

    // ----- fault injection -------------------------------------------------

    /// Arms a one-shot stream abort: after acking through `after_offset`, the next
    /// ingested record aborts the stream (retryable UNAVAILABLE if `retryable`, else a
    /// terminal error). `after_offset = -1` aborts on the first record. Fires once until
    /// re-armed or [`reset`](Self::reset).
    pub fn arm_inject(&self, after_offset: i64, retryable: bool) {
        self.inject_after_offset
            .store(after_offset, Ordering::SeqCst);
        self.inject_retryable.store(retryable, Ordering::SeqCst);
        self.inject_fired.store(false, Ordering::SeqCst);
        self.inject_armed.store(true, Ordering::SeqCst);
    }

    /// If armed and `offset` is the first past `after_offset`, fires once and returns
    /// `Some(retryable)` so the caller aborts the stream; otherwise `None`.
    pub(crate) fn take_inject(&self, offset: i64) -> Option<bool> {
        if !self.inject_armed.load(Ordering::SeqCst) {
            return None;
        }
        if offset <= self.inject_after_offset.load(Ordering::SeqCst) {
            return None;
        }
        // One-shot: only the first record past the threshold fires.
        if self
            .inject_fired
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return None;
        }
        Some(self.inject_retryable.load(Ordering::SeqCst))
    }

    // ----- lifecycle -------------------------------------------------------

    /// Clears all counters, captured bytes, ack gating and stream ids.
    pub fn reset(&self) {
        self.call_count.store(0, Ordering::SeqCst);
        self.captured_message_count.store(0, Ordering::SeqCst);
        self.on_completed_count.store(0, Ordering::SeqCst);
        self.cancel_count.store(0, Ordering::SeqCst);
        self.captured_record_count.store(0, Ordering::SeqCst);
        self.ack_offset.store(-1, Ordering::SeqCst);
        self.stream_counter.store(0, Ordering::SeqCst);
        self.records.lock().expect("records lock").clear();
        self.ack_gate_enabled.store(false, Ordering::SeqCst);
        self.ack_gate
            .forget_permits(self.ack_gate.available_permits());
        self.inject_armed.store(false, Ordering::SeqCst);
        self.inject_fired.store(false, Ordering::SeqCst);
        self.inject_after_offset.store(-1, Ordering::SeqCst);
        self.inject_retryable.store(true, Ordering::SeqCst);
    }

    /// Asks the serving task to stop. Safe to call before the server awaits it.
    pub fn request_shutdown(&self) {
        // notify_one stores a permit, so a request made before the waiter exists is kept.
        self.shutdown.notify_one();
    }

    /// Resolves once [`request_shutdown`](Self::request_shutdown) has been called.
    pub async fn shutdown_requested(&self) {
        self.shutdown.notified().await;
    }

    // ----- crate-internal recording helpers --------------------------------

    pub(crate) fn inc_call(&self) {
        self.call_count.fetch_add(1, Ordering::SeqCst);
    }
    pub(crate) fn inc_message(&self) {
        self.captured_message_count.fetch_add(1, Ordering::SeqCst);
    }
    pub(crate) fn inc_on_completed(&self) {
        self.on_completed_count.fetch_add(1, Ordering::SeqCst);
    }
    pub(crate) fn inc_cancel(&self) {
        self.cancel_count.fetch_add(1, Ordering::SeqCst);
    }
    pub(crate) fn next_stream_id(&self) -> String {
        let n = self.stream_counter.fetch_add(1, Ordering::SeqCst) + 1;
        format!("mock_stream_{n}")
    }
    pub(crate) fn capture_record(&self, offset: i64, bytes: Vec<u8>) {
        self.records
            .lock()
            .expect("records lock")
            .insert(offset, bytes);
        self.captured_record_count.fetch_add(1, Ordering::SeqCst);
    }
    /// Waits for an ack permit when acks are held; no-op otherwise.
    pub(crate) async fn await_ack_gate(&self) {
        if self.ack_gate_enabled.load(Ordering::SeqCst) {
            // The semaphore is never closed, so acquire cannot fail.
            if let Ok(permit) = self.ack_gate.acquire().await {
                permit.forget();
            }
        }
    }
    /// Records an ack; the watermark only moves forward.
    pub(crate) fn note_ack(&self, offset: i64) {
        self.ack_offset.fetch_max(offset, Ordering::SeqCst);
    }
}

/// Serves Zerobus + MockAdmin on `listener` until `extra_shutdown` resolves or
/// `MockAdmin.Shutdown` is called. Returns once in-flight RPCs drain.
pub async fn serve_with_shutdown<F>(
    listener: TcpListener,
    state: Arc<MockState>,
    extra_shutdown: F,
) -> Result<(), tonic::transport::Error>
where
    F: Future<Output = ()> + Send + 'static,
{
    use proto::mock::mock_admin_server::MockAdminServer;
    use proto::zerobus::zerobus_server::ZerobusServer;
    use tokio_stream::wrappers::TcpListenerStream;

    let shutdown = {
        let state = Arc::clone(&state);
        async move {
            tokio::select! {
                _ = extra_shutdown => {}
                _ = state.shutdown_requested() => {}
            }
        }
    };

    // TODO(arrow-flight): register FlightServiceServer<MockFlight> here once the
    // Flight data plane lands (see mock_flight.rs).
    tonic::transport::Server::builder()
        .add_service(ZerobusServer::new(MockZerobus::new(Arc::clone(&state))))
        .add_service(MockAdminServer::new(MockAdmin::new(Arc::clone(&state))))
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown)
        .await
}

/// A mock server running on a background task, for embedding in tests.
pub struct RunningMockServer {
    addr: SocketAddr,
    state: Arc<MockState>,
    join: JoinHandle<Result<(), tonic::transport::Error>>,
}

impl RunningMockServer {
    /// Bound address, e.g. `127.0.0.1:54321`.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Endpoint string for client construction, e.g. `http://127.0.0.1:54321`.
    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Shared state for direct counter/capture inspection from Rust tests.
    pub fn state(&self) -> &Arc<MockState> {
        &self.state
    }

    /// Requests shutdown and waits for the serving task to finish.
    pub async fn shutdown(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.state.request_shutdown();
        self.join.await??;
        Ok(())
    }
}

/// Binds `bind` (use `127.0.0.1:0` for an ephemeral port) and serves in the
/// background. Stop it with [`RunningMockServer::shutdown`] or `MockAdmin.Shutdown`.
pub async fn start_server(bind: SocketAddr) -> std::io::Result<RunningMockServer> {
    let listener = TcpListener::bind(bind).await?;
    let addr = listener.local_addr()?;
    let state = Arc::new(MockState::new());
    let join = tokio::spawn(serve_with_shutdown(
        listener,
        Arc::clone(&state),
        std::future::pending(),
    ));
    Ok(RunningMockServer { addr, state, join })
}
