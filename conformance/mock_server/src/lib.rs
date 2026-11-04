//! Conformance mock server: hosts the Zerobus gRPC data plane
//! (`databricks.zerobus.Zerobus`) and the out-of-band `MockAdmin` control plane on one port.
//!
//! Counters and captured bytes live in [`MockState`], shared by both services behind an
//! `Arc`. Counters are global per mock instance.
//!
//! TEMPORARY: this crate implements its own *reactive* Zerobus mock, separate from
//! test-support's *scripted* `MockZerobusServer`. The duplication is intentional for now; the
//! plan is to extract one shared gRPC engine with a `ResponsePolicy` seam (scripted for
//! rust/tests, reactive for conformance) into test-support and drop this second data plane.

use std::future::Future;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::net::TcpListener;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

pub mod mock_admin;
pub mod mock_grpc;

pub use mock_admin::MockAdmin;
pub use mock_grpc::MockGrpc;

/// Generated protobuf/gRPC code.
pub mod proto {
    /// `databricks.zerobus` — the real Zerobus data-plane protocol. Re-exported from the shared
    /// test-support crate's generated bindings, so the mock and rust/tests share ONE set of types.
    pub mod zerobus {
        pub use zerobus_test_support::databricks::zerobus::*;
    }
    /// `zerobus.conformance.mock` — the MockAdmin control plane (owned by this crate).
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
    /// Non-graceful, non-cancel connection drops/aborts (`GetAbortCount`).
    abort_count: AtomicI64,
    /// Ingest records captured with their raw bytes (`GetCapturedRecordCount`).
    captured_record_count: AtomicI64,
    /// Highest cumulative ack sent; -1 when nothing has been acked yet.
    ack_offset: AtomicI64,
    /// Source of unique stream ids. Never reset, so ids are not reused across scenarios.
    stream_counter: AtomicU64,
    /// Bumped by `reset`. A stream task from an earlier generation stops serving.
    generation: AtomicU64,
    /// Raw wire bytes of every ingested record, in arrival order (append-only). Stores bytes
    /// only; never decodes. Ephemeral reconnects resend with reset wire offsets, so captures
    /// are appended rather than overwritten — mirrors rust/tests' `ingest_record_requests`.
    records: Mutex<Vec<(i64, Vec<u8>)>>,
    /// One-shot stream-abort injection, armed via `MockAdmin.InjectStreamError`.
    inject_armed: AtomicBool,
    inject_fired: AtomicBool,
    inject_after_offset: AtomicI64,
    inject_retryable: AtomicBool,
    /// Set by `request_shutdown`; waiters are woken through `shutdown`.
    shutting_down: AtomicBool,
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
            abort_count: AtomicI64::new(0),
            captured_record_count: AtomicI64::new(0),
            ack_offset: AtomicI64::new(-1),
            stream_counter: AtomicU64::new(0),
            generation: AtomicU64::new(0),
            records: Mutex::new(Vec::new()),
            inject_armed: AtomicBool::new(false),
            inject_fired: AtomicBool::new(false),
            inject_after_offset: AtomicI64::new(-1),
            inject_retryable: AtomicBool::new(true),
            shutting_down: AtomicBool::new(false),
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
    pub fn abort_count(&self) -> i64 {
        self.abort_count.load(Ordering::SeqCst)
    }
    pub fn captured_record_count(&self) -> i64 {
        self.captured_record_count.load(Ordering::SeqCst)
    }
    /// Highest cumulative ack sent, or -1 if none. A monotonic max across connections (reset
    /// only by MockAdmin.Reset); wire offsets restart on reconnect, so it can reflect an
    /// earlier connection — assert on counts.
    pub fn ack_offset(&self) -> i64 {
        self.ack_offset.load(Ordering::SeqCst)
    }
    /// Raw captured bytes for `offset` — the first record captured at that wire offset. A single
    /// connection has at most one; across a reconnect wire offsets reset, so recovery scenarios
    /// assert on counts, not per-offset bytes.
    pub fn record_bytes(&self, offset: i64) -> Option<Vec<u8>> {
        self.records
            .lock()
            .expect("records lock")
            .iter()
            .find(|(o, _)| *o == offset)
            .map(|(_, bytes)| bytes.clone())
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

    /// Clears counters, captured bytes and injection, and bumps the generation so a stream
    /// still open from the previous scenario stops serving. Stream ids are never reused.
    pub fn reset(&self) {
        self.generation.fetch_add(1, Ordering::SeqCst);
        self.call_count.store(0, Ordering::SeqCst);
        self.captured_message_count.store(0, Ordering::SeqCst);
        self.on_completed_count.store(0, Ordering::SeqCst);
        self.cancel_count.store(0, Ordering::SeqCst);
        self.abort_count.store(0, Ordering::SeqCst);
        self.captured_record_count.store(0, Ordering::SeqCst);
        self.ack_offset.store(-1, Ordering::SeqCst);
        self.records.lock().expect("records lock").clear();
        self.inject_armed.store(false, Ordering::SeqCst);
        self.inject_fired.store(false, Ordering::SeqCst);
        self.inject_after_offset.store(-1, Ordering::SeqCst);
        self.inject_retryable.store(true, Ordering::SeqCst);
    }

    /// Asks the serving task and every open stream to stop. Terminal; safe to call before
    /// anyone waits.
    pub fn request_shutdown(&self) {
        self.shutting_down.store(true, Ordering::SeqCst);
        self.shutdown.notify_waiters();
    }

    /// Resolves once [`request_shutdown`](Self::request_shutdown) has been called. Safe for
    /// multiple waiters (the server plus every open stream).
    pub async fn shutdown_requested(&self) {
        loop {
            if self.shutting_down.load(Ordering::SeqCst) {
                return;
            }
            // Register for a wakeup before re-checking the flag, so a request that races with
            // this call is not lost (`notify_waiters` only wakes current waiters).
            let notified = self.shutdown.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.shutting_down.load(Ordering::SeqCst) {
                return;
            }
            notified.await;
        }
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
    pub(crate) fn inc_abort(&self) {
        self.abort_count.fetch_add(1, Ordering::SeqCst);
    }
    /// Current generation; a stream task stops once this changes (a `reset`).
    pub(crate) fn generation(&self) -> u64 {
        self.generation.load(Ordering::SeqCst)
    }
    pub(crate) fn next_stream_id(&self) -> String {
        let n = self.stream_counter.fetch_add(1, Ordering::SeqCst) + 1;
        format!("mock_stream_{n}")
    }
    pub(crate) fn capture_record(&self, offset: i64, bytes: Vec<u8>) {
        self.records
            .lock()
            .expect("records lock")
            .push((offset, bytes));
        self.captured_record_count.fetch_add(1, Ordering::SeqCst);
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

    // Match the SDK's ~10 MiB record ceiling; tonic's default 4 MiB would reject valid records.
    const MAX_DECODE_BYTES: usize = 10 * 1024 * 1024;

    let shutdown = {
        let state = Arc::clone(&state);
        async move {
            tokio::select! {
                // An OS signal resolves the server future; also release open streams.
                _ = extra_shutdown => state.request_shutdown(),
                _ = state.shutdown_requested() => {}
            }
        }
    };

    // Zerobus (native gRPC) data plane and the MockAdmin control plane, both on one port.
    tonic::transport::Server::builder()
        .add_service(
            ZerobusServer::new(MockGrpc::new(Arc::clone(&state)))
                .max_decoding_message_size(MAX_DECODE_BYTES),
        )
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
