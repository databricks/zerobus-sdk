//! Layer-1 conformance harness for the Rust Zerobus SDK.
//!
//! Handshake: binds `127.0.0.1:0`, prints exactly `HARNESS_ENDPOINT=127.0.0.1:<port>`
//! to stdout, then serves the `TestHarness` gRPC service. Logs go to stderr only.
//! Exits on the `Shutdown` RPC or SIGINT/SIGTERM.

mod harness;
mod records;

/// Generated from `conformance/harness/proto/test_harness.proto` by `build.rs`.
pub mod pb {
    tonic::include_proto!("zerobus.conformance.harness");
}

use std::io::Write;

use tokio::sync::watch;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tracing_subscriber::EnvFilter;

use crate::harness::HarnessService;
use crate::pb::test_harness_server::TestHarnessServer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // stdout is reserved for the endpoint line, so all logging goes to stderr.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    {
        let mut stdout = std::io::stdout().lock();
        writeln!(stdout, "HARNESS_ENDPOINT={addr}")?;
        stdout.flush()?;
    }
    tracing::info!(%addr, "TestHarness listening");

    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let service = HarnessService::new(shutdown_tx);

    let shutdown = async move {
        tokio::select! {
            _ = shutdown_rx.wait_for(|stop| *stop) => {}
            _ = wait_for_signal() => tracing::info!("termination signal received"),
        }
    };

    Server::builder()
        .add_service(TestHarnessServer::new(service))
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown)
        .await?;

    tracing::info!("harness exited");
    Ok(())
}

/// Resolves on SIGINT or SIGTERM.
async fn wait_for_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut terminate = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
