//! `zerobus-mock-server`: launches the Zerobus data plane and MockAdmin on one
//! `127.0.0.1` port. Prints `MOCK_ENDPOINT=127.0.0.1:<port>` to stdout (the only
//! stdout output); logs go to stderr. Exits on MockAdmin.Shutdown, SIGINT, or SIGTERM.

use std::future::pending;
use std::io::Write;
use std::sync::Arc;

use tokio::net::TcpListener;
use tracing::info;
use tracing_subscriber::EnvFilter;
use zerobus_mock_server::{serve_with_shutdown, MockState};

/// Resolves on SIGINT or SIGTERM. If a handler cannot be installed, that signal is
/// ignored (pending) rather than triggering an immediate shutdown.
async fn os_shutdown_signal() {
    let ctrl_c = async {
        if tokio::signal::ctrl_c().await.is_err() {
            pending::<()>().await;
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(_) => pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;

    {
        // Handshake line for the orchestrator: exactly one line on stdout, flushed.
        let mut stdout = std::io::stdout().lock();
        writeln!(stdout, "MOCK_ENDPOINT={addr}")?;
        stdout.flush()?;
    }
    info!("zerobus-mock-server listening on {addr}");

    let state = Arc::new(MockState::new());
    serve_with_shutdown(listener, state, os_shutdown_signal()).await?;

    info!("zerobus-mock-server stopped");
    Ok(())
}
