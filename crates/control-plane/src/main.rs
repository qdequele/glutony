//! Control-plane binary: reads its configuration from the environment, connects to
//! Postgres, applies migrations and serves the internal HTTP API.

use std::net::SocketAddr;

use anyhow::Context;
use meili_ingest_control_plane::{AppState, app, db};
use tracing_subscriber::EnvFilter;

/// Default listen address (SPEC §13).
const DEFAULT_BIND: &str = "0.0.0.0:9000";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let database_url =
        std::env::var("DATABASE_URL").context("DATABASE_URL environment variable is required")?;
    let bind = std::env::var("BIND").unwrap_or_else(|_| DEFAULT_BIND.to_string());
    let addr: SocketAddr = bind
        .parse()
        .with_context(|| format!("BIND {bind:?} is not a valid socket address"))?;

    tracing::info!("connecting to Postgres");
    let pool = db::connect(&database_url)
        .await
        .context("connecting to Postgres")?;
    db::migrate(&pool).await.context("applying migrations")?;
    tracing::info!("migrations applied");

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("binding {addr}"))?;
    tracing::info!(%addr, "control plane listening");

    axum::serve(listener, app(AppState::new(pool.clone())))
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("http server")?;

    tracing::info!("shutting down");
    pool.close().await;
    Ok(())
}

/// JSON tracing filtered by `RUST_LOG` (default `info`).
fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(filter)
        .with_current_span(false)
        .init();
}

/// Resolves on SIGINT or SIGTERM.
async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(e) = tokio::signal::ctrl_c().await {
            tracing::error!(error = %e, "failed to install Ctrl+C handler");
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(e) => tracing::error!(error = %e, "failed to install SIGTERM handler"),
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    tracing::info!("shutdown signal received");
}
