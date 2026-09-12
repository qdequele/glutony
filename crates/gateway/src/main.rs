//! `meili-gateway` binary: loads the configuration, connects to Temporal, builds the
//! router and serves it with graceful shutdown.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use meili_ingest_blob::BlobStore;
use meili_ingest_gateway::state::{AppState, GatewayConfig, TemporalStarter};
use temporalio_client::{Client, ClientOptions, Connection, ConnectionOptions, Url};
use tracing_subscriber::EnvFilter;

/// Connect to Temporal, retrying a few times so the gateway survives starting before
/// the Temporal frontend is reachable.
async fn connect_temporal(config: &GatewayConfig) -> anyhow::Result<Client> {
    let url: Url = config.temporal_url.parse().with_context(|| format!("invalid TEMPORAL_URL {:?}", config.temporal_url))?;
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        match Connection::connect(ConnectionOptions::new(url.clone()).build()).await {
            Ok(connection) => {
                let client = Client::new(connection, ClientOptions::new(config.temporal_namespace.clone()).build())
                    .context("cannot build Temporal client")?;
                tracing::info!(url = %config.temporal_url, namespace = %config.temporal_namespace, "connected to Temporal");
                return Ok(client);
            }
            Err(e) if attempt < 10 => {
                let backoff = Duration::from_secs(u64::from(attempt).min(5));
                tracing::warn!(attempt, "Temporal connection failed ({e}); retrying in {backoff:?}");
                tokio::time::sleep(backoff).await;
            }
            Err(e) => return Err(anyhow::Error::new(e).context("cannot connect to Temporal")),
        }
    }
}

/// Resolve on SIGTERM or ctrl-c.
async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(e) = tokio::signal::ctrl_c().await {
            tracing::error!("failed to listen for ctrl-c: {e}");
        }
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(e) => tracing::error!("failed to listen for SIGTERM: {e}"),
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let config = GatewayConfig::from_env().context("invalid gateway configuration")?;
    tracing::info!(config = ?config, "starting meili-gateway");
    if config.envoy_trusted_header.is_none() {
        tracing::warn!("ENVOY_TRUSTED_HEADER is unset: trusting X-Meili-* headers from any client");
    }

    let blob = BlobStore::from_url(&config.blob_store_url)
        .with_context(|| format!("cannot open blob store {:?}", config.blob_store_url))?;
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .context("cannot build HTTP client")?;
    let temporal = connect_temporal(&config).await?;
    let bind = config.bind.clone();
    let state = AppState::new(config, Arc::new(TemporalStarter(temporal)), blob, http);
    let app = meili_ingest_gateway::router(state);

    let listener = tokio::net::TcpListener::bind(&bind).await.with_context(|| format!("cannot bind {bind}"))?;
    tracing::info!(bind = %bind, "listening");
    axum::serve(listener, app).with_graceful_shutdown(shutdown_signal()).await.context("server error")?;
    tracing::info!("meili-gateway stopped");
    Ok(())
}
