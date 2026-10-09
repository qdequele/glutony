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
    let url: Url = config
        .temporal_url
        .parse()
        .with_context(|| format!("invalid TEMPORAL_URL {:?}", config.temporal_url))?;
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        match Connection::connect(ConnectionOptions::new(url.clone()).build()).await {
            Ok(connection) => {
                let client = Client::new(
                    connection,
                    ClientOptions::new(config.temporal_namespace.clone()).build(),
                )
                .context("cannot build Temporal client")?;
                tracing::info!(url = %config.temporal_url, namespace = %config.temporal_namespace, "connected to Temporal");
                return Ok(client);
            }
            Err(e) if attempt < 10 => {
                let backoff = Duration::from_secs(u64::from(attempt).min(5));
                tracing::warn!(
                    attempt,
                    "Temporal connection failed ({e}); retrying in {backoff:?}"
                );
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
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let config = GatewayConfig::from_env().context("invalid gateway configuration")?;
    config.validate()?;
    tracing::info!(config = ?config, "starting meili-gateway");
    if config.envoy_trusted_header.is_none() {
        tracing::warn!("ENVOY_TRUSTED_HEADER is unset: trusting X-Meili-* headers from any client");
    }
    if !meili_ingest_gateway::auth::management_auth_enabled(&config) {
        tracing::warn!(
            "LAB_SERVICE_TOKEN and ADMIN_API_KEY are unset: management routes are open \
             (tenant from trusted X-Meili-* headers); keep them off any public hostname"
        );
    }

    let blob = BlobStore::from_url(&config.blob_store_url)
        .with_context(|| format!("cannot open blob store {:?}", config.blob_store_url))?;
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .context("cannot build HTTP client")?;
    let temporal = connect_temporal(&config).await?;
    let bind = config.bind.clone();
    // Meilisearch connections. Without SOURCE_SECRET_KEY the /connections routes answer
    // 501 rather than store a key unsealed; a malformed key or host policy stops the
    // gateway instead of silently running with something other than what was written.
    let connection_key = meili_ingest_source::SecretKey::from_env()
        .context("invalid SOURCE_SECRET_KEY")?
        .map(Arc::new);
    let host_policy =
        meili_ingest_source::HostPolicy::from_env().context("invalid MEILI_CONNECTION_HOSTS")?;
    if connection_key.is_none() {
        tracing::warn!("SOURCE_SECRET_KEY is not set: /connections is disabled (501)");
    }
    tracing::info!(policy = ?host_policy, "Meilisearch connection host policy");

    let fetch_policy =
        meili_ingest_source::HostPolicy::from_env_var(meili_ingest_source::FETCH_HOSTS_ENV)
            .context("invalid SOURCE_FETCH_HOSTS")?;
    tracing::info!(policy = ?fetch_policy, "scheduled-source fetch host policy");

    // Lab identity and credit pre-check (spec v2 §3.6, §8.1). Optional: without
    // LAB_INSTANCE_* this gateway never talks to the Lab. One synchronous attempt
    // first: a 401 aborts boot (wrong or revoked credentials); any other failure is
    // retried in the background, and Lab-account jobs are refused until it succeeds.
    let lab = match meili_ingest_lab::LabCredentials::from_env()
        .context("invalid LAB_* configuration")?
    {
        Some(creds) => {
            tracing::info!(
                url = creds.url(),
                instance_id = creds.instance_id(),
                "Lab client enabled"
            );
            // The credit check runs on the request path: its own client with short
            // timeouts (the shared one waits 30 s), and no redirects so the bearer
            // secret is never replayed to another host.
            let lab_http = reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(2))
                .timeout(Duration::from_secs(5))
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .context("cannot build the Lab HTTP client")?;
            let client = Arc::new(meili_ingest_gateway::lab::LabClient::new(creds, lab_http));
            match client.refresh_identity().await {
                Ok(info) => tracing::info!(
                    kind = ?info.kind,
                    product = %info.product,
                    region = ?info.region,
                    "Lab instance identity confirmed"
                ),
                Err(meili_ingest_lab::LabError::Unauthorized) => anyhow::bail!(
                    "the Lab rejected LAB_INSTANCE_ID / LAB_INSTANCE_SECRET (401); fix the credentials"
                ),
                Err(e) => {
                    tracing::warn!(error = %e, "could not confirm the Lab identity; retrying in the background");
                    let resolver = client.clone();
                    tokio::spawn(async move {
                        resolver.resolve_identity(Duration::from_secs(30)).await
                    });
                }
            }
            Some(client)
        }
        None => None,
    };

    let schedules = Arc::new(meili_ingest_gateway::schedules::TemporalSchedules(
        temporal.clone(),
    ));
    let mut state = AppState::new(config, Arc::new(TemporalStarter(temporal)), blob, http)
        .with_connections(meili_ingest_gateway::connections::ConnectionConfig::new(
            connection_key,
            host_policy,
        ))
        .with_sources(schedules, fetch_policy);
    if let Some(lab) = lab {
        state = state.with_lab(lab);
    }
    let app = meili_ingest_gateway::router(state);

    let listener = tokio::net::TcpListener::bind(&bind)
        .await
        .with_context(|| format!("cannot bind {bind}"))?;
    tracing::info!(bind = %bind, "listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("server error")?;
    tracing::info!("meili-gateway stopped");
    Ok(())
}
