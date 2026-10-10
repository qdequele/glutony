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
    let internal_token = meili_ingest_control_plane::control_plane_token_policy(
        std::env::var("CONTROL_PLANE_TOKEN").ok(),
        std::env::var("CONTROL_PLANE_TOKEN_DISABLED")
            .map(|v| v.trim().eq_ignore_ascii_case("true") || v.trim() == "1")
            .unwrap_or(false),
    )?;
    let addr: SocketAddr = bind
        .parse()
        .with_context(|| format!("BIND {bind:?} is not a valid socket address"))?;

    tracing::info!("connecting to Postgres");
    let pool = db::connect(&database_url)
        .await
        .context("connecting to Postgres")?;
    db::migrate(&pool).await.context("applying migrations")?;
    tracing::info!("migrations applied");

    let lab_config = meili_ingest_control_plane::lab_sender::LabConfig::from_env()
        .context("invalid Lab events configuration")?;
    let mut state = AppState::new(pool.clone()).with_internal_token(internal_token);
    if let Some(creds) = lab_config.as_ref().and_then(|c| c.credentials()) {
        // Workers ask `GET /internal/lab/credits/{account}` before a source run; this
        // client is on that path: short timeouts, and no redirects so the bearer
        // secret is never replayed to another host.
        let http = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(2))
            .timeout(std::time::Duration::from_secs(5))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .context("cannot build the Lab HTTP client")?;
        state = state.with_lab_credits(std::sync::Arc::new(
            meili_ingest_lab::AccountCreditCache::new(creds.clone(), http),
        ));
        tracing::info!("credit pre-check for source runs enabled");
    }
    let cancel = tokio_util::sync::CancellationToken::new();
    let sender = match lab_config {
        Some(config) => {
            tracing::info!(url = %config.url, legacy = config.is_legacy(), "lab events sender enabled");
            if let Some(creds) = config.credentials() {
                // Bounded like the sender's own client: a hung Lab must not block boot.
                let http = reqwest::Client::builder()
                    .connect_timeout(std::time::Duration::from_secs(2))
                    .timeout(std::time::Duration::from_secs(10))
                    .build()?;
                match meili_ingest_lab::fetch_instance_info(&http, creds).await {
                    Ok(info) => {
                        // Credentials of another product's engine abort boot: its
                        // events would be attributed to that product.
                        meili_ingest_lab::check_product(&info)?;
                        tracing::info!(
                            kind = ?info.kind,
                            product = %info.product,
                            region = ?info.region,
                            "Lab instance identity confirmed"
                        )
                    }
                    // Spec §3.6: a 401 aborts boot; the credentials are wrong or revoked.
                    Err(meili_ingest_lab::LabError::Unauthorized) => anyhow::bail!(
                        "the Lab rejected LAB_INSTANCE_ID / LAB_INSTANCE_SECRET (401); fix the credentials"
                    ),
                    Err(e) => tracing::warn!(
                        error = %e,
                        "could not confirm this deployment's Lab identity; events are sent anyway and retried"
                    ),
                }
            }
            let sender = meili_ingest_control_plane::lab_sender::LabSender::new(
                meili_ingest_control_plane::lab_events::LabEventRepo::new(pool.clone()),
                config,
                state.metrics.clone(),
                state.lab_notify.clone(),
            )?;
            Some(tokio::spawn(sender.run(cancel.clone())))
        }
        None => {
            tracing::info!("LAB_URL is unset: lab events are kept in the outbox, not sent");
            None
        }
    };

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("binding {addr}"))?;
    tracing::info!(%addr, "control plane listening");

    axum::serve(listener, app(state))
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("http server")?;

    cancel.cancel();
    if let Some(handle) = sender {
        let _ = handle.await;
    }

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
