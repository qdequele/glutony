//! Control-plane binary: reads its configuration from the environment, connects to
//! Postgres, applies migrations and serves the internal HTTP API.

use anyhow::Context;
use meili_ingest_control_plane::boot::BootConfig;
use meili_ingest_control_plane::{AppState, app, db};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    // Validate the whole configuration BEFORE touching Postgres: a binary that
    // migrates and then refuses its configuration leaves the database ahead of the
    // image still running (sqlx refuses a migration it does not know).
    let BootConfig {
        database_url,
        addr,
        internal_token,
        lab: lab_config,
        jobs_retention_days,
        blob_ttl_days,
    } = BootConfig::from_env()?;
    if let Some(config) = &lab_config {
        // Also before Postgres: wrong credentials abort boot (spec §3.6).
        meili_ingest_control_plane::boot::confirm_lab_identity(config).await?;
    }

    tracing::info!("connecting to Postgres");
    let pool = db::connect(&database_url)
        .await
        .context("connecting to Postgres")?;
    db::migrate(&pool).await.context("applying migrations")?;
    tracing::info!("migrations applied");

    if internal_token.is_none() && lab_config.is_some() {
        tracing::warn!(
            "CONTROL_PLANE_TOKEN_DISABLED=true with Lab credentials: GET /internal/lab/ai \
             answers 403 token_required, so LAB_AI_GATEWAY workers cannot run Lab AI steps"
        );
    }
    // The Lab seams mean tenants: a global pipeline may then pin no connection.
    let mut state = AppState::new(pool.clone())
        .with_internal_token(internal_token)
        .with_tenants(lab_config.is_some())
        .with_blob_ttl_days(blob_ttl_days);
    if blob_ttl_days == 0 {
        tracing::info!("BLOB_TTL_DAYS=0: job blobs are kept until a bucket rule removes them");
    } else {
        tracing::info!(
            days = blob_ttl_days,
            "workers delete a job's blobs this many days after it ended"
        );
    }
    if let Some(creds) = lab_config.as_ref().map(|c| c.credentials()) {
        // Workers ask `GET /internal/lab/credits/{account}` before a source run and
        // `GET /internal/lab/ai/{account}` before an AI step. This client is on that
        // path: short timeouts, and no redirects so the bearer secret is never
        // replayed to another host.
        let http = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(2))
            .timeout(std::time::Duration::from_secs(5))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .context("cannot build the Lab HTTP client")?;
        state = state
            .with_lab_credits(std::sync::Arc::new(
                meili_ingest_lab::AccountCreditCache::new(creds.clone(), http.clone()),
            ))
            .with_lab_ai(std::sync::Arc::new(meili_ingest_lab::AiTargetCache::new(
                creds.clone(),
                http,
            )));
        tracing::info!("credit pre-check and AI target lookup enabled");
    }
    let cancel = tokio_util::sync::CancellationToken::new();
    let sender = match lab_config {
        Some(config) => {
            tracing::info!(url = %config.url(), "lab events sender enabled");
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

    let retention = jobs_retention_days.map(|days| {
        tracing::info!(days, "job and source-run history retention enabled");
        tokio::spawn(
            meili_ingest_control_plane::retention::Retention::new(pool.clone(), days)
                .run(cancel.clone()),
        )
    });

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("binding {addr}"))?;
    tracing::info!(%addr, "control plane listening");

    axum::serve(listener, app(state))
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("http server")?;

    cancel.cancel();
    for handle in [sender, retention].into_iter().flatten() {
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
