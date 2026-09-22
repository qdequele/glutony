//! meili-ingest worker binary. Polls one Temporal task queue (`TASK_QUEUE`) and runs
//! the `PipelineWorkflow` plus the `execute_step` / `expand_fan_out` activities.

use std::str::FromStr;
use std::sync::Arc;

use anyhow::Context;
use meili_ingest_blob::BlobStore;
use meili_ingest_worker::{PipelineWorkflow, PluginRegistry, StepActivities, WorkerConfig};
use temporalio_client::{Client, ClientOptions, Connection, ConnectionOptions, Url};
use temporalio_sdk::runtime::worker_tuner::{FixedSizeSlotSupplier, TunerHolder};
use temporalio_sdk::{Runtime, Worker, WorkerOptions};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = dotenvy::dotenv();
    init_tracing();
    let config = WorkerConfig::from_env()?;
    tracing::info!(
        task_queue = %config.task_queue,
        temporal_url = %config.temporal_url,
        namespace = %config.temporal_namespace,
        "starting meili-ingest worker"
    );

    // Plugins
    let mut registry = PluginRegistry::builtin();
    if let Some(spec) = &config.external_plugins {
        registry.load_external(spec).await;
    }
    tracing::info!(plugins = ?registry.names(), unavailable = ?registry.unavailable(), "plugins registered");
    let registry = Arc::new(registry);

    // Blob store
    let blob = match &config.blob_store_url {
        Some(url) => BlobStore::from_url(url).context("invalid BLOB_STORE_URL")?,
        None => BlobStore::from_env()?,
    };

    // Publish manifests to the control plane (best effort).
    if let Some(cp) = &config.control_plane_url {
        publish_manifests(cp, &registry).await;
    }

    // Temporal
    let url = Url::from_str(&config.temporal_url).context("invalid TEMPORAL_URL")?;
    let runtime = Runtime::from_current_tokio(Default::default())?;
    let connection = Connection::connect(ConnectionOptions::new(url).build())
        .await
        .context("connecting to Temporal")?;
    let client = Client::new(
        connection,
        ClientOptions::new(config.temporal_namespace.clone()).build(),
    )?;

    // Usage reporting is optional: without TINYBIRD_TOKEN the activity is a no-op.
    let usage = meili_ingest_usage::UsageClient::from_env()
        .context("invalid usage analytics configuration")?;
    match &usage {
        Some(c) => tracing::info!(datasource = c.datasource(), "usage analytics enabled"),
        None => tracing::warn!("usage analytics disabled (TINYBIRD_TOKEN is not set)"),
    }

    // Meilisearch connections: optional, so a worker without SOURCE_SECRET_KEY still
    // runs every request-driven pipeline and only fails steps that name a connection. A
    // malformed policy, unlike a missing key, stops the worker: silently falling back
    // to a different policy than the operator wrote would be worse than not starting.
    let connection_key = meili_ingest_source::SecretKey::from_env()
        .context("invalid SOURCE_SECRET_KEY")?
        .map(Arc::new);
    let host_policy =
        meili_ingest_source::HostPolicy::from_env().context("invalid MEILI_CONNECTION_HOSTS")?;
    if connection_key.is_none() {
        tracing::warn!(
            "SOURCE_SECRET_KEY is not set: steps that name a Meilisearch connection will fail"
        );
    }
    tracing::info!(policy = ?host_policy, "Meilisearch connection host policy");

    let activities = StepActivities::new(registry, blob, config.payload_spill_bytes)
        .with_usage(usage)
        .with_control_plane(config.control_plane_url.clone())
        .with_connections(meili_ingest_worker::connection::ConnectionSettings {
            key: connection_key,
            policy: host_policy,
        });
    let tuner = TunerHolder::builder()
        .workflow_task_slot_supplier(FixedSizeSlotSupplier::new(50))
        .activity_task_slot_supplier(FixedSizeSlotSupplier::new(config.max_concurrent_activities))
        .local_activity_task_slot_supplier(FixedSizeSlotSupplier::new(10))
        .nexus_task_slot_supplier(FixedSizeSlotSupplier::new(10))
        .build();
    let options = WorkerOptions::new(config.task_queue.clone())
        .register_workflow::<PipelineWorkflow>()?
        .register_activities(activities)
        .tuner(tuner)
        .graceful_shutdown_period(std::time::Duration::from_secs(30))
        .build();
    let mut worker = Worker::new(&runtime, client, options)?;

    let shutdown = worker.shutdown_handle();
    tokio::spawn(async move {
        wait_for_signal().await;
        tracing::info!("shutdown signal received, draining worker");
        shutdown();
    });

    tracing::info!(task_queue = %config.task_queue, "worker polling");
    worker.run().await.context("worker run failed")?;
    tracing::info!("worker stopped");
    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let json = std::env::var("LOG_FORMAT")
        .map(|v| v != "text")
        .unwrap_or(true);
    if json {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .json()
            .init();
    } else {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    }
}

async fn wait_for_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, "cannot listen for SIGTERM");
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = term.recv() => {},
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

async fn publish_manifests(control_plane_url: &str, registry: &PluginRegistry) {
    let url = format!(
        "{}/internal/plugins",
        control_plane_url.trim_end_matches('/')
    );
    let manifests = registry.manifests();
    match reqwest::Client::new()
        .post(&url)
        .json(&manifests)
        .send()
        .await
    {
        Ok(resp) if resp.status().is_success() => {
            tracing::info!(
                count = manifests.len(),
                "published plugin manifests to control plane"
            );
        }
        Ok(resp) => {
            tracing::warn!(status = %resp.status(), "control plane rejected plugin manifests");
        }
        Err(e) => {
            tracing::warn!(error = %e, url = %url, "could not publish plugin manifests");
        }
    }
}
