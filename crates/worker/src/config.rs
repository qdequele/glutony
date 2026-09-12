//! Worker configuration from environment variables (SPEC §13 "Worker").

use anyhow::Context;

/// Runtime configuration of a worker process.
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// Temporal frontend URL, e.g. `http://temporal-frontend:7233`.
    pub temporal_url: String,
    /// Temporal namespace.
    pub temporal_namespace: String,
    /// Task queue this worker polls (`workers-general`, `workers-llm`, `workers-gpu`, `workers-io`).
    pub task_queue: String,
    /// Control plane base URL, used to publish plugin manifests at boot.
    pub control_plane_url: Option<String>,
    /// Blob store URL (`BLOB_STORE_URL`).
    pub blob_store_url: Option<String>,
    /// Outputs larger than this many bytes are spilled to the blob store.
    pub payload_spill_bytes: usize,
    /// `EXTERNAL_PLUGINS` spec string (`wasm:/path.wasm,grpc:http://host:50051`).
    pub external_plugins: Option<String>,
    /// Maximum number of concurrent activities on this worker.
    pub max_concurrent_activities: usize,
}

impl WorkerConfig {
    /// Read configuration from the environment.
    pub fn from_env() -> anyhow::Result<Self> {
        let payload_spill_bytes = match std::env::var("PAYLOAD_SPILL_BYTES") {
            Ok(v) => v
                .parse::<usize>()
                .context("PAYLOAD_SPILL_BYTES must be an integer number of bytes")?,
            Err(_) => 1024 * 1024,
        };
        let max_concurrent_activities = match std::env::var("MAX_CONCURRENT_ACTIVITIES") {
            Ok(v) => v
                .parse::<usize>()
                .context("MAX_CONCURRENT_ACTIVITIES must be an integer")?,
            Err(_) => 20,
        };
        Ok(Self {
            temporal_url: std::env::var("TEMPORAL_URL")
                .unwrap_or_else(|_| "http://temporal-frontend:7233".to_string()),
            temporal_namespace: std::env::var("TEMPORAL_NAMESPACE")
                .unwrap_or_else(|_| "default".to_string()),
            task_queue: std::env::var("TASK_QUEUE")
                .unwrap_or_else(|_| "workers-general".to_string()),
            control_plane_url: std::env::var("CONTROL_PLANE_URL")
                .ok()
                .filter(|s| !s.is_empty()),
            blob_store_url: std::env::var("BLOB_STORE_URL")
                .ok()
                .filter(|s| !s.is_empty()),
            payload_spill_bytes,
            external_plugins: std::env::var("EXTERNAL_PLUGINS")
                .ok()
                .filter(|s| !s.is_empty()),
            max_concurrent_activities,
        })
    }
}
