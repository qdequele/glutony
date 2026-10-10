//! Worker configuration from environment variables (SPEC §13 "Worker").

use anyhow::Context;

/// Runtime configuration of a worker process.
#[derive(Clone)]
pub struct WorkerConfig {
    /// Temporal frontend URL, e.g. `http://temporal-frontend:7233`.
    pub temporal_url: String,
    /// Temporal namespace.
    pub temporal_namespace: String,
    /// Task queue this worker polls (`workers-general` or `workers-io`).
    pub task_queue: String,
    /// Control plane base URL, used to publish plugin manifests at boot.
    pub control_plane_url: Option<String>,
    /// Bearer token for the control plane's `/internal/*` routes (`CONTROL_PLANE_TOKEN`).
    pub control_plane_token: Option<String>,
    /// Blob store URL (`BLOB_STORE_URL`).
    pub blob_store_url: Option<String>,
    /// Outputs larger than this many bytes are spilled to the blob store.
    pub payload_spill_bytes: usize,
    /// `EXTERNAL_PLUGINS` spec string (`wasm:/path.wasm,grpc:http://host:50051`).
    pub external_plugins: Option<String>,
    /// Maximum number of concurrent activities on this worker.
    pub max_concurrent_activities: usize,
    /// Post Lab billing events to the control plane's outbox (`LAB_EVENTS_ENABLED`).
    pub lab_events_enabled: bool,
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
            control_plane_token: control_plane_token(std::env::var("CONTROL_PLANE_TOKEN").ok())?,
            blob_store_url: std::env::var("BLOB_STORE_URL")
                .ok()
                .filter(|s| !s.is_empty()),
            payload_spill_bytes,
            external_plugins: std::env::var("EXTERNAL_PLUGINS")
                .ok()
                .filter(|s| !s.is_empty()),
            max_concurrent_activities,
            lab_events_enabled: parse_flag(std::env::var("LAB_EVENTS_ENABLED").ok().as_deref()),
        })
    }
}

impl std::fmt::Debug for WorkerConfig {
    /// Redacts `control_plane_token`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerConfig")
            .field("temporal_url", &self.temporal_url)
            .field("temporal_namespace", &self.temporal_namespace)
            .field("task_queue", &self.task_queue)
            .field("control_plane_url", &self.control_plane_url)
            .field(
                "control_plane_token",
                &self.control_plane_token.as_ref().map(|_| "<redacted>"),
            )
            .field("blob_store_url", &self.blob_store_url)
            .field("payload_spill_bytes", &self.payload_spill_bytes)
            .field("external_plugins", &self.external_plugins)
            .field("max_concurrent_activities", &self.max_concurrent_activities)
            .field("lab_events_enabled", &self.lab_events_enabled)
            .finish()
    }
}

/// `CONTROL_PLANE_TOKEN`, trimmed; blank is unset. The example Secret's `CHANGE_ME`
/// placeholder is refused: it is a public value.
fn control_plane_token(raw: Option<String>) -> anyhow::Result<Option<String>> {
    let token = raw.map(|s| s.trim().to_string()).filter(|s| !s.is_empty());
    if token.as_deref() == Some("CHANGE_ME") {
        anyhow::bail!(
            "CONTROL_PLANE_TOKEN is the example placeholder CHANGE_ME; set the real token \
             the control plane uses (openssl rand -hex 32)"
        );
    }
    Ok(token)
}

/// `true`/`1` (any case) -> on; anything else or unset -> off.
fn parse_flag(raw: Option<&str>) -> bool {
    raw.map(str::trim)
        .is_some_and(|v| v == "1" || v.eq_ignore_ascii_case("true"))
}

/// Whether Lab events would be priced with the bundled placeholder list prices: they
/// are enabled and the loaded provider cost table is the compiled-in one (no
/// `PROVIDER_COSTS_FILE`, or an unedited copy of it).
pub fn lab_events_use_placeholder_prices(
    lab_events_enabled: bool,
    costs: &meili_ingest_plugin_sdk::cost::ProviderCosts,
) -> bool {
    lab_events_enabled && *costs == meili_ingest_plugin_sdk::cost::ProviderCosts::bundled()
}

#[cfg(test)]
mod tests {
    #[test]
    fn placeholder_prices_are_flagged_only_for_lab_events() {
        use meili_ingest_plugin_sdk::cost::ProviderCosts;
        let bundled = ProviderCosts::bundled();
        assert!(super::lab_events_use_placeholder_prices(true, &bundled));
        // An unedited copy of the bundled file is the same table.
        let copy =
            ProviderCosts::from_toml(include_str!("../../../k8s/provider-costs.toml")).unwrap();
        assert!(super::lab_events_use_placeholder_prices(true, &copy));
        assert!(!super::lab_events_use_placeholder_prices(false, &bundled));
        let own = ProviderCosts::from_toml(
            "[llm_enricher.default]\ninput_per_mtok = 1\noutput_per_mtok = 2\n",
        )
        .unwrap();
        assert!(!super::lab_events_use_placeholder_prices(true, &own));
    }

    #[test]
    fn lab_events_flag_parsing() {
        for (raw, expected) in [
            ("true", true),
            ("1", true),
            ("TRUE", true),
            ("false", false),
            ("0", false),
            ("", false),
        ] {
            assert_eq!(super::parse_flag(Some(raw)), expected, "{raw:?}");
        }
        assert!(!super::parse_flag(None));
    }

    #[test]
    fn the_control_plane_token_refuses_the_placeholder() {
        assert_eq!(super::control_plane_token(None).unwrap(), None);
        assert_eq!(super::control_plane_token(Some(" ".into())).unwrap(), None);
        assert_eq!(
            super::control_plane_token(Some(" tok\n".into()))
                .unwrap()
                .as_deref(),
            Some("tok")
        );
        for placeholder in ["CHANGE_ME", " CHANGE_ME\n"] {
            let err = super::control_plane_token(Some(placeholder.into()))
                .unwrap_err()
                .to_string();
            assert!(
                err.contains("CONTROL_PLANE_TOKEN") && err.contains("CHANGE_ME"),
                "{err}"
            );
        }
    }

    #[test]
    fn debug_redacts_the_control_plane_token() {
        let cfg = super::WorkerConfig {
            temporal_url: String::new(),
            temporal_namespace: String::new(),
            task_queue: String::new(),
            control_plane_url: None,
            control_plane_token: Some("cp-token-value".into()),
            blob_store_url: None,
            payload_spill_bytes: 0,
            external_plugins: None,
            max_concurrent_activities: 1,
            lab_events_enabled: false,
        };
        let shown = format!("{cfg:?}");
        assert!(!shown.contains("cp-token-value"), "{shown}");
        assert!(shown.contains("<redacted>"), "{shown}");
    }
}
