//! Worker configuration from environment variables (SPEC §13 "Worker").

use anyhow::Context;
use meili_ingest_plugin_sdk::env::flag;

/// Runtime configuration of a worker process.
#[derive(Clone)]
pub struct WorkerConfig {
    /// Temporal frontend URL, e.g. `http://temporal-frontend:7233`.
    pub temporal_url: String,
    /// Temporal namespace.
    pub temporal_namespace: String,
    /// Task queue this worker polls (`workers-general`, where every plugin runs).
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
    /// Run the AI steps of Lab accounts on the team's Gateway, with the target the
    /// control plane serves (`LAB_AI_GATEWAY`).
    pub lab_ai_gateway: bool,
    /// How long in-flight activities may run after SIGTERM / SIGINT before they are
    /// cancelled and handed back to Temporal (`SHUTDOWN_GRACE_SECS`, default 60).
    pub shutdown_grace: std::time::Duration,
    /// Bounds on one archive upload (`ARCHIVE_MAX_FILES`, `ARCHIVE_MAX_FILE_MB`,
    /// `ARCHIVE_MAX_UNPACKED_MB`, `ARCHIVE_MAX_DEPTH`).
    pub archive_limits: meili_ingest_archive::Limits,
    /// Decompressed archive content held in memory at once, in bytes
    /// (`ARCHIVE_INFLIGHT_MB`).
    pub archive_inflight_bytes: u64,
    /// Bounds on one Parquet / Avro / MessagePack step (`PARSER_MAX_ROWS`,
    /// `PARSER_MAX_DECODED_MB`).
    pub parser_limits: meili_ingest_plugin_json::DecodeLimits,
}

/// Default of `SHUTDOWN_GRACE_SECS`: with [`SHUTDOWN_HARD_MARGIN`] the worker is gone
/// 90 s after SIGTERM, inside ECS's 120 s stop timeout and the Kubernetes manifests'
/// `terminationGracePeriodSeconds: 120`.
pub const DEFAULT_SHUTDOWN_GRACE_SECS: u64 = 60;

/// After the grace period, how long cancelled activities get to report back before
/// the process exits anyway.
pub const SHUTDOWN_HARD_MARGIN: std::time::Duration = std::time::Duration::from_secs(30);

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
        let shutdown_grace = shutdown_grace(std::env::var("SHUTDOWN_GRACE_SECS").ok().as_deref())?;
        let archive_limits = archive_limits(|name| std::env::var(name).ok())?;
        let archive_inflight_bytes = archive_inflight_bytes(|name| std::env::var(name).ok())?;
        let parser_limits = parser_limits(|name| std::env::var(name).ok())?;
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
            lab_events_enabled: lab_events_enabled(
                std::env::var("LAB_EVENTS_ENABLED").ok().as_deref(),
            ),
            lab_ai_gateway: lab_ai_gateway(std::env::var("LAB_AI_GATEWAY").ok().as_deref()),
            shutdown_grace,
            archive_limits,
            archive_inflight_bytes,
            parser_limits,
        })
    }
}

/// The archive limits, each from its variable when set (blank is unset). Sizes are in
/// MiB. Zero is refused: an archive that may hold nothing is a misconfiguration.
pub fn archive_limits(
    env: impl Fn(&str) -> Option<String>,
) -> anyhow::Result<meili_ingest_archive::Limits> {
    let read = |name: &str| positive_var(&env, name);
    let defaults = meili_ingest_archive::Limits::default();
    let as_usize = |n: u64| usize::try_from(n).unwrap_or(usize::MAX);
    Ok(meili_ingest_archive::Limits {
        max_files: read("ARCHIVE_MAX_FILES")?.map_or(defaults.max_files, as_usize),
        max_file_bytes: read("ARCHIVE_MAX_FILE_MB")?
            .map_or(defaults.max_file_bytes, |n| n.saturating_mul(MIB)),
        max_total_bytes: read("ARCHIVE_MAX_UNPACKED_MB")?
            .map_or(defaults.max_total_bytes, |n| n.saturating_mul(MIB)),
        max_depth: read("ARCHIVE_MAX_DEPTH")?.map_or(defaults.max_depth, as_usize),
    })
}

const MIB: u64 = 1024 * 1024;

/// A positive integer variable; blank is unset, zero and anything else refused.
fn positive_var(env: &impl Fn(&str) -> Option<String>, name: &str) -> anyhow::Result<Option<u64>> {
    match env(name)
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
    {
        None => Ok(None),
        Some(v) => match v.parse::<u64>() {
            Ok(0) => anyhow::bail!("{name} must be greater than 0"),
            Ok(n) => Ok(Some(n)),
            Err(_) => anyhow::bail!("{name} must be a positive integer, got {v:?}"),
        },
    }
}

/// `ARCHIVE_INFLIGHT_MB`, in bytes: how much decompressed archive content the worker
/// holds in memory at once (default [`crate::archive_activity::DEFAULT_INFLIGHT_BYTES`]).
/// The archive activities raise it to `ARCHIVE_MAX_FILE_MB` when it is lower.
pub fn archive_inflight_bytes(env: impl Fn(&str) -> Option<String>) -> anyhow::Result<u64> {
    Ok(positive_var(&env, "ARCHIVE_INFLIGHT_MB")?
        .map_or(crate::archive_activity::DEFAULT_INFLIGHT_BYTES, |n| {
            n.saturating_mul(MIB)
        }))
}

/// What one `parquet_parser` / `avro_parser` / `msgpack_parser` step may decode
/// (`PARSER_MAX_ROWS`, `PARSER_MAX_DECODED_MB`), each from its variable when set
/// (blank is unset), with the same rules as [`archive_limits`].
pub fn parser_limits(
    env: impl Fn(&str) -> Option<String>,
) -> anyhow::Result<meili_ingest_plugin_json::DecodeLimits> {
    let defaults = meili_ingest_plugin_json::DecodeLimits::default();
    Ok(meili_ingest_plugin_json::DecodeLimits {
        max_rows: positive_var(&env, "PARSER_MAX_ROWS")?.unwrap_or(defaults.max_rows),
        max_decoded_bytes: positive_var(&env, "PARSER_MAX_DECODED_MB")?
            .map_or(defaults.max_decoded_bytes, |n| n.saturating_mul(MIB)),
    })
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
            .field("lab_ai_gateway", &self.lab_ai_gateway)
            .field("shutdown_grace", &self.shutdown_grace)
            .field("archive_limits", &self.archive_limits)
            .field("archive_inflight_bytes", &self.archive_inflight_bytes)
            .field("parser_limits", &self.parser_limits)
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

/// `SHUTDOWN_GRACE_SECS`: whole seconds; blank or unset is the default.
fn shutdown_grace(raw: Option<&str>) -> anyhow::Result<std::time::Duration> {
    let secs = match raw.map(str::trim).filter(|s| !s.is_empty()) {
        Some(v) => v
            .parse::<u64>()
            .context("SHUTDOWN_GRACE_SECS must be an integer number of seconds")?,
        None => DEFAULT_SHUTDOWN_GRACE_SECS,
    };
    Ok(std::time::Duration::from_secs(secs))
}

/// `LAB_EVENTS_ENABLED`: a [`flag`], off unless set.
fn lab_events_enabled(raw: Option<&str>) -> bool {
    flag("LAB_EVENTS_ENABLED", raw)
}

/// `LAB_AI_GATEWAY`: a [`flag`], off unless set.
fn lab_ai_gateway(raw: Option<&str>) -> bool {
    flag("LAB_AI_GATEWAY", raw)
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
    fn shutdown_grace_parsing() {
        use std::time::Duration;
        assert_eq!(
            super::shutdown_grace(None).unwrap(),
            Duration::from_secs(60)
        );
        assert_eq!(
            super::shutdown_grace(Some(" ")).unwrap(),
            Duration::from_secs(60)
        );
        assert_eq!(
            super::shutdown_grace(Some("45\n")).unwrap(),
            Duration::from_secs(45)
        );
        assert!(super::shutdown_grace(Some("1m")).is_err());
    }

    #[test]
    fn lab_events_flag_parsing() {
        for (raw, expected) in [
            ("true", true),
            ("1", true),
            ("TRUE", true),
            ("yes", true),
            ("On", true),
            ("false", false),
            ("0", false),
            ("off", false),
            ("", false),
            ("enabled", false),
        ] {
            assert_eq!(super::lab_events_enabled(Some(raw)), expected, "{raw:?}");
        }
        assert!(!super::lab_events_enabled(None));
    }

    #[test]
    fn lab_ai_gateway_flag_parsing() {
        assert!(super::lab_ai_gateway(Some(" true\n")));
        assert!(super::lab_ai_gateway(Some("1")));
        assert!(super::lab_ai_gateway(Some("yes")));
        assert!(super::lab_ai_gateway(Some("ON")));
        assert!(!super::lab_ai_gateway(Some("no")));
        assert!(!super::lab_ai_gateway(Some("y")), "unrecognised is off");
        assert!(!super::lab_ai_gateway(None), "off unless asked for");
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
            lab_ai_gateway: true,
            shutdown_grace: std::time::Duration::from_secs(1),
            archive_limits: meili_ingest_archive::Limits::default(),
            archive_inflight_bytes: crate::archive_activity::DEFAULT_INFLIGHT_BYTES,
            parser_limits: meili_ingest_plugin_json::DecodeLimits::default(),
        };
        let shown = format!("{cfg:?}");
        assert!(!shown.contains("cp-token-value"), "{shown}");
        assert!(shown.contains("<redacted>"), "{shown}");
        assert!(shown.contains("lab_ai_gateway: true"), "{shown}");
    }

    #[test]
    fn archive_limits_default_and_parse() {
        let none = |_: &str| None;
        assert_eq!(
            super::archive_limits(none).unwrap(),
            meili_ingest_archive::Limits::default()
        );
        let env = |name: &str| match name {
            "ARCHIVE_MAX_FILES" => Some("200".to_string()),
            "ARCHIVE_MAX_FILE_MB" => Some(" 5 ".to_string()),
            "ARCHIVE_MAX_UNPACKED_MB" => Some("".to_string()),
            "ARCHIVE_MAX_DEPTH" => Some("1".to_string()),
            _ => None,
        };
        let l = super::archive_limits(env).unwrap();
        assert_eq!(l.max_files, 200);
        assert_eq!(l.max_file_bytes, 5 * 1024 * 1024);
        assert_eq!(
            l.max_total_bytes,
            meili_ingest_archive::Limits::default().max_total_bytes
        );
        assert_eq!(l.max_depth, 1);
        for bad in ["0", "-1", "lots"] {
            let env = |name: &str| (name == "ARCHIVE_MAX_FILES").then(|| bad.to_string());
            let err = super::archive_limits(env).unwrap_err().to_string();
            assert!(err.contains("ARCHIVE_MAX_FILES"), "{err}");
        }
    }

    #[test]
    fn archive_inflight_budget_default_and_parse() {
        assert_eq!(
            super::archive_inflight_bytes(|_| None).unwrap(),
            crate::archive_activity::DEFAULT_INFLIGHT_BYTES
        );
        let env = |name: &str| (name == "ARCHIVE_INFLIGHT_MB").then(|| " 64 ".to_string());
        assert_eq!(
            super::archive_inflight_bytes(env).unwrap(),
            64 * 1024 * 1024
        );
        for bad in ["0", "-1", "lots"] {
            let env = |name: &str| (name == "ARCHIVE_INFLIGHT_MB").then(|| bad.to_string());
            let err = super::archive_inflight_bytes(env).unwrap_err().to_string();
            assert!(err.contains("ARCHIVE_INFLIGHT_MB"), "{err}");
        }
    }

    #[test]
    fn parser_limits_default_and_parse() {
        use meili_ingest_plugin_json::DecodeLimits;
        assert_eq!(
            super::parser_limits(|_| None).unwrap(),
            DecodeLimits::default()
        );
        let env = |name: &str| match name {
            "PARSER_MAX_ROWS" => Some(" 5000 ".to_string()),
            "PARSER_MAX_DECODED_MB" => Some("64".to_string()),
            _ => None,
        };
        let l = super::parser_limits(env).unwrap();
        assert_eq!(l.max_rows, 5000);
        assert_eq!(l.max_decoded_bytes, 64 * 1024 * 1024);
        let blank = |name: &str| (name == "PARSER_MAX_ROWS").then(String::new);
        assert_eq!(
            super::parser_limits(blank).unwrap().max_rows,
            DecodeLimits::default().max_rows
        );
        for var in ["PARSER_MAX_ROWS", "PARSER_MAX_DECODED_MB"] {
            for bad in ["0", "-1", "lots"] {
                let env = |name: &str| (name == var).then(|| bad.to_string());
                let err = super::parser_limits(env).unwrap_err().to_string();
                assert!(err.contains(var), "{err}");
            }
        }
    }
}
