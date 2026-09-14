//! The types a source is made of.
//!
//! [`Location`] is a tagged union from day one so the v2 connectors (`zip`, `bucket`,
//! `api`) need no migration — only a new variant.

use std::collections::BTreeMap;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::SourceError;

/// Where a source's content comes from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Location {
    /// One HTTP(S) URL, possibly templated. The only variant in v1.
    Url {
        /// URL, rendered through [`crate::template::render`] before fetching.
        url: String,
        /// HTTP method; `GET` when absent.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        method: Option<String>,
        /// Non-secret headers. Secrets belong in [`FetchAuth`].
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        headers: BTreeMap<String, String>,
    },
}

/// How a source authenticates. Always stored sealed; never returned by the API.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum FetchAuth {
    /// `Authorization: Bearer <token>`.
    Bearer {
        /// The token.
        token: String,
    },
    /// HTTP basic auth.
    Basic {
        /// Username.
        username: String,
        /// Password.
        password: String,
    },
    /// Arbitrary secret headers, e.g. `X-Api-Key`.
    Headers {
        /// Header name → value.
        headers: BTreeMap<String, String>,
    },
}

/// Masked view of a credential, safe to return from the API and to log.
///
/// Header *names* are preserved because they are not secret and make the edit form
/// usable; every value becomes `****`.
pub fn redact(auth: &FetchAuth) -> serde_json::Value {
    match auth {
        FetchAuth::Bearer { .. } => serde_json::json!({ "kind": "bearer", "token": "****" }),
        FetchAuth::Basic { username, .. } => serde_json::json!({
            "kind": "basic",
            "username": username,
            "password": "****",
        }),
        FetchAuth::Headers { headers } => {
            let masked: BTreeMap<&str, &str> =
                headers.keys().map(|k| (k.as_str(), "****")).collect();
            serde_json::json!({ "kind": "headers", "headers": masked })
        }
    }
}

/// What the previous run learned about the upstream, used to skip unchanged content.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IncrementalState {
    /// Value of the last `ETag` response header.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,
    /// Value of the last `Last-Modified` response header.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_modified: Option<String>,
    /// Hex-encoded blake3 of the last body.
    ///
    /// Hex rather than raw bytes so it survives JSON and Temporal payloads unchanged;
    /// the `sources.last_hash` column is `TEXT` to match.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,
}

/// A source as the control plane stores it, minus its sealed secrets.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceDefinition {
    /// Surrogate id, also the Temporal schedule's suffix.
    pub id: Uuid,
    /// Handle, unique per project.
    pub uid: String,
    /// Display name.
    pub name: String,
    /// Optional description.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Tenant scope; `None` = global.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    /// Pipeline this source feeds.
    pub pipeline_uid: String,
    /// Where the content comes from.
    pub location: Location,
    /// Cron expression. Validated by Temporal on create (Decision 10).
    pub cron: String,
    /// IANA timezone for the cron and for date templating.
    #[serde(default = "utc")]
    pub timezone: String,
    /// Whether the schedule is paused.
    #[serde(default)]
    pub paused: bool,
    /// Index override; falls back to the pipeline's pattern then the deployment default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_name: Option<String>,
    /// Temporal schedule id.
    pub schedule_id: String,
    /// Set when the source's pipeline was deleted; archived sources never fire.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub archived_at: Option<DateTime<Utc>>,
}

fn utc() -> String {
    "UTC".to_string()
}

/// Terminal outcome of one source run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunOutcome {
    /// Upstream was byte-identical; no job was created and nothing was billed.
    Unchanged,
    /// At least one job was started.
    Ingested,
    /// The run failed before starting any job.
    Failed,
}

impl RunOutcome {
    /// Stable string stored in `source_runs.outcome`.
    pub fn as_str(self) -> &'static str {
        match self {
            RunOutcome::Unchanged => "unchanged",
            RunOutcome::Ingested => "ingested",
            RunOutcome::Failed => "failed",
        }
    }
}

impl FromStr for RunOutcome {
    type Err = SourceError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "unchanged" => Ok(RunOutcome::Unchanged),
            "ingested" => Ok(RunOutcome::Ingested),
            "failed" => Ok(RunOutcome::Failed),
            other => Err(SourceError::Payload(format!(
                "unknown run outcome {other:?}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn location_is_tagged_by_kind() {
        let loc = Location::Url {
            url: "https://example.test/a.json".into(),
            method: None,
            headers: BTreeMap::new(),
        };
        let json = serde_json::to_value(&loc).expect("serialize");
        assert_eq!(json["kind"], "url");
        assert_eq!(json["url"], "https://example.test/a.json");
        let back: Location = serde_json::from_value(json).expect("deserialize");
        assert_eq!(back, loc);
    }

    #[test]
    fn an_unknown_location_kind_is_rejected() {
        // v2 connectors add variants; until then an unknown kind must not silently
        // deserialize into the url variant.
        let json = serde_json::json!({ "kind": "bucket", "uri": "s3://b/p/" });
        assert!(serde_json::from_value::<Location>(json).is_err());
    }

    #[test]
    fn an_unknown_field_is_rejected() {
        let json = serde_json::json!({
            "kind": "url",
            "url": "https://example.test/a",
            "typo": true,
        });
        assert!(serde_json::from_value::<Location>(json).is_err());
    }

    #[test]
    fn fetch_auth_is_tagged_by_kind() {
        let auth = FetchAuth::Bearer {
            token: "t0ken".into(),
        };
        let json = serde_json::to_value(&auth).expect("serialize");
        assert_eq!(json["kind"], "bearer");
        let back: FetchAuth = serde_json::from_value(json).expect("deserialize");
        assert_eq!(back, auth);
    }

    #[test]
    fn redact_never_exposes_a_secret() {
        for auth in [
            FetchAuth::Bearer {
                token: "t0ken".into(),
            },
            FetchAuth::Basic {
                username: "u".into(),
                password: "p4ss".into(),
            },
            FetchAuth::Headers {
                headers: BTreeMap::from([("X-Api-Key".to_string(), "k3y".to_string())]),
            },
        ] {
            let rendered = serde_json::to_string(&redact(&auth)).expect("serialize");
            assert!(rendered.contains("****"), "{rendered} must be masked");
            for secret in ["t0ken", "p4ss", "k3y"] {
                assert!(!rendered.contains(secret), "{rendered} leaked {secret}");
            }
        }
    }

    #[test]
    fn redact_keeps_the_kind_and_non_secret_names() {
        let auth = FetchAuth::Headers {
            headers: BTreeMap::from([("X-Api-Key".to_string(), "k3y".to_string())]),
        };
        let json = redact(&auth);
        assert_eq!(json["kind"], "headers");
        // The header NAME is not a secret and is useful when editing.
        assert_eq!(json["headers"]["X-Api-Key"], "****");
    }

    #[test]
    fn run_outcome_roundtrips_through_its_string() {
        for o in [
            RunOutcome::Unchanged,
            RunOutcome::Ingested,
            RunOutcome::Failed,
        ] {
            assert_eq!(o.as_str().parse::<RunOutcome>().expect("parses"), o);
        }
        assert!("nonsense".parse::<RunOutcome>().is_err());
    }

    #[test]
    fn incremental_state_defaults_to_empty() {
        let s = IncrementalState::default();
        assert!(s.etag.is_none() && s.last_modified.is_none() && s.hash.is_none());
    }

    #[test]
    fn a_source_definition_roundtrips_and_defaults_timezone() {
        let json = serde_json::json!({
            "id": "11111111-1111-1111-1111-111111111111",
            "uid": "tmdb",
            "name": "TMDB daily export",
            "pipeline_uid": "builtin.json",
            "location": { "kind": "url", "url": "https://example.test/a.json" },
            "cron": "30 0 * * *",
            "schedule_id": "source-tmdb",
        });
        let def: SourceDefinition = serde_json::from_value(json).expect("deserialize");
        assert_eq!(def.timezone, "UTC", "timezone defaults to UTC");
        assert!(!def.paused);
        assert!(def.archived_at.is_none());
        let back: SourceDefinition =
            serde_json::from_value(serde_json::to_value(&def).expect("serialize"))
                .expect("deserialize");
        assert_eq!(back, def);
    }
}
