//! Scheduled sources, gateway side: the control-plane calls, save-time validation, and
//! the redacted public shape. The routes live in [`crate::handlers::sources`].
//!
//! The gateway seals a source's fetch credential before it reaches the control plane
//! and never returns it; a source's *destination* is not stored here at all — it is the
//! Meilisearch connection its pipeline's `meili_indexer` step names (spec Decision 6).

use chrono::{DateTime, Utc};
use meili_ingest_plugin_sdk::PipelineDefinition;
use meili_ingest_source::{
    FetchAuth, HostPolicy, IncrementalState, Location, RunOutcome, SecretKey, SourceDefinition,
    SourceError, open_json, redact, render,
};
use serde::{Deserialize, Serialize};
use url::Url;
use uuid::Uuid;

use crate::error::GatewayError;
use crate::state::ControlPlaneClient;

// ---------------------------------------------------------------------------
// Control-plane wire types (mirror `meili-ingest-control-plane::sources`).
// ---------------------------------------------------------------------------

/// A source as the control plane stores it; the credential is still sealed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceRecord {
    /// The public shape.
    #[serde(flatten)]
    pub definition: SourceDefinition,
    /// Sealed fetch credential.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fetch_auth: Option<Vec<u8>>,
    /// What the previous run learned.
    #[serde(default)]
    pub state: IncrementalState,
    /// When the last run started.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_run_at: Option<DateTime<Utc>>,
    /// Outcome of the last run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_status: Option<String>,
    /// Error of the last run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

/// One run of a source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunRecord {
    /// Run id.
    pub run_id: Uuid,
    /// Source this run belongs to.
    pub source_id: Uuid,
    /// When the run started.
    pub started_at: DateTime<Utc>,
    /// When it finished.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<DateTime<Utc>>,
    /// Terminal outcome.
    pub outcome: RunOutcome,
    /// Source items ingested.
    #[serde(default)]
    pub items: i32,
    /// Jobs started.
    #[serde(default)]
    pub job_ids: Vec<Uuid>,
    /// Failure message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Body of `POST /internal/sources`.
#[derive(Debug, Clone, Serialize)]
pub struct NewSource {
    /// Surrogate id, which also names the schedule.
    pub id: Uuid,
    /// Handle.
    pub uid: String,
    /// Display name.
    pub name: String,
    /// Description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Tenant scope.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    /// Pipeline fed.
    pub pipeline_uid: String,
    /// Where to fetch.
    pub location: Location,
    /// Cron.
    pub cron: String,
    /// Timezone.
    pub timezone: String,
    /// Index override.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_name: Option<String>,
    /// Sealed credential.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fetch_auth: Option<Vec<u8>>,
    /// Temporal schedule id.
    pub schedule_id: String,
}

/// Body of `PATCH /internal/sources/{uid}`. `fetch_auth: Some(None)` serializes as
/// `null`, which the control plane reads as "clear".
#[derive(Debug, Clone, Default, Serialize)]
pub struct SourcePatch {
    /// New name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// New description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// New pipeline.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pipeline_uid: Option<String>,
    /// New location.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<Location>,
    /// New cron.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cron: Option<String>,
    /// New timezone.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timezone: Option<String>,
    /// New index override.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_name: Option<String>,
    /// Replace (`Some(Some)`), clear (`Some(None)`) or keep (`None`) the credential.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fetch_auth: Option<Option<Vec<u8>>>,
    /// New paused flag.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub paused: Option<bool>,
}

// ---------------------------------------------------------------------------
// Public API shapes.
// ---------------------------------------------------------------------------

fn utc() -> String {
    "UTC".to_string()
}

/// `POST /sources` body.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateSource {
    /// Handle, unique per project.
    pub uid: String,
    /// Display name; defaults to the uid.
    #[serde(default)]
    pub name: Option<String>,
    /// Description.
    #[serde(default)]
    pub description: Option<String>,
    /// Pipeline to run on every tick. Its `meili_indexer` must name a connection.
    pub pipeline: String,
    /// Where to fetch from.
    pub location: Location,
    /// Cron expression, validated by Temporal.
    pub cron: String,
    /// IANA timezone for the cron and for URL templates.
    #[serde(default = "utc")]
    pub timezone: String,
    /// Index override.
    #[serde(default)]
    pub index: Option<String>,
    /// Credential for the fetch. Sealed before it leaves the gateway.
    #[serde(default)]
    pub auth: Option<FetchAuth>,
}

/// `PATCH /sources/{uid}` body. `auth` omitted keeps it, `null` clears it.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateSource {
    /// New name.
    #[serde(default)]
    pub name: Option<String>,
    /// New description.
    #[serde(default)]
    pub description: Option<String>,
    /// New pipeline.
    #[serde(default)]
    pub pipeline: Option<String>,
    /// New location.
    #[serde(default)]
    pub location: Option<Location>,
    /// New cron.
    #[serde(default)]
    pub cron: Option<String>,
    /// New timezone.
    #[serde(default)]
    pub timezone: Option<String>,
    /// New index override.
    #[serde(default)]
    pub index: Option<String>,
    /// Replace, clear (`null`) or keep (absent) the credential.
    #[serde(default, deserialize_with = "double_option")]
    pub auth: Option<Option<FetchAuth>>,
}

fn double_option<'de, D, T>(d: D) -> Result<Option<Option<T>>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(d).map(Some)
}

/// A source as the API returns it. The credential is shown only as its kind and header
/// names, never a value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourceView {
    /// Handle.
    pub uid: String,
    /// Display name.
    pub name: String,
    /// Description.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Tenant scope.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    /// Pipeline fed.
    pub pipeline: String,
    /// Where it fetches from.
    pub location: Location,
    /// Cron.
    pub cron: String,
    /// Timezone.
    pub timezone: String,
    /// Whether the schedule is paused.
    pub paused: bool,
    /// Index override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index: Option<String>,
    /// Redacted credential, when one is set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<serde_json::Value>,
    /// Set when the source's pipeline was deleted; archived sources never fire.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub archived_at: Option<DateTime<Utc>>,
    /// Next scheduled run, on single-source reads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_run_at: Option<DateTime<Utc>>,
    /// When the last run started.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_run_at: Option<DateTime<Utc>>,
    /// Outcome of the last run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_status: Option<String>,
    /// Error of the last run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

impl SourceView {
    /// The public view of a record. `key` opens the credential only to learn its kind.
    pub fn from_record(
        r: SourceRecord,
        key: &SecretKey,
        next_run_at: Option<DateTime<Utc>>,
    ) -> Self {
        let auth = r.fetch_auth.as_deref().map(|sealed| {
            match open_json::<FetchAuth>(key, sealed) {
                Ok(auth) => redact(&auth),
                // Still say a credential exists without pretending to know its kind.
                Err(_) => serde_json::json!({ "kind": "unknown", "sealed": true }),
            }
        });
        let d = r.definition;
        Self {
            uid: d.uid,
            name: d.name,
            description: d.description,
            project_id: d.project_id,
            pipeline: d.pipeline_uid,
            location: d.location,
            cron: d.cron,
            timezone: d.timezone,
            paused: d.paused,
            index: d.index_name,
            auth,
            archived_at: d.archived_at,
            next_run_at,
            last_run_at: r.last_run_at,
            last_status: r.last_status,
            last_error: r.last_error,
        }
    }
}

// ---------------------------------------------------------------------------
// Validation.
// ---------------------------------------------------------------------------

/// Check a location can be fetched: its template renders (known tokens, known
/// timezone), the result is a URL, and the host passes `SOURCE_FETCH_HOSTS`. The fetch
/// re-checks the host at every run and every redirect; this is the early, friendly
/// failure.
pub async fn validate_location(
    location: &Location,
    timezone: &str,
    policy: &HostPolicy,
) -> Result<(), GatewayError> {
    let Location::Url { url, .. } = location;
    let rendered = render(url, Utc::now(), timezone).map_err(unprocessable)?;
    let parsed = Url::parse(&rendered).map_err(|e| {
        GatewayError::Unprocessable(format!("location {rendered:?} is not a URL: {e}"))
    })?;
    policy.check(&parsed).await.map_err(unprocessable)
}

fn unprocessable(e: SourceError) -> GatewayError {
    GatewayError::Unprocessable(e.to_string())
}

/// A scheduled run has no request, so the pipeline must supply its own destination.
pub fn require_pinned(pipeline: &PipelineDefinition) -> Result<(), GatewayError> {
    if pipeline.pins_destination() {
        Ok(())
    } else {
        Err(GatewayError::Unprocessable(format!(
            "pipeline {:?} cannot run on a schedule: its meili_indexer step names no \
             Meilisearch `connection`, and a scheduled run has no request to supply a \
             destination",
            pipeline.uid
        )))
    }
}

// ---------------------------------------------------------------------------
// Control-plane calls.
// ---------------------------------------------------------------------------

fn reword(e: GatewayError) -> GatewayError {
    match e {
        GatewayError::Invalid(msg) => GatewayError::Unprocessable(msg),
        other => other,
    }
}

fn not_found_to_none<T>(r: Result<T, GatewayError>) -> Result<Option<T>, GatewayError> {
    match r {
        Ok(v) => Ok(Some(v)),
        Err(GatewayError::NotFound(_)) => Ok(None),
        Err(e) => Err(e),
    }
}

impl ControlPlaneClient {
    fn source_url(&self, prefix: &str, key: &str, suffix: &str) -> Result<Url, GatewayError> {
        let mut url = Url::parse(&self.url(&format!("/internal/{prefix}/")))
            .map_err(|e| GatewayError::Internal(format!("control plane url: {e}")))?;
        {
            let mut segments = url.path_segments_mut().map_err(|()| {
                GatewayError::Internal("control plane url cannot be a base".into())
            })?;
            segments.pop_if_empty().push(key);
            if !suffix.is_empty() {
                segments.push(suffix);
            }
        }
        Ok(url)
    }

    /// `GET /internal/sources`.
    pub async fn list_sources(
        &self,
        project_id: Option<&str>,
        include_archived: bool,
    ) -> Result<Vec<SourceRecord>, GatewayError> {
        let mut query = Self::project_query(project_id);
        if include_archived {
            query.push(("include_archived", "true".into()));
        }
        let req = self.http.get(self.url("/internal/sources")).query(&query);
        self.send_json(req, "list sources").await
    }

    /// `GET /internal/sources/{uid}`; `Ok(None)` when missing.
    pub async fn get_source(
        &self,
        uid: &str,
        project_id: Option<&str>,
    ) -> Result<Option<SourceRecord>, GatewayError> {
        let req = self
            .http
            .get(self.source_url("sources", uid, "")?)
            .query(&Self::project_query(project_id));
        not_found_to_none(self.send_json(req, "get source").await)
    }

    /// `POST /internal/sources`.
    pub async fn create_source(&self, new: &NewSource) -> Result<SourceRecord, GatewayError> {
        let req = self.http.post(self.url("/internal/sources")).json(new);
        self.send_json(req, "create source").await.map_err(reword)
    }

    /// `PATCH /internal/sources/{uid}`; `Ok(None)` when not in exactly this scope.
    pub async fn update_source(
        &self,
        uid: &str,
        project_id: Option<&str>,
        patch: &SourcePatch,
    ) -> Result<Option<SourceRecord>, GatewayError> {
        let req = self
            .http
            .patch(self.source_url("sources", uid, "")?)
            .query(&Self::project_query(project_id))
            .json(patch);
        not_found_to_none(self.send_json(req, "update source").await).map_err(reword)
    }

    /// `DELETE /internal/sources/{uid}`.
    pub async fn delete_source(
        &self,
        uid: &str,
        project_id: Option<&str>,
    ) -> Result<(), GatewayError> {
        let req = self
            .http
            .delete(self.source_url("sources", uid, "")?)
            .query(&Self::project_query(project_id));
        self.send_empty(req, "delete source").await
    }

    /// `PUT /internal/sources-by-id/{id}/paused`.
    pub async fn set_source_paused(&self, id: Uuid, paused: bool) -> Result<(), GatewayError> {
        let req = self
            .http
            .put(self.source_url("sources-by-id", &id.to_string(), "paused")?)
            .json(&serde_json::json!({ "paused": paused }));
        self.send_empty(req, "pause source").await
    }

    /// `GET /internal/sources-by-id/{id}/runs`.
    pub async fn list_source_runs(
        &self,
        id: Uuid,
        limit: i64,
    ) -> Result<Vec<RunRecord>, GatewayError> {
        let req = self
            .http
            .get(self.source_url("sources-by-id", &id.to_string(), "runs")?)
            .query(&[("limit", limit.to_string())]);
        self.send_json(req, "list source runs").await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn url(u: &str) -> Location {
        Location::Url {
            url: u.into(),
            method: None,
            headers: BTreeMap::new(),
        }
    }

    #[tokio::test]
    async fn an_unknown_template_token_is_rejected_on_save() {
        let err = validate_location(
            &url("https://x.example/{{ nope }}"),
            "UTC",
            &HostPolicy::Any,
        )
        .await
        .expect_err("unknown token");
        assert!(matches!(err, GatewayError::Unprocessable(_)), "{err:?}");
    }

    #[tokio::test]
    async fn an_unknown_timezone_is_rejected_on_save() {
        let err = validate_location(
            &url("https://x.example/a"),
            "Mars/Olympus",
            &HostPolicy::Any,
        )
        .await
        .expect_err("unknown tz");
        assert!(err.to_string().contains("timezone"), "{err}");
    }

    #[tokio::test]
    async fn the_fetch_policy_is_applied_on_save() {
        let err = validate_location(
            &url("http://files.internal/export.json"),
            "UTC",
            &HostPolicy::Public,
        )
        .await
        .expect_err("public-only rejects http");
        assert!(matches!(err, GatewayError::Unprocessable(_)), "{err:?}");
        validate_location(
            &url("http://files.internal/export_{{ date:%Y }}.json"),
            "UTC",
            &HostPolicy::parse("files.internal").expect("policy"),
        )
        .await
        .expect("an allowlisted host is accepted");
    }

    #[test]
    fn a_pipeline_without_a_connection_cannot_be_scheduled() {
        let mut p = PipelineDefinition {
            uid: "plain".into(),
            name: "plain".into(),
            description: None,
            version: 1,
            trigger: None,
            steps: vec![meili_ingest_plugin_sdk::StepDefinition::new(
                "index",
                "meili_indexer",
            )],
            builtin: false,
            project_id: None,
        };
        let err = require_pinned(&p).expect_err("unpinned");
        assert!(err.to_string().contains("connection"), "{err}");
        p.steps[0].config = serde_json::json!({ "connection": "prod" });
        require_pinned(&p).expect("pinned");
    }

    #[test]
    fn the_view_shows_the_credential_kind_but_never_its_value() {
        let key = SecretKey::from_bytes([2; 32]);
        let sealed = meili_ingest_source::seal_json(
            &key,
            &FetchAuth::Bearer {
                token: "s3cret".into(),
            },
        )
        .expect("seal");
        let record = SourceRecord {
            definition: SourceDefinition {
                id: Uuid::nil(),
                uid: "tmdb".into(),
                name: "TMDB".into(),
                description: None,
                project_id: None,
                pipeline_uid: "movies".into(),
                location: url("https://x.example/a"),
                cron: "0 3 * * *".into(),
                timezone: "UTC".into(),
                paused: false,
                index_name: None,
                schedule_id: "source-x".into(),
                archived_at: None,
            },
            fetch_auth: Some(sealed),
            state: IncrementalState::default(),
            last_run_at: None,
            last_status: None,
            last_error: None,
        };
        let view = SourceView::from_record(record, &key, None);
        let json = serde_json::to_string(&view).expect("serialize");
        assert_eq!(
            view.auth,
            Some(serde_json::json!({"kind": "bearer", "token": "****"}))
        );
        assert!(!json.contains("s3cret"), "{json}");
    }

    #[test]
    fn a_patch_body_distinguishes_clear_from_keep() {
        let clear: UpdateSource = serde_json::from_str(r#"{"auth": null}"#).expect("json");
        assert!(matches!(clear.auth, Some(None)), "null clears");
        let keep: UpdateSource = serde_json::from_str("{}").expect("json");
        assert!(keep.auth.is_none(), "absent keeps");
        let wire = SourcePatch {
            fetch_auth: Some(None),
            ..Default::default()
        };
        assert_eq!(
            serde_json::to_value(&wire).expect("json"),
            serde_json::json!({ "fetch_auth": null }),
            "the control plane receives an explicit null"
        );
    }
}
