//! User pipeline repository (Postgres) and the `/pipelines` handlers.

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use meili_ingest_plugin_sdk::{
    INDEXER_PLUGIN, PipelineDefinition, PluginManifest, pinned_connection,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use sqlx::types::Json as SqlJson;

use crate::builtin_pipelines::{builtin_pipelines, is_builtin_uid, is_known_plugin};
use crate::error::CpError;
use crate::{AppState, JsonBody, tenant_scope};

/// Repository over the `pipelines` table.
#[derive(Debug, Clone)]
pub struct PipelineRepo {
    pool: PgPool,
}

/// One row of `pipelines`, as read back from Postgres.
#[derive(Debug, sqlx::FromRow)]
struct PipelineRow {
    uid: String,
    version: i32,
    definition: SqlJson<PipelineDefinition>,
    tenant_id: Option<String>,
}

impl PipelineRow {
    /// Turn the row into the API shape: the JSONB definition with authoritative
    /// `version`/`tenant_id` from the columns and `builtin` forced to `false`.
    fn into_definition(self) -> PipelineDefinition {
        let mut def = self.definition.0;
        def.uid = self.uid;
        def.version = u32::try_from(self.version).unwrap_or(1);
        def.tenant_id = self.tenant_id;
        def.builtin = false;
        def
    }
}

impl PipelineRepo {
    /// Repository over `pool`.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Global pipelines plus the ones scoped to `tenant_id` (when given).
    /// Tenant rows come first, then global rows; each group sorted by uid.
    pub async fn list(&self, tenant_id: Option<&str>) -> Result<Vec<PipelineDefinition>, CpError> {
        let rows: Vec<PipelineRow> = sqlx::query_as(
            "SELECT uid, version, definition, tenant_id FROM pipelines \
             WHERE tenant_id IS NULL OR tenant_id = $1 \
             ORDER BY (tenant_id IS NULL), uid",
        )
        .bind(tenant_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(PipelineRow::into_definition).collect())
    }

    /// Every user pipeline across all tenants (used to fill the resolver cache).
    pub async fn list_all(&self) -> Result<Vec<PipelineDefinition>, CpError> {
        let rows: Vec<PipelineRow> = sqlx::query_as(
            "SELECT uid, version, definition, tenant_id FROM pipelines \
             ORDER BY (tenant_id IS NULL), tenant_id, uid",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(PipelineRow::into_definition).collect())
    }

    /// Fetch one pipeline by uid: the tenant-scoped row when `tenant_id` is given and
    /// exists, otherwise the global row.
    pub async fn get(
        &self,
        uid: &str,
        tenant_id: Option<&str>,
    ) -> Result<Option<PipelineDefinition>, CpError> {
        let row: Option<PipelineRow> = sqlx::query_as(
            "SELECT uid, version, definition, tenant_id FROM pipelines \
             WHERE uid = $1 AND (tenant_id IS NULL OR tenant_id = $2) \
             ORDER BY (tenant_id IS NULL) LIMIT 1",
        )
        .bind(uid)
        .bind(tenant_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(PipelineRow::into_definition))
    }

    /// Insert or update by `(uid, tenant_id)`. On update the stored `version` is
    /// incremented and `updated_at` set to `now()`. Returns the stored definition.
    pub async fn upsert(&self, def: &PipelineDefinition) -> Result<PipelineDefinition, CpError> {
        let version = i32::try_from(def.version.max(1)).unwrap_or(1);
        let row: PipelineRow = sqlx::query_as(
            "INSERT INTO pipelines (uid, name, description, version, definition, tenant_id) \
             VALUES ($1, $2, $3, $4, $5, $6) \
             ON CONFLICT (uid, COALESCE(tenant_id, '')) DO UPDATE SET \
                name = EXCLUDED.name, \
                description = EXCLUDED.description, \
                version = pipelines.version + 1, \
                definition = EXCLUDED.definition, \
                updated_at = now() \
             RETURNING uid, version, definition, tenant_id",
        )
        .bind(&def.uid)
        .bind(&def.name)
        .bind(def.description.as_deref())
        .bind(version)
        .bind(SqlJson(def))
        .bind(def.tenant_id.as_deref())
        .fetch_one(&self.pool)
        .await?;
        Ok(row.into_definition())
    }

    /// Delete the row identified by `(uid, tenant_id)`. Returns whether a row existed.
    pub async fn delete(&self, uid: &str, tenant_id: Option<&str>) -> Result<bool, CpError> {
        let res = sqlx::query(
            "DELETE FROM pipelines WHERE uid = $1 AND COALESCE(tenant_id, '') = COALESCE($2, '')",
        )
        .bind(uid)
        .bind(tenant_id)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() > 0)
    }

    /// Delete a pipeline and archive every source that feeds from it, in one
    /// transaction. Returns whether the pipeline existed and the ids of the sources
    /// archived, so the caller can delete their Temporal schedules.
    ///
    /// Deleting is **never blocked** by a source referencing the pipeline. Archiving
    /// rather than cascade-deleting the sources keeps the credentials a tenant supplied
    /// by hand: destroying those as a side effect of an unrelated delete is not
    /// recoverable, whereas an archived source can be repointed and unarchived.
    pub async fn delete_cascading(
        &self,
        uid: &str,
        tenant_id: Option<&str>,
    ) -> Result<(bool, Vec<uuid::Uuid>), CpError> {
        let mut tx = self.pool.begin().await?;

        let archived: Vec<(uuid::Uuid,)> = sqlx::query_as(
            "UPDATE sources SET archived_at = now(), paused = true, updated_at = now() \
             WHERE pipeline_uid = $1 \
               AND COALESCE(tenant_id, '') = COALESCE($2, '') \
               AND archived_at IS NULL \
             RETURNING id",
        )
        .bind(uid)
        .bind(tenant_id)
        .fetch_all(&mut *tx)
        .await?;

        let res = sqlx::query(
            "DELETE FROM pipelines WHERE uid = $1 AND COALESCE(tenant_id, '') = COALESCE($2, '')",
        )
        .bind(uid)
        .bind(tenant_id)
        .execute(&mut *tx)
        .await?;

        let deleted = res.rows_affected() > 0;
        if !deleted {
            // Nothing was deleted, so nothing should have been archived either.
            tx.rollback().await?;
            return Ok((false, Vec::new()));
        }
        tx.commit().await?;
        Ok((deleted, archived.into_iter().map(|(id,)| id).collect()))
    }
}

/// `?tenant_id=` query parameter shared by the pipeline routes.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantQuery {
    /// Tenant scope; falls back to the `X-Meili-Project-Id` header.
    #[serde(default, alias = "project_id")]
    pub tenant_id: Option<String>,
}

/// Plugin names used by `def` that are neither in [`builtin_plugin_names`] nor in
/// `registered` (deduplicated, in step order).
///
/// [`builtin_plugin_names`]: crate::builtin_pipelines::builtin_plugin_names
pub fn unknown_plugins(def: &PipelineDefinition, registered: &[String]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for step in &def.steps {
        let name = step.plugin.as_str();
        if is_known_plugin(name) || registered.iter().any(|r| r == name) {
            continue;
        }
        if !out.iter().any(|o| o == name) {
            out.push(name.to_owned());
        }
    }
    out
}

/// Static checks shared by the handler and unit tests: reserved uid, normalization
/// and structural validation. Mutates `def` (normalization) on success.
pub fn prepare_definition(def: &mut PipelineDefinition) -> Result<(), CpError> {
    if is_builtin_uid(&def.uid) {
        return Err(CpError::Builtin(format!(
            "pipeline uid {:?} is reserved: built-in pipelines cannot be created or overwritten",
            def.uid
        )));
    }
    def.normalize();
    def.validate()
        .map_err(|e| CpError::Validation(e.to_string()))?;
    def.builtin = false;
    Ok(())
}

/// Every step whose `config` does not satisfy its plugin's `config_schema`, one message
/// per violation, prefixed with the step id and plugin.
///
/// The schemas are the ones `GET /plugins` serves (JSON Schema draft 2020-12). A step
/// whose plugin has no manifest in `manifests` is skipped: with no worker registered
/// yet there is nothing to check against, and the run will still fail loudly. A schema
/// that does not compile is skipped too (and logged), so one broken external plugin
/// cannot block every pipeline that uses it. `format` is an annotation only, as the
/// draft says: the custom formats (`meili-connection`, `jev-questions`) have their own
/// checks.
pub fn config_errors(def: &PipelineDefinition, manifests: &[PluginManifest]) -> Vec<String> {
    let mut out = Vec::new();
    for step in &def.steps {
        let Some(manifest) = manifests.iter().find(|m| m.name == step.plugin) else {
            continue;
        };
        let validator = match jsonschema::validator_for(&manifest.config_schema) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(plugin = %manifest.name, "config_schema does not compile: {e}");
                continue;
            }
        };
        for e in validator.iter_errors(&step.config) {
            let at = e.instance_path().to_string();
            let at = if at.is_empty() {
                String::new()
            } else {
                format!(" at {at}")
            };
            out.push(format!(
                "step {:?} ({}): invalid config{at}: {e}",
                step.id, step.plugin
            ));
        }
    }
    out
}

/// The checks that need the database, shared by validate and create so the two can
/// never disagree: unknown plugins (422 `unknown_plugin`), step configs against their
/// plugin's schema and indexer connections that do not exist for the tenant (both 422
/// `validation`).
///
/// The connection lookup is the one the worker does at run time (the tenant's row, else
/// the global one), so a pipeline that passes here does not fail later with
/// "connection not found" unless the connection is deleted in between.
async fn check_against_registry(state: &AppState, def: &PipelineDefinition) -> Result<(), CpError> {
    let manifests = crate::plugins::registered_manifests(&state.pool).await?;
    let registered: Vec<String> = manifests.iter().map(|m| m.name.clone()).collect();
    let unknown = unknown_plugins(def, &registered);
    if !unknown.is_empty() {
        return Err(unknown_plugin_error(&unknown));
    }

    let mut errors = config_errors(def, &manifests);
    let connections = state.connections();
    for step in def.steps.iter().filter(|s| s.plugin == INDEXER_PLUGIN) {
        let Some(uid) = pinned_connection(&step.config) else {
            continue;
        };
        if connections
            .get(uid, def.tenant_id.as_deref())
            .await?
            .is_none()
        {
            errors.push(format!(
                "step {:?} ({}): connection {uid:?} does not exist for this tenant",
                step.id, step.plugin
            ));
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(CpError::Validation(errors.join("; ")))
    }
}

/// Build the `unknown plugin` error for a list of names.
fn unknown_plugin_error(names: &[String]) -> CpError {
    let list = names
        .iter()
        .map(|n| format!("{n:?}"))
        .collect::<Vec<_>>()
        .join(", ");
    CpError::UnknownPlugin(format!(
        "unknown plugin {list}: not built in and not registered by any worker"
    ))
}

/// `GET /pipelines?tenant_id=` → user pipelines (tenant first, then global) followed
/// by the built-ins.
pub async fn list_pipelines(
    State(state): State<AppState>,
    Query(q): Query<TenantQuery>,
    headers: HeaderMap,
) -> Result<Json<Vec<PipelineDefinition>>, CpError> {
    let tenant_id = tenant_scope(q.tenant_id.as_deref(), &headers);
    let mut out = state.pipelines().list(tenant_id.as_deref()).await?;
    out.extend(builtin_pipelines());
    Ok(Json(out))
}

/// Result of a dry-run validation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ValidationResult {
    /// Always `true`; a rejected pipeline is returned as a 422 error instead.
    pub valid: bool,
    /// Topological execution order of the steps.
    pub order: Vec<String>,
    /// The definition after normalization, so a caller can see the `depends_on`
    /// links that the implicit sequential rule filled in.
    pub normalized: PipelineDefinition,
}

/// `POST /pipelines/validate` → 200, or 422/403 with the same errors a create would
/// produce. Persists nothing.
///
/// Exists so an editor can tell the author their pipeline has a cycle, an unknown
/// plugin, a bad step config, a missing connection or a bad fan-out *before* saving.
/// It runs the identical code path as [`create_pipeline`] so the two can never
/// disagree.
pub async fn validate_pipeline(
    State(state): State<AppState>,
    headers: HeaderMap,
    JsonBody(mut def): JsonBody<PipelineDefinition>,
) -> Result<Json<ValidationResult>, CpError> {
    prepare_definition(&mut def)?;
    def.tenant_id = tenant_scope(def.tenant_id.as_deref(), &headers);
    check_against_registry(&state, &def).await?;

    let order = def
        .validate()
        .map_err(|e| CpError::Validation(e.to_string()))?;
    Ok(Json(ValidationResult {
        valid: true,
        order,
        normalized: def,
    }))
}

/// `POST /pipelines` → 201 with the stored definition.
///
/// Normalizes and validates the body (422 `validation`), rejects the `builtin.`
/// namespace (403 `builtin`), rejects unknown plugins (422 `unknown_plugin`), step
/// configs that break their plugin's schema and indexer connections the tenant does
/// not have (422 `validation`), and upserts by `(uid, tenant_id)`. The scope comes from the body's `tenant_id` or
/// the `X-Meili-Project-Id` header.
pub async fn create_pipeline(
    State(state): State<AppState>,
    headers: HeaderMap,
    JsonBody(mut def): JsonBody<PipelineDefinition>,
) -> Result<Response, CpError> {
    prepare_definition(&mut def)?;
    def.tenant_id = tenant_scope(def.tenant_id.as_deref(), &headers);
    check_against_registry(&state, &def).await?;

    let stored = state.pipelines().upsert(&def).await?;
    state.cache.invalidate().await;
    tracing::info!(
        uid = %stored.uid,
        tenant_id = ?stored.tenant_id,
        version = stored.version,
        "pipeline stored"
    );
    Ok((StatusCode::CREATED, Json(stored)).into_response())
}

/// `GET /pipelines/{uid}?tenant_id=` → the pipeline or 404.
///
/// Built-in uids are answered from the static table without touching the database.
pub async fn get_pipeline(
    State(state): State<AppState>,
    Path(uid): Path<String>,
    Query(q): Query<TenantQuery>,
    headers: HeaderMap,
) -> Result<Json<PipelineDefinition>, CpError> {
    if is_builtin_uid(&uid) {
        return find_builtin(&uid).map(Json);
    }
    let tenant_id = tenant_scope(q.tenant_id.as_deref(), &headers);
    if let Some(def) = state.pipelines().get(&uid, tenant_id.as_deref()).await? {
        return Ok(Json(def));
    }
    find_builtin(&uid).map(Json)
}

/// Body of `DELETE /pipelines/{uid}`: the sources the delete archived.
///
/// Returned so the gateway can delete their Temporal schedules; without it they would
/// keep firing into a source that can no longer run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeletedPipeline {
    /// Ids of the sources archived because they fed this pipeline.
    pub archived_sources: Vec<uuid::Uuid>,
}

/// `DELETE /pipelines/{uid}?tenant_id=` → 200 [`DeletedPipeline`], 403 for built-ins,
/// 404 when missing.
pub async fn delete_pipeline(
    State(state): State<AppState>,
    Path(uid): Path<String>,
    Query(q): Query<TenantQuery>,
    headers: HeaderMap,
) -> Result<Json<DeletedPipeline>, CpError> {
    if is_builtin_uid(&uid) {
        return Err(CpError::Builtin(format!(
            "pipeline {uid:?} is built in and cannot be deleted"
        )));
    }
    let tenant_id = tenant_scope(q.tenant_id.as_deref(), &headers);
    let (deleted, archived) = state
        .pipelines()
        .delete_cascading(&uid, tenant_id.as_deref())
        .await?;
    if deleted {
        state.cache.invalidate().await;
        if !archived.is_empty() {
            // The gateway deletes the corresponding Temporal schedules; logging the ids
            // here is what makes an orphaned schedule traceable if that step fails.
            tracing::info!(
                uid = %uid,
                archived_sources = ?archived,
                "pipeline deleted; dependent sources archived"
            );
        }
        tracing::info!(uid = %uid, tenant_id = ?tenant_id, "pipeline deleted");
        Ok(Json(DeletedPipeline {
            archived_sources: archived,
        }))
    } else {
        Err(CpError::NotFound(format!(
            "pipeline {uid:?} not found (tenant_id={tenant_id:?})"
        )))
    }
}

fn find_builtin(uid: &str) -> Result<PipelineDefinition, CpError> {
    builtin_pipelines()
        .into_iter()
        .find(|p| p.uid == uid)
        .ok_or_else(|| CpError::NotFound(format!("pipeline {uid:?} not found")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use meili_ingest_plugin_sdk::StepDefinition;

    fn def(uid: &str, plugins: &[&str]) -> PipelineDefinition {
        PipelineDefinition {
            uid: uid.into(),
            name: String::new(),
            description: None,
            version: 1,
            trigger: None,
            steps: plugins
                .iter()
                .enumerate()
                .map(|(i, p)| StepDefinition::new(format!("s{i}"), *p))
                .collect(),
            builtin: false,
            tenant_id: None,
        }
    }

    #[test]
    fn unknown_plugins_ignores_builtin_and_registered() {
        let d = def(
            "p",
            &["pdf_extractor", "custom_a", "custom_b", "custom_a", "ocr"],
        );
        assert_eq!(unknown_plugins(&d, &[]), vec!["custom_a", "custom_b"]);
        assert_eq!(
            unknown_plugins(&d, &["custom_a".to_string()]),
            vec!["custom_b"]
        );
        assert!(unknown_plugins(&d, &["custom_a".into(), "custom_b".into()]).is_empty());
    }

    #[test]
    fn prepare_rejects_builtin_namespace() {
        let mut d = def("builtin.pdf", &["pdf_extractor"]);
        let err = prepare_definition(&mut d).err().map(|e| e.code());
        assert_eq!(err, Some("builtin"));
    }

    #[test]
    fn prepare_normalizes_and_validates() {
        let mut d = def("ok", &["pdf_extractor", "chunker", "meili_indexer"]);
        d.builtin = true; // clients cannot claim builtin status
        prepare_definition(&mut d).unwrap();
        assert_eq!(d.name, "ok");
        assert_eq!(d.steps[1].depends_on, vec!["s0"]);
        assert_eq!(d.steps[2].depends_on, vec!["s1"]);
        assert!(!d.builtin);

        let mut empty = def("empty", &[]);
        let err = prepare_definition(&mut empty)
            .err()
            .map(|e| (e.code(), e.to_string()));
        assert_eq!(
            err,
            Some(("validation", "pipeline has no steps".to_string()))
        );

        let mut cyclic = def("cyc", &["chunker", "chunker"]);
        cyclic.steps[0].depends_on = vec!["s1".into()];
        cyclic.steps[1].depends_on = vec!["s0".into()];
        let err = prepare_definition(&mut cyclic).err().map(|e| e.code());
        assert_eq!(err, Some("validation"));
    }

    #[test]
    fn config_errors_name_the_step_the_plugin_and_the_field() {
        let chunker = PluginManifest::new("chunker", "0.1.0").config_schema(serde_json::json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "additionalProperties": false,
            "properties": { "chunk_size": { "type": "integer", "minimum": 1 } }
        }));
        let mut d = def("p", &["chunker", "meili_indexer"]);
        d.steps[0].config = serde_json::json!({ "chunk_size": "big", "nope": 1 });
        let errors = config_errors(&d, std::slice::from_ref(&chunker));
        assert_eq!(errors.len(), 2, "{errors:?}");
        assert!(
            errors[0].starts_with("step \"s0\" (chunker): invalid config"),
            "{errors:?}"
        );
        assert!(
            errors
                .iter()
                .any(|e| e.contains("/chunk_size") && e.contains("\"big\"")),
            "{errors:?}"
        );
        assert!(errors.iter().any(|e| e.contains("nope")), "{errors:?}");

        d.steps[0].config = serde_json::json!({ "chunk_size": 64 });
        assert!(config_errors(&d, &[chunker]).is_empty());
    }

    #[test]
    fn config_errors_skip_unregistered_plugins_and_broken_schemas() {
        let mut d = def("p", &["chunker", "custom"]);
        d.steps[0].config = serde_json::json!({ "anything": true });
        assert!(config_errors(&d, &[]).is_empty(), "no manifest, no check");

        let broken = PluginManifest::new("chunker", "0.1.0")
            .config_schema(serde_json::json!({ "type": "not-a-type" }));
        assert!(config_errors(&d, &[broken]).is_empty());
    }

    #[test]
    fn unknown_plugin_error_lists_names() {
        let e = unknown_plugin_error(&["foo".into(), "bar".into()]);
        assert_eq!(e.code(), "unknown_plugin");
        assert!(e.to_string().starts_with("unknown plugin \"foo\", \"bar\""));
    }

    #[test]
    fn tenant_query_accepts_the_old_key() {
        use axum::extract::Query;
        let uri: axum::http::Uri = "http://cp/pipelines?project_id=t1".parse().unwrap();
        let Query(q) = Query::<TenantQuery>::try_from_uri(&uri).unwrap();
        assert_eq!(q.tenant_id.as_deref(), Some("t1"));
        let uri: axum::http::Uri = "http://cp/pipelines?tenant_id=t2".parse().unwrap();
        let Query(q) = Query::<TenantQuery>::try_from_uri(&uri).unwrap();
        assert_eq!(q.tenant_id.as_deref(), Some("t2"));
    }
}
