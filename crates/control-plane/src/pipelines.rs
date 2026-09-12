//! User pipeline repository (Postgres) and the `/pipelines` handlers.

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use meili_ingest_plugin_sdk::PipelineDefinition;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use sqlx::types::Json as SqlJson;

use crate::builtin_pipelines::{builtin_pipelines, is_builtin_uid, is_known_plugin};
use crate::error::CpError;
use crate::{AppState, JsonBody, project_scope};

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
    project_id: Option<String>,
}

impl PipelineRow {
    /// Turn the row into the API shape: the JSONB definition with authoritative
    /// `version`/`project_id` from the columns and `builtin` forced to `false`.
    fn into_definition(self) -> PipelineDefinition {
        let mut def = self.definition.0;
        def.uid = self.uid;
        def.version = u32::try_from(self.version).unwrap_or(1);
        def.project_id = self.project_id;
        def.builtin = false;
        def
    }
}

impl PipelineRepo {
    /// Repository over `pool`.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Global pipelines plus the ones scoped to `project_id` (when given).
    /// Tenant rows come first, then global rows; each group sorted by uid.
    pub async fn list(&self, project_id: Option<&str>) -> Result<Vec<PipelineDefinition>, CpError> {
        let rows: Vec<PipelineRow> = sqlx::query_as(
            "SELECT uid, version, definition, project_id FROM pipelines \
             WHERE project_id IS NULL OR project_id = $1 \
             ORDER BY (project_id IS NULL), uid",
        )
        .bind(project_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(PipelineRow::into_definition).collect())
    }

    /// Every user pipeline across all tenants (used to fill the resolver cache).
    pub async fn list_all(&self) -> Result<Vec<PipelineDefinition>, CpError> {
        let rows: Vec<PipelineRow> = sqlx::query_as(
            "SELECT uid, version, definition, project_id FROM pipelines \
             ORDER BY (project_id IS NULL), project_id, uid",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(PipelineRow::into_definition).collect())
    }

    /// Fetch one pipeline by uid: the tenant-scoped row when `project_id` is given and
    /// exists, otherwise the global row.
    pub async fn get(
        &self,
        uid: &str,
        project_id: Option<&str>,
    ) -> Result<Option<PipelineDefinition>, CpError> {
        let row: Option<PipelineRow> = sqlx::query_as(
            "SELECT uid, version, definition, project_id FROM pipelines \
             WHERE uid = $1 AND (project_id IS NULL OR project_id = $2) \
             ORDER BY (project_id IS NULL) LIMIT 1",
        )
        .bind(uid)
        .bind(project_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(PipelineRow::into_definition))
    }

    /// Insert or update by `(uid, project_id)`. On update the stored `version` is
    /// incremented and `updated_at` set to `now()`. Returns the stored definition.
    pub async fn upsert(&self, def: &PipelineDefinition) -> Result<PipelineDefinition, CpError> {
        let version = i32::try_from(def.version.max(1)).unwrap_or(1);
        let row: PipelineRow = sqlx::query_as(
            "INSERT INTO pipelines (uid, name, description, version, definition, project_id) \
             VALUES ($1, $2, $3, $4, $5, $6) \
             ON CONFLICT (uid, COALESCE(project_id, '')) DO UPDATE SET \
                name = EXCLUDED.name, \
                description = EXCLUDED.description, \
                version = pipelines.version + 1, \
                definition = EXCLUDED.definition, \
                updated_at = now() \
             RETURNING uid, version, definition, project_id",
        )
        .bind(&def.uid)
        .bind(&def.name)
        .bind(def.description.as_deref())
        .bind(version)
        .bind(SqlJson(def))
        .bind(def.project_id.as_deref())
        .fetch_one(&self.pool)
        .await?;
        Ok(row.into_definition())
    }

    /// Delete the row identified by `(uid, project_id)`. Returns whether a row existed.
    pub async fn delete(&self, uid: &str, project_id: Option<&str>) -> Result<bool, CpError> {
        let res = sqlx::query(
            "DELETE FROM pipelines WHERE uid = $1 AND COALESCE(project_id, '') = COALESCE($2, '')",
        )
        .bind(uid)
        .bind(project_id)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() > 0)
    }

    /// Names present in the `plugins` table (manifests registered by workers).
    pub async fn registered_plugin_names(&self) -> Result<Vec<String>, CpError> {
        let rows: Vec<(String,)> = sqlx::query_as("SELECT name FROM plugins")
            .fetch_all(&self.pool)
            .await?;
        Ok(rows.into_iter().map(|(n,)| n).collect())
    }
}

/// `?project_id=` query parameter shared by the pipeline routes.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectQuery {
    /// Tenant scope; falls back to the `X-Meili-Project-Id` header.
    #[serde(default)]
    pub project_id: Option<String>,
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

/// `GET /pipelines?project_id=` → user pipelines (tenant first, then global) followed
/// by the built-ins.
pub async fn list_pipelines(
    State(state): State<AppState>,
    Query(q): Query<ProjectQuery>,
    headers: HeaderMap,
) -> Result<Json<Vec<PipelineDefinition>>, CpError> {
    let project_id = project_scope(q.project_id.as_deref(), &headers);
    let mut out = state.pipelines().list(project_id.as_deref()).await?;
    out.extend(builtin_pipelines());
    Ok(Json(out))
}

/// `POST /pipelines` → 201 with the stored definition.
///
/// Normalizes and validates the body (422 `validation`), rejects the `builtin.`
/// namespace (403 `builtin`), rejects unknown plugins (422 `unknown_plugin`) and
/// upserts by `(uid, project_id)`. The scope comes from the body's `project_id` or
/// the `X-Meili-Project-Id` header.
pub async fn create_pipeline(
    State(state): State<AppState>,
    headers: HeaderMap,
    JsonBody(mut def): JsonBody<PipelineDefinition>,
) -> Result<Response, CpError> {
    prepare_definition(&mut def)?;
    def.project_id = project_scope(def.project_id.as_deref(), &headers);

    // Only hit the database when the static list does not already cover every step.
    let statically_unknown = unknown_plugins(&def, &[]);
    if !statically_unknown.is_empty() {
        let registered = state.pipelines().registered_plugin_names().await?;
        let unknown = unknown_plugins(&def, &registered);
        if !unknown.is_empty() {
            return Err(unknown_plugin_error(&unknown));
        }
    }

    let stored = state.pipelines().upsert(&def).await?;
    state.cache.invalidate().await;
    tracing::info!(
        uid = %stored.uid,
        project_id = ?stored.project_id,
        version = stored.version,
        "pipeline stored"
    );
    Ok((StatusCode::CREATED, Json(stored)).into_response())
}

/// `GET /pipelines/{uid}?project_id=` → the pipeline or 404.
///
/// Built-in uids are answered from the static table without touching the database.
pub async fn get_pipeline(
    State(state): State<AppState>,
    Path(uid): Path<String>,
    Query(q): Query<ProjectQuery>,
    headers: HeaderMap,
) -> Result<Json<PipelineDefinition>, CpError> {
    if is_builtin_uid(&uid) {
        return find_builtin(&uid).map(Json);
    }
    let project_id = project_scope(q.project_id.as_deref(), &headers);
    if let Some(def) = state.pipelines().get(&uid, project_id.as_deref()).await? {
        return Ok(Json(def));
    }
    find_builtin(&uid).map(Json)
}

/// `DELETE /pipelines/{uid}?project_id=` → 204, 403 for built-ins, 404 when missing.
pub async fn delete_pipeline(
    State(state): State<AppState>,
    Path(uid): Path<String>,
    Query(q): Query<ProjectQuery>,
    headers: HeaderMap,
) -> Result<StatusCode, CpError> {
    if is_builtin_uid(&uid) {
        return Err(CpError::Builtin(format!(
            "pipeline {uid:?} is built in and cannot be deleted"
        )));
    }
    let project_id = project_scope(q.project_id.as_deref(), &headers);
    if state
        .pipelines()
        .delete(&uid, project_id.as_deref())
        .await?
    {
        state.cache.invalidate().await;
        tracing::info!(uid = %uid, project_id = ?project_id, "pipeline deleted");
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(CpError::NotFound(format!(
            "pipeline {uid:?} not found (project_id={project_id:?})"
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
            project_id: None,
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
    fn unknown_plugin_error_lists_names() {
        let e = unknown_plugin_error(&["foo".into(), "bar".into()]);
        assert_eq!(e.code(), "unknown_plugin");
        assert!(e.to_string().starts_with("unknown plugin \"foo\", \"bar\""));
    }
}
