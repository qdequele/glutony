//! Meilisearch connection repository (`meili_connections`).
//!
//! A connection is a named, tenant-scoped Meilisearch destination: a host and a sealed
//! API key. A pipeline's `meili_indexer` step references one by uid, so the key lives in
//! exactly one place and pipeline JSON never contains a secret (spec Decision 11).
//!
//! Like [`crate::sources`], this repository never decrypts: the key moves through it as
//! opaque sealed bytes. Only the gateway (to validate a connection on save) and the
//! worker activity (to use it) hold a `SecretKey`.
//!
//! Scoping follows [`crate::pipelines::PipelineRepo`]: reads return the tenant's row
//! when it exists and fall back to the global one, while writes touch exactly the
//! caller's scope — a tenant must never be able to repoint or delete a global
//! connection.

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use chrono::{DateTime, Utc};
use meili_ingest_plugin_sdk::INDEXER_PLUGIN;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

use crate::error::CpError;
use crate::pipelines::TenantQuery;
use crate::{AppState, JsonBody, tenant_scope};

/// Postgres `unique_violation`.
const UNIQUE_VIOLATION: &str = "23505";

/// Everything needed to create a connection. The key arrives already sealed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewConnection {
    /// Surrogate id.
    pub id: Uuid,
    /// The name an indexer step references.
    pub uid: String,
    /// Display name.
    pub name: String,
    /// Tenant scope; `None` = global.
    #[serde(default, skip_serializing_if = "Option::is_none", alias = "project_id")]
    pub tenant_id: Option<String>,
    /// Meilisearch URL. Not secret.
    pub host: String,
    /// Sealed API key.
    pub api_key: Vec<u8>,
}

/// Partial update. Absent fields are left untouched, which is what makes a `PATCH` that
/// omits the key keep it. A key can be replaced but never cleared: a connection without
/// one is meaningless.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectionPatch {
    /// New display name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// New host.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    /// New sealed key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key: Option<Vec<u8>>,
}

/// A connection as stored, key still sealed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::FromRow)]
pub struct ConnectionRecord {
    /// Surrogate id.
    pub id: Uuid,
    /// The name an indexer step references.
    pub uid: String,
    /// Display name.
    pub name: String,
    /// Tenant scope; `None` = global.
    #[serde(default, skip_serializing_if = "Option::is_none", alias = "project_id")]
    pub tenant_id: Option<String>,
    /// Meilisearch URL.
    pub host: String,
    /// Sealed API key.
    pub api_key: Vec<u8>,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Last write.
    pub updated_at: DateTime<Utc>,
}

/// Repository over `meili_connections`.
#[derive(Debug, Clone)]
pub struct ConnectionRepo {
    pool: PgPool,
}

impl ConnectionRepo {
    /// Repository over `pool`.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Global connections plus the ones scoped to `tenant_id`; tenant rows first.
    pub async fn list(&self, tenant_id: Option<&str>) -> Result<Vec<ConnectionRecord>, CpError> {
        Ok(sqlx::query_as(
            "SELECT id, uid, name, tenant_id, host, api_key, created_at, updated_at \
             FROM meili_connections \
             WHERE tenant_id IS NULL OR tenant_id = $1 \
             ORDER BY (tenant_id IS NULL), uid",
        )
        .bind(tenant_id)
        .fetch_all(&self.pool)
        .await?)
    }

    /// Fetch one connection by uid: the tenant's row when `tenant_id` is given and it
    /// exists, otherwise the global row.
    ///
    /// This is the lookup management reads use (a tenant sees its row, else the global
    /// template). Jobs use [`ConnectionRepo::get_owned`].
    pub async fn get(
        &self,
        uid: &str,
        tenant_id: Option<&str>,
    ) -> Result<Option<ConnectionRecord>, CpError> {
        Ok(sqlx::query_as(
            "SELECT id, uid, name, tenant_id, host, api_key, created_at, updated_at \
             FROM meili_connections \
             WHERE uid = $1 AND (tenant_id IS NULL OR tenant_id = $2) \
             ORDER BY (tenant_id IS NULL) \
             LIMIT 1",
        )
        .bind(uid)
        .bind(tenant_id)
        .fetch_optional(&self.pool)
        .await?)
    }

    /// The tenant's own row only. This is the lookup a tenant job uses: a global
    /// connection is a template an operator may copy, never a key a tenant can write
    /// with (spec v2, tenant isolation).
    pub async fn get_owned(
        &self,
        uid: &str,
        tenant_id: &str,
    ) -> Result<Option<ConnectionRecord>, CpError> {
        Ok(sqlx::query_as(
            "SELECT id, uid, name, tenant_id, host, api_key, created_at, updated_at \
             FROM meili_connections \
             WHERE uid = $1 AND tenant_id = $2",
        )
        .bind(uid)
        .bind(tenant_id)
        .fetch_optional(&self.pool)
        .await?)
    }

    /// Insert a connection. A uid already taken in the same scope is a
    /// [`CpError::Validation`] naming it, not a raw database error.
    pub async fn insert(&self, new: &NewConnection) -> Result<ConnectionRecord, CpError> {
        sqlx::query_as(
            "INSERT INTO meili_connections (id, uid, name, tenant_id, host, api_key) \
             VALUES ($1, $2, $3, $4, $5, $6) \
             RETURNING id, uid, name, tenant_id, host, api_key, created_at, updated_at",
        )
        .bind(new.id)
        .bind(&new.uid)
        .bind(&new.name)
        .bind(new.tenant_id.as_deref())
        .bind(&new.host)
        .bind(&new.api_key)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| {
            let duplicate = matches!(
                &e,
                sqlx::Error::Database(db) if db.code().as_deref() == Some(UNIQUE_VIOLATION)
            );
            if duplicate {
                CpError::Validation(format!(
                    "a connection named {:?} already exists for this tenant",
                    new.uid
                ))
            } else {
                CpError::Db(e)
            }
        })
    }

    /// Apply a partial update to the row in exactly `tenant_id`'s scope. `None` when no
    /// such row exists.
    pub async fn update(
        &self,
        uid: &str,
        tenant_id: Option<&str>,
        patch: &ConnectionPatch,
    ) -> Result<Option<ConnectionRecord>, CpError> {
        Ok(sqlx::query_as(
            "UPDATE meili_connections SET \
                name = COALESCE($3, name), \
                host = COALESCE($4, host), \
                api_key = COALESCE($5, api_key), \
                updated_at = now() \
             WHERE uid = $1 AND COALESCE(tenant_id, '') = COALESCE($2, '') \
             RETURNING id, uid, name, tenant_id, host, api_key, created_at, updated_at",
        )
        .bind(uid)
        .bind(tenant_id)
        .bind(patch.name.as_deref())
        .bind(patch.host.as_deref())
        .bind(patch.api_key.as_deref())
        .fetch_optional(&self.pool)
        .await?)
    }

    /// Delete the connection in exactly `tenant_id`'s scope.
    ///
    /// Never blocked by pipelines that reference it (spec Decision 15): those fail at run
    /// time naming the missing connection. [`ConnectionRepo::used_by`] lets a caller
    /// show what a delete will break before doing it.
    pub async fn delete(&self, uid: &str, tenant_id: Option<&str>) -> Result<bool, CpError> {
        let done = sqlx::query(
            "DELETE FROM meili_connections \
             WHERE uid = $1 AND COALESCE(tenant_id, '') = COALESCE($2, '')",
        )
        .bind(uid)
        .bind(tenant_id)
        .execute(&self.pool)
        .await?;
        Ok(done.rows_affected() > 0)
    }

    /// Uids of the pipelines whose `meili_indexer` step names this connection, sorted.
    ///
    /// For a tenant connection that is the tenant's pipelines plus global ones (a global
    /// pipeline run for that tenant resolves the name in its scope). For a global
    /// connection it is every pipeline naming it, since any tenant without its own
    /// connection of that name falls back to the global one. That can over-report a
    /// pipeline whose tenant shadows the name, which is the safe direction for a "what
    /// will this delete break" hint.
    pub async fn used_by(
        &self,
        uid: &str,
        tenant_id: Option<&str>,
    ) -> Result<Vec<String>, CpError> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT p.uid FROM pipelines p \
             WHERE EXISTS ( \
                 SELECT 1 FROM jsonb_array_elements(COALESCE(p.definition -> 'steps', '[]'::jsonb)) \
                     AS s(step) \
                 WHERE s.step ->> 'plugin' = $3 \
                   AND s.step -> 'config' ->> 'connection' = $1) \
               AND ($2::text IS NULL OR p.tenant_id IS NULL OR p.tenant_id = $2) \
             ORDER BY p.uid",
        )
        .bind(uid)
        .bind(tenant_id)
        .bind(INDEXER_PLUGIN)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|(u,)| u).collect())
    }
}

// ---------------------------------------------------------------------------
// `/internal/connections` handlers. Internal only: the gateway owns validation, sealing
// and redaction, and the worker resolves an indexer step's connection through
// `get_owned` (`scope=tenant`) for a tenant job, `get` for a global one.
// ---------------------------------------------------------------------------

/// `GET /internal/connections?tenant_id=` → the tenant's plus global connections.
pub async fn list_connections(
    State(state): State<AppState>,
    Query(q): Query<TenantQuery>,
    headers: HeaderMap,
) -> Result<Json<Vec<ConnectionRecord>>, CpError> {
    let tenant_id = tenant_scope(q.tenant_id.as_deref(), &headers);
    Ok(Json(state.connections().list(tenant_id.as_deref()).await?))
}

/// `POST /internal/connections` body [`NewConnection`] → 201 [`ConnectionRecord`].
pub async fn create_connection(
    State(state): State<AppState>,
    JsonBody(new): JsonBody<NewConnection>,
) -> Result<Response, CpError> {
    let stored = state.connections().insert(&new).await?;
    tracing::info!(uid = %stored.uid, tenant_id = ?stored.tenant_id, "connection created");
    Ok((StatusCode::CREATED, Json(stored)).into_response())
}

/// Query of `GET /internal/connections/{uid}`.
#[derive(Debug, Deserialize)]
pub struct ConnectionQuery {
    /// Tenant scope; falls back to the `X-Meili-Project-Id` header.
    #[serde(default, alias = "project_id")]
    pub tenant_id: Option<String>,
    /// `tenant`: only the tenant's own row, what a tenant job may use.
    #[serde(default)]
    pub scope: Option<String>,
}

/// `GET /internal/connections/{uid}?tenant_id=[&scope=tenant]`.
///
/// Without `scope`: the tenant's row, else the global one (management reads). With
/// `scope=tenant`: the tenant's own row only (job lookups); it needs a tenant.
pub async fn get_connection(
    State(state): State<AppState>,
    Path(uid): Path<String>,
    Query(q): Query<ConnectionQuery>,
    headers: HeaderMap,
) -> Result<Json<ConnectionRecord>, CpError> {
    let tenant_id = tenant_scope(q.tenant_id.as_deref(), &headers);
    let row = match (q.scope.as_deref(), tenant_id.as_deref()) {
        (Some("tenant"), Some(t)) => state.connections().get_owned(&uid, t).await?,
        (Some("tenant"), None) => {
            return Err(CpError::Validation("scope=tenant needs a tenant_id".into()));
        }
        (Some(other), _) => {
            return Err(CpError::Validation(format!("unknown scope {other:?}")));
        }
        (None, t) => state.connections().get(&uid, t).await?,
    };
    row.map(Json)
        .ok_or_else(|| not_found(&uid, tenant_id.as_deref()))
}

/// `PATCH /internal/connections/{uid}?tenant_id=` body [`ConnectionPatch`].
pub async fn patch_connection(
    State(state): State<AppState>,
    Path(uid): Path<String>,
    Query(q): Query<TenantQuery>,
    headers: HeaderMap,
    JsonBody(patch): JsonBody<ConnectionPatch>,
) -> Result<Json<ConnectionRecord>, CpError> {
    let tenant_id = tenant_scope(q.tenant_id.as_deref(), &headers);
    state
        .connections()
        .update(&uid, tenant_id.as_deref(), &patch)
        .await?
        .map(Json)
        .ok_or_else(|| not_found(&uid, tenant_id.as_deref()))
}

/// `DELETE /internal/connections/{uid}?tenant_id=` → 204, or 404 when missing.
/// Never blocked by pipelines referencing it (spec Decision 15).
pub async fn delete_connection(
    State(state): State<AppState>,
    Path(uid): Path<String>,
    Query(q): Query<TenantQuery>,
    headers: HeaderMap,
) -> Result<StatusCode, CpError> {
    let tenant_id = tenant_scope(q.tenant_id.as_deref(), &headers);
    let repo = state.connections();
    // Read before deleting only so the log names what the delete just broke.
    let used_by = repo.used_by(&uid, tenant_id.as_deref()).await?;
    if repo.delete(&uid, tenant_id.as_deref()).await? {
        tracing::info!(
            uid = %uid,
            tenant_id = ?tenant_id,
            used_by = ?used_by,
            "connection deleted"
        );
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(not_found(&uid, tenant_id.as_deref()))
    }
}

/// `GET /internal/connections/{uid}/used_by?tenant_id=` → pipeline uids.
pub async fn connection_used_by(
    State(state): State<AppState>,
    Path(uid): Path<String>,
    Query(q): Query<TenantQuery>,
    headers: HeaderMap,
) -> Result<Json<Vec<String>>, CpError> {
    let tenant_id = tenant_scope(q.tenant_id.as_deref(), &headers);
    Ok(Json(
        state
            .connections()
            .used_by(&uid, tenant_id.as_deref())
            .await?,
    ))
}

fn not_found(uid: &str, tenant_id: Option<&str>) -> CpError {
    CpError::NotFound(format!(
        "connection {uid:?} not found (tenant_id={tenant_id:?})"
    ))
}
