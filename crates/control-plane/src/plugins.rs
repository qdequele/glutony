//! Plugin manifest registry: workers `POST /internal/plugins` at boot, the gateway
//! proxies `GET /plugins`.

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use meili_ingest_plugin_sdk::{PluginKind, PluginManifest};
use sqlx::PgPool;
use sqlx::types::Json as SqlJson;

use crate::builtin_pipelines::{builtin_plugin_names, is_in_repo_plugin};
use crate::error::CpError;
use crate::{AppState, JsonBody};

/// Upsert manifests by name in one transaction.
pub async fn upsert_manifests(pool: &PgPool, manifests: &[PluginManifest]) -> Result<(), CpError> {
    let mut tx = pool.begin().await?;
    for m in manifests {
        sqlx::query(
            "INSERT INTO plugins (name, manifest, updated_at) VALUES ($1, $2, now()) \
             ON CONFLICT (name) DO UPDATE SET manifest = EXCLUDED.manifest, updated_at = now()",
        )
        .bind(&m.name)
        .bind(SqlJson(m))
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

/// All registered manifests, sorted by name.
pub async fn registered_manifests(pool: &PgPool) -> Result<Vec<PluginManifest>, CpError> {
    let rows: Vec<(SqlJson<PluginManifest>,)> =
        sqlx::query_as("SELECT manifest FROM plugins ORDER BY name")
            .fetch_all(pool)
            .await?;
    Ok(rows.into_iter().map(|(SqlJson(m),)| m).collect())
}

/// Minimal manifests (name + kind only) for every known plugin name, used when no
/// worker has registered yet so `GET /plugins` is never empty.
pub fn static_manifests() -> Vec<PluginManifest> {
    builtin_plugin_names()
        .iter()
        .map(|name| {
            let (kind, version) = if is_in_repo_plugin(name) {
                (PluginKind::Builtin, env!("CARGO_PKG_VERSION"))
            } else {
                (PluginKind::Grpc, "external")
            };
            PluginManifest::new(*name, version).kind(kind)
        })
        .collect()
}

/// `POST /internal/plugins` body `PluginManifest[]` → 204 (upsert by name).
pub async fn register_plugins(
    State(state): State<AppState>,
    JsonBody(manifests): JsonBody<Vec<PluginManifest>>,
) -> Result<StatusCode, CpError> {
    for m in &manifests {
        if m.name.trim().is_empty() {
            return Err(CpError::Validation(
                "plugin manifest has an empty name".into(),
            ));
        }
    }
    upsert_manifests(&state.pool, &manifests).await?;
    tracing::info!(count = manifests.len(), "plugin manifests registered");
    Ok(StatusCode::NO_CONTENT)
}

/// `GET /plugins` → registered manifests, or the static list when none registered.
pub async fn list_plugins(
    State(state): State<AppState>,
) -> Result<Json<Vec<PluginManifest>>, CpError> {
    let mut list = registered_manifests(&state.pool).await?;
    if list.is_empty() {
        list = static_manifests();
    }
    Ok(Json(list))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn static_manifests_cover_every_known_name() {
        let list = static_manifests();
        assert_eq!(list.len(), builtin_plugin_names().len());
        let names: Vec<&str> = list.iter().map(|m| m.name.as_str()).collect();
        assert_eq!(names, builtin_plugin_names());
        let by_name = |n: &str| list.iter().find(|m| m.name == n).map(|m| m.kind);
        assert_eq!(by_name("pdf_extractor"), Some(PluginKind::Builtin));
        assert_eq!(by_name("whisper_transcriber"), Some(PluginKind::Grpc));
        // Round-trips through JSON (what the gateway receives).
        let json = serde_json::to_string(&list).unwrap();
        let back: Vec<PluginManifest> = serde_json::from_str(&json).unwrap();
        assert_eq!(back, list);
    }
}
