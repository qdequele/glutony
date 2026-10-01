//! `GET /catalog` — the curated action and workflow catalog.
//!
//! Unlike `/plugins`, this is not proxied: the catalog is static data compiled
//! into the binary with no control plane or database behind it. Serving it here
//! removes a hop and a 502 path from data that cannot fail to load.

use axum::Json;
use axum::extract::State;
use meili_ingest_router::catalog::{Catalog, catalog};

use crate::auth::Scope;
use crate::state::AppState;

/// `GET /catalog`. Cannot fail once the caller is authenticated, hence no `Result`.
/// It takes the state only so the [`Scope`] extractor can read the auth config.
pub async fn get_catalog(State(_state): State<AppState>, _scope: Scope) -> Json<Catalog> {
    Json(catalog())
}

#[cfg(test)]
mod tests {
    use crate::test_support::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;
    use wiremock::MockServer;

    #[tokio::test]
    async fn catalog_is_served() {
        let server = MockServer::start().await;
        let (app, _) = test_app(&server, GatewayConfig::default()).await;
        let resp = app
            .oneshot(Request::get("/catalog").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let json = json_body(resp).await;
        assert!(!json["actions"].as_array().unwrap().is_empty());
        assert!(!json["workflows"].as_array().unwrap().is_empty());
        assert_eq!(json["actions"][0]["category"], "fetch");
    }

    /// The catalog has no control plane behind it, so it answers even when the
    /// control plane is unreachable — the reason it is not proxied.
    #[tokio::test]
    async fn catalog_survives_a_dead_control_plane() {
        let cfg = GatewayConfig {
            control_plane_url: "http://127.0.0.1:9".into(),
            ..GatewayConfig::default()
        };
        let (app, _) = test_app_with_url(cfg, "http://127.0.0.1:9").await;
        let resp = app
            .oneshot(Request::get("/catalog").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
