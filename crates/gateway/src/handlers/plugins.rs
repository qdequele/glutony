//! `GET /plugins` — proxies the control plane's plugin manifest registry (plan
//! Decision 7).

use axum::extract::State;
use axum::Json;
use meili_ingest_plugin_sdk::PluginManifest;

use crate::error::GatewayError;
use crate::state::AppState;

/// `GET /plugins`.
pub async fn list_plugins(State(state): State<AppState>) -> Result<Json<Vec<PluginManifest>>, GatewayError> {
    Ok(Json(state.control_plane.list_plugins().await?))
}

#[cfg(test)]
mod tests {
    use crate::test_support::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use meili_ingest_plugin_sdk::PluginManifest;
    use tower::ServiceExt;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn plugins_are_proxied() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/plugins"))
            .respond_with(ResponseTemplate::new(200).set_body_json(vec![
                PluginManifest::new("pdf_extractor", "0.1.0"),
                PluginManifest::new("chunker", "0.1.0"),
            ]))
            .mount(&server)
            .await;
        let (app, _) = test_app(&server, GatewayConfig::default()).await;
        let resp = app.oneshot(Request::get("/plugins").body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let json = json_body(resp).await;
        assert_eq!(json.as_array().unwrap().len(), 2);
        assert_eq!(json[0]["name"], "pdf_extractor");
    }

    #[tokio::test]
    async fn control_plane_down_is_502() {
        let cfg = GatewayConfig { control_plane_url: "http://127.0.0.1:9".into(), ..GatewayConfig::default() };
        let (app, _) = test_app_with_url(cfg, "http://127.0.0.1:9").await;
        let resp = app.oneshot(Request::get("/plugins").body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
    }
}
