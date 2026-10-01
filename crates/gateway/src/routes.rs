//! Every route the gateway mounts, and whether it is a data or a management route.
//!
//! This table is the source of truth the tests below hold `router()` (in `lib.rs`),
//! `docs/openapi.yaml` and the auth split (spec §4.1) against: adding a route means
//! adding it here, in `lib.rs` and in the OpenAPI spec, or the build fails.

/// Which auth model a route follows (spec §4.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteClass {
    /// The caller's Meilisearch key plus trusted edge headers.
    Data,
    /// `ManagementAuth`: Lab service token, admin key, or open mode.
    Management,
}

/// One mounted route.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RouteSpec {
    /// Upper-case HTTP method.
    pub method: &'static str,
    /// Path as written in `router()`.
    pub path: &'static str,
    /// Auth model.
    pub class: RouteClass,
}

const fn data(method: &'static str, path: &'static str) -> RouteSpec {
    RouteSpec {
        method,
        path,
        class: RouteClass::Data,
    }
}
const fn mgmt(method: &'static str, path: &'static str) -> RouteSpec {
    RouteSpec {
        method,
        path,
        class: RouteClass::Management,
    }
}

/// Every route `router()` mounts, except the admin UI's.
pub const ROUTES: &[RouteSpec] = &[
    data("GET", "/health"),
    data("POST", "/ingest"),
    data("POST", "/ingest/batch"),
    data("POST", "/ingest/pipeline/{name}"),
    data("POST", "/indexes/{index_uid}/ingest"),
    data("POST", "/indexes/{index_uid}/ingest/batch"),
    data("POST", "/indexes/{index_uid}/ingest/pipeline/{name}"),
    data("GET", "/jobs/{id}"),
    mgmt("GET", "/jobs"),
    mgmt("POST", "/jobs/{id}/cancel"),
    mgmt("GET", "/pipelines"),
    mgmt("POST", "/pipelines"),
    mgmt("POST", "/pipelines/validate"),
    mgmt("GET", "/pipelines/{name}"),
    mgmt("DELETE", "/pipelines/{name}"),
    mgmt("GET", "/connections"),
    mgmt("POST", "/connections"),
    mgmt("GET", "/connections/{uid}"),
    mgmt("PATCH", "/connections/{uid}"),
    mgmt("DELETE", "/connections/{uid}"),
    mgmt("GET", "/sources"),
    mgmt("POST", "/sources"),
    mgmt("GET", "/sources/{uid}"),
    mgmt("PATCH", "/sources/{uid}"),
    mgmt("DELETE", "/sources/{uid}"),
    mgmt("POST", "/sources/{uid}/pause"),
    mgmt("POST", "/sources/{uid}/unpause"),
    mgmt("POST", "/sources/{uid}/run"),
    mgmt("GET", "/sources/{uid}/runs"),
    mgmt("GET", "/plugins"),
    mgmt("GET", "/catalog"),
    mgmt("GET", "/usage"),
];

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use tower::ServiceExt;
    use wiremock::MockServer;

    use super::*;
    use crate::test_support::*;

    /// `/jobs/{job_id}` and `/jobs/{id}` are the same route.
    fn normalize(path: &str) -> String {
        regex::Regex::new(r"\{[^}]+\}")
            .unwrap()
            .replace_all(path, "{}")
            .into_owned()
    }

    fn table() -> BTreeSet<(String, String)> {
        ROUTES
            .iter()
            .map(|r| (r.method.to_string(), normalize(r.path)))
            .collect()
    }

    #[test]
    fn the_table_lists_every_route_in_lib_rs() {
        let source = include_str!("lib.rs");
        let re = regex::Regex::new(r#"\.route\(\s*"([^"]+)""#).unwrap();
        let mounted: BTreeSet<String> =
            re.captures_iter(source).map(|c| normalize(&c[1])).collect();
        let listed: BTreeSet<String> = ROUTES.iter().map(|r| normalize(r.path)).collect();
        assert_eq!(mounted, listed, "lib.rs router() and ROUTES disagree");
    }

    fn openapi() -> serde_yaml::Value {
        serde_yaml::from_str(include_str!("../../../docs/openapi.yaml")).unwrap()
    }

    #[test]
    fn the_table_matches_the_openapi_spec() {
        let spec = openapi();
        let mut documented = BTreeSet::new();
        for (path, item) in spec["paths"].as_mapping().unwrap() {
            for (method, _) in item.as_mapping().unwrap() {
                let method = method.as_str().unwrap();
                if ["get", "post", "put", "patch", "delete"].contains(&method) {
                    documented.insert((method.to_uppercase(), normalize(path.as_str().unwrap())));
                }
            }
        }
        assert_eq!(documented, table(), "docs/openapi.yaml and ROUTES disagree");
    }

    #[test]
    fn the_openapi_security_follows_the_route_class() {
        let spec = openapi();
        for r in ROUTES {
            let item = spec["paths"]
                .as_mapping()
                .unwrap()
                .iter()
                .find(|(p, _)| normalize(p.as_str().unwrap()) == normalize(r.path))
                .map(|(_, v)| v)
                .unwrap();
            let op = &item[r.method.to_lowercase().as_str()];
            let schemes: BTreeSet<String> = op["security"]
                .as_sequence()
                .unwrap_or_else(|| panic!("{} {} has no security block", r.method, r.path))
                .iter()
                .flat_map(|req| req.as_mapping().unwrap().keys().cloned())
                .map(|k| k.as_str().unwrap().to_string())
                .collect();
            let management = schemes.contains("LabServiceToken") && schemes.contains("AdminKey");
            assert_eq!(
                management,
                r.class == RouteClass::Management,
                "{} {}: security {schemes:?}",
                r.method,
                r.path
            );
        }
    }

    fn concrete(path: &str) -> String {
        path.replace("{id}", "00000000-0000-0000-0000-000000000001")
            .replace("{name}", "p")
            .replace("{uid}", "u")
            .replace("{index_uid}", "docs")
    }

    #[tokio::test]
    async fn every_management_route_is_guarded() {
        let server = MockServer::start().await;
        let config = GatewayConfig {
            admin_api_key: Some("admin-secret".into()),
            ..GatewayConfig::default()
        };
        let (app, _) = test_app(&server, config).await;
        for r in ROUTES.iter().filter(|r| r.class == RouteClass::Management) {
            let req = Request::builder()
                .method(r.method)
                .uri(concrete(r.path))
                .body(Body::empty())
                .unwrap();
            let resp = app.clone().oneshot(req).await.unwrap();
            assert_eq!(
                resp.status(),
                StatusCode::UNAUTHORIZED,
                "{} {}",
                r.method,
                r.path
            );
        }
    }

    #[tokio::test]
    async fn every_data_route_is_mounted() {
        let server = MockServer::start().await;
        let (app, _) = test_app(&server, GatewayConfig::default()).await;
        for r in ROUTES.iter().filter(|r| r.class == RouteClass::Data) {
            let req = Request::builder()
                .method(r.method)
                .uri(concrete(r.path))
                .body(Body::empty())
                .unwrap();
            let resp = app.clone().oneshot(req).await.unwrap();
            let status = resp.status();
            let body = resp.into_body().collect().await.unwrap().to_bytes();
            assert_ne!(
                status,
                StatusCode::METHOD_NOT_ALLOWED,
                "{} {}",
                r.method,
                r.path
            );
            // The router's own 404 has an empty body; a handler's 404 is JSON.
            assert!(
                status != StatusCode::NOT_FOUND || !body.is_empty(),
                "{} {} is not mounted",
                r.method,
                r.path
            );
        }
    }
}
