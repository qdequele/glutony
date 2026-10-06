//! Management API authentication (spec §4).
//!
//! Three modes, chosen by configuration:
//!
//! | `LAB_SERVICE_TOKEN` | `ADMIN_API_KEY` | caller sends | principal |
//! |---|---|---|---|
//! | set | - | `Bearer <token>` + **required** `X-Glutony-Tenant-Id` | `Lab` |
//! | - | set | `Bearer <key>`, `X-Glutony-Tenant-Id` optional | `Admin` |
//! | unset | unset | nothing | `Open`: tenant from the trusted edge, as before |
//!
//! When either secret is set, the tenant of a management request comes **only** from
//! `X-Glutony-Tenant-Id`: trusted `X-Meili-*` headers front ingest, they never manage.
//!
//! `GET /jobs/{id}` is read by both sides: an ingest client polling through the edge,
//! and the Lab. [`authorize_job_read`] accepts either, and with auth on refuses a caller
//! that is neither rather than showing it every tenant's jobs.

use axum::extract::FromRequestParts;
use axum::http::HeaderMap;
use axum::http::request::Parts;
use subtle::ConstantTimeEq;

use crate::context::{bearer_token, header, resolve_tenant_id};
use crate::error::GatewayError;
use crate::state::{AppState, GatewayConfig};

/// Header the Lab (or an operator) names the tenant with.
pub const H_GLUTONY_TENANT_ID: &str = "x-glutony-tenant-id";

/// Who is calling a management route.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Principal {
    /// The Meilisearch Lab, with `LAB_SERVICE_TOKEN`; always acts for one tenant.
    Lab,
    /// An operator with `ADMIN_API_KEY`; global unless it names a tenant.
    Admin,
    /// No management auth configured (today's behavior).
    Open,
}

/// The authenticated caller of a management route and the tenant it acts for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scope {
    /// Who called.
    pub principal: Principal,
    /// The tenant the call acts for; `None` is the global scope.
    pub tenant_id: Option<String>,
}

impl Scope {
    /// The tenant, borrowed.
    pub fn tenant(&self) -> Option<&str> {
        self.tenant_id.as_deref()
    }
}

/// Whether any management secret is configured.
pub fn management_auth_enabled(config: &GatewayConfig) -> bool {
    config.lab_service_token.is_some() || config.admin_api_key.is_some()
}

/// Authenticate a management request.
pub fn authorize(headers: &HeaderMap, config: &GatewayConfig) -> Result<Scope, GatewayError> {
    if !management_auth_enabled(config) {
        return Ok(Scope {
            principal: Principal::Open,
            tenant_id: resolve_tenant_id(headers, config)?,
        });
    }
    let principal = bearer_token(headers)
        .and_then(|token| management_principal(config, &token))
        .ok_or_else(|| GatewayError::Unauthorized("missing or invalid management token".into()))?;
    let tenant_id = match header(headers, H_GLUTONY_TENANT_ID) {
        Some(t) => {
            meili_ingest_plugin_sdk::validate_tenant_id(&t).map_err(GatewayError::InvalidTenant)?;
            Some(t)
        }
        None if principal == Principal::Lab => {
            return Err(GatewayError::InvalidTenant(format!(
                "{H_GLUTONY_TENANT_ID} is required with the Lab service token"
            )));
        }
        None => None,
    };
    Ok(Scope {
        principal,
        tenant_id,
    })
}

/// Authenticate `GET /jobs/{id}` and return the tenant the read is scoped to (`None`:
/// every job).
///
/// - Auth off: the trusted edge tenant, as on every data route.
/// - Auth on, a valid management token: that principal's scope, as on a management
///   route; an admin without `X-Glutony-Tenant-Id` sees every job.
/// - Auth on, otherwise: the trusted edge tenant is required. Without one the read is a
///   `401`, never unchecked. A bearer that is not a management token (a Meilisearch
///   key sent along through the edge) does not count against the caller.
pub fn authorize_job_read(
    headers: &HeaderMap,
    config: &GatewayConfig,
) -> Result<Option<String>, GatewayError> {
    if !management_auth_enabled(config) {
        return resolve_tenant_id(headers, config);
    }
    if bearer_token(headers).is_some_and(|token| management_principal(config, &token).is_some()) {
        return Ok(authorize(headers, config)?.tenant_id);
    }
    match resolve_tenant_id(headers, config)? {
        Some(tenant) => Ok(Some(tenant)),
        None => Err(GatewayError::Unauthorized(
            "missing or invalid management token, and no trusted edge tenant".into(),
        )),
    }
}

/// The management principal a bearer token authenticates, if any.
fn management_principal(config: &GatewayConfig, token: &str) -> Option<Principal> {
    if token_matches(config.lab_service_token.as_deref(), token) {
        Some(Principal::Lab)
    } else if token_matches(config.admin_api_key.as_deref(), token) {
        Some(Principal::Admin)
    } else {
        None
    }
}

/// Constant-time comparison against a configured secret.
fn token_matches(expected: Option<&str>, got: &str) -> bool {
    expected.is_some_and(|e| bool::from(e.as_bytes().ct_eq(got.as_bytes())))
}

impl FromRequestParts<AppState> for Scope {
    type Rejection = GatewayError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        authorize(&parts.headers, &state.config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.insert(
                axum::http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                HeaderValue::from_str(v).unwrap(),
            );
        }
        h
    }

    fn cfg(lab: Option<&str>, admin: Option<&str>) -> GatewayConfig {
        GatewayConfig {
            lab_service_token: lab.map(str::to_string),
            admin_api_key: admin.map(str::to_string),
            ..GatewayConfig::default()
        }
    }

    #[test]
    fn open_mode_takes_the_tenant_from_the_edge() {
        let c = cfg(None, None);
        assert_eq!(
            authorize(&HeaderMap::new(), &c).unwrap(),
            Scope {
                principal: Principal::Open,
                tenant_id: None
            }
        );
        let h = headers(&[("x-meili-project-id", "proj-1")]);
        assert_eq!(authorize(&h, &c).unwrap().tenant(), Some("proj-1"));
    }

    #[test]
    fn a_missing_or_wrong_token_is_401() {
        let c = cfg(Some("lab-secret"), Some("admin-secret"));
        for h in [
            HeaderMap::new(),
            headers(&[("authorization", "Bearer nope")]),
            headers(&[("authorization", "Basic lab-secret")]),
            headers(&[("authorization", "Bearer lab-secretX")]),
        ] {
            let err = authorize(&h, &c).unwrap_err();
            assert_eq!(err.code(), "unauthorized", "{h:?}");
        }
        // A wrong token wins over a bad tenant: never tell an unauthenticated caller
        // anything about tenants.
        let h = headers(&[
            ("authorization", "Bearer nope"),
            ("x-glutony-tenant-id", "a/b"),
        ]);
        assert_eq!(authorize(&h, &c).unwrap_err().code(), "unauthorized");
    }

    #[test]
    fn the_lab_must_name_a_valid_tenant() {
        let c = cfg(Some("lab-secret"), None);
        let h = headers(&[("authorization", "Bearer lab-secret")]);
        assert_eq!(authorize(&h, &c).unwrap_err().code(), "invalid_tenant");
        let h = headers(&[
            ("authorization", "Bearer lab-secret"),
            ("x-glutony-tenant-id", "a b"),
        ]);
        assert_eq!(authorize(&h, &c).unwrap_err().code(), "invalid_tenant");
        let h = headers(&[
            ("authorization", "Bearer lab-secret"),
            ("x-glutony-tenant-id", "acct-1"),
        ]);
        assert_eq!(
            authorize(&h, &c).unwrap(),
            Scope {
                principal: Principal::Lab,
                tenant_id: Some("acct-1".into())
            }
        );
    }

    #[test]
    fn the_admin_is_global_unless_it_names_a_tenant() {
        let c = cfg(None, Some("admin-secret"));
        let h = headers(&[("authorization", "Bearer admin-secret")]);
        assert_eq!(
            authorize(&h, &c).unwrap(),
            Scope {
                principal: Principal::Admin,
                tenant_id: None
            }
        );
        let h = headers(&[
            ("authorization", "bearer admin-secret"),
            ("x-glutony-tenant-id", "t1"),
        ]);
        assert_eq!(authorize(&h, &c).unwrap().tenant(), Some("t1"));
    }

    #[test]
    fn a_token_only_counts_for_the_mode_it_configures() {
        // The admin key does not open the Lab mode and vice versa.
        let c = cfg(None, Some("admin-secret"));
        let h = headers(&[
            ("authorization", "Bearer lab-secret"),
            ("x-glutony-tenant-id", "t1"),
        ]);
        assert_eq!(authorize(&h, &c).unwrap_err().code(), "unauthorized");
    }

    #[test]
    fn auth_on_ignores_edge_tenant_headers() {
        // ENVOY_TRUSTED_HEADER unset, so X-Meili-* would be trusted in open mode.
        let c = cfg(Some("lab-secret"), None);
        let h = headers(&[
            ("authorization", "Bearer lab-secret"),
            ("x-glutony-tenant-id", "acct-b"),
            ("x-meili-tenant-id", "acct-a"),
            ("x-meili-project-id", "proj-a"),
        ]);
        assert_eq!(authorize(&h, &c).unwrap().tenant(), Some("acct-b"));
        assert!(management_auth_enabled(&c));
        assert!(!management_auth_enabled(&cfg(None, None)));
    }

    #[test]
    fn a_job_read_takes_the_management_scope_when_authenticated() {
        let c = cfg(Some("lab-secret"), Some("admin-secret"));
        let h = headers(&[
            ("authorization", "Bearer lab-secret"),
            ("x-glutony-tenant-id", "acct-b"),
            ("x-meili-tenant-id", "acct-a"),
        ]);
        assert_eq!(
            authorize_job_read(&h, &c).unwrap().as_deref(),
            Some("acct-b")
        );
        let h = headers(&[("authorization", "Bearer admin-secret")]);
        assert_eq!(authorize_job_read(&h, &c).unwrap(), None);
        // A Lab token without a tenant is still refused.
        let h = headers(&[("authorization", "Bearer lab-secret")]);
        assert_eq!(
            authorize_job_read(&h, &c).unwrap_err().code(),
            "invalid_tenant"
        );
    }

    #[test]
    fn a_job_read_falls_back_to_the_trusted_edge_tenant() {
        let c = GatewayConfig {
            envoy_trusted_header: Some("edge-secret".into()),
            ..cfg(Some("lab-secret"), None)
        };
        let h = headers(&[
            ("x-meili-tenant-id", "acct-a"),
            ("x-meili-envoy-secret", "edge-secret"),
            // A Meilisearch key sent along is not a failed management login.
            ("authorization", "Bearer some-meili-key"),
        ]);
        assert_eq!(
            authorize_job_read(&h, &c).unwrap().as_deref(),
            Some("acct-a")
        );
    }

    #[test]
    fn auth_on_refuses_an_untenanted_unauthenticated_job_read() {
        let c = GatewayConfig {
            envoy_trusted_header: Some("edge-secret".into()),
            ..cfg(Some("lab-secret"), None)
        };
        for h in [
            HeaderMap::new(),
            headers(&[("authorization", "Bearer nope")]),
            // Edge tenant without, or with the wrong, edge secret: not trusted.
            headers(&[("x-meili-tenant-id", "acct-a")]),
            headers(&[
                ("x-meili-tenant-id", "acct-a"),
                ("x-meili-envoy-secret", "wrong"),
            ]),
            // A trusted edge that names no tenant.
            headers(&[("x-meili-envoy-secret", "edge-secret")]),
        ] {
            assert_eq!(
                authorize_job_read(&h, &c).unwrap_err().code(),
                "unauthorized",
                "{h:?}"
            );
        }
    }

    #[test]
    fn open_mode_job_reads_are_unchanged() {
        let c = cfg(None, None);
        assert_eq!(authorize_job_read(&HeaderMap::new(), &c).unwrap(), None);
        let h = headers(&[("x-meili-tenant-id", "acct-a")]);
        assert_eq!(
            authorize_job_read(&h, &c).unwrap().as_deref(),
            Some("acct-a")
        );
        // In open mode a bearer is never a management token.
        let h = headers(&[("authorization", "Bearer lab-secret")]);
        assert_eq!(authorize_job_read(&h, &c).unwrap(), None);
    }
}
