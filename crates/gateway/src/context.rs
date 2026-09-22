//! Tenant context resolution (SPEC §3.2, §3.4) and the Envoy trust rule (plan Decision 6).
//!
//! Resolution order:
//!
//! 1. `X-Meili-Host` → host
//! 2. `X-Meili-Api-Key` → api_key
//! 3. `X-Meili-Project-Id` → project_id
//! 4. `X-Meili-Index` → index (starting point)
//! 5. `?index=` query param → index (overrides the header)
//! 6. `Authorization: Bearer <key>` → api_key (self-hosted fallback)
//! 7. `MEILI_URL` / `MEILI_API_KEY` env vars → host / api_key (self-hosted fallback)
//!
//! Steps 1–4 (and `X-Meili-Region`) are only applied when the `X-Meili-*` headers are
//! trusted: either `ENVOY_TRUSTED_HEADER` is unset (dev mode), or the request carries
//! `X-Meili-Envoy-Secret` equal to it. Otherwise every `X-Meili-*` header is ignored and
//! the request is treated as standalone.

use axum::http::HeaderMap;
use meili_ingest_plugin_sdk::MeiliContext;
use meili_ingest_router::mime_to_default_index;

use crate::error::GatewayError;
use crate::state::GatewayConfig;

/// Envoy-injected header: Meilisearch host URL.
pub const H_HOST: &str = "x-meili-host";
/// Envoy-injected header: Meilisearch API key.
pub const H_API_KEY: &str = "x-meili-api-key";
/// Envoy-injected header: project (tenant) id.
pub const H_PROJECT_ID: &str = "x-meili-project-id";
/// Envoy-injected header: index name.
pub const H_INDEX: &str = "x-meili-index";
/// Envoy-injected header: region tag.
pub const H_REGION: &str = "x-meili-region";
/// Shared secret proving the request came through Envoy.
pub const H_ENVOY_SECRET: &str = "x-meili-envoy-secret";

/// Non-empty, trimmed header value.
fn header(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(str::to_string)
}

/// Whether the `X-Meili-*` headers of this request may be honoured.
pub fn envoy_headers_trusted(headers: &HeaderMap, config: &GatewayConfig) -> bool {
    match &config.envoy_trusted_header {
        None => true,
        Some(secret) => header(headers, H_ENVOY_SECRET).as_deref() == Some(secret.as_str()),
    }
}

/// Read an `X-Meili-*` header, but only when the request is trusted.
pub fn trusted_header(headers: &HeaderMap, config: &GatewayConfig, name: &str) -> Option<String> {
    if envoy_headers_trusted(headers, config) {
        header(headers, name)
    } else {
        None
    }
}

/// Tenant id of the request, if any (honours the Envoy trust rule). Used by routes that
/// only need scoping (pipelines, plugins, jobs) and must not require credentials.
pub fn resolve_project_id(headers: &HeaderMap, config: &GatewayConfig) -> Option<String> {
    trusted_header(headers, config, H_PROJECT_ID)
}

/// `Authorization: Bearer <token>` → token.
fn bearer_token(headers: &HeaderMap) -> Option<String> {
    let raw = header(headers, "authorization")?;
    let (scheme, token) = raw.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let token = token.trim();
    if token.is_empty() {
        None
    } else {
        Some(token.to_string())
    }
}

/// Resolve the [`MeiliContext`] of a request following SPEC §3.2.
///
/// `query_index` is the `?index=` query parameter (already URL-decoded). Returns
/// [`GatewayError::MissingContext`] when the host or API key cannot be determined.
pub fn resolve_context(
    headers: &HeaderMap,
    query_index: Option<&str>,
    config: &GatewayConfig,
) -> Result<MeiliContext, GatewayError> {
    let trusted = envoy_headers_trusted(headers, config);
    if !trusted && headers.contains_key(H_HOST) {
        tracing::debug!("ignoring X-Meili-* headers: missing or wrong X-Meili-Envoy-Secret");
    }
    let envoy = |name: &str| if trusted { header(headers, name) } else { None };

    // 1–4: Envoy headers.
    let mut host = envoy(H_HOST);
    let mut api_key = envoy(H_API_KEY);
    let project_id = envoy(H_PROJECT_ID);
    let mut index = envoy(H_INDEX);
    let region = envoy(H_REGION);

    // 5: query param overrides the header.
    if let Some(q) = query_index.map(str::trim).filter(|q| !q.is_empty()) {
        index = Some(q.to_string());
    }

    // 6: Authorization: Bearer fallback for the key.
    if api_key.is_none() {
        api_key = bearer_token(headers);
    }

    // 7: env fallbacks.
    if host.is_none() {
        host = config.meili_url.clone();
    }
    if api_key.is_none() {
        api_key = config.meili_api_key.clone();
    }

    match (host, api_key) {
        (Some(host), Some(api_key)) => Ok(MeiliContext {
            project_id,
            host: Some(host),
            api_key: Some(api_key),
            index,
            region,
        }),
        (None, _) => Err(GatewayError::MissingContext(
            "no Meilisearch host: send X-Meili-Host (via Envoy) or set MEILI_URL".into(),
        )),
        (Some(_), None) => Err(GatewayError::MissingContext(
            "no Meilisearch API key: send X-Meili-Api-Key (via Envoy), Authorization: Bearer <key>, or set MEILI_API_KEY"
                .into(),
        )),
    }
}

/// Finish the index chain of SPEC §3.4 and write the result into `ctx.index`.
///
/// `ctx.index` already holds the header/query value (or `None`). The pipeline trigger's
/// `index_pattern` overrides it when set; otherwise the MIME default applies, and when
/// that is the generic `"documents"` the configured `default_index` is used instead.
/// Returns the resolved index name.
pub fn resolve_index(
    ctx: &mut MeiliContext,
    pipeline_index_pattern: Option<&str>,
    mime: &str,
    default_index: &str,
) -> String {
    let pattern = pipeline_index_pattern
        .map(str::trim)
        .filter(|p| !p.is_empty());
    let resolved = match (
        pattern,
        ctx.index
            .as_deref()
            .map(str::trim)
            .filter(|i| !i.is_empty()),
    ) {
        (Some(p), _) => p.to_string(),
        (None, Some(existing)) => existing.to_string(),
        (None, None) => {
            let d = mime_to_default_index(mime);
            if d == "documents" {
                default_index.to_string()
            } else {
                d.to_string()
            }
        }
    };
    ctx.index = Some(resolved.clone());
    resolved
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn cfg() -> GatewayConfig {
        GatewayConfig::default()
    }

    fn cfg_env() -> GatewayConfig {
        GatewayConfig {
            meili_url: Some("http://env:7700".into()),
            meili_api_key: Some("envKey".into()),
            ..Default::default()
        }
    }

    fn cfg_secret() -> GatewayConfig {
        GatewayConfig {
            envoy_trusted_header: Some("s3cret".into()),
            ..Default::default()
        }
    }

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.append(
                axum::http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                HeaderValue::from_str(v).unwrap(),
            );
        }
        h
    }

    fn envoy_headers() -> HeaderMap {
        headers(&[
            (H_HOST, "https://xxx.us-west.meilisearch.io"),
            (H_API_KEY, "envoyKey"),
            (H_PROJECT_ID, "xxx"),
            (H_INDEX, "from-header"),
            (H_REGION, "us-west"),
        ])
    }

    // --- step 1-4: Envoy headers ---------------------------------------------------

    #[test]
    fn full_envoy_headers_resolve_every_field() {
        let ctx = resolve_context(&envoy_headers(), None, &cfg()).unwrap();
        assert_eq!(
            ctx,
            MeiliContext {
                project_id: Some("xxx".into()),
                host: Some("https://xxx.us-west.meilisearch.io".into()),
                api_key: Some("envoyKey".into()),
                index: Some("from-header".into()),
                region: Some("us-west".into()),
            }
        );
    }

    #[test]
    fn host_and_key_headers_alone_are_enough() {
        let h = headers(&[(H_HOST, "http://h"), (H_API_KEY, "k")]);
        let ctx = resolve_context(&h, None, &cfg()).unwrap();
        assert_eq!(ctx.host.as_deref(), Some("http://h"));
        assert_eq!(ctx.api_key.as_deref(), Some("k"));
        assert_eq!(ctx.project_id, None);
        assert_eq!(ctx.index, None);
        assert_eq!(ctx.region, None);
    }

    #[test]
    fn empty_header_values_are_ignored() {
        let h = headers(&[(H_HOST, "  "), (H_API_KEY, "k"), (H_INDEX, "")]);
        let err = resolve_context(&h, None, &cfg()).unwrap_err();
        assert!(matches!(err, GatewayError::MissingContext(m) if m.contains("host")));
    }

    // --- step 5: query overrides header ------------------------------------------

    #[test]
    fn query_index_overrides_header_index() {
        let ctx = resolve_context(&envoy_headers(), Some("from-query"), &cfg()).unwrap();
        assert_eq!(ctx.index.as_deref(), Some("from-query"));
    }

    #[test]
    fn empty_query_index_does_not_override() {
        let ctx = resolve_context(&envoy_headers(), Some("  "), &cfg()).unwrap();
        assert_eq!(ctx.index.as_deref(), Some("from-header"));
    }

    #[test]
    fn query_index_applies_without_header_index() {
        let h = headers(&[(H_HOST, "http://h"), (H_API_KEY, "k")]);
        let ctx = resolve_context(&h, Some("q"), &cfg()).unwrap();
        assert_eq!(ctx.index.as_deref(), Some("q"));
    }

    // --- step 6: Authorization: Bearer ----------------------------------------------

    #[test]
    fn bearer_is_api_key_fallback() {
        let h = headers(&[
            (H_HOST, "http://localhost:7700"),
            ("authorization", "Bearer masterKey"),
        ]);
        let ctx = resolve_context(&h, None, &cfg()).unwrap();
        assert_eq!(ctx.api_key.as_deref(), Some("masterKey"));
        assert_eq!(ctx.host.as_deref(), Some("http://localhost:7700"));
    }

    #[test]
    fn envoy_api_key_header_beats_bearer() {
        let mut h = envoy_headers();
        h.insert("authorization", HeaderValue::from_static("Bearer other"));
        let ctx = resolve_context(&h, None, &cfg()).unwrap();
        assert_eq!(ctx.api_key.as_deref(), Some("envoyKey"));
    }

    #[test]
    fn bearer_scheme_is_case_insensitive_and_other_schemes_are_ignored() {
        let h = headers(&[(H_HOST, "http://h"), ("authorization", "bearer   k1  ")]);
        assert_eq!(
            resolve_context(&h, None, &cfg())
                .unwrap()
                .api_key
                .as_deref(),
            Some("k1")
        );
        let h = headers(&[(H_HOST, "http://h"), ("authorization", "Basic abc")]);
        assert!(matches!(
            resolve_context(&h, None, &cfg()),
            Err(GatewayError::MissingContext(_))
        ));
        let h = headers(&[(H_HOST, "http://h"), ("authorization", "Bearer ")]);
        assert!(matches!(
            resolve_context(&h, None, &cfg()),
            Err(GatewayError::MissingContext(_))
        ));
    }

    // --- step 7: env fallbacks ------------------------------------------------------

    #[test]
    fn standalone_env_only() {
        let ctx = resolve_context(&HeaderMap::new(), None, &cfg_env()).unwrap();
        assert_eq!(ctx.host.as_deref(), Some("http://env:7700"));
        assert_eq!(ctx.api_key.as_deref(), Some("envKey"));
        assert_eq!(ctx.project_id, None);
        assert_eq!(ctx.index, None);
    }

    #[test]
    fn standalone_env_with_query_index() {
        let ctx = resolve_context(&HeaderMap::new(), Some("mine"), &cfg_env()).unwrap();
        assert_eq!(ctx.index.as_deref(), Some("mine"));
    }

    #[test]
    fn headers_beat_env() {
        let ctx = resolve_context(&envoy_headers(), None, &cfg_env()).unwrap();
        assert_eq!(
            ctx.host.as_deref(),
            Some("https://xxx.us-west.meilisearch.io")
        );
        assert_eq!(ctx.api_key.as_deref(), Some("envoyKey"));
    }

    #[test]
    fn bearer_beats_env_key_and_env_host_fills_in() {
        let h = headers(&[("authorization", "Bearer bearerKey")]);
        let ctx = resolve_context(&h, None, &cfg_env()).unwrap();
        assert_eq!(ctx.api_key.as_deref(), Some("bearerKey"));
        assert_eq!(ctx.host.as_deref(), Some("http://env:7700"));
    }

    #[test]
    fn env_host_only_with_header_key() {
        let cfg = GatewayConfig {
            meili_url: Some("http://env:7700".into()),
            ..Default::default()
        };
        let h = headers(&[(H_API_KEY, "k")]);
        let ctx = resolve_context(&h, None, &cfg).unwrap();
        assert_eq!(ctx.host.as_deref(), Some("http://env:7700"));
        assert_eq!(ctx.api_key.as_deref(), Some("k"));
    }

    // --- nothing → 400 ----------------------------------------------------------------

    #[test]
    fn nothing_is_400_missing_context() {
        let err = resolve_context(&HeaderMap::new(), None, &cfg()).unwrap_err();
        assert!(matches!(err, GatewayError::MissingContext(_)));
        assert_eq!(err.status(), axum::http::StatusCode::BAD_REQUEST);
    }

    #[test]
    fn host_without_any_key_is_400() {
        let h = headers(&[(H_HOST, "http://h")]);
        let err = resolve_context(&h, None, &cfg()).unwrap_err();
        assert!(matches!(err, GatewayError::MissingContext(m) if m.contains("API key")));
    }

    #[test]
    fn key_without_any_host_is_400() {
        let h = headers(&[(H_API_KEY, "k")]);
        let err = resolve_context(&h, None, &cfg()).unwrap_err();
        assert!(matches!(err, GatewayError::MissingContext(m) if m.contains("host")));
    }

    #[test]
    fn error_messages_never_contain_the_key() {
        let h = headers(&[(H_API_KEY, "TOPSECRET")]);
        let err = resolve_context(&h, None, &cfg()).unwrap_err();
        assert!(!err.to_string().contains("TOPSECRET"));
    }

    // --- Envoy secret -----------------------------------------------------------------

    #[test]
    fn secret_unset_trusts_headers() {
        assert!(envoy_headers_trusted(&envoy_headers(), &cfg()));
        assert!(resolve_context(&envoy_headers(), None, &cfg()).is_ok());
    }

    #[test]
    fn secret_set_and_present_trusts_headers() {
        let mut h = envoy_headers();
        h.insert(H_ENVOY_SECRET, HeaderValue::from_static("s3cret"));
        assert!(envoy_headers_trusted(&h, &cfg_secret()));
        let ctx = resolve_context(&h, None, &cfg_secret()).unwrap();
        assert_eq!(ctx.project_id.as_deref(), Some("xxx"));
        assert_eq!(ctx.api_key.as_deref(), Some("envoyKey"));
    }

    #[test]
    fn secret_set_but_absent_ignores_headers() {
        let h = envoy_headers();
        assert!(!envoy_headers_trusted(&h, &cfg_secret()));
        let err = resolve_context(&h, None, &cfg_secret()).unwrap_err();
        assert!(matches!(err, GatewayError::MissingContext(_)));
    }

    #[test]
    fn secret_set_but_wrong_ignores_headers() {
        let mut h = envoy_headers();
        h.insert(H_ENVOY_SECRET, HeaderValue::from_static("nope"));
        assert!(!envoy_headers_trusted(&h, &cfg_secret()));
        assert!(matches!(
            resolve_context(&h, None, &cfg_secret()),
            Err(GatewayError::MissingContext(_))
        ));
    }

    #[test]
    fn wrong_secret_falls_back_to_standalone_without_tenant() {
        let cfg = GatewayConfig {
            envoy_trusted_header: Some("s3cret".into()),
            meili_url: Some("http://env:7700".into()),
            meili_api_key: Some("envKey".into()),
            ..Default::default()
        };
        let mut h = envoy_headers();
        h.insert(H_ENVOY_SECRET, HeaderValue::from_static("nope"));
        let ctx = resolve_context(&h, Some("q"), &cfg).unwrap();
        assert_eq!(ctx.host.as_deref(), Some("http://env:7700"));
        assert_eq!(ctx.api_key.as_deref(), Some("envKey"));
        assert_eq!(ctx.project_id, None, "tenant header must not be honoured");
        assert_eq!(ctx.region, None);
        assert_eq!(
            ctx.index.as_deref(),
            Some("q"),
            "query param is not an X-Meili header"
        );
    }

    #[test]
    fn wrong_secret_still_honours_bearer() {
        let mut h = envoy_headers();
        h.insert(H_ENVOY_SECRET, HeaderValue::from_static("nope"));
        h.insert("authorization", HeaderValue::from_static("Bearer b"));
        let cfg = GatewayConfig {
            envoy_trusted_header: Some("s3cret".into()),
            meili_url: Some("http://env:7700".into()),
            ..Default::default()
        };
        let ctx = resolve_context(&h, None, &cfg).unwrap();
        assert_eq!(ctx.api_key.as_deref(), Some("b"));
    }

    #[test]
    fn resolve_project_id_follows_trust_rule() {
        assert_eq!(
            resolve_project_id(&envoy_headers(), &cfg()).as_deref(),
            Some("xxx")
        );
        assert_eq!(resolve_project_id(&envoy_headers(), &cfg_secret()), None);
        let mut h = envoy_headers();
        h.insert(H_ENVOY_SECRET, HeaderValue::from_static("s3cret"));
        assert_eq!(
            resolve_project_id(&h, &cfg_secret()).as_deref(),
            Some("xxx")
        );
        assert_eq!(resolve_project_id(&HeaderMap::new(), &cfg()), None);
    }

    // --- index chain (SPEC §3.4) --------------------------------------------------------

    fn ctx_with_index(index: Option<&str>) -> MeiliContext {
        MeiliContext {
            project_id: None,
            host: Some("http://h".into()),
            api_key: Some("k".into()),
            index: index.map(str::to_string),
            region: None,
        }
    }

    #[test]
    fn index_pattern_overrides_header_and_query() {
        let mut ctx = ctx_with_index(Some("from-query"));
        let idx = resolve_index(&mut ctx, Some("contracts"), "application/pdf", "documents");
        assert_eq!(idx, "contracts");
        assert_eq!(ctx.index.as_deref(), Some("contracts"));
    }

    #[test]
    fn header_or_query_index_kept_without_pattern() {
        let mut ctx = ctx_with_index(Some("mine"));
        assert_eq!(
            resolve_index(&mut ctx, None, "video/mp4", "documents"),
            "mine"
        );
        let mut ctx = ctx_with_index(Some("mine"));
        assert_eq!(
            resolve_index(&mut ctx, Some("   "), "video/mp4", "documents"),
            "mine"
        );
    }

    #[test]
    fn mime_default_when_nothing_set() {
        for (mime, expected) in [
            ("video/mp4", "videos"),
            ("audio/mpeg", "audio"),
            ("image/png", "images"),
            ("text/html", "pages"),
            ("text/csv", "datasets"),
        ] {
            let mut ctx = ctx_with_index(None);
            assert_eq!(
                resolve_index(&mut ctx, None, mime, "fallback"),
                expected,
                "{mime}"
            );
            assert_eq!(ctx.index.as_deref(), Some(expected));
        }
    }

    #[test]
    fn global_default_replaces_generic_documents() {
        let mut ctx = ctx_with_index(None);
        assert_eq!(
            resolve_index(&mut ctx, None, "application/pdf", "my-docs"),
            "my-docs"
        );
        let mut ctx = ctx_with_index(None);
        assert_eq!(
            resolve_index(&mut ctx, None, "application/pdf", "documents"),
            "documents"
        );
        let mut ctx = ctx_with_index(Some(""));
        assert_eq!(
            resolve_index(&mut ctx, None, "application/octet-stream", "my-docs"),
            "my-docs"
        );
    }

    #[test]
    fn full_chain_precedence() {
        // pattern > query/header > mime default > global default
        let mut ctx = ctx_with_index(Some("hdr"));
        assert_eq!(
            resolve_index(&mut ctx, Some("pat"), "video/mp4", "glob"),
            "pat"
        );
        let mut ctx = ctx_with_index(Some("hdr"));
        assert_eq!(resolve_index(&mut ctx, None, "video/mp4", "glob"), "hdr");
        let mut ctx = ctx_with_index(None);
        assert_eq!(resolve_index(&mut ctx, None, "video/mp4", "glob"), "videos");
        let mut ctx = ctx_with_index(None);
        assert_eq!(resolve_index(&mut ctx, None, "text/plain", "glob"), "glob");
    }
}
