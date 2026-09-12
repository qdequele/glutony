//! Serves the admin UI from inside the gateway binary.
//!
//! The UI is a Next.js static export compiled in at build time behind the `ui`
//! feature, so production runs one container with no Node runtime and the browser
//! talks to the API same-origin (no CORS, no second hostname to configure).
//!
//! Without the feature the routes below answer 404 and the gateway is exactly the
//! API server it was before. That keeps `cargo build` working for contributors who
//! have no Node toolchain, and keeps the API-only deployment honest.

use axum::Router;

#[cfg(feature = "ui")]
mod embedded {
    use axum::Router;
    use axum::body::Body;
    use axum::extract::Path;
    use axum::http::{HeaderValue, StatusCode, Uri, header};
    use axum::response::{IntoResponse, Response};
    use axum::routing::get;
    use rust_embed::Embed;

    /// The exported site. `ui/out` is produced by `pnpm build` in `ui/`.
    #[derive(Embed)]
    #[folder = "$CARGO_MANIFEST_DIR/../../ui/out"]
    struct Assets;

    /// Mount the UI at `/ui`, with a catch-all that falls back to the exported
    /// `index.html` so client-side routes survive a refresh.
    pub fn router() -> Router<crate::state::AppState> {
        Router::new()
            .route("/ui", get(|| async { serve("index.html") }))
            .route(
                "/ui/{*path}",
                get(|Path(path): Path<String>| async move { serve(&path) }),
            )
    }

    fn serve(path: &str) -> Response {
        let candidate = path.trim_start_matches('/');
        // Next.js `trailingSlash: true` exports `pipelines/index.html`.
        for key in [
            candidate.to_string(),
            format!("{}/index.html", candidate.trim_end_matches('/')),
            format!("{candidate}.html"),
        ] {
            if let Some(file) = Assets::get(&key) {
                let mime = mime_guess::from_path(&key).first_or_octet_stream();
                let mut resp = Response::new(Body::from(file.data.into_owned()));
                if let Ok(value) = HeaderValue::from_str(mime.as_ref()) {
                    resp.headers_mut().insert(header::CONTENT_TYPE, value);
                }
                // Hashed asset paths are immutable; everything else must revalidate
                // or a deploy would leave stale HTML pointing at deleted bundles.
                let cache = if key.starts_with("_next/static/") {
                    "public, max-age=31536000, immutable"
                } else {
                    "no-cache"
                };
                resp.headers_mut()
                    .insert(header::CACHE_CONTROL, HeaderValue::from_static(cache));
                return resp;
            }
        }
        // Unknown path inside the SPA: hand back the shell and let the router decide.
        match Assets::get("index.html") {
            Some(file) => {
                let mut resp = Response::new(Body::from(file.data.into_owned()));
                resp.headers_mut().insert(
                    header::CONTENT_TYPE,
                    HeaderValue::from_static("text/html; charset=utf-8"),
                );
                resp
            }
            None => (StatusCode::NOT_FOUND, "ui asset not found").into_response(),
        }
    }

    /// Redirect the bare root to the UI so operators can just open the host.
    pub async fn root_redirect(_uri: Uri) -> Response {
        axum::response::Redirect::temporary("/ui/").into_response()
    }
}

/// Routes serving the admin UI, empty unless the `ui` feature is on.
pub fn router() -> Router<crate::state::AppState> {
    #[cfg(feature = "ui")]
    {
        embedded::router().route("/", axum::routing::get(embedded::root_redirect))
    }
    #[cfg(not(feature = "ui"))]
    {
        Router::new()
    }
}

/// Whether this binary has the UI compiled in (reported by `GET /health`).
pub const fn is_embedded() -> bool {
    cfg!(feature = "ui")
}
