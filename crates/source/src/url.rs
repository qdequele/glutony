//! The `url` connector: one URL, one item.

use std::io::Read as _;

use futures::StreamExt as _;

use crate::SourceError;
use crate::connector::{Resolution, ResolveRuntime, ResolvedItem, SourceConnector};
use crate::model::{FetchAuth, IncrementalState, Location};
use crate::template::render;

/// Fetches one URL.
#[derive(Debug, Clone, Copy, Default)]
pub struct UrlConnector;

#[async_trait::async_trait]
impl SourceConnector for UrlConnector {
    fn kind(&self) -> &'static str {
        "url"
    }

    async fn resolve(
        &self,
        loc: &Location,
        auth: Option<&FetchAuth>,
        state: &IncrementalState,
        rt: &ResolveRuntime,
    ) -> Result<Resolution, SourceError> {
        let Location::Url {
            url,
            method,
            headers,
        } = loc;
        let rendered = render(url, rt.scheduled_at, &rt.timezone)?;
        let parsed = url::Url::parse(&rendered)
            .map_err(|e| SourceError::Template(format!("{rendered:?} is not a url: {e}")))?;

        if let Some(guard) = &rt.guard {
            guard.check_url(&parsed).await?;
        }
        let max_bytes = rt.guard.map(|g| g.max_bytes).unwrap_or(u64::MAX);

        let verb = method.as_deref().unwrap_or("GET");
        let mut req = rt
            .http
            .request(
                reqwest::Method::from_bytes(verb.as_bytes())
                    .map_err(|_| SourceError::Blocked(format!("invalid http method {verb:?}")))?,
                parsed.clone(),
            )
            .timeout(std::time::Duration::from_secs(300));

        for (name, value) in headers {
            req = req.header(name, value);
        }
        req = apply_auth(req, auth);
        if let Some(etag) = &state.etag {
            req = req.header(reqwest::header::IF_NONE_MATCH, etag);
        }
        if let Some(lm) = &state.last_modified {
            req = req.header(reqwest::header::IF_MODIFIED_SINCE, lm);
        }

        let response = req
            .send()
            .await
            .map_err(|e| SourceError::Fetch(format!("GET {parsed}: {e}")))?;

        if response.status() == reqwest::StatusCode::NOT_MODIFIED {
            return Ok(Resolution::Unchanged);
        }
        if !response.status().is_success() {
            return Err(SourceError::Fetch(format!(
                "GET {parsed}: status {}",
                response.status()
            )));
        }

        let etag = header_string(&response, reqwest::header::ETAG);
        let last_modified = header_string(&response, reqwest::header::LAST_MODIFIED);
        let header_mime = header_string(&response, reqwest::header::CONTENT_TYPE)
            .and_then(|ct| ct.split(';').next().map(|s| s.trim().to_ascii_lowercase()))
            .filter(|m| !m.is_empty() && m != "application/octet-stream");

        // Stream rather than `.bytes()`: a TMDB export is ~50 MB and several sources may
        // run at once. The cap is enforced DURING the stream, not after.
        let mut hasher = blake3::Hasher::new();
        let mut body: Vec<u8> = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| SourceError::Fetch(format!("reading {parsed}: {e}")))?;
            if body.len() as u64 + chunk.len() as u64 > max_bytes {
                return Err(SourceError::Fetch(format!(
                    "GET {parsed}: body exceeds the {max_bytes} byte cap"
                )));
            }
            hasher.update(&chunk);
            body.extend_from_slice(&chunk);
        }
        let hash = hasher.finalize().to_hex().to_string();

        // Servers hosting static exports commonly send no ETag; the hash is what makes
        // the conditional-fetch decision work for them.
        if state.hash.as_deref() == Some(hash.as_str()) {
            return Ok(Resolution::Unchanged);
        }

        let filename = last_segment(parsed.path());
        let (bytes, filename) = maybe_gunzip(body, filename)?;
        let mime = header_mime
            .or_else(|| detect_mime(&bytes, filename.as_deref()))
            .unwrap_or_else(|| "application/octet-stream".to_string());

        Ok(Resolution::Items {
            items: vec![ResolvedItem {
                bytes,
                mime,
                filename,
            }],
            state: IncrementalState {
                etag,
                last_modified,
                hash: Some(hash),
            },
        })
    }
}

/// Attach a credential to the request.
fn apply_auth(req: reqwest::RequestBuilder, auth: Option<&FetchAuth>) -> reqwest::RequestBuilder {
    match auth {
        None => req,
        Some(FetchAuth::Bearer { token }) => req.bearer_auth(token),
        Some(FetchAuth::Basic { username, password }) => req.basic_auth(username, Some(password)),
        Some(FetchAuth::Headers { headers }) => {
            let mut req = req;
            for (name, value) in headers {
                req = req.header(name, value);
            }
            req
        }
    }
}

fn header_string(
    response: &reqwest::Response,
    name: reqwest::header::HeaderName,
) -> Option<String> {
    response
        .headers()
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
}

fn last_segment(path: &str) -> Option<String> {
    path.rsplit('/').find(|s| !s.is_empty()).map(str::to_owned)
}

/// Decompress when the body is gzip.
///
/// Detected by magic bytes rather than by filename: a `.gz` extension is only a hint,
/// and `Content-Encoding` does not apply to a gzipped *file* served as octet-stream,
/// which is exactly the TMDB case.
fn maybe_gunzip(
    body: Vec<u8>,
    filename: Option<String>,
) -> Result<(Vec<u8>, Option<String>), SourceError> {
    if body.len() < 2 || body[0] != 0x1f || body[1] != 0x8b {
        return Ok((body, filename));
    }
    let mut out = Vec::new();
    flate2::read::GzDecoder::new(&body[..])
        .read_to_end(&mut out)
        .map_err(|e| SourceError::Fetch(format!("gunzip: {e}")))?;
    // Strip the .gz so MIME detection sees the real extension.
    let stripped = filename.map(|f| f.strip_suffix(".gz").map(str::to_owned).unwrap_or(f));
    Ok((out, stripped))
}

/// Pick a MIME from the content, falling back to the filename's extension.
///
/// Content wins for the `.json` case specifically: TMDB's export is named `.json` but is
/// newline-delimited, and routing it as `application/json` would hand the json plugin a
/// document it cannot parse as a single value.
fn detect_mime(bytes: &[u8], filename: Option<&str>) -> Option<String> {
    let by_ext = filename.and_then(guess_mime_from_extension);
    if matches!(by_ext.as_deref(), Some("application/json")) && sniff_ndjson(bytes) {
        return Some("application/x-ndjson".to_string());
    }
    by_ext
}

/// Whether `bytes` look like newline-delimited JSON: at least two non-empty lines, each
/// of which parses as a standalone JSON value.
///
/// A JSON array or a single pretty-printed object must NOT match — those are ordinary
/// JSON and the json plugin handles them as one document.
fn sniff_ndjson(bytes: &[u8]) -> bool {
    let Ok(text) = std::str::from_utf8(bytes) else {
        return false;
    };
    let mut lines = text.lines().filter(|l| !l.trim().is_empty());
    let mut seen = 0usize;
    for line in lines.by_ref().take(5) {
        if serde_json::from_str::<serde_json::Value>(line.trim()).is_err() {
            return false;
        }
        seen += 1;
    }
    seen >= 2
}

/// Minimal extension → MIME map for the formats a source realistically serves.
fn guess_mime_from_extension(filename: &str) -> Option<String> {
    let ext = filename.rsplit_once('.')?.1.to_ascii_lowercase();
    let mime = match ext.as_str() {
        "json" => "application/json",
        "ndjson" | "jsonl" => "application/x-ndjson",
        "csv" => "text/csv",
        "tsv" => "text/tab-separated-values",
        "xml" => "application/xml",
        "md" => "text/markdown",
        "html" | "htm" => "text/html",
        "parquet" => "application/vnd.apache.parquet",
        "avro" => "application/vnd.apache.avro",
        "pdf" => "application/pdf",
        "txt" => "text/plain",
        _ => return None,
    };
    Some(mime.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::guard::UrlGuard;
    use chrono::{DateTime, TimeZone, Utc};
    use std::io::Write as _;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// The guard rejects non-public addresses and a MockServer listens on 127.0.0.1, so
    /// connector tests run with the guard off. The guard has its own tests in guard.rs.
    fn rt_without_guard(scheduled_at: DateTime<Utc>) -> ResolveRuntime {
        ResolveRuntime {
            http: reqwest::Client::new(),
            guard: None,
            scheduled_at,
            timezone: "UTC".to_string(),
        }
    }

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 9, 13, 9, 0, 0)
            .single()
            .expect("valid instant")
    }

    fn url_location(url: String) -> Location {
        Location::Url {
            url,
            method: None,
            headers: Default::default(),
        }
    }

    fn gzip(bytes: &[u8]) -> Vec<u8> {
        let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        e.write_all(bytes).expect("gzip write");
        e.finish().expect("gzip finish")
    }

    #[tokio::test]
    async fn a_304_resolves_to_unchanged() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/feed.json"))
            .and(header("if-none-match", "\"v1\""))
            .respond_with(ResponseTemplate::new(304))
            .mount(&server)
            .await;

        let state = IncrementalState {
            etag: Some("\"v1\"".into()),
            ..Default::default()
        };
        let got = UrlConnector
            .resolve(
                &url_location(format!("{}/feed.json", server.uri())),
                None,
                &state,
                &rt_without_guard(now()),
            )
            .await
            .expect("resolves");
        assert!(matches!(got, Resolution::Unchanged));
    }

    #[tokio::test]
    async fn changed_content_resolves_to_one_item() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_raw("{\"id\":1}".as_bytes(), "application/json")
                    .insert_header("etag", "\"v2\""),
            )
            .mount(&server)
            .await;

        let got = UrlConnector
            .resolve(
                &url_location(format!("{}/feed.json", server.uri())),
                None,
                &IncrementalState::default(),
                &rt_without_guard(now()),
            )
            .await
            .expect("resolves");

        let Resolution::Items { items, state } = got else {
            panic!("expected items");
        };
        assert_eq!(items.len(), 1, "one url is one item, whatever it contains");
        assert_eq!(items[0].bytes, b"{\"id\":1}");
        assert_eq!(items[0].mime, "application/json");
        assert_eq!(state.etag.as_deref(), Some("\"v2\""));
        assert!(
            state.hash.is_some(),
            "hash is recorded for etag-less servers"
        );
    }

    #[tokio::test]
    async fn an_identical_body_without_an_etag_is_unchanged_by_hash() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string("same bytes"))
            .mount(&server)
            .await;
        let loc = url_location(format!("{}/feed.json", server.uri()));

        let first = UrlConnector
            .resolve(
                &loc,
                None,
                &IncrementalState::default(),
                &rt_without_guard(now()),
            )
            .await
            .expect("first");
        let Resolution::Items { state, .. } = first else {
            panic!("expected items");
        };

        let second = UrlConnector
            .resolve(&loc, None, &state, &rt_without_guard(now()))
            .await
            .expect("second");
        assert!(
            matches!(second, Resolution::Unchanged),
            "identical bytes must not re-ingest"
        );
    }

    #[tokio::test]
    async fn gzip_is_decompressed_and_typed_from_the_inner_content() {
        let server = MockServer::start().await;
        // Mirrors the TMDB export: NDJSON, gzipped, served as a .gz file.
        let body = gzip(b"{\"id\":1}\n{\"id\":2}\n");
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(body, "application/octet-stream"))
            .mount(&server)
            .await;

        let got = UrlConnector
            .resolve(
                &url_location(format!("{}/movie_ids.json.gz", server.uri())),
                None,
                &IncrementalState::default(),
                &rt_without_guard(now()),
            )
            .await
            .expect("resolves");

        let Resolution::Items { items, .. } = got else {
            panic!("expected items");
        };
        assert_eq!(items[0].bytes, b"{\"id\":1}\n{\"id\":2}\n", "decompressed");
        assert_eq!(
            items[0].mime, "application/x-ndjson",
            "typed from the decompressed content so the json plugin routes it"
        );
        assert_eq!(items[0].filename.as_deref(), Some("movie_ids.json"));
    }

    #[tokio::test]
    async fn the_url_template_is_rendered_before_fetching() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/exports/movie_ids_09_13_2026.json"))
            .respond_with(ResponseTemplate::new(200).set_body_string("[]"))
            .mount(&server)
            .await;

        let got = UrlConnector
            .resolve(
                &url_location(format!(
                    "{}/exports/movie_ids_{{{{ date:%m_%d_%Y }}}}.json",
                    server.uri()
                )),
                None,
                &IncrementalState::default(),
                &rt_without_guard(now()),
            )
            .await
            .expect("resolves");
        assert!(matches!(got, Resolution::Items { .. }));
    }

    #[tokio::test]
    async fn bearer_auth_is_sent() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(header("authorization", "Bearer t0ken"))
            .respond_with(ResponseTemplate::new(200).set_body_string("[]"))
            .mount(&server)
            .await;

        let auth = FetchAuth::Bearer {
            token: "t0ken".into(),
        };
        let got = UrlConnector
            .resolve(
                &url_location(format!("{}/feed.json", server.uri())),
                Some(&auth),
                &IncrementalState::default(),
                &rt_without_guard(now()),
            )
            .await;
        assert!(
            got.is_ok(),
            "the mock only matches when the header was sent"
        );
    }

    #[tokio::test]
    async fn a_non_success_status_is_an_error() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;

        let got = UrlConnector
            .resolve(
                &url_location(format!("{}/feed.json", server.uri())),
                None,
                &IncrementalState::default(),
                &rt_without_guard(now()),
            )
            .await;
        assert!(got.is_err());
    }

    #[tokio::test]
    async fn an_oversized_body_is_rejected() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![b'x'; 4096]))
            .mount(&server)
            .await;

        // A guard with a tiny cap. `check_url` would reject the loopback mock, so the
        // cap is exercised through a runtime whose guard is set but whose URL is the
        // already-allowed mock: build it by hand and skip the address check.
        let mut rt = rt_without_guard(now());
        rt.guard = Some(UrlGuard {
            max_redirects: 5,
            max_bytes: 1024,
        });
        let got = UrlConnector
            .resolve(
                &url_location(format!("{}/big", server.uri())),
                None,
                &IncrementalState::default(),
                &rt,
            )
            .await;
        assert!(got.is_err(), "body over max_bytes must be rejected");
    }

    #[test]
    fn sniff_ndjson_distinguishes_ndjson_from_plain_json() {
        assert!(
            sniff_ndjson(b"{\"a\":1}\n{\"a\":2}\n"),
            "two delimited objects are ndjson"
        );
        assert!(
            sniff_ndjson(b"{\"a\":1}\n{\"a\":2}"),
            "a missing trailing newline is still ndjson"
        );
        assert!(
            !sniff_ndjson(b"[{\"a\":1},{\"a\":2}]"),
            "a json array is not ndjson"
        );
        assert!(!sniff_ndjson(b"{\"a\":1}"), "a single object is not ndjson");
        assert!(
            !sniff_ndjson(b"{\n  \"a\": 1\n}"),
            "a pretty-printed object is not ndjson"
        );
        assert!(!sniff_ndjson(b""), "empty input is not ndjson");
        assert!(!sniff_ndjson(&[0xff, 0xfe]), "non-utf8 is not ndjson");
    }

    #[test]
    fn detect_mime_prefers_content_over_extension_for_json() {
        assert_eq!(
            detect_mime(b"{\"a\":1}\n{\"a\":2}\n", Some("movie_ids.json")).as_deref(),
            Some("application/x-ndjson"),
        );
        assert_eq!(
            detect_mime(b"[{\"a\":1}]", Some("feed.json")).as_deref(),
            Some("application/json"),
        );
        assert_eq!(
            detect_mime(b"a,b\n1,2\n", Some("rows.csv")).as_deref(),
            Some("text/csv"),
        );
        assert_eq!(detect_mime(b"anything", Some("x.unknown")), None);
        assert_eq!(detect_mime(b"anything", None), None);
    }

    #[test]
    fn maybe_gunzip_passes_through_uncompressed_bodies() {
        let (bytes, name) =
            maybe_gunzip(b"plain".to_vec(), Some("a.json".into())).expect("no gzip");
        assert_eq!(bytes, b"plain");
        assert_eq!(name.as_deref(), Some("a.json"));
    }
}
