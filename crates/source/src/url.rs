//! The `url` connector: one URL, one item.

use std::io::Read as _;

use futures::StreamExt as _;

use crate::SourceError;
use crate::connector::{Resolution, ResolveRuntime, ResolvedItem, SourceConnector};
use crate::guard::UrlGuard;
use crate::host_policy::HostPolicy;
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

        let max_bytes = rt.limits.max_bytes;
        let verb = method.as_deref().unwrap_or("GET");
        let mut method = reqwest::Method::from_bytes(verb.as_bytes())
            .map_err(|_| SourceError::Blocked(format!("invalid http method {verb:?}")))?;
        let origin = parsed.origin();

        // Redirects are followed here, not by the client, so the host policy sees every
        // hop: a public host redirecting to 169.254.169.254 is the obvious bypass.
        // One deadline for the whole fetch, redirects and body included. reqwest's
        // per-request timeout runs until the body has been read, so each hop is given
        // what is left of it.
        let deadline = tokio::time::Instant::now() + rt.limits.total_timeout;
        let mut current = parsed.clone();
        let mut hops = 0usize;
        let response = loop {
            if let Some(policy) = &rt.policy {
                policy.check_disclosing(&current, rt.list_allowed).await?;
            }
            let mut req = rt
                .http
                .request(method.clone(), current.clone())
                .timeout(deadline.saturating_duration_since(tokio::time::Instant::now()));
            for (name, value) in headers {
                req = req.header(name, value);
            }
            // Credentials go to the source's own origin only, the way browsers drop
            // Authorization on a cross-origin redirect.
            if current.origin() == origin {
                req = apply_auth(req, auth);
            }
            if let Some(etag) = &state.etag {
                req = req.header(reqwest::header::IF_NONE_MATCH, etag);
            }
            if let Some(lm) = &state.last_modified {
                req = req.header(reqwest::header::IF_MODIFIED_SINCE, lm);
            }

            let response = req
                .send()
                .await
                .map_err(|e| SourceError::Fetch(format!("{method} {current}: {e}")))?;
            let status = response.status();
            if !status.is_redirection() || status == reqwest::StatusCode::NOT_MODIFIED {
                break response;
            }
            if hops >= rt.limits.max_redirects {
                return Err(SourceError::Fetch(format!(
                    "{parsed}: more than {} redirects",
                    rt.limits.max_redirects
                )));
            }
            let location = response
                .headers()
                .get(reqwest::header::LOCATION)
                .and_then(|v| v.to_str().ok())
                .ok_or_else(|| {
                    SourceError::Fetch(format!("{current}: {status} without a Location header"))
                })?;
            let next = current.join(location).map_err(|e| {
                SourceError::Fetch(format!("{current}: invalid redirect {location:?}: {e}"))
            })?;
            // 303 always becomes a GET; 301/302 historically do for non-GET methods.
            if status == reqwest::StatusCode::SEE_OTHER
                || (matches!(status.as_u16(), 301 | 302) && method != reqwest::Method::HEAD)
            {
                method = reqwest::Method::GET;
            }
            current = next;
            hops += 1;
        };

        if response.status() == reqwest::StatusCode::NOT_MODIFIED {
            return Ok(Resolution::Unchanged);
        }
        let status = response.status();
        if !status.is_success() {
            let message = format!("GET {parsed}: status {status}");
            let transient = !status.is_client_error()
                || matches!(
                    status,
                    reqwest::StatusCode::REQUEST_TIMEOUT | reqwest::StatusCode::TOO_MANY_REQUESTS
                );
            return Err(if transient {
                SourceError::Fetch(message)
            } else {
                SourceError::Rejected(message)
            });
        }

        let etag = header_string(&response, reqwest::header::ETAG);
        let last_modified = header_string(&response, reqwest::header::LAST_MODIFIED);
        let content_type = header_string(&response, reqwest::header::CONTENT_TYPE);
        let header_mime = content_type
            .as_deref()
            .and_then(|ct| ct.split(';').next().map(|s| s.trim().to_ascii_lowercase()))
            .filter(|m| !m.is_empty() && m != "application/octet-stream");
        // Kept apart from the MIME, which routing compares as a bare essence.
        let charset = content_type.as_deref().and_then(content_type_charset);

        // Stream rather than `.bytes()`: a TMDB export is ~50 MB and several sources may
        // run at once. The cap is enforced DURING the stream, not after.
        let mut hasher = blake3::Hasher::new();
        let mut body: Vec<u8> = Vec::new();
        let mut stream = response.bytes_stream();
        let idle = rt.limits.idle_timeout;
        loop {
            // A body that keeps arriving may take the whole deadline; one that stops
            // arriving is given up after `idle`, not held until the deadline.
            let next = tokio::time::timeout(idle, stream.next())
                .await
                .map_err(|_| {
                    SourceError::Fetch(format!(
                        "reading {parsed}: no data received for {} s",
                        idle.as_secs_f32()
                    ))
                })?;
            let Some(chunk) = next else { break };
            let chunk = chunk.map_err(|e| {
                SourceError::Fetch(if e.is_timeout() {
                    format!(
                        "reading {parsed}: the download did not finish within {} s",
                        rt.limits.total_timeout.as_secs_f32()
                    )
                } else {
                    format!("reading {parsed}: {e}")
                })
            })?;
            if body.len() as u64 + chunk.len() as u64 > max_bytes {
                return Err(SourceError::TooLarge(format!(
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

        // Inflating and sniffing up to `max_bytes` is seconds of CPU: off the async
        // worker threads, which the worker's other activities share.
        let filename = last_segment(parsed.path());
        let (bytes, filename, mime) = tokio::task::spawn_blocking(move || {
            let (bytes, filename) = maybe_gunzip(body, filename, max_bytes)?;
            let mime = header_mime
                .or_else(|| detect_mime(&bytes, filename.as_deref()))
                .unwrap_or_else(|| "application/octet-stream".to_string());
            Ok::<_, SourceError>((bytes, filename, mime))
        })
        .await
        .map_err(|e| SourceError::Fetch(format!("decoding {parsed}: {e}")))??;

        Ok(Resolution::Items {
            items: vec![ResolvedItem {
                bytes,
                mime,
                filename,
                charset,
            }],
            state: IncrementalState {
                etag,
                last_modified,
                hash: Some(hash),
            },
        })
    }
}

/// Learn the media type behind `url` without downloading it, for a URL whose name says
/// nothing (`https://example.com/`, `/api/users`).
///
/// Sends a `HEAD`, then a `GET` whose body is never read when the `HEAD` fails or names
/// no type (some servers refuse `HEAD`). Redirects are followed here, not by `http`, so
/// `policy` sees every hop, as in [`UrlConnector`]. `Ok(None)` when the server names no
/// type, only `application/octet-stream`, or answers with an error; a hop refused by
/// `policy` is an error.
pub async fn probe_content_type(
    http: &reqwest::Client,
    policy: Option<&HostPolicy>,
    url: &url::Url,
    limits: &UrlGuard,
) -> Result<Option<String>, SourceError> {
    for method in [reqwest::Method::HEAD, reqwest::Method::GET] {
        let mut current = url.clone();
        let mut hops = 0usize;
        let response = loop {
            if let Some(policy) = policy {
                policy.check(&current).await?;
            }
            let sent = http
                .request(method.clone(), current.clone())
                .timeout(std::time::Duration::from_secs(10))
                .send()
                .await;
            let Ok(response) = sent else { break None };
            let status = response.status();
            if !status.is_redirection() {
                break Some(response);
            }
            let next = response
                .headers()
                .get(reqwest::header::LOCATION)
                .and_then(|v| v.to_str().ok())
                .and_then(|location| current.join(location).ok());
            match next {
                Some(next) if hops < limits.max_redirects => {
                    current = next;
                    hops += 1;
                }
                _ => break None,
            }
        };
        let mime = response
            .filter(|r| r.status().is_success())
            .and_then(|r| header_string(&r, reqwest::header::CONTENT_TYPE))
            .and_then(|ct| ct.split(';').next().map(|s| s.trim().to_ascii_lowercase()))
            .filter(|m| !m.is_empty() && m != "application/octet-stream");
        if mime.is_some() {
            return Ok(mime);
        }
    }
    Ok(None)
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

/// The `charset` parameter of a `Content-Type` value, unquoted and lower-cased;
/// `None` when there is none or it is empty.
fn content_type_charset(value: &str) -> Option<String> {
    value.split(';').skip(1).find_map(|param| {
        let (name, raw) = param.split_once('=')?;
        if !name.trim().eq_ignore_ascii_case("charset") {
            return None;
        }
        let label = raw.trim().trim_matches('"').trim().to_ascii_lowercase();
        (!label.is_empty()).then_some(label)
    })
}

fn last_segment(path: &str) -> Option<String> {
    path.rsplit('/').find(|s| !s.is_empty()).map(str::to_owned)
}

/// Decompress when the body is gzip.
///
/// Detected by magic bytes rather than by filename: a `.gz` extension is only a hint,
/// and `Content-Encoding` does not apply to a gzipped *file* served as octet-stream,
/// which is exactly the TMDB case.
///
/// The output is capped at `max_bytes` like the download: gzip compresses repetitive
/// input by over 1000x, so a small file could otherwise expand to exhaust the worker's
/// memory.
fn maybe_gunzip(
    body: Vec<u8>,
    filename: Option<String>,
    max_bytes: u64,
) -> Result<(Vec<u8>, Option<String>), SourceError> {
    if body.len() < 2 || body[0] != 0x1f || body[1] != 0x8b {
        return Ok((body, filename));
    }
    let mut out = Vec::new();
    flate2::read::GzDecoder::new(&body[..])
        .take(max_bytes.saturating_add(1))
        .read_to_end(&mut out)
        .map_err(|e| SourceError::Fetch(format!("gunzip: {e}")))?;
    if out.len() as u64 > max_bytes {
        return Err(SourceError::TooLarge(format!(
            "decompressed content exceeds the {max_bytes} byte cap"
        )));
    }
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

/// Whether `bytes` look like newline-delimited JSON: at least two non-empty lines, the
/// first five of which each parse as a standalone JSON value.
///
/// A JSON array or a single pretty-printed object must NOT match — those are ordinary
/// JSON and the json plugin handles them as one document.
///
/// The body can be hundreds of MiB, so nothing here is proportional to it in memory. A
/// second line is looked for before anything is parsed: a minified document is one
/// line, and parsing it to learn that it is not NDJSON would build its whole tree. The
/// lines that are checked are validated without being materialised.
fn sniff_ndjson(bytes: &[u8]) -> bool {
    let mut lines = bytes
        .split(|b| *b == b'\n')
        .map(<[u8]>::trim_ascii)
        .filter(|line| !line.is_empty());
    let (Some(first), Some(second)) = (lines.next(), lines.next()) else {
        return false;
    };
    [first, second]
        .into_iter()
        .chain(lines.take(3))
        .all(|line| serde_json::from_slice::<serde::de::IgnoredAny>(line).is_ok())
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
    use crate::host_policy::HostPolicy;
    use chrono::{DateTime, TimeZone, Utc};
    use std::io::Write as _;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// The guard rejects non-public addresses and a MockServer listens on 127.0.0.1, so
    /// connector tests run with the guard off. The guard has its own tests in guard.rs.
    fn rt_without_guard(scheduled_at: DateTime<Utc>) -> ResolveRuntime {
        ResolveRuntime {
            http: ResolveRuntime::http_client(),
            policy: None,
            list_allowed: false,
            limits: UrlGuard::default(),
            scheduled_at,
            timezone: "UTC".to_string(),
        }
    }

    /// A runtime whose policy allows exactly the given mock servers.
    fn rt_allowing(servers: &[&MockServer]) -> ResolveRuntime {
        let hosts: Vec<String> = servers
            .iter()
            .map(|s| s.uri().trim_start_matches("http://").to_string())
            .collect();
        let mut rt = rt_without_guard(now());
        rt.policy = Some(HostPolicy::parse(&hosts.join(",")).expect("policy"));
        rt
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
    async fn the_declared_charset_is_kept_apart_from_the_mime() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/legacy.html"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/html; charset=\"ISO-8859-1\"")
                    .set_body_bytes(b"<p>Caf\xE9</p>".to_vec()),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/feed.json"))
            .respond_with(ResponseTemplate::new(200).set_body_raw("[]", "application/json"))
            .mount(&server)
            .await;

        let resolve = |p: &str| {
            let location = url_location(format!("{}{p}", server.uri()));
            async move {
                let got = UrlConnector
                    .resolve(
                        &location,
                        None,
                        &IncrementalState::default(),
                        &rt_without_guard(now()),
                    )
                    .await
                    .expect("resolves");
                let Resolution::Items { mut items, .. } = got else {
                    panic!("expected items");
                };
                items.remove(0)
            }
        };
        let item = resolve("/legacy.html").await;
        assert_eq!(item.mime, "text/html");
        assert_eq!(item.charset.as_deref(), Some("iso-8859-1"));
        assert_eq!(item.bytes, b"<p>Caf\xE9</p>");
        assert_eq!(resolve("/feed.json").await.charset, None);
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
    async fn a_client_error_is_permanent_and_a_server_error_is_transient() {
        for (status, permanent) in [
            (400, true),
            (401, true),
            (403, true),
            (404, true),
            (410, true),
            (408, false),
            (429, false),
            (500, false),
            (503, false),
        ] {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .respond_with(ResponseTemplate::new(status))
                .mount(&server)
                .await;
            let err = UrlConnector
                .resolve(
                    &url_location(format!("{}/feed.json", server.uri())),
                    None,
                    &IncrementalState::default(),
                    &rt_without_guard(now()),
                )
                .await
                .expect_err("non-success");
            assert_eq!(
                matches!(err, SourceError::Rejected(_)),
                permanent,
                "{status}: {err:?}"
            );
            assert!(err.to_string().contains(&status.to_string()), "{err}");
        }
    }

    #[tokio::test]
    async fn an_oversized_body_is_rejected() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![b'x'; 4096]))
            .mount(&server)
            .await;

        // No address policy (the mock is loopback), but a tiny size cap: the cap must be
        // what rejects this, not the address check.
        let mut rt = rt_without_guard(now());
        rt.limits = UrlGuard {
            max_bytes: 1024,
            ..UrlGuard::default()
        };
        let err = UrlConnector
            .resolve(
                &url_location(format!("{}/big", server.uri())),
                None,
                &IncrementalState::default(),
                &rt,
            )
            .await
            .expect_err("body over max_bytes must be rejected");
        assert!(err.to_string().contains("byte cap"), "{err}");
    }

    fn redirect_to(location: &str) -> ResponseTemplate {
        ResponseTemplate::new(302).insert_header("location", location)
    }

    #[tokio::test]
    async fn a_redirect_is_followed_and_a_relative_location_resolves() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/old"))
            .respond_with(redirect_to("/new.json"))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/new.json"))
            .respond_with(ResponseTemplate::new(200).set_body_raw("[]", "application/json"))
            .mount(&server)
            .await;
        let got = UrlConnector
            .resolve(
                &url_location(format!("{}/old", server.uri())),
                None,
                &IncrementalState::default(),
                &rt_allowing(&[&server]),
            )
            .await
            .expect("follows the redirect");
        assert!(matches!(got, Resolution::Items { .. }));
    }

    #[tokio::test]
    async fn a_redirect_to_a_host_the_policy_forbids_is_not_fetched() {
        // `allowed` is permitted; it redirects to `forbidden`, which is not. The policy
        // must see the second hop, so `forbidden` is never contacted.
        let allowed = MockServer::start().await;
        let forbidden = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(redirect_to(&format!("{}/secret", forbidden.uri())))
            .mount(&allowed)
            .await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string("internal"))
            .expect(0)
            .mount(&forbidden)
            .await;

        let err = UrlConnector
            .resolve(
                &url_location(format!("{}/start", allowed.uri())),
                None,
                &IncrementalState::default(),
                &rt_allowing(&[&allowed]),
            )
            .await
            .expect_err("the redirect target is forbidden");
        assert!(matches!(err, SourceError::Blocked(_)), "{err:?}");
        // `expect(0)` on `forbidden` is verified when it drops.
    }

    #[tokio::test]
    async fn credentials_are_not_forwarded_to_another_origin() {
        let origin = MockServer::start().await;
        let other = MockServer::start().await;
        Mock::given(method("GET"))
            .and(header("authorization", "Bearer t0ken"))
            .respond_with(redirect_to(&format!("{}/file.json", other.uri())))
            .mount(&origin)
            .await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_raw("[]", "application/json"))
            .mount(&other)
            .await;

        let auth = FetchAuth::Bearer {
            token: "t0ken".into(),
        };
        UrlConnector
            .resolve(
                &url_location(format!("{}/start", origin.uri())),
                Some(&auth),
                &IncrementalState::default(),
                &rt_allowing(&[&origin, &other]),
            )
            .await
            .expect("resolves through the redirect");

        let at_other = other.received_requests().await.expect("recorded");
        assert_eq!(at_other.len(), 1);
        assert!(
            !at_other[0].headers.contains_key("authorization"),
            "the bearer token must not follow a redirect to another origin"
        );
    }

    #[tokio::test]
    async fn a_redirect_loop_stops_at_the_cap() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(redirect_to("/again"))
            .mount(&server)
            .await;
        let err = UrlConnector
            .resolve(
                &url_location(format!("{}/again", server.uri())),
                None,
                &IncrementalState::default(),
                &rt_allowing(&[&server]),
            )
            .await
            .expect_err("loops forever otherwise");
        assert!(err.to_string().contains("more than 5 redirects"), "{err}");
        let requests = server.received_requests().await.expect("recorded").len();
        assert_eq!(
            requests, 6,
            "the first request plus five redirects, then stop"
        );
    }

    /// A server that answers any request with `chunks` one-byte pieces of body, `gap`
    /// apart. wiremock can only delay a whole response, not trickle its body.
    async fn dripping_server(chunks: usize, gap: std::time::Duration) -> String {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut seen = Vec::new();
                    let mut buf = [0u8; 1024];
                    while !seen.windows(4).any(|w| w == b"\r\n\r\n") {
                        match socket.read(&mut buf).await {
                            Ok(n) if n > 0 => seen.extend_from_slice(&buf[..n]),
                            _ => return,
                        }
                    }
                    let head = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\n\
                         content-length: {chunks}\r\nconnection: close\r\n\r\n"
                    );
                    if socket.write_all(head.as_bytes()).await.is_err() {
                        return;
                    }
                    for _ in 0..chunks {
                        tokio::time::sleep(gap).await;
                        if socket.write_all(b"x").await.is_err() {
                            return;
                        }
                        let _ = socket.flush().await;
                    }
                });
            }
        });
        format!("http://{addr}")
    }

    async fn fetch_dripping(
        chunks: usize,
        gap_ms: u64,
        limits: UrlGuard,
    ) -> (Result<Resolution, SourceError>, std::time::Duration) {
        let server = dripping_server(chunks, std::time::Duration::from_millis(gap_ms)).await;
        let mut rt = rt_without_guard(now());
        rt.limits = limits;
        let started = std::time::Instant::now();
        let got = UrlConnector
            .resolve(
                &url_location(format!("{server}/slow.txt")),
                None,
                &IncrementalState::default(),
                &rt,
            )
            .await;
        (got, started.elapsed())
    }

    #[tokio::test]
    async fn a_slow_body_that_keeps_arriving_is_read_to_the_end() {
        // 1.5 s of body, far longer than the idle timeout: only a gap counts as idle.
        let limits = UrlGuard {
            idle_timeout: std::time::Duration::from_millis(500),
            total_timeout: std::time::Duration::from_secs(30),
            ..UrlGuard::default()
        };
        let (got, _) = fetch_dripping(30, 50, limits).await;
        let Resolution::Items { items, .. } = got.expect("a progressing download completes") else {
            panic!("expected items");
        };
        assert_eq!(items[0].bytes, vec![b'x'; 30]);
    }

    #[tokio::test]
    async fn a_body_that_stalls_is_given_up_after_the_idle_timeout() {
        let limits = UrlGuard {
            idle_timeout: std::time::Duration::from_millis(200),
            total_timeout: std::time::Duration::from_secs(30),
            ..UrlGuard::default()
        };
        let (got, took) = fetch_dripping(3, 5_000, limits).await;
        let err = got.expect_err("a stalled body fails");
        assert!(
            matches!(&err, SourceError::Fetch(m) if m.contains("no data received")),
            "retryable, and says why: {err:?}"
        );
        assert!(
            took < std::time::Duration::from_secs(3),
            "long before the total deadline: {took:?}"
        );
    }

    #[tokio::test]
    async fn the_whole_fetch_has_one_deadline() {
        // Never idle, but 10 s of body against a 500 ms deadline.
        let limits = UrlGuard {
            idle_timeout: std::time::Duration::from_secs(30),
            total_timeout: std::time::Duration::from_millis(500),
            ..UrlGuard::default()
        };
        let (got, took) = fetch_dripping(200, 50, limits).await;
        let err = got.expect_err("past the deadline");
        assert!(
            matches!(&err, SourceError::Fetch(m) if m.contains("did not finish within 0.5 s")),
            "retryable, and says why: {err:?}"
        );
        assert!(took < std::time::Duration::from_secs(5), "{took:?}");
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
    fn a_single_line_document_is_not_ndjson_whatever_its_size() {
        // A minified export: one line, with or without a trailing newline.
        let mut doc = String::from("[");
        for i in 0..50_000 {
            if i > 0 {
                doc.push(',');
            }
            doc.push_str(&format!("{{\"id\":{i},\"title\":\"movie {i}\"}}"));
        }
        doc.push(']');
        assert!(!sniff_ndjson(doc.as_bytes()));
        assert!(!sniff_ndjson(format!("{doc}\n").as_bytes()));
        assert!(!sniff_ndjson(format!("\n\n{doc}\r\n\n").as_bytes()));
    }

    #[test]
    fn ndjson_with_very_long_records_is_still_ndjson() {
        // Records carrying embeddings or long text run to megabytes per line.
        let record = format!("{{\"id\":1,\"text\":\"{}\"}}", "x".repeat(2 << 20));
        let body = format!("{record}\n{record}\n");
        assert!(sniff_ndjson(body.as_bytes()));
        assert_eq!(
            detect_mime(body.as_bytes(), Some("export.json")).as_deref(),
            Some("application/x-ndjson"),
        );
    }

    #[test]
    fn ndjson_sniffing_tolerates_crlf_and_blank_lines() {
        assert!(sniff_ndjson(b"{\"a\":1}\r\n\r\n{\"a\":2}\r\n"));
        assert!(!sniff_ndjson(b"{\"a\":1}\nnot json\n"));
        assert!(
            sniff_ndjson(b"{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n{\"a\":4}\n{\"a\":5}\nnot json\n"),
            "only the first five lines are inspected"
        );
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
    fn a_gzip_bomb_is_stopped_at_the_cap_not_after_inflating() {
        use std::io::Write;
        // 1 MiB of zeros compresses to about 1 KiB.
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::best());
        enc.write_all(&vec![0u8; 1 << 20]).expect("compress");
        let bomb = enc.finish().expect("finish");
        assert!(bomb.len() < 8 * 1024, "small on the wire: {}", bomb.len());

        let err = maybe_gunzip(bomb.clone(), Some("x.json.gz".into()), 64 * 1024)
            .expect_err("inflating past the cap is refused");
        assert!(matches!(err, SourceError::TooLarge(_)), "{err}");

        let (out, name) =
            maybe_gunzip(bomb, Some("x.json.gz".into()), 1 << 20).expect("exactly at the cap");
        assert_eq!(out.len(), 1 << 20);
        assert_eq!(name.as_deref(), Some("x.json"));
    }

    #[test]
    fn maybe_gunzip_passes_through_uncompressed_bodies() {
        let (bytes, name) =
            maybe_gunzip(b"plain".to_vec(), Some("a.json".into()), 1024).expect("no gzip");
        assert_eq!(bytes, b"plain");
        assert_eq!(name.as_deref(), Some("a.json"));
    }

    #[tokio::test]
    async fn fetches_send_a_user_agent() {
        // Wikimedia and several CDNs answer 403 to a request without one.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(header("user-agent", crate::USER_AGENT))
            .respond_with(ResponseTemplate::new(200).set_body_string("ok"))
            .mount(&server)
            .await;
        let got = UrlConnector
            .resolve(
                &url_location(format!("{}/a.txt", server.uri())),
                None,
                &IncrementalState::default(),
                &rt_without_guard(now()),
            )
            .await
            .expect("the mock only answers a request carrying the user agent");
        assert!(matches!(got, Resolution::Items { .. }));
    }

    #[tokio::test]
    async fn probe_reads_the_type_from_a_head_request() {
        let server = MockServer::start().await;
        Mock::given(method("HEAD"))
            .and(path("/"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/html; charset=UTF-8"),
            )
            .expect(1)
            .mount(&server)
            .await;
        let url = url::Url::parse(&format!("{}/", server.uri())).unwrap();
        let got = probe_content_type(
            &ResolveRuntime::http_client(),
            None,
            &url,
            &UrlGuard::default(),
        )
        .await
        .unwrap();
        assert_eq!(got.as_deref(), Some("text/html"));
    }

    #[tokio::test]
    async fn probe_falls_back_to_get_when_head_is_refused() {
        let server = MockServer::start().await;
        Mock::given(method("HEAD"))
            .respond_with(ResponseTemplate::new(405))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/users"))
            .respond_with(ResponseTemplate::new(200).set_body_raw("[]", "application/json"))
            .mount(&server)
            .await;
        let url = url::Url::parse(&format!("{}/users", server.uri())).unwrap();
        let got = probe_content_type(
            &ResolveRuntime::http_client(),
            None,
            &url,
            &UrlGuard::default(),
        )
        .await
        .unwrap();
        assert_eq!(got.as_deref(), Some("application/json"));
    }

    #[tokio::test]
    async fn probe_knows_nothing_from_octet_stream_or_an_error() {
        let server = MockServer::start().await;
        Mock::given(path("/bin"))
            .respond_with(ResponseTemplate::new(200).set_body_raw("x", "application/octet-stream"))
            .mount(&server)
            .await;
        Mock::given(path("/gone"))
            .respond_with(ResponseTemplate::new(404).set_body_raw("x", "text/html"))
            .mount(&server)
            .await;
        for p in ["/bin", "/gone"] {
            let url = url::Url::parse(&format!("{}{p}", server.uri())).unwrap();
            let got = probe_content_type(
                &ResolveRuntime::http_client(),
                None,
                &url,
                &UrlGuard::default(),
            )
            .await
            .unwrap();
            assert_eq!(got, None, "{p}");
        }
    }

    #[tokio::test]
    async fn probe_checks_every_redirect_hop_against_the_policy() {
        let allowed = MockServer::start().await;
        let other = MockServer::start().await;
        Mock::given(path("/start"))
            .respond_with(
                ResponseTemplate::new(302)
                    .insert_header("location", format!("{}/elsewhere", other.uri()).as_str()),
            )
            .mount(&allowed)
            .await;
        Mock::given(path("/elsewhere"))
            .respond_with(ResponseTemplate::new(200).insert_header("content-type", "text/html"))
            .expect(0)
            .mount(&other)
            .await;
        let rt = rt_allowing(&[&allowed]);
        let url = url::Url::parse(&format!("{}/start", allowed.uri())).unwrap();
        let err = probe_content_type(&rt.http, rt.policy.as_ref(), &url, &rt.limits)
            .await
            .unwrap_err();
        assert!(matches!(err, SourceError::Blocked(_)), "{err:?}");
    }
}
