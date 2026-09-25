//! # meili-ingest blob store
//!
//! Temporal payloads are capped at 2 MiB, but uploads and step outputs can be far
//! larger. This crate wraps [`object_store`] so that:
//!
//! * the **gateway** can inline small uploads as [`PluginInput::Bytes`] and stage
//!   large ones as [`PluginInput::Ref`]`(`[`ContentRef::Staged`]`)` ([`BlobStore::stage_upload`]);
//! * the **worker** can turn every [`ContentRef`] (URL, S3-style URI, staged object)
//!   back into bytes before a plugin runs ([`BlobStore::resolve_input`]), and spill
//!   oversized outputs to the store ([`BlobStore::spill_output`]) for the next step
//!   to hydrate ([`BlobStore::hydrate_output`]).
//!
//! Plugins never see refs: the activity runner resolves them.
//!
//! URL refs are fetched under an optional [`FetchGuard`] ([`BlobStore::with_fetch_guard`]):
//! the guard's [`UrlCheck`] runs before the first request **and before every redirect
//! hop**, which the guard follows itself so an allowed host cannot bounce the fetch to
//! a loopback or metadata address. The worker always installs one (`SOURCE_FETCH_HOSTS`).
//!
//! `s3://` / `gs://` / `az://` refs are NOT guarded: they are read with the worker's
//! ambient cloud credentials, so anyone who can submit an ingest can read any object
//! those credentials reach — including other tenants' staged uploads when the blob
//! store shares the bucket. Scope the worker's credentials accordingly.
//!
//! Supported store URLs: `file://<dir>` (created when missing), `memory://`,
//! `s3://bucket/prefix`, `gs://bucket/prefix`, `az://container/prefix`. Cloud
//! credentials come from the usual provider environment variables.

use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use meili_ingest_plugin_sdk::{Blob, ContentRef, PluginInput, PluginOutput};
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use url::Url;
use uuid::Uuid;

/// MIME type of a step output that was serialized to JSON and spilled to the store.
pub const SPILLED_OUTPUT_MIME: &str = "application/vnd.meili-ingest.output+json";

/// Default store URL when `BLOB_STORE_URL` is unset.
pub const DEFAULT_STORE_URL: &str = "file://./blobs";

/// Fallback MIME type when nothing better is known.
const OCTET_STREAM: &str = "application/octet-stream";

/// Errors returned by [`BlobStore`].
#[derive(Debug, thiserror::Error)]
pub enum BlobError {
    /// The object does not exist in the store.
    #[error("object not found: {0}")]
    NotFound(String),
    /// Any other object store failure.
    #[error("object store error: {0}")]
    Store(#[from] object_store::Error),
    /// HTTP fetch failure (network error or non-2xx status).
    #[error("http error: {0}")]
    Http(String),
    /// The URL / URI could not be parsed or has an unsupported scheme.
    #[error("invalid url: {0}")]
    Url(String),
    /// JSON (de)serialization of a spilled output failed.
    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),
    /// The operation is not supported for this input.
    #[error("unsupported: {0}")]
    Unsupported(String),
    /// The [`FetchGuard`] refused a URL (the first one or a redirect target) before any
    /// request was sent to it. Permanent: retrying fetches the same forbidden address.
    #[error("blocked url: {0}")]
    Blocked(String),
}

/// Decides whether a URL may be fetched on a tenant's behalf. Implemented by the worker
/// over its `SOURCE_FETCH_HOSTS` policy; kept as a trait so this crate does not depend on
/// the policy's crate.
#[async_trait::async_trait]
pub trait UrlCheck: Send + Sync {
    /// `Err(reason)` refuses the URL.
    async fn check(&self, url: &Url) -> Result<(), String>;
}

/// Guarded fetching of [`ContentRef::Url`]: a [`UrlCheck`] plus a client that never
/// follows redirects on its own, so each hop is checked before it is requested.
#[derive(Clone)]
pub struct FetchGuard {
    check: Arc<dyn UrlCheck>,
    http: reqwest::Client,
    max_redirects: usize,
}

impl fmt::Debug for FetchGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FetchGuard")
            .field("max_redirects", &self.max_redirects)
            .finish_non_exhaustive()
    }
}

impl FetchGuard {
    /// Guard every URL fetch with `check`, following at most 5 redirects.
    pub fn new(check: Arc<dyn UrlCheck>) -> Self {
        Self {
            check,
            // Redirects are followed by `get` below, never by the client: a client that
            // followed them would fetch whatever internal address a public host
            // redirects to without the check ever seeing it.
            http: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .unwrap_or_default(),
            max_redirects: 5,
        }
    }

    /// GET `url`, checking it and every redirect target before requesting it.
    async fn get(&self, url: &Url) -> Result<reqwest::Response, BlobError> {
        let mut current = url.clone();
        let mut hops = 0usize;
        loop {
            self.check
                .check(&current)
                .await
                .map_err(BlobError::Blocked)?;
            let response = self
                .http
                .get(current.clone())
                .send()
                .await
                .map_err(|e| BlobError::Http(format!("GET {current}: {e}")))?;
            let status = response.status();
            if !status.is_redirection() {
                return Ok(response);
            }
            if hops >= self.max_redirects {
                return Err(BlobError::Http(format!(
                    "GET {url}: more than {} redirects",
                    self.max_redirects
                )));
            }
            let location = response
                .headers()
                .get(reqwest::header::LOCATION)
                .and_then(|v| v.to_str().ok())
                .ok_or_else(|| {
                    BlobError::Http(format!("GET {current}: {status} without a Location header"))
                })?;
            current = current.join(location).map_err(|e| {
                BlobError::Http(format!("GET {current}: invalid redirect {location:?}: {e}"))
            })?;
            hops += 1;
        }
    }
}

impl BlobError {
    /// Map an [`object_store::Error`], turning `NotFound` into [`BlobError::NotFound`].
    fn from_store(err: object_store::Error) -> Self {
        match err {
            object_store::Error::NotFound { path, .. } => BlobError::NotFound(path),
            other => BlobError::Store(other),
        }
    }
}

/// A handle to the configured object store plus an optional key prefix.
///
/// Cheap to clone (the underlying store is reference counted).
#[derive(Clone)]
pub struct BlobStore {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
    url: String,
    fetch_guard: Option<FetchGuard>,
}

impl fmt::Debug for BlobStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlobStore")
            .field("url", &self.url)
            .field("prefix", &self.prefix.as_ref())
            .field("fetch_guard", &self.fetch_guard)
            .finish()
    }
}

impl BlobStore {
    /// Build the store from the `BLOB_STORE_URL` environment variable
    /// (default [`DEFAULT_STORE_URL`]).
    pub fn from_env() -> anyhow::Result<Self> {
        let url = std::env::var("BLOB_STORE_URL").unwrap_or_else(|_| DEFAULT_STORE_URL.to_owned());
        Self::from_url(&url)
    }

    /// Build the store from a URL: `file://`, `memory://`, `s3://`, `gs://`, `az://`.
    ///
    /// For `file://` the directory is created when missing and used as the root of
    /// the store. For cloud schemes the path part of the URL becomes a key prefix.
    pub fn from_url(url: &str) -> anyhow::Result<Self> {
        let url = url.trim();
        if let Some(rest) = url.strip_prefix("memory://") {
            return Ok(Self {
                store: Arc::new(InMemory::new()),
                prefix: Path::from(rest.trim_matches('/')),
                url: url.to_owned(),
                fetch_guard: None,
            });
        }
        if let Some(rest) = url.strip_prefix("file://") {
            let dir = if rest.is_empty() { "." } else { rest };
            std::fs::create_dir_all(dir)
                .map_err(|e| anyhow::anyhow!("cannot create blob directory {dir:?}: {e}"))?;
            let fs = LocalFileSystem::new_with_prefix(dir)?;
            return Ok(Self {
                store: Arc::new(fs),
                prefix: Path::default(),
                url: url.to_owned(),
                fetch_guard: None,
            });
        }
        let parsed =
            Url::parse(url).map_err(|e| anyhow::anyhow!("invalid BLOB_STORE_URL {url:?}: {e}"))?;
        let (store, prefix) = object_store::parse_url(&parsed)?;
        Ok(Self {
            store: Arc::from(store),
            prefix,
            url: url.to_owned(),
            fetch_guard: None,
        })
    }

    /// Fetch every [`ContentRef::Url`] through `guard`. Without one, URL refs are fetched
    /// with the caller's client and its redirect policy, unchecked.
    pub fn with_fetch_guard(mut self, guard: FetchGuard) -> Self {
        self.fetch_guard = Some(guard);
        self
    }

    /// An in-memory store (for tests and local experiments).
    pub fn memory() -> Self {
        Self {
            store: Arc::new(InMemory::new()),
            prefix: Path::default(),
            url: "memory://".to_owned(),
            fetch_guard: None,
        }
    }

    /// The URL this store was built from.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Key of an object staged for a job: `jobs/<job_id>/<name>`.
    pub fn staged_key(job_id: Uuid, name: &str) -> String {
        format!("jobs/{job_id}/{name}")
    }

    /// Absolute store path for a key (prefix applied).
    fn full_path(&self, key: &str) -> Path {
        if self.prefix.as_ref().is_empty() {
            Path::from(key)
        } else {
            Path::from(format!("{}/{}", self.prefix.as_ref(), key))
        }
    }

    /// Write bytes at `key` (overwrites).
    pub async fn put(&self, key: &str, bytes: Bytes) -> Result<(), BlobError> {
        let path = self.full_path(key);
        self.store
            .put(&path, PutPayload::from(bytes))
            .await
            .map_err(BlobError::from_store)?;
        Ok(())
    }

    /// Read the whole object at `key`.
    pub async fn get(&self, key: &str) -> Result<Bytes, BlobError> {
        let path = self.full_path(key);
        let result = self.store.get(&path).await.map_err(BlobError::from_store)?;
        result.bytes().await.map_err(BlobError::from_store)
    }

    /// Delete the object at `key`.
    pub async fn delete(&self, key: &str) -> Result<(), BlobError> {
        let path = self.full_path(key);
        self.store
            .delete(&path)
            .await
            .map_err(BlobError::from_store)
    }

    /// Fetch the content behind a [`ContentRef`].
    ///
    /// * [`ContentRef::Url`] → HTTP GET with `http`; MIME from the hint or the
    ///   `Content-Type` header (parameters stripped); filename from the hint or the
    ///   last URL path segment.
    /// * [`ContentRef::S3`] → any URI understood by [`object_store::parse_url`]
    ///   (`s3://`, `gs://`, `az://`, ...), a fresh store per call, credentials from
    ///   the environment; MIME from the hint or guessed from the key's extension.
    /// * [`ContentRef::Staged`] → read from this store.
    pub async fn fetch_ref(
        &self,
        r: &ContentRef,
        http: &reqwest::Client,
    ) -> Result<Blob, BlobError> {
        match r {
            ContentRef::Url {
                url,
                mime,
                filename,
            } => {
                let parsed =
                    Url::parse(url).map_err(|e| BlobError::Url(format!("{url:?}: {e}")))?;
                if !matches!(parsed.scheme(), "http" | "https") {
                    return Err(BlobError::Unsupported(format!(
                        "unsupported URL scheme {:?} in {url:?}",
                        parsed.scheme()
                    )));
                }
                let response = match &self.fetch_guard {
                    Some(guard) => guard.get(&parsed).await?,
                    None => http
                        .get(parsed.clone())
                        .send()
                        .await
                        .map_err(|e| BlobError::Http(format!("GET {url}: {e}")))?,
                };
                let status = response.status();
                if !status.is_success() {
                    return Err(BlobError::Http(format!("GET {url}: status {status}")));
                }
                let header_mime = response
                    .headers()
                    .get(reqwest::header::CONTENT_TYPE)
                    .and_then(|v| v.to_str().ok())
                    .and_then(strip_mime_params);
                let data = response
                    .bytes()
                    .await
                    .map_err(|e| BlobError::Http(format!("GET {url}: reading body: {e}")))?;
                let name = filename
                    .clone()
                    .or_else(|| last_segment(parsed.path()).map(str::to_owned));
                let mime = mime
                    .clone()
                    .or(header_mime)
                    .or_else(|| name.as_deref().and_then(guess_mime))
                    .unwrap_or_else(|| OCTET_STREAM.to_owned());
                Ok(Blob::new(data.to_vec(), mime, name))
            }
            ContentRef::S3 {
                uri,
                mime,
                filename,
            } => {
                let parsed =
                    Url::parse(uri).map_err(|e| BlobError::Url(format!("{uri:?}: {e}")))?;
                let (store, path) =
                    object_store::parse_url(&parsed).map_err(BlobError::from_store)?;
                let result = store.get(&path).await.map_err(BlobError::from_store)?;
                let data = result.bytes().await.map_err(BlobError::from_store)?;
                let name = filename
                    .clone()
                    .or_else(|| last_segment(path.as_ref()).map(str::to_owned));
                let mime = mime
                    .clone()
                    .or_else(|| name.as_deref().and_then(guess_mime))
                    .unwrap_or_else(|| OCTET_STREAM.to_owned());
                Ok(Blob::new(data.to_vec(), mime, name))
            }
            ContentRef::Staged {
                uri,
                mime,
                filename,
            } => {
                let data = self.get(uri).await?;
                let name = filename
                    .clone()
                    .or_else(|| last_segment(uri).map(str::to_owned));
                let mime = mime
                    .clone()
                    .or_else(|| name.as_deref().and_then(guess_mime))
                    .unwrap_or_else(|| OCTET_STREAM.to_owned());
                Ok(Blob::new(data.to_vec(), mime, name))
            }
        }
    }

    /// Resolve every reference in a plugin input so the plugin only sees bytes.
    ///
    /// [`PluginInput::Ref`] becomes [`PluginInput::Bytes`]; [`PluginInput::Many`] is
    /// walked recursively, hydrating spilled outputs and fetching refs inside it.
    /// `Bytes`, `Documents` and `Empty` are returned unchanged.
    pub async fn resolve_input(
        &self,
        input: PluginInput,
        http: &reqwest::Client,
    ) -> Result<PluginInput, BlobError> {
        match input {
            PluginInput::Ref(r) => {
                // A staged reference may point at a *spilled step output* (JSON encoding
                // of a `PluginOutput`) rather than at raw content: hydrate it back into
                // documents instead of handing the plugin a blob of JSON bytes.
                let resolved = self.resolve_output(PluginOutput::Ref(r), http).await?;
                Ok(PluginInput::from(resolved))
            }
            PluginInput::Many(outputs) => {
                let mut resolved = Vec::with_capacity(outputs.len());
                for output in outputs {
                    resolved.push(self.resolve_output(output, http).await?);
                }
                Ok(PluginInput::Many(resolved))
            }
            other => Ok(other),
        }
    }

    /// Hydrate a spilled output and fetch any remaining refs, recursing through `Many`.
    async fn resolve_output(
        &self,
        output: PluginOutput,
        http: &reqwest::Client,
    ) -> Result<PluginOutput, BlobError> {
        match self.hydrate_output(output).await? {
            PluginOutput::Ref(r) => Ok(PluginOutput::Bytes(self.fetch_ref(&r, http).await?)),
            PluginOutput::Many(outputs) => {
                let mut resolved = Vec::with_capacity(outputs.len());
                for output in outputs {
                    resolved.push(Box::pin(self.resolve_output(output, http)).await?);
                }
                Ok(PluginOutput::Many(resolved))
            }
            other => Ok(other),
        }
    }

    /// Spill an oversized step output to the store.
    ///
    /// When `output.approx_size() > threshold`, the output is serialized to JSON and
    /// stored at `staged_key(job_id, "<step_id>[-<branch>].json")`; the returned value
    /// is a [`PluginOutput::Ref`] to a [`ContentRef::Staged`] with MIME
    /// [`SPILLED_OUTPUT_MIME`]. Otherwise the output is returned unchanged.
    pub async fn spill_output(
        &self,
        job_id: Uuid,
        step_id: &str,
        branch: Option<usize>,
        output: PluginOutput,
        threshold: usize,
    ) -> Result<PluginOutput, BlobError> {
        let size = output.approx_size();
        if size <= threshold {
            return Ok(output);
        }
        let name = match branch {
            Some(b) => format!("{step_id}-{b}.json"),
            None => format!("{step_id}.json"),
        };
        let key = Self::staged_key(job_id, &name);
        let bytes = serde_json::to_vec(&output)?;
        tracing::debug!(job_id = %job_id, step_id = %step_id, ?branch, approx_size = size, key = %key, "spilling step output to blob store");
        self.put(&key, Bytes::from(bytes)).await?;
        Ok(PluginOutput::Ref(ContentRef::Staged {
            uri: key,
            mime: Some(SPILLED_OUTPUT_MIME.to_owned()),
            filename: None,
        }))
    }

    /// Undo [`BlobStore::spill_output`]: a `Ref(Staged{ mime == SPILLED_OUTPUT_MIME })`
    /// is read back and deserialized into the original output. `Many` is walked
    /// recursively; every other variant (including non-spilled refs) is returned unchanged.
    pub async fn hydrate_output(&self, output: PluginOutput) -> Result<PluginOutput, BlobError> {
        match output {
            PluginOutput::Ref(ContentRef::Staged {
                uri,
                mime: Some(mime),
                ..
            }) if mime == SPILLED_OUTPUT_MIME => {
                let bytes = self.get(&uri).await?;
                let inner: PluginOutput = serde_json::from_slice(&bytes)?;
                Box::pin(self.hydrate_output(inner)).await
            }
            PluginOutput::Many(outputs) => {
                let mut hydrated = Vec::with_capacity(outputs.len());
                for output in outputs {
                    hydrated.push(Box::pin(self.hydrate_output(output)).await?);
                }
                Ok(PluginOutput::Many(hydrated))
            }
            other => Ok(other),
        }
    }

    /// Gateway helper: inline the upload when `blob.data.len() <= inline_max`,
    /// otherwise stage it at `staged_key(job_id, "input/<filename or upload.bin>")`
    /// and return a [`PluginInput::Ref`] carrying the blob's MIME and filename.
    pub async fn stage_upload(
        &self,
        job_id: Uuid,
        blob: Blob,
        inline_max: usize,
    ) -> Result<PluginInput, BlobError> {
        if blob.data.len() <= inline_max {
            return Ok(PluginInput::Bytes(blob));
        }
        let Blob {
            data,
            mime,
            filename,
        } = blob;
        let object_name = filename
            .as_deref()
            .and_then(last_segment)
            .unwrap_or("upload.bin");
        let key = Self::staged_key(job_id, &format!("input/{object_name}"));
        tracing::debug!(job_id = %job_id, key = %key, size = data.len(), "staging upload in blob store");
        self.put(&key, Bytes::from(data)).await?;
        Ok(PluginInput::Ref(ContentRef::Staged {
            uri: key,
            mime: Some(mime),
            filename,
        }))
    }
}

/// `type/subtype; params` → `type/subtype` (lower-cased); `None` when empty.
fn strip_mime_params(value: &str) -> Option<String> {
    let essence = value
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    if essence.is_empty() {
        None
    } else {
        Some(essence)
    }
}

/// Last non-empty `/`-separated segment of a path or key.
fn last_segment(path: &str) -> Option<&str> {
    path.rsplit('/').find(|s| !s.is_empty())
}

/// MIME guessed from a filename's extension.
fn guess_mime(name: &str) -> Option<String> {
    mime_guess::from_path(name).first_raw().map(str::to_owned)
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn resolve_input_hydrates_a_spilled_output_reference() {
        // A fan-out branch that was spilled comes back as PluginInput::Ref; resolving it
        // must yield the original documents, not the JSON bytes of the spilled file.
        let store = BlobStore::memory();
        let job = Uuid::new_v4();
        let docs = vec![
            meili_ingest_plugin_sdk::Document::with_id("a", "x".repeat(200)),
            meili_ingest_plugin_sdk::Document::with_id("b", "y".repeat(200)),
        ];
        let spilled = store
            .spill_output(
                job,
                "step",
                Some(0),
                PluginOutput::Documents(docs.clone()),
                16,
            )
            .await
            .expect("spill");
        assert!(matches!(spilled, PluginOutput::Ref(_)));
        let resolved = store
            .resolve_input(PluginInput::from(spilled), &reqwest::Client::new())
            .await
            .expect("resolve");
        assert_eq!(resolved, PluginInput::Documents(docs));
    }

    use super::*;
    use meili_ingest_plugin_sdk::Document;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// Allows exactly the listed `host:port` authorities.
    struct AllowOnly(Vec<String>);

    #[async_trait::async_trait]
    impl UrlCheck for AllowOnly {
        async fn check(&self, url: &Url) -> Result<(), String> {
            let authority = format!(
                "{}:{}",
                url.host_str().unwrap_or_default(),
                url.port_or_known_default().unwrap_or_default()
            );
            if self.0.contains(&authority) {
                Ok(())
            } else {
                Err(format!("{url} is not allowed"))
            }
        }
    }

    fn authority(server: &MockServer) -> String {
        server.uri().trim_start_matches("http://").to_string()
    }

    fn url_ref(url: String) -> ContentRef {
        ContentRef::Url {
            url,
            mime: None,
            filename: None,
        }
    }

    #[tokio::test]
    async fn guarded_fetch_refuses_a_forbidden_url_without_requesting_it() {
        let forbidden = MockServer::start().await;
        let store =
            BlobStore::memory().with_fetch_guard(FetchGuard::new(Arc::new(AllowOnly(vec![]))));
        let err = store
            .fetch_ref(
                &url_ref(format!("{}/secret", forbidden.uri())),
                &reqwest::Client::new(),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, BlobError::Blocked(_)), "{err:?}");
        assert!(forbidden.received_requests().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn guarded_fetch_checks_every_redirect_hop() {
        let allowed = MockServer::start().await;
        let forbidden = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/doc.pdf"))
            .respond_with(
                ResponseTemplate::new(302)
                    .insert_header("location", format!("{}/metrics", forbidden.uri())),
            )
            .mount(&allowed)
            .await;
        let store =
            BlobStore::memory().with_fetch_guard(FetchGuard::new(Arc::new(AllowOnly(vec![
                authority(&allowed),
            ]))));
        let err = store
            .fetch_ref(
                &url_ref(format!("{}/doc.pdf", allowed.uri())),
                &reqwest::Client::new(),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, BlobError::Blocked(_)), "{err:?}");
        assert!(
            forbidden.received_requests().await.unwrap().is_empty(),
            "the redirect target must never be requested"
        );
    }

    #[tokio::test]
    async fn guarded_fetch_follows_an_allowed_relative_redirect() {
        let allowed = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/old"))
            .respond_with(ResponseTemplate::new(301).insert_header("location", "/new.txt"))
            .mount(&allowed)
            .await;
        Mock::given(method("GET"))
            .and(path("/new.txt"))
            .respond_with(ResponseTemplate::new(200).set_body_string("hello"))
            .mount(&allowed)
            .await;
        let store =
            BlobStore::memory().with_fetch_guard(FetchGuard::new(Arc::new(AllowOnly(vec![
                authority(&allowed),
            ]))));
        let blob = store
            .fetch_ref(
                &url_ref(format!("{}/old", allowed.uri())),
                &reqwest::Client::new(),
            )
            .await
            .unwrap();
        assert_eq!(blob.data, b"hello");
        assert_eq!(blob.filename.as_deref(), Some("old"));
    }

    #[tokio::test]
    async fn guarded_fetch_caps_redirects() {
        let allowed = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(302).insert_header("location", "/loop"))
            .mount(&allowed)
            .await;
        let store =
            BlobStore::memory().with_fetch_guard(FetchGuard::new(Arc::new(AllowOnly(vec![
                authority(&allowed),
            ]))));
        let err = store
            .fetch_ref(
                &url_ref(format!("{}/loop", allowed.uri())),
                &reqwest::Client::new(),
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("more than 5 redirects"), "{err}");
    }

    fn docs(n: usize) -> Vec<Document> {
        (0..n)
            .map(|i| Document::with_id(format!("doc-{i}"), "x".repeat(200)))
            .collect()
    }

    #[test]
    fn staged_key_format() {
        let id = Uuid::nil();
        assert_eq!(
            BlobStore::staged_key(id, "input/a.pdf"),
            "jobs/00000000-0000-0000-0000-000000000000/input/a.pdf"
        );
    }

    #[tokio::test]
    async fn put_get_delete_roundtrip() {
        let store = BlobStore::memory();
        store
            .put("a/b.bin", Bytes::from_static(b"hello"))
            .await
            .unwrap();
        assert_eq!(
            store.get("a/b.bin").await.unwrap(),
            Bytes::from_static(b"hello")
        );
        store.delete("a/b.bin").await.unwrap();
        assert!(matches!(
            store.get("a/b.bin").await,
            Err(BlobError::NotFound(_))
        ));
    }

    #[tokio::test]
    async fn memory_store_with_prefix_isolates_keys() {
        let store = BlobStore::from_url("memory:///tenant-a").unwrap();
        store.put("k", Bytes::from_static(b"v")).await.unwrap();
        assert_eq!(store.get("k").await.unwrap(), Bytes::from_static(b"v"));
        assert_eq!(store.full_path("k").as_ref(), "tenant-a/k");
    }

    #[tokio::test]
    async fn file_store_creates_directory_and_roundtrips() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("nested").join("blobs");
        let store = BlobStore::from_url(&format!("file://{}", target.display())).unwrap();
        assert!(target.is_dir());
        store
            .put("jobs/x/y.bin", Bytes::from_static(b"data"))
            .await
            .unwrap();
        assert_eq!(
            store.get("jobs/x/y.bin").await.unwrap(),
            Bytes::from_static(b"data")
        );
        assert!(target.join("jobs").join("x").join("y.bin").is_file());
        store.delete("jobs/x/y.bin").await.unwrap();
        assert!(matches!(
            store.get("jobs/x/y.bin").await,
            Err(BlobError::NotFound(_))
        ));
    }

    #[tokio::test]
    async fn stage_upload_inlines_small_blobs() {
        let store = BlobStore::memory();
        let blob = Blob::new(vec![1, 2, 3], "application/pdf", Some("a.pdf".into()));
        let input = store
            .stage_upload(Uuid::new_v4(), blob.clone(), 3)
            .await
            .unwrap();
        assert_eq!(input, PluginInput::Bytes(blob));
    }

    #[tokio::test]
    async fn stage_upload_stages_large_blobs() {
        let store = BlobStore::memory();
        let job = Uuid::new_v4();
        let blob = Blob::new(vec![7u8; 100], "application/pdf", Some("big.pdf".into()));
        let input = store.stage_upload(job, blob, 99).await.unwrap();
        let expected_key = BlobStore::staged_key(job, "input/big.pdf");
        assert_eq!(
            input,
            PluginInput::Ref(ContentRef::Staged {
                uri: expected_key.clone(),
                mime: Some("application/pdf".into()),
                filename: Some("big.pdf".into()),
            })
        );
        assert_eq!(
            store.get(&expected_key).await.unwrap(),
            Bytes::from(vec![7u8; 100])
        );
    }

    #[tokio::test]
    async fn stage_upload_without_filename_uses_upload_bin() {
        let store = BlobStore::memory();
        let job = Uuid::new_v4();
        let input = store
            .stage_upload(
                job,
                Blob::new(vec![0u8; 10], "application/octet-stream", None),
                0,
            )
            .await
            .unwrap();
        match input {
            PluginInput::Ref(ContentRef::Staged { uri, filename, .. }) => {
                assert_eq!(uri, BlobStore::staged_key(job, "input/upload.bin"));
                assert_eq!(filename, None);
            }
            other => panic!("expected staged ref, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn spill_below_threshold_is_unchanged() {
        let store = BlobStore::memory();
        let output = PluginOutput::Documents(docs(2));
        let same = store
            .spill_output(Uuid::new_v4(), "extract", None, output.clone(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(same, output);
    }

    #[tokio::test]
    async fn spill_then_hydrate_returns_identical_output() {
        let store = BlobStore::memory();
        let job = Uuid::new_v4();
        let output = PluginOutput::Many(vec![
            PluginOutput::Documents(docs(5)),
            PluginOutput::Bytes(Blob::new(vec![1, 2, 3], "audio/wav", None)),
            PluginOutput::Empty,
        ]);
        let spilled = store
            .spill_output(job, "chunk", Some(3), output.clone(), 0)
            .await
            .unwrap();
        assert_eq!(
            spilled,
            PluginOutput::Ref(ContentRef::Staged {
                uri: BlobStore::staged_key(job, "chunk-3.json"),
                mime: Some(SPILLED_OUTPUT_MIME.into()),
                filename: None,
            })
        );
        let hydrated = store.hydrate_output(spilled).await.unwrap();
        assert_eq!(hydrated, output);

        // Without a branch the key has no suffix.
        let spilled = store
            .spill_output(job, "chunk", None, output.clone(), 0)
            .await
            .unwrap();
        assert_eq!(spilled.kind(), meili_ingest_plugin_sdk::OutputKind::Ref);
        assert!(
            store
                .get(&BlobStore::staged_key(job, "chunk.json"))
                .await
                .is_ok()
        );
        assert_eq!(store.hydrate_output(spilled).await.unwrap(), output);
    }

    #[tokio::test]
    async fn hydrate_leaves_non_spilled_refs_and_recurses_many() {
        let store = BlobStore::memory();
        let job = Uuid::new_v4();
        let inner = PluginOutput::Documents(docs(3));
        let spilled = store
            .spill_output(job, "s", Some(0), inner.clone(), 0)
            .await
            .unwrap();
        let raw_ref = PluginOutput::Ref(ContentRef::Staged {
            uri: "jobs/x/raw.bin".into(),
            mime: Some("application/pdf".into()),
            filename: None,
        });
        let many = PluginOutput::Many(vec![spilled, raw_ref.clone(), PluginOutput::Empty]);
        let hydrated = store.hydrate_output(many).await.unwrap();
        assert_eq!(
            hydrated,
            PluginOutput::Many(vec![inner, raw_ref, PluginOutput::Empty])
        );
    }

    #[tokio::test]
    async fn resolve_input_turns_staged_ref_into_bytes() {
        let store = BlobStore::memory();
        let http = reqwest::Client::new();
        let job = Uuid::new_v4();
        let blob = Blob::new(vec![9u8; 50], "text/plain", Some("notes.txt".into()));
        let input = store.stage_upload(job, blob.clone(), 0).await.unwrap();
        assert!(matches!(input, PluginInput::Ref(_)));
        let resolved = store.resolve_input(input, &http).await.unwrap();
        assert_eq!(resolved, PluginInput::Bytes(blob));
    }

    #[tokio::test]
    async fn resolve_input_staged_without_hints_guesses_mime_and_filename() {
        let store = BlobStore::memory();
        let http = reqwest::Client::new();
        store
            .put("jobs/j/input/report.pdf", Bytes::from_static(b"%PDF"))
            .await
            .unwrap();
        let input = PluginInput::Ref(ContentRef::Staged {
            uri: "jobs/j/input/report.pdf".into(),
            mime: None,
            filename: None,
        });
        let resolved = store.resolve_input(input, &http).await.unwrap();
        assert_eq!(
            resolved,
            PluginInput::Bytes(Blob::new(
                b"%PDF".to_vec(),
                "application/pdf",
                Some("report.pdf".into())
            ))
        );
    }

    #[tokio::test]
    async fn resolve_input_recurses_through_many() {
        let store = BlobStore::memory();
        let http = reqwest::Client::new();
        let job = Uuid::new_v4();

        let spilled_docs = PluginOutput::Documents(docs(4));
        let spilled = store
            .spill_output(job, "enrich", Some(0), spilled_docs.clone(), 0)
            .await
            .unwrap();

        let blob = Blob::new(vec![5u8; 20], "audio/wav", Some("a.wav".into()));
        let staged_key = BlobStore::staged_key(job, "enrich-1.bin");
        store
            .put(&staged_key, Bytes::from(blob.data.clone()))
            .await
            .unwrap();
        let staged_ref = PluginOutput::Ref(ContentRef::Staged {
            uri: staged_key,
            mime: Some("audio/wav".into()),
            filename: Some("a.wav".into()),
        });

        let plain = PluginOutput::Documents(docs(1));
        let nested = PluginOutput::Many(vec![staged_ref.clone()]);

        let input = PluginInput::Many(vec![
            spilled,
            staged_ref,
            plain.clone(),
            nested,
            PluginOutput::Empty,
        ]);
        let resolved = store.resolve_input(input, &http).await.unwrap();
        assert_eq!(
            resolved,
            PluginInput::Many(vec![
                spilled_docs,
                PluginOutput::Bytes(blob.clone()),
                plain,
                PluginOutput::Many(vec![PluginOutput::Bytes(blob)]),
                PluginOutput::Empty,
            ])
        );
    }

    #[tokio::test]
    async fn resolve_input_leaves_bytes_and_documents_unchanged() {
        let store = BlobStore::memory();
        let http = reqwest::Client::new();
        let bytes = PluginInput::Bytes(Blob::new(vec![1], "text/plain", None));
        assert_eq!(
            store.resolve_input(bytes.clone(), &http).await.unwrap(),
            bytes
        );
        let documents = PluginInput::Documents(docs(2));
        assert_eq!(
            store.resolve_input(documents.clone(), &http).await.unwrap(),
            documents
        );
        assert_eq!(
            store
                .resolve_input(PluginInput::Empty, &http)
                .await
                .unwrap(),
            PluginInput::Empty
        );
    }

    #[tokio::test]
    async fn fetch_ref_url_uses_content_type_and_last_segment() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/files/report.pdf"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "application/pdf; charset=binary")
                    .set_body_bytes(b"%PDF-1.4 fake".to_vec()),
            )
            .mount(&server)
            .await;

        let store = BlobStore::memory();
        let http = reqwest::Client::new();
        let r = ContentRef::Url {
            url: format!("{}/files/report.pdf?token=abc", server.uri()),
            mime: None,
            filename: None,
        };
        let blob = store.fetch_ref(&r, &http).await.unwrap();
        assert_eq!(blob.data, b"%PDF-1.4 fake".to_vec());
        assert_eq!(blob.mime, "application/pdf");
        assert_eq!(blob.filename.as_deref(), Some("report.pdf"));

        // Hints win over the response headers / URL.
        let r = ContentRef::Url {
            url: format!("{}/files/report.pdf", server.uri()),
            mime: Some("application/x-custom".into()),
            filename: Some("renamed.pdf".into()),
        };
        let blob = store.fetch_ref(&r, &http).await.unwrap();
        assert_eq!(blob.mime, "application/x-custom");
        assert_eq!(blob.filename.as_deref(), Some("renamed.pdf"));

        // Through resolve_input the ref becomes bytes.
        let input = PluginInput::Ref(ContentRef::Url {
            url: format!("{}/files/report.pdf", server.uri()),
            mime: None,
            filename: None,
        });
        let resolved = store.resolve_input(input, &http).await.unwrap();
        assert!(matches!(resolved, PluginInput::Bytes(b) if b.mime == "application/pdf"));
    }

    #[tokio::test]
    async fn fetch_ref_url_non_success_status_is_http_error() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/missing"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        let store = BlobStore::memory();
        let http = reqwest::Client::new();
        let r = ContentRef::Url {
            url: format!("{}/missing", server.uri()),
            mime: None,
            filename: None,
        };
        let err = store.fetch_ref(&r, &http).await.unwrap_err();
        assert!(matches!(err, BlobError::Http(msg) if msg.contains("404")));
    }

    #[tokio::test]
    async fn fetch_ref_url_rejects_bad_urls() {
        let store = BlobStore::memory();
        let http = reqwest::Client::new();
        let r = ContentRef::Url {
            url: "not a url".into(),
            mime: None,
            filename: None,
        };
        assert!(matches!(
            store.fetch_ref(&r, &http).await,
            Err(BlobError::Url(_))
        ));
        let r = ContentRef::Url {
            url: "ftp://example.com/x".into(),
            mime: None,
            filename: None,
        };
        assert!(matches!(
            store.fetch_ref(&r, &http).await,
            Err(BlobError::Unsupported(_))
        ));
    }

    #[tokio::test]
    async fn fetch_ref_staged_missing_is_not_found() {
        let store = BlobStore::memory();
        let http = reqwest::Client::new();
        let r = ContentRef::Staged {
            uri: "jobs/nope".into(),
            mime: None,
            filename: None,
        };
        assert!(matches!(
            store.fetch_ref(&r, &http).await,
            Err(BlobError::NotFound(_))
        ));
    }

    #[test]
    fn helpers() {
        assert_eq!(
            strip_mime_params("Text/HTML; charset=utf-8").as_deref(),
            Some("text/html")
        );
        assert_eq!(strip_mime_params("  "), None);
        assert_eq!(last_segment("/a/b/c.pdf"), Some("c.pdf"));
        assert_eq!(last_segment("/a/b/"), Some("b"));
        assert_eq!(last_segment(""), None);
        assert_eq!(guess_mime("x.csv").as_deref(), Some("text/csv"));
        assert_eq!(guess_mime("x.unknownext"), None);
    }
}
