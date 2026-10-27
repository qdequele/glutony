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
//! `s3://` / `gs://` / `az://` refs (`{"s3": …}` ingest bodies) are not covered by the
//! guard; an [`ObjectRefPolicy`] ([`BlobStore::with_object_ref_policy`]) governs them
//! instead. By default they are read **anonymously** (unsigned requests: public objects
//! only), never from the blob store's own bucket, and optionally only from an allowlist
//! of buckets. Signing them with the process's ambient cloud credentials is an explicit
//! opt-in (`OBJECT_REF_AMBIENT_CREDENTIALS=true`): those credentials can read the blob
//! store, where other tenants' uploads are staged. Only cloud object-store schemes are
//! accepted ([`check_object_ref`]): `file://` and `memory://` would otherwise read the
//! worker's own filesystem or process memory.
//!
//! Supported store URLs: `file://<dir>` (created when missing), `memory://`,
//! `s3://bucket/prefix`, `gs://bucket/prefix`, `az://container/prefix`. Cloud stores
//! are configured from the provider's environment variables (`AWS_*`, `GOOGLE_*`,
//! `AZURE_*`): region, endpoint, and credentials from the environment, the ECS task
//! role (`AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`), web identity, or the instance
//! profile.

use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use meili_ingest_plugin_sdk::{Blob, ContentRef, PluginInput, PluginOutput};
use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
use object_store::azure::MicrosoftAzureBuilder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, ObjectStoreScheme, PutPayload};
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
    /// HTTP fetch failure that may clear up: network error, 408, 429 or 5xx status.
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
    /// A URL fetch got a client error other than 408 / 429 (404, 403, 410, ...), on the
    /// URL itself or on a redirect target. Permanent: the same request gets the same answer.
    #[error("rejected url: {0}")]
    Rejected(String),
}

/// Schemes a [`ContentRef::S3`] ref may use: cloud object stores, each with a bucket or
/// container as host. Everything else [`object_store::parse_url`] understands is refused,
/// above all `file://` (the worker's filesystem) and `memory://`.
pub const OBJECT_REF_SCHEMES: &[&str] = &["s3", "s3a", "gs", "az", "adl", "azure", "abfs", "abfss"];

/// Parse a tenant-supplied object-store URI (`{"s3": …}` ingest bodies) and refuse any
/// scheme outside [`OBJECT_REF_SCHEMES`], or one without a bucket. A refusal is
/// [`BlobError::Blocked`]: permanent, retrying reads the same forbidden location.
pub fn check_object_ref(uri: &str) -> Result<Url, BlobError> {
    let parsed = Url::parse(uri).map_err(|e| BlobError::Url(format!("{uri:?}: {e}")))?;
    if !OBJECT_REF_SCHEMES.contains(&parsed.scheme()) {
        return Err(BlobError::Blocked(format!(
            "object ref scheme {:?} is not allowed in {uri:?} (expected one of {})",
            parsed.scheme(),
            OBJECT_REF_SCHEMES.join(", ")
        )));
    }
    if parsed.host_str().is_none_or(str::is_empty) {
        return Err(BlobError::Blocked(format!(
            "object ref {uri:?} names no bucket"
        )));
    }
    Ok(parsed)
}

/// `true` / `1` signs tenant object refs with the process's ambient cloud credentials.
pub const OBJECT_REF_AMBIENT_CREDENTIALS_ENV: &str = "OBJECT_REF_AMBIENT_CREDENTIALS";
/// Comma-separated bucket (or Azure container) names tenant object refs may name.
pub const OBJECT_REF_BUCKETS_ENV: &str = "OBJECT_REF_BUCKETS";

/// How tenant-supplied object refs ([`ContentRef::S3`]) are read.
///
/// The default is the safe one: unsigned requests (public objects only), any bucket
/// but the blob store's own. The blob store's bucket is refused whatever the policy.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ObjectRefPolicy {
    /// Sign requests with the process's ambient cloud credentials
    /// ([`OBJECT_REF_AMBIENT_CREDENTIALS_ENV`]). Off: requests are anonymous.
    pub ambient_credentials: bool,
    /// Only these buckets / containers ([`OBJECT_REF_BUCKETS_ENV`]); `None` allows any.
    pub buckets: Option<Vec<String>>,
}

impl ObjectRefPolicy {
    /// Read [`OBJECT_REF_AMBIENT_CREDENTIALS_ENV`] and [`OBJECT_REF_BUCKETS_ENV`].
    pub fn from_env() -> anyhow::Result<Self> {
        Self::parse(
            std::env::var(OBJECT_REF_AMBIENT_CREDENTIALS_ENV)
                .ok()
                .as_deref(),
            std::env::var(OBJECT_REF_BUCKETS_ENV).ok().as_deref(),
        )
    }

    /// Parse the two variables. An unrecognised flag value is an error rather than a
    /// silent default: a typo must not decide whose credentials sign tenant reads.
    pub fn parse(ambient: Option<&str>, buckets: Option<&str>) -> anyhow::Result<Self> {
        let ambient_credentials = match ambient.map(str::trim).unwrap_or("") {
            "" | "0" => false,
            "1" => true,
            v if v.eq_ignore_ascii_case("false") => false,
            v if v.eq_ignore_ascii_case("true") => true,
            other => anyhow::bail!(
                "{OBJECT_REF_AMBIENT_CREDENTIALS_ENV} must be true or false, got {other:?}"
            ),
        };
        let buckets = match buckets.map(str::trim).filter(|s| !s.is_empty()) {
            None => None,
            Some(list) => {
                let names: Vec<String> = list
                    .split(',')
                    .map(|s| s.trim().to_ascii_lowercase())
                    .filter(|s| !s.is_empty())
                    .collect();
                if let Some(bad) = names.iter().find(|n| n.contains(['/', ':', '@'])) {
                    anyhow::bail!("{OBJECT_REF_BUCKETS_ENV} lists bucket names, not URIs: {bad:?}");
                }
                Some(names)
            }
        };
        Ok(Self {
            ambient_credentials,
            buckets,
        })
    }
}

/// Cloud provider of an object-store URI, for comparing buckets across schemes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Provider {
    S3,
    Gcs,
    Azure,
}

/// The provider and bucket (Azure: container) an object-store URI names.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Bucket {
    provider: Provider,
    name: String,
}

/// The bucket of a URI in one of the [`OBJECT_REF_SCHEMES`]; `None` for anything else
/// (`file://`, `memory://`, `https://` store URLs).
fn bucket_of(url: &Url) -> Option<Bucket> {
    let host = url.host_str().filter(|h| !h.is_empty());
    let (provider, name) = match url.scheme() {
        "s3" | "s3a" => (Provider::S3, host?),
        "gs" => (Provider::Gcs, host?),
        // `abfs[s]://<container>@<account>.dfs.core.windows.net/...` names the container
        // as the user; every other form names it as the host.
        "az" | "adl" | "azure" | "abfs" | "abfss" if !url.username().is_empty() => {
            (Provider::Azure, url.username())
        }
        "az" | "adl" | "azure" | "abfs" | "abfss" => (Provider::Azure, host?),
        _ => return None,
    };
    Some(Bucket {
        provider,
        name: name.to_ascii_lowercase(),
    })
}

/// Environment snapshot the cloud builders read (`AWS_*`). A parameter rather than a
/// global read so tests can pass their own.
type Env = [(String, String)];

fn process_env() -> Vec<(String, String)> {
    std::env::vars().collect()
}

/// [`AmazonS3Builder::from_env`] over `env`: every `AWS_*` variable it understands
/// (region, endpoint, static keys, ECS container credentials, web identity, ...).
fn aws_builder(env: &Env) -> AmazonS3Builder {
    env.iter()
        .filter(|(k, _)| k.starts_with("AWS_"))
        .filter_map(|(k, v)| {
            k.to_ascii_lowercase()
                .parse::<AmazonS3ConfigKey>()
                .ok()
                .map(|key| (key, v))
        })
        .fold(AmazonS3Builder::new(), |b, (key, v)| b.with_config(key, v))
}

/// Whether `env` points S3 at a custom endpoint (MinIO, LocalStack, ...).
fn aws_custom_endpoint(env: &Env) -> bool {
    env.iter().any(|(k, v)| {
        matches!(
            k.as_str(),
            "AWS_ENDPOINT" | "AWS_ENDPOINT_URL" | "AWS_ENDPOINT_URL_S3"
        ) && !v.trim().is_empty()
    })
}

/// Whether `env` names an AWS region.
fn aws_region_set(env: &Env) -> bool {
    env.iter().any(|(k, v)| {
        matches!(k.as_str(), "AWS_REGION" | "AWS_DEFAULT_REGION") && !v.trim().is_empty()
    })
}

/// Build the cloud store behind `url` from the provider's environment variables.
///
/// Not [`object_store::parse_url`]: it starts from an empty builder, so it ignores
/// `AWS_REGION` (always `us-east-1`), custom endpoints and the ECS task role
/// (`AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`), leaving only the EC2 instance profile.
/// `anonymous` sends unsigned requests: no credential is fetched or sent.
fn cloud_store(
    url: &Url,
    env: &Env,
    anonymous: bool,
    region: Option<String>,
) -> Result<(Box<dyn ObjectStore>, Path), object_store::Error> {
    let (scheme, path) = ObjectStoreScheme::parse(url)?;
    let store: Box<dyn ObjectStore> = match scheme {
        ObjectStoreScheme::AmazonS3 => {
            let mut b = aws_builder(env).with_url(url.as_str());
            if let Some(region) = region {
                b = b.with_region(region);
            }
            if anonymous {
                b = b.with_skip_signature(true);
            }
            Box::new(b.build()?)
        }
        ObjectStoreScheme::GoogleCloudStorage => {
            let mut b = GoogleCloudStorageBuilder::from_env().with_url(url.as_str());
            if anonymous {
                b = b.with_skip_signature(true);
            }
            Box::new(b.build()?)
        }
        ObjectStoreScheme::MicrosoftAzure => {
            let mut b = MicrosoftAzureBuilder::from_env().with_url(url.as_str());
            if anonymous {
                b = b.with_skip_signature(true);
            }
            Box::new(b.build()?)
        }
        // Plain `http(s)://` stores (WebDAV, ...).
        _ => return object_store::parse_url(url),
    };
    Ok((store, path))
}

/// The store a tenant object ref is read from: signed with the ambient credentials
/// only when `ambient`.
///
/// For S3 the bucket's region is looked up first (one unsigned `HEAD` to
/// `<bucket>.s3.amazonaws.com`), so a bucket outside `AWS_REGION` answers instead of
/// redirecting. Skipped for a custom endpoint; the env region stays when it fails.
async fn tenant_store(
    url: &Url,
    bucket: &Bucket,
    ambient: bool,
    env: &Env,
) -> Result<(Box<dyn ObjectStore>, Path), BlobError> {
    let region = if bucket.provider == Provider::S3 && !aws_custom_endpoint(env) {
        match object_store::aws::resolve_bucket_region(&bucket.name, &Default::default()).await {
            Ok(region) => Some(region),
            Err(e) => {
                tracing::debug!(bucket = %bucket.name, error = %e, "cannot resolve the bucket region");
                None
            }
        }
    } else {
        None
    };
    cloud_store(url, env, !ambient, region).map_err(BlobError::from_store)
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
    object_refs: ObjectRefPolicy,
    /// The bucket this store lives in, refused to tenant object refs.
    own_bucket: Option<Bucket>,
}

impl fmt::Debug for BlobStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlobStore")
            .field("url", &self.url)
            .field("prefix", &self.prefix.as_ref())
            .field("fetch_guard", &self.fetch_guard)
            .field("object_refs", &self.object_refs)
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
                object_refs: ObjectRefPolicy::default(),
                own_bucket: None,
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
                object_refs: ObjectRefPolicy::default(),
                own_bucket: None,
            });
        }
        Self::from_cloud_url(url, &process_env())
    }

    /// A cloud store (`s3://`, `gs://`, `az://`, ...) configured from `env`.
    fn from_cloud_url(url: &str, env: &Env) -> anyhow::Result<Self> {
        let parsed =
            Url::parse(url).map_err(|e| anyhow::anyhow!("invalid BLOB_STORE_URL {url:?}: {e}"))?;
        let own_bucket = bucket_of(&parsed);
        match &own_bucket {
            Some(b)
                if b.provider == Provider::S3
                    && !aws_region_set(env)
                    && !aws_custom_endpoint(env) =>
            {
                tracing::warn!(
                    bucket = %b.name,
                    "BLOB_STORE_URL is on S3 but neither AWS_REGION nor AWS_DEFAULT_REGION is set: \
                     requests go to us-east-1 (ECS sets AWS_REGION, EC2 does not)"
                );
            }
            Some(_) => {}
            None => tracing::warn!(
                url = %url,
                "cannot tell which bucket BLOB_STORE_URL names: tenant object refs to it are not \
                 refused; use the s3://, gs:// or az:// form"
            ),
        }
        let (store, prefix) = cloud_store(&parsed, env, false, None)?;
        Ok(Self {
            store: Arc::from(store),
            prefix,
            url: url.to_owned(),
            fetch_guard: None,
            object_refs: ObjectRefPolicy::default(),
            own_bucket,
        })
    }

    /// Fetch every [`ContentRef::Url`] through `guard`. Without one, URL refs are fetched
    /// with the caller's client and its redirect policy, unchecked.
    pub fn with_fetch_guard(mut self, guard: FetchGuard) -> Self {
        self.fetch_guard = Some(guard);
        self
    }

    /// Read tenant object refs ([`ContentRef::S3`]) under `policy`. Without this, the
    /// default policy applies: anonymous reads, any bucket but this store's.
    pub fn with_object_ref_policy(mut self, policy: ObjectRefPolicy) -> Self {
        self.object_refs = policy;
        self
    }

    /// The policy tenant object refs are read under.
    pub fn object_ref_policy(&self) -> &ObjectRefPolicy {
        &self.object_refs
    }

    /// [`check_object_ref`], then refuse the blob store's own bucket and any bucket
    /// outside the policy's allowlist. Every refusal is [`BlobError::Blocked`].
    pub fn authorize_object_ref(&self, uri: &str) -> Result<Url, BlobError> {
        self.authorize(uri).map(|(url, _)| url)
    }

    fn authorize(&self, uri: &str) -> Result<(Url, Bucket), BlobError> {
        let parsed = check_object_ref(uri)?;
        let bucket = bucket_of(&parsed)
            .ok_or_else(|| BlobError::Blocked(format!("object ref {uri:?} names no bucket")))?;
        if self.own_bucket.as_ref() == Some(&bucket) {
            return Err(BlobError::Blocked(format!(
                "object ref {uri:?} names the blob store's bucket"
            )));
        }
        if let Some(allowed) = &self.object_refs.buckets
            && !allowed.contains(&bucket.name)
        {
            return Err(BlobError::Blocked(format!(
                "object ref {uri:?} names bucket {:?}, not in {OBJECT_REF_BUCKETS_ENV}",
                bucket.name
            )));
        }
        Ok((parsed, bucket))
    }

    /// An in-memory store (for tests and local experiments).
    pub fn memory() -> Self {
        Self {
            store: Arc::new(InMemory::new()),
            prefix: Path::default(),
            url: "memory://".to_owned(),
            fetch_guard: None,
            object_refs: ObjectRefPolicy::default(),
            own_bucket: None,
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
    ///   last URL path segment. A final 4xx status other than 408 / 429 is
    ///   [`BlobError::Rejected`]; other failures are [`BlobError::Http`].
    /// * [`ContentRef::S3`] → a cloud object-store URI ([`OBJECT_REF_SCHEMES`]:
    ///   `s3://`, `gs://`, `az://`, ...) allowed by [`BlobStore::authorize_object_ref`],
    ///   read with a fresh store per call, unsigned unless the [`ObjectRefPolicy`] opts
    ///   into the ambient credentials; MIME from the hint or guessed from the key's
    ///   extension. Refused refs (`file://`, the blob store's bucket, ...) are
    ///   [`BlobError::Blocked`].
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
                    let message = format!("GET {url}: status {status}");
                    // 408 and 429 are the client errors that mean "try again later".
                    let transient = !status.is_client_error()
                        || matches!(
                            status,
                            reqwest::StatusCode::REQUEST_TIMEOUT
                                | reqwest::StatusCode::TOO_MANY_REQUESTS
                        );
                    return Err(if transient {
                        BlobError::Http(message)
                    } else {
                        BlobError::Rejected(message)
                    });
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
                let (parsed, bucket) = self.authorize(uri)?;
                let (store, path) = tenant_store(
                    &parsed,
                    &bucket,
                    self.object_refs.ambient_credentials,
                    &process_env(),
                )
                .await?;
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

    /// Non-success statuses and whether retrying the fetch could ever change them.
    const STATUS_TABLE: [(u16, bool); 9] = [
        (400, true),
        (401, true),
        (403, true),
        (404, true),
        (410, true),
        (408, false),
        (429, false),
        (500, false),
        (503, false),
    ];

    fn assert_classified(err: &BlobError, status: u16, permanent: bool) {
        if permanent {
            assert!(matches!(err, BlobError::Rejected(_)), "{status}: {err:?}");
        } else {
            assert!(matches!(err, BlobError::Http(_)), "{status}: {err:?}");
        }
        assert!(err.to_string().contains(&status.to_string()), "{err}");
    }

    #[tokio::test]
    async fn a_client_error_is_permanent_and_a_server_error_is_transient() {
        for (status, permanent) in STATUS_TABLE {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .respond_with(ResponseTemplate::new(status))
                .mount(&server)
                .await;
            let url = url_ref(format!("{}/doc.pdf", server.uri()));
            let unguarded = BlobStore::memory()
                .fetch_ref(&url, &reqwest::Client::new())
                .await
                .expect_err("non-success");
            assert_classified(&unguarded, status, permanent);
            let guarded = BlobStore::memory()
                .with_fetch_guard(FetchGuard::new(Arc::new(AllowOnly(vec![authority(
                    &server,
                )]))))
                .fetch_ref(&url, &reqwest::Client::new())
                .await
                .expect_err("non-success");
            assert_classified(&guarded, status, permanent);
        }
    }

    #[tokio::test]
    async fn a_client_error_after_a_redirect_is_permanent() {
        for (status, permanent) in [(404, true), (410, true), (429, false), (502, false)] {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/old"))
                .respond_with(ResponseTemplate::new(301).insert_header("location", "/gone"))
                .mount(&server)
                .await;
            Mock::given(method("GET"))
                .and(path("/gone"))
                .respond_with(ResponseTemplate::new(status))
                .mount(&server)
                .await;
            let url = url_ref(format!("{}/old", server.uri()));
            let unguarded = BlobStore::memory()
                .fetch_ref(&url, &reqwest::Client::new())
                .await
                .expect_err("non-success");
            assert_classified(&unguarded, status, permanent);
            let guarded = BlobStore::memory()
                .with_fetch_guard(FetchGuard::new(Arc::new(AllowOnly(vec![authority(
                    &server,
                )]))))
                .fetch_ref(&url, &reqwest::Client::new())
                .await
                .expect_err("non-success");
            assert_classified(&guarded, status, permanent);
        }
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

    #[tokio::test]
    async fn fetch_ref_object_ref_refuses_the_local_filesystem() {
        // `object_store::parse_url` maps `file://` to the worker's own filesystem: an
        // `{"s3": "file:///…"}` ingest body must not read (and index) a local file.
        let dir = tempfile::tempdir().unwrap();
        let secret = dir.path().join("secret.txt");
        std::fs::write(&secret, b"do not index me").unwrap();
        let store = BlobStore::memory();
        let http = reqwest::Client::new();
        for uri in [
            format!("file://{}", secret.display()),
            "memory:///anything".to_owned(),
            "https://example.com/a.txt".to_owned(),
        ] {
            let r = ContentRef::S3 {
                uri: uri.clone(),
                mime: None,
                filename: None,
            };
            assert!(
                matches!(store.fetch_ref(&r, &http).await, Err(BlobError::Blocked(_))),
                "{uri} must be blocked"
            );
        }
    }

    #[test]
    fn check_object_ref_accepts_cloud_schemes_only() {
        for ok in [
            "s3://bucket/key.mp4",
            "gs://b/k",
            "az://container/k",
            "abfss://c/k",
        ] {
            assert!(check_object_ref(ok).is_ok(), "{ok}");
        }
        for blocked in [
            "file:///etc/passwd",
            "file:/etc/passwd",
            "memory:///k",
            "http://h/k",
        ] {
            assert!(
                matches!(check_object_ref(blocked), Err(BlobError::Blocked(_))),
                "{blocked}"
            );
        }
        assert!(matches!(
            check_object_ref("not a uri"),
            Err(BlobError::Url(_))
        ));
    }

    fn env(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    /// An S3 environment pointing at `server`, with fake static keys.
    fn s3_env(server: &MockServer) -> Vec<(String, String)> {
        env(&[
            ("AWS_ENDPOINT", &server.uri()),
            ("AWS_ALLOW_HTTP", "true"),
            ("AWS_REGION", "eu-west-3"),
            ("AWS_ACCESS_KEY_ID", "AKIDEXAMPLE"),
            ("AWS_SECRET_ACCESS_KEY", "not-a-real-secret"),
        ])
    }

    fn s3_ref(uri: &str) -> ContentRef {
        ContentRef::S3 {
            uri: uri.to_owned(),
            mime: None,
            filename: None,
        }
    }

    #[test]
    fn object_ref_policy_parsing() {
        assert_eq!(
            ObjectRefPolicy::parse(None, None).unwrap(),
            ObjectRefPolicy::default()
        );
        for on in ["true", "TRUE", "1", " true\n"] {
            assert!(
                ObjectRefPolicy::parse(Some(on), None)
                    .unwrap()
                    .ambient_credentials,
                "{on:?}"
            );
        }
        for off in ["", "false", "0", "False"] {
            assert!(
                !ObjectRefPolicy::parse(Some(off), None)
                    .unwrap()
                    .ambient_credentials,
                "{off:?}"
            );
        }
        assert!(ObjectRefPolicy::parse(Some("yes"), None).is_err());
        let p = ObjectRefPolicy::parse(None, Some(" Exports, public-data ,,")).unwrap();
        assert_eq!(
            p.buckets,
            Some(vec!["exports".to_owned(), "public-data".to_owned()])
        );
        assert_eq!(
            ObjectRefPolicy::parse(None, Some(" ")).unwrap().buckets,
            None
        );
        assert!(ObjectRefPolicy::parse(None, Some("s3://exports")).is_err());
    }

    #[test]
    fn bucket_of_reads_the_bucket_of_every_object_ref_scheme() {
        let b = |u: &str| bucket_of(&Url::parse(u).unwrap());
        let s3 = |n: &str| {
            Some(Bucket {
                provider: Provider::S3,
                name: n.into(),
            })
        };
        assert_eq!(b("s3://Blobs/glutony/x"), s3("blobs"));
        assert_eq!(b("s3a://blobs"), s3("blobs"));
        assert_eq!(b("gs://blobs/k").unwrap().provider, Provider::Gcs);
        assert_eq!(b("az://ctr/k").unwrap().name, "ctr");
        assert_eq!(
            b("abfss://ctr@acct.dfs.core.windows.net/k"),
            Some(Bucket {
                provider: Provider::Azure,
                name: "ctr".into()
            })
        );
        assert_eq!(b("file:///data/blobs"), None);
        assert_eq!(b("https://blobs.s3.eu-west-3.amazonaws.com"), None);
    }

    #[test]
    fn the_s3_builder_reads_region_and_ecs_task_credentials_from_the_env() {
        let b = aws_builder(&env(&[
            ("AWS_REGION", "eu-west-3"),
            (
                "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
                "/v2/credentials/abc",
            ),
            ("HOME", "/root"),
        ]));
        assert_eq!(
            b.get_config_value(&AmazonS3ConfigKey::Region).as_deref(),
            Some("eu-west-3")
        );
        assert_eq!(
            b.get_config_value(&AmazonS3ConfigKey::ContainerCredentialsRelativeUri)
                .as_deref(),
            Some("/v2/credentials/abc")
        );
        // Building needs no network: credentials are fetched on the first request.
        let store = BlobStore::from_cloud_url(
            "s3://lab-blobs/glutony",
            &env(&[
                ("AWS_REGION", "eu-west-3"),
                (
                    "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
                    "/v2/credentials/abc",
                ),
            ]),
        )
        .unwrap();
        assert_eq!(store.prefix.as_ref(), "glutony");
        assert_eq!(store.url(), "s3://lab-blobs/glutony");
    }

    #[tokio::test]
    async fn an_s3_blob_store_writes_signed_requests_under_its_prefix() {
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/lab-blobs/glutony/jobs/j/input/a.bin"))
            .respond_with(ResponseTemplate::new(200).insert_header("etag", "\"1\""))
            .expect(1)
            .mount(&server)
            .await;
        let store = BlobStore::from_cloud_url("s3://lab-blobs/glutony", &s3_env(&server)).unwrap();
        store
            .put("jobs/j/input/a.bin", Bytes::from_static(b"payload"))
            .await
            .expect("put");
        let requests = server.received_requests().await.unwrap();
        let auth = requests[0]
            .headers
            .get("authorization")
            .expect("signed")
            .to_str()
            .unwrap();
        assert!(auth.contains("/eu-west-3/s3/aws4_request"), "{auth}");
    }

    #[tokio::test]
    async fn tenant_object_refs_are_unsigned_unless_ambient_credentials_are_opted_in() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/tenant-data/docs/a.json"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("etag", "\"1\"")
                    .insert_header("last-modified", "Mon, 12 Oct 2026 10:00:00 GMT")
                    .set_body_bytes(b"{}".to_vec()),
            )
            .mount(&server)
            .await;
        let url = Url::parse("s3://tenant-data/docs/a.json").unwrap();
        let bucket = bucket_of(&url).unwrap();
        for ambient in [false, true] {
            let (store, path) = tenant_store(&url, &bucket, ambient, &s3_env(&server))
                .await
                .unwrap();
            let body = store.get(&path).await.unwrap().bytes().await.unwrap();
            assert_eq!(&body[..], b"{}");
        }
        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 2);
        assert!(
            requests[0].headers.get("authorization").is_none(),
            "anonymous reads must not be signed"
        );
        assert!(requests[1].headers.get("authorization").is_some());
    }

    #[tokio::test]
    async fn object_refs_to_the_blob_store_bucket_are_refused() {
        let server = MockServer::start().await;
        let store = BlobStore::from_cloud_url("s3://lab-blobs/glutony", &s3_env(&server))
            .unwrap()
            .with_object_ref_policy(ObjectRefPolicy {
                ambient_credentials: true,
                buckets: None,
            });
        let http = reqwest::Client::new();
        for uri in [
            "s3://lab-blobs/glutony/jobs/other-tenant/input/secret.pdf",
            "s3a://LAB-BLOBS/anything",
            "s3://lab-blobs",
        ] {
            assert!(
                matches!(store.authorize_object_ref(uri), Err(BlobError::Blocked(_))),
                "{uri}"
            );
            assert!(
                matches!(
                    store.fetch_ref(&s3_ref(uri), &http).await,
                    Err(BlobError::Blocked(_))
                ),
                "{uri}"
            );
        }
        // Another provider's bucket of the same name is a different bucket.
        assert!(store.authorize_object_ref("gs://lab-blobs/k").is_ok());
        assert!(store.authorize_object_ref("s3://tenant-data/k").is_ok());
        assert!(server.received_requests().await.unwrap().is_empty());
    }

    #[test]
    fn object_refs_outside_the_allowlist_are_refused() {
        let store = BlobStore::memory()
            .with_object_ref_policy(ObjectRefPolicy::parse(None, Some("exports")).unwrap());
        assert!(store.authorize_object_ref("s3://exports/a.csv").is_ok());
        assert!(store.authorize_object_ref("s3://EXPORTS/a.csv").is_ok());
        assert!(matches!(
            store.authorize_object_ref("s3://elsewhere/a.csv"),
            Err(BlobError::Blocked(_))
        ));
        assert!(matches!(
            store.authorize_object_ref("file:///etc/passwd"),
            Err(BlobError::Blocked(_))
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
