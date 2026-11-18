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
//! Both kinds of ref are held to a size cap ([`BlobStore::with_ref_max_bytes`],
//! `REF_MAX_MB`, 512 MiB by default): the request that names one is a few bytes
//! whatever it points at, and the content is read into the worker's memory. A guarded
//! URL fetch also gives up on a server that stops sending ([`FETCH_READ_TIMEOUT`]).
//!
//! Supported store URLs: `file://<dir>` (created when missing), `memory://`,
//! `s3://bucket/prefix`, `gs://bucket/prefix`, `az://container/prefix`. Cloud stores
//! are configured from the provider's environment variables (`AWS_*`, `GOOGLE_*`,
//! `AZURE_*`): region, endpoint, and credentials from the environment, the ECS task
//! role (`AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`), web identity, or the instance
//! profile.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use meili_ingest_plugin_sdk::{Blob, ContentRef, Document, PluginInput, PluginOutput};
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

/// `User-Agent` sent when fetching a URL ref. Several hosts (Wikimedia, some CDNs)
/// answer `403` to a request that carries none, which reqwest sends by default.
pub const FETCH_USER_AGENT: &str = concat!(
    "meili-ingest/",
    env!("CARGO_PKG_VERSION"),
    " (+https://github.com/qdequele/glutony)"
);

/// How long a guarded URL fetch waits for the connection to open.
pub const FETCH_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// How long a guarded URL fetch waits for the next bytes of the response, headers
/// included, before it gives up on a server that went silent.
pub const FETCH_READ_TIMEOUT: Duration = Duration::from_secs(60);

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
    /// URL itself or on a redirect target, or a URL or object ref turned out larger than
    /// the ref size cap ([`BlobStore::with_ref_max_bytes`]). Permanent: the same request
    /// gets the same answer.
    #[error("rejected url: {0}")]
    Rejected(String),
}

/// Megabytes (MiB) a URL or object ref may weigh; larger ones fail the step.
pub const REF_MAX_MB_ENV: &str = "REF_MAX_MB";

/// Default cap on a URL or object ref: 512 MiB, the cap of a scheduled source.
pub const DEFAULT_REF_MAX_BYTES: u64 = 512 * 1024 * 1024;

/// The ref size cap from [`REF_MAX_MB_ENV`], in bytes.
pub fn ref_max_bytes_from_env() -> anyhow::Result<u64> {
    ref_max_bytes(std::env::var(REF_MAX_MB_ENV).ok().as_deref())
}

/// Parse [`REF_MAX_MB_ENV`]: a positive integer of MiB, [`DEFAULT_REF_MAX_BYTES`] when
/// unset or blank. Anything else is an error: a typo must not lift the cap.
pub fn ref_max_bytes(value: Option<&str>) -> anyhow::Result<u64> {
    let Some(value) = value.map(str::trim).filter(|v| !v.is_empty()) else {
        return Ok(DEFAULT_REF_MAX_BYTES);
    };
    value
        .parse::<u64>()
        .ok()
        .filter(|mb| *mb > 0)
        .and_then(|mb| mb.checked_mul(1024 * 1024))
        .ok_or_else(|| {
            anyhow::anyhow!("{REF_MAX_MB_ENV} must be a positive integer, got {value:?}")
        })
}

/// Schemes a [`ContentRef::S3`] ref may use: cloud object stores, each with a bucket or
/// container as host. Everything else [`object_store::parse_url`] understands is refused,
/// above all `file://` (the worker's filesystem) and `memory://`.
pub const OBJECT_REF_SCHEMES: &[&str] = &["s3", "s3a", "gs", "az", "adl", "azure", "abfs", "abfss"];

/// Parse a tenant-supplied object-store URI (`{"s3": …}` ingest bodies) and refuse any
/// scheme outside [`OBJECT_REF_SCHEMES`], one without a bucket, or an `adl://` /
/// `azure://` URI with userinfo. A refusal is
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
    // `adl://` and `azure://` name the container as the host and ignore the userinfo: one
    // that carries any reads another container than the one it appears to name.
    if matches!(parsed.scheme(), "adl" | "azure")
        && (!parsed.username().is_empty() || parsed.password().is_some())
    {
        return Err(BlobError::Blocked(format!(
            "object ref {uri:?} carries a user: {}:// names the container as the host \
             (use abfss://<container>@<account>.dfs.core.windows.net/... to name both)",
            parsed.scheme()
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
        // Same reading as object_store's Azure builder, which is what the request is
        // built from: `abfs[s]://<container>@<account>.dfs.core.windows.net/...` (and the
        // `az://` spelling of it) names the container as the user; `adl://` and
        // `azure://` ignore the userinfo and name it as the host, like every other form.
        "az" | "abfs" | "abfss" if !url.username().is_empty() => (Provider::Azure, url.username()),
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

/// The guard's client: no redirects of its own, and timeouts on connecting and on each
/// read. Not a total timeout, which would cut a large download from a slow but live
/// server; a server that goes silent is dropped after `read`.
fn guard_client(connect: Duration, read: Duration) -> reqwest::Client {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .user_agent(FETCH_USER_AGENT)
        .connect_timeout(connect)
        .read_timeout(read)
        .build()
        .unwrap_or_default()
}

impl FetchGuard {
    /// Guard every URL fetch with `check`, following at most 5 redirects.
    pub fn new(check: Arc<dyn UrlCheck>) -> Self {
        Self {
            check,
            // Redirects are followed by `get` below, never by the client: a client that
            // followed them would fetch whatever internal address a public host
            // redirects to without the check ever seeing it.
            http: guard_client(FETCH_CONNECT_TIMEOUT, FETCH_READ_TIMEOUT),
            max_redirects: 5,
        }
    }

    /// Replace the default timeouts ([`FETCH_CONNECT_TIMEOUT`], [`FETCH_READ_TIMEOUT`]).
    /// `read` bounds each wait for data, headers included, not the whole download.
    pub fn with_timeouts(mut self, connect: Duration, read: Duration) -> Self {
        self.http = guard_client(connect, read);
        self
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
    /// Largest URL or object ref [`BlobStore::fetch_ref`] reads, in bytes.
    ref_max_bytes: u64,
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
                ref_max_bytes: DEFAULT_REF_MAX_BYTES,
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
                ref_max_bytes: DEFAULT_REF_MAX_BYTES,
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
            ref_max_bytes: DEFAULT_REF_MAX_BYTES,
            own_bucket,
        })
    }

    /// Fetch every [`ContentRef::Url`] through `guard`. Without one, URL refs are fetched
    /// with the caller's client, its redirect policy and its timeouts (none by default),
    /// unchecked. The size cap applies either way.
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

    /// Refuse URL and object refs larger than `max_bytes` (default
    /// [`DEFAULT_REF_MAX_BYTES`]). Staged objects are not held to it: the gateway
    /// accepted them under its own upload limit.
    pub fn with_ref_max_bytes(mut self, max_bytes: u64) -> Self {
        self.ref_max_bytes = max_bytes;
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
            ref_max_bytes: DEFAULT_REF_MAX_BYTES,
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
    ///
    /// A URL or object ref larger than the cap ([`BlobStore::with_ref_max_bytes`]) is
    /// [`BlobError::Rejected`]: the body is streamed and dropped as soon as it passes
    /// the cap, never buffered whole. Staged objects are not capped.
    ///
    /// The blob's [`Blob::origin`] is the URL without its query string, the object
    /// URI, or the staged ref's own `origin`, so generated document ids stay distinct
    /// per origin and stable across re-ingests.
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
                        .header(reqwest::header::USER_AGENT, FETCH_USER_AGENT)
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
                let content_type = response
                    .headers()
                    .get(reqwest::header::CONTENT_TYPE)
                    .and_then(|v| v.to_str().ok());
                let header_mime = content_type.and_then(strip_mime_params);
                // Kept apart from the MIME, which routing compares as a bare essence.
                let charset = content_type.and_then(mime_charset);
                let data = read_capped_response(response, self.ref_max_bytes, url).await?;
                let name = filename
                    .clone()
                    .or_else(|| last_segment(parsed.path()).map(str::to_owned));
                let mime = mime
                    .clone()
                    .or(header_mime)
                    .or_else(|| name.as_deref().and_then(guess_mime))
                    .unwrap_or_else(|| OCTET_STREAM.to_owned());
                Ok(Blob::new(data, mime, name)
                    .with_origin(Some(url_origin(&parsed)))
                    .with_charset(charset))
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
                let data = read_capped_object(result, self.ref_max_bytes, uri).await?;
                let name = filename
                    .clone()
                    .or_else(|| last_segment(path.as_ref()).map(str::to_owned));
                let mime = mime
                    .clone()
                    .or_else(|| name.as_deref().and_then(guess_mime))
                    .unwrap_or_else(|| OCTET_STREAM.to_owned());
                Ok(Blob::new(data, mime, name).with_origin(Some(uri.clone())))
            }
            ContentRef::Staged {
                uri,
                mime,
                filename,
                origin,
                charset,
            } => {
                let data = self.get(uri).await?;
                let name = filename
                    .clone()
                    .or_else(|| last_segment(uri).map(str::to_owned));
                let mime = mime
                    .clone()
                    .or_else(|| name.as_deref().and_then(guess_mime))
                    .unwrap_or_else(|| OCTET_STREAM.to_owned());
                // Not capped: the gateway accepted it under its own upload limit.
                Ok(Blob::new(Vec::from(data), mime, name)
                    .with_origin(origin.clone())
                    .with_charset(charset.clone()))
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
    /// When the output serializes to more than `threshold` bytes of JSON (what it would
    /// weigh in the activity result, see [`PluginOutput::serialized_size_exceeds`]), that
    /// JSON is stored at `staged_key(job_id, "<step_id>[-<branch>].json")` (`/` and `%` of
    /// the step id percent-encoded, so the name is one key segment); the returned value
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
        if !output.serialized_size_exceeds(threshold) {
            return Ok(output);
        }
        // One key segment, whatever the step id: a step named `input/x` must not spill
        // into the upload namespace, which `hydrate_output` refuses to read.
        let step = step_id.replace('%', "%25").replace('/', "%2F");
        let name = match branch {
            Some(b) => format!("{step}-{b}.json"),
            None => format!("{step}.json"),
        };
        let key = Self::staged_key(job_id, &name);
        let bytes = serde_json::to_vec(&output)?;
        tracing::debug!(job_id = %job_id, step_id = %step_id, ?branch, size = bytes.len(), key = %key, "spilling step output to blob store");
        self.put(&key, Bytes::from(bytes)).await?;
        Ok(PluginOutput::Ref(ContentRef::Staged {
            uri: key,
            mime: Some(SPILLED_OUTPUT_MIME.to_owned()),
            filename: None,
            origin: None,
            charset: None,
        }))
    }

    /// Undo [`BlobStore::spill_output`]: a `Ref(Staged{ mime == SPILLED_OUTPUT_MIME })`
    /// whose key is one `spill_output` writes is read back and deserialized into the
    /// original output. `Many` is walked recursively; every other variant (including
    /// non-spilled refs) is returned unchanged.
    ///
    /// The MIME alone is not proof: what is deserialized here is trusted as a
    /// worker-written output, and its refs may name any key of the store. A staged
    /// upload (`jobs/<job>/input/...`) holds tenant bytes and is never read that way,
    /// whatever MIME its ref carries.
    pub async fn hydrate_output(&self, output: PluginOutput) -> Result<PluginOutput, BlobError> {
        match output {
            PluginOutput::Ref(ContentRef::Staged {
                uri,
                mime: Some(mime),
                ..
            }) if mime == SPILLED_OUTPUT_MIME && is_spill_key(&uri) => {
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
    /// otherwise stage it at `staged_key(job_id, "input/upload.<ext>")` (`upload.bin`
    /// without a short ASCII extension) and return a [`PluginInput::Ref`] carrying the
    /// blob's MIME and filename. The filename is never part of the key.
    pub async fn stage_upload(
        &self,
        job_id: Uuid,
        blob: Blob,
        inline_max: usize,
    ) -> Result<PluginInput, BlobError> {
        let mut blob = blob;
        // [`SPILLED_OUTPUT_MIME`] is the worker's to give. The MIME of an upload comes
        // from the tenant (a multipart part, a source's `Content-Type`), and a staged ref
        // carrying that one would be deserialized as a step output.
        if is_spilled_output_mime(&blob.mime) {
            tracing::warn!(job_id = %job_id, "upload claims the reserved spilled-output MIME; staging it as {OCTET_STREAM}");
            blob.mime = OCTET_STREAM.to_owned();
        }
        if blob.data.len() <= inline_max {
            return Ok(PluginInput::Bytes(blob));
        }
        let Blob {
            data,
            mime,
            filename,
            origin,
            charset,
        } = blob;
        let object_name = staged_object_name(filename.as_deref());
        let key = Self::staged_key(job_id, &format!("input/{object_name}"));
        tracing::debug!(job_id = %job_id, key = %key, size = data.len(), "staging upload in blob store");
        self.put(&key, Bytes::from(data)).await?;
        Ok(PluginInput::Ref(ContentRef::Staged {
            uri: key,
            mime: Some(mime),
            filename,
            origin,
            charset,
        }))
    }

    /// Gateway helper for inline documents: keep them in the workflow input when they
    /// serialize to at most `inline_max` bytes, otherwise store them at
    /// `staged_key(job_id, "gateway/documents.json")` and return a [`PluginInput::Ref`]
    /// with MIME [`SPILLED_OUTPUT_MIME`].
    ///
    /// The object is the JSON of a [`PluginOutput::Documents`], the encoding of a
    /// spilled step output, so [`BlobStore::resolve_input`] hands the first step the
    /// same documents it would have received inline, not a blob of JSON bytes.
    ///
    /// It lives under `gateway/`, which nothing else can write: a step output is one
    /// key segment (`<step>.json`, `/` percent-encoded) and an upload goes under
    /// `input/`, which [`BlobStore::hydrate_output`] refuses to read as documents
    /// because its content is the tenant's.
    pub async fn stage_documents(
        &self,
        job_id: Uuid,
        documents: Vec<Document>,
        inline_max: usize,
    ) -> Result<PluginInput, BlobError> {
        let output = PluginOutput::Documents(documents);
        // The exact size, not `approx_size`: what is kept inline goes into a Temporal
        // payload, which has a hard limit.
        let bytes = serde_json::to_vec(&output)?;
        if bytes.len() <= inline_max {
            return Ok(PluginInput::from(output));
        }
        let key = Self::staged_key(job_id, "gateway/documents.json");
        tracing::debug!(job_id = %job_id, key = %key, size = bytes.len(), "staging inline documents in blob store");
        self.put(&key, Bytes::from(bytes)).await?;
        Ok(PluginInput::Ref(ContentRef::Staged {
            uri: key,
            mime: Some(SPILLED_OUTPUT_MIME.to_owned()),
            filename: None,
            origin: None,
            charset: None,
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

/// The `charset` parameter of a `Content-Type` value, unquoted and lower-cased;
/// `None` when there is none or it is empty.
fn mime_charset(value: &str) -> Option<String> {
    value.split(';').skip(1).find_map(|param| {
        let (name, raw) = param.split_once('=')?;
        if !name.trim().eq_ignore_ascii_case("charset") {
            return None;
        }
        let label = raw.trim().trim_matches('"').trim().to_ascii_lowercase();
        (!label.is_empty()).then_some(label)
    })
}

/// The [`Blob::origin`] of a fetched URL: the URL without its query string, fragment
/// or credentials. Query strings of one-off URL ingests are mostly signatures and
/// tokens (pre-signed links) that change on every request, and re-ingesting the same
/// file must replace its documents, not duplicate them.
fn url_origin(url: &Url) -> String {
    let mut url = url.clone();
    url.set_query(None);
    url.set_fragment(None);
    // Only fails for URLs that cannot carry credentials, which then have none.
    let _ = url.set_username("");
    let _ = url.set_password(None);
    url.to_string()
}

/// Last non-empty `/`-separated segment of a path or key.
fn last_segment(path: &str) -> Option<&str> {
    path.rsplit('/').find(|s| !s.is_empty())
}

/// Whether a MIME type, whatever its case and parameters, is [`SPILLED_OUTPUT_MIME`].
fn is_spilled_output_mime(mime: &str) -> bool {
    mime.split(';')
        .next()
        .unwrap_or("")
        .trim()
        .eq_ignore_ascii_case(SPILLED_OUTPUT_MIME)
}

/// Whether `key` can be a spilled output: `jobs/<job>/<name>` outside the upload
/// namespace `jobs/<job>/input/`, where [`BlobStore::stage_upload`] writes. Empty
/// segments are skipped, as the store does when it turns a key into a path.
fn is_spill_key(key: &str) -> bool {
    let mut segments = key.split('/').filter(|s| !s.is_empty());
    segments.next() == Some("jobs")
        && segments.next().is_some()
        && segments.next().is_some_and(|name| name != "input")
}

/// The error for a ref larger than the cap. [`BlobError::Rejected`]: permanent, the
/// next attempt would download the same content.
fn over_cap(what: &str, size: Option<u64>, max_bytes: u64) -> BlobError {
    let size = match size {
        Some(size) => format!("is {size} bytes"),
        None => "is larger".to_owned(),
    };
    BlobError::Rejected(format!(
        "{what}: the content {size}, over the {max_bytes} byte cap ({REF_MAX_MB_ENV})"
    ))
}

/// Read the body of a URL fetch, refusing more than `max_bytes`: up front when the
/// server announces a larger `Content-Length`, and while streaming in any case, since a
/// chunked response announces nothing and a length can lie.
async fn read_capped_response(
    mut response: reqwest::Response,
    max_bytes: u64,
    url: &str,
) -> Result<Vec<u8>, BlobError> {
    let what = format!("GET {url}");
    let announced = response.content_length();
    if announced.is_some_and(|len| len > max_bytes) {
        return Err(over_cap(&what, announced, max_bytes));
    }
    // Reserved once when the length is known (and within the cap), so the body is not
    // copied again and again as the buffer grows.
    let mut body = Vec::with_capacity(announced.unwrap_or(0) as usize);
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|e| BlobError::Http(format!("{what}: reading body: {e}")))?
    {
        if body.len() as u64 + chunk.len() as u64 > max_bytes {
            return Err(over_cap(&what, None, max_bytes));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

/// Read an object, refusing one larger than `max_bytes` before any of it is read: the
/// store reports the size with the response headers, ahead of the body.
async fn read_capped_object(
    result: object_store::GetResult,
    max_bytes: u64,
    uri: &str,
) -> Result<Vec<u8>, BlobError> {
    let size = result.meta.size;
    if size > max_bytes {
        return Err(over_cap(uri, Some(size), max_bytes));
    }
    let data = result.bytes().await.map_err(BlobError::from_store)?;
    Ok(Vec::from(data))
}

/// Longest filename extension kept in a staged upload's key.
const STAGED_EXTENSION_MAX: usize = 16;

/// Name of the object an upload is staged under: `upload.<ext>`, or `upload.bin` when
/// the filename has no short ASCII alphanumeric extension.
///
/// The filename itself stays out of the key. object_store percent-encodes every
/// non-ASCII byte of a key segment (three characters per byte), so a moderately long
/// non-Latin name overflows the 255-byte file name limit of a `file://` store, and a
/// longer one the 1024-byte key limit of S3. A job stages one upload, so the fixed
/// name cannot collide; the extension is only there for whoever lists the store.
fn staged_object_name(filename: Option<&str>) -> String {
    let extension = filename
        .and_then(last_segment)
        .and_then(|name| name.rsplit_once('.'))
        .map(|(_, ext)| ext)
        .filter(|ext| {
            (1..=STAGED_EXTENSION_MAX).contains(&ext.len())
                && ext.bytes().all(|b| b.is_ascii_alphanumeric())
        });
    match extension {
        Some(ext) => format!("upload.{}", ext.to_ascii_lowercase()),
        None => "upload.bin".to_owned(),
    }
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
    async fn url_fetches_send_a_user_agent_guarded_or_not() {
        // Wikimedia answers 403 to a request without one: the image pipeline failed on
        // every upload.wikimedia.org URL.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(wiremock::matchers::header("user-agent", FETCH_USER_AGENT))
            .respond_with(ResponseTemplate::new(200).set_body_string("ok"))
            .mount(&server)
            .await;
        let url = format!("{}/a.png", server.uri());
        let guarded =
            BlobStore::memory().with_fetch_guard(FetchGuard::new(Arc::new(AllowOnly(vec![
                authority(&server),
            ]))));
        assert_eq!(
            guarded
                .fetch_ref(&url_ref(url.clone()), &reqwest::Client::new())
                .await
                .unwrap()
                .data,
            b"ok"
        );
        assert_eq!(
            BlobStore::memory()
                .fetch_ref(&url_ref(url), &reqwest::Client::new())
                .await
                .unwrap()
                .data,
            b"ok"
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

    /// A server that answers every connection with `response`, then holds it open
    /// without sending anything more.
    async fn raw_server(response: Vec<u8>) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let response = response.clone();
                tokio::spawn(async move {
                    let mut request = [0u8; 2048];
                    let _ = socket.read(&mut request).await;
                    let _ = socket.write_all(&response).await;
                    tokio::time::sleep(std::time::Duration::from_secs(120)).await;
                });
            }
        });
        format!("http://{addr}")
    }

    fn guarded(base: &str) -> BlobStore {
        BlobStore::memory().with_fetch_guard(FetchGuard::new(Arc::new(AllowOnly(vec![
            base.trim_start_matches("http://").to_owned(),
        ]))))
    }

    #[tokio::test]
    async fn a_url_ref_over_the_size_cap_is_rejected_guarded_or_not() {
        // A `{"url": …}` body is a few bytes whatever it points at: the whole body was
        // buffered (twice) in the worker, with no limit.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![7u8; 2000]))
            .mount(&server)
            .await;
        let r = url_ref(format!("{}/big.bin", server.uri()));
        let http = reqwest::Client::new();
        for store in [BlobStore::memory(), guarded(&server.uri())] {
            let err = store
                .clone()
                .with_ref_max_bytes(1999)
                .fetch_ref(&r, &http)
                .await
                .map(|blob| blob.data.len())
                .expect_err("over the cap");
            // Permanent: the next attempt downloads the same body.
            assert!(matches!(err, BlobError::Rejected(_)), "{err:?}");
            assert!(err.to_string().contains("1999 byte cap"), "{err}");
            // A body of exactly the cap passes.
            let blob = store
                .with_ref_max_bytes(2000)
                .fetch_ref(&r, &http)
                .await
                .unwrap();
            assert_eq!(blob.data, vec![7u8; 2000]);
        }
    }

    #[tokio::test]
    async fn a_url_ref_without_a_content_length_is_capped_while_streaming() {
        let mut response = b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n".to_vec();
        for _ in 0..64 {
            response.extend_from_slice(b"400\r\n");
            response.extend_from_slice(&[b'x'; 1024]);
            response.extend_from_slice(b"\r\n");
        }
        response.extend_from_slice(b"0\r\n\r\n");
        let base = raw_server(response).await;
        let r = url_ref(format!("{base}/stream"));
        let http = reqwest::Client::new();
        for store in [BlobStore::memory(), guarded(&base)] {
            let err = store
                .clone()
                .with_ref_max_bytes(10 * 1024)
                .fetch_ref(&r, &http)
                .await
                .map(|blob| blob.data.len())
                .expect_err("over the cap");
            assert!(matches!(err, BlobError::Rejected(_)), "{err:?}");
            let blob = store
                .with_ref_max_bytes(64 * 1024)
                .fetch_ref(&r, &http)
                .await
                .unwrap();
            assert_eq!(blob.data.len(), 64 * 1024);
        }
    }

    #[tokio::test]
    async fn a_stalled_url_fetch_times_out() {
        // No timeout at all: a server that accepted the connection and went silent held
        // the activity until its start-to-close timeout, on every attempt.
        use std::time::Duration;
        for (what, response) in [
            ("before the headers", Vec::new()),
            (
                "in the middle of the body",
                b"HTTP/1.1 200 OK\r\ncontent-length: 100\r\n\r\nabc".to_vec(),
            ),
        ] {
            let base = raw_server(response).await;
            let store = BlobStore::memory().with_fetch_guard(
                FetchGuard::new(Arc::new(AllowOnly(vec![
                    base.trim_start_matches("http://").to_owned(),
                ])))
                .with_timeouts(Duration::from_secs(5), Duration::from_millis(200)),
            );
            let r = url_ref(format!("{base}/slow"));
            let http = reqwest::Client::new();
            let fetch = store.fetch_ref(&r, &http);
            let err = tokio::time::timeout(Duration::from_secs(10), fetch)
                .await
                .unwrap_or_else(|_| panic!("stalled {what}: the fetch never gave up"))
                .expect_err("stalled");
            // Transient: the next attempt may find the server answering.
            assert!(matches!(err, BlobError::Http(_)), "{what}: {err:?}");
        }
    }

    #[tokio::test]
    async fn an_object_over_the_size_cap_is_rejected_before_it_is_read() {
        let store = InMemory::new();
        let location = Path::from("exports/big.bin");
        store
            .put(&location, PutPayload::from(vec![7u8; 2000]))
            .await
            .unwrap();
        let err = read_capped_object(
            store.get(&location).await.unwrap(),
            1999,
            "s3://exports/big.bin",
        )
        .await
        .map(|data| data.len())
        .expect_err("over the cap");
        assert!(matches!(err, BlobError::Rejected(_)), "{err:?}");
        assert!(err.to_string().contains("s3://exports/big.bin"), "{err}");
        let data = read_capped_object(store.get(&location).await.unwrap(), 2000, "s3://x/y")
            .await
            .unwrap();
        assert_eq!(data, vec![7u8; 2000]);
    }

    #[tokio::test]
    async fn staged_refs_are_not_held_to_the_ref_size_cap() {
        // The gateway accepted the upload under its own limit; the worker must read it.
        let store = BlobStore::memory().with_ref_max_bytes(10);
        let blob = Blob::new(vec![1u8; 100], "application/pdf", Some("a.pdf".into()));
        let staged = store
            .stage_upload(Uuid::new_v4(), blob.clone(), 0)
            .await
            .unwrap();
        let resolved = store
            .resolve_input(staged, &reqwest::Client::new())
            .await
            .unwrap();
        assert_eq!(resolved, PluginInput::Bytes(blob));
    }

    #[test]
    fn the_ref_size_cap_is_read_in_megabytes() {
        assert_eq!(ref_max_bytes(None).unwrap(), DEFAULT_REF_MAX_BYTES);
        assert_eq!(ref_max_bytes(Some(" ")).unwrap(), DEFAULT_REF_MAX_BYTES);
        assert_eq!(DEFAULT_REF_MAX_BYTES, 512 * 1024 * 1024);
        assert_eq!(ref_max_bytes(Some(" 2048 ")).unwrap(), 2048 * 1024 * 1024);
        for bad in ["0", "-1", "1.5", "lots", "99999999999999999999"] {
            assert!(ref_max_bytes(Some(bad)).is_err(), "{bad}");
        }
        assert_eq!(BlobStore::memory().ref_max_bytes, DEFAULT_REF_MAX_BYTES);
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
        let expected_key = BlobStore::staged_key(job, "input/upload.pdf");
        assert_eq!(
            input,
            PluginInput::Ref(ContentRef::Staged {
                uri: expected_key.clone(),
                mime: Some("application/pdf".into()),
                filename: Some("big.pdf".into()),
                origin: None,
                charset: None,
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
    async fn staged_documents_resolve_back_to_the_same_documents() {
        let store = BlobStore::memory();
        let job = Uuid::new_v4();
        let mut documents = docs(3);
        documents[0].title = Some("a title".into());
        documents[0]
            .fields
            .insert("year".into(), serde_json::json!(1999));
        let exact = serde_json::to_vec(&PluginOutput::Documents(documents.clone()))
            .unwrap()
            .len();

        // At the limit they stay inline, and nothing is written.
        let inline = store
            .stage_documents(job, documents.clone(), exact)
            .await
            .unwrap();
        assert_eq!(inline, PluginInput::Documents(documents.clone()));
        let key = BlobStore::staged_key(job, "gateway/documents.json");
        assert!(matches!(store.get(&key).await, Err(BlobError::NotFound(_))));

        // One byte over, they are staged, and the worker reads the same documents.
        let staged = store
            .stage_documents(job, documents.clone(), exact - 1)
            .await
            .unwrap();
        assert_eq!(
            staged,
            PluginInput::Ref(ContentRef::Staged {
                uri: key,
                mime: Some(SPILLED_OUTPUT_MIME.into()),
                filename: None,
                origin: None,
                charset: None,
            })
        );
        let resolved = store
            .resolve_input(staged, &reqwest::Client::new())
            .await
            .unwrap();
        assert_eq!(resolved, PluginInput::Documents(documents));
    }

    #[tokio::test]
    async fn stage_upload_takes_any_filename_on_a_file_store() {
        // object_store percent-encodes every non-ASCII byte of a key segment, so a
        // 30-character CJK name became a 270+ byte file name: ENAMETOOLONG on the
        // default `file://` store, and a 500 for the upload.
        let dir = tempfile::tempdir().unwrap();
        let store = BlobStore::from_url(&format!("file://{}", dir.path().display())).unwrap();
        let http = reqwest::Client::new();
        for name in [
            format!("{}.pdf", "日".repeat(30)),
            format!("{}.pdf", "ж".repeat(60)),
            format!("report.{}", "日".repeat(90)),
            format!("{}.pdf", "a".repeat(300)),
            "sub/dir/100% #1 ?.PDF".to_owned(),
        ] {
            let blob = Blob::new(vec![7u8; 16], "application/pdf", Some(name.clone()));
            let input = store
                .stage_upload(Uuid::new_v4(), blob.clone(), 0)
                .await
                .unwrap_or_else(|e| panic!("staging {name:?}: {e}"));
            assert!(matches!(input, PluginInput::Ref(_)), "{name}");
            // The name travels in the ref, not in the key.
            let resolved = store.resolve_input(input, &http).await.unwrap();
            assert_eq!(resolved, PluginInput::Bytes(blob), "{name}");
        }
    }

    #[test]
    fn staged_object_names_are_short_and_ascii() {
        let n = |f: &str| staged_object_name(Some(f));
        assert_eq!(n("big.pdf"), "upload.pdf");
        assert_eq!(n("a/b/Report.final.DOCX"), "upload.docx");
        assert_eq!(n("archive.tar.gz"), "upload.gz");
        assert_eq!(n("README"), "upload.bin");
        assert_eq!(n("trailing."), "upload.bin");
        assert_eq!(n("dir/"), "upload.bin");
        assert_eq!(n("x.日本"), "upload.bin");
        assert_eq!(n("x.a%b"), "upload.bin");
        assert_eq!(n(&format!("x.{}", "a".repeat(17))), "upload.bin");
        assert_eq!(staged_object_name(None), "upload.bin");
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
    async fn spill_decision_measures_the_serialized_output() {
        // What must stay under Temporal's payload limit is the JSON the activity
        // returns: metadata and escaping count, not only ids, content and fields.
        const MIB: usize = 1024 * 1024;
        let store = BlobStore::memory();
        let is_spilled = |output: PluginOutput| {
            let store = store.clone();
            async move {
                let serialized = serde_json::to_vec(&output).unwrap().len();
                let out = store
                    .spill_output(Uuid::new_v4(), "step", None, output, MIB)
                    .await
                    .unwrap();
                (matches!(out, PluginOutput::Ref(_)), serialized)
            }
        };

        // One document carrying 3 MiB of extra metadata (an external plugin's output).
        let mut heavy = Document::with_id("a", "tiny");
        heavy
            .meta
            .extra
            .insert("raw".into(), serde_json::json!("x".repeat(3 * MIB)));
        let (spilled, serialized) = is_spilled(PluginOutput::Documents(vec![heavy])).await;
        assert!(spilled, "{serialized} bytes went inline");

        // Chunker-shaped: many tiny chunks that each repeat a long path three times.
        let path = "d/".repeat(150);
        let chunks: Vec<Document> = (0..1700)
            .map(|i| {
                let mut doc = Document::with_id(format!("{path}_{i}"), "c".repeat(50));
                doc.meta.source = Some(path.clone());
                doc.meta.filename = Some(path.clone());
                doc.meta.parent_id = Some(path.clone());
                doc
            })
            .collect();
        let (spilled, serialized) = is_spilled(PluginOutput::Documents(chunks)).await;
        assert!(serialized > 2 * MIB, "{serialized}");
        assert!(spilled, "{serialized} bytes went inline");

        // Content that doubles once escaped.
        let quotes = Document::with_id("q", "\"".repeat(600 * 1024));
        let (spilled, serialized) = is_spilled(PluginOutput::Documents(vec![quotes])).await;
        assert!(spilled, "{serialized} bytes went inline");

        // And an output that does fit stays inline, however many documents it has.
        let small: Vec<Document> = (0..5000)
            .map(|i| Document::with_id(format!("d{i}"), "x"))
            .collect();
        let (spilled, serialized) = is_spilled(PluginOutput::Documents(small)).await;
        assert!(serialized < MIB, "{serialized}");
        assert!(!spilled, "{serialized} bytes were spilled");
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
                origin: None,
                charset: None,
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

    /// A staged PDF of another job, and the JSON of a `PluginOutput` pointing at it.
    async fn secret_and_forged_output(store: &BlobStore) -> Vec<u8> {
        let victim = BlobStore::staged_key(Uuid::new_v4(), "input/upload.pdf");
        store
            .put(&victim, Bytes::from_static(b"%PDF secret"))
            .await
            .unwrap();
        serde_json::to_vec(&PluginOutput::Ref(ContentRef::Staged {
            uri: victim,
            mime: Some("application/pdf".into()),
            filename: Some("report.pdf".into()),
            origin: None,
            charset: None,
        }))
        .unwrap()
    }

    #[tokio::test]
    async fn an_upload_claiming_the_spilled_output_mime_stays_bytes() {
        // The MIME of an upload is the tenant's to choose (a source's Content-Type, a
        // multipart part's). One that claimed the spilled-output type was deserialized
        // as a worker-written `PluginOutput`, whose refs name any key of the store.
        let store = BlobStore::memory();
        let http = reqwest::Client::new();
        let body = secret_and_forged_output(&store).await;
        for mime in [
            SPILLED_OUTPUT_MIME.to_owned(),
            SPILLED_OUTPUT_MIME.to_ascii_uppercase(),
            format!("{SPILLED_OUTPUT_MIME}; charset=utf-8"),
        ] {
            let blob = Blob::new(body.clone(), mime.clone(), None);
            let staged = store
                .stage_upload(Uuid::new_v4(), blob.clone(), 0)
                .await
                .unwrap();
            let PluginInput::Ref(ContentRef::Staged {
                mime: Some(staged_mime),
                ..
            }) = &staged
            else {
                panic!("expected a staged ref, got {staged:?}");
            };
            assert_eq!(staged_mime, OCTET_STREAM, "{mime}");
            let resolved = store.resolve_input(staged, &http).await.unwrap();
            assert!(
                matches!(&resolved, PluginInput::Bytes(b) if b.data == body && b.mime == OCTET_STREAM),
                "{mime}: {resolved:?}"
            );
            // Inlined uploads lose the reserved type as well.
            let inlined = store
                .stage_upload(Uuid::new_v4(), blob, usize::MAX)
                .await
                .unwrap();
            assert_eq!(
                inlined,
                PluginInput::Bytes(Blob::new(body.clone(), OCTET_STREAM, None)),
                "{mime}"
            );
        }
    }

    #[tokio::test]
    async fn the_spilled_output_mime_is_ignored_on_an_upload_key() {
        // A staged ref written before uploads lost the reserved MIME (Temporal history
        // of a workflow in flight) still carries it: the key says it is an upload.
        let store = BlobStore::memory();
        let http = reqwest::Client::new();
        let body = secret_and_forged_output(&store).await;
        let job = Uuid::new_v4();
        for key in [
            BlobStore::staged_key(job, "input/evil.json"),
            format!("jobs/{job}//input/evil.json"),
            format!("/jobs/{job}/input/nested/evil.json"),
            "elsewhere/evil.json".to_owned(),
        ] {
            store.put(&key, Bytes::from(body.clone())).await.unwrap();
            let forged = ContentRef::Staged {
                uri: key.clone(),
                mime: Some(SPILLED_OUTPUT_MIME.into()),
                filename: None,
                origin: None,
                charset: None,
            };
            assert_eq!(
                store
                    .hydrate_output(PluginOutput::Ref(forged.clone()))
                    .await
                    .unwrap(),
                PluginOutput::Ref(forged.clone()),
                "{key}"
            );
            let resolved = store
                .resolve_input(PluginInput::Ref(forged), &http)
                .await
                .unwrap();
            assert!(
                matches!(&resolved, PluginInput::Bytes(b) if b.data == body),
                "{key}: {resolved:?}"
            );
        }
    }

    #[tokio::test]
    async fn a_spilled_output_never_lands_in_the_upload_namespace() {
        // Step ids are free text: one starting with `input/` must not make its spilled
        // output look like an upload (and stop being hydrated).
        let store = BlobStore::memory();
        let job = Uuid::new_v4();
        let output = PluginOutput::Documents(docs(2));
        for (step, name) in [
            ("input/extract", "input%2Fextract.json"),
            ("a/b", "a%2Fb.json"),
            ("50%/done", "50%25%2Fdone.json"),
            ("plain", "plain.json"),
        ] {
            let spilled = store
                .spill_output(job, step, None, output.clone(), 0)
                .await
                .unwrap();
            let PluginOutput::Ref(ContentRef::Staged { uri, .. }) = &spilled else {
                panic!("expected a staged ref, got {spilled:?}");
            };
            assert_eq!(uri, &BlobStore::staged_key(job, name), "{step}");
            assert_eq!(store.hydrate_output(spilled).await.unwrap(), output);
        }
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
            origin: None,
            charset: None,
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
            origin: None,
            charset: None,
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
            origin: None,
            charset: None,
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
        // The origin is the URL without its query string: a pre-signed link fetched
        // again with a new signature still names the same file.
        let origin = format!("{}/files/report.pdf", server.uri());
        assert_eq!(blob.origin.as_deref(), Some(origin.as_str()));

        // Hints win over the response headers / URL.
        let r = ContentRef::Url {
            url: format!("{}/files/report.pdf", server.uri()),
            mime: Some("application/x-custom".into()),
            filename: Some("renamed.pdf".into()),
        };
        let blob = store.fetch_ref(&r, &http).await.unwrap();
        assert_eq!(blob.mime, "application/x-custom");
        assert_eq!(blob.filename.as_deref(), Some("renamed.pdf"));
        assert_eq!(blob.origin.as_deref(), Some(origin.as_str()));

        // Through resolve_input the ref becomes bytes.
        let input = PluginInput::Ref(ContentRef::Url {
            url: format!("{}/files/report.pdf", server.uri()),
            mime: None,
            filename: None,
        });
        let resolved = store.resolve_input(input, &http).await.unwrap();
        assert!(matches!(resolved, PluginInput::Bytes(b) if b.mime == "application/pdf"));
    }

    #[test]
    fn url_origins_drop_the_query_fragment_and_credentials_only() {
        let origin = |u: &str| url_origin(&Url::parse(u).unwrap());
        assert_eq!(
            origin("https://user:pw@A.example/export/data.json?sig=1#top"),
            "https://a.example/export/data.json"
        );
        // Same filename, different host or path: different origins.
        assert_ne!(
            origin("https://a.example/export/data.json"),
            origin("https://b.example/export/data.json")
        );
        assert_ne!(
            origin("https://a.example/v1/data.json"),
            origin("https://a.example/v2/data.json")
        );
    }

    #[tokio::test]
    async fn a_staged_blob_keeps_its_origin() {
        // A source stages what it fetched; the worker reads it back before the first
        // step, and the origin must survive the round trip for ids to be tagged.
        let store = BlobStore::memory();
        let http = reqwest::Client::new();
        let blob = Blob::new(b"[]".to_vec(), "application/json", Some("data.json".into()))
            .with_origin(Some("source:t1/products".into()));
        let staged = store.stage_upload(Uuid::new_v4(), blob, 0).await.unwrap();
        let PluginInput::Ref(r @ ContentRef::Staged { .. }) = &staged else {
            panic!("expected a staged ref, got {staged:?}");
        };
        let back = store.fetch_ref(r, &http).await.unwrap();
        assert_eq!(back.origin.as_deref(), Some("source:t1/products"));
        assert_eq!(back.filename.as_deref(), Some("data.json"));

        // An upload has none, staged or not.
        let upload = Blob::new(b"[]".to_vec(), "application/json", Some("data.json".into()));
        let staged = store.stage_upload(Uuid::new_v4(), upload, 0).await.unwrap();
        let PluginInput::Ref(r) = &staged else {
            panic!("expected a staged ref, got {staged:?}");
        };
        assert_eq!(store.fetch_ref(r, &http).await.unwrap().origin, None);
    }

    #[tokio::test]
    async fn fetch_ref_url_keeps_the_declared_charset_out_of_the_mime() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/legacy.html"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "Text/HTML; Charset=\"Windows-1252\"")
                    .set_body_bytes(b"<p>Caf\xE9</p>".to_vec()),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/plain.html"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/html")
                    .set_body_bytes(b"<p>Cafe</p>".to_vec()),
            )
            .mount(&server)
            .await;

        let store = BlobStore::memory();
        let http = reqwest::Client::new();
        let fetch = |p: &str| ContentRef::Url {
            url: format!("{}{p}", server.uri()),
            mime: None,
            filename: None,
        };
        let blob = store
            .fetch_ref(&fetch("/legacy.html"), &http)
            .await
            .unwrap();
        assert_eq!(blob.mime, "text/html", "routing sees the bare essence");
        assert_eq!(blob.charset.as_deref(), Some("windows-1252"));
        // The bytes are handed over as fetched, not decoded.
        assert_eq!(blob.data, b"<p>Caf\xE9</p>".to_vec());

        let blob = store.fetch_ref(&fetch("/plain.html"), &http).await.unwrap();
        assert_eq!(blob.charset, None);
    }

    #[tokio::test]
    async fn a_staged_blob_keeps_its_charset() {
        let store = BlobStore::memory();
        let http = reqwest::Client::new();
        let blob = Blob::new(
            b"<p>Caf\xE9</p>".to_vec(),
            "text/html",
            Some("a.html".into()),
        )
        .with_charset(Some("windows-1252".into()));
        let staged = store.stage_upload(Uuid::new_v4(), blob, 0).await.unwrap();
        let PluginInput::Ref(r @ ContentRef::Staged { .. }) = &staged else {
            panic!("expected a staged ref, got {staged:?}");
        };
        let back = store.fetch_ref(r, &http).await.unwrap();
        assert_eq!(back.charset.as_deref(), Some("windows-1252"));
        assert_eq!(back.mime, "text/html");
    }

    #[test]
    fn the_charset_is_read_from_the_content_type_parameters() {
        assert_eq!(
            mime_charset("text/html; charset=UTF-8").as_deref(),
            Some("utf-8")
        );
        assert_eq!(
            mime_charset("text/html;foo=bar; CHARSET = \"Shift_JIS\" ").as_deref(),
            Some("shift_jis")
        );
        assert_eq!(mime_charset("text/html"), None);
        assert_eq!(mime_charset("text/html; charset="), None);
        assert_eq!(mime_charset("text/html; charset=\"\""), None);
        assert_eq!(mime_charset("text/html; boundary=x"), None);
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
            origin: None,
            charset: None,
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
    fn azure_refs_name_their_container_the_way_object_store_reads_it() {
        // object_store takes the container of `adl://` and `azure://` from the host and
        // ignores the userinfo; only `az` / `abfs[s]` read it from the user.
        let b = |u: &str| bucket_of(&Url::parse(u).unwrap()).unwrap().name;
        assert_eq!(b("azure://ctr/k"), "ctr");
        assert_eq!(b("adl://ctr/k"), "ctr");
        assert_eq!(b("azure://x@ctr/k"), "ctr");
        assert_eq!(b("adl://x@ctr/k"), "ctr");
        assert_eq!(b("abfs://ctr@acct.blob.core.windows.net/k"), "ctr");
        assert_eq!(b("az://ctr@acct.dfs.fabric.microsoft.com/k"), "ctr");
    }

    #[test]
    fn azure_userinfo_cannot_relabel_the_container() {
        // The userinfo of `adl://` / `azure://` was taken as the container by the checks
        // while the read went to the host: a free label that passed for any container.
        let mut own = BlobStore::memory().with_object_ref_policy(ObjectRefPolicy {
            ambient_credentials: true,
            buckets: None,
        });
        own.own_bucket = bucket_of(&Url::parse("az://lab-blobs/glutony").unwrap());
        let listed = BlobStore::memory()
            .with_object_ref_policy(ObjectRefPolicy::parse(None, Some("exports")).unwrap());
        for scheme in ["azure", "adl"] {
            for uri in [
                format!("{scheme}://exports@lab-blobs/glutony/jobs/x/input/secret.pdf"),
                format!("{scheme}://lab-blobs/glutony/jobs/x/input/secret.pdf"),
                format!("{scheme}://:pw@lab-blobs/k"),
            ] {
                assert!(
                    matches!(own.authorize_object_ref(&uri), Err(BlobError::Blocked(_))),
                    "{uri}"
                );
            }
            let uri = format!("{scheme}://exports@private/k");
            assert!(
                matches!(
                    listed.authorize_object_ref(&uri),
                    Err(BlobError::Blocked(_))
                ),
                "{uri}"
            );
            assert!(matches!(check_object_ref(&uri), Err(BlobError::Blocked(_))));
            // Without userinfo the host is the container, checked as such.
            assert!(
                listed
                    .authorize_object_ref(&format!("{scheme}://exports/k"))
                    .is_ok()
            );
            assert!(matches!(
                listed.authorize_object_ref(&format!("{scheme}://private/k")),
                Err(BlobError::Blocked(_))
            ));
        }
        // The forms that do name the container as the user keep working.
        assert!(
            listed
                .authorize_object_ref("abfss://exports@acct.dfs.core.windows.net/k")
                .is_ok()
        );
        assert!(matches!(
            own.authorize_object_ref("abfss://lab-blobs@acct.dfs.core.windows.net/k"),
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
