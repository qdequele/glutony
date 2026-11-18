//! Activities of the `ArchiveWorkflow`.
//!
//! [`ArchiveActivities::expand_archive`] reads the archive once, stages every file it
//! holds in the blob store and writes a manifest of them; it returns only the
//! manifest's key, so no file content ever enters workflow history.
//! [`ArchiveActivities::prepare_archive_files`] then turns a page of that manifest into
//! child `PipelineWorkflow` inputs, routing each file exactly as the gateway routes an
//! upload: by its own type and name, for the archive's tenant.
//!
//! Every file's job id is derived from the archive job id and the file's position in
//! the archive, so a retried activity overwrites what an earlier attempt staged and
//! recorded instead of leaving orphans behind.

use std::cell::Cell;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use chrono::Utc;
use meili_ingest_archive::{ArchiveError, Event, Format, Limits};
use meili_ingest_blob::{BlobError, BlobStore};
use meili_ingest_plugin_sdk::{
    ARCHIVE_ISSUES_KEPT, ArchiveFileIssue, Blob, ForcedPipeline, JobStatus, MeiliContext,
    PipelineDefinition, PipelineWorkflowInput, PluginError, PluginInput,
};
use meili_ingest_router::{canonical_mime, detect_mime, mime_to_default_index};
use serde::{Deserialize, Serialize};
use temporalio_macros::activities;
use temporalio_sdk::activities::{ActivityContext, ActivityError};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

use crate::activity::to_activity_error;
use crate::source_activity::SourceActivities;

/// How often the expansion heartbeats while it works.
const KEEPALIVE: Duration = Duration::from_secs(20);

/// Default of `ARCHIVE_INFLIGHT_MB`: the decompressed files one worker holds in memory
/// at once, between the walk and the blob store.
pub const DEFAULT_INFLIGHT_BYTES: u64 = 512 * 1024 * 1024;

/// The budget is counted in KiB, so a `u32` permit request covers any file.
const BUDGET_UNIT: u64 = 1024;

/// Bytes of decompressed files held in memory at once, across every expansion of this
/// worker. A file is reserved before it is decompressed and let go once staged.
#[derive(Debug)]
pub struct InflightBudget {
    permits: Arc<Semaphore>,
    /// Total, in [`BUDGET_UNIT`]s.
    units: u32,
    /// Bytes reserved now, and the most ever reserved at once.
    in_use: Arc<AtomicU64>,
    high_water: Arc<AtomicU64>,
}

/// Part of the [`InflightBudget`], held while one file is in memory.
#[derive(Debug)]
pub struct Reservation {
    _permit: OwnedSemaphorePermit,
    bytes: u64,
    in_use: Arc<AtomicU64>,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.in_use.fetch_sub(self.bytes, Ordering::SeqCst);
    }
}

impl InflightBudget {
    /// A budget of `bytes`.
    pub fn new(bytes: u64) -> Self {
        let units = u32::try_from(bytes.div_ceil(BUDGET_UNIT).max(1)).unwrap_or(u32::MAX);
        Self {
            permits: Arc::new(Semaphore::new(units as usize)),
            units,
            in_use: Arc::new(AtomicU64::new(0)),
            high_water: Arc::new(AtomicU64::new(0)),
        }
    }

    /// The whole budget, in bytes.
    pub fn bytes(&self) -> u64 {
        u64::from(self.units) * BUDGET_UNIT
    }

    /// The most bytes ever reserved at once.
    pub fn high_water(&self) -> u64 {
        self.high_water.load(Ordering::SeqCst)
    }

    /// Wait until `bytes` fit, then hold them. A file larger than the whole budget
    /// takes all of it, so it is always admitted once it is alone.
    pub async fn reserve(&self, bytes: u64) -> Reservation {
        let units = u32::try_from(bytes.div_ceil(BUDGET_UNIT))
            .unwrap_or(u32::MAX)
            .min(self.units);
        let permit = Arc::clone(&self.permits)
            .acquire_many_owned(units)
            .await
            .expect("the budget's semaphore is never closed");
        let bytes = u64::from(units) * BUDGET_UNIT;
        let now = self.in_use.fetch_add(bytes, Ordering::SeqCst) + bytes;
        self.high_water.fetch_max(now, Ordering::SeqCst);
        Reservation {
            _permit: permit,
            bytes,
            in_use: Arc::clone(&self.in_use),
        }
    }
}

/// Shared state of the archive activities.
pub struct ArchiveActivities {
    /// Control plane access, blob store and default index (shared with the source
    /// activities, which need the same three).
    pub base: SourceActivities,
    /// Bounds on one archive.
    pub limits: Limits,
    /// One expansion at a time on this worker: each holds its whole archive in memory
    /// while it walks it.
    expansions: Arc<Semaphore>,
    /// Decompressed files held in memory at once.
    budget: Arc<InflightBudget>,
}

impl ArchiveActivities {
    /// Activities using `base` for the control plane and the blob store, with the
    /// default in-flight budget ([`DEFAULT_INFLIGHT_BYTES`]).
    pub fn new(base: SourceActivities, limits: Limits) -> Self {
        let budget = Arc::new(InflightBudget::new(inflight_budget(
            DEFAULT_INFLIGHT_BYTES,
            &limits,
        )));
        Self {
            base,
            limits,
            expansions: Arc::new(Semaphore::new(1)),
            budget,
        }
    }

    /// Hold at most `bytes` of decompressed files in memory at once
    /// (`ARCHIVE_INFLIGHT_MB`), never less than one file of the largest size allowed.
    pub fn with_inflight_bytes(mut self, bytes: u64) -> Self {
        self.budget = Arc::new(InflightBudget::new(inflight_budget(bytes, &self.limits)));
        self
    }

    /// The in-flight budget.
    pub fn budget(&self) -> &InflightBudget {
        &self.budget
    }

    fn blob(&self) -> &BlobStore {
        &self.base.blob
    }
}

/// The in-flight budget for `bytes` asked: at least one file of
/// [`Limits::max_file_bytes`], or that file could never be admitted.
pub fn inflight_budget(bytes: u64, limits: &Limits) -> u64 {
    bytes.max(limits.max_file_bytes)
}

/// Input of [`ArchiveActivities::expand_archive`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExpandArchiveInput {
    /// The archive job.
    pub job_id: Uuid,
    /// The archive itself (a reference).
    pub input: PluginInput,
    /// The job's tenant: a tenant is not told the fetch allowlist when its URL is
    /// refused. Absent from histories recorded before it existed (then standalone).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<String>,
}

/// Output of [`ArchiveActivities::expand_archive`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExpandArchiveOutput {
    /// Blob-store key of the manifest.
    pub manifest: String,
    /// Files in the manifest.
    pub files: usize,
    /// Entries skipped while reading the archive.
    pub skipped: usize,
    /// The first [`ARCHIVE_ISSUES_KEPT`] of them.
    pub skipped_samples: Vec<ArchiveFileIssue>,
}

/// One staged file of an archive.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ManifestEntry {
    /// The job this file will run as.
    pub job_id: Uuid,
    /// Path inside the archive.
    pub path: String,
    /// Detected type.
    pub mime: String,
    /// The staged file.
    pub input: PluginInput,
}

/// Input of [`ArchiveActivities::prepare_archive_files`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PrepareArchiveInput {
    /// The archive job.
    pub job_id: Uuid,
    /// Blob-store key of the manifest.
    pub manifest: String,
    /// First manifest entry of this page.
    pub offset: usize,
    /// Most entries in this page.
    pub limit: usize,
    /// The request's tenant context.
    pub context: MeiliContext,
    /// Pipeline forced onto every file, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub forced_pipeline: Option<ForcedPipeline>,
    /// Index every file must write to, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub locked_index: Option<String>,
}

/// One file ready to run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArchiveChildJob {
    /// Path inside the archive.
    pub path: String,
    /// Everything its `PipelineWorkflow` needs.
    pub workflow_input: PipelineWorkflowInput,
}

/// Output of [`ArchiveActivities::prepare_archive_files`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PrepareArchiveOutput {
    /// Manifest entries this page covered.
    pub consumed: usize,
    /// Files to start.
    pub jobs: Vec<ArchiveChildJob>,
    /// Files no pipeline handles.
    pub skipped: Vec<ArchiveFileIssue>,
    /// Files that cannot run as asked (no destination, wrong index).
    pub failed: Vec<ArchiveFileIssue>,
}

/// Input of [`ArchiveActivities::record_archive_finished`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArchiveFinishedInput {
    /// The archive job.
    pub job_id: Uuid,
    /// Its terminal status.
    pub status: JobStatus,
    /// Why it failed.
    pub error: Option<String>,
}

/// Body of `POST /internal/resolve`.
#[derive(Debug, Serialize)]
struct ResolveRequest<'a> {
    mime: &'a str,
    filename: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    tenant_id: Option<&'a str>,
}

/// Response of `POST /internal/resolve`.
#[derive(Debug, Deserialize)]
struct ResolveResponse {
    pipeline: PipelineDefinition,
    #[serde(default)]
    index_pattern: Option<String>,
}

/// An archive [`Event`] as it crosses from the blocking walker to the async activity:
/// a file already carries its detected MIME.
#[derive(Debug, PartialEq)]
enum Walked {
    /// A file to ingest, with its MIME.
    File {
        path: String,
        data: Vec<u8>,
        mime: String,
    },
    /// An entry that will not be ingested, and why.
    Skipped { path: String, reason: String },
}

impl Walked {
    /// Detect the MIME of a walked file. Call it on the walker's thread.
    fn detect(event: Event) -> Self {
        match event {
            Event::File { path, data } => {
                let mime = detect_mime(&data, Some(&path), None);
                Self::File { path, data, mime }
            }
            Event::Skipped { path, reason } => Self::Skipped { path, reason },
        }
    }
}

/// The job id of the `ordinal`-th file of archive `job_id`: stable across retries.
pub fn file_job_id(job_id: Uuid, ordinal: usize) -> Uuid {
    Uuid::new_v5(&job_id, format!("archive-file-{ordinal}").as_bytes())
}

/// Blob-store key of an archive's manifest.
pub fn manifest_key(job_id: Uuid) -> String {
    BlobStore::staged_key(job_id, "archive/manifest.json")
}

fn basename(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

fn issue(path: &str, reason: impl Into<String>, job_id: Option<Uuid>) -> ArchiveFileIssue {
    ArchiveFileIssue {
        path: path.to_string(),
        reason: reason.into(),
        job_id,
    }
}

/// What retrying can and cannot fix when reading the archive.
fn from_archive_error(e: ArchiveError) -> PluginError {
    match e {
        // Only raised when this activity stopped listening, i.e. on its own error path.
        ArchiveError::Stopped(m) => PluginError::Retryable(m),
        other => PluginError::NonRetryable(other.to_string()),
    }
}

fn from_blob_error(what: &str, e: BlobError) -> PluginError {
    match e {
        BlobError::Blocked(_) | BlobError::Rejected(_) => {
            PluginError::NonRetryable(format!("{what}: {e}"))
        }
        other => PluginError::Retryable(format!("{what}: {other}")),
    }
}

/// The index a file writes to, in the gateway's order: the route's index, the
/// pipeline's own fixed index, the trigger's `index_pattern`, the request's index, then
/// the type's default (`documents` meaning the deployment default).
pub fn file_index(
    locked: Option<&str>,
    pipeline: &PipelineDefinition,
    pattern: Option<&str>,
    request_index: Option<&str>,
    mime: &str,
    default_index: &str,
) -> String {
    fn non_blank(s: Option<&str>) -> Option<&str> {
        s.map(str::trim).filter(|s| !s.is_empty())
    }
    if let Some(i) = non_blank(locked).or(pipeline.fixed_index()) {
        return i.to_string();
    }
    if let Some(i) = non_blank(pattern).or(non_blank(request_index)) {
        return i.to_string();
    }
    match mime_to_default_index(mime) {
        "documents" => default_index.to_string(),
        other => other.to_string(),
    }
}

impl ArchiveActivities {
    /// Read, stage and list every file of the archive.
    pub async fn expand(
        &self,
        input: &ExpandArchiveInput,
    ) -> Result<ExpandArchiveOutput, PluginError> {
        // Taken before the archive is read into memory: expansions waiting their turn
        // hold nothing.
        let expansion = Arc::clone(&self.expansions)
            .acquire_owned()
            .await
            .expect("the expansion semaphore is never closed");
        let blob = match &input.input {
            // A tenant's archive is not told the fetch allowlist; a standalone one is.
            PluginInput::Ref(r) => self
                .blob()
                .with_guard_disclosure(input.tenant_id.is_none())
                .fetch_ref(r, &self.base.http)
                .await
                .map_err(|e| from_blob_error("fetching the archive", e))?,
            PluginInput::Bytes(b) => b.clone(),
            other => {
                return Err(PluginError::NonRetryable(format!(
                    "an archive job needs a file, got {:?} input",
                    other.kind()
                )));
            }
        };
        // Magic bytes first: a URL can be named `.zip` and serve a tarball.
        let format = Format::sniff(&blob.data)
            .or_else(|| Format::from_mime(canonical_mime(&blob.mime)))
            .ok_or_else(|| {
                PluginError::NonRetryable(format!(
                    "{} is not a zip, tar or gzip file",
                    blob.filename.as_deref().unwrap_or("the upload")
                ))
            })?;

        let Blob {
            data,
            filename,
            origin,
            ..
        } = blob;
        let limits = self.limits.clone();
        // What bounds memory is the budget, not the channel: each file is reserved
        // before it is decompressed and let go once staged.
        let (tx, mut rx) = tokio::sync::mpsc::channel::<(Walked, Option<Reservation>)>(4);
        let budget = Arc::clone(&self.budget);
        let handle = tokio::runtime::Handle::current();
        let walker = tokio::task::spawn_blocking(move || {
            // The walk holds the archive: the next expansion starts once it is done.
            let _expansion = expansion;
            let abandoned = || "the archive expansion was abandoned".to_string();
            // The reservation of the file being decompressed, until its event.
            let pending: Cell<Option<Reservation>> = Cell::new(None);
            meili_ingest_archive::walk_reserving(
                &data,
                format,
                filename.as_deref(),
                &limits,
                &mut |bytes| {
                    // An earlier reservation not used by a file (a nested archive
                    // being opened) is let go first: it could block this one.
                    pending.set(None);
                    // Waiting for room must not outlive the expansion: once it gives
                    // up (an error, a cancellation), the receiver is dropped.
                    let reserved = handle.block_on(async {
                        tokio::select! {
                            r = budget.reserve(bytes) => Some(r),
                            () = tx.closed() => None,
                        }
                    });
                    pending.set(Some(reserved.ok_or_else(abandoned)?));
                    Ok(())
                },
                &mut |e| {
                    // A skipped entry frees its reservation right here.
                    let reserved = match &e {
                        Event::File { .. } => pending.take(),
                        Event::Skipped { .. } => {
                            pending.set(None);
                            None
                        }
                    };
                    // Detection can read a whole file (a JSON sniff parses it), so it
                    // runs here, on the blocking thread that walks the archive, and
                    // never on the async threads that receive the files.
                    tx.blocking_send((Walked::detect(e), reserved))
                        .map_err(|_| abandoned())
                },
            )
        });

        let mut entries: Vec<ManifestEntry> = Vec::new();
        let mut skipped = 0usize;
        let mut skipped_samples = Vec::new();
        // The reservation lives to the end of the iteration: the file is staged.
        while let Some((event, _reservation)) = rx.recv().await {
            match event {
                Walked::File { path, data, mime } => {
                    let job_id = file_job_id(input.job_id, entries.len());
                    // Always staged: bytes never enter workflow history. The full path
                    // is the filename, so documents keep their folder and two `a.pdf`
                    // in different folders get different ids.
                    let staged = self
                        .blob()
                        .stage_upload(
                            job_id,
                            // A fetched archive's files keep its origin: their ids are
                            // tagged with it like the archive's own would be.
                            Blob::new(data, mime.clone(), Some(path.clone()))
                                .with_origin(origin.clone()),
                            0,
                        )
                        .await
                        .map_err(|e| from_blob_error("staging a file of the archive", e))?;
                    entries.push(ManifestEntry {
                        job_id,
                        path,
                        mime,
                        input: staged,
                    });
                }
                Walked::Skipped { path, reason } => {
                    skipped += 1;
                    if skipped_samples.len() < ARCHIVE_ISSUES_KEPT {
                        skipped_samples.push(issue(&path, reason, None));
                    }
                }
            }
        }
        let summary = walker
            .await
            .map_err(|e| PluginError::Retryable(format!("archive expansion crashed: {e}")))?
            .map_err(from_archive_error)?;

        let manifest = manifest_key(input.job_id);
        let bytes = serde_json::to_vec(&entries)
            .map_err(|e| PluginError::NonRetryable(format!("archive manifest: {e}")))?;
        self.blob()
            .put(&manifest, bytes.into())
            .await
            .map_err(|e| from_blob_error("writing the archive manifest", e))?;
        tracing::info!(
            job_id = %input.job_id,
            files = entries.len(),
            skipped,
            decompressed_bytes = summary.decompressed_bytes,
            "archive expanded"
        );
        Ok(ExpandArchiveOutput {
            manifest,
            files: entries.len(),
            skipped,
            skipped_samples,
        })
    }

    /// [`Self::expand`], given up as soon as `cancelled` resolves.
    ///
    /// Giving up drops the expansion, which closes the channel its walk feeds: the walk
    /// stops at its next entry instead of staging the rest of an archive nobody wants
    /// any more. What was staged until then stays under the job's keys, as after any
    /// failed expansion.
    pub async fn expand_unless(
        &self,
        input: &ExpandArchiveInput,
        cancelled: impl Future<Output = ()>,
    ) -> Result<ExpandArchiveOutput, PluginError> {
        tokio::select! {
            biased;
            () = cancelled => Err(PluginError::Cancelled),
            expanded = self.expand(input) => expanded,
        }
    }

    /// Route one page of the manifest into child jobs.
    pub async fn prepare(
        &self,
        input: &PrepareArchiveInput,
    ) -> Result<PrepareArchiveOutput, PluginError> {
        let bytes = self
            .blob()
            .get(&input.manifest)
            .await
            .map_err(|e| from_blob_error("reading the archive manifest", e))?;
        let entries: Vec<ManifestEntry> = serde_json::from_slice(&bytes)
            .map_err(|e| PluginError::NonRetryable(format!("archive manifest: {e}")))?;
        let page: Vec<&ManifestEntry> = entries
            .iter()
            .skip(input.offset)
            .take(input.limit.max(1))
            .collect();

        let mut out = PrepareArchiveOutput {
            consumed: page.len(),
            jobs: Vec::with_capacity(page.len()),
            skipped: Vec::new(),
            failed: Vec::new(),
        };
        let has_destination = input.context.host.is_some() && input.context.api_key.is_some();
        for entry in page {
            let (pipeline, pattern) = match &input.forced_pipeline {
                Some(f) => (f.pipeline.clone(), f.index_pattern.clone()),
                None => match self
                    .resolve(
                        &entry.mime,
                        basename(&entry.path),
                        input.context.tenant_id.as_deref(),
                    )
                    .await?
                {
                    Some(r) => (r.pipeline, r.index_pattern),
                    None => {
                        out.skipped.push(issue(
                            &entry.path,
                            format!("no pipeline handles {}", entry.mime),
                            None,
                        ));
                        continue;
                    }
                },
            };
            if !pipeline.pins_destination() && !has_destination {
                out.failed.push(issue(
                    &entry.path,
                    format!(
                        "pipeline {:?} needs a Meilisearch destination and the request \
                         named none",
                        pipeline.uid
                    ),
                    None,
                ));
                continue;
            }
            let locked = input.locked_index.as_deref();
            if let Some(index) = locked
                && let Some(other) = pipeline.step_indexes().find(|named| *named != index)
            {
                out.failed.push(issue(
                    &entry.path,
                    format!(
                        "pipeline {:?} writes to index {other:?}, not {index:?}",
                        pipeline.uid
                    ),
                    None,
                ));
                continue;
            }
            let mut context = input.context.clone();
            // A file whose pipeline writes through connections never reads the
            // request's destination: keep the caller's key out of its history.
            if pipeline.pins_destination() {
                context.host = None;
                context.api_key = None;
            }
            context.index = Some(file_index(
                locked,
                &pipeline,
                pattern.as_deref(),
                input.context.index.as_deref(),
                &entry.mime,
                &self.base.default_index,
            ));
            self.record_file_job(input.job_id, entry.job_id, &pipeline, &context)
                .await;
            out.jobs.push(ArchiveChildJob {
                path: entry.path.clone(),
                workflow_input: PipelineWorkflowInput {
                    job_id: entry.job_id,
                    pipeline,
                    input: entry.input.clone(),
                    context,
                },
            });
        }
        Ok(out)
    }

    /// Ask the control plane which pipeline handles a file. `None` when none does.
    async fn resolve(
        &self,
        mime: &str,
        filename: &str,
        tenant_id: Option<&str>,
    ) -> Result<Option<ResolveResponse>, PluginError> {
        let url = self.base.url(&["internal", "resolve"])?;
        let body = ResolveRequest {
            mime,
            filename,
            tenant_id,
        };
        let resp = crate::connection::authed(
            self.base.http.post(url).json(&body),
            self.base.control_plane_token.as_deref(),
        )
        .send()
        .await
        .map_err(|e| PluginError::Retryable(format!("control plane unreachable: {e}")))?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !resp.status().is_success() {
            return Err(PluginError::Retryable(format!(
                "control plane returned {} resolving a pipeline for {mime}",
                resp.status()
            )));
        }
        resp.json::<ResolveResponse>()
            .await
            .map(Some)
            .map_err(|e| PluginError::NonRetryable(format!("resolve: unreadable response: {e}")))
    }

    /// Create the file job's row, linked to its archive. Best effort, like every job
    /// row: Temporal is the source of truth.
    async fn record_file_job(
        &self,
        archive_job_id: Uuid,
        job_id: Uuid,
        pipeline: &PipelineDefinition,
        context: &MeiliContext,
    ) {
        let now = Utc::now();
        let body = serde_json::json!({
            "job_id": job_id,
            "workflow_id": PipelineWorkflowInput::workflow_id(job_id),
            "pipeline_uid": pipeline.uid,
            "tenant_id": context.tenant_id,
            "index_name": context.index,
            "status": JobStatus::Queued.as_str(),
            "started_at": now,
            "updated_at": now,
            "parent_job_id": archive_job_id,
        });
        let result = match self.base.url(&["internal", "jobs"]) {
            Ok(url) => {
                self.base
                    .send_json(self.base.http.post(url).json(&body), "create job")
                    .await
            }
            Err(e) => Err(e),
        };
        if let Err(e) = result {
            tracing::warn!(job_id = %job_id, "could not record an archive file's job row: {e}");
        }
    }

    /// Write the archive job's terminal status to its row.
    pub async fn finished(&self, input: &ArchiveFinishedInput) -> Result<(), PluginError> {
        let mut body = serde_json::json!({ "status": input.status.as_str() });
        if let Some(e) = &input.error {
            body["error"] = serde_json::json!(e.chars().take(1000).collect::<String>());
        }
        let url = self
            .base
            .url(&["internal", "jobs", &input.job_id.to_string()])?;
        let found = self
            .base
            .send_json_unless_gone(self.base.http.patch(url).json(&body), "update job")
            .await?;
        if !found {
            tracing::warn!(job_id = %input.job_id, "no cached job row for the archive");
        }
        Ok(())
    }
}

#[activities]
impl ArchiveActivities {
    /// Read the archive, stage its files and write their manifest.
    #[activity]
    pub async fn expand_archive(
        self: Arc<Self>,
        ctx: ActivityContext,
        input: ExpandArchiveInput,
    ) -> Result<ExpandArchiveOutput, ActivityError> {
        let hb = ctx.clone();
        let keepalive = tokio::spawn(async move {
            let mut tick = tokio::time::interval(KEEPALIVE);
            tick.tick().await;
            loop {
                tick.tick().await;
                if let Err(e) = hb.record_heartbeat("expanding".to_string()).await {
                    tracing::debug!(error = %e, "archive heartbeat failed");
                }
            }
        });
        // Cancellation reaches an activity through its heartbeats.
        let result = self.expand_unless(&input, ctx.cancelled()).await;
        keepalive.abort();
        result.map_err(to_activity_error)
    }

    /// Route one page of the manifest into child jobs.
    #[activity]
    pub async fn prepare_archive_files(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: PrepareArchiveInput,
    ) -> Result<PrepareArchiveOutput, ActivityError> {
        self.prepare(&input).await.map_err(to_activity_error)
    }

    /// Write the archive job's terminal status to its row.
    #[activity]
    pub async fn record_archive_finished(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: ArchiveFinishedInput,
    ) -> Result<(), ActivityError> {
        self.finished(&input).await.map_err(to_activity_error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use meili_ingest_plugin_sdk::{ContentRef, PipelineTrigger, StepDefinition};
    use std::io::Write;
    use wiremock::matchers::{body_partial_json, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn zip_of(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let mut w = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
        let opts = zip::write::SimpleFileOptions::default();
        for (name, data) in entries {
            w.start_file(*name, opts).unwrap();
            w.write_all(data).unwrap();
        }
        w.finish().unwrap().into_inner()
    }

    fn pipeline(uid: &str, plugin_index: Option<&str>) -> PipelineDefinition {
        let mut index = StepDefinition::new("index", "meili_indexer").depends_on(["extract"]);
        if let Some(i) = plugin_index {
            index.config = serde_json::json!({ "index": i });
        }
        PipelineDefinition {
            uid: uid.into(),
            name: uid.into(),
            description: None,
            version: 1,
            trigger: Some(PipelineTrigger {
                content_types: vec!["application/pdf".into()],
                filename_pattern: None,
                index_pattern: None,
            }),
            steps: vec![StepDefinition::new("extract", "pdf_extractor"), index],
            builtin: true,
            tenant_id: None,
        }
    }

    fn activities(cp: &str) -> ArchiveActivities {
        ArchiveActivities::new(
            SourceActivities::new(Some(cp.to_string()), BlobStore::memory()),
            Limits::default(),
        )
    }

    fn destination() -> MeiliContext {
        MeiliContext {
            tenant_id: Some("t1".into()),
            host: Some("http://meili:7700".into()),
            api_key: Some("key".into()),
            ..Default::default()
        }
    }

    async fn staged_archive(a: &ArchiveActivities, job_id: Uuid, data: Vec<u8>) -> PluginInput {
        a.blob()
            .stage_upload(
                job_id,
                Blob::new(data, "application/zip", Some("q3.zip".into())),
                0,
            )
            .await
            .unwrap()
    }

    #[test]
    fn walked_files_carry_their_mime_from_the_walker() {
        // The MIME is settled where the event is built (the blocking walker), so the
        // async loop never parses a file to find its type.
        let walked = Walked::detect(Event::File {
            path: "data/rows".into(),
            data: b"[{\"id\":1},{\"id\":2}]".to_vec(),
        });
        assert_eq!(
            walked,
            Walked::File {
                path: "data/rows".into(),
                data: b"[{\"id\":1},{\"id\":2}]".to_vec(),
                mime: "application/json".into(),
            }
        );
        assert_eq!(
            Walked::detect(Event::File {
                path: "docs/README.md".into(),
                data: b"<div align=\"center\">\n# T\n".to_vec(),
            }),
            Walked::File {
                path: "docs/README.md".into(),
                data: b"<div align=\"center\">\n# T\n".to_vec(),
                mime: "text/markdown".into(),
            }
        );
        assert_eq!(
            Walked::detect(Event::Skipped {
                path: "a".into(),
                reason: "empty".into(),
            }),
            Walked::Skipped {
                path: "a".into(),
                reason: "empty".into(),
            }
        );
    }

    #[test]
    fn file_job_ids_are_stable_and_distinct() {
        let job = Uuid::new_v4();
        assert_eq!(file_job_id(job, 3), file_job_id(job, 3));
        assert_ne!(file_job_id(job, 3), file_job_id(job, 4));
        assert_ne!(file_job_id(job, 3), file_job_id(Uuid::new_v4(), 3));
    }

    #[test]
    fn the_file_index_follows_the_gateway_order() {
        let p = pipeline("builtin.pdf", None);
        let pdf = "application/pdf";
        assert_eq!(
            file_index(Some("locked"), &p, Some("pat"), Some("req"), pdf, "d"),
            "locked"
        );
        assert_eq!(
            file_index(None, &p, Some("pat"), Some("req"), pdf, "d"),
            "pat"
        );
        assert_eq!(file_index(None, &p, None, Some("req"), pdf, "d"), "req");
        assert_eq!(file_index(None, &p, None, None, pdf, "d"), "d");
        assert_eq!(file_index(None, &p, None, None, "image/png", "d"), "images");
        let fixed = pipeline("p", Some("fixed"));
        assert_eq!(
            file_index(None, &fixed, Some("pat"), Some("req"), pdf, "d"),
            "fixed"
        );
    }

    #[tokio::test]
    async fn expansion_stages_every_file_and_lists_the_skipped_ones() {
        let a = activities("http://unused");
        let job_id = Uuid::new_v4();
        let data = zip_of(&[
            ("reports/eu/a.pdf", b"%PDF-1.7 eu"),
            ("reports/us/a.pdf", b"%PDF-1.7 us"),
            ("empty.txt", b""),
        ]);
        let input = staged_archive(&a, job_id, data).await;
        let out = a
            .expand(&ExpandArchiveInput {
                job_id,
                input,
                tenant_id: None,
            })
            .await
            .unwrap();
        assert_eq!(out.files, 2);
        assert_eq!(out.skipped, 1);
        assert_eq!(out.skipped_samples[0].path, "empty.txt");
        assert_eq!(out.manifest, manifest_key(job_id));

        let entries: Vec<ManifestEntry> =
            serde_json::from_slice(&a.blob().get(&out.manifest).await.unwrap()).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].path, "reports/eu/a.pdf");
        assert_eq!(entries[0].mime, "application/pdf");
        assert_eq!(entries[0].job_id, file_job_id(job_id, 0));
        // Staged under the file's own job, with the archive path as its filename.
        let PluginInput::Ref(r @ ContentRef::Staged { .. }) = &entries[1].input else {
            panic!("expected a staged ref, got {:?}", entries[1].input);
        };
        let blob = a
            .blob()
            .fetch_ref(r, &reqwest::Client::new())
            .await
            .unwrap();
        assert_eq!(blob.data, b"%PDF-1.7 us");
        assert_eq!(blob.filename.as_deref(), Some("reports/us/a.pdf"));
    }

    #[tokio::test]
    async fn the_files_of_a_fetched_archive_keep_its_origin() {
        // `https://a.example/q3.zip` and `https://b.example/q3.zip` both hold
        // `report.pdf`: the origin is what keeps their documents apart.
        let a = activities("http://unused");
        let job_id = Uuid::new_v4();
        let archive = Blob::new(
            zip_of(&[("report.pdf", b"%PDF-1.7")]),
            "application/zip",
            Some("q3.zip".into()),
        )
        .with_origin(Some("https://a.example/q3.zip".into()));
        let input = a.blob().stage_upload(job_id, archive, 0).await.unwrap();
        let out = a
            .expand(&ExpandArchiveInput {
                job_id,
                input,
                tenant_id: None,
            })
            .await
            .unwrap();
        let entries: Vec<ManifestEntry> =
            serde_json::from_slice(&a.blob().get(&out.manifest).await.unwrap()).unwrap();
        let PluginInput::Ref(r) = &entries[0].input else {
            panic!("expected a staged ref, got {:?}", entries[0].input);
        };
        let blob = a
            .blob()
            .fetch_ref(r, &reqwest::Client::new())
            .await
            .unwrap();
        assert_eq!(blob.origin.as_deref(), Some("https://a.example/q3.zip"));
        assert_eq!(blob.filename.as_deref(), Some("report.pdf"));
    }

    #[tokio::test]
    async fn a_file_that_is_no_archive_fails_for_good() {
        let a = activities("http://unused");
        let job_id = Uuid::new_v4();
        let input = PluginInput::Bytes(Blob::new(b"hello".to_vec(), "text/plain", None));
        let err = a
            .expand(&ExpandArchiveInput {
                job_id,
                input,
                tenant_id: None,
            })
            .await
            .unwrap_err();
        assert!(!err.is_retryable(), "{err}");
        assert!(
            err.to_string().contains("not a zip, tar or gzip file"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn too_many_files_fails_the_archive_for_good() {
        let mut a = activities("http://unused");
        a.limits.max_files = 1;
        let job_id = Uuid::new_v4();
        let input = staged_archive(&a, job_id, zip_of(&[("a.txt", b"a"), ("b.txt", b"b")])).await;
        let err = a
            .expand(&ExpandArchiveInput {
                job_id,
                input,
                tenant_id: None,
            })
            .await
            .unwrap_err();
        assert!(!err.is_retryable(), "{err}");
        assert!(err.to_string().contains("more than 1 files"), "{err}");
    }

    #[tokio::test]
    async fn a_cancelled_expansion_stops_and_writes_no_manifest() {
        // The archive comes from a host that takes its time: the expansion is still
        // fetching it when the job is cancelled.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/q3.zip"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(zip_of(&[("a.txt", b"a")]))
                    .set_delay(Duration::from_secs(30)),
            )
            .mount(&server)
            .await;
        let a = activities("http://unused");
        let job_id = Uuid::new_v4();
        let input = ExpandArchiveInput {
            job_id,
            input: PluginInput::Ref(ContentRef::Url {
                url: format!("{}/q3.zip", server.uri()),
                mime: Some("application/zip".into()),
                filename: Some("q3.zip".into()),
            }),
            tenant_id: None,
        };
        let cancelled = async {
            while server
                .received_requests()
                .await
                .unwrap_or_default()
                .is_empty()
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };
        let out = tokio::time::timeout(Duration::from_secs(10), a.expand_unless(&input, cancelled))
            .await
            .expect("a cancelled expansion returns at once");
        assert!(matches!(out, Err(PluginError::Cancelled)), "{out:?}");
        assert!(a.blob().get(&manifest_key(job_id)).await.is_err());

        // Not cancelled, it runs to the end.
        let job_id = Uuid::new_v4();
        let input = ExpandArchiveInput {
            job_id,
            input: staged_archive(&a, job_id, zip_of(&[("a.txt", b"a")])).await,
            tenant_id: None,
        };
        let out = a
            .expand_unless(&input, std::future::pending())
            .await
            .unwrap();
        assert_eq!(out.files, 1);
    }

    #[tokio::test]
    async fn files_are_routed_each_by_its_own_type() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/internal/resolve"))
            .and(body_partial_json(
                serde_json::json!({"mime": "application/pdf", "filename": "a.pdf", "tenant_id": "t1"}),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "pipeline": pipeline("builtin.pdf", None),
                "index_pattern": "contracts",
            })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/internal/resolve"))
            .respond_with(
                ResponseTemplate::new(404).set_body_json(serde_json::json!({"error": "none"})),
            )
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/internal/jobs"))
            .and(body_partial_json(
                serde_json::json!({"pipeline_uid": "builtin.pdf"}),
            ))
            .respond_with(ResponseTemplate::new(201))
            .expect(1)
            .mount(&server)
            .await;

        let a = activities(&server.uri());
        let job_id = Uuid::new_v4();
        let data = zip_of(&[
            ("docs/a.pdf", b"%PDF-1.7"),
            ("tool.bin", &[0u8, 1, 2, 0xff]),
        ]);
        let input = staged_archive(&a, job_id, data).await;
        let expanded = a
            .expand(&ExpandArchiveInput {
                job_id,
                input,
                tenant_id: None,
            })
            .await
            .unwrap();

        let out = a
            .prepare(&PrepareArchiveInput {
                job_id,
                manifest: expanded.manifest,
                offset: 0,
                limit: 50,
                context: destination(),
                forced_pipeline: None,
                locked_index: None,
            })
            .await
            .unwrap();
        assert_eq!(out.consumed, 2);
        assert_eq!(out.jobs.len(), 1);
        let job = &out.jobs[0];
        assert_eq!(job.path, "docs/a.pdf");
        assert_eq!(job.workflow_input.job_id, file_job_id(job_id, 0));
        assert_eq!(job.workflow_input.pipeline.uid, "builtin.pdf");
        assert_eq!(
            job.workflow_input.context.index.as_deref(),
            Some("contracts")
        );
        assert_eq!(job.workflow_input.context.tenant_id.as_deref(), Some("t1"));
        assert_eq!(out.skipped.len(), 1);
        assert_eq!(out.skipped[0].path, "tool.bin");
        assert_eq!(
            out.skipped[0].reason,
            "no pipeline handles application/octet-stream"
        );
        assert!(out.failed.is_empty());

        // The second page starts where the first ended.
        let rest = a
            .prepare(&PrepareArchiveInput {
                job_id,
                manifest: manifest_key(job_id),
                offset: 2,
                limit: 50,
                context: destination(),
                forced_pipeline: None,
                locked_index: None,
            })
            .await
            .unwrap();
        assert_eq!(rest.consumed, 0);
    }

    #[tokio::test]
    async fn a_forced_pipeline_skips_routing_and_checks_the_destination() {
        // No resolve mock: a forced pipeline must not ask the control plane.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/internal/jobs"))
            .respond_with(ResponseTemplate::new(201))
            .mount(&server)
            .await;
        let a = activities(&server.uri());
        let job_id = Uuid::new_v4();
        let input = staged_archive(&a, job_id, zip_of(&[("a.txt", b"hello")])).await;
        let expanded = a
            .expand(&ExpandArchiveInput {
                job_id,
                input,
                tenant_id: None,
            })
            .await
            .unwrap();
        let forced = ForcedPipeline {
            pipeline: pipeline("my-text", None),
            index_pattern: None,
        };
        let page = |context: MeiliContext, locked_index: Option<String>, forced: ForcedPipeline| {
            PrepareArchiveInput {
                job_id,
                manifest: expanded.manifest.clone(),
                offset: 0,
                limit: 10,
                context,
                forced_pipeline: Some(forced),
                locked_index,
            }
        };

        let ok = a
            .prepare(&page(destination(), Some("locked".into()), forced.clone()))
            .await
            .unwrap();
        assert_eq!(ok.jobs[0].workflow_input.pipeline.uid, "my-text");
        assert_eq!(
            ok.jobs[0].workflow_input.context.index.as_deref(),
            Some("locked")
        );

        let no_dest = a
            .prepare(&page(
                MeiliContext {
                    tenant_id: Some("t1".into()),
                    ..Default::default()
                },
                None,
                forced.clone(),
            ))
            .await
            .unwrap();
        assert!(no_dest.jobs.is_empty());
        assert!(
            no_dest.failed[0]
                .reason
                .contains("needs a Meilisearch destination"),
            "{:?}",
            no_dest.failed
        );

        let pinned_elsewhere = ForcedPipeline {
            pipeline: pipeline("fixed", Some("other")),
            index_pattern: None,
        };
        let wrong_index = a
            .prepare(&page(
                destination(),
                Some("locked".into()),
                pinned_elsewhere,
            ))
            .await
            .unwrap();
        assert!(wrong_index.jobs.is_empty());
        assert!(
            wrong_index.failed[0]
                .reason
                .contains("writes to index \"other\""),
            "{:?}",
            wrong_index.failed
        );
    }

    #[tokio::test]
    async fn a_file_whose_pipeline_pins_a_connection_gets_no_request_key() {
        // The archive's own input keeps the request's destination for the files that
        // need it; a file job that writes through a connection must not carry the
        // caller's key into its workflow history.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/internal/jobs"))
            .respond_with(ResponseTemplate::new(201))
            .mount(&server)
            .await;
        let a = activities(&server.uri());
        let job_id = Uuid::new_v4();
        let input = staged_archive(&a, job_id, zip_of(&[("a.txt", b"hello")])).await;
        let expanded = a
            .expand(&ExpandArchiveInput {
                job_id,
                input,
                tenant_id: None,
            })
            .await
            .unwrap();
        let mut pinned = pipeline("pinned", None);
        pinned.steps[1].config = serde_json::json!({ "connection": "prod-movies" });

        for (pipeline, key, host) in [
            (pinned, None, None),
            (
                pipeline("plain", None),
                Some("key"),
                Some("http://meili:7700"),
            ),
        ] {
            let out = a
                .prepare(&PrepareArchiveInput {
                    job_id,
                    manifest: expanded.manifest.clone(),
                    offset: 0,
                    limit: 10,
                    context: destination(),
                    forced_pipeline: Some(ForcedPipeline {
                        pipeline,
                        index_pattern: None,
                    }),
                    locked_index: Some("locked".into()),
                })
                .await
                .unwrap();
            let job = &out.jobs[0].workflow_input;
            assert_eq!(job.context.api_key.as_deref(), key, "{}", job.pipeline.uid);
            assert_eq!(job.context.host.as_deref(), host, "{}", job.pipeline.uid);
            assert_eq!(job.context.tenant_id.as_deref(), Some("t1"));
            assert_eq!(job.context.index.as_deref(), Some("locked"));
        }
    }

    /// An in-memory store whose writes each take `delay`, like a remote store that is
    /// slower than decompression.
    #[derive(Debug)]
    struct SlowStore {
        inner: object_store::memory::InMemory,
        delay: Duration,
        /// Writes still to fail.
        failing: std::sync::atomic::AtomicUsize,
    }

    impl std::fmt::Display for SlowStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("SlowStore")
        }
    }

    #[async_trait::async_trait]
    impl object_store::ObjectStore for SlowStore {
        async fn put_opts(
            &self,
            location: &object_store::path::Path,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            tokio::time::sleep(self.delay).await;
            let fail = self
                .failing
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok();
            if fail {
                return Err(object_store::Error::Generic {
                    store: "SlowStore",
                    source: "write refused".into(),
                });
            }
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &object_store::path::Path,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(
            &self,
            location: &object_store::path::Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(location, options).await
        }
        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<
                'static,
                object_store::Result<object_store::path::Path>,
            >,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
        {
            self.inner.delete_stream(locations)
        }
        fn list(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    fn slow_activities(delay: Duration, limits: Limits, inflight: u64) -> ArchiveActivities {
        failing_activities(delay, limits, inflight, 0)
    }

    fn failing_activities(
        delay: Duration,
        limits: Limits,
        inflight: u64,
        failing: usize,
    ) -> ArchiveActivities {
        let store = BlobStore::from_object_store(Arc::new(SlowStore {
            inner: object_store::memory::InMemory::new(),
            delay,
            failing: failing.into(),
        }));
        ArchiveActivities::new(SourceActivities::new(None, store), limits)
            .with_inflight_bytes(inflight)
    }

    const MIB: usize = 1024 * 1024;

    /// A zip of `n` files of 1 MiB each, deflated to almost nothing.
    fn mib_files_zip(n: usize) -> Vec<u8> {
        let content = vec![b'a'; MIB];
        let mut w = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
        let opts = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated);
        for i in 0..n {
            w.start_file(format!("f{i}.txt"), opts).unwrap();
            w.write_all(&content).unwrap();
        }
        w.finish().unwrap().into_inner()
    }

    fn mib_files_input(data: Vec<u8>) -> ExpandArchiveInput {
        ExpandArchiveInput {
            job_id: Uuid::new_v4(),
            input: PluginInput::Bytes(Blob::new(data, "application/zip", None)),
        }
    }

    fn mib_limits() -> Limits {
        Limits {
            max_file_bytes: MIB as u64,
            ..Limits::default()
        }
    }

    #[tokio::test]
    async fn one_archive_expands_at_a_time_per_worker() {
        let a = Arc::new(slow_activities(
            Duration::ZERO,
            mib_limits(),
            2 * MIB as u64,
        ));
        // Another expansion is running.
        let running = Arc::clone(&a.expansions).acquire_owned().await.unwrap();
        let waiting = tokio::spawn({
            let a = Arc::clone(&a);
            async move { a.expand(&mib_files_input(mib_files_zip(1))).await }
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!waiting.is_finished(), "it waits for its turn");
        drop(running);
        let out = tokio::time::timeout(Duration::from_secs(10), waiting)
            .await
            .expect("it runs once the other is done")
            .unwrap()
            .unwrap();
        assert_eq!(out.files, 1);
    }

    #[tokio::test]
    async fn a_failed_expansion_frees_the_walk_waiting_for_room() {
        // The first file cannot be staged; the walk is by then waiting for room for
        // the second. The expansion fails, and its walk stops instead of waiting
        // forever with the worker's expansion slot.
        let a = failing_activities(Duration::from_millis(50), mib_limits(), MIB as u64, 1);
        let err = a
            .expand(&mib_files_input(mib_files_zip(4)))
            .await
            .unwrap_err();
        assert!(err.is_retryable(), "{err}");
        assert!(err.to_string().contains("staging a file"), "{err}");
        let out = tokio::time::timeout(
            Duration::from_secs(10),
            a.expand(&mib_files_input(mib_files_zip(2))),
        )
        .await
        .expect("the next expansion gets the slot")
        .unwrap();
        assert_eq!(out.files, 2);
        assert_eq!(a.budget().high_water(), MIB as u64);
    }

    #[test]
    fn the_budget_always_admits_the_largest_file() {
        let limits = mib_limits();
        assert_eq!(inflight_budget(10, &limits), MIB as u64);
        assert_eq!(inflight_budget(3 * MIB as u64, &limits), 3 * MIB as u64);
        let a = ArchiveActivities::new(SourceActivities::new(None, BlobStore::memory()), limits)
            .with_inflight_bytes(1);
        assert_eq!(a.budget().bytes(), MIB as u64);
        assert_eq!(
            ArchiveActivities::new(
                SourceActivities::new(None, BlobStore::memory()),
                Limits::default()
            )
            .budget()
            .bytes(),
            DEFAULT_INFLIGHT_BYTES
        );
    }

    #[tokio::test]
    async fn decompressed_files_held_in_memory_stay_within_the_budget() {
        // Eight 1 MiB files that compress to almost nothing, a store that takes a
        // while to write each: decompression outruns staging. A 2 MiB budget holds two
        // files at a time, however many the channel could queue.
        let limits = Limits {
            max_file_bytes: MIB as u64,
            ..Limits::default()
        };
        let a = slow_activities(Duration::from_millis(40), limits, 2 * MIB as u64);
        let data = mib_files_zip(8);
        assert!(data.len() < MIB / 10, "the zip is small: {}", data.len());
        let out = a.expand(&mib_files_input(data)).await.unwrap();
        assert_eq!(out.files, 8);
        assert!(
            a.budget().high_water() <= 2 * MIB as u64,
            "{} bytes of files were held at once",
            a.budget().high_water()
        );
    }

    #[tokio::test]
    async fn the_archive_job_row_gets_its_final_status() {
        let server = MockServer::start().await;
        let job_id = Uuid::new_v4();
        Mock::given(method("PATCH"))
            .and(path(format!("/internal/jobs/{job_id}")))
            .and(body_partial_json(
                serde_json::json!({"status": "failed", "error": "2 of 3 files failed"}),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(1)
            .mount(&server)
            .await;
        activities(&server.uri())
            .finished(&ArchiveFinishedInput {
                job_id,
                status: JobStatus::Failed,
                error: Some("2 of 3 files failed".into()),
            })
            .await
            .unwrap();
    }
}
