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

use std::sync::Arc;
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
use uuid::Uuid;

use crate::activity::to_activity_error;
use crate::source_activity::SourceActivities;

/// How often the expansion heartbeats while it works.
const KEEPALIVE: Duration = Duration::from_secs(20);

/// Shared state of the archive activities.
pub struct ArchiveActivities {
    /// Control plane access, blob store and default index (shared with the source
    /// activities, which need the same three).
    pub base: SourceActivities,
    /// Bounds on one archive.
    pub limits: Limits,
}

impl ArchiveActivities {
    /// Activities using `base` for the control plane and the blob store.
    pub fn new(base: SourceActivities, limits: Limits) -> Self {
        Self { base, limits }
    }

    fn blob(&self) -> &BlobStore {
        &self.base.blob
    }
}

/// Input of [`ArchiveActivities::expand_archive`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExpandArchiveInput {
    /// The archive job.
    pub job_id: Uuid,
    /// The archive itself (a reference).
    pub input: PluginInput,
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
        let blob = match &input.input {
            PluginInput::Ref(r) => self
                .blob()
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

        let Blob { data, filename, .. } = blob;
        let limits = self.limits.clone();
        // Small buffer: at most a few files are held in memory between the walk and
        // the blob store.
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Event>(4);
        let walker = tokio::task::spawn_blocking(move || {
            meili_ingest_archive::walk(&data, format, filename.as_deref(), &limits, &mut |e| {
                tx.blocking_send(e)
                    .map_err(|_| "the archive expansion was abandoned".to_string())
            })
        });

        let mut entries: Vec<ManifestEntry> = Vec::new();
        let mut skipped = 0usize;
        let mut skipped_samples = Vec::new();
        while let Some(event) = rx.recv().await {
            match event {
                Event::File { path, data } => {
                    let job_id = file_job_id(input.job_id, entries.len());
                    let mime = detect_mime(&data, Some(&path), None);
                    // Always staged: bytes never enter workflow history. The full path
                    // is the filename, so documents keep their folder and two `a.pdf`
                    // in different folders get different ids.
                    let staged = self
                        .blob()
                        .stage_upload(job_id, Blob::new(data, mime.clone(), Some(path.clone())), 0)
                        .await
                        .map_err(|e| from_blob_error("staging a file of the archive", e))?;
                    entries.push(ManifestEntry {
                        job_id,
                        path,
                        mime,
                        input: staged,
                    });
                }
                Event::Skipped { path, reason } => {
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
        let result = self.expand(&input).await;
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
            .expand(&ExpandArchiveInput { job_id, input })
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
    async fn a_file_that_is_no_archive_fails_for_good() {
        let a = activities("http://unused");
        let job_id = Uuid::new_v4();
        let input = PluginInput::Bytes(Blob::new(b"hello".to_vec(), "text/plain", None));
        let err = a
            .expand(&ExpandArchiveInput { job_id, input })
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
            .expand(&ExpandArchiveInput { job_id, input })
            .await
            .unwrap_err();
        assert!(!err.is_retryable(), "{err}");
        assert!(err.to_string().contains("more than 1 files"), "{err}");
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
            .expand(&ExpandArchiveInput { job_id, input })
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
            .expand(&ExpandArchiveInput { job_id, input })
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
