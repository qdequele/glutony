//! Deleting what finished jobs left in the blob store.
//!
//! Every job writes under `jobs/<job_id>/`: the staged upload or source item, the step
//! outputs spilled past `PAYLOAD_SPILL_BYTES`, an archive's manifest. Nothing needs
//! them once the job is over, except a Temporal reset or a re-run of a failed job, so
//! they are kept `BLOB_TTL_DAYS` (control plane, default 7) after the job ended, then
//! deleted.
//!
//! The control plane knows which jobs are due (its job table); every worker, which
//! has the blob store, asks it on a timer ([`SWEEP_INTERVAL`]), deletes their blobs,
//! and reports them swept. A claim is a lease: two workers never sweep the same job at
//! once, and a job whose blobs could not all be deleted is claimed again later.
//!
//! An archive job's files are staged under their own job ids. Each file job sweeps its
//! own; the archive also deletes those its manifest lists (or, without a manifest, the
//! file ids in order until one has nothing), so files that never got a job row of
//! their own do not stay behind.
//!
//! Jobs without a row, and objects nobody can trace to a job, are left to a bucket
//! lifecycle rule on `jobs/` (docs: "Blob store retention").

use std::time::Duration;

use meili_ingest_blob::{BlobError, BlobStore};
use meili_ingest_plugin_sdk::ARCHIVE_JOB_PIPELINE;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::archive_activity::{file_job_id, manifest_key};

/// How often a worker asks the control plane for jobs to sweep.
pub const SWEEP_INTERVAL: Duration = Duration::from_secs(10 * 60);

/// Jobs claimed per request.
const BATCH: usize = 100;

/// Most batches swept in one round, so a large backlog is spread over several.
const MAX_BATCHES_PER_ROUND: usize = 20;

/// Answer of `POST /internal/blob-sweep/claim`.
#[derive(Debug, Deserialize)]
struct Claim {
    #[serde(default)]
    jobs: Vec<ClaimedJob>,
}

#[derive(Debug, Deserialize)]
struct ClaimedJob {
    job_id: Uuid,
    #[serde(default)]
    pipeline_uid: String,
}

/// Body of `POST /internal/blob-sweep/done`.
#[derive(Debug, Serialize)]
struct Done<'a> {
    job_ids: &'a [Uuid],
}

/// The part of an archive manifest entry the sweep reads.
#[derive(Debug, Deserialize)]
struct ManifestJob {
    job_id: Uuid,
}

/// Deletes the blobs of the jobs the control plane says are due.
#[derive(Debug, Clone)]
pub struct BlobSweeper {
    http: reqwest::Client,
    control_plane_url: String,
    control_plane_token: Option<String>,
    blob: BlobStore,
}

impl BlobSweeper {
    /// A sweeper asking `control_plane_url` and deleting from `blob`.
    pub fn new(
        control_plane_url: &str,
        control_plane_token: Option<String>,
        blob: BlobStore,
    ) -> Self {
        Self {
            http: reqwest::Client::new(),
            control_plane_url: control_plane_url.trim_end_matches('/').to_string(),
            control_plane_token,
            blob,
        }
    }

    /// Sweep forever, every `interval`. Errors are logged and the next round retries.
    pub async fn run(self, interval: Duration) {
        let mut tick = tokio::time::interval(interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            match self.sweep().await {
                Ok(0) => tracing::debug!("no job blobs to delete"),
                Ok(jobs) => tracing::info!(jobs, "deleted the blobs of finished jobs"),
                Err(e) => tracing::warn!(error = %e, "could not sweep job blobs; retrying later"),
            }
        }
    }

    /// One round: claim due jobs, delete their blobs, report them, until the control
    /// plane has no more (or [`MAX_BATCHES_PER_ROUND`]). Returns the jobs swept.
    pub async fn sweep(&self) -> Result<usize, String> {
        let mut swept = 0;
        for _ in 0..MAX_BATCHES_PER_ROUND {
            let claimed = self.claim().await?;
            let mut done = Vec::with_capacity(claimed.len());
            for job in &claimed {
                match self.delete_job_blobs(job).await {
                    Ok(objects) => {
                        tracing::debug!(job_id = %job.job_id, objects, "job blobs deleted");
                        done.push(job.job_id);
                    }
                    // Not reported: claimed again once the lease is over.
                    Err(e) => tracing::warn!(
                        job_id = %job.job_id,
                        error = %e,
                        "could not delete a job's blobs"
                    ),
                }
            }
            if !done.is_empty() {
                self.report(&done).await?;
            }
            swept += done.len();
            if claimed.len() < BATCH {
                break;
            }
        }
        Ok(swept)
    }

    /// Everything the job left: its own prefix and, for an archive, its files'.
    async fn delete_job_blobs(&self, job: &ClaimedJob) -> Result<usize, BlobError> {
        let mut deleted = 0;
        if job.pipeline_uid == ARCHIVE_JOB_PIPELINE {
            // Before the archive's own prefix, which holds the manifest: an
            // interrupted sweep still finds it next time.
            match self.archive_files(job.job_id).await? {
                Some(files) => {
                    for file in files {
                        deleted += self.blob.delete_job(file).await?;
                    }
                }
                None => {
                    // Without a manifest (the expansion failed midway), the files are
                    // the archive's ordinals from 0, up to the first one with nothing.
                    for ordinal in 0.. {
                        let n = self
                            .blob
                            .delete_job(file_job_id(job.job_id, ordinal))
                            .await?;
                        if n == 0 {
                            break;
                        }
                        deleted += n;
                    }
                }
            }
        }
        deleted += self.blob.delete_job(job.job_id).await?;
        Ok(deleted)
    }

    /// The file jobs an archive's manifest lists; `None` without a readable one.
    async fn archive_files(&self, job_id: Uuid) -> Result<Option<Vec<Uuid>>, BlobError> {
        let bytes = match self.blob.get(&manifest_key(job_id)).await {
            Ok(b) => b,
            Err(BlobError::NotFound(_)) => return Ok(None),
            Err(e) => return Err(e),
        };
        Ok(serde_json::from_slice::<Vec<ManifestJob>>(&bytes)
            .ok()
            .map(|entries| entries.into_iter().map(|e| e.job_id).collect()))
    }

    async fn claim(&self) -> Result<Vec<ClaimedJob>, String> {
        let url = format!("{}/internal/blob-sweep/claim", self.control_plane_url);
        let resp = crate::connection::authed(
            self.http
                .post(url)
                .json(&serde_json::json!({ "limit": BATCH })),
            self.control_plane_token.as_deref(),
        )
        .send()
        .await
        .map_err(|e| format!("control plane unreachable: {e}"))?;
        if !resp.status().is_success() {
            return Err(format!(
                "control plane returned {} claiming jobs to sweep",
                resp.status()
            ));
        }
        resp.json::<Claim>()
            .await
            .map(|c| c.jobs)
            .map_err(|e| format!("unreadable claim: {e}"))
    }

    async fn report(&self, job_ids: &[Uuid]) -> Result<(), String> {
        let url = format!("{}/internal/blob-sweep/done", self.control_plane_url);
        let resp = crate::connection::authed(
            self.http.post(url).json(&Done { job_ids }),
            self.control_plane_token.as_deref(),
        )
        .send()
        .await
        .map_err(|e| format!("control plane unreachable: {e}"))?;
        if !resp.status().is_success() {
            return Err(format!(
                "control plane returned {} recording swept jobs",
                resp.status()
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{body_json, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// One object under the job's prefix.
    async fn put(store: &BlobStore, job: Uuid, name: &str, body: &[u8]) {
        store
            .put(&BlobStore::staged_key(job, name), body.to_vec().into())
            .await
            .unwrap();
    }

    async fn exists(store: &BlobStore, job: Uuid, name: &str) -> bool {
        store.get(&BlobStore::staged_key(job, name)).await.is_ok()
    }

    async fn control_plane(jobs: serde_json::Value, done: serde_json::Value) -> MockServer {
        let cp = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/internal/blob-sweep/claim"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"enabled": true, "jobs": jobs})),
            )
            .expect(1)
            .mount(&cp)
            .await;
        Mock::given(method("POST"))
            .and(path("/internal/blob-sweep/done"))
            .and(body_json(serde_json::json!({ "job_ids": done })))
            .respond_with(ResponseTemplate::new(204))
            .expect(1)
            .mount(&cp)
            .await;
        cp
    }

    #[tokio::test]
    async fn a_sweep_deletes_the_claimed_jobs_blobs_and_reports_them() {
        let blob = BlobStore::memory();
        let (pdf, archive, kept) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
        put(&blob, pdf, "input/a.pdf", b"%PDF").await;
        put(&blob, pdf, "extract.json", b"{}").await;
        // The archive's two files: one ran (and has a row), one never got a job row.
        let files = [file_job_id(archive, 0), file_job_id(archive, 1)];
        put(&blob, archive, "input/q3.zip", b"PK").await;
        let manifest = serde_json::json!([
            {"job_id": files[0], "path": "a.pdf", "mime": "application/pdf", "input": {}},
            {"job_id": files[1], "path": "b.pdf", "mime": "application/pdf", "input": {}},
        ]);
        put(
            &blob,
            archive,
            "archive/manifest.json",
            manifest.to_string().as_bytes(),
        )
        .await;
        for f in files {
            put(&blob, f, "input/x.pdf", b"%PDF").await;
        }
        // Not claimed: still running, or not due yet.
        put(&blob, kept, "input/c.pdf", b"%PDF").await;

        let cp = control_plane(
            serde_json::json!([
                {"job_id": pdf, "pipeline_uid": "builtin.pdf"},
                {"job_id": archive, "pipeline_uid": "archive"},
            ]),
            serde_json::json!([pdf, archive]),
        )
        .await;
        let sweeper = BlobSweeper::new(&cp.uri(), Some("t".into()), blob.clone());
        assert_eq!(sweeper.sweep().await.unwrap(), 2);

        assert!(!exists(&blob, pdf, "input/a.pdf").await);
        assert!(!exists(&blob, pdf, "extract.json").await);
        assert!(!exists(&blob, archive, "input/q3.zip").await);
        assert!(!exists(&blob, archive, "archive/manifest.json").await);
        for f in files {
            assert!(!exists(&blob, f, "input/x.pdf").await, "{f}");
        }
        assert!(exists(&blob, kept, "input/c.pdf").await);
        let auth = cp.received_requests().await.unwrap();
        assert!(
            auth.iter().all(|r| r
                .headers
                .get("authorization")
                .is_some_and(|v| v == "Bearer t")),
            "every call carries the control-plane token"
        );
    }

    #[tokio::test]
    async fn an_archive_without_a_manifest_sweeps_its_files_in_order() {
        // The expansion staged two files, then failed before writing the manifest.
        let blob = BlobStore::memory();
        let archive = Uuid::new_v4();
        put(&blob, archive, "input/q3.zip", b"PK").await;
        let staged = [file_job_id(archive, 0), file_job_id(archive, 1)];
        for f in staged {
            put(&blob, f, "input/x.pdf", b"%PDF").await;
        }
        let cp = control_plane(
            serde_json::json!([{"job_id": archive, "pipeline_uid": "archive"}]),
            serde_json::json!([archive]),
        )
        .await;
        BlobSweeper::new(&cp.uri(), None, blob.clone())
            .sweep()
            .await
            .unwrap();
        for f in staged {
            assert!(!exists(&blob, f, "input/x.pdf").await, "{f}");
        }
        assert!(!exists(&blob, archive, "input/q3.zip").await);
    }

    #[tokio::test]
    async fn nothing_due_reports_nothing() {
        let cp = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/internal/blob-sweep/claim"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"enabled": false, "jobs": []})),
            )
            .expect(1)
            .mount(&cp)
            .await;
        Mock::given(path("/internal/blob-sweep/done"))
            .respond_with(ResponseTemplate::new(204))
            .expect(0)
            .mount(&cp)
            .await;
        let sweeper = BlobSweeper::new(&cp.uri(), None, BlobStore::memory());
        assert_eq!(sweeper.sweep().await.unwrap(), 0);

        // An older control plane without the route: an error, retried next round.
        let old = MockServer::start().await;
        let err = BlobSweeper::new(&old.uri(), None, BlobStore::memory())
            .sweep()
            .await
            .unwrap_err();
        assert!(err.contains("404"), "{err}");
    }
}
