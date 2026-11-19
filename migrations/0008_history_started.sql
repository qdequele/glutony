-- Indexes for the opt-in history retention (JOBS_RETENTION_DAYS, crate::retention),
-- which deletes finished jobs and source runs by `started_at`, and for `GET /jobs`
-- without a tenant, which sorts every job by `started_at`. Additive only: older code
-- ignores them.
CREATE INDEX IF NOT EXISTS jobs_started ON jobs (started_at);

CREATE INDEX IF NOT EXISTS source_runs_started ON source_runs (started_at);
