-- Scheduled sources: a location fetched on a cron and run through a pipeline.
-- See docs/superpowers/specs/2026-09-13-scheduled-sources-design.md.

CREATE TABLE IF NOT EXISTS sources (
    id            UUID PRIMARY KEY,
    uid           TEXT NOT NULL,
    name          TEXT NOT NULL,
    description   TEXT,
    project_id    TEXT,
    pipeline_uid  TEXT NOT NULL,
    location      JSONB NOT NULL,          -- tagged union; v1 only writes kind = "url"
    cron          TEXT NOT NULL,
    timezone      TEXT NOT NULL DEFAULT 'UTC',
    paused        BOOLEAN NOT NULL DEFAULT false,
    index_name    TEXT,
    fetch_auth    BYTEA,                   -- sealed; NULL = unauthenticated
    meili_ctx     BYTEA NOT NULL,          -- sealed: host + api_key + region
    last_etag     TEXT,
    last_modified TEXT,
    last_hash     TEXT,                    -- hex blake3 of the last fetched body
    last_run_at   TIMESTAMPTZ,
    last_status   TEXT,
    last_error    TEXT,
    schedule_id   TEXT NOT NULL,           -- Temporal schedule id
    archived_at   TIMESTAMPTZ,             -- set when the source's pipeline was deleted
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Same shape as pipelines_uid_project: a uid may exist once globally (project_id IS
-- NULL) and once per tenant, hence the surrogate key plus an expression-based index.
CREATE UNIQUE INDEX IF NOT EXISTS sources_uid_project
    ON sources (uid, COALESCE(project_id, ''));

-- Supports the archive cascade when a pipeline is deleted.
CREATE INDEX IF NOT EXISTS sources_pipeline
    ON sources (pipeline_uid, COALESCE(project_id, ''));

-- Run history. Duplicates what Temporal knows for the same reason `jobs` does: workflow
-- history is retention-limited and cannot be filtered per tenant without a visibility
-- query per request. Temporal remains the source of truth.
CREATE TABLE IF NOT EXISTS source_runs (
    run_id      UUID PRIMARY KEY,
    source_id   UUID NOT NULL REFERENCES sources (id) ON DELETE CASCADE,
    started_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    outcome     TEXT NOT NULL,             -- unchanged | ingested | failed
    items       INT NOT NULL DEFAULT 0,
    job_ids     UUID[] NOT NULL DEFAULT '{}',
    error       TEXT
);

CREATE INDEX IF NOT EXISTS source_runs_source_started
    ON source_runs (source_id, started_at DESC);

-- Lets a job trace back to the source that triggered it. NULL for request-driven jobs.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS source_id UUID;

CREATE INDEX IF NOT EXISTS jobs_source
    ON jobs (source_id, started_at DESC);
