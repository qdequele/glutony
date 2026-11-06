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
    -- No Meilisearch context here: a cron run has no request to carry one, so the
    -- destination lives on the pipeline's meili_indexer step as a named connection.
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

-- Named Meilisearch destinations. A pipeline's meili_indexer step references one by uid,
-- so a key lives in exactly one place and pipeline JSON never contains a secret. The key
-- is sealed; the control plane only ever moves the sealed bytes.
CREATE TABLE IF NOT EXISTS meili_connections (
    id          UUID PRIMARY KEY,
    uid         TEXT NOT NULL,
    name        TEXT NOT NULL,
    project_id  TEXT,                      -- NULL = global / self-hosted
    host        TEXT NOT NULL,             -- not secret; returned by the API
    api_key     BYTEA NOT NULL,            -- sealed; never returned
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS meili_connections_uid_project
    ON meili_connections (uid, COALESCE(project_id, ''));

-- Lets a job trace back to the source that triggered it. NULL for request-driven jobs.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS source_id UUID;

CREATE INDEX IF NOT EXISTS jobs_source
    ON jobs (source_id, started_at DESC);
