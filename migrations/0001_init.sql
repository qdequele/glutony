-- meili-ingest control plane: initial schema (SPEC §12 + plan "Control plane HTTP API").

-- User-defined pipeline definitions.
-- The same `uid` may exist once globally (project_id IS NULL) and once per tenant,
-- hence the surrogate primary key and the expression-based unique index.
CREATE TABLE IF NOT EXISTS pipelines (
    id           BIGSERIAL PRIMARY KEY,
    uid          TEXT NOT NULL,
    name         TEXT NOT NULL,
    description  TEXT,
    version      INT NOT NULL DEFAULT 1,
    definition   JSONB NOT NULL,          -- full PipelineDefinition as JSON
    project_id   TEXT,                    -- NULL = global; set = scoped to one tenant
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS pipelines_uid_project
    ON pipelines (uid, COALESCE(project_id, ''));

-- Job tracking (denormalized mirror of Temporal state, for quick status queries).
CREATE TABLE IF NOT EXISTS jobs (
    job_id        UUID PRIMARY KEY,
    workflow_id   TEXT NOT NULL,
    pipeline_uid  TEXT NOT NULL,
    project_id    TEXT,
    index_name    TEXT,
    status        TEXT NOT NULL DEFAULT 'queued',
    current_step  TEXT,
    error         TEXT,
    started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS jobs_project_started
    ON jobs (project_id, started_at DESC);

-- Plugin manifests reported by workers at boot (upsert by name).
CREATE TABLE IF NOT EXISTS plugins (
    name        TEXT PRIMARY KEY,
    manifest    JSONB NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
