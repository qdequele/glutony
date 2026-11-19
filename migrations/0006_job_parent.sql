-- Links the job of each file of an archive upload to the archive's own job, so
-- `GET /jobs?parent_job_id=` lists them. NULL for every other job. Additive only:
-- older code ignores the column.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS parent_job_id UUID;

CREATE INDEX IF NOT EXISTS jobs_parent
    ON jobs (parent_job_id, started_at DESC)
    WHERE parent_job_id IS NOT NULL;
