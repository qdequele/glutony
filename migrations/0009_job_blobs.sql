-- Deleting what a job left in the blob store once it has been over for BLOB_TTL_DAYS.
-- A worker claims due jobs (blobs_claimed_at, a lease) and reports them swept
-- (blobs_deleted_at). Additive only: older code ignores both columns.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS blobs_claimed_at TIMESTAMPTZ;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS blobs_deleted_at TIMESTAMPTZ;

-- The jobs still to sweep, oldest first.
CREATE INDEX IF NOT EXISTS jobs_blobs_due
    ON jobs (updated_at)
    WHERE blobs_deleted_at IS NULL AND status IN ('succeeded', 'failed', 'cancelled');
