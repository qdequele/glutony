-- Roll back migrations 0003 (tenant_id), 0004 (lab_events) and 0005 (its v2 rewrite,
-- dropped with the table) so the previous glutony binary starts: sqlx refuses a
-- database with a migration it does not know.
--
--   psql "$DATABASE_URL" --single-transaction -v ON_ERROR_STOP=1 -f scripts/rollback-lab-seams.sql
--
-- Undelivered Lab events are lost by a rollback, so the script refuses while any
-- exist. Deliver them (the sender drains the outbox) or force it:
--
--   PGOPTIONS='-c glutony.rollback_force=on' psql "$DATABASE_URL" --single-transaction -v ON_ERROR_STOP=1 -f scripts/rollback-lab-seams.sql
--
-- `--single-transaction` makes it all-or-nothing. The file has no BEGIN/COMMIT of its
-- own on purpose: run as one batch (as the test does), Postgres already wraps it in
-- one implicit transaction, and an explicit BEGIN would leave the connection in an
-- aborted transaction when the guard fires.
--
-- Stop every glutony service first. See docs/superpowers/specs/2026-10-01-lab-ready-seams-design.md §10.

DO $$
BEGIN
    IF to_regclass('lab_events') IS NOT NULL
       AND coalesce(current_setting('glutony.rollback_force', true), '') <> 'on'
       AND EXISTS (SELECT 1 FROM lab_events WHERE delivered_at IS NULL)
    THEN
        RAISE EXCEPTION 'lab_events has undelivered rows; let the sender deliver them or set glutony.rollback_force = on';
    END IF;
END $$;

DROP TABLE IF EXISTS lab_events;

ALTER TABLE pipelines         RENAME COLUMN tenant_id TO project_id;
ALTER TABLE jobs              RENAME COLUMN tenant_id TO project_id;
ALTER TABLE sources           RENAME COLUMN tenant_id TO project_id;
ALTER TABLE meili_connections RENAME COLUMN tenant_id TO project_id;

ALTER INDEX pipelines_uid_tenant         RENAME TO pipelines_uid_project;
ALTER INDEX jobs_tenant_started          RENAME TO jobs_project_started;
ALTER INDEX sources_uid_tenant           RENAME TO sources_uid_project;
ALTER INDEX sources_pipeline_tenant      RENAME TO sources_pipeline;
ALTER INDEX meili_connections_uid_tenant RENAME TO meili_connections_uid_project;

DELETE FROM _sqlx_migrations WHERE version IN (3, 4, 5);
