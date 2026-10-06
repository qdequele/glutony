-- The owner of a row is an opaque tenant (a Lab account, a Cloud project, or NULL for
-- the global scope). See docs/superpowers/specs/2026-10-01-lab-ready-seams-design.md §3.
-- Renames only touch the catalog: no rewrite. Expression indexes follow the column.

ALTER TABLE pipelines         RENAME COLUMN project_id TO tenant_id;
ALTER TABLE jobs              RENAME COLUMN project_id TO tenant_id;
ALTER TABLE sources           RENAME COLUMN project_id TO tenant_id;
ALTER TABLE meili_connections RENAME COLUMN project_id TO tenant_id;

ALTER INDEX pipelines_uid_project         RENAME TO pipelines_uid_tenant;
ALTER INDEX jobs_project_started          RENAME TO jobs_tenant_started;
ALTER INDEX sources_uid_project           RENAME TO sources_uid_tenant;
ALTER INDEX sources_pipeline              RENAME TO sources_pipeline_tenant;
ALTER INDEX meili_connections_uid_project RENAME TO meili_connections_uid_tenant;
