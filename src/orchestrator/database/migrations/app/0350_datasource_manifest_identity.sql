-- migration:     0350_datasource_manifest_identity.sql
-- description:   Datasources become Connectors (connector_drivers.md, slice
--                D3a): each row gains the manifest Connector resource it is
--                written through to, and the general platform-owned marker.
-- depends-on:    0345_validate_pinned_input_admission_count.sql
-- expected:      < 5s. Two nullable columns without a default (catalog-only
--                on PG 11+) and two constraints added NOT VALID; 0352
--                validates them.
-- locks:         ACCESS EXCLUSIVE on datasources and SHARE ROW EXCLUSIVE on
--                srw_resources, each brief and bounded by lock_timeout.
-- transactional: yes
--
-- manifest_resource_id follows experts and projects (0234): a reference to
-- the srw_resources row that holds the connector's definition. For a
-- datasource the resource uid is the datasource id itself, so every stored
-- selection already names its Connector; the column marks the row as written
-- through and lets the startup backfill tell a retired resource from a
-- missing one. No SQL backfill: the resources are built in Python by
-- migrate_stored_connectors, which reads the encrypted credentials.
--
-- managed_key is the platform-owned marker, after experts.managed_key (0064):
-- project-kb:<project> for a project's own knowledge base today, later
-- project-cloud:<project> and user-cloud-root:<user>. It replaces the
-- config.native_project_id marker, which stays as a mirror for one release
-- because SQL predicates still read it. 0353 makes the key unique.
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';
SET LOCAL idle_in_transaction_session_timeout = '1min';

ALTER TABLE public.datasources ADD COLUMN IF NOT EXISTS manifest_resource_id UUID;
ALTER TABLE public.datasources
    ADD CONSTRAINT datasources_manifest_resource_id_fkey
    FOREIGN KEY (manifest_resource_id) REFERENCES public.srw_resources(id)
    ON DELETE RESTRICT NOT VALID;

ALTER TABLE public.datasources ADD COLUMN IF NOT EXISTS managed_key TEXT;
ALTER TABLE public.datasources
    ADD CONSTRAINT datasources_managed_key_shape
    CHECK (
        managed_key IS NULL
        OR managed_key ~ '^[a-z][a-z0-9-]{0,62}:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    ) NOT VALID;

COMMENT ON COLUMN public.datasources.manifest_resource_id IS
    'The manifest Connector resource this row is written through to; its uid is the datasource id. NULL until the row is written through or backfilled, and for rows left on the legacy path.';
COMMENT ON COLUMN public.datasources.managed_key IS
    'Stable platform identity of a connector SRW provisions itself (project-kb:<project>, ...). Platform-owned rows refuse policy edits, deletes, links and unlinks through the API.';

COMMIT;
