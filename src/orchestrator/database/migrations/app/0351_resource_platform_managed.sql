-- migration:     0351_resource_platform_managed.sql
-- description:   The resource side of the platform-owned marker (slice D3a):
--                a Connector resource written through from a platform-owned
--                datasource carries the row's managed key as its reason.
-- depends-on:    0350_datasource_manifest_identity.sql
-- expected:      < 1s. A nullable column without a default is a catalog-only
--                change on PG 11+; no row is rewritten.
-- locks:         ACCESS EXCLUSIVE on srw_resources for the column add (brief).
-- transactional: yes
--
-- This is deliberately not installation_managed (0307): those rows belong to
-- the startup reconciler and the chart. Nor is it managed_by alone: a Project
-- apply retires the managed children it omits, so a Project manifest that
-- forgot its knowledge base would retire it. Only the datasource write-through
-- writes a marked row; the resource API refuses to edit or delete one.
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';
SET LOCAL idle_in_transaction_session_timeout = '1min';

ALTER TABLE public.srw_resources ADD COLUMN IF NOT EXISTS platform_managed TEXT;

COMMENT ON COLUMN public.srw_resources.platform_managed IS
    'Managed key of the platform-owned domain row this resource mirrors (project-kb:<project>, ...). Only the platform write-through writes it; the API refuses edits and deletes.';

COMMIT;
