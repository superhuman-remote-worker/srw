-- migration:     0307_installation_managed_resources.sql
-- description:   Marks manifest resources that the installation owns: the
--                chart's built-in WorkspaceTemplates. Only the orchestrator's
--                startup reconciler writes marked rows; the API refuses to
--                edit or delete them.
-- depends-on:    0234_manifest_resources.sql
-- expected:      < 1s. ADD COLUMN with a constant default is a catalog-only
--                change on PG 11+; no row is rewritten.
-- locks:         ACCESS EXCLUSIVE on srw_resources for the column add (brief).
-- transactional: yes

BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';
SET LOCAL idle_in_transaction_session_timeout = '1min';

ALTER TABLE srw_resources
    ADD COLUMN IF NOT EXISTS installation_managed BOOLEAN NOT NULL DEFAULT FALSE;

COMMENT ON COLUMN srw_resources.installation_managed IS
    'TRUE for resources the installation owns. Only the startup reconciler writes them; the API refuses edits and deletes.';

COMMIT;
