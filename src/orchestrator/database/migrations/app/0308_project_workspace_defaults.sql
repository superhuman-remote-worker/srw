-- migration:     0308_project_workspace_defaults.sql
-- description:   Per-Project workspace defaults (Slice A2b): a tier mode per
--                role and a template per tier. The Project Settings tab writes
--                'settings' rows; a Project manifest that sets
--                defaults.workspace writes and owns a 'manifest' row.
-- depends-on:    0307_installation_managed_resources.sql
-- expected:      < 1s. Creates one empty table.
-- locks:         SHARE ROW EXCLUSIVE on projects and users for the foreign keys (brief).
-- transactional: yes

BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';
SET LOCAL idle_in_transaction_session_timeout = '1min';

CREATE TABLE IF NOT EXISTS project_workspace_defaults (
    project_id UUID PRIMARY KEY REFERENCES projects(id) ON DELETE CASCADE,
    jobs_mode TEXT CHECK (jobs_mode IN ('none', 'virtual', 'container', 'vm')),
    sessions_mode TEXT CHECK (sessions_mode IN ('none', 'virtual', 'container', 'vm')),
    container_template JSONB,
    vm_template JSONB,
    source TEXT NOT NULL DEFAULT 'settings' CHECK (source IN ('settings', 'manifest')),
    manifest_revision TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_by UUID REFERENCES users(id) ON DELETE SET NULL,
    CONSTRAINT project_workspace_defaults_revision_follows_source
        CHECK ((source = 'manifest') = (manifest_revision IS NOT NULL))
);

COMMENT ON TABLE project_workspace_defaults IS
    'Workspace defaults per Project: tier modes for Jobs and Sessions, and a template per tier. The resolver reads only this table for the Project layer.';

COMMIT;
