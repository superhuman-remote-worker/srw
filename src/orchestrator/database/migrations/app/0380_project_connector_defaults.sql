-- migration:     0380_project_connector_defaults.sql
-- description:   Per-Project connector defaults (slice D3c): the connectors a
--                Project's admin attaches to new jobs and sessions that leave
--                the selection to their defaults. The Project Settings API
--                writes 'settings' rows; a Project manifest that sets
--                defaults.connectors writes and owns a 'manifest' row. The
--                project's own knowledge base is never stored: it is the
--                implied, platform-owned first entry.
-- depends-on:    0363_connector_service_pod_key_idx.notx.sql
-- expected:      < 1s. Creates one empty table.
-- locks:         SHARE ROW EXCLUSIVE on projects and users for the foreign keys (brief).
-- transactional: yes

BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';
SET LOCAL idle_in_transaction_session_timeout = '1min';

CREATE TABLE IF NOT EXISTS project_connector_defaults (
    project_id UUID PRIMARY KEY REFERENCES projects(id) ON DELETE CASCADE,
    connector_ids UUID[] NOT NULL DEFAULT '{}',
    source TEXT NOT NULL DEFAULT 'settings' CHECK (source IN ('settings', 'manifest')),
    manifest_revision TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_by UUID REFERENCES users(id) ON DELETE SET NULL,
    CONSTRAINT project_connector_defaults_revision_follows_source
        CHECK ((source = 'manifest') = (manifest_revision IS NOT NULL))
);

COMMENT ON TABLE project_connector_defaults IS
    'Connector defaults per Project: the linked connectors (datasource ids, which are their Connector uids) attached to new work that takes its defaults. The project knowledge base is implied, never stored.';

COMMIT;
