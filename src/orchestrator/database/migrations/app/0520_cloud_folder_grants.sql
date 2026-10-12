-- migration:     0520_cloud_folder_grants.sql
-- description:   Per-folder grants of the main cloud (main cloud as
--                connectors, slices 5 and 7, "Per-folder grants"). One row
--                per Nextcloud principal SRW creates for one project folder
--                connector, one project member and one kind: a reader
--                (srw-reader-f-<id>, its group srw-rog-f-<id> granted read
--                only on the folder) or a writer (srw-writer-f-<id>, its
--                group srw-rwg-f-<id> granted read, update, create and
--                delete, never share). Project mounts carry these instead of
--                the agent-service account. The principal's password is an
--                APP_ENCRYPTION_KEY ciphertext, stored before the first
--                provider call so every retry uses the same one and dropped
--                once the principal is gone. Lifecycle: minting, active (a
--                reader only once its read-only probe passed), revoking,
--                revoked, failed (a mint that did not finish, cleaned up)
--                and abandoned (SRW gave up the revoke; the principal may be
--                left in the main cloud). The connector, project and user
--                carry no foreign key, as connector_minted_credentials'
--                owners do: the revoke outlives them, and a sweep requests
--                it once they are gone. The installation does: a grant names
--                exactly one retained main-cloud installation.
-- depends-on:    0432_validate_vm_never_app_ready_retained_stop.sql
-- expected:      < 1s (one empty table and its indexes).
-- locks:         catalog locks; SHARE ROW EXCLUSIVE on the small
--                main_cloud_backend_instances table for the foreign key.
-- transactional: yes

BEGIN;
SET LOCAL lock_timeout                        = '2s';
SET LOCAL statement_timeout                   = '15min';
SET LOCAL idle_in_transaction_session_timeout = '5min';
SET LOCAL timezone                            = 'UTC';

CREATE TABLE public.cloud_folder_grants (
    id                     UUID PRIMARY KEY,
    kind                   TEXT NOT NULL,
    datasource_id          UUID NOT NULL,
    project_id             UUID NOT NULL,
    -- The project member the principal serves: one principal per member,
    -- so removing a member revokes exactly that member's mounts.
    user_id                UUID NOT NULL,
    backend_id             TEXT NOT NULL,
    backend_instance_id    UUID NOT NULL,
    -- sha256 of the folder handle the grant was made on: a connector
    -- re-pointed at another folder never reuses it.
    folder_handle_sha256   TEXT NOT NULL,
    principal_id           TEXT NOT NULL,
    group_id               TEXT NOT NULL,
    credentials            TEXT,
    webdav_url             TEXT NOT NULL,
    status                 TEXT NOT NULL DEFAULT 'minting',
    failure_code           TEXT,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- When a reader's read-only probe passed (a writer is never probed).
    verified_at            TIMESTAMPTZ,
    activated_at           TIMESTAMPTZ,
    revoke_requested_at    TIMESTAMPTZ,
    revoke_reason          TEXT,
    revoke_attempts        INTEGER NOT NULL DEFAULT 0,
    revoke_next_at         TIMESTAMPTZ,
    revoke_error           TEXT,
    revoked_at             TIMESTAMPTZ,
    CONSTRAINT cloud_folder_grants_kind_check
        CHECK (kind IN ('reader', 'writer')),
    CONSTRAINT cloud_folder_grants_status_check
        CHECK (status IN ('minting', 'active', 'revoking', 'revoked', 'failed',
                          'abandoned')),
    CONSTRAINT cloud_folder_grants_folder_check
        CHECK (folder_handle_sha256 ~ '^[0-9a-f]{64}$'),
    -- The principal and its group are named after the row: a revoke of one
    -- grant can never address another's.
    CONSTRAINT cloud_folder_grants_principal_check
        CHECK (principal_id = CASE kind WHEN 'reader' THEN 'srw-reader-f-'
                                        ELSE 'srw-writer-f-' END
                              || replace(id::text, '-', '')),
    CONSTRAINT cloud_folder_grants_group_check
        CHECK (group_id = CASE kind WHEN 'reader' THEN 'srw-rog-f-'
                                    ELSE 'srw-rwg-f-' END
                          || replace(id::text, '-', '')),
    CONSTRAINT cloud_folder_grants_webdav_check
        CHECK (webdav_url <> ''),
    -- A minting or active grant holds its password; a done one none.
    CONSTRAINT cloud_folder_grants_credentials_check
        CHECK (status NOT IN ('minting', 'active')
               OR NULLIF(credentials, '') IS NOT NULL),
    CONSTRAINT cloud_folder_grants_done_check
        CHECK (status NOT IN ('revoked', 'failed', 'abandoned')
               OR credentials IS NULL),
    -- An active grant was minted, and a reader verified read-only.
    CONSTRAINT cloud_folder_grants_active_check
        CHECK (status <> 'active'
               OR (activated_at IS NOT NULL
                   AND (kind <> 'reader' OR verified_at IS NOT NULL))),
    CONSTRAINT cloud_folder_grants_failure_check
        CHECK ((status <> 'failed' OR failure_code IS NOT NULL)
               AND (failure_code IS NULL
                    OR failure_code ~ '^[a-z][a-z0-9_]{0,63}$')),
    CONSTRAINT cloud_folder_grants_revoke_check
        CHECK ((status NOT IN ('revoking', 'revoked', 'failed', 'abandoned')
                OR (revoke_requested_at IS NOT NULL AND revoke_reason IS NOT NULL))
               AND ((status IN ('revoked', 'failed', 'abandoned'))
                    = (revoked_at IS NOT NULL))),
    CONSTRAINT cloud_folder_grants_attempts_check
        CHECK (revoke_attempts >= 0),
    CONSTRAINT cloud_folder_grants_backend_fk
        FOREIGN KEY (backend_instance_id, backend_id)
        REFERENCES public.main_cloud_backend_instances (id, backend_id)
        ON DELETE RESTRICT
);

-- One live grant per connector, member and kind: every mount of that
-- member's sessions reuses it. A revoking one no longer counts, so a
-- re-pointed folder gets its new grant without waiting for the old revoke.
CREATE UNIQUE INDEX uq_cloud_folder_grants_live
    ON public.cloud_folder_grants (datasource_id, user_id, kind)
    WHERE status IN ('minting', 'active');
-- The sweep: revokes due and mints a lost process left behind.
CREATE INDEX idx_cloud_folder_grants_pending
    ON public.cloud_folder_grants (status, revoke_next_at)
    WHERE status IN ('minting', 'revoking');
-- The revoke requests of a project's delete, a member's removal and a
-- user's retirement.
CREATE INDEX idx_cloud_folder_grants_project
    ON public.cloud_folder_grants (project_id, user_id)
    WHERE status IN ('minting', 'active', 'revoking');
CREATE INDEX idx_cloud_folder_grants_user
    ON public.cloud_folder_grants (user_id)
    WHERE status IN ('minting', 'active', 'revoking');

COMMENT ON TABLE public.cloud_folder_grants IS
    'Per-folder Nextcloud principals SRW creates for one project folder '
    'connector, member and kind (reader or writer); project mounts use them '
    'instead of agent-service. Passwords are APP_ENCRYPTION_KEY ciphertexts, '
    'dropped once revoked. No foreign keys to connector, project or user: '
    'the revoke outlives them.';

COMMIT;
