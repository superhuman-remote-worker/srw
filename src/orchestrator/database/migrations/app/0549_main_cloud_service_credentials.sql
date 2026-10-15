-- migration:     0549_main_cloud_service_credentials.sql
-- description:   The app passwords SRW's own main-cloud service accounts sign
--                in with (bundled Nextcloud performance, WP1b): one row per
--                installation and login name, today only agent-service. A
--                login password costs Nextcloud a full Argon2 login on every
--                request; an app password costs a cached token lookup. The
--                credential is an APP_ENCRYPTION_KEY ciphertext. token_name is
--                the name Nextcloud lists the token under (the User-Agent of
--                the request that minted it), so a stale one can be found.
--                revision is the compare-and-set counter the orchestrator
--                replicas agree on one token with: the first insert wins, and
--                a replica that re-mints after a 401 replaces the row only at
--                the revision it read. Deleting a row makes the next use mint
--                a new token.
-- depends-on:    0186_protected_cloud_instance_authority.sql
-- expected:      < 1s (one empty table).
-- locks:         catalog locks; SHARE ROW EXCLUSIVE on the small
--                main_cloud_backend_instances table for the foreign key.
-- transactional: yes

BEGIN;
SET LOCAL lock_timeout                        = '2s';
SET LOCAL statement_timeout                   = '30s';
SET LOCAL idle_in_transaction_session_timeout = '5min';

CREATE TABLE public.main_cloud_service_credentials (
    backend_instance_id    UUID NOT NULL
        REFERENCES public.main_cloud_backend_instances (id),
    login_name             TEXT NOT NULL,
    credential_ciphertext  TEXT NOT NULL,
    token_name             TEXT NOT NULL,
    revision               BIGINT NOT NULL DEFAULT 1,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT main_cloud_service_credentials_pkey
        PRIMARY KEY (backend_instance_id, login_name),
    CONSTRAINT main_cloud_service_credentials_revision_check
        CHECK (revision > 0),
    CONSTRAINT main_cloud_service_credentials_login_check
        CHECK (login_name <> ''),
    -- Never a plaintext credential.
    CONSTRAINT main_cloud_service_credentials_ciphertext_check
        CHECK (credential_ciphertext LIKE 'v1:%')
);

COMMENT ON TABLE public.main_cloud_service_credentials IS
    'App passwords of SRW''s own main-cloud service accounts (agent-service), '
    'one per installation and login name, as APP_ENCRYPTION_KEY ciphertexts. '
    'revision is the compare-and-set counter replicas agree on one token with.';

COMMIT;
