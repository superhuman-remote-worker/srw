-- migration:     0347_connector_credential_leases.sql
-- description:   Credential leases (connector drivers slice C2, "The lease
--                service"). One live lease per workspace-owning execution
--                (a Job or a session thread) and connector. The scl_ token
--                is stored as a SHA-256 digest for lookup plus an
--                APP_ENCRYPTION_KEY ciphertext so SRW can deliver the same
--                token again on every claim, attach and pod recycle instead
--                of minting a lease per turn. A lease expires a TTL after its
--                last renewal; only the leader-gated sweeper renews it, and
--                only while its execution is live in durable state. The
--                owner foreign keys cascade like runtime_actor_grants, so
--                every delete path revokes first.
-- depends-on:    0346_connector_driver_identities.sql
-- expected:      < 1s (one empty table and its indexes).
-- locks:         catalog locks; SHARE ROW EXCLUSIVE on jobs, threads and
--                datasources for the FKs.
-- transactional: yes

BEGIN;
SET LOCAL lock_timeout                        = '2s';
SET LOCAL statement_timeout                   = '15min';
SET LOCAL idle_in_transaction_session_timeout = '5min';
SET LOCAL timezone                            = 'UTC';

CREATE TABLE public.connector_credential_leases (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    token_hash         BYTEA NOT NULL UNIQUE,
    token_ciphertext   TEXT NOT NULL,
    token_last_four    TEXT NOT NULL,
    job_id             UUID REFERENCES public.jobs(id) ON DELETE CASCADE,
    thread_id          UUID REFERENCES public.threads(id) ON DELETE CASCADE,
    connector_id       UUID NOT NULL
                           REFERENCES public.datasources(id) ON DELETE CASCADE,
    driver             TEXT NOT NULL,
    image_digest       TEXT,
    access             TEXT NOT NULL,
    issued_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at         TIMESTAMPTZ NOT NULL,
    last_renewed_at    TIMESTAMPTZ,
    last_exchanged_at  TIMESTAMPTZ,
    exchange_count     BIGINT NOT NULL DEFAULT 0,
    revoked_at         TIMESTAMPTZ,
    revoke_reason      TEXT,
    CONSTRAINT connector_credential_leases_owner_check
        CHECK (num_nonnulls(job_id, thread_id) = 1),
    CONSTRAINT connector_credential_leases_hash_check
        CHECK (octet_length(token_hash) = 32),
    CONSTRAINT connector_credential_leases_ciphertext_check
        CHECK (token_ciphertext <> ''),
    CONSTRAINT connector_credential_leases_last_four_check
        CHECK (char_length(token_last_four) = 4),
    CONSTRAINT connector_credential_leases_driver_check
        CHECK (driver <> ''),
    CONSTRAINT connector_credential_leases_access_check
        CHECK (access <> ''),
    CONSTRAINT connector_credential_leases_digest_check
        CHECK (image_digest IS NULL OR image_digest ~ '^sha256:[0-9a-f]{64}$'),
    CONSTRAINT connector_credential_leases_window_check
        CHECK (expires_at > issued_at),
    CONSTRAINT connector_credential_leases_count_check
        CHECK (exchange_count >= 0),
    CONSTRAINT connector_credential_leases_revoke_check
        CHECK ((revoked_at IS NULL) = (revoke_reason IS NULL))
);

-- One live lease per execution and connector. An expired lease is retired
-- (revoked_at = its expiry, reason 'expired') before a new one is issued.
CREATE UNIQUE INDEX uq_connector_credential_leases_live_job
    ON public.connector_credential_leases (job_id, connector_id)
    WHERE revoked_at IS NULL AND job_id IS NOT NULL;
CREATE UNIQUE INDEX uq_connector_credential_leases_live_thread
    ON public.connector_credential_leases (thread_id, connector_id)
    WHERE revoked_at IS NULL AND thread_id IS NOT NULL;
-- The sweeper's scan and the owner/connector cascades.
CREATE INDEX idx_connector_credential_leases_live_expiry
    ON public.connector_credential_leases (expires_at)
    WHERE revoked_at IS NULL;
CREATE INDEX idx_connector_credential_leases_job
    ON public.connector_credential_leases (job_id)
    WHERE job_id IS NOT NULL;
CREATE INDEX idx_connector_credential_leases_thread
    ON public.connector_credential_leases (thread_id)
    WHERE thread_id IS NOT NULL;
CREATE INDEX idx_connector_credential_leases_connector
    ON public.connector_credential_leases (connector_id);

COMMENT ON TABLE public.connector_credential_leases IS
    'Connector credential leases (scl_ tokens). token_hash is the lookup key; '
    'token_ciphertext lets SRW deliver the same token again. Renewed only by '
    'the server-side sweeper while the owning execution is live.';

COMMIT;
