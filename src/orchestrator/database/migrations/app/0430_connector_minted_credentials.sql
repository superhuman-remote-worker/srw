-- migration:     0430_connector_minted_credentials.sql
-- description:   Provider-minted connector credentials (connector drivers
--                slice C5, "Three ways to give an agent ephemeral
--                authority", item 1). One row per credential SRW minted at a
--                provider for a workspace-owning execution (a Job or a
--                session thread, or a connector's Test) and a connector: a
--                Kubernetes TokenRequest token bound to a per-row Secret, or
--                a GitHub App installation token. It holds the minted token
--                and what its revoke needs (the bound Secret and the minting
--                credential, or the API base) as APP_ENCRYPTION_KEY
--                ciphertexts, its expiry and its lifecycle: minting, live
--                (the one an execution's deliveries hand out), superseded (a
--                newer one is live; it lapses at its own expiry), revoking,
--                revoked, and abandoned (SRW gave up the revoke; what it
--                named may be left at the provider). A revoked or abandoned
--                row keeps no token and no minting credential. Owners and
--                connectors carry no foreign key, as
--                connector_bind_time_bindings do: a revoke at the provider
--                still runs after the job, thread or connector is gone.
-- depends-on:    0420_connector_driver_registrations.sql
-- expected:      < 1s (one empty table and its indexes).
-- locks:         catalog locks only (no foreign key to an existing table).
-- transactional: yes

BEGIN;
SET LOCAL lock_timeout                        = '2s';
SET LOCAL statement_timeout                   = '15min';
SET LOCAL idle_in_transaction_session_timeout = '5min';
SET LOCAL timezone                            = 'UTC';

CREATE TABLE public.connector_minted_credentials (
    id                     UUID PRIMARY KEY,
    owner_kind             TEXT NOT NULL,
    owner_id               UUID NOT NULL,
    connector_id           UUID NOT NULL,
    provider               TEXT NOT NULL,
    -- The provider's host[:port] (the API server's, GitHub's API's): the
    -- revoke sweep takes one row per host at a time, so one dead host
    -- never holds every revoke slot. Not secret.
    provider_host          TEXT NOT NULL,
    access                 TEXT NOT NULL,
    config_digest          TEXT NOT NULL,
    status                 TEXT NOT NULL DEFAULT 'minting',
    material_ciphertext    TEXT NOT NULL,
    token_ciphertext       TEXT,
    token_last_four        TEXT,
    expires_at             TIMESTAMPTZ,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    minted_at              TIMESTAMPTZ,
    superseded_at          TIMESTAMPTZ,
    -- When a GitHub App token went into a workspace's clone URL (the
    -- token-in-URL fallback): once the git swap driver serves the
    -- connector, that token is revoked.
    delivered_at           TIMESTAMPTZ,
    revoke_requested_at    TIMESTAMPTZ,
    revoke_reason          TEXT,
    revoke_attempts        INTEGER NOT NULL DEFAULT 0,
    revoke_next_at         TIMESTAMPTZ,
    revoked_at             TIMESTAMPTZ,
    revoke_error           TEXT,
    CONSTRAINT connector_minted_credentials_owner_check
        CHECK (owner_kind IN ('job', 'thread', 'test')),
    CONSTRAINT connector_minted_credentials_provider_check
        CHECK (provider IN ('kubernetes', 'github_app')),
    CONSTRAINT connector_minted_credentials_access_check
        CHECK (access <> ''),
    CONSTRAINT connector_minted_credentials_digest_check
        CHECK (config_digest ~ '^sha256:[0-9a-f]{64}$'),
    CONSTRAINT connector_minted_credentials_status_check
        CHECK (status IN ('minting', 'live', 'superseded', 'revoking', 'revoked',
                          'abandoned')),
    CONSTRAINT connector_minted_credentials_material_check
        CHECK (material_ciphertext <> ''),
    CONSTRAINT connector_minted_credentials_last_four_check
        CHECK (token_last_four IS NULL OR char_length(token_last_four) = 4),
    -- A live or superseded credential was minted: its token and expiry are
    -- known. A done one keeps no token.
    CONSTRAINT connector_minted_credentials_minted_check
        CHECK (status NOT IN ('live', 'superseded')
               OR (token_ciphertext IS NOT NULL AND expires_at IS NOT NULL
                   AND minted_at IS NOT NULL)),
    CONSTRAINT connector_minted_credentials_done_check
        CHECK (status NOT IN ('revoked', 'abandoned') OR token_ciphertext IS NULL),
    CONSTRAINT connector_minted_credentials_superseded_check
        CHECK (status <> 'superseded' OR superseded_at IS NOT NULL),
    CONSTRAINT connector_minted_credentials_revoke_check
        CHECK ((status NOT IN ('revoking', 'revoked', 'abandoned')
                OR (revoke_requested_at IS NOT NULL AND revoke_reason IS NOT NULL))
               AND ((status IN ('revoked', 'abandoned')) = (revoked_at IS NOT NULL))),
    CONSTRAINT connector_minted_credentials_attempts_check
        CHECK (revoke_attempts >= 0)
);

-- One live credential per execution and connector: every delivery hands out
-- the same one until it is past half its lifetime.
CREATE UNIQUE INDEX uq_connector_minted_credentials_live
    ON public.connector_minted_credentials (owner_kind, owner_id, connector_id)
    WHERE status = 'live';
-- The sweep: revokes due, mints abandoned, credentials past their expiry,
-- and whether anything is left to do at all.
CREATE INDEX idx_connector_minted_credentials_pending
    ON public.connector_minted_credentials (status, revoke_next_at)
    WHERE status IN ('minting', 'revoking');
CREATE INDEX idx_connector_minted_credentials_expiry
    ON public.connector_minted_credentials (expires_at)
    WHERE status IN ('live', 'superseded');
-- The revoke requests of an execution's end and of a connector's change.
CREATE INDEX idx_connector_minted_credentials_owner
    ON public.connector_minted_credentials (owner_kind, owner_id)
    WHERE status NOT IN ('revoked', 'abandoned');
CREATE INDEX idx_connector_minted_credentials_connector
    ON public.connector_minted_credentials (connector_id)
    WHERE status NOT IN ('revoked', 'abandoned');
-- Retention of done rows.
CREATE INDEX idx_connector_minted_credentials_done
    ON public.connector_minted_credentials (revoked_at)
    WHERE status IN ('revoked', 'abandoned');

COMMENT ON TABLE public.connector_minted_credentials IS
    'Credentials SRW minted at a provider for one execution (or a Test) and '
    'connector (C5): a Kubernetes TokenRequest token bound to a per-row '
    'Secret, or a GitHub App installation token. Token and revoke inputs are '
    'APP_ENCRYPTION_KEY ciphertexts, dropped once revoked or abandoned. No '
    'foreign keys: the revoke outlives the execution and the connector.';

COMMIT;
