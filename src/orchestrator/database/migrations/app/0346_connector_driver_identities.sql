-- migration:     0346_connector_driver_identities.sql
-- description:   Per-driver identities for the credential lease exchange
--                (connector drivers slice C2). A driver pod presents an
--                opaque sdi_ token minted when SRW creates the pod; SRW stores
--                only its SHA-256 digest and reads the binding (connector,
--                driver, pod or image digest) from this row, never from the
--                request. Service-plane hosting (D5) mints one per driver pod
--                and revokes it when the pod stops; the shared internal key
--                is never accepted in its place.
-- depends-on:    0345_validate_pinned_input_admission_count.sql
-- expected:      < 1s (one empty table and its indexes).
-- locks:         catalog locks; SHARE ROW EXCLUSIVE on datasources for the FK.
-- transactional: yes

BEGIN;
SET LOCAL lock_timeout                        = '2s';
SET LOCAL statement_timeout                   = '15min';
SET LOCAL idle_in_transaction_session_timeout = '5min';
SET LOCAL timezone                            = 'UTC';

CREATE TABLE public.connector_driver_identities (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    token_hash       BYTEA NOT NULL UNIQUE,
    token_last_four  TEXT NOT NULL,
    connector_id     UUID NOT NULL
                         REFERENCES public.datasources(id) ON DELETE CASCADE,
    driver           TEXT NOT NULL,
    image_digest     TEXT,
    pod_namespace    TEXT,
    pod_name         TEXT,
    pod_uid          TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_used_at     TIMESTAMPTZ,
    revoked_at       TIMESTAMPTZ,
    revoke_reason    TEXT,
    CONSTRAINT connector_driver_identities_hash_check
        CHECK (octet_length(token_hash) = 32),
    CONSTRAINT connector_driver_identities_last_four_check
        CHECK (char_length(token_last_four) = 4),
    CONSTRAINT connector_driver_identities_driver_check
        CHECK (driver <> ''),
    CONSTRAINT connector_driver_identities_digest_check
        CHECK (image_digest IS NULL OR image_digest ~ '^sha256:[0-9a-f]{64}$'),
    CONSTRAINT connector_driver_identities_revoke_check
        CHECK ((revoked_at IS NULL) = (revoke_reason IS NULL))
);

CREATE INDEX idx_connector_driver_identities_connector
    ON public.connector_driver_identities (connector_id);
CREATE INDEX idx_connector_driver_identities_pod
    ON public.connector_driver_identities (pod_uid)
    WHERE revoked_at IS NULL AND pod_uid IS NOT NULL;

COMMENT ON TABLE public.connector_driver_identities IS
    'Connector driver identities (sdi_ tokens, SHA-256 only). The lease '
    'exchange authenticates a driver by one of these plus a lease token; '
    'the binding is read from the row, never from the request.';

COMMIT;
