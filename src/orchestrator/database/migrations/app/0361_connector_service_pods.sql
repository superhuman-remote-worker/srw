-- migration:     0361_connector_service_pods.sql
-- description:   Service-plane driver pods (connector drivers D5). A shared
--                service pod is one connector_driver_identities row: its
--                sdi_ identity, minted before the pod is created, plus the
--                pod's lifecycle. The pod key is (connector_id,
--                image_digest, credential_generation); credential_generation
--                is a keyed fingerprint of what the pod's immutable Secret
--                holds (never the secret itself) and is NULL on identities
--                that back no service pod (a gate's fictitious driver). The
--                leader-gated reconciler starts a pod for a key bindings
--                need, records its pinned egress and readiness, marks it idle
--                when its last binding ends, revokes the identity when it
--                stops and records when its Kubernetes objects were seen
--                gone. 0362 validates the CHECKs; 0363 makes the key unique
--                among live pods.
-- depends-on:    0360_connector_driver_images.sql
-- expected:      < 1s. Nullable columns without a default (catalog-only on
--                PG 11+) and CHECKs added NOT VALID on a small table.
-- locks:         ACCESS EXCLUSIVE on connector_driver_identities, brief,
--                retried with backoff on lock timeout or deadlock.
-- transactional: yes
--
-- Every statement is idempotent (IF NOT EXISTS, or a pg_constraint check).
BEGIN;
SET LOCAL lock_timeout                        = '2s';
SET LOCAL statement_timeout                   = '15min';
SET LOCAL idle_in_transaction_session_timeout = '5min';
SET LOCAL timezone                            = 'UTC';

DO $$
DECLARE
    max_attempts CONSTANT int := 30;
    cap_ms       CONSTANT bigint := 60000;
    base_ms      CONSTANT bigint := 10;
    delay_ms              bigint;
    done                  boolean := false;
BEGIN
    FOR i IN 1..max_attempts LOOP
        BEGIN
            LOCK TABLE public.connector_driver_identities IN ACCESS EXCLUSIVE MODE;
            ALTER TABLE public.connector_driver_identities
                ADD COLUMN IF NOT EXISTS credential_generation TEXT,
                ADD COLUMN IF NOT EXISTS image_reference TEXT,
                ADD COLUMN IF NOT EXISTS egress JSONB,
                ADD COLUMN IF NOT EXISTS egress_resolved_at TIMESTAMPTZ,
                ADD COLUMN IF NOT EXISTS ready_at TIMESTAMPTZ,
                ADD COLUMN IF NOT EXISTS last_bound_at TIMESTAMPTZ,
                ADD COLUMN IF NOT EXISTS idle_since TIMESTAMPTZ,
                ADD COLUMN IF NOT EXISTS removed_at TIMESTAMPTZ,
                ADD COLUMN IF NOT EXISTS launch_error TEXT;
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conrelid = 'public.connector_driver_identities'::regclass
                  AND conname = 'connector_driver_identities_service_pod_check'
            ) THEN
                -- A service pod names its digest, its pod and its reference.
                ALTER TABLE public.connector_driver_identities
                    ADD CONSTRAINT connector_driver_identities_service_pod_check
                    CHECK (
                        credential_generation IS NULL
                        OR (
                            credential_generation <> ''
                            AND image_digest IS NOT NULL
                            AND pod_namespace IS NOT NULL
                            AND pod_name IS NOT NULL
                            AND image_reference IS NOT NULL
                        )
                    ) NOT VALID;
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conrelid = 'public.connector_driver_identities'::regclass
                  AND conname = 'connector_driver_identities_egress_check'
            ) THEN
                ALTER TABLE public.connector_driver_identities
                    ADD CONSTRAINT connector_driver_identities_egress_check
                    CHECK (
                        (egress IS NULL OR jsonb_typeof(egress) = 'object')
                        AND (egress IS NULL) = (egress_resolved_at IS NULL)
                    ) NOT VALID;
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conrelid = 'public.connector_driver_identities'::regclass
                  AND conname = 'connector_driver_identities_removed_check'
            ) THEN
                -- Objects are only ever removed for a stopped pod.
                ALTER TABLE public.connector_driver_identities
                    ADD CONSTRAINT connector_driver_identities_removed_check
                    CHECK (removed_at IS NULL OR revoked_at IS NOT NULL) NOT VALID;
            END IF;
            done := true;
            EXIT;
        EXCEPTION WHEN lock_not_available OR deadlock_detected THEN
            delay_ms := round(random() * least(cap_ms, base_ms * 2 ^ i));
            PERFORM pg_sleep(delay_ms::numeric / 1000);
        END;
    END LOOP;
    IF NOT done THEN
        RAISE EXCEPTION 'lock acquisition failed after % attempts', max_attempts;
    END IF;
END $$;

COMMENT ON COLUMN public.connector_driver_identities.credential_generation IS
    'Service pods only: a keyed fingerprint (hmac-sha256) of what the pod''s immutable Secret holds and of its egress tier, part of the pod key with connector_id and image_digest. A change starts a new pod; the old one drains.';
COMMENT ON COLUMN public.connector_driver_identities.egress IS
    'The pod''s pinned egress: each declared host with the addresses written into its NetworkPolicy and hostAliases, and the DNS status. egress_resolved_at is when they were resolved.';
COMMENT ON COLUMN public.connector_driver_identities.idle_since IS
    'When the pod''s last binding ended (or a newer pod superseded it); the reconciler stops it after the idle timeout.';
COMMENT ON COLUMN public.connector_driver_identities.removed_at IS
    'When the pod''s Kubernetes objects were seen gone, after its identity was revoked. A live row with removed_at NULL counts against the installation cap.';

COMMIT;
