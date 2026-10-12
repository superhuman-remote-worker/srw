-- migration:     0390_connector_service_pod_replacement.sql
-- description:   Service pods replaced after a re-pin (connector drivers D5a,
--                a prerequisite from the D5 review). A shared service pod
--                that never goes idle re-resolves its pinned egress hosts on a
--                timer; when an address set changed, the reconciler starts a
--                replacement for the same pod key (connector, digest,
--                credential generation) with the new NetworkPolicy and
--                hostAliases, moves the connector's endpoint Service to it
--                once it is ready, and stops the old pod after a short drain.
--                replaced_at marks the old pod while both are live; 0391 makes
--                the pod key unique among pods not being replaced, and 0392
--                drops 0363's index, which counted both.
-- depends-on:    0380_project_connector_defaults.sql
-- expected:      < 1s. One nullable column without a default (catalog-only on
--                PG 11+) on a small table.
-- locks:         ACCESS EXCLUSIVE on connector_driver_identities, brief,
--                retried with backoff on lock timeout or deadlock.
-- transactional: yes
--
-- Idempotent (IF NOT EXISTS).
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
                ADD COLUMN IF NOT EXISTS replaced_at TIMESTAMPTZ;
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

COMMENT ON COLUMN public.connector_driver_identities.replaced_at IS
    'Service pods only: when the reconciler started a replacement for this pod because a pinned egress host now resolves to other addresses. The replacement holds the pod key; this pod keeps serving until the replacement is ready, then stops after a short drain (reason egress_repinned).';

COMMIT;
