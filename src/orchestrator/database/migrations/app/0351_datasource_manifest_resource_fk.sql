-- migration:     0351_datasource_manifest_resource_fk.sql
-- description:   The foreign key from datasources.manifest_resource_id (0350)
--                to srw_resources, added NOT VALID; 0353 validates it.
-- depends-on:    0350_datasource_manifest_identity.sql
-- expected:      < 1s. NOT VALID skips the scan; the column arrives all-NULL.
-- locks:         SHARE ROW EXCLUSIVE on datasources and srw_resources (brief,
--                retried with backoff). In its own file so 0350's ACCESS
--                EXCLUSIVE lock on datasources is never held while this one
--                waits on srw_resources.
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout                        = '2s';
SET LOCAL statement_timeout                   = '15min';
SET LOCAL idle_in_transaction_session_timeout = '5min';

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
            ALTER TABLE public.datasources
                ADD CONSTRAINT datasources_manifest_resource_id_fkey
                FOREIGN KEY (manifest_resource_id) REFERENCES public.srw_resources(id)
                ON DELETE RESTRICT NOT VALID;
            done := true;
            EXIT;
        EXCEPTION WHEN lock_not_available THEN
            delay_ms := round(random() * least(cap_ms, base_ms * 2 ^ i));
            PERFORM pg_sleep(delay_ms::numeric / 1000);
        END;
    END LOOP;
    IF NOT done THEN
        RAISE EXCEPTION 'lock acquisition failed after % attempts', max_attempts;
    END IF;
END $$;

COMMIT;
