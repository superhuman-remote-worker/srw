-- migration:     0350_datasource_manifest_identity.sql
-- description:   Datasources become Connectors (connector_drivers.md, slice
--                D3a): each row gains a reference to the manifest Connector
--                resource it is written through to, and the general
--                platform-owned marker.
-- depends-on:    0345_validate_pinned_input_admission_count.sql
-- expected:      < 1s. Two nullable columns without a default (catalog-only
--                on PG 11+) and a CHECK added NOT VALID; 0353 validates it.
-- locks:         AccessExclusiveLock on datasources (brief, retried with
--                backoff). The foreign key, which also locks srw_resources,
--                is 0351's, so this lock never waits on that table.
-- transactional: yes
--
-- manifest_resource_id follows experts and projects (0234): the
-- srw_resources row holding the connector's definition. For a datasource the
-- resource uid is the datasource id itself, so every stored selection already
-- names its Connector; the column marks the row as written through. There is
-- no SQL backfill: migrate_stored_connectors builds the resources in Python,
-- because it reads the encrypted credentials.
--
-- managed_key is the platform-owned marker, after experts.managed_key (0064):
-- project-kb:<project> for a project's own knowledge base today, later
-- project-cloud:<project> and user-cloud-root:<user>. It replaces the
-- config.native_project_id marker, which stays as a mirror for one release
-- because SQL predicates still read it. 0354 makes the key unique.
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
                ADD COLUMN IF NOT EXISTS manifest_resource_id UUID,
                ADD COLUMN IF NOT EXISTS managed_key TEXT,
                ADD CONSTRAINT datasources_managed_key_shape CHECK (
                    managed_key IS NULL
                    OR managed_key ~ '^[a-z][a-z0-9-]{0,62}:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                ) NOT VALID;
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

COMMENT ON COLUMN public.datasources.manifest_resource_id IS
    'The manifest Connector resource this row is written through to; its uid is the datasource id. NULL while the row stays on the legacy path (an ownerless row not linked to exactly one project, a legacy job clone, a type no driver serves); after its project was deleted it can name a retired resource.';
COMMENT ON COLUMN public.datasources.managed_key IS
    'Stable platform identity of a connector SRW provisions itself (project-kb:<project>, ...). Platform-owned rows refuse policy edits, deletes, links and unlinks through the API.';

COMMIT;
