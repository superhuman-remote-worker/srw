-- migration:     0350_datasource_manifest_identity.sql
-- description:   Datasources become Connectors (connector_drivers.md, slice
--                D3a): each row gains a reference to the manifest Connector
--                resource it is written through to and the general
--                platform-owned marker; each resource gains the resource side
--                of that marker and the row version it was last written from.
-- depends-on:    0345_validate_pinned_input_admission_count.sql
-- expected:      < 1s. Nullable columns without a default (catalog-only on
--                PG 11+), a CHECK and a foreign key added NOT VALID; 0351
--                validates both.
-- locks:         ACCESS EXCLUSIVE on srw_resources, then on datasources, by
--                one LOCK TABLE at the start of each attempt, before any
--                ALTER. The runner applies every pending transactional file in
--                one transaction, so once an attempt succeeds both locks are
--                held until that transaction commits: through 0351 and any
--                later pending transactional file. Within an attempt the file
--                may hold srw_resources while it waits for datasources; if
--                that wait passes lock_timeout or deadlocks with an
--                application transaction (a manifest apply writes
--                srw_resources and then reads datasources; the datasource
--                write-through does the reverse), the attempt is rolled back,
--                which releases what it got, and retried with backoff. No
--                lock this file takes is held while it sleeps, and a lost
--                attempt leaves no dirty ledger row.
-- transactional: yes
--
-- datasources.manifest_resource_id follows experts and projects (0234): the
-- srw_resources row holding the connector's definition. For a datasource the
-- resource uid is the datasource id itself, so every stored selection already
-- names its Connector; the column marks the row as written through. There is
-- no SQL backfill: migrate_stored_connectors builds the resources in Python,
-- because it reads the encrypted credentials.
--
-- datasources.managed_key is the platform-owned marker, after
-- experts.managed_key (0064): project-kb:<project> for a project's own
-- knowledge base today, later project-cloud:<project> and
-- user-cloud-root:<user>. It replaces the config.native_project_id marker,
-- which stays as a mirror for one release because SQL predicates still read
-- it. 0352 makes the key unique.
--
-- srw_resources.platform_managed carries that key on the resource. It is
-- deliberately not installation_managed (0307), which belongs to the startup
-- reconciler and the chart, and not managed_by alone: a Project apply retires
-- the managed children it omits. Only the datasource write-through writes a
-- marked row; the resource API refuses to edit or delete one.
--
-- srw_resources.linked_updated_at is the linked row's updated_at the resource
-- was last written from: the startup backfill compares it with the row's,
-- by inequality, so a write that bypassed the write-through is found even when
-- its transaction began before the resource's.
--
-- Every statement is idempotent (IF NOT EXISTS, or a pg_constraint check), so
-- applying the file again after a ledger row was deleted by hand is a no-op.
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
            LOCK TABLE public.srw_resources, public.datasources
                IN ACCESS EXCLUSIVE MODE;
            ALTER TABLE public.datasources
                ADD COLUMN IF NOT EXISTS manifest_resource_id UUID,
                ADD COLUMN IF NOT EXISTS managed_key TEXT;
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conrelid = 'public.datasources'::regclass
                  AND conname = 'datasources_managed_key_shape'
            ) THEN
                ALTER TABLE public.datasources
                    ADD CONSTRAINT datasources_managed_key_shape CHECK (
                        managed_key IS NULL
                        OR managed_key ~ '^[a-z][a-z0-9-]{0,62}:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                    ) NOT VALID;
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conrelid = 'public.datasources'::regclass
                  AND conname = 'datasources_manifest_resource_id_fkey'
            ) THEN
                ALTER TABLE public.datasources
                    ADD CONSTRAINT datasources_manifest_resource_id_fkey
                    FOREIGN KEY (manifest_resource_id)
                    REFERENCES public.srw_resources(id)
                    ON DELETE RESTRICT NOT VALID;
            END IF;
            ALTER TABLE public.srw_resources
                ADD COLUMN IF NOT EXISTS platform_managed TEXT,
                ADD COLUMN IF NOT EXISTS linked_updated_at TIMESTAMPTZ;
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

COMMENT ON COLUMN public.datasources.manifest_resource_id IS
    'The manifest Connector resource this row is written through to; its uid is the datasource id. NULL while the row stays on the legacy path (an ownerless row not linked to exactly one project, a legacy job clone, a type no driver serves); after its project was deleted it can name a retired resource.';
COMMENT ON COLUMN public.datasources.managed_key IS
    'Stable platform identity of a connector SRW provisions itself (project-kb:<project>, ...). Platform-owned rows refuse policy edits, deletes, links and unlinks through the API.';
COMMENT ON COLUMN public.srw_resources.platform_managed IS
    'Managed key of the platform-owned domain row this resource mirrors (project-kb:<project>, ...). Only the platform write-through writes it; the API refuses edits and deletes.';
COMMENT ON COLUMN public.srw_resources.linked_updated_at IS
    'The linked domain row''s updated_at this resource was last written from. A row whose updated_at differs needs its resource rewritten.';

COMMIT;
