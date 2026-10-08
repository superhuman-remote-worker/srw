-- migration:     0351_validate_datasource_manifest_identity.sql
-- description:   Validate the CHECK and the foreign key 0350 added NOT VALID.
-- depends-on:    0350_datasource_manifest_identity.sql
-- expected:      < 30s. One scan of datasources per constraint. Both columns
--                arrive all-NULL from 0350, and the write-through stores only
--                keys of the checked shape and ids of existing resources.
-- locks:         SHARE UPDATE EXCLUSIVE on datasources and ROW SHARE on
--                srw_resources when this file runs on its own. Applied in the
--                same runner transaction as 0350, it runs under 0350's ACCESS
--                EXCLUSIVE locks on both tables and waits for nothing.
-- transactional: yes
--
-- Idempotent: validating a constraint that is already valid is a no-op.
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '15min';
SET LOCAL idle_in_transaction_session_timeout = '5min';

ALTER TABLE public.datasources
    VALIDATE CONSTRAINT datasources_manifest_resource_id_fkey;
ALTER TABLE public.datasources
    VALIDATE CONSTRAINT datasources_managed_key_shape;

COMMIT;
