-- migration:     0352_validate_datasource_manifest_identity.sql
-- description:   Validate the two constraints 0350 added NOT VALID.
-- depends-on:    0351_resource_platform_managed.sql
-- expected:      < 30s. One scan of datasources per constraint under SHARE
--                UPDATE EXCLUSIVE; reads and writes continue. Both columns
--                arrive all-NULL from 0350, and the write-through stores only
--                keys of the checked shape and ids of existing resources.
-- locks:         SHARE UPDATE EXCLUSIVE on datasources; ROW SHARE on
--                srw_resources for the foreign key.
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '15min';
SET LOCAL idle_in_transaction_session_timeout = '5min';

ALTER TABLE public.datasources
    VALIDATE CONSTRAINT datasources_manifest_resource_id_fkey;
ALTER TABLE public.datasources
    VALIDATE CONSTRAINT datasources_managed_key_shape;

COMMIT;
