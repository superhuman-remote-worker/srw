-- migration:     0352_datasources_managed_key_idx.notx.sql
-- description:   One row per platform identity: a managed key names at most
--                one connector (the precedent is uq_experts_managed_key,
--                0065). The write-through and the startup backfill stamp a
--                key only when no other row holds it.
-- depends-on:    0351_validate_datasource_manifest_identity.sql
-- expected:      < 1s. datasources holds one row per configured connector and
--                the partial predicate indexes only the platform-owned ones.
-- locks:         SHARE UPDATE EXCLUSIVE on datasources; writes continue. The
--                runner applies non-transactional files after the
--                transactional batch has committed, so 0350's locks are gone.
-- transactional: no (CREATE INDEX CONCURRENTLY must run outside a transaction)
--
-- Idempotent through IF NOT EXISTS. Recovery: an interrupted CONCURRENTLY
-- build leaves an INVALID index behind, and IF NOT EXISTS then silently
-- no-ops on a rerun. Recover with
--     DROP INDEX CONCURRENTLY IF EXISTS uq_datasources_managed_key;
-- then rerun. Detect it with
--     SELECT indexrelid::regclass FROM pg_index
--     WHERE NOT indisvalid
--       AND indexrelid::regclass::text = 'uq_datasources_managed_key';

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS uq_datasources_managed_key
    ON datasources (managed_key)
    WHERE managed_key IS NOT NULL;
