-- migration:     0391_connector_service_pod_key_v2_idx.notx.sql
-- description:   One live service pod per pod key among the pods not being
--                replaced (connector drivers D5a). A re-pin starts a
--                replacement for the same connector, image digest and
--                credential generation while the old pod (replaced_at set)
--                still serves; two reconciler passes racing to start the same
--                pod still collide here. 0392 drops 0363's index, which would
--                refuse the replacement.
-- depends-on:    0390_connector_service_pod_replacement.sql
-- expected:      < 1s. connector_driver_identities holds a row per driver pod
--                and the predicate indexes only live service pods.
-- locks:         SHARE UPDATE EXCLUSIVE on connector_driver_identities;
--                writes continue. The runner applies non-transactional files
--                after the transactional batch has committed.
-- transactional: no (CREATE INDEX CONCURRENTLY must run outside a transaction)
-- rollout:       Deliberately NOT "IF NOT EXISTS", following 0132's and
--                0203's runbook: IF NOT EXISTS reports success against an
--                INVALID same-name shell left by a failed concurrent build,
--                which would record 0391 as applied while the pod key went
--                unenforced (and 0392 then drops the only index that held
--                it). The runner re-reads schema_migrations and skips an
--                applied .notx migration, so the clause buys nothing on the
--                success path. A failed build is repaired explicitly:
--                    SELECT i.indisvalid, i.indisready
--                      FROM pg_index AS i
--                      JOIN pg_class AS c ON c.oid = i.indexrelid
--                     WHERE c.relname = 'uq_connector_driver_identities_serving_key';
--                DROP INDEX CONCURRENTLY the invalid shell, repair the dirty
--                ledger row, then re-run. A duplicate key is not cosmetic:
--                two pods of one key would both hold a live identity and the
--                endpoint Service would serve from either.
--                This index IS the uniqueness enforcement; there is no
--                follow-up constraint migration to adopt it. The squawk-ignore
--                below acknowledges exactly that prefer-robust-stmts
--                trade-off (as 0207 does).

-- squawk-ignore prefer-robust-stmts
CREATE UNIQUE INDEX CONCURRENTLY uq_connector_driver_identities_serving_key
    ON public.connector_driver_identities (connector_id, image_digest, credential_generation)
    WHERE revoked_at IS NULL AND credential_generation IS NOT NULL AND replaced_at IS NULL;
