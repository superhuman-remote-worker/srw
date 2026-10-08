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

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS uq_connector_driver_identities_serving_key
    ON public.connector_driver_identities (connector_id, image_digest, credential_generation)
    WHERE revoked_at IS NULL AND credential_generation IS NOT NULL AND replaced_at IS NULL;
