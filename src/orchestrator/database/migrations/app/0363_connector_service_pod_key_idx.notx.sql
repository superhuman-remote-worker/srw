-- migration:     0363_connector_service_pod_key_idx.notx.sql
-- description:   One live service pod per pod key: connector, image digest
--                and credential generation (connector drivers D5, decision
--                7). Two reconciler passes racing to start the same pod
--                collide here instead of starting two.
-- depends-on:    0362_validate_connector_service_pods.sql
-- expected:      < 1s. connector_driver_identities holds a row per driver pod
--                and the predicate indexes only live service pods.
-- locks:         SHARE UPDATE EXCLUSIVE on connector_driver_identities;
--                writes continue. The runner applies non-transactional files
--                after the transactional batch has committed.
-- transactional: no (CREATE INDEX CONCURRENTLY must run outside a transaction)

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS uq_connector_driver_identities_live_service
    ON public.connector_driver_identities (connector_id, image_digest, credential_generation)
    WHERE revoked_at IS NULL AND credential_generation IS NOT NULL;
