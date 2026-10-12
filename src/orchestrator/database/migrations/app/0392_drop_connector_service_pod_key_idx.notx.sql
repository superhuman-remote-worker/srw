-- migration:     0392_drop_connector_service_pod_key_idx.notx.sql
-- description:   Drop 0363's pod-key index (connector drivers D5a). It counts
--                a pod being replaced, so it would refuse the replacement a
--                re-pin starts; 0391's index keeps the key unique among the
--                pods not being replaced.
-- depends-on:    0391_connector_service_pod_key_v2_idx.notx.sql
-- expected:      < 1s. DROP INDEX CONCURRENTLY waits out users of the index
--                without blocking writes.
-- locks:         ShareUpdateExclusiveLock only (CONCURRENTLY).
-- transactional: NO (.notx -- DROP INDEX CONCURRENTLY cannot run in a txn)

DROP INDEX CONCURRENTLY IF EXISTS uq_connector_driver_identities_live_service;
