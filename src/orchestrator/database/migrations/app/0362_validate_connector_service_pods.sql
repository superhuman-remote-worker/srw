-- migration:     0362_validate_connector_service_pods.sql
-- description:   Validate the CHECKs 0361 added NOT VALID.
-- depends-on:    0361_connector_service_pods.sql
-- expected:      < 1s. One scan of connector_driver_identities (a row per
--                driver pod) per constraint; every new column is NULL.
-- locks:         SHARE UPDATE EXCLUSIVE on connector_driver_identities when
--                this file runs on its own; in the same runner transaction as
--                0361, it runs under 0361's lock and waits for nothing.
-- transactional: yes
--
-- Idempotent: validating a constraint that is already valid is a no-op.
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '15min';
SET LOCAL idle_in_transaction_session_timeout = '5min';

ALTER TABLE public.connector_driver_identities
    VALIDATE CONSTRAINT connector_driver_identities_service_pod_check;
ALTER TABLE public.connector_driver_identities
    VALIDATE CONSTRAINT connector_driver_identities_egress_check;
ALTER TABLE public.connector_driver_identities
    VALIDATE CONSTRAINT connector_driver_identities_removed_check;

COMMIT;
