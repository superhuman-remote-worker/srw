-- migration:     0544_automations_live_trigger.sql
-- description:   Live automations: trigger type `live` fires on a session
--                arriving at a primary connector; `job_shape` carries the
--                job's connectors and workspace (a missing `workspace` key
--                means the default, null means none).
-- depends-on:    0543_drop_telephony.sql
-- expected:      < 1s. Small admin table; every constraint lands NOT VALID —
--                0545 validates.
-- locks:         ACCESS EXCLUSIVE on automations for the swap and the new
--                columns (no rewrite: the jsonb default is a constant).
-- transactional: yes

BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';
SET LOCAL idle_in_transaction_session_timeout = '1min';

-- One transaction: no row can be written between the DROP and the ADD. The
-- new set is a strict superset of the old one.
ALTER TABLE automations
    DROP CONSTRAINT IF EXISTS automations_trigger_type_check;

ALTER TABLE automations
    ADD CONSTRAINT automations_trigger_type_check
    CHECK (trigger_type IN ('cron', 'event', 'live')) NOT VALID;

ALTER TABLE automations
    ADD COLUMN IF NOT EXISTS trigger_connector_id uuid;

ALTER TABLE automations
    ADD COLUMN IF NOT EXISTS job_shape jsonb NOT NULL DEFAULT '{}'::jsonb;

-- A connector a live automation listens on cannot be deleted under it.
ALTER TABLE automations
    ADD CONSTRAINT automations_trigger_connector_fk
    FOREIGN KEY (trigger_connector_id) REFERENCES datasources(id)
    ON DELETE RESTRICT NOT VALID;

ALTER TABLE automations
    ADD CONSTRAINT live_trigger_has_connector
    CHECK (trigger_type <> 'live' OR trigger_connector_id IS NOT NULL) NOT VALID;

ALTER TABLE automations
    ADD CONSTRAINT automations_job_shape_object
    CHECK (jsonb_typeof(job_shape) = 'object') NOT VALID;

COMMIT;
