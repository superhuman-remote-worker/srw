-- migration:     0545_validate_automations_live_trigger.sql
-- description:   Validate the constraints 0544 added NOT VALID.
-- depends-on:    0544_automations_live_trigger.sql
-- expected:      < 1s; every existing row is cron or event with an empty
--                job shape.
-- locks:         SHARE UPDATE EXCLUSIVE on automations; ROW SHARE on
--                datasources for the foreign key. No writes are blocked.
-- transactional: yes

BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';
SET LOCAL idle_in_transaction_session_timeout = '1min';

ALTER TABLE automations VALIDATE CONSTRAINT automations_trigger_type_check;
ALTER TABLE automations VALIDATE CONSTRAINT automations_trigger_connector_fk;
ALTER TABLE automations VALIDATE CONSTRAINT live_trigger_has_connector;
ALTER TABLE automations VALIDATE CONSTRAINT automations_job_shape_object;

COMMIT;
