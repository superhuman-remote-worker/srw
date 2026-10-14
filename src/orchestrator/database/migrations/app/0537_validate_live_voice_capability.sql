-- migration:     0537_validate_live_voice_capability.sql
-- description:   Validate the models_capabilities_check constraint 0536 added
--                NOT VALID.
-- depends-on:    0536_live_voice_capability.sql
-- expected:      < 1s. Scans the admin-curated models table (tens of rows);
--                every row satisfied the previous, stricter constraint.
-- locks:         SHARE UPDATE EXCLUSIVE on models; no writes are blocked.
-- transactional: yes

BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';
SET LOCAL idle_in_transaction_session_timeout = '1min';

ALTER TABLE models VALIDATE CONSTRAINT models_capabilities_check;

COMMIT;
