-- migration:     0559_harness_image_columns.sql
-- description:   agents.image records the image a pod reports at
--                registration; jobs.harness_image records the image a Job
--                needs (NULL = the installed default image), so dispatch can
--                pair them.
-- depends-on:    0558_stateless_lite_upgrade_replaces_end_stamp.sql
-- expected:      < 1s. Two nullable column additions; no default, no row
--                rewrite.
-- locks:         brief ACCESS EXCLUSIVE on agents and jobs for the catalog change only
-- transactional: yes

BEGIN;
SET LOCAL lock_timeout                        = '2s';
SET LOCAL statement_timeout                   = '15min';
SET LOCAL idle_in_transaction_session_timeout = '5min';
SET LOCAL timezone                            = 'UTC';

ALTER TABLE agents ADD COLUMN IF NOT EXISTS image TEXT;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS harness_image TEXT;

COMMIT;
