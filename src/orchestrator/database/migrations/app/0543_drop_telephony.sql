-- migration:     0543_drop_telephony.sql
-- description:   Retire the phone build's telephony tables (0529-0535). Live
--                sessions (0538) replace them; the phone line is a connector
--                now. No backfill: the tables only ever held test calls.
-- depends-on:    0542_live_sessions_connector_status.notx.sql
-- expected:      < 1s; the tables are tiny or empty.
-- locks:         ACCESS EXCLUSIVE on the dropped tables only.
-- transactional: yes

BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';
SET LOCAL idle_in_transaction_session_timeout = '1min';

-- squawk-ignore ban-drop-table
DROP TABLE IF EXISTS call_events;
-- squawk-ignore ban-drop-table
DROP TABLE IF EXISTS calls;
-- squawk-ignore ban-drop-table
DROP TABLE IF EXISTS telephony_lines;
-- squawk-ignore ban-drop-table
DROP TABLE IF EXISTS telephony_settings;

COMMIT;
