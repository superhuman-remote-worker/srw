-- migration: 0535_call_events_cursor.notx.sql
-- description: Online index for the phone front.
-- depends-on: 0529_telephony.sql
-- expected: short on new tables
-- locks: ShareUpdateExclusiveLock; writes remain available
-- transactional: NO

CREATE INDEX CONCURRENTLY IF NOT EXISTS call_events_cursor ON call_events(call_id, id);
