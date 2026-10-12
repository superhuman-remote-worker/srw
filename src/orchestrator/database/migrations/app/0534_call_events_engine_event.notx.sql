-- migration: 0534_call_events_engine_event.notx.sql
-- description: Online index for the phone front.
-- depends-on: 0529_telephony.sql
-- expected: short on new tables
-- locks: ShareUpdateExclusiveLock; writes remain available
-- transactional: NO

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS call_events_engine_event ON call_events(call_id, event_id) WHERE event_id IS NOT NULL;
