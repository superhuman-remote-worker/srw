-- migration:     0540_live_session_events_event.notx.sql
-- description:   One journal row per engine or client event id.
-- depends-on:    0538_live_sessions.sql
-- expected:      short on new tables
-- locks:         ShareUpdateExclusiveLock; writes remain available
-- transactional: NO

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS live_session_events_event ON live_session_events(session_id, event_id) WHERE event_id IS NOT NULL;
