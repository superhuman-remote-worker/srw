-- migration:     0539_live_session_events_cursor.notx.sql
-- description:   Cursor index for journal reads.
-- depends-on:    0538_live_sessions.sql
-- expected:      short on new tables
-- locks:         ShareUpdateExclusiveLock; writes remain available
-- transactional: NO

CREATE INDEX CONCURRENTLY IF NOT EXISTS live_session_events_cursor ON live_session_events(session_id, id);
