-- migration:     0542_live_sessions_connector_status.notx.sql
-- description:   A connector's sessions by status (pod adoption).
-- depends-on:    0538_live_sessions.sql
-- expected:      short on new tables
-- locks:         ShareUpdateExclusiveLock; writes remain available
-- transactional: NO

CREATE INDEX CONCURRENTLY IF NOT EXISTS live_sessions_connector_status ON live_sessions(connector_id, status);
