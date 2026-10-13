-- migration:     0541_live_sessions_job.notx.sql
-- description:   A job's live sessions.
-- depends-on:    0538_live_sessions.sql
-- expected:      short on new tables
-- locks:         ShareUpdateExclusiveLock; writes remain available
-- transactional: NO

CREATE INDEX CONCURRENTLY IF NOT EXISTS live_sessions_job ON live_sessions(job_id);
