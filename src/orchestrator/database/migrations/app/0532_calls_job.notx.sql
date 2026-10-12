-- migration: 0532_calls_job.notx.sql
-- description: Online index for the phone front.
-- depends-on: 0529_telephony.sql
-- expected: short on new tables
-- locks: ShareUpdateExclusiveLock; writes remain available
-- transactional: NO

CREATE INDEX CONCURRENTLY IF NOT EXISTS calls_job ON calls(job_id);
