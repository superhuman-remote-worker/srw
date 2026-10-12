-- migration: 0533_calls_recorder.notx.sql
-- description: Online index for the phone front.
-- depends-on: 0529_telephony.sql
-- expected: short on new tables
-- locks: ShareUpdateExclusiveLock; writes remain available
-- transactional: NO

CREATE INDEX CONCURRENTLY IF NOT EXISTS calls_recorder ON calls(status, recorder_lease_until);
