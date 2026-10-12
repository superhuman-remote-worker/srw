-- migration: 0531_telephony_lines_default.notx.sql
-- description: Online index for the phone front.
-- depends-on: 0529_telephony.sql
-- expected: short on new tables
-- locks: ShareUpdateExclusiveLock; writes remain available
-- transactional: NO

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS telephony_lines_default ON telephony_lines(settings_id) WHERE enabled AND is_default;
