-- migration: 0530_telephony_lines_number.notx.sql
-- description: Online index for the phone front.
-- depends-on: 0529_telephony.sql
-- expected: short on new tables
-- locks: ShareUpdateExclusiveLock; writes remain available
-- transactional: NO

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS telephony_lines_number ON telephony_lines(number) WHERE number IS NOT NULL;
