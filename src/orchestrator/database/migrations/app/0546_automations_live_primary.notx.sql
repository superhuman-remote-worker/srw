-- migration:     0546_automations_live_primary.notx.sql
-- description:   One enabled live automation per primary connector (the
--                save path checks first and maps this to a 409).
-- depends-on:    0545_validate_automations_live_trigger.sql
-- expected:      short; partial index over live rows only
-- locks:         ShareUpdateExclusiveLock; writes remain available
-- transactional: NO

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS uq_automations_live_primary ON automations(trigger_connector_id) WHERE enabled AND trigger_type = 'live';
