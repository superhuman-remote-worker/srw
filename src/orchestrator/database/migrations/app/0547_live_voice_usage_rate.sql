-- migration:     0547_live_voice_usage_rate.sql
-- description:   Price GPT-Live voice seconds: OpenAI bills gpt-live-1 at
--                $0.05 per minute, per second. The connector pod reports the
--                session's seconds when it ends (usage_events source `live`).
-- depends-on:    0546_automations_live_primary.notx.sql
-- expected:      < 1s; one row.
-- locks:         ROW EXCLUSIVE on usage_rates.
-- transactional: yes

BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';
SET LOCAL idle_in_transaction_session_timeout = '1min';

INSERT INTO usage_rates (category, resource, unit, rate_usd, effective_from)
VALUES ('llm', 'gpt-live-1', 'live-voice-second', 0.05 / 60, '2026-10-01T00:00:00Z')
ON CONFLICT DO NOTHING;

COMMIT;
