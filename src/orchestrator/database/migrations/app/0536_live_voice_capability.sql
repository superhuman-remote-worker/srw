-- migration:     0536_live_voice_capability.sql
-- description:   Add `live_voice` to the locked catalog capability enum: a
--                full-duplex voice model (GPT-Live) that a phone-line
--                connector names. It is never bound as a chat model.
-- depends-on:    0535_call_events_cursor.notx.sql
-- expected:      < 1s. Swaps the CHECK on the small admin-curated models
--                table; NOT VALID so no scan runs under the lock — 0537
--                validates.
-- locks:         ACCESS EXCLUSIVE on models for the constraint swap (tens of
--                rows, admin writes only).
-- transactional: yes

BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';
SET LOCAL idle_in_transaction_session_timeout = '1min';

-- One transaction: no row can be written between the DROP and the ADD. The
-- new constraint is a strict superset of the old one (one more allowed
-- value), so every existing row already satisfies it; NOT VALID keeps the
-- swap lock-cheap and 0537 validates.
ALTER TABLE models
    DROP CONSTRAINT IF EXISTS models_capabilities_check;

ALTER TABLE models
    ADD CONSTRAINT models_capabilities_check CHECK (
        cardinality(capabilities) >= 1
        AND capabilities <@ ARRAY[
            'chat', 'auxiliary', 'embedding', 'vision', 'whisper', 'tts',
            'search', 'fetch', 'rerank', 'live_voice'
        ]::TEXT[]
    ) NOT VALID;

COMMIT;
