-- migration:     0571_image_generation_capability.sql
-- description:   Add `image_generation` to the locked catalog capability
--                enum: a text-to-image / image-edit model (gpt-image, Gemini
--                image, the subscription proxy's image routes) that the
--                agent's generate_image tool calls. It is the row's only
--                capability and is never bound as a chat or vision model.
-- depends-on:    0570_deepseek_v4_1_flash_family.sql
-- expected:      < 1s. Swaps the CHECK on the small admin-curated models
--                table; NOT VALID so no scan runs under the lock — 0572
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
-- swap lock-cheap and 0572 validates.
ALTER TABLE models
    DROP CONSTRAINT IF EXISTS models_capabilities_check;

ALTER TABLE models
    ADD CONSTRAINT models_capabilities_check CHECK (
        cardinality(capabilities) >= 1
        AND capabilities <@ ARRAY[
            'chat', 'auxiliary', 'embedding', 'vision', 'whisper', 'tts',
            'search', 'fetch', 'rerank', 'live_voice', 'image_generation'
        ]::TEXT[]
    ) NOT VALID;

COMMIT;
