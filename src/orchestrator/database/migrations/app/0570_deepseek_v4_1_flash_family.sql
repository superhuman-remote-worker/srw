-- DeepSeek V4.1 Flash takes images; V4 Pro and the earlier V4 Flash
-- checkpoints do not. They shared the model family `deepseek`, which the
-- config matrix marks text-only, so a session on V4.1 Flash never saw an
-- image. V4.1 Flash now has its own family, `deepseek-v4.1-flash`.
--
-- The agent resolves a model's family from its name (family_of), so new
-- dispatches pick the new family without this migration. It corrects two
-- things stored under the old name:

-- 1. A catalog row's family, which Admin -> Models shows. The pattern is
--    family_of()'s: deepseek-flash, deepseek-v4.1-flash or
--    deepseek-v4-flash-vision-exp, after an optional provider path, for the
--    name sent to the provider (the served name when there is one).
UPDATE models
SET family = 'deepseek-v4.1-flash',
    updated_at = CURRENT_TIMESTAMP
WHERE family = 'deepseek'
  AND lower(coalesce(served_model_id, model_id))
      ~ '(^|/)deepseek-(flash|v4\.1-flash|v4-flash-vision-exp)($|[^[:alnum:]_.])';

-- 2. An administrator's overrides for the `deepseek` family, which applied
--    to V4.1 Flash until now. Each is copied to the new family, except the
--    two settings the families differ in. An override the new family
--    already has is kept.
INSERT INTO config_overrides (
    family, kind, name, content, content_format, notes,
    created_by, updated_by, value_json
)
SELECT
    'deepseek-v4.1-flash', kind, name, content, content_format, notes,
    created_by, updated_by, value_json
FROM config_overrides
WHERE family = 'deepseek'
  AND NOT (
      kind = 'settings'
      AND (name = 'multimodal' OR name = 'image_tokens' OR name LIKE 'image\_tokens.%')
  )
ON CONFLICT DO NOTHING;
