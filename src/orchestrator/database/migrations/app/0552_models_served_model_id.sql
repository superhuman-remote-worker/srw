-- migration:     0552_models_served_model_id.sql
-- description:   One model id, one catalog row. Adds served_model_id, the
--                string sent to the provider when it differs from the
--                catalog id, and renames every duplicated model_id except
--                the row that routes today, so 0553 can make model_id
--                unique. The row that keeps the bare id is the one
--                resolve_catalog_model picked: enabled first, then a system
--                row before an endpoint row, then the oldest. Every other
--                row becomes <id>-<slug> (slug = provider_ref for a system
--                row, a slug of the endpoint label for an endpoint row, plus
--                -2, -3 ... while taken) and keeps sending the old id through
--                served_model_id. Row UUIDs do not change and no traffic
--                moves: nothing could reach the renamed rows before.
-- depends-on:    0549_main_cloud_service_credentials.sql
-- expected:      < 1s (a nullable column without a default; the rename
--                touches only duplicated ids).
-- locks:         brief ACCESS EXCLUSIVE on models.
-- transactional: yes

BEGIN;
SET LOCAL lock_timeout                        = '2s';
SET LOCAL statement_timeout                   = '30s';
SET LOCAL idle_in_transaction_session_timeout = '5min';

ALTER TABLE public.models ADD COLUMN served_model_id TEXT;

COMMENT ON COLUMN public.models.served_model_id IS
    'The model name sent to the provider when it differs from model_id. '
    'NULL sends model_id (minus a routing prefix such as openrouter/). '
    'When set, model_id is this name plus a -suffix of the same family, '
    'e.g. claude-opus-5-5-api serving claude-opus-5-5.';

DO $$
DECLARE
    dup RECORD;
    candidate TEXT;
    suffix INTEGER;
BEGIN
    FOR dup IN
        SELECT ranked.id, ranked.model_id, ranked.slug
        FROM (
            SELECT
                m.id,
                m.model_id,
                CASE
                    WHEN m.provider_kind = 'system' THEN m.provider_ref
                    ELSE coalesce(
                        nullif(
                            btrim(
                                lower(regexp_replace(e.label, '[^A-Za-z0-9]+', '-', 'g')),
                                '-'
                            ),
                            ''
                        ),
                        'endpoint'
                    )
                END AS slug,
                row_number() OVER (
                    PARTITION BY m.model_id
                    ORDER BY
                        m.enabled DESC,
                        (m.provider_kind = 'system') DESC,
                        m.created_at ASC NULLS LAST,
                        m.id
                ) AS rank
            FROM public.models AS m
            LEFT JOIN public.llm_endpoints AS e
                ON m.provider_kind = 'endpoint' AND e.id::text = m.provider_ref
        ) AS ranked
        WHERE ranked.rank > 1
        ORDER BY ranked.model_id, ranked.rank
    LOOP
        candidate := dup.model_id || '-' || dup.slug;
        suffix := 1;
        WHILE EXISTS (SELECT 1 FROM public.models WHERE model_id = candidate) LOOP
            suffix := suffix + 1;
            candidate := dup.model_id || '-' || dup.slug || '-' || suffix;
        END LOOP;
        UPDATE public.models
        SET served_model_id = dup.model_id,
            model_id = candidate,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = dup.id;
        RAISE NOTICE 'models: % (%) renamed to %, still serving %',
            dup.model_id, dup.id, candidate, dup.model_id;
    END LOOP;

    IF EXISTS (
        SELECT 1 FROM public.models GROUP BY model_id HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION 'models: duplicate model_id values remain after the rename';
    END IF;
END
$$;

COMMIT;
