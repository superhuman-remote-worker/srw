-- migration:     0554_thread_permission_mode_read_only.sql
-- description:   Accept the read_only permission mode (renamed from supervised) and default to it.
-- depends-on:    0553_models_model_id_unique.notx.sql
-- expected:      < 5s. A NOT VALID check swap and a column default; no backfill.
-- transactional: yes

-- Expand step of the supervised -> read_only rename
-- (knowledge-base/knowledge/features/tool_permission_tiers.md, D2). Both
-- spellings stay valid while agent images from before the rename can still
-- run; a later contract migration rewrites the rows and drops 'supervised'.
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '15min';

ALTER TABLE public.threads
    ADD CONSTRAINT valid_permission_mode_v2 CHECK (
        permission_mode IN ('read_only', 'supervised', 'auto_accept', 'autonomous')
    )
    NOT VALID;

ALTER TABLE public.threads
    DROP CONSTRAINT valid_permission_mode;

ALTER TABLE public.threads
    ALTER COLUMN permission_mode SET DEFAULT 'read_only';

COMMIT;
