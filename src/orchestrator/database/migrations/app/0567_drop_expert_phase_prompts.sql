-- migration:     0567_drop_expert_phase_prompts.sql
-- description:   The expert editor's Strategic / Tactical prompt fields are
--                retired. A worker's phase guidance is the strategic-phase /
--                tactical-phase skills; the stored text was only appended to
--                them, and a copy of a bundled expert carried that expert's
--                own phase skill a second time. Strip the two keys from every
--                stored expert's prompts. The text is deleted, not moved:
--                operators were told to back it up before this ran.
--                Expert manifests in srw_resources are not rewritten here:
--                their revisions are content digests that other resources pin,
--                and the config resolver ignores the two keys
--                (config_resolver._OVERLAY_PROMPT_KEYS), so a manifest that
--                still carries one never reaches a job and loses it on its
--                next save.
-- depends-on:    0566_job_title.sql
-- expected:      < 1s; data-only (no DDL, so the schema_current artifact is
--                unchanged). Idempotent: stripped rows no longer match.
--                Touches 0 rows on k3d and the dev cluster.
-- locks:         Row locks on the matching experts rows only.
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

UPDATE public.experts
   SET prompts = prompts - 'strategic' - 'tactical'
 WHERE jsonb_typeof(prompts) = 'object'
   AND prompts ?| ARRAY['strategic', 'tactical'];

COMMIT;
