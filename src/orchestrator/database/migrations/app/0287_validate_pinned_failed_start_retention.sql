-- migration: 0287_validate_pinned_failed_start_retention.sql
-- description: Validate the additive failed-start workspace retention shapes.
-- depends-on: 0286_pinned_failed_start_retirement.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

-- Existing rows satisfy the old shapes, which imply each widened replacement.
-- Validate separately from the short exclusive-lock introduction in 0286.
ALTER TABLE public.thread_workspace_provision_intents
    VALIDATE CONSTRAINT thread_workspace_cleanup_disposition,
    VALIDATE CONSTRAINT thread_workspace_retained_storage_source,
    VALIDATE CONSTRAINT thread_workspace_provision_intents_check9;

COMMIT;
