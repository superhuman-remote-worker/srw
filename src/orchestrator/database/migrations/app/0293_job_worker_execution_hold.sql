-- migration: 0293_job_worker_execution_hold.sql
-- description: Preserve a restriction-only worker loss marker against stale context writers.
-- depends-on: 0292_job_retired_workspace_terminal_projection.sql
-- transactional: yes
-- expected: < 5s. Trigger installation only; no row scan or backfill.
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';
SET LOCAL idle_in_transaction_session_timeout = '30s';

CREATE FUNCTION public.preserve_job_worker_execution_hold() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF COALESCE(OLD.context, '{}'::jsonb) ? '_worker_execution_hold'
       AND (NOT (COALESCE(NEW.context, '{}'::jsonb) ? '_worker_execution_hold')
            OR NEW.context->'_worker_execution_hold'
               IS DISTINCT FROM OLD.context->'_worker_execution_hold') THEN
        RAISE EXCEPTION 'A pending worker execution hold is immutable'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER jobs_worker_execution_hold_immutable
    BEFORE UPDATE OF context ON public.jobs
    FOR EACH ROW EXECUTE FUNCTION public.preserve_job_worker_execution_hold();

COMMENT ON FUNCTION public.preserve_job_worker_execution_hold() IS
    'Restriction only: no process-zero, cleanup, replay or settlement authority. Phase A has no clearing writer; malformed presence remains restrictive.';

COMMIT;
