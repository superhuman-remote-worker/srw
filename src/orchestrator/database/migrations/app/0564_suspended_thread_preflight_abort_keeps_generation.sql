-- migration:     0564_suspended_thread_preflight_abort_keeps_generation.sql
-- description:   Aborting an unauthorized retirement preflight on a thread that
--                is already 'suspended' no longer rotates its runtime
--                generation. The suspension-settle branch of
--                enforce_thread_ended_transition matched any soft-token clear
--                that leaves the status 'suspended', including the
--                marker-only abort of an End that was refused (a no-force
--                Officer decommission with jobs in flight, or the stale
--                preflight reaper). The rotation then tripped
--                threads_runtime_retirement_ownership ("runtime ownership
--                cannot change while aborting retirement"): a 500 on decommission
--                and a reaper step that failed every sweep. A real settle into
--                'suspended' is authorized and still rotates.
-- depends-on:    0529_vm_bound_source_free_end.sql
-- expected:      < 1s (function replacement only).
-- locks:         none beyond the function's catalog row.
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';
SET LOCAL idle_in_transaction_session_timeout = '5min';

DO $migration$
DECLARE
    definition text;
    old_fragment text := $old$          AND NEW.runtime_retirement_token IS NULL
          AND NEW.status = 'suspended' THEN$old$;
    new_fragment text := $new$          AND NEW.runtime_retirement_token IS NULL
          AND NEW.status = 'suspended'
          -- Not the marker-only abort of an unauthorized preflight on a
          -- thread that was already suspended: that keeps its generation.
          AND NOT (
              OLD.status = 'suspended'
              AND OLD.runtime_retirement_authorized_at IS NULL
              AND OLD.runtime_retirement_stage_receipt IS NULL
              AND OLD.runtime_retirement_local_quiescence IS NULL
              AND OLD.runtime_retirement_external_cleanup IS NULL
          ) THEN$new$;
BEGIN
    definition := pg_get_functiondef('public.enforce_thread_ended_transition()'::regprocedure);
    IF (length(definition)-length(replace(definition,old_fragment,'')))/length(old_fragment) <> 1 THEN
        RAISE EXCEPTION '0564 requires one exact suspension-settle rotation branch';
    END IF;
    EXECUTE replace(definition,old_fragment,new_fragment);
END;
$migration$;

COMMIT;
