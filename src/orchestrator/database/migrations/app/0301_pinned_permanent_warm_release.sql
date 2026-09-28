-- migration:     0301_pinned_permanent_warm_release.sql
-- description:   Permit exact deleted-owner warm finalizer release receipts.
-- depends-on:    0300_ide_restore_zero_effect_cancellation.sql
-- expected:      < 5s. One function replacement, no owner scan.
-- transactional: yes

BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '15min';

CREATE FUNCTION public.pinned_deleted_owner_warm_release_authorized(
    warm public.thread_agent_warm_binding_protections
) RETURNS boolean LANGUAGE SQL VOLATILE AS $$
    SELECT NOT EXISTS (
               SELECT 1 FROM public.threads thread_row
                WHERE thread_row.id = warm.thread_id
           )
       AND EXISTS (
               SELECT 1 FROM public.thread_runtime_retirement_outcomes outcome
                WHERE outcome.thread_id = warm.thread_id
                  AND outcome.runtime_generation = warm.runtime_generation
                  AND outcome.agent_id = warm.agent_id
                  AND outcome.runtime_attach_token = warm.runtime_attach_token
                  AND outcome.disposition = 'ended'
                  AND outcome.permanent = true
                  AND outcome.outcome = 'deleted'
           )
       AND NOT EXISTS (
               SELECT 1 FROM public.agents actor
                WHERE actor.id = warm.agent_id
                  AND (actor.thread_id IS NOT NULL
                       OR actor.current_job_id IS NOT NULL
                       OR actor.status::text NOT IN ('draining', 'offline')
                       OR actor.hostname IS DISTINCT FROM warm.pod_name
                       OR actor.pod_uid IS DISTINCT FROM warm.pod_uid)
           );
$$;

DO $migration$
DECLARE
    definition text;
    old_fragment text := $old$    ELSIF NEW.status = 'releasing' THEN
        IF thread_row.id IS NULL
           OR thread_row.agent_id IS NOT DISTINCT FROM NEW.agent_id
           OR agent_row.id IS NULL
           OR agent_row.thread_id IS NOT NULL
           OR agent_row.status::text <> 'draining' THEN
            RAISE EXCEPTION 'warm binding release is not fenced'
                USING ERRCODE = '23514',
                      CONSTRAINT = 'thread_agent_warm_binding_reciprocity';
        END IF;$old$;
    new_fragment text := $new$    ELSIF NEW.status = 'releasing' THEN
        IF (thread_row.id IS NULL
            OR thread_row.agent_id IS NOT DISTINCT FROM NEW.agent_id
            OR agent_row.id IS NULL
            OR agent_row.thread_id IS NOT NULL
            OR agent_row.status::text <> 'draining')
           AND public.pinned_deleted_owner_warm_release_authorized(NEW)
               IS NOT TRUE THEN
            RAISE EXCEPTION 'warm binding release is not fenced'
                USING ERRCODE = '23514',
                      CONSTRAINT = 'thread_agent_warm_binding_reciprocity';
        END IF;$new$;
BEGIN
    definition := pg_get_functiondef(
        'public.validate_thread_agent_warm_binding_protection()'::regprocedure
    );
    IF strpos(definition, new_fragment) > 0 THEN
        RETURN;
    END IF;
    IF strpos(definition, old_fragment) = 0 THEN
        RAISE EXCEPTION 'warm release reciprocity branch drifted';
    END IF;
    EXECUTE replace(definition, old_fragment, new_fragment);
END;
$migration$;

COMMIT;
