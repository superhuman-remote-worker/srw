-- migration:     0301_pinned_permanent_warm_release.sql
-- description:   Permit exact deleted-owner warm finalizer release receipts.
-- depends-on:    0300_ide_restore_zero_effect_cancellation.sql
-- expected:      < 5s. Catalog changes; existing rows stay protected.
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

-- Old replicas select ordinary 'releasing' and may return a live Pod to the
-- pool. A distinct terminal state is invisible to those readers. The agent
-- reservation trigger fences old writers until online indexes also cover it.
ALTER TABLE public.thread_agent_warm_binding_protections
    DROP CONSTRAINT thread_agent_warm_binding_protections_status_check;
ALTER TABLE public.thread_agent_warm_binding_protections
    ADD CONSTRAINT thread_agent_warm_binding_protections_status_check
    CHECK (status IN ('planned', 'protecting', 'protected', 'bound',
                     'releasing', 'terminal_release', 'released', 'aborted'))
    NOT VALID;

DO $migration$
DECLARE
    definition text;
    old_fragment text := $old$((status)::text = 'releasing'::text)$old$;
    new_fragment text := $new$((status)::text = ANY ((ARRAY['releasing'::character varying, 'terminal_release'::character varying])::text[]))$new$;
BEGIN
    SELECT pg_get_constraintdef(oid) INTO definition
      FROM pg_constraint
     WHERE conrelid = 'public.thread_agent_warm_binding_protections'::regclass
       AND conname = 'thread_agent_warm_binding_protections_check2';
    IF definition IS NULL OR strpos(definition, old_fragment) = 0 THEN
        RAISE EXCEPTION 'warm release shape constraint drifted';
    END IF;
    EXECUTE 'ALTER TABLE public.thread_agent_warm_binding_protections '
        || 'DROP CONSTRAINT thread_agent_warm_binding_protections_check2';
    EXECUTE 'ALTER TABLE public.thread_agent_warm_binding_protections '
        || 'ADD CONSTRAINT thread_agent_warm_binding_protections_check2 '
        || replace(definition, old_fragment, new_fragment) || ' NOT VALID';
END;
$migration$;

DO $migration$
DECLARE
    definition text;
    old_start text := $old$                OR (OLD.status IN ('protected', 'bound')
                    AND NEW.status = 'releasing'$old$;
    new_start text := $new$                OR (((OLD.status IN ('protected', 'bound')
                    AND NEW.status = 'releasing')
                    OR (OLD.status = 'bound'
                    AND NEW.status = 'terminal_release'))$new$;
    old_finish text := $old$                OR (OLD.status = 'releasing' AND NEW.status = 'released'$old$;
    new_finish text := $new$                OR (OLD.status IN ('releasing', 'terminal_release') AND NEW.status = 'released'$new$;
    old_guard text := $old$    IF TG_OP = 'UPDATE' THEN
        IF NEW.protection_id IS DISTINCT FROM OLD.protection_id$old$;
    new_guard text := $new$    IF TG_OP = 'UPDATE' THEN
        IF OLD.status = 'terminal_release' AND NEW.status = 'released'
           AND NEW.release_outcome IS DISTINCT FROM 'exact_absent_v1' THEN
            RAISE EXCEPTION 'terminal warm release requires absent Pod receipt'
                USING ERRCODE = '23514',
                      CONSTRAINT = 'thread_agent_warm_binding_authority';
        END IF;
        IF NEW.protection_id IS DISTINCT FROM OLD.protection_id$new$;
BEGIN
    definition := pg_get_functiondef(
        'public.enforce_thread_agent_warm_binding_protection()'::regprocedure
    );
    IF strpos(definition, old_start) = 0
       OR strpos(definition, old_finish) = 0
       OR strpos(definition, old_guard) = 0 THEN
        RAISE EXCEPTION 'warm release transition function drifted';
    END IF;
    definition := replace(definition, old_start, new_start);
    definition := replace(definition, old_finish, new_finish);
    EXECUTE replace(definition, old_guard, new_guard);
END;
$migration$;

CREATE OR REPLACE FUNCTION public.enforce_pinned_warm_agent_reservation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM public.thread_agent_warm_binding_protections warm
         WHERE warm.agent_id = NEW.id
           AND warm.status IN ('planned', 'protecting', 'protected',
                               'releasing', 'terminal_release')
           AND (
               NEW.thread_id IS NOT NULL
               OR NEW.current_job_id IS NOT NULL
               OR (warm.status = 'terminal_release' AND (
                   NEW.status::text NOT IN ('draining', 'offline')
                   OR NEW.hostname IS DISTINCT FROM warm.pod_name
                   OR NEW.pod_uid IS DISTINCT FROM warm.pod_uid
               ))
               OR (warm.status <> 'terminal_release'
                   AND NEW.status::text <> 'draining')
           )
    ) THEN
        RAISE EXCEPTION 'warm Pod has unresolved protection authority'
            USING ERRCODE = '23514',
                  CONSTRAINT = 'agents_pinned_warm_binding_authority';
    END IF;
    RETURN NEW;
END;
$$;

-- Do not let an older registration writer reinsert the exact actor ID after
-- its old row disappeared; the terminal receipt may still own that identity.
DROP TRIGGER zzz_agents_pinned_warm_binding_authority ON public.agents;
CREATE TRIGGER zzz_agents_pinned_warm_binding_authority
BEFORE INSERT OR UPDATE ON public.agents
FOR EACH ROW
EXECUTE FUNCTION public.enforce_pinned_warm_agent_reservation();

CREATE OR REPLACE FUNCTION public.enforce_pinned_warm_create_exclusion()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM public.thread_agent_warm_binding_protections warm
         WHERE warm.thread_id = NEW.thread_id
           AND warm.runtime_generation = NEW.runtime_generation
           AND warm.status IN ('planned', 'protecting', 'protected',
                               'releasing', 'terminal_release')
    ) THEN
        RAISE EXCEPTION 'pinned Pod create races warm binding protection'
            USING ERRCODE = '23514',
                  CONSTRAINT = 'thread_agent_warm_binding_create_exclusion';
    END IF;
    RETURN NEW;
END;
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
        IF thread_row.id IS NULL
           OR thread_row.agent_id IS NOT DISTINCT FROM NEW.agent_id
           OR agent_row.id IS NULL
           OR agent_row.thread_id IS NOT NULL
           OR agent_row.status::text <> 'draining' THEN
            RAISE EXCEPTION 'warm binding release is not fenced'
                USING ERRCODE = '23514',
                      CONSTRAINT = 'thread_agent_warm_binding_reciprocity';
        END IF;
    ELSIF NEW.status = 'terminal_release' THEN
        IF public.pinned_deleted_owner_warm_release_authorized(NEW)
               IS NOT TRUE THEN
            RAISE EXCEPTION 'terminal warm release lacks deleted-owner authority'
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
