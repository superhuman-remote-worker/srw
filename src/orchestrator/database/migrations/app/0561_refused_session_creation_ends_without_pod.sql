-- migration:     0561_refused_session_creation_ends_without_pod.sql
-- description:   A stateless Session whose only workspace Pod create the
--                cluster refused can be Ended and deleted: once End settled
--                that creation with no Pod, the guards read its UID-less
--                workspace record as metadata.
-- depends-on:    0560_refused_job_creation_error_is_metadata.sql
-- expected:      < 1s. Adds one classifier and patches two trigger functions
--                from their catalog definitions; no row scan.
-- locks:         Function-catalog lock only; installed row triggers remain.
-- transactional: yes

BEGIN;
SET LOCAL lock_timeout                        = '2s';
SET LOCAL statement_timeout                   = '15min';
SET LOCAL idle_in_transaction_session_timeout = '5min';
SET LOCAL timezone                            = 'UTC';

-- When the API server refuses a Session's one Pod create (a LimitRange, a
-- quota, an admission policy), the creation never records a runtime or Pod UID
-- and the Session's workspace record stays a pending Kubernetes record with
-- no `_runtime_incarnation`. End retires it as `unpublished_creation`: the
-- creation's cancellation reads the Pod by name and settles with no Pod. Both
-- guards read a UID-less Kubernetes record as authority to retire, so neither
-- that settlement's projection nor the owner delete could ever pass.
--
-- Read it as metadata only for the exact creation the End marker names, when
-- it is the Session's latest one and it settled aborted with no Pod, its Pod
-- effect never observed. Owner deletion also needs that creation to have
-- reclaimed its storage, so no PVC or Service outlives the row.
CREATE OR REPLACE FUNCTION public.stateless_unpublished_creation_settled_without_pod(
    requested_thread UUID,
    requested_state JSONB,
    require_reclaimed BOOLEAN
)
RETURNS BOOLEAN
LANGUAGE SQL
STABLE
AS $function$
    SELECT COALESCE((
        SELECT jsonb_typeof(requested_state -> 'workspace_container') = 'object'
           AND requested_state #>> '{workspace_container,provisioner}' = 'k8s'
           AND requested_state #>> '{workspace_container,_runtime_incarnation}'
               IS NULL
           AND COALESCE(requested_state -> '_workspace_binding', '{}'::JSONB)
               = '{}'::JSONB
           AND r.id::TEXT = COALESCE(
               requested_state #>> ARRAY[
                   '_stateless_claim_retirement', 'unpublished_creation',
                   'reservation_id'
               ],
               requested_state #>> ARRAY[
                   '_stateless_workspace_retirement_settled',
                   'unpublished_creation', 'reservation_id'
               ]
           )
           AND r.operation_kind = 'create'
           AND r.settled_at IS NOT NULL
           AND r.phase = 'aborted'
           AND r.result_kind = 'aborted'
           AND r.pod_uid IS NULL
           AND r.runtime_incarnation IS NULL
           AND r.cancel_target_disposition = 'deleted'
           AND r.cancel_cleanup_completed_at IS NOT NULL
           AND r.external_effects #>> '{pod,observed_uid}' IS NULL
           AND (
               NOT require_reclaimed
               OR r.cancel_resource_policy = 'terminal_reclaim'
           )
          FROM public.managed_repository_workspace_creation_reservations AS r
         WHERE r.owner_kind = 'thread'
           AND r.owner_id = requested_thread
           AND r.scope = 'workspace_container'
         ORDER BY r.reservation_generation DESC
         LIMIT 1
    ), FALSE);
$function$;

DO $migration$
DECLARE
    definition TEXT;
    old_fragment TEXT := $old$    -- A Job whose Pod the cluster refused holds only that error (0560).
    IF source_kind = 'job'
       AND public.job_workspace_creation_error_is_authority_free(old_workspace)
    THEN
        old_workspace := '{}'::JSONB;
    END IF;$old$;
    new_fragment TEXT := $new$    -- A Job whose Pod the cluster refused holds only that error (0560).
    IF source_kind = 'job'
       AND public.job_workspace_creation_error_is_authority_free(old_workspace)
    THEN
        old_workspace := '{}'::JSONB;
    END IF;
    -- A Session whose only Pod create the cluster refused has no Pod once
    -- End settled that creation (0561).
    IF source_kind = 'thread'
       AND public.stateless_unpublished_creation_settled_without_pod(
           source_id, old_state, FALSE
       )
    THEN
        old_workspace := '{}'::JSONB;
    END IF;$new$;
BEGIN
    definition := pg_get_functiondef(
        'public.enforce_managed_repository_process_zero_transition()'::regprocedure
    );
    IF strpos(definition, new_fragment) > 0 THEN
        RETURN;
    END IF;
    IF strpos(definition, old_fragment) = 0 THEN
        RAISE EXCEPTION
            'Unexpected enforce_managed_repository_process_zero_transition definition';
    END IF;
    EXECUTE replace(definition, old_fragment, new_fragment);
END;
$migration$;

DO $migration$
DECLARE
    definition TEXT;
    old_fragment TEXT := $old$        IF source_kind = 'job' AND scope_name = 'workspace_container'
           AND public.job_workspace_creation_error_is_authority_free(runtime_state)
           AND public.job_workspace_creation_reclaimed_without_pod(OLD.id) THEN
            CONTINUE;
        END IF;$old$;
    new_fragment TEXT := $new$        IF source_kind = 'job' AND scope_name = 'workspace_container'
           AND public.job_workspace_creation_error_is_authority_free(runtime_state)
           AND public.job_workspace_creation_reclaimed_without_pod(OLD.id) THEN
            CONTINUE;
        END IF;
        -- A Session whose only Pod create the cluster refused, once End
        -- settled that creation with no Pod and reclaimed its storage (0561).
        IF source_kind = 'thread' AND scope_name = 'workspace_container'
           AND public.stateless_unpublished_creation_settled_without_pod(
               OLD.id, source_state, TRUE
           ) THEN
            CONTINUE;
        END IF;$new$;
BEGIN
    definition := pg_get_functiondef(
        'public.prevent_workspace_owner_delete_before_cleanup()'::regprocedure
    );
    IF strpos(definition, new_fragment) > 0 THEN
        RETURN;
    END IF;
    IF strpos(definition, old_fragment) = 0 THEN
        RAISE EXCEPTION
            'Unexpected prevent_workspace_owner_delete_before_cleanup definition';
    END IF;
    EXECUTE replace(definition, old_fragment, new_fragment);
END;
$migration$;

COMMIT;
