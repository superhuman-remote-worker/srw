-- migration:     0560_refused_job_creation_error_is_metadata.sql
-- description:   A Job whose workspace Pod the cluster refused at admission
--                can settle and be deleted: the guards read its error-only
--                workspace record as metadata.
-- depends-on:    0559_harness_image_columns.sql
-- expected:      < 1s. Adds two classifiers and patches two trigger functions
--                from their catalog definitions; no row scan.
-- locks:         Function-catalog lock only; installed row triggers remain.
-- transactional: yes

BEGIN;
SET LOCAL lock_timeout                        = '2s';
SET LOCAL statement_timeout                   = '15min';
SET LOCAL idle_in_transaction_session_timeout = '5min';
SET LOCAL timezone                            = 'UTC';

-- When the API server refuses a Job's Pod (a LimitRange, a quota, an admission
-- policy), the creation fails before any runtime is recorded, and the Job's
-- workspace record holds only the cluster's message
-- (container_provisioner._record_creation_diagnostic). It names no process,
-- Pod, lease, endpoint or provisioner. Both guards read any non-empty record
-- without a provisioner as authority to retire, so the creation could never
-- settle and the Job could never be deleted. Read it as the metadata it is,
-- like the legacy heartbeat's timestamp-only record.
CREATE OR REPLACE FUNCTION public.job_workspace_creation_error_is_authority_free(
    workspace JSONB
)
RETURNS BOOLEAN
LANGUAGE SQL
IMMUTABLE
AS $function$
    SELECT COALESCE(
        jsonb_typeof(workspace) = 'object'
        AND jsonb_typeof(workspace -> 'error') = 'string'
        AND workspace - ARRAY['error', 'status'] = '{}'::JSONB
        AND COALESCE(jsonb_typeof(workspace -> 'status'), 'string') = 'string',
        FALSE
    );
$function$;

-- Deleting the Job also needs its latest creation settled with no Pod and its
-- storage reclaimed, so no PVC or Service outlives the row.
CREATE OR REPLACE FUNCTION public.job_workspace_creation_reclaimed_without_pod(
    requested_job_id UUID
)
RETURNS BOOLEAN
LANGUAGE SQL
STABLE
AS $function$
    SELECT COALESCE((
        SELECT r.settled_at IS NOT NULL
           AND r.phase = 'aborted'
           AND r.result_kind = 'aborted'
           AND r.pod_uid IS NULL
           AND r.runtime_incarnation IS NULL
           AND r.cancel_resource_policy = 'terminal_reclaim'
           AND r.cancel_cleanup_completed_at IS NOT NULL
          FROM public.managed_repository_workspace_creation_reservations AS r
         WHERE r.owner_kind = 'job'
           AND r.owner_id = requested_job_id
           AND r.scope = 'workspace_container'
         ORDER BY r.reservation_generation DESC
         LIMIT 1
    ), FALSE);
$function$;

DO $migration$
DECLARE
    definition TEXT;
    old_fragment TEXT := $old$    IF TG_OP = 'DELETE' AND source_kind = 'thread'
       AND public.stateless_virtual_permanent_delete_is_authority_free(
           source_id, old_state
       )
    THEN
        old_workspace := '{}'::JSONB;
    END IF;$old$;
    new_fragment TEXT := $new$    IF TG_OP = 'DELETE' AND source_kind = 'thread'
       AND public.stateless_virtual_permanent_delete_is_authority_free(
           source_id, old_state
       )
    THEN
        old_workspace := '{}'::JSONB;
    END IF;
    -- A Job whose Pod the cluster refused holds only that error (0560).
    IF source_kind = 'job'
       AND public.job_workspace_creation_error_is_authority_free(old_workspace)
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
           AND runtime_state - 'last_activity' = '{}'::JSONB
           AND jsonb_typeof(runtime_state -> 'last_activity') = 'string' THEN
            CONTINUE;
        END IF;$old$;
    new_fragment TEXT := $new$        IF source_kind = 'job' AND scope_name = 'workspace_container'
           AND runtime_state - 'last_activity' = '{}'::JSONB
           AND jsonb_typeof(runtime_state -> 'last_activity') = 'string' THEN
            CONTINUE;
        END IF;
        -- A Job whose Pod the cluster refused holds only that error, once its
        -- creation settled with no Pod and reclaimed its storage (0560).
        IF source_kind = 'job' AND scope_name = 'workspace_container'
           AND public.job_workspace_creation_error_is_authority_free(runtime_state)
           AND public.job_workspace_creation_reclaimed_without_pod(OLD.id) THEN
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
