-- migration:     0311_stateless_virtual_permanent_delete.sql
-- description:   Permit exact ended stateless virtual ownership to disappear
--                after application-attested prefix purge.
-- depends-on:    0310_validate_container_startup_stage_authority.sql
-- expected:      < 1s. Function replacements only; no row rewrite.
-- locks:         Function-catalog locks; existing owner triggers remain active.
-- transactional: yes

BEGIN;
SET LOCAL lock_timeout                        = '2s';
SET LOCAL statement_timeout                   = '15min';
SET LOCAL idle_in_transaction_session_timeout = '5min';
SET LOCAL timezone                            = 'UTC';

-- SQL cannot attest the external object-store purge. The application does so
-- under its lifecycle lock before DELETE. This classifier only distinguishes
-- the exact nonphysical virtual retention projection from an unstamped legacy
-- Kubernetes workspace. Every other owner/physical cleanup fence still runs.
CREATE FUNCTION public.stateless_virtual_permanent_delete_is_authority_free(
    requested_owner UUID, requested_state JSONB
) RETURNS BOOLEAN
LANGUAGE plpgsql STABLE
AS $function$
DECLARE
    owner_row public.threads%ROWTYPE;
    workspace JSONB;
    binding JSONB;
    settled JSONB;
BEGIN
    SELECT * INTO owner_row FROM public.threads WHERE id = requested_owner;
    IF NOT FOUND OR owner_row.execution_lane <> 'stateless'
       OR owner_row.status::TEXT <> 'ended'
       OR owner_row.metadata IS DISTINCT FROM requested_state
       OR owner_row.agent_id IS NOT NULL
       OR owner_row.control_admission_agent_id IS NOT NULL
       OR owner_row.runtime_attach_token IS NOT NULL
       OR owner_row.runtime_retirement_token IS NOT NULL
       OR owner_row.runtime_retirement_permanent IS NOT NULL
       OR owner_row.runtime_retirement_started_at IS NOT NULL
       OR owner_row.runtime_retirement_authorized_at IS NOT NULL
       OR owner_row.runtime_retirement_context IS NOT NULL
       OR owner_row.runtime_retirement_stage_receipt IS NOT NULL
       OR owner_row.runtime_retirement_local_quiescence IS NOT NULL
       OR owner_row.runtime_retirement_external_cleanup IS NOT NULL
       OR owner_row.runtime_authority_exposed
    THEN
        RETURN FALSE;
    END IF;

    workspace := requested_state->'workspace_container';
    binding := requested_state->'_workspace_binding';
    settled := requested_state->'_stateless_workspace_retirement_settled';
    IF (
        jsonb_typeof(requested_state) = 'object'
        AND requested_state #>> '{config_override,workspace,backend}' = 'virtual'
        AND workspace = '{"volume_reclaimed":false}'::JSONB
        AND jsonb_typeof(binding) = 'object'
        AND binding - ARRAY[
            'generation', 'kind', 'backing_id', 'ssh_host_key_fingerprint'
        ] = '{}'::JSONB
        AND binding->>'kind' = 'virtual'
        AND binding->>'generation'
            ~* '^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
        AND binding->>'backing_id' ~ '^rclone:[0-9a-f]{64}$'
        AND binding->'ssh_host_key_fingerprint' = 'null'::JSONB
        AND jsonb_typeof(settled) = 'object'
        AND settled - ARRAY[
            'terminal_token', 'cleanup_complete', 'permanent', 'backing_id',
            'runtime_incarnation', 'snapshot_restore_required',
            'workspace_absence_proven'
        ] = '{}'::JSONB
        AND jsonb_typeof(settled->'terminal_token') = 'number'
        AND settled->>'terminal_token' ~ '^(0|[1-9][0-9]*)$'
        AND settled->'cleanup_complete' = 'true'::JSONB
        AND settled->'permanent' = 'true'::JSONB
        AND settled->'backing_id' = binding->'backing_id'
        AND settled->'runtime_incarnation' = 'null'::JSONB
        AND settled->'snapshot_restore_required' = 'false'::JSONB
        AND settled->'workspace_absence_proven' = 'false'::JSONB
        AND COALESCE(requested_state->'vm', '{}'::JSONB) = '{}'::JSONB
        AND COALESCE(requested_state->'ide_session', '{}'::JSONB) = '{}'::JSONB
        AND COALESCE(requested_state->'agent_pod', '{}'::JSONB) = '{}'::JSONB
        AND NOT requested_state ?| ARRAY[
            '_stateless_workspace_retirement_pending',
            '_stateless_claim_retirement',
            '_stateless_resident_retirement_ack',
            '_stateless_shell_retirement_ack',
            '_stateless_claim_losses',
            '_stateless_claim_loss_hold',
            '_stateless_active_claim'
        ]
    ) IS NOT TRUE THEN
        RETURN FALSE;
    END IF;

    -- A normal permanent delete removes its exact closed queue inside the
    -- same transaction. Any remaining queue or live physical claim prevents
    -- classifying this as purely virtual, even if metadata looks harmless.
    IF EXISTS (SELECT 1 FROM public.run_queue WHERE unit_id = requested_owner)
       OR EXISTS (SELECT 1 FROM public.docker_workspace_leases
           WHERE owner_kind = 'thread' AND owner_id = requested_owner
             AND status <> 'released')
       OR EXISTS (SELECT 1 FROM public.thread_agent_workspace_claims
           WHERE thread_id = requested_owner
             AND status IN ('planned', 'ready', 'revoking', 'fenced'))
       OR EXISTS (SELECT 1 FROM public.thread_agent_pod_provision_intents
           WHERE thread_id = requested_owner
             AND status IN ('planned', 'revoking'))
       OR EXISTS (SELECT 1 FROM public.thread_workspace_provision_intents
           WHERE thread_id = requested_owner
             AND status IN ('planned', 'revoking', 'fenced'))
       OR EXISTS (SELECT 1 FROM public.managed_repository_workspace_creation_reservations
           WHERE owner_kind = 'thread' AND owner_id = requested_owner
             AND settled_at IS NULL)
       OR EXISTS (
           SELECT 1 FROM public.managed_repository_workspace_creation_reservations creation
           WHERE creation.owner_kind = 'thread'
             AND creation.owner_id = requested_owner
             AND creation.runtime_incarnation IS NOT NULL
             AND NOT EXISTS (
                 SELECT 1 FROM public.managed_repository_workspace_cleanup_intents intent
                 WHERE intent.owner_kind = 'thread' AND intent.owner_id = requested_owner
                   AND intent.scope = 'workspace_container'
                   AND intent.runtime_incarnation = creation.runtime_incarnation
                   AND intent.resource_policy = 'terminal_reclaim'
                   AND intent.target_disposition = 'deleted'
                   AND intent.result_kind = 'settled'
                   AND intent.cleanup_completed_at IS NOT NULL
                   AND intent.settled_at IS NOT NULL
             )
       )
       OR EXISTS (SELECT 1 FROM public.managed_repository_workspace_cleanup_intents
           WHERE owner_kind = 'thread' AND owner_id = requested_owner
             AND settled_at IS NULL)
       OR EXISTS (
           SELECT 1 FROM public.managed_repository_process_zero_receipts receipt
           WHERE receipt.owner_kind = 'thread' AND receipt.owner_id = requested_owner
             AND receipt.provisioner = 'k8s'
             AND receipt.scope IN ('workspace_container', 'stateless_workspace')
             AND NOT EXISTS (
                 SELECT 1 FROM public.managed_repository_workspace_cleanup_intents intent
                 WHERE intent.owner_kind = 'thread' AND intent.owner_id = requested_owner
                   AND intent.scope = 'workspace_container'
                   AND intent.runtime_incarnation::TEXT = receipt.runtime_incarnation
                   AND intent.resource_policy = 'terminal_reclaim'
                   AND intent.target_disposition = 'deleted'
                   AND intent.result_kind = 'settled'
                   AND intent.cleanup_completed_at IS NOT NULL
                   AND intent.settled_at IS NOT NULL
             )
       )
    THEN
        RETURN FALSE;
    END IF;
    RETURN TRUE;
END;
$function$;

DO $migration$
DECLARE
    definition TEXT;
    old_fragment TEXT := $old$    IF source_kind = 'thread'
       AND OLD.status::TEXT = 'ended'
       AND public.stateless_none_workspace_outcome_is_authority_free(old_state)
    THEN
        old_workspace := '{}'::JSONB;
    END IF;$old$;
    new_fragment TEXT := $new$    IF source_kind = 'thread'
       AND OLD.status::TEXT = 'ended'
       AND public.stateless_none_workspace_outcome_is_authority_free(old_state)
    THEN
        old_workspace := '{}'::JSONB;
    END IF;
    IF TG_OP = 'DELETE' AND source_kind = 'thread'
       AND public.stateless_virtual_permanent_delete_is_authority_free(
           source_id, old_state
       )
    THEN
        old_workspace := '{}'::JSONB;
    END IF;$new$;
BEGIN
    definition := pg_get_functiondef(
        'public.enforce_managed_repository_process_zero_transition()'::regprocedure
    );
    IF strpos(definition, old_fragment) = 0
       OR strpos(substr(definition, strpos(definition, old_fragment) + 1), old_fragment) > 0
    THEN
        RAISE EXCEPTION 'Unexpected process-zero transition definition';
    END IF;
    EXECUTE replace(definition, old_fragment, new_fragment);
END;
$migration$;

DO $migration$
DECLARE
    definition TEXT;
    old_fragment TEXT := $old$        IF source_kind = 'thread'
           AND scope_name = 'workspace_container'
           AND public.stateless_none_workspace_outcome_is_authority_free(
               source_state
           )
        THEN
            CONTINUE;
        END IF;$old$;
    new_fragment TEXT := $new$        IF source_kind = 'thread'
           AND scope_name = 'workspace_container'
           AND public.stateless_none_workspace_outcome_is_authority_free(
               source_state
           )
        THEN
            CONTINUE;
        END IF;
        IF source_kind = 'thread'
           AND scope_name = 'workspace_container'
           AND public.stateless_virtual_permanent_delete_is_authority_free(
               OLD.id, source_state
           )
        THEN
            CONTINUE;
        END IF;$new$;
BEGIN
    definition := pg_get_functiondef(
        'public.prevent_workspace_owner_delete_before_cleanup()'::regprocedure
    );
    IF strpos(definition, old_fragment) = 0
       OR strpos(substr(definition, strpos(definition, old_fragment) + 1), old_fragment) > 0
    THEN
        RAISE EXCEPTION 'Unexpected workspace owner-delete definition';
    END IF;
    EXECUTE replace(definition, old_fragment, new_fragment);
END;
$migration$;

COMMIT;
