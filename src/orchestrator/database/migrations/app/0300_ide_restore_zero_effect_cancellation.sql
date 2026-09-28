-- migration:     0300_ide_restore_zero_effect_cancellation.sql
-- description:   Let an exact same-transaction 0198 cancellation receipt close
--                a UID-less IDE restore attempt before any external effect.
-- depends-on:    0299_pinned_vm_actuator_launcher_identity.sql
-- expected:      < 5s. One trigger-function replacement, no owner scan.
-- transactional: yes

BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '15min';

CREATE FUNCTION public.managed_repo_uidless_ide_abort_authorized_now(
    requested_owner_id UUID,
    old_state JSONB,
    new_state JSONB
)
RETURNS BOOLEAN LANGUAGE SQL VOLATILE AS $$
    SELECT old_state #>> '{ide_session,status}' = 'restoring'
       AND old_state #>> '{ide_session,restore_type}' = 'k8s_container'
       AND old_state #>> '{ide_session,_runtime_incarnation}' IS NULL
       AND old_state #>> '{ide_session,container_id}' IS NULL
       AND old_state #>> '{ide_session,_restore_attempt_id}' IS NOT NULL
       AND EXISTS (
           SELECT 1
             FROM public.managed_repository_workspace_creation_reservations r
            WHERE r.owner_kind = 'job'
              AND r.owner_id = requested_owner_id
              AND r.scope = 'ide'
              AND r.operation_kind = 'restore'
              AND r.id::TEXT = old_state #>> '{ide_session,_creation_reservation_id}'
              AND r.claim_token::TEXT = old_state #>> '{ide_session,_creation_claim_token}'
              AND r.lifecycle_fingerprint->>'restore_attempt_id' =
                  old_state #>> '{ide_session,_restore_attempt_id}'
              AND r.lifecycle_fingerprint->>'restore_source' =
                  old_state #>> '{ide_session,source}'
              AND r.lifecycle_fingerprint->>'restore_snapshot_type' =
                  old_state #>> '{ide_session,snapshot_type}'
              AND r.phase = 'aborted' AND r.result_kind = 'aborted'
              AND r.cancel_target_disposition = 'expired'
              AND r.cancel_requested_at IS NOT NULL
              AND r.cancel_cleanup_completed_at IS NOT NULL
              AND r.cancel_projection_transaction_id = txid_current()
              AND r.settled_at IS NOT NULL
              AND r.external_mutation_started_at IS NULL
              AND r.runtime_incarnation IS NULL AND r.pod_uid IS NULL
       )
       AND public.managed_repo_cancelled_creation_projection_authorized_now(
           'job', requested_owner_id, 'ide', old_state, new_state
       );
$$;

-- 0238, 0246 and 0247 already amended this function. Replace only the
-- effective rejection branch so their later guards remain byte-identical.
DO $migration$
DECLARE
    definition TEXT;
    old_fragment TEXT := $old$        ELSIF destructive_transition AND runtime_id IS NULL
           AND old_ide <> '{}'::JSONB THEN
            RAISE EXCEPTION USING
                ERRCODE = '23514',
                CONSTRAINT = 'managed_repository_ide_runtime_identity_required',
                MESSAGE = 'IDE runtime identity is required before destructive teardown';$old$;
    new_fragment TEXT := $new$        ELSIF destructive_transition AND runtime_id IS NULL
           AND old_ide <> '{}'::JSONB
           AND NOT (
               TG_OP = 'UPDATE'
               AND source_kind = 'job'
               AND public.managed_repo_uidless_ide_abort_authorized_now(
                   source_id, old_state, new_state
               )
           ) THEN
            RAISE EXCEPTION USING
                ERRCODE = '23514',
                CONSTRAINT = 'managed_repository_ide_runtime_identity_required',
                MESSAGE = 'IDE runtime identity is required before destructive teardown';$new$;
BEGIN
    definition := pg_get_functiondef(
        'public.enforce_managed_repository_process_zero_transition()'::regprocedure
    );
    IF strpos(definition, new_fragment) > 0 THEN
        RETURN;
    END IF;
    IF strpos(definition, old_fragment) = 0
       OR strpos(substring(definition FROM strpos(definition, old_fragment) + 1), old_fragment) > 0 THEN
        RAISE EXCEPTION 'Unexpected process-zero transition definition';
    END IF;
    EXECUTE replace(definition, old_fragment, new_fragment);
END;
$migration$;

COMMIT;
