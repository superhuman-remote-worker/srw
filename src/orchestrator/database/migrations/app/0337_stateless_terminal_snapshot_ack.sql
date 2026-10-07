-- migration:     0337_stateless_terminal_snapshot_ack.sql
-- description:   Admit only the exact stateless emptyDir End snapshot proof and
--                retirement rebase flag clear through the workspace envelope.
-- depends-on:    0336_vm_adopted_stop_receipt_parity.sql
-- expected:      < 5s. Function definitions only; no row backfill.
-- locks:         Function-definition locks. Runtime checks lock owner -> queue.
-- transactional: yes

BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '5min';
SET LOCAL idle_in_transaction_session_timeout = '5min';
SET LOCAL timezone = 'UTC';

CREATE FUNCTION public.stateless_terminal_snapshot_flag_projection_authorized(
    requested_owner UUID, old_state JSONB, new_state JSONB
) RETURNS BOOLEAN LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    queue_row RECORD;
    old_marker JSONB;
    new_marker JSONB;
    old_workspace JSONB;
    new_workspace JSONB;
    binding JSONB;
    resident_ack JSONB;
    shell_ack JSONB;
    old_runtime TEXT;
    new_runtime TEXT;
    token BIGINT;
    replacement_has_receipt BOOLEAN;
    expected_marker JSONB;
    expected_state JSONB;
    replacement_ack JSONB;
    proof JSONB;
    proof_kind TEXT;
BEGIN
    IF jsonb_typeof(old_state) IS DISTINCT FROM 'object'
       OR jsonb_typeof(new_state) IS DISTINCT FROM 'object'
       OR old_state -> '_stateless_workspace_retirement_pending'
            IS DISTINCT FROM 'true'::JSONB
       OR old_state ?| ARRAY[
           '_stateless_workspace_retirement_settled',
           '_stateless_claim_losses', '_stateless_claim_loss_hold',
           '_stateless_active_claim'
       ]
       OR jsonb_typeof(old_state -> '_stateless_claim_retirement')
            IS DISTINCT FROM 'object'
       OR jsonb_typeof(old_state -> 'workspace_container')
            IS DISTINCT FROM 'object'
       OR jsonb_typeof(old_state -> '_workspace_binding')
            IS DISTINCT FROM 'object' THEN
        RETURN FALSE;
    END IF;
    old_marker := old_state -> '_stateless_claim_retirement';
    old_workspace := old_state -> 'workspace_container';
    new_workspace := new_state -> 'workspace_container';
    binding := old_state -> '_workspace_binding';
    resident_ack := old_state -> '_stateless_resident_retirement_ack';
    shell_ack := old_state -> '_stateless_shell_retirement_ack';
    old_runtime := old_marker ->> 'runtime_incarnation';
    IF (
        old_marker -> 'terminal_token' IS NOT NULL
        AND jsonb_typeof(old_marker -> 'terminal_token') = 'number'
        AND old_marker ->> 'terminal_token' ~ '^[1-9][0-9]*$'
        AND old_marker -> 'permanent' = 'false'::JSONB
        AND old_marker -> 'claimant_quiesced' = 'true'::JSONB
        AND old_marker -> 'resident_cleanup_required' = 'true'::JSONB
        AND old_marker -> 'shell_retirement_required' = 'true'::JSONB
        AND old_marker -> 'residents_retired' = 'true'::JSONB
        AND old_marker -> 'remote_retired' = 'true'::JSONB
        AND old_marker -> 'workspace_absence_proven' = 'false'::JSONB
        AND old_runtime ~ '^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
        AND jsonb_typeof(resident_ack) = 'object'
        AND jsonb_typeof(shell_ack) = 'object'
    ) IS NOT TRUE THEN
        RETURN FALSE;
    END IF;
    token := (old_marker ->> 'terminal_token')::BIGINT;
    SELECT * INTO queue_row FROM public.run_queue
     WHERE unit_id=requested_owner FOR UPDATE;
    IF NOT FOUND OR queue_row.unit_kind <> 'session_turn'
       OR queue_row.state <> 'done' OR queue_row.leased_by IS NOT NULL
       OR queue_row.lease_token <> token THEN
        RETURN FALSE;
    END IF;
    -- Match the authoritative parser's exact ACK tuple for both protocol and
    -- terminal-runtime proof. An explicitly null retired_by never defaults.
    FOREACH proof IN ARRAY ARRAY[resident_ack, shell_ack] LOOP
        proof_kind := proof ->> 'kind';
        IF proof_kind NOT IN ('protocol', 'workspace_runtime_terminal')
           OR proof -> 'terminal_token' IS DISTINCT FROM to_jsonb(token)
           OR proof ->> 'runtime_incarnation' IS DISTINCT FROM old_runtime
           OR (
               proof = resident_ack AND old_marker ? 'residents_retired_by'
               AND old_marker ->> 'residents_retired_by' IS DISTINCT FROM proof_kind
           ) OR (
               proof = shell_ack AND old_marker ? 'remote_retired_by'
               AND old_marker ->> 'remote_retired_by' IS DISTINCT FROM proof_kind
           ) OR (
               proof_kind = 'protocol' AND (
                   proof ->> 'workspace_generation' IS DISTINCT FROM
                       old_marker ->> 'workspace_generation'
                   OR proof ->> 'endpoint_generation' IS DISTINCT FROM
                       old_marker ->> 'endpoint_generation'
                   OR proof ->> 'host_key_fingerprint' IS DISTINCT FROM
                       old_marker ->> 'host_key_fingerprint'
               )
           ) THEN
            RETURN FALSE;
        END IF;
    END LOOP;
    IF old_marker ->> 'workspace_generation' IS DISTINCT FROM
           old_marker ->> 'endpoint_generation'
       OR old_marker ->> 'workspace_generation' !~
           '^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
       OR old_marker ->> 'host_key_fingerprint' !~ '^SHA256:[^[:space:]]+$'
       OR length(old_marker ->> 'host_key_fingerprint') > 128 THEN
        RETURN FALSE;
    END IF;

    -- A capture ACK changes precisely one flag on the same live runtime.
    IF COALESCE(old_workspace -> '_snapshot_restore_required', 'false'::JSONB)
            = 'false'::JSONB
       AND new_state = jsonb_set(
           old_state, '{workspace_container,_snapshot_restore_required}',
           'true'::JSONB, TRUE
       ) THEN
        RETURN (
            old_workspace ->> 'provisioner' = 'k8s'
            AND old_workspace ->> 'status' IN ('ready', 'retiring_process_zero')
            AND old_workspace ->> '_runtime_incarnation' = old_runtime
            AND old_workspace ->> '_canvas_workspace_generation' =
                old_marker ->> 'endpoint_generation'
            AND binding ->> 'kind' = 'remote'
            AND binding ->> 'generation' =
                old_marker ->> 'workspace_generation'
            AND binding ->> 'ssh_host_key_fingerprint' =
                old_marker ->> 'host_key_fingerprint'
            AND binding ->> 'backing_id' =
                'k8s-pod:' || (old_workspace ->> 'namespace') || ':' || old_runtime
            AND old_workspace ->> 'namespace' IS NOT NULL
            AND old_workspace ->> 'namespace' <> ''
            AND resident_ack ->> 'kind' = 'protocol'
            AND shell_ack ->> 'kind' = 'protocol'
        ) IS TRUE;
    END IF;

    -- Rebase replaces a retired runtime's marker/ACK tuple. The predecessor's
    -- flag cannot remain authority for the successor, even at the same token.
    IF COALESCE(old_workspace -> '_snapshot_restore_required', 'false'::JSONB)
            NOT IN ('false'::JSONB, 'true'::JSONB)
       OR jsonb_typeof(new_workspace) IS DISTINCT FROM 'object'
       OR new_workspace IS DISTINCT FROM jsonb_set(
           old_workspace, '{_snapshot_restore_required}', 'false'::JSONB, TRUE
       )
       OR jsonb_typeof(new_state -> '_stateless_claim_retirement')
            IS DISTINCT FROM 'object' THEN
        RETURN FALSE;
    END IF;
    new_marker := new_state -> '_stateless_claim_retirement';
    new_runtime := new_workspace ->> '_runtime_incarnation';
    replacement_has_receipt := EXISTS (
        SELECT 1 FROM public.managed_repository_process_zero_receipts
         WHERE owner_kind='thread' AND owner_id=requested_owner
           AND scope='workspace_container' AND provisioner='k8s'
           AND runtime_incarnation=new_runtime
    );
    IF (
        old_runtime <> new_runtime
        AND new_runtime ~ '^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
        AND new_workspace ->> 'provisioner' = 'k8s'
        AND binding ->> 'kind' = 'remote'
        AND binding ->> 'backing_id' =
            'k8s-pod:' || (new_workspace ->> 'namespace') || ':' || new_runtime
        AND new_workspace ->> 'namespace' IS NOT NULL
        AND new_workspace ->> 'namespace' <> ''
        AND binding ->> 'generation' =
            new_workspace ->> '_canvas_workspace_generation'
        AND binding ->> 'generation' ~
            '^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
        AND binding ->> 'ssh_host_key_fingerprint' ~ '^SHA256:[^[:space:]]+$'
        AND length(binding ->> 'ssh_host_key_fingerprint') <= 128
        AND (
            (new_workspace ->> 'status' = 'ready' AND NOT replacement_has_receipt)
            OR (new_workspace ->> 'status' IN ('retiring_process_zero', 'deleted')
                AND replacement_has_receipt)
        )
        AND EXISTS (
            SELECT 1 FROM public.managed_repository_process_zero_receipts
             WHERE owner_kind='thread' AND owner_id=requested_owner
               AND scope='workspace_container' AND provisioner='k8s'
               AND runtime_incarnation=old_runtime
        )
    ) IS NOT TRUE THEN
        RETURN FALSE;
    END IF;
    expected_marker := (old_marker - 'residents_retired_by' - 'remote_retired_by')
        || jsonb_build_object(
            'shell_retirement_required', TRUE,
            'resident_cleanup_required', TRUE,
            'residents_retired', replacement_has_receipt,
            'remote_retired', replacement_has_receipt,
            'workspace_absence_proven', FALSE,
            'workspace_generation', binding -> 'generation',
            'endpoint_generation', new_workspace -> '_canvas_workspace_generation',
            'runtime_incarnation', new_runtime,
            'host_key_fingerprint', binding -> 'ssh_host_key_fingerprint'
        );
    expected_state := jsonb_set(old_state,
        '{workspace_container,_snapshot_restore_required}', 'false'::JSONB, TRUE
    );
    expected_state := jsonb_set(expected_state,
        '{_stateless_claim_retirement}', expected_marker, TRUE
    ) - '_stateless_resident_retirement_ack'
      - '_stateless_shell_retirement_ack';
    IF replacement_has_receipt THEN
        expected_marker := expected_marker || jsonb_build_object(
            'residents_retired_by', 'workspace_runtime_terminal',
            'remote_retired_by', 'workspace_runtime_terminal'
        );
        replacement_ack := jsonb_build_object(
            'kind', 'workspace_runtime_terminal',
            'terminal_token', token,
            'runtime_incarnation', new_runtime
        );
        expected_state := jsonb_set(expected_state,
            '{_stateless_claim_retirement}', expected_marker, TRUE
        ) || jsonb_build_object(
            '_stateless_resident_retirement_ack', replacement_ack,
            '_stateless_shell_retirement_ack', replacement_ack
        );
    END IF;
    RETURN new_state = expected_state;
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
    RETURN FALSE;
END;
$$;

-- The current trigger body is repeated below with one narrowly scoped
-- snapshot/rebase projection exception. Existing migration bytes stay frozen.
CREATE OR REPLACE FUNCTION public.prevent_retired_workspace_runtime_rebinding() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    source_kind TEXT;
    source_id UUID;
    old_state JSONB;
    new_state JSONB;
    scope_name TEXT;
    state_key TEXT;
    old_runtime TEXT;
    new_runtime TEXT;
    old_status TEXT;
    new_status TEXT;
    new_reservation TEXT;
    new_claim_token TEXT;
    old_runtime_state JSONB;
    new_runtime_state JSONB;
    old_envelope JSONB;
    new_envelope JSONB;
    old_identity_envelope JSONB;
    new_identity_envelope JSONB;
    creation_authorized BOOLEAN;
    cancelled_creation_retirement_authorized BOOLEAN;
    uidless_creation_authorized BOOLEAN;
    cleanup_projection_authorized BOOLEAN;
    restore_projection_authorized BOOLEAN;
    cancelled_creation_projection_authorized BOOLEAN;
    cancel_claim_projection_authorized BOOLEAN;
    adoption_reversal_authorized BOOLEAN;
    terminal_cancel_projection_authorized BOOLEAN;
    safe_retirement_projection BOOLEAN;
    terminal_snapshot_projection_authorized BOOLEAN;
    managed_k8s_envelope BOOLEAN;
    uidless_k8s_candidate BOOLEAN;
    initial_uidless_precreate BOOLEAN;
    uidless_precreate_progress BOOLEAN;
    matching_pending BOOLEAN;
    owner_pending BOOLEAN;
    owner_unsettled_receipt BOOLEAN;
    has_receipt BOOLEAN;
    old_settled BOOLEAN;
    new_settled BOOLEAN;
BEGIN
    IF TG_TABLE_NAME = 'threads'
       AND to_jsonb(NEW) ->> 'execution_lane' = 'pinned' THEN
        RETURN NEW;
    END IF;
    source_kind := CASE WHEN TG_TABLE_NAME = 'jobs' THEN 'job' ELSE 'thread' END;
    source_id := NEW.id;
    old_state := CASE
        WHEN TG_OP = 'INSERT' THEN '{}'::JSONB
        WHEN TG_TABLE_NAME = 'jobs'
            THEN COALESCE(to_jsonb(OLD) -> 'context', '{}'::JSONB)
        ELSE COALESCE(to_jsonb(OLD) -> 'metadata', '{}'::JSONB)
    END;
    new_state := CASE
        WHEN TG_TABLE_NAME = 'jobs'
            THEN COALESCE(to_jsonb(NEW) -> 'context', '{}'::JSONB)
        ELSE COALESCE(to_jsonb(NEW) -> 'metadata', '{}'::JSONB)
    END;

    IF source_kind = 'thread'
       AND to_jsonb(NEW) ->> 'execution_lane' = 'stateless'
       AND jsonb_typeof(new_state #> ARRAY[
           'workspace_container', '_runtime_creation'
       ]) = 'object'
       AND new_state #>> ARRAY[
           'workspace_container', '_runtime_creation', 'generation'
       ] IS DISTINCT FROM to_jsonb(NEW) ->> 'runtime_generation' THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            CONSTRAINT = 'stateless_workspace_runtime_generation_mismatch',
            MESSAGE = 'Stateless workspace projection must match the thread runtime generation';
    END IF;

    FOREACH scope_name IN ARRAY ARRAY['workspace_container', 'ide'] LOOP
        IF scope_name = 'ide' AND source_kind <> 'job' THEN
            CONTINUE;
        END IF;
        state_key := CASE WHEN scope_name = 'ide' THEN 'ide_session'
                          ELSE 'workspace_container' END;
        old_runtime := old_state #>> ARRAY[state_key, '_runtime_incarnation'];
        new_runtime := new_state #>> ARRAY[state_key, '_runtime_incarnation'];
        old_status := old_state #>> ARRAY[state_key, 'status'];
        -- Restoring the UID of a settled permanent cleanup is a terminal
        -- projection, never a new runtime binding. All other fields and the
        -- current owner/queue/receipt tuple must agree before this exception.
        IF source_kind = 'thread' AND TG_OP = 'UPDATE'
           AND scope_name = 'workspace_container'
           AND to_jsonb(NEW) ->> 'status' = to_jsonb(OLD) ->> 'status'
           AND to_jsonb(NEW) ->> 'status' = 'ended'
           AND to_jsonb(NEW) ->> 'execution_lane' = to_jsonb(OLD) ->> 'execution_lane'
           AND to_jsonb(NEW) ->> 'runtime_generation' = to_jsonb(OLD) ->> 'runtime_generation'
           AND public.stateless_terminal_reclaim_projection_is_authorized(
               source_id, new_runtime, old_state, new_state
           ) THEN
            CONTINUE;
        END IF;
        new_status := new_state #>> ARRAY[state_key, 'status'];
        new_reservation := new_state #>> ARRAY[
            state_key, '_creation_reservation_id'
        ];
        new_claim_token := new_state #>> ARRAY[
            state_key, '_creation_claim_token'
        ];
        old_runtime_state := old_state -> state_key;
        new_runtime_state := new_state -> state_key;
        managed_k8s_envelope := old_runtime IS NOT NULL
            OR new_runtime IS NOT NULL
            OR old_state #>> ARRAY[state_key, '_creation_reservation_id']
                IS NOT NULL
            OR new_reservation IS NOT NULL
            OR (
                scope_name = 'workspace_container'
                AND (
                    old_state #>> ARRAY[state_key, 'provisioner'] = 'k8s'
                    OR new_state #>> ARRAY[state_key, 'provisioner'] = 'k8s'
                )
            )
            OR (
                scope_name = 'ide'
                AND (
                    old_state #>> ARRAY[state_key, 'restore_type'] =
                        'k8s_container'
                    OR new_state #>> ARRAY[state_key, 'restore_type'] =
                        'k8s_container'
                )
            );
        uidless_k8s_candidate := old_runtime IS NULL
            AND jsonb_typeof(old_runtime_state) = 'object'
            AND (
                (
                    scope_name = 'workspace_container'
                    AND (
                        old_runtime_state ->> 'provisioner' = 'k8s'
                        OR (
                            NOT (old_runtime_state ? 'provisioner')
                            AND NOT (old_runtime_state ? 'container_id')
                        )
                    )
                )
                OR (
                    scope_name = 'ide'
                    AND (
                        old_runtime_state ->> 'restore_type' = 'k8s_container'
                        OR (
                            NOT (old_runtime_state ? 'restore_type')
                            AND NOT (old_runtime_state ? 'container_id')
                        )
                    )
                )
            );
        old_envelope := public.managed_repository_workspace_authority_envelope(
            old_state, scope_name
        );
        new_envelope := public.managed_repository_workspace_authority_envelope(
            new_state, scope_name
        );
        old_identity_envelope := old_envelope - ARRAY[
            'status', '_runtime_incarnation', '_creation_reservation_id',
            '_creation_claim_token', '_snapshot_restore_required'
        ];
        new_identity_envelope := new_envelope - ARRAY[
            'status', '_runtime_incarnation', '_creation_reservation_id',
            '_creation_claim_token', '_snapshot_restore_required'
        ];
        creation_authorized := new_runtime IS NOT NULL AND
            public.managed_repository_workspace_creation_is_authorized(
                source_kind, source_id, scope_name, new_runtime,
                new_reservation, new_claim_token
            );
        cancelled_creation_retirement_authorized :=
            TG_OP = 'UPDATE' AND NEW.id = OLD.id
            AND source_kind = 'job'
            AND NEW.status::TEXT = 'cancelled' AND OLD.status::TEXT = 'cancelled'
            AND to_jsonb(NEW) ->> 'execution_lane' = 'stateless'
            AND to_jsonb(OLD) ->> 'execution_lane' = 'stateless'
            AND to_jsonb(NEW) ->> 'assigned_agent_id' IS NULL
            AND to_jsonb(OLD) ->> 'assigned_agent_id' IS NULL
            AND public.managed_repo_cancelled_creation_retirement_is_authorized_now(
                source_kind, source_id, scope_name, old_state, new_state
            );
        uidless_creation_authorized := new_runtime IS NULL AND
            public.managed_repository_workspace_uidless_creation_is_authorized(
                source_kind, source_id, scope_name,
                new_reservation, new_claim_token
            );
        cleanup_projection_authorized := old_runtime IS NOT NULL AND
            public.managed_repo_workspace_cleanup_projection_authorized_now(
                source_kind, source_id, scope_name, old_runtime,
                old_state, new_state
            );
        restore_projection_authorized := new_runtime IS NOT NULL AND
            public.managed_repo_workspace_restore_projection_authorized_now(
                source_kind, source_id, scope_name, new_runtime,
                new_reservation, new_claim_token, old_state, new_state
            );
        cancelled_creation_projection_authorized :=
            public.managed_repo_cancelled_creation_projection_authorized_now(
                source_kind, source_id, scope_name, old_state, new_state
            );
        cancel_claim_projection_authorized :=
            public.managed_repo_cancel_claim_projection_authorized_now(
                source_kind, source_id, scope_name, new_runtime,
                new_reservation, new_claim_token, old_state, new_state
            );
        terminal_cancel_projection_authorized :=
            public.managed_repo_terminal_cancel_projection_authorized_now(
                source_kind, source_id, scope_name, new_runtime,
                new_reservation, new_claim_token, old_state, new_state
            );
        adoption_reversal_authorized := old_runtime IS NOT NULL
            AND new_runtime IS NULL
            AND public.managed_repo_adoption_reversal_authorized_now(
                source_kind, source_id, scope_name, old_runtime,
                old_state, new_state
            );
        safe_retirement_projection := old_runtime IS NOT NULL
            AND new_runtime = old_runtime
            AND new_status = 'retiring_process_zero'
            AND (old_envelope - 'status') = (new_envelope - 'status');
        terminal_snapshot_projection_authorized := FALSE;
        IF source_kind = 'thread' AND scope_name = 'workspace_container'
           AND TG_OP = 'UPDATE'
           AND to_jsonb(OLD) ->> 'status' = 'ended'
           AND to_jsonb(NEW) ->> 'status' = 'ended'
           AND to_jsonb(OLD) ->> 'execution_lane' = 'stateless'
           AND to_jsonb(NEW) ->> 'execution_lane' = 'stateless'
           AND old_envelope IS DISTINCT FROM new_envelope THEN
            terminal_snapshot_projection_authorized :=
                public.stateless_terminal_snapshot_flag_projection_authorized(
                    source_id, old_state, new_state
                );
        END IF;
        initial_uidless_precreate := new_runtime IS NULL
            AND new_status IN ('pending', 'creating', 'restoring')
            AND (
                TG_OP = 'INSERT'
                OR old_runtime_state IS NULL
                OR old_runtime_state = '{}'::JSONB
            );
        uidless_precreate_progress := TG_OP = 'UPDATE'
            AND old_runtime IS NULL
            AND new_runtime IS NULL
            AND old_status IN ('pending', 'creating', 'restoring')
            AND new_status IN ('pending', 'creating', 'restoring')
            AND old_identity_envelope = new_identity_envelope;

        IF TG_OP = 'UPDATE'
           AND old_runtime IS NULL
           AND uidless_k8s_candidate
           AND jsonb_typeof(old_runtime_state) = 'object'
           AND old_runtime_state <> '{}'::JSONB
           AND (
               old_identity_envelope IS DISTINCT FROM new_identity_envelope
               OR (
                   old_status IN (
                       'failed', 'deleted', 'retiring_process_zero',
                       'expired', 'cleanup_pending', 'suspended'
                   )
                   AND new_status IN (
                       'pending', 'creating', 'created', 'restoring',
                       'ready', 'active', 'idle'
                   )
               )
           )
           AND NOT creation_authorized
           AND NOT cancelled_creation_retirement_authorized
           AND NOT uidless_creation_authorized
           AND NOT cleanup_projection_authorized
           AND NOT restore_projection_authorized
           AND NOT cancelled_creation_projection_authorized
           AND NOT cancel_claim_projection_authorized
           AND NOT terminal_cancel_projection_authorized
           AND NOT (
               scope_name = 'ide'
               AND public.vm_ide_heartbeat_cleanup_is_authorized(
                   source_kind, source_id, old_state, new_state
               )
           ) THEN
            RAISE EXCEPTION USING
                ERRCODE = '23514',
                CONSTRAINT = CASE WHEN scope_name = 'ide'
                    THEN 'managed_repository_uidless_ide_runtime_transition_forbidden'
                    ELSE 'managed_repository_uidless_workspace_runtime_transition_forbidden' END,
                MESSAGE = 'A non-empty UID-less Kubernetes runtime cannot be recycled without exact authority';
        END IF;

        SELECT
            EXISTS (
                SELECT 1
                  FROM public.managed_repository_workspace_cleanup_intents AS intent
                 WHERE intent.owner_kind = source_kind
                   AND intent.owner_id = source_id
                   AND intent.scope = scope_name
                   AND intent.settled_at IS NULL
            ),
            EXISTS (
                SELECT 1
                  FROM public.managed_repository_workspace_cleanup_intents AS intent
                 WHERE intent.owner_kind = source_kind
                   AND intent.owner_id = source_id
                   AND intent.scope = scope_name
                   AND intent.runtime_incarnation::TEXT = old_runtime
                   AND intent.settled_at IS NULL
            ),
            EXISTS (
                SELECT 1
                  FROM public.managed_repository_process_zero_receipts AS receipt
                 WHERE receipt.owner_kind = source_kind
                   AND receipt.owner_id = source_id
                   AND receipt.provisioner = 'k8s'
                   AND receipt.scope IN (
                       scope_name,
                       CASE WHEN scope_name = 'workspace_container'
                            AND source_kind = 'thread'
                            THEN 'stateless_workspace'
                            ELSE scope_name END
                   )
                   AND NOT EXISTS (
                       SELECT 1
                         FROM public.managed_repository_workspace_cleanup_intents AS intent
                        WHERE intent.owner_kind = source_kind
                          AND intent.owner_id = source_id
                          AND intent.scope = scope_name
                          AND intent.runtime_incarnation::TEXT =
                              receipt.runtime_incarnation
                          AND intent.result_kind IN ('settled', 'superseded')
                   )
            )
          INTO owner_pending, matching_pending, owner_unsettled_receipt;

        IF old_runtime IS NULL AND (owner_pending OR owner_unsettled_receipt)
           AND (
               new_runtime IS DISTINCT FROM old_runtime
               OR new_status IS DISTINCT FROM old_status
           ) THEN
            RAISE EXCEPTION USING
                ERRCODE = '23514',
                CONSTRAINT = CASE WHEN scope_name = 'ide'
                    THEN 'managed_repository_ide_cleanup_in_progress'
                    ELSE 'managed_repository_workspace_cleanup_in_progress' END,
                MESSAGE = 'A Kubernetes runtime may not change before exact cleanup settlement';
        END IF;

        IF old_runtime IS NOT NULL THEN
            has_receipt := public.managed_repository_workspace_has_process_zero_receipt(
                source_kind, source_id, scope_name, old_runtime
            );
            old_settled := public.managed_repository_workspace_cleanup_projection_is_settled(
                source_kind, source_id, scope_name, old_runtime,
                old_runtime, old_status
            );
            new_settled := public.managed_repository_workspace_cleanup_projection_is_settled(
                source_kind, source_id, scope_name, old_runtime,
                new_runtime, new_status
            );

            IF (matching_pending OR (has_receipt AND NOT old_settled))
               AND (
                   new_runtime IS DISTINCT FROM old_runtime
                   OR new_status IS DISTINCT FROM old_status
               )
               AND NOT (
                   matching_pending
                   AND new_runtime = old_runtime
                   AND new_status = 'retiring_process_zero'
               )
               AND NOT new_settled THEN
                RAISE EXCEPTION USING
                    ERRCODE = '23514',
                    CONSTRAINT = CASE WHEN scope_name = 'ide'
                        THEN 'managed_repository_ide_cleanup_in_progress'
                        ELSE 'managed_repository_workspace_cleanup_in_progress' END,
                    MESSAGE = 'A Kubernetes runtime may not change before exact cleanup settlement';
            END IF;

            IF has_receipt AND old_settled
               AND new_runtime = old_runtime
               AND new_status IS DISTINCT FROM old_status THEN
                RAISE EXCEPTION USING
                    ERRCODE = '23514',
                    CONSTRAINT = CASE WHEN scope_name = 'ide'
                        THEN 'managed_repository_retired_ide_runtime_reactivation'
                        ELSE 'managed_repository_retired_workspace_runtime_reactivation' END,
                    MESSAGE = 'A settled retired runtime may not be reactivated';
            END IF;
        END IF;

        IF new_runtime IS DISTINCT FROM old_runtime
           AND new_runtime IS NOT NULL THEN
            IF public.managed_repository_workspace_has_process_zero_receipt(
                source_kind, source_id, scope_name, new_runtime
            ) THEN
                RAISE EXCEPTION USING
                    ERRCODE = '23514',
                    CONSTRAINT = CASE WHEN scope_name = 'ide'
                        THEN 'managed_repository_retired_ide_runtime_rebind'
                        ELSE 'managed_repository_retired_workspace_runtime_rebind' END,
                    MESSAGE = 'A retired Kubernetes runtime may not be rebound';
            END IF;
            IF NOT creation_authorized
               AND NOT cancelled_creation_retirement_authorized THEN
                RAISE EXCEPTION USING
                    ERRCODE = '23514',
                    CONSTRAINT = CASE WHEN scope_name = 'ide'
                        THEN 'managed_repository_ide_creation_reservation_required'
                        ELSE 'managed_repository_workspace_creation_reservation_required' END,
                    MESSAGE = 'A new Kubernetes runtime requires exact creation reservation authority';
            END IF;
        END IF;

        IF managed_k8s_envelope
           AND old_envelope IS DISTINCT FROM new_envelope
           AND NOT creation_authorized
           AND NOT cancelled_creation_retirement_authorized
           AND NOT uidless_creation_authorized
           AND NOT cleanup_projection_authorized
           AND NOT restore_projection_authorized
           AND NOT cancelled_creation_projection_authorized
           AND NOT cancel_claim_projection_authorized
           AND NOT terminal_cancel_projection_authorized
           AND NOT safe_retirement_projection
           AND NOT terminal_snapshot_projection_authorized
           AND NOT adoption_reversal_authorized
           AND NOT initial_uidless_precreate
           AND NOT uidless_precreate_progress THEN
            RAISE EXCEPTION USING
                ERRCODE = '23514',
                CONSTRAINT = CASE WHEN scope_name = 'ide'
                    THEN 'managed_repository_ide_authority_envelope_immutable'
                    ELSE 'managed_repository_workspace_authority_envelope_immutable' END,
                MESSAGE = 'Kubernetes runtime authority fields require exact durable authority';
        END IF;
    END LOOP;
    RETURN NEW;
END;
$$;

COMMIT;
