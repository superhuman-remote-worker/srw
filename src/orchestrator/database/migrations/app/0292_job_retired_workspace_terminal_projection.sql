-- migration: 0292_job_retired_workspace_terminal_projection.sql
-- description: Admit terminal storage cleanup without reactivating a settled deleted Job runtime.
-- depends-on: 0291_vm_thread_source_actor_provenance.sql
-- transactional: yes
-- expected: < 5s. Function replacement only; no row backfill.
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';
SET LOCAL idle_in_transaction_session_timeout = '30s';

CREATE OR REPLACE FUNCTION public.cancel_workspace_creation_on_terminal_owner_transition() RETURNS trigger
    LANGUAGE plpgsql
    AS $_$
DECLARE
    source_kind TEXT;
    source_state JSONB;
    state_key TEXT;
    reservation RECORD;
    rotated_token BIGINT;
    thread_terminal_token BIGINT;
    locked_queue_token BIGINT;
    thread_terminal_reclaim BOOLEAN := FALSE;
    desired_resource_policy TEXT;
    scope_name TEXT;
    runtime_state JSONB;
    runtime_uid TEXT;
    inserted_cleanup UUID;
BEGIN
    IF NEW.status IS NOT DISTINCT FROM OLD.status OR NOT (
        (TG_TABLE_NAME = 'jobs' AND NEW.status::TEXT IN (
            'completed', 'failed', 'cancelled'
        ))
        OR (TG_TABLE_NAME = 'threads' AND NEW.status::TEXT = 'ended')
    ) THEN
        RETURN NEW;
    END IF;

    IF TG_TABLE_NAME = 'threads' AND NEW.execution_lane = 'pinned' THEN
        RETURN NEW;
    END IF;

    source_kind := CASE WHEN TG_TABLE_NAME = 'jobs' THEN 'job' ELSE 'thread' END;
    source_state := CASE WHEN TG_TABLE_NAME = 'jobs'
        THEN COALESCE(to_jsonb(NEW) -> 'context', '{}'::JSONB)
        ELSE COALESCE(to_jsonb(NEW) -> 'metadata', '{}'::JSONB)
    END;

    -- Do not wait while the row-update already owns the owner lock: an
    -- external creator holds the matching session lock and must reacquire the
    -- owner row to persist its exact observed UID, so blocking here would
    -- deadlock.  A terminal writer instead acquires the same domain with a
    -- non-blocking transaction lock or fails atomically and retries after the
    -- bounded, joined Kubernetes mutation completes.  All token rotation and
    -- cancellation below therefore occur under the shared owner/scope guard.
    IF NOT pg_try_advisory_xact_lock(hashtextextended(
        'workspace_runtime_mutation:' || source_kind || ':'
            || NEW.id::TEXT || ':workspace_container', 0
    )) THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001',
            MESSAGE = 'Workspace mutation is still in progress';
    END IF;
    IF source_kind = 'job' AND NOT pg_try_advisory_xact_lock(hashtextextended(
        'workspace_runtime_mutation:' || source_kind || ':'
            || NEW.id::TEXT || ':ide', 0
    )) THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001',
            MESSAGE = 'IDE mutation is still in progress';
    END IF;
    IF source_kind = 'thread'
       AND (source_state #>> ARRAY[
           '_stateless_claim_retirement', 'permanent'
       ]) = 'true' THEN
        BEGIN
            thread_terminal_token := (
                source_state #>> ARRAY[
                    '_stateless_claim_retirement', 'terminal_token'
                ]
            )::BIGINT;
        EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
            thread_terminal_token := NULL;
        END;
        IF thread_terminal_token IS NOT NULL AND thread_terminal_token > 0 THEN
            SELECT lease_token
              INTO locked_queue_token
              FROM public.run_queue
             WHERE unit_id = NEW.id
               AND unit_kind = 'session_turn'
               AND state = 'done'
               AND lease_token = thread_terminal_token
               AND leased_by IS NULL
             FOR UPDATE;
            thread_terminal_reclaim := locked_queue_token IS NOT NULL;
        END IF;
    END IF;

    -- A terminal owner cannot admit or continue post-create restore work.
    -- Clearing the lease under the owner-row lock makes the running claimant's
    -- next renewal fail; the settled creation receipt remains as exact-B
    -- evidence for subsequent cleanup.
    UPDATE public.managed_repository_workspace_creation_reservations
       SET restore_work_claimed_by = NULL,
           restore_work_claim_expires_at = NULL,
           restore_work_next_attempt_at = now()
     WHERE owner_kind = source_kind
       AND owner_id = NEW.id
       AND operation_kind = 'restore'
       AND result_kind = 'settled'
       AND restore_work_completed_at IS NULL;

    FOR reservation IN
        SELECT *
          FROM public.managed_repository_workspace_creation_reservations
         WHERE owner_kind = source_kind
           AND owner_id = NEW.id
           AND settled_at IS NULL
         ORDER BY scope
         FOR UPDATE
    LOOP
        desired_resource_policy := CASE
            WHEN reservation.scope = 'workspace_container'
                 AND (
                     source_kind = 'job'
                     OR (source_kind = 'thread' AND thread_terminal_reclaim)
                 )
            THEN 'terminal_reclaim'
            ELSE 'preserve'
        END;
        IF reservation.phase = 'reserved'
           AND reservation.external_mutation_started_at IS NULL THEN
            UPDATE public.managed_repository_workspace_creation_reservations
               SET cancel_requested_at = now(),
                   cancel_target_disposition = CASE
                       WHEN reservation.scope = 'ide' THEN 'deleted'
                       ELSE 'deleted'
                   END,
                   cancel_resource_policy = desired_resource_policy,
                   cancel_snapshot_restore_required = FALSE,
                   settled_at = now(),
                   phase = 'aborted',
                   result_kind = 'aborted'
             WHERE id = reservation.id;
            CONTINUE;
        END IF;

        rotated_token := nextval(
            'public.managed_repository_workspace_creation_claim_seq'
        );
        UPDATE public.managed_repository_workspace_creation_reservations
           SET cancel_requested_at = COALESCE(cancel_requested_at, now()),
               cancel_target_disposition = 'deleted',
               cancel_resource_policy = desired_resource_policy,
               cancel_suspended_at = NULL,
               cancel_snapshot_restore_required = FALSE,
               claimed_by = 'terminal-owner-transition',
               claim_token = rotated_token,
               expires_at = GREATEST(
                   now() + INTERVAL '1 millisecond',
                   created_at + INTERVAL '1 millisecond'
               ),
               attempts = attempts + 1,
               next_attempt_at = now()
         WHERE id = reservation.id;

        state_key := CASE WHEN reservation.scope = 'ide'
            THEN 'ide_session' ELSE 'workspace_container' END;
        IF reservation.runtime_incarnation IS NOT NULL
           AND source_state #>> ARRAY[
               state_key, '_runtime_incarnation'
           ] = reservation.runtime_incarnation::TEXT
           AND source_state #>> ARRAY[
               state_key, '_creation_reservation_id'
           ] = reservation.id::TEXT
           AND source_state #>> ARRAY[
               state_key, '_creation_claim_token'
           ] = reservation.claim_token::TEXT THEN
            source_state := jsonb_set(
                source_state,
                ARRAY[state_key, '_creation_claim_token'],
                to_jsonb(rotated_token::TEXT),
                FALSE
            );
        END IF;
    END LOOP;

    -- Class-A terminal state is also the durable cleanup admission point for
    -- a runtime whose creation generation already settled.  The trigger does
    -- not claim external work; it freezes the exact UID and leaves resource
    -- capture/deletion to the guarded reconciler.
    FOR scope_name, state_key IN
        SELECT * FROM (VALUES
            ('workspace_container'::TEXT, 'workspace_container'::TEXT),
            ('ide'::TEXT, 'ide_session'::TEXT)
        ) AS scopes(scope_name, state_key)
    LOOP
        IF source_kind = 'thread' AND scope_name = 'ide' THEN
            CONTINUE;
        END IF;
        runtime_state := source_state -> state_key;
        IF jsonb_typeof(runtime_state) <> 'object'
           OR (scope_name = 'workspace_container'
               AND runtime_state ->> 'provisioner' <> 'k8s')
           OR (scope_name = 'ide'
               AND runtime_state ->> 'restore_type' <> 'k8s_container') THEN
            CONTINUE;
        END IF;
        runtime_uid := runtime_state ->> '_runtime_incarnation';
        IF runtime_uid IS NULL
           OR runtime_uid !~* '^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' THEN
            CONTINUE;
        END IF;
        desired_resource_policy := CASE
            WHEN source_kind = 'job' THEN 'terminal_reclaim'
            WHEN scope_name = 'workspace_container' AND thread_terminal_reclaim
                THEN 'terminal_reclaim'
            ELSE 'preserve'
        END;
        inserted_cleanup := NULL;

        -- A terminal Class-A transition must not lose to a cleanup generation
        -- admitted just before it took the owner lock.  Promote that exact
        -- live-runtime generation in place; the one-active-per-scope index
        -- then remains the serialization backstop.  Ambiguous discovery is
        -- now exact because the owner projection supplies the immutable UID.
        UPDATE public.managed_repository_workspace_cleanup_intents
           SET intent_source = 'current',
               admission_source = 'explicit',
               target_disposition = 'deleted',
               resource_policy = desired_resource_policy,
               reclaim_shared_resources = (
                   scope_name = 'workspace_container'
                   AND desired_resource_policy = 'terminal_reclaim'
               ),
               lifecycle_fingerprint = lifecycle_fingerprint
                   || jsonb_build_object(
                       'owner_status', NEW.status::TEXT,
                       'runtime_status', COALESCE(
                           runtime_state ->> 'status', ''
                       ),
                       'admitted_by', 'terminal_owner_transition'
                   ),
               terminal_queue_token = CASE
                   WHEN source_kind = 'thread' THEN locked_queue_token
                   ELSE 0
               END,
               suspended_at = NULL,
               snapshot_restore_required = FALSE,
               phase = CASE WHEN phase = 'ambiguous'
                   THEN 'prepared' ELSE phase END,
               next_attempt_at = now()
         WHERE owner_kind = source_kind
           AND owner_id = NEW.id
           AND scope = scope_name
           AND runtime_incarnation::TEXT = runtime_uid
           AND settled_at IS NULL
        RETURNING id INTO inserted_cleanup;

        IF inserted_cleanup IS NULL THEN
        INSERT INTO public.managed_repository_workspace_cleanup_intents (
            owner_kind, owner_id, thread_runtime_generation,
            scope, runtime_incarnation,
            intent_source, admission_source, target_disposition,
            resource_policy, reclaim_shared_resources,
            lifecycle_fingerprint, terminal_queue_token, pod_uid,
            capture_complete, snapshot_restore_required, phase
        ) VALUES (
            source_kind, NEW.id,
            CASE WHEN source_kind = 'thread'
                 THEN (to_jsonb(NEW) ->> 'runtime_generation')::UUID
                 ELSE NULL END,
            scope_name, runtime_uid::UUID,
            'current', 'explicit', 'deleted', desired_resource_policy,
            (scope_name = 'workspace_container'
                AND desired_resource_policy = 'terminal_reclaim'),
            jsonb_build_object(
                'owner_status', NEW.status::TEXT,
                'runtime_status', COALESCE(runtime_state ->> 'status', ''),
                'admitted_by', 'terminal_owner_transition'
            ),
            CASE WHEN source_kind = 'thread' THEN locked_queue_token ELSE 0 END,
            runtime_uid::UUID, FALSE, FALSE, 'prepared'
        ) ON CONFLICT (
            owner_kind, owner_id, scope, runtime_incarnation,
            target_disposition, resource_policy
        ) DO NOTHING
        RETURNING id INTO inserted_cleanup;
        END IF;

        IF EXISTS (
            SELECT 1
              FROM public.managed_repository_workspace_cleanup_intents AS intent
             WHERE intent.owner_kind = source_kind
               AND intent.owner_id = NEW.id
               AND intent.scope = scope_name
               AND intent.runtime_incarnation::TEXT = runtime_uid
               AND intent.result_kind IS NULL
        ) AND NOT (
            -- A settled deleted Job runtime already reached process zero.
            -- Admit its new terminal storage intent above, but retain the
            -- immutable retired projection instead of reactivating its UID.
            source_kind = 'job'
            AND scope_name = 'workspace_container'
            AND runtime_state ->> 'status' = 'deleted'
            AND public.managed_repository_workspace_has_process_zero_receipt(
                source_kind, NEW.id, scope_name, runtime_uid
            )
            AND public.managed_repository_workspace_cleanup_projection_is_settled(
                source_kind, NEW.id, scope_name, runtime_uid,
                runtime_uid, 'deleted'
            )
        ) THEN
            runtime_state := jsonb_set(
                runtime_state, ARRAY['status'],
                to_jsonb('retiring_process_zero'::TEXT), TRUE
            );
            source_state := jsonb_set(
                source_state, ARRAY[state_key], runtime_state, TRUE
            );
        END IF;
    END LOOP;

    IF TG_TABLE_NAME = 'jobs' THEN
        NEW := jsonb_populate_record(
            NEW, jsonb_build_object('context', source_state)
        );
    ELSE
        NEW := jsonb_populate_record(
            NEW, jsonb_build_object('metadata', source_state)
        );
    END IF;
    RETURN NEW;
END;
$_$;

COMMIT;
