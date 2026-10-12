-- migration: 0332_cancelled_unbound_workspace_retirement.sql
-- description: Bind only exact retirement authority for a cancelled Job's accepted CREATE.
-- depends-on: 0331_pinned_pvc_create_response_receipts.sql
-- transactional: yes
-- expected: < 5s. Function definitions only; no row backfill.
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

CREATE FUNCTION public.managed_repo_cancelled_creation_retirement_is_authorized_now(
    requested_owner_kind TEXT,
    requested_owner_id UUID,
    requested_scope TEXT,
    old_state JSONB,
    new_state JSONB
)
RETURNS BOOLEAN LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    owner_row RECORD;
    reservation RECORD;
    old_workspace JSONB;
    new_workspace JSONB;
    effect JSONB;
    resource_kind TEXT;
    resource_uid TEXT;
BEGIN
    -- This is retirement authority, never creator or Session authority.
    IF requested_owner_kind IS DISTINCT FROM 'job'
       OR requested_scope IS DISTINCT FROM 'workspace_container'
       OR jsonb_typeof(old_state) IS DISTINCT FROM 'object'
       OR jsonb_typeof(new_state) IS DISTINCT FROM 'object' THEN
       RETURN FALSE;
    END IF;
    IF EXISTS (
        SELECT 1 FROM jsonb_each(
            public.managed_repository_workspace_authority_envelope(
                old_state, 'workspace_container'
            )
        ) AS authority
        WHERE authority.value IS DISTINCT FROM 'null'::JSONB
    ) THEN
        -- Preserve diagnostics, never stale physical or connection authority.
        RETURN FALSE;
    END IF;
    old_workspace := old_state -> 'workspace_container';
    IF old_workspace IS NULL THEN
        old_workspace := '{}'::JSONB;
    END IF;
    new_workspace := new_state -> 'workspace_container';
    IF jsonb_typeof(old_workspace) IS DISTINCT FROM 'object'
       OR jsonb_typeof(new_workspace) IS DISTINCT FROM 'object'
       OR old_workspace ?| ARRAY[
           'status', 'provisioner', '_runtime_incarnation',
           '_creation_reservation_id', '_creation_claim_token', 'container_id'
       ]
       OR old_state ? '_workspace_binding'
       OR (new_workspace ->> 'status') IS DISTINCT FROM 'retiring_process_zero'
       OR (new_workspace ->> 'provisioner') IS DISTINCT FROM 'k8s'
       OR (old_state - 'workspace_container') IS DISTINCT FROM
          (new_state - 'workspace_container') THEN
        RETURN FALSE;
    END IF;
    SELECT status::TEXT AS status, execution_lane, assigned_agent_id, context
      INTO owner_row FROM public.jobs
     WHERE id = requested_owner_id FOR UPDATE;
    IF NOT FOUND OR owner_row.status IS DISTINCT FROM 'cancelled'
       OR owner_row.execution_lane IS DISTINCT FROM 'stateless'
       OR owner_row.assigned_agent_id IS NOT NULL
       OR owner_row.context IS DISTINCT FROM old_state THEN
        RETURN FALSE;
    END IF;
    SELECT * INTO reservation
      FROM public.managed_repository_workspace_creation_reservations
     WHERE owner_kind = 'job' AND owner_id = requested_owner_id
       AND scope = 'workspace_container'
       AND id::TEXT = new_workspace ->> '_creation_reservation_id'
       AND claim_token::TEXT = new_workspace ->> '_creation_claim_token'
       AND runtime_incarnation::TEXT = new_workspace ->> '_runtime_incarnation'
     FOR UPDATE;
    IF NOT FOUND OR reservation.operation_kind IS DISTINCT FROM 'create'
       OR reservation.phase IS DISTINCT FROM 'runtime_bound'
       OR reservation.settled_at IS NOT NULL
       OR reservation.cancel_requested_at IS NULL
       OR reservation.expires_at <= clock_timestamp()
       OR reservation.claimed_by IS NULL
       OR reservation.claim_token <= 0
       OR reservation.runtime_incarnation IS NULL
       OR reservation.pod_uid IS DISTINCT FROM reservation.runtime_incarnation
       OR reservation.cancel_target_disposition IS DISTINCT FROM 'deleted'
       OR reservation.cancel_resource_policy IS DISTINCT FROM 'terminal_reclaim'
       OR reservation.cancel_suspended_at IS NOT NULL
       OR reservation.cancel_snapshot_restore_required IS DISTINCT FROM FALSE
       OR jsonb_typeof(reservation.external_effects) IS DISTINCT FROM 'object'
       OR new_state IS DISTINCT FROM jsonb_set(
           old_state, '{workspace_container}', old_workspace || jsonb_build_object(
               'status', 'retiring_process_zero', 'provisioner', 'k8s',
               '_runtime_incarnation', reservation.runtime_incarnation::TEXT,
               '_creation_reservation_id', reservation.id::TEXT,
               '_creation_claim_token', reservation.claim_token::TEXT
           ), TRUE
       ) THEN
        RETURN FALSE;
    END IF;
    IF (SELECT count(*) FROM public.managed_repository_workspace_creation_reservations
         WHERE owner_kind = 'job' AND owner_id = requested_owner_id
           AND scope = 'workspace_container' AND settled_at IS NULL) <> 1
       OR EXISTS (SELECT 1 FROM public.managed_repository_workspace_cleanup_intents
           WHERE owner_kind = 'job' AND owner_id = requested_owner_id
             AND scope = 'workspace_container'
             AND (settled_at IS NULL
                  OR runtime_incarnation = reservation.runtime_incarnation))
       OR public.managed_repository_workspace_has_process_zero_receipt(
           'job', requested_owner_id, 'workspace_container',
           reservation.runtime_incarnation::TEXT
       ) THEN
        RETURN FALSE;
    END IF;
    IF EXISTS (SELECT 1 FROM jsonb_object_keys(reservation.external_effects) AS key
               WHERE key NOT IN ('pod', 'pvc', 'seed', 'service')) THEN
        RETURN FALSE;
    END IF;
    FOR resource_kind, resource_uid IN
        SELECT * FROM (VALUES
            ('pod'::TEXT, reservation.pod_uid::TEXT),
            ('pvc'::TEXT, reservation.pvc_uid::TEXT),
            ('seed'::TEXT, reservation.seed_configmap_uid::TEXT),
            ('service'::TEXT, reservation.service_uid::TEXT)
        ) AS resources(kind, uid)
    LOOP
        effect := reservation.external_effects -> resource_kind;
        IF effect IS NULL AND resource_uid IS NULL THEN
            CONTINUE;
        END IF;
        IF jsonb_typeof(effect) IS DISTINCT FROM 'object'
           OR jsonb_typeof(effect -> 'issued_at') IS DISTINCT FROM 'string'
           OR jsonb_typeof(effect -> 'claim_token') IS DISTINCT FROM 'number'
           OR (effect ->> 'claim_token') !~ '^[1-9][0-9]*$'
           OR (effect ->> 'claim_token')::BIGINT > reservation.claim_token
           OR (effect ->> 'issued_at')::TIMESTAMPTZ > now() THEN
            RETURN FALSE;
        END IF;
        IF resource_uid IS NOT NULL THEN
            IF (effect ->> 'observed_uid') IS DISTINCT FROM resource_uid
               OR jsonb_typeof(effect -> 'observed_at') IS DISTINCT FROM 'string'
               OR (effect ->> 'observed_at')::TIMESTAMPTZ > now()
               OR (effect ->> 'issued_at')::TIMESTAMPTZ >
                  (effect ->> 'observed_at')::TIMESTAMPTZ THEN
                RETURN FALSE;
            END IF;
        ELSIF effect ->> 'observed_uid' IS NOT NULL
           OR jsonb_typeof(effect -> 'ambiguity_until') IS DISTINCT FROM 'string'
           OR (effect ->> 'ambiguity_until')::TIMESTAMPTZ > now() THEN
            -- The existing quiescence rule: no unobserved accepted request
            -- may still land. Observing a UID after its deadline is valid.
            RETURN FALSE;
        END IF;
    END LOOP;
    RETURN TRUE;
EXCEPTION WHEN invalid_text_representation OR invalid_datetime_format
    OR datetime_field_overflow OR numeric_value_out_of_range THEN
    RETURN FALSE;
END;
$$;

-- Retain all intervening process-zero/IDE/terminal replay rules. Apply only
-- the three exact retirement exceptions to the currently installed guard.
DO $migration$
DECLARE
    definition TEXT;
    fragment TEXT;
BEGIN
    definition := pg_get_functiondef(
        'public.prevent_retired_workspace_runtime_rebinding()'::regprocedure
    );
    fragment := '    creation_authorized BOOLEAN;';
    IF strpos(definition, fragment) = 0 THEN
        RAISE EXCEPTION 'Unexpected workspace retirement guard declaration';
    END IF;
    definition := replace(definition, fragment, fragment || E'\n'
        || '    cancelled_creation_retirement_authorized BOOLEAN;');
    fragment := '        uidless_creation_authorized := new_runtime IS NULL AND';
    IF strpos(definition, fragment) = 0 THEN
        RAISE EXCEPTION 'Unexpected workspace retirement guard evaluation';
    END IF;
    definition := replace(definition, fragment,
        $new$        cancelled_creation_retirement_authorized :=
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
$new$ || fragment);
    fragment := E'           AND NOT creation_authorized\n';
    IF (length(definition) - length(replace(definition, fragment, '')))
        / length(fragment) <> 2 THEN
        RAISE EXCEPTION 'Unexpected workspace retirement envelope guards';
    END IF;
    definition := replace(definition, fragment,
        fragment || E'           AND NOT cancelled_creation_retirement_authorized\n');
    fragment := '            IF NOT creation_authorized THEN';
    IF strpos(definition, fragment) = 0 THEN
        RAISE EXCEPTION 'Unexpected workspace retirement UID guard';
    END IF;
    definition := replace(definition, fragment,
        $new$            IF NOT creation_authorized
               AND NOT cancelled_creation_retirement_authorized THEN$new$);
    EXECUTE definition;
END;
$migration$;

-- A nonempty UID-less auxiliary object also lacks a provisioner stamp. The
-- process-zero guard must accept this same exact retirement, without treating
-- the old object as an empty placeholder or permitting destructive teardown.
DO $migration$
DECLARE
    definition TEXT;
    fragment TEXT := $old$    ELSIF old_workspace <> '{}'::JSONB
       -- A status-only placeholder$old$;
BEGIN
    definition := pg_get_functiondef(
        'public.enforce_managed_repository_process_zero_transition()'::regprocedure
    );
    IF strpos(definition, fragment) = 0 THEN
        RAISE EXCEPTION 'Unexpected workspace provisioner authority guard';
    END IF;
    EXECUTE replace(definition, fragment,
        $new$    ELSIF old_workspace <> '{}'::JSONB
       AND NOT (
           TG_OP = 'UPDATE' AND NEW.id = OLD.id
           AND source_kind = 'job'
           AND NEW.status::TEXT = 'cancelled' AND OLD.status::TEXT = 'cancelled'
           AND to_jsonb(NEW) ->> 'execution_lane' = 'stateless'
           AND to_jsonb(OLD) ->> 'execution_lane' = 'stateless'
           AND to_jsonb(NEW) ->> 'assigned_agent_id' IS NULL
           AND to_jsonb(OLD) ->> 'assigned_agent_id' IS NULL
           AND public.managed_repo_cancelled_creation_retirement_is_authorized_now(
               source_kind, source_id, 'workspace_container', old_state, new_state
           )
       )
       -- A status-only placeholder$new$);
END;
$migration$;
COMMIT;
