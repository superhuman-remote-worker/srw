-- migration: 0288_vm_initial_creation_cleanup_lineage.sql
-- description: Permit initial VM cleanup through exact not-started attach-abort lineage.
-- depends-on: 0287_validate_pinned_failed_start_retention.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

-- This is terminal source authority, never permission to issue a VM effect.
-- Callers hold the owner row before the immutable source row. Outcomes are
-- append-only and their insert authority is the atomic attach-abort transition.
CREATE FUNCTION public.thread_vm_creation_cleanup_lineage(
    owner_row public.threads, retry public.vm_creation_retries,
    require_initial boolean DEFAULT false
) RETURNS text LANGUAGE plpgsql STABLE AS $$
DECLARE
    cursor_generation uuid := retry.thread_runtime_generation;
    visited uuid[] := ARRAY[]::uuid[];
    edge public.thread_runtime_attach_abort_outcomes%ROWTYPE;
    edge_count integer;
    hops integer := 0;
    vm jsonb;
    workspace jsonb;
BEGIN
    IF retry.owner_kind IS DISTINCT FROM 'thread'
       OR retry.thread_id IS DISTINCT FROM owner_row.id
       OR retry.job_id IS NOT NULL
       OR owner_row.execution_lane IS DISTINCT FROM 'pinned'
       OR retry.thread_runtime_generation IS NULL THEN
        RETURN NULL;
    END IF;
    IF NOT require_initial
       AND retry.thread_runtime_generation = owner_row.runtime_generation
       AND retry.thread_agent_id IS NOT DISTINCT FROM owner_row.agent_id
       AND retry.thread_attach_token IS NOT DISTINCT FROM owner_row.runtime_attach_token THEN
        RETURN 'exact';
    END IF;

    vm := COALESCE(owner_row.metadata->'vm',
                   owner_row.runtime_retirement_context->'vm_creation_source'->'captured_vm');
    workspace := owner_row.metadata->'workspace_container';
    IF owner_row.runtime_retirement_external_cleanup IS NOT NULL
       AND owner_row.runtime_retirement_external_cleanup=
           public.pinned_retirement_external_cleanup_expected(
               owner_row.runtime_retirement_context,owner_row.runtime_generation,
               owner_row.runtime_retirement_token)
       AND public.pinned_retirement_external_resources_absent(owner_row.id,owner_row.metadata) THEN
        -- Permanent endpoint clearing publishes a full deleted-workspace
        -- tombstone even for an initially absent workspace. Use the original
        -- captured shape only through that exact cleanup receipt.
        workspace := owner_row.runtime_retirement_context->'workspace_container';
    END IF;
    IF owner_row.status IS DISTINCT FROM 'created'
       OR retry.origin IS DISTINCT FROM 'initial'
       OR retry.expected_pvc_uid IS NOT NULL
       OR retry.thread_wake_operation_id IS NOT NULL
       OR retry.observed_vm_uid IS NOT NULL
       OR COALESCE(retry.canonical_request->'workspace_storage','null'::jsonb) <> 'null'::jsonb
       OR jsonb_typeof(vm) IS DISTINCT FROM 'object'
       OR vm->>'creation_request_id' IS DISTINCT FROM retry.request_id::text
       OR vm->>'provision_generation' IS DISTINCT FROM retry.provision_generation::text
       OR vm->>'vm_uid' IS NOT NULL
       OR vm->>'rootdisk' = 'kept'
       OR vm->>'idle_wake_operation_id' IS NOT NULL
       OR vm->>'idle_predecessor_pvc_uid' IS NOT NULL
       OR COALESCE(owner_row.metadata->'_workspace_binding','null'::jsonb) <> 'null'::jsonb
       OR (workspace IS NOT NULL AND workspace <> 'null'::jsonb
           AND (jsonb_typeof(workspace) IS DISTINCT FROM 'object'
                OR workspace - 'repo_name' - 'git_remote_url' <> '{}'::jsonb))
       OR EXISTS (SELECT 1 FROM public.vm_creation_retries r
           WHERE r.thread_id=retry.thread_id AND r.request_id<>retry.request_id)
       OR EXISTS (SELECT 1 FROM public.vm_creation_effects e
           WHERE e.request_id=retry.request_id AND e.effect_kind='vm' AND e.state='observed')
       OR EXISTS (SELECT 1 FROM public.vm_idle_operations i
           WHERE i.owner_kind='thread' AND i.owner_id=retry.thread_id)
       OR EXISTS (SELECT 1 FROM public.thread_runtime_retirement_outcomes o
           WHERE o.thread_id=retry.thread_id AND o.outcome='settled'
             -- Soft End appends this exact outcome before its guarded row
             -- transition. It is completion of this End, not prior history.
             AND ROW(o.runtime_generation,o.retirement_token) IS DISTINCT FROM
                 ROW(owner_row.runtime_generation,owner_row.runtime_retirement_token))
       OR EXISTS (SELECT 1 FROM public.thread_workspace_provision_intents p
           WHERE p.thread_id=retry.thread_id)
       OR EXISTS (SELECT 1 FROM public.vm_workspace_recoveries r
           WHERE r.owner_kind='thread' AND r.owner_id=retry.thread_id)
       OR EXISTS (SELECT 1 FROM public.srw_execution_specs e
           JOIN public.srw_execution_workspace_bindings b ON b.execution_id=e.id
           WHERE e.work_kind='Session' AND e.work_id=retry.thread_id) THEN
        RETURN NULL;
    END IF;
    -- Exact-source cleanup remains compatible with retained wake. The new
    -- no-VM final actor protocol must independently pass all fresh guards.
    IF retry.thread_runtime_generation = owner_row.runtime_generation
       AND retry.thread_agent_id IS NOT DISTINCT FROM owner_row.agent_id
       AND retry.thread_attach_token IS NOT DISTINCT FROM owner_row.runtime_attach_token THEN
        RETURN 'exact';
    END IF;
    -- NULL/NULL is the immutable legacy prebind source, admitted while both
    -- owner and inverse agent were unbound. It cannot authorize creation now.
    IF cursor_generation = owner_row.runtime_generation THEN
        IF retry.thread_agent_id IS NULL AND retry.thread_attach_token IS NULL THEN
            RETURN 'initial_attach_abort_v1';
        END IF;
        RETURN NULL;
    END IF;
    LOOP
        -- Explicit bound also covers damaged history; no recursive unbounded
        -- scan, cycle, or arbitrarily chosen branch can authorize cleanup.
        IF hops >= 4096 OR cursor_generation = ANY(visited) THEN
            RETURN NULL;
        END IF;
        visited := array_append(visited,cursor_generation);
        SELECT count(*) INTO edge_count FROM (
            SELECT 1 FROM public.thread_runtime_attach_abort_outcomes o
             WHERE o.thread_id=retry.thread_id AND o.runtime_generation=cursor_generation
             LIMIT 2
        ) candidates;
        IF edge_count <> 1 THEN
            RETURN NULL;
        END IF;
        SELECT o.* INTO edge FROM public.thread_runtime_attach_abort_outcomes o
         WHERE o.thread_id=retry.thread_id AND o.runtime_generation=cursor_generation;
        IF edge.release_kind IS DISTINCT FROM 'process_zero'
           OR edge.quiescence_protocol IS DISTINCT FROM 'agent_attach_not_started_v1'
           OR edge.workspace_generation IS NOT NULL
           OR edge.workspace_runtime_incarnation IS NOT NULL
           OR edge.agent_id IS NULL OR edge.runtime_attach_token IS NULL
           OR NULLIF(btrim(edge.agent_pod_uid),'') IS NULL
           OR (hops=0 AND retry.thread_agent_id IS NOT NULL
               AND (edge.agent_id IS DISTINCT FROM retry.thread_agent_id
                    OR edge.runtime_attach_token IS DISTINCT FROM retry.thread_attach_token)) THEN
            RETURN NULL;
        END IF;
        cursor_generation := edge.successor_generation;
        hops := hops+1;
        IF cursor_generation = owner_row.runtime_generation THEN
            RETURN 'initial_attach_abort_v1';
        END IF;
    END LOOP;
END;
$$;

-- Current End owns cleanup; the captured source and all carriers retain the
-- original actor/G. Re-evaluate append-only lineage instead of trusting JSON.
CREATE FUNCTION public.valid_thread_vm_creation_retirement_source(
    retry public.vm_creation_retries, require_captured boolean
) RETURNS boolean LANGUAGE sql STABLE AS $$
    SELECT EXISTS (SELECT 1 FROM public.threads t
        WHERE t.id=retry.thread_id AND t.execution_lane='pinned'
          AND t.runtime_retirement_token IS NOT NULL
          AND t.runtime_retirement_authorized_at IS NOT NULL
          AND t.runtime_retirement_context->>'settle_status'='ended'
          AND t.runtime_retirement_context->>'generation'=t.runtime_generation::text
          AND t.runtime_retirement_context->>'agent_id' IS NOT DISTINCT FROM t.agent_id::text
          AND t.runtime_retirement_context->>'runtime_attach_token' IS NOT DISTINCT FROM t.runtime_attach_token::text
          AND public.thread_vm_creation_cleanup_lineage(t,retry)=COALESCE(
              t.runtime_retirement_context->'vm_creation_source'->>'cleanup_protocol','exact')
          AND t.runtime_retirement_context->'vm_creation_source'->>'request_id'=retry.request_id::text
          AND t.runtime_retirement_context->'vm_creation_source'->>'provision_generation'=retry.provision_generation::text
          AND t.runtime_retirement_context->'vm_creation_source'->>'thread_runtime_generation'=retry.thread_runtime_generation::text
          AND t.runtime_retirement_context->'vm_creation_source'->>'thread_agent_id' IS NOT DISTINCT FROM retry.thread_agent_id::text
          AND t.runtime_retirement_context->'vm_creation_source'->>'thread_attach_token' IS NOT DISTINCT FROM retry.thread_attach_token::text
          AND t.runtime_retirement_context->'vm_creation_source'->>'request_digest'=retry.request_digest
          AND t.runtime_retirement_context->'vm_creation_source'->>'controller_configuration_digest'=retry.controller_configuration_digest
          AND t.runtime_retirement_context->'vm_creation_source'->'captured_vm'->>'creation_request_id'=retry.request_id::text
          AND t.runtime_retirement_context->'vm_creation_source'->'captured_vm'->>'provision_generation'=retry.provision_generation::text
          AND COALESCE(t.runtime_retirement_context->'vm_creation_source'->'captured_vm'->'vm_uid','null'::jsonb)='null'::jsonb
          AND (COALESCE(t.runtime_retirement_context->'vm_creation_source'->'captured_vm'->'rootdisk_pvc_uid','null'::jsonb)='null'::jsonb
               OR t.runtime_retirement_context->'vm_creation_source'->'captured_vm'->>'rootdisk_pvc_uid'=retry.observed_pvc_uid::text)
          AND (t.metadata->'vm'=t.runtime_retirement_context->'vm_creation_source'->'captured_vm'
               OR (NOT require_captured AND retry.state='settled' AND NOT t.metadata ? 'vm')));
$$;

CREATE OR REPLACE FUNCTION public.valid_vm_creation_thread_disposition_identity(
    retry public.vm_creation_retries, require_captured boolean
) RETURNS boolean LANGUAGE sql STABLE AS $$
    SELECT (retry.owner_kind='thread' AND retry.job_id IS NULL
       AND retry.thread_id IS NOT NULL
       AND retry.canonical_request->>'entity_type'='thread'
       AND retry.canonical_request->>'job_id'=retry.thread_id::text
       AND retry.canonical_request->>'provision_generation'=retry.provision_generation::text
       AND COALESCE(retry.canonical_request->'workspace_storage','null'::jsonb)='null'::jsonb
       AND retry.cancellation_disposition->>'owner_kind'='thread'
       AND retry.cancellation_disposition->>'thread_id'=retry.thread_id::text
       AND retry.cancellation_disposition->>'job_id'=retry.thread_id::text
       AND retry.cancellation_disposition->>'thread_runtime_generation'=retry.thread_runtime_generation::text
       AND retry.cancellation_disposition->>'thread_agent_id' IS NOT DISTINCT FROM retry.thread_agent_id::text
       AND retry.cancellation_disposition->>'thread_attach_token' IS NOT DISTINCT FROM retry.thread_attach_token::text
       AND retry.cancellation_disposition->>'thread_wake_operation_id' IS NOT DISTINCT FROM retry.thread_wake_operation_id::text
       AND ((retry.disposition_carrier_uid IS NULL
             AND retry.creation_carrier_uid IS NOT NULL
             AND retry.cancellation_disposition->>'carrier_uid'=retry.creation_carrier_uid::text
             AND retry.cancellation_disposition->>'namespace'=retry.creation_carrier_namespace)
            OR (retry.disposition_carrier_uid IS NOT NULL
             AND retry.creation_carrier_uid IS NULL
             AND retry.cancellation_disposition->>'carrier_kind'='thread_creation_cancel'
             AND retry.cancellation_disposition->>'carrier_uid'=retry.disposition_carrier_uid::text
             AND retry.cancellation_disposition->>'namespace'=retry.disposition_carrier_namespace))
       AND ((retry.cancellation_disposition->>'disk_policy'='purge_new_thread_disk'
             AND retry.expected_pvc_uid IS NULL)
            OR (retry.cancellation_disposition->>'disk_policy'='retain'
             AND retry.creation_carrier_uid IS NOT NULL
             AND retry.disposition_carrier_uid IS NULL
             AND retry.expected_pvc_uid IS NOT NULL
             AND retry.observed_pvc_uid=retry.expected_pvc_uid
             AND retry.thread_wake_operation_id IS NOT NULL
             AND retry.cancellation_disposition->'objects'->'rootdisk'->>'pvc_uid'=retry.expected_pvc_uid::text
             AND retry.cancellation_disposition->'source'=jsonb_build_object(
                 'kind','retained','pvc_uid',retry.expected_pvc_uid::text)
             AND EXISTS (SELECT 1 FROM public.vm_creation_effects root
                 WHERE root.request_id=retry.request_id
                   AND root.effect_kind='rootdisk' AND root.state='observed'
                   AND root.evidence->>'pvc_uid'=retry.expected_pvc_uid::text
                   AND root.evidence->>'uid'=retry.cancellation_disposition->'objects'->'rootdisk'->>'uid'
                   AND root.carrier_intent->>'thread_wake_operation_id'=retry.thread_wake_operation_id::text
                   AND root.carrier_intent->'rootdisk_source'=retry.cancellation_disposition->'source')
             AND NOT EXISTS (SELECT 1 FROM public.vm_creation_effects e
                 WHERE e.request_id=retry.request_id AND e.effect_kind='workspace_attach'
                   AND e.state<>'rejected')
             AND EXISTS (SELECT 1 FROM public.vm_idle_operations idle
                 WHERE idle.id=retry.thread_wake_operation_id
                   AND idle.owner_kind='thread' AND idle.owner_id=retry.thread_id
                   AND idle.release_kind='pinned_thread'
                   AND idle.wake_request_id=retry.request_id
                   AND idle.wake_generation=retry.provision_generation
                   AND idle.pvc_uid=retry.expected_pvc_uid
                   AND idle.stop_verified_at IS NOT NULL
                   AND idle.stop_evidence->>'retained_pvc'='true'
                   AND idle.stop_evidence->>'pvc_uid'=retry.expected_pvc_uid::text
                   AND idle.phase IN ('waking','wake_held')
                   AND idle.closed_at IS NULL)))
       AND retry.cancellation_disposition->'workspace_storage'='null'::jsonb
       AND retry.cancellation_disposition->'workspace_instance_id'='null'::jsonb
       AND public.valid_thread_vm_creation_retirement_source(retry,require_captured)
       AND EXISTS (SELECT 1 FROM public.threads t WHERE t.id=retry.thread_id
           AND retry.cancellation_disposition->>'retirement_token'=t.runtime_retirement_token::text)) IS TRUE;
$$;

CREATE OR REPLACE FUNCTION public.thread_vm_creation_never_issued_source(
    requested_thread uuid, requested_generation text
)
RETURNS boolean LANGUAGE sql STABLE AS $$
    SELECT EXISTS (
        SELECT 1 FROM public.threads t
        JOIN public.vm_creation_retries r
          ON r.request_id = (t.runtime_retirement_context
                             ->'vm_creation_source'->>'request_id')::uuid
        LEFT JOIN public.vm_workspace_cleanup_admissions a
          ON a.id = r.creation_admission_id
        WHERE t.id = requested_thread
          AND t.execution_lane = 'pinned'
          AND t.runtime_retirement_token IS NOT NULL
          AND t.runtime_retirement_authorized_at IS NOT NULL
          AND t.runtime_retirement_context->'vm_creation_source'->>'request_id'
              ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          AND public.valid_thread_vm_creation_retirement_source(r,false)
          AND t.runtime_retirement_context->'vm_creation_source'
              ->>'provision_generation' = requested_generation
          AND r.owner_kind = 'thread' AND r.thread_id = t.id
          AND r.job_id IS NULL
          AND r.provision_generation::text = requested_generation
          AND r.request_digest = t.runtime_retirement_context
              ->'vm_creation_source'->>'request_digest'
          AND r.controller_configuration_digest = t.runtime_retirement_context
              ->'vm_creation_source'->>'controller_configuration_digest'
          AND r.state = 'settled' AND (
              (r.reason = 'creation_never_issued'
               AND r.observed_vm_uid IS NULL AND r.observed_pvc_uid IS NULL
               AND (a.id IS NULL OR (a.completed_at IS NOT NULL
                     AND a.outcome = 'never_issued' AND a.owner_kind = 'thread'
                     AND a.owner_id = t.id))
               AND NOT EXISTS (SELECT 1 FROM public.vm_creation_effects e
                   WHERE e.request_id = r.request_id
                     AND e.state IN ('issued','observed')))
              OR (r.reason = 'creation_disposed'
                  AND r.observed_vm_uid IS NULL
                  AND a.completed_at IS NOT NULL
                  AND a.outcome = 'creation_disposed'
                  AND a.owner_kind = 'thread' AND a.owner_id = t.id
                  AND public.valid_vm_creation_disposition_evidence(r))
          )
          AND NOT EXISTS (
              SELECT 1 FROM public.vm_resource_reservations v
               WHERE v.request_id = r.request_id AND v.state <> 'released'
          )
    );
$$;
CREATE FUNCTION public.pinned_vm_creation_agent_zero_source(
    requested_thread uuid, requested_runtime uuid, requested_token uuid
) RETURNS boolean LANGUAGE sql STABLE AS $$
    SELECT EXISTS (SELECT 1 FROM public.threads t
        JOIN public.vm_creation_retries r ON r.thread_id=t.id
          AND r.request_id::text=t.runtime_retirement_context->'vm_creation_source'->>'request_id'
        WHERE t.id=requested_thread AND t.runtime_generation=requested_runtime
          AND t.runtime_retirement_token=requested_token
          AND r.origin='initial' AND r.expected_pvc_uid IS NULL
          AND r.thread_wake_operation_id IS NULL
          AND public.thread_vm_creation_cleanup_lineage(t,r,true) IS NOT NULL
          AND t.runtime_retirement_context->>'workspace_backend'='vm'
          AND t.runtime_retirement_context->'vm'='null'::jsonb
          AND COALESCE(t.runtime_retirement_context->'workspace_binding','null'::jsonb) IN ('null'::jsonb,'{}'::jsonb)
          AND (COALESCE(t.runtime_retirement_context->'workspace_container','null'::jsonb)='null'::jsonb
               OR (jsonb_typeof(t.runtime_retirement_context->'workspace_container')='object'
                   AND (t.runtime_retirement_context->'workspace_container')-'repo_name'-'git_remote_url'='{}'::jsonb))
          AND public.thread_vm_creation_never_issued_source(t.id,
              t.runtime_retirement_context->'vm_creation_source'->>'provision_generation'));
$$;

-- Keep the existing current actor/G/token/Pod receipt checks. Only protocol
-- selection changes, after the independent positive source settlement proof.
DO $migration$
DECLARE
    definition text;
    old_fragment text := $old$                    WHEN NEW.runtime_retirement_context->>'workspace_backend'
                         IN ('vm', 'remote')
                    THEN NEW.runtime_retirement_local_quiescence->>'quiescence_protocol'$old$;
    new_fragment text := $new$                    WHEN public.pinned_vm_creation_agent_zero_source(
                        NEW.id,NEW.runtime_generation,NEW.runtime_retirement_token)
                    THEN NEW.runtime_retirement_local_quiescence->>'quiescence_protocol'='agent_runtime_zero_v1'
                         AND NEW.runtime_retirement_local_quiescence->>'workspace_generation' IS NULL
                         AND NEW.runtime_retirement_local_quiescence->>'workspace_runtime_incarnation' IS NULL
                         AND NEW.runtime_retirement_local_quiescence->>'vm_creation_request_id'=
                             NEW.runtime_retirement_context->'vm_creation_source'->>'request_id'
                         AND NEW.runtime_retirement_local_quiescence->>'vm_creation_provision_generation'=
                             NEW.runtime_retirement_context->'vm_creation_source'->>'provision_generation'
                    WHEN NEW.runtime_retirement_context->>'workspace_backend'
                         IN ('vm', 'remote')
                    THEN NEW.runtime_retirement_local_quiescence->>'quiescence_protocol'$new$;
BEGIN
    definition := pg_get_functiondef('public.enforce_thread_ended_transition()'::regprocedure);
    IF (length(definition)-length(replace(definition,old_fragment,'')))/length(old_fragment) <> 1 THEN
        RAISE EXCEPTION '0288 requires one exact VM receipt publication predicate';
    END IF;
    EXECUTE replace(definition,old_fragment,new_fragment);
END;
$migration$;

DO $migration$
DECLARE
    definition text;
    old_fragment text := $old$        ELSIF workspace_backend IN ('vm', 'remote') THEN$old$;
    new_fragment text := $new$        ELSIF public.pinned_vm_creation_agent_zero_source(
            OLD.id,OLD.runtime_generation,OLD.runtime_retirement_token) THEN
            expected_protocol := 'agent_runtime_zero_v1';
            expected_workspace_generation := '';
            expected_workspace_runtime := '';
        ELSIF workspace_backend IN ('vm', 'remote') THEN$new$;
BEGIN
    definition := pg_get_functiondef('public.enforce_pinned_thread_delete_authority()'::regprocedure);
    IF (length(definition)-length(replace(definition,old_fragment,'')))/length(old_fragment) <> 1 THEN
        RAISE EXCEPTION '0288 requires one exact VM permanent-zero predicate';
    END IF;
    EXECUTE replace(definition,old_fragment,new_fragment);
END;
$migration$;

CREATE FUNCTION public.enforce_vm_creation_source_terminal_settlement()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.runtime_retirement_context->'vm_creation_source' NOT IN ('null'::jsonb,'{}'::jsonb)
       AND OLD.runtime_retirement_authorized_at IS NOT NULL
       AND (TG_OP='DELETE' OR (NEW.runtime_retirement_token IS NULL AND NEW.status IN ('ended','suspended')))
       AND (NOT public.thread_vm_creation_never_issued_source(OLD.id,
                OLD.runtime_retirement_context->'vm_creation_source'->>'provision_generation')
            OR ((OLD.runtime_retirement_local_quiescence->>'vm_creation_request_id' IS NOT NULL
                 OR OLD.runtime_retirement_context->'vm_creation_source'->>'cleanup_protocol'='initial_attach_abort_v1')
                AND (NOT public.pinned_vm_creation_agent_zero_source(
                        OLD.id,OLD.runtime_generation,OLD.runtime_retirement_token)
                    OR (OLD.runtime_authority_exposed AND (
                        OLD.runtime_retirement_local_quiescence->>'vm_creation_request_id' IS DISTINCT FROM
                            OLD.runtime_retirement_context->'vm_creation_source'->>'request_id'
                        OR OLD.runtime_retirement_local_quiescence->>'vm_creation_provision_generation' IS DISTINCT FROM
                            OLD.runtime_retirement_context->'vm_creation_source'->>'provision_generation'))))) THEN
        RAISE EXCEPTION 'initial VM End lacks settled source and independent current actor zero'
            USING ERRCODE='23514', CONSTRAINT='thread_vm_creation_source_terminal_settlement';
    END IF;
    IF TG_OP='DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER thread_vm_creation_source_terminal_settlement
BEFORE UPDATE OF status,runtime_retirement_token OR DELETE ON public.threads
FOR EACH ROW EXECUTE FUNCTION public.enforce_vm_creation_source_terminal_settlement();

COMMIT;
