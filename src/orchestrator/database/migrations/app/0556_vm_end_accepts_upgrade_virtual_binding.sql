-- migration:     0556_vm_end_accepts_upgrade_virtual_binding.sql
-- description:   End a VM Session that kept the binding of its old virtual
--                object-store files from an upgrade (E3).
-- depends-on:    0555_validate_thread_permission_mode_read_only.sql
-- transactional: yes

BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

-- Until the VM install dropped it, an upgrade from the virtual tier left the
-- Session's virtual binding next to its VM, and End captured both. The
-- captured context can't change, so End accepts exactly that binding, and
-- only for a VM: an object-store prefix with no process, Pod or disk behind
-- it. Python's canonical_virtual_binding (shared/pinned_workspace_evidence.py)
-- is the same test.
CREATE FUNCTION public.pinned_canonical_virtual_binding(binding jsonb)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT CASE
        WHEN jsonb_typeof(binding) IS DISTINCT FROM 'object' THEN false
        ELSE COALESCE(
            binding ?& ARRAY['generation', 'kind', 'backing_id', 'ssh_host_key_fingerprint']
            AND binding - ARRAY['generation', 'kind', 'backing_id', 'ssh_host_key_fingerprint']
                = '{}'::jsonb
            AND binding->'kind' = '"virtual"'::jsonb
            AND jsonb_typeof(binding->'backing_id') = 'string'
            AND binding->>'backing_id' ~ '^rclone:[0-9a-f]{64}$'
            AND jsonb_typeof(binding->'generation') = 'string'
            AND binding->>'generation'
                ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            AND binding->'ssh_host_key_fingerprint' = 'null'::jsonb,
            false
        )
    END
$$;

-- 0330's body; only the VM branch's binding test changes.
CREATE OR REPLACE FUNCTION public.pinned_retirement_external_cleanup_expected(
    retirement_context jsonb,
    runtime_generation uuid,
    retirement_token uuid
)
RETURNS jsonb
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    workspace jsonb;
    binding jsonb;
    agent_claim jsonb;
    workspace_intent jsonb;
    vm jsonb;
    protected_ro jsonb;
    backend text;
    workspace_evidence boolean := false;
    binding_evidence boolean := false;
    vm_evidence boolean := false;
    ro_live boolean := false;
    workspace_protocol text;
BEGIN
    IF jsonb_typeof(retirement_context) IS DISTINCT FROM 'object'
       OR COALESCE(jsonb_typeof(retirement_context->'workspace_container'), 'null')
          NOT IN ('object', 'null')
       OR COALESCE(jsonb_typeof(retirement_context->'workspace_binding'), 'null')
          NOT IN ('object', 'null')
       OR COALESCE(jsonb_typeof(retirement_context->'agent_workspace_claim'), 'null')
          NOT IN ('object', 'null')
       OR COALESCE(
            jsonb_typeof(retirement_context->'workspace_provision_intent'),
            'null'
          ) NOT IN ('object', 'null')
       OR COALESCE(jsonb_typeof(retirement_context->'vm'), 'null')
          NOT IN ('object', 'null')
       OR COALESCE(jsonb_typeof(retirement_context->'protected_ro'), 'null')
          NOT IN ('object', 'null') THEN
        RETURN NULL;
    END IF;

    workspace := CASE
        WHEN jsonb_typeof(retirement_context->'workspace_container') = 'object'
        THEN retirement_context->'workspace_container'
        ELSE '{}'::jsonb
    END;
    binding := CASE
        WHEN jsonb_typeof(retirement_context->'workspace_binding') = 'object'
        THEN retirement_context->'workspace_binding'
        ELSE '{}'::jsonb
    END;
    agent_claim := CASE
        WHEN jsonb_typeof(retirement_context->'agent_workspace_claim') = 'object'
        THEN retirement_context->'agent_workspace_claim'
        ELSE '{}'::jsonb
    END;
    workspace_intent := CASE
        WHEN jsonb_typeof(retirement_context->'workspace_provision_intent') = 'object'
        THEN retirement_context->'workspace_provision_intent'
        ELSE '{}'::jsonb
    END;
    vm := CASE
        WHEN jsonb_typeof(retirement_context->'vm') = 'object'
        THEN retirement_context->'vm'
        ELSE '{}'::jsonb
    END;
    protected_ro := CASE
        WHEN jsonb_typeof(retirement_context->'protected_ro') = 'object'
        THEN retirement_context->'protected_ro'
        ELSE '{}'::jsonb
    END;
    backend := COALESCE(retirement_context->>'workspace_backend', '');
    workspace_evidence := (
        COALESCE(workspace->>'status', '') NOT IN ('', 'deleted')
        OR workspace->>'_runtime_incarnation' IS NOT NULL
        OR workspace->>'_docker_workspace_lease_id' IS NOT NULL
        OR workspace->>'pod_ip' IS NOT NULL
        OR workspace->>'pod_name' IS NOT NULL
        OR workspace->>'host' IS NOT NULL
        OR workspace->>'port' IS NOT NULL
        OR workspace->>'ide_host' IS NOT NULL
        OR workspace->>'ide_port' IS NOT NULL
        OR workspace->>'_canvas_workspace_generation' IS NOT NULL
    );
    binding_evidence := binding <> '{}'::jsonb;
    vm_evidence := (
        COALESCE(vm->>'status', '') NOT IN ('', 'deleted')
        OR vm->>'provision_generation' IS NOT NULL
        OR vm->>'identity_provision_generation' IS NOT NULL
        OR vm->>'vm_uid' IS NOT NULL
        OR vm->>'_runtime_incarnation' IS NOT NULL
        OR vm->>'rootdisk_pvc_uid' IS NOT NULL
        OR vm->>'ssh_host' IS NOT NULL
        OR vm->>'ssh_port' IS NOT NULL
        OR vm->>'_canvas_workspace_generation' IS NOT NULL
    );
    ro_live := COALESCE(protected_ro->>'status', '') IN (
        'engaging', 'active', 'revoking'
    );

    IF agent_claim <> '{}'::jsonb AND (
        COALESCE(agent_claim->>'claim_id', '') !~
            '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        OR COALESCE(agent_claim->>'thread_id', '') !~
            '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        OR agent_claim->>'thread_id' IS DISTINCT FROM retirement_context->>'thread_id'
        OR COALESCE(agent_claim->>'created_runtime_generation', '') !~
            '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        OR COALESCE(agent_claim->>'create_attempt', '') !~
            '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        OR COALESCE(agent_claim->>'provisioner', '') NOT IN ('agent', 'persistent')
        OR NULLIF(agent_claim->>'pvc_name', '') IS NULL
        OR COALESCE(agent_claim->>'status', '') NOT IN ('planned', 'ready')
        OR (
            (agent_claim->>'status' = 'ready')
            IS DISTINCT FROM (NULLIF(agent_claim->>'pvc_uid', '') IS NOT NULL)
        )
    ) THEN
        RETURN NULL;
    END IF;

    IF workspace_intent <> '{}'::jsonb AND (
        COALESCE(workspace_intent->>'attempt_id', '') !~
            '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        OR workspace_intent->>'thread_id'
            IS DISTINCT FROM retirement_context->>'thread_id'
        OR NOT public.pinned_workspace_provision_capture_shape_is_current(
            (retirement_context->>'thread_id')::uuid,runtime_generation,workspace_intent)
        OR NULLIF(workspace_intent->>'namespace', '') IS NULL
        OR NULLIF(workspace_intent->>'pod_name', '') IS NULL
        OR NULLIF(workspace_intent->>'network_tier', '') IS NULL
        OR COALESCE(workspace_intent->>'manifest_fingerprint', '')
            !~ '^[0-9a-f]{64}$'
        OR COALESCE(workspace_intent->>'status', '')
            NOT IN ('planned', 'fenced', 'retired')
        OR jsonb_typeof(workspace_intent->'previous_binding')
            IS DISTINCT FROM 'object'
        OR workspace_intent->'previous_binding'
            IS DISTINCT FROM binding
        OR (
            NULLIF(workspace_intent->>'created_agent_id', '') IS NULL
        ) IS DISTINCT FROM (
            NULLIF(workspace_intent->>'created_attach_token', '') IS NULL
        )
        OR (
            NULLIF(workspace_intent->>'pvc_name', '') IS NULL
        ) IS DISTINCT FROM (
            NULLIF(workspace_intent->>'service_name', '') IS NULL
        )
    ) THEN
        RETURN NULL;
    END IF;

    IF workspace_intent <> '{}'::jsonb THEN
        IF vm_evidence OR backend NOT IN ('sandbox', 'virtual', 'none') THEN
            RETURN NULL;
        END IF;
        workspace_protocol := 'workspace_provision_fence_v1';
    ELSIF backend = 'sandbox' THEN
        IF vm_evidence OR (binding_evidence AND binding->>'kind' = 'virtual') THEN
            RETURN NULL;
        END IF;
        workspace_protocol := CASE
            WHEN workspace_evidence OR binding_evidence
            THEN 'sandbox_actuator_zero_v1'
            ELSE 'external_none_v1'
        END;
    ELSIF backend IN ('vm', 'remote') THEN
        -- A Session upgraded from the virtual tier keeps the binding of its
        -- old object-store files. Nothing runs behind it; the VM teardown is
        -- still the whole cleanup.
        IF workspace_evidence OR (binding_evidence AND NOT (
            backend = 'vm' AND public.pinned_canonical_virtual_binding(binding)
        )) THEN
            RETURN NULL;
        END IF;
        workspace_protocol := CASE
            WHEN vm_evidence THEN 'workspace_actuator_zero_v1'
            ELSE 'external_none_v1'
        END;
    ELSIF backend = 'virtual' THEN
        IF workspace_evidence OR vm_evidence
           OR (binding_evidence AND (
               binding->>'kind' IS DISTINCT FROM 'virtual'
               OR COALESCE(binding->>'backing_id', '')
                    !~ '^rclone:[0-9a-f]{64}$'
           )) THEN
            RETURN NULL;
        END IF;
        workspace_protocol := CASE
            WHEN binding_evidence THEN 'virtual_backing_zero_v1'
            ELSE 'external_none_v1'
        END;
    ELSIF backend = 'none' THEN
        IF workspace_evidence OR binding_evidence OR vm_evidence THEN
            RETURN NULL;
        END IF;
        workspace_protocol := 'external_none_v1';
    ELSE
        RETURN NULL;
    END IF;

    IF ro_live AND (
        NULLIF(protected_ro->>'id', '') IS NULL
        OR NULLIF(protected_ro->>'runtime_generation', '') IS NULL
        OR NULLIF(protected_ro->>'engage_attempt', '') IS NULL
        OR NULLIF(protected_ro->>'grant_handle', '') IS NULL
    ) THEN
        RETURN NULL;
    END IF;

    RETURN jsonb_build_object(
        'version', 1,
        'runtime_generation', runtime_generation::text,
        'retirement_token', retirement_token::text,
        'cleanup_actor', 'orchestrator',
        'workspace_cleanup_protocol', workspace_protocol,
        'agent_workspace_cleanup_protocol', CASE
            WHEN agent_claim <> '{}'::jsonb
            THEN 'k8s_pvc_name_tombstone_v1' ELSE NULL
        END,
        'protected_reader_cleanup_protocol', CASE
            WHEN ro_live THEN 'protected_reader_zero_v1' ELSE NULL
        END,
        'captured_resources', jsonb_build_object(
            'workspace_backend', backend,
            'workspace_container', retirement_context->'workspace_container',
            'workspace_binding', retirement_context->'workspace_binding',
            'agent_workspace_claim', retirement_context->'agent_workspace_claim',
            'workspace_provision_intent',
                retirement_context->'workspace_provision_intent',
            'vm', retirement_context->'vm',
            'protected_ro', retirement_context->'protected_ro'
        )
    );
END;
$$;

-- 0315's body; only the captured binding test changes.
CREATE OR REPLACE FUNCTION public.pinned_vm_actuator_request_valid(
    owner_row public.threads, marker jsonb, require_actor boolean DEFAULT false
) RETURNS boolean LANGUAGE plpgsql STABLE AS $$
DECLARE
    context jsonb := owner_row.runtime_retirement_context;
    vm jsonb := context->'vm';
    pod jsonb := context->'agent_pod';
    actor public.agents%ROWTYPE;
    expected jsonb;
    workspace_incarnation text := vm->>'active_pod_uid';
    legacy_durable_marker boolean := NOT require_actor
        AND marker IS NOT NULL
        AND marker = owner_row.runtime_retirement_actuator_request
        AND marker->>'workspace_runtime_incarnation' = vm->>'vm_uid';
BEGIN
    IF marker->>'kind' = 'agent_pre_setup_retirement_v1' THEN
        RETURN public.pinned_pre_setup_retirement_request_valid(owner_row,marker,require_actor);
    END IF;
    IF owner_row.execution_lane IS DISTINCT FROM 'pinned'
       OR owner_row.runtime_retirement_token IS NULL
       OR owner_row.runtime_retirement_authorized_at IS NULL
       OR owner_row.agent_id IS NULL OR owner_row.runtime_attach_token IS NULL
       OR context->>'thread_id' IS DISTINCT FROM owner_row.id::text
       OR context->>'generation' IS DISTINCT FROM owner_row.runtime_generation::text
       OR context->>'agent_id' IS DISTINCT FROM owner_row.agent_id::text
       OR context->>'runtime_attach_token' IS DISTINCT FROM owner_row.runtime_attach_token::text
       OR context->>'settle_status' IS DISTINCT FROM 'ended'
       OR COALESCE(context->>'workspace_backend','') NOT IN ('vm','remote')
       OR jsonb_typeof(vm) IS DISTINCT FROM 'object'
       OR vm->'identity_authenticated' IS DISTINCT FROM 'true'::jsonb
       OR vm->>'identity_provision_generation' IS DISTINCT FROM vm->>'provision_generation'
       OR COALESCE(vm->>'provision_generation','') !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       OR NULLIF(vm->>'vm_uid','') IS NULL
       OR (vm->>'_runtime_incarnation' IS NOT NULL AND vm->>'_runtime_incarnation' IS DISTINCT FROM vm->>'vm_uid')
       OR (owner_row.runtime_retirement_permanent AND NULLIF(vm->>'rootdisk_pvc_uid','') IS NULL)
       OR (COALESCE(context->'workspace_binding','null'::jsonb) NOT IN ('null'::jsonb,'{}'::jsonb)
           AND NOT (context->>'workspace_backend' = 'vm'
                    AND public.pinned_canonical_virtual_binding(context->'workspace_binding')))
       OR COALESCE(context->'workspace_provision_intent','null'::jsonb) NOT IN ('null'::jsonb,'{}'::jsonb)
       OR public.pinned_retirement_external_cleanup_expected(context, owner_row.runtime_generation,
            owner_row.runtime_retirement_token)->>'workspace_cleanup_protocol' IS DISTINCT FROM 'workspace_actuator_zero_v1'
       OR pod->>'protection_protocol' IS DISTINCT FROM 'finalizer_v1'
       OR NULLIF(pod->>'namespace','') IS NULL OR NULLIF(pod->>'pod_name','') IS NULL
       OR NULLIF(pod->>'pod_uid','') IS NULL
       OR context->'agent'->>'hostname' IS DISTINCT FROM pod->>'pod_name'
       OR context->'agent'->>'pod_uid' IS DISTINCT FROM pod->>'pod_uid'
       OR NULLIF(btrim(marker->>'process_generation'),'') IS NULL THEN
        RETURN false;
    END IF;
    -- Delivery attests the launcher Pod UID, independently of the VM UID
    -- used by the unchanged teardown/process-zero protocol. New requests must
    -- carry that exact captured launcher; no alternate VM/VMI identity admits.
    IF legacy_durable_marker THEN
        -- 0298 could admit a VM-UID marker without a launcher field. Preserve
        -- only continuation of that immutable, already stored full tuple.
        workspace_incarnation := vm->>'vm_uid';
    ELSIF COALESCE(workspace_incarnation,'') !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
        RETURN false;
    END IF;
    expected := jsonb_build_object(
        'kind','vm_local_drain_complete_v1', 'thread_id',owner_row.id::text,
        'agent_id',owner_row.agent_id::text, 'pod_uid',pod->>'pod_uid',
        'process_generation',marker->>'process_generation',
        'runtime_generation',owner_row.runtime_generation::text,
        'runtime_attach_token',owner_row.runtime_attach_token::text,
        'retirement_token',owner_row.runtime_retirement_token::text,
        'disposition','ended', 'permanent',owner_row.runtime_retirement_permanent,
        'workspace_generation',vm->>'provision_generation',
        'workspace_runtime_incarnation',workspace_incarnation
    );
    IF marker IS DISTINCT FROM expected THEN RETURN false; END IF;
    SELECT * INTO actor FROM public.agents WHERE id=owner_row.agent_id;
    -- A vanished actor is allowed only for continuation of a durable marker.
    IF actor.id IS NULL THEN RETURN NOT require_actor; END IF;
    RETURN actor.thread_id IS NOT DISTINCT FROM owner_row.id
       AND actor.pod_uid IS NOT DISTINCT FROM pod->>'pod_uid'
       AND actor.hostname IS NOT DISTINCT FROM pod->>'pod_name'
       AND actor.metadata->>'dispatch_process_generation' IS NOT DISTINCT FROM marker->>'process_generation';
END;
$$;

COMMIT;
