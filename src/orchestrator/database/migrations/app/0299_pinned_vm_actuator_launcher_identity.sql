-- Match the native VM workspace attestation without rewriting durable history.
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
       OR COALESCE(context->'workspace_binding','null'::jsonb) NOT IN ('null'::jsonb,'{}'::jsonb)
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
