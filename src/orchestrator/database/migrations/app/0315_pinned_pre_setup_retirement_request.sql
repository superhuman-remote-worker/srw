-- A monotonic pre-setup assertion nominates cleanup; it never proves physical zero.
CREATE FUNCTION public.pinned_pre_setup_retirement_request_valid(
    owner_row public.threads, marker jsonb, require_actor boolean DEFAULT false
) RETURNS boolean LANGUAGE plpgsql STABLE AS $$
DECLARE
    context jsonb := owner_row.runtime_retirement_context;
    pod jsonb := context->'agent_pod';
    actor public.agents%ROWTYPE;
    expected jsonb;
    source jsonb := context->'vm_creation_source';
    physical jsonb := context->'vm';
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
       OR owner_row.status NOT IN ('created','active')
       OR owner_row.runtime_authority_exposed IS DISTINCT FROM true
       OR COALESCE(context->>'workspace_backend','') NOT IN ('vm','remote')
       OR pod->>'protection_protocol' IS DISTINCT FROM 'finalizer_v1'
       OR NULLIF(pod->>'namespace','') IS NULL OR NULLIF(pod->>'pod_name','') IS NULL
       OR NULLIF(pod->>'pod_uid','') IS NULL
       OR context->'agent'->>'hostname' IS DISTINCT FROM pod->>'pod_name'
       OR context->'agent'->>'pod_uid' IS DISTINCT FROM pod->>'pod_uid'
       OR owner_row.metadata->'agent_pod' IS DISTINCT FROM pod THEN
        RETURN false;
    END IF;
    -- Physical identity is captured by Begin, never supplied or fabricated by
    -- a booting actor. An uncreated VM requires its immutable creation source.
    IF COALESCE(physical,'null'::jsonb) = 'null'::jsonb THEN
        IF context->>'workspace_backend' IS DISTINCT FROM 'vm'
           OR jsonb_typeof(source) IS DISTINCT FROM 'object'
           OR source->'captured_vm'->>'vm_uid' IS NOT NULL
           OR owner_row.metadata->'vm' IS DISTINCT FROM source->'captured_vm'
           OR NOT EXISTS (
               SELECT 1 FROM public.vm_creation_retries c
                WHERE c.request_id::text=source->>'request_id'
                  AND c.owner_kind='thread' AND c.thread_id=owner_row.id
                  AND c.provision_generation::text=source->>'provision_generation'
                  AND c.thread_runtime_generation::text=source->>'thread_runtime_generation'
                  AND c.request_digest=source->>'request_digest'
           ) THEN RETURN false; END IF;
    ELSIF jsonb_typeof(physical) IS DISTINCT FROM 'object'
       OR physical->'identity_authenticated' IS DISTINCT FROM 'true'::jsonb
       OR physical->>'identity_provision_generation' IS DISTINCT FROM physical->>'provision_generation'
       OR NULLIF(physical->>'vm_uid','') IS NULL
       OR (owner_row.runtime_retirement_permanent AND NULLIF(physical->>'rootdisk_pvc_uid','') IS NULL)
       OR public.pinned_retirement_external_cleanup_expected(context,owner_row.runtime_generation,
            owner_row.runtime_retirement_token)->>'workspace_cleanup_protocol'
            IS DISTINCT FROM 'workspace_actuator_zero_v1' THEN
        RETURN false;
    END IF;
    expected := jsonb_build_object(
        'kind','agent_pre_setup_retirement_v1','thread_id',owner_row.id::text,
        'agent_id',owner_row.agent_id::text,'pod_uid',pod->>'pod_uid',
        'runtime_generation',owner_row.runtime_generation::text,
        'runtime_attach_token',owner_row.runtime_attach_token::text,
        'retirement_token',owner_row.runtime_retirement_token::text,
        'disposition','ended','permanent',owner_row.runtime_retirement_permanent
    );
    IF marker IS DISTINCT FROM expected THEN RETURN false; END IF;
    IF NOT EXISTS (
        SELECT 1 FROM public.thread_agent_pod_provision_intents i
         WHERE i.attempt_id::text=pod->>'provision_attempt' AND i.thread_id=owner_row.id
           AND i.runtime_generation=owner_row.runtime_generation AND i.status='published'
           AND i.provisioner='persistent' AND i.namespace=pod->>'namespace'
           AND i.pod_name=pod->>'pod_name' AND i.pod_uid=pod->>'pod_uid'
           AND i.protection_protocol='finalizer_v1'
    ) THEN RETURN false; END IF;
    SELECT * INTO actor FROM public.agents WHERE id=owner_row.agent_id;
    IF actor.id IS NULL THEN
        IF require_actor THEN RETURN false; END IF;
    ELSIF actor.thread_id IS DISTINCT FROM owner_row.id
       OR actor.pod_uid IS DISTINCT FROM pod->>'pod_uid'
       OR actor.hostname IS DISTINCT FROM pod->>'pod_name'
       OR actor.agent_mode IS DISTINCT FROM 'persistent'
       OR actor.current_job_id IS NOT NULL THEN RETURN false;
    END IF;
    -- Supported producers lock the owner and refuse T. Process input generation
    -- is distinct from G, so work is checked by the captured actor/Pod tuple.
    IF EXISTS (SELECT 1 FROM public.thread_input_deliveries
                WHERE thread_id=owner_row.id AND owner_agent_id=owner_row.agent_id
                  AND owner_pod_uid=pod->>'pod_uid'
                  AND state IN ('owned','queued','admitted','settled'))
       OR EXISTS (SELECT 1 FROM public.thread_control_requests
                   WHERE thread_id=owner_row.id AND runtime_generation=owner_row.runtime_generation)
       OR EXISTS (SELECT 1 FROM public.thread_permission_requests
                   WHERE thread_id=owner_row.id AND status='pending')
       OR EXISTS (SELECT 1 FROM public.threads WHERE parent_thread_id=owner_row.id) THEN
        RETURN false;
    END IF;
    RETURN true;
END;
$$;

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
