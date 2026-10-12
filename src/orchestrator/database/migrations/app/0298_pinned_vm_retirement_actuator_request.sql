-- Local drain is permission to run the existing exact stop actuator, never zero.
ALTER TABLE public.threads ADD COLUMN IF NOT EXISTS runtime_retirement_actuator_request jsonb;
ALTER TABLE public.thread_runtime_retirement_outcomes ADD COLUMN IF NOT EXISTS actuator_request jsonb;

CREATE FUNCTION public.pinned_vm_actuator_request_valid(
    owner_row public.threads, marker jsonb, require_actor boolean DEFAULT false
) RETURNS boolean LANGUAGE plpgsql STABLE AS $$
DECLARE
    context jsonb := owner_row.runtime_retirement_context;
    vm jsonb := context->'vm';
    pod jsonb := context->'agent_pod';
    actor public.agents%ROWTYPE;
    expected jsonb;
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
    expected := jsonb_build_object(
        'kind','vm_local_drain_complete_v1', 'thread_id',owner_row.id::text,
        'agent_id',owner_row.agent_id::text, 'pod_uid',pod->>'pod_uid',
        'process_generation',marker->>'process_generation',
        'runtime_generation',owner_row.runtime_generation::text,
        'runtime_attach_token',owner_row.runtime_attach_token::text,
        'retirement_token',owner_row.runtime_retirement_token::text,
        'disposition','ended', 'permanent',owner_row.runtime_retirement_permanent,
        'workspace_generation',vm->>'provision_generation',
        'workspace_runtime_incarnation',vm->>'vm_uid'
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

CREATE FUNCTION public.guard_pinned_vm_actuator_request() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='INSERT' THEN
        IF NEW.runtime_retirement_actuator_request IS NOT NULL THEN
            RAISE EXCEPTION 'VM actuator request requires existing retirement' USING ERRCODE='23514';
        END IF;
        RETURN NEW;
    END IF;
    IF OLD.runtime_retirement_actuator_request IS NOT NULL THEN
        IF NEW.runtime_retirement_token IS DISTINCT FROM OLD.runtime_retirement_token THEN
            IF NEW.runtime_retirement_token IS NOT NULL OR NEW.status IS DISTINCT FROM 'ended'
               OR NEW.agent_id IS NOT NULL OR NEW.runtime_attach_token IS NOT NULL
               OR NOT EXISTS (
                   SELECT 1 FROM public.thread_runtime_retirement_outcomes o
                    WHERE o.thread_id=OLD.id AND o.runtime_generation=OLD.runtime_generation
                      AND o.retirement_token=OLD.runtime_retirement_token
                      AND o.actuator_request=OLD.runtime_retirement_actuator_request
               ) THEN
                RAISE EXCEPTION 'VM actuator request may clear only at exact settlement' USING ERRCODE='23514';
            END IF;
            NEW.runtime_retirement_actuator_request := NULL;
        ELSIF NEW.runtime_retirement_actuator_request IS DISTINCT FROM OLD.runtime_retirement_actuator_request THEN
            RAISE EXCEPTION 'VM actuator request is immutable while pending' USING ERRCODE='23514';
        END IF;
    ELSIF NEW.runtime_retirement_actuator_request IS NOT NULL THEN
        IF NEW.runtime_retirement_token IS DISTINCT FROM OLD.runtime_retirement_token
           OR NOT public.pinned_vm_actuator_request_valid(NEW,NEW.runtime_retirement_actuator_request,true) THEN
            RAISE EXCEPTION 'VM actuator request lacks exact current authority' USING ERRCODE='23514';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER threads_vm_actuator_request_guard BEFORE INSERT OR UPDATE ON public.threads
FOR EACH ROW EXECUTE FUNCTION public.guard_pinned_vm_actuator_request();

CREATE FUNCTION public.capture_pinned_vm_actuator_request() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE owner_row public.threads%ROWTYPE;
BEGIN
    IF NEW.actuator_request IS NOT NULL THEN
        RAISE EXCEPTION 'archived VM actuator request is server-owned' USING ERRCODE='23514';
    END IF;
    -- Existing insert-authority guard proves physical zero and exact settlement.
    SELECT * INTO owner_row FROM public.threads WHERE id=NEW.thread_id FOR SHARE;
    IF owner_row.runtime_generation=NEW.runtime_generation
       AND owner_row.runtime_retirement_token=NEW.retirement_token THEN
        NEW.actuator_request := owner_row.runtime_retirement_actuator_request;
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER thread_runtime_retirement_outcomes_z_capture_vm_actuator
BEFORE INSERT ON public.thread_runtime_retirement_outcomes
FOR EACH ROW EXECUTE FUNCTION public.capture_pinned_vm_actuator_request();

COMMENT ON COLUMN public.threads.runtime_retirement_actuator_request IS
'Exact authenticated local VM drain handoff; permits existing actuator only, never process zero or End completion.';
COMMENT ON COLUMN public.thread_runtime_retirement_outcomes.actuator_request IS
'Server-captured full handoff identity for exact lost-response reconciliation; historical rows stay NULL.';
