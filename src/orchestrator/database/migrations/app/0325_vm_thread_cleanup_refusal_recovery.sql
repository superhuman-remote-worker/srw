-- Exact completed-teardown recovery; original refusals are never reopened.
-- The narrow protocol covers adopted non-quota pinned permanent retirements.
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE public.vm_thread_cleanup_refusal_recoveries (
    refused_admission_id uuid PRIMARY KEY REFERENCES public.vm_workspace_cleanup_admissions(id),
    successor_admission_id uuid NOT NULL UNIQUE REFERENCES public.vm_workspace_cleanup_admissions(id),
    child_admission_id uuid NOT NULL REFERENCES public.vm_workspace_cleanup_admissions(id),
    creation_request_id uuid NOT NULL REFERENCES public.vm_creation_retries(request_id),
    thread_id uuid NOT NULL REFERENCES public.vm_thread_creation_owners(thread_id),
    runtime_generation uuid NOT NULL,
    retirement_token uuid NOT NULL,
    provision_generation uuid NOT NULL,
    vm_uid uuid NOT NULL,
    pvc_uid uuid NOT NULL,
    physical_stop jsonb NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (refused_admission_id<>successor_admission_id AND refused_admission_id<>child_admission_id
        AND successor_admission_id<>child_admission_id)
);

CREATE FUNCTION public.validate_vm_thread_cleanup_refusal_recovery(r public.vm_thread_cleanup_refusal_recoveries)
RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE
    owner_row public.threads%ROWTYPE;
    source public.vm_creation_retries%ROWTYPE;
    refused public.vm_workspace_cleanup_admissions%ROWTYPE;
    successor public.vm_workspace_cleanup_admissions%ROWTYPE;
    child public.vm_workspace_cleanup_admissions%ROWTYPE;
    vm jsonb;
    current_vm jsonb;
    expected_digest text;
BEGIN
    SELECT * INTO owner_row FROM public.threads WHERE id=r.thread_id FOR UPDATE;
    SELECT * INTO source FROM public.vm_creation_retries WHERE request_id=r.creation_request_id FOR UPDATE;
    SELECT * INTO refused FROM public.vm_workspace_cleanup_admissions WHERE id=r.refused_admission_id FOR UPDATE;
    SELECT * INTO successor FROM public.vm_workspace_cleanup_admissions WHERE id=r.successor_admission_id FOR UPDATE;
    SELECT * INTO child FROM public.vm_workspace_cleanup_admissions WHERE id=r.child_admission_id FOR UPDATE;
    vm := owner_row.runtime_retirement_context->'vm';
    current_vm := owner_row.metadata->'vm';
    expected_digest := 'sha256:' || encode(sha256(convert_to(format(
        '{"owner_id":"%s","owner_kind":"thread","provision_generation":"%s","purge_disk":true,"pvc_uid":"%s","resource":"vm_workspace","source":"pinned_thread_retirement","vm_uid":"%s"}',
        r.thread_id,r.provision_generation,r.pvc_uid,r.vm_uid),'UTF8')),'hex');
    IF owner_row.id IS NULL OR owner_row.execution_lane IS DISTINCT FROM 'pinned'
       OR owner_row.runtime_generation IS DISTINCT FROM r.runtime_generation
       OR owner_row.runtime_retirement_token IS DISTINCT FROM r.retirement_token
       OR owner_row.runtime_retirement_authorized_at IS NULL
       OR owner_row.runtime_retirement_permanent IS DISTINCT FROM true
       OR owner_row.runtime_retirement_context->>'thread_id' IS DISTINCT FROM r.thread_id::text
       OR owner_row.runtime_retirement_context->>'generation' IS DISTINCT FROM r.runtime_generation::text
       OR owner_row.runtime_retirement_context->>'settle_status' IS DISTINCT FROM 'ended'
       OR owner_row.runtime_retirement_context->>'workspace_backend' IS DISTINCT FROM 'vm'
       OR vm->'identity_authenticated' IS DISTINCT FROM 'true'::jsonb
       OR vm->>'identity_provision_generation' IS DISTINCT FROM r.provision_generation::text
       OR vm->>'provision_generation' IS DISTINCT FROM r.provision_generation::text
       OR vm->>'vm_uid' IS DISTINCT FROM r.vm_uid::text
       OR vm->>'rootdisk_pvc_uid' IS DISTINCT FROM r.pvc_uid::text
       OR current_vm->>'provision_generation' IS DISTINCT FROM r.provision_generation::text
       OR current_vm->>'vm_uid' IS DISTINCT FROM r.vm_uid::text
       OR current_vm->>'rootdisk_pvc_uid' IS DISTINCT FROM r.pvc_uid::text
       OR vm->>'creation_request_id' IS DISTINCT FROM r.creation_request_id::text
       OR current_vm->>'creation_request_id' IS DISTINCT FROM r.creation_request_id::text
       OR COALESCE(current_vm->>'status','') NOT IN ('retiring_process_zero','deleting','deleted')
       OR source.owner_kind IS DISTINCT FROM 'thread' OR source.thread_id IS DISTINCT FROM r.thread_id
       OR source.thread_runtime_generation IS DISTINCT FROM r.runtime_generation
       OR source.thread_agent_id IS DISTINCT FROM owner_row.agent_id
       OR source.thread_attach_token IS DISTINCT FROM owner_row.runtime_attach_token
       OR source.provision_generation IS DISTINCT FROM r.provision_generation
       OR source.observed_vm_uid::text IS DISTINCT FROM r.vm_uid::text
       OR source.observed_pvc_uid::text IS DISTINCT FROM r.pvc_uid::text
       OR source.state IS DISTINCT FROM 'succeeded' OR source.reason IS DISTINCT FROM 'creation_adopted'
       OR source.canonical_request->>'entity_type' IS DISTINCT FROM 'thread'
       OR source.canonical_request->>'job_id' IS DISTINCT FROM r.thread_id::text
       OR source.canonical_request->>'provision_generation' IS DISTINCT FROM r.provision_generation::text
       OR source.controller_configuration->>'version' IS DISTINCT FROM '1'
       OR COALESCE(source.controller_configuration->'resource_admission','null'::jsonb)<>'null'::jsonb
       OR NOT EXISTS (SELECT 1 FROM public.vm_workspace_cleanup_admissions c
           WHERE c.id=source.creation_admission_id AND c.owner_kind='thread' AND c.owner_id=r.thread_id
             AND c.source='controller_vm_create' AND c.completed_at IS NOT NULL AND c.outcome='adopted')
       OR EXISTS (SELECT 1 FROM public.vm_resource_reservations c WHERE c.request_id=source.request_id)
       OR EXISTS (SELECT 1 FROM public.vm_resource_waiters c WHERE c.request_id=source.request_id)
       OR EXISTS (SELECT 1 FROM public.vm_creation_effects e WHERE e.request_id=source.request_id AND e.state='issued')
       OR NOT EXISTS (SELECT 1 FROM public.vm_thread_creation_owners a
           WHERE a.thread_id=r.thread_id AND a.live_thread_id=r.thread_id AND a.deleted_at IS NULL)
       OR refused.owner_kind IS DISTINCT FROM 'thread' OR refused.owner_id IS DISTINCT FROM r.thread_id
       OR refused.pvc_uid IS DISTINCT FROM r.pvc_uid OR refused.source IS DISTINCT FROM 'pinned_thread_retirement'
       OR refused.parent_admission_id IS NOT NULL OR refused.intent_digest IS DISTINCT FROM expected_digest
       OR refused.completed_at IS NULL OR refused.outcome IS DISTINCT FROM 'identity_superseded'
       OR successor.owner_kind IS DISTINCT FROM refused.owner_kind OR successor.owner_id IS DISTINCT FROM refused.owner_id
       OR successor.pvc_uid IS DISTINCT FROM refused.pvc_uid OR successor.source IS DISTINCT FROM refused.source
       OR successor.intent_digest IS DISTINCT FROM refused.intent_digest OR successor.parent_admission_id IS NOT NULL
       OR successor.request_id IS NOT DISTINCT FROM refused.request_id
       OR (successor.completed_at IS NOT NULL AND successor.outcome IS DISTINCT FROM 'completed')
       OR child.owner_kind IS DISTINCT FROM refused.owner_kind OR child.owner_id IS DISTINCT FROM refused.owner_id
       OR child.pvc_uid IS DISTINCT FROM refused.pvc_uid OR child.parent_admission_id IS DISTINCT FROM refused.id
       OR child.source IS DISTINCT FROM 'controller_rootdisk_delete' OR child.completed_at IS NULL
       OR child.outcome IS DISTINCT FROM 'deleted'
       OR EXISTS (SELECT 1 FROM public.vm_workspace_cleanup_admissions c
           WHERE c.completed_at IS NULL AND c.id<>r.successor_admission_id
             AND (c.owner_kind='thread' AND c.owner_id=r.thread_id OR c.pvc_uid=r.pvc_uid))
       OR EXISTS (SELECT 1 FROM public.vm_workspace_recoveries recovery
           LEFT JOIN public.vm_workspace_recovery_retention_pins pin
             ON pin.recovery_id=recovery.id AND pin.released_at IS NULL
           WHERE recovery.resolved_at IS NULL
             AND (recovery.owner_kind='thread' AND recovery.owner_id=r.thread_id OR pin.pvc_uid=r.pvc_uid))
       OR EXISTS (SELECT 1 FROM public.vm_idle_access_leases a
           WHERE a.owner_kind='thread' AND a.owner_id=r.thread_id AND a.closed_at IS NULL
             AND a.expires_at>clock_timestamp())
       OR NOT EXISTS (SELECT 1 FROM public.managed_repository_process_zero_receipts p
           WHERE p.owner_kind='thread' AND p.owner_id=r.thread_id AND p.scope='vm' AND p.provisioner='vm'
             AND p.runtime_incarnation=r.provision_generation::text)
       OR r.physical_stop->'version' IS DISTINCT FROM '1'::jsonb
       OR r.physical_stop->>'kind' IS DISTINCT FROM 'vm_cleanup_physical_stop'
       OR r.physical_stop->>'owner_kind' IS DISTINCT FROM 'thread'
       OR r.physical_stop->>'owner_id' IS DISTINCT FROM r.thread_id::text
       OR r.physical_stop->>'provision_generation' IS DISTINCT FROM r.provision_generation::text
       OR r.physical_stop->>'vm_uid' IS DISTINCT FROM r.vm_uid::text
       OR r.physical_stop->>'pvc_uid' IS DISTINCT FROM r.pvc_uid::text
       OR r.physical_stop->>'vmi_uid' IS DISTINCT FROM vm->>'vmi_uid'
       OR r.physical_stop->>'launcher_uid' IS DISTINCT FROM vm->>'active_pod_uid'
       OR r.physical_stop->'vm_absent' IS DISTINCT FROM 'true'::jsonb
       OR r.physical_stop->'vmi_absent' IS DISTINCT FROM 'true'::jsonb
       OR r.physical_stop->'launcher_absent' IS DISTINCT FROM 'true'::jsonb
       OR r.physical_stop->'same_generation_replacement' IS DISTINCT FROM 'false'::jsonb
       OR r.physical_stop->'controller_authenticated' IS DISTINCT FROM 'true'::jsonb
       OR r.physical_stop->>'pvc_disposition' IS DISTINCT FROM 'purged' THEN
        RAISE EXCEPTION 'VM cleanup refusal recovery authority unproven' USING ERRCODE='23514';
    END IF;
    RETURN true;
END;
$$;

CREATE FUNCTION public.guard_vm_thread_cleanup_refusal_recovery() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP<>'INSERT' THEN
        RAISE EXCEPTION 'VM cleanup refusal recovery evidence is immutable' USING ERRCODE='23514';
    END IF;
    PERFORM public.validate_vm_thread_cleanup_refusal_recovery(NEW);
    RETURN NEW;
END;
$$;
CREATE TRIGGER guard_vm_thread_cleanup_refusal_recovery
BEFORE INSERT OR UPDATE OR DELETE ON public.vm_thread_cleanup_refusal_recoveries
FOR EACH ROW EXECUTE FUNCTION public.guard_vm_thread_cleanup_refusal_recovery();

CREATE FUNCTION public.guard_vm_thread_recovered_cleanup_admission() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE r public.vm_thread_cleanup_refusal_recoveries%ROWTYPE;
BEGIN
    SELECT * INTO r FROM public.vm_thread_cleanup_refusal_recoveries
        WHERE OLD.id IN (refused_admission_id,successor_admission_id,child_admission_id);
    IF FOUND AND (TG_OP='DELETE' OR NEW IS DISTINCT FROM OLD) THEN
        IF TG_OP='UPDATE' AND OLD.id=r.successor_admission_id AND OLD.completed_at IS NULL
           AND NEW.completed_at IS NOT NULL AND NEW.outcome='completed'
           AND (to_jsonb(NEW)-'completed_at'-'outcome')=(to_jsonb(OLD)-'completed_at'-'outcome') THEN
            PERFORM public.validate_vm_thread_cleanup_refusal_recovery(r);
            RETURN NEW;
        END IF;
        RAISE EXCEPTION 'Recovered VM cleanup admissions are immutable' USING ERRCODE='23514';
    END IF;
    IF TG_OP='DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER guard_vm_thread_recovered_cleanup_admission
BEFORE UPDATE OR DELETE ON public.vm_workspace_cleanup_admissions
FOR EACH ROW EXECUTE FUNCTION public.guard_vm_thread_recovered_cleanup_admission();
COMMIT;
