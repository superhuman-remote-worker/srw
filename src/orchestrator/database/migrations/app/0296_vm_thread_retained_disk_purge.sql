-- migration: 0296_vm_thread_retained_disk_purge.sql
-- description: Separate permanent disk disposition after a settled soft VM End.
-- depends-on: 0295_vm_thread_cleanup_resource_release.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE public.vm_thread_retained_disk_purge_authorities (
    cleanup_admission_id uuid PRIMARY KEY REFERENCES public.vm_workspace_cleanup_admissions(id),
    compute_cleanup_admission_id uuid NOT NULL UNIQUE REFERENCES public.vm_resource_thread_cleanup_authorities(cleanup_admission_id),
    runtime_generation uuid NOT NULL,
    retirement_token uuid NOT NULL,
    source_revision bigint NOT NULL,
    cleanup_request_id uuid NOT NULL,
    intent_digest text NOT NULL,
    retirement_context jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE public.vm_thread_retained_disk_purge_receipts (
    cleanup_admission_id uuid PRIMARY KEY REFERENCES public.vm_thread_retained_disk_purge_authorities(cleanup_admission_id),
    purge_evidence jsonb NOT NULL,
    accepted_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- Immutable predecessor actors describe the old settled outcome. The new
-- permanent Begin has no actor. Never reinterpret its token as old authority.
-- No advisory lock here: deletion already holds the owner row. Service callers
-- take the owner/PVC advisory prefix before entering this common row-lock order.
CREATE FUNCTION public.validate_vm_thread_retained_disk_purge(
    d public.vm_thread_retained_disk_purge_authorities, after_endpoint_cleanup boolean DEFAULT false)
RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE
    a public.vm_resource_thread_cleanup_authorities%ROWTYPE;
    s public.vm_resource_thread_cleanup_stops%ROWTYPE;
    owner_row public.threads%ROWTYPE;
    source public.vm_creation_retries%ROWTYPE;
    charge public.vm_resource_reservations%ROWTYPE;
    old_cleanup public.vm_workspace_cleanup_admissions%ROWTYPE;
    cleanup public.vm_workspace_cleanup_admissions%ROWTYPE;
    soft public.thread_runtime_retirement_outcomes%ROWTYPE;
    vm jsonb;
    captured jsonb;
    expected_stop jsonb;
    old_digest text;
    new_digest text;
BEGIN
    SELECT * INTO a FROM public.vm_resource_thread_cleanup_authorities
        WHERE cleanup_admission_id=d.compute_cleanup_admission_id;
    SELECT * INTO owner_row FROM public.threads WHERE id=a.thread_id FOR UPDATE;
    SELECT * INTO source FROM public.vm_creation_retries WHERE request_id=a.request_id FOR UPDATE;
    PERFORM id FROM public.vm_workspace_cleanup_admissions
        WHERE id IN (a.cleanup_admission_id,d.cleanup_admission_id) ORDER BY id FOR UPDATE;
    SELECT * INTO old_cleanup FROM public.vm_workspace_cleanup_admissions WHERE id=a.cleanup_admission_id;
    SELECT * INTO cleanup FROM public.vm_workspace_cleanup_admissions WHERE id=d.cleanup_admission_id;
    SELECT * INTO charge FROM public.vm_resource_reservations WHERE id=a.reservation_id FOR UPDATE;
    SELECT * INTO s FROM public.vm_resource_thread_cleanup_stops WHERE cleanup_admission_id=a.cleanup_admission_id;
    SELECT * INTO soft FROM public.thread_runtime_retirement_outcomes
        WHERE thread_id=a.thread_id AND runtime_generation=a.runtime_generation AND retirement_token=a.retirement_token;
    expected_stop := jsonb_build_object(
        'version',1,'kind','vm_cleanup_physical_stop','owner_kind','thread','owner_id',a.thread_id,
        'provision_generation',a.provision_generation,'vm_uid',a.vm_uid,'vmi_uid',a.vmi_uid,
        'launcher_uid',a.launcher_uid,'pvc_uid',a.pvc_uid,
        'vm_absent',true,'vmi_absent',true,'launcher_absent',true,
        'same_generation_replacement',false,'controller_authenticated',true,'pvc_disposition','retained');
    -- These keys/values are fixed ASCII UUIDs and literals, sorted exactly as
    -- cleanup_intent_digest's compact JSON. jsonb::text is NOT its encoding.
    old_digest := 'sha256:' || encode(sha256(convert_to(format(
        '{"owner_id":"%s","owner_kind":"thread","provision_generation":"%s","purge_disk":false,"pvc_uid":"%s","resource":"vm_workspace","source":"pinned_thread_retirement","vm_uid":"%s"}',
        a.thread_id,a.provision_generation,a.pvc_uid,a.vm_uid),'UTF8')),'hex');
    new_digest := 'sha256:' || encode(sha256(convert_to(format(
        '{"compute_cleanup_admission_id":"%s","owner_id":"%s","owner_kind":"thread","provision_generation":"%s","purge_disk":true,"pvc_uid":"%s","resource":"vm_workspace","retirement_token":"%s","runtime_generation":"%s","source":"pinned_thread_retained_disk_purge","vm_uid":"%s"}',
        a.cleanup_admission_id,a.thread_id,a.provision_generation,a.pvc_uid,d.retirement_token,d.runtime_generation,a.vm_uid),'UTF8')),'hex');
    IF a.cleanup_admission_id IS NULL OR a.purge_disk IS DISTINCT FROM false
       OR s.cleanup_admission_id IS NULL OR s.stop_evidence IS DISTINCT FROM expected_stop
       OR old_cleanup.id IS NULL OR old_cleanup.owner_kind IS DISTINCT FROM 'thread'
       OR old_cleanup.owner_id IS DISTINCT FROM a.thread_id OR old_cleanup.pvc_uid IS DISTINCT FROM a.pvc_uid
       OR old_cleanup.parent_admission_id IS NOT NULL
       OR old_cleanup.source IS DISTINCT FROM 'pinned_thread_retirement'
       OR old_cleanup.request_id IS DISTINCT FROM a.cleanup_request_id
       OR old_cleanup.intent_digest IS DISTINCT FROM old_digest OR a.intent_digest IS DISTINCT FROM old_digest
       OR old_cleanup.completed_at IS NULL OR old_cleanup.outcome IS DISTINCT FROM 'completed'
       OR soft.thread_id IS NULL OR soft.permanent IS DISTINCT FROM false
       OR soft.outcome IS DISTINCT FROM 'settled' OR soft.disposition IS DISTINCT FROM 'ended'
       OR soft.agent_id IS DISTINCT FROM a.agent_id OR soft.runtime_attach_token IS DISTINCT FROM a.attach_token
       OR old_cleanup.completed_at>soft.settled_at
       OR source.resolved_at IS NULL OR source.resolved_at>old_cleanup.admitted_at
       OR NOT EXISTS (SELECT 1 FROM public.managed_repository_process_zero_receipts p
           WHERE p.id=s.process_zero_receipt_id AND p.owner_kind='thread' AND p.owner_id=a.thread_id
             AND p.scope='vm' AND p.provisioner='vm' AND p.runtime_incarnation=a.provision_generation::text
             AND p.observed_at<=soft.settled_at)
       OR charge.id IS NULL OR charge.state IS DISTINCT FROM 'released'
       OR charge.resource_version IS DISTINCT FROM 2 OR charge.request_id IS DISTINCT FROM a.request_id
       OR charge.revision IS DISTINCT FROM a.reservation_revision
       OR charge.released_at IS NULL OR charge.released_at<s.accepted_at
       OR charge.release_evidence IS DISTINCT FROM jsonb_build_object(
           'kind','exact_cleanup_compute_absent','owner_kind','thread','thread_id',a.thread_id,
           'cleanup_admission_id',a.cleanup_admission_id,'reservation_revision',a.reservation_revision,
           'stop_evidence_digest','sha256:'||encode(sha256(convert_to(s.stop_evidence::text,'UTF8')),'hex'))
       OR charge.vmi_uid IS DISTINCT FROM a.vmi_uid OR charge.launcher_uid IS DISTINCT FROM a.launcher_uid
       OR (charge.vm_uid IS NOT NULL AND charge.vm_uid IS DISTINCT FROM a.vm_uid)
       OR (a.vmi_uid IS NULL AND (source.ready_at IS NOT NULL OR charge.vm_uid IS NOT NULL OR a.launcher_uid IS NOT NULL))
       OR (a.vmi_uid IS NOT NULL AND (source.ready_at IS NULL OR charge.vm_uid IS DISTINCT FROM a.vm_uid OR a.launcher_uid IS NULL))
       OR EXISTS (SELECT 1 FROM public.vm_resource_reservations r WHERE r.request_id=a.request_id AND r.revision>a.reservation_revision)
       OR EXISTS (SELECT 1 FROM public.vm_resource_recovery_successors r WHERE r.reservation_id=a.reservation_id
           AND (r.successor_vmi_uid IS DISTINCT FROM a.vmi_uid OR r.successor_launcher_uid IS DISTINCT FROM a.launcher_uid))
       OR NOT EXISTS (SELECT 1 FROM public.vm_resource_waiters w WHERE w.request_id=a.request_id
           AND w.owner_kind='thread' AND w.thread_id=a.thread_id AND w.provision_generation=a.provision_generation AND w.state='released') THEN
        RAISE EXCEPTION 'VM retained disk predecessor unproven' USING ERRCODE='23514';
    END IF;
    IF owner_row.id IS NULL OR owner_row.execution_lane IS DISTINCT FROM 'pinned'
       OR owner_row.status IS DISTINCT FROM 'ended' OR owner_row.ended_at IS DISTINCT FROM soft.settled_at
       OR owner_row.runtime_generation IS DISTINCT FROM a.runtime_generation
       OR d.runtime_generation IS DISTINCT FROM a.runtime_generation
       OR source.request_id IS NULL OR source.owner_kind IS DISTINCT FROM 'thread'
       OR source.thread_id IS DISTINCT FROM a.thread_id OR source.state IS DISTINCT FROM 'succeeded'
       OR source.origin IS DISTINCT FROM 'initial' OR source.thread_wake_operation_id IS NOT NULL
       OR source.thread_runtime_generation IS DISTINCT FROM a.runtime_generation
       OR source.thread_agent_id IS DISTINCT FROM a.agent_id OR source.thread_attach_token IS DISTINCT FROM a.attach_token
       OR source.provision_generation IS DISTINCT FROM a.provision_generation
       OR source.observed_vm_uid IS DISTINCT FROM a.vm_uid OR source.observed_pvc_uid IS DISTINCT FROM a.pvc_uid
       OR source.controller_configuration->>'version' IS DISTINCT FROM '3'
       OR source.revision IS DISTINCT FROM d.source_revision
       OR owner_row.runtime_retirement_token IS DISTINCT FROM d.retirement_token
       OR d.retirement_token=a.retirement_token OR owner_row.runtime_retirement_permanent IS DISTINCT FROM true
       OR owner_row.runtime_retirement_authorized_at IS NULL
       OR owner_row.agent_id IS NOT NULL OR owner_row.runtime_attach_token IS NOT NULL
       OR owner_row.control_admission_agent_id IS NOT NULL
       OR EXISTS (SELECT 1 FROM public.agents agent WHERE agent.thread_id=a.thread_id)
       OR owner_row.runtime_retirement_context IS DISTINCT FROM d.retirement_context
       OR d.retirement_context->>'thread_id' IS DISTINCT FROM a.thread_id::text
       OR d.retirement_context->>'generation' IS DISTINCT FROM a.runtime_generation::text
       OR d.retirement_context->>'entry_status' IS DISTINCT FROM 'ended'
       OR d.retirement_context->>'settle_status' IS DISTINCT FROM 'ended'
       OR d.retirement_context->'agent_id' IS DISTINCT FROM 'null'::jsonb
       OR d.retirement_context->'runtime_attach_token' IS DISTINCT FROM 'null'::jsonb
       OR d.retirement_context->'control_admission_agent_id' IS DISTINCT FROM 'null'::jsonb
       OR cleanup.id IS NULL OR cleanup.owner_kind IS DISTINCT FROM 'thread'
       OR cleanup.owner_id IS DISTINCT FROM a.thread_id OR cleanup.pvc_uid IS DISTINCT FROM a.pvc_uid
       OR cleanup.parent_admission_id IS NOT NULL
       OR cleanup.source IS DISTINCT FROM 'pinned_thread_retained_disk_purge'
       OR cleanup.request_id IS DISTINCT FROM d.cleanup_request_id
       OR cleanup.intent_digest IS DISTINCT FROM new_digest OR d.intent_digest IS DISTINCT FROM new_digest
       OR (cleanup.completed_at IS NOT NULL AND (cleanup.outcome IS DISTINCT FROM 'completed'
           OR NOT EXISTS (SELECT 1 FROM public.vm_thread_retained_disk_purge_receipts p WHERE p.cleanup_admission_id=d.cleanup_admission_id))) THEN
        RAISE EXCEPTION 'VM retained disk current authority changed' USING ERRCODE='23514';
    END IF;
    captured := d.retirement_context->'vm';
    vm := owner_row.metadata->'vm';
    IF after_endpoint_cleanup AND NOT (owner_row.metadata ? 'vm')
       AND owner_row.runtime_retirement_external_cleanup IS NOT NULL
       AND owner_row.runtime_retirement_external_cleanup =
           public.pinned_retirement_external_cleanup_expected(d.retirement_context,d.runtime_generation,d.retirement_token)
       AND EXISTS (SELECT 1 FROM public.vm_thread_retained_disk_purge_receipts p WHERE p.cleanup_admission_id=d.cleanup_admission_id) THEN
        vm := captured;
    END IF;
    -- A never-Ready charge stays unbound even if metadata recorded an observed
    -- VMI/launcher before Ready. As in 0295, only an actual reservation binding
    -- constrains those fields; its physical proof still requires whole-runtime absence.
    IF vm->>'creation_request_id' IS DISTINCT FROM a.request_id::text
       OR vm->>'provision_generation' IS DISTINCT FROM a.provision_generation::text
       OR vm->>'identity_provision_generation' IS DISTINCT FROM a.provision_generation::text
       OR vm->'identity_authenticated' IS DISTINCT FROM 'true'::jsonb
       OR vm->>'vm_uid' IS DISTINCT FROM a.vm_uid::text OR vm->>'rootdisk_pvc_uid' IS DISTINCT FROM a.pvc_uid::text
       OR vm->>'status' IS DISTINCT FROM 'deleted'
       OR (vm->>'_runtime_incarnation' IS NOT NULL AND vm->>'_runtime_incarnation'<>a.vm_uid::text)
       OR (a.vmi_uid IS NOT NULL AND (vm->>'vmi_uid' IS DISTINCT FROM a.vmi_uid::text OR vm->>'active_pod_uid' IS DISTINCT FROM a.launcher_uid::text))
       OR captured->>'creation_request_id' IS DISTINCT FROM a.request_id::text
       OR captured->>'provision_generation' IS DISTINCT FROM a.provision_generation::text
       OR captured->>'identity_provision_generation' IS DISTINCT FROM a.provision_generation::text
       OR captured->'identity_authenticated' IS DISTINCT FROM 'true'::jsonb
       OR captured->>'vm_uid' IS DISTINCT FROM a.vm_uid::text OR captured->>'rootdisk_pvc_uid' IS DISTINCT FROM a.pvc_uid::text
       OR captured->>'status' IS DISTINCT FROM 'deleted'
       OR (captured->>'_runtime_incarnation' IS NOT NULL AND captured->>'_runtime_incarnation'<>a.vm_uid::text)
       OR (a.vmi_uid IS NOT NULL AND (captured->>'vmi_uid' IS DISTINCT FROM a.vmi_uid::text OR captured->>'active_pod_uid' IS DISTINCT FROM a.launcher_uid::text))
       OR EXISTS (SELECT 1 FROM public.vm_idle_access_leases l WHERE l.owner_kind='thread' AND l.owner_id=a.thread_id
           AND l.closed_at IS NULL AND l.expires_at>clock_timestamp())
       OR EXISTS (SELECT 1 FROM public.vm_workspace_recoveries r
           LEFT JOIN public.vm_workspace_recovery_retention_pins pin ON pin.recovery_id=r.id AND pin.released_at IS NULL
           WHERE r.resolved_at IS NULL AND ((r.owner_kind='thread' AND r.owner_id=a.thread_id) OR pin.pvc_uid=a.pvc_uid))
       OR EXISTS (SELECT 1 FROM public.vm_workspace_recovery_retention_pins pin WHERE pin.pvc_uid=a.pvc_uid AND pin.released_at IS NULL)
       OR EXISTS (SELECT 1 FROM public.vm_workspace_cleanup_admissions c
           WHERE ((c.owner_kind='thread' AND c.owner_id=a.thread_id) OR c.pvc_uid=a.pvc_uid)
             AND c.completed_at IS NULL AND c.id<>d.cleanup_admission_id AND c.parent_admission_id IS DISTINCT FROM d.cleanup_admission_id) THEN
        RAISE EXCEPTION 'VM retained disk backing or access changed' USING ERRCODE='23514';
    END IF;
    RETURN true;
END;
$$;

CREATE FUNCTION public.guard_vm_thread_retained_disk_purge_authority() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP<>'INSERT' THEN
        RAISE EXCEPTION 'VM retained disk authority is append-only' USING ERRCODE='23514';
    END IF;
    PERFORM public.validate_vm_thread_retained_disk_purge(NEW);
    RETURN NEW;
END;
$$;
CREATE TRIGGER guard_vm_thread_retained_disk_purge_authority
BEFORE INSERT OR UPDATE OR DELETE ON public.vm_thread_retained_disk_purge_authorities
FOR EACH ROW EXECUTE FUNCTION public.guard_vm_thread_retained_disk_purge_authority();

CREATE FUNCTION public.validate_vm_thread_retained_disk_purge_receipt(
    d public.vm_thread_retained_disk_purge_authorities, proof jsonb, after_endpoint_cleanup boolean DEFAULT false)
RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE a public.vm_resource_thread_cleanup_authorities%ROWTYPE;
BEGIN
    PERFORM public.validate_vm_thread_retained_disk_purge(d,after_endpoint_cleanup);
    SELECT * INTO a FROM public.vm_resource_thread_cleanup_authorities WHERE cleanup_admission_id=d.compute_cleanup_admission_id;
    IF proof IS DISTINCT FROM jsonb_build_object(
        'version',1,'kind','vm_cleanup_physical_stop','owner_kind','thread','owner_id',a.thread_id,
        'provision_generation',a.provision_generation,'vm_uid',a.vm_uid,'vmi_uid',a.vmi_uid,
        'launcher_uid',a.launcher_uid,'pvc_uid',a.pvc_uid,
        'vm_absent',true,'vmi_absent',true,'launcher_absent',true,
        'same_generation_replacement',false,'controller_authenticated',true,'pvc_disposition','purged') THEN
        RAISE EXCEPTION 'VM retained disk purge unproven' USING ERRCODE='23514';
    END IF;
    RETURN true;
END;
$$;
CREATE FUNCTION public.guard_vm_thread_retained_disk_purge_receipt() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE d public.vm_thread_retained_disk_purge_authorities%ROWTYPE;
BEGIN
    IF TG_OP<>'INSERT' THEN
        RAISE EXCEPTION 'VM retained disk receipt is append-only' USING ERRCODE='23514';
    END IF;
    SELECT * INTO d FROM public.vm_thread_retained_disk_purge_authorities WHERE cleanup_admission_id=NEW.cleanup_admission_id;
    IF d.cleanup_admission_id IS NULL THEN
        RAISE EXCEPTION 'VM retained disk authority missing' USING ERRCODE='23514';
    END IF;
    PERFORM public.validate_vm_thread_retained_disk_purge_receipt(d,NEW.purge_evidence);
    RETURN NEW;
END;
$$;
CREATE TRIGGER guard_vm_thread_retained_disk_purge_receipt
BEFORE INSERT OR UPDATE OR DELETE ON public.vm_thread_retained_disk_purge_receipts
FOR EACH ROW EXECUTE FUNCTION public.guard_vm_thread_retained_disk_purge_receipt();

CREATE FUNCTION public.guard_vm_thread_retained_disk_purge_completion() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE d public.vm_thread_retained_disk_purge_authorities%ROWTYPE;
        proof jsonb;
BEGIN
    -- Keep this parent's identity immutable while its live owner still exists.
    -- In particular a source rewrite must not evade the completion trigger.
    IF TG_OP='UPDATE' AND (
       (NEW.id,NEW.owner_kind,NEW.owner_id,NEW.pvc_uid,NEW.source,NEW.request_id,
        NEW.intent_digest,NEW.parent_admission_id,NEW.admitted_at) IS DISTINCT FROM
       (OLD.id,OLD.owner_kind,OLD.owner_id,OLD.pvc_uid,OLD.source,OLD.request_id,
        OLD.intent_digest,OLD.parent_admission_id,OLD.admitted_at)
       OR (OLD.completed_at IS NOT NULL AND NEW IS DISTINCT FROM OLD)) THEN
        RAISE EXCEPTION 'VM retained disk cleanup identity is immutable' USING ERRCODE='23514';
    END IF;
    IF NEW.completed_at IS NOT NULL THEN
        SELECT * INTO d FROM public.vm_thread_retained_disk_purge_authorities WHERE cleanup_admission_id=NEW.id;
        SELECT purge_evidence INTO proof FROM public.vm_thread_retained_disk_purge_receipts WHERE cleanup_admission_id=NEW.id;
        IF NEW.outcome IS DISTINCT FROM 'completed' OR d.cleanup_admission_id IS NULL OR proof IS NULL THEN
            RAISE EXCEPTION 'VM retained disk completion lacks purge receipt' USING ERRCODE='23514';
        END IF;
        PERFORM public.validate_vm_thread_retained_disk_purge_receipt(d,proof);
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER guard_vm_thread_retained_disk_purge_completion_insert
BEFORE INSERT ON public.vm_workspace_cleanup_admissions
FOR EACH ROW WHEN (NEW.source='pinned_thread_retained_disk_purge')
EXECUTE FUNCTION public.guard_vm_thread_retained_disk_purge_completion();
CREATE TRIGGER guard_vm_thread_retained_disk_purge_completion_update
BEFORE UPDATE ON public.vm_workspace_cleanup_admissions
FOR EACH ROW WHEN (OLD.source='pinned_thread_retained_disk_purge' OR NEW.source='pinned_thread_retained_disk_purge')
EXECUTE FUNCTION public.guard_vm_thread_retained_disk_purge_completion();

-- Keep the complete 0295 evidence function, including failed-initial handling.
ALTER FUNCTION public.vm_thread_creation_delete_evidence(public.threads)
RENAME TO vm_thread_creation_compute_delete_evidence;
CREATE FUNCTION public.vm_thread_creation_delete_evidence(owner_row public.threads)
RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE evidence jsonb;
        d public.vm_thread_retained_disk_purge_authorities%ROWTYPE;
        a public.vm_resource_thread_cleanup_authorities%ROWTYPE;
        proof jsonb;
BEGIN
    evidence := public.vm_thread_creation_compute_delete_evidence(owner_row);
    IF evidence IS NOT NULL THEN RETURN evidence; END IF;
    SELECT disk.* INTO d FROM public.vm_thread_retained_disk_purge_authorities disk
        JOIN public.vm_resource_thread_cleanup_authorities compute ON compute.cleanup_admission_id=disk.compute_cleanup_admission_id
        WHERE compute.thread_id=owner_row.id AND disk.runtime_generation=owner_row.runtime_generation
          AND disk.retirement_token=owner_row.runtime_retirement_token;
    IF NOT FOUND THEN RETURN NULL; END IF;
    SELECT * INTO a FROM public.vm_resource_thread_cleanup_authorities WHERE cleanup_admission_id=d.compute_cleanup_admission_id;
    SELECT purge_evidence INTO proof FROM public.vm_thread_retained_disk_purge_receipts WHERE cleanup_admission_id=d.cleanup_admission_id;
    IF proof IS NULL
       OR NOT EXISTS (SELECT 1 FROM public.vm_workspace_cleanup_admissions c WHERE c.id=d.cleanup_admission_id AND c.completed_at IS NOT NULL AND c.outcome='completed')
       OR NOT EXISTS (SELECT 1 FROM public.thread_runtime_retirement_outcomes o
           WHERE o.thread_id=owner_row.id AND o.runtime_generation=d.runtime_generation AND o.retirement_token=d.retirement_token
             AND o.permanent AND o.outcome='deleted' AND o.disposition='ended' AND o.agent_id IS NULL AND o.runtime_attach_token IS NULL)
       OR owner_row.runtime_retirement_external_cleanup IS NULL
       OR owner_row.runtime_retirement_external_cleanup IS DISTINCT FROM
           public.pinned_retirement_external_cleanup_expected(d.retirement_context,d.runtime_generation,d.retirement_token)
       OR NOT public.pinned_retirement_external_resources_absent(owner_row.id,owner_row.metadata)
       OR EXISTS (SELECT 1 FROM public.vm_creation_retries r WHERE r.owner_kind='thread' AND r.thread_id=owner_row.id AND r.state<>'succeeded')
       OR EXISTS (SELECT 1 FROM public.vm_creation_effects e JOIN public.vm_creation_retries r USING(request_id)
           WHERE r.owner_kind='thread' AND r.thread_id=owner_row.id AND e.state='issued')
       OR EXISTS (SELECT 1 FROM public.vm_resource_reservations v JOIN public.vm_creation_retries r USING(request_id)
           WHERE r.owner_kind='thread' AND r.thread_id=owner_row.id AND v.state<>'released')
       OR EXISTS (SELECT 1 FROM public.vm_resource_waiters w WHERE w.owner_kind='thread' AND w.thread_id=owner_row.id AND w.state NOT IN ('released','cancelled'))
       OR EXISTS (SELECT 1 FROM public.vm_workspace_cleanup_admissions c WHERE c.owner_kind='thread' AND c.owner_id=owner_row.id AND c.completed_at IS NULL) THEN
        RETURN NULL;
    END IF;
    PERFORM public.validate_vm_thread_retained_disk_purge_receipt(d,proof,true);
    RETURN jsonb_build_object('version',3,'kind','retained_vm_disk_purge',
        'request_id',a.request_id,'compute_cleanup_admission_id',a.cleanup_admission_id,
        'disk_cleanup_admission_id',d.cleanup_admission_id,
        'reservation_id',a.reservation_id,'reservation_revision',a.reservation_revision,
        'runtime_generation',d.runtime_generation,'compute_retirement_token',a.retirement_token,
        'retirement_token',d.retirement_token,'local_quiescence',owner_row.runtime_retirement_local_quiescence,
        'external_cleanup',owner_row.runtime_retirement_external_cleanup,'purge_evidence',proof);
END;
$$;
COMMIT;
