-- migration: 0295_vm_thread_cleanup_resource_release.sql
-- description: Exact pinned End authority and authenticated compute release.
-- depends-on: 0294_workspace_creation_owner_history_idx.notx.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE public.vm_resource_thread_cleanup_authorities (
    cleanup_admission_id uuid PRIMARY KEY REFERENCES public.vm_workspace_cleanup_admissions(id),
    reservation_id uuid NOT NULL UNIQUE REFERENCES public.vm_resource_reservations(id),
    reservation_revision bigint NOT NULL,
    request_id uuid NOT NULL REFERENCES public.vm_creation_retries(request_id),
    thread_id uuid NOT NULL REFERENCES public.vm_thread_creation_owners(thread_id),
    runtime_generation uuid NOT NULL,
    retirement_token uuid NOT NULL,
    agent_id uuid,
    attach_token uuid,
    provision_generation uuid NOT NULL,
    vm_uid uuid NOT NULL,
    pvc_uid uuid NOT NULL,
    vmi_uid uuid,
    launcher_uid uuid,
    purge_disk boolean NOT NULL,
    intent_digest text NOT NULL,
    cleanup_request_id uuid NOT NULL,
    retirement_context jsonb,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK ((vmi_uid IS NULL)=(launcher_uid IS NULL))
);

CREATE TABLE public.vm_resource_thread_cleanup_stops (
    cleanup_admission_id uuid PRIMARY KEY REFERENCES public.vm_resource_thread_cleanup_authorities(cleanup_admission_id),
    process_zero_receipt_id uuid NOT NULL REFERENCES public.managed_repository_process_zero_receipts(id),
    stop_evidence jsonb NOT NULL,
    accepted_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- Shared by preparation, receipt insertion and release. Lock the live owner
-- before its source/cleanup/charge and recheck after controller I/O. An ended
-- row is eligible only when the original exact soft-End lineage survives.
CREATE FUNCTION public.validate_vm_thread_cleanup_authority(a public.vm_resource_thread_cleanup_authorities, allow_released boolean DEFAULT false)
RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE
    owner_row public.threads%ROWTYPE;
    source public.vm_creation_retries%ROWTYPE;
    charge public.vm_resource_reservations%ROWTYPE;
    cleanup public.vm_workspace_cleanup_admissions%ROWTYPE;
    outcome public.thread_runtime_retirement_outcomes%ROWTYPE;
    vm jsonb;
    expected_digest text;
BEGIN
    SELECT * INTO owner_row FROM public.threads WHERE id=a.thread_id FOR UPDATE;
    SELECT * INTO source FROM public.vm_creation_retries WHERE request_id=a.request_id FOR UPDATE;
    SELECT * INTO cleanup FROM public.vm_workspace_cleanup_admissions WHERE id=a.cleanup_admission_id FOR UPDATE;
    SELECT * INTO charge FROM public.vm_resource_reservations WHERE id=a.reservation_id FOR UPDATE;
    vm := owner_row.metadata->'vm';
    -- Permanent endpoint cleanup deliberately removes metadata.vm after this
    -- authority's purge and debit. Its existing exact external receipt then
    -- authorizes reading the immutable Begin snapshot for final deletion.
    IF allow_released AND a.purge_disk AND NOT (owner_row.metadata ? 'vm')
       AND owner_row.runtime_retirement_external_cleanup IS NOT NULL
       AND owner_row.runtime_retirement_external_cleanup =
           public.pinned_retirement_external_cleanup_expected(owner_row.runtime_retirement_context,
               owner_row.runtime_generation,owner_row.runtime_retirement_token) THEN
        vm := a.retirement_context->'vm';
    END IF;
    expected_digest := 'sha256:' || encode(sha256(convert_to(format(
        '{"owner_id":"%s","owner_kind":"thread","provision_generation":"%s","purge_disk":%s,"pvc_uid":"%s","resource":"vm_workspace","source":"pinned_thread_retirement","vm_uid":"%s"}',
        a.thread_id,a.provision_generation,a.purge_disk::text,a.pvc_uid,a.vm_uid), 'UTF8')),'hex');
    IF owner_row.id IS NULL OR owner_row.execution_lane IS DISTINCT FROM 'pinned'
       OR owner_row.runtime_generation IS DISTINCT FROM a.runtime_generation
       OR source.request_id IS NULL OR source.owner_kind IS DISTINCT FROM 'thread'
       OR source.thread_id IS DISTINCT FROM a.thread_id OR source.state IS DISTINCT FROM 'succeeded'
       OR source.thread_runtime_generation IS DISTINCT FROM a.runtime_generation
       OR source.thread_agent_id IS DISTINCT FROM a.agent_id
       OR source.thread_attach_token IS DISTINCT FROM a.attach_token
       OR source.provision_generation IS DISTINCT FROM a.provision_generation
       OR source.observed_vm_uid IS DISTINCT FROM a.vm_uid
       OR source.observed_pvc_uid IS DISTINCT FROM a.pvc_uid
       OR source.controller_configuration->>'version' IS DISTINCT FROM '3'
       OR charge.id IS NULL OR charge.request_id IS DISTINCT FROM a.request_id
       OR charge.revision IS DISTINCT FROM a.reservation_revision OR charge.resource_version<>2
       OR (charge.state NOT IN ('reserved','active','warm','teardown')
           AND NOT (allow_released AND charge.state='released'))
       OR charge.vmi_uid IS DISTINCT FROM a.vmi_uid
       OR charge.launcher_uid IS DISTINCT FROM a.launcher_uid
       OR (charge.vm_uid IS NOT NULL AND charge.vm_uid IS DISTINCT FROM a.vm_uid)
       OR (a.vmi_uid IS NULL AND (source.ready_at IS NOT NULL OR charge.vm_uid IS NOT NULL))
       OR (a.vmi_uid IS NOT NULL AND (source.ready_at IS NULL OR charge.vm_uid IS DISTINCT FROM a.vm_uid))
       OR EXISTS (SELECT 1 FROM public.vm_resource_reservations newer
           WHERE newer.request_id=a.request_id AND newer.revision>a.reservation_revision)
       OR EXISTS (SELECT 1 FROM public.vm_resource_recovery_successors successor
           WHERE successor.reservation_id=a.reservation_id
             AND (successor.successor_vmi_uid IS DISTINCT FROM a.vmi_uid
                  OR successor.successor_launcher_uid IS DISTINCT FROM a.launcher_uid))
       OR NOT EXISTS (SELECT 1 FROM public.vm_resource_waiters w
           WHERE w.request_id=a.request_id AND w.owner_kind='thread' AND w.thread_id=a.thread_id
             AND w.provision_generation=a.provision_generation
             AND (w.state='admitted' OR (allow_released AND w.state='released')))
       OR cleanup.id IS NULL OR cleanup.owner_kind IS DISTINCT FROM 'thread'
       OR cleanup.owner_id IS DISTINCT FROM a.thread_id OR cleanup.pvc_uid IS DISTINCT FROM a.pvc_uid
       OR cleanup.source IS DISTINCT FROM 'pinned_thread_retirement'
       OR cleanup.request_id IS DISTINCT FROM a.cleanup_request_id
       OR cleanup.intent_digest IS DISTINCT FROM a.intent_digest OR a.intent_digest<>expected_digest
       OR (cleanup.completed_at IS NOT NULL AND cleanup.outcome IS DISTINCT FROM 'completed')
       OR vm->>'creation_request_id' IS DISTINCT FROM a.request_id::text
       OR vm->>'provision_generation' IS DISTINCT FROM a.provision_generation::text
       OR vm->>'identity_provision_generation' IS DISTINCT FROM a.provision_generation::text
       OR vm->'identity_authenticated' IS DISTINCT FROM 'true'::jsonb
       OR vm->>'vm_uid' IS DISTINCT FROM a.vm_uid::text
       OR vm->>'rootdisk_pvc_uid' IS DISTINCT FROM a.pvc_uid::text
       OR (vm->>'_runtime_incarnation' IS NOT NULL AND vm->>'_runtime_incarnation'<>a.vm_uid::text)
       OR (a.vmi_uid IS NOT NULL AND (
           vm->>'vmi_uid' IS DISTINCT FROM a.vmi_uid::text
           OR vm->>'active_pod_uid' IS DISTINCT FROM a.launcher_uid::text)) THEN
        RAISE EXCEPTION 'VM thread cleanup source identity changed' USING ERRCODE='23514';
    END IF;
    IF owner_row.runtime_retirement_token IS NOT NULL THEN
        IF owner_row.runtime_retirement_token IS DISTINCT FROM a.retirement_token
           OR owner_row.runtime_retirement_authorized_at IS NULL
           OR owner_row.runtime_retirement_permanent IS DISTINCT FROM a.purge_disk
           OR owner_row.agent_id IS DISTINCT FROM a.agent_id
           OR owner_row.runtime_attach_token IS DISTINCT FROM a.attach_token
           OR a.retirement_context IS NULL
           OR a.retirement_context IS DISTINCT FROM owner_row.runtime_retirement_context
           OR a.retirement_context->>'thread_id' IS DISTINCT FROM a.thread_id::text
           OR a.retirement_context->>'generation' IS DISTINCT FROM a.runtime_generation::text
           OR a.retirement_context->>'settle_status' IS DISTINCT FROM 'ended'
           OR a.retirement_context->'vm'->>'provision_generation' IS DISTINCT FROM a.provision_generation::text
           OR a.retirement_context->'vm'->>'vm_uid' IS DISTINCT FROM a.vm_uid::text
           OR a.retirement_context->'vm'->>'rootdisk_pvc_uid' IS DISTINCT FROM a.pvc_uid::text THEN
            RAISE EXCEPTION 'VM thread cleanup retirement changed' USING ERRCODE='23514';
        END IF;
    ELSE
        SELECT * INTO outcome FROM public.thread_runtime_retirement_outcomes
         WHERE thread_id=a.thread_id AND runtime_generation=a.runtime_generation
           AND retirement_token=a.retirement_token;
        IF owner_row.status IS DISTINCT FROM 'ended' OR owner_row.agent_id IS NOT NULL
           OR owner_row.runtime_attach_token IS NOT NULL OR a.purge_disk
           OR outcome.thread_id IS NULL OR outcome.disposition IS DISTINCT FROM 'ended'
           OR outcome.permanent OR outcome.outcome IS DISTINCT FROM 'settled'
           OR outcome.agent_id IS DISTINCT FROM a.agent_id
           OR outcome.runtime_attach_token IS DISTINCT FROM a.attach_token
           OR owner_row.ended_at IS DISTINCT FROM outcome.settled_at
           OR vm->>'status' IS DISTINCT FROM 'deleted'
           OR cleanup.completed_at IS NULL OR cleanup.completed_at>outcome.settled_at
           OR source.resolved_at IS NULL OR source.resolved_at>cleanup.admitted_at
           OR NOT EXISTS (SELECT 1 FROM public.managed_repository_process_zero_receipts p
               WHERE p.owner_kind='thread' AND p.owner_id=a.thread_id AND p.scope='vm'
                 AND p.provisioner='vm' AND p.runtime_incarnation=a.provision_generation::text
                 AND p.observed_at<=outcome.settled_at) THEN
            RAISE EXCEPTION 'VM thread cleanup settled lineage unproven' USING ERRCODE='23514';
        END IF;
    END IF;
    RETURN true;
END;
$$;

CREATE FUNCTION public.guard_vm_thread_cleanup_authority() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP<>'INSERT' THEN
        RAISE EXCEPTION 'VM thread cleanup authority is append-only' USING ERRCODE='23514';
    END IF;
    PERFORM public.validate_vm_thread_cleanup_authority(NEW);
    RETURN NEW;
END;
$$;
CREATE TRIGGER guard_vm_thread_cleanup_authority
BEFORE INSERT OR UPDATE OR DELETE ON public.vm_resource_thread_cleanup_authorities
FOR EACH ROW EXECUTE FUNCTION public.guard_vm_thread_cleanup_authority();

CREATE FUNCTION public.validate_vm_thread_cleanup_stop(a public.vm_resource_thread_cleanup_authorities,
    s public.vm_resource_thread_cleanup_stops, allow_released boolean DEFAULT false) RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE expected jsonb;
BEGIN
    PERFORM public.validate_vm_thread_cleanup_authority(a,allow_released);
    expected := jsonb_build_object(
        'version',1,'kind','vm_cleanup_physical_stop','owner_kind','thread','owner_id',a.thread_id,
        'provision_generation',a.provision_generation,'vm_uid',a.vm_uid,'vmi_uid',a.vmi_uid,
        'launcher_uid',a.launcher_uid,'pvc_uid',a.pvc_uid,
        'vm_absent',true,'vmi_absent',true,'launcher_absent',true,
        'same_generation_replacement',false,'controller_authenticated',true,
        'pvc_disposition',CASE WHEN a.purge_disk THEN 'purged' ELSE 'retained' END);
    IF s.stop_evidence IS DISTINCT FROM expected
       OR NOT EXISTS (SELECT 1 FROM public.vm_resource_reservations r
           WHERE r.id=a.reservation_id AND (r.state='teardown' OR (allow_released AND r.state='released')))
       OR NOT EXISTS (SELECT 1 FROM public.vm_workspace_cleanup_admissions c
           WHERE c.id=a.cleanup_admission_id AND c.completed_at IS NOT NULL AND c.outcome='completed')
       OR NOT EXISTS (SELECT 1 FROM public.managed_repository_process_zero_receipts p
           WHERE p.id=s.process_zero_receipt_id AND p.owner_kind='thread' AND p.owner_id=a.thread_id
             AND p.scope='vm' AND p.provisioner='vm' AND p.runtime_incarnation=a.provision_generation::text) THEN
        RAISE EXCEPTION 'VM thread cleanup stop unproven' USING ERRCODE='23514';
    END IF;
    RETURN true;
END;
$$;
CREATE FUNCTION public.guard_vm_thread_cleanup_stop() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE authority public.vm_resource_thread_cleanup_authorities%ROWTYPE;
BEGIN
    IF TG_OP<>'INSERT' THEN
        RAISE EXCEPTION 'VM thread cleanup stop is append-only' USING ERRCODE='23514';
    END IF;
    SELECT * INTO authority FROM public.vm_resource_thread_cleanup_authorities
        WHERE cleanup_admission_id=NEW.cleanup_admission_id;
    IF authority.cleanup_admission_id IS NULL THEN
        RAISE EXCEPTION 'VM thread cleanup authority missing' USING ERRCODE='23514';
    END IF;
    PERFORM public.validate_vm_thread_cleanup_stop(authority,NEW);
    RETURN NEW;
END;
$$;
CREATE TRIGGER guard_vm_thread_cleanup_stop
BEFORE INSERT OR UPDATE OR DELETE ON public.vm_resource_thread_cleanup_stops
FOR EACH ROW EXECUTE FUNCTION public.guard_vm_thread_cleanup_stop();

-- The preexisting trigger function is unchanged. Its WHEN predicate routes
-- only a fully validated new thread cleanup through this narrow extension.
-- Job cleanup, idle release, cancellation, and all malformed old proofs keep
-- their original guard and transition/immutability triggers.
CREATE FUNCTION public.valid_vm_thread_cleanup_release(old_row public.vm_resource_reservations,
    new_row public.vm_resource_reservations) RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE a public.vm_resource_thread_cleanup_authorities%ROWTYPE;
        s public.vm_resource_thread_cleanup_stops%ROWTYPE;
BEGIN
    IF new_row.resource_version<>2 OR new_row.state<>'released' OR old_row.state='released'
       OR new_row.release_evidence->>'kind' IS DISTINCT FROM 'exact_cleanup_compute_absent'
       OR NOT EXISTS (SELECT 1 FROM public.vm_resource_waiters w
           WHERE w.request_id=new_row.request_id AND w.owner_kind='thread') THEN
        RETURN false;
    END IF;
    SELECT * INTO a FROM public.vm_resource_thread_cleanup_authorities
        WHERE cleanup_admission_id::text=new_row.release_evidence->>'cleanup_admission_id';
    SELECT * INTO s FROM public.vm_resource_thread_cleanup_stops WHERE cleanup_admission_id=a.cleanup_admission_id;
    IF a.cleanup_admission_id IS NULL OR s.cleanup_admission_id IS NULL OR old_row.state<>'teardown'
       OR a.reservation_id IS DISTINCT FROM new_row.id OR a.reservation_revision IS DISTINCT FROM new_row.revision
       OR a.request_id IS DISTINCT FROM new_row.request_id
       OR new_row.release_evidence IS DISTINCT FROM jsonb_build_object(
           'kind','exact_cleanup_compute_absent','owner_kind','thread','thread_id',a.thread_id,
           'cleanup_admission_id',a.cleanup_admission_id,'reservation_revision',a.reservation_revision,
           'stop_evidence_digest','sha256:'||encode(sha256(convert_to(s.stop_evidence::text,'UTF8')),'hex')) THEN
        RAISE EXCEPTION 'VM thread cleanup release unproven' USING ERRCODE='23514';
    END IF;
    PERFORM public.validate_vm_thread_cleanup_stop(a,s);
    RETURN true;
END;
$$;
DROP TRIGGER guard_vm_resource_release_v2 ON public.vm_resource_reservations;
CREATE TRIGGER guard_vm_resource_release_v2 BEFORE UPDATE ON public.vm_resource_reservations
FOR EACH ROW WHEN (NOT public.valid_vm_thread_cleanup_release(OLD,NEW))
EXECUTE FUNCTION public.guard_vm_resource_release_v2();
-- Preserve the failed-initial deletion protocol verbatim. Adopted cleanup uses
-- its own authority/physical-purge receipt rather than forging that settlement.
ALTER FUNCTION public.vm_thread_creation_delete_evidence(public.threads)
RENAME TO vm_thread_creation_failed_initial_delete_evidence;
CREATE FUNCTION public.vm_thread_creation_delete_evidence(owner_row public.threads)
RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE evidence jsonb;
        a public.vm_resource_thread_cleanup_authorities%ROWTYPE;
        s public.vm_resource_thread_cleanup_stops%ROWTYPE;
        charge public.vm_resource_reservations%ROWTYPE;
BEGIN
    evidence := public.vm_thread_creation_failed_initial_delete_evidence(owner_row);
    IF evidence IS NOT NULL THEN RETURN evidence; END IF;
    SELECT * INTO a FROM public.vm_resource_thread_cleanup_authorities
        WHERE thread_id=owner_row.id AND runtime_generation=owner_row.runtime_generation
          AND retirement_token=owner_row.runtime_retirement_token AND purge_disk;
    IF NOT FOUND THEN RETURN NULL; END IF;
    SELECT * INTO s FROM public.vm_resource_thread_cleanup_stops
        WHERE cleanup_admission_id=a.cleanup_admission_id;
    IF NOT FOUND THEN RETURN NULL; END IF;
    SELECT * INTO charge FROM public.vm_resource_reservations WHERE id=a.reservation_id FOR UPDATE;
    IF owner_row.runtime_retirement_permanent IS DISTINCT FROM true
       OR charge.state IS DISTINCT FROM 'released'
       OR charge.release_evidence IS DISTINCT FROM jsonb_build_object(
           'kind','exact_cleanup_compute_absent','owner_kind','thread','thread_id',a.thread_id,
           'cleanup_admission_id',a.cleanup_admission_id,'reservation_revision',a.reservation_revision,
           'stop_evidence_digest','sha256:'||encode(sha256(convert_to(s.stop_evidence::text,'UTF8')),'hex'))
       OR NOT EXISTS (SELECT 1 FROM public.thread_runtime_retirement_outcomes o
           WHERE o.thread_id=owner_row.id AND o.runtime_generation=a.runtime_generation
             AND o.retirement_token=a.retirement_token AND o.permanent AND o.outcome='deleted'
             AND o.disposition='ended' AND o.agent_id IS NOT DISTINCT FROM a.agent_id
             AND o.runtime_attach_token IS NOT DISTINCT FROM a.attach_token)
       OR owner_row.runtime_retirement_external_cleanup IS NULL
       OR owner_row.runtime_retirement_external_cleanup IS DISTINCT FROM
           public.pinned_retirement_external_cleanup_expected(owner_row.runtime_retirement_context,
               owner_row.runtime_generation,owner_row.runtime_retirement_token)
       OR NOT public.pinned_retirement_external_resources_absent(owner_row.id,owner_row.metadata)
       OR EXISTS (SELECT 1 FROM public.vm_creation_retries r
           WHERE r.owner_kind='thread' AND r.thread_id=owner_row.id AND r.state<>'succeeded')
       OR EXISTS (SELECT 1 FROM public.vm_creation_effects e
           JOIN public.vm_creation_retries r USING(request_id)
           WHERE r.owner_kind='thread' AND r.thread_id=owner_row.id AND e.state='issued')
       OR EXISTS (SELECT 1 FROM public.vm_resource_reservations v
           JOIN public.vm_creation_retries r USING(request_id)
           WHERE r.owner_kind='thread' AND r.thread_id=owner_row.id AND v.state<>'released')
       OR EXISTS (SELECT 1 FROM public.vm_resource_waiters w
           WHERE w.owner_kind='thread' AND w.thread_id=owner_row.id AND w.state NOT IN ('released','cancelled'))
       OR EXISTS (SELECT 1 FROM public.vm_workspace_cleanup_admissions c
           WHERE c.owner_kind='thread' AND c.owner_id=owner_row.id AND c.completed_at IS NULL) THEN
        RETURN NULL;
    END IF;
    PERFORM public.validate_vm_thread_cleanup_stop(a,s,true);
    RETURN jsonb_build_object('version',2,'kind','adopted_vm_cleanup',
        'request_id',a.request_id,'cleanup_admission_id',a.cleanup_admission_id,
        'reservation_id',a.reservation_id,'reservation_revision',a.reservation_revision,
        'runtime_generation',a.runtime_generation,'retirement_token',a.retirement_token,
        'local_quiescence',owner_row.runtime_retirement_local_quiescence,
        'external_cleanup',owner_row.runtime_retirement_external_cleanup,
        'stop_evidence',s.stop_evidence);
END;
$$;
COMMIT;
