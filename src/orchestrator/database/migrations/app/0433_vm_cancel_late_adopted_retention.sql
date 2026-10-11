-- migration: 0433_vm_cancel_late_adopted_retention.sql
-- description: Retain exact Cancel-before-late-adoption Job VM without Ready.
-- depends-on: 0432_validate_vm_never_app_ready_retained_stop.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

-- This branch supplements policy 1 only. The caller still holds the existing
-- owner/PVC, queue/Job, retry, and reservation locks. A settled retry is never
-- treated as succeeded or Ready; all creation effects must already be observed.
CREATE FUNCTION public.vm_cancel_late_adopted_source(
    owner uuid, generation uuid, expected_vm uuid, pvc uuid, expected_request uuid
) RETURNS boolean LANGUAGE sql STABLE AS $body$
    SELECT EXISTS (
        SELECT 1 FROM public.jobs j
        JOIN public.run_queue q ON q.unit_id=j.id
        JOIN public.vm_creation_retries r ON r.owner_kind='job' AND r.job_id=j.id
        JOIN public.vm_resource_reservations v ON v.request_id=r.request_id
        JOIN public.vm_workspace_cleanup_admissions c ON c.id=r.creation_admission_id
        WHERE j.id=owner AND j.status='cancelled' AND j.execution_lane='stateless'
          AND j.parent_job_id IS NULL AND j.assigned_agent_id IS NULL
          AND COALESCE(j.context->'inherits_parent_workspace','false'::jsonb)='false'::jsonb
          AND j.context->'_stateless_cancel_cleanup_pending'='true'::jsonb
          AND j.context->'_vm_job_retained_resume' IS NULL
          AND j.context->>'_vm_creation_pending'=expected_request::text
          AND j.context->'vm'->>'creation_request_id'=expected_request::text
          AND j.context->'vm'->>'status' IN ('waiting_creation_configuration','retiring_process_zero')
          AND j.context->'vm'->>'provision_generation'=generation::text
          AND j.context->'vm'->>'identity_provision_generation'=generation::text
          AND j.context->'vm'->'identity_authenticated'='true'::jsonb
          AND j.context->'vm'->>'vm_uid'=expected_vm::text
          AND j.context->'vm'->>'rootdisk_pvc_uid'=pvc::text
          AND COALESCE(j.context->'vm'->'workspace_storage','null'::jsonb)='null'::jsonb
          AND q.unit_kind='worker_batch' AND q.state='done'
          AND q.leased_by IS NULL AND q.leased_until IS NULL
          AND r.request_id=expected_request AND r.provision_generation=generation
          AND r.state='settled' AND r.reason='creation_adopted'
          AND r.resolved_at IS NOT NULL AND r.boot_counted
          AND r.ready_at IS NULL AND r.claim_token IS NULL AND r.claim_expires_at IS NULL
          AND r.observed_vm_uid=expected_vm AND r.observed_pvc_uid=pvc
          AND COALESCE(r.canonical_request->'workspace_storage','null'::jsonb)='null'::jsonb
          AND c.owner_kind='job' AND c.owner_id=owner
          AND c.completed_at IS NOT NULL AND c.outcome='adopted'
          AND v.state IN ('reserved','active','warm','teardown')
          AND NOT EXISTS (SELECT 1 FROM public.vm_creation_retries later
              WHERE later.owner_kind='job' AND later.job_id=owner
                AND (later.created_at,later.request_id)>(r.created_at,r.request_id))
          AND (SELECT count(*) FROM public.vm_creation_effects e
               WHERE e.request_id=r.request_id)=3
          AND (SELECT count(DISTINCT e.effect_kind) FROM public.vm_creation_effects e
               WHERE e.request_id=r.request_id AND e.effect_kind IN ('rootdisk','cloud_init','vm')
                 AND e.state='observed' AND e.resolved_at IS NOT NULL
                 AND e.evidence<>'{}'::jsonb)=3
          AND EXISTS (SELECT 1 FROM public.vm_creation_effects e
              WHERE e.request_id=r.request_id AND e.effect_kind='rootdisk'
                AND e.evidence->>'pvc_uid'=pvc::text)
          AND EXISTS (SELECT 1 FROM public.vm_creation_effects e
              WHERE e.request_id=r.request_id AND e.effect_kind='vm'
                AND e.evidence->>'uid'=expected_vm::text
                AND e.evidence->>'pvc_uid'=pvc::text)
          AND NOT EXISTS (SELECT 1 FROM public.vm_creation_effects e
              WHERE e.request_id=r.request_id AND e.state='issued')
    );
$body$;

-- Copy the latest policy-1 selector (0341) and the current held-stop guards
-- (0431) verbatim except for this exact settled/adopted branch.
CREATE OR REPLACE FUNCTION public.vm_job_cancel_retention_candidate(
    owner uuid, generation uuid, expected_vm uuid, pvc uuid, old_parent uuid,
    retaining_parent uuid DEFAULT NULL, existing_stop boolean DEFAULT false
) RETURNS jsonb LANGUAGE plpgsql AS $body$
DECLARE j public.jobs%ROWTYPE;
        r public.vm_creation_retries%ROWTYPE;
        v public.vm_resource_reservations%ROWTYPE;
        q public.run_queue%ROWTYPE;
        vm jsonb;
BEGIN
    SELECT * INTO q FROM public.run_queue WHERE unit_id=owner FOR UPDATE;
    SELECT * INTO j FROM public.jobs WHERE id=owner FOR UPDATE;
    SELECT * INTO r FROM public.vm_creation_retries
     WHERE owner_kind='job' AND job_id=owner AND provision_generation=generation FOR UPDATE;
    SELECT * INTO v FROM public.vm_resource_reservations WHERE request_id=r.request_id FOR UPDATE;
    vm := j.context->'vm';
    IF j.id IS NULL OR j.status IS DISTINCT FROM 'cancelled'
       OR j.execution_lane IS DISTINCT FROM 'stateless'
       OR j.assigned_agent_id IS NOT NULL OR j.parent_job_id IS NOT NULL
       OR COALESCE(j.context->'inherits_parent_workspace','false'::jsonb) IS DISTINCT FROM 'false'::jsonb
       OR j.context->'_stateless_cancel_cleanup_pending' IS DISTINCT FROM 'true'::jsonb
       OR jsonb_typeof(vm) IS DISTINCT FROM 'object'
       OR vm->>'provision_generation' IS DISTINCT FROM generation::text
       OR vm->>'vm_uid' IS DISTINCT FROM expected_vm::text
       OR vm->>'rootdisk_pvc_uid' IS DISTINCT FROM pvc::text
       OR vm->>'status' IS NULL OR (vm->>'status' NOT IN ('created','ssh_pending','ssh_unreachable','retiring_process_zero')
           AND NOT (vm->>'status'='waiting_creation_configuration' AND r.state='settled'
               AND public.vm_cancel_late_adopted_source(owner,generation,expected_vm,pvc,r.request_id))
           AND NOT (vm->>'status'='deleted' AND v.state='teardown'
               AND public.vm_job_deleted_retention_replay_allowed(1,owner,generation,
                   expected_vm,pvc,retaining_parent,existing_stop,r.request_id,
                   v.id,v.revision,v.vmi_uid,v.launcher_uid,v.node_uid)))
       OR vm->'identity_authenticated' IS DISTINCT FROM 'true'::jsonb
       OR vm->>'identity_provision_generation' IS DISTINCT FROM generation::text
       OR COALESCE(vm->'workspace_storage','null'::jsonb)<>'null'::jsonb
       OR COALESCE(r.canonical_request->'workspace_storage','null'::jsonb)<>'null'::jsonb
       OR q.unit_id IS NULL OR q.unit_kind IS DISTINCT FROM 'worker_batch'
       OR q.state IS DISTINCT FROM 'done' OR q.leased_by IS NOT NULL OR q.leased_until IS NOT NULL
       OR r.request_id IS NULL OR (r.state IS DISTINCT FROM 'succeeded'
           AND NOT (r.state='settled' AND vm->>'status' IN ('waiting_creation_configuration','retiring_process_zero')
               AND public.vm_cancel_late_adopted_source(owner,generation,expected_vm,pvc,r.request_id)))
       OR r.reason IS DISTINCT FROM 'creation_adopted' OR r.resolved_at IS NULL
       OR r.ready_at IS NOT NULL OR r.claim_token IS NOT NULL OR r.claim_expires_at IS NOT NULL
       OR r.observed_vm_uid IS DISTINCT FROM expected_vm OR r.observed_pvc_uid IS DISTINCT FROM pvc
       OR r.controller_configuration->'version' IS DISTINCT FROM '3'::jsonb
       OR r.controller_configuration->'persistent_rootdisk' IS DISTINCT FROM 'true'::jsonb
       OR r.controller_configuration->'headscale_enabled' IS DISTINCT FROM 'false'::jsonb
       OR v.id IS NULL OR v.resource_version IS DISTINCT FROM 2
       OR v.state NOT IN ('reserved','active','warm','teardown')
       OR v.vm_uid IS DISTINCT FROM expected_vm OR v.vmi_uid IS NULL OR v.launcher_uid IS NULL
       OR vm->>'vmi_uid' IS DISTINCT FROM v.vmi_uid::text
       OR (vm->>'active_pod_uid' IS NOT NULL AND vm->>'active_pod_uid'<>v.launcher_uid::text)
       OR r.controller_configuration->'resource_admission'->>'cluster_id' IS DISTINCT FROM v.cluster_id
       OR COALESCE(r.controller_configuration->>'namespace','') !~ '^[a-z0-9]([-a-z0-9]*[a-z0-9])?$'
       OR length(r.controller_configuration->>'namespace')>63
       OR EXISTS (SELECT 1 FROM public.srw_execution_specs e
           JOIN public.srw_execution_workspace_bindings b ON b.execution_id=e.id
           WHERE e.work_kind='Job' AND e.work_id=owner)
       OR EXISTS (SELECT 1 FROM public.srw_workspace_instances w WHERE w.pvc_uid=pvc::text)
       OR EXISTS (SELECT 1 FROM public.vm_resource_recovery_successors s WHERE s.reservation_id=v.id)
       OR EXISTS (SELECT 1 FROM public.vm_creation_retries later WHERE later.owner_kind='job'
           AND later.job_id=owner AND (later.created_at,later.request_id)>(r.created_at,r.request_id))
       OR EXISTS (SELECT 1 FROM public.vm_creation_effects e WHERE e.request_id=r.request_id AND e.state='issued')
       OR EXISTS (SELECT 1 FROM public.agents a WHERE a.current_job_id=owner AND a.status NOT IN ('offline','failed','completed'))
       OR EXISTS (SELECT 1 FROM public.jobs other WHERE other.id<>owner AND
           (other.context->'vm'->>'inherited_from_job_id'=owner::text
            OR other.context->'vm'->>'rootdisk_pvc_uid'=pvc::text))
       OR EXISTS (SELECT 1 FROM public.vm_idle_access_leases l WHERE l.owner_kind='job' AND l.owner_id=owner
           AND l.closed_at IS NULL AND l.expires_at>clock_timestamp())
       OR EXISTS (SELECT 1 FROM public.vm_remote_operation_leases l WHERE l.owner_kind='job' AND l.owner_id=owner
           AND l.settled_at IS NULL AND l.lease_expires_at>clock_timestamp())
       OR EXISTS (SELECT 1 FROM public.vm_idle_operations o WHERE o.owner_kind='job' AND o.owner_id=owner AND o.closed_at IS NULL)
       OR EXISTS (SELECT 1 FROM public.vm_workspace_recoveries h WHERE h.resolved_at IS NULL
           AND ((h.owner_kind='job' AND h.owner_id=owner) OR h.root_pvc_uid=pvc))
       OR EXISTS (SELECT 1 FROM public.vm_workspace_recovery_retention_pins p WHERE p.pvc_uid=pvc AND p.released_at IS NULL)
       OR EXISTS (SELECT 1 FROM public.vm_workspace_recovery_jobs h WHERE h.job_id=owner AND h.resolved_at IS NULL)
       OR EXISTS (SELECT 1 FROM public.vm_workspace_cleanup_admissions c
           WHERE ((c.owner_kind='job' AND c.owner_id=owner) OR c.pvc_uid=pvc)
           AND c.completed_at IS NULL AND c.id IS DISTINCT FROM old_parent
           AND c.id IS DISTINCT FROM retaining_parent)
       OR EXISTS (SELECT 1 FROM public.vm_workspace_cleanup_admissions c WHERE
           c.parent_admission_id=old_parent OR c.parent_admission_id=retaining_parent)
       OR EXISTS (SELECT 1 FROM public.vm_pre_ssh_stop_intents i WHERE i.job_id=owner
           AND i.provision_generation=generation
           AND (NOT existing_stop OR i.cleanup_admission_id IS DISTINCT FROM retaining_parent))
       OR (NOT existing_stop AND EXISTS (SELECT 1 FROM public.managed_repository_process_zero_receipts z
           WHERE z.owner_kind='job' AND z.owner_id=owner AND z.scope='vm'
           AND z.provisioner='vm' AND z.runtime_incarnation=generation::text))
       OR (j.context ? '_job_terminal_vm_cleanup' AND (
           j.context->'_job_terminal_vm_cleanup'->>'version'='1'
           AND j.context->'_job_terminal_vm_cleanup'->>'provision_generation'=generation::text
           AND j.context->'_job_terminal_vm_cleanup'->>'admission_id'
               IN (old_parent::text,retaining_parent::text)) IS DISTINCT FROM true) THEN
        RETURN NULL;
    END IF;
    RETURN jsonb_build_object('creation_request_id',r.request_id,'reservation_id',v.id,
        'reservation_revision',v.revision,'vmi_uid',v.vmi_uid,'launcher_uid',v.launcher_uid,
        'node_uid',v.node_uid,'namespace',r.controller_configuration->>'namespace','cluster_id',v.cluster_id);
END;
$body$;

CREATE OR REPLACE FUNCTION public.guard_vm_pre_ssh_stop_intent() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    admission public.vm_workspace_cleanup_admissions%ROWTYPE;
    retry public.vm_creation_retries%ROWTYPE;
    reservation public.vm_resource_reservations%ROWTYPE;
    owner_vm jsonb;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'VM pre-SSH stop intent is append-only' USING ERRCODE='23514';
    END IF;
    IF NEW.frozen->>'kind' IN ('vm_initial_ready_positive_stop_candidate_v1','vm_job_never_app_ready_retained_stop_candidate_v1') THEN
    IF NEW.frozen->>'kind'='vm_initial_ready_positive_stop_candidate_v1' THEN
        NEW.initial_ready_xact_id := pg_current_xact_id();
    ELSE
        NEW.held_stop_xact_id := pg_current_xact_id();
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended('workspace-recovery:job:'||NEW.job_id,0));
    PERFORM pg_advisory_xact_lock(hashtextextended('workspace-recovery-pvc:'||(SELECT pvc_uid::text FROM public.vm_workspace_cleanup_admissions WHERE id=NEW.cleanup_admission_id),0));
    PERFORM 1 FROM public.run_queue WHERE unit_id=NEW.job_id FOR UPDATE;
    END IF;
    SELECT context->'vm' INTO owner_vm FROM public.jobs
     WHERE id=NEW.job_id FOR UPDATE;
    SELECT * INTO admission FROM public.vm_workspace_cleanup_admissions
     WHERE id=NEW.cleanup_admission_id FOR UPDATE;
    SELECT * INTO retry FROM public.vm_creation_retries
     WHERE request_id=NEW.creation_request_id FOR UPDATE;
    SELECT * INTO reservation FROM public.vm_resource_reservations
     WHERE id=NEW.reservation_id FOR UPDATE;
    IF admission.id IS NULL OR admission.owner_kind IS DISTINCT FROM 'job'
       OR admission.owner_id IS DISTINCT FROM NEW.job_id
       OR admission.completed_at IS NOT NULL
       OR admission.pvc_uid IS DISTINCT FROM NEW.pvc_uid
       OR admission.intent_digest IS DISTINCT FROM NEW.cleanup_intent_digest
       OR retry.request_id IS NULL OR retry.job_id IS DISTINCT FROM NEW.job_id
       OR retry.provision_generation IS DISTINCT FROM NEW.provision_generation
       OR (retry.state IS DISTINCT FROM 'succeeded'
           AND NOT (retry.state='settled'
               AND NEW.frozen->>'kind'='vm_job_never_app_ready_retained_stop_candidate_v1'
               AND public.vm_cancel_late_adopted_source(
                   NEW.job_id,NEW.provision_generation,
                   NEW.vm_uid,NEW.pvc_uid,
                   NEW.creation_request_id)))
       OR retry.observed_vm_uid IS DISTINCT FROM NEW.vm_uid
       OR retry.observed_pvc_uid IS DISTINCT FROM NEW.pvc_uid
       OR (CASE WHEN NEW.frozen->>'kind'='vm_initial_ready_positive_stop_candidate_v1'
           THEN retry.ready_at IS NULL OR NOT public.vm_initial_ready_stop_authorized(NEW)
           WHEN NEW.frozen->>'kind'='vm_job_never_app_ready_retained_stop_candidate_v1'
           THEN retry.ready_at IS NOT NULL OR NOT public.vm_never_app_ready_stop_authorized(NEW)
           ELSE retry.ready_at IS NOT NULL OR NEW.frozen->>'kind' IS DISTINCT FROM 'vm_pre_ssh_stop_candidate_v1' END)
       OR reservation.id IS NULL OR reservation.request_id IS DISTINCT FROM retry.request_id
       OR reservation.revision IS DISTINCT FROM NEW.reservation_revision
       OR reservation.state IS DISTINCT FROM 'teardown'
       OR reservation.vm_uid IS DISTINCT FROM NEW.vm_uid
       OR reservation.vmi_uid IS DISTINCT FROM NEW.vmi_uid
       OR reservation.launcher_uid IS DISTINCT FROM NEW.launcher_uid
       OR reservation.node_uid IS DISTINCT FROM NEW.node_uid
       OR owner_vm->>'provision_generation' IS DISTINCT FROM NEW.provision_generation::text
       OR owner_vm->>'vm_uid' IS DISTINCT FROM NEW.vm_uid::text
       OR owner_vm->>'rootdisk_pvc_uid' IS DISTINCT FROM NEW.pvc_uid::text
       OR owner_vm->>'status' IS DISTINCT FROM 'retiring_process_zero'
       OR (NEW.frozen->>'kind'<>'vm_job_never_app_ready_retained_stop_candidate_v1'
           AND NEW.prior_zero_receipt_id IS NOT NULL)
       OR (NEW.frozen->>'kind'<>'vm_job_never_app_ready_retained_stop_candidate_v1' AND EXISTS (
           SELECT 1 FROM public.managed_repository_process_zero_receipts z
           WHERE z.owner_kind='job' AND z.owner_id=NEW.job_id
             AND z.scope='vm' AND z.provisioner='vm'
             AND z.runtime_incarnation=NEW.provision_generation::text))
       OR EXISTS (SELECT 1 FROM public.vm_resource_recovery_successors s
                   WHERE s.reservation_id=reservation.id)
       OR NEW.frozen->>'job_id' IS DISTINCT FROM NEW.job_id::text
       OR NEW.frozen->>'provision_generation' IS DISTINCT FROM NEW.provision_generation::text
       OR NEW.frozen->>'vm_uid' IS DISTINCT FROM NEW.vm_uid::text
       OR NEW.frozen->>'vmi_uid' IS DISTINCT FROM NEW.vmi_uid::text
       OR NEW.frozen->>'launcher_uid' IS DISTINCT FROM NEW.launcher_uid::text
       OR NEW.frozen->>'pvc_uid' IS DISTINCT FROM NEW.pvc_uid::text
       OR NEW.frozen->>'node_uid' IS DISTINCT FROM NEW.node_uid::text
       OR jsonb_typeof(NEW.frozen->'vm_generation') IS DISTINCT FROM 'number'
       OR NEW.frozen->>'vm_generation' !~ '^[1-9][0-9]*$'
       OR NEW.frozen_digest IS DISTINCT FROM
          'sha256:'||encode(sha256(convert_to(NEW.frozen::text,'UTF8')),'hex') THEN
        RAISE EXCEPTION 'VM pre-SSH stop intent lacks exact retirement authority'
            USING ERRCODE='23514', CONSTRAINT='vm_pre_ssh_stop_intent_authority';
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION public.guard_vm_pre_ssh_stop_proof() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    intent public.vm_pre_ssh_stop_intents%ROWTYPE;
    admission public.vm_workspace_cleanup_admissions%ROWTYPE;
    retry public.vm_creation_retries%ROWTYPE;
    reservation public.vm_resource_reservations%ROWTYPE;
    owner_vm jsonb;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'VM pre-SSH stop proof is append-only' USING ERRCODE='23514';
    END IF;
    IF EXISTS(SELECT 1 FROM public.vm_pre_ssh_stop_intents i WHERE i.cleanup_admission_id=NEW.cleanup_admission_id AND i.frozen->>'kind' IN ('vm_initial_ready_positive_stop_candidate_v1','vm_job_never_app_ready_retained_stop_candidate_v1')) THEN
    PERFORM pg_advisory_xact_lock(hashtextextended('workspace-recovery:job:'||NEW.job_id,0));
    PERFORM pg_advisory_xact_lock(hashtextextended('workspace-recovery-pvc:'||(SELECT pvc_uid::text FROM public.vm_workspace_cleanup_admissions WHERE id=NEW.cleanup_admission_id),0));
    PERFORM 1 FROM public.run_queue WHERE unit_id=NEW.job_id FOR UPDATE;
    END IF;
    SELECT context->'vm' INTO owner_vm FROM public.jobs
     WHERE id=NEW.job_id FOR UPDATE;
    SELECT * INTO admission FROM public.vm_workspace_cleanup_admissions
     WHERE id=NEW.cleanup_admission_id FOR UPDATE;
    -- The intent is immutable. Read it before taking the same retry/charge
    -- lock order as the application store; no intent row lock is needed.
    SELECT * INTO intent FROM public.vm_pre_ssh_stop_intents
     WHERE cleanup_admission_id=NEW.cleanup_admission_id;
    SELECT * INTO retry FROM public.vm_creation_retries
     WHERE request_id=intent.creation_request_id FOR UPDATE;
    SELECT * INTO reservation FROM public.vm_resource_reservations
     WHERE id=intent.reservation_id FOR UPDATE;
    IF intent.frozen->>'kind'='vm_initial_ready_positive_stop_candidate_v1' THEN
        IF intent.initial_ready_xact_id IS NULL OR intent.initial_ready_xact_id=pg_current_xact_id() THEN
            RAISE EXCEPTION 'Initial Ready proof requires committed stop intent' USING ERRCODE='23514';
        END IF;
        NEW.initial_ready_xact_id := pg_current_xact_id();
    ELSIF intent.frozen->>'kind'='vm_job_never_app_ready_retained_stop_candidate_v1' THEN
        IF intent.held_stop_xact_id IS NULL OR intent.held_stop_xact_id=pg_current_xact_id() THEN
            RAISE EXCEPTION 'Held stop proof requires committed stop intent' USING ERRCODE='23514';
        END IF;
        NEW.held_stop_xact_id := pg_current_xact_id();
    END IF;
    IF intent.cleanup_admission_id IS NULL
       OR intent.job_id IS DISTINCT FROM NEW.job_id
       OR intent.provision_generation IS DISTINCT FROM NEW.provision_generation
       OR intent.frozen_digest IS DISTINCT FROM NEW.frozen_digest
       OR admission.id IS NULL OR admission.completed_at IS NOT NULL
       OR admission.owner_kind IS DISTINCT FROM 'job'
       OR admission.owner_id IS DISTINCT FROM intent.job_id
       OR admission.pvc_uid IS DISTINCT FROM intent.pvc_uid
       OR admission.intent_digest IS DISTINCT FROM intent.cleanup_intent_digest
       OR retry.request_id IS NULL
       OR retry.job_id IS DISTINCT FROM intent.job_id
       OR retry.provision_generation IS DISTINCT FROM intent.provision_generation
       OR (retry.state IS DISTINCT FROM 'succeeded'
           AND NOT (retry.state='settled'
               AND intent.frozen->>'kind'='vm_job_never_app_ready_retained_stop_candidate_v1'
               AND public.vm_cancel_late_adopted_source(
                   intent.job_id,intent.provision_generation,
                   intent.vm_uid,intent.pvc_uid,
                   intent.creation_request_id)))
       OR (CASE WHEN intent.frozen->>'kind'='vm_initial_ready_positive_stop_candidate_v1'
           THEN retry.ready_at IS NULL OR NOT public.vm_initial_ready_stop_authorized(intent)
               OR NOT public.valid_vm_initial_ready_positive_stop(intent.frozen,NEW.terminal_evidence,NEW.frozen_digest)
           WHEN intent.frozen->>'kind'='vm_job_never_app_ready_retained_stop_candidate_v1'
           THEN retry.ready_at IS NOT NULL OR NOT public.vm_never_app_ready_stop_authorized(intent)
               OR NOT public.valid_vm_initial_ready_positive_stop(intent.frozen,
                   jsonb_set(NEW.terminal_evidence,'{kind}','"vm_initial_ready_positive_stop_v1"'::jsonb),NEW.frozen_digest)
           ELSE retry.ready_at IS NOT NULL OR intent.frozen->>'kind' IS DISTINCT FROM 'vm_pre_ssh_stop_candidate_v1' END)
       OR retry.observed_vm_uid IS DISTINCT FROM intent.vm_uid
       OR retry.observed_pvc_uid IS DISTINCT FROM intent.pvc_uid
       OR reservation.id IS NULL OR reservation.state IS DISTINCT FROM 'teardown'
       OR reservation.request_id IS DISTINCT FROM intent.creation_request_id
       OR reservation.revision IS DISTINCT FROM intent.reservation_revision
       OR reservation.vm_uid IS DISTINCT FROM intent.vm_uid
       OR reservation.vmi_uid IS DISTINCT FROM intent.vmi_uid
       OR reservation.launcher_uid IS DISTINCT FROM intent.launcher_uid
       OR reservation.node_uid IS DISTINCT FROM intent.node_uid
       OR owner_vm->>'status' IS DISTINCT FROM 'retiring_process_zero'
       OR owner_vm->>'provision_generation' IS DISTINCT FROM NEW.provision_generation::text
       OR NEW.terminal_evidence->>'kind' IS DISTINCT FROM (CASE WHEN intent.frozen->>'kind'='vm_initial_ready_positive_stop_candidate_v1'
           THEN 'vm_initial_ready_positive_stop_v1'
           WHEN intent.frozen->>'kind'='vm_job_never_app_ready_retained_stop_candidate_v1'
           THEN 'vm_job_never_app_ready_retained_positive_stop_v1'
           ELSE 'vm_pre_ssh_positive_stop_v1' END)
       OR NEW.terminal_evidence->>'frozen_digest' IS DISTINCT FROM NEW.frozen_digest
       OR NEW.terminal_evidence->>'vm_run_strategy' IS DISTINCT FROM 'Halted'
       OR NEW.terminal_evidence->>'vm_generation' IS DISTINCT FROM
          ((intent.frozen->>'vm_generation')::bigint+1)::text
       OR NEW.terminal_evidence->>'node_ready' IS DISTINCT FROM 'true'
       OR NEW.terminal_evidence->>'same_generation_replacement' IS DISTINCT FROM 'false'
       OR NEW.terminal_evidence->>'pod_finalizer' IS DISTINCT FROM 'srw.io/vm-pre-ssh-positive-stop'
       OR NEW.terminal_evidence->>'pod_intent_digest' IS DISTINCT FROM NEW.frozen_digest
       OR NEW.terminal_evidence->>'vm_uid' IS DISTINCT FROM intent.vm_uid::text
       OR NEW.terminal_evidence->>'launcher_uid' IS DISTINCT FROM intent.launcher_uid::text
       OR NEW.terminal_evidence->>'node_uid' IS DISTINCT FROM intent.node_uid::text
       OR NEW.terminal_evidence->>'controller_authenticated' IS DISTINCT FROM 'true'
       OR NEW.evidence_digest IS DISTINCT FROM
          'sha256:'||encode(sha256(convert_to(NEW.terminal_evidence::text,'UTF8')),'hex') THEN
        RAISE EXCEPTION 'VM pre-SSH stop proof lacks exact current authority'
            USING ERRCODE='23514', CONSTRAINT='vm_pre_ssh_stop_proof_authority';
    END IF;
    RETURN NEW;
END;
$$;

-- Keep the receipt guard exact through the physical stop and retained disk proof.
CREATE OR REPLACE FUNCTION public.guard_vm_resource_cleanup_stop_receipt() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    cleanup RECORD;
    charge RECORD;
    successor RECORD;
    source RECORD;
    owner_vm JSONB;
    proof JSONB;
BEGIN
    IF TG_OP<>'INSERT' THEN
        RAISE EXCEPTION 'VM resource cleanup stop receipt is append-only' USING ERRCODE='23514';
    END IF;
    SELECT * INTO cleanup FROM public.vm_workspace_cleanup_admissions
     WHERE id=NEW.cleanup_admission_id FOR UPDATE;
    SELECT * INTO charge FROM public.vm_resource_reservations
     WHERE id=NEW.reservation_id FOR UPDATE;
    SELECT w.job_id,w.provision_generation,r.state AS retry_state,
           r.observed_vm_uid,r.observed_pvc_uid,r.ready_at,r.reason
      INTO source FROM public.vm_resource_waiters w
      JOIN public.vm_creation_retries r ON r.request_id=w.request_id
     WHERE w.request_id=NEW.request_id;
    SELECT successor_vmi_uid,successor_launcher_uid INTO successor
      FROM public.vm_resource_recovery_successors
     WHERE reservation_id=NEW.reservation_id ORDER BY ordinal DESC LIMIT 1;
    SELECT context->'vm' INTO owner_vm FROM public.jobs WHERE id=NEW.job_id;
    proof := NEW.stop_evidence;
    IF cleanup.id IS NULL OR cleanup.owner_kind<>'job'
       OR cleanup.owner_id<>NEW.job_id OR cleanup.pvc_uid<>NEW.pvc_uid
       OR cleanup.intent_digest<>NEW.intent_digest
       OR cleanup.source='vm_idle_release'
       OR cleanup.completed_at IS NULL OR cleanup.outcome<>'completed'
       OR charge.id IS NULL OR charge.request_id<>NEW.request_id
       OR charge.resource_version<>2 OR charge.state<>'teardown'
       OR charge.vm_uid IS DISTINCT FROM NEW.vm_uid
       OR COALESCE(successor.successor_vmi_uid,charge.vmi_uid) IS DISTINCT FROM NEW.vmi_uid
       OR COALESCE(successor.successor_launcher_uid,charge.launcher_uid) IS DISTINCT FROM NEW.launcher_uid
       OR source.job_id IS DISTINCT FROM NEW.job_id
       OR source.provision_generation IS DISTINCT FROM NEW.provision_generation
       OR (source.retry_state<>'succeeded' AND NOT (source.retry_state='settled'
           AND cleanup.source='job_terminal_vm_release'
           AND public.vm_cancel_late_adopted_source(NEW.job_id,NEW.provision_generation,
               NEW.vm_uid,NEW.pvc_uid,NEW.request_id)))
       OR source.observed_vm_uid IS DISTINCT FROM NEW.vm_uid
       OR source.observed_pvc_uid IS DISTINCT FROM NEW.pvc_uid
       OR owner_vm->>'provision_generation' IS DISTINCT FROM NEW.provision_generation::text
       OR owner_vm->>'vm_uid' IS DISTINCT FROM NEW.vm_uid::text
       OR owner_vm->>'rootdisk_pvc_uid' IS DISTINCT FROM NEW.pvc_uid::text
       OR owner_vm->>'vmi_uid' IS DISTINCT FROM NEW.vmi_uid::text
       OR (
           owner_vm->>'active_pod_uid' IS DISTINCT FROM NEW.launcher_uid::text
           AND NOT (
               owner_vm->>'active_pod_uid' IS NULL
               AND source.ready_at IS NULL
               AND source.reason IS NOT DISTINCT FROM 'creation_adopted'
               AND NOT EXISTS (
                   SELECT 1 FROM public.vm_resource_recovery_successors
                    WHERE reservation_id=NEW.reservation_id
               )
           )
       )
       OR proof->>'version' IS DISTINCT FROM '1'
       OR proof->>'kind' IS DISTINCT FROM 'vm_cleanup_physical_stop'
       OR proof->>'job_id' IS DISTINCT FROM NEW.job_id::text
       OR proof->>'provision_generation' IS DISTINCT FROM NEW.provision_generation::text
       OR proof->>'vm_uid' IS DISTINCT FROM NEW.vm_uid::text
       OR proof->>'vmi_uid' IS DISTINCT FROM NEW.vmi_uid::text
       OR proof->>'launcher_uid' IS DISTINCT FROM NEW.launcher_uid::text
       OR proof->>'pvc_uid' IS DISTINCT FROM NEW.pvc_uid::text
       OR proof->>'vm_absent' IS DISTINCT FROM 'true'
       OR proof->>'vmi_absent' IS DISTINCT FROM 'true'
       OR proof->>'launcher_absent' IS DISTINCT FROM 'true'
       OR proof->>'controller_authenticated' IS DISTINCT FROM 'true'
       OR proof->>'same_generation_replacement' IS DISTINCT FROM 'false'
       OR proof->>'pvc_disposition' IS NULL
       OR proof->>'pvc_disposition' NOT IN ('retained','purged')
       OR NOT EXISTS (SELECT 1 FROM public.managed_repository_process_zero_receipts p
                      WHERE p.owner_kind='job' AND p.owner_id=NEW.job_id
                        AND p.scope='vm' AND p.provisioner='vm'
                        AND p.runtime_incarnation=NEW.provision_generation::text) THEN
        RAISE EXCEPTION 'VM resource cleanup stop proof changed' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;

COMMIT;
