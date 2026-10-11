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
    owner uuid, generation uuid, expected_vm uuid, pvc uuid, expected_request uuid,
    retaining_parent uuid DEFAULT NULL, existing_stop boolean DEFAULT false,
    receipt_completion boolean DEFAULT false
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
          AND (j.context->'vm'->>'status' IN ('waiting_creation_configuration','retiring_process_zero')
            OR (j.context->'vm'->>'status'='deleted'
              AND retaining_parent IS NOT NULL AND existing_stop IS TRUE
              AND EXISTS (
                SELECT 1 FROM public.vm_job_cancel_retention_authorities a
                JOIN public.vm_workspace_cleanup_admissions held
                  ON held.id=a.cleanup_admission_id
                JOIN public.vm_pre_ssh_stop_intents i
                  ON i.cleanup_admission_id=a.cleanup_admission_id
                JOIN public.vm_pre_ssh_stop_proofs p
                  ON p.cleanup_admission_id=i.cleanup_admission_id
                JOIN public.managed_repository_process_zero_receipts z
                  ON z.owner_kind='job' AND z.owner_id=a.job_id
                 AND z.scope='vm' AND z.provisioner='vm'
                 AND z.runtime_incarnation=a.provision_generation::text
                WHERE a.cleanup_admission_id=retaining_parent
                  AND a.policy_version=1 AND a.admitted_xact_id<>pg_current_xact_id()
                  AND a.job_id=owner AND a.provision_generation=generation
                  AND a.creation_request_id=expected_request
                  AND a.reservation_id=v.id AND a.reservation_revision=v.revision
                  AND a.vm_uid=expected_vm AND a.vmi_uid=v.vmi_uid
                  AND a.launcher_uid=v.launcher_uid AND a.node_uid=v.node_uid
                  AND a.pvc_uid=pvc
                  AND held.owner_kind='job' AND held.owner_id=owner
                  AND held.pvc_uid=pvc AND held.source='job_terminal_vm_release'
                  AND held.parent_admission_id IS NULL
                  AND held.request_id=a.cleanup_request_id
                  AND held.intent_digest=a.intent_digest
                  AND i.job_id=owner AND i.provision_generation=generation
                  AND i.creation_request_id=expected_request
                  AND i.reservation_id=v.id AND i.reservation_revision=v.revision
                  AND i.vm_uid=expected_vm AND i.vmi_uid=v.vmi_uid
                  AND i.launcher_uid=v.launcher_uid AND i.pvc_uid=pvc
                  AND i.node_uid=v.node_uid
                  AND i.frozen->>'kind'='vm_job_never_app_ready_retained_stop_candidate_v1'
                  AND i.held_stop_xact_id IS NOT NULL
                  AND i.held_stop_xact_id<>pg_current_xact_id()
                  AND p.job_id=owner AND p.provision_generation=generation
                  AND p.frozen_digest=i.frozen_digest
                  AND p.terminal_evidence->>'kind'='vm_job_never_app_ready_retained_positive_stop_v1'
                  AND p.held_stop_xact_id IS NOT NULL
                  AND p.held_stop_xact_id<>i.held_stop_xact_id
                  AND p.held_stop_xact_id<>pg_current_xact_id()
                  AND ((NOT receipt_completion AND
                    public.vm_job_deleted_retention_replay_allowed(
                      1,owner,generation,expected_vm,pvc,retaining_parent,
                      existing_stop,expected_request,v.id,v.revision,
                      v.vmi_uid,v.launcher_uid,v.node_uid))
                    OR (receipt_completion AND held.completed_at IS NOT NULL
                      AND held.outcome='completed'))
              )))
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
           AND NOT (r.state='settled'
               AND public.vm_cancel_late_adopted_source(owner,generation,expected_vm,pvc,
                   r.request_id,retaining_parent,existing_stop)))
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
               NEW.vm_uid,NEW.pvc_uid,NEW.request_id,NEW.cleanup_admission_id,true,true)))
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

-- A settled source may feed owner Resume or Delete only after its policy-1
-- authority has fully discharged VM compute and retained its exact disk.
-- Each caller separately checks the current owner and pending markers.
CREATE FUNCTION public.vm_cancel_late_adopted_retained_terminal(parent_id uuid)
RETURNS boolean LANGUAGE sql VOLATILE AS $body$
    SELECT EXISTS (
        SELECT 1 FROM public.vm_job_cancel_retention_authorities a
        JOIN public.vm_creation_retries r ON r.request_id=a.creation_request_id
        JOIN public.vm_workspace_cleanup_admissions c ON c.id=r.creation_admission_id
        JOIN public.vm_resource_reservations v ON v.id=a.reservation_id
        JOIN public.vm_pre_ssh_stop_intents i ON i.cleanup_admission_id=a.cleanup_admission_id
        JOIN public.vm_pre_ssh_stop_proofs p ON p.cleanup_admission_id=a.cleanup_admission_id
        WHERE a.cleanup_admission_id=parent_id AND a.policy_version=1
          AND a.job_retained_resume_id IS NULL
          AND r.owner_kind='job' AND r.job_id=a.job_id
          AND r.provision_generation=a.provision_generation
          AND r.state='settled' AND r.reason='creation_adopted'
          AND r.resolved_at IS NOT NULL AND r.boot_counted
          AND r.ready_at IS NULL AND r.claim_token IS NULL AND r.claim_expires_at IS NULL
          AND r.observed_vm_uid=a.vm_uid AND r.observed_pvc_uid=a.pvc_uid
          AND c.owner_kind='job' AND c.owner_id=a.job_id
          AND c.completed_at IS NOT NULL AND c.outcome='adopted'
          AND v.request_id=r.request_id AND v.state='released'
          AND v.revision=a.reservation_revision
          AND i.job_id=a.job_id AND i.provision_generation=a.provision_generation
          AND i.creation_request_id=r.request_id
          AND i.frozen->>'kind'='vm_job_never_app_ready_retained_stop_candidate_v1'
          AND p.job_id=a.job_id AND p.provision_generation=a.provision_generation
          AND p.frozen_digest=i.frozen_digest
          AND p.terminal_evidence->>'kind'='vm_job_never_app_ready_retained_positive_stop_v1'
          AND (SELECT count(*) FROM public.vm_creation_effects e
              WHERE e.request_id=r.request_id)=3
          AND (SELECT count(DISTINCT e.effect_kind) FROM public.vm_creation_effects e
              WHERE e.request_id=r.request_id AND e.effect_kind IN ('rootdisk','cloud_init','vm')
                AND e.state='observed' AND e.resolved_at IS NOT NULL
                AND e.evidence<>'{}'::jsonb)=3
          AND NOT EXISTS (SELECT 1 FROM public.vm_creation_effects e
              WHERE e.request_id=r.request_id AND e.state='issued')
          AND public.vm_job_cancel_retention_settled(parent_id)
    );
$body$;

-- 0340's worker-hold, owner/queue/PVC/predecessor and custody checks stay exact.
CREATE OR REPLACE FUNCTION public.guard_vm_job_retained_resume() RETURNS trigger
LANGUAGE plpgsql AS $body$
DECLARE j public.jobs%ROWTYPE;
        a public.vm_job_cancel_retention_authorities%ROWTYPE;
        root public.vm_job_cancel_retention_authorities%ROWTYPE;
        r public.vm_creation_retries%ROWTYPE;
        q public.run_queue%ROWTYPE;
        previous public.vm_job_retained_resumes%ROWTYPE;
        terminal public.vm_job_retained_resume_terminals%ROWTYPE;
BEGIN
    IF TG_OP<>'INSERT' THEN
        RAISE EXCEPTION 'Retained Job Resume is append-only' USING ERRCODE='23514';
    END IF;
    NEW.admitted_xact_id := pg_current_xact_id();
    PERFORM pg_advisory_xact_lock(hashtextextended('workspace-recovery:job:'||NEW.job_id,0));
    PERFORM pg_advisory_xact_lock(hashtextextended('workspace-recovery-pvc:'||NEW.pvc_uid,0));
    SELECT * INTO q FROM public.run_queue WHERE unit_id=NEW.job_id FOR UPDATE;
    SELECT * INTO j FROM public.jobs WHERE id=NEW.job_id FOR UPDATE;
    SELECT * INTO a FROM public.vm_job_cancel_retention_authorities WHERE cleanup_admission_id=NEW.physical_cleanup_admission_id;
    SELECT * INTO root FROM public.vm_job_cancel_retention_authorities WHERE cleanup_admission_id=NEW.root_retention_admission_id;
    SELECT * INTO r FROM public.vm_creation_retries WHERE request_id=a.creation_request_id FOR SHARE;
    IF j.id IS NULL OR j.user_id IS DISTINCT FROM NEW.requested_by
       OR j.status NOT IN ('cancelled','failed','paused') OR j.execution_lane IS DISTINCT FROM 'stateless'
       OR j.assigned_agent_id IS NOT NULL OR j.parent_job_id IS NOT NULL
       OR COALESCE(j.context->'inherits_parent_workspace','false'::jsonb)<>'false'::jsonb
       OR j.context ?| ARRAY['_stateless_cancel_cleanup_pending','_stateless_delete_pending','_completion_control_claim','_worker_execution_hold']
       OR a.cleanup_admission_id IS NULL OR a.job_id IS DISTINCT FROM NEW.job_id
       OR a.pvc_uid IS DISTINCT FROM NEW.pvc_uid OR root.job_id IS DISTINCT FROM NEW.job_id
       OR root.pvc_uid IS DISTINCT FROM NEW.pvc_uid
       OR public.vm_job_cancel_retention_discharged(root.cleanup_admission_id)
       OR NOT public.vm_job_cancel_retention_settled(a.cleanup_admission_id)
       OR NEW.predecessor_request_id IS DISTINCT FROM a.creation_request_id
       OR NEW.predecessor_generation IS DISTINCT FROM a.provision_generation
       OR NEW.source_revision IS DISTINCT FROM r.revision
       OR r.request_id IS NULL OR (r.state IS DISTINCT FROM 'succeeded'
           AND NOT (r.state='settled' AND a.policy_version=1
               AND public.vm_cancel_late_adopted_retained_terminal(a.cleanup_admission_id)))
       OR COALESCE(r.canonical_request->'workspace_storage','null'::jsonb)<>'null'::jsonb
       OR q.unit_id IS NULL OR q.unit_kind IS DISTINCT FROM 'worker_batch' OR q.state IS DISTINCT FROM 'done'
       OR q.leased_by IS NOT NULL OR q.leased_until IS NOT NULL
       OR NEW.retained_vm->>'status' IS DISTINCT FROM 'deleted'
       OR NEW.retained_vm->'identity_authenticated' IS DISTINCT FROM 'true'::jsonb
       OR NEW.retained_vm->>'provision_generation' IS DISTINCT FROM a.provision_generation::text
       OR NEW.retained_vm->>'identity_provision_generation' IS DISTINCT FROM a.provision_generation::text
       OR NEW.retained_vm->>'vm_uid' IS DISTINCT FROM a.vm_uid::text
       OR NEW.retained_vm->>'rootdisk_pvc_uid' IS DISTINCT FROM a.pvc_uid::text
       OR COALESCE(NEW.retained_vm->'workspace_storage','null'::jsonb)<>'null'::jsonb
       OR EXISTS (SELECT 1 FROM public.vm_creation_retries source WHERE source.request_id=NEW.request_id OR source.provision_generation=NEW.provision_generation)
       OR EXISTS (SELECT 1 FROM public.vm_workspace_cleanup_admissions c WHERE c.completed_at IS NULL
           AND ((c.owner_kind='job' AND c.owner_id=NEW.job_id) OR c.pvc_uid=NEW.pvc_uid))
       OR EXISTS (SELECT 1 FROM public.vm_workspace_recoveries recovery WHERE recovery.resolved_at IS NULL
           AND ((recovery.owner_kind='job' AND recovery.owner_id=NEW.job_id) OR recovery.root_pvc_uid=NEW.pvc_uid))
       OR EXISTS (SELECT 1 FROM public.vm_workspace_recovery_retention_pins pin WHERE pin.pvc_uid=NEW.pvc_uid AND pin.released_at IS NULL)
       OR EXISTS (SELECT 1 FROM public.vm_idle_access_leases lease WHERE lease.owner_kind='job' AND lease.owner_id=NEW.job_id
           AND lease.closed_at IS NULL AND lease.expires_at>clock_timestamp())
       OR EXISTS (SELECT 1 FROM public.vm_idle_operations idle WHERE idle.owner_kind='job' AND idle.owner_id=NEW.job_id AND idle.closed_at IS NULL)
       OR EXISTS (SELECT 1 FROM public.vm_resource_reservations charge JOIN public.vm_creation_retries source USING(request_id)
           WHERE source.owner_kind='job' AND source.job_id=NEW.job_id AND charge.state<>'released')
       OR EXISTS (SELECT 1 FROM public.vm_creation_effects effect JOIN public.vm_creation_retries source USING(request_id)
           WHERE source.owner_kind='job' AND source.job_id=NEW.job_id AND effect.state='issued') THEN
        RAISE EXCEPTION 'Retained Job Resume authority is unproven' USING ERRCODE='23514';
    END IF;
    IF NEW.predecessor_terminal_id IS NULL THEN
        IF NEW.physical_cleanup_admission_id IS DISTINCT FROM NEW.root_retention_admission_id
           OR NEW.retained_vm IS DISTINCT FROM j.context->'vm'
           OR j.context ? '_vm_job_retained_resume'
           OR EXISTS (SELECT 1 FROM public.vm_creation_retries later WHERE later.owner_kind='job' AND later.job_id=NEW.job_id
               AND (later.created_at,later.request_id)>(r.created_at,r.request_id)) THEN
            RAISE EXCEPTION 'Retained Job Resume predecessor changed' USING ERRCODE='23514';
        END IF;
    ELSE
        SELECT * INTO terminal FROM public.vm_job_retained_resume_terminals WHERE id=NEW.predecessor_terminal_id;
        SELECT * INTO previous FROM public.vm_job_retained_resumes WHERE id=terminal.resume_id;
        IF terminal.id IS NULL OR previous.job_id IS DISTINCT FROM NEW.job_id
           OR previous.root_retention_admission_id IS DISTINCT FROM NEW.root_retention_admission_id
           OR previous.pvc_uid IS DISTINCT FROM NEW.pvc_uid
           OR terminal.physical_cleanup_admission_id IS DISTINCT FROM NEW.physical_cleanup_admission_id
           OR j.context->>'_vm_job_retained_resume' IS DISTINCT FROM previous.id::text
           OR (terminal.terminal_kind<>'kept_compute' AND NEW.retained_vm IS DISTINCT FROM previous.retained_vm) THEN
            RAISE EXCEPTION 'Retained Job Resume terminal changed' USING ERRCODE='23514';
        END IF;
    END IF;
    RETURN NEW;
END;
$body$;

-- Preserve 0339's full purge chain; admit the one completed policy-1 source
-- at final and predecessor positions, never an unknown issued or open stop.
CREATE OR REPLACE FUNCTION public.validate_vm_job_retained_disk_purge(
    parent_id uuid, require_receipt boolean DEFAULT false
) RETURNS boolean LANGUAGE plpgsql AS $body$
DECLARE
    d public.vm_job_retained_disk_purge_authorities%ROWTYPE;
    parent public.vm_workspace_cleanup_admissions%ROWTYPE;
    owner_row public.jobs%ROWTYPE;
    retry public.vm_creation_retries%ROWTYPE;
    old_retry public.vm_creation_retries%ROWTYPE;
    old_parent public.vm_workspace_cleanup_admissions%ROWTYPE;
    old_stop public.vm_resource_cleanup_stop_receipts%ROWTYPE;
    charge public.vm_resource_reservations%ROWTYPE;
    final_charge public.vm_resource_reservations%ROWTYPE;
    link public.vm_job_retained_disk_purge_predecessors%ROWTYPE;
    receipt public.vm_job_retained_disk_purge_receipts%ROWTYPE;
    instance public.srw_workspace_instances%ROWTYPE;
    binding jsonb;
    expected_digest text;
    expected_request uuid;
    link_count bigint := 0;
    retry_count bigint := 0;
    previous_generation bigint := 0;
    previous_execution_id uuid;
    previous_execution_revision text;
    previous_execution_generation bigint;
    scope_namespace text;
    scope_cluster_id text;
    terminal_tail jsonb;
BEGIN
    -- The public path already holds owner/PVC advisory locks. These row locks
    -- also make a direct/generic completion trigger re-read the actual current
    -- owner, parent and Released workspace rather than trusting a prior API
    -- response or a stale candidate projection.
    SELECT * INTO d FROM public.vm_job_retained_disk_purge_authorities
     WHERE cleanup_admission_id=parent_id FOR SHARE;
    SELECT * INTO owner_row FROM public.jobs WHERE id=d.job_id FOR UPDATE;
    PERFORM 1 FROM public.vm_job_creation_owners
     WHERE job_id=d.job_id FOR SHARE;
    SELECT * INTO parent FROM public.vm_workspace_cleanup_admissions
     WHERE id=parent_id FOR SHARE;
    SELECT * INTO retry FROM public.vm_creation_retries
     WHERE request_id=d.final_request_id FOR SHARE;
    SELECT * INTO final_charge FROM public.vm_resource_reservations
     WHERE request_id=d.final_request_id FOR SHARE;
    terminal_tail := public.vm_job_retained_purge_tail(parent_id);
    scope_namespace := retry.controller_configuration->>'namespace';
    scope_cluster_id := final_charge.cluster_id;
    expected_request := public.uuid_generate_v5(public.uuid_ns_url(),
        'vm-workspace-cleanup:public_vm_delete:job:'||d.job_id::text||':'||
        d.provision_generation::text||':'||d.vm_uid::text||':'||d.pvc_uid::text);
    expected_digest := 'sha256:'||encode(sha256(convert_to(format(
        '{"owner_id":"%s","owner_kind":"job","provision_generation":"%s",' ||
        '"purge_disk":true,"pvc_uid":"%s","resource":"vm_workspace",' ||
        '"source":"public_vm_delete","vm_uid":"%s"}',
        d.job_id,d.provision_generation,d.pvc_uid,d.vm_uid),'UTF8')),'hex');
    IF d.cleanup_admission_id IS NULL OR parent.id IS NULL
       OR parent.owner_kind IS DISTINCT FROM 'job'
       OR parent.owner_id IS DISTINCT FROM d.job_id
       OR parent.source IS DISTINCT FROM 'public_vm_delete'
       OR parent.parent_admission_id IS NOT NULL
       OR parent.pvc_uid IS DISTINCT FROM d.pvc_uid
       OR parent.request_id IS DISTINCT FROM expected_request
       OR d.cleanup_request_id IS DISTINCT FROM expected_request
       OR parent.intent_digest IS DISTINCT FROM expected_digest
       OR d.intent_digest IS DISTINCT FROM expected_digest
       OR (parent.completed_at IS NOT NULL AND parent.outcome IS DISTINCT FROM 'completed')
       OR owner_row.id IS NULL OR owner_row.parent_job_id IS NOT NULL
       OR owner_row.status NOT IN ('completed','failed','cancelled')
       OR NOT EXISTS (SELECT 1 FROM public.vm_job_creation_owners o
           WHERE o.job_id=d.job_id AND o.live_job_id=d.job_id AND o.deleted_at IS NULL)
       OR retry.request_id IS NULL OR retry.owner_kind IS DISTINCT FROM 'job'
       OR retry.job_id IS DISTINCT FROM d.job_id
       OR retry.provision_generation IS DISTINCT FROM d.provision_generation
       OR (retry.state IS DISTINCT FROM 'succeeded' AND NOT (retry.state='settled'
           AND EXISTS (SELECT 1 FROM public.vm_job_cancel_retention_authorities a
               WHERE a.creation_request_id=retry.request_id AND a.job_id=d.job_id
                 AND a.provision_generation=d.provision_generation
                 AND a.vm_uid=d.vm_uid AND a.pvc_uid=d.pvc_uid
                 AND public.vm_cancel_late_adopted_retained_terminal(a.cleanup_admission_id))))
       OR retry.resolved_at IS NULL
       OR retry.observed_vm_uid IS DISTINCT FROM d.vm_uid
       OR retry.observed_pvc_uid IS DISTINCT FROM d.pvc_uid
       OR retry.creation_admission_id IS NULL
       OR final_charge.id IS NULL OR final_charge.state IS DISTINCT FROM 'released'
       OR scope_namespace IS NULL OR length(scope_namespace) NOT BETWEEN 1 AND 63
       OR scope_namespace !~ '^[a-z0-9]([-a-z0-9]*[a-z0-9])?$'
       OR scope_cluster_id IS NULL OR length(scope_cluster_id)=0
       OR retry.controller_configuration->'resource_admission'->>'cluster_id'
           IS DISTINCT FROM scope_cluster_id
       OR (d.retained_terminal_id IS NULL AND EXISTS(SELECT 1 FROM public.vm_job_retained_resumes typed WHERE typed.job_id=d.job_id))
       OR (d.retained_terminal_id IS NOT NULL AND terminal_tail IS NULL)
       OR (d.retained_terminal_id IS NULL AND (false
       OR owner_row.context->'vm'->>'provision_generation' IS DISTINCT FROM d.provision_generation::text
       OR owner_row.context->'vm'->>'vm_uid' IS DISTINCT FROM d.vm_uid::text
       OR owner_row.context->'vm'->>'rootdisk_pvc_uid' IS DISTINCT FROM d.pvc_uid::text
       OR EXISTS (SELECT 1 FROM public.vm_creation_retries later
           WHERE later.owner_kind='job' AND later.job_id=d.job_id
             AND (later.created_at,later.request_id)>(retry.created_at,retry.request_id))
       ))
       OR EXISTS (SELECT 1 FROM public.vm_resource_reservations held
           JOIN public.vm_creation_retries source USING (request_id)
           WHERE source.owner_kind='job' AND source.job_id=d.job_id
             AND held.state<>'released')
       OR EXISTS (SELECT 1 FROM public.vm_creation_effects effect
           JOIN public.vm_creation_retries source USING (request_id)
           WHERE source.owner_kind='job' AND source.job_id=d.job_id
             AND effect.state='issued')
       OR EXISTS (SELECT 1 FROM public.vm_resource_waiters waiter
           WHERE waiter.owner_kind='job' AND waiter.job_id=d.job_id
             AND waiter.state NOT IN ('released','cancelled'))
       OR EXISTS (SELECT 1 FROM public.vm_workspace_recoveries recovery
           WHERE recovery.resolved_at IS NULL AND
             ((recovery.owner_kind='job' AND recovery.owner_id=d.job_id)
              OR recovery.root_pvc_uid=d.pvc_uid))
       OR EXISTS (SELECT 1 FROM public.vm_workspace_recovery_retention_pins pin
           WHERE pin.pvc_uid=d.pvc_uid AND pin.released_at IS NULL)
       OR EXISTS (SELECT 1 FROM public.vm_idle_access_leases lease
           WHERE lease.owner_kind='job' AND lease.owner_id=d.job_id
             AND lease.closed_at IS NULL AND lease.expires_at>clock_timestamp())
       OR EXISTS (SELECT 1 FROM public.vm_workspace_cleanup_admissions other
           WHERE other.owner_kind='job' AND other.owner_id=d.job_id
             AND other.completed_at IS NULL AND other.id<>parent_id
             AND other.source<>'terminal_checkpoint_prune'
             AND NOT (other.source='controller_rootdisk_delete'
               AND other.parent_admission_id=parent_id
               AND other.pvc_uid=d.pvc_uid
               AND NOT require_receipt
               AND other.request_id IS NOT NULL
               AND other.intent_digest ~ '^sha256:[0-9a-f]{64}$')) THEN
        RAISE EXCEPTION 'Job retained disk purge current authority changed'
            USING ERRCODE='23514',CONSTRAINT='vm_job_retained_purge_authority';
    END IF;
    IF d.binding_kind='bound' THEN
        binding := retry.canonical_request->'workspace_storage';
        IF jsonb_typeof(binding) IS DISTINCT FROM 'object'
           OR jsonb_typeof(binding->'generation') IS DISTINCT FROM 'number'
           OR (binding->>'generation') !~ '^[1-9][0-9]*$' THEN
            RAISE EXCEPTION 'Job retained disk workspace generation unproven'
                USING ERRCODE='23514',CONSTRAINT='vm_job_retained_purge_binding';
        END IF;
        SELECT * INTO instance FROM public.srw_workspace_instances
         WHERE id=d.workspace_instance_id FOR UPDATE;
        IF jsonb_typeof(binding) IS DISTINCT FROM 'object'
           OR binding->>'uid' IS DISTINCT FROM d.workspace_instance_id::text
           OR binding->>'generation' IS DISTINCT FROM d.workspace_generation::text
           OR (d.workspace_generation>1 AND
               binding->>'pvc_uid' IS DISTINCT FROM d.pvc_uid::text)
           OR (binding->>'pvc_uid' IS NOT NULL AND
               binding->>'pvc_uid' IS DISTINCT FROM d.pvc_uid::text)
           OR binding->>'owner_kind' IS DISTINCT FROM 'job'
           OR binding->>'owner_id' IS DISTINCT FROM d.job_id::text
           OR instance.id IS NULL OR instance.generation IS DISTINCT FROM d.workspace_generation
           OR instance.pvc_uid IS DISTINCT FROM d.pvc_uid::text
           OR instance.status IS DISTINCT FROM 'Released'
           OR instance.execution_id IS NOT NULL OR instance.pod_uid IS NOT NULL
           OR NOT EXISTS (SELECT 1 FROM public.srw_execution_workspace_bindings b
                WHERE b.execution_id=retry.execution_id
                  AND b.instance_id=d.workspace_instance_id)
           OR EXISTS (SELECT 1 FROM public.srw_workspace_instances other
                WHERE other.id<>d.workspace_instance_id AND other.pvc_uid=d.pvc_uid::text)
           OR EXISTS (SELECT 1 FROM public.srw_workspace_instances other
                WHERE other.id=d.workspace_instance_id AND other.status<>'Released') THEN
            RAISE EXCEPTION 'Job retained disk workspace release unproven'
                USING ERRCODE='23514',CONSTRAINT='vm_job_retained_purge_binding';
        END IF;
    ELSE
        IF d.binding_kind IS DISTINCT FROM 'unbound'
           OR d.workspace_instance_id IS NOT NULL OR d.workspace_generation IS NOT NULL
           OR COALESCE(retry.canonical_request->'workspace_storage','null'::jsonb)<>'null'::jsonb
           OR EXISTS (SELECT 1 FROM public.srw_workspace_instances foreign_workspace
                WHERE foreign_workspace.pvc_uid=d.pvc_uid::text)
           OR EXISTS (SELECT 1 FROM public.srw_execution_workspace_bindings b
                WHERE b.execution_id=retry.execution_id)
           OR EXISTS (SELECT 1 FROM public.vm_workspace_recovery_jobs r
                WHERE r.job_id=d.job_id AND r.resolved_at IS NULL) THEN
            RAISE EXCEPTION 'Job retained rootdisk has another owner'
                USING ERRCODE='23514',CONSTRAINT='vm_job_retained_purge_unbound';
        END IF;
    END IF;

    FOR link IN SELECT p.* FROM public.vm_job_retained_disk_purge_predecessors p
        JOIN public.vm_creation_retries source ON source.request_id=p.source_request_id
        WHERE p.cleanup_admission_id=parent_id
        ORDER BY source.created_at,source.request_id
        FOR SHARE OF p
    LOOP
        link_count := link_count+1;
        SELECT * INTO old_retry FROM public.vm_creation_retries
         WHERE request_id=link.source_request_id FOR SHARE;
        SELECT * INTO old_parent FROM public.vm_workspace_cleanup_admissions
         WHERE id=link.old_cleanup_admission_id FOR SHARE;
        SELECT * INTO old_stop FROM public.vm_resource_cleanup_stop_receipts
         WHERE cleanup_admission_id=link.old_cleanup_admission_id FOR SHARE;
        SELECT * INTO charge FROM public.vm_resource_reservations
         WHERE id=link.reservation_id FOR SHARE;
        IF old_retry.request_id IS NULL OR old_retry.owner_kind IS DISTINCT FROM 'job'
           OR old_retry.job_id IS DISTINCT FROM d.job_id
           OR (old_retry.state IS DISTINCT FROM 'succeeded'
               AND NOT (old_retry.state='settled'
                   AND public.vm_cancel_late_adopted_retained_terminal(old_parent.id)))
           OR old_retry.resolved_at IS NULL
           OR old_retry.observed_pvc_uid IS DISTINCT FROM d.pvc_uid
           OR old_retry.observed_vm_uid IS DISTINCT FROM old_stop.vm_uid
           OR old_retry.creation_admission_id IS NULL
           OR old_stop.cleanup_admission_id IS NULL
           OR old_stop.request_id IS DISTINCT FROM old_retry.request_id
           OR old_stop.job_id IS DISTINCT FROM d.job_id
           OR old_stop.provision_generation IS DISTINCT FROM old_retry.provision_generation
           OR old_stop.pvc_uid IS DISTINCT FROM d.pvc_uid
           OR old_stop.reservation_id IS DISTINCT FROM link.reservation_id
           OR old_stop.intent_digest IS DISTINCT FROM old_parent.intent_digest
           OR old_stop.stop_evidence->>'pvc_disposition' IS DISTINCT FROM 'retained'
           OR old_stop.stop_evidence->>'vm_absent' IS DISTINCT FROM 'true'
           OR old_stop.stop_evidence->>'vmi_absent' IS DISTINCT FROM 'true'
           OR old_stop.stop_evidence->>'launcher_absent' IS DISTINCT FROM 'true'
           OR old_stop.stop_evidence->>'controller_authenticated' IS DISTINCT FROM 'true'
           OR old_stop.stop_evidence->>'same_generation_replacement' IS DISTINCT FROM 'false'
           OR link.stop_evidence_digest IS DISTINCT FROM
               'sha256:'||encode(sha256(convert_to(old_stop.stop_evidence::text,'UTF8')),'hex')
           OR old_parent.id IS NULL OR old_parent.owner_kind IS DISTINCT FROM 'job'
           OR old_parent.owner_id IS DISTINCT FROM d.job_id
           OR old_parent.pvc_uid IS DISTINCT FROM d.pvc_uid
           OR old_parent.parent_admission_id IS NOT NULL
           OR old_parent.completed_at IS NULL OR old_parent.outcome IS DISTINCT FROM 'completed'
           OR old_parent.source NOT IN (
               'dispatcher_vm_recycle','lifecycle_vm_reap',
               'job_terminal_vm_release','public_vm_delete')
           OR charge.id IS NULL OR charge.request_id IS DISTINCT FROM old_retry.request_id
           OR charge.revision IS DISTINCT FROM link.reservation_revision
           OR charge.state IS DISTINCT FROM 'released'
           OR charge.cluster_id IS DISTINCT FROM scope_cluster_id
           OR old_retry.controller_configuration->>'namespace' IS DISTINCT FROM scope_namespace
           OR old_retry.controller_configuration->'resource_admission'->>'cluster_id'
               IS DISTINCT FROM scope_cluster_id
           OR charge.vm_uid IS DISTINCT FROM old_stop.vm_uid
           OR charge.release_evidence->>'kind' IS DISTINCT FROM 'exact_cleanup_compute_absent'
           OR charge.release_evidence->>'cleanup_admission_id' IS DISTINCT FROM old_parent.id::text
           OR charge.release_evidence->>'stop_evidence_digest' IS DISTINCT FROM link.stop_evidence_digest
           OR NOT EXISTS (SELECT 1 FROM public.managed_repository_process_zero_receipts z
               WHERE z.owner_kind='job' AND z.owner_id=d.job_id
                 AND z.scope='vm' AND z.provisioner='vm'
                 AND z.runtime_incarnation=old_retry.provision_generation::text)
           OR EXISTS (SELECT 1 FROM public.vm_pre_ssh_stop_intents i
               WHERE i.job_id=d.job_id AND i.provision_generation=old_retry.provision_generation
                 AND (i.cleanup_admission_id<>old_parent.id OR
                 NOT EXISTS (SELECT 1 FROM public.vm_pre_ssh_stop_proofs proof
                     WHERE proof.cleanup_admission_id=i.cleanup_admission_id
                       AND proof.job_id=d.job_id
                       AND proof.provision_generation=old_retry.provision_generation
                       AND proof.terminal_evidence->>'vm_uid'=old_stop.vm_uid::text
                       AND proof.terminal_evidence->>'launcher_uid'=old_stop.launcher_uid::text)))
           OR EXISTS (SELECT 1 FROM public.vm_resource_recovery_successors successor
               WHERE successor.reservation_id=charge.id
                 AND (successor.owner_id<>d.job_id
                      OR successor.provision_generation<>old_retry.provision_generation
                      OR successor.vm_uid<>old_stop.vm_uid
                      OR successor.root_pvc_uid<>d.pvc_uid)) THEN
            RAISE EXCEPTION 'Job retained disk predecessor changed'
                USING ERRCODE='23514',CONSTRAINT='vm_job_retained_purge_predecessor';
        END IF;
        IF d.binding_kind='bound' THEN
            binding := old_retry.canonical_request->'workspace_storage';
            IF jsonb_typeof(binding) IS DISTINCT FROM 'object'
               OR jsonb_typeof(binding->'generation') IS DISTINCT FROM 'number'
               OR (binding->>'generation') !~ '^[1-9][0-9]*$' THEN
                RAISE EXCEPTION 'Job retained disk predecessor generation unproven'
                    USING ERRCODE='23514',CONSTRAINT='vm_job_retained_purge_predecessor_binding';
            END IF;
            IF jsonb_typeof(binding) IS DISTINCT FROM 'object'
               OR binding->>'uid' IS DISTINCT FROM d.workspace_instance_id::text
               OR binding->>'owner_id' IS DISTINCT FROM d.job_id::text
               OR binding->>'owner_kind' IS DISTINCT FROM 'job'
               OR (binding->>'pvc_uid' IS NOT NULL AND
                   binding->>'pvc_uid' IS DISTINCT FROM d.pvc_uid::text)
               OR ((binding->>'generation')::bigint>1 AND
                   binding->>'pvc_uid' IS DISTINCT FROM d.pvc_uid::text)
               OR (binding->>'generation')::bigint<previous_generation
               OR (binding->>'generation')::bigint>d.workspace_generation
               OR ((binding->>'generation')::bigint=previous_generation
                   AND (old_retry.execution_id IS DISTINCT FROM previous_execution_id
                     OR old_retry.execution_revision IS DISTINCT FROM previous_execution_revision
                     OR old_retry.execution_generation IS DISTINCT FROM previous_execution_generation))
               OR NOT EXISTS (SELECT 1 FROM public.srw_execution_workspace_bindings b
                    WHERE b.execution_id=old_retry.execution_id
                      AND b.instance_id=d.workspace_instance_id) THEN
                RAISE EXCEPTION 'Job retained disk predecessor binding changed'
                    USING ERRCODE='23514',CONSTRAINT='vm_job_retained_purge_predecessor_binding';
            END IF;
            previous_generation := (binding->>'generation')::bigint;
            previous_execution_id := old_retry.execution_id;
            previous_execution_revision := old_retry.execution_revision;
            previous_execution_generation := old_retry.execution_generation;
        ELSE
            IF COALESCE(old_retry.canonical_request->'workspace_storage','null'::jsonb)<>'null'::jsonb THEN
                RAISE EXCEPTION 'Job retained rootdisk predecessor changed'
                    USING ERRCODE='23514';
            END IF;
        END IF;
    END LOOP;
    IF NOT EXISTS (SELECT 1 FROM public.vm_job_retained_disk_purge_predecessors final_link
        JOIN public.vm_resource_cleanup_stop_receipts final_stop
          ON final_stop.cleanup_admission_id=final_link.old_cleanup_admission_id
        WHERE final_link.cleanup_admission_id=parent_id
          AND final_link.source_request_id=d.final_request_id
          AND final_stop.vmi_uid=d.vmi_uid
          AND final_stop.launcher_uid=d.launcher_uid) THEN
        RAISE EXCEPTION 'Job retained disk final runtime identity changed'
            USING ERRCODE='23514',CONSTRAINT='vm_job_retained_purge_predecessor';
    END IF;
    SELECT count(*) INTO retry_count FROM public.vm_creation_retries r
     WHERE r.owner_kind='job' AND r.job_id=d.job_id;
    IF link_count=0 OR retry_count<link_count
       OR NOT EXISTS (SELECT 1 FROM public.vm_job_retained_disk_purge_predecessors p
           WHERE p.cleanup_admission_id=parent_id AND p.source_request_id=d.final_request_id)
       OR EXISTS (SELECT 1 FROM public.vm_resource_cleanup_stop_receipts s
           WHERE s.job_id=d.job_id AND s.stop_evidence->>'pvc_disposition'='retained'
             AND NOT EXISTS (SELECT 1 FROM public.vm_job_retained_disk_purge_predecessors p
                 WHERE p.cleanup_admission_id=parent_id AND p.source_request_id=s.request_id)) THEN
        RAISE EXCEPTION 'Job retained disk predecessor chain incomplete'
            USING ERRCODE='23514',CONSTRAINT='vm_job_retained_purge_chain';
    END IF;
    -- An earlier retry outside this same-PVC retained chain still needs its
    -- own already-completed ordinary purged or never-issued terminal packet.
    -- The 0326 logical predecessor guard continues to reject physical A then
    -- never-issued B; this branch does not fabricate an attestor for B.
    FOR old_retry IN SELECT r.* FROM public.vm_creation_retries r
      WHERE r.owner_kind='job' AND r.job_id=d.job_id
        AND NOT EXISTS (SELECT 1 FROM public.vm_job_retained_disk_purge_predecessors p
            WHERE p.cleanup_admission_id=parent_id AND p.source_request_id=r.request_id)
      ORDER BY r.created_at,r.request_id
    LOOP
        IF terminal_tail IS NOT NULL AND EXISTS(SELECT 1 FROM jsonb_array_elements(terminal_tail) item
            JOIN public.vm_job_retained_resume_terminals t ON t.id::text=item->>'terminal_id'
            WHERE t.source_request_id=old_retry.request_id AND t.terminal_kind IN ('never_issued','creation_disposed')) THEN
            CONTINUE;
        END IF;
        IF (old_retry.created_at,old_retry.request_id)>=(retry.created_at,retry.request_id)
           OR COALESCE(public.vm_job_terminal_prior_packet_evidence(old_retry.request_id)->>'kind','')
              NOT IN ('never_issued','physical_stop') THEN
            RAISE EXCEPTION 'Job retained disk has unclassified retry history'
                USING ERRCODE='23514',CONSTRAINT='vm_job_retained_purge_chain';
        END IF;
    END LOOP;
    SELECT * INTO receipt FROM public.vm_job_retained_disk_purge_receipts
     WHERE cleanup_admission_id=parent_id;
    IF require_receipt AND receipt.cleanup_admission_id IS NULL THEN
        RAISE EXCEPTION 'Job retained disk purge lacks signed receipt'
            USING ERRCODE='23514',CONSTRAINT='vm_job_retained_purge_receipt';
    END IF;
    IF receipt.cleanup_admission_id IS NOT NULL AND (
       receipt.chain_digest IS DISTINCT FROM public.vm_job_retained_disk_purge_chain_digest(parent_id)
       OR receipt.purge_evidence IS DISTINCT FROM (jsonb_build_object(
           'version',1,'kind','vm_cleanup_physical_stop','job_id',d.job_id,
           'provision_generation',d.provision_generation,'vm_uid',d.vm_uid,
           'vmi_uid',d.vmi_uid,'launcher_uid',d.launcher_uid,'pvc_uid',d.pvc_uid,
           'vm_absent',true,'vmi_absent',true,'launcher_absent',true,
           'same_generation_replacement',false,'controller_authenticated',true,
           'pvc_disposition','purged',
           'controller_scope',jsonb_build_object(
               'version',1,'namespace',scope_namespace,'cluster_id',scope_cluster_id)) ||
           CASE WHEN d.binding_kind='bound'
               THEN jsonb_build_object('captured_workspace_storage',
                   jsonb_set(retry.canonical_request->'workspace_storage',
                       '{pvc_uid}',to_jsonb(d.pvc_uid::text),true))
               ELSE '{}'::jsonb END)) THEN
        RAISE EXCEPTION 'Job retained disk purge receipt changed'
            USING ERRCODE='23514',CONSTRAINT='vm_job_retained_purge_receipt';
    END IF;
    RETURN true;
END;
$body$;

COMMIT;
