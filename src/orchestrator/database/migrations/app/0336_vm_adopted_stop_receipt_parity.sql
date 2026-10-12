-- Match the existing Python cleanup authority for a physically bound,
-- adopted Job VM that never reached guest Ready. Missing/JSON-null owner Pod
-- binding is permitted only with creation_adopted, no Ready receipt, and no
-- recovery successor; exact physical IDs, signed absence, and process-zero
-- remain mandatory. No owner context or runtime execution authority is added.
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
       OR source.retry_state<>'succeeded'
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
