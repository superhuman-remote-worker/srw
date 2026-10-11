-- migration: 0434_vm_job_late_adopted_terminal_audit.sql
-- A completed 0433 late-adopted retained purge must yield the same immutable
-- terminal packet as an ordinary succeeded predecessor. All purge and Job
-- deletion guards remain in force; unknown or still-issued sources stay held.
BEGIN;

CREATE OR REPLACE FUNCTION public.vm_job_terminal_packet_without_retained_noeffect(source_request uuid) RETURNS jsonb
    LANGUAGE plpgsql
    AS $$
DECLARE prior jsonb;
        r public.vm_creation_retries%ROWTYPE;
        d public.vm_job_retained_disk_purge_authorities%ROWTYPE;
        p public.vm_job_retained_disk_purge_predecessors%ROWTYPE;
        old_stop public.vm_resource_cleanup_stop_receipts%ROWTYPE;
        parent public.vm_workspace_cleanup_admissions%ROWTYPE;
        charge public.vm_resource_reservations%ROWTYPE;
BEGIN
    prior := public.vm_job_terminal_prior_packet_evidence(source_request);
    IF prior IS NOT NULL THEN RETURN prior; END IF;
    SELECT * INTO r FROM public.vm_creation_retries WHERE request_id=source_request;
    IF r.request_id IS NULL OR r.owner_kind IS DISTINCT FROM 'job' THEN RETURN NULL; END IF;
    SELECT * INTO p FROM public.vm_job_retained_disk_purge_predecessors
      WHERE source_request_id=source_request;
    -- 0433 admits a cancelled, late-adopted policy-1 source to typed disk
    -- purge only after its original retained physical/custody parent settles.
    -- Keep all older succeeded paths unchanged and reject every other settled
    -- retry before constructing a terminal packet.
    IF r.state IS DISTINCT FROM 'succeeded'
       AND NOT (r.state='settled' AND r.reason='creation_adopted'
           AND p.source_request_id IS NOT DISTINCT FROM r.request_id
           AND public.vm_cancel_late_adopted_retained_terminal(
               p.old_cleanup_admission_id)) THEN RETURN NULL; END IF;
    SELECT * INTO d FROM public.vm_job_retained_disk_purge_authorities
      WHERE cleanup_admission_id=p.cleanup_admission_id;
    SELECT * INTO parent FROM public.vm_workspace_cleanup_admissions
      WHERE id=d.cleanup_admission_id;
    SELECT * INTO old_stop FROM public.vm_resource_cleanup_stop_receipts
      WHERE cleanup_admission_id=p.old_cleanup_admission_id;
    SELECT * INTO charge FROM public.vm_resource_reservations
      WHERE id=p.reservation_id;
    IF p.source_request_id IS NULL OR d.cleanup_admission_id IS NULL
       OR parent.completed_at IS NULL OR parent.outcome IS DISTINCT FROM 'completed'
       OR d.job_id IS DISTINCT FROM r.job_id
       OR d.pvc_uid IS DISTINCT FROM r.observed_pvc_uid
       OR old_stop.request_id IS DISTINCT FROM r.request_id
       OR old_stop.provision_generation IS DISTINCT FROM r.provision_generation
       OR charge.state IS DISTINCT FROM 'released' THEN RETURN NULL; END IF;
    PERFORM public.validate_vm_job_retained_disk_purge(d.cleanup_admission_id,true);
    RETURN jsonb_build_object(
        'kind','retained_late_purge','job_id',r.job_id,
        'request_id',r.request_id,'provision_generation',r.provision_generation,
        'vm_uid',r.observed_vm_uid,'pvc_uid',r.observed_pvc_uid,
        'execution_id',r.execution_id,'execution_revision',r.execution_revision,
        'execution_generation',r.execution_generation,
        'execution_chain',public.vm_job_execution_chain_evidence(r.execution_id),
        'cleanup_admission_id',d.cleanup_admission_id,
        'cleanup_intent_digest',parent.intent_digest,
        'old_cleanup_admission_id',p.old_cleanup_admission_id,
        'old_stop_evidence_digest',p.stop_evidence_digest,
        'old_reservation_id',p.reservation_id,
        'old_reservation_revision',p.reservation_revision,
        'purge_generation',d.provision_generation,
        'binding_kind',d.binding_kind,
        'workspace_instance_id',d.workspace_instance_id,
        'workspace_generation',d.workspace_generation,
        'chain_digest',public.vm_job_retained_disk_purge_chain_digest(d.cleanup_admission_id),
        'process_zero_receipt_ids',COALESCE((
            SELECT jsonb_agg(z.id ORDER BY z.id)
            FROM public.managed_repository_process_zero_receipts z
            WHERE z.owner_kind='job' AND z.owner_id=r.job_id
              AND z.scope='vm' AND z.provisioner='vm'
              AND z.runtime_incarnation=r.provision_generation::text),'[]'::jsonb),
        'retry_reason',r.reason,'resolved_at',r.resolved_at);
END;
$$;

COMMIT;
