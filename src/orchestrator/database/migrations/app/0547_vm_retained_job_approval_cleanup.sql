-- migration: 0547_vm_retained_job_approval_cleanup.sql
-- Ordinary approval must release a retained continuation's compute while
-- preserving its exact disk. Recognize its durable terminal marker and permit
-- first keep-admission before the marker is bound to that admission. All source,
-- queue, runtime, reservation, signed-stop and disk-custody predicates remain.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE OR REPLACE FUNCTION public.vm_job_retained_terminal_authorized(owner uuid) RETURNS boolean
    LANGUAGE sql STABLE
    AS $$
    SELECT EXISTS(SELECT 1 FROM public.jobs j WHERE j.id=owner AND (
        -- Ordinary Approve commits this marker with completed status. A finished
        -- review command has no finalizing lease to authorize the later sweep.
        (j.status='completed' AND j.completed_at IS NOT NULL AND j.freeze_data IS NULL
          AND j.context->'_job_terminal_vm_cleanup' @> '{"version":1,"source":"approve"}'::jsonb
          AND j.context->'_job_terminal_vm_cleanup'->>'provision_generation' IS NOT NULL
          AND j.context->'_job_terminal_vm_cleanup'->>'provision_generation'=
              j.context->'vm'->>'provision_generation')
        OR (j.status='cancelled' AND j.context->'_stateless_cancel_cleanup_pending'='true'::jsonb)
        OR (j.status IN ('cancelled','failed','completed') AND j.context->'_stateless_delete_pending'='true'::jsonb)
        OR EXISTS(SELECT 1 FROM public.job_completion_commands command
            JOIN public.completion_effects effect ON effect.producer_id=command.id
            LEFT JOIN public.completion_effects disposition ON disposition.producer_kind='job_completion'
                AND disposition.producer_id=command.id AND disposition.effect_name='main_status_write' AND disposition.state='done'
            LEFT JOIN public.completion_effects entry ON entry.producer_kind='job_completion'
                AND entry.producer_id=command.id AND entry.effect_name='late_callback_guard' AND entry.state='done'
            WHERE command.job_id=j.id AND command.report_seq=j.completion_seq_hwm
              AND command.state='finalizing' AND command.lease_expires_at>clock_timestamp()
              AND command.deadline_at>clock_timestamp() AND effect.producer_kind='job_completion'
              AND effect.effect_name='workspace_archive_teardown' AND effect.state='pending'
              AND effect.complete_by>clock_timestamp()
              AND effect.detail->'teardown_authorization'->'active'='true'::jsonb
              AND effect.detail->'teardown_authorization'->>'report_seq'=command.report_seq::text
              AND j.status IN ('completed','failed','cancelled')
              AND COALESCE(NULLIF(disposition.detail#>>'{output,new_status}',''),
                  CASE WHEN entry.detail#>>'{output,matched}'='true' THEN entry.detail#>>'{output,entry_status}' END)=j.status::text)));
$$;

CREATE OR REPLACE FUNCTION public.vm_job_retained_stop_candidate(owner uuid, generation uuid, expected_vm uuid, pvc uuid, old_parent uuid, retaining_parent uuid DEFAULT NULL::uuid, existing_stop boolean DEFAULT false) RETURNS jsonb
    LANGUAGE plpgsql
    AS $_$
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
    IF j.id IS NULL OR NOT public.vm_job_retained_terminal_authorized(owner)
       OR j.execution_lane IS DISTINCT FROM 'stateless'
       OR j.assigned_agent_id IS NOT NULL OR j.parent_job_id IS NOT NULL
       OR COALESCE(j.context->'inherits_parent_workspace','false'::jsonb) IS DISTINCT FROM 'false'::jsonb
       OR jsonb_typeof(vm) IS DISTINCT FROM 'object'
       OR vm->>'provision_generation' IS DISTINCT FROM generation::text
       OR vm->>'vm_uid' IS DISTINCT FROM expected_vm::text
       OR vm->>'rootdisk_pvc_uid' IS DISTINCT FROM pvc::text
       OR vm->>'status' IS NULL OR (vm->>'status' NOT IN ('created','ssh_pending','ssh_unreachable','ready','retiring_process_zero')
           AND NOT (vm->>'status'='deleted' AND v.state='teardown'
               AND public.vm_job_deleted_retention_replay_allowed(2,owner,generation,
                   expected_vm,pvc,retaining_parent,existing_stop,r.request_id,
                   v.id,v.revision,v.vmi_uid,v.launcher_uid,v.node_uid)))
       OR vm->'identity_authenticated' IS DISTINCT FROM 'true'::jsonb
       OR vm->>'identity_provision_generation' IS DISTINCT FROM generation::text
       OR COALESCE(vm->'workspace_storage','null'::jsonb)<>'null'::jsonb
       OR COALESCE(r.canonical_request->'workspace_storage','null'::jsonb)<>'null'::jsonb
       OR q.unit_id IS NULL OR q.unit_kind IS DISTINCT FROM 'worker_batch'
       OR q.state IS DISTINCT FROM 'done' OR q.leased_by IS NOT NULL OR q.leased_until IS NOT NULL
       OR r.request_id IS NULL OR r.state IS DISTINCT FROM 'succeeded'
       OR r.reason IS DISTINCT FROM 'creation_adopted' OR r.resolved_at IS NULL
       OR r.claim_token IS NOT NULL OR r.claim_expires_at IS NOT NULL
       OR NOT EXISTS(SELECT 1 FROM public.vm_job_retained_resumes op
           WHERE op.id=r.job_retained_resume_id AND op.request_id=r.request_id
             AND op.job_id=owner AND op.provision_generation=generation AND op.pvc_uid=pvc
             AND j.context->>'_vm_job_retained_resume'=op.id::text
             AND op.admitted_xact_id<>pg_current_xact_id()
             AND r.job_retained_resume_admitted_xact_id<>pg_current_xact_id()
             AND public.vm_job_cancel_retention_settled(op.physical_cleanup_admission_id)
             AND NOT public.vm_job_cancel_retention_discharged(op.root_retention_admission_id)
             AND NOT EXISTS(SELECT 1 FROM public.vm_job_retained_resume_terminals t WHERE t.resume_id=op.id))
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
           AND (
               j.context->'_job_terminal_vm_cleanup'->>'admission_id'
                   IN (old_parent::text,retaining_parent::text)
               OR (
                   -- The approved marker precedes first-parent admission. Once
                   -- bound, replay must match that exact parent above; explicit
                   -- JSON null is invalid and must not act like a missing key.
                   NOT (j.context->'_job_terminal_vm_cleanup' ? 'admission_id')
                   AND j.status='completed' AND j.completed_at IS NOT NULL
                   AND j.freeze_data IS NULL
                   AND j.context->'_job_terminal_vm_cleanup'
                       @> '{"version":1,"source":"approve"}'::jsonb
               )
           )) IS DISTINCT FROM true) THEN
        RETURN NULL;
    END IF;
    RETURN jsonb_build_object('creation_request_id',r.request_id,'reservation_id',v.id,
        'reservation_revision',v.revision,'vmi_uid',v.vmi_uid,'launcher_uid',v.launcher_uid,
        'node_uid',v.node_uid,'namespace',r.controller_configuration->>'namespace','cluster_id',v.cluster_id);
END;
$_$;
