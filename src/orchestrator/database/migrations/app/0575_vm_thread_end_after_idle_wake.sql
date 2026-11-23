-- A pinned VM Session's idle wake creates the successor VM on the kept rootdisk
-- before any agent is bound: origin 'initial', expected_pvc_uid = the kept
-- disk, thread_wake_operation_id set, no agent or attach token. End, Resume
-- after End and Delete after End knew only initial sources on a fresh disk
-- bound to their agent and retained-Resume sources, so every End after a wake
-- raised 'VM thread cleanup source identity changed' and stayed open.
--
-- One predicate admits a wake source as one more exact lineage, never by
-- dropping a check: its closed Ready wake operation of this thread names this
-- generation, request, VM and kept disk, and the agent is either none (asleep)
-- or the fresh agent proven on the End's own immutable capture with
-- finish_thread_wake's proof (the captured agent's own finalizer-protected
-- Pod, not the Pod the sleep stopped) in the generation the wake's VM was
-- created in. Every other guard, quota and charge check is unchanged.
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

CREATE FUNCTION public.valid_vm_thread_wake_cleanup_source(
    a public.vm_resource_thread_cleanup_authorities, source public.vm_creation_retries
) RETURNS boolean LANGUAGE sql STABLE AS $$
    SELECT (source.owner_kind='thread' AND source.origin='initial'
       AND source.thread_id=a.thread_id
       AND source.thread_retained_resume_id IS NULL
       AND source.thread_wake_operation_id IS NOT NULL
       AND source.thread_agent_id IS NULL AND source.thread_attach_token IS NULL
       AND source.thread_runtime_generation=a.runtime_generation
       AND source.provision_generation=a.provision_generation
       AND source.expected_pvc_uid=source.observed_pvc_uid
       AND source.observed_vm_uid=a.vm_uid AND source.observed_pvc_uid=a.pvc_uid
       AND EXISTS (SELECT 1 FROM public.vm_idle_operations op
           WHERE op.id=source.thread_wake_operation_id
             AND op.owner_kind='thread' AND op.owner_id=a.thread_id
             AND op.release_kind='pinned_thread' AND op.retained_kind='rootdisk'
             AND op.wake_generation=source.provision_generation
             AND op.wake_request_id=source.request_id
             AND op.pvc_uid=source.expected_pvc_uid
             AND op.phase='ready' AND op.closed_at IS NOT NULL AND op.wake_ready_at IS NOT NULL
             AND op.thread_wake_ready_identity->>'generation'=source.provision_generation::text
             AND op.thread_wake_ready_identity->>'vm_uid'=source.observed_vm_uid::text
             AND op.thread_wake_ready_identity->>'pvc_uid'=source.observed_pvc_uid::text
             AND ((a.agent_id IS NULL AND a.attach_token IS NULL)
                  OR (a.agent_id IS NOT NULL AND a.attach_token IS NOT NULL
                      AND a.retirement_context->>'agent_id'=a.agent_id::text
                      AND a.retirement_context->>'runtime_attach_token'=a.attach_token::text
                      AND a.retirement_context->'agent'->>'id'=a.agent_id::text
                      AND a.retirement_context->'agent'->>'thread_id'=a.thread_id::text
                      AND a.retirement_context->'agent_pod'->>'protection_protocol'='finalizer_v1'
                      AND a.retirement_context->'agent_pod'->>'pod_name'=a.retirement_context->'agent'->>'hostname'
                      AND a.retirement_context->'agent_pod'->>'pod_uid'=a.retirement_context->'agent'->>'pod_uid'
                      AND a.retirement_context->'agent_pod'->>'pod_uid'
                          IS DISTINCT FROM op.thread_agent_pod_identity->>'pod_uid')))
    ) IS TRUE;
$$;

-- 0323's definition with the wake lineage added to its actor and origin clauses.
CREATE OR REPLACE FUNCTION public.valid_vm_thread_nonquota_cleanup_source(
    a public.vm_resource_thread_cleanup_authorities, source public.vm_creation_retries
) RETURNS boolean LANGUAGE sql STABLE AS $$
    SELECT (public.valid_vm_thread_nonquota_creation(source)
       AND a.reservation_id IS NULL AND a.reservation_revision IS NULL
       AND source.thread_id=a.thread_id
       AND ((source.thread_runtime_generation=a.runtime_generation
             AND source.thread_agent_id IS NOT DISTINCT FROM a.agent_id
             AND source.thread_attach_token IS NOT DISTINCT FROM a.attach_token)
            OR public.valid_vm_thread_wake_cleanup_source(a,source)
            OR public.vm_thread_creation_pre_setup_abort_path_evidence(
                json_populate_record(NULL::public.threads,json_build_object(
                    'id',a.thread_id,'runtime_generation',a.runtime_generation,
                    'runtime_retirement_started_at',a.created_at)),source) IS NOT NULL)
       AND source.provision_generation=a.provision_generation
       AND source.observed_vm_uid=a.vm_uid AND source.observed_pvc_uid=a.pvc_uid
       AND a.vmi_uid::text IS NOT DISTINCT FROM a.retirement_context->'vm'->>'vmi_uid'
       AND a.launcher_uid::text IS NOT DISTINCT FROM a.retirement_context->'vm'->>'active_pod_uid'
       AND (source.state='succeeded' AND source.reason='creation_adopted'
            OR public.valid_vm_thread_retained_cleanup_source(a,source))
       AND source.boot_counted
       AND (source.origin='initial' AND source.expected_pvc_uid IS NULL
            OR public.valid_vm_thread_retained_resume_source(source)
            OR public.valid_vm_thread_wake_cleanup_source(a,source))
       AND NOT EXISTS (SELECT 1 FROM public.vm_creation_effects e
           WHERE e.request_id=source.request_id AND e.state='issued')
       AND EXISTS (SELECT 1 FROM public.vm_workspace_cleanup_admissions c
           WHERE c.id=source.creation_admission_id AND c.owner_kind='thread'
             AND c.owner_id=a.thread_id AND c.source='controller_vm_create'
             AND c.completed_at IS NOT NULL AND c.outcome='adopted')
    ) IS TRUE;
$$;

-- Patch the installed definitions in place, as 0323 did: each replaced clause
-- must occur exactly once, every other guard stays verbatim.
DO $migration$
DECLARE
    patch record;
    definition text;
    wake text := 'public.valid_vm_thread_wake_cleanup_source(a,source)';
BEGIN
    FOR patch IN SELECT * FROM (VALUES
        -- End, quota branch: the plain source/authority agent equality is
        -- waived only for an exact wake source.
        ('public.validate_vm_thread_cleanup_authority(public.vm_resource_thread_cleanup_authorities,boolean)'::text,
         E'OR (NOT terminal_handoff AND NOT public.valid_vm_thread_nonquota_cleanup_source(a,source) AND (\n'::text,
         E'OR (NOT terminal_handoff AND NOT public.valid_vm_thread_nonquota_cleanup_source(a,source) AND NOT '
             || wake || E' AND (\n'),
        -- Resume after End: a wake predecessor only with its exact evidence.
        ('public.validate_vm_thread_retained_compute(uuid)',
         E'       OR source.thread_wake_operation_id IS NOT NULL\n       OR NOT (source.origin=''initial'' AND source.expected_pvc_uid IS NULL\n',
         E'       OR (source.thread_wake_operation_id IS NOT NULL AND NOT ' || wake
             || E')\n       OR NOT (source.origin=''initial'' AND source.expected_pvc_uid IS NULL\n           OR '
             || wake || E'\n'),
        ('public.validate_vm_thread_retained_compute(uuid)',
         E'OR (NOT COALESCE(public.valid_vm_thread_retained_cleanup_source(a,source),false) AND NOT public.valid_vm_thread_nonquota_cleanup_source(a,source) AND (\n',
         E'OR (NOT COALESCE(public.valid_vm_thread_retained_cleanup_source(a,source),false) AND NOT public.valid_vm_thread_nonquota_cleanup_source(a,source) AND NOT '
             || wake || E' AND (\n'),
        -- Delete after End: the same.
        ('public.validate_vm_thread_retained_disk_purge(public.vm_thread_retained_disk_purge_authorities,boolean)',
         'OR NOT (source.origin=''initial'' OR public.valid_vm_thread_retained_resume_source(source)) OR source.thread_wake_operation_id IS NOT NULL',
         'OR NOT (source.origin=''initial'' OR public.valid_vm_thread_retained_resume_source(source)) OR (source.thread_wake_operation_id IS NOT NULL AND NOT '
             || wake || ')'),
        ('public.validate_vm_thread_retained_disk_purge(public.vm_thread_retained_disk_purge_authorities,boolean)',
         E'OR (NOT COALESCE(public.valid_vm_thread_retained_cleanup_source(a,source),false) AND NOT public.valid_vm_thread_nonquota_cleanup_source(a,source) AND (\n',
         E'OR (NOT COALESCE(public.valid_vm_thread_retained_cleanup_source(a,source),false) AND NOT public.valid_vm_thread_nonquota_cleanup_source(a,source) AND NOT '
             || wake || E' AND (\n')
    ) AS p(signature, old_clause, new_clause) LOOP
        definition := pg_get_functiondef(patch.signature::regprocedure);
        IF (length(definition)-length(replace(definition,patch.old_clause,'')))/length(patch.old_clause)<>1 THEN
            RAISE EXCEPTION '0575 requires the exact clause once in %: %',patch.signature,patch.old_clause;
        END IF;
        EXECUTE replace(definition,patch.old_clause,patch.new_clause);
    END LOOP;
END;
$migration$;
COMMIT;
