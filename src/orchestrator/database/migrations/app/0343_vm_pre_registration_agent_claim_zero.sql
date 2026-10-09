-- migration: 0343_vm_pre_registration_agent_claim_zero.sql
-- description: Permit exact retained Agent PVC claim during never-issued VM Session End.
-- depends-on: 0342_vm_pre_registration_agent_zero.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

-- The 0342 predicate correctly excludes VM compute authority, but a normal
-- published Agent Pod can own a ready workspace PVC before VM admission. That
-- PVC is retained on soft End and is independently reconciled by the existing
-- claim lifecycle. Match its one captured claim to the durable ready row and
-- the published Pod intent; no other claim or VM authority is adopted.
CREATE OR REPLACE FUNCTION public.pinned_vm_pre_registration_no_vm_source(
    p_thread uuid, p_runtime uuid, p_token uuid
) RETURNS boolean LANGUAGE sql STABLE AS $body$
    SELECT EXISTS (
        SELECT 1 FROM public.threads t
         WHERE t.id=p_thread AND t.runtime_generation=p_runtime
           AND t.runtime_retirement_token=p_token
           AND t.execution_lane='pinned' AND t.status='created'
           AND t.runtime_authority_exposed IS TRUE
           AND t.runtime_retirement_authorized_at IS NOT NULL
           AND t.agent_id IS NULL AND t.control_admission_agent_id IS NULL
           AND t.runtime_attach_token IS NULL
           AND t.runtime_retirement_context->>'entry_status'='created'
           AND t.runtime_retirement_context->>'workspace_backend'='vm'
           AND t.runtime_retirement_context->>'agent_id' IS NULL
           AND t.runtime_retirement_context->>'control_admission_agent_id' IS NULL
           AND t.runtime_retirement_context->>'runtime_attach_token' IS NULL
           AND t.runtime_retirement_context->'agent' IN ('null'::jsonb,'{}'::jsonb)
           AND t.runtime_retirement_context->'agent_pod_provision_intent'='null'::jsonb
           AND jsonb_typeof(t.runtime_retirement_context->'agent_pod')='object'
           AND NULLIF(t.runtime_retirement_context->'agent_pod'->>'pod_name','') IS NOT NULL
           AND NULLIF(t.runtime_retirement_context->'agent_pod'->>'pod_uid','') IS NOT NULL
           AND t.runtime_retirement_context->'vm'='null'::jsonb
           AND t.runtime_retirement_context->'vm_creation_source'='null'::jsonb
           AND NOT (t.metadata ? 'vm')
           AND t.runtime_retirement_context->'workspace_binding' IN ('null'::jsonb,'{}'::jsonb)
           AND (t.runtime_retirement_context->'workspace_container'='null'::jsonb
                OR (jsonb_typeof(t.runtime_retirement_context->'workspace_container')='object'
                    AND (t.runtime_retirement_context->'workspace_container')
                        - 'repo_name' - 'git_remote_url'='{}'::jsonb))
           AND t.runtime_retirement_context->'workspace_provision_intent'='null'::jsonb
           AND (
               (t.runtime_retirement_context->'agent_workspace_claim'='null'::jsonb
                AND NOT EXISTS (
                    SELECT 1 FROM public.thread_agent_workspace_claims c
                     WHERE c.thread_id=t.id AND c.status IN ('planned','ready','revoking')))
               OR
               (jsonb_typeof(t.runtime_retirement_context->'agent_workspace_claim')='object'
                AND t.runtime_retirement_context->'agent_workspace_claim'->>'status'='ready'
                AND t.runtime_retirement_context->'agent_workspace_claim'->>'thread_id'=t.id::text
                AND t.runtime_retirement_context->'agent_pod'->>'runtime_generation'=t.runtime_generation::text
                AND EXISTS (
                    SELECT 1 FROM public.thread_agent_workspace_claims c
                    JOIN public.thread_agent_pod_provision_intents p
                      ON p.workspace_claim_id=c.claim_id
                     WHERE c.thread_id=t.id
                       AND c.claim_id::text=t.runtime_retirement_context->'agent_workspace_claim'->>'claim_id'
                       AND c.created_runtime_generation::text=
                           t.runtime_retirement_context->'agent_workspace_claim'->>'created_runtime_generation'
                       AND c.create_attempt::text=t.runtime_retirement_context->'agent_workspace_claim'->>'create_attempt'
                       AND c.provisioner::text=t.runtime_retirement_context->'agent_workspace_claim'->>'provisioner'
                       AND c.provisioner IN ('agent','persistent')
                       AND c.pvc_name=t.runtime_retirement_context->'agent_workspace_claim'->>'pvc_name'
                       AND (
                           (c.status='ready'
                            AND c.pvc_uid=t.runtime_retirement_context->'agent_workspace_claim'->>'pvc_uid'
                            AND c.fenced_at IS NULL AND c.gc_after IS NULL)
                           OR
                           (t.runtime_retirement_permanent IS TRUE
                            AND c.status IN ('fenced','reclaimed')
                            AND c.fenced_at IS NOT NULL
                            AND c.pvc_uid IS DISTINCT FROM
                                t.runtime_retirement_context->'agent_workspace_claim'->>'pvc_uid'
                            AND t.runtime_retirement_local_quiescence->>'quiescence_protocol'='agent_runtime_zero_v1'
                            AND t.runtime_retirement_local_quiescence->>'agent_pod_uid'=
                                t.runtime_retirement_context->'agent_pod'->>'pod_uid')
                       )
                       AND NULLIF(c.pvc_uid,'') IS NOT NULL
                       AND c.namespace=t.runtime_retirement_context->'agent_workspace_claim'->>'namespace'
                       AND c.namespace=t.runtime_retirement_context->'agent_pod'->>'namespace'
                       AND c.protection_protocol='finalizer_v1'
                       AND c.protection_protocol=t.runtime_retirement_context->'agent_workspace_claim'->>'protection_protocol'
                       AND c.protection_protocol=t.runtime_retirement_context->'agent_pod'->>'protection_protocol'
                       AND p.thread_id=t.id
                       AND p.runtime_generation=t.runtime_generation
                       AND p.attempt_id::text=t.runtime_retirement_context->'agent_pod'->>'provision_attempt'
                       AND p.status='published'
                       AND p.pod_name=t.runtime_retirement_context->'agent_pod'->>'pod_name'
                       AND p.pod_uid=t.runtime_retirement_context->'agent_pod'->>'pod_uid'
                       AND p.namespace=c.namespace
                       AND p.provisioner=c.provisioner
                       AND p.protection_protocol='finalizer_v1'
                )
                AND NOT EXISTS (
                    SELECT 1 FROM public.thread_agent_workspace_claims other
                     WHERE other.thread_id=t.id
                       AND other.status IN ('planned','ready','revoking')
                       AND other.claim_id::text IS DISTINCT FROM
                           t.runtime_retirement_context->'agent_workspace_claim'->>'claim_id'))
           )
           AND NOT EXISTS (SELECT 1 FROM public.agents a
                            WHERE a.thread_id=t.id
                               OR a.hostname=t.runtime_retirement_context->'agent_pod'->>'pod_name'
                               OR a.pod_uid=t.runtime_retirement_context->'agent_pod'->>'pod_uid')
           AND NOT EXISTS (SELECT 1 FROM public.thread_agent_pod_provision_intents p
                            WHERE p.thread_id=t.id AND p.status IN ('planned','revoking'))
           AND NOT EXISTS (SELECT 1 FROM public.thread_workspace_provision_intents p
                            WHERE p.thread_id=t.id AND p.status IN ('planned','revoking'))
           AND NOT EXISTS (SELECT 1 FROM public.vm_creation_retries r
                            WHERE r.owner_kind='thread' AND r.thread_id=t.id)
           AND NOT EXISTS (SELECT 1 FROM public.vm_resource_waiters w
                            WHERE w.owner_kind='thread' AND w.thread_id=t.id)
           AND NOT EXISTS (SELECT 1 FROM public.vm_thread_creation_owners o
                            WHERE o.thread_id=t.id)
           AND NOT EXISTS (SELECT 1 FROM public.vm_thread_retained_resumes r
                            WHERE r.thread_id=t.id)
           AND NOT EXISTS (SELECT 1 FROM public.vm_idle_operations i
                            WHERE i.owner_kind='thread' AND i.owner_id=t.id)
           AND NOT EXISTS (SELECT 1 FROM public.vm_idle_access_leases l
                            WHERE l.owner_kind='thread' AND l.owner_id=t.id)
           AND NOT EXISTS (SELECT 1 FROM public.vm_workspace_recoveries r
                            WHERE r.owner_kind='thread' AND r.owner_id=t.id)
           AND NOT EXISTS (SELECT 1 FROM public.vm_resource_thread_cleanup_authorities a
                            WHERE a.thread_id=t.id)
           AND NOT EXISTS (SELECT 1 FROM public.srw_execution_specs e
                            JOIN public.srw_execution_workspace_bindings b
                              ON b.execution_id=e.id
                            WHERE e.work_kind='Session' AND e.work_id=t.id)
    );
$body$;

COMMIT;
