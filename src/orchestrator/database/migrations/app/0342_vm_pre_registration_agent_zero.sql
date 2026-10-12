-- migration: 0342_vm_pre_registration_agent_zero.sql
-- description: Permit exact Agent Pod zero before any pinned VM authority exists.
-- depends-on: 0341_vm_job_deleted_retention_replay.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

-- A created VM Session can publish its Agent Pod before VM admission. If End
-- wins that interval, the captured Pod can be stopped without a VM actuator.
-- An issued or captured VM must still use its existing creation/actuator proof.
CREATE FUNCTION public.pinned_vm_pre_registration_no_vm_source(
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
           AND t.runtime_retirement_context->'agent_workspace_claim'='null'::jsonb
           AND NOT EXISTS (SELECT 1 FROM public.agents a
                            WHERE a.thread_id=t.id
                               OR a.hostname=t.runtime_retirement_context->'agent_pod'->>'pod_name'
                               OR a.pod_uid=t.runtime_retirement_context->'agent_pod'->>'pod_uid')
           AND NOT EXISTS (SELECT 1 FROM public.thread_agent_pod_provision_intents p
                            WHERE p.thread_id=t.id AND p.status IN ('planned','revoking'))
           AND NOT EXISTS (SELECT 1 FROM public.thread_agent_workspace_claims c
                            WHERE c.thread_id=t.id AND c.status IN ('planned','ready','revoking'))
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

-- Retain every existing Pod identity, actor, generation, and receipt check in
-- the two triggers. Only choose the agent-zero protocol for the exact pre-VM
-- source, before the existing issued-creation and VM-actuator alternatives.
DO $migration$
DECLARE
    definition text;
    old_fragment text := $old$                    WHEN public.pinned_vm_creation_agent_zero_source($old$;
    new_fragment text := $new$                    WHEN public.pinned_vm_pre_registration_no_vm_source(
                        NEW.id,NEW.runtime_generation,NEW.runtime_retirement_token)
                    THEN NEW.runtime_retirement_local_quiescence->>'quiescence_protocol'='agent_runtime_zero_v1'
                         AND NEW.runtime_retirement_local_quiescence->>'workspace_generation' IS NULL
                         AND NEW.runtime_retirement_local_quiescence->>'workspace_runtime_incarnation' IS NULL
                         AND NEW.runtime_retirement_local_quiescence->>'vm_creation_request_id' IS NULL
                         AND NEW.runtime_retirement_local_quiescence->>'vm_creation_provision_generation' IS NULL
                    WHEN public.pinned_vm_creation_agent_zero_source($new$;
BEGIN
    definition := pg_get_functiondef('public.enforce_thread_ended_transition()'::regprocedure);
    IF (length(definition)-length(replace(definition,old_fragment,'')))/length(old_fragment) <> 1 THEN
        RAISE EXCEPTION '0342 requires one exact pre-VM UPDATE protocol branch';
    END IF;
    EXECUTE replace(definition,old_fragment,new_fragment);
END;
$migration$;

DO $migration$
DECLARE
    definition text;
    old_fragment text := $old$        ELSIF public.pinned_vm_creation_agent_zero_source($old$;
    new_fragment text := $new$        ELSIF public.pinned_vm_pre_registration_no_vm_source(
            OLD.id,OLD.runtime_generation,OLD.runtime_retirement_token) THEN
            expected_protocol := CASE
                WHEN local_quiescence->>'vm_creation_request_id' IS NULL
                 AND local_quiescence->>'vm_creation_provision_generation' IS NULL
                THEN 'agent_runtime_zero_v1' ELSE NULL END;
            expected_workspace_generation := '';
            expected_workspace_runtime := '';
        ELSIF public.pinned_vm_creation_agent_zero_source($new$;
BEGIN
    definition := pg_get_functiondef('public.enforce_pinned_thread_delete_authority()'::regprocedure);
    IF (length(definition)-length(replace(definition,old_fragment,'')))/length(old_fragment) <> 1 THEN
        RAISE EXCEPTION '0342 requires one exact pre-VM DELETE protocol branch';
    END IF;
    EXECUTE replace(definition,old_fragment,new_fragment);
END;
$migration$;

COMMIT;
