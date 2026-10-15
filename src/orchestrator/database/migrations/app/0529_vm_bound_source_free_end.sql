-- migration:     0529_vm_bound_source_free_end.sql
-- description:   Permit an exact bound, created pinned VM owner to retire its
--                stopped Agent Pod before any VM creation source exists.
-- depends-on:    0528_vm_job_pinned_ready_cancel_retention.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '15min';
SET LOCAL idle_in_transaction_session_timeout = '5min';
SET LOCAL timezone = 'UTC';

-- Source-free is an assertion about durable authority, not about a missing
-- JSON key alone. Check these ledgers both before the zero receipt and after
-- Pod/claim cleanup; cleanup admissions have their own owner key and can
-- exist without a VM retry row.
CREATE FUNCTION public.pinned_vm_bound_no_vm_obligations_absent(
    p_thread uuid, p_runtime uuid, p_agent uuid, p_pod_uid text
) RETURNS boolean LANGUAGE sql STABLE AS $body$
    SELECT NOT EXISTS (SELECT 1 FROM public.vm_creation_retries r
                        WHERE r.owner_kind='thread' AND r.thread_id=p_thread)
       AND NOT EXISTS (SELECT 1 FROM public.vm_resource_waiters w
                        WHERE w.owner_kind='thread' AND w.thread_id=p_thread)
       AND NOT EXISTS (SELECT 1 FROM public.vm_thread_creation_owners o
                        WHERE o.thread_id=p_thread)
       AND NOT EXISTS (SELECT 1 FROM public.vm_thread_retained_resumes r
                        WHERE r.thread_id=p_thread)
       AND NOT EXISTS (SELECT 1 FROM public.vm_idle_operations i
                        WHERE i.owner_kind='thread' AND i.owner_id=p_thread)
       AND NOT EXISTS (SELECT 1 FROM public.vm_idle_access_leases l
                        WHERE l.owner_kind='thread' AND l.owner_id=p_thread)
       AND NOT EXISTS (SELECT 1 FROM public.vm_workspace_recoveries r
                        WHERE r.owner_kind='thread' AND r.owner_id=p_thread)
       AND NOT EXISTS (SELECT 1 FROM public.vm_resource_thread_cleanup_authorities a
                        WHERE a.thread_id=p_thread)
       AND NOT EXISTS (SELECT 1 FROM public.vm_workspace_cleanup_admissions a
                        WHERE a.owner_kind='thread' AND a.owner_id=p_thread)
       AND NOT EXISTS (SELECT 1 FROM public.thread_workspace_provision_intents i
                        WHERE i.thread_id=p_thread AND
                              (i.runtime_generation=p_runtime OR i.status IN ('planned','revoking')))
       AND NOT EXISTS (SELECT 1 FROM public.thread_agent_pod_provision_intents i
                        WHERE i.thread_id=p_thread AND i.status IN ('planned','revoking'))
       AND NOT EXISTS (SELECT 1 FROM public.srw_execution_specs e
                        JOIN public.srw_execution_workspace_bindings b ON b.execution_id=e.id
                        WHERE e.work_kind='Session' AND e.work_id=p_thread)
       AND NOT EXISTS (SELECT 1 FROM public.thread_input_deliveries d
                        WHERE d.thread_id=p_thread AND d.owner_agent_id=p_agent
                          AND d.owner_pod_uid=p_pod_uid
                          AND d.state IN ('owned','queued','admitted','settled'))
       AND NOT EXISTS (SELECT 1 FROM public.thread_control_requests c
                        WHERE c.thread_id=p_thread AND c.runtime_generation=p_runtime)
       AND NOT EXISTS (SELECT 1 FROM public.thread_permission_requests p
                        WHERE p.thread_id=p_thread AND p.status='pending')
       AND NOT EXISTS (SELECT 1 FROM public.thread_interrupt_requests i
                        WHERE i.thread_id=p_thread AND
                              (i.outcome IS NULL OR (i.outcome='applied' AND
                               NOT (COALESCE(i.result,'{}'::jsonb) ? 'consumed_input_seq'))))
       AND NOT EXISTS (SELECT 1 FROM public.run_queue q
                        WHERE q.unit_id=p_thread AND (q.state<>'done' OR q.leased_by IS NOT NULL))
       AND NOT EXISTS (SELECT 1 FROM public.completion_effects e
                        WHERE (e.scope_id=p_thread OR e.producer_id=p_thread)
                          AND (e.state<>'done' OR e.claimed_by IS NOT NULL));
$body$;

-- This predicate is intentionally distinct from the agentless
-- pinned_vm_pre_registration_no_vm_source and from issued-creation proof.
-- It runs while the exact bound actor and its protected Pod still exist.
CREATE FUNCTION public.pinned_vm_bound_no_vm_source(
    p_thread uuid, p_runtime uuid, p_token uuid
) RETURNS boolean LANGUAGE sql STABLE AS $body$
    SELECT EXISTS (
        SELECT 1 FROM public.threads t
        JOIN public.agents a ON a.id=t.agent_id AND a.thread_id=t.id
        WHERE t.id=p_thread AND t.runtime_generation=p_runtime
          AND t.runtime_retirement_token=p_token
          AND t.kind='session' AND t.execution_lane='pinned' AND t.status='created'
          AND t.runtime_authority_exposed IS TRUE
          AND t.runtime_retirement_authorized_at IS NOT NULL
          AND t.agent_id IS NOT NULL AND t.runtime_attach_token IS NOT NULL
          AND (t.control_admission_agent_id IS NULL
               OR t.control_admission_agent_id=t.agent_id)
          AND a.current_job_id IS NULL
          AND jsonb_typeof(t.runtime_retirement_context->'version')='number'
          AND t.runtime_retirement_context->>'version'='1'
          AND t.runtime_retirement_context->>'thread_id'=t.id::text
          AND t.runtime_retirement_context->>'generation'=t.runtime_generation::text
          AND t.runtime_retirement_context->>'entry_status'='created'
          AND t.runtime_retirement_context->>'settle_status'='ended'
          AND t.runtime_retirement_context->>'workspace_backend'='vm'
          AND t.runtime_retirement_context->'protected_cloud'='false'::jsonb
          AND t.runtime_retirement_context->>'protected_cloud_state'='off'
          AND (t.runtime_retirement_context->'retained_soft_workspace' IS NULL
               OR t.runtime_retirement_context->'retained_soft_workspace'='null'::jsonb)
          AND t.runtime_retirement_context->'runtime_authority_exposed'='true'::jsonb
          AND t.runtime_retirement_context->>'agent_id'=t.agent_id::text
          AND t.runtime_retirement_context->>'runtime_attach_token'=t.runtime_attach_token::text
          AND t.runtime_retirement_context->>'control_admission_agent_id'
              IS NOT DISTINCT FROM t.control_admission_agent_id::text
          AND jsonb_typeof(t.runtime_retirement_context->'agent')='object'
          AND t.runtime_retirement_context->'agent'->>'id'=a.id::text
          AND t.runtime_retirement_context->'agent'->>'thread_id'=t.id::text
          AND t.runtime_retirement_context->'agent'->>'hostname'=a.hostname
          AND t.runtime_retirement_context->'agent'->>'pod_uid'=a.pod_uid
          AND jsonb_typeof(t.runtime_retirement_context->'agent_pod')='object'
          AND t.metadata->'agent_pod'=t.runtime_retirement_context->'agent_pod'
          AND t.runtime_retirement_context->'agent_pod'->>'runtime_generation'=t.runtime_generation::text
          AND t.runtime_retirement_context->'agent_pod'->>'pod_name'=a.hostname
          AND t.runtime_retirement_context->'agent_pod'->>'pod_uid'=a.pod_uid
          AND NULLIF(t.runtime_retirement_context->'agent_pod'->>'namespace','') IS NOT NULL
          AND t.runtime_retirement_context->'agent_pod'->>'protection_protocol'='finalizer_v1'
          AND (t.runtime_retirement_context->'route' IS NULL
               OR t.runtime_retirement_context->'route' IN ('null'::jsonb,'{}'::jsonb)
               OR (jsonb_typeof(t.runtime_retirement_context->'route')='object'
                   AND t.runtime_retirement_context->'route'->>'owner_pod_uid'=a.pod_uid
                   AND t.runtime_retirement_context->'route'->>'namespace'=
                       t.runtime_retirement_context->'agent_pod'->>'namespace'))
          AND t.runtime_retirement_context->'agent_pod_provision_intent'='null'::jsonb
          AND NOT EXISTS (SELECT 1 FROM public.thread_agent_pod_provision_intents other_intent
              WHERE other_intent.thread_id=t.id
                AND other_intent.runtime_generation=t.runtime_generation
                AND other_intent.status='published'
                AND other_intent.attempt_id::text IS DISTINCT FROM
                    t.runtime_retirement_context->'agent_pod'->>'provision_attempt')
          AND (
              (t.runtime_retirement_context->'agent_pod'->>'warm_binding_protection' IS NOT NULL
               AND t.runtime_retirement_context->'agent_pod'->>'provision_attempt' IS NULL
               AND EXISTS (
                   SELECT 1 FROM public.thread_agent_warm_binding_protections w
                   WHERE w.protection_id::text=t.runtime_retirement_context->'agent_pod'->>'warm_binding_protection'
                     AND w.thread_id=t.id AND w.runtime_generation=t.runtime_generation
                     AND w.runtime_attach_token=t.runtime_attach_token AND w.agent_id=t.agent_id
                     AND w.status='bound' AND w.source IN ('attach','legacy_binding')
                     AND w.evidence_protocol='exact_live_finalizer_v1'
                     AND w.namespace=t.runtime_retirement_context->'agent_pod'->>'namespace'
                     AND w.pod_name=a.hostname AND w.pod_uid=a.pod_uid))
              OR
              (t.runtime_retirement_context->'agent_pod'->>'provision_attempt' IS NOT NULL
               AND t.runtime_retirement_context->'agent_pod'->>'warm_binding_protection' IS NULL
               AND EXISTS (
                   SELECT 1 FROM public.thread_agent_pod_provision_intents p
                   WHERE p.attempt_id::text=t.runtime_retirement_context->'agent_pod'->>'provision_attempt'
                     AND p.thread_id=t.id AND p.runtime_generation=t.runtime_generation
                     AND p.status='published' AND p.provisioner IN ('agent','persistent')
                     AND p.namespace=t.runtime_retirement_context->'agent_pod'->>'namespace'
                     AND p.pod_name=a.hostname AND p.pod_uid=a.pod_uid
                     AND p.protection_protocol='finalizer_v1')))
          AND t.runtime_retirement_context->'vm'='null'::jsonb
          AND t.runtime_retirement_context->'vm_creation_source'='null'::jsonb
          AND NOT (t.metadata ? 'vm')
          AND t.runtime_retirement_context->'workspace_binding' IN ('null'::jsonb,'{}'::jsonb)
          AND COALESCE(t.metadata->'_workspace_binding','null'::jsonb) IN ('null'::jsonb,'{}'::jsonb)
          AND (t.runtime_retirement_context->'workspace_container'='null'::jsonb
               OR (jsonb_typeof(t.runtime_retirement_context->'workspace_container')='object'
                   AND (t.runtime_retirement_context->'workspace_container')
                       - 'repo_name' - 'git_remote_url'='{}'::jsonb))
          AND (t.metadata->'workspace_container' IS NULL
               OR t.metadata->'workspace_container'='null'::jsonb
               OR (jsonb_typeof(t.metadata->'workspace_container')='object'
                   AND (t.metadata->'workspace_container')
                       - 'repo_name' - 'git_remote_url'='{}'::jsonb))
          AND t.runtime_retirement_context->'workspace_provision_intent'='null'::jsonb
          AND t.runtime_retirement_context->'protected_ro'='null'::jsonb
          AND (
              (t.runtime_retirement_context->'agent_workspace_claim'='null'::jsonb
               AND NOT EXISTS (SELECT 1 FROM public.thread_agent_workspace_claims c
                               WHERE c.thread_id=t.id AND c.status IN ('planned','ready','revoking')))
              OR
              (jsonb_typeof(t.runtime_retirement_context->'agent_workspace_claim')='object'
               AND t.runtime_retirement_context->'agent_workspace_claim'->>'status'='ready'
               AND t.runtime_retirement_context->'agent_workspace_claim'->>'thread_id'=t.id::text
               AND EXISTS (
                   SELECT 1 FROM public.thread_agent_workspace_claims c
                   JOIN public.thread_agent_pod_provision_intents p ON p.workspace_claim_id=c.claim_id
                   WHERE c.thread_id=t.id
                     AND c.claim_id::text=t.runtime_retirement_context->'agent_workspace_claim'->>'claim_id'
                     AND c.created_runtime_generation::text=t.runtime_retirement_context->'agent_workspace_claim'->>'created_runtime_generation'
                     AND c.create_attempt::text=t.runtime_retirement_context->'agent_workspace_claim'->>'create_attempt'
                     AND c.provisioner=t.runtime_retirement_context->'agent_workspace_claim'->>'provisioner'
                     AND c.provisioner IN ('agent','persistent')
                     AND c.pvc_name=t.runtime_retirement_context->'agent_workspace_claim'->>'pvc_name'
                     AND c.pvc_uid=t.runtime_retirement_context->'agent_workspace_claim'->>'pvc_uid'
                     AND NULLIF(c.pvc_uid,'') IS NOT NULL
                     AND c.status='ready' AND c.fenced_at IS NULL AND c.gc_after IS NULL
                     AND c.namespace=t.runtime_retirement_context->'agent_workspace_claim'->>'namespace'
                     AND c.namespace=t.runtime_retirement_context->'agent_pod'->>'namespace'
                     AND c.protection_protocol='finalizer_v1'
                     AND c.protection_protocol=t.runtime_retirement_context->'agent_workspace_claim'->>'protection_protocol'
                     AND p.thread_id=t.id AND p.runtime_generation=t.runtime_generation
                     AND p.attempt_id::text=t.runtime_retirement_context->'agent_pod'->>'provision_attempt'
                     AND p.status='published' AND p.pod_name=a.hostname AND p.pod_uid=a.pod_uid
                     AND p.namespace=c.namespace AND p.provisioner=c.provisioner
                     AND p.protection_protocol='finalizer_v1')
               AND NOT EXISTS (SELECT 1 FROM public.thread_agent_workspace_claims other
                               WHERE other.thread_id=t.id AND other.status IN ('planned','ready','revoking')
                                 AND other.claim_id::text IS DISTINCT FROM
                                     t.runtime_retirement_context->'agent_workspace_claim'->>'claim_id')))
          AND NOT EXISTS (SELECT 1 FROM public.agents other
                          WHERE other.id<>a.id AND (other.thread_id=t.id
                              OR other.hostname=a.hostname OR other.pod_uid=a.pod_uid))
          AND public.pinned_vm_bound_no_vm_obligations_absent(t.id,t.runtime_generation,t.agent_id,a.pod_uid)
    );
$body$;

-- Permanent cleanup clears metadata.agent_pod and detaches agents.thread_id
-- before inserting its outcome. Recheck immutable frozen authority, the
-- receipt which only the live-bound UPDATE branch could append, and today's
-- debt. The exact outcome exists at DELETE but not yet at outcome INSERT.
CREATE FUNCTION public.pinned_vm_bound_no_vm_source_terminal(
    p_thread uuid, p_runtime uuid, p_token uuid, require_outcome boolean
) RETURNS boolean LANGUAGE sql STABLE AS $body$
    SELECT EXISTS (
        SELECT 1 FROM public.threads t
        JOIN public.agents a ON a.id=t.agent_id AND a.thread_id IS NULL
        WHERE t.id=p_thread AND t.runtime_generation=p_runtime
          AND t.runtime_retirement_token=p_token
          AND t.kind='session' AND t.execution_lane='pinned' AND t.status='created'
          AND t.runtime_authority_exposed IS TRUE
          AND t.runtime_retirement_permanent IS TRUE
          AND t.runtime_retirement_authorized_at IS NOT NULL
          AND t.agent_id IS NOT NULL AND t.runtime_attach_token IS NOT NULL
          AND (t.control_admission_agent_id IS NULL
               OR t.control_admission_agent_id=t.agent_id)
          AND a.current_job_id IS NULL
          AND jsonb_typeof(t.runtime_retirement_context->'version')='number'
          AND t.runtime_retirement_context->>'version'='1'
          AND t.runtime_retirement_context->>'thread_id'=t.id::text
          AND t.runtime_retirement_context->>'generation'=t.runtime_generation::text
          AND t.runtime_retirement_context->>'entry_status'='created'
          AND t.runtime_retirement_context->>'settle_status'='ended'
          AND t.runtime_retirement_context->>'workspace_backend'='vm'
          AND t.runtime_retirement_context->'protected_cloud'='false'::jsonb
          AND t.runtime_retirement_context->>'protected_cloud_state'='off'
          AND (t.runtime_retirement_context->'retained_soft_workspace' IS NULL
               OR t.runtime_retirement_context->'retained_soft_workspace'='null'::jsonb)
          AND t.runtime_retirement_context->'runtime_authority_exposed'='true'::jsonb
          AND t.runtime_retirement_context->>'agent_id'=t.agent_id::text
          AND t.runtime_retirement_context->>'runtime_attach_token'=t.runtime_attach_token::text
          AND t.runtime_retirement_context->>'control_admission_agent_id'
              IS NOT DISTINCT FROM t.control_admission_agent_id::text
          AND jsonb_typeof(t.runtime_retirement_context->'agent')='object'
          AND t.runtime_retirement_context->'agent'->>'id'=a.id::text
          AND t.runtime_retirement_context->'agent'->>'thread_id'=t.id::text
          AND t.runtime_retirement_context->'agent'->>'hostname'=a.hostname
          AND t.runtime_retirement_context->'agent'->>'pod_uid'=a.pod_uid
          AND jsonb_typeof(t.runtime_retirement_context->'agent_pod')='object'
          AND COALESCE(t.metadata->'agent_pod','null'::jsonb) IN ('null'::jsonb,'{}'::jsonb)
          AND t.runtime_retirement_context->'agent_pod'->>'runtime_generation'=t.runtime_generation::text
          AND t.runtime_retirement_context->'agent_pod'->>'pod_name'=a.hostname
          AND t.runtime_retirement_context->'agent_pod'->>'pod_uid'=a.pod_uid
          AND NULLIF(t.runtime_retirement_context->'agent_pod'->>'namespace','') IS NOT NULL
          AND t.runtime_retirement_context->'agent_pod'->>'protection_protocol'='finalizer_v1'
          AND (t.runtime_retirement_context->'route' IS NULL
               OR t.runtime_retirement_context->'route' IN ('null'::jsonb,'{}'::jsonb)
               OR (jsonb_typeof(t.runtime_retirement_context->'route')='object'
                   AND t.runtime_retirement_context->'route'->>'owner_pod_uid'=a.pod_uid
                   AND t.runtime_retirement_context->'route'->>'namespace'=
                       t.runtime_retirement_context->'agent_pod'->>'namespace'))
          AND t.runtime_retirement_context->'agent_pod_provision_intent'='null'::jsonb
          AND NOT EXISTS (SELECT 1 FROM public.thread_agent_pod_provision_intents other_intent
              WHERE other_intent.thread_id=t.id
                AND other_intent.runtime_generation=t.runtime_generation
                AND other_intent.status='published'
                AND other_intent.attempt_id::text IS DISTINCT FROM
                    t.runtime_retirement_context->'agent_pod'->>'provision_attempt')
          AND (
              (t.runtime_retirement_context->'agent_pod'->>'warm_binding_protection' IS NOT NULL
               AND t.runtime_retirement_context->'agent_pod'->>'provision_attempt' IS NULL
               AND EXISTS (SELECT 1 FROM public.thread_agent_warm_binding_protections w
                   WHERE w.protection_id::text=t.runtime_retirement_context->'agent_pod'->>'warm_binding_protection'
                     AND w.thread_id=t.id AND w.runtime_generation=t.runtime_generation
                     AND w.runtime_attach_token=t.runtime_attach_token AND w.agent_id=t.agent_id
                     AND w.status IN ('bound','terminal_release')
                     AND w.source IN ('attach','legacy_binding')
                     AND w.evidence_protocol='exact_live_finalizer_v1'
                     AND w.namespace=t.runtime_retirement_context->'agent_pod'->>'namespace'
                     AND w.pod_name=a.hostname AND w.pod_uid=a.pod_uid))
              OR
              (t.runtime_retirement_context->'agent_pod'->>'provision_attempt' IS NOT NULL
               AND t.runtime_retirement_context->'agent_pod'->>'warm_binding_protection' IS NULL
               AND EXISTS (SELECT 1 FROM public.thread_agent_pod_provision_intents p
                   WHERE p.attempt_id::text=t.runtime_retirement_context->'agent_pod'->>'provision_attempt'
                     AND p.thread_id=t.id AND p.runtime_generation=t.runtime_generation
                     AND p.status IN ('published','fenced','retired')
                     AND p.provisioner IN ('agent','persistent')
                     AND p.namespace=t.runtime_retirement_context->'agent_pod'->>'namespace'
                     AND p.pod_name=a.hostname AND p.pod_uid=a.pod_uid
                     AND p.protection_protocol='finalizer_v1')))
          AND t.runtime_retirement_context->'vm'='null'::jsonb
          AND t.runtime_retirement_context->'vm_creation_source'='null'::jsonb
          AND NOT (t.metadata ? 'vm')
          AND jsonb_typeof(t.metadata->'workspace_container')='object'
          AND ((t.metadata->'workspace_container') - 'repo_name' - 'git_remote_url'
                   = '{}'::jsonb
               OR (t.metadata->'workspace_container') - 'repo_name' - 'git_remote_url'
                   = jsonb_build_object(
                       'status','deleted',
                       'pod_ip',NULL,'pod_name',NULL,'host',NULL,'port',NULL,
                       'ide_host',NULL,'ide_port',NULL,
                       '_canvas_workspace_generation',NULL,
                       '_runtime_incarnation',NULL,
                       '_docker_workspace_lease_id',NULL))
          AND t.runtime_retirement_context->'workspace_binding' IN ('null'::jsonb,'{}'::jsonb)
          AND COALESCE(t.metadata->'_workspace_binding','null'::jsonb) IN ('null'::jsonb,'{}'::jsonb)
          AND (t.runtime_retirement_context->'workspace_container'='null'::jsonb
               OR (jsonb_typeof(t.runtime_retirement_context->'workspace_container')='object'
                   AND (t.runtime_retirement_context->'workspace_container')
                       - 'repo_name' - 'git_remote_url'='{}'::jsonb))
          AND t.runtime_retirement_context->'workspace_provision_intent'='null'::jsonb
          AND t.runtime_retirement_context->'protected_ro'='null'::jsonb
          AND ((t.runtime_retirement_context->'agent_workspace_claim'='null'::jsonb
                AND NOT EXISTS (SELECT 1 FROM public.thread_agent_workspace_claims c
                    WHERE c.thread_id=t.id AND c.status IN ('planned','ready','revoking','fenced')))
               OR (jsonb_typeof(t.runtime_retirement_context->'agent_workspace_claim')='object'
                   AND t.runtime_retirement_context->'agent_workspace_claim'->>'status'='ready'
                   AND EXISTS (SELECT 1 FROM public.thread_agent_workspace_claims c
                       JOIN public.thread_agent_pod_provision_intents p ON p.workspace_claim_id=c.claim_id
                       WHERE c.thread_id=t.id
                         AND c.claim_id::text=t.runtime_retirement_context->'agent_workspace_claim'->>'claim_id'
                         AND c.created_runtime_generation::text=t.runtime_retirement_context->'agent_workspace_claim'->>'created_runtime_generation'
                         AND c.create_attempt::text=t.runtime_retirement_context->'agent_workspace_claim'->>'create_attempt'
                         AND c.provisioner=t.runtime_retirement_context->'agent_workspace_claim'->>'provisioner'
                         AND c.pvc_name=t.runtime_retirement_context->'agent_workspace_claim'->>'pvc_name'
                         AND c.namespace=t.runtime_retirement_context->'agent_workspace_claim'->>'namespace'
                         AND c.protection_protocol='finalizer_v1'
                         AND c.status IN ('ready','fenced','reclaimed')
                         AND (c.status<>'ready' OR c.pvc_uid=t.runtime_retirement_context->'agent_workspace_claim'->>'pvc_uid')
                         AND (c.status='ready' OR c.fenced_at IS NOT NULL)
                         AND p.thread_id=t.id AND p.runtime_generation=t.runtime_generation
                         AND p.attempt_id::text=t.runtime_retirement_context->'agent_pod'->>'provision_attempt'
                         AND p.status IN ('published','fenced','retired')
                         AND p.namespace=c.namespace AND p.provisioner=c.provisioner
                         AND p.pod_name=a.hostname AND p.pod_uid=a.pod_uid
                         AND p.protection_protocol='finalizer_v1')
                   AND NOT EXISTS (SELECT 1 FROM public.thread_agent_workspace_claims other
                       WHERE other.thread_id=t.id AND other.status IN ('planned','ready','revoking','fenced')
                         AND other.claim_id::text IS DISTINCT FROM
                             t.runtime_retirement_context->'agent_workspace_claim'->>'claim_id')))
          AND NOT EXISTS (SELECT 1 FROM public.agents other
              WHERE other.id<>a.id AND (other.thread_id=t.id OR other.hostname=a.hostname
                                        OR other.pod_uid=a.pod_uid))
          AND public.pinned_vm_bound_no_vm_obligations_absent(t.id,t.runtime_generation,t.agent_id,a.pod_uid)
          AND public.pinned_retirement_external_resources_absent(t.id,t.metadata)
          AND t.runtime_retirement_external_cleanup IS NOT NULL
          AND t.runtime_retirement_external_cleanup IS NOT DISTINCT FROM
              public.pinned_retirement_external_cleanup_expected(
                  t.runtime_retirement_context,t.runtime_generation,t.runtime_retirement_token)
          AND t.runtime_retirement_local_quiescence IS NOT NULL
          AND t.runtime_retirement_local_quiescence->>'version'='1'
          AND t.runtime_retirement_local_quiescence->>'runtime_generation'=t.runtime_generation::text
          AND t.runtime_retirement_local_quiescence->>'retirement_token'=t.runtime_retirement_token::text
          AND t.runtime_retirement_local_quiescence->>'agent_id'=t.agent_id::text
          AND t.runtime_retirement_local_quiescence->>'runtime_attach_token'=t.runtime_attach_token::text
          AND t.runtime_retirement_local_quiescence->>'settle_status'='ended'
          AND t.runtime_retirement_local_quiescence->>'quiescence_actor'='orchestrator'
          AND t.runtime_retirement_local_quiescence->>'quiescence_protocol'='agent_runtime_zero_v1'
          AND t.runtime_retirement_local_quiescence->>'agent_pod_name'=a.hostname
          AND t.runtime_retirement_local_quiescence->>'agent_pod_uid'=a.pod_uid
          AND t.runtime_retirement_local_quiescence->>'workspace_generation' IS NULL
          AND t.runtime_retirement_local_quiescence->>'workspace_runtime_incarnation' IS NULL
          AND t.runtime_retirement_local_quiescence->>'vm_creation_request_id' IS NULL
          AND t.runtime_retirement_local_quiescence->>'vm_creation_provision_generation' IS NULL
          AND (NOT require_outcome OR EXISTS (
              SELECT 1 FROM public.thread_runtime_retirement_outcomes o
              WHERE o.thread_id=t.id AND o.runtime_generation=t.runtime_generation
                AND o.retirement_token=t.runtime_retirement_token
                AND o.disposition='ended' AND o.permanent AND o.outcome='deleted'
                AND o.agent_id=t.agent_id AND o.runtime_attach_token=t.runtime_attach_token))
    );
$body$;

-- Preserve the existing shape/identity checks and select the agent-zero
-- protocol only for the exact live bound source-free owner.
DO $migration$
DECLARE
    definition text;
    old_fragment text := $old$                    WHEN public.pinned_vm_pre_registration_no_vm_source($old$;
    new_fragment text := $new$                    WHEN public.pinned_vm_bound_no_vm_source(
                        NEW.id,NEW.runtime_generation,NEW.runtime_retirement_token)
                    THEN NEW.runtime_retirement_local_quiescence->>'quiescence_protocol'='agent_runtime_zero_v1'
                         AND NEW.runtime_retirement_local_quiescence->>'quiescence_actor'='orchestrator'
                         AND NEW.runtime_retirement_local_quiescence->>'agent_pod_name'=
                             NEW.runtime_retirement_context->'agent_pod'->>'pod_name'
                         AND NEW.runtime_retirement_local_quiescence->>'agent_pod_uid'=
                             NEW.runtime_retirement_context->'agent_pod'->>'pod_uid'
                         AND NEW.runtime_retirement_local_quiescence->>'workspace_generation' IS NULL
                         AND NEW.runtime_retirement_local_quiescence->>'workspace_runtime_incarnation' IS NULL
                         AND NEW.runtime_retirement_local_quiescence->>'vm_creation_request_id' IS NULL
                         AND NEW.runtime_retirement_local_quiescence->>'vm_creation_provision_generation' IS NULL
                    WHEN public.pinned_vm_pre_registration_no_vm_source($new$;
BEGIN
    definition := pg_get_functiondef('public.enforce_thread_ended_transition()'::regprocedure);
    IF (length(definition)-length(replace(definition,old_fragment,'')))/length(old_fragment) <> 1 THEN
        RAISE EXCEPTION '0529 requires one exact bound VM UPDATE protocol branch';
    END IF;
    EXECUTE replace(definition,old_fragment,new_fragment);
END;
$migration$;

-- The direct permanent transaction has already detached the Agent and
-- inserted its exact outcome when DELETE runs. No live-owner predicate is
-- valid on this edge; the receipt was validated at its earlier UPDATE.
DO $migration$
DECLARE
    definition text;
    old_fragment text := $old$        ELSIF public.pinned_vm_pre_registration_no_vm_source($old$;
    new_fragment text := $new$        ELSIF public.pinned_vm_bound_no_vm_source_terminal(
            OLD.id,OLD.runtime_generation,OLD.runtime_retirement_token,true) THEN
            expected_protocol := 'agent_runtime_zero_v1';
            expected_workspace_generation := '';
            expected_workspace_runtime := '';
        ELSIF public.pinned_vm_pre_registration_no_vm_source($new$;
BEGIN
    definition := pg_get_functiondef('public.enforce_pinned_thread_delete_authority()'::regprocedure);
    IF (length(definition)-length(replace(definition,old_fragment,'')))/length(old_fragment) <> 1 THEN
        RAISE EXCEPTION '0529 requires one exact bound VM DELETE protocol branch';
    END IF;
    EXECUTE replace(definition,old_fragment,new_fragment);
END;
$migration$;

-- Outcome INSERT occurs after the Agent detach and before DELETE. Its
-- append-only row is not yet visible to the terminal predicate, so require
-- all other exact proof here and require the row itself at DELETE.
DO $migration$
DECLARE
    definition text;
    old_fragment text := $old$       OR (NEW.permanent AND (
            NEW.disposition <> 'ended'$old$;
    new_fragment text := $new$       OR (NEW.permanent AND thread_row.status='created'
           AND retirement_context->>'workspace_backend'='vm'
           AND retirement_context->>'agent_id' IS NOT NULL
           AND retirement_context->'vm'='null'::jsonb
           AND retirement_context->'vm_creation_source'='null'::jsonb
           AND NOT public.pinned_vm_bound_no_vm_source_terminal(
               NEW.thread_id,NEW.runtime_generation,NEW.retirement_token,false))
       OR (NEW.permanent AND (
            NEW.disposition <> 'ended'$new$;
BEGIN
    definition := pg_get_functiondef('public.enforce_thread_runtime_retirement_outcome_insert()'::regprocedure);
    IF (length(definition)-length(replace(definition,old_fragment,'')))/length(old_fragment) <> 1 THEN
        RAISE EXCEPTION '0529 requires one exact bound VM outcome branch';
    END IF;
    EXECUTE replace(definition,old_fragment,new_fragment);
END;
$migration$;

COMMIT;
