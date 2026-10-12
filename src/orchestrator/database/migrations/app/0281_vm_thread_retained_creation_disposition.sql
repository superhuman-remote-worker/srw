-- migration: 0281_vm_thread_retained_creation_disposition.sql
-- description: Bind retained pinned-thread wake partial cleanup to its original PVC.
-- depends-on: 0280_vm_thread_cancel_carrier.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

CREATE OR REPLACE FUNCTION public.valid_vm_creation_thread_disposition_identity(
    retry public.vm_creation_retries, require_captured boolean
) RETURNS boolean LANGUAGE sql STABLE AS $$
    SELECT (retry.owner_kind='thread' AND retry.job_id IS NULL
       AND retry.thread_id IS NOT NULL
       AND retry.canonical_request->>'entity_type'='thread'
       AND retry.canonical_request->>'job_id'=retry.thread_id::text
       AND retry.canonical_request->>'provision_generation'=retry.provision_generation::text
       AND COALESCE(retry.canonical_request->'workspace_storage','null'::jsonb)='null'::jsonb
       AND retry.cancellation_disposition->>'owner_kind'='thread'
       AND retry.cancellation_disposition->>'thread_id'=retry.thread_id::text
       AND retry.cancellation_disposition->>'job_id'=retry.thread_id::text
       AND retry.cancellation_disposition->>'thread_runtime_generation'=retry.thread_runtime_generation::text
       AND retry.cancellation_disposition->>'thread_agent_id' IS NOT DISTINCT FROM retry.thread_agent_id::text
       AND retry.cancellation_disposition->>'thread_attach_token' IS NOT DISTINCT FROM retry.thread_attach_token::text
       AND retry.cancellation_disposition->>'thread_wake_operation_id' IS NOT DISTINCT FROM retry.thread_wake_operation_id::text
       AND ((retry.disposition_carrier_uid IS NULL
             AND retry.creation_carrier_uid IS NOT NULL
             AND retry.cancellation_disposition->>'carrier_uid'=retry.creation_carrier_uid::text
             AND retry.cancellation_disposition->>'namespace'=retry.creation_carrier_namespace)
            OR (retry.disposition_carrier_uid IS NOT NULL
             AND retry.creation_carrier_uid IS NULL
             AND retry.cancellation_disposition->>'carrier_kind'='thread_creation_cancel'
             AND retry.cancellation_disposition->>'carrier_uid'=retry.disposition_carrier_uid::text
             AND retry.cancellation_disposition->>'namespace'=retry.disposition_carrier_namespace))
       AND ((retry.cancellation_disposition->>'disk_policy'='purge_new_thread_disk'
             AND retry.expected_pvc_uid IS NULL)
            OR (retry.cancellation_disposition->>'disk_policy'='retain'
             AND retry.creation_carrier_uid IS NOT NULL
             AND retry.disposition_carrier_uid IS NULL
             AND retry.expected_pvc_uid IS NOT NULL
             AND retry.observed_pvc_uid=retry.expected_pvc_uid
             AND retry.thread_wake_operation_id IS NOT NULL
             AND retry.cancellation_disposition->'objects'->'rootdisk'->>'pvc_uid'=retry.expected_pvc_uid::text
             AND retry.cancellation_disposition->'source'=jsonb_build_object(
                 'kind','retained','pvc_uid',retry.expected_pvc_uid::text)
             AND EXISTS (SELECT 1 FROM public.vm_creation_effects root
                 WHERE root.request_id=retry.request_id
                   AND root.effect_kind='rootdisk' AND root.state='observed'
                   AND root.evidence->>'pvc_uid'=retry.expected_pvc_uid::text
                   AND root.evidence->>'uid'=retry.cancellation_disposition->'objects'->'rootdisk'->>'uid'
                   AND root.carrier_intent->>'thread_wake_operation_id'=retry.thread_wake_operation_id::text
                   AND root.carrier_intent->'rootdisk_source'=retry.cancellation_disposition->'source')
             AND NOT EXISTS (SELECT 1 FROM public.vm_creation_effects e
                 WHERE e.request_id=retry.request_id AND e.effect_kind='workspace_attach'
                   AND e.state<>'rejected')
             AND EXISTS (SELECT 1 FROM public.vm_idle_operations idle
                 WHERE idle.id=retry.thread_wake_operation_id
                   AND idle.owner_kind='thread' AND idle.owner_id=retry.thread_id
                   AND idle.release_kind='pinned_thread'
                   AND idle.wake_request_id=retry.request_id
                   AND idle.wake_generation=retry.provision_generation
                   AND idle.pvc_uid=retry.expected_pvc_uid
                   AND idle.stop_verified_at IS NOT NULL
                   AND idle.stop_evidence->>'retained_pvc'='true'
                   AND idle.stop_evidence->>'pvc_uid'=retry.expected_pvc_uid::text
                   AND idle.phase IN ('waking','wake_held')
                   AND idle.closed_at IS NULL)))
       AND retry.cancellation_disposition->'workspace_storage'='null'::jsonb
       AND retry.cancellation_disposition->'workspace_instance_id'='null'::jsonb
       AND EXISTS (SELECT 1 FROM public.threads t
           WHERE t.id=retry.thread_id AND t.execution_lane='pinned'
             AND t.runtime_generation=retry.thread_runtime_generation
             AND t.runtime_retirement_token IS NOT NULL
             AND t.runtime_retirement_authorized_at IS NOT NULL
             AND t.runtime_retirement_context->>'settle_status'='ended'
             AND retry.cancellation_disposition->>'retirement_token'=t.runtime_retirement_token::text
             AND t.runtime_retirement_context->'vm_creation_source'->>'request_id'=retry.request_id::text
             AND t.runtime_retirement_context->'vm_creation_source'->>'provision_generation'=retry.provision_generation::text
             AND t.runtime_retirement_context->'vm_creation_source'->>'thread_runtime_generation'=retry.thread_runtime_generation::text
             AND t.runtime_retirement_context->'vm_creation_source'->>'request_digest'=retry.request_digest
             AND t.runtime_retirement_context->'vm_creation_source'->>'controller_configuration_digest'=retry.controller_configuration_digest
             AND t.runtime_retirement_context->'vm_creation_source'->>'thread_agent_id' IS NOT DISTINCT FROM retry.thread_agent_id::text
             AND t.runtime_retirement_context->'vm_creation_source'->>'thread_attach_token' IS NOT DISTINCT FROM retry.thread_attach_token::text
             AND t.runtime_retirement_context->'vm_creation_source'->'captured_vm'->>'creation_request_id'=retry.request_id::text
             AND t.runtime_retirement_context->'vm_creation_source'->'captured_vm'->>'provision_generation'=retry.provision_generation::text
             AND COALESCE(t.runtime_retirement_context->'vm_creation_source'->'captured_vm'->'vm_uid','null'::jsonb)='null'::jsonb
             AND (COALESCE(t.runtime_retirement_context->'vm_creation_source'->'captured_vm'->'rootdisk_pvc_uid','null'::jsonb)='null'::jsonb
                  OR t.runtime_retirement_context->'vm_creation_source'->'captured_vm'->>'rootdisk_pvc_uid'=retry.observed_pvc_uid::text)
             AND (CASE WHEN require_captured THEN
                 t.metadata->'vm'=t.runtime_retirement_context->'vm_creation_source'->'captured_vm'
                 ELSE t.metadata->'vm'=t.runtime_retirement_context->'vm_creation_source'->'captured_vm'
                      OR NOT t.metadata ? 'vm' END))) IS TRUE;
$$;

COMMIT;
