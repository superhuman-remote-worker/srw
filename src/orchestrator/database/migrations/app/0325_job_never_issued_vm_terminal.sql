-- migration:     0325_job_never_issued_vm_terminal.sql
-- description:   Admit exact cancelled Job VM logical non-issuance and proven predecessors.
-- depends-on:    0324_legacy_nonquota_resume_delete.sql
-- expected:      < 1s. Function additions/replacement only; no row rewrite.
-- locks:         Function-catalog locks; existing owner triggers remain active.
-- transactional: yes

BEGIN;
SET LOCAL lock_timeout                        = '2s';
SET LOCAL statement_timeout                   = '15min';
SET LOCAL idle_in_transaction_session_timeout = '5min';
SET LOCAL timezone                            = 'UTC';

-- Logical non-issuance is not a physical-stop receipt. Historical projection
-- remains durable after a later Resume; current mutation requires each prior
-- generation to have its own completed exact logical parent.

CREATE FUNCTION public.job_vm_creation_never_issued_evidence(
    requested_job uuid, requested_generation text
) RETURNS boolean LANGUAGE sql STABLE AS $function$
    SELECT EXISTS (
        SELECT 1 FROM public.jobs j
        JOIN public.vm_creation_retries r ON r.job_id=j.id
            AND r.owner_kind='job' AND r.provision_generation::text=requested_generation
        JOIN public.vm_resource_waiters w ON w.request_id=r.request_id
        WHERE j.id=requested_job
          AND r.origin='initial' AND r.state='settled'
          AND r.reason='creation_never_issued' AND r.resolved_at IS NOT NULL
          AND r.expected_pvc_uid IS NULL AND r.observed_pvc_uid IS NULL
          AND r.observed_vm_uid IS NULL AND r.ready_at IS NULL
          AND r.boot_counted IS FALSE AND r.claim_token IS NULL
          AND r.creation_admission_id IS NULL
          AND r.creation_carrier_uid IS NULL AND r.creation_carrier_namespace IS NULL
          AND r.disposition_carrier_uid IS NULL AND r.disposition_carrier_namespace IS NULL
          AND r.cancellation_disposition IS NULL
          AND r.cancellation_progress='{}'::jsonb
          AND r.cancellation_completion='{}'::jsonb
          AND r.predecessor_cleanup_admission_id IS NULL
          AND r.predecessor_evidence='{}'::jsonb
          AND jsonb_typeof(r.canonical_request)='object'
          AND r.canonical_request->>'entity_type'='job'
          AND r.canonical_request->>'job_id'=j.id::text
          AND r.canonical_request->>'provision_generation'=requested_generation
          AND COALESCE(r.canonical_request->'preparation','null'::jsonb)='null'::jsonb
          AND COALESCE(r.canonical_request->'workspace_storage','null'::jsonb)='null'::jsonb
          AND jsonb_typeof(r.controller_configuration)='object'
          AND r.controller_configuration->'version'='3'::jsonb
          AND w.owner_kind='job' AND w.job_id=j.id AND w.thread_id IS NULL
          AND w.provision_generation=r.provision_generation
          AND w.request_digest=r.request_digest AND w.state='cancelled'
          AND w.reason='job_cancelled'
          AND NOT EXISTS (SELECT 1 FROM public.vm_creation_effects e
              WHERE e.request_id=r.request_id)
          AND NOT EXISTS (SELECT 1 FROM public.vm_resource_reservations v
              WHERE v.request_id=r.request_id)
    );
$function$;

CREATE FUNCTION public.job_vm_creation_never_issued_terminal_source(
    requested_job uuid, requested_generation text
) RETURNS boolean LANGUAGE sql STABLE AS $function$
    SELECT public.job_vm_creation_never_issued_evidence(
        requested_job, requested_generation
    ) AND EXISTS (
        SELECT 1 FROM public.vm_workspace_cleanup_admissions parent
        WHERE parent.owner_kind='job' AND parent.owner_id=requested_job
          AND parent.source='job_terminal_vm_release'
          AND parent.parent_admission_id IS NULL AND parent.pvc_uid IS NULL
          AND parent.completed_at IS NOT NULL AND parent.outcome='completed'
          AND parent.request_id=public.uuid_generate_v5(
              '6ba7b811-9dad-11d1-80b4-00c04fd430c8'::uuid,
              'vm-workspace-cleanup:job_terminal_vm_release:job:' ||
              requested_job::text || ':' || requested_generation || ':None:'
          )
          AND parent.intent_digest='sha256:' || pg_catalog.encode(
              pg_catalog.sha256(pg_catalog.convert_to(
                  '{"owner_id":"' || requested_job::text ||
                  '","owner_kind":"job","provision_generation":"' ||
                  requested_generation ||
                  '","purge_disk":true,"pvc_uid":"","resource":"vm_workspace",' ||
                  '"source":"job_terminal_vm_release","vm_uid":""}',
                  'UTF8'
              )), 'hex'
          )
    );
$function$;

CREATE FUNCTION public.job_vm_creation_never_issued_predecessors(
    requested_job uuid, requested_generation text
) RETURNS boolean LANGUAGE sql STABLE AS $function$
    SELECT EXISTS (
        SELECT 1 FROM public.jobs j
        JOIN public.vm_creation_retries current_retry
          ON current_retry.job_id=j.id
         AND current_retry.owner_kind='job'
         AND current_retry.provision_generation::text=requested_generation
        WHERE j.id=requested_job
          AND (
            (COALESCE(j.context->'last_vm','null'::jsonb)
                IN ('null'::jsonb,'{}'::jsonb)
             AND NOT EXISTS (
                 SELECT 1 FROM public.vm_creation_retries prior
                 WHERE prior.job_id=j.id
                   AND prior.provision_generation<>current_retry.provision_generation
             ))
            OR
            (jsonb_typeof(j.context->'last_vm')='object'
             AND j.context->'last_vm'->>'provision_generation' IS NOT NULL
             AND j.context->'last_vm'->>'provision_generation'<>requested_generation
             AND j.context->'last_vm'->>'status'='deleted'
             AND j.context->'last_vm'->'identity_authenticated'='false'::jsonb
             AND j.context->'last_vm'->'provision_attempts'='0'::jsonb
             AND j.context->'last_vm'->>'identity_provision_generation' IS NULL
             AND j.context->'last_vm'->>'vm_uid' IS NULL
             AND j.context->'last_vm'->>'vmi_uid' IS NULL
             AND j.context->'last_vm'->>'active_pod_uid' IS NULL
             AND j.context->'last_vm'->>'_runtime_incarnation' IS NULL
             AND j.context->'last_vm'->>'rootdisk_pvc_uid' IS NULL
             AND j.context->'last_vm'->>'cloud_init_secret_uid' IS NULL
             AND j.context->'last_vm'->>'ssh_host' IS NULL
             AND j.context->'last_vm'->>'ssh_port' IS NULL
             AND COALESCE(j.context->'last_vm'->'preparation_request','null'::jsonb)='null'::jsonb
             AND COALESCE(j.context->'last_vm'->'preparation','null'::jsonb)='null'::jsonb
             AND COALESCE(j.context->'last_vm'->'workspace_storage','null'::jsonb)='null'::jsonb
             AND EXISTS (
                 SELECT 1 FROM public.vm_creation_retries previous_retry
                 WHERE previous_retry.job_id=j.id
                   AND previous_retry.owner_kind='job'
                   AND previous_retry.provision_generation::text=
                       j.context->'last_vm'->>'provision_generation'
                   AND previous_retry.provision_generation<>current_retry.provision_generation
                   AND previous_retry.created_at<current_retry.created_at
                   AND previous_retry.request_id=(
                       SELECT latest.request_id FROM public.vm_creation_retries latest
                       WHERE latest.job_id=j.id
                         AND latest.provision_generation<>current_retry.provision_generation
                       ORDER BY latest.created_at DESC,latest.request_id DESC LIMIT 1
                   )
                   AND j.context->'last_vm'->'creation_preflight'->>'request_id'=
                       previous_retry.request_id::text
                   AND j.context->'last_vm'->'creation_request'->>'provision_generation'=
                       previous_retry.provision_generation::text
                   AND j.context->'last_vm'->'creation_request'->'request'=
                       previous_retry.canonical_request
                   AND j.context->'last_vm'->'creation_request'->>'request_digest'=
                       previous_retry.request_digest
                   AND j.context->'last_vm'->'creation_request'->'controller_configuration'=
                       previous_retry.controller_configuration
                   AND j.context->'last_vm'->'creation_request'->>'controller_configuration_digest'=
                       previous_retry.controller_configuration_digest
                   AND public.job_vm_creation_never_issued_terminal_source(
                       j.id,previous_retry.provision_generation::text)
             )
             AND NOT EXISTS (
                 SELECT 1 FROM public.vm_creation_retries other
                 WHERE other.job_id=j.id
                   AND other.provision_generation<>current_retry.provision_generation
                   AND NOT public.job_vm_creation_never_issued_terminal_source(
                       j.id,other.provision_generation::text)
             )
             AND NOT EXISTS (
                 SELECT 1 FROM public.vm_workspace_cleanup_admissions other_parent
                 WHERE other_parent.owner_kind='job' AND other_parent.owner_id=j.id
                   AND other_parent.source='job_terminal_vm_release'
                   AND other_parent.request_id<>public.uuid_generate_v5(
                       '6ba7b811-9dad-11d1-80b4-00c04fd430c8'::uuid,
                       'vm-workspace-cleanup:job_terminal_vm_release:job:' ||
                       j.id::text || ':' || requested_generation || ':None:'
                   )
                   AND NOT EXISTS (
                       SELECT 1 FROM public.vm_creation_retries known
                       WHERE known.job_id=j.id
                         AND known.provision_generation<>
                             current_retry.provision_generation
                         AND other_parent.request_id=public.uuid_generate_v5(
                             '6ba7b811-9dad-11d1-80b4-00c04fd430c8'::uuid,
                             'vm-workspace-cleanup:job_terminal_vm_release:job:' ||
                             j.id::text || ':' ||
                             known.provision_generation::text || ':None:'
                         )
                         AND public.job_vm_creation_never_issued_terminal_source(
                             j.id,known.provision_generation::text)
                   )
             ))
          )
    );
$function$;

CREATE OR REPLACE FUNCTION public.job_vm_creation_never_issued_source(
    requested_job uuid, requested_generation text
) RETURNS boolean LANGUAGE sql STABLE AS $function$
    SELECT public.job_vm_creation_never_issued_evidence(
        requested_job, requested_generation
    ) AND EXISTS (
        SELECT 1 FROM public.jobs j
        JOIN public.vm_creation_retries r ON r.job_id=j.id
            AND r.owner_kind='job' AND r.provision_generation::text=requested_generation
        WHERE j.id=requested_job AND j.status::text='cancelled'
          AND j.execution_lane='stateless' AND j.parent_job_id IS NULL
          AND j.assigned_agent_id IS NULL
          AND j.context->'_stateless_cancel_cleanup_pending'='true'::jsonb
          AND j.context->>'_vm_creation_pending'=r.request_id::text
          AND j.context->'vm'->>'provision_generation'=requested_generation
          AND j.context->'vm'->>'status'='waiting_creation_configuration'
          AND j.context->'vm'->'provision_attempts'='0'::jsonb
          AND j.context->'vm'->'identity_authenticated'='false'::jsonb
          AND j.context->'vm'->>'identity_provision_generation' IS NULL
          AND j.context->'vm'->>'vm_uid' IS NULL
          AND j.context->'vm'->>'vmi_uid' IS NULL
          AND j.context->'vm'->>'active_pod_uid' IS NULL
          AND j.context->'vm'->>'_runtime_incarnation' IS NULL
          AND j.context->'vm'->>'rootdisk_pvc_uid' IS NULL
          AND j.context->'vm'->>'cloud_init_secret_uid' IS NULL
          AND j.context->'vm'->>'ssh_host' IS NULL
          AND j.context->'vm'->>'ssh_port' IS NULL
          AND COALESCE(j.context->'vm'->'preparation_request','null'::jsonb)='null'::jsonb
          AND COALESCE(j.context->'vm'->'preparation','null'::jsonb)='null'::jsonb
          AND COALESCE(j.context->'vm'->'workspace_storage','null'::jsonb)='null'::jsonb
          AND jsonb_typeof(j.context->'vm'->'creation_preflight')='object'
          AND j.context->'vm'->'creation_preflight'->>'state'='admitted'
          AND j.context->'vm'->'creation_preflight'->>'request_id'=r.request_id::text
          AND j.context->'vm'->'creation_preflight'->>'job_id'=j.id::text
          AND COALESCE(j.context->'vm'->'creation_preflight'->'expected_pvc_uid',
              'null'::jsonb)='null'::jsonb
          AND COALESCE(j.context->'vm'->'creation_preflight'->'predecessor_cleanup_admission_id',
              'null'::jsonb)='null'::jsonb
          AND COALESCE(j.context->'vm'->'creation_preflight'->'predecessor_evidence',
              '{}'::jsonb)='{}'::jsonb
          AND j.context->'vm'->'creation_preflight'->'request'->>'entity_type'='job'
          AND j.context->'vm'->'creation_preflight'->'request'->>'job_id'=j.id::text
          AND j.context->'vm'->'creation_preflight'->'request'->>'provision_generation'=requested_generation
          AND COALESCE(j.context->'vm'->'creation_preflight'->'request'->'preparation',
              'null'::jsonb)='null'::jsonb
          AND COALESCE(j.context->'vm'->'creation_preflight'->'request'->'workspace_storage',
              'null'::jsonb)='null'::jsonb
          AND j.context->'vm'->'creation_preflight'->>'request_digest' ~ '^sha256:[0-9a-f]{64}$'
          AND j.context->'vm'->'creation_preflight'->>'execution_id'=r.execution_id::text
          AND j.context->'vm'->'creation_preflight'->>'execution_revision'=r.execution_revision
          AND j.context->'vm'->'creation_preflight'->>'execution_generation'=r.execution_generation::text
          AND jsonb_typeof(j.context->'vm'->'creation_request')='object'
          AND j.context->'vm'->'creation_request'->'version'='1'::jsonb
          AND j.context->'vm'->'creation_request'->>'provision_generation'=requested_generation
          AND j.context->'vm'->'creation_request'->'initial_request'='true'::jsonb
          AND j.context->'vm'->'creation_request'->'issuance_authority_bound'='false'::jsonb
          AND j.context->'vm'->'creation_request'->'controller_configuration_authenticated'='true'::jsonb
          AND j.context->'vm'->'creation_request'->'request'=r.canonical_request
          AND j.context->'vm'->'creation_request'->>'request_digest'=r.request_digest
          AND j.context->'vm'->'creation_request'->'controller_configuration'=r.controller_configuration
          AND j.context->'vm'->'creation_request'->>'controller_configuration_digest'=r.controller_configuration_digest
          AND public.job_vm_creation_never_issued_predecessors(j.id,requested_generation)
          AND COALESCE(j.context->'workspace_container','null'::jsonb)='null'::jsonb
          AND COALESCE(j.context->'ide_session','null'::jsonb)='null'::jsonb
          AND NOT EXISTS (SELECT 1 FROM public.srw_execution_workspace_bindings binding
              JOIN public.vm_creation_retries r ON r.execution_id=binding.execution_id
              WHERE r.job_id=j.id AND r.provision_generation::text=requested_generation)
          AND NOT EXISTS (SELECT 1 FROM public.vm_idle_operations idle
              WHERE idle.owner_kind='job' AND idle.owner_id=j.id
                AND idle.provision_generation::text=requested_generation)
          AND NOT EXISTS (SELECT 1 FROM public.vm_idle_access_leases lease
              WHERE lease.owner_kind='job' AND lease.owner_id=j.id
                AND lease.closed_at IS NULL)
          AND NOT EXISTS (SELECT 1 FROM public.vm_workspace_recoveries recovery
              WHERE recovery.owner_kind='job' AND recovery.owner_id=j.id
                AND recovery.resolved_at IS NULL)
          AND NOT EXISTS (SELECT 1 FROM public.vm_workspace_recovery_jobs recovery_job
              WHERE recovery_job.job_id=j.id AND recovery_job.resolved_at IS NULL)
          AND NOT EXISTS (SELECT 1 FROM public.managed_repository_authorities authority
              WHERE authority.authority_kind='job' AND authority.authority_id=j.id
                AND authority.status IN ('provisioning','active','revoking'))
          AND NOT EXISTS (SELECT 1 FROM public.managed_repository_creation_intents intent
              WHERE intent.authority_kind='job' AND intent.authority_id=j.id
                AND intent.status<>'deleted')
          AND NOT EXISTS (SELECT 1 FROM public.managed_repository_workspace_creation_reservations creation
              WHERE creation.owner_kind='job' AND creation.owner_id=j.id)
          AND NOT EXISTS (SELECT 1 FROM public.managed_repository_workspace_cleanup_intents cleanup
              WHERE cleanup.owner_kind='job' AND cleanup.owner_id=j.id)
    );
$function$;

CREATE OR REPLACE FUNCTION public.managed_repository_process_zero_receipt_exists(
    requested_owner_kind text, requested_owner_id uuid,
    requested_scope text, requested_provisioner text, requested_runtime text
) RETURNS boolean LANGUAGE plpgsql STABLE AS $function$
BEGIN
    IF requested_runtime IS NULL
       OR (requested_scope = 'ide_local'
           AND requested_runtime !~ '^[0-9a-f]{64}$')
       OR (requested_scope <> 'ide_local'
           AND requested_runtime !~
           '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$')
    THEN
        RETURN false;
    END IF;
    IF EXISTS (
        SELECT 1 FROM public.managed_repository_process_zero_receipts receipt
         WHERE receipt.owner_kind = requested_owner_kind
           AND receipt.owner_id = requested_owner_id
           AND receipt.scope = requested_scope
           AND receipt.provisioner = requested_provisioner
           AND receipt.runtime_incarnation = requested_runtime
    ) THEN
        RETURN true;
    END IF;
    RETURN requested_scope = 'vm' AND requested_provisioner = 'vm'
       AND (
           (requested_owner_kind = 'thread'
            AND public.thread_vm_creation_never_issued_source(
                requested_owner_id, requested_runtime))
           OR (requested_owner_kind = 'job'
            AND public.job_vm_creation_never_issued_terminal_source(
                requested_owner_id, requested_runtime))
       );
END;
$function$;

COMMIT;
