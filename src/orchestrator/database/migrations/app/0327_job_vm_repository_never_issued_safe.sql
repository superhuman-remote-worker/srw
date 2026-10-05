-- migration:     0327_job_vm_repository_never_issued_safe.sql
-- description:   Admit exact undelivered own repository pair for cancelled VM Jobs.
-- depends-on:    0326_job_never_issued_vm_terminal.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

-- Source INSERTs serialize with the Job row locked by logical settlement.
-- UPDATE guards must never take this parent lock after their child row lock.
CREATE FUNCTION public.lock_job_repository_source_insert()
RETURNS trigger LANGUAGE plpgsql AS $guard$
BEGIN
    IF NEW.authority_kind='job' THEN
        PERFORM 1 FROM public.jobs WHERE id=NEW.authority_id FOR SHARE;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'managed repository Job owner is absent'
                USING ERRCODE='23514';
        END IF;
    END IF;
    RETURN NEW;
END;
$guard$;
CREATE TRIGGER a_job_repository_source_insert
BEFORE INSERT ON public.managed_repository_authorities
FOR EACH ROW EXECUTE FUNCTION public.lock_job_repository_source_insert();
CREATE TRIGGER a_job_repository_source_insert
BEFORE INSERT ON public.managed_repository_creation_intents
FOR EACH ROW EXECUTE FUNCTION public.lock_job_repository_source_insert();

-- These append-only writers have no direct Job FK. Their first INSERT must
-- join the same owner serialization as the terminal projection.
CREATE FUNCTION public.lock_job_vm_nondelivery_insert()
RETURNS trigger LANGUAGE plpgsql AS $guard$
DECLARE owned_job uuid;
BEGIN
    IF TG_TABLE_NAME='managed_repository_process_zero_receipts'
       OR TG_TABLE_NAME='managed_repository_workspace_creation_reservations'
       OR TG_TABLE_NAME='managed_repository_workspace_cleanup_intents' THEN
        IF NEW.owner_kind='job' THEN owned_job := NEW.owner_id; END IF;
    ELSIF TG_TABLE_NAME='srw_execution_specs' THEN
        IF NEW.work_kind='Job' THEN owned_job := NEW.work_id; END IF;
    ELSE
        SELECT work_id INTO owned_job FROM public.srw_execution_specs
        WHERE id=NEW.execution_id AND work_kind='Job';
    END IF;
    IF owned_job IS NOT NULL THEN
        PERFORM 1 FROM public.jobs WHERE id=owned_job FOR SHARE;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'VM Job non-delivery source requires live owner'
                USING ERRCODE='23514';
        END IF;
    END IF;
    RETURN NEW;
END;
$guard$;
CREATE TRIGGER a_job_vm_nondelivery_insert
BEFORE INSERT ON public.managed_repository_process_zero_receipts
FOR EACH ROW EXECUTE FUNCTION public.lock_job_vm_nondelivery_insert();
CREATE TRIGGER a_job_vm_nondelivery_insert
BEFORE INSERT ON public.managed_repository_workspace_creation_reservations
FOR EACH ROW EXECUTE FUNCTION public.lock_job_vm_nondelivery_insert();
CREATE TRIGGER a_job_vm_nondelivery_insert
BEFORE INSERT ON public.managed_repository_workspace_cleanup_intents
FOR EACH ROW EXECUTE FUNCTION public.lock_job_vm_nondelivery_insert();
CREATE TRIGGER a_job_vm_nondelivery_insert
BEFORE INSERT ON public.srw_execution_specs
FOR EACH ROW EXECUTE FUNCTION public.lock_job_vm_nondelivery_insert();
CREATE TRIGGER a_job_vm_nondelivery_insert
BEFORE INSERT ON public.srw_execution_attempts
FOR EACH ROW EXECUTE FUNCTION public.lock_job_vm_nondelivery_insert();
CREATE TRIGGER a_job_vm_nondelivery_insert
BEFORE INSERT ON public.srw_execution_spec_revisions
FOR EACH ROW EXECUTE FUNCTION public.lock_job_vm_nondelivery_insert();
CREATE TRIGGER a_job_vm_nondelivery_insert
BEFORE INSERT ON public.srw_execution_workspace_bindings
FOR EACH ROW EXECUTE FUNCTION public.lock_job_vm_nondelivery_insert();
CREATE TRIGGER a_job_vm_nondelivery_insert
BEFORE INSERT ON public.srw_workspace_instances
FOR EACH ROW EXECUTE FUNCTION public.lock_job_vm_nondelivery_insert();

-- Repository creation and deploy-key rotation count independently. No
-- credential ciphertext is copied into this proof or terminal packet.
CREATE FUNCTION public.job_vm_never_issued_repository_safe(requested_job uuid)
RETURNS boolean LANGUAGE sql STABLE AS $predicate$
    SELECT EXISTS (
        SELECT 1 FROM public.jobs j
        WHERE j.id=requested_job AND j.parent_job_id IS NULL
          AND j.assigned_agent_id IS NULL
          AND NOT EXISTS (SELECT 1 FROM public.worker_batch_attempts batch
              WHERE batch.job_id=j.id AND (batch.bundle_authorized_at IS NOT NULL
                  OR batch.authority_digest IS NOT NULL))
          AND NOT EXISTS (SELECT 1 FROM public.srw_execution_workspace_bindings b
              JOIN public.srw_execution_specs x ON x.id=b.execution_id
              WHERE x.work_kind='Job' AND x.work_id=j.id)
          AND NOT EXISTS (SELECT 1 FROM public.srw_execution_attempts attempt
              JOIN public.srw_execution_specs x ON x.id=attempt.execution_id
              WHERE x.work_kind='Job' AND x.work_id=j.id)
          AND NOT EXISTS (SELECT 1 FROM public.srw_workspace_instances instance
              WHERE instance.owner_id=j.id OR instance.execution_id IN (
                  SELECT x.id FROM public.srw_execution_specs x
                  WHERE x.work_kind='Job' AND x.work_id=j.id))
          AND NOT EXISTS (SELECT 1 FROM public.managed_repository_workspace_creation_reservations reservation
              WHERE reservation.owner_kind='job' AND reservation.owner_id=j.id)
          AND NOT EXISTS (SELECT 1 FROM public.managed_repository_workspace_cleanup_intents cleanup
              WHERE cleanup.owner_kind='job' AND cleanup.owner_id=j.id)
          AND NOT EXISTS (SELECT 1 FROM public.managed_repository_process_zero_receipts receipt
              WHERE receipt.owner_kind='job' AND receipt.owner_id=j.id)
          AND NOT EXISTS (SELECT 1 FROM public.vm_creation_retries r
              WHERE r.job_id=j.id AND (
                  r.owner_kind<>'job' OR r.state<>'settled' OR r.ready_at IS NOT NULL
                  OR r.expected_pvc_uid IS NOT NULL OR r.observed_pvc_uid IS NOT NULL
                  OR r.observed_vm_uid IS NOT NULL OR r.creation_admission_id IS NOT NULL
                  OR r.creation_carrier_uid IS NOT NULL OR r.disposition_carrier_uid IS NOT NULL
                  OR COALESCE(r.canonical_request->'preparation','null'::jsonb) <> 'null'::jsonb
                  OR COALESCE(r.canonical_request->'workspace_storage','null'::jsonb) <> 'null'::jsonb))
          AND (
              (j.repo_name IS NULL AND j.context->>'git_remote_url' IS NULL
               AND NOT EXISTS (SELECT 1 FROM public.managed_repository_authorities a
                   WHERE a.authority_kind='job' AND a.authority_id=j.id
                     AND a.status IN ('provisioning','active','revoking'))
               AND NOT EXISTS (SELECT 1 FROM public.managed_repository_creation_intents i
                   WHERE i.authority_kind='job' AND i.authority_id=j.id
                     AND i.status<>'deleted'))
              OR EXISTS (
                  SELECT 1 FROM public.managed_repository_authorities a
                  JOIN public.managed_repository_creation_intents i
                    ON i.id=a.creation_intent_id
                  WHERE a.authority_kind='job' AND a.authority_id=j.id
                    AND i.authority_kind='job' AND i.authority_id=j.id
                    AND a.status='active' AND i.status='created'
                    AND a.forge_key_id>0 AND a.activated_at IS NOT NULL
                    AND i.repository_created_at IS NOT NULL
                    AND a.access_mode='write' AND i.access_mode='write'
                    AND a.generation>0 AND i.generation>0
                    AND a.repository_owner=i.repository_owner
                    AND a.repo_name=i.repo_name
                    AND a.project_id IS NOT DISTINCT FROM i.project_id
                    AND a.project_id IS NOT DISTINCT FROM j.project_id
                    AND a.repo_name=j.repo_name
                    AND a.repo_name='job-' || left(j.id::text,8)
                    AND a.clean_repo_url=j.context->>'git_remote_url'
                    AND NOT public.managed_repository_url_has_userinfo(a.clean_repo_url)
                    AND right(a.clean_repo_url,length('/' || a.repository_owner ||
                        '/' || a.repo_name || '.git'))='/' || a.repository_owner ||
                        '/' || a.repo_name || '.git'
                    AND (SELECT count(*) FROM public.managed_repository_authorities other
                         WHERE other.authority_kind='job' AND other.authority_id=j.id
                           AND other.status IN ('provisioning','active','revoking'))=1
                    AND (SELECT count(*) FROM public.managed_repository_creation_intents other
                         WHERE other.authority_kind='job' AND other.authority_id=j.id
                           AND other.status<>'deleted')=1
              )
          )
    );
$predicate$;

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
          AND public.job_vm_never_issued_repository_safe(j.id)
          AND NOT EXISTS (SELECT 1 FROM public.managed_repository_workspace_creation_reservations creation
              WHERE creation.owner_kind='job' AND creation.owner_id=j.id)
          AND NOT EXISTS (SELECT 1 FROM public.managed_repository_workspace_cleanup_intents cleanup
              WHERE cleanup.owner_kind='job' AND cleanup.owner_id=j.id)
    );
$function$;

COMMIT;
