-- migration: 0282_vm_job_cancel_carrier.sql
-- description: Fence cancellation-only Job Leases without granting VM effects.
-- depends-on: 0281_vm_thread_retained_creation_disposition.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

-- The old constraint implies the replacement for every existing row. NOT VALID
-- avoids a table scan under this migration's lock; new/updated rows are checked.
ALTER TABLE public.vm_creation_retries
    DROP CONSTRAINT vm_thread_cancel_carrier_exclusive,
    ADD CONSTRAINT vm_cancel_carrier_exclusive CHECK (
        disposition_carrier_uid IS NULL OR
        (owner_kind IN ('thread', 'job') AND creation_carrier_uid IS NULL)
    ) NOT VALID;

CREATE FUNCTION public.valid_vm_creation_job_cancel_identity(
    retry public.vm_creation_retries
) RETURNS boolean LANGUAGE sql STABLE AS $$
    SELECT (retry.owner_kind='job' AND retry.job_id IS NOT NULL
       AND retry.thread_id IS NULL
       AND retry.creation_carrier_uid IS NULL
       AND retry.expected_pvc_uid IS NULL
       AND retry.observed_pvc_uid IS NULL
       AND retry.observed_vm_uid IS NULL
       AND retry.controller_configuration->>'version'='3'
       AND jsonb_typeof(retry.controller_configuration->'resource_admission')='object'
       AND retry.canonical_request->>'entity_type'='job'
       AND retry.canonical_request->>'job_id'=retry.job_id::text
       AND retry.canonical_request->>'provision_generation'=retry.provision_generation::text
       AND retry.cancellation_disposition->>'carrier_kind'='job_creation_cancel'
       AND retry.cancellation_disposition->>'request_id'=retry.request_id::text
       AND retry.cancellation_disposition->>'admission_id'=retry.creation_admission_id::text
       AND retry.cancellation_disposition->>'job_id'=retry.job_id::text
       AND retry.cancellation_disposition->>'provision_generation'=retry.provision_generation::text
       AND retry.cancellation_disposition->>'carrier_uid'=retry.disposition_carrier_uid::text
       AND retry.cancellation_disposition->>'namespace'=retry.disposition_carrier_namespace
       AND retry.disposition_carrier_namespace=retry.controller_configuration->>'namespace'
       AND retry.cancellation_disposition->'effects'='[]'::jsonb
       AND retry.cancellation_disposition->'objects'='{}'::jsonb
       AND retry.cancellation_disposition->'source'='null'::jsonb
       AND retry.cancellation_disposition->>'source_resolution'=(CASE
           WHEN COALESCE(retry.canonical_request->'preparation','null'::jsonb)<>'null'::jsonb
             OR retry.controller_configuration->'golden_enabled' IS DISTINCT FROM 'false'::jsonb
           THEN 'unknown' ELSE 'not_required' END)
       AND NOT EXISTS (SELECT 1 FROM public.vm_creation_effects e
           WHERE e.request_id=retry.request_id)
       AND EXISTS (SELECT 1 FROM public.jobs j
           WHERE j.id=retry.job_id AND j.status::text='cancelled'
             AND j.context->'vm'->>'provision_generation'=retry.provision_generation::text)
       AND EXISTS (SELECT 1 FROM public.vm_workspace_cleanup_admissions a
           WHERE a.id=retry.creation_admission_id
             AND a.source='controller_vm_create'
             AND a.owner_kind='job' AND a.owner_id=retry.job_id
             AND a.request_id=uuid_generate_v5(uuid_ns_url(),
                 'vm-create:' || retry.request_id::text)
             AND a.pvc_uid IS NULL)
       AND EXISTS (SELECT 1 FROM public.vm_resource_reservations r
           JOIN public.vm_resource_waiters w ON w.request_id=r.request_id
           WHERE r.request_id=retry.request_id
             AND r.resource_version=2 AND r.vm_uid IS NULL
             AND w.owner_kind='job' AND w.job_id=retry.job_id
             AND w.provision_generation=retry.provision_generation
             AND r.cluster_id=w.cluster_id
             AND r.policy_digest=w.policy_digest
             AND r.cluster_id=retry.controller_configuration->'resource_admission'->>'cluster_id'
             AND r.policy_digest=retry.controller_configuration->'resource_admission'->>'policy_digest'
             AND ((retry.state='cancel_requested'
                   AND r.state='reserved' AND w.state='admitted')
                  OR (retry.state='settled'
                   AND r.state='released' AND w.state='released'
                   AND r.release_evidence->>'disposition_id'=retry.cancellation_disposition->>'disposition_id')))
    ) IS TRUE;
$$;

CREATE OR REPLACE FUNCTION public.guard_vm_creation_disposition() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='INSERT' THEN
        IF NEW.cancellation_disposition IS NOT NULL OR NEW.cancellation_progress<>'{}'::jsonb
           OR NEW.disposition_carrier_uid IS NOT NULL THEN
            RAISE EXCEPTION 'Creation disposition requires locked cancellation' USING ERRCODE='23514';
        END IF;
        RETURN NEW;
    END IF;
    IF OLD.disposition_carrier_uid IS NOT NULL AND (
       NEW.disposition_carrier_uid IS DISTINCT FROM OLD.disposition_carrier_uid OR
       NEW.disposition_carrier_namespace IS DISTINCT FROM OLD.disposition_carrier_namespace) THEN
        RAISE EXCEPTION 'Cancellation carrier is immutable' USING ERRCODE='23514';
    END IF;
    IF NEW.disposition_carrier_uid IS NOT NULL AND NOT ((
       NEW.creation_carrier_uid IS NULL AND
       NEW.cancellation_disposition IS NOT NULL AND
       NEW.cancellation_disposition->>'carrier_uid' IS NOT DISTINCT FROM NEW.disposition_carrier_uid::text AND
       NEW.cancellation_disposition->>'namespace' IS NOT DISTINCT FROM NEW.disposition_carrier_namespace AND
       ((NEW.owner_kind='thread' AND
         NEW.cancellation_disposition->>'carrier_kind'='thread_creation_cancel' AND
         NOT EXISTS(SELECT 1 FROM public.vm_creation_effects WHERE request_id=NEW.request_id))
        OR (NEW.owner_kind='job' AND public.valid_vm_creation_job_cancel_identity(NEW)))
    ) IS TRUE) THEN
        RAISE EXCEPTION 'Cancellation carrier cannot create' USING ERRCODE='23514';
    END IF;
    IF OLD.cancellation_disposition IS NOT NULL AND
       NEW.cancellation_disposition IS DISTINCT FROM OLD.cancellation_disposition THEN
        RAISE EXCEPTION 'Creation cancellation disposition is immutable' USING ERRCODE='23514';
    END IF;
    IF NEW.cancellation_disposition IS NOT NULL AND OLD.cancellation_disposition IS NULL THEN
        IF OLD.state<>'cancel_requested' OR NEW.state<>'cancel_requested' OR
           NEW.creation_admission_id IS NULL OR
           (NEW.creation_carrier_uid IS NULL AND NEW.disposition_carrier_uid IS NULL) OR
           (NEW.owner_kind='thread' AND NOT public.valid_vm_creation_thread_disposition_identity(NEW,true)) OR
           EXISTS(SELECT 1 FROM public.vm_creation_effects WHERE request_id=NEW.request_id
                  AND (state='issued' OR (effect_kind='vm' AND state<>'rejected'))) THEN
            RAISE EXCEPTION 'Creation disposition requires no possible VM issuance' USING ERRCODE='23514';
        END IF;
    END IF;
    IF NEW.cancellation_disposition IS NOT NULL AND NEW.state<>'cancel_requested' AND
       NOT (OLD.state='cancel_requested' AND NEW.state='settled'
            AND NEW.reason='creation_disposed' AND NEW.resolved_at IS NOT NULL
            AND NEW.claim_token IS NULL AND NEW.claim_expires_at IS NULL
            AND public.valid_vm_creation_disposition_evidence(NEW)) THEN
        RAISE EXCEPTION 'Creation disposition has not completed' USING ERRCODE='23514';
    END IF;
    IF EXISTS(SELECT 1 FROM jsonb_each(OLD.cancellation_progress) AS p
              WHERE NEW.cancellation_progress->p.key IS DISTINCT FROM p.value) OR
       (NEW.cancellation_progress - ARRAY['cloud_init','rootdisk','workspace_attachment','source'])<>'{}'::jsonb THEN
        RAISE EXCEPTION 'Creation cancellation progress is monotonic' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;

COMMIT;
