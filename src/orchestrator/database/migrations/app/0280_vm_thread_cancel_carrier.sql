-- migration: 0280_vm_thread_cancel_carrier.sql
-- description: Separate signed cancellation-only carrier for source-pin-before-effect End.
-- depends-on: 0279_vm_thread_creation_disposition.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE public.vm_creation_retries
    ADD COLUMN disposition_carrier_uid uuid,
    ADD COLUMN disposition_carrier_namespace text,
    ADD CONSTRAINT vm_thread_cancel_carrier_pair CHECK (
        (disposition_carrier_uid IS NULL) = (disposition_carrier_namespace IS NULL)
        AND (disposition_carrier_namespace IS NULL OR disposition_carrier_namespace <> '')
    ),
    ADD CONSTRAINT vm_thread_cancel_carrier_exclusive CHECK (
        disposition_carrier_uid IS NULL OR
        (owner_kind='thread' AND creation_carrier_uid IS NULL)
    );

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
       AND retry.cancellation_disposition->>'disk_policy'='purge_new_thread_disk'
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
        RAISE EXCEPTION 'Thread cancellation carrier is immutable' USING ERRCODE='23514';
    END IF;
    IF NEW.disposition_carrier_uid IS NOT NULL AND (
       NEW.owner_kind<>'thread' OR NEW.creation_carrier_uid IS NOT NULL OR
       NEW.cancellation_disposition IS NULL OR
       NEW.cancellation_disposition->>'carrier_kind'<>'thread_creation_cancel' OR
       NEW.cancellation_disposition->>'carrier_uid' IS DISTINCT FROM NEW.disposition_carrier_uid::text OR
       NEW.cancellation_disposition->>'namespace' IS DISTINCT FROM NEW.disposition_carrier_namespace OR
       EXISTS(SELECT 1 FROM public.vm_creation_effects WHERE request_id=NEW.request_id)
    ) THEN
        RAISE EXCEPTION 'Thread cancellation carrier cannot create' USING ERRCODE='23514';
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
