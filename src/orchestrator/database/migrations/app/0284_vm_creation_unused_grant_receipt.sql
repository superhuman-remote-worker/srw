-- migration: 0284_vm_creation_unused_grant_receipt.sql
-- description: Persist immutable issuer receipt hashes and guard unused-grant attention.
-- depends-on: 0283_validate_vm_job_cancel_carrier.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE public.vm_creation_effects
    ADD COLUMN issuer_receipt_sha256 bytea;
ALTER TABLE public.vm_creation_effects
    ADD CONSTRAINT vm_creation_issuer_receipt_sha256_length
    CHECK (issuer_receipt_sha256 IS NULL OR octet_length(issuer_receipt_sha256)=32)
    NOT VALID;

CREATE OR REPLACE FUNCTION public.guard_vm_creation_effect_identity() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF ROW(NEW.effect_nonce,NEW.request_id,NEW.effect_number,NEW.effect_kind,
           NEW.carrier_uid,NEW.carrier_namespace,NEW.carrier_intent,NEW.issued_at,
           NEW.issuer_receipt_sha256)
       IS DISTINCT FROM
       ROW(OLD.effect_nonce,OLD.request_id,OLD.effect_number,OLD.effect_kind,
           OLD.carrier_uid,OLD.carrier_namespace,OLD.carrier_intent,OLD.issued_at,
           OLD.issuer_receipt_sha256)
       OR (OLD.state<>'issued' AND NEW IS DISTINCT FROM OLD) THEN
        RAISE EXCEPTION 'VM creation effect identity or resolved evidence is immutable' USING ERRCODE='23514';
    END IF;
    IF NEW.evidence->>'outcome'='not_attempted' AND (
        NEW.state='rejected'
        AND NEW.issuer_receipt_sha256 IS NOT NULL
        AND NEW.evidence=jsonb_build_object(
            'outcome','not_attempted','reason',NEW.evidence->>'reason')
        AND NEW.evidence->>'reason' IN (
            'resource_inventory_unavailable','resource_node_changed',
            'creation_carrier_changed','creation_observed_object_missing',
            'creation_observed_object_changed','retained_disk_changed',
            'workspace_recovery_held','workspace_attachment_unproven',
            'creation_existing_vm_unproven',
            'creation_rootdisk_source_unproven')
    ) IS NOT TRUE THEN
        RAISE EXCEPTION 'Invalid unused creation grant evidence'
            USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION public.valid_vm_creation_unused_grant_attention(
    retry public.vm_creation_retries
) RETURNS boolean LANGUAGE sql STABLE AS $$
    SELECT EXISTS(
        SELECT 1 FROM public.vm_creation_effects e
        WHERE e.request_id=retry.request_id
          AND e.effect_number=(
              SELECT max(effect_number) FROM public.vm_creation_effects
              WHERE request_id=retry.request_id)
          AND e.state='rejected'
          AND e.issuer_receipt_sha256 IS NOT NULL
          AND e.evidence=jsonb_build_object(
              'outcome','not_attempted','reason',e.evidence->>'reason')
          AND e.evidence->>'reason' IN (
              'resource_node_changed','creation_carrier_changed',
              'creation_observed_object_missing',
              'creation_observed_object_changed','retained_disk_changed',
              'workspace_recovery_held','workspace_attachment_unproven',
              'creation_existing_vm_unproven',
              'creation_rootdisk_source_unproven')
    );
$$;

CREATE OR REPLACE FUNCTION public.guard_vm_creation_retry_identity() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF ROW(NEW.request_id,NEW.job_id,NEW.provision_generation,NEW.origin,
           NEW.request_digest,NEW.canonical_request,NEW.controller_configuration_digest,
           NEW.execution_id,NEW.execution_revision,NEW.execution_generation,NEW.admission_deadline,
           NEW.expected_pvc_uid,NEW.predecessor_evidence,NEW.predecessor_cleanup_admission_id,NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.request_id,OLD.job_id,OLD.provision_generation,OLD.origin,
           OLD.request_digest,OLD.canonical_request,OLD.controller_configuration_digest,
           OLD.execution_id,OLD.execution_revision,OLD.execution_generation,OLD.admission_deadline,
           OLD.expected_pvc_uid,OLD.predecessor_evidence,OLD.predecessor_cleanup_admission_id,OLD.created_at)
       OR (OLD.creation_admission_id IS NOT NULL AND NEW.creation_admission_id IS DISTINCT FROM OLD.creation_admission_id)
       OR (OLD.observed_vm_uid IS NOT NULL AND NEW.observed_vm_uid IS DISTINCT FROM OLD.observed_vm_uid)
       OR (OLD.observed_pvc_uid IS NOT NULL AND NEW.observed_pvc_uid IS DISTINCT FROM OLD.observed_pvc_uid)
       OR (OLD.boot_counted AND NOT NEW.boot_counted)
       OR NEW.revision < OLD.revision THEN
        RAISE EXCEPTION 'VM creation retry identity is immutable' USING ERRCODE='23514';
    END IF;
    IF NEW.state <> OLD.state AND NOT (
        (OLD.state='queued' AND NEW.state IN ('reconciling','cancel_requested','succeeded')) OR
        (OLD.state='queued' AND NEW.state='attention'
         AND NEW.reason='vm_creation_retry_blocked'
         AND NEW.claim_token IS NULL AND NEW.claim_expires_at IS NULL
         AND NEW.revision=OLD.revision+1
         AND public.valid_vm_creation_unused_grant_attention(NEW)) OR
        (OLD.state='reconciling' AND NEW.state IN ('queued','attention','succeeded','cancel_requested')) OR
        (OLD.state='attention' AND NEW.state IN ('queued','cancel_requested','succeeded')) OR
        (OLD.state='cancel_requested' AND NEW.state='settled')
    ) THEN
        RAISE EXCEPTION 'Invalid VM creation retry transition' USING ERRCODE='23514';
    END IF;
    IF NEW.state='succeeded' AND OLD.state IN ('queued','attention') AND NOT (
        NEW.boot_counted AND NEW.reason='creation_adopted' AND EXISTS (
            SELECT 1 FROM vm_creation_effects e
            WHERE e.request_id=NEW.request_id AND e.effect_kind='vm' AND e.state='observed'
              AND e.evidence->>'uid'=NEW.observed_vm_uid::text
              AND e.evidence->>'pvc_uid'=NEW.observed_pvc_uid::text
        )
    ) THEN
        RAISE EXCEPTION 'Late VM adoption requires observed issuance' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;

COMMIT;
