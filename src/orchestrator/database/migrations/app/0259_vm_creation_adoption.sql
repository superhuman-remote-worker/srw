-- migration: 0259_vm_creation_adoption.sql
-- description: Exact late VM adoption can settle without a live observer claim.
-- depends-on: 0258_vm_creation_effects.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';
CREATE OR REPLACE FUNCTION guard_vm_creation_retry_identity() RETURNS trigger LANGUAGE plpgsql AS $$
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
