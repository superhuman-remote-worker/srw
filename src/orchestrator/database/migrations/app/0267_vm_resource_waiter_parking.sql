-- migration: 0267_vm_resource_waiter_parking.sql
-- description: Park ineligible resource waiters while preserving age and protection.
-- depends-on: 0266_vm_creation_cancellation_disposition.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';
ALTER TABLE public.vm_resource_waiters DROP CONSTRAINT vm_resource_waiters_state_check;
ALTER TABLE public.vm_resource_waiters ADD CONSTRAINT vm_resource_waiters_state_check
    CHECK (state IN ('waiting','nonfit','parked','admitted','cancelled','released')) NOT VALID;

CREATE OR REPLACE FUNCTION public.guard_vm_resource_waiter() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='INSERT' THEN
        -- The parent fields are already immutable. Its existing PK/FK retains
        -- identity; this comparison avoids a blocking duplicate UNIQUE index.
        IF NOT EXISTS (SELECT 1 FROM public.vm_creation_retries r
            WHERE r.request_id=NEW.request_id AND r.job_id=NEW.job_id
            AND r.provision_generation=NEW.provision_generation AND r.request_digest=NEW.request_digest) THEN
            RAISE EXCEPTION 'VM resource waiter source identity mismatch' USING ERRCODE='23503';
        END IF;
        IF NEW.state<>'waiting' OR NEW.bypasses<>0 OR NEW.protected_order IS NOT NULL THEN
            RAISE EXCEPTION 'VM resource waiter must start waiting' USING ERRCODE='23514';
        END IF;
        RETURN NEW;
    END IF;
    IF ROW(NEW.request_id,NEW.job_id,NEW.provision_generation,NEW.cluster_id,NEW.policy_digest,
           NEW.owner_key,NEW.project_id,NEW.priority,NEW.request_digest,NEW.guest_vcpus,
           NEW.guest_memory_bytes,NEW.cpu_millicores,NEW.memory_bytes,NEW.kvm_devices,NEW.placement,NEW.enqueued_at)
       IS DISTINCT FROM
       ROW(OLD.request_id,OLD.job_id,OLD.provision_generation,OLD.cluster_id,OLD.policy_digest,
           OLD.owner_key,OLD.project_id,OLD.priority,OLD.request_digest,OLD.guest_vcpus,
           OLD.guest_memory_bytes,OLD.cpu_millicores,OLD.memory_bytes,OLD.kvm_devices,OLD.placement,OLD.enqueued_at)
       OR NEW.revision < OLD.revision OR NEW.bypasses < OLD.bypasses
       OR (OLD.protected_order IS NOT NULL AND NEW.protected_order IS DISTINCT FROM OLD.protected_order) THEN
        RAISE EXCEPTION 'VM resource waiter identity is immutable' USING ERRCODE='23514';
    END IF;
    IF NEW.state <> OLD.state AND NOT (
        (OLD.state='waiting' AND NEW.state IN ('nonfit','admitted','cancelled','parked')) OR
        (OLD.state='nonfit' AND NEW.state IN ('waiting','cancelled','parked')) OR
        (OLD.state='parked' AND NEW.state IN ('waiting','cancelled')) OR
        (OLD.state='admitted' AND NEW.state='released')
    ) THEN
        RAISE EXCEPTION 'Invalid VM resource waiter transition' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
COMMIT;
