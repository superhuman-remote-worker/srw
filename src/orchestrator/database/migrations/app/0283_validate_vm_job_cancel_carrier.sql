-- migration: 0283_validate_vm_job_cancel_carrier.sql
-- description: Validate the relaxed, no-effect cancellation Lease constraint.
-- depends-on: 0282_vm_job_cancel_carrier.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE public.vm_creation_retries
    VALIDATE CONSTRAINT vm_cancel_carrier_exclusive;

COMMIT;
