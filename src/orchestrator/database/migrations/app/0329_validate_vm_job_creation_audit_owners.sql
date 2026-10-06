-- migration:     0329_validate_vm_job_creation_audit_owners.sql
-- description:   Validate backfilled durable VM Job retry owners separately.
-- depends-on:    0328_vm_job_creation_audit_owners.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';
ALTER TABLE public.vm_creation_retries
    VALIDATE CONSTRAINT vm_creation_retries_job_audit_owner_fkey;
COMMIT;
