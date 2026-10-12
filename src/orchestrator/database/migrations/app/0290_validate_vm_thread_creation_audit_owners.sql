-- migration: 0290_validate_vm_thread_creation_audit_owners.sql
-- description: Validate the backfilled immutable VM thread audit owner references.
-- depends-on: 0289_vm_thread_creation_audit_owners.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';
ALTER TABLE public.vm_creation_retries VALIDATE CONSTRAINT vm_creation_retries_audit_owner_fkey;
ALTER TABLE public.vm_resource_waiters VALIDATE CONSTRAINT vm_resource_waiters_audit_owner_fkey;
COMMIT;
