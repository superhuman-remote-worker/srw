-- migration: 0285_validate_vm_creation_unused_grant_receipt.sql
-- description: Validate the issuer receipt hash length constraint.
-- depends-on: 0284_vm_creation_unused_grant_receipt.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE public.vm_creation_effects
    VALIDATE CONSTRAINT vm_creation_issuer_receipt_sha256_length;

COMMIT;
