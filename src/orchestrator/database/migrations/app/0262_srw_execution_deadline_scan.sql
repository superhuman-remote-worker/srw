-- migration: 0262_srw_execution_deadline_scan.sql
-- description: Bound deadline scans without starving jobs behind blocked cancels.
-- depends-on: 0261_vm_creation_attachment_effect.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

-- This position grants no execution authority and intentionally has no job FK:
-- a removed job must remain a usable boundary for the next keyset scan.
CREATE TABLE public.srw_execution_deadline_scan (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    created_at TIMESTAMPTZ,
    job_id UUID,
    CONSTRAINT srw_execution_deadline_scan_position CHECK (
        (created_at IS NULL) = (job_id IS NULL)
    )
);
INSERT INTO public.srw_execution_deadline_scan(singleton) VALUES(TRUE);
COMMIT;
