-- migration:     0305_validate_pinned_permanent_warm_release.sql
-- description:   Validate broadened warm release checks in a later transaction.
-- depends-on:    0301_pinned_permanent_warm_release.sql
-- expected:      Two online scans of the append-only warm-binding ledger.
-- transactional: yes

BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '15min';

ALTER TABLE public.thread_agent_warm_binding_protections
    VALIDATE CONSTRAINT thread_agent_warm_binding_protections_status_check;
ALTER TABLE public.thread_agent_warm_binding_protections
    VALIDATE CONSTRAINT thread_agent_warm_binding_protections_check2;

COMMIT;
