-- migration: 0344_pinned_input_admission_count.sql
-- description: Count each input delivery's provider admissions so the pinned
--              lane can bound serving an event again after its runtime died.
-- depends-on: 0343_vm_pre_registration_agent_claim_zero.sql
-- transactional: yes
-- expected: < 5s. A column with a constant default (no table rewrite on
--           PG 11+) and a NOT VALID check; 0345 validates it.
--
-- The pinned lane now serves again an event admitted by a runtime that died
-- (shared.persistent_input_delivery.reserve_stale_pinned_admissions), and a
-- recovery turn may delegate again (parallel_subagents.md D3), so each crash
-- can produce a new continuation that supersedes the last. Without a count a
-- turn that kills its runtime every time would be served forever. The pinned
-- admission CAS increments this column and the pre-provider unadmit takes it
-- back; the pinned claim sums it along the supersedes_input_seq chain and
-- parks the input at the bound (state 'deferred', deferred_reason
-- 'max_attempts') until its owner retries, which resets the chain to zero.
-- The stateless lane does not write it: its unit already parks at
-- run_queue.max_attempts.
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE public.thread_input_deliveries
    ADD COLUMN admission_count integer NOT NULL DEFAULT 0;

ALTER TABLE public.thread_input_deliveries
    ADD CONSTRAINT thread_input_deliveries_admission_count_nonnegative
    CHECK (admission_count >= 0) NOT VALID;

COMMENT ON COLUMN public.thread_input_deliveries.admission_count IS
    'Pinned provider admissions of this delivery since its owner last retried it. Summed along the supersedes_input_seq chain to bound repeated recovery; the stateless lane leaves it 0.';

COMMIT;
