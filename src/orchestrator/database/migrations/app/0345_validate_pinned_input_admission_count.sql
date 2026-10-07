-- migration: 0345_validate_pinned_input_admission_count.sql
-- description: Validate the admission_count check added NOT VALID by 0344.
-- depends-on: 0344_pinned_input_admission_count.sql
-- transactional: yes
-- expected: < 30s. One scan of thread_input_deliveries under SHARE UPDATE
--           EXCLUSIVE; reads and writes continue.
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '15min';

ALTER TABLE public.thread_input_deliveries
    VALIDATE CONSTRAINT thread_input_deliveries_admission_count_nonnegative;

COMMIT;
