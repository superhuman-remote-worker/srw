-- migration: 0432_validate_vm_never_app_ready_retained_stop.sql
-- description: Validate immutable prior process-zero receipt linkage.
-- depends-on: 0431_vm_never_app_ready_retained_stop.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';
ALTER TABLE public.vm_pre_ssh_stop_intents VALIDATE CONSTRAINT vm_pre_ssh_stop_prior_zero_fk;
COMMIT;
