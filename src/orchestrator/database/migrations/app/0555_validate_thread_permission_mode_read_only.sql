-- migration:     0555_validate_thread_permission_mode_read_only.sql
-- description:   Validate the read_only permission mode check after expansion.
-- depends-on:    0554_thread_permission_mode_read_only.sql
-- transactional: yes

BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '15min';

ALTER TABLE public.threads
    VALIDATE CONSTRAINT valid_permission_mode_v2;

COMMIT;
