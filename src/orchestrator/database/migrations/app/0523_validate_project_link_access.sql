-- migration:     0523_validate_project_link_access.sql
-- description:   Validate the CHECKs 0522 added NOT VALID.
-- depends-on:    0522_project_link_access.sql
-- expected:      < 1s. One scan of project_datasources (a row per project
--                link) per constraint; every access is NULL before any
--                writer sets one.
-- locks:         SHARE UPDATE EXCLUSIVE on project_datasources when this
--                file runs on its own; in the same runner transaction as
--                0522, it runs under 0522's lock and waits for nothing.
-- transactional: yes
--
-- Idempotent: validating a constraint that is already valid is a no-op.
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '15min';
SET LOCAL idle_in_transaction_session_timeout = '5min';

ALTER TABLE public.project_datasources
    VALIDATE CONSTRAINT project_datasources_access_check;
ALTER TABLE public.project_datasources
    VALIDATE CONSTRAINT project_datasources_access_or_read_only_check;

COMMIT;
