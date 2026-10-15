-- migration:     0548_project_main_cloud_folder_error.sql
-- description:   Why a project has no main-cloud folder: the closed error
--                kind of its last failed create (a CloudBackendErrorKind
--                value such as already_exists or permission_denied), which
--                the project page shows. NULL before any failure and again
--                once a create succeeds. Never the cloud's own words.
-- depends-on:    0001_initial.sql
-- expected:      < 1s (a nullable column without a default: catalog only).
-- locks:         brief ACCESS EXCLUSIVE on projects.
-- transactional: yes

BEGIN;
SET LOCAL lock_timeout                        = '2s';
SET LOCAL statement_timeout                   = '30s';
SET LOCAL idle_in_transaction_session_timeout = '5min';

ALTER TABLE public.projects ADD COLUMN main_cloud_folder_error TEXT;

COMMENT ON COLUMN public.projects.main_cloud_folder_error IS
    'Why the project''s main-cloud folder could not be created: the error '
    'kind of the last failed attempt (already_exists, permission_denied, '
    'unavailable, ...). NULL when none failed or once one succeeded. '
    'Written by project cloud provisioning, shown on the project page.';

COMMIT;
