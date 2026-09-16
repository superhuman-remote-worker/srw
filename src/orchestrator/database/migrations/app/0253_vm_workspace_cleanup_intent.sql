-- migration:     0253_vm_workspace_cleanup_intent.sql
-- description:   Bind VM workspace cleanup replay to the full destructive intent.
-- depends-on:    0252_vm_workspace_recovery_controls.sql
-- transactional: yes

BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE vm_workspace_cleanup_admissions
    ADD COLUMN intent_digest text;

UPDATE vm_workspace_cleanup_admissions
SET intent_digest = 'legacy-md5:' || md5(
    owner_kind || ':' || owner_id::text || ':' || source || ':' ||
    request_id::text || ':' || COALESCE(pvc_uid::text, '')
);

ALTER TABLE vm_workspace_cleanup_admissions
    ALTER COLUMN intent_digest SET NOT NULL,
    ADD CONSTRAINT vm_workspace_cleanup_admissions_intent_digest_check
        CHECK (intent_digest <> '');

COMMIT;
