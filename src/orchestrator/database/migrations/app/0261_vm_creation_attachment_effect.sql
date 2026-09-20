-- migration: 0261_vm_creation_attachment_effect.sql
-- description: Allow the fixed retained-workspace attachment stage in create history.
-- depends-on: 0260_vm_creation_ready_release.sql
-- transactional: yes
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';
ALTER TABLE vm_creation_effects DROP CONSTRAINT vm_creation_effects_effect_kind_check;
ALTER TABLE vm_creation_effects ADD CONSTRAINT vm_creation_effects_effect_kind_check
    CHECK (effect_kind IN ('workspace_attach','rootdisk','cloud_init','vm')) NOT VALID;
ALTER TABLE vm_creation_effects VALIDATE CONSTRAINT vm_creation_effects_effect_kind_check;
COMMIT;
