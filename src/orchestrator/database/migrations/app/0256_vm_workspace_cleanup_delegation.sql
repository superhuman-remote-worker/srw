-- Preserve the controller's disk reservation after its parent VM cleanup ends.
ALTER TABLE vm_workspace_cleanup_admissions
    ADD COLUMN parent_admission_id uuid
        REFERENCES vm_workspace_cleanup_admissions(id),
    ADD CONSTRAINT vm_workspace_cleanup_admissions_not_self_parent
        CHECK (parent_admission_id IS DISTINCT FROM id);

DROP INDEX vm_workspace_cleanup_admissions_one_open_owner;
CREATE UNIQUE INDEX vm_workspace_cleanup_admissions_one_open_owner
    ON vm_workspace_cleanup_admissions (owner_kind, owner_id)
    WHERE completed_at IS NULL AND parent_admission_id IS NULL;
CREATE UNIQUE INDEX vm_workspace_cleanup_admissions_one_open_child
    ON vm_workspace_cleanup_admissions (parent_admission_id)
    WHERE completed_at IS NULL AND parent_admission_id IS NOT NULL;
