-- migration:     0252_vm_workspace_recovery_controls.sql
-- description:   Atomic retry transfer and cleanup admission for VM workspace recovery.
-- depends-on:    0251_vm_workspace_recovery_attention_identity.sql
-- transactional: yes

BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE vm_workspace_recoveries
    ADD COLUMN superseded_by uuid REFERENCES vm_workspace_recoveries(id);
ALTER TABLE vm_workspace_recoveries
    DROP CONSTRAINT vm_workspace_recoveries_phase_check,
    DROP CONSTRAINT vm_workspace_recoveries_check2;
ALTER TABLE vm_workspace_recoveries
    ADD CONSTRAINT vm_workspace_recoveries_phase_check
        CHECK (phase IN ('recovering','paused_attention','recovered','cancelled','superseded')),
    ADD CONSTRAINT vm_workspace_recoveries_resolution_check
        CHECK ((resolved_at IS NULL AND phase IN ('recovering','paused_attention'))
            OR (resolved_at IS NOT NULL AND phase IN ('recovered','cancelled','superseded'))),
    ADD CONSTRAINT vm_workspace_recoveries_supersession_check
        CHECK ((phase='superseded' AND superseded_by IS NOT NULL)
            OR (phase<>'superseded' AND superseded_by IS NULL));

ALTER TABLE vm_workspace_recovery_jobs
    DROP CONSTRAINT vm_workspace_recovery_jobs_participation_check,
    DROP CONSTRAINT vm_workspace_recovery_jobs_check3;
ALTER TABLE vm_workspace_recovery_jobs
    ADD CONSTRAINT vm_workspace_recovery_jobs_participation_check
        CHECK (participation IN ('held','attention','released','cancelled','transferred')),
    ADD CONSTRAINT vm_workspace_recovery_jobs_resolution_check
        CHECK ((resolved_at IS NULL AND participation IN ('held','attention'))
            OR (resolved_at IS NOT NULL AND participation IN ('released','cancelled','transferred')));

CREATE TABLE vm_workspace_cleanup_admissions (
    id uuid PRIMARY KEY,
    owner_kind text NOT NULL CHECK (owner_kind IN ('job','thread')),
    owner_id uuid NOT NULL,
    pvc_uid uuid,
    source text NOT NULL CHECK (source <> ''),
    request_id uuid NOT NULL,
    admitted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    outcome text,
    UNIQUE (owner_kind, owner_id, request_id),
    CHECK ((completed_at IS NULL AND outcome IS NULL)
        OR (completed_at IS NOT NULL AND outcome IS NOT NULL AND outcome <> ''))
);
CREATE UNIQUE INDEX vm_workspace_cleanup_admissions_one_open_owner
ON vm_workspace_cleanup_admissions(owner_kind, owner_id)
WHERE completed_at IS NULL;

COMMIT;
