-- migration:     0254_vm_workspace_recovery_reconciler.sql
-- description:   Durable phases and retry accounting for bounded workspace recovery.
-- depends-on:    0253_vm_workspace_cleanup_intent.sql
-- transactional: yes

BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE vm_workspace_recoveries
    ADD COLUMN recovery_attempts integer NOT NULL DEFAULT 0
        CHECK (recovery_attempts >= 0);

ALTER TABLE vm_workspace_recoveries
    DROP CONSTRAINT vm_workspace_recoveries_phase_check,
    DROP CONSTRAINT vm_workspace_recoveries_resolution_check;

ALTER TABLE vm_workspace_recoveries
    ADD CONSTRAINT vm_workspace_recoveries_phase_check CHECK (phase IN (
        'recovering', 'observing', 'waiting_runtime', 'verifying_stop',
        'attesting', 'reconciling_outcome', 'paused_attention',
        'recovered', 'cancelled', 'superseded'
    )),
    ADD CONSTRAINT vm_workspace_recoveries_resolution_check CHECK (
        (resolved_at IS NULL AND phase IN (
            'recovering', 'observing', 'waiting_runtime', 'verifying_stop',
            'attesting', 'reconciling_outcome', 'paused_attention'
        )) OR
        (resolved_at IS NOT NULL AND phase IN (
            'recovered', 'cancelled', 'superseded'
        ))
    );

DROP INDEX vm_workspace_recoveries_due;
CREATE INDEX vm_workspace_recoveries_due
ON vm_workspace_recoveries (next_check_at, deadline_at, id)
WHERE phase IN (
    'recovering', 'observing', 'waiting_runtime', 'verifying_stop',
    'attesting', 'reconciling_outcome'
) AND resolved_at IS NULL;

COMMIT;
