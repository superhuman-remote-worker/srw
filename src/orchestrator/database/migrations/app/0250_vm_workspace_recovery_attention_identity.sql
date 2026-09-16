-- Attention holds may retain incomplete identity alongside other blockers.
-- Automatic recovery and resolved outcomes still require exact runtime identity.
ALTER TABLE vm_workspace_recoveries
    DROP CONSTRAINT vm_workspace_recoveries_exact_runtime_identity,
    ADD CONSTRAINT vm_workspace_recoveries_exact_runtime_identity CHECK (
        (prior_vmi_uid IS NOT NULL AND prior_launcher_uid IS NOT NULL
         AND provision_generation IS NOT NULL AND namespace IS NOT NULL
         AND vm_uid IS NOT NULL AND root_pvc_uid IS NOT NULL)
        OR phase = 'paused_attention'
    );
