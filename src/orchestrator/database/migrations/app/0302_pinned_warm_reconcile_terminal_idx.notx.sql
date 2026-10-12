-- migration:     0302_pinned_warm_reconcile_terminal_idx.notx.sql
-- description:   Include terminal release obligations in the warm reaper index.
-- depends-on:    0301_pinned_permanent_warm_release.sql
-- expected:      One online index build over the append-only warm ledger.
-- locks:         SHARE UPDATE EXCLUSIVE (CONCURRENTLY), no exclusive table lock.
-- transactional: no
-- rollout:       Existing index remains valid throughout. An interrupted build
--                may leave an INVALID same-name shell; inspect pg_index
--                indisvalid/indisready, DROP INDEX CONCURRENTLY that shell,
--                repair the dirty migration ledger, and retry. Do not use
--                IF NOT EXISTS, which could accept an invalid shell.

-- squawk-ignore prefer-robust-stmts
CREATE INDEX CONCURRENTLY idx_thread_agent_warm_binding_reconcile_v2
    ON public.thread_agent_warm_binding_protections(
        status, lease_expires_at, effect_expires_at
    ) WHERE status IN ('planned', 'protecting', 'protected', 'releasing',
                       'terminal_release');
