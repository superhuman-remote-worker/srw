-- migration:     0304_pinned_warm_agent_terminal_idx.notx.sql
-- description:   Keep warm actor uniqueness across terminal release.
-- depends-on:    0303_pinned_warm_thread_terminal_idx.notx.sql
-- expected:      One online unique index build; old unique index stays live.
-- locks:         SHARE UPDATE EXCLUSIVE (CONCURRENTLY), no exclusive table lock.
-- transactional: no
-- rollout:       On interrupted build, inspect this index's pg_index
--                indisvalid/indisready, DROP INDEX CONCURRENTLY the INVALID
--                shell, repair the dirty ledger, and retry. IF NOT EXISTS
--                could silently accept an unusable shell.

-- squawk-ignore prefer-robust-stmts
CREATE UNIQUE INDEX CONCURRENTLY idx_thread_agent_warm_binding_agent_active_v2
    ON public.thread_agent_warm_binding_protections(agent_id)
    WHERE status IN ('planned', 'protecting', 'protected', 'bound', 'releasing',
                     'terminal_release');
