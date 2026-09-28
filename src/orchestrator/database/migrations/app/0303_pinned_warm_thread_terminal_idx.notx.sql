-- migration:     0303_pinned_warm_thread_terminal_idx.notx.sql
-- description:   Keep thread/G warm uniqueness across terminal release.
-- depends-on:    0301_pinned_permanent_warm_release.sql
-- expected:      One online unique index build; old unique index stays live.
-- locks:         SHARE UPDATE EXCLUSIVE (CONCURRENTLY), no exclusive table lock.
-- transactional: no
-- rollout:       On interrupted build, inspect this index's pg_index
--                indisvalid/indisready, DROP INDEX CONCURRENTLY the INVALID
--                shell, repair the dirty ledger, and retry. IF NOT EXISTS
--                could silently accept an unusable shell.

-- squawk-ignore prefer-robust-stmts
CREATE UNIQUE INDEX CONCURRENTLY idx_thread_agent_warm_binding_thread_active_v2
    ON public.thread_agent_warm_binding_protections(thread_id, runtime_generation)
    WHERE status IN ('planned', 'protecting', 'protected', 'bound', 'releasing',
                     'terminal_release');
