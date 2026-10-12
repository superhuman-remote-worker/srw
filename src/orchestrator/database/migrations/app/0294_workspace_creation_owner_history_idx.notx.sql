-- migration:     0294_workspace_creation_owner_history_idx.notx.sql
-- description:   Bound the per-owner historical-source exclusion used when
--                rediscovering already-issued initial Session workspaces.
-- depends-on:    0198_non_pinned_workspace_lifecycle_authority.sql
-- expected:      One online index build; no row or lifecycle authority changes.
-- locks:         SHARE UPDATE EXCLUSIVE (CONCURRENTLY), no exclusive table lock.
-- transactional: no
-- rollout:       The runner skips an applied migration. Do not accept an INVALID
--                same-name shell from an interrupted build via IF NOT EXISTS.
--                Inspect pg_index.indisvalid/indisready, drop an invalid shell
--                concurrently, repair the dirty ledger and retry normally.

-- squawk-ignore prefer-robust-stmts
CREATE INDEX CONCURRENTLY managed_repository_workspace_creation_owner_history
    ON public.managed_repository_workspace_creation_reservations
       (owner_kind, owner_id, scope, id);
