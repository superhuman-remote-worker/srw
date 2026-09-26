"""Empty query dependency for tests with deliberately partial Job schemas."""


async def create_empty_container_recovery_ledger(conn):
    """Let native recovery guards parse without inventing cleanup evidence.

    PostgreSQL resolves the ledger even when a Job has no recovery marker.
    These legacy fixtures exercise unrelated Job CAS/context contracts and
    never insert a receipt. Cleanup admission and settlement tests continue
    to use the full migration chain and its constraints/triggers.
    """
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS managed_repository_workspace_cleanup_intents (
            id uuid PRIMARY KEY,
            owner_kind text,
            owner_id uuid,
            scope text,
            runtime_incarnation uuid,
            intent_generation bigint,
            target_disposition text,
            resource_policy text,
            reclaim_shared_resources boolean,
            result_kind text,
            settled_at timestamptz
        );
        TRUNCATE managed_repository_workspace_cleanup_intents;
        """
    )
