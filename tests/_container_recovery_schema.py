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
        CREATE OR REPLACE FUNCTION
            public.managed_repository_workspace_cleanup_projection_is_settled(
                text, uuid, text, text, text, text
            )
        RETURNS boolean LANGUAGE sql STABLE AS $$ SELECT false $$;
        """
    )


async def create_undischarged_pinned_cancel_retention_ledger(conn):
    """Model the pending policy-4 CAS dependency in partial Job schemas.

    The production discharge proof lives in migration 0528. These fixtures
    exercise unrelated resume/context writes and only need its conservative
    undischarged state; full-chain tests cover actual settlement/discharge.
    """
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS vm_job_cancel_retention_authorities (
            cleanup_admission_id uuid PRIMARY KEY,
            job_id uuid NOT NULL,
            policy_version integer NOT NULL
        );
        CREATE OR REPLACE FUNCTION public.vm_job_cancel_retention_discharged(uuid)
        RETURNS boolean LANGUAGE sql STABLE AS $$ SELECT false $$;
        TRUNCATE vm_job_cancel_retention_authorities;
        """
    )
