"""Real-Postgres semantics for the completion-aware expired-lease rescuer."""

import asyncio
import json
import time
from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest
from testcontainers.postgres import PostgresContainer

from orchestrator.database.postgres import (
    LeaseRecoveryBatch,
    PostgresDB,
    RecoveredJob,
)


SCHEMA_FILE = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "orchestrator"
    / "database"
    / "schema_current.sql"
)


@pytest.mark.asyncio
async def test_expired_command_routes_away_from_legacy_lease_recovery():
    """An expired finalizer lease must never fall through to agent recovery."""
    with PostgresContainer("postgres:15") as pg:
        dsn = pg.get_connection_url().replace("postgresql+psycopg2", "postgresql")
        conn = await asyncpg.connect(dsn)
        db = None
        try:
            await conn.execute(SCHEMA_FILE.read_text())
            # pg_dump deliberately leaves its replay session with an empty
            # search_path; runtime pool connections start on ``public``.
            await conn.execute("SET search_path TO public")
            agent_id = await conn.fetchval(
                "INSERT INTO agents (config_name, hostname, status) "
                "VALUES ('defaults', 'completion-route-test', 'working') "
                "RETURNING id"
            )
            command_job_id, legacy_job_id = await conn.fetchrow(
                """
                WITH command_job AS (
                    INSERT INTO jobs (
                        description, status, execution_lane,
                        assigned_agent_id, lease_expires_at
                    ) VALUES (
                        'expired finalizer owns recovery', 'processing', 'pinned',
                        $1, now() - interval '1 minute'
                    ) RETURNING id
                ), legacy_job AS (
                    INSERT INTO jobs (
                        description, status, execution_lane,
                        assigned_agent_id, lease_expires_at
                    ) VALUES (
                        'legacy agent recovery', 'processing', 'pinned',
                        $1, now() - interval '1 minute'
                    ) RETURNING id
                )
                SELECT command_job.id, legacy_job.id
                FROM command_job CROSS JOIN legacy_job
                """,
                agent_id,
            )
            await conn.execute(
                """
                INSERT INTO job_completion_commands (
                    job_id, report_seq, client_report_id, payload,
                    payload_digest, accepted_lease_token, origin, requested_by,
                    state, attempts, max_attempts, run_after, lease_expires_at,
                    deadline_at, code_version
                ) VALUES (
                    $1, 1, $2, $3::jsonb, 'digest', 1, 'agent', 'test-agent',
                    'finalizing', 1, 5, now() - interval '1 minute',
                    now() - interval '1 second', now() + interval '1 hour',
                    'test'
                )
                """,
                command_job_id,
                uuid4(),
                json.dumps({"result": "ok"}),
            )

            db = PostgresDB(connection_string=dsn, min_connections=1, max_connections=2)
            await db.connect()

            assert await db.recover_expired_lease_jobs(
                completion_commands_enabled=True
            ) == LeaseRecoveryBatch(
                recovered_jobs=(RecoveredJob(job_id=str(legacy_job_id)),)
            )
            routed = await conn.fetchrow(
                "SELECT status, assigned_agent_id, lease_expires_at "
                "FROM jobs WHERE id=$1",
                command_job_id,
            )
            assert routed["status"] == "processing"
            assert routed["assigned_agent_id"] == agent_id
            assert routed["lease_expires_at"] is not None

            # Flag-off remains the exact legacy path: the same row is now
            # recovered once command ownership is deliberately ignored.
            assert await db.recover_expired_lease_jobs(
                completion_commands_enabled=False
            ) == LeaseRecoveryBatch(
                recovered_jobs=(RecoveredJob(job_id=str(command_job_id)),)
            )
            legacy = await conn.fetchrow(
                "SELECT status, assigned_agent_id, lease_expires_at "
                "FROM jobs WHERE id=$1",
                command_job_id,
            )
            assert legacy["status"] == "paused"
            assert legacy["assigned_agent_id"] is None
            assert legacy["lease_expires_at"] is None
        finally:
            if db is not None:
                await db.disconnect()
            await conn.close()


@pytest.mark.asyncio
async def test_unchanged_lease_recovery_circuit_is_atomic_and_project_scoped():
    """Three unchanged agent cycles park once and queue only the owner wake."""

    with PostgresContainer("postgres:15") as pg:
        dsn = pg.get_connection_url().replace("postgresql+psycopg2", "postgresql")
        conn = await asyncpg.connect(dsn)
        db = None
        try:
            await conn.execute(SCHEMA_FILE.read_text())
            await conn.execute("SET search_path TO public")
            project_id, foreign_project_id, vacant_project_id = await conn.fetchrow(
                """
                WITH owner AS (
                    INSERT INTO projects (name) VALUES ('lease owner') RETURNING id
                ), foreign_project AS (
                    INSERT INTO projects (name) VALUES ('foreign') RETURNING id
                ), vacant AS (
                    INSERT INTO projects (name) VALUES ('vacant') RETURNING id
                )
                SELECT owner.id, foreign_project.id, vacant.id
                FROM owner CROSS JOIN foreign_project CROSS JOIN vacant
                """
            )
            officer_metadata = json.dumps(
                {"config_override": {"officer": {"enabled": True, "auto_pull": False}}}
            )
            owner_thread_id, foreign_thread_id = await conn.fetchrow(
                """
                WITH owner_thread AS (
                    INSERT INTO threads (project_id, status, metadata)
                    VALUES ($1, 'active', $3::jsonb) RETURNING id
                ), foreign_thread AS (
                    INSERT INTO threads (project_id, status, metadata)
                    VALUES ($2, 'active', $3::jsonb) RETURNING id
                )
                SELECT owner_thread.id, foreign_thread.id
                FROM owner_thread CROSS JOIN foreign_thread
                """,
                project_id,
                foreign_project_id,
                officer_metadata,
            )
            await conn.execute(
                """
                INSERT INTO project_officers (project_id, thread_id)
                VALUES ($1, $2), ($3, $4), ($5, NULL)
                """,
                project_id,
                owner_thread_id,
                foreign_project_id,
                foreign_thread_id,
                vacant_project_id,
            )
            # Agent 1 holds the expired leases. The redispatch claimants are
            # free 'ready' agents: the pinned claim CAS serializes on the agent
            # row and refuses a busy one, so agent 4's refused claim below
            # proves the trip marker rather than agent availability.
            agent_ids = await conn.fetch(
                """
                INSERT INTO agents (config_name, hostname, status)
                VALUES ('defaults', 'lease-agent-1', 'working'),
                       ('defaults', 'lease-agent-2', 'ready'),
                       ('defaults', 'lease-agent-3', 'ready'),
                       ('defaults', 'lease-agent-4', 'ready')
                RETURNING id
                """
            )
            job_id = await conn.fetchval(
                """
                INSERT INTO jobs (
                    description, status, project_id, execution_lane,
                    assigned_agent_id, lease_expires_at, context
                ) VALUES (
                    'circuit target', 'processing', $1, 'pinned', $2,
                    now()-interval '1 second',
                    jsonb_build_object(
                        'completion_decision',
                        jsonb_build_object(
                            'tool_call_id', 'stable-decision',
                            'recorded_at', '2026-08-18T10:00:00+00:00'
                        )
                    )
                ) RETURNING id
                """,
                project_id,
                agent_ids[0]["id"],
            )

            db = PostgresDB(connection_string=dsn, min_connections=1, max_connections=6)
            await db.connect()
            audit_count = 17

            async def audit_fingerprint(job_ids):
                return {str(item): audit_count for item in job_ids}

            async def rearm(target_job_id, agent_id):
                assert await db.claim_job_for_agent(str(target_job_id), str(agent_id))
                await conn.execute(
                    """
                    UPDATE jobs
                    SET lease_expires_at=now()-interval '1 second'
                    WHERE id=$1
                    """,
                    target_job_id,
                )

            first = await db.recover_expired_lease_jobs(
                audit_fingerprint_provider=audit_fingerprint
            )
            assert first == LeaseRecoveryBatch(
                recovered_jobs=(
                    RecoveredJob(job_id=str(job_id), project_id=str(project_id)),
                )
            )
            await rearm(job_id, agent_ids[1]["id"])
            second = await db.recover_expired_lease_jobs(
                audit_fingerprint_provider=audit_fingerprint
            )
            assert second == LeaseRecoveryBatch(
                recovered_jobs=(
                    RecoveredJob(job_id=str(job_id), project_id=str(project_id)),
                )
            )

            # Both replicas discover the same expired row. Post/thread/job
            # locks plus the processing-state CAS permit one trip and no
            # double increment, outbox insert, or redispatch result.
            await rearm(job_id, agent_ids[2]["id"])
            contenders = await asyncio.gather(
                db.recover_expired_lease_jobs(
                    audit_fingerprint_provider=audit_fingerprint
                ),
                db.recover_expired_lease_jobs(
                    audit_fingerprint_provider=audit_fingerprint
                ),
            )
            all_trips = [trip for batch in contenders for trip in batch.circuit_trips]
            assert len(all_trips) == 1
            assert all_trips[0].job_id == str(job_id)
            assert all_trips[0].project_id == str(project_id)
            assert all_trips[0].officer_thread_id == str(owner_thread_id)
            assert all_trips[0].notification_queued is True
            assert sum(len(batch.recovered_job_ids) for batch in contenders) == 0

            parked = await conn.fetchrow(
                "SELECT status, assigned_agent_id, lease_expires_at, "
                "freeze_data, error_message, error_details, context "
                "FROM jobs WHERE id=$1",
                job_id,
            )
            freeze = json.loads(parked["freeze_data"])
            context = json.loads(parked["context"])
            assert parked["status"] == "paused"
            assert parked["assigned_agent_id"] is None
            assert parked["lease_expires_at"] is None
            assert freeze["freeze_type"] == "redispatch_livelock"
            assert freeze["automatic_redispatch"] is False
            assert context["_lease_recovery"]["state"] == "tripped"
            assert context["_lease_recovery"]["unchanged_recoveries"] == 3
            assert context["_lease_recovery"]["generation"]
            assert context["_lease_recovery"]["last_recovered_agent_id"] == str(
                agent_ids[2]["id"]
            )
            assert "redispatch_livelock" in parked["error_message"]
            assert json.loads(parked["error_details"])["classification"] == (
                "redispatch_livelock"
            )

            wakes = await conn.fetch(
                "SELECT thread_id, project_id, source, dedup_key "
                "FROM session_wake_events WHERE source='lease_recovery'"
            )
            assert [dict(row) for row in wakes] == [
                {
                    "thread_id": owner_thread_id,
                    "project_id": project_id,
                    "source": "lease_recovery",
                    "dedup_key": (
                        f"redispatch_livelock:{job_id}:"
                        f"{context['_lease_recovery']['generation']}"
                    ),
                }
            ]

            # Even if a generic path were to clear the freeze accidentally,
            # the server-owned trip marker blocks scan, CAS claim, and the
            # direct dispatch authority. Restore the diagnostic afterwards.
            await conn.execute("UPDATE jobs SET freeze_data=NULL WHERE id=$1", job_id)
            assert all(
                str(row["id"]) != str(job_id)
                for row in await db.get_dispatchable_jobs(limit=100)
            )
            assert not await db.claim_job_for_agent(
                str(job_id), str(agent_ids[3]["id"])
            )
            await conn.execute(
                "UPDATE jobs SET freeze_data=$2::jsonb WHERE id=$1",
                job_id,
                json.dumps(freeze),
            )

            # A read outage retains the prior audit component instead of
            # inventing movement. A later real audit advance starts a fresh
            # sequence; mere agent churn above did not.
            reset_job_id = await conn.fetchval(
                """
                INSERT INTO jobs (
                    description, status, project_id, execution_lane,
                    assigned_agent_id, lease_expires_at
                ) VALUES (
                    'audit reset target', 'processing', $1, 'pinned', $2,
                    now()-interval '1 second'
                ) RETURNING id
                """,
                project_id,
                agent_ids[0]["id"],
            )
            audit_count = 30
            assert (
                str(reset_job_id)
                in (
                    await db.recover_expired_lease_jobs(
                        audit_fingerprint_provider=audit_fingerprint
                    )
                ).recovered_job_ids
            )
            await rearm(reset_job_id, agent_ids[1]["id"])

            async def unavailable_audit(_job_ids):
                raise RuntimeError("audit unavailable")

            unavailable = await db.recover_expired_lease_jobs(
                audit_fingerprint_provider=unavailable_audit
            )
            assert str(reset_job_id) in unavailable.recovered_job_ids
            unavailable_context = json.loads(
                await conn.fetchval(
                    "SELECT context FROM jobs WHERE id=$1", reset_job_id
                )
            )
            assert unavailable_context["_lease_recovery"]["unchanged_recoveries"] == 2
            await rearm(reset_job_id, agent_ids[2]["id"])
            audit_count = 31
            reset = await db.recover_expired_lease_jobs(
                audit_fingerprint_provider=audit_fingerprint
            )
            assert str(reset_job_id) in reset.recovered_job_ids
            reset_context = json.loads(
                await conn.fetchval(
                    "SELECT context FROM jobs WHERE id=$1", reset_job_id
                )
            )
            assert reset_context["_lease_recovery"]["unchanged_recoveries"] == 1

            # Establishing the first available audit baseline after an outage
            # is not itself progress. It preserves the sequence; only a later
            # count delta resets it.
            unknown_job_id = await conn.fetchval(
                """
                INSERT INTO jobs (
                    description, status, project_id, execution_lane,
                    assigned_agent_id, lease_expires_at
                ) VALUES (
                    'unknown audit baseline', 'processing', $1, 'pinned', $2,
                    now()-interval '1 second'
                ) RETURNING id
                """,
                project_id,
                agent_ids[0]["id"],
            )
            first_unknown = await db.recover_expired_lease_jobs(
                audit_fingerprint_provider=unavailable_audit
            )
            assert str(unknown_job_id) in first_unknown.recovered_job_ids
            await rearm(unknown_job_id, agent_ids[1]["id"])
            restored = await db.recover_expired_lease_jobs(
                audit_fingerprint_provider=audit_fingerprint
            )
            assert str(unknown_job_id) in restored.recovered_job_ids
            restored_context = json.loads(
                await conn.fetchval(
                    "SELECT context FROM jobs WHERE id=$1", unknown_job_id
                )
            )
            assert restored_context["_lease_recovery"]["unchanged_recoveries"] == 2

            # Vacant ownership records the trip durably on both the job and
            # post ledger; no unrelated Officer wake is fabricated.
            vacant_job_id = await conn.fetchval(
                """
                INSERT INTO jobs (
                    description, status, project_id, execution_lane,
                    assigned_agent_id, lease_expires_at
                ) VALUES (
                    'vacant circuit target', 'processing', $1, 'pinned', $2,
                    now()-interval '1 second'
                ) RETURNING id
                """,
                vacant_project_id,
                agent_ids[0]["id"],
            )
            for cycle in range(3):
                if cycle:
                    await rearm(vacant_job_id, agent_ids[cycle]["id"])
                vacant_result = await db.recover_expired_lease_jobs()
            assert len(vacant_result.circuit_trips) == 1
            assert vacant_result.circuit_trips[0].officer_destination == (
                "while_vacant"
            )
            vacant_state = json.loads(
                await conn.fetchval(
                    "SELECT state FROM project_officers WHERE project_id=$1",
                    vacant_project_id,
                )
            )
            assert any(
                entry.get("job_id") == str(vacant_job_id)
                and entry.get("status") == "redispatch_livelock"
                for entry in vacant_state["while_vacant"]
            )
            assert (
                await conn.fetchval(
                    "SELECT count(*) FROM session_wake_events "
                    "WHERE thread_id=$1 AND source='lease_recovery'",
                    foreign_thread_id,
                )
                == 0
            )
        finally:
            if db is not None:
                await db.disconnect()
            await conn.close()


@pytest.mark.asyncio
async def test_legacy_orphan_and_registration_paths_leave_leased_rows_to_expiry():
    """The predicate, not detector order, partitions legacy and leased recovery."""

    with PostgresContainer("postgres:15") as pg:
        dsn = pg.get_connection_url().replace("postgresql+psycopg2", "postgresql")
        conn = await asyncpg.connect(dsn)
        db = None
        try:
            await conn.execute(SCHEMA_FILE.read_text())
            await conn.execute("SET search_path TO public")
            db = PostgresDB(connection_string=dsn, min_connections=1, max_connections=4)
            await db.connect()

            for enabled in (False, True):
                suffix = "on" if enabled else "off"
                offline_agent = await conn.fetchval(
                    "INSERT INTO agents (config_name, hostname, status) "
                    "VALUES ('defaults', $1, 'offline') RETURNING id",
                    f"authority-{suffix}",
                )
                leased_id, legacy_id = await conn.fetchrow(
                    """
                    WITH leased AS (
                        INSERT INTO jobs (
                            description, status, execution_lane,
                            assigned_agent_id, lease_expires_at
                        ) VALUES ($1, 'processing', 'pinned', $3,
                                  now()-interval '1 second')
                        RETURNING id
                    ), legacy AS (
                        INSERT INTO jobs (
                            description, status, execution_lane,
                            assigned_agent_id, lease_expires_at
                        ) VALUES ($2, 'processing', 'pinned', $3, NULL)
                        RETURNING id
                    )
                    SELECT leased.id, legacy.id
                    FROM leased CROSS JOIN legacy
                    """,
                    f"leased flag {suffix}",
                    f"legacy flag {suffix}",
                    offline_agent,
                )

                # This is the production detector order. The legacy sweep may
                # consume only the NULL-lease row even though both point at the
                # same offline agent.
                assert (
                    await db.recover_orphaned_jobs(completion_commands_enabled=enabled)
                ).count == 1
                rows = await conn.fetch(
                    "SELECT id, status, assigned_agent_id FROM jobs "
                    "WHERE id=ANY($1::uuid[]) ORDER BY id",
                    [leased_id, legacy_id],
                )
                by_id = {row["id"]: row for row in rows}
                assert by_id[leased_id]["status"] == "processing"
                assert by_id[leased_id]["assigned_agent_id"] == offline_agent
                assert by_id[legacy_id]["status"] == "paused"
                assert by_id[legacy_id]["assigned_agent_id"] is None
                assert (
                    await db.recover_expired_lease_jobs(
                        completion_commands_enabled=enabled
                    )
                ) == LeaseRecoveryBatch(
                    recovered_jobs=(RecoveredJob(job_id=str(leased_id)),)
                )

                same_host_agent = await conn.fetchval(
                    "INSERT INTO agents (config_name, hostname, status) "
                    "VALUES ('defaults', $1, 'working') RETURNING id",
                    f"same-host-{suffix}",
                )
                registered_leased, registered_legacy = await conn.fetchrow(
                    """
                    WITH leased AS (
                        INSERT INTO jobs (
                            description, status, execution_lane,
                            assigned_agent_id, lease_expires_at
                        ) VALUES ($1, 'processing', 'pinned', $3,
                                  now()+interval '1 minute')
                        RETURNING id
                    ), legacy AS (
                        INSERT INTO jobs (
                            description, status, execution_lane,
                            assigned_agent_id, lease_expires_at
                        ) VALUES ($2, 'processing', 'pinned', $3, NULL)
                        RETURNING id
                    )
                    SELECT leased.id, legacy.id
                    FROM leased CROSS JOIN legacy
                    """,
                    f"registered leased {suffix}",
                    f"registered legacy {suffix}",
                    same_host_agent,
                )
                registration = await db.register_agent(
                    config_name="defaults",
                    pod_ip="10.42.0.9",
                    hostname=f"same-host-{suffix}",
                    completion_commands_enabled=enabled,
                )
                assert registration["agent_id"] == str(same_host_agent)
                registered = await conn.fetch(
                    "SELECT id, status, assigned_agent_id, lease_expires_at "
                    "FROM jobs WHERE id=ANY($1::uuid[])",
                    [registered_leased, registered_legacy],
                )
                by_id = {row["id"]: row for row in registered}
                assert by_id[registered_leased]["status"] == "processing"
                assert by_id[registered_leased]["assigned_agent_id"] == (
                    same_host_agent
                )
                assert by_id[registered_leased]["lease_expires_at"] is not None
                assert by_id[registered_legacy]["status"] == "paused"
                assert by_id[registered_legacy]["assigned_agent_id"] is None
                assert await db.route_pinned_agent_release_to_lease_recovery(
                    str(registered_leased),
                    completion_commands_enabled=enabled,
                    expected_agent_id=str(same_host_agent),
                )
                assert (
                    await db.recover_orphaned_jobs(completion_commands_enabled=enabled)
                ).count == 0
                registered_recovery = await db.recover_expired_lease_jobs(
                    completion_commands_enabled=enabled
                )
                assert registered_recovery.recovered_job_ids == (
                    str(registered_leased),
                )
                registered_context = json.loads(
                    await conn.fetchval(
                        "SELECT context FROM jobs WHERE id=$1", registered_leased
                    )
                )
                assert (
                    registered_context["_lease_recovery"]["unchanged_recoveries"] == 1
                )
        finally:
            if db is not None:
                await db.disconnect()
            await conn.close()


@pytest.mark.asyncio
async def test_real_detector_order_counts_offline_ready_and_deleted_cycles_once():
    """The former bypass now reaches the third-cycle project-scoped circuit."""

    with PostgresContainer("postgres:15") as pg:
        dsn = pg.get_connection_url().replace("postgresql+psycopg2", "postgresql")
        conn = await asyncpg.connect(dsn)
        db = None
        try:
            await conn.execute(SCHEMA_FILE.read_text())
            await conn.execute("SET search_path TO public")
            project_id = await conn.fetchval(
                "INSERT INTO projects (name) VALUES ('detector order') RETURNING id"
            )
            metadata = json.dumps(
                {"config_override": {"officer": {"enabled": True, "auto_pull": False}}}
            )
            thread_id = await conn.fetchval(
                "INSERT INTO threads (project_id, status, metadata) "
                "VALUES ($1, 'active', $2::jsonb) RETURNING id",
                project_id,
                metadata,
            )
            await conn.execute(
                "INSERT INTO project_officers (project_id, thread_id) VALUES ($1,$2)",
                project_id,
                thread_id,
            )
            agents = await conn.fetch(
                "INSERT INTO agents (config_name, hostname, status) VALUES "
                "('defaults','detector-offline','offline'),"
                "('defaults','detector-ready','ready'),"
                "('defaults','detector-deleted','ready') RETURNING id"
            )
            job_id = await conn.fetchval(
                "INSERT INTO jobs (description,status,project_id,execution_lane,"
                "assigned_agent_id,lease_expires_at) VALUES "
                "('ordered circuit','processing',$1,'pinned',$2,"
                "now()-interval '1 second') RETURNING id",
                project_id,
                agents[0]["id"],
            )
            db = PostgresDB(connection_string=dsn, min_connections=1, max_connections=6)
            await db.connect()

            # Offline cycle 1: legacy first, lease authority second.
            assert (await db.recover_orphaned_jobs()).count == 0
            assert await db.recover_expired_lease_jobs() == LeaseRecoveryBatch(
                recovered_jobs=(
                    RecoveredJob(job_id=str(job_id), project_id=str(project_id)),
                )
            )

            # Ready cycle 2: use the real claim funnel, then expire its lease.
            assert await db.claim_job_for_agent(str(job_id), str(agents[1]["id"]))
            await conn.execute(
                "UPDATE jobs SET lease_expires_at=now()-interval '1 second' "
                "WHERE id=$1",
                job_id,
            )
            assert (await db.recover_orphaned_jobs()).count == 0
            assert await db.recover_expired_lease_jobs() == LeaseRecoveryBatch(
                recovered_jobs=(
                    RecoveredJob(job_id=str(job_id), project_id=str(project_id)),
                )
            )

            # Deleted-agent cycle 3: ON DELETE SET NULL must not make the
            # legacy sweep steal the still-leased row.
            assert await db.claim_job_for_agent(str(job_id), str(agents[2]["id"]))
            await conn.execute(
                "UPDATE jobs SET lease_expires_at=now()-interval '1 second' "
                "WHERE id=$1",
                job_id,
            )
            assert await db.delete_agent(str(agents[2]["id"]))
            assert (await db.recover_orphaned_jobs()).count == 0
            third = await db.recover_expired_lease_jobs()
            assert len(third.circuit_trips) == 1
            assert third.circuit_trips[0].job_id == str(job_id)
            assert third.circuit_trips[0].project_id == str(project_id)
            assert third.circuit_trips[0].officer_thread_id == str(thread_id)
            assert third.circuit_trips[0].notification_queued is True

            parked = await conn.fetchrow(
                "SELECT status,assigned_agent_id,lease_expires_at,context "
                "FROM jobs WHERE id=$1",
                job_id,
            )
            context = json.loads(parked["context"])
            assert parked["status"] == "paused"
            assert parked["assigned_agent_id"] is None
            assert parked["lease_expires_at"] is None
            assert context["_lease_recovery"]["unchanged_recoveries"] == 3
            assert (
                await conn.fetchval(
                    "SELECT count(*) FROM session_wake_events "
                    "WHERE project_id=$1 AND source='lease_recovery'",
                    project_id,
                )
                == 1
            )
        finally:
            if db is not None:
                await db.disconnect()
            await conn.close()


@pytest.mark.asyncio
async def test_circuit_ack_is_atomic_claimable_and_starts_a_fresh_window():
    with PostgresContainer("postgres:15") as pg:
        dsn = pg.get_connection_url().replace("postgresql+psycopg2", "postgresql")
        conn = await asyncpg.connect(dsn)
        db = None
        try:
            await conn.execute(SCHEMA_FILE.read_text())
            await conn.execute("SET search_path TO public")
            agents = await conn.fetch(
                "INSERT INTO agents (config_name,hostname,status) VALUES "
                "('defaults','ack-agent-1','ready'),"
                "('defaults','ack-agent-2','ready') RETURNING id"
            )
            job_id = await conn.fetchval(
                """
                INSERT INTO jobs (
                    description,status,execution_lane,assigned_agent_id,
                    lease_expires_at,context
                ) VALUES (
                    'ack target','processing','pinned',$1,
                    now()-interval '1 second',
                    jsonb_build_object('completion_decision',
                        jsonb_build_object('tool_call_id','decision-before-trip'))
                ) RETURNING id
                """,
                agents[0]["id"],
            )
            db = PostgresDB(connection_string=dsn, min_connections=1, max_connections=6)
            await db.connect()
            for cycle in range(3):
                if cycle:
                    assert await db.claim_job_for_agent(
                        str(job_id), str(agents[cycle % 2]["id"])
                    )
                    await conn.execute(
                        "UPDATE jobs SET lease_expires_at=now()-interval '1 second' "
                        "WHERE id=$1",
                        job_id,
                    )
                tripped = await db.recover_expired_lease_jobs()
            assert len(tripped.circuit_trips) == 1
            before = json.loads(
                await conn.fetchval("SELECT context FROM jobs WHERE id=$1", job_id)
            )
            generation = before["_lease_recovery"]["generation"]

            results = await asyncio.gather(
                db.acknowledge_lease_recovery_circuit(
                    str(job_id),
                    expected_status="paused",
                    expected_generation=generation,
                    acknowledged_by={"caller_kind": "human", "user_id": "operator-a"},
                    context_merge={"queued_feedback": "operator inspected incident"},
                    completion_commands_enabled=True,
                ),
                db.acknowledge_lease_recovery_circuit(
                    str(job_id),
                    expected_status="paused",
                    expected_generation=generation,
                    acknowledged_by={"caller_kind": "human", "user_id": "operator-b"},
                    completion_commands_enabled=True,
                ),
            )
            assert sorted(results) == [False, True]
            acknowledged = await conn.fetchrow(
                "SELECT status,assigned_agent_id,lease_expires_at,freeze_data,"
                "error_message,error_details,context FROM jobs WHERE id=$1",
                job_id,
            )
            context = json.loads(acknowledged["context"])
            assert acknowledged["status"] == "paused"
            assert acknowledged["assigned_agent_id"] is None
            assert acknowledged["lease_expires_at"] is None
            assert acknowledged["freeze_data"] is None
            assert acknowledged["error_message"] is None
            assert acknowledged["error_details"] is None
            assert "_lease_recovery" not in context
            assert "completion_decision" not in context
            assert context["queued_feedback"] == "operator inspected incident"
            assert context["_lease_recovery_last_trip"]["recovery"]["generation"] == (
                generation
            )
            assert (
                context["_lease_recovery_last_trip"]["freeze_data"]["freeze_type"]
                == "redispatch_livelock"
            )

            # Two claimants see one dispatchable row; the jobs-row claim CAS
            # permits exactly one owner.
            claims = await asyncio.gather(
                db.claim_job_for_agent(str(job_id), str(agents[0]["id"])),
                db.claim_job_for_agent(str(job_id), str(agents[1]["id"])),
            )
            assert sorted(claims) == [False, True]

            # Acknowledgement removed the active episode, so the very next
            # unchanged loss is recovery 1, not 4. Two more real claim/expiry
            # cycles establish a fresh 1 -> 2 -> 3 window.
            await conn.execute(
                "UPDATE jobs SET lease_expires_at=now()-interval '1 second' "
                "WHERE id=$1",
                job_id,
            )
            first = await db.recover_expired_lease_jobs()
            assert first.recovered_job_ids == (str(job_id),)
            first_context = json.loads(
                await conn.fetchval("SELECT context FROM jobs WHERE id=$1", job_id)
            )
            assert first_context["_lease_recovery"]["unchanged_recoveries"] == 1
            assert first_context["_lease_recovery"]["generation"] != generation
            for expected in (2, 3):
                assert await db.claim_job_for_agent(
                    str(job_id), str(agents[expected % 2]["id"])
                )
                await conn.execute(
                    "UPDATE jobs SET lease_expires_at=now()-interval '1 second' "
                    "WHERE id=$1",
                    job_id,
                )
                result = await db.recover_expired_lease_jobs()
                current = json.loads(
                    await conn.fetchval("SELECT context FROM jobs WHERE id=$1", job_id)
                )
                assert current["_lease_recovery"]["unchanged_recoveries"] == expected
            assert len(result.circuit_trips) == 1
        finally:
            if db is not None:
                await db.disconnect()
            await conn.close()


@pytest.mark.asyncio
async def test_audit_fingerprint_timeout_cannot_stall_or_reset_recovery_sequence():
    with PostgresContainer("postgres:15") as pg:
        dsn = pg.get_connection_url().replace("postgresql+psycopg2", "postgresql")
        conn = await asyncpg.connect(dsn)
        db = None
        try:
            await conn.execute(SCHEMA_FILE.read_text())
            await conn.execute("SET search_path TO public")
            agent_id = await conn.fetchval(
                "INSERT INTO agents (config_name,hostname,status) "
                "VALUES ('defaults','timeout-agent','ready') RETURNING id"
            )
            job_id = await conn.fetchval(
                "INSERT INTO jobs (description,status,execution_lane,"
                "assigned_agent_id,lease_expires_at) VALUES "
                "('timeout target','processing','pinned',$1,"
                "now()-interval '1 second') RETURNING id",
                agent_id,
            )
            db = PostgresDB(connection_string=dsn, min_connections=1, max_connections=3)
            await db.connect()

            async def never_returns(_job_ids):
                await asyncio.Event().wait()
                return {}

            started = time.monotonic()
            for expected in (1, 2, 3):
                if expected > 1:
                    assert await db.claim_job_for_agent(str(job_id), str(agent_id))
                    await conn.execute(
                        "UPDATE jobs SET lease_expires_at=now()-interval '1 second' "
                        "WHERE id=$1",
                        job_id,
                    )
                result = await asyncio.wait_for(
                    db.recover_expired_lease_jobs(
                        audit_fingerprint_provider=never_returns,
                        audit_fingerprint_timeout_seconds=0.05,
                    ),
                    timeout=1.0,
                )
                context = json.loads(
                    await conn.fetchval("SELECT context FROM jobs WHERE id=$1", job_id)
                )
                assert context["_lease_recovery"]["unchanged_recoveries"] == expected
            assert time.monotonic() - started < 1.0
            assert len(result.circuit_trips) == 1
            assert context["_lease_recovery"]["fingerprint"]["audit"] == {
                "available": False
            }
        finally:
            if db is not None:
                await db.disconnect()
            await conn.close()
