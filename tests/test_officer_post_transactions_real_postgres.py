"""Real-PostgreSQL proofs for Officer Post admission and handoff authority.

The mocked suites prove endpoint plumbing; this module proves the properties
that depend on PostgreSQL row locks and rollback semantics:

* manual and automatic creates linearize on one stable project_officers row;
* final-slot and ticket-claim races insert exactly one job;
* stale preparation cannot cross hold/config/decommission/recommission;
* predecessor jobs occupy successor capacity through durable lineage; and
* every decommission substep rolls back as one unit, while a committed retry
  is idempotent and exposes durable (not delivered) route fallback intent.
"""

from __future__ import annotations


import asyncio
import json
import time
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from functools import partial
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

import orchestrator.main as orch_main
from orchestrator.routers import officers as officers_router
from orchestrator.services import officer_notices
from orchestrator.database.postgres import OfficerPostLifecycleConflict, PostgresDB
from orchestrator.schemas.job_create import JobCreate
from orchestrator.services.job_admission_config import JobAdmissionConfig
from orchestrator.services.job_admission_officer import (
    JobAdmissionOfficerDependencies,
    prepare_job_admission_officer,
)
from orchestrator.services.officer_admission import (
    OfficerAdmissionConflict,
    SlotAdmissionError,
    admit_and_create_job,
    admit_and_create_job_in_transaction,
    count_in_flight_by_slot,
    prepare_officer_admission,
)
from shared.persistent_input_delivery import (
    mark_input_delivery_queued,
    persist_input_delivery,
    transition_input_delivery,
)
from shared.workspace_contract import (
    LEGACY_K8S_RUNTIME_ADOPTION_KEY,
    WORKSPACE_CONTRACT_CONTEXT_KEY,
    WORKSPACE_DISPATCH_AUTHORITY_CONTEXT_KEY,
    resolve_workspace_runtime,
)
from shared.worker_queue import claim_worker_batch
from tests._previous_release_seed import (
    seed_previous_release_row as _seed_previous_release_row,
)
from orchestrator.application import jobs as jobs_composition
from orchestrator.application import workflows as workflows_composition
import functools

SCHEMA_FILE = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "orchestrator"
    / "database"
    / "schema_current.sql"
)
CLAIM_MIGRATION_FILE = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "orchestrator"
    / "database"
    / "migrations"
    / "app"
    / "0162_officer_ticket_claims.sql"
)
DELIVERABLE_MIGRATION_FILE = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "orchestrator"
    / "database"
    / "migrations"
    / "app"
    / "0182_deliverable_contract_authority.sql"
)
RUNTIME_AUTHORITY_MIGRATION_FILE = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "orchestrator"
    / "database"
    / "migrations"
    / "app"
    / "0185_thread_runtime_generation_retirement.sql"
)

DECOMMISSION_STEPS = (
    "post_locked",
    "thread_locked",
    "in_flight_checked",
    "state_harvested",
    "wake_rows_locked",
    "wake_entries_folded",
    "wakes_cleared",
    "routes_fallback_staged",
    "incarnation_appended",
    "post_unlinked",
    "thread_disabled",
)

READY_GENERATION = datetime(2026, 8, 16, 7, 0, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def pg_dsn():
    try:
        container = PostgresContainer("postgres:15")
        container.start()
    except Exception as exc:
        pytest.skip(f"local Postgres container unavailable: {exc}")
    try:
        yield container.get_connection_url().replace(
            "postgresql+psycopg2", "postgresql"
        )
    finally:
        container.stop()


@pytest_asyncio.fixture(scope="module")
async def _schema_applied(pg_dsn):
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(SCHEMA_FILE.read_text())
        await conn.execute(RUNTIME_AUTHORITY_MIGRATION_FILE.read_text())
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def db(pg_dsn, _schema_applied, monkeypatch):
    monkeypatch.setenv("EXPERTS_DB_ENABLED", "false")
    store = PostgresDB(
        connection_string=pg_dsn,
        min_connections=1,
        max_connections=10,
    )
    await store.connect()
    async with store.acquire() as conn:
        # Several migration-boundary cases below intentionally drop the claim
        # ledger and replay 0162 in this module-scoped database. Restore the
        # small 0182 claim-audit tail before the next independently isolated
        # test so later production methods see the actual migration head.
        if not await conn.fetchval(
            "SELECT to_regclass('public.officer_ticket_claims') IS NOT NULL"
        ):
            await conn.execute(CLAIM_MIGRATION_FILE.read_text())
        await conn.execute(
            """
            ALTER TABLE officer_ticket_claims
                ADD COLUMN IF NOT EXISTS completion_outcome_kind_at_delete TEXT;
            DO $restore_0182$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint
                     WHERE conname = 'officer_ticket_claim_delete_outcome_kind'
                       AND conrelid = 'officer_ticket_claims'::regclass
                ) THEN
                    ALTER TABLE officer_ticket_claims
                        ADD CONSTRAINT officer_ticket_claim_delete_outcome_kind
                        CHECK (
                            completion_outcome_kind_at_delete IS NULL
                            OR completion_outcome_kind_at_delete =
                               'blocked_undelivered'
                        ) NOT VALID;
                END IF;
            END
            $restore_0182$;
            CREATE OR REPLACE FUNCTION audit_officer_ticket_claim_job_delete()
            RETURNS trigger
            LANGUAGE plpgsql
            AS $function$
            BEGIN
                UPDATE officer_ticket_claims
                   SET job_deleted_at = COALESCE(
                           job_deleted_at, statement_timestamp()
                       ),
                       job_status_at_delete = COALESCE(
                           job_status_at_delete, OLD.status
                       ),
                       completion_outcome_kind_at_delete = COALESCE(
                           completion_outcome_kind_at_delete,
                           OLD.completion_outcome_kind
                       ),
                       deletion_reason = COALESCE(
                           deletion_reason,
                           'database_delete_compatibility_trigger'
                       )
                 WHERE job_id = OLD.id;
                RETURN OLD;
            END
            $function$;
            """
        )
        if not await conn.fetchval(
            """
            SELECT EXISTS (
                SELECT 1 FROM pg_trigger
                 WHERE tgname = 'trg_officer_ticket_delivery_writer'
                   AND tgrelid = 'jobs'::regclass
                   AND NOT tgisinternal
            )
            """
        ):
            migration_sql = DELIVERABLE_MIGRATION_FILE.read_text()
            start = migration_sql.index(
                "CREATE FUNCTION public.enforce_officer_ticket_delivery_writer()"
            )
            terminator = (
                "FOR EACH ROW EXECUTE FUNCTION "
                "public.enforce_officer_ticket_delivery_writer();"
            )
            end = migration_sql.index(terminator, start) + len(terminator)
            await conn.execute(migration_sql[start:end])
        await conn.execute(
            "TRUNCATE job_message_routes, session_wake_events, message_log, "
            "jobs, project_officers, threads, projects CASCADE"
        )
        # TRUNCATE reaches users and capability_grants through their FKs,
        # including nullable granted_by; seed after resetting every test.
        await conn.execute("""
            INSERT INTO capability_grants(scope_kind,key,value_json) VALUES
                ('global','shell_tools','true'),
                ('global','delegation','true'),
                ('global','vm_workspace','true'),
                ('global','autonomy_ceiling','"full"')
            ON CONFLICT(scope_kind,scope_id,key) DO UPDATE SET value_json=EXCLUDED.value_json
        """)
    from orchestrator.services.manifest_experts import seed_bundled_expert_manifests

    await seed_bundled_expert_manifests(
        store, Path(__file__).resolve().parents[1] / "config"
    )
    try:
        yield store
    finally:
        await store.close()


def _roster(*, count: int = 1) -> dict:
    return {
        "line": {
            "count": count,
            "category": "executor",
            "model": "MiniMax-M3",
            "backend": "sandbox",
        }
    }


async def _seed_post(
    db: PostgresDB,
    *,
    count: int = 1,
    auto_pull: bool = False,
    post_state: dict | None = None,
    officer_state: dict | None = None,
) -> dict[str, str]:
    project_id = uuid4()
    thread_id = uuid4()
    owner_id = uuid4()
    runtime_officer = {
        "enabled": True,
        "auto_pull": auto_pull,
        "slots": _roster(count=count),
    }
    metadata = {
        "config_override": {"officer": runtime_officer},
        "officer_state": officer_state or {},
    }
    durable_config = {"officer": {"slots": _roster(count=count)}}
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO projects (id, name) VALUES ($1, 'post tx proof')",
            project_id,
        )
        # Config edits now publish a canonical Project under its actual owner.
        await conn.execute(
            "INSERT INTO users(id,display_name,is_approved,default_project_id) VALUES($1,'Post owner',TRUE,$2)",
            owner_id,
            project_id,
        )
        await conn.execute(
            "INSERT INTO project_members(project_id,user_id,role) VALUES($1,$2,'owner')",
            project_id,
            owner_id,
        )
        await conn.execute(
            """
            INSERT INTO threads (id, project_id, status, metadata)
            VALUES ($1, $2, 'active', $3::jsonb)
            """,
            thread_id,
            project_id,
            json.dumps(metadata),
        )
        await conn.execute(
            """
            INSERT INTO project_officers (
                project_id, thread_id, config_override, state
            ) VALUES ($1, $2, $3::jsonb, $4::jsonb)
            """,
            project_id,
            thread_id,
            json.dumps(durable_config),
            json.dumps(post_state or {}),
        )
    return {"project_id": str(project_id), "thread_id": str(thread_id)}


async def _seed_successor(db: PostgresDB, seed: dict[str, str], *, count: int = 1):
    thread_id = uuid4()
    metadata = {
        "config_override": {
            "officer": {
                "enabled": True,
                "auto_pull": False,
                "slots": _roster(count=count),
            }
        }
    }
    async with db.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO threads (id, project_id, status, metadata)
            VALUES ($1, $2, 'active', $3::jsonb)
            """,
            thread_id,
            UUID(seed["project_id"]),
            json.dumps(metadata),
        )
    return str(thread_id)


async def _prepare(
    db: PostgresDB,
    seed: dict[str, str],
    *,
    auto_pull: bool = False,
    requested_config_override: dict | None = None,
):
    return await prepare_officer_admission(
        db,
        project_id=seed["project_id"],
        thread_id=seed["thread_id"],
        requested_slot="line",
        require_auto_pull=auto_pull,
        expected_category="executor" if auto_pull else None,
        requested_config_override=requested_config_override,
    )


def _job_kwargs(label: str) -> dict:
    return {
        "description": label,
        "config_name": "worker_base",
        "config_override": {"autonomy": "full"},
        "context": {},
        "wake_on_complete": True,
    }


async def _job_count(db: PostgresDB) -> int:
    async with db.acquire() as conn:
        return int(await conn.fetchval("SELECT COUNT(*) FROM jobs"))


async def _claim_rows(
    db: PostgresDB, project_id: str, ticket_note_id: str | None = None
) -> list[dict]:
    async with db.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT *
              FROM officer_ticket_claims
             WHERE project_id = $1
               AND ($2::text IS NULL OR ticket_note_id = $2)
             ORDER BY ready_generation_at, claimed_at, id
            """,
            UUID(project_id),
            ticket_note_id,
        )
    return [dict(row) for row in rows]


def _plan_index_names(node: dict) -> set[str]:
    names = {str(node["Index Name"])} if node.get("Index Name") else set()
    for child in node.get("Plans") or []:
        names.update(_plan_index_names(child))
    return names


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("override", "code"),
    [
        ({"workspace": {"backend": "vm"}}, "slot_backend_conflict"),
        ({"llm": {"model": "gpt-5.6-sol"}}, "slot_model_conflict"),
    ],
)
async def test_slot_pin_conflict_precedes_job_and_ticket_claim(db, override, code):
    seed = await _seed_post(db)

    with pytest.raises(OfficerAdmissionConflict) as raised:
        await _prepare(db, seed, requested_config_override=override)

    assert raised.value.code == code
    assert raised.value.fields["slot"] == "line"
    assert await _job_count(db) == 0
    assert await _claim_rows(db, seed["project_id"]) == []


@pytest.mark.asyncio
async def test_slot_default_stamps_one_server_owned_workspace_contract(db):
    seed = await _seed_post(db)
    preparation = await _prepare(db, seed)
    job = await admit_and_create_job(
        db,
        preparation=preparation,
        job_kwargs=_job_kwargs("slot default contract"),
        ticket_note_id="slot-default-contract",
        ticket_ready_at=READY_GENERATION,
    )
    stored = await db.get_job(str(job["id"]))
    context = _json(stored["context"])
    config = _json(stored["config_override"])

    assert config["workspace"]["backend"] == "sandbox"
    assert context["_workspace_contract"] == {
        "version": 1,
        "requested_backend": None,
        "assigned_backend": "sandbox",
        "assignment_source": "officer_slot:line",
    }
    assert len(await _claim_rows(db, seed["project_id"])) == 1


@pytest.mark.asyncio
async def test_common_create_funnel_strips_forged_workspace_authority(db):
    job = await db.create_job(
        description="forged workspace authority",
        config_override={
            "workspace": {
                "backend": "remote",
                "remote": {"host": "foreign.internal", "key_path": "/secret"},
            }
        },
        context={
            "workspace_backend": "sandbox",
            "_workspace_contract": {
                "version": 1,
                "assigned_backend": "sandbox",
                "assignment_source": "caller",
            },
            "workspace_runtime": {"effective_backend": "sandbox"},
            "workspace_container": {
                "status": "ready",
                "host": "foreign.internal",
            },
            "vm": {"status": "ready", "ssh_host": "foreign-vm.internal"},
        },
    )
    stored = await db.get_job(str(job["id"]))
    context = _json(stored["context"])
    config = _json(stored["config_override"])

    assert config["workspace"] == {"backend": "vm"}
    assert context["_workspace_contract"] == {
        "version": 1,
        "requested_backend": "vm",
        "assigned_backend": "vm",
        "assignment_source": "resolved_config",
    }
    assert not {
        "workspace_backend",
        "workspace_runtime",
        "workspace_container",
        "vm",
    }.intersection(context)


@pytest.mark.asyncio
async def test_opposite_tier_callbacks_cannot_change_effective_backend(db):
    vm_job = await db.create_job(
        description="vm callback race",
        config_override={"workspace": {"backend": "vm"}},
        requested_workspace_backend="vm",
        workspace_assignment_source="request",
    )
    await _merge_previous_release_workspace(
        db,
        vm_job["id"],
        {
            "status": "ready",
            "host": "stale-container.internal",
            "_runtime_incarnation": str(uuid4()),
        },
    )
    vm_mismatch = resolve_workspace_runtime(await db.get_job(str(vm_job["id"])))
    assert vm_mismatch.state == "mismatch"
    assert vm_mismatch.effective_backend is None

    await db.merge_vm_context(
        str(vm_job["id"]),
        {
            "status": "ready",
            "ssh_host": "current-vm.internal",
            "provision_generation": str(uuid4()),
        },
    )
    vm_ready = resolve_workspace_runtime(await db.get_job(str(vm_job["id"])))
    assert vm_ready.effective_backend == "vm"
    assert vm_ready.stale_backend == "sandbox"

    sandbox_job = await db.create_job(
        description="sandbox callback race",
        config_override={"workspace": {"backend": "sandbox"}},
        requested_workspace_backend="sandbox",
        workspace_assignment_source="request",
    )
    await asyncio.gather(
        db.merge_vm_context(
            str(sandbox_job["id"]),
            {
                "status": "ready",
                "ssh_host": "late-vm.internal",
                "provision_generation": str(uuid4()),
            },
        ),
        _merge_previous_release_workspace(
            db,
            sandbox_job["id"],
            {
                "status": "ready",
                "host": "current-container.internal",
                "_runtime_incarnation": str(uuid4()),
            },
        ),
    )
    sandbox_ready = resolve_workspace_runtime(await db.get_job(str(sandbox_job["id"])))
    assert sandbox_ready.effective_backend == "sandbox"
    assert sandbox_ready.stale_backend == "vm"


@pytest.mark.asyncio
async def test_stateless_admission_cannot_claim_vm_or_ambiguous_legacy_job(db):
    vm_job = await db.create_job(
        description="stateless VM refusal",
        config_override={"workspace": {"backend": "vm"}},
        requested_workspace_backend="vm",
        workspace_assignment_source="request",
        execution_lane="stateless",
    )
    await _merge_previous_release_workspace(
        db,
        vm_job["id"],
        {
            "status": "ready",
            "provisioner": "k8s",
            "pod_ip": "10.42.0.10",
            "_runtime_incarnation": str(uuid4()),
        },
    )
    assert await db.admit_stateless_worker_job(
        str(vm_job["id"]), fair_key=None, priority=5
    ) == (False, None)

    legacy = await db.create_job(
        description="observed legacy ambiguity",
        config_override={"workspace": {"backend": "sandbox"}},
        execution_lane="stateless",
    )
    async with db.acquire() as conn:
        await _seed_previous_release_row(
            conn,
            "jobs",
            """
            UPDATE jobs
               SET context = (context - '_workspace_contract')
                   || jsonb_build_object(
                       'workspace_backend', 'vm',
                       'workspace_container', jsonb_build_object(
                           'status', 'ready',
                           'provisioner', 'k8s',
                           'pod_ip', '10.42.0.11',
                           '_runtime_incarnation', $2::text
                       )
                   )
             WHERE id = $1
            """,
            legacy["id"],
            str(uuid4()),
        )
    assert await db.admit_stateless_worker_job(
        str(legacy["id"]), fair_key=None, priority=5
    ) == (False, None)

    async with db.acquire() as conn:
        assert not await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM run_queue WHERE unit_id=ANY($1::uuid[]))",
            [vm_job["id"], legacy["id"]],
        )


@pytest.mark.asyncio
async def test_stateless_claim_is_bound_to_exact_queue_lease_and_tier(db):
    job = await db.create_job(
        description="stateless dispatch fence",
        config_override={"workspace": {"backend": "sandbox"}},
        requested_workspace_backend="sandbox",
        workspace_assignment_source="request",
        execution_lane="stateless",
    )
    await _merge_previous_release_workspace(
        db,
        job["id"],
        {
            "status": "ready",
            "provisioner": "k8s",
            "pod_ip": "10.42.0.12",
            "_runtime_incarnation": str(uuid4()),
        },
    )
    admitted, _queue_result = await db.admit_stateless_worker_job(
        str(job["id"]), fair_key=None, priority=5
    )
    assert admitted is True

    # The immediately previous worker CAS leased the queue then changed only
    # jobs.status. Migration 0175 must roll back both writes rather than let an
    # old replica dispatch without the tier/lease marker.
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.CheckViolationError) as raised:
            async with conn.transaction():
                await conn.execute(
                    """
                    UPDATE run_queue
                       SET state='leased', lease_token=lease_token+1,
                           leased_by='pre-contract-worker',
                           last_leased_by='pre-contract-worker',
                           leased_until=now()+interval '60 seconds'
                     WHERE unit_id=$1
                    """,
                    job["id"],
                )
                await conn.execute(
                    """
                    UPDATE jobs
                       SET status='processing', assigned_agent_id=NULL,
                           lease_expires_at=NULL
                     WHERE id=$1
                    """,
                    job["id"],
                )
        assert (
            raised.value.constraint_name == "job_workspace_dispatch_requires_authority"
        )
        queue = await conn.fetchrow(
            "SELECT state, lease_token, leased_by FROM run_queue WHERE unit_id=$1",
            job["id"],
        )
        stored = await conn.fetchrow(
            "SELECT status, context FROM jobs WHERE id=$1", job["id"]
        )
        assert dict(queue) == {
            "state": "queued",
            "lease_token": 0,
            "leased_by": None,
        }
        assert stored["status"] == "created"
        assert WORKSPACE_DISPATCH_AUTHORITY_CONTEXT_KEY not in _json(stored["context"])

    claim = await claim_worker_batch(
        db,
        pod_name="workspace-contract-worker",
        affinity_grace_seconds=0,
    )
    assert claim is not None
    stored = await db.get_job(str(job["id"]))
    marker = _json(stored["context"])[WORKSPACE_DISPATCH_AUTHORITY_CONTEXT_KEY]
    assert marker["dispatch_kind"] == "stateless"
    assert marker["assigned_backend"] == "sandbox"
    assert marker["worker_pod"] == "workspace-contract-worker"
    assert marker["queue_lease_token"] == claim.lease_token
    assert marker["queue_leased_until"]


@pytest.mark.asyncio
async def test_lite_to_sandbox_contract_transition_has_one_cas_winner(db):
    job = await db.create_job(
        description="atomic tier transition",
        config_override={"workspace": {"backend": "virtual"}},
        requested_workspace_backend="virtual",
        workspace_assignment_source="request",
    )
    async with db.acquire() as conn:
        await conn.execute("UPDATE jobs SET status='processing' WHERE id=$1", job["id"])

    outcomes = await asyncio.gather(
        *(
            db.begin_job_workspace_tier_transition(
                str(job["id"]),
                expected_backend="virtual",
                target_backend="sandbox",
                requested_backend="virtual",
                assignment_source="runtime_workspace_upgrade",
                expected_status="processing",
            )
            for _ in range(2)
        )
    )
    assert sorted(outcomes) == [False, True]

    stored = await db.get_job(str(job["id"]))
    context = _json(stored["context"])
    config = _json(stored["config_override"])
    assert config["workspace"]["backend"] == "sandbox"
    assert context["workspace_container"]["status"] == "pending"
    assert context["_workspace_contract"] == {
        "version": 1,
        "requested_backend": "virtual",
        "assigned_backend": "sandbox",
        "assignment_source": "runtime_workspace_upgrade",
    }


@pytest.mark.asyncio
async def test_pre_contract_officer_insert_rolls_back_ticket_claim(db):
    """A pre-0175 admission replica cannot commit half of BP-05's pair."""

    seed = await _seed_post(db)
    job_id = uuid4()
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.CheckViolationError) as raised:
            async with conn.transaction():
                await conn.execute(
                    """
                    INSERT INTO officer_ticket_claims (
                        project_id, ticket_note_id, ready_generation_at,
                        source, officer_thread_id, officer_incarnation,
                        officer_slot, work_category,
                        admission_config_fingerprint,
                        admission_lineage_size, job_id
                    ) VALUES (
                        $1, 'old-replica-ticket', $3, 'manual', $2, 0,
                        'line', 'executor', $4, 1, $5
                    )
                    """,
                    UUID(seed["project_id"]),
                    UUID(seed["thread_id"]),
                    READY_GENERATION,
                    "a" * 64,
                    job_id,
                )
                # This is the production shape an immediately previous
                # Officer-admission replica could emit: valid BP-05 authority,
                # but no workspace-contract stamp.
                await conn.execute(
                    """
                    INSERT INTO jobs (
                        id, description, status, project_id,
                        created_by_thread_id, origin, execution_lane,
                        config_override, context
                    ) VALUES (
                        $1, 'old replica Officer job', 'created', $2, $3,
                        'officer', 'pinned',
                        '{"workspace":{"backend":"sandbox"}}'::jsonb,
                        '{}'::jsonb
                    )
                    """,
                    job_id,
                    UUID(seed["project_id"]),
                    UUID(seed["thread_id"]),
                )
        assert raised.value.constraint_name == "officer_job_requires_workspace_contract"
        assert not await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM jobs WHERE id=$1)", job_id
        )
        assert not await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM officer_ticket_claims WHERE job_id=$1)",
            job_id,
        )


@pytest.mark.asyncio
async def test_dispatch_claim_requires_atomic_workspace_authority_marker(db):
    """An old claimant fails closed; the current claimant binds agent + lease."""

    job = await db.create_job(
        description="mixed-version dispatch fence",
        config_override={"workspace": {"backend": "sandbox"}},
        requested_workspace_backend="sandbox",
        workspace_assignment_source="request",
    )
    agent_id = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO agents (
                id, config_name, hostname, pod_ip, pod_uid, status, agent_mode
            ) VALUES ($1, 'worker_base', $2, '127.0.0.1', $3, 'ready', 'worker')
            """,
            agent_id,
            f"workspace-contract-{agent_id}",
            f"workspace-contract-pod-{agent_id}",
        )
        with pytest.raises(asyncpg.CheckViolationError) as raised:
            await conn.execute(
                """
                UPDATE jobs
                   SET status='processing', assigned_agent_id=$2,
                       lease_expires_at=now()+interval '60 seconds'
                 WHERE id=$1
                """,
                job["id"],
                agent_id,
            )
        assert (
            raised.value.constraint_name == "job_workspace_dispatch_requires_authority"
        )
        unchanged = await conn.fetchrow(
            "SELECT status, assigned_agent_id FROM jobs WHERE id=$1", job["id"]
        )
        assert unchanged["status"] == "created"
        assert unchanged["assigned_agent_id"] is None

    assert await db.claim_job_for_agent(str(job["id"]), str(agent_id)) is True
    stored = await db.get_job(str(job["id"]))
    context = _json(stored["context"])
    marker = context[WORKSPACE_DISPATCH_AUTHORITY_CONTEXT_KEY]
    assert marker["version"] == 1
    assert marker["dispatch_kind"] == "pinned"
    assert marker["contract_version"] == 1
    assert marker["assigned_backend"] == "sandbox"
    assert marker["agent_id"] == str(agent_id)
    assert marker["lease_expires_at"]
    assert not any(
        private in repr(marker)
        for private in ("host", "port", "token", "credential", "key")
    )

    public = jobs_composition.redact_job_config_override(stored)
    assert WORKSPACE_DISPATCH_AUTHORITY_CONTEXT_KEY not in repr(public)
    assert WORKSPACE_CONTRACT_CONTEXT_KEY not in repr(public.get("context"))


@pytest.mark.asyncio
async def test_legacy_workspace_claim_is_conservative_and_fenced(db):
    agent_id = uuid4()
    good = await db.create_job(
        description="unambiguous legacy sandbox",
        config_override={"workspace": {"backend": "sandbox"}},
    )
    bad = await db.create_job(
        description="contradictory legacy workspace",
        config_override={"workspace": {"backend": "sandbox"}},
    )
    async with db.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO agents (
                id, config_name, hostname, pod_ip, pod_uid, status, agent_mode
            ) VALUES ($1, 'worker_base', $2, '127.0.0.1', $3, 'ready', 'worker')
            """,
            agent_id,
            f"legacy-workspace-{agent_id}",
            f"legacy-workspace-pod-{agent_id}",
        )
        await conn.execute(
            """
            UPDATE jobs
               SET context=(context - $2::text)
             WHERE id=$1
            """,
            good["id"],
            WORKSPACE_CONTRACT_CONTEXT_KEY,
        )
        await conn.execute(
            """
            UPDATE jobs
               SET context=(context - $2::text)
                    || jsonb_build_object('workspace_backend', 'vm')
             WHERE id=$1
            """,
            bad["id"],
            WORKSPACE_CONTRACT_CONTEXT_KEY,
        )

    assert await db.claim_job_for_agent(str(bad["id"]), str(agent_id)) is False
    assert await db.claim_job_for_agent(str(good["id"]), str(agent_id)) is True
    stored = await db.get_job(str(good["id"]))
    marker = _json(stored["context"])[WORKSPACE_DISPATCH_AUTHORITY_CONTEXT_KEY]
    assert marker["contract_version"] == 0
    assert marker["assigned_backend"] == "sandbox"


# 0197/0198 fence a *writer* that publishes Kubernetes runtime authority with
# no durable reservation behind it.  Rows written by the previous release were
# never subject to that fence -- the trigger did not exist yet.  Reproducing
# that ordering means seeding with the exact named trigger absent and then
# putting it back, which is also the proof that installing the migration over
# historical data is accepted.  This is deliberately not
# `session_replication_role = replica`: that would silently drop foreign keys
# and every other trigger in the statement, including the ones these tests are
# about.


async def _merge_previous_release_workspace(db, job_id, payload):
    """Land a workspace_container callback the way the previous release did.

    0198 now requires exact creation-reservation authority behind any Pod UID
    a writer publishes.  The tier-resolution and stateless-admission proofs
    below are about what the *reader* concludes from such a projection, not
    about how it came to exist, so they install it as a pre-tranche writer.
    """

    async with db.acquire() as conn:
        await _seed_previous_release_row(
            conn,
            "jobs",
            "UPDATE jobs SET context = jsonb_set("
            "COALESCE(context, '{}'::jsonb), '{workspace_container}', "
            "COALESCE(context->'workspace_container', '{}'::jsonb) "
            "|| $2::jsonb, true) WHERE id = $1",
            job_id if not isinstance(job_id, str) else UUID(job_id),
            json.dumps(payload),
        )


async def _seed_exact_pre_0175_k8s_job(db, *, status="created"):
    """Persist the exact K8s job-runtime JSON emitted by the prior release."""

    job = await db.create_job(
        description="pre-0175 Kubernetes job runtime",
        config_override={"workspace": {"backend": "sandbox"}},
    )
    historical_context = {
        "workspace_container": {
            "status": "ready",
            "provisioner": "k8s",
            "pod_ip": "10.42.1.17",
            "port": 30022,
            "host": "workspace-job.internal",
        }
    }
    async with db.acquire() as conn:
        await _seed_previous_release_row(
            conn,
            "jobs",
            """
            UPDATE jobs
               SET status=$2,
                   config_override='{"workspace":{"backend":"container"}}'::jsonb,
                   context=$3::jsonb
             WHERE id=$1
            """,
            job["id"],
            status,
            json.dumps(historical_context),
        )
    stored = await db.get_job(str(job["id"]))
    assert _json(stored["context"]) == historical_context
    return stored


def _k8s_job_attestation(
    *,
    runtime_incarnation=None,
    host="workspace-job.internal",
    pod_ip="10.42.1.17",
):
    from orchestrator.services.container_provisioner import WorkspaceRuntimeAttestation

    backing = str(uuid4())
    return WorkspaceRuntimeAttestation(
        backing_id=f"k8s-pvc:test:{backing}",
        workspace_generation=backing,
        runtime_incarnation=runtime_incarnation or str(uuid4()),
        ssh_host_key_fingerprint="SHA256:" + ("a" * 43),
        host=host,
        pod_ip=pod_ip,
        port=30022,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["created", "paused"])
async def test_pre_0175_k8s_job_runtime_adopts_after_live_attestation(db, status):
    from orchestrator.services.job_workspace_adoption import (
        LegacyK8sAdoptionOutcome,
        ensure_legacy_k8s_job_runtime_authority,
    )

    job = await _seed_exact_pre_0175_k8s_job(db, status=status)
    attestation = _k8s_job_attestation()
    provisioner = SimpleNamespace(
        attest_workspace_runtime=AsyncMock(return_value=attestation)
    )

    result = await ensure_legacy_k8s_job_runtime_authority(db, provisioner, job)

    assert result.outcome is LegacyK8sAdoptionOutcome.ADOPTED
    stored = await db.get_job(str(job["id"]))
    runtime = _json(stored["context"])["workspace_container"]
    assert runtime["_runtime_incarnation"] == attestation.runtime_incarnation
    assert runtime["_legacy_k8s_runtime_adoption"] == {
        "version": 1,
        "runtime_incarnation": attestation.runtime_incarnation,
        "workspace_generation": attestation.workspace_generation,
        "ssh_host_key_fingerprint": attestation.ssh_host_key_fingerprint,
    }
    assert resolve_workspace_runtime(stored).ready


@pytest.mark.asyncio
async def test_pre_0175_k8s_adoption_is_one_cas_and_claim_marker_is_preserved(db):
    from orchestrator.services.job_workspace_adoption import (
        LegacyK8sAdoptionOutcome,
        ensure_legacy_k8s_job_runtime_authority,
    )

    job = await _seed_exact_pre_0175_k8s_job(db)
    inventory = await db.list_uidless_k8s_job_workspace_rows()
    assert [str(row["id"]) for row in inventory] == [str(job["id"])]
    attestation = _k8s_job_attestation()
    provisioner = SimpleNamespace(
        attest_workspace_runtime=AsyncMock(return_value=attestation)
    )

    outcomes = await asyncio.gather(
        ensure_legacy_k8s_job_runtime_authority(db, provisioner, job),
        ensure_legacy_k8s_job_runtime_authority(db, provisioner, job),
    )

    observed = [outcome.outcome for outcome in outcomes]
    assert observed.count(LegacyK8sAdoptionOutcome.ADOPTED) == 1
    assert set(observed).issubset(
        {
            LegacyK8sAdoptionOutcome.ADOPTED,
            LegacyK8sAdoptionOutcome.CONVERGED,
        }
    )
    converged = await ensure_legacy_k8s_job_runtime_authority(db, provisioner, job)
    assert converged.outcome is LegacyK8sAdoptionOutcome.CONVERGED
    assert await db.list_uidless_k8s_job_workspace_rows() == []

    agent_id = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO agents (
                id, config_name, hostname, pod_ip, pod_uid, status, agent_mode
            ) VALUES ($1, 'worker_base', $2, '127.0.0.1', $3, 'ready', 'worker')
            """,
            agent_id,
            f"legacy-adoption-{agent_id}",
            f"legacy-adoption-pod-{agent_id}",
        )
    assert await db.claim_job_for_agent(str(job["id"]), str(agent_id)) is True
    claimed = await db.get_job(str(job["id"]))
    context = _json(claimed["context"])
    assert context["workspace_container"]["_runtime_incarnation"] == (
        attestation.runtime_incarnation
    )
    assert (
        context[WORKSPACE_DISPATCH_AUTHORITY_CONTEXT_KEY]["assigned_backend"]
        == "sandbox"
    )


@pytest.mark.asyncio
async def test_pre_0175_adoption_cas_yields_to_concurrent_tier_transition(db):
    from orchestrator.services.job_workspace_adoption import (
        LegacyK8sAdoptionOutcome,
        ensure_legacy_k8s_job_runtime_authority,
    )

    job = await _seed_exact_pre_0175_k8s_job(db)
    started = asyncio.Event()
    release = asyncio.Event()
    attestation = _k8s_job_attestation()

    async def delayed_attestation(_owner):
        started.set()
        await release.wait()
        return attestation

    provisioner = SimpleNamespace(attest_workspace_runtime=delayed_attestation)
    adopting = asyncio.create_task(
        ensure_legacy_k8s_job_runtime_authority(db, provisioner, job)
    )
    await asyncio.wait_for(started.wait(), timeout=2)
    vm_generation = str(uuid4())
    async with db.acquire() as conn:
        # The racing writer is the previous release's tier transition: it
        # abandons a UID-less Kubernetes projection wholesale, which 0198 now
        # governs.  The subject here is the adoption CAS yielding to it, so
        # seed it the way the migration met such a writer.
        await _seed_previous_release_row(
            conn,
            "jobs",
            """
            UPDATE jobs
               SET config_override='{"workspace":{"backend":"vm"}}'::jsonb,
                   context=$2::jsonb
             WHERE id=$1
            """,
            job["id"],
            json.dumps(
                {
                    "_workspace_contract": {
                        "version": 1,
                        "requested_backend": "vm",
                        "assigned_backend": "vm",
                        "assignment_source": "operator",
                    },
                    "vm": {
                        "status": "ready",
                        "ssh_host": "vm.internal",
                        "ssh_port": 22,
                        "provision_generation": vm_generation,
                    },
                }
            ),
        )
    release.set()
    outcome = await asyncio.wait_for(adopting, timeout=2)

    assert outcome.outcome is LegacyK8sAdoptionOutcome.NOT_NEEDED
    assert outcome.reason == "workspace_snapshot_changed"
    stored = await db.get_job(str(job["id"]))
    assert resolve_workspace_runtime(stored).effective_backend == "vm"
    assert "_legacy_k8s_runtime_adoption" not in repr(stored)


@pytest.mark.asyncio
async def test_pre_0175_post_cas_pod_replacement_reverts_tentative_stamp(db):
    from orchestrator.services.job_workspace_adoption import (
        LegacyK8sAdoptionOutcome,
        ensure_legacy_k8s_job_runtime_authority,
    )

    job = await _seed_exact_pre_0175_k8s_job(db)
    predecessor = _k8s_job_attestation()
    provisioner = SimpleNamespace(
        attest_workspace_runtime=AsyncMock(
            side_effect=[
                predecessor,
                predecessor,
                _k8s_job_attestation(),
            ]
        )
    )

    outcome = await ensure_legacy_k8s_job_runtime_authority(db, provisioner, job)

    assert outcome.outcome is LegacyK8sAdoptionOutcome.RETRY
    assert outcome.reason == "kubernetes_runtime_changed_after_persistence"
    stored = await db.get_job(str(job["id"]))
    assert _json(stored["context"]) == {
        "workspace_container": {
            "status": "ready",
            "provisioner": "k8s",
            "pod_ip": "10.42.1.17",
            "port": 30022,
            "host": "workspace-job.internal",
        }
    }


@pytest.mark.asyncio
async def test_adoption_is_one_shot_and_a_replacement_needs_creation_authority(db):
    """Adoption converts one historical row once, and never reopens it.

    The previous release could hand the same deterministic name to a fresh
    Pod, so re-adoption used to be the recovery path for a replaced runtime.
    Once a row carries a durable reservation and a proven Pod UID it is no
    longer historical: a replacement is an ordinary creation and must present
    exact creation authority.  Adoption must not remain available as a second,
    evidence-lighter door onto the same row.
    """

    from orchestrator.services.job_workspace_adoption import (
        LegacyK8sAdoptionOutcome,
        ensure_legacy_k8s_job_runtime_authority,
    )

    job = await _seed_exact_pre_0175_k8s_job(db, status="paused")
    predecessor = _k8s_job_attestation()
    provisioner = SimpleNamespace(
        attest_workspace_runtime=AsyncMock(return_value=predecessor)
    )
    adopted = await ensure_legacy_k8s_job_runtime_authority(db, provisioner, job)
    assert adopted.outcome is LegacyK8sAdoptionOutcome.ADOPTED

    # Leaving the adopted runtime is the cleanup fence's business now.
    with pytest.raises(asyncpg.CheckViolationError):
        await db.merge_workspace_container_context(
            str(job["id"]), {"status": "deleted"}
        )

    # A replacement Pod UID is a creation, and creations need a reservation.
    replacement = _k8s_job_attestation(
        host="workspace-replacement.internal", pod_ip="10.42.2.19"
    )
    with pytest.raises(asyncpg.CheckViolationError):
        await db.merge_workspace_container_context(
            str(job["id"]),
            {
                "status": "ready",
                "provisioner": "k8s",
                "host": replacement.host,
                "pod_ip": replacement.pod_ip,
                "port": replacement.port,
                "_runtime_incarnation": replacement.runtime_incarnation,
            },
        )

    # And the bridge itself refuses to open a second generation over a row
    # that is no longer the exact UID-less historical shape.
    provisioner.attest_workspace_runtime.return_value = replacement
    again = await ensure_legacy_k8s_job_runtime_authority(
        db, provisioner, await db.get_job(str(job["id"]))
    )
    assert again.outcome is LegacyK8sAdoptionOutcome.RETRY
    assert again.reason == "adoption_reservation_unavailable"

    stored = await db.get_job(str(job["id"]))
    runtime = _json(stored["context"])["workspace_container"]
    assert runtime["_runtime_incarnation"] == predecessor.runtime_incarnation
    assert runtime[LEGACY_K8S_RUNTIME_ADOPTION_KEY]["workspace_generation"] == (
        predecessor.workspace_generation
    )
    assert resolve_workspace_runtime(stored).ready


@pytest.mark.asyncio
async def test_historical_non_ready_k8s_job_is_never_adopted(db):
    """A previous-release row that is not ready is not an adoption candidate.

    Ordinary workspace recovery must stay free to delete and recreate it
    rather than wait for an attestation that cannot succeed, so the bridge
    refuses before it reads Kubernetes at all.
    """

    from orchestrator.services.job_workspace_adoption import (
        LegacyK8sAdoptionOutcome,
        ensure_legacy_k8s_job_runtime_authority,
    )

    job = await _seed_exact_pre_0175_k8s_job(db, status="paused")
    async with db.acquire() as conn:
        await _seed_previous_release_row(
            conn,
            "jobs",
            "UPDATE jobs SET context = jsonb_set(context, "
            "'{workspace_container,status}', '\"deleted\"'::jsonb) WHERE id = $1",
            job["id"],
        )
    provisioner = SimpleNamespace(attest_workspace_runtime=AsyncMock())

    outcome = await ensure_legacy_k8s_job_runtime_authority(
        db, provisioner, await db.get_job(str(job["id"]))
    )

    assert outcome.outcome is LegacyK8sAdoptionOutcome.NOT_NEEDED
    provisioner.attest_workspace_runtime.assert_not_awaited()


@pytest.mark.asyncio
async def test_stamped_sandbox_residue_adopts_but_unstamped_both_tier_refuses(db):
    from orchestrator.services.job_workspace_adoption import (
        LegacyK8sAdoptionOutcome,
        ensure_legacy_k8s_job_runtime_authority,
    )

    stamped = await db.create_job(
        description="stamped sandbox with stale VM residue",
        config_override={"workspace": {"backend": "sandbox"}},
    )
    vm_generation = str(uuid4())
    async with db.acquire() as conn:
        await _seed_previous_release_row(
            conn,
            "jobs",
            "UPDATE jobs SET context = COALESCE(context, '{}'::jsonb) "
            "|| $2::jsonb WHERE id = $1",
            stamped["id"],
            json.dumps(
                {
                    "workspace_container": {
                        "status": "ready",
                        "provisioner": "k8s",
                        "pod_ip": "10.42.1.17",
                        "port": 30022,
                        "host": "workspace-job.internal",
                    },
                    "vm": {
                        "status": "ready",
                        "ssh_host": "stale-vm.internal",
                        "ssh_port": 22,
                        "provision_generation": vm_generation,
                    },
                }
            ),
        )
    ambiguous = await _seed_exact_pre_0175_k8s_job(db)
    await db.merge_job_context(
        str(ambiguous["id"]),
        {
            "vm": {
                "status": "ready",
                "ssh_host": "stale-vm.internal",
                "ssh_port": 22,
                "provision_generation": str(uuid4()),
            }
        },
    )
    attestation = _k8s_job_attestation()
    provisioner = SimpleNamespace(
        attest_workspace_runtime=AsyncMock(return_value=attestation)
    )

    accepted = await ensure_legacy_k8s_job_runtime_authority(
        db, provisioner, await db.get_job(str(stamped["id"]))
    )
    refused = await ensure_legacy_k8s_job_runtime_authority(
        db, provisioner, await db.get_job(str(ambiguous["id"]))
    )

    assert accepted.outcome is LegacyK8sAdoptionOutcome.ADOPTED
    assert resolve_workspace_runtime(accepted.authority_job).stale_backend == "vm"
    assert refused.outcome is LegacyK8sAdoptionOutcome.NOT_NEEDED
    assert refused.reason == "authority_ambiguous"
    ambiguous_stored = await db.get_job(str(ambiguous["id"]))
    assert LEGACY_K8S_RUNTIME_ADOPTION_KEY not in repr(ambiguous_stored)


@pytest.mark.asyncio
async def test_bp07_strict_job_is_parked_then_concurrently_activated_once(db):
    from orchestrator.services.officer_preflight import ensure_officer_job_activated

    seed = await _seed_post(db)
    job = await admit_and_create_job(
        db,
        preparation=await _prepare(db, seed),
        job_kwargs=_job_kwargs("BP-07 strict activation"),
        ticket_note_id="bp07-ticket",
        ticket_ready_at=READY_GENERATION,
        strict_provisioning=True,
    )
    job = await db.get_job(str(job["id"]))
    assert job["status"] == "paused"
    assert _json(job["freeze_data"])["freeze_type"] == "officer_preflight"
    assert await db.claim_job_for_agent(str(job["id"]), str(uuid4())) is False

    provision_calls = 0

    async def provision(_job, *, category=None):
        nonlocal provision_calls
        provision_calls += 1
        await asyncio.sleep(0.05)

    outcomes = await asyncio.gather(
        ensure_officer_job_activated(db, job, provision=provision),
        ensure_officer_job_activated(db, job, provision=provision),
    )
    stored = await db.get_job(str(job["id"]))
    assert provision_calls == 1
    assert stored["status"] == "created"
    assert stored["freeze_data"] is None
    assert sum(outcome.attempted for outcome in outcomes) == 1
    assert await _job_count(db) == 1
    assert len(await _claim_rows(db, seed["project_id"], "bp07-ticket")) == 1


@pytest.mark.asyncio
async def test_bp07_repository_and_cloud_preflights_never_enter_breaker_history(db):
    from orchestrator.services.job_provisioning import JobProvisioningError
    from orchestrator.services.officer_preflight import ensure_officer_job_activated

    seed = await _seed_post(db, count=2)
    jobs = []
    for index, phase in enumerate(("repository", "cloud"), start=1):
        job = await admit_and_create_job(
            db,
            preparation=await _prepare(db, seed),
            job_kwargs=_job_kwargs(f"BP-07 {phase} failure"),
            ticket_note_id=f"bp07-{phase}",
            ticket_ready_at=READY_GENERATION + timedelta(seconds=index),
            strict_provisioning=True,
        )

        async def fail(_job, *, category=None, _phase=phase):
            raise JobProvisioningError(
                f"{_phase} unavailable", phase=_phase, retryable=True
            )

        outcome = await ensure_officer_job_activated(db, job, provision=fail)
        assert outcome.state == "retryable-failed"
        jobs.append(job)

    # This is the following-tick query, not an in-memory exception shortcut.
    history = await db.list_officer_distinct_terminal_outcomes(
        [seed["thread_id"]], slot="line", limit=2
    )
    assert history == []
    assert await _job_count(db) == 2
    assert len(await _claim_rows(db, seed["project_id"])) == 2


@pytest.mark.asyncio
async def test_bp07_real_pg_faults_before_and_after_activation_are_recoverable(db):
    from orchestrator.services.officer_preflight import ensure_officer_job_activated

    seed = await _seed_post(db, count=2)
    durable_resources: set[str] = set()

    async def provision(job, *, category=None):
        # Models the production create-or-get repository/cloud provisioning:
        # retrying the call cannot create a second durable resource.
        durable_resources.add(str(job["id"]))

    before = await admit_and_create_job(
        db,
        preparation=await _prepare(db, seed),
        job_kwargs=_job_kwargs("BP-07 crash before activation"),
        ticket_note_id="bp07-before",
        ticket_ready_at=READY_GENERATION,
        strict_provisioning=True,
    )

    def crash_before(step):
        if step == "after_provisioning_before_activation":
            raise RuntimeError("crash-before-activation")

    with pytest.raises(RuntimeError, match="crash-before-activation"):
        await ensure_officer_job_activated(
            db,
            before,
            provision=provision,
            lease_seconds=0,
            fault_injector=crash_before,
        )
    assert await db.claim_job_for_agent(str(before["id"]), str(uuid4())) is False
    recovered = await ensure_officer_job_activated(
        db, before, provision=provision, lease_seconds=0
    )
    assert recovered.activated is True

    after = await admit_and_create_job(
        db,
        preparation=await _prepare(db, seed),
        job_kwargs=_job_kwargs("BP-07 crash after activation"),
        ticket_note_id="bp07-after",
        ticket_ready_at=READY_GENERATION + timedelta(seconds=1),
        strict_provisioning=True,
    )

    def crash_after(step):
        if step == "after_activation":
            raise RuntimeError("crash-after-activation")

    with pytest.raises(RuntimeError, match="crash-after-activation"):
        await ensure_officer_job_activated(
            db, after, provision=provision, fault_injector=crash_after
        )
    calls_before_recovery = set(durable_resources)
    recovered_after = await ensure_officer_job_activated(db, after, provision=provision)
    assert recovered_after.activated is True
    assert recovered_after.attempted is False
    assert (
        durable_resources
        == calls_before_recovery
        == {
            str(before["id"]),
            str(after["id"]),
        }
    )
    assert await _job_count(db) == 2
    assert len(await _claim_rows(db, seed["project_id"])) == 2


@pytest.mark.asyncio
async def test_bp08_materialization_lease_and_projection_converge_once(db):
    seed = await _seed_post(db)
    kwargs = {
        "project_id": seed["project_id"],
        "note_id": "bp08-ticket",
        "content": "---\nid: bp08-ticket\ntype: feature\n---\n# Ticket\n",
        "content_hash": "bp08-hash",
    }
    first, second = await asyncio.gather(
        db.begin_knowledge_materialization(**kwargs),
        db.begin_knowledge_materialization(**kwargs),
    )
    assert sum(bool(row["attempt_claimed"]) for row in (first, second)) == 1
    owner = first if first["attempt_claimed"] else second
    intent_id = str(owner["id"])
    canonical = await db.finish_knowledge_materialization(
        intent_id,
        canonical=True,
        attempt_token=str(owner["attempt_token"]),
        path="knowledge/bp08-ticket.md",
    )
    assert canonical and canonical["canonical_state"] == "canonical"
    assert await db.unresolved_knowledge_note_ids(
        seed["project_id"], ["bp08-ticket"]
    ) == {"bp08-ticket"}
    projected = await db.finish_knowledge_projection(
        intent_id, project_id=seed["project_id"], synced=True
    )
    projected_again = await db.finish_knowledge_projection(
        intent_id, project_id=seed["project_id"], synced=True
    )
    assert projected["projected_at"] == projected_again["projected_at"]
    assert (
        await db.unresolved_knowledge_note_ids(seed["project_id"], ["bp08-ticket"])
        == set()
    )


@pytest.mark.asyncio
async def test_bp08_in_flight_lease_is_not_superseded_by_a_concurrent_writer(db):
    """kb_gardening E4 S2: two writers with different bytes for one note.
    The second `begin` must not steal the first's unexpired lease — that made
    the first writer report `intent-lease-lost` after its commit had landed.
    Only an expired (crashed) attempt is superseded by a newer write."""
    seed = await _seed_post(db)
    base = {
        "project_id": seed["project_id"],
        "note_id": "bp08-race",
    }
    first = await db.begin_knowledge_materialization(
        **base, content="v1", content_hash="race-v1"
    )
    assert first["attempt_claimed"] is True
    second = await db.begin_knowledge_materialization(
        **base, content="v2", content_hash="race-v2"
    )
    assert second["attempt_claimed"] is True

    # The first writer's lease survives, so its own finish still lands.
    finished = await db.finish_knowledge_materialization(
        str(first["id"]),
        canonical=True,
        attempt_token=str(first["attempt_token"]),
        path="knowledge/bp08-race.md",
    )
    assert finished is not None
    assert finished["canonical_state"] == "canonical"

    # An expired lease IS superseded by the next write (crash recovery).
    third = await db.begin_knowledge_materialization(
        **base, content="v3", content_hash="race-v3", lease_seconds=0
    )
    assert third["attempt_claimed"] is True
    fourth = await db.begin_knowledge_materialization(
        **base, content="v4", content_hash="race-v4"
    )
    assert fourth["attempt_claimed"] is True
    stolen = await db.finish_knowledge_materialization(
        str(third["id"]),
        canonical=True,
        attempt_token=str(third["attempt_token"]),
    )
    assert stolen is None  # third's attempt was superseded; its lease is gone


@pytest.mark.asyncio
async def test_bp08_a_prior_canonical_payload_can_become_current_again(db):
    seed = await _seed_post(db)

    async def canonicalize(content_hash: str):
        intent = await db.begin_knowledge_materialization(
            project_id=seed["project_id"],
            note_id="bp08-cycle",
            content=content_hash,
            content_hash=content_hash,
        )
        assert intent["attempt_claimed"] is True
        return await db.finish_knowledge_materialization(
            str(intent["id"]),
            canonical=True,
            attempt_token=str(intent["attempt_token"]),
        )

    first = await canonicalize("resolved")
    await canonicalize("active")
    repeated = await db.begin_knowledge_materialization(
        project_id=seed["project_id"],
        note_id="bp08-cycle",
        content="resolved",
        content_hash="resolved",
    )

    assert repeated["attempt_claimed"] is True
    assert repeated["id"] != first["id"]


@pytest.mark.asyncio
async def test_bp10_duplicate_floor_ticks_queue_one_durable_wake(db):
    from orchestrator.services.session_wake import notify_officer

    seed = await _seed_post(db)
    kwargs = {
        "expected_thread_id": seed["thread_id"],
        "pool": "line",
        "payload": {"pool": "line", "ready": 0, "floor": 1},
        "policy_debounce_seconds": 6 * 3600,
        "notifier": notify_officer,
    }
    first, second = await asyncio.gather(
        db.queue_officer_floor_wake(seed["project_id"], **kwargs),
        db.queue_officer_floor_wake(seed["project_id"], **kwargs),
    )
    assert sum(bool(row["queued"]) for row in (first, second)) == 1, (first, second)
    async with db.acquire() as conn:
        assert await conn.fetchval("SELECT COUNT(*) FROM session_wake_events") == 1
        assert (
            await conn.fetchval("SELECT COUNT(*) FROM officer_floor_wake_episodes") == 1
        )
    debounced = await db.queue_officer_floor_wake(
        seed["project_id"],
        **kwargs,
    )
    assert debounced["state"] == "policy_debounce"
    assert debounced["attempted"] is False


@pytest.mark.asyncio
async def test_bp10_outbox_rollback_retries_without_consuming_policy_debounce(db):
    from orchestrator.services.session_wake import notify_officer

    seed = await _seed_post(db)
    now = datetime.now(timezone.utc)

    def fail_after_insert(step):
        if step == "after_outbox_insert":
            raise RuntimeError("fault after insert")

    failed = await db.queue_officer_floor_wake(
        seed["project_id"],
        expected_thread_id=seed["thread_id"],
        pool="line",
        payload={"pool": "line"},
        policy_debounce_seconds=6 * 3600,
        retry_backoff_seconds=60,
        notifier=notify_officer,
        now=now,
        fault_injector=fail_after_insert,
    )
    assert failed["queued"] is False
    async with db.acquire() as conn:
        assert await conn.fetchval("SELECT COUNT(*) FROM session_wake_events") == 0
        assert await conn.fetchval(
            "SELECT last_queued_at IS NULL FROM officer_floor_wake_episodes"
        )

    retried = await db.queue_officer_floor_wake(
        seed["project_id"],
        expected_thread_id=seed["thread_id"],
        pool="line",
        payload={"pool": "line"},
        policy_debounce_seconds=6 * 3600,
        retry_backoff_seconds=60,
        notifier=notify_officer,
        now=now + timedelta(seconds=61),
    )
    assert retried["queued"] is True, retried
    async with db.acquire() as conn:
        assert await conn.fetchval("SELECT COUNT(*) FROM session_wake_events") == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["missing", "false", "raises"])
async def test_bp10_notifier_failures_do_not_queue_or_debounce(db, mode):
    seed = await _seed_post(db)

    async def notifier(*args, **kwargs):
        if mode == "raises":
            raise RuntimeError("notifier exploded")
        return False

    result = await db.queue_officer_floor_wake(
        seed["project_id"],
        expected_thread_id=seed["thread_id"],
        pool="line",
        payload={"pool": "line"},
        policy_debounce_seconds=6 * 3600,
        notifier=None if mode == "missing" else notifier,
    )
    assert result["attempted"] is True
    assert result["queued"] is False
    async with db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT attempt_count, last_queued_at, failure_class "
            "FROM officer_floor_wake_episodes"
        )
        assert row["attempt_count"] == 1
        assert row["last_queued_at"] is None
        assert (
            row["failure_class"]
            == {
                "missing": "missing_notifier",
                "false": "notifier_false",
                "raises": "notifier_exception",
            }[mode]
        )
        assert await conn.fetchval("SELECT COUNT(*) FROM session_wake_events") == 0


@pytest.mark.asyncio
async def test_bp10_delivery_updates_the_same_durable_episode(db):
    from orchestrator.services.session_wake import notify_officer

    seed = await _seed_post(db)
    queued = await db.queue_officer_floor_wake(
        seed["project_id"],
        expected_thread_id=seed["thread_id"],
        pool="line",
        payload={"pool": "line"},
        policy_debounce_seconds=6 * 3600,
        notifier=notify_officer,
    )
    claimed = await db.claim_pending_session_wake_events(limit=10)
    assert [row["id"] for row in claimed] == [queued["wake_event_id"]]
    assigned = await db.assign_session_wake_delivery_groups([queued["wake_event_id"]])
    assert len(assigned) == 1

    agent_id = uuid4()
    attach_token = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO agents "
            "(id,config_name,hostname,pod_ip,pod_uid,status,agent_mode) "
            "VALUES ($1,'centurion',$2,'127.0.0.1','bp10-pod','session',"
            "'persistent')",
            agent_id,
            f"persistent-{seed['thread_id'][:12]}",
        )
        async with conn.transaction():
            runtime_generation = await conn.fetchval(
                "SELECT runtime_generation FROM threads WHERE id=$1 FOR UPDATE",
                UUID(seed["thread_id"]),
            )
            process_generation = uuid4()
            # A pinned binding already live when 0200 landed; the subject here
            # is Officer Post transaction behaviour, not the bind authority.
            await conn.execute("SET LOCAL session_replication_role = 'replica'")
            await conn.execute(
                "UPDATE threads SET agent_id=$2, control_admission_agent_id=$2, "
                "runtime_attach_token=$3 WHERE id=$1",
                UUID(seed["thread_id"]),
                agent_id,
                attach_token,
            )
            await conn.execute(
                "UPDATE agents SET thread_id=$2 WHERE id=$1",
                agent_id,
                UUID(seed["thread_id"]),
            )
            delivery = await persist_input_delivery(
                conn,
                thread_id=seed["thread_id"],
                delivery_id=assigned[0]["delivery_id"],
                role="event",
                content="deliver this floor-wake episode",
                source="officer_wake",
                turn_number=1,
                agent_id=agent_id,
                pod_uid="bp10-pod",
                runtime_generation=process_generation,
                session_runtime_generation=runtime_generation,
                runtime_attach_token=attach_token,
            )
            assert await mark_input_delivery_queued(
                conn,
                delivery_id=assigned[0]["delivery_id"],
                agent_id=agent_id,
                pod_uid="bp10-pod",
                runtime_generation=process_generation,
                session_runtime_generation=runtime_generation,
                runtime_attach_token=attach_token,
                claim_generation=int(delivery["claim_generation"]),
            )
            assert await transition_input_delivery(
                conn,
                delivery_id=assigned[0]["delivery_id"],
                agent_id=agent_id,
                pod_uid="bp10-pod",
                runtime_generation=process_generation,
                session_runtime_generation=runtime_generation,
                runtime_attach_token=attach_token,
                claim_generation=int(delivery["claim_generation"]),
                transition="admitted",
                turn_number=1,
            )
    await db.finish_session_wake_events([queued["wake_event_id"]])

    outcomes = await db.list_officer_floor_wake_outcomes(seed["project_id"])
    assert outcomes[0]["state"] == "delivered"
    assert outcomes[0]["last_attempted_at"] is not None
    assert outcomes[0]["last_queued_at"] is not None
    assert outcomes[0]["delivered_at"] is not None


@pytest.mark.asyncio
async def test_bp10_durable_intent_survives_hold_and_decommission_supersedes_it(db):
    from orchestrator.services.session_wake import notify_officer

    seed = await _seed_post(db)
    queued = await db.queue_officer_floor_wake(
        seed["project_id"],
        expected_thread_id=seed["thread_id"],
        pool="line",
        payload={"pool": "line"},
        policy_debounce_seconds=6 * 3600,
        notifier=notify_officer,
    )
    assert queued["queued"] is True, queued
    await db.set_project_officer_hold(
        seed["project_id"],
        expected_thread_id=seed["thread_id"],
        hold={"kind": "maintenance", "since": "now", "note": "proof"},
    )
    assert await db.claim_pending_session_wake_events(limit=10) == []
    async with db.acquire() as conn:
        assert await conn.fetchval("SELECT state FROM session_wake_events") == "pending"

    result = await db.decommission_project_officer(
        seed["project_id"], seed["thread_id"], reason="BP-10 proof"
    )
    assert result["transitioned"] is True
    async with db.acquire() as conn:
        assert await conn.fetchval("SELECT COUNT(*) FROM session_wake_events") == 0
        assert (
            await conn.fetchval("SELECT state FROM officer_floor_wake_episodes")
            == "superseded"
        )


@pytest.mark.asyncio
async def test_bp10_decommission_racing_queue_leaves_no_orphaned_wake(db):
    from orchestrator.services.session_wake import notify_officer

    seed = await _seed_post(db)
    queued, decommissioned = await asyncio.gather(
        db.queue_officer_floor_wake(
            seed["project_id"],
            expected_thread_id=seed["thread_id"],
            pool="line",
            payload={"pool": "line"},
            policy_debounce_seconds=6 * 3600,
            notifier=notify_officer,
        ),
        db.decommission_project_officer(
            seed["project_id"], seed["thread_id"], reason="BP-10 race proof"
        ),
    )
    assert decommissioned["transitioned"] is True
    assert queued["state"] in {"queued", "stale_incarnation", "unavailable"}
    async with db.acquire() as conn:
        assert await conn.fetchval("SELECT COUNT(*) FROM session_wake_events") == 0
        active = await conn.fetchval(
            "SELECT COUNT(*) FROM officer_floor_wake_episodes "
            "WHERE state IN ('retryable', 'queued') AND resolved_at IS NULL"
        )
        assert active == 0


@pytest.mark.asyncio
async def test_bp10_hold_racing_queue_preserves_or_refuses_one_durable_intent(db):
    from orchestrator.services.session_wake import notify_officer

    seed = await _seed_post(db)
    queued, held = await asyncio.gather(
        db.queue_officer_floor_wake(
            seed["project_id"],
            expected_thread_id=seed["thread_id"],
            pool="line",
            payload={"pool": "line"},
            policy_debounce_seconds=6 * 3600,
            notifier=notify_officer,
        ),
        db.set_project_officer_hold(
            seed["project_id"],
            expected_thread_id=seed["thread_id"],
            hold={"kind": "maintenance", "since": "now", "note": "race"},
        ),
    )
    held_metadata = _json(held["thread"]["metadata"])
    assert held_metadata["config_override"]["officer"]["hold"]["kind"] == (
        "maintenance"
    )
    assert queued["state"] in {"queued", "held"}
    async with db.acquire() as conn:
        event_count = await conn.fetchval("SELECT COUNT(*) FROM session_wake_events")
    assert event_count == (1 if queued["state"] == "queued" else 0)
    # If queuing won the post lock, hold must keep the already-durable intent
    # pending rather than deliver or discard it.
    assert await db.claim_pending_session_wake_events(limit=10) == []


@pytest.mark.asyncio
async def test_bp06_distinct_breaker_and_stale_claim_queries_are_semantically_complete(
    db,
):
    """Former LIMIT 10/50 windows stay correct at 10k ledger rows."""

    seed = await _seed_post(db)
    project_id = UUID(seed["project_id"])
    thread_id = UUID(seed["thread_id"])
    base = datetime.now(timezone.utc)

    async with db.acquire() as conn:
        # Supported high population: unrelated lineage rows make the production
        # lineage index's selectivity and plan visible, not just its existence.
        await conn.execute(
            """
            INSERT INTO officer_ticket_claims (
                project_id, ticket_note_id, claimed_at, source,
                officer_thread_id, officer_slot, work_category, job_id,
                job_deleted_at, job_status_at_delete
            )
            SELECT $1, 'noise-' || n,
                   $2::timestamptz - n * interval '1 second',
                   'legacy_unversioned', gen_random_uuid(), 'noise',
                   'researcher', gen_random_uuid(), $2, 'completed'
              FROM generate_series(1, 10000) AS n
            """,
            project_id,
            base,
        )

        rows = []
        latest_repeated_job = None
        # Eleven terminal outcomes for one ticket occupy the former breaker
        # window. The older second ticket is the required distinct outcome.
        for index in range(11):
            job_id = uuid4()
            if index == 0:
                latest_repeated_job = job_id
            rows.append(
                (
                    project_id,
                    "ticket-repeated",
                    base - timedelta(seconds=index + 1),
                    thread_id,
                    job_id,
                    base,
                    "failed",
                )
            )
        older_distinct_job = uuid4()
        rows.append(
            (
                project_id,
                "ticket-distinct",
                base - timedelta(seconds=20),
                thread_id,
                older_distinct_job,
                base,
                "failed",
            )
        )
        # Mixed non-terminal rows are newer but must be rejected by the SQL
        # predicate before breaker limiting.
        for index in range(12):
            rows.append(
                (
                    project_id,
                    f"ticket-open-{index}",
                    base + timedelta(seconds=index + 1),
                    thread_id,
                    uuid4(),
                    None,
                    None,
                )
            )
        await conn.executemany(
            """
            INSERT INTO officer_ticket_claims (
                project_id, ticket_note_id, claimed_at, source,
                officer_thread_id, officer_slot, work_category, job_id,
                job_deleted_at, job_status_at_delete
            ) VALUES ($1, $2, $3, 'legacy_unversioned', $4, 'line',
                      'executor', $5, $6, $7)
            """,
            rows,
        )

        stale_rows = [
            (
                project_id,
                f"stale-{index:02d}",
                base - timedelta(hours=100 + index),
                thread_id,
                uuid4(),
            )
            for index in range(60)
        ]
        await conn.executemany(
            """
            INSERT INTO officer_ticket_claims (
                project_id, ticket_note_id, claimed_at, source,
                officer_thread_id, officer_slot, work_category, job_id
            ) VALUES ($1, $2, $3, 'legacy_unversioned', $4, 'stale',
                      'researcher', $5)
            """,
            stale_rows,
        )
        fresh_non_executors = [
            (
                project_id,
                f"fresh-researcher-{index}",
                base + timedelta(minutes=10, seconds=index),
                thread_id,
                uuid4(),
            )
            for index in range(12)
        ]
        await conn.executemany(
            """
            INSERT INTO officer_ticket_claims (
                project_id, ticket_note_id, claimed_at, source,
                officer_thread_id, officer_slot, work_category, job_id
            ) VALUES ($1, $2, $3, 'legacy_unversioned', $4, 'line',
                      'researcher', $5)
            """,
            fresh_non_executors,
        )
        await conn.execute(
            """
            INSERT INTO officer_ticket_claims (
                project_id, ticket_note_id, claimed_at, source,
                officer_thread_id, officer_slot, work_category, job_id,
                job_deleted_at, job_status_at_delete
            )
            SELECT $1, 'spend-' || n,
                   $2::timestamptz - n * interval '1 second',
                   'legacy_unversioned', $3, 'spend', 'researcher',
                   gen_random_uuid(), $2, 'completed'
              FROM generate_series(1, 101) AS n
            """,
            project_id,
            base,
            thread_id,
        )
        await conn.execute("ANALYZE officer_ticket_claims")

        plan_doc = await conn.fetchval(
            """
            EXPLAIN (ANALYZE, FORMAT JSON)
            SELECT claim.id
              FROM officer_ticket_claims claim
              LEFT JOIN jobs live ON live.id = claim.job_id
             WHERE claim.officer_thread_id = ANY($1::uuid[])
               AND claim.officer_slot = 'line'
               AND COALESCE(live.status, claim.job_status_at_delete)
                   IN ('completed', 'failed', 'cancelled')
             ORDER BY claim.claimed_at DESC, claim.id DESC
             LIMIT 2
            """,
            [thread_id],
        )

    started = time.perf_counter()
    outcomes = await db.list_officer_distinct_terminal_outcomes(
        [seed["thread_id"]], slot="line", limit=2
    )
    stale = await db.list_stale_officer_claims(
        [seed["thread_id"]], stale_before=base - timedelta(hours=4)
    )
    oldest = await db.get_oldest_open_officer_claim([seed["thread_id"]])
    live_executor = await db.list_officer_slot_claims(
        [seed["thread_id"]], work_category="executor", limit=1
    )
    terminal_executor = await db.list_officer_slot_claims(
        [seed["thread_id"]],
        work_category="executor",
        include_terminal=True,
        terminal_only=True,
        limit=1,
    )
    spend_ids = await db.list_officer_slot_claims(
        [seed["thread_id"]], slot="spend", include_terminal=True, limit=None
    )
    elapsed = time.perf_counter() - started

    assert [row["ticket_note_id"] for row in outcomes] == [
        "ticket-repeated",
        "ticket-distinct",
    ]
    assert outcomes[0]["id"] == latest_repeated_job
    assert outcomes[1]["id"] == older_distinct_job
    assert len(stale) == 60
    assert stale[0]["ticket_note_id"] == "stale-59"
    assert oldest and oldest["ticket_note_id"] == "stale-59"
    assert live_executor[0]["ticket_note_id"] == "ticket-open-11"
    assert terminal_executor[0]["ticket_note_id"] == "ticket-repeated"
    assert len(spend_ids) == 101
    assert elapsed < 5.0

    if isinstance(plan_doc, str):
        plan_doc = json.loads(plan_doc)
    plan = plan_doc[0]["Plan"]
    assert "idx_officer_ticket_claims_lineage_slot_claimed" in _plan_index_names(plan)
    print(
        "BP-06 app query latency "
        f"{elapsed * 1000:.2f}ms; plan {plan['Actual Total Time']:.2f}ms"
    )


async def _remove_claim_migration_boundary(db: PostgresDB) -> None:
    """Return the test database to the instant before migration 0162."""

    async with db.acquire() as conn:
        await conn.execute(
            """
            DROP TRIGGER IF EXISTS trg_officer_ticket_delivery_writer ON jobs;
            DROP TRIGGER IF EXISTS officer_ticket_claim_job_integrity ON jobs;
            DROP TRIGGER IF EXISTS officer_ticket_claim_job_delete_audit ON jobs;
            DROP FUNCTION IF EXISTS enforce_officer_ticket_delivery_writer();
            DROP FUNCTION IF EXISTS enforce_officer_ticket_claim_job_integrity();
            DROP FUNCTION IF EXISTS audit_officer_ticket_claim_job_delete();
            DROP TABLE IF EXISTS officer_ticket_claims;
            """
        )


def _historical_ticket_context(
    seed: dict[str, str],
    preparation,
    *,
    ticket: str,
    generation: datetime | str,
) -> dict:
    """Trusted shape emitted by the deployed pre-0162 automatic admission."""

    return {
        "ticket_note_id": ticket,
        "officer_slot": "line",
        "work_category": "executor",
        "officer_admission": {
            "project_id": seed["project_id"],
            "thread_id": seed["thread_id"],
            "incarnation": preparation.incarnation,
            "slot": "line",
            "category": "executor",
            "config_fingerprint": preparation.config_fingerprint,
            "lineage_size": 1,
            "ticket_ready_at": (
                generation.isoformat()
                if isinstance(generation, datetime)
                else generation
            ),
        },
    }


def _legacy_ticket_context(
    seed: dict[str, str],
    preparation,
    *,
    ticket: str,
    partial_admission: bool = False,
) -> dict:
    """Field-observed manual history: no trusted ready generation."""

    context = {
        "ticket_note_id": ticket,
        "officer_slot": "line",
        "work_category": "executor",
    }
    if partial_admission:
        context["officer_admission"] = {
            "project_id": seed["project_id"],
            "thread_id": seed["thread_id"],
            "incarnation": preparation.incarnation,
            "slot": "line",
            "category": "executor",
            "config_fingerprint": preparation.config_fingerprint,
            "lineage_size": 1,
        }
    return context


async def _insert_old_writer_ticket_job(
    conn,
    seed: dict[str, str],
    preparation,
    *,
    ticket: str,
    generation: datetime | str,
    status: str = "created",
) -> dict:
    """Issue the SQL shape an old replica used, bypassing new app stripping."""

    row = await conn.fetchrow(
        """
        INSERT INTO jobs (
            description, config_name, context, status, project_id,
            created_by_thread_id
        ) VALUES ($1, 'worker_base', $2::jsonb, $3, $4, $5)
        RETURNING id, created_at, status
        """,
        f"historical {ticket}",
        json.dumps(
            _historical_ticket_context(
                seed,
                preparation,
                ticket=ticket,
                generation=generation,
            )
        ),
        status,
        UUID(seed["project_id"]),
        UUID(seed["thread_id"]),
    )
    return dict(row)


async def _insert_legacy_ticket_job(
    conn,
    seed: dict[str, str],
    preparation,
    *,
    ticket: str,
    partial_admission: bool = False,
    status: str = "completed",
) -> dict:
    """Insert the stamp-less/partial manual shapes observed on main dev."""

    row = await conn.fetchrow(
        """
        INSERT INTO jobs (
            description, config_name, context, status, project_id,
            created_by_thread_id
        ) VALUES ($1, 'worker_base', $2::jsonb, $3, $4, $5)
        RETURNING id, created_at, status
        """,
        f"legacy {ticket}",
        json.dumps(
            _legacy_ticket_context(
                seed,
                preparation,
                ticket=ticket,
                partial_admission=partial_admission,
            )
        ),
        status,
        UUID(seed["project_id"]),
        UUID(seed["thread_id"]),
    )
    return dict(row)


async def _seed_officer_job(
    db: PostgresDB,
    seed: dict[str, str],
    *,
    thread_id: str,
    status: str,
    label: str,
) -> dict:
    job = await db.create_job(
        description=label,
        project_id=seed["project_id"],
        config_name="worker_base",
        created_by_thread_id=thread_id,
        context={"officer_slot": "line"},
    )
    if status != "created":
        async with db.acquire() as conn:
            await conn.execute(
                "UPDATE jobs SET status=$2 WHERE id=$1", job["id"], status
            )
    return job


async def _race(*calls):
    ready = asyncio.Event()

    async def _run(call):
        await ready.wait()
        try:
            return await call()
        except Exception as exc:  # outcome inspected by each proof
            return exc

    tasks = [asyncio.create_task(_run(call)) for call in calls]
    ready.set()
    return await asyncio.gather(*tasks)


@pytest.mark.asyncio
@pytest.mark.parametrize("hold_during_ticket_read", [False, True])
async def test_manual_preparation_reads_ticket_outside_final_admission_transaction(
    db, hold_during_ticket_read
):
    seed = await _seed_post(db)
    ticket_entered, release_ticket = asyncio.Event(), asyncio.Event()

    async def fetch_ticket(project_id, note_id):
        assert (project_id, note_id) == (seed["project_id"], "prepared-ticket")
        ticket_entered.set()
        await release_ticket.wait()
        return {
            "project_id": project_id,
            "status": "active",
            "note_type": "feature",
            "tags": ["ready", "category:executor"],
            "ready_at": READY_GENERATION,
        }

    config = JobAdmissionConfig(
        context={"officer_slot": "line"},
        project_id=seed["project_id"],
        config_name="worker_base",
        config_override=None,
        expert_id=None,
        request_config_override=None,
        requested_workspace_backend=None,
        root_creation=True,
    )
    pending = asyncio.create_task(
        prepare_job_admission_officer(
            command=JobCreate(
                description="prepared dispatch",
                thread_id=seed["thread_id"],
                ticket="prepared-ticket",
            ),
            config=config,
            dependencies=JobAdmissionOfficerDependencies(
                store=db,
                prepare_officer=partial(prepare_officer_admission, db),
                fetch_ticket=fetch_ticket,
            ),
        )
    )
    try:
        await asyncio.wait_for(ticket_entered.wait(), 5)
        assert await _job_count(db) == 0
        assert await _claim_rows(db, seed["project_id"]) == []
        if hold_during_ticket_read:
            # This writer takes the durable post lock. It must complete while
            # vector lookup is suspended, then fence the stale preparation.
            await asyncio.wait_for(
                db.set_project_officer_hold(
                    seed["project_id"],
                    expected_thread_id=seed["thread_id"],
                    hold={"kind": "maintenance", "since": "now", "note": "proof"},
                ),
                5,
            )
    finally:
        release_ticket.set()
        prepared = await asyncio.wait_for(pending, 5)

    async def insert():
        return await admit_and_create_job(
            db,
            preparation=prepared.preparation,
            job_kwargs={
                **_job_kwargs("prepared dispatch"),
                "context": prepared.context,
                "config_override": prepared.config_override,
            },
            ticket_note_id="prepared-ticket",
            ticket_ready_at=prepared.ticket_ready_at,
            ticket_claim_source="manual",
        )

    if hold_during_ticket_read:
        with pytest.raises(OfficerAdmissionConflict) as exc:
            await insert()
        assert exc.value.code == "officer_held"
        assert await _job_count(db) == 0
        assert await _claim_rows(db, seed["project_id"]) == []
    else:
        job = await insert()
        claims = await _claim_rows(db, seed["project_id"], "prepared-ticket")
        assert await _job_count(db) == 1 and len(claims) == 1
        assert claims[0]["job_id"] == job["id"]
        assert claims[0]["ready_generation_at"] == READY_GENERATION
        assert claims[0]["source"] == "manual"


@pytest.mark.asyncio
async def test_two_manual_creates_race_for_one_remaining_slot(db):
    seed = await _seed_post(db, count=1)
    preparation = await _prepare(db, seed)

    outcomes = await _race(
        lambda: admit_and_create_job(
            db,
            preparation=preparation,
            job_kwargs=_job_kwargs("manual A"),
            ticket_note_id="ticket-a",
            ticket_ready_at=READY_GENERATION,
        ),
        lambda: admit_and_create_job(
            db,
            preparation=preparation,
            job_kwargs=_job_kwargs("manual B"),
            ticket_note_id="ticket-b",
            ticket_ready_at=READY_GENERATION,
        ),
    )

    assert sum(isinstance(outcome, dict) for outcome in outcomes) == 1
    assert sum(isinstance(outcome, SlotAdmissionError) for outcome in outcomes) == 1
    assert await _job_count(db) == 1
    assert len(await _claim_rows(db, seed["project_id"])) == 1


@pytest.mark.asyncio
async def test_same_ticket_manual_manual_race_is_one_job_and_normal_conflict(db):
    seed = await _seed_post(db, count=2)
    preparation = await _prepare(db, seed)

    outcomes = await _race(
        *(
            lambda label=label: admit_and_create_job(
                db,
                preparation=preparation,
                job_kwargs=_job_kwargs(label),
                ticket_note_id="same-ticket",
                ticket_ready_at=READY_GENERATION,
            )
            for label in ("manual A", "manual B")
        )
    )

    conflicts = [o for o in outcomes if isinstance(o, OfficerAdmissionConflict)]
    assert sum(isinstance(outcome, dict) for outcome in outcomes) == 1
    assert len(conflicts) == 1
    assert conflicts[0].code == "ticket_claimed"
    assert await _job_count(db) == 1
    assert len(await _claim_rows(db, seed["project_id"], "same-ticket")) == 1


@pytest.mark.asyncio
async def test_same_ticket_manual_tick_race_is_one_job_and_normal_conflict(db):
    # auto_pull=true exists only in this isolated fixture; production remains
    # release-blocked with the control disabled.
    seed = await _seed_post(db, count=2, auto_pull=True)
    manual = await _prepare(db, seed)
    tick = await _prepare(db, seed, auto_pull=True)
    ready_at = datetime.now(timezone.utc) - timedelta(seconds=1)

    outcomes = await _race(
        lambda: admit_and_create_job(
            db,
            preparation=manual,
            job_kwargs=_job_kwargs("manual"),
            ticket_note_id="manual-tick",
            ticket_ready_at=ready_at,
        ),
        lambda: admit_and_create_job(
            db,
            preparation=tick,
            job_kwargs=_job_kwargs("tick"),
            ticket_note_id="manual-tick",
            ticket_ready_at=ready_at,
        ),
    )

    conflicts = [o for o in outcomes if isinstance(o, OfficerAdmissionConflict)]
    assert sum(isinstance(outcome, dict) for outcome in outcomes) == 1
    assert len(conflicts) == 1
    assert conflicts[0].code == "ticket_claimed"
    assert await _job_count(db) == 1
    claims = await _claim_rows(db, seed["project_id"], "manual-tick")
    assert len(claims) == 1
    assert claims[0]["source"] in {"manual", "tick"}


@pytest.mark.asyncio
async def test_claim_and_job_insert_roll_back_together(db, monkeypatch):
    seed = await _seed_post(db)
    preparation = await _prepare(db, seed)
    observed = {}

    async def fail_after_claim(**kwargs):
        conn = kwargs["conn"]
        observed["claims_inside"] = await conn.fetchval(
            "SELECT COUNT(*) FROM officer_ticket_claims"
        )
        observed["jobs_inside"] = await conn.fetchval("SELECT COUNT(*) FROM jobs")
        raise RuntimeError("fault between claim and job insert")

    monkeypatch.setattr(db, "create_job", fail_after_claim)
    with pytest.raises(RuntimeError, match="fault between claim and job insert"):
        await admit_and_create_job(
            db,
            preparation=preparation,
            job_kwargs=_job_kwargs("rollback proof"),
            ticket_note_id="rollback-ticket",
            ticket_ready_at=READY_GENERATION,
        )

    assert observed == {"claims_inside": 1, "jobs_inside": 0}
    assert await _job_count(db) == 0
    assert await _claim_rows(db, seed["project_id"]) == []


@pytest.mark.asyncio
async def test_terminal_delete_retains_claim_and_equal_or_older_stays_ineligible(db):
    seed = await _seed_post(db, count=2)
    preparation = await _prepare(db, seed)
    job = await admit_and_create_job(
        db,
        preparation=preparation,
        job_kwargs=_job_kwargs("terminal deletion"),
        ticket_note_id="delete-terminal",
        ticket_ready_at=READY_GENERATION,
    )
    async with db.acquire() as conn:
        await conn.execute("UPDATE jobs SET status='completed' WHERE id=$1", job["id"])

    actor = str(uuid4())
    assert await db.delete_job(
        str(job["id"]),
        deletion_actor_user_id=actor,
        deletion_reason="acceptance_terminal_delete",
    )
    claim = (await _claim_rows(db, seed["project_id"], "delete-terminal"))[0]
    assert claim["job_id"] == job["id"]
    assert claim["job_deleted_at"] is not None
    assert claim["job_status_at_delete"] == "completed"
    assert str(claim["deletion_actor_user_id"]) == actor
    assert claim["deletion_reason"] == "acceptance_terminal_delete"

    for generation in (
        READY_GENERATION - timedelta(seconds=1),
        READY_GENERATION,
    ):
        with pytest.raises(OfficerAdmissionConflict) as exc:
            await admit_and_create_job(
                db,
                preparation=await _prepare(db, seed),
                job_kwargs=_job_kwargs("must remain claimed"),
                ticket_note_id="delete-terminal",
                ticket_ready_at=generation,
            )
        assert exc.value.code == "ticket_claimed"
    assert await _job_count(db) == 0


@pytest.mark.asyncio
async def test_nonterminal_delete_retains_claim_and_blocks_newer_generation(db):
    seed = await _seed_post(db, count=2)
    job = await admit_and_create_job(
        db,
        preparation=await _prepare(db, seed),
        job_kwargs=_job_kwargs("nonterminal deletion"),
        ticket_note_id="delete-nonterminal",
        ticket_ready_at=READY_GENERATION,
    )
    assert await db.delete_job(
        str(job["id"]), deletion_reason="acceptance_nonterminal_delete"
    )

    claim = (await _claim_rows(db, seed["project_id"], "delete-nonterminal"))[0]
    assert claim["job_status_at_delete"] == "created"
    states = await db.ticket_claim_states(seed["project_id"], ["delete-nonterminal"])
    assert states["delete-nonterminal"]["has_non_terminal"] is True
    with pytest.raises(OfficerAdmissionConflict) as exc:
        await admit_and_create_job(
            db,
            preparation=await _prepare(db, seed),
            job_kwargs=_job_kwargs("new generation remains blocked"),
            ticket_note_id="delete-nonterminal",
            ticket_ready_at=READY_GENERATION + timedelta(minutes=1),
        )
    assert exc.value.code == "ticket_claimed"


@pytest.mark.asyncio
async def test_old_writer_terminal_delete_is_audited_and_does_not_wedge_re_ready(db):
    seed = await _seed_post(db, count=2)
    job = await admit_and_create_job(
        db,
        preparation=await _prepare(db, seed),
        job_kwargs=_job_kwargs("retention deletion"),
        ticket_note_id="retention-ticket",
        ticket_ready_at=READY_GENERATION,
    )
    async with db.acquire() as conn:
        await conn.execute("UPDATE jobs SET status='completed' WHERE id=$1", job["id"])
        # Simulate a legacy retention DELETE that knows nothing about BP-05.
        # Migration 0162's BEFORE DELETE trigger must make terminality durable.
        await conn.execute("DELETE FROM jobs WHERE id=$1", job["id"])

    claim = (await _claim_rows(db, seed["project_id"], "retention-ticket"))[0]
    assert claim["job_deleted_at"] is not None
    assert claim["job_status_at_delete"] == "completed"
    assert claim["deletion_reason"] == "database_delete_compatibility_trigger"
    states = await db.ticket_claim_states(seed["project_id"], ["retention-ticket"])
    assert states["retention-ticket"]["has_non_terminal"] is False
    replacement = await admit_and_create_job(
        db,
        preparation=await _prepare(db, seed),
        job_kwargs=_job_kwargs("newer trusted generation"),
        ticket_note_id="retention-ticket",
        ticket_ready_at=READY_GENERATION + timedelta(minutes=1),
    )
    assert replacement["id"] != job["id"]


@pytest.mark.asyncio
async def test_old_writer_nonterminal_delete_is_audited_and_stays_blocked(db):
    seed = await _seed_post(db, count=2)
    job = await admit_and_create_job(
        db,
        preparation=await _prepare(db, seed),
        job_kwargs=_job_kwargs("old nonterminal delete"),
        ticket_note_id="old-delete-live",
        ticket_ready_at=READY_GENERATION,
    )
    async with db.acquire() as conn:
        await conn.execute("DELETE FROM jobs WHERE id=$1", job["id"])

    claim = (await _claim_rows(db, seed["project_id"], "old-delete-live"))[0]
    assert claim["job_status_at_delete"] == "created"
    assert claim["deletion_reason"] == "database_delete_compatibility_trigger"
    states = await db.ticket_claim_states(seed["project_id"], ["old-delete-live"])
    assert states["old-delete-live"]["has_non_terminal"] is True
    with pytest.raises(OfficerAdmissionConflict):
        await admit_and_create_job(
            db,
            preparation=await _prepare(db, seed),
            job_kwargs=_job_kwargs("must remain blocked"),
            ticket_note_id="old-delete-live",
            ticket_ready_at=READY_GENERATION + timedelta(minutes=1),
        )


@pytest.mark.asyncio
async def test_newer_generation_after_terminal_work_claims_exactly_once(db):
    seed = await _seed_post(db, count=2)
    first = await admit_and_create_job(
        db,
        preparation=await _prepare(db, seed),
        job_kwargs=_job_kwargs("first generation"),
        ticket_note_id="re-ready-ticket",
        ticket_ready_at=READY_GENERATION,
    )
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET status='completed' WHERE id=$1", first["id"]
        )

    newer = READY_GENERATION + timedelta(minutes=1)
    preparation = await _prepare(db, seed)
    outcomes = await _race(
        *(
            lambda label=label: admit_and_create_job(
                db,
                preparation=preparation,
                job_kwargs=_job_kwargs(label),
                ticket_note_id="re-ready-ticket",
                ticket_ready_at=newer,
            )
            for label in ("new generation A", "new generation B")
        )
    )
    assert sum(isinstance(outcome, dict) for outcome in outcomes) == 1
    assert (
        sum(isinstance(outcome, OfficerAdmissionConflict) for outcome in outcomes) == 1
    )
    assert len(await _claim_rows(db, seed["project_id"], "re-ready-ticket")) == 2
    assert await _job_count(db) == 2


@pytest.mark.asyncio
async def test_newer_generation_cannot_duplicate_still_nonterminal_work(db):
    seed = await _seed_post(db, count=2)
    await admit_and_create_job(
        db,
        preparation=await _prepare(db, seed),
        job_kwargs=_job_kwargs("live first generation"),
        ticket_note_id="live-re-ready-ticket",
        ticket_ready_at=READY_GENERATION,
    )
    with pytest.raises(OfficerAdmissionConflict) as exc:
        await admit_and_create_job(
            db,
            preparation=await _prepare(db, seed),
            job_kwargs=_job_kwargs("forbidden newer generation"),
            ticket_note_id="live-re-ready-ticket",
            ticket_ready_at=READY_GENERATION + timedelta(minutes=1),
        )
    assert exc.value.code == "ticket_claimed"
    assert len(await _claim_rows(db, seed["project_id"], "live-re-ready-ticket")) == 1


@pytest.mark.asyncio
async def test_claims_are_project_scoped_and_survive_recommission(db):
    first_seed = await _seed_post(db, count=2)
    second_seed = await _seed_post(db, count=1)
    for seed in (first_seed, second_seed):
        job = await admit_and_create_job(
            db,
            preparation=await _prepare(db, seed),
            job_kwargs=_job_kwargs("project-scoped generation"),
            ticket_note_id="shared-slug",
            ticket_ready_at=READY_GENERATION,
        )
        async with db.acquire() as conn:
            await conn.execute(
                "UPDATE jobs SET status='completed' WHERE id=$1", job["id"]
            )

    await db.decommission_project_officer(
        first_seed["project_id"],
        first_seed["thread_id"],
        reason="claim continuity",
        force=True,
    )
    successor = await _seed_successor(db, first_seed, count=2)
    assert await db.register_project_officer_thread(first_seed["project_id"], successor)
    successor_seed = {**first_seed, "thread_id": successor}
    await admit_and_create_job(
        db,
        preparation=await _prepare(db, successor_seed),
        job_kwargs=_job_kwargs("successor generation"),
        ticket_note_id="shared-slug",
        ticket_ready_at=READY_GENERATION + timedelta(minutes=1),
    )

    first_claims = await _claim_rows(db, first_seed["project_id"], "shared-slug")
    second_claims = await _claim_rows(db, second_seed["project_id"], "shared-slug")
    assert [str(row["officer_thread_id"]) for row in first_claims] == [
        first_seed["thread_id"],
        successor,
    ]
    assert len(second_claims) == 1


@pytest.mark.asyncio
async def test_migration_backfill_is_idempotent_and_preserves_generations(db):
    seed = await _seed_post(db, count=3)
    preparation = await _prepare(db, seed)
    older = READY_GENERATION - timedelta(hours=2)
    newer = READY_GENERATION - timedelta(hours=1)
    await _remove_claim_migration_boundary(db)
    async with db.acquire() as conn:
        first = await _insert_old_writer_ticket_job(
            conn,
            seed,
            preparation,
            ticket="backfill-ticket",
            generation=older,
            status="completed",
        )
        second = await _insert_old_writer_ticket_job(
            conn,
            seed,
            preparation,
            ticket="backfill-ticket",
            generation=newer,
            status="failed",
        )
    migration_sql = CLAIM_MIGRATION_FILE.read_text()
    async with db.acquire() as conn:
        await conn.execute(migration_sql)
        start = migration_sql.index("DO $backfill$")
        end = migration_sql.index("$backfill$;", start) + len("$backfill$;")
        backfill_sql = migration_sql[start:end]
        await conn.execute(backfill_sql)

    claims = await _claim_rows(db, seed["project_id"])
    assert len(claims) == 2
    assert {row["source"] for row in claims} == {"backfill"}
    ticket_generations = {
        row["ready_generation_at"]
        for row in claims
        if row["ticket_note_id"] == "backfill-ticket"
    }
    assert ticket_generations == {older, newer}
    assert {row["job_id"] for row in claims} == {first["id"], second["id"]}


@pytest.mark.asyncio
async def test_backfilled_job_accepts_unrelated_context_merge_but_not_claim_removal(db):
    """Backfill preserves the source-less pre-0162 admission-stamp shape."""

    seed = await _seed_post(db)
    preparation = await _prepare(db, seed)
    await _remove_claim_migration_boundary(db)
    async with db.acquire() as conn:
        historical = await _insert_old_writer_ticket_job(
            conn,
            seed,
            preparation,
            ticket="backfill-merge-ticket",
            generation=READY_GENERATION,
            status="completed",
        )
        await conn.execute(CLAIM_MIGRATION_FILE.read_text())

    assert await db.merge_job_context(
        str(historical["id"]), {"ordinary_runtime_update": "preserved"}
    )
    async with db.acquire() as conn:
        context = _json(
            await conn.fetchval(
                "SELECT context FROM jobs WHERE id=$1", historical["id"]
            )
        )
        source = await conn.fetchval(
            "SELECT source FROM officer_ticket_claims WHERE job_id=$1",
            historical["id"],
        )
    assert context["ordinary_runtime_update"] == "preserved"
    assert "ticket_claim_source" not in context["officer_admission"]
    assert source == "backfill"

    with pytest.raises(asyncpg.CheckViolationError) as exc:
        await db.delete_job_context_keys(str(historical["id"]), ["ticket_note_id"])
    assert exc.value.constraint_name == "officer_ticket_claim_job_integrity"
    assert "cannot remove" in str(exc.value)


@pytest.mark.asyncio
async def test_migration_backfills_field_observed_legacy_shapes_as_rearm_barriers(db):
    """Six stamp-less rows plus one partial stamp reproduce main-dev history."""

    seed = await _seed_post(db, count=10)
    preparation = await _prepare(db, seed)
    await _remove_claim_migration_boundary(db)
    async with db.acquire() as conn:
        stamp_less = [
            await _insert_legacy_ticket_job(
                conn,
                seed,
                preparation,
                ticket=f"legacy-stampless-{index}",
            )
            for index in range(6)
        ]
        partial = await _insert_legacy_ticket_job(
            conn,
            seed,
            preparation,
            ticket="legacy-partial",
            partial_admission=True,
        )
        before_cutover = await conn.fetchval("SELECT clock_timestamp()")
        await conn.execute(CLAIM_MIGRATION_FILE.read_text())
        after_cutover = await conn.fetchval("SELECT clock_timestamp()")

    claims = await _claim_rows(db, seed["project_id"])
    assert len(claims) == 7
    assert {row["job_id"] for row in claims} == {
        *(row["id"] for row in stamp_less),
        partial["id"],
    }
    assert {row["source"] for row in claims} == {"legacy_unversioned"}
    assert {row["ready_generation_at"] for row in claims} == {None}
    barriers = {row["claimed_at"] for row in claims}
    assert len(barriers) == 1
    barrier = barriers.pop()
    assert before_cutover <= barrier <= after_cutover
    assert {row["officer_incarnation"] for row in claims} == {None}
    assert {row["admission_config_fingerprint"] for row in claims} == {None}
    assert {row["admission_lineage_size"] for row in claims} == {None}

    states = await db.ticket_claim_states(
        seed["project_id"], ["legacy-stampless-0", "legacy-partial"]
    )
    assert states["legacy-stampless-0"]["ready_generation_at"] is None
    assert states["legacy-stampless-0"]["legacy_rearm_after"] == barrier
    assert states["legacy-stampless-0"]["has_non_terminal"] is False

    with pytest.raises(OfficerAdmissionConflict) as exc:
        await admit_and_create_job(
            db,
            preparation=await _prepare(db, seed),
            job_kwargs=_job_kwargs("equal-to-cutover must stay consumed"),
            ticket_note_id="legacy-stampless-0",
            ticket_ready_at=barrier,
        )
    assert exc.value.code == "ticket_claimed"
    assert "after the durable-claim cutover" in exc.value.detail

    rearmed_at = barrier + timedelta(microseconds=1)
    rearmed = await admit_and_create_job(
        db,
        preparation=await _prepare(db, seed),
        job_kwargs=_job_kwargs("explicitly re-readied after cutover"),
        ticket_note_id="legacy-stampless-0",
        ticket_ready_at=rearmed_at,
    )
    ticket_claims = await _claim_rows(db, seed["project_id"], "legacy-stampless-0")
    assert {row["source"] for row in ticket_claims} == {
        "legacy_unversioned",
        "manual",
    }
    assert {row["ready_generation_at"] for row in ticket_claims} == {
        None,
        rearmed_at,
    }
    assert rearmed["id"] in {row["job_id"] for row in ticket_claims}

    assert await db.merge_job_context(
        str(partial["id"]), {"ordinary_runtime_update": "preserved"}
    )
    with pytest.raises(asyncpg.CheckViolationError) as removal:
        await db.delete_job_context_keys(str(partial["id"]), ["ticket_note_id"])
    assert removal.value.constraint_name == "officer_ticket_claim_job_integrity"


@pytest.mark.asyncio
async def test_deleted_nonterminal_legacy_barrier_still_blocks_newer_generation(db):
    seed = await _seed_post(db, count=2)
    preparation = await _prepare(db, seed)
    await _remove_claim_migration_boundary(db)
    async with db.acquire() as conn:
        historical = await _insert_legacy_ticket_job(
            conn,
            seed,
            preparation,
            ticket="legacy-deleted-nonterminal",
            status="created",
        )
        await conn.execute(CLAIM_MIGRATION_FILE.read_text())
        barrier = await conn.fetchval(
            "SELECT claimed_at FROM officer_ticket_claims WHERE job_id=$1",
            historical["id"],
        )
        await conn.execute("DELETE FROM jobs WHERE id=$1", historical["id"])
        audit = await conn.fetchrow(
            """
            SELECT job_deleted_at, job_status_at_delete
              FROM officer_ticket_claims
             WHERE job_id=$1
            """,
            historical["id"],
        )

    assert audit["job_deleted_at"] is not None
    assert audit["job_status_at_delete"] == "created"
    with pytest.raises(OfficerAdmissionConflict) as exc:
        await admit_and_create_job(
            db,
            preparation=await _prepare(db, seed),
            job_kwargs=_job_kwargs("must remain blocked"),
            ticket_note_id="legacy-deleted-nonterminal",
            ticket_ready_at=barrier + timedelta(seconds=1),
        )
    assert exc.value.code == "ticket_claimed"
    assert "non-terminal" in exc.value.detail


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field",
    ["project_id", "thread_id", "ticket_ready_at", "incarnation", "lineage_size"],
)
async def test_migration_quarantines_incomplete_historical_admission(db, field):
    seed = await _seed_post(db)
    preparation = await _prepare(db, seed)
    await _remove_claim_migration_boundary(db)
    async with db.acquire() as conn:
        historical = await _insert_old_writer_ticket_job(
            conn,
            seed,
            preparation,
            ticket=f"missing-{field}",
            generation=READY_GENERATION,
            status="completed",
        )
        await conn.execute(
            """
            UPDATE jobs
               SET context = jsonb_set(
                   context,
                   '{officer_admission}',
                   (context->'officer_admission') - $2::text
               )
             WHERE id = $1
            """,
            historical["id"],
            field,
        )
        await conn.execute(CLAIM_MIGRATION_FILE.read_text())
        claim = await conn.fetchrow(
            "SELECT * FROM officer_ticket_claims WHERE job_id=$1",
            historical["id"],
        )

    assert claim["source"] == "legacy_unversioned"
    assert claim["ready_generation_at"] is None
    assert claim["officer_incarnation"] is None
    assert claim["admission_config_fingerprint"] is None
    assert claim["admission_lineage_size"] is None


@pytest.mark.asyncio
async def test_old_writer_ticket_insert_is_rejected_after_migration(db):
    seed = await _seed_post(db)
    preparation = await _prepare(db, seed)
    context = _historical_ticket_context(
        seed,
        preparation,
        ticket="post-migration-old-writer",
        generation=READY_GENERATION,
    )
    attempted_id = uuid4()
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.CheckViolationError) as exc:
            await conn.execute(
                """
                INSERT INTO jobs (
                    id, description, config_name, context, project_id,
                    created_by_thread_id
                ) VALUES ($1, 'old rolling replica', 'worker_base', $2::jsonb,
                          $3, $4)
                """,
                attempted_id,
                json.dumps(context),
                UUID(seed["project_id"]),
                UUID(seed["thread_id"]),
            )
        assert exc.value.constraint_name == "officer_ticket_claim_job_integrity"
        assert "rolling upgrade" in str(exc.value)
        assert not await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM jobs WHERE id=$1)", attempted_id
        )
        assert not await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM officer_ticket_claims WHERE job_id=$1)",
            attempted_id,
        )


@pytest.mark.asyncio
async def test_writer_committed_before_migration_lock_is_backfilled(db):
    seed = await _seed_post(db)
    preparation = await _prepare(db, seed)
    await _remove_claim_migration_boundary(db)
    writer = await db._pool.acquire()
    migrator = await db._pool.acquire()
    transaction = writer.transaction()
    await transaction.start()
    committed = False
    try:
        historical = await _insert_old_writer_ticket_job(
            writer,
            seed,
            preparation,
            ticket="crossing-writer",
            generation=READY_GENERATION,
            status="completed",
        )
        migration_task = asyncio.create_task(
            migrator.execute(CLAIM_MIGRATION_FILE.read_text())
        )
        await asyncio.sleep(0.05)
        assert not migration_task.done(), "migration must wait for the old writer"
        await transaction.commit()
        committed = True
        await asyncio.wait_for(migration_task, timeout=5)
    finally:
        if not committed:
            await transaction.rollback()
        await db._pool.release(writer)
        await db._pool.release(migrator)

    claim = (await _claim_rows(db, seed["project_id"], "crossing-writer"))[0]
    assert claim["job_id"] == historical["id"]
    assert claim["source"] == "backfill"


@pytest.mark.asyncio
async def test_migration_rejects_same_generation_historical_collision(db):
    seed = await _seed_post(db, count=3)
    preparation = await _prepare(db, seed)
    await _remove_claim_migration_boundary(db)
    async with db.acquire() as conn:
        first = await _insert_old_writer_ticket_job(
            conn,
            seed,
            preparation,
            ticket="collision-ticket",
            generation=READY_GENERATION,
            status="completed",
        )
        second = await _insert_old_writer_ticket_job(
            conn,
            seed,
            preparation,
            ticket="collision-ticket",
            generation=READY_GENERATION,
            status="created",
        )

    migration_sql = CLAIM_MIGRATION_FILE.read_text()
    try:
        async with db.acquire() as conn:
            with pytest.raises(asyncpg.UniqueViolationError) as exc:
                await conn.execute(migration_sql)
        message = str(exc.value)
        assert "BP-05 backfill collision" in message
        assert str(first["id"]) in message
        assert str(second["id"]) in message
        assert "completed" in message
        assert "created" in message
    finally:
        async with db.acquire() as conn:
            await conn.execute("DELETE FROM jobs WHERE id=$1", second["id"])
            await conn.execute(migration_sql)


@pytest.mark.asyncio
async def test_migration_quarantines_unverifiable_historical_claim_context(db):
    seed = await _seed_post(db)
    preparation = await _prepare(db, seed)
    await _remove_claim_migration_boundary(db)
    async with db.acquire() as conn:
        forged = await _insert_old_writer_ticket_job(
            conn,
            seed,
            preparation,
            ticket="unverifiable-ticket",
            generation="model-authored-garbage",
            status="completed",
        )

    async with db.acquire() as conn:
        await conn.execute(CLAIM_MIGRATION_FILE.read_text())
        claim = await conn.fetchrow(
            "SELECT * FROM officer_ticket_claims WHERE job_id=$1", forged["id"]
        )
    assert claim["source"] == "legacy_unversioned"
    assert claim["ready_generation_at"] is None
    assert claim["claimed_at"] > forged["created_at"]


@pytest.mark.asyncio
async def test_final_admission_overwrites_model_claim_provenance(db):
    seed = await _seed_post(db)
    forged_generation = "2099-01-01T00:00:00+00:00"
    job = await admit_and_create_job(
        db,
        preparation=await _prepare(db, seed),
        job_kwargs={
            **_job_kwargs("forged context"),
            "context": {
                "ticket_note_id": "other-project-ticket",
                "officer_slot": "forged-slot",
                "officer_admission": {
                    "project_id": str(uuid4()),
                    "thread_id": str(uuid4()),
                    "ticket_ready_at": forged_generation,
                },
            },
        },
        ticket_note_id="authoritative-ticket",
        ticket_ready_at=READY_GENERATION,
    )

    async with db.acquire() as conn:
        context = await conn.fetchval("SELECT context FROM jobs WHERE id=$1", job["id"])
    context = _json(context)
    assert context["ticket_note_id"] == "authoritative-ticket"
    assert context["officer_slot"] == "line"
    assert context["officer_admission"]["project_id"] == seed["project_id"]
    assert context["officer_admission"]["thread_id"] == seed["thread_id"]
    assert context["officer_admission"]["ticket_ready_at"] == (
        READY_GENERATION.isoformat()
    )
    assert forged_generation not in json.dumps(context)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field",
    ["project_id", "thread_id", "ticket_ready_at", "incarnation", "lineage_size"],
)
async def test_claim_integrity_rejects_missing_admission_identity(db, field):
    seed = await _seed_post(db)
    job = await admit_and_create_job(
        db,
        preparation=await _prepare(db, seed),
        job_kwargs=_job_kwargs(f"protected {field}"),
        ticket_note_id=f"protected-{field}",
        ticket_ready_at=READY_GENERATION,
    )

    async with db.acquire() as conn:
        with pytest.raises(asyncpg.CheckViolationError) as exc:
            await conn.execute(
                """
                UPDATE jobs
                   SET context = jsonb_set(
                       context,
                       '{officer_admission}',
                       (context->'officer_admission') - $2::text
                   )
                 WHERE id = $1
                """,
                job["id"],
                field,
            )
        assert exc.value.constraint_name == "officer_ticket_claim_job_integrity"
        context = _json(
            await conn.fetchval("SELECT context FROM jobs WHERE id=$1", job["id"])
        )
    assert field in context["officer_admission"]


@pytest.mark.asyncio
async def test_database_funnel_strips_raw_claim_context_from_ordinary_jobs(db):
    seed = await _seed_post(db)
    job = await db.create_job(
        description="ordinary raw context bypass",
        project_id=seed["project_id"],
        created_by_thread_id=seed["thread_id"],
        context={
            "evidence_manifest": {
                "version": 1,
                "job_id": "forged",
                "source_repository": "victim-private-repo",
            },
            "ticket_note_id": "forged-ticket",
            "officer_admission": {"ticket_ready_at": "2099-01-01T00:00:00Z"},
            "ticket_ready_at": "2099-01-01T00:00:00Z",
            "ticket_claim_source": "manual",
            "officer_slot": "line",
            "ordinary": "preserved",
        },
    )
    async with db.acquire() as conn:
        context = _json(
            await conn.fetchval("SELECT context FROM jobs WHERE id=$1", job["id"])
        )
    assert context["ordinary"] == "preserved"
    assert context["officer_slot"] == "line"
    assert "evidence_manifest" not in context
    assert not set(context) & {
        "ticket_note_id",
        "officer_admission",
        "ticket_ready_at",
        "ticket_claim_source",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("internal", [False, True], ids=["public", "internal"])
async def test_http_creation_paths_cannot_persist_raw_claim_context(db, internal):
    import orchestrator.security.access as access_module
    from orchestrator.schemas.job_create import JobCreate
    from tests._b09_control_seams import create_job

    user_id = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO users (id, display_name, is_approved)
            VALUES ($1, 'BP-05 raw context proof', true)
            """,
            user_id,
        )

    headers = {}
    if internal:
        headers = {
            "X-Internal-Key": "bp05-test-key",
            "X-MCP-User-Id": str(user_id),
        }
    request = SimpleNamespace(headers=headers, query_params={})
    body = JobCreate(
        description=f"{('internal' if internal else 'public')} raw bypass",
        context={
            "ordinary": "preserved",
            "evidence_manifest": {
                "version": 1,
                "job_id": "forged",
                "source_repository": "victim-private-repo",
            },
            "ticket_note_id": "forged-ticket",
            "officer_admission": {"ticket_ready_at": "2099-01-01T00:00:00Z"},
            "ready_generation_at": "2099-01-01T00:00:00Z",
            "ticket_claim_source": "forged",
            "officer_slot": "line",
        },
    )
    principal = {"id": user_id, "is_admin": False}
    patches = (
        patch("orchestrator.main.app.state.resources.postgres_db", db),
        patch(
            "orchestrator.security.auth.require_approved_user",
            AsyncMock(return_value=principal),
        ),
        patch(
            "orchestrator.application.access.enforce_readiness_gate",
            AsyncMock(return_value=None),
        ),
        patch(
            "orchestrator.application.jobs.require_job_project_access",
            AsyncMock(return_value=None),
        ),
        patch(
            "orchestrator.services.deployment_gates.is_experts_db_enabled",
            MagicMock(return_value=False),
        ),
        patch(
            "orchestrator.services.job_datasource_selection.inherit_parent_datasource_ids",
            AsyncMock(return_value=[]),
        ),
        patch(
            "orchestrator.services.grant_enforcement.enforce_job_create_grants",
            AsyncMock(return_value=None),
        ),
        patch("orchestrator.services.job_provisioning.provision_job_repo", AsyncMock()),
        patch(
            "orchestrator.services.subjob_completion.spawn_scholar_subjob",
            AsyncMock(return_value=None),
        ),
        patch("orchestrator.services.job_dispatcher.trigger_dispatch", MagicMock()),
    )
    with ExitStack() as stack:
        stack.enter_context(
            patch.object(access_module, "_INTERNAL_KEY", "bp05-test-key")
        )
        for active_patch in patches:
            stack.enter_context(active_patch)
        result = await create_job(request, body)

    async with db.acquire() as conn:
        context = _json(
            await conn.fetchval("SELECT context FROM jobs WHERE id=$1", result["id"])
        )
    assert context["ordinary"] == "preserved"
    assert context["officer_slot"] == "line"
    assert "evidence_manifest" not in context
    assert not set(context) & {
        "ticket_note_id",
        "officer_admission",
        "ready_generation_at",
        "ticket_claim_source",
    }


@pytest.mark.asyncio
async def test_completion_merge_can_record_server_owned_evidence_manifest(db):
    seed = await _seed_post(db)
    job = await db.create_job(
        description="completion evidence authority",
        project_id=seed["project_id"],
    )
    manifest = {
        "version": 1,
        "job_id": str(job["id"]),
        "source_repository": "job-authoritative",
        "source_ref": "main",
        "source_revision": "a" * 40,
        "entries": [],
    }
    assert await db.merge_job_context(str(job["id"]), {"evidence_manifest": manifest})
    stored = await db.get_job(str(job["id"]))
    assert _json(stored["context"])["evidence_manifest"] == manifest


@pytest.mark.asyncio
@pytest.mark.parametrize("claimed", [False, True], ids=["ordinary", "claimed"])
async def test_delete_response_reports_only_an_actual_durable_claim(db, claimed):
    from tests._b09_control_seams import delete_job

    seed = await _seed_post(db)
    if claimed:
        created = await admit_and_create_job(
            db,
            preparation=await _prepare(db, seed),
            job_kwargs=_job_kwargs("claimed deletion response"),
            ticket_note_id="deletion-response-ticket",
            ticket_ready_at=READY_GENERATION,
        )
    else:
        created = await db.create_job(
            description="ordinary deletion response",
            project_id=seed["project_id"],
        )
    job = await db.get_job(str(created["id"]))
    admin = {"id": str(uuid4()), "is_admin": True}
    gitea = MagicMock(is_initialized=False)
    snapshots = MagicMock(is_available=False)
    vector = MagicMock()
    vector_conn = MagicMock()
    vector_conn.execute = AsyncMock()
    vector.acquire.return_value.__aenter__ = AsyncMock(return_value=vector_conn)
    vector.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
    post_commit_lookup = AsyncMock(
        side_effect=AssertionError("deletion must not perform a post-commit read")
    )

    with (
        patch("orchestrator.main.app.state.resources.postgres_db", db),
        patch.object(db, "job_has_durable_ticket_claim", post_commit_lookup),
        patch(
            "orchestrator.security.access.require_job_access",
            AsyncMock(return_value=(admin, job)),
        ),
        patch(
            "orchestrator.services.thread_retirement.ThreadRetirementOperations.archive_and_cleanup_workspace",
            AsyncMock(return_value=[]),
        ),
        patch("orchestrator.main.app.state.resources.gitea_client", gitea),
        patch("orchestrator.services.snapshot_service.snapshot_service", snapshots),
        patch("orchestrator.main.app.state.resources.vector_db", vector),
    ):
        result = await delete_job(
            SimpleNamespace(headers={}, query_params={}), str(created["id"])
        )

    assert result["ticket_claim_retained"] is claimed
    assert result["ticket_rearmed"] is False
    assert ("remains durable" in result["message"]) is claimed
    post_commit_lookup.assert_not_awaited()


@pytest.mark.asyncio
async def test_lineage_claim_query_has_a_supporting_index_plan(db):
    seed = await _seed_post(db)
    job = await admit_and_create_job(
        db,
        preparation=await _prepare(db, seed),
        job_kwargs=_job_kwargs("indexed lineage claim"),
        ticket_note_id="indexed-lineage-ticket",
        ticket_ready_at=READY_GENERATION,
    )
    async with db.acquire() as conn:
        await conn.execute("SET enable_seqscan = off")
        plan_rows = await conn.fetch(
            """
            EXPLAIN (COSTS OFF)
            SELECT claim.job_id
              FROM officer_ticket_claims claim
              LEFT JOIN jobs live ON live.id = claim.job_id
             WHERE claim.officer_thread_id = ANY($1::uuid[])
               AND claim.officer_slot = $2
             ORDER BY claim.claimed_at DESC
             LIMIT 20
            """,
            [UUID(seed["thread_id"])],
            "line",
        )
    plan = "\n".join(str(row[0]) for row in plan_rows)
    assert "idx_officer_ticket_claims_lineage_slot_claimed" in plan
    assert str(job["id"])


@pytest.mark.asyncio
async def test_backlog_query_and_admission_reject_enabled_thread_not_holding_post(db):
    seed = await _seed_post(db)
    orphan_id = await _seed_successor(db, seed)

    commissioned = await db.list_commissioned_officer_posts_for_backlog()
    assert [str(row["id"]) for row in commissioned] == [seed["thread_id"]]
    with pytest.raises(OfficerAdmissionConflict) as exc:
        await prepare_officer_admission(
            db,
            project_id=seed["project_id"],
            thread_id=orphan_id,
            requested_slot="line",
            require_auto_pull=False,
        )
    assert exc.value.code == "stale_incarnation"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    [
        "hold",
        "disable",
        "auto_pull_disable",
        "roster",
        "decommission",
        "recommission",
    ],
)
async def test_final_boundary_rejects_lifecycle_or_roster_change(db, mutation):
    # Tick-shaped optimistic read: every preparation saw auto_pull=true.
    # The lifecycle/config writer commits before the final transaction; the
    # shared Post lock must make the stale tick lose with no job or BP-05
    # claim, regardless of which authority changed.
    seed = await _seed_post(db, auto_pull=True)
    preparation = await _prepare(db, seed, auto_pull=True)

    if mutation == "hold":
        await db.set_project_officer_hold(
            seed["project_id"],
            expected_thread_id=seed["thread_id"],
            hold={"kind": "maintenance", "since": "now", "note": "proof"},
        )
    elif mutation == "disable":
        await db.update_project_officer_post(
            seed["project_id"],
            config_updates={"officer": {"enabled": False}},
        )
    elif mutation == "auto_pull_disable":
        await db.update_project_officer_post(
            seed["project_id"],
            config_updates={"officer": {"auto_pull": False}},
        )
    elif mutation == "roster":
        await db.update_project_officer_post(
            seed["project_id"],
            config_updates={"officer": {"slots": _roster(count=0)}},
        )
    else:
        await db.decommission_project_officer(
            seed["project_id"], seed["thread_id"], reason="proof"
        )
        if mutation == "recommission":
            successor = await _seed_successor(db, seed)
            assert await db.register_project_officer_thread(
                seed["project_id"], successor
            )

    with pytest.raises((OfficerAdmissionConflict, SlotAdmissionError)):
        await admit_and_create_job(
            db,
            preparation=preparation,
            job_kwargs=_job_kwargs(mutation),
            ticket_note_id=f"ticket-{mutation}",
            ticket_ready_at=READY_GENERATION,
            ticket_claim_source="tick",
        )
    assert await _job_count(db) == 0
    assert await _claim_rows(db, seed["project_id"]) == []


def _officer_post_request():
    """A request whose application resolves the Post's dependencies exactly as
    ``main`` does.

    Both factories read ``postgres_db`` and the release fence per call, so the
    rebinds each case makes above still steer the operation — and driving the
    route declarations keeps the owner/member gates in the picture.
    """
    request = MagicMock()
    request.app.state.officer_post_lifecycle_dependencies_factory = functools.partial(
        workflows_composition.officer_post_lifecycle_dependencies,
        orch_main.app.state.resources,
    )
    request.app.state.officer_post_view_dependencies_factory = functools.partial(
        workflows_composition.officer_post_view_dependencies,
        orch_main.app.state.resources,
    )
    return request


@pytest.mark.asyncio
async def test_bp01_controls_round_trip_and_survive_recommission(db, monkeypatch):
    seed = await _seed_post(db)
    monkeypatch.setattr(orch_main.app.state.resources, "postgres_db", db)
    monkeypatch.setattr(
        orch_main.app.state.resources.settings,
        "officer_auto_pull_release_enabled",
        True,
    )
    monkeypatch.setattr(
        officers_router,
        "require_project_owner",
        AsyncMock(
            return_value=({"id": str(uuid4()), "is_admin": True}, {"name": "proof"})
        ),
    )
    monkeypatch.setattr(
        officers_router,
        "require_project_member",
        AsyncMock(
            return_value=({"id": str(uuid4()), "is_admin": True}, {"name": "proof"})
        ),
    )
    monkeypatch.setattr(
        officer_notices, "inject_officer_notice", AsyncMock(return_value=False)
    )
    request = _officer_post_request()
    roster = {
        "research": {
            "count": 1,
            "category": "researcher",
            "model": "MiniMax-M3",
            "backend": "sandbox",
            "spend_ceiling_daily": 7.25,
        }
    }

    updated = await officers_router.patch_project_officer(
        request,
        seed["project_id"],
        {
            "auto_pull": True,
            "worker_spend_ceiling_daily": 19.5,
            "slots": roster,
        },
    )
    summary = await officers_router.get_project_officer_summary(
        request, seed["project_id"]
    )
    post = await db.get_project_officer(seed["project_id"])
    thread = await db.get_thread(seed["thread_id"])
    expected = {
        "auto_pull": True,
        "worker_spend_ceiling_daily": 19.5,
        "slots": roster,
    }

    assert updated["config_override"]["officer"] == expected
    assert post["config_override"]["officer"] == expected
    assert _json(thread["metadata"])["config_override"]["officer"] == {
        **expected,
        "enabled": True,
    }
    assert summary["backlog"]["auto_pull"] is True
    assert summary["backlog"]["worker_spend_ceiling_daily"] == 19.5
    assert summary["backlog"]["auto_pull_control"] == {
        "enable_available": True,
        "source": "deployment_policy",
        "reason": None,
    }
    assert summary["kit"]["research"] == {**roster["research"], "in_flight": 0}

    prepared = await prepare_officer_admission(
        db,
        project_id=seed["project_id"],
        thread_id=seed["thread_id"],
        requested_slot="research",
        require_auto_pull=True,
        expected_category="researcher",
    )
    await officers_router.patch_project_officer(
        request, seed["project_id"], {"auto_pull": False}
    )
    with pytest.raises(OfficerAdmissionConflict) as exc:
        await admit_and_create_job(
            db,
            preparation=prepared,
            job_kwargs=_job_kwargs("BP-01 disable fence"),
            ticket_note_id="bp01-disabled",
            ticket_ready_at=READY_GENERATION,
        )
    assert exc.value.code == "auto_pull_disabled"
    assert await _job_count(db) == 0

    await officers_router.patch_project_officer(
        request, seed["project_id"], {"auto_pull": True}
    )
    await db.decommission_project_officer(
        seed["project_id"], seed["thread_id"], reason="BP-01 recommission"
    )
    durable = (await db.get_project_officer(seed["project_id"]))["config_override"]
    assert durable["officer"] == expected
    successor_id = uuid4()
    successor_config = json.loads(json.dumps(durable))
    successor_config["officer"]["enabled"] = True
    async with db.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO threads (id, project_id, status, metadata)
            VALUES ($1, $2, 'active', $3::jsonb)
            """,
            successor_id,
            UUID(seed["project_id"]),
            json.dumps({"config_override": successor_config}),
        )
    assert await db.register_project_officer_thread(
        seed["project_id"],
        str(successor_id),
        expected_post_config_override=durable,
    )
    successor = await db.get_thread(str(successor_id))
    assert _json(successor["metadata"])["config_override"]["officer"] == {
        **expected,
        "enabled": True,
    }


@pytest.mark.asyncio
async def test_bp11_whole_roster_replaces_post_thread_admission_and_recommission(
    db, monkeypatch
):
    seed = await _seed_post(db)
    original = {
        "line": {
            "count": 2,
            "category": "executor",
            "model": "old-model",
            "backend": "sandbox",
        },
        "scout": {
            "count": 1,
            "category": "researcher",
            "model": "scout-model",
            "backend": "virtual",
            "spend_ceiling_daily": 7.5,
        },
    }
    await db.update_project_officer_post(
        seed["project_id"], config_updates={"officer": {"slots": original}}
    )

    survivor = {"scout": original["scout"]}
    monkeypatch.setattr(orch_main.app.state.resources, "postgres_db", db)
    monkeypatch.setattr(
        officers_router,
        "require_project_owner",
        AsyncMock(
            return_value=({"id": str(uuid4()), "is_admin": True}, {"name": "proof"})
        ),
    )
    monkeypatch.setattr(
        officers_router,
        "require_project_member",
        AsyncMock(
            return_value=({"id": str(uuid4()), "is_admin": True}, {"name": "proof"})
        ),
    )
    monkeypatch.setattr(
        officer_notices, "inject_officer_notice", AsyncMock(return_value=False)
    )
    request = _officer_post_request()
    api_update = await officers_router.patch_project_officer(
        request, seed["project_id"], {"slots": survivor}
    )
    api_summary = await officers_router.get_project_officer_summary(
        request, seed["project_id"]
    )
    post = await db.get_project_officer(seed["project_id"])
    thread = await db.get_thread(seed["thread_id"])
    thread_slots = _json(thread["metadata"])["config_override"]["officer"]["slots"]

    assert api_update["config_override"]["officer"]["slots"] == survivor
    assert api_summary["kit"] == {
        "scout": {**survivor["scout"], "in_flight": 0},
    }
    assert post["config_override"]["officer"]["slots"] == survivor
    assert thread_slots == survivor
    assert "line" not in thread_slots

    with pytest.raises(SlotAdmissionError, match="Unknown slot 'line'"):
        await prepare_officer_admission(
            db,
            project_id=seed["project_id"],
            thread_id=seed["thread_id"],
            requested_slot="line",
            require_auto_pull=False,
        )
    assert (
        await prepare_officer_admission(
            db,
            project_id=seed["project_id"],
            thread_id=seed["thread_id"],
            requested_slot="scout",
            require_auto_pull=False,
        )
    ).slot_name == "scout"

    remaining = {
        "builders": {
            **survivor["scout"],
            "count": 0,
        }
    }
    await db.update_project_officer_post(
        seed["project_id"], config_updates={"officer": {"slots": remaining}}
    )
    renamed_post = await db.get_project_officer(seed["project_id"])
    renamed_thread = await db.get_thread(seed["thread_id"])
    assert renamed_post["config_override"]["officer"]["slots"] == remaining
    assert (
        _json(renamed_thread["metadata"])["config_override"]["officer"]["slots"]
        == remaining
    )
    assert set(renamed_post["config_override"]["officer"]["slots"]) == {"builders"}
    with pytest.raises(SlotAdmissionError, match="Unknown slot 'scout'"):
        await prepare_officer_admission(
            db,
            project_id=seed["project_id"],
            thread_id=seed["thread_id"],
            requested_slot="scout",
            require_auto_pull=False,
        )
    with pytest.raises(SlotAdmissionError, match="0/0"):
        await prepare_officer_admission(
            db,
            project_id=seed["project_id"],
            thread_id=seed["thread_id"],
            requested_slot="builders",
            require_auto_pull=False,
        )

    reopened = {"builders": {**remaining["builders"], "count": 1}}
    await db.update_project_officer_post(
        seed["project_id"], config_updates={"officer": {"slots": reopened}}
    )
    preparation = await prepare_officer_admission(
        db,
        project_id=seed["project_id"],
        thread_id=seed["thread_id"],
        requested_slot="builders",
        require_auto_pull=False,
    )
    assert preparation.slot_name == "builders"
    in_flight = await admit_and_create_job(
        db,
        preparation=preparation,
        job_kwargs=_job_kwargs("BP-11 visible drain"),
        ticket_note_id="bp11-drain",
        ticket_ready_at=READY_GENERATION,
    )
    in_flight_row = await db.get_job(str(in_flight["id"]))
    assert str(in_flight_row["created_by_thread_id"]) == seed["thread_id"]
    assert in_flight_row["status"] not in {"completed", "failed", "cancelled"}
    assert _json(in_flight_row["context"])["officer_slot"] == "builders"
    drained = {"builders": {**remaining["builders"], "count": 0}}
    await db.update_project_officer_post(
        seed["project_id"], config_updates={"officer": {"slots": drained}}
    )
    assert (await db.get_job(str(in_flight["id"]))) is not None
    async with db.acquire() as conn:
        assert await count_in_flight_by_slot(conn, [UUID(seed["thread_id"])]) == {
            "builders": 1
        }
    # The preparation fast fence rejects the zero cap before the final
    # post-locked capacity count; the durable job and count above prove the
    # in-flight work was neither hidden nor cancelled by the shrink.
    with pytest.raises(SlotAdmissionError, match="0/0"):
        await prepare_officer_admission(
            db,
            project_id=seed["project_id"],
            thread_id=seed["thread_id"],
            requested_slot="builders",
            require_auto_pull=False,
        )
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET status='completed' WHERE id=$1", in_flight["id"]
        )
    await db.update_project_officer_post(
        seed["project_id"], config_updates={"officer": {"slots": reopened}}
    )
    assert (
        await prepare_officer_admission(
            db,
            project_id=seed["project_id"],
            thread_id=seed["thread_id"],
            requested_slot="builders",
            require_auto_pull=False,
        )
    ).slot_name == "builders"

    await db.decommission_project_officer(
        seed["project_id"], seed["thread_id"], reason="BP-11 recommission"
    )
    durable = (await db.get_project_officer(seed["project_id"]))["config_override"]
    successor_id = uuid4()
    successor_config = json.loads(json.dumps(durable))
    successor_config.setdefault("officer", {})["enabled"] = True
    async with db.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO threads (id, project_id, status, metadata)
            VALUES ($1, $2, 'active', $3::jsonb)
            """,
            successor_id,
            UUID(seed["project_id"]),
            json.dumps({"config_override": successor_config}),
        )
    assert await db.register_project_officer_thread(
        seed["project_id"],
        str(successor_id),
        expected_post_config_override=durable,
    )
    recommissioned = await db.get_thread(str(successor_id))
    assert (
        _json(recommissioned["metadata"])["config_override"]["officer"]["slots"]
        == reopened
    )

    flattened = await db.update_project_officer_post(
        seed["project_id"], config_updates={"officer": {"slots": None}}
    )
    assert flattened["post"]["config_override"]["officer"]["slots"] is None
    flattened_thread = await db.get_thread(str(successor_id))
    assert (
        _json(flattened_thread["metadata"])["config_override"]["officer"]["slots"]
        is None
    )
    flat_preparation = await prepare_officer_admission(
        db,
        project_id=seed["project_id"],
        thread_id=str(successor_id),
        requested_slot="removed-name-is-ignored-on-flat-cap",
        require_auto_pull=False,
    )
    assert flat_preparation.slot_name is None


@pytest.mark.asyncio
async def test_bp11_concurrent_whole_roster_writes_never_form_a_union(db):
    seed = await _seed_post(db)
    roster_a = {"alpha": {"count": 1, "model": "a", "backend": "sandbox"}}
    roster_b = {"beta": {"count": 2, "model": "b", "backend": "virtual"}}

    async with db.acquire() as blocker:
        transaction = blocker.transaction()
        await transaction.start()
        await blocker.fetchrow(
            "SELECT project_id FROM project_officers WHERE project_id=$1 FOR UPDATE",
            UUID(seed["project_id"]),
        )
        write_a = asyncio.create_task(
            db.update_project_officer_post(
                seed["project_id"],
                config_updates={"officer": {"slots": roster_a}},
            )
        )
        write_b = asyncio.create_task(
            db.update_project_officer_post(
                seed["project_id"],
                config_updates={"officer": {"slots": roster_b}},
            )
        )
        await asyncio.sleep(0.1)
        assert not write_a.done() and not write_b.done()
        await transaction.commit()

    results = await asyncio.gather(write_a, write_b)
    observed = [
        result["post"]["config_override"]["officer"]["slots"] for result in results
    ]
    assert roster_a in observed and roster_b in observed
    post = await db.get_project_officer(seed["project_id"])
    thread = await db.get_thread(seed["thread_id"])
    final_slots = post["config_override"]["officer"]["slots"]
    assert final_slots in (roster_a, roster_b)
    assert (
        _json(thread["metadata"])["config_override"]["officer"]["slots"] == final_slots
    )


@pytest.mark.asyncio
async def test_bp01_concurrent_enable_disable_writes_never_form_a_hybrid(db):
    seed = await _seed_post(db)
    enabled = {
        "auto_pull": True,
        "worker_spend_ceiling_daily": 25.0,
        "slots": {
            "research": {
                "count": 2,
                "category": "researcher",
                "backend": "sandbox",
                "spend_ceiling_daily": 8.0,
            }
        },
    }
    disabled = {
        "auto_pull": False,
        "worker_spend_ceiling_daily": None,
        "slots": {
            "test": {
                "count": 1,
                "category": "tester",
                "backend": "sandbox",
                "spend_ceiling_daily": 3.0,
            }
        },
    }

    async with db.acquire() as blocker:
        transaction = blocker.transaction()
        await transaction.start()
        await blocker.fetchrow(
            "SELECT project_id FROM project_officers WHERE project_id=$1 FOR UPDATE",
            UUID(seed["project_id"]),
        )
        write_enabled = asyncio.create_task(
            db.update_project_officer_post(
                seed["project_id"], config_updates={"officer": enabled}
            )
        )
        write_disabled = asyncio.create_task(
            db.update_project_officer_post(
                seed["project_id"], config_updates={"officer": disabled}
            )
        )
        await asyncio.sleep(0.1)
        assert not write_enabled.done() and not write_disabled.done()
        await transaction.commit()

    results = await asyncio.gather(write_enabled, write_disabled)
    observed = [result["post"]["config_override"]["officer"] for result in results]
    assert enabled in observed and disabled in observed
    post = await db.get_project_officer(seed["project_id"])
    thread = await db.get_thread(seed["thread_id"])
    final = post["config_override"]["officer"]
    assert final in (enabled, disabled)
    assert _json(thread["metadata"])["config_override"]["officer"] == {
        **final,
        "enabled": True,
    }


@pytest.mark.asyncio
async def test_predecessor_job_occupies_successor_lineage_capacity(db):
    seed = await _seed_post(db, count=1)
    old_preparation = await _prepare(db, seed)
    await admit_and_create_job(
        db,
        preparation=old_preparation,
        job_kwargs=_job_kwargs("predecessor work"),
        ticket_note_id="predecessor",
        ticket_ready_at=READY_GENERATION,
    )
    await db.decommission_project_officer(
        seed["project_id"], seed["thread_id"], reason="rotate", force=True
    )
    successor = await _seed_successor(db, seed, count=1)
    assert await db.register_project_officer_thread(seed["project_id"], successor)
    successor_seed = {**seed, "thread_id": successor}
    new_preparation = await _prepare(db, successor_seed)

    with pytest.raises(SlotAdmissionError):
        await admit_and_create_job(
            db,
            preparation=new_preparation,
            job_kwargs=_job_kwargs("successor work"),
            ticket_note_id="successor",
            ticket_ready_at=READY_GENERATION,
        )
    assert await _job_count(db) == 1


@pytest.mark.asyncio
async def test_ordinary_job_insert_does_not_wait_for_officer_post_lock(db):
    seed = await _seed_post(db)
    async with db.acquire() as lock_conn:
        transaction = lock_conn.transaction()
        await transaction.start()
        try:
            await lock_conn.fetchrow(
                "SELECT project_id FROM project_officers "
                "WHERE project_id=$1 FOR UPDATE",
                UUID(seed["project_id"]),
            )
            result = await asyncio.wait_for(
                db.create_job(
                    description="ordinary session work",
                    project_id=seed["project_id"],
                    config_name="worker_base",
                ),
                timeout=1.0,
            )
        finally:
            await transaction.rollback()
    assert result["id"]


@pytest.mark.asyncio
async def test_admission_wins_then_no_force_decommission_warns_and_keeps_post(db):
    seed = await _seed_post(db, count=2)
    preparation = await _prepare(db, seed)
    inserted = asyncio.Event()
    allow_admission_commit = asyncio.Event()

    async def hold_admission_open():
        async with db.acquire() as conn:
            async with conn.transaction():
                job = await admit_and_create_job_in_transaction(
                    db,
                    conn,
                    preparation=preparation,
                    job_kwargs=_job_kwargs("admission wins"),
                    ticket_note_id="race-admission-wins",
                    ticket_ready_at=READY_GENERATION,
                )
                inserted.set()
                await allow_admission_commit.wait()
                return job

    admission_task = asyncio.create_task(hold_admission_open())
    await asyncio.wait_for(inserted.wait(), timeout=2)
    handoff_task = asyncio.create_task(
        db.decommission_project_officer(
            seed["project_id"], seed["thread_id"], reason="race", force=False
        )
    )
    await asyncio.sleep(0.1)
    assert not handoff_task.done()

    allow_admission_commit.set()
    job, handoff = await asyncio.gather(admission_task, handoff_task)

    assert job["id"]
    assert handoff["transitioned"] is False
    assert handoff["blocked_by_in_flight"] is True
    assert [item["job_id"] for item in handoff["in_flight_jobs"]] == [str(job["id"])]
    post = await db.get_project_officer(seed["project_id"])
    thread = await db.get_thread(seed["thread_id"])
    assert str(post["thread_id"]) == seed["thread_id"]
    assert thread["status"] == "active"


@pytest.mark.asyncio
async def test_no_force_decommission_wins_then_racing_admission_is_rejected(db):
    seed = await _seed_post(db, count=1)
    preparation = await _prepare(db, seed)
    post_locked = asyncio.Event()
    allow_handoff = asyncio.Event()

    async def pause_after_post_lock(step):
        if step == "post_locked":
            post_locked.set()
            await allow_handoff.wait()

    handoff_task = asyncio.create_task(
        db.decommission_project_officer(
            seed["project_id"],
            seed["thread_id"],
            reason="race",
            force=False,
            fault_injector=pause_after_post_lock,
        )
    )
    await asyncio.wait_for(post_locked.wait(), timeout=2)
    admission_task = asyncio.create_task(
        admit_and_create_job(
            db,
            preparation=preparation,
            job_kwargs=_job_kwargs("handoff wins"),
            ticket_note_id="race-handoff-wins",
            ticket_ready_at=READY_GENERATION,
        )
    )
    await asyncio.sleep(0.1)
    assert not admission_task.done()

    allow_handoff.set()
    handoff, admission = await asyncio.gather(
        handoff_task, admission_task, return_exceptions=True
    )

    assert handoff["transitioned"] is True
    assert isinstance(admission, OfficerAdmissionConflict)
    assert admission.code in {"post_vacant", "stale_incarnation"}
    assert await _job_count(db) == 0


@pytest.mark.asyncio
async def test_no_force_gate_counts_every_nonterminal_status_across_lineage(db):
    seed = await _seed_post(db)
    predecessor = await _seed_successor(db, seed)
    async with db.acquire() as conn:
        await conn.execute(
            """
            UPDATE project_officers
               SET incarnations = jsonb_build_array(
                   jsonb_build_object('thread_id', $2::text))
             WHERE project_id = $1
            """,
            UUID(seed["project_id"]),
            predecessor,
        )

    nonterminal = (
        "created",
        "processing",
        "pending_review",
        "paused",
        "reviewing",
        "waiting",
        "waiting_for_reply",
    )
    for index, status in enumerate(nonterminal):
        await _seed_officer_job(
            db,
            seed,
            thread_id=predecessor if index % 2 else seed["thread_id"],
            status=status,
            label=f"lineage {status}",
        )
    for status in ("completed", "failed", "cancelled"):
        await _seed_officer_job(
            db,
            seed,
            thread_id=predecessor,
            status=status,
            label=f"terminal {status}",
        )

    handoff = await db.decommission_project_officer(
        seed["project_id"], seed["thread_id"], reason="lineage gate"
    )

    assert handoff["blocked_by_in_flight"] is True
    assert {item["status"] for item in handoff["in_flight_jobs"]} == set(nonterminal)
    assert (
        str((await db.get_project_officer(seed["project_id"]))["thread_id"])
        == seed["thread_id"]
    )


async def _seed_decommission_debris(db: PostgresDB, seed: dict[str, str]) -> str:
    job = await db.create_job(
        description="route owner",
        project_id=seed["project_id"],
        config_name="worker_base",
    )
    await db.enqueue_session_wake_event(
        seed["thread_id"],
        source="job_transition",
        dedup_key="job-transition-proof",
        payload={
            "job_id": str(job["id"]),
            "status": "completed",
            "description": "finished while retiring",
        },
        project_id=seed["project_id"],
    )
    await db.enqueue_session_wake_event(
        seed["thread_id"],
        source="timer",
        dedup_key="timer-proof",
        payload={"reason": "routine"},
        project_id=seed["project_id"],
    )
    route_id = str(uuid4())
    assert await db.create_message_route(
        {
            "route_id": route_id,
            "job_id": str(job["id"]),
            "project_id": seed["project_id"],
            "thread_id": "worker-thread",
            "policy_snapshot": {"applied": "officer_first"},
            "state": "pending_officer",
            "blocking": True,
            "officer_thread_id": seed["thread_id"],
            "officer_incarnation": 0,
            "officer_deadline": datetime.now(timezone.utc),
            "total_deadline": datetime.now(timezone.utc) + timedelta(hours=1),
            "transitions": [],
        }
    )
    return route_id


def _json(value):
    return json.loads(value) if isinstance(value, str) else value


@pytest.mark.asyncio
@pytest.mark.parametrize("fault_step", DECOMMISSION_STEPS)
async def test_decommission_fault_after_each_substep_rolls_back(db, fault_step):
    initial_post_state = {"standing": "before"}
    seed = await _seed_post(
        db,
        post_state=initial_post_state,
        officer_state={"digest": [{"subject": "harvest me"}]},
    )
    route_id = await _seed_decommission_debris(db, seed)

    def inject(step):
        if step == fault_step:
            raise RuntimeError(f"fault after {step}")

    with pytest.raises(RuntimeError, match=f"fault after {fault_step}"):
        await db.decommission_project_officer(
            seed["project_id"],
            seed["thread_id"],
            reason="fault proof",
            fault_injector=inject,
        )

    async with db.acquire() as conn:
        post = await conn.fetchrow(
            "SELECT thread_id, state, incarnations FROM project_officers "
            "WHERE project_id=$1",
            UUID(seed["project_id"]),
        )
        thread = await conn.fetchrow(
            "SELECT status, metadata FROM threads WHERE id=$1",
            UUID(seed["thread_id"]),
        )
        wakes = await conn.fetchval(
            "SELECT COUNT(*) FROM session_wake_events WHERE thread_id=$1",
            UUID(seed["thread_id"]),
        )
        route = await conn.fetchrow(
            "SELECT state, transitions, user_delivery_at "
            "FROM job_message_routes WHERE route_id=$1",
            UUID(route_id),
        )
    assert str(post["thread_id"]) == seed["thread_id"]
    assert _json(post["state"]) == initial_post_state
    assert _json(post["incarnations"]) == []
    assert thread["status"] == "active"
    assert _json(thread["metadata"])["config_override"]["officer"]["enabled"]
    assert wakes == 2
    assert route["state"] == "pending_officer"
    assert _json(route["transitions"]) == []
    assert route["user_delivery_at"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize("fault_step", DECOMMISSION_STEPS)
async def test_retirement_authorization_and_officer_mutations_rollback_together(
    db, fault_step
):
    """No crash point leaves a mutated Post behind a hidden preflight."""
    seed = await _seed_post(
        db,
        post_state={"standing": "before"},
        officer_state={"digest": [{"subject": "harvest me"}]},
    )
    await _seed_decommission_debris(db, seed)
    retirement = await db.begin_pinned_thread_retirement(
        seed["thread_id"], permanent=False, settle_status="ended"
    )

    def inject(step):
        if step == fault_step:
            raise RuntimeError(f"retirement fault after {step}")

    with pytest.raises(RuntimeError, match=f"retirement fault after {fault_step}"):
        await db.decommission_project_officer(
            seed["project_id"],
            seed["thread_id"],
            reason="retirement rollback proof",
            retirement_token=retirement["token"],
            retirement_generation=retirement["generation"],
            retirement_settle_status="ended",
            fault_injector=inject,
        )

    async with db.acquire() as conn:
        post = await conn.fetchrow(
            "SELECT thread_id,state,incarnations FROM project_officers "
            "WHERE project_id=$1",
            UUID(seed["project_id"]),
        )
        thread = await conn.fetchrow(
            "SELECT metadata,runtime_retirement_token,"
            "runtime_retirement_authorized_at FROM threads WHERE id=$1",
            UUID(seed["thread_id"]),
        )
    assert str(post["thread_id"]) == seed["thread_id"]
    assert _json(post["state"]) == {"standing": "before"}
    assert _json(post["incarnations"]) == []
    assert _json(thread["metadata"])["config_override"]["officer"]["enabled"]
    assert str(thread["runtime_retirement_token"]) == retirement["token"]
    assert thread["runtime_retirement_authorized_at"] is None
    assert await db.abort_pinned_thread_retirement(
        seed["thread_id"],
        token=retirement["token"],
        generation=retirement["generation"],
    )


@pytest.mark.asyncio
async def test_no_force_officer_refusal_keeps_preflight_hidden_and_abortable(db):
    seed = await _seed_post(db)
    await _seed_officer_job(
        db,
        seed,
        thread_id=seed["thread_id"],
        status="processing",
        label="in-flight refusal proof",
    )
    retirement = await db.begin_pinned_thread_retirement(
        seed["thread_id"], permanent=False, settle_status="ended"
    )

    result = await db.decommission_project_officer(
        seed["project_id"],
        seed["thread_id"],
        reason="no-force refusal",
        force=False,
        retirement_token=retirement["token"],
        retirement_generation=retirement["generation"],
        retirement_settle_status="ended",
    )
    assert result["blocked_by_in_flight"] is True
    current = await db.get_thread(seed["thread_id"])
    assert current["runtime_retirement_authorized_at"] is None
    assert _json(current["metadata"])["config_override"]["officer"]["enabled"]
    assert (
        str((await db.get_project_officer(seed["project_id"]))["thread_id"])
        == seed["thread_id"]
    )
    assert await db.abort_pinned_thread_retirement(
        seed["thread_id"],
        token=retirement["token"],
        generation=retirement["generation"],
    )


@pytest.mark.asyncio
async def test_repeated_decommission_is_idempotent_and_stages_one_fallback(db):
    seed = await _seed_post(
        db,
        post_state={"standing": "before"},
        officer_state={"digest": [{"subject": "harvest me"}]},
    )
    route_id = await _seed_decommission_debris(db, seed)

    first = await db.decommission_project_officer(
        seed["project_id"], seed["thread_id"], reason="retired"
    )
    second = await db.decommission_project_officer(
        seed["project_id"], seed["thread_id"], reason="retired"
    )

    assert first["transitioned"] is True
    assert len(first["routes"]) == 1
    assert first["routes"][0]["user_delivery_at"] is None
    assert second["transitioned"] is False
    assert second["already_decommissioned"] is True
    assert second["routes"] == []
    async with db.acquire() as conn:
        post = await conn.fetchrow(
            "SELECT thread_id, state, incarnations FROM project_officers "
            "WHERE project_id=$1",
            UUID(seed["project_id"]),
        )
        thread = await conn.fetchrow(
            "SELECT status, metadata FROM threads WHERE id=$1",
            UUID(seed["thread_id"]),
        )
        wakes = await conn.fetchval(
            "SELECT COUNT(*) FROM session_wake_events WHERE thread_id=$1",
            UUID(seed["thread_id"]),
        )
        route = await conn.fetchrow(
            "SELECT state, transitions, user_delivery_at "
            "FROM job_message_routes WHERE route_id=$1",
            UUID(route_id),
        )
    state = _json(post["state"])
    incarnations = _json(post["incarnations"])
    assert post["thread_id"] is None
    assert state["standing"] == "before"
    assert state["digest"] == [{"subject": "harvest me"}]
    assert len(state["while_vacant"]) == 1
    assert len(incarnations) == 1
    assert incarnations[0]["thread_id"] == seed["thread_id"]
    assert wakes == 0
    assert route["state"] == "escalated_to_user"
    assert len(_json(route["transitions"])) == 1
    # The lifecycle transaction created durable outbox intent only. No
    # notification provider ran inside it, so acceptance remains unstamped.
    assert route["user_delivery_at"] is None
    # The post handoff disables Officer mode; exact retirement settlement is
    # the sole owner of the later terminal lifecycle edge.
    assert thread["status"] == "active"
    assert not _json(thread["metadata"])["config_override"]["officer"]["enabled"]


@pytest.mark.asyncio
async def test_direct_end_of_orphan_does_not_touch_occupied_post_debris(db):
    seed = await _seed_post(
        db,
        post_state={"standing": "keep"},
        officer_state={"digest": [{"subject": "current only"}]},
    )
    orphan = await _seed_successor(db, seed)
    route_id = await _seed_decommission_debris(db, seed)
    assert await db.enqueue_session_wake_event(
        orphan,
        source="job_transition",
        dedup_key="orphan-wake",
        payload={"job_id": "orphan-job", "status": "completed"},
        project_id=seed["project_id"],
    )

    retired = await db.decommission_project_officer(
        seed["project_id"],
        orphan,
        reason="direct_end",
        allow_orphan_retirement=True,
    )

    assert retired["transitioned"] is False
    assert retired["orphan_retired"] is True
    assert retired["current_thread_id"] == seed["thread_id"]
    async with db.acquire() as conn:
        post = await conn.fetchrow(
            "SELECT thread_id, state, incarnations FROM project_officers "
            "WHERE project_id=$1",
            UUID(seed["project_id"]),
        )
        current = await conn.fetchrow(
            "SELECT status, metadata FROM threads WHERE id=$1",
            UUID(seed["thread_id"]),
        )
        ended_orphan = await conn.fetchrow(
            "SELECT status, metadata FROM threads WHERE id=$1", UUID(orphan)
        )
        wake_count = await conn.fetchval(
            "SELECT COUNT(*) FROM session_wake_events WHERE thread_id=ANY($1::uuid[])",
            [UUID(seed["thread_id"]), UUID(orphan)],
        )
        route = await conn.fetchrow(
            "SELECT state, transitions FROM job_message_routes WHERE route_id=$1",
            UUID(route_id),
        )
    assert str(post["thread_id"]) == seed["thread_id"]
    assert _json(post["state"]) == {"standing": "keep"}
    assert _json(post["incarnations"]) == []
    assert current["status"] == "active"
    assert _json(current["metadata"])["config_override"]["officer"]["enabled"]
    assert ended_orphan["status"] == "active"
    assert not _json(ended_orphan["metadata"])["config_override"]["officer"]["enabled"]
    assert wake_count == 3
    assert route["state"] == "pending_officer"
    assert _json(route["transitions"]) == []


@pytest.mark.asyncio
async def test_orphan_end_wins_vacant_post_race_and_registration_loses(db):
    seed = await _seed_post(db)
    await db.decommission_project_officer(
        seed["project_id"], seed["thread_id"], reason="vacate"
    )
    orphan = await _seed_successor(db, seed)
    assert await db.enqueue_session_wake_event(
        orphan,
        source="job_transition",
        dedup_key="preserve-orphan-wake",
        payload={"job_id": "orphan-job", "status": "completed"},
        project_id=seed["project_id"],
    )
    post_locked = asyncio.Event()
    allow_end = asyncio.Event()

    async def pause_after_post_lock(step):
        if step == "post_locked":
            post_locked.set()
            await allow_end.wait()

    end_task = asyncio.create_task(
        db.decommission_project_officer(
            seed["project_id"],
            orphan,
            reason="direct_end",
            allow_orphan_retirement=True,
            fault_injector=pause_after_post_lock,
        )
    )
    await asyncio.wait_for(post_locked.wait(), timeout=2)
    registration_task = asyncio.create_task(
        db.register_project_officer_thread(seed["project_id"], orphan)
    )
    await asyncio.sleep(0.1)
    assert not registration_task.done()

    allow_end.set()
    ended, registration = await asyncio.gather(end_task, registration_task)

    assert ended["orphan_retired"] is True
    assert registration is None
    post = await db.get_project_officer(seed["project_id"])
    thread = await db.get_thread(orphan)
    async with db.acquire() as conn:
        wake_count = await conn.fetchval(
            "SELECT COUNT(*) FROM session_wake_events WHERE thread_id=$1",
            UUID(orphan),
        )
    assert post["thread_id"] is None
    assert len(post["incarnations"]) == 1
    assert post["incarnations"][0]["thread_id"] == seed["thread_id"]
    assert thread["status"] == "active"
    assert wake_count == 1


@pytest.mark.asyncio
async def test_commission_continuity_racing_decommission_is_preserved_and_truthful(db):
    seed = await _seed_post(db, post_state={"sitrep_fingerprints": {"old-job": "fp"}})
    await db.decommission_project_officer(
        seed["project_id"], seed["thread_id"], reason="vacate"
    )
    continuity_entry = {
        "job_id": "completed-while-vacant",
        "status": "completed",
        "description": "carry me",
        "at": datetime.now(timezone.utc).isoformat(),
    }
    await db.append_project_officer_while_vacant(seed["project_id"], [continuity_entry])
    successor = await _seed_successor(db, seed)
    post_linked = asyncio.Event()
    allow_continuity = asyncio.Event()

    async def pause_after_link(step):
        if step == "post_linked":
            post_linked.set()
            await allow_continuity.wait()

    registration_task = asyncio.create_task(
        db.register_project_officer_thread(
            seed["project_id"],
            successor,
            commission_continuity=True,
            fault_injector=pause_after_link,
        )
    )
    await asyncio.wait_for(post_linked.wait(), timeout=2)
    handoff_task = asyncio.create_task(
        db.decommission_project_officer(
            seed["project_id"], successor, reason="racing handoff", force=True
        )
    )
    await asyncio.sleep(0.1)
    assert not handoff_task.done()

    allow_continuity.set()
    registration, handoff = await asyncio.gather(registration_task, handoff_task)

    continuity = registration["commission_continuity"]
    assert continuity["brief_enqueued"] is True
    assert continuity["while_vacant"] == [continuity_entry]
    assert handoff["transitioned"] is True
    assert (
        await db.confirm_project_officer_incarnation(seed["project_id"], successor)
        is False
    )
    post = await db.get_project_officer(seed["project_id"])
    thread = await db.get_thread(successor)
    async with db.acquire() as conn:
        wake_count = await conn.fetchval(
            "SELECT COUNT(*) FROM session_wake_events WHERE thread_id=$1",
            UUID(successor),
        )
    assert post["thread_id"] is None
    assert post["state"]["while_vacant"] == [continuity_entry]
    assert post["state"]["sitrep_fingerprints"] == {"old-job": "fp"}
    assert thread["status"] == "active"
    assert wake_count == 0


@pytest.mark.asyncio
async def test_completion_vs_commission_event_appears_exactly_once(db):
    seed = await _seed_post(db)
    await db.decommission_project_officer(
        seed["project_id"], seed["thread_id"], reason="vacate"
    )
    successor = await _seed_successor(db, seed)

    outcomes = await _race(
        lambda: db.route_project_officer_job_transition(
            seed["project_id"],
            job_id="race-job",
            status="completed",
            description="completion race",
            dedup_key="race-job:completed",
        ),
        lambda: db.register_project_officer_thread(
            seed["project_id"], successor, commission_continuity=True
        ),
    )

    assert all(isinstance(outcome, dict) for outcome in outcomes)
    post = await db.get_project_officer(seed["project_id"])
    assert str(post["thread_id"]) == successor
    assert post["state"].get("while_vacant") == []
    async with db.acquire() as conn:
        rows = await conn.fetch(
            "SELECT source, payload FROM session_wake_events "
            "WHERE thread_id=$1 ORDER BY id",
            UUID(successor),
        )
    occurrences = 0
    for row in rows:
        payload = _json(row["payload"])
        if row["source"] == "commission":
            occurrences += sum(
                1
                for entry in payload.get("while_vacant") or []
                if entry.get("job_id") == "race-job"
                and entry.get("status") == "completed"
            )
        elif row["source"] == "job_transition":
            occurrences += int(
                payload.get("job_id") == "race-job"
                and payload.get("status") == "completed"
            )
    assert occurrences == 1
    assert {row["source"] for row in rows} in (
        {"commission"},
        {"commission", "job_transition"},
    )


@pytest.mark.asyncio
async def test_losing_commission_cannot_patch_winning_officer_configuration(db):
    seed = await _seed_post(db)
    await db.decommission_project_officer(
        seed["project_id"], seed["thread_id"], reason="vacate"
    )
    initial = await db.get_project_officer(seed["project_id"])
    generation = initial["updated_at"]
    winning = await db.update_project_officer_post(
        seed["project_id"],
        config_updates={"officer": {"slots": _roster(count=2)}},
        expected_vacant_updated_at=generation,
    )
    winning_config = winning["post"]["config_override"]
    winner = await _seed_successor(db, seed, count=2)
    linked = asyncio.Event()
    allow_registration = asyncio.Event()

    async def pause_after_link(step):
        if step == "post_linked":
            linked.set()
            await allow_registration.wait()

    registration_task = asyncio.create_task(
        db.register_project_officer_thread(
            seed["project_id"],
            winner,
            expected_post_config_override=winning_config,
            fault_injector=pause_after_link,
        )
    )
    await asyncio.wait_for(linked.wait(), timeout=2)
    losing_update_task = asyncio.create_task(
        db.update_project_officer_post(
            seed["project_id"],
            config_updates={"officer": {"slots": _roster(count=9)}},
            expected_vacant_updated_at=generation,
        )
    )
    await asyncio.sleep(0.1)
    assert not losing_update_task.done()

    allow_registration.set()
    registration, losing_update = await asyncio.gather(
        registration_task, losing_update_task, return_exceptions=True
    )

    assert registration is not None
    assert isinstance(losing_update, OfficerPostLifecycleConflict)
    assert losing_update.code == "commission_generation_changed"
    post = await db.get_project_officer(seed["project_id"])
    thread = await db.get_thread(winner)
    assert post["config_override"]["officer"]["slots"]["line"]["count"] == 2
    assert (
        _json(thread["metadata"])["config_override"]["officer"]["slots"]["line"][
            "count"
        ]
        == 2
    )


@pytest.mark.asyncio
async def test_concurrent_decommission_blocks_successor_until_handoff_commit(db):
    seed = await _seed_post(db)
    successor = await _seed_successor(db, seed)
    post_locked = asyncio.Event()
    allow_commit = asyncio.Event()

    async def pause_after_post_lock(step):
        if step == "post_locked":
            post_locked.set()
            await allow_commit.wait()

    handoff_task = asyncio.create_task(
        db.decommission_project_officer(
            seed["project_id"],
            seed["thread_id"],
            reason="rotate",
            fault_injector=pause_after_post_lock,
        )
    )
    await asyncio.wait_for(post_locked.wait(), timeout=2)
    register_task = asyncio.create_task(
        db.register_project_officer_thread(seed["project_id"], successor)
    )
    await asyncio.sleep(0.1)
    assert not register_task.done()

    allow_commit.set()
    handoff, registration = await asyncio.gather(handoff_task, register_task)
    assert handoff["transitioned"] is True
    assert registration is not None
    post = await db.get_project_officer(seed["project_id"])
    assert str(post["thread_id"]) == successor
    assert len(post["incarnations"]) == 1
    assert post["incarnations"][0]["thread_id"] == seed["thread_id"]


@pytest.mark.asyncio
async def test_blocking_route_snapshot_cannot_land_after_decommission(db):
    """The route/freeze unit shares the post lock prefix with handoff.

    Once decommission owns the post, a stale officer-routing snapshot waits;
    after the handoff commits it loses cleanly and leaves no route, wake,
    message, or frozen job behind.
    """
    seed = await _seed_post(db)
    job = await db.create_job(
        description="worker awaiting route",
        project_id=seed["project_id"],
        config_name="worker_base",
    )
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET status='processing' WHERE id=$1",
            job["id"],
        )

    post_locked = asyncio.Event()
    allow_commit = asyncio.Event()

    async def pause_after_post_lock(step):
        if step == "post_locked":
            post_locked.set()
            await allow_commit.wait()

    handoff_task = asyncio.create_task(
        db.decommission_project_officer(
            seed["project_id"],
            seed["thread_id"],
            reason="route race",
            fault_injector=pause_after_post_lock,
        )
    )
    await asyncio.wait_for(post_locked.wait(), timeout=2)

    route_id = str(uuid4())
    route_task = asyncio.create_task(
        db.create_routed_blocking_freeze(
            str(job["id"]),
            {
                "status": "waiting_for_reply",
                "freeze_type": "blocking_message",
                "thread_id": "worker-thread",
                "route_id": route_id,
            },
            route={
                "route_id": route_id,
                "job_id": str(job["id"]),
                "project_id": seed["project_id"],
                "thread_id": "worker-thread",
                "policy_snapshot": {"applied": "officer_first"},
                "state": "pending_officer",
                "officer_thread_id": seed["thread_id"],
                "officer_incarnation": 0,
                "officer_deadline": datetime.now(timezone.utc),
                "total_deadline": datetime.now(timezone.utc) + timedelta(hours=1),
                "transitions": [],
            },
            message_entry={
                "subject": "Need input",
                "message": "Which path?",
                "status": "sent",
            },
            wake={
                "thread_id": seed["thread_id"],
                "source": "worker_message",
                "dedup_key": f"route:{route_id}",
                "payload": {"route_id": route_id},
            },
            expected_lane="pinned",
        )
    )
    await asyncio.sleep(0.1)
    assert not route_task.done()

    allow_commit.set()
    handoff, routed = await asyncio.gather(handoff_task, route_task)
    assert handoff["transitioned"] is True
    assert routed is None
    async with db.acquire() as conn:
        stored_job = await conn.fetchrow(
            "SELECT status, freeze_data FROM jobs WHERE id=$1", job["id"]
        )
        routes = await conn.fetchval("SELECT COUNT(*) FROM job_message_routes")
        wakes = await conn.fetchval("SELECT COUNT(*) FROM session_wake_events")
        messages = await conn.fetchval("SELECT COUNT(*) FROM message_log")
    assert stored_job["status"] == "processing"
    assert stored_job["freeze_data"] is None
    assert routes == wakes == messages == 0


@pytest.mark.asyncio
async def test_hold_with_route_reason_escalates_blocking_routes(db):
    """A hold that carries a ``route_reason`` must reach PostgreSQL.

    The route-escalation branch only runs when a hold is stamped *and* the
    caller names a reason — the shape every real caller uses (conference
    open, the maintenance-hold endpoint, archive quiesce) and the one shape
    no other proof in this module exercised. Its ``jsonb_build_object``
    arguments carry no inferable type, so an uncast parameter makes asyncpg
    fail at PREPARE with ``IndeterminateDatatypeError`` — a failure the
    conference path swallows as non-fatal and the endpoint returns as a 500.
    """
    seed = await _seed_post(db)
    job = await _seed_officer_job(
        db,
        seed,
        thread_id=seed["thread_id"],
        status="processing",
        label="hold route proof",
    )
    async with db.acquire() as conn:
        route_id = await conn.fetchval(
            """
            INSERT INTO job_message_routes (
                job_id, project_id, thread_id, officer_thread_id,
                state, blocking
            ) VALUES ($1, $2, $3, $4, 'pending_officer', TRUE)
            RETURNING route_id
            """,
            job["id"],
            UUID(seed["project_id"]),
            seed["thread_id"],
            UUID(seed["thread_id"]),
        )

    result = await db.set_project_officer_hold(
        seed["project_id"],
        expected_thread_id=seed["thread_id"],
        hold={"kind": "conference", "since": "now", "thread_id": str(uuid4())},
        route_reason="officer_hold",
    )

    metadata = _json(result["thread"]["metadata"])
    assert metadata["config_override"]["officer"]["hold"]["kind"] == "conference"
    assert [str(r["route_id"]) for r in result["routes"]] == [str(route_id)]

    async with db.acquire() as conn:
        stored = await conn.fetchrow(
            "SELECT state, transitions FROM job_message_routes WHERE route_id=$1",
            route_id,
        )
    assert stored["state"] == "escalated_to_user"
    transition = _json(stored["transitions"])[-1]
    assert transition["to"] == "escalated_to_user"
    assert transition["note"] == "officer_hold"
    assert transition["actor_id"] == "drain:officer_hold"
