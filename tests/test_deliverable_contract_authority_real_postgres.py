"""Real-PostgreSQL authority and race proofs for deliverable contracts."""

from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

from orchestrator.database.postgres import PostgresDB
from orchestrator.services.officer_admission import (
    OfficerAdmissionConflict,
    admit_and_create_job,
    prepare_officer_admission,
    record_rejected_ticket_delivery_requirement,
)
from orchestrator.services.deliverable_contracts import (
    DeliveryContractConflict,
    prepare_delivery_contract,
)
from agent.database.postgres_db import PostgresDB as AgentPostgresDB


SCHEMA_FILE = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "orchestrator"
    / "database"
    / "schema_current.sql"
)
PR_REVISION = "a" * 40


@pytest.fixture(scope="module")
def pg_dsn():
    try:
        container = PostgresContainer("postgres:15")
        container.start()
    except Exception as exc:
        pytest.skip(f"local PostgreSQL container unavailable: {exc}")
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
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def db(pg_dsn, _schema_applied):
    store = PostgresDB(
        connection_string=pg_dsn,
        min_connections=1,
        max_connections=10,
    )
    await store.connect()
    async with store.acquire() as conn:
        await conn.execute(
            "TRUNCATE officer_ticket_deliverable_requirements, "
            "officer_ticket_claims, job_pull_request_authorities, "
            "job_deliverable_contracts, "
            "job_datasources, project_datasources, datasources, jobs, "
            "project_officers, threads, projects CASCADE"
        )
    try:
        yield store
    finally:
        await store.close()


async def _seed_repository_job(
    db: PostgresDB,
    *,
    forge: str = "github",
    connection_url: str = "https://github.com/acme/widget",
) -> tuple[UUID, UUID, UUID, int]:
    project_id = uuid4()
    datasource_id = uuid4()
    job_id = uuid4()
    async with db.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "INSERT INTO projects (id, name) VALUES ($1, 'delivery project')",
                project_id,
            )
            await conn.execute(
                """
                INSERT INTO datasources (
                    id, name, type, connection_url, config, read_only,
                    policy_revision
                ) VALUES ($1, $2, 'repository', $3, $4::jsonb, FALSE, 7)
                """,
                datasource_id,
                f"Widget-{str(datasource_id)[:8]}",
                connection_url,
                json.dumps({"forge": forge}),
            )
            await conn.execute(
                "INSERT INTO project_datasources "
                "(project_id, datasource_id, read_only) VALUES ($1, $2, FALSE)",
                project_id,
                datasource_id,
            )
            policy_revision = int(
                await conn.fetchval(
                    "SELECT policy_revision FROM datasources WHERE id=$1",
                    datasource_id,
                )
            )
            # Exact rolling-writer shape: the insert carries forged evidence,
            # then the current writer promotes the capture row to exact
            # server authority in the same transaction. A later promotion is
            # deliberately forbidden by the immutability trigger.
            await conn.execute(
                """
                INSERT INTO jobs (id, description, project_id, status, context)
                VALUES ($1, 'publication', $2, 'processing', $3::jsonb)
                """,
                job_id,
                project_id,
                json.dumps(
                    {
                        "required_deliverables": ["pr:Acme/Widget"],
                        "pull_request": {
                            "repo": "victim/private",
                            "number": 99,
                        },
                        "deliverable_contract_provenance": {"forged": True},
                    }
                ),
            )
            await conn.execute(
                "INSERT INTO job_datasources (job_id, datasource_id) VALUES ($1, $2)",
                job_id,
                datasource_id,
            )
            await conn.execute(
                """
                UPDATE job_deliverable_contracts
                   SET normalized_deliverables = ARRAY['pr:acme/widget'],
                       pr_repositories = ARRAY['acme/widget'],
                       pr_bindings = $2::jsonb,
                       contract_digest = 'server-test',
                       provenance = 'server_normalized'
                 WHERE job_id = $1
                """,
                job_id,
                json.dumps(
                    [
                        {
                            "repository": "acme/widget",
                            "datasource_id": str(datasource_id),
                            "forge": forge,
                            "policy_revision": policy_revision,
                        }
                    ]
                ),
            )
    return project_id, datasource_id, job_id, policy_revision


@pytest.mark.asyncio
async def test_old_writer_is_stripped_and_contract_cannot_mutate_or_complete_unproven(
    db,
):
    _project_id, _datasource_id, job_id, _revision = await _seed_repository_job(db)
    async with db.acquire() as conn:
        row = await conn.fetchrow("SELECT context FROM jobs WHERE id=$1", job_id)
        assert "pull_request" not in row["context"]
        assert "deliverable_contract_provenance" not in row["context"]

        with pytest.raises(asyncpg.CheckViolationError) as mutation:
            await conn.execute(
                "UPDATE jobs SET context = jsonb_set(context, "
                "'{required_deliverables}', '[\"kb:failure-note\"]') "
                "WHERE id=$1",
                job_id,
            )
        assert mutation.value.constraint_name == "job_deliverable_contract_is_immutable"

        with pytest.raises(asyncpg.CheckViolationError) as authority_mutation:
            await conn.execute(
                "UPDATE job_deliverable_contracts "
                "SET normalized_deliverables=ARRAY['kb:laundered'], "
                "pr_repositories=ARRAY[]::text[], pr_bindings='[]'::jsonb "
                "WHERE job_id=$1",
                job_id,
            )
        assert (
            authority_mutation.value.constraint_name
            == "job_deliverable_contract_row_is_immutable"
        )

        with pytest.raises(asyncpg.CheckViolationError) as completion:
            await conn.execute("UPDATE jobs SET status='completed' WHERE id=$1", job_id)
        assert completion.value.constraint_name == "pr_deliverable_requires_live_proof"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "declared",
    [
        "repos/KurortEngine/docs/demo.html",
        "./repos/KurortEngine/docs/demo.html",
        "/repos/KurortEngine/docs/demo.html",
    ],
)
async def test_old_replica_cannot_complete_historical_cloned_repo_contract(
    db, declared
):
    job_id = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO jobs (id, description, status, context)
            VALUES (
                $1, 'old replica publication', 'processing',
                jsonb_build_object(
                    'required_deliverables', jsonb_build_array($2::text)
                )
            )
            """,
            job_id,
            declared,
        )
        contract = await conn.fetchrow(
            "SELECT normalized_deliverables, provenance "
            "FROM job_deliverable_contracts WHERE job_id=$1",
            job_id,
        )
        assert dict(contract) == {
            "normalized_deliverables": [declared],
            "provenance": "rolling_trigger_backfill",
        }
        with pytest.raises(asyncpg.CheckViolationError) as late_adoption:
            await conn.execute(
                "UPDATE job_deliverable_contracts "
                "SET provenance='server_normalized' WHERE job_id=$1",
                job_id,
            )
        assert (
            late_adoption.value.constraint_name
            == "job_deliverable_contract_row_is_immutable"
        )
        with pytest.raises(asyncpg.CheckViolationError) as completion:
            await conn.execute("UPDATE jobs SET status='completed' WHERE id=$1", job_id)
        assert completion.value.constraint_name == "pr_deliverable_requires_live_proof"


@pytest.mark.asyncio
async def test_old_replica_cannot_admit_claimed_ticket_without_current_receipt(db):
    project_id, thread_id = await _seed_officer(db)
    generation = datetime(2026, 8, 24, 7, 30, tzinfo=timezone.utc)
    job_id = uuid4()

    async with db.acquire() as conn:
        with pytest.raises(asyncpg.CheckViolationError) as old_writer:
            async with conn.transaction():
                await db.insert_officer_ticket_claim(
                    conn=conn,
                    project_id=project_id,
                    ticket_note_id="rolling-ticket",
                    ready_generation_at=generation,
                    source="manual",
                    officer_thread_id=thread_id,
                    officer_incarnation=1,
                    officer_slot="line",
                    work_category="executor",
                    # Previous replicas already emitted the BP-05 SHA-256
                    # admission fingerprint.  Keep that historical field valid
                    # so the transaction reaches the additive 0182 fence.
                    admission_config_fingerprint="0" * 64,
                    admission_lineage_size=2,
                    job_id=job_id,
                )
                # Exact pre-0182 writer behavior: it inserts the claim and job,
                # but knows nothing about the server-normalized contract row.
                # The capture trigger may create a compatibility row; that is
                # intentionally insufficient for ticket admission.
                await conn.execute(
                    """
                    INSERT INTO jobs (
                        id, description, project_id, status, origin,
                        created_by_thread_id, context
                    ) VALUES (
                        $1, 'old rolling Officer', $2, 'created', 'officer',
                        $3, $4::jsonb
                    )
                    """,
                    job_id,
                    UUID(project_id),
                    UUID(thread_id),
                    json.dumps(
                        {
                            "ticket_note_id": "rolling-ticket",
                            "officer_slot": "line",
                            "work_category": "executor",
                            "officer_admission": {
                                "project_id": project_id,
                                "thread_id": thread_id,
                                "incarnation": 1,
                                "lineage_size": 2,
                                "ticket_ready_at": generation.isoformat(),
                                "ticket_claim_source": "manual",
                                "config_fingerprint": "0" * 64,
                                "slot": "line",
                                "category": "executor",
                            },
                            "_workspace_contract": {
                                "version": 1,
                                "requested_backend": None,
                                "assigned_backend": "sandbox",
                                "assignment_source": "officer_slot:line",
                            },
                            "required_deliverables": ["kb:failure-note"],
                        }
                    ),
                )
        assert (
            old_writer.value.constraint_name
            == "officer_ticket_delivery_writer_is_current"
        )

    async with db.acquire() as conn:
        assert await conn.fetchval("SELECT count(*) FROM jobs") == 0
        assert await conn.fetchval("SELECT count(*) FROM officer_ticket_claims") == 0


@pytest.mark.asyncio
async def test_current_officer_admission_stamps_empty_contract_receipt(db):
    project_id, thread_id = await _seed_officer(db)
    preparation = await prepare_officer_admission(
        db,
        project_id=project_id,
        thread_id=thread_id,
        requested_slot="line",
    )
    job = await admit_and_create_job(
        db,
        preparation=preparation,
        job_kwargs={
            "description": "current rolling Officer",
            "config_name": "worker_base",
            "context": {},
            "config_override": {},
            "datasource_ids": [],
            "datasource_selection_provenance": {},
            "datasource_policy_revisions": {},
            "execution_lane": "pinned",
        },
        ticket_note_id="current-ticket",
        ticket_ready_at=datetime(2026, 8, 24, 7, 31, tzinfo=timezone.utc),
    )
    contract = await db.get_job_deliverable_contract(str(job["id"]))
    assert contract is not None
    assert contract["normalized_deliverables"] == []
    assert contract["pr_repositories"] == []
    assert contract["provenance"] == "server_normalized"


@pytest.mark.asyncio
async def test_common_create_funnel_strips_pr_and_provenance_but_keeps_contract(db):
    created = await db.create_job(
        description="caller tried to forge delivery",
        context={
            "required_deliverables": ["kb:legitimate-report"],
            "pull_request": {"repo": "victim/private", "number": 9},
            "deliverable_contract_provenance": {"source": "caller"},
            "prior_deliverable_contract": ["repos/victim/private.md"],
            "required_pr_repositories": ["victim/private"],
        },
        origin="user",
    )
    stored = await db.get_job(str(created["id"]))
    context = stored["context"]
    context = json.loads(context) if isinstance(context, str) else context
    assert context["required_deliverables"] == ["kb:legitimate-report"]
    assert not {
        "pull_request",
        "deliverable_contract_provenance",
        "prior_deliverable_contract",
        "required_pr_repositories",
    } & set(context)
    contract = await db.get_job_deliverable_contract(str(created["id"]))
    assert contract["normalized_deliverables"] == ["kb:legitimate-report"]
    assert contract["provenance"] == "server_normalized"


@pytest.mark.asyncio
async def test_context_only_pr_evidence_cannot_create_authority_or_complete(db):
    _project_id, datasource_id, job_id, _revision = await _seed_repository_job(db)
    forged = {
        "forge": "github",
        "repo": "acme/widget",
        "number": 9,
        "url": "https://github.com/acme/widget/pull/9",
        "head": "feature/delivery",
        "base": "develop",
    }
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.CheckViolationError) as raw_update:
            await conn.execute(
                "UPDATE jobs SET context = context || "
                "jsonb_build_object('pull_request', $2::jsonb) WHERE id=$1",
                job_id,
                json.dumps(forged),
            )
        assert (
            raw_update.value.constraint_name
            == "pull_request_projection_requires_authority"
        )

    # The current generic merge strips the field before SQL; the database
    # trigger independently fences a rolling old writer above.
    assert await db.merge_job_context(str(job_id), {"pull_request": forged})
    assert await db.get_job_pull_request_authority(str(job_id)) is None
    assert not await db.mark_job_pr_deliverable_verified(
        str(job_id),
        datasource_id=str(datasource_id),
        repository="acme/widget",
        number=9,
        record_id=str(uuid4()),
        record_generation=1,
        head="feature/delivery",
        base="develop",
        head_revision=PR_REVISION,
        state="open",
    )
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.CheckViolationError) as completion:
            await conn.execute("UPDATE jobs SET status='completed' WHERE id=$1", job_id)
        assert completion.value.constraint_name == "pr_deliverable_requires_live_proof"


@pytest.mark.asyncio
async def test_exact_live_proof_allows_completion_but_detach_race_fails_closed(
    db, pg_dsn
):
    _project_id, datasource_id, job_id, _revision = await _seed_repository_job(db)
    record = {
        "forge": "github",
        "repo": "acme/widget",
        "number": 9,
        "url": "https://github.com/acme/widget/pull/9",
        "head": "feature/delivery",
        "base": "develop",
    }
    writer = AgentPostgresDB(pg_dsn, min_connections=1, max_connections=2)
    await writer.connect()
    try:
        assert await writer.jobs.record_pull_request(
            job_id, datasource_id, record, source_revision=PR_REVISION
        )
    finally:
        await writer.close()
    authority = await db.get_job_pull_request_authority(str(job_id))
    assert authority is not None
    assert await db.mark_job_pr_deliverable_verified(
        str(job_id),
        datasource_id=str(datasource_id),
        repository="acme/widget",
        number=9,
        record_id=str(authority["record_id"]),
        record_generation=authority["record_generation"],
        head="feature/delivery",
        base="develop",
        head_revision=PR_REVISION,
        state="open",
    )
    async with db.acquire() as conn:
        await conn.execute("UPDATE jobs SET status='completed' WHERE id=$1", job_id)

    _project_id, datasource_id, racing_job_id, _revision = await _seed_repository_job(
        db
    )
    writer = AgentPostgresDB(pg_dsn, min_connections=1, max_connections=2)
    await writer.connect()
    try:
        assert await writer.jobs.record_pull_request(
            racing_job_id,
            datasource_id,
            record,
            source_revision=PR_REVISION,
        )
    finally:
        await writer.close()
    racing_authority = await db.get_job_pull_request_authority(str(racing_job_id))
    assert await db.mark_job_pr_deliverable_verified(
        str(racing_job_id),
        datasource_id=str(datasource_id),
        repository="acme/widget",
        number=9,
        record_id=str(racing_authority["record_id"]),
        record_generation=racing_authority["record_generation"],
        head="feature/delivery",
        base="develop",
        head_revision=PR_REVISION,
        state="open",
    )
    async with db.acquire() as conn:
        await conn.execute(
            "DELETE FROM job_datasources WHERE job_id=$1 AND datasource_id=$2",
            racing_job_id,
            datasource_id,
        )
        with pytest.raises(asyncpg.CheckViolationError) as detached:
            await conn.execute(
                "UPDATE jobs SET status='completed' WHERE id=$1", racing_job_id
            )
        assert detached.value.constraint_name == "pr_deliverable_requires_live_proof"


@pytest.mark.asyncio
async def test_repo_open_pr_writer_requires_exact_writable_attachment(db, pg_dsn):
    _project_id, datasource_id, job_id, _revision = await _seed_repository_job(db)
    writer = AgentPostgresDB(pg_dsn, min_connections=1, max_connections=2)
    await writer.connect()
    try:
        valid = {
            "forge": "github",
            "repo": "ACME/Widget",
            "number": 9,
            "url": "https://github.com/acme/widget/pull/9",
            "head": " feature/delivery ",
            "base": "develop",
            "ignored_private_field": "must-not-persist",
        }
        assert await writer.jobs.record_pull_request(
            job_id, datasource_id, valid, source_revision=PR_REVISION
        )
        stored = await db.get_job(str(job_id))
        context = stored["context"]
        context = json.loads(context) if isinstance(context, str) else context
        record = context["pull_request"]
        assert record == {
            "forge": "github",
            "repo": "acme/widget",
            "number": 9,
            "url": "https://github.com/acme/widget/pull/9",
            "head": "feature/delivery",
            "base": "develop",
        }

        assert not await writer.jobs.record_pull_request(
            job_id,
            datasource_id,
            {**valid, "repo": "victim/private"},
            source_revision=PR_REVISION,
        )
        assert not await writer.jobs.record_pull_request(
            job_id,
            datasource_id,
            {
                **valid,
                "url": "https://secret@example.invalid/acme/widget/pull/9",
            },
            source_revision=PR_REVISION,
        )
        async with db.acquire() as conn:
            await conn.execute(
                "UPDATE datasources SET read_only=TRUE WHERE id=$1",
                datasource_id,
            )
        assert not await writer.jobs.record_pull_request(
            job_id, datasource_id, valid, source_revision=PR_REVISION
        )
    finally:
        await writer.close()


@pytest.mark.asyncio
async def test_repo_open_pr_writer_refuses_a_public_repository_with_no_mode(db, pg_dsn):
    """Decision 32: a public connector whose creator never chose a mode is
    read-only, so it records no PR; one published read-write does."""
    _project_id, datasource_id, job_id, _revision = await _seed_repository_job(db)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE datasources SET is_global=TRUE, read_only=NULL WHERE id=$1",
            datasource_id,
        )
    writer = AgentPostgresDB(pg_dsn, min_connections=1, max_connections=2)
    await writer.connect()
    try:
        record = {
            "forge": "github",
            "repo": "acme/widget",
            "number": 9,
            "url": "https://github.com/acme/widget/pull/9",
            "head": "feature/delivery",
            "base": "develop",
        }
        assert not await writer.jobs.record_pull_request(
            job_id, datasource_id, record, source_revision=PR_REVISION
        )
        async with db.acquire() as conn:
            await conn.execute(
                "UPDATE datasources SET read_only=FALSE WHERE id=$1", datasource_id
            )
        assert await writer.jobs.record_pull_request(
            job_id, datasource_id, record, source_revision=PR_REVISION
        )
    finally:
        await writer.close()


@pytest.mark.asyncio
async def test_gitea_writer_accepts_only_configured_public_internal_host_pair(
    db, pg_dsn, monkeypatch
):
    monkeypatch.setenv("GITEA_INTERNAL_URL", "http://srw-gitea:3000")
    monkeypatch.setenv("GITEA_URL", "https://git.srw.works")
    _project_id, datasource_id, job_id, _revision = await _seed_repository_job(
        db,
        forge="gitea",
        connection_url="http://srw-gitea:3000/acme/widget.git",
    )
    record = {
        "forge": "gitea",
        "repo": "acme/widget",
        "number": 9,
        "url": "https://git.srw.works/acme/widget/pulls/9",
        "head": "feature/delivery",
        "base": "develop",
    }
    writer = AgentPostgresDB(pg_dsn, min_connections=1, max_connections=2)
    await writer.connect()
    try:
        assert not await writer.jobs.record_pull_request(
            job_id,
            datasource_id,
            {**record, "url": "https://attacker.example/acme/widget/pulls/9"},
            source_revision=PR_REVISION,
        )
        assert await writer.jobs.record_pull_request(
            job_id,
            datasource_id,
            record,
            source_revision=PR_REVISION,
        )
    finally:
        await writer.close()

    authority = await db.get_job_pull_request_authority(str(job_id))
    assert authority is not None
    assert authority["datasource_id"] == datasource_id
    assert authority["forge"] == "gitea"
    assert authority["url"] == record["url"]


@pytest.mark.asyncio
async def test_pr_record_retry_is_idempotent_and_replacement_invalidates_proof(
    db, pg_dsn
):
    _project_id, datasource_id, job_id, _revision = await _seed_repository_job(db)
    first = {
        "forge": "github",
        "repo": "acme/widget",
        "number": 9,
        "url": "https://github.com/acme/widget/pull/9",
        "head": "feature/delivery",
        "base": "develop",
    }
    writer = AgentPostgresDB(pg_dsn, min_connections=1, max_connections=2)
    await writer.connect()
    try:
        assert await writer.jobs.record_pull_request(
            job_id, datasource_id, first, source_revision=PR_REVISION
        )
        original = await db.get_job_pull_request_authority(str(job_id))
        assert await db.mark_job_pr_deliverable_verified(
            str(job_id),
            datasource_id=str(datasource_id),
            repository="acme/widget",
            number=9,
            record_id=str(original["record_id"]),
            record_generation=original["record_generation"],
            head="feature/delivery",
            base="develop",
            head_revision=PR_REVISION,
            state="open",
        )

        # Simulated committed write with a lost response: an exact retry
        # returns success without minting a new generation or losing proof.
        assert await writer.jobs.record_pull_request(
            job_id, datasource_id, first, source_revision=PR_REVISION
        )
        replayed = await db.get_job_pull_request_authority(str(job_id))
        assert replayed["record_id"] == original["record_id"]
        assert replayed["record_generation"] == original["record_generation"]
        assert replayed["verified_at"] is not None

        replacement = {
            **first,
            "number": 10,
            "url": "https://github.com/acme/widget/pull/10",
        }
        assert await writer.jobs.record_pull_request(
            job_id, datasource_id, replacement, source_revision=PR_REVISION
        )
    finally:
        await writer.close()

    current = await db.get_job_pull_request_authority(str(job_id))
    assert current["record_id"] != original["record_id"]
    assert current["record_generation"] == original["record_generation"] + 1
    assert current["verified_at"] is None
    assert not await db.mark_job_pr_deliverable_verified(
        str(job_id),
        datasource_id=str(datasource_id),
        repository="acme/widget",
        number=9,
        record_id=str(original["record_id"]),
        record_generation=original["record_generation"],
        head="feature/delivery",
        base="develop",
        head_revision=PR_REVISION,
        state="open",
    )
    assert not await db.mark_job_pr_deliverable_verified(
        str(job_id),
        datasource_id=str(datasource_id),
        repository="acme/widget",
        number=10,
        record_id=str(current["record_id"]),
        record_generation=current["record_generation"],
        head="unrelated",
        base="develop",
        head_revision=PR_REVISION,
        state="open",
    )
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.CheckViolationError) as completion:
            await conn.execute("UPDATE jobs SET status='completed' WHERE id=$1", job_id)
        assert completion.value.constraint_name == "pr_deliverable_requires_live_proof"


@pytest.mark.asyncio
async def test_concurrent_pr_replacement_and_completion_have_one_truthful_winner(
    db, pg_dsn
):
    _project_id, datasource_id, job_id, _revision = await _seed_repository_job(db)
    first = {
        "forge": "github",
        "repo": "acme/widget",
        "number": 9,
        "url": "https://github.com/acme/widget/pull/9",
        "head": "feature/delivery",
        "base": "develop",
    }
    writer = AgentPostgresDB(pg_dsn, min_connections=1, max_connections=3)
    await writer.connect()
    try:
        assert await writer.jobs.record_pull_request(
            job_id, datasource_id, first, source_revision=PR_REVISION
        )
        authority = await db.get_job_pull_request_authority(str(job_id))
        assert await db.mark_job_pr_deliverable_verified(
            str(job_id),
            datasource_id=str(datasource_id),
            repository="acme/widget",
            number=9,
            record_id=str(authority["record_id"]),
            record_generation=authority["record_generation"],
            head="feature/delivery",
            base="develop",
            head_revision=PR_REVISION,
            state="open",
        )

        replacement = {
            **first,
            "number": 10,
            "url": "https://github.com/acme/widget/pull/10",
        }

        async def replace() -> bool:
            return await writer.jobs.record_pull_request(
                job_id,
                datasource_id,
                replacement,
                source_revision=PR_REVISION,
            )

        async def complete() -> bool:
            try:
                async with db.acquire() as conn:
                    await conn.execute(
                        "UPDATE jobs SET status='completed' WHERE id=$1", job_id
                    )
            except asyncpg.CheckViolationError as exc:
                assert exc.constraint_name == "pr_deliverable_requires_live_proof"
                return False
            return True

        replacement_won, completion_won = await asyncio.gather(replace(), complete())
    finally:
        await writer.close()

    assert replacement_won is not completion_won
    stored = await db.get_job(str(job_id))
    final_authority = await db.get_job_pull_request_authority(str(job_id))
    if completion_won:
        assert stored["status"] == "completed"
        assert final_authority["record_id"] == authority["record_id"]
        assert final_authority["verified_at"] is not None
    else:
        assert stored["status"] == "processing"
        assert final_authority["number"] == 10
        assert final_authority["verified_at"] is None


@pytest.mark.asyncio
async def test_blocked_undelivered_is_terminal_and_cannot_resume(db):
    job_id = uuid4()
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.CheckViolationError) as forged:
            await conn.execute(
                "INSERT INTO jobs (id, description, status, "
                "completion_outcome_kind) VALUES ($1, 'forged blocker', "
                "'cancelled', 'blocked_undelivered')",
                uuid4(),
            )
        assert forged.value.constraint_name == "completion_outcome_is_server_owned"

        await conn.execute(
            "INSERT INTO jobs (id, description, status) "
            "VALUES ($1, 'honest negative', 'processing')",
            job_id,
        )
        await conn.execute(
            "UPDATE jobs SET status='cancelled', "
            "completion_outcome_kind='blocked_undelivered' WHERE id=$1",
            job_id,
        )
        with pytest.raises(asyncpg.CheckViolationError) as resumed:
            await conn.execute("UPDATE jobs SET status='created' WHERE id=$1", job_id)
        assert resumed.value.constraint_name == "blocked_undelivered_is_terminal"


@pytest.mark.asyncio
async def test_deleted_blocker_retains_claim_but_is_absent_from_breaker_history(db):
    project_id, thread_id = await _seed_officer(db)
    preparation = await prepare_officer_admission(
        db,
        project_id=project_id,
        thread_id=thread_id,
        requested_slot="line",
    )
    generation = datetime(2026, 8, 24, 7, 0, tzinfo=timezone.utc)
    contract = {
        "deliverables": ["kb:honest-negative"],
        "pr_repositories": [],
        "pr_bindings": [],
        "digest": "honest-negative",
    }
    job = await admit_and_create_job(
        db,
        preparation=preparation,
        job_kwargs=_officer_job_kwargs(contract),
        ticket_note_id="blocked-ticket",
        ticket_ready_at=generation,
    )
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET status='cancelled', "
            "completion_outcome_kind='blocked_undelivered' WHERE id=$1",
            job["id"],
        )

    stats = await db.get_job_statistics(project_ids=[project_id])
    assert stats["blocked_undelivered"] == 1
    assert stats["completed"] == 0
    blocked_page = await db.query_jobs(
        project_ids=[project_id], statuses=["blocked_undelivered"]
    )
    cancelled_page = await db.query_jobs(
        project_ids=[project_id], statuses=["cancelled"]
    )
    assert [str(row["id"]) for row in blocked_page.jobs] == [str(job["id"])]
    assert cancelled_page.jobs == []
    daily = await db.get_daily_statistics(days=1, scope_project_id=project_id)
    assert len(daily) == 1
    assert daily[0]["jobs_completed"] == 0
    assert daily[0]["jobs_cancelled"] == 0
    assert daily[0]["jobs_blocked_undelivered"] == 1
    async with db.acquire() as conn:
        summary = await conn.fetchrow(
            "SELECT status, completion_outcome_kind FROM job_summary WHERE id=$1",
            job["id"],
        )
    assert dict(summary) == {
        "status": "cancelled",
        "completion_outcome_kind": "blocked_undelivered",
    }

    async with db.acquire() as conn:
        await conn.execute("DELETE FROM jobs WHERE id=$1", job["id"])
        tombstone = await conn.fetchrow(
            "SELECT job_status_at_delete, completion_outcome_kind_at_delete "
            "FROM officer_ticket_claims WHERE job_id=$1",
            job["id"],
        )
    assert dict(tombstone) == {
        "job_status_at_delete": "cancelled",
        "completion_outcome_kind_at_delete": "blocked_undelivered",
    }
    assert (
        await db.list_officer_distinct_terminal_outcomes(
            [thread_id], slot="line", limit=2
        )
        == []
    )
    with pytest.raises(OfficerAdmissionConflict) as replay:
        await admit_and_create_job(
            db,
            preparation=preparation,
            job_kwargs=_officer_job_kwargs(contract),
            ticket_note_id="blocked-ticket",
            ticket_ready_at=generation,
        )
    assert replay.value.code == "ticket_claimed"

    retried = await admit_and_create_job(
        db,
        preparation=preparation,
        job_kwargs=_officer_job_kwargs(contract),
        ticket_note_id="blocked-ticket",
        ticket_ready_at=generation + timedelta(seconds=1),
    )
    assert retried["status"] == "created"
    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM officer_ticket_claims "
                "WHERE project_id=$1 AND ticket_note_id='blocked-ticket'",
                UUID(project_id),
            )
            == 2
        )


async def _seed_officer(db: PostgresDB) -> tuple[str, str]:
    project_id = uuid4()
    thread_id = uuid4()
    officer = {
        "enabled": True,
        "auto_pull": False,
        "slots": {
            "line": {
                "count": 2,
                "category": "executor",
                "model": "MiniMax-M3",
                "backend": "sandbox",
            }
        },
    }
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO projects (id, name) VALUES ($1, 'Officer project')",
            project_id,
        )
        await conn.execute(
            """
            INSERT INTO threads (id, project_id, status, metadata)
            VALUES ($1, $2, 'active', $3::jsonb)
            """,
            thread_id,
            project_id,
            json.dumps({"config_override": {"officer": officer}}),
        )
        await conn.execute(
            """
            INSERT INTO project_officers (
                project_id, thread_id, config_override, incarnations
            ) VALUES ($1, $2, $3::jsonb, '[]'::jsonb)
            """,
            project_id,
            thread_id,
            json.dumps({"officer": {"slots": officer["slots"]}}),
        )
    return str(project_id), str(thread_id)


async def _await_post_lock_waiters(observer: Any, expected: int) -> None:
    """Block until ``expected`` backends are queued on a heavyweight lock.

    Postgres hands the contended tuple to waiters in arrival order, so staging
    a winner means proving the first contender is *already queued* before the
    second one starts.  A fixed sleep cannot prove that: each contender must
    first open a fresh pool connection (the pool starts at ``min_size=1`` and
    the blocker holds that one warm connection), and a slow handshake on the
    first contender silently hands the lock to the second.

    Must run outside a transaction — ``pg_stat_activity`` is served from a
    per-transaction snapshot, so polling inside one returns the same stale
    answer forever.
    """

    deadline = time.monotonic() + 30.0
    while True:
        waiting = await observer.fetchval(
            """
            SELECT count(*)
              FROM pg_stat_activity
             WHERE datname = current_database()
               AND wait_event_type = 'Lock'
               AND pid <> pg_backend_pid()
            """
        )
        if waiting >= expected:
            return
        if time.monotonic() >= deadline:
            raise AssertionError(
                f"timed out waiting for {expected} lock waiter(s); saw {waiting}"
            )
        await asyncio.sleep(0.01)


def _officer_job_kwargs(contract: dict) -> dict:
    deliverables = list(contract.get("deliverables") or [])
    return {
        "description": "publication attempt",
        "config_name": "worker_base",
        "context": {"required_deliverables": deliverables},
        "config_override": {},
        "delivery_contract": contract,
        "datasource_ids": [],
        "datasource_selection_provenance": {},
        "datasource_policy_revisions": {},
        "execution_lane": "pinned",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "rejected_path",
    [
        "repos/Widget/docs/demo.html",
        "./repos/Widget/docs/demo.html",
        "/repos/Widget/docs/demo.html",
        "  ./repos/Widget/docs/demo.html  ",
    ],
)
async def test_same_generation_normalized_rejection_then_kb_rewrite_creates_nothing(
    db, rejected_path
):
    project_id, thread_id = await _seed_officer(db)
    preparation = await prepare_officer_admission(
        db,
        project_id=project_id,
        thread_id=thread_id,
        requested_slot="line",
        requested_config_override=None,
    )
    generation = datetime(2026, 8, 24, 8, 0, tzinfo=timezone.utc)
    datasource = {
        "id": str(uuid4()),
        "type": "repository",
        "connection_url": "https://github.com/acme/widget",
        "config": {"forge": "github"},
        "read_only": False,
        "project_read_only": False,
        "policy_revision": 1,
    }
    with pytest.raises(DeliveryContractConflict) as rejected:
        prepare_delivery_contract([rejected_path], datasources=[datasource])
    assert rejected.value.code == "external_repository_requires_pr"
    recorded = await record_rejected_ticket_delivery_requirement(
        db,
        preparation=preparation,
        ticket_note_id="reception-demo",
        ticket_ready_at=generation,
        required_pr_repositories=rejected.value.fields["required_pr_repositories"],
    )
    assert recorded["recorded"] is True

    kb_contract = {
        "deliverables": ["kb:reception-cockpit-demo-publication-report"],
        "pr_repositories": [],
        "pr_bindings": [],
        "digest": "kb-downgrade",
    }
    with pytest.raises(OfficerAdmissionConflict) as refusal:
        await admit_and_create_job(
            db,
            preparation=preparation,
            job_kwargs=_officer_job_kwargs(kb_contract),
            ticket_note_id="reception-demo",
            ticket_ready_at=generation,
            ticket_claim_source="manual",
        )
    assert refusal.value.code == "deliverable_contract_downgrade"
    async with db.acquire() as conn:
        assert await conn.fetchval("SELECT count(*) FROM jobs") == 0
        assert await conn.fetchval("SELECT count(*) FROM officer_ticket_claims") == 0

    # A later explicit re-ready generation establishes a fresh contract.
    accepted = await admit_and_create_job(
        db,
        preparation=preparation,
        job_kwargs=_officer_job_kwargs(kb_contract),
        ticket_note_id="reception-demo",
        ticket_ready_at=generation + timedelta(seconds=1),
        ticket_claim_source="manual",
    )
    assert accepted["status"] == "created"


@pytest.mark.asyncio
async def test_normalized_rejection_and_rewrite_serialize_on_the_post_lock(db):
    project_id, thread_id = await _seed_officer(db)
    preparation = await prepare_officer_admission(
        db,
        project_id=project_id,
        thread_id=thread_id,
        requested_slot="line",
    )
    generation = datetime(2026, 8, 24, 9, 0, tzinfo=timezone.utc)
    kb_contract = {
        "deliverables": ["kb:failure-note"],
        "pr_repositories": [],
        "pr_bindings": [],
        "digest": "kb-race",
    }
    with pytest.raises(DeliveryContractConflict) as rejected:
        prepare_delivery_contract(
            ["./repos/Widget/docs/demo.html"],
            datasources=[
                {
                    "id": str(uuid4()),
                    "type": "repository",
                    "connection_url": "https://github.com/acme/widget",
                    "config": {"forge": "github"},
                    "read_only": False,
                    "project_read_only": False,
                    "policy_revision": 1,
                }
            ],
        )

    # The observer is acquired before the blocker so that polling never has to
    # open a connection while the queue is being measured.
    async with db.acquire() as observer, db.acquire() as blocker:
        transaction = blocker.transaction()
        await transaction.start()
        await blocker.fetchval(
            "SELECT project_id FROM project_officers WHERE project_id=$1 FOR UPDATE",
            UUID(project_id),
        )
        record_task = asyncio.create_task(
            record_rejected_ticket_delivery_requirement(
                db,
                preparation=preparation,
                ticket_note_id="race-ticket",
                ticket_ready_at=generation,
                required_pr_repositories=rejected.value.fields[
                    "required_pr_repositories"
                ],
            )
        )
        await _await_post_lock_waiters(observer, 1)
        rewrite_task = asyncio.create_task(
            admit_and_create_job(
                db,
                preparation=preparation,
                job_kwargs=_officer_job_kwargs(kb_contract),
                ticket_note_id="race-ticket",
                ticket_ready_at=generation,
                ticket_claim_source="manual",
            )
        )
        await _await_post_lock_waiters(observer, 2)
        await transaction.commit()

    assert (await record_task)["recorded"] is True
    with pytest.raises(OfficerAdmissionConflict) as refusal:
        await rewrite_task
    assert refusal.value.code == "deliverable_contract_downgrade"
    async with db.acquire() as conn:
        assert await conn.fetchval("SELECT count(*) FROM jobs") == 0
        assert await conn.fetchval("SELECT count(*) FROM officer_ticket_claims") == 0
