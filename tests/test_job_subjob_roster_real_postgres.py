"""The subjob roster and the unfiltered subjob count, against real PostgreSQL.

Both behaviours under test live entirely in SQL, so mocking asyncpg would only
assert the mock. What is actually at stake:

* **The roster walks the tree, the list walks the filter.** ``query_jobs`` rides
  a child along only when the child is in the *matched* set, and the jobs list's
  default filter is ``origin IN ('user','session')`` while every subjob is
  stamped ``origin='subjob'``. On k3d that excluded all 33 children from every
  page — a parent parked in ``waiting`` (which means *blocked on a child*)
  rendered as a stalled row with no children at all. ``get_job_subjob_roster``
  must be immune to that by construction, and ``subjob_count`` must report the
  real tree while the rendered child rows keep reporting the filtered one. Those
  two numbers disagreeing is the signal, not a bug.

* **Recursion has to terminate.** ``jobs.parent_job_id`` has no cycle
  constraint, so both walks are depth-capped and dedupe by id. A cycle is not
  hypothetical paranoia here — it is a plain ``UPDATE`` away, and an uncapped
  recursive CTE spins until the connection dies.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import asyncpg
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

from orchestrator.database.postgres import PostgresDB
from orchestrator.security import crypto
from orchestrator.application import http as http_composition
from orchestrator.application import jobs as jobs_composition
from orchestrator.security import auth as auth_module
import functools

SCHEMA_FILE = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "orchestrator"
    / "database"
    / "schema_current.sql"
)

HUMAN = ["user", "session"]


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
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def db(pg_dsn, _schema_applied, monkeypatch):
    monkeypatch.setenv("EXPERTS_DB_ENABLED", "false")
    monkeypatch.setenv("APP_ENCRYPTION_KEY", "P" * 32)
    crypto.reset_cipher_cache()
    store = PostgresDB(connection_string=pg_dsn, min_connections=1, max_connections=5)
    await store.connect()
    async with store.acquire() as conn:
        await conn.execute("TRUNCATE jobs CASCADE")
    try:
        yield store
    finally:
        await store.close()


async def _job(
    db,
    *,
    description: str,
    origin: str = "user",
    parent: str | None = None,
    status: str = "completed",
    config_name: str | None = None,
    created_at: datetime | None = None,
) -> str:
    job_id = uuid.uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO jobs (id, description, status, origin, parent_job_id,
                              config_name, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, COALESCE($7, NOW()))
            """,
            job_id,
            description,
            status,
            origin,
            uuid.UUID(parent) if parent else None,
            config_name,
            created_at,
        )
    return str(job_id)


class TestRosterIgnoresTheFilter:
    @pytest.mark.asyncio
    async def test_a_subjob_the_list_hides_is_still_in_the_roster(self, db):
        """The whole point, in one assertion.

        The parent is `waiting` and its only child is `origin='subjob'`, which
        is exactly the shape that renders as a childless parked row in the list.
        """
        parent = await _job(db, description="build a calculator", status="waiting")
        child = await _job(
            db,
            description="Research phase for: build a calculator",
            origin="subjob",
            parent=parent,
            status="processing",
            config_name="scholar",
        )

        rows = await db.query_jobs(origins=HUMAN, limit=25, offset=0)
        rendered = [str(r["id"]) for r in rows.jobs]
        assert rendered == [parent], "the child must be filtered out of the list"

        roster = await db.get_job_subjob_roster(parent)
        assert [str(r["id"]) for r in roster] == [child]
        assert roster[0]["status"] == "processing"
        assert roster[0]["config_name"] == "scholar"
        assert roster[0]["depth"] == 0

    @pytest.mark.asyncio
    async def test_the_count_reports_the_tree_while_the_rows_report_the_filter(
        self, db
    ):
        """The two numbers are allowed to disagree — that gap IS the filter."""
        parent = await _job(db, description="parent", status="waiting")
        await _job(db, description="scholar", origin="subjob", parent=parent)
        await _job(db, description="critic", origin="subjob", parent=parent)

        rows = await db.query_jobs(origins=HUMAN, limit=25, offset=0)
        root = next(r for r in rows.jobs if str(r["id"]) == parent)
        rendered_children = [r for r in rows.jobs if not r["is_display_root"]]

        assert root["subjob_count"] == 2
        assert rendered_children == []

    @pytest.mark.asyncio
    async def test_a_genuinely_childless_job_counts_zero(self, db):
        """Zero must stay distinguishable from hidden — it is the other half."""
        job = await _job(db, description="a leaf")
        rows = await db.query_jobs(origins=HUMAN, limit=25, offset=0)
        root = next(r for r in rows.jobs if str(r["id"]) == job)
        assert root["subjob_count"] == 0
        assert await db.get_job_subjob_roster(job) == []

    @pytest.mark.asyncio
    async def test_an_unfiltered_child_row_carries_its_own_count(self, db):
        """A child riding along must not inherit its parent's number.

        The count CTE is seeded from every row the query returns, not just the
        display roots, precisely so a nested row does not silently read 0 (or,
        worse, its parent's total).
        """
        parent = await _job(db, description="parent")
        child = await _job(
            db, description="child", origin="subjob", parent=parent, status="completed"
        )
        await _job(db, description="grandchild", origin="subjob", parent=child)

        rows = await db.query_jobs(origins=[*HUMAN, "subjob"], limit=25, offset=0)
        by_id = {str(r["id"]): r for r in rows.jobs}
        assert by_id[parent]["subjob_count"] == 2  # child + grandchild
        assert by_id[child]["subjob_count"] == 1  # grandchild only


class TestRosterShape:
    @pytest.mark.asyncio
    async def test_depth_increases_with_generation(self, db):
        parent = await _job(db, description="parent")
        child = await _job(db, description="child", origin="subjob", parent=parent)
        grandchild = await _job(
            db, description="grandchild", origin="subjob", parent=child
        )

        roster = await db.get_job_subjob_roster(parent)
        depths = {str(r["id"]): r["depth"] for r in roster}
        assert depths == {child: 0, grandchild: 1}

    @pytest.mark.asyncio
    async def test_ordered_by_depth_then_creation(self, db):
        parent = await _job(db, description="parent")
        first = await _job(db, description="first", origin="subjob", parent=parent)
        second = await _job(db, description="second", origin="subjob", parent=parent)
        deep = await _job(db, description="deep", origin="subjob", parent=first)

        roster = await db.get_job_subjob_roster(parent)
        assert [str(r["id"]) for r in roster] == [first, second, deep]

    @pytest.mark.asyncio
    async def test_terminal_children_are_included(self, db):
        """Unlike `get_descendant_jobs`, whose callers are cancelling live work."""
        parent = await _job(db, description="parent")
        done = await _job(
            db, description="done", origin="subjob", parent=parent, status="completed"
        )
        failed = await _job(
            db, description="failed", origin="subjob", parent=parent, status="failed"
        )
        roster = await db.get_job_subjob_roster(parent)
        assert {str(r["id"]) for r in roster} == {done, failed}

    @pytest.mark.asyncio
    async def test_a_malformed_id_is_an_empty_roster_not_an_exception(self, db):
        assert await db.get_job_subjob_roster("not-a-uuid") == []

    @pytest.mark.asyncio
    async def test_an_unknown_job_is_an_empty_roster(self, db):
        assert await db.get_job_subjob_roster(str(uuid.uuid4())) == []


@pytest.mark.asyncio
async def test_recovery_owner_evidence_is_consistent_in_list_and_detail_sql(db):
    owner = await _job(db, description="recovery owner", status="paused")
    child = await _job(
        db,
        description="shared recovery child",
        origin="subjob",
        parent=owner,
        status="paused",
    )
    async with db.acquire() as conn:
        recovery_id = await conn.fetchval(
            """
            INSERT INTO vm_workspace_recoveries (
                owner_kind, owner_id, workspace_contract_digest, cluster_name,
                phase, reason_code
            ) VALUES (
                'job', $1, 'sha256:list-detail-projection', 'test-cluster',
                'paused_attention', 'prior_runtime_unfenced'
            ) RETURNING id
            """,
            uuid.UUID(owner),
        )
        await conn.executemany(
            """
            INSERT INTO vm_workspace_recovery_jobs (
                recovery_id, job_id, prior_queue_state, prior_job_status,
                participation
            ) VALUES ($1, $2, 'non_worker', 'paused', 'attention')
            """,
            [
                (recovery_id, uuid.UUID(owner)),
                (recovery_id, uuid.UUID(child)),
            ],
        )

    listed = await db.query_jobs(origins=[*HUMAN, "subjob"], limit=25, offset=0)

    def recovery_payload(value):
        return json.loads(value) if isinstance(value, str) else value

    list_recovery = {
        str(row["id"]): recovery_payload(row["_workspace_recovery"])
        for row in listed.jobs
    }
    owner_detail = await db.get_job(owner)
    child_detail = await db.get_job(child)

    assert list_recovery[owner]["canonical_owner"] is True
    assert list_recovery[child]["canonical_owner"] is False
    assert owner_detail is not None
    assert child_detail is not None
    assert (
        recovery_payload(owner_detail["_workspace_recovery"])["canonical_owner"] is True
    )
    assert (
        recovery_payload(child_detail["_workspace_recovery"])["canonical_owner"]
        is False
    )


class TestCyclesTerminate:
    @pytest.mark.asyncio
    async def test_a_parent_cycle_does_not_spin_and_yields_each_job_once(self, db):
        """`jobs.parent_job_id` has no cycle constraint — one UPDATE away.

        Both the depth cap and the dedupe are load-bearing: without the cap the
        recursion never ends, and without the dedupe the same job comes back
        once per lap.
        """
        a = await _job(db, description="a")
        b = await _job(db, description="b", origin="subjob", parent=a)
        async with db.acquire() as conn:
            await conn.execute(
                "UPDATE jobs SET parent_job_id = $1 WHERE id = $2",
                uuid.UUID(b),
                uuid.UUID(a),
            )

        roster = await db.get_job_subjob_roster(a, max_depth=5)
        ids = [str(r["id"]) for r in roster]
        assert sorted(ids) == sorted({*ids}), "each job must appear exactly once"
        assert set(ids) == {a, b}

    @pytest.mark.asyncio
    async def test_the_list_count_survives_a_cycle_too(self, db):
        a = await _job(db, description="a")
        b = await _job(db, description="b", origin="subjob", parent=a)
        async with db.acquire() as conn:
            await conn.execute(
                "UPDATE jobs SET parent_job_id = $1 WHERE id = $2",
                uuid.UUID(b),
                uuid.UUID(a),
            )
        rows = await db.query_jobs(origins=HUMAN, limit=25, offset=0)

        # `a` is still a display root: its parent `b` does not match the human
        # origin filter, which is the same flattening rule that lets an
        # `origin=subjob` search return children as top-level rows.
        assert [str(r["id"]) for r in rows.jobs] == [a]

        # Reaching this assertion at all is the proof the count CTE terminates.
        # The number is 2 because the cycle makes `a` its own descendant — odd,
        # but the honest answer for a cyclic graph, and bounded rather than
        # infinite. Asserting DISTINCT is what matters: without it the same two
        # jobs would be counted once per lap, up to the depth cap.
        assert rows.jobs[0]["subjob_count"] == 2


@pytest_asyncio.fixture
async def list_client(db, monkeypatch):
    """Mounted public list route with real SQL and no background application."""
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    import httpx
    from fastapi import FastAPI
    from orchestrator import main
    from orchestrator.routers.job_reads import router as job_reads_router

    monkeypatch.setattr(main.app.state.resources, "postgres_db", db)
    monkeypatch.setattr(
        auth_module,
        "require_approved_user",
        AsyncMock(return_value={"id": str(uuid.uuid4()), "is_admin": True}),
    )
    monkeypatch.setattr(
        main.app.state.resources, "audit_reader", SimpleNamespace(is_available=False)
    )
    app = FastAPI(default_response_class=http_composition.CustomJSONResponse)
    app.state.job_reads_dependencies_factory = functools.partial(
        jobs_composition.job_reads_dependencies, main.app.state.resources
    )
    app.include_router(job_reads_router)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://list.test"
    ) as client:
        yield client


class TestListWirePagination:
    @pytest.mark.asyncio
    async def test_tied_roots_keep_children_and_watermark_excludes_new_insert(
        self, db, list_client
    ):
        from orchestrator.schemas.job_list import PublicJobListPage

        stamp = datetime(2026, 9, 6, 8, tzinfo=timezone.utc)
        a = await _job(db, description="a", created_at=stamp)
        b = await _job(db, description="b", created_at=stamp)
        first_root, second_root = sorted([a, b], reverse=True)
        child = await _job(
            db,
            description="child",
            parent=first_root,
            origin="subjob",
            created_at=stamp,
        )
        # Real SQL must discard its private workspace join columns before the
        # public model sees them. Synthetic data only; no provider/workspace.
        async with db.acquire() as conn:
            await conn.execute(
                """UPDATE jobs SET context = '{"vm":{"ssh_host":"synthetic-private"}}',
                   config_override = '{"llm":{"api_key":"synthetic-private"}}'
                   WHERE id = $1""",
                uuid.UUID(first_root),
            )
        query = [
            ("origin", "user"),
            ("origin", "subjob"),
            ("status", "completed"),
            ("limit", "1"),
        ]
        first = await list_client.get("/api/jobs", params=query)
        assert first.status_code == 200, first.text
        page = first.json()
        PublicJobListPage.model_validate_json(first.text, strict=True)
        assert [j["id"] for j in page["jobs"]] == [first_root, child]
        assert page["limit"] == 1 and page["total"] == 2 and page["has_more"] is True
        assert page["jobs"][0]["config_name"] is None
        for private in (
            "synthetic-private",
            "_workspace_context",
            "_workspace_config_override",
            "project_has_cloud_folder",
        ):
            assert private not in first.text

        watermark = datetime.fromisoformat(page["as_of"])
        await _job(
            db, description="newer insert", created_at=watermark + timedelta(seconds=1)
        )
        frozen = query + [("as_of", page["as_of"]), ("include_total", "false")]
        second = await list_client.get("/api/jobs", params=frozen + [("offset", "1")])
        assert second.status_code == 200, second.text
        next_page = second.json()
        PublicJobListPage.model_validate_json(second.text, strict=True)
        assert [j["id"] for j in next_page["jobs"]] == [second_root]
        assert next_page["total"] is None and next_page["total_is_capped"] is False
        assert next_page["has_more"] is False and next_page["as_of"] == page["as_of"]
        repeated = await list_client.get("/api/jobs", params=frozen)
        assert [j["id"] for j in repeated.json()["jobs"]] == [first_root, child]
        empty = await list_client.get("/api/jobs", params=frozen + [("offset", "2")])
        assert empty.json()["jobs"] == [] and empty.json()["has_more"] is False

    @pytest.mark.asyncio
    async def test_real_capped_count_is_independent_of_has_more(
        self, db, list_client, monkeypatch
    ):
        monkeypatch.setattr("orchestrator.database.postgres.JOB_COUNT_CAP", 1)
        for name in ("a", "b", "c"):
            await _job(db, description=name)
        first = await list_client.get("/api/jobs", params={"limit": 2})
        assert first.status_code == 200, first.text
        assert first.json()["total"] == 1 and first.json()["total_is_capped"] is True
        assert first.json()["has_more"] is True
        last = await list_client.get(
            "/api/jobs",
            params={"limit": 2, "offset": 2, "as_of": first.json()["as_of"]},
        )
        assert last.status_code == 200, last.text
        assert last.json()["total"] == 1 and last.json()["total_is_capped"] is True
        assert last.json()["has_more"] is False
