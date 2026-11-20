"""Real-Postgres parity tests for AuditStore (PR3).

Seeds rows via SyncAuditWriter, reads them back through AuditStore, and asserts
the load-bearing translations: the pre/post logical-row stitch, global
step_number preserved under FilterCategory, the single-query version, and
list_llm_requests surfacing token_usage + the status/call_type filters.

Skips cleanly without testcontainers[postgres] or a container runtime.
"""

from __future__ import annotations

import asyncio
import re
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import asyncpg
import pytest

pytest.importorskip("testcontainers.postgres")

from orchestrator.database.audit_store import AuditStore  # noqa: E402
from orchestrator.database.migrate import run_migrations  # noqa: E402
from orchestrator.database.postgres import MIGRATIONS_AUDIT_DIR  # noqa: E402
from agent.database.audit_writer import SyncAuditWriter  # noqa: E402

pytestmark = pytest.mark.asyncio
AUDIT_IMAGE = "postgres:16"


def _swap_db(dsn: str, dbname: str) -> str:
    head, _, tail = dsn.rpartition("/")
    query = "?" + tail.split("?", 1)[1] if "?" in tail else ""
    return f"{head}/{dbname}{query}"


@pytest.fixture(scope="module")
def pg_dsn():
    from testcontainers.postgres import PostgresContainer

    container = PostgresContainer(AUDIT_IMAGE)
    try:
        container.start()
    except Exception as exc:
        pytest.skip(f"no container runtime for testcontainers: {exc}")
    try:
        yield re.sub(
            r"^postgresql\+\w+://", "postgresql://", container.get_connection_url()
        )
    finally:
        container.stop()


def _seed(dsn: str) -> dict:
    """Seed a known fixture set via the real writer. Returns ids/job."""
    now = datetime.now(timezone.utc)
    iso = now.isoformat().replace("+00:00", "Z")
    job = str(uuid4())
    w = SyncAuditWriter(dsn)
    rid_main = w.insert_llm_request(
        {
            "job_id": job,
            "agent_type": "universal",
            "call_type": "main",
            "model": "gpt",
            "iteration": 2,
            "timestamp": now,
            "latency_ms": 11,
            "request": {"messages": [], "message_count": 0},
            "response": {"content": "yo", "tool_calls": [{"name": "read_file"}]},
            "metadata": None,
            "auxiliary_metadata": None,
            "metrics": {"token_usage": {"total_tokens": 9}},
        }
    )
    w.insert_llm_request(
        {
            "job_id": job,
            "agent_type": "memory",
            "call_type": "auxiliary",
            "model": "aux",
            "iteration": None,
            "timestamp": now,
            "latency_ms": 1,
            "request": {"messages": [], "message_count": 0},
            "response": {},
            "metadata": {
                "status": "error",
                "error": {"type": "TimeoutError", "message": "boom"},
            },
            "auxiliary_metadata": None,
            "metrics": {"token_usage": {}},
        }
    )
    # codex/Responses-API shape: token_usage empty, counts only in the
    # LangChain-normalized usage_metadata (job c3fc9128 regression).
    w.insert_llm_request(
        {
            "job_id": job,
            "agent_type": "universal",
            "call_type": "codex",
            "model": "gpt-5.6-sol",
            "iteration": 3,
            "timestamp": now,
            "latency_ms": 5,
            "request": {"messages": [], "message_count": 0},
            "response": {"content": "hi", "tool_calls": []},
            "metadata": None,
            "auxiliary_metadata": None,
            "metrics": {
                "token_usage": {},
                "usage_metadata": {
                    "input_tokens": 15869,
                    "output_tokens": 461,
                    "total_tokens": 16330,
                    "input_token_details": {"cache_read": 0},
                    "output_token_details": {"reasoning": 89},
                },
            },
        }
    )
    # tool pre/post (stitch target)
    tpre = w.insert_audit_pre(
        {
            "job_id": job,
            "agent_type": "u",
            "iteration": 2,
            "step_type": "tool",
            "node_name": "tools",
            "phase": "tactical",
            "phase_number": 2,
            "timestamp": now,
            "latency_ms": None,
            "payload": {
                "tool": {"name": "read_file", "arguments": {"path": "/x"}},
                "started_at": iso,
            },
            "metadata": None,
        }
    )
    w.insert_audit_post(
        tpre,
        {"tool": {"result_preview": "done", "success": True}, "completed_at": iso},
        40,
        None,
    )
    # llm pre (no post)
    w.insert_audit_pre(
        {
            "job_id": job,
            "agent_type": "u",
            "iteration": 2,
            "step_type": "llm",
            "node_name": "execute",
            "phase": None,
            "phase_number": None,
            "timestamp": now,
            "latency_ms": None,
            "payload": {"llm": {"model": "gpt"}},
            "metadata": None,
        }
    )
    # error step
    w.insert_audit_pre(
        {
            "job_id": job,
            "agent_type": "u",
            "iteration": 2,
            "step_type": "error",
            "node_name": "execute",
            "phase": None,
            "phase_number": None,
            "timestamp": now,
            "latency_ms": None,
            "payload": {"error": {"message": "x"}},
            "metadata": None,
        }
    )
    w.close()
    return {"job": job, "rid_main": rid_main}


@pytest.fixture
def seeded(pg_dsn):
    dbname = f"audit_r_{uuid4().hex[:12]}"

    async def setup():
        admin = await asyncpg.connect(pg_dsn)
        try:
            await admin.execute(f'CREATE DATABASE "{dbname}"')
        finally:
            await admin.close()
        pool = await asyncpg.create_pool(
            _swap_db(pg_dsn, dbname), min_size=1, max_size=2
        )
        try:
            await run_migrations(pool, MIGRATIONS_AUDIT_DIR)
        finally:
            await pool.close()

    async def teardown():
        admin = await asyncpg.connect(pg_dsn)
        try:
            await admin.execute(f'DROP DATABASE IF EXISTS "{dbname}" WITH (FORCE)')
        finally:
            await admin.close()

    asyncio.run(setup())
    dsn = _swap_db(pg_dsn, dbname)
    info = _seed(dsn)
    info["dsn"] = dsn
    yield info
    asyncio.run(teardown())


async def _store(dsn) -> AuditStore:
    s = AuditStore(dsn)
    await s.connect()
    assert s.is_available
    return s


async def test_logical_row_stitch(seeded):
    s = await _store(seeded["dsn"])
    try:
        res = await s.get_job_audit(seeded["job"])
        assert res["total"] == 3  # 3 pre rows (tool, llm, error); post is not counted
        tool = next(
            e for e in res["entries"] if e.get("tool", {}).get("name") == "read_file"
        )
        # pre fields + post delta merged into one logical row
        assert tool["tool"]["name"] == "read_file"
        assert tool["tool"]["result_preview"] == "done"
        assert tool["tool"]["success"] is True
        assert tool["latency_ms"] == 40  # coalesced from post
        assert tool["started_at"].endswith("Z")
    finally:
        await s.disconnect()


async def test_strict_audit_counts_report_authoritative_pre_rows(seeded):
    s = await _store(seeded["dsn"])
    try:
        missing = str(uuid4())
        counts = await s.get_audit_counts_strict([seeded["job"], missing])
        assert counts == {seeded["job"]: 3, missing: 0}
    finally:
        await s.disconnect()


async def test_lean_projection_drops_metadata_and_heavy_subkeys(seeded):
    # Seed fat rows: boot metadata + heavy tool `arguments`/`state`, and a long
    # error traceback. The lean projection must strip all of these.
    now = datetime.now(timezone.utc)
    iso = now.isoformat().replace("+00:00", "Z")
    w = SyncAuditWriter(seeded["dsn"])
    w.insert_audit_pre(
        {
            "job_id": seeded["job"],
            "agent_type": "u",
            "iteration": 9,
            "step_type": "tool",
            "node_name": "tools",
            "phase": "tactical",
            "phase_number": 2,
            "timestamp": now,
            "latency_ms": None,
            "payload": {
                "tool": {
                    "name": "write_file",
                    "arguments": {"path": "/big", "content": "A" * 5000},
                },
                "state": {"message_count": 7},
                "started_at": iso,
            },
            "metadata": {
                "description": "d",
                "project_id": "p",
                "resolved_config": {"blob": "Z" * 5000},
            },
        }
    )
    w.insert_audit_pre(
        {
            "job_id": seeded["job"],
            "agent_type": "u",
            "iteration": 9,
            "step_type": "error",
            "node_name": "execute",
            "phase": None,
            "phase_number": None,
            "timestamp": now,
            "latency_ms": None,
            "payload": {
                "error": {"type": "Boom", "message": "kaboom", "traceback": "T" * 3000}
            },
            "metadata": None,
        }
    )
    w.close()

    s = await _store(seeded["dsn"])
    try:
        fat = await s.get_job_audit(seeded["job"], lean=False)
        lean = await s.get_job_audit(seeded["job"], lean=True)
        assert fat["total"] == lean["total"]  # same logical rows either way

        def pick(res, key, val):
            return next(
                e
                for e in res["entries"]
                if e.get(key, {}).get("name" if key == "tool" else "type") == val
            )

        wf_fat = pick(fat, "tool", "write_file")
        wf_lean = pick(lean, "tool", "write_file")
        err_lean = pick(lean, "error", "Boom")

        # Fat keeps boot metadata + heavy sub-keys.
        assert wf_fat["metadata"]["resolved_config"]
        assert wf_fat["tool"]["arguments"]["content"]
        assert wf_fat["state"]["message_count"] == 7

        # Lean drops metadata entirely + the expand-only heavy sub-keys, but keeps
        # the render fields (tool name, step_number, timestamp).
        assert "metadata" not in wf_lean
        assert "arguments" not in wf_lean["tool"]
        assert "state" not in wf_lean
        assert wf_lean["tool"]["name"] == "write_file"
        assert wf_lean["step_number"] == wf_fat["step_number"]
        # Error traceback stripped; type/message (rendered) kept.
        assert "traceback" not in err_lean["error"]
        assert err_lean["error"]["message"] == "kaboom"
    finally:
        await s.disconnect()


async def test_get_audit_step_returns_full_row(seeded):
    s = await _store(seeded["dsn"])
    try:
        listing = await s.get_job_audit(seeded["job"])
        first = listing["entries"][0]
        detail = await s.get_audit_step(seeded["job"], first["id"])
        assert detail is not None
        assert detail["id"] == first["id"]
        assert detail["step_number"] == first["step_number"]
        # Unknown id → None (not an exception).
        assert await s.get_audit_step(seeded["job"], 999999999) is None
    finally:
        await s.disconnect()


async def test_step_number_global_under_filter(seeded):
    s = await _store(seeded["dsn"])
    try:
        full = await s.get_job_audit(seeded["job"])
        assert [e["step_number"] for e in full["entries"]] == [1, 2, 3]
        # Filtering must NOT renumber: the tool row keeps its global step_number 1.
        tools = await s.get_job_audit(seeded["job"], filter_category="tools")
        assert [e["step_number"] for e in tools["entries"]] == [1]
        errors = await s.get_job_audit(seeded["job"], filter_category="errors")
        assert [e["step_number"] for e in errors["entries"]] == [3]
    finally:
        await s.disconnect()


async def test_version_counts_logical(seeded):
    s = await _store(seeded["dsn"])
    try:
        ver = await s.get_job_version(seeded["job"])
        assert ver["auditEntryCount"] == 3  # pre rows only
        assert ver["graphDeltaCount"] == 0
        assert isinstance(ver["version"], int)
        # Empty job -> None.
        assert await s.get_job_version(str(uuid4())) is None
    finally:
        await s.disconnect()


def _pre_row(job_id: str, step_type: str, timestamp: datetime) -> dict:
    return {
        "job_id": job_id,
        "agent_type": "worker",
        "iteration": 1,
        "step_type": step_type,
        "node_name": "n",
        "timestamp": timestamp,
        "payload": {},
    }


async def test_last_activity_is_newest_timestamp_not_newest_row(seeded):
    """Job 3677b309: a lease's ``claim_timing`` row is inserted when the lease
    ends but stamped with its claim time, so the newest row by id is not the
    newest step. ``end`` (and the version's ``lastUpdate``) must be the newest
    step; ``start`` stays the first-inserted row."""
    job_id = str(uuid4())
    now = datetime.now(timezone.utc).replace(microsecond=0)
    first_step = now - timedelta(minutes=5)
    last_step = now - timedelta(seconds=20)
    writer = SyncAuditWriter(seeded["dsn"])
    try:
        writer.insert_audit_pre(_pre_row(job_id, "llm", first_step))
        writer.insert_audit_pre(_pre_row(job_id, "tool", now - timedelta(minutes=2)))
        writer.insert_audit_pre(_pre_row(job_id, "tool", last_step))
        # Record-keeping rows inserted last, stamped earlier.
        writer.insert_audit_pre(
            _pre_row(job_id, "audit_receipt", now - timedelta(minutes=1))
        )
        writer.insert_audit_pre(
            _pre_row(job_id, "claim_timing", now - timedelta(minutes=4))
        )
    finally:
        writer.close()

    store = await _store(seeded["dsn"])
    try:
        time_range = await store.get_audit_time_range(job_id)
        version = await store.get_job_version(job_id)
        assert await store.get_audit_time_range(str(uuid4())) is None
    finally:
        await store.disconnect()

    def _iso(value: datetime) -> str:
        return value.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    assert time_range == {"start": _iso(first_step), "end": _iso(last_step)}
    assert version["lastUpdate"] == _iso(last_step)


async def test_last_activity_never_later_than_database_clock(seeded):
    """Pre rows carry the agent's clock; one running ahead must not report
    activity in the future."""
    job_id = str(uuid4())
    before = datetime.now(timezone.utc)
    writer = SyncAuditWriter(seeded["dsn"])
    try:
        writer.insert_audit_pre(_pre_row(job_id, "tool", before + timedelta(hours=1)))
    finally:
        writer.close()

    store = await _store(seeded["dsn"])
    try:
        time_range = await store.get_audit_time_range(job_id)
    finally:
        await store.disconnect()
    after = datetime.now(timezone.utc)

    end = datetime.fromisoformat(time_range["end"].replace("Z", "+00:00"))
    assert before <= end <= after


async def test_list_llm_requests_token_usage_and_status(seeded):
    s = await _store(seeded["dsn"])
    try:
        lst = await s.list_llm_requests(seeded["job"])
        assert lst["total"] == 3
        main = next(e for e in lst["entries"] if e["call_type"] == "main")
        assert main["token_usage"] == {"total_tokens": 9}  # surfaced from metrics
        assert main["tool_calls"] == [{"name": "read_file"}]
        # Responses-API row: empty token_usage falls back to the normalized
        # usage_metadata, re-keyed to the Chat Completions names the formatter
        # and Cockpit debug view read.
        codex = next(e for e in lst["entries"] if e["call_type"] == "codex")
        assert codex["token_usage"] == {
            "prompt_tokens": 15869,
            "completion_tokens": 461,
            "total_tokens": 16330,
        }
        # status filter reads from metadata (where archive_error folds it).
        errs = await s.list_llm_requests(seeded["job"], status="error")
        assert errs["total"] == 1
        assert errs["entries"][0]["error"]["type"] == "TimeoutError"
    finally:
        await s.disconnect()


async def test_list_claim_timings_returns_payloads_in_claim_time_order(seeded):
    job_id = str(uuid4())
    now = datetime.now(timezone.utc)
    writer = SyncAuditWriter(seeded["dsn"])
    try:
        for timestamp, outcome in (
            (now, "terminal:completed"),
            (now - timedelta(seconds=30), "rotated"),
        ):
            writer.insert_audit_pre(
                {
                    "job_id": job_id,
                    "agent_type": "worker",
                    "iteration": 1,
                    "step_type": "claim_timing",
                    "node_name": "worker_claim",
                    "timestamp": timestamp,
                    "payload": {
                        "claimed_at": timestamp.isoformat(),
                        "outcome": outcome,
                    },
                }
            )
    finally:
        writer.close()

    store = await _store(seeded["dsn"])
    try:
        rows = await store.list_claim_timings(job_id)
    finally:
        await store.disconnect()

    assert [row["payload"]["outcome"] for row in rows] == [
        "rotated",
        "terminal:completed",
    ]
    assert all(row["timestamp"].endswith("Z") for row in rows)


async def test_get_request_and_non_uuid(seeded):
    s = await _store(seeded["dsn"])
    try:
        req = await s.get_request(seeded["rid_main"])
        assert req["id"] == seeded["rid_main"] and req["call_type"] == "main"
        assert await s.get_request("not-a-number") is None  # str route param parity
        assert await s.get_request(999999) is None
        # Non-UUID job_id must degrade to empty, never 500.
        bad = await s.get_job_audit("not-a-uuid")
        assert bad["total"] == 0 and bad["entries"] == []
    finally:
        await s.disconnect()
