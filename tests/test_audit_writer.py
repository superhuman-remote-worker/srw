"""Real-Postgres tests for SyncAuditWriter (PR2).

Spins up an ephemeral PostgreSQL via testcontainers, applies the audit migration,
and exercises the writer against it. The writer's facade methods are synchronous
(they marshal onto a private loop on a daemon thread), so these tests are plain
sync functions; the async DB setup/readback runs via ``asyncio.run`` inside the
fixtures/helpers and is independent of the writer's own loop.

Skips cleanly without ``testcontainers[postgres]`` (requirements-dev.txt) or a
container runtime — see tests/test_audit_store.py for the same pattern.
"""

from __future__ import annotations

import asyncio
import json
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from uuid import uuid4

import asyncpg
import pytest

pytest.importorskip("testcontainers.postgres")

from orchestrator.database.migrate import run_migrations  # noqa: E402
from orchestrator.database.postgres import MIGRATIONS_AUDIT_DIR  # noqa: E402
from agent.database.audit_writer import SyncAuditWriter  # noqa: E402
from orchestrator.database.audit_store import AuditStore  # noqa: E402

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
        import re

        yield re.sub(
            r"^postgresql\+\w+://", "postgresql://", container.get_connection_url()
        )
    finally:
        container.stop()


@pytest.fixture
def audit_dsn(pg_dsn):
    """A fresh, migrated database; yields its DSN. Dropped on teardown."""
    dbname = f"audit_w_{uuid4().hex[:12]}"

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
    yield _swap_db(pg_dsn, dbname)
    asyncio.run(teardown())


def _fetchrow(dsn, sql, *args):
    async def go():
        conn = await asyncpg.connect(dsn)
        try:
            return await conn.fetchrow(sql, *args)
        finally:
            await conn.close()

    return asyncio.run(go())


def _make_writer(dsn) -> SyncAuditWriter:
    w = SyncAuditWriter(dsn)
    assert w.ensure_ready() is True
    return w


def test_insert_llm_request_roundtrip(audit_dsn):
    w = _make_writer(audit_dsn)
    try:
        job = str(uuid4())
        rid = w.insert_llm_request(
            {
                "job_id": job,
                "agent_type": "universal",
                "call_type": "main",
                "model": "m",
                "iteration": 1,
                "timestamp": datetime.now(timezone.utc),
                "latency_ms": 5,
                "request": {
                    "messages": [{"type": "HumanMessage", "content": "hi"}],
                    "message_count": 1,
                },
                "response": {"type": "AIMessage", "content": "yo"},
                "metadata": None,
                "auxiliary_metadata": None,
                "metrics": {"token_usage": {"total_tokens": 7}},
            }
        )
        assert isinstance(rid, int) and rid >= 1
        row = _fetchrow(
            audit_dsn, "SELECT request, metrics FROM llm_requests WHERE id=$1", rid
        )
        import json

        req = (
            json.loads(row["request"])
            if isinstance(row["request"], str)
            else row["request"]
        )
        assert req["messages"][0]["content"] == "hi"
    finally:
        w.close()


def test_two_phase_audit_post(audit_dsn):
    w = _make_writer(audit_dsn)
    try:
        job = str(uuid4())
        pre = w.insert_audit_pre(
            {
                "job_id": job,
                "agent_type": "u",
                "iteration": 1,
                "step_type": "tool",
                "node_name": "tools",
                "phase": None,
                "phase_number": None,
                "timestamp": datetime.now(timezone.utc),
                "latency_ms": None,
                "payload": {"tool": {"name": "t"}},
                "metadata": None,
            }
        )
        assert isinstance(pre, int)
        # valid pre -> True; missing pre -> False; None pre -> False (no DB touch)
        assert w.insert_audit_post(pre, {"tool": {"success": True}}, 9, None) is True
        assert w.insert_audit_post(9_999_999, {"x": 1}, 1, None) is False
        assert w.insert_audit_post(None, {"x": 1}, 1, None) is False
        post = _fetchrow(
            audit_dsn,
            "SELECT job_id, pre_id FROM agent_audit WHERE pre_id=$1 AND event_phase='post'",
            pre,
        )
        assert post is not None and post["pre_id"] == pre
        assert str(post["job_id"]) == job  # derived from the pre row
    finally:
        w.close()


def test_thread_safety_from_worker_thread(audit_dsn):
    w = _make_writer(audit_dsn)
    try:
        job = str(uuid4())

        def write():
            return w.insert_audit_pre(
                {
                    "job_id": job,
                    "agent_type": "vision",
                    "iteration": 0,
                    "step_type": "llm",
                    "node_name": "execute",
                    "timestamp": datetime.now(timezone.utc),
                    "payload": {"llm": {"model": "v"}},
                    "metadata": None,
                }
            )

        with ThreadPoolExecutor(max_workers=2) as ex:
            ids = [f.result(timeout=20) for f in [ex.submit(write), ex.submit(write)]]
        assert all(isinstance(i, int) and i >= 1 for i in ids)
        assert ids[0] != ids[1]
    finally:
        w.close()


def test_nul_in_tool_output_still_writes_llm_request_and_chat_rows(audit_dsn):
    """N62: Postgres jsonb rejects ``\\u0000``; the row must land anyway."""
    w = _make_writer(audit_dsn)
    try:
        job = str(uuid4())
        tool_output = "PK\x03\x04\x00\x00binary\x00tail"
        rid = w.insert_llm_request(
            {
                "job_id": job,
                "model": "m",
                "timestamp": datetime.now(timezone.utc),
                "request": {
                    "messages": [{"type": "ToolMessage", "content": tool_output}],
                    "key\x00": "literal \\u0000 stays text",
                },
                "response": {"type": "AIMessage", "content": "read it"},
            }
        )
        assert isinstance(rid, int)
        w.insert_chat_entry(
            {
                "job_id": job,
                "timestamp": datetime.now(timezone.utc),
                "request_id": rid,
                "inputs": [{"type": "ToolMessage", "content": tool_output}],
                "response": {"type": "AIMessage", "content": "read it"},
            }
        )
        req = _fetchrow(audit_dsn, "SELECT request FROM llm_requests WHERE id=$1", rid)
        chat = _fetchrow(
            audit_dsn, "SELECT inputs FROM chat_history WHERE request_id=$1", rid
        )
        assert chat is not None
        request = json.loads(req["request"])
        assert request["messages"][0]["content"] == ("PK\x03\x04��binary�tail")
        assert request["key�"] == "literal \\u0000 stays text"
        assert json.loads(chat["inputs"])[0]["content"] == ("PK\x03\x04��binary�tail")
    finally:
        w.close()


def test_from_env_unconfigured_returns_none(monkeypatch):
    for k in (
        "AUDIT_POSTGRES_USER",
        "AUDIT_POSTGRES_PASSWORD",
        "AUDIT_POSTGRES_HOST",
        "AUDIT_POSTGRES_DB",
        "AUDIT_DB_URL",
    ):
        monkeypatch.delenv(k, raising=False)
    assert SyncAuditWriter.from_env() is None


_RUNTIME_ACCESS = "synthetic-postgres-runtime-access-never-a-real-credential"
_RUNTIME_REFRESH = "synthetic-postgres-runtime-refresh-never-a-real-credential"
_RUNTIME_IDENTITY = {
    "caller_kind": "worker",
    "project_id": "synthetic-project",
    "project_role": "viewer",
    "thread_id": "synthetic-thread",
    "officer_incarnation": None,
    "user_id": "synthetic-user",
}


def _runtime_metadata(origin):
    return {
        "origin": origin,
        "nested": {"retained": [1, None]},
        "runtime_actor": {
            **_RUNTIME_IDENTITY,
            "access_credential": _RUNTIME_ACCESS,
            "refresh_credential": _RUNTIME_REFRESH,
            "access_expires_at": "2026-09-06T00:00:00+00:00",
            "refresh_expires_at": "2026-09-07T00:00:00+00:00",
        },
    }


def _json_value(value):
    return json.loads(value) if isinstance(value, str) else value


def _assert_runtime_metadata_redacted(value, origin):
    value = _json_value(value)
    assert value == {
        "origin": origin,
        "nested": {"retained": [1, None]},
        "runtime_actor": _RUNTIME_IDENTITY,
    }
    encoded = json.dumps(value)
    for excluded in (
        _RUNTIME_ACCESS,
        _RUNTIME_REFRESH,
        "access_credential",
        "refresh_credential",
        "access_expires_at",
        "refresh_expires_at",
    ):
        assert excluded not in encoded


def test_runtime_credentials_never_reach_writer_jsonb(audit_dsn):
    """Use the sync facade, real private-loop codec, SQL and raw DB readback."""
    job = str(uuid4())
    llm = {
        "job_id": job,
        "model": "synthetic-model",
        "timestamp": datetime.now(timezone.utc),
        "request": {"messages": [{"content": "unchanged-input"}]},
        "response": {"content": "unchanged-output"},
        "metadata": _runtime_metadata("llm-column"),
        "auxiliary_metadata": _runtime_metadata("llm-auxiliary"),
    }
    pre = {
        "job_id": job,
        "step_type": "tool",
        "timestamp": datetime.now(timezone.utc),
        "phase": "strategic",
        "payload": {
            "metadata": _runtime_metadata("pre-shadow"),
            "tool": {"name": "synthetic-tool", "arguments": {"keep": True}},
        },
        "metadata": _runtime_metadata("pre-column"),
    }
    post = {
        "metadata": _runtime_metadata("post-shadow"),
        "tool": {"result": "unchanged-result"},
        "phase": {"complete": True},
    }
    originals = deepcopy((llm, pre, post))
    writer = _make_writer(audit_dsn)
    try:
        request_id = writer.insert_llm_request(llm)
        assert isinstance(request_id, int)
        pre_id = writer.insert_audit_pre(pre)
        assert isinstance(pre_id, int)
        assert writer.insert_audit_post(pre_id, post, 7, request_id) is True
        stored_llm = _fetchrow(
            audit_dsn,
            "SELECT request, response, metadata, auxiliary_metadata "
            "FROM llm_requests WHERE id=$1",
            request_id,
        )
        stored_pre = _fetchrow(
            audit_dsn,
            "SELECT metadata, payload FROM agent_audit "
            "WHERE id=$1 AND event_phase='pre'",
            pre_id,
        )
        stored_post = _fetchrow(
            audit_dsn,
            "SELECT metadata, payload, request_id, latency_ms FROM agent_audit "
            "WHERE pre_id=$1 AND event_phase='post'",
            pre_id,
        )
        assert _json_value(stored_llm["request"]) == llm["request"]
        assert _json_value(stored_llm["response"]) == llm["response"]
        _assert_runtime_metadata_redacted(stored_llm["metadata"], "llm-column")
        _assert_runtime_metadata_redacted(
            stored_llm["auxiliary_metadata"], "llm-auxiliary"
        )
        _assert_runtime_metadata_redacted(stored_pre["metadata"], "pre-column")
        pre_payload = _json_value(stored_pre["payload"])
        _assert_runtime_metadata_redacted(pre_payload["metadata"], "pre-shadow")
        assert pre_payload["tool"] == pre["payload"]["tool"]
        post_payload = _json_value(stored_post["payload"])
        _assert_runtime_metadata_redacted(post_payload["metadata"], "post-shadow")
        assert post_payload["tool"] == post["tool"]
        assert post_payload["phase"] == post["phase"]
        # The append-only post SQL has never copied the pre metadata column.
        assert stored_post["metadata"] is None
        assert stored_post["request_id"] == request_id
        assert stored_post["latency_ms"] == 7
        assert (llm, pre, post) == originals
    finally:
        writer.close()


@pytest.mark.parametrize("metadata_shadow", (False, True))
def test_historical_runtime_credentials_filtered_by_real_detail_reads(
    audit_dsn, metadata_shadow
):
    """Insert old-format rows directly; prove projection without a DB rewrite."""
    job = str(uuid4())
    pre_payload = {"tool": {"name": "synthetic-tool", "arguments": {"keep": True}}}
    post_payload = {"tool": {"result": "unchanged-result"}, "phase": {"complete": True}}
    if metadata_shadow:
        pre_payload["metadata"] = _runtime_metadata("pre-shadow")
        post_payload["metadata"] = _runtime_metadata("post-shadow")

    async def exercise():
        # Plain asyncpg uses raw JSON strings. These inserts deliberately bypass
        # the writer so an old stored credential is present before each read.
        conn = await asyncpg.connect(audit_dsn)
        store = AuditStore(audit_dsn)
        try:
            request_id = await conn.fetchval(
                "INSERT INTO llm_requests "
                "(job_id, model, request, response, metadata, auxiliary_metadata) "
                "VALUES ($1::uuid, 'synthetic-model', $2::jsonb, $3::jsonb, "
                "$4::jsonb, $5::jsonb) RETURNING id",
                job,
                json.dumps({"messages": [{"content": "unchanged-input"}]}),
                json.dumps({"content": "unchanged-output"}),
                json.dumps(_runtime_metadata("llm-column")),
                json.dumps(_runtime_metadata("llm-auxiliary")),
            )
            pre_id = await conn.fetchval(
                "INSERT INTO agent_audit "
                "(job_id, step_type, phase, payload, metadata) "
                "VALUES ($1::uuid, 'tool', 'strategic', $2::jsonb, $3::jsonb) "
                "RETURNING id",
                job,
                json.dumps(pre_payload),
                json.dumps(_runtime_metadata("pre-column")),
            )
            await conn.execute(
                "INSERT INTO agent_audit "
                "(job_id, step_type, event_phase, pre_id, payload) "
                "VALUES ($1::uuid, 'tool', 'post', $2, $3::jsonb)",
                job,
                pre_id,
                json.dumps(post_payload),
            )

            async def raw_rows():
                llm = await conn.fetchrow(
                    "SELECT metadata, auxiliary_metadata FROM llm_requests WHERE id=$1",
                    request_id,
                )
                audit = await conn.fetch(
                    "SELECT metadata, payload FROM agent_audit "
                    "WHERE job_id=$1::uuid ORDER BY id",
                    job,
                )
                return dict(llm), [dict(row) for row in audit]

            before = await raw_rows()
            _assert_historical_runtime_metadata(before[0]["metadata"], "llm-column")
            _assert_historical_runtime_metadata(
                before[0]["auxiliary_metadata"], "llm-auxiliary"
            )
            _assert_historical_runtime_metadata(before[1][0]["metadata"], "pre-column")
            if metadata_shadow:
                for index, origin in enumerate(("pre-shadow", "post-shadow")):
                    _assert_historical_runtime_metadata(
                        _json_value(before[1][index]["payload"])["metadata"], origin
                    )
            await store.connect()
            assert store.is_available
            request = await store.get_request(str(request_id))
            assert request is not None
            _assert_runtime_metadata_redacted(request["metadata"], "llm-column")
            _assert_runtime_metadata_redacted(
                request["auxiliary_metadata"], "llm-auxiliary"
            )
            assert request["response"] == {"content": "unchanged-output"}
            detail = await store.get_audit_step(job, pre_id)
            assert detail is not None
            _assert_runtime_metadata_redacted(
                detail["metadata"], "post-shadow" if metadata_shadow else "pre-column"
            )
            assert detail["phase"] == {"complete": True}
            assert detail["tool"] == {
                "name": "synthetic-tool",
                "arguments": {"keep": True},
                "result": "unchanged-result",
            }
            assert await raw_rows() == before
        finally:
            await store.disconnect()
            await conn.close()

    asyncio.run(exercise())


def _assert_historical_runtime_metadata(value, origin):
    value = _json_value(value)
    assert value == _runtime_metadata(origin)
    assert value["runtime_actor"]["access_credential"] == _RUNTIME_ACCESS
    assert value["runtime_actor"]["refresh_credential"] == _RUNTIME_REFRESH


# ---------------------------------------------------------------------------
# A real server that goes away and comes back (the unit-level cases, with a
# fake store, are tests/test_audit_writer_recovery.py)
# ---------------------------------------------------------------------------


def _admin(pg_dsn, sql):
    async def go():
        conn = await asyncpg.connect(pg_dsn)
        try:
            await conn.execute(sql)
        finally:
            await conn.close()

    asyncio.run(go())


def _fetch(dsn, sql, *args):
    async def go():
        conn = await asyncpg.connect(dsn)
        try:
            return await conn.fetch(sql, *args)
        finally:
            await conn.close()

    return asyncio.run(go())


def test_rows_written_during_an_outage_arrive_after_it(pg_dsn, audit_dsn, monkeypatch):
    """The database refuses connections and drops the open ones, as a restart
    does. Rows made meanwhile are spooled, then written in order with their
    references mapped to real ids, and the receipt says the record is whole."""
    from agent.core.archiver import LLMArchiver
    from agent.database import audit_writer
    from langchain_core.messages import AIMessage, HumanMessage

    monkeypatch.setattr(audit_writer, "_RECONNECT_BASE_S", 0.05)
    monkeypatch.setattr(audit_writer, "_RECONNECT_MAX_S", 0.2)
    dbname = audit_dsn.rsplit("/", 1)[1].split("?")[0]
    w = _make_writer(audit_dsn)
    w._backoff = 0.05
    archiver = LLMArchiver(writer=w)
    job = str(uuid4())

    def step(n):
        request_id = archiver.archive(
            job, "u", [HumanMessage(f"q{n}")], AIMessage(f"a{n}"), "m", iteration=n
        )
        audit_id = archiver.audit_tool_call(
            job_id=job,
            agent_type="u",
            iteration=n,
            tool_name="t",
            call_id=f"c{n}",
            arguments={"n": n},
        )
        archiver.update_tool_result(audit_id, f"result {n}", True, 1)
        return request_id, audit_id

    try:
        before = step(1)
        assert before[0] > 0 and before[1] > 0

        _admin(pg_dsn, f'ALTER DATABASE "{dbname}" ALLOW_CONNECTIONS false')
        _admin(
            pg_dsn,
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            f"WHERE datname = '{dbname}'",
        )
        during = step(2)
        assert during[0] < 0 and during[1] < 0  # placeholders: nothing was lost
        assert w.spooled == 4
        assert archiver.close_job_record(job, "u", flush_timeout_s=0.3)["spooled"] == 4

        _admin(pg_dsn, f'ALTER DATABASE "{dbname}" ALLOW_CONNECTIONS true')
        assert w.flush(10.0) is True
        after = step(3)
        assert after[0] > 0
        archiver.close_job_record(job, "u", flush_timeout_s=5.0)

        llm = _fetch(
            audit_dsn,
            "SELECT id, iteration, metrics FROM llm_requests WHERE job_id=$1::uuid "
            "ORDER BY id",
            job,
        )
        assert [r["iteration"] for r in llm] == [1, 2, 3]
        import json as _json

        replayed = [bool(_json.loads(r["metrics"]).get("audit_replayed")) for r in llm]
        assert replayed == [False, True, False]

        chat = _fetch(
            audit_dsn,
            "SELECT request_id FROM chat_history WHERE job_id=$1::uuid ORDER BY id",
            job,
        )
        assert [r["request_id"] for r in chat] == [r["id"] for r in llm]

        # Every tool step has its result row, attached to the right pre row.
        pairs = _fetch(
            audit_dsn,
            "SELECT p.payload->'tool'->>'call_id' AS call, "
            "       q.payload->'tool'->>'result_preview' AS result "
            "FROM agent_audit p JOIN agent_audit q "
            "  ON q.pre_id = p.id AND q.event_phase = 'post' "
            "WHERE p.job_id=$1::uuid AND p.step_type='tool' AND p.event_phase='pre' "
            "ORDER BY p.id",
            job,
        )
        assert [(r["call"], r["result"]) for r in pairs] == [
            ("c1", "result 1"),
            ("c2", "result 2"),
            ("c3", "result 3"),
        ]

        receipts = [
            _json.loads(r["payload"])["audit_receipt"]
            for r in _fetch(
                audit_dsn,
                "SELECT payload FROM agent_audit WHERE job_id=$1::uuid "
                "AND step_type='audit_receipt' ORDER BY id",
                job,
            )
        ]
        assert len(receipts) == 2
        first, second = receipts
        # The first receipt was made while the store was down and reached it
        # after the outage: it counts what had arrived by then.
        assert first["complete"] is True and first["dropped"] == 0
        assert first["rows"]["llm"] == {"written": 1, "replayed": 1, "dropped": 0}
        assert first["outages"] == 1
        assert second["complete"] is True
        assert second["rows"]["llm"] == {"written": 1, "replayed": 0, "dropped": 0}
        assert sum(
            r["rows"]["llm"]["written"] + r["rows"]["llm"]["replayed"] for r in receipts
        ) == len(llm)
    finally:
        _admin(pg_dsn, f'ALTER DATABASE "{dbname}" ALLOW_CONNECTIONS true')
        w.close()


def test_a_server_down_at_first_use_does_not_disable_the_writer(
    pg_dsn, audit_dsn, monkeypatch
):
    from agent.database import audit_writer

    monkeypatch.setattr(audit_writer, "_RECONNECT_BASE_S", 0.05)
    monkeypatch.setattr(audit_writer, "_RECONNECT_MAX_S", 0.2)
    dbname = audit_dsn.rsplit("/", 1)[1].split("?")[0]
    _admin(pg_dsn, f'ALTER DATABASE "{dbname}" ALLOW_CONNECTIONS false')
    w = SyncAuditWriter(audit_dsn)
    w._backoff = 0.05
    job = str(uuid4())
    try:
        placeholder = w.insert_llm_request(
            {
                "job_id": job,
                "model": "m",
                "timestamp": datetime.now(timezone.utc),
                "request": {"messages": []},
                "response": {},
            }
        )
        assert placeholder is not None and placeholder < 0
        _admin(pg_dsn, f'ALTER DATABASE "{dbname}" ALLOW_CONNECTIONS true')
        assert w.flush(10.0) is True
        rows = _fetch(
            audit_dsn, "SELECT id FROM llm_requests WHERE job_id=$1::uuid", job
        )
        assert len(rows) == 1
    finally:
        _admin(pg_dsn, f'ALTER DATABASE "{dbname}" ALLOW_CONNECTIONS true')
        w.close()
