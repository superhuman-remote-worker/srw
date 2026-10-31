"""The P4 offline measurement (bench/queries/p4_same_conversation_step*.sql).

Step 1 reads the audit database's archived main requests once and writes the
injected memory handles with each conversation's position; step 2 resolves
them in the vector database and applies the take-in's rule. Both run through
psql exactly as documented, against one pgvector container holding a migrated
audit database and a migrated vector database. The request bodies are built
by the production code (typed entries, the carrier fold, the archiver's
message form), so a format change shows up here.

Skips without psql, testcontainers or a container runtime.
"""

from __future__ import annotations

import asyncio
import json
import re
import shutil
import subprocess
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import pytest
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage

pytest.importorskip("testcontainers.postgres")
asyncpg = pytest.importorskip("asyncpg")

from agent.core.archiver import _message_to_dict  # noqa: E402
from agent.core.context import make_summary_message  # noqa: E402
from orchestrator.database.migrate import run_migrations  # noqa: E402
from orchestrator.database.postgres import (  # noqa: E402
    MIGRATIONS_AUDIT_DIR,
    MIGRATIONS_VECTOR_DIR,
)
from shared.runtime.core.context_entries import (  # noqa: E402
    fold_context_entries,
    make_context_entry,
    memory_handle,
)
from shared.runtime.services.recall_store import MemoryRecord, RecallStore  # noqa: E402

STEP1 = "bench/queries/p4_same_conversation_step1_audit.sql"
STEP2 = "bench/queries/p4_same_conversation_step2_vector.sql"
PG_IMAGE = "pgvector/pgvector:pg16"

NOW = datetime.now(timezone.utc).replace(microsecond=0)
WINDOW_FROM = NOW - timedelta(hours=1)
WINDOW_TO = NOW + timedelta(hours=1)
WORKER = uuid.UUID("aaaaaaaa-0000-4000-8000-000000000001")
SESSION = uuid.UUID("bbbbbbbb-0000-4000-8000-000000000002")
OTHER = uuid.UUID("cccccccc-0000-4000-8000-000000000003")


def _memory(n: int, job: uuid.UUID, end: Optional[int]) -> MemoryRecord:
    return MemoryRecord(
        id=uuid.UUID(int=n),
        job_id=job,
        content=f"Fact number {n}.",
        importance=0.7,
        source_turn_start=None if end is None else end - 1,
        source_turn_end=end,
    )


# Worker: turn 1 injects A (source turn 1, in context) and C (another job);
# turn 3, after a compaction that removed turns 1-2, injects A again (now
# evicted), B (source turn 3, in context) and D (unstamped, after the first
# compaction: not in context).
A, B, C, D = (
    _memory(1, WORKER, 1),
    _memory(2, WORKER, 3),
    _memory(3, OTHER, 1),
    _memory(4, WORKER, None),
)
# Session: the summary first shows at turn 5 (boundary 4). Turn 5 injects E
# (source turn 4) and F (turn 2), both summarised; turn 6 injects H (turn 5,
# in context).
E, F, H = _memory(5, SESSION, 4), _memory(6, SESSION, 2), _memory(7, SESSION, 5)


def _entry(*memories: MemoryRecord) -> HumanMessage:
    body = RecallStore.render_memory_entry(
        [(m, memory_handle(m.id), False) for m in memories]
    )
    return make_context_entry("memory", body, section="memory")


def _call(call_id: str) -> AIMessage:
    return AIMessage(
        content="",
        tool_calls=[{"name": "read_file", "args": {"path": "x"}, "id": call_id}],
    )


def _result(call_id: str, content: str = "file text") -> ToolMessage:
    return ToolMessage(content=content, tool_call_id=call_id, name="read_file")


def _request(history: List[Any]) -> Dict[str, Any]:
    """The archived form of one request: the folded messages."""
    messages = [_message_to_dict(m) for m in fold_context_entries(history)]
    return {"messages": messages, "message_count": len(messages)}


SYSTEM = SystemMessage(content="system prompt")
TASK = HumanMessage(content="Do the task.")
SUMMARY = make_summary_message("Turns 1-2: read the files.")


def _worker_requests() -> List[Dict[str, Any]]:
    turn1 = [SYSTEM, TASK, _call("c1"), _result("c1"), _entry(A, C)]
    turn2 = [*turn1, _call("c2"), _result("c2")]
    # memory_search pulled C after the compaction: handles outside a memory
    # entry are not injections.
    pulled = _result("c3", "Search result:\n\n" + RecallStore.render_memory_list([C]))
    turn3 = [SYSTEM, SUMMARY, _call("c3"), pulled, _entry(A, B, D)]
    return [_request(turn1), _request(turn2), _request(turn3)]


def _session_requests() -> List[Dict[str, Any]]:
    turn5 = [SYSTEM, SUMMARY, HumanMessage(content="Hello"), _entry(E, F)]
    turn6 = [*turn5, AIMessage(content="Hi."), HumanMessage(content="More?"), _entry(H)]
    return [_request(turn5), _request(turn6)]


def _dsn(base: str, database: str) -> str:
    return re.sub(r"/[^/]*$", f"/{database}", base)


@pytest.fixture(scope="module")
def databases(tmp_path_factory):
    if shutil.which("psql") is None:
        pytest.skip("psql is not installed")
    from testcontainers.postgres import PostgresContainer

    container = PostgresContainer(PG_IMAGE)
    try:
        container.start()
    except Exception as exc:  # pragma: no cover - env without a runtime
        pytest.skip(f"no container runtime for testcontainers: {exc}")
    try:
        base = re.sub(
            r"^postgresql\+\w+://", "postgresql://", container.get_connection_url()
        )
        audit, vector = _dsn(base, "p4_audit"), _dsn(base, "p4_vector")
        asyncio.run(_seed(base, audit, vector))
        yield audit, vector
    finally:
        container.stop()


async def _seed(base: str, audit: str, vector: str) -> None:
    admin = await asyncpg.connect(base)
    try:
        await admin.execute("CREATE DATABASE p4_audit")
        await admin.execute("CREATE DATABASE p4_vector")
    finally:
        await admin.close()
    for dsn, migrations in (
        (audit, MIGRATIONS_AUDIT_DIR),
        (vector, MIGRATIONS_VECTOR_DIR),
    ):
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2)
        try:
            await run_migrations(pool, migrations)
        finally:
            await pool.close()

    conn = await asyncpg.connect(audit)
    try:
        insert = """
            INSERT INTO llm_requests (job_id, agent_type, call_type, model,
                                      iteration, "timestamp", request, response)
            VALUES ($1, $2, $3, 'm', $4, $5, $6::jsonb, '{}'::jsonb)
        """
        # One worker request before the window: counted, never read.
        await conn.execute(
            insert,
            WORKER,
            "developer",
            "main",
            0,
            WINDOW_FROM - timedelta(minutes=5),
            json.dumps({"messages": "not read"}),
        )
        for index, body in enumerate(_worker_requests()):
            await conn.execute(
                insert,
                WORKER,
                "developer",
                "main",
                index + 1,
                NOW + timedelta(minutes=index),
                json.dumps(body),
            )
        # A subagent call of the same job is not a main request.
        await conn.execute(
            insert,
            WORKER,
            "explorer",
            "subagent",
            9,
            NOW,
            json.dumps(_request([TASK, _entry(B)])),
        )
        for index, body in enumerate(_session_requests()):
            await conn.execute(
                insert,
                SESSION,
                "persistent",
                "main",
                5 + index,
                NOW + timedelta(minutes=index),
                json.dumps(body),
            )
        for appended in (2, 3):
            await conn.execute(
                """
                INSERT INTO agent_audit (job_id, agent_type, step_type,
                                         "timestamp", payload)
                VALUES ($1, 'developer', 'memory_inject', $2, $3::jsonb)
                """,
                WORKER,
                NOW,
                json.dumps({"count": appended, "appended": appended, "present": 0}),
            )
    finally:
        await conn.close()

    conn = await asyncpg.connect(vector)
    try:
        for memory in (A, B, C, D, E, F, H):
            await conn.execute(
                """
                INSERT INTO memories (id, job_id, content, source_turn_start,
                                      source_turn_end, created_at)
                VALUES ($1, $2, $3, $4, $5, $6)
                """,
                memory.id,
                memory.job_id,
                memory.content,
                memory.source_turn_start,
                memory.source_turn_end,
                WINDOW_FROM - timedelta(minutes=30),
            )
    finally:
        await conn.close()


def _psql(dsn: str, script: str, *variables: str) -> subprocess.CompletedProcess:
    args = ["psql", dsn, "-X", "-q", "--no-align", "--tuples-only", "-F", "|"]
    for variable in variables:
        args += ["-v", variable]
    result = subprocess.run(
        [*args, "-f", script], capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stderr
    return result


@pytest.fixture(scope="module")
def step1(databases, tmp_path_factory) -> Dict[str, Any]:
    audit, _ = databases
    out = tmp_path_factory.mktemp("p4") / "step1.json"
    _psql(
        audit,
        STEP1,
        f"from={WINDOW_FROM.isoformat()}",
        f"to={WINDOW_TO.isoformat()}",
        f"out={out}",
    )
    return {"path": out, "doc": json.loads(out.read_text())}


def _injections(doc: Dict[str, Any], job: uuid.UUID) -> List[tuple]:
    return [
        (i["turn"], i["compacted_through"], i["position_exact"], i["handle"])
        for i in doc["injections"]
        if i["job_id"] == str(job)
    ]


def _h(memory: MemoryRecord) -> str:
    return memory_handle(memory.id)


class TestStep1Audit:
    def test_worker_injections_are_the_tail_entries_with_their_position(self, step1):
        assert _injections(step1["doc"], WORKER) == sorted(
            [(1, 0, True, _h(A)), (1, 0, True, _h(C))], key=lambda r: r[3]
        ) + sorted(
            [(3, 2, True, _h(A)), (3, 2, True, _h(B)), (3, 2, True, _h(D))],
            key=lambda r: r[3],
        )

    def test_session_position_comes_from_the_summarys_first_turn(self, step1):
        assert _injections(step1["doc"], SESSION) == sorted(
            [(5, 4, True, _h(E)), (5, 4, True, _h(F))], key=lambda r: r[3]
        ) + [(6, 4, True, _h(H))]

    def test_jobs_carry_the_audit_cross_check(self, step1):
        jobs = {j["job_id"]: j for j in step1["doc"]["jobs"]}
        worker = jobs[str(WORKER)]
        assert (worker["lane"], worker["prior_requests"], worker["requests"]) == (
            "worker",
            1,
            3,
        )
        assert worker["injected"] == worker["audit_appended"] == 5
        assert worker["audit_rows"] == 2
        assert worker["audit_arm_b_skipped"] is None
        session = jobs[str(SESSION)]
        assert (session["lane"], session["requests"], session["injected"]) == (
            "session",
            2,
            3,
        )
        assert session["audit_rows"] == 0

    def test_a_job_filter_narrows_the_scan(self, databases, tmp_path):
        audit, _ = databases
        out = tmp_path / "only-session.json"
        _psql(
            audit,
            STEP1,
            f"from={WINDOW_FROM.isoformat()}",
            f"to={WINDOW_TO.isoformat()}",
            f"job_ids={SESSION}",
            f"out={out}",
        )
        doc = json.loads(out.read_text())
        assert [j["job_id"] for j in doc["jobs"]] == [str(SESSION)]


class TestStep2Vector:
    def test_the_rule_counts_what_arm_b_would_skip(self, databases, step1):
        _, vector = databases
        result = _psql(vector, STEP2, f"step1={step1['path']}")
        rows = [line.split("|") for line in result.stdout.splitlines() if line]
        totals = [row for row in rows if len(row) == 9]
        assert [row[0] for row in totals] == ["session", "worker", "ALL"]
        by_lane = {row[0]: row for row in totals}
        # lane | injected | same_conversation | source_in_context |
        # of_which_unstamped | pct | injected_exact | in_context_exact | pct
        assert by_lane["worker"][1:5] == ["5", "4", "2", "0"]
        assert by_lane["session"][1:5] == ["3", "3", "1", "0"]
        assert by_lane["ALL"][1:6] == ["8", "7", "3", "0", "37.5"]
        per_job = {row[0]: row for row in rows if len(row) == 11}
        # job | lane | injected | same | in_context | evicted | compacted |
        # requests | audit_rows | audit_appended | audit_arm_b_skipped
        assert per_job[str(WORKER)][1:10] == [
            "worker",
            "5",
            "4",
            "2",
            "2",
            "t",
            "3",
            "2",
            "5",
        ]
        assert per_job[str(SESSION)][1:8] == ["session", "3", "3", "1", "2", "t", "2"]
