"""Unit tests for the archiver's Postgres backend branch (PR2).

DB-free: a ``FakeWriter`` captures the row dicts ``LLMArchiver`` builds, so these
run in the normal ``pytest`` suite and assert the field assembly, datetime->ISO-Z
serialization, chat cascade, two-phase audit payloads, and ``archive_error``
folding without standing up Postgres. The writer itself is exercised against a
real server in ``tests/test_audit_writer.py``.
"""

from __future__ import annotations

from uuid import uuid4

import pytest
from langchain_core.messages import (
    AIMessage,
    HumanMessage,
    SystemMessage,
    ToolMessage,
)

from agent.core.archiver import LLMArchiver
from shared.runtime.core.injection_markers import KNOWLEDGE_TOOL_CALL_ID_PREFIX
from shared.runtime.core.workspace_injection import (
    create_instruction_tool_messages,
    create_todos_human_message,
)


class FakeWriter:
    """Captures writer calls; mimics SyncAuditWriter's return contract."""

    def __init__(self, ready: bool = True):
        self.ready = ready
        self.llm_rows: list[dict] = []
        self.pre_rows: list[dict] = []
        self.posts: list[tuple] = []
        self.chat_rows: list[dict] = []
        self._id = 0

    def ensure_ready(self) -> bool:
        return self.ready

    def insert_llm_request(self, row):
        self._id += 1
        self.llm_rows.append(row)
        return self._id

    def insert_audit_pre(self, row):
        self._id += 1
        self.pre_rows.append(row)
        return self._id

    def insert_audit_post(self, pre_id, payload, latency_ms, request_id=None):
        if pre_id is None:
            return False
        self.posts.append((pre_id, payload, latency_ms, request_id))
        return True

    def insert_chat_entry(self, row):
        self.chat_rows.append(row)

    def close(self):
        pass


@pytest.fixture
def fw():
    return FakeWriter()


@pytest.fixture
def archiver(fw):
    return LLMArchiver(writer=fw)


def test_archive_builds_row_and_cascades_chat(archiver, fw):
    job = str(uuid4())
    messages = [SystemMessage("sys"), HumanMessage("hello")]
    response = AIMessage("hi there")
    rid = archiver.archive(
        job,
        "universal",
        messages,
        response,
        "gpt-x",
        latency_ms=12,
        iteration=3,
        call_type="main",
        metadata={"u": uuid4()},
    )
    assert isinstance(rid, int) and rid >= 1
    assert len(fw.llm_rows) == 1
    row = fw.llm_rows[0]
    assert row["job_id"] == job
    assert row["call_type"] == "main"
    assert row["model"] == "gpt-x"
    assert row["iteration"] == 3
    assert row["request"]["message_count"] == 2
    assert row["response"]["content"] == "hi there"
    assert "token_usage" in row["metrics"]
    # UUID in metadata is serialized to a string for JSONB.
    assert isinstance(row["metadata"]["u"], str)
    # call_type == 'main' cascades a chat row carrying the int request_id.
    assert len(fw.chat_rows) == 1
    assert fw.chat_rows[0]["request_id"] == rid
    assert fw.chat_rows[0]["inputs"][0]["content"] == "hello"  # system excluded


def test_archive_non_main_skips_chat(archiver, fw):
    archiver.archive(
        str(uuid4()),
        "vision",
        [HumanMessage("x")],
        AIMessage("y"),
        "m",
        call_type="vision",
    )
    assert len(fw.llm_rows) == 1
    assert fw.chat_rows == []


def test_audit_tool_call_serializes_datetimes(archiver, fw):
    pre_id = archiver.audit_tool_call(
        str(uuid4()), "universal", 1, "read_file", "call-1", {"path": "/x"}
    )
    assert isinstance(pre_id, int)
    row = fw.pre_rows[-1]
    assert row["step_type"] == "tool"
    assert row["node_name"] == "tools"
    assert row["payload"]["tool"]["name"] == "read_file"
    # started_at datetime -> ISO-8601 'Z' string in the JSONB payload.
    assert isinstance(row["payload"]["started_at"], str)
    assert row["payload"]["started_at"].endswith("Z")
    assert row["payload"]["completed_at"] is None


def test_update_tool_result_builds_post_payload(archiver, fw):
    pre_id = archiver.audit_tool_call(str(uuid4()), "universal", 1, "t", "c", {"a": 1})
    ok = archiver.update_tool_result(pre_id, "the result", True, 50)
    assert ok is True
    sent_pre, payload, latency, request_id = fw.posts[-1]
    assert sent_pre == pre_id
    assert latency == 50
    assert request_id is None
    assert payload["tool"]["result_preview"] == "the result"
    assert payload["tool"]["success"] is True
    assert payload["completed_at"].endswith("Z")


def test_update_llm_response_threads_request_id(archiver, fw):
    audit_id = archiver.audit_llm_call(str(uuid4()), "universal", 1, "gpt", 5, 10)
    ok = archiver.update_llm_response(
        audit_id,
        request_id=999,
        response_preview="hi",
        tool_calls=[{"id": "a", "name": "t"}],
        output_chars=2,
        latency_ms=20,
    )
    assert ok is True
    sent_pre, payload, latency, request_id = fw.posts[-1]
    assert sent_pre == audit_id
    assert request_id == 999  # hard column
    assert payload["llm"]["request_id"] == 999  # wire field
    assert payload["llm"]["metrics"]["tool_call_count"] == 1


def test_update_tool_result_none_pre_id_returns_false(archiver):
    assert archiver.update_tool_result(None, "x", True, 1) is False


def test_archive_error_folds_status_into_metadata(archiver, fw):
    rid = archiver.archive_error(
        str(uuid4()),
        "universal",
        [HumanMessage("q")],
        "gpt",
        "boom",
        "TimeoutError",
        call_type="auxiliary",
    )
    assert isinstance(rid, int)
    row = fw.llm_rows[-1]
    assert row["response"] == {}  # NOT NULL satisfied with an empty response
    assert row["metadata"]["status"] == "error"
    assert row["metadata"]["error"]["type"] == "TimeoutError"
    assert row["metrics"]["output_chars"] == 0


def test_lean_job_metadata_strips_only_heavy_keys():
    from agent.core.archiver import _lean_job_metadata

    # No heavy keys → same object back (cheap identity path); None passes through.
    light = {"description": "d", "project_id": "p"}
    assert _lean_job_metadata(light) is light
    assert _lean_job_metadata(None) is None

    heavy = {
        "description": "d",
        "project_id": "p",
        "resolved_config": {"agent": {"x": "y" * 1000}},
        "config_override": {"a": 1},
        "datasources": [{"id": 1}],
        "repositories": [{"url": "r"}],
    }
    assert _lean_job_metadata(heavy) == {"description": "d", "project_id": "p"}
    assert heavy["resolved_config"]  # original dict left untouched (shallow copy)


def test_audit_step_strips_heavy_job_metadata(archiver, fw):
    # The OOM path: the graph stamps the whole job metadata on every audit step.
    archiver.audit_step(
        str(uuid4()),
        "universal",
        "llm",
        "process",
        1,
        data={"llm": {"model": "m"}},
        metadata={
            "description": "build it",
            "project_id": "p1",
            "resolved_config": {"agent": {"blob": "z" * 5000}},
            "config_override": {"a": 1},
            "datasources": [{"id": "d"}],
            "repositories": [{"url": "u"}],
        },
    )
    md = fw.pre_rows[-1]["metadata"]
    assert md == {"description": "build it", "project_id": "p1"}
    for k in ("resolved_config", "config_override", "datasources", "repositories"):
        assert k not in md


def test_archive_strips_heavy_job_metadata_from_llm_row(archiver, fw):
    archiver.archive(
        str(uuid4()),
        "universal",
        [HumanMessage("hi")],
        AIMessage("yo"),
        "gpt-x",
        metadata={"description": "d", "resolved_config": {"big": "z" * 5000}},
    )
    md = fw.llm_rows[-1]["metadata"]
    assert md == {"description": "d"}
    assert "resolved_config" not in md


def test_unready_writer_short_circuits(archiver, fw):
    fw.ready = False
    assert archiver.audit_step(str(uuid4()), "u", "tool", "tools", 1) is None
    assert fw.pre_rows == []


def test_from_env_gating(monkeypatch):
    for k in (
        "AUDIT_DB_URL",
        "AUDIT_POSTGRES_USER",
        "AUDIT_POSTGRES_PASSWORD",
        "AUDIT_POSTGRES_HOST",
        "AUDIT_POSTGRES_DB",
    ):
        monkeypatch.delenv(k, raising=False)
    # No audit DB configured -> archiving disabled.
    assert LLMArchiver.from_env() is None
    # A DSN -> an archiver bound to the Postgres audit store (no connection yet).
    monkeypatch.setenv("AUDIT_DB_URL", "postgresql://u:p@localhost:5599/db")
    arch = LLMArchiver.from_env()
    assert arch is not None and arch._writer is not None


def test_writer_only_construction(fw):
    # The Postgres audit store is the only backend: the archiver binds to the
    # injected writer and carries no Mongo state.
    arch = LLMArchiver(writer=fw)
    assert arch._writer is fw


# ---------------------------------------------------------------------------
# Chat delta vs the transient tail-injection block (todos/knowledge/…)
# ---------------------------------------------------------------------------


def _payload_with_injections(todos: str = "Current Tasks\n  - [ ] todo_1: explore"):
    """History + tail-injection block, shaped as graph.py assembles it."""
    real_tc_id = "call_real123"
    history = [
        SystemMessage("sys"),
        HumanMessage("do the task"),
        AIMessage(
            "",
            tool_calls=[
                {"name": "read_file", "args": {"path": "a.md"}, "id": real_tc_id}
            ],
        ),
        ToolMessage("file body here", tool_call_id=real_tc_id, name="read_file"),
    ]
    kb_id = f"{KNOWLEDGE_TOOL_CALL_ID_PREFIX}abcd1234"
    kb_pair = [
        AIMessage(
            "",
            tool_calls=[{"name": "kb_search", "args": {"query": "ctx"}, "id": kb_id}],
        ),
        ToolMessage("--- Project Knowledge ---\n[1] a note", tool_call_id=kb_id),
    ]
    instr_ai, instr_tool = create_instruction_tool_messages(
        "instructions.md", "READ ME"
    )
    tail = kb_pair + [instr_ai, instr_tool, create_todos_human_message(todos)]
    return history + tail, real_tc_id


def test_chat_delta_survives_tail_injections(archiver, fw):
    """Real tool results are the delta; injections become context descriptors."""
    messages, real_tc_id = _payload_with_injections()
    archiver.archive(str(uuid4()), "universal", messages, AIMessage("next"), "gpt-x")

    inputs = fw.chat_rows[-1]["inputs"]
    real = [i for i in inputs if i["type"] != "context"]
    ctx = [i for i in inputs if i["type"] == "context"]

    # The delta is the real tool result — not the injected block.
    assert [i["type"] for i in real] == ["tool"]
    assert real[0]["tool_call_id"] == real_tc_id
    assert real[0]["content"] == "file body here"
    # No raw injected content stored as human/tool inputs.
    assert not any(i["content"].startswith("<active_tasks>") for i in real)

    assert {c["kind"] for c in ctx} == {"knowledge", "instruction", "todos"}
    for c in ctx:
        assert set(c) >= {"kind", "hash", "chars", "content_preview", "content"}
    instr = next(c for c in ctx if c["kind"] == "instruction")
    assert instr["label"] == "instructions.md"
    # Todos entry keeps the wrapper so the preview is self-identifying.
    todos = next(c for c in ctx if c["kind"] == "todos")
    assert todos["content"].startswith("<active_tasks>")


def test_chat_context_content_only_on_change(archiver, fw):
    """Unchanged injections store hash+preview only; changes re-store content."""
    job = str(uuid4())
    messages, _ = _payload_with_injections()
    archiver.archive(job, "universal", messages, AIMessage("a"), "gpt-x")
    archiver.archive(job, "universal", messages, AIMessage("b"), "gpt-x")

    second = [i for i in fw.chat_rows[1]["inputs"] if i["type"] == "context"]
    assert second and all("content" not in c for c in second)
    assert all(c["content_preview"] for c in second)

    changed, _ = _payload_with_injections(todos="Current Tasks\n  - [x] todo_1: done")
    archiver.archive(job, "universal", changed, AIMessage("c"), "gpt-x")
    third = {c["kind"]: c for c in fw.chat_rows[2]["inputs"] if c["type"] == "context"}
    assert "content" in third["todos"]
    assert "content" not in third["knowledge"]

    # A different job shares no hash state with the first.
    archiver.archive(str(uuid4()), "universal", messages, AIMessage("d"), "gpt-x")
    fourth = [i for i in fw.chat_rows[3]["inputs"] if i["type"] == "context"]
    assert all("content" in c for c in fourth)


def test_chat_tool_call_args_stored_when_long(archiver, fw):
    long_cmd = "x" * 500
    response = AIMessage(
        "",
        tool_calls=[
            {"name": "shell_execute", "args": {"command": long_cmd}, "id": "c1"},
            {"name": "read_file", "args": {"path": "a.md"}, "id": "c2"},
        ],
    )
    archiver.archive(str(uuid4()), "universal", [HumanMessage("q")], response, "gpt-x")
    tcs = {t["id"]: t for t in fw.chat_rows[-1]["response"]["tool_calls"]}
    assert tcs["c1"]["args_preview"].endswith("... [truncated]")
    assert len(tcs["c1"]["args"]) > len(tcs["c1"]["args_preview"])
    assert long_cmd[:300] in tcs["c1"]["args"]
    # Short args fit in the preview — no duplicate full copy.
    assert "args" not in tcs["c2"]


def test_chat_delta_labels_phase_instruction_block_as_context(archiver, fw):
    """A delivered phase block is history, not a user turn: on its delivery
    turn it is archived as a context descriptor (kind=phase_instruction,
    labelled by the artifact path), never as a human bubble."""
    from shared.runtime.core.workspace_injection import create_phase_instruction_message

    block = create_phase_instruction_message(
        "skills/research-guide/SKILL.md", "GUIDE BODY", "tactical", "2:tactical"
    )
    messages = [
        SystemMessage("sys"),
        HumanMessage("do the task"),
        AIMessage(
            "",
            tool_calls=[{"name": "read_file", "args": {"path": "a.md"}, "id": "c1"}],
        ),
        ToolMessage("file body", tool_call_id="c1", name="read_file"),
        block,
        create_todos_human_message("Current Tasks"),
    ]
    job = str(uuid4())
    archiver.archive(job, "universal", messages, AIMessage("next"), "gpt-x")

    inputs = fw.chat_rows[-1]["inputs"]
    real = [i for i in inputs if i["type"] != "context"]
    assert [i["type"] for i in real] == ["tool"]
    assert not any("[phase: " in (i.get("content") or "") for i in real)
    ctx = {c["kind"]: c for c in inputs if c["type"] == "context"}
    assert set(ctx) == {"phase_instruction", "todos"}
    entry = ctx["phase_instruction"]
    assert entry["label"] == "skills/research-guide/SKILL.md"
    assert entry["content"].startswith("[phase: tactical]")
    assert "GUIDE BODY" in entry["content"]

    # Next turn the block sits before the model's reply: it is history now,
    # not part of the delta.
    later = messages[:-1] + [AIMessage("next"), HumanMessage("continue"), messages[-1]]
    archiver.archive(job, "universal", later, AIMessage("ok"), "gpt-x")
    inputs2 = fw.chat_rows[-1]["inputs"]
    assert [c["kind"] for c in inputs2 if c["type"] == "context"] == ["todos"]
    assert [i["type"] for i in inputs2 if i["type"] != "context"] == ["human"]
