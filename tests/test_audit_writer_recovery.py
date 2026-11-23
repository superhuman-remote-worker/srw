"""The audit writer survives an unreachable store and says what it lost.

Before this, one failed connect switched the writer off for the life of the
process, a write that failed while the store restarted was logged and gone,
and nothing told a job its record had a hole.

Pinned here, against a fake store that can be taken down and brought back:

- a down store never disables the writer: it is retried after a backoff, and
  between tries a write costs no network round trip;
- rows that could not be written wait in a spool and are written in order when
  the store answers, with their cross-references (a chat row's ``request_id``,
  a post row's ``pre_id``) mapped from placeholder to real ids;
- a row the server rejects for its content is dropped at once, and a row whose
  month has no partition waits;
- the spool is bounded, and ``flush`` drains it before a job ends;
- ``job_stats`` counts written, replayed, dropped and waiting rows per job.

The real-server path (asyncpg, JSONB codec, SQL) is ``tests/test_audit_writer.py``.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pytest

from agent.core.archiver import LLMArchiver
from agent.database import audit_writer
from agent.database.audit_writer import SyncAuditWriter, _is_content_error
from langchain_core.messages import AIMessage, HumanMessage

JOB = "11111111-1111-1111-1111-111111111111"
OTHER_JOB = "22222222-2222-2222-2222-222222222222"
NOW = datetime(2026, 11, 14, 10, 0, 0, tzinfo=timezone.utc)


class PgError(Exception):
    """A server error with a SQLSTATE, as asyncpg raises it."""

    def __init__(self, sqlstate: str, message: str = "server said no") -> None:
        super().__init__(message)
        self.sqlstate = sqlstate


class FakeStore:
    """Stands in for the writer's loop + pool: rows land in ``self.rows``.

    ``down`` makes every connect and write fail like a refused connection.
    ``reject`` maps a row kind to the error its next write raises.
    """

    def __init__(self) -> None:
        self.down = False
        self.reject: Dict[str, Exception] = {}
        self.rows: List[Dict[str, Any]] = []
        self.connects = 0
        self.attempts = 0
        self._id = 100

    def install(self, writer: SyncAuditWriter, monkeypatch) -> None:
        store = self

        def connect_locked() -> bool:
            import time

            if time.monotonic() < writer._retry_at:
                return False
            store.connects += 1
            if store.down:
                writer._mark_down(ConnectionRefusedError("connection refused"))
                return False
            writer._pool = object()
            return True

        def submit(operation, timeout: float = 0.0):
            return operation()

        monkeypatch.setattr(writer, "_connect_locked", connect_locked)
        monkeypatch.setattr(writer, "_submit", submit)
        for kind in ("llm", "pre", "post", "chat"):
            monkeypatch.setattr(writer, _METHODS[kind], self._op(kind))

    def _op(self, kind: str):
        def make(*args):
            def run():
                self.attempts += 1
                if self.down:
                    raise ConnectionRefusedError("connection refused")
                if kind in self.reject:
                    raise self.reject.pop(kind)
                self._id += 1
                if kind in ("llm", "pre", "chat"):
                    self.rows.append({"kind": kind, "id": self._id, **args[0]})
                    return self._id if kind != "chat" else None
                pre_id, payload, latency_ms, request_id = args
                if not any(r["kind"] == "pre" and r["id"] == pre_id for r in self.rows):
                    return False
                self.rows.append(
                    {
                        "kind": "post",
                        "id": self._id,
                        "pre_id": pre_id,
                        "payload": payload,
                        "request_id": request_id,
                    }
                )
                return True

            return run

        return make

    def of(self, kind: str) -> List[Dict[str, Any]]:
        return [r for r in self.rows if r["kind"] == kind]


_METHODS = {
    "llm": "_insert_llm_request",
    "pre": "_insert_audit_pre",
    "post": "_insert_audit_post",
    "chat": "_insert_chat_entry",
}


@pytest.fixture
def store():
    return FakeStore()


@pytest.fixture
def writer(store, monkeypatch):
    # No waiting in tests: a failed try may be repeated at once.
    monkeypatch.setattr(audit_writer, "_RECONNECT_BASE_S", 0.0)
    monkeypatch.setattr(audit_writer, "_RECONNECT_MAX_S", 0.0)
    w = SyncAuditWriter("postgresql://unused/unused")
    w._backoff = 0.0
    store.install(w, monkeypatch)
    return w


def _llm(job: str = JOB, n: int = 0) -> Dict[str, Any]:
    return {
        "job_id": job,
        "model": "m",
        "timestamp": NOW,
        "request": {"messages": [{"n": n}]},
        "response": {},
        "metrics": {"n": n},
    }


def _pre(job: str = JOB, n: int = 0) -> Dict[str, Any]:
    return {
        "job_id": job,
        "step_type": "tool",
        "timestamp": NOW,
        "payload": {"tool": {"name": f"t{n}"}},
    }


def _chat(request_id: Optional[int], job: str = JOB) -> Dict[str, Any]:
    return {"job_id": job, "timestamp": NOW, "request_id": request_id}


# ---------------------------------------------------------------------------
# The store goes away and comes back
# ---------------------------------------------------------------------------


def test_a_down_store_does_not_disable_the_writer(writer, store):
    store.down = True
    assert writer.insert_llm_request(_llm(n=1)) < 0
    assert writer.ensure_ready() is True  # still takes rows

    store.down = False
    real = writer.insert_llm_request(_llm(n=2))
    assert real > 0
    # The older row went first.
    assert [r["metrics"]["n"] for r in store.of("llm")] == [1, 2]
    assert writer.spooled == 0


def test_the_first_connect_failing_is_not_final(writer, store):
    """The old one-shot gate: a store down at boot meant no audit, ever."""
    store.down = True
    assert writer.ensure_ready() is True
    writer.insert_audit_pre(_pre())
    store.down = False
    assert writer.flush(1.0) is True
    assert len(store.of("pre")) == 1


def test_spooled_rows_replay_in_order_with_their_references(writer, store):
    """A model call, its chat row, a tool step and its result, all while down."""
    store.down = True
    request_id = writer.insert_llm_request(_llm(n=1))
    writer.insert_chat_entry(_chat(request_id))
    pre_id = writer.insert_audit_pre(_pre(n=1))
    assert writer.insert_audit_post(pre_id, {"tool": {"success": True}}, 12, request_id)
    assert request_id < 0 and pre_id < 0 and request_id != pre_id
    assert store.rows == []
    assert writer.spooled == 4

    store.down = False
    assert writer.flush(1.0) is True

    assert [r["kind"] for r in store.rows] == ["llm", "chat", "pre", "post"]
    llm, chat, pre, post = store.rows
    assert chat["request_id"] == llm["id"] > 0
    assert post["pre_id"] == pre["id"] > 0
    assert post["request_id"] == llm["id"]
    # Replayed rows say so; their content is otherwise what was handed in.
    assert llm["metrics"] == {"n": 1, "audit_replayed": True}
    assert pre["payload"]["audit_replayed"] is True
    assert post["payload"] == {"tool": {"success": True}, "audit_replayed": True}


def test_a_placeholder_still_works_after_its_row_was_replayed(writer, store):
    """A tool that started during the outage finishes after it."""
    store.down = True
    pre_id = writer.insert_audit_pre(_pre())
    store.down = False
    writer.insert_llm_request(_llm())  # drains the spool first

    assert writer.insert_audit_post(pre_id, {"tool": {}}, 5) is True
    assert store.of("post")[0]["pre_id"] == store.of("pre")[0]["id"]
    # Written after the outage, so not a replay.
    assert "audit_replayed" not in store.of("post")[0]["payload"]


def test_a_write_failing_midway_is_spooled_not_lost(writer, store):
    assert writer.insert_llm_request(_llm(n=1)) > 0
    store.down = True  # the store restarts between two writes
    placeholder = writer.insert_llm_request(_llm(n=2))
    assert placeholder < 0
    store.down = False
    assert writer.insert_llm_request(_llm(n=3)) > 0
    assert [r["metrics"]["n"] for r in store.of("llm")] == [1, 2, 3]


def test_no_network_during_the_backoff(writer, store, monkeypatch):
    monkeypatch.setattr(audit_writer, "_RECONNECT_BASE_S", 60.0)
    writer._backoff = 60.0
    store.down = True
    writer.insert_llm_request(_llm(n=1))
    tries = (store.connects, store.attempts)
    for n in range(2, 6):
        assert writer.insert_llm_request(_llm(n=n)) < 0
    assert (store.connects, store.attempts) == tries
    assert writer.spooled == 5


def test_backoff_doubles_to_its_cap(monkeypatch):
    monkeypatch.setattr(audit_writer, "_RECONNECT_BASE_S", 1.0)
    monkeypatch.setattr(audit_writer, "_RECONNECT_MAX_S", 30.0)
    w = SyncAuditWriter("postgresql://unused/unused")
    seen = []
    for _ in range(7):
        seen.append(w._backoff)
        w._mark_down(ConnectionRefusedError("refused"))
    assert seen == [1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0]
    w._mark_up()
    assert w._backoff == 1.0 and w._retry_at == 0.0


# ---------------------------------------------------------------------------
# Rows that are dropped
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "error,content",
    [
        (PgError("22P02"), True),  # invalid text representation
        (PgError("23502"), True),  # not-null violation
        (PgError("42703"), True),  # undefined column
        (PgError("23514"), False),  # no partition for the month: wait
        (PgError("57P03"), False),  # the database system is starting up
        (PgError("08006"), False),  # connection failure
        (PgError("53300"), False),  # too many connections
        (ConnectionRefusedError("refused"), False),
        (TimeoutError("timed out"), False),
        (ValueError("cannot encode"), True),
        (KeyError("job_id"), True),
    ],
)
def test_which_errors_drop_a_row(error, content):
    assert _is_content_error(error) is content


def test_a_rejected_row_is_dropped_and_the_next_one_written(writer, store):
    store.reject["llm"] = PgError("22P02")
    assert writer.insert_llm_request(_llm(n=1)) is None
    assert writer.spooled == 0
    assert writer.insert_llm_request(_llm(n=2)) > 0
    assert [r["metrics"]["n"] for r in store.of("llm")] == [2]
    stats = writer.job_stats(JOB)
    assert stats["rows"]["llm"] == {"written": 1, "replayed": 0, "dropped": 1}
    assert stats["complete"] is False


def test_a_missing_partition_waits(writer, store):
    store.reject["llm"] = PgError("23514", "no partition of relation found for row")
    assert writer.insert_llm_request(_llm(n=1)) < 0
    assert writer.flush(1.0) is True
    assert len(store.of("llm")) == 1


def test_rows_that_depend_on_a_dropped_row(writer, store):
    """The pre row is rejected on replay: its post row has nothing to attach
    to, and the chat row of a dropped model call keeps no dangling id."""
    store.down = True
    request_id = writer.insert_llm_request(_llm())
    writer.insert_chat_entry(_chat(request_id))
    pre_id = writer.insert_audit_pre(_pre())
    writer.insert_audit_post(pre_id, {"tool": {}}, 1)
    store.down = False
    store.reject = {"llm": PgError("22P02"), "pre": PgError("22P02")}
    assert writer.flush(1.0) is True

    assert [r["kind"] for r in store.rows] == ["chat"]
    assert store.rows[0]["request_id"] is None
    stats = writer.job_stats(JOB)
    assert stats["dropped"] == 3
    assert stats["rows"]["chat"]["replayed"] == 1


def test_the_spool_is_bounded(writer, store):
    writer._spool_max_rows = 3
    store.down = True
    results = [writer.insert_llm_request(_llm(n=n)) for n in range(5)]
    assert [r is not None for r in results] == [True, True, True, False, False]
    assert writer.ensure_ready() is False  # full: do not build more rows
    store.down = False
    writer.flush(1.0)
    assert [r["metrics"]["n"] for r in store.of("llm")] == [0, 1, 2]
    stats = writer.job_stats(JOB)
    assert stats["rows"]["llm"] == {"written": 0, "replayed": 3, "dropped": 2}


def test_flush_gives_up_after_its_timeout(writer, store):
    store.down = True
    writer.insert_llm_request(_llm())
    assert writer.flush(0.05) is False
    assert writer.spooled == 1


# ---------------------------------------------------------------------------
# What a job is told
# ---------------------------------------------------------------------------


def test_job_stats_are_per_job(writer, store):
    writer.insert_llm_request(_llm(JOB))
    pre_id = writer.insert_audit_pre(_pre(JOB))
    writer.insert_audit_post(pre_id, {"tool": {}}, 1)
    store.down = True
    writer.insert_llm_request(_llm(OTHER_JOB))

    mine = writer.job_stats(JOB)
    assert mine["rows"] == {
        "llm": {"written": 1, "replayed": 0, "dropped": 0},
        "pre": {"written": 1, "replayed": 0, "dropped": 0},
        "post": {"written": 1, "replayed": 0, "dropped": 0},
    }
    assert mine["complete"] is True and mine["spooled"] == 0 and mine["outages"] == 0

    theirs = writer.job_stats(OTHER_JOB)
    assert theirs["spooled"] == 1 and theirs["complete"] is False
    assert theirs["outages"] == 1
    assert "refused" in theirs["last_error"]

    store.down = False
    writer.flush(1.0)
    theirs = writer.job_stats(OTHER_JOB, reset=True)
    assert theirs["complete"] is True and theirs["replayed"] == 1
    assert writer.job_stats(OTHER_JOB)["rows"] == {}


def test_the_archiver_keeps_archiving_through_an_outage(writer, store):
    """End to end over the archiver: nothing is skipped while the store is
    down, and the rows are linked once it is back."""
    archiver = LLMArchiver(writer=writer)
    store.down = True
    request_id = archiver.archive(
        JOB, "universal", [HumanMessage("q")], AIMessage("a"), "m", iteration=1
    )
    audit_id = archiver.audit_tool_call(
        job_id=JOB,
        agent_type="universal",
        iteration=1,
        tool_name="read_file",
        call_id="c1",
        arguments={"path": "a.md"},
    )
    assert request_id is not None and audit_id is not None
    assert archiver.update_tool_result(audit_id, "contents", True, 3) is True

    store.down = False
    assert writer.flush(1.0) is True
    assert [r["kind"] for r in store.rows] == ["llm", "chat", "pre", "post"]
    assert store.of("chat")[0]["request_id"] == store.of("llm")[0]["id"]
    assert store.of("post")[0]["pre_id"] == store.of("pre")[0]["id"]
    assert writer.job_stats(JOB)["complete"] is True


# ---------------------------------------------------------------------------
# The receipt a job leaves
# ---------------------------------------------------------------------------


def _receipts(store, job: str = JOB) -> List[Dict[str, Any]]:
    return [
        r["payload"]["audit_receipt"]
        for r in store.of("pre")
        if r.get("step_type") == "audit_receipt" and r["job_id"] == job
    ]


def _run_a_step(archiver, job: str = JOB) -> None:
    archiver.archive(job, "universal", [HumanMessage("q")], AIMessage("a"), "m")
    audit_id = archiver.audit_tool_call(
        job_id=job,
        agent_type="universal",
        iteration=1,
        tool_name="read_file",
        call_id="c1",
        arguments={},
    )
    archiver.update_tool_result(audit_id, "ok", True, 1)


def test_a_clean_job_leaves_a_complete_receipt(writer, store):
    archiver = LLMArchiver(writer=writer)
    _run_a_step(archiver)
    stats = archiver.close_job_record(JOB, "universal", flush_timeout_s=0.1)

    assert stats["complete"] is True
    (receipt,) = _receipts(store)
    assert receipt["complete"] is True
    assert receipt["dropped"] == 0 and receipt["spooled"] == 0
    assert receipt["rows"] == {
        "llm": {"written": 1, "replayed": 0, "dropped": 0},
        "chat": {"written": 1, "replayed": 0, "dropped": 0},
        "pre": {"written": 1, "replayed": 0, "dropped": 0},
        "post": {"written": 1, "replayed": 0, "dropped": 0},
    }
    # The receipt row is not one of the rows it counts.
    row = next(r for r in store.of("pre") if r["step_type"] == "audit_receipt")
    assert row["node_name"] == "job_end" and "closed_at" in row["payload"]
    assert len(store.of("llm")) == receipt["rows"]["llm"]["written"]


def test_a_job_that_wrote_nothing_leaves_no_receipt(writer, store):
    archiver = LLMArchiver(writer=writer)
    assert archiver.close_job_record(JOB, "universal", flush_timeout_s=0.1) is None
    assert store.rows == []


def test_each_piece_of_a_job_gets_its_own_receipt(writer, store):
    archiver = LLMArchiver(writer=writer)
    _run_a_step(archiver)
    archiver.close_job_record(JOB, "universal", flush_timeout_s=0.1)
    _run_a_step(archiver)
    _run_a_step(archiver)
    archiver.close_job_record(JOB, "universal", flush_timeout_s=0.1)

    first, second = _receipts(store)
    assert first["rows"]["llm"]["written"] == 1
    assert second["rows"]["llm"]["written"] == 2
    assert len(store.of("llm")) == 3


def test_a_receipt_written_after_the_outage_counts_what_arrived(writer, store):
    """The store is down when the job ends and comes back later (a stateless
    executor lives on): the receipt reaches the store last, with the counts as
    they are then, not as they were at the close."""
    archiver = LLMArchiver(writer=writer)
    store.down = True
    _run_a_step(archiver)
    at_close = archiver.close_job_record(JOB, "universal", flush_timeout_s=0.05)
    assert at_close["complete"] is False and at_close["spooled"] == 4
    assert store.rows == []

    store.down = False
    assert writer.flush(1.0) is True
    (receipt,) = _receipts(store)
    assert receipt["complete"] is True and receipt["spooled"] == 0
    assert receipt["replayed"] == 4 and receipt["outages"] == 1
    assert receipt["rows"]["llm"] == {"written": 0, "replayed": 1, "dropped": 0}
    # Everything the receipt counts is in the store, before the receipt.
    assert [r["kind"] for r in store.rows] == ["llm", "chat", "pre", "post", "pre"]
    assert writer.job_stats(JOB)["rows"] == {}


def test_a_receipt_reports_dropped_rows(writer, store):
    archiver = LLMArchiver(writer=writer)
    store.reject["llm"] = PgError("22P02")
    _run_a_step(archiver)
    stats = archiver.close_job_record(JOB, "universal", flush_timeout_s=0.1)

    assert stats["complete"] is False
    (receipt,) = _receipts(store)
    assert receipt["complete"] is False and receipt["dropped"] == 1
    assert receipt["rows"]["llm"] == {"written": 0, "replayed": 0, "dropped": 1}
    assert "22P02" in receipt["last_error"]


def test_a_writer_without_receipts_is_left_alone():
    """An injected sink (tests, alternates) need not implement the receipt."""
    from tests.test_archiver_pg import FakeWriter

    archiver = LLMArchiver(writer=FakeWriter())
    assert archiver.close_job_record(JOB, "universal") is None
