"""Agent-side recovery of an abandoned session delegation turn (WP2).

Design: knowledge-base/knowledge/features/parallel_subagents.md §5.4–§5.7,
§6.2 and §7. The orchestrator side (the one-transaction settle and the plans
it lists) is pinned on Postgres in ``test_session_subagent_batch_settle_pg``;
the path end to end in ``test_session_subagent_batch_recovery_pg``. Here: the
turn grouping, which members the agent names and which carry text, the
fallbacks to the single-child path, the member texts, and the verdicts.
"""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

import agent.subagents.batch_recovery as batch_mod
from agent.subagents import ContextProbe
from agent.subagents.batch_recovery import (
    INTERRUPTED_HEADER,
    BatchRecoveryError,
    interrupted_envelope,
    last_assistant_text,
    tail_within,
)
from agent.subagents.envelope import count_tokens
from agent.subagents.persistence import RestoredSubagentTranscript
from tests.test_subagent_background_runtime import StrictLedger
from tests.test_subagent_runtime import make_parent, runtime_for

INPUT_ID = "11111111-1111-4111-8111-111111111111"
OTHER_INPUT_ID = "22222222-2222-4222-8222-222222222222"
SECRET = "Zx9-internal-credential-7Qp2Lm5Rt8"
REPORT = "Austria: 41,200 EUR net at 60,000 gross. Sources cited below."
LAST_WORDS = "Checked the 2026 tax tables for Germany; social charges still open."


def _uuid() -> str:
    return str(uuid4())


class BatchLedger(StrictLedger):
    """A session ledger with plans, the batch settle and a scripted verdict."""

    def __init__(self) -> None:
        super().__init__()
        self.plans: list[dict[str, Any]] | None = []
        self.settle_calls: list[dict[str, Any]] = []
        self.verdicts: list[dict[str, Any]] = []
        self.recovery_listings = 0
        self.plain_listings = 0
        self.transcripts: dict[str, list[Any]] = {}
        self.after_stale: dict[str, Any] | None = None

    async def list_live(self, parent_job_id: str) -> list[dict[str, Any]]:
        self.plain_listings += 1
        return [dict(row) for row in self.live]

    async def list_live_recovery(self, parent_thread_id: str) -> dict[str, Any]:
        self.recovery_listings += 1
        if self.recovery_listings > 1 and self.after_stale is not None:
            return self.after_stale
        return {
            "subagents": [dict(row) for row in self.live],
            "recovery_turns": self.plans,
        }

    def adopt_live(self, row: dict[str, Any]) -> bool:
        return True

    async def load_messages(self, subagent_id: str) -> RestoredSubagentTranscript:
        return RestoredSubagentTranscript(
            messages=list(self.transcripts.get(subagent_id, [])), turn_number=0
        )

    async def settle_batch(self, **request: Any) -> dict[str, Any]:
        self.settle_calls.append(request)
        verdict = self.verdicts.pop(0) if self.verdicts else {"result": "applied"}
        return {
            "parent_input_message_id": request["parent_input_message_id"],
            "parent_iteration": request["parent_iteration"],
            "supersedes_input_seq": 7,
            "delivery_id": _uuid() if verdict["result"] == "applied" else None,
            **verdict,
        }


def _child(
    handle: str,
    *,
    call_id: str,
    input_id: str = INPUT_ID,
    iteration: int = 1,
    status: str = "running",
    turns: int = 2,
    tokens: int = 300,
    report_path: str | None = None,
    subagent_type: str = "explorer",
) -> dict[str, Any]:
    ended = status not in {"queued", "running"}
    return {
        "thread_id": _uuid(),
        "runtime_generation": _uuid(),
        "parent_thread_id": "parent-job",
        "handle": handle,
        "subagent_type": subagent_type,
        "status": status,
        "thread_status": "ended" if ended else "active",
        "outcome": status if ended else None,
        "recovery_kind": "terminal_foreground" if ended else "live",
        "parent_tool_call_id": call_id,
        "parent_input_message_id": input_id,
        "parent_iteration": iteration,
        "run_in_background": False,
        "turns": turns,
        "tokens": tokens,
        "report_path": report_path,
        "started_at": "2026-09-29T10:00:00+00:00",
        "ended_at": "2026-09-29T10:01:40+00:00" if ended else None,
    }


def _call(
    index: int,
    call_class: str,
    child: dict[str, Any] | None = None,
    *,
    call_id: str | None = None,
) -> dict[str, Any]:
    live = child is not None and child["thread_status"] != "ended"
    needs_entry = child is not None and (live or call_class == "ended")
    return {
        "index": index,
        "tool_call_id": call_id or (child or {}).get("parent_tool_call_id") or _uuid(),
        "parent_ai_message_id": _uuid(),
        "class": call_class,
        "subagent_type": (child or {}).get("subagent_type") or "explorer",
        "description": f"country {index}",
        "thread_id": child["thread_id"] if child else None,
        "runtime_generation": child["runtime_generation"] if child else None,
        "handle": child["handle"] if child else None,
        "subagent_status": child["status"] if child else None,
        "outcome": child["outcome"] if child else None,
        "needs_entry": needs_entry,
        "needs_message": call_class in {"ended", "live"},
    }


def _plan(calls, *, input_id: str = INPUT_ID, iteration: int = 1, error=None):
    plan = {
        "parent_input_message_id": input_id,
        "parent_iteration": iteration,
        "delivery_id": _uuid(),
        "supersedes_input_seq": None if error else 7,
        "calls": [] if error else calls,
    }
    if error:
        plan["error"] = error
    return plan


def _recovering_runtime(tmp_path, ledger, *, contract: bool = False, probe=None):
    ctx, root = make_parent(tmp_path)
    ctx._session_subagent_batch_settle_contract = contract
    ctx.redaction_secrets = (SECRET,)
    if probe is not None:
        ctx.parent_context_probe = lambda: probe
    runtime = runtime_for(
        ctx,
        factory=lambda *_: pytest.fail("recovery constructed a provider"),
        ledger=ledger,
    )
    runtime.host.delivery_channel = "event"
    return ctx, root, runtime


def _spill(root, handle: str, text: str) -> str:
    path = f".subagents/{handle}/report.md"
    (root / ".subagents" / handle).mkdir(parents=True, exist_ok=True)
    (root / path).write_text(text)
    return path


def _four(root):
    """A batch of four: ended with a spilled report, live, two never started."""

    done = _child(
        "explorer-0000",
        call_id="call-0",
        status="completed",
        turns=5,
        tokens=1200,
        report_path=_spill(root, "explorer-0000", REPORT),
    )
    live = _child("explorer-0001", call_id="call-1")
    calls = [
        _call(0, "ended", done),
        _call(1, "live", live),
        _call(2, "not_started", call_id="call-2"),
        _call(3, "not_started", call_id="call-3"),
    ]
    return done, live, calls


# ---------------------------------------------------------------------------
# Member text
# ---------------------------------------------------------------------------


def test_interrupted_envelope_text() -> None:
    text = interrupted_envelope(
        handle="reader-0001",
        subagent_type="reader",
        turns=3,
        tokens=12345,
        tail="Germany done, Austria pending.",
        tail_cut=False,
    )
    lines = text.splitlines()
    assert (
        lines[0]
        == INTERRUPTED_HEADER
        == ("[delegate_agent: INTERRUPTED - no final report]")
    )
    assert lines[1] == "handle: reader-0001   type: reader"
    assert lines[2] == (
        "This subagent was still working when the process running this "
        "conversation was replaced. It did not finish and produced no final "
        "report. This was an infrastructure interruption, not a failure of the "
        "task, and nobody cancelled it."
    )
    assert lines[3] == "Progress before the interruption: 3 turns, 12,345 tokens."
    assert lines[4] == "Last message from the subagent (partial and unverified):"
    assert lines[5].startswith('<subagent_report handle="reader-0001"')
    assert lines[6] == "Germany done, Austria pending."
    assert lines[7] == "</subagent_report>"
    assert lines[8:] == [
        "Anything it changed in the workspace is still there and may be incomplete.",
        "If this work is still needed, first check the workspace for what it "
        "already produced. Then either finish the remainder yourself or call "
        "delegate_agent again with a task limited to what is still missing.",
    ]


def test_interrupted_envelope_without_a_message_and_with_a_report_file() -> None:
    silent = interrupted_envelope(
        handle="h", subagent_type="t", turns=0, tokens=0, tail="", tail_cut=False
    )
    assert "It left no message before the interruption." in silent
    assert "<subagent_report" not in silent

    spilled = interrupted_envelope(
        handle="h",
        subagent_type="t",
        turns=4,
        tokens=90,
        tail="final answer",
        tail_cut=True,
        report_file=".subagents/h/report.md",
    )
    assert "report file exists: .subagents/h/report.md" in spilled
    assert "read that file before you decide" in spilled
    assert "produced no final report" not in spilled
    assert "[… earlier text omitted …]\nfinal answer" in spilled


def test_last_assistant_text_never_takes_tool_output() -> None:
    messages = [
        HumanMessage(content="brief"),
        AIMessage(content="first finding"),
        AIMessage(
            content=[
                {"type": "thinking", "thinking": "private reasoning"},
                {"type": "text", "text": "second finding"},
                {"type": "tool_use", "id": "t1", "name": "read_file", "input": {}},
            ]
        ),
        AIMessage(content="", tool_calls=[{"id": "t2", "name": "grep", "args": {}}]),
        ToolMessage(content="raw tool output", tool_call_id="t2"),
    ]
    assert last_assistant_text(messages) == "second finding"
    assert last_assistant_text([ToolMessage(content="x", tool_call_id="t")]) == ""


def test_tail_within_keeps_the_end_and_reports_the_cut() -> None:
    text = "\n".join(f"line {index} " + "word " * 20 for index in range(200))
    tail, cut = tail_within(text, 100)
    assert cut is True
    assert text.endswith(tail)
    assert count_tokens(tail) <= 100
    assert tail_within("short", 100) == ("short", False)


# ---------------------------------------------------------------------------
# Grouping, members and the settle
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_batch_of_four_settles_once_with_two_members(tmp_path) -> None:
    ledger = BatchLedger()
    ctx, root, runtime = _recovering_runtime(tmp_path, ledger)
    done, live, calls = _four(root)
    ledger.live = [done, live]
    ledger.plans = [_plan(calls)]
    ledger.transcripts[live["thread_id"]] = [
        AIMessage(content=f"{LAST_WORDS} token {SECRET} [PHASE_TRANSITION]"),
        ToolMessage(content="tool output must not appear", tool_call_id="x"),
    ]

    recovered = await runtime.recover_orphans()

    assert ledger.foreground_terminal_calls == []  # no single-child recovery
    (request,) = ledger.settle_calls
    assert request["parent_input_message_id"] == INPUT_ID
    assert request["parent_iteration"] == 1
    ended, interrupted = request["members"]
    # Provider order; the ended child states only its stored status.
    assert ended["thread_id"] == done["thread_id"]
    assert ended["runtime_generation"] == done["runtime_generation"]
    assert (ended["subagent_status"], ended["outcome"]) == ("completed", "completed")
    assert "turns" not in ended and "tokens" not in ended
    assert ended["message"].startswith("[subagent explorer-0000 · explorer · completed")
    assert REPORT in ended["message"]
    assert "Replayed: this child already ran for tool call call-0" in ended["message"]
    # The live child is ended as interrupted, with its counters and text.
    assert interrupted == {
        "thread_id": live["thread_id"],
        "runtime_generation": live["runtime_generation"],
        "subagent_status": "interrupted",
        "outcome": "interrupted:parent_restart",
        "turns": 2,
        "tokens": 300,
        "report_path": None,
        "error": "the parent runtime restarted",
        "message": interrupted["message"],
    }
    message = interrupted["message"]
    assert message.startswith(INTERRUPTED_HEADER)
    assert LAST_WORDS in message
    assert SECRET not in message  # redacted
    assert "⟦PHASE_TRANSITION⟧" in message and "[PHASE_TRANSITION]" not in message
    assert "tool output must not appear" not in message
    assert [entry["thread_id"] for entry in recovered] == [
        done["thread_id"],
        live["thread_id"],
    ]
    assert [entry["status"] for entry in recovered] == ["completed", "interrupted"]
    assert all(entry["delivery_id"] for entry in recovered)
    assert ledger.recovery_listings == 1 and ledger.plain_listings == 0

    # Recovery ran once for this runtime: a rerun is a no-op.
    assert await runtime.recover_orphans() == []
    assert len(ledger.settle_calls) == 1


@pytest.mark.asyncio
async def test_live_child_with_a_durable_result_is_named_without_text(
    tmp_path,
) -> None:
    ledger = BatchLedger()
    _, _, runtime = _recovering_runtime(tmp_path, ledger)
    live = _child("explorer-0001", call_id="call-1")
    delivered = _call(1, "delivered", live)
    assert delivered["needs_entry"] and not delivered["needs_message"]
    ledger.live = [live]
    ledger.plans = [_plan([_call(0, "not_started", call_id="call-0"), delivered])]

    await runtime.recover_orphans()

    (request,) = ledger.settle_calls
    (member,) = request["members"]
    assert "message" not in member
    assert (member["subagent_status"], member["outcome"]) == (
        "interrupted",
        "interrupted:parent_restart",
    )


@pytest.mark.asyncio
async def test_a_live_child_whose_report_was_spilled_points_at_the_file(
    tmp_path,
) -> None:
    ledger = BatchLedger()
    _, root, runtime = _recovering_runtime(tmp_path, ledger)
    live = _child("explorer-0001", call_id="call-1")
    path = _spill(root, "explorer-0001", "the finished report")
    ledger.live = [live]
    ledger.plans = [
        _plan([_call(0, "live", live), _call(1, "not_started", call_id="c")])
    ]
    ledger.transcripts[live["thread_id"]] = [AIMessage(content="the finished report")]

    await runtime.recover_orphans()

    (member,) = ledger.settle_calls[0]["members"]
    assert member["report_path"] == path
    assert f"report file exists: {path}" in member["message"]


@pytest.mark.asyncio
async def test_partial_approval_still_settles_the_turn_as_a_batch(tmp_path) -> None:
    """Two calls, one declined: the per-child path must not run (§7)."""

    ledger = BatchLedger()
    _, _, runtime = _recovering_runtime(tmp_path, ledger)
    live = _child("explorer-0001", call_id="call-1")
    ledger.live = [live]
    ledger.plans = [
        _plan([_call(0, "declined", call_id="call-0"), _call(1, "live", live)])
    ]

    await runtime.recover_orphans()

    assert len(ledger.settle_calls) == 1
    assert ledger.foreground_terminal_calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "shape",
    [
        "one_call",
        "plan_error",
        "no_plan",
        "no_plans",
        "malformed_plans",
        "no_recovery_listing",
    ],
)
async def test_fallbacks_use_the_single_child_path(tmp_path, shape) -> None:
    ledger = BatchLedger()
    if shape == "no_recovery_listing":
        ledger.list_live_recovery = None  # a ledger that cannot list plans
    _, _, runtime = _recovering_runtime(tmp_path, ledger)
    live = _child("explorer-0001", call_id="call-1")
    ledger.live = [live]
    ledger.transcripts[live["thread_id"]] = [AIMessage(content="partial")]
    batch = [_call(0, "live", live), _call(1, "not_started")]
    if shape == "one_call":
        ledger.plans = [_plan([_call(0, "live", live)])]
    elif shape == "plan_error":
        ledger.plans = [_plan([], error="foreground recovery parent input is missing")]
    elif shape == "no_plan":
        ledger.plans = [_plan(batch, input_id=OTHER_INPUT_ID)]
    elif shape == "no_plans":
        ledger.plans = None  # an orchestrator that predates plans
    elif shape == "malformed_plans":
        batch[0] = {**batch[0], "runtime_generation": "not-a-uuid"}
        ledger.plans = [_plan(batch)]
    else:
        ledger.plans = [_plan(batch)]

    recovered = await runtime.recover_orphans()

    assert ledger.settle_calls == []
    ((child_id, fields),) = ledger.foreground_terminal_calls
    assert child_id == live["thread_id"]
    assert fields["outcome"] == "interrupted:parent_restart"
    assert recovered[0]["thread_id"] == live["thread_id"]
    if shape == "no_recovery_listing":
        assert (ledger.recovery_listings, ledger.plain_listings) == (0, 1)


@pytest.mark.asyncio
@pytest.mark.parametrize("contract", [True, False])
async def test_plans_not_the_attach_capability_decide_the_batch(
    tmp_path, contract
) -> None:
    """A pinned pod that attached itself, or the pinned VM path, may lack the
    attach advertisement and still inherit a multi-call turn. The listed plans
    prove the orchestrator can settle it; the flag only gates new batches."""

    ledger = BatchLedger()
    _, _, runtime = _recovering_runtime(tmp_path, ledger, contract=contract)
    live = _child("explorer-0001", call_id="call-1")
    ledger.live = [live]
    ledger.plans = [
        _plan([_call(0, "declined", call_id="call-0"), _call(1, "live", live)])
    ]

    await runtime.recover_orphans()

    assert len(ledger.settle_calls) == 1
    assert ledger.foreground_terminal_calls == []
    assert (ledger.recovery_listings, ledger.plain_listings) == (1, 0)


@pytest.mark.asyncio
async def test_a_listed_child_the_settle_did_not_name_recovers_on_its_own(
    tmp_path, caplog
) -> None:
    ledger = BatchLedger()
    _, root, runtime = _recovering_runtime(tmp_path, ledger)
    done, live, calls = _four(root)
    stray = _child("explorer-0007", call_id="call-7")  # same turn, not in the plan
    ledger.live = [done, stray, live]
    ledger.plans = [_plan(calls)]
    ledger.transcripts[stray["thread_id"]] = [AIMessage(content="stray")]

    recovered = await runtime.recover_orphans()

    assert len(ledger.settle_calls) == 1
    assert [child for child, _ in ledger.foreground_terminal_calls] == [
        stray["thread_id"]
    ]
    assert {entry["thread_id"] for entry in recovered} == {
        done["thread_id"],
        live["thread_id"],
        stray["thread_id"],
    }
    assert "not a member of its settled plan" in caplog.text


@pytest.mark.asyncio
async def test_turns_are_settled_or_recovered_each_on_their_own(tmp_path) -> None:
    ledger = BatchLedger()
    _, root, runtime = _recovering_runtime(tmp_path, ledger)
    done, live, calls = _four(root)
    single = _child(
        "explorer-0009", call_id="call-9", input_id=OTHER_INPUT_ID, iteration=2
    )
    ledger.live = [done, single, live]
    ledger.plans = [
        _plan(calls),
        _plan([_call(0, "live", single)], input_id=OTHER_INPUT_ID, iteration=2),
    ]

    recovered = await runtime.recover_orphans()

    assert len(ledger.settle_calls) == 1
    assert [child for child, _ in ledger.foreground_terminal_calls] == [
        single["thread_id"]
    ]
    assert [entry["thread_id"] for entry in recovered] == [
        done["thread_id"],
        live["thread_id"],
        single["thread_id"],
    ]


@pytest.mark.asyncio
async def test_budget_is_split_by_the_members_that_carry_text(
    tmp_path, monkeypatch
) -> None:
    probe = ContextProbe(
        last_provider_input_tokens=None,
        current_token_count=10_000,
        compaction_threshold_tokens=110_000,
        model_max_context_tokens=128_000,
    )
    ledger = BatchLedger()
    _, root, runtime = _recovering_runtime(tmp_path, ledger, probe=probe)
    done, live, calls = _four(root)
    # A live child whose result is already durable is a member without text:
    # it must not count in the divisor.
    settled_live = _child("explorer-0005", call_id="call-5")
    calls.append(_call(4, "delivered", settled_live))
    ledger.live = [done, live, settled_live]
    ledger.plans = [_plan(calls)]
    ledger.transcripts[live["thread_id"]] = [AIMessage(content="x " * 30_000)]
    seen: list[tuple[str, Any]] = []
    replay = batch_mod.render_replay_envelope
    budget = batch_mod.return_budget

    def spy_replay(row, **kwargs):
        seen.append(("replay", kwargs["n_in_batch"], kwargs["probe"]))
        return replay(row, **kwargs)

    def spy_budget(entry_budget, used_probe, n_in_batch=1):
        seen.append(("budget", n_in_batch, used_probe))
        return budget(entry_budget, used_probe, n_in_batch)

    monkeypatch.setattr(batch_mod, "render_replay_envelope", spy_replay)
    monkeypatch.setattr(batch_mod, "return_budget", spy_budget)

    await runtime.recover_orphans()

    # Three members, two carry text (the never-started calls are rendered by
    # the server, the durable one needs none), whatever the runtime's
    # process-local batch size says.
    assert len(ledger.settle_calls[0]["members"]) == 3
    assert runtime.batch_size == 1
    assert seen == [("replay", 2, probe), ("budget", 2, probe)]
    # explorer: entry 2000 tokens; headroom 100k shared by 2 → 25k → 2000;
    # the interrupted tail gets a quarter of it.
    message = ledger.settle_calls[0]["members"][1]["message"]
    tail = message.split("[… earlier text omitted …]\n", 1)[1]
    tail = tail.split("\n</subagent_report>", 1)[0]
    assert tail.strip() and set(tail.split()) == {"x"}
    assert count_tokens(tail) <= 2000 * batch_mod.INTERRUPTED_TAIL_SHARE


# ---------------------------------------------------------------------------
# Verdicts
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_already_delivered_closes_members_without_a_continuation(
    tmp_path,
) -> None:
    ledger = BatchLedger()
    _, root, runtime = _recovering_runtime(tmp_path, ledger)
    done, live, calls = _four(root)
    ledger.live = [done, live]
    ledger.plans = [_plan(calls)]
    ledger.verdicts = [{"result": "already_delivered"}]

    recovered = await runtime.recover_orphans()

    assert [entry["delivery_id"] for entry in recovered] == [None, None]
    assert ledger.foreground_terminal_calls == []


@pytest.mark.asyncio
async def test_idempotent_with_owed_children_finishes_them_one_at_a_time(
    tmp_path,
) -> None:
    """Mixed versions: an older agent recovered one member per child and wrote
    the turn's continuation, then died. The settle answers ``idempotent`` and
    can write nothing more; the members still listed are closed on the
    single-child path, so they leave the live list and stop blocking rewind."""

    ledger = BatchLedger()
    _, root, runtime = _recovering_runtime(tmp_path, ledger)
    done, live, calls = _four(root)
    calls[0]["class"] = "delivered"  # the older agent's recovered member
    calls[0]["needs_entry"] = calls[0]["needs_message"] = False
    other = _child(
        "explorer-0002", call_id="call-2", status="completed", report_path=None
    )
    calls[2] = _call(2, "ended", other)
    ledger.live = [live, other]
    ledger.plans = [_plan(calls)]
    ledger.transcripts[live["thread_id"]] = [AIMessage(content="partial")]
    ledger.verdicts = [{"result": "idempotent", "delivery_id": _uuid()}]

    recovered = await runtime.recover_orphans()

    assert len(ledger.settle_calls) == 1
    assert [child for child, _ in ledger.foreground_terminal_calls] == [
        live["thread_id"],
        other["thread_id"],
    ]
    assert {entry["thread_id"] for entry in recovered} == {
        live["thread_id"],
        other["thread_id"],
    }


@pytest.mark.asyncio
async def test_a_stale_settle_lists_again_and_retries_once(tmp_path) -> None:
    ledger = BatchLedger()
    _, root, runtime = _recovering_runtime(tmp_path, ledger)
    done, live, calls = _four(root)
    ledger.live = [done, live]
    ledger.plans = [_plan(calls)]
    ledger.verdicts = [{"result": "stale", "reason": "generation_differs"}]

    recovered = await runtime.recover_orphans()

    assert ledger.recovery_listings == 2
    assert len(ledger.settle_calls) == 2
    assert ledger.settle_calls[0]["members"] == ledger.settle_calls[1]["members"]
    assert len(recovered) == 2


@pytest.mark.asyncio
async def test_a_turn_that_stays_stale_leaves_recovery_incomplete(tmp_path) -> None:
    ledger = BatchLedger()
    _, root, runtime = _recovering_runtime(tmp_path, ledger)
    done, live, calls = _four(root)
    ledger.live = [done, live]
    ledger.plans = [_plan(calls)]
    ledger.verdicts = [{"result": "stale"}, {"result": "stale"}]

    with pytest.raises(BatchRecoveryError):
        await runtime.recover_orphans()

    # Nothing was marked complete: the next attach runs recovery again.
    ledger.verdicts = []
    recovered = await runtime.recover_orphans()
    assert len(recovered) == 2
    assert len(ledger.settle_calls) == 3


@pytest.mark.asyncio
async def test_a_turn_already_settled_when_listed_again_is_left_alone(
    tmp_path,
) -> None:
    ledger = BatchLedger()
    _, root, runtime = _recovering_runtime(tmp_path, ledger)
    done, live, calls = _four(root)
    ledger.live = [done, live]
    ledger.plans = [_plan(calls)]
    ledger.verdicts = [{"result": "stale"}]
    ledger.after_stale = {"subagents": [], "recovery_turns": []}

    assert await runtime.recover_orphans() == []
    assert len(ledger.settle_calls) == 1
    assert ledger.foreground_terminal_calls == []


# ---------------------------------------------------------------------------
# The orchestrator client
# ---------------------------------------------------------------------------


PARENT = "aaaaaaaa-1111-4222-8333-444444444444"


def _client():
    from unittest.mock import AsyncMock, MagicMock

    from agent.api.orchestrator_client import OrchestratorClient

    client = OrchestratorClient(
        orchestrator_url="http://orchestrator:8085",
        pod_ip="10.0.0.5",
        pod_port=8002,
        hostname="session-agent",
        config_name="interactive",
    )
    client._client = MagicMock()
    client._client.post = AsyncMock()
    return client


def _authority():
    from shared.session_subagent_authority import SessionParentAuthority

    return SessionParentAuthority(
        execution_lane="stateless",
        parent_thread_id=PARENT,
        lease_token=3,
        executor_id="worker-1",
        executor_pod_uid="pod-uid",
    )


def _response(status: int, payload: dict):
    from unittest.mock import MagicMock

    response = MagicMock(status_code=status)
    response.json.return_value = payload
    return response


@pytest.mark.asyncio
async def test_client_lists_plans_and_tolerates_an_orchestrator_without_them():
    client = _client()
    row = {
        "thread_id": _uuid(),
        "runtime_generation": _uuid(),
        "parent_thread_id": PARENT,
        "parent_job_id": None,
    }
    plan = _plan([])
    client._client.post.return_value = _response(
        200, {"subagents": [row], "recovery_turns": [plan]}
    )
    listed = await client.list_live_session_subagent_recovery(
        PARENT, parent_authority=_authority()
    )
    assert listed["recovery_turns"] == [plan]
    assert [r["thread_id"] for r in listed["subagents"]] == [row["thread_id"]]
    url = client._client.post.await_args.args[0]
    assert url.endswith(f"/api/agents/threads/{PARENT}/subagents/live")

    client._client.post.return_value = _response(200, {"subagents": [row]})
    listed = await client.list_live_session_subagent_recovery(
        PARENT, parent_authority=_authority()
    )
    assert listed["recovery_turns"] is None
    # Malformed plans never fail the listing: they read as none.
    for malformed in ({"turn": 1}, ["not a plan"], "plans"):
        client._client.post.return_value = _response(
            200, {"subagents": [row], "recovery_turns": malformed}
        )
        listed = await client.list_live_session_subagent_recovery(
            PARENT, parent_authority=_authority()
        )
        assert listed["recovery_turns"] is None
        assert [r["thread_id"] for r in listed["subagents"]] == [row["thread_id"]]
    assert (
        await client.list_live_session_subagent_threads(
            PARENT, parent_authority=_authority()
        )
        == listed["subagents"]
    )


@pytest.mark.asyncio
async def test_client_settle_request_and_verdicts():
    from agent.api.orchestrator_client import SubagentPersistenceError
    from shared.session_subagent_authority import SessionParentAuthorityRefused
    from shared.session_subagent_batch import session_subagent_batch_delivery_id

    client = _client()
    member = {"thread_id": _uuid(), "runtime_generation": _uuid(), "message": "m"}
    identity = {"parent_input_message_id": INPUT_ID, "parent_iteration": 1}
    ours = str(session_subagent_batch_delivery_id(PARENT, INPUT_ID))

    async def settle():
        return await client.settle_session_subagent_batch(
            PARENT,
            parent_authority=_authority(),
            parent_input_message_id=INPUT_ID,
            parent_iteration=1,
            members=[member],
        )

    client._client.post.return_value = _response(
        200,
        {
            "result": "applied",
            **identity,
            "delivery_id": ours,
            "delivery_state": "queued",
        },
    )
    assert (await settle())["result"] == "applied"
    (url,) = client._client.post.await_args.args
    body = client._client.post.await_args.kwargs["json"]
    assert url.endswith(f"/api/agents/threads/{PARENT}/subagents/settle-batch")
    assert body == {
        "parent_authority": _authority().to_wire(),
        **identity,
        "members": [member],
    }

    # A continuation id this settle did not derive is refused ...
    client._client.post.return_value = _response(
        200,
        {
            "result": "applied",
            **identity,
            "delivery_id": _uuid(),
            "delivery_state": "q",
        },
    )
    with pytest.raises(SubagentPersistenceError):
        await settle()
    # ... unless it is an existing one (idempotent: maybe a single-child one).
    client._client.post.return_value = _response(
        200,
        {
            "result": "idempotent",
            **identity,
            "delivery_id": _uuid(),
            "delivery_state": "queued",
        },
    )
    assert (await settle())["result"] == "idempotent"
    # A verdict for another turn is refused.
    client._client.post.return_value = _response(
        200, {"result": "already_delivered", **identity, "parent_iteration": 2}
    )
    with pytest.raises(SubagentPersistenceError):
        await settle()
    # 409 stale hands back the server's view; nothing was written.
    stale = {"result": "stale", "reason": "members_differ", **identity, "calls": []}
    client._client.post.return_value = _response(409, {"detail": stale})
    assert await settle() == stale
    # 409 authority stays typed; any other 409 or error is a persistence error.
    client._client.post.return_value = _response(
        409,
        {"detail": {"code": SessionParentAuthorityRefused.code, "reason": "stale"}},
    )
    with pytest.raises(SessionParentAuthorityRefused):
        await settle()
    for status, payload in (
        (409, {"detail": {"code": "subagent_delivery_conflict"}}),
        (400, {"detail": "a delivered session child needs a message"}),
        (500, {"detail": "boom"}),
    ):
        client._client.post.return_value = _response(status, payload)
        with pytest.raises(SubagentPersistenceError):
            await settle()


# ---------------------------------------------------------------------------
# Attach order (F15)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_attach_recovers_children_before_it_restores_the_transcript():
    """A batch settle writes tool results into the transcript, so restore runs
    after recovery and loads them beside their calls. Both run with the input
    queue closed: no input reaches the loop until both are done."""

    import asyncio
    from pathlib import Path
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, MagicMock, patch

    import agent.api.persistent_app as mod

    order: list[tuple[str, Any]] = []
    built: list[dict[str, Any]] = []

    class FakeSession:
        def __init__(self, *args, **kwargs):
            built.append(kwargs)
            self.cloud_mount_manager = None
            self.cloud_mount_error = None
            self.workspace_manager = SimpleNamespace(
                path=Path("/workspace"), backend=MagicMock()
            )
            self.workspace_sync = None
            self.postgres_conn = None
            self.tool_context = None

        async def setup(self, **kwargs):
            return None

        async def recover_subagents(self):
            order.append(("recover", mod._session_input.queue))

    async def restore():
        order.append(("restore", mod._session_input.queue))

    workspace_override = {"remote": {"host": "10.42.0.10"}}
    fake_agent = SimpleNamespace(
        config=object(),
        _tactical_llm=None,
        _llm=object(),
        _auxiliary_llm=object(),
        postgres_conn=None,
        vector_conn=None,
    )
    fake_orchestrator = SimpleNamespace(
        get_thread_workspace=AsyncMock(return_value=workspace_override)
    )
    mod._session = None
    mod._session_identity._thread_id = None
    with (
        patch.object(mod, "_agent", fake_agent),
        patch.object(mod, "_orchestrator_client", fake_orchestrator),
        patch.object(mod, "PersistentSession", FakeSession),
        patch.object(
            mod, "_poll_workspace_ready", new=AsyncMock(return_value=workspace_override)
        ),
        patch.object(mod, "_build_sync_coordinator"),
        patch.object(mod, "_restore_session_messages", new=restore),
        patch.object(mod, "_update_thread_status", new=AsyncMock()),
        patch.object(mod, "_start_watchdogs"),
    ):
        try:
            # The capability as the stateless executor passes it from the
            # claim bundle (``turn_executor.claim_bundle_attach``).
            await mod._attach_session(
                "thread-1", session_subagent_batch_settle_contract=1
            )
            queue_after_attach = mod._session_input.queue
        finally:
            mod._session = None
            mod._session_identity._thread_id = None
            mod._session_input.teardown()

    assert order == [("recover", None), ("restore", None)]
    assert isinstance(queue_after_attach, asyncio.Queue)
    # The session publishes it on the tool context that recovery reads
    # (``_session_subagent_batch_settle_contract``) before tools load.
    (session_kwargs,) = built
    assert session_kwargs["subagent_batch_settle_contract"] is True


# ---------------------------------------------------------------------------
# Pinned: the continuation's turn number after it ran (invariant 12)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("continuation_turn", [1, 2])
def test_pinned_restore_order_holds_when_the_continuation_is_renumbered(
    continuation_turn,
) -> None:
    """On the pinned lane the loop's turn-start re-save rewrites the
    continuation's ``turn_number`` from the abandoned turn (1) to the turn that
    ran it (2). Restore orders by the admitted turn first, which is 2 either
    way, so the order is the same with or without the rewrite: each result
    behind its call, the continuation and its answer, then the input typed
    during the batch (hinted 2, run as 3)."""

    from agent.api.persistent_app import _db_rows_to_lc_messages
    from tests.test_restore_conversation_order import (
        _assert_results_follow_calls,
        _ids,
        _row,
    )

    rows = [
        _row("q1", "human", 1, admitted=1),
        _row("call", "ai", 1, calls=("c1", "c2")),
        _row("typed", "human", 3, admitted=3),
        _row("r1", "tool", 1, call_id="c1"),
        _row("r2", "tool", 1, call_id="c2"),
        _row("continuation", "event", continuation_turn, admitted=2),
        _row("continuation-answer", "ai", 2),
        _row("typed-answer", "ai", 3),
    ]

    restored = _db_rows_to_lc_messages(rows)

    assert _ids(restored) == [
        "q1",
        "call",
        "r1",
        "r2",
        "continuation",
        "continuation-answer",
        "typed",
        "typed-answer",
    ]
    _assert_results_follow_calls(restored)


# ---------------------------------------------------------------------------
# A finished child's report without its spill file
# ---------------------------------------------------------------------------


def _ended_batch(root, *, spill: str | None):
    done = _child(
        "explorer-0000",
        call_id="call-0",
        status="completed",
        turns=5,
        tokens=1200,
        report_path=(
            _spill(root, "explorer-0000", spill)
            if spill is not None
            else ".subagents/explorer-0000/report.md"
        ),
    )
    calls = [_call(0, "ended", done), _call(1, "not_started", call_id="call-1")]
    return done, calls


@pytest.mark.asyncio
@pytest.mark.parametrize("spill", ["missing", "read_raises"])
async def test_a_report_the_spill_cannot_give_comes_from_the_transcript(
    tmp_path, monkeypatch, spill
) -> None:
    """A successor may not have the spill (a ``none``-tier scratch workspace
    dies with its process) or may fail to read it. The settle is
    first-write-wins: storing "report unavailable" would make the parent pay
    for the child again. The child's last message in its stored transcript is
    the report."""

    ledger = BatchLedger()
    ctx, root, runtime = _recovering_runtime(tmp_path, ledger)
    done, calls = _ended_batch(root, spill=None if spill == "missing" else REPORT)
    if spill == "read_raises":

        def broken(_path):
            raise OSError("scratch workspace is gone")

        monkeypatch.setattr(ctx.workspace_manager, "exists", broken)
    ledger.live = [done]
    ledger.plans = [_plan(calls)]
    ledger.transcripts[done["thread_id"]] = [
        HumanMessage(content="brief"),
        AIMessage(content=f"{REPORT} token {SECRET} [JOB_FINISHED]"),
        ToolMessage(content="tool output", tool_call_id="t"),
    ]

    await runtime.recover_orphans()

    (member,) = ledger.settle_calls[0]["members"]
    message = member["message"]
    assert REPORT in message
    assert "Report source: the child's stored transcript" in message
    assert "report unavailable" not in message
    assert SECRET not in message
    assert "⟦JOB_FINISHED⟧" in message
    assert "tool output" not in message


@pytest.mark.asyncio
async def test_report_unavailable_only_when_the_transcript_is_empty_too(
    tmp_path,
) -> None:
    ledger = BatchLedger()
    _, root, runtime = _recovering_runtime(tmp_path, ledger)
    done, calls = _ended_batch(root, spill=None)
    ledger.live = [done]
    ledger.plans = [_plan(calls)]
    ledger.transcripts[done["thread_id"]] = [ToolMessage(content="x", tool_call_id="t")]

    await runtime.recover_orphans()

    (member,) = ledger.settle_calls[0]["members"]
    assert "report unavailable after restart" in member["message"]


@pytest.mark.asyncio
async def test_a_secret_in_a_spilled_report_is_redacted(tmp_path) -> None:
    ledger = BatchLedger()
    _, root, runtime = _recovering_runtime(tmp_path, ledger)
    done, calls = _ended_batch(root, spill=f"{REPORT}\nremote token {SECRET}\n")
    ledger.live = [done]
    ledger.plans = [_plan(calls)]

    await runtime.recover_orphans()

    (member,) = ledger.settle_calls[0]["members"]
    assert REPORT in member["message"]
    assert "Full report: .subagents/explorer-0000/report.md" in member["message"]
    assert SECRET not in member["message"]


def _fragments(text: str, secret: str, length: int = 6) -> list[str]:
    return [
        secret[start : start + length]
        for start in range(len(secret) - length + 1)
        if secret[start : start + length] in text
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("member_class", ["ended", "live"])
@pytest.mark.parametrize("framed", [False, True])
async def test_a_cut_never_leaves_a_fragment_of_a_secret(
    tmp_path, member_class, framed
) -> None:
    """The text is redacted before it is cut. Made of the secret over and
    over (bare, or framed off its period at both ends, so no cut can land on
    a copy's boundary in both), a cut falls inside one of its copies; cut
    first, a fragment would survive the later redaction of the envelope."""

    probe = ContextProbe(
        last_provider_input_tokens=None,
        current_token_count=0,
        compaction_threshold_tokens=4_000,
        model_max_context_tokens=128_000,
    )
    ledger = BatchLedger()
    _, root, runtime = _recovering_runtime(tmp_path, ledger, probe=probe)
    flood = f"Findings: {SECRET * 3_000} end." if framed else SECRET * 3_000
    if member_class == "ended":
        child, calls = _ended_batch(root, spill=flood)
    else:
        child = _child("explorer-0001", call_id="call-1")
        calls = [_call(0, "live", child), _call(1, "not_started", call_id="c")]
        ledger.transcripts[child["thread_id"]] = [AIMessage(content=flood)]
    ledger.live = [child]
    ledger.plans = [_plan(calls)]

    await runtime.recover_orphans()

    (member,) = ledger.settle_calls[0]["members"]
    assert "elided" in member["message"] or "earlier text omitted" in member["message"]
    assert _fragments(member["message"], SECRET) == []
