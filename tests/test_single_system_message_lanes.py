"""A single-system-message family gets one leading system message in every lane.

``single_system_message`` (model_config_matrix.yaml: ``qwen3.8-27b``,
``qwen3.8-max``) runs every request of ``ReasoningChatOpenAI`` through
``fold_system_messages`` (reasoning_chat.py), because the Qwen3.5+ templates
raise on a system message that is not first. The fold merges a leading run of
system messages into one and turns a later one into a user turn where it
stands. Both change what the model sees: a merge rewrites message 0, which the
whole cached prefix hangs on, and a role flip on Qwen3.5/3.6 moves the template's
"last user query", so the tool loop's reasoning is rendered differently.

Since append-only context injection, nothing SRW sends should need the fold:
context entries ride user and tool carriers, the compaction summary is a marked
user message (compaction WP9) and the legacy injection path is gone (WP6).
These tests drive each lane through the client the loader builds for
``qwen3.8-27b``, behind the prefix harness's fake provider, record every message
list that reaches the fold, and require exactly one system message, first, so
the fold changes nothing:

- worker: the execute node's tool loop and its Layer-1 safety and Layer-0
  emergency rebuilds, with the tools node's progress nudge (a SystemMessage
  in state) appended after every tool round;
- session: two live turns, and the restored probe turn of the restore-fidelity
  scenarios, including Path A after a compaction (the summary row);
- subagent: a fresh child, and fork children seeded from a session parent
  (whose durable list starts with its own prompt) and from a worker parent
  (whose state holds a nudge);
- auxiliary: a summarizer pass (``AuxiliaryLLM.complete``).

Not covered on purpose: a checkpoint from before WP9 holds its summary as a
``[Summary of prior work]`` SystemMessage. The worker seats it right after the
prompt and the fold merges it into message 0; that is the case the fold is for.
"""

from __future__ import annotations

import copy
import time
from typing import Any, Dict, List, Optional

import pytest
from langchain_core.messages import (
    AIMessage,
    BaseMessage,
    HumanMessage,
    SystemMessage,
    ToolMessage,
)

from shared.runtime.llm import reasoning_chat
from shared.runtime.llm.reasoning_chat import fold_system_messages
from tests import _prompt_cache_prefix_harness as harness
from tests import test_session_restore_wire_fidelity as fidelity
from tests import test_subagent_driver as driver_cases

# langchain-openai warns about SRW's `reasoning_effort` in model_kwargs on
# every Chat Completions client this module builds; it is not under test here.
pytestmark = pytest.mark.filterwarnings(
    "ignore:Parameters .*reasoning_effort.* should be specified explicitly:UserWarning"
)

#: Qwen3.8-27B on a self-hosted OpenAI-compatible server (vLLM).
QWEN = harness.Family(
    "qwen3.8-27b",
    "openai_chat",
    "qwen3.8-27b",
    "openai",
    harness.VLLM_URL,
    None,
    True,
)
QWEN_CLIENT = "chat-qwen3.8"
PARENT_PROMPT = "You are the parent session under test. PARENT-PROMPT-MARKER"


@pytest.fixture
def fold_inputs(monkeypatch: Any) -> List[List[Dict[str, Any]]]:
    """Every message list that reaches ``fold_system_messages``, as it arrived."""
    seen: List[List[Dict[str, Any]]] = []
    real = reasoning_chat.fold_system_messages

    def recording(messages: list) -> list:
        seen.append(copy.deepcopy(messages))
        return real(messages)

    monkeypatch.setattr(reasoning_chat, "fold_system_messages", recording)
    return seen


def assert_one_leading_system_message(
    seen: List[List[Dict[str, Any]]], requests: Optional[int] = None
) -> None:
    """Each request has exactly one system message, first: the fold is a no-op.

    ``requests`` (when known) is the number of requests the lane sent; the fold
    must have seen each, which also proves the family arms it.
    """
    assert seen, "no request reached the fold"
    if requests is not None:
        assert len(seen) == requests, f"the fold saw {len(seen)} of {requests}"
    for k, messages in enumerate(seen):
        roles = [m.get("role") for m in messages]
        assert roles[:1] == ["system"] and roles.count("system") == 1, (
            f"request {k} reaches the fold with roles {roles}"
        )
        assert fold_system_messages(messages) == messages


def test_the_family_arms_the_fold():
    from shared.runtime.core.loader import create_llm

    assert create_llm(QWEN.llm_config()).single_system_message is True


# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "scenario",
    ["worker-tool-loop", "worker-safety-rebuild", "worker-emergency-rebuild"],
)
async def test_worker_requests(scenario, tmp_path, monkeypatch, fold_inputs):
    spec = dict(harness.SCENARIOS[scenario])
    assert spec.pop("kind") == "worker"
    requests = await harness.run_worker_scenario(
        QWEN,
        sources=harness.VARIANTS["injected"],
        workdir=tmp_path,
        monkeypatch=monkeypatch,
        nudges=True,
        **spec,
    )
    assert_one_leading_system_message(fold_inputs, len(requests))
    # The nudges were in state from the first tool round on; no request
    # carries one (the execute node sends no SystemMessage but the prompt).
    assert len(requests) > 1
    assert not any(
        harness.text_occurrences(r.body, harness.PROGRESS_NUDGE) for r in requests
    )


# ---------------------------------------------------------------------------
# Session
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_session_requests(monkeypatch, fold_inputs):
    requests = await harness.run_session_scenario(
        QWEN, sources=harness.VARIANTS["injected"], monkeypatch=monkeypatch
    )
    assert_one_leading_system_message(fold_inputs, len(requests))


@pytest.mark.asyncio
@pytest.mark.parametrize("scenario", ["compaction", "event-row", "reasoning"])
async def test_session_restore_requests(scenario, monkeypatch, fold_inputs):
    """Live turns, then a restore from the stored rows and the probe turn."""
    from shared.runtime.core.message_markers import is_compaction_summary

    monkeypatch.setitem(harness.FAMILIES, QWEN.id, QWEN)
    monkeypatch.setitem(fidelity.CLIENTS, QWEN_CLIENT, QWEN.id)
    probe = await fidelity.run_case(scenario, QWEN_CLIENT, monkeypatch)

    script = fidelity.SCENARIOS[scenario].script()
    assert_one_leading_system_message(fold_inputs, len(script) + 1)
    # The restore itself yields no SystemMessage: the loop adds the prompt.
    assert not any(isinstance(m, SystemMessage) for m in probe.restored_messages)
    if scenario == "compaction":
        # Path A: the history restarts at the summary hand-back, a user message.
        assert is_compaction_summary(probe.restored_messages[0])
        assert "## Objective" in probe.restored_raw


# ---------------------------------------------------------------------------
# Subagents
# ---------------------------------------------------------------------------


async def _run_child(
    tmp_path: Any, monkeypatch: Any, messages: Optional[List[BaseMessage]]
) -> List[harness.Captured]:
    """One brief through ``SubagentDriver`` on the real qwen3.8 client."""
    from agent.subagents import (
        ChildBudgets,
        RecordingLedger,
        SubagentDriver,
        build_child,
    )
    from shared.runtime.core.loader import create_llm

    provider = harness.FakeProvider(
        (harness.Step(text="STEP00 The parent was reading notes."),), QWEN
    )
    provider.install(monkeypatch)
    parent_ctx, _ = driver_cases.make_parent(tmp_path)
    host = driver_cases.make_host()
    build = await build_child(
        driver_cases.explorer_entry(),
        parent_context=parent_ctx,
        host=host,
        handle="explorer-0001",
        subagent_type="explorer",
        llm_factory=lambda _cfg, _lim: create_llm(QWEN.llm_config()),
    )
    driver = SubagentDriver(
        build,
        host=host,
        parent_context=parent_ctx,
        subagent_id="child-0001",
        budgets=ChildBudgets(50, 250_000, 2000, 300, 900),
        ledger=RecordingLedger(),
        clock=time.monotonic,
        archive_fn=lambda **_kw: None,
        watcher_poll_interval=0.01,
        messages=messages,
    )
    try:
        result = await driver.run("Report what the parent was doing.")
    finally:
        await driver.close()
    assert result.status == "completed", result
    assert result.text == "STEP00 The parent was reading notes."
    return provider.requests


def _session_parent() -> List[BaseMessage]:
    """A session's durable list: the loop keeps its own prompt at index 0
    (``run_persistent_loop``) and stamps that list as the fork source."""
    return [
        SystemMessage(content=PARENT_PROMPT),
        HumanMessage(content="What is in notes.md?"),
        AIMessage(
            content="",
            tool_calls=[
                {
                    "id": "p1",
                    "name": "read_file",
                    "args": {"path": "notes.md"},
                    "type": "tool_call",
                }
            ],
        ),
        ToolMessage(content="- item one\n- item two\n- item three", tool_call_id="p1"),
        AIMessage(content="notes.md lists three open items."),
    ]


def _worker_parent() -> List[BaseMessage]:
    """A worker's state: the tools node appended a progress nudge."""
    return [
        HumanMessage(content=harness.TASK_BRIEF),
        AIMessage(
            content="",
            tool_calls=[
                {
                    "id": "p1",
                    "name": "read_file",
                    "args": {"path": "brief.md"},
                    "type": "tool_call",
                }
            ],
        ),
        ToolMessage(content="the brief", tool_call_id="p1"),
        SystemMessage(content=harness.PROGRESS_NUDGE),
        AIMessage(content="I have read the brief."),
    ]


@pytest.mark.asyncio
async def test_fresh_subagent_requests(tmp_path, monkeypatch, fold_inputs):
    requests = await _run_child(tmp_path, monkeypatch, None)
    assert_one_leading_system_message(fold_inputs, len(requests))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "parent, history, foreign",
    [
        (_session_parent, "notes.md lists three open items.", PARENT_PROMPT),
        (_worker_parent, "I have read the brief.", harness.PROGRESS_NUDGE),
    ],
    ids=["session-parent", "worker-parent"],
)
async def test_fork_subagent_requests(
    parent, history, foreign, tmp_path, monkeypatch, fold_inputs
):
    """The child sends its own prompt and the parent's history, and neither
    the parent's prompt nor a nudge the parent's requests never carried."""
    from agent.subagents import seed_fork_history

    seed = seed_fork_history(parent(), child_model=QWEN.model, parent_model=QWEN.model)
    requests = await _run_child(tmp_path, monkeypatch, seed)
    assert_one_leading_system_message(fold_inputs, len(requests))
    assert harness.text_occurrences(requests[0].body, history) == 1
    assert not harness.text_occurrences(requests[0].body, foreign)


# ---------------------------------------------------------------------------
# Auxiliary
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_auxiliary_summarizer_request(monkeypatch, fold_inputs):
    from shared.runtime.core.loader import create_llm
    from shared.runtime.services.auxiliary import AuxiliaryLLM, SummarizeTask

    provider = harness.FakeProvider(
        (harness.Step(text="STEP00 ## Objective\nRead the brief."),), QWEN
    )
    provider.install(monkeypatch)
    aux = AuxiliaryLLM(create_llm(QWEN.llm_config()), max_context_tokens=100_000)
    reply = await aux.complete(
        SummarizeTask("user: read brief.md\nassistant: done", "Summarize.")
    )
    assert "## Objective" in str(reply.content)
    assert_one_leading_system_message(fold_inputs, len(provider.requests))
