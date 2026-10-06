"""``SubagentDriver`` on the real ``run_persistent_loop`` (U3 WP1, plan B.3/B.4).

The U0 spike's scenarios A–F re-run through the driver (real loader config
through the roster resolver, real ``ContextManager``, real ``read_file`` /
``list_files`` over ``FilesystemTestBackend``, the scripted
``tests/_fake_chat_model.FakeChatModel``), plus the budgets (turn cap →
forced synthesis, token cap, staleness with a patched clock), the ``⚠``
placeholder and ``on_error`` classifications, the shared copy never
committing the parent tree, the null stores, the retry ceiling, audit and
metering under the parent, the sudo forward and the delivery machinery
staying unarmed.
"""

from __future__ import annotations

import asyncio
import subprocess
import time
from typing import Any, Dict, List

import pytest
from langchain_core.messages import (
    AIMessage,
    HumanMessage,
    SystemMessage,
    ToolMessage,
)
from langchain_core.tools import tool

from shared.runtime.core.message_markers import PERSIST_ROLE_KEY
from shared.runtime.core.subagent_roster import resolve_subagent_roster
from agent.core.workspace import WorkspaceManager, WorkspaceManagerConfig
from agent.persistent_graph import PermissionOutcome
from agent.subagents import (
    ChildBudgets,
    RecordingLedger,
    SimpleParentHost,
    SubagentDriver,
    build_child,
    seed_fork_history,
)
from agent.subagents.driver import PLACEHOLDER_PREFIX, SYNTH_PROMPT
from agent.subagents.driver import SubagentAuthorityLost
from agent.tools.context import ToolContext
from tests._fake_chat_model import HANG, FakeChatModel, text_turn, tool_turn
from tests._fs_backend import FilesystemTestBackend

_PARENT_LLM = {
    "model": "gpt-4o-mini",
    "provider": "openai",
    "api_key": "sk-parent-test",
    "model_max_context_tokens": 128000,
}
PARENT_TOOLS = ["read_file", "list_files", "search_files", "write_file", "edit_file"]
SECRET = "The note says the secret word is MARMALADE."


class FakeClock:
    """A monotonic clock the tests advance by hand."""

    def __init__(self, start: float = 1000.0):
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class FakeArchiver:
    """Records ``audit_tool_call`` / ``update_tool_result`` like the archiver."""

    def __init__(self):
        self.starts: List[Dict[str, Any]] = []
        self.results: List[Dict[str, Any]] = []

    def audit_tool_call(self, **kw):
        self.starts.append(kw)
        return f"audit-{len(self.starts)}"

    def update_tool_result(self, audit_doc_id, result, success, latency_ms, error=None):
        self.results.append(
            {
                "id": audit_doc_id,
                "result": result,
                "success": success,
                "latency_ms": latency_ms,
                "error": error,
            }
        )
        return True


def explorer_entry(**overrides) -> dict:
    """A resolved ``$ref: subagents/explorer`` roster entry (the real resolver)."""
    data = {
        "agent_id": "parent",
        "display_name": "Parent",
        "llm": dict(_PARENT_LLM),
        "subagents": {
            "roster": {"explorer": {"$ref": "subagents/explorer", **overrides}}
        },
    }
    return resolve_subagent_roster(data, db_refs={}, on_missing="raise")["subagents"][
        "roster"
    ]["explorer"]


def make_parent(tmp_path, *, git: bool = False):
    root = tmp_path / "ws"
    (root / "notes").mkdir(parents=True)
    (root / "notes" / "hello.md").write_text(
        "# hello\n\nthe secret word is MARMALADE\n"
    )
    ws = WorkspaceManager(
        job_id="parent-job",
        base_path=root,
        backend=FilesystemTestBackend(root),
        config=WorkspaceManagerConfig(git_versioning=git),
    )
    ws.initialize()
    ctx = ToolContext(
        workspace_manager=ws,
        config={"shell": {}},
        _job_metadata={"job_id": "parent-job", "project_id": "proj"},
        _resolved_tool_names=list(PARENT_TOOLS),
    )
    return ctx, root


@pytest.fixture
def parent(tmp_path):
    return make_parent(tmp_path)


def make_host(**kw) -> SimpleParentHost:
    return SimpleParentHost(
        job_id="parent-job",
        agent_type="developer",
        audit_metadata={"job_id": "parent-job", "config_name": "developer"},
        **kw,
    )


async def make_driver(
    parent_ctx,
    script,
    *,
    entry=None,
    budgets=None,
    host=None,
    clock=None,
    archiver=None,
    archive_fn=None,
    messages=None,
    ledger=None,
    handle="explorer-0001",
):
    fake = FakeChatModel(script)
    host = host or make_host()
    build = await build_child(
        entry or explorer_entry(),
        parent_context=parent_ctx,
        host=host,
        handle=handle,
        subagent_type="explorer",
        llm_factory=lambda cfg, lim: fake,
    )
    archived: List[Dict[str, Any]] = []

    def _record(**kw):
        archived.append(kw)

    driver = SubagentDriver(
        build,
        host=host,
        parent_context=parent_ctx,
        subagent_id="child-0001",
        budgets=budgets or ChildBudgets(50, 250_000, 2000, 300, 900),
        ledger=ledger or RecordingLedger(),
        clock=clock or time.monotonic,
        archiver=archiver,
        archive_fn=archive_fn or _record,
        watcher_poll_interval=0.01,
        messages=messages,
    )
    driver.archived = archived  # type: ignore[attr-defined]
    return driver, fake, build


def _brief(driver):
    return driver.messages[driver._brief_start :]


# ---------------------------------------------------------------------------
# Spike scenarios A / B / F
# ---------------------------------------------------------------------------


class TestScenarioABF:
    @pytest.mark.asyncio
    async def test_brief_runs_to_completion_and_the_follow_up_is_turn_two(self, parent):
        parent_ctx, _ = parent
        driver, fake, build = await make_driver(
            parent_ctx,
            [
                tool_turn("read_file", {"path": "notes/hello.md"}, "call_read_1"),
                text_turn(SECRET),
                tool_turn("list_files", {"path": "notes"}, "call_list_1"),
                text_turn("Listed. Done."),
            ],
        )
        try:
            # A — one brief: tool call → real read_file → final text.
            result = await driver.run(
                "Read notes/hello.md and tell me the secret word."
            )
            assert result.status == "completed"
            assert result.ok
            assert result.text == SECRET
            assert result.streamed_text == SECRET  # on_token accumulation == final
            assert result.turns == 2  # provider calls
            assert result.tool_calls == 1
            assert result.tokens == 100 + 8 + 120 + 12
            assert result.handle == "explorer-0001"
            assert result.subagent_type == "explorer"
            assert result.parked_call is None and result.error is None
            assert not result.partial and not result.sudo_requested
            tool_msgs = [m for m in driver.messages if isinstance(m, ToolMessage)]
            assert "MARMALADE" in str(tool_msgs[0].content)  # the REAL read_file ran
            assert isinstance(driver.messages[0], SystemMessage)
            assert "Current date:" in str(driver.messages[0].content)

            # B — a follow-up delivered as role=event runs as turn 2.
            result2 = await driver.run(
                "[parent] Now list notes/ and finish.", role="event"
            )
            assert result2.status == "completed"
            assert result2.text == "Listed. Done."
            assert driver.turn_number == 2
            carrier = next(
                m
                for m in driver.messages
                if isinstance(m, HumanMessage) and "[parent]" in str(m.content)
            )
            assert carrier.additional_kwargs[PERSIST_ROLE_KEY] == "event"
            # The second brief's counters started fresh.
            assert result2.turns == 2 and result2.tool_calls == 1
            assert len(fake.calls) == 4

            # The transcript went through the ledger, message by message.
            kinds = [type(m).__name__ for _, m, _ in driver.ledger.messages]
            assert kinds == [
                "HumanMessage",
                "AIMessage",
                "ToolMessage",
                "AIMessage",
                "HumanMessage",
                "AIMessage",
                "ToolMessage",
                "AIMessage",
            ]
            assert all(sid == "child-0001" for sid, _, _ in driver.ledger.messages)
        finally:
            # F — the stop sentinel makes the loop return cleanly.
            await driver.close()
        task = driver._loop_task
        assert task.done() and not task.cancelled() and task.exception() is None
        assert driver._watcher_task is None

    @pytest.mark.asyncio
    async def test_llm_calls_are_metered_under_the_parent_as_subagent(self, parent):
        parent_ctx, _ = parent
        driver, fake, _ = await make_driver(
            parent_ctx,
            [
                tool_turn("read_file", {"path": "notes/hello.md"}, "c1"),
                text_turn(SECRET),
            ],
        )
        try:
            await driver.run("read it")
            # Let the archive threads land.
            for _ in range(20):
                if len(driver.archived) >= 2:
                    break
                await asyncio.sleep(0.02)
        finally:
            await driver.close()
        assert len(driver.archived) == 2
        for row in driver.archived:
            assert row["job_id"] == "parent-job"
            assert row["agent_type"] == "developer"
            assert row["call_type"] == "subagent"
            aux = row["auxiliary_metadata"]
            assert aux["subagent_id"] == "child-0001"
            assert aux["subagent_handle"] == "explorer-0001"
            assert aux["subagent_type"] == "explorer"
            assert aux["turn"] == 1
            assert row["metadata"]["input_tokens"] in (100, 120)
        assert [r["auxiliary_metadata"]["provider_call"] for r in driver.archived] == [
            1,
            2,
        ]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("mode", ["legacy", "append_only"])
    async def test_the_child_reads_the_injection_mode_from_its_config(
        self, parent, mode
    ):
        """A child runs the session loop, so it reads
        ``context_management.injection_mode`` from its own config: in
        append_only mode the archive receives the unfolded request
        (``history_messages``) for the chat delta; legacy calls are unchanged.
        A child has no charter, memory, knowledge or subagent runtime, so it
        appends no entries and both requests carry the same messages."""
        parent_ctx, _ = parent
        driver, fake, build = await make_driver(
            parent_ctx,
            [
                tool_turn("read_file", {"path": "notes/hello.md"}, "c1"),
                text_turn(SECRET),
            ],
        )
        build.config.context_management.injection_mode = mode
        try:
            await driver.run("read it")
            for _ in range(20):
                if len(driver.archived) >= 2:
                    break
                await asyncio.sleep(0.02)
        finally:
            await driver.close()
        assert len(driver.archived) == 2
        for row in driver.archived:
            if mode == "legacy":
                assert "history_messages" not in row
            else:
                assert [m.content for m in row["history_messages"]] == [
                    m.content for m in row["messages"]
                ]
        assert not any(
            (getattr(m, "additional_kwargs", None) or {}).get(PERSIST_ROLE_KEY)
            == "context"
            for _sid, m, _turn in driver.ledger.messages
        )

    @pytest.mark.asyncio
    async def test_tool_calls_are_audited_under_the_parent_with_subagent_tags(
        self, parent
    ):
        parent_ctx, _ = parent
        parent_ctx._current_phase = "tactical"
        parent_ctx._current_phase_number = 4
        archiver = FakeArchiver()
        driver, _, _ = await make_driver(
            parent_ctx,
            [
                tool_turn("read_file", {"path": "notes/hello.md"}, "c1"),
                text_turn(SECRET),
            ],
            archiver=archiver,
        )
        try:
            await driver.run("read it")
        finally:
            await driver.close()
        assert len(archiver.starts) == 1 and len(archiver.results) == 1
        start = archiver.starts[0]
        assert start["job_id"] == "parent-job"
        assert start["agent_type"] == "developer"
        assert start["tool_name"] == "read_file"
        assert start["call_id"] == "c1"
        assert start["phase"] == "tactical" and start["phase_number"] == 4
        meta = start["metadata"]
        assert meta["config_name"] == "developer"  # the parent's metadata rides along
        assert meta["subagent_id"] == "child-0001"
        assert meta["subagent_handle"] == "explorer-0001"
        assert meta["subagent_type"] == "explorer"
        done = archiver.results[0]
        assert done["id"] == "audit-1" and done["success"] is True
        assert "MARMALADE" in done["result"]

    @pytest.mark.asyncio
    async def test_inbox_items_never_carry_delivery_identity_and_stores_are_none(
        self, parent, monkeypatch
    ):
        """U0 #9/#3: only content/id/role on the items; no memory/KB stores."""
        parent_ctx, _ = parent
        import agent.subagents.driver as driver_module

        captured: Dict[str, Any] = {}
        real = driver_module.run_persistent_loop

        async def _spy(**kwargs):
            captured.update(kwargs)
            return await real(**kwargs)

        monkeypatch.setattr(driver_module, "run_persistent_loop", _spy)
        items: List[Any] = []
        driver, _, _ = await make_driver(parent_ctx, [text_turn("ok")])
        orig = driver.get_user_input

        async def _peek(*a, **k):
            item = await orig(*a, **k)
            items.append(item)
            return item

        driver.get_user_input = _peek
        try:
            await driver.run("hi")
        finally:
            await driver.close()
        assert items and set(items[0]) == {"content", "id", "role"}
        for key in ("recall_store", "knowledge_store", "memory_service", "project_ids"):
            assert captured[key] is None, key
        assert captured["defer_memory_extraction_to_outbox"] is False
        assert captured["memory_thread_id"] == "child-0001"
        assert captured["get_current_tools"] == driver.current_tools
        assert captured["get_current_system_prompt"]() == driver.build.system_prompt
        cbs = captured["callbacks"]
        for name in (
            "admit_input_delivery",
            "defer_input_delivery",
            "cancel_input_delivery",
            "settle_input_delivery",
            "before_turn_authorization",
        ):
            assert getattr(cbs, name) is None, name
        assert cbs.before_provider_admission is not None
        assert cbs.before_provider_execution == driver.before_provider_execution
        assert cbs.on_tool_execution_start == driver.on_tool_execution_start
        assert cbs.hard_interrupt_event is driver.hard_interrupt_event


# ---------------------------------------------------------------------------
# Scenario C — fork pre-seed
# ---------------------------------------------------------------------------


class TestScenarioC:
    @pytest.mark.asyncio
    async def test_fork_seed_is_persisted_then_runs_under_a_transient_child_prompt(
        self, parent
    ):
        parent_ctx, _ = parent
        parent_history = [
            SystemMessage(content="[Summary of prior work] parent summary"),
            HumanMessage(content="parent turn 1: please list files"),
            AIMessage(
                content="",
                tool_calls=[
                    {"id": "p1", "name": "list_files", "args": {}, "type": "tool_call"}
                ],
            ),
            ToolMessage(content="notes/hello.md", tool_call_id="p1"),
            AIMessage(content="There is one file."),
            HumanMessage(content="parent turn 2: read it"),
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "id": "p2_open",
                        "name": "read_file",
                        "args": {"path": "x"},
                        "type": "tool_call",
                    }
                ],
            ),
        ]
        seed = seed_fork_history(parent_history)
        initial_seed = tuple(seed)
        driver, fake, build = await make_driver(
            parent_ctx,
            [text_turn("Fork child reporting: history received.")],
            messages=seed,
        )
        try:
            result = await driver.run(
                "You are a fork. Summarize what the parent was doing."
            )
        finally:
            await driver.close()
        assert result.status == "completed"
        assert result.text == "Fork child reporting: history received."
        provider_input = fake.calls[0]
        assert isinstance(provider_input[0], SystemMessage)
        assert "parent summary" not in str(provider_input[0].content)
        assert str(provider_input[0].content).startswith(
            str(build.system_prompt).split("\n")[0][:20]
        )
        assert isinstance(provider_input[1], SystemMessage)
        assert provider_input[1].content == "[Summary of prior work] parent summary"
        assert not any(
            isinstance(m, AIMessage)
            and any(tc["id"] == "p2_open" for tc in m.tool_calls)
            for m in provider_input
        )
        assert any("There is one file." in str(m.content) for m in provider_input)
        assert any("fork of the parent" in str(m.content) for m in provider_input)
        # The child prompt is provider-only: the durable list still starts at
        # the compacted summary and the bulk seed contains the exact reminted
        # objects handed to the driver.
        assert isinstance(driver.messages[0], SystemMessage)
        assert driver.messages[0].content == "[Summary of prior work] parent summary"
        assert not any(
            getattr(message, "id", None) == driver._transient_prompt_id
            for message in driver.messages
        )
        assert len(driver.ledger.seeds) == 1
        seed_subagent_id, persisted_seed = driver.ledger.seeds[0]
        assert seed_subagent_id == "child-0001"
        assert len(persisted_seed) == len(initial_seed)
        assert all(
            saved is original for saved, original in zip(persisted_seed, initial_seed)
        )
        assert not any(
            getattr(message, "id", None) == driver._transient_prompt_id
            for message in persisted_seed
        )

    @pytest.mark.asyncio
    async def test_seed_persistence_refusal_aborts_once_before_provider_start(
        self, parent
    ):
        class RefusingLedger(RecordingLedger):
            def __init__(self):
                super().__init__()
                self.seed_attempts = 0

            async def persist_seed(self, subagent_id, messages):
                self.seed_attempts += 1
                return False

        parent_ctx, _ = parent
        ledger = RefusingLedger()
        seed = seed_fork_history([SystemMessage(content="[Summary] durable")])
        driver, fake, _ = await make_driver(
            parent_ctx,
            [text_turn("must never run")],
            messages=seed,
            ledger=ledger,
        )
        try:
            with pytest.raises(RuntimeError, match="persistence was refused"):
                await driver.run("brief")
            with pytest.raises(RuntimeError, match="seed is not durable"):
                await driver.run("brief again")
        finally:
            await driver.close()
        assert ledger.seed_attempts == 1
        assert fake.calls == []
        assert not driver.started

    @pytest.mark.asyncio
    async def test_seed_persistence_exception_aborts_before_provider_start(
        self, parent
    ):
        class FailingLedger(RecordingLedger):
            async def persist_seed(self, subagent_id, messages):
                raise RuntimeError("database unavailable")

        parent_ctx, _ = parent
        driver, fake, _ = await make_driver(
            parent_ctx,
            [text_turn("must never run")],
            messages=seed_fork_history([]),
            ledger=FailingLedger(),
        )
        try:
            with pytest.raises(RuntimeError, match="failed before provider start"):
                await driver.run("brief")
        finally:
            await driver.close()
        assert fake.calls == []
        assert not driver.started


# ---------------------------------------------------------------------------
# Scenario D — an unanswered gate parks the turn
# ---------------------------------------------------------------------------


class TestScenarioD:
    @pytest.mark.asyncio
    async def test_permission_check_auto_approves_and_never_asks(self, parent):
        parent_ctx, _ = parent
        driver, _, _ = await make_driver(parent_ctx, [])
        outcome = await driver.permission_check(
            "read_file", {"path": "x"}, "c", "extra", k=1
        )
        assert outcome is PermissionOutcome.APPROVED

    @pytest.mark.asyncio
    async def test_no_answer_is_classified_parked_before_the_repair_strips_it(
        self, parent
    ):
        parent_ctx, _ = parent
        driver, fake, _ = await make_driver(
            parent_ctx,
            [
                tool_turn("read_file", {"path": "notes/hello.md"}, "call_gate_1"),
                text_turn("continued after the late answer"),
            ],
        )

        async def _no_answer(*a, **k):
            return PermissionOutcome.NO_ANSWER

        driver.permission_check = _no_answer
        # The callbacks object is built at start(); rebuild it with the patch.
        try:
            result = await driver.run("Read notes/hello.md.")
            assert result.status == "parked"
            assert result.kind == "parked"
            assert result.parked_call["id"] == "call_gate_1"
            assert result.parked_call["name"] == "read_file"
            assert result.text == "" and not result.partial
            assert not any(isinstance(m, ToolMessage) for m in _brief(driver))
            # A late follow-up continues: the open call is repaired away.
            driver.permission_check = driver.__class__.permission_check.__get__(driver)
            result2 = await driver.run(
                "[parent] gate answered late; continue.", role="event"
            )
            assert result2.status == "completed"
            assert not any(
                isinstance(m, AIMessage)
                and any(tc["id"] == "call_gate_1" for tc in m.tool_calls)
                for m in fake.calls[1]
            )
        finally:
            await driver.close()


# ---------------------------------------------------------------------------
# Scenario E — compaction due, no auxiliary: fast-fail, no retry burn
# ---------------------------------------------------------------------------


class TestScenarioE:
    @pytest.mark.asyncio
    async def test_compaction_without_aux_fast_fails(self, parent):
        parent_ctx, _ = parent
        history = []
        for i in range(8):
            history.append(
                HumanMessage(content=f"parent turn {i}: " + "lorem ipsum " * 30)
            )
            history.append(AIMessage(content=f"answer {i}: " + "dolor sit amet " * 30))
        driver, fake, build = await make_driver(
            parent_ctx,
            [text_turn("Survived compaction without a summarizer.")],
            messages=list(history),
        )
        cm = build.context_manager
        cm.config.compaction_threshold_tokens = 50
        cm.config.summarization_threshold_tokens = 50
        cm.config.message_count_min_tokens = 50
        cm.config.keep_recent_messages = 2
        assert cm.should_summarize(driver.messages)
        started = time.perf_counter()
        try:
            result = await driver.run("x " * 400)
        finally:
            await driver.close()
        elapsed = time.perf_counter() - started
        assert result.status == "completed"
        assert result.text == "Survived compaction without a summarizer."
        assert cm.compaction_runs == 0
        assert elapsed < 5.0, f"summarizer retry burn is back: {elapsed:.1f}s"


# ---------------------------------------------------------------------------
# Budgets: turn cap, token cap, staleness
# ---------------------------------------------------------------------------


class TestBudgets:
    @pytest.mark.asyncio
    async def test_turn_cap_forces_one_tool_less_synthesis_turn(self, parent):
        parent_ctx, _ = parent
        driver, fake, build = await make_driver(
            parent_ctx,
            [
                tool_turn("read_file", {"path": "notes/hello.md"}, "c1"),
                tool_turn("list_files", {"path": "notes"}, "c2"),
                text_turn("Synthesis: MARMALADE, one file."),
            ],
            budgets=ChildBudgets(2, 250_000, 2000, 300, 900),
        )
        seen_bindings: List[int] = []
        real_current = driver.current_tools

        def _spy():
            llm, tools = real_current()
            seen_bindings.append(len(tools))
            return llm, tools

        driver.current_tools = _spy
        try:
            result = await driver.run("read then list")
        finally:
            await driver.close()
        assert result.status == "capped:turns"
        assert result.kind == "capped"
        assert result.text == "Synthesis: MARMALADE, one file."
        assert result.turns == 3  # cap + the synthesis call
        assert result.tool_calls == 1  # the second batch never ran
        # The interruption event is in the durable history, no fake ToolMessage.
        events = [
            m
            for m in driver.messages
            if isinstance(m, HumanMessage)
            and "[tool-call interruption]" in str(m.content)
        ]
        assert len(events) == 1
        assert sum(1 for m in driver.messages if isinstance(m, ToolMessage)) == 1
        # The synthesis turn ran with an EMPTY binding and the light runner's prompt.
        assert seen_bindings == [3, 0]
        synth_input = fake.calls[2]
        prompt = SYNTH_PROMPT.format(reason="turn budget")
        carrier = synth_input[-1]
        assert isinstance(carrier, HumanMessage) and str(carrier.content) == prompt
        assert carrier.additional_kwargs.get(PERSIST_ROLE_KEY) == "event"
        # And the binding is back for the next brief.
        assert len(driver.current_tools()[1]) == 3

    @pytest.mark.asyncio
    async def test_turn_cap_on_a_final_answer_needs_no_synthesis(self, parent):
        parent_ctx, _ = parent
        driver, fake, _ = await make_driver(
            parent_ctx,
            [
                tool_turn("read_file", {"path": "notes/hello.md"}, "c1"),
                text_turn(SECRET),
            ],
            budgets=ChildBudgets(2, 250_000, 2000, 300, 900),
        )
        try:
            result = await driver.run("read it")
            assert result.status == "completed"
            assert result.text == SECRET
            assert len(fake.calls) == 2
            assert driver.check_interrupt() is None  # nothing left armed
        finally:
            await driver.close()

    @pytest.mark.asyncio
    async def test_token_cap_forces_synthesis(self, parent):
        parent_ctx, _ = parent
        driver, fake, _ = await make_driver(
            parent_ctx,
            [
                tool_turn("read_file", {"path": "notes/hello.md"}, "c1"),
                tool_turn("list_files", {"path": "notes"}, "c2"),
                text_turn("Synthesis after the token cap."),
            ],
            budgets=ChildBudgets(50, 150, 2000, 300, 900),
        )
        try:
            result = await driver.run("read then list")
        finally:
            await driver.close()
        assert result.status == "capped:tokens"
        assert result.text == "Synthesis after the token cap."
        assert result.tokens == 108 + 108 + 132
        assert SYNTH_PROMPT.format(reason="token budget") in [
            str(m.content) for m in fake.calls[2] if isinstance(m, HumanMessage)
        ]

    @pytest.mark.asyncio
    async def test_stale_idle_escalates_to_a_hard_interrupt(self, parent):
        parent_ctx, _ = parent
        clock = FakeClock()
        driver, fake, _ = await make_driver(
            parent_ctx,
            [HANG],
            budgets=ChildBudgets(50, 250_000, 2000, 300, 900),
            clock=clock,
        )
        run_task = asyncio.create_task(driver.run("hang"))
        await asyncio.wait_for(fake.hang_started.wait(), timeout=5)
        assert driver.running and driver.in_tool_since is None
        # Under the idle threshold: nothing.
        clock.advance(299)
        await asyncio.sleep(0.05)
        assert driver.stale_armed_at is None and not run_task.done()
        # Idle past stale_idle_s: the soft arm (graceful + synthesis).
        clock.advance(2)
        await asyncio.sleep(0.05)
        assert driver.stale_armed_at is not None
        assert driver._synth_pending and not run_task.done()
        # Still no return after stale_in_tool_s / 2: the hard interrupt.
        clock.advance(450 + 1)
        result = await asyncio.wait_for(run_task, timeout=5)
        assert result.status == "interrupted:stale"
        assert result.text == "" and not result.partial
        assert driver.hard_interrupt_event.is_set()
        assert driver._loop_task.done()
        await driver.close()

    @pytest.mark.asyncio
    async def test_stale_in_tool_escalates(self, parent):
        parent_ctx, _ = parent
        clock = FakeClock()
        entered = asyncio.Event()

        @tool
        async def block_forever(seconds: int = 0) -> str:
            """Block until cancelled (staleness test tool)."""
            entered.set()
            await asyncio.Event().wait()
            return "never"

        driver, fake, build = await make_driver(
            parent_ctx,
            [tool_turn("block_forever", {"seconds": 1}, "c1"), text_turn("unreached")],
            budgets=ChildBudgets(50, 250_000, 2000, 300, 900),
            clock=clock,
        )
        build.tools.append(block_forever)
        run_task = asyncio.create_task(driver.run("block"))
        await asyncio.wait_for(entered.wait(), timeout=5)
        assert driver.in_tool_since is not None
        # stale_in_tool_s is 900 here: just under it nothing happens ...
        clock.advance(899)
        await asyncio.sleep(0.05)
        assert driver.stale_armed_at is None
        # ... past it the soft arm, then the hard interrupt after 900 / 2.
        clock.advance(2)
        await asyncio.sleep(0.05)
        assert driver.stale_armed_at is not None
        clock.advance(450 + 1)
        result = await asyncio.wait_for(run_task, timeout=5)
        assert result.status == "interrupted:stale"
        assert result.tool_calls == 1
        await driver.close()

    @pytest.mark.asyncio
    async def test_watcher_is_quiet_while_idle_between_briefs(self, parent):
        parent_ctx, _ = parent
        clock = FakeClock()
        driver, _, _ = await make_driver(parent_ctx, [text_turn("ok")], clock=clock)
        try:
            result = await driver.run("hi")
            assert result.status == "completed"
            clock.advance(10_000)
            await asyncio.sleep(0.05)
            assert driver.stale_armed_at is None
            assert driver.alive
        finally:
            await driver.close()


# ---------------------------------------------------------------------------
# Error classifications
# ---------------------------------------------------------------------------


class TestErrors:
    @pytest.mark.asyncio
    async def test_empty_response_placeholder_is_an_error_not_a_result(self, parent):
        parent_ctx, _ = parent
        driver, _, _ = await make_driver(parent_ctx, [text_turn("")])
        try:
            result = await driver.run("say nothing")
        finally:
            await driver.close()
        assert result.status == "error"
        assert result.text == ""
        assert result.error.startswith(PLACEHOLDER_PREFIX)
        assert "empty response" in result.error

    @pytest.mark.asyncio
    async def test_output_truncation_keeps_the_partial_text_as_an_error(self, parent):
        parent_ctx, _ = parent
        driver, _, _ = await make_driver(
            parent_ctx, [text_turn("half an answer", finish_reason="length")]
        )
        try:
            result = await driver.run("go")
        finally:
            await driver.close()
        assert result.status == "error"
        assert result.text == "half an answer"
        assert result.partial
        assert "truncated" in result.error

    @pytest.mark.asyncio
    async def test_on_error_after_the_retry_ceiling_is_an_error(
        self, parent, monkeypatch
    ):
        monkeypatch.setattr("agent.persistent_graph._SESSION_LLM_RETRY_BASE_DELAY", 0.0)
        parent_ctx, _ = parent
        boom = [RuntimeError("connection reset") for _ in range(10)]
        entry = explorer_entry(limits={"llm_inproc_retries": 2})
        driver, fake, build = await make_driver(parent_ctx, boom, entry=entry)
        assert build.config.limits.llm_inproc_retries == 2
        try:
            result = await driver.run("go")
        finally:
            await driver.close()
        assert result.status == "error"
        assert result.error  # the loop's user-facing turn error, via on_error
        assert len(fake.calls) == 2, (
            "the child's own retry ceiling, not the session's 3"
        )

    @pytest.mark.asyncio
    async def test_default_retry_ceiling_is_the_subagent_overlays_three(
        self, parent, monkeypatch
    ):
        monkeypatch.setattr("agent.persistent_graph._SESSION_LLM_RETRY_BASE_DELAY", 0.0)
        parent_ctx, _ = parent
        script = [
            RuntimeError("flap"),
            RuntimeError("flap"),
            text_turn("third time lucky"),
        ]
        driver, fake, build = await make_driver(parent_ctx, script)
        assert build.config.limits.llm_inproc_retries == 3
        try:
            result = await driver.run("go")
        finally:
            await driver.close()
        assert result.status == "completed"
        assert result.text == "third time lucky"
        assert len(fake.calls) == 3

    @pytest.mark.asyncio
    async def test_a_turn_that_ends_on_a_tool_message_is_never_promoted(self, parent):
        parent_ctx, _ = parent
        driver, _, _ = await make_driver(parent_ctx, [])
        driver._brief_start = len(driver.messages)
        driver.messages.extend(
            [
                HumanMessage(content="brief"),
                AIMessage(content="working on it"),
                AIMessage(
                    content="",
                    tool_calls=[
                        {
                            "id": "t1",
                            "name": "read_file",
                            "args": {},
                            "type": "tool_call",
                        }
                    ],
                ),
                ToolMessage(content="SECRET FILE CONTENT", tool_call_id="t1"),
            ]
        )
        result = driver.classify()
        assert result.status == "error"
        assert "SECRET FILE CONTENT" not in result.text
        assert result.text == "working on it" and result.partial
        assert "tool result" in result.error

    @pytest.mark.asyncio
    async def test_provider_admission_closed_is_interrupted_drain(self, parent):
        parent_ctx, _ = parent
        host = make_host(admission_fn=lambda: False)
        driver, fake, _ = await make_driver(parent_ctx, [text_turn("never")], host=host)
        try:
            result = await driver.run("go")
        finally:
            await driver.close()
        assert result.status == "interrupted:drain"
        assert fake.calls == []  # no provider spend after the fence closed
        assert "admission is closed" in result.error


# ---------------------------------------------------------------------------
# Shared tree: git untouched, sudo forwarded, steer/stop
# ---------------------------------------------------------------------------


def _git(args, cwd):
    return subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True)


class TestSharedTree:
    @pytest.mark.asyncio
    async def test_child_writes_never_auto_commit_the_parent_tree(self, tmp_path):
        parent_ctx, root = make_parent(tmp_path, git=True)
        parent_git = parent_ctx.workspace_manager.git_manager
        assert parent_git is not None and parent_git.is_active
        before = _git(["rev-list", "--count", "HEAD"], root).stdout.strip()
        entry = explorer_entry(
            tools={"workspace": ["read_file", "write_file"]}, write_policy="full"
        )
        driver, fake, build = await make_driver(
            parent_ctx,
            [
                tool_turn(
                    "write_file",
                    {"path": "child_out.md", "content": "from child"},
                    "w1",
                ),
                text_turn("wrote it"),
            ],
            entry=entry,
        )
        assert build.isolation == "shared"
        assert build.workspace_manager is not parent_ctx.workspace_manager
        assert build.workspace_manager.backend is parent_ctx.workspace_manager.backend
        try:
            result = await driver.run("write child_out.md")
        finally:
            await driver.close()
        assert result.status == "completed"
        assert (root / "child_out.md").read_text() == "from child"  # in the parent ROOT
        after = _git(["rev-list", "--count", "HEAD"], root).stdout.strip()
        assert after == before, (
            "the loop's turn-end auto-commit touched the parent tree"
        )
        assert "child_out.md" in _git(["status", "--porcelain"], root).stdout
        # The parent's real manager is untouched and still active.
        assert parent_ctx.workspace_manager.git_manager is parent_git

    @pytest.mark.asyncio
    async def test_child_sudo_freeze_is_forwarded_to_the_parent_tagged(self, parent):
        parent_ctx, _ = parent
        driver, fake, build = await make_driver(
            parent_ctx, [tool_turn("needs_sudo", {}, "s1"), text_turn("continued")]
        )
        child_ctx = build.tool_context

        @tool
        def needs_sudo() -> str:
            """Pretend a sudo command was intercepted (test tool)."""
            child_ctx.request_freeze(
                {
                    "freeze_type": "vm_upgrade_required",
                    "reason": "sudo",
                    "command": "sudo x",
                }
            )
            return "This command requires elevated privileges (sudo)."

        build.tools.append(needs_sudo)
        try:
            result = await driver.run("run sudo")
        finally:
            await driver.close()
        assert result.status == "completed"
        assert result.sudo_requested
        assert child_ctx.consume_freeze_request() is None  # consumed by the loop
        forwarded = parent_ctx.consume_freeze_request()
        assert forwarded["freeze_type"] == "vm_upgrade_required"
        assert forwarded["command"] == "sudo x"
        assert forwarded["subagent_handle"] == "explorer-0001"
        assert forwarded["subagent_id"] == "child-0001"

    @pytest.mark.asyncio
    async def test_stop_mid_turn_is_interrupted_stopped(self, parent):
        parent_ctx, _ = parent
        driver, fake, _ = await make_driver(parent_ctx, [HANG])
        run_task = asyncio.create_task(driver.run("hang"))
        await asyncio.wait_for(fake.hang_started.wait(), timeout=5)
        await driver.stop(timeout=2)
        result = await asyncio.wait_for(run_task, timeout=5)
        assert result.status == "interrupted:stopped"
        assert not driver.alive

    @pytest.mark.asyncio
    async def test_steer_queues_an_event_and_arms_a_graceful_interrupt_mid_turn(
        self, parent
    ):
        parent_ctx, _ = parent
        driver, _, _ = await make_driver(parent_ctx, [])
        driver.steer("idle steer")
        item = driver.inbox.get_nowait()
        assert item["role"] == "event" and item["content"] == "idle steer"
        assert driver.check_interrupt() is None
        driver.running = True
        driver.steer("mid-turn steer")
        assert driver._steer_pending
        assert driver.check_interrupt() == "graceful"
        assert driver.check_interrupt() is None  # one-shot
        driver.running = False

    @pytest.mark.asyncio
    async def test_callbacks_accept_positional_and_keyword_tails(self, parent):
        parent_ctx, _ = parent
        driver, _, _ = await make_driver(parent_ctx, [])
        await driver.on_token("t", "x", y=1)
        await driver.on_thinking("th", message_id="m1")
        await driver.on_tool_start("read_file", {"path": "p"}, "c1", "x", y=1)
        await driver.on_tool_result("read_file", "r", "c1", True, "x", y=1)
        await driver.on_turn_start(1, "x", y=1)
        await driver.on_turn_complete(
            1, {"input_tokens": 1}, "msg", "thread", "id", skip=True
        )
        await driver.on_error("boom", turn_id=1, extra="x")
        await driver.on_usage({"input_tokens": 5, "output_tokens": 2}, "x", y=1)
        await driver.on_workspace_upgrade_needed(
            {"freeze_type": "vm_upgrade_required"}, "x", y=1
        )
        await driver.on_context_compacted("summary", 10, 2)
        await driver.persist_message(HumanMessage(content="m"), "x", y=1)
        assert driver.tokens == 7 and driver.errors == ["boom"]
        assert driver.before_provider_admission("x", y=1) is True
        parent_ctx.consume_freeze_request()

    @pytest.mark.asyncio
    async def test_exact_authority_refusal_prevents_provider_spend(self, parent):
        parent_ctx, _ = parent

        async def stale():
            return False

        driver, fake, _ = await make_driver(
            parent_ctx,
            [text_turn("must never run")],
            host=make_host(effect_authority_fn=stale),
        )
        try:
            result = await driver.run("try")
        finally:
            await driver.close()
        assert result.status == "interrupted:authority"
        assert fake.calls == []

    @pytest.mark.asyncio
    async def test_exact_authority_is_rechecked_at_real_tool_effect(self, parent):
        parent_ctx, _ = parent
        probes = 0

        async def current_once():
            nonlocal probes
            probes += 1
            return probes == 1

        driver, fake, _ = await make_driver(
            parent_ctx,
            [tool_turn("read_file", {"path": "notes/hello.md"}, "read-1")],
            host=make_host(effect_authority_fn=current_once),
        )
        try:
            result = await driver.run("read")
        finally:
            await driver.close()
        assert result.status == "interrupted:authority"
        assert len(fake.calls) == 1
        assert probes == 2
        assert not any(isinstance(msg, ToolMessage) for msg in _brief(driver))

    @pytest.mark.asyncio
    async def test_tool_effect_callback_raises_a_typed_authority_loss(self, parent):
        parent_ctx, _ = parent

        async def stale():
            return False

        driver, _, _ = await make_driver(
            parent_ctx, [], host=make_host(effect_authority_fn=stale)
        )
        with pytest.raises(SubagentAuthorityLost):
            await driver.on_tool_execution_start("read_file", "read-1")
        assert driver.authority_lost is True
