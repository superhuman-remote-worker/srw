"""``delegate_agent`` — foreground U3 spawn plus durable U4 background mode.

Design: knowledge-base/knowledge/features/universal_experts_and_subagents.md
§0 D1 (the shape), §1.3; plan B.6 / B.8 / B.12 / B.13 / WP2. The tool is
driven exactly as the parent's tool node drives it — a LangGraph ``ToolNode``
inside a one-node graph injects the tool call id — and every child is the
real ``build_child`` + ``SubagentDriver`` over ``FilesystemTestBackend`` on
the scripted ``tests/_fake_chat_model.FakeChatModel``.
"""

from __future__ import annotations

import asyncio
import re
import time
from types import SimpleNamespace
from typing import Any, Callable, List, Optional

import pytest
from langchain_core.messages import (
    AIMessage,
    HumanMessage,
    SystemMessage,
    ToolMessage,
)
from langgraph.graph import END, START, MessagesState, StateGraph
from langgraph.prebuilt import ToolNode

from shared.runtime.core.loader import LLMConfig
from shared.runtime.core.subagent_roster import resolve_subagent_roster
from agent.core.workspace import WorkspaceManager, WorkspaceManagerConfig
from agent.subagents import (
    SUBAGENT_STATUSES,
    RecordingLedger,
    SubagentRuntime,
    WorkerHost,
)
from agent.subagents.child import WRITE_POLICIES
from agent.subagents.fork import FORK_NOTICE
from agent.tools.context import ToolContext
from agent.tools.delegation import create_delegation_tools, get_delegation_metadata
from agent.tools.delegation.delegate_agent import (
    DELEGATE_AGENT_METADATA,
    build_description,
    create_delegate_agent_tools,
)
from agent.tools.registry import TOOL_REGISTRY, load_tools
from tests._fake_chat_model import FakeChatModel, text_turn, tool_turn
from tests._fs_backend import FilesystemTestBackend

_PARENT_LLM = {
    "model": "gpt-4o-mini",
    "provider": "openai",
    "api_key": "sk-parent-test",
    "model_max_context_tokens": 128000,
}
PARENT_TOOLS = ["read_file", "list_files", "search_files", "write_file", "edit_file"]
HANDLE = re.compile(r"^[a-z0-9-]+-[0-9a-f]{4}$")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def resolve_roster(entries: dict, *, default: Optional[str] = None) -> dict:
    """A RESOLVED ``subagents`` block (the shape ``tool_config`` carries)."""
    data = {
        "agent_id": "parent",
        "display_name": "Parent",
        "llm": dict(_PARENT_LLM),
        "subagents": {"roster": entries, **({"default": default} if default else {})},
    }
    return resolve_subagent_roster(data, db_refs={}, on_missing="raise")["subagents"]


def explorer_roster(default: Optional[str] = "explorer", **overrides) -> dict:
    return resolve_roster(
        {"explorer": {"$ref": "subagents/explorer", **overrides}}, default=default
    )


def make_parent(
    tmp_path,
    *,
    subagents: Optional[dict] = None,
    max_concurrent: int = 2,
    enabled: bool = True,
    run_in_background_default: bool = False,
    names: Optional[List[str]] = None,
):
    root = tmp_path / "ws"
    (root / "notes").mkdir(parents=True)
    (root / "notes" / "hello.md").write_text(
        "# hello\n\nthe secret word is MARMALADE\n"
    )
    ws = WorkspaceManager(
        job_id="parent-job",
        base_path=root,
        backend=FilesystemTestBackend(root),
        config=WorkspaceManagerConfig(git_versioning=False),
    )
    ws.initialize()
    ctx = ToolContext(
        workspace_manager=ws,
        config={
            "shell": {},
            "agent_id": "developer",
            "delegation": {
                "enabled": enabled,
                "max_concurrent": max_concurrent,
                "run_in_background_default": run_in_background_default,
            },
            "tools": {"delegation": ["delegate_agent"]},
            "subagents": subagents if subagents is not None else explorer_roster(),
        },
        _job_metadata={
            "job_id": "parent-job",
            "project_id": "proj",
            "config_name": "developer",
        },
        _llm_config=LLMConfig(**_PARENT_LLM),
        _resolved_tool_names=list(names or PARENT_TOOLS),
    )
    return ctx, root


def install(
    ctx: ToolContext,
    *,
    factory: Callable[[Any, Any], Any],
    ledger: Optional[RecordingLedger] = None,
    hex_source: Optional[Callable[[], str]] = None,
) -> SubagentRuntime:
    """What ``agent.py`` does after ``load_tools`` — with a scripted child LLM."""
    host = WorkerHost.from_context(ctx)
    ctx._parent_host = host
    runtime = SubagentRuntime.from_context(
        ctx,
        host,
        ledger=ledger if ledger is not None else RecordingLedger(),
        llm_factory=factory,
        hex_source=hex_source,
        driver_kwargs={
            "watcher_poll_interval": 0.01,
            "archiver": None,
            "archive_fn": lambda **kw: None,
        },
    )
    ctx.subagent_runtime = runtime
    return runtime


def the_tool(ctx: ToolContext):
    tools = [t for t in create_delegation_tools(ctx) if t.name == "delegate_agent"]
    assert len(tools) == 1
    return tools[0]


async def invoke(tool, call_id: str, **args) -> str:
    """Invoke as the ToolNode does — a full ToolCall, so the id is injected."""
    message = await tool.ainvoke(
        {"id": call_id, "name": "delegate_agent", "args": args, "type": "tool_call"}
    )
    assert isinstance(message, ToolMessage)
    return message.content


def scripted(scripts: List[list], made: Optional[list] = None):
    """A factory handing each new child the next script."""

    def factory(cfg, limits):
        fake = FakeChatModel(scripts.pop(0))
        if made is not None:
            made.append(fake)
        return fake

    return factory


class EchoModel(FakeChatModel):
    """Answers every brief with ``echo: <last human text>`` after ``delay``
    seconds and records the ``(start, end)`` of each provider call."""

    def __init__(self, *, delay: float = 0.0, spans: Optional[list] = None):
        super().__init__([])
        self.delay = delay
        self.spans = spans if spans is not None else []

    async def astream(self, messages, **kw):
        self.calls.append(list(messages))
        start = time.monotonic()
        await asyncio.sleep(self.delay)
        last = next(
            (m for m in reversed(messages) if isinstance(m, HumanMessage)), None
        )
        content = last.content if last is not None else ""
        text = content if isinstance(content, str) else str(content)
        for chunk in text_turn(f"echo: {text}"):
            await asyncio.sleep(0)
            yield chunk
        self.spans.append((start, time.monotonic()))


def brief_args(i: int = 1, **extra) -> dict:
    return {
        "description": f"brief {i}",
        "prompt": f"brief {i}",
        "subagent_type": "explorer",
        **extra,
    }


# ---------------------------------------------------------------------------
# Schema, registry, description
# ---------------------------------------------------------------------------


class TestSchemaAndRegistry:
    def test_registry_entry(self):
        meta = TOOL_REGISTRY["delegate_agent"]
        assert meta == DELEGATE_AGENT_METADATA["delegate_agent"]
        assert meta["category"] == "delegation"
        assert meta["phases"] == ["strategic", "tactical"]
        assert meta["grant"] == "explicit"
        assert meta["gate"]
        assert meta["short_description"]
        assert "delegate_agent" in get_delegation_metadata()
        # Never deferred: the per-call description IS the model-facing text.
        assert not meta.get("defer_to_workspace")

    def test_model_facing_schema(self, tmp_path):
        ctx, _ = make_parent(tmp_path)
        tool = the_tool(ctx)
        schema = tool.tool_call_schema.model_json_schema()
        props = schema["properties"]
        assert list(props) == [
            "description",
            "prompt",
            "subagent_type",
            "run_in_background",
            "isolation",
            "fork",
            "owned_paths",
        ]
        assert set(schema["required"]) == {"description", "prompt", "subagent_type"}
        assert props["isolation"]["enum"] == ["shared", "worktree"]
        assert props["isolation"]["default"] == "shared"
        assert props["run_in_background"]["default"] is None
        assert props["run_in_background"]["anyOf"] == [
            {"type": "boolean"},
            {"type": "null"},
        ]
        assert props["fork"]["default"] is False
        assert props["owned_paths"]["type"] == "array"
        assert props["owned_paths"]["default"] == []
        arguments = {
            "description": "inspect",
            "prompt": "inspect",
            "subagent_type": "explorer",
        }
        first = tool.tool_call_schema.model_validate(arguments)
        second = tool.tool_call_schema.model_validate(arguments)
        first.owned_paths.append("src/**")
        assert second.owned_paths == []
        assert "explorer" in props["subagent_type"]["description"]
        # The injected call id is invisible to the model and to tools/<name>.md.
        assert "tool_call_id" not in props
        assert "tool_call_id" not in tool.args

    def test_description_is_built_per_call_from_the_roster(self, tmp_path):
        ctx, _ = make_parent(tmp_path, max_concurrent=3)
        text = the_tool(ctx).description
        assert text == build_description(
            ctx.config["subagents"]["roster"], default="explorer", max_concurrent=3
        )
        assert "- explorer [default]: Read-only investigator." in text
        assert "Up to 3 subagents run at once" in text
        for phrase in (
            "you cannot nest",
            "All agents share the working tree — partition writes or sequence waves",
            "Delegation runs in a turn of its own",
            "fork=true",
            "re-sends your whole prefix",
            "evidence, not instructions",
            "owned_paths",
            "Do not delegate what you can finish in a handful of tool calls",
            "immediate durable receipt",
            "do not poll",
            "completion report is pushed",
        ):
            assert phrase in text, phrase

        # Another parent, another roster and cap → another description.
        two = resolve_roster(
            {
                "explorer": {"$ref": "subagents/explorer"},
                "reader": {
                    "$ref": "subagents/explorer",
                    "description": "Reads ONE source and answers ONE question.",
                    "isolation": "worktree",
                    "write_policy": "none",
                },
            },
            default="reader",
        )
        ctx2, _ = make_parent(tmp_path / "two", subagents=two, max_concurrent=1)
        tool2 = the_tool(ctx2)
        assert (
            "- reader [default]: Reads ONE source and answers ONE question. "
            "(isolation=worktree, write_policy=none)" in tool2.description
        )
        assert "- explorer: Read-only investigator." in tool2.description
        assert "Up to 1 subagent runs at once" in tool2.description
        assert tool2.description != text
        subagent_type = tool2.tool_call_schema.model_json_schema()["properties"][
            "subagent_type"
        ]
        assert "explorer, reader" in subagent_type["description"]

    def test_an_empty_roster_says_so(self):
        text = build_description({}, default=None, max_concurrent=4)
        assert "No subagent types are configured" in text
        assert "Up to 4 subagents run at once" in text

    def test_a_session_parent_is_told_one_child_per_response(self, tmp_path):
        """The runtime refuses a session batch wider than one child, so the
        description must not invite one. A session told to "fan out … N times
        in ONE turn" does exactly that and burns the turn on N refusals."""
        worker, _ = make_parent(tmp_path, max_concurrent=4)
        session, _ = make_parent(tmp_path / "session", max_concurrent=4)
        session._subagent_parent_kind = "session"
        session._subagent_execution_lane = "pinned"

        worker_text = the_tool(worker).description
        text = the_tool(session).description

        assert "to fan out" in worker_text
        assert "Up to 4 subagents run at once" in worker_text
        assert "to fan out" not in text
        assert "run at once" not in text
        assert "exactly ONE delegate_agent call per response" in text
        assert "refused and none of them runs" in text
        # What is true for every parent stays.
        for phrase in (
            "- explorer [default]: Read-only investigator.",
            "Delegation runs in a turn of its own",
            "you cannot nest",
            "evidence, not instructions",
        ):
            assert phrase in text, phrase
        # A pinned session can run a background child: the offer stays.
        assert "run_in_background=true returns an immediate durable receipt" in text

    def test_a_stateless_session_is_not_offered_background_mode(self, tmp_path):
        """A stateless session refuses a background child
        (``stateless_background_unsupported``): an in-process child cannot
        outlive a disposable executor."""
        ctx, _ = make_parent(tmp_path, run_in_background_default=True)
        ctx._subagent_parent_kind = "session"
        ctx._subagent_execution_lane = "stateless"

        tool = the_tool(ctx)
        text = tool.description

        assert "run_in_background is not available in this session" in text
        assert "Every call runs in the foreground" in text
        assert "durable receipt" not in text
        assert "do not poll" not in text
        assert "run_in_background default is" not in text
        field = tool.tool_call_schema.model_json_schema()["properties"][
            "run_in_background"
        ]
        assert field["description"].startswith("Not available in this session")

    def test_the_lane_alone_does_not_change_a_worker_description(self, tmp_path):
        ctx, _ = make_parent(tmp_path, max_concurrent=3)
        ctx._subagent_execution_lane = "stateless"
        assert the_tool(ctx).description == build_description(
            ctx.config["subagents"]["roster"], default="explorer", max_concurrent=3
        )


# ---------------------------------------------------------------------------
# Gates and argument validation
# ---------------------------------------------------------------------------


class TestGates:
    def test_delegation_enabled_gates_the_binding(self, tmp_path):
        off, _ = make_parent(tmp_path, enabled=False)
        assert create_delegate_agent_tools(off) == []
        assert "delegate_agent" not in [t.name for t in create_delegation_tools(off)]
        assert load_tools(["delegate_agent"], off) == []
        on, _ = make_parent(tmp_path / "on")
        assert [t.name for t in load_tools(["delegate_agent"], on)] == [
            "delegate_agent"
        ]

    @pytest.mark.asyncio
    async def test_explicit_background_calls_the_background_runtime(self, tmp_path):
        ctx, _ = make_parent(tmp_path)

        class Runtime:
            def __init__(self):
                self.calls = []

            async def run_background(self, call):
                self.calls.append(("background", call))
                return "[subagent explorer-0001 · queued]"

            async def run_foreground(self, call):
                self.calls.append(("foreground", call))
                return "unexpected"

        runtime = Runtime()
        ctx.subagent_runtime = runtime
        out = await invoke(the_tool(ctx), "c1", **brief_args(run_in_background=True))
        assert out == "[subagent explorer-0001 · queued]"
        assert [(kind, call.run_in_background) for kind, call in runtime.calls] == [
            ("background", True)
        ]

    @pytest.mark.asyncio
    async def test_stateless_session_fanout_and_recovery_fail_before_child_start(
        self, tmp_path
    ):
        ctx, _ = make_parent(tmp_path)

        class Runtime:
            batch_size = 2

            def __init__(self):
                self.calls = []

            async def run_background(self, call):
                self.calls.append(call)
                return "unexpected"

            async def run_foreground(self, call):
                self.calls.append(call)
                return "unexpected"

        runtime = Runtime()
        ctx.subagent_runtime = runtime
        ctx._subagent_parent_kind = "session"
        ctx._session_parent_authority_provider = lambda: SimpleNamespace(
            execution_lane="stateless"
        )

        fanout = await invoke(the_tool(ctx), "c1", **brief_args())
        assert fanout.startswith("Error: sessions may delegate only one")
        assert runtime.calls == []

        runtime.batch_size = 1
        ctx._stateless_subagent_recovery_active = True
        recovery = await invoke(the_tool(ctx), "c2", **brief_args())
        assert recovery.startswith("Error: delegate_agent is disabled")
        assert runtime.calls == []

    @pytest.mark.asyncio
    async def test_omission_uses_config_default_but_explicit_false_wins(self, tmp_path):
        ctx, _ = make_parent(tmp_path, run_in_background_default=True)

        class Runtime:
            def __init__(self):
                self.calls = []

            async def run_background(self, call):
                self.calls.append(("background", call))
                return "background"

            async def run_foreground(self, call):
                self.calls.append(("foreground", call))
                return "foreground"

        runtime = Runtime()
        ctx.subagent_runtime = runtime
        assert await invoke(the_tool(ctx), "c1", **brief_args()) == "background"
        assert (
            await invoke(the_tool(ctx), "c2", **brief_args(run_in_background=False))
            == "foreground"
        )
        assert [(kind, call.run_in_background) for kind, call in runtime.calls] == [
            ("background", True),
            ("foreground", False),
        ]

    @pytest.mark.asyncio
    async def test_a_stateless_session_never_defaults_to_background(self, tmp_path):
        """An omitted flag is not a request, so the expert's background
        default must not select a mode this lane refuses. An explicit ``true``
        is a request and still reaches the runtime, whose authority check
        refuses it."""
        ctx, _ = make_parent(tmp_path, run_in_background_default=True)
        ctx._subagent_parent_kind = "session"
        ctx._subagent_execution_lane = "stateless"

        class Runtime:
            batch_size = 1

            def __init__(self):
                self.calls = []

            async def run_background(self, call):
                self.calls.append(("background", call))
                return "background"

            async def run_foreground(self, call):
                self.calls.append(("foreground", call))
                return "foreground"

        runtime = Runtime()
        ctx.subagent_runtime = runtime
        assert await invoke(the_tool(ctx), "c1", **brief_args()) == "foreground"
        assert (
            await invoke(the_tool(ctx), "c2", **brief_args(run_in_background=True))
            == "background"
        )
        assert [(kind, call.run_in_background) for kind, call in runtime.calls] == [
            ("foreground", False),
            ("background", True),
        ]

    @pytest.mark.asyncio
    async def test_an_unknown_type_lists_the_roster(self, tmp_path):
        roster = resolve_roster(
            {
                "explorer": {"$ref": "subagents/explorer"},
                "reader": {"$ref": "subagents/explorer"},
            }
        )
        ctx, _ = make_parent(tmp_path, subagents=roster)
        made: list = []
        install(ctx, factory=scripted([[text_turn("never")]], made))
        out = await invoke(the_tool(ctx), "c1", **brief_args(subagent_type="nope"))
        assert out == (
            "Error: unknown subagent_type 'nope' — this expert's roster: "
            "explorer, reader"
        )
        assert made == []

    @pytest.mark.asyncio
    async def test_an_empty_type_falls_back_to_the_roster_default(self, tmp_path):
        ctx, _ = make_parent(tmp_path)
        install(ctx, factory=scripted([[text_turn("via default")]]))
        out = await invoke(the_tool(ctx), "c1", **brief_args(subagent_type=""))
        assert out.startswith("[subagent explorer-")
        assert "via default" in out

    @pytest.mark.asyncio
    async def test_no_default_and_no_type_is_an_error(self, tmp_path):
        ctx, _ = make_parent(tmp_path, subagents=explorer_roster(default=None))
        install(ctx, factory=scripted([[text_turn("never")]]))
        out = await invoke(the_tool(ctx), "c1", **brief_args(subagent_type=""))
        assert (
            out == "Error: subagent_type is required — this expert's roster: explorer"
        )

    @pytest.mark.asyncio
    async def test_an_empty_prompt_is_refused(self, tmp_path):
        ctx, _ = make_parent(tmp_path)
        made: list = []
        install(ctx, factory=scripted([[text_turn("never")]], made))
        out = await invoke(the_tool(ctx), "c1", **brief_args(prompt="   "))
        assert out.startswith("Error: prompt is required")
        assert made == []

    @pytest.mark.asyncio
    async def test_owned_paths_is_required_by_the_policy(self, tmp_path):
        roster = resolve_roster(
            {
                "implementer": {
                    "$ref": "subagents/explorer",
                    "write_policy": "owned_paths",
                    "tools": {"workspace": ["read_file", "write_file"]},
                }
            },
            default="implementer",
        )
        ctx, _ = make_parent(tmp_path, subagents=roster)
        ledger = RecordingLedger()
        install(
            ctx,
            factory=scripted([[text_turn("wrote it")]]),
            ledger=ledger,
            hex_source=iter(["0001", "0002"]).__next__,
        )
        tool = the_tool(ctx)
        refused = await invoke(tool, "c1", **brief_args(subagent_type="implementer"))
        assert refused == (
            "Error: subagent implementer-0001: write_policy=owned_paths needs "
            "owned_paths (the globs this child may write) — pass them on the spawn"
        )
        assert ledger.opened == []
        ok = await invoke(
            tool,
            "c2",
            **brief_args(subagent_type="implementer", owned_paths=["notes/**"]),
        )
        assert ok.startswith("[subagent implementer-0002 · implementer · completed")
        ((_, opened),) = ledger.opened
        assert opened["write_policy"] == "owned_paths"


# ---------------------------------------------------------------------------
# Running children
# ---------------------------------------------------------------------------


class TestRun:
    @pytest.mark.asyncio
    async def test_the_envelope_is_the_tool_result_and_the_report_is_spilled(
        self, tmp_path
    ):
        ctx, root = make_parent(tmp_path)
        made: list = []
        runtime = install(
            ctx,
            factory=scripted(
                [
                    [
                        tool_turn("read_file", {"path": "notes/hello.md"}, "c1"),
                        text_turn("The secret word is MARMALADE."),
                    ]
                ],
                made,
            ),
            hex_source=lambda: "7f3a",
        )
        out = await invoke(
            the_tool(ctx),
            "call-1",
            description="find the secret",
            prompt="Read notes/hello.md and report the secret word.",
            subagent_type="explorer",
        )
        assert out.startswith(
            "[subagent explorer-7f3a · explorer · completed · 2 turns / "
        )
        assert '<subagent_report handle="explorer-7f3a"' in out
        assert "The secret word is MARMALADE." in out
        assert "Full report: .subagents/explorer-7f3a/report.md" in out
        assert (
            root / ".subagents" / "explorer-7f3a" / "report.md"
        ).read_text().strip() == ("The secret word is MARMALADE.")
        # The brief reached the child verbatim as its first human message and
        # the child really read the file through its own read_file.
        first = made[0].calls[0]
        assert any(
            isinstance(m, HumanMessage)
            and "Read notes/hello.md and report the secret word." in str(m.content)
            for m in first
        )
        assert any(
            isinstance(m, ToolMessage) and "MARMALADE" in str(m.content)
            for m in made[0].calls[1]
        )
        record = runtime.records[("parent-job", "call-1")]
        assert record.status == "completed"
        assert record.envelope == out
        assert record.handle == "explorer-7f3a"

    @pytest.mark.asyncio
    async def test_idempotent_re_execution_returns_the_stored_report(self, tmp_path):
        ctx, _ = make_parent(tmp_path)
        made: list = []
        ledger = RecordingLedger()
        install(
            ctx,
            factory=scripted([[text_turn("first")], [text_turn("second")]], made),
            ledger=ledger,
        )
        tool = the_tool(ctx)
        first = await invoke(tool, "call-1", **brief_args(1))
        again = await invoke(tool, "call-1", **brief_args(1))
        assert again == first
        assert len(made) == 1  # no second child, no second spend
        assert len(ledger.opened) == 1
        other = await invoke(tool, "call-2", **brief_args(2))
        assert other != first and "second" in other
        assert len(made) == 2

    @pytest.mark.asyncio
    async def test_fan_out_runs_concurrently_under_the_semaphore_in_waves(
        self, tmp_path
    ):
        ctx, _ = make_parent(tmp_path, max_concurrent=2)
        spans: list = []
        install(ctx, factory=lambda cfg, lim: EchoModel(delay=0.15, spans=spans))
        tool = the_tool(ctx)
        outs = await asyncio.gather(
            *(invoke(tool, f"call-{i}", **brief_args(i)) for i in range(3))
        )
        for i, out in enumerate(outs):
            assert f"echo: brief {i}" in out, out
        assert len(spans) == 3
        starts = sorted(s for s, _ in spans)
        ends = sorted(e for _, e in spans)
        # The first two ran together; the third waited for a free slot.
        assert starts[1] - starts[0] < 0.1
        assert starts[2] >= ends[0] - 0.01
        overlap = max(
            sum(1 for s2, e2 in spans if s2 < e1 and e2 > s1) for s1, e1 in spans
        )
        assert overlap == 2

    @pytest.mark.asyncio
    async def test_the_ledger_receives_the_status_vocabulary(self, tmp_path):
        ctx, _ = make_parent(tmp_path)
        ledger = RecordingLedger()
        install(ctx, factory=scripted([[text_turn("All good.")]]), ledger=ledger)
        await invoke(
            the_tool(ctx),
            "call-1",
            description="say hi",
            prompt="Say all good.",
            subagent_type="explorer",
        )
        ((sid, opened),) = ledger.opened
        assert opened["status"] == "running"
        assert HANDLE.match(opened["handle"]) and opened["handle"].startswith(
            "explorer-"
        )
        assert opened["subagent_type"] == "explorer"
        assert opened["parent_job_id"] == "parent-job"
        assert opened["parent_thread_id"] is None
        assert opened["parent_tool_call_id"] == "call-1"
        assert opened["brief_description"] == "say hi"
        assert opened["isolation"] == "shared"
        assert opened["write_policy"] in WRITE_POLICIES
        assert opened["fork"] is False
        final = [f for s, f in ledger.updates if s == sid][-1]
        assert final["status"] == "completed"
        assert final["outcome"] == "completed"
        assert final["turns"] == 1
        assert final["tokens"] > 0
        assert final["report_path"] == f".subagents/{opened['handle']}/report.md"
        assert final["error"] is None
        seen = {f["status"] for _, f in ledger.opened + ledger.updates}
        assert seen <= set(SUBAGENT_STATUSES)
        # The transcript went through the same ledger (the driver's part).
        assert any(s == sid for s, _, _ in ledger.messages)

    @pytest.mark.asyncio
    async def test_a_capped_child_records_capped_with_the_outcome(self, tmp_path):
        ctx, _ = make_parent(
            tmp_path, subagents=explorer_roster(limits={"max_turns": 2})
        )
        ledger = RecordingLedger()
        install(
            ctx,
            factory=scripted(
                [
                    [
                        tool_turn("read_file", {"path": "notes/hello.md"}, "c1"),
                        tool_turn("list_files", {"path": "notes"}, "c2"),
                        text_turn("Synthesis: MARMALADE, one file."),
                    ]
                ]
            ),
            ledger=ledger,
        )
        out = await invoke(the_tool(ctx), "call-1", **brief_args())
        assert "· capped:turns ·" in out
        assert "Synthesis: MARMALADE, one file." in out
        final = ledger.updates[-1][1]
        assert final["status"] == "capped"
        assert final["outcome"] == "capped:turns"

    @pytest.mark.asyncio
    async def test_a_closed_admission_fence_ends_the_child_without_spend(
        self, tmp_path
    ):
        ctx, _ = make_parent(tmp_path)
        ctx.provider_admission = lambda: False  # what agent.py wires to the drain seam
        made: list = []
        ledger = RecordingLedger()
        install(ctx, factory=scripted([[text_turn("never")]], made), ledger=ledger)
        out = await invoke(the_tool(ctx), "call-1", **brief_args())
        assert "· interrupted:drain ·" in out
        assert made[0].calls == []  # zero provider calls
        final = ledger.updates[-1][1]
        assert final["status"] == "interrupted"
        assert final["outcome"] == "interrupted:drain"

    @pytest.mark.asyncio
    async def test_fork_seeds_the_child_with_the_parents_durable_history(
        self, tmp_path
    ):
        ctx, _ = make_parent(tmp_path)
        ctx._fork_source = [
            SystemMessage(content="PARENT SYSTEM PROMPT"),
            HumanMessage(content="Parent asked about MARMALADE"),
            AIMessage(content="Parent answered."),
        ]
        made: list = []
        install(
            ctx,
            factory=scripted([[text_turn("Forked.")], [text_turn("Fresh.")]], made),
        )
        tool = the_tool(ctx)
        await invoke(tool, "call-1", **brief_args(prompt="Continue.", fork=True))
        forked = made[0].calls[0]
        texts = [str(m.content) for m in forked]
        assert any("Parent asked about MARMALADE" in t for t in texts)
        assert any(FORK_NOTICE in t for t in texts)
        # Every durable SystemMessage survives U5's exact fork seed. A real
        # parent's system prompt is transient and therefore never appears in
        # `_fork_source`; this synthetic durable one models a compacted summary.
        assert any(t == "PARENT SYSTEM PROMPT" for t in texts)
        assert isinstance(forked[0], SystemMessage)  # the child's own prompt
        assert texts[-1].strip().endswith("Continue.") or "Continue." in texts[-1]

        await invoke(tool, "call-2", **brief_args(prompt="Fresh brief.", fork=False))
        fresh = [str(m.content) for m in made[1].calls[0]]
        assert not any("Parent asked about MARMALADE" in t for t in fresh)

    @pytest.mark.asyncio
    async def test_a_second_concurrent_shared_writer_is_refused(self, tmp_path):
        roster = resolve_roster(
            {
                "writer": {
                    "$ref": "subagents/explorer",
                    "write_policy": "full",
                    "tools": {"workspace": ["read_file", "write_file"]},
                }
            },
            default="writer",
        )
        ctx, _ = make_parent(tmp_path, subagents=roster)
        install(ctx, factory=lambda cfg, lim: EchoModel(delay=0.2))
        tool = the_tool(ctx)
        a, b = await asyncio.gather(
            invoke(tool, "call-a", **brief_args(1, subagent_type="writer")),
            invoke(tool, "call-b", **brief_args(2, subagent_type="writer")),
        )
        ran, refused = sorted([a, b], key=lambda s: s.startswith("Error"))
        assert ran.startswith("[subagent writer-")
        assert refused.startswith("Error: subagent writer-")
        assert "already holds write tools in the shared tree" in refused
        assert "use isolation=worktree for parallel writers" in refused

    @pytest.mark.asyncio
    async def test_through_the_tool_node_each_call_gets_its_own_id(self, tmp_path):
        """The parent's ToolNode path: a batch of two calls → two envelopes,
        each keyed by its injected tool call id (the idempotency key)."""
        ctx, _ = make_parent(tmp_path)
        runtime = install(
            ctx,
            factory=lambda cfg, lim: EchoModel(),
            hex_source=iter(["aaaa", "bbbb"]).__next__,
        )
        graph = StateGraph(MessagesState)
        graph.add_node("tools", ToolNode([the_tool(ctx)]))
        graph.add_edge(START, "tools")
        graph.add_edge("tools", END)
        app = graph.compile()
        ai = AIMessage(
            content="",
            tool_calls=[
                {
                    "id": "call-1",
                    "name": "delegate_agent",
                    "args": brief_args(1),
                    "type": "tool_call",
                },
                {
                    "id": "call-2",
                    "name": "delegate_agent",
                    "args": brief_args(2),
                    "type": "tool_call",
                },
            ],
        )
        out = await app.ainvoke({"messages": [ai]})
        results = [m for m in out["messages"] if isinstance(m, ToolMessage)]
        assert [m.tool_call_id for m in results] == ["call-1", "call-2"]
        assert "echo: brief 1" in results[0].content
        assert "echo: brief 2" in results[1].content
        assert sorted(runtime.records) == [
            ("parent-job", "call-1"),
            ("parent-job", "call-2"),
        ]
        handles = {r.handle for r in runtime.records.values()}
        assert handles == {"explorer-aaaa", "explorer-bbbb"}
