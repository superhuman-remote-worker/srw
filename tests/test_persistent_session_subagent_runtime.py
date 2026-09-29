"""PersistentSession's U5 wiring around the shared child runtime."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from agent.subagents.host import SessionHost
from tests.test_persistent_session import _make_session


def _context(*, tools=("delegate_agent",)) -> SimpleNamespace:
    return SimpleNamespace(
        _resolved_tool_names=list(tools),
        subagent_runtime=None,
        _parent_host=None,
    )


def test_delegation_tool_installs_a_true_thread_parent_runtime():
    authority_provider = MagicMock()
    admission = MagicMock(return_value=True)
    effect_authority = AsyncMock(return_value=True)
    event_callback = AsyncMock()
    session = _make_session(
        orchestrator_client=object(),
        session_parent_authority_provider=authority_provider,
        subagent_provider_admission=admission,
        subagent_effect_authority=effect_authority,
        subagent_event_callback=event_callback,
    )
    session.postgres_conn = object()
    session.tool_context = _context()
    ledger = object()
    runtime = object()

    with (
        patch(
            "agent.subagents.session_persistence.SessionSubagentLedger.from_context",
            return_value=ledger,
        ) as ledger_factory,
        patch(
            "agent.subagents.runtime.SubagentRuntime.from_context",
            return_value=runtime,
        ) as runtime_factory,
    ):
        session._install_session_subagent_runtime()

    ledger_factory.assert_called_once_with(session.tool_context)
    context_arg, host_arg = runtime_factory.call_args.args
    assert context_arg is session.tool_context
    assert isinstance(host_arg, SessionHost)
    assert host_arg.parent_ref.kind == "thread"
    assert host_arg.parent_ref.id == session.thread_id
    assert host_arg.correlation_id == session.thread_id
    assert host_arg.delivery_channel == "event"
    assert host_arg.agent_type == "persistent"
    assert runtime_factory.call_args.kwargs == {"ledger": ledger}
    assert session.tool_context._parent_host is host_arg
    assert session.tool_context.subagent_runtime is runtime


@pytest.mark.parametrize(
    "missing",
    [
        "orchestrator_client",
        "postgres_conn",
        "session_parent_authority_provider",
        "subagent_provider_admission",
        "subagent_effect_authority",
    ],
)
def test_delegation_enabled_session_fails_closed_when_authority_wiring_is_missing(
    missing: str,
):
    session = _make_session(
        orchestrator_client=object(),
        session_parent_authority_provider=lambda: object(),
        subagent_provider_admission=lambda: True,
        subagent_effect_authority=lambda: True,
    )
    session.postgres_conn = object()
    session.tool_context = _context()
    setattr(session, missing, None)

    with pytest.raises(RuntimeError, match="lacks exact durable parent authority"):
        session._install_session_subagent_runtime()

    assert session.tool_context.subagent_runtime is None


def test_session_without_delegation_controls_does_not_require_a_child_runtime():
    session = _make_session()
    session.tool_context = _context(tools=("read_file",))

    session._install_session_subagent_runtime()

    assert session.tool_context.subagent_runtime is None


def test_fully_wired_session_installs_hidden_runtime_for_revoked_config_recovery():
    session = _make_session(
        orchestrator_client=object(),
        session_parent_authority_provider=lambda: object(),
        subagent_provider_admission=lambda: True,
        subagent_effect_authority=lambda: True,
    )
    session.postgres_conn = object()
    session.tool_context = _context(tools=("read_file",))
    ledger = object()
    runtime = object()

    with (
        patch(
            "agent.subagents.session_persistence.SessionSubagentLedger.from_context",
            return_value=ledger,
        ),
        patch(
            "agent.subagents.runtime.SubagentRuntime.from_context",
            return_value=runtime,
        ),
    ):
        session._install_session_subagent_runtime()

    assert session.tool_context.subagent_runtime is runtime


@pytest.mark.asyncio
async def test_recovery_and_quiescence_delegate_to_the_installed_runtime_once():
    runtime = SimpleNamespace(
        recover_orphans=AsyncMock(),
        quiesce=AsyncMock(),
        resume=AsyncMock(),
    )
    session = _make_session()
    session.tool_context = SimpleNamespace(subagent_runtime=runtime)

    await session.recover_subagents()
    await session.quiesce_subagents("retiring")
    await session.quiesce_subagents("duplicate")
    await session.resume_subagents()
    await session.resume_subagents()
    await session.quiesce_subagents("retiring again")

    runtime.recover_orphans.assert_awaited_once_with()
    assert runtime.quiesce.await_args_list == [
        (("retiring",), {}),
        (("retiring again",), {}),
    ]
    runtime.resume.assert_awaited_once_with()


def test_context_probe_reads_live_manager_state_each_time():
    session = _make_session()
    session.tool_context = SimpleNamespace()
    session.context_manager = SimpleNamespace(
        state=SimpleNamespace(
            last_provider_input_tokens=123,
            current_token_count=456,
        ),
        config=SimpleNamespace(
            compaction_threshold_tokens=789,
            model_max_context_tokens=1000,
        ),
    )

    session._wire_subagent_context_probe()
    first = session.tool_context.parent_context_probe()
    session.context_manager.state.current_token_count = 654
    second = session.tool_context.parent_context_probe()

    assert first.current_token_count == 456
    assert second.current_token_count == 654
    assert second.compaction_threshold_tokens == 789
    assert second.model_max_context_tokens == 1000


@pytest.mark.parametrize(
    ("shell_owner_token", "lane"),
    [(None, "pinned"), (7, "stateless")],
)
def test_tool_setup_publishes_the_session_lane_before_tools_load(
    shell_owner_token, lane
):
    """``delegate_agent`` builds its description when the tool is created, so
    the parent kind and the lane must be on the context before any factory
    runs. Only the stateless executor sets ``shell_owner_token``."""

    session = _make_session(shell_owner_token=shell_owner_token)
    seen: dict = {}

    def _capture() -> None:
        seen["kind"] = session.tool_context._subagent_parent_kind
        seen["lane"] = session.tool_context._subagent_execution_lane

    with (
        patch.object(session, "_load_tools_for_backend", side_effect=_capture),
        patch.object(session, "_install_session_subagent_runtime"),
    ):
        session._setup_tools(None)

    assert seen == {"kind": "session", "lane": lane}
