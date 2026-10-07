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
        seen["settle"] = session.tool_context._session_subagent_batch_settle_contract
        seen["fanout"] = session.tool_context._session_subagent_fanout

    with (
        patch.object(session, "_load_tools_for_backend", side_effect=_capture),
        patch.object(session, "_install_session_subagent_runtime"),
    ):
        session._setup_tools(None)

    assert seen == {"kind": "session", "lane": lane, "settle": False, "fanout": False}


@pytest.mark.parametrize("advertised", [False, True])
def test_tool_setup_publishes_the_batch_settle_capability_before_tools_load(
    advertised,
):
    """The orchestrator's two fan-out inputs (parallel_subagents.md §12): the
    attach payload's ``session_subagent_batch_settle_contract`` and its
    operator switch ``session_subagent_fanout``, on the context before any
    factory builds the delegate_agent description."""

    session = _make_session(
        subagent_batch_settle_contract=advertised, subagent_fanout=advertised
    )
    seen: dict = {}

    def _capture() -> None:
        seen["settle"] = session.tool_context._session_subagent_batch_settle_contract
        seen["fanout"] = session.tool_context._session_subagent_fanout

    with (
        patch.object(session, "_load_tools_for_backend", side_effect=_capture),
        patch.object(session, "_install_session_subagent_runtime"),
    ):
        session._setup_tools(None)

    assert seen == {"settle": advertised, "fanout": advertised}
    # The tool config carries the parent's parallel-tool-call ability live.
    assert session.tool_context.config["parallel_tool_calls"] is (
        session.config.llm.parallel_tool_calls
    )


def test_live_config_refresh_hands_a_raised_cap_to_queued_children():
    """``refresh_delegation_description`` runs on every live config update; it
    wakes the runtime's limiter even when no delegate_agent tool is bound (the
    hidden lifecycle runtime still holds queued children)."""

    session = _make_session()
    runtime = SimpleNamespace(refresh_concurrency=MagicMock(return_value=2))
    session.tool_context = SimpleNamespace(subagent_runtime=runtime)
    session.tools = None

    assert session.refresh_delegation_description() is False
    runtime.refresh_concurrency.assert_called_once_with()


@pytest.mark.parametrize(
    ("shell_owner_token", "background"), [(None, True), (7, False)]
)
@pytest.mark.asyncio
async def test_the_prompt_floor_offers_background_only_where_the_lane_has_it(
    shell_owner_token, background
):
    """A stateless session cannot run a background child, so its delegation
    floor must not offer one (the tool description says the same since WP0)."""

    session = _make_session(shell_owner_token=shell_owner_token)
    with (
        patch.object(session, "_setup_workspace", new_callable=AsyncMock),
        patch.object(session, "_setup_tools"),
        patch.object(session, "_bind_tools"),
        patch.object(session, "_setup_context_manager"),
        patch.object(session, "_setup_shell_manager"),
        patch.object(session, "_setup_memory"),
        patch(
            "agent.api.persistent_session.get_phase_system_prompt",
            return_value="prompt",
        ) as build,
    ):
        await session.setup(llm=MagicMock())

    assert build.call_args.kwargs["delegation_background_available"] is background


@pytest.mark.asyncio
async def test_an_attached_sessions_host_reads_its_retirement_from_the_termination_owner():
    """P6 review: the subagent host of an attached session reads whether its
    retirement was authorized from the pinned runtime's termination
    coordinator (``retirement_authorized_now``), through the real attach
    composition, bound to the life that attached it."""
    import agent.api.persistent_app as pa
    from tests.test_session_delegation_fanout_config import (
        _attach_until_construction,
        _lite_workspace,
    )

    seen = await _attach_until_construction(_lite_workspace())
    session = _make_session(
        orchestrator_client=object(),
        session_parent_authority_provider=lambda: object(),
        subagent_provider_admission=lambda: True,
        subagent_effect_authority=lambda: True,
        subagent_retirement_authorized=seen["subagent_retirement_authorized"],
    )
    session.postgres_conn = object()
    session.tool_context = _context()
    with (
        patch(
            "agent.subagents.session_persistence.SessionSubagentLedger.from_context",
            return_value=object(),
        ),
        patch(
            "agent.subagents.runtime.SubagentRuntime.from_context",
            return_value=object(),
        ) as runtime_factory,
    ):
        session._install_session_subagent_runtime()
    (_, host) = runtime_factory.call_args.args

    with patch.object(
        pa._session_termination,
        "retirement_authorized_now",
        new=AsyncMock(return_value=True),
    ) as read_now:
        assert await host.retirement_authorized() is True
    ((life,), _) = read_now.await_args
    assert life[0] == "11111111-1111-4111-8111-111111111111"
