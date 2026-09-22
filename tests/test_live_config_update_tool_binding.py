"""A live ``config.update`` must leave the name list and the factories agreeing.

The bound toolset is an intersection: a name survives only if it is BOTH in
the list ``get_all_tool_names(session.config)`` resolved AND built by its
category factory, which reads ``session.tool_context.config``. ``_setup_tools``
built that dict once, and a live update replaced ``session.config`` without
it, so the two halves read different snapshots — item 3 of
knowledge-base/knowledge/issues/live_config_update_buries_extra_and_empties_the_shell_group.md.

These tests bind through the REAL shell and delegation factories and the real
name resolution (every other category is stubbed). The handler-level ones are
the suite port of the bound-set assertion that issue's k3d gate ran by hand.
"""

import dataclasses
import uuid
from contextlib import ExitStack
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from agent.api.persistent_session import PersistentSession
from agent.tools.registry import TOOL_REGISTRY
from agent.tools.registry import load_tools as real_load_tools
from shared.runtime.core.loader import (
    deep_merge,
    load_agent_config,
    load_agent_config_from_dict,
    resolve_config_path,
)

SHELL_GROUP = ["cancel_command", "run_command", "shell_execute", "shell_read"]
EXECUTORS = {"run_command", "shell_execute"}
DELEGATION_GROUP = [
    "delegate_agent",
    "list_agents",
    "message_agent",
    "stop_agent",
    "wait_agent",
]
_REAL_CATEGORIES = {"shell", "delegation"}


class _Workspace:
    """Shell-capable (sandbox-tier) workspace double."""

    virtual_overlay = None
    is_initialized = True
    job_id = None

    def __init__(self):
        self.backend = SimpleNamespace(
            supports_shell=True, supports_file_tools=True, sudo_action="freeze"
        )
        self._files = {}

    def register_virtual_provider(self, provider):
        pass

    def get_path(self, rel):
        return f"/tmp/ws/{rel}"

    def write_file(self, rel, content):
        self._files[rel] = content

    def exists(self, rel):
        return rel in self._files

    def read_file(self, rel):
        return self._files[rel]

    def delete_file(self, rel):
        self._files.pop(rel, None)


def _named(name):
    tool = MagicMock()
    tool.name = name
    return tool


def _load_tools(names, context):
    """Real factories for the categories under test; stubs for the rest."""
    real = [n for n in names if TOOL_REGISTRY[n].get("category") in _REAL_CATEGORIES]
    return real_load_tools(real, context) + [
        _named(n) for n in names if n not in set(real)
    ]


def _binding_patches():
    stack = ExitStack()
    for target, kwargs in (
        ("agent.api.persistent_session.load_tools", {"side_effect": _load_tools}),
        (
            "agent.api.persistent_session.apply_description_overrides",
            {"side_effect": lambda tools: tools},
        ),
        (
            "agent.api.persistent_session.apply_instruction_enforcement",
            {"side_effect": lambda tools, ctx: tools},
        ),
        (
            "agent.api.persistent_session.supports_parallel_tool_calls",
            {"return_value": False},
        ),
        (
            "shared.runtime.services.guardrails.apply_guardrails_to_tools",
            {"side_effect": lambda tools, model=None: tools},
        ),
    ):
        stack.enter_context(patch(target, **kwargs))
    return stack


def _config(*, shell_mode=None, override=None):
    """``session_base`` with the shell group enumerated, as a session carries it.

    ``shell_mode`` stands in for the family default the settings matrix wrote
    at boot (``gpt-5.6`` → ``persistent``; families without one stay on the
    stateless floor).
    """
    path, deployment_dir = resolve_config_path("session_base")
    data = dataclasses.asdict(load_agent_config(path, deployment_dir))
    data = deep_merge(data, {"tools": {"shell": list(SHELL_GROUP)}, **(override or {})})
    if shell_mode is not None:
        data["extra"].setdefault("shell", {})["mode"] = shell_mode
    return load_agent_config_from_dict(data, deployment_dir=deployment_dir)


def _boot(config):
    session = PersistentSession(thread_id=str(uuid.uuid4()), config=config)
    session.workspace_manager = _Workspace()
    session.shell_manager = MagicMock()
    session._llm = MagicMock()
    with _binding_patches():
        session._setup_tools(None)
    return session


def _bound(session, group):
    return {t.name for t in session.tools} & set(group)


class TestResetupRereadsTheReplacedConfig:
    """``resetup_tools_for_backend`` after ``self.config`` was replaced."""

    def test_shell_mode_flip_binds_the_executor_the_name_list_asked_for(self):
        """A model swap onto a persistent-shell family flips ``shell.mode``.

        Before the fix the factory kept building the boot (stateless) tools
        while the name list asked for ``shell_execute``: the bind kept
        ``cancel_command`` + ``shell_read`` and nothing that can run a command.
        """
        session = _boot(_config())
        assert _bound(session, SHELL_GROUP) == {
            "cancel_command",
            "run_command",
            "shell_read",
        }, "precondition: boot on the stateless floor"

        session.config = _config(shell_mode="persistent")
        with _binding_patches():
            session.resetup_tools_for_backend()

        assert _bound(session, EXECUTORS) == {"shell_execute"}
        assert session.tool_context.config["shell"]["mode"] == "persistent"

    def test_live_delegation_tick_binds_the_delegation_group(self):
        """The pinned-lane twin of the stateless Delegation defect: the tick
        writes names AND ``delegation.enabled``, but the factory gated on the
        boot ``delegation`` block and built nothing."""
        session = _boot(_config())
        assert not _bound(session, DELEGATION_GROUP)

        session.config = _config(
            override={
                "tools": {"delegation": list(DELEGATION_GROUP)},
                "delegation": {"enabled": True},
            }
        )
        with _binding_patches():
            session.resetup_tools_for_backend()

        assert _bound(session, DELEGATION_GROUP) == set(DELEGATION_GROUP)
        assert session.tool_context.config["delegation"]["enabled"] is True

    def test_refresh_is_in_place_and_drops_only_vanished_keys(self):
        """Tools share the dict by reference; a key removed from the config
        goes, the runtime facts stay."""
        session = _boot(_config(override={"retired_setting": {"x": 1}}))
        shared = session.tool_context.config
        assert "retired_setting" in shared

        extra = dict(session.config.extra)
        del extra["retired_setting"]
        session.config = dataclasses.replace(session.config, extra=extra)
        session.refresh_tool_context_config()

        assert session.tool_context.config is shared
        assert "retired_setting" not in shared
        assert shared["cloud_mount"]["root"] == "/cloud"
        assert shared["_resolved_skills"] == extra["_resolved_skills"]


def _handler_patches(monkeypatch, session):
    import agent.api.persistent_app as mod

    client = SimpleNamespace(
        # Historical-generation answer: the enriched override is the fragment.
        update_thread_config=AsyncMock(side_effect=lambda _tid, fragment, **_: fragment)
    )
    broadcast = MagicMock()
    send = AsyncMock()
    monkeypatch.setattr(mod, "_session", session)
    monkeypatch.setattr(mod, "_orchestrator_client", client)
    monkeypatch.setattr(mod, "_thread_id", session.thread_id)
    monkeypatch.setattr(mod, "_ws_send", send)
    monkeypatch.setattr(mod, "_broadcast", broadcast)
    monkeypatch.setattr(mod, "_wire_session_aux_archiver", lambda: None)
    monkeypatch.setattr(mod, "_model_swap_fit_ladder", AsyncMock(return_value=None))
    monkeypatch.setattr(mod, "resolve_memory_extraction_prompt", lambda _cfg: "")
    monkeypatch.setattr(
        "shared.runtime.core.loader.create_llm", lambda *_a, **_k: MagicMock()
    )
    return mod, broadcast, send


@pytest.mark.asyncio
class TestLiveConfigUpdateBoundSet:
    """Through ``_handle_config_update``: what actually binds after the frame."""

    @pytest.mark.parametrize(
        ("boot_mode", "executor"),
        [("persistent", "shell_execute"), (None, "run_command")],
        ids=["persistent-family", "stateless-family"],
    )
    async def test_tools_only_update_keeps_an_executing_shell_tool(
        self, monkeypatch, boot_mode, executor
    ):
        """The original symptom: a tools-only update left ``shell_read`` alone."""
        session = _boot(_config(shell_mode=boot_mode))
        assert executor in _bound(session, EXECUTORS)
        mod, broadcast, send = _handler_patches(monkeypatch, session)

        with _binding_patches():
            await mod._handle_config_update(
                MagicMock(), {"tools": {"shell": list(SHELL_GROUP)}}
            )

        send.assert_not_awaited()
        broadcast.assert_called_once()
        assert _bound(session, EXECUTORS) == {executor}

    async def test_model_swap_onto_a_persistent_family_binds_shell_execute(
        self, monkeypatch
    ):
        session = _boot(_config())
        mod, broadcast, send = _handler_patches(monkeypatch, session)

        with _binding_patches():
            await mod._handle_config_update(
                MagicMock(),
                {
                    "llm": {"model": "gpt-5.6-sol"},
                    "tools": {"shell": list(SHELL_GROUP)},
                },
            )

        send.assert_not_awaited()
        assert session.config.extra["shell"]["mode"] == "persistent"
        assert _bound(session, EXECUTORS) == {"shell_execute"}

    async def test_llm_only_update_refreshes_call_time_tool_config(self, monkeypatch):
        """No reload happens on an llm-only frame; tools that read the model's
        window or modality at call time must still see the new model."""
        session = _boot(_config())
        before = dict(session.tool_context.config)
        mod, broadcast, send = _handler_patches(monkeypatch, session)

        with _binding_patches():
            await mod._handle_config_update(
                MagicMock(), {"llm": {"model": "gpt-5.6-sol"}}
            )

        send.assert_not_awaited()
        config = session.tool_context.config
        assert config["model_max_context_tokens"] == (
            session.config.limits.model_max_context_tokens
        )
        assert config["model_max_context_tokens"] != before["model_max_context_tokens"]
        assert config["multimodal"] is session.config.llm.multimodal
        assert config["shell"]["mode"] == "persistent"
        # Runtime-derived keys ride the asdict round trip and must survive.
        assert config["_resolved_skills"] == before["_resolved_skills"]
