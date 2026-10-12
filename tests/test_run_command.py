"""Tests for the run_command tool and shell mode toggle.

ShellManager delegates to the workspace backend, so these tests script
``backend.shell_run`` returns to exercise the tool layer — mode toggle,
tail truncation, still-running passthrough, interactive-prompt translation,
error-pattern warnings — without tmux. The live shell execution machinery
is RemoteBackend's, covered by test_workspace_backends.py.
"""

import uuid
from unittest.mock import MagicMock, patch

import pytest

from agent.tools.shell.shell_manager import (
    COLLIDING_COMMAND_TEMPLATE,
    STILL_RUNNING_TEMPLATE,
    ShellManager,
)
from agent.tools.shell.shell_tools import create_shell_tools


def make_backend():
    """Mock workspace backend with shell support and a benign default."""
    backend = MagicMock()
    backend.supports_shell = True
    backend.shell_run.return_value = "Exit code: 0\n--- stdout ---\nok"
    backend.shell_send.return_value = "Sent"
    backend.shell_read.return_value = ("output", {"tab": "default"})
    backend.shell_read_with_offset.return_value = (
        "output",
        {"tab": "default"},
    )
    backend.shell_format_tab_header.return_value = "[Shells: default]"
    return backend


@pytest.fixture
def backend():
    return make_backend()


@pytest.fixture
def manager(backend):
    return ShellManager(job_id=str(uuid.uuid4()), backend=backend)


def _make_context(manager, mode="stateless"):
    """Create a mock ToolContext with the given shell mode."""
    context = MagicMock()
    context.shell_manager = manager

    config = {
        "shell": {"mode": mode},
        "max_output_chars": 50000,
        "shell_max_read_lines": 200,
    }
    context.get_config = lambda key, default=None: config.get(key, default)
    return context


class TestModeToggle:
    """Tests for shell mode-based tool selection."""

    def test_stateless_mode_returns_run_command(self, manager):
        context = _make_context(manager, mode="stateless")
        tools = create_shell_tools(context)
        tool_names = {t.name for t in tools}
        assert "run_command" in tool_names
        assert "shell_read" in tool_names
        assert "shell_execute" not in tool_names

    def test_persistent_mode_returns_shell_execute(self, manager):
        context = _make_context(manager, mode="persistent")
        tools = create_shell_tools(context)
        tool_names = {t.name for t in tools}
        assert "shell_execute" in tool_names
        assert "shell_read" in tool_names
        assert "run_command" not in tool_names

    def test_default_mode_is_stateless(self, manager):
        """When no mode is configured, default to stateless."""
        context = MagicMock()
        context.shell_manager = manager
        context.get_config = lambda key, default=None: {
            "max_output_chars": 50000,
            "shell_max_read_lines": 200,
        }.get(key, default)

        tools = create_shell_tools(context)
        tool_names = {t.name for t in tools}
        assert "run_command" in tool_names
        assert "shell_execute" not in tool_names


class TestFactoryFollowsTheResolvedNameList:
    """``shell.mode`` is resolved ONCE, by ``get_all_tool_names``' aliasing.

    The bind is the intersection of that name list and what the factory
    builds; while the factory re-read the mode from its own config, a
    disagreement bound no executor at all (the ``shell_read``-only session in
    knowledge-base/knowledge/issues/live_config_update_buries_extra_and_empties_the_shell_group.md,
    item 5). ``load_tools`` now hands the factory the resolved names.
    """

    @pytest.mark.parametrize(
        ("config_mode", "requested"),
        [
            ("stateless", ["shell_execute", "shell_read"]),
            ("persistent", ["cancel_command", "run_command", "shell_read"]),
        ],
        ids=["persistent-names", "stateless-names"],
    )
    def test_load_tools_binds_every_resolved_name(
        self, manager, config_mode, requested
    ):
        from agent.tools.registry import load_tools

        context = _make_context(manager, mode=config_mode)
        names = {t.name for t in load_tools(requested, context)}
        assert names == set(requested)

    @pytest.mark.parametrize("mode", ["stateless", "persistent"])
    def test_names_without_one_executor_leave_the_mode_to_config(self, manager, mode):
        context = _make_context(manager, mode=mode)
        executor = "shell_execute" if mode == "persistent" else "run_command"
        for requested in (["shell_read"], ["run_command", "shell_execute"]):
            names = {
                t.name for t in create_shell_tools(context, requested_names=requested)
            }
            assert executor in names


class TestCancelCommand:
    """cancel_command exposes a Ctrl+C/abort for the stateless tool set."""

    def _get_cancel(self, manager):
        context = _make_context(manager, mode="stateless")
        tools = create_shell_tools(context)
        return next(t for t in tools if t.name == "cancel_command")

    def test_present_in_stateless_mode(self, manager):
        context = _make_context(manager, mode="stateless")
        names = {t.name for t in create_shell_tools(context)}
        assert "cancel_command" in names

    def test_absent_in_persistent_mode(self, manager):
        context = _make_context(manager, mode="persistent")
        names = {t.name for t in create_shell_tools(context)}
        assert "cancel_command" not in names

    def test_delegates_to_manager_cancel(self, manager, backend):
        backend.shell_cancel.return_value = (
            "Sent Ctrl+C to tab 'default'; the command was interrupted "
            "and the tab is free."
        )
        tool = self._get_cancel(manager)
        result = tool.invoke({})
        backend.shell_cancel.assert_called_once_with("default")
        assert "free" in result.lower()


class TestRunCommand:
    """Tests for the run_command tool layer over a scripted backend."""

    def _get_run_command(self, manager):
        context = _make_context(manager, mode="stateless")
        tools = create_shell_tools(context)
        return next(t for t in tools if t.name == "run_command")

    def test_basic_execution(self, manager, backend):
        backend.shell_run.return_value = "Exit code: 0\n--- stdout ---\nhello-world"
        tool = self._get_run_command(manager)
        result = tool.invoke({"command": "echo hello-world"})
        assert "Exit code: 0" in result
        assert "hello-world" in result
        assert backend.shell_run.call_args[0][0] == "echo hello-world"
        assert backend.shell_run.call_args[1]["working_dir"] == "."

    def test_handoff_lost_tab_uses_tool_level_missing_tab_error(self, manager, backend):
        backend.shell_ensure_tab.side_effect = KeyError(
            "Tab 'default' no longer exists. Available: build"
        )
        tool = self._get_run_command(manager)

        result = tool.invoke({"command": "echo must-not-run"})

        assert result == ("Error: \"Tab 'default' no longer exists. Available: build\"")
        backend.shell_run.assert_not_called()

    def test_working_dir_forwarded(self, manager, backend):
        tool = self._get_run_command(manager)
        tool.invoke({"command": "pwd", "working_dir": "repo"})
        assert backend.shell_run.call_args[1]["working_dir"] == "repo"

    def test_null_working_dir_is_normalized_to_workspace_root(self, manager, backend):
        tool = self._get_run_command(manager)
        tool.invoke({"command": "pwd", "working_dir": None})
        assert backend.shell_run.call_args[1]["working_dir"] == "."

    def test_exit_code_captured(self, manager, backend):
        backend.shell_run.return_value = "Exit code: 1\n(no output)"
        tool = self._get_run_command(manager)
        result = tool.invoke({"command": "false"})
        assert "Exit code: 1" in result

    def test_no_tab_header_in_output(self, manager, backend):
        backend.shell_run.return_value = "Exit code: 0\n--- stdout ---\ntest"
        tool = self._get_run_command(manager)
        result = tool.invoke({"command": "echo test"})
        assert "[Shells:" not in result

    def test_tail_truncation(self, manager, backend):
        lines = "\n".join(f"line_{i}" for i in range(1, 51))
        backend.shell_run.return_value = f"Exit code: 0\n--- stdout ---\n{lines}"
        tool = self._get_run_command(manager)
        result = tool.invoke({"command": "seq", "tail": 10})
        assert "line_50" in result  # Last line present
        assert "line_1\n" not in result  # Early lines truncated
        assert "truncated" in result

    def test_default_tail_is_30(self, manager, backend):
        lines = "\n".join(f"line_{i}" for i in range(1, 61))
        backend.shell_run.return_value = f"Exit code: 0\n--- stdout ---\n{lines}"
        tool = self._get_run_command(manager)
        result = tool.invoke({"command": "seq"})
        assert "line_60" in result
        assert "30 lines truncated" in result

    def test_blocked_command_rejected(self, manager, backend):
        tool = self._get_run_command(manager)
        result = tool.invoke({"command": "reboot"})
        assert "blocked" in result.lower() or "Error" in result
        backend.shell_run.assert_not_called()

    def test_timeout_forwarded_to_backend(self, manager, backend):
        tool = self._get_run_command(manager)
        tool.invoke({"command": "sleep 5", "timeout": 42})
        assert backend.shell_run.call_args[1]["timeout"] == 42

    def test_omitted_timeout_forwarded_as_none(self, manager, backend):
        """None timeout passes through so the backend applies its soft
        no-change timeout (an explicit timeout disables it backend-side)."""
        tool = self._get_run_command(manager)
        tool.invoke({"command": "echo hi"})
        assert backend.shell_run.call_args[1]["timeout"] is None

    def test_still_running_passthrough_not_error(self, manager, backend):
        """A still-running result passes through honestly — not the old
        'requires interactive input' error (the original stall bug)."""
        backend.shell_run.return_value = STILL_RUNNING_TEMPLATE.format(
            tab="default", elapsed=30, quiet=30, terminal_state="(working)"
        )
        tool = self._get_run_command(manager)
        result = tool.invoke({"command": "sleep 999"})
        assert "still running" in result.lower()
        assert "Exit code: -1" in result
        assert "requires interactive input" not in result.lower()

    def test_interactive_prompt_becomes_error(self, manager, backend):
        """Stateless run_command can't answer prompts — the tool translates
        the backend's interactive-prompt report into a clear error."""
        backend.shell_run.return_value = (
            "Interactive prompt detected (password prompt). The command is "
            "waiting for input on tab 'default'.\n"
            "--- terminal state ---\nPassword:"
        )
        tool = self._get_run_command(manager)
        result = tool.invoke({"command": "ssh host"})
        assert "requires interactive input" in result
        assert "non-interactive" in result

    def test_still_running_suggests_cancel(self, manager, backend):
        """A still-running result points the model at cancel_command."""
        backend.shell_run.return_value = STILL_RUNNING_TEMPLATE.format(
            tab="default", elapsed=120, quiet=30, terminal_state="(empty)"
        )
        tool = self._get_run_command(manager)
        result = tool.invoke({"command": "pytest tests/"})
        assert "cancel_command" in result

    def test_colliding_command_suggests_cancel(self, manager, backend):
        """A wedged tab (previous command still running) points at cancel_command."""
        backend.shell_run.return_value = COLLIDING_COMMAND_TEMPLATE.format(
            tab="default", terminal_state="(empty)"
        )
        tool = self._get_run_command(manager)
        result = tool.invoke({"command": "ps -ef"})
        assert "cancel_command" in result

    def test_interactive_prompt_suggests_cancel(self, manager, backend):
        """A stateless agent stuck at a prompt is told it can cancel_command."""
        backend.shell_run.return_value = (
            "Interactive prompt detected (password prompt). The command is "
            "waiting for input on tab 'default'.\n"
            "--- terminal state ---\nPassword:"
        )
        tool = self._get_run_command(manager)
        result = tool.invoke({"command": "ssh host"})
        assert "cancel_command" in result

    def test_error_pattern_warning(self, manager, backend):
        backend.shell_run.return_value = (
            "Exit code: 0\n--- stdout ---\nTraceback (most recent call last):"
        )
        tool = self._get_run_command(manager)
        result = tool.invoke({"command": "python broken.py"})
        assert "Possible error" in result or "Python traceback" in result

    def test_no_output_command(self, manager, backend):
        backend.shell_run.return_value = "Exit code: 0\n(no output)"
        tool = self._get_run_command(manager)
        result = tool.invoke({"command": "true"})
        assert "Exit code: 0" in result


class TestShellExecuteWorkingDir:
    """The persistent tool applies working_dir only to command paths."""

    def _get_shell_execute(self, manager):
        context = _make_context(manager, mode="persistent")
        tools = create_shell_tools(context)
        return next(t for t in tools if t.name == "shell_execute")

    def test_sync_working_dir_forwarded(self, manager, backend):
        tool = self._get_shell_execute(manager)
        tool.invoke({"command": "pwd", "working_dir": "repo"})
        backend.shell_run.assert_called_once_with(
            "pwd",
            tab_name="default",
            timeout=None,
            working_dir="repo",
        )

    def test_async_working_dir_forwarded_to_send(self, manager, backend):
        tool = self._get_shell_execute(manager)
        with patch("agent.tools.shell.shell_tools.time.sleep"):
            tool.invoke(
                {
                    "command": "npm run dev",
                    "name": "dev",
                    "is_async": True,
                    "working_dir": "repo",
                }
            )
        backend.shell_send.assert_called_once_with(
            "dev",
            "npm run dev",
            enter=True,
            working_dir="repo",
            allow_busy=False,
        )

    def test_keys_mode_ignores_working_dir(self, manager, backend):
        tool = self._get_shell_execute(manager)
        with patch("agent.tools.shell.shell_tools.time.sleep"):
            tool.invoke(
                {
                    "command": "C-c",
                    "keys": True,
                    "working_dir": "repo",
                }
            )
        backend.shell_send.assert_called_once_with(
            "default",
            "C-c",
            enter=False,
            working_dir=None,
            allow_busy=True,
        )
        backend.shell_run.assert_not_called()


class TestToolNameAliasing:
    """Tests for shell mode aliasing in get_all_tool_names."""

    def test_stateless_aliases_shell_execute_to_run_command(self):
        from shared.runtime.core.loader import get_all_tool_names

        config = MagicMock()
        config.tools.workspace = []
        config.tools.core = []
        config.tools.document = []
        config.tools.research = []
        config.tools.browser_direct = []
        config.tools.citation = []
        config.tools.graph = []
        config.tools.sql = []
        config.tools.mongodb = []
        config.tools.git = []
        config.tools.shell = ["shell_execute", "shell_read"]
        config.tools.evaluation = []
        config.tools.knowledge = []
        config.tools.communication = []
        config.tools.webdav = []
        config.tools.delegation = []
        config.tools.orchestrator = []
        config.extra = {"shell": {"mode": "stateless"}}

        names = get_all_tool_names(config)
        assert "run_command" in names
        assert "shell_execute" not in names
        assert "shell_read" in names

    def test_persistent_aliases_run_command_to_shell_execute(self):
        from shared.runtime.core.loader import get_all_tool_names

        config = MagicMock()
        config.tools.workspace = []
        config.tools.core = []
        config.tools.document = []
        config.tools.research = []
        config.tools.browser_direct = []
        config.tools.citation = []
        config.tools.graph = []
        config.tools.sql = []
        config.tools.mongodb = []
        config.tools.git = []
        config.tools.shell = ["run_command", "shell_read"]
        config.tools.evaluation = []
        config.tools.knowledge = []
        config.tools.communication = []
        config.tools.webdav = []
        config.tools.delegation = []
        config.tools.orchestrator = []
        config.extra = {"shell": {"mode": "persistent"}}

        names = get_all_tool_names(config)
        assert "shell_execute" in names
        assert "run_command" not in names
        assert "shell_read" in names

    def test_default_mode_is_stateless(self):
        from shared.runtime.core.loader import get_all_tool_names

        config = MagicMock()
        config.tools.workspace = []
        config.tools.core = []
        config.tools.document = []
        config.tools.research = []
        config.tools.browser_direct = []
        config.tools.citation = []
        config.tools.graph = []
        config.tools.sql = []
        config.tools.mongodb = []
        config.tools.git = []
        config.tools.shell = ["shell_execute"]
        config.tools.evaluation = []
        config.tools.knowledge = []
        config.tools.communication = []
        config.tools.webdav = []
        config.tools.delegation = []
        config.tools.orchestrator = []
        config.extra = {}  # No shell config at all

        names = get_all_tool_names(config)
        assert "run_command" in names
