"""Credential-bearing Git remote URLs must not reach model-visible tool output,
the audit trail, the session transcript or the shell-state view.

knowledge-base/knowledge/issues/workspace_git_credentials_in_tool_and_audit_output.md:
a worker ran ``git remote -v`` in a token-authenticated clone and both URLs,
token included, landed in its tool result and in a coordinator's
``get_shell_state`` response. One tool-result string fans out to the model,
the audit row and the archives, so each tool loop redacts it once, where it
builds the ToolMessage. These tests drive the two loops (worker graph and
persistent session/subagent) and the orchestrator's shell-state proxy with a
synthetic token and assert where it must NOT appear — and that the remote,
the diagnostics and the tool arguments survive.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langchain_core.messages import AIMessage, ToolMessage
from langchain_core.tools import tool

from agent.graph import create_audited_tool_node
from agent.persistent_graph import PersistentLoopCallbacks, run_persistent_loop
from agent.tools.shell.coding_tools import _truncate_output
from orchestrator.services import job_diagnostics
from shared.runtime.core.loader import LimitsConfig

# Synthetic, never a real credential. 40 hex: a Gitea token, shaped exactly
# like a commit SHA — so outside a URL only the known value identifies it.
TOKEN = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b"
REMOTE = f"https://oauth2:{TOKEN}@gitea.local/org/repo.git"
REMOTE_V = f"origin\t{REMOTE} (fetch)\norigin\t{REMOTE} (push)\n"
PUSH_FAILURE = (
    f"fatal: unable to access '{REMOTE}/': The requested URL returned error: 403"
)


def _workspace_context(token: str = TOKEN) -> MagicMock:
    """A tool context whose workspace cloned one repository with ``token``."""
    workspace = SimpleNamespace(
        source_repo_meta={"repo": {"forge": "gitea", "token": token}},
        config=SimpleNamespace(git_remote_url=None),
    )
    context = MagicMock()
    context.workspace_manager = workspace
    return context


def _assert_clean(text: str) -> None:
    assert TOKEN not in text
    assert TOKEN[:20] not in text and TOKEN[20:] not in text


# =============================================================================
# Worker graph: create_audited_tool_node
# =============================================================================


@dataclass
class _FakeLLMConfig:
    model: Optional[str] = None


@dataclass
class _FakeConfig:
    agent_id: str = "test_agent"
    limits: LimitsConfig = field(default_factory=LimitsConfig)
    llm: _FakeLLMConfig = field(default_factory=_FakeLLMConfig)


def _graph_state(*calls: dict) -> dict:
    return {
        "messages": [AIMessage(content="", tool_calls=list(calls))],
        "job_id": "test-job",
        "iteration": 1,
        "is_strategic_phase": False,
        "phase_number": 1,
        "metadata": {},
    }


async def _run_graph_batch(tool_messages: list, *, tool_context=None):
    fake_tool = MagicMock()
    fake_tool.name = "shell_execute"
    auditor = MagicMock()
    auditor.audit_tool_call.side_effect = lambda **kw: f"doc-{kw['call_id']}"
    calls = [
        {"name": "shell_execute", "id": m.tool_call_id, "args": {"command": "x"}}
        for m in tool_messages
    ]
    with (
        patch("agent.graph.ToolNode") as tool_node_cls,
        patch("agent.graph.get_archiver", return_value=auditor),
    ):
        tool_node = AsyncMock()
        tool_node.ainvoke = AsyncMock(return_value={"messages": tool_messages})
        tool_node_cls.return_value = tool_node
        node = create_audited_tool_node(
            [fake_tool], _FakeConfig(), tool_context=tool_context
        )
        result = await node(_graph_state(*calls))
    return result, auditor


class TestWorkerGraphToolResults:
    @pytest.mark.asyncio
    async def test_git_remote_v_is_redacted_in_transcript_and_audit(self):
        result, auditor = await _run_graph_batch(
            [ToolMessage(content=REMOTE_V, tool_call_id="c1", name="shell_execute")]
        )

        (message,) = [m for m in result["messages"] if isinstance(m, ToolMessage)]
        _assert_clean(message.content)
        assert message.content.count("gitea.local/org/repo.git") == 2

        audited = auditor.update_tool_result.call_args.kwargs
        _assert_clean(audited["result"])
        assert "gitea.local/org/repo.git" in audited["result"]

    @pytest.mark.asyncio
    async def test_the_error_path_is_redacted_too(self):
        result, auditor = await _run_graph_batch(
            [
                ToolMessage(
                    content=f"Error: {PUSH_FAILURE}",
                    tool_call_id="c1",
                    name="shell_execute",
                    status="error",
                )
            ]
        )

        (message,) = [m for m in result["messages"] if isinstance(m, ToolMessage)]
        _assert_clean(message.content)
        assert "The requested URL returned error: 403" in message.content
        audited = auditor.update_tool_result.call_args.kwargs
        _assert_clean(audited["result"])
        _assert_clean(audited["error"] or "")

    @pytest.mark.asyncio
    async def test_a_bare_workspace_token_is_caught_as_a_known_value(self):
        result, _ = await _run_graph_batch(
            [ToolMessage(content=f"GIT_TOKEN={TOKEN}\n", tool_call_id="c1", name="x")],
            tool_context=_workspace_context(),
        )
        (message,) = [m for m in result["messages"] if isinstance(m, ToolMessage)]
        _assert_clean(message.content)

    @pytest.mark.asyncio
    async def test_the_call_arguments_are_left_alone(self):
        # Only results are redacted: a marker in a write_file body would be
        # written into the file.
        state = _graph_state(
            {"name": "shell_execute", "id": "c1", "args": {"command": REMOTE}}
        )
        fake_tool = MagicMock()
        fake_tool.name = "shell_execute"
        with (
            patch("agent.graph.ToolNode") as tool_node_cls,
            patch("agent.graph.get_archiver", return_value=None),
        ):
            tool_node = AsyncMock()
            tool_node.ainvoke = AsyncMock(
                return_value={
                    "messages": [ToolMessage(content="ok", tool_call_id="c1", name="x")]
                }
            )
            tool_node_cls.return_value = tool_node
            node = create_audited_tool_node([fake_tool], _FakeConfig())
            await node(state)
        sent = tool_node.ainvoke.call_args.args[0]["messages"][-1]
        assert sent.tool_calls[0]["args"]["command"] == REMOTE

    @pytest.mark.asyncio
    async def test_ordinary_output_is_byte_identical(self):
        plain = (
            "origin\thttps://gitea.local/org/repo.git (fetch)\n"
            f"commit {TOKEN[:12]} def check(token: str) -> bool:\n"
        )
        result, _ = await _run_graph_batch(
            [ToolMessage(content=plain, tool_call_id="c1", name="x")]
        )
        (message,) = [m for m in result["messages"] if isinstance(m, ToolMessage)]
        assert message.content == plain


# =============================================================================
# Persistent loop (sessions, and subagent children, which run on it)
# =============================================================================


def _callbacks(**overrides) -> PersistentLoopCallbacks:
    defaults = dict(
        get_user_input=AsyncMock(return_value="hello"),
        on_token=AsyncMock(),
        on_thinking=AsyncMock(),
        on_tool_start=AsyncMock(),
        on_tool_execution_start=AsyncMock(),
        on_tool_result=AsyncMock(),
        permission_check=AsyncMock(return_value=True),
        on_turn_start=AsyncMock(),
        on_turn_complete=AsyncMock(),
        on_error=AsyncMock(),
        check_interrupt=MagicMock(return_value=False),
        on_vm_upgrade_needed=None,
    )
    defaults.update(overrides)
    return PersistentLoopCallbacks(**defaults)


def _one_tool_turn_llm(tool_name: str):
    calls = 0

    async def _astream(messages, **kw):
        nonlocal calls
        calls += 1
        if calls == 1:
            yield AIMessage(
                content="",
                tool_calls=[{"name": tool_name, "args": {}, "id": "tc1"}],
            )
        else:
            yield AIMessage(content="done")

    llm = AsyncMock()
    llm.reasoning = None
    llm.astream = _astream
    return llm


async def _run_session_turn(tool_obj, *, tool_context=None):
    turns = 0

    async def _input():
        nonlocal turns
        turns += 1
        if turns == 1:
            return "show the remotes"
        raise asyncio.CancelledError

    config = MagicMock()
    config.llm.timeout = 600
    config.memory.enabled = False
    config.context_management.max_summary_length = 10000
    callbacks = _callbacks(get_user_input=_input)
    messages: list = []
    await run_persistent_loop(
        llm_with_tools=_one_tool_turn_llm(tool_obj.name),
        tools=[tool_obj],
        context_manager=AsyncMock(
            ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
        ),
        config=config,
        system_prompt="sys",
        callbacks=callbacks,
        messages=messages,
        tool_context=tool_context,
    )
    return callbacks, messages


class TestPersistentLoopToolResults:
    @pytest.mark.asyncio
    async def test_git_remote_v_is_redacted_everywhere_it_fans_out(self):
        @tool
        def git_remotes() -> str:
            """List remotes."""
            return REMOTE_V

        callbacks, messages = await _run_session_turn(git_remotes)

        tool_messages = [m for m in messages if isinstance(m, ToolMessage)]
        assert tool_messages, "the tool never ran"
        for message in tool_messages:
            _assert_clean(message.content)
            assert "gitea.local/org/repo.git" in message.content
        # on_tool_result is the cockpit frame AND (for a subagent) the audit
        # row: it must carry the same cleaned string.
        streamed = callbacks.on_tool_result.call_args.args[1]
        _assert_clean(streamed)
        assert "gitea.local/org/repo.git" in streamed

    @pytest.mark.asyncio
    async def test_a_raised_fetch_failure_is_redacted(self):
        @tool
        def git_fetch() -> str:
            """Fetch."""
            raise RuntimeError(PUSH_FAILURE)

        callbacks, messages = await _run_session_turn(git_fetch)

        streamed = callbacks.on_tool_result.call_args.args[1]
        assert callbacks.on_tool_result.call_args.kwargs.get("is_error") is True
        _assert_clean(streamed)
        assert "returned error: 403" in streamed
        for message in messages:
            if isinstance(message, ToolMessage):
                _assert_clean(message.content)

    @pytest.mark.asyncio
    async def test_a_bare_workspace_token_is_caught_as_a_known_value(self):
        @tool
        def print_env() -> str:
            """Env."""
            return f"GIT_TOKEN={TOKEN}"

        callbacks, _ = await _run_session_turn(
            print_env, tool_context=_workspace_context()
        )
        _assert_clean(callbacks.on_tool_result.call_args.args[1])


# =============================================================================
# Truncation must not strand a fragment
# =============================================================================


class TestTruncationCut:
    def test_a_cut_inside_the_token_leaves_no_fragment(self):
        # The tail is kept; with no newline near the cut, the kept text starts
        # mid-line. Cut exactly inside the token.
        line = f"origin\t{REMOTE} (fetch)"
        text = "x" * 400 + line
        cut_inside = len(text) - (line.index(TOKEN) + 20)
        out = _truncate_output(text, max_chars=len(line) - line.index(TOKEN) - 20)
        assert cut_inside > 0
        _assert_clean(out)
        assert "gitea.local/org/repo.git" in out

    def test_a_huge_output_is_scanned_only_near_the_cut(self):
        # 5 MiB of scrollback: only the kept tail plus a margin is redacted,
        # yet a credential straddling the cut still leaves nothing behind.
        line = f"origin\t{REMOTE} (fetch)"
        text = "x" * 5_000_000 + line
        max_chars = len(line) - line.index(TOKEN) - 20
        out = _truncate_output(text, max_chars=max_chars)
        _assert_clean(out)
        assert out.endswith("@gitea.local/org/repo.git (fetch)")

    def test_a_known_token_cut_inside_a_long_line_leaves_no_fragment(self):
        # No URL shape to recognize and no newline near the cut: only the
        # known value, passed in by the shell tools, identifies it.
        text = "y" * 90_000 + f" GIT_TOKEN={TOKEN} " + "z" * 300
        # 330 keeps the last 29 characters of the token — without the known
        # value, TOKEN[20:] would survive the cut.
        assert TOKEN[20:] in _truncate_output(text, max_chars=330)
        out = _truncate_output(text, max_chars=330, secrets=[TOKEN])
        _assert_clean(out)

    def test_untruncated_output_is_untouched_by_the_cutter(self):
        # Short output is the loop's to redact; the cutter only redacts when it
        # is about to cut.
        assert _truncate_output(REMOTE_V, max_chars=10_000) == REMOTE_V


# =============================================================================
# Orchestrator shell-state proxy (the surface the incident was recorded on)
# =============================================================================


class _ShellResponse:
    status_code = 200
    text = ""

    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


def _shell_client(payload):
    class _Client:
        def __init__(self, *_a, **_kw):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_a):
            return False

        async def post(self, url, json):
            return _ShellResponse(payload)

    return _Client


async def _shell_state(payload):
    target = SimpleNamespace(
        agent={"pod_ip": "10.0.0.9", "pod_port": 8001},
        recipient=SimpleNamespace(model_dump=lambda mode: {}),
    )
    dependencies = job_diagnostics.JobDiagnosticsDependencies(
        workspace=MagicMock(),
        snapshots=MagicMock(),
        audit_reader=MagicMock(),
        prepare_pinned_job_mutation_target=AsyncMock(return_value=target),
    )
    with patch.object(job_diagnostics.httpx, "AsyncClient", _shell_client(payload)):
        return await job_diagnostics.get_job_shell_state(
            job_id="job-1",
            job={"status": "processing", "assigned_agent_id": "agent-1"},
            dependencies=dependencies,
        )


class TestShellStateProxy:
    @pytest.mark.asyncio
    async def test_panes_are_redacted_even_from_an_older_agent(self):
        # The agent image is not trusted to have redacted: the proxy does it.
        result = await _shell_state(
            {
                "tabs": [
                    {"name": "git", "type": "shell", "recent_output": REMOTE_V},
                    {"name": "work", "type": "shell", "recent_output": "ls\nok\n"},
                ]
            }
        )
        git_tab, work_tab = result["tabs"]
        _assert_clean(git_tab["recent_output"])
        assert git_tab["recent_output"].count("gitea.local/org/repo.git") == 2
        assert git_tab["redacted"] is True and git_tab["redacted_count"] == 2
        # A clean pane is returned exactly as before, with no redaction keys.
        assert work_tab == {
            "name": "work",
            "type": "shell",
            "recent_output": "ls\nok\n",
        }

    @pytest.mark.asyncio
    async def test_a_message_only_response_passes_through(self):
        payload = {"tabs": [], "message": "No active shell sessions"}
        assert await _shell_state(payload) == payload

    def test_the_formatter_says_the_pane_was_edited(self):
        from shared.orch_surface.formatters import format_shell_state

        text = format_shell_state(
            "job-1",
            {
                "tabs": [
                    {
                        "name": "git",
                        "type": "shell",
                        "total_lines": 2,
                        "recent_output": "origin https://[REDACTED]@h/x.git",
                        "redacted": True,
                        "redacted_count": 2,
                    }
                ]
            },
        )
        assert "2 secret-shaped value(s) redacted" in text


# =============================================================================
# Known secrets: the agent's own tokens, as pairs, and across a worktree child
# =============================================================================


class TestWorkspaceSecrets:
    def test_repo_tokens_come_with_their_basic_auth_pair(self):
        from agent.core.tool_output_redaction import workspace_secrets

        secrets = workspace_secrets(_workspace_context())
        assert TOKEN in secrets and f"oauth2:{TOKEN}" in secrets

    def test_a_credential_bearing_workspace_remote_contributes_its_pair(self):
        from agent.core.tool_output_redaction import workspace_secrets

        context = MagicMock()
        context.redaction_secrets = ()
        context.workspace_manager = SimpleNamespace(
            source_repo_meta={},
            config=SimpleNamespace(git_remote_url=REMOTE),
        )
        secrets = workspace_secrets(context)
        assert TOKEN in secrets and f"oauth2:{TOKEN}" in secrets

    def test_inherited_secrets_are_honored_without_a_workspace(self):
        from agent.core.tool_output_redaction import redact_tool_result

        context = SimpleNamespace(redaction_secrets=(TOKEN,), workspace_manager=None)
        _assert_clean(redact_tool_result(f"echo {TOKEN}", context))

    def test_a_curl_basic_header_is_caught(self):
        import base64

        from agent.core.tool_output_redaction import redact_tool_result

        header = base64.b64encode(f"oauth2:{TOKEN}".encode()).decode()
        out = redact_tool_result(
            f"> Authorization: Basic {header}\n", _workspace_context()
        )
        assert header not in out
