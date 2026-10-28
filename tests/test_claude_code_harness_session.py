import asyncio
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pytest
from srw_claude_code_harness import session as session_module
from srw_claude_code_harness.config import FIXED_SETTINGS, parse_config
from srw_claude_code_harness.session import (
    CLI_FIXED_ENV,
    MCP_SERVER_NAME,
    build_options,
    cli_env,
    run_session,
    scrub_process_environment,
)
from srw_claude_code_harness.transcript import Transcript


def _sdk():
    return pytest.importorskip("claude_agent_sdk")


def _result(subtype="success", is_error=False, result="done"):
    return _sdk().ResultMessage(
        subtype=subtype,
        duration_ms=10,
        duration_api_ms=5,
        is_error=is_error,
        num_turns=1,
        session_id="s-1",
        result=result,
    )


def _assistant(text):
    sdk = _sdk()
    return sdk.AssistantMessage(content=[sdk.TextBlock(text=text)], model="m")


class FakeClient:
    """Stands in for ``ClaudeSDKClient``.

    ``hang`` makes ``receive_messages`` wait after the scripted messages until
    ``interrupt()`` answers with a ResultMessage (unless ``answer_interrupt`` is
    False, in which case it never yields again).
    """

    def __init__(
        self,
        options,
        messages,
        *,
        hang=False,
        answer_interrupt=True,
        fail_receive=None,
        fail_connect=None,
        fail_interrupt=None,
        hang_disconnect=False,
    ):
        self.options = options
        self.messages = list(messages)
        self.hang = hang
        self.answer_interrupt = answer_interrupt
        self.fail_receive = fail_receive
        self.fail_connect = fail_connect
        self.fail_interrupt = fail_interrupt
        self.hang_disconnect = hang_disconnect
        self.prompts = []
        self.connected = False
        self.interrupted = False
        self.disconnected = False
        self._answer = asyncio.Event()
        self._release_disconnect = asyncio.Event()

    async def connect(self):
        if self.fail_connect is not None:
            raise self.fail_connect
        self.connected = True

    async def query(self, prompt):
        self.prompts.append(prompt)

    async def receive_messages(self):
        for message in self.messages:
            yield message
        if self.fail_receive is not None:
            raise self.fail_receive
        if self.hang:
            await self._answer.wait()
            yield _result(subtype="success", is_error=False, result="interrupted")

    async def interrupt(self):
        self.interrupted = True
        if self.fail_interrupt is not None:
            raise self.fail_interrupt
        if self.answer_interrupt:
            self._answer.set()

    async def disconnect(self):
        self.disconnected = True  # called; with hang_disconnect it never returns
        if self.hang_disconnect:
            await self._release_disconnect.wait()

    def release_disconnect(self):
        self._release_disconnect.set()


def _factory(clients, messages=(), **kwargs):
    def make(options):
        client = FakeClient(options, messages, **kwargs)
        clients.append(client)
        return client

    return make


def _lines(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines()]


@pytest.mark.asyncio
async def test_run_session_records_every_message_and_returns_the_result(tmp_path):
    _sdk()
    clients = []
    final = _result()
    messages = [_assistant("working"), final, _assistant("after the result")]
    transcript = Transcript(tmp_path / "transcript.jsonl")
    outcome = await run_session(
        "do the task",
        "opts",
        transcript=transcript,
        deadline_seconds=30,
        stop=asyncio.Event(),
        client_factory=_factory(clients, messages),
    )
    transcript.close()
    client = clients[0]
    assert client.options == "opts" and client.prompts == ["do the task"]
    assert client.connected and client.disconnected and not client.interrupted
    assert outcome.result is final
    assert (outcome.timed_out, outcome.interrupted, outcome.error) == (
        False,
        False,
        None,
    )
    lines = _lines(tmp_path / "transcript.jsonl")
    sdk_lines = [line for line in lines if line["kind"] != "harness"]
    assert [line["kind"] for line in sdk_lines] == ["AssistantMessage", "ResultMessage"]
    assert sdk_lines[0]["payload"]["content"] == [{"text": "working"}]
    assert sdk_lines[1]["payload"]["subtype"] == "success"
    assert "after the result" not in (tmp_path / "transcript.jsonl").read_text()


@pytest.mark.asyncio
async def test_run_session_interrupts_at_the_deadline(tmp_path):
    _sdk()
    clients = []
    transcript = Transcript(tmp_path / "t.jsonl")
    started = time.monotonic()
    outcome = await run_session(
        "p",
        None,
        transcript=transcript,
        deadline_seconds=0.2,
        stop=asyncio.Event(),
        client_factory=_factory(clients, [_assistant("busy")], hang=True),
    )
    transcript.close()
    assert time.monotonic() - started < 5
    assert outcome.timed_out is True and outcome.interrupted is False
    assert outcome.error is None and outcome.result.result == "interrupted"
    assert clients[0].interrupted and clients[0].disconnected
    lines = _lines(tmp_path / "t.jsonl")
    assert [line["kind"] for line in lines if line["kind"] != "harness"] == [
        "AssistantMessage",
        "ResultMessage",
    ]
    events = [line["payload"]["event"] for line in lines if line["kind"] == "harness"]
    assert "deadline" in events


@pytest.mark.asyncio
async def test_run_session_interrupts_when_stop_is_set(tmp_path):
    _sdk()
    clients = []
    stop = asyncio.Event()
    asyncio.get_running_loop().call_later(0.1, stop.set)
    transcript = Transcript(tmp_path / "t.jsonl")
    outcome = await run_session(
        "p",
        None,
        transcript=transcript,
        deadline_seconds=60,
        stop=stop,
        client_factory=_factory(clients, hang=True),
    )
    transcript.close()
    assert outcome.interrupted is True and outcome.timed_out is False
    assert outcome.result is not None and outcome.error is None
    assert clients[0].interrupted and clients[0].disconnected
    events = [
        line["payload"]["event"]
        for line in _lines(tmp_path / "t.jsonl")
        if line["kind"] == "harness"
    ]
    assert "stop" in events


@pytest.mark.asyncio
async def test_run_session_does_not_start_when_stop_is_already_set(tmp_path):
    clients = []
    stop = asyncio.Event()
    stop.set()
    transcript = Transcript(tmp_path / "t.jsonl")
    outcome = await run_session(
        "p",
        None,
        transcript=transcript,
        deadline_seconds=60,
        stop=stop,
        client_factory=_factory(clients),
    )
    transcript.close()
    assert clients == []
    assert outcome.interrupted is True and outcome.result is None


@pytest.mark.asyncio
async def test_run_session_turns_sdk_exceptions_into_error(tmp_path):
    _sdk()
    clients = []
    transcript = Transcript(tmp_path / "t.jsonl")
    outcome = await run_session(
        "p",
        None,
        transcript=transcript,
        deadline_seconds=30,
        stop=asyncio.Event(),
        client_factory=_factory(
            clients, [_assistant("a")], fail_receive=RuntimeError("cli exited 1")
        ),
    )
    transcript.close()
    assert outcome.result is None and "cli exited 1" in outcome.error
    assert outcome.timed_out is False and outcome.interrupted is False
    assert clients[0].disconnected
    errors = [
        line["payload"]
        for line in _lines(tmp_path / "t.jsonl")
        if line["kind"] == "harness" and line["payload"]["event"] == "error"
    ]
    assert errors and "cli exited 1" in errors[0]["error"]


@pytest.mark.asyncio
async def test_run_session_turns_connect_failures_into_error(tmp_path):
    _sdk()
    clients = []
    transcript = Transcript(tmp_path / "t.jsonl")
    outcome = await run_session(
        "p",
        None,
        transcript=transcript,
        deadline_seconds=30,
        stop=asyncio.Event(),
        client_factory=_factory(clients, fail_connect=OSError("no cli")),
    )
    transcript.close()
    assert "no cli" in outcome.error and clients[0].prompts == []
    assert clients[0].disconnected


@pytest.mark.asyncio
async def test_run_session_gives_up_when_the_interrupt_gets_no_answer(tmp_path):
    _sdk()
    clients = []
    transcript = Transcript(tmp_path / "t.jsonl")
    started = time.monotonic()
    # Bounded so a regression fails instead of hanging the suite.
    outcome = await asyncio.wait_for(
        run_session(
            "p",
            None,
            transcript=transcript,
            deadline_seconds=0.2,
            stop=asyncio.Event(),
            client_factory=_factory(clients, hang=True, answer_interrupt=False),
            interrupt_grace_seconds=0.3,
        ),
        timeout=5,
    )
    transcript.close()
    assert time.monotonic() - started < 0.2 + 0.3 + 1.0
    assert outcome.timed_out is True and outcome.result is None
    assert clients[0].interrupted and clients[0].disconnected


@pytest.mark.asyncio
async def test_run_session_stop_path_fits_the_pod_grace_period(tmp_path):
    """SIGTERM with a wedged CLI: no interrupt answer, a disconnect that hangs."""
    _sdk()
    clients = []
    stop = asyncio.Event()
    asyncio.get_running_loop().call_later(0.2, stop.set)
    transcript = Transcript(tmp_path / "t.jsonl")
    started = time.monotonic()
    outcome = await asyncio.wait_for(
        run_session(
            "p",
            None,
            transcript=transcript,
            deadline_seconds=3600,
            stop=stop,
            client_factory=_factory(
                clients, hang=True, answer_interrupt=False, hang_disconnect=True
            ),
        ),
        timeout=30,
    )
    elapsed = time.monotonic() - started
    transcript.close()
    clients[0].release_disconnect()
    await asyncio.sleep(0)
    # 0.2 s + STOP_INTERRUPT_GRACE_SECONDS (5) + STOP_DISCONNECT_WAIT_SECONDS (2)
    assert elapsed < 9
    assert outcome.interrupted is True and outcome.timed_out is False
    assert outcome.result is None and outcome.error is None
    assert clients[0].interrupted and clients[0].disconnected
    events = [
        line["payload"]["event"]
        for line in _lines(tmp_path / "t.jsonl")
        if line["kind"] == "harness"
    ]
    assert "no_result_after_interrupt" in events and "disconnect_abandoned" in events


@pytest.mark.asyncio
async def test_run_session_stop_during_the_deadline_grace_switches_to_the_stop_budget(
    tmp_path, monkeypatch
):
    _sdk()
    monkeypatch.setattr(session_module, "STOP_INTERRUPT_GRACE_SECONDS", 1.0)
    clients = []
    stop = asyncio.Event()
    # The deadline fires at 0.2 s; stop arrives 0.3 s into the 15 s grace.
    asyncio.get_running_loop().call_later(0.5, stop.set)
    transcript = Transcript(tmp_path / "t.jsonl")
    started = time.monotonic()
    outcome = await asyncio.wait_for(
        run_session(
            "p",
            None,
            transcript=transcript,
            deadline_seconds=0.2,
            stop=stop,
            client_factory=_factory(clients, hang=True, answer_interrupt=False),
        ),
        timeout=30,
    )
    elapsed = time.monotonic() - started
    transcript.close()
    # 0.5 s + the (patched) 1 s stop budget, far under the 15 s deadline grace.
    assert elapsed < 3
    assert outcome.timed_out is True and outcome.interrupted is True
    assert outcome.result is None and clients[0].disconnected
    stops = [
        line["payload"]
        for line in _lines(tmp_path / "t.jsonl")
        if line["kind"] == "harness" and line["payload"]["event"] == "stop"
    ]
    assert stops and stops[0]["phase"] == "deadline_grace"


@pytest.mark.asyncio
async def test_run_session_does_not_wait_the_grace_when_the_interrupt_fails(tmp_path):
    _sdk()
    clients = []
    transcript = Transcript(tmp_path / "t.jsonl")
    started = time.monotonic()
    outcome = await run_session(
        "p",
        None,
        transcript=transcript,
        deadline_seconds=0.2,
        stop=asyncio.Event(),
        client_factory=_factory(
            clients, hang=True, fail_interrupt=Exception("Control request timeout")
        ),
    )
    transcript.close()
    assert time.monotonic() - started < 5
    assert outcome.timed_out is True and outcome.result is None
    assert outcome.error is None and clients[0].disconnected
    failed = [
        line["payload"]
        for line in _lines(tmp_path / "t.jsonl")
        if line["kind"] == "harness" and line["payload"]["event"] == "interrupt_failed"
    ]
    assert failed and "Control request timeout" in failed[0]["error"]


@pytest.mark.asyncio
async def test_run_session_reports_a_stream_that_ends_without_a_result(tmp_path):
    _sdk()
    transcript = Transcript(tmp_path / "t.jsonl")
    outcome = await run_session(
        "p",
        None,
        transcript=transcript,
        deadline_seconds=30,
        stop=asyncio.Event(),
        client_factory=_factory([], [_assistant("a")]),
    )
    transcript.close()
    assert outcome.result is None and "without a ResultMessage" in outcome.error


@pytest.mark.asyncio
async def test_run_session_propagates_cancellation_after_cleaning_up(tmp_path):
    _sdk()
    clients = []
    transcript = Transcript(tmp_path / "t.jsonl")
    task = asyncio.create_task(
        run_session(
            "p",
            None,
            transcript=transcript,
            deadline_seconds=60,
            stop=asyncio.Event(),
            client_factory=_factory(clients, hang=True),
        )
    )
    await asyncio.sleep(0.1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    transcript.close()
    assert clients[0].disconnected


def test_cli_env_contains_only_the_fixed_set_and_the_credential(tmp_path):
    env = cli_env(
        parse_config({"version": 1, "model": "m", "autoCompact": False}),
        ("ANTHROPIC_API_KEY", "k"),
        run_dir=tmp_path,
        path="/usr/bin",
    )
    assert set(env) == set(CLI_FIXED_ENV) | {
        "PATH",
        "HOME",
        "CLAUDE_CONFIG_DIR",
        "TMPDIR",
        "DISABLE_AUTO_COMPACT",
        "ANTHROPIC_API_KEY",
    }
    assert env["ANTHROPIC_API_KEY"] == "k" and env["PATH"] == "/usr/bin"
    assert env["HOME"] == str(tmp_path / "home")
    assert env["CLAUDE_CONFIG_DIR"] == str(tmp_path / "claude")
    assert env["TMPDIR"] == str(tmp_path / "tmp")
    assert env["DISABLE_AUTO_COMPACT"] == "1"
    assert all(env[name] == value for name, value in CLI_FIXED_ENV.items())
    assert all((tmp_path / d).is_dir() for d in ("home", "claude", "tmp"))
    assert "ANTHROPIC_BASE_URL" not in env


def test_cli_env_sets_the_base_url_only_when_configured(tmp_path):
    """apiBaseUrl is the one setting that decides where the credential goes:
    it reaches the CLI as ANTHROPIC_BASE_URL and nothing else changes."""
    without = cli_env(
        parse_config({"version": 1, "model": "muse-spark-1.3"}),
        ("ANTHROPIC_API_KEY", "k"),
        run_dir=tmp_path / "a",
        path="/bin",
    )
    with_url = cli_env(
        parse_config(
            {
                "version": 1,
                "model": "muse-spark-1.3",
                "apiBaseUrl": "https://api.meta.ai",
            }
        ),
        ("ANTHROPIC_API_KEY", "k"),
        run_dir=tmp_path / "b",
        path="/bin",
    )
    assert with_url["ANTHROPIC_BASE_URL"] == "https://api.meta.ai"
    assert set(with_url) == set(without) | {"ANTHROPIC_BASE_URL"}


def test_cli_env_with_oauth_token_and_auto_compact_on(tmp_path):
    env = cli_env(
        parse_config({"version": 1, "model": "m"}),
        ("CLAUDE_CODE_OAUTH_TOKEN", "t"),
        run_dir=tmp_path,
        path="/bin",
    )
    assert env["CLAUDE_CODE_OAUTH_TOKEN"] == "t"
    assert "DISABLE_AUTO_COMPACT" not in env and "ANTHROPIC_API_KEY" not in env


def test_cli_env_refuses_other_credential_names(tmp_path):
    with pytest.raises(ValueError):
        cli_env(
            parse_config({"version": 1, "model": "m"}),
            ("LD_PRELOAD", "/x.so"),
            run_dir=tmp_path,
            path="/bin",
        )


def test_cli_fixed_env_values():
    assert CLI_FIXED_ENV == {
        "CLAUDE_CODE_DISABLE_AUTO_MEMORY": "1",
        "CLAUDE_CODE_DISABLE_CLAUDE_MDS": "1",
        "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
        "ENABLE_TOOL_SEARCH": "false",
        "DISABLE_AUTOUPDATER": "1",
        "DISABLE_TELEMETRY": "1",
        "DISABLE_ERROR_REPORTING": "1",
    }


def test_scrub_process_environment_keeps_only_the_named_variables(monkeypatch):
    # Register every current variable with monkeypatch so teardown restores the
    # whole process environment that the scrub removes.
    for name, value in list(os.environ.items()):
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("SECRET_DB", "x")
    monkeypatch.setenv("PATH", "/usr/bin")
    removed = scrub_process_environment(keep=["PATH"])
    assert removed["SECRET_DB"] == "x" and "SECRET_DB" not in os.environ
    assert dict(os.environ) == {"PATH": "/usr/bin"}
    assert "PATH" not in removed


def test_scrub_process_environment_refuses_a_bare_string():
    with pytest.raises(TypeError):
        scrub_process_environment(keep="PATH")


def _stub_workspace():
    from srw_claude_code_harness.workspace import Workspace

    ws = Workspace.__new__(Workspace)
    ws.root = "/home/agent-host/workspace"
    ws.cwd = ws.root
    return ws


def test_build_options_disables_builtins_and_allows_only_the_mcp_tools():
    pytest.importorskip("claude_agent_sdk")
    from srw_claude_code_harness.tools import make_tools

    options = build_options(
        parse_config(
            {
                "version": 1,
                "model": "m",
                "systemPrompt": {"type": "preset", "append": "A"},
                "effort": "high",
                "thinking": {"type": "adaptive"},
                "limits": {"maxTurns": 7, "maxBudgetUsd": 3},
            }
        ),
        tools=make_tools(_stub_workspace(), ["Bash", "Read"]),
        env={"PATH": "/usr/bin"},
        cwd=Path("/tmp"),
    )
    assert options.tools == [] and options.allowed_tools == [
        "mcp__ws__Bash",
        "mcp__ws__Read",
    ]
    assert (
        options.permission_mode == "default"
        and options.setting_sources == []
        and options.strict_mcp_config is True
    )
    assert (
        options.system_prompt
        == {"type": "preset", "preset": "claude_code", "append": "A"}
        and options.effort == "high"
    )
    assert (
        options.hooks is None
        and options.settings is None
        and options.plugins == []
        and options.extra_args == {}
        and options.fallback_model is None
    )
    assert options.cli_path is None and options.add_dirs == []
    assert options.agents is None and options.skills is None
    assert list(options.mcp_servers) == [MCP_SERVER_NAME]
    assert options.mcp_servers[MCP_SERVER_NAME]["type"] == "sdk"
    assert (options.model, options.max_turns, options.max_budget_usd) == ("m", 7, 3.0)
    assert options.thinking == {"type": "adaptive"}
    assert options.env == {"PATH": "/usr/bin"} and options.cwd == Path("/tmp")
    assert options.include_partial_messages is False


def test_build_options_system_prompt_variants():
    pytest.importorskip("claude_agent_sdk")
    from srw_claude_code_harness.tools import make_tools

    tools = make_tools(_stub_workspace(), ["Bash"])

    def prompt(system_prompt):
        data = {"version": 1, "model": "m"}
        if system_prompt is not None:
            data["systemPrompt"] = system_prompt
        return build_options(
            parse_config(data), tools=tools, env={}, cwd=Path("/tmp")
        ).system_prompt

    assert prompt(None) == {"type": "preset", "preset": "claude_code"}
    assert prompt({"type": "custom", "text": "Be terse."}) == "Be terse."


def test_mcp_server_name_matches_the_recorded_fixed_setting():
    assert MCP_SERVER_NAME == "ws" == FIXED_SETTINGS["mcpServerName"]


@dataclass
class _Point:
    x: int
    tags: list


class _Opaque:
    def __repr__(self):
        return "<opaque>"


def test_transcript_writes_one_json_object_per_line(tmp_path):
    path = tmp_path / "sub" / "transcript.jsonl"
    transcript = Transcript(path)
    transcript.record("point", _Point(1, ["a"]))
    transcript.record("harness", {"event": "deadline", "obj": _Opaque()})
    transcript.record("opaque", _Opaque())
    # Every line is on disk before close (a crash mid-run leaves a readable file).
    lines = _lines(path)
    transcript.close()
    transcript.close()
    assert [line["kind"] for line in lines] == ["point", "harness", "opaque"]
    assert lines[0]["payload"] == {"x": 1, "tags": ["a"]}
    assert lines[1]["payload"] == {"event": "deadline", "obj": "<opaque>"}
    assert lines[2]["payload"] == "<opaque>"
    stamp = datetime.fromisoformat(lines[0]["ts"])
    assert stamp.utcoffset() == timezone.utc.utcoffset(None)


def test_transcript_records_sdk_messages_as_objects(tmp_path):
    _sdk()
    transcript = Transcript(tmp_path / "t.jsonl")
    transcript.record("ResultMessage", _result())
    transcript.close()
    payload = _lines(tmp_path / "t.jsonl")[0]["payload"]
    assert payload["subtype"] == "success" and payload["session_id"] == "s-1"


def test_transcript_survives_payloads_that_cannot_be_serialised(tmp_path):
    cyclic: dict = {}
    cyclic["self"] = cyclic
    transcript = Transcript(tmp_path / "t.jsonl")
    transcript.record("cyclic", cyclic)
    transcript.record("tuple-keys", {(1, 2): "v"})
    transcript.close()
    lines = _lines(tmp_path / "t.jsonl")
    assert [line["kind"] for line in lines] == ["cyclic", "tuple-keys"]
    assert all(isinstance(line["payload"], str) for line in lines)
