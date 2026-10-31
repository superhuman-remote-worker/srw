"""JobRunner: one job against the local sshd workspace, Claude Code as FakeClient."""

import asyncio
import dataclasses
import json
from datetime import datetime
from pathlib import Path

import pytest
import pytest_asyncio
from srw_claude_code_harness.job import Limits, parse_job_start
from srw_claude_code_harness.runner import JobRunner
from srw_claude_code_harness.workspace import WorkspaceError
from tests._claude_code_harness_fakes import (
    API_KEY,
    call_tool,
    connect_via,
    fake_client_factory,
    never_connect,
    remote_for,
    result_message,
    sdk_or_skip,
    start_body,
    until,
)
from tests._claude_code_harness_sshd import LocalSshd


@pytest_asyncio.fixture
async def sshd(tmp_path):
    server = await LocalSshd.start(tmp_path / "workspace")
    yield server
    await server.close()


def _runner(tmp_path, *, connect, client_factory=None, stop=None):
    return JobRunner(
        client=None,
        pod_uid="pod-uid-1",
        run_dir=tmp_path / "run",
        stop=stop or asyncio.Event(),
        connect=connect,
        client_factory=client_factory,
    )


def _spec(remote=None, **changes):
    return parse_job_start(start_body(remote, **changes))


@pytest.mark.asyncio
async def test_a_successful_session_becomes_a_reviewable_report(tmp_path, sshd):
    sdk_or_skip()
    clients = []

    async def write_and_edit(options):
        await call_tool(options, "Write", {"file_path": "b.md", "content": "x\n"})
        await call_tool(options, "Write", {"file_path": "a.md", "content": "y\n"})
        await call_tool(
            options,
            "Edit",
            {"file_path": "b.md", "old_string": "x", "new_string": "z"},
        )

    runner = _runner(
        tmp_path,
        connect=connect_via(sshd, []),
        client_factory=fake_client_factory(
            clients, [result_message(result="Report written.")], on_query=write_and_edit
        ),
    )
    report = await runner.run(_spec(remote_for(sshd.access)))
    root = sshd.access.root
    # Default autonomy is "partial": the job waits in Review, as SRW's agent's does.
    assert (report.should_stop, report.goal_achieved, report.error) == (
        True,
        False,
        None,
    )
    freeze = report.freeze_data
    assert freeze["status"] == "job_completed"
    assert freeze["freeze_type"] == "job_complete"
    assert freeze["summary"] == "Report written."
    assert freeze["deliverables"] == ["a.md", "b.md"]
    assert freeze["confidence"] == 1.0 and freeze["job_id"] == "job-1"
    assert freeze["model"] == "claude-test"
    assert datetime.fromisoformat(freeze["timestamp"]).tzinfo is not None

    [client] = clients
    options = client.options
    assert options.model == "claude-test"
    assert options.system_prompt["append"] == "Be terse."
    assert len(options.allowed_tools) == 4 and options.tools == []
    assert options.env["ANTHROPIC_API_KEY"] == API_KEY
    assert Path(options.cwd) == Path(root)
    assert client.disconnected

    copied = Path(root) / ".srw/claude-code/transcript.jsonl"
    kinds = [json.loads(line)["kind"] for line in copied.read_text().splitlines()]
    assert "ResultMessage" in kinds


@pytest.mark.asyncio
async def test_a_route_this_harness_cannot_speak_is_a_job_error(tmp_path):
    connects = []
    runner = _runner(tmp_path, connect=never_connect(connects))
    report = await runner.run(_spec(protocol="openai-responses"))
    assert report.goal_achieved is False and report.freeze_data is None
    assert report.error["type"] == "job_error"
    assert report.error["recoverable"] is False
    assert "anthropic-messages" in report.error["message"]
    assert connects == []


@pytest.mark.asyncio
async def test_an_unreachable_workspace_is_reported_as_unavailable(tmp_path):
    async def connect(access):
        raise WorkspaceError("cannot connect to the workspace: refused")

    report = await _runner(tmp_path, connect=connect).run(_spec())
    assert report.goal_achieved is False
    assert report.error == {
        "message": "cannot connect to the workspace: refused",
        "type": "workspace_unavailable",
        "recoverable": False,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("messages", "expected"),
    [
        (
            lambda: [result_message(subtype="error_max_turns")],
            "run error: error_max_turns",
        ),
        (
            lambda: [result_message(is_error=True)],
            "run error: the result is marked as an error",
        ),
        (lambda: [], "run error: no result"),
    ],
)
async def test_an_unsuccessful_session_is_a_job_error(
    tmp_path, sshd, messages, expected
):
    sdk_or_skip()
    runner = _runner(
        tmp_path,
        connect=connect_via(sshd, []),
        client_factory=fake_client_factory([], messages()),
    )
    report = await runner.run(_spec(remote_for(sshd.access)))
    assert report.goal_achieved is False and report.freeze_data is None
    assert report.error["type"] == "job_error"
    assert report.error["message"].startswith(expected)


@pytest.mark.asyncio
async def test_the_wall_clock_limit_is_a_job_error(tmp_path, sshd):
    sdk_or_skip()
    clients = []
    runner = _runner(
        tmp_path,
        connect=connect_via(sshd, []),
        client_factory=fake_client_factory(clients, [], hang=True),
    )
    spec = dataclasses.replace(
        _spec(remote_for(sshd.access)), limits=Limits(timeout_seconds=0)
    )
    report = await runner.run(spec)
    assert clients[0].interrupted
    assert report.error["type"] == "job_error"
    assert report.error["message"].startswith("timed out")


@pytest.mark.asyncio
async def test_a_stopped_session_returns_no_report(tmp_path, sshd):
    sdk_or_skip()
    clients, stop = [], asyncio.Event()
    runner = _runner(
        tmp_path,
        connect=connect_via(sshd, []),
        client_factory=fake_client_factory(clients, [], hang=True),
        stop=stop,
    )
    run = asyncio.create_task(runner.run(_spec(remote_for(sshd.access))))
    await asyncio.wait_for(until(lambda: clients and clients[0].queried.is_set()), 10)
    stop.set()
    assert await asyncio.wait_for(run, 15) is None
    assert clients[0].interrupted
    # Best effort even after a stop: the transcript is in the workspace.
    assert (Path(sshd.access.root) / ".srw/claude-code/transcript.jsonl").is_file()


@pytest.mark.asyncio
async def test_a_stop_while_connecting_abandons_the_connection(tmp_path):
    stop, cancelled = asyncio.Event(), asyncio.Event()

    async def connect(access):
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    runner = _runner(tmp_path, connect=connect, stop=stop)
    run = asyncio.create_task(runner.run(_spec()))
    await asyncio.sleep(0.05)
    stop.set()
    assert await asyncio.wait_for(run, 5) is None
    assert cancelled.is_set()


@pytest.mark.asyncio
async def test_the_workspace_is_closed_after_the_run(tmp_path, sshd):
    sdk_or_skip()
    closed = []

    async def connect(access):
        workspace = await connect_via(sshd, [])(access)
        original = workspace.close

        async def close():
            closed.append(True)
            await original()

        workspace.close = close
        return workspace

    runner = _runner(
        tmp_path,
        connect=connect,
        client_factory=fake_client_factory([], [result_message()]),
    )
    report = await runner.run(_spec(remote_for(sshd.access)))
    assert report.error is None and report.freeze_data is not None
    assert closed == [True]


@pytest.mark.asyncio
async def test_full_autonomy_reports_the_goal_achieved(tmp_path, sshd):
    sdk_or_skip()
    runner = _runner(
        tmp_path,
        connect=connect_via(sshd, []),
        client_factory=fake_client_factory([], [result_message()]),
    )
    spec = _spec(remote_for(sshd.access), autonomy="full")
    report = await runner.run(spec)
    assert report.goal_achieved is True and report.error is None


@pytest.mark.asyncio
async def test_partial_autonomy_leaves_the_job_for_review(tmp_path, sshd):
    sdk_or_skip()
    runner = _runner(
        tmp_path,
        connect=connect_via(sshd, []),
        client_factory=fake_client_factory([], [result_message()]),
    )
    spec = _spec(remote_for(sshd.access), autonomy="partial")
    report = await runner.run(spec)
    assert report.goal_achieved is False
    assert report.freeze_data["status"] == "job_completed"
