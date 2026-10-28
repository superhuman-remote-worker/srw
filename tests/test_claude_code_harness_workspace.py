import asyncio
from dataclasses import replace
from pathlib import Path

import pytest
import pytest_asyncio
from srw_claude_code_harness import workspace as workspace_mod
from srw_claude_code_harness.workspace import Workspace, WorkspaceError
from tests._claude_code_harness_sshd import LocalSshd


@pytest_asyncio.fixture
async def sshd(tmp_path):
    server = await LocalSshd.start(tmp_path / "workspace")
    yield server
    await server.close()


@pytest_asyncio.fixture
async def workspace(sshd):
    ws = await Workspace.connect(sshd.access)
    yield ws
    await ws.close()


@pytest.mark.asyncio
async def test_connect_retries_refused_connections_until_sshd_listens(tmp_path):
    """A fresh workspace Pod's sshd and the network policy admitting the harness
    both lag behind the Pod's readiness: a refused connection is retried."""
    server = await LocalSshd.start(tmp_path / "workspace", listen=False)
    loop = asyncio.get_running_loop()
    before = loop.time()
    connecting = asyncio.create_task(
        Workspace.connect(server.access, deadline_seconds=15)
    )
    await asyncio.sleep(1.0)
    assert not connecting.done()  # still retrying against the closed port
    await server.listen()
    ws = await connecting
    try:
        assert (await ws.run("echo ok")).stdout == "ok\n"
        assert 1.0 <= loop.time() - before < 10
    finally:
        await ws.close()
        await server.close()


@pytest.mark.asyncio
async def test_connect_gives_up_after_the_deadline_on_refused_connections(tmp_path):
    server = await LocalSshd.start(tmp_path / "workspace", listen=False)
    before = asyncio.get_running_loop().time()
    with pytest.raises(WorkspaceError, match="cannot connect to the workspace"):
        await Workspace.connect(server.access, deadline_seconds=1.0)
    assert asyncio.get_running_loop().time() - before < 5
    await server.close()


@pytest.mark.asyncio
async def test_connect_does_not_retry_a_rejected_host_key(tmp_path, sshd):
    """A permanent SSH error (another server's host key in known_hosts) fails at
    once instead of burning the deadline."""
    import asyncssh

    other = asyncssh.generate_private_key("ssh-ed25519").export_public_key().decode()
    algorithm, blob = other.split()[:2]
    known_hosts = tmp_path / "wrong_known_hosts"
    known_hosts.write_text(f"[127.0.0.1]:{sshd.access.port} {algorithm} {blob}\n")
    access = sshd.access.__class__(
        **{**sshd.access.__dict__, "known_hosts_file": str(known_hosts)}
    )
    before = asyncio.get_running_loop().time()
    with pytest.raises(WorkspaceError, match="cannot connect to the workspace"):
        await Workspace.connect(access, deadline_seconds=30)
    assert asyncio.get_running_loop().time() - before < 5


@pytest.mark.asyncio
async def test_cwd_persists_between_calls_but_shell_variables_do_not(workspace):
    await workspace.run("mkdir -p sub && cd sub && X=1")
    second = await workspace.run('pwd; echo "X=${X:-unset}"')
    assert second.cwd.endswith("/sub") and second.stdout == f"{second.cwd}\nX=unset\n"


@pytest.mark.asyncio
async def test_env_file_is_loaded_into_every_command(workspace):
    await workspace.write_env_file({"NEO4J_URI": "bolt://db:7687", "PW": "it's"})
    assert (
        await workspace.run("echo $NEO4J_URI $PW")
    ).stdout == "bolt://db:7687 it's\n"


@pytest.mark.asyncio
async def test_exit_code_stdout_and_stderr_are_separated(workspace):
    result = await workspace.run("echo out; echo err >&2; exit 3")
    assert (result.exit_code, result.stdout, result.stderr, result.timed_out) == (
        3,
        "out\n",
        "err\n",
        False,
    )


@pytest.mark.asyncio
async def test_run_kills_the_process_group_on_timeout(workspace, sshd):
    marker = sshd.access.root + "/still-alive"
    result = await workspace.run(f"(sleep 2; touch {marker}) & sleep 30", timeout=1)
    assert result.timed_out is True and result.exit_code == 124
    # The background subshell would touch the marker 2 s in; the group kill at 1 s
    # must have taken it down too.
    await asyncio.sleep(2.5)
    assert not (await workspace.exists(marker))


@pytest.mark.asyncio
async def test_cd_outside_the_root_resets_cwd(workspace, sshd):
    await workspace.run("cd /")
    assert workspace.cwd == sshd.access.root


@pytest.mark.asyncio
async def test_write_read_and_stream_round_trip_binary(workspace):
    data = bytes(range(256)) * 1000
    await workspace.write_bytes("dir/blob.bin", data)
    assert await workspace.read_bytes("dir/blob.bin") == data
    assert await workspace.read_bytes("dir/blob.bin", limit=10) == data[:10]
    assert await workspace.is_binary("dir/blob.bin") is True

    async def chunks():
        for _ in range(3):
            yield b"x" * 65536

    assert await workspace.write_stream("big.txt", chunks()) == 3 * 65536
    assert await workspace.is_binary("big.txt") is False
    collected = b"".join([c async for c in workspace.read_stream("cat big.txt")])
    assert len(collected) == 3 * 65536


def test_resolve_keeps_paths_inside_the_root(sshd):
    ws = Workspace.__new__(Workspace)
    ws.root = "/home/agent-host/workspace"
    ws.cwd = "/home/agent-host/workspace/sub"
    assert ws.resolve("a.csv") == "/home/agent-host/workspace/sub/a.csv"
    assert ws.resolve("/home/agent-host/workspace/x") == "/home/agent-host/workspace/x"
    for bad in ("../../etc/passwd", "/etc/passwd", "/home/agent-host/workspace2/x"):
        with pytest.raises(WorkspaceError):
            ws.resolve(bad)


@pytest.mark.asyncio
async def test_run_reports_a_timeout_when_the_command_ignores_sigterm(workspace):
    result = await workspace.run("trap '' TERM; sleep 30", timeout=1)
    assert result.timed_out is True and result.exit_code in (137, -9)


@pytest.mark.asyncio
async def test_a_deleted_cwd_falls_back_to_the_root(workspace, sshd):
    await workspace.run("mkdir gone && cd gone")
    await workspace.run('rmdir "$PWD"')
    result = await workspace.run("pwd")
    assert result.exit_code == 0 and result.stdout == sshd.access.root + "\n"
    assert "reset" in result.stderr and workspace.cwd == sshd.access.root


@pytest.mark.asyncio
async def test_empty_writes_and_env_file_mode(workspace, sshd):
    await workspace.write_bytes("empty.txt", b"")
    assert await workspace.read_bytes("empty.txt") == b""
    assert await workspace.write_stream("empty2.txt", _no_chunks()) == 0
    await workspace.write_env_file({})
    await workspace.write_env_file({"A": "1"})
    mode = (await workspace.run('stat -c %a "$HOME/.srw-env"')).stdout
    assert mode == "600\n"
    with pytest.raises(WorkspaceError):
        await workspace.write_env_file({"BAD-NAME": "x"})


async def _no_chunks():
    return
    yield b""


@pytest.mark.asyncio
async def test_a_nul_in_a_command_is_refused_and_the_connection_survives(workspace):
    with pytest.raises(WorkspaceError):
        await workspace.run("echo a\x00b")
    assert (await workspace.run("echo ok")).stdout == "ok\n"


@pytest.mark.asyncio
async def test_read_stream_starts_at_the_root_and_reports_failure(workspace, sshd):
    await workspace.run("mkdir -p deep && cd deep")
    output = b"".join([c async for c in workspace.read_stream("pwd")])
    assert output == (sshd.access.root + "\n").encode()
    with pytest.raises(WorkspaceError):
        [c async for c in workspace.read_stream("echo part; false")]


@pytest.mark.asyncio
async def test_a_background_job_does_not_hold_the_call_open(workspace):
    loop = asyncio.get_running_loop()
    started = loop.time()
    result = await workspace.run("echo before; sleep 40 &", timeout=5)
    assert loop.time() - started < 3
    assert (result.stdout, result.exit_code, result.timed_out) == ("before\n", 0, False)


@pytest.mark.asyncio
async def test_captured_output_is_bounded_with_head_and_tail(workspace):
    result = await workspace.run("yes | head -c 2000000", capture_limit=1000)
    assert len(result.stdout.encode()) <= 1100
    assert "\n… [1999000 bytes omitted] …\n" in result.stdout
    assert result.stdout.startswith("y\ny\n") and result.stdout.endswith("y\ny\n")
    assert (result.stdout_bytes, result.stderr_bytes) == (2000000, 0)


@pytest.mark.asyncio
async def test_large_output_spills_to_the_given_file_and_small_output_does_not(
    workspace,
):
    spill = workspace.resolve(".srw/tool-output/1.txt")
    small = await workspace.run("echo hi", spill_path=spill)
    assert small.spilled_to is None and not await workspace.exists(spill)
    big = await workspace.run("yes abc | head -c 50000", spill_path=spill)
    assert big.spilled_to == spill and big.stdout_bytes == 50000
    assert len(await workspace.read_bytes(spill)) == 50000


@pytest.mark.asyncio
async def test_cwd_is_recorded_even_when_the_command_exits_early(workspace):
    result = await workspace.run("mkdir -p sub && cd sub && exit 3")
    assert result.exit_code == 3 and workspace.cwd.endswith("/sub")


@pytest.mark.asyncio
async def test_pagers_are_disabled(workspace):
    assert (await workspace.run("echo $PAGER $GIT_PAGER")).stdout == "cat cat\n"


@pytest.mark.asyncio
async def test_a_cwd_containing_a_newline_round_trips(workspace):
    await workspace.run("mkdir -p $'a\\nb' && cd $'a\\nb'")
    assert workspace.cwd.endswith("/a\nb")
    assert (await workspace.run("pwd")).stdout == workspace.cwd + "\n"


# -- shell start-up files (Ruling R18) --------------------------------------------

_JUNK = (
    "echo junk-out; echo junk-err >&2; printf 'SRW1\\0' ; trap 'echo late-junk' EXIT"
)


@pytest.mark.asyncio
async def test_start_up_output_does_not_corrupt_run(workspace, sshd):
    sshd.prefix = _JUNK
    result = await workspace.run(
        "mkdir -p sub && cd sub && echo hi && echo oops >&2; exit 3"
    )
    assert (result.exit_code, result.stdout, result.stderr) == (3, "hi\n", "oops\n")
    assert result.cwd == sshd.access.root + "/sub" == workspace.cwd


@pytest.mark.asyncio
async def test_start_up_output_does_not_corrupt_file_helpers(workspace, sshd):
    data = bytes(range(256)) * 300
    await workspace.write_bytes("blob.bin", data)
    sshd.prefix = _JUNK
    assert await workspace.read_bytes("blob.bin") == data
    assert await workspace.read_bytes("blob.bin", limit=7) == data[:7]
    assert await workspace.exists("blob.bin") is True
    assert await workspace.exists("nope") is False
    assert await workspace.is_binary("blob.bin") is True
    assert await workspace.stat("blob.bin") == ("regular file", len(data))
    await workspace.write_bytes("copy.bin", data)
    await workspace.write_env_file({"A": "1"})
    sshd.prefix = None
    assert await workspace.read_bytes("copy.bin") == data
    streamed = b"".join([c async for c in workspace.read_stream("cat blob.bin")])
    assert streamed == data


@pytest.mark.asyncio
async def test_a_hanging_start_up_file_cannot_hang_a_helper(
    workspace, sshd, monkeypatch
):
    monkeypatch.setattr(workspace_mod, "_HELPER_GRACE", 0.2)
    monkeypatch.setattr(workspace_mod, "_BACKSTOP_GRACE", 0.2)
    await workspace.write_bytes("keep.txt", b"original")
    sshd.prefix = "sleep 4"
    loop = asyncio.get_running_loop()
    started = loop.time()

    async def stream():
        return [c async for c in workspace.read_stream("cat keep.txt", timeout=1)]

    outcomes = await asyncio.gather(
        workspace.exists("keep.txt", timeout=1),
        workspace.read_bytes("keep.txt", timeout=1),
        workspace.stat("keep.txt", timeout=1),
        workspace.write_bytes("keep.txt", b"replaced", timeout=1),
        workspace.write_env_file({"A": "1"}, timeout=1),
        stream(),
        return_exceptions=True,
    )
    assert loop.time() - started < 3
    for outcome in outcomes:
        assert isinstance(outcome, WorkspaceError) and "timed out" in str(outcome)
    result = await workspace.run("echo hi", timeout=1)
    assert result.timed_out is True and loop.time() - started < 5
    # Once the start-up file finishes, the abandoned write finds its channel
    # closed and dies at the marker, before it can truncate the file.
    sshd.prefix = None
    await asyncio.sleep(max(0.0, 5.5 - (loop.time() - started)))
    assert await workspace.read_bytes("keep.txt") == b"original"


@pytest.mark.asyncio
async def test_endless_start_up_stdout_fails_fast(workspace, sshd):
    sshd.prefix = "yes junk"
    loop = asyncio.get_running_loop()
    started = loop.time()
    with pytest.raises(WorkspaceError, match="before the command started"):
        await workspace.exists("x", timeout=60)
    with pytest.raises(WorkspaceError, match="before the command started"):
        await workspace.run("echo hi", timeout=60)
    assert loop.time() - started < 10


@pytest.mark.asyncio
async def test_endless_start_up_stderr_is_bounded(workspace, sshd, monkeypatch):
    monkeypatch.setattr(workspace_mod, "_HELPER_GRACE", 0.2)
    monkeypatch.setattr(workspace_mod, "_BACKSTOP_GRACE", 0.2)
    seen = []
    collect = workspace_mod._collect

    async def recording(reader, framed, buffer, cap, *, strict):
        seen.append((framed, buffer, cap))
        await collect(reader, framed, buffer, cap, strict=strict)

    monkeypatch.setattr(workspace_mod, "_collect", recording)
    sshd.prefix = "yes junk >&2"
    with pytest.raises(WorkspaceError, match="timed out"):
        await workspace.exists("x", timeout=1)
    result = await workspace.run("echo hi", timeout=1, capture_limit=1000)
    assert result.timed_out is True
    assert 0 < len(result.stderr.encode()) <= 1000 + workspace_mod._CHANNEL_SLACK
    assert len(seen) == 4
    for framed, buffer, cap in seen:
        assert len(buffer) <= cap
        assert len(framed.preamble) <= max(cap, workspace_mod.STDERR_LIMIT_BYTES)


@pytest.mark.asyncio
async def test_helper_output_is_capped(workspace):
    await workspace.run("head -c 100000 /dev/zero > zeros")
    with pytest.raises(WorkspaceError, match="exceeds 1000 bytes"):
        await workspace._exec("cat zeros", stdout_limit=1000)
    error = await workspace._exec("head -c 300000 /dev/zero | tr '\\0' e >&2; exit 4")
    assert error[0] == 4 and len(error[2]) == workspace_mod.STDERR_LIMIT_BYTES


@pytest.mark.asyncio
async def test_write_refuses_a_target_that_is_not_a_regular_file(workspace):
    await workspace.run("mkfifo pipe && mkdir dir")
    for name in ("pipe", "dir"):
        with pytest.raises(WorkspaceError, match="is not a regular file"):
            await asyncio.wait_for(workspace.write_bytes(name, b"x"), 10)


@pytest.mark.asyncio
async def test_run_removes_its_directory_when_the_channel_closes_early(
    workspace, sshd, monkeypatch
):
    # The backstop closes the channel while the command still runs; the outer
    # shell must survive the closed channel (SIGPIPE) and clean up.
    monkeypatch.setattr(workspace_mod, "_BACKSTOP_GRACE", 0.3)
    result = await workspace.run("trap '' TERM; sleep 2; echo done", timeout=1)
    assert result.timed_out is True and result.exit_code == -1
    scratch = Path(sshd.access.root) / ".srw-run"
    for _ in range(50):
        if not any(scratch.iterdir()):
            break
        await asyncio.sleep(0.1)
    assert list(scratch.iterdir()) == []


@pytest.mark.asyncio
async def test_write_stream_stops_feeding_once_the_remote_has_exited(workspace):
    await workspace.run("mkfifo pipe")

    async def slow():
        yield b"x"
        await asyncio.sleep(60)
        yield b"y"

    with pytest.raises(WorkspaceError, match="not a regular file"):
        await asyncio.wait_for(workspace.write_stream("pipe", slow()), 10)


@pytest.mark.asyncio
async def test_connect_accepts_the_matching_fingerprint(sshd):
    access = replace(
        sshd.access, known_hosts_file=None, host_key_fingerprint=sshd.fingerprint
    )
    ws = await Workspace.connect(access)
    await ws.close()


@pytest.mark.asyncio
async def test_connect_refuses_a_wrong_fingerprint(sshd):
    access = replace(
        sshd.access, known_hosts_file=None, host_key_fingerprint="SHA256:" + "A" * 43
    )
    with pytest.raises(WorkspaceError, match="host key mismatch"):
        await Workspace.connect(access)


@pytest.mark.asyncio
async def test_connect_refuses_a_malformed_fingerprint(sshd):
    access = replace(
        sshd.access, known_hosts_file=None, host_key_fingerprint="SHA256:short"
    )
    with pytest.raises(WorkspaceError, match="malformed"):
        await Workspace.connect(access)


@pytest.mark.asyncio
async def test_connect_refuses_without_any_host_key_material(sshd):
    access = replace(sshd.access, known_hosts_file=None, host_key_fingerprint=None)
    with pytest.raises(WorkspaceError, match="no host key"):
        await Workspace.connect(access)
