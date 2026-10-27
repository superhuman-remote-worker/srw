import asyncio

import pytest
import pytest_asyncio
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
