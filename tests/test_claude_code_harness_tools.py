import asyncio

import pytest
import pytest_asyncio
from srw_claude_code_harness.tools import (
    EDIT_MAX_BYTES,
    OUTPUT_LIMIT_CHARS,
    ToolHandlers,
    make_tools,
    truncate_output,
)
from srw_claude_code_harness.workspace import Workspace
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


@pytest.fixture
def handlers(workspace):
    return ToolHandlers(workspace)


@pytest.mark.asyncio
async def test_bash_returns_stdout_stderr_and_exit_code(handlers):
    out = await handlers.bash({"command": "echo hi; echo bad >&2; exit 2"})
    assert (
        out["content"][0]["text"] == "hi\n\nbad\n\nExit code 2"
        and "is_error" not in out
    )


@pytest.mark.asyncio
async def test_bash_timeout_is_an_error_result(handlers):
    out = await handlers.bash({"command": "sleep 20", "timeout": 1000})
    assert out["is_error"] is True and "timed out" in out["content"][0]["text"]


@pytest.mark.asyncio
async def test_bash_output_is_truncated_and_saved(handlers, workspace, sshd):
    out = await handlers.bash({"command": "yes abcdefghij | head -c 100000"})
    text = out["content"][0]["text"]
    assert (
        len(text) < 31_000
        and "truncated" in text
        and f"{sshd.access.root}/.srw/tool-output/1.txt" in text
    )
    assert len(await workspace.read_bytes(".srw/tool-output/1.txt")) == 100000
    again = await handlers.bash({"command": "yes abcdefghij | head -c 100000"})
    assert f"{sshd.access.root}/.srw/tool-output/2.txt" in again["content"][0]["text"]


@pytest.mark.asyncio
async def test_bash_keeps_cwd_and_survives_concurrent_calls(handlers):
    import asyncio

    await handlers.bash({"command": "mkdir -p d && cd d"})
    a, b = await asyncio.gather(
        handlers.bash({"command": "pwd"}), handlers.bash({"command": "pwd"})
    )
    assert a["content"][0]["text"].strip().endswith("/d")
    assert b["content"][0]["text"].strip().endswith("/d")


@pytest.mark.asyncio
async def test_read_numbers_lines_and_honours_offset_and_limit(handlers, workspace):
    await workspace.write_bytes("f.txt", b"a\nb\nc\nd\n")
    assert (await handlers.read({"file_path": "f.txt", "offset": 2, "limit": 2}))[
        "content"
    ][0]["text"] == "     2\tb\n     3\tc"


@pytest.mark.asyncio
async def test_read_empty_and_long_lines(handlers, workspace):
    await workspace.write_bytes("e.txt", b"")
    assert (await handlers.read({"file_path": "e.txt"}))["content"][0][
        "text"
    ] == "(empty file)"
    await workspace.write_bytes("l.txt", b"x" * 5000 + b"\n")
    line = (await handlers.read({"file_path": "l.txt"}))["content"][0]["text"]
    assert line.endswith("…") and len(line) < 2100


@pytest.mark.asyncio
async def test_read_refuses_binary_and_missing_files(handlers, workspace):
    await workspace.write_bytes("b.bin", b"\x00\x01")
    assert (await handlers.read({"file_path": "b.bin"}))["is_error"] is True
    assert (
        "does not exist"
        in (await handlers.read({"file_path": "nope"}))["content"][0]["text"]
    )


@pytest.mark.asyncio
async def test_read_refuses_paths_outside_the_workspace_root(handlers):
    out = await handlers.read({"file_path": "/etc/passwd"})
    assert out["is_error"] is True and "outside" in out["content"][0]["text"]
    for result in (
        await handlers.write({"file_path": "../../x", "content": "x"}),
        await handlers.edit(
            {"file_path": "/etc/passwd", "old_string": "a", "new_string": "b"}
        ),
    ):
        assert result["is_error"] is True and "outside" in result["content"][0]["text"]


@pytest.mark.asyncio
async def test_write_creates_parents_and_edit_replaces_exactly_once(
    handlers, workspace
):
    await handlers.write({"file_path": "a/b/c.py", "content": "x = 1\ny = x\n"})
    once = await handlers.edit(
        {"file_path": "a/b/c.py", "old_string": "x = 1", "new_string": "x = 2"}
    )
    assert "Replaced 1" in once["content"][0]["text"]
    many = await handlers.edit(
        {"file_path": "a/b/c.py", "old_string": "x", "new_string": "z"}
    )
    assert many["is_error"] is True and "2" in many["content"][0]["text"]
    assert (
        "Replaced 2"
        in (
            await handlers.edit(
                {
                    "file_path": "a/b/c.py",
                    "old_string": "x",
                    "new_string": "z",
                    "replace_all": True,
                }
            )
        )["content"][0]["text"]
    )
    assert await workspace.read_bytes("a/b/c.py") == b"z = 2\ny = z\n"


@pytest.mark.asyncio
async def test_edit_error_cases(handlers, workspace):
    await workspace.write_bytes("f.txt", b"abc\n")
    await workspace.write_bytes("b.bin", b"\x00abc")
    for args in (
        {"file_path": "f.txt", "old_string": "a", "new_string": "a"},
        {"file_path": "f.txt", "old_string": "zzz", "new_string": "y"},
        {"file_path": "nope", "old_string": "a", "new_string": "b"},
        {"file_path": "b.bin", "old_string": "a", "new_string": "b"},
    ):
        assert (await handlers.edit(args))["is_error"] is True
    assert (
        "not found"
        in (
            await handlers.edit(
                {"file_path": "f.txt", "old_string": "zzz", "new_string": "y"}
            )
        )["content"][0]["text"]
    )
    assert await workspace.read_bytes("f.txt") == b"abc\n"


def test_truncate_keeps_head_and_tail():
    text, truncated = truncate_output("H" * 40000 + "T" * 40000, limit=30000)
    assert (
        truncated
        and text.startswith("H" * 100)
        and text.endswith("T" * 100)
        and "truncated" in text
    )
    assert truncate_output("short") == ("short", False)


def test_make_tools_selects_only_the_requested_names(workspace):
    pytest.importorskip("claude_agent_sdk")
    tools = make_tools(workspace, ["Bash", "Read"])
    assert [t.name for t in tools] == ["Bash", "Read"]
    assert set(tools[0].input_schema["properties"]) == {"command", "timeout"}
    assert tools[0].input_schema["required"] == ["command"]


@pytest.mark.asyncio
async def test_read_window_of_a_huge_file_is_fast(handlers, workspace):
    await workspace.run("yes 0123456789 | head -c 50000000 > big.txt")
    out = await asyncio.wait_for(
        handlers.read({"file_path": "big.txt", "limit": 5}), 10
    )
    assert out["content"][0]["text"].count("\n") == 4 and "is_error" not in out


@pytest.mark.asyncio
async def test_read_refuses_devices_and_fifos_without_hanging(handlers, workspace):
    await workspace.run("ln -s /dev/zero z && mkfifo f")
    for name in ("z", "f"):
        out = await asyncio.wait_for(handlers.read({"file_path": name}), 10)
        assert out["is_error"] is True
        assert "not a regular file" in out["content"][0]["text"]


@pytest.mark.asyncio
async def test_read_output_is_capped_with_continuation_hint(handlers, workspace):
    await workspace.write_bytes("w.txt", ("x" * 79 + "\n").encode() * 2000)
    text = (await handlers.read({"file_path": "w.txt"}))["content"][0]["text"]
    assert len(text) <= OUTPUT_LIMIT_CHARS + 200 and "continue with offset=" in text


@pytest.mark.asyncio
async def test_bash_timeout_with_large_output_is_truncated_and_saved(
    handlers, workspace, sshd
):
    out = await handlers.bash(
        {"command": "yes x | head -c 400000; sleep 5", "timeout": 1000}
    )
    text = out["content"][0]["text"]
    path = f"{sshd.access.root}/.srw/tool-output/1.txt"
    assert out["is_error"] is True and "timed out" in text and path in text
    assert len(text) < 31_000 and await workspace.exists(path)


@pytest.mark.asyncio
async def test_saved_output_path_is_absolute_even_after_cd(handlers, sshd):
    await handlers.bash({"command": "mkdir -p proj && cd proj"})
    out = await handlers.bash({"command": "yes abcdefghij | head -c 100000"})
    path = f"{sshd.access.root}/.srw/tool-output/1.txt"
    assert path in out["content"][0]["text"]
    assert "is_error" not in await handlers.read({"file_path": path, "limit": 2})


@pytest.mark.asyncio
async def test_edit_refuses_files_over_the_size_limit(handlers, workspace):
    await workspace.run("head -c 11534336 /dev/zero | tr '\\0' a > huge.txt")
    out = await handlers.edit(
        {"file_path": "huge.txt", "old_string": "a", "new_string": "b"}
    )
    assert out["is_error"] is True and "too large" in out["content"][0]["text"]


_JUNK = "echo junk-out; echo junk-err >&2; trap 'echo late-junk' EXIT"


@pytest.mark.asyncio
async def test_read_and_edit_ignore_start_up_output(handlers, workspace, sshd):
    original = "alpha\nbeta é\ngamma\n".encode()
    await workspace.write_bytes("f.txt", original)
    sshd.prefix = _JUNK
    out = await handlers.read({"file_path": "f.txt"})
    assert out == {
        "content": [
            {"type": "text", "text": "     1\talpha\n     2\tbeta é\n     3\tgamma"}
        ]
    }
    edited = await handlers.edit(
        {"file_path": "f.txt", "old_string": "beta", "new_string": "delta"}
    )
    assert "Replaced 1" in edited["content"][0]["text"]
    written = await handlers.write({"file_path": "g.txt", "content": "x\ny\n"})
    assert "is_error" not in written
    sshd.prefix = None
    assert await workspace.read_bytes("f.txt") == original.replace(b"beta", b"delta")
    assert await workspace.read_bytes("g.txt") == b"x\ny\n"


@pytest.mark.asyncio
async def test_write_refuses_a_fifo_without_hanging(handlers, workspace):
    await workspace.run("mkfifo f")
    out = await asyncio.wait_for(handlers.write({"file_path": "f", "content": "x"}), 10)
    assert out["is_error"] is True
    assert "not a regular file" in out["content"][0]["text"]


@pytest.mark.asyncio
async def test_edit_refuses_a_file_that_grew_past_the_limit_after_the_stat(
    handlers, workspace, monkeypatch
):
    await workspace.run("head -c 11534336 /dev/zero | tr '\\0' a > grown.txt")
    stat = workspace.stat

    async def stale_stat(path, **kwargs):
        kind, _ = await stat(path, **kwargs)
        return kind, 10  # what the stat saw before the file grew

    monkeypatch.setattr(workspace, "stat", stale_stat)
    reads = []
    read_bytes = workspace.read_bytes

    async def recording(path, **kwargs):
        data = await read_bytes(path, **kwargs)
        reads.append((kwargs.get("limit"), len(data)))
        return data

    monkeypatch.setattr(workspace, "read_bytes", recording)
    out = await handlers.edit(
        {"file_path": "grown.txt", "old_string": "a", "new_string": "b"}
    )
    assert out["is_error"] is True and "too large" in out["content"][0]["text"]
    limit = EDIT_MAX_BYTES + 1
    assert (limit, limit) in reads  # the content read was bounded


@pytest.mark.asyncio
async def test_write_and_edit_record_the_resolved_paths(workspace):
    handlers = ToolHandlers(workspace)
    assert handlers.written_paths == set()
    await handlers.write({"file_path": "a.txt", "content": "x"})
    await handlers.edit({"file_path": "a.txt", "old_string": "x", "new_string": "y"})
    await handlers.write({"file_path": "sub/b.txt", "content": "z"})
    await handlers.read({"file_path": "a.txt"})
    await handlers.edit({"file_path": "missing", "old_string": "x", "new_string": "y"})
    assert handlers.written_paths == {
        workspace.resolve("a.txt"),
        workspace.resolve("sub/b.txt"),
    }


def test_make_tools_uses_the_given_handlers(workspace):
    pytest.importorskip("claude_agent_sdk")
    handlers = ToolHandlers(workspace)
    tools = make_tools(workspace, ["Write"], handlers=handlers)
    assert [t.name for t in tools] == ["Write"] and tools[0].handler == handlers.write


def test_tool_names_live_in_tools_and_config_reexports_them():
    from srw_claude_code_harness import config, tools

    assert tools.TOOL_NAMES == ("Bash", "Read", "Write", "Edit")
    assert config.TOOL_NAMES is tools.TOOL_NAMES
