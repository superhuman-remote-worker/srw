import asyncio
import dataclasses
import hashlib
import json
import os
import tarfile
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import pytest
import pytest_asyncio
from srw_claude_code_harness import bundle as bundle_mod
from srw_claude_code_harness.bundle import (
    BundleError,
    archive_workspace,
    build_bundle,
    stage_inputs,
    upload,
)
from srw_claude_code_harness.config import HarnessConfig, Limits, SystemPrompt
from srw_claude_code_harness.result import write_result
from srw_claude_code_harness.session import SessionOutcome
from srw_claude_code_harness.task import InputFile, Task, UploadTarget
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


@pytest.mark.asyncio
async def test_stage_input_streams_in_chunks(workspace):
    data = b"x" * (5 * 1024 * 1024)
    reads = 0

    class Body(httpx.AsyncByteStream):
        async def __aiter__(self):
            nonlocal reads
            for i in range(0, len(data), 1024 * 1024):
                reads += 1
                yield data[i : i + 1024 * 1024]

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(200, stream=Body()))
    )
    out = await stage_inputs(
        workspace, [InputFile("http://x/a", "in/a.bin")], client=client
    )
    assert out == [
        {
            "path": "in/a.bin",
            "bytes": len(data),
            "sha256": hashlib.sha256(data).hexdigest(),
        }
    ]
    assert reads >= 5
    assert await workspace.read_bytes("in/a.bin") == data


@pytest.mark.asyncio
async def test_stage_input_http_error_raises(workspace):
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(404))
    )
    with pytest.raises(BundleError, match="input in/a.bin: HTTP 404"):
        await stage_inputs(
            workspace, [InputFile("http://x/a", "in/a.bin")], client=client
        )


@pytest.mark.asyncio
async def test_stage_input_midway_failure_removes_partial_file(workspace):
    class Body(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b"partial"
            raise httpx.ReadError("boom")

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(200, stream=Body()))
    )
    with pytest.raises(BundleError, match="in/a.bin"):
        await stage_inputs(
            workspace, [InputFile("http://x/a", "in/a.bin")], client=client
        )
    assert not await workspace.exists("in/a.bin")


@pytest.mark.asyncio
async def test_stage_input_is_bounded_in_time(workspace):
    class Body(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b"partial"
            await asyncio.sleep(60)

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(200, stream=Body()))
    )
    loop = asyncio.get_running_loop()
    started = loop.time()
    with pytest.raises(BundleError, match="in/a.bin: .*timed out"):
        await stage_inputs(
            workspace, [InputFile("http://x/a", "in/a.bin")], client=client, timeout=1
        )
    assert loop.time() - started < 10
    assert not await workspace.exists("in/a.bin")


def _archived(dest) -> dict[str, bytes]:
    with tarfile.open(dest, "r:gz") as tar:
        return {
            m.name: tar.extractfile(m).read() for m in tar.getmembers() if m.isfile()
        }


@pytest.mark.asyncio
async def test_archive_skips_files_over_the_limit(workspace, tmp_path):
    await workspace.write_bytes("small.txt", b"0123456789")
    await workspace.write_bytes("big.bin", b"\0" * (2 * 1024 * 1024))
    dest = tmp_path / "ws.tar.gz"
    info = await archive_workspace(workspace, dest, limit_bytes=1024 * 1024)
    assert list(_archived(dest)) == ["./small.txt"]
    assert info == {
        "bytes": dest.stat().st_size,
        "skipped": ["./big.bin"],
        "excludedInputs": [],
        "omitted": [],
        "omittedCount": 0,
        "omittedBytes": 0,
        "truncated": False,
    }


@pytest.mark.asyncio
async def test_archive_excludes_saved_tool_output_and_secrets(workspace, tmp_path):
    await workspace.write_bytes("keep.txt", b"k")
    await workspace.write_bytes(".srw/tool-output/o.txt", b"o")
    await workspace.write_env_file({"PW": "secret"})
    await workspace.run("true")
    dest = tmp_path / "ws.tar.gz"
    await archive_workspace(workspace, dest)
    assert list(_archived(dest)) == ["./keep.txt"]


@pytest.mark.asyncio
async def test_archive_keeps_the_smallest_files_within_the_total_budget(
    workspace, tmp_path
):
    sizes = {"a.bin": 40, "b.bin": 30, "c.bin": 30, "d.bin": 10, "sub/e.bin": 50}
    for name, size in sizes.items():
        await workspace.write_bytes(name, b"x" * size)
    await workspace.run("mkdir empty && ln -s a.bin link")
    dest = tmp_path / "ws.tar.gz"
    info = await archive_workspace(workspace, dest, total_limit_bytes=75)
    # 10 + 30 + 30 fit; the tie between b and c is broken by path.
    assert set(_archived(dest)) == {"./d.bin", "./b.bin", "./c.bin"}
    assert info["omitted"] == ["./a.bin", "./sub/e.bin"]
    assert (info["omittedCount"], info["omittedBytes"]) == (2, 90)
    assert info["truncated"] is True
    with tarfile.open(dest) as tar:
        names = set(tar.getnames())
    # Directories and symlinks are archived as before.
    assert {"./empty", "./sub", "./link"} <= names


@pytest.mark.asyncio
async def test_archive_leaves_out_staged_inputs(workspace, tmp_path):
    await workspace.write_bytes("data/in.csv", b"input")
    await workspace.write_bytes("data/out.csv", b"output")
    dest = tmp_path / "ws.tar.gz"
    info = await archive_workspace(workspace, dest, exclude_paths=["data/in.csv"])
    assert _archived(dest) == {"./data/out.csv": b"output"}
    assert info["excludedInputs"] == ["./data/in.csv"]


@pytest.mark.asyncio
async def test_archive_handles_any_file_name_exactly(workspace, tmp_path):
    names = ["new\nline.txt", "-dash.txt", "star*.txt", "back\\slash.txt", "tab\tx"]
    for index, name in enumerate(names):
        await workspace.write_bytes(name, str(index).encode())
    await workspace.write_bytes("star-x.txt", b"big" * 100)
    dest = tmp_path / "ws.tar.gz"
    # The budget keeps the five one-byte files and omits star-x.txt.
    info = await archive_workspace(workspace, dest, total_limit_bytes=5)
    assert _archived(dest) == {
        f"./{name}": str(index).encode() for index, name in enumerate(names)
    }
    assert info["omitted"] == ["./star-x.txt"]


@pytest.mark.asyncio
async def test_archive_survives_start_up_output_and_unreadable_files(
    workspace, sshd, tmp_path
):
    if os.geteuid() == 0:
        pytest.skip("root reads every file")
    await workspace.write_bytes("ok.txt", b"fine")
    await workspace.write_bytes("locked.txt", b"secret")
    await workspace.run("chmod 000 locked.txt")
    sshd.prefix = "echo junk-out; echo junk-err >&2; trap 'echo late-junk' EXIT"
    dest = tmp_path / "ws.tar.gz"
    try:
        await archive_workspace(workspace, dest)
    finally:
        sshd.prefix = None
        await workspace.run("chmod 600 locked.txt")
    # Valid gzip (tarfile reads to the end) with the readable file intact.
    assert _archived(dest) == {"./ok.txt": b"fine"}
    assert not any((Path(sshd.access.root) / ".srw-run").glob("archive.*"))


@pytest.mark.asyncio
async def test_archive_is_bounded_in_time(workspace, sshd, tmp_path):
    await workspace.write_bytes("a.txt", b"a")
    sshd.prefix = "sleep 5"
    dest = tmp_path / "ws.tar.gz"
    loop = asyncio.get_running_loop()
    started = loop.time()
    with pytest.raises(BundleError, match="timed out"):
        await archive_workspace(workspace, dest, timeout=1)
    assert loop.time() - started < 5 and not dest.exists()


@pytest.mark.asyncio
async def test_archive_file_over_the_byte_cap_fails_cleanly(workspace, tmp_path):
    await workspace.run("head -c 300000 /dev/urandom > noise.bin")
    dest = tmp_path / "ws.tar.gz"
    with pytest.raises(BundleError, match="exceeds 100000 bytes"):
        await archive_workspace(workspace, dest, max_archive_bytes=100_000)
    assert not dest.exists()


@pytest.mark.asyncio
async def test_archive_tolerates_tar_exit_code_one(workspace, tmp_path, monkeypatch):
    await workspace.write_bytes("a.txt", b"a")
    real = workspace.read_stream

    def wrapped(command, **kwargs):
        # Simulate "file changed as we read it": tar's exit 1 after a good archive.
        replaced = command.replace('-T "$list"\n', '-T "$list"; (exit 1)\n', 1)
        assert replaced != command
        return real(replaced, **kwargs)

    monkeypatch.setattr(workspace, "read_stream", wrapped)
    dest = tmp_path / "ws.tar.gz"
    info = await archive_workspace(workspace, dest)
    assert info["bytes"] > 0 and _archived(dest) == {"./a.txt": b"a"}


@pytest.mark.asyncio
async def test_archive_refuses_non_gzip_output(workspace, tmp_path, monkeypatch):
    real = workspace.read_stream
    seen = []

    def wrapped(command, **kwargs):
        replaced = command.replace(
            "tar --null", "sh -c 'echo usage: tar >&2; exit 1' --null", 1
        )
        assert replaced != command
        seen.append(replaced)
        return real(replaced, **kwargs)

    monkeypatch.setattr(workspace, "read_stream", wrapped)
    dest = tmp_path / "ws.tar.gz"
    with pytest.raises(BundleError, match="no gzip output"):
        await archive_workspace(workspace, dest)
    assert seen and not dest.exists()


def test_build_bundle_is_an_uncompressed_tar_and_drops_the_archive(tmp_path):
    (tmp_path / "t.jsonl").write_text("{}\n")
    (tmp_path / "r.json").write_text("{}")
    (tmp_path / "w.tar.gz").write_bytes(b"zz")
    out = build_bundle(
        tmp_path,
        transcript=tmp_path / "t.jsonl",
        result=tmp_path / "r.json",
        workspace_archive=tmp_path / "w.tar.gz",
    )
    assert out == tmp_path / "bundle.tar"
    with tarfile.open(out, "r:") as tar:
        assert sorted(tar.getnames()) == [
            "result.json",
            "transcript.jsonl",
            "workspace.tar.gz",
        ]
        assert tar.extractfile("workspace.tar.gz").read() == b"zz"
    assert not (tmp_path / "w.tar.gz").exists()
    out2 = build_bundle(
        tmp_path,
        transcript=tmp_path / "t.jsonl",
        result=tmp_path / "r.json",
        workspace_archive=None,
    )
    with tarfile.open(out2, "r:") as tar:
        assert sorted(tar.getnames()) == ["result.json", "transcript.jsonl"]


def test_build_bundle_stops_when_cancelled(tmp_path):
    (tmp_path / "t.jsonl").write_text("{}\n")
    (tmp_path / "r.json").write_text("{}")
    cancel = threading.Event()
    cancel.set()
    with pytest.raises(BundleError, match="cancelled"):
        build_bundle(
            tmp_path,
            transcript=tmp_path / "t.jsonl",
            result=tmp_path / "r.json",
            workspace_archive=None,
            cancel=cancel,
        )


@pytest.fixture
def no_backoff(monkeypatch):
    monkeypatch.setattr(bundle_mod, "_BACKOFF_SECONDS", (0.0, 0.0))


@pytest.mark.asyncio
async def test_upload_retries_then_succeeds(tmp_path, no_backoff):
    f = tmp_path / "bundle.tar"
    f.write_bytes(b"payload" * 1000)
    seen = []

    async def handler(request):
        body = await request.aread()
        seen.append((request.method, request.headers["x-token"], body))
        return httpx.Response(503 if len(seen) < 3 else 200)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    target = UploadTarget("http://x/up", "PUT", (("X-Token", "t"),))
    assert await upload(target, f, client=client) == 200
    assert len(seen) == 3
    assert all(m == "PUT" and h == "t" and b == f.read_bytes() for m, h, b in seen)


@pytest.mark.asyncio
async def test_upload_gives_up_after_three_attempts(tmp_path, no_backoff):
    f = tmp_path / "b"
    f.write_bytes(b"x")
    calls = 0

    def handler(request):
        nonlocal calls
        calls += 1
        raise httpx.ConnectError("down")

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    with pytest.raises(BundleError):
        await upload(UploadTarget("http://x/up"), f, client=client)
    assert calls == 3


@pytest.mark.asyncio
async def test_upload_does_not_retry_4xx(tmp_path, no_backoff):
    f = tmp_path / "b"
    f.write_bytes(b"x")
    calls = 0

    def handler(request):
        nonlocal calls
        calls += 1
        return httpx.Response(403)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    with pytest.raises(BundleError, match="403"):
        await upload(UploadTarget("http://x/up"), f, client=client)
    assert calls == 1


@dataclasses.dataclass
class FakeResult:
    subtype: str = "success"
    is_error: bool = False
    result: str | None = "done"
    num_turns: int = 3
    total_cost_usd: float | None = 0.12
    usage: dict | None = None
    model_usage: dict | None = None
    session_id: str = "s1"
    stop_reason: str | None = "end_turn"
    duration_ms: int = 1000
    duration_api_ms: int = 800
    api_error_status: int | None = None


def _config():
    return HarnessConfig(
        model="m",
        system_prompt=SystemPrompt("preset"),
        tools=("Bash",),
        limits=Limits(),
        effort=None,
        thinking=None,
        auto_compact=True,
        workspace_env=(),
    )


def _write(tmp_path, outcome):
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    return write_result(
        tmp_path / "result.json",
        config_data={"model": "m"},
        config=_config(),
        task=Task("do it", (), None),
        outcome=outcome,
        started=start,
        finished=start + timedelta(seconds=90),
        exit_code=0,
        exit_reason="completed",
        inputs=[],
        archive=None,
        versions={"harness": "1", "sdk": "2", "cli": "3"},
    )


def test_write_result_shape(tmp_path):
    outcome = SessionOutcome(FakeResult(usage={"a": 1}), False, False, None)
    data = _write(tmp_path, outcome)
    assert json.loads((tmp_path / "result.json").read_text()) == data
    assert data["version"] == 1 and data["durationSeconds"] == 90
    assert data["prompt"] == "do it" and data["configHash"]
    assert data["result"]["finalMessage"] == "done"
    assert data["result"]["sessionId"] == "s1" and data["result"]["usage"] == {"a": 1}
    assert data["timedOut"] is False and data["interrupted"] is False
    assert data["startedAt"].startswith("2026-01-01T00:00:00")


def test_write_result_without_outcome(tmp_path):
    data = _write(tmp_path, None)
    assert data["result"] is None and data["error"] is None
    assert data["timedOut"] is False and data["interrupted"] is False
    assert not list(tmp_path.glob("*.tmp"))
