import asyncio
import dataclasses
import io
import json
import os
import signal
import tarfile
import time
from pathlib import Path

import httpx
import pytest
import pytest_asyncio
import srw_claude_code_harness
from srw_claude_code_harness import __main__ as entry
from srw_claude_code_harness.bundle import BundleError
from srw_claude_code_harness.config import Limits
from srw_claude_code_harness.workspace import Workspace, WorkspaceError
from tests._claude_code_harness_sshd import LocalSshd

TOKEN = "oauth-token-value-0123456789"
API_KEY = "api-key-value-0123456789"
DB_PASSWORD = "db-password-value-0123456789"
INPUT_URL = "https://files.example/datasets/customers.csv?token=input-secret"
UPLOAD_URL = "https://archive.example/runs/42/bundle.tar.gz?X-Amz-Signature=sig-secret"
CSV = b"id,name\n1,Ada\n2,Grace\n"
SECRETS = (TOKEN, API_KEY, DB_PASSWORD, INPUT_URL, UPLOAD_URL, "sig-secret")


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
    """Stands in for ``ClaudeSDKClient`` (the Task 10 pattern).

    ``hang`` waits after the scripted messages until ``interrupt()`` answers with
    a success ResultMessage; ``on_query`` runs when the prompt is sent.
    """

    def __init__(self, options, messages, *, hang=False, on_query=None):
        self.options = options
        self.messages = list(messages)
        self.hang = hang
        self.on_query = on_query
        self.prompts = []
        self.interrupted = False
        self.disconnected = False
        self._answer = asyncio.Event()

    async def connect(self):
        pass

    async def query(self, prompt):
        self.prompts.append(prompt)
        if self.on_query is not None:
            self.on_query()

    async def receive_messages(self):
        for message in self.messages:
            yield message
        if self.hang:
            await self._answer.wait()
            yield _result(result="interrupted")

    async def interrupt(self):
        self.interrupted = True
        self._answer.set()

    async def disconnect(self):
        self.disconnected = True


def _factory(clients, messages=(), **kwargs):
    def make(options):
        client = FakeClient(options, messages, **kwargs)
        clients.append(client)
        return client

    return make


class Http:
    """MockTransport handler: serves the input, records every upload."""

    def __init__(self, *, upload_status=200, upload_handler=None):
        self.gets = []
        self.uploads = []
        self.upload_status = upload_status
        self.upload_handler = upload_handler

    async def __call__(self, request):
        if request.method == "GET":
            self.gets.append(str(request.url))
            return httpx.Response(200, content=CSV)
        self.uploads.append(
            {
                "method": request.method,
                "url": str(request.url),
                "body": request.content,
            }
        )
        if self.upload_handler is not None:
            return await self.upload_handler(request, len(self.uploads))
        return httpx.Response(self.upload_status)

    def client(self):
        return httpx.AsyncClient(transport=httpx.MockTransport(self))


def _members(body: bytes) -> dict[str, bytes]:
    with tarfile.open(fileobj=io.BytesIO(body), mode="r:gz") as tar:
        return {m.name: tar.extractfile(m).read() for m in tar.getmembers()}


def _bindings(access) -> dict:
    return {
        "workspace": {
            "transport": "ssh",
            "host": access.host,
            "port": access.port,
            "user": access.user,
            "root": access.root,
            "privateKeyFile": access.private_key_file,
            "knownHostsFile": access.known_hosts_file,
        }
    }


_DUMMY_BINDINGS = {
    "workspace": {
        "transport": "ssh",
        "host": "127.0.0.1",
        "port": 1,
        "user": "nobody",
        "root": "/nonexistent/workspace",
        "privateKeyFile": "/nonexistent/id_ed25519",
        "knownHostsFile": "/nonexistent/known_hosts",
    }
}


def _files(tmp_path, *, bindings, config=None, task=None, upload=True) -> dict:
    config = config or {
        "version": 1,
        "model": "claude-test",
        "workspaceEnv": ["DB_PASSWORD"],
    }
    data: dict = {"inputs": [{"url": INPUT_URL, "path": "data/customers.csv"}]}
    if upload:
        data["upload"] = {"url": UPLOAD_URL, "method": "PUT"}
    task = task or {"text": "Load data/customers.csv into the database.", "data": data}
    paths = {}
    for name, content in (
        ("config_file", config),
        ("task_file", task),
        ("bindings_file", bindings),
    ):
        path = tmp_path / "delivered" / f"{name}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(content))
        paths[name] = path
    return paths


def _environ(**overrides) -> dict:
    environ = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "CLAUDE_CODE_OAUTH_TOKEN": TOKEN,
        "ANTHROPIC_API_KEY": API_KEY,
        "DB_PASSWORD": DB_PASSWORD,
        "UNRELATED_SECRET": "unrelated-secret-value",
    }
    for name, value in overrides.items():
        if value is None:
            environ.pop(name, None)
        else:
            environ[name] = value
    return environ


def _never_connect(calls):
    async def connect(access):
        calls.append(access)
        raise AssertionError("connect must not be called")

    return connect


def _recording_connect(calls):
    async def connect(access):
        calls.append(access)
        return await Workspace.connect(access)

    return connect


def _result_json(run_dir: Path) -> dict:
    return json.loads((run_dir / "result.json").read_text())


def _assert_no_secrets(text: str) -> None:
    for secret in SECRETS:
        assert secret not in text


@pytest_asyncio.fixture
async def sshd(tmp_path):
    server = await LocalSshd.start(tmp_path / "workspace")
    yield server
    await server.close()


async def _run(tmp_path, paths, *, environ=None, stop=None, **kwargs):
    run_dir = tmp_path / "run"
    code = await entry.run(
        **paths,
        environ=environ if environ is not None else _environ(),
        run_dir=run_dir,
        stop=stop or asyncio.Event(),
        **kwargs,
    )
    return code, run_dir


@pytest.mark.asyncio
async def test_happy_path_uploads_a_bundle_and_exits_0(tmp_path, sshd, capsys):
    sdk = _sdk()
    clients, http = [], Http()
    environ_before = dict(os.environ)
    paths = _files(tmp_path, bindings=_bindings(sshd.access))
    async with http.client() as client:
        code, run_dir = await _run(
            tmp_path,
            paths,
            client_factory=_factory(clients, [_assistant("working"), _result()]),
            http_client=client,
        )
    assert code == entry.EXIT_OK == 0
    assert dict(os.environ) == environ_before  # run() never touches os.environ

    root = Path(sshd.access.root)
    assert (root / "data/customers.csv").read_bytes() == CSV
    env_file = (root / ".srw-env").read_text()
    assert "export DB_PASSWORD=" in env_file and DB_PASSWORD in env_file
    assert "UNRELATED_SECRET" not in env_file and TOKEN not in env_file

    [client] = clients
    assert client.prompts == ["Load data/customers.csv into the database."]
    env = client.options.env
    assert env["CLAUDE_CODE_OAUTH_TOKEN"] == TOKEN  # preferred over the API key
    assert "ANTHROPIC_API_KEY" not in env and "DB_PASSWORD" not in env
    assert "UNRELATED_SECRET" not in env
    assert Path(client.options.cwd) == root

    result = _result_json(run_dir)
    assert result["exitCode"] == 0 and result["exitReason"] == "success"
    assert result["result"]["subtype"] == "success"
    assert result["inputs"][0]["path"] == "data/customers.csv"
    assert result["inputs"][0]["bytes"] == len(CSV)
    assert result["archive"]["bytes"] > 0
    assert result["versions"]["harness"] == srw_claude_code_harness.__version__
    assert result["versions"]["sdk"] == sdk.__version__
    assert result["config"]["workspaceEnv"] == ["DB_PASSWORD"]

    [put] = http.uploads
    assert put["method"] == "PUT" and put["url"] == UPLOAD_URL
    members = _members(put["body"])
    assert set(members) == {"transcript.jsonl", "result.json", "workspace.tar.gz"}
    assert json.loads(members["result.json"])["exitCode"] == 0
    with tarfile.open(fileobj=io.BytesIO(members["workspace.tar.gz"])) as tar:
        names = tar.getnames()
    assert "./data/customers.csv" in names and "./.srw-env" not in names
    lines = members["transcript.jsonl"].splitlines()
    kinds = [json.loads(line)["kind"] for line in lines]
    assert "AssistantMessage" in kinds and "ResultMessage" in kinds

    out = capsys.readouterr()
    assert "[srw-harness]" in out.out
    _assert_no_secrets(out.out + out.err)
    _assert_no_secrets((run_dir / "result.json").read_text())


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "overrides",
    [
        {"CLAUDE_CODE_OAUTH_TOKEN": None, "ANTHROPIC_API_KEY": None},
        {"CLAUDE_CODE_OAUTH_TOKEN": "", "ANTHROPIC_API_KEY": ""},
    ],
)
async def test_missing_credential_exits_2_before_connecting(tmp_path, overrides):
    calls, http = [], Http()
    paths = _files(tmp_path, bindings=_DUMMY_BINDINGS)
    async with http.client() as client:
        code, run_dir = await _run(
            tmp_path,
            paths,
            environ=_environ(**overrides),
            connect=_never_connect(calls),
            http_client=client,
        )
    assert code == entry.EXIT_CONFIG_ERROR == 2
    assert calls == [] and http.gets == []
    result = _result_json(run_dir)
    assert result["exitCode"] == 2
    assert "CLAUDE_CODE_OAUTH_TOKEN" in result["exitReason"]
    assert "ANTHROPIC_API_KEY" in result["exitReason"]
    # The failure is still reported to the upload target (transcript + result).
    [put] = http.uploads
    members = _members(put["body"])
    assert set(members) == {"transcript.jsonl", "result.json"}
    assert json.loads(members["result.json"])["exitCode"] == 2


@pytest.mark.asyncio
async def test_api_key_is_used_when_there_is_no_oauth_token(tmp_path, sshd):
    clients = []
    paths = _files(tmp_path, bindings=_bindings(sshd.access), upload=False)
    async with Http().client() as client:
        code, _ = await _run(
            tmp_path,
            paths,
            environ=_environ(CLAUDE_CODE_OAUTH_TOKEN=""),
            client_factory=_factory(clients, [_result()]),
            http_client=client,
        )
    assert code == 0
    env = clients[0].options.env
    assert env["ANTHROPIC_API_KEY"] == API_KEY and "CLAUDE_CODE_OAUTH_TOKEN" not in env


@pytest.mark.asyncio
async def test_missing_workspace_env_name_exits_2(tmp_path, capsys):
    calls = []
    config = {"version": 1, "model": "m", "workspaceEnv": ["DB_PASSWORD", "DB_HOST"]}
    paths = _files(tmp_path, bindings=_DUMMY_BINDINGS, config=config, upload=False)
    code, run_dir = await _run(tmp_path, paths, connect=_never_connect(calls))
    assert code == entry.EXIT_CONFIG_ERROR
    assert calls == []
    result = _result_json(run_dir)
    assert result["exitCode"] == 2 and "DB_HOST" in result["exitReason"]
    out = capsys.readouterr()
    assert "DB_HOST" in out.out + out.err
    _assert_no_secrets(out.out + out.err + (run_dir / "result.json").read_text())


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("which", "content"),
    [
        ("config_file", {"version": 1, "model": "m", "hooks": {}}),
        ("config_file", "not json"),
        ("task_file", {"data": {}}),
        ("bindings_file", {"workspace": None}),
    ],
)
async def test_invalid_delivered_files_exit_2_and_still_write_result(
    tmp_path, which, content
):
    calls = []
    paths = _files(tmp_path, bindings=_DUMMY_BINDINGS, upload=False)
    paths[which].write_text(
        content if isinstance(content, str) else json.dumps(content)
    )
    code, run_dir = await _run(tmp_path, paths, connect=_never_connect(calls))
    assert code == entry.EXIT_CONFIG_ERROR and calls == []
    result = _result_json(run_dir)
    assert result["exitCode"] == 2 and result["exitReason"]


@pytest.mark.asyncio
async def test_input_staging_failure_exits_2(tmp_path, sshd):
    paths = _files(tmp_path, bindings=_bindings(sshd.access), upload=False)
    clients = []
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(404))
    )
    async with client:
        code, run_dir = await _run(
            tmp_path, paths, client_factory=_factory(clients), http_client=client
        )
    assert code == entry.EXIT_CONFIG_ERROR and clients == []
    result = _result_json(run_dir)
    assert result["exitCode"] == 2 and "HTTP 404" in result["exitReason"]
    _assert_no_secrets((run_dir / "result.json").read_text())


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error", [WorkspaceError("cannot connect to the workspace"), RuntimeError("boom")]
)
async def test_connect_failure_exits_1_and_writes_result(tmp_path, error):
    async def connect(access):
        raise error

    paths = _files(tmp_path, bindings=_DUMMY_BINDINGS, upload=False)
    code, run_dir = await _run(tmp_path, paths, connect=connect)
    assert code == entry.EXIT_HARNESS_ERROR == 1
    result = _result_json(run_dir)
    assert result["exitCode"] == 1 and str(error) in result["exitReason"]


@pytest.mark.asyncio
async def test_result_error_exits_3_but_still_uploads(tmp_path, sshd):
    clients, http = [], Http()
    paths = _files(tmp_path, bindings=_bindings(sshd.access))
    failure = _result(subtype="error_max_turns", is_error=True, result=None)
    async with http.client() as client:
        code, run_dir = await _run(
            tmp_path,
            paths,
            client_factory=_factory(clients, [failure]),
            http_client=client,
        )
    assert code == entry.EXIT_RUN_ERROR == 3
    result = _result_json(run_dir)
    assert result["exitCode"] == 3 and "error_max_turns" in result["exitReason"]
    [put] = http.uploads
    assert "workspace.tar.gz" in _members(put["body"])


@pytest.mark.asyncio
async def test_upload_failure_does_not_mask_a_run_error(tmp_path, sshd):
    paths = _files(tmp_path, bindings=_bindings(sshd.access))
    failure = _result(subtype="error_during_execution", is_error=True)
    async with Http(upload_status=403).client() as client:
        code, run_dir = await _run(
            tmp_path, paths, client_factory=_factory([], [failure]), http_client=client
        )
    assert code == entry.EXIT_RUN_ERROR
    assert _result_json(run_dir)["exitCode"] == 3


def _short_timeout(monkeypatch, seconds):
    parse = entry.parse_config

    def parse_config(data):
        config = parse(data)
        limits = Limits(config.limits.max_turns, config.limits.max_budget_usd, seconds)
        return dataclasses.replace(config, limits=limits)

    monkeypatch.setattr(entry, "parse_config", parse_config)


@pytest.mark.asyncio
async def test_wall_clock_timeout_exits_4(tmp_path, sshd, monkeypatch):
    _short_timeout(monkeypatch, 0.3)
    clients, http = [], Http()
    paths = _files(tmp_path, bindings=_bindings(sshd.access))
    started = time.monotonic()
    async with http.client() as client:
        code, run_dir = await _run(
            tmp_path,
            paths,
            client_factory=_factory(clients, [_assistant("busy")], hang=True),
            http_client=client,
        )
    assert time.monotonic() - started < 15
    # The interrupt was answered with a success result: still interrupted.
    assert code == entry.EXIT_INTERRUPTED == 4 and clients[0].interrupted
    result = _result_json(run_dir)
    assert result["exitCode"] == 4 and result["timedOut"] is True
    assert result["result"]["subtype"] == "success"
    # The deadline path archives the workspace as usual.
    [put] = http.uploads
    assert "workspace.tar.gz" in _members(put["body"])


@pytest.mark.asyncio
async def test_sigterm_during_session_writes_partial_result_and_exits_4(tmp_path, sshd):
    clients, http = [], Http()
    stop = asyncio.Event()
    paths = _files(tmp_path, bindings=_bindings(sshd.access))
    started = time.monotonic()
    async with http.client() as client:
        code, run_dir = await _run(
            tmp_path,
            paths,
            stop=stop,
            client_factory=_factory(
                clients, [_assistant("working")], hang=True, on_query=stop.set
            ),
            http_client=client,
        )
    assert time.monotonic() - started < 15
    assert code == entry.EXIT_INTERRUPTED and clients[0].interrupted
    result = _result_json(run_dir)
    assert result["exitCode"] == 4 and result["interrupted"] is True
    assert result["archive"] is None
    # On SIGTERM the bundle carries the transcript and the result only.
    [put] = http.uploads
    members = _members(put["body"])
    assert set(members) == {"transcript.jsonl", "result.json"}
    assert json.loads(members["result.json"])["exitCode"] == 4
    assert b"AssistantMessage" in members["transcript.jsonl"]


@pytest.mark.asyncio
async def test_sigterm_before_session_exits_4(tmp_path, sshd):
    calls, clients, http = [], [], Http()
    stop = asyncio.Event()
    stop.set()
    paths = _files(tmp_path, bindings=_bindings(sshd.access))
    async with http.client() as client:
        code, run_dir = await _run(
            tmp_path,
            paths,
            stop=stop,
            connect=_recording_connect(calls),
            client_factory=_factory(clients, [_result()]),
            http_client=client,
        )
    assert code == entry.EXIT_INTERRUPTED
    assert calls == [] and clients == [] and http.gets == []
    result = _result_json(run_dir)
    assert result["exitCode"] == 4 and result["interrupted"] is True
    [put] = http.uploads
    assert set(_members(put["body"])) == {"transcript.jsonl", "result.json"}


@pytest.mark.asyncio
async def test_sigterm_during_upload_reuploads_a_small_bundle_and_exits_4(
    tmp_path, sshd
):
    stop = asyncio.Event()

    async def upload_handler(request, attempt):
        if attempt == 1:
            stop.set()
            await asyncio.sleep(60)
        return httpx.Response(200)

    http = Http(upload_handler=upload_handler)
    paths = _files(tmp_path, bindings=_bindings(sshd.access))
    started = time.monotonic()
    async with http.client() as client:
        code, run_dir = await _run(
            tmp_path,
            paths,
            stop=stop,
            client_factory=_factory([], [_result()]),
            http_client=client,
        )
    assert time.monotonic() - started < 15
    assert code == entry.EXIT_INTERRUPTED
    first, second = http.uploads
    assert "workspace.tar.gz" in _members(first["body"])
    members = _members(second["body"])
    assert set(members) == {"transcript.jsonl", "result.json"}
    assert json.loads(members["result.json"])["exitCode"] == 4
    assert _result_json(run_dir)["exitCode"] == 4


@pytest.mark.asyncio
async def test_stop_path_upload_is_bounded_and_keeps_exit_4(
    tmp_path, sshd, monkeypatch
):
    monkeypatch.setattr(entry, "STOP_UPLOAD_SECONDS", 0.5)
    stop = asyncio.Event()
    stop.set()

    async def never_answers(request, attempt):
        await asyncio.sleep(60)

    paths = _files(tmp_path, bindings=_bindings(sshd.access))
    started = time.monotonic()
    async with Http(upload_handler=never_answers).client() as client:
        code, run_dir = await _run(tmp_path, paths, stop=stop, http_client=client)
    assert time.monotonic() - started < 5
    assert code == entry.EXIT_INTERRUPTED
    assert _result_json(run_dir)["exitCode"] == 4


@pytest.mark.asyncio
async def test_upload_failure_exits_5(tmp_path, sshd):
    http = Http(upload_status=403)
    paths = _files(tmp_path, bindings=_bindings(sshd.access))
    async with http.client() as client:
        code, run_dir = await _run(
            tmp_path,
            paths,
            client_factory=_factory([], [_result()]),
            http_client=client,
        )
    assert code == entry.EXIT_UPLOAD_ERROR == 5
    assert len(http.uploads) == 1  # 4xx is not retried
    result = _result_json(run_dir)
    assert result["exitCode"] == 5 and "HTTP 403" in result["exitReason"]
    _assert_no_secrets((run_dir / "result.json").read_text())


@pytest.mark.asyncio
async def test_archive_failure_exits_1_and_uploads_without_the_archive(
    tmp_path, sshd, monkeypatch
):
    async def broken_archive(workspace, dest, **kwargs):
        raise BundleError("workspace archive failed: tar exploded")

    monkeypatch.setattr(entry, "archive_workspace", broken_archive)
    http = Http()
    paths = _files(tmp_path, bindings=_bindings(sshd.access))
    async with http.client() as client:
        code, run_dir = await _run(
            tmp_path,
            paths,
            client_factory=_factory([], [_result()]),
            http_client=client,
        )
    assert code == entry.EXIT_HARNESS_ERROR
    result = _result_json(run_dir)
    assert result["exitCode"] == 1 and "tar exploded" in result["archive"]["error"]
    [put] = http.uploads
    assert set(_members(put["body"])) == {"transcript.jsonl", "result.json"}


@pytest.mark.asyncio
async def test_no_upload_target_skips_upload_and_exits_0(tmp_path, sshd):
    http = Http()
    paths = _files(tmp_path, bindings=_bindings(sshd.access), upload=False)
    async with http.client() as client:
        code, run_dir = await _run(
            tmp_path,
            paths,
            client_factory=_factory([], [_result()]),
            http_client=client,
        )
    assert code == entry.EXIT_OK
    assert http.uploads == [] and http.gets == [INPUT_URL]
    assert _result_json(run_dir)["exitCode"] == 0


@pytest.mark.asyncio
async def test_download_client_follows_redirects_and_upload_client_does_not():
    def handler(request):
        if request.url.path == "/old":
            return httpx.Response(302, headers={"Location": "https://h.example/new"})
        return httpx.Response(200, content=b"ok")

    download, upload_client = entry._http_clients(httpx.MockTransport(handler))
    async with download, upload_client:
        response = await download.get("https://h.example/old")
        assert response.status_code == 200 and response.content == b"ok"
        response = await upload_client.put("https://h.example/old", content=b"x")
        assert response.status_code == 302


_DELIVERY = ("SRW_CONFIG_FILE", "SRW_TASK_FILE", "SRW_BINDINGS_FILE")


def _no_scrub(monkeypatch, calls):
    def scrub(keep):
        calls.append(("scrub", list(keep)))
        return {}

    monkeypatch.setattr(entry, "scrub_process_environment", scrub)


def test_main_requires_the_three_delivery_files(monkeypatch, capsys):
    calls = []
    _no_scrub(monkeypatch, calls)
    for name in _DELIVERY:
        monkeypatch.delenv(name, raising=False)
    assert entry.main([]) == 2
    out = capsys.readouterr()
    assert "SRW_CONFIG_FILE" in out.out + out.err
    assert calls == []


def test_main_without_bindings_says_a_workspace_is_required(
    monkeypatch, capsys, tmp_path
):
    calls = []
    _no_scrub(monkeypatch, calls)
    paths = _files(tmp_path, bindings=_DUMMY_BINDINGS)
    monkeypatch.setenv("SRW_CONFIG_FILE", str(paths["config_file"]))
    monkeypatch.setenv("SRW_TASK_FILE", str(paths["task_file"]))
    monkeypatch.delenv("SRW_BINDINGS_FILE", raising=False)
    assert entry.main([]) == 2
    out = capsys.readouterr()
    assert "a workspace is required" in out.out + out.err
    assert calls == []


def test_main_names_a_delivery_file_that_does_not_exist(monkeypatch, capsys):
    calls = []
    _no_scrub(monkeypatch, calls)
    for name in _DELIVERY:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("SRW_CONFIG_FILE", "/nonexistent-srw-config.json")
    assert entry.main([]) == 2
    out = capsys.readouterr()
    text = out.out + out.err
    assert "SRW_CONFIG_FILE" in text and "/nonexistent-srw-config.json" not in text
    assert calls == []


def test_main_captures_the_environment_scrubs_it_and_sets_stop_on_sigterm(
    monkeypatch, tmp_path
):
    calls = []
    _no_scrub(monkeypatch, calls)
    paths = _files(tmp_path, bindings=_DUMMY_BINDINGS)
    for name, key in zip(_DELIVERY, ("config_file", "task_file", "bindings_file")):
        monkeypatch.setenv(name, str(paths[key]))
    monkeypatch.setenv("SRW_TEST_MARKER", "marker-value")
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    monkeypatch.setattr(entry.tempfile, "mkdtemp", lambda **kwargs: str(run_dir))

    async def fake_run(**kwargs):
        calls.append(("run", kwargs))
        os.kill(os.getpid(), signal.SIGTERM)
        try:
            await asyncio.wait_for(kwargs["stop"].wait(), 5)
        except asyncio.TimeoutError:
            return 99
        return entry.EXIT_INTERRUPTED

    monkeypatch.setattr(entry, "run", fake_run)
    # Guard: without the harness's handler, SIGTERM must not kill pytest.
    previous = signal.signal(signal.SIGTERM, lambda *args: None)
    try:
        assert entry.main([]) == entry.EXIT_INTERRUPTED
    finally:
        signal.signal(signal.SIGTERM, previous)
    (scrub, keep), (name, kwargs) = calls
    assert (scrub, keep, name) == ("scrub", ["PATH"], "run")
    assert kwargs["environ"]["SRW_TEST_MARKER"] == "marker-value"
    assert kwargs["config_file"] == paths["config_file"]
    assert kwargs["task_file"] == paths["task_file"]
    assert kwargs["bindings_file"] == paths["bindings_file"]
    assert kwargs["run_dir"] == run_dir
