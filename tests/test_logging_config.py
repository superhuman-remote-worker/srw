"""Shared logging behaviour and application adapter policy remain compatible."""

import asyncio
import io
import json
import logging
import subprocess
import sys
import time
import warnings
from pathlib import Path

import pytest
import httpx

from orchestrator import logging_config as orch_log
from agent.core import logging_config as agent_log


@pytest.fixture(params=[orch_log, agent_log], ids=["orchestrator", "agent"])
def mod(request):
    """The logging module under test (both flavours)."""
    return request.param


def _record(msg, *args, exc_info=None):
    return logging.LogRecord(
        name="test.logger",
        level=logging.INFO,
        pathname="/src/app/file.py",
        lineno=42,
        msg=msg,
        args=args,
        exc_info=exc_info,
    )


def _emit_json(mod, msg, *args, exc_info=None):
    return json.loads(
        mod.JsonLogFormatter(component="test").format(
            _record(msg, *args, exc_info=exc_info)
        )
    )


class TestJsonFormatter:
    def test_output_is_parseable_with_core_fields(self, mod):
        rec = _emit_json(mod, "hello %s", "world")
        assert rec["message"] == "hello world"
        assert rec["level"] == "INFO"
        assert rec["logger"] == "test.logger"
        assert rec["component"] == "test"
        assert rec["file"] == "file.py:42"
        assert rec["ts"].endswith("Z")

    def test_correlation_context_injected(self, mod):
        with mod.log_context(job_id="job-123", agent_id="agent-7"):
            rec = _emit_json(mod, "working")
        assert rec["job_id"] == "job-123"
        assert rec["agent_id"] == "agent-7"

    def test_context_cleared_after_scope(self, mod):
        with mod.log_context(job_id="job-123"):
            pass
        rec = _emit_json(mod, "after")
        assert "job_id" not in rec

    def test_bind_reset_roundtrip(self, mod):
        token = mod.bind_log_context(thread_id="t-1")
        assert _emit_json(mod, "x")["thread_id"] == "t-1"
        mod.reset_log_context(token)
        assert "thread_id" not in _emit_json(mod, "y")

    def test_none_and_empty_values_skipped(self, mod):
        # k8s sets unused env vars to "" (e.g. SESSION_BOUND_THREAD_ID on
        # worker pods); "" must not leak as a noise field.
        with mod.log_context(job_id=None, thread_id=""):
            rec = _emit_json(mod, "no ids")
        assert "job_id" not in rec
        assert "thread_id" not in rec

    def test_exc_info_included(self, mod):
        try:
            raise ValueError("boom")
        except ValueError:
            import sys

            rec = _emit_json(mod, "failed", exc_info=sys.exc_info())
        assert "ValueError: boom" in rec["exc"]

    def test_stable_fields_and_context_extra_precedence(self, mod):
        record = _record("hello %s", "世界")
        record.created = 0
        record.job_id = "extra-job"
        record.detail = "extra-detail"
        record.artifact = Path("result.txt")
        record.color_message = "ANSI duplicate"
        with mod.log_context(
            job_id="context-job", detail="context-detail", logger="ignored-logger"
        ):
            line = mod.JsonLogFormatter().format(record)
        assert line == (
            '{"ts": "1970-01-01T00:00:00Z", "level": "INFO", '
            '"logger": "test.logger", '
            f'"component": "{mod.COMPONENT}", '
            '"message": "hello 世界", "file": "file.py:42", '
            '"job_id": "context-job", "detail": "context-detail", '
            '"artifact": "result.txt"}'
        )

    @pytest.mark.asyncio
    async def test_task_context_inherits_without_leaking_to_siblings(self, mod):
        ready = [asyncio.Event(), asyncio.Event()]
        formatter = mod.JsonLogFormatter()

        async def emit_in_task(index):
            with mod.log_context(job_id=f"job-{index}"):
                ready[index].set()
                await ready[1 - index].wait()
                current = json.loads(formatter.format(_record("working")))
            restored = json.loads(formatter.format(_record("after")))
            return current, restored

        with mod.log_context(agent_id="parent-agent", job_id="parent-job"):
            results = await asyncio.gather(emit_in_task(0), emit_in_task(1))
            assert _emit_json(mod, "parent")["job_id"] == "parent-job"
        for index, (current, restored) in enumerate(results):
            assert current["job_id"] == f"job-{index}"
            assert current["agent_id"] == "parent-agent"
            assert restored["job_id"] == "parent-job"
        assert "job_id" not in _emit_json(mod, "outside")


def test_adapters_keep_independent_context_when_loaded_together():
    record = _record("same process")
    agent_formatter = agent_log.JsonLogFormatter()
    orch_formatter = orch_log.JsonLogFormatter()
    with agent_log.log_context(job_id="agent-job", agent_id="agent-1"):
        with orch_log.log_context(job_id="orchestrator-job", request_id="request-1"):
            agent_entry = json.loads(agent_formatter.format(record))
            orch_entry = json.loads(orch_formatter.format(record))
        assert "job_id" not in json.loads(orch_formatter.format(record))
        assert json.loads(agent_formatter.format(record))["job_id"] == "agent-job"
    assert agent_entry["job_id"] == "agent-job"
    assert "request_id" not in agent_entry
    assert orch_entry["job_id"] == "orchestrator-job"
    assert "agent_id" not in orch_entry
    assert "job_id" not in json.loads(agent_formatter.format(record))


class TestAdapterFormatting:
    def test_default_text_format_preserved(self, mod, monkeypatch):
        monkeypatch.delenv("LOG_FORMAT", raising=False)
        record = _record("hello password=example-value")
        record.created = 0
        formatter = mod.build_formatter()
        formatter.converter = time.gmtime
        expected = {
            "agent": (
                "1970-01-01 00:00:00 - test.logger - INFO - "
                "hello password=***REDACTED***"
            ),
            "orchestrator": (
                "1970-01-01 00:00:00 INFO test.logger: hello password=***REDACTED***"
            ),
        }
        assert formatter.format(record) == expected[mod.COMPONENT]

    def test_json_selection_and_component_override(self, mod, monkeypatch):
        monkeypatch.setenv("LOG_FORMAT", "JSON")
        formatter = mod.build_formatter(component="custom-component")
        assert isinstance(formatter, mod.JsonLogFormatter)
        assert json.loads(formatter.format(_record("hello")))["component"] == (
            "custom-component"
        )

    @pytest.mark.parametrize("log_format", ["text", "json"])
    def test_exception_and_stack_are_redacted(self, mod, monkeypatch, log_format):
        monkeypatch.setenv("LOG_FORMAT", log_format)
        try:
            raise ValueError("password=exception-value")
        except ValueError:
            record = _record("failed", exc_info=sys.exc_info())
        record.stack_info = "Stack: api_key=stack-value"
        line = mod.build_formatter().format(record)
        assert "exception-value" not in line
        assert "stack-value" not in line
        assert "ValueError: password=***REDACTED***" in line
        assert "Stack: api_key=***REDACTED***" in line


@pytest.fixture
def isolated_logging(monkeypatch):
    """Restore process logging/warning policy after exercising application setup."""
    root = logging.getLogger()
    monkeypatch.setattr(root, "handlers", root.handlers[:])
    monkeypatch.setattr(root, "level", root.level)
    monkeypatch.setattr(warnings, "showwarning", warnings.showwarning)
    # An earlier app import can enable capture before pytest replaces showwarning.
    # Reset the latch so configure_logging captures this test's current hook;
    # monkeypatch restores both the inherited latch and hook after the test.
    monkeypatch.setattr(logging, "_warnings_showwarning", None)
    for name in ("logging-test.application", "logging-test.library", "uvicorn.access"):
        logger = logging.getLogger(name)
        monkeypatch.setattr(logger, "level", logging.NOTSET)
        monkeypatch.setattr(logger, "disabled", False)


def test_setup_keeps_debug_policy_and_captures_warnings(
    mod, monkeypatch, isolated_logging, capsys
):
    monkeypatch.setenv("LOG_FORMAT", "json")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    monkeypatch.delenv("DEBUG_ALL", raising=False)
    mod.configure_logging(app_namespaces=("logging-test.application",))
    logging.getLogger("logging-test.application").debug("application-debug")
    logging.getLogger("logging-test.library").debug("library-debug")
    logging.getLogger("logging-test.library").info("library-info")
    with warnings.catch_warnings():
        warnings.simplefilter("always")
        warnings.warn("password=warning-value", UserWarning, stacklevel=1)
    records = [json.loads(line) for line in capsys.readouterr().err.splitlines()]
    assert [record["logger"] for record in records] == [
        "logging-test.application",
        "logging-test.library",
        "py.warnings",
    ]
    assert records[0]["message"] == "application-debug"
    assert records[1]["message"] == "library-info"
    assert "warning-value" not in records[2]["message"]
    assert "password=***REDACTED***" in records[2]["message"]
    assert all(record["component"] == mod.COMPONENT for record in records)
    assert not logging.getLogger("uvicorn.access").disabled


def test_orchestrator_access_logging_policy_stays_optional(isolated_logging):
    orch_log.configure_logging(disable_uvicorn_access=True)
    assert logging.getLogger("uvicorn.access").disabled


@pytest.mark.parametrize(
    ("module_name", "forbidden_apps"),
    [
        ("shared.logging_format", ["agent", "orchestrator"]),
        ("agent.core.logging_config", ["orchestrator"]),
        ("orchestrator.logging_config", ["agent"]),
    ],
)
def test_fresh_import_is_inert(module_name, forbidden_apps):
    script = """
import importlib
import logging
import sys
import warnings

root = logging.getLogger()
handler = logging.NullHandler()
root.addHandler(handler)
root.setLevel(logging.ERROR)
original_showwarning = warnings.showwarning
original_loggers = dict(root.manager.loggerDict)
original_modules = set(sys.modules)
importlib.import_module(sys.argv[1])
assert root.handlers == [handler]
assert root.level == logging.ERROR
assert warnings.showwarning is original_showwarning
assert root.manager.loggerDict == original_loggers
allowed_roots = sys.stdlib_module_names | {"shared", sys.argv[1].split(".")[0]}
new_roots = {name.split(".")[0] for name in set(sys.modules) - original_modules}
assert new_roots <= allowed_roots, new_roots - allowed_roots
for prefix in ["shared.runtime", "langchain", "langchain_core", "langgraph", *sys.argv[2:]]:
    assert not any(name == prefix or name.startswith(prefix + ".") for name in sys.modules), prefix
"""
    result = subprocess.run(
        [sys.executable, "-I", "-c", script, module_name, *forbidden_apps],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout == ""
    assert result.stderr == ""


class TestRedaction:
    @pytest.mark.parametrize("log_format", ["text", "json"])
    @pytest.mark.asyncio
    async def test_httpx_lifecycle_signature_is_masked_without_losing_diagnostics(
        self, monkeypatch, log_format
    ):
        monkeypatch.setenv("LOG_FORMAT", log_format)
        output = io.StringIO()
        handler = logging.StreamHandler(output)
        handler.setFormatter(orch_log.build_formatter())
        logger = logging.getLogger("httpx")
        old_level, old_propagate = logger.level, logger.propagate
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
        try:
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(lambda _: httpx.Response(200))
            ) as client:
                response = await client.get(
                    "http://controller.test/vms/job-1",
                    params={
                        "provision_generation": "generation-2",
                        "lifecycle_auth": "synthetic-signature-123",
                        "lifecycle_auth_issued_at": "123",
                        "lifecycle_auth_request_id": "request-1",
                    },
                )
        finally:
            logger.removeHandler(handler)
            logger.setLevel(old_level)
            logger.propagate = old_propagate
        assert response.status_code == 200
        assert response.request.url.params["lifecycle_auth"] == (
            "synthetic-signature-123"
        )
        line = output.getvalue()
        if log_format == "json":
            line = json.loads(line)["message"]
        assert "synthetic-signature-123" not in line
        assert "GET" in line
        assert "/vms/job-1?provision_generation=generation-2" in line
        assert "lifecycle_auth=***REDACTED***" in line
        assert "lifecycle_auth_issued_at=123" in line
        assert "lifecycle_auth_request_id=request-1" in line
        assert "200" in line

    def test_lifecycle_signature_redacts_parameterized_message_and_exception(self, mod):
        url = (
            "/vms/job-1?lifecycle_auth=synthetic-signature-123"
            "&provision_generation=generation-2"
        )
        try:
            raise ValueError(f"request failed: {url}")
        except ValueError:
            exc_info = sys.exc_info()
        record = _record("request %s", url, exc_info=exc_info)
        for formatter in (mod.JsonLogFormatter(), mod.TextLogFormatter()):
            line = formatter.format(record)
            assert "synthetic-signature-123" not in line
            assert "lifecycle_auth=***REDACTED***" in line
            assert "provision_generation=generation-2" in line

    @pytest.mark.parametrize(
        "secret",
        [
            "api_key=sk-abcdef1234567890ABCDEFGH",
            "Authorization: Bearer eyJhbGciOi.JzdWIiOiI.SflKxwRJ",
            "password: hunter2supersecret",
            "client_secret=ZZxx9911aabbccddeeff",
            'token="ghp_ABCDEFGHIJKLMNOP1234567890"',
        ],
    )
    def test_secrets_are_masked(self, mod, secret):
        out = mod.redact(secret)
        assert mod._REDACTED in out
        # The raw secret value must not survive.
        for needle in ("sk-abcdef", "eyJhbGci", "hunter2", "ZZxx9911", "ghp_ABCDEF"):
            if needle in secret:
                assert needle not in out

    def test_non_secret_text_untouched(self, mod):
        msg = "job job-123 completed in 4200ms with status ok"
        assert mod.redact(msg) == msg

    def test_credential_bearing_remote_keeps_its_host(self, mod):
        # A failed push logs its remote; archived logs are read back by
        # get_job_log (workspace_git_credentials_in_tool_and_audit_output).
        token = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b"
        out = mod.redact(
            f"Tool git_push failed: fatal: unable to access "
            f"'https://oauth2:{token}@gitea.local/org/repo.git/': error 403"
        )
        assert token not in out and "oauth2" not in out
        assert f"https://{mod._REDACTED}@gitea.local/org/repo.git/" in out
        # A password-only userinfo is still a credential.
        assert "s3cretPass9" not in mod.redact("redis://:s3cretPass9@cache:6379/0")

    def test_a_hostile_jwt_run_is_linear(self, mod):
        # `\b` treats `-` as a boundary, so `eyJ-eyJ-…` started a JWT match at
        # every repetition and each rescanned the record: 0.85 s for 80 KB.
        import time

        best = float("inf")
        for _ in range(3):
            started = time.perf_counter()
            mod.redact("eyJ-" * 20_000)
            best = min(best, time.perf_counter() - started)
        assert best < 0.1

    def test_plain_remotes_are_untouched(self, mod):
        msg = (
            "cloned https://gitea.local/org/repo.git and "
            "ssh://git@gitea.local:2222/org/repo.git"
        )
        assert mod.redact(msg) == msg

    def test_json_message_is_redacted(self, mod):
        rec = _emit_json(mod, "dispatching with api_key=sk-LIVE1234567890abcdefXYZ")
        assert "sk-LIVE1234567890abcdefXYZ" not in json.dumps(rec)
        assert mod._REDACTED in rec["message"]

    def test_redacted_json_stays_valid(self, mod):
        # redaction must not break JSON parsing (no stray quotes/newlines)
        line = mod.JsonLogFormatter(component="test").format(
            _record("Authorization: Bearer eyJabc.def.ghijklmnop")
        )
        json.loads(line)  # must not raise


class TestCorrelationIdMiddleware:
    """Orchestrator-only ASGI middleware — request_id for the whole request."""

    async def _request_id_seen_downstream(self, headers):
        seen = {}

        async def downstream(scope, receive, send):
            # Must be visible to the downstream app (i.e. route handlers).
            seen["request_id"] = orch_log._log_context.get().get("request_id")

        mw = orch_log.CorrelationIdMiddleware(downstream)

        async def receive():
            return {"type": "http.request"}

        async def send(_message):
            return None

        await mw({"type": "http", "headers": headers}, receive, send)
        return seen["request_id"]

    @pytest.mark.asyncio
    async def test_honors_inbound_request_id(self):
        rid = await self._request_id_seen_downstream(
            [(b"x-request-id", b"trace-abc-123")]
        )
        assert rid == "trace-abc-123"
        # bound only for the request — cleared afterwards
        assert "request_id" not in orch_log._log_context.get()

    @pytest.mark.asyncio
    async def test_generates_when_absent(self):
        rid = await self._request_id_seen_downstream([])
        assert rid and len(rid) == 12

    @pytest.mark.asyncio
    async def test_sanitizes_hostile_request_id(self):
        rid = await self._request_id_seen_downstream(
            [(b"x-request-id", b'bad\nid"<script>')]
        )
        assert rid is not None
        for bad in ("\n", '"', "<", ">"):
            assert bad not in rid

    @pytest.mark.asyncio
    async def test_non_http_scope_passes_through(self):
        called = {}

        async def downstream(scope, receive, send):
            called["ok"] = True

        mw = orch_log.CorrelationIdMiddleware(downstream)
        await mw({"type": "websocket"}, None, None)
        assert called["ok"]
