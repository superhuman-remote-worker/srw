"""Credential delivery executes real scripts with synthetic values only."""

import importlib.machinery
import importlib.util
import json
import os
from pathlib import Path
import shlex
import subprocess
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from shared.credential_connectors import (
    collect_credential_env,
    normalize_credential_env,
)
from shared.runtime.core.backends.remote import RemoteBackend
from shared.runtime.core.credential_env import INSTALL_CREDENTIAL_ENV


@pytest.mark.parametrize(
    "name", ["1BAD", "bad-name", "PATH", "BASH_ENV", "SRW_TOKEN", "LD_PRELOAD"]
)
def test_invalid_or_reserved_env_names(name):
    with pytest.raises(ValueError) as caught:
        normalize_credential_env({name: "synthetic-value"})
    assert "synthetic-value" not in str(caught.value)


def test_conflicting_connector_names_are_rejected():
    with pytest.raises(ValueError, match="Multiple attached connectors"):
        collect_credential_env(
            [
                {
                    "type": "credentials",
                    "credentials": {"env_vars": {"API_KEY": "one"}},
                },
                {"type": "generic", "credentials": {"env_vars": {"API_KEY": "two"}}},
            ]
        )


@pytest.mark.parametrize("env_vars", [{}, None])
def test_a_credentials_connector_without_variables_is_refused(env_vars):
    """The credentials spec's env_vars slot is required; generic's is not."""
    credentials = {} if env_vars is None else {"env_vars": env_vars}
    with pytest.raises(ValueError, match="Add at least one credential"):
        collect_credential_env([{"type": "credentials", "credentials": credentials}])
    assert (
        collect_credential_env([{"type": "generic", "credentials": credentials}]) == {}
    )


def test_only_env_connectors_contribute_variables():
    assert collect_credential_env(
        [
            {"type": "generic", "credentials": {"env_vars": {"A": "1"}}},
            {"type": "credentials", "credentials": {"env_vars": {"B": "2"}}},
            {"type": "postgresql", "credentials": {"env_vars": {"C": "3"}}},
        ]
    ) == {"A": "1", "B": "2"}


def _install(target, values):
    subprocess.run(
        ["python3", "-c", INSTALL_CREDENTIAL_ENV, str(target)],
        input=json.dumps(values),
        text=True,
        capture_output=True,
        check=True,
    )


def _read_vars(target, names):
    code = (
        "import json,os; print(json.dumps({n:os.environ.get(n) for n in "
        + repr(names)
        + "}))"
    )
    output = subprocess.check_output(
        ["bash", "-c", f". {shlex.quote(str(target))}; python3 -c {shlex.quote(code)}"],
        text=True,
    )
    return json.loads(output)


def test_install_retains_fields_and_quotes_shell_content(tmp_path):
    target = tmp_path / "session with spaces" / "env.sh"
    marker = tmp_path / "must-not-exist"
    value = f"quote' newline\n$(touch {marker}) `touch {marker}` $HOME"
    _install(target, {"API_KEY": value, "RETAINED": "old"})
    assert _read_vars(target, ["API_KEY", "RETAINED"]) == {
        "API_KEY": value,
        "RETAINED": "old",
    }
    assert not marker.exists()
    _install(target, {"API_KEY": "replacement"})
    assert _read_vars(target, ["API_KEY", "RETAINED"]) == {
        "API_KEY": "replacement",
        "RETAINED": "old",
    }
    assert target.stat().st_mode & 0o777 == 0o600
    assert target.with_suffix(".json").stat().st_mode & 0o777 == 0o600


def test_remote_delivery_uses_private_transport_and_work_identity(
    tmp_path, monkeypatch
):
    paths = []
    for job_id, value in [("job-one", "first"), ("job-two", "second")]:
        backend = RemoteBackend(host="unused", job_id=job_id)
        monkeypatch.setattr(backend, "_init_shell", lambda: None)
        monkeypatch.setattr(
            backend, "_resolve_home_path", lambda path: str(tmp_path / path)
        )
        sent = []

        def send(command, secret, **kwargs):
            sent.append((command, secret))
            assert value not in command
            subprocess.run(
                ["bash", "-c", command],
                input=secret,
                text=True,
                capture_output=True,
                check=True,
            )
            return True

        monkeypatch.setattr(backend, "execute_claim_resource_with_secret_stdin", send)
        backend.install_credential_environment({"API_KEY": value})
        assert sent and json.loads(sent[0][1]) == {"API_KEY": value}
        paths.append(backend._credential_env_path)

        def execute(command, timeout=30):
            return subprocess.check_output(["bash", "-c", command], text=True)

        monkeypatch.setattr(backend, "_exec", execute)
        assert backend.exec_command('printf "%s" "$API_KEY"') == value
        command, _ = backend._build_guarded_shell_command(
            'printf "%s\\n" "$API_KEY"', "__DONE_test__", None
        )
        assert (
            subprocess.check_output(["bash", "-c", command], text=True).splitlines()[0]
            == value
        )
    assert paths[0] != paths[1]


def test_generic_credentials_do_not_enter_agent_environment(monkeypatch):
    from tests._connector_runtime import open_harness

    monkeypatch.delenv("SYNTHETIC_CONNECTOR_KEY", raising=False)
    connections, _ = open_harness(
        [
            {
                "type": "generic",
                "credentials": {"env_vars": {"SYNTHETIC_CONNECTOR_KEY": "value"}},
            }
        ]
    )
    assert connections == {}
    assert "SYNTHETIC_CONNECTOR_KEY" not in os.environ


def _deliver_env(entries, workspace):
    from agent.connectors import RuntimeContext, deliveries_from_payload
    from agent.connectors.env import EnvFileMaterializer

    EnvFileMaterializer().materialize(
        deliveries_from_payload(entries),
        RuntimeContext(execution="session", workspace_manager=workspace),
    )


def test_credentials_require_shell_workspace():
    workspace = SimpleNamespace(backend=SimpleNamespace(supports_shell=False))
    with pytest.raises(ValueError, match="sandbox or VM"):
        _deliver_env(
            [
                {
                    "type": "credentials",
                    "credentials": {"env_vars": {"API_KEY": "synthetic"}},
                },
            ],
            workspace,
        )


def test_the_environment_materializer_installs_every_env_connector():
    installed = []
    workspace = SimpleNamespace(
        backend=SimpleNamespace(
            supports_shell=True, install_credential_environment=installed.append
        )
    )
    _deliver_env(
        [
            {"type": "generic", "credentials": {"env_vars": {"A": "1"}}},
            {"type": "postgresql", "credentials": {"env_vars": {"IGNORED": "x"}}},
            {"type": "credentials", "credentials": {"env_vars": {"B": "2"}}},
        ],
        workspace,
    )
    assert installed == [{"A": "1", "B": "2"}]


@pytest.mark.parametrize(
    ("entries", "message"),
    [
        (
            [
                {"type": "credentials", "credentials": {"env_vars": {"K": "one"}}},
                {"type": "generic", "credentials": {"env_vars": {"K": "two"}}},
            ],
            "Multiple attached connectors define K",
        ),
        (
            [{"type": "credentials", "credentials": {"env_vars": {}}}],
            "Add at least one credential environment variable",
        ),
        (
            [{"type": "generic", "credentials": {"env_vars": {"PATH": "/x"}}}],
            "reserved by the workspace",
        ),
        (
            [{"type": "credentials", "credentials": {"env_vars": ["API_KEY"]}}],
            "must be a name/value object",
        ),
        (
            [{"type": "generic", "credentials": {"env_vars": None}}],
            "must be a name/value object",
        ),
    ],
)
def test_the_environment_materializer_keeps_the_delivery_errors(entries, message):
    """The same refusals collect_credential_env gave, at delivery time."""
    from agent.connectors import deliveries_from_payload
    from agent.connectors.env import credential_environment

    with pytest.raises(ValueError, match=message):
        credential_environment(deliveries_from_payload(entries))


def _browser_executor():
    loader = importlib.machinery.SourceFileLoader(
        "credential_browser_exec_test",
        str(Path(__file__).parents[1] / "docker/browser-exec"),
    )
    spec = importlib.util.spec_from_loader(loader.name, loader)
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    return module


def test_browser_client_resolves_env_locally(monkeypatch, capsys):
    module = _browser_executor()
    monkeypatch.setenv("TAX_PASSWORD", "synthetic-password")
    request = MagicMock(return_value=b'{"dom":"filled"}\n')
    monkeypatch.setattr(module, "_request", request)
    args = {"ref": 42, "env_var": "TAX_PASSWORD"}
    assert module.run_client("type", args, 1) == 0
    assert json.loads(request.call_args.args[0])["args"] == {
        "ref": 42,
        "text": "synthetic-password",
    }
    assert args == {"ref": 42, "env_var": "TAX_PASSWORD"}
    assert "synthetic-password" not in capsys.readouterr().out


def test_browser_missing_variable_never_sends_input(monkeypatch, capsys):
    module = _browser_executor()
    monkeypatch.delenv("MISSING_CREDENTIAL", raising=False)
    request = MagicMock()
    monkeypatch.setattr(module, "_request", request)
    assert (
        module.run_client("type", {"ref": 1, "env_var": "MISSING_CREDENTIAL"}, 1) == 1
    )
    request.assert_not_called()
    assert "not set" in capsys.readouterr().out


@pytest.mark.asyncio
async def test_browser_tool_only_dispatches_reference():
    from agent.tools.research.browser_direct import create_browser_direct_tools

    context = SimpleNamespace(
        config={},
        browser_exec=AsyncMock(return_value={"dom": "filled"}),
        should_include_screenshots=lambda: False,
        get_max_dom_chars=lambda: 1000,
    )
    tool = next(
        t for t in create_browser_direct_tools(context) if t.name == "browser_type"
    )
    await tool.ainvoke({"ref": 42, "env_var": "TAX_PASSWORD"})
    assert context.browser_exec.await_args.kwargs["env_var"] == "TAX_PASSWORD"
    assert "text" not in context.browser_exec.await_args.kwargs
    context.browser_exec.reset_mock()
    result = await tool.ainvoke(
        {"ref": 42, "env_var": "TAX_PASSWORD", "text": "literal"}
    )
    assert "exactly one" in result
    context.browser_exec.assert_not_awaited()
