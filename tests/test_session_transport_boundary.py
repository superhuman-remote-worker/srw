"""The session transport must not reach back into the runtime or app factories.

``pyproject.toml`` carries the matching import-linter contract for static
imports. This guard additionally rejects dynamic retrieval (``importlib``,
``__import__``, ``sys.modules``) and proves in a fresh interpreter that
importing the transport loads no application, runtime or graph module.
"""

from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

import pytest

import agent.api

TRANSPORT_MODULES = (
    "agent.api.session_contract",
    "agent.api.session_http",
    "agent.api.session_websocket",
    "agent.api.session_canvas_control",
    "agent.api._session_auth",
    "agent.api.session_transport",
)

FORBIDDEN = (
    "agent.api.persistent_app",
    "agent.api.dual_app",
    "agent.api.app",
    "agent.api.turn_executor",
    "agent.persistent_graph",
    "agent.graph",
    "agent.agent",
    "langgraph",
)

API_DIR = Path(agent.api.__file__).parent


def _source(module: str) -> str:
    return (API_DIR / f"{module.rsplit('.', 1)[1]}.py").read_text()


@pytest.mark.parametrize("module", TRANSPORT_MODULES)
def test_transport_source_names_no_runtime_or_factory_module(module):
    tree = ast.parse(_source(module))
    imported: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            base = node.module or ""
            imported.append(base)
            imported.extend(f"{base}.{alias.name}" for alias in node.names)
        elif isinstance(node, ast.Call):
            func = node.func
            name = getattr(func, "attr", None) or getattr(func, "id", None)
            assert name not in {"import_module", "__import__"}, (
                f"{module} imports dynamically"
            )
        elif isinstance(node, ast.Attribute) and node.attr == "modules":
            assert getattr(node.value, "id", None) != "sys", (
                f"{module} reads sys.modules"
            )
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            for forbidden in ("persistent_app", "dual_app", "turn_executor"):
                assert forbidden not in node.value.split(), (
                    f"{module} names {forbidden} in a string"
                )
    for name in imported:
        for forbidden in FORBIDDEN:
            assert name != forbidden and not name.startswith(forbidden + "."), (
                f"{module} imports {name}"
            )


def test_importing_the_transport_loads_no_application_or_runtime():
    code = f"""
import importlib, sys
for module in {TRANSPORT_MODULES!r}:
    importlib.import_module(module)
loaded = sorted(set({FORBIDDEN!r}) & sys.modules.keys())
assert not loaded, loaded
"""
    result = subprocess.run(
        [sys.executable, "-I", "-c", code],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr


def test_contract_module_stays_framework_free():
    code = """
import sys
import agent.api.session_contract
assert "fastapi" not in sys.modules, "session_contract imported fastapi"
assert "agent.api.persistent_app" not in sys.modules
"""
    result = subprocess.run(
        [sys.executable, "-I", "-c", code],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr


def test_both_applications_register_the_transport_from_its_owners():
    """No factory keeps a private copy of the transport handlers."""

    for factory in ("persistent_app", "dual_app"):
        source = (API_DIR / f"{factory}.py").read_text()
        assert "register_session_http_routes(" in source
        assert "register_session_websocket_routes(" in source
        for retired in (
            "def handle_api_input",
            "def handle_api_interrupt",
            "def handle_api_approve",
            "def handle_persistent_websocket",
            "def _handle_canvas_control",
            "def _run_subscriber_pump",
            "validate_session_token",
        ):
            assert retired not in source, f"{factory} still defines {retired}"
