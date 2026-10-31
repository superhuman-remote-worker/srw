"""Regression checks for the MCP job-creation/dispatch contract."""

import ast
import importlib
import os
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from shared.orch_surface.formatters import format_created_job
from shared.orch_surface.jobs import get_descriptor


SERVER_PATH = Path(__file__).parent.parent / "src" / "mcp_server" / "server.py"


def _async_function(tree: ast.AST, name: str) -> ast.AsyncFunctionDef:
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == name:
            return node
    raise AssertionError(f"MCP function {name!r} not found")


def test_created_job_response_describes_automatic_dispatch():
    rendered = format_created_job(
        {"id": "job-1", "status": "created", "description": "research"},
        "scholar",
    )

    assert "Queued for automatic workspace provisioning" in rendered
    assert "Next step: Use assign_job" not in rendered


def test_created_job_response_reports_workspace_assignment() -> None:
    rendered = format_created_job(
        {
            "id": "job-1",
            "status": "created",
            "workspace_contract": {
                "requested_backend": "vm",
                "assigned_backend": "vm",
                "effective_backend": None,
                "state": "waiting",
            },
        },
        "developer",
    )

    assert "Workspace: requested=vm, assigned=vm, effective=pending" in rendered


def test_mcp_creation_signatures_expose_deliverable_contract():
    tree = ast.parse(SERVER_PATH.read_text(encoding="utf-8"))

    assert (
        "required_deliverables"
        in get_descriptor("create_job").public_signature.parameters
    )
    function = _async_function(tree, "create_project_job")
    argument_names = {argument.arg for argument in function.args.args}
    assert "required_deliverables" in argument_names


def test_both_creation_tools_take_the_one_expert_selector():
    """`expert` accepts a bundled slug or a DB UUID on either tool, so a
    caller can pass any id `list_experts` printed without knowing which store
    it came from. The single-store aliases stay for existing callers.
    See knowledge-base/knowledge/issues/experts_one_catalogue_two_selection_paths.md."""
    signature = get_descriptor("create_job").public_signature.parameters
    assert {"expert", "config_name", "expert_id"} <= set(signature)

    tree = ast.parse(SERVER_PATH.read_text(encoding="utf-8"))
    function = _async_function(tree, "create_project_job")
    argument_names = {argument.arg for argument in function.args.args}
    assert {"expert", "config_name", "expert_id"} <= argument_names
    assert "list_experts" in (ast.get_docstring(function) or "")


def test_workspace_reader_promises_committed_gitea_state():
    tree = ast.parse(SERVER_PATH.read_text(encoding="utf-8"))
    function = _async_function(tree, "get_workspace_file")
    doc = ast.get_docstring(function) or ""

    assert "Gitea-backed" in doc
    assert "committed state" in doc


def _mcp_server():
    os.environ.setdefault("MCP_TRANSPORT", "stdio")
    return importlib.import_module("mcp_server.server")


@pytest.mark.asyncio
async def test_workspace_reader_describes_a_binary_file_instead_of_its_base64():
    server = _mcp_server()
    client = AsyncMock()
    client.get_workspace_file.return_value = {
        "path": "output/report.pdf",
        "content": "JVBERi0xLjcK4uPP0w==",
        "encoding": "base64",
        "size": 14,
    }

    with patch.object(server, "_get_client", return_value=client):
        rendered = await server.get_workspace_file("job-1", "output/report.pdf")

    assert "Binary file, 14 bytes" in rendered
    assert "JVBERi0" not in rendered


@pytest.mark.asyncio
async def test_workspace_reader_shows_text_unchanged():
    server = _mcp_server()
    client = AsyncMock()
    client.get_workspace_file.return_value = {
        "path": "plan.md",
        "content": "# Plan",
        "encoding": "utf-8",
        "size": 6,
    }

    with patch.object(server, "_get_client", return_value=client):
        rendered = await server.get_workspace_file("job-1", "plan.md")

    assert rendered == "Workspace file: plan.md (job job-1)\n---\n# Plan"
