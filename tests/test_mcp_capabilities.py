"""Contract tests for the orchestrator-facing MCP tool surface."""

from __future__ import annotations

import ast
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

from mcp_server.capabilities import TOOL_CAPABILITIES, WORKFLOW_DECISIONS
from shared.orch_surface.jobs import JOB_DESCRIPTORS

ROOT = Path(__file__).parent.parent
SERVER_PATH = ROOT / "src" / "mcp_server" / "server.py"


def _registered_tool_names() -> set[str]:
    tree = ast.parse(SERVER_PATH.read_text(encoding="utf-8"))
    handwritten = {
        node.name
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and any(
            isinstance(decorator, ast.Name) and decorator.id == "mcp_tool"
            for decorator in node.decorator_list
        )
    }
    return handwritten | {item.name for item in JOB_DESCRIPTORS}


def _source_schema() -> dict:
    """Load the real FastMCP schema in a clean interpreter.

    ``tests/test_mcp.py`` intentionally mocks FastMCP at import time, so a
    subprocess keeps this protocol contract independent of test order.
    """
    script = """
import asyncio
import json
from mcp_server.server import canonical_tool_schema

async def main():
    tools, digest = await canonical_tool_schema()
    print(json.dumps({"tools": tools, "digest": digest}, sort_keys=True))

asyncio.run(main())
"""
    env = dict(os.environ)
    env["MCP_TRANSPORT"] = "stdio"
    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=ROOT,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(completed.stdout.strip().splitlines()[-1])


def _build_info(schema_artifact: Path) -> dict:
    script = """
import asyncio
import json
from mcp_server.server import _mcp_build_info

async def main():
    print(json.dumps(await _mcp_build_info(), sort_keys=True))

asyncio.run(main())
"""
    env = dict(os.environ)
    env.update(
        {
            "MCP_TRANSPORT": "stdio",
            "MCP_SCHEMA_ARTIFACT": str(schema_artifact),
            "SRW_SOURCE_REVISION": "source-test-revision",
            "SRW_RELEASE_VERSION": "test-release",
            "SRW_ARTIFACT_DIGEST": "sha256:image-test-digest",
        }
    )
    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=ROOT,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(completed.stdout.strip().splitlines()[-1])


def test_every_registered_tool_has_exactly_one_capability_contract() -> None:
    assert _registered_tool_names() == set(TOOL_CAPABILITIES)
    # 115 existing tools plus seven native manifest/resource operations.
    assert len(TOOL_CAPABILITIES) == 122


def test_capability_contract_records_required_risk_and_transport_fields() -> None:
    for name, contract in TOOL_CAPABILITIES.items():
        assert contract.name == name
        assert contract.operation.split(" ", 1)[0] in {
            "GET",
            "POST",
            "PUT",
            "PATCH",
            "DELETE",
        }
        assert contract.authorization
        assert contract.side_effects
        assert (
            contract.schema_source == "raw MCP tools/list inputSchema and outputSchema"
        )
        assert contract.coverage
        assert set(contract.annotations) == {
            "readOnlyHint",
            "destructiveHint",
            "idempotentHint",
            "openWorldHint",
        }
        if contract.read_only:
            assert not contract.destructive
            assert contract.idempotent
        else:
            assert "single attempt" in contract.retry_policy


def test_raw_tools_list_matches_manifest_annotations_and_canonical_digest() -> None:
    document = _source_schema()
    tools = document["tools"]
    assert {tool["name"] for tool in tools} == set(TOOL_CAPABILITIES)

    for tool in tools:
        contract = TOOL_CAPABILITIES[tool["name"]]
        assert tool["annotations"] == contract.annotations
        metadata = tool["_meta"]["io.srw.capability"]
        assert metadata == contract.metadata()
        assert tool["inputSchema"]["type"] == "object"
        assert "outputSchema" in tool

    canonical = json.dumps(
        {"tools": tools},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    assert document["digest"] == f"sha256:{hashlib.sha256(canonical).hexdigest()}"


def test_health_build_info_reports_digest_provenance_and_artifact_match(
    tmp_path: Path,
) -> None:
    schema = _source_schema()
    artifact = tmp_path / "tool-schema.json"
    artifact.write_text(json.dumps(schema), encoding="utf-8")

    info = _build_info(artifact)

    assert info["tool_schema_revision"] == "14"
    assert info["tool_schema_digest"] == schema["digest"]
    assert info["schema_artifact_digest"] == schema["digest"]
    assert info["schema_artifact_status"] == "match"
    assert info["tool_count"] == 122
    assert info["source_revision"] == "source-test-revision"
    assert info["release_version"] == "test-release"
    assert info["artifact_digest"] == "sha256:image-test-digest"
    assert info["python_version"]
    assert info["fastmcp_version"]
    assert info["mcp_sdk_version"]

    artifact.write_text(
        json.dumps({"digest": schema["digest"], "tools": []}), encoding="utf-8"
    )
    assert _build_info(artifact)["schema_artifact_status"] == "mismatch"


def test_priority_job_project_and_connector_schema_drift_is_closed() -> None:
    tools = {tool["name"]: tool for tool in _source_schema()["tools"]}

    list_status = tools["list_jobs"]["inputSchema"]["properties"]["status"]
    assert "paused" in list_status["anyOf"][0]["enum"]

    job_fields = tools["create_job"]["inputSchema"]["properties"]
    assert {
        "expert_id",
        "kickoff_message",
        "priority",
        "context",
        "slot",
        "workspace",
    } <= set(job_fields)
    # project_id is deliberately model-visible on both lanes (explicit value
    # wins over the hidden lineage default); the rest of the lineage stays
    # server-bound.
    assert "project_id" in job_fields
    assert not {"user_id", "thread_id", "parent_job_id"} & set(job_fields)
    # datasource_ids left this descriptor with the dispatch-time attach
    # (2afbf956): its callers are dispatchers with no basis for connector
    # surgery, and an advertised array invited the empty one, which reads as
    # neutral but means "attach nothing". Omission now resolves the project's
    # auto-attach defaults server-side. Narrowing stays on the surfaces a
    # human reviews — create_project_job below, REST, cockpit.
    assert "datasource_ids" not in job_fields

    project_job_fields = tools["create_project_job"]["inputSchema"]["properties"]
    assert {
        "expert_id",
        "kickoff_message",
        "priority",
        "context",
        "required_deliverables",
        "workspace",
    } <= set(project_job_fields)
    assert project_job_fields["datasource_ids"]["type"] == "array"
    assert "anyOf" not in project_job_fields["datasource_ids"]

    persistent_thread_schema = tools["create_persistent_thread"]["inputSchema"]
    persistent_thread_fields = persistent_thread_schema["properties"]
    assert "workspace" in persistent_thread_fields
    assert persistent_thread_fields["datasource_ids"]["type"] == "array"
    assert "anyOf" not in persistent_thread_fields["datasource_ids"]
    assert "datasource_ids" not in persistent_thread_schema.get("required", [])

    for tool_name in ("create_project", "update_project"):
        properties = tools[tool_name]["inputSchema"]["properties"]
        assert "default_config_override" in properties

    connector = tools["create_datasource"]["inputSchema"]["properties"]
    assert set(connector["type"]["enum"]) == {
        "generic",
        "repository",
        "kb",
        "postgresql",
        "neo4j",
        "mongodb",
        "webdav",
        "email",
        "mcp",
        "kubeconfig",
        "ssh_key",
        "generic_file",
    }
    assert {
        "config",
        "is_global",
        "read_only",
        "scope_mode",
        "project_ids",
        "auto_attach",
    } <= set(connector)
    assert "job_id" not in connector
    assert connector["scope_mode"]["enum"] == ["all", "projects"]
    assert connector["project_ids"]["type"] == "array"

    connector_update = tools["update_datasource"]["inputSchema"]["properties"]
    assert {
        "config",
        "is_global",
        "read_only",
        "scope_mode",
        "project_ids",
        "auto_attach",
        "policy_revision",
    } <= set(connector_update)
    for field in ("scope_mode", "project_ids", "auto_attach", "policy_revision"):
        assert "anyOf" not in connector_update[field]

    connector_list = tools["list_datasources"]["inputSchema"]["properties"]
    assert {
        "ds_type",
        "q",
        "project_id",
        "scope_mode",
        "auto_attach",
        "visibility",
        "ownership",
        "availability",
        "limit",
        "cursor",
    } <= set(connector_list)
    assert connector_list["scope_mode"]["enum"] == ["all", "projects"]
    assert connector_list["visibility"]["enum"] == ["public", "private"]
    assert connector_list["ownership"]["enum"] == ["mine", "shared"]
    assert connector_list["availability"]["enum"] == [
        "all",
        "projects",
        "unavailable",
    ]

    connector_get = tools["get_datasource"]["inputSchema"]
    assert connector_get["required"] == ["datasource_id"]


def test_missing_rest_workflows_have_an_explicit_decision() -> None:
    expected = {
        "direct job message-thread retrieval",
        "job accept/reject review actions",
        "job export",
        "job workspace upgrade, snapshot, and IDE controls",
        "persistent-session input and interrupt",
        "persistent approvals",
        "persistent configuration and tool-group mutation",
        "persistent cloud-diff internals",
        "datasource eligibility",
        "datasource indexing status and reindex",
        "user expert and skill administration",
    }
    assert {decision.workflow for decision in WORKFLOW_DECISIONS} == expected
    for decision in WORKFLOW_DECISIONS:
        assert decision.disposition in {
            "required",
            "intentionally_excluded",
            "superseded",
        }
        assert decision.authorization
        assert decision.reason
