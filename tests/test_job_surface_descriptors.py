"""Single-source contracts for the unified job-management surface."""

from __future__ import annotations

import ast
import base64
import inspect
import json
from pathlib import Path
import subprocess
import sys
from typing import Any

import httpx
import pytest

from mcp_server.capabilities import TOOL_CAPABILITIES
from mcp_server.job_adapter import register_job_tools
from shared.orch_surface.client import AsyncCockpitClient
from shared.orch_surface.jobs import (
    CallerCtx,
    JOB_DESCRIPTORS,
    JobToolResult,
    get_descriptor,
    make_bound_handler,
)
from agent.tools.context import ToolContext
from agent.tools.orchestrator import jobs as jobs_module
from agent.tools.orchestrator.jobs import create_orchestrator_tools


ROOT = Path(__file__).parent.parent
SHARED_ROOT = ROOT / "src" / "shared" / "orch_surface"


class FakeMcp:
    def __init__(self) -> None:
        self.tools: dict[str, Any] = {}
        self.options: dict[str, dict[str, Any]] = {}

    def tool(self, function: Any, **options: Any) -> Any:
        name = options.get("name") or function.__name__
        self.tools[name] = function
        self.options[name] = options
        return function


def _langchain_tools() -> dict[str, Any]:
    return {
        item.name: item
        for item in create_orchestrator_tools(ToolContext(user_id="user-1"))
        if item.name != "get_session_context"
    }


def _without_framework_schema_decoration(schema: dict[str, Any]) -> dict[str, Any]:
    normalized = json.loads(json.dumps(schema))
    normalized.pop("title", None)
    normalized.pop("description", None)
    normalized.pop("additionalProperties", None)

    def strip_decoration(value: Any) -> None:
        if isinstance(value, dict):
            value.pop("title", None)
            # Per-property descriptions are docstring-derived decoration and
            # SDK-version-dependent: some mcp releases parse Google-style
            # `Args:` blocks into property descriptions, others don't (first
            # bitten by the M3 routing tools — the only descriptors with
            # parseable Args sections). Parity here is about STRUCTURE —
            # names, types, required, defaults — so strip descriptions at
            # every level, like titles.
            value.pop("description", None)
            for nested in value.values():
                strip_decoration(nested)
        elif isinstance(value, list):
            for nested in value:
                strip_decoration(nested)

    strip_decoration(normalized)
    return normalized


def test_descriptor_inventory_is_unique_complete_and_classified() -> None:
    # 43 = the 37 unified descriptors + E4's three job_evidence tools
    # (get_job_completion_report, list_job_evidence, read_job_evidence)
    # + M3's three officer message actions (reply_to_job_message,
    # escalate_job_message, acknowledge_job_message).
    names = [item.name for item in JOB_DESCRIPTORS]
    assert names == sorted(names)
    assert len(names) == len(set(names)) == 43
    assert {item.group for item in JOB_DESCRIPTORS} == {
        "job_control",
        "job_inspection",
    }
    # officer_supervision_surface E2: machine-readable planes replace the
    # coarse control/observability/object triple.
    assert {item.plane for item in JOB_DESCRIPTORS} == {
        "job_control",
        "job_observability",
        "job_evidence",
        "job_workspace",
    }
    assert all(
        item.description == inspect.getdoc(item.handler) for item in JOB_DESCRIPTORS
    )
    assert all(tuple(item.public_signature.parameters) for item in JOB_DESCRIPTORS)


def test_officer_defaults_follow_the_plane_boundary() -> None:
    """E2 policy pins, independent of the JSON fixture: the background
    officer never defaults into the object plane, and every job_control /
    job_observability / job_evidence descriptor granted to the officer is
    exactly the §3 set."""
    for item in JOB_DESCRIPTORS:
        if item.plane == "job_workspace":
            assert "officer" not in item.caller_defaults, item.name
        if item.plane == "job_evidence":
            assert "officer" in item.caller_defaults, item.name
    officer_control = {
        item.name
        for item in JOB_DESCRIPTORS
        if item.plane == "job_control" and "officer" in item.caller_defaults
    }
    assert officer_control == {
        "acknowledge_job_message",
        "approve_job",
        "cancel_job",
        "create_job",
        "escalate_job_message",
        "pause_job",
        "reply_to_job_message",
        "resume_job_with_feedback",
        "send_message_to_job",
        "steer_job",
    }


def test_caller_context_scopes_only_one_trusted_project_binding() -> None:
    project_id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    assert (
        CallerCtx(kind="session", project_ids=(project_id,)).scope_header
        == f"project:{project_id}"
    )
    assert CallerCtx(kind="session").scope_header is None
    assert (
        CallerCtx(
            kind="session",
            project_ids=(project_id, "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
        ).scope_header
        is None
    )


def test_shared_surface_imports_only_stdlib_httpx_and_shared_contracts() -> None:
    violations: list[str] = []
    shared_contracts = (
        "shared.orch_surface",
        "shared.runtime_actor",
        "shared.job_outcome",
        "shared.expert_reference",
        "shared.worker_execution_hold",
    )
    for path in SHARED_ROOT.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                modules = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                modules = [node.module]
            else:
                continue
            for module in modules:
                if (
                    module.split(".", 1)[0] not in sys.stdlib_module_names
                    and module.split(".", 1)[0] != "httpx"
                    and not any(
                        module == allowed or module.startswith(allowed + ".")
                        for allowed in shared_contracts
                    )
                ):
                    violations.append(f"{path.relative_to(ROOT)} imports {module}")
    assert violations == []


def test_mcp_and_langchain_register_exactly_the_descriptor_inventory() -> None:
    fake = FakeMcp()
    registered = register_job_tools(
        fake,
        # Providers are deliberately lazy; registration must not open a client.
        client_provider=lambda: None,  # type: ignore[arg-type,return-value]
        caller_provider=lambda: CallerCtx(kind="mcp"),
        capabilities=TOOL_CAPABILITIES,
    )

    expected = {item.name for item in JOB_DESCRIPTORS}
    assert set(registered) == expected
    assert set(fake.tools) == expected
    assert set(_langchain_tools()) == expected
    for item in JOB_DESCRIPTORS:
        assert (
            fake.options[item.name]["annotations"]
            == TOOL_CAPABILITIES[item.name].annotations
        )


def test_adapter_schemas_match_for_every_job_descriptor() -> None:
    # Some older tests intentionally mock ``mcp`` at collection time. Load the
    # real FastMCP schema in a clean interpreter so parity is order-independent.
    script = """
import asyncio
import json
from mcp_server.server import canonical_tool_schema
from shared.orch_surface.jobs import JOB_DESCRIPTORS

async def main():
    tools, _ = await canonical_tool_schema()
    names = {item.name for item in JOB_DESCRIPTORS}
    print(json.dumps({item["name"]: item["inputSchema"] for item in tools if item["name"] in names}))

asyncio.run(main())
"""
    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    mcp_tools = json.loads(completed.stdout.strip().splitlines()[-1])
    langchain_tools = _langchain_tools()

    assert set(mcp_tools) == set(langchain_tools)
    for item in JOB_DESCRIPTORS:
        langchain_schema = langchain_tools[item.name].args_schema.model_json_schema()
        assert _without_framework_schema_decoration(
            mcp_tools[item.name]
        ) == _without_framework_schema_decoration(langchain_schema), item.name
        # Whitespace-insensitive: whether the adapter takes raw ``__doc__``
        # (indented continuation lines) or ``inspect.getdoc`` (dedented) is a
        # dependency-version detail — the contract is the same TEXT.
        assert " ".join(langchain_tools[item.name].description.split()) == " ".join(
            item.description.split()
        ), item.name


@pytest.mark.asyncio
async def test_mcp_and_langchain_render_the_same_handler_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/jobs"
        return httpx.Response(
            200,
            json=[
                {
                    "id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                    "status": "processing",
                    "config_name": "worker_base",
                    "created_at": "2026-08-14T08:00:00Z",
                    "audit_count": 2,
                }
            ],
        )

    client = AsyncCockpitClient(
        base_url="http://orchestrator.test",
        transport=httpx.MockTransport(handler),
    )
    fake = FakeMcp()
    register_job_tools(
        fake,
        client_provider=lambda: client,
        caller_provider=lambda: CallerCtx(kind="mcp", user_id="user-1"),
        capabilities=TOOL_CAPABILITIES,
    )
    monkeypatch.setattr(jobs_module, "_get_surface_client", lambda: client)

    try:
        mcp_output = await fake.tools["list_jobs"]()
        langchain_output = await _langchain_tools()["list_jobs"].ainvoke({})
    finally:
        await client.close()

    assert mcp_output == langchain_output


@pytest.mark.asyncio
async def test_evidence_image_is_typed_for_mcp_and_base64_free_for_text_lane() -> None:
    encoded = base64.b64encode(b"bounded-png").decode("ascii")

    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/jobs/job-1/evidence/ev-shot"
        return httpx.Response(
            200,
            json={
                "entry": {
                    "id": "ev-shot",
                    "kind": "screenshot",
                    "label": "known feature",
                    "media_type": "image/png",
                    "byte_size": 11,
                    "sha256": "abc",
                    "availability": "available",
                },
                "attachment": {
                    "type": "image",
                    "media_type": "image/png",
                    "base64_data": encoded,
                    "byte_size": 11,
                    "width": 2,
                    "height": 2,
                },
            },
        )

    client = AsyncCockpitClient(
        base_url="http://orchestrator.test",
        transport=httpx.MockTransport(handler),
    )
    fake = FakeMcp()
    register_job_tools(
        fake,
        client_provider=lambda: client,
        caller_provider=lambda: CallerCtx(kind="mcp"),
        capabilities=TOOL_CAPABILITIES,
    )
    text_invoke = make_bound_handler(
        get_descriptor("read_job_evidence"),
        client_provider=lambda: client,
        caller_provider=lambda: CallerCtx(kind="session", supports_multimodal=False),
    )
    try:
        mcp_output = await fake.tools["read_job_evidence"](
            job_id="job-1", evidence_id="ev-shot"
        )
        text_output = await text_invoke(job_id="job-1", evidence_id="ev-shot")
    finally:
        await client.close()

    assert isinstance(mcp_output, list)
    assert type(mcp_output[0]).__name__ == "TextContent"
    assert type(mcp_output[1]).__name__ == "ImageContent"
    assert mcp_output[1].data == encoded
    assert mcp_output[1].mimeType == "image/png"
    assert encoded not in mcp_output[0].text
    assert isinstance(text_output, str)
    assert encoded not in text_output
    assert "text-only" in text_output


@pytest.mark.asyncio
async def test_multimodal_descriptor_returns_exactly_one_attachment() -> None:
    encoded = base64.b64encode(b"one-image").decode("ascii")

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "entry": {
                    "id": "ev-shot",
                    "kind": "screenshot",
                    "media_type": "image/png",
                    "availability": "available",
                },
                "attachment": {
                    "type": "image",
                    "media_type": "image/png",
                    "base64_data": encoded,
                    "byte_size": 9,
                    "width": 1,
                    "height": 1,
                },
            },
        )

    client = AsyncCockpitClient(
        base_url="http://orchestrator.test",
        transport=httpx.MockTransport(handler),
    )
    invoke = make_bound_handler(
        get_descriptor("read_job_evidence"),
        client_provider=lambda: client,
        caller_provider=lambda: CallerCtx(kind="session", supports_multimodal=True),
    )
    try:
        result = await invoke(job_id="job-1", evidence_id="ev-shot")
    finally:
        await client.close()
    assert isinstance(result, JobToolResult)
    assert result.image is not None
    assert result.image.decoded() == b"one-image"
    assert encoded not in result.text


@pytest.mark.asyncio
async def test_mcp_job_ids_remain_verbatim_without_agent_prefix_resolution() -> None:
    observed_paths: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        observed_paths.append(request.url.path)
        return httpx.Response(200, json={"id": "job-id...", "status": "paused"})

    client = AsyncCockpitClient(
        base_url="http://orchestrator.test",
        transport=httpx.MockTransport(handler),
    )
    invoke = make_bound_handler(
        get_descriptor("get_job"),
        client_provider=lambda: client,
        caller_provider=lambda: CallerCtx(kind="mcp"),
    )
    try:
        await invoke(job_id="job-id...")
    finally:
        await client.close()

    assert observed_paths == ["/api/jobs/job-id..."]


@pytest.mark.asyncio
async def test_create_job_forwards_ticket_and_category_to_the_funnel() -> None:
    """The Resavio live-fire gap (2026-08-15): JobCreate.ticket existed at
    the funnel but the descriptor never exposed it, so the officer improvised
    a ``backlog_ticket`` context key the claim ledger cannot read — his
    dispatch left the ticket unclaimed and a second dispatch was possible.
    The descriptor must forward ``ticket`` and ``work_category`` as the
    typed body fields the funnel stamps (context.ticket_note_id, the
    precedence-law record), alongside slot's context translation."""
    observed_bodies: list[dict] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/jobs"
        observed_bodies.append(json.loads(request.content))
        return httpx.Response(
            200,
            json={
                "id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                "status": "created",
                "config_name": "worker_base",
            },
        )

    client = AsyncCockpitClient(
        base_url="http://orchestrator.test",
        transport=httpx.MockTransport(handler),
    )
    invoke = make_bound_handler(
        get_descriptor("create_job"),
        client_provider=lambda: client,
        caller_provider=lambda: CallerCtx(kind="session", user_id="user-1"),
    )
    try:
        await invoke(
            description="verify the preserved UI candidate",
            slot="test",
            ticket="backlog-tester-final-runtime-acceptance",
            work_category="tester",
            context={
                "ordinary": "preserved",
                "evidence_manifest": {
                    "source_repository": "victim-private-repo",
                    "source_revision": "f" * 40,
                },
                "ticket_note_id": "forged-context-ticket",
                "officer_admission": {"ticket_claim_source": "forged"},
                "ticket_ready_at": "2099-01-01T00:00:00Z",
                "ticket_claim_source": "forged",
                "pull_request": {"repo": "victim/private", "number": 9},
                "required_deliverables": ["kb:laundered"],
                "deliverable_contract_provenance": {"source": "forged"},
                "prior_deliverable_contract": ["repos/private/report.md"],
                "required_pr_repositories": ["victim/private"],
            },
        )
    finally:
        await client.close()

    (body,) = observed_bodies
    assert body["ticket"] == "backlog-tester-final-runtime-acceptance"
    assert body["work_category"] == "tester"
    assert body["context"]["officer_slot"] == "test"
    assert body["context"]["ordinary"] == "preserved"
    assert not set(body["context"]) & {
        "evidence_manifest",
        "ticket_note_id",
        "officer_admission",
        "ticket_ready_at",
        "ticket_claim_source",
        "pull_request",
        "required_deliverables",
        "deliverable_contract_provenance",
        "prior_deliverable_contract",
        "required_pr_repositories",
    }


@pytest.mark.asyncio
async def test_assign_job_preserves_queued_output_without_claiming_an_agent() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/jobs/job-1/assign/agent-1"
        return httpx.Response(200, json={"status": "queued", "reason": "provisioning"})

    client = AsyncCockpitClient(
        base_url="http://orchestrator.test",
        transport=httpx.MockTransport(handler),
    )
    invoke = make_bound_handler(
        get_descriptor("assign_job"),
        client_provider=lambda: client,
        caller_provider=lambda: CallerCtx(kind="mcp"),
    )
    try:
        output = await invoke(job_id="job-1", agent_id="agent-1")
    finally:
        await client.close()

    assert "Status: queued" in output
    assert "Agent:" not in output


def test_generated_job_surface_artifacts_are_current() -> None:
    completed = subprocess.run(
        [sys.executable, "scripts/generate-job-surface.py", "--check"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


def test_cockpit_generated_mirror_contains_every_canonical_name() -> None:
    source = (ROOT / "cockpit/src/app/core/tools/job-surface.generated.ts").read_text(
        encoding="utf-8"
    )
    for item in JOB_DESCRIPTORS:
        assert f'name: "{item.name}"' in source

    legacy = {
        "create_worker_job",
        "cancel_worker_job",
        "pause_worker_job",
        "resume_worker_job",
        "approve_worker_job",
        "steer_worker_job",
        "list_worker_jobs",
        "get_worker_job",
        "get_job_workspace_file",
        "list_job_workspace_files",
    }
    assert not any(name in source for name in legacy)


def test_stored_config_migration_covers_every_hard_rename_and_config_store() -> None:
    source = (
        ROOT
        / "src/orchestrator/database/migrations/app/0156_unified_job_tool_names.sql"
    ).read_text(encoding="utf-8")
    renames = {
        "create_worker_job": "create_job",
        "cancel_worker_job": "cancel_job",
        "pause_worker_job": "pause_job",
        "resume_worker_job": "resume_job_with_feedback",
        "approve_worker_job": "approve_job",
        "steer_worker_job": "steer_job",
        "list_worker_jobs": "list_jobs",
        "get_worker_job": "get_job",
        "get_job_workspace_file": "get_job_file",
        "list_job_workspace_files": "list_job_files",
    }
    for legacy, canonical in renames.items():
        assert f"WHEN '{legacy}' THEN '{canonical}'" in source
    # Missing ``tools`` must be a no-op. SQL's three-valued ``<>`` comparison
    # would otherwise fall through and turn a valid config into SQL NULL.
    assert "jsonb_typeof(config) IS DISTINCT FROM 'object'" in source
    assert "jsonb_typeof(config->'tools') IS DISTINCT FROM 'object'" in source
    assert "raw_name = 'approve_job'" in source
    assert "THEN 'approve_job_verdict'" in source
    for table in (
        "experts",
        "project_experts",
        "projects",
        "automations",
        "config_overrides",
        "threads",
        "jobs",
    ):
        assert f"UPDATE {table}" in source


def test_active_runtime_config_and_cockpit_have_no_legacy_tool_names() -> None:
    legacy = {
        "create_worker_job",
        "cancel_worker_job",
        "pause_worker_job",
        "resume_worker_job",
        "approve_worker_job",
        "steer_worker_job",
        "list_worker_jobs",
        "get_worker_job",
        "get_job_workspace_file",
        "list_job_workspace_files",
    }
    offenders: list[str] = []
    roots = (ROOT / "src" / "agent", ROOT / "config", ROOT / "cockpit/src")
    suffixes = {".py", ".yaml", ".yml", ".json", ".ts", ".html", ".md", ".txt"}
    for root in roots:
        for path in root.rglob("*"):
            if (
                not path.is_file()
                or path.suffix not in suffixes
                or path.name.endswith(".spec.ts")
            ):
                continue
            text = path.read_text(encoding="utf-8")
            for name in legacy:
                if name in text:
                    offenders.append(f"{path.relative_to(ROOT)}: {name}")
    assert offenders == []
