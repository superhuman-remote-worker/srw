"""Focused contracts for descriptor-backed LangChain job tools."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
import pytest

from shared.orch_surface.client import AsyncCockpitClient
from shared.runtime_actor import RUNTIME_ACTOR_HEADER, RuntimeActorContext
from agent.tools.context import ToolContext
from agent.tools.orchestrator import jobs as jobs_module
from agent.tools.orchestrator.jobs import create_orchestrator_tools


JOB_ID = "19707fa1-0000-4000-8000-000000000001"
PARENT_ID = "11111111-2222-3333-4444-555555555555"


def _tool(tools: list[Any], name: str) -> Any:
    return next(item for item in tools if item.name == name)


class Recorder:
    """Small HTTP backend that records the request contract under test."""

    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self.jobs: list[dict[str, Any]] = [
            {
                "id": JOB_ID,
                "status": "paused",
                "config_name": "worker_base",
                "created_at": "2026-08-14T08:00:00Z",
                "audit_count": 3,
            }
        ]

    async def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        path = request.url.path
        if request.method == "POST" and path == "/api/jobs":
            body = json.loads(request.content)
            return httpx.Response(
                201,
                json={
                    "id": JOB_ID,
                    "status": "created",
                    "description": body["description"],
                },
            )
        if request.method == "GET" and path == "/api/jobs":
            return httpx.Response(200, json=self.jobs)
        if request.method == "GET" and path == f"/api/jobs/{JOB_ID}":
            return httpx.Response(200, json=self.jobs[0])
        if request.method == "GET" and path.endswith("/repo/file"):
            return httpx.Response(
                200,
                json={"path": "plan.md", "content": "the plan", "size": 8},
            )
        if request.method == "GET" and path.endswith("/repo/contents"):
            return httpx.Response(
                200,
                json=[{"name": "plan.md", "type": "file", "size": 8}],
            )
        if request.method in {"PUT", "POST"}:
            return httpx.Response(
                200,
                json={"status": "ok", "delivery_strategy": "guidance_next_turn"},
            )
        raise AssertionError(f"unexpected request: {request.method} {request.url}")


def _install_surface_client(
    monkeypatch: pytest.MonkeyPatch,
    recorder: Recorder,
) -> AsyncCockpitClient:
    client = AsyncCockpitClient(
        base_url="http://orchestrator.test",
        transport=httpx.MockTransport(recorder),
    )
    monkeypatch.setattr(jobs_module, "_get_surface_client", lambda: client)
    return client


def _json_body(request: httpx.Request) -> dict[str, Any]:
    return json.loads(request.content)


@pytest.mark.asyncio
async def test_create_job_uses_hidden_trusted_lineage_without_scope_header(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MCP_INTERNAL_KEY", "internal-test-key")
    recorder = Recorder()
    client = _install_surface_client(monkeypatch, recorder)
    context = ToolContext(
        _thread_id="thread-1",
        _project_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        _job_metadata={"job_id": PARENT_ID},
        user_id="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
    )

    try:
        result = await _tool(create_orchestrator_tools(context), "create_job").ainvoke(
            {
                "description": "trusted child",
                # Extra model input must not override the hidden binding.
                "user_id": "dddddddd-dddd-dddd-dddd-dddddddddddd",
            }
        )
    finally:
        await client.close()

    request = recorder.requests[-1]
    body = _json_body(request)
    assert body["thread_id"] == "thread-1"
    assert body["parent_job_id"] == PARENT_ID
    # Omitted project_id falls back to the trusted lineage default.
    assert body["project_id"] == "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    assert body["user_id"] == "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
    assert request.headers["X-MCP-User-Id"] == body["user_id"]
    # The session/agent lane never sends X-MCP-Scope: stamping it activated
    # server-side project fencing this lane never had (NULL-project jobs
    # vanished from list_jobs and 403'd on get_job). Officer-lane scoping is
    # reintroduced deliberately by officer_supervision_surface E2.
    assert "X-MCP-Scope" not in request.headers
    assert request.headers["X-Internal-Key"] == "internal-test-key"
    assert "Job created successfully" in result


@pytest.mark.asyncio
async def test_create_job_explicit_project_id_wins_over_lineage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorder = Recorder()
    client = _install_surface_client(monkeypatch, recorder)
    context = ToolContext(
        _thread_id="thread-1",
        _project_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        user_id="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
    )

    try:
        await _tool(create_orchestrator_tools(context), "create_job").ainvoke(
            {
                "description": "explicit project target",
                "project_id": "cccccccc-cccc-cccc-cccc-cccccccccccc",
            }
        )
    finally:
        await client.close()

    body = _json_body(recorder.requests[-1])
    # Explicit project_id reaches the API body; server-side membership
    # validation decides whether it is accepted.
    assert body["project_id"] == "cccccccc-cccc-cccc-cccc-cccccccccccc"
    assert body["user_id"] == "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"


def _officer_actor(project_id: str) -> RuntimeActorContext:
    return RuntimeActorContext(
        caller_kind="officer",
        project_id=project_id,
        project_role="owner",
        thread_id="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
        officer_incarnation=2,
        user_id="user-1",
        access_credential="sra_abcdefghijklmnopqrstuvwxyzABCDEFG123456789",
        refresh_credential="srr_abcdefghijklmnopqrstuvwxyzABCDEFG123456789",
        access_expires_at=datetime.now(timezone.utc) + timedelta(minutes=4),
        refresh_expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
    )


def _worker_actor(user_id: str) -> RuntimeActorContext:
    return RuntimeActorContext(
        caller_kind="worker",
        user_id=user_id,
        access_credential="sra_abcdefghijklmnopqrstuvwxyzABCDEFG123456789",
    )


def test_caller_ctx_detects_officer_only_from_runtime_actor() -> None:
    """Parsed config may shape tools, but it can never mint actor authority."""
    context = ToolContext(
        user_id="user-1",
        config={"officer": {"enabled": True}},  # not the runtime fact
    )
    assert jobs_module._caller_ctx(context).kind == "session"

    officer_context = ToolContext(
        user_id="user-1",
        config={"officer_session": True},
        runtime_actor=_officer_actor("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
    )
    assert jobs_module._caller_ctx(officer_context).kind == "officer"

    config_only_context = ToolContext(
        user_id="user-1",
        config={"officer_session": True},
    )
    assert jobs_module._caller_ctx(config_only_context).kind == "session"


@pytest.mark.asyncio
async def test_worker_runtime_actor_forwards_job_owner_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Lifecycle workers authenticate guarded job reads as the owning user."""
    monkeypatch.setenv("MCP_INTERNAL_KEY", "internal-test-key")
    recorder = Recorder()
    client = _install_surface_client(monkeypatch, recorder)
    context = ToolContext(
        runtime_actor=_worker_actor("user-1"),
        _job_metadata={"job_id": JOB_ID},
    )

    try:
        await _tool(create_orchestrator_tools(context), "get_job").ainvoke(
            {"job_id": JOB_ID}
        )
    finally:
        await client.close()

    request = recorder.requests[-1]
    assert request.headers["X-MCP-User-Id"] == "user-1"
    assert request.headers[RUNTIME_ACTOR_HEADER].startswith("sra_")
    assert request.headers["X-Internal-Key"] == "internal-test-key"


@pytest.mark.asyncio
async def test_officer_lane_stamps_project_scope_and_fails_closed_unbound(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """E2 scope split: officer calls carry X-MCP-Scope: project:<uuid>;
    an officer bound to zero (or many) projects gets a binding error, never
    an unscoped fleet view. The plain session lane still sends no scope."""
    recorder = Recorder()
    client = _install_surface_client(monkeypatch, recorder)
    project_id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    officer_ctx = ToolContext(
        user_id="user-1",
        _project_id=project_id,
        _project_ids=[project_id],
        config={"officer_session": True},
        runtime_actor=_officer_actor(project_id),
    )
    try:
        await _tool(create_orchestrator_tools(officer_ctx), "list_jobs").ainvoke({})
        scoped_request = recorder.requests[-1]

        unbound_ctx = ToolContext(
            user_id="user-1",
            config={"officer_session": True},
            runtime_actor=_officer_actor(project_id),
        )
        refusal = await _tool(
            create_orchestrator_tools(unbound_ctx), "list_jobs"
        ).ainvoke({})

        session_ctx = ToolContext(
            user_id="user-1",
            _project_id=project_id,
            _project_ids=[project_id],
        )
        await _tool(create_orchestrator_tools(session_ctx), "list_jobs").ainvoke({})
        session_request = recorder.requests[-1]
    finally:
        await client.close()

    assert scoped_request.headers["X-MCP-Scope"] == f"project:{project_id}"
    assert scoped_request.headers[RUNTIME_ACTOR_HEADER].startswith("sra_")
    assert "Officer project binding error" in refusal
    assert "X-MCP-Scope" not in session_request.headers


@pytest.mark.asyncio
async def test_create_job_always_asks_for_the_projects_connector_defaults(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The dispatch plane does not name connectors — it cannot.

    ``datasource_ids`` was a parameter here, tri-state and correctly documented.
    The only thing it ever did in anger was let an officer pass ``[]`` — which
    means "attach none" — because the schema advertised an array and the empty
    one reads as the neutral value. His workers came up with no repository
    checkout and the century idled for a night.

    The callers of this plane are dispatchers (officer, session) with no basis
    for connector surgery, so the lever is gone rather than merely discouraged:
    a model cannot mis-set what it is never shown. Narrowing still exists on the
    surfaces where a human reviews the choice — MCP, REST, cockpit.
    """
    recorder = Recorder()
    client = _install_surface_client(monkeypatch, recorder)
    tool = _tool(create_orchestrator_tools(ToolContext()), "create_job")
    try:
        await tool.ainvoke({"description": "connector contract"})
    finally:
        await client.close()

    body = _json_body(recorder.requests[-1])
    assert body["use_datasource_defaults"] is True
    assert "datasource_ids" not in body
    assert "datasource_ids" not in tool.args_schema.model_json_schema()["properties"]


@pytest.mark.asyncio
async def test_create_job_slot_wins_without_repurposing_kickoff_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorder = Recorder()
    client = _install_surface_client(monkeypatch, recorder)
    try:
        await _tool(create_orchestrator_tools(ToolContext()), "create_job").ainvoke(
            {
                "description": "brief lanes",
                "kickoff_message": "safe opening brief",
                "context": {"officer_slot": "old-slot", "trace": "kept"},
                "slot": "researcher",
            }
        )
    finally:
        await client.close()

    body = _json_body(recorder.requests[-1])
    assert body["kickoff_message"] == "safe opening brief"
    assert "instructions" not in body
    assert body["context"] == {"officer_slot": "researcher", "trace": "kept"}
    assert "slot" not in body


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "config_override",
    [
        {"llm": {"api_key": "nope"}},
        {"llm": {"provider_api_key": "nope"}},
        {"transport": {"base_url": "https://example.invalid"}},
        {"env_keys": ["SECRET"]},
    ],
)
async def test_create_job_rejects_forbidden_override_keys(
    monkeypatch: pytest.MonkeyPatch,
    config_override: dict[str, Any],
) -> None:
    recorder = Recorder()
    client = _install_surface_client(monkeypatch, recorder)
    try:
        result = await _tool(
            create_orchestrator_tools(ToolContext()), "create_job"
        ).ainvoke({"description": "unsafe", "config_override": config_override})
    finally:
        await client.close()

    assert result.startswith("Refusing to create job")
    assert recorder.requests == []


@pytest.mark.asyncio
async def test_create_job_preserves_expert_config_validation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorder = Recorder()
    client = _install_surface_client(monkeypatch, recorder)
    try:
        result = await _tool(
            create_orchestrator_tools(ToolContext()), "create_job"
        ).ainvoke(
            {
                "description": "invalid selector pair",
                "expert_id": "expert-1",
                "config_name": "developer",
            }
        )
    finally:
        await client.close()

    assert "expert_id cannot be combined" in result
    assert recorder.requests == []


@pytest.mark.asyncio
async def test_unique_job_prefix_resolution_is_preserved_for_agent_tools(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorder = Recorder()
    client = _install_surface_client(monkeypatch, recorder)
    try:
        result = await _tool(
            create_orchestrator_tools(ToolContext(user_id="user-1")), "get_job"
        ).ainvoke({"job_id": "19707fa1..."})
    finally:
        await client.close()

    assert [request.url.path for request in recorder.requests] == [
        "/api/jobs",
        f"/api/jobs/{JOB_ID}",
    ]
    assert f"Job: {JOB_ID}" in result


@pytest.mark.asyncio
async def test_job_lifecycle_routes_and_steer_are_non_destructive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorder = Recorder()
    client = _install_surface_client(monkeypatch, recorder)
    tools = create_orchestrator_tools(ToolContext(user_id="user-1"))
    try:
        await _tool(tools, "pause_job").ainvoke({"job_id": JOB_ID})
        await _tool(tools, "cancel_job").ainvoke({"job_id": JOB_ID})
        result = await _tool(tools, "steer_job").ainvoke(
            {"job_id": JOB_ID, "message": "keep the plan", "urgent": True}
        )
    finally:
        await client.close()

    assert [(request.method, request.url.path) for request in recorder.requests] == [
        ("PUT", f"/api/jobs/{JOB_ID}/pause"),
        ("PUT", f"/api/jobs/{JOB_ID}/cancel"),
        ("POST", f"/api/jobs/{JOB_ID}/messages/officer/reply"),
    ]
    assert _json_body(recorder.requests[-1]) == {
        "message": "keep the plan",
        "urgent": True,
    }
    assert "guidance_next_turn" in result


@pytest.mark.asyncio
async def test_canonical_file_tools_use_shared_mcp_schema_and_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorder = Recorder()
    client = _install_surface_client(monkeypatch, recorder)
    tools = create_orchestrator_tools(ToolContext(user_id="user-1"))
    try:
        file_result = await _tool(tools, "get_job_file").ainvoke(
            {"job_id": JOB_ID, "file_path": "plan.md"}
        )
        list_result = await _tool(tools, "list_job_files").ainvoke(
            {"job_id": JOB_ID, "path": ""}
        )
    finally:
        await client.close()

    assert "File: plan.md (ref: HEAD, 8 bytes)" in file_result
    assert "[file] plan.md" in list_result


def test_create_job_schema_has_no_model_selectable_lineage() -> None:
    schema = _tool(
        create_orchestrator_tools(ToolContext()), "create_job"
    ).args_schema.model_json_schema()
    fields = set(schema["properties"])
    assert fields == {
        "description",
        # One selector for the whole expert catalogue (bundled slug or DB
        # UUID); config_name/expert_id remain as deprecated single-store
        # aliases. tests/test_unified_expert_selection.py owns the contract.
        "expert",
        "config_name",
        "expert_id",
        # datasource_ids is deliberately absent: connectors are resolved
        # server-side from the project's auto-attach defaults. See
        # test_create_job_always_asks_for_the_projects_connector_defaults.
        "instructions",
        "kickoff_message",
        "config_override",
        "context",
        # project_id is deliberately model-visible (the old agent-lane
        # create_worker_job had it); explicit wins over the lineage default.
        "project_id",
        "priority",
        "required_deliverables",
        "slot",
        "ticket",
        "work_category",
        # workspace is model-visible because the shared descriptor also
        # builds the MCP create_job (schema parity). Child jobs inherit the
        # parent's workspace; the server refuses it on them. See
        # tests/test_mcp_workspace_choice.py.
        "workspace",
    }
    assert not {"user_id", "thread_id", "parent_job_id"} & fields


@pytest.mark.asyncio
async def test_get_session_context_remains_available(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class DiscoveryClient:
        async def __aenter__(self) -> DiscoveryClient:
            return self

        async def __aexit__(self, *args: Any) -> None:
            return None

        async def get(self, url: str) -> httpx.Response:
            request = httpx.Request("GET", url)
            if url.endswith("/api/models"):
                return httpx.Response(
                    200,
                    request=request,
                    json={"groups": [{"models": ["gpt-5.6"]}]},
                )
            return httpx.Response(
                200,
                request=request,
                json={"is_admin": True, "grants": None},
            )

    monkeypatch.setattr(jobs_module, "_get_client", lambda **_: DiscoveryClient())
    context = ToolContext(
        _thread_id="thread-1",
        _project_id="project-1",
        user_id="user-1",
    )
    result = await _tool(
        create_orchestrator_tools(context), "get_session_context"
    ).ainvoke({})

    assert "Thread ID: thread-1" in result
    assert "Primary project ID: project-1" in result
    assert "Available chat models: gpt-5.6" in result
    assert "admin (unrestricted)" in result
