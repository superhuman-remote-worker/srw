"""HTTP characterization for the complete inspection/diagnostics extraction.

These cases pin sequencing and privacy at the mounted boundary. Existing
evidence, liveness, roster and access suites exercise the underlying policies.
"""

import asyncio
import copy
import json
import os
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import httpx
import pytest
from fastapi import HTTPException

from orchestrator.routers import diagnostics as diagnostics_routes
from orchestrator.routers import job_artifacts as artifacts_routes
from orchestrator.routers import job_audit as audit_routes
from orchestrator.routers import job_inspection as inspection_routes
from orchestrator.services import diagnostics, job_artifacts, job_inspection
from tests._mounted_router import mount_router


JOB = "33333333-3333-4333-8333-333333333333"
USER = "11111111-1111-4111-8111-111111111111"
PROJECT = "22222222-2222-4222-8222-222222222222"
THREAD = "44444444-4444-4444-8444-444444444444"
STAMP = datetime(2026, 9, 7, 8, tzinfo=timezone.utc)
JOB_PATH = f"/api/jobs/{JOB}"
PATHS = (
    "/api/health",
    "/debug/emails",
    "/debug/emails/{name}",
    "/api/workspace/status",
    "/api/jobs/{job_id}/progress",
    "/api/jobs/{job_id}/evidence",
    "/api/jobs/{job_id}/completion-report",
    "/api/jobs/{job_id}/evidence/{evidence_id}",
    "/api/jobs/{job_id}/subjobs",
    "/api/jobs/{job_id}/subagents",
    "/api/persistent/threads/{thread_id}/subagents",
    "/api/jobs/{job_id}/audit",
    "/api/jobs/{job_id}/audit/step/{step_id}",
    "/api/requests/{doc_id}",
    "/api/jobs/{job_id}/audit/timerange",
    "/api/jobs/{job_id}/chat",
    "/api/jobs/{job_id}/chat/entry/{entry_id}",
    "/api/jobs/{job_id}/todos",
    "/api/jobs/{job_id}/todos/current",
    "/api/jobs/{job_id}/todos/archives",
    "/api/jobs/{job_id}/todos/archives/{filename}",
    "/api/jobs/{job_id}/version",
    "/api/jobs/{job_id}/brief",
    "/api/me/active-jobs",
)


@pytest.fixture
def inspection_wire(tmp_path):
    return _inspection_wire(tmp_path)


def _inspection_wire(tmp_path):
    from orchestrator.services import job_evidence

    tmp_path.mkdir(exist_ok=True)
    wire = SimpleNamespace(
        events=[],
        denied=False,
        user={"id": USER, "is_admin": False, "is_approved": True},
        job={"id": UUID(JOB), "status": "completed", "context": {}},
        parent={"id": UUID(THREAD), "kind": "session"},
        scope=None,
    )

    async def guard(name, result):
        wire.events.append(name)
        if wire.denied:
            raise HTTPException(403, "synthetic refusal")
        return result

    async def approved(request, db):
        return await guard("approved", wire.user)

    async def job_access(request, db, job_id):
        return await guard("job_auth", (wire.user, copy.deepcopy(wire.job)))

    async def thread_owner(request, db, thread_id):
        return await guard("thread_auth", (wire.user, copy.deepcopy(wire.parent)))

    async def internal(request):
        return await guard("internal", None)

    async def admin(request):
        return await guard("admin", wire.user)

    def reader(name, result):
        async def read(*args, **kwargs):
            wire.events.append(name)
            return copy.deepcopy(result)

        return AsyncMock(side_effect=read)

    wire.db = SimpleNamespace(
        get_job=reader("get_job", wire.job),
        get_job_progress=reader(
            "progress",
            {
                "status": "completed",
                "extension": 7,
                "progress_percent": None,
                "eta_seconds": None,
            },
        ),
        get_agent=reader("agent", None),
        get_job_subjob_roster=reader("subjobs", []),
        list_subagent_threads=reader("job_roster", []),
        list_session_subagent_threads=reader("session_roster", []),
        query_jobs=reader("query", SimpleNamespace(jobs=[])),
    )
    wire.audit = SimpleNamespace(
        is_available=False,
        get_job_audit=reader("audit", {"entries": [], "extension": UUID(JOB)}),
        get_audit_step=reader("audit_step", None),
        get_request=reader("request", None),
        get_audit_time_range=reader("timerange", None),
        get_chat_history=reader("chat", {"entries": [], "extension": STAMP}),
        get_chat_entry=reader("chat_entry", None),
        get_job_version=reader("version", None),
    )
    wire.forge = SimpleNamespace(
        is_initialized=False,
        list_contents=reader("contents", None),
        get_file_content=reader("file", None),
    )
    wire.resolve_repo = reader("repo", ("synthetic-repo", "output"))
    wire.visible = reader("visible", [UUID(PROJECT)])
    wire.workspace = SimpleNamespace(base_path=tmp_path, is_available=True)
    wire.email = SimpleNamespace(
        _build_system_notification_html=Mock(return_value="<html>system</html>"),
        _build_agent_message_html=Mock(return_value="<html>agent</html>"),
        render_notification_html=Mock(return_value="<html>permission</html>"),
    )
    wire.guards = SimpleNamespace(
        approved=approved,
        job_access=job_access,
        thread_owner=thread_owner,
        internal=internal,
        admin=admin,
    )
    wire.getenv = os.getenv
    wire.evidence = job_evidence
    wire.factories = {
        "job_inspection_dependencies_factory": lambda: inspection_routes.JobInspectionDependencies(
            store=wire.db,
            inspections=job_inspection.JobInspectionDependencies(
                store=wire.db,
                audit_reader=wire.audit,
                user_visible_project_ids=wire.visible,
                mcp_scope_project_id=lambda _user: wire.scope,
                active_job_statuses=job_inspection.ME_ACTIVE_JOB_STATUSES,
            ),
            require_approved_user=wire.guards.approved,
            require_job_access=wire.guards.job_access,
            require_thread_owner=wire.guards.thread_owner,
            require_internal=wire.guards.internal,
        ),
        "job_audit_dependencies_factory": lambda: audit_routes.JobAuditDependencies(
            store=wire.db,
            audit_reader=wire.audit,
            require_admin=wire.guards.admin,
            require_approved_user=wire.guards.approved,
            require_job_access=wire.guards.job_access,
        ),
        "job_artifacts_dependencies_factory": lambda: artifacts_routes.JobArtifactDependencies(
            store=wire.db,
            artifacts=job_artifacts.JobArtifactDependencies(
                store=wire.db,
                forge=wire.forge,
                resolve_job_repo=wire.resolve_repo,
                evidence=wire.evidence,
            ),
            require_job_access=wire.guards.job_access,
        ),
        "diagnostics_dependencies_factory": lambda: diagnostics_routes.DiagnosticsDependencies(
            operations=diagnostics.DiagnosticDependencies(
                workspace=wire.workspace, email_renderer=wire.email, getenv=wire.getenv
            ),
            require_admin=wire.guards.admin,
        ),
    }
    wire.app = mount_router(
        inspection_routes.router,
        audit_routes.router,
        artifacts_routes.router,
        diagnostics_routes.router,
        factories=wire.factories,
    )
    return wire


async def get(wire, path, **kwargs):
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=wire.app), base_url="http://inspection.test"
    ) as client:
        return await client.get(path, **kwargs)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "suffix",
    [
        "progress",
        "evidence",
        "completion-report",
        "evidence/opaque",
        "subjobs",
        "subagents",
        "audit",
        "audit/step/1",
        "audit/timerange",
        "chat",
        "chat/entry/1",
        "todos",
        "todos/current",
        "todos/archives",
        "todos/archives/todos_phase.md",
        "version",
    ],
)
async def test_job_authorization_precedes_every_inspection_reader(
    inspection_wire, suffix
):
    inspection_wire.denied = True
    response = await get(inspection_wire, f"{JOB_PATH}/{suffix}")
    assert response.status_code == 403
    assert inspection_wire.events == ["job_auth"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path,gate",
    [
        (f"/api/persistent/threads/{THREAD}/subagents", "thread_auth"),
        (f"{JOB_PATH}/brief", "internal"),
        ("/api/me/active-jobs", "approved"),
        ("/api/workspace/status", "admin"),
    ],
)
async def test_distinct_non_job_guards(inspection_wire, path, gate):
    inspection_wire.denied = True
    response = await get(inspection_wire, path)
    assert response.status_code == 403
    assert inspection_wire.events == [gate]


@pytest.mark.asyncio
@pytest.mark.parametrize("suffix", ["audit", "chat"])
async def test_unavailable_lists_keep_legacy_and_rest_pagination(
    inspection_wire, suffix
):
    response = await get(
        inspection_wire,
        f"{JOB_PATH}/{suffix}",
        params={"page": -1, "pageSize": 17, "offset": 23, "limit": 9},
    )
    assert response.status_code == 200
    assert response.json() == {
        "entries": [],
        "total": 0,
        "page": -1,
        "pageSize": 9,
        "offset": 23,
        "limit": 9,
        "hasMore": False,
        "error": "Audit store not available",
    }
    assert inspection_wire.events == ["job_auth"]


@pytest.mark.asyncio
@pytest.mark.parametrize("suffix", ["audit/timerange", "version"])
async def test_optional_audit_metadata_is_json_null(inspection_wire, suffix):
    response = await get(inspection_wire, f"{JOB_PATH}/{suffix}")
    assert response.status_code == 200
    assert response.content == b"null"
    assert inspection_wire.events == ["job_auth"]


@pytest.mark.asyncio
@pytest.mark.parametrize("suffix", ["audit/step/1", "chat/entry/1"])
async def test_detail_audit_outage_is_503_then_missing_entry_is_404(
    inspection_wire, suffix
):
    response = await get(inspection_wire, f"{JOB_PATH}/{suffix}")
    assert response.status_code == 503
    assert inspection_wire.events == ["job_auth"]
    inspection_wire.audit.is_available = True
    response = await get(inspection_wire, f"{JOB_PATH}/{suffix}")
    assert response.status_code == 404


@pytest.mark.asyncio
@pytest.mark.parametrize("suffix", ["audit", "chat"])
async def test_query_validation_still_precedes_auth(inspection_wire, suffix):
    inspection_wire.denied = True
    response = await get(
        inspection_wire, f"{JOB_PATH}/{suffix}", params={"pageSize": 201}
    )
    assert response.status_code == 422
    assert inspection_wire.events == []


@pytest.mark.asyncio
async def test_audit_reader_receives_both_pagination_styles_and_raw_extensions(
    inspection_wire,
):
    inspection_wire.audit.is_available = True
    response = await get(
        inspection_wire,
        f"{JOB_PATH}/audit",
        params={
            "page": -1,
            "pageSize": 17,
            "offset": 23,
            "limit": 9,
            "order": "desc",
            "filter": "errors",
            "lean": "true",
        },
    )
    assert response.status_code == 200
    assert response.json()["extension"] == JOB
    inspection_wire.audit.get_job_audit.assert_awaited_once_with(
        job_id=JOB,
        page=-1,
        page_size=17,
        offset=23,
        limit=9,
        order="desc",
        filter_category="errors",
        lean=True,
    )


@pytest.mark.asyncio
async def test_request_outage_is_reported_before_any_authorization(inspection_wire):
    inspection_wire.denied = True
    response = await get(inspection_wire, "/api/requests/opaque")
    assert response.status_code == 503
    assert inspection_wire.events == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "document,gate,status",
    [
        (None, "approved", 404),
        ({"job_id": JOB}, "job_auth", 200),
        ({"job_id": None, "extension": 7}, "admin", 200),
    ],
)
@pytest.mark.parametrize("denied", [False, True])
async def test_request_reads_then_authorizes_its_own_scope(
    inspection_wire, document, gate, status, denied
):
    wire = inspection_wire
    wire.audit.is_available = True
    wire.denied = denied

    async def read(doc_id):
        wire.events.append("request")
        return document

    wire.audit.get_request.side_effect = read
    response = await get(wire, "/api/requests/opaque")
    assert response.status_code == (403 if denied else status)
    assert wire.events == ["request", gate]
    if response.status_code == 200:
        assert response.json() == document


@pytest.mark.asyncio
async def test_request_reader_failure_is_500_without_later_auth(inspection_wire):
    inspection_wire.audit.is_available = True
    inspection_wire.audit.get_request.side_effect = RuntimeError(
        "synthetic unavailable"
    )
    response = await get(inspection_wire, "/api/requests/opaque")
    assert response.status_code == 500
    assert response.json() == {"detail": "synthetic unavailable"}
    assert inspection_wire.events == []


@pytest.mark.asyncio
async def test_progress_preserves_extensions_and_honest_terminal_liveness(
    inspection_wire,
):
    response = await get(inspection_wire, f"{JOB_PATH}/progress")
    assert response.status_code == 200
    payload = response.json()
    assert payload["extension"] == 7
    assert payload["state"] == "terminal"
    assert payload["progress_percent"] is None
    assert payload["eta_seconds"] is None
    assert inspection_wire.events[:2] == ["job_auth", "progress"]


@pytest.mark.asyncio
async def test_session_roster_rejects_non_session_before_store_read(inspection_wire):
    inspection_wire.parent["kind"] = "subagent"
    response = await get(inspection_wire, f"/api/persistent/threads/{THREAD}/subagents")
    assert response.status_code == 404
    assert response.json() == {"detail": "Parent session not found"}
    assert inspection_wire.events == ["thread_auth"]


@pytest.mark.asyncio
@pytest.mark.parametrize("as_text", [False, True])
async def test_both_rosters_publish_one_sanitized_spawn_shape(inspection_wire, as_text):
    metadata = {
        "private": "synthetic-secret",
        "subagent": {"brief_description": "child", "owned_paths": ["report.md"]},
    }
    row = {
        "id": UUID(THREAD),
        "parent_job_id": UUID(JOB),
        "parent_thread_id": UUID(THREAD),
        "metadata": json.dumps(metadata) if as_text else metadata,
        "status": "ended",
        "subagent_status": "completed",
        "total_turns": "3",
        "runtime_generation": 0,
        "recovery_kind": "settled",
        "created_at": STAMP,
    }
    inspection_wire.db.list_subagent_threads.side_effect = None
    inspection_wire.db.list_subagent_threads.return_value = [row]
    inspection_wire.db.list_session_subagent_threads.side_effect = None
    inspection_wire.db.list_session_subagent_threads.return_value = [row]
    job = await get(inspection_wire, f"{JOB_PATH}/subagents")
    session = await get(inspection_wire, f"/api/persistent/threads/{THREAD}/subagents")
    assert job.status_code == session.status_code == 200
    assert job.json()["job_id"] == JOB
    assert session.json()["parent_thread_id"] == THREAD
    assert job.json()["subagents"] == session.json()["subagents"]
    child = job.json()["subagents"][0]
    assert child["description"] == "child"
    assert child["turns"] == 3
    assert child["runtime_generation"] == "0"
    assert child["recovery_kind"] == "settled"
    assert "metadata" not in child
    assert "synthetic-secret" not in job.text


@pytest.mark.asyncio
async def test_evidence_exception_never_discloses_private_coordinates(
    inspection_wire, monkeypatch
):
    from orchestrator.services import job_evidence

    monkeypatch.setattr(
        job_evidence, "parse_manifest", lambda _job: {"entries": [{"id": "opaque"}]}
    )
    monkeypatch.setattr(
        job_evidence, "find_entry", lambda manifest, entry_id: manifest["entries"][0]
    )
    read = AsyncMock(side_effect=RuntimeError("synthetic-private-repository/token"))
    monkeypatch.setattr(job_evidence, "read_evidence_entry", read)
    response = await get(
        inspection_wire, f"{JOB_PATH}/evidence/opaque", params={"offset": 19}
    )
    assert response.status_code == 500
    assert response.json() == {
        "detail": "Evidence read failed without exposing private object details"
    }
    assert "synthetic-private" not in response.text
    assert read.await_args.kwargs["offset"] == 19
    assert read.await_args.kwargs["gitea"] is inspection_wire.forge


@pytest.mark.asyncio
async def test_todo_outage_has_distinct_summary_current_and_archive_shapes(
    inspection_wire,
):
    summary = await get(inspection_wire, f"{JOB_PATH}/todos")
    assert summary.json() == {
        "job_id": JOB,
        "current": None,
        "archives": [],
        "has_workspace": False,
    }
    current = await get(inspection_wire, f"{JOB_PATH}/todos/current")
    assert current.status_code == 404
    archives = await get(inspection_wire, f"{JOB_PATH}/todos/archives")
    assert archives.json() == []
    inspection_wire.resolve_repo.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("filename", ["..private.md", "folder\\private.md"])
async def test_archive_path_refusal_precedes_forge_read(inspection_wire, filename):
    inspection_wire.forge.is_initialized = True
    response = await get(inspection_wire, f"{JOB_PATH}/todos/archives/{filename}")
    assert response.status_code == 404
    assert inspection_wire.events == ["job_auth"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "context",
    [{"required_deliverables": ["answer.md"]}, '{"kickoff_message":"go"}', "malformed"],
)
async def test_internal_brief_retains_jsonb_and_missing_defaults(
    inspection_wire, context
):
    inspection_wire.db.get_job.side_effect = None
    inspection_wire.db.get_job.return_value = {"description": None, "context": context}
    response = await get(inspection_wire, f"{JOB_PATH}/brief")
    assert response.status_code == 200
    payload = response.json()
    assert payload["description"] == ""
    assert set(payload) == {"description", "required_deliverables", "kickoff_message"}
    assert inspection_wire.events == ["internal"]


@pytest.mark.asyncio
@pytest.mark.parametrize("admin", [False, True])
async def test_active_jobs_keeps_admin_own_only_and_nonadmin_visibility_or(
    inspection_wire, admin
):
    wire = inspection_wire
    wire.user["is_admin"] = admin
    wire.scope = UUID(PROJECT)
    wire.db.query_jobs.side_effect = None
    wire.db.query_jobs.return_value = SimpleNamespace(
        jobs=[{"status": "paused"}, {"status": "completed"}]
    )
    response = await get(wire, "/api/me/active-jobs", params={"limit": 7})
    assert response.status_code == 200
    assert response.json() == [{"status": "paused"}]
    wire.db.query_jobs.assert_awaited_once_with(
        owner_user_id=None if admin else USER,
        visible_project_ids=None if admin else [PROJECT],
        scope_project_id=PROJECT,
        statuses=["created", "paused", "pending_review", "processing"],
        user_id=USER if admin else None,
        limit=7,
        include_total=False,
    )
    assert wire.visible.await_count == (0 if admin else 1)


@pytest.mark.asyncio
async def test_health_does_not_need_authentication(inspection_wire):
    inspection_wire.denied = True
    response = await get(inspection_wire, "/api/health")
    assert response.json() == {"status": "ok"}
    assert inspection_wire.events == []


@pytest.mark.asyncio
async def test_workspace_diagnostic_stays_admin_and_limits_listing(
    inspection_wire, monkeypatch
):
    monkeypatch.setenv("WORKSPACE_PATH", "synthetic-configured-path")
    for number in range(25):
        (inspection_wire.workspace.base_path / str(number)).touch()
    response = await get(inspection_wire, "/api/workspace/status")
    assert response.status_code == 200
    assert len(response.json()["entries"]) == 20
    assert response.json()["env_workspace_path"] == "synthetic-configured-path"
    assert inspection_wire.events == ["admin"]


@pytest.mark.asyncio
@pytest.mark.parametrize("name", ["system", "agent", "permission", "unknown"])
async def test_disabled_email_preview_is_indistinguishable_from_absent_route(
    inspection_wire, monkeypatch, name
):
    monkeypatch.delenv("EMAIL_PREVIEW_ENABLED", raising=False)
    response = await get(inspection_wire, f"/debug/emails/{name}")
    missing = await get(inspection_wire, "/missing-route")
    assert response.status_code == missing.status_code == 404
    assert response.json() == missing.json()
    assert inspection_wire.events == []


@pytest.mark.asyncio
@pytest.mark.parametrize("name", ["system", "agent", "permission"])
async def test_enabled_email_preview_uses_real_renderer_port_and_html_response(
    inspection_wire, monkeypatch, name
):
    monkeypatch.setenv("EMAIL_PREVIEW_ENABLED", " YES ")
    response = await get(inspection_wire, f"/debug/emails/{name}")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    assert response.text == f"<html>{name}</html>"


@pytest.mark.asyncio
async def test_request_availability_is_read_once_outside_error_translation(
    inspection_wire,
):
    class BrokenAvailability:
        reads = 0

        @property
        def is_available(self):
            self.reads += 1
            raise RuntimeError("synthetic availability failure")

    reader = BrokenAvailability()
    inspection_wire.audit = reader
    with pytest.raises(RuntimeError, match="synthetic availability failure"):
        await get(inspection_wire, "/api/requests/opaque")
    assert reader.reads == 1
    assert inspection_wire.events == []


@pytest.mark.asyncio
async def test_request_unexpected_auth_failure_retains_existing_500_translation(
    inspection_wire,
):
    inspection_wire.audit.is_available = True
    inspection_wire.audit.get_request.side_effect = None
    inspection_wire.audit.get_request.return_value = None
    inspection_wire.guards.approved = AsyncMock(
        side_effect=RuntimeError("synthetic auth failure")
    )
    response = await get(inspection_wire, "/api/requests/opaque")
    assert response.status_code == 500
    assert response.json() == {"detail": "synthetic auth failure"}


@pytest.mark.asyncio
async def test_four_router_factories_stay_with_their_own_app_under_concurrent_reads(
    tmp_path, monkeypatch
):
    from orchestrator import main

    first = _inspection_wire(tmp_path / "first")
    second = _inspection_wire(tmp_path / "second")
    for label, wire in [("first", first), ("second", second)]:
        wire.audit.is_available = True
        wire.audit.get_job_audit.side_effect = None
        wire.audit.get_job_audit.return_value = {"owner": label}
        wire.db.query_jobs.side_effect = None
        wire.db.query_jobs.return_value = SimpleNamespace(
            jobs=[{"status": "paused", "owner": label}]
        )
        wire.getenv = lambda name, default=None: "true"
        wire.email._build_system_notification_html.return_value = (
            f"<html>{label}</html>"
        )
    second.forge.is_initialized = True
    second.forge.list_contents.side_effect = None
    second.forge.list_contents.return_value = []

    from orchestrator.services import email as email_module
    from orchestrator.services import workspace as workspace_module

    class ForbiddenApplicationCollaborator:
        def __getattr__(self, name):
            raise AssertionError(f"router used application collaborator: {name}")

    # The default application's resources (the former main globals) and the
    # process-wide singletons on their owning modules.
    for owner, name in (
        (main.app.state.resources, "postgres_db"),
        (main.app.state.resources, "audit_reader"),
        (workspace_module, "workspace_service"),
        (email_module, "email_service"),
        (main.app.state.resources, "gitea_client"),
    ):
        monkeypatch.setattr(owner, name, ForbiddenApplicationCollaborator())
    paths = (
        "/api/me/active-jobs",
        f"{JOB_PATH}/audit",
        f"{JOB_PATH}/todos",
        "/debug/emails/system",
    )
    responses = await asyncio.gather(
        *(get(wire, path) for wire in (first, second) for path in paths)
    )
    assert all(response.status_code == 200 for response in responses)
    assert responses[0].json()[0]["owner"] == responses[1].json()["owner"] == "first"
    assert responses[4].json()[0]["owner"] == responses[5].json()["owner"] == "second"
    assert responses[2].json()["has_workspace"] is False
    assert responses[6].json()["has_workspace"] is True
    assert responses[3].text == "<html>first</html>"
    assert responses[7].text == "<html>second</html>"


@pytest.mark.asyncio
async def test_all_evidence_reads_use_only_the_app_evidence_operations(
    tmp_path, monkeypatch
):
    from orchestrator.services import job_evidence

    first = _inspection_wire(tmp_path / "first")
    second = _inspection_wire(tmp_path / "second")
    for label, wire in [("first", first), ("second", second)]:
        entry = {
            "id": "opaque",
            "kind": "completion_report",
            "inline_content": json.dumps({"owner": label}),
            "source": {"revision": f"{label}-revision"},
        }
        wire.evidence = SimpleNamespace(
            parse_manifest=Mock(
                return_value={"recorded_at": label, "entries": [entry]}
            ),
            public_manifest=Mock(return_value={"owner": label}),
            find_entry=Mock(return_value=entry),
            read_evidence_entry=AsyncMock(return_value={"owner": label}),
        )
    monkeypatch.setattr(
        job_evidence,
        "parse_manifest",
        Mock(side_effect=AssertionError("global evidence fallback")),
    )
    paths = ("evidence", "completion-report", "evidence/opaque")
    responses = await asyncio.gather(
        *(get(wire, f"{JOB_PATH}/{path}") for wire in (first, second) for path in paths)
    )
    assert all(response.status_code == 200 for response in responses)
    for offset, label, wire in [(0, "first", first), (3, "second", second)]:
        assert responses[offset].json()["owner"] == label
        assert responses[offset + 1].json()["report"]["owner"] == label
        assert responses[offset + 2].json()["owner"] == label
        assert wire.evidence.parse_manifest.call_count == 3
        assert wire.evidence.read_evidence_entry.await_args.kwargs["db"] is wire.db
        assert (
            wire.evidence.read_evidence_entry.await_args.kwargs["gitea"] is wire.forge
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "router,path",
    [
        (inspection_routes.router, f"{JOB_PATH}/progress"),
        (audit_routes.router, f"{JOB_PATH}/audit"),
        (artifacts_routes.router, f"{JOB_PATH}/evidence"),
        (diagnostics_routes.router, "/debug/emails"),
    ],
)
async def test_missing_app_factory_has_no_main_fallback(router, path):
    wire = SimpleNamespace(app=mount_router(router))
    with pytest.raises(AttributeError, match="dependencies_factory"):
        await get(wire, path)
