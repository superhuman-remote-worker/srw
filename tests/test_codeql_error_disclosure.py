"""No HTTP response carries exception text, a traceback, or unescaped input.

CodeQL reported 16 ``py/stack-trace-exposure`` and 2 ``py/reflective-xss``
findings against these handlers. Both classes are pinned here, over the real
HTTP transport, because the leak is a property of the *response body* and only
a client-side view proves it is gone.

Each error-path case monkeypatches the collaborator the handler calls so it
raises, then asserts three things at once: the status code the handler already
used is unchanged, the body carries neither the exception's message nor a
``Traceback``, and an ``error_ref`` joins the response to the server-side log
line that does hold the full exception.

The reflected-XSS cases drive the two magic-link routes with a live
``<script>`` payload in the token path segment and in the tool metadata, and
assert the response escapes it rather than echoing markup.
"""

from __future__ import annotations

import re
import sys
import urllib.parse
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from tests._mounted_router import mount_router


# The text of the exceptions raised below. No response body may contain it.
BOOM = "postgres://user:hunter2@db.internal/secret rejected at 10.1.2.3:5432"
XSS = "<script>alert(1)</script>"
# The token arrives as a single path segment, and ASGI unquotes ``%2F`` before
# routing — a payload containing "/" would 404 on the split path instead of
# reaching the handler. This slash-free payload reflects into the same sink
# (the form ``action`` attribute) and breaks out of it just as effectively.
XSS_PATH = '"><img src=x onerror=alert(1)>'

USER_ID = "00000000-0000-0000-0000-0000000000c1"
USER = {"id": USER_ID, "is_admin": False}
PROJECT_ID = "00000000-0000-0000-0000-0000000000b1"
DATASOURCE_ID = "00000000-0000-0000-0000-0000000000d1"
JOB_ID = "a6fa6f2a-9101-4c1e-9b9e-0000000000c1"
AGENT_ID = "11111111-1111-4111-8111-111111111111"
PROCESS_GENERATION = "33333333-3333-4333-8333-333333333333"


def assert_no_leak(payload: object) -> None:
    """The serialized response may not disclose internals."""
    body = str(payload)
    assert BOOM not in body
    assert "hunter2" not in body
    assert "Traceback" not in body
    assert "RuntimeError" not in body
    assert "OSError" not in body


def error_ref_of(payload: dict) -> str:
    """Every sanitized error body carries a correlation id for the log line."""
    ref = payload.get("error_ref")
    assert isinstance(ref, str) and len(ref) == 12, payload
    return ref


# =============================================================================
# POST /api/datasources/{datasource_id}/test — six probe branches, all 200
# =============================================================================


def _datasource_wire(row: dict):
    from orchestrator.routers.datasources import DatasourcesDependencies as RouteDeps
    from orchestrator.routers.datasources import router
    from orchestrator.services.datasources import DatasourceDependencies as OpDeps
    from orchestrator.services.kb_task_registry import KbDatasourceTaskRegistry
    from orchestrator.services.knowledge_index import KnowledgeIndexDependencies

    store = SimpleNamespace(get_datasource=AsyncMock(return_value=dict(row)))

    async def approved(_request, _store):
        return USER

    async def datasource_owner(_request, _store, _datasource_id):
        return USER, dict(row)

    async def project_gate(_request, _store, project_id, **_kw):
        return USER, {"id": project_id}

    async def job_access(_request, _store, job_id):
        return USER, {"id": job_id}

    ops = OpDeps(
        store=store,
        vector_db=MagicMock(),
        knowledge_index=KnowledgeIndexDependencies(
            store=store,
            vector_db=MagicMock(),
            gitea_client=MagicMock(),
            logger=MagicMock(),
            tasks=KbDatasourceTaskRegistry(),
            inject_system_kb_embedding_profile=AsyncMock(return_value=None),
        ),
        mcp_datasources_enabled=lambda: False,
        validate_mcp_datasource=lambda _url, _creds: None,
    )
    deps = RouteDeps(
        store=store,
        operations=ops,
        require_approved_user=approved,
        require_project_member=project_gate,
        require_project_owner=project_gate,
        require_datasource_access=datasource_owner,
        require_datasource_owner=datasource_owner,
        require_job_access=job_access,
    )
    app = mount_router(
        router, factories={"datasources_dependencies_factory": lambda: deps}
    )
    return TestClient(app)


def _row(ds_type: str) -> dict:
    return {
        "id": DATASOURCE_ID,
        "name": "prod-db",
        "type": ds_type,
        "created_by": USER_ID,
        "credentials": {"username": "dbuser", "password": "hunter2"},
        "connection_url": "postgresql://dbuser:hunter2@db.internal:5432/app",
    }


def _exploding_module(attribute: str) -> MagicMock:
    """A stand-in driver module whose first live call raises ``BOOM``."""
    module = MagicMock()
    target = module
    parts = attribute.split(".")
    for part in parts[:-1]:
        target = getattr(target, part)
    getattr(target, parts[-1]).side_effect = RuntimeError(BOOM)
    return module


@pytest.mark.parametrize(
    ("ds_type", "target", "patcher"),
    [
        ("kb", "orchestrator.services.kb_datasources.test_kb_datasource", "attr"),
        ("postgresql", "orchestrator.services.datasources.asyncpg.connect", "attr"),
        ("neo4j", "neo4j", "module"),
        ("mongodb", "pymongo", "module"),
        ("webdav", "webdav3.client", "module"),
        ("email", "orchestrator.services.datasources.probe_email_connection", "attr"),
    ],
)
def test_a_failing_connector_probe_reports_without_the_exception_text(
    ds_type, target, patcher
):
    """Each probe branch keeps its 200 ``status: error`` report, minus the leak."""
    client = _datasource_wire(_row(ds_type))

    if patcher == "attr":
        context = patch(target, side_effect=RuntimeError(BOOM))
    else:
        attribute = {
            "neo4j": "GraphDatabase.driver",
            "pymongo": "MongoClient",
            "webdav3.client": "Client",
        }[target]
        context = patch.dict(
            sys.modules, {target: _exploding_module(attribute)}, clear=False
        )

    with context:
        response = client.post(f"/api/datasources/{DATASOURCE_ID}/test")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "error"
    assert_no_leak(body)
    error_ref_of(body)


def test_a_crashing_probe_gate_is_a_500_without_the_exception_text():
    """The catch-all re-raise at the end of ``test_datasource``."""

    async def exploding_gate(*_args, **_kwargs):
        raise RuntimeError(BOOM)

    client = _datasource_wire(_row("postgresql"))
    client.app.state.datasources_dependencies_factory().require_datasource_owner  # noqa: B018
    from orchestrator.routers.datasources import DatasourcesDependencies as RouteDeps

    deps = client.app.state.datasources_dependencies_factory()
    patched = RouteDeps(
        store=deps.store,
        operations=deps.operations,
        require_approved_user=deps.require_approved_user,
        require_project_member=deps.require_project_member,
        require_project_owner=deps.require_project_owner,
        require_datasource_access=exploding_gate,
        require_datasource_owner=exploding_gate,
        require_job_access=deps.require_job_access,
    )
    client.app.state.datasources_dependencies_factory = lambda: patched

    response = client.post(f"/api/datasources/{DATASOURCE_ID}/test")

    assert response.status_code == 500
    assert_no_leak(response.json())


# =============================================================================
# POST /api/admin/system-settings/main_cloud/test — two probe failures, 200
# =============================================================================

MAIN_CLOUD = "/api/admin/system-settings/main_cloud"


def _main_cloud_client(monkeypatch, *, build=None, ensure=None):
    from orchestrator.routers import main_cloud_settings as route_module
    from orchestrator.services import main_cloud_settings as ops
    from orchestrator.services.cloud import config as cloud_config

    monkeypatch.setattr(cloud_config, "missing_secret_envs", lambda *_a, **_k: [])
    if build is not None:
        monkeypatch.setattr(ops, "build_backend", build)
    else:
        probe = SimpleNamespace(
            ensure_initialized=ensure,
            health_check=AsyncMock(),
            close=AsyncMock(),
        )
        monkeypatch.setattr(ops, "build_backend", lambda **_k: probe)

    async def require_admin(_request):
        return {"id": "admin-1"}

    operations = ops.MainCloudSettingsDependencies(
        store=SimpleNamespace(delete_system_setting=AsyncMock(return_value=None)),
        cloud_router=SimpleNamespace(active=None, active_instance_id=None),
        rebind_cloud_router=lambda _backend: None,
        thread_mount_dependencies=lambda: None,
    )
    dependencies = route_module.MainCloudSettingsRouteDependencies(
        operations=operations,
        require_admin=require_admin,
    )
    app = mount_router(
        route_module.router,
        factories={"main_cloud_settings_dependencies_factory": lambda: dependencies},
    )
    return TestClient(app, raise_server_exceptions=False)


def _main_cloud_body() -> dict:
    return {
        "value": {"backend_id": "opencloud", "base_url": "https://cloud.example"},
        "credentials_ref": "env:OC_CLIENT_SECRET",
        "expected_activation_revision": 0,
    }


def test_a_failing_backend_build_keeps_its_reason_without_the_exception_text(
    monkeypatch,
):
    def build(**_kwargs):
        raise RuntimeError(BOOM)

    client = _main_cloud_client(monkeypatch, build=build)

    response = client.post(f"{MAIN_CLOUD}/test", json=_main_cloud_body())

    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is False
    assert body["detail"] == "build_backend failed"
    assert_no_leak(body)
    error_ref_of(body)


def test_a_failing_backend_init_keeps_its_reason_without_the_exception_text(
    monkeypatch,
):
    client = _main_cloud_client(
        monkeypatch, ensure=AsyncMock(side_effect=RuntimeError(BOOM))
    )

    response = client.post(f"{MAIN_CLOUD}/test", json=_main_cloud_body())

    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is False
    assert body["detail"] == "ensure_initialized raised"
    assert_no_leak(body)
    error_ref_of(body)


# =============================================================================
# POST /api/jobs/{job_id}/{accept,reject} — an unroutable cloud backend, 503
# =============================================================================


def _job_diff_client():
    from orchestrator.routers.job_diff import JobDiffDependencies, router
    from orchestrator.services import job_diff_review

    job = {
        "id": JOB_ID,
        "status": "pending_review",
        "project_id": PROJECT_ID,
        "diff_status": "pending",
        "cloud_diff_baseline_commit": "a" * 40,
        "repo_name": "job-a6fa6f2a",
        "execution_lane": "pinned",
        "context": {},
    }
    project = {
        "id": PROJECT_ID,
        "main_cloud_backend": "nextcloud",
        "main_cloud_folder_handle": "opaque-handle",
    }

    def unroutable(_project):
        raise RuntimeError(BOOM)

    store = SimpleNamespace(
        get_project=AsyncMock(return_value=project),
        update_job_cloud_diff=AsyncMock(),
        update_job_merge_status=AsyncMock(),
        update_job_status=AsyncMock(),
        merge_job_context=AsyncMock(),
    )
    operations = job_diff_review.JobDiffReviewDependencies(
        store=store,
        vector_store=SimpleNamespace(name="vector"),
        forge=SimpleNamespace(is_initialized=True, name="forge"),
        cloud_router=SimpleNamespace(for_project=unroutable),
        get_completion_control=MagicMock(),
        guard_completion_control=AsyncMock(),
        claim_completion_control=AsyncMock(return_value=None),
        abort_completion_control_claim=AsyncMock(),
        advance_project_loop=AsyncMock(),
    )
    deps = JobDiffDependencies(
        store=SimpleNamespace(name="auth-store"),
        diff_review=operations,
        require_job_access=AsyncMock(return_value=(USER, job)),
    )
    app = mount_router(
        router, factories={"job_diff_dependencies_factory": lambda: deps}
    )
    return TestClient(app, raise_server_exceptions=False)


def test_an_unroutable_cloud_backend_names_the_backend_but_not_the_exception():
    """``accept`` is the only arm that routes to a cloud backend.

    ``reject`` reaches the same module without ever calling
    ``cloud_router.for_project``; its own ``str(exc)`` at
    ``job_diff_review.py:554`` is the self-composed
    ``CompletionControlClaimConflict`` vocabulary the cockpit branches on, so
    that one stays.
    """
    client = _job_diff_client()

    response = client.post(f"/api/jobs/{JOB_ID}/accept")

    assert response.status_code == 503
    detail = response.json()["detail"]
    assert "Cloud backend 'nextcloud' unavailable" in detail
    assert_no_leak(response.json())
    # ``HTTPException.detail`` is a bare string here, so the correlation id
    # travels inside it rather than as a sibling key.
    assert re.search(r"\(error_ref=[0-9a-f]{12}\)", detail), detail


# =============================================================================
# Agent API — persistent_app, dual_app, app
# =============================================================================


def _persistent_client(monkeypatch, *, session, terminate=None):
    from agent.api import persistent_app as module

    app = module.create_persistent_app("config", "thread-1")
    monkeypatch.setattr(module, "_stateless_mode", lambda: False)
    monkeypatch.setattr(module, "_session", session)
    monkeypatch.setattr(module, "_thread_id", "thread-1")
    monkeypatch.setattr(module, "_sessions_served", 1)
    monkeypatch.setattr(
        module, "_canonical_pinned_session_identity_fingerprint", lambda _v: "fp"
    )
    monkeypatch.setattr(
        module, "_current_pinned_session_identity_fingerprint", lambda: "fp"
    )
    monkeypatch.setattr(module, "_registered_pinned_agent_id", lambda: AGENT_ID)
    monkeypatch.setattr(module, "_session_runtime_generation", "gen-1")
    monkeypatch.setattr(module, "_session_runtime_attach_token", "tok-1")
    if terminate is not None:
        monkeypatch.setattr(module, "_terminate_session", terminate)
    return TestClient(app, raise_server_exceptions=False), module


def _reset_headers() -> dict[str, str]:
    return {
        "X-Agent-ID": AGENT_ID,
        "X-Session-Runtime-Generation": "gen-1",
        "X-Session-Runtime-Attach-Token": "tok-1",
    }


def _reset_body() -> dict[str, str]:
    return {
        "thread_id": "thread-1",
        "workspace_generation": "wsgen-1",
        "workspace_runtime_incarnation": "inc-1",
    }


def _overlay_session(reset_error: Exception) -> SimpleNamespace:
    def reset_cloud_overlay():
        raise reset_error

    return SimpleNamespace(
        overlay_mount_manager=MagicMock(),
        protected_cloud_required=True,
        protected_workspace_generation="wsgen-1",
        protected_workspace_runtime_incarnation="inc-1",
        reset_cloud_overlay=reset_cloud_overlay,
    )


def test_persistent_detach_failure_is_a_500_without_the_exception_text(monkeypatch):
    client, _ = _persistent_client(
        monkeypatch,
        session=MagicMock(),
        terminate=AsyncMock(side_effect=RuntimeError(BOOM)),
    )

    response = client.post(
        "/session/detach", json={"session_identity_fingerprint": "fp"}
    )

    assert response.status_code == 500
    assert_no_leak(response.json())
    error_ref_of(response.json())


def test_an_unavailable_cloud_overlay_stays_a_404_without_the_exception_text(
    monkeypatch,
):
    from agent.api.persistent_session import CloudOverlayUnavailable

    client, _ = _persistent_client(
        monkeypatch, session=_overlay_session(CloudOverlayUnavailable(BOOM))
    )

    response = client.post(
        "/cloud-overlay/reset", json=_reset_body(), headers=_reset_headers()
    )

    assert response.status_code == 404
    assert_no_leak(response.json())
    error_ref_of(response.json())


def test_a_failing_cloud_overlay_reset_stays_a_500_without_the_exception_text(
    monkeypatch,
):
    client, _ = _persistent_client(
        monkeypatch, session=_overlay_session(RuntimeError(BOOM))
    )

    response = client.post(
        "/cloud-overlay/reset", json=_reset_body(), headers=_reset_headers()
    )

    assert response.status_code == 500
    assert_no_leak(response.json())
    error_ref_of(response.json())


def _shell_state_client(monkeypatch, module_name: str):
    if module_name == "app":
        from agent.api import app as module

        app = module.create_app()
    else:
        from agent.api import dual_app as module

        app = module.create_dual_app()

    shell = MagicMock()
    shell.list_tabs.side_effect = RuntimeError(BOOM)
    agent = MagicMock()
    agent._shell_manager = shell
    monkeypatch.setattr(module, "_agent", agent)
    monkeypatch.setattr(module, "_current_job_id", JOB_ID)
    monkeypatch.setattr(
        module,
        "_orchestrator_client",
        SimpleNamespace(
            agent_id=AGENT_ID, dispatch_process_generation=PROCESS_GENERATION
        ),
    )
    return TestClient(app, raise_server_exceptions=False)


@pytest.mark.parametrize("module_name", ["app", "dual_app"])
def test_a_failing_shell_state_read_stays_a_200_without_the_exception_text(
    monkeypatch, module_name
):
    client = _shell_state_client(monkeypatch, module_name)

    response = client.post(
        "/system/shell-state",
        json={
            "expected_agent_id": AGENT_ID,
            "expected_pod_uid": None,
            "expected_process_generation": PROCESS_GENERATION,
            "expected_job_id": JOB_ID,
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["tabs"] == []
    assert_no_leak(body)
    error_ref_of(body)


def test_dual_app_detach_failure_is_a_500_without_the_exception_text(monkeypatch):
    from agent.api import dual_app as module
    from agent.api import persistent_app as pa

    app = module.create_dual_app()
    monkeypatch.setattr(module, "_pod_state", module.PodState.SESSION)
    monkeypatch.setattr(pa, "_thread_id", "thread-1")
    monkeypatch.setattr(
        pa, "_canonical_pinned_session_identity_fingerprint", lambda _v: "fp"
    )
    monkeypatch.setattr(
        pa, "_current_pinned_session_identity_fingerprint", lambda: "fp"
    )
    monkeypatch.setattr(
        pa, "_terminate_session", AsyncMock(side_effect=RuntimeError(BOOM))
    )
    client = TestClient(app, raise_server_exceptions=False)

    response = client.post(
        "/session/detach", json={"session_identity_fingerprint": "fp"}
    )

    assert response.status_code == 500
    assert_no_leak(response.json())
    error_ref_of(response.json())


# =============================================================================
# Reflected XSS — GET /magic/approve/{token}, POST /magic/extend/{token}
# =============================================================================


class _Conn:
    """Serves the queued rows in order; the queue is shared across acquires."""

    def __init__(self, rows: list):
        self._rows = rows

    async def fetchrow(self, *_args, **_kwargs):
        return self._rows.pop(0) if self._rows else None


class _DB:
    def __init__(self, rows: list):
        self._rows = list(rows)

    def acquire(self):
        @asynccontextmanager
        async def _cm():
            yield _Conn(self._rows)

        return _cm()


def _magic_client(
    monkeypatch, *, tool_name: str = "run_command", rows=None, thread_id=None
):
    """The magic routes, mounted from their router with one application's store.

    The router reads ``headless_notifications`` as a module at call time, so the
    token validation is patched on that module object.
    """
    from orchestrator.routers import thread_permissions

    permission_row = {
        "id": "perm-1",
        "tool_name": tool_name,
        "tool_args": {"cmd": "ls"},
        "status": "pending",
    }
    monkeypatch.setattr(
        thread_permissions.headless_notifications,
        "validate_magic_link",
        AsyncMock(
            return_value={
                "approval_id": "perm-1",
                "intended_decision": "approved",
                "thread_id": thread_id,
            }
        ),
    )
    store = _DB(rows if rows is not None else [permission_row])
    app = mount_router(
        thread_permissions.router,
        factories={
            "thread_permission_dependencies_factory": (
                lambda: thread_permissions.ThreadPermissionDependencies(
                    store=store,
                    require_thread_owner=AsyncMock(),
                    notification_service=MagicMock(),
                    cockpit_url=lambda: "http://localhost:4200",
                    wake_after_permission_decision=AsyncMock(),
                )
            )
        },
    )
    return TestClient(app, raise_server_exceptions=False)


def test_the_confirmation_page_escapes_a_script_payload_in_the_token(monkeypatch):
    client = _magic_client(monkeypatch)

    response = client.get(f"/magic/approve/{urllib.parse.quote(XSS_PATH, safe='')}")

    assert response.status_code == 200
    # The token lands in a form ``action`` attribute, where it is sanitized by
    # percent-encoding: the quote that would close the attribute and the angle
    # brackets that would open a tag cannot appear raw anywhere in the page.
    assert XSS_PATH not in response.text
    assert "onerror=alert(1)" not in response.text
    assert "%22%3E%3Cimg" in response.text


def test_the_confirmation_page_escapes_a_script_payload_in_the_tool_name(monkeypatch):
    client = _magic_client(monkeypatch, tool_name=XSS)

    response = client.get("/magic/approve/plain-token")

    assert response.status_code == 200
    assert XSS not in response.text
    assert "<script>" not in response.text.lower()
    assert "&lt;script&gt;" in response.text


def test_the_extend_page_escapes_a_script_payload_in_the_token(monkeypatch):
    """``/magic/extend`` re-renders the same page, so the token reflects again.

    A bound ``thread_id`` is required — without one the handler answers 400
    before rendering. The extend UPDATE returns the bumped row, then the
    permission row is re-read: two ``fetchrow`` calls, hence two queued rows.
    """
    client = _magic_client(
        monkeypatch,
        thread_id="11111111-2222-4333-8444-555555555555",
        rows=[
            {"extend_count": 1},
            {
                "tool_name": "run_command",
                "tool_args": {"cmd": "ls"},
                "status": "pending",
            },
        ],
    )

    response = client.post(f"/magic/extend/{urllib.parse.quote(XSS_PATH, safe='')}")

    assert response.status_code == 200
    assert XSS_PATH not in response.text
    assert "onerror=alert(1)" not in response.text
    assert "%22%3E%3Cimg" in response.text
