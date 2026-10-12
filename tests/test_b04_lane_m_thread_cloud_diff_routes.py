"""Wire contracts for the extracted protected-cloud diff review routes.

R1.B04 lane M. The behaviours pinned here are the ones the move could quietly
change:

* the owner gate runs **before** the protected-mode gate on all five routes,
  so a non-owner never learns whether a thread is protected;
* a non-protected thread answers 404 with a plain string detail, not 403;
* the per-file 404 distinguishes ``not_in_staged_diff`` from
  ``staged_content_unreadable`` — the review UI explains them differently;
* restage's two 409s (``no_workspace`` and
  ``cloud_stage_authority_unavailable``) keep their order and their dict
  bodies, and staging is scheduled through the application's task registry;
* apply/reject map ``StagedApplyError`` verbatim, answer 422 on a malformed
  epoch, and 502 on a partial write;
* overlay reset is captured against the producer of the reviewed bytes, and
  refuses when the current runtime is a different generation.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from tests._mounted_router import mount_router

from orchestrator.services import protected_cloud_engage as engage
from orchestrator.services import thread_cloud_diff as ops
from orchestrator.routers import thread_cloud_diff as route_module

THREAD_ID = "thread-cd-1"
USER = {"id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", "email": "u@example.test"}
BASE = f"/api/agents/threads/{THREAD_ID}/cloud-diff"

RUNTIME_GENERATION = "44444444-4444-4444-8444-444444444444"
AGENT_ID = "55555555-5555-4555-8555-555555555555"
ATTACH_TOKEN = "66666666-6666-4666-8666-666666666666"
WORKSPACE_GENERATION = "77777777-7777-4777-8777-777777777777"
WORKSPACE_RUNTIME = "88888888-8888-4888-8888-888888888888"


# --------------------------------------------------------------------------- #
# Doubles
# --------------------------------------------------------------------------- #


class _StageRegistry:
    """The cloud-stage half of the contract's ``CloudTaskRegistry``."""

    def __init__(self) -> None:
        self.started: list[str] = []

    def stage_has(self, key: str) -> bool:
        return key in self.started

    def stage_start(self, key: str, factory) -> None:
        if key in self.started:
            return
        self.started.append(key)
        self.factory = factory


def _thread(*, protected: bool = True, **over: Any) -> dict[str, Any]:
    thread = {
        "id": THREAD_ID,
        "user_id": USER["id"],
        "execution_lane": "pinned",
        "agent_id": AGENT_ID,
        "runtime_generation": RUNTIME_GENERATION,
        "runtime_attach_token": ATTACH_TOKEN,
        "runtime_retirement_token": None,
        "metadata": {
            "protected_cloud": protected,
            "workspace_container": {
                "status": "ready",
                "pod_ip": "10.0.0.9",
                "runtime_incarnation": WORKSPACE_RUNTIME,
                "_canvas_workspace_generation": WORKSPACE_GENERATION,
            },
            "_workspace_binding": {
                "kind": "remote",
                "generation": WORKSPACE_GENERATION,
            },
        },
    }
    thread.update(over)
    return thread


def _store(**over: Any) -> SimpleNamespace:
    @asynccontextmanager
    async def thread_advisory_lock(_thread_id):
        yield True

    store = SimpleNamespace(
        thread_advisory_lock=thread_advisory_lock,
        get_ro_mount_by_thread=AsyncMock(return_value=None),
        get_thread=AsyncMock(return_value=None),
        get_agent=AsyncMock(return_value=None),
    )
    for key, value in over.items():
        setattr(store, key, value)
    return store


def _wire(
    *,
    thread=None,
    owner_gate=None,
    store=None,
    protected_enabled: bool = True,
    snapshot_service=None,
    registry=None,
):
    """Mount the real router with only this application's factories."""
    calls = SimpleNamespace(owner=[])
    backing = store if store is not None else _store()
    stage_registry = registry if registry is not None else _StageRegistry()

    async def require_thread_owner(_request, _store, thread_id):
        calls.owner.append(thread_id)
        if owner_gate is not None:
            return await owner_gate(thread_id)
        return USER, (thread if thread is not None else _thread())

    protected = engage.ProtectedCloudEngageDependencies(
        store=backing,
        cloud_router=SimpleNamespace(),
        cloud_tasks=SimpleNamespace(),
        is_protected_cloud_mode_enabled=lambda: protected_enabled,
        thread_workspace_backend=lambda _t: "sandbox",
    )
    operations = ops.ThreadCloudDiffDependencies(
        store=backing,
        cloud_router=SimpleNamespace(),
        snapshot_service=snapshot_service or SimpleNamespace(),
        vm_provisioner=SimpleNamespace(),
        cloud_tasks=stage_registry,
        protected_cloud=protected,
        is_protected_cloud_mode_enabled=lambda: protected_enabled,
    )
    dependencies = route_module.ThreadCloudDiffRouteDependencies(
        store=backing,
        operations=operations,
        require_thread_owner=require_thread_owner,
    )
    app = mount_router(
        route_module.router,
        factories={"thread_cloud_diff_dependencies_factory": lambda: dependencies},
    )
    return SimpleNamespace(
        client=TestClient(app, raise_server_exceptions=False),
        calls=calls,
        store=backing,
        operations=operations,
        registry=stage_registry,
        app=app,
    )


class _Summary:
    def __init__(self, files, meta=None):
        self.files = files
        self.meta = meta or {}


def _file(path, status="modified", binary=False):
    return SimpleNamespace(path=path, status=status, binary=binary)


def _call(client, method, path):
    """GET takes no body; POST carries the epoch the apply/reject pair reads."""
    if method == "get":
        return client.get(path)
    return client.post(path, json={"epoch": 1})


# --------------------------------------------------------------------------- #
# The owner gate runs first, on every route
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "method,path",
    [
        ("get", BASE),
        ("get", f"{BASE}/a/b.txt"),
        ("post", f"{BASE}/restage"),
        ("post", f"{BASE}/apply"),
        ("post", f"{BASE}/reject"),
    ],
)
def test_the_owner_gate_refuses_before_anything_is_read(method, path):
    async def deny(_thread_id):
        raise HTTPException(status_code=403, detail="Not your thread")

    wired = _wire(owner_gate=deny)

    response = _call(wired.client, method, path)

    assert response.status_code == 403
    assert response.json()["detail"] == "Not your thread"
    assert wired.calls.owner == [THREAD_ID]
    wired.store.get_ro_mount_by_thread.assert_not_awaited()


@pytest.mark.parametrize(
    "method,path",
    [
        ("get", BASE),
        ("get", f"{BASE}/a/b.txt"),
        ("post", f"{BASE}/restage"),
        ("post", f"{BASE}/apply"),
        ("post", f"{BASE}/reject"),
    ],
)
def test_a_non_protected_thread_is_404_with_a_string_detail(method, path):
    wired = _wire(thread=_thread(protected=False))

    response = _call(wired.client, method, path)

    assert response.status_code == 404
    assert response.json()["detail"] == "Thread is not in protected cloud mode."


@pytest.mark.parametrize(
    "method,path",
    [
        ("get", BASE),
        ("post", f"{BASE}/restage"),
    ],
)
def test_the_feature_flag_alone_makes_a_protected_thread_a_404(method, path):
    wired = _wire(protected_enabled=False)

    response = _call(wired.client, method, path)

    assert response.status_code == 404


def test_metadata_handed_back_as_a_json_string_is_still_parsed():
    thread = _thread()
    thread["metadata"] = '{"protected_cloud": true}'
    wired = _wire(thread=thread)

    response = wired.client.get(BASE)

    assert response.status_code == 200


# --------------------------------------------------------------------------- #
# Summary
# --------------------------------------------------------------------------- #


def test_summary_with_no_mount_row_is_an_empty_epoch_zero_body():
    wired = _wire()

    body = wired.client.get(BASE).json()

    assert body == {
        "thread_id": THREAD_ID,
        "epoch": 0,
        "staged_at": None,
        "counts": {"added": 0, "modified": 0, "deleted": 0},
        "protected_mount": None,
        "files": [],
    }


def test_summary_renders_the_staged_file_list(monkeypatch):
    summary = _Summary(
        [_file("a.txt"), _file("bin.png", status="added", binary=True)],
        {"epoch": 4, "staged_at": "2026-01-01T00:00:00Z", "counts": {"added": 1}},
    )
    src = SimpleNamespace(summary=AsyncMock(return_value=summary))
    monkeypatch.setattr(
        ops,
        "_thread_cloud_diff_source",
        AsyncMock(return_value=({"id": "r"}, src, "MyProject")),
    )
    wired = _wire()

    body = wired.client.get(BASE).json()

    assert body["epoch"] == 4
    assert body["protected_mount"] == "MyProject"
    assert body["counts"] == {"added": 1}
    assert body["files"] == [
        {"path": "a.txt", "status": "modified", "binary": False},
        {"path": "bin.png", "status": "added", "binary": True},
    ]


# --------------------------------------------------------------------------- #
# Per-file content
# --------------------------------------------------------------------------- #


def test_a_path_outside_the_staged_set_is_not_in_staged_diff(monkeypatch):
    src = SimpleNamespace(
        file=AsyncMock(return_value=None),
        summary=AsyncMock(return_value=_Summary([_file("other.txt")])),
    )
    monkeypatch.setattr(
        ops, "_thread_cloud_diff_source", AsyncMock(return_value=({}, src, None))
    )
    wired = _wire()

    response = wired.client.get(f"{BASE}/a.txt")

    assert response.status_code == 404
    assert response.json()["detail"]["code"] == "not_in_staged_diff"


def test_a_listed_but_unreadable_path_says_so(monkeypatch):
    """A torn manifest/tar pair is a different explanation from a re-stage."""
    src = SimpleNamespace(
        file=AsyncMock(return_value=None),
        summary=AsyncMock(return_value=_Summary([_file("a.txt")])),
    )
    monkeypatch.setattr(
        ops, "_thread_cloud_diff_source", AsyncMock(return_value=({}, src, None))
    )
    wired = _wire()

    response = wired.client.get(f"{BASE}/a.txt")

    assert response.status_code == 404
    assert response.json()["detail"]["code"] == "staged_content_unreadable"


def test_nothing_staged_at_all_is_also_not_in_staged_diff():
    wired = _wire()

    response = wired.client.get(f"{BASE}/a.txt")

    assert response.status_code == 404
    assert response.json()["detail"]["code"] == "not_in_staged_diff"


def test_file_content_body_shape(monkeypatch):
    content = SimpleNamespace(
        path="a.txt",
        status="modified",
        old_content="x",
        new_content="y",
        old_binary=False,
        new_binary=False,
    )
    src = SimpleNamespace(file=AsyncMock(return_value=content))
    monkeypatch.setattr(
        ops, "_thread_cloud_diff_source", AsyncMock(return_value=({}, src, None))
    )
    wired = _wire()

    body = wired.client.get(f"{BASE}/a.txt").json()

    assert body == {
        "thread_id": THREAD_ID,
        "path": "a.txt",
        "status": "modified",
        "old_content": "x",
        "new_content": "y",
        "old_binary": False,
        "new_binary": False,
    }


# --------------------------------------------------------------------------- #
# Restage
# --------------------------------------------------------------------------- #


def test_restage_409s_before_reading_any_row_when_no_workspace(monkeypatch):
    from orchestrator.services.cloud_staging import stage as stage_module

    monkeypatch.setattr(stage_module, "_resolve_workspace_ssh", lambda _m: None)
    wired = _wire()

    response = wired.client.post(f"{BASE}/restage")

    assert response.status_code == 409
    assert response.json()["detail"] == {"code": "no_workspace"}
    wired.store.get_ro_mount_by_thread.assert_not_awaited()


def test_restage_409s_when_stage_authority_is_unavailable(monkeypatch):
    from orchestrator.services.cloud_staging import stage as stage_module

    monkeypatch.setattr(stage_module, "_resolve_workspace_ssh", lambda _m: ("h", 22))
    monkeypatch.setattr(ops, "_capture_cloud_stage_authority", lambda *_a: None)
    monkeypatch.setattr(ops, "_thread_selected_vm_workspace", lambda _t: False)
    wired = _wire()

    response = wired.client.post(f"{BASE}/restage")

    assert response.status_code == 409
    assert response.json()["detail"] == {"code": "cloud_stage_authority_unavailable"}


def test_restage_409s_when_the_runtime_is_already_retiring(monkeypatch):
    from orchestrator.services.cloud_staging import stage as stage_module

    monkeypatch.setattr(stage_module, "_resolve_workspace_ssh", lambda _m: ("h", 22))
    monkeypatch.setattr(
        ops,
        "_capture_cloud_stage_authority",
        lambda *_a: {"runtime_retirement_token": 9},
    )
    monkeypatch.setattr(ops, "_thread_selected_vm_workspace", lambda _t: True)
    wired = _wire()

    response = wired.client.post(f"{BASE}/restage")

    assert response.status_code == 409
    assert response.json()["detail"] == {"code": "cloud_stage_authority_unavailable"}


def test_restage_schedules_through_the_registry_under_the_stage_key(monkeypatch):
    from orchestrator.services.cloud_staging import stage as stage_module

    monkeypatch.setattr(stage_module, "_resolve_workspace_ssh", lambda _m: ("h", 22))
    monkeypatch.setattr(
        ops, "_capture_cloud_stage_authority", lambda *_a: {"runtime_generation": "g"}
    )
    monkeypatch.setattr(ops, "_cloud_stage_task_key", lambda tid, _a: f"{tid}:key")
    wired = _wire()

    response = wired.client.post(f"{BASE}/restage")

    assert response.status_code == 200
    assert response.json() == {"scheduled": True}
    assert wired.registry.started == [f"{THREAD_ID}:key"]


def test_restage_is_deduped_by_the_registry(monkeypatch):
    from orchestrator.services.cloud_staging import stage as stage_module

    monkeypatch.setattr(stage_module, "_resolve_workspace_ssh", lambda _m: ("h", 22))
    monkeypatch.setattr(
        ops, "_capture_cloud_stage_authority", lambda *_a: {"runtime_generation": "g"}
    )
    monkeypatch.setattr(ops, "_cloud_stage_task_key", lambda tid, _a: f"{tid}:key")
    wired = _wire()

    wired.client.post(f"{BASE}/restage")
    wired.client.post(f"{BASE}/restage")

    assert wired.registry.started == [f"{THREAD_ID}:key"]


# --------------------------------------------------------------------------- #
# Apply / reject
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("verb", ["apply", "reject"])
def test_a_malformed_epoch_is_422(verb):
    wired = _wire()

    response = wired.client.post(f"{BASE}/{verb}", json={"epoch": "soon"})

    assert response.status_code == 422
    assert response.json()["detail"] == {"code": "invalid_epoch"}


@pytest.mark.parametrize("verb", ["apply", "reject"])
def test_a_missing_epoch_defaults_to_the_sentinel(monkeypatch, verb):
    from orchestrator.services.cloud_staging import apply as apply_module

    seen = {}

    async def _run(**kwargs):
        seen.update(kwargs)
        return {"applied": 0}

    monkeypatch.setattr(apply_module, "apply_staged_diff", _run)
    monkeypatch.setattr(apply_module, "reject_staged_diff", _run)
    wired = _wire()

    response = wired.client.post(f"{BASE}/{verb}", json={})

    assert response.status_code == 200
    assert seen["epoch"] == -1


@pytest.mark.parametrize("verb", ["apply", "reject"])
def test_a_staged_apply_error_maps_verbatim(monkeypatch, verb):
    from orchestrator.services.cloud_staging import apply as apply_module

    error = apply_module.StagedApplyError(410, {"code": "staging_missing"})

    async def _raise(**_kwargs):
        raise error

    monkeypatch.setattr(apply_module, "apply_staged_diff", _raise)
    monkeypatch.setattr(apply_module, "reject_staged_diff", _raise)
    wired = _wire()

    response = wired.client.post(f"{BASE}/{verb}", json={"epoch": 3})

    assert response.status_code == 410
    assert response.json()["detail"] == {"code": "staging_missing"}


def test_apply_partial_write_failure_is_502_with_the_result_merged(monkeypatch):
    from orchestrator.services.cloud_staging import apply as apply_module

    async def _partial(**_kwargs):
        return {"written": 2, "errors": ["a.txt"]}

    monkeypatch.setattr(apply_module, "apply_staged_diff", _partial)
    wired = _wire()

    response = wired.client.post(f"{BASE}/apply", json={"epoch": 3})

    assert response.status_code == 502
    assert response.json()["detail"] == {
        "code": "partial_write_failure",
        "written": 2,
        "errors": ["a.txt"],
    }


def test_reject_never_treats_errors_as_a_502(monkeypatch):
    """Reject has no cloud leg, so it has no partial-write gate — moved as is."""
    from orchestrator.services.cloud_staging import apply as apply_module

    async def _rejected(**_kwargs):
        return {"errors": ["ignored"]}

    monkeypatch.setattr(apply_module, "reject_staged_diff", _rejected)
    wired = _wire()

    response = wired.client.post(f"{BASE}/reject", json={"epoch": 3})

    assert response.status_code == 200
    assert response.json() == {"thread_id": THREAD_ID, "errors": ["ignored"]}


def test_apply_passes_the_routers_own_collaborators_down(monkeypatch):
    from orchestrator.services.cloud_staging import apply as apply_module

    seen = {}

    async def _run(**kwargs):
        seen.update(kwargs)
        return {"ok": True}

    monkeypatch.setattr(apply_module, "apply_staged_diff", _run)
    wired = _wire()

    wired.client.post(f"{BASE}/apply", json={"epoch": 3})

    assert seen["postgres_db"] is wired.operations.store
    assert seen["main_cloud_router"] is wired.operations.cloud_router
    assert seen["snapshot_service"] is wired.operations.snapshot_service


# --------------------------------------------------------------------------- #
# Overlay reset authority
# --------------------------------------------------------------------------- #


def _producer() -> dict[str, str]:
    return {
        "agent_id": AGENT_ID,
        "runtime_generation": RUNTIME_GENERATION,
        "runtime_attach_token": ATTACH_TOKEN,
        "workspace_generation": WORKSPACE_GENERATION,
        "workspace_runtime_incarnation": WORKSPACE_RUNTIME,
    }


def _reset_thread() -> dict[str, Any]:
    thread = _thread()
    thread["metadata"]["workspace_container"]["workspace_runtime_incarnation"] = (
        WORKSPACE_RUNTIME
    )
    return thread


def test_overlay_reset_authority_matches_the_producer_of_the_reviewed_bytes(
    monkeypatch,
):
    monkeypatch.setattr(ops, "WORKSPACE_RUNTIME_INCARNATION_KEY", "runtime_incarnation")

    assert (
        ops._capture_thread_overlay_reset_authority(
            _reset_thread(), {"producer": _producer()}
        )
        == _producer()
    )


def test_overlay_reset_refuses_a_summary_from_an_earlier_generation(monkeypatch):
    monkeypatch.setattr(ops, "WORKSPACE_RUNTIME_INCARNATION_KEY", "runtime_incarnation")
    producer = _producer()
    producer["runtime_generation"] = "99999999-9999-4999-8999-999999999999"

    assert (
        ops._capture_thread_overlay_reset_authority(
            _reset_thread(), {"producer": producer}
        )
        is None
    )


def test_overlay_reset_refuses_a_summary_without_producer_identity(monkeypatch):
    monkeypatch.setattr(ops, "WORKSPACE_RUNTIME_INCARNATION_KEY", "runtime_incarnation")

    assert ops._capture_thread_overlay_reset_authority(_reset_thread(), {}) is None
    assert ops._capture_thread_overlay_reset_authority(_reset_thread(), None) is None


def test_overlay_reset_refuses_a_stateless_or_unbound_runtime(monkeypatch):
    monkeypatch.setattr(ops, "WORKSPACE_RUNTIME_INCARNATION_KEY", "runtime_incarnation")
    thread = _reset_thread()
    thread["execution_lane"] = "stateless"

    assert ops._current_thread_overlay_reset_authority(thread) is None


@pytest.mark.asyncio
async def test_reset_overlay_is_false_without_captured_authority():
    operations = _wire().operations

    assert (
        await ops._reset_thread_overlay(THREAD_ID, None, dependencies=operations)
        is False
    )


@pytest.mark.asyncio
async def test_reset_overlay_is_false_when_the_runtime_moved_on(monkeypatch):
    monkeypatch.setattr(ops, "WORKSPACE_RUNTIME_INCARNATION_KEY", "runtime_incarnation")
    moved = _reset_thread()
    moved["runtime_generation"] = "99999999-9999-4999-8999-999999999999"
    store = _store(get_thread=AsyncMock(return_value=moved))
    operations = _wire(store=store).operations

    assert (
        await ops._reset_thread_overlay(THREAD_ID, _producer(), dependencies=operations)
        is False
    )
    store.get_agent.assert_not_awaited()


# --------------------------------------------------------------------------- #
# Per-invocation dependency resolution
# --------------------------------------------------------------------------- #


def test_a_rebound_factory_is_observed_by_the_next_request():
    """The dependency is built per request, so swapping what the application's
    factory returns changes the next answer without remounting."""
    protected = engage.ProtectedCloudEngageDependencies(
        store=_store(),
        cloud_router=SimpleNamespace(),
        cloud_tasks=SimpleNamespace(),
        is_protected_cloud_mode_enabled=lambda: True,
        thread_workspace_backend=lambda _t: "sandbox",
    )

    def _dependencies(thread):
        async def require_thread_owner(_request, _store, _thread_id):
            return USER, thread

        return route_module.ThreadCloudDiffRouteDependencies(
            store=_store(),
            operations=ops.ThreadCloudDiffDependencies(
                store=_store(),
                cloud_router=SimpleNamespace(),
                snapshot_service=SimpleNamespace(),
                vm_provisioner=SimpleNamespace(),
                cloud_tasks=_StageRegistry(),
                protected_cloud=protected,
                is_protected_cloud_mode_enabled=lambda: True,
            ),
            require_thread_owner=require_thread_owner,
        )

    state = {"deps": _dependencies(_thread())}
    app = mount_router(
        route_module.router,
        factories={"thread_cloud_diff_dependencies_factory": lambda: state["deps"]},
    )
    client = TestClient(app, raise_server_exceptions=False)

    assert client.get(BASE).status_code == 200

    state["deps"] = _dependencies(_thread(protected=False))
    assert client.get(BASE).status_code == 404


@pytest.mark.parametrize(
    ("role", "applied"),
    [("owner", True), ("editor", True), ("viewer", False), (None, False)],
)
def test_apply_needs_an_owner_or_editor_of_the_folders_project(
    monkeypatch, role, applied
):
    """Main cloud slice 5: a protected folder may be another project's; its
    staged changes are written only by an owner or editor of that project,
    now (an administrator outside it included)."""
    from orchestrator.services.cloud_staging import apply as apply_module

    folder_project = "abababab-abab-4bab-8bab-abababababab"
    calls: list[dict] = []

    async def _run(**kwargs):
        calls.append(kwargs)
        return {"written": 1}

    monkeypatch.setattr(apply_module, "apply_staged_diff", _run)
    store = _store(
        get_ro_mount_by_thread=AsyncMock(
            return_value={"source_binding": {"source_ref": folder_project}}
        ),
        get_user_project_roles=AsyncMock(
            return_value={folder_project: role} if role else {}
        ),
    )
    wired = _wire(store=store)

    response = wired.client.post(f"{BASE}/apply", json={"epoch": 3})

    if applied:
        assert response.status_code == 200, response.text
        assert len(calls) == 1
    else:
        assert response.status_code == 403
        assert response.json()["detail"]["code"] == "protected_cloud_apply_not_member"
        assert calls == []
    store.get_user_project_roles.assert_awaited_once_with(USER["id"], [folder_project])
