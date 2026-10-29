"""GET /internal/units/{unit_id}/cloud-token: a Virtual Session's cloud folder
bearer, minted per call under the claim lease (S5 of
features/cloud_folders_on_virtual_sessions.md § Provider neutrality).

The route's policy (``services/cloud_remote_token``): the claim bundle's
two-layer auth (internal key, then proof of the LIVE lease with one generic
403), the plan the executor serves named by fingerprint and incarnation, the
folder bound again on the ``virtual`` tier, the provider minting the token,
and nothing of it persisted or logged.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException

from orchestrator.services import agent_cloud_mounts, cloud_remote_token
from orchestrator.services.agent_cloud_mounts import LiveMountSet
from orchestrator.services.cloud import DataPlaneToken
from orchestrator.services.cloud.errors import CloudBackendError, CloudBackendErrorKind

UNIT_ID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
POD_NAME = "stateless-agent-1"
POD_UID = "aaaaaaaa-1111-4222-8333-bbbbbbbbbbbb"
FINGERPRINT = "f" * 64
INCARNATION = "c" * 32
SECRET = "very-secret-client-secret"
TOKEN = "eyJ.bearer.token"

LEASED_ROW = {
    "unit_kind": "session_turn",
    "state": "leased",
    "lease_token": 7,
    "leased_by": POD_NAME,
    "thread_lane": "stateless",
}


class _AsyncCM:
    def __init__(self, value=None):
        self._value = value

    async def __aenter__(self):
        return self._value

    async def __aexit__(self, *exc):
        return False


class FakeDB:
    def __init__(self, *, run_queue_row, thread):
        self._row = run_queue_row
        self._thread = thread
        self.conn = MagicMock()
        self.conn.fetchrow = AsyncMock(return_value=run_queue_row)
        self.list_thread_mounts = AsyncMock(return_value=[])

    def acquire(self):
        return _AsyncCM(self.conn)

    async def get_thread(self, tid):
        return self._thread


def _thread(**over):
    thread = {
        "id": UNIT_ID,
        "user_id": "u-1",
        "execution_lane": "stateless",
        "main_cloud_share_handle": None,
        "metadata": {
            "cloud_mount_status": {
                "version": 1,
                "delivery": "remote",
                "fingerprint": FINGERPRINT,
                "runtime_incarnation": INCARNATION,
                "mounts": {
                    "oc": {
                        "mount_kind": "project",
                        "target_path": "cloud/oc",
                        "access": "read_write",
                        "state": "mounted",
                    }
                },
                "excluded": [],
            }
        },
    }
    thread.update(over)
    return thread


def _built(name="oc", *, auth=None, backend="opencloud"):
    return {
        "mount_id": f"m-{name}",
        "mount_kind": "project",
        "backend": backend,
        "target_path": f"/cloud/{name}",
        "workspace_name": name,
        "access": "read_write",
        "source_ref": f"c-{name}",
        "source": {
            "type": "webdav",
            "config": {"url": "https://cloud.example/dav/spaces/d/", "vendor": "x"},
        },
        "auth": auth
        if auth is not None
        else {
            "type": "keycloak_client_credentials",
            "issuer": "https://auth.example/realms/srw",
            "client_id": "opencloud-orchestrator",
            "client_secret": SECRET,
        },
    }


def _backend(**over):
    backend = SimpleNamespace(
        backend_id="opencloud",
        is_initialized=True,
        mint_data_plane_token=AsyncMock(return_value=DataPlaneToken(TOKEN, 300.0)),
    )
    for key, value in over.items():
        setattr(backend, key, value)
    return backend


def _deps(db, backend):
    cloud = SimpleNamespace(
        cloud_router=SimpleNamespace(for_thread_optional=lambda thread: backend)
    )
    return cloud_remote_token.CloudRemoteTokenDependencies(
        db=db, require_internal=AsyncMock(), cloud_mount_dependencies=lambda: cloud
    )


def _live(monkeypatch, *mounts):
    seen: dict = {}

    async def live(thread, *, mount_rows, runtime_is_vm, dependencies, **kw):
        seen.update(kw, runtime_is_vm=runtime_is_vm, thread=thread["id"])
        return LiveMountSet(mounts=list(mounts))

    monkeypatch.setattr(agent_cloud_mounts, "_resolve_live_mount_set", live)
    return seen


async def _mint(deps, **over):
    kwargs = dict(
        lease_token=7,
        pod_name=POD_NAME,
        pod_uid=POD_UID,
        mount_id="m-oc",
        fingerprint=FINGERPRINT,
        incarnation=INCARNATION,
        dependencies=deps,
    )
    kwargs.update(over)
    unit_id = kwargs.pop("unit_id", UNIT_ID)
    return await cloud_remote_token.mint_cloud_remote_token(unit_id, **kwargs)


async def _status(deps, **over) -> int:
    with pytest.raises(HTTPException) as info:
        await _mint(deps, **over)
    return info.value.status_code


@pytest.mark.asyncio
async def test_the_executor_of_the_live_lease_gets_the_folders_bearer(
    monkeypatch, caplog
):
    seen = _live(monkeypatch, _built(), _built("nc", auth={"type": "basic"}))
    backend = _backend()
    db = FakeDB(run_queue_row=dict(LEASED_ROW), thread=_thread())
    with caplog.at_level(logging.DEBUG):
        out = await _mint(_deps(db, backend))
    assert out == {"mount_id": "m-oc", "token": TOKEN, "expires_in": 300.0}
    # Bound again exactly as the delivery binds: the virtual tier, no VM.
    assert seen == {
        "workspace_backend": "virtual",
        "runtime_is_vm": False,
        "thread": UNIT_ID,
    }
    backend.mint_data_plane_token.assert_awaited_once_with(auth=_built()["auth"])
    assert "run_queue" in db.conn.fetchrow.await_args.args[0]
    assert db.conn.fetchrow.await_args.args[1] == UNIT_ID
    # The token and the client secret never reach a log line.
    assert TOKEN not in caplog.text and SECRET not in caplog.text
    assert "m-oc" in caplog.text and INCARNATION in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "row",
    [
        {**LEASED_ROW, "lease_token": 6},
        {**LEASED_ROW, "state": "runnable"},
        {**LEASED_ROW, "leased_by": "another-pod"},
    ],
)
async def test_a_stale_token_another_pod_and_not_leased_are_one_generic_403(
    monkeypatch, row
):
    _live(monkeypatch, _built())
    db = FakeDB(run_queue_row=row, thread=_thread())
    with pytest.raises(HTTPException) as info:
        await _mint(_deps(db, _backend()))
    assert info.value.status_code == 403
    assert info.value.detail == "Lease validation failed"


@pytest.mark.asyncio
async def test_an_unknown_or_malformed_unit_is_404(monkeypatch):
    _live(monkeypatch, _built())
    db = FakeDB(run_queue_row=None, thread=None)
    deps = _deps(db, _backend())
    assert await _status(deps) == 404
    assert await _status(deps, unit_id="not-a-uuid") == 404
    db.conn.fetchrow.assert_awaited_once()  # the malformed id never reaches SQL


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "row",
    [
        {**LEASED_ROW, "unit_kind": "worker_batch"},
        {**LEASED_ROW, "thread_lane": "pinned"},
    ],
)
async def test_a_worker_unit_or_a_pinned_thread_is_409(monkeypatch, row):
    _live(monkeypatch, _built())
    db = FakeDB(run_queue_row=row, thread=_thread())
    assert await _status(_deps(db, _backend())) == 409


@pytest.mark.asyncio
async def test_a_token_is_minted_only_for_the_threads_current_plan(monkeypatch):
    _live(monkeypatch, _built())
    db = FakeDB(run_queue_row=dict(LEASED_ROW), thread=_thread())
    deps = _deps(db, _backend())
    assert await _status(deps, fingerprint="0" * 64) == 409
    assert await _status(deps, incarnation="other-claim") == 409
    # A thread without a remote plan (a Pod's record, or none) mints nothing.
    pod_record = _thread(metadata={"cloud_mount_status": {"version": 1}})
    assert (
        await _status(
            _deps(FakeDB(run_queue_row=dict(LEASED_ROW), thread=pod_record), _backend())
        )
        == 409
    )
    no_record = _thread(metadata={})
    assert (
        await _status(
            _deps(FakeDB(run_queue_row=dict(LEASED_ROW), thread=no_record), _backend())
        )
        == 409
    )


@pytest.mark.asyncio
async def test_a_folder_the_session_has_no_entry_for_is_404(monkeypatch):
    _live(monkeypatch, _built("other"))
    db = FakeDB(run_queue_row=dict(LEASED_ROW), thread=_thread())
    assert await _status(_deps(db, _backend())) == 404


@pytest.mark.asyncio
async def test_only_a_token_route_auth_kind_is_minted(monkeypatch):
    _live(
        monkeypatch,
        _built("nc", auth={"type": "basic", "password": "p"}),
        _built("home", auth={"type": "keycloak_user_impersonation"}),
    )
    db = FakeDB(run_queue_row=dict(LEASED_ROW), thread=_thread())
    deps = _deps(db, _backend())
    assert await _status(deps, mount_id="m-nc") == 409
    assert await _status(deps, mount_id="m-home") == 409


@pytest.mark.asyncio
async def test_a_provider_that_mints_none_is_409(monkeypatch):
    _live(monkeypatch, _built())
    db = FakeDB(run_queue_row=dict(LEASED_ROW), thread=_thread())
    no_capability = SimpleNamespace(backend_id="opencloud", is_initialized=True)
    assert await _status(_deps(db, no_capability)) == 409
    assert await _status(_deps(db, _backend(is_initialized=False))) == 409
    assert await _status(_deps(db, _backend(backend_id="nextcloud"))) == 409
    assert await _status(_deps(db, None)) == 409
    refusing = _backend(
        mint_data_plane_token=AsyncMock(
            side_effect=CloudBackendError(
                CloudBackendErrorKind.NOT_SUPPORTED, "no", backend="opencloud"
            )
        )
    )
    assert await _status(_deps(db, refusing)) == 409


@pytest.mark.asyncio
async def test_a_provider_that_cannot_mint_now_is_503(monkeypatch, caplog):
    _live(monkeypatch, _built())
    db = FakeDB(run_queue_row=dict(LEASED_ROW), thread=_thread())
    failing = _backend(
        mint_data_plane_token=AsyncMock(
            side_effect=CloudBackendError(
                CloudBackendErrorKind.AUTHENTICATION_FAILED,
                f"keycloak said no: {SECRET}",
                backend="opencloud",
            )
        )
    )
    with caplog.at_level(logging.WARNING):
        assert await _status(_deps(db, failing)) == 503
    assert SECRET not in caplog.text
    crashing = _backend(mint_data_plane_token=AsyncMock(side_effect=RuntimeError("x")))
    assert await _status(_deps(db, crashing)) == 503


def test_the_application_composes_the_route_and_its_dependencies():
    from orchestrator import main as orch_main
    from orchestrator.application import workspace as workspace_composition
    from orchestrator.security import access

    deps = workspace_composition.cloud_remote_token_dependencies(
        orch_main.app.state.resources
    )
    assert deps.require_internal is access.require_internal
    assert isinstance(
        deps.cloud_mount_dependencies(), agent_cloud_mounts.AgentCloudMountDependencies
    )
    paths = {getattr(route, "path", None) for route in orch_main.app.routes}
    assert "/internal/units/{unit_id}/cloud-token" in paths
    assert callable(orch_main.app.state.cloud_remote_token_dependencies_factory)


@pytest.mark.asyncio
async def test_the_router_guards_with_the_internal_key_and_never_caches(monkeypatch):
    from orchestrator.routers import cloud_remote_token as routes

    guard = AsyncMock()
    deps = cloud_remote_token.CloudRemoteTokenDependencies(
        db=None, require_internal=guard, cloud_mount_dependencies=lambda: None
    )
    minted = AsyncMock(
        return_value={"mount_id": "m-oc", "token": TOKEN, "expires_in": 1.0}
    )
    monkeypatch.setattr(cloud_remote_token, "mint_cloud_remote_token", minted)
    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(cloud_remote_token_dependencies_factory=lambda: deps)
        )
    )
    response = await routes.internal_unit_cloud_token(
        UNIT_ID,
        request,
        lease_token=7,
        pod_name=POD_NAME,
        pod_uid=POD_UID,
        mount_id="m-oc",
        fingerprint=FINGERPRINT,
        incarnation=INCARNATION,
    )
    guard.assert_awaited_once_with(request)
    assert response.headers["cache-control"] == "no-store"
    assert minted.await_args.args == (UNIT_ID,)
    assert minted.await_args.kwargs["dependencies"] is deps
    assert minted.await_args.kwargs["mount_id"] == "m-oc"
