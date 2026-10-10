"""The per-folder grant service without a database: the reader's probe,
the sweep loop's cadence, and the project hooks that revoke at once."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.services import projects
from orchestrator.services.cloud import folder_grants, ro_probe
from orchestrator.services.cloud.base import CanaryFixture
from orchestrator.services.cloud.handles import ProjectFolderHandle

PROJECT_ID = "22222222-2222-4222-8222-222222222222"
USER_ID = "33333333-3333-4333-8333-333333333333"
WEBDAV = "http://nc/remote.php/dav/groupfolders/srw-reader-f-x/Folder/"


class _ProbeBackend:
    backend_id = "nextcloud"

    def __init__(self) -> None:
        self.client = SimpleNamespace(aclose=AsyncMock())
        self.seeded = CanaryFixture(path=".srw-ro-canary/probe.txt", version_ref="1/2")
        self.removed: list[CanaryFixture] = []

    def folder_principal_client(self, principal_id, password):
        self.principal = (principal_id, password)
        return self.client

    async def seed_canary_fixture(self, handle):
        return self.seeded

    async def remove_canary_fixture(self, handle, fixture):
        self.removed.append(fixture)


def _handle() -> ProjectFolderHandle:
    return ProjectFolderHandle(
        backend="nextcloud", native_id="7", vendor_meta={"mountpoint": "Folder"}
    )


@pytest.mark.asyncio
async def test_the_reader_probe_runs_as_the_reader_against_its_mount(monkeypatch):
    backend = _ProbeBackend()
    floors = AsyncMock(return_value=ro_probe.RoProbeResult())
    probe = AsyncMock(return_value=ro_probe.RoProbeResult())
    monkeypatch.setattr(ro_probe, "check_version_floors", floors)
    monkeypatch.setattr(ro_probe, "probe_read_only", probe)

    await folder_grants._verify_read_only(
        backend,
        _handle(),
        principal_id="srw-reader-f-x",
        password="pw",
        webdav_url=WEBDAV,
    )

    assert backend.principal == ("srw-reader-f-x", "pw")
    floors.assert_awaited_once_with(backend.client, WEBDAV, backend="nextcloud")
    args, kwargs = probe.await_args
    assert args == (backend.client, WEBDAV, ".srw-ro-canary/probe.txt")
    assert kwargs == {
        "dav_root": "http://nc/remote.php/dav",
        "username": "srw-reader-f-x",
        "version_ref": "1/2",
        "trash_ref": None,
    }
    assert backend.removed == [backend.seeded]
    backend.client.aclose.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("which", ["floors", "probe"])
async def test_a_reader_that_can_write_fails_and_its_canary_is_removed(
    monkeypatch, which
):
    backend = _ProbeBackend()
    failed = ro_probe.RoProbeResult(failures=["PUT -> 201 (expected 403/405)"])
    monkeypatch.setattr(
        ro_probe,
        "check_version_floors",
        AsyncMock(
            return_value=failed if which == "floors" else ro_probe.RoProbeResult()
        ),
    )
    monkeypatch.setattr(ro_probe, "probe_read_only", AsyncMock(return_value=failed))

    with pytest.raises(folder_grants._ProbeFailed, match="expected 403"):
        await folder_grants._verify_read_only(
            backend,
            _handle(),
            principal_id="srw-reader-f-x",
            password="pw",
            webdav_url=WEBDAV,
        )
    assert backend.removed == [backend.seeded]
    backend.client.aclose.assert_awaited_once()


@pytest.mark.asyncio
async def test_the_sweep_loop_reads_the_router_each_tick_and_survives_a_failure(
    monkeypatch,
):
    shutdown = asyncio.Event()
    routers = [object(), object()]
    seen: list[object] = []

    async def sweep(store, router):
        seen.append(router)
        if len(seen) == 1:
            raise RuntimeError("nextcloud down")
        shutdown.set()
        return folder_grants.FolderGrantSweepReport()

    monkeypatch.setattr(folder_grants, "sweep_folder_grants_once", sweep)
    router = MagicMock(side_effect=routers)

    await asyncio.wait_for(
        folder_grants.folder_grant_sweep_loop(
            shutdown, store=object(), router=router, interval_seconds=0.01
        ),
        timeout=5,
    )
    assert seen == routers


def _project_dependencies(store):
    return projects.ProjectDependencies(
        store=store,
        vector_db=SimpleNamespace(acquire=_acquire),
        forge=SimpleNamespace(is_initialized=False),
        keycloak_groups=SimpleNamespace(is_initialized=False),
        main_cloud_router=SimpleNamespace(
            for_project_optional=MagicMock(return_value=None)
        ),
        logger=MagicMock(),
        provisioning=MagicMock(),
        with_validated_tool_overrides=lambda value: value,
    )


@asynccontextmanager
async def _acquire():
    yield SimpleNamespace(execute=AsyncMock())


@pytest.mark.asyncio
async def test_removing_a_member_revokes_their_grants_of_that_project(monkeypatch):
    revoke = AsyncMock(return_value=1)
    monkeypatch.setattr(folder_grants, "revoke_folder_grants", revoke)
    store = SimpleNamespace(
        get_user_role_in_project=AsyncMock(return_value="editor"),
        remove_project_member=AsyncMock(return_value=True),
        get_user=AsyncMock(return_value=None),
    )
    deps = _project_dependencies(store)

    await projects.remove_project_member(
        PROJECT_ID, USER_ID, caller={"id": USER_ID}, dependencies=deps
    )

    revoke.assert_awaited_once_with(
        store,
        deps.main_cloud_router,
        reason="member_removed",
        project_id=PROJECT_ID,
        user_id=USER_ID,
    )


@pytest.mark.asyncio
async def test_deleting_a_project_revokes_its_grants_before_the_row_goes(
    monkeypatch,
):
    order: list[str] = []

    async def revoke(*_args, **kwargs):
        order.append(f"revoke:{kwargs['reason']}")
        return 0

    async def delete(_project_id):
        order.append("delete")
        return True

    monkeypatch.setattr(folder_grants, "revoke_folder_grants", revoke)
    store = SimpleNamespace(
        get_project_repositories=AsyncMock(return_value=[]),
        acquire=_acquire,
        delete_project=delete,
    )

    await projects.delete_project(
        PROJECT_ID,
        project={"id": PROJECT_ID, "is_default": False},
        dependencies=_project_dependencies(store),
    )

    assert order == ["revoke:project_deleted", "delete"]


@pytest.mark.asyncio
async def test_a_hook_never_fails_its_caller():
    store = SimpleNamespace(
        request_cloud_folder_grant_revokes=AsyncMock(side_effect=OSError("db down"))
    )
    assert (
        await folder_grants.revoke_folder_grants(
            store, object(), reason="member_removed", project_id=PROJECT_ID
        )
        == 0
    )
    with pytest.raises(ValueError):
        await folder_grants.revoke_folder_grants(
            store, object(), reason="because", project_id=PROJECT_ID
        )


def test_the_agent_can_say_why_a_folder_has_no_grant():
    """Every reason a plan may leave a folder out has the agent's words."""
    from orchestrator.services.cloud_connector_mounts import EXCLUSION_REASONS
    from shared.runtime.services.cloud_mount.sidecar import REASON_TEXT

    assert {"grant_unavailable", "reader_unavailable"} <= set(EXCLUSION_REASONS)
    assert set(EXCLUSION_REASONS) <= set(REASON_TEXT)
