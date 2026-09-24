"""The thread-mount transport repair — the other half of the 0186 backfill.

Stamping a project's installation does not touch the ``thread_mounts`` rows
minted while it was unstamped; those keep a provider name and nothing else,
and one of them is enough for workspace delivery to discard every mount the
thread has (``_build_agent_cloud_mount`` is all-or-fallback). The repair
rebuilds each such row through the builder thread create uses and writes it
in place — and, like its sibling, it must never guess. These tests pin the
refusals as much as the happy path: an unstamped project, an installation the
router cannot resolve, and a rebuilt row that is itself partial all leave the
row untouched and say so.
"""

from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from orchestrator.routers.main_cloud_settings import repair_thread_mount_transport
from orchestrator.services.cloud.errors import FeatureNotAvailable
from orchestrator.application import access as access_composition
from orchestrator.application import workspace as workspace_composition


INSTANCE = "4e72e665-1f70-4b69-9804-d981b51416e6"
GROUP_FOLDER_URL = "http://srw-nextcloud/remote.php/dav/files/agent-service/SRW/"
HOME_URL = "http://srw-nextcloud/remote.php/dav/files/9edad2a0/"


def _project(*, is_default=False, stamped=True, name="Superhuman Remote Worker"):
    return {
        "id": str(uuid4()),
        "name": name,
        "is_default": is_default,
        "main_cloud_backend": "nextcloud",
        "main_cloud_backend_instance_id": INSTANCE if stamped else None,
        "main_cloud_folder_handle": (
            None
            if is_default
            else '{"backend": "nextcloud", "native_id": "14", "vendor_meta": {}}'
        ),
    }


def _partial_row(project, *, mount_kind="project", thread_status="active"):
    return {
        "id": uuid4(),
        "thread_id": uuid4(),
        "thread_status": thread_status,
        "mount_kind": mount_kind,
        "target_path": "" if mount_kind == "project_default" else "projects/srw-2",
        "source_ref": project["id"],
        "backend_id": "nextcloud",
        "backend_instance_id": None,
        "webdav_url": None,
    }


def _backend():
    backend = MagicMock()
    backend.is_initialized = True
    backend.backend_id = "nextcloud"
    backend.backend_instance_id = INSTANCE
    backend.get_project_folder_webdav_url = MagicMock(return_value=GROUP_FOLDER_URL)
    backend.resolve_user_identity = AsyncMock(return_value="owner-identity")
    home = MagicMock()
    home.webdav_url = HOME_URL
    home.handle = MagicMock()
    home.handle.to_db.return_value = "nextcloud:home:9edad2a0"
    backend.get_user_home = AsyncMock(return_value=home)
    return backend


def _router(backend=None, *, refuse=False):
    router = MagicMock()
    if refuse:
        router.for_project.side_effect = FeatureNotAvailable(
            "legacy project backend-instance authority", backend="nextcloud"
        )
        router.for_project_optional.return_value = None
    else:
        router.for_project.return_value = backend
        router.for_project_optional.return_value = backend
    return router


def _db(*, partial, projects):
    db = MagicMock()
    db.survey_partial_thread_mounts = AsyncMock(return_value=partial)
    by_id = {p["id"]: p for p in projects}
    db.get_project = AsyncMock(side_effect=lambda pid: by_id.get(str(pid)))
    db.repair_thread_mount_transport = AsyncMock(return_value=True)
    # The default-project builder resolves the owner's cloud identity; these
    # are the awaitables services/cloud/identity.py reads on every resolve.
    db.get_project_members = AsyncMock(
        return_value=[
            {
                "role": "owner",
                "user_id": "owner-uuid",
                "email": "alice@example.com",
                "display_name": "Alice",
            }
        ]
    )
    db.get_user = AsyncMock(return_value={"id": "owner-uuid", "keycloak_sub": "sub-1"})
    db.get_user_cloud_identity = AsyncMock(return_value={})
    db.merge_user_cloud_identity = AsyncMock(return_value=True)
    return db


@contextmanager
def _run(db, router):
    """Call the endpoint with the admin gate stubbed and the application's
    store and cloud router replaced.

    The route reads its collaborators from ``MainCloudSettingsRouteDependencies``,
    which the workspace composition builds from the application's resources
    this patches — including the ``thread_mount_dependencies`` factory the
    repair rebuilds rows through, so the real builder runs against the fake
    store and router wired here. The admin gate is the access composition's,
    bound to the same application.
    """
    import orchestrator.main

    resources = orchestrator.main.app.state.resources
    admin_gate = AsyncMock(return_value={"id": "admin"})

    async def require_admin(bound_resources, request):
        assert bound_resources is resources
        return await admin_gate(request)

    with (
        patch.object(access_composition, "require_admin", require_admin),
        patch.object(resources, "postgres_db", db),
        patch.object(resources, "main_cloud_router", router),
    ):
        dependencies = workspace_composition.main_cloud_settings_dependencies(resources)
        assert dependencies.operations.store is db
        assert dependencies.operations.cloud_router is router
        assert dependencies.require_admin.func is require_admin
        assert dependencies.require_admin.args == (resources,)
        yield SimpleNamespace(dependencies=dependencies, admin_gate=admin_gate)


class TestRepairRefusals:
    @pytest.mark.asyncio
    async def test_unstamped_project_is_skipped_with_a_pointer_at_the_backfill(self):
        project = _project(stamped=False)
        db = _db(partial=[_partial_row(project)], projects=[project])
        with _run(db, _router(_backend())) as ctx:
            result = await repair_thread_mount_transport(
                MagicMock(), apply=True, dependencies=ctx.dependencies
            )
        assert result["status"] == "ok"
        assert result["repaired"] == 0 and result["skipped"] == 1
        (entry,) = result["plan"]
        assert entry["action"] == "skip"
        assert entry["reason"] == "project_unstamped"
        assert "backfill-instance-authority" in entry["detail"]
        db.repair_thread_mount_transport.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_unresolvable_installation_is_never_written(self):
        """The router refusing the project is the pre-0186 shape itself: the
        rebuilt transport has no webdav_url, and a partial rebuild is exactly
        what the repair must not persist."""
        project = _project()
        db = _db(partial=[_partial_row(project)], projects=[project])
        with _run(db, _router(refuse=True)) as ctx:
            result = await repair_thread_mount_transport(
                MagicMock(), apply=True, dependencies=ctx.dependencies
            )
        (entry,) = result["plan"]
        assert entry["action"] == "skip"
        assert entry["reason"] == "transport_unresolvable"
        db.repair_thread_mount_transport.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_missing_project_is_skipped(self):
        project = _project()
        db = _db(partial=[_partial_row(project)], projects=[])
        with _run(db, _router(_backend())) as ctx:
            result = await repair_thread_mount_transport(
                MagicMock(), apply=True, dependencies=ctx.dependencies
            )
        (entry,) = result["plan"]
        assert entry["action"] == "skip"
        assert entry["reason"] == "project_missing"
        db.repair_thread_mount_transport.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_admin_gate_runs_before_anything_is_read(self):
        db = _db(partial=[], projects=[])
        with _run(db, _router(_backend())) as ctx:
            request = MagicMock()
            await repair_thread_mount_transport(request, dependencies=ctx.dependencies)
        ctx.admin_gate.assert_awaited_once_with(request)


class TestRepairHappyPath:
    @pytest.mark.asyncio
    async def test_dry_run_is_the_default_and_writes_nothing(self):
        project = _project()
        row = _partial_row(project)
        db = _db(partial=[row], projects=[project])
        with _run(db, _router(_backend())) as ctx:
            result = await repair_thread_mount_transport(
                MagicMock(), dependencies=ctx.dependencies
            )
        assert result["status"] == "dry_run"
        assert result["applied"] is False
        assert result["repairable"] == 1 and result["repaired"] == 0
        (entry,) = result["plan"]
        assert entry["action"] == "repair"
        assert entry["mount_id"] == str(row["id"])
        assert entry["backend_instance_id"] == INSTANCE
        assert entry["webdav_url"] == GROUP_FOLDER_URL
        db.repair_thread_mount_transport.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_apply_rewrites_the_row_in_place(self):
        """The persisted row keeps its id and its create-time target_path
        (``projects/srw-2`` — a collision suffix the rebuild would not
        reproduce); only the transport columns are written."""
        project = _project()
        row = _partial_row(project)
        db = _db(partial=[row], projects=[project])
        with _run(db, _router(_backend())) as ctx:
            result = await repair_thread_mount_transport(
                MagicMock(), apply=True, dependencies=ctx.dependencies
            )
        assert result["status"] == "ok" and result["applied"] is True
        assert result["repaired"] == 1 and result["skipped"] == 0
        db.repair_thread_mount_transport.assert_awaited_once_with(
            str(row["id"]),
            backend_id="nextcloud",
            backend_instance_id=INSTANCE,
            cloud_handle=project["main_cloud_folder_handle"],
            webdav_url=GROUP_FOLDER_URL,
            target_user_sub=None,
        )
        (entry,) = result["plan"]
        assert entry["target_path"] == "projects/srw-2"
        assert entry["written"] is True

    @pytest.mark.asyncio
    async def test_default_project_row_is_rebuilt_as_the_owner_home(self):
        project = _project(is_default=True, name="alice's Project")
        row = _partial_row(project, mount_kind="project_default")
        db = _db(partial=[row], projects=[project])
        with _run(db, _router(_backend())) as ctx:
            result = await repair_thread_mount_transport(
                MagicMock(), apply=True, dependencies=ctx.dependencies
            )
        assert result["repaired"] == 1
        db.repair_thread_mount_transport.assert_awaited_once_with(
            str(row["id"]),
            backend_id="nextcloud",
            backend_instance_id=INSTANCE,
            cloud_handle="nextcloud:home:9edad2a0",
            webdav_url=HOME_URL,
            target_user_sub="sub-1",
        )
        (entry,) = result["plan"]
        assert entry["target_user_sub"] is True  # presence only, never the value

    @pytest.mark.asyncio
    async def test_one_bad_row_does_not_block_the_others(self):
        """The all-or-fallback rule lives in delivery, not here: a thread with
        one unresolvable row still gets its other rows repaired."""
        good, bad = _project(name="Good"), _project(name="Bad", stamped=False)
        rows = [_partial_row(good), _partial_row(bad)]
        db = _db(partial=rows, projects=[good, bad])
        with _run(db, _router(_backend())) as ctx:
            result = await repair_thread_mount_transport(
                MagicMock(), apply=True, dependencies=ctx.dependencies
            )
        assert result["repaired"] == 1 and result["skipped"] == 1
        actions = {e["project_id"]: e["action"] for e in result["plan"]}
        assert actions == {good["id"]: "repair", bad["id"]: "skip"}

    @pytest.mark.asyncio
    async def test_nothing_partial_is_a_noop(self):
        db = _db(partial=[], projects=[])
        with _run(db, _router(_backend())) as ctx:
            result = await repair_thread_mount_transport(
                MagicMock(), apply=True, dependencies=ctx.dependencies
            )
        assert result["status"] == "noop"
        assert result["applied"] is False
        db.get_project.assert_not_awaited()
        db.repair_thread_mount_transport.assert_not_awaited()
