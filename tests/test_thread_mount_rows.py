"""Unit tests for the thread-mount-row builders in ``orchestrator/main.py``.

Phase 2 of ``knowledge-base/knowledge/features/cloud_collaboration_model.md`` §9 introduces the
``project_default`` row shape — default projects mount the owner's cloud
home at the workspace root rather than under ``projects/<slug>/``. These
tests cover the builder helpers (``build_thread_mount_rows`` and
``build_default_project_mount_row``) directly so the wiring is exercised
without standing up the full thread-create path.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import orchestrator.main as orch_main
from orchestrator.services import thread_mount_rows as thread_mount_rows_service

import orchestrator.main
from orchestrator.application import preparation as preparation_composition
from orchestrator.application import workspace as workspace_composition
from orchestrator.services import agent_cloud_mounts as agent_cloud_mounts_module
from orchestrator.services import thread_mount_rows as thread_mount_rows_module


_BACKEND_INSTANCE_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
_THREAD_ID = "11111111-1111-4111-8111-111111111111"
_RUNTIME_GENERATION = "22222222-2222-4222-8222-222222222222"


def _project(
    *, project_id: str, is_default: bool = False, name: str = "Project"
) -> dict:
    """Shape a project row the way ``get_project`` returns it."""
    return {
        "id": project_id,
        "name": name,
        "is_default": is_default,
        "main_cloud_backend": "opencloud",
        "main_cloud_backend_instance_id": _BACKEND_INSTANCE_ID,
        "main_cloud_folder_handle": (
            "opencloud:drive-abc:" if not is_default else None
        ),
    }


def _user_home(webdav_url: str = "https://oc.test/dav/spaces/drive-xyz/"):
    home = MagicMock()
    home.webdav_url = webdav_url
    handle = MagicMock()
    handle.to_db.return_value = "opencloud:drive-xyz:user_home"
    home.handle = handle
    return home


def _backend(*, initialized: bool = True, backend_id: str = "opencloud"):
    backend = MagicMock()
    backend.is_initialized = initialized
    backend.backend_id = backend_id
    backend.backend_instance_id = _BACKEND_INSTANCE_ID
    backend.resolve_user_identity = AsyncMock(return_value="user-xyz")
    backend.get_user_home = AsyncMock(return_value=_user_home())
    backend.get_project_folder_webdav_url = MagicMock(
        return_value="https://oc.test/dav/spaces/drive-abc/"
    )
    return backend


def _owner_member(
    *,
    user_id: str = "owner-uuid",
    email: str = "alice@example.com",
    display_name: str = "Alice",
) -> dict:
    return {
        "role": "owner",
        "user_id": user_id,
        "email": email,
        "display_name": display_name,
    }


def _owner_user_record(*, keycloak_sub: str = "alice-keycloak-sub") -> dict:
    return {"id": "owner-uuid", "keycloak_sub": keycloak_sub}


def _fake_db() -> MagicMock:
    """Base postgres_db stand-in. The identity-cache methods always exist on
    the real Database (services/cloud/identity.py reads them on every
    resolve), so every fake needs awaitable stubs; tests override the rest.
    """
    db = MagicMock()
    db.get_user_cloud_identity = AsyncMock(return_value={})
    db.merge_user_cloud_identity = AsyncMock(return_value=True)
    return db


@pytest.mark.asyncio
async def test_default_project_emits_user_home_row():
    """Default project → ``project_default`` row with target_path='' and
    the owner's home Space's webdav URL. ``target_user_sub`` carries the
    Keycloak ``sub`` of the owner so the agent can do RFC 8693 exchange.
    """
    from orchestrator.services.thread_mount_rows import (
        build_default_project_mount_row,
    )

    project = _project(project_id="p-default", is_default=True, name="Default")
    backend = _backend()

    fake_db = _fake_db()
    fake_db.get_project_members = AsyncMock(return_value=[_owner_member()])
    fake_db.get_user = AsyncMock(return_value=_owner_user_record())
    router = MagicMock()
    router.for_project.return_value = backend
    router.for_project_optional.return_value = backend

    with (
        patch("orchestrator.main.app.state.resources.postgres_db", fake_db),
        patch("orchestrator.main.app.state.resources.main_cloud_router", router),
    ):
        row = await build_default_project_mount_row(
            "p-default",
            project,
            dependencies=preparation_composition.thread_mount_dependencies(
                orch_main.app.state.resources
            ),
        )

    assert row is not None
    assert row["mount_kind"] == "project_default"
    assert row["target_path"] == ""
    assert row["source_kind"] == "user_home"
    assert row["source_ref"] == "p-default"
    assert row["backend_id"] == "opencloud"
    assert row["webdav_url"] == "https://oc.test/dav/spaces/drive-xyz/"
    assert row["cloud_handle"] == "opencloud:drive-xyz:user_home"
    assert row["target_user_sub"] == "alice-keycloak-sub"
    backend.resolve_user_identity.assert_awaited_once_with("alice@example.com", "alice")
    backend.get_user_home.assert_awaited_once_with("user-xyz")


@pytest.mark.asyncio
async def test_default_project_no_owner_returns_none():
    """Owner missing from the project → fall back to legacy session folder."""
    from orchestrator.services.thread_mount_rows import (
        build_default_project_mount_row,
    )

    project = _project(project_id="p", is_default=True)
    fake_db = _fake_db()
    fake_db.get_project_members = AsyncMock(return_value=[])
    fake_db.get_user = AsyncMock(return_value=None)
    router = MagicMock()
    _b = _backend()
    router.for_project.return_value = _b
    router.for_project_optional.return_value = _b

    with (
        patch("orchestrator.main.app.state.resources.postgres_db", fake_db),
        patch("orchestrator.main.app.state.resources.main_cloud_router", router),
    ):
        row = await build_default_project_mount_row(
            "p",
            project,
            dependencies=preparation_composition.thread_mount_dependencies(
                orch_main.app.state.resources
            ),
        )
    assert row is None


@pytest.mark.asyncio
async def test_default_project_owner_missing_keycloak_sub_returns_none():
    """Owner exists but has never SSO'd, so we don't have their Keycloak
    sub yet → can't do token-exchange → no row. Caller falls back to
    legacy session folder so the thread still has SOMETHING.
    """
    from orchestrator.services.thread_mount_rows import (
        build_default_project_mount_row,
    )

    project = _project(project_id="p", is_default=True)
    fake_db = _fake_db()
    fake_db.get_project_members = AsyncMock(return_value=[_owner_member()])
    fake_db.get_user = AsyncMock(
        return_value={"id": "owner-uuid", "keycloak_sub": None}
    )
    router = MagicMock()
    _b = _backend()
    router.for_project.return_value = _b
    router.for_project_optional.return_value = _b

    with (
        patch("orchestrator.main.app.state.resources.postgres_db", fake_db),
        patch("orchestrator.main.app.state.resources.main_cloud_router", router),
    ):
        row = await build_default_project_mount_row(
            "p",
            project,
            dependencies=preparation_composition.thread_mount_dependencies(
                orch_main.app.state.resources
            ),
        )
    assert row is None


@pytest.mark.asyncio
async def test_default_project_user_home_unresolvable_returns_none():
    """Owner exists on the backend but ``get_user_home`` returns None
    (e.g. drive not yet provisioned) → fall back, no row.
    """
    from orchestrator.services.thread_mount_rows import (
        build_default_project_mount_row,
    )

    project = _project(project_id="p", is_default=True)
    backend = _backend()
    backend.get_user_home = AsyncMock(return_value=None)

    fake_db = _fake_db()
    fake_db.get_project_members = AsyncMock(
        return_value=[_owner_member(email="bob@example.com", display_name="Bob")]
    )
    fake_db.get_user = AsyncMock(return_value=_owner_user_record())
    router = MagicMock()
    router.for_project.return_value = backend
    router.for_project_optional.return_value = backend

    with (
        patch("orchestrator.main.app.state.resources.postgres_db", fake_db),
        patch("orchestrator.main.app.state.resources.main_cloud_router", router),
    ):
        row = await build_default_project_mount_row(
            "p",
            project,
            dependencies=preparation_composition.thread_mount_dependencies(
                orch_main.app.state.resources
            ),
        )
    assert row is None


@pytest.mark.asyncio
async def test_default_project_backend_uninitialized_returns_none():
    from orchestrator.services.thread_mount_rows import (
        build_default_project_mount_row,
    )

    project = _project(project_id="p", is_default=True)
    backend = _backend(initialized=False)
    fake_db = _fake_db()
    fake_db.get_project_members = AsyncMock(return_value=[])
    fake_db.get_user = AsyncMock(return_value=None)
    router = MagicMock()
    router.for_project.return_value = backend
    router.for_project_optional.return_value = backend

    with (
        patch("orchestrator.main.app.state.resources.postgres_db", fake_db),
        patch("orchestrator.main.app.state.resources.main_cloud_router", router),
    ):
        row = await build_default_project_mount_row(
            "p",
            project,
            dependencies=preparation_composition.thread_mount_dependencies(
                orch_main.app.state.resources
            ),
        )
    assert row is None


@pytest.mark.asyncio
async def test_build_thread_mount_rows_mixes_default_and_non_default():
    """One default project + one non-default project → two rows of the
    right shapes. The default-project row points at workspace root; the
    non-default row lives under ``projects/<slug>/``.
    """
    from orchestrator.services.thread_mount_rows import build_thread_mount_rows

    default_project = _project(project_id="p-default", is_default=True, name="My Home")
    other_project = _project(project_id="p-other", is_default=False, name="Alpha")
    fake_db = _fake_db()

    async def get_project(pid: str):
        return {"p-default": default_project, "p-other": other_project}.get(pid)

    fake_db.get_project = AsyncMock(side_effect=get_project)
    fake_db.get_project_members = AsyncMock(
        return_value=[_owner_member(email="carol@example.com", display_name="Carol")]
    )
    fake_db.get_user = AsyncMock(
        return_value=_owner_user_record(keycloak_sub="carol-sub")
    )
    backend = _backend()
    router = MagicMock()
    router.for_project.return_value = backend
    router.for_project_optional.return_value = backend
    router.for_backend.return_value = backend

    with (
        patch("orchestrator.main.app.state.resources.postgres_db", fake_db),
        patch("orchestrator.main.app.state.resources.main_cloud_router", router),
    ):
        rows = await build_thread_mount_rows(
            ["p-default", "p-other"],
            dependencies=preparation_composition.thread_mount_dependencies(
                orch_main.app.state.resources
            ),
        )

    assert len(rows) == 2
    by_kind = {r["mount_kind"]: r for r in rows}
    assert by_kind["project_default"]["target_path"] == ""
    assert by_kind["project_default"]["source_ref"] == "p-default"
    assert by_kind["project"]["target_path"] == "projects/alpha"
    assert by_kind["project"]["source_ref"] == "p-other"


@pytest.mark.asyncio
async def test_project_ids_from_mounts_includes_project_default():
    """Phase 2: a ``project_default`` row counts as a project attachment
    for downstream datasource/visibility resolution.
    """
    from orchestrator.services.thread_mount_rows import project_ids_from_mounts

    rows = [
        {"mount_kind": "project_default", "source_ref": "p-default"},
        {"mount_kind": "project", "source_ref": "p-alpha"},
        {"mount_kind": "repo", "source_ref": "r-1"},
    ]
    assert project_ids_from_mounts(rows) == ["p-default", "p-alpha"]


@pytest.mark.asyncio
async def test_thread_project_ids_preserves_scope_when_default_mount_is_unavailable():
    """Logical connector scope must survive a failed cloud-home mount build."""
    fake_db = _fake_db()
    fake_db.list_thread_mounts = AsyncMock(return_value=[])
    fake_db.get_thread = AsyncMock(
        return_value={
            "id": "thread-1",
            "project_id": "p-default",
            "metadata": {},
        }
    )
    fake_db.replace_thread_mounts = AsyncMock()

    with (
        patch("orchestrator.main.app.state.resources.postgres_db", fake_db),
        patch.object(
            thread_mount_rows_service,
            "build_thread_mount_rows",
            AsyncMock(return_value=[]),
        ),
    ):
        project_ids = await thread_mount_rows_module.thread_project_ids(
            "thread-1",
            dependencies=preparation_composition.thread_mount_dependencies(
                orchestrator.main.app.state.resources
            ),
        )

    assert project_ids == ["p-default"]
    fake_db.replace_thread_mounts.assert_not_awaited()


# ---------------------------------------------------------------------------
# Phase 4 — session-folder skip predicate
# ---------------------------------------------------------------------------


def test_should_skip_session_folder_with_project_default_mount():
    """A ``project_default`` row with webdav_url means the user's cloud
    home is mounted at workspace root — session folder is redundant.
    (Phase 2 behavior, preserved by Phase 4.)
    """
    import orchestrator.main

    rows = [
        {
            "mount_kind": "project_default",
            "target_path": "",
            "webdav_url": "https://oc.test/dav/spaces/drive-xyz/",
        }
    ]
    assert (
        thread_mount_rows_module.should_skip_session_folder(
            rows,
            dependencies=preparation_composition.thread_mount_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
        is True
    )


def test_rclone_driver_keeps_session_folder_fallback(monkeypatch):
    """With the lazy mount driver enabled, keep the regular session folder
    provisioned so unsupported user-home auth has a safe fallback.
    """
    import orchestrator.main

    monkeypatch.setenv("CLOUD_WORKSPACE_DRIVER", "rclone_mount")
    rows = [
        {
            "mount_kind": "project_default",
            "target_path": "",
            "webdav_url": "https://nc.test/remote.php/dav/files/alice/",
        }
    ]
    assert (
        thread_mount_rows_module.should_skip_session_folder(
            rows,
            dependencies=preparation_composition.thread_mount_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
        is False
    )


def test_should_skip_session_folder_with_non_default_project_mount():
    """Phase 4: a regular ``project`` mount (non-default, under
    ``projects/<name>/``) also counts as a user-visible cloud surface
    — session folder is redundant for it too. This is the new Phase 4
    behavior; pre-Phase-4 this returned False and the session folder
    was unnecessarily provisioned alongside.
    """
    import orchestrator.main

    rows = [
        {
            "mount_kind": "project",
            "target_path": "projects/alpha",
            "webdav_url": "https://oc.test/dav/spaces/drive-abc/",
        }
    ]
    assert (
        thread_mount_rows_module.should_skip_session_folder(
            rows,
            dependencies=preparation_composition.thread_mount_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
        is True
    )


def test_should_skip_session_folder_with_repo_mount():
    """Forward-compatibility: a ``repo`` row with a transport URL
    is also a user-visible surface (Phase 3b territory). The predicate
    is mount-kind-agnostic — any mount with a working webdav_url
    short-circuits the session folder.
    """
    import orchestrator.main

    rows = [
        {
            "mount_kind": "repo",
            "target_path": "repos/alpha",
            "webdav_url": "https://gitea.test/some/repo",
        }
    ]
    assert (
        thread_mount_rows_module.should_skip_session_folder(
            rows,
            dependencies=preparation_composition.thread_mount_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
        is True
    )


def test_should_skip_session_folder_empty_mounts():
    """No mounts → fall back to legacy session folder so the thread
    isn't left with zero cloud surfaces. This is the fallback case
    Phase 4 deliberately preserves (unattached sessions still get a
    folder).
    """
    import orchestrator.main

    assert (
        thread_mount_rows_module.should_skip_session_folder(
            [],
            dependencies=preparation_composition.thread_mount_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
        is False
    )


def test_should_skip_session_folder_mount_without_webdav_url():
    """A mount row exists but its ``webdav_url`` is None — e.g.
    backend wasn't initialized when the row was built, or
    user-home resolution failed transiently. The row is not a usable
    sync target, so don't skip the fallback. (Same observable-state
    safety net Phase 2 introduced.)
    """
    import orchestrator.main

    rows = [
        {
            "mount_kind": "project_default",
            "target_path": "",
            "webdav_url": None,
        },
        {
            "mount_kind": "project",
            "target_path": "projects/alpha",
            "webdav_url": "",  # empty string is also falsy
        },
    ]
    assert (
        thread_mount_rows_module.should_skip_session_folder(
            rows,
            dependencies=preparation_composition.thread_mount_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
        is False
    )


def test_should_skip_session_folder_returns_true_on_first_usable_mount():
    """At least one usable row is enough — predicate is short-circuit
    OR semantics. Verifies a mix of failed + working rows still
    skips the session folder.
    """
    import orchestrator.main

    rows = [
        {"mount_kind": "project", "target_path": "projects/a", "webdav_url": None},
        {
            "mount_kind": "project",
            "target_path": "projects/b",
            "webdav_url": "https://oc.test/dav/spaces/drive-b/",
        },
    ]
    assert (
        thread_mount_rows_module.should_skip_session_folder(
            rows,
            dependencies=preparation_composition.thread_mount_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
        is True
    )


@pytest.mark.asyncio
async def test_build_agent_cloud_mount_falls_back_to_session_folder(monkeypatch):
    """If a default user-home row lacks safe rclone credentials, the rclone
    payload uses the regular session folder instead of the eager home clone.
    """
    import orchestrator.main
    from orchestrator.services.cloud import (
        CloudBackendError,
        CloudBackendErrorKind,
        RcloneMountSpec,
    )

    class Backend:
        backend_id = "nextcloud"
        is_initialized = True

        async def build_rclone_mount_spec(self, *, mount_kind, **kwargs):
            if mount_kind != "session_folder":
                raise CloudBackendError(
                    CloudBackendErrorKind.NOT_SUPPORTED,
                    "missing explicit user-home credentials",
                    backend=self.backend_id,
                )
            return RcloneMountSpec(
                source_type="webdav",
                source_config={
                    "url": "https://nc.test/remote.php/dav/files/agent/session/",
                    "vendor": "nextcloud",
                    "user": "agent-service",
                },
                auth={"type": "basic", "password": "agent-pass"},
            )

    monkeypatch.setenv("CLOUD_WORKSPACE_DRIVER", "rclone_mount")
    router = MagicMock()
    _tb = Backend()
    router.for_thread.return_value = _tb
    router.for_thread_optional.return_value = _tb
    router.for_backend_instance.return_value = Backend()
    thread = {
        "id": "thread-1",
        "main_cloud_backend": "nextcloud",
        "main_cloud_backend_instance_id": _BACKEND_INSTANCE_ID,
        "main_cloud_session_handle": "sessions/thread-1",
    }
    rows = [
        {
            "id": "mount-home",
            "mount_kind": "project_default",
            "target_path": "",
            "source_ref": "p-default",
            "backend_id": "nextcloud",
            "backend_instance_id": _BACKEND_INSTANCE_ID,
            "cloud_handle": (
                '{"backend":"nextcloud","native_id":"home:alice",'
                '"vendor_meta":{"kind":"user_home","username":"alice"}}'
            ),
        }
    ]

    with patch("orchestrator.main.app.state.resources.main_cloud_router", router):
        payload = await agent_cloud_mounts_module._build_agent_cloud_mount(
            thread,
            mount_rows=rows,
            metadata={"vm": {"status": "ready", "ssh_host": "10.0.0.5"}},
            dependencies=workspace_composition.agent_cloud_mount_dependencies(
                orchestrator.main.app.state.resources
            ),
        )

    assert payload is not None
    assert payload["driver"] == "rclone"
    assert payload["fallback"] is True
    assert payload["required"] is False
    assert len(payload["mounts"]) == 1
    mount = payload["mounts"][0]
    assert mount["mount_kind"] == "session_folder"
    assert mount["target_path"] == "/cloud/home"
    assert mount["workspace_name"] == "home"


@pytest.mark.asyncio
async def test_build_agent_cloud_mount_uses_supported_thread_mount(monkeypatch):
    import orchestrator.main
    from orchestrator.services.cloud import RcloneMountSpec

    class Backend:
        backend_id = "nextcloud"
        is_initialized = True

        async def build_rclone_mount_spec(self, *, mount_kind, target_path, **kwargs):
            return RcloneMountSpec(
                source_type="webdav",
                source_config={
                    "url": "https://nc.test/remote.php/dav/files/alice/",
                    "vendor": "nextcloud",
                    "user": "alice",
                },
                auth={"type": "basic", "password": "app-pass"},
            )

    monkeypatch.setenv("CLOUD_WORKSPACE_DRIVER", "rclone_mount")
    router = MagicMock()
    router.for_backend_instance.return_value = Backend()
    rows = [
        {
            "id": "mount-home",
            "mount_kind": "project_default",
            "target_path": "",
            "source_ref": "p-default",
            "backend_id": "nextcloud",
            "backend_instance_id": _BACKEND_INSTANCE_ID,
            "cloud_handle": (
                '{"backend":"nextcloud","native_id":"home:alice",'
                '"vendor_meta":{"kind":"user_home","username":"alice"}}'
            ),
        }
    ]

    with patch("orchestrator.main.app.state.resources.main_cloud_router", router):
        payload = await agent_cloud_mounts_module._build_agent_cloud_mount(
            {"id": "thread-1"},
            mount_rows=rows,
            metadata={"vm": {"status": "ready", "ssh_host": "10.0.0.5"}},
            dependencies=workspace_composition.agent_cloud_mount_dependencies(
                orchestrator.main.app.state.resources
            ),
        )

    assert payload is not None
    assert payload["fallback"] is False
    assert payload["required"] is False
    assert payload["mounts"][0]["mount_kind"] == "project_default"
    assert payload["mounts"][0]["target_path"] == "/cloud/home"


@pytest.mark.asyncio
async def test_build_agent_cloud_mount_vm_runtime_is_readonly_and_public(monkeypatch):
    """A cross-cluster VM runtime mounts read-only (root tier) and requests the
    public WebDAV URL — the internal service URL isn't reachable from the vm
    cluster (knowledge-base/knowledge/issues/workspace_upgrade_drops_cloud_mount.md)."""
    import orchestrator.main
    from orchestrator.services.cloud import RcloneMountSpec

    captured: dict = {}

    class Backend:
        backend_id = "opencloud"
        is_initialized = True

        async def build_rclone_mount_spec(
            self, *, mount_kind, target_path, access, prefer_public_url=False, **kwargs
        ):
            captured["access"] = access
            captured["prefer_public_url"] = prefer_public_url
            return RcloneMountSpec(
                source_type="webdav",
                source_config={
                    "url": "https://cloud.public.test/dav/spaces/d/",
                    "vendor": "infinitescale",
                },
                auth={"type": "keycloak_client_credentials"},
            )

    monkeypatch.setenv("CLOUD_WORKSPACE_DRIVER", "rclone_mount")
    router = MagicMock()
    _tb = Backend()
    router.for_thread.return_value = _tb
    router.for_thread_optional.return_value = _tb
    thread = {
        "id": "t1",
        "main_cloud_backend": "opencloud",
        "main_cloud_backend_instance_id": _BACKEND_INSTANCE_ID,
        "main_cloud_session_handle": "sessions/t1",
    }

    with patch("orchestrator.main.app.state.resources.main_cloud_router", router):
        payload = await agent_cloud_mounts_module._build_agent_cloud_mount(
            thread,
            mount_rows=[],
            metadata={"vm": {"status": "ready", "ssh_host": "100.64.0.5"}},
            dependencies=workspace_composition.agent_cloud_mount_dependencies(
                orchestrator.main.app.state.resources
            ),
        )

    assert payload is not None
    assert captured["prefer_public_url"] is True
    assert captured["access"] == "read_only"
    assert payload["mounts"][0]["access"] == "read_only"


@pytest.mark.asyncio
async def test_build_agent_cloud_mount_pod_runtime_is_readwrite_and_internal(
    monkeypatch,
):
    """A same-cluster workspace pod keeps read-write + the internal URL (no
    public-edge hairpin, works on local k3d)."""
    import orchestrator.main
    from orchestrator.services.cloud import RcloneMountSpec

    captured: dict = {}

    class Backend:
        backend_id = "opencloud"
        is_initialized = True

        async def build_rclone_mount_spec(
            self, *, mount_kind, target_path, access, prefer_public_url=False, **kwargs
        ):
            captured["access"] = access
            captured["prefer_public_url"] = prefer_public_url
            return RcloneMountSpec(
                source_type="webdav",
                source_config={
                    "url": "http://srw-opencloud:9200/dav/spaces/d/",
                    "vendor": "infinitescale",
                },
                auth={"type": "keycloak_client_credentials"},
            )

    monkeypatch.setenv("CLOUD_WORKSPACE_DRIVER", "rclone_mount")
    monkeypatch.delenv("CLOUD_RCLONE_ALLOW_CONTAINER", raising=False)
    router = MagicMock()
    _tb = Backend()
    router.for_thread.return_value = _tb
    router.for_thread_optional.return_value = _tb
    thread = {
        "id": "t1",
        "main_cloud_backend": "opencloud",
        "main_cloud_backend_instance_id": _BACKEND_INSTANCE_ID,
        "main_cloud_session_handle": "sessions/t1",
    }

    with patch("orchestrator.main.app.state.resources.main_cloud_router", router):
        payload = await agent_cloud_mounts_module._build_agent_cloud_mount(
            thread,
            mount_rows=[],
            metadata={
                "workspace_container": {"status": "ready", "pod_ip": "10.42.0.10"}
            },
            dependencies=workspace_composition.agent_cloud_mount_dependencies(
                orchestrator.main.app.state.resources
            ),
        )

    assert payload is not None
    assert captured["prefer_public_url"] is False
    assert captured["access"] == "read_write"
    assert payload["mounts"][0]["access"] == "read_write"


@pytest.mark.asyncio
async def test_build_agent_cloud_mount_uses_container_runtime_by_default(monkeypatch):
    import orchestrator.main
    from orchestrator.services.cloud import RcloneMountSpec

    class Backend:
        backend_id = "nextcloud"
        is_initialized = True

        async def build_rclone_mount_spec(self, *, mount_kind, target_path, **kwargs):
            assert mount_kind == "session_folder"
            assert target_path == "/cloud/home"
            return RcloneMountSpec(
                source_type="webdav",
                source_config={
                    "url": "https://nc.test/remote.php/dav/files/agent/session/",
                    "vendor": "nextcloud",
                    "user": "agent-service",
                },
                auth={"type": "basic", "password": "agent-pass"},
            )

    monkeypatch.setenv("CLOUD_WORKSPACE_DRIVER", "rclone_mount")
    monkeypatch.delenv("CLOUD_RCLONE_ALLOW_CONTAINER", raising=False)
    router = MagicMock()
    _tb = Backend()
    router.for_thread.return_value = _tb
    router.for_thread_optional.return_value = _tb
    thread = {
        "id": "thread-1",
        "main_cloud_backend": "nextcloud",
        "main_cloud_backend_instance_id": _BACKEND_INSTANCE_ID,
        "main_cloud_session_handle": "sessions/thread-1",
    }

    with patch("orchestrator.main.app.state.resources.main_cloud_router", router):
        payload = await agent_cloud_mounts_module._build_agent_cloud_mount(
            thread,
            mount_rows=[],
            metadata={
                "workspace_container": {
                    "status": "ready",
                    "pod_ip": "10.42.0.10",
                }
            },
            dependencies=workspace_composition.agent_cloud_mount_dependencies(
                orchestrator.main.app.state.resources
            ),
        )

    assert payload is not None
    assert payload["driver"] == "rclone"
    assert payload["fallback"] is False
    assert payload["mounts"][0]["mount_kind"] == "session_folder"


@pytest.mark.asyncio
async def test_build_agent_cloud_mount_reconstructs_only_exact_terminal_runtime(
    monkeypatch,
):
    """End may retire an existing mount after Begin projects process-zero,
    without making that terminal workspace eligible for ordinary delivery.
    """
    import orchestrator.main
    from orchestrator.services.cloud import RcloneMountSpec

    runtime_incarnation = "33333333-3333-4333-8333-333333333333"
    fingerprint = "SHA256:" + ("A" * 43)

    class Backend:
        backend_id = "nextcloud"
        is_initialized = True

        async def build_rclone_mount_spec(self, *, mount_kind, target_path, **kwargs):
            assert mount_kind == "session_folder"
            assert target_path == "/cloud/home"
            return RcloneMountSpec(
                source_type="webdav",
                source_config={
                    "url": "https://nc.test/remote.php/dav/files/agent/session/",
                    "vendor": "nextcloud",
                    "user": "agent-service",
                },
                auth={"type": "basic", "password": "agent-pass"},
            )

    metadata = {
        "workspace_container": {
            "status": "retiring_process_zero",
            "provisioner": "k8s",
            "pod_ip": "10.42.0.10",
            "port": 30022,
            "_canvas_workspace_generation": _RUNTIME_GENERATION,
            "_runtime_incarnation": runtime_incarnation,
        },
        "_workspace_binding": {
            "generation": _RUNTIME_GENERATION,
            "kind": "remote",
            "backing_id": "k8s-pvc:agent-workspaces:pvc-uid",
            "ssh_host_key_fingerprint": fingerprint,
        },
        "_stateless_workspace_retirement_pending": True,
        "_stateless_claim_retirement": {
            "terminal_token": 8,
            "claimant_quiesced": True,
            "shell_retirement_required": True,
            "resident_cleanup_required": True,
            "residents_retired": False,
            "remote_retired": False,
            "permanent": True,
            "workspace_absence_proven": False,
            "workspace_generation": _RUNTIME_GENERATION,
            "endpoint_generation": _RUNTIME_GENERATION,
            "runtime_incarnation": runtime_incarnation,
            "host_key_fingerprint": fingerprint,
        },
    }
    thread = {
        "id": _THREAD_ID,
        "status": "ended",
        "execution_lane": "stateless",
        "main_cloud_backend": "nextcloud",
        "main_cloud_backend_instance_id": _BACKEND_INSTANCE_ID,
        "main_cloud_session_handle": f"sessions/{_THREAD_ID}",
        "metadata": metadata,
    }
    router = MagicMock()
    _tb = Backend()
    router.for_thread.return_value = _tb
    router.for_thread_optional.return_value = _tb
    monkeypatch.setenv("CLOUD_WORKSPACE_DRIVER", "rclone_mount")
    monkeypatch.delenv("CLOUD_RCLONE_ALLOW_CONTAINER", raising=False)

    with patch("orchestrator.main.app.state.resources.main_cloud_router", router):
        ordinary_payload = await agent_cloud_mounts_module._build_agent_cloud_mount(
            thread,
            mount_rows=[],
            metadata=metadata,
            dependencies=workspace_composition.agent_cloud_mount_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
        wrong_token_payload = await agent_cloud_mounts_module._build_agent_cloud_mount(
            thread,
            mount_rows=[],
            metadata=metadata,
            terminal_retirement_token=9,
            dependencies=workspace_composition.agent_cloud_mount_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
        terminal_payload = await agent_cloud_mounts_module._build_agent_cloud_mount(
            thread,
            mount_rows=[],
            metadata=metadata,
            terminal_retirement_token=8,
            dependencies=workspace_composition.agent_cloud_mount_dependencies(
                orchestrator.main.app.state.resources
            ),
        )

        metadata["_stateless_claim_retirement"]["runtime_incarnation"] = (
            "44444444-4444-4444-8444-444444444444"
        )
        mismatched_payload = await agent_cloud_mounts_module._build_agent_cloud_mount(
            thread,
            mount_rows=[],
            metadata=metadata,
            terminal_retirement_token=8,
            dependencies=workspace_composition.agent_cloud_mount_dependencies(
                orchestrator.main.app.state.resources
            ),
        )

    assert ordinary_payload is None
    assert wrong_token_payload is None
    assert terminal_payload is not None
    assert terminal_payload["driver"] == "rclone"
    assert terminal_payload["mounts"][0]["mount_kind"] == "session_folder"
    assert mismatched_payload is None


@pytest.mark.asyncio
async def test_build_agent_cloud_mount_container_runtime_can_be_disabled(monkeypatch):
    import orchestrator.main

    monkeypatch.setenv("CLOUD_WORKSPACE_DRIVER", "rclone_mount")
    monkeypatch.setenv("CLOUD_RCLONE_ALLOW_CONTAINER", "false")
    payload = await agent_cloud_mounts_module._build_agent_cloud_mount(
        {
            "id": "thread-1",
            "main_cloud_backend": "nextcloud",
            "main_cloud_session_handle": "sessions/thread-1",
        },
        mount_rows=[],
        metadata={
            "workspace_container": {
                "status": "ready",
                "pod_ip": "10.42.0.10",
            }
        },
        dependencies=workspace_composition.agent_cloud_mount_dependencies(
            orchestrator.main.app.state.resources
        ),
    )

    assert payload is None


# ---------------------------------------------------------------------------
# Phase 3a — multi-project mount path collision handling
# ---------------------------------------------------------------------------


def _multi_project_db(projects: list[dict]) -> MagicMock:
    """Build a fake postgres_db that resolves each project_id to a row."""
    fake_db = _fake_db()
    table = {p["id"]: p for p in projects}

    async def get_project(pid: str):
        return table.get(pid)

    fake_db.get_project = AsyncMock(side_effect=get_project)
    fake_db.get_project_members = AsyncMock(return_value=[_owner_member()])
    fake_db.get_user = AsyncMock(return_value=_owner_user_record())
    return fake_db


def _router_for_backend(backend: MagicMock) -> MagicMock:
    router = MagicMock()
    router.for_project.return_value = backend
    router.for_project_optional.return_value = backend
    router.for_backend.return_value = backend
    return router


@pytest.mark.asyncio
async def test_collision_two_same_named_projects_get_distinct_paths():
    """Two non-default projects with the same name → first wins ``projects/alpha``,
    second gets ``projects/alpha-2``. UNIQUE (thread_id, target_path) at
    persistence time always holds.
    """
    from orchestrator.services.thread_mount_rows import build_thread_mount_rows

    fake_db = _multi_project_db(
        [
            _project(project_id="p-1", name="Alpha"),
            _project(project_id="p-2", name="Alpha"),
        ]
    )
    backend = _backend()

    with (
        patch("orchestrator.main.app.state.resources.postgres_db", fake_db),
        patch(
            "orchestrator.main.app.state.resources.main_cloud_router",
            _router_for_backend(backend),
        ),
    ):
        rows = await build_thread_mount_rows(
            ["p-1", "p-2"],
            dependencies=preparation_composition.thread_mount_dependencies(
                orch_main.app.state.resources
            ),
        )

    assert len(rows) == 2
    assert [r["target_path"] for r in rows] == [
        "projects/alpha",
        "projects/alpha-2",
    ]
    assert [r["source_ref"] for r in rows] == ["p-1", "p-2"]


@pytest.mark.asyncio
async def test_collision_case_insensitive():
    """``_slugify_mount_name`` lowercases, so "Alpha" and "alpha" produce
    the same slug. Collision logic must still dedup the second one.
    """
    from orchestrator.services.thread_mount_rows import build_thread_mount_rows

    fake_db = _multi_project_db(
        [
            _project(project_id="p-1", name="Alpha"),
            _project(project_id="p-2", name="alpha"),
        ]
    )
    backend = _backend()

    with (
        patch("orchestrator.main.app.state.resources.postgres_db", fake_db),
        patch(
            "orchestrator.main.app.state.resources.main_cloud_router",
            _router_for_backend(backend),
        ),
    ):
        rows = await build_thread_mount_rows(
            ["p-1", "p-2"],
            dependencies=preparation_composition.thread_mount_dependencies(
                orch_main.app.state.resources
            ),
        )

    assert [r["target_path"] for r in rows] == [
        "projects/alpha",
        "projects/alpha-2",
    ]


@pytest.mark.asyncio
async def test_collision_three_same_named_projects():
    """Three same-named → ``alpha``, ``alpha-2``, ``alpha-3``. The suffix
    counter walks forward and doesn't reuse freed-up indices (none get
    freed in this scenario anyway).
    """
    from orchestrator.services.thread_mount_rows import build_thread_mount_rows

    fake_db = _multi_project_db(
        [
            _project(project_id="p-1", name="Alpha"),
            _project(project_id="p-2", name="Alpha"),
            _project(project_id="p-3", name="Alpha"),
        ]
    )
    backend = _backend()

    with (
        patch("orchestrator.main.app.state.resources.postgres_db", fake_db),
        patch(
            "orchestrator.main.app.state.resources.main_cloud_router",
            _router_for_backend(backend),
        ),
    ):
        rows = await build_thread_mount_rows(
            ["p-1", "p-2", "p-3"],
            dependencies=preparation_composition.thread_mount_dependencies(
                orch_main.app.state.resources
            ),
        )

    assert [r["target_path"] for r in rows] == [
        "projects/alpha",
        "projects/alpha-2",
        "projects/alpha-3",
    ]


@pytest.mark.asyncio
async def test_no_collision_unique_names_unaffected():
    """Sanity check: unique names don't acquire suffixes (regression guard
    in case the suffix loop is ever rewritten with an off-by-one).
    """
    from orchestrator.services.thread_mount_rows import build_thread_mount_rows

    fake_db = _multi_project_db(
        [
            _project(project_id="p-1", name="Alpha"),
            _project(project_id="p-2", name="Beta"),
            _project(project_id="p-3", name="Gamma"),
        ]
    )
    backend = _backend()

    with (
        patch("orchestrator.main.app.state.resources.postgres_db", fake_db),
        patch(
            "orchestrator.main.app.state.resources.main_cloud_router",
            _router_for_backend(backend),
        ),
    ):
        rows = await build_thread_mount_rows(
            ["p-1", "p-2", "p-3"],
            dependencies=preparation_composition.thread_mount_dependencies(
                orch_main.app.state.resources
            ),
        )

    assert [r["target_path"] for r in rows] == [
        "projects/alpha",
        "projects/beta",
        "projects/gamma",
    ]


@pytest.mark.asyncio
async def test_dedupe_repeated_project_id():
    """Same UUID twice in project_ids → one row, not two — and definitely
    not a row at ``projects/alpha`` plus a phantom ``projects/alpha-2``
    pointing at the same source_ref.
    """
    from orchestrator.services.thread_mount_rows import build_thread_mount_rows

    fake_db = _multi_project_db(
        [
            _project(project_id="p-1", name="Alpha"),
        ]
    )
    backend = _backend()

    with (
        patch("orchestrator.main.app.state.resources.postgres_db", fake_db),
        patch(
            "orchestrator.main.app.state.resources.main_cloud_router",
            _router_for_backend(backend),
        ),
    ):
        rows = await build_thread_mount_rows(
            ["p-1", "p-1", "p-1"],
            dependencies=preparation_composition.thread_mount_dependencies(
                orch_main.app.state.resources
            ),
        )

    assert len(rows) == 1
    assert rows[0]["target_path"] == "projects/alpha"
    assert rows[0]["source_ref"] == "p-1"


@pytest.mark.asyncio
async def test_collision_with_default_project_present():
    """Default project at workspace root + two collision-named non-default
    projects. The default's empty ``target_path`` doesn't share the
    namespace with non-defaults (which live under ``projects/``), so the
    suffix logic only fires between the non-defaults.
    """
    from orchestrator.services.thread_mount_rows import build_thread_mount_rows

    fake_db = _multi_project_db(
        [
            _project(project_id="p-default", is_default=True, name="My Home"),
            _project(project_id="p-1", name="Alpha"),
            _project(project_id="p-2", name="Alpha"),
        ]
    )
    backend = _backend()

    with (
        patch("orchestrator.main.app.state.resources.postgres_db", fake_db),
        patch(
            "orchestrator.main.app.state.resources.main_cloud_router",
            _router_for_backend(backend),
        ),
    ):
        rows = await build_thread_mount_rows(
            ["p-default", "p-1", "p-2"],
            dependencies=preparation_composition.thread_mount_dependencies(
                orch_main.app.state.resources
            ),
        )

    assert len(rows) == 3
    paths = [r["target_path"] for r in rows]
    assert "" in paths  # default at root
    assert "projects/alpha" in paths
    assert "projects/alpha-2" in paths
    # source_refs preserved in input order
    assert [r["source_ref"] for r in rows] == ["p-default", "p-1", "p-2"]


def test_protected_cloud_mount_payload_is_ro_lower_plus_overlay():
    row = {
        "backend": "nextcloud",
        "reader_id": "srw-reader-abc",
        "credentials": "app-pass-xyz",
        "webdav_url": "https://nc.internal/remote.php/dav/files/srw-reader-abc/Proj/",
        "auth_kind": "basic",
        "status": "active",
    }
    payload = agent_cloud_mounts_module._build_protected_cloud_mount(
        row, thread_id="thread-1"
    )
    assert payload["driver"] == "rclone"
    assert payload["protected"] is True
    assert payload["required"] is True
    # overlay layout obeys the snapshot placement rule (design §11.3)
    ov = payload["overlay"]
    assert ov["upper"].startswith("/home/agent-host/.overlay")
    assert ov["work"] == "/home/agent-host/.overlay/work"
    assert ov["merged"] == "/cloud/merged"
    assert ov["lower"] == "/cloud/lower"
    assert ov["quota_bytes"] == 8 * 1024**3
    assert isinstance(ov["quota_bytes"], int)
    # single RO lower mount, reader creds (NOT agent-service), read_only
    assert len(payload["mounts"]) == 1
    m = payload["mounts"][0]
    assert m["access"] == "read_only"
    assert m["target_path"] == "/cloud/lower"
    assert m["source"]["config"]["url"] == row["webdav_url"]
    assert m["source"]["config"]["user"] == "srw-reader-abc"
    assert m["auth"] == {"type": "basic", "password": "app-pass-xyz"}
    # tell the agent NOT to install workspace/cloud -> lower; the overlay owns it
    assert payload["skip_workspace_links"] is True


def test_protected_cloud_mount_none_for_inactive_or_non_nextcloud():
    assert (
        agent_cloud_mounts_module._build_protected_cloud_mount(
            {"status": "revoked", "backend": "nextcloud"}, thread_id="t"
        )
        is None
    )
    assert (
        agent_cloud_mounts_module._build_protected_cloud_mount(
            {"status": "active", "backend": "opencloud"}, thread_id="t"
        )
        is None
    )


# ---------------------------------------------------------------------------
# F1/F4 (B8 review) — _build_agent_cloud_mount fail-closed matrix for the
# protected_cloud marker. The marker ALONE must route into the protected
# branch; the branch must never fall through to the LIVE builders below it.
# ---------------------------------------------------------------------------

_ACTIVE_NC_ROW = {
    "backend": "nextcloud",
    "reader_id": "srw-reader-abc",
    "credentials": "app-pass-xyz",
    "webdav_url": "https://nc.internal/remote.php/dav/files/srw-reader-abc/Proj/",
    "auth_kind": "basic",
    "status": "active",
    "runtime_generation": _RUNTIME_GENERATION,
}

# A live thread_mount row: if the protected branch ever fell through to the
# live builders (the pre-fix bug), this would resolve to a real rw payload
# instead of None/protected — every matrix case below asserts that never
# happens regardless of what live rows are present.
_LIVE_MOUNT_ROWS = [
    {
        "id": "mount-home",
        "mount_kind": "project_default",
        "target_path": "",
        "source_ref": "p-default",
        "backend_id": "nextcloud",
        "cloud_handle": (
            '{"backend":"nextcloud","native_id":"home:alice",'
            '"vendor_meta":{"kind":"user_home","username":"alice"}}'
        ),
    }
]


@pytest.mark.asyncio
async def test_build_agent_cloud_mount_protected_marker_flag_off_returns_none(
    monkeypatch,
):
    """(a) marker present + flag OFF -> None, never the live builders."""
    import orchestrator.main

    monkeypatch.setenv("CLOUD_WORKSPACE_DRIVER", "rclone_mount")
    monkeypatch.delenv("CLOUD_RCLONE_ALLOW_CONTAINER", raising=False)
    with patch(
        "orchestrator.services.deployment_gates.is_protected_cloud_mode_enabled",
        return_value=False,
    ):
        payload = await agent_cloud_mounts_module._build_agent_cloud_mount(
            {
                "id": _THREAD_ID,
                "status": "active",
                "runtime_generation": _RUNTIME_GENERATION,
                "runtime_retirement_token": None,
            },
            mount_rows=_LIVE_MOUNT_ROWS,
            metadata={
                "protected_cloud": True,
                "workspace_container": {"status": "ready", "pod_ip": "10.42.0.10"},
            },
            dependencies=workspace_composition.agent_cloud_mount_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
    assert payload is None


@pytest.mark.asyncio
async def test_build_agent_cloud_mount_protected_marker_vm_tier_returns_none(
    monkeypatch,
):
    """(b) marker + flag ON + VM-ready metadata -> None (v1 is
    container-runtime-only; the reader webdav_url is internal-only)."""
    import orchestrator.main

    monkeypatch.setenv("CLOUD_WORKSPACE_DRIVER", "rclone_mount")
    with patch(
        "orchestrator.services.deployment_gates.is_protected_cloud_mode_enabled",
        return_value=True,
    ):
        payload = await agent_cloud_mounts_module._build_agent_cloud_mount(
            {"id": "thread-1"},
            mount_rows=_LIVE_MOUNT_ROWS,
            metadata={
                "protected_cloud": True,
                "vm": {"status": "ready", "ssh_host": "10.0.0.5"},
            },
            dependencies=workspace_composition.agent_cloud_mount_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
    assert payload is None


@pytest.mark.asyncio
async def test_build_agent_cloud_mount_protected_marker_active_row_returns_payload(
    monkeypatch,
):
    """(c) marker + flag ON + container runtime + active NC row -> the
    RO-lower + overlay payload, protected=True."""
    import orchestrator.main

    monkeypatch.setenv("CLOUD_WORKSPACE_DRIVER", "rclone_mount")
    monkeypatch.delenv("CLOUD_RCLONE_ALLOW_CONTAINER", raising=False)
    with (
        patch(
            "orchestrator.services.deployment_gates.is_protected_cloud_mode_enabled",
            return_value=True,
        ),
        patch(
            "orchestrator.main.app.state.resources.postgres_db.get_ro_mount_by_thread",
            new=AsyncMock(return_value=_ACTIVE_NC_ROW),
        ),
    ):
        payload = await agent_cloud_mounts_module._build_agent_cloud_mount(
            {
                "id": _THREAD_ID,
                "status": "active",
                "runtime_generation": _RUNTIME_GENERATION,
                "runtime_retirement_token": None,
            },
            mount_rows=[],
            metadata={
                "protected_cloud": True,
                "workspace_container": {"status": "ready", "pod_ip": "10.42.0.10"},
            },
            dependencies=workspace_composition.agent_cloud_mount_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
    assert payload is not None
    assert payload["protected"] is True
    assert payload["required"] is True
    assert payload["mounts"][0]["access"] == "read_only"
    assert payload["mounts"][0]["source"]["config"]["user"] == "srw-reader-abc"


@pytest.mark.asyncio
async def test_build_agent_cloud_mount_protected_marker_no_row_returns_none(
    monkeypatch,
):
    """(d) marker + flag ON + container runtime + no active row -> None.

    No task is registered in ``cloud_task_registry.protected_engage_tasks``
    for this thread_id
    and no ``protected_cloud_error`` is recorded, so this exercises F-I1's
    poll-exhaustion path (3x get_ro_mount_by_thread, sleep(3) between) —
    ``asyncio.sleep`` is patched so the 9s worst case doesn't slow the suite.
    Fail-closed: exhausting the poll still returns None, never a live mount.
    """
    import orchestrator.main

    monkeypatch.setenv("CLOUD_WORKSPACE_DRIVER", "rclone_mount")
    monkeypatch.delenv("CLOUD_RCLONE_ALLOW_CONTAINER", raising=False)
    # Defensive: no in-flight engage task registered for this thread_id (a
    # leaked registration from another test would take the await-task branch
    # instead of the poll branch this test targets).
    orchestrator.main.app.state.resources.cloud_task_registry.protected_engage_tasks.pop(
        (_THREAD_ID, _RUNTIME_GENERATION), None
    )
    with (
        patch(
            "orchestrator.services.deployment_gates.is_protected_cloud_mode_enabled",
            return_value=True,
        ),
        patch(
            "orchestrator.main.app.state.resources.postgres_db.get_ro_mount_by_thread",
            new=AsyncMock(return_value=None),
        ) as get_row,
        patch("asyncio.sleep", new=AsyncMock()) as sleep,
    ):
        payload = await agent_cloud_mounts_module._build_agent_cloud_mount(
            {
                "id": _THREAD_ID,
                "status": "active",
                "runtime_generation": _RUNTIME_GENERATION,
                "runtime_retirement_token": None,
            },
            mount_rows=[],
            metadata={
                "protected_cloud": True,
                "workspace_container": {"status": "ready", "pod_ip": "10.42.0.10"},
            },
            dependencies=workspace_composition.agent_cloud_mount_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
    assert payload is None
    # Initial lookup + 3 poll attempts.
    assert get_row.await_count == 4
    assert sleep.await_count == 3
