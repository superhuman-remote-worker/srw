"""Unit tests for the legacy thread mount rows (``thread_mount_rows``).

Phase 2 of ``knowledge-base/knowledge/features/cloud_collaboration_model.md``
§9 introduced the ``project_default`` row shape (a default project mounting
its owner's cloud home); main cloud slice 4 replaced it with the user-root
connector, so a default project writes no row any more, while rows written
before still count for the project scope. Since slice 8 nothing writes a
row; these tests cover what still reads them (the project scope, End's
terminal reconstruction, the protected payload) without standing up the full
thread-create path.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import orchestrator.main
from orchestrator.application import preparation as preparation_composition
from orchestrator.application import workspace as workspace_composition
from orchestrator.services import agent_cloud_mounts as agent_cloud_mounts_module
from orchestrator.services import thread_mount_rows as thread_mount_rows_module


_BACKEND_INSTANCE_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
_THREAD_ID = "11111111-1111-4111-8111-111111111111"
_RUNTIME_GENERATION = "22222222-2222-4222-8222-222222222222"


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
async def test_thread_project_ids_answers_from_the_column_and_writes_no_row():
    """A session without rows keeps its scope, and none is written for it: its
    cloud folders are its connectors (main cloud slice 8)."""
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

    with patch("orchestrator.main.app.state.resources.postgres_db", fake_db):
        project_ids = await thread_mount_rows_module.thread_project_ids(
            "thread-1",
            dependencies=preparation_composition.thread_mount_dependencies(
                orchestrator.main.app.state.resources
            ),
        )

    assert project_ids == ["p-default"]
    fake_db.replace_thread_mounts.assert_not_awaited()


# The connector rule's deliveries (each attached cloud connector on its own,
# no session folder in a missing folder's place) are pinned in
# tests/test_cloud_connector_mounts.py (main cloud slice 3).


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
