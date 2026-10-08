"""Characterization + wire contracts for the extracted agent cloud payloads.

R1.B04 lane M. These pin the behaviours the move could quietly change:

* the protected branch is entered by the **marker alone**, and every refusal
  inside it returns ``None`` — never a live (agent-service credentialed)
  mount;
* the engage registry is consulted with the exact
  ``(thread_id, runtime_generation)`` key, and awaiting an in-flight task is
  preferred over the bare poll;
* an unresolvable installation is skipped, never substituted with the active
  backend;
* every injected collaborator is read from the dependencies object rather
  than a module global, so the application can rebind it per invocation.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from orchestrator.services import agent_cloud_mounts as mounts
from orchestrator.services.cloud import FeatureNotAvailable

THREAD_ID = "22222222-2222-4222-8222-222222222222"
GENERATION = "11111111-1111-4111-8111-111111111111"


# --------------------------------------------------------------------------- #
# Doubles
# --------------------------------------------------------------------------- #


class _Registry:
    """The protected-engage half of the contract's ``CloudTaskRegistry``."""

    def __init__(self) -> None:
        self._tasks: dict[tuple[str, str], asyncio.Task] = {}

    def protected_engage_get(self, key):
        return self._tasks.get(key)

    def protected_engage_register(self, key, task):
        self._tasks[key] = task

        def _done(finished):
            if self._tasks.get(key) is finished:
                self._tasks.pop(key, None)

        task.add_done_callback(_done)

    @property
    def protected_engage_tasks(self):
        return self._tasks


def _deps(
    *,
    store=None,
    cloud_router=None,
    cloud_tasks=None,
    protected_enabled: bool = True,
    driver: str = "rclone_mount",
    slugify=None,
) -> mounts.AgentCloudMountDependencies:
    return mounts.AgentCloudMountDependencies(
        store=store if store is not None else SimpleNamespace(),
        cloud_router=cloud_router if cloud_router is not None else SimpleNamespace(),
        cloud_tasks=cloud_tasks if cloud_tasks is not None else _Registry(),
        is_protected_cloud_mode_enabled=lambda: protected_enabled,
        cloud_workspace_driver=lambda: driver,
        slugify_mount_name=slugify or (lambda name: name.lower()),
    )


def _ready_container_metadata(**over: Any) -> dict[str, Any]:
    metadata = {"workspace_container": {"status": "ready", "pod_ip": "10.0.0.1"}}
    metadata.update(over)
    return metadata


def _protected_thread(**over: Any) -> dict[str, Any]:
    thread = {
        "id": THREAD_ID,
        "status": "created",
        "execution_lane": "pinned",
        "runtime_generation": GENERATION,
        "runtime_retirement_token": None,
        "user_id": "user-1",
    }
    thread.update(over)
    return thread


def _active_ro_row() -> dict[str, Any]:
    return {
        "status": "active",
        "backend": "nextcloud",
        "webdav_url": "https://nc.internal/remote.php/dav/files/reader/Proj/",
        "reader_id": "reader",
        "credentials": "reader-secret",
    }


# --------------------------------------------------------------------------- #
# _build_protected_cloud_mount — pure payload
# --------------------------------------------------------------------------- #


def test_protected_mount_uses_the_reader_credential_and_overlay_layout():
    payload = mounts._build_protected_cloud_mount(_active_ro_row(), thread_id=THREAD_ID)

    assert payload["protected"] is True
    assert payload["required"] is True
    assert payload["skip_workspace_links"] is True
    assert payload["fallback"] is False
    assert payload["overlay"] == {
        "lower": mounts._PROTECTED_LOWER_TARGET,
        "upper": mounts._PROTECTED_OVERLAY_UPPER,
        "work": mounts._PROTECTED_OVERLAY_WORK,
        "merged": mounts._PROTECTED_OVERLAY_MERGED,
        "quota_bytes": mounts._PROTECTED_UPPERDIR_QUOTA_BYTES,
    }
    (mount,) = payload["mounts"]
    assert mount["access"] == "read_only"
    assert mount["source"]["config"]["user"] == "reader"
    # The reader credential, never an agent-service one.
    assert mount["auth"] == {"type": "basic", "password": "reader-secret"}


@pytest.mark.parametrize(
    "row",
    [
        {},
        {"status": "revoked", "backend": "nextcloud"},
        {"status": "active", "backend": "opencloud"},
    ],
)
def test_protected_mount_is_none_unless_active_nextcloud(row):
    assert mounts._build_protected_cloud_mount(row, thread_id=THREAD_ID) is None


# --------------------------------------------------------------------------- #
# _build_agent_cloud_mount — the protected branch
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_protected_marker_with_flag_off_returns_no_mount_at_all():
    """Flag off => protected threads get NO cloud, never the live builders."""
    dependencies = _deps(protected_enabled=False, store=SimpleNamespace())

    payload = await mounts._build_agent_cloud_mount(
        _protected_thread(),
        mount_rows=[{"backend_id": "nextcloud", "cloud_handle": "{}"}],
        metadata=_ready_container_metadata(protected_cloud=True),
        dependencies=dependencies,
    )

    assert payload is None


@pytest.mark.asyncio
async def test_malformed_protected_marker_returns_none():
    dependencies = _deps()

    payload = await mounts._build_agent_cloud_mount(
        _protected_thread(),
        mount_rows=None,
        metadata=_ready_container_metadata(protected_cloud="yes"),
        dependencies=dependencies,
    )

    assert payload is None


@pytest.mark.asyncio
async def test_protected_marker_on_vm_tier_returns_none():
    dependencies = _deps()

    payload = await mounts._build_agent_cloud_mount(
        _protected_thread(),
        mount_rows=None,
        metadata={
            "protected_cloud": True,
            "vm": {"status": "ready", "ssh_host": "10.1.1.1"},
        },
        dependencies=dependencies,
    )

    assert payload is None


@pytest.mark.asyncio
async def test_protected_without_runtime_authority_refuses():
    store = SimpleNamespace(get_ro_mount_by_thread=AsyncMock(return_value=None))
    dependencies = _deps(store=store)

    payload = await mounts._build_agent_cloud_mount(
        _protected_thread(runtime_generation=None),
        mount_rows=None,
        metadata=_ready_container_metadata(protected_cloud=True),
        dependencies=dependencies,
    )

    assert payload is None
    store.get_ro_mount_by_thread.assert_not_awaited()


@pytest.mark.asyncio
async def test_protected_awaits_the_inflight_engage_task_for_this_generation():
    """The registry key is the exact ``(thread_id, generation)`` pair, and an
    in-flight task is awaited instead of polled."""
    registry = _Registry()
    started = asyncio.Event()

    async def _engage() -> None:
        started.set()
        await asyncio.sleep(0)

    task = asyncio.create_task(_engage())
    registry.protected_engage_register((THREAD_ID, GENERATION), task)
    await started.wait()

    rows = iter([None, _active_ro_row()])
    store = SimpleNamespace(
        get_ro_mount_by_thread=AsyncMock(side_effect=lambda _tid: next(rows))
    )
    dependencies = _deps(store=store, cloud_tasks=registry)

    payload = await mounts._build_agent_cloud_mount(
        _protected_thread(),
        mount_rows=None,
        metadata=_ready_container_metadata(protected_cloud=True),
        dependencies=dependencies,
    )

    assert payload is not None and payload["protected"] is True
    assert store.get_ro_mount_by_thread.await_count == 2


@pytest.mark.asyncio
async def test_protected_skips_the_poll_when_an_error_is_already_recorded():
    store = SimpleNamespace(get_ro_mount_by_thread=AsyncMock(return_value=None))
    dependencies = _deps(store=store)

    payload = await mounts._build_agent_cloud_mount(
        _protected_thread(),
        mount_rows=None,
        metadata=_ready_container_metadata(
            protected_cloud=True, protected_cloud_error="refused"
        ),
        dependencies=dependencies,
    )

    assert payload is None
    # Exactly one read: the poll fallback is suppressed by the terminal error.
    assert store.get_ro_mount_by_thread.await_count == 1


@pytest.mark.asyncio
async def test_terminal_token_never_waits_for_an_engage_task():
    """End reconstructs an already-mounted grant; it must not schedule/await."""
    registry = _Registry()
    store = SimpleNamespace(get_ro_mount_by_thread=AsyncMock(return_value=None))
    dependencies = _deps(store=store, cloud_tasks=registry)
    thread = _protected_thread(status="ended", execution_lane="stateless")
    metadata = {
        "protected_cloud": True,
        "workspace_container": {"status": "retiring_process_zero"},
    }

    async def _never() -> None:  # pragma: no cover - must not be awaited
        raise AssertionError("terminal reconstruction must not await engage")

    task = asyncio.create_task(_never())
    registry.protected_engage_register((THREAD_ID, GENERATION), task)

    payload = await mounts._build_agent_cloud_mount(
        thread,
        mount_rows=None,
        metadata=metadata,
        terminal_retirement_token=7,
        dependencies=dependencies,
    )

    # The narrower terminal gate refuses this shape outright, so nothing is
    # read and nothing is awaited.
    assert payload is None
    store.get_ro_mount_by_thread.assert_not_awaited()
    task.cancel()


# --------------------------------------------------------------------------- #
# Runtime gates
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("driver", ["sync", "", "rclone"])
def test_rclone_support_requires_the_rclone_mount_driver(driver):
    dependencies = _deps(driver=driver)
    assert (
        mounts._runtime_supports_rclone_mount(
            _ready_container_metadata(), dependencies=dependencies
        )
        is False
    )


def test_rclone_support_accepts_a_ready_vm_or_container():
    dependencies = _deps()
    assert mounts._runtime_supports_rclone_mount(
        {"vm": {"status": "ready", "ssh_host": "h"}}, dependencies=dependencies
    )
    assert mounts._runtime_supports_rclone_mount(
        _ready_container_metadata(), dependencies=dependencies
    )
    assert not mounts._runtime_supports_rclone_mount(
        {"workspace_container": {"status": "creating"}}, dependencies=dependencies
    )


def test_container_runtime_can_be_disabled_by_env(monkeypatch):
    monkeypatch.setenv("CLOUD_RCLONE_ALLOW_CONTAINER", "false")
    dependencies = _deps()
    assert not mounts._runtime_supports_rclone_mount(
        _ready_container_metadata(), dependencies=dependencies
    )


def test_terminal_retirement_gate_refuses_a_live_pinned_thread():
    dependencies = _deps()
    metadata = _ready_container_metadata()
    assert not mounts._runtime_supports_terminal_rclone_retirement(
        _protected_thread(), metadata, terminal_token=1, dependencies=dependencies
    )


# --------------------------------------------------------------------------- #
# Mount naming + row builders
# --------------------------------------------------------------------------- #


def test_mount_name_uses_the_injected_slugifier_and_suffixes_collisions():
    calls: list[str] = []

    def slugify(name: str) -> str:
        calls.append(name)
        return "proj"

    dependencies = _deps(slugify=slugify)
    used: set[str] = set()

    first = mounts._cloud_mount_name(
        {"target_path": "/a/Proj"}, used, dependencies=dependencies
    )
    second = mounts._cloud_mount_name(
        {"target_path": "/b/Proj"}, used, dependencies=dependencies
    )
    home = mounts._cloud_mount_name(
        {"mount_kind": "project_default"}, used, dependencies=dependencies
    )

    assert (first, second, home) == ("proj", "proj-2", "home")
    assert calls == ["Proj", "Proj"]


@pytest.mark.asyncio
async def test_rclone_row_builder_skips_an_unresolvable_installation():
    """An installation this replica cannot resolve is skipped — never replaced
    with the active backend."""

    def for_backend_instance(_id, *, expected_backend_id):
        raise FeatureNotAvailable("instance", backend=expected_backend_id)

    dependencies = _deps(
        cloud_router=SimpleNamespace(for_backend_instance=for_backend_instance)
    )

    built = await mounts._build_rclone_mount_from_row(
        {"backend_id": "nextcloud", "cloud_handle": "{}", "id": "m1"},
        workspace_name="home",
        dependencies=dependencies,
    )

    assert built is None


# --------------------------------------------------------------------------- #
# _resolve_cloud_session_url / _build_agent_cloud_sync
# --------------------------------------------------------------------------- #


def test_session_url_is_none_when_the_thread_backend_is_uninitialized():
    router = SimpleNamespace(
        for_thread_optional=lambda _t: SimpleNamespace(
            is_initialized=False, backend_id="nextcloud"
        )
    )
    dependencies = _deps(cloud_router=router)

    assert (
        mounts._resolve_cloud_session_url(
            {"nc_session_folder": "nextcloud:1"}, dependencies=dependencies
        )
        is None
    )


def test_session_url_falls_back_to_the_project_default_mount():
    backend = SimpleNamespace(
        is_initialized=True,
        backend_id="nextcloud",
        get_project_folder_browser_url=lambda _h: "https://cloud/f/9",
    )
    router = SimpleNamespace(
        for_thread_optional=lambda _t: None,
        for_backend_instance=lambda _id, *, expected_backend_id: backend,
    )
    dependencies = _deps(cloud_router=router)

    url = mounts._resolve_cloud_session_url(
        {},
        [
            {"mount_kind": "project", "backend_id": "nextcloud", "cloud_handle": "x"},
            {
                "mount_kind": "project_default",
                "backend_id": "nextcloud",
                "backend_instance_id": "i1",
                "cloud_handle": '{"backend":"nextcloud","native_id":"9"}',
            },
        ],
        dependencies=dependencies,
    )

    assert url == "https://cloud/f/9"


def test_cloud_sync_is_none_when_nothing_resolves():
    router = SimpleNamespace(
        for_thread_optional=lambda _t: None,
        for_backend_instance=lambda *_a, **_k: (_ for _ in ()).throw(
            FeatureNotAvailable("instance", backend="nextcloud")
        ),
    )
    dependencies = _deps(cloud_router=router)

    assert (
        mounts._build_agent_cloud_sync(
            {},
            mount_rows=[
                {
                    "backend_id": "nextcloud",
                    "webdav_url": "https://nc/dav",
                    "backend_instance_id": "i1",
                }
            ],
            dependencies=dependencies,
        )
        is None
    )


def test_cloud_sync_emits_v2_with_basic_auth_for_nextcloud():
    """The adapter builds the entry (``cloud_sync_config``), not this module."""
    from orchestrator.services.cloud import NextcloudBackend, NextcloudSettings

    backend = NextcloudBackend(
        NextcloudSettings(
            base_url="http://nc.internal",
            public_url="https://nc",
            admin_user="admin",
            admin_password="admin-pw",
            agent_user="agent",
            agent_password="pw",
        )
    )
    backend._initialized = True
    router = SimpleNamespace(
        for_thread_optional=lambda _t: None,
        for_backend_instance=lambda _id, *, expected_backend_id: backend,
    )
    dependencies = _deps(cloud_router=router)

    payload = mounts._build_agent_cloud_sync(
        {},
        mount_rows=[
            {
                "id": "m1",
                "mount_kind": "project",
                "target_path": "P",
                "backend_id": "nextcloud",
                "backend_instance_id": "i1",
                "webdav_url": "https://nc/dav",
            }
        ],
        dependencies=dependencies,
    )

    assert payload["version"] == 2
    assert payload["session_folder"] is None
    (mount,) = payload["mounts"]
    assert mount["auth"] == {"type": "basic", "username": "agent", "password": "pw"}


def test_cloud_sync_skips_a_row_without_transport_details():
    router = SimpleNamespace(for_thread_optional=lambda _t: None)
    dependencies = _deps(cloud_router=router)

    assert (
        mounts._build_agent_cloud_sync(
            {},
            mount_rows=[{"backend_id": "nextcloud"}, {"webdav_url": "https://nc/dav"}],
            dependencies=dependencies,
        )
        is None
    )


# --------------------------------------------------------------------------- #
# Per-invocation dependency resolution
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_a_rebound_router_is_observed_by_the_next_call():
    """The dependencies object is rebuilt per invocation, so swapping the
    application's router between calls changes the answer."""
    live = SimpleNamespace(
        is_initialized=True,
        backend_id="nextcloud",
        get_session_folder_browser_url=lambda _h: "https://one/",
    )
    state = {"router": SimpleNamespace(for_thread_optional=lambda _t: live)}

    def factory():
        return _deps(cloud_router=state["router"])

    thread = {"nc_session_folder": "nextcloud:1"}
    assert (
        mounts._resolve_cloud_session_url(thread, dependencies=factory())
        == "https://one/"
    )

    state["router"] = SimpleNamespace(for_thread_optional=lambda _t: None)
    assert mounts._resolve_cloud_session_url(thread, dependencies=factory()) is None
