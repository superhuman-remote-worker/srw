"""Unit tests for protected-cloud engage wiring in ``orchestrator/main.py``.

Task B8 ties Slice A's fail-closed ``engage_ro_mount`` gate to thread create:
a protected thread with a Nextcloud project mount gets engaged once, and a
refusal is recorded on the thread's metadata WITHOUT raising — the session
must still boot (with no cloud mount), never fall back to a live mount.

R1.B04 moved the engage machinery to ``services.protected_cloud_engage`` and
the mount builders to ``services.agent_cloud_mounts``; ``main`` keeps thin
wrappers for the callers it still owns. So a patch target here is chosen by
who resolves the name:

* names still resolved *inside* ``main`` (``_schedule_protected_engage`` as
  ``resume_thread`` calls it, ``_reserve_session_attach_binding``) stay
  patched on ``main``;
* names a moved function resolves in its own module (``engage_ro_mount``,
  ``_protected_cloud_delivery_state``, ``_record_protected_error``,
  ``_engage_protected_cloud_for_thread``) are patched on that module —
  patching them on ``main`` would silently not intercept;
* collaborators main's dependency factories read live (``postgres_db``,
  ``main_cloud_router``, ``_is_protected_cloud_mode_enabled``) stay patched on
  ``main``, and the dependencies object is built inside the patch stack.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import orchestrator.main

# NOTE: import via the bare ``services.cloud...`` path, matching main.py's own
# import (``from services.cloud.ro_engage import ...`` — see main.py's cloud
# imports and the module-identity note in the docstring below). ``orchestrator``
# is on sys.path as its own root (conftest.py) AND importable as a package
# (``orchestrator.services.cloud.ro_engage``), so the two import spellings
# resolve to two DIFFERENT module objects with two different (non-`is`-equal)
# ``RoEngageRefused`` classes. Importing the "orchestrator."-prefixed spelling
# here made ``except RoEngageRefused`` in main.py never match the instance this
# test's side_effect raises, silently mis-testing the refusal path (it fell
# through to the generic ``except Exception`` branch instead).
from orchestrator.services import protected_cloud_engage as engage_service
from orchestrator.services.cloud.ro_engage import RoEngageRefused
from orchestrator.services.cloud.protected_reader_authority import (
    ProtectedNextcloudReaderGrantPlan,
)
from orchestrator.services.cloud_staging.source_identity import (
    ProtectedMountSourceIdentity,
)
from orchestrator.application import sessions as sessions_composition
from orchestrator.application import workspace as workspace_composition
from orchestrator.services import agent_cloud_mounts as agent_cloud_mounts_module
from orchestrator.services import deployment_gates as deployment_gates_module
from orchestrator.services import (
    protected_cloud_engage as protected_cloud_engage_module,
)
from orchestrator.services import (
    session_attach_binding as session_attach_binding_module,
)
import fastapi as fastapi_module


@asynccontextmanager
async def _owned_workspace_lifecycle_lock(*_args, **_kwargs):
    yield True


def _capture_background_tasks():
    """Capture route-owned tasks so mocks remain installed through completion."""

    tasks = []
    native_create_task = asyncio.create_task

    def create_task(coro, *args, **kwargs):
        task = native_create_task(coro, *args, **kwargs)
        tasks.append(task)
        return task

    return tasks, create_task


_ENGAGE_THREAD_ID = "22222222-2222-4222-8222-222222222222"
_ENGAGE_GENERATION = "11111111-1111-4111-8111-111111111111"
_ENGAGE_TASK_KEY = (_ENGAGE_THREAD_ID, _ENGAGE_GENERATION)
_BACKEND_INSTANCE_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
_SOURCE_REF = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
_SOURCE = ProtectedMountSourceIdentity(
    backend_instance_id=_BACKEND_INSTANCE_ID,
    source_ref=_SOURCE_REF,
    target_path="projects/example",
    native_id="17",
    mountpoint="Proj",
)
_PROTECTED_MOUNT_ROWS = [
    {
        "id": "33333333-3333-4333-8333-333333333333",
        "mount_kind": "project",
        "backend_id": "nextcloud",
        "backend_instance_id": _BACKEND_INSTANCE_ID,
        "source_kind": "project_folder",
        "source_ref": _SOURCE_REF,
        "target_path": "projects/example",
        "cloud_handle": (
            '{"backend":"nextcloud","native_id":"17",'
            '"vendor_meta":{"mountpoint":"Proj"}}'
        ),
    }
]


def _engage_task_registry() -> dict:
    """The live protected-engage slot table.

    R1.B04 replaced main's ``_protected_engage_tasks`` module dict with the
    ``protected_engage`` half of the application-owned ``CloudTaskRegistry``.
    The property hands back the *same* dict the code reads, so seeding a
    sentinel and asserting eviction both still work.
    """
    return (
        orchestrator.main.app.state.resources.cloud_task_registry.protected_engage_tasks
    )


def _engage_runtime_thread() -> dict:
    return {
        "id": _ENGAGE_THREAD_ID,
        "status": "created",
        "execution_lane": "pinned",
        "runtime_generation": _ENGAGE_GENERATION,
        "runtime_retirement_token": None,
        "user_id": "user-1",
        "metadata": {"protected_cloud": True},
    }


def _active_ro_row(*, attempt: str = "44444444-4444-4444-8444-444444444444") -> dict:
    plan = ProtectedNextcloudReaderGrantPlan(
        engage_attempt=attempt,
        backend_instance_id=_BACKEND_INSTANCE_ID,
        source=_SOURCE,
    )
    return {
        "id": "55555555-5555-4555-8555-555555555555",
        "thread_id": _ENGAGE_THREAD_ID,
        "user_id": "user-1",
        "backend": "nextcloud",
        "backend_instance_id": _BACKEND_INSTANCE_ID,
        "reader_id": plan.reader_id,
        "grant_group_id": plan.group_id,
        "credentials": "app-pass-xyz",
        "webdav_url": (
            f"https://nc.internal/remote.php/dav/files/{plan.reader_id}/Proj/"
        ),
        "auth_kind": "basic",
        "status": "active",
        "etag_baseline": {},
        "runtime_generation": _ENGAGE_GENERATION,
        "engage_attempt": attempt,
        "selected_mount_id": _PROTECTED_MOUNT_ROWS[0]["id"],
        "source_binding": _SOURCE.binding,
        "source_binding_sha256": _SOURCE.sha256,
        "grant_handle": plan.grant_handle,
        "grant_handle_sha256": plan.grant_handle_sha256,
    }


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("id", None),
        ("engage_attempt", None),
        ("etag_baseline", None),
        ("credentials", ""),
        ("reader_id", ""),
        ("webdav_url", ""),
        ("auth_kind", "bearer"),
    ],
)
def test_active_ro_match_requires_complete_deliverable_attempt(field, value):
    row = {**_active_ro_row(), field: value}
    assert not protected_cloud_engage_module._ro_mount_matches_protected_selection(
        row,
        _PROTECTED_MOUNT_ROWS,
        thread_id=_ENGAGE_THREAD_ID,
        user_id="user-1",
        runtime_generation=_ENGAGE_GENERATION,
    )


def test_active_ro_match_accepts_complete_exact_attempt():
    assert protected_cloud_engage_module._ro_mount_matches_protected_selection(
        _active_ro_row(),
        _PROTECTED_MOUNT_ROWS,
        thread_id=_ENGAGE_THREAD_ID,
        user_id="user-1",
        runtime_generation=_ENGAGE_GENERATION,
    )


@pytest.mark.asyncio
async def test_runtime_ready_probe_rechecks_lifecycle_after_reader_await():
    thread_id = "22222222-2222-4222-8222-222222222222"
    live = {
        "id": thread_id,
        "status": "created",
        "execution_lane": "pinned",
        "runtime_generation": "11111111-1111-4111-8111-111111111111",
        "runtime_retirement_token": None,
        "user_id": "user-1",
        "metadata": {"protected_cloud": True},
    }
    ended = {**live, "status": "ended"}
    delivery_started = asyncio.Event()
    release_delivery = asyncio.Event()

    async def delayed_delivery(_thread, _metadata, **_kwargs):
        delivery_started.set()
        await release_delivery.wait()
        return "ready", None

    # Entry capture, loop authority check, then the final post-reader check.
    get_thread = AsyncMock(side_effect=[live, live, ended])
    with (
        patch.object(
            orchestrator.main.app.state.resources.postgres_db, "get_thread", get_thread
        ),
        # The probe body lives in the service now and resolves the delivery
        # helper in its own namespace; patching main's wrapper would leave the
        # real probe running and this test waiting on an event nobody sets.
        patch.object(
            engage_service,
            "_protected_cloud_delivery_state",
            side_effect=delayed_delivery,
        ) as delivery,
    ):
        probe = asyncio.create_task(
            protected_cloud_engage_module._await_protected_cloud_runtime_ready(
                thread_id,
                timeout_s=1,
                dependencies=workspace_composition.protected_cloud_engage_dependencies(
                    orchestrator.main.app.state.resources
                ),
            )
        )
        await delivery_started.wait()
        release_delivery.set()
        assert await probe is False

    assert get_thread.await_count == 3
    delivery.assert_awaited_once()


@pytest.mark.asyncio
async def test_attach_inner_reader_flip_fails_without_scheduling_engage():
    """The lifecycle->datasource attach tail never reacquires lifecycle.

    Callers perform the potentially scheduling reader gate before taking the
    lifecycle lock. If the selected reader flips afterward, this inner gate is
    a zero-wait refusal; it must not enqueue an engage task behind its own
    outer lifecycle owner or reserve/deliver credentials.
    """
    generation = "11111111-1111-4111-8111-111111111111"
    thread = {
        "id": "22222222-2222-4222-8222-222222222222",
        "status": "created",
        "execution_lane": "pinned",
        "runtime_generation": generation,
        "runtime_retirement_token": None,
        "metadata": {"protected_cloud": True},
    }
    reserve = AsyncMock()
    with (
        patch.object(
            orchestrator.main.app.state.resources.postgres_db,
            "get_thread",
            AsyncMock(return_value=thread),
        ),
        patch.object(
            engage_service,
            "_protected_cloud_delivery_state",
            AsyncMock(return_value=("engaging", None)),
        ) as delivery,
        # The scheduler this gate must NOT reach is the one the probe would
        # resolve — in the service module, not main's wrapper.
        patch.object(engage_service, "_schedule_protected_engage") as schedule,
        patch.object(
            session_attach_binding_module, "reserve_session_attach_binding", reserve
        ),
    ):
        result = await asyncio.wait_for(
            session_attach_binding_module.send_session_attach_locked(
                {"id": "33333333-3333-4333-8333-333333333333"},
                str(thread["id"]),
                expected_runtime_generation=generation,
                dependencies=sessions_composition.session_attach_binding_dependencies(
                    orchestrator.main.app.state.resources
                ),
            ),
            timeout=0.25,
        )

    assert result is False
    # The stub really is the one the probe consulted: the refusal below is its
    # "engaging" verdict, and the real helper would have hit the mocked store.
    delivery.assert_awaited_once()
    schedule.assert_not_called()
    reserve.assert_not_awaited()


@pytest.mark.asyncio
async def test_engage_called_for_protected_thread_with_project_mount():
    mount_rows = _PROTECTED_MOUNT_ROWS
    thread = _engage_runtime_thread()

    # Mock postgres_db.acquire for the success-path metadata clear
    mock_conn = AsyncMock()
    mock_db_context = AsyncMock()
    mock_db_context.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_db_context.__aexit__ = AsyncMock(return_value=None)

    with (
        patch.object(
            deployment_gates_module,
            "is_protected_cloud_mode_enabled",
            return_value=True,
        ),
        patch.object(
            orchestrator.main.app.state.resources.postgres_db,
            "get_thread",
            AsyncMock(return_value=thread),
        ),
        patch.object(
            orchestrator.main.app.state.resources.postgres_db,
            "list_thread_mounts",
            AsyncMock(return_value=mount_rows),
        ),
        patch.object(
            orchestrator.main.app.state.resources.postgres_db,
            "get_ro_mount_by_thread",
            AsyncMock(return_value=None),
        ),
        patch.object(engage_service, "engage_ro_mount", new=AsyncMock()) as engage,
        patch.object(
            orchestrator.main.app.state.resources.main_cloud_router,
            "for_backend_instance",
        ) as for_backend,
        patch.object(
            orchestrator.main.app.state.resources.postgres_db,
            "acquire",
            return_value=mock_db_context,
        ),
    ):
        for_backend.return_value = object()
        await engage_service._engage_protected_cloud_for_thread(
            _ENGAGE_THREAD_ID,
            user_id="user-1",
            mount_rows=mount_rows,
            metadata={},
            runtime_generation=_ENGAGE_GENERATION,
            dependencies=workspace_composition.protected_cloud_engage_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
    engage.assert_awaited_once()


@pytest.mark.asyncio
async def test_engage_success_clears_stale_protected_cloud_error():
    """After a successful engage, any stale protected_cloud_error from a
    prior refused/flag-off attempt must be cleared so the attach-time poll
    fallback isn't suppressed on other HA replicas."""
    mount_rows = _PROTECTED_MOUNT_ROWS
    thread = _engage_runtime_thread()

    # Mock the postgres_db.acquire() context manager and connection
    mock_conn = AsyncMock()
    mock_db_context = AsyncMock()
    mock_db_context.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_db_context.__aexit__ = AsyncMock(return_value=None)

    with (
        patch.object(
            deployment_gates_module,
            "is_protected_cloud_mode_enabled",
            return_value=True,
        ),
        patch.object(
            orchestrator.main.app.state.resources.postgres_db,
            "get_thread",
            AsyncMock(return_value=thread),
        ),
        patch.object(
            orchestrator.main.app.state.resources.postgres_db,
            "list_thread_mounts",
            AsyncMock(return_value=mount_rows),
        ),
        patch.object(
            orchestrator.main.app.state.resources.postgres_db,
            "get_ro_mount_by_thread",
            AsyncMock(return_value=None),
        ),
        patch.object(engage_service, "engage_ro_mount", new=AsyncMock()),
        patch.object(
            orchestrator.main.app.state.resources.main_cloud_router,
            "for_backend_instance",
        ) as for_backend,
        patch.object(
            orchestrator.main.app.state.resources.postgres_db,
            "acquire",
            return_value=mock_db_context,
        ),
    ):
        for_backend.return_value = object()
        await engage_service._engage_protected_cloud_for_thread(
            _ENGAGE_THREAD_ID,
            user_id="user-1",
            mount_rows=mount_rows,
            metadata={},
            runtime_generation=_ENGAGE_GENERATION,
            dependencies=workspace_composition.protected_cloud_engage_dependencies(
                orchestrator.main.app.state.resources
            ),
        )

    # Verify the metadata-key-delete statement was executed
    mock_conn.execute.assert_awaited_once()
    args = mock_conn.execute.await_args[0]
    assert "- 'protected_cloud_error'" in args[0]
    assert args[1] == _ENGAGE_THREAD_ID
    assert args[2] == _ENGAGE_GENERATION


@pytest.mark.asyncio
async def test_engage_refusal_records_error_and_does_not_raise():
    mount_rows = _PROTECTED_MOUNT_ROWS
    recorded: list[str] = []
    thread = _engage_runtime_thread()
    with (
        patch.object(
            deployment_gates_module,
            "is_protected_cloud_mode_enabled",
            return_value=True,
        ),
        patch.object(
            orchestrator.main.app.state.resources.postgres_db,
            "get_thread",
            AsyncMock(return_value=thread),
        ),
        patch.object(
            orchestrator.main.app.state.resources.postgres_db,
            "list_thread_mounts",
            AsyncMock(return_value=mount_rows),
        ),
        patch.object(
            orchestrator.main.app.state.resources.postgres_db,
            "get_ro_mount_by_thread",
            AsyncMock(return_value=None),
        ),
        patch.object(
            engage_service,
            "engage_ro_mount",
            new=AsyncMock(side_effect=RoEngageRefused("floor")),
        ),
        patch.object(
            orchestrator.main.app.state.resources.main_cloud_router,
            "for_backend_instance",
            return_value=object(),
        ),
        patch.object(
            engage_service,
            "_record_protected_error",
            new=AsyncMock(side_effect=lambda tid, msg, **_kwargs: recorded.append(msg)),
        ),
    ):
        # must NOT raise — a refusal is recorded, the session boots with no mount
        await engage_service._engage_protected_cloud_for_thread(
            _ENGAGE_THREAD_ID,
            user_id="user-1",
            mount_rows=mount_rows,
            metadata={},
            runtime_generation=_ENGAGE_GENERATION,
            dependencies=workspace_composition.protected_cloud_engage_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
    assert recorded and "refused" in recorded[0]


# ---------------------------------------------------------------------------
# Task B10 — F-I1: engage-vs-attach race (module-level task registry)
# ---------------------------------------------------------------------------

_ACTIVE_NC_ROW = {
    "backend": "nextcloud",
    "reader_id": "srw-reader-abc",
    "credentials": "app-pass-xyz",
    "webdav_url": "https://nc.internal/remote.php/dav/files/srw-reader-abc/Proj/",
    "auth_kind": "basic",
    "status": "active",
}


@pytest.mark.asyncio
async def test_schedule_protected_engage_registers_and_clears_task():
    """_schedule_protected_engage must register the task in the
    protected-engage registry immediately (before it runs) and clear the slot
    once it completes — the GC hazard + registry lookup this whole wave
    depends on."""
    with (
        # ``_schedule_protected_engage``'s task body resolves the engage
        # coroutine in the service module; main has no such attribute.
        patch.object(
            engage_service, "_engage_protected_cloud_for_thread", new=AsyncMock()
        ) as engage,
        patch.object(
            orchestrator.main.app.state.resources.postgres_db,
            "thread_advisory_lock",
            side_effect=_owned_workspace_lifecycle_lock,
        ),
    ):
        task = protected_cloud_engage_module._schedule_protected_engage(
            _ENGAGE_THREAD_ID,
            user_id="user-1",
            mount_rows=[],
            runtime_generation=_ENGAGE_GENERATION,
            dependencies=workspace_composition.protected_cloud_engage_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
        assert _engage_task_registry().get(_ENGAGE_TASK_KEY) is task
        await task
    assert _ENGAGE_TASK_KEY not in _engage_task_registry()
    engage.assert_awaited_once()


@pytest.mark.asyncio
async def test_cross_replica_same_generation_engage_mints_only_once():
    """A queued peer observes the first replica's exact active row.

    The in-memory task registry is not shared across replicas, so the lifecycle
    lock — plus an in-lock durable-row recheck — must make the second task an
    idempotent no-op. A second mint would rotate the shared reader password
    after the first attempt's credentials became deliverable.
    """

    lifecycle_lock = asyncio.Lock()
    state: dict[str, dict | None] = {"row": None}

    @asynccontextmanager
    async def serialized_lifecycle(*_args, **_kwargs):
        async with lifecycle_lock:
            yield True

    async def publish_first_attempt(**kwargs):
        state["row"] = _active_ro_row(attempt=kwargs["plan"].engage_attempt)
        return SimpleNamespace(grant_handle=state["row"]["grant_handle"])

    mock_conn = AsyncMock()
    mock_db_context = AsyncMock()
    mock_db_context.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_db_context.__aexit__ = AsyncMock(return_value=None)
    engage = AsyncMock(side_effect=publish_first_attempt)
    backend = MagicMock()
    backend._base_url = "https://nc.internal"
    backend.revoke_protected_reader_attempt = AsyncMock()

    with (
        patch.object(
            deployment_gates_module,
            "is_protected_cloud_mode_enabled",
            return_value=True,
        ),
        patch.object(
            orchestrator.main.app.state.resources.postgres_db,
            "thread_advisory_lock",
            side_effect=serialized_lifecycle,
        ),
        patch.object(
            orchestrator.main.app.state.resources.postgres_db,
            "get_thread",
            AsyncMock(return_value=_engage_runtime_thread()),
        ),
        patch.object(
            orchestrator.main.app.state.resources.postgres_db,
            "list_thread_mounts",
            AsyncMock(return_value=_PROTECTED_MOUNT_ROWS),
        ),
        patch.object(
            orchestrator.main.app.state.resources.postgres_db,
            "get_ro_mount_by_thread",
            AsyncMock(side_effect=lambda _tid: state["row"]),
        ),
        patch.object(engage_service, "engage_ro_mount", engage),
        patch.object(
            orchestrator.main.app.state.resources.main_cloud_router,
            "for_backend_instance",
            return_value=backend,
        ),
        patch.object(
            orchestrator.main.app.state.resources.postgres_db,
            "acquire",
            return_value=mock_db_context,
        ),
    ):
        first = protected_cloud_engage_module._schedule_protected_engage(
            _ENGAGE_THREAD_ID,
            user_id="user-1",
            mount_rows=_PROTECTED_MOUNT_ROWS,
            runtime_generation=_ENGAGE_GENERATION,
            dependencies=workspace_composition.protected_cloud_engage_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
        # A separate replica has a separate registry and can schedule the same
        # work. Calling the scheduler twice deliberately bypasses local dedupe.
        second = protected_cloud_engage_module._schedule_protected_engage(
            _ENGAGE_THREAD_ID,
            user_id="user-1",
            mount_rows=_PROTECTED_MOUNT_ROWS,
            runtime_generation=_ENGAGE_GENERATION,
            dependencies=workspace_composition.protected_cloud_engage_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
        await asyncio.gather(first, second)

    engage.assert_awaited_once()
    backend.revoke_protected_reader_attempt.assert_not_awaited()
    assert state["row"]["status"] == "active"


@pytest.mark.asyncio
async def test_build_agent_cloud_mount_awaits_inflight_engage_task_then_returns_payload(
    monkeypatch,
):
    """Registry await path: a task is already registered for this thread_id
    (create-time engage still in flight) when the attach path asks for the
    mount. The first lookup finds nothing; awaiting the registered task lets
    it finish, and the RE-lookup afterward finds the row it just wrote."""
    import orchestrator.main

    monkeypatch.setenv("CLOUD_WORKSPACE_DRIVER", "rclone_mount")
    monkeypatch.delenv("CLOUD_RCLONE_ALLOW_CONTAINER", raising=False)

    async def _noop() -> None:
        return None

    task = asyncio.create_task(_noop())
    _engage_task_registry()[_ENGAGE_TASK_KEY] = task

    calls = {"n": 0}

    async def _get_row(_tid):
        calls["n"] += 1
        # Pre-await: engage hasn't written the row yet. Post-await: it has.
        return None if calls["n"] == 1 else _ACTIVE_NC_ROW

    try:
        with (
            patch(
                "orchestrator.services.deployment_gates.is_protected_cloud_mode_enabled",
                return_value=True,
            ),
            patch(
                "orchestrator.main.app.state.resources.postgres_db.get_ro_mount_by_thread",
                new=AsyncMock(side_effect=_get_row),
            ),
        ):
            payload = await agent_cloud_mounts_module._build_agent_cloud_mount(
                _engage_runtime_thread(),
                mount_rows=[],
                metadata={
                    "protected_cloud": True,
                    "workspace_container": {"status": "ready", "pod_ip": "10.42.0.10"},
                },
                dependencies=workspace_composition.agent_cloud_mount_dependencies(
                    orchestrator.main.app.state.resources
                ),
            )
    finally:
        _engage_task_registry().pop(_ENGAGE_TASK_KEY, None)
        if not task.done():
            task.cancel()

    assert payload is not None
    assert payload["protected"] is True
    assert calls["n"] == 2


@pytest.mark.asyncio
async def test_build_agent_cloud_mount_inflight_task_timeout_still_fails_closed(
    monkeypatch,
):
    """A registered task that never finishes in time must not hang the
    attach path forever, and must never surface a mount after the bounded
    wait — fail-closed even on a timeout, not just an outright refusal."""
    import orchestrator.main

    monkeypatch.setenv("CLOUD_WORKSPACE_DRIVER", "rclone_mount")
    monkeypatch.delenv("CLOUD_RCLONE_ALLOW_CONTAINER", raising=False)

    async def _hang() -> None:
        await asyncio.Event().wait()  # never completes on its own

    task = asyncio.create_task(_hang())
    _engage_task_registry()[_ENGAGE_TASK_KEY] = task

    try:
        with (
            patch(
                "orchestrator.services.deployment_gates.is_protected_cloud_mode_enabled",
                return_value=True,
            ),
            patch(
                "orchestrator.main.app.state.resources.postgres_db.get_ro_mount_by_thread",
                new=AsyncMock(return_value=None),
            ) as get_row,
            patch(
                "orchestrator.services.agent_cloud_mounts.asyncio.wait_for",
                new=AsyncMock(side_effect=asyncio.TimeoutError()),
            ) as wait_for,
        ):
            payload = await agent_cloud_mounts_module._build_agent_cloud_mount(
                _engage_runtime_thread(),
                mount_rows=[],
                metadata={
                    "protected_cloud": True,
                    "workspace_container": {"status": "ready", "pod_ip": "10.42.0.10"},
                },
                dependencies=workspace_composition.agent_cloud_mount_dependencies(
                    orchestrator.main.app.state.resources
                ),
            )
            # The bounded wait really was the patched one (the real one would
            # have blocked on ``_hang`` for its full 30s), and the flag patch
            # really did open the protected branch — otherwise the row lookup
            # would never have run at all.
            wait_for.assert_awaited_once()
            assert get_row.await_count == 2
    finally:
        _engage_task_registry().pop(_ENGAGE_TASK_KEY, None)
        task.cancel()

    assert payload is None


@pytest.mark.asyncio
async def test_build_agent_cloud_mount_skips_poll_when_error_already_recorded(
    monkeypatch,
):
    """No task registered (e.g. HA — a different replica ran create) AND a
    terminal ``protected_cloud_error`` is already on the thread: the engage
    already ran to completion with nothing to wait for, so the poll loop
    must be skipped entirely rather than burning ~9s to rediscover 'no
    row'."""
    import orchestrator.main

    monkeypatch.setenv("CLOUD_WORKSPACE_DRIVER", "rclone_mount")
    monkeypatch.delenv("CLOUD_RCLONE_ALLOW_CONTAINER", raising=False)
    _engage_task_registry().pop(_ENGAGE_TASK_KEY, None)

    with (
        patch(
            "orchestrator.services.deployment_gates.is_protected_cloud_mode_enabled",
            return_value=True,
        ),
        patch(
            "orchestrator.main.app.state.resources.postgres_db.get_ro_mount_by_thread",
            new=AsyncMock(return_value=None),
        ) as get_row,
        patch(
            "orchestrator.services.agent_cloud_mounts.asyncio.sleep", new=AsyncMock()
        ) as sleep,
    ):
        payload = await agent_cloud_mounts_module._build_agent_cloud_mount(
            _engage_runtime_thread(),
            mount_rows=[],
            metadata={
                "protected_cloud": True,
                "protected_cloud_error": "protected mode refused: floor",
                "workspace_container": {"status": "ready", "pod_ip": "10.42.0.10"},
            },
            dependencies=workspace_composition.agent_cloud_mount_dependencies(
                orchestrator.main.app.state.resources
            ),
        )

    assert payload is None
    sleep.assert_not_awaited()
    assert get_row.await_count == 1


@pytest.mark.asyncio
async def test_build_agent_cloud_mount_poll_finds_row_before_cap(monkeypatch):
    """Poll path (no task, no recorded error): the row can land mid-poll —
    the loop must stop as soon as it does, not always exhaust all 3
    attempts."""
    import orchestrator.main

    monkeypatch.setenv("CLOUD_WORKSPACE_DRIVER", "rclone_mount")
    monkeypatch.delenv("CLOUD_RCLONE_ALLOW_CONTAINER", raising=False)
    _engage_task_registry().pop(_ENGAGE_TASK_KEY, None)

    calls = {"n": 0}

    async def _get_row(_tid):
        calls["n"] += 1
        return _ACTIVE_NC_ROW if calls["n"] >= 3 else None

    with (
        patch(
            "orchestrator.services.deployment_gates.is_protected_cloud_mode_enabled",
            return_value=True,
        ),
        patch(
            "orchestrator.main.app.state.resources.postgres_db.get_ro_mount_by_thread",
            new=AsyncMock(side_effect=_get_row),
        ),
        patch(
            "orchestrator.services.agent_cloud_mounts.asyncio.sleep", new=AsyncMock()
        ) as sleep,
    ):
        payload = await agent_cloud_mounts_module._build_agent_cloud_mount(
            _engage_runtime_thread(),
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
    # Initial lookup (miss) + 2 poll attempts to land the row on the 3rd
    # total call; the 3rd poll sleep never fires because the loop breaks.
    assert calls["n"] == 3
    assert sleep.await_count == 2


# ---------------------------------------------------------------------------
# Task B10 — F-I2: resume re-engage
# ---------------------------------------------------------------------------

_RESUME_THREAD_ID = "77777777-7777-4777-8777-777777777777"
_ENDED_RUNTIME_GENERATION = "88888888-8888-4888-8888-888888888888"
_RESUMED_RUNTIME_GENERATION = "99999999-9999-4999-8999-999999999999"


def _resume_thread_row(**overrides) -> dict:
    thread = {
        "id": _RESUME_THREAD_ID,
        "execution_lane": "pinned",
        "user_id": "user-1",
        "status": "ended",
        "runtime_generation": _ENDED_RUNTIME_GENERATION,
        "runtime_retirement_token": None,
        "metadata": {
            "protected_cloud": True,
            "config_override": {"workspace": {"backend": "sandbox"}},
        },
    }
    thread.update(overrides)
    return thread


@pytest.mark.asyncio
@pytest.mark.parametrize("execution_lane", ["stateless", "future-lane"])
async def test_resume_refuses_unavailable_non_pinned_lane(execution_lane):
    """Malformed stateless and unknown lanes fail before lifecycle mutation."""
    from tests._b09_control_seams import resume_thread

    thread = _resume_thread_row(execution_lane=execution_lane)
    db = AsyncMock()
    db.resume_thread = AsyncMock()

    with (
        patch(
            "orchestrator.security.access.require_thread_owner",
            AsyncMock(return_value=({"id": "user-1"}, thread)),
        ),
        patch("orchestrator.main.app.state.resources.postgres_db", db),
        pytest.raises(fastapi_module.HTTPException) as exc,
    ):
        await resume_thread(_RESUME_THREAD_ID, object())

    assert exc.value.status_code == 409
    db.resume_thread.assert_not_awaited()


@pytest.mark.asyncio
async def test_stateless_sandbox_resume_skips_registered_agent_and_ensures_workspace():
    """A valid queue-lane resume is topology-neutral and restores only state."""
    from tests._b09_control_seams import resume_thread

    generation = "11111111-1111-4111-8111-111111111111"
    thread = _resume_thread_row(
        execution_lane="stateless",
        main_cloud_session_handle="existing-session-folder",
        main_cloud_share_handle="existing-share",
        metadata={
            "config_override": {"workspace": {"backend": "sandbox"}},
            "workspace_container": {
                "status": "deleted",
                "provisioner": "k8s",
                "pod_ip": None,
                "_canvas_workspace_generation": generation,
                "_runtime_incarnation": None,
                "_snapshot_restore_required": False,
            },
            "_workspace_binding": {
                "generation": generation,
                "kind": "remote",
                "backing_id": "k8s-pvc:agent-workspaces:pvc-uid",
                "ssh_host_key_fingerprint": "SHA256:trusted",
            },
            "_stateless_workspace_retirement_settled": {
                "terminal_token": 8,
                "cleanup_complete": True,
                "permanent": False,
                "backing_id": "k8s-pvc:agent-workspaces:pvc-uid",
                "runtime_incarnation": "22222222-2222-4222-8222-222222222222",
                "snapshot_restore_required": False,
            },
        },
    )
    user = {"id": "user-1"}
    state = {"thread": thread}

    async def resume_row(_thread_id):
        state["thread"] = {
            **thread,
            "status": "created",
            "runtime_generation": _RESUMED_RUNTIME_GENERATION,
        }
        return True

    db = MagicMock()
    db.get_user = AsyncMock(return_value=user)
    db.resume_thread = AsyncMock(side_effect=resume_row)
    db.get_thread = AsyncMock(side_effect=lambda _tid: dict(state["thread"]))
    db.stateless_session_workspace_ensure_lock = MagicMock(
        side_effect=_owned_workspace_lifecycle_lock
    )
    db.begin_stateless_thread_workspace_retirement = AsyncMock(
        return_value={
            "state": "settled",
            "terminal_token": 8,
            "permanent": False,
            "backing_id": "k8s-pvc:agent-workspaces:pvc-uid",
            "runtime_incarnation": "22222222-2222-4222-8222-222222222222",
            "snapshot_restore_required": False,
            "workspace_absence_proven": False,
            "retry": True,
        }
    )
    db.list_thread_mounts = AsyncMock(return_value=[])
    agent_provisioner = MagicMock(is_available=True)
    agent_provisioner.provision_agent = AsyncMock()
    persistent_provisioner = MagicMock(is_available=True)
    persistent_provisioner.create_agent_pod = AsyncMock()
    find_idle = AsyncMock()
    ensure_workspace = AsyncMock()

    with (
        patch(
            "orchestrator.security.access.require_thread_owner",
            AsyncMock(return_value=(user, thread)),
        ),
        patch("orchestrator.main.app.state.resources.postgres_db", db),
        patch(
            "orchestrator.services.thread_mount_rows.thread_project_ids",
            AsyncMock(return_value=[]),
        ),
        patch(
            "orchestrator.services.thread_project_authorization.revalidate_thread_project_ids",
            AsyncMock(return_value=[]),
        ),
        patch(
            "orchestrator.services.thread_resume.thread_config_drift",
            AsyncMock(return_value=[]),
        ),
        patch(
            "orchestrator.services.session_config_resolution.require_supported_protected_session_class",
            AsyncMock(),
        ),
        patch(
            "orchestrator.services.agent_provisioner.agent_provisioner",
            agent_provisioner,
        ),
        patch(
            "orchestrator.services.persistent_provisioner.persistent_provisioner",
            persistent_provisioner,
        ),
        patch(
            "orchestrator.services.session_attach_binding.find_idle_persistent_agent",
            find_idle,
        ),
        patch(
            "orchestrator.services.session_provisioner.ensure_session_workspace",
            ensure_workspace,
        ),
    ):
        result = await resume_thread(_RESUME_THREAD_ID, object())
        await asyncio.sleep(0)

    assert result == {"status": "created", "thread_id": _RESUME_THREAD_ID}
    db.resume_thread.assert_awaited_once_with(_RESUME_THREAD_ID)
    find_idle.assert_not_awaited()
    agent_provisioner.provision_agent.assert_not_awaited()
    persistent_provisioner.create_agent_pod.assert_not_awaited()
    ensure_workspace.assert_awaited_once()


@pytest.mark.asyncio
async def test_stateless_resume_refuses_retirement_marker_before_mutation():
    from tests._b09_control_seams import resume_thread

    thread = _resume_thread_row(
        execution_lane="stateless",
        metadata={
            "_stateless_workspace_retirement_pending": True,
            "config_override": {"workspace": {"backend": "sandbox"}},
            "workspace_container": {
                "status": "ready",
                "provisioner": "k8s",
                "pod_ip": "10.42.0.25",
                "port": 30022,
                "pod_name": "ws-thread-111111111111",
                "namespace": "agent-workspaces",
                "_canvas_workspace_generation": (
                    "11111111-1111-4111-8111-111111111111"
                ),
                "_runtime_incarnation": "22222222-2222-4222-8222-222222222222",
            },
            "_workspace_binding": {
                "generation": "11111111-1111-4111-8111-111111111111",
                "kind": "remote",
                "backing_id": "k8s-pvc:agent-workspaces:pvc-uid",
                "ssh_host_key_fingerprint": "SHA256:trusted",
            },
        },
    )
    db = MagicMock()
    db.get_user = AsyncMock(return_value={"id": "user-1"})
    db.resume_thread = AsyncMock(return_value=True)
    db.get_thread = AsyncMock(return_value=thread)
    db.stateless_session_workspace_ensure_lock = MagicMock(
        side_effect=_owned_workspace_lifecycle_lock
    )

    with (
        patch(
            "orchestrator.security.access.require_thread_owner",
            AsyncMock(return_value=({}, thread)),
        ),
        patch("orchestrator.main.app.state.resources.postgres_db", db),
        patch(
            "orchestrator.services.thread_mount_rows.thread_project_ids",
            AsyncMock(return_value=[]),
        ),
        patch(
            "orchestrator.services.thread_project_authorization.revalidate_thread_project_ids",
            AsyncMock(return_value=[]),
        ),
        patch(
            "orchestrator.services.thread_resume.thread_config_drift",
            AsyncMock(return_value=[]),
        ),
        pytest.raises(fastapi_module.HTTPException) as exc,
    ):
        await resume_thread(_RESUME_THREAD_ID, object())

    assert exc.value.status_code == 503
    db.resume_thread.assert_not_awaited()


@pytest.mark.asyncio
async def test_resume_cas_loss_to_retirement_fails_closed():
    from tests._b09_control_seams import resume_thread

    thread = _resume_thread_row(metadata={})
    db = MagicMock()
    db.get_user = AsyncMock(return_value={"id": "user-1"})
    db.resume_thread = AsyncMock(return_value=False)
    db.list_thread_mounts = AsyncMock(return_value=[])

    with (
        patch(
            "orchestrator.security.access.require_thread_owner",
            AsyncMock(return_value=({}, thread)),
        ),
        patch("orchestrator.main.app.state.resources.postgres_db", db),
        patch(
            "orchestrator.services.thread_mount_rows.thread_project_ids",
            AsyncMock(return_value=[]),
        ),
        patch(
            "orchestrator.services.thread_project_authorization.revalidate_thread_project_ids",
            AsyncMock(return_value=[]),
        ),
        patch(
            "orchestrator.services.thread_resume.thread_config_drift",
            AsyncMock(return_value=[]),
        ),
        pytest.raises(fastapi_module.HTTPException) as exc,
    ):
        await resume_thread(_RESUME_THREAD_ID, object())

    assert exc.value.status_code == 409


@pytest.mark.asyncio
async def test_resume_refetch_refuses_lane_changed_while_task_was_scheduled():
    """A pinned entry snapshot is not authority for the background bind."""
    from tests._b09_control_seams import resume_thread

    thread = _resume_thread_row(
        metadata={},
        main_cloud_session_handle="existing-session-folder",
        main_cloud_share_handle="existing-share",
    )
    user = {"id": "user-1"}
    db = MagicMock()
    db.get_user = AsyncMock(return_value=user)
    db.resume_thread = AsyncMock()
    db.list_thread_mounts = AsyncMock(return_value=[])
    db.get_thread = AsyncMock(
        return_value={
            **thread,
            "execution_lane": "stateless",
            "status": "created",
            "runtime_generation": _RESUMED_RUNTIME_GENERATION,
            "agent_id": None,
        }
    )
    lock_cm = AsyncMock()
    lock_cm.__aenter__.return_value = None
    lock_cm.__aexit__.return_value = False
    db.thread_advisory_lock = MagicMock(return_value=lock_cm)
    provisioner = MagicMock(is_available=True)
    provisioner.provision_agent = AsyncMock()
    find_idle = AsyncMock()
    attach = AsyncMock()
    ensure_workspace = AsyncMock()

    with (
        patch(
            "orchestrator.security.access.require_thread_owner",
            AsyncMock(return_value=(user, thread)),
        ),
        patch("orchestrator.main.app.state.resources.postgres_db", db),
        patch(
            "orchestrator.services.thread_mount_rows.thread_project_ids",
            AsyncMock(return_value=[]),
        ),
        patch(
            "orchestrator.services.thread_project_authorization.revalidate_thread_project_ids",
            AsyncMock(return_value=[]),
        ),
        patch(
            "orchestrator.services.thread_resume.thread_config_drift",
            AsyncMock(return_value=[]),
        ),
        patch(
            "orchestrator.services.deployment_gates.is_protected_cloud_mode_enabled",
            return_value=False,
        ),
        patch("orchestrator.services.agent_provisioner.agent_provisioner", provisioner),
        patch(
            "orchestrator.services.persistent_provisioner.persistent_provisioner",
            MagicMock(is_available=False),
        ),
        patch(
            "orchestrator.services.session_attach_binding.find_idle_persistent_agent",
            find_idle,
        ),
        patch(
            "orchestrator.services.session_attach_binding.send_session_attach", attach
        ),
        patch(
            "orchestrator.services.thread_resume.await_late_cloud_setup",
            AsyncMock(),
        ),
        patch(
            "orchestrator.services.session_provisioner.ensure_session_workspace",
            ensure_workspace,
        ),
    ):
        result = await resume_thread(_RESUME_THREAD_ID, object())
        await asyncio.sleep(0)
        await asyncio.sleep(0)

    assert result == {"status": "created", "thread_id": _RESUME_THREAD_ID}
    find_idle.assert_not_awaited()
    attach.assert_not_awaited()
    provisioner.provision_agent.assert_not_awaited()


@pytest.mark.asyncio
async def test_resume_refetches_lane_after_failed_pool_reservation():
    """A warm-attach refusal cannot fall through after a concurrent flip."""
    from tests._b09_control_seams import resume_thread

    thread = _resume_thread_row(
        metadata={},
        main_cloud_session_handle="existing-session-folder",
        main_cloud_share_handle="existing-share",
    )
    user = {"id": "user-1"}
    pinned = {
        **thread,
        "status": "created",
        "runtime_generation": _RESUMED_RUNTIME_GENERATION,
        "agent_id": None,
    }
    stateless = {**pinned, "execution_lane": "stateless"}
    state = {"thread": pinned}
    db = MagicMock()
    db.get_user = AsyncMock(return_value=user)
    db.resume_thread = AsyncMock()
    db.list_thread_mounts = AsyncMock(return_value=[])
    db.get_thread = AsyncMock(side_effect=lambda _tid: dict(state["thread"]))
    lock_cm = AsyncMock()
    lock_cm.__aenter__.return_value = None
    lock_cm.__aexit__.return_value = False
    db.thread_advisory_lock = MagicMock(return_value=lock_cm)
    provisioner = MagicMock(is_available=True)
    provisioner.provision_agent = AsyncMock()
    idle = {
        "id": "agent-pool",
        "hostname": "pool-1",
        "pod_ip": "10.0.0.5",
        "pod_port": 8001,
    }

    async def lose_lane(*_args, **_kwargs):
        state["thread"] = stateless
        return False

    attach = AsyncMock(side_effect=lose_lane)

    with (
        patch(
            "orchestrator.security.access.require_thread_owner",
            AsyncMock(return_value=(user, thread)),
        ),
        patch("orchestrator.main.app.state.resources.postgres_db", db),
        patch(
            "orchestrator.services.thread_mount_rows.thread_project_ids",
            AsyncMock(return_value=[]),
        ),
        patch(
            "orchestrator.services.thread_project_authorization.revalidate_thread_project_ids",
            AsyncMock(return_value=[]),
        ),
        patch(
            "orchestrator.services.thread_resume.thread_config_drift",
            AsyncMock(return_value=[]),
        ),
        patch(
            "orchestrator.services.session_config_resolution.require_supported_protected_session_class",
            AsyncMock(),
        ),
        patch(
            "orchestrator.services.thread_project_authorization.thread_has_knowledge_scope",
            AsyncMock(return_value=False),
        ),
        patch(
            "orchestrator.services.dispatch_credentials.inject_thread_dispatch_credentials",
            AsyncMock(return_value={}),
        ),
        patch(
            "orchestrator.services.deployment_gates.is_protected_cloud_mode_enabled",
            return_value=False,
        ),
        patch("orchestrator.services.agent_provisioner.agent_provisioner", provisioner),
        patch(
            "orchestrator.services.persistent_provisioner.persistent_provisioner",
            MagicMock(is_available=False),
        ),
        patch(
            "orchestrator.services.session_attach_binding.find_idle_persistent_agent",
            AsyncMock(return_value=idle),
        ),
        patch(
            "orchestrator.services.session_attach_binding.send_session_attach", attach
        ),
        patch(
            "orchestrator.services.thread_resume.await_late_cloud_setup",
            AsyncMock(),
        ),
        patch(
            "orchestrator.services.session_provisioner.ensure_session_workspace",
            AsyncMock(),
        ),
    ):
        result = await resume_thread(_RESUME_THREAD_ID, object())
        await asyncio.sleep(0)
        await asyncio.sleep(0)

    assert result == {"status": "created", "thread_id": _RESUME_THREAD_ID}
    attach.assert_awaited_once()
    provisioner.provision_agent.assert_not_awaited()


@pytest.mark.asyncio
async def test_resume_schedules_reengage_when_no_active_row():
    """F-I2: end -> resume must re-engage protected cloud mode when the
    Slice A reconciler already revoked the grant (no active
    cloud_ro_mounts row) — otherwise a protected thread stays permanently
    mount-less after its first end/resume cycle."""
    from tests._b09_control_seams import resume_thread

    thread = _resume_thread_row()
    user = {"id": "user-1"}
    db = AsyncMock()
    resumed = {
        **thread,
        "status": "created",
        "runtime_generation": _RESUMED_RUNTIME_GENERATION,
    }
    db.resume_thread = AsyncMock(return_value=True)
    db.get_thread = AsyncMock(return_value=resumed)
    db.list_thread_mounts = AsyncMock(return_value=[])
    db.get_ro_mount_by_thread = AsyncMock(return_value=None)
    spawned, create_task = _capture_background_tasks()

    with (
        patch(
            "orchestrator.security.access.require_thread_owner",
            AsyncMock(return_value=(user, thread)),
        ),
        patch("orchestrator.main.app.state.resources.postgres_db", db),
        patch(
            "orchestrator.services.thread_mount_rows.thread_project_ids",
            AsyncMock(return_value=[]),
        ),
        patch(
            "orchestrator.services.thread_project_authorization.revalidate_thread_project_ids",
            AsyncMock(return_value=[]),
        ),
        patch(
            "orchestrator.services.thread_resume.thread_config_drift",
            AsyncMock(return_value=[]),
        ),
        patch(
            "orchestrator.services.session_config_resolution.require_supported_protected_session_class",
            AsyncMock(),
        ),
        patch(
            "orchestrator.services.deployment_gates.is_protected_cloud_mode_enabled",
            return_value=True,
        ),
        patch(
            "orchestrator.services.protected_cloud_engage._schedule_protected_engage"
        ) as schedule,
        patch(
            "orchestrator.services.agent_provisioner.agent_provisioner",
            MagicMock(is_available=False),
        ),
        patch(
            "orchestrator.services.persistent_provisioner.persistent_provisioner",
            MagicMock(is_available=False),
        ),
        patch(
            "orchestrator.services.session_provisioner.ensure_session_workspace",
            new=AsyncMock(),
        ),
        patch("asyncio.create_task", side_effect=create_task),
    ):
        result = await resume_thread(_RESUME_THREAD_ID, object())
        await asyncio.gather(*spawned)

    assert all(task.done() and task.exception() is None for task in spawned)

    assert result == {"status": "created", "thread_id": _RESUME_THREAD_ID}
    db.get_ro_mount_by_thread.assert_awaited_once_with(_RESUME_THREAD_ID)
    schedule.assert_called_once()
    _, kwargs = schedule.call_args
    assert kwargs["user_id"] == "user-1"
    assert kwargs["mount_rows"] == []


@pytest.mark.asyncio
async def test_resume_skips_reengage_when_active_row_present():
    """No re-engage when a live grant already covers this thread — resume
    must not spam a fresh reader/grant on every reconnect."""
    from tests._b09_control_seams import resume_thread

    thread = _resume_thread_row()
    user = {"id": "user-1"}
    db = AsyncMock()
    resumed = {
        **thread,
        "status": "created",
        "runtime_generation": _RESUMED_RUNTIME_GENERATION,
    }
    db.resume_thread = AsyncMock(return_value=True)
    db.get_thread = AsyncMock(return_value=resumed)
    db.list_thread_mounts = AsyncMock(return_value=[])
    db.get_ro_mount_by_thread = AsyncMock(return_value=_ACTIVE_NC_ROW)
    spawned, create_task = _capture_background_tasks()

    with (
        patch(
            "orchestrator.security.access.require_thread_owner",
            AsyncMock(return_value=(user, thread)),
        ),
        patch("orchestrator.main.app.state.resources.postgres_db", db),
        patch(
            "orchestrator.services.thread_mount_rows.thread_project_ids",
            AsyncMock(return_value=[]),
        ),
        patch(
            "orchestrator.services.thread_project_authorization.revalidate_thread_project_ids",
            AsyncMock(return_value=[]),
        ),
        patch(
            "orchestrator.services.thread_resume.thread_config_drift",
            AsyncMock(return_value=[]),
        ),
        patch(
            "orchestrator.services.session_config_resolution.require_supported_protected_session_class",
            AsyncMock(),
        ),
        patch(
            "orchestrator.services.deployment_gates.is_protected_cloud_mode_enabled",
            return_value=True,
        ),
        patch(
            "orchestrator.services.protected_cloud_engage._schedule_protected_engage"
        ) as schedule,
        patch(
            "orchestrator.services.agent_provisioner.agent_provisioner",
            MagicMock(is_available=False),
        ),
        patch(
            "orchestrator.services.persistent_provisioner.persistent_provisioner",
            MagicMock(is_available=False),
        ),
        patch(
            "orchestrator.services.session_provisioner.ensure_session_workspace",
            new=AsyncMock(),
        ),
        patch("asyncio.create_task", side_effect=create_task),
    ):
        await resume_thread(_RESUME_THREAD_ID, object())
        await asyncio.gather(*spawned)

    assert all(task.done() and task.exception() is None for task in spawned)
    schedule.assert_not_called()


@pytest.mark.asyncio
async def test_resume_skips_reengage_for_non_protected_thread():
    """Regression guard: an ordinary (non-protected) thread's resume must
    never touch the protected-engage registry."""
    from tests._b09_control_seams import resume_thread

    thread = _resume_thread_row(metadata={})
    user = {"id": "user-1"}
    db = AsyncMock()
    resumed = {
        **thread,
        "status": "created",
        "runtime_generation": _RESUMED_RUNTIME_GENERATION,
    }
    db.resume_thread = AsyncMock(return_value=True)
    db.get_thread = AsyncMock(return_value=resumed)
    db.list_thread_mounts = AsyncMock(return_value=[])
    db.get_ro_mount_by_thread = AsyncMock(return_value=None)
    spawned, create_task = _capture_background_tasks()

    with (
        patch(
            "orchestrator.security.access.require_thread_owner",
            AsyncMock(return_value=(user, thread)),
        ),
        patch("orchestrator.main.app.state.resources.postgres_db", db),
        patch(
            "orchestrator.services.thread_mount_rows.thread_project_ids",
            AsyncMock(return_value=[]),
        ),
        patch(
            "orchestrator.services.thread_project_authorization.revalidate_thread_project_ids",
            AsyncMock(return_value=[]),
        ),
        patch(
            "orchestrator.services.thread_resume.thread_config_drift",
            AsyncMock(return_value=[]),
        ),
        patch(
            "orchestrator.services.session_config_resolution.require_supported_protected_session_class",
            AsyncMock(),
        ),
        patch(
            "orchestrator.services.deployment_gates.is_protected_cloud_mode_enabled",
            return_value=True,
        ),
        patch(
            "orchestrator.services.protected_cloud_engage._schedule_protected_engage"
        ) as schedule,
        patch(
            "orchestrator.services.agent_provisioner.agent_provisioner",
            MagicMock(is_available=False),
        ),
        patch(
            "orchestrator.services.persistent_provisioner.persistent_provisioner",
            MagicMock(is_available=False),
        ),
        patch(
            "orchestrator.services.session_provisioner.ensure_session_workspace",
            new=AsyncMock(),
        ),
        patch("asyncio.create_task", side_effect=create_task),
    ):
        await resume_thread(_RESUME_THREAD_ID, object())
        await asyncio.gather(*spawned)

    assert all(task.done() and task.exception() is None for task in spawned)
    schedule.assert_not_called()
    db.get_ro_mount_by_thread.assert_not_awaited()
