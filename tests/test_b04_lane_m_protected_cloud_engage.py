"""Characterization + wire contracts for the extracted protected-cloud engage.

R1.B04 lane M. The properties pinned here are the ones the move could quietly
break, and each of them is a security boundary rather than a convenience:

* the **closed set** of protected-cloud error codes, and the ``ValueError``
  that keeps anything outside it out of the database;
* the credential-free wait payload a polling agent receives;
* the ordering of ``_protected_cloud_delivery_state``'s refusals — marker,
  then tier, then flag, then stored code, then legacy raw error;
* ``_schedule_protected_engage`` taking the thread advisory lock, registering
  under the exact ``(thread_id, generation)`` key, and a stale done-callback
  never clobbering a newer registration;
* ``_resolve_protected_reader_backend`` re-resolving the captured installation
  from durable authority and **raising** rather than falling back to the
  active backend.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.services import protected_cloud_engage as engage
from orchestrator.services.cloud import FeatureNotAvailable
from orchestrator.services.cloud.protected_reader_authority import (
    ProtectedNextcloudReaderGrantPlan,
)
from orchestrator.services.cloud.ro_engage import RoEngageRefused
from orchestrator.services.cloud_staging.source_identity import (
    ProtectedMountSourceIdentity,
)

THREAD_ID = "22222222-2222-4222-8222-222222222222"
GENERATION = "11111111-1111-4111-8111-111111111111"
TASK_KEY = (THREAD_ID, GENERATION)
BACKEND_INSTANCE_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
SOURCE_REF = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
MOUNT_ID = "33333333-3333-4333-8333-333333333333"

SOURCE = ProtectedMountSourceIdentity(
    backend_instance_id=BACKEND_INSTANCE_ID,
    source_ref=SOURCE_REF,
    target_path="projects/example",
    native_id="17",
    mountpoint="Proj",
)
MOUNT_ROWS = [
    {
        "id": MOUNT_ID,
        "mount_kind": "project",
        "backend_id": "nextcloud",
        "backend_instance_id": BACKEND_INSTANCE_ID,
        "source_kind": "project_folder",
        "source_ref": SOURCE_REF,
        "target_path": "projects/example",
        "cloud_handle": (
            '{"backend":"nextcloud","native_id":"17",'
            '"vendor_meta":{"mountpoint":"Proj"}}'
        ),
    }
]


# --------------------------------------------------------------------------- #
# Doubles
# --------------------------------------------------------------------------- #


class _Registry:
    """A faithful stand-in for the contract's ``CloudTaskRegistry``.

    ``protected_engage_register`` attaches the done-callback that pops the slot
    only when it is still this exact task — the property the stale-callback
    case below exists to prove.
    """

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


class _Conn:
    def __init__(self) -> None:
        self.calls: list[tuple] = []

    async def execute(self, *args):
        self.calls.append(args)


def _store(conn: _Conn | None = None, **over: Any) -> SimpleNamespace:
    connection = conn if conn is not None else _Conn()

    @asynccontextmanager
    async def acquire():
        yield connection

    @asynccontextmanager
    async def thread_advisory_lock(_thread_id):
        yield True

    store = SimpleNamespace(
        acquire=acquire,
        thread_advisory_lock=thread_advisory_lock,
        conn=connection,
        get_thread=AsyncMock(return_value=None),
        list_thread_mounts=AsyncMock(return_value=list(MOUNT_ROWS)),
        get_ro_mount_by_thread=AsyncMock(return_value=None),
        get_main_cloud_backend_instance=AsyncMock(return_value=None),
    )
    for key, value in over.items():
        setattr(store, key, value)
    return store


def _deps(
    *,
    store=None,
    cloud_router=None,
    cloud_tasks=None,
    protected_enabled: bool = True,
    workspace_backend: str = "sandbox",
) -> engage.ProtectedCloudEngageDependencies:
    return engage.ProtectedCloudEngageDependencies(
        store=store if store is not None else _store(),
        cloud_router=cloud_router if cloud_router is not None else SimpleNamespace(),
        cloud_tasks=cloud_tasks if cloud_tasks is not None else _Registry(),
        is_protected_cloud_mode_enabled=lambda: protected_enabled,
        thread_workspace_backend=lambda _thread: workspace_backend,
    )


def _thread(**over: Any) -> dict[str, Any]:
    thread = {
        "id": THREAD_ID,
        "status": "created",
        "execution_lane": "pinned",
        "runtime_generation": GENERATION,
        "runtime_retirement_token": None,
        "user_id": "user-1",
        "metadata": {"protected_cloud": True},
    }
    thread.update(over)
    return thread


# --------------------------------------------------------------------------- #
# The closed error-code set
# --------------------------------------------------------------------------- #


def test_protected_cloud_error_codes_are_exactly_these_six():
    assert engage._PROTECTED_CLOUD_ERROR_CODES == frozenset(
        {
            "feature_disabled",
            "malformed_protected_marker",
            "unsupported_workspace_tier",
            "no_protected_mount",
            "engage_refused",
            "engage_failed",
        }
    )


@pytest.mark.asyncio
async def test_record_protected_error_refuses_an_unknown_code_without_writing():
    dependencies = _deps()

    with pytest.raises(ValueError, match="Unknown protected cloud error code"):
        await engage._record_protected_error(
            THREAD_ID,
            "boom",
            code="not_a_real_code",
            dependencies=dependencies,
        )

    assert dependencies.store.conn.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("code", sorted(engage._PROTECTED_CLOUD_ERROR_CODES))
async def test_record_protected_error_writes_under_the_generation_fence(code):
    dependencies = _deps()

    await engage._record_protected_error(
        THREAD_ID,
        "why",
        code=code,
        expected_runtime_generation=GENERATION,
        dependencies=dependencies,
    )

    (sql, thread_id, payload, generation), *rest = dependencies.store.conn.calls
    assert rest == []
    assert thread_id == THREAD_ID
    assert generation == GENERATION
    assert "runtime_retirement_token IS NULL" in sql
    assert "runtime_generation=$3::uuid" in sql
    assert f'"protected_cloud_error_code": "{code}"' in payload


# --------------------------------------------------------------------------- #
# The credential-free wait payload
# --------------------------------------------------------------------------- #


def test_wait_payload_never_carries_runtime_or_cloud_coordinates():
    payload = engage._protected_workspace_wait_payload(state="engaging")

    assert payload["status"] == "creating"
    assert payload["protected_cloud_state"] == "engaging"
    assert payload["protected_cloud_error_code"] is None
    for key in (
        "pod_ip",
        "pod_name",
        "namespace",
        "vm_ssh_host",
        "ssh_key_path",
        "git_remote_url",
        "managed_repository_credentials",
        "resolved_config",
        "datasources",
        "nc_session_folder",
        "cloud_sync",
        "cloud_mount",
    ):
        assert payload[key] is None, key
    assert payload["project_ids"] == []


# The non-ready allowlist of an agent image from before C1, verbatim. During a
# rollout such an agent still polls this endpoint, and its allowlist refuses
# any key it does not know (``workspace_ssh_identities`` among them), which
# would fail the protected attach instead of waiting.
_PRE_C1_AGENT_NON_READY_KEYS = frozenset(
    {
        "status",
        "protected_cloud",
        "protected_cloud_state",
        "protected_cloud_error_code",
        "pod_ip",
        "pod_name",
        "pod_port",
        "namespace",
        "vm_status",
        "vm_ssh_host",
        "vm_ssh_port",
        "vm_name",
        "ssh_key_path",
        "workspace_generation",
        "workspace_runtime_incarnation",
        "workspace_ssh_host_key_fingerprint",
        "git_remote_url",
        "managed_repository_credentials",
        "repositories",
        "resolved_config",
        "config_override",
        "project_ids",
        "datasources",
        "nc_session_folder",
        "cloud_sync",
        "cloud_mount",
        "cloud_sync_degraded",
        "canvas_presentation_available",
        "canvas_live_apps_available",
        "canvas_shared_browser_available",
    }
)


@pytest.mark.parametrize("state", ["engaging", "failed"])
def test_wait_payload_keys_are_ones_every_agent_generation_accepts(state):
    from agent.api import session_workspace

    payload = engage._protected_workspace_wait_payload(
        state=state, error_code="engage_refused"
    )

    assert set(payload) == _PRE_C1_AGENT_NON_READY_KEYS
    assert "workspace_ssh_identities" not in payload
    # And the current agent reads it as the wait (or refusal) it is, never as
    # a malformed payload.
    if state == "engaging":
        assert session_workspace.protected_workspace_delivery(payload) == "engaging"
    else:
        with pytest.raises(
            session_workspace.ProtectedCloudUnavailable,
            match=r"engage was refused \(engage_refused\)",
        ):
            session_workspace.protected_workspace_delivery(payload)


def test_wait_payload_failed_state_carries_only_the_sanitized_code():
    payload = engage._protected_workspace_wait_payload(
        state="failed", error_code="engage_refused"
    )

    assert payload["status"] == "failed"
    assert payload["protected_cloud_state"] == "failed"
    assert payload["protected_cloud_error_code"] == "engage_refused"


def test_wait_payload_drops_an_error_code_when_not_failed():
    payload = engage._protected_workspace_wait_payload(
        state="engaging", error_code="engage_refused"
    )
    assert payload["protected_cloud_error_code"] is None


# --------------------------------------------------------------------------- #
# _protected_cloud_delivery_state — refusal ordering
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_delivery_state_is_ready_for_an_ordinary_session():
    dependencies = _deps()
    assert await engage._protected_cloud_delivery_state(
        _thread(), {}, dependencies=dependencies
    ) == ("ready", None)


@pytest.mark.asyncio
async def test_delivery_state_marker_beats_tier_and_flag():
    """A malformed marker answers before the tier or the feature flag is
    consulted, so a disabled deployment cannot relabel it."""
    dependencies = _deps(protected_enabled=False, workspace_backend="vm")

    assert await engage._protected_cloud_delivery_state(
        _thread(), {"protected_cloud": "maybe"}, dependencies=dependencies
    ) == ("failed", "malformed_protected_marker")


@pytest.mark.asyncio
async def test_delivery_state_tier_beats_the_feature_flag():
    dependencies = _deps(protected_enabled=False, workspace_backend="vm")

    assert await engage._protected_cloud_delivery_state(
        _thread(), {"protected_cloud": True}, dependencies=dependencies
    ) == ("failed", "unsupported_workspace_tier")


@pytest.mark.asyncio
async def test_delivery_state_reports_feature_disabled_on_a_sandbox_tier():
    dependencies = _deps(protected_enabled=False)

    assert await engage._protected_cloud_delivery_state(
        _thread(), {"protected_cloud": True}, dependencies=dependencies
    ) == ("failed", "feature_disabled")


@pytest.mark.asyncio
async def test_delivery_state_passes_through_a_stored_terminal_code():
    dependencies = _deps()

    assert await engage._protected_cloud_delivery_state(
        _thread(),
        {"protected_cloud": True, "protected_cloud_error_code": "no_protected_mount"},
        dependencies=dependencies,
    ) == ("failed", "no_protected_mount")


@pytest.mark.asyncio
async def test_delivery_state_sanitizes_a_legacy_raw_error():
    """Pre-code rows carry only an operator-facing string; it never leaves."""
    dependencies = _deps()

    state, code = await engage._protected_cloud_delivery_state(
        _thread(),
        {"protected_cloud": True, "protected_cloud_error": "nextcloud said 401 at /x"},
        dependencies=dependencies,
    )

    assert (state, code) == ("failed", "engage_failed")


@pytest.mark.asyncio
async def test_delivery_state_is_engaging_without_runtime_authority():
    dependencies = _deps()

    assert await engage._protected_cloud_delivery_state(
        _thread(runtime_generation=None),
        {"protected_cloud": True},
        dependencies=dependencies,
    ) == ("engaging", None)


# --------------------------------------------------------------------------- #
# _ro_mount_matches_protected_selection
# --------------------------------------------------------------------------- #


def _active_ro_row(**over: Any) -> dict[str, Any]:
    plan = ProtectedNextcloudReaderGrantPlan(
        engage_attempt="44444444-4444-4444-8444-444444444444",
        backend_instance_id=BACKEND_INSTANCE_ID,
        source=SOURCE,
    )
    row = {
        "id": "55555555-5555-4555-8555-555555555555",
        "thread_id": THREAD_ID,
        "user_id": "user-1",
        "backend": "nextcloud",
        "backend_instance_id": BACKEND_INSTANCE_ID,
        "reader_id": plan.reader_id,
        "grant_group_id": plan.group_id,
        "credentials": "app-pass-xyz",
        "webdav_url": (
            f"https://nc.internal/remote.php/dav/files/{plan.reader_id}/Proj/"
        ),
        "auth_kind": "basic",
        "status": "active",
        "etag_baseline": {},
        "runtime_generation": GENERATION,
        "selected_mount_id": MOUNT_ID,
        "engage_attempt": plan.engage_attempt,
        "source_binding": SOURCE.binding,
        "source_binding_sha256": SOURCE.sha256,
        "grant_handle": plan.grant_handle,
        "grant_handle_sha256": plan.grant_handle_sha256,
    }
    row.update(over)
    return row


def test_selection_identity_is_none_when_any_field_is_missing():
    assert engage._protected_mount_selection_identity(None) is None
    assert engage._protected_mount_selection_identity({"id": MOUNT_ID}) is None
    assert engage._protected_mount_selection_identity(MOUNT_ROWS[0]) == (
        MOUNT_ID,
        "project",
        "nextcloud",
        SOURCE_REF,
        MOUNT_ROWS[0]["cloud_handle"],
    )


def test_complete_attempt_matches_its_selection():
    assert engage._ro_mount_matches_protected_selection(
        _active_ro_row(),
        MOUNT_ROWS,
        thread_id=THREAD_ID,
        user_id="user-1",
        runtime_generation=GENERATION,
    )


@pytest.mark.parametrize(
    "field,value",
    [
        ("status", "revoked"),
        ("backend", "opencloud"),
        ("auth_kind", "bearer"),
        ("credentials", ""),
        ("reader_id", ""),
        ("webdav_url", ""),
        ("user_id", "someone-else"),
        ("thread_id", "another-thread"),
        ("runtime_generation", "99999999-9999-4999-8999-999999999999"),
        ("etag_baseline", None),
        ("selected_mount_id", "not-a-uuid"),
    ],
)
def test_an_incomplete_attempt_is_never_deliverable(field, value):
    assert not engage._ro_mount_matches_protected_selection(
        _active_ro_row(**{field: value}),
        MOUNT_ROWS,
        thread_id=THREAD_ID,
        user_id="user-1",
        runtime_generation=GENERATION,
    )


# --------------------------------------------------------------------------- #
# Installation authority
# --------------------------------------------------------------------------- #


def _plan() -> ProtectedNextcloudReaderGrantPlan:
    return ProtectedNextcloudReaderGrantPlan(
        engage_attempt="44444444-4444-4444-8444-444444444444",
        backend_instance_id=BACKEND_INSTANCE_ID,
        source=SOURCE,
    )


@pytest.mark.asyncio
async def test_reader_backend_prefers_the_cached_exact_installation():
    cached = SimpleNamespace(backend_id="nextcloud")
    router = SimpleNamespace(
        for_backend_instance=lambda _id, *, expected_backend_id: cached,
        resolve_backend_instance=AsyncMock(),
    )
    dependencies = _deps(cloud_router=router)

    assert (
        await engage._resolve_protected_reader_backend(
            _plan(), dependencies=dependencies
        )
        is cached
    )
    router.resolve_backend_instance.assert_not_awaited()


@pytest.mark.asyncio
async def test_reader_backend_rebuilds_from_durable_authority_when_uncached():
    rebuilt = SimpleNamespace(backend_id="nextcloud")
    authority = object()

    def for_backend_instance(_id, *, expected_backend_id):
        raise FeatureNotAvailable("instance", backend=expected_backend_id)

    router = SimpleNamespace(
        for_backend_instance=for_backend_instance,
        resolve_backend_instance=AsyncMock(return_value=rebuilt),
    )
    store = _store(get_main_cloud_backend_instance=AsyncMock(return_value=authority))
    dependencies = _deps(store=store, cloud_router=router)

    assert (
        await engage._resolve_protected_reader_backend(
            _plan(), dependencies=dependencies
        )
        is rebuilt
    )
    router.resolve_backend_instance.assert_awaited_once_with(authority)


@pytest.mark.asyncio
async def test_reader_backend_raises_rather_than_using_the_active_backend():
    """No fallback: an unknown installation is a refusal, never a guess."""

    def for_backend_instance(_id, *, expected_backend_id):
        raise FeatureNotAvailable("instance", backend=expected_backend_id)

    router = SimpleNamespace(
        for_backend_instance=for_backend_instance,
        active=SimpleNamespace(backend_id="nextcloud"),
        resolve_backend_instance=AsyncMock(),
    )
    dependencies = _deps(cloud_router=router)

    with pytest.raises(RuntimeError, match="installation is unavailable"):
        await engage._resolve_protected_reader_backend(
            _plan(), dependencies=dependencies
        )
    router.resolve_backend_instance.assert_not_awaited()


# --------------------------------------------------------------------------- #
# _schedule_protected_engage — registry semantics
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_schedule_takes_the_advisory_lock_and_registers_under_the_key(
    monkeypatch,
):
    registry = _Registry()
    locked: list[str] = []

    @asynccontextmanager
    async def thread_advisory_lock(thread_id):
        locked.append(thread_id)
        yield True

    store = _store(thread_advisory_lock=thread_advisory_lock)
    dependencies = _deps(store=store, cloud_tasks=registry)
    ran = asyncio.Event()

    async def _fake_engage(*_args, **_kwargs):
        ran.set()

    monkeypatch.setattr(engage, "_engage_protected_cloud_for_thread", _fake_engage)

    task = engage._schedule_protected_engage(
        THREAD_ID,
        user_id="user-1",
        mount_rows=list(MOUNT_ROWS),
        metadata={"protected_cloud": True},
        runtime_generation=GENERATION,
        dependencies=dependencies,
    )

    assert registry.protected_engage_get(TASK_KEY) is task
    await task
    assert ran.is_set()
    assert locked == [THREAD_ID]
    # The registry's done-callback pops the slot it still owns.
    assert registry.protected_engage_get(TASK_KEY) is None


@pytest.mark.asyncio
async def test_a_stale_done_callback_never_clobbers_a_newer_registration(monkeypatch):
    """A resume re-engage firing right after a create engage must survive the
    first task finishing."""
    registry = _Registry()
    dependencies = _deps(cloud_tasks=registry)
    gates = [asyncio.Event(), asyncio.Event()]
    served = iter(gates)

    async def _fake_engage(*_args, **_kwargs):
        await next(served).wait()

    monkeypatch.setattr(engage, "_engage_protected_cloud_for_thread", _fake_engage)

    first = engage._schedule_protected_engage(
        THREAD_ID,
        user_id="user-1",
        mount_rows=list(MOUNT_ROWS),
        runtime_generation=GENERATION,
        dependencies=dependencies,
    )
    second = engage._schedule_protected_engage(
        THREAD_ID,
        user_id="user-1",
        mount_rows=list(MOUNT_ROWS),
        runtime_generation=GENERATION,
        dependencies=dependencies,
    )
    assert registry.protected_engage_get(TASK_KEY) is second

    gates[0].set()
    await first
    await asyncio.sleep(0)

    # The finished (stale) task must not have removed the newer one.
    assert registry.protected_engage_get(TASK_KEY) is second

    gates[1].set()
    await second
    await asyncio.sleep(0)
    assert registry.protected_engage_get(TASK_KEY) is None


@pytest.mark.asyncio
async def test_a_different_generation_gets_its_own_registry_slot(monkeypatch):
    registry = _Registry()
    dependencies = _deps(cloud_tasks=registry)
    other_generation = "99999999-9999-4999-8999-999999999999"

    async def _fake_engage(*_args, **_kwargs):
        await asyncio.sleep(0)

    monkeypatch.setattr(engage, "_engage_protected_cloud_for_thread", _fake_engage)

    a = engage._schedule_protected_engage(
        THREAD_ID,
        user_id="user-1",
        mount_rows=None,
        runtime_generation=GENERATION,
        dependencies=dependencies,
    )
    b = engage._schedule_protected_engage(
        THREAD_ID,
        user_id="user-1",
        mount_rows=None,
        runtime_generation=other_generation,
        dependencies=dependencies,
    )

    assert registry.protected_engage_get(TASK_KEY) is a
    assert registry.protected_engage_get((THREAD_ID, other_generation)) is b
    await asyncio.gather(a, b)


# --------------------------------------------------------------------------- #
# _engage_protected_cloud_for_thread — fail closed, never raise
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_engage_refuses_when_the_generation_moved_on():
    store = _store(get_thread=AsyncMock(return_value=_thread(runtime_generation=None)))
    dependencies = _deps(store=store)

    await engage._engage_protected_cloud_for_thread(
        THREAD_ID,
        user_id="user-1",
        mount_rows=list(MOUNT_ROWS),
        metadata={"protected_cloud": True},
        runtime_generation=GENERATION,
        dependencies=dependencies,
    )

    assert store.conn.calls == []


@pytest.mark.asyncio
async def test_engage_records_feature_disabled_without_touching_the_backend():
    store = _store(get_thread=AsyncMock(return_value=_thread()))
    dependencies = _deps(store=store, protected_enabled=False)

    await engage._engage_protected_cloud_for_thread(
        THREAD_ID,
        user_id="user-1",
        mount_rows=list(MOUNT_ROWS),
        metadata={"protected_cloud": True},
        runtime_generation=GENERATION,
        dependencies=dependencies,
    )

    (_sql, _tid, payload, _gen), *rest = store.conn.calls
    assert rest == []
    assert '"protected_cloud_error_code": "feature_disabled"' in payload


@pytest.mark.asyncio
async def test_engage_records_no_protected_mount_when_nothing_is_selectable():
    store = _store(get_thread=AsyncMock(return_value=_thread()))
    dependencies = _deps(store=store)

    await engage._engage_protected_cloud_for_thread(
        THREAD_ID,
        user_id="user-1",
        mount_rows=[],
        metadata={"protected_cloud": True},
        runtime_generation=GENERATION,
        dependencies=dependencies,
    )

    (_sql, _tid, payload, _gen), *rest = store.conn.calls
    assert rest == []
    assert '"protected_cloud_error_code": "no_protected_mount"' in payload


@pytest.mark.asyncio
async def test_engage_refusal_is_recorded_and_never_raised(monkeypatch):
    store = _store(get_thread=AsyncMock(return_value=_thread()))
    router = SimpleNamespace(
        for_backend_instance=lambda _id, *, expected_backend_id: SimpleNamespace(
            backend_id="nextcloud", _base_url="https://nc.internal"
        ),
        resolve_backend_instance=AsyncMock(),
    )
    dependencies = _deps(store=store, cloud_router=router)

    monkeypatch.setattr(
        engage,
        "engage_ro_mount",
        AsyncMock(side_effect=RoEngageRefused("probe saw a live write")),
    )

    await engage._engage_protected_cloud_for_thread(
        THREAD_ID,
        user_id="user-1",
        mount_rows=list(MOUNT_ROWS),
        metadata={"protected_cloud": True},
        runtime_generation=GENERATION,
        dependencies=dependencies,
    )

    (_sql, _tid, payload, _gen), *rest = store.conn.calls
    assert rest == []
    assert '"protected_cloud_error_code": "engage_refused"' in payload


@pytest.mark.asyncio
async def test_engage_refuses_a_selection_that_changed_before_it_ran():
    """Never mint a grant for mount A when the thread now selects mount B."""
    moved = [dict(MOUNT_ROWS[0], id="99999999-9999-4999-8999-999999999999")]
    store = _store(
        get_thread=AsyncMock(return_value=_thread()),
        list_thread_mounts=AsyncMock(return_value=moved),
    )
    dependencies = _deps(store=store)

    await engage._engage_protected_cloud_for_thread(
        THREAD_ID,
        user_id="user-1",
        mount_rows=list(MOUNT_ROWS),
        metadata={"protected_cloud": True},
        runtime_generation=GENERATION,
        dependencies=dependencies,
    )

    # A stale selection is a silent refusal, not a recorded terminal error:
    # a later attempt on the current selection must stay possible.
    assert store.conn.calls == []


# --------------------------------------------------------------------------- #
# _await_protected_cloud_runtime_ready
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_await_is_a_no_op_for_an_ordinary_session():
    store = _store(get_thread=AsyncMock(return_value=_thread(metadata={})))
    dependencies = _deps(store=store)

    assert (
        await engage._await_protected_cloud_runtime_ready(
            THREAD_ID, dependencies=dependencies
        )
        is True
    )


@pytest.mark.asyncio
async def test_await_fails_closed_without_runtime_authority():
    store = _store(get_thread=AsyncMock(return_value=_thread(runtime_generation=None)))
    dependencies = _deps(store=store)

    assert (
        await engage._await_protected_cloud_runtime_ready(
            THREAD_ID, dependencies=dependencies
        )
        is False
    )


@pytest.mark.asyncio
async def test_await_fails_closed_on_a_malformed_marker():
    store = _store(
        get_thread=AsyncMock(return_value=_thread(metadata={"protected_cloud": "x"}))
    )
    dependencies = _deps(store=store)

    assert (
        await engage._await_protected_cloud_runtime_ready(
            THREAD_ID, dependencies=dependencies
        )
        is False
    )


@pytest.mark.asyncio
async def test_await_schedules_one_engage_when_the_registry_is_empty(monkeypatch):
    store = _store(get_thread=AsyncMock(return_value=_thread()))
    registry = _Registry()
    dependencies = _deps(store=store, cloud_tasks=registry)
    scheduled = MagicMock()
    monkeypatch.setattr(engage, "_schedule_protected_engage", scheduled)

    result = await engage._await_protected_cloud_runtime_ready(
        THREAD_ID, timeout_s=0.0, dependencies=dependencies
    )

    assert result is False
    assert scheduled.call_count == 1
    assert scheduled.call_args.kwargs["runtime_generation"] == GENERATION
    assert scheduled.call_args.kwargs["user_id"] == "user-1"


@pytest.mark.asyncio
async def test_await_never_schedules_when_asked_not_to(monkeypatch):
    store = _store(get_thread=AsyncMock(return_value=_thread()))
    dependencies = _deps(store=store)
    scheduled = MagicMock()
    monkeypatch.setattr(engage, "_schedule_protected_engage", scheduled)

    result = await engage._await_protected_cloud_runtime_ready(
        THREAD_ID, timeout_s=0.0, allow_schedule=False, dependencies=dependencies
    )

    assert result is False
    scheduled.assert_not_called()
