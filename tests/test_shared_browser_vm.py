"""What the shared browser does on demand for a VM Session's workspace."""

from __future__ import annotations

import pytest

from orchestrator.services import shared_browser_vm as browser_vm
from orchestrator.services import vm_idle_access, vm_idle_lifecycle
from tests._canvas_vm_fixtures import VM_THREAD_ID, vm_context, vm_thread


def _thread(vm_status: str, *, status: str = "active", lane: str = "pinned") -> dict:
    thread = vm_thread(vm_context(status=vm_status, rootdisk="kept"))
    thread["status"] = status
    thread["execution_lane"] = lane
    return thread


@pytest.mark.parametrize(
    ("vm_status", "status", "idle_release", "expected"),
    [
        ("provisioning", "active", False, "coming_up"),
        ("restoring", "created", False, "coming_up"),
        ("waiting_capacity", "active", False, "coming_up"),
        ("suspended", "suspended", True, "asleep"),
        ("suspending", "awaiting_user", True, "asleep"),
        ("suspended", "suspended", False, None),
        ("suspended", "active", True, None),
        ("failed", "active", True, None),
        ("deleted", "suspended", True, None),
        ("ready", "active", True, None),
        ("provisioning", "ended", True, None),
    ],
)
def test_wake_state(monkeypatch, vm_status, status, idle_release, expected):
    if idle_release:
        monkeypatch.setenv("WORKSPACE_IDLE_RELEASE_ENABLED", "true")
    else:
        monkeypatch.delenv("WORKSPACE_IDLE_RELEASE_ENABLED", raising=False)

    assert browser_vm.vm_browser_wake_state(_thread(vm_status, status=status)) == (
        expected
    )


def test_a_stateless_thread_is_never_woken_for_the_browser():
    assert browser_vm.vm_browser_wake_state(
        _thread("provisioning", lane="stateless")
    ) is (None)


def test_which_sessions_are_vm_sessions(monkeypatch):
    monkeypatch.setenv("WORKSPACE_IDLE_RELEASE_ENABLED", "true")
    ready_upgraded = vm_thread(backend="sandbox")
    asleep_upgraded = _thread("suspended", status="suspended")
    asleep_upgraded["metadata"]["config_override"]["workspace"]["backend"] = "virtual"
    upgrading = vm_thread(vm_context(status="provisioning"), backend="sandbox")
    container = {"metadata": {"config_override": {"workspace": {"backend": "sandbox"}}}}

    assert browser_vm.thread_uses_vm_workspace(vm_thread(vm_context(status="failed")))
    assert browser_vm.thread_uses_vm_workspace(ready_upgraded)
    assert browser_vm.thread_uses_vm_workspace(asleep_upgraded)
    # An upgrade still in progress keeps the ready container.
    assert not browser_vm.thread_uses_vm_workspace(upgrading)
    assert not browser_vm.thread_uses_vm_workspace(container)


class _Lifecycle:
    calls: list = []
    answer = None
    error: BaseException | None = None

    def __init__(self, db):
        self.db = db

    async def request_thread_wake(self, thread_id, **kwargs):
        type(self).calls.append((self.db, thread_id, kwargs))
        if type(self).error is not None:
            raise type(self).error
        return type(self).answer


@pytest.fixture()
def lifecycle(monkeypatch):
    monkeypatch.setenv("WORKSPACE_IDLE_RELEASE_ENABLED", "true")
    _Lifecycle.calls, _Lifecycle.answer, _Lifecycle.error = [], None, None
    monkeypatch.setattr(vm_idle_lifecycle, "VMIdleLifecycleStore", _Lifecycle)
    return _Lifecycle


@pytest.mark.asyncio
async def test_a_sleeping_vm_is_woken_without_resuming_the_session(lifecycle):
    lifecycle.answer = {"wake_id": "w1"}
    db = object()

    assert await browser_vm.request_vm_browser_wake(
        db, _thread("suspended", status="suspended"), user_id="u1"
    )
    assert lifecycle.calls == [
        (
            db,
            VM_THREAD_ID,
            {
                "execution_requested": False,
                "access_kind": "ssh",
                "access_claimant": "u1:shared-browser",
            },
        )
    ]


@pytest.mark.asyncio
async def test_a_sleeping_vm_without_a_wake_is_not_coming(lifecycle):
    assert not await browser_vm.request_vm_browser_wake(
        object(), _thread("suspended", status="suspended"), user_id="u1"
    )
    lifecycle.error = RuntimeError("database unavailable")
    assert not await browser_vm.request_vm_browser_wake(
        object(), _thread("suspended", status="suspended"), user_id="u1"
    )


@pytest.mark.asyncio
async def test_a_vm_coming_up_is_waited_for_with_or_without_a_wake(lifecycle):
    assert await browser_vm.request_vm_browser_wake(
        object(), _thread("provisioning"), user_id="u1"
    )


@pytest.mark.asyncio
async def test_nothing_is_requested_for_a_vm_nothing_will_bring_up(lifecycle):
    assert not await browser_vm.request_vm_browser_wake(
        object(), _thread("failed"), user_id="u1"
    )
    assert lifecycle.calls == []


class _AccessStore:
    def __init__(self, db):
        self.db = db
        self.events: list = []
        self.acquire_answers: list = [{"id": "lease-1"}, {"id": "lease-2"}]
        self.renew_answer = True
        _AccessStore.last = self

    async def acquire(self, **kwargs):
        self.events.append(("acquire", kwargs))
        return self.acquire_answers.pop(0) if self.acquire_answers else None

    async def renew(self, lease_id, **kwargs):
        self.events.append(("renew", lease_id, kwargs))
        return self.renew_answer

    async def close(self, lease_id, **kwargs):
        self.events.append(("close", lease_id, kwargs))
        return True


@pytest.fixture()
def access_store(monkeypatch):
    monkeypatch.setattr(vm_idle_access, "VMIdleAccessStore", _AccessStore)
    return _AccessStore


@pytest.mark.asyncio
async def test_viewer_access_acquires_renews_and_closes_one_lease(access_store):
    access = browser_vm.VMViewerAccess(object(), VM_THREAD_ID, "u1")

    assert await access.acquire() is True
    assert await access.renew() is True
    await access.close()
    await access.close()  # idempotent

    store = access_store.last
    kinds = [event[0] for event in store.events]
    assert kinds == ["acquire", "renew", "close"]
    acquired = store.events[0][1]
    assert acquired["owner_kind"] == "thread"
    assert acquired["owner_id"] == VM_THREAD_ID
    assert acquired["kind"] == "ssh"
    assert acquired["claimant"].startswith("u1:shared-browser:")
    assert store.events[1][1] == "lease-1"
    assert store.events[2][2]["claimant"] == acquired["claimant"]
    assert access.held is False


@pytest.mark.asyncio
async def test_viewer_access_past_its_cap_takes_a_fresh_lease(access_store):
    access = browser_vm.VMViewerAccess(object(), VM_THREAD_ID, "u1")
    await access.acquire()
    access_store.last.renew_answer = False

    assert await access.renew() is True

    kinds = [event[0] for event in access_store.last.events]
    assert kinds == ["acquire", "renew", "close", "acquire"]
    assert access.held is True


@pytest.mark.asyncio
async def test_viewer_access_failures_are_best_effort(access_store):
    access = browser_vm.VMViewerAccess(object(), VM_THREAD_ID, "u1")
    access_store.last.acquire_answers = []

    assert await access.acquire() is False
    assert access.held is False
    await access.close()
    assert [event[0] for event in access_store.last.events] == ["acquire"]


def test_a_ready_vm_waiting_for_its_probe_is_coming_up(monkeypatch):
    monkeypatch.setenv("VM_MODE", "same-cluster")
    thread = vm_thread(vm_context(ssh_ready_source="daemon_registration"))

    assert browser_vm.vm_browser_wake_state(thread) == "coming_up"
    assert browser_vm.vm_browser_wake_state(vm_thread(vm_context())) is None
    monkeypatch.setenv("VM_MODE", "external")
    assert browser_vm.vm_browser_wake_state(thread) is None


def test_a_woken_vm_before_its_wake_is_marked_ready_is_coming_up(monkeypatch):
    # The successor is ready and probed, but keeps the sleep's remote-I/O
    # closure until the idle lifecycle marks its wake ready.
    monkeypatch.setenv("VM_MODE", "same-cluster")
    woken = vm_thread(
        vm_context(_suspend_remote_io_closed="op-1", idle_wake_operation_id="op-1")
    )
    other = vm_thread(
        vm_context(_suspend_remote_io_closed="op-1", idle_wake_operation_id="op-2")
    )

    assert browser_vm.vm_browser_wake_state(woken) == "coming_up"
    assert browser_vm.vm_browser_wake_state(other) is None
