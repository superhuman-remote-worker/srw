"""What the shared browser does on demand for a VM Session's workspace.

A Container Session's Open browser provisions a cold workspace, and an open
viewer marks the workspace active so it is not reaped mid-view. A same-cluster
VM does the same through its own lifecycle, never through a parallel one:

- **Wake.** A VM the idle lifecycle put to sleep on its kept rootdisk is woken
  by the access-only wake the VM IDE and the SSH gateway use
  (``VMIdleLifecycleStore.request_thread_wake`` with
  ``execution_requested=False``): the VM comes back, the Session itself (its
  agent) stays asleep. A VM that is already coming up (initial creation, or a
  wake under way) needs nothing; the open waits for it.
- **Keep awake.** While a viewer is attached, the relay holds a VM access
  lease, the obligation the idle lifecycle checks before it releases a VM.
  Without idle release no VM sleeps while its Session is open, so no lease is
  taken then.

Both use the access kind ``ssh``: the browser reaches the guest through a
direct channel on the pinned SSH transport, and the access kinds are a closed
set in the database. The claimant names the shared browser.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Literal
from uuid import uuid4

from shared.backend_kinds import VM_BACKENDS

logger = logging.getLogger(__name__)

BROWSER_VM_ACCESS_KIND = "ssh"

# A VM on its way up by itself: an initial creation, a restore, or a wake the
# idle lifecycle is already running.
VM_COMING_UP_STATUSES = frozenset(
    {
        "provisioning",
        "created",
        "starting",
        "ssh_pending",
        "restoring",
        "waiting_capacity",
        "waiting_golden",
        "waiting_headscale",
        "waiting_preparation",
    }
)
# A VM the idle lifecycle is putting, or has put, to sleep on its kept disk.
VM_ASLEEP_STATUSES = frozenset({"suspending", "suspended"})

VMWakeState = Literal["coming_up", "asleep"]


def _mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            return {}
    return value if isinstance(value, dict) else {}


def vm_idle_release_enabled() -> bool:
    """The deployment's VM idle release (``orchestrator.vmIdleRelease``)."""

    return os.getenv("WORKSPACE_IDLE_RELEASE_ENABLED", "").strip().lower() == "true"


def vm_ready_but_coming_up(vm: dict[str, Any]) -> bool:
    """A same-cluster VM reported ready that cannot be used for a moment yet.

    Every boot and every wake pass through this: the guest daemon registers
    ready a few seconds before the provisioner's own probe proves it, and a
    VM woken from idle keeps the sleep's remote-I/O closure until the idle
    lifecycle marks its wake ready (one reconcile pass later). Until then its
    identity is not attested, yet it is still coming up, not untrusted, so
    Open browser waits for it.
    """

    from shared.workspace_contract import vm_mode_from_env

    if vm.get("status") != "ready" or vm_mode_from_env() != "same-cluster":
        return False
    if vm.get("ssh_ready_source") != "provisioner_probe":
        return True
    marker = vm.get("_suspend_remote_io_closed")
    return bool(marker) and marker == vm.get("idle_wake_operation_id")


def vm_browser_wake_state(thread: dict[str, Any]) -> VMWakeState | None:
    """Whether Open browser can wait for this Session's VM, and why.

    ``coming_up``: the VM is starting by itself. ``asleep``: the idle
    lifecycle released it and an access-only wake can bring it back. ``None``:
    nothing will bring it up for the browser (an ended Session, a failed or
    deleted VM, or no idle release on this deployment).
    """

    if thread.get("execution_lane") != "pinned" or thread.get("status") == "ended":
        return None
    vm = _mapping(_mapping(thread.get("metadata")).get("vm"))
    status = vm.get("status")
    if status in VM_COMING_UP_STATUSES:
        return "coming_up"
    if vm_ready_but_coming_up(vm):
        return "coming_up"
    if (
        status in VM_ASLEEP_STATUSES
        and thread.get("status") in {"awaiting_user", "suspended"}
        and vm_idle_release_enabled()
    ):
        return "asleep"
    return None


def thread_uses_vm_workspace(thread: dict[str, Any]) -> bool:
    """Whether the shared browser should treat this Session as a VM one.

    The declared backend, a ready VM (which the Canvas resolver selects
    whatever the declared backend says), or a VM asleep on its disk (an
    upgraded Session keeps declaring its old tier).
    """

    metadata = _mapping(thread.get("metadata"))
    vm = _mapping(metadata.get("vm"))
    if vm.get("status") == "ready":
        return True
    backend = metadata.get("workspace_backend")
    if not isinstance(backend, str) or not backend:
        workspace = _mapping(_mapping(metadata.get("config_override")).get("workspace"))
        backend = workspace.get("backend")
    if backend in VM_BACKENDS:
        return True
    return vm_browser_wake_state(thread) == "asleep"


def _browser_claimant(user_id: str, *, connection: str | None = None) -> str:
    suffix = f":{connection}" if connection else ""
    return f"{user_id}:shared-browser{suffix}"


async def request_vm_browser_wake(
    db: Any, thread: dict[str, Any], *, user_id: str
) -> bool:
    """Ask for the access-only wake; ``True`` when the VM will come up.

    The claimant is stable per user and Session, so the Cockpit's repeated
    open (every few seconds while it waits) renews one access lease bound to
    one wake instead of minting a lease per request.
    """

    state = vm_browser_wake_state(thread)
    if state is None:
        return False
    from orchestrator.services.vm_idle_lifecycle import VMIdleLifecycleStore

    try:
        wake = await VMIdleLifecycleStore(db).request_thread_wake(
            str(thread.get("id") or ""),
            execution_requested=False,
            access_kind=BROWSER_VM_ACCESS_KIND,
            access_claimant=_browser_claimant(str(user_id)),
        )
    except Exception:
        logger.warning(
            "Shared-browser VM wake request failed for thread %s",
            thread.get("id"),
            exc_info=True,
        )
        wake = None
    if wake is not None:
        logger.info(
            "Shared-browser open joined the VM wake for thread %s", thread.get("id")
        )
        return True
    # A VM that is starting by itself (initial creation) has no idle wake to
    # join; the open simply waits for it.
    return state == "coming_up"


class VMViewerAccess:
    """One viewer's VM access lease: acquired on attach, renewed, closed."""

    def __init__(self, db: Any, thread_id: str, user_id: str) -> None:
        from orchestrator.services.vm_idle_access import VMIdleAccessStore

        self._store = VMIdleAccessStore(db)
        self._thread_id = thread_id
        self._user_id = user_id
        self._lease_id: str | None = None
        self._claimant: str | None = None

    @property
    def held(self) -> bool:
        return self._lease_id is not None

    async def acquire(self) -> bool:
        claimant = _browser_claimant(self._user_id, connection=uuid4().hex)
        try:
            lease = await self._store.acquire(
                owner_kind="thread",
                owner_id=self._thread_id,
                kind=BROWSER_VM_ACCESS_KIND,
                claimant=claimant,
            )
        except Exception:
            logger.debug(
                "Shared-browser VM access lease unavailable for thread %s",
                self._thread_id,
                exc_info=True,
            )
            lease = None
        if lease is None:
            return False
        self._lease_id = str(lease["id"])
        self._claimant = claimant
        return True

    async def renew(self) -> bool:
        """Extend the lease; past its one-hour cap, take a fresh one."""

        if self._lease_id is not None and self._claimant is not None:
            try:
                if await self._store.renew(
                    self._lease_id,
                    owner_kind="thread",
                    owner_id=self._thread_id,
                    kind=BROWSER_VM_ACCESS_KIND,
                    claimant=self._claimant,
                ):
                    return True
            except Exception:
                logger.debug(
                    "Shared-browser VM access renewal failed for thread %s",
                    self._thread_id,
                    exc_info=True,
                )
            await self.close()
        return await self.acquire()

    async def close(self) -> None:
        lease_id, claimant = self._lease_id, self._claimant
        self._lease_id = self._claimant = None
        if lease_id is None or claimant is None:
            return
        try:
            await self._store.close(
                lease_id,
                owner_kind="thread",
                owner_id=self._thread_id,
                kind=BROWSER_VM_ACCESS_KIND,
                claimant=claimant,
            )
        except Exception:
            logger.warning(
                "Shared-browser VM access lease could not be closed for thread %s; "
                "it expires by itself",
                self._thread_id,
                exc_info=True,
            )


__all__ = [
    "BROWSER_VM_ACCESS_KIND",
    "VMViewerAccess",
    "VM_ASLEEP_STATUSES",
    "VM_COMING_UP_STATUSES",
    "request_vm_browser_wake",
    "thread_uses_vm_workspace",
    "vm_browser_wake_state",
    "vm_idle_release_enabled",
    "vm_ready_but_coming_up",
]
