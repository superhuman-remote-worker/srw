"""Operator controls over the VM workspace tier.

Extracted verbatim from ``orchestrator.main`` (R1.B05 lane P, census group
``R_GRANTS``). Two decisions, both about the VM tier and neither about
capability grants:

* :func:`check_vm_permission` — may this principal get a VM at all? A global
  kill-switch in ``system_settings`` blocks EVERYONE including admins; a
  non-admin additionally needs the effective ``vm_workspace`` grant
  (``user_can_use_vm``, granted by default since 2026-11-11; the legacy
  ``can_use_vm`` column is read only when the grant read fails). This is the
  operator gate that runs *alongside* the PDP's ``vm_workspace`` check, never
  instead of it.
* :func:`vm_needs_release` — does a stored VM context still own cluster
  resources worth reclaiming?

Properties moved unchanged:

* **The kill-switch is checked before the admin bypass.** Reordering those two
  would let an admin provision a VM on a deployment that has globally disabled
  them.
* **``enabled is False`` is the only disabling value.** An absent or malformed
  settings row reads as enabled (fail-open for fresh installs), and a DB read
  failure is logged and deferred to the per-user check rather than being fatal.
* **Falsy context is NOT "needs release".** ``vm_needs_release`` is a denylist
  over ``status``; without the leading truthiness test an entity that never had
  a VM would pass the denylist and fire a spurious teardown.

The setting key is imported from ``system_settings`` rather than re-spelled, so
this gate and the admin CRUD surface can never read two different rows.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from fastapi import HTTPException

from orchestrator.services.system_settings import VM_WORKSPACES_SETTING_KEY

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class VmPermissionDependencies:
    """Collaborators for one VM operator-gate check, resolved per invocation.

    ``store`` is main's ``postgres_db``; it is rebuilt per call rather than
    captured at import so a test that rebinds the name on ``orchestrator.main``
    still steers both the kill-switch read and the per-user check.
    """

    store: Any


async def check_vm_permission(
    user: dict | None,
    *,
    job_needs_vm: bool,
    dependencies: VmPermissionDependencies,
) -> None:
    """Enforce admin VM-workspace controls.

    Raises HTTPException(403) when either:
      - The global kill-switch `system_settings['vm_workspaces']` is set
        to `{"enabled": false}` (blocks everyone, including admins).
      - The user is a non-admin whose effective `vm_workspace` grant is off.

    No-op when ``job_needs_vm`` is False. Absent/malformed setting row is
    treated as enabled (fail-open for fresh installs).
    """
    if not job_needs_vm:
        return
    row: dict | None = None
    try:
        row = await dependencies.store.get_system_setting(VM_WORKSPACES_SETTING_KEY)
    except Exception:
        # DB read failure is non-fatal for the gate — defer to per-user check.
        logger.exception("Failed to read vm_workspaces kill-switch; fail-open")
    value = (row or {}).get("value") or {}
    if isinstance(value, dict) and value.get("enabled") is False:
        raise HTTPException(
            status_code=403,
            detail="VM workspaces are globally disabled by the administrator",
        )
    if user and user.get("is_admin"):
        return
    if not user or not await dependencies.store.user_can_use_vm(user):
        raise HTTPException(
            status_code=403,
            detail="User is not permitted to use VM workspaces",
        )


def vm_needs_release(vm_ctx: dict | None) -> bool:
    """True when a VM context still owns cluster resources worth reclaiming.

    Shared by the thread and job branches of ``_archive_and_cleanup_workspace``
    so the two cannot drift apart again. They had: the job side used this
    denylist, the thread side an allowlist of
    ``("provisioning", "created", "ready")``. Every other status fell through on
    the thread side, so ``release_thread_vm`` never ran and each suspended VM
    leaked a 20 GiB rootdisk DataVolume plus its Headscale node — permanently,
    since kept-disk GC is jobs-only and the controller orphan backstop ships
    off. See knowledge-base/knowledge/issues/vm_reliability_assessment.md P1-7.

    A falsy context means the entity never had a VM, which must NOT count as
    "needs release" — the absent status would otherwise pass the denylist and
    fire a spurious teardown.
    """
    return bool(vm_ctx) and vm_ctx.get("status") not in ("deleted", "deleting")


__all__ = [
    "VmPermissionDependencies",
    "check_vm_permission",
    "vm_needs_release",
]
