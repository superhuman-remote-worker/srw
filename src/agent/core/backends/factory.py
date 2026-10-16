"""Factory for the lite (no-workspace-pod) backends, keyed on ``workspace.backend``.

The two new tiers (``knowledge-base/knowledge/features/no_workspace_agent_mode.md`` §4) are
selected by the existing ``workspace.backend`` config value, exactly like the
legacy ``sandbox``/``vm`` aliases:

- ``virtual`` -> :class:`VirtualWorkspaceBackend` over an object store built
  from the dispatch payload's ``mounts`` (the §4 ``rclone_spec``).
- ``none`` -> :class:`ScratchBackend` (disposable agent-local tmpdir).

The ``sandbox``/``vm`` (``RemoteBackend`` over SSH) construction stays at its
existing bootstrap seams — those carry site-specific SSH/retry behaviour, so
rather than fold them in here, the seams check :data:`LITE_BACKENDS` first and
call :func:`create_lite_backend` only for the new, shell-less tiers. Lite
backends need no SSH, no readiness-wait, and no privileged/workspace pod.
"""

from typing import Any

from shared.runtime.core.workspace_backend import WorkspaceBackend

from shared.backend_kinds import (
    LITE_BACKENDS as LITE_BACKENDS,
    VM_BACKENDS as VM_BACKENDS,
    is_lite_backend as is_lite_backend,
    is_vm_backend as is_vm_backend,
)


def create_lite_backend(
    workspace_config: Any, *, job_id: str, max_read_bytes: int | None = None
) -> WorkspaceBackend:
    """Construct the lite backend for ``virtual``/``none``.

    Args:
        workspace_config: The resolved workspace config (needs ``.backend`` and,
            for ``virtual``, ``.mounts``).
        job_id: Job/thread id — names the scratch tmpdir and provides the
            default object-store key prefix.
        max_read_bytes: A ``virtual`` backend's single-object read ceiling;
            None keeps the backend's default.

    Raises:
        ValueError: If called for a non-lite backend, or ``virtual`` has no
            mount spec.
    """
    backend = workspace_config.backend

    if backend == "none":
        from agent.core.backends.scratch import ScratchBackend

        return ScratchBackend(job_id=job_id)

    if backend == "virtual":
        from shared.runtime.core.backends.rclone import object_store_from_spec
        from agent.core.backends.virtual import VirtualWorkspaceBackend

        mounts = getattr(workspace_config, "mounts", None) or []
        if not mounts:
            raise ValueError(
                "workspace.backend='virtual' requires workspace.mounts (the "
                "object-store spec from dispatch); none was provided"
            )
        mount = mounts[0]
        store = object_store_from_spec(mount)
        prefix = mount.get("prefix") or f"jobs/{job_id}/"
        if max_read_bytes is None:
            return VirtualWorkspaceBackend(store, prefix=prefix)
        return VirtualWorkspaceBackend(
            store, prefix=prefix, max_read_bytes=max_read_bytes
        )

    raise ValueError(
        f"create_lite_backend called with non-lite backend {backend!r}; "
        f"expected one of {sorted(LITE_BACKENDS)}"
    )
