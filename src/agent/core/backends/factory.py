"""Factory for the lite (no-workspace-pod) backends, keyed on ``workspace.backend``.

The two new tiers (``knowledge-base/knowledge/features/no_workspace_agent_mode.md`` §4) are
selected by the existing ``workspace.backend`` config value, exactly like the
legacy ``sandbox``/``vm`` aliases:

- ``virtual`` -> :class:`VirtualWorkspaceBackend` over an object store built
  from the dispatch payload's ``mounts`` (the §4 ``rclone_spec``). The first
  entry is the workspace prefix; every further entry is a cloud folder the
  backend serves under ``cloud/<name>``
  (``features/cloud_folders_on_virtual_sessions.md`` § The remote descriptor),
  each over its own store and rclone alias.
- ``none`` -> :class:`ScratchBackend` (disposable agent-local tmpdir).

The ``sandbox``/``vm`` (``RemoteBackend`` over SSH) construction stays at its
existing bootstrap seams — those carry site-specific SSH/retry behaviour, so
rather than fold them in here, the seams check :data:`LITE_BACKENDS` first and
call :func:`create_lite_backend` only for the new, shell-less tiers. Lite
backends need no SSH, no readiness-wait, and no privileged/workspace pod.
"""

from typing import TYPE_CHECKING, Any, Callable, Mapping

from shared.runtime.core.workspace_backend import WorkspaceBackend

if TYPE_CHECKING:
    from agent.core.backends.virtual import CloudRemote

from shared.backend_kinds import (
    LITE_BACKENDS as LITE_BACKENDS,
    VM_BACKENDS as VM_BACKENDS,
    is_lite_backend as is_lite_backend,
    is_vm_backend as is_vm_backend,
)


#: The descriptor auth kind whose bearer the executor fetches per call from
#: the orchestrator's token route (``cloud_remote_plan.OAUTH_TOKEN``).
OAUTH_TOKEN_AUTH = "oauth_token"


def cloud_remote_from_entry(
    entry: Mapping[str, Any],
    *,
    token_source_for: Callable[[Mapping[str, Any]], Any] | None = None,
) -> "CloudRemote":
    """One cloud folder entry of ``workspace.mounts`` as a
    :class:`~agent.core.backends.virtual.CloudRemote`.

    The entry (§ The remote descriptor of the feature note) carries ``name``
    (``outbox``, the project's slug, ``my_cloud``; ``cloud/<name>`` is
    accepted too), ``access``, ``mount_kind``, ``mount_id``, ``shared`` for an
    outbox, ``rclone_spec`` = ``{type, config, root}`` and ``auth``. Each
    folder gets its own rclone alias so several remotes in one process never
    share an environment namespace. An entry of the ``oauth_token`` auth kind
    carries no credential: ``token_source_for(entry)`` supplies the live one
    (``agent.core.backends.cloud_token``); without it the folder binds with a
    source that refuses, so it reports as failed rather than failing the
    workspace.

    Raises:
        ValueError: The entry has no name or no remote type.
    """
    from shared.runtime.core.backends.rclone import (
        cloud_object_store_from_spec,
        rclone_remote_name,
    )
    from agent.core.backends.virtual import CLOUD_ROOT, CloudRemote

    name = str(entry.get("name") or "").strip().strip("/")
    if name.startswith(CLOUD_ROOT + "/"):
        name = name[len(CLOUD_ROOT) + 1 :]
    if not name or "/" in name:
        raise ValueError(f"cloud folder entry has no usable name: {name!r}")
    shared = entry.get("shared")
    auth = entry.get("auth")
    live_credential = None
    if isinstance(auth, Mapping) and auth.get("type") == OAUTH_TOKEN_AUTH:
        if callable(token_source_for):
            live_credential = token_source_for(entry)
        else:
            from agent.core.backends.cloud_token import unavailable_token_source

            live_credential = unavailable_token_source(
                "this executor has no token route for the cloud folder"
            )
    return CloudRemote(
        name=name,
        path=f"{CLOUD_ROOT}/{name}",
        store=cloud_object_store_from_spec(
            dict(entry),
            remote_name=rclone_remote_name(f"cloud_{name}"),
            live_credential=live_credential,
        ),
        access=str(entry.get("access") or "read_write"),
        kind=str(entry.get("mount_kind") or ""),
        mount_id=str(entry.get("mount_id") or ""),
        shared=shared if isinstance(shared, bool) else None,
    )


def create_lite_backend(
    workspace_config: Any,
    *,
    job_id: str,
    max_read_bytes: int | None = None,
    cloud_folders: bool = True,
    cloud_token_source_for: Callable[[Mapping[str, Any]], Any] | None = None,
) -> WorkspaceBackend:
    """Construct the lite backend for ``virtual``/``none``.

    Args:
        workspace_config: The resolved workspace config (needs ``.backend`` and,
            for ``virtual``, ``.mounts``).
        job_id: Job/thread id — names the scratch tmpdir and provides the
            default object-store key prefix.
        max_read_bytes: A ``virtual`` backend's single-object read ceiling;
            None keeps the backend's default.
        cloud_folders: Whether the cloud folder entries after the first mount
            are bound. The workspace upgrade's seed and the legacy mirror
            push pass False: they copy or push the workspace prefix, never a
            user's cloud.
        cloud_token_source_for: How a cloud folder of the ``oauth_token``
            auth kind gets its per-call bearer
            (:func:`cloud_remote_from_entry`); None binds such a folder as
            failed.

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
        remotes = (
            [
                cloud_remote_from_entry(entry, token_source_for=cloud_token_source_for)
                for entry in mounts[1:]
            ]
            if cloud_folders
            else []
        )
        if max_read_bytes is None:
            return VirtualWorkspaceBackend(store, prefix=prefix, cloud_remotes=remotes)
        return VirtualWorkspaceBackend(
            store, prefix=prefix, max_read_bytes=max_read_bytes, cloud_remotes=remotes
        )

    raise ValueError(
        f"create_lite_backend called with non-lite backend {backend!r}; "
        f"expected one of {sorted(LITE_BACKENDS)}"
    )
