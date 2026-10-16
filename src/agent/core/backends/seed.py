"""Cross-backend workspace seeding — copy every file from one backend to another.

Used by the workspace-tier upgrade (``workspace_tier_upgrade.md`` §4.2 S3a for
sessions, §4.3 W1 for worker jobs): when a lite (``virtual``) session/job
upgrades to a real ``sandbox`` container, the agent copies the object-store
prefix down into the freshly provisioned pod while BOTH backends are live. It
is a pure in-process copy — the agent already holds the object-store
credentials (lite internal creds never leave the agent process), so no
orchestrator transfer is involved.
"""

from __future__ import annotations

import logging
import posixpath
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Mapping

from agent.core.backends.overlay import unwrap_backend

if TYPE_CHECKING:
    from shared.runtime.core.workspace_backend import WorkspaceBackend

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------
# Seeded-content sentinel
#
# "Does this workspace still hold the content that was seeded for its job?"
# used to be answered by probing for a real ``task_brief.md``. Virtual
# directories removed that file's last writer (it is served, not written), so
# the probe would answer "unseeded" forever — silently rewinding same-pod
# resumes to the last phase boundary and letting a git-less resume fall through
# to initialize()'s ``rm -rf``.
#
# A dedicated marker replaces it: purpose-named, not agent-facing (so it adds
# no agent surface), and unaffected by Slice 2.
# See knowledge-base/knowledge/features/virtual_directories.md.
# --------------------------------------------------------------------------

SEEDED_MARKER = ".srw_seeded"

# Workspaces seeded before SEEDED_MARKER existed carry a real task_brief.md
# instead. Accepting it as evidence degrades toward the SAFE branch — no wipe,
# no silent rewind — for workspaces already in flight when this shipped.
_LEGACY_SEEDED_SENTINEL = "task_brief.md"

# Fixed body, deliberately: a timestamp would re-dirty the file on every boot
# and churn the workspace repo.
_SEEDED_MARKER_BODY = (
    "Superhuman Remote Worker seeded this workspace.\n"
    "Framework marker — do not edit. See knowledge-base/knowledge/features/virtual_directories.md.\n"
)


def mark_workspace_seeded(backend: Any) -> bool:
    """Record that this workspace holds real seeded content. Never raises.

    Written on the fresh-init path only — exactly where the old ``task_brief.md``
    sentinel was written — so the marker means "this workspace was seeded for
    this job", not "this process booted".

    Returns:
        True when the marker was written by this call.
    """
    real = unwrap_backend(backend)
    try:
        if real.exists(SEEDED_MARKER):
            return False
        real.write_file(SEEDED_MARKER, _SEEDED_MARKER_BODY)
        logger.debug("Wrote seeded-workspace marker %s", SEEDED_MARKER)
        return True
    except Exception as e:  # a missing marker degrades safely; never block boot
        logger.warning("Could not write seeded-workspace marker: %s", e)
        return False


def workspace_is_seeded(backend: Any) -> bool:
    """True when this workspace still holds the content seeded for its job.

    Always probes the REAL backend: virtual files always "exist", so probing
    through the overlay would report every freshly wiped pod as seeded and skip
    the re-seed.

    A probe failure counts as "not found" for that name — the same fail-safe the
    call sites used before — but the legacy sentinel is still consulted, so a
    transient error on one name cannot on its own condemn a seeded workspace.
    """
    real = unwrap_backend(backend)
    for name in (SEEDED_MARKER, _LEGACY_SEEDED_SENTINEL):
        try:
            if real.exists(name):
                return True
        except Exception as e:
            logger.warning("Seeded-content probe for %r failed: %s", name, e)
    return False


def reseed_missing_files(
    backend: "WorkspaceBackend", seed_files: Dict[str, str]
) -> List[str]:
    """Idempotently (re)write agent-authored seed files that are absent.

    A sandbox pod is provisioned WITH its git clone (instructions.md, tools/),
    then the agent writes files on top (task_brief.md, bound skills). If the pod
    is torn down and re-provisioned mid-run, the clone comes back but the
    agent-written files do not, so the agent reconnects onto a workspace missing
    them (knowledge-base/knowledge/issues/reviewing_parent_pod_reaped_under_critic.md Issue 4). This
    restores exactly the agent-authored files that are currently absent.

    Write-if-absent, never clobber: a file that survived the re-provision may
    have been edited by the agent since seeding, so it is left untouched.

    Args:
        backend: The (re)connected workspace backend to write into.
        seed_files: Mapping of workspace-relative path → original content.

    Returns:
        The paths that were (re)written (absent → restored), in input order.
    """
    written: List[str] = []
    for path, content in seed_files.items():
        try:
            if backend.exists(path):
                continue
            parent = posixpath.dirname(path)
            if parent:
                backend.mkdir(parent)
            backend.write_file(path, content)
            written.append(path)
        except Exception:
            # One un-writable seed file must not abort the rest; the agent still
            # detects+reports a genuinely un-seedable workspace (BLOCKER.md).
            logger.exception("Re-seed: failed to restore %r (continuing)", path)
    if written:
        logger.info(
            "Re-seeded %d missing agent-authored file(s): %s", len(written), written
        )
    return written


def seed_workspace(
    src: "WorkspaceBackend",
    dst: "WorkspaceBackend",
    *,
    exclude: "Callable[[str], bool] | None" = None,
) -> int:
    """Copy every file from ``src`` into ``dst``, preserving relative paths.

    Walks the source recursively (``agent.walk()``), reads each file as bytes,
    creates the parent directory in the destination, and writes the bytes.

    Verify-before-return: after copying, every source file must exist in the
    destination. A mismatch raises ``RuntimeError`` so the caller can refuse to
    swap onto a half-seeded workspace (the upgrade then fails cleanly instead of
    silently losing files). Using the same relative path for both the write and
    the existence check makes the verification robust to per-backend path
    formatting.

    Args:
        src: Source backend (e.g. the live ``VirtualWorkspaceBackend``).
        dst: Destination backend (e.g. the freshly connected ``RemoteBackend``).
        exclude: Paths for which ``exclude(rel)`` is true are neither copied
            nor verified — the stateless upgrade leaves cloud-synced roots to
            the first pull.

    Returns:
        The number of files written.

    Raises:
        RuntimeError: If any source file is missing from the destination after
            the copy.
    """
    files = src.walk()
    if exclude is not None:
        files = [rel for rel in files if not exclude(rel)]
    written = 0
    skipped: list[str] = []
    for rel in files:
        try:
            data = src.read_file(rel, binary=True)
        except (OSError, FileNotFoundError) as e:
            # walk() can surface entries that aren't readable as regular files:
            # mount points (notably the OpenCloud `cloud/` rclone mount, which
            # stats as a non-dir over SFTP), symlinks, sockets, FIFOs. Skip them
            # instead of failing the whole upgrade — one un-copyable mount must
            # not abort a VM/sandbox swap. The cloud mount in particular is a
            # live rclone mount (not copyable); it is re-established separately
            # by the upgrade handler, which re-runs _setup_cloud_mount against
            # the new backend (knowledge-base/knowledge/issues/workspace_upgrade_drops_cloud_mount.md).
            logger.warning("Seed: skipping unreadable entry %r: %s", rel, e)
            skipped.append(rel)
            continue
        parent = posixpath.dirname(rel)
        if parent:
            dst.mkdir(parent)
        dst.write_file(rel, data)
        written += 1

    # Verify only the files we actually attempted to copy (skipped entries are
    # expected to be absent in the destination).
    expected = [rel for rel in files if rel not in skipped]
    missing = [rel for rel in expected if not dst.exists(rel)]
    if missing:
        raise RuntimeError(
            f"Workspace seed incomplete: {len(missing)} of {len(expected)} file(s) "
            f"missing in destination (e.g. {missing[:3]})"
        )

    logger.info(
        "Seeded %d file(s) (skipped %d unreadable) from %s to %s",
        written,
        len(skipped),
        type(src).__name__,
        type(dst).__name__,
    )
    return written


def seed_from_lite_workspace(
    workspace: Mapping[str, Any],
    *,
    owner_id: str,
    destination: "WorkspaceBackend",
    exclude: "Callable[[str], bool] | None" = None,
) -> int:
    """Copy an old lite workspace (``virtual`` mounts or ``none``) into ``destination``.

    Shared by the stateless container upgrade (C1) and the VM upgrade's
    recovery copy (C2). ``none`` has nothing durable to copy.
    """
    from types import SimpleNamespace

    from agent.core.backends.factory import create_lite_backend

    backend = workspace.get("backend")
    if backend == "none":
        return 0
    if backend != "virtual":
        raise ValueError(f"cannot seed from a {backend!r} workspace")
    source = create_lite_backend(
        SimpleNamespace(backend="virtual", mounts=list(workspace.get("mounts") or [])),
        job_id=owner_id,
    )
    return seed_workspace(source, destination, exclude=exclude)
