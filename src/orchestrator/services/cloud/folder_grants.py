"""Per-folder grants of the main cloud: one principal per folder, member and kind.

A project folder connector (``cloud_folder``, ``project-cloud:<project>``)
is mounted as a principal of the provider's own that is granted that one
folder and nothing else, never as the service account (main cloud as
connectors, "Per-folder grants"; slices 5 and 7). There is one per connector,
project member and kind:

* a **reader** (``srw-reader-f-<id>``, its group ``srw-rog-f-<id>`` granted
  read only): the ``read_only`` level. It is active only once the read-only
  probe protected mode uses has passed as that principal, so read-only is the
  credential's doing, not an rclone flag's;
* a **writer** (``srw-writer-f-<id>``, its group ``srw-rwg-f-<id>`` granted
  read, update, create and delete, never share): the ``read_write`` level.
  It cannot reach another project's folder.

``cloud_folder_grants`` (0520) records each one, named after its row. The
password is generated and stored encrypted before the first provider call,
so a retry (this process or another, after a crash) completes the same
principal; it is dropped once the principal is gone.

* :func:`ensure_folder_grant` returns the slot's active grant, or mints one
  under the slot's advisory lock (remote calls, a probe for a reader: never
  under the thread's lock). A failed mint is revoked and recorded ``failed``
  with a closed code (:data:`FAILURE_CODES`); it raises
  :class:`FolderGrantUnavailable`, and nothing takes the grant's place.
* :func:`lookup_active_folder_grant` only reads, so it is safe where the
  thread's advisory lock is held.
* :func:`revoke_folder_grants` requests the revoke of a selection and runs
  it at once, best effort: a project's delete, a member's removal. A user's
  delete requests it in its own transaction (``PostgresDB.delete_user``).
* :func:`folder_grant_sweep_loop` (leader-gated, beside the RO reader
  reconciler) finishes due revokes with backoff, gives up after
  :data:`MAX_REVOKE_ATTEMPTS` (``abandoned``, a security event) yet keeps
  trying every :data:`ABANDONED_RETRY_SECONDS`, since the principal may
  still be live; revokes a mint a lost process left behind, and revokes
  every active grant that no longer serves: its connector or project is
  gone, its member left the project (or, for a
  writer, is a viewer of it now), its installation is not the active one,
  or the project's folder is another now.

The adapter side is ``SupportsFolderGrant`` (``cloud.base``); a provider
without it keeps its service account, and the matrix says so.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import secrets
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID, uuid4

from orchestrator.services.cloud import ro_probe
from orchestrator.services.cloud.base import CloudMountSubject, SupportsFolderGrant
from orchestrator.services.cloud.errors import CloudBackendError
from orchestrator.services.cloud.handles import ProjectFolderHandle
from orchestrator.services.cloud.ro_engage import _dav_root_from_webdav_url

logger = logging.getLogger(__name__)

#: The grant kinds: the ``read_only`` level's principal and the
#: ``read_write`` level's.
KINDS: tuple[str, ...] = ("reader", "writer")
#: The kind each strict ``cloud_folder`` level is mounted with.
KIND_FOR_ACCESS: dict[str, str] = {"read_only": "reader", "read_write": "writer"}

#: Why a grant is unavailable, as closed codes: a mint's ``failure_code``
#: and a mount's exclusion ``detail``. ``backend_unavailable``: the folder's
#: installation cannot be reached, or is not the active one;
#: ``not_supported``: its provider has no per-folder grant;
#: ``folder_invalid``: the connector names no folder SRW grants;
#: ``provision_failed``: the provider refused a step; ``probe_failed``: a
#: reader's read-only probe did not pass; ``busy``: another mint of the slot
#: did not finish in time; ``revoked``: its revoke was requested while it was
#: minted; ``mint_abandoned``: a lost process left it half made;
#: ``not_member``: the user is no member of the folder's project (an
#: administrator neither); ``error``: anything else.
FAILURE_CODES: tuple[str, ...] = (
    "backend_unavailable",
    "not_supported",
    "folder_invalid",
    "provision_failed",
    "probe_failed",
    "busy",
    "revoked",
    "mint_abandoned",
    "not_member",
    "error",
)
#: Why a grant's revoke was requested (``revoke_reason``).
REVOKE_REASONS: tuple[str, ...] = (
    "mint_failed",
    "mint_abandoned",
    "connector_gone",
    "project_deleted",
    "member_removed",
    "role_lowered",
    "installation_inactive",
    "repointed",
    "credential_unreadable",
    "user_retired",
)

#: How long a mint may stay ``minting`` before the sweep treats it as lost
#: (a live one holds its slot's lock and finishes in seconds).
MINT_ABANDON_SECONDS = 600.0
#: Bounds of one revoke and of a sweep pass.
REVOKE_ROW_SECONDS = 30.0
REVOKES_PER_PASS = 50
MAX_REVOKE_ATTEMPTS = 12
REVOKE_RETRY_BASE_SECONDS = 30.0
REVOKE_RETRY_MAX_SECONDS = 3600.0
#: How often a revoke SRW gave up on is still tried: its principal may be a
#: live account in the main cloud, so ``abandoned`` is never the end.
ABANDONED_RETRY_SECONDS = REVOKE_RETRY_MAX_SECONDS
#: The security event a given-up revoke records (``security_events``,
#: which outlives the grant's connector, project and user).
REVOKE_ABANDONED_EVENT = "cloud_folder_grant_revoke_abandoned"
#: The sweep's interval: a backstop; the hooks revoke at once.
SWEEP_SECONDS = 300.0
#: How long a reader's probe waits for its canary to be removed.
CANARY_CLEANUP_SECONDS = 10.0


class FolderGrantUnavailable(Exception):
    """No grant for this folder, member and kind; ``code`` is one of
    :data:`FAILURE_CODES`, ``detail`` a line a person can read."""

    def __init__(self, code: str, detail: str = "") -> None:
        super().__init__(detail or code)
        self.code = code
        self.detail = detail or code


class _ProbeFailed(Exception):
    """The reader's read-only probe did not pass."""


def principal_ids(kind: str, grant_id: str) -> tuple[str, str]:
    """The principal and group a grant row of ``kind`` names (0520 checks
    the same spelling)."""
    hexid = UUID(str(grant_id)).hex
    if kind == "reader":
        return f"srw-reader-f-{hexid}", f"srw-rog-f-{hexid}"
    if kind == "writer":
        return f"srw-writer-f-{hexid}", f"srw-rwg-f-{hexid}"
    raise ValueError(f"unknown per-folder grant kind {kind!r}")


def _folder_sha256(folder_handle: str) -> str:
    return hashlib.sha256(folder_handle.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class FolderGrantTarget:
    """The folder a grant is for: one project folder connector and its
    project's current folder (the project's own columns, which the mount
    uses too, ``cloud_connector_mounts.bind_project_folder``)."""

    datasource_id: str
    project_id: str
    backend_id: str
    backend_instance_id: str
    folder_handle: str

    @property
    def folder_handle_sha256(self) -> str:
        return _folder_sha256(self.folder_handle)

    def handle(self) -> ProjectFolderHandle:
        return ProjectFolderHandle.from_db(self.folder_handle, backend=self.backend_id)

    @classmethod
    def from_mount_row(
        cls, row: Mapping[str, Any], *, project_id: str | None
    ) -> FolderGrantTarget | None:
        """From a bound project folder's mount row (the connector's id, the
        project's current provider, installation and handle), or ``None``
        when it names no folder."""
        values = (
            row.get("id"),
            project_id,
            row.get("backend_id"),
            row.get("backend_instance_id"),
            row.get("cloud_handle"),
        )
        if not all(values):
            return None
        return cls(*(str(value) for value in values))


async def folder_grant_target(
    store: Any, connector_row: Mapping[str, Any]
) -> FolderGrantTarget | None:
    """The folder ``connector_row`` (a project's own ``cloud_folder``
    connector) is granted on, or ``None``: not a project folder SRW created,
    or a project that is gone or names no folder now."""
    from orchestrator.services.connector_drivers.cloud_folder import (
        PROJECT_CLOUD,
        normalize_cloud_folder_config,
    )
    from shared.connectors.platform import platform_owned

    try:
        config = normalize_cloud_folder_config(connector_row.get("config"))
    except ValueError:
        return None
    project_id = config.get("project_id")
    if (
        config["folder_kind"] != "project"
        or not project_id
        or platform_owned(connector_row) != f"{PROJECT_CLOUD}:{project_id}"
    ):
        return None
    project = await store.get_project(project_id)
    if not project:
        return None
    return FolderGrantTarget.from_mount_row(
        {
            "id": connector_row.get("id"),
            "backend_id": project.get("main_cloud_backend"),
            "backend_instance_id": project.get("main_cloud_backend_instance_id"),
            "cloud_handle": project.get("main_cloud_folder_handle"),
        },
        project_id=str(project_id),
    )


@dataclass(frozen=True)
class FolderGrant:
    """One active grant: the principal a mount authenticates as."""

    id: str
    kind: str
    principal_id: str
    webdav_url: str
    password: str = field(repr=False)

    def subject(self) -> CloudMountSubject:
        """The mount subject the adapter's rclone spec is built for."""
        return CloudMountSubject(username=self.principal_id, credential=self.password)


def _matches(row: Mapping[str, Any], target: FolderGrantTarget) -> bool:
    return (
        str(row.get("backend_instance_id") or "") == target.backend_instance_id
        and str(row.get("backend_id") or "") == target.backend_id
        and str(row.get("project_id") or "") == target.project_id
        and row.get("folder_handle_sha256") == target.folder_handle_sha256
    )


def _usable(
    row: Mapping[str, Any] | None, target: FolderGrantTarget
) -> FolderGrant | None:
    if (
        not isinstance(row, Mapping)
        or row.get("status") != "active"
        or not row.get("credentials")
        or not _matches(row, target)
    ):
        return None
    return FolderGrant(
        id=str(row["id"]),
        kind=str(row["kind"]),
        principal_id=str(row["principal_id"]),
        webdav_url=str(row["webdav_url"]),
        password=str(row["credentials"]),
    )


async def lookup_active_folder_grant(
    store: Any, target: FolderGrantTarget, *, user_id: str, kind: str
) -> FolderGrant | None:
    """The slot's active grant on ``target``'s folder, or ``None``. Reads
    only: no lock, no provider call."""
    row = await store.get_live_cloud_folder_grant(
        target.datasource_id, str(user_id), kind
    )
    return _usable(row, target)


async def _resolve_backend(
    store: Any, router: Any, *, backend_instance_id: str, backend_id: str
) -> Any:
    """The exact retained installation, never the active one in its place."""
    try:
        backend = router.for_backend_instance(
            backend_instance_id, expected_backend_id=backend_id
        )
    except Exception:
        authority = await store.get_main_cloud_backend_instance(
            backend_instance_id, expected_backend_id=backend_id
        )
        if authority is None:
            raise FolderGrantUnavailable(
                "backend_unavailable", "the folder's main-cloud installation is unknown"
            ) from None
        try:
            backend = await router.resolve_backend_instance(authority)
        except Exception as exc:
            raise FolderGrantUnavailable(
                "backend_unavailable",
                "the folder's main-cloud installation cannot be reached",
            ) from exc
    if not getattr(backend, "is_initialized", False):
        raise FolderGrantUnavailable(
            "backend_unavailable", "the folder's main cloud is not ready"
        )
    if not isinstance(backend, SupportsFolderGrant):
        raise FolderGrantUnavailable(
            "not_supported", "the folder's main cloud has no per-folder grant"
        )
    return backend


async def _grant_backend(store: Any, router: Any, target: FolderGrantTarget) -> Any:
    active = getattr(router, "active_instance_id", None)
    if active and str(active) != target.backend_instance_id:
        # The sweep revokes grants of an inactive installation; minting one
        # would only be undone.
        raise FolderGrantUnavailable(
            "backend_unavailable",
            "the folder lives in a main-cloud installation that is not the active one",
        )
    return await _resolve_backend(
        store,
        router,
        backend_instance_id=target.backend_instance_id,
        backend_id=target.backend_id,
    )


async def _verify_read_only(
    backend: Any,
    handle: ProjectFolderHandle,
    *,
    principal_id: str,
    password: str,
    webdav_url: str,
) -> None:
    """The protected engage gate's probe, as the reader: version floors and
    every mutating verb and side channel rejected, with a positive read
    control. A copy of ``ro_engage.engage_ro_mount``'s probe block, which
    stays as it is."""
    canary = None
    client = backend.folder_principal_client(principal_id, password)
    try:
        canary = await backend.seed_canary_fixture(handle)
        floors = await ro_probe.check_version_floors(
            client, webdav_url, backend=backend.backend_id
        )
        if not floors.ok:
            raise _ProbeFailed(
                f"version floor check failed: {floors.failures or floors.inconclusive}"
            )
        result = await ro_probe.probe_read_only(
            client,
            webdav_url,
            canary.path,
            dav_root=_dav_root_from_webdav_url(webdav_url),
            username=principal_id,
            version_ref=canary.version_ref,
            trash_ref=canary.trash_ref,
        )
        if not result.ok:
            raise _ProbeFailed(
                "read-only probe did not pass: "
                f"failures={result.failures} inconclusive={result.inconclusive} "
                f"skipped={result.skipped}"
            )
    finally:
        if canary is not None:
            try:
                # Bounded: a slow trash purge must not hold the grant back
                # (a canary left in the trash only takes space).
                await asyncio.wait_for(
                    backend.remove_canary_fixture(handle, canary),
                    CANARY_CLEANUP_SECONDS,
                )
            except Exception:  # pragma: no cover - best effort
                logger.warning(
                    "failed to remove the folder reader's canary", exc_info=True
                )
        try:
            await client.aclose()
        except Exception:  # pragma: no cover - best effort
            pass


async def _mint(
    store: Any,
    router: Any,
    backend: Any,
    target: FolderGrantTarget,
    row: Mapping[str, Any],
) -> FolderGrant:
    """Make the provider side of a ``minting`` row, then activate it; on a
    failure, revoke it at once and raise."""
    grant_id = str(row["id"])
    kind = str(row["kind"])
    handle = target.handle()
    code = "error"
    try:
        await backend.ensure_folder_principal(
            handle,
            principal_id=str(row["principal_id"]),
            group_id=str(row["group_id"]),
            password=str(row["credentials"]),
            kind=kind,
        )
        if kind == "reader":
            await _verify_read_only(
                backend,
                handle,
                principal_id=str(row["principal_id"]),
                password=str(row["credentials"]),
                webdav_url=str(row["webdav_url"]),
            )
    except _ProbeFailed as exc:
        code, why = "probe_failed", str(exc)
    except CloudBackendError as exc:
        code, why = "provision_failed", f"{exc.kind.value}: {exc}"
    except Exception as exc:
        code, why = "error", type(exc).__name__
    else:
        active = await store.activate_cloud_folder_grant(
            grant_id, verified=kind == "reader"
        )
        grant = _usable(active, target)
        if grant is None:
            raise FolderGrantUnavailable(
                "revoked", "the grant was revoked while it was made"
            )
        logger.info(
            "Per-folder %s grant %s active for connector %s",
            kind,
            grant_id,
            target.datasource_id,
        )
        return grant
    logger.warning(
        "Per-folder %s grant %s for connector %s failed (%s): %s",
        kind,
        grant_id,
        target.datasource_id,
        code,
        why,
    )
    rows = await store.request_cloud_folder_grant_revokes(
        reason="mint_failed", grant_id=grant_id, failure_code=code
    )
    await _revoke_rows(store, router, rows)
    raise FolderGrantUnavailable(code, why)


async def ensure_folder_grant(
    store: Any,
    router: Any,
    target: FolderGrantTarget,
    *,
    user_id: str,
    kind: str,
    lock_timeout_s: float = 30.0,
) -> FolderGrant:
    """The active grant of ``target``'s folder for ``user_id`` and ``kind``,
    minted when there is none (or the one there is was made on another
    folder or installation, whose revoke this requests). Calls the provider:
    never under the thread's advisory lock. Raises
    :class:`FolderGrantUnavailable`."""
    if kind not in KINDS:
        raise ValueError(f"unknown per-folder grant kind {kind!r}")
    user_id = str(user_id)
    roles = await store.get_user_project_roles(user_id, [target.project_id])
    if str(target.project_id) not in roles:
        # Only a member of the folder's project, an administrator included:
        # the sweep revokes a non-member's grant (member_removed).
        raise FolderGrantUnavailable(
            "not_member", "the user is not a member of the folder's project"
        )
    found = await lookup_active_folder_grant(store, target, user_id=user_id, kind=kind)
    if found is not None:
        return found
    backend = await _grant_backend(store, router, target)
    try:
        async with store.cloud_folder_grant_lock(
            target.datasource_id, user_id, kind, wait_timeout_s=lock_timeout_s
        ):
            row = await store.get_live_cloud_folder_grant(
                target.datasource_id, user_id, kind
            )
            found = _usable(row, target)
            if found is not None:
                return found
            if row is not None and (
                not _matches(row, target) or not row.get("credentials")
            ):
                # Made on another folder, or its password no longer decrypts:
                # it serves nothing any more.
                stale = await store.request_cloud_folder_grant_revokes(
                    reason="repointed"
                    if not _matches(row, target)
                    else "credential_unreadable",
                    grant_id=str(row["id"]),
                    failure_code="mint_abandoned"
                    if row.get("status") == "minting"
                    else None,
                )
                await _revoke_rows(store, router, stale)
                row = None
            if row is None:
                grant_id = str(uuid4())
                principal_id, group_id = principal_ids(kind, grant_id)
                webdav_url = backend.folder_grant_webdav_url(
                    target.handle(), principal_id
                )
                if not webdav_url:
                    raise FolderGrantUnavailable(
                        "folder_invalid", "the connector's folder has no WebDAV path"
                    )
                row = await store.insert_cloud_folder_grant(
                    grant_id=grant_id,
                    kind=kind,
                    datasource_id=target.datasource_id,
                    project_id=target.project_id,
                    user_id=user_id,
                    backend_id=target.backend_id,
                    backend_instance_id=target.backend_instance_id,
                    folder_handle_sha256=target.folder_handle_sha256,
                    principal_id=principal_id,
                    group_id=group_id,
                    credentials=secrets.token_urlsafe(32),
                    webdav_url=webdav_url,
                )
                if row is None:
                    raise FolderGrantUnavailable(
                        "busy", "another mint of this grant is under way"
                    )
            # A ``minting`` row here is a lost attempt's (a live one would
            # hold this lock): completing it reuses its stored password.
            return await _mint(store, router, backend, target, row)
    except TimeoutError:
        raise FolderGrantUnavailable(
            "busy", "another mint of this grant did not finish in time"
        ) from None


#: This process's mints in flight, by slot: a caller that stops waiting
#: leaves its mint running (an interrupted one would be redone from its
#: probe), and the next caller joins it rather than queue on its lock.
_MINTS: dict[tuple[str, str, str], asyncio.Task[FolderGrant]] = {}


def start_folder_grant(
    store: Any,
    router: Any,
    target: FolderGrantTarget,
    *,
    user_id: str,
    kind: str,
) -> asyncio.Task[FolderGrant]:
    """:func:`ensure_folder_grant` as a task this module keeps until it
    ends; a caller bounds its own wait with ``asyncio.shield``."""
    key = (target.datasource_id, str(user_id), kind)
    task = _MINTS.get(key)
    if task is None or task.done():
        task = asyncio.create_task(
            ensure_folder_grant(store, router, target, user_id=str(user_id), kind=kind)
        )
        _MINTS[key] = task

        def forget(done: asyncio.Task[FolderGrant], key=key) -> None:
            if _MINTS.get(key) is done:
                del _MINTS[key]
            if not done.cancelled():
                done.exception()  # retrieved: a failure is the caller's to log

        task.add_done_callback(forget)
    return task


# --------------------------------------------------------------------------- #
# Revoke
# --------------------------------------------------------------------------- #


def _backoff(attempt: int) -> float:
    return min(
        REVOKE_RETRY_BASE_SECONDS * 2 ** max(0, attempt - 1), REVOKE_RETRY_MAX_SECONDS
    )


async def _record_abandoned(
    store: Any, row: Mapping[str, Any], *, attempts: int, why: str
) -> None:
    """The security event of a revoke SRW gave up on; never raises."""
    try:
        await store.record_security_event(
            event_type=REVOKE_ABANDONED_EVENT,
            resource_type="cloud_folder_grant",
            user_id=str(row.get("user_id") or "") or None,
            resource_id=str(row["id"]),
            detail=(
                f"principal={row.get('principal_id')} group={row.get('group_id')} "
                f"kind={row.get('kind')} project={row.get('project_id')} "
                f"connector={row.get('datasource_id')} "
                f"installation={row.get('backend_instance_id')} "
                f"reason={row.get('revoke_reason')} attempts={attempts} "
                f"error={why[:200]}"
            ),
        )
    except Exception:
        logger.error(
            "Per-folder grant %s: its abandoned revoke was not recorded as a "
            "security event",
            row.get("id"),
        )


async def _revoke_one(store: Any, router: Any, row: Mapping[str, Any]) -> str:
    """One ``revoking`` (or ``abandoned``) grant: ``revoked``, ``retried``
    (later, with backoff) or ``abandoned`` (given up now: a security event,
    then tried every :data:`ABANDONED_RETRY_SECONDS`)."""
    given_up = row.get("status") == "abandoned"
    attempts = int(row.get("revoke_attempts") or 0) + 1
    try:
        backend = await _resolve_backend(
            store,
            router,
            backend_instance_id=str(row["backend_instance_id"]),
            backend_id=str(row["backend_id"]),
        )
        await asyncio.wait_for(
            backend.revoke_folder_principal(
                principal_id=str(row["principal_id"]), group_id=str(row["group_id"])
            ),
            REVOKE_ROW_SECONDS,
        )
    except Exception as exc:
        if isinstance(exc, FolderGrantUnavailable):
            why = f"{exc.code}: {exc.detail}"
        elif isinstance(exc, CloudBackendError):
            why = f"{exc.kind.value}: {exc}"
        elif isinstance(exc, (asyncio.TimeoutError, TimeoutError)):
            why = "the revoke took too long"
        else:
            why = type(exc).__name__
        if given_up:
            await store.retry_cloud_folder_grant_revoke(
                str(row["id"]),
                attempts=attempts,
                error=why[:500],
                delay_seconds=ABANDONED_RETRY_SECONDS,
            )
            logger.warning(
                "Revoking per-folder grant %s (%s), given up on, failed again "
                "(attempt %d): %s; its principal may be live in the main cloud",
                row["id"],
                row.get("principal_id"),
                attempts,
                why,
            )
            return "retried"
        if attempts >= MAX_REVOKE_ATTEMPTS:
            if await store.finish_cloud_folder_grant_revoke(
                str(row["id"]),
                abandoned=True,
                error=why[:500],
                attempts=attempts,
                retry_seconds=ABANDONED_RETRY_SECONDS,
            ):
                await _record_abandoned(store, row, attempts=attempts, why=why)
            logger.warning(
                "Giving up revoking per-folder grant %s (%s) after %d attempts: %s; "
                "its principal may be left in the main cloud, and SRW tries "
                "again every %.0fs",
                row["id"],
                row.get("principal_id"),
                attempts,
                why,
                ABANDONED_RETRY_SECONDS,
            )
            return "abandoned"
        await store.retry_cloud_folder_grant_revoke(
            str(row["id"]),
            attempts=attempts,
            error=why[:500],
            delay_seconds=_backoff(attempts),
        )
        logger.warning(
            "Revoking per-folder grant %s failed, attempt %d: %s",
            row["id"],
            attempts,
            why,
        )
        return "retried"
    await store.finish_cloud_folder_grant_revoke(str(row["id"]))
    logger.info(
        "Per-folder grant %s revoked (%s)%s",
        row["id"],
        row.get("revoke_reason"),
        f" after it was given up, attempt {attempts}" if given_up else "",
    )
    return "revoked"


async def _revoke_rows(
    store: Any, router: Any, rows: list[Mapping[str, Any]]
) -> dict[str, int]:
    """Revoke ``rows`` (already ``revoking``) now, one at a time; a failure
    is left to the sweep."""
    outcomes: dict[str, int] = {}
    for row in rows:
        try:
            outcome = await _revoke_one(store, router, row)
        except Exception:
            logger.warning(
                "Revoking per-folder grant %s failed", row.get("id"), exc_info=True
            )
            outcome = "retried"
        outcomes[outcome] = outcomes.get(outcome, 0) + 1
    return outcomes


async def revoke_folder_grants(
    store: Any,
    router: Any,
    *,
    reason: str,
    datasource_id: str | None = None,
    project_id: str | None = None,
    user_id: str | None = None,
    kind: str | None = None,
) -> int:
    """Request the revoke of every live grant the selection names (of one
    ``kind`` only, when given) and run it at once; returns how many were
    revoked now. Best effort: it never raises, and what it could not finish
    the sweep does."""
    if reason not in REVOKE_REASONS:
        raise ValueError(f"unknown per-folder grant revoke reason {reason!r}")
    try:
        rows = await store.request_cloud_folder_grant_revokes(
            reason=reason,
            datasource_id=datasource_id,
            project_id=project_id,
            user_id=user_id,
            kind=kind,
        )
        if not rows:
            return 0
        outcomes = await _revoke_rows(store, router, rows)
    except Exception:
        logger.warning(
            "Revoking per-folder grants (%s, project %s, user %s) failed; the "
            "sweep retries",
            reason,
            project_id,
            user_id,
            exc_info=True,
        )
        return 0
    return outcomes.get("revoked", 0)


# --------------------------------------------------------------------------- #
# Sweep
# --------------------------------------------------------------------------- #


@dataclass
class FolderGrantSweepReport:
    abandoned_mints: int = 0
    stale: int = 0
    revoked: int = 0
    retried: int = 0
    given_up: int = 0

    def any(self) -> bool:
        return any(vars(self).values())


async def sweep_folder_grants_once(store: Any, router: Any) -> FolderGrantSweepReport:
    """One pass: revoke lost mints and every active grant that no longer
    serves (``PostgresDB.list_stale_cloud_folder_grants``), then run the due
    revokes, at most :data:`REVOKES_PER_PASS`."""
    report = FolderGrantSweepReport()
    report.abandoned_mints = await store.request_abandoned_cloud_folder_grant_mints(
        older_than_seconds=MINT_ABANDON_SECONDS
    )
    active = getattr(router, "active_instance_id", None)
    for row in await store.list_stale_cloud_folder_grants(
        active_instance_id=str(active) if active else None
    ):
        report.stale += len(
            await store.request_cloud_folder_grant_revokes(
                reason=str(row["reason"]), grant_id=str(row["id"])
            )
        )
    for row in await store.list_due_cloud_folder_grant_revokes(limit=REVOKES_PER_PASS):
        outcome = await _revoke_one(store, router, row)
        if outcome == "revoked":
            report.revoked += 1
        elif outcome == "abandoned":
            report.given_up += 1
        else:
            report.retried += 1
    return report


async def folder_grant_sweep_loop(
    shutdown_event: asyncio.Event,
    *,
    store: Any,
    router: Callable[[], Any],
    interval_seconds: float = SWEEP_SECONDS,
) -> None:
    """Leader-gated loop: :func:`sweep_folder_grants_once` every interval.

    Best effort: a failed pass is logged and the next one runs on time;
    every step is re-derived from the table, so a pass a leadership change
    cancels is run again by the next leader. Returns only on shutdown.
    """
    logger.info("Cloud folder grant sweep started (every %.0fs)", interval_seconds)
    while not shutdown_event.is_set():
        try:
            report = await sweep_folder_grants_once(store, router())
            if report.any():
                logger.info("cloud folder grants: %s", vars(report))
        except Exception as exc:
            logger.warning("cloud folder grant sweep error: %s", exc)
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=interval_seconds)
            break
        except asyncio.TimeoutError:
            pass
    logger.info("Cloud folder grant sweep stopped")


__all__ = [
    "ABANDONED_RETRY_SECONDS",
    "FAILURE_CODES",
    "KINDS",
    "KIND_FOR_ACCESS",
    "REVOKE_ABANDONED_EVENT",
    "REVOKE_REASONS",
    "FolderGrant",
    "FolderGrantSweepReport",
    "FolderGrantTarget",
    "FolderGrantUnavailable",
    "ensure_folder_grant",
    "folder_grant_sweep_loop",
    "folder_grant_target",
    "lookup_active_folder_grant",
    "principal_ids",
    "revoke_folder_grants",
    "start_folder_grant",
    "sweep_folder_grants_once",
]
