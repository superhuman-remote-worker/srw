"""A user's own root in the main cloud, as a connector (main cloud slice 4).

Every approved user whose main cloud offers ``cloud_folder · user_root ·
read_write`` has one ``cloud_folder`` connector SRW keeps for them, under
the managed key ``user-cloud-root:<user>`` (``shared.connectors.platform``):
"My cloud", recorded as created by the user, linked to their personal
project only and a default of its work (``auto_attach``), so a session in
the personal project preselects it and nowhere else offers it. Only its
user may attach it, an administrator or a member of the personal project
included (``native_user``). The API refuses to create, edit, link, unlink
or delete it (``managed_refusal``); it goes with its user's account
(``manifest_user_retirement``).

The row starts **not connected**: it names the user's account but holds no
credential. The user connects it once (``POST /api/me/cloud-root/connect``,
"Connect your cloud"): SRW starts the provider's consent flow
(``SupportsUserRootGrant``), the user approves it in their browser, and a
background poller here (every :data:`POLL_SECONDS`, for at most
:data:`FLOW_SECONDS`) takes the credential, checks whose account it is
(:func:`complete_connect`: the account's e-mail must be the SRW user's,
and an administrator of the cloud is refused), proves it reads the
account's files, and only then stores it as the row's encrypted
credentials. Any refusal revokes the new credential at once; a reconnect
revokes the previous one once the new one is stored. A disconnect revokes
it and clears it.

What a session mounts: :func:`bind_user_root_folder`, the ``user_root``
binder of the connector mount rule (``cloud_connector_mounts``), reads the
row only (it runs under the thread's lock). A row without a credential is
excluded ``cloud_not_connected``, one whose credential the cloud refused
``cloud_grant_revoked``; nothing ever takes its place. Before each new Pod,
outside the lock, :func:`verify_selected_user_root` probes the credential
once and records a refusal.

The credential is never logged, raised, put in an exclusion, a thread's
metadata, a plan or a security event: it lives in the row's encrypted
credentials (and the connector's resource secret) and reaches only the
Pod's mount secret.

The connector's ``config`` holds the non-secret facts and changes only on
a transition: ``account_id`` (the cloud account it is connected to) and
``folder_handle`` once connected, ``grant`` (``state`` connected or revoked,
``source``, ``client_name``, ``granted_at``, ``revoked_at``, ``reason``) and
``connect_failure`` (the last attempt's closed ``reason`` and ``at``).

Lifecycle: :func:`ensure_user_root_connector` creates the row when a user
is admitted (first login, approval, an administrator's create, the status
read) and re-points it when the active installation changed (its
credential cleared: the user connects again);
:func:`backfill_user_root_connectors` runs it for every approved user at
startup and logs one count line.

Design: knowledge-base/knowledge/features/main_cloud_as_connectors.md
(slice 4, "Overnight run: slices 3 to 8") and
nextcloud_user_home_app_token_grant.md (Lifecycle, Security).
"""

from __future__ import annotations

import asyncio
import functools
import logging
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from orchestrator.services.cloud.errors import CloudBackendError
from orchestrator.services.cloud.user_root_grant import (
    LoginFlowGrant,
    SupportsUserRootGrant,
)
from orchestrator.services.connector_drivers.cloud_folder import (
    CloudFolderUnavailable,
)
from orchestrator.services.connector_drivers.platform import (
    validate_platform_connector,
)
from orchestrator.services.connector_drivers.registry import (
    ConnectorDriverRegistry,
    builtin_connector_drivers,
)
from shared.connectors.platform import USER_CLOUD_ROOT, managed_key, native_user

logger = logging.getLogger(__name__)

#: The stored type of the main cloud's folders (``srw.cloud-folder/v1``).
CLOUD_FOLDER_TYPE = "cloud_folder"
#: The support-matrix cell a provider declares to have user roots.
USER_ROOT_FOLDER = ("cloud_folder", "user_root", "read_write")
#: The name every user's root carries, and the workspace name it mounts at
#: (``cloud/my_cloud``).
ROOT_NAME = "My cloud"
MOUNT_NAME = "my_cloud"
ROOT_DESCRIPTION = (
    "Your own files in the main cloud: everything you see in Files, project "
    "folders you belong to included. Connect it once; your personal "
    "project's sessions mount it with full access."
)
#: The only way a credential is acquired today.
SOURCE_LOGIN_FLOW = "login_flow"

#: How often a connect flow asks the cloud whether the user finished, and
#: for how long (the provider's own flow lives about as long).
POLL_SECONDS = 3.0
FLOW_SECONDS = 20 * 60

#: What the status read reports (``GET /api/me/cloud-root``).
STATES: tuple[str, ...] = (
    "unsupported",
    "not_connected",
    "connecting",
    "connected",
    "revoked",
    "failed",
)
#: Why a user has no root to connect: no main cloud, a provider without
#: user roots, the deployment turned grants off, no personal project.
UNSUPPORTED_REASONS: tuple[str, ...] = (
    "no_main_cloud",
    "provider_unsupported",
    "grant_mode_off",
    "no_personal_project",
)
#: Why a connect attempt stored nothing (``config.connect_failure.reason``):
#: the account is another person's (its e-mail is not the user's, or
#: either has none), it administers the cloud or one of its groups, it is
#: one of SRW's own service accounts, it is not the account SRW already
#: knows as the user's in this installation (``users.cloud_identity``), it
#: is disabled, its credential could not read its files, the user did not
#: finish in time, the cloud could not be asked, or anything else went
#: wrong. A probe at a Pod's start records ``account_missing`` when the
#: account has no files.
CONNECT_FAILURES: tuple[str, ...] = (
    "wrong_account",
    "admin_account",
    "service_account",
    "identity_mismatch",
    "account_disabled",
    "verify_failed",
    "expired",
    "provider_error",
    "account_missing",
    "error",
)
#: Why a credential was revoked (``config.grant.reason``): its user
#: disconnected it, or the cloud refused it (revoked in its device list,
#: the account disabled).
REVOKE_REASONS: tuple[str, ...] = ("user", "rejected_by_cloud")
#: What :func:`ensure_user_root_connector` did, as the backfill counts it.
OUTCOMES: tuple[str, ...] = (
    "created",
    "present",
    "repointed",
    "skipped_unsupported",
    "skipped_unapproved",
    "skipped_no_project",
    "deferred",
)
#: The exclusion reasons the binder adds to the mount rule's.
EXCLUSION_REASONS: tuple[str, ...] = ("cloud_not_connected", "cloud_grant_revoked")
#: The most users one backfill pass reads.
_BACKFILL_LIMIT = 100_000

#: What names the installation a root's account lives in.
_INSTALLATION_KEYS = ("backend_id", "backend_instance_id")


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime) -> str:
    return value.isoformat()


@functools.cache
def _builtin_registry() -> ConnectorDriverRegistry:
    return builtin_connector_drivers()


def _registry(store: Any) -> ConnectorDriverRegistry:
    registry = getattr(store, "connector_drivers", None)
    if isinstance(registry, ConnectorDriverRegistry):
        return registry
    return _builtin_registry()


def user_root_key(user_id: Any) -> str:
    """The managed key of ``user_id``'s own cloud root connector."""
    return managed_key(USER_CLOUD_ROOT, user_id)


def is_user_root(row: Mapping[str, Any] | None) -> bool:
    """Whether ``row`` is a user's own cloud root connector."""
    if not row or native_user(row) is None:
        return False
    key = row.get("managed_key")
    return isinstance(key, str) and key.startswith(f"{USER_CLOUD_ROOT}:")


# --------------------------------------------------------------------------- #
# Which cloud offers a root
# --------------------------------------------------------------------------- #


def _offers_user_root(backend: Any) -> bool:
    declared = getattr(backend, "capabilities", None)
    return bool(
        declared is not None
        and hasattr(declared, "offers")
        and declared.offers(*USER_ROOT_FOLDER)
    )


def active_root_provider(cloud_router: Any) -> tuple[Any | None, str | None]:
    """The active main cloud when it has user roots to connect, else why
    not (one of :data:`UNSUPPORTED_REASONS` but ``no_personal_project``)."""
    if (
        cloud_router is None
        or getattr(cloud_router, "active_instance_id", None) is None
    ):
        return None, "no_main_cloud"
    backend = getattr(cloud_router, "active", None)
    if backend is None or not getattr(backend, "is_initialized", False):
        return None, "no_main_cloud"
    if not isinstance(backend, SupportsUserRootGrant) or not _offers_user_root(backend):
        return None, "provider_unsupported"
    if backend.user_root_grant_mode == "off":
        return None, "grant_mode_off"
    return backend, None


def _row_provider(cloud_router: Any, row: Mapping[str, Any]) -> Any | None:
    """The installation ``row`` names, when the router still holds it and
    it can grant a root; ``None`` otherwise."""
    config = row.get("config") if isinstance(row.get("config"), Mapping) else {}
    instance = config.get("backend_instance_id")
    backend_id = config.get("backend_id")
    if cloud_router is None or not instance or not backend_id:
        return None
    try:
        backend = cloud_router.for_backend_instance(
            str(instance), expected_backend_id=str(backend_id)
        )
    except Exception:
        return None
    if not getattr(backend, "is_initialized", False) or not isinstance(
        backend, SupportsUserRootGrant
    ):
        return None
    return backend


# --------------------------------------------------------------------------- #
# The row
# --------------------------------------------------------------------------- #


def _config(row: Mapping[str, Any] | None) -> dict[str, Any]:
    config = row.get("config") if row else None
    return dict(config) if isinstance(config, Mapping) else {}


def base_config(project_id: Any, backend: Any) -> dict[str, Any]:
    """A not-connected root's config: the account's cloud, no account yet."""
    return {
        "folder_kind": "user_root",
        "project_id": str(project_id),
        "backend_id": str(backend.backend_id),
        "backend_instance_id": str(backend.backend_instance_id),
    }


async def _user_row(store: Any, user: Mapping[str, Any] | Any) -> dict[str, Any] | None:
    if isinstance(user, Mapping) and {"id", "is_approved", "default_project_id"} <= set(
        user
    ):
        return dict(user)
    user_id = user.get("id") if isinstance(user, Mapping) else user
    if not user_id:
        return None
    found = await store.get_user(str(user_id))
    return dict(found) if found else None


async def _ensure(store: Any, cloud_router: Any, user: Any) -> str:
    backend, _reason = active_root_provider(cloud_router)
    if backend is None:
        return "skipped_unsupported"
    row_user = await _user_row(store, user)
    if row_user is None or not row_user.get("is_approved"):
        return "skipped_unapproved"
    project_id = row_user.get("default_project_id")
    if not project_id:
        return "skipped_no_project"
    user_id = str(row_user["id"])
    key = user_root_key(user_id)
    wanted = base_config(project_id, backend)
    existing = await store.get_datasource_by_managed_key(key)
    if existing is not None:
        current = _config(existing)
        if all(current.get(k) == wanted[k] for k in _INSTALLATION_KEYS):
            return "present"
        # The active installation changed: the old credential names another
        # cloud's account. Revoke it there if that cloud is still reachable,
        # and let the user connect again. The row keeps its project.
        wanted["project_id"] = current.get("project_id") or wanted["project_id"]
        await _repoint(store, cloud_router, existing, wanted)
        return "repointed"
    normalized = await validate_platform_connector(
        _registry(store),
        CLOUD_FOLDER_TYPE,
        name=ROOT_NAME,
        connection_url=None,
        config=wanted,
    )
    created = await store.create_datasource(
        name=ROOT_NAME,
        ds_type=CLOUD_FOLDER_TYPE,
        connection_url=None,
        description=ROOT_DESCRIPTION,
        credentials=normalized.credentials,
        config=normalized.config or wanted,
        created_by=user_id,
        read_only=None,
        scope_mode="projects",
        auto_attach=True,
        project_ids=[str(project_id)],
        managed_key=key,
    )
    # None: another pass (a replica, a rerun) created it first.
    return "created" if created else "present"


async def _repoint(
    store: Any,
    cloud_router: Any,
    existing: Mapping[str, Any],
    wanted: dict[str, Any],
) -> None:
    full = await store.get_datasource(str(existing["id"])) or existing
    credentials = full.get("credentials") or {}
    if credentials.get("password"):
        old = _row_provider(cloud_router, full)
        if old is not None:
            await old.revoke_user_root(
                str(credentials.get("username") or ""), str(credentials["password"])
            )
    await store.update_datasource(str(existing["id"]), credentials={}, config=wanted)
    logger.info(
        "User cloud root %s re-pointed to installation %s; its user connects again",
        existing.get("id"),
        wanted["backend_instance_id"],
    )


async def ensure_user_root_connector(
    store: Any, cloud_router: Any, user: Any, *, log: Any = None
) -> str:
    """Create or re-point ``user``'s own cloud root connector (a user row,
    or a user id). Returns one of :data:`OUTCOMES`; a failure is logged and
    answers ``deferred``. Never raises."""
    log = log or logger
    try:
        return await _ensure(store, cloud_router, user)
    except Exception as exc:
        log.warning(
            "User %s: cloud root connector not ensured (%s); the next start retries it",
            user.get("id") if isinstance(user, Mapping) else user,
            exc,
        )
        return "deferred"


async def ensure_user_root_if_cloud(
    store: Any, cloud_router: Any, user: Any, *, log: Any = None
) -> str | None:
    """:func:`ensure_user_root_connector` when the active main cloud has
    user roots, else ``None``. Never raises."""
    if not user or active_root_provider(cloud_router)[0] is None:
        return None
    return await ensure_user_root_connector(store, cloud_router, user, log=log)


async def backfill_user_root_connectors(
    store: Any, cloud_router: Any, *, log: Any = None
) -> dict[str, int]:
    """Give every approved user their cloud root connector, and re-point
    the ones another installation's. Idempotent: a second pass finds every
    row ``present``. Logs one count line, an error when a user was
    ``deferred``."""
    log = log or logger
    counts = dict.fromkeys(OUTCOMES, 0)
    _backend, reason = active_root_provider(cloud_router)
    if reason is not None:
        log.info("User cloud root connectors: none to keep (%s)", reason)
        return counts
    for user in await store.list_users(limit=_BACKFILL_LIMIT):
        if not user.get("is_approved"):
            continue
        outcome = await ensure_user_root_connector(store, cloud_router, user, log=log)
        counts[outcome] += 1
    (log.error if counts["deferred"] else log.info)(
        "User cloud root connectors: %s", counts
    )
    return counts


# --------------------------------------------------------------------------- #
# State
# --------------------------------------------------------------------------- #


def _at(value: Any) -> str:
    return str(value or "")


def root_state(row: Mapping[str, Any] | None, *, connecting: bool = False) -> str:
    """The row's state, as :data:`STATES` names it (``unsupported`` is the
    caller's)."""
    if row is None:
        return "unsupported"
    if connecting:
        return "connecting"
    config = _config(row)
    grant = config.get("grant") if isinstance(config.get("grant"), Mapping) else {}
    if grant.get("state") == "connected" and config.get("account_id"):
        return "connected"
    failure = config.get("connect_failure")
    if isinstance(failure, Mapping) and (
        grant.get("state") != "revoked"
        or _at(failure.get("at")) >= _at(grant.get("revoked_at"))
    ):
        return "failed"
    if grant.get("state") == "revoked":
        return "revoked"
    return "not_connected"


def public_status(
    row: Mapping[str, Any] | None,
    *,
    reason: str | None = None,
    flow: "ConnectFlow | None" = None,
) -> dict[str, Any]:
    """The status read's body: never a credential."""
    if row is None or reason is not None:
        return {
            "supported": False,
            "reason": reason or "provider_unsupported",
            "state": "unsupported",
            "connector_id": str(row["id"]) if row else None,
            "name": ROOT_NAME,
            "account_id": None,
            "granted_at": None,
            "client_name": None,
            "revoked_at": None,
            "revoked_reason": None,
            "failure": None,
            "connecting": None,
        }
    config = _config(row)
    grant = config.get("grant") if isinstance(config.get("grant"), Mapping) else {}
    failure = config.get("connect_failure")
    state = root_state(row, connecting=flow is not None)
    return {
        "supported": True,
        "reason": None,
        "state": state,
        "connector_id": str(row["id"]),
        "name": str(row.get("name") or ROOT_NAME),
        "account_id": config.get("account_id") if state == "connected" else None,
        "granted_at": grant.get("granted_at") if state == "connected" else None,
        "client_name": grant.get("client_name"),
        "revoked_at": grant.get("revoked_at")
        if grant.get("state") == "revoked"
        else None,
        "revoked_reason": grant.get("reason")
        if grant.get("state") == "revoked"
        else None,
        "failure": (
            {"reason": failure.get("reason"), "at": failure.get("at")}
            if isinstance(failure, Mapping)
            else None
        ),
        "connecting": (
            {"login_url": flow.login_url, "expires_at": _iso(flow.expires_at)}
            if flow is not None
            else None
        ),
    }


async def own_root(
    store: Any, cloud_router: Any, user: Mapping[str, Any]
) -> tuple[dict[str, Any] | None, str | None]:
    """The caller's root row (ensured first), or why there is none."""
    _backend, reason = active_root_provider(cloud_router)
    if reason is None:
        row_user = await _user_row(store, user) or {}
        if not row_user.get("default_project_id"):
            reason = "no_personal_project"
        else:
            await ensure_user_root_connector(store, cloud_router, row_user)
    row = await store.get_datasource_by_managed_key(user_root_key(user["id"]))
    return (dict(row) if row else None), reason


# --------------------------------------------------------------------------- #
# Connect, complete, disconnect
# --------------------------------------------------------------------------- #


async def _security_event(
    store: Any,
    event_type: str,
    *,
    user_id: str,
    connector_id: str,
    detail: str | None = None,
) -> None:
    try:
        await store.record_security_event(
            event_type=event_type,
            resource_type="cloud_root",
            user_id=user_id,
            resource_id=connector_id,
            detail=detail,
        )
    except Exception:
        logger.warning("Cloud root %s: %s not recorded", connector_id, event_type)


async def _record_failure(store: Any, row_id: str, reason: str) -> None:
    """The last attempt's failure on the row; the credential is untouched."""
    row = await store.get_datasource(row_id)
    if row is None:
        return
    config = _config(row)
    config["connect_failure"] = {"reason": reason, "at": _iso(_now())}
    await store.update_datasource(row_id, config=config)


def _same_email(left: Any, right: Any) -> bool:
    left, right = str(left or "").strip(), str(right or "").strip()
    return bool(left) and bool(right) and left.casefold() == right.casefold()


async def _known_account_id(store: Any, user_id: str, backend: Any) -> str | None:
    """The cloud account SRW already resolved as ``user_id``'s in this
    installation (``users.cloud_identity``), or ``None``.

    The cache is keyed by provider; an entry that names another
    installation is not this one's. One that names none (resolved before
    entries carried it) is taken as this installation's, as every other
    reader of the cache takes it.
    """
    identity = await store.get_user_cloud_identity(user_id)
    entry = identity.get(backend.backend_id) if isinstance(identity, Mapping) else None
    if not isinstance(entry, Mapping) or not entry.get("user_id"):
        return None
    instance = entry.get("backend_instance_id")
    current = getattr(backend, "backend_instance_id", None)
    if instance and current and str(instance) != str(current):
        return None
    return str(entry["user_id"])


async def complete_connect(
    store: Any,
    backend: SupportsUserRootGrant,
    user: Mapping[str, Any],
    grant: LoginFlowGrant,
) -> str:
    """Store the credential a finished consent flow handed SRW, if it is
    the user's own account's. Returns ``connected`` or a
    :data:`CONNECT_FAILURES` reason; a refused credential is revoked.

    The order matters: whose account (asked with the credential itself,
    then the cloud's own record as its administrator: no administrator or
    group admin, none of SRW's service accounts, the user's e-mail, and the
    account SRW already knows as theirs when it knows one), then a probe of
    its files, then stored, then the previous credential revoked.
    """
    user_id = str(user["id"])
    key = user_root_key(user_id)
    row = await store.get_datasource_by_managed_key(key)
    if row is None:
        await backend.revoke_user_root(grant.login_name, grant.credential)
        return "error"
    row_id = str(row["id"])
    # The account's e-mail is compared with the user's as stored now.
    current_user = await store.get_user(user_id) or user

    async def refuse(reason: str) -> str:
        revoked = await backend.revoke_user_root(grant.login_name, grant.credential)
        await _record_failure(store, row_id, reason)
        await _security_event(
            store,
            "cloud_root_rejected",
            user_id=user_id,
            connector_id=row_id,
            detail=f"{reason}; credential revoked: {'yes' if revoked else 'no'}",
        )
        logger.info("Cloud root %s: connect refused (%s)", row_id, reason)
        return reason

    try:
        account_id = await backend.user_root_account_id(
            grant.login_name, grant.credential
        )
        if not account_id:
            return await refuse("verify_failed")
        account = await backend.user_root_account(account_id)
    except CloudBackendError:
        return await refuse("provider_error")
    if account is None:
        return await refuse("verify_failed")
    if account.administrator:
        return await refuse("admin_account")
    if account.service_account:
        return await refuse("service_account")
    if not _same_email(account.email, current_user.get("email")):
        return await refuse("wrong_account")
    # An e-mail can be set on any account (by its user, or a group admin):
    # once SRW knows which account is the user's here, only that one is.
    try:
        known = await _known_account_id(store, user_id, backend)
    except Exception:
        logger.warning("Cloud root %s: the user's cloud identity was not read", row_id)
        return await refuse("error")
    if known is not None and known != account.account_id:
        return await refuse("identity_mismatch")
    if not account.enabled:
        return await refuse("account_disabled")
    if (
        await backend.verify_user_root(
            account.account_id, grant.login_name, grant.credential
        )
        != "ok"
    ):
        return await refuse("verify_failed")

    full = await store.get_datasource(row_id) or row
    previous = dict(full.get("credentials") or {})
    config = _config(full)
    config.pop("connect_failure", None)
    config.update(
        {
            "folder_handle": backend.user_root_handle(account.account_id).to_db(),
            "account_id": account.account_id,
            "grant": {
                "state": "connected",
                "source": SOURCE_LOGIN_FLOW,
                "client_name": backend.user_root_client_name,
                "granted_at": _iso(_now()),
            },
        }
    )
    try:
        stored = await store.update_datasource(
            row_id,
            credentials={"username": grant.login_name, "password": grant.credential},
            config=config,
        )
    except Exception:
        stored = False
        logger.warning("Cloud root %s: the new credential was not stored", row_id)
    if not stored:
        # Never leave a credential alive that SRW does not hold.
        await backend.revoke_user_root(grant.login_name, grant.credential)
        await _record_failure(store, row_id, "error")
        return "error"
    if previous.get("password") and previous.get("password") != grant.credential:
        if not await backend.revoke_user_root(
            str(previous.get("username") or ""), str(previous["password"])
        ):
            logger.warning(
                "Cloud root %s: the previous credential could not be revoked; "
                "it stays in the account's device list until its user removes it",
                row_id,
            )
    try:
        await store.merge_user_cloud_identity(
            user_id,
            backend.backend_id,
            {
                "user_id": account.account_id,
                "resolved_at": _iso(_now()),
                "verified_by": "user_root_grant",
                "backend_instance_id": str(
                    getattr(backend, "backend_instance_id", "") or ""
                )
                or None,
            },
        )
    except Exception:
        logger.warning("User %s: cloud identity not recorded", user_id)
    await _security_event(
        store,
        "cloud_root_granted",
        user_id=user_id,
        connector_id=row_id,
        detail=f"source={SOURCE_LOGIN_FLOW}",
    )
    logger.info("Cloud root %s connected", row_id)
    return "connected"


async def revoke_root_credential(
    store: Any,
    cloud_router: Any,
    row: Mapping[str, Any],
    *,
    reason: str,
    revoke_remote: bool = True,
) -> bool | None:
    """Clear the row's credential and record why. With ``revoke_remote``
    the cloud is asked to revoke it first (best effort): True when it no
    longer accepts it, False when it could not be asked, None when there
    was no credential."""
    full = await store.get_datasource(str(row["id"])) or row
    credentials = full.get("credentials") or {}
    revoked: bool | None = None
    if credentials.get("password") and revoke_remote:
        backend = _row_provider(cloud_router, full)
        revoked = (
            await backend.revoke_user_root(
                str(credentials.get("username") or ""), str(credentials["password"])
            )
            if backend is not None
            else False
        )
    config = _config(full)
    grant = dict(config.get("grant") or {})
    if grant or credentials.get("password"):
        grant.update({"state": "revoked", "revoked_at": _iso(_now()), "reason": reason})
        config["grant"] = grant
    await store.update_datasource(str(row["id"]), credentials={}, config=config)
    return revoked


async def disconnect(
    store: Any,
    cloud_router: Any,
    flows: "CloudRootConnectFlows",
    user: Mapping[str, Any],
) -> dict[str, Any]:
    """The user's own disconnect: an in-flight flow cancelled, the
    credential revoked (``reason: user``) and cleared."""
    await flows.cancel(str(user["id"]))
    row, reason = await own_root(store, cloud_router, user)
    if row is None:
        return {**public_status(None, reason=reason), "provider_revoked": None}
    revoked = await revoke_root_credential(store, cloud_router, row, reason="user")
    await _security_event(
        store,
        "cloud_root_revoked",
        user_id=str(user["id"]),
        connector_id=str(row["id"]),
        detail=f"reason=user; provider revoked: {revoked}",
    )
    fresh = await store.get_datasource_by_managed_key(user_root_key(user["id"]))
    return {**public_status(fresh), "provider_revoked": revoked}


async def retirement_revoke(
    store: Any, cloud_router: Any, user_id: Any
) -> Callable[[], Awaitable[None]]:
    """For a user's account deletion: their root's credential, read before
    the delete (the row goes with the account), and the revoke to run once
    the delete succeeded, so a refused delete leaves the root working.

    The revoke is best effort: if the cloud cannot be asked, the credential
    stays in the account's device list and SRW no longer holds it. Never
    raises.
    """

    async def nothing() -> None:
        return None

    try:
        row = await store.get_datasource_by_managed_key(user_root_key(user_id))
        full = await store.get_datasource(str(row["id"])) if row else None
        credentials = (full or {}).get("credentials") or {}
        backend = _row_provider(cloud_router, full) if full else None
    except Exception:
        logger.warning(
            "User %s: their cloud root could not be read before the account's deletion",
            user_id,
            exc_info=True,
        )
        return nothing
    if not credentials.get("password"):
        return nothing
    login_name = str(credentials.get("username") or "")
    credential = str(credentials["password"])

    async def revoke() -> None:
        try:
            revoked = backend is not None and await backend.revoke_user_root(
                login_name, credential
            )
        except Exception:
            revoked = False
        if not revoked:
            logger.warning(
                "User %s: their cloud root's credential could not be revoked "
                "with the account's deletion; it stays in the cloud's device "
                "list",
                user_id,
            )

    return revoke


#: The keys of a flow in flight in the row's encrypted credentials, beside a
#: connected root's ``username`` and ``password``: its poll token, the page
#: its user opens and when it ends. Any replica, and the next start, picks
#: the flow up from them.
_PENDING_KEYS = ("pending_poll_token", "pending_login_url", "pending_expires_at")


@dataclass(frozen=True)
class PendingConnect:
    """A connect flow in flight, as the row records it; never printed."""

    poll_token: str
    login_url: str
    expires_at: datetime

    def __repr__(self) -> str:
        return f"PendingConnect(expires_at={self.expires_at.isoformat()})"


def _pending_of(
    credentials: Mapping[str, Any] | None, *, expired: bool = False
) -> PendingConnect | None:
    """The flow in flight the credentials record; one past its end only
    with ``expired`` (its own poller's last look)."""
    credentials = credentials or {}
    token = credentials.get("pending_poll_token")
    url = credentials.get("pending_login_url")
    try:
        expires = datetime.fromisoformat(str(credentials.get("pending_expires_at")))
    except (TypeError, ValueError):
        return None
    if not token or not url or (expires <= _now() and not expired):
        return None
    return PendingConnect(str(token), str(url), expires)


async def _write_pending(
    store: Any, row_id: str, pending: PendingConnect | None
) -> None:
    """Record (or clear) the row's flow in flight; its credential stays."""
    full = await store.get_datasource(row_id)
    if full is None:
        return
    credentials = {
        key: value
        for key, value in (full.get("credentials") or {}).items()
        if key not in _PENDING_KEYS
    }
    if pending is None and len(credentials) == len(full.get("credentials") or {}):
        return
    if pending is not None:
        credentials.update(
            {
                "pending_poll_token": pending.poll_token,
                "pending_login_url": pending.login_url,
                "pending_expires_at": pending.expires_at.isoformat(),
            }
        )
    await store.update_datasource(row_id, credentials=credentials)


async def _pending_for(
    store: Any, row_id: str, *, expired: bool = False
) -> PendingConnect | None:
    full = await store.get_datasource(row_id)
    return _pending_of((full or {}).get("credentials"), expired=expired)


@dataclass
class ConnectFlow:
    """One user's consent flow, polled by this replica."""

    user_id: str
    connector_id: str
    login_url: str = field(repr=False)
    started_at: datetime
    expires_at: datetime
    task: "asyncio.Task[None] | None" = None


class CloudRootConnectFlows:
    """The connect flows this application polls, at most one per user.
    Owned by the application (``ApplicationResources``) and drained at
    shutdown.

    A flow in flight is recorded in the root's encrypted credentials
    (:data:`_PENDING_KEYS`), so it outlives the replica that started it: a
    status read on any replica, and :func:`resume_pending_connects` at
    startup, resume polling it. Its outcome is written to the row; a poller
    whose flow another one finished (the record cleared or replaced) stops
    without a word. A flow nobody polls to its end would leave its user a
    granted app password SRW never holds.
    """

    def __init__(
        self,
        *,
        poll_seconds: float = POLL_SECONDS,
        flow_seconds: float = FLOW_SECONDS,
    ) -> None:
        self._flows: dict[str, ConnectFlow] = {}
        self._poll_seconds = poll_seconds
        self._flow_seconds = flow_seconds

    def active(self, user_id: Any) -> ConnectFlow | None:
        flow = self._flows.get(str(user_id))
        if flow is None or flow.task is None or flow.task.done():
            return None
        return flow

    async def cancel(self, user_id: Any) -> bool:
        """Stop polling ``user_id``'s flow here; True when one was running.
        Its record stays (:func:`cancel_connect` clears it)."""
        flow = self._flows.pop(str(user_id), None)
        if flow is None or flow.task is None or flow.task.done():
            return False
        flow.task.cancel()
        try:
            await flow.task
        except (asyncio.CancelledError, Exception):
            pass
        return True

    def _spawn(
        self,
        *,
        user: Mapping[str, Any],
        row_id: str,
        pending: PendingConnect,
        store: Any,
        backend: SupportsUserRootGrant,
    ) -> ConnectFlow:
        user_id = str(user["id"])
        flow = ConnectFlow(
            user_id=user_id,
            connector_id=row_id,
            login_url=pending.login_url,
            started_at=_now(),
            expires_at=pending.expires_at,
        )
        self._flows[user_id] = flow
        flow.task = asyncio.create_task(
            self._poll(flow, pending.poll_token, store, backend, dict(user)),
            name=f"cloud-root-connect-{user_id}",
        )
        return flow

    async def start(
        self, store: Any, cloud_router: Any, user: Mapping[str, Any]
    ) -> ConnectFlow:
        """A new flow for ``user`` (an earlier one is replaced).
        ``LookupError`` with the reason when there is no root to connect;
        ``CloudBackendError`` when the cloud cannot start one."""
        user_id = str(user["id"])
        row, reason = await own_root(store, cloud_router, user)
        if row is None or reason is not None:
            raise LookupError(reason or "provider_unsupported")
        backend = _row_provider(cloud_router, row)
        if backend is None:
            raise LookupError("no_main_cloud")
        await self.cancel(user_id)
        started = await backend.start_login_flow()
        pending = PendingConnect(
            started.poll_token,
            started.login_url,
            _now() + timedelta(seconds=self._flow_seconds),
        )
        await _write_pending(store, str(row["id"]), pending)
        return self._spawn(
            user=user,
            row_id=str(row["id"]),
            pending=pending,
            store=store,
            backend=backend,
        )

    async def resume(
        self, store: Any, cloud_router: Any, user: Mapping[str, Any]
    ) -> ConnectFlow | None:
        """``user``'s flow: the one polled here, else the one the row
        records (started elsewhere, or before a restart), polled from now
        on; ``None`` without one. Never raises."""
        active = self.active(user["id"])
        if active is not None:
            return active
        try:
            row = await store.get_datasource_by_managed_key(user_root_key(user["id"]))
            if row is None:
                return None
            pending = await _pending_for(store, str(row["id"]))
            backend = _row_provider(cloud_router, row) if pending else None
            if pending is None or backend is None:
                return None
            return self._spawn(
                user=user,
                row_id=str(row["id"]),
                pending=pending,
                store=store,
                backend=backend,
            )
        except Exception:
            logger.warning(
                "User %s: a connect flow in flight was not resumed",
                user.get("id"),
                exc_info=True,
            )
            return None

    async def _poll(
        self,
        flow: ConnectFlow,
        poll_token: str,
        store: Any,
        backend: SupportsUserRootGrant,
        user: dict[str, Any],
    ) -> None:
        async def still_ours() -> bool:
            pending = await _pending_for(store, flow.connector_id, expired=True)
            return pending is not None and pending.poll_token == poll_token

        try:
            while _now() < flow.expires_at:
                await asyncio.sleep(self._poll_seconds)
                if not await still_ours():
                    return  # finished, cancelled or replaced elsewhere
                try:
                    grant = await backend.poll_login_flow(poll_token)
                except CloudBackendError as exc:
                    logger.debug(
                        "Cloud root %s: poll failed (%s); retrying",
                        flow.connector_id,
                        exc.kind.value,
                    )
                    continue
                if grant is not None:
                    await complete_connect(store, backend, user, grant)
                    if await still_ours():
                        await _write_pending(store, flow.connector_id, None)
                    return
            if await still_ours():
                await _write_pending(store, flow.connector_id, None)
                await _record_failure(store, flow.connector_id, "expired")
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.warning(
                "Cloud root %s: connect flow failed", flow.connector_id, exc_info=True
            )
            try:
                await _write_pending(store, flow.connector_id, None)
                await _record_failure(store, flow.connector_id, "error")
            except Exception:
                pass
        finally:
            if self._flows.get(flow.user_id) is flow:
                self._flows.pop(flow.user_id, None)

    async def drain(self) -> None:
        """Cancel and await every poller (shutdown). The flows' records
        stay: the next start resumes them."""
        flows = list(self._flows.values())
        self._flows.clear()
        tasks = [flow.task for flow in flows if flow.task is not None]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


async def cancel_connect(
    store: Any,
    cloud_router: Any,
    flows: CloudRootConnectFlows,
    user: Mapping[str, Any],
) -> bool:
    """The user's own cancel: the poller stopped and the flow's record
    cleared, so no replica resumes it. True when a flow was in flight."""
    cancelled = await flows.cancel(str(user["id"]))
    row = await store.get_datasource_by_managed_key(user_root_key(user["id"]))
    if row is not None:
        cancelled = (await _pending_for(store, str(row["id"]))) is not None or cancelled
        await _write_pending(store, str(row["id"]), None)
    return cancelled


async def resume_pending_connects(
    store: Any, cloud_router: Any, flows: CloudRootConnectFlows
) -> int:
    """At startup: every flow in flight a row records, polled again (its
    user granted it while no replica polled, or will). Returns how many.
    Never raises."""
    resumed = 0
    try:
        rows = await store.fetch(
            "SELECT created_by FROM datasources WHERE managed_key LIKE $1 "
            "AND created_by IS NOT NULL",
            f"{USER_CLOUD_ROOT}:%",
        )
        for row in rows:
            user = await store.get_user(str(row["created_by"]))
            if user and await flows.resume(store, cloud_router, user):
                resumed += 1
    except Exception:
        logger.warning("Connect flows in flight were not resumed", exc_info=True)
    if resumed:
        logger.info("User cloud roots: %d connect flow(s) in flight resumed", resumed)
    return resumed


async def status(
    store: Any,
    cloud_router: Any,
    flows: CloudRootConnectFlows,
    user: Mapping[str, Any],
) -> dict[str, Any]:
    """``GET /api/me/cloud-root``: the caller's root, ensured lazily."""
    row, reason = await own_root(store, cloud_router, user)
    flow = await flows.resume(store, cloud_router, user) if reason is None else None
    return public_status(row, reason=reason, flow=flow)


# --------------------------------------------------------------------------- #
# Mounting
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class UserRootCredential:
    """The login name and credential a root mounts with; never printed."""

    login_name: str
    credential: str

    def __repr__(self) -> str:
        return "UserRootCredential(<redacted>)"


def _unavailable(connector_id: str, reason: str, detail: str) -> CloudFolderUnavailable:
    return CloudFolderUnavailable(connector_id, reason, detail)


async def user_root_mount(
    row: Mapping[str, Any],
    *,
    owner_id: str | None,
    store: Any,
    cloud_router: Any = None,
) -> tuple[dict[str, Any], UserRootCredential] | CloudFolderUnavailable:
    """What a session of ``owner_id`` mounts for its root connector ``row``:
    the folder's mount row and its credential, or why nothing.

    Reads the database only (the thread's lock may be held). The driver has
    already resolved the level for the session's tier.
    """
    connector_id = str(row.get("id") or "")
    if not is_user_root(row) or not owner_id or native_user(row) != str(owner_id):
        return _unavailable(
            connector_id,
            "not_eligible",
            "Only its own user's sessions mount a cloud root",
        )
    if cloud_router is not None:
        backend = _row_provider(cloud_router, row)
        if backend is not None and backend.user_root_grant_mode == "off":
            return _unavailable(
                connector_id,
                "access_unavailable",
                "Personal cloud roots are turned off on this deployment",
            )
    full = await store.get_datasource(connector_id)
    if full is None:
        return _unavailable(connector_id, "not_eligible", "This cloud root is gone")
    config = _config(full)
    grant = config.get("grant") if isinstance(config.get("grant"), Mapping) else {}
    credentials = full.get("credentials") or {}
    if grant.get("state") == "revoked":
        return _unavailable(
            connector_id,
            "cloud_grant_revoked",
            "The cloud no longer accepts this connection: reconnect your cloud",
        )
    if (
        grant.get("state") != "connected"
        or not config.get("account_id")
        or not config.get("folder_handle")
        or not credentials.get("password")
        or not credentials.get("username")
    ):
        return _unavailable(
            connector_id,
            "cloud_not_connected",
            "Your cloud is not connected: connect it in Settings, Connectors",
        )
    mount_row = {
        "id": connector_id,
        "source_kind": "cloud_folder",
        "source_ref": connector_id,
        "mount_kind": "user_root",
        "target_path": f"cloud/{MOUNT_NAME}",
        "backend_id": config.get("backend_id"),
        "backend_instance_id": config.get("backend_instance_id"),
        "cloud_handle": config["folder_handle"],
    }
    return mount_row, UserRootCredential(
        str(credentials["username"]), str(credentials["password"])
    )


async def verify_selected_user_root(
    thread: Mapping[str, Any],
    *,
    selected_ids: list[str],
    store: Any,
    cloud_router: Any,
) -> str | None:
    """Before a session's new Pod, outside its lock: probe its owner's root
    once when the session selected it, and record what the cloud refused.
    Returns the probe's outcome (``None`` when nothing was probed). Never
    raises.

    ``rejected`` revokes the row (``rejected_by_cloud``) at once, which also
    keeps the cloud's brute-force counter from charging SRW for every Pod
    (the cloud already refuses the credential: nothing to revoke there);
    ``missing`` has the cloud revoke the credential, best effort, then
    clears it (``account_missing``); ``unreachable`` mounts anyway, and the
    mount reports the live state.
    """
    owner_id = thread.get("user_id")
    if not owner_id or not selected_ids:
        return None
    try:
        row = await store.get_datasource_by_managed_key(user_root_key(owner_id))
        if row is None or str(row["id"]) not in {str(i) for i in selected_ids}:
            return None
        full = await store.get_datasource(str(row["id"]))
        if full is None:
            return None
        config = _config(full)
        credentials = full.get("credentials") or {}
        grant = config.get("grant") if isinstance(config.get("grant"), Mapping) else {}
        if (
            grant.get("state") != "connected"
            or not credentials.get("password")
            or not config.get("account_id")
        ):
            return None
        backend = _row_provider(cloud_router, full)
        if backend is None:
            return None
        outcome = await backend.verify_user_root(
            str(config["account_id"]),
            str(credentials.get("username") or ""),
            str(credentials["password"]),
        )
        if outcome == "rejected":
            await revoke_root_credential(
                store,
                cloud_router,
                full,
                reason="rejected_by_cloud",
                revoke_remote=False,
            )
            await _security_event(
                store,
                "cloud_root_revoked",
                user_id=str(owner_id),
                connector_id=str(row["id"]),
                detail="reason=rejected_by_cloud",
            )
            logger.info(
                "Cloud root %s: the cloud refused its credential; revoked",
                row["id"],
            )
        elif outcome == "missing":
            # The cloud still took the credential (it only found no files):
            # have it revoked before SRW forgets it, best effort, so no app
            # password is left live that SRW no longer holds.
            try:
                revoked = await backend.revoke_user_root(
                    str(credentials.get("username") or ""),
                    str(credentials["password"]),
                )
            except Exception:
                revoked = False
            config.pop("grant", None)
            for stale in ("account_id", "folder_handle"):
                config.pop(stale, None)
            config["connect_failure"] = {
                "reason": "account_missing",
                "at": _iso(_now()),
            }
            await store.update_datasource(str(row["id"]), credentials={}, config=config)
            await _security_event(
                store,
                "cloud_root_revoked",
                user_id=str(owner_id),
                connector_id=str(row["id"]),
                detail=f"reason=account_missing; provider revoked: {revoked}",
            )
            if not revoked:
                logger.warning(
                    "Cloud root %s: its account has no files and its credential "
                    "could not be revoked; it stays in the account's device "
                    "list until its user removes it",
                    row["id"],
                )
        return outcome
    except Exception:
        logger.warning(
            "Session %s: its cloud root could not be probed",
            thread.get("id"),
            exc_info=True,
        )
        return None


async def prepare_session_root(
    thread: Mapping[str, Any], *, store: Any, cloud_router: Any
) -> str | None:
    """:func:`verify_selected_user_root` for a session's own selection: the
    provisioner's pre-plan step, outside the thread's lock. Never raises."""
    from orchestrator.services.cloud_connector_mounts import selected_ids
    from orchestrator.services.stateless_workspace_gate import (
        thread_metadata_object,
    )

    try:
        ids = selected_ids(thread_metadata_object(dict(thread)))
    except Exception:
        return None
    return await verify_selected_user_root(
        thread, selected_ids=ids, store=store, cloud_router=cloud_router
    )


__all__ = [
    "CLOUD_FOLDER_TYPE",
    "CONNECT_FAILURES",
    "EXCLUSION_REASONS",
    "FLOW_SECONDS",
    "MOUNT_NAME",
    "OUTCOMES",
    "POLL_SECONDS",
    "REVOKE_REASONS",
    "ROOT_NAME",
    "STATES",
    "UNSUPPORTED_REASONS",
    "USER_ROOT_FOLDER",
    "CloudRootConnectFlows",
    "ConnectFlow",
    "PendingConnect",
    "UserRootCredential",
    "active_root_provider",
    "backfill_user_root_connectors",
    "base_config",
    "cancel_connect",
    "complete_connect",
    "disconnect",
    "ensure_user_root_connector",
    "ensure_user_root_if_cloud",
    "is_user_root",
    "own_root",
    "prepare_session_root",
    "public_status",
    "resume_pending_connects",
    "retirement_revoke",
    "revoke_root_credential",
    "root_state",
    "status",
    "user_root_key",
    "user_root_mount",
    "verify_selected_user_root",
]
