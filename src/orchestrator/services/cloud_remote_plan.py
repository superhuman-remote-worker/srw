"""A Virtual Session's cloud folders, planned per claim (``cloud_remote``).

A Virtual Session has no workspace Pod and nothing mounts for it. Its
attached cloud connectors reach the agent as further entries of
``workspace.mounts`` instead (``features/cloud_folders_on_virtual_sessions.md``
§ The remote descriptor): per bound folder one remote descriptor the Virtual
backend serves under ``cloud/<name>`` through explicit rclone operations.

The planner reuses the Container's binding chain
(:func:`agent_cloud_mounts._resolve_live_mount_set` with the ``virtual``
tier), so eligibility, viewer ceilings, per-folder grants, the outbox-first
order and the closed exclusion reasons are the same; it only changes what a
bound folder becomes (a descriptor instead of a sidecar mount) and where the
credential goes (the executor process, in-flight, never a row). It runs on
every delivery of the thread's workspace to an executor (the stateless lane
re-attaches on any executor), and records what it planned in
``threads.metadata.cloud_mount_status`` (``delivery: remote``), which the
agent's probe then completes through the report route.

A remote whose provider signs in with a bearer (OpenCloud) travels without any
credential, as the ``oauth_token`` auth kind: the executor fetches the folder's
short-lived token from the internal token route under its claim lease
(``services/cloud_remote_token.py``), so the provider's client secret never
reaches the pooled executor (ruling 9). A bearer the route does not mint (an
impersonated user) is still left out with ``delivery_unsupported``.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from orchestrator.services import agent_cloud_mounts, cloud_mount_plan
from orchestrator.services.cloud_mount_plan import rclone_obscure
from orchestrator.services.cloud_mount_sidecar import plan_fingerprint
from orchestrator.services.stateless_workspace_gate import thread_metadata_object

logger = logging.getLogger(__name__)

#: The file-tools root folder every cloud folder lives under, on every tier.
CLOUD_ROOT = "cloud"
#: The auth kinds of a bound mount whose bearer the orchestrator mints per
#: call for a Virtual Session's executor (the token route): the provider's
#: service-account kind. Impersonation stays with the sidecar.
TOKEN_ROUTE_AUTH_KINDS = frozenset({"keycloak_client_credentials"})
#: The descriptor auth kind the executor answers through the token route.
OAUTH_TOKEN = "oauth_token"
#: rclone options that would carry a credential; never part of a token
#: entry's config, whatever a provider's source config says.
_CREDENTIAL_OPTIONS = (
    "pass",
    "password",
    "bearer_token",
    "bearer_token_command",
    "token",
)


@dataclass(frozen=True)
class CloudRemotePlan:
    """What one delivery hands a Virtual Session's executor.

    ``entries`` are the remote descriptors (credential included: in-flight
    only), ``excluded`` the rows that were left out with their closed reason,
    ``fingerprint`` the sha256 of the non-secret body and ``incarnation`` the
    name of this claim's plan (fresh per delivery), which the agent's report
    repeats so the orchestrator keeps the state for the right claim.
    """

    entries: tuple[dict[str, Any], ...]
    excluded: tuple[dict[str, Any], ...]
    fingerprint: str
    incarnation: str

    @property
    def record(self) -> dict[str, Any]:
        """The non-secret body the status record is built from."""
        return {
            "fingerprint": self.fingerprint,
            "mounts": [
                {
                    "name": entry["name"],
                    "mount_kind": entry.get("mount_kind"),
                    "target_path": entry["path"],
                    "access": entry.get("access"),
                    "mount_id": entry.get("mount_id"),
                    "source_ref": entry.get("source_ref"),
                }
                for entry in self.entries
            ],
            "excluded": [dict(entry) for entry in self.excluded],
        }

    def agent_payload(self) -> dict[str, Any]:
        """What the workspace delivery carries to the executor."""
        return {
            "mounts": [dict(entry) for entry in self.entries],
            "plan": {"fingerprint": self.fingerprint, "incarnation": self.incarnation},
        }


def remote_entry(built: Mapping[str, Any]) -> dict[str, Any] | None:
    """A bound mount (``_build_rclone_mount_from_row``) as the descriptor
    the Virtual backend's factory reads, or ``None`` when the executor
    cannot serve it: anything but a static WebDAV password (``basic``) or a
    bearer the orchestrator mints through the token route (``oauth_token``,
    :data:`TOKEN_ROUTE_AUTH_KINDS`)."""
    source = built.get("source") or {}
    auth = built.get("auth") or {}
    config = source.get("config") or {}
    name = str(built.get("workspace_name") or "")
    kind = auth.get("type")
    if (
        source.get("type") != "webdav"
        or not isinstance(config, Mapping)
        or not config.get("url")
        or not name
        or "/" in name
    ):
        return None
    rclone_config = {str(key): str(value) for key, value in config.items()}
    if kind == "basic":
        password = auth.get("password")
        if not isinstance(password, str) or not password:
            return None
        # Obscuring is encoding, not encryption (rclone's own format): the
        # entry travels in-flight only and is never persisted.
        rclone_config["pass"] = rclone_obscure(password)
        entry_auth: dict[str, Any] = {"type": "basic"}
    elif kind in TOKEN_ROUTE_AUTH_KINDS:
        # The bearer is minted per call by the orchestrator under the
        # executor's claim lease; nothing of the provider's own credential
        # travels in the entry.
        for option in _CREDENTIAL_OPTIONS:
            rclone_config.pop(option, None)
        entry_auth = {"type": OAUTH_TOKEN}
    else:
        return None
    access = "read_only" if built.get("access") == "read_only" else "read_write"
    return {
        "name": name,
        "path": f"{CLOUD_ROOT}/{name}",
        "mount_id": str(built.get("mount_id") or name),
        "mount_kind": str(built.get("mount_kind") or "project"),
        "source_ref": str(built["source_ref"]) if built.get("source_ref") else None,
        "backend": str(built.get("backend") or ""),
        "access": access,
        "rclone_spec": {
            "type": "webdav",
            "config": rclone_config,
            "root": str(source.get("root") or ""),
        },
        "auth": entry_auth,
    }


def remote_refusal(built: Mapping[str, Any]) -> dict[str, str]:
    """A bound mount the executor cannot serve (a bearer the token route does
    not mint, such as an impersonated user's), named instead of delivered."""
    refusal = {
        "source_ref": str(built.get("source_ref") or built.get("mount_id") or ""),
        "mount_kind": str(built.get("mount_kind") or "project"),
        "reason": "delivery_unsupported",
        "detail": "bearer_token",
        "name": str(built.get("workspace_name") or ""),
    }
    if built.get("access"):
        refusal["level"] = str(built["access"])
    return refusal


async def resolve_cloud_remote_plan(
    thread: dict[str, Any],
    *,
    mount_rows: list[dict[str, Any]] | None,
    dependencies: agent_cloud_mounts.AgentCloudMountDependencies,
) -> CloudRemotePlan | None:
    """The plan for one delivery of a Virtual Session's workspace, or
    ``None`` when nothing is bound and nothing was left out (a session
    without cloud connectors starts exactly as before).

    Not for a background officer (its cloud is object plane) nor a protected
    session (never Virtual). Under a cloud workspace driver other than
    ``rclone_mount`` the legacy mirror still serves the session folder, so
    the planner stays out of its way (the mirror's retirement is the slice
    after this one).
    """
    if dependencies.cloud_workspace_driver() != "rclone_mount":
        return None
    metadata = thread_metadata_object(thread)
    if cloud_mount_plan._officer(metadata):
        return None
    if cloud_mount_plan.protected_cloud_marker_state(metadata) != "off":
        return None
    live = await agent_cloud_mounts._resolve_live_mount_set(
        thread,
        mount_rows=mount_rows,
        runtime_is_vm=False,
        dependencies=dependencies,
        workspace_backend="virtual",
    )
    excluded: list[dict[str, Any]] = [
        {key: str(value) for key, value in entry.items()} for entry in live.excluded
    ]
    entries: list[dict[str, Any]] = []
    shared = bool(thread.get("main_cloud_share_handle"))
    for built in live.mounts:
        entry = remote_entry(built)
        if entry is None:
            excluded.append(remote_refusal(built))
            continue
        if entry["mount_kind"] == "cloud_outbox":
            entry["shared"] = shared
        entries.append(entry)
    if not entries and not excluded:
        return None
    body = {
        "mounts": [
            {
                "name": entry["name"],
                "mount_kind": entry["mount_kind"],
                "target_path": entry["path"],
                "access": entry["access"],
                "mount_id": entry["mount_id"],
                "source_ref": entry["source_ref"],
            }
            for entry in entries
        ],
        "excluded": excluded,
    }
    return CloudRemotePlan(
        entries=tuple(entries),
        excluded=tuple(excluded),
        fingerprint=plan_fingerprint(body),
        incarnation=uuid4().hex,
    )


__all__ = [
    "CLOUD_ROOT",
    "OAUTH_TOKEN",
    "TOKEN_ROUTE_AUTH_KINDS",
    "CloudRemotePlan",
    "remote_entry",
    "remote_refusal",
    "resolve_cloud_remote_plan",
]
