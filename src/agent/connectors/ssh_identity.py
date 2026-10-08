"""``ssh_identity``: connector SSH keys, held by ``ssh-agent`` in the workspace.

An SSH repository's or ``ssh_key`` connector's key travels apart from its
payload entry, in the hidden ``workspace_ssh_identities`` list (slice C1).
This materializer loads that list through C1's
``shared.runtime.core.workspace_ssh_identity`` unchanged: each identity into
its own ``ssh-agent``, per identity and never fatal, every private key popped
from the payload. A session owns its workspace, so its attach also retires
identities no longer delivered; a live detach retires exactly the detached
ones. The payload entry keeps only a non-secret ``ssh_identity`` reference,
which the clone (``agent.connectors.checkout``) and the README read.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Sequence
from typing import Any, Dict, Optional

from agent.connectors.base import Delivery, FactsLines, RuntimeContext

logger = logging.getLogger(__name__)

#: What an ``unavailable`` SSH identity's fixed reason code means. Anything
#: else is shown as the generic reason: the field is never echoed verbatim
#: into a README (or a log), which another user's connector would control.
_SSH_UNAVAILABLE_REASONS = {
    "ssh_endpoint_invalid": "its SSH host, user, port or repository URL is not allowed",
    "ssh_key_invalid": "its SSH key could not be parsed",
    "ssh_key_passphrase": "its SSH key is passphrase-protected",
    "known_hosts_invalid": "its pinned host key is not valid for its host",
    "default_known_hosts_invalid": "the default host keys of this deployment are invalid",
    "ssh_identity_unresolvable": "it has no stable identity",
}

_SSH_KEY_NOT_LOADED = "its key could not be loaded into this workspace's ssh-agent"


def ssh_unavailable_reason(identity: Dict[str, Any]) -> str:
    return _SSH_UNAVAILABLE_REASONS.get(
        str(identity.get("unavailable") or ""),
        "it cannot be delivered to this workspace",
    )


def ssh_clone_target(
    ds: Dict[str, Any], ssh_identity_status: Optional[Dict[str, str]]
) -> tuple[Optional[str], str]:
    """``(clone_url, "")`` for a loaded SSH identity, else ``(None, reason)``."""

    identity = ds.get("ssh_identity")
    if not isinstance(identity, dict):
        return None, "no workspace SSH identity was delivered for it"
    if identity.get("unavailable"):
        return None, ssh_unavailable_reason(identity)
    clone_url = str(identity.get("clone_url") or "")
    alias = str(identity.get("alias") or "")
    path = r"[A-Za-z0-9._~-]+(?:/[A-Za-z0-9._~-]+)*"
    # ``ssh://alias/path`` (absolute) or the scp form ``alias:path``, which
    # keeps a relative path relative exactly as the connector URL had it.
    if not re.fullmatch(r"srw-repo-[a-f0-9]{32}", alias) or not re.fullmatch(
        rf"(?:ssh://{alias}/|{alias}:){path}", clone_url
    ):
        return None, "its SSH identity is malformed"
    if ssh_identity_status is not None:
        status = ssh_identity_status.get(str(identity.get("authority_id") or ""))
        if status != "ready":
            return None, f"its key is not loaded ({status or 'not delivered'})"
    return clone_url, ""


def _ssh_identity_unloaded(
    identity: Dict[str, Any], ssh_identity_status: Optional[Dict[str, str]]
) -> bool:
    """True when the materializer ran and this identity's key is not held."""

    if not isinstance(ssh_identity_status, dict):
        return False
    authority = str(identity.get("authority_id") or "")
    return ssh_identity_status.get(authority) != "ready"


def ssh_identity_note(
    ds: Dict[str, Any], ssh_identity_status: Optional[Dict[str, str]] = None
) -> str:
    """Suffix for an SSH-key repository line: how git reaches it."""

    identity = ds.get("ssh_identity")
    if not isinstance(identity, dict):
        return ""
    if identity.get("unavailable"):
        return f"; not available: {ssh_unavailable_reason(identity)}"
    if _ssh_identity_unloaded(identity, ssh_identity_status):
        return f"; not available: {_SSH_KEY_NOT_LOADED}"
    return (
        f"; git uses SSH alias `{identity.get('alias')}`, whose key an "
        "ssh-agent holds (never on disk)"
    )


def _ssh_key_host(ds: Dict[str, Any]) -> Optional[str]:
    """The host a delivered ``ssh_key`` connector declares, if any."""

    identity = ds.get("ssh_identity")
    if (
        not isinstance(identity, dict)
        or not identity.get("alias")
        or identity.get("unavailable")
        or not identity.get("host")
    ):
        return None
    return str(identity["host"]).lower()


def _ssh_key_usage(
    ds: Dict[str, Any],
    ssh_identity_status: Optional[Dict[str, str]] = None,
    shared_hosts: frozenset[str] = frozenset(),
) -> str:
    """How the agent uses an ``ssh_key`` connector held by an ssh-agent.

    ``shared_hosts`` are hosts more than one delivered ``ssh_key`` connector
    declares. OpenSSH takes the first matching ``Host`` block, so a plain
    ``ssh <host>`` offers only one of their keys; each is named by its alias.
    """

    identity = ds.get("ssh_identity")
    if not isinstance(identity, dict) or not identity.get("alias"):
        return "not available: its key was not delivered to this workspace."
    if identity.get("unavailable"):
        return f"not available: {ssh_unavailable_reason(identity)}."
    if _ssh_identity_unloaded(identity, ssh_identity_status):
        return f"not available: {_SSH_KEY_NOT_LOADED}."
    fingerprint = (
        f" Key `{identity.get('fingerprint')}`." if identity.get("fingerprint") else ""
    )
    held = "The key is held by an ssh-agent and never written to disk."
    host = identity.get("host")
    if host and str(host).lower() in shared_hosts:
        return (
            f"use the alias: `ssh {identity['alias']}` reaches `{host}` with this "
            f"key. Another ssh_key connector here also declares `{host}`, so a "
            f"plain `ssh {host}` may offer the other key. {held}{fingerprint}"
        )
    if host:
        user = identity.get("user")
        port = identity.get("port")
        target = f"{user}@{host}" if user else str(host)
        port_clause = f" (port {port})" if port and port != 22 else ""
        return (
            f"`ssh {target}`{port_clause} uses it, as does the alias "
            f"`{identity['alias']}`. {held}{fingerprint}"
        )
    slug = str(identity["alias"]).removeprefix("srw-repo-")
    return (
        "use it with `ssh -o IdentityAgent=~/.ssh/srw-managed/sockets/"
        f"{slug}.sock <user>@<host>`. {held}{fingerprint}"
    )


def _authorities(deliveries: Sequence[Delivery]) -> set[str]:
    return {
        str(delivery.entry["ssh_identity"]["authority_id"])
        for delivery in deliveries
        if isinstance(delivery.entry.get("ssh_identity"), dict)
        and delivery.entry["ssh_identity"].get("authority_id")
    }


class SshIdentityMaterializer:
    form = "ssh_identity"

    def materialize(self, deliveries: Sequence[Delivery], rt: RuntimeContext) -> None:
        """Load ``rt.ssh_identities`` while the workspace initializes.

        Sets ``rt.ssh_identity_status`` to ``{authority_id: status}``. A
        session also retires every connector identity it no longer delivers
        (a stateless session applies connector edits at its next attach,
        never through the live detach). Every DELIVERED identity is kept,
        loaded or not: one whose load failed this time must not cost a
        healthy resident agent, or its learned host key, that an earlier
        attach left. Jobs never prune: a root and its children share one
        workspace.
        """
        from shared.runtime.core.workspace_ssh_identity import (
            materialize_workspace_ssh_identities,
            prune_workspace_ssh_identities,
        )

        backend = rt.workspace_backend
        payloads, rt.ssh_identities = rt.ssh_identities, None
        try:
            rt.ssh_identity_status = materialize_workspace_ssh_identities(
                payloads, backend
            )
        finally:
            del payloads
        if (
            rt.execution == "session"
            and getattr(backend, "supports_shell", False)
            and not prune_workspace_ssh_identities(
                list(rt.ssh_identity_status), backend
            )
        ):
            logger.warning(
                "Could not retire detached connector SSH identities; "
                "terminal teardown retires them"
            )

    def replace(
        self,
        old: Sequence[Delivery],
        new: Sequence[Delivery],
        rt: RuntimeContext,
    ) -> None:
        """Retire detached connector identities, then (re)load the rest."""
        from shared.runtime.core.workspace_ssh_identity import (
            materialize_workspace_ssh_identities,
            retire_workspace_ssh_identities,
        )

        if rt.ssh_identity_status is None:
            rt.ssh_identity_status = {}
        payloads, rt.ssh_identities = rt.ssh_identities, None
        backend = rt.workspace_backend
        detached = sorted(_authorities(old) - _authorities(new))
        if detached:
            if backend is not None and retire_workspace_ssh_identities(
                detached, backend
            ):
                for authority in detached:
                    rt.ssh_identity_status.pop(authority, None)
            else:
                logger.warning(
                    "Could not retire %d detached connector SSH identities; "
                    "terminal teardown retires them",
                    len(detached),
                )
        if payloads:
            rt.ssh_identity_status.update(
                materialize_workspace_ssh_identities(payloads, backend)
            )

    def facts(
        self, deliveries: Sequence[Delivery], rt: RuntimeContext
    ) -> list[FactsLines]:
        declared = [_ssh_key_host(delivery.entry) for delivery in deliveries]
        shared_hosts = frozenset(
            host for host in declared if host and declared.count(host) > 1
        )
        return [
            FactsLines(
                "Credential Files",
                delivery.index,
                [
                    f"- **{delivery.entry.get('name', 'Unnamed')}** "
                    f"({delivery.entry.get('type', 'unknown')}) — "
                    + _ssh_key_usage(
                        delivery.entry, rt.ssh_identity_status, shared_hosts
                    )
                ],
            )
            for delivery in deliveries
        ]
