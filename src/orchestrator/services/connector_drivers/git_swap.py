"""``srw.git-swap/v1``: token repositories through SRW's git swap driver (C3).

The control-plane half of the git swap driver: a variant of the repository
type, found by name, that serves the rows of token repositories on HTTPS.
The connector stays a ``repository`` connector (its form, its create and
update, its Test are ``srw.repository/v1``'s); what changes is delivery:

* :func:`route_token_repository` decides, at bind, what a token repository's
  payload entry becomes. Through the swap: the clean upstream URL as its
  remote, a ``git_swap`` block the lease step fills with the driver's
  endpoint and SRW's certificate authority, and only the forge token the
  agent process keeps for the pull-request tools (the lease step adds the
  lease). Where the swap cannot serve it (the driver is not installed, or
  the URL is not HTTPS on port 443) the installation's fallback applies,
  explicitly and logged: the token in the clone URL as before C3
  (``token-in-url``, the default), or no delivery with the reason
  (``refuse``);
* :meth:`GitSwapDriver.lease_upstream` answers the lease exchange: the forge
  token and the one upstream the driver may reach;
* :meth:`GitSwapDriver.service_connector` is the connector its pods are built
  from: the clean upstream URL and its host, which the pod's egress pins.

Installed only when the chart turns it on (``connectors.drivers.gitSwap``)
with service-pod hosting and SRW's driver certificate authority.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

from orchestrator.services.connector_drivers.base import DatasourceDriver
from orchestrator.services.datasource_config import stored_json_object
from shared.connectors.builtin import GIT_SWAP_SPEC
from shared.connectors.git_swap import (
    FALLBACK_REFUSE,
    UnservedUpstream,
    swap_upstream,
)

logger = logging.getLogger(__name__)

#: Fallbacks already logged, by (connector, reason): every bind of a long
#: session would repeat the line.
_NOTED: set[tuple[str, str]] = set()
_NOTED_MAX = 1024


def _token_auth(entry: Mapping[str, Any], credentials: Mapping[str, Any]) -> bool:
    """Whether a repository entry clones with a token (the agent's
    ``checkout_auth`` rule: an explicit method, else an SSH identity or key
    means SSH, a token means token)."""
    method = credentials.get("auth_method")
    if not method:
        if entry.get("ssh_identity") is not None or credentials.get("ssh_key"):
            method = "ssh"
        elif credentials.get("token"):
            method = "token"
    return method == "token" and bool(credentials.get("token"))


class GitSwapDriver(DatasourceDriver):
    """The git swap driver's control plane; its pods run ``image_reference``."""

    def __init__(self, image_reference: str) -> None:
        super().__init__(GIT_SWAP_SPEC, serves_stored_type=False)
        #: SRW's srw-driver-git-swap image, as the chart names it (a tag
        #: follows, a digest pins).
        self.image_reference = image_reference

    def lease_upstream(self, row: Mapping[str, Any]) -> dict[str, Any]:
        credentials = stored_json_object(row.get("credentials"))
        if not _token_auth(row, credentials):
            raise ValueError("The repository connector holds no token")
        upstream = swap_upstream(row.get("connection_url"))
        return {
            "credential": str(credentials["token"]),
            "allowed_upstream": [upstream.url],
        }

    def service_connector(self, row: Mapping[str, Any]) -> Mapping[str, Any]:
        try:
            upstream = swap_upstream(row.get("connection_url"))
        except UnservedUpstream:
            # No host to pin: the pod is refused at launch (and a running one
            # stops: its declared egress no longer holds).
            return {**row, "config": {}}
        return {**row, "config": {"upstream": upstream.url, "host": upstream.host}}


def route_token_repository(
    entry: dict[str, Any],
    *,
    git_swap: Any,
    fallback: str,
) -> None:
    """Route a repository payload entry, in place (see the module docstring).

    ``git_swap`` is the installed :class:`GitSwapDriver` or ``None``. An entry
    that does not clone with a token is left alone. A fallback is logged on
    this module's logger once per connector and reason (every claim of a
    session binds again); the ``token-in-url`` entry is the one SRW sent
    before C3, byte for byte.
    """
    credentials = entry.get("credentials")
    if not isinstance(credentials, Mapping) or not _token_auth(entry, credentials):
        return
    reason = None
    if git_swap is None:
        reason = "the git swap driver is not installed (connectors.drivers.gitSwap)"
    else:
        try:
            upstream = swap_upstream(entry.get("connection_url"))
        except UnservedUpstream as exc:
            reason = f"the git swap driver cannot serve it: {exc}"
    if reason is None:
        entry["connection_url"] = upstream.url
        entry["credentials"] = {"auth_method": "token", "token": credentials["token"]}
        entry["git_swap"] = {}
        return
    connector = str(entry.get("datasource_id") or entry.get("name") or "?")
    refused = fallback == FALLBACK_REFUSE
    note = (connector, reason)
    if note not in _NOTED:
        if len(_NOTED) >= _NOTED_MAX:
            _NOTED.clear()
        _NOTED.add(note)
        logger.warning(
            "Repository connector %s %s: %s (connectors.drivers.gitSwap.fallback=%s)",
            connector,
            "is not delivered" if refused else "delivers its token in the clone URL",
            reason,
            fallback,
        )
    if refused:
        entry["credentials"] = {}
        entry["git_swap"] = {
            "unavailable": (
                f"{reason}; this installation refuses token-in-URL delivery"
            )
        }


__all__ = ["GitSwapDriver", "route_token_repository"]
