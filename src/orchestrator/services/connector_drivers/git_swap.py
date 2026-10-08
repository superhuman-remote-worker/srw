"""``srw.git-swap/v1``: token repositories through SRW's git swap driver (C3).

The control-plane half of the git swap driver: a variant of the repository
type, found by name, that serves the rows of token repositories on HTTPS.
The connector stays a ``repository`` connector (its form, its create and
update, its Test are ``srw.repository/v1``'s); what changes is delivery:

* :func:`route_token_repository` decides, at bind, whether a token
  repository's payload entry is a swap candidate: the driver is installed
  and the URL is HTTPS on port 443. A candidate keeps its URL as stored and
  its forge token and gains an empty ``git_swap`` block; the lease step decides
  per entry whether the driver serves it now (the workspace, the
  connector's pods, the upstream, the image:
  :mod:`orchestrator.services.connector_git_swap_delivery`) and turns it
  into the driver's entry (the clean upstream URL, a lease, the driver's
  endpoint and SRW's certificate authority, and only the forge token the
  agent process keeps for the pull-request tools) or puts it on the
  installation's fallback. Where the driver is installed but cannot serve
  the URL (not HTTPS on port 443), the fallback applies at once, explicitly
  and logged: the token in the clone URL as before C3 (``token-in-url``,
  the default), or no delivery (``refuse``); either way the entry says
  why. An installation without the driver sends what it always sent (or
  refuses, where its fallback says so);
* :meth:`GitSwapDriver.lease_upstream` answers the lease exchange: the forge
  token and the one upstream the driver may reach;
* :meth:`GitSwapDriver.service_connector` is the connector its pods are built
  from: the clean upstream URL and its host, which the pod's egress pins,
  and the connector's ``upstream_ca`` (a forge behind a private CA).

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


def token_auth(entry: Mapping[str, Any], credentials: Mapping[str, Any]) -> bool:
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
        if not token_auth(row, credentials):
            raise ValueError("The repository connector holds no token")
        upstream = swap_upstream(row.get("connection_url"))
        return {
            "credential": str(credentials["token"]),
            "allowed_upstream": [upstream.url],
        }

    def service_connector(self, row: Mapping[str, Any]) -> Mapping[str, Any]:
        from orchestrator.services.connector_git_swap_delivery import upstream_ca_of

        try:
            upstream = swap_upstream(row.get("connection_url"))
        except UnservedUpstream:
            # No host to pin: the pod is refused at launch (and a running one
            # stops: its declared egress no longer holds).
            return {**row, "config": {}}
        config = {"upstream": upstream.url, "host": upstream.host}
        ca = upstream_ca_of(row.get("config"))
        if ca is not None:
            # The only roots the pod trusts its upstream with (a private CA);
            # a change starts a new pod (the credential generation).
            config["upstream_ca"] = ca
        return {**row, "config": config}


def route_token_repository(
    entry: dict[str, Any],
    *,
    git_swap: Any,
    fallback: str,
) -> None:
    """Route a repository payload entry, in place (see the module docstring).

    ``git_swap`` is the installed :class:`GitSwapDriver` or ``None``. An entry
    that does not clone with a token is left alone. A fallback is logged
    once per connector and reason (every claim of a session binds again);
    the ``token-in-url`` entry is the one SRW sent before C3 plus the block
    that says why.
    """
    from orchestrator.services.connector_git_swap_delivery import apply_fallback

    credentials = entry.get("credentials")
    if not isinstance(credentials, Mapping) or not token_auth(entry, credentials):
        return
    if git_swap is None:
        # An installation without the driver: the entry SRW always sent,
        # untouched (or refused, where the installation says so).
        if fallback == FALLBACK_REFUSE:
            apply_fallback(
                entry,
                "the git swap driver is not installed (connectors.drivers.gitSwap)",
                fallback=fallback,
            )
        return
    try:
        swap_upstream(entry.get("connection_url"))
    except UnservedUpstream as exc:
        apply_fallback(
            entry, f"the git swap driver cannot serve it: {exc}", fallback=fallback
        )
        return
    # A candidate: the lease step decides per delivery.
    entry["git_swap"] = {}


__all__ = ["GitSwapDriver", "route_token_repository", "token_auth"]
