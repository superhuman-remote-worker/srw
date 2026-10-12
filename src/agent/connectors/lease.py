"""``lease_token``: credential lease tokens, written under ``~/.srw-credentials/``.

A connector whose driver delivers by lease (slice C2) never sends its
upstream credential to the agent. The orchestrator issues a lease for the
workspace-owning execution and sends its token instead; this materializer
writes it to ``~/.srw-credentials/leases/<connector id>`` (0600) over the
workspace's secret stdin channel, never through tmux, and never into the
agent process's environment. C0 keeps the directory out of snapshots.

A driver that needs the token (the swap driver of C3) reads that file. The
file belongs to the physical workspace, so a backend swap writes it again on
the new host; a live detach removes it (the orchestrator has already revoked
the lease, so the file is housekeeping, not revocation).
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import Any

from agent.connectors.base import (
    Delivery,
    FactsLines,
    RuntimeContext,
    declared_read_only_note,
)
from shared.connectors.leases import lease_file_name

logger = logging.getLogger(__name__)


def _leases(deliveries: Sequence[Delivery]) -> dict[str, str]:
    """Connector id -> token, the last delivery of a connector winning."""
    leases: dict[str, str] = {}
    for delivery in deliveries:
        for value in delivery.values("lease_token"):
            leases[str(value["connector_id"])] = str(value["token"])
    return leases


def _install(backend: Any, leases: dict[str, str]) -> None:
    for connector_id, token in leases.items():
        backend.install_connector_lease(connector_id, token)


class LeaseTokenMaterializer:
    form = "lease_token"

    def materialize(self, deliveries: Sequence[Delivery], rt: RuntimeContext) -> None:
        leases = _leases(deliveries)
        if not leases:
            return
        backend = rt.workspace_backend
        if backend is None or not getattr(backend, "supports_shell", False):
            # The lite tier refuses these connectors at attach
            # (``supported_backends``); a payload that still names one
            # delivers nothing rather than failing the execution.
            logger.warning(
                "%d lease connector(s) need a shell workspace; none delivered",
                len(leases),
            )
            return
        _install(backend, leases)

    def replace(
        self,
        old: Sequence[Delivery],
        new: Sequence[Delivery],
        rt: RuntimeContext,
    ) -> None:
        backend = rt.workspace_backend
        if backend is None or not getattr(backend, "supports_shell", False):
            return
        current = _leases(new)
        for connector_id in sorted(set(_leases(old)) - set(current)):
            try:
                backend.remove_connector_lease(connector_id)
            except Exception:
                logger.warning(
                    "Could not remove the lease file of detached connector %s",
                    connector_id,
                    exc_info=True,
                )
        _install(backend, current)

    def on_backend_swap(self, deliveries: Sequence[Delivery], backend: Any) -> None:
        leases = _leases(deliveries)
        if leases:
            _install(backend, leases)

    def facts(
        self, deliveries: Sequence[Delivery], rt: RuntimeContext
    ) -> list[FactsLines]:
        out: list[FactsLines] = []
        for delivery in deliveries:
            ds = delivery.entry
            lines = [
                f"- **{ds.get('name', 'Unnamed')}** ({ds.get('type', 'unknown')})"
                f"{declared_read_only_note(ds)}"
            ]
            for value in delivery.values("lease_token"):
                lines.append(
                    "  Credential lease: `~/"
                    f"{lease_file_name(str(value['connector_id']))}` holds a "
                    "short-lived token for this connector's driver, not the "
                    "credential itself. Never print or copy it."
                )
            out.append(FactsLines("Other", delivery.index, lines))
        return out
