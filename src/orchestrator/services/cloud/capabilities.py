"""What a main-cloud provider can deliver: the provider support matrix.

Each adapter declares, as data, which combination of connector type, folder
kind, access level and workspace tier it delivers (``capabilities`` on the
adapter class). Code outside the adapters asks this declaration instead of
comparing a ``backend_id`` with a provider name
(``scripts/check_cloud_provider_branches.py`` keeps it that way), and the
admin Main cloud page renders it.

Following manifest §7, a provider **offers** a level only where it enforces
it: ``enforced_by`` says how, in one line, and a level it cannot enforce is
``unsupported`` with the reason, never offered with an asterisk. ``planned``
names a combination a later slice of the design builds. The workspace tiers
use the driver contract's vocabulary (``sandbox``, ``vm``, ``virtual``,
``none``) and list only the tiers whose delivery path exists today.

The vocabulary is the driver contract's on purpose: when ``cloud_folder``
becomes a driver (slice 3), :meth:`ProviderCapabilities.access_levels` is its
spec's ``access_levels``, so the D2 capability matrix shows the same answer.

Design: knowledge-base/knowledge/features/main_cloud_as_connectors.md,
"Provider support matrix", and connector_drivers.md, D4.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from shared.connectors.contract import WORKSPACE_BACKENDS, AccessLevel

ConnectorType = Literal["cloud_folder", "cloud_folder_checkout", "cloud_outbox"]
FolderKind = Literal["project", "user_root"]
Status = Literal["offered", "planned", "unsupported"]
STATUSES: tuple[str, ...] = ("offered", "planned", "unsupported")

#: Every combination the matrix answers, in the order the page lists them.
#: ``(connector type, folder kind, access level)``; an outbox has no folder
#: kind (SRW creates its folder per execution).
MATRIX_ROWS: tuple[tuple[str, str | None, str], ...] = (
    ("cloud_folder", "project", "read_only"),
    ("cloud_folder", "project", "read_write"),
    ("cloud_folder", "project", "protected"),
    ("cloud_folder", "user_root", "read_write"),
    ("cloud_folder", "user_root", "read_only"),
    ("cloud_folder", "user_root", "protected"),
    ("cloud_folder_checkout", "project", "reviewed_write_back"),
    ("cloud_outbox", None, "read_write"),
)

#: Access ranks per connector type, as a driver spec ranks its levels (the
#: highest rank wins across connectors of one category). ``protected`` sits
#: below ``read_write``: its writes reach the folder only after review.
ACCESS_RANKS: dict[str, dict[str, int]] = {
    "cloud_folder": {"read_only": 10, "protected": 20, "read_write": 30},
    "cloud_folder_checkout": {"reviewed_write_back": 10},
    "cloud_outbox": {"read_write": 10},
}

#: The protected review lane: a project folder at the ``protected`` level.
PROTECTED_PROJECT_FOLDER = ("cloud_folder", "project", "protected")


@dataclass(frozen=True, slots=True)
class CloudCapability:
    """One cell of the matrix: what a provider does for one combination.

    ``note`` is the ``enforced_by`` line for an offered or planned level and
    the reason for an unsupported one. ``supported_backends`` are the
    workspace tiers it is offered on (empty unless offered or planned);
    ``slice`` is the design slice that builds a planned one.
    """

    connector_type: str
    folder_kind: str | None
    access: str
    status: Status
    note: str
    supported_backends: frozenset[str] = frozenset()
    slice: int | None = None

    @property
    def key(self) -> tuple[str, str | None, str]:
        return (self.connector_type, self.folder_kind, self.access)


@dataclass(frozen=True, slots=True)
class ProviderCapabilities:
    """One adapter's declaration: one cell per :data:`MATRIX_ROWS` entry."""

    backend_id: str
    title: str
    capabilities: tuple[CloudCapability, ...]

    def capability(
        self, connector_type: str, folder_kind: str | None, access: str
    ) -> CloudCapability | None:
        key = (connector_type, folder_kind, access)
        return next((cap for cap in self.capabilities if cap.key == key), None)

    def offers(
        self,
        connector_type: str,
        folder_kind: str | None,
        access: str,
        *,
        workspace_backend: str | None = None,
    ) -> bool:
        """Whether the combination is offered (on ``workspace_backend``)."""
        cap = self.capability(connector_type, folder_kind, access)
        return bool(
            cap is not None
            and cap.status == "offered"
            and (
                workspace_backend is None or workspace_backend in cap.supported_backends
            )
        )

    def access_levels(
        self, connector_type: str, folder_kind: str | None
    ) -> tuple[AccessLevel, ...]:
        """The offered levels as a driver spec's ``access_levels``, by rank."""
        ranks = ACCESS_RANKS.get(connector_type, {})
        levels = [
            AccessLevel(id=cap.access, rank=ranks[cap.access], enforced_by=cap.note)
            for cap in self.capabilities
            if cap.connector_type == connector_type
            and cap.folder_kind == folder_kind
            and cap.status == "offered"
            and cap.access in ranks
        ]
        return tuple(sorted(levels, key=lambda level: level.rank))


def validate_capabilities(declared: ProviderCapabilities) -> list[str]:
    """Every problem with a declaration, as lines (empty when valid)."""
    problems: list[str] = []
    if not declared.backend_id or not declared.title:
        problems.append("backend_id and title are required")
    keys = [cap.key for cap in declared.capabilities]
    if len(keys) != len(set(keys)):
        problems.append("a combination is declared twice")
    missing = sorted(set(MATRIX_ROWS) - set(keys), key=str)
    if missing:
        problems.append(f"combinations not declared: {missing}")
    for cap in declared.capabilities:
        where = "/".join(str(part) for part in cap.key)
        if cap.key not in MATRIX_ROWS:
            problems.append(f"{where} is not a matrix combination")
        if cap.status not in STATUSES:
            problems.append(f"{where}: status {cap.status!r} is not one of {STATUSES}")
        if not cap.note:
            problems.append(f"{where}: needs an enforced_by line or a reason")
        unknown = sorted(cap.supported_backends - WORKSPACE_BACKENDS)
        if unknown:
            problems.append(f"{where}: unknown workspace tiers {unknown}")
        if cap.status == "unsupported" and cap.supported_backends:
            problems.append(f"{where}: an unsupported level names no tiers")
        if cap.status != "unsupported" and not cap.supported_backends:
            problems.append(f"{where}: an offered or planned level names its tiers")
        if (cap.status == "planned") != (cap.slice is not None):
            problems.append(f"{where}: a planned level, and only one, names a slice")
    return problems


def capability_matrix(
    providers: list[ProviderCapabilities], *, active: str | None = None
) -> dict[str, Any]:
    """The matrix as JSON: one row per combination, one cell per provider."""
    return {
        "providers": [
            {
                "backend_id": declared.backend_id,
                "title": declared.title,
                "active": declared.backend_id == active,
            }
            for declared in providers
        ],
        "rows": [
            {
                "connector_type": connector_type,
                "folder_kind": folder_kind,
                "access": access,
                "cells": {
                    declared.backend_id: _cell(
                        declared.capability(connector_type, folder_kind, access)
                    )
                    for declared in providers
                },
            }
            for connector_type, folder_kind, access in MATRIX_ROWS
        ],
    }


def _cell(cap: CloudCapability | None) -> dict[str, Any]:
    if cap is None:
        return {
            "status": "unsupported",
            "note": "not declared",
            "workspace_backends": [],
            "slice": None,
        }
    return {
        "status": cap.status,
        "note": cap.note,
        "workspace_backends": sorted(cap.supported_backends),
        "slice": cap.slice,
    }


__all__ = [
    "ACCESS_RANKS",
    "MATRIX_ROWS",
    "PROTECTED_PROJECT_FOLDER",
    "STATUSES",
    "CloudCapability",
    "ProviderCapabilities",
    "capability_matrix",
    "validate_capabilities",
]
