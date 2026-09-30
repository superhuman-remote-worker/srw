"""Which workspace work gets when it names none (Slice A2b).

Two lookups, each Project → installation → built-in: a tier *mode* per role
(``jobs``, ``sessions``) and one template per tier (``container``, ``vm``)
shared by both roles. This module is pure. The orchestrator supplies the
Project row, and the chart supplies ``WORKSPACE_DEFAULTS`` and the built-ins.
"""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass

logger = logging.getLogger(__name__)

ENV_NAME = "WORKSPACE_DEFAULTS"
WORKSPACE_MODES = ("none", "virtual", "container", "vm")
TEMPLATE_TIERS = ("container", "vm")
MODE_BACKEND = {
    "none": "none",
    "virtual": "virtual",
    "container": "sandbox",
    "vm": "vm",
}
MODE_RANK = {"none": 0, "virtual": 0, "container": 1, "vm": 2}
BUILTIN_TEMPLATES = {"container": "container-full", "vm": "vm-full"}
CATALOG_SHARED = {"kind": "Catalog", "name": "shared"}
UPGRADE_REFUSED = "An upgrade must move to a higher tier than the current one."

_BACKEND_MODE = {
    "none": "none",
    "virtual": "virtual",
    "sandbox": "container",
    "container": "container",
    "vm": "vm",
    "remote": "vm",
}
_ROLE_FIELD = {"worker": "jobs", "job": "jobs", "session": "sessions"}


class InvalidWorkspaceDefaults(ValueError):
    """``WORKSPACE_DEFAULTS`` holds something the chart never renders."""


class UpgradeRefused(ValueError):
    """The requested upgrade doesn't move up a tier."""


def role_field(role: str) -> str:
    try:
        return _ROLE_FIELD[role]
    except KeyError:
        raise ValueError(f"Unknown workspace role {role!r}") from None


def backend_mode(backend: str) -> str:
    try:
        return _BACKEND_MODE[backend]
    except KeyError:
        raise ValueError(f"Unknown workspace backend {backend!r}") from None


def next_mode(current: str) -> str:
    return "container" if MODE_RANK[current] == 0 else "vm"


@dataclass(frozen=True)
class InstallationDefaults:
    jobs: str = "container"
    sessions: str = "virtual"
    container: str | None = None
    vm: str | None = None


def installation_defaults(
    environ: Mapping[str, str] | None = None,
) -> InstallationDefaults:
    """Parse the chart's values; an empty field means the shipped value."""
    raw = (os.environ if environ is None else environ).get(ENV_NAME)
    if not raw:
        return InstallationDefaults()
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise InvalidWorkspaceDefaults(f"{ENV_NAME} is not JSON: {exc.msg}") from None
    if not isinstance(value, dict) or set(value) - {
        "jobs",
        "sessions",
        "container",
        "vm",
    }:
        raise InvalidWorkspaceDefaults(
            f"{ENV_NAME} must be an object with jobs, sessions, container and vm."
        )
    shipped = InstallationDefaults()
    fields: dict[str, str | None] = {}
    for field in ("jobs", "sessions"):
        mode = value.get(field) or getattr(shipped, field)
        if mode not in WORKSPACE_MODES:
            raise InvalidWorkspaceDefaults(
                f"workspace.defaults.{field} must be one of {', '.join(WORKSPACE_MODES)}."
            )
        fields[field] = mode
    for field in TEMPLATE_TIERS:
        name = value.get(field) or None
        if name is not None and not isinstance(name, str):
            raise InvalidWorkspaceDefaults(
                f"workspace.defaults.{field} must be a template name."
            )
        fields[field] = name
    return InstallationDefaults(**fields)


def default_backend(role: str, environ: Mapping[str, str] | None = None) -> str:
    """The installation's backend for a role, for floors that must never raise."""
    try:
        defaults = installation_defaults(environ)
    except InvalidWorkspaceDefaults as exc:
        logger.error("Ignoring invalid %s: %s", ENV_NAME, exc)
        defaults = InstallationDefaults()
    return MODE_BACKEND[getattr(defaults, role_field(role))]


@dataclass(frozen=True)
class ProjectDefaults:
    jobs: str | None = None
    sessions: str | None = None
    container: dict | None = None
    vm: dict | None = None
    source: str = "settings"
    manifest_revision: str | None = None


@dataclass(frozen=True)
class Upgrade:
    current: str
    requested: str | None = None


@dataclass(frozen=True)
class WorkspaceResolution:
    mode: str
    template: dict | None
    tier_source: str
    template_source: str | None
    project_revision: str | None = None

    def binding(self) -> dict | None:
        if self.mode == "none":
            return None
        if self.template is None:
            return {"template": {"inline": {"backend": MODE_BACKEND[self.mode]}}}
        return {"template": deepcopy(self.template)}

    def sources(self) -> dict:
        return {"tier": self.tier_source, "template": self.template_source}

    def template_name(self) -> str | None:
        ref = (self.template or {}).get("ref")
        return ref.get("name") if isinstance(ref, dict) else None


def resolve_defaults(
    role: str,
    *,
    project: ProjectDefaults | None,
    installation: InstallationDefaults,
    builtins: frozenset[str],
    upgrade: Upgrade | None = None,
) -> WorkspaceResolution:
    """Tier: upgrade request, else Project mode, else installation mode.
    Template (container and VM only): Project, else installation, else built-in."""
    field = role_field(role)
    if upgrade is not None:
        mode = upgrade.requested or next_mode(upgrade.current)
        if mode not in TEMPLATE_TIERS or MODE_RANK[mode] <= MODE_RANK[upgrade.current]:
            raise UpgradeRefused(UPGRADE_REFUSED)
        tier_source = "upgrade"
    elif project is not None and getattr(project, field) is not None:
        mode, tier_source = getattr(project, field), "project"
    else:
        mode, tier_source = getattr(installation, field), "installation"
    template, template_source = None, None
    if mode in TEMPLATE_TIERS:
        template, template_source = tier_template(
            mode, project=project, installation=installation, builtins=builtins
        )
    uses_project = "project" in (tier_source, template_source)
    revision = (
        project.manifest_revision
        if uses_project and project is not None and project.source == "manifest"
        else None
    )
    return WorkspaceResolution(mode, template, tier_source, template_source, revision)


def tier_template(
    tier: str,
    *,
    project: ProjectDefaults | None,
    installation: InstallationDefaults,
    builtins: frozenset[str],
) -> tuple[dict | None, str]:
    """The template a container or VM of this Project gets, and its layer."""
    if project is not None and getattr(project, tier) is not None:
        return deepcopy(getattr(project, tier)), "project"
    name = getattr(installation, tier)
    if name:
        return {"ref": {"name": name, "scope": dict(CATALOG_SHARED)}}, "installation"
    builtin = BUILTIN_TEMPLATES[tier]
    if builtin in builtins:
        return {"ref": {"name": builtin, "scope": dict(CATALOG_SHARED)}}, "builtin"
    return None, "builtin"
