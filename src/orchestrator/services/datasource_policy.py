"""Central connector availability, authorization, and default selection.

This module deliberately has no FastAPI dependency. HTTP, MCP, schedulers and
session lifecycle callers translate its small exception surface at their own
boundary, while every path shares the same target-bound policy decision.

The functions return IDs, not credentials. Callers persist the complete result
and only then use the ordinary explicit datasource resolver to deliver secrets.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Literal, Mapping
from uuid import UUID

from shared.connectors.builtin import spec_for_row
from shared.connectors.contract import read_only_ceiling
from shared.connectors.platform import (
    CLOUD_OUTBOX,
    managed_kind,
    native_cloud_project,
    native_project,
    native_user,
)

LITE_WORKSPACE_BACKENDS = frozenset({"virtual", "none"})
GENERIC_UNAVAILABLE_DETAIL = "One or more selected connectors are unavailable"
SHELL_WORKSPACE_DETAIL = (
    "Connectors that need a shell (repositories, credential and generic "
    "environments, SSH keys) require a sandbox or VM workspace"
)


class DatasourceUnavailableError(PermissionError):
    """A complete selection failed authorization or scope validation."""

    def __init__(self) -> None:
        super().__init__(GENERIC_UNAVAILABLE_DETAIL)


class DatasourceWorkspaceTierError(ValueError):
    """An explicit connector whose driver needs a shell cannot run on a lite
    workspace (repositories, credential and generic environments, SSH keys)."""


class DatasourceExecutionKindError(ValueError):
    """An explicit connector whose driver serves another kind of execution
    (a cloud folder mounts into sessions only); the message says which."""


def _normalized_ids(values: Iterable[str] | None) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values or ():
        try:
            normalized = str(UUID(str(value)))
        except (TypeError, ValueError) as exc:
            raise DatasourceUnavailableError() from exc
        if normalized not in seen:
            seen.add(normalized)
            result.append(normalized)
    return result


def _row_project_ids(row: dict[str, Any]) -> set[str]:
    values = row.get("project_ids") or ()
    return {str(value) for value in values}


def _native_project_id(row: dict[str, Any]) -> str | None:
    # The project whose own connector this is: its knowledge base, its cloud
    # folder. A malformed server-owned marker names no project, so the row is
    # never treated as a portable ordinary connector either: migration 0082
    # leaves it restricted and unavailable.
    return native_project(row)


def _user_kind_refuses(row: Mapping[str, Any], owner_id: str | None) -> bool:
    """Whether a user's own connector (a ``USER_KINDS`` managed key) is
    another's to attach.

    Only its user's work may: no administrator override, project link,
    membership or trusted inheritance reaches it, and ownerless work never
    does.
    """
    user = native_user(row)
    if user is None:
        return False
    return not (
        owner_id is not None
        and user == owner_id
        and str(row.get("created_by") or "") == owner_id
    )


def _native_default(
    row: Mapping[str, Any], owner_id: str, target_project_ids: set[str]
) -> bool:
    """Whether the row is the work's own: a target project's own connector
    (its knowledge base, its cloud folder) or the owner's own (a user kind).
    These attach whenever work takes no explicit list."""
    project = native_project(row)
    if project is not None:
        return project in target_project_ids
    return native_user(row) == owner_id


def _scope_matches(
    row: dict[str, Any],
    target_project_ids: set[str],
    member_projects: Iterable[str] = (),
) -> bool:
    native_project_id = _native_project_id(row)
    if native_project_id is not None:
        # A project's own connectors represent one binding per project. In a
        # multi-project session each selected project's native binding is
        # independently valid, rather than requiring one synthetic row to be
        # linked to every project. A project's cloud folder alone reaches
        # past its project: into any work of a member of its project
        # (``member_projects``: the owner's projects), never as a default.
        if native_project_id in target_project_ids:
            return True
        return native_cloud_project(row) is not None and (
            native_project_id in set(member_projects)
        )
    if row.get("scope_mode") == "all":
        return True
    if row.get("scope_mode") != "projects" or not target_project_ids:
        return False
    return target_project_ids.issubset(_row_project_ids(row))


def access_ceiling_for(row: Mapping[str, Any], role: str | None) -> str | None:
    """The highest level the owner may bind ``row`` at, from the owner's
    role in the row's project, or ``None`` for no ceiling.

    Only a project's cloud folder has one: a viewer of its project gets at
    most its read-only level (``read_only_ceiling``), in its own project's
    work and another's alike; an editor or owner, or a non-member admin of
    its own project, gets every level. Every other connector keeps its
    policy (``None``).
    """
    if native_cloud_project(row) is None or role != "viewer":
        return None
    spec = spec_for_row(row)
    # A strict spec always has a level that writes nothing; one that does
    # not leaves a viewer no level at all, never a write.
    return (read_only_ceiling(spec) if spec is not None else None) or "none"


def execution_kind_detail(row: Mapping[str, Any], execution_kind: str) -> str:
    """Why an explicit selection of ``row`` is refused for ``execution_kind``,
    in the words of its driver's spec."""
    spec = spec_for_row(row)
    title = spec.title if spec is not None else "These"
    served = " and ".join(f"{kind}s" for kind in (spec.executions if spec else ()))
    return (
        f"{title} connectors attach to {served or 'no execution'} only; "
        f"a {execution_kind} cannot attach one"
    )


def execution_kind_refuses(row: Mapping[str, Any], execution_kind: str | None) -> bool:
    """Whether the connector cannot be attached to this kind of execution.

    The connector's driver names the executions it serves
    (``spec.executions``: a cloud folder mounts into sessions only). ``None``
    asks nothing; a type no driver serves refuses nothing.
    """
    if execution_kind is None:
        return False
    spec = spec_for_row(row)
    return spec is not None and execution_kind not in spec.executions


def workspace_tier_refuses(
    row: Mapping[str, Any], workspace_backend: str | None
) -> bool:
    """Whether a lite workspace tier cannot serve this connector.

    The connector's driver names the workspace backends it works with
    (``spec.supported_backends``): repositories, credential and generic
    environments, SSH keys and credential files need a shell, which the lite
    tiers (virtual, none) do not have. Only a lite tier is ever refused here; the attach-time
    revalidation passes no backend at all. The one rule behind create-time
    authorization, implicit-default filtering and dispatch.
    """
    backend = str(workspace_backend or "").lower()
    if backend not in LITE_WORKSPACE_BACKENDS:
        return False
    spec = spec_for_row(row)
    return spec is not None and backend not in spec.supported_backends


async def _validate_effective_owner(
    db,
    effective_work_owner_id: str | None,
    target_project_ids: list[str],
) -> bool:
    """Validate the authoritative work owner and target membership.

    ``False`` is reserved for genuinely ownerless trusted/system work. A named
    but missing, unapproved, or no-longer-member owner fails the whole
    operation; it must not degrade to an empty/default admin universe.
    """
    if effective_work_owner_id is None:
        return False
    try:
        normalized_owner_id = str(UUID(str(effective_work_owner_id)))
    except (TypeError, ValueError) as exc:
        raise DatasourceUnavailableError() from exc
    owner = await db.get_user(normalized_owner_id)
    if not owner or owner.get("is_approved") is False:
        raise DatasourceUnavailableError()
    if (
        target_project_ids
        and not owner.get("is_admin")
        and not await db.user_is_member_of_projects(
            normalized_owner_id, target_project_ids
        )
    ):
        raise DatasourceUnavailableError()
    return True


@dataclass(frozen=True)
class ItemVerdict:
    """One connector's availability decision, for callers that must report
    rather than deny. ``reason`` is None exactly when ``denied`` is False;
    ``detail`` is a sentence a person can read, for a refusal that may say
    why (``execution_kind``). ``access_ceiling`` is the highest level an
    allowed connector may be bound at for the owner (``access_ceiling_for``:
    a project viewer's cloud folder), ``None`` for no ceiling."""

    datasource_id: str
    denied: bool
    reason: str | None = None
    detail: str | None = None
    access_ceiling: str | None = None


async def classify_datasource_selection(
    db,
    actor: dict[str, Any] | None,
    effective_work_owner_id: str | None,
    datasource_ids: list[str] | None,
    target_project_ids: list[str] | None,
    workspace_backend: str | None,
    allow_admin_explicit_override: bool = False,
    trusted_system_inheritance: bool = False,
    legacy_job_id: str | None = None,
    execution_kind: str | None = None,
) -> tuple[list[ItemVerdict], dict[str, int]]:
    """Per-item availability verdicts plus the policy snapshot of allowed rows.

    The reporting half of :func:`authorize_datasource_selection`, which is now a
    thin wrapper over this. Malformed uuids and a vanished owner row still raise
    here because both were checked BEFORE the per-item loop pre-refactor, so
    raising early is faithful. An unreadable ``policy_revision`` is reported as
    a ``corrupt_revision`` verdict instead, preserving list order; the wrapper
    turns it into the same generic denial the original raised. Corruption is not
    drift — it is not an acknowledgeable reason.

    ``workspace_tier`` is returned as a verdict rather than raised so the caller
    controls precedence; see the wrapper.
    """
    normalized_ids = _normalized_ids(datasource_ids)
    normalized_projects = _normalized_ids(target_project_ids)
    try:
        normalized_legacy_job_id = (
            str(UUID(str(legacy_job_id))) if legacy_job_id else None
        )
    except (TypeError, ValueError) as exc:
        raise DatasourceUnavailableError() from exc
    if not normalized_ids:
        return [], {}

    owner_is_authoritative = await _validate_effective_owner(
        db, effective_work_owner_id, normalized_projects
    )
    owner_id = (
        str(UUID(str(effective_work_owner_id)))
        if owner_is_authoritative and effective_work_owner_id
        else None
    )
    target_set = set(normalized_projects)
    if normalized_legacy_job_id is None:
        rows = await db.get_datasource_policy_rows(normalized_ids)
    else:
        rows = await db.get_datasource_policy_rows(
            normalized_ids,
            legacy_job_id=normalized_legacy_job_id,
        )
    by_id = {str(row["id"]): row for row in rows}
    # The owner's role in the project of every project cloud folder named:
    # a member of its project may attach it in any work, a viewer read-only.
    cloud_projects = sorted(
        {project for row in rows if (project := native_cloud_project(row)) is not None}
    )
    owner_roles: dict[str, str] = (
        await db.get_user_project_roles(owner_id, cloud_projects)
        if owner_id is not None and cloud_projects
        else {}
    )

    actor_is_admin = bool(actor and actor.get("is_admin"))
    admin_override = allow_admin_explicit_override and actor_is_admin

    verdicts: list[ItemVerdict] = []
    revisions: dict[str, int] = {}
    for datasource_id in normalized_ids:
        row = by_id.get(datasource_id)
        if row is None:
            verdicts.append(ItemVerdict(datasource_id, True, "deleted"))
            continue
        if not _scope_matches(row, target_set, owner_roles):
            verdicts.append(ItemVerdict(datasource_id, True, "out_of_scope"))
            continue

        linked_to_every_target = bool(target_set) and target_set.issubset(
            _row_project_ids(row)
        )
        native_target = _native_project_id(row)
        native_access = bool(
            owner_is_authoritative
            and native_target is not None
            and (
                native_target in target_set
                or (
                    native_cloud_project(row) is not None
                    and native_target in owner_roles
                )
            )
        )
        legacy_job_binding = bool(
            normalized_legacy_job_id
            and str(row.get("job_id") or "") == normalized_legacy_job_id
        )
        execution_authorized = (
            trusted_system_inheritance
            or legacy_job_binding
            or (owner_id is not None and str(row.get("created_by") or "") == owner_id)
            or bool(row.get("is_global"))
            or (owner_is_authoritative and linked_to_every_target)
            or native_access
            or admin_override
        )
        if not execution_authorized or _user_kind_refuses(row, owner_id):
            verdicts.append(ItemVerdict(datasource_id, True, "revoked"))
            continue
        if workspace_tier_refuses(row, workspace_backend):
            verdicts.append(ItemVerdict(datasource_id, True, "workspace_tier"))
            continue
        if execution_kind_refuses(row, execution_kind):
            verdicts.append(
                ItemVerdict(
                    datasource_id,
                    True,
                    "execution_kind",
                    execution_kind_detail(row, str(execution_kind)),
                )
            )
            continue
        try:
            policy_revision = int(row["policy_revision"])
            if policy_revision < 1:
                raise ValueError
        except (KeyError, TypeError, ValueError):
            verdicts.append(ItemVerdict(datasource_id, True, "corrupt_revision"))
            continue
        revisions[datasource_id] = policy_revision
        verdicts.append(
            ItemVerdict(
                datasource_id,
                False,
                None,
                access_ceiling=access_ceiling_for(
                    row, owner_roles.get(native_target or "")
                ),
            )
        )
    return verdicts, revisions


async def authorize_datasource_selection(
    db,
    actor: dict[str, Any] | None,
    effective_work_owner_id: str | None,
    datasource_ids: list[str] | None,
    target_project_ids: list[str] | None,
    workspace_backend: str | None,
    allow_admin_explicit_override: bool = False,
    trusted_system_inheritance: bool = False,
    legacy_job_id: str | None = None,
    execution_kind: str | None = None,
) -> tuple[list[str], dict[str, int]]:
    """Authorize a complete selection and return its exact policy snapshot.

    The return order follows the first occurrence in ``datasource_ids``.
    Missing, malformed, unauthorized and out-of-scope IDs all raise the same
    non-enumerating exception. ``trusted_system_inheritance`` is narrowly for
    internal reuse of an already materialized selection: it bypasses user
    execution access, but never scope or workspace-tier checks and never scans
    for ambient defaults. ``execution_kind`` (``session`` or ``job``) refuses
    a connector its driver serves only for the other kind, with
    :class:`DatasourceExecutionKindError`; ``None`` asks nothing.
    """
    verdicts, revisions = await classify_datasource_selection(
        db,
        actor,
        effective_work_owner_id,
        datasource_ids,
        target_project_ids,
        workspace_backend,
        allow_admin_explicit_override=allow_admin_explicit_override,
        trusted_system_inheritance=trusted_system_inheritance,
        legacy_job_id=legacy_job_id,
        execution_kind=execution_kind,
    )
    # Missing rows were rejected by a len() check that ran BEFORE the per-item
    # loop, so "deleted" is position-independent and outranks everything else.
    if any(v.reason == "deleted" for v in verdicts):
        raise DatasourceUnavailableError()
    # Every other failure raised from inside that loop, so it is strictly
    # first-in-list-order: the first denied item decides which error surfaces.
    for verdict in verdicts:
        if not verdict.denied:
            continue
        if verdict.reason == "workspace_tier":
            raise DatasourceWorkspaceTierError(SHELL_WORKSPACE_DETAIL)
        if verdict.reason == "execution_kind":
            raise DatasourceExecutionKindError(verdict.detail)
        raise DatasourceUnavailableError()
    return [v.datasource_id for v in verdicts], revisions


async def authorize_datasource_ids(
    db,
    actor: dict[str, Any] | None,
    effective_work_owner_id: str | None,
    datasource_ids: list[str] | None,
    target_project_ids: list[str] | None,
    workspace_backend: str | None,
    allow_admin_explicit_override: bool = False,
    trusted_system_inheritance: bool = False,
    legacy_job_id: str | None = None,
) -> list[str]:
    """Compatibility wrapper returning only authorized connector IDs."""
    selected, _revisions = await authorize_datasource_selection(
        db,
        actor,
        effective_work_owner_id,
        datasource_ids,
        target_project_ids,
        workspace_backend,
        allow_admin_explicit_override,
        trusted_system_inheritance,
        legacy_job_id,
    )
    return selected


async def default_datasource_selection(
    db,
    effective_work_owner_id: str | None,
    target_project_ids: list[str] | None,
    workspace_backend: str | None,
    *,
    execution_kind: Literal["session", "job"],
    native_only: bool = False,
    unattended: bool = False,
) -> tuple[list[str], dict[str, int]]:
    """Return default IDs and the policy revisions used to select them.

    Shared/public connector publisher preferences never affect another user.
    The work's own connectors (``_native_default``: a target project's
    knowledge base and cloud folder, the owner's own) are the project-managed
    exception, and a project admin's connector defaults
    (``project_connector_defaults``, slice D3c) add the connectors linked to
    every target project that the project chose (candidate rows flagged
    ``project_default``), whatever their owner's own ``auto_attach``.
    ``native_only`` keeps only the work's own connectors: what work that
    names no connector list attaches when the deployment does not attach
    defaults on omission. Connectors a lite workspace tier cannot serve
    (``workspace_tier_refuses``), or this kind of execution cannot attach
    (``execution_kind_refuses``: a cloud folder is a session's), are silently
    omitted because this is an implicit default rather than an explicit user
    requirement. ``unattended`` work (a loop's iteration, an automation's
    fire, an officer's auto-pulled ticket) leaves the owner's outbox out:
    nobody started it to receive its deliverables in a shared folder.

    The revision snapshot comes from the exact candidate rows used for the
    decision. Callers pass both values to the atomic creation API, which locks
    and compares those revisions before inserting work and attachments. A
    separate follow-up revision read would leave a gap in which a scope or
    auto-attach edit could be mistaken for the policy that selected the IDs.
    """
    normalized_projects = _normalized_ids(target_project_ids)
    if effective_work_owner_id is None:
        return [], {}
    await _validate_effective_owner(db, effective_work_owner_id, normalized_projects)
    owner_id = str(UUID(str(effective_work_owner_id)))
    target_set = set(normalized_projects)
    rows = await db.list_default_datasource_candidates(owner_id, normalized_projects)

    selected: list[str] = []
    revisions: dict[str, int] = {}
    for row in rows:
        if not _scope_matches(row, target_set) or _user_kind_refuses(row, owner_id):
            continue
        owned = str(row.get("created_by") or "") == owner_id
        native_default = _native_default(row, owner_id, target_set)
        if native_only and not native_default:
            continue
        project_default = (
            bool(row.get("project_default"))
            and bool(target_set)
            and target_set.issubset(_row_project_ids(row))
        )
        if not (
            (row.get("auto_attach") and (owned or native_default)) or project_default
        ):
            continue
        if workspace_tier_refuses(row, workspace_backend):
            continue
        if execution_kind_refuses(row, execution_kind):
            continue
        if unattended and managed_kind(row) == CLOUD_OUTBOX:
            continue
        try:
            datasource_id = str(UUID(str(row["id"])))
            policy_revision = int(row["policy_revision"])
            if policy_revision < 1:
                raise ValueError
        except (KeyError, TypeError, ValueError) as exc:
            raise DatasourceUnavailableError() from exc
        if datasource_id not in revisions:
            selected.append(datasource_id)
            revisions[datasource_id] = policy_revision
    return selected, revisions


async def default_datasource_ids(
    db,
    effective_work_owner_id: str | None,
    target_project_ids: list[str] | None,
    workspace_backend: str | None,
    *,
    execution_kind: Literal["session", "job"],
    native_only: bool = False,
) -> list[str]:
    """Compatibility wrapper returning only creation-time default IDs."""
    selected, _revisions = await default_datasource_selection(
        db,
        effective_work_owner_id,
        target_project_ids,
        workspace_backend,
        execution_kind=execution_kind,
        native_only=native_only,
    )
    return selected


__all__ = [
    "DatasourceExecutionKindError",
    "DatasourceUnavailableError",
    "DatasourceWorkspaceTierError",
    "GENERIC_UNAVAILABLE_DETAIL",
    "SHELL_WORKSPACE_DETAIL",
    "ItemVerdict",
    "access_ceiling_for",
    "authorize_datasource_ids",
    "authorize_datasource_selection",
    "classify_datasource_selection",
    "default_datasource_ids",
    "default_datasource_selection",
    "execution_kind_detail",
    "execution_kind_refuses",
    "workspace_tier_refuses",
]
