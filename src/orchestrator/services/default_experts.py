"""Managed expert seeds and authoritative root-creation default selection.

The YAML bundles are bootstrap material only.  Once inserted, the DB row is the
runtime source of truth and startup never overwrites operator edits.  Both jobs
and sessions call :func:`resolve_root_expert`, which keeps precedence and grant
semantics out of individual HTTP/MCP/automation entry points.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from orchestrator.services.grants_service import resolve_grants_for
from shared.runtime.core.expert_resolution import (
    validate_expert_persona_placeholders,
    with_role_tag,
)
from shared.runtime.core.loader import canonical_config_name
from shared.runtime.core.srw_manifest_config import read_srw_config

logger = logging.getLogger(__name__)

ExpertType = Literal["worker", "session"]

BASE_CONFIG_NAMES: dict[ExpertType, str] = {
    "worker": "worker_base",
    "session": "session_base",
}

MANAGED_SEEDS: tuple[dict[str, Any], ...] = (
    {
        "managed_key": "application-default-worker-seed",
        "directory": "general-worker",
        "expert_type": "worker",
        # 2: the General Worker gained the shell tools (a v1 row has no
        # `tools` key, so the additive upgrade hands it the bundle's) and
        # the unedited v1 description, which promised "no shell", is replaced.
        # 3 (2026-11-11, office skills): the unedited v1 persona, written for
        # a worker without a shell, is replaced by the outcome-first one that
        # names the workspace's tools and the office skills.
        "seed_version": 3,
    },
    {
        "managed_key": "application-default-session-seed",
        "directory": "assistant",
        "expert_type": "session",
        # 2: the assistant gained its roster. 3 repairs only that exact
        # managed v2 roster's shell-enabled implementer; an operator's own
        # roster remains authoritative. Other upgrades remain additive.
        # 4 (2026-11-11, shell_tools granted by default): the assistant gains
        # the shell tools (a row without a `tools` key gets the bundle's), the
        # exact managed v3 roster gets its implementer's shell back, and the
        # unedited v3 description, which listed no shell, is replaced.
        "seed_version": 4,
    },
)


# Exact historical seed content, independent of future bundle edits. Comparing
# the whole subtree preserves even small operator changes to a roster entry.
_ASSISTANT_V2_SUBAGENTS = {
    "default": "explorer",
    "roster": {
        "explorer": {"$ref": "subagents/explorer"},
        "reader": {"$ref": "subagents/reader"},
        "implementer": {"$ref": "subagents/implementer"},
    },
}
_ASSISTANT_V3_SUBAGENTS = {
    "default": "explorer",
    "roster": {
        "explorer": {"$ref": "subagents/explorer"},
        "reader": {"$ref": "subagents/reader"},
        "implementer": {
            "$ref": "subagents/implementer",
            "tools": {"shell": []},
        },
    },
}
# Seed 4 lifts seed 3's pin: the roster is the v2 one again. A v2 row already
# carries it, so it gets no roster repair on its way to 4.
_ASSISTANT_V4_SUBAGENTS = _ASSISTANT_V2_SUBAGENTS
# The managed assistant's description through seed 3, exactly as seeded
# (stripped). Seed 4 replaces only this text; an operator's own stays.
_ASSISTANT_V3_DESCRIPTION = (
    "General-purpose interactive assistant and the default for new sessions.\n"
    "Collaborates with the user turn by turn on research, writing, analysis,\n"
    "planning, and light coding — rather than running an autonomous batch job.\n"
    "Inherits the safe persistent toolset (files, research, browser, citations,\n"
    "git, knowledge) from session_base; its only specialization is\n"
    "a conversational, generalist persona (persona.txt)."
)


# The managed General Worker's v1 description, exactly as seeded (stripped).
# Seed 2 replaces only this text; an operator's own description stays.
_GENERAL_WORKER_V1_DESCRIPTION = (
    "General-purpose worker for research, writing, analysis, planning, and file\n"
    "deliverables. It starts with a safe, non-orchestrating tool profile: no shell,\n"
    "delegation, scholar pre-job, critic verification, automations, or loops unless\n"
    "an administrator or copied expert explicitly enables them."
)

# The managed General Worker's persona through seed 2, exactly as seeded
# (compared stripped). Seed 3 replaces only this text; an operator's own stays.
_GENERAL_WORKER_V1_PERSONA = (
    "<role>\n"
    "General-purpose worker that completes a bounded assignment and leaves a clear,\n"
    "verified deliverable in the workspace.\n"
    "</role>\n"
    "\n"
    "<goal>\n"
    "Understand the requested outcome, gather the evidence needed, produce the work,\n"
    "and verify it before reporting completion.\n"
    "</goal>\n"
    "\n"
    "<operating_rules>\n"
    "1. Start from the requested outcome and keep the work scoped to it.\n"
    "2. Use available workspace and research tools when evidence is needed.\n"
    "3. Record useful intermediate work in the workspace rather than relying on memory.\n"
    "4. Verify factual claims and inspect the final deliverable before completion.\n"
    "5. State uncertainty and blockers plainly; never invent sources or tool results.\n"
    "6. Avoid destructive or irreversible actions unless the assignment authorizes them.\n"
    "</operating_rules>\n"
    "\n"
    "<identity_anchors>\n"
    "Remain a careful general worker. You finish the assigned\n"
    "job, verify the result, and do not silently expand its scope.\n"
    "</identity_anchors>"
)


class DefaultExpertUnavailable(RuntimeError):
    """A healthy root-creation path has no compatible application default."""


class ExpertSelectionError(ValueError):
    """An explicit/default expert is invisible, incompatible, or disallowed."""


@dataclass(frozen=True)
class ExpertSelection:
    expert: dict[str, Any]
    source: Literal["explicit", "project", "user", "application"]
    project_override: dict[str, Any] | None = None


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        value = json.loads(value)
    return dict(value) if isinstance(value, dict) else {}


def load_seed_bundle(
    config_dir: Path, *, directory: str, expert_type: ExpertType
) -> dict[str, Any]:
    """Read one bundled expert as a raw DB overlay (never a merged snapshot)."""
    expert_dir = config_dir / "experts" / directory
    config_path = expert_dir / "config.yaml"
    raw = read_srw_config(config_path)
    if not isinstance(raw, dict):
        raise ValueError(f"Invalid managed expert config: {config_path}")
    # Both sides canonical: the public root names (`worker_base`/`session_base`)
    # are what `$extends` values and BASE_CONFIG_NAMES canonicalise to, however
    # the seed spells its base (`defaults`, `overlays/worker`, ...).
    extends = canonical_config_name(str(raw.pop("$extends", "worker_base")))
    expected = canonical_config_name(BASE_CONFIG_NAMES[expert_type])
    if extends != expected:
        raise ValueError(
            f"Managed expert {directory!r} extends {extends!r}; expected {expected!r}"
        )
    raw.pop("connections", None)

    prompts: dict[str, str] = {}
    for key, filename in (
        ("persona", "persona.txt"),
        ("instructions", "instructions.md"),
        ("summarization", "summarization_prompt.txt"),
    ):
        path = expert_dir / filename
        if path.exists():
            prompts[key] = path.read_text(encoding="utf-8")
    validate_expert_persona_placeholders(prompts)

    return {
        "name": directory,
        "display_name": str(
            raw.get("display_name") or directory.replace("-", " ").title()
        ),
        "description": str(raw.get("description") or "").strip(),
        "icon": str(raw.get("icon") or "smart_toy"),
        "color": str(raw.get("color") or "#6B7280"),
        # tags ∪ {role}: the seeded row carries its role tag like every row
        # written through the API (U1 B.4) — no SQL, the same pure helper.
        "tags": with_role_tag(expert_type, raw.get("tags")),
        "expert_type": expert_type,
        "config": raw,
        "prompts": prompts,
    }


async def upgrade_managed_seed(
    db, *, spec: dict[str, Any], bundle: dict[str, Any], row: dict[str, Any]
) -> dict[str, Any] | None:
    """Upgrade a managed seed while preserving operator-owned content.

    Copies only the bundle's top-level ``config`` keys the row does NOT carry
    and stamps the new version; a key the row has — whatever its value — is
    the operator's and stays. Returns the updated row, or ``None`` when the
    row is current. This is how a seeded row gains a block the bundle grew
    later (the assistant's ``subagents`` roster: every deployment seeded before
    2026-09-07 bound ``delegate_agent`` with nothing to delegate to; the
    ``tools`` shell group of the General Worker and the assistant, 2026-11-11).
    Exact-content repairs replace a value the bundle changed: the managed v2
    and v3 assistant rosters, and the shipped descriptions that promised no
    shell (General Worker v1, assistant v3). The SQL compares each again under
    the row lock, so a concurrent operator edit cannot be overwritten using
    this earlier read.
    """
    current = int(row.get("seed_version") or 0)
    target = int(spec["seed_version"])
    if current >= target:
        return None
    existing = _json_object(row.get("config"))
    additions = {
        key: value
        for key, value in (bundle.get("config") or {}).items()
        if key not in existing
    }
    repair = {}
    session_seed = spec["managed_key"] == "application-default-session-seed"
    if (
        session_seed
        and current == 2
        and target == 3
        and existing.get("subagents") == _ASSISTANT_V2_SUBAGENTS
    ):
        repair = {
            "expected_seed_version": 2,
            "expected_subagents": _ASSISTANT_V2_SUBAGENTS,
            # A future bundle may carry unrelated roster changes. A v2 row
            # still gets this historical repair, never those newer values.
            "replacement_subagents": _ASSISTANT_V3_SUBAGENTS,
        }
    if (
        session_seed
        and current == 3
        and target >= 4
        and existing.get("subagents") == _ASSISTANT_V3_SUBAGENTS
    ):
        # Seed 4 undoes seed 3's pin on exactly the roster seed 3 wrote. (A v2
        # row skips both repairs: its roster already is the v4 one.)
        repair = {
            "expected_seed_version": 3,
            "expected_subagents": _ASSISTANT_V3_SUBAGENTS,
            "replacement_subagents": _ASSISTANT_V4_SUBAGENTS,
        }
    if session_seed and current < 4 <= target:
        # Compared again under the row lock: an edited description stays.
        repair["expected_description"] = _ASSISTANT_V3_DESCRIPTION
        repair["replacement_description"] = bundle["description"]
    if (
        spec["managed_key"] == "application-default-worker-seed"
        and current < 2 <= target
    ):
        # Compared again under the row lock: an edited description stays.
        repair["expected_description"] = _GENERAL_WORKER_V1_DESCRIPTION
        repair["replacement_description"] = bundle["description"]
    if (
        spec["managed_key"] == "application-default-worker-seed"
        and current < 3 <= target
        and bundle.get("prompts", {}).get("persona")
    ):
        # Compared again under the row lock: an edited persona stays.
        repair["expected_persona"] = _GENERAL_WORKER_V1_PERSONA
        repair["replacement_persona"] = bundle["prompts"]["persona"]
    updated = await db.upgrade_managed_expert_seed(
        managed_key=spec["managed_key"],
        seed_version=target,
        config_additions=additions,
        **repair,
    )
    if updated:
        logger.info(
            "Managed expert %s: seed %s -> %s, added config keys %s",
            spec["managed_key"],
            current,
            target,
            sorted(additions) or "none",
        )
    return updated


async def seed_managed_default_experts(db, config_dir: Path) -> dict[str, str]:
    """Insert missing managed experts and missing application pointers.

    Both operations are insert-only.  Existing expert content and an operator's
    current application pointer are preserved across restarts and upgrades.
    The one exception is :func:`upgrade_managed_seed`: a row behind the
    bundle's ``seed_version`` gains the top-level config keys it lacks. Only
    the exact managed v2/v3 assistant rosters, the exact shipped
    descriptions (General Worker v1, assistant v3) and the exact shipped
    General Worker v1 persona receive a conditional repair; operator
    variants and unrelated content remain unchanged.
    """
    seeded: dict[str, str] = {}
    for spec in MANAGED_SEEDS:
        bundle = load_seed_bundle(
            config_dir,
            directory=spec["directory"],
            expert_type=spec["expert_type"],
        )
        row, created = await db.upsert_managed_expert(
            managed_key=spec["managed_key"],
            seed_version=spec["seed_version"],
            **bundle,
        )
        if not created:
            row = (
                await upgrade_managed_seed(db, spec=spec, bundle=bundle, row=row) or row
            )
        if row["expert_type"] != spec["expert_type"]:
            raise RuntimeError(
                f"Managed expert {spec['managed_key']} has incompatible type "
                f"{row['expert_type']}"
            )
        await db.ensure_application_expert_default(
            expert_type=spec["expert_type"], expert_id=str(row["id"])
        )
        seeded[spec["expert_type"]] = str(row["id"])
    return seeded


async def personal_defaults_allowed(
    db,
    *,
    user_id: str,
    project_ids: list[str] | None = None,
    is_admin: bool = False,
) -> bool:
    if is_admin:
        return True
    grants = await resolve_grants_for(
        db, user_id=user_id, project_ids=list(project_ids or [])
    )
    return bool(grants.get("personal_default_experts", True))


async def validate_explicit_expert(
    db,
    *,
    expert_id: str,
    expert_type: ExpertType,
    user_id: str,
    project_ids: list[str] | None = None,
    is_admin: bool = False,
) -> ExpertSelection:
    row = await db.get_expert_visible_by_id(
        expert_id,
        user_id=user_id,
        project_ids=list(project_ids or []),
        is_admin=is_admin,
    )
    if not row:
        raise ExpertSelectionError("Expert is unavailable")
    if row["expert_type"] != expert_type:
        # Universal experts (U1, D4): every expert is usable in every role —
        # `resolve_config` re-roots the row's fragment onto the requested
        # role's overlay (a session expert dispatched as a worker gains the
        # phase loop's keys, and vice versa). The row's own `expert_type`
        # stays its primary role for listings and the default slots; an
        # explicit cross-role pick is logged, not refused. The default-slot
        # endpoints keep their own type checks (a slot is per role).
        logger.info(
            "Expert %s (%s, role %s) explicitly selected for the %s role; "
            "resolving on the %s overlay",
            row.get("name") or expert_id,
            expert_id,
            row["expert_type"],
            expert_type,
            expert_type,
        )
    return ExpertSelection(expert=row, source="explicit")


async def resolve_root_expert(
    db,
    *,
    expert_type: ExpertType,
    user_id: str,
    project_id: str | None = None,
    explicit_expert_id: str | None = None,
    is_admin: bool = False,
) -> ExpertSelection:
    """Resolve ``explicit > project > personal > application`` for a root."""
    project_ids = [project_id] if project_id else []
    if explicit_expert_id:
        explicit = await validate_explicit_expert(
            db,
            expert_id=explicit_expert_id,
            expert_type=expert_type,
            user_id=user_id,
            project_ids=project_ids,
            is_admin=is_admin,
        )
        project_override = None
        if project_id:
            link = await db.get_project_expert_link(
                project_id=project_id, expert_id=explicit_expert_id
            )
            if link:
                if link.get("project_manifest_composed"):
                    from orchestrator.services.manifest_projects import (
                        project_expert_for_execution,
                    )

                    frozen = await project_expert_for_execution(
                        db, project_id, explicit_expert_id, expert_type
                    )
                    if frozen:
                        explicit = ExpertSelection(expert=frozen, source="explicit")
                else:
                    project_override = _json_object(link.get("config_override")) or None
        return ExpertSelection(
            expert=explicit.expert,
            source="explicit",
            project_override=project_override,
        )

    if project_id:
        project = await db.get_project_default_expert(
            project_id=project_id, expert_type=expert_type
        )
        if project:
            return ExpertSelection(
                expert=project,
                source="project",
                project_override=(
                    None
                    if project.get("project_composed")
                    else _json_object(project.get("config_override")) or None
                ),
            )

    if await personal_defaults_allowed(
        db,
        user_id=user_id,
        project_ids=project_ids,
        is_admin=is_admin,
    ):
        personal = await db.get_user_expert_default(
            user_id=user_id, expert_type=expert_type
        )
        # The DB constraints guarantee type and the set endpoint guarantees
        # ownership.  Re-check ownership to fail closed if old/manual data exists.
        if personal and str(personal.get("owner_id")) == str(user_id):
            return ExpertSelection(expert=personal, source="user")

    application = await db.get_application_expert_default(expert_type)
    if application:
        return ExpertSelection(expert=application, source="application")
    raise DefaultExpertUnavailable(
        f"No application {expert_type} expert default is configured"
    )


__all__ = [
    "BASE_CONFIG_NAMES",
    "DefaultExpertUnavailable",
    "ExpertSelection",
    "ExpertSelectionError",
    "load_seed_bundle",
    "personal_defaults_allowed",
    "resolve_root_expert",
    "seed_managed_default_experts",
    "validate_explicit_expert",
]
