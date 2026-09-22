"""Catalogue reads, bundled resources and effective expert/skill projection.

The application owns the state and collaborators. This module neither constructs
runtime infrastructure nor resolves dependencies through the main application.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Literal

from fastapi import HTTPException
import yaml

from orchestrator.schemas.expert_catalog import (
    ExpertInfo,
    SkillInfo,
)
from orchestrator.services.catalogue_resources import project_settings_subsection
from orchestrator.services.config_overrides import deep_merge_dicts
from shared.runtime.core.loader import (
    ROOT_NAMES,
    canonical_config_name,
    chain_root,
    expert_phase_prompt_bodies,
    load_and_merge_config,
    load_role_base,
    normalize_llm_tiers,
    prune_ignored_keys,
    reroot_extends,
    resolve_config_path,
)
from shared.runtime.core.loader import INHERIT_MODEL
from shared.runtime.core.tool_policy import enumerate_only_members
from shared.runtime.core.srw_manifest_config import (
    SRW_HARNESS_ADAPTER,
    read_srw_config,
    srw_private_config,
    validate_srw_asset_name,
)

from orchestrator.services.expert_catalog_contracts import ExpertCatalogDependencies

logger = logging.getLogger(__name__)


def _private_layers(fragment: dict, layers: list[dict]) -> dict:
    result = fragment
    for layer in layers:
        result = deep_merge_dicts(result, layer)
    return result


def role_base_or_empty(role: str) -> dict[str, Any]:
    """The fully merged role base (``expert_base`` + the role overlay), or
    ``{}`` with a warning when the bundled files cannot be read — the same
    tolerance the old raw ``worker_base.yaml`` reads had. Never read a base
    file directly: an overlay alone is only the role's residue."""
    try:
        return load_role_base(role)
    except Exception as exc:
        logger.warning("Role base %r unavailable: %s", role, exc)
        return {}


def expert_info_from_dir(entry: Path, *, library: bool = False) -> ExpertInfo | None:
    """One ``ExpertInfo`` from ``<entry>/config.yaml``; ``None`` for a
    non-expert directory or an unreadable file (logged, never fatal).

    The role is the chain's ROOT (expert -> ... -> role overlay), not only
    the direct ``$extends``; a chain rooted straight on expert_base or one
    that cannot be followed lists as a worker, as before. ``tags`` is the
    YAML's ``tags`` ∪ {role tag} (U1 B.4): a bundled expert carries its
    chain root's role, a subagent-library entry carries ``subagent`` — the
    directory it lives in IS its authoring. Additive metadata the list
    filters read alongside ``expert_type``.
    """
    from shared.runtime.core.expert_resolution import with_role_tag

    config_path = entry / "config.yaml"
    if not entry.is_dir() or not config_path.exists():
        return None
    try:
        data = read_srw_config(config_path)

        root = chain_root(str(config_path)) or canonical_config_name(
            str(data.get("$extends") or "worker_base")
        )
        expert_type: Literal["worker", "session"] = (
            "session" if root == "session_base" else "worker"
        )

        description = str(data.get("description") or "").strip()
        # Summarize tools if no description
        if not description:
            tools = data.get("tools") or {}
            tool_categories = [k for k in tools if tools[k]]
            description = (
                f"Agent with {', '.join(tool_categories)} tools."
                if tool_categories
                else "Custom agent configuration."
            )

        return ExpertInfo(
            id=entry.name,
            display_name=data.get("display_name", entry.name.replace("_", " ").title()),
            description=description,
            icon=data.get("icon", "psychology"),
            color=data.get("color", "#cba6f7"),
            tags=with_role_tag(
                "subagent" if library else expert_type, data.get("tags")
            ),
            expert_type=expert_type,
        )
    except Exception as e:
        logger.warning(f"Failed to parse expert config {config_path}: {e}")
        return None


def expert_matches_type(
    type: str | None, expert_type: str, tags: list[str] | None, *, by_role: bool
) -> bool:
    """The one list filter (U1 B.4): ``tags ∪ {expert_type}``. A row lists
    under ``?type=X`` when its role is X or it carries the tag X, so a row is
    never hidden for lacking a tag and ``?type=subagent`` lists everything
    tagged for the subagent role. ``by_role=False`` (the subagent library)
    matches by tag only: those entries never appear in the default listing
    or under their fallback ``expert_type``."""
    if type is None:
        return by_role
    return (by_role and expert_type == type) or type in (tags or [])


def effective_models_from_layers(
    expert_llm: dict[str, Any] | None,
    account_default: str | None,
    system_default: str | None,
    expert_subagents: dict[str, Any] | None = None,
) -> dict[str, dict[str, Any]]:
    """Per-slot effective model + provenance for the create-form pickers.

    Mirrors the dispatch precedence (most-specific wins) so the UI can show
    the model the agent will actually run when the picker is left untouched.
    v1 layers (the project ``default_config_override`` layer is deferred — it
    is ~always null and would need project context threaded through the
    forms):

        expert pin  ->  account default_model  ->  system registry default

    ``expert_llm`` is the expert's OWN llm fragment (DB overlay or bundled
    leaf), NOT the merged config — a bundled, model-agnostic expert has ``{}``
    here and so resolves to the account/system default, exactly like dispatch
    (the bundled base's placeholder model is replaced by the default floor
    before the expert merges). ``expert_subagents`` is the fragment's own
    ``subagents`` block (the roster-wide ``subagents.llm.model`` is the
    "subagent model" picker since U1).

    Since U1 an expert has ONE model (``llm.model``): the per-phase tiers are
    gone, and a legacy fragment is read through the loader's compat mapping
    first, so a stored ``llm.strategic`` pin surfaces as ``model`` and a stored
    ``llm.subagent`` as the ``subagent`` slot. Returns
    ``{slot: {"model": str|None, "source": str}}`` for slots ``model`` (the
    expert's model), ``subagent`` (``subagents.llm.model`` when pinned to a
    real model, else ``model`` — ``inherit`` IS the parent's model) and
    ``session`` (= ``model``). The ``strategic`` / ``tactical`` aliases the
    cockpit read in between are gone (U1 WP6 switched every reader to
    ``model``). ``source`` is one of ``expert`` / ``account_default`` /
    ``system_default``.
    """
    fragment: dict[str, Any] = {"llm": dict(expert_llm or {})}
    if isinstance(expert_subagents, dict):
        fragment["subagents"] = expert_subagents
    fragment = normalize_llm_tiers(fragment, source="effective-models")
    llm = fragment.get("llm") or {}
    subagents = fragment.get("subagents")
    roster_llm = subagents.get("llm") if isinstance(subagents, dict) else None

    def _top() -> dict[str, Any]:
        if llm.get("model"):
            return {"model": llm["model"], "source": "expert"}
        if account_default:
            return {"model": account_default, "source": "account_default"}
        return {"model": system_default, "source": "system_default"}

    top = _top()
    subagent_pin = roster_llm.get("model") if isinstance(roster_llm, dict) else None
    subagent = (
        {"model": subagent_pin, "source": "expert"}
        if subagent_pin and subagent_pin != INHERIT_MODEL
        else dict(top)
    )
    return {
        "model": dict(top),
        "subagent": subagent,
        "session": dict(top),
    }


def validate_skill_frontmatter(frontmatter: dict[str, Any]) -> None:
    """Reject credential sections in SKILL.md frontmatter (reuses expert deny-scan)."""
    from shared.runtime.core.expert_resolution import hard_deny_scan

    offending = hard_deny_scan(frontmatter)
    if offending:
        raise HTTPException(
            status_code=422,
            detail="SKILL.md frontmatter may not set credential sections: "
            + ", ".join(sorted(offending)),
        )


def parse_skill_bundle(files: dict[str, str]) -> tuple[str, str, dict[str, str]]:
    """Validate paths, parse SKILL.md, deny-scan. Returns (name, description, files)."""
    from shared.runtime.core.skill_resolution import is_reserved_system_skill_name
    from shared.runtime.core.skill_format import (
        SkillFormatError,
        parse_skill_md,
        skill_identity,
        validate_skill_files,
    )

    try:
        validate_skill_files(files)
        fm, _body = parse_skill_md(files["SKILL.md"])
        name, description = skill_identity(fm)
    except SkillFormatError as e:
        raise HTTPException(status_code=422, detail=str(e)) from e
    if is_reserved_system_skill_name(name):
        raise HTTPException(
            status_code=422,
            detail=(
                f"Skill name '{name}' is reserved for a managed SRW product "
                "artifact; use a distinct name for extensions"
            ),
        )
    validate_skill_frontmatter(fm)
    return name, description, files


def skill_row_to_meta(row: dict[str, Any]) -> dict[str, Any]:
    """Project a skills row into the catalog metadata shape."""
    return {
        "id": str(row["id"]),
        "name": row["name"],
        "display_name": row["display_name"],
        "description": row.get("description") or "",
        "icon": row["icon"],
        "color": row["color"],
        "tags": row.get("tags") or [],
        "version": row.get("version"),
        "owner_id": str(row["owner_id"]) if row.get("owner_id") else None,
    }


def skill_row_visible(row: dict[str, Any], user: dict[str, Any]) -> bool:
    """Whether ``user`` may read a DB skill row by id.

    The listing's rule (``list_skills_visible``: owned or global) plus admins,
    who may already edit and delete any row. Skills have no project junction
    (0031), so project membership grants nothing here.
    """
    return (
        bool(user.get("is_admin"))
        or row.get("is_global") is True
        or str(row.get("owner_id") or "") == str(user["id"])
    )


def db_expert_to_bundle_src(row: dict[str, Any]) -> dict[str, Any]:
    """Normalize a DB expert row into the bundle-source shape (JSONB str-tolerant)."""
    if "harness_adapter" in row and row["harness_adapter"] != SRW_HARNESS_ADAPTER:
        raise HTTPException(
            409, "Export or copy this harness through its Expert manifest."
        )
    cfg = row.get("config") or {}
    if isinstance(cfg, str):
        cfg = json.loads(cfg)
    prm = row.get("prompts") or {}
    if isinstance(prm, str):
        prm = json.loads(prm)
    return {
        "name": row["name"],
        "display_name": row["display_name"],
        "expert_type": row["expert_type"],
        "description": row.get("description"),
        "icon": row["icon"],
        "color": row["color"],
        "tags": row.get("tags") or [],
        "config": cfg,
        "prompts": prm,
        **(
            {"harness_config_layers": row["harness_config_layers"]}
            if row.get("harness_config_layers")
            else {}
        ),
        **{
            key: row[key]
            for key in ("harness_config_name", "harness_asset_name")
            if row.get(key)
        },
    }


class ExpertCatalogService:
    def __init__(self, deps: ExpertCatalogDependencies) -> None:
        self.deps = deps
        self.store = deps.store
        self.state = deps.state

    async def bundled_manifest(self, expert_id: str) -> dict[str, Any] | None:
        """Current saved revision for a bundled selector, after bootstrap."""
        if self.deps.manifests is None:
            return None
        name = (
            "subagent-" + expert_id.removeprefix("subagents/")
            if expert_id.startswith("subagents/")
            else expert_id
        )
        row = await self.deps.manifests.by_name(
            "Expert", {"kind": "Catalog", "name": "shared"}, name
        )
        if row is None:
            raise HTTPException(404, "The bundled Expert resource is unavailable.")
        return row["document"]

    def scan_experts(self) -> list[ExpertInfo]:
        """Scan config/experts/ for expert configurations.

        Only ``config/experts/*/config.yaml`` is listed — the chain roots
        (``expert_base.yaml``, ``overlays/*.yaml``) are bases, not experts, the
        public base ids (``worker_base`` / ``session_base``) are served by
        ``_load_expert_detail`` by name, and the subagent library
        (``config/subagents/*``) is a separate scan (``_scan_subagent_library``)
        that only ever lists by tag. Each entry's ``tags`` carries its role.
        """
        experts_dir = self.deps.get_config_dir() / "experts"
        if not experts_dir.is_dir():
            return []
        experts: list[ExpertInfo] = []
        for entry in sorted(experts_dir.iterdir()):
            info = expert_info_from_dir(entry)
            if info is not None:
                experts.append(info)
        return experts

    def scan_subagent_library(self) -> list[ExpertInfo]:
        """Scan config/subagents/ — the shared library of small experts a roster
        references (``{$ref: subagents/<name>}``; universal_experts_and_subagents.md
        §1.1). Same schema as an expert, same ``ExpertInfo``; ``tags`` always
        carries ``subagent`` (the directory is the authoring), ``expert_type`` is
        the chain root's role (``worker`` for a chain on ``expert_base``) and is
        never what lists them: ``list_experts`` includes a library entry only
        when the requested ``type`` matches one of its tags.
        """
        library_dir = self.deps.get_config_dir() / "subagents"
        if not library_dir.is_dir():
            return []
        entries: list[ExpertInfo] = []
        for entry in sorted(library_dir.iterdir()):
            info = expert_info_from_dir(entry, library=True)
            if info is not None:
                entries.append(info)
        return entries

    def listed_expert(self, expert_id: str) -> ExpertInfo | None:
        """The cached listing entry for a bundled expert id, else a subagent-
        library id — bundled wins on a name clash, exactly like a bare ``$ref``.
        ``None`` for anything else (DB rows are looked up by UUID elsewhere)."""
        if self.state.experts is None:
            self.state.experts = self.scan_experts()
        info = next((e for e in self.state.experts if e.id == expert_id), None)
        if info is not None:
            return info
        if self.state.library is None:
            self.state.library = self.scan_subagent_library()
        return next((e for e in self.state.library if e.id == expert_id), None)

    async def list_experts(
        self, type: str | None = None, *, user: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """List experts: bundled (disk) + DB rows visible to the caller (owned +
        project-linked + global), each tagged with ``source``. **P4e** — approved
        users only.

        ``type`` narrows by ROLE OR TAG (``expert_type == type or type in tags``,
        U1 B.4) — ``?type=worker`` / ``?type=session`` list as before plus any
        row tagged for that role; ``?type=subagent`` lists the subagent library
        (``config/subagents/*``, ``source: library``) and every expert tagged
        ``subagent``. Without ``type`` the listing is unchanged: bundled experts
        + DB rows, never the library. A bundled expert's role is inferred from
        its chain root; every entry's ``tags`` includes its role.
        """
        if self.state.experts is None:
            self.state.experts = self.scan_experts()
        if self.state.library is None:
            self.state.library = self.scan_subagent_library()
        # ``name`` is the slug callers use to reference an expert by name (e.g. the
        # project loop's role_sequence, a roster ``$ref``). For bundled experts the
        # id IS the slug; for library entries it is the unambiguous
        # ``subagents/<id>`` spelling; for DB rows it's the separate name column
        # (id is a UUID).
        result = [
            {
                **e.model_dump(),
                "source": "bundled",
                "storage_kind": "bundled",
                "name": e.id,
            }
            for e in self.state.experts
            if expert_matches_type(type, e.expert_type, e.tags, by_role=True)
        ]
        result += [
            {
                **e.model_dump(),
                "source": "library",
                "storage_kind": "library",
                "name": f"subagents/{e.id}",
            }
            for e in self.state.library
            if expert_matches_type(type, e.expert_type, e.tags, by_role=False)
        ]
        if self.deps.experts_enabled():
            visible = await self.deps.visible_project_ids(user, self.store)
            pids = [] if visible == "all" else [str(p) for p in visible]
            # Fetched without the SQL role filter: the filter reads tags too, and
            # a row tagged for another role must list under that role as well.
            rows = await self.store.list_experts_visible(
                user_id=str(user["id"]), project_ids=pids, expert_type=None
            )
            rows = [
                r
                for r in rows
                if expert_matches_type(
                    type, r["expert_type"], list(r.get("tags") or []), by_role=True
                )
            ]
            managed_names = {r["name"] for r in rows if r.get("managed_key")}
            if managed_names:
                # Assistant/General Worker remain on disk as bootstrap templates,
                # but their managed DB copies are the selectable runtime entries.
                result = [r for r in result if r.get("name") not in managed_names]
            result += [
                {
                    "id": str(r["id"]),
                    "name": r["name"],
                    "display_name": r["display_name"],
                    "description": r.get("description") or "",
                    "icon": r["icon"],
                    "color": r["color"],
                    "tags": r.get("tags") or [],
                    "expert_type": r["expert_type"],
                    "source": (
                        "managed"
                        if r.get("managed_key")
                        else ("global" if r["is_global"] else "user")
                    ),
                    "storage_kind": "db",
                    "managed_key": r.get("managed_key"),
                    "owner_id": str(r["owner_id"]) if r.get("owner_id") else None,
                    "harness_adapter": r.get("harness_adapter", SRW_HARNESS_ADAPTER),
                    "manifest_uid": r.get("manifest_uid"),
                }
                for r in rows
            ]
        if self.deps.manifests is not None:
            resources = await self.deps.manifests.list_scope(
                {"kind": "Catalog", "name": "shared"}, kind="Expert"
            )
            by_name = {r["document"]["metadata"]["name"]: r for r in resources}
            for item in result:
                resource = (
                    by_name.get(
                        "subagent-" + item["name"].removeprefix("subagents/")
                        if item["storage_kind"] == "library"
                        else item["name"]
                    )
                    if item["storage_kind"] in {"bundled", "library"}
                    else None
                )
                if resource:
                    document = resource["document"]
                    annotations = document["metadata"].get("annotations", {})
                    for field in (
                        "display_name",
                        "description",
                        "icon",
                        "color",
                        "expert_type",
                    ):
                        item[field] = annotations.get(
                            "srw.io/" + field.replace("_", "-"), item[field]
                        )
                    item["tags"] = document["metadata"].get("tags", [])
                    item["harness_adapter"] = document["spec"]["runtime"].get("adapter")
                    item["manifest_uid"] = str(resource["id"])
            bound = {
                item["manifest_uid"]
                for item in result
                if item["storage_kind"] == "db" and item.get("manifest_uid")
            }
            result = [
                item
                for item in result
                if item["storage_kind"] == "db"
                or (item.get("manifest_uid") and item["manifest_uid"] not in bound)
            ]
        return result

    async def reload_experts(self) -> dict[str, Any]:
        """Force reload of expert configurations cache. **Admin only** (P4d) —
        reloads expert YAML from disk."""
        self.state.experts = self.scan_experts()
        self.state.library = self.scan_subagent_library()
        return {"status": "reloaded", "count": len(self.state.experts)}

    async def compute_expert_effective_models(
        self,
        expert_llm: dict[str, Any] | None,
        user_id: str | None,
        expert_subagents: dict[str, Any] | None = None,
    ) -> dict[str, dict[str, Any]]:
        """Combine the expert's own pins with the user's account default_model and
        the system registry chat default (``resolve_default_for_capability`` — the
        same source dispatch uses)."""
        account_default = None
        if user_id:
            settings = await self.store.get_user_settings(str(user_id)) or {}
            account_default = settings.get("default_model")
        system_default = await self.store.resolve_default_for_capability("chat")
        return effective_models_from_layers(
            expert_llm, account_default, system_default, expert_subagents
        )

    async def load_expert_detail(
        self,
        expert_id: str,
        *,
        user_id: str | None = None,
        defaults_type: Literal["worker", "session"] | None = None,
        include_account_defaults: bool = False,
        role: str | None = None,
    ) -> dict[str, Any]:
        """Load full expert detail: merged config + instructions content. DB-backed
        experts (UUID) resolve their fragment onto the expert_type base; bundled
        experts resolve from disk as before; a subagent-library id
        (``config/subagents/<name>``) resolves on the subagent overlay.

        ``role`` (``worker`` / ``session`` / ``subagent``) resolves the expert in
        THAT role instead of its own — the same re-rooting ``resolve_config``
        applies when a session expert is dispatched as a worker (U1 D4), so a
        cross-role picker previews the config the job or session will actually
        run. The served ``expert_type`` stays the expert's own role (its identity
        and default slot); ``resolved_role`` names the role the config was
        resolved for.

        When ``user_id`` is provided, attaches ``effective_models`` (per-slot model +
        provenance) so the create-form picker can show what will actually run if left
        untouched — see Layer 3 in
        knowledge-base/knowledge/issues/loop_ran_codex_spark_not_selected_model_then_hung_on_cooldown.md.

        ``include_account_defaults`` inserts the caller's account layer between the
        framework base and the expert fragment, exactly where ``resolve_config``
        puts ``base_defaults``. **Create forms must set it**: without it the form
        resolves a different config than the one create/dispatch will build, and any
        control keyed off a resolved value silently disagrees with the server. That
        is not hypothetical — a New Session form reading ``workspace.backend`` as
        the base's ``sandbox`` (instead of the account's ``virtual``) left
        clone-based repository connectors selectable, and every create 400'd on the
        lite-backend rule. It is deliberately OFF by default so the expert *editor*
        keeps diffing against the pure framework baseline; folding personal
        preferences into that baseline would let them be saved into a shared expert.
        """
        if self.deps.experts_enabled() and self.deps.looks_like_uuid(expert_id):
            row = await self.store.get_expert_by_id(expert_id)
            if not row:
                return {}
            if (
                "harness_adapter" in row
                and row["harness_adapter"] != SRW_HARNESS_ADAPTER
            ):
                return {
                    "id": str(row["id"]),
                    "manifest": row["manifest"],
                    "harness_adapter": row["harness_adapter"],
                    "config": {},
                    "settings_matrix": {},
                    "effective_models": None,
                    "instructions": None,
                    "enumerate_only": {},
                }
            # The role base, fully merged (expert_base + overlay) — the same base
            # `resolve_config` puts under a DB fragment at dispatch: the row's own
            # role, or the requested one for a cross-role preview.
            role_used = role or str(row["expert_type"])
            base = role_base_or_empty(role_used)
            base_name = row.get("harness_config_name")
            if base_name and canonical_config_name(base_name) not in ROOT_NAMES:
                from orchestrator.services.config_overrides import validated_config_name

                path, _ = resolve_config_path(validated_config_name(base_name))
                base = load_and_merge_config(path, role=role_used)
            cfg = row.get("config") or {}
            if isinstance(cfg, str):
                cfg = json.loads(cfg)
            account_layer = (
                await self.deps.account_defaults_layer(user_id, role_used)
                if include_account_defaults
                else {}
            )
            from shared.runtime.core.expert_resolution import build_expert_config

            merged, _ = build_expert_config(deep_merge_dicts(base, account_layer), row)
            from shared.runtime.core.workspace_selection import (
                bind_execution_workspace,
                execution_workspace_config,
            )

            if role_used in ("session", "worker"):
                merged = bind_execution_workspace(
                    merged, execution_workspace_config(account_layer, role=role_used)
                )
            merged = prune_ignored_keys(merged)
            for key in ("connections", "$ignore_keys"):
                merged.pop(key, None)
            prompts = row.get("prompts") or {}
            if isinstance(prompts, str):
                prompts = json.loads(prompts)
            effective_leaf = _private_layers(cfg, row.get("harness_config_layers", []))
            effective = (
                await self.compute_expert_effective_models(
                    effective_leaf.get("llm") or {},
                    user_id,
                    effective_leaf.get("subagents"),
                )
                if user_id
                else None
            )
            # Keep DB-backed detail responses at parity with bundled experts: the
            # forms resolve model-family defaults client-side from the raw matrix.
            # `defaults_tools` used to ride here too — the mode base's tool lists,
            # so the forms could turn an expert-disabled category back on. It is
            # gone: the base ships `[]` for every category worth re-enabling, so
            # the payload it produced was empty and the re-enable emitted nothing.
            # `enumerate_only` replaces it and is a different kind of thing: not a
            # copy of a config layer that can go stale, but the registry's own
            # answer to "what must a caller write to turn this category on", for
            # the one category (`shell`) that refuses `true`. Without it a form
            # with no resolved read can only send `true`, which 400s naming a rule
            # the user has no way to satisfy from the form.
            raw_matrix = self.deps.load_settings_matrix(self.deps.get_config_dir())
            asset_prompts = {}
            if row.get("harness_asset_name"):
                _, asset_dir = resolve_config_path(
                    validate_srw_asset_name(row["harness_asset_name"])
                )
                if not asset_dir:
                    raise HTTPException(
                        422, "The SRW Expert asset directory is unavailable."
                    )
                asset_dir = Path(asset_dir)
                asset_matrix = asset_dir / "model_config_matrix.yaml"
                if asset_matrix.exists():
                    raw_matrix = deep_merge_dicts(
                        raw_matrix,
                        project_settings_subsection(
                            yaml.safe_load(asset_matrix.read_text()) or {}
                        ),
                    )
                for key, filename in (
                    ("instructions", "instructions.md"),
                    ("persona", "persona.txt"),
                ):
                    path = asset_dir / filename
                    if path.is_file():
                        asset_prompts[key] = path.read_text(encoding="utf-8")
            asset_prompts.update(prompts)
            return {
                "id": str(row["id"]),
                "display_name": row["display_name"],
                "description": row.get("description") or "",
                "icon": row["icon"],
                "color": row["color"],
                "tags": row.get("tags") or [],
                "expert_type": row["expert_type"],
                "source": (
                    "managed"
                    if row.get("managed_key")
                    else ("global" if row.get("is_global") else "user")
                ),
                "storage_kind": "db",
                "managed_key": row.get("managed_key"),
                "workspace_preference": (row.get("manifest") or {})
                .get("spec", {})
                .get("workspacePreference"),
                "manifest": row.get("manifest"),
                "harness_adapter": row.get("harness_adapter", SRW_HARNESS_ADAPTER),
                "config": merged,
                "instructions": asset_prompts.get("instructions"),
                "persona": asset_prompts.get("persona"),
                "enumerate_only": enumerate_only_members(),
                "settings_matrix": raw_matrix,
                "effective_models": effective,
                "resolved_role": role_used,
            }
        config_dir = self.deps.get_config_dir()
        private = {}
        saved_manifest = None
        account_layer = {}

        # Load expert config
        if expert_id in {
            "default",
            "defaults",
            "worker_base",
            "persistent_default",
            "persistent_defaults",
            "session_base",
        }:
            inferred_type = (
                "session"
                if canonical_config_name(expert_id) == "session_base"
                else defaults_type
            )
            # The public base ids resolve to the role base (expert_base + overlay);
            # `agent_id` stays `worker_base` / `session_base` because the overlay
            # declares it, so the served detail is unchanged by the split. An
            # explicit `role` wins over the id (the roots are one thing in
            # different roles — the same rule `resolve_config` applies).
            role_used = role or ("session" if inferred_type == "session" else "worker")
            defaults = role_base_or_empty(role_used)
            # `defaults` stays the pristine framework base; the account layer is
            # merged on top only for `merged`.
            account_layer = (
                await self.deps.account_defaults_layer(user_id, role_used)
                if include_account_defaults
                else {}
            )
            merged = deep_merge_dicts(defaults, account_layer)
            expert_config_dir = config_dir
            # The "defaults" virtual expert is model-agnostic — no expert-level model
            # pin, so its effective model is the account/system default.
            expert_llm_leaf: dict[str, Any] = {}
            expert_subagents: dict[str, Any] | None = None
        else:
            expert_dir = config_dir / "experts" / expert_id
            library_entry = False
            if not (expert_dir / "config.yaml").exists():
                # The subagent library (config/subagents/<name>): a roster target,
                # served by its bare name when no bundled expert shadows it — the
                # precedence a bare `$ref` uses. Its natural role is subagent.
                expert_dir = config_dir / "subagents" / expert_id
                library_entry = True
            config_path = expert_dir / "config.yaml"
            if not expert_dir.is_dir() or not config_path.exists():
                return {}
            saved_manifest = await self.bundled_manifest(
                f"subagents/{expert_id}" if library_entry else expert_id
            )
            if saved_manifest is None:
                installed_document = yaml.safe_load(config_path.read_text()) or {}
                if installed_document.get("kind") == "Expert":
                    saved_manifest = installed_document
            if (
                saved_manifest
                and saved_manifest["spec"]["runtime"].get("adapter")
                != SRW_HARNESS_ADAPTER
            ):
                return {
                    "manifest": saved_manifest,
                    "harness_adapter": None,
                    "config": {},
                    "settings_matrix": {},
                    "effective_models": None,
                    "instructions": None,
                    "enumerate_only": {},
                }
            private = srw_private_config(saved_manifest) if saved_manifest else {}
            expert_data = (
                private.get("config", {})
                if saved_manifest
                else read_srw_config(config_path)
            )

            # Resolve $extends to the expert's parent chain — a role overlay on
            # expert_base for every bundled expert (another expert's chain is
            # followed the same way). The chain ROOT names the expert's own role;
            # an unknown parent falls back to the worker base, as before. For a
            # requested `role` (or a library entry, whose role is subagent) the
            # link is re-rooted onto that role's overlay exactly as
            # `resolve_config` re-roots a bundled leaf at dispatch.
            root = chain_root(str(config_path))
            own_role = "session" if root == "session_base" else "worker"
            reroot_role = role or ("subagent" if library_entry else None)
            role_used = reroot_role or own_role
            extends_name = str(
                private.get("config_name") or expert_data.pop("$extends", "worker_base")
            )
            expert_data.pop("$extends", None)
            parent_name, parent_role = reroot_extends(extends_name, reroot_role)
            parent_path, _ = resolve_config_path(parent_name)
            if Path(parent_path).is_file():
                defaults = load_and_merge_config(parent_path, role=parent_role)
            else:
                defaults = role_base_or_empty(role_used)

            # Account layer sits above the framework base and below the bundled
            # expert leaf — the same slot `resolve_config` gives `base_defaults`.
            account_layer = (
                (await self.deps.account_defaults_layer(user_id, role_used))
                if include_account_defaults
                else {}
            )
            base_layer = deep_merge_dicts(defaults, account_layer)
            from shared.runtime.core.expert_resolution import build_expert_config

            merged, _ = build_expert_config(
                base_layer,
                {
                    "config": expert_data,
                    "harness_config_layers": private.get("layers", []),
                },
            )
            merged = prune_ignored_keys(merged)
            expert_config_dir = expert_dir
            if private.get("asset_name"):
                _, selected_asset_dir = resolve_config_path(private["asset_name"])
                if not selected_asset_dir:
                    raise HTTPException(
                        422, "The SRW Expert asset directory is unavailable."
                    )
                expert_config_dir = Path(selected_asset_dir)
            # The expert's OWN llm fragment (leaf, pre-merge) — a model-agnostic
            # bundled expert has `llm: {}` here and resolves to the default floor.
            effective_leaf = _private_layers(expert_data, private.get("layers", []))
            expert_llm_leaf = effective_leaf.get("llm") or {}
            expert_subagents = effective_leaf.get("subagents")

        from shared.runtime.core.workspace_selection import (
            bind_execution_workspace,
            execution_workspace_config,
        )

        # Framework presets preview the same execution defaults as authored
        # Experts. Their private base no longer carries a workspace backend.
        if role_used in ("session", "worker"):
            merged = bind_execution_workspace(
                merged, execution_workspace_config(account_layer, role=role_used)
            )

        # Load the raw settings_matrix for the client to resolve per-model defaults.
        # Do NOT apply it to merged — the client resolves based on the user's model selection.
        raw_matrix = self.deps.load_settings_matrix(config_dir)
        if expert_config_dir and expert_config_dir != config_dir:
            expert_matrix_path = expert_config_dir / "model_config_matrix.yaml"
            if expert_matrix_path.exists():
                with open(expert_matrix_path) as f:
                    expert_parsed = yaml.safe_load(f) or {}
                expert_settings = project_settings_subsection(expert_parsed)
                raw_matrix = deep_merge_dicts(raw_matrix, expert_settings)

        # Load instructions content
        instructions_content = None
        # Check for expert-specific instructions.md first
        instr_path = expert_config_dir / "instructions.md"
        if (
            expert_id
            not in {
                "default",
                "defaults",
                "worker_base",
                "persistent_default",
                "persistent_defaults",
                "session_base",
            }
            and instr_path.exists()
        ):
            instructions_content = instr_path.read_text(encoding="utf-8")
        else:
            # Fall back to template referenced in config
            template_name = merged.get("workspace", {}).get(
                "instructions_template", "instructions.md"
            )
            template_path = config_dir / "prompts" / template_name
            if template_path.exists():
                instructions_content = template_path.read_text(encoding="utf-8")
        if "instructions" in private.get("prompts", {}):
            instructions_content = private["prompts"]["instructions"]

        # Remove internal/sensitive keys from merged config
        for key in ("$extends", "$ignore_keys", "connections"):
            merged.pop(key, None)

        effective = (
            await self.compute_expert_effective_models(
                expert_llm_leaf, user_id, expert_subagents
            )
            if user_id
            else None
        )
        return {
            "workspace_preference": (saved_manifest or {})
            .get("spec", {})
            .get("workspacePreference"),
            "config": merged,
            "instructions": instructions_content,
            "enumerate_only": enumerate_only_members(),
            "settings_matrix": raw_matrix,
            "effective_models": effective,
            "resolved_role": role_used,
            **(
                {"persona": private["prompts"]["persona"]}
                if "persona" in private.get("prompts", {})
                else {}
            ),
        }

    async def get_expert(
        self,
        expert_id: str,
        type: Literal["worker", "session"] | None = None,
        account_defaults: bool = False,
        role: Literal["worker", "session", "subagent"] | None = None,
        *,
        user: dict[str, Any],
    ) -> dict[str, Any]:
        """Get full expert detail including merged config and instructions content.

        **P4e** — gated to approved users (shared catalog metadata, not per-user).

        Returns the expert's configuration (merged with defaults) and the raw
        instructions.md content, enabling the cockpit to pre-populate the job
        creation form.

        ``account_defaults=true`` folds the caller's account fallback layer into
        ``config`` at the precedence ``resolve_config`` uses. The New Session / New
        Job forms pass it so what they render is what create/dispatch will resolve;
        the expert editor must NOT, or a personal preference could be saved into a
        shared expert. See ``_load_expert_detail``.

        ``role`` resolves the expert in that role (a cross-role picker: a session
        expert previewed as the worker a job will run) — see ``_load_expert_detail``.
        ``type`` only selects the base behind the public base ids (``defaults`` /
        ``worker_base`` / ``session_base``); ``role`` wins over it when both are
        given. A subagent-library id (``config/subagents/<name>``, listed under
        ``?type=subagent``) is served here too, on the subagent overlay by default.
        """
        _uid = str(user["id"])

        # DB-backed expert (UUID): the detail payload is self-contained.
        if self.deps.experts_enabled() and self.deps.looks_like_uuid(expert_id):
            visible = await self.deps.visible_project_ids(user, self.store)
            row = await self.store.get_expert_visible_by_id(
                expert_id,
                user_id=_uid,
                project_ids=[] if visible == "all" else [str(p) for p in visible],
                is_admin=bool(user.get("is_admin")),
            )
            detail = (
                await self.load_expert_detail(
                    expert_id,
                    user_id=_uid,
                    include_account_defaults=account_defaults,
                    role=role,
                )
                if row
                else {}
            )
            if not detail:
                raise HTTPException(
                    status_code=404, detail=f"Expert not found: {expert_id}"
                )
            return detail

        if expert_id in {
            "default",
            "defaults",
            "worker_base",
            "persistent_default",
            "persistent_defaults",
            "session_base",
        }:
            # "defaults" is a virtual expert representing the framework base for
            # the requested agent type. Worker remains the backward-compatible
            # default; session creation explicitly requests session_base.
            detail = await self.load_expert_detail(
                expert_id,
                user_id=_uid,
                defaults_type=type,
                include_account_defaults=account_defaults,
                role=role,
            )
            if not detail:
                raise HTTPException(status_code=404, detail="Defaults config not found")
            return detail

        # Verify the bundled expert (or subagent-library entry) exists.
        expert_info = self.listed_expert(expert_id)
        if not expert_info:
            raise HTTPException(
                status_code=404, detail=f"Expert not found: {expert_id}"
            )

        detail = await self.load_expert_detail(
            expert_id,
            user_id=_uid,
            include_account_defaults=account_defaults,
            role=role,
        )
        if not detail:
            raise HTTPException(
                status_code=404, detail=f"Expert config not found: {expert_id}"
            )

        return {
            **expert_info.model_dump(),
            **detail,
        }

    def require_experts_db(self) -> None:
        """The DB-experts feature is fully behind EXPERTS_DB_ENABLED."""
        if not self.deps.experts_enabled():
            raise HTTPException(
                status_code=404, detail="DB-backed experts are not enabled"
            )

    def validate_expert_fragment(self, config: dict[str, Any]) -> dict[str, Any]:
        """Gate an expert's authored fragment; return it in canonical form.

        Two independent checks, and the second was missing for the whole of this
        feature's life:

        1. **Credentials** — reject credential sections (decision 10, hard-deny),
           422 as before.
        2. **The tools vocabulary** — the same
           :func:`~src.core.tool_policy.validate_tool_override_fragment` every other
           write boundary runs, 400 as everywhere else. An expert's ``config`` is an
           authored config layer merged under every job and session it drives, and
           the credential scan cannot see a *cross-category smuggle*:
           ``tools.canvas: ["run_command"]`` was storable by any approved user,
           invisible to the grants PDP (which keys off the category name) and bound
           as shell by ``load_tools`` (which regroups by *registry* category).

        It also closes a quieter one: a stored fragment is normalised on the way OUT
        (``expert_resolution.build_expert_config``), so a shape
        ``normalize_tool_policy`` refuses — ``tools.shell: true`` — was storable and
        then made the expert unresolvable. Refusing here turns a later resolve
        failure into an immediate 400.

        3. **The roster** (U1 WP4) — a ``subagents`` block is checked the way the
           resolver will read it: the shape, every ``$ref`` (a bundled / library
           ref must exist on disk and its ``$extends`` chain must be sane; a DB
           ref is a UUID whose visibility to the author is the async half,
           :func:`_require_visible_roster_refs`) — 422, an authoring error like
           the credential scan — and each entry's ``tools`` through the same
           vocabulary gate as the top level (400). Dispatch never fails a job
           over a roster; the save is where a broken one is refused.

        Returns the fragment with ``tools`` normalised to ``list[str]`` (at the
        top level and in every roster entry), so callers persist the canonical
        form and the save-time PDP (which reads ``_truthy(tools.get(...))``, and
        gets ``{}`` / ``{only: []}`` backwards) only ever sees a list.

        4. **Legacy delegation** (U3 WP4) — a fragment re-submitted from a row
           authored before U3 may still grant ``spawn_subagent`` /
           ``delegate_work`` (mapped to ``delegate_agent`` by the shared tools
           gate) and carry ``delegation.mode`` / ``.light`` / the heavy-path
           timeouts (dropped by ``normalize_delegation_block``); the row is
           persisted canonical, so the managed seed rows never 422 on their next
           edit.
        """
        from shared.runtime.core.expert_resolution import hard_deny_scan
        from shared.runtime.core.loader import normalize_delegation_block
        from shared.runtime.core.subagent_roster import (
            RosterResolutionError,
            validate_roster_fragment,
        )

        offending = hard_deny_scan(config)
        if offending:
            raise HTTPException(
                status_code=422,
                detail="config may not set credential sections: "
                + ", ".join(sorted(offending)),
            )
        validated = (
            normalize_delegation_block(
                self.deps.with_validated_tool_overrides(config), source="expert-save"
            )
            or {}
        )
        subagents = validated.get("subagents")
        if subagents is None:
            return validated
        try:
            validate_roster_fragment(subagents)
        except RosterResolutionError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        if "roster" not in subagents:
            return validated
        canonical_roster: dict[str, Any] = {}
        for name, entry in (subagents.get("roster") or {}).items():
            try:
                canonical_roster[str(name)] = self.deps.with_validated_tool_overrides(
                    entry
                )
            except HTTPException as exc:
                raise HTTPException(
                    status_code=exc.status_code,
                    detail=f"subagents.roster.{name}: {exc.detail}",
                ) from exc
        return {**validated, "subagents": {**subagents, "roster": canonical_roster}}

    async def require_visible_roster_refs(
        self, config: dict[str, Any] | None, *, user: dict[str, Any]
    ) -> None:
        """The async half of the roster ``$ref`` check (U1 B.3): every DB expert
        a fragment's ``subagents.roster`` names must be visible to the AUTHOR
        (owned, global, or linked to one of the author's projects) — 422
        otherwise. A ref that passes here is materialised at dispatch by id
        (``_prefetch_roster_refs``); one the author cannot see is refused at
        save rather than silently dropped later. Runs on create / update /
        import (the authored writes); no database call when the roster names no
        DB ref."""
        from shared.runtime.core.subagent_roster import collect_roster_db_refs

        refs = collect_roster_db_refs(config or {})
        if not refs:
            return
        visible = await self.deps.visible_project_ids(user, self.store)
        pids = [] if visible == "all" else [str(p) for p in visible]
        for ref in sorted(refs):
            row = await self.store.get_expert_visible_by_id(
                ref,
                user_id=str(user["id"]),
                project_ids=pids,
                is_admin=bool(user.get("is_admin")),
            )
            if not row:
                raise HTTPException(
                    status_code=422,
                    detail=(
                        f"subagents.roster: $ref {ref!r} is not an expert visible to "
                        "you (own it, or select a global / project-linked expert)"
                    ),
                )

    def require_skills_db(self) -> None:
        """The DB-skills feature is fully behind SKILLS_DB_ENABLED."""
        if not self.deps.skills_enabled():
            raise HTTPException(
                status_code=404, detail="DB-backed skills are not enabled"
            )

    def scan_skills(self) -> list[SkillInfo]:
        """Scan config/skills/<name>/SKILL.md for bundled skills.

        A skill whose frontmatter says ``catalog: hidden`` (the worker's phase
        skills, U2) is not a catalog entry: it never enters the model-invoked
        menu, a session's skill list or the cockpit's bundled list. It still
        reaches an agent through a deterministic ``instruction_files`` binding
        (frozen by ``serialize_resolved_config`` straight from disk) and stays
        readable with ``use_skill`` once materialised.
        """
        from shared.runtime.core.skill_format import (
            SkillFormatError,
            is_catalog_hidden,
            parse_skill_md,
            skill_identity,
        )

        skills_dir = self.deps.get_config_dir() / "skills"
        skills: list[SkillInfo] = []
        if not skills_dir.is_dir():
            return skills
        for entry in sorted(skills_dir.iterdir()):
            skill_md = entry / "SKILL.md"
            if not entry.is_dir() or not skill_md.exists():
                continue
            try:
                fm, _ = parse_skill_md(skill_md.read_text(encoding="utf-8"))
                if is_catalog_hidden(fm):
                    continue
                name, description = skill_identity(fm)
                skills.append(
                    SkillInfo(
                        id=entry.name,
                        name=name,
                        display_name=fm.get(
                            "display_name", name.replace("-", " ").title()
                        ),
                        description=description,
                        icon=fm.get("icon", "extension"),
                        color=fm.get("color", "#6B7280"),
                        tags=fm.get("tags", []),
                    )
                )
            except (SkillFormatError, OSError, ValueError) as e:
                logger.warning(f"Failed to parse bundled skill {skill_md}: {e}")
        return skills

    def bundled_skill_bundle(self, skill_name: str) -> dict[str, Any] | None:
        """Read a bundled skill's full directory into a metadata + files dict."""
        from shared.runtime.core.skill_format import (
            SkillFormatError,
            parse_skill_md,
            skill_dir_under,
            skill_identity,
            validate_skill_path,
        )

        # ``skill_name`` is a route's non-UUID skill id: a name that is no
        # skill slug is simply not a bundled skill.
        try:
            skill_dir = skill_dir_under(
                self.deps.get_config_dir() / "skills", skill_name
            )
        except SkillFormatError:
            return None
        skill_md = skill_dir / "SKILL.md"
        if not skill_dir.is_dir() or not skill_md.exists():
            return None
        files: dict[str, str] = {}
        for fp in sorted(skill_dir.rglob("*")):
            if not fp.is_file():
                continue
            rel = str(fp.relative_to(skill_dir))
            try:
                validate_skill_path(rel)
                files[rel] = fp.read_text(encoding="utf-8")
            except (ValueError, UnicodeDecodeError):
                continue
        fm, _ = parse_skill_md(files["SKILL.md"])
        name, description = skill_identity(fm)
        return {
            "id": skill_name,
            "name": name,
            "display_name": fm.get("display_name", name.replace("-", " ").title()),
            "description": description,
            "icon": fm.get("icon", "extension"),
            "color": fm.get("color", "#6B7280"),
            "tags": fm.get("tags", []),
            "files": files,
        }

    async def gather_in_scope_skills(
        self, user_id: str | None, project_ids: list[str] | None = None
    ) -> dict[str, Any]:
        """Build the resolved-blob skills payload: the precedence-deduped menu plus
        the file tree for each winning skill. Bundled (disk) + DB (owned + global).
        Returns {} when skills are disabled or there is no user. Slice 2."""
        from shared.runtime.core.skill_resolution import (
            is_reserved_system_skill_name,
            resolve_skill_menu,
        )

        if not self.deps.skills_enabled() or not user_id:
            return {}

        if self.state.skills is None:
            self.state.skills = self.scan_skills()

        rows: list[dict[str, Any]] = []
        for s in self.state.skills:
            # Managed system skills are injected only by the persistent-session
            # runtime. They are intentionally absent from autonomous worker
            # catalogs and cannot participate in ordinary DB precedence.
            if is_reserved_system_skill_name(s.name):
                continue
            rows.append(
                {
                    **s.model_dump(),
                    "owner_id": None,
                    "is_global": False,
                    "created_at": "",
                    "_source": "bundled",
                    "_ref": s.id,  # bundled dir name
                }
            )
        for r in await self.store.list_skills_visible(user_id=str(user_id)):
            if is_reserved_system_skill_name(str(r.get("name", ""))):
                continue
            rows.append(
                {
                    **skill_row_to_meta(r),
                    "owner_id": str(r["owner_id"]) if r.get("owner_id") else None,
                    "is_global": r["is_global"],
                    "created_at": str(r.get("created_at", "")),
                    "_source": "global" if r["is_global"] else "user",
                    "_ref": str(r["id"]),
                }
            )

        menu_rows = resolve_skill_menu(
            rows, user_id=str(user_id), project_ids=set(project_ids or [])
        )

        menu: list[dict[str, Any]] = []
        files: dict[str, dict[str, str]] = {}
        for row in menu_rows:
            menu.append(
                {
                    "id": row.get("id"),
                    "name": row["name"],
                    "display_name": row.get("display_name"),
                    "description": row.get("description") or "",
                    "icon": row.get("icon"),
                    "color": row.get("color"),
                    "tags": row.get("tags") or [],
                }
            )
            if row["_source"] == "bundled":
                bundle = self.bundled_skill_bundle(row["_ref"])
                if bundle:
                    files[row["name"]] = bundle["files"]
            else:
                files[row["name"]] = await self.store.get_skill_files(row["_ref"])

        return {"menu": menu, "files": files}

    def bundled_expert_bundle(self, expert_id: str) -> dict[str, Any] | None:
        """Portable bundle from a bundled (disk) expert: raw config.yaml fragment
        (minus $extends/connections) + persona/instructions files + cache metadata.
        None if not found. expert_type is inferred from $extends."""
        if self.state.experts is None:
            self.state.experts = self.scan_experts()
        info = next((e for e in self.state.experts if e.id == expert_id), None)
        if not info:
            return None
        expert_dir = self.deps.get_config_dir() / "experts" / expert_id
        config_path = expert_dir / "config.yaml"
        if not config_path.exists():
            return None
        raw = read_srw_config(config_path)
        extends = canonical_config_name(str(raw.pop("$extends", "worker_base")))
        raw.pop("connections", None)
        # Part 2: capture all prompt segments a fork should round-trip.
        # strategic/tactical come from the expert-local phase skills (U2: the
        # bodies of skills/<phase>-phase/SKILL.md), keeping the DB shape — at
        # delivery they are the fenced <expert_workflow> addendum of the phase block.
        prompts: dict[str, Any] = {}
        for key, fname in (
            ("persona", "persona.txt"),
            ("instructions", "instructions.md"),
            ("summarization", "summarization_prompt.txt"),
        ):
            fp = expert_dir / fname
            if fp.exists():
                prompts[key] = fp.read_text(encoding="utf-8")
        prompts.update(expert_phase_prompt_bodies(expert_dir))
        return {
            "name": expert_id,
            "display_name": info.display_name,
            "description": info.description,
            "icon": info.icon,
            "color": info.color,
            "tags": info.tags,
            "expert_type": "session" if extends == "session_base" else "worker",
            "config": raw,
            "prompts": prompts,
        }

    async def bundled_expert_source(self, expert_id: str) -> dict[str, Any] | None:
        """Use the saved private fragment for copies; files supply prompt assets."""
        bundle = self.bundled_expert_bundle(expert_id)
        document = await self.bundled_manifest(expert_id)
        if not bundle or document is None:
            return bundle
        if document["spec"]["runtime"].get("adapter") != SRW_HARNESS_ADAPTER:
            raise HTTPException(
                409, "Export or copy this harness through its Expert manifest."
            )
        private = srw_private_config(document)
        config = private.get("config", {})
        config.pop("$extends", None)
        config.pop("connections", None)
        bundle["config"] = config
        bundle["prompts"].update(private.get("prompts", {}))
        if private.get("layers"):
            bundle["harness_config_layers"] = private["layers"]
        for key in ("config_name", "asset_name"):
            if private.get(key):
                bundle["harness_" + key] = private[key]
        return bundle

    async def list_skills(self, *, user: dict[str, Any]) -> list[dict[str, Any]]:
        """List skills: bundled (disk) + DB rows visible to the caller (owned + global),
        each tagged with ``source``. Read-only; tags-and-concatenates (precedence is a
        Slice-2 resolver concern)."""
        if self.state.skills is None:
            self.state.skills = self.scan_skills()
        result = [{**s.model_dump(), "source": "bundled"} for s in self.state.skills]
        if self.deps.skills_enabled():
            rows = await self.store.list_skills_visible(user_id=str(user["id"]))
            result += [
                {
                    **skill_row_to_meta(r),
                    "source": "global" if r["is_global"] else "user",
                }
                for r in rows
            ]
        return result

    async def reload_skills(self) -> dict[str, Any]:
        """Force reload of bundled skill cache. **Admin only**."""
        self.state.skills = self.scan_skills()
        return {"status": "reloaded", "count": len(self.state.skills)}

    async def get_visible_skill_row(
        self, skill_id: str, *, user: dict[str, Any]
    ) -> dict[str, Any] | None:
        """A DB skill row by UUID, or None when it is missing OR invisible to
        ``user``. The two are deliberately indistinguishable — every by-id skill
        route answers 404 for both, as expert reads by id do — so holding a UUID
        is neither a read grant nor an existence oracle."""
        row = await self.store.get_skill_by_id(skill_id)
        return row if row and skill_row_visible(row, user) else None

    async def get_skill(self, skill_id: str, *, user: dict[str, Any]) -> dict[str, Any]:
        """Full skill detail (metadata + file tree). DB skill by UUID, else bundled."""
        if self.deps.skills_enabled() and self.deps.looks_like_uuid(skill_id):
            row = await self.get_visible_skill_row(skill_id, user=user)
            if not row:
                raise HTTPException(
                    status_code=404, detail=f"Skill not found: {skill_id}"
                )
            files = await self.store.get_skill_files(skill_id)
            return {
                **skill_row_to_meta(row),
                "source": "global" if row["is_global"] else "user",
                "files": files,
            }
        bundle = self.bundled_skill_bundle(skill_id)
        if not bundle:
            raise HTTPException(status_code=404, detail=f"Skill not found: {skill_id}")
        return {**bundle, "source": "bundled"}

    async def get_project_jobs_repo(self, project_id: str) -> str | None:
        """Get the jobs repo name for a project. Returns None if not found."""
        repos = await self.store.get_project_repositories(project_id, role="jobs")
        if not repos:
            return None
        return repos[0].get("name")

    async def list_project_experts(self, project_id: str) -> list[dict[str, Any]]:
        """List DB-backed experts linked to a project.

        During the safe migration, an old project's ``experts/`` directory remains
        a read-only fallback when it has no structured links yet.
        """
        linked = await self.store.list_project_linked_experts(project_id)
        if linked:
            return [
                {
                    "id": str(row["id"]),
                    "name": row["name"],
                    "display_name": row["display_name"],
                    "description": row.get("description") or "",
                    "icon": row.get("icon") or "psychology",
                    "color": row.get("color") or "#cba6f7",
                    "tags": row.get("tags") or [],
                    "expert_type": row.get("expert_type") or "worker",
                    "source": "project",
                    "storage_kind": "db",
                    "default_for": row.get("default_for"),
                }
                for row in linked
            ]

        if not self.deps.forge.is_initialized:
            return []

        repo_name = await self.get_project_jobs_repo(project_id)
        if not repo_name:
            return []

        try:
            entries = await self.deps.forge.list_contents(repo_name, "experts")
        except Exception:
            return []

        if not entries:
            return []

        experts: list[dict[str, Any]] = []
        for entry in entries:
            if entry.get("type") != "dir":
                continue
            name = entry.get("name", "")
            try:
                content = await self.deps.forge.get_file_content(
                    repo_name, f"experts/{name}/config.yaml"
                )
                if not content:
                    continue
                data = yaml.safe_load(content) or {}

                description = data.get("description", "").strip()
                if not description:
                    tools = data.get("tools", {})
                    tool_categories = [k for k in tools if tools[k]]
                    description = (
                        f"Agent with {', '.join(tool_categories)} tools."
                        if tool_categories
                        else "Custom agent configuration."
                    )

                experts.append(
                    ExpertInfo(
                        id=name,
                        display_name=data.get(
                            "display_name", name.replace("_", " ").title()
                        ),
                        description=description,
                        icon=data.get("icon", "psychology"),
                        color=data.get("color", "#cba6f7"),
                        tags=data.get("tags", []),
                    ).model_dump()
                )
            except Exception as e:
                logger.warning(f"Failed to parse project expert {name}: {e}")

        return experts

    async def get_project_expert(
        self, project_id: str, expert_name: str, *, caller: dict[str, Any]
    ) -> dict[str, Any]:
        """Get full detail for a linked expert, with a legacy Git fallback."""
        linked = await self.store.get_project_linked_expert(project_id, expert_name)
        if linked:
            detail = await self.load_expert_detail(
                str(linked["id"]), user_id=str(caller["id"])
            )
            project_override = linked.get("project_config_override") or {}
            if isinstance(project_override, str):
                try:
                    project_override = json.loads(project_override)
                except (json.JSONDecodeError, TypeError):
                    project_override = {}
            if isinstance(project_override, dict) and project_override:
                detail["config"] = deep_merge_dicts(
                    detail.get("config") or {}, project_override
                )
            detail["name"] = linked["name"]
            detail["source"] = "project"
            detail["default_for"] = linked.get("default_for")
            return detail

        if not self.deps.forge.is_initialized:
            raise HTTPException(status_code=503, detail="Gitea not available")

        repo_name = await self.get_project_jobs_repo(project_id)
        if not repo_name:
            raise HTTPException(status_code=404, detail="Project expert not found")

        # Read config
        config_content = await self.deps.forge.get_file_content(
            repo_name, f"experts/{expert_name}/config.yaml"
        )
        if not config_content:
            raise HTTPException(
                status_code=404, detail=f"Expert not found: {expert_name}"
            )

        try:
            expert_data = yaml.safe_load(config_content) or {}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Invalid YAML: {e}") from e

        # Build info
        description = expert_data.get("description", "").strip()
        if not description:
            tools = expert_data.get("tools", {})
            tool_categories = [k for k in tools if tools[k]]
            description = (
                f"Agent with {', '.join(tool_categories)} tools."
                if tool_categories
                else "Custom agent configuration."
            )

        info = ExpertInfo(
            id=expert_name,
            display_name=expert_data.get(
                "display_name", expert_name.replace("_", " ").title()
            ),
            description=description,
            icon=expert_data.get("icon", "psychology"),
            color=expert_data.get("color", "#cba6f7"),
            tags=expert_data.get("tags", []),
        )

        # Merge with the worker role base (Gitea-stored experts are worker experts)
        config_dir = self.deps.get_config_dir()
        defaults = role_base_or_empty("worker")

        expert_data_clean = dict(expert_data)
        expert_data_clean.pop("$extends", None)
        merged = prune_ignored_keys(deep_merge_dicts(defaults, expert_data_clean))
        for key in ("$extends", "$ignore_keys", "connections"):
            merged.pop(key, None)

        # Load the raw settings_matrix for the client to resolve per-model defaults.
        # Do NOT apply it to merged — the client resolves based on the user's model selection.
        raw_matrix = self.deps.load_settings_matrix(config_dir)

        # Read instructions
        instructions_content = await self.deps.forge.get_file_content(
            repo_name, f"experts/{expert_name}/instructions.md"
        )

        return {
            **info.model_dump(),
            "config": merged,
            "instructions": instructions_content,
            "settings_matrix": raw_matrix,
        }
