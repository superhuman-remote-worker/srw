"""Reusable expert/skill authoring and default-selection operations.

Adapters resolve identity and feature availability in their original order.
Request scanning/save gates arrive as bound policy operations so fork validation,
grant checks and writes retain their sequencing without importing HTTP Request.
"""

from __future__ import annotations

import logging
from typing import Any, Literal

from fastapi import HTTPException

from orchestrator.schemas.expert_catalog import (
    ExpertCreate,
    ExpertUpdate,
    SkillCreate,
    SkillUpdate,
    ExpertDefaultSetRequest,
    ExpertDefaultForkRequest,
    validate_expert_prompt_source_or_422,
)
from orchestrator.services.config_resolver import resolve_config
from orchestrator.services.default_experts import (
    BASE_CONFIG_NAMES,
    DefaultExpertUnavailable,
    ExpertSelectionError,
    personal_defaults_allowed,
    resolve_root_expert,
)

from orchestrator.services.expert_catalog import (
    ExpertCatalogService,
    db_expert_to_bundle_src,
    parse_skill_bundle,
    skill_row_to_meta,
)
from orchestrator.services.expert_catalog_contracts import (
    ExpertCatalogStore,
    ExpertWritePolicy,
    SkillArchive,
    DefaultModels,
    PrefetchRosterRefs,
)

logger = logging.getLogger(__name__)


def default_expert_summary(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if not row:
        return None
    return {
        "id": str(row["id"]),
        "name": row["name"],
        "display_name": row["display_name"],
        "description": row.get("description") or "",
        "icon": row.get("icon") or "smart_toy",
        "color": row.get("color") or "#6B7280",
        "tags": row.get("tags") or [],
        "expert_type": row["expert_type"],
        "owner_id": str(row["owner_id"]) if row.get("owner_id") else None,
        "is_global": bool(row.get("is_global")),
        "managed_key": row.get("managed_key"),
        "storage_kind": "db",
    }


class ExpertAuthoringService:
    def __init__(
        self,
        *,
        store: ExpertCatalogStore,
        catalog: ExpertCatalogService,
        resolve_default_models: DefaultModels,
        prefetch_roster_refs: PrefetchRosterRefs,
    ) -> None:
        self.store = store
        self.catalog = catalog
        self.resolve_default_models = resolve_default_models
        self.prefetch_roster_refs = prefetch_roster_refs

    async def create_forked_skill(
        self,
        src: dict[str, Any],
        owner_id: str,
        suffix: str = "copy",
        *,
        prefer_original: bool = False,
    ) -> dict[str, Any]:
        """Create an owned skill from a source dict. ``prefer_original`` (import) tries
        the source name first and only suffixes on collision, storing the SKILL.md
        verbatim so a clean import->export round-trips byte-for-byte; duplicate always
        suffixes ``-copy``. The SKILL.md 'name' is rewritten only when the slug changes."""
        from shared.runtime.core.skill_format import set_skill_name

        base_name = src["name"]
        candidates = [base_name] if prefer_original else []
        candidates.append(f"{base_name}-{suffix}")
        candidates += [f"{base_name}-{suffix}-{i}" for i in range(2, 8)]
        for cand in candidates:
            name = cand[:100]
            renamed = name != base_name
            files = dict(src["files"])
            if renamed:
                files["SKILL.md"] = set_skill_name(src["files"]["SKILL.md"], name)
            display = (
                f"{src['display_name']} ({suffix})" if renamed else src["display_name"]
            )
            try:
                return await self.store.create_skill(
                    name=name,
                    display_name=display[:200],
                    description=src.get("description"),
                    icon=src.get("icon", "extension"),
                    color=src.get("color", "#6B7280"),
                    tags=src.get("tags") or [],
                    owner_id=owner_id,
                    files=files,
                )
            except HTTPException:
                raise
            except Exception as e:
                if "uq_skills_name_owner" in str(e):
                    continue
                raise
        raise HTTPException(status_code=409, detail="No free name for the copy")

    async def create_forked_expert(
        self, src: dict[str, Any], owner_id: str, suffix: str = "copy"
    ) -> dict[str, Any]:
        """Create an owned expert from a bundle dict, suffixing the name on collision
        (decision 4/27 fork-on-copy)."""
        from shared.runtime.core.expert_resolution import with_role_tag

        base_name = src["name"]
        name = f"{base_name}-{suffix}"[:100]
        prompts = validate_expert_prompt_source_or_422(src.get("prompts") or {}) or {}
        for attempt in range(6):
            try:
                return await self.store.create_expert(
                    name=name,
                    display_name=f"{src['display_name']} ({suffix})"[:200],
                    expert_type=src["expert_type"],
                    owner_id=owner_id,
                    description=src.get("description"),
                    icon=src.get("icon", "smart_toy"),
                    color=src.get("color", "#6B7280"),
                    tags=with_role_tag(src["expert_type"], src.get("tags")),
                    config=src.get("config") or {},
                    prompts=prompts,
                    **(
                        {"srw_layers": src["harness_config_layers"]}
                        if src.get("harness_config_layers")
                        else {}
                    ),
                    **{
                        "srw_" + key: src["harness_" + key]
                        for key in ("config_name", "asset_name")
                        if src.get("harness_" + key)
                    },
                )
            except HTTPException:
                raise
            except Exception as e:
                if "uq_experts_name_owner" in str(e):
                    name = f"{base_name}-{suffix}-{attempt + 1}"[:100]
                    continue
                raise
        raise HTTPException(status_code=409, detail="No free name for the copy")

    async def create_expert(
        self,
        body: ExpertCreate,
        *,
        user: dict[str, Any],
        write_policy: ExpertWritePolicy,
    ) -> dict[str, Any]:
        """Create an owned DB expert. Slice 1: hard-deny validated, no grants yet.
        The stored ``tags`` always carry the role (``tags ∪ {expert_type}``, U1)."""
        from shared.runtime.core.expert_resolution import with_role_tag

        if body.config:
            body.config = self.catalog.validate_expert_fragment(body.config)
            await self.catalog.require_visible_roster_refs(body.config, user=user)
        await write_policy.enforce_save(body.config or {}, user=user)
        try:
            return await self.store.create_expert(
                name=body.name,
                display_name=body.display_name,
                expert_type=body.expert_type,
                owner_id=str(user["id"]),
                description=body.description,
                icon=body.icon,
                color=body.color,
                tags=with_role_tag(body.expert_type, body.tags),
                config=body.config,
                prompts=body.prompts,
            )
        except HTTPException:
            raise
        except Exception as e:
            if "uq_experts_name_owner" in str(e):
                raise HTTPException(
                    status_code=409,
                    detail=f"You already have an expert named '{body.name}'",
                ) from e
            raise

    async def update_expert(
        self,
        expert_id: str,
        body: ExpertUpdate,
        *,
        user: dict[str, Any],
        write_policy: ExpertWritePolicy,
    ) -> dict[str, Any]:
        """Update an owned DB expert (owner or admin). Bundled experts have no row."""
        if not self.catalog.deps.looks_like_uuid(expert_id):
            raise HTTPException(status_code=403, detail="Bundled experts are read-only")
        existing = await self.store.get_expert_by_id(expert_id)
        if not existing:
            raise HTTPException(status_code=404, detail="Expert not found")
        if "harness_adapter" in existing and existing["harness_adapter"] != "srw/v1":
            raise HTTPException(409, "Edit this harness through its Expert manifest.")
        if str(existing["owner_id"]) != str(user["id"]) and not user.get("is_admin"):
            raise HTTPException(
                status_code=403, detail="Only the owner may edit this expert"
            )
        if body.config is not None:
            body.config = self.catalog.validate_expert_fragment(body.config)
            await self.catalog.require_visible_roster_refs(body.config, user=user)
        await write_policy.enforce_save(body.config or {}, user=user)
        fields = body.model_dump(exclude_unset=True)
        if "tags" in fields:
            # tags ∪ {role}: the row's role tag survives every tag edit (U1 B.4).
            from shared.runtime.core.expert_resolution import with_role_tag

            fields["tags"] = with_role_tag(existing["expert_type"], fields["tags"])
        updated = await self.store.update_expert(
            expert_id, updated_by=str(user["id"]), **fields
        )
        if updated and existing.get("managed_key"):
            await self.store.record_managed_expert_update(
                expert_id=expert_id,
                expert_type=existing["expert_type"],
                actor_user_id=str(user["id"]),
            )
        return updated

    async def delete_expert(
        self, expert_id: str, *, user: dict[str, Any]
    ) -> dict[str, Any]:
        """Delete an owned DB expert (owner or admin). Blocks (409) while
        live-referenced (decision 15)."""
        if not self.catalog.deps.looks_like_uuid(expert_id):
            raise HTTPException(
                status_code=403, detail="Bundled experts cannot be deleted"
            )
        existing = await self.store.get_expert_by_id(expert_id)
        if not existing:
            raise HTTPException(status_code=404, detail="Expert not found")
        if existing.get("managed_key"):
            raise HTTPException(
                status_code=409,
                detail="Managed platform experts cannot be deleted; change the application default instead",
            )
        if str(existing["owner_id"]) != str(user["id"]) and not user.get("is_admin"):
            raise HTTPException(
                status_code=403, detail="Only the owner may delete this expert"
            )
        blockers = await self.store.expert_delete_blockers(expert_id)
        if blockers:
            raise HTTPException(
                status_code=409,
                detail={
                    "message": "Expert is in use; repoint or remove these first",
                    "blockers": blockers,
                },
            )
        await self.store.delete_expert(expert_id)
        return {"deleted": True}

    async def duplicate_expert(
        self, expert_id: str, *, user: dict[str, Any], write_policy: ExpertWritePolicy
    ) -> dict[str, Any]:
        """Fork any visible expert (bundled or DB) into an owned copy — 'start from
        scholar' (decision 4: copy, not live link)."""
        if self.catalog.deps.looks_like_uuid(expert_id):
            visible = await self.catalog.deps.visible_project_ids(user, self.store)
            row = await self.store.get_expert_visible_by_id(
                expert_id,
                user_id=str(user["id"]),
                project_ids=[] if visible == "all" else [str(p) for p in visible],
                is_admin=bool(user.get("is_admin")),
            )
            if not row:
                raise HTTPException(status_code=404, detail="Expert not found")
            src = db_expert_to_bundle_src(row)
        else:
            src = await self.catalog.bundled_expert_source(expert_id)
            if not src:
                raise HTTPException(status_code=404, detail="Expert not found")
        # A fork is a new write by a new principal, and the source row may be
        # someone else's (visibility, not ownership, is the test above). Validating
        # here is what stops a legacy smuggled fragment being copied forward — the
        # same reason `fork_my_expert_default` validates its source. Both
        # `_bundled_expert_bundle` and `_db_expert_to_bundle_src` build a fresh
        # dict, so assigning into `src` cannot corrupt a cache or the source row.
        src["config"] = self.catalog.validate_expert_fragment(src.get("config") or {})
        # Fifth of five expert-write routes. The other four call
        # _enforce_expert_save right after validating theirs — the kill-switch
        # half of that is NOT optional here just because the row already existed:
        # without it, a user can mint an owned DB expert by copying any VISIBLE
        # expert (not necessarily their own) while the administrator has
        # user_experts disabled. Grants are a different story on this one route
        # (2026-08-04 decision): the source config may be another principal's and
        # commonly needs a grant the copier does not hold and should not have to
        # ask for — measured, that refused 7 of the 11 shipped experts, including
        # `scholar`, this route's own advertised use ("start from scholar"). So
        # this strips what the copier's grants forbid and reports it, instead of
        # refusing outright the way the other four routes still do.
        await write_policy.enforce_save_prelude()
        src["config"], dropped = await write_policy.strip_save_grants(
            src["config"], user=user
        )
        forked = await self.create_forked_expert(src, str(user["id"]), suffix="copy")
        return {**forked, "dropped": dropped}

    async def export_expert(
        self, expert_id: str, *, user: dict[str, Any]
    ) -> dict[str, Any]:
        """Serialize an expert to a portable bundle (decision 27). DB experts export
        their raw fragment; bundled experts export their on-disk config."""
        from shared.runtime.core.expert_resolution import to_export_bundle

        if self.catalog.deps.looks_like_uuid(expert_id):
            visible = await self.catalog.deps.visible_project_ids(user, self.store)
            row = await self.store.get_expert_visible_by_id(
                expert_id,
                user_id=str(user["id"]),
                project_ids=[] if visible == "all" else [str(p) for p in visible],
                is_admin=bool(user.get("is_admin")),
            )
            if not row:
                raise HTTPException(status_code=404, detail="Expert not found")
            if row.get("harness_config_layers") or row.get("harness_asset_name"):
                raise HTTPException(
                    409,
                    "Export this Expert through its manifest to retain private layers and installed asset selections.",
                )
            return to_export_bundle(db_expert_to_bundle_src(row))
        bundle = await self.catalog.bundled_expert_source(expert_id)
        if not bundle:
            raise HTTPException(status_code=404, detail="Expert not found")
        if bundle.get("harness_config_layers") or bundle.get("harness_asset_name"):
            raise HTTPException(
                409,
                "Export this Expert through its manifest to retain private layers and installed asset selections.",
            )
        return to_export_bundle(bundle)

    async def import_expert(
        self,
        body: ExpertCreate,
        *,
        user: dict[str, Any],
        write_policy: ExpertWritePolicy,
    ) -> dict[str, Any]:
        """Create an owned expert from a posted bundle (decision 27). Same validation
        as create; fork-on-import (name collision -> suffix)."""
        from shared.runtime.core.expert_resolution import with_role_tag

        if body.config:
            body.config = self.catalog.validate_expert_fragment(body.config)
            await self.catalog.require_visible_roster_refs(body.config, user=user)
        await write_policy.enforce_save(body.config or {}, user=user)
        name = body.name
        for attempt in range(6):
            try:
                return await self.store.create_expert(
                    name=name,
                    display_name=body.display_name,
                    expert_type=body.expert_type,
                    owner_id=str(user["id"]),
                    description=body.description,
                    icon=body.icon,
                    color=body.color,
                    tags=with_role_tag(body.expert_type, body.tags),
                    config=body.config,
                    prompts=body.prompts,
                )
            except HTTPException:
                raise
            except Exception as e:
                if "uq_experts_name_owner" in str(e):
                    name = (
                        f"{body.name}-import"
                        if attempt == 0
                        else f"{body.name}-import-{attempt}"
                    )
                    continue
                raise
        raise HTTPException(status_code=409, detail="No free name for the import")

    async def get_my_expert_defaults(
        self, project_id: str | None = None, *, user: dict[str, Any]
    ) -> dict[str, Any]:
        """Effective and editable personal defaults for the current user."""
        uid = str(user["id"])
        allowed = await personal_defaults_allowed(
            self.store,
            user_id=uid,
            project_ids=[project_id] if project_id else [],
            is_admin=bool(user.get("is_admin")),
        )
        slots: dict[str, Any] = {}
        for expert_type in ("worker", "session"):
            application = await self.store.get_application_expert_default(expert_type)
            personal = await self.store.get_user_expert_default(
                user_id=uid, expert_type=expert_type
            )
            try:
                selection = await resolve_root_expert(
                    self.store,
                    expert_type=expert_type,
                    user_id=uid,
                    project_id=project_id,
                    is_admin=bool(user.get("is_admin")),
                )
                effective = selection.expert
                effective_source = selection.source
            except DefaultExpertUnavailable:
                effective = None
                effective_source = "application"
            slots[expert_type] = {
                "application": default_expert_summary(application),
                "personal": default_expert_summary(personal),
                "effective": default_expert_summary(effective),
                "source": effective_source,
            }
        return {"personal_defaults_allowed": allowed, "defaults": slots}

    async def set_my_expert_default(
        self,
        expert_type: Literal["worker", "session"],
        body: ExpertDefaultSetRequest,
        *,
        user: dict[str, Any],
    ) -> dict[str, Any]:
        uid = str(user["id"])
        if not await personal_defaults_allowed(
            self.store, user_id=uid, is_admin=bool(user.get("is_admin"))
        ):
            raise HTTPException(
                status_code=403,
                detail="Your administrator has disabled personal default experts",
            )
        try:
            await self.store.set_user_expert_default(
                user_id=uid, expert_type=expert_type, expert_id=body.expert_id
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        row = await self.store.get_user_expert_default(
            user_id=uid, expert_type=expert_type
        )
        return {"default": default_expert_summary(row), "source": "user"}

    async def clear_my_expert_default(
        self, expert_type: Literal["worker", "session"], *, user: dict[str, Any]
    ) -> dict[str, Any]:
        """Clear is intentionally allowed even after the grant is revoked."""
        deleted = await self.store.clear_user_expert_default(
            user_id=str(user["id"]), expert_type=expert_type
        )
        application = await self.store.get_application_expert_default(expert_type)
        return {
            "deleted": deleted,
            "default": default_expert_summary(application),
            "source": "application",
        }

    async def fork_my_expert_default(
        self,
        expert_type: Literal["worker", "session"],
        body: ExpertDefaultForkRequest,
        *,
        user: dict[str, Any],
        write_policy: ExpertWritePolicy,
    ) -> dict[str, Any]:
        """Atomically copy a visible expert and select the owned copy as default.

        Two independent 403 gates precede any write, and neither is optional:
        `personal_defaults_allowed` (this route's own switch — a personal default
        may be disabled while user-defined experts generally are not) and then
        the `user_experts` kill switch inside `_enforce_expert_save_prelude` (the
        same switch every expert-write route shares). Do not reorder or merge
        them.
        """
        uid = str(user["id"])
        if not await personal_defaults_allowed(
            self.store, user_id=uid, is_admin=bool(user.get("is_admin"))
        ):
            raise HTTPException(
                status_code=403,
                detail="Your administrator has disabled personal default experts",
            )

        if body.expert_id and not self.catalog.deps.looks_like_uuid(body.expert_id):
            source = await self.catalog.bundled_expert_source(body.expert_id)
            if not source:
                raise HTTPException(status_code=404, detail="Expert not found")
            if source["expert_type"] != expert_type:
                raise HTTPException(
                    status_code=422, detail="Expert type does not match"
                )
        else:
            try:
                selection = await resolve_root_expert(
                    self.store,
                    expert_type=expert_type,
                    user_id=uid,
                    explicit_expert_id=body.expert_id,
                    is_admin=bool(user.get("is_admin")),
                )
            except ExpertSelectionError as exc:
                raise HTTPException(status_code=422, detail=str(exc)) from exc
            except DefaultExpertUnavailable as exc:
                raise HTTPException(status_code=503, detail=str(exc)) from exc
            source = db_expert_to_bundle_src(selection.expert)

        source["config"] = self.catalog.validate_expert_fragment(
            source.get("config") or {}
        )
        source["prompts"] = (
            validate_expert_prompt_source_or_422(source.get("prompts") or {}) or {}
        )
        # Same 2026-08-04 decision as duplicate_expert (task 3 of the plan above):
        # this is `duplicate` plus "select the copy as my default", and its source
        # config carries the identical 7-of-11 exposure — a bundled expert or
        # another principal's DB row commonly needs a grant this caller does not
        # hold (measured, this blocked `scholar`, the one this route's set-default
        # UI actually offers to fork from). Strip what the caller's grants forbid
        # and report it, instead of refusing outright. `_enforce_expert_save_prelude`
        # still runs first (kill-switch + raw scan), unconditionally, before either
        # gate below — `_strip_save_grants` re-runs `evaluate` on the STRIPPED
        # result and 422s if anything survives, so an incomplete strip map can only
        # ever produce a false refusal here, never a permitted escape.
        await write_policy.enforce_save_prelude()
        # A default SLOT is per role, so a DB source must match it too (the
        # bundled branch above refuses the same way) — even though an explicit
        # cross-role pick is allowed for a root job or session since U1 (D4:
        # `validate_explicit_expert` logs, not refuses). After the shared gates,
        # like every other route-local check.
        if source.get("expert_type") != expert_type:
            raise HTTPException(status_code=422, detail="Expert type does not match")
        from shared.runtime.core.expert_resolution import with_role_tag

        source["tags"] = with_role_tag(expert_type, source.get("tags"))
        source["config"], dropped = await write_policy.strip_save_grants(
            source["config"], user=user
        )
        try:
            row = await self.store.fork_and_set_user_expert_default(
                user_id=uid, expert_type=expert_type, source=source
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return {
            "default": default_expert_summary(row),
            "source": "user",
            "dropped": dropped,
        }

    async def get_application_expert_defaults(self) -> dict[str, Any]:
        rows = await self.store.list_application_expert_defaults()
        by_type = {row["expert_type"]: default_expert_summary(row) for row in rows}
        return {"defaults": by_type}

    async def set_application_expert_default(
        self,
        expert_type: Literal["worker", "session"],
        body: ExpertDefaultSetRequest,
        *,
        admin: dict[str, Any],
    ) -> dict[str, Any]:
        target = await self.store.get_expert_by_id(body.expert_id)
        if not target:
            raise HTTPException(status_code=404, detail="Expert not found")
        if target.get("expert_type") != expert_type:
            raise HTTPException(
                status_code=422, detail="Expert type does not match default slot"
            )
        if not target.get("is_global"):
            raise HTTPException(
                status_code=422, detail="Application defaults must be global experts"
            )

        # Admins may author broader profiles, but an application default must fit
        # the deployment-wide grant floor or ordinary users could be assigned a
        # profile that can never dispatch. User/project restrictions are still
        # evaluated later for the actual runner.
        from orchestrator.services.grants_service import resolve_grants_for
        from shared.runtime.core.capability_grants import evaluate

        capture: dict[str, Any] = {}
        resolve_config(
            base_config_name=BASE_CONFIG_NAMES[expert_type],
            base_defaults=await self.resolve_default_models(None),
            expert_row=target,
            expert_type=expert_type,
            capture=capture,
            db_refs=await self.prefetch_roster_refs(expert_row=target),
        )
        global_grants = await resolve_grants_for(
            self.store, user_id=None, project_ids=[]
        )
        violations = evaluate(capture["merged_fragment"], global_grants)
        if violations:
            raise HTTPException(
                status_code=422,
                detail=(
                    "Application default exceeds the deployment capability floor: "
                    + "; ".join(violations)
                ),
            )
        try:
            await self.store.set_application_expert_default(
                expert_type=expert_type,
                expert_id=body.expert_id,
                actor_user_id=str(admin["id"]),
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        row = await self.store.get_application_expert_default(expert_type)
        return {"default": default_expert_summary(row)}

    async def set_project_expert_default(
        self,
        project_id: str,
        expert_type: Literal["worker", "session"],
        body: ExpertDefaultSetRequest,
        *,
        user: dict[str, Any],
    ) -> dict[str, Any]:
        visible = await self.store.get_expert_visible_by_id(
            body.expert_id,
            user_id=str(user["id"]),
            project_ids=[project_id],
            is_admin=bool(user.get("is_admin")),
        )
        if not visible:
            raise HTTPException(status_code=404, detail="Expert not found")
        try:
            await self.store.set_project_default_expert(
                project_id=project_id,
                expert_type=expert_type,
                expert_id=body.expert_id,
                actor_user_id=str(user["id"]),
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        row = await self.store.get_project_default_expert(
            project_id=project_id, expert_type=expert_type
        )
        return {"default": default_expert_summary(row)}

    async def clear_project_expert_default(
        self,
        project_id: str,
        expert_type: Literal["worker", "session"],
        *,
        user: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "deleted": await self.store.clear_project_default_expert(
                project_id=project_id,
                expert_type=expert_type,
                actor_user_id=str(user["id"]),
            )
        }

    async def create_skill(
        self, body: SkillCreate, *, user: dict[str, Any]
    ) -> dict[str, Any]:
        """Create an owned DB skill from its file tree (Slice 1: deny-scan validated)."""
        name, description, files = parse_skill_bundle(body.files)
        try:
            return await self.store.create_skill(
                name=name,
                display_name=body.display_name or name,
                description=description,
                icon=body.icon,
                color=body.color,
                tags=body.tags,
                owner_id=str(user["id"]),
                files=files,
            )
        except HTTPException:
            raise
        except Exception as e:
            if "uq_skills_name_owner" in str(e):
                raise HTTPException(
                    status_code=409, detail=f"You already have a skill named '{name}'"
                ) from e
            raise

    async def update_skill(
        self, skill_id: str, body: SkillUpdate, *, user: dict[str, Any]
    ) -> dict[str, Any]:
        """Update an owned DB skill (owner or admin). Bundled skills are read-only.
        ``name`` is immutable — an edited SKILL.md whose frontmatter name differs is
        rejected (rename = create a new skill)."""
        if not self.catalog.deps.looks_like_uuid(skill_id):
            raise HTTPException(status_code=403, detail="Bundled skills are read-only")
        existing = await self.catalog.get_visible_skill_row(skill_id, user=user)
        if not existing:
            raise HTTPException(status_code=404, detail="Skill not found")
        if str(existing["owner_id"]) != str(user["id"]) and not user.get("is_admin"):
            raise HTTPException(
                status_code=403, detail="Only the owner may edit this skill"
            )
        from shared.runtime.core.skill_resolution import is_reserved_system_skill_name

        if is_reserved_system_skill_name(str(existing.get("name", ""))):
            raise HTTPException(
                status_code=422,
                detail=(
                    f"Skill name '{existing['name']}' is reserved for a managed SRW "
                    "product artifact and cannot be updated"
                ),
            )
        fields = body.model_dump(exclude_unset=True, exclude={"files"})
        if fields.get("is_global") is True and not user.get("is_admin"):
            # A global skill outranks the bundled one of the same name in every
            # user's menu (resolve_skill_menu), so publishing is an admin act.
            # Un-publishing your own stays open.
            raise HTTPException(
                status_code=403, detail="Only an admin may publish a skill globally"
            )
        files = body.files
        if files is not None:
            name, description, files = parse_skill_bundle(files)
            if name != existing["name"]:
                raise HTTPException(
                    status_code=422,
                    detail=f"SKILL.md name '{name}' must match the skill's name "
                    f"'{existing['name']}'; create a new skill to rename",
                )
            fields["description"] = description
        return await self.store.update_skill(
            skill_id, updated_by=str(user["id"]), files=files, **fields
        )

    async def delete_skill(
        self, skill_id: str, *, user: dict[str, Any]
    ) -> dict[str, Any]:
        """Delete an owned DB skill (owner or admin). Files cascade away."""
        if not self.catalog.deps.looks_like_uuid(skill_id):
            raise HTTPException(
                status_code=403, detail="Bundled skills cannot be deleted"
            )
        existing = await self.catalog.get_visible_skill_row(skill_id, user=user)
        if not existing:
            raise HTTPException(status_code=404, detail="Skill not found")
        if str(existing["owner_id"]) != str(user["id"]) and not user.get("is_admin"):
            raise HTTPException(
                status_code=403, detail="Only the owner may delete this skill"
            )
        await self.store.delete_skill(skill_id)
        return {"deleted": True}

    async def duplicate_skill(
        self, skill_id: str, *, user: dict[str, Any]
    ) -> dict[str, Any]:
        """Fork any visible skill (bundled or DB) into an owned copy."""
        if self.catalog.deps.looks_like_uuid(skill_id):
            row = await self.catalog.get_visible_skill_row(skill_id, user=user)
            if not row:
                raise HTTPException(status_code=404, detail="Skill not found")
            src = {
                **skill_row_to_meta(row),
                "files": await self.store.get_skill_files(skill_id),
            }
        else:
            src = self.catalog.bundled_skill_bundle(skill_id)
            if not src:
                raise HTTPException(status_code=404, detail="Skill not found")
        return await self.create_forked_skill(src, str(user["id"]))

    async def export_skill(
        self, skill_id: str, *, user: dict[str, Any]
    ) -> SkillArchive:
        """Serialize a skill to a native zipped directory (drops into .claude/skills)."""
        from shared.runtime.core.skill_format import pack_skill_zip

        if self.catalog.deps.looks_like_uuid(skill_id):
            row = await self.catalog.get_visible_skill_row(skill_id, user=user)
            if not row:
                raise HTTPException(status_code=404, detail="Skill not found")
            name, files = row["name"], await self.store.get_skill_files(skill_id)
        else:
            bundle = self.catalog.bundled_skill_bundle(skill_id)
            if not bundle:
                raise HTTPException(status_code=404, detail="Skill not found")
            name, files = bundle["name"], bundle["files"]
        return SkillArchive(name=name, content=pack_skill_zip(name, files))

    async def import_skill(
        self, archive: bytes, *, user: dict[str, Any]
    ) -> dict[str, Any]:
        """Create an owned skill from an uploaded skill zip (fork-on-name-collision)."""
        from shared.runtime.core.skill_format import SkillFormatError, unpack_skill_zip

        try:
            files = unpack_skill_zip(archive)
        except SkillFormatError as e:
            raise HTTPException(status_code=422, detail=str(e)) from e
        name, description, files = parse_skill_bundle(files)
        src = {
            "name": name,
            "display_name": name.replace("-", " ").title(),
            "description": description,
            "files": files,
        }
        return await self.create_forked_skill(
            src, str(user["id"]), suffix="import", prefer_original=True
        )
