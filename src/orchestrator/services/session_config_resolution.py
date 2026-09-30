"""The session PDP: resolve a thread's config to a delivered blob, and pre-flight it.

Extracted verbatim from ``orchestrator.main`` (R1.B05 lane P, census group
``R_POLICY``). :func:`resolve_session_config` is the single decision point that
attach, drift collection and both pre-flights run — one merge implementation, so
the settings dialog can never promise something different from what attach
enforces.

Layer order is the contract and is not reordered here:
``session_base`` -> account/system fallback (:func:`resolve_session_account_defaults`)
-> DB expert (``metadata.expert_id``) -> project-expert link override -> thread
``config_override`` -> credential injection. Sessions RE-RESOLVE on every
(re)attach; there is no freeze.

Properties moved unchanged:

* **Fail closed on grants, fail open on everything else.** ``GrantDenied``
  escapes the generic ``except`` deliberately — an unvetted override is never
  delivered — while any other resolve failure logs and returns ``None`` so the
  agent falls back to ``config_name``. The ``status`` dict carries
  ``disabled`` / ``ok`` / ``denied`` / ``error`` and, on denial, the violation
  list the drift collector reads instead of re-merging.
* **The grant strip runs INSIDE ``resolve_config``.** :func:`acknowledged_grant_strip`
  builds the hook that is applied to the fully merged ``data`` the delivered
  blob is built from — not just to the detached capture the PDP evaluates.
  Stripping only the capture leaves the delivered blob carrying the capability
  the grant revoked (the round-1 privilege-escalation finding). The
  ``enforce_dispatch_grants`` call afterwards stays authoritative.
* **Two trust levels for roster ``$ref``s.** :func:`prefetch_roster_refs` fetches
  a ref named by the EXPERT ROW by id (it was checked at save), and a ref named
  by an OVERRIDE layer with the RUNNER's visibility — an override cannot pull
  another user's private expert into a session. Anything missing is simply
  absent; dispatch never fails over its roster. No DB call at all when no layer
  names a ref.
* **A UUID in the ``config_name`` slot is a cockpit conflation**, resolved onto
  the real session base rather than treated as a file name.
* **The protected-cloud marker has three states.**
  :func:`session_grant_violations` classifies with
  ``session_runtime_admission.protected_cloud_marker_state`` and rejects
  ``malformed`` with its own 409 — never by truthiness.
* **Pre-flights fail OPEN on a non-grant error** and return ``[]``, exactly as
  attach does, so a resolve failure cannot make a startable session
  unstartable. :func:`session_endpoint_violations` additionally returns ``[]``
  on ``GrantDenied`` so the grant pre-flight owns that rejection and it is not
  double-reported.

Every collaborator arrives per invocation through
:class:`SessionConfigDependencies`. That is not ceremony: ``postgres_db`` and
the feature gates are rebound by tests on ``orchestrator.main``, and the four
cross-batch callables are owned by other batches entirely (B01 skills, B05 lane
C credentials and registry seeding, B05 root thread projects, B06 knowledge
scope). Resolving any of them in this module's own namespace would make a
rebind on main green but inert (port contract §P3).
"""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from typing import Any

from fastapi import HTTPException

from orchestrator.services.config_drift import acknowledged_grant_keys
from orchestrator.services.config_overrides import deep_merge_dicts, looks_like_uuid
from orchestrator.services.config_resolver import (
    inject_blob_credentials,
    resolve_config,
)
from orchestrator.services.grant_enforcement import (
    GrantDenied,
    strip_acknowledged_grants,
)
from orchestrator.services.session_class_policy import protected_cloud_officer_active
from orchestrator.services.session_runtime_admission import (
    protected_cloud_marker_state,
)
from orchestrator.services.session_tool_policy import (
    session_tool_group_disabled_markers,
)
from orchestrator.services.manifest_execution_snapshot import (
    apply_srw_delivery_bindings,
    read_execution,
    srw_snapshot_config,
)
from orchestrator.services.workspace_tier_policy import thread_workspace_backend
from shared.runtime.core.loader import canonical_config_name

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SessionConfigDependencies:
    """Collaborators for one session config resolution, resolved per invocation.

    ``store`` is main's ``postgres_db``. ``is_experts_db_enabled`` and
    ``user_experts_enabled`` are the two gates whose ``and`` decides whether a
    resolve happens at all — both are passed as callables so the short-circuit
    is evaluated live. The remaining fields are owned by other batches and
    reach this module only through here:

    * ``gather_in_scope_skills`` (B01) — ``(user_id, project_ids) -> payload``
    * ``seed_registry_model_overrides`` (B05 lane C) —
      ``(request_override, *, user_id) -> request_override``
    * ``inject_thread_dispatch_credentials`` (B05 lane C) —
      ``(config_override, *, user_id, project_id, include_kb_profile) -> blob``
    * ``thread_project_ids`` (B05 root) — ``(thread_id) -> [project_id]``
    * ``thread_has_knowledge_scope`` (B06) —
      ``(*, project_ids, datasource_ids) -> bool``
    * ``resolve_runner_grants`` / ``enforce_dispatch_grants`` —
      ``services.grant_enforcement``, reached through main's wrappers so a
      rebind there still steers the PDP.
    """

    store: Any
    is_experts_db_enabled: Callable[[], bool]
    user_experts_enabled: Callable[[], Awaitable[bool]]
    resolve_runner_grants: Callable[..., Awaitable[dict[str, Any] | None]]
    enforce_dispatch_grants: Callable[..., Awaitable[None]]
    gather_in_scope_skills: Callable[..., Awaitable[Any]]
    seed_registry_model_overrides: Callable[..., Awaitable[dict[str, Any] | None]]
    inject_thread_dispatch_credentials: Callable[..., Awaitable[dict[str, Any]]]
    thread_project_ids: Callable[[str], Awaitable[list[str]]]
    thread_has_knowledge_scope: Callable[..., Awaitable[bool]]


async def resolve_default_models(
    user_id: str | None, *, dependencies: "SessionConfigDependencies"
) -> dict[str, Any]:
    """Effective default chat + auxiliary MODEL NAMES for a user (no transport).

    Mirrors the model selection in ``_inject_dispatch_credentials`` /
    ``_inject_thread_dispatch_credentials``: a user's pinned default wins, else
    the system capability default. Returned as a config layer
    (``{"llm": {"model": ...}, "auxiliary": {"model": ...}}``) that
    ``resolve_config`` applies above the base config's placeholder model and below
    the expert. base_url/api_key for the chosen models are injected into the
    delivery blob, not here. Reused by job dispatch AND session attach.
    """
    out: dict[str, Any] = {}
    user_settings: dict[str, Any] = {}
    if user_id:
        user_settings = await dependencies.store.get_user_settings(str(user_id)) or {}
    chat = user_settings.get(
        "default_model"
    ) or await dependencies.store.resolve_default_for_capability("chat")
    aux = user_settings.get(
        "default_auxiliary_model"
    ) or await dependencies.store.resolve_default_for_capability("auxiliary")
    if chat:
        out.setdefault("llm", {})["model"] = chat
    reasoning = user_settings.get("default_reasoning_level")
    if reasoning:
        # Account reasoning is a gap-filler like the account model: the mode
        # base supplies the system floor, while an expert/request pin must win.
        out.setdefault("llm", {})["reasoning_level"] = reasoning
    if aux:
        out.setdefault("auxiliary", {})["model"] = aux
    return out


async def prefetch_roster_refs(
    *,
    expert_row: dict[str, Any] | None = None,
    overrides: Iterable[dict[str, Any] | None] = (),
    user_id: str | None = None,
    project_ids: list[str] | None = None,
    dependencies: "SessionConfigDependencies",
) -> dict[str, dict[str, Any]]:
    """``{expert_selector: row}`` for each stored Expert a config's ``subagents.roster``
    names by ``$ref`` — the rows ``resolve_config(db_refs=...)`` materialises
    (``src/core/subagent_roster.py``; the resolver itself never touches the DB,
    and matches the keys case-insensitively).

    Two trust levels, by layer. A ref inside the EXPERT ROW's fragment was
    checked against its author at save (``_require_visible_roster_refs``) and
    the row is the authority: fetched by id. A ref inside an OVERRIDE layer
    (the job / thread ``config_override``, a project link's override) was
    never save-checked against any DB row, so it is fetched with the RUNNER's
    visibility — an override cannot pull another user's private expert into
    a job. Whatever is missing or invisible is simply absent from the map: the
    resolver drops that entry, logs, and records ``agent._roster_warnings``;
    dispatch never fails a job over its roster (U1 B.3).

    Installed name selectors use the canonical shared Catalog revision. Missing
    names are recorded as empty entries, so resolution drops them without reading
    an obsolete file. No database call occurs when no layer names any ref.
    """
    from shared.runtime.core.subagent_roster import (
        collect_roster_db_refs,
        collect_roster_named_refs,
    )

    expert_refs: set[str] = set()
    named_refs: set[str] = set()
    if expert_row:
        fragment = expert_row.get("config") or {}
        if isinstance(fragment, str):
            try:
                fragment = json.loads(fragment)
            except ValueError:
                fragment = {}
        expert_refs = collect_roster_db_refs(fragment)
        named_refs |= collect_roster_named_refs(fragment)
    override_refs: set[str] = set()
    for layer in overrides:
        override_refs |= collect_roster_db_refs(layer)
        named_refs |= collect_roster_named_refs(layer)
    if not expert_refs and not override_refs and not named_refs:
        return {}

    db_refs: dict[str, dict[str, Any]] = {}
    for ref in sorted(expert_refs):
        row = await dependencies.store.get_expert_by_id(ref)
        if row:
            db_refs[ref] = row
        else:
            logger.warning(
                "Roster prefetch: expert fragment names unknown expert %s", ref
            )
    pending = sorted(override_refs - set(db_refs))
    if pending:
        is_admin = False
        if user_id:
            runner = await dependencies.store.get_user(user_id)
            is_admin = bool((runner or {}).get("is_admin"))
        for ref in pending:
            if user_id:
                row = await dependencies.store.get_expert_visible_by_id(
                    ref,
                    user_id=user_id,
                    project_ids=list(project_ids or []),
                    is_admin=is_admin,
                )
            else:
                row = await dependencies.store.get_expert_by_id(ref)
            if row:
                db_refs[ref] = row
            else:
                logger.warning(
                    "Roster prefetch: override names expert %s that is unknown "
                    "or not visible to user %s",
                    ref,
                    user_id,
                )
    if named_refs:
        from orchestrator.services.manifest_experts import bundled_expert_for_execution

        for ref in sorted(named_refs):
            try:
                row = await bundled_expert_for_execution(dependencies.store, ref)
            except HTTPException as exc:
                if exc.status_code != 409:
                    raise
                row = None
            # Presence is intentional: the shared loader must not restore a
            # retired canonical definition from the installed image's file.
            db_refs[ref] = row or {}
    return db_refs


async def resolve_session_account_defaults(
    user_id: str | None,
    all_user_settings: dict[str, Any] | None = None,
    *,
    dependencies: "SessionConfigDependencies",
) -> dict[str, Any]:
    """Account-level session fallbacks, always below the selected expert."""
    out = await resolve_default_models(user_id, dependencies=dependencies)
    if not user_id:
        return out
    settings = (
        all_user_settings
        if all_user_settings is not None
        else (await dependencies.store.get_user_settings(str(user_id)) or {})
    )
    persistent = (settings or {}).get("persistent_agent") or {}
    layer: dict[str, Any] = {}
    if persistent.get("model"):
        layer["llm"] = {"model": persistent["model"]}
    interactive: dict[str, Any] = {}
    for key in ("permission_mode", "idle_timeout_minutes"):
        if persistent.get(key) is not None:
            interactive[key] = persistent[key]
    if interactive:
        layer["interactive"] = interactive
    headless: dict[str, Any] = {}
    if persistent.get("headless_mode"):
        headless["mode"] = persistent["headless_mode"]
    if persistent.get("headless_attention_sleep_minutes") is not None:
        headless["attention_sleep_minutes"] = int(
            persistent["headless_attention_sleep_minutes"]
        )
    if headless:
        layer["headless"] = headless
    return deep_merge_dicts(out, layer)


async def account_defaults_layer(
    user_id: str | None, expert_type: str, *, dependencies: "SessionConfigDependencies"
) -> dict[str, Any]:
    """The account fallback layer that create/dispatch feeds ``resolve_config``.

    Single source for "what the caller's account contributes below the expert",
    so the create forms can render the same resolved config the server will
    actually build. Sessions get the full session layer (model, interactive
    and headless preferences); workers get only the default-model floor,
    matching the ``base_defaults`` the job dispatcher passes. Neither layer
    carries a workspace tier any more — Slice A2b moved that to the defaults
    chain (``select_execution_workspace``), so it is no longer an account
    preference. Both mirror the exact calls in ``create_thread`` and the
    dispatcher.

    Returns ``{}`` for an anonymous caller: with no account there is no layer,
    and the framework base is already the honest answer.
    """
    if not user_id:
        return {}
    if expert_type == "session":
        return await resolve_session_account_defaults(
            user_id, dependencies=dependencies
        )
    return await resolve_default_models(user_id, dependencies=dependencies)


async def acknowledged_grant_strip(
    metadata: dict[str, Any],
    *,
    user_id: str | None,
    project_id: str | None,
    dependencies: "SessionConfigDependencies",
) -> Callable[[dict], dict] | None:
    """Build ``resolve_config``'s ``grant_strip`` hook from a thread's
    acknowledged grant drift, or ``None`` when there is nothing to strip (no
    acknowledgment, or ``_resolve_runner_grants`` says admin bypass).

    Shared by :func:`resolve_session_config` (the delivered blob) and
    :func:`_merged_session_tool_policy` (the tool-groups report), so the two
    can never disagree about which acknowledged grants are currently still
    violated — before this helper existed, the report used a bare
    ``resolve_config`` call with no strip at all, so an acknowledged
    ``catalog_authoring`` violation (say) still read "on" in the settings
    view after the delivered blob had already dropped it. See
    knowledge-history/done/session_config_drift_resume.md §3.3.
    """
    ack_grant_keys = acknowledged_grant_keys(metadata)
    if not ack_grant_keys:
        return None
    grants_for_strip = await dependencies.resolve_runner_grants(
        runner_user_id=user_id,
        project_ids=[project_id] if project_id else [],
    )
    if grants_for_strip is None:
        return None
    return lambda fragment: strip_acknowledged_grants(
        fragment, grants_for_strip, ack_grant_keys
    )


async def resolve_session_config(
    thread: dict[str, Any],
    metadata: dict[str, Any],
    *,
    config_override: dict[str, Any] | None = None,
    status: dict[str, Any] | None = None,
    resolve_base_when_experts_disabled: bool = False,
    dependencies: "SessionConfigDependencies",
) -> dict[str, Any] | None:
    """Deliver the current admitted session generation with live authorization.

    New sessions read their canonical rendered configuration. A settings update
    publishes another generation; an attached turn retains its delivered copy.
    Historical rows without a canonical record retain their old resolver until
    imported. ``config_override`` carries authorized transient workspace and
    connector-tool bindings, not a replacement for frozen model/prompt settings.
    """
    execution = await read_execution(dependencies.store, "Session", str(thread["id"]))
    if execution is not None:
        try:
            resolved, policy = srw_snapshot_config(execution)
            user_id = str(thread["user_id"]) if thread.get("user_id") else None
            project_id = str(thread["project_id"]) if thread.get("project_id") else None
            # Ordered controls have their own durable inbox and materialized
            # columns. Check exactly the values the final attach will deliver,
            # including an acknowledged control newer than this config revision.
            interactive = {
                "permission_mode": str(thread.get("permission_mode") or "supervised")
            }
            if thread.get("narration_mode") is not None:
                interactive["narration_mode"] = str(thread["narration_mode"])
            for fragment in (resolved["agent"], policy):
                fragment["interactive"] = {
                    **(fragment.get("interactive") or {}),
                    **interactive,
                }
            strip = await acknowledged_grant_strip(
                metadata,
                user_id=user_id,
                project_id=project_id,
                dependencies=dependencies,
            )
            resolved, policy = apply_srw_delivery_bindings(
                resolved,
                policy,
                config_override,
                grant_strip=strip,
            )
            resolved["agent"].update(session_tool_group_disabled_markers(policy))
            await dependencies.enforce_dispatch_grants(
                policy,
                runner_user_id=user_id,
                project_ids=[project_id] if project_id else [],
            )
            knowledge_project_ids = (
                [project_id]
                if project_id
                else await dependencies.thread_project_ids(str(thread["id"]))
            )
            include_kb_profile = await dependencies.thread_has_knowledge_scope(
                project_ids=knowledge_project_ids,
                datasource_ids=metadata.get("datasource_ids"),
            )
            delivered = await inject_blob_credentials(
                resolved,
                lambda co: dependencies.inject_thread_dispatch_credentials(
                    co,
                    user_id=user_id,
                    project_id=project_id,
                    include_kb_profile=include_kb_profile,
                ),
            )
            # Included in the stateless attach fingerprint, so a queued turn
            # sees a new generation while an in-flight turn keeps its copy.
            delivered["execution_snapshot"] = {
                "id": str(execution["id"]),
                "generation": execution["generation"],
                "revision": execution["revision"],
            }
            if status is not None:
                status["state"] = "ok"
            return delivered
        except GrantDenied as denied:
            if status is not None:
                status.update(state="denied", grant_violations=list(denied.violations))
            raise
        except Exception:
            if status is not None:
                status["state"] = "error"
            raise
    experts_enabled = (
        dependencies.is_experts_db_enabled()
        and await dependencies.user_experts_enabled()
    )
    if not experts_enabled and not (
        resolve_base_when_experts_disabled or (status or {}).get("_capture_manifest")
    ):
        if status is not None:
            status["state"] = "disabled"
        return None
    try:
        user_id = str(thread["user_id"]) if thread.get("user_id") else None
        project_id = str(thread["project_id"]) if thread.get("project_id") else None
        # The resident session-memory drain also serves deployments where the
        # expert catalog is disabled.  Its explicit opt-in resolves only the
        # bundled session base + account/request layers; a dormant expert id
        # must not become active merely because an outbox worker needs fresh
        # credentials.  Normal attach callers keep the early-return above.
        expert_id = metadata.get("expert_id") if experts_enabled else None
        expert_row = (
            await dependencies.store.get_expert_by_id(str(expert_id))
            if expert_id
            else None
        )
        base = canonical_config_name(thread.get("config_name") or "session_base")
        if looks_like_uuid(base):
            # Sentinel / cockpit-conflated expert UUID → resolve onto the real
            # session base; the expert is delivered via expert_id, not the name.
            base = "session_base"
        request_override = (
            config_override
            if config_override is not None
            else (metadata.get("config_override") or None)
        )
        base_defaults = await resolve_session_account_defaults(
            user_id, dependencies=dependencies
        )
        # A Session that predates snapshots, and never materialized a
        # backend into its config_override, still needs a tier to render.
        # Slice A2b: that tier comes from the defaults chain, not an account
        # preference — templates still reach Sessions only through
        # snapshots, so this renders the tier alone.
        if not ((request_override or {}).get("workspace") or {}).get("backend"):
            from orchestrator.services.workspace_defaults_resolution import (
                resolve_workspace_defaults,
            )
            from shared.workspace_defaults import MODE_BACKEND

            resolution = await resolve_workspace_defaults(
                dependencies.store, role="session", project_id=project_id
            )
            base_defaults = {
                **base_defaults,
                "workspace": {"backend": MODE_BACKEND[resolution.mode]},
            }
        project_overrides = None
        if project_id and expert_id:
            link = await dependencies.store.get_project_expert_link(
                project_id=project_id, expert_id=str(expert_id)
            )
            if link:
                project_overrides = link.get("config_override") or None
                if isinstance(project_overrides, str):
                    project_overrides = json.loads(project_overrides)
        _cap: dict = {}
        _skills_payload = await dependencies.gather_in_scope_skills(
            user_id, [project_id] if project_id else None
        )
        # Per-model registry overrides must reach the matrix as explicit llm
        # keys, else the blob bakes the family window and the admin
        # context_window cap is silently dropped (see
        # _seed_registry_model_overrides).
        request_override = await dependencies.seed_registry_model_overrides(
            request_override, user_id=user_id
        )
        # Grants resolved BEFORE resolve_config (not after) so the strip can run
        # as its grant_strip hook, on the SAME `data` the delivered blob is built
        # from — not just on the detached capture the PDP evaluates. Stripping
        # only the capture leaves the delivered blob carrying the very
        # capability the grant revoked (round-1 finding). None when there is
        # nothing acknowledged, or when _resolve_runner_grants says admin.
        # Shared with the tool-groups report — see acknowledged_grant_strip.
        _grant_strip = await acknowledged_grant_strip(
            metadata, user_id=user_id, project_id=project_id, dependencies=dependencies
        )
        roster_refs = await prefetch_roster_refs(
            expert_row=expert_row,
            overrides=(project_overrides, request_override),
            user_id=user_id,
            project_ids=[project_id] if project_id else [],
            dependencies=dependencies,
        )
        resolved = resolve_config(
            base_config_name=base,
            base_defaults=base_defaults,
            expert_row=expert_row,
            project_overrides=project_overrides,
            request_override=request_override,
            expert_type="session",
            capture=_cap,
            skills=_skills_payload,
            grant_strip=_grant_strip,
            db_refs=roster_refs,
        )
        # Bound skills are delivered deterministically (instructions channel);
        # strip them from the model-invoked catalog so they aren't double-offered.
        from shared.runtime.core.skill_resolution import filter_bound_skills

        filter_bound_skills(resolved)
        # Empty session-control groups are meaningful at every layer.  Derive
        # runtime injection markers from the fully merged config, so a safe base
        # or expert can disable Automations & Loops without requiring the create
        # form to redundantly submit an empty request override.
        session_tool_markers = session_tool_group_disabled_markers(
            _cap["merged_fragment"]
        )
        if session_tool_markers:
            resolved.setdefault("agent", {}).update(session_tool_markers)
        # Session dispatch PEP (decision 9): the merged config — including
        # interactive.permission_mode and any persistent_agent keys baked into
        # config_override — must fit the runner's grants. GrantDenied escapes the
        # generic except below (fail closed: never deliver the unvetted override).
        # _cap["merged_fragment"] already reflects the grant_strip hook above
        # (same `data` the delivered blob was built from); this re-check stays
        # authoritative — it re-runs evaluate() on whatever that hook returned.
        # Historical attachments also overlay materialized controls. Validate
        # those exact values before capturing the first canonical generation.
        materialized = {}
        if "permission_mode" in thread:
            materialized["permission_mode"] = str(
                thread.get("permission_mode") or "supervised"
            )
        if thread.get("narration_mode") is not None:
            materialized["narration_mode"] = str(thread["narration_mode"])
        if materialized:
            for fragment in (resolved["agent"], _cap["merged_fragment"]):
                fragment["interactive"] = {
                    **fragment.get("interactive", {}),
                    **materialized,
                }
        await dependencies.enforce_dispatch_grants(
            _cap["merged_fragment"],
            runner_user_id=user_id,
            project_ids=[project_id] if project_id else [],
        )
        knowledge_project_ids = (
            [project_id]
            if project_id
            else await dependencies.thread_project_ids(str(thread["id"]))
        )
        include_kb_profile = await dependencies.thread_has_knowledge_scope(
            project_ids=knowledge_project_ids,
            datasource_ids=metadata.get("datasource_ids"),
        )
        if status is not None and status.get("_capture_manifest"):
            from orchestrator.services.manifest_experts import installed_srw_image
            from orchestrator.services.manifest_session_delivery import (
                prepare_delivery_snapshot,
            )

            status["_manifest_snapshot"] = prepare_delivery_snapshot(
                thread,
                metadata,
                resolved,
                _cap["merged_fragment"],
                image=getattr(dependencies.store, "manifest_runtime_image", None)
                or installed_srw_image(),
                config_name=base,
                expert=expert_row,
                refs=roster_refs,
            )
        delivered = await inject_blob_credentials(
            resolved,
            lambda co: dependencies.inject_thread_dispatch_credentials(
                co,
                user_id=user_id,
                project_id=project_id,
                include_kb_profile=include_kb_profile,
            ),
        )
        if status is not None:
            status["state"] = "ok"
        return delivered
    except GrantDenied as gd:
        if status is not None:
            status["state"] = "denied"
            # The drift collector reads these rather than re-merging the config
            # itself — one merge implementation, so the dialog can never promise
            # something different from what attach enforces.
            status["grant_violations"] = list(gd.violations)
        raise
    except Exception:
        logger.exception(
            "Session resolve failed for thread %s; falling back to config_name",
            thread.get("id"),
        )
        if status is not None:
            status["state"] = "error"
        return None


async def require_supported_protected_session_class(
    thread: dict[str, Any],
    metadata: dict[str, Any],
    *,
    dependencies: "SessionConfigDependencies",
) -> None:
    """Fail closed when a protected row selects an unsupported runtime class.

    This check intentionally runs before Resume's ended->created CAS as well
    as before provisioning.  Older rows can predate create-time class
    materialization, so only the fully resolved expert/account/request stack
    is authoritative.
    """

    if protected_cloud_marker_state(metadata) != "on":
        return
    class_status: dict[str, Any] = {}
    try:
        effective_config = await resolve_session_config(
            thread,
            metadata,
            status=class_status,
            resolve_base_when_experts_disabled=True,
            dependencies=dependencies,
        )
        if effective_config is None or protected_cloud_officer_active(effective_config):
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "protected_cloud_unsupported_session_class",
                    "message": (
                        "Protected cloud sessions are not supported for the "
                        "background Officer runtime."
                    ),
                },
            )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "protected_cloud_session_class_unverified",
                "message": "Protected cloud session class could not be verified.",
            },
        ) from exc


async def session_grant_violations(
    thread: dict[str, Any], *, dependencies: "SessionConfigDependencies"
) -> list[str]:
    """Pre-flight the capability grants for a session's resolved config.

    Returns the violation messages (``[]`` = allowed, or enforcement off /
    non-grant resolve error → fail-open, same as the attach path), running the
    SAME PDP as attach (``resolve_session_config`` → ``_enforce_dispatch_grants``)
    but discarding the resolved blob. Lets the provisioning paths
    (``provision_or_assign`` and ``routers/sessions._do_prepare``) reject a
    never-startable session up front — emitting ``session.lifecycle: failed``
    with the real reason — instead of spawning a dedicated agent pod that 403s at
    the workspace endpoint and exits "to be rebound" (a permanent grant denial is
    not recoverable by a rebind), leaving the cockpit to poll ``/connection``
    until its ~5m40s ready timeout.
    See knowledge-base/knowledge/issues/session_permission_mode_grant_denied_ready_timeout.md.
    """
    metadata = thread.get("metadata") or {}
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except (json.JSONDecodeError, TypeError):
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "session_metadata_malformed",
                    "message": "This session's stored state is invalid.",
                },
            )
    protected_marker = protected_cloud_marker_state(metadata)
    if protected_marker == "malformed":
        raise HTTPException(
            status_code=409,
            detail={
                "code": "protected_cloud_malformed",
                "message": "Protected cloud session state is invalid.",
            },
        )
    if protected_marker == "on" and thread_workspace_backend(thread) != "sandbox":
        raise HTTPException(
            status_code=409,
            detail={
                "code": "protected_cloud_unsupported_workspace",
                "message": "Protected cloud sessions require the Container tier.",
            },
        )
    await require_supported_protected_session_class(
        thread, metadata, dependencies=dependencies
    )
    try:
        await resolve_session_config(thread, metadata, dependencies=dependencies)
        return []
    except GrantDenied as gd:
        return list(gd.violations)


async def session_endpoint_violations(
    thread: dict[str, Any], *, dependencies: "SessionConfigDependencies"
) -> list[str]:
    """Pre-flight the model-role transports for a session's resolved config.

    Returns per-role reasons (``[]`` = every configured role has a usable
    transport, or experts off / resolve error → fail-open, same as the attach
    path). Runs the SAME resolve+inject as attach (``resolve_session_config``,
    which credential-injects the delivery blob) and then checks each configured
    role (primary llm, auxiliary, embedding, rerank) against the loader's raise
    conditions (``shared.runtime.core.transport_resolution``). The reranker
    binds RERANK_BASE_URL when the ``rerank`` catalog slot delivered one and
    otherwise rides the embedding endpoint, so both transports are checked.

    Lets ``provision_or_assign`` / ``routers/sessions._do_prepare`` reject a
    never-startable session up front — emitting ``session.lifecycle: failed``
    with the real reason — instead of spawning a pod that crashes at agent
    startup (e.g. the memory reranker with no reachable endpoint), releasing the
    workspace, and hanging the cockpit on ``/connection`` until its ready
    timeout. See knowledge-base/knowledge/issues/openrouter_auxiliary_crashes_session_via_memory_reranker.md.
    """
    metadata = thread.get("metadata") or {}
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except (json.JSONDecodeError, TypeError):
            metadata = {}
    try:
        delivered = await resolve_session_config(
            thread, metadata, dependencies=dependencies
        )
    except GrantDenied:
        # The grant pre-flight owns this rejection; don't double-report.
        return []
    except Exception:
        # Resolve failure → agent falls back to config_name (fail-open), same as
        # session_grant_violations.
        return []
    if not delivered:
        return []  # experts disabled → no injection → nothing to validate

    from shared.runtime.core.transport_resolution import (
        embedding_role_violation,
        llm_role_violation,
        rerank_role_violation,
    )

    agent_blob = delivered.get("agent") or {}
    env_keys = agent_blob.get("env_keys") or {}
    # The orchestrator injects some provider keys into env_keys (e.g.
    # OPENROUTER_API_KEY) rather than onto the role section; combine both so the
    # key check matches what the agent's loader will actually see.
    combined_env = dict(os.environ)
    combined_env.update({k: v for k, v in env_keys.items() if v is not None})

    violations: list[str] = []
    for role in ("llm", "auxiliary"):
        section = agent_blob.get(role)
        if isinstance(section, dict):
            reason = llm_role_violation(role, section, env=combined_env)
            if reason:
                violations.append(reason)
    emb_reason = embedding_role_violation(env_keys)
    if emb_reason:
        violations.append(emb_reason)
    rerank_reason = rerank_role_violation(env_keys)
    if rerank_reason:
        violations.append(rerank_reason)
    return violations


def endpoint_violations_detail(violations: list[str]) -> str:
    return "session cannot start — unusable model transport: " + "; ".join(violations)


__all__ = [
    "SessionConfigDependencies",
    "account_defaults_layer",
    "acknowledged_grant_strip",
    "endpoint_violations_detail",
    "prefetch_roster_refs",
    "require_supported_protected_session_class",
    "resolve_default_models",
    "resolve_session_account_defaults",
    "resolve_session_config",
    "session_endpoint_violations",
    "session_grant_violations",
]
