"""Session tool policy: what the agent will bind, and what a request may ask for.

Extracted verbatim from ``orchestrator.main`` (R1.B05 lane P, census groups
``R_POLICY`` and ``R_SESSION_POLICY``). Three related jobs live here, and they
are one module because they share a vocabulary — ``tools.<category>`` — and
would drift apart if split:

* **Prediction.** :func:`merged_session_tool_policy` /
  :func:`merged_session_tool_groups` run the SAME ``resolve_config`` layering
  the delivered blob does, minus the layers that provably cannot reach
  ``tools``; :func:`legacy_session_tool_policy` /
  :func:`legacy_session_tool_groups` predict the experts-off agent path, whose
  append rule answers the OPPOSITE for an unset group. Both are *predictions*
  and are served as such (D6).
* **The delegation gate.** :func:`apply_delegation_gate` is the one statement
  that ``delegate_agent`` needs ``delegation.enabled`` true IN ADDITION to the
  names in ``tools.delegation``. Both policy functions call it; collapsing it
  into "the names are enough" would make the pane offer a tick the tool factory
  refuses to honour.
* **The write boundary.** :func:`validated_tool_overrides` is the single
  vocabulary for session create, session runtime update and job create: every
  category is checked against the registry and anything the boundary will not
  honour is REJECTED, never silently dropped. It is a shape-and-vocabulary
  gate; capability grants stay the authorization decision point elsewhere.

Copy semantics moved unchanged: :func:`with_validated_tool_overrides` returns
the caller's object untouched when there is no ``tools`` key, and a NEW dict
(``{**config_override, "tools": ...}``) when there is; the input is never
mutated. :func:`apply_delegation_gate` is the opposite — it mutates its
argument in place and returns ``None``.

The ``explicitly_disabled`` family and
:func:`session_tool_group_disabled_markers` key on ``== []`` — an *explicit
empty list*, which is an authoritative "off" and not the same as an absent key.
Truthiness would erase that distinction.

No application collaborator is needed: ``resolve_config`` reads YAML off disk,
so every function here is SYNCHRONOUS and callers run them in
``asyncio.to_thread``. The module logger is its own; nothing is captured from
``orchestrator.main``.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Callable, Optional

from fastapi import HTTPException

from orchestrator.services.config_resolver import resolve_config
from shared.runtime.core.loader import (
    ROLE_ROOTS,
    load_and_merge_config,
    resolve_config_path,
)
from shared.runtime.core.session_tool_overrides import (
    LEGACY_APPENDED_GROUPS,
    SESSION_TOOL_OVERRIDE_NAMES,
    session_tool_group_enablement,
)
from shared.runtime.core.tool_policy import (
    ToolPolicyError,
    validate_tool_override_fragment,
)
from shared.runtime.core.tool_report import layer_provenance

logger = logging.getLogger(__name__)


def merged_session_tool_groups(
    *,
    base_config_name: str,
    expert_row: dict[str, Any] | None,
    project_overrides: dict[str, Any] | None,
    request_override: dict[str, Any] | None,
) -> dict[str, bool]:
    """The closed session tool groups as the RESOLVED path will bind them.

    SYNCHRONOUS — ``resolve_config`` parses YAML off disk and reads prompt
    files. Call it from ``asyncio.to_thread`` so it cannot block the loop.

    Runs the SAME ``resolve_config`` layering as ``_resolve_session_config``
    minus everything that provably cannot reach ``tools``. Anything skipped
    here that CAN move a tool group is a correctness bug, so the ledger is
    explicit:

    - ``base_defaults`` (``_resolve_session_account_defaults``) emits only
      ``llm``/``auxiliary``/``interactive``/``headless``/``workspace``, and
      the settings matrix its model choice feeds
      (``shared.runtime.core.loader._apply_settings_matrix``) writes only
      ``llm``, ``limits``, ``shell.mode`` and the family's session cap
      (``delegation.family_session_max_concurrent``, never a tool gate).
      Saves 2 round trips.
    - ``_seed_registry_model_overrides`` only ``setdefault``s ``llm.*``.
    - ``skills`` is written to the returned blob AFTER ``resolve_config`` takes
      the ``capture`` deepcopy, so it cannot appear in the merged fragment.
    - ``_enforce_dispatch_grants`` is a PDP, not a transform. Skipping it means
      a grant-denied session still gets a resolved-shaped answer; that session
      never attaches, and this read surface must not become a second 403.
    - ``_thread_project_ids`` / ``_thread_has_knowledge_scope`` /
      ``inject_blob_credentials`` all act on the post-capture delivery copy and
      inject transport/KB-profile keys only.
    - The attach-time ``config_override`` (warm-pool) differs from the stored
      one only by ``workspace.*`` and datasource categories
      (``graph``/``sql``/``mongodb``/``webdav``/``email``/``mcp``) — disjoint
      from these groups.
    - ``grant_strip`` — the acknowledged-grant-downgrade hook
      ``_resolve_session_config`` passes to ``resolve_config`` — is skipped
      HERE too, and it does NOT belong on the "safe to skip" side of this
      ledger the way the entries above do: an acknowledged grant violation
      CAN delete a closed group's only enabling key (``catalog_authoring`` is
      both a closed session tool group and a key ``strip_to_grants`` drops).
      This function has no caller today that carries a thread/metadata to
      build the hook from, so it is left unthreaded rather than faked.
      :func:`merged_session_tool_policy` — whose two HTTP callers DO have a
      thread — accepts and forwards ``grant_strip`` instead; see
      ``_acknowledged_grant_strip``. If this function grows a caller that
      needs a drift-aware answer, thread it through the same way rather than
      silently reporting the pre-strip merge.

    Kept, because each CAN set ``tools.*``: the base config name, the expert
    row, the project-expert link override, and the request override (which is
    where a live Settings toggle lands, so this stays fresh after a toggle).
    """
    merged, _provenance = merged_session_tool_policy(
        base_config_name=base_config_name,
        expert_row=expert_row,
        project_overrides=project_overrides,
        request_override=request_override,
    )
    return session_tool_group_enablement({"tools": merged})


def merged_session_tool_policy(
    *,
    base_config_name: str,
    expert_row: dict[str, Any] | None,
    project_overrides: dict[str, Any] | None,
    request_override: dict[str, Any] | None,
    expert_type: str = "session",
    grant_strip: Callable[[dict], dict] | None = None,
    db_refs: dict[str, Any] | None = None,
    capture: dict[str, Any] | None = None,
) -> tuple[dict[str, list[str]], dict[str, str]]:
    """``(merged tools mapping, category -> deciding layer)``.

    ``db_refs`` are the prefetched DB expert rows a ``subagents.roster``
    names (:func:`_prefetch_roster_refs`); without them a DB ``$ref`` entry
    drops out of the resolve. ``capture`` is the caller's dict for
    ``resolve_config``'s ``merged_fragment`` — the tool-groups endpoints read
    the materialised roster off it to report what a Delegation tick reaches.

    SYNCHRONOUS — see :func:`merged_session_tool_groups`, whose skip ledger
    this shares because it runs the same resolve, with ONE exception:
    ``grant_strip`` is NOT skipped here. A caller that holds a thread and its
    metadata (the two tool-groups HTTP endpoints) can build the identical
    hook ``_resolve_session_config`` passes to ``resolve_config`` — see
    ``_acknowledged_grant_strip`` — so an acknowledged grant's downgrade
    shows up in the reported policy instead of only in the delivered blob.

    This is a *prediction* and is only ever served as one. It cannot see the
    agent's runtime injection layer or its backend capability gate, so it is
    structurally weaker than the agent's own report — not merely staler. D6.

    Provenance exploits ``deep_merge``'s list-replacement rule: for
    ``tools.<category>`` the most specific layer naming the category owns it
    outright, so "which layer decided it" is the last hit down the chain.

    ``expert_type`` selects which prompt/config leaf ``resolve_config`` reads and
    was hardcoded to ``"session"`` until 2026-08-04, which is why job create had
    no honest toolset view: asking it for a worker prediction returned a
    session's answer, so the surface was left rendering six static rows instead.
    """
    if capture is None:
        capture = {}
    resolve_config(
        base_config_name=base_config_name,
        base_defaults=None,
        expert_row=expert_row,
        project_overrides=project_overrides,
        request_override=request_override,
        expert_type=expert_type,
        capture=capture,
        grant_strip=grant_strip,
        db_refs=db_refs,
    )
    merged_fragment = capture.get("merged_fragment") or {}
    merged = merged_fragment.get("tools")
    merged = merged if isinstance(merged, dict) else {}
    # ``delegate_agent`` and its control plane are ``grant: explicit`` twice
    # over: the factory needs the names in ``tools.delegation`` AND
    # ``delegation.enabled`` true. A prediction that reports the names while
    # the gate is off is "5 predicted" for tools the agent will refuse to bind
    # — the pane then offers a tick that changes nothing. Report the category
    # as it will bind: empty (off, settable — ticking now writes the gate too).
    apply_delegation_gate(merged, merged_fragment.get("delegation"))

    base_fragment: dict[str, Any] = {}
    try:
        # The same role re-rooting resolve_config just applied: a root name is
        # answered by the role's base, a bundled expert's chain is re-rooted —
        # otherwise provenance would blame a base layer that was never merged.
        base_path, _ = resolve_config_path(base_config_name)
        base_fragment = (
            load_and_merge_config(
                base_path,
                role=expert_type if expert_type in ROLE_ROOTS else None,
            )
            or {}
        )
    except Exception:
        logger.warning(
            "Tool-group provenance could not load base config '%s'; the base "
            "layer will read as unset",
            base_config_name,
        )

    expert_fragment = (expert_row or {}).get("config") or {}
    if isinstance(expert_fragment, str):
        try:
            expert_fragment = json.loads(expert_fragment)
        except (json.JSONDecodeError, TypeError):
            expert_fragment = {}

    provenance = layer_provenance(
        [
            ("base", base_fragment),
            ("expert", expert_fragment),
            ("project", project_overrides),
            ("request", request_override),
        ]
    )
    return merged, provenance


def legacy_session_tool_groups(
    base_config_name: str,
    request_override: dict[str, Any] | None,
) -> dict[str, bool]:
    """The closed groups as the LEGACY (experts-off) agent path will bind them.

    SYNCHRONOUS — loads the base YAML. Call via ``asyncio.to_thread``.

    This path answers the OPPOSITE of the resolved path for an unset group.
    ``persistent_app._apply_session_tool_group_markers`` sets a disable marker
    only on an explicit ``config_override.tools.<group> == []``, and
    ``persistent_session._setup_tools`` then APPENDS the canonical lists for
    every group in ``LEGACY_APPENDED_GROUPS`` whenever the marker is absent —
    so an unset group is ENABLED regardless of the base YAML's ``[]``.

    ``canvas`` is asymmetric: its branch is strip-only with no append, so it
    additionally requires a non-empty ``tools.canvas`` in the loaded base.

    Fidelity caveat: when the thread has no ``config_name`` the agent falls back
    to the POD's boot YAML, which the orchestrator cannot observe; we proxy with
    ``session_base``. Affects ``canvas`` only, and only when experts are off.
    """
    explicit = (request_override or {}).get("tools")
    explicit = explicit if isinstance(explicit, dict) else {}
    groups = {group: explicit.get(group) != [] for group in LEGACY_APPENDED_GROUPS}
    canvas_names: Any = explicit.get("canvas")
    if canvas_names is None:
        try:
            base_path, _ = resolve_config_path(base_config_name)
            base_tools = (load_and_merge_config(base_path) or {}).get("tools") or {}
            canvas_names = base_tools.get("canvas")
        except Exception:
            logger.warning(
                "Legacy tool-group probe could not load base config '%s'; "
                "reporting canvas as enabled",
                base_config_name,
            )
            canvas_names = None
        if canvas_names is None:
            canvas_names = ["_unknown_base_assume_enabled"]
    groups["canvas"] = bool(canvas_names)
    # Groups the legacy agent never learned to append (today: catalog_authoring).
    # No append branch means no "unset reads as enabled" inversion — they follow
    # the resolved rule, so only an explicit non-empty request turns them on.
    # Reporting otherwise would predict a write capability the agent cannot bind.
    for group in SESSION_TOOL_OVERRIDE_NAMES:
        groups.setdefault(group, bool(explicit.get(group)))
    return groups


def legacy_session_tool_policy(
    base_config_name: str,
    request_override: dict[str, Any] | None,
) -> tuple[dict[str, list[str]], dict[str, str]]:
    """``(predicted tools mapping, provenance)`` for the LEGACY agent path.

    SYNCHRONOUS — loads the base YAML. Call via ``asyncio.to_thread``.

    Same prediction status as :func:`merged_session_tool_policy`, plus the
    legacy path's own asymmetry: the compatibility groups are APPENDED by
    ``persistent_session._load_tools_for_backend`` whenever no explicit ``[]``
    disable marker is present, so an unset group is ENABLED here and DISABLED
    on the resolved path. :func:`legacy_session_tool_groups` is the pinned
    statement of that rule; this reuses it rather than restating it.
    """
    base_tools: dict[str, Any] = {}
    base_delegation: Any = None
    try:
        base_path, _ = resolve_config_path(base_config_name)
        base_config = load_and_merge_config(base_path) or {}
        base_tools = base_config.get("tools") or {}
        base_delegation = base_config.get("delegation")
    except Exception:
        logger.warning(
            "Legacy tool-policy probe could not load base config '%s'", base_config_name
        )
    override_tools = (request_override or {}).get("tools")
    override_tools = override_tools if isinstance(override_tools, dict) else {}

    merged: dict[str, list[str]] = {}
    for key, value in {**base_tools, **override_tools}.items():
        merged[key] = list(value) if isinstance(value, (list, tuple)) else []

    # The append rule, expressed once: an enabled closed group carries its
    # canonical names even when the base ships the key empty.
    for group, enabled in legacy_session_tool_groups(
        base_config_name, request_override
    ).items():
        if enabled and not merged.get(group):
            merged[group] = sorted(SESSION_TOOL_OVERRIDE_NAMES[group])
        elif not enabled:
            merged[group] = []

    provenance = layer_provenance(
        [("base", {"tools": base_tools}), ("request", {"tools": override_tools})]
    )
    for group in SESSION_TOOL_OVERRIDE_NAMES:
        provenance.setdefault(group, "runtime")
    # Same explicit-grant gate as the resolved path; the request layer's
    # ``delegation`` block wins over the base's, as deep_merge would have it.
    request_delegation = (request_override or {}).get("delegation")
    apply_delegation_gate(
        merged,
        request_delegation if isinstance(request_delegation, dict) else base_delegation,
    )
    return merged, provenance


def apply_delegation_gate(
    merged_tools: dict[str, list[str]], delegation_block: Any
) -> None:
    """Empty ``merged_tools["delegation"]`` unless ``delegation.enabled`` is
    true — the binding rule of ``agent.tools.delegation.create_delegation_tools``,
    restated for the prediction so the pane never shows names the factory
    will not build. Mutates in place; no-op when nothing is named."""
    if not merged_tools.get("delegation"):
        return
    enabled = (
        delegation_block.get("enabled") if isinstance(delegation_block, dict) else None
    )
    if enabled is not True:
        merged_tools["delegation"] = []


SESSION_TOOL_DISABLED_MARKERS = {
    "orchestrator": "_fleet_management_disabled",
    "job_control": "_job_control_disabled",
    "job_inspection": "_job_inspection_disabled",
    "agent_catalog": "_agent_catalog_disabled",
    "workflows": "_workflows_disabled",
    "canvas": "_canvas_disabled",
}


def validated_tool_overrides(
    config_override: Any,
) -> dict[str, list[str]]:
    """Validate a request's ``tools`` mapping, or 400.

    The one vocabulary for every write boundary — session create, session
    runtime update, job create.  Every category is checked against the
    registry, and anything the boundary will not honour is **rejected**, not
    dropped: the predecessor honoured four hand-curated groups and silently
    discarded the other eight the New Session form renders, so unticking
    ``research`` or ``shell`` was accepted and never applied.  Same rationale
    as ``_validated_reasoning_level`` — garbage fails loud here instead of
    disappearing.

    This is a shape-and-vocabulary gate.  Capability grants
    (``_enforce_session_create_grants`` / ``_enforce_job_create_grants``) stay
    the authorization decision point; nothing here duplicates them.
    """
    try:
        return validate_tool_override_fragment(config_override)
    except ToolPolicyError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def with_validated_tool_overrides(
    config_override: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Return ``config_override`` with its ``tools`` mapping validated in place.

    Assignment, not filtering: :func:`validated_tool_overrides` raises on
    anything it will not honour, so what comes back covers every category the
    caller wrote — normalised to the canonical ``list[str]`` the PDP and the
    loader both read.
    """
    if not isinstance(config_override, dict) or "tools" not in config_override:
        return config_override
    return {**config_override, "tools": validated_tool_overrides(config_override)}


def validated_session_fleet_tools_override(
    config_override: Any,
) -> Optional[list[str]]:
    """Extract the New Session Fleet Management tool toggle."""
    return validated_tool_overrides(config_override).get("orchestrator")


def fleet_management_explicitly_disabled(
    config_override: dict[str, Any] | None,
) -> bool:
    tools = (config_override or {}).get("tools")
    return isinstance(tools, dict) and tools.get("orchestrator") == []


def agent_catalog_explicitly_disabled(
    config_override: dict[str, Any] | None,
) -> bool:
    tools = (config_override or {}).get("tools")
    return isinstance(tools, dict) and tools.get("agent_catalog") == []


def workflows_explicitly_disabled(
    config_override: dict[str, Any] | None,
) -> bool:
    tools = (config_override or {}).get("tools")
    return isinstance(tools, dict) and tools.get("workflows") == []


def session_tool_group_disabled_markers(
    config_override: dict[str, Any] | None,
) -> dict[str, bool]:
    tools = (config_override or {}).get("tools")
    if not isinstance(tools, dict):
        return {}
    return {
        marker: True
        for group, marker in SESSION_TOOL_DISABLED_MARKERS.items()
        if tools.get(group) == []
    }


__all__ = [
    "SESSION_TOOL_DISABLED_MARKERS",
    "agent_catalog_explicitly_disabled",
    "apply_delegation_gate",
    "fleet_management_explicitly_disabled",
    "legacy_session_tool_groups",
    "legacy_session_tool_policy",
    "merged_session_tool_groups",
    "merged_session_tool_policy",
    "session_tool_group_disabled_markers",
    "validated_session_fleet_tools_override",
    "validated_tool_overrides",
    "with_validated_tool_overrides",
    "workflows_explicitly_disabled",
]
