"""Single source of agent config resolution (supersedes agent-side Decision 6).

Composes ``src/core/loader`` steps so the orchestrator produces the full, frozen
config blob the agent hydrates. Reused by job dispatch AND session attach —
identical resolution; only timing/delivery differ. See
``knowledge-base/knowledge/superpowers/specs/2026-06-17-orchestrator-resolved-config-design.md``.

Pure + synchronous: no DB, no network. The caller fetches the expert row and
passes it in; injects credentials into the *delivery* copy afterwards (the
serialized blob is already free of ``llm.api_key``); and strips any remaining
secrets with ``redact_config_override`` before persisting.
"""

from __future__ import annotations

import copy
import logging
from typing import Awaitable, Callable, Optional

from shared.runtime.core.loader import (
    INHERIT_MODEL,
    ROLE_ROOTS,
    ROOT_NAMES,
    _apply_settings_matrix,
    authored_llm_keys,
    canonical_config_name,
    deep_merge,
    ensure_phase_skill_bindings,
    load_agent_config_from_dict,
    load_and_merge_config,
    normalize_delegation_block,
    normalize_llm_tiers,
    prune_ignored_keys,
    reroot_extends,
    resolve_bundled_config_path,
    serialize_resolved_config,
    strip_loader_owned_keys,
)
from shared.runtime.core.subagent_roster import resolve_subagent_roster
from shared.runtime.core.tool_policy import normalize_tool_policy

logger = logging.getLogger(__name__)

# Prompt segments a DB/forked expert may override — one family-agnostic version
# each (model adaptation stays in the systemprompt_<family> wrapper). persona +
# instructions are v1 (migration 0028); strategic/tactical/summarization are
# Part 2 (full DB-expert prompt parity). Reused to build the ``_db_prompt_keys``
# provenance marker the render path fences on.
_OVERLAY_PROMPT_KEYS = (
    "persona",
    "instructions",
    "strategic",
    "tactical",
    "summarization",
)


def _raw_leaf_llm_keys(config_path: str) -> set:
    """llm keys explicitly set in the base config's own leaf file.

    Mirrors ``load_agent_config``: the settings matrix refines only the keys
    it owns and must never clobber an explicitly-set llm value. Without this
    the base-only resolve would diverge from the agent's ``from_config``
    fallback. ``authored_llm_keys`` is chain-aware for a role root (the
    overlay + ``expert_base`` pair is one authored base), lifts legacy tier
    blocks first, and returns an empty set for unreadable input.
    """
    return authored_llm_keys(config_path)


def resolve_config(
    *,
    base_config_name: str,
    base_defaults: Optional[dict] = None,
    expert_row: Optional[dict] = None,
    project_overrides: Optional[dict] = None,
    db_overrides: Optional[dict] = None,
    user_settings: Optional[dict] = None,
    request_override: Optional[dict] = None,
    expert_type: str = "session",
    capture: Optional[dict] = None,
    skills: Optional[dict] = None,
    grant_strip: Optional[Callable[[dict], dict]] = None,
    db_refs: Optional[dict] = None,
) -> dict:
    """Resolve the full agent config to a ``serialize_resolved_config``-shaped blob.

    Layer order (global_expert_management.md:246-260, most-specific wins)::

        bundled base -> base_defaults -> expert fragment
        -> project_experts.config_override -> DB config_overrides (0022)
        -> user persistent_agent settings -> request config_override

    The ``subagents.roster`` is materialised AFTER those layers (so a request
    override's ``subagents.roster.<name>.llm.model`` deep-merges into the
    entry first) and BEFORE the capture / grant strip (so the PDP sees the
    resolved entries, not ``{$ref: critic}``). ``db_refs`` is the caller's
    prefetched ``{expert_uuid: row}`` map for UUID ``$ref``s; an entry that
    cannot be resolved is dropped and recorded in ``agent._roster_warnings``
    — dispatch never fails a job over its roster.

    ``base_defaults`` sits just above the bundled base and *below* the expert:
    use it for the system/user **default model** selection (model names only), so
    the base config's placeholder model is replaced by the real default while an
    expert or request override still wins. Transport (base_url/api_key) for the
    final model is injected into the delivery copy by ``inject_blob_credentials``.

    The returned blob carries NO credentials (``serialize_resolved_config``
    strips ``llm.api_key``); the caller injects creds into the delivery copy and
    strips the rest before persisting.

    ``expert_type`` is the ROLE the config is resolved for (``worker`` /
    ``session`` / ``subagent``): the bundled base's ``$extends`` chain is
    re-rooted onto that role's overlay (``expert_base <- overlays/<role> <-
    expert``, universal_experts_and_subagents.md §1.1), so a session expert
    resolves as a worker for a job and vice versa. When ``base_config_name``
    is itself a chain root (``worker_base`` / ``session_base`` / ...) the role
    wins over the name — the roots are one thing in different roles. Any other
    string keeps the chain's own root (call-site intent only, as before).

    ``grant_strip``, when given, runs on the fully-merged ``data`` before BOTH
    ``capture`` and the returned blob are built from it — so the PDP's capture
    and the blob the agent hydrates always agree. Applying it only to
    ``capture`` would silence the dispatch check while still delivering the
    stripped capability.
    """
    if (
        expert_row
        and "harness_adapter" in expert_row
        and expert_row["harness_adapter"] != "srw/v1"
    ):
        raise ValueError(
            "Generic Experts cannot be resolved by the SRW harness adapter"
        )
    if expert_row and expert_row.get("harness_config_name"):
        from orchestrator.services.config_overrides import validated_config_name

        base_config_name = validated_config_name(expert_row["harness_config_name"])
    role = expert_type if expert_type in ROLE_ROOTS else None
    if role is not None and canonical_config_name(base_config_name) in ROOT_NAMES:
        base_config_name = ROLE_ROOTS[role]
    base_path, deployment_dir = resolve_bundled_config_path(base_config_name)
    if expert_row and expert_row.get("harness_asset_name"):
        # Asset selection supplies prompt/matrix/skill files only. The saved
        # Expert's actual base and authored leaf remain its sole configuration
        # inputs; importing its original disk leaf would resurrect removed keys.
        from shared.runtime.core.srw_manifest_config import validate_srw_asset_name

        asset_name = validate_srw_asset_name(expert_row["harness_asset_name"])
        _asset_path, deployment_dir = resolve_bundled_config_path(asset_name)
        if not deployment_dir:
            raise ValueError("SRW harness asset_name does not select installed assets")

    # A named bundled expert is logically an expert overlay, not the base layer.
    # Split its leaf from $extends so account fallbacks can sit above the real
    # framework base but below the bundled expert, matching DB expert precedence.
    # (Previously account defaults silently replaced bundled expert models.)
    # A chain root is the base itself, never a leaf on top of another base.
    bundled_leaf: dict = {}
    parent_path: str | None = None
    parent_role: str | None = None
    try:
        from shared.runtime.core.srw_manifest_config import read_srw_config

        raw_leaf = read_srw_config(base_path)
        if (
            isinstance(raw_leaf, dict)
            and raw_leaf.get("$extends")
            and canonical_config_name(base_config_name) not in ROOT_NAMES
        ):
            # Same re-rooting rule the loader applies to a chain link: a link
            # to any root becomes the requested role's overlay.
            parent_name, parent_role = reroot_extends(str(raw_leaf["$extends"]), role)
            parent_path, _ = resolve_bundled_config_path(parent_name)
            bundled_leaf = dict(raw_leaf)
            bundled_leaf.pop("$extends", None)
            # Read straight off disk, so it bypasses load_and_merge_config's
            # normalisation and needs its own (tool policy + legacy llm tiers).
            bundled_leaf = normalize_tool_policy(
                bundled_leaf, source=f"bundled-leaf:{base_config_name}"
            )
            bundled_leaf = normalize_llm_tiers(
                bundled_leaf, source=f"bundled-leaf:{base_config_name}"
            )
            bundled_leaf = normalize_delegation_block(
                bundled_leaf, source=f"bundled-leaf:{base_config_name}"
            )
    except Exception:
        raw_leaf = {}

    if parent_path:
        data = load_and_merge_config(parent_path, role=parent_role)
        explicit_llm_keys = _raw_leaf_llm_keys(parent_path)
    else:
        data = load_and_merge_config(base_path, role=role)
        explicit_llm_keys = _raw_leaf_llm_keys(base_path)

    # Default-model floor: replace the base placeholder model before the expert
    # merges (the expert/request still override it).
    if base_defaults:
        base_defaults = strip_loader_owned_keys(
            normalize_delegation_block(
                normalize_llm_tiers(
                    normalize_tool_policy(base_defaults, source="base-defaults"),
                    source="base-defaults",
                ),
                source="base-defaults",
            )
        )
        if base_defaults.get("llm"):
            explicit_llm_keys |= set(base_defaults["llm"].keys())
        data = deep_merge(data, base_defaults)

    if bundled_leaf:
        if bundled_leaf.get("llm"):
            explicit_llm_keys |= set(bundled_leaf["llm"].keys())
        data = deep_merge(data, bundled_leaf)

    # Expert fragment is the BASE overlay (below project/db/user/request layers,
    # decision 24 replacement-not-merge onto the type base). Mirrors the retired
    # agent-side _apply_db_expert; the caller picks base_config_name by type.
    prompts_override: dict = {}
    if expert_row is not None:
        from shared.runtime.core.expert_resolution import (
            build_expert_config,
            expert_layer_source,
        )

        expert_cfg = expert_row.get("config") or {}
        if isinstance(expert_cfg, str):
            import json

            expert_cfg = json.loads(expert_cfg)
        # The explicit llm keys must reflect the LIFTED shape (a legacy
        # strategic pin's params are explicit for the matrix, not family
        # defaults). build_expert_config normalises the row itself with the
        # same source label, so the deprecation warning logs once.
        expert_cfg = normalize_llm_tiers(
            expert_cfg, source=expert_layer_source(expert_row)
        )
        explicit_llm_keys |= set((expert_cfg.get("llm") or {}).keys())
        for index, layer in enumerate(expert_row.get("harness_config_layers", [])):
            layer = normalize_llm_tiers(layer, source=f"expert-private-layer:{index}")
            explicit_llm_keys |= set((layer.get("llm") or {}).keys())
        data, prompts_override = build_expert_config(data, expert_row)

    # Every remaining authored layer, normalised at birth. Layer-local
    # expansion is what lets deep_merge stay untouched: each layer is already
    # list[str] by the time it merges, so the existing "most specific layer
    # that mentions a category wins it wholesale" rule carries unchanged.
    # Server-generated request fragments (_critic_config_override,
    # the campaign-loop {"loop": ["loop_plan"]}) ride this same path and need
    # no special handling. The legacy llm tiers are mapped per layer for the
    # same reason: a job override's strategic pin resolves against THAT
    # layer's own llm.model, never against a lower layer's. Every layer is
    # caller-authored, so loader-owned (``_``-prefixed) keys are stripped
    # before the merge: a request override carrying ``_db_prompt_keys: []``
    # must not be able to unfence the DB prompts marked below (security
    # audit 2026-08-27, finding #2).
    for _source, layer in (
        ("project-override", project_overrides),
        ("db-override", db_overrides),
        ("user-settings", user_settings),
        ("request-override", request_override),
    ):
        if layer:
            layer = strip_loader_owned_keys(
                normalize_delegation_block(
                    normalize_llm_tiers(
                        normalize_tool_policy(layer, source=_source), source=_source
                    ),
                    source=_source,
                )
            )
            if layer.get("llm"):
                explicit_llm_keys |= set(layer["llm"].keys())
            data = deep_merge(data, layer)

    # Infrastructure is selected independently of the Expert's private layers.
    # Keep the delivery format expected by the SRW harness, but never let an
    # Expert select a backend or size a VM through its behavioral configuration.
    if role in ("worker", "session"):
        from shared.runtime.core.workspace_selection import (
            bind_execution_workspace,
            execution_workspace_config,
        )

        data = bind_execution_workspace(
            data,
            execution_workspace_config(
                base_defaults,
                project_overrides,
                user_settings,
                request_override,
                role=role,
            ),
        )

    # Provenance markers are written LAST, after every authored layer has
    # merged, so this DB-loading path is their only writer: no layer above or
    # below can set, clear, or replace them.
    if expert_row is not None:
        # decision 7: mark the DB-authored persona so the render path fences it.
        data["_persona_source"] = "db"
        # Part 2: record which prompt segments are DB-authored (untrusted) so the
        # render path fences strategic/tactical (fence_phase_directive) and brace-
        # escapes summarization. Only the segments actually present in the row —
        # inherited (disk) segments stay trusted.
        data["_db_prompt_keys"] = [
            k for k in _OVERLAY_PROMPT_KEYS if prompts_override.get(k)
        ]

    # Pruning point 2 of 3: the role overlay's ignored keys are dropped again
    # after the request layers, so a job/thread override cannot re-introduce
    # what the role does not read (e.g. `workspace.backend` for a subagent).
    data = prune_ignored_keys(data)

    # Phase-skill floor (U2, worker role): the strategic-phase /
    # tactical-phase bindings replaced an unconditional system-prompt swap, so
    # a worker must always carry them. `deep_merge` replaces lists wholesale —
    # an expert that authors its own `instruction_files` (a DB expert forked
    # before U2, a session expert dispatched as a worker) would otherwise lose
    # its phase guidance silently. Restored here, before the capture and the
    # freeze, so the PDP view and the blob agree.
    if role == "worker":
        data = ensure_worker_phase_skill_bindings(data)

    # Roster materialisation: every `subagents.roster` entry becomes its fully
    # merged subagent-role config (pruning point 3 of 3 runs per entry inside).
    # The parent's llm is final here — an `inherit` entry copies the model the
    # request layers selected. The parent's own matrix pass below never
    # touches the roster; each entry ran its own for its model family.
    if data.get("subagents") is not None:
        data = resolve_subagent_roster(
            data,
            db_refs=db_refs or {},
            deployment_dir=deployment_dir,
            on_missing="drop",
        )

    _apply_settings_matrix(data, explicit_llm_keys, deployment_dir)

    # Idempotent sweep, and it must land HERE — before the capture, not inside
    # load_agent_config_from_dict. capture["merged_fragment"] is what the
    # dispatch PDP evaluates, and capability_grants._truthy is wrong on two raw
    # policy shapes: it reads {} as false (grant violation missed) and
    # {only: []} as true (violation fabricated for an empty group). Both are
    # refused by the normaliser, and the PDP only ever sees lists.
    data = normalize_tool_policy(data)

    # Applied HERE so the capture the PDP evaluates and the blob the agent
    # hydrates are the same stripped config. Stripping only the capture would
    # silence the check while still delivering the capability.
    if grant_strip is not None:
        data = grant_strip(data)

    if capture is not None:
        # Full merged config in fragment shape — the policy view for the dispatch
        # PEP (single PDP). The base's shell/delegation are present here; deny-by-
        # default is reconciled by grandfathering existing users (migration 0030).
        capture["merged_fragment"] = copy.deepcopy(data)

    config = load_agent_config_from_dict(data, deployment_dir=deployment_dir)
    blob = serialize_resolved_config(config, model=config.llm.model)

    # Overlay the DB expert's prompt segments onto the resolved prompts so the
    # frozen blob matches what the render path emits: serialize_resolved_config
    # only sees disk + config, never the expert row's out-of-band prompts. The
    # untrusted segments are fenced at render via the _persona_source /
    # _db_prompt_keys markers (decision 7 + Part 2).
    for _k in _OVERLAY_PROMPT_KEYS:
        if prompts_override.get(_k):
            blob["prompts"][_k] = prompts_override[_k]

    # Slice-2 skills runtime: attach the pre-gathered in-scope skill menu + file
    # trees. DB I/O happens in the caller (orchestrator/main.py); this keeps the
    # blob shape identical for jobs and sessions and unit-testable without a DB.
    if skills:
        blob["skills"] = skills

    return blob


def ensure_worker_phase_skill_bindings(data: dict) -> dict:
    """Return ``data`` with the two phase-skill bindings present in
    ``instruction_files`` (prepended when missing). Phase skills are the
    only live source of worker phase guidance after U2 WP6."""
    entries = data.get("instruction_files")
    entries, restored = ensure_phase_skill_bindings(
        entries if isinstance(entries, list) else []
    )
    if restored:
        logger.info(
            "Restored phase-skill bindings %s for worker expert %r (its "
            "instruction_files replaced the worker overlay's list)",
            restored,
            data.get("agent_id"),
        )
        data["instruction_files"] = entries
    return data


def unrouted_model_slots(blob: dict) -> list[str]:
    """Return descriptions of model-bearing slots in a *credentialed* delivery
    blob that have NO transport — no ``base_url``, ``api_key``, or ``provider``.

    Such a slot has no way to reach its endpoint: the agent's ``create_llm``
    falls back to the OpenAI factory default (``api.openai.com``) and the model
    almost certainly 401/404s with an opaque error (incident: job eec20eeb —
    codex phase pins shipped without transport). Dispatch uses this to **fail
    fast** with an actionable message instead of letting the agent misroute.

    Conservative by design: a slot is only flagged when it carries a ``model``
    yet none of the three routing fields. A model with a ``provider`` (factory
    default base URL) or an inline ``api_key`` is considered routable.

    Since U1 the model-bearing slots are ``llm``, ``llm.summarization``,
    ``auxiliary``, the roster-wide ``subagents.llm`` and every roster entry's
    ``llm`` (+ its ``summarization``); the message names the entry. A roster
    entry marked ``llm._inherit_llm`` carries its parent's model NAME and is
    credentialed by that name like any other slot, so it is checked like any
    other slot; the bare ``inherit`` sentinel (a model-less parent) is not a
    model and is skipped.
    """
    agent = blob.get("agent") or {}
    llm = agent.get("llm") or {}
    problems: list[str] = []

    def _check(section: object, label: str) -> None:
        if not isinstance(section, dict):
            return
        model = section.get("model")
        if not model or model == INHERIT_MODEL:
            return
        if not (
            section.get("base_url") or section.get("api_key") or section.get("provider")
        ):
            problems.append(f"{label} model {model!r}")

    _check(llm, "llm")
    if isinstance(llm, dict):
        _check(llm.get("summarization"), "llm.summarization")
    _check(agent.get("auxiliary"), "auxiliary")
    subagents = agent.get("subagents")
    if isinstance(subagents, dict):
        _check(subagents.get("llm"), "subagents.llm")
        roster = subagents.get("roster")
        if isinstance(roster, dict):
            for name, entry in roster.items():
                if not isinstance(entry, dict):
                    continue
                entry_llm = entry.get("llm")
                _check(entry_llm, f"subagents.roster.{name}.llm")
                if isinstance(entry_llm, dict):
                    _check(
                        entry_llm.get("summarization"),
                        f"subagents.roster.{name}.llm.summarization",
                    )
    return problems


async def inject_blob_credentials(
    blob: dict,
    injector: Callable[[dict], Awaitable[dict] | Awaitable[None]],
) -> dict:
    """Return a DELIVERY copy of ``blob`` with transport credentials injected.

    The input blob (the persistable copy) is never mutated and stays secret-free;
    credentials live only on the returned copy. Besides the model and env-key
    sections, ``research`` is carried through this seam because search/fetch
    adapters are selected per dispatch rather than from pod environment.

    ``injector`` is an async callable taking a ``config_override``-shaped dict
    (``{"llm": {...}, "auxiliary": {...}, "env_keys": {...}}``) and enriching it
    in place (and/or returning it): i.e. the orchestrator's existing
    ``_inject_dispatch_credentials`` (jobs) or ``_inject_thread_dispatch_credentials``
    (sessions). Seeding the override from the blob's resolved model means the
    injector routes credentials for the *resolved* model, not the YAML default.
    """
    delivered = copy.deepcopy(blob)
    agent = delivered.setdefault("agent", {})

    co: dict = {"llm": dict(agent.get("llm") or {})}
    if agent.get("auxiliary"):
        co["auxiliary"] = dict(agent["auxiliary"])
    if agent.get("env_keys"):
        co["env_keys"] = dict(agent["env_keys"])
    if isinstance(agent.get("research"), dict):
        co["research"] = copy.deepcopy(agent["research"])
    # The roster's model slots (U1): the roster-wide ``subagents.llm`` and
    # each entry's ``llm`` — ONLY the llm blocks, never the entries' full
    # configs, so the injector's input stays a config_override-shaped dict
    # and nothing but transport is merged back. An inheriting entry carries
    # its parent's model NAME here (the resolver copied it), so it is routed
    # by that name exactly like the top level.
    subagents = agent.get("subagents")
    if isinstance(subagents, dict):
        co_sub: dict = {}
        if isinstance(subagents.get("llm"), dict) and subagents["llm"]:
            co_sub["llm"] = copy.deepcopy(subagents["llm"])
        roster = subagents.get("roster")
        if isinstance(roster, dict):
            co_roster = {
                name: {"llm": copy.deepcopy(entry["llm"])}
                for name, entry in roster.items()
                if isinstance(entry, dict) and isinstance(entry.get("llm"), dict)
            }
            if co_roster:
                co_sub["roster"] = co_roster
        if co_sub:
            co["subagents"] = co_sub

    # Drop None-valued model keys so the injector's gap-fill / setdefault logic
    # treats them as absent (a serialized config carries explicit model=None /
    # base_url=None defaults that would otherwise block injection). Mirrors
    # _inject_thread_dispatch_credentials' own None-stripping.
    #
    # This MUST reach the nested override blocks (llm.summarization, the
    # roster entries' llm blocks; the strategic/tactical tiers it was written
    # for are lifted into llm.model by the loader since U1), not just the
    # top-level section. serialize_resolved_config emits those with explicit
    # base_url=None / provider=None leaves; a present-but-None key defeats the
    # injector's setdefault, so endpoint-backed phase pins (e.g. codex models)
    # shipped without transport and 401'd against api.openai.com while the
    # base model — whose top-level None WAS stripped — worked, masking the gap
    # (incident: job eec20eeb / "Research 01").
    def _strip_none_keys(d: dict) -> None:
        for _k in [_k for _k, _v in d.items() if _v is None]:
            del d[_k]

    def _strip_llm_block(block: object) -> None:
        if isinstance(block, dict):
            _strip_none_keys(block)
            for _sub in block.values():
                if isinstance(_sub, dict):
                    _strip_none_keys(_sub)

    for _sect in ("llm", "auxiliary"):
        _strip_llm_block(co.get(_sect))
    if isinstance(co.get("subagents"), dict):
        _strip_llm_block(co["subagents"].get("llm"))
        for _entry in (co["subagents"].get("roster") or {}).values():
            if isinstance(_entry, dict):
                _strip_llm_block(_entry.get("llm"))

    result = await injector(co)
    if isinstance(result, dict):
        co = result

    agent["llm"] = deep_merge(agent.get("llm") or {}, co.get("llm") or {})
    if co.get("auxiliary"):
        agent["auxiliary"] = deep_merge(agent.get("auxiliary") or {}, co["auxiliary"])
    if co.get("env_keys"):
        agent.setdefault("env_keys", {}).update(co["env_keys"])
    # Search/fetch resolution is authoritative for this dispatch. Replace the
    # section rather than deep-merging it so an injector can remove a stale
    # capability or same-row fallback by omitting it from the resolved block.
    if isinstance(co.get("research"), dict) and co["research"]:
        agent["research"] = copy.deepcopy(co["research"])
    else:
        agent.pop("research", None)
    if isinstance(co.get("live_session"), dict):
        agent["live_session"] = copy.deepcopy(co["live_session"])
    else:
        agent.pop("live_session", None)
    co_sub = co.get("subagents")
    if isinstance(co_sub, dict) and isinstance(agent.get("subagents"), dict):
        sub = agent["subagents"]
        if isinstance(co_sub.get("llm"), dict):
            sub["llm"] = deep_merge(sub.get("llm") or {}, co_sub["llm"])
        roster = sub.get("roster")
        for name, co_entry in (co_sub.get("roster") or {}).items():
            entry = roster.get(name) if isinstance(roster, dict) else None
            if (
                isinstance(entry, dict)
                and isinstance(co_entry, dict)
                and isinstance(co_entry.get("llm"), dict)
            ):
                entry["llm"] = deep_merge(entry.get("llm") or {}, co_entry["llm"])
    return delivered
