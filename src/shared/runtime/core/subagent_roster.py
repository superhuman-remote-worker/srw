"""Materialise an expert's ``subagents.roster`` (U1 — universal experts).

An expert may declare built-in subagents
(knowledge-base/knowledge/features/universal_experts_and_subagents.md §1.1)::

    subagents:
      default: explorer
      llm: {model: claude-haiku-4-5}              # roster-wide default (optional)
      roster:
        explorer: {$ref: subagents/explorer}      # library entry
        reviewer: {$ref: critic, llm: {model: inherit}}   # a FULL expert as a child
        implementer:                              # inline "small expert"
          description: Implements ONE bounded change and returns the diff.
          tools: {workspace: [read_file, write_file, edit_file]}

Every entry resolves through the same chain as an expert, on the subagent
role overlay::

    expert_base <- overlays/subagent <- [$ref target's own $extends chain]
                <- subagents.llm (roster-wide) <- the entry's sibling keys
                (job/thread override already deep-merged into them)
                -> "inherit" model sentinel -> settings matrix (per entry,
                   the entry's own model family) -> parent-only keys pruned

The result is a plain, fully merged config dict per entry — what
``load_agent_config_from_dict`` parses for a child (U3) and what the
orchestrator freezes into ``jobs.resolved_config``. Bookkeeping keys on a
resolved entry: ``_ref`` / ``_ref_kind`` (``bundled`` | ``library`` | ``db``)
/ ``_ref_name`` (DB rows) / ``_deployment_dir`` (repo-relative directory the
entry's prompt files live in) / ``llm._inherit_llm`` (model copied from the
parent; see ``loader.inherit_parent_llm``) / ``prompts`` + ``_persona_source``
(DB rows: the row's prompt text inlined).

``$ref`` grammar: a bundled expert directory name (``critic``, or
``experts/critic``), a library entry (``explorer`` or ``subagents/explorer``;
``config/subagents/<name>/config.yaml``), or a DB expert UUID. Nothing else —
never a path. A bare name tries the bundled experts first, then the library.
DB rows are only visible to the orchestrator: the caller prefetches them into
``db_refs`` (``{uuid: row}``); anywhere else a UUID ref is dropped with a
warning. Depth is 1 (D7): a referenced expert's own ``subagents`` block is
dropped. The target's ``$extends`` chain is walked with a visited set and a
hop cap (``MAX_REF_HOPS``) before it is loaded, so a cyclic or runaway chain
is a clean :class:`RosterResolutionError` instead of a RecursionError.

Failure policy (``on_missing``): ``"raise"`` — an unknown / invalid disk ref,
a cycle or a malformed entry raises :class:`RosterResolutionError` (the
authoring error surfaces: expert save → 422, a bundled config → the boot or
the test fails); ``"drop"`` — the entry is dropped, logged, and recorded in
``data["_roster_warnings"]`` (dispatch never fails a job over its roster). A
UUID ref with no prefetched row is dropped + recorded under BOTH policies.
"""

from __future__ import annotations

import copy
import json
import logging
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import yaml

from shared.runtime.core.expert_resolution import validate_live_voice_placeholders
from shared.runtime.core.loader import (
    IGNORE_KEYS_DIRECTIVE,
    INHERIT_MODEL,
    ROLE_ROOTS,
    ROOT_NAMES,
    _apply_settings_matrix,
    _root_name_for_path,
    authored_llm_keys,
    canonical_config_name,
    deep_merge,
    get_project_root,
    inherit_parent_llm,
    load_and_merge_config,
    load_role_base,
    normalize_llm_tiers,
    prune_ignored_keys,
    resolve_config_path,
    strip_loader_owned_keys,
)
from shared.runtime.core.tool_policy import normalize_tool_policy

logger = logging.getLogger(__name__)

#: The role every roster entry resolves on.
SUBAGENT_ROLE = "subagent"
#: Maximum number of expert-to-expert ``$extends`` links a ``$ref`` target's
#: chain may have before it reaches a chain root (``a -> b -> c -> d ->
#: <root>`` is 3 hops). A hop is one non-root link.
MAX_REF_HOPS = 3
#: Where dropped-entry / default warnings are recorded on the config dict
#: (an unknown key, so it rides ``config.extra`` and the frozen blob).
ROSTER_WARNINGS_KEY = "_roster_warnings"

_ON_MISSING = ("raise", "drop")
_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)
#: ``critic`` | ``experts/critic`` | ``explorer`` | ``subagents/explorer`` —
#: one path component, optionally prefixed by the directory it lives in.
_DISK_REF_RE = re.compile(r"^(?:(experts|subagents)/)?([A-Za-z0-9][A-Za-z0-9_.-]*)$")


class RosterResolutionError(ValueError):
    """A roster entry cannot be materialised: unknown or malformed ``$ref``,
    a ``$extends`` cycle / chain deeper than ``MAX_REF_HOPS`` behind the
    target, or an entry that is not a mapping. The message names the entry
    (``subagents.roster.<name>: ...``). A ``ValueError`` so callers that
    already translate loader errors need no new branch."""


class _DroppedEntry(Exception):
    """Internal: the entry is dropped under every policy (a DB expert id with
    no prefetched row — the caller simply cannot see the row from here)."""


@dataclass
class _Context:
    base: Dict[str, Any]
    base_llm_keys: Set[str]
    parent_llm: Dict[str, Any]
    roster_llm: Dict[str, Any]
    db_refs: Dict[str, Any]
    parent_deployment_dir: Optional[str]
    warnings: List[str] = field(default_factory=list)


def _config_root() -> Path:
    """``config/`` under the project root (a seam tests can point elsewhere)."""
    return get_project_root() / "config"


def _portable_dir(directory: Optional[str]) -> Optional[str]:
    """A directory as recorded on a frozen entry: relative to the project
    root when inside it (the orchestrator and the agent image share the
    ``config/`` tree but not the absolute prefix), absolute otherwise."""
    if not directory:
        return None
    try:
        path = Path(directory).resolve()
        root = get_project_root().resolve()
        if path.is_relative_to(root):
            return str(path.relative_to(root))
    except (OSError, ValueError):
        pass
    return str(directory)


def is_db_ref(ref: Any) -> bool:
    """True when ``ref`` is a DB expert id (the only ``$ref`` kind a caller
    must prefetch — see :func:`collect_roster_db_refs`)."""
    return isinstance(ref, str) and bool(_UUID_RE.match(ref.strip()))


def _raw_roster(layer: Any) -> Dict[Any, Any]:
    """``layer["subagents"]["roster"]`` when every level is a mapping, else
    ``{}`` — the tolerant read the prefetch / authoring helpers share."""
    if not isinstance(layer, dict):
        return {}
    subagents = layer.get("subagents")
    if not isinstance(subagents, dict):
        return {}
    roster = subagents.get("roster")
    return roster if isinstance(roster, dict) else {}


def collect_roster_db_refs(layer: Any) -> Set[str]:
    """Every DB expert id one authored layer's ``subagents.roster`` names.

    The orchestrator calls this on each layer it is about to hand to
    ``resolve_config`` (the expert fragment, the project / request overrides)
    and prefetches the rows into ``db_refs`` — the resolver itself never
    touches a database. Malformed shapes yield nothing (the resolver reports
    them; this is only the prefetch list)."""
    out: Set[str] = set()
    for entry in _raw_roster(layer).values():
        if isinstance(entry, dict) and is_db_ref(entry.get("$ref")):
            out.add(str(entry["$ref"]).strip())
    return out


def collect_roster_named_refs(layer: Any) -> Set[str]:
    """Installed name selectors to bind to Catalog revisions on the server."""
    return {
        entry["$ref"].strip()
        for entry in _raw_roster(layer).values()
        if isinstance(entry, dict)
        and isinstance(entry.get("$ref"), str)
        and _DISK_REF_RE.fullmatch(entry["$ref"].strip())
        and not is_db_ref(entry["$ref"])
    }


def validate_roster_fragment(subagents: Any) -> Set[str]:
    """Authoring-time check of a raw ``subagents`` block (expert save).

    Checks what can be checked without a database: the block, its ``llm`` /
    ``roster`` and every entry are mappings; every ``$ref`` is a non-empty
    string; a bundled / library ref exists on disk and its ``$extends`` chain
    is sane (the same lookup and guard the resolver uses, so "saves" and
    "resolves" cannot disagree). Raises :class:`RosterResolutionError` (the
    caller turns it into a 422). Returns the DB expert ids the roster names —
    only the orchestrator can check those are visible to the author.

    ``subagents.default`` naming no entry is NOT an error here, matching the
    resolver, which only records it.
    """
    if not isinstance(subagents, dict):
        raise RosterResolutionError(
            f"subagents: expected a mapping, got {type(subagents).__name__}"
        )
    llm = subagents.get("llm")
    if llm is not None and not isinstance(llm, dict):
        raise RosterResolutionError(
            f"subagents.llm: expected a mapping, got {type(llm).__name__}"
        )
    roster = subagents.get("roster")
    if roster is None:
        roster = {}
    if not isinstance(roster, dict):
        raise RosterResolutionError(
            f"subagents.roster: expected a mapping, got {type(roster).__name__}"
        )
    db_refs: Set[str] = set()
    for raw_name, raw in roster.items():
        name = str(raw_name)
        if not isinstance(raw, dict):
            raise RosterResolutionError(
                f"subagents.roster.{name}: entry must be a mapping (inline keys or "
                f"{{$ref: <expert>}}), got {type(raw).__name__}"
            )
        _validate_live_entry(name, raw)
        if "$ref" not in raw:
            continue
        ref = raw.get("$ref")
        if not isinstance(ref, str) or not ref.strip():
            raise RosterResolutionError(
                f"subagents.roster.{name}: $ref must be a non-empty string, got {ref!r}"
            )
        ref = ref.strip()
        if is_db_ref(ref):
            db_refs.add(ref)
            continue
        path, _kind, _directory = _locate_disk_ref(name, ref)
        _guard_extends_chain(name, ref, path)
    return db_refs


#: The only ``driver`` a roster entry may name; absent = an LLM child.
LIVE_DRIVER = "live"
_LIVE_TEXT_KEYS = ("fallback", "follow_up", "goodbye")
_LIVE_FILE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


def _validate_live_entry(name: str, raw: Dict[str, Any]) -> None:
    """The shape of a live voice entry (``driver: live`` + ``live``).

    Mirrors ``$defs/subagentRosterEntry`` in ``config/schema.json``. The
    orchestrator builds the voice's instructions from the ``live`` block and
    reads a ``persona_file`` / ``facts_file`` from the expert's directory, so a
    file is a bare name there; its prose must be plain text like a persona.
    """
    where = f"subagents.roster.{name}"
    driver = raw.get("driver")
    if driver is not None and str(driver) != LIVE_DRIVER:
        raise RosterResolutionError(
            f"{where}.driver: {driver!r} is not a driver (the only one is "
            f"{LIVE_DRIVER!r}; omit it for an LLM subagent)"
        )
    live = raw.get("live")
    if live is None:
        return
    if driver != LIVE_DRIVER:
        raise RosterResolutionError(f"{where}.live: requires driver: {LIVE_DRIVER}")
    if not isinstance(live, dict):
        raise RosterResolutionError(
            f"{where}.live: expected a mapping, got {type(live).__name__}"
        )
    for text_key, file_key in (("persona", "persona_file"), ("facts", "facts_file")):
        if live.get(text_key) is not None and live.get(file_key) is not None:
            raise RosterResolutionError(
                f"{where}.live: give {text_key} or {file_key}, not both"
            )
        file_name = live.get(file_key)
        if file_name is not None and (
            not isinstance(file_name, str) or not _LIVE_FILE_RE.match(file_name)
        ):
            raise RosterResolutionError(
                f"{where}.live.{file_key}: a file name in the expert's directory, "
                f"got {file_name!r}"
            )
    for key in ("persona", "facts", "greeting", "voice"):
        value = live.get(key)
        if value is not None and not isinstance(value, str):
            raise RosterResolutionError(f"{where}.live.{key}: expected a string")
    timeout = live.get("answer_timeout_s")
    if timeout is not None and (
        isinstance(timeout, bool)
        or not isinstance(timeout, (int, float))
        or not 1 <= timeout <= 600
    ):
        raise RosterResolutionError(
            f"{where}.live.answer_timeout_s: seconds between 1 and 600"
        )
    texts = live.get("texts")
    if texts is not None:
        if not isinstance(texts, dict) or any(
            key not in _LIVE_TEXT_KEYS or not isinstance(value, str)
            for key, value in texts.items()
        ):
            raise RosterResolutionError(
                f"{where}.live.texts: a mapping of strings with keys "
                f"{', '.join(_LIVE_TEXT_KEYS)}"
            )
    try:
        validate_live_voice_placeholders({"roster": {name: raw}})
    except ValueError as exc:
        raise RosterResolutionError(str(exc)) from None


def _lookup_row(db_refs: Dict[str, Any], ref: str) -> Optional[Dict[str, Any]]:
    row = db_refs.get(ref)
    if row is not None:
        return row
    wanted = ref.lower()
    for key, value in db_refs.items():
        if str(key).lower() == wanted:
            return value
    return None


def _locate_disk_ref(name: str, ref: str) -> Tuple[str, str, str]:
    """``(config.yaml path, kind, deployment_dir)`` for a bundled / library ref."""
    match = _DISK_REF_RE.match(ref)
    if not match:
        raise RosterResolutionError(
            f"subagents.roster.{name}: $ref {ref!r} is not a bundled expert "
            "name, a subagents/<name> library entry or a DB expert id"
        )
    prefix, stem = match.groups()
    root = _config_root()
    candidates: List[Tuple[str, Path]] = []
    if prefix in (None, "experts"):
        candidates.append(("bundled", root / "experts" / stem))
    if prefix in (None, "subagents"):
        candidates.append(("library", root / "subagents" / stem))
    for kind, directory in candidates:
        config_file = directory / "config.yaml"
        if config_file.is_file():
            return str(config_file), kind, str(directory)
    raise RosterResolutionError(
        f"subagents.roster.{name}: $ref {ref!r} not found — expected "
        f"config/experts/{stem}/config.yaml or config/subagents/{stem}/config.yaml"
    )


def _guard_extends_chain(name: str, ref: str, path: str) -> None:
    """Refuse a target whose ``$extends`` chain cycles or runs past
    ``MAX_REF_HOPS`` before reaching a chain root."""
    seen: Set[str] = set()
    hops = 0
    current = canonical_config_name(str(path))
    while True:
        if _root_name_for_path(current) is not None:
            return
        key = os.path.normpath(os.path.abspath(current))
        if key in seen:
            raise RosterResolutionError(
                f"subagents.roster.{name}: $ref {ref!r} — $extends cycle at {current}"
            )
        seen.add(key)
        try:
            from shared.runtime.core.srw_manifest_config import read_srw_config

            raw = read_srw_config(current)
        except (OSError, ValueError, yaml.YAMLError) as exc:
            raise RosterResolutionError(
                f"subagents.roster.{name}: $ref {ref!r} — cannot read {current}: {exc}"
            ) from exc
        parent = raw.get("$extends") if isinstance(raw, dict) else None
        if not parent:
            return
        parent_name = canonical_config_name(str(parent))
        if parent_name in ROOT_NAMES:
            return
        hops += 1
        if hops > MAX_REF_HOPS:
            raise RosterResolutionError(
                f"subagents.roster.{name}: $ref {ref!r} — $extends chain deeper "
                f"than {MAX_REF_HOPS} expert links"
            )
        current, _ = resolve_config_path(parent_name)


def _load_disk_target(
    ctx: _Context, name: str, ref: str
) -> Tuple[Dict[str, Any], Set[str], Dict[str, Any], Optional[str]]:
    path, kind, deployment_dir = _locate_disk_ref(name, ref)
    _guard_extends_chain(name, ref, path)
    chain = load_and_merge_config(path, role=SUBAGENT_ROLE)
    # A chained target already sits on the subagent base (re-rooted); a
    # standalone leaf (no $extends) is put on it here. Idempotent for the
    # former: the base is pruned the same way the chain is.
    merged = deep_merge(ctx.base, chain)
    meta = {
        "_ref": ref,
        "_ref_kind": kind,
        "_deployment_dir": _portable_dir(deployment_dir),
    }
    return merged, authored_llm_keys(path), meta, deployment_dir


def _load_db_target(
    ctx: _Context, name: str, ref: str, row: Dict[str, Any]
) -> Tuple[Dict[str, Any], Set[str], Dict[str, Any], Optional[str]]:
    if "harness_adapter" in row and row["harness_adapter"] != "srw/v1":
        raise RosterResolutionError(
            f"subagents.roster.{name}: a generic harness cannot run inside an SRW process"
        )
    config_name = row.get("harness_config_name")
    if config_name and canonical_config_name(config_name) not in ROOT_NAMES:
        raise RosterResolutionError(
            f"subagents.roster.{name}: unsupported SRW config_name {config_name!r}; "
            "stored roster targets must use expert_base, worker_base, session_base "
            "or subagent_base so their authored settings can be re-rooted onto "
            "subagent_base"
        )
    from shared.runtime.core.expert_resolution import (
        build_expert_config,
        expert_layer_source,
    )

    fragment = row.get("config") or {}
    if isinstance(fragment, str):
        fragment = json.loads(fragment)
    if not isinstance(fragment, dict):
        fragment = {}
    # build_expert_config normalises the row itself (same source label, so the
    # deprecation log de-duplicates); the lifted shape is what counts as the
    # target's explicit llm keys for the matrix.
    fragment = normalize_llm_tiers(fragment, source=expert_layer_source(row))
    merged, prompts = build_expert_config(ctx.base, row)
    target_llm_keys = set((fragment.get("llm") or {}).keys())
    for index, private_layer in enumerate(row.get("harness_config_layers", [])):
        normalized = normalize_llm_tiers(
            private_layer, source=f"{expert_layer_source(row)}:layer:{index}"
        )
        target_llm_keys |= set((normalized.get("llm") or {}).keys())
    for column in ("display_name", "description"):
        if not fragment.get(column) and row.get(column):
            merged[column] = row[column]
    if not fragment.get("tags") and row.get("tags"):
        merged["tags"] = list(row["tags"])
    if isinstance(prompts, dict) and prompts:
        # The row's prompt text is inlined (no disk to resolve it from at
        # spawn) and marked DB-authored so the render path fences it, exactly
        # like the orchestrator resolver marks a DB expert's own persona.
        merged["prompts"] = copy.deepcopy(prompts)
        merged["_persona_source"] = "db"
        merged["_db_prompt_keys"] = [k for k, v in prompts.items() if v]
    meta = {"_ref": ref, "_ref_kind": "db", "_ref_name": row.get("name")}
    deployment_dir = None
    selector = row.get("harness_asset_name")
    if selector:
        # A Catalog source may acquire a UUID when used as a Project default.
        # Its settings remain frozen in the row; only its trusted harness asset
        # directory supplies the bundled prompt files and model-family matrix.
        _path, _kind, deployment_dir = _locate_disk_ref(name, selector)
        meta["_deployment_dir"] = _portable_dir(deployment_dir)
    return merged, target_llm_keys, meta, deployment_dir


def _load_target(
    ctx: _Context, name: str, ref: str
) -> Tuple[Dict[str, Any], Set[str], Dict[str, Any], Optional[str]]:
    if _UUID_RE.match(ref) or any(
        str(key).lower() == ref.lower() for key in ctx.db_refs
    ):
        row = _lookup_row(ctx.db_refs, ref)
        if not row:
            raise _DroppedEntry(
                f"subagents.roster.{name}: $ref {ref!r} has no prefetched row "
                "(only the orchestrator resolves stored Expert definitions) — "
                "entry dropped"
            )
        return _load_db_target(ctx, name, ref, row)
    return _load_disk_target(ctx, name, ref)


def _resolve_entry(ctx: _Context, name: str, raw: Any) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        raise RosterResolutionError(
            f"subagents.roster.{name}: entry must be a mapping (inline keys or "
            f"{{$ref: <expert>}}), got {type(raw).__name__}"
        )
    # The entry's sibling keys are an authored layer like any other: tool
    # policy + legacy llm tiers normalised at birth, before the merge.
    layer = normalize_llm_tiers(
        normalize_tool_policy(raw, source=f"roster:{name}"), source=f"roster:{name}"
    )
    ref = layer.get("$ref")
    # The entry merges on top of a DB target that was just marked
    # ``_db_prompt_keys`` / ``_persona_source``: its sibling keys are authored
    # (they come from the parent expert's roster) and may not carry those
    # markers, or the child's DB prompts would render unfenced.
    overrides = strip_loader_owned_keys({k: v for k, v in layer.items() if k != "$ref"})

    if ref is None:
        merged = copy.deepcopy(ctx.base)
        target_llm_keys: Set[str] = set()
        meta: Dict[str, Any] = {}
        entry_dir = ctx.parent_deployment_dir
        if entry_dir:
            # Inline entries are part of their parent: `prompts` file names
            # resolve against the parent expert's directory.
            meta["_deployment_dir"] = _portable_dir(entry_dir)
        if "display_name" not in overrides:
            merged["display_name"] = name
    else:
        if not isinstance(ref, str) or not ref.strip():
            raise RosterResolutionError(
                f"subagents.roster.{name}: $ref must be a non-empty string, got {ref!r}"
            )
        merged, target_llm_keys, meta, entry_dir = _load_target(ctx, name, ref.strip())

    # Roster-wide llm: below the entry's own llm, above the base + $ref chain.
    if ctx.roster_llm:
        merged = deep_merge(merged, {"llm": ctx.roster_llm})
    merged = deep_merge(merged, overrides)

    # Depth 1 (D7): neither a referenced expert nor an inline entry gets a
    # roster of its own.
    nested = merged.pop("subagents", None)
    if nested:
        logger.info(
            "subagents.roster.%s: nested subagents block dropped (children get no "
            "roster of their own — depth 1)",
            name,
        )
    for directive in ("$ref", "$extends", "$comment", "$schema"):
        merged.pop(directive, None)
    merged["agent_id"] = name

    llm = merged.get("llm")
    if not isinstance(llm, dict):
        llm = {}
    merged["llm"] = llm
    override_llm = overrides.get("llm")
    explicit_llm_keys = (
        set(ctx.base_llm_keys)
        | set(target_llm_keys)
        | set(ctx.roster_llm)
        | set(override_llm if isinstance(override_llm, dict) else {})
    )
    if llm.get("model") == INHERIT_MODEL:
        copied = inherit_parent_llm(llm, ctx.parent_llm)
        if not copied:
            ctx.warnings.append(
                f"subagents.roster.{name}: llm.model is 'inherit' but the parent "
                "config names no model — left unresolved"
            )
        explicit_llm_keys |= copied

    # The entry's own model family drives its params / context window — the
    # parent's matrix pass never touches the roster.
    _apply_settings_matrix(merged, explicit_llm_keys, entry_dir)

    # Pruning point 3 of 3: whatever the subagent overlay ignores is dropped
    # again after the entry's own keys (and the job/thread override merged
    # into them) landed; the directive itself does not travel in the blob.
    prune_ignored_keys(merged)
    merged.pop(IGNORE_KEYS_DIRECTIVE, None)
    merged.update(meta)
    return merged


def resolve_subagent_roster(
    data: Dict[str, Any],
    *,
    db_refs: Optional[Dict[str, Any]],
    deployment_dir: Optional[str] = None,
    on_missing: str = "raise",
) -> Dict[str, Any]:
    """Materialise ``data["subagents"]`` in place and return ``data``.

    Args:
        data: The parent's fully merged config dict (its ``llm`` is what an
            ``inherit`` entry copies, so call this after the parent's layers
            are final — the settings matrix does not change ``llm.model``).
        db_refs: ``{expert_uuid: expert_row}`` prefetched by the caller for
            every UUID ``$ref`` the roster may name. ``{}`` / ``None`` where
            no DB is reachable; a UUID ref then drops with a warning.
        deployment_dir: The parent's deployment directory (the bundled
            expert's dir) — inline entries resolve their ``prompts`` file
            names and per-expert matrix overrides against it.
        on_missing: ``"raise"`` (authoring errors surface) or ``"drop"``
            (never fail the caller; drop + log + ``_roster_warnings``).

    A missing ``subagents`` key is left missing (the dataclass default
    covers it); a present one is rewritten to the canonical
    ``{"default", "llm", "roster"}`` shape with every entry materialised.
    """
    if on_missing not in _ON_MISSING:
        raise ValueError(f"on_missing must be one of {_ON_MISSING}, got {on_missing!r}")
    block = data.get("subagents")
    if block is None:
        return data

    warnings: List[str] = []
    if not isinstance(block, dict):
        warnings.append(
            f"subagents: expected a mapping, got {type(block).__name__} — ignored"
        )
        block = {}

    roster_llm = block.get("llm")
    if roster_llm is None:
        roster_llm = {}
    elif isinstance(roster_llm, dict):
        roster_llm = copy.deepcopy(roster_llm)
    else:
        warnings.append(
            f"subagents.llm: expected a mapping, got {type(roster_llm).__name__} — ignored"
        )
        roster_llm = {}

    raw_roster = block.get("roster")
    if raw_roster is None:
        raw_roster = {}
    elif not isinstance(raw_roster, dict):
        warnings.append(
            f"subagents.roster: expected a mapping, got {type(raw_roster).__name__} — ignored"
        )
        raw_roster = {}

    resolved: Dict[str, Dict[str, Any]] = {}
    if raw_roster:
        root_path, _ = resolve_config_path(ROLE_ROOTS[SUBAGENT_ROLE])
        parent_llm = data.get("llm")
        ctx = _Context(
            base=load_role_base(SUBAGENT_ROLE),
            base_llm_keys=authored_llm_keys(root_path),
            parent_llm=parent_llm if isinstance(parent_llm, dict) else {},
            roster_llm=roster_llm,
            db_refs=dict(db_refs or {}),
            parent_deployment_dir=deployment_dir,
        )
        for raw_name, raw_entry in raw_roster.items():
            name = str(raw_name)
            try:
                resolved[name] = _resolve_entry(ctx, name, raw_entry)
            except _DroppedEntry as exc:
                warnings.append(str(exc))
            except RosterResolutionError as exc:
                if on_missing == "raise":
                    raise
                warnings.append(f"{exc} — entry dropped")
        warnings.extend(ctx.warnings)

    default = block.get("default")
    default = str(default) if default else None
    if default and default not in resolved:
        warnings.append(f"subagents.default {default!r} names no roster entry")

    data["subagents"] = {"default": default, "llm": roster_llm, "roster": resolved}
    if warnings:
        for message in warnings:
            logger.warning("config: %s", message)
        existing = data.get(ROSTER_WARNINGS_KEY)
        data[ROSTER_WARNINGS_KEY] = (
            list(existing) if isinstance(existing, list) else []
        ) + warnings
    return data


def roster_summary(subagents: Any) -> Dict[str, Any]:
    """The roster as a settings surface reports it: ``{"default", "roster":
    [{"name", "description", "ref"}, ...]}``.

    Takes the ``subagents`` block of a config in either shape — materialised
    (``resolve_subagent_roster`` ran: entries carry ``description`` and
    ``_ref``) or raw (``{$ref: subagents/explorer}``; ``description`` may be
    absent and ``ref`` is the raw ``$ref``). Never raises: a malformed block
    reads as an empty roster, which is what ``delegate_agent`` would bind.

    The Cockpit's Delegation row shows this next to the toggle so a tick on an
    expert with no roster is refused by the reader, not discovered by the
    model — the seeded ``assistant`` shipped without one and a ticked
    Delegation bound a ``delegate_agent`` whose every call errored (main-dev
    thread 54e31e45, 2026-09-07).
    """
    if not isinstance(subagents, dict):
        return {"default": None, "roster": []}
    raw_roster = subagents.get("roster")
    entries: List[Dict[str, Any]] = []
    if isinstance(raw_roster, dict):
        for raw_name, entry in raw_roster.items():
            name = str(raw_name)
            if not isinstance(entry, dict):
                continue
            description = entry.get("description")
            ref = entry.get("_ref") or entry.get("$ref")
            entries.append(
                {
                    "name": name,
                    "description": (str(description).strip() if description else None),
                    "ref": str(ref) if ref else None,
                }
            )
    default = subagents.get("default")
    default = str(default) if default else None
    if default and default not in {e["name"] for e in entries}:
        default = None
    return {"default": default, "roster": entries}
