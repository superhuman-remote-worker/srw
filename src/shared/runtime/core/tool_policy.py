"""Tool-group policy: an authoring vocabulary that can say "on".

``tools.<category>`` is a list of tool names, and that one field carries two
unrelated facts — **membership** (which tools are in the group) and **policy**
(whether the group is on).  Membership already has an authority:
``TOOL_REGISTRY``, which knows every tool and its category.  Config should
carry policy.

Every registry category has at least one tool, so no group is ever legitimately
empty: every ``[]`` in every config is a disable marker.  That makes "off"
self-describing and "on" inexpressible — re-enabling needs the full member
list, which the caller looks for in the layer it is overriding, which says
``[]``.  Hence the shipped defect: the New Session form can disable a tool
group and can never enable one
(``knowledge-base/knowledge/issues/session_create_tool_toggles_cannot_enable_a_group.md``).

This module is the fix, and it is a **front-end, not a rewrite**.  Five
authoring forms normalise down to the single ``list[str]`` representation the
whole stack already speaks:

===========================  =====================================================
Form                         Resolves to
===========================  =====================================================
``true``                     every config-grantable tool the registry has here
``false``                    ``[]``
``{except: [names]}``        the config-grantable set minus these; tracks the
                             registry
``{only:   [names]}``        exactly these, as written; frozen
``[names]``                  legacy spelling of ``{only: [names]}`` — unchanged
``[]``                       legacy spelling of ``false`` — unchanged
===========================  =====================================================

Both legacy spellings are *already* canonical, so :func:`normalize_tool_policy`
is the **identity function on every declaration that exists today**.  That is
the single most important property here: the enabling commit changes no
resolved tool set anywhere, the eight downstream ``== []`` consumers keep
working untouched, and DB-backed experts need no migration.  It is pinned by
``tests/test_tool_policy.py::TestIdentityProperty`` and by
``tests/test_config_tool_grants_snapshot.py``.

One category is narrower than the table above: **`shell` accepts `only` (or a
bare list) and `false` (or `[]`), and nothing else.** `true` and `except` both
auto-track the registry — `except` is `true` minus a fixed subtraction,
recomputed on every resolution — and a tool added to a code-execution category
must not reach live configs without a diff. See
:data:`ENUMERATE_ONLY_CATEGORIES`.

**Normalisation is layer-local.** Expanding a policy needs only the registry
and the value — never the parent layer.  So it runs per-layer at birth and the
existing ``deep_merge`` "lists replace, dicts merge" semantics carry the whole
layer model unmodified.  A consequence worth stating out loud: a child
``{except: [c]}`` under a parent ``{only: [a, b]}`` resolves to the *whole
category* minus ``c``, which is wider than its parent.  That is intended; see
``TestMergeOrder.test_parent_only_child_except_WIDENS_and_that_is_intended``.

This module is also the **request boundary's** vocabulary — see
:func:`validate_tool_override_fragment`.  Config layers and API requests are
the same kind of object, so they get the same answer to "is this a legal
``tools.<category>`` value, and do these names live in this category?".  The
difference is only what a failure means: a config file raises at load, a
request gets a 400.

Design: ``knowledge-base/knowledge/features/tool_config_policy_vs_membership.md``.
"""

from __future__ import annotations

import logging
from typing import Any, Iterable

logger = logging.getLogger(__name__)

#: The sentinel ``mcp`` resolves to.  Expanded by
#: ``src.tools.registry.expand_tool_wildcards`` *after* MCP discovery.
MCP_WILDCARD = "*"

#: Config keys that are not registry categories but address one.  ``coding`` is
#: the pre-rename spelling of ``shell``, still honoured by ``ToolsConfig``
#: (``tools_data.get("shell", tools_data.get("coding", []))``).  No shipped
#: config uses it; it is mapped here so a policy value under it cannot become a
#: silent ``[]``.
LEGACY_CATEGORY_ALIASES: dict[str, str] = {"coding": "shell"}

#: Tool NAMES that no longer exist, keyed by the category they lived in, and
#: the tool a layer naming them means today.  ``spawn_subagent`` (the light
#: in-process reader) and ``delegate_work`` / ``resume_delegation_child`` (the
#: heavy child-job pair) were deleted in U3 WP4 with ``delegate_agent`` as the
#: one delegation tool — a hard rename, no alias tool.  Stored DB expert
#: fragments, job/thread overrides and frozen blobs that still grant the old
#: names are mapped here (deduplicated, logged once per layer), so no row needs
#: a data migration and the request boundary keeps accepting a fragment the
#: cockpit re-submits from an old row.  Only lists and mappings are affected;
#: a name outside its home category still fails the foreign-name gate.
LEGACY_TOOL_NAME_ALIASES: dict[str, dict[str, str]] = {
    "delegation": {
        "spawn_subagent": "delegate_agent",
        "delegate_work": "delegate_agent",
        "resume_delegation_child": "delegate_agent",
    },
}

#: Tool names that moved to another category, keyed by the category they left.
#: ``use_skill`` was a ``workspace`` member until 2026-10-15, when it became the
#: ``skills`` category so a rewritten ``workspace`` list could no longer drop
#: it (knowledge-base/knowledge/features/skills_tool_category.md D5). Stored
#: layers and requests that still name it under ``workspace`` are rewritten
#: layer-locally by :func:`_move_relocated_tool_names`.
RELOCATED_TOOL_NAMES: dict[str, dict[str, str]] = {
    "workspace": {"use_skill": "skills"},
}

#: A ``true`` a STORED layer may still carry for a category that has since
#: become enumerate-only, and the exact list it means.  ``tools.delegation:
#: true`` was legal (it expanded to ``[spawn_subagent]``) until U3 WP4 made
#: the category enumerate; jobs and threads created through the cockpit's
#: delegation toggle in that window carry the raw ``true`` in their stored
#: override, and a resume must keep resolving them.  Mapped by
#: :func:`normalize_tool_policy` (the config-layer seam) to THIS list — never
#: "all members", so U4's control-plane tools in the same category never land
#: under an old ``true`` unreviewed — with one warning per (source, category).
#: The request boundary (:func:`validate_tool_override_fragment`) keeps
#: refusing ``true``: a new request has the served enumeration to send.
LEGACY_TRUE_EXPANSIONS: dict[str, tuple[str, ...]] = {
    "delegation": ("delegate_agent",),
}

# One deprecation warning per (source, category, renamed names) — sessions
# re-normalise their layers on every attach, so an unbounded log would spam.
_ALIAS_LOG_SEEN: set[tuple] = set()
_ALIAS_LOG_SEEN_MAX = 1024


def _log_legacy_tool_names_once(
    source: str, category: str, renamed: dict[str, str]
) -> None:
    key = (source, category, tuple(sorted(renamed)))
    if key in _ALIAS_LOG_SEEN:
        return
    if len(_ALIAS_LOG_SEEN) >= _ALIAS_LOG_SEEN_MAX:
        _ALIAS_LOG_SEEN.clear()
    _ALIAS_LOG_SEEN.add(key)
    logger.warning(
        "config: legacy tool name(s) %s in tools.%s of %s → %s (deleted in U3; "
        "see universal_experts_and_subagents.md §0 D1/D2)",
        sorted(renamed),
        category,
        source,
        sorted(set(renamed.values())),
    )


def _move_relocated_tool_names(tools: dict[str, Any], *, source: str) -> dict[str, Any]:
    """Rewrite a layer's ``tools`` mapping for :data:`RELOCATED_TOOL_NAMES`.

    Layer-local, like every other normalisation here. A name listed in its old
    category (bare list or ``only``) leaves it and is granted in its new one,
    unless this layer already says something about the new category, which
    then wins. A name in an old category's ``except`` asked for the tool to be
    off, so the new category becomes ``[]`` (again unless the layer sets it);
    an ``except`` left empty is ``true``. Returns ``tools`` itself when nothing
    moves.
    """
    moved: dict[str, list[str]] = {}
    out = dict(tools)
    for old_category, relocations in RELOCATED_TOOL_NAMES.items():
        value = out.get(old_category)
        if isinstance(value, (list, tuple)):
            listed, key, granted = list(value), None, True
        elif isinstance(value, dict) and isinstance(value.get("only"), (list, tuple)):
            listed, key, granted = list(value["only"]), "only", True
        elif isinstance(value, dict) and isinstance(value.get("except"), (list, tuple)):
            listed, key, granted = list(value["except"]), "except", False
        else:
            continue
        hits = [name for name in listed if name in relocations]
        if not hits:
            continue
        kept = [name for name in listed if name not in relocations]
        if key is None:
            out[old_category] = kept
        elif key == "only":
            out[old_category] = {**value, "only": kept} if kept else []
        else:
            out[old_category] = {**value, "except": kept} if kept else True
        for name in hits:
            new_category = relocations[name]
            moved.setdefault(new_category, []).append(name)
            if new_category in tools:
                continue
            current = out.setdefault(new_category, [])
            if granted and name not in current:
                current.append(name)
    if not moved:
        return tools
    log_key = (source, "relocated", tuple(sorted(moved)))
    if log_key not in _ALIAS_LOG_SEEN:
        if len(_ALIAS_LOG_SEEN) >= _ALIAS_LOG_SEEN_MAX:
            _ALIAS_LOG_SEEN.clear()
        _ALIAS_LOG_SEEN.add(log_key)
        logger.info(
            "config: %s names %s under an old tool category; moved to %s "
            "(see skills_tool_category.md D5)",
            source,
            sorted(n for names in moved.values() for n in names),
            sorted(moved),
        )
    return out


def _expand_legacy_true(category: str, *, source: str) -> list[str]:
    """``tools.<category>: true`` on a stored layer of a now enumerate-only
    category → the fixed :data:`LEGACY_TRUE_EXPANSIONS` list, logged once per
    (source, category)."""
    names = list(LEGACY_TRUE_EXPANSIONS[category])
    key = (source, category, True)
    if key not in _ALIAS_LOG_SEEN:
        if len(_ALIAS_LOG_SEEN) >= _ALIAS_LOG_SEEN_MAX:
            _ALIAS_LOG_SEEN.clear()
        _ALIAS_LOG_SEEN.add(key)
        logger.warning(
            "config: legacy `tools.%s: true` in %s → %s (%s enumerates since U3; "
            "write {only: %s} — see universal_experts_and_subagents.md §0 D7)",
            category,
            source,
            names,
            category,
            names,
        )
    return names


def _rename_legacy_tool_names(
    names: list[str], category: str, *, source: str
) -> list[str]:
    """Map deleted tool names onto their successor; identity when none occur."""
    aliases = LEGACY_TOOL_NAME_ALIASES.get(category)
    if not aliases or not any(name in aliases for name in names):
        return names
    out: list[str] = []
    renamed: dict[str, str] = {}
    for name in names:
        target = aliases.get(name)
        if target is not None:
            renamed[name] = target
            name = target
        if name not in out:
            out.append(name)
    _log_legacy_tool_names_once(source, category, renamed)
    return out


#: Categories that must enumerate their tools and may never auto-track the
#: registry.  ``only`` (and its legacy spelling, a bare list) is the sole form
#: that does not auto-track: ``true`` means "this category and whatever is
#: added to it later", and ``{except: [...]}`` means *exactly the same thing*
#: minus a fixed subtraction, recomputed from the registry on every
#: resolution.  For a code-execution category both are the wrong default — a
#: tool added to ``shell`` in the registry would land in every config that used
#: either form, with no diff to review anywhere.  ``only`` forces a titled
#: commit.  ``false`` (and ``[]``) stay legal: turning a category off carries no
#: membership and cannot acquire anything.
#:
#: Note this is *not* the mode-alias argument.  ``run_command`` /
#: ``shell_execute`` being rewritten by ``get_all_tool_names`` from
#: ``extra.shell.mode`` makes naming both halves redundant, not dangerous, and
#: ``config/experts/bughunter`` already names both today.
#:
#: ``delegation`` joined in U3 WP4: its one tool, ``delegate_agent``, is
#: ``grant: "explicit"`` (``true`` must never hand out a spawn tool — the
#: design's D7), so ``true`` would expand to ``[]`` and read as "off" while
#: the settings toggle believed it turned delegation on.  Refusing ``true``
#: makes the category enumerate like ``shell`` — the cockpit already handles
#: that generically through ``enumerate_only_members`` — and the U4 control
#: plane (``wait_agent`` / ``message_agent`` / ``stop_agent`` /
#: ``list_agents``) will land in the same category without silently reaching
#: every config that said ``true``.
ENUMERATE_ONLY_CATEGORIES: frozenset[str] = frozenset({"shell", "delegation"})

_POLICY_KEYS = ("only", "except")


def _enumerate_only_error(category: str, form: str) -> "ToolPolicyError":
    return ToolPolicyError(
        f"tools.{category}: {form} is not accepted. {category} must enumerate "
        f"its tools — write {{only: [...]}} (or a bare list, the same thing) "
        f"or false. Both `true` and `except` auto-track the registry, so a tool "
        f"added to the {category} category later would land here with no diff "
        f"to review; for a code-execution or delegation category that is the "
        f"wrong default. "
        f"See knowledge-base/knowledge/features/tool_config_policy_vs_membership.md, "
        f"'`shell` accepts `only` and `false`'."
    )


class ToolPolicyError(ValueError):
    """A ``tools.<category>`` value is not a legal policy or membership form."""


def _registry():
    """Read the common catalog without importing live tool factories.

    The agent runtime updates this same process-local metadata object when
    MCP tools are discovered. The list forms never need to import it.
    """
    from shared import tool_catalog as registry

    return registry


def config_tool_categories() -> frozenset[str]:
    """Every category a config may address.

    The registry's categories plus ``mcp``, which has no static membership
    because ``register_mcp_tools`` discovers it at runtime.  This is the set
    ``ToolsConfig`` mirrors as dataclass fields and ``config/schema.json``
    mirrors as ``tools`` properties; the three are pinned to agree by
    ``tests/test_tool_policy.py::TestCategoryVocabularyAgreement``.
    """
    return frozenset(_registry().get_categories()) | {"mcp"}


def _grantable(category: str) -> list[str]:
    """The category's members that *config* is allowed to grant, sorted.

    Excludes both classified tiers, for the same reason but not the same rule:

    * ``grant: "code"`` — runtime code binds it instead of config, on a gate
      that lives somewhere else entirely (``officer.enabled``,
      ``cloud_mount.active``, an attached datasource).  Excluding these is what
      makes ``core: true`` reproduce today's six tools exactly.
    * ``grant: "explicit"`` — config may grant it, but only by naming it.
      Holds a closed session group's ``true`` expansion at exactly
      ``SESSION_TOOL_OVERRIDE_NAMES`` where the category mixes tiers
      (``job_control``, which holds ``steer_job``). The ``*_bundle``
      writes no longer need it: they live in ``catalog_authoring``, so
      ``agent_catalog: true`` cannot reach them structurally rather than by
      exception.

    Neither mark affects an explicitly written name — see
    :func:`expand_tool_policy`.
    """
    reg = _registry()
    return sorted(
        name
        for name in reg.get_tools_by_category(category)
        if "grant" not in reg.TOOL_REGISTRY[name]
    )


def _enumerable(category: str) -> list[str]:
    """The category's members an ``only`` list may name: everything config
    grants, whether by policy (unmarked) or by naming (``grant: "explicit"``)
    — i.e. all but the ``grant: "code"`` tier."""
    reg = _registry()
    return sorted(
        name
        for name in reg.get_tools_by_category(category)
        if reg.TOOL_REGISTRY[name].get("grant") != "code"
    )


def expand_category_true(category: str) -> list[str]:
    """Expand ``tools.<category>: true``.

    Raises:
        ToolPolicyError: for an unknown category, or for ``shell`` — see the
            mode-alias note below.
    """
    _require_known_category(category)

    # ``get_tools_by_category("mcp")`` is PROCESS-LOCAL: ``register_mcp_tools``
    # mutates the global registry per job/session and is never called in the
    # orchestrator process, where resolution happens.  A registry-derived
    # expansion would be ``[]`` in the orchestrator and a different,
    # session-dependent set in every agent.  Keep the existing sentinel.
    if category == "mcp":
        return [MCP_WILDCARD]

    if category in ENUMERATE_ONLY_CATEGORIES:
        raise _enumerate_only_error(category, "true")

    names = _grantable(category)
    if category == "orchestrator":
        # ``tools.orchestrator: true`` predates the 0156 job-group split, and
        # 0156's non-array guard skipped boolean policy values whole-row — so
        # stored boolean rows still mean the PRE-SPLIT category, which
        # included the job tools. Expand the boolean to that pre-split union
        # (today's orchestrator members plus the grantable
        # job_control/job_inspection members) so legacy rows do not silently
        # lose their job surface. Explicit lists are untouched: a written
        # ``orchestrator: [...]`` stays exactly as written.
        names = sorted(
            {*names, *_grantable("job_control"), *_grantable("job_inspection")}
        )
    if not names:
        # Whole-category ``grant: "code"``.  The expansion is correct — config
        # grants nothing here — but it reads backwards (``sql: true`` resolving
        # to the same ``[]`` that means "off"), so say so rather than let it
        # look like a policy that worked.
        logger.warning(
            "tools.%s: true expands to [] — every tool in this category is "
            "code-granted (%s decides). Config does not manage this category; "
            "delete the key rather than declaring it.",
            category,
            _registry().CODE_GRANTED_CATEGORIES.get(category, "runtime code"),
        )
    return names


def enumerate_only_members() -> dict[str, list[str]]:
    """Category -> the enumeration a caller must send to turn it ON.

    :data:`ENUMERATE_ONLY_CATEGORIES` refuses ``true`` because ``true``
    auto-tracks the registry and a code-execution category must not acquire a
    tool without a diff.  That rule is right, and it leaves a client with no
    way to *ask* for the category — the New Session form cannot offer "shell
    on" unless something tells it the four names to write.

    Serving them from the registry is what stops that becoming a fifth
    hand-maintained parallel list in the cockpit.  The enumeration a caller
    echoes back is frozen at the moment it is written, so the auto-tracking
    hazard does not follow it: a tool added to ``shell`` tomorrow does not
    appear in a session created today.

    Every member config may name: the ``true`` expansion's grantable subset
    plus the ``grant: "explicit"`` tier — an ``only`` list is exactly how an
    explicit tool is granted (``delegation``'s one member is explicit; served
    here, it is enablable at all).  Never a ``grant: "code"`` name, which in
    an ``only`` list would assert config manages it.
    """
    return {
        category: _enumerable(category)
        for category in sorted(ENUMERATE_ONLY_CATEGORIES)
    }


def expand_tool_policy(
    value: Any, category: str, *, source: str = "config-layer"
) -> list[str]:
    """Resolve one ``tools.<category>`` value to the canonical ``list[str]``.

    Layer-local: consults only ``value`` and the registry, never a parent
    layer.  Always returns a fresh list.  ``source`` labels the layer in the
    deprecation warning a :data:`LEGACY_TOOL_NAME_ALIASES` hit logs.

    ``only`` and a bare list are returned **as written** — never intersected
    with the ``true`` expansion.  That asymmetry is the entire point of the
    ``explicit`` tier, and it is load-bearing: ``config/experts/centurion``
    names ``steer_job`` and ``get_stuck_jobs``, both ``explicit``, and
    an intersection would silently revoke them.
    """
    _require_known_category(category)

    if value is True:
        return expand_category_true(category)
    if value is False:
        return []
    if isinstance(value, (list, tuple)):
        return _as_name_list(value, category, "tools.%s" % category, source=source)
    if isinstance(value, dict):
        return _expand_mapping(value, category, source=source)

    raise ToolPolicyError(
        f"tools.{category}: unsupported value {value!r} "
        f"({type(value).__name__}). Write true, false, a list of tool names, "
        f"{{only: [...]}} or {{except: [...]}}."
    )


def normalize_tool_policy(fragment: Any, *, source: str = "config-layer") -> Any:
    """Return ``fragment`` with every ``tools.<category>`` in canonical form.

    Never mutates the input — config layers are caller-owned and several are
    re-used across resolutions.  A fragment with no ``tools`` mapping is
    returned unchanged, by identity.

    Idempotent, which is what makes the belt-and-braces placement at several
    call sites safe.  ``source`` names the layer (the same label
    ``normalize_llm_tiers`` uses) in the one-per-layer deprecation warning a
    deleted tool name (:data:`LEGACY_TOOL_NAME_ALIASES`) or a stored ``true``
    on a now enumerate-only category (:data:`LEGACY_TRUE_EXPANSIONS`) triggers.
    Both are config-layer compat only — the request boundary does not map them.

    Keys that address no known category pass through **verbatim**, preserving
    today's end-to-end silent-ignore for a typo like ``tools.workspaces``.
    ``config/schema.json``'s ``additionalProperties: false`` is what catches
    those, at authoring time, without changing runtime behaviour.
    """
    if not isinstance(fragment, dict):
        return fragment
    tools = fragment.get("tools")
    if not isinstance(tools, dict):
        return fragment

    tools = _move_relocated_tool_names(tools, source=source)
    known = config_tool_categories()
    normalized: dict[str, Any] = {}
    for key, value in tools.items():
        category = LEGACY_CATEGORY_ALIASES.get(key, key)
        if category not in known:
            normalized[key] = value
            continue
        if value is True and category in LEGACY_TRUE_EXPANSIONS:
            normalized[key] = _expand_legacy_true(category, source=source)
            continue
        normalized[key] = expand_tool_policy(value, category, source=source)

    return {**fragment, "tools": normalized}


def assert_tool_policy_canonical(tools_data: Any, *, where: str) -> None:
    """Raise unless every addressable ``tools.<category>`` is already a list.

    The last line of defence, immediately before ``ToolsConfig``.  Its fields
    are typed ``List[str]`` and ``get_all_tool_names`` silently returns ``[]``
    for a non-list, so a ``true`` leaking this far would **silently disable**
    the group — the exact failure mode this whole feature exists to repair.
    Raise rather than coerce: a policy value arriving here is a missed
    :func:`normalize_tool_policy` call site, not an input.
    """
    if not isinstance(tools_data, dict):
        return
    known = config_tool_categories()
    for key, value in tools_data.items():
        category = LEGACY_CATEGORY_ALIASES.get(key, key)
        if category not in known:
            continue
        if not isinstance(value, list):
            raise ToolPolicyError(
                f"tools.{key} reached {where} as {type(value).__name__} "
                f"({value!r}). Tool policy must be normalised before the "
                f"dataclass — call normalize_tool_policy() on this fragment. "
                f"See knowledge-base/knowledge/features/tool_config_policy_vs_membership.md."
            )


def validate_tool_override_fragment(
    config_override: Any, *, source: str = "request"
) -> dict[str, list[str]]:
    """Validate a **request-layer** ``tools`` mapping against the registry.

    The one tool vocabulary for every write boundary: session create, session
    runtime update, and job create.  A ``tools.<category>`` fragment is
    accepted when the category is real and every name in it belongs to that
    category *per the registry*.  Anything else raises
    :class:`ToolPolicyError`, which each boundary turns into a 400.

    **Reject, never drop.**  The predecessor
    (``session_tool_overrides.validate_session_tool_overrides``) honoured four
    hand-curated groups and silently discarded the rest, so unticking eight of
    the twelve categories the New Session form renders was accepted and thrown
    away — a restriction shown to the user and never applied.  Silence is the
    defect; a boundary that will not honour a fragment must say so.

    Membership is the **whole** category, not
    :func:`expand_category_true`'s grantable subset.  ``grant: "code"`` and
    ``grant: "explicit"`` restrict *category-level policy*, not an explicit
    name — ``config/experts/centurion`` names ``steer_job`` and
    ``get_stuck_jobs`` — so treating "not in ``true``'s expansion" as "not
    allowed" would make this validator a second, stricter grant system.  It is
    not one: capability grants (``src/shared/runtime/core/capability_grants.py``) remain the
    PDP.  This is a shape-and-vocabulary gate and nothing more.

    ``mcp`` has no static registry membership — ``register_mcp_tools``
    discovers it per session, and never in the orchestrator process.  Names
    there cannot be checked positively, so the rule inverts: a name the
    registry knows under a *different* category is rejected (that is the
    smuggle), and an unrecognised one passes as a presumed runtime-discovered
    MCP tool.

    Returns the accepted mapping keyed by **canonical** category, with every
    value normalised to the ``list[str]`` the rest of the stack speaks — so a
    caller can assign it straight back over ``config_override["tools"]`` and
    know the PDP downstream reads canonical lists rather than raw policy
    values (see the ``_truthy`` table in
    ``knowledge-base/knowledge/features/tool_config_policy_vs_membership.md``).  A fragment with no
    ``tools`` key returns ``{}``.
    """
    if not isinstance(config_override, dict) or "tools" not in config_override:
        return {}

    tools = config_override.get("tools")
    if not isinstance(tools, dict):
        raise ToolPolicyError(
            f"tools: expected an object keyed by tool category; got "
            f"{type(tools).__name__} ({tools!r})."
        )

    tools = _move_relocated_tool_names(tools, source=source)
    known = config_tool_categories()
    accepted: dict[str, list[str]] = {}
    claimed_by: dict[str, str] = {}
    for key, value in tools.items():
        if not isinstance(key, str):
            raise ToolPolicyError(f"tools: category keys must be strings; got {key!r}.")
        category = LEGACY_CATEGORY_ALIASES.get(key, key)
        if category not in known:
            raise ToolPolicyError(
                f"tools.{key}: unknown tool category. Known categories: "
                f"{', '.join(sorted(known))}."
            )
        if category in accepted:
            # Only reachable through an alias (``coding`` + ``shell``).  Two
            # values for one category is unresolvable, and picking one is the
            # silent drop this function exists to remove.
            raise ToolPolicyError(
                f"tools.{key} and tools.{claimed_by[category]} both address the "
                f"{category} category. Write one of them."
            )
        claimed_by[category] = key
        names = expand_tool_policy(value, category, source=source)
        if not names and _is_affirmative(value):
            # The caller asked for ON and the expansion is empty, because every
            # tool here is ``grant: "code"``.  ``expand_category_true`` logs a
            # warning and returns ``[]``, which is right for a config layer and
            # wrong for a request: returning 200 on "turn SQL on" while turning
            # nothing on is *accepted and discarded*, the very thing this
            # boundary exists to remove.  ``false`` / ``[]`` stay legal — asking
            # to turn off a category nobody manages is a harmless no-op.
            raise ToolPolicyError(
                f"tools.{key}: this would turn nothing on. Every tool in the "
                f"{category} category is granted by runtime code "
                f"({_registry().CODE_GRANTED_CATEGORIES.get(category, 'runtime code')} "
                f"decides), so config cannot enable it. Drop the key; write "
                f"false only if you meant to assert it is off."
            )
        # The foreign-name gate closes the smuggle where a caller NAMES a
        # tool under the wrong key. Affirmative forms (``true`` and
        # ``{except: ...}``) derive their names from our own registry
        # expansion — ``except``'s named exclusions are already validated
        # inside ``_expand_mapping`` — so the result-level check is vacuous
        # for them, and it must not fire on the one deliberate exception:
        # ``orchestrator: true`` expanding to the pre-split union that
        # includes job_control/job_inspection members (see
        # ``expand_category_true``).
        foreign = () if _is_affirmative(value) else _foreign_tool_names(names, category)
        if foreign:
            raise ToolPolicyError(
                f"tools.{key}: {', '.join(foreign)}. A tool list may only name "
                f"tools of its own category — the loader resolves a name "
                f"against the global registry, not against the key it arrived "
                f"under, so a foreign name would bind the foreign tool."
            )
        accepted[category] = names
    return accepted


# ---------------------------------------------------------------------------
# internals
# ---------------------------------------------------------------------------
def _is_affirmative(value: Any) -> bool:
    """Whether a policy value asks for tools rather than for none.

    ``true`` and ``{except: [...]}`` are the two forms that can *request* a
    non-empty set and still expand to ``[]`` (a wholly ``grant: "code"``
    category).  A bare list and ``{only: [...]}`` cannot — both are refused
    when empty — and ``false`` / ``[]`` are the negative forms.
    """
    return value is True or (isinstance(value, dict) and "except" in value)


def _foreign_tool_names(names: list[str], category: str) -> list[str]:
    """Describe every name in ``names`` that does not belong to ``category``."""
    reg = _registry()
    if category == "mcp":
        offenders = [
            name
            for name in names
            if name != MCP_WILDCARD
            and (reg.TOOL_REGISTRY.get(name) or {}).get("category") not in (None, "mcp")
        ]
    else:
        members = set(reg.get_tools_by_category(category))
        offenders = [name for name in names if name not in members]

    described: list[str] = []
    for name in sorted(set(offenders)):
        home = (reg.TOOL_REGISTRY.get(name) or {}).get("category")
        described.append(
            f"'{name}' is in tools.{home}"
            if home
            else f"'{name}' is not a registered tool"
        )
    return described


def _require_known_category(category: str) -> None:
    if category not in config_tool_categories():
        raise ToolPolicyError(
            f"tools.{category}: unknown tool category. Known categories: "
            f"{', '.join(sorted(config_tool_categories()))}."
        )


def _as_name_list(
    value: Iterable[Any], category: str, label: str, *, source: str = "config-layer"
) -> list[str]:
    names = list(value)
    bad = [n for n in names if not isinstance(n, str)]
    if bad:
        raise ToolPolicyError(
            f"{label}: expected a list of tool-name strings; got {bad!r}."
        )
    return _rename_legacy_tool_names(names, category, source=source)


def _expand_mapping(
    value: dict, category: str, *, source: str = "config-layer"
) -> list[str]:
    keys = set(value)
    present = [k for k in _POLICY_KEYS if k in keys]

    if not keys:
        raise ToolPolicyError(
            f"tools.{category}: an empty mapping is a wordy spelling of false. "
            f"Write false. (It is also a live hazard: the dispatch PDP's "
            f"_truthy() reads {{}} as false and would miss a grant violation.)"
        )
    unknown = sorted(keys - set(_POLICY_KEYS))
    if unknown:
        raise ToolPolicyError(
            f"tools.{category}: unknown policy key(s) {unknown}. "
            f"A mapping takes exactly one of 'only' or 'except'."
        )
    if len(present) != 1:
        raise ToolPolicyError(
            f"tools.{category}: 'only' and 'except' in one mapping. The two "
            f"intuitive readings (only minus except, or one wins) diverge, and "
            f"the combination is reachable by accident through deep_merge. "
            f"Write one or the other."
        )

    key = present[0]
    names = _as_name_list(
        _require_list(value[key], category, key),
        category,
        f"tools.{category}.{key}",
        source=source,
    )

    if key == "only":
        if not names:
            raise ToolPolicyError(
                f"tools.{category}: an empty 'only' is a wordy spelling of "
                f"false. Write false. (It is also a live hazard: the dispatch "
                f"PDP's _truthy() reads {{only: []}} as true and would "
                f"fabricate a grant violation for an empty group.)"
            )
        # AS WRITTEN.  Never intersected with expand_category_true — an
        # explicit name is an explicit name, for both `code` and `explicit`
        # marks.  Intersecting would silently revoke centurion's
        # steer_job / get_stuck_jobs.
        return names

    # Checked before the empty-list case so `{except: []}` on an
    # enumerate-only category is told the real rule rather than "write true",
    # which is itself refused there.
    if category in ENUMERATE_ONLY_CATEGORIES:
        raise _enumerate_only_error(category, "'except'")
    if category == "mcp":
        raise ToolPolicyError(
            "tools.mcp: 'except' is not meaningful — MCP tool names are "
            "discovered at runtime, so mcp has no registry membership to "
            "subtract from. Write true (the '*' sentinel), false, or an "
            "explicit list."
        )
    if not names:
        raise ToolPolicyError(
            f"tools.{category}: an empty 'except' is a wordy spelling of true. "
            f"Write true."
        )

    members = set(_registry().get_tools_by_category(category))
    foreign = sorted(set(names) - members)
    if foreign:
        raise ToolPolicyError(
            f"tools.{category}: 'except' names {foreign}, which are not in the "
            f"{category} category. 'except' subtracts from that category and "
            f"nothing else."
        )

    excluded = set(names)
    return [n for n in _grantable(category) if n not in excluded]


def _require_list(value: Any, category: str, key: str) -> list:
    if not isinstance(value, (list, tuple)):
        raise ToolPolicyError(
            f"tools.{category}.{key}: expected a list of tool names; got "
            f"{type(value).__name__}."
        )
    return list(value)


__all__ = [
    "ENUMERATE_ONLY_CATEGORIES",
    "LEGACY_CATEGORY_ALIASES",
    "LEGACY_TOOL_NAME_ALIASES",
    "LEGACY_TRUE_EXPANSIONS",
    "MCP_WILDCARD",
    "ToolPolicyError",
    "assert_tool_policy_canonical",
    "config_tool_categories",
    "enumerate_only_members",
    "expand_category_true",
    "expand_tool_policy",
    "normalize_tool_policy",
    "validate_tool_override_fragment",
]
