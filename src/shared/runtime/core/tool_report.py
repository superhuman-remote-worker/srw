"""What toolset does this agent actually have? — one answer, measured not guessed.

Nothing in the system could answer that question, which is why a capability
regression survived eleven days and why ``GET .../tool-groups`` shipped as a
*lean* orchestrator-side re-derivation covering 4 of 24 categories.

This module is the shared vocabulary for the fix.  The governing decision (D6,
``knowledge-base/knowledge/issues/tool_configuration_defects_and_fix_roadmap.md``) is that **the
resolved answer comes from the agent, not from a re-implementation**: the agent
binds the toolset, so the agent reports it and the API serves that.  Rebuilding
the runtime-injection layer orchestrator-side would be a second implementation
of one fact, and divergence between two implementations of one fact is the
original bug in this whole series.

So there are three kinds of answer and they are never interchangeable:

``origin="agent"``
    **Measured, in full.**  A running agent enumerated ``session.tools`` after
    binding and returned the structured report below.  ``observed_at`` and
    ``backend`` are set.

``origin="agent_partial"``
    **Measured, but only the names.**  The agent answered with a bound-tool
    list and nothing else — today that is an image predating
    ``GET /session/toolset``, whose ``/status`` publishes the same
    ``session.tools`` with no timestamp, no workspace capabilities, and no
    agent-side categorisation (so MCP tools land in
    :data:`UNCLASSIFIED_CATEGORY`).  The tool names are as trustworthy as
    ``"agent"``; everything *around* them is missing, and a caller that renders
    a workspace-tier explanation must not do so from this.

``origin="prediction"``
    **Forecast.**  No agent exists yet (the New Session form) or none is
    reachable.  Derived from the merged config, which cannot see the runtime
    injection layer or the backend capability gate — so it is *structurally*
    weaker, not merely older.  ``observed_at`` is ``None`` and
    ``prediction_reason`` says which case this is.

A prediction presented as a measurement is D1 violated at a new seam, and so is
a *degraded* measurement presented as a complete one — the second is subtler and
was shipped once here: with no ``backend``, the tier gate is invisible and a
no-shell session rendered its execution groups as ordinary unticked checkboxes.

``origin`` is therefore the discriminator, and the only one: do **not** infer
measured-ness from ``observed_at``, which is legitimately ``None`` on
``agent_partial``.  :data:`MEASURED_ORIGINS` is the check you want.

Three states, per D7 — a checkbox cannot express the truth D1 requires:

``on``           at least one tool of this category is bound
``off``          none bound, and turning the group on would work
``unavailable``  none bound, and no amount of ticking would change that

``unavailable`` always carries a ``reason``.  Every category also carries
``settable`` (may config turn this on at all?) and ``decided_by`` (which layer
produced the answer), because "off" and "off and you cannot change it" are
different facts and the UI has to draw them differently.

Kept framework-free so the agent runtime, the orchestrator and their tests all
share one definition and cannot drift.
"""

from __future__ import annotations

from typing import Any, Iterable, Mapping, Optional, Sequence

from shared.connectors.builtin import tool_categories

#: Wire-format version of the agent's report.  Bumped when the payload shape
#: changes incompatibly; the orchestrator refuses to read a version it does not
#: know rather than mis-parsing it into a confident wrong answer.
REPORT_VERSION = 1

#: Bucket for a bound tool name the *reader's* registry does not know.  Real in
#: the orchestrator: MCP tools are registered into the agent's process registry
#: at attach (``register_mcp_tools``) and are invisible anywhere else, so a flat
#: name list categorised outside the agent loses them.  Named rather than
#: dropped — a tool the agent holds and the report omits is the bug we are here
#: to kill.
UNCLASSIFIED_CATEGORY = "unclassified"

STATE_ON = "on"
STATE_OFF = "off"
STATE_UNAVAILABLE = "unavailable"

ORIGIN_AGENT = "agent"
ORIGIN_AGENT_PARTIAL = "agent_partial"
ORIGIN_PREDICTION = "prediction"

#: The origins whose ``tools`` came from a running agent. Use this rather than
#: ``== ORIGIN_AGENT`` wherever the question is "is this measured?", so a
#: degraded measurement is not mistaken for a forecast.
MEASURED_ORIGINS: frozenset[str] = frozenset({ORIGIN_AGENT, ORIGIN_AGENT_PARTIAL})

DECIDED_BY_GRANT = "grant"
DECIDED_BY_BACKEND = "backend"
DECIDED_BY_RUNTIME = "runtime"
DECIDED_BY_REGISTRY = "registry"
DECIDED_BY_UNSET = "unset"

#: Authored config layers, least specific first — the order ``resolve_config``
#: merges them in (``orchestrator/services/config_resolver.resolve_config``).
#: ``deep_merge`` replaces lists wholesale, so for ``tools.<category>`` the
#: LAST layer naming the category owns it outright.  That makes provenance a
#: lookup rather than a diff.
CONFIG_LAYERS: tuple[str, ...] = (
    "base",
    "base_defaults",
    "expert",
    "project",
    "db",
    "user",
    "request",
)

#: Capability grant -> the categories it gates, mirroring
#: ``src.core.capability_grants.evaluate``.  That function is the PDP and stays
#: the only enforcement; this is the *explanation*, which is why
#: ``tests/test_tool_report.py::TestGrantMapMatchesThePDP`` re-derives it by
#: running ``evaluate`` per category rather than trusting this literal.
GRANT_GATED_CATEGORIES: dict[str, frozenset[str]] = {
    "shell_tools": frozenset({"shell"}),
    "browser": frozenset({"browser_direct"}),
    "delegation": frozenset({"delegation"}),
    "datasource_tools": frozenset(tool_categories()),
    "catalog_authoring": frozenset({"catalog_authoring"}),
}

_GRANT_REASONS: dict[str, str] = {
    "shell_tools": "requires the shell_tools capability grant",
    "browser": "the browser capability grant is not held",
    "delegation": "requires the delegation capability grant",
    "datasource_tools": "the datasource_tools capability grant is not held",
    "catalog_authoring": "requires the catalog_authoring capability grant",
}


class ToolReportError(ValueError):
    """An agent toolset report is unreadable — wrong version or wrong shape."""


# ---------------------------------------------------------------------------
# vocabulary
# ---------------------------------------------------------------------------
def report_categories() -> list[str]:
    """Every category the report covers, sorted.

    The same vocabulary ``tools.<category>`` may address
    (:func:`shared.runtime.core.tool_policy.config_tool_categories`), so a category a
    config can name is always a category the report answers for — including
    ``mcp``, which has no static registry membership.
    """
    from shared.runtime.core.tool_policy import config_tool_categories

    return sorted(config_tool_categories())


def categorize_tool_names(names: Iterable[str]) -> dict[str, list[str]]:
    """Group bound tool names by their registry category, author order kept.

    Classification only — this never decides *whether* a tool is bound, it
    reads a list of names that already are.  Names the local registry does not
    know land in :data:`UNCLASSIFIED_CATEGORY`.
    """
    from shared.tool_catalog import TOOL_REGISTRY

    out: dict[str, list[str]] = {}
    for name in names:
        meta = TOOL_REGISTRY.get(name) or {}
        category = meta.get("category") or UNCLASSIFIED_CATEGORY
        out.setdefault(category, []).append(name)
    return out


def backend_capabilities(backend: Any) -> Optional[dict[str, bool]]:
    """The three capabilities ``filter_tools_by_backend`` gates on.

    ``None`` for ``backend=None``, mirroring that function's no-op: an absent
    backend gates nothing, and reporting ``False`` for it would invent a
    restriction the agent is not applying.
    """
    if backend is None:
        return None
    return {
        "supports_shell": bool(getattr(backend, "supports_shell", False)),
        "supports_file_tools": bool(getattr(backend, "supports_file_tools", True)),
        "supports_canvas_presentation": bool(
            getattr(backend, "supports_canvas_presentation", False)
        ),
    }


def backend_blocked_categories(caps: Optional[Mapping[str, Any]]) -> dict[str, str]:
    """Category -> why this workspace backend cannot carry it.

    Derived from ``registry._EXECUTION_CATEGORIES`` rather than a second list of
    the same three names.
    """
    if not caps:
        return {}
    from shared.tool_catalog import _EXECUTION_CATEGORIES

    blocked: dict[str, str] = {}
    if not caps.get("supports_shell", False):
        for category in _EXECUTION_CATEGORIES:
            blocked[category] = (
                "this workspace tier has no shell, so the backend capability "
                "gate drops these tools"
            )
    if not caps.get("supports_file_tools", True):
        blocked["workspace"] = "this workspace tier has no file tools"
        blocked["canvas"] = "this workspace tier has no file tools"
    if not caps.get("supports_canvas_presentation", False):
        blocked.setdefault(
            "canvas", "this workspace tier cannot materialise canvas documents"
        )
    return blocked


def grant_blocked_categories(grants: Optional[Mapping[str, Any]]) -> dict[str, str]:
    """Category -> why the owner's capability grants forbid it.

    ``grants=None`` is the admin bypass (``_resolve_runner_grants`` returns
    ``None`` for an admin) and blocks nothing.
    """
    if grants is None:
        return {}
    # An absent key falls back to the catalog default, read exactly the way the
    # PDP reads it (``grants.get(key, <default>)`` in ``capability_grants``).
    from shared.runtime.core.capability_grants import CATALOG

    blocked: dict[str, str] = {}
    for key, categories in GRANT_GATED_CATEGORIES.items():
        default = CATALOG.get(key, {}).get("default", False)
        if not grants.get(key, default):
            for category in categories:
                blocked.setdefault(category, _GRANT_REASONS[key])
    return blocked


def code_granted_categories() -> dict[str, str]:
    """Category -> the gate that grants it, for categories config cannot set."""
    from shared.tool_catalog import CODE_GRANTED_CATEGORIES

    return {
        category: f"granted by the runtime, not by config ({gate})"
        for category, gate in CODE_GRANTED_CATEGORIES.items()
    }


def code_granted_tools() -> dict[str, str]:
    """Tool name -> the gate that binds it, for every ``grant: "code"`` tool.

    The per-TOOL tier of the same classification :func:`code_granted_categories`
    reports per category, and the one the category map structurally cannot see:
    ``srw_cloud_status`` sits in ``shell``, ``sleep`` / ``notify_user`` /
    ``request_workspace_upgrade`` in ``core`` and ``checkout_project_repository``
    in ``orchestrator`` — three categories config manages heavily. The agent
    appends exactly these AFTER the merge
    (``src/agent/api/persistent_session.py::_load_tools_for_backend``), so config can
    neither grant nor revoke them.

    Read off ``TOOL_REGISTRY`` rather than listed, so a new re-append site that
    carries the mark is covered without editing this module — and one that does
    not carry the mark is a registry omission, which is the right place to fix
    it.
    """
    from shared.tool_catalog import TOOL_REGISTRY

    return {
        name: str(meta.get("gate") or "runtime code")
        for name, meta in TOOL_REGISTRY.items()
        if meta.get("grant") == "code"
    }


# ---------------------------------------------------------------------------
# the agent's half — measurement
# ---------------------------------------------------------------------------
def build_agent_toolset_report(
    *,
    thread_id: Optional[str],
    tool_names: Sequence[str],
    backend: Any = None,
    observed_at: Optional[str] = None,
) -> dict[str, Any]:
    """The agent's wire payload: the toolset it ACTUALLY bound.

    ``tool_names`` must be read off the live ``session.tools`` objects, after
    ``load_tools`` / ``filter_tools_by_backend`` / the never-bind-zero floor —
    i.e. the same list the model is offered.  Anything derived from the config
    instead would make this a prediction wearing a measurement's label.

    Categorisation happens HERE, in the agent process, because only this
    process has MCP tools in its registry.
    """
    from datetime import datetime, timezone

    names = [n for n in tool_names if isinstance(n, str)]
    return {
        "version": REPORT_VERSION,
        "thread_id": thread_id,
        "observed_at": observed_at
        or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "backend": backend_capabilities(backend),
        "tools": names,
        "categories": categorize_tool_names(names),
    }


def read_agent_toolset_report(payload: Any) -> dict[str, list[str]]:
    """Extract ``category -> bound names`` from an agent report.

    Raises:
        ToolReportError: unknown version or unusable shape.  Refusing is the
            point — a mis-parsed report would be served as a *measurement*, and
            a confidently wrong measurement is worse than an honest prediction.
    """
    if not isinstance(payload, dict):
        raise ToolReportError(f"agent report is {type(payload).__name__}, not a dict")
    version = payload.get("version")
    if version != REPORT_VERSION:
        raise ToolReportError(
            f"agent report version {version!r} is not {REPORT_VERSION} — refusing "
            f"to read it as a measurement"
        )
    categories = payload.get("categories")
    if not isinstance(categories, dict):
        raise ToolReportError("agent report has no 'categories' mapping")
    out: dict[str, list[str]] = {}
    for category, names in categories.items():
        if not isinstance(category, str) or not isinstance(names, (list, tuple)):
            raise ToolReportError(f"agent report category {category!r} is malformed")
        out[category] = [n for n in names if isinstance(n, str)]
    return out


# ---------------------------------------------------------------------------
# the orchestrator's half — composition
# ---------------------------------------------------------------------------
def layer_provenance(layers: Sequence[tuple[str, Any]]) -> dict[str, str]:
    """Category -> the most specific authored layer that names it.

    ``layers`` is ``(label, fragment)`` pairs, least specific FIRST.  Because
    ``deep_merge`` replaces lists rather than merging them, the last layer
    naming a category owns its value outright — so "which layer decided it" is
    the last hit, not a reconciliation.
    """
    from shared.runtime.core.tool_policy import LEGACY_CATEGORY_ALIASES

    out: dict[str, str] = {}
    for label, fragment in layers:
        if not isinstance(fragment, dict):
            continue
        tools = fragment.get("tools")
        if not isinstance(tools, dict):
            continue
        for key in tools:
            out[LEGACY_CATEGORY_ALIASES.get(key, key)] = label
    return out


def compose_tool_view(
    *,
    measured: Optional[Mapping[str, Sequence[str]]],
    configured: Mapping[str, Sequence[str]],
    provenance: Optional[Mapping[str, str]] = None,
    backend_caps: Optional[Mapping[str, Any]] = None,
    grants: Optional[Mapping[str, Any]] = None,
) -> dict[str, dict[str, Any]]:
    """Per-category ``{state, reason, settable, decided_by, tools}``.

    ``measured`` is the agent's report (``None`` = no agent, so this is a
    prediction).  ``configured`` is the merged config's ``tools`` mapping,
    already normalised to ``list[str]`` by
    :func:`shared.runtime.core.tool_policy.normalize_tool_policy`.

    When ``measured`` is present it decides ``state`` outright.  ``configured``
    is then used ONLY to explain the answer — never to correct it.  That
    asymmetry is D6: the agent is the authority on what it bound, and anything
    that "fixes up" its answer has reintroduced the second implementation.

    **``off`` is a promise, and it is only made when it can be kept.**  On a
    measurement, a category whose merged config grants tools while the agent
    bound none is ``unavailable``, not ``off``: config already asked and the
    answer was no, so a checkbox there offers an "on" that writes config and
    changes no binding.  The live gate produced exactly that —
    ``knowledge: configured=10 kb_*, tools=0`` — and it also covers the case
    where the reason is a backend gate the reader cannot see (an agent report
    without ``backend``: the tools are still measurably absent even when the
    cause is not legible).  ``off`` therefore means "nothing here, and nothing
    has said no".

    **And ``on`` is revocable, or it is not a switch.**  The symmetric case:
    everything the agent bound is a per-tool code grant, so unticking writes
    ``[]``, the runtime re-appends after the merge, and the box comes back
    ticked — for the life of the session.  That is ``on`` with
    ``settable: False`` and a reason naming the tools and their gates, not a
    live checkbox.  ``srw_cloud_status`` makes it the default topology's
    behaviour for ``shell``.
    """
    provenance = provenance or {}
    blocked_by_grant = grant_blocked_categories(grants)
    blocked_by_backend = backend_blocked_categories(backend_caps)
    code_granted = code_granted_categories()
    code_granted_tool = code_granted_tools()

    known = set(report_categories())
    categories = sorted(known | set(measured or {}) | set(configured or {}))

    view: dict[str, dict[str, Any]] = {}
    for category in categories:
        conf = [n for n in (configured.get(category) or [])]
        if measured is None:
            bound = None
            # Creation previews know the selected tier. Configured tools which
            # cannot bind there must not be forecast as available. Measurements
            # below remain the agent's answer, including a contradictory one.
            tools = [] if category in blocked_by_backend else conf
        else:
            bound = [n for n in (measured.get(category) or [])]
            tools = bound

        grant_reason = blocked_by_grant.get(category)
        backend_reason = blocked_by_backend.get(category)
        code_reason = code_granted.get(category)

        # A measured category that config populated and the agent did not bind.
        # Weakest of the four reasons because it states the OUTCOME without the
        # cause: a backend gate, a knowledge store that never came up, a tool
        # that failed to instantiate. Ranked last so a legible cause always wins
        # the message, but it fires even when no cause is legible — which is the
        # whole point, since a degraded report carries no backend to blame.
        unbound_reason = None
        if bound is not None and conf and not bound:
            unbound_reason = (
                f"the merged config grants {len(conf)} tool(s) here and the "
                f"agent bound none, so enabling this group would not change "
                f"what it holds"
            )

        # ...and the mirror: **on is revocable**, or it is not offered as a
        # switch. Everything the agent bound here carries the registry's
        # per-tool ``grant: "code"`` mark, so the config that would remove it
        # does not exist — the agent re-appends these AFTER the merge. Unticking
        # writes ``tools.<c>: []`` and the next measurement reports the same
        # tools, forever, which is a checkbox that cannot lose. Fires only when
        # unticking would change *nothing*: a category whose bound set also
        # holds a config-granted name is an ordinary ticked box, because
        # unticking really does drop that name.
        locked_on_reason = None
        if bound and all(name in code_granted_tool for name in bound):
            gates = sorted({code_granted_tool[name] for name in bound})
            locked_on_reason = (
                f"the runtime binds {', '.join(bound)} here regardless of config "
                f"({'; '.join(gates)}), so unticking this group cannot release it"
            )

        # Precedence is deliberate: a denied grant is the strongest statement
        # (nothing about the workspace or the config can restore it), then the
        # workspace tier, then "config never managed this category at all",
        # then the two bare observations above — which are mutually exclusive,
        # one needing an empty bound set and the other a full one.
        reason = (
            grant_reason
            or backend_reason
            or code_reason
            or locked_on_reason
            or unbound_reason
        )
        settable = reason is None

        if tools:
            state = STATE_ON
        elif settable:
            state = STATE_OFF
        else:
            state = STATE_UNAVAILABLE

        if grant_reason:
            decided_by = DECIDED_BY_GRANT
        elif backend_reason:
            decided_by = DECIDED_BY_BACKEND
        elif bound is not None and sorted(bound) != sorted(conf):
            # The merged config and the bound set disagree: something after the
            # merge moved it — the runtime injection layer, or a code grant. A
            # per-tool code grant reads as ``registry`` for the same reason a
            # category-level one does: the registry's ``grant`` mark is what
            # classified it, and one rule is easier to keep honest than two.
            decided_by = (
                DECIDED_BY_REGISTRY
                if (code_reason or locked_on_reason)
                else DECIDED_BY_RUNTIME
            )
        elif code_reason:
            decided_by = DECIDED_BY_REGISTRY
        else:
            decided_by = provenance.get(category, DECIDED_BY_UNSET)

        entry: dict[str, Any] = {
            "state": state,
            "settable": settable,
            "reason": reason,
            "decided_by": decided_by,
            "tools": tools,
        }
        if bound is not None:
            entry["configured"] = conf
        view[category] = entry
    return view


def tool_groups_from_view(view: Mapping[str, Mapping[str, Any]]) -> dict[str, bool]:
    """The closed session groups as booleans, for the existing consumers.

    The cockpit reads ``tool_groups`` and nothing else today
    (``api.service.ts::getSessionToolGroups``).  Deriving it FROM the composed
    view rather than alongside it is what stops the endpoint growing a second
    answer to its own question.
    """
    from shared.runtime.core.session_tool_overrides import SESSION_TOOL_OVERRIDE_NAMES

    return {
        group: (view.get(group) or {}).get("state") == STATE_ON
        for group in SESSION_TOOL_OVERRIDE_NAMES
    }


__all__ = [
    "CONFIG_LAYERS",
    "DECIDED_BY_BACKEND",
    "DECIDED_BY_GRANT",
    "DECIDED_BY_REGISTRY",
    "DECIDED_BY_RUNTIME",
    "DECIDED_BY_UNSET",
    "GRANT_GATED_CATEGORIES",
    "MEASURED_ORIGINS",
    "ORIGIN_AGENT",
    "ORIGIN_AGENT_PARTIAL",
    "ORIGIN_PREDICTION",
    "REPORT_VERSION",
    "STATE_OFF",
    "STATE_ON",
    "STATE_UNAVAILABLE",
    "UNCLASSIFIED_CATEGORY",
    "ToolReportError",
    "backend_blocked_categories",
    "backend_capabilities",
    "build_agent_toolset_report",
    "categorize_tool_names",
    "code_granted_categories",
    "code_granted_tools",
    "compose_tool_view",
    "grant_blocked_categories",
    "layer_provenance",
    "read_agent_toolset_report",
    "report_categories",
    "tool_groups_from_view",
]
