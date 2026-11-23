# src/core/capability_grants.py
"""Pure capability-grants logic: catalog, resolution, policy decision. No DB/
framework imports — hermetically unit-testable (the security boundary). Async DB
glue is in orchestrator/services/grants_service.py.

Spec: knowledge-history/done/global_expert_management.md (decisions 8, 9, 19, 21-23).
Restrict-only (decision 22): a more-specific scope may only narrow an inherited
value. Defaults: see the note above ``CATALOG`` (what the user's own agent needs
is granted; what reaches other users, acts unasked outside the platform or runs
unattended is not)."""

from __future__ import annotations

import copy
from typing import Any

from shared.connectors.builtin import tool_categories
from shared.permission_modes import PERMISSION_MODES, normalize_permission_mode

_AUTONOMY_ORDER = ["dependent", "guided", "partial", "review", "full"]
_PERMISSION_ORDER = list(PERMISSION_MODES)
#: Every tool category the `datasource_tools` grant gates, shared by `evaluate`
#: (the check) and `strip_to_grants` (the fix) so the two cannot drift apart.
#: The connector drivers' specs say which categories they bind.
_DATASOURCE_TOOL_CATEGORIES = tool_categories()

#: Every key's ``description`` is shown under it on Admin → Capability Grants
#: (the grants API returns this dict as JSON), so it is written for an
#: administrator deciding whether to allow it, not for a developer.
#:
#: Defaults (2026-11-11, owner decision): a key that only lets the user's own
#: agent get work done in its own workspace is granted by default: shell,
#: delegation, VM workspaces, catalog authoring, autonomous Sessions and
#: full-autonomy Jobs.
#: Keys whose effect reaches other users (public_datasources), leaves the
#: platform unasked (email_autonomous_send), skips a review check
#: (complete_unmerged_pr) or starts Jobs with nobody watching
#: (unattended_operations) stay deny-by-default, as does running a custom
#: harness image (custom_harness_images), which only builders of one need.
#: An administrator restores a stricter posture with a global grant: every
#: key is restrict-only, so a global cap binds every project and user.
CATALOG: dict[str, dict[str, Any]] = {
    # Lets a user fork/set a personal worker/session default.  Clearing an
    # existing pointer remains allowed even when this grant is later revoked;
    # runtime resolution simply treats the dormant pointer as absent.
    "personal_default_experts": {
        "type": "bool",
        "default": True,
        "restrict_only": True,
        "description": "Choose their own default Expert for new Jobs and Sessions.",
    },
    # Granted by default (2026-11-11). A VM is the agent's own machine; its
    # cost is capacity, which the operator controls separately: the global
    # `vm_workspaces` system setting blocks everyone, admins included
    # (vm_workspace_policy.check_vm_permission), and an install without a VM
    # provisioner fails a VM Job at dispatch with a message saying so, as it
    # already did for admins.
    "vm_workspace": {
        "type": "bool",
        "default": True,
        "restrict_only": True,
        "description": (
            "Run Jobs and Sessions in a full virtual machine. Needs VM support "
            "on this installation."
        ),
    },
    # Granted by default (2026-11-11). Agents run in an isolated workspace,
    # and a worker that cannot run a command cannot even render a PDF; the
    # default General Worker carries the shell tools on that basis. An
    # administrator who wants the old posture sets a global `shell_tools:
    # false` grant (restrict-only, so it caps every project and user below it).
    "shell_tools": {
        "type": "bool",
        "default": True,
        "restrict_only": True,
        "description": (
            "Use Experts that run commands in their workspace, for example to "
            "convert a document to PDF, run a script or install a package."
        ),
    },
    # Granted by default (2026-11-11). A delegated child runs in-process on
    # the parent's own workspace with tools capped by the parent's, so the
    # only thing it adds is token spend. Until then every shipped expert that
    # delegates (Engineer, Scholar, Developer, Critic, …) was refused for a
    # non-admin and replaced by the General Worker.
    "delegation": {
        "type": "bool",
        "default": True,
        "restrict_only": True,
        "description": (
            "Use Experts that hand parts of a task to subagents. Subagents work "
            "in the same workspace and add to the token cost."
        ),
    },
    # Publish datasources org-wide (is_global). Deny-by-default: publishing
    # hands the publisher's stored credentials to every user's agents.
    # Spec: knowledge-base/knowledge/features/public_datasources.md
    "public_datasources": {
        "type": "bool",
        "default": False,
        "restrict_only": True,
        "description": (
            "Publish a datasource to every user. Their agents then connect with "
            "the publisher's stored credentials."
        ),
    },
    # Unattended email send: allows setting unattended_send=true on an email
    # datasource, which skips the human send-approval freeze on email_send.
    # Deny-by-default and re-checked fail-closed at dispatch (a revoked grant
    # forces the flag back off). Spec: knowledge-base/knowledge/features/email_datasource.md
    "email_autonomous_send": {
        "type": "bool",
        "default": False,
        "restrict_only": True,
        "description": (
            "Let an email datasource send mail without a person approving each "
            "message first."
        ),
    },
    # Agent-authored catalogue entries: lets a session's agent create or update
    # experts, skills and automations on the user's behalf (tools.catalog_authoring).
    # Granted by default since 2026-11-11. Each write calls the endpoint the
    # Cockpit calls, as the session's owner, so the agent can do only what the
    # user could do by hand: their own entries, never a bundled or global
    # expert. The tools are still not injected by default; an expert must name
    # them. An automation the agent writes starts disabled unless it asks for
    # allow_enabled, and a user may already create an enabled one in the UI.
    # Spec: knowledge-base/knowledge/features/agent_authored_catalog_entries.md
    "catalog_authoring": {
        "type": "bool",
        "default": True,
        "restrict_only": True,
        "description": (
            "Let the agent create and edit the user's own Experts, Skills and "
            "Automations. Bundled and global entries stay read-only."
        ),
    },
    # Sealing a job whose delivered pull request is not merged. A job whose
    # deliverable is a PR is done when that PR lands, not when the branch is
    # pushed; without this gate a reviewer marks work complete on the strength
    # of a green job screen. Permission polarity — the grant LIFTS the block, so
    # default False means "the gate applies". Read directly at the enforcement
    # points via user_can_complete_unmerged_pr, never through evaluate(): there
    # is no config fragment involved, only live forge state. Deny-by-default and
    # NOT backfilled — the gate can only fire on a job carrying a recorded
    # context.pull_request, and none existed when the key was introduced.
    # Spec: knowledge-base/knowledge/features/merged_pr_completion_grant.md
    "complete_unmerged_pr": {
        "type": "bool",
        "default": False,
        "restrict_only": True,
        "description": (
            "Mark a Job done while its pull request is still open. Without it, "
            "such a Job is done only once the pull request is merged."
        ),
    },
    # The project self-improvement loop and the commissioned officer: the two
    # surfaces that spawn jobs with no human in the loop. Deny-by-default and
    # NOT backfilled — nobody held it before, and an unattended loop or officer
    # is an unbounded token spend, which is the tier control a grant exists to
    # be. ONE key rather than one each: officer scheduling is already a mode of
    # the loop engine (loop_unified_engine.md, "Third mode added 2026-07-30")
    # and the two surfaces are converging, so a split would have to be re-merged.
    #
    # Enforced on two paths, deliberately, because the two surfaces are reached
    # differently:
    #   - the officer half rides `evaluate` below, on `officer.enabled`. That
    #     covers the commission endpoint AND a hand-rolled thread create with
    #     the flag in `config_override` — and because sessions re-resolve grants
    #     on every attach, revoking it stands a RUNNING officer down rather than
    #     only blocking new ones.
    #   - the loop half has no config fragment to evaluate, so it is read
    #     directly at the endpoints and at the spawn choke point, via
    #     `user_can_run_unattended_operations` (the same shape as
    #     `complete_unmerged_pr` above).
    # Spec: knowledge-history/done/unattended_operations_grant.md.
    "unattended_operations": {
        "type": "bool",
        "default": False,
        "restrict_only": True,
        "description": (
            "Run project loops and commission officers. Both start Jobs on their "
            "own with nobody watching, so their cost has no upper limit."
        ),
    },
    # Run an admin-approved custom harness image (manifestHosting.approvedImages)
    # as a generic manifest Job. Deny-by-default and NOT backfilled: nobody held
    # it before. Checked at generic Job admission via
    # `user_can_run_custom_harness_images`.
    # Spec: knowledge-base/knowledge/superpowers/specs/2026-10-26-custom-harness-claude-code-design.md
    "custom_harness_images": {
        "type": "bool",
        "default": False,
        "restrict_only": True,
        "description": (
            "Run an administrator-approved custom harness image as a Job, "
            "instead of the built-in agent."
        ),
    },
    "datasource_tools": {
        "type": "bool",
        "default": True,
        "restrict_only": True,
        "description": (
            "Use the tools of attached connectors, such as databases, email and "
            "cloud storage."
        ),
    },
    "browser": {
        "type": "bool",
        "default": True,
        "restrict_only": True,
        "description": "Use Experts that drive a real web browser.",
    },
    "model_selection": {
        "type": "list",
        "default": None,
        "restrict_only": True,
        "description": (
            "Limit which models the user may pick. Empty means every enabled model."
        ),
    },
    # Default ceiling is full since 2026-11-11 (review before that). The
    # ceiling is only what a user MAY pick: a Job still defaults to `review`
    # (config/overlays/worker.yaml) unless the user or the expert chooses
    # otherwise. Known gap, owner's call: under `full` a Job the agent itself
    # scored confidence 0 still ends `completed`
    # (knowledge-base/knowledge/issues/full_autonomy_completes_a_zero_confidence_job.md).
    "autonomy_ceiling": {
        "type": "enum",
        "default": "full",
        "restrict_only": True,
        "order": _AUTONOMY_ORDER,
        "description": (
            "The most autonomy a user may give a Job. 'review' stops for a person "
            "before the Job counts as done; 'full' never stops."
        ),
    },
    "permission_mode": {
        "type": "enum",
        # Default ceiling is autonomous since 2026-11-11 (auto_accept from
        # 2026-06-26, supervised — now read_only — before that). The ceiling
        # is only what a user MAY pick: a Session still starts in read_only
        # (DEFAULT_PERMISSION_MODE) unless the user or the expert chooses
        # otherwise. autonomous runs shell and external effects without
        # asking, inside the agent's own workspace; sending mail unattended
        # stays behind `email_autonomous_send`. A global-scope grant can
        # restrict back to auto_accept or read_only. Stored rows may still say
        # `supervised`; _VALUE_NORMALIZERS maps them before any comparison.
        # knowledge-base/knowledge/issues/session_permission_mode_grant_denied_ready_timeout.md (Phase 5).
        "default": "autonomous",
        "restrict_only": True,
        "order": _PERMISSION_ORDER,
        "description": (
            "The most a user may let a Session do without asking. 'autonomous' "
            "runs commands and outward actions such as a git push without asking."
        ),
    },
}

#: Stored values that predate a rename, mapped before any comparison. Kept out
#: of CATALOG, which the grants API returns as JSON.
_VALUE_NORMALIZERS = {"permission_mode": normalize_permission_mode}


def meet(spec: dict, a: Any, b: Any) -> Any:
    """Greatest-lower-bound (the restrict-only combinator). bool->AND,
    enum->more restrictive by catalog order, list->intersection (None = ⊤)."""
    t = spec["type"]
    if t == "bool":
        return bool(a) and bool(b)
    if t == "enum":
        order = spec["order"]
        return a if order.index(a) <= order.index(b) else b
    if t == "list":
        if a is None:
            return b
        if b is None:
            return a
        bset = set(b)
        return [x for x in a if x in bset]
    raise ValueError(f"unknown catalog type {t!r}")


def _scope_value(rows: list[dict], key: str, spec: dict) -> Any:
    """The value one scope asserts for key, meeting duplicates (multi-project ->
    most restrictive). None => scope does not set the key."""
    normalize = _VALUE_NORMALIZERS.get(key, lambda value: value)
    vals = [normalize(r["value_json"]) for r in rows if r.get("key") == key]
    if not vals:
        return None
    acc = vals[0]
    for v in vals[1:]:
        acc = meet(spec, acc, v)
    return acc


def resolve_grants(
    *, user_rows: list[dict], project_rows: list[dict], global_rows: list[dict]
) -> dict[str, Any]:
    """Resolve every catalog key for one principal. granted = most-specific scope
    that sets it (user>project>global) else catalog default; restrict-only keys
    are clamped to the meet of every scope that set the key (decision 22 — a child
    can never widen past a parent cap)."""
    out: dict[str, Any] = {}
    for key, spec in CATALOG.items():
        u = _scope_value(user_rows, key, spec)
        p = _scope_value(project_rows, key, spec)
        gl = _scope_value(global_rows, key, spec)
        set_pairs = [(s, v) for s, v in ((2, u), (1, p), (0, gl)) if v is not None]
        if not set_pairs:
            out[key] = spec["default"]
            continue
        granted = max(set_pairs, key=lambda t: t[0])[1]
        if spec["restrict_only"]:
            eff = granted
            for _s, v in set_pairs:
                eff = meet(spec, eff, v)
            out[key] = eff
        else:
            out[key] = granted
    return out


def _truthy(x: Any) -> bool:
    return x not in (None, False, 0, "", [], {})


#: The roster ``llm.model`` value meaning "the parent's model" — never a
#: selection of its own, so the model gate skips it (the parent's pin is what
#: gets checked). Mirrors ``src.core.loader.INHERIT_MODEL`` without importing
#: the loader (this module stays framework-free).
_INHERIT_MODEL = "inherit"


def _roster_entries(fragment: dict) -> list[tuple[str, dict]]:
    """``(name, entry)`` for every mapping under ``subagents.roster`` — the
    raw authored entries at save time, the materialised subagent configs at
    dispatch (``resolve_config`` resolves the roster BEFORE the capture, so
    the PDP never sees ``{$ref: critic}``; U1 WP3). Malformed shapes yield
    nothing: the resolver reports them, the PDP only gates what would run."""
    subagents = fragment.get("subagents")
    if not isinstance(subagents, dict):
        return []
    roster = subagents.get("roster")
    if not isinstance(roster, dict):
        return []
    return [
        (str(name), entry) for name, entry in roster.items() if isinstance(entry, dict)
    ]


def _fragment_models(fragment: dict) -> list[str]:
    llm = fragment.get("llm") or {}
    out = []
    for v in (
        llm.get("model"),
        (llm.get("strategic") or {}).get("model"),
        (llm.get("tactical") or {}).get("model"),
    ):
        if isinstance(v, str) and v:
            out.append(v)
    # The roster's model slots: the roster-wide ``subagents.llm.model`` and
    # every entry's own pin. ``inherit`` is the parent's (already listed)
    # model, not a selection.
    subagents = fragment.get("subagents")
    roster_llm = subagents.get("llm") if isinstance(subagents, dict) else None
    candidates = [roster_llm.get("model")] if isinstance(roster_llm, dict) else []
    for _name, entry in _roster_entries(fragment):
        entry_llm = entry.get("llm")
        if isinstance(entry_llm, dict):
            candidates.append(entry_llm.get("model"))
    for v in candidates:
        if isinstance(v, str) and v and v != _INHERIT_MODEL:
            out.append(v)
    return out


def _granted(grants: dict, key: str) -> Any:
    """The resolved value of ``key``, or its catalog default when ``grants``
    lacks it. ``resolve_grants`` always fills every key, so the default only
    applies to a partial dict (tests, hand-built callers); reading it from
    ``CATALOG`` keeps the PDP and the catalog from drifting apart."""
    return grants.get(key, CATALOG[key]["default"])


def _enum_exceeds(value: Any, ceiling: str, order: list[str]) -> bool:
    return (
        isinstance(value, str)
        and value in order
        and order.index(value) > order.index(ceiling)
    )


def evaluate(fragment: dict, grants: dict, *, is_admin: bool = False) -> list[str]:
    """Violation messages for a config vs a resolved grant set ([] = allowed). The
    single PDP: fed the raw expert fragment at save-time and the full merged config
    at dispatch. Admins short-circuit. Absent gated keys never violate."""
    if is_admin:
        return []
    v: list[str] = []
    tools = fragment.get("tools") or {}
    ws = fragment.get("workspace") or {}
    deleg = fragment.get("delegation") or {}
    inter = fragment.get("interactive") or {}
    officer = fragment.get("officer") or {}

    if not _granted(grants, "vm_workspace") and ws.get("backend") == "vm":
        v.append("vm_workspace: workspace.backend='vm' requires the vm_workspace grant")
    if not _granted(grants, "shell_tools") and _truthy(tools.get("shell")):
        v.append("shell_tools: tools.shell requires the shell_tools grant")
    # delegation gates on the .enabled flag (a settings dict) OR a non-empty tool list,
    # NOT mere presence of the settings dict.
    if not _granted(grants, "delegation") and (
        deleg.get("enabled") is True or _truthy(tools.get("delegation"))
    ):
        v.append("delegation: delegation requires the delegation grant")
    if not _granted(grants, "datasource_tools") and any(
        _truthy(tools.get(k)) for k in _DATASOURCE_TOOL_CATEGORIES
    ):
        v.append("datasource_tools: connector tools are not permitted")
    if not _granted(grants, "browser") and _truthy(tools.get("browser_direct")):
        v.append("browser: tools.browser_direct is not permitted")
    # Gates on `officer.enabled` only — the flag that makes a thread a live
    # officer. Every other officer key (slots, sleep bounds, pools, conference)
    # is inert configuration on a post nobody holds, so a user without the grant
    # may still read and edit the durable kit; he just cannot raise anyone onto
    # it. Mirrors main._officer_meta_enabled's truthy set rather than `_truthy`,
    # which would read the string "false" as enabled.
    if not _granted(grants, "unattended_operations") and officer.get("enabled") in (
        True,
        "true",
        "True",
        1,
    ):
        v.append(
            "unattended_operations: a commissioned officer requires the "
            "unattended_operations grant"
        )
    if not _granted(grants, "catalog_authoring") and _truthy(
        tools.get("catalog_authoring")
    ):
        v.append(
            "catalog_authoring: tools.catalog_authoring requires the "
            "catalog_authoring grant"
        )

    # Roster entries (U1 WP4): a child runs with the tools its entry names,
    # so the same tool gates apply per entry — a `$ref: critic` child must
    # not hand a shell to a runner without shell_tools. Only the tool and
    # model gates: `workspace.backend`, `autonomy`, `delegation` /
    # `tools.delegation`, `tools.core` and `officer` are pruned from every
    # entry by the subagent overlay (they never run), and
    # `interactive.permission_mode` is `autonomous` on every child by design
    # (a child never asks a human — the runner is that enforcement layer).
    # Messages keep the `<grant>: ` prefix `strip_to_grants` keys off.
    for name, entry in _roster_entries(fragment):
        etools = entry.get("tools")
        if not isinstance(etools, dict):
            continue
        at = f"subagents.roster.{name}"
        if not _granted(grants, "shell_tools") and _truthy(etools.get("shell")):
            v.append(f"shell_tools: {at}.tools.shell requires the shell_tools grant")
        if not _granted(grants, "datasource_tools") and any(
            _truthy(etools.get(k)) for k in _DATASOURCE_TOOL_CATEGORIES
        ):
            v.append(f"datasource_tools: {at} connector tools are not permitted")
        if not _granted(grants, "browser") and _truthy(etools.get("browser_direct")):
            v.append(f"browser: {at}.tools.browser_direct is not permitted")
        if not _granted(grants, "catalog_authoring") and _truthy(
            etools.get("catalog_authoring")
        ):
            v.append(
                f"catalog_authoring: {at}.tools.catalog_authoring requires the "
                "catalog_authoring grant"
            )

    allowed = grants.get("model_selection")  # None = all
    if allowed is not None:
        for m in _fragment_models(fragment):
            if m not in allowed:
                v.append(f"model_selection: model '{m}' is not in the permitted set")

    if _enum_exceeds(
        fragment.get("autonomy"),
        _granted(grants, "autonomy_ceiling"),
        _AUTONOMY_ORDER,
    ):
        v.append(
            f"autonomy_ceiling: autonomy '{fragment.get('autonomy')}' exceeds the ceiling"
        )
    if _enum_exceeds(
        normalize_permission_mode(inter.get("permission_mode")),
        normalize_permission_mode(_granted(grants, "permission_mode")),
        _PERMISSION_ORDER,
    ):
        v.append(
            f"permission_mode: '{inter.get('permission_mode')}' exceeds the ceiling"
        )
    return v


def _drop_key(container: Any, key: str) -> None:
    """Delete `key` from `container` if it is a dict and holds it. No-op
    otherwise (absent parent, or a parent that isn't a dict) — the caller
    never has to guard the shape first."""
    if isinstance(container, dict):
        container.pop(key, None)


def strip_to_grants(fragment: dict, grants: dict) -> tuple[dict, list[str]]:
    """Return (fragment minus what `grants` forbids, the grant keys dropped).

    For `duplicate_expert` only (2026-08-04 decision,
    knowledge-base/knowledge/superpowers/plans/2026-08-04-expert-write-gate-holes.md, task 3): a
    source config exceeding the copier's grants is stripped to what they can
    have rather than refused outright, because the route exists to fork a
    config the copier likely intends to edit anyway ("start from scholar") —
    refusing blocked step one of that exact workflow for most default-grants
    users. The other four expert-write routes are untouched and still refuse
    via `evaluate` + a 422.

    One-for-one with `evaluate`'s ten rules, and kept beside it (rather than
    a second, route-side implementation) so the two cannot drift — `evaluate`
    is called here, on the ORIGINAL fragment, to find what to strip.

    Every rule deletes the offending key outright, never zeroes or replaces
    it: absent means "inherit the base", and the base is the conservative
    floor. Two exceptions written into the shape rather than the value:
    `delegation` removes `tools.delegation` and only the `.enabled` flag
    inside `delegation`, never the whole `delegation` settings dict —
    `evaluate` gates on `.enabled is True` or a non-empty `tools.delegation`,
    not on the settings dict merely existing, so deleting the dict would also
    drop `max_concurrent`/`run_in_background_default`, which were never the
    violation. For
    the same reason `unattended_operations` drops only `officer.enabled` and
    keeps the rest of the kit (slots, sleep bounds, pools).

    Never deletes an emptied PARENT dict (`tools: {}` is left as `{}`, not
    removed, if that is all that remains of it) and never mutates `fragment`
    in place — `duplicate_expert` reuses the source bundle's dict.

    THIS IS ADVISORY, NOT AUTHORITATIVE. The caller MUST re-run `evaluate` on
    the result and refuse (422) if any violation survives — see
    `orchestrator.main._strip_save_grants`. That re-check is the safety
    property: an entry here that is missing or wrong can only ever produce an
    unnecessary drop or a false refusal, never a permitted escape.
    """
    out = copy.deepcopy(fragment)
    flagged = {v.split(":", 1)[0] for v in evaluate(fragment, grants)}
    dropped: list[str] = []
    # The live entry dicts inside `out` — every tool-category branch below
    # drops from the top-level `tools` AND from each roster entry's `tools`,
    # one-for-one with the per-entry gates in `evaluate`.
    roster_tools = [
        entry.get("tools")
        for _name, entry in _roster_entries(out)
        if isinstance(entry.get("tools"), dict)
    ]

    def _drop_tool_category(key: str) -> None:
        _drop_key(out.get("tools"), key)
        for etools in roster_tools:
            _drop_key(etools, key)

    if "shell_tools" in flagged:
        _drop_tool_category("shell")
        dropped.append("shell_tools")
    if "delegation" in flagged:
        _drop_key(out.get("tools"), "delegation")
        _drop_key(out.get("delegation"), "enabled")
        dropped.append("delegation")
    if "datasource_tools" in flagged:
        for category in _DATASOURCE_TOOL_CATEGORIES:
            _drop_tool_category(category)
        dropped.append("datasource_tools")
    if "browser" in flagged:
        _drop_tool_category("browser_direct")
        dropped.append("browser")
    if "catalog_authoring" in flagged:
        _drop_tool_category("catalog_authoring")
        dropped.append("catalog_authoring")
    if "unattended_operations" in flagged:
        _drop_key(out.get("officer"), "enabled")
        dropped.append("unattended_operations")
    if "vm_workspace" in flagged:
        _drop_key(out.get("workspace"), "backend")
        dropped.append("vm_workspace")
    if "model_selection" in flagged:
        allowed = grants.get("model_selection")

        def _drop_model_pin(block: Any) -> None:
            if (
                isinstance(block, dict)
                and isinstance(block.get("model"), str)
                and block["model"] != _INHERIT_MODEL
                and block["model"] not in allowed
            ):
                del block["model"]

        llm = out.get("llm")
        if isinstance(llm, dict) and allowed is not None:
            _drop_model_pin(llm)
            for tier in ("strategic", "tactical"):
                _drop_model_pin(llm.get(tier))
        subagents = out.get("subagents")
        if isinstance(subagents, dict) and allowed is not None:
            # The roster's own slots: the roster-wide pin and each entry's.
            _drop_model_pin(subagents.get("llm"))
            for _name, entry in _roster_entries(out):
                _drop_model_pin(entry.get("llm"))
        dropped.append("model_selection")
    if "autonomy_ceiling" in flagged:
        _drop_key(out, "autonomy")
        dropped.append("autonomy_ceiling")
    if "permission_mode" in flagged:
        _drop_key(out.get("interactive"), "permission_mode")
        dropped.append("permission_mode")

    return out, dropped
