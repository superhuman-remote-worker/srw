"""Knowledge base tools for the Universal Agent.

Provides tools for interacting with native and datasource-backed OKF knowledge bases:
- Writing: kb_write, kb_update (write-through to Neo4j + pgvector)
- Reading: kb_read, kb_list, kb_search
- Graph: kb_related, kb_contradictions, kb_provenance, kb_unanswered
- Export: kb_export

These tools use the system Neo4j connection (not the external datasource
connector). Connection comes from ToolContext.knowledge_graph.

See knowledge-base/knowledge/features/project_knowledge_base.md for full architecture.
"""

import asyncio
import hashlib
import logging
import os
import re
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Literal, NamedTuple, Optional, Sequence, Union

import httpx
from langchain_core.tools import tool

from shared.backlog_tags import (
    READY_TAG,
    has_tag,
    is_officer_only_tag,
    normalize_tags,
)
from shared.runtime_actor import (
    RUNTIME_ACTOR_HEADER,
    RUNTIME_ACTOR_REFRESH_HEADER,
    RuntimeActorContext,
    RuntimeAuthorizationResult,
)

from shared.runtime.services.knowledge_graph import (
    CONFIDENCE_LEVELS,
    DEFAULT_PRIORITY_RANK,
    NOTE_STATUSES,
    NOTE_TYPES,
    PRIORITY_RANKS,
    PRIORITY_WORDS,
    slugify,
)
from agent.core.redaction_write_guard import (
    adds_redaction_marker,
    redaction_marker_refusal,
)
from agent.services.knowledge.bindings import KnowledgeBinding, split_note_handle
from agent.tools.context import ToolContext
from shared.runtime.knowledge.chunker import embedding_version_for_service
from shared.runtime.knowledge.gardener import (
    Finding,
    dead_url_findings,
    external_url_map,
    is_reserved,
    near_duplicate_findings,
    lint_kb,
    note_title,
    parse_note_md,
    render_index_md,
)

from shared.tool_catalog.definitions import (
    KNOWLEDGE_TOOLS_METADATA as KNOWLEDGE_TOOLS_METADATA,
)

logger = logging.getLogger(__name__)

# Closed vocabularies for the kb tool schemas, mirroring the frozensets in
# services/knowledge_graph.py.
#
# Must be Literal, not str: these values only ever reached the model through
# docstring prose, and prose drifts — the kb_write docstring listed nine of the
# ten NOTE_TYPES for long enough that nobody noticed 'datasource' was missing.
# A Literal puts the vocabulary in the args_schema, which is serialized on every
# call anyway and cannot silently disagree with the enforcement in kb_write.
# See knowledge-base/knowledge/issues/agent_tool_fixed_vocabularies_invisible_to_model.md.
#
# Spelled literally (not derived from the frozensets) so they stay valid static
# annotations; tests/test_tool_vocabularies.py asserts they stay in sync.
NoteTypeValue = Literal[
    "goal",
    "plan",
    "decision",
    "learning",
    "code",
    "source",
    "question",
    "state",
    "retrospective",
    "datasource",
    "feature",
    "issue",
    "idea",
    "charter",
    "report",
]
NoteStatusValue = Literal["active", "resolved", "superseded", "archived"]
NoteConfidenceValue = Literal["high", "medium", "low"]
PriorityValue = Literal["high", "normal", "low"]

# Note types eligible for a backlog priority (Task 3, project-backlog-pipeline).
# Priority is scoped to tickets: this gates whether kb_write/kb_update surface
# it in the OKF frontmatter and kb_list — never whether it reaches the
# pgvector row, which always carries the rank the caller asked for (or
# DEFAULT_PRIORITY_RANK), so a note's priority survives a later re-index.
_TICKET_TYPES = ("feature", "issue", "idea")

# kb_gardening G5 — the retirement guard. A note that one of these ACTIVE
# note types links to is that note's evidence and must not be retired by an
# agent; the guard is enforced here, in the tool, not in any prompt.
_RETIRE_ROOT_TYPES = (
    "decision",
    "goal",
    "plan",
    "charter",
    "feature",
    "issue",
    "idea",
    "code",
    "question",
)
#: Tags that mark a note as protected from agent retirement.
_RETIRE_PROTECTED_TAGS = frozenset({"pinned", "ready", "parallel-safe"})
#: Notes younger than this are never retired by an agent (a curator racing
#: the main agent's freshly written note is the case this closes).
_RETIRE_MIN_AGE = timedelta(hours=24)


def _retire_denied(
    existing: Dict[str, Any], inbound_durable: List[Dict[str, Any]]
) -> Optional[str]:
    """Why ``existing`` may not be retired by an agent, or None if it may.

    Pure function over the row (``get_note_by_slug`` shape) and the active
    durable notes that link to it (``get_inbound_links``). Every refusal names
    its rule so the agent can pick the right alternative (supersede, close a
    ticket, ask the officer).
    """
    note_type = str(existing.get("type") or "")
    if note_type == "charter":
        return "the charter is never retired"
    if note_type in _TICKET_TYPES:
        return (
            "it is a backlog ticket — close it with "
            "kb_update(status='resolved' or 'archived') so the pipeline records the outcome"
        )
    if note_type == "report":
        return "officer reports are retired by the officer, not by workers"
    tags = set(normalize_tags(existing.get("tags") or []))
    protected = sorted(tags & _RETIRE_PROTECTED_TAGS)
    if protected:
        return f"it is tagged {', '.join(protected)}"
    if existing.get("ready_at"):
        return "it is authorised for dispatch (ready_at is set)"
    if inbound_durable:
        names = ", ".join(
            f"{link.get('id')} ({link.get('type')})" for link in inbound_durable[:5]
        )
        more = (
            "" if len(inbound_durable) <= 5 else f" and {len(inbound_durable) - 5} more"
        )
        return f"active durable notes link to it as evidence: {names}{more}"
    created = existing.get("created")
    if isinstance(created, datetime):
        if created.tzinfo is None:
            created = created.replace(tzinfo=timezone.utc)
        if datetime.now(timezone.utc) - created < _RETIRE_MIN_AGE:
            return "it is younger than 24 hours"
    return None


# Shown by the genuinely graph-shaped tools when Neo4j is absent (slice-3 PR4c).
# CONTRADICTS / DERIVED_FROM / ANSWERS edges and the Neo4j export have no
# files-canonical representation — degrade honestly instead of faking results
# from the generic body-link table (which only carries "references" edges).
_GRAPH_TIER_MSG = (
    "requires the Graph tier (Neo4j), which is not enabled for this knowledge "
    "base. Notes remain searchable (kb_search) and 1-hop links are available "
    "via kb_related."
)


def _content_hash(text: str) -> str:
    """Stable content fingerprint for exact-duplicate detection."""
    return hashlib.sha256((text or "").encode()).hexdigest()


def _marker_refusal(
    existing: Dict[str, Any],
    content: Optional[str],
    append: Optional[str],
    note: str,
) -> Optional[str]:
    """kb_update's refusal to commit a redaction marker the note lacks.

    A note read back through a tool result is credential-redacted; rewriting
    it from that view would commit ``[REDACTED]`` over the real value
    (agent.core.redaction_write_guard). Compared against the note's current
    content, so a note that already quotes the marker stays editable.
    """
    prior = existing.get("content") or ""
    if content is not None:
        prospective = content
    elif append is not None:
        prospective = prior + "\n\n" + append
    else:
        return None
    if adds_redaction_marker(prior, prospective):
        return redaction_marker_refusal(f"note '{note}'", "kb_update")
    return None


def _runtime_actor_for_project(
    context: "ToolContext", project_id: str
) -> RuntimeActorContext | None:
    """Return only an actor attached to the exact writable native binding."""

    bindings = [
        binding
        for binding in (getattr(context, "knowledge_bindings", None) or [])
        if isinstance(binding, KnowledgeBinding)
    ]
    for binding in bindings:
        if (
            binding.is_native
            and binding.writable
            and str(binding.kb_id) == str(project_id)
        ):
            actor = binding.runtime_actor
            return actor if isinstance(actor, RuntimeActorContext) else None
    if bindings:
        return None
    # Legacy/test contexts predating explicit bindings retain a fail-closed
    # exact-project fallback. Production attach/dispatch paths always bind the
    # actor above to their sole writable native scope.
    actor = getattr(context, "runtime_actor", None)
    if isinstance(actor, RuntimeActorContext) and actor.project_id == str(project_id):
        return actor
    return None


def _authorization_denial(
    *,
    code: str,
    action: str,
    actor: RuntimeActorContext | None,
    message: str,
) -> RuntimeAuthorizationResult:
    return RuntimeAuthorizationResult(
        authorized=False,
        code=code,
        action=action,
        actor=(actor.audit_payload() if actor else {"caller_kind": "unresolved"}),
        message=message,
    )


def _request_runtime_actor_authorization(
    context: "ToolContext", project_id: str, action: str
) -> RuntimeAuthorizationResult:
    """Ask the orchestrator PEP before any sensitive knowledge mutation."""

    actor = _runtime_actor_for_project(context, project_id)
    base_url = os.getenv("ORCHESTRATOR_URL", "http://localhost:8085").rstrip("/")
    base_headers: Dict[str, str] = {}
    internal_key = os.getenv("MCP_INTERNAL_KEY", "")
    if internal_key:
        base_headers["X-Internal-Key"] = internal_key

    try:
        with httpx.Client(timeout=10.0, headers=base_headers) as client:
            if (
                actor is not None
                and actor.access_needs_refresh()
                and actor.refresh_credential
            ):
                refreshed = client.post(
                    f"{base_url}/api/runtime-actors/refresh",
                    headers={RUNTIME_ACTOR_REFRESH_HEADER: actor.refresh_credential},
                )
                if refreshed.status_code != 200:
                    return _authorization_result_from_response(
                        refreshed, action=action, fallback_actor=actor
                    )
                try:
                    refreshed_payload = refreshed.json().get("runtime_actor")
                except Exception:
                    refreshed_payload = None
                if not actor.apply_refreshed_payload(refreshed_payload):
                    return _authorization_denial(
                        code="malformed_refresh",
                        action=action,
                        actor=actor,
                        message="Runtime actor refresh response was malformed.",
                    )

            headers: Dict[str, str] = {}
            if actor is not None and actor.access_credential:
                headers[RUNTIME_ACTOR_HEADER] = actor.access_credential
            response = client.post(
                f"{base_url}/api/runtime-actors/authorize",
                headers=headers,
                json={"action": action, "project_id": project_id},
            )
    except Exception as exc:  # noqa: BLE001 - fail closed before all writes
        return _authorization_denial(
            code="authorization_unavailable",
            action=action,
            actor=actor,
            message=f"Authorization service is unavailable ({type(exc).__name__}).",
        )
    return _authorization_result_from_response(
        response, action=action, fallback_actor=actor
    )


def _authorization_result_from_response(
    response: httpx.Response,
    *,
    action: str,
    fallback_actor: RuntimeActorContext | None,
) -> RuntimeAuthorizationResult:
    try:
        payload = response.json()
    except Exception:
        payload = {}
    detail = payload.get("detail") if isinstance(payload, dict) else None
    source = detail if isinstance(detail, dict) else payload
    if response.status_code == 200 and isinstance(source, dict):
        return RuntimeAuthorizationResult(
            authorized=source.get("authorized") is True,
            code=str(source.get("code") or "authorized"),
            action=str(source.get("action") or action),
            actor=source.get("actor")
            if isinstance(source.get("actor"), dict)
            else (
                fallback_actor.audit_payload()
                if fallback_actor
                else {"caller_kind": "unresolved"}
            ),
            message=str(source.get("message") or "Runtime actor is authorized."),
        )
    return _authorization_denial(
        code=str(source.get("code") or f"http_{response.status_code}")
        if isinstance(source, dict)
        else f"http_{response.status_code}",
        action=action,
        actor=fallback_actor,
        message=str(source.get("message") or "Runtime actor was denied.")
        if isinstance(source, dict)
        else "Runtime actor was denied.",
    )


def _has_officer_authority(
    context: "ToolContext", project_id: str, action: str = "machine_tags"
) -> RuntimeAuthorizationResult:
    """Authorize dispatch/charter authority from the server-derived actor."""

    return _request_runtime_actor_authorization(context, project_id, action)


def _charter_write_denied(context: "ToolContext", project_id: str) -> Optional[str]:
    """Return an explicit, audited denial for a charter mutation."""

    result = _has_officer_authority(context, project_id, "charter")
    return None if result else result.tool_message()


def _machine_tag_mutation_requested(
    existing: Optional[List[str]],
    *,
    add: Optional[List[str]] = None,
    remove: Optional[List[str]] = None,
    replace: Optional[List[str]] = None,
) -> bool:
    """Whether the request would grant or withdraw an officer-only tag."""

    if replace is not None:
        current = {tag for tag in normalize_tags(existing) if is_officer_only_tag(tag)}
        requested = {tag for tag in normalize_tags(replace) if is_officer_only_tag(tag)}
        return current != requested
    return any(
        is_officer_only_tag(tag)
        for tag in normalize_tags([*(add or []), *(remove or [])])
    )


class _TagResolution(NamedTuple):
    """The outcome of applying a tag mutation to a note.

    ``ready`` is the tri-state flag ``KnowledgeStore.upsert_note`` takes:
    True/False when this write explicitly granted or withdrew dispatch
    authorization, None when it said nothing about it.
    """

    tags: List[str]
    ready: Optional[bool]
    dropped: List[str]
    changed: bool


def _resolve_tags(
    existing: Optional[List[str]],
    *,
    add: Optional[List[str]] = None,
    remove: Optional[List[str]] = None,
    replace: Optional[List[str]] = None,
    officer_authority: bool,
) -> _TagResolution:
    """Apply a tag mutation, enforcing the officer-only namespace.

    ``replace`` (the ``set_tags`` argument) is absolute and wins over
    ``add``/``remove``; otherwise the result is ``existing + add - remove``.
    Removal happens before addition so a caller that swaps a value in one call
    — drop ``category:researcher``, add ``category:executor`` — cannot have the
    removal cancel its own addition when the lists overlap.

    Without officer authority the caller's officer-only tags are dropped AND
    the note's existing ones are carried over untouched. Stripping the input
    alone would not be enough: ``set_tags`` is absolute, so a worker could
    un-ready a queued ticket simply by rewriting the list without it. The
    invariant is that a worker write can neither grant nor withdraw dispatch
    authorization, which is also why ``ready`` comes back None on that path.
    """
    current = normalize_tags(existing)

    if replace is not None:
        requested = normalize_tags(replace)
        if officer_authority:
            return _TagResolution(
                tags=requested,
                ready=has_tag(requested, READY_TAG),
                dropped=[],
                changed=requested != current,
            )
        base = [t for t in requested if not is_officer_only_tag(t)]
        carried = [t for t in current if is_officer_only_tag(t)]
        resolved = base + [t for t in carried if t not in base]
        return _TagResolution(
            tags=resolved,
            ready=None,
            dropped=[t for t in requested if is_officer_only_tag(t)],
            changed=resolved != current,
        )

    add_list = normalize_tags(add)
    remove_list = normalize_tags(remove)
    dropped: List[str] = []
    if not officer_authority:
        dropped = [t for t in add_list + remove_list if is_officer_only_tag(t)]
        add_list = [t for t in add_list if not is_officer_only_tag(t)]
        remove_list = [t for t in remove_list if not is_officer_only_tag(t)]

    resolved = [t for t in current if t not in remove_list]
    for tag in add_list:
        if tag not in resolved:
            resolved.append(tag)

    if not officer_authority:
        ready: Optional[bool] = None
    elif has_tag(add_list, READY_TAG):
        ready = True
    elif has_tag(remove_list, READY_TAG):
        ready = False
    else:
        # Silence, not a re-assertion. A content edit on a ready ticket carries
        # `ready` in its tag list but must NOT bump ready_at: that would re-arm
        # a ticket the tick has already claimed and dispatch a second job for
        # work in flight.
        ready = None

    return _TagResolution(
        tags=resolved, ready=ready, dropped=dropped, changed=resolved != current
    )


def _dropped_tag_notice(dropped: List[str]) -> str:
    """Tell the caller what was refused, rather than silently discarding it.

    A worker that asked for ``ready`` and got no acknowledgement would keep
    asking; naming the boundary costs one clause and teaches the model the
    actual model of the system.
    """
    if not dropped:
        return ""
    names = ", ".join(sorted(set(dropped)))
    return (
        f" (ignored {names}: dispatch-authorization tags are set by the "
        f"officer, not from a worker job)"
    )


def _apply_ready_frontmatter(
    note: Dict[str, Any],
    ready: Optional[bool],
    existing: Optional[Dict[str, Any]] = None,
) -> None:
    """Carry ``ready_at`` into the OKF file this write is about to materialise.

    The file is canonical, so the authorization has to live there too — a
    rebuild from files that dropped it would silently park every ready ticket
    in the vault. Present-and-set means armed, absent means "this file says
    nothing", which is what ``upsert_kb_note`` COALESCEs against the stored
    value; a rebuilt row with neither fails closed, which is the right
    direction for a dispatch authorization.

    The timestamp comes from this process while the index row gets ``NOW()``
    from Postgres. They differ by under a millisecond, and the only comparison
    that reads them is against a job's ``created_at`` a tick or more later.
    """
    if ready is True:
        note["ready_at"] = datetime.now(timezone.utc).isoformat()
    elif ready is False:
        note.pop("ready_at", None)
    elif existing and existing.get("ready_at"):
        prior = existing["ready_at"]
        note["ready_at"] = (
            prior.isoformat() if isinstance(prior, datetime) else str(prior)
        )


def _carry_timestamps(note: Dict[str, Any], existing: Dict[str, Any]) -> None:
    """Keep the note's birth date and stamp this write's time, in the file.

    ``kb_write`` puts ``created:``/``modified:`` into the frontmatter because
    the row is rebuilt from the file, and ``created_at`` is deliberately
    absent from ``upsert_kb_note``'s ``ON CONFLICT DO UPDATE`` list — a row
    that ever lands NULL there can never be repaired by a later sweep, and
    sorts every agent-filed ticket to the bottom of its backlog band forever
    (project_backlog.py's ``created_at ASC NULLS LAST``). Every ``kb_update``
    rewrites the whole file from scratch, so without this the first update
    strips both lines back off — potentially before any sweep has read them,
    since an inline index is allowed to defer.

    ``created`` is carried from the existing note verbatim, never
    regenerated: an edit is not a birth. It is serialised via ``isoformat()``
    when the source gives one (a tz-aware ``datetime`` off the pgvector row,
    a ``neo4j.time.DateTime`` off the graph), so the emitted scalar is
    something ``note_fields``' YAML parse resolves back to a timestamp
    instead of a repr. A note with no known creation time gains no line —
    absent stays absent, which is what ``upsert_kb_note`` COALESCEs against.

    ``modified`` is always stamped. kb_update is a mutation tool and its
    caller asserted a change, so the cost — the materialisation endpoint's
    blob-SHA ``skipped/unchanged`` short-circuit can no longer fire for an
    update, and a semantically-null update commits — is accepted rather than
    diffing before/after inside a fail-closed write path.
    """
    created = existing.get("created")
    if created:
        note["created"] = (
            created.isoformat() if hasattr(created, "isoformat") else str(created)
        )
    note["modified"] = datetime.now(timezone.utc).isoformat()


def _describe_update(
    *,
    content: Optional[str],
    append: Optional[str],
    status: Optional[str],
    confidence: Optional[str],
    add_links: Optional[List[dict]],
    tag_change: _TagResolution,
) -> List[str]:
    """The human-readable change list both kb_update paths report.

    Shared so the two backends cannot describe the same edit differently. The
    readiness line is called out by name rather than folded into a tag count:
    arming a ticket is the act that lets a job be spawned, and it should never
    read as "+1 tag(s)".
    """
    changes: List[str] = []
    if content is not None:
        changes.append("content replaced")
    if append is not None:
        changes.append("content appended")
    if status:
        changes.append(f"status → {status}")
    if confidence:
        changes.append(f"confidence → {confidence}")
    if tag_change.changed:
        changes.append(f"tags → [{', '.join(tag_change.tags)}]")
    if tag_change.ready is True:
        changes.append("READY for dispatch")
    elif tag_change.ready is False:
        changes.append("dispatch authorization withdrawn")
    if add_links:
        changes.append(f"+{len(add_links)} link(s)")
    return changes


# The kb_lint URL sweep checks at most this many unique external URLs per run;
# the remainder is reported loudly (never a silent cap).
_URL_SWEEP_CAP = 25

# Upper bound on the notes kb_lint/kb_index pull out of the knowledge index in
# one call — a guard against loading an unbounded number of note bodies into
# agent memory. Sized above the largest live vault (~3.2k notes); a run that
# hits it says so loudly, same posture as _URL_SWEEP_CAP.
_VAULT_SCAN_CAP = 5000


def _check_external_url(url: str, timeout: float = 5.0) -> Optional[str]:
    """Reachability probe for the kb_lint dead-URL sweep.

    Returns a failure reason for CLEAR negatives only — HTTP 404/410 and
    DNS/connection-level failures. Bot-shy responses (403/405) and transient
    5xx count as alive, keeping the rule false-positive-shy.
    """
    import urllib.error
    import urllib.request

    req = urllib.request.Request(
        url, method="HEAD", headers={"User-Agent": "srw-kb-lint/1.0"}
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout):
            return None
    except urllib.error.HTTPError as e:
        return f"HTTP {e.code}" if e.code in (404, 410) else None
    except urllib.error.URLError as e:
        return f"unreachable: {e.reason}"
    except Exception as e:
        return f"unreachable: {e}"


# Tool metadata for registry


#: Fail-closed message every degraded KB tool returns during a vector/KB
#: outage on a background-officer session (officer_knowledge_plane.md §3.1).
#: The wake sitrep carries the matching `project knowledge unavailable` line.
KB_UNAVAILABLE_ERROR = (
    "Error: project knowledge unavailable — the knowledge base backing this "
    "project cannot be reached. Knowledge reads and writes fail closed; job "
    "supervision, paging, and messaging continue to work. Do NOT reconstruct "
    "the backlog or project truth from memory or conversation — retry the "
    "knowledge tool once the outage clears."
)


def create_degraded_knowledge_tools(names: List[str]) -> List[Any]:
    """Fail-closed stand-ins for KB tools during a knowledge outage.

    A background officer must survive a vector/KB outage with supervision
    intact (officer_knowledge_plane.md §3.1): instead of silently dropping the
    knowledge grant (which would surface as baffling unknown-tool failures),
    each granted name binds to a stub that accepts any arguments and returns
    :data:`KB_UNAVAILABLE_ERROR`. Only names from the knowledge registry are
    honored. Backlog-derived dispatch (``auto_pull``, officer_backlog_pools)
    does not exist yet — when it ships it must consult the same availability
    state and fail closed rather than dispatch from a reconstructed queue.
    """
    from langchain_core.tools import StructuredTool
    from pydantic import BaseModel, ConfigDict

    class _AnyKbArgs(BaseModel):
        """Accepts whatever the model sends; the stub ignores it anyway."""

        model_config = ConfigDict(extra="allow")

    def _unavailable(**_kwargs: Any) -> str:
        return KB_UNAVAILABLE_ERROR

    tools: List[Any] = []
    for name in names:
        meta = KNOWLEDGE_TOOLS_METADATA.get(name)
        if meta is None:
            continue
        description = (
            f"{meta.get('short_description') or name} "
            "(currently DEGRADED: project knowledge unavailable — calls fail "
            "closed until the outage clears)"
        )
        tools.append(
            StructuredTool.from_function(
                func=_unavailable,
                name=name,
                description=description,
                args_schema=_AnyKbArgs,
            )
        )
    return tools


def _get_project_id(context: ToolContext) -> Optional[str]:
    """Get the sole writable native KB id, preserving legacy contexts."""
    bindings = _explicit_bindings(context)
    if bindings:
        for binding in bindings:
            if binding.is_native and binding.writable:
                return str(binding.kb_id)
        return None
    return context.project_id


def _get_project_ids(context: ToolContext) -> List[str]:
    """Get every authorized KB id, preserving legacy project scoping."""
    bindings = _explicit_bindings(context)
    if bindings:
        return [str(binding.kb_id) for binding in bindings]
    return list(context.project_ids)


def _explicit_bindings(context: ToolContext) -> List[KnowledgeBinding]:
    """Return real runtime bindings without mistaking test mocks for them."""
    value = getattr(context, "knowledge_bindings", None)
    if not isinstance(value, (list, tuple)):
        return []
    return [binding for binding in value if isinstance(binding, KnowledgeBinding)]


def _read_bindings(context: ToolContext) -> List[KnowledgeBinding]:
    """Authorized KBs, synthesized from project ids for old runtimes/tests."""
    bindings = _explicit_bindings(context)
    if bindings:
        return bindings

    result: List[KnowledgeBinding] = []
    for index, raw_id in enumerate(context.project_ids):
        try:
            kb_id = uuid.UUID(str(raw_id))
        except (TypeError, ValueError):
            continue
        alias = "project" if index == 0 else f"project-{kb_id.hex[:8]}"
        result.append(
            KnowledgeBinding(
                kb_id=kb_id,
                alias=alias,
                name="Project Knowledge"
                if index == 0
                else f"Project Knowledge {kb_id.hex[:8]}",
                kind="native",
                writable=index == 0,
                root_path="knowledge",
            )
        )
    return result


def _native_bindings(context: ToolContext) -> List[KnowledgeBinding]:
    """Native project scopes eligible for graph-only operations."""
    return [binding for binding in _read_bindings(context) if binding.is_native]


def _binding_for_project_id(
    context: ToolContext, project_id: Optional[str]
) -> Optional[KnowledgeBinding]:
    """The bound scope a resolved project id names, if any.

    Inverse of ``str(binding.kb_id)``: the write helpers thread a resolved
    *id* down, but naming the knowledge base in a message (or qualifying a
    handle) needs the binding back. ``None`` for a legacy context with nothing
    to resolve against — the caller then renders bare, as it always did.
    """
    if not project_id:
        return None
    needle = str(project_id)
    for binding in _read_bindings(context):
        if str(binding.kb_id) == needle:
            return binding
    return None


def _resolve_binding(context: ToolContext, selector: str) -> Optional[KnowledgeBinding]:
    needle = str(selector or "").strip().lower()
    for binding in _read_bindings(context):
        if binding.alias.lower() == needle or str(binding.kb_id).lower() == needle:
            return binding
    return None


def _binding_choices(bindings: List[KnowledgeBinding]) -> str:
    return ", ".join(f"{binding.alias} ({binding.name})" for binding in bindings)


def _select_bindings(
    context: ToolContext, selector: Optional[str]
) -> tuple[List[KnowledgeBinding], Optional[str]]:
    bindings = _read_bindings(context)
    if not bindings:
        return [], "Error: No project_id available."
    if not selector:
        return bindings, None
    binding = _resolve_binding(context, selector)
    if binding is None:
        return [], (
            f"Error: Knowledge base '{selector}' is not selected. Available: "
            f"{_binding_choices(bindings)}."
        )
    return [binding], None


def _write_scope_error(context: ToolContext) -> str:
    if _explicit_bindings(context):
        return (
            "Error: No writable native knowledge base is selected. "
            "External knowledge bases are read-only."
        )
    return "Error: No project_id available."


def _resolve_write_target(
    context: ToolContext, kb: Optional[str]
) -> tuple[Optional[KnowledgeBinding], Optional[str]]:
    """The native binding a write lands in (B5).

    ``writable`` marks the *default* target, not the only one: an agent with
    two native knowledge bases attached could previously only ever write to
    the first, and a note meant for the other silently landed in the session's
    own project. ``kb`` names any native binding explicitly; omitted, the
    target is the writable (default) native, exactly as before.

    External (datasource-backed) knowledge bases stay read-only — they are
    mirrors of somebody else's repository, with no write path at all.

    Returns ``(binding, None)`` or ``(None, error)``; the error is the tool's
    verbatim reply.
    """
    if kb:
        binding = _resolve_binding(context, kb)
        if binding is None:
            return None, (
                f"Error: Knowledge base '{kb}' is not selected. Available: "
                f"{_binding_choices(_read_bindings(context))}."
            )
        if not binding.is_native:
            natives = _binding_choices(_native_bindings(context))
            return None, (
                f"Error: Knowledge base '{binding.alias}' is read-only "
                f"(external). Native knowledge bases you can target with "
                f"kb=: {natives}."
            )
        return binding, None

    project_id = _get_project_id(context)
    if not project_id:
        return None, _write_scope_error(context)
    for binding in _read_bindings(context):
        if str(binding.kb_id) == project_id:
            return binding, None
    # A legacy context can carry a bare ``project_id`` with nothing in
    # ``project_ids`` for ``_read_bindings`` to synthesize from. That scope has
    # always been writable, so stand a binding up for it rather than refusing a
    # write that worked before write targets existed.
    try:
        kb_id = uuid.UUID(str(project_id))
    except (TypeError, ValueError):
        return None, _write_scope_error(context)
    return (
        KnowledgeBinding(
            kb_id=kb_id,
            alias="project",
            name="Project Knowledge",
            kind="native",
            writable=True,
            root_path="knowledge",
        ),
        None,
    )


def _native_scope_error(context: ToolContext) -> str:
    if _explicit_bindings(context):
        return "No native knowledge base with a Graph tier is selected."
    return "Error: No project_id available."


def _resolve_note_scope(
    context: ToolContext,
    note: str,
    kb: Optional[str],
) -> tuple[List[KnowledgeBinding], str, bool, Optional[str]]:
    """Resolve a note handle and optional selector into authorized bindings."""
    handle_alias, slug = split_note_handle(note)
    if not slug:
        return [], slug, False, "Error: A note slug is required."
    if handle_alias and kb:
        handle_binding = _resolve_binding(context, handle_alias)
        kb_binding = _resolve_binding(context, kb)
        if (
            handle_binding is None
            or kb_binding is None
            or handle_binding.kb_id != kb_binding.kb_id
        ):
            return (
                [],
                slug,
                False,
                (f"Error: Note handle selects '{handle_alias}' but kb selects '{kb}'."),
            )
    selector = handle_alias or kb
    bindings, error = _select_bindings(context, selector)
    return bindings, slug, selector is not None, error


# =============================================================================
# OKF markdown serialization (files-canonical KB)
# See knowledge-base/knowledge/features/okf_knowledge_base.md §7, §11.
# =============================================================================


def _derive_description(content: str) -> str:
    """One-sentence description from a note body (OKF progressive disclosure).

    Skips heading lines; takes the first sentence of the first prose line,
    capped at 200 chars. Slice 1 has no ``description`` input on most write
    paths, but every OKF note's frontmatter should still carry one — the whole
    economy of ``index.md`` files runs on it (§7).
    """
    for line in (content or "").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        match = re.match(r"(.+?[.!?])(\s|$)", line)
        desc = match.group(1) if match else line
        return desc[:200].strip()
    return ""


def _yaml_quote(value: str) -> str:
    """Quote a free-text YAML scalar, escaping quotes and collapsing newlines."""
    flat = " ".join(str(value).splitlines()).replace('"', '\\"')
    return f'"{flat}"'


def _render_note_md(note: Dict[str, Any]) -> str:
    """Serialize a note dict to an OKF/markdown document (pure function).

    OKF conventions (knowledge-base/knowledge/features/okf_knowledge_base.md §7): ``type`` +
    ``description`` frontmatter, standard **markdown** links (never wikilinks)
    for the emergent graph, in-note ``author``/``job``/``branch`` provenance
    (squash-merge erases git authorship). Optional fields are omitted when
    absent. Shared by ``kb_write``/``kb_update`` materialisation and ``kb_export``.

    Expected keys (all optional except ``id``/``type``): ``id``, ``type``,
    ``title``, ``description``, ``content``, ``tags``, ``keywords``,
    ``confidence``, ``status``, ``priority``, ``ready_at``, ``author``,
    ``job``, ``branch``, ``created``, ``modified``, ``superseded_by``,
    ``relationships`` ([{type, target}]).
    """
    note_id = note.get("id", "unknown")
    content = note.get("content", "") or ""
    description = note.get("description") or _derive_description(content)

    fm: List[str] = ["---", f"id: {note_id}", f"type: {note.get('type', 'unknown')}"]
    if description:
        fm.append(f"description: {_yaml_quote(description)}")
    if note.get("tags"):
        fm.append(f"tags: [{', '.join(_yaml_quote(t) for t in note['tags'])}]")
    if note.get("keywords"):
        fm.append(f"keywords: [{', '.join(_yaml_quote(k) for k in note['keywords'])}]")
    if note.get("confidence"):
        fm.append(f"confidence: {note['confidence']}")
    # Backlog rank as a human-facing word. Omitted when absent so non-ticket
    # notes keep their existing frontmatter byte-for-byte.
    if note.get("priority") is not None:
        raw_priority = note["priority"]
        word = (
            PRIORITY_WORDS.get(int(raw_priority))
            if isinstance(raw_priority, int)
            else str(raw_priority).strip().lower()
        )
        if word in ("high", "normal", "low"):
            fm.append(f"priority: {word}")
    # Dispatch authorization (B2). Quoted — a bare ISO timestamp is a YAML
    # timestamp scalar, and the reindexer wants the string back unchanged.
    # Omitted when absent, so every non-ticket note's frontmatter stays
    # byte-identical, same rule as priority.
    if note.get("ready_at"):
        fm.append(f"ready_at: {_yaml_quote(str(note['ready_at']))}")
    fm.append(f"status: {note.get('status', 'active')}")
    # Provenance (§7) — origin binding that git authorship can't carry.
    if note.get("author"):
        fm.append(f"author: {note['author']}")
    if note.get("job"):
        fm.append(f"job: {note['job']}")
    if note.get("branch"):
        fm.append(f"branch: {note['branch']}")
    if note.get("created"):
        fm.append(f"created: {note['created']}")
    if note.get("modified"):
        fm.append(f"modified: {note['modified']}")
    if note.get("superseded_by"):
        fm.append(f"superseded_by: {note['superseded_by']}")
    fm.append("---")
    fm.append("")

    # Prepend the title as an H1 — unless the body already opens with its own
    # H1, which would render the title twice (run-8 nit, docs §11.1).
    if content.lstrip().startswith("# "):
        body: List[str] = [content]
    else:
        body = [f"# {note.get('title') or note_id}", "", content]

    # Relationships as standard markdown links, grouped by type (§7).
    rels = note.get("relationships") or []
    if rels:
        body.extend(["", "## Relationships"])
        by_type: Dict[str, List[str]] = {}
        for r in rels:
            rt = r.get("type", "REFERENCES")
            by_type.setdefault(rt, []).append(r.get("target", "?"))
        for rel_type, targets in by_type.items():
            links = ", ".join(f"[{t}]({t}.md)" for t in targets)
            body.append(f"**{rel_type}:** {links}")

    return "\n".join(fm) + "\n".join(body) + "\n"


def _note_provenance(context: ToolContext) -> Dict[str, Optional[str]]:
    """Resolve in-note provenance (author role, job, git branch) from context."""
    author: Optional[str] = None
    meta = getattr(context, "_job_metadata", None)
    if isinstance(meta, dict):
        author = meta.get("config_name")
    if not author:
        try:
            author = context.config.get("agent_id")
        except Exception:
            author = None
    branch: Optional[str] = None
    try:
        branch = context.workspace_manager.git_manager.current_branch()
    except Exception:
        branch = None
    return {"author": author, "job": context.job_id, "branch": branch}


# =============================================================================
# Server-side materialisation
# (knowledge-base/knowledge/features/knowledge_base_repo_separation.md §3, step 4)
#
# A note used to be written twice: the row into ``knowledge_index``, and the
# file into the agent's own workspace checkout (``_dual_write_note``). That
# second write welded the vault to whatever repo the workspace happened to be
# a clone of, and — because it was guarded on ``has_git()`` — skipped entirely
# wherever there was no git: persistent sessions, lite tiers, repo-less
# projects. Their notes stayed pathless, and ``kb_read``/``kb_search`` gate on
# ``path IS NOT NULL``, so they were invisible to every reader.
#
# There is one write path now and it is server-side: the agent POSTs the
# rendered markdown, the orchestrator commits it into whichever repo §5's
# ``resolve_kb_repo`` picks for the project. It needs neither git nor a
# workspace, which is what makes the note visible on those runtimes for the
# first time. Rendering stays here (the orchestrator does not know OKF's note
# format); only the commit moved.
# =============================================================================

# Generous, because losing the note is worse than blocking the tool call: a
# missed materialisation leaves a row no reader can see (§10).
_MATERIALIZE_TIMEOUT_SECONDS = 30.0

# Deliberately identical to the prefix orchestrator/services/kb_materialize.py
# logs under, so ONE grep/alert rule — `kb-materialize:` at ERROR — catches a
# broken vault write from either side of the seam. §10 asks for a signal that
# is actually looked at: after this change a failed materialisation means the
# note exists in Postgres and is invisible to every reader, which is not a
# warning.
_MATERIALIZE_LOG = "kb-materialize:"


def _post_vault_file(
    project_id: str,
    slug: str,
    content: str,
    job_id: Optional[str],
    retrieval_messages: Optional[List[str]] = None,
    expected_blob_sha: Optional[str] = None,
) -> Dict[str, Any]:
    """POST one rendered note to the orchestrator's materialisation endpoint.

    Returns the endpoint's ``{status, reason, repo, branch, path, operation}``
    verbatim, or a synthesized ``failed`` result when the call itself did not
    complete. **Never raises** — every caller must be able to log and carry on,
    exactly as the old non-fatal file write did.

    The endpoint answers HTTP 200 for every KB-level outcome on purpose (its
    failure vocabulary lives in the body), so a non-200 here is a transport or
    auth problem, not a refused note.

    ``retrieval_messages`` is the one note field the markdown cannot carry —
    OKF frontmatter has no such key, so the POST body is its only route to
    ``knowledge_index``. The key is omitted entirely unless there is something
    to send, because the endpoint reads a missing/None value as "leave the
    stored value alone" (``KnowledgeStore.upsert_kb_note``'s COALESCE
    sentinel). Sending ``[]`` would instead blank an earlier write's messages.
    """
    base_url = os.getenv("ORCHESTRATOR_URL", "http://localhost:8085").rstrip("/")
    url = f"{base_url}/api/projects/{project_id}/knowledge/materialize"
    headers: Dict[str, str] = {}
    internal_key = os.getenv("MCP_INTERNAL_KEY", "")
    if internal_key:
        headers["X-Internal-Key"] = internal_key

    payload: Dict[str, Any] = {"slug": slug, "content": content}
    if job_id:
        payload["job_id"] = str(job_id)
    if retrieval_messages:
        payload["retrieval_messages"] = list(retrieval_messages)
    if expected_blob_sha:
        # Compare-and-swap (kb_gardening G3): the endpoint refuses the write
        # if the repo no longer holds this blob at the path.
        payload["expected_blob_sha"] = str(expected_blob_sha)

    try:
        with httpx.Client(
            timeout=_MATERIALIZE_TIMEOUT_SECONDS, headers=headers
        ) as client:
            response = client.post(url, json=payload)
    except Exception as e:  # noqa: BLE001 — non-fatal by contract
        return {"status": "failed", "reason": f"unreachable: {e.__class__.__name__}"}

    if response.status_code != 200:
        return {"status": "failed", "reason": f"http-{response.status_code}"}

    try:
        body = response.json()
    except Exception:  # noqa: BLE001 — a non-JSON 200 is still a failed write
        return {"status": "failed", "reason": "malformed-response"}
    if not isinstance(body, dict) or not body.get("status"):
        return {"status": "failed", "reason": "malformed-response"}
    return body


def _materialize_note(
    context: ToolContext,
    slug: str,
    note: Dict[str, Any],
    retrieval_messages: Optional[List[str]] = None,
    expected_blob_sha: Optional[str] = None,
    *,
    project_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Commit a note as ``knowledge/<slug>.md`` in the project's KB repo.

    Renders ``note`` with the same serializer the file has always used, stamps
    in-note provenance (author/job/branch — squash-merge erases git
    authorship), and hands the markdown to the orchestrator, which owns the
    commit *and* the searchable row it indexes from that commit. No workspace
    and no git are required, and none is touched.

    The endpoint never raises for expected repository failures, but callers
    must fail closed unless the returned state proves the canonical write.
    Retry intent is persisted before the remote mutation.

    ``retrieval_messages`` rides along out of band because the markdown has
    nowhere to put it; ``None`` (the default, and what every caller with no
    opinion passes) means "leave whatever is stored alone".

    ``project_id`` names the knowledge base to commit into. Callers that have
    already resolved a write target (``kb_write``'s ``kb=``) pass it; omitted,
    it falls back to the session's default writable native scope, which is
    what every caller did before targets existed.

    Returns the endpoint's status dict, for callers that want to report the
    outcome. ``status`` is ``committed`` / ``skipped`` / ``failed``, and
    ``indexed`` / ``index_reason`` say whether it is searchable yet.
    """
    project_id = project_id or _get_project_id(context)
    if not project_id:
        # No writable native KB in scope. The row write upstream already
        # failed its own scope check in that case, so this is belt-and-braces.
        logger.error(
            "%s no writable project knowledge base in scope — note '%s' was "
            "NOT materialised and stays invisible to kb_read/kb_search",
            _MATERIALIZE_LOG,
            slug,
        )
        return {"status": "failed", "reason": "no-project-scope"}

    try:
        enriched = dict(note)
        for key, value in _note_provenance(context).items():
            if value:
                enriched[key] = value
        content = _render_note_md(enriched)
        job_id = context.job_id
    except Exception as e:  # noqa: BLE001 — a serializer bug must not fail kb_write
        logger.error(
            "%s could not render note '%s' for project %s: %r — NOT "
            "materialised, and invisible to kb_read/kb_search until rewritten",
            _MATERIALIZE_LOG,
            slug,
            project_id,
            e,
            exc_info=True,
        )
        return {"status": "failed", "reason": "render-error"}

    result = _post_vault_file(
        project_id,
        slug,
        content,
        job_id,
        retrieval_messages=retrieval_messages,
        expected_blob_sha=expected_blob_sha,
    )

    if str(result.get("status")) == "failed":
        logger.error(
            "%s note '%s' (project %s) was NOT materialised (%s) — its durable "
            "intent remains unresolved and callers must leave the searchable "
            "projection unchanged",
            _MATERIALIZE_LOG,
            slug,
            project_id,
            result.get("reason") or "unknown",
        )
    else:
        logger.debug(
            "%s note '%s' (project %s): %s%s",
            _MATERIALIZE_LOG,
            slug,
            project_id,
            result.get("status"),
            f" ({result.get('reason')})" if result.get("reason") else "",
        )
    return result


def _canonical_materialization_succeeded(result: Dict[str, Any]) -> bool:
    """Whether the result proves the desired bytes are canonical and durable."""
    if result.get("canonical_state") == "canonical":
        return True
    return result.get("status") == "committed" or (
        result.get("status") == "skipped"
        and result.get("reason") in {"unchanged", "already-canonical"}
    )


def _canonical_materialization_error(
    slug: str,
    result: Dict[str, Any],
    *,
    target: Optional[KnowledgeBinding] = None,
    alternatives: Sequence[KnowledgeBinding] = (),
    surface: str = "write",
) -> str:
    """The tool's verbatim reply for a canonical write that did not land.

    ``target`` is the knowledge base the write was aimed at and
    ``alternatives`` the native ones the author could aim at instead. Both
    matter only for a *permanent* failure: with several knowledge bases in
    scope, "it failed" without naming which one leaves the author retrying
    the same doomed target forever (WP5.4).

    ``surface`` names *how* the caller retargets, because the two surfaces do
    not share a mechanism (final review, Important 3). ``kb_write`` takes a
    ``kb=`` argument; ``kb_update``/``kb_delete`` have no such parameter — they
    retarget by qualifying the note handle as ``alias:slug`` (B5). Naming
    ``kb=`` on an update is an instruction the caller cannot follow.
    """
    state = result.get("canonical_state") or "failed"
    reason = result.get("reason") or "unknown"
    retry = result.get("retry_state") or "unknown"
    # Older endpoint shapes omit the field; they always recorded their intent.
    recorded = result.get("recorded", True)
    if reason == "precondition-failed":
        # The compare-and-swap token did not match: another writer changed or
        # removed the note between this caller's read and its write. Nothing
        # was applied and nothing will be replayed — re-read, then decide.
        return (
            f"Error: '{slug}' changed (or was removed) since you read it — "
            "your update was NOT applied to avoid overwriting the other "
            "writer's version. kb_read it again and re-apply what still holds."
        )
    if retry == "permanent":
        if reason == "no-repo":
            where = f"{target.alias} — {target.name}" if target else "the target"
            others = ", ".join(
                f"{binding.alias} ({binding.name})"
                for binding in alternatives
                if target is None or binding.kb_id != target.kb_id
            )
            how = "a qualified `alias:slug` handle" if surface == "update" else "kb="
            hint = (
                f" Native knowledge bases you can target with {how}: {others}."
                if others
                else ""
            )
            return (
                f"Error: '{slug}' was NOT written and will not be retried — the "
                f"target knowledge base ({where}) has no vault repository, so "
                f"notes cannot be stored there.{hint}"
            )
        return (
            f"Error: '{slug}' was NOT written and will not be retried "
            f"(reason={reason})."
        )
    # Nothing was recorded means there is no pending-sync row to inspect —
    # pointing the author at one would send them looking for nothing.
    tail = " Retry, or inspect the pending-sync ledger." if recorded else " Retry."
    return (
        f"Error: canonical knowledge write for '{slug}' did not complete "
        f"(state={state}, reason={reason}, retry={retry}). The mutation remains "
        f"unapplied.{tail}"
    )


def _index_state_suffix(result: Dict[str, Any]) -> str:
    """How the tool reports whether the note is searchable yet.

    The orchestrator indexes inline with the commit, but defers a note that is
    too large or arrives while a rebuild holds the KB lock. Saying "created"
    without saying which happened is how the old docstring came to promise
    immediate searchability it could not deliver.
    """
    if result.get("indexed"):
        return "[canonical=canonical, indexed=yes]"
    reason = result.get("index_reason") or "pending"
    return f"[canonical=canonical, indexed=deferred:{reason}]"


# ``_report_projection`` used to POST the agent's own projection outcome back
# to the intent ledger. Slice A deleted both writes it reported on, and the
# materialisation endpoint now closes the ledger itself the moment it indexes
# the commit (kb_materialize.py `finish_knowledge_projection`), so there is
# nothing left for the agent to report. ``_canonical_ready_at`` went with it:
# its only consumers were the two deleted `upsert_note(ready_at=...)`
# arguments — the READY authorization now travels solely as the file's
# `ready_at:` frontmatter line (`_apply_ready_frontmatter`).


# The OKF vault root — the same prefix the reindexer scans and the
# materialisation endpoint commits under
# (orchestrator/services/kb_reindex.py KNOWLEDGE_PREFIX).
_VAULT_ROOT = "knowledge"


def _export_dir_error(path: str) -> Optional[str]:
    """Reject an export destination that would corrupt the vault, else None.

    ``kb_export`` creates ``path`` as a directory and writes one
    ``<note_id>.md`` into it per note. Two destinations are never what the
    caller meant, and both have been observed live:

    1. **A note filename.** Passing ``knowledge/some-note.md`` (or any
       ``*.md``) makes a DIRECTORY whose name ends in ``.md``. Git then
       cannot hold a blob at that name, so the note it was named after
       ceases to exist as a file.
    2. **Anywhere under ``knowledge/``.** The reindexer globs
       ``knowledge/**/*.md`` recursively, so an export into the vault gives
       every note a second file with the *same* OKF id. That collides on
       ``uq_knowledge_project_note``, which no ``ON CONFLICT (kb_id, path)``
       arbiter can absorb — one copy loses and re-fails on every reindex,
       forever. The vault's own files already ARE the canonical OKF export,
       so there is nothing a copy inside it could add.

    Returns an operator-readable message naming the fix, or None to proceed.
    """
    cleaned = str(path or "").strip().replace("\\", "/").strip("/")
    parts = [p for p in cleaned.split("/") if p not in ("", ".")]
    if not parts:
        return (
            "Error: kb_export needs a destination directory "
            "(e.g. `exports/kb-2026-01-01`), not an empty path."
        )
    if parts[0] == _VAULT_ROOT:
        return (
            f"Error: refusing to export into `{_VAULT_ROOT}/` — that is the "
            "vault itself, and its `*.md` files already ARE the canonical OKF "
            "export. A copy inside it gives every note a duplicate id and "
            "breaks the KB index. Export somewhere else (e.g. `exports/kb`)."
        )
    if parts[-1].lower().endswith(".md"):
        return (
            f"Error: `{path}` looks like a note file, but kb_export needs a "
            "DIRECTORY to write into — it would create a directory named "
            f"`{parts[-1]}` and fill it with one file per note. Pass a "
            "directory (e.g. `exports/kb`) instead."
        )
    return None


# =============================================================================
# Gardener source of truth — the knowledge index, not the workspace
# (knowledge-base/knowledge/features/knowledge_base_repo_separation.md §5a)
#
# kb_lint/kb_index used to glob `knowledge/*.md` off the workspace. Once the
# vault moves into its own server-side repo that glob does not fail, it returns
# nothing — a healthy KB reported as empty, which is the most dangerous outcome
# of the repo separation. Both tools therefore read `knowledge_index` rows and
# render each one back into the OKF note it materialises to, using the very
# serializer that writes the file (`_render_note_md`), so every existing lint
# rule keeps working on byte-equivalent input.
#
# What the index cannot see, and why that is acceptable: `invalid-yaml` /
# `missing-frontmatter` / `missing-title` describe defects of a *file*, and a
# row cannot be malformed in those ways — a note that fails to parse never
# becomes a row at all (the reindexer's `note_fields` hardening), and the
# reindex watermark is where that surfaces. In exchange the index shows a class
# of defect files never could: a row no file backs (`path IS NULL`), invisible
# to kb_read/kb_search. See `_unmaterialised_finding`.
# =============================================================================


def _vault_path_intent(path: Optional[str]) -> tuple[bool, Optional[str]]:
    """Classify a gardener ``path`` argument as ``(targets_the_vault, error)``.

    The argument is vestigial for the vault itself — those notes come from the
    index now, wherever their files live. It survives for the one question the
    index cannot answer: an explicit *other* markdown directory in the
    workspace (`docs`, a repository datasource checkout). So:

    - omitted, or ``knowledge`` (the old default) → the project KB, read from
      the index. Never re-interpreted as "glob a workspace directory that no
      longer holds the vault" — that reinterpretation is precisely the
      silent-empty failure this refactor exists to remove.
    - somewhere *inside* the vault (``knowledge/sub``) → refused with a reason,
      for the same reason: there is nothing there to glob.
    - anything else → a workspace directory, globbed as before.
    """
    cleaned = str(path or "").strip().replace("\\", "/").strip("/")
    parts = [p for p in cleaned.split("/") if p not in ("", ".")]
    if not parts:
        return True, None
    if parts[0].lower() != _VAULT_ROOT:
        return False, None
    if len(parts) == 1:
        return True, None
    return True, (
        f"Error: `{path}` points inside the knowledge vault, which is no longer "
        "a workspace directory — its notes live in the knowledge index and in "
        "the knowledge repo, so there is nothing there to scan. Omit `path` to "
        "target the whole knowledge base."
    )


def _store_note_markdown(row: Dict[str, Any], root: str) -> Dict[str, str]:
    """Render one ``knowledge_index`` row as the OKF note file it materialises to.

    ``{"path", "text"}`` — the shape ``lint_kb``/``external_url_map`` consume.
    The path is the row's real file path when the reindexer has stamped one,
    otherwise the flat vault path the note will land at, so a finding is always
    anchored somewhere the agent can act on. ``description`` is intentionally
    left out: the index has no such column, so ``_render_note_md`` derives it
    from the body exactly as it does on the write path.
    """
    return {
        "path": row.get("path") or f"{root}/{row.get('id')}.md",
        "text": _render_note_md(
            {
                "id": row.get("id"),
                "type": row.get("type") or "unknown",
                "title": row.get("title"),
                "content": row.get("content") or "",
                "tags": row.get("tags") or [],
                "keywords": row.get("keywords") or [],
                "confidence": row.get("confidence"),
                "status": row.get("status"),
                "priority": row.get("priority"),
                "job": str(row["job_id"]) if row.get("job_id") else None,
                "created": row.get("created"),
                "modified": row.get("modified"),
                "superseded_by": row.get("superseded_by"),
            }
        ),
    }


def _unmaterialised_finding(rows: List[Dict[str, Any]], root: str) -> List[Finding]:
    """One aggregate warning for index rows that no file backs yet.

    These are the notes ``kb_read``/``kb_list`` cannot return (both gate on
    ``path IS NOT NULL``), so a KB where materialisation is broken reads empty
    while linting full. Aggregated rather than per-note on purpose: a lite-tier
    KB can have every row pathless, and 3,000 identical warnings would bury the
    findings that need acting on.
    """
    pending = [str(r.get("id")) for r in rows if not r.get("path")]
    if not pending:
        return []
    shown = ", ".join(pending[:5])
    more = f" (+{len(pending) - 5} more)" if len(pending) > 5 else ""
    return [
        Finding(
            "unmaterialised-note",
            "warning",
            root,
            f"{len(pending)} of {len(rows)} note(s) have no file backing them "
            f"yet: {shown}{more} — they are in the knowledge index but "
            "`kb_read`/`kb_search` cannot see them until the note is written "
            "into the knowledge repo and picked up by a reindex",
        )
    ]


def create_kb_tools(
    context: ToolContext,
    verdict_service: Any = None,
    verdict_prompt: Optional[str] = None,
) -> List[Any]:
    """Create knowledge base tools with injected context.

    Args:
        context: ToolContext with knowledge_graph and knowledge_store
        verdict_service: Optional KnowledgeVerdictService (OKF KB slice 2 PR2).
            When present (only the curator passes it), ``kb_write`` routes each
            candidate through the ingestion verdict gate before writing —
            ADD / UPDATE / SUPERSEDE / DISCARD. Every other caller (loop/worker
            agents) omits it and writes ungated, exactly as before.
        verdict_prompt: The verdict system prompt, resolved at event time.

    Returns:
        List of LangChain tool functions
    """
    kg = context.knowledge_graph
    ks = context.knowledge_store

    # Capture the event loop at tool creation time (async graph setup).
    # Sync tools run in executor threads — they must schedule async work
    # on the original loop to preserve asyncpg connection pool affinity.
    # Using asyncio.run() from a thread would create a NEW loop, breaking
    # connections tied to the original one ("attached to a different loop").
    try:
        _creator_loop = asyncio.get_running_loop()
    except RuntimeError:
        _creator_loop = None

    def _run_async(coro):
        """Run async coroutine from sync tool context on the original event loop."""
        if _creator_loop and _creator_loop.is_running():
            future = asyncio.run_coroutine_threadsafe(coro, _creator_loop)
            return future.result()
        return asyncio.run(coro)

    # Neo4j is OPTIONAL (slice-3 PR4c): the pgvector index is canonical for
    # retrieval and the OKF files for content, so the KB works without a graph.
    # Only the store is required; graph-shaped tools degrade when kg is None.
    if not ks:
        raise ValueError("Knowledge tools require knowledge_store in ToolContext")

    _has_bound_scopes = bool(_explicit_bindings(context))

    def _read_from_binding(
        binding: KnowledgeBinding, note_id: str
    ) -> Optional[Dict[str, Any]]:
        """Read one note through the backend appropriate to its KB kind.

        Native KBs try the graph first and fall through to the index (H1):
        a vault-imported note exists only in Postgres, an agent-written one
        may exist only in Neo4j, and neither store is authoritative for the
        other. A note present in either resolves.
        """
        if binding.is_native and kg is not None:
            data = kg.read_note(str(binding.kb_id), note_id)
            if data:
                return data
            logger.debug("kb_read: %s not in graph, trying index", note_id)
        return _run_async(ks.get_note_by_slug(binding.kb_id, note_id))

    def _matching_notes(
        bindings: List[KnowledgeBinding], note_id: str
    ) -> List[tuple[KnowledgeBinding, Dict[str, Any]]]:
        matches: List[tuple[KnowledgeBinding, Dict[str, Any]]] = []
        for binding in bindings:
            data = _read_from_binding(binding, note_id)
            if data:
                matches.append((binding, data))
                # Legacy multi-project reads historically returned the first hit.
                if not _has_bound_scopes:
                    break
        return matches

    def _ambiguous_note(note_id: str, matches: List[Any]) -> str:
        aliases = [
            match[0].alias if isinstance(match, tuple) else match.alias
            for match in matches
        ]
        handles = ", ".join(f"{alias}:{note_id}" for alias in aliases)
        return (
            f"Error: Note '{note_id}' is ambiguous across selected knowledge "
            f"bases. Use a qualified handle or kb selector: {handles}."
        )

    def _qualified(binding: KnowledgeBinding, note_id: str) -> str:
        return binding.handle(note_id) if _has_bound_scopes else note_id

    def _updated_handle(project_id: Optional[str], note_id: str) -> str:
        """What an ``Updated **…**`` line quotes back to the author.

        The mutation may have landed in any native scope (``kb=``, or a
        qualified handle), so a bare slug no longer says where — and it is the
        string the author copies into the next kb_read/kb_update. Degrades to
        the bare slug for single-scope and legacy runtimes.

        Callers depend on the ``Updated **`` prefix surviving this (kb_write's
        SUPERSEDE retire loop keys its success test on it), which it does: the
        qualification happens *inside* the bold markers.
        """
        binding = _binding_for_project_id(context, project_id)
        return _qualified(binding, note_id) if binding is not None else note_id

    def _external_snapshot_marker(bindings: List[KnowledgeBinding]) -> str:
        """Best-effort convergence marker for external indexed content."""
        snapshots: List[str] = []
        for binding in bindings:
            if binding.is_native:
                continue
            commit = (
                binding.indexed_commit
                if isinstance(binding.indexed_commit, str)
                else None
            )
            status = "ready"
            source_head: Optional[str] = None
            try:
                watermark = _run_async(ks.get_watermark(binding.kb_id))
                live_commit = getattr(watermark, "indexed_commit", None)
                if isinstance(live_commit, str) and live_commit:
                    commit = live_commit
                live_status = getattr(watermark, "status", None)
                if isinstance(live_status, str) and live_status:
                    status = live_status
                live_head = getattr(watermark, "source_head", None)
                if isinstance(live_head, str) and live_head:
                    source_head = live_head
            except Exception as e:
                logger.debug(
                    "Knowledge watermark lookup skipped for %s: %s",
                    binding.alias,
                    e,
                )
            if status != "ready":
                last_clean = (
                    f"last clean @ {commit[:12]}" if commit else "no clean commit"
                )
                attempted = f", source @ {source_head[:12]}" if source_head else ""
                snapshots.append(
                    f"[{binding.alias}] {status} — {last_clean}{attempted}"
                )
            else:
                snapshots.append(
                    f"[{binding.alias}] @ {commit[:12]}"
                    if commit
                    else f"[{binding.alias}] watermark unavailable"
                )
        if not snapshots:
            return ""
        label = (
            "External indexed snapshot"
            if len(snapshots) == 1
            else "External indexed snapshots"
        )
        return f"**{label}:** {', '.join(snapshots)}"

    def _index_readiness_notice(bindings: List[KnowledgeBinding]) -> str:
        """Advisory for the zero-result branches when a bound KB is still indexing.

        Covers native *and* external bindings (unlike ``_external_snapshot_marker``,
        which is external-only). Without this, an agent querying a KB that is still
        embedding gets an empty result indistinguishable from a genuine miss and may
        wrongly conclude the KB is empty. Only emits for an explicit non-ready
        watermark status; a missing watermark or lookup failure stays silent so we
        never raise a false "indexing" alarm.

        A watermark whose ``wedged_since`` is set means the same note has failed to
        index for several sweeps running — the KB is not "still indexing", it has a
        stuck note that will not resolve itself. That case gets its own honest
        wording instead of the rebuilding one. ``wedged_since``/``last_error`` are
        read with ``getattr`` so an old watermark row (or a test double without the
        new fields) degrades to today's rebuilding text.

        A watermark's ``advisory`` (migration 0022) records what the last sweep
        *skipped* rather than failed — a duplicate note id, say. That is exactly
        the explanation a zero-result branch owes the reader ("the note you are
        looking for exists in the vault but was not indexed"), and it holds for a
        ``ready`` knowledge base too: a skipped duplicate leaves the index clean
        and still incomplete. So the advisory is collected for every binding
        regardless of status, and rendered last (final review, Important 2).
        """
        notices: List[str] = []
        wedged: List[str] = []
        advisories: List[str] = []
        for binding in bindings:
            try:
                watermark = _run_async(ks.get_watermark(binding.kb_id))
            except Exception as e:
                logger.debug(
                    "Knowledge readiness lookup skipped for %s: %s",
                    binding.alias,
                    e,
                )
                continue
            # ``isinstance`` rather than a bare truth test: a MagicMock-shaped
            # watermark (or an old row) yields a non-string here, and a false
            # advisory line is worse than none.
            advisory = getattr(watermark, "advisory", None)
            if isinstance(advisory, str) and advisory.strip():
                advisories.append(f"[{binding.alias}] {advisory.strip()}")
            status = getattr(watermark, "status", None)
            if not isinstance(status, str) or status == "ready":
                continue
            wedged_since = getattr(watermark, "wedged_since", None)
            if isinstance(wedged_since, datetime):
                # Defensive: asyncpg returns tz-aware datetimes for TIMESTAMPTZ;
                # naive values only come from hand-built rows.
                if wedged_since.tzinfo is None:
                    wedged_since = wedged_since.replace(tzinfo=timezone.utc)
                hours = max(
                    1,
                    int(
                        (datetime.now(timezone.utc) - wedged_since).total_seconds()
                        // 3600
                    ),
                )
                m = re.match(
                    r"^(\d+) note operation",
                    str(getattr(watermark, "last_error", "") or ""),
                )
                count = m.group(1) if m else "some"
                wedged.append(
                    f"[{binding.alias}] {count} note(s) have failed to index for {hours} h "
                    f"(see orchestrator log `kb_reindex[{binding.kb_id}]`); the rest of this "
                    "knowledge base is current."
                )
                continue
            commit = getattr(watermark, "indexed_commit", None)
            source_head = getattr(watermark, "source_head", None)
            clean = (
                f"last clean @ {commit[:12]}"
                if isinstance(commit, str) and commit
                else "no clean commit yet"
            )
            attempted = (
                f", source @ {source_head[:12]}"
                if isinstance(source_head, str) and source_head
                else ""
            )
            notices.append(f"[{binding.alias}] {status} — {clean}{attempted}")
        if not notices and not wedged and not advisories:
            return ""
        parts = []
        if wedged:
            parts.append("⚠️ " + "; ".join(wedged))
        if notices:
            parts.append(
                "⚠️ Still indexing — results may be incomplete: " + "; ".join(notices)
            )
        parts.extend(f"ℹ️ {line}" for line in advisories)
        return "\n".join(parts)

    # =========================================================================
    # Write Tools
    # =========================================================================

    def _update_existing_kgless(
        note: str,
        content: Optional[str],
        append: Optional[str],
        status: Optional[str],
        confidence: Optional[str],
        add_tags: Optional[List[str]],
        add_links: Optional[List[dict]],
        priority: Optional[int] = None,
        remove_tags: Optional[List[str]] = None,
        set_tags: Optional[List[str]] = None,
        project_id: Optional[str] = None,
    ) -> str:
        """Neo4j-less update: read the row from the store, apply the mutation in
        Python (the graph does this in Cypher), write the OKF file back.

        Same return contract and status/confidence validation as the Neo4j path.
        ``add_links`` round-trip as generic body links via the reindexer — the
        graph-only relationship *type* is not preserved (no Neo4j to hold it),
        consistent with the honest graph-tier degrade elsewhere in PR4c.

        ``project_id`` names the knowledge base to read and rewrite in; omitted,
        it is the session's default writable native scope. It must be threaded
        wherever the scope is derived below: a read from one knowledge base and
        a write into another silently mutates the wrong note (B5).
        """
        project_id = project_id or _get_project_id(context)
        if not project_id:
            return _write_scope_error(context)

        if status is not None and status not in NOTE_STATUSES:
            return f"Error: Invalid status: {status}"
        if confidence is not None and confidence not in CONFIDENCE_LEVELS:
            return f"Error: Invalid confidence: {confidence}"

        try:
            existing = _run_async(ks.get_note_by_slug(uuid.UUID(project_id), note))
            if not existing:
                return f"Error: Note '{note}' not found in project."
            refusal = _marker_refusal(existing, content, append, note)
            if refusal:
                return refusal

            if (existing.get("type") or "") == "charter":
                denied = _charter_write_denied(context, project_id)
                if denied:
                    return denied

            machine_tags_authorized = False
            if _machine_tag_mutation_requested(
                existing.get("tags"),
                add=add_tags,
                remove=remove_tags,
                replace=set_tags,
            ):
                authorization = _has_officer_authority(context, project_id)
                if not authorization:
                    return authorization.tool_message()
                machine_tags_authorized = True

            if content is not None:
                new_content = content
            elif append is not None:
                new_content = (existing.get("content") or "") + "\n\n" + append
            else:
                new_content = existing.get("content") or ""

            new_status = status or existing.get("status") or "active"
            new_confidence = (
                confidence if confidence is not None else existing.get("confidence")
            )
            # None means "leave unchanged" — never fall back to
            # DEFAULT_PRIORITY_RANK here, or every status/tag/content-only
            # edit would silently reset an existing ticket's priority to
            # normal (Global constraint).
            new_priority = (
                priority
                if priority is not None
                else existing.get("priority", DEFAULT_PRIORITY_RANK)
            )
            new_type = existing.get("type") or "learning"
            new_title = existing.get("title") or note

            tag_change = _resolve_tags(
                existing.get("tags"),
                add=add_tags,
                remove=remove_tags,
                replace=set_tags,
                officer_authority=machine_tags_authorized,
            )
            merged_tags = tag_change.tags

            # The OKF file is the only write: the materialisation endpoint
            # commits it and owns the searchable row it indexes from that
            # commit (Slice A). Everything the row needs has to be in these
            # bytes, or in the POST that carries them.
            updated_note = {
                "id": note,
                "type": new_type,
                "title": new_title,
                "description": existing.get("description"),
                "content": new_content,
                "tags": merged_tags,
                "keywords": existing.get("keywords", []),
                "confidence": new_confidence,
                "status": new_status,
                "relationships": add_links or [],
            }
            if new_type in _TICKET_TYPES:
                updated_note["priority"] = new_priority
            _apply_ready_frontmatter(updated_note, tag_change.ready, existing)
            _carry_timestamps(updated_note, existing)
            materialization = _materialize_note(
                context,
                note,
                updated_note,
                # None means "leave the stored value alone". get_note_by_slug
                # does not read the column, so this tier never has messages of
                # its own to forward. None is the better-typed way to say
                # that, but it is NOT what protects the stored value: the
                # protection is `_post_vault_file`'s `if retrieval_messages:`
                # guard, which drops [] from the payload exactly as it drops
                # None. Simplify that guard away and BOTH become blanking
                # writes (see test_omits_retrieval_messages_entirely_...).
                existing.get("retrieval_messages"),
                # Compare-and-swap on the blob this row was indexed from
                # (kb_gardening G3): a concurrent rewrite or removal of the
                # note makes this write fail loudly instead of silently
                # winning — or silently re-creating a deleted file.
                expected_blob_sha=existing.get("blob_sha"),
                project_id=project_id,
            )
            if not _canonical_materialization_succeeded(materialization):
                return _canonical_materialization_error(
                    note,
                    materialization,
                    target=_binding_for_project_id(context, project_id),
                    alternatives=_native_bindings(context),
                    # kb_update/kb_delete have no `kb=` argument; they retarget
                    # by qualifying the handle (`alias:slug`).
                    surface="update",
                )

            changes = _describe_update(
                content=content,
                append=append,
                status=status,
                confidence=confidence,
                add_links=add_links,
                tag_change=tag_change,
            )
            return (
                f"Updated **{_updated_handle(project_id, note)}**: "
                f"{', '.join(changes)}"
                f"{_dropped_tag_notice(tag_change.dropped)}"
                f" {_index_state_suffix(materialization)}"
            )
        except Exception as e:
            logger.error(f"kb_update failed: {e}")
            return f"Error updating note: {e}"

    def _update_existing(
        note: str,
        content: Optional[str] = None,
        append: Optional[str] = None,
        status: Optional[str] = None,
        confidence: Optional[str] = None,
        priority: Optional[int] = None,
        add_tags: Optional[List[str]] = None,
        add_links: Optional[List[dict]] = None,
        remove_tags: Optional[List[str]] = None,
        set_tags: Optional[List[str]] = None,
        project_id: Optional[str] = None,
    ) -> str:
        """Update canonical git first, then the optional Neo4j projection.

        The searchable row is not written here: the materialisation endpoint
        indexes it from the commit this makes (Slice A). Neo4j is the only
        projection left for the tool to drive, and it degrades best-effort.

        ``project_id`` names the knowledge base to read and rewrite in; omitted,
        it is the session's default writable native scope, which is what
        ``kb_update`` passes. ``kb_write`` delegates here with an explicit
        target (``kb=``), and every scope derivation below has to honour it —
        reading the note from one knowledge base and committing the rewrite
        into another would silently mutate a same-slug note in the wrong place
        and report success (B5).
        """
        if kg is None:
            return _update_existing_kgless(
                note,
                content,
                append,
                status,
                confidence,
                add_tags,
                add_links,
                priority=priority,
                remove_tags=remove_tags,
                set_tags=set_tags,
                project_id=project_id,
            )

        project_id = project_id or _get_project_id(context)
        if not project_id:
            return _write_scope_error(context)

        if status is not None and status not in NOTE_STATUSES:
            return f"Error: Invalid status: {status}"
        if confidence is not None and confidence not in CONFIDENCE_LEVELS:
            return f"Error: Invalid confidence: {confidence}"

        try:
            try:
                existing = kg.read_note(project_id, note)
            except Exception as exc:
                logger.warning("kb_update type pre-read failed: %s", exc)
                return (
                    "Error: could not verify the note type before update; "
                    "refusing the write. No changes were made."
                )
            if not isinstance(existing, dict):
                return f"Error: Note '{note}' not found in project."
            refusal = _marker_refusal(existing, content, append, note)
            if refusal:
                return refusal
            if (existing.get("type") or "") == "charter":
                denied = _charter_write_denied(context, project_id)
                if denied:
                    return denied

            prior_tags = existing.get("tags")
            machine_tags_authorized = False
            if _machine_tag_mutation_requested(
                prior_tags,
                add=add_tags,
                remove=remove_tags,
                replace=set_tags,
            ):
                authorization = _has_officer_authority(context, project_id)
                if not authorization:
                    return authorization.tool_message()
                machine_tags_authorized = True
            tag_change = _resolve_tags(
                prior_tags,
                add=add_tags,
                remove=remove_tags,
                replace=set_tags,
                officer_authority=machine_tags_authorized,
            )
            _prior_normalized = normalize_tags(prior_tags)
            if content is not None:
                new_content = content
            elif append is not None:
                new_content = (existing.get("content") or "") + "\n\n" + append
            else:
                new_content = existing.get("content") or ""
            new_type = existing.get("type") or "learning"
            new_status = status or existing.get("status") or "active"
            new_confidence = (
                confidence if confidence is not None else existing.get("confidence")
            )

            prior_row: Optional[Dict[str, Any]] = None
            new_priority: Optional[int] = None
            # The graph is the read source on this tier, but the row carries
            # the compare-and-swap token (blob_sha) — read it for every
            # rewrite, not only the ticket-priority case it used to serve.
            try:
                prior_row = _run_async(ks.get_note_by_slug(uuid.UUID(project_id), note))
            except Exception as exc:
                if new_type in _TICKET_TYPES:
                    return (
                        "Error: could not read the ticket's current READY/priority "
                        f"state before canonical update ({exc.__class__.__name__})."
                    )
                prior_row = None
            if new_type in _TICKET_TYPES:
                new_priority = (
                    priority
                    if priority is not None
                    else (prior_row or {}).get("priority", DEFAULT_PRIORITY_RANK)
                )

            relationships = list(existing.get("relationships") or [])
            relationships.extend(add_links or [])
            desired = {
                "id": note,
                "type": new_type,
                "title": existing.get("title") or note,
                "description": existing.get("description"),
                "content": new_content,
                "tags": tag_change.tags,
                "keywords": existing.get("keywords") or [],
                "confidence": new_confidence,
                "status": new_status,
                "superseded_by": existing.get("superseded_by"),
                "relationships": relationships,
            }
            if new_type in _TICKET_TYPES:
                desired["priority"] = new_priority
            _apply_ready_frontmatter(desired, tag_change.ready, prior_row)
            _carry_timestamps(desired, existing)
            materialization = _materialize_note(
                context,
                note,
                desired,
                # Carried across the rewrite: the markdown has nowhere to put
                # them, and None is the "leave the stored value alone"
                # sentinel the endpoint COALESCEs against. Passing [] here
                # would NOT blank them — `_post_vault_file`'s
                # `if retrieval_messages:` guard omits an empty list from the
                # payload exactly as it omits None. That guard is where the
                # protection lives; None is only the better-typed way to say
                # "no opinion" (see test_omits_retrieval_messages_entirely_...).
                existing.get("retrieval_messages"),
                expected_blob_sha=(prior_row or {}).get("blob_sha"),
                project_id=project_id,
            )
            if not _canonical_materialization_succeeded(materialization):
                return _canonical_materialization_error(
                    note,
                    materialization,
                    target=_binding_for_project_id(context, project_id),
                    alternatives=_native_bindings(context),
                    # As in `_update_existing_kgless`: the retarget mechanism on
                    # this surface is the qualified handle, not `kb=`.
                    surface="update",
                )

            # Neo4j is an optional derived graph, not the dispatch/search
            # projection repaired by the canonical Git reindexer. Preserve its
            # best-effort degradation contract without letting it falsify the
            # durable Git/pgvector convergence ledger.
            graph_error: Optional[Exception] = None
            try:
                updated = kg.update_note(
                    project_id=project_id,
                    note_id=note,
                    content=new_content,
                    append=None,
                    status=new_status,
                    confidence=new_confidence,
                    add_tags=[
                        tag for tag in tag_change.tags if tag not in _prior_normalized
                    ],
                    add_links=add_links,
                    remove_tags=[
                        tag for tag in _prior_normalized if tag not in tag_change.tags
                    ],
                )
                if not updated:
                    raise RuntimeError("graph note disappeared during projection")
            except Exception as exc:
                graph_error = exc
                logger.warning(
                    "optional knowledge graph projection failed for %s: %s",
                    note,
                    exc,
                )
            if graph_error is not None:
                return (
                    f"Error: '{note}' is canonical, but its optional graph "
                    f"projection failed ({graph_error}); the mutation was not "
                    f"reported as Updated. {_index_state_suffix(materialization)}"
                )

            changes = _describe_update(
                content=content,
                append=append,
                status=status,
                confidence=confidence,
                add_links=add_links,
                tag_change=tag_change,
            )

            return (
                f"Updated **{_updated_handle(project_id, note)}**: "
                f"{', '.join(changes)}"
                f"{_dropped_tag_notice(tag_change.dropped)}"
                f" {_index_state_suffix(materialization)}"
            )

        except Exception as e:
            logger.error(f"kb_update failed: {e}")
            return f"Error updating note: {e}"

    @tool
    def kb_write(
        title: str,
        type: NoteTypeValue,
        content: str,
        description: Optional[str] = None,
        tags: Optional[List[str]] = None,
        keywords: Optional[List[str]] = None,
        confidence: Optional[NoteConfidenceValue] = None,
        priority: PriorityValue = "normal",
        links: Optional[List[dict]] = None,
        retrieval_messages: Optional[List[str]] = None,
        kb: Optional[str] = None,
    ) -> str:
        """Create a new knowledge note in the project knowledge base.

        Write-through: creates the note in Neo4j (source of truth) AND
        materializes the note as an OKF markdown file at
        ``knowledge/<slug>.md`` in the project's knowledge repository —
        committed server-side, so it needs neither a workspace nor git.

        The note is committed to the knowledge repository and, in the normal
        case, indexed before this call returns — so kb_search and kb_read find
        it immediately. A large note, or one written while the knowledge base
        is rebuilding, reports ``indexed=deferred:<reason>`` and becomes
        searchable on the next sweep.

        Args:
            title: Note title (generates the slug ID, e.g. "chose-jwt-over-oauth")
            type: Note type — one of: goal, plan, decision, learning, code,
                source, question, state, retrospective, datasource, feature,
                issue, idea, charter, report. 'charter' is the project's
                pinned standing-orders note (one active per project; sessions
                only — worker jobs must file 'report' notes instead)
            content: Full markdown body of the note
            description: One-sentence summary for progressive-disclosure indexes.
                         Strongly recommended; derived from the content's first
                         sentence when omitted.
            tags: List of tag names (e.g. ["authentication", "security"])
            keywords: List of keyword strings for search
            confidence: Confidence level — high, medium, or low
            priority: Backlog rank for feature/issue/idea tickets: "high",
                "normal" (default) or "low". A LABEL only — it orders the
                backlog list the loop is shown and nothing refuses or
                reprioritizes work because of it.
            links: Relationships to other notes — list of {"target": "note-slug", "type": "RELATIONSHIP_TYPE"}.
                   Types: REFERENCES, DERIVED_FROM, SUPPORTS, CONTRADICTS, ANSWERS, DEPENDS_ON, SUPERSEDES, IMPLEMENTS
            retrieval_messages: Synthetic queries describing when this note should be retrieved
                               (e.g. ["What auth approach should I use?", "Why JWT over OAuth?"])
            kb: Target knowledge base alias (native only). Default: the
                session's primary project knowledge base.

        Returns:
            Confirmation with the note's slug ID, or error message
        """
        target, target_error = _resolve_write_target(context, kb)
        if target_error:
            return target_error
        assert target is not None  # _resolve_write_target never returns (None, None)
        project_id = str(target.kb_id)

        # Normalize machine tags (lowercase) and enforce the officer-only
        # namespace at the one write path that could otherwise create an
        # already-authorized ticket. `existing` is empty: a new note has no
        # prior tags to carry over.
        machine_tags_authorized = False
        if _machine_tag_mutation_requested(None, replace=tags or []):
            authorization = _has_officer_authority(context, project_id)
            if not authorization:
                return authorization.tool_message()
            machine_tags_authorized = True
        _new_tags = _resolve_tags(
            None,
            replace=tags or [],
            officer_authority=machine_tags_authorized,
        )
        tags = _new_tags.tags
        _tag_notice = _dropped_tag_notice(_new_tags.dropped)

        if type == "charter":
            denied = _charter_write_denied(context, project_id)
            if denied:
                return denied
            # One ACTIVE charter per project (centurion.md §5) — enforced here,
            # not by a DB constraint, so a duplicate charter file can never
            # wedge the reindexer. A same-title rewrite would otherwise fork a
            # content-hashed twin slug below, silently splitting the charter.
            try:
                _existing_charter = _run_async(
                    ks.get_charter_note(uuid.UUID(project_id))
                )
            except Exception as e:
                logger.warning(f"charter lookup failed (refusing write): {e}")
                return (
                    "Error: could not verify the project's existing charter — "
                    "refusing to write a possible duplicate. Retry, or edit "
                    "the known charter with kb_update."
                )
            if _existing_charter:
                return (
                    f"Error: this project already has an active charter "
                    f"('{_existing_charter['id']}'). One charter per project — "
                    f"edit it with kb_update('{_existing_charter['id']}', ...) "
                    f"(posture block only, unless you are the Legate)."
                )

        # Exact-duplicate short-circuit (Step 1 hardening, docs §11.1): a
        # same-slug write with byte-identical content is a pure no-op for every
        # writer — skip the gate and the create. This kills the run-8 twin-file
        # duplication for bare loop agents the verdict gate never reaches.
        # `read_note` returns the note dict (or None); a non-dict means no match.
        candidate_slug = slugify(title)
        if kg is None:
            existing = _run_async(
                ks.get_note_by_slug(uuid.UUID(project_id), candidate_slug)
            )
        else:
            existing = kg.read_note(project_id, candidate_slug)
        prior_content = existing.get("content") if isinstance(existing, dict) else ""
        if adds_redaction_marker(prior_content, content):
            return redaction_marker_refusal(f"note '{candidate_slug}'", "kb_write")
        if isinstance(existing, dict) and _content_hash(content) == _content_hash(
            existing.get("content", "")
        ):
            # "Nothing to write" is not "nothing to do". A note whose first
            # write deferred its index (oversized, KB lock held, embedding
            # provider down) sits here canonical-but-unsearchable, and
            # re-writing it is the *documented* repair: kb_materialize's
            # `_is_canonical` admits `skipped/unchanged` for exactly this
            # retry. Returning early unconditionally made that repair
            # unreachable from kb_write — the note stayed invisible until the
            # next sweep, and the result reported no index state at all, so
            # the author could not tell. Probe first, then either report the
            # truth or run the repair.
            try:
                already_indexed = bool(
                    _run_async(
                        ks.note_is_indexed(uuid.UUID(project_id), candidate_slug)
                    )
                )
            except Exception as e:
                # A probe must never turn a no-op into a write. Assume healthy
                # and keep the historical behaviour exactly.
                logger.warning(
                    "kb_write: index-state probe failed for %r (%s) — assuming "
                    "indexed and short-circuiting as before",
                    candidate_slug,
                    e,
                )
                already_indexed = True
            if already_indexed:
                return (
                    f"Note '{_qualified(target, candidate_slug)}' already exists "
                    f"with identical content — no change written. "
                    f"{_index_state_suffix({'indexed': True})}"
                )
            # Canonical but unsearchable. Route through the normal update path
            # (the same delegation the verdict gate's UPDATE action uses) so
            # the file is re-rendered and re-materialised by one code path
            # rather than a second, subtly different one here.
            #
            # The probe above ran against `project_id`, so the repair has to as
            # well: `_update_existing` otherwise resolves the DEFAULT writable
            # scope, and under a non-default `kb=` it would read and overwrite
            # a same-slug note in the wrong knowledge base and report success.
            return _update_existing(
                candidate_slug, content=content, project_id=project_id
            )

        # Ingestion verdict gate (slice 2 PR2) — only when the curator wired a
        # service. Adjudicate the candidate against its nearest active notes
        # before writing: DISCARD skips, UPDATE redirects the edit onto the
        # duplicate, SUPERSEDE writes then retires the stale note(s), ADD (or any
        # non-fatal gate error) falls through to a normal create.
        supersede_targets: List[Any] = []
        if verdict_service is not None and verdict_prompt:
            try:
                from agent.services.knowledge.ingestion import gate_candidate

                decision = _run_async(
                    gate_candidate(
                        verdict_service,
                        ks,
                        uuid.UUID(project_id),
                        content=content,
                        prompt=verdict_prompt,
                    )
                )
                action = decision.verdict.action
                if action == "DISCARD":
                    dup = (
                        f" (duplicate of {decision.targets[0].note_id})"
                        if decision.targets
                        else ""
                    )
                    return (
                        f"Skipped note '{title}' — verdict DISCARD{dup}: "
                        f"{decision.verdict.reason}"
                    )
                if action == "UPDATE" and decision.targets:
                    # The gate adjudicated against `project_id`'s notes, so the
                    # redirected edit lands there too — never in the default.
                    return _update_existing(
                        decision.targets[0].note_id,
                        content=content,
                        project_id=project_id,
                    )
                if action == "SUPERSEDE" and decision.targets:
                    supersede_targets = decision.targets
            except Exception as e:
                logger.warning(
                    f"knowledge verdict gate failed (non-fatal, writing ungated): {e}"
                )

        try:
            if type not in NOTE_TYPES:
                raise ValueError(
                    f"Invalid note_type: {type}. Must be one of {NOTE_TYPES}"
                )
            if confidence and confidence not in CONFIDENCE_LEVELS:
                raise ValueError(
                    f"Invalid confidence: {confidence}. "
                    f"Must be one of {CONFIDENCE_LEVELS}"
                )
            if not candidate_slug:
                slug = f"note-{_content_hash(content)[:8]}"
            elif isinstance(existing, dict):
                slug = f"{candidate_slug}-{_content_hash(content)[:6]}"
            else:
                slug = candidate_slug

            # What the author must quote to read or update this note again.
            # With several knowledge bases in scope a bare slug is ambiguous —
            # and, now that `kb=` can steer the write, it no longer even says
            # which one it landed in. `_qualified` degrades to the bare slug
            # for single-scope runtimes, so their output is unchanged.
            handle = _qualified(target, slug)

            rank = PRIORITY_RANKS[priority]
            # Both timestamps go in the FILE, not down a side channel. The
            # column has no DEFAULT and `note_fields` sources created_at only
            # from this frontmatter line, so a note that omits it stores NULL
            # forever — which sorts every agent-filed ticket to the bottom of
            # its backlog band (project_backlog.py's `created_at ASC NULLS
            # LAST`) and, via modified_at, distorts the search recency arm:
            # that arm is a bare `ORDER BY ki.modified_at DESC`
            # (vector_schema_current.sql) with no NULLS LAST, and Postgres
            # sorts NULLs FIRST under DESC — so an unstamped note does not
            # rank last there, it monopolises the top of the arm's window.
            # One stamp for both: a note is not modified after the write that
            # created it.
            stamped_at = datetime.now(timezone.utc).isoformat()
            new_note = {
                "id": slug,
                "type": type,
                "title": title,
                "description": description,
                "content": content,
                "tags": tags,
                "keywords": keywords,
                "confidence": confidence,
                "status": "active",
                "created": stamped_at,
                "modified": stamped_at,
                "relationships": links or [],
            }
            if type in _TICKET_TYPES:
                new_note["priority"] = rank
            _apply_ready_frontmatter(new_note, _new_tags.ready)
            materialization = _materialize_note(
                context, slug, new_note, retrieval_messages, project_id=project_id
            )
            if not _canonical_materialization_succeeded(materialization):
                return _canonical_materialization_error(
                    slug,
                    materialization,
                    target=target,
                    alternatives=_native_bindings(context),
                    surface="write",
                )

            graph_error: Optional[Exception] = None
            if kg is not None:
                try:
                    kg.create_note(
                        project_id=project_id,
                        title=title,
                        note_type=type,
                        content=content,
                        tags=tags,
                        keywords=keywords,
                        confidence=confidence,
                        job_id=context.job_id,
                        phase=context.config.get("current_phase"),
                        retrieval_messages=retrieval_messages,
                        links=links,
                        note_id=slug,
                    )
                except Exception as exc:
                    graph_error = exc
                    logger.warning(
                        "optional knowledge graph projection failed for %s: %s",
                        slug,
                        exc,
                    )
            if graph_error is not None:
                return (
                    f"Error: '{slug}' is canonical, but its optional graph "
                    f"projection failed ({graph_error}); the mutation was not "
                    f"reported as Created. {_index_state_suffix(materialization)}"
                )

            # Verdict SUPERSEDE: retire the stale note(s) the candidate replaces,
            # pointing them at the new note (status=superseded + SUPERSEDED_BY).
            if supersede_targets:
                retired = []
                retire_failures = []
                for t in supersede_targets:
                    try:
                        retire_result = _update_existing(
                            t.note_id,
                            status="superseded",
                            add_links=[{"target": slug, "type": "SUPERSEDED_BY"}],
                            # The stale note lives in the knowledge base the
                            # candidate was written to; retiring the default's
                            # same-slug note instead would leave a dangling
                            # SUPERSEDED_BY and still count as retired.
                            project_id=project_id,
                        )
                        if retire_result.startswith("Updated **"):
                            # Qualified: the retirement landed in `target`, and
                            # a bare slug here would not say which knowledge
                            # base the author should look in to undo it.
                            retired.append(_qualified(target, t.note_id))
                        else:
                            retire_failures.append(f"{t.note_id}: {retire_result}")
                    except Exception as e:
                        logger.warning(f"supersede retire failed for {t.note_id}: {e}")
                        retire_failures.append(f"{t.note_id}: {e}")
                if retire_failures:
                    failures = "; ".join(retire_failures)
                    return (
                        f"Error: '{slug}' is canonical, but its SUPERSEDE "
                        f"disposition did not converge: {failures} "
                        f"{_index_state_suffix(materialization)}"
                    )
                if retired:
                    return (
                        f"Created knowledge note: **{handle}** (type={type}) — "
                        f"superseded {', '.join(retired)}{_tag_notice} "
                        f"{_index_state_suffix(materialization)}"
                    )

            link_info = ""
            if links:
                link_info = f", {len(links)} link(s)"

            return (
                f"Created knowledge note: **{handle}** (type={type}{link_info})"
                f"{_tag_notice} {_index_state_suffix(materialization)}"
            )

        except ValueError as e:
            return f"Error: {e}"
        except Exception as e:
            logger.error(f"kb_write failed: {e}")
            return f"Error creating knowledge note: {e}"

    @tool
    def kb_update(
        note: str,
        content: Optional[str] = None,
        append: Optional[str] = None,
        status: Optional[NoteStatusValue] = None,
        confidence: Optional[NoteConfidenceValue] = None,
        priority: Optional[PriorityValue] = None,
        add_tags: Optional[List[str]] = None,
        add_links: Optional[List[dict]] = None,
        remove_tags: Optional[List[str]] = None,
        set_tags: Optional[List[str]] = None,
    ) -> str:
        """Update an existing knowledge note.

        Write-through: rewrites the note's OKF markdown file at
        ``knowledge/<slug>.md`` in the project's knowledge repository —
        committed server-side, so it needs neither a workspace nor git — and
        updates Neo4j where a graph is configured.

        The rewrite is committed and, in the normal case, re-indexed before
        this call returns — so kb_search and kb_read see the edit
        immediately. A large note, or one updated while the knowledge base is
        rebuilding, reports ``indexed=deferred:<reason>`` and picks the edit
        up on the next sweep.

        Args:
            note: Note slug ID (e.g. "chose-jwt-over-oauth"), or a qualified
                `alias:slug` handle to target another native knowledge base
            content: Replace the entire content (mutually exclusive with append)
            append: Append text to existing content (mutually exclusive with content)
            status: New status — active, resolved, superseded, or archived
            confidence: New confidence level — high, medium, or low
            priority: Change the backlog rank; omit to leave it unchanged.
            add_tags: Additional tags to add
            add_links: Additional relationships — list of {"target": "slug", "type": "RELATIONSHIP_TYPE"}
            remove_tags: Tags to remove. Use this to retract a tag rather than
                leaving it stuck — swapping a ticket's `category:` needs the old
                one removed, or it matches two work pools at once.
            set_tags: Replace the tag list entirely (wins over add_tags /
                remove_tags when given).

        Returns:
            Confirmation or error message
        """
        if set_tags is not None and (add_tags or remove_tags):
            return (
                "Error: set_tags replaces the whole tag list — do not combine "
                "it with add_tags/remove_tags in one call."
            )
        # A qualified handle picks the knowledge base this edit lands in.
        # `writable` marks the DEFAULT native, not the only writable one (B5):
        # gating on it here meant a session with two native knowledge bases
        # could only ever edit the first, and — worse — the alias was dropped
        # after validation, so the edit was derived against the default scope
        # anyway. Refuse externals (no write path at all), honour the rest.
        target: Optional[KnowledgeBinding] = None
        alias, note_slug = split_note_handle(note)
        if alias and _has_bound_scopes:
            binding = _resolve_binding(context, alias)
            if binding is None:
                return (
                    f"Error: Knowledge base '{alias}' is not selected. Available: "
                    f"{_binding_choices(_read_bindings(context))}."
                )
            if not binding.is_native:
                return (
                    f"Error: Knowledge base '{binding.alias}' is read-only "
                    "(external). External knowledge bases cannot be updated."
                )
            target = binding
            note = note_slug
        return _update_existing(
            note,
            content=content,
            append=append,
            status=status,
            confidence=confidence,
            # The resolved target, not the default writable native: every
            # scope derivation inside `_update_existing` keys on this, and
            # dropping it here would read and rewrite a same-slug note in the
            # wrong knowledge base while reporting success.
            project_id=str(target.kb_id) if target is not None else None,
            # None means "leave unchanged" — converted to a rank only when the
            # caller actually asked for a change (Global constraint: never
            # silently reset an existing ticket's priority to normal).
            priority=PRIORITY_RANKS[priority] if priority is not None else None,
            add_tags=add_tags,
            add_links=add_links,
            remove_tags=remove_tags,
            set_tags=set_tags,
        )

    @tool
    def kb_delete(note: str, reason: str) -> str:
        """Retire a knowledge note: archive it with a reason.

        This is NOT a hard delete. The note's status becomes ``archived``: it
        drops out of kb_search and prompt injection immediately, stays
        readable by slug with kb_read, and is undone with
        ``kb_update(note, status="active")``. The file stays in the knowledge
        repository; physical removal happens later in a separate grace-period
        lane, and git history keeps every byte either way. The reason is
        written into the note (and its revision history), so a reviewer can
        see who retired what and why.

        Refused, naming the rule, when the note is: the project charter; a
        backlog ticket (close those with kb_update status resolved/archived);
        tagged ``pinned``; tagged ``ready`` / ``parallel-safe`` or otherwise
        authorised for dispatch; linked from an ACTIVE decision, goal, plan,
        charter or ticket (it is that note's evidence); or younger than 24 h.
        Retiring an already-archived note is a no-op, not an error.

        Args:
            note: Note slug ID (e.g. "iter-12-state-snapshot"), or a qualified
                `alias:slug` handle to target another native knowledge base
            reason: One line: why this note no longer earns its place
                (superseded by X, duplicate of Y, snapshot of a moved-on
                state, ...). Required.

        Returns:
            Confirmation with the undo instruction, or a refusal naming the rule.
        """
        reason_text = " ".join(str(reason or "").split())
        if len(reason_text) < 8:
            return (
                "Error: give a reason of at least a few words — it is journaled "
                "in the note so a reviewer can see why it was retired."
            )
        # As in kb_update: a qualified handle names the knowledge base being
        # retired from, and every derivation below (the row pre-read, the
        # inbound-link guard, the rewrite) has to use it rather than the
        # session default (B5).
        target: Optional[KnowledgeBinding] = None
        alias, note_slug = split_note_handle(note)
        if alias and _has_bound_scopes:
            binding = _resolve_binding(context, alias)
            if binding is None:
                return (
                    f"Error: Knowledge base '{alias}' is not selected. Available: "
                    f"{_binding_choices(_read_bindings(context))}."
                )
            if not binding.is_native:
                return (
                    f"Error: Knowledge base '{binding.alias}' is read-only "
                    "(external). External knowledge bases cannot be retired from."
                )
            target = binding
            note = note_slug
        project_id = (
            str(target.kb_id) if target is not None else _get_project_id(context)
        )
        if not project_id:
            return _write_scope_error(context)
        try:
            project_uuid = uuid.UUID(project_id)
            existing = _run_async(ks.get_note_by_slug(project_uuid, note))
            if not existing:
                return f"Error: Note '{note}' not found in project."
            if (existing.get("status") or "") == "archived":
                return f"'{note}' is already archived — nothing to do."
            try:
                inbound_durable = _run_async(
                    ks.get_inbound_links(
                        project_uuid,
                        note,
                        active_only=True,
                        note_types=list(_RETIRE_ROOT_TYPES),
                    )
                )
            except Exception as exc:  # noqa: BLE001 — fail closed on the guard
                return (
                    f"Error: could not check what links to '{note}' "
                    f"({exc.__class__.__name__}); refusing to retire blind."
                )
            denied = _retire_denied(existing, inbound_durable)
            if denied:
                return (
                    f"Refused: '{note}' was not retired — {denied}. "
                    "If it is genuinely stale, say so in a note the officer "
                    "reads, or supersede it with a newer note instead."
                )
        except Exception as e:  # noqa: BLE001
            logger.error(f"kb_delete failed: {e}")
            return f"Error retiring note: {e}"

        provenance = _note_provenance(context)
        who = provenance.get("author") or "agent"
        job = str(provenance.get("job") or "")
        stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        footer = (
            f"> **Retired** {stamp} by {who}"
            f"{f' (job {job[:8]})' if job else ''}: {reason_text}"
        )
        result = _update_existing(
            note,
            content=None,
            append=footer,
            status="archived",
            confidence=None,
            priority=None,
            add_tags=None,
            add_links=None,
            remove_tags=None,
            set_tags=None,
            project_id=project_id,
        )
        if result.startswith("Error"):
            return result
        suffix = result[result.rfind("[") :] if "[" in result else ""
        # The undo instruction is a handle the author pastes back into
        # kb_update — bare, it would reopen the DEFAULT knowledge base's
        # same-slug note rather than the one just retired.
        handle = _updated_handle(project_id, note)
        return (
            f"Retired **{handle}** (status=archived): {reason_text}. Hidden from "
            "kb_search and injection; still readable with kb_read; undo with "
            f'kb_update(note="{handle}", status="active"). {suffix}'.rstrip()
        )

    # =========================================================================
    # Read Tools
    # =========================================================================

    @tool
    def kb_read(note: str, kb: Optional[str] = None) -> str:
        """Read a full knowledge note with metadata and relationships.

        Args:
            note: Note slug or qualified ``alias:slug`` handle
            kb: Optional selected knowledge-base alias or UUID

        Returns:
            Full note content with metadata, tags, and relationships
        """
        bindings, note_slug, _scoped, error = _resolve_note_scope(context, note, kb)
        if error:
            return error

        try:
            matches = _matching_notes(bindings, note_slug)
            if not matches:
                notice = _index_readiness_notice(bindings)
                if notice:
                    return f"Note '{note}' not found.\n\n{notice}"
                return f"Note '{note}' not found."
            if len(matches) > 1:
                return _ambiguous_note(note_slug, matches)
            binding, data = matches[0]

            # Format output
            lines = [
                f"# {data.get('title', note_slug)}",
                "",
                f"**ID:** {data.get('id', note_slug)}",
                f"**Type:** {data.get('type', 'unknown')}",
                f"**Status:** {data.get('status', 'unknown')}",
            ]
            if _has_bound_scopes:
                lines.extend(
                    [
                        f"**Knowledge Base:** {binding.name} (`{binding.alias}`)",
                        f"**Handle:** `{binding.handle(note_slug)}`",
                    ]
                )
                snapshot_marker = _external_snapshot_marker([binding])
                if snapshot_marker:
                    lines.append(snapshot_marker)

            if data.get("confidence"):
                lines.append(f"**Confidence:** {data['confidence']}")
            if data.get("tags"):
                lines.append(f"**Tags:** {', '.join(data['tags'])}")
            if data.get("keywords"):
                lines.append(f"**Keywords:** {', '.join(data['keywords'])}")
            if data.get("job_id"):
                lines.append(f"**Job:** {data['job_id']}")
            if data.get("phase") is not None:
                lines.append(f"**Phase:** {data['phase']}")
            if data.get("created"):
                lines.append(f"**Created:** {data['created']}")
            if data.get("modified"):
                lines.append(f"**Modified:** {data['modified']}")

            lines.extend(["", "---", "", data.get("content", "(no content)")])

            # Relationships
            rels = data.get("relationships", [])
            if rels:
                lines.extend(["", "## Outgoing Relationships"])
                for r in rels:
                    target = _qualified(binding, r["target"])
                    lines.append(
                        f"- **{r['type']}** → [[{target}]] "
                        f"({r.get('target_title', '')})"
                    )

            incoming = data.get("incoming_relationships", [])
            if incoming:
                lines.extend(["", "## Incoming Relationships"])
                for r in incoming:
                    source = _qualified(binding, r["source"])
                    lines.append(
                        f"- [[{source}]] ({r.get('source_title', '')}) "
                        f"**{r['type']}** → this"
                    )

            return "\n".join(lines)

        except Exception as e:
            logger.error(f"kb_read failed: {e}")
            return f"Error reading note: {e}"

    @tool
    def kb_list(
        type: Optional[NoteTypeValue] = None,
        tag: Optional[str] = None,
        status: Optional[NoteStatusValue] = None,
        job_id: Optional[str] = None,
        kb: Optional[str] = None,
    ) -> str:
        """List knowledge notes with optional filters.

        Args:
            type: Filter by note type (goal, plan, decision, learning, code, source, question, state, retrospective)
            tag: Filter by tag name
            status: Filter by status (active, resolved, superseded, archived). Default: all
            job_id: Filter by the job that LAST WROTE the note (UUID) — not
                necessarily the one that created it. ``knowledge_index.job_id``
                is stamped by every canonical write, so a note another job
                later edited answers to the editor's id, not the author's.
            kb: Optional selected knowledge-base alias or UUID

        Returns:
            Formatted list of matching notes
        """
        bindings, error = _select_bindings(context, kb)
        if error:
            return error

        try:
            notes: List[tuple[KnowledgeBinding, Dict[str, Any]]] = []
            for binding in bindings:
                if binding.is_native and kg is not None:
                    found = kg.list_notes(
                        project_id=str(binding.kb_id),
                        note_type=type,
                        tag=tag,
                        status=status,
                        job_id=job_id,
                    )
                    # Same H1 fallback as _read_from_binding: a native KB
                    # whose notes were imported by the reindex sweep lives
                    # only in Postgres, so an empty graph result isn't
                    # necessarily an empty KB. No merge of both stores'
                    # results here — a KB lives in one world today; a future
                    # merge would need de-dup by `id`.
                    if not found:
                        found = _run_async(
                            ks.list_notes(
                                kb_id=binding.kb_id,
                                note_type=type,
                                tag=tag,
                                status=status,
                                job_id=job_id,
                            )
                        )
                else:
                    found = _run_async(
                        ks.list_notes(
                            kb_id=binding.kb_id,
                            note_type=type,
                            tag=tag,
                            status=status,
                            job_id=job_id,
                        )
                    )
                notes.extend((binding, item) for item in found)

            if not notes:
                filters = []
                if type:
                    filters.append(f"type={type}")
                if tag:
                    filters.append(f"tag={tag}")
                if status:
                    filters.append(f"status={status}")
                filter_str = f" (filters: {', '.join(filters)})" if filters else ""
                base = f"No knowledge notes found{filter_str}."
                notice = _index_readiness_notice(bindings)
                return f"{base}\n\n{notice}" if notice else base

            lines: List[str] = []
            if type is None and tag is None and status is None and job_id is None:
                for binding in bindings:
                    try:
                        vocab = _run_async(ks.tag_vocabulary(binding.kb_id))
                        if isinstance(vocab, list) and vocab:
                            prefix = f"[{binding.alias}] " if _has_bound_scopes else ""
                            lines.append(
                                prefix
                                + "**Tags:** "
                                + ", ".join(f"{t} ({n})" for t, n in vocab)
                            )
                    except Exception as e:
                        logger.debug(
                            "kb_list tag_vocabulary skipped for %s: %s",
                            binding.alias,
                            e,
                        )
                        continue
                if lines:
                    lines.append("")

            lines.append(f"**Knowledge Notes** ({len(notes)} results):")
            lines.append("")

            for binding, n in notes:
                status_icon = "●" if n.get("status") == "active" else "○"
                confidence = f" [{n['confidence']}]" if n.get("confidence") else ""
                # Priority is a backlog-ticket concept only (feature/issue/
                # idea) — every other note type's line is byte-identical to
                # before this existed (Global constraint).
                priority_tag = ""
                if n.get("type") in _TICKET_TYPES:
                    word = PRIORITY_WORDS.get(
                        n.get("priority", DEFAULT_PRIORITY_RANK), "normal"
                    )
                    priority_tag = f" [priority: {word}]"
                note_id = n.get("id", "?")
                handle = _qualified(binding, note_id)
                source = f"[{binding.alias}] " if _has_bound_scopes else ""
                lines.append(
                    f"{status_icon} {source}**{handle}** — "
                    f"{n.get('title', '(untitled)')} "
                    f"({n.get('type', '?')}{confidence}{priority_tag})"
                )

            snapshot_marker = _external_snapshot_marker(bindings)
            if snapshot_marker:
                lines.extend(["", snapshot_marker])

            return "\n".join(lines)

        except Exception as e:
            logger.error(f"kb_list failed: {e}")
            return f"Error listing notes: {e}"

    @tool
    def kb_search(
        query: Optional[str] = None,
        max_results: int = 10,
        kb: Optional[str] = None,
        exact: Optional[Union[str, List[str]]] = None,
        tags: Optional[Union[str, List[str]]] = None,
    ) -> str:
        """Search the knowledge base from several angles in one call.

        Angles (give at least one; they compose):
          query — natural language, for MEANING (semantic + full-text + recency).
          exact — an identifier, slug, commit sha, error string or exact phrase
                  (case-insensitive substring; a list matches any). Use this
                  whenever you know the literal text — stemmed search cannot
                  match `sales_page_2026_09` or `KB_REINDEX_SWEEP_SECONDS`.
          tags  — boost notes carrying these tags (a nudge, not a filter; use
                  kb_list(tag=) to filter). Pass a list, not a comma-joined
                  string (that is one tag).
        Each hit shows which angles matched it, e.g. ⟨dense+exact⟩. For every
        occurrence with surrounding lines use `kb_grep`.
        If embeddings are unavailable, query falls back to full-text and
        literal matching over indexed content, with a notice. Notes that have
        not reached the index cannot be searched yet.
        """
        bindings, error = _select_bindings(context, kb)
        if error:
            return error
        kb_ids = [binding.kb_id for binding in bindings]
        binding_by_id = {str(binding.kb_id): binding for binding in bindings}

        def _as_list(value):
            if value is None:
                return []
            return [value] if isinstance(value, str) else list(value)

        exact_terms = [
            item.strip()
            for item in _as_list(exact)
            if isinstance(item, str) and item.strip()
        ]
        # Tags are canonicalised through the same case-fold as every other
        # tag-consuming path in this file (kb_write, kb_update, the reindexer)
        # so `tags=["Sales"]` matches what was actually stored as "sales".
        # `exact` is left alone: it's a literal-text search, not a tag lookup.
        tag_terms = normalize_tags(_as_list(tags))

        if not ((query or "").strip() or exact_terms or tag_terms):
            return "Error: give at least one of query, exact, or tags."

        extra_angles = bool(exact_terms or tag_terms)

        angle_parts = []
        if query:
            angle_parts.append(f"query '{query}'")
        if exact_terms:
            angle_parts.append("exact " + ", ".join(f"'{t}'" for t in exact_terms))
        if tag_terms:
            angle_parts.append("tags " + ", ".join(tag_terms))
        angle_desc = ", ".join(angle_parts)

        # Filter to the live pipeline stamp so mixed-model/chunker vectors can't
        # drift into the result set. Resolved from the same EmbeddingService that
        # embeds the query, so it matches what the reindexer stamped; fall back to
        # no filter if the service can't report a model (never over-filter blind).
        try:
            current_version = embedding_version_for_service(ks.embedding_service)
        except Exception:
            current_version = None

        try:
            search_kwargs = {
                "kb_ids": kb_ids,
                "query": (query or "") if extra_angles else query,
                "embedding_version": current_version,
                "match_count": max_results,
            }
            if exact_terms:
                search_kwargs["exact"] = exact_terms
            if tag_terms:
                search_kwargs["tags"] = tag_terms

            results = _run_async(ks.search_chunks(**search_kwargs))
            search_notice = getattr(results, "notice", "")
            if not isinstance(search_notice, str):
                search_notice = ""

            if not results:
                base = (
                    f"No knowledge notes match ({angle_desc})."
                    if extra_angles
                    else f"No knowledge notes match '{query}'."
                )
                notice = _index_readiness_notice(bindings)
                return "\n\n".join(
                    part for part in (base, search_notice, notice) if part
                )

            header = (
                f"**Search Results** ({len(results)} matches — {angle_desc})"
                if extra_angles
                else f"**Search Results** ({len(results)} matches for '{query}')"
            )
            # Native single-KB compatibility header. External bindings use the
            # richer convergence marker below so a partial mixed cache is never
            # mislabeled as an immutable snapshot at the last clean commit.
            if len(kb_ids) == 1 and bindings[0].is_native:
                try:
                    wm = _run_async(ks.get_watermark(kb_ids[0]))
                    commit = getattr(wm, "indexed_commit", None)
                    if commit:
                        header += f" — index @ {str(commit)[:8]}"
                except Exception as e:
                    logger.debug(f"kb_search watermark lookup skipped: {e}")

            lines = [f"{header}:", ""]
            if search_notice:
                lines.extend([search_notice, ""])

            for i, note in enumerate(results, 1):
                meta_parts = [note.note_type]
                if note.confidence:
                    meta_parts.append(note.confidence)
                meta = ", ".join(meta_parts)

                # Truncate content for display
                preview = note.content[:200].replace("\n", " ")
                if len(note.content) > 200:
                    preview += "..."

                result_binding = binding_by_id.get(str(getattr(note, "kb_id", "")))
                if result_binding is None and len(bindings) == 1:
                    result_binding = bindings[0]
                note_handle = note.note_id
                source = ""
                if _has_bound_scopes and result_binding is not None:
                    note_handle = result_binding.handle(note.note_id)
                    source = f"[{result_binding.alias}] "
                arms = getattr(note, "matched_arms", None)
                arm_suffix = (
                    f" ⟨{'+'.join(arms)}⟩" if isinstance(arms, list) and arms else ""
                )
                lines.append(
                    f"**[{i}]** {source}**{note_handle}** — {note.title} "
                    f"({meta}){arm_suffix}"
                )
                lines.append(f"  {preview}")
                lines.append("")

            if extra_angles:
                if exact_terms:
                    # Per-term, not per-arm: matched_arms only says a hit
                    # matched *some* exact term, not which one, so an
                    # aggregate count would misreport a term with zero hits
                    # as having the same count as one that matched everything.
                    # Plain case-insensitive substring over title+content —
                    # the term is not escaped here (the store already does
                    # that for the ILIKE it ran).
                    for term in exact_terms:
                        term_lower = term.lower()
                        count = sum(
                            1
                            for note in results
                            if term_lower in f"{note.title}\n{note.content}".lower()
                        )
                        lines.append(f"exact '{term}': {count} shown")
                if tag_terms:
                    tag_hits = sum(
                        1
                        for note in results
                        if isinstance(getattr(note, "matched_arms", None), list)
                        and "tag" in note.matched_arms
                    )
                    lines.append(f"tags {', '.join(tag_terms)}: {tag_hits} boosted")

            if _has_bound_scopes and len(kb_ids) > 1:
                snapshots = []
                for binding in (item for item in bindings if item.is_native):
                    try:
                        watermark = _run_async(ks.get_watermark(binding.kb_id))
                        commit = getattr(watermark, "indexed_commit", None)
                    except Exception as e:
                        logger.debug(
                            "kb_search watermark lookup skipped for %s: %s",
                            binding.alias,
                            e,
                        )
                        commit = None
                    snapshots.append(
                        f"[{binding.alias}] @ {str(commit)[:8]}"
                        if commit
                        else f"[{binding.alias}] unavailable"
                    )
                if snapshots:
                    lines.append(f"**Native index snapshots:** {', '.join(snapshots)}")

            snapshot_marker = _external_snapshot_marker(bindings)
            if snapshot_marker:
                lines.append(snapshot_marker)

            return "\n".join(lines)

        except Exception as e:
            logger.error(f"kb_search failed: {e}")
            return (
                f"Error searching knowledge base: {e}\n"
                "Try kb_grep(pattern=...) for literal lines or "
                "kb_search(exact=...) for literal note matches."
            )

    @tool
    def kb_grep(
        pattern: str,
        kb: Optional[str] = None,
        regex: bool = False,
        max_matches: int = 50,
    ) -> str:
        """List every line in the knowledge base matching a pattern, with context.

        The enumeration counterpart to kb_search(exact=): no ranking, every
        occurrence, one line of context each side. Case-insensitive substring by
        default; regex=True for a POSIX regex; `^`/`$` anchor per line. Use it to
        find all mentions of an identifier, error string or slug, or to confirm
        something is absent. Matches note bodies; use `kb_search(exact=)` to match
        titles too. Capped at max_matches lines; the tail says how many notes were
        cut.
        """
        bindings, error = _select_bindings(context, kb)
        if error:
            return error
        try:
            matches, total = _run_async(
                ks.grep_notes(
                    [b.kb_id for b in bindings],
                    pattern,
                    regex=regex,
                    max_matches=max(1, min(int(max_matches), 500)),
                    # Body-only, deliberately (final review, Important 1). The
                    # store's title branch makes a title-only hit a *candidate*:
                    # it counts toward `total` and occupies a LIMIT slot, but
                    # `grep_notes` extracts lines from `content` alone, so it
                    # renders nothing. Left on, kb_grep printed "No lines match"
                    # for content that IS present and offered "raise
                    # max_matches" for notes that can never render a line.
                    # Titles are `kb_search(exact=)`'s job; grep is a body tool.
                    include_titles=False,
                )
            )
        except ValueError as e:
            return f"Error: {e}"
        except Exception as e:
            logger.error(f"kb_grep failed: {e}")
            return f"Error searching knowledge base: {e}"
        if not matches:
            notice = _index_readiness_notice(bindings)
            base = f"No lines match '{pattern}'."
            return f"{base}\n\n{notice}" if notice else base

        by_id = {str(b.kb_id): b for b in bindings}
        lines: List[str] = [f"**Grep** '{pattern}' — {total} matching note(s):", ""]
        current = None
        shown_notes = set()
        for m in matches:
            key = (str(m.kb_id), m.note_id)
            if key != current:
                binding = by_id.get(str(m.kb_id)) or bindings[0]
                handle = _qualified(binding, m.note_id)
                if current is not None:
                    lines.append("")
                lines.append(f"**{handle}** — {m.title}")
                current = key
                shown_notes.add(key)
            for b in m.before:
                lines.append(f"      {b}")
            lines.append(f"  L{m.line_no}: {m.line}")
            for a in m.after:
                lines.append(f"      {a}")
        rest = total - len(shown_notes)
        if rest > 0:
            lines.append("")
            lines.append(
                f"… {rest} more matching note(s); narrow the pattern or raise max_matches."
            )
        return "\n".join(lines)

    # =========================================================================
    # Graph Query Tools
    # =========================================================================

    @tool
    def kb_related(note: str, max_hops: int = 2, kb: Optional[str] = None) -> str:
        """Find notes related to a given note via graph traversal.

        Traverses all relationship types up to max_hops edges away.

        Args:
            note: Note slug or qualified ``alias:slug`` handle
            max_hops: Maximum relationship hops (1-3, default 2)
            kb: Optional selected knowledge-base alias or UUID

        Returns:
            List of related notes with relationship types and distance
        """
        bindings, note_slug, _scoped, error = _resolve_note_scope(context, note, kb)
        if error:
            return error

        try:
            if _has_bound_scopes:
                matches = _matching_notes(bindings, note_slug)
                if not matches:
                    return f"Note '{note}' not found."
                if len(matches) > 1:
                    return _ambiguous_note(note_slug, matches)
                target_bindings = [matches[0][0]]
            else:
                # Preserve the legacy cross-project aggregation contract.
                target_bindings = bindings

            results: List[tuple[KnowledgeBinding, Dict[str, Any]]] = []
            for binding in target_bindings:
                if binding.is_native and kg is not None:
                    found = kg.get_related(
                        project_id=str(binding.kb_id),
                        note_id=note_slug,
                        max_hops=max_hops,
                    )
                else:
                    # Datasource KBs and graph-less native KBs use the indexed
                    # body-link table. It deliberately provides one hop only.
                    found = _run_async(
                        ks.get_related_notes(kb_id=binding.kb_id, note_id=note_slug)
                    )
                results.extend((binding, item) for item in found)

            if not results:
                return f"No related notes found for '{note}'."

            display_note = _qualified(target_bindings[0], note_slug)
            lines = [
                f"**Related to '{display_note}'** ({len(results)} notes):",
                "",
            ]
            for binding, r in results:
                distance = r.get("distance", "?")
                rel_types = r.get("rel_types", [])
                rel_str = " → ".join(rel_types) if rel_types else "?"
                status = f" [{r['status']}]" if r.get("status") != "active" else ""
                related_id = _qualified(binding, r.get("id", "?"))
                source = f"[{binding.alias}] " if _has_bound_scopes else ""
                lines.append(
                    f"  ({distance} hop{'s' if distance != 1 else ''}) "
                    f"{source}**{related_id}** — {r.get('title', '?')} "
                    f"({r.get('type', '?')}{status}) via {rel_str}"
                )

            snapshot_marker = _external_snapshot_marker(target_bindings)
            if snapshot_marker:
                lines.extend(["", snapshot_marker])

            return "\n".join(lines)

        except Exception as e:
            logger.error(f"kb_related failed: {e}")
            return f"Error finding related notes: {e}"

    @tool
    def kb_contradictions() -> str:
        """Find all active contradiction pairs in the knowledge base.

        Returns notes connected by CONTRADICTS relationships where both
        notes are still active. Use this to identify conflicting knowledge
        that needs resolution.

        Returns:
            List of contradiction pairs
        """
        if kg is None:
            return f"Contradiction detection {_GRAPH_TIER_MSG}"

        bindings = _native_bindings(context)
        if not bindings:
            return _native_scope_error(context)

        try:
            results: List[tuple[KnowledgeBinding, Dict[str, Any]]] = []
            for binding in bindings:
                found = kg.get_contradictions(str(binding.kb_id))
                results.extend((binding, item) for item in found)

            if not results:
                return "No active contradictions found in the knowledge base."

            lines = [f"**Active Contradictions** ({len(results)}):", ""]
            for binding, r in results:
                note_a = _qualified(binding, r.get("note_a", "?"))
                note_b = _qualified(binding, r.get("note_b", "?"))
                lines.append(
                    f"  **{note_a}** ({r.get('title_a', '?')}) "
                    f"⟷ CONTRADICTS ⟷ "
                    f"**{note_b}** ({r.get('title_b', '?')})"
                )

            return "\n".join(lines)

        except Exception as e:
            logger.error(f"kb_contradictions failed: {e}")
            return f"Error finding contradictions: {e}"

    @tool
    def kb_provenance(note: str) -> str:
        """Trace the derivation chain of a knowledge note.

        Follows DERIVED_FROM relationships backwards to find the original
        sources that a note was derived from.

        Args:
            note: Note slug ID to trace

        Returns:
            Ordered provenance chain from the note back to its sources
        """
        if kg is None:
            return f"Provenance tracing {_GRAPH_TIER_MSG}"

        alias, note_slug = split_note_handle(note)
        if alias and _has_bound_scopes:
            selected = _resolve_binding(context, alias)
            if selected is None:
                return (
                    f"Error: Knowledge base '{alias}' is not selected. Available: "
                    f"{_binding_choices(_read_bindings(context))}."
                )
            if not selected.is_native:
                return f"Provenance tracing {_GRAPH_TIER_MSG}"
            bindings = [selected]
        else:
            bindings = _native_bindings(context)
        if not bindings:
            return _native_scope_error(context)

        try:
            results: List[tuple[KnowledgeBinding, Dict[str, Any]]] = []
            for binding in bindings:
                found = kg.get_provenance(str(binding.kb_id), note_slug)
                results.extend((binding, item) for item in found)

            if not results:
                return f"No provenance chain found for '{note}'."

            lines = [f"**Provenance of '{note}'**:", ""]
            for binding, r in results:
                depth = r.get("depth", "?")
                result_id = _qualified(binding, r.get("id", "?"))
                lines.append(
                    f"  {'  ' * (depth - 1)}↑ **{result_id}** — "
                    f"{r.get('title', '?')} ({r.get('type', '?')})"
                )

            return "\n".join(lines)

        except Exception as e:
            logger.error(f"kb_provenance failed: {e}")
            return f"Error tracing provenance: {e}"

    @tool
    def kb_unanswered() -> str:
        """List open questions that have no answers in the knowledge base.

        Returns question notes (type="question", status="active") that
        have no ANSWERS relationship pointing to or from them.

        Returns:
            List of unanswered questions
        """
        if kg is None:
            return f"Unanswered-question tracking {_GRAPH_TIER_MSG}"

        bindings = _native_bindings(context)
        if not bindings:
            return _native_scope_error(context)

        try:
            results: List[tuple[KnowledgeBinding, Dict[str, Any]]] = []
            for binding in bindings:
                found = kg.get_unanswered(str(binding.kb_id))
                results.extend((binding, item) for item in found)

            if not results:
                return "No unanswered questions in the knowledge base."

            lines = [f"**Unanswered Questions** ({len(results)}):", ""]
            for binding, r in results:
                content = r.get("content", "")
                preview = content[:150].replace("\n", " ")
                if len(content) > 150:
                    preview += "..."
                result_id = _qualified(binding, r.get("id", "?"))
                lines.append(f"  ❓ **{result_id}** — {r.get('title', preview)}")

            return "\n".join(lines)

        except Exception as e:
            logger.error(f"kb_unanswered failed: {e}")
            return f"Error finding unanswered questions: {e}"

    # =========================================================================
    # Export
    # =========================================================================

    @tool
    def kb_export(path: str) -> str:
        """Export the project knowledge base as OKF/markdown files.

        Dumps all notes from Neo4j as .md files with YAML frontmatter and
        standard markdown links for relationships (OKF convention). Requires a
        workspace backend; one-way export for human browsing or migration.

        Args:
            path: Directory to write the export files into. Must be a
                directory outside `knowledge/` — never a note filename and
                never inside the vault (the vault's own files are already the
                canonical export).

        Returns:
            Summary of exported files
        """
        # Validate before any Neo4j/workspace work: the failure mode is a
        # 1000-file directory that has to be deleted by hand.
        dest_error = _export_dir_error(path)
        if dest_error:
            return dest_error

        if kg is None:
            # kb_export is the one-time Neo4j → OKF migration dump. Without Neo4j
            # the vault's `knowledge/*.md` files ARE the canonical OKF export
            # already, so there is nothing to migrate out of the graph.
            return (
                "Nothing to export: this knowledge base has no Graph tier (Neo4j). "
                "The `knowledge/*.md` files in the project's knowledge repository "
                "are already the canonical OKF export."
            )

        bindings = _native_bindings(context)
        if not bindings:
            return _native_scope_error(context)

        workspace = context.workspace_manager if context.has_workspace() else None

        try:
            notes = []
            for binding in bindings:
                notes.extend(kg.get_all_notes_for_export(str(binding.kb_id)))
            if not notes:
                return "Knowledge base is empty — nothing to export."

            # A workspace backend is required: never write to the local agent
            # host (ephemeral, invisible to the workspace pod's clone). Uses the
            # shared OKF serializer (markdown links, provenance) — same output
            # as the kb_write/kb_update materialisation.
            if workspace is None:
                return (
                    "Error: kb_export requires a workspace backend; refusing to "
                    "write to the local agent host (invisible to the workspace)."
                )

            export_rel = path.rstrip("/")
            workspace.create_directory(export_rel)

            exported = 0
            for note in notes:
                note_id = note.get("id", "unknown")
                workspace.write_file(
                    f"{export_rel}/{note_id}.md", _render_note_md(note)
                )
                exported += 1

            return (
                f"Exported {exported} note(s) to `{path}/`.\n"
                "Open in any OKF-compatible Markdown viewer."
            )

        except Exception as e:
            logger.error(f"kb_export failed: {e}")
            return f"Error exporting knowledge base: {e}"

    # =========================================================================
    # Maintenance / gardener tools (slice 2)
    # =========================================================================

    def _vault_binding() -> Optional[KnowledgeBinding]:
        """The native KB the gardener tools operate on — the project vault.

        Exactly one: `knowledge/` was one directory in one workspace, and
        linting several projects' KBs as a single vault would invent
        cross-project ``duplicate-id`` findings. Prefers the writable native
        binding (the primary project, the same one ``_get_project_id`` picks)
        and falls back to the first native scope for read-only multi-project
        contexts.
        """
        natives = _native_bindings(context)
        for binding in natives:
            if binding.writable:
                return binding
        return natives[0] if natives else None

    def _vault_rows(
        binding: KnowledgeBinding,
    ) -> tuple[List[Dict[str, Any]], Optional[str]]:
        """Every ``knowledge_index`` row of a KB, or an operator-readable error."""
        try:
            rows = _run_async(ks.list_notes_full(binding.kb_id, limit=_VAULT_SCAN_CAP))
        except Exception as e:
            logger.error(f"knowledge index read failed for {binding.alias}: {e}")
            return [], f"Error reading the knowledge index: {e}"
        return list(rows or []), None

    def _scan_truncated(rows: List[Dict[str, Any]]) -> bool:
        return len(rows) >= _VAULT_SCAN_CAP

    @tool
    def kb_lint(path: Optional[str] = None, check_urls: bool = False) -> str:
        """Lint the knowledge base for structural, id and link issues.

        Reads every note from the knowledge index — not from the workspace, so
        it works on lite tiers and persistent sessions too — and checks
        required keys (id/type/description), id format/uniqueness, dead and
        broken-supersede links, orphans, oversized notes, slug-forked twins,
        notes no file backs yet, and embedding-near-duplicates. Read-only —
        returns a report; it never edits notes.

        Args:
            path: Omit it to lint the project knowledge base (the normal case).
                Pass a directory to lint some *other* markdown vault in the
                workspace instead (e.g. `docs`, a repository datasource
                checkout); that mode needs a workspace.
            check_urls: Also probe external http(s) links and flag clearly
                dead ones (404/410, unreachable host). Off by default —
                it is slow (network) and capped per run.

        Returns:
            A markdown lint report (errors then warnings), or a status message.
        """
        notes: List[Dict[str, str]] = []
        extra_findings: List[Finding] = []

        targets_vault, path_error = _vault_path_intent(path)
        if path_error:
            return path_error

        if targets_vault:
            binding = _vault_binding()
            if binding is None:
                return (
                    "Error: no project knowledge base is in scope to lint. "
                    "Pass `path` to lint a markdown directory in the workspace "
                    "instead."
                )
            root = binding.root_path or _VAULT_ROOT
            rows, error = _vault_rows(binding)
            if error:
                return error
            if not rows:
                return (
                    f"No knowledge notes found in {binding.name} "
                    f"(`{binding.alias}`) — the knowledge index holds no notes "
                    "for this knowledge base."
                )
            notes = [_store_note_markdown(row, root) for row in rows]
            extra_findings = _unmaterialised_finding(rows, root)
            if _scan_truncated(rows):
                extra_findings.append(
                    Finding(
                        "vault-scan-truncated",
                        "warning",
                        root,
                        f"only the first {_VAULT_SCAN_CAP} notes were linted "
                        "(scan cap) — findings below are incomplete",
                    )
                )
        else:
            # Explicit non-vault directory: the one job the index cannot do.
            root = str(path).rstrip("/")
            if not context.has_workspace():
                return (
                    f"Error: linting `{root}/` needs a workspace backend, and "
                    "this runtime has none. Omit `path` to lint the project "
                    "knowledge base, which reads from the knowledge index and "
                    "needs no workspace."
                )
            ws = context.workspace_manager
            try:
                entries = ws.list_files(root, "*.md")
            except Exception as e:
                return f"Error listing `{root}/`: {e}"
            for rel in entries:
                if rel.endswith("/"):
                    continue
                try:
                    notes.append({"path": rel, "text": ws.read_file(rel)})
                except Exception as e:
                    logger.warning(f"kb_lint: could not read {rel}: {e}")
            if not notes:
                return f"No markdown notes found under `{root}/`."

        report = lint_kb(notes)
        report.findings.extend(extra_findings)

        # Embedding-backed near-duplicate pass (the slice-2 deferred rule):
        # the pgvector index already holds one embedding per note, so one
        # self-join yields every high-similarity pair — no re-embedding here.
        # Non-fatal by design, matching the write-through posture: the
        # deterministic report stands alone when the index is unreachable.
        project_id = _get_project_id(context)
        if project_id:
            try:
                # Lint policy owns its floor: 0.97 (07-05 runbook decision) —
                # the store default (0.9) is unusable noise for a merge signal.
                pairs = _run_async(
                    ks.find_near_duplicate_pairs(
                        uuid.UUID(project_id), min_similarity=0.97
                    )
                )
                report.findings.extend(
                    near_duplicate_findings(pairs, [n["path"] for n in notes])
                )
            except Exception as e:
                logger.warning(f"kb_lint: near-duplicate pass skipped: {e}")

        # Opt-in dead-external-URL sweep: probe each unique http(s) link once,
        # flag only clear negatives, and report any capped remainder loudly.
        if check_urls:
            url_map = external_url_map(notes)
            urls = sorted(url_map)
            checked, skipped = urls[:_URL_SWEEP_CAP], urls[_URL_SWEEP_CAP:]
            dead: Dict[str, str] = {}
            for url in checked:
                reason = _check_external_url(url)
                if reason:
                    dead[url] = reason
            report.findings.extend(dead_url_findings(dead, url_map))
            if skipped:
                report.findings.append(
                    Finding(
                        "url-sweep-truncated",
                        "warning",
                        root,
                        f"{len(skipped)} external URL(s) not checked this run "
                        f"(cap {_URL_SWEEP_CAP}) — re-run to continue",
                    )
                )
        return report.format_markdown()

    @tool
    def kb_index(path: Optional[str] = None) -> str:
        """Regenerate the OKF `index.md` for a markdown vault.

        For the project knowledge base this reports the note count only: the
        vault lives in the project's knowledge repository, not in the
        workspace, so its `index.md` is never written from here. Use `kb_list`
        for the same grouping live.

        For an explicit workspace directory it writes a heading-grouped
        `[Title](slug.md) - description` index to `<path>/index.md` (OKF §6
        shape, no frontmatter). Content outside the auto-generated markers is
        preserved, so human-authored sections survive. Reserved files
        (index.md/log.md) are skipped.

        Args:
            path: Omit it for the project knowledge base (the normal case).
                Pass a directory to index some *other* markdown vault in the
                workspace instead; that mode needs a workspace, and is the
                only mode that writes a file.

        Returns:
            A status message with the note count, naming the file when one was
            written.
        """
        if _has_bound_scopes and not _get_project_id(context):
            return _write_scope_error(context)

        metas: List[Dict[str, Any]] = []
        truncated = False

        targets_vault, path_error = _vault_path_intent(path)
        if path_error:
            return path_error

        if targets_vault:
            binding = _vault_binding()
            if binding is None:
                return _write_scope_error(context)
            root = binding.root_path or _VAULT_ROOT
            rows, error = _vault_rows(binding)
            if error:
                return error
            truncated = _scan_truncated(rows)
            for row in rows:
                note_id = row.get("id")
                # Reserved ids (index/log) are generated artefacts, never notes.
                if not note_id or is_reserved(f"{note_id}.md"):
                    continue
                content = row.get("content") or ""
                metas.append(
                    {
                        "id": note_id,
                        "type": row.get("type") or "misc",
                        # The index has no description column, so derive it the
                        # same way the write path does when frontmatter omits
                        # one — the bullets stay populated either way.
                        "description": _derive_description(content),
                        "title": row.get("title") or note_title(content) or note_id,
                    }
                )
            if not metas:
                return (
                    f"No indexable notes found in {binding.name} "
                    f"(`{binding.alias}`) — the knowledge index holds no notes "
                    "for this knowledge base."
                )
            # knowledge_base_repo_separation §7 step 4: this used to write
            # `<root>/index.md` through the workspace, the last vault write in
            # this module besides the note materialisation. It cannot stay.
            # The vault is not a workspace directory any more — writing OKF
            # navigation into the checkout would put `index.md` in the jobs
            # repo while every note it links to is committed to the knowledge
            # repo, which is precisely the split this design removes. Nor can
            # the agent read the repo's current `index.md`, so regenerating it
            # blind would silently drop the human-authored sections outside
            # the generated markers that `render_index_md` exists to preserve.
            #
            # Generated vault navigation needs a server-side owner. The note
            # materialisation endpoint is not it: `index`/`log` are reserved
            # OKF basenames it refuses by contract (they are generated
            # artefacts the reindexer never indexes), and the reindexer —
            # which already walks every note on every sweep and can read the
            # existing file — is the natural home. Until that exists,
            # kb_index reports the grouping instead of writing it somewhere
            # wrong. Nothing is lost: no code reads `knowledge/index.md`, and
            # `kb_list` gives the same grouping live.
            summary = (
                f"Indexed {len(metas)} note(s) from the knowledge index of "
                f"{binding.name} (`{binding.alias}`)."
            )
            if truncated:
                summary += (
                    f" WARNING: only the first {_VAULT_SCAN_CAP} notes were "
                    "read (scan cap) — the grouping is incomplete."
                )
            return summary + (
                f" `{root}/index.md` was NOT rewritten: the vault lives in the "
                "project's knowledge repository, not in this workspace, so its "
                "generated navigation is never written from here. The notes "
                "themselves are unaffected — `kb_list` gives the same grouping "
                "live, and `kb_read`/`kb_search` reach every one of them."
            )
        else:
            root = str(path).rstrip("/")
            if not context.has_workspace():
                return (
                    f"Error: indexing `{root}/` needs a workspace backend, and "
                    "this runtime has none. Omit `path` to index the project "
                    "knowledge base, which reads from the knowledge index."
                )
            ws = context.workspace_manager
            try:
                entries = ws.list_files(root, "*.md")
            except Exception as e:
                return f"Error listing `{root}/`: {e}"
            for rel in entries:
                if rel.endswith("/") or is_reserved(rel):
                    continue
                try:
                    fm, body = parse_note_md(ws.read_file(rel))
                except Exception as e:
                    # Malformed YAML (ValueError) or an unreadable file —
                    # kb_lint surfaces the former; skip it for indexing anyway.
                    logger.warning(f"kb_index: skipping {rel}: {e}")
                    continue
                note_id = fm.get("id") if fm else None
                if not note_id:
                    continue  # unindexable (kb_lint reports the missing id)
                metas.append(
                    {
                        "id": note_id,
                        "type": fm.get("type") or "misc",
                        "description": fm.get("description"),
                        "title": note_title(body) or note_id,
                    }
                )
            if not metas:
                return f"No indexable notes found under `{root}/`."

        # Only the explicit non-vault mode reaches here — an ordinary markdown
        # directory in the workspace (`docs`, a repository datasource
        # checkout), which is a workspace file by definition and stays one.
        index_rel = f"{root}/index.md"
        existing = ws.read_file(index_rel) if ws.exists(index_rel) else None
        try:
            ws.write_file(index_rel, render_index_md(metas, existing=existing))
        except Exception as e:
            return f"Error writing `{index_rel}`: {e}"
        return f"Regenerated `{index_rel}` from {len(metas)} note(s)."

    # =========================================================================
    # Return all tools
    # =========================================================================

    return [
        kb_write,
        kb_update,
        kb_delete,
        kb_read,
        kb_list,
        kb_search,
        kb_grep,
        kb_related,
        kb_contradictions,
        kb_provenance,
        kb_unanswered,
        kb_export,
        kb_lint,
        kb_index,
    ]
