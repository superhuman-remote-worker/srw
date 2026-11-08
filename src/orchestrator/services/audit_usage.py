"""Materialize LLM token usage from the audit trail → ``usage_events``.

This reads prompt/completion token counts straight from the auditdb
``llm_requests`` table the agent already writes for every LLM call — worker jobs
AND persistent sessions — so metering does not depend on routing traffic through
a proxy.

Token source per row (extracted server-side as text via ``->>`` so this does not
depend on the read pool carrying a jsonb codec):
- worker rows: ``metrics.token_usage`` — the provider usage block
  (``prompt_tokens``/``completion_tokens``, or ``input_tokens``/``output_tokens``
  on Anthropic-wire).
- session rows (``agent_type='persistent'``): the normalized counts the
  persistent loop stashes in ``metadata.input_tokens`` / ``metadata.output_tokens``
  (streaming providers often leave ``response_metadata.token_usage`` empty).

Cost: none is written here. Cost-free token rows are emitted per call;
:meth:`UsageLedger.record_events` snapshots the $/token rate from
``usage_rates`` (seeded from OpenRouter — see ``openrouter_pricing.py``) exactly
as it priced the gateway rows. OpenAI-protocol cache reads are split into
``prompt-token`` (uncached) + ``cached-prompt-token`` (cache read) so the two
prompt rows sum to the provider's total prompt count. Reasoning tokens are a
*subset* of the completion total (billed at the completion rate), so they ride in
``details`` for observability, never as a separate priced row (that would
double-count cost).

Resource: the recorded ``llm_requests.model``, which agents write as the catalog
``model_id`` — what ``usage_rates`` is keyed on. Older agent images (still
running during a rollout) record auxiliary calls and native compaction under the
client's wire name instead (``openrouter/x/y`` sends ``x/y``). Such a string is
mapped to the catalog id only when it is not a catalog id itself and exactly
one catalog row sends it; otherwise it is recorded unchanged and stays unpriced
(fail closed).
``details.model`` always keeps the recorded string.

Attribution: ``llm_requests.job_id`` is a job id for worker rows and a thread id
for session rows. Resolved against the app-DB ``jobs`` / ``threads`` tables to
``user_id`` / ``project_id`` (soft — a deleted job's tokens still meter, just
unattributed) and carried as ``ref_kind`` / ``ref_id`` for per-job/thread cost.

Cursor: keyed on ``timestamp``, NOT ``id``. ``llm_requests.id`` is a BIGSERIAL but
NOT monotonic with insertion time in practice — the audit sequence has been reset
(verified live: a frozen high-id band coexists with an active band whose ids climb
from a low value), so a recent row can carry an id far below an older row's. A
max-id anchor would then silently meter nothing. ``timestamp`` (``now()`` at
write) is monotonic with insertion, so it's the safe cursor; ``id`` stays the
unique dedupe ``source_id`` (the bands don't overlap). The cursor only advances
over a *contiguous* run of rows older than ``min_age_s`` (the first too-fresh row
halts the tick) to dodge the assign-before-commit visibility window. Idempotent
regardless — the ledger dedupes on ``(source='audit', source_id, unit, ts)`` — so
a re-scan never double-counts. ``source='audit'`` keeps these rows in their own
idempotency namespace.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Optional

import asyncpg

from orchestrator.services.usage_ledger import UsageEvent, UsageLedger

logger = logging.getLogger(__name__)

# agent_type value the persistent loop writes; discriminates session rows (whose
# job_id is a thread id) from worker rows (whose job_id is a job id).
_SESSION_AGENT_TYPE = "persistent"

_DEFAULT_BATCH = 1000
# Aging window (seconds) a row must clear before its timestamp may advance the
# cursor — see the module docstring's "assign-before-commit" note.
_DEFAULT_MIN_AGE_S = 60.0
# ``since_ts=None`` floor → materialize from the beginning (full backfill).
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

# Pull the token counts server-side as TEXT (``->>``), independent of any jsonb
# codec on the read pool. The fallback order per dimension is applied in Python
# (:func:`_first_int`): worker's metrics.token_usage first, then the
# LangChain-normalized metrics.usage_metadata, then session's
# metadata.*_tokens. metrics is NOT NULL DEFAULT '{}'; metadata may be NULL.
#
# usage_metadata (um_*) is the ONLY home for token counts on the Responses API
# (codex/gpt-5.x worker jobs): those rows carry token_usage = {} AND a NULL
# metadata, so without the um_* fallbacks they resolved to 0/0 and were dropped
# as "nothing to meter" — no codex worker job ever reached the ledger (job
# c3fc9128, 07-18: 206 requests / ~17.3M tokens skipped).
#
# Cached prompt tokens have three homes, tried in order (:func:`_first_int`):
#   1. m_cached      — metrics.token_usage.prompt_tokens_details.cached_tokens
#                      (worker, Chat Completions: minimax/gemma/etc.)
#   2. m_cached_norm — metrics.usage_metadata.input_token_details.cache_read
#                      (worker, Responses API — see above)
#   3. md_cached     — metadata.cached_tokens (session/persistent turns)
# Codex cached tokens showed 0 in the By-Model view because only (1) was read.
#
# Ordered by (timestamp, id): timestamp is the monotonic cursor, id the stable
# tiebreak within a timestamp.
_SELECT_SQL = """
SELECT id, job_id, agent_type, call_type, model, timestamp,
       metrics->'token_usage'->>'prompt_tokens'     AS m_prompt,
       metrics->'token_usage'->>'input_tokens'      AS m_input,
       metrics->'token_usage'->>'completion_tokens' AS m_completion,
       metrics->'token_usage'->>'output_tokens'     AS m_output,
       metrics->'token_usage'->>'reasoning_tokens'  AS m_reasoning,
       metrics->'token_usage'->'prompt_tokens_details'->>'cached_tokens'
                                                   AS m_cached,
       metrics->'usage_metadata'->'input_token_details'->>'cache_read'
                                                   AS m_cached_norm,
       metrics->'usage_metadata'->>'input_tokens'   AS um_input,
       metrics->'usage_metadata'->>'output_tokens'  AS um_output,
       metrics->'usage_metadata'->'output_token_details'->>'reasoning'
                                                   AS um_reasoning,
       metadata->>'input_tokens'                    AS md_input,
       metadata->>'output_tokens'                   AS md_output,
       metadata->>'cached_tokens'                   AS md_cached
FROM llm_requests
WHERE timestamp > $1
ORDER BY timestamp, id
LIMIT $2
"""


def _first_int(*vals: Any) -> int:
    """First positive integer among ``vals`` (text/None), else 0.

    The ``->>`` extracts arrive as strings ("1234", occasionally "1234.0");
    tolerate both and skip absent / non-numeric / non-positive entries so the
    per-dimension fallback chain moves to the next candidate.
    """
    for v in vals:
        if v is None:
            continue
        try:
            n = int(float(v))
        except (TypeError, ValueError):
            continue
        if n > 0:
            return n
    return 0


# The routing prefixes the agent's chat factories strip from a catalog id
# before sending it: ``loader._create_openrouter_llm`` drops ``openrouter/``,
# and the groq, mistral and codex factories drop their own. Each factory strips
# only its own prefix, so a row's prefix must name the row's own provider.
_FACTORY_STRIPPED_PREFIXES = frozenset({"openrouter", "groq", "mistral", "codex"})

_CATALOG_SQL = (
    "SELECT model_id, served_model_id, provider_kind, provider_ref FROM models"
)

# details.resource_from on an event whose resource was mapped from a wire name.
RESOURCE_FROM_WIRE_NAME = "unique_wire_name"


def catalog_wire_name(row: Any) -> str:
    """The model name an agent's chat client sends for catalog ``row``.

    The served model id when the row names one, else the catalog id, minus the
    routing prefix the row's provider factory strips (a provider-key row
    ``openrouter/anthropic/claude-sonnet-5.5`` sends
    ``anthropic/claude-sonnet-5.5``). An endpoint row sends its id unchanged.
    """
    name = row["served_model_id"] or row["model_id"]
    prefix, sep, rest = str(name).partition("/")
    provider = (row["provider_ref"] or "").lower()
    if (
        sep
        and rest
        and row["provider_kind"] == "system"
        and prefix.lower() == provider
        and provider in _FACTORY_STRIPPED_PREFIXES
    ):
        return rest
    return str(name)


def unique_wire_names(rows: Iterable[Any]) -> dict[str, str]:
    """``{wire name: catalog id}`` for the wire names a recorded string may map.

    A wire name is kept only when exactly one catalog row sends it and it is
    not itself a catalog ``model_id``: two rows that strip to the same name
    (``openrouter/x/y`` and another gateway's ``…/x/y``) are ambiguous, and a
    string that is a catalog id already names its own row. Fail closed.
    """
    rows = [r for r in rows if r["model_id"]]
    catalog_ids = {str(r["model_id"]) for r in rows}
    senders: dict[str, list[str]] = {}
    for r in rows:
        senders.setdefault(catalog_wire_name(r), []).append(str(r["model_id"]))
    return {
        wire: ids[0]
        for wire, ids in senders.items()
        if len(ids) == 1 and wire not in catalog_ids
    }


async def _load_unique_wire_names(app_pool: asyncpg.Pool) -> dict[str, str]:
    """:func:`unique_wire_names` over the catalog. Non-fatal: ``{}`` on error,
    so every recorded string stays as it is."""
    try:
        async with app_pool.acquire() as conn:
            rows = await conn.fetch(_CATALOG_SQL)
    except Exception:
        logger.warning(
            "catalog read for usage resource mapping failed (non-fatal)",
            exc_info=True,
        )
        return {}
    return unique_wire_names(rows)


async def _resolve_owners(
    app_pool: asyncpg.Pool,
    job_ids: Iterable[Any],
    thread_ids: Iterable[Any],
) -> dict[Any, tuple[Any, Any]]:
    """Map ``{id: (user_id, project_id)}`` for the batch's job + thread ids.

    Two batched app-DB reads (jobs, threads). Non-fatal: an attribution failure
    logs and returns what resolved so far — the tokens still meter, just
    unattributed — rather than dropping the tick's usage.
    """
    owners: dict[Any, tuple[Any, Any]] = {}
    job_ids = list(job_ids)
    thread_ids = list(thread_ids)
    try:
        async with app_pool.acquire() as conn:
            if job_ids:
                for r in await conn.fetch(
                    "SELECT id, user_id, project_id FROM jobs "
                    "WHERE id = ANY($1::uuid[])",
                    job_ids,
                ):
                    owners[r["id"]] = (r["user_id"], r["project_id"])
            if thread_ids:
                for r in await conn.fetch(
                    "SELECT id, user_id, project_id FROM threads "
                    "WHERE id = ANY($1::uuid[])",
                    thread_ids,
                ):
                    owners[r["id"]] = (r["user_id"], r["project_id"])
    except Exception:
        logger.warning("usage attribution lookup failed (non-fatal)", exc_info=True)
    return owners


async def materialize_llm_usage_from_audit(
    audit_pool: Optional[asyncpg.Pool],
    app_pool: Optional[asyncpg.Pool],
    ledger: Optional[UsageLedger],
    *,
    since_ts: Optional[datetime] = None,
    batch_limit: int = _DEFAULT_BATCH,
    min_age_s: float = _DEFAULT_MIN_AGE_S,
    now: Optional[datetime] = None,
) -> dict[str, Any]:
    """Materialize ``llm_requests`` rows after ``since_ts`` into ``usage_events``.

    Emits cost-free ``prompt-token`` / ``cached-prompt-token`` /
    ``completion-token`` rows (the ledger prices them from ``usage_rates``),
    attributed to the row's user/project.
    Advances the cursor only over a contiguous run of rows older than
    ``min_age_s`` (see module docstring). ``since_ts=None`` materializes from the
    beginning. Returns ``{"materialized", "cursor", "scanned"}`` where ``cursor``
    is a timestamp. Non-fatal: no-ops (cursor unchanged) when any pool or the
    ledger is unavailable.
    """
    if (
        audit_pool is None
        or app_pool is None
        or ledger is None
        or not ledger.is_available
    ):
        return {"materialized": 0, "cursor": since_ts, "scanned": 0}

    async with audit_pool.acquire() as conn:
        rows = await conn.fetch(_SELECT_SQL, since_ts or _EPOCH, batch_limit)
    if not rows:
        return {"materialized": 0, "cursor": since_ts, "scanned": 0}

    cutoff = (now or datetime.now(timezone.utc)) - timedelta(seconds=min_age_s)

    # Contiguous aged prefix: stop at the first row newer than the cutoff so the
    # cursor never advances past a timestamp that may still have a mid-commit
    # sibling not yet visible.
    aged = []
    for r in rows:
        if r["timestamp"] > cutoff:
            break
        aged.append(r)
    if not aged:
        return {"materialized": 0, "cursor": since_ts, "scanned": len(rows)}

    job_ids = {
        r["job_id"] for r in aged if (r["agent_type"] or "") != _SESSION_AGENT_TYPE
    }
    thread_ids = {
        r["job_id"] for r in aged if (r["agent_type"] or "") == _SESSION_AGENT_TYPE
    }
    owners = await _resolve_owners(app_pool, job_ids, thread_ids)
    wire_names = await _load_unique_wire_names(app_pool)

    events: list[UsageEvent] = []
    new_cursor = since_ts
    for r in aged:
        new_cursor = r["timestamp"]  # advance over every aged row, priced or not
        prompt = _first_int(r["m_prompt"], r["m_input"], r["um_input"], r["md_input"])
        completion = _first_int(
            r["m_completion"], r["m_output"], r["um_output"], r["md_output"]
        )
        if not prompt and not completion:
            continue  # health check / audio / errored call — nothing to meter
        cached = min(
            _first_int(r["m_cached"], r["m_cached_norm"], r["md_cached"]), prompt
        )
        uncached_prompt = prompt - cached
        is_session = (r["agent_type"] or "") == _SESSION_AGENT_TYPE
        user_id, project_id = owners.get(r["job_id"], (None, None))
        reasoning = _first_int(r["m_reasoning"], r["um_reasoning"])
        recorded_model = str(r["model"])
        resource = wire_names.get(recorded_model, recorded_model)
        details: dict[str, Any] = {
            "model": recorded_model,
            "llm_request_id": r["id"],
            "agent_type": r["agent_type"],
            "call_type": r["call_type"],
        }
        if resource != recorded_model:
            details["resource_from"] = RESOURCE_FROM_WIRE_NAME
        if reasoning:
            details["reasoning_tokens"] = reasoning
        if cached:
            details["cached_prompt_tokens"] = cached
        common = dict(
            category="llm",
            resource=resource,
            source="audit",
            source_id=str(r["id"]),
            ts=r["timestamp"],
            user_id=user_id,
            project_id=project_id,
            ref_kind="thread" if is_session else "job",
            ref_id=str(r["job_id"]),
            details=details,
        )
        if uncached_prompt:
            events.append(
                UsageEvent(quantity=uncached_prompt, unit="prompt-token", **common)
            )
        if cached:
            events.append(
                UsageEvent(quantity=cached, unit="cached-prompt-token", **common)
            )
        if completion:
            events.append(
                UsageEvent(quantity=completion, unit="completion-token", **common)
            )

    inserted = await ledger.record_events(events)
    if inserted:
        logger.info(
            "LLM usage (audit): materialized %d ledger row(s) from %d audit row(s)",
            inserted,
            len(aged),
        )
    return {"materialized": inserted, "cursor": new_cursor, "scanned": len(rows)}


async def llm_usage_poll_loop(
    shutdown_event: asyncio.Event,
    *,
    audit_db: Any,
    app_store: Any,
    usage_ledger: Any,
    logger: Any = logger,
    interval: float = 120.0,
) -> None:
    """Background loop: materialize the audit trail into usage_events (LLM cost).

    Reads new ``llm_requests`` rows via ``materialize_llm_usage_from_audit`` and
    prices them from ``usage_rates`` (seeded by ``llm_pricing_sync_loop``) — no
    proxy in the path. Idempotent at the ledger; never raises into the lifespan.

    Forward-only: the cursor anchors at the current max ``llm_requests.timestamp``
    on startup, so the audit materializer does not backfill historical rows unless
    an operator deliberately lowers the anchor. The cursor is a timestamp, NOT an
    id: ``llm_requests.id`` is not monotonic with time. No-op when the ledger or
    either pool is unavailable.

    Extracted from ``orchestrator.main`` (R1.B05 lane C). ``audit_db``,
    ``app_store``, ``usage_ledger`` and ``logger`` are rebound by the
    application during ``lifespan``, so they are passed in at the single call
    site rather than read from a module global — the same shape
    ``workspace_metering_loop`` and ``llm_pricing_sync_loop`` already use.
    ``asyncio.CancelledError`` is deliberately NOT caught: a poll loop that
    swallows it blocks shutdown.
    """
    if usage_ledger is None or not usage_ledger.is_available:
        logger.info("LLM usage poll loop disabled (usage ledger unavailable)")
        return
    if audit_db is None or audit_db.pool is None or app_store.pool is None:
        logger.info("LLM usage poll loop disabled (audit/app pool unavailable)")
        return

    # Forward-only anchor: start after the newest existing row by timestamp.
    # Falls back to wall-clock now when empty.
    cursor = datetime.now(timezone.utc)
    try:
        async with audit_db.pool.acquire() as conn:
            newest = await conn.fetchval("SELECT max(timestamp) FROM llm_requests")
        if newest is not None:
            cursor = newest
    except Exception:
        logger.warning(
            "LLM usage anchor query failed; starting from now", exc_info=True
        )
    logger.info(
        "LLM usage poll loop starting (audit source, anchor ts=%s, interval=%ss)",
        cursor,
        interval,
    )
    try:
        while not shutdown_event.is_set():
            try:
                res = await materialize_llm_usage_from_audit(
                    audit_db.pool, app_store.pool, usage_ledger, since_ts=cursor
                )
                cursor = res.get("cursor") or cursor
            except Exception:
                logger.exception("LLM usage poll tick failed (non-fatal)")
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass
    finally:
        logger.info("LLM usage poll loop stopped")


__all__ = ["llm_usage_poll_loop", "materialize_llm_usage_from_audit"]
