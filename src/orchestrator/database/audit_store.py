"""Async reader for the Postgres audit store (the cockpit-facing surface).

Counterpart to the agent's :class:`~agent.database.audit_writer.SyncAuditWriter`.
Emits the exact wire shape the audit endpoints and cockpit consume, so the read
path stays fixed and endpoint/cockpit code is unchanged.

Design: ``knowledge-base/knowledge/features/postgres_audit_store_implementation.md`` §5. The two
load-bearing translations:
- **Logical-row stitch (D3):** ``agent_audit`` is append-only (pre row at
  dispatch, post row at completion). Every read returns ONE row per ``pre`` row
  with the latest ``post`` merged in via ``LEFT JOIN LATERAL`` + a two-level
  ``jsonb`` merge, so counts / ``step_number`` / wire shape stay stable and the
  cockpit needs no collapse logic.
- **step_number (D10):** synthesized with ``ROW_NUMBER()`` over ALL pre rows of
  the job, *then* filtered — the wire contract exposes a global step_number
  under a ``FilterCategory``, so numbering the filtered subset would be wrong.

Degraded behavior: ``is_available`` is startup-latched (connect/disconnect
only); runtime DB loss surfaces as exceptions -> 500s; the per-endpoint degraded
shapes live in ``main.py`` and stay verbatim.
"""

from __future__ import annotations

import json
import logging
import math
import uuid
from datetime import timezone
from typing import Any, AsyncIterator, Dict, List, Literal, Optional, Sequence

import asyncpg

from shared.db_url import build_postgres_url
from shared.runtime_actor import audit_metadata_payload

logger = logging.getLogger(__name__)


def _to_iso_utc(timestamp: Any) -> str:
    """Datetime -> ISO string with a UTC 'Z' suffix.

    Naive datetimes are assumed UTC (isoformat + 'Z', microseconds); tz-aware
    values (what asyncpg returns) are converted to UTC and rendered to
    millisecond precision + 'Z' — matching the legacy reader's output.
    """
    if hasattr(timestamp, "isoformat"):
        if hasattr(timestamp, "tzinfo") and timestamp.tzinfo is None:
            return timestamp.isoformat() + "Z"
        elif hasattr(timestamp, "astimezone"):
            utc_dt = timestamp.astimezone(timezone.utc)
            return utc_dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        return timestamp.isoformat()
    return str(timestamp)


def _normalize_token_usage(token_usage: Any, usage_metadata: Any) -> Dict[str, Any]:
    """Token counts in Chat Completions keys, whichever home they were born in.

    Responses-API rows (codex/gpt-5.x) carry ``token_usage = {}`` — their counts
    live only in LangChain's normalized ``usage_metadata`` — so re-key that as
    the fallback; otherwise every codex request lists as ``tokens=[?/?/?]``.
    """
    if isinstance(token_usage, dict) and token_usage:
        return token_usage
    if isinstance(usage_metadata, dict) and usage_metadata:
        return {
            k: usage_metadata[src]
            for k, src in (
                ("prompt_tokens", "input_tokens"),
                ("completion_tokens", "output_tokens"),
                ("total_tokens", "total_tokens"),
            )
            if usage_metadata.get(src) is not None
        }
    return {}


# Filter category -> step_types.
FILTER_MAPPINGS: Dict[str, List[str]] = {
    "all": [],
    "messages": ["llm"],
    "tools": ["tool"],
    "errors": ["error"],
}
FilterCategory = Literal["all", "messages", "tools", "errors"]

# Cypher tool names that produce graph deltas (current + legacy).
GRAPH_TOOL_NAMES = ["cypher_query", "cypher_execute", "execute_cypher_query"]

# A job's last activity: the newest timestamp among its newest pre rows by id,
# never later than the database clock. The newest row by id is not always the
# newest step: a stateless lease's ``claim_timing`` row is written when the
# lease ends but stamped with the claim time, an ``audit_receipt`` row lands
# at batch end, and rows replayed after an audit-store outage arrive late.
# The window keeps the read on the ``(job_id, id)`` index; a whole-job maximum
# would read every pre row. The cap absorbs an agent clock ahead of the
# database (pre rows carry the agent's clock). No rows: no row, so NULL.
_AUDIT_TAIL_ROWS = 50
_LAST_ACTIVITY_SQL = f"""
    SELECT LEAST(max(tail.timestamp), now()) FROM (
        SELECT timestamp FROM agent_audit
        WHERE job_id=$1::uuid AND event_phase='pre'
        ORDER BY id DESC LIMIT {_AUDIT_TAIL_ROWS}
    ) AS tail
    HAVING count(*) > 0
"""


async def _init_connection(conn: asyncpg.Connection) -> None:
    """Text JSONB codec: payload/request/response/metrics come back as dicts."""
    await conn.set_type_codec(
        "jsonb",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
        format="text",
    )


# Logical-row SELECT building blocks. ``_STITCH_CORE`` numbers all pre rows (for
# step_number), overlays the latest post row, and merges payloads two levels deep
# (jsonb || is shallow, so the 'tool'/'llm' subobjects are re-merged explicitly).
# ``_STITCH_LEAN`` is the list/timeline projection: identical column shape (so the
# same ``_audit_row_to_doc`` handles both) but NULLs the boot-only ``metadata``
# blob (never rendered) and drops the expand-only heavy payload sub-keys (tool
# arguments, error traceback, state snapshot) so a page stays small even when a
# step wrote a big file or a long stack trace; the detail view fetches the full
# row via ``get_audit_step``. Composed from shared parts so the two can't drift.
# Callers append WHERE/ORDER/OFFSET/LIMIT referencing alias ``f``; $1 is job_id.
_STITCH_NUMBERED = """
WITH numbered AS (
    SELECT a.*, ROW_NUMBER() OVER (ORDER BY a.id) AS step_number
    FROM agent_audit a
    WHERE a.job_id = $1::uuid AND a.event_phase = 'pre'
)"""

_STITCH_MERGED_PAYLOAD = """(
         f.payload || COALESCE(p.payload, '{}'::jsonb)
         || CASE WHEN p.payload ? 'tool'
                 THEN jsonb_build_object(
                     'tool', COALESCE(f.payload->'tool', '{}'::jsonb) || (p.payload->'tool'))
                 ELSE '{}'::jsonb END
         || CASE WHEN p.payload ? 'llm'
                 THEN jsonb_build_object(
                     'llm', COALESCE(f.payload->'llm', '{}'::jsonb) || (p.payload->'llm'))
                 ELSE '{}'::jsonb END
       )"""

_STITCH_FROM = """
FROM numbered f
LEFT JOIN LATERAL (
    SELECT post.payload, post.latency_ms
    FROM agent_audit post
    WHERE post.pre_id = f.id AND post.event_phase = 'post'
    ORDER BY post.id DESC LIMIT 1
) p ON TRUE
"""

_STITCH_CORE = f"""{_STITCH_NUMBERED}
SELECT f.id, f.job_id, f.agent_type, f.iteration, f.step_type, f.node_name,
       f.phase, f.phase_number, f.timestamp, f.metadata, f.step_number,
       COALESCE(p.latency_ms, f.latency_ms) AS latency_ms,
       {_STITCH_MERGED_PAYLOAD} AS payload
{_STITCH_FROM}"""

# List/timeline projection: no metadata, heavy expand-only sub-keys removed.
_STITCH_LEAN = f"""{_STITCH_NUMBERED}
SELECT f.id, f.job_id, f.agent_type, f.iteration, f.step_type, f.node_name,
       f.phase, f.phase_number, f.timestamp, NULL::jsonb AS metadata, f.step_number,
       COALESCE(p.latency_ms, f.latency_ms) AS latency_ms,
       {_STITCH_MERGED_PAYLOAD} #- '{{state}}' #- '{{tool,arguments}}' #- '{{error,traceback}}' AS payload
{_STITCH_FROM}"""


def _audit_row_to_doc(r: asyncpg.Record) -> Dict[str, Any]:
    """Stitched agent_audit row -> wire doc.

    The merged ``payload`` is splatted over the row LAST (a dict ``update``) —
    including the documented ``phase_complete`` quirk where ``payload.phase``
    (an object) shadows the ``phase`` column.
    ``event_phase`` / ``pre_id`` / the hard ``request_id`` are never on the wire.
    """
    doc: Dict[str, Any] = {
        "id": r["id"],
        "job_id": str(r["job_id"]),
        "agent_type": r["agent_type"],
        "iteration": r["iteration"],
        "step_type": r["step_type"],
        "node_name": r["node_name"],
        "step_number": r["step_number"],
        "timestamp": r["timestamp"],
    }
    if r["phase"] is not None:
        doc["phase"] = r["phase"]
    if r["phase_number"] is not None:
        doc["phase_number"] = r["phase_number"]
    if r["latency_ms"] is not None:
        doc["latency_ms"] = r["latency_ms"]
    if r["metadata"] is not None:
        doc["metadata"] = r["metadata"]
    payload = r["payload"]
    if isinstance(payload, dict):
        doc.update(payload)
    # Keep payload-shadow precedence, then redact the final metadata projection.
    if "metadata" in doc:
        doc["metadata"] = audit_metadata_payload(doc["metadata"])
    return doc


def _lean_chat_element(el: Any) -> Any:
    """Strip a full ``content`` body from one inputs/response/reasoning element.

    Keeps ``content_preview`` and marks the element ``truncated`` (+ ``chars``)
    so the UI knows a detail fetch has more. Elements whose content already
    equals the preview pass through unchanged.
    """
    if not isinstance(el, dict):
        return el
    content = el.get("content")
    preview = el.get("content_preview")
    if isinstance(content, str) and isinstance(preview, str) and content != preview:
        el = {k: v for k, v in el.items() if k != "content"}
        el["truncated"] = True
        el.setdefault("chars", len(content))
    return el


def _lean_chat_doc(doc: Dict[str, Any]) -> Dict[str, Any]:
    """chat_history doc -> lean listing projection (previews only).

    Drops full message bodies from ``inputs`` (including the full copies on
    ``context`` change turns), ``response.content``, per-tool-call ``args``,
    and ``reasoning.content``; :meth:`AuditStore.get_chat_entry` serves the
    complete doc on demand. Pure + non-mutating so it's unit-testable and safe
    on shared row dicts.
    """
    doc = dict(doc)
    inputs = doc.get("inputs")
    if isinstance(inputs, list):
        doc["inputs"] = [_lean_chat_element(el) for el in inputs]
    response = doc.get("response")
    if isinstance(response, dict):
        response = _lean_chat_element(response)
        tool_calls = response.get("tool_calls")
        if isinstance(tool_calls, list):
            lean_tcs = []
            for tc in tool_calls:
                if isinstance(tc, dict) and "args" in tc:
                    tc = {k: v for k, v in tc.items() if k != "args"}
                    tc["args_truncated"] = True
                lean_tcs.append(tc)
            response = dict(response)
            response["tool_calls"] = lean_tcs
        doc["response"] = response
    reasoning = doc.get("reasoning")
    if isinstance(reasoning, dict):
        doc["reasoning"] = _lean_chat_element(reasoning)
    return doc


class AuditStore:
    """Async reader over the audit Postgres read pool."""

    def __init__(self, dsn: Optional[str] = None):
        self._dsn = dsn or build_postgres_url(
            "AUDIT_POSTGRES", fallback_env="AUDIT_DB_URL"
        )
        self._pool: Optional[asyncpg.Pool] = None
        self._available = False

    @property
    def is_available(self) -> bool:
        """Startup-latched (connect/disconnect only)."""
        return self._available

    @property
    def pool(self) -> Optional[asyncpg.Pool]:
        return self._pool

    async def connect(self) -> None:
        """Create the read pool + ping. Never raises (parity)."""
        if self._pool is not None:
            return
        if not self._dsn:
            self._available = False
            logger.info("Audit DB URL not configured. Read features disabled.")
            return
        try:
            self._pool = await asyncpg.create_pool(
                self._dsn,
                min_size=1,
                max_size=5,
                command_timeout=30,
                statement_cache_size=0,
                server_settings={"application_name": "srw-audit-read"},
                init=_init_connection,
            )
            async with self._pool.acquire() as conn:
                await conn.execute("SELECT 1")
            self._available = True
            logger.info("Audit store (read) connected")
        except Exception as e:
            logger.warning(f"Audit store connection failed: {e}")
            self._available = False
            if self._pool is not None:
                await self._pool.close()
            self._pool = None

    async def disconnect(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None
            self._available = False
            logger.info("Audit store connection closed")

    # -- agent_audit reads --------------------------------------------------

    async def get_job_audit(
        self,
        job_id: str,
        page: int = 1,
        page_size: int = 50,
        filter_category: FilterCategory = "all",
        offset: Optional[int] = None,
        limit: Optional[int] = None,
        order: Literal["asc", "desc"] = "asc",
        lean: bool = False,
    ) -> Dict[str, Any]:
        effective_size = limit if limit is not None else page_size
        empty = {
            "entries": [],
            "total": 0,
            "page": max(1, page),
            "pageSize": effective_size,
            "offset": offset if offset is not None else 0,
            "limit": effective_size,
            "hasMore": False,
        }
        if not self._available or self._pool is None:
            return empty
        try:
            step_types = FILTER_MAPPINGS.get(filter_category, [])
            async with self._pool.acquire() as conn:
                if step_types:
                    total = await conn.fetchval(
                        "SELECT count(*) FROM agent_audit WHERE job_id=$1::uuid "
                        "AND event_phase='pre' AND step_type = ANY($2)",
                        job_id,
                        step_types,
                    )
                else:
                    total = await conn.fetchval(
                        "SELECT count(*) FROM agent_audit WHERE job_id=$1::uuid "
                        "AND event_phase='pre'",
                        job_id,
                    )
                total = total or 0

                if offset is not None:
                    effective_skip = offset
                else:
                    effective_page = page
                    if effective_page == -1:
                        effective_page = (
                            max(1, math.ceil(total / effective_size))
                            if effective_size
                            else 1
                        )
                    effective_skip = (effective_page - 1) * effective_size

                has_more = (effective_skip + effective_size) < total
                order_sql = "ASC" if order == "asc" else "DESC"
                core = _STITCH_LEAN if lean else _STITCH_CORE

                if step_types:
                    sql = (
                        core
                        + f" WHERE f.step_type = ANY($2) ORDER BY f.id {order_sql} "
                        "OFFSET $3 LIMIT $4"
                    )
                    rows = await conn.fetch(
                        sql, job_id, step_types, effective_skip, effective_size
                    )
                else:
                    sql = core + f" ORDER BY f.id {order_sql} OFFSET $2 LIMIT $3"
                    rows = await conn.fetch(sql, job_id, effective_skip, effective_size)

            entries = [_audit_row_to_doc(r) for r in rows]
            response_page = (
                (effective_skip // effective_size) + 1 if effective_size else 1
            )
            return {
                "entries": entries,
                "total": total,
                "page": response_page,
                "pageSize": effective_size,
                "offset": effective_skip,
                "limit": effective_size,
                "hasMore": has_more,
            }
        except Exception as e:
            logger.warning(f"get_job_audit failed: {e}")
            return empty

    async def get_audit_step(
        self, job_id: str, step_id: int
    ) -> Optional[Dict[str, Any]]:
        """Full stitched doc for one audit step (by ``agent_audit.id``).

        For the detail/expand view: includes the heavy payload + metadata that the
        lean list projection drops. One row, so the size is fine. Returns ``None``
        if the store is unavailable or ``step_id`` isn't a pre row of this job.
        """
        if not self._available or self._pool is None:
            return None
        try:
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow(
                    _STITCH_CORE + " WHERE f.id = $2 LIMIT 1", job_id, int(step_id)
                )
            return _audit_row_to_doc(row) if row else None
        except Exception as e:
            logger.warning(f"get_audit_step failed: {e}")
            return None

    async def get_audit_count(self, job_id: str) -> int:
        if not self._available or self._pool is None:
            return 0
        try:
            async with self._pool.acquire() as conn:
                n = await conn.fetchval(
                    "SELECT count(*) FROM agent_audit WHERE job_id=$1::uuid "
                    "AND event_phase='pre'",
                    job_id,
                )
            return n or 0
        except Exception as e:
            logger.warning(f"get_audit_count failed: {e}")
            return 0

    async def get_audit_counts(self, job_ids: Sequence[str]) -> Dict[str, int]:
        """Per-job pre-row counts in one query (collapses the N+1 enrichers)."""
        result = {str(j): 0 for j in job_ids}
        if not self._available or self._pool is None or not job_ids:
            return result
        try:
            uuids: List[uuid.UUID] = []
            for j in job_ids:
                try:
                    uuids.append(uuid.UUID(str(j)))
                except (ValueError, AttributeError, TypeError):
                    pass  # non-UUID id -> stays 0 (parity with "no match")
            if not uuids:
                return result
            async with self._pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT job_id, count(*) AS n FROM agent_audit "
                    "WHERE job_id = ANY($1::uuid[]) AND event_phase='pre' "
                    "GROUP BY job_id",
                    uuids,
                )
            for r in rows:
                result[str(r["job_id"])] = r["n"]
            return result
        except Exception as e:
            logger.warning(f"get_audit_counts failed: {e}")
            return result

    async def get_audit_counts_strict(self, job_ids: Sequence[str]) -> Dict[str, int]:
        """Per-job audit counts without converting storage failure to zero.

        Liveness containment uses absence of count movement as authoritative
        evidence.  The ordinary Cockpit enricher intentionally degrades a read
        failure to zeros, but doing that here would make an audit outage look
        like real movement and reset the redispatch circuit.  Keep this narrow
        strict seam for recovery policy while preserving the existing UI/API
        behavior of :meth:`get_audit_counts`.
        """

        if not self._available or self._pool is None:
            raise RuntimeError("audit store is unavailable")
        result = {str(job_id): 0 for job_id in job_ids}
        if not job_ids:
            return result
        uuids: List[uuid.UUID] = []
        for job_id in job_ids:
            try:
                uuids.append(uuid.UUID(str(job_id)))
            except (ValueError, AttributeError, TypeError):
                pass
        if not uuids:
            return result
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT job_id, count(*) AS n FROM agent_audit "
                "WHERE job_id = ANY($1::uuid[]) AND event_phase='pre' "
                "GROUP BY job_id",
                uuids,
            )
        for row in rows:
            result[str(row["job_id"])] = row["n"]
        return result

    async def get_audit_time_range(self, job_id: str) -> Optional[Dict[str, str]]:
        """``start`` is the first-inserted pre row's timestamp; ``end`` is the
        job's last activity (``_LAST_ACTIVITY_SQL``). Liveness and elapsed time
        read ``end`` only."""
        if not self._available or self._pool is None:
            return None
        try:
            async with self._pool.acquire() as conn:
                first = await conn.fetchval(
                    "SELECT timestamp FROM agent_audit WHERE job_id=$1::uuid "
                    "AND event_phase='pre' ORDER BY id ASC LIMIT 1",
                    job_id,
                )
                last = await conn.fetchval(_LAST_ACTIVITY_SQL, job_id)
            if first is None or last is None:
                return None
            return {"start": _to_iso_utc(first), "end": _to_iso_utc(last)}
        except Exception as e:
            logger.warning(f"get_audit_time_range failed: {e}")
            return None

    async def iter_tool_calls(self, job_id: str) -> AsyncIterator[Dict[str, Any]]:
        """Stream stitched tool-step docs (replaces the raw graph_routes cursor).

        Keyset-chunked by id (1000/batch) so the previously unbounded scan can't
        pin a server-side cursor. Yields the same stitched+splatted docs the
        cypher regex parser already consumes.
        """
        if not self._available or self._pool is None:
            return
        sql = (
            _STITCH_CORE
            + " WHERE f.step_type = 'tool' AND f.id > $2 ORDER BY f.id ASC LIMIT 1000"
        )
        cursor_id = 0
        while True:
            try:
                async with self._pool.acquire() as conn:
                    rows = await conn.fetch(sql, job_id, cursor_id)
            except Exception as e:
                logger.warning(f"iter_tool_calls failed: {e}")
                return
            if not rows:
                return
            for r in rows:
                yield _audit_row_to_doc(r)
            cursor_id = rows[-1]["id"]
            if len(rows) < 1000:
                return

    # -- chat_history reads -------------------------------------------------

    async def get_chat_history(
        self,
        job_id: str,
        page: int = 1,
        page_size: int = 50,
        offset: Optional[int] = None,
        limit: Optional[int] = None,
        lean: bool = False,
    ) -> Dict[str, Any]:
        """Paginated chat turns.

        Supports offset/limit (REST-style) or page/pageSize (legacy); offset/limit
        wins when both are provided. Mirrors ``get_job_audit`` so a single endpoint
        can serve both styles and the response echoes both.

        ``lean=True`` strips full message bodies from each entry (keeping the
        previews plus ``truncated``/``chars`` markers) so the debug chat panel
        can page cheaply and hydrate single turns on demand via
        :meth:`get_chat_entry` — same split as the audit lean projection +
        ``get_audit_step``.
        """
        effective_size = limit if limit is not None else page_size
        empty = {
            "entries": [],
            "total": 0,
            "page": max(1, page),
            "pageSize": effective_size,
            "offset": offset if offset is not None else 0,
            "limit": effective_size,
            "hasMore": False,
        }
        if not self._available or self._pool is None:
            return empty
        try:
            async with self._pool.acquire() as conn:
                total = await conn.fetchval(
                    "SELECT count(*) FROM chat_history WHERE job_id=$1::uuid", job_id
                )
                total = total or 0
                if offset is not None:
                    effective_skip = offset
                else:
                    if page == -1:
                        page = (
                            max(1, math.ceil(total / effective_size))
                            if effective_size
                            else 1
                        )
                    effective_skip = (page - 1) * effective_size
                rows = await conn.fetch(
                    "SELECT * FROM chat_history WHERE job_id=$1::uuid "
                    "ORDER BY timestamp, id OFFSET $2 LIMIT $3",
                    job_id,
                    effective_skip,
                    effective_size,
                )
            response_page = (
                (effective_skip // effective_size) + 1 if effective_size else 1
            )
            entries = [self._chat_row_to_doc(r) for r in rows]
            if lean:
                entries = [_lean_chat_doc(d) for d in entries]
            return {
                "entries": entries,
                "total": total,
                "page": response_page,
                "pageSize": effective_size,
                "offset": effective_skip,
                "limit": effective_size,
                "hasMore": (effective_skip + effective_size) < total,
            }
        except Exception as e:
            logger.warning(f"get_chat_history failed: {e}")
            return empty

    async def get_chat_entry(
        self, job_id: str, entry_id: Any
    ) -> Optional[Dict[str, Any]]:
        """Full doc for one chat turn (by ``chat_history.id``).

        Detail fetch behind the lean listing: returns the complete inputs /
        response / reasoning bodies the ``lean`` projection drops. ``entry_id``
        is int-parsed (non-numeric → ``None`` → the endpoint's 404-after-auth).
        Returns ``None`` when the store is unavailable or the row isn't part of
        this job.
        """
        if not self._available or self._pool is None:
            return None
        try:
            eid = int(entry_id)
        except (TypeError, ValueError):
            return None
        try:
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT * FROM chat_history WHERE job_id=$1::uuid AND id=$2 "
                    "LIMIT 1",
                    job_id,
                    eid,
                )
            return self._chat_row_to_doc(row) if row else None
        except Exception as e:
            logger.warning(f"get_chat_entry failed: {e}")
            return None

    @staticmethod
    def _chat_row_to_doc(r: asyncpg.Record) -> Dict[str, Any]:
        # The wire contract always includes iteration/latency_ms (even None); phase/phase_number/
        # reasoning are conditional.
        doc: Dict[str, Any] = {
            "id": r["id"],
            "job_id": str(r["job_id"]),
            "agent_type": r["agent_type"],
            "timestamp": r["timestamp"],
            "iteration": r["iteration"],
            "model": r["model"],
            "latency_ms": r["latency_ms"],
            "inputs": r["inputs"],
            "response": r["response"],
            "request_id": r["request_id"],
        }
        if r["phase"] is not None:
            doc["phase"] = r["phase"]
        if r["phase_number"] is not None:
            doc["phase_number"] = r["phase_number"]
        if r["reasoning"] is not None:
            doc["reasoning"] = r["reasoning"]
        return doc

    # -- llm_requests reads -------------------------------------------------

    async def get_request(self, doc_id: Any) -> Optional[Dict[str, Any]]:
        """Single llm_requests row by id. ``doc_id`` int-parsed (route stays str:
        non-numeric -> None -> the endpoint's 404-after-auth, not a 422)."""
        if not self._available or self._pool is None:
            return None
        try:
            rid = int(doc_id)
        except (TypeError, ValueError):
            return None
        try:
            async with self._pool.acquire() as conn:
                r = await conn.fetchrow(
                    "SELECT id, job_id, agent_type, call_type, model, iteration, "
                    "timestamp, latency_ms, request, response, metadata, "
                    "auxiliary_metadata, metrics FROM llm_requests WHERE id = $1",
                    rid,
                )
            if r is None:
                return None
            doc: Dict[str, Any] = {
                "id": r["id"],
                "job_id": str(r["job_id"]),
                "agent_type": r["agent_type"],
                "timestamp": r["timestamp"],
                "model": r["model"],
                "call_type": r["call_type"],
                "request": r["request"],
                "response": r["response"],
                "metrics": r["metrics"],
            }
            if r["latency_ms"] is not None:
                doc["latency_ms"] = r["latency_ms"]
            if r["iteration"] is not None:
                doc["iteration"] = r["iteration"]
            if r["metadata"] is not None:
                doc["metadata"] = audit_metadata_payload(r["metadata"])
            if r["auxiliary_metadata"] is not None:
                doc["auxiliary_metadata"] = audit_metadata_payload(
                    r["auxiliary_metadata"]
                )
            return doc
        except Exception as e:
            logger.warning(f"get_request failed: {e}")
            return None

    async def list_llm_requests(
        self,
        job_id: str,
        limit: int = 20,
        offset: int = 0,
        call_type: Optional[str] = None,
        status: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Summary listing. ``token_usage`` now surfaced from its real nested home
        (metrics->'token_usage') — fixes the silently-absent field (D14). Error
        rows carry status/error in ``metadata`` (see archiver.archive_error), so
        the ``status`` filter and surfaced fields read from there."""
        limit = min(limit, 100)
        empty = {
            "entries": [],
            "total": 0,
            "offset": offset,
            "limit": limit,
            "hasMore": False,
        }
        if not self._available or self._pool is None:
            return empty
        try:
            clauses = ["job_id = $1::uuid"]
            params: List[Any] = [job_id]
            if call_type and call_type != "all":
                params.append(call_type)
                clauses.append(f"call_type = ${len(params)}")
            if status:
                params.append(status)
                clauses.append(f"metadata->>'status' = ${len(params)}")
            where = " AND ".join(clauses)

            async with self._pool.acquire() as conn:
                total = await conn.fetchval(
                    f"SELECT count(*) FROM llm_requests WHERE {where}", *params
                )
                total = total or 0
                off_n = len(params) + 1
                lim_n = len(params) + 2
                rows = await conn.fetch(
                    f"SELECT id, job_id, timestamp, model, iteration, latency_ms, response, "
                    f"metrics->'token_usage' AS token_usage, "
                    f"metrics->'usage_metadata' AS usage_metadata, call_type, "
                    f"metadata->>'status' AS status, metadata->'error' AS error "
                    f"FROM llm_requests WHERE {where} "
                    f"ORDER BY timestamp, id OFFSET ${off_n} LIMIT ${lim_n}",
                    *params,
                    offset,
                    limit,
                )
            entries = []
            for r in rows:
                doc: Dict[str, Any] = {
                    "id": r["id"],
                    "job_id": str(r["job_id"]),
                    "timestamp": _to_iso_utc(r["timestamp"]),
                    "model": r["model"],
                    "token_usage": _normalize_token_usage(
                        r["token_usage"], r["usage_metadata"]
                    ),
                    "call_type": r["call_type"] or "main",
                }
                if r["iteration"] is not None:
                    doc["iteration"] = r["iteration"]
                if r["latency_ms"] is not None:
                    doc["latency_ms"] = r["latency_ms"]
                if r["status"] is not None:
                    doc["status"] = r["status"]
                if r["error"] is not None:
                    doc["error"] = r["error"]
                response = r["response"] if isinstance(r["response"], dict) else {}
                doc["tool_calls"] = [
                    {"name": tc.get("name", "?")}
                    if isinstance(tc, dict)
                    else {"name": str(tc)}
                    for tc in (response.get("tool_calls") or [])
                ]
                entries.append(doc)
            return {
                "entries": entries,
                "total": total,
                "offset": offset,
                "limit": limit,
                "hasMore": (offset + limit) < total,
            }
        except Exception as e:
            logger.warning(f"list_llm_requests failed: {e}")
            return empty

    async def list_claim_timings(self, job_id: str) -> List[Dict[str, Any]]:
        """Return worker ``claim_timing`` payloads in claim timestamp order."""

        if not self._available or self._pool is None:
            return []
        try:
            async with self._pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT timestamp, payload FROM agent_audit "
                    "WHERE job_id=$1::uuid AND event_phase='pre' "
                    "AND step_type='claim_timing' ORDER BY timestamp, id",
                    job_id,
                )
            return [
                {
                    "timestamp": _to_iso_utc(row["timestamp"]),
                    "payload": (
                        row["payload"] if isinstance(row["payload"], dict) else {}
                    ),
                }
                for row in rows
            ]
        except Exception as e:
            logger.warning(f"list_claim_timings failed: {e}")
            return []

    # -- cache-invalidation version ----------------------------------------

    async def get_job_version(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Single race-free statement (replaces 4 sequential queries).

        Exposed counts are logical (pre rows); the hash gains a 4th component
        (raw row count incl. post rows) so in-place result arrivals invalidate
        the cockpit cache — closing an in-place-update staleness hole.
        """
        if not self._available or self._pool is None:
            return None
        try:
            async with self._pool.acquire() as conn:
                r = await conn.fetchrow(
                    f"""
                    SELECT
                      count(*) FILTER (WHERE event_phase='pre') AS audit_count,
                      count(*) FILTER (
                          WHERE event_phase='pre' AND step_type='tool'
                          AND payload->'tool'->>'name' = ANY($2)) AS graph_count,
                      count(*) AS row_count,
                      ({_LAST_ACTIVITY_SQL}) AS last_ts,
                      (SELECT count(*) FROM chat_history
                        WHERE job_id=$1::uuid) AS chat_count
                    FROM agent_audit WHERE job_id = $1::uuid
                    """,
                    job_id,
                    GRAPH_TOOL_NAMES,
                )
            audit_count = r["audit_count"] or 0
            if audit_count == 0:
                return None
            chat_count = r["chat_count"] or 0
            graph_count = r["graph_count"] or 0
            row_count = r["row_count"] or 0
            return {
                "version": hash((audit_count, chat_count, graph_count, row_count)),
                "auditEntryCount": audit_count,
                "chatEntryCount": chat_count,
                "graphDeltaCount": graph_count,
                "lastUpdate": _to_iso_utc(r["last_ts"]) if r["last_ts"] else None,
            }
        except Exception as e:
            logger.warning(f"get_job_version failed: {e}")
            return None


__all__ = ["AuditStore", "FILTER_MAPPINGS", "FilterCategory"]
