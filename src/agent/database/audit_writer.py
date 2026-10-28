"""Thread-safe synchronous write seam for the Postgres audit store.

The agent's :class:`~agent.core.archiver.LLMArchiver` calls its persistence methods
**synchronously and un-awaited** from two contexts: async graph nodes (on the
event-loop thread) *and* real ``ThreadPoolExecutor`` threads (vision/audio via
``run_async``). Several of those calls must also return a server-generated
``BIGINT`` id synchronously (the graph threads ``request_id`` / ``audit_doc_id``
between the pre and post phases). asyncpg is async-only, so this class owns a
**single daemon thread running a private event loop + the asyncpg write pool**,
and exposes blocking, thread-safe facade methods that marshal coroutines onto
that loop via ``run_coroutine_threadsafe(...).result(timeout=...)``.

Blocking the caller for one INSERT round-trip keeps the same latency class as
the fire-and-forget write it replaces, so no call site changes.

Design: ``knowledge-base/knowledge/features/postgres_audit_store_implementation.md`` §5. Counterpart
reader is ``src/orchestrator/database/audit_store.py`` (PR 3); the schema is
``migrations/audit/0001_initial.sql``.

Error contract:
- One-shot connect gate: the first failure to stand up the loop/pool (5 s
  timeout) **permanently disables** writing for the process — mirrors the
  archiver's old ``_connection_attempted`` latch.
- Every facade method swallows all exceptions → ``logger.warning`` (including the
  SQLSTATE so ``23514`` = missing partition is greppable) → ``None``/``False``.
- The writer is never closed in production (daemon thread dies with the process;
  every call blocks, so no writes can be pending at exit). ``close()`` exists for
  tests only.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import threading
import uuid
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import quote

import asyncpg

from shared.runtime_actor import audit_metadata_payload, redact_audit_payload_metadata

logger = logging.getLogger(__name__)

# Pool/loop startup must not hang the first write indefinitely; a bounded
# one-shot connect budget.
_CONNECT_TIMEOUT_S = 5.0
# Per-write budget. Generous vs a single-row INSERT but bounded so a wedged
# backend can't pin a caller forever (it blocks the event loop on the hot path).
_WRITE_TIMEOUT_S = 15.0


def _build_audit_dsn() -> Optional[str]:
    """Build the audit DSN from ``AUDIT_POSTGRES_*`` (preferred) or ``AUDIT_DB_URL``.

    Mirrors ``orchestrator/utils/db_url.build_postgres_url('AUDIT_POSTGRES',
    fallback_env='AUDIT_DB_URL')`` — kept independent so the agent process need
    not import the orchestrator package.
    """
    user = os.getenv("AUDIT_POSTGRES_USER")
    password = os.getenv("AUDIT_POSTGRES_PASSWORD")
    if user and password:
        host = os.getenv("AUDIT_POSTGRES_HOST")
        port = os.getenv("AUDIT_POSTGRES_PORT") or "5432"
        db = os.getenv("AUDIT_POSTGRES_DB")
        if host and db:
            return (
                f"postgresql://{quote(user, safe='')}:"
                f"{quote(password, safe='')}@{host}:{port}/{db}"
            )
    return os.getenv("AUDIT_DB_URL")


def _json_default(obj):
    """Backstop encoder for JSONB params (the archiver already pre-serializes)."""
    if isinstance(obj, datetime):
        dt = (
            obj.astimezone(timezone.utc)
            if obj.tzinfo
            else obj.replace(tzinfo=timezone.utc)
        )
        return dt.isoformat().replace("+00:00", "Z")
    if isinstance(obj, uuid.UUID):
        return str(obj)
    return str(obj)


# A JSON ``\u0000`` escape that is not itself an escaped backslash: an odd run
# of backslashes before ``u0000``.
_JSON_NUL_ESCAPE = re.compile(r"(?<!\\)((?:\\\\)*)\\u0000")


def _encode_jsonb(value) -> str:
    r"""``json.dumps`` for a JSONB param, with every U+0000 made U+FFFD.

    Postgres ``jsonb`` rejects the ``\u0000`` escape ``json.dumps`` writes for a
    NUL character, and tool output (a binary file read, a terminal capture)
    carries them. The failed INSERT was swallowed by the facade, so the whole
    ``llm_requests`` / ``chat_history`` row was silently lost (N62). Replacing
    on the encoded text also covers keys and ``_json_default``'s ``str()``
    fallbacks; the substring check keeps the common case a single scan.
    """
    text = json.dumps(value, default=_json_default)
    if "\\u0000" in text:
        text = _JSON_NUL_ESCAPE.sub(r"\1\\ufffd", text)
    return text


async def _init_connection(conn: asyncpg.Connection) -> None:
    """Register a text JSONB codec so dicts go in and come back out as dicts."""
    await conn.set_type_codec(
        "jsonb",
        encoder=_encode_jsonb,
        decoder=json.loads,
        schema="pg_catalog",
        format="text",
    )


_SQL_INSERT_LLM = """
INSERT INTO llm_requests (job_id, agent_type, call_type, model, iteration,
                          timestamp, latency_ms, request, response, metadata,
                          auxiliary_metadata, metrics)
VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
RETURNING id
"""

_SQL_INSERT_AUDIT_PRE = """
INSERT INTO agent_audit (job_id, agent_type, iteration, step_type, node_name,
                         phase, phase_number, timestamp, latency_ms, payload,
                         metadata)
VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
RETURNING id
"""

# Derives job_id + descriptive columns from the pre row (the update_* callers
# carry only the pre row's id, not job_id). Inserts 0 rows when the pre row is
# missing → "INSERT 0 0", which the facade maps to False (no row updated).
_SQL_INSERT_AUDIT_POST = """
INSERT INTO agent_audit (job_id, agent_type, iteration, step_type, node_name,
                         phase, phase_number, event_phase, pre_id, request_id,
                         latency_ms, payload)
SELECT p.job_id, p.agent_type, p.iteration, p.step_type, p.node_name, p.phase,
       p.phase_number, 'post', p.id, $2, $3, $4
FROM agent_audit p
WHERE p.id = $1 AND p.event_phase = 'pre'
"""

_SQL_INSERT_CHAT = """
INSERT INTO chat_history (job_id, agent_type, iteration, model, timestamp,
                          latency_ms, phase, phase_number, request_id, inputs,
                          response, reasoning)
VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
"""


def _fmt_pg_error(exc: Exception) -> str:
    """Error string including the SQLSTATE when present (23514 = missing partition)."""
    sqlstate = getattr(exc, "sqlstate", None)
    return (
        f"{type(exc).__name__}[{sqlstate}]: {exc}"
        if sqlstate
        else f"{type(exc).__name__}: {exc}"
    )


class SyncAuditWriter:
    """Blocking, thread-safe writer backed by a private loop + asyncpg pool."""

    def __init__(self, dsn: str):
        self._dsn = dsn
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._pool: Optional[asyncpg.Pool] = None
        self._disabled = False  # one-shot connect gate
        self._lock = threading.Lock()

    @classmethod
    def from_env(cls) -> Optional["SyncAuditWriter"]:
        """Return a writer if the audit DSN is configured, else ``None``.

        When the audit DSN is unconfigured, ``None`` here means
        ``get_archiver()`` returns ``None`` and all guarded call sites no-op
        (so unconfigured-under-pytest stays DB-free).
        """
        dsn = _build_audit_dsn()
        if not dsn:
            logger.debug(
                "Audit writer disabled (AUDIT_POSTGRES_* / AUDIT_DB_URL unset)"
            )
            return None
        return cls(dsn)

    # -- lifecycle ----------------------------------------------------------

    def _run_loop(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    async def _create_pool(self) -> asyncpg.Pool:
        return await asyncpg.create_pool(
            self._dsn,
            min_size=1,
            max_size=2,
            command_timeout=10,
            statement_cache_size=0,  # PgBouncer-transaction-mode safe
            server_settings={
                "application_name": "srw-audit-write",
                "synchronous_commit": "off",  # ≤~600ms loss window, no corruption
            },
            init=_init_connection,
        )

    def _ensure_started(self) -> bool:
        """One-shot: start the loop+pool. First failure disables for the process."""
        if self._pool is not None:
            return True
        if self._disabled:
            return False
        with self._lock:
            if self._pool is not None:
                return True
            if self._disabled:
                return False
            try:
                self._loop = asyncio.new_event_loop()
                self._thread = threading.Thread(
                    target=self._run_loop, name="srw-audit-writer", daemon=True
                )
                self._thread.start()
                fut = asyncio.run_coroutine_threadsafe(self._create_pool(), self._loop)
                self._pool = fut.result(timeout=_CONNECT_TIMEOUT_S)
                logger.info("Audit writer connected (Postgres audit store)")
                return True
            except Exception as exc:
                logger.warning(
                    "Audit writer connect failed (%s) — archiving disabled for "
                    "this process",
                    _fmt_pg_error(exc),
                )
                self._disabled = True
                self._stop_loop()
                return False

    def ensure_ready(self) -> bool:
        """Public gate for the archiver's pre-build short-circuit."""
        return self._ensure_started()

    def _stop_loop(self) -> None:
        if self._loop is not None and self._loop.is_running():
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread is not None:
            self._thread.join(timeout=5)
        self._loop = None
        self._thread = None

    def _submit(self, coro):
        """Run ``coro`` on the writer loop from any thread and block for the result."""
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result(
            timeout=_WRITE_TIMEOUT_S
        )

    def close(self) -> None:
        """Close the pool + stop the loop. Tests only; production never calls it."""
        if self._pool is not None and self._loop is not None:
            try:
                asyncio.run_coroutine_threadsafe(self._pool.close(), self._loop).result(
                    timeout=5
                )
            except Exception:
                pass
        self._pool = None
        self._stop_loop()

    # -- write surface ------------------------------------------------------

    def insert_llm_request(self, row: dict) -> Optional[int]:
        if not self._ensure_started():
            return None
        try:
            return self._submit(self._insert_llm_request(row))
        except Exception as exc:
            logger.warning("Audit insert_llm_request failed (%s)", _fmt_pg_error(exc))
            return None

    async def _insert_llm_request(self, row: dict) -> Optional[int]:
        async with self._pool.acquire() as conn:
            return await conn.fetchval(
                _SQL_INSERT_LLM,
                row["job_id"],
                row.get("agent_type"),
                row.get("call_type", "main"),
                row["model"],
                row.get("iteration"),
                row["timestamp"],
                row.get("latency_ms"),
                row["request"],
                row["response"],
                audit_metadata_payload(row.get("metadata")),
                audit_metadata_payload(row.get("auxiliary_metadata")),
                row.get("metrics") or {},
            )

    def insert_audit_pre(self, row: dict) -> Optional[int]:
        if not self._ensure_started():
            return None
        try:
            return self._submit(self._insert_audit_pre(row))
        except Exception as exc:
            logger.warning("Audit insert_audit_pre failed (%s)", _fmt_pg_error(exc))
            return None

    async def _insert_audit_pre(self, row: dict) -> Optional[int]:
        async with self._pool.acquire() as conn:
            return await conn.fetchval(
                _SQL_INSERT_AUDIT_PRE,
                row["job_id"],
                row.get("agent_type"),
                row.get("iteration"),
                row["step_type"],
                row.get("node_name"),
                row.get("phase"),
                row.get("phase_number"),
                row["timestamp"],
                row.get("latency_ms"),
                redact_audit_payload_metadata(row.get("payload")) or {},
                audit_metadata_payload(row.get("metadata")),
            )

    def insert_audit_post(
        self,
        pre_id: Optional[int],
        payload: dict,
        latency_ms: Optional[int],
        request_id: Optional[int] = None,
    ) -> bool:
        # pre_id=None means there is no pre row to update:
        # no DB touch, False.
        if pre_id is None:
            return False
        if not self._ensure_started():
            return False
        try:
            return self._submit(
                self._insert_audit_post(int(pre_id), payload, latency_ms, request_id)
            )
        except Exception as exc:
            logger.warning("Audit insert_audit_post failed (%s)", _fmt_pg_error(exc))
            return False

    async def _insert_audit_post(
        self, pre_id: int, payload: dict, latency_ms: Optional[int], request_id
    ) -> bool:
        async with self._pool.acquire() as conn:
            status = await conn.execute(
                _SQL_INSERT_AUDIT_POST,
                pre_id,
                request_id,
                latency_ms,
                redact_audit_payload_metadata(payload) or {},
            )
        # "INSERT 0 1" on success, "INSERT 0 0" when the pre row was missing.
        return status.rsplit(" ", 1)[-1] == "1"

    def insert_chat_entry(self, row: dict) -> None:
        if not self._ensure_started():
            return
        try:
            self._submit(self._insert_chat_entry(row))
        except Exception as exc:
            logger.warning("Audit insert_chat_entry failed (%s)", _fmt_pg_error(exc))

    async def _insert_chat_entry(self, row: dict) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                _SQL_INSERT_CHAT,
                row["job_id"],
                row.get("agent_type"),
                row.get("iteration"),
                row.get("model"),
                row["timestamp"],
                row.get("latency_ms"),
                row.get("phase"),
                row.get("phase_number"),
                row.get("request_id"),
                row.get("inputs") or [],
                row.get("response") or {},
                row.get("reasoning"),
            )
