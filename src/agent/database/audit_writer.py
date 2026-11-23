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
- The store being unreachable never disables the writer. A failed connect or
  write marks the store down and the next try waits out a backoff (1 s doubling
  to 30 s); between tries nothing touches the network, so a down store costs a
  caller no time.
- A row that could not be written is kept in a bounded in-memory spool and
  written, in order, once the store answers again. A caller whose row was
  spooled gets a placeholder id (negative), which later rows may reference
  (``request_id``, ``pre_id``); the writer maps it to the real id on replay.
  A replayed row carries ``audit_replayed: true`` (``metrics`` or ``payload``):
  if the first try reached the server before the connection dropped, the row
  exists twice with the same ``timestamp``.
- A row is dropped, never retried, when the server rejects its content
  (SQLSTATE class 22/23/42, except ``23514`` = no partition for its month,
  which creating the partition fixes) or when the spool is full.
- Nothing raises: every facade method logs a warning (with the SQLSTATE) and
  returns ``None``/``False``. What was written, replayed, dropped or is still
  spooled is counted per job (:meth:`SyncAuditWriter.job_stats`), so the
  archiver can leave a receipt that says whether a job's record is complete.
- The spool lives in this process. ``flush()`` drains it before a job ends;
  rows still spooled when the process exits are lost, and the missing receipt
  says so.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import threading
import time
import uuid
from collections import OrderedDict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Deque, Dict, Optional, Tuple
from urllib.parse import quote

import asyncpg

from shared.runtime_actor import audit_metadata_payload, redact_audit_payload_metadata

logger = logging.getLogger(__name__)

# Pool/loop startup must not hang a write indefinitely; a bounded connect budget.
_CONNECT_TIMEOUT_S = 5.0
# Per-write budget. Generous vs a single-row INSERT but bounded so a wedged
# backend can't pin a caller forever (it blocks the event loop on the hot path).
_WRITE_TIMEOUT_S = 15.0
# After a failure the store is left alone for this long, doubling per failure.
_RECONNECT_BASE_S = 1.0
_RECONNECT_MAX_S = 30.0
# What one write may spend on older, spooled rows before its own.
_DRAIN_BUDGET_S = 2.0
# Spool bounds. A model-call row is the request it archives (100 KB and more),
# so the byte bound is the one that binds; past either, new rows are dropped.
_SPOOL_MAX_ROWS = 5000
_SPOOL_MAX_BYTES = 256 * 1024 * 1024
# Placeholder ids remembered for rows that reference them later.
_RESOLVED_MAX = 20000


def _spool_limits() -> Tuple[int, int]:
    """Spool bounds, overridable per deployment (rows, megabytes)."""

    def _int(name: str, default: int) -> int:
        try:
            value = int(os.getenv(name, "") or default)
        except ValueError:
            return default
        return value if value > 0 else default

    return (
        _int("AUDIT_SPOOL_MAX_ROWS", _SPOOL_MAX_ROWS),
        _int("AUDIT_SPOOL_MAX_MB", _SPOOL_MAX_BYTES // (1024 * 1024)) * 1024 * 1024,
    )


def _synchronous_commit() -> str:
    """``synchronous_commit`` for the writer's sessions.

    ``off`` (default) acknowledges a row before it is on disk: a database
    crash can lose the last ~600 ms of acknowledged rows, and the writer never
    learns of it. ``AUDIT_SYNCHRONOUS_COMMIT=on`` waits for the flush, for a
    record that must not lose a row it reported as written.
    """
    value = os.getenv("AUDIT_SYNCHRONOUS_COMMIT", "").strip().lower()
    return value if value in ("on", "off", "local", "remote_write") else "off"


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


def _is_content_error(exc: BaseException) -> bool:
    """Whether the server (or the encoder) rejected the row itself.

    The same row would fail the same way, so it is dropped. Everything else is
    the store being unreachable, restarting or overloaded, and the row waits.
    ``23514`` is a row with no partition for its month: the orchestrator's
    partition maintenance creates it, so that one waits too.
    """
    sqlstate = getattr(exc, "sqlstate", None) or ""
    if sqlstate == "23514":
        return False
    if sqlstate[:2] in ("22", "23", "42"):
        return True
    # asyncpg's client-side DataError (a value it cannot encode) is a ValueError.
    return isinstance(exc, (ValueError, TypeError, KeyError))


def _approx_size(value: Any) -> int:
    """A cheap size for the spool's byte bound (not a serialized length)."""
    if isinstance(value, str):
        return len(value)
    if isinstance(value, dict):
        return 64 + sum(_approx_size(k) + _approx_size(v) for k, v in value.items())
    if isinstance(value, (list, tuple)):
        return 64 + sum(_approx_size(v) for v in value)
    return 16


@dataclass
class _Pending:
    """One write: live, or waiting in the spool for the store to come back."""

    kind: str  # "llm" | "pre" | "post" | "chat"
    job_id: Optional[str]
    args: Dict[str, Any]
    local_id: Optional[int] = None  # placeholder handed to the caller
    size: int = 0


def _new_counts() -> Dict[str, int]:
    return {"written": 0, "replayed": 0, "dropped": 0}


class SyncAuditWriter:
    """Blocking, thread-safe writer backed by a private loop + asyncpg pool."""

    def __init__(self, dsn: str):
        self._dsn = dsn
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._pool: Optional[asyncpg.Pool] = None
        # One writer at a time: rows reach the store in the order they were
        # made, and the spool and its counters need no finer locking.
        self._lock = threading.RLock()
        # Down state: no network before ``_retry_at`` (monotonic seconds).
        self._retry_at = 0.0
        self._backoff = _RECONNECT_BASE_S
        self._down_since: Optional[datetime] = None
        self._last_error: Optional[str] = None
        # Rows waiting for the store, oldest first.
        self._spool: Deque[_Pending] = deque()
        self._spool_bytes = 0
        self._spool_max_rows, self._spool_max_bytes = _spool_limits()
        self._next_local_id = -1
        # placeholder id -> real id (None: the row was dropped).
        self._resolved: "OrderedDict[int, Optional[int]]" = OrderedDict()
        # pre row id (real or placeholder) -> job, for counting post rows.
        self._pre_jobs: "OrderedDict[int, Optional[str]]" = OrderedDict()
        self._counts: Dict[str, Dict[str, Dict[str, int]]] = {}
        self._outages: Dict[str, int] = {}

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
            timeout=_CONNECT_TIMEOUT_S,
            statement_cache_size=0,  # PgBouncer-transaction-mode safe
            server_settings={
                "application_name": "srw-audit-write",
                "synchronous_commit": _synchronous_commit(),
            },
            init=_init_connection,
        )

    def _mark_down(self, exc: BaseException) -> None:
        """Leave the store alone for a backoff; say so once per outage."""
        self._last_error = _fmt_pg_error(exc)
        if self._down_since is None:
            self._down_since = datetime.now(timezone.utc)
            logger.warning(
                "Audit store unreachable (%s) — rows are kept and written when "
                "it answers again",
                self._last_error,
            )
        self._retry_at = time.monotonic() + self._backoff
        self._backoff = min(self._backoff * 2, _RECONNECT_MAX_S)

    def _mark_up(self) -> None:
        if self._down_since is not None:
            down_for = (datetime.now(timezone.utc) - self._down_since).total_seconds()
            logger.info(
                "Audit store reachable again after %.0fs; %d row(s) to replay",
                down_for,
                len(self._spool),
            )
        self._down_since = None
        self._retry_at = 0.0
        self._backoff = _RECONNECT_BASE_S

    def _connect_locked(self) -> bool:
        """Have a pool, or try for one unless a backoff is running."""
        if time.monotonic() < self._retry_at:
            return False
        if self._pool is not None:
            return True
        try:
            if self._loop is None:
                self._loop = asyncio.new_event_loop()
                self._thread = threading.Thread(
                    target=self._run_loop, name="srw-audit-writer", daemon=True
                )
                self._thread.start()
            self._pool = self._submit(self._create_pool(), _CONNECT_TIMEOUT_S + 1.0)
        except Exception as exc:
            self._mark_down(exc)
            return False
        logger.info("Audit writer connected (Postgres audit store)")
        return True

    def _ensure_started(self) -> bool:
        """Whether a write can be attempted right now."""
        with self._lock:
            return self._connect_locked()

    def ensure_ready(self) -> bool:
        """Gate for the archiver's pre-build short-circuit.

        True while the store is down too: the row is then built and spooled.
        False only when the spool can take no more, so a caller does not build
        a row that would be dropped.
        """
        with self._lock:
            if self._connect_locked():
                return True
            return self._spool_has_room()

    def _stop_loop(self) -> None:
        if self._loop is not None and self._loop.is_running():
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread is not None:
            self._thread.join(timeout=5)
        self._loop = None
        self._thread = None

    def _submit(self, coro, timeout: float = _WRITE_TIMEOUT_S):
        """Run ``coro`` on the writer loop from any thread and block for the result."""
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        try:
            return future.result(timeout=timeout)
        except BaseException:
            # A caller that gave up must not leave the coroutine holding one
            # of the pool's two connections.
            future.cancel()
            raise

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

    # -- counters -----------------------------------------------------------

    def _count(self, op: _Pending, outcome: str) -> None:
        job = self._counts.setdefault(op.job_id or "", {})
        job.setdefault(op.kind, _new_counts())[outcome] += 1

    def job_stats(
        self,
        job_id: str,
        *,
        reset: bool = False,
        _exclude: Optional[_Pending] = None,
    ) -> Dict[str, Any]:
        """What this process did with ``job_id``'s rows since the last receipt.

        Per kind (``llm``, ``pre``, ``post``, ``chat``): rows written first
        try, written on replay, dropped. ``spooled``: rows still waiting.
        ``complete`` is True when nothing was dropped and nothing waits.
        """
        with self._lock:
            kinds = {
                kind: dict(counts)
                for kind, counts in (self._counts.get(job_id) or {}).items()
            }
            spooled = sum(
                1 for op in self._spool if op.job_id == job_id and op is not _exclude
            )
            dropped = sum(counts["dropped"] for counts in kinds.values())
            stats: Dict[str, Any] = {
                "rows": kinds,
                "spooled": spooled,
                "dropped": dropped,
                "replayed": sum(counts["replayed"] for counts in kinds.values()),
                "complete": dropped == 0 and spooled == 0,
                "outages": self._outages.get(job_id, 0),
            }
            if self._last_error and (dropped or spooled or stats["replayed"]):
                stats["last_error"] = self._last_error
            if reset:
                self._counts.pop(job_id, None)
                self._outages.pop(job_id, None)
            return stats

    # -- spool --------------------------------------------------------------

    def _spool_has_room(self) -> bool:
        return (
            len(self._spool) < self._spool_max_rows
            and self._spool_bytes < self._spool_max_bytes
        )

    def _remember(self, table: "OrderedDict", key: int, value: Any) -> None:
        table[key] = value
        while len(table) > _RESOLVED_MAX:
            table.popitem(last=False)

    def _park_locked(self, op: _Pending) -> Any:
        """Keep ``op`` for later; return what its caller gets meanwhile."""
        if not self._spool_has_room():
            self._count(op, "dropped")
            logger.error(
                "Audit spool full (%d rows, %d MB): dropping a %s row of job %s",
                len(self._spool),
                self._spool_bytes // (1024 * 1024),
                op.kind,
                op.job_id,
            )
            return self._failure_value(op)
        if op.size == 0:
            op.size = _approx_size(op.args)
        if op.kind in ("llm", "pre") and op.local_id is None:
            op.local_id = self._next_local_id
            self._next_local_id -= 1
            if op.kind == "pre":
                self._remember(self._pre_jobs, op.local_id, op.job_id)
        if not any(queued.job_id == op.job_id for queued in self._spool):
            self._outages[op.job_id or ""] = self._outages.get(op.job_id or "", 0) + 1
        self._spool.append(op)
        self._spool_bytes += op.size
        if op.kind in ("llm", "pre"):
            return op.local_id
        return True if op.kind == "post" else None

    @staticmethod
    def _failure_value(op: _Pending) -> Any:
        return False if op.kind == "post" else None

    def _resolve(self, ref: Any) -> Tuple[bool, Any]:
        """Map a placeholder id to its row's real id.

        Returns ``(known, id)``: an id from the store passes through; a
        placeholder whose row was dropped, or that is still spooled, is
        ``(False, None)``.
        """
        if not isinstance(ref, int) or ref >= 0:
            return True, ref
        if ref in self._resolved and self._resolved[ref] is not None:
            return True, self._resolved[ref]
        return False, None

    def _run_locked(self, op: _Pending, *, replay: bool) -> Tuple[str, Any]:
        """Try one write. Returns ``("ok", value)``, ``("wait", None)`` when
        the store is not answering, or ``("drop", None)``."""
        args = op.args
        try:
            if op.kind == "llm":
                row = args["row"]
                if replay:
                    row = {
                        **row,
                        "metrics": {
                            **(row.get("metrics") or {}),
                            "audit_replayed": True,
                        },
                    }
                value = self._submit(self._insert_llm_request(row))
            elif op.kind == "pre":
                row = args["row"]
                if args.get("receipt"):
                    # The counts as they are now, when the receipt reaches the
                    # store: every row of the job queued before it has been
                    # written or dropped by then.
                    row = {
                        **row,
                        "payload": {
                            **(row.get("payload") or {}),
                            "audit_receipt": self.job_stats(
                                op.job_id or "", _exclude=op
                            ),
                        },
                    }
                if replay:
                    row = {
                        **row,
                        "payload": {
                            **(row.get("payload") or {}),
                            "audit_replayed": True,
                        },
                    }
                value = self._submit(self._insert_audit_pre(row))
            elif op.kind == "post":
                known, pre_id = self._resolve(args["pre_id"])
                if not known:
                    # Its pre row never reached the store: nothing to attach to.
                    return "drop", None
                _, request_id = self._resolve(args["request_id"])
                payload = args["payload"]
                if replay:
                    payload = {**(payload or {}), "audit_replayed": True}
                value = self._submit(
                    self._insert_audit_post(
                        int(pre_id), payload, args["latency_ms"], request_id
                    )
                )
                if not value:
                    # "INSERT 0 0": the store holds no pre row with that id.
                    return "drop", None
            else:  # chat
                row = args["row"]
                _, request_id = self._resolve(row.get("request_id"))
                value = self._submit(
                    self._insert_chat_entry({**row, "request_id": request_id})
                )
        except Exception as exc:
            if _is_content_error(exc):
                logger.warning(
                    "Audit %s row rejected, dropped (%s)", op.kind, _fmt_pg_error(exc)
                )
                self._last_error = _fmt_pg_error(exc)
                return "drop", None
            self._mark_down(exc)
            return "wait", None
        return "ok", value

    def _settle(self, op: _Pending, outcome: str, value: Any, *, replay: bool) -> None:
        """Book one finished write: counters and the placeholder map."""
        if op.args.get("receipt"):
            if outcome == "ok":
                # The receipt holds the counts; the next one starts from zero.
                self._counts.pop(op.job_id or "", None)
                self._outages.pop(op.job_id or "", None)
            if op.local_id is not None:
                self._remember(
                    self._resolved, op.local_id, value if outcome == "ok" else None
                )
            return
        if outcome == "ok":
            self._count(op, "replayed" if replay else "written")
            if op.local_id is not None:
                self._remember(self._resolved, op.local_id, value)
            if op.kind == "pre" and isinstance(value, int):
                self._remember(self._pre_jobs, value, op.job_id)
        else:
            self._count(op, "dropped")
            if op.local_id is not None:
                self._remember(self._resolved, op.local_id, None)

    def _drain_locked(self, budget_s: float) -> bool:
        """Write spooled rows, oldest first, for at most ``budget_s``.

        Returns True when the spool is empty.
        """
        deadline = time.monotonic() + budget_s
        while self._spool:
            if time.monotonic() >= deadline or not self._connect_locked():
                return False
            op = self._spool[0]
            outcome, value = self._run_locked(op, replay=True)
            if outcome == "wait":
                return False
            if self._down_since is not None:
                self._mark_up()  # the store answered: says how many rows wait
            self._spool.popleft()
            self._spool_bytes -= op.size
            self._settle(op, outcome, value, replay=True)
        return True

    def _write(self, op: _Pending) -> Any:
        """Write ``op`` after everything spooled before it, or spool it."""
        with self._lock:
            if self._spool and not self._drain_locked(_DRAIN_BUDGET_S):
                return self._park_locked(op)
            if not self._connect_locked():
                return self._park_locked(op)
            outcome, value = self._run_locked(op, replay=False)
            if outcome == "wait":
                return self._park_locked(op)
            self._settle(op, outcome, value, replay=False)
            if outcome == "ok":
                if self._down_since is not None:
                    self._mark_up()
                return value
            return self._failure_value(op)

    def flush(self, timeout_s: float = 30.0) -> bool:
        """Write every spooled row, waiting out the backoff, for ``timeout_s``.

        For the end of a job: what is still spooled when the process exits is
        lost. Blocks the calling thread (call it off the event loop). Returns
        True when nothing is left.
        """
        deadline = time.monotonic() + max(0.0, timeout_s)
        while True:
            with self._lock:
                if not self._spool:
                    return True
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                if self._drain_locked(min(remaining, _DRAIN_BUDGET_S)):
                    return True
                wait = max(0.05, self._retry_at - time.monotonic())
            time.sleep(min(wait, max(0.0, deadline - time.monotonic()), 1.0))

    @property
    def spooled(self) -> int:
        """Rows waiting for the store."""
        return len(self._spool)

    # -- write surface ------------------------------------------------------

    def insert_llm_request(self, row: dict) -> Optional[int]:
        return self._write(
            _Pending("llm", str(row.get("job_id") or "") or None, {"row": row})
        )

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
        return self._write(
            _Pending("pre", str(row.get("job_id") or "") or None, {"row": row})
        )

    def insert_job_receipt(self, row: dict) -> Optional[int]:
        """Write ``row`` (an ``agent_audit`` row) as a job's audit receipt.

        The writer adds ``payload.audit_receipt``: the job's counts
        (:meth:`job_stats`) at the moment the row reaches the store, which is
        after every row of the job queued before it. So a receipt that exists
        is final for the rows before it: ``complete`` is whether any was
        dropped. A job without a receipt has a record of unknown completeness.
        Writing it starts the job's counts afresh.
        """
        return self._write(
            _Pending(
                "pre",
                str(row.get("job_id") or "") or None,
                {"row": row, "receipt": True},
            )
        )

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
        with self._lock:
            job_id = self._pre_jobs.get(int(pre_id))
        return bool(
            self._write(
                _Pending(
                    "post",
                    job_id,
                    {
                        "pre_id": int(pre_id),
                        "payload": payload,
                        "latency_ms": latency_ms,
                        "request_id": request_id,
                    },
                )
            )
        )

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
        self._write(
            _Pending("chat", str(row.get("job_id") or "") or None, {"row": row})
        )

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
