"""PostgreSQL Database Manager with async connection pooling.

This module provides a modern async PostgreSQL interface using asyncpg with:
- Async connection pooling
- Namespace-based operations (jobs, requirements, citations)
- Named query loading from SQL files
- CRUD operations with proper async patterns

Part of Phase 1 database refactoring - see knowledge-base/knowledge/db_refactor.md
"""

import json
import logging
import os
import re
import uuid
from contextlib import asynccontextmanager
from itertools import groupby
from pathlib import Path
from typing import Optional, Any, List, Dict, Tuple

from shared.subagent_parent_authority import (
    ParentExecutionAuthority,
    require_parent_execution_authority,
    require_parent_execution_settlement_authority,
)
from shared.session_subagent_authority import (
    SessionParentAuthority,
    require_session_parent_authority,
)
from shared.row_identity import (
    _THREAD_MSG_ID_NS as _THREAD_MSG_ID_NS,
    _coerce_row_id as _coerce_row_id,
)

try:
    import asyncpg
except ImportError:
    asyncpg = None

logger = logging.getLogger(__name__)

QUERIES_DIR = Path(__file__).parent / "queries" / "postgres"


def _active_run_queue_lease():
    """``(unit_id, lease_token)`` when a stateless-executor lease is active.

    Pinned-lane processes never set the lease ContextVar, so this returns
    ``None`` and every fenced code path below keeps today's exact behavior.
    Imported lazily: ``agent.api.lease_context`` is stdlib-only, but importing
    it at module scope from here would couple import order to ``agent.api``'s
    package init (which pulls the full app tree).
    """
    try:
        from agent.api.lease_context import get_current_lease
    except Exception:  # pragma: no cover - defensive (partial installs)
        return None
    return get_current_lease()


def _active_run_queue_lease_for_thread(thread_id: str):
    """Return the active lease only when it owns ``thread_id`` exactly.

    A mutable lease handle is repointed between claims.  Callers must bind the
    snapshot they fence to the thread whose durable state they are about to
    access; fencing a valid queue row for thread A must never authorize SQL for
    thread B.  The comparison happens before acquiring a connection so this
    mismatch is a fail-closed, zero-SQL path.

    No active handle means the pinned lane and preserves its historical
    unfenced behavior.
    """

    lease = _active_run_queue_lease()
    if lease is None:
        return None

    unit_id, lease_token = lease
    if str(unit_id) == str(thread_id):
        return lease

    from agent.api.lease_context import LeaseLostError, mark_current_lease_lost

    mark_current_lease_lost()
    logger.error(
        "run_queue lease/thread mismatch: unit=%s target_thread=%s token=%s",
        unit_id,
        thread_id,
        lease_token,
    )
    raise LeaseLostError(
        f"run_queue lease for unit {unit_id} cannot access thread {thread_id}"
    )


async def _require_run_queue_fence(conn, lease) -> None:
    """§5.2 exact-lease fence for a stateless persist transaction.

    ``FOR SHARE`` on the run_queue row blocks a concurrent reaper steal until
    this transaction commits, so check-then-write cannot interleave with a
    steal. Zero rows ⇒ the lease is lost ⇒ raise :class:`LeaseLostError` so
    the enclosing transaction rolls back and nothing lands.  It is normally
    the transaction's first SQL statement.  A durable-state table with a
    ``threads`` foreign key first takes its required parent-row authority lock
    to preserve the repository-wide ``threads -> run_queue`` lock order; the
    fence still precedes every mutation/fenced persist statement.

    Torn-turn invariant (stateless_agents.md §5.2): a turn's durable footprint
    spans multiple fenced transactions (per-append message rows, the turn-end
    reconcile, the compaction checkpoint row, completion). Each is fenced
    individually; a steal BETWEEN them leaves a torn turn, and that is
    accepted because sessions rebuild from ``thread_messages`` + the consumed
    watermark alone — a checkpoint-ahead-of-messages tear is converged by the
    next claim's rebuild, and the claim-time skip-if-answered watermark
    prevents the double-answer.
    """
    from agent.api.lease_context import LeaseLostError, mark_current_lease_lost
    from shared.run_queue import fence_lease

    unit_id, lease_token = lease
    ok = await fence_lease(conn, unit_id=unit_id, lease_token=lease_token)
    if not ok:
        mark_current_lease_lost()
        logger.error(
            "run_queue fence rejected: unit=%s token=%s — lease lost, "
            "aborting fenced thread persist",
            unit_id,
            lease_token,
        )
        raise LeaseLostError(
            f"run_queue lease lost for unit {unit_id} (token {lease_token})"
        )


class PostgresDB:
    """PostgreSQL database manager with async connection pooling.

    Provides namespace-based operations for:
    - jobs: Job tracking and management
    - requirements: Requirement storage and queries
    - citations: Citation management

    Example:
        ```python
        db = PostgresDB()
        await db.connect()

        # Jobs are created by the orchestrator, not here — db.jobs is
        # read/update only (see JobsNamespace.create).

        # Get pending jobs
        jobs = await db.jobs.get_pending()

        await db.close()
        ```
    """

    def __init__(
        self,
        connection_string: Optional[str] = None,
        min_connections: int = None,
        max_connections: int = None,
        command_timeout: float = None,
    ):
        """Initialize PostgreSQL database manager.

        Args:
            connection_string: PostgreSQL connection URL. Falls back to DATABASE_URL env var.
            min_connections: Minimum pool size (default: 2)
            max_connections: Maximum pool size (default: 10)
            command_timeout: Query timeout in seconds (default: 60.0)

        Raises:
            ImportError: If asyncpg is not installed
            ValueError: If no connection string provided
        """
        if asyncpg is None:
            raise ImportError(
                "asyncpg is required for PostgreSQL support. "
                "Install it with: pip install asyncpg"
            )

        from shared.db_url import build_postgres_url

        self._connection_string = connection_string or build_postgres_url(
            "POSTGRES",
            fallback_env="DATABASE_URL",
        )
        if not self._connection_string:
            raise ValueError(
                "Database connection string required. Set "
                "POSTGRES_USER + POSTGRES_PASSWORD (with POSTGRES_HOST/PORT/DB "
                "from ConfigMap) or DATABASE_URL, or pass connection_string."
            )

        self._min_connections = min_connections or int(
            os.getenv("POSTGRES_MIN_CONNECTIONS", "2")
        )
        self._max_connections = max_connections or int(
            os.getenv("POSTGRES_MAX_CONNECTIONS", "10")
        )
        self._command_timeout = command_timeout or 60.0

        self._pool: Optional[asyncpg.Pool] = None
        self._queries: Dict[str, str] = {}  # Cache for loaded queries

        # Initialize namespaces
        # (No citations namespace: citations live in the vector store and are
        # written only through CitationEngine. The old CitationsNamespace here
        # targeted this app DB, which has no citations table — it was dead and
        # would have thrown had anything called it.)
        self.jobs = JobsNamespace(self)
        self.config_overrides = ConfigOverridesNamespace(self)

        logger.info("PostgresDB initialized (not connected yet)")

    async def connect(self) -> None:
        """Establish async connection pool.

        Creates an asyncpg connection pool with configured size and timeout.
        Registers pgvector type codec on each connection if available.
        This method is idempotent - safe to call multiple times.
        """
        if self._pool is None:

            async def _init_connection(conn):
                """Register pgvector type codec on new connections."""
                try:
                    from pgvector.asyncpg import register_vector

                    await register_vector(conn)
                except (ImportError, ValueError):
                    pass  # pgvector not installed or extension not on this DB

            self._pool = await asyncpg.create_pool(
                self._connection_string,
                min_size=self._min_connections,
                max_size=self._max_connections,
                command_timeout=self._command_timeout,
                init=_init_connection,
            )
            logger.info(
                f"PostgreSQL connection pool established "
                f"(min={self._min_connections}, max={self._max_connections})"
            )

    async def close(self) -> None:
        """Close connection pool.

        Gracefully closes all connections in the pool.
        This method is idempotent - safe to call multiple times.
        """
        if self._pool:
            await self._pool.close()
            self._pool = None
            logger.info("PostgreSQL connection pool closed")

    @asynccontextmanager
    async def acquire(self):
        """Acquire a connection from the pool.

        Context manager for getting a connection from the pool.
        Connection is automatically returned when context exits.

        Yields:
            asyncpg.Connection: Database connection

        Raises:
            RuntimeError: If not connected to database
        """
        if self._pool is None:
            raise RuntimeError("Not connected to database. Call connect() first.")
        async with self._pool.acquire() as conn:
            yield conn

    async def execute(self, query: str, *args) -> str:
        """Execute a query without returning results.

        Args:
            query: SQL query string with $1, $2, etc. placeholders
            *args: Query parameters

        Returns:
            Command status string (e.g., "UPDATE 1")
        """
        async with self.acquire() as conn:
            return await conn.execute(query, *args)

    async def fetch(self, query: str, *args) -> List[asyncpg.Record]:
        """Fetch multiple rows.

        Args:
            query: SQL query string with $1, $2, etc. placeholders
            *args: Query parameters

        Returns:
            List of Record objects
        """
        async with self.acquire() as conn:
            return await conn.fetch(query, *args)

    async def fetchrow(self, query: str, *args) -> Optional[asyncpg.Record]:
        """Fetch a single row.

        Args:
            query: SQL query string with $1, $2, etc. placeholders
            *args: Query parameters

        Returns:
            Single Record or None if no results
        """
        async with self.acquire() as conn:
            return await conn.fetchrow(query, *args)

    async def fetchval(self, query: str, *args) -> Any:
        """Fetch a single value.

        Args:
            query: SQL query string with $1, $2, etc. placeholders
            *args: Query parameters

        Returns:
            Single value from first column of first row
        """
        async with self.acquire() as conn:
            return await conn.fetchval(query, *args)

    def _load_query(self, filename: str, query_name: str) -> str:
        """Load a named query from a .sql file.

        Queries are cached after first load. Query files use the format:

        ```sql
        -- name: query_name
        SELECT ...;

        -- name: another_query
        SELECT ...;
        ```

        Args:
            filename: SQL file name (e.g., "complex.sql")
            query_name: Name of the query to load

        Returns:
            SQL query string

        Raises:
            ValueError: If query not found in file
        """
        cache_key = f"{filename}:{query_name}"
        if cache_key in self._queries:
            return self._queries[cache_key]

        file_path = QUERIES_DIR / filename
        if not file_path.exists():
            raise ValueError(f"Query file not found: {file_path}")

        content = file_path.read_text()

        # Parse named queries: -- name: query_name
        pattern = r"--\s*name:\s*(\w+)\s*\n(.*?)(?=--\s*name:|\Z)"
        matches = re.findall(pattern, content, re.DOTALL)

        for name, sql in matches:
            self._queries[f"{filename}:{name}"] = sql.strip()

        if cache_key not in self._queries:
            raise ValueError(f"Query '{query_name}' not found in {filename}")

        return self._queries[cache_key]

    @staticmethod
    def _row_to_dict(row: Optional[asyncpg.Record]) -> Optional[Dict[str, Any]]:
        """Convert asyncpg Record to dictionary.

        Args:
            row: asyncpg Record or None

        Returns:
            Dictionary with column names as keys, or None if row is None
        """
        if row is None:
            return None
        return dict(row)

    @property
    def is_connected(self) -> bool:
        """Check if connected to database."""
        return self._pool is not None

    async def get_thread(self, thread_id: str) -> Optional[Dict[str, Any]]:
        """Get thread by ID."""
        row = await self.fetchrow(
            "SELECT * FROM threads WHERE id = $1",
            thread_id,
        )
        return self._row_to_dict(row)

    async def end_thread(self, thread_id: str) -> None:
        """Legacy non-pinned fallback; pinned End is orchestrator-owned."""
        async with self.acquire() as conn:
            await conn.execute(
                """
                UPDATE threads
                SET status   = 'ended',
                    ended_at = CURRENT_TIMESTAMP,
                    control_admission_agent_id = NULL
                WHERE id = $1
                  AND execution_lane <> 'pinned'
                """,
                thread_id,
            )

    async def update_thread_status(self, thread_id: str, status: str) -> bool:
        """Update a live thread status without reviving an ended row."""
        async with self.acquire() as conn:
            result = await conn.execute(
                """
                UPDATE threads
                SET status        = $2,
                    last_activity = CURRENT_TIMESTAMP,
                    control_admission_agent_id = CASE
                        WHEN $2 IN ('ended', 'suspended') THEN NULL
                        ELSE control_admission_agent_id
                    END
                WHERE id = $1
                  AND (status <> 'ended' OR $2 = 'ended')
                  AND NOT (
                      execution_lane = 'pinned'
                      AND $2 IN ('ending', 'ended', 'suspended')
                  )
                """,
                thread_id,
                status,
            )
        return result == "UPDATE 1"

    # The subagent read shape — the same columns the orchestrator's
    # ``get_subagent_thread_by_call`` / ``list_subagent_threads`` return, so
    # a replayed envelope and a cockpit row are built from the same facts.
    _SUBAGENT_THREAD_COLUMNS = (
        "id, kind, parent_job_id, parent_thread_id, parent_tool_call_id, "
        "subagent_handle, subagent_type, subagent_status, subagent_outcome, "
        "subagent_error, report_path, status, title, total_turns, total_tokens, "
        "runtime_generation, metadata, created_at, last_activity, ended_at"
    )

    async def parent_execution_authority_current(
        self, parent_authority: ParentExecutionAuthority
    ) -> bool:
        """Await an exact DB authority probe immediately before external I/O.

        ``True`` is the sole admitted result.  A stale execution raises
        :class:`ParentExecutionAuthorityRefused`; connection/query failures
        propagate separately, so an outage can never masquerade as a current
        or empty parent.  The immutable captured authority is the only input.
        """

        async with self.acquire() as conn:
            async with conn.transaction():
                await require_parent_execution_authority(
                    conn,
                    parent_authority,
                    parent_job_id=parent_authority.parent_job_id,
                    mutation=False,
                )
        return True

    async def parent_execution_settlement_authority_current(
        self, parent_authority: ParentExecutionAuthority
    ) -> bool:
        """Prove this exact worker can finish already-admitted child writes."""

        async with self.acquire() as conn:
            async with conn.transaction():
                await require_parent_execution_settlement_authority(
                    conn,
                    parent_authority,
                    parent_job_id=parent_authority.parent_job_id,
                    mutation=False,
                )
        return True

    async def update_subagent_thread(
        self,
        thread_id: str,
        *,
        parent_job_id: str,
        parent_authority: ParentExecutionAuthority,
        runtime_generation: str,
        status: Optional[str] = None,
        subagent_status: Optional[str] = None,
        outcome: Optional[str] = None,
        turns: Optional[int] = None,
        tokens: Optional[int] = None,
        report_path: Optional[str] = None,
        error: Optional[str] = None,
        ended: bool = False,
    ) -> bool:
        """Record a subagent child's lifecycle on its ``threads`` row (U3 B.1).

        Guarded by ``kind = 'subagent'`` and the exact runtime generation so
        this writer can never touch a session row or a revived successor, and
        deliberately separate from :meth:`update_thread_status`,
        which refuses ``ended`` on the pinned lane because a session's End is
        orchestrator-owned — a child has no pod, no workspace and no agents
        row, so its end is the ledger's to write. Every field is optional and
        a ``None`` leaves the column alone (COALESCE): the runtime sends the
        terminal kind, outcome, counters and report path in one write, the
        cancel path sends what it has. ``ended`` stamps ``ended_at`` once.
        ``total_turns`` here is the child's provider-call count, not the
        session turn counter the message activity bump maintains.
        """
        try:
            generation_uuid = uuid.UUID(str(runtime_generation))
            child_uuid = uuid.UUID(str(thread_id))
            parent_uuid = uuid.UUID(str(parent_job_id))
        except (ValueError, TypeError, AttributeError):
            return False
        async with self.acquire() as conn:
            async with conn.transaction():
                authority_gate = (
                    require_parent_execution_settlement_authority
                    if ended
                    else require_parent_execution_authority
                )
                await authority_gate(
                    conn,
                    parent_authority,
                    parent_job_id=parent_uuid,
                    mutation=True,
                )
                result = await conn.execute(
                    """
                UPDATE threads
                   SET status           = COALESCE($2::text, status),
                       subagent_status  = COALESCE($3::text, subagent_status),
                       subagent_outcome = COALESCE($4::text, subagent_outcome),
                       total_turns      = COALESCE($5::integer, total_turns),
                       total_tokens     = COALESCE($6::integer, total_tokens),
                       report_path      = COALESCE($7::text, report_path),
                       subagent_error   = COALESCE($8::text, subagent_error),
                       ended_at         = CASE
                           WHEN $9::boolean THEN COALESCE(ended_at, CURRENT_TIMESTAMP)
                           ELSE ended_at
                       END,
                       last_activity    = CURRENT_TIMESTAMP
                 WHERE id = $1::uuid
                   AND kind = 'subagent'
                   AND runtime_generation = $10::uuid
                   AND parent_job_id = $11::uuid
                   AND status <> 'ended'
                """,
                    str(child_uuid),
                    status,
                    subagent_status,
                    outcome,
                    turns,
                    tokens,
                    report_path,
                    error,
                    bool(ended),
                    str(generation_uuid),
                    str(parent_uuid),
                )
        return result == "UPDATE 1"

    async def save_subagent_thread_message(
        self,
        thread_id: str,
        *,
        parent_job_id: str,
        parent_authority: ParentExecutionAuthority,
        runtime_generation: str,
        role: str,
        content: Optional[str] = None,
        tool_calls: Optional[Any] = None,
        turn_number: Optional[int] = None,
        metrics: Optional[dict] = None,
        tool_call_id: Optional[str] = None,
        thinking: Optional[str] = None,
        reasoning: Optional[Any] = None,
        tool_results: Optional[Any] = None,
        provider: Optional[str] = None,
        provider_raw: Optional[Any] = None,
        additional_kwargs: Optional[dict] = None,
        response_metadata: Optional[dict] = None,
        id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Persist one child transcript row under parent + generation fences.

        This is intentionally separate from :meth:`save_thread_message`.
        A stateless worker's queue unit is its parent *job*, not the child
        thread; routing it through the session-thread fence is both the wrong
        lock graph and a guaranteed lease/thread mismatch.
        """

        try:
            parent_uuid = uuid.UUID(str(parent_job_id))
            child_uuid = uuid.UUID(str(thread_id))
            generation_uuid = uuid.UUID(str(runtime_generation))
        except (ValueError, TypeError, AttributeError):
            return None
        msg_id = _coerce_row_id(id)
        insert_args = (
            msg_id,
            str(child_uuid),
            role,
            content,
            json.dumps(tool_calls) if tool_calls is not None else None,
            turn_number,
            json.dumps(metrics) if metrics is not None else None,
            tool_call_id,
            thinking,
            json.dumps(reasoning) if reasoning is not None else None,
            json.dumps(tool_results) if tool_results is not None else None,
            provider,
            json.dumps(provider_raw) if provider_raw is not None else None,
            json.dumps(additional_kwargs) if additional_kwargs is not None else None,
            json.dumps(response_metadata) if response_metadata is not None else None,
        )
        async with self.acquire() as conn:
            async with conn.transaction():
                await require_parent_execution_authority(
                    conn,
                    parent_authority,
                    parent_job_id=parent_uuid,
                    mutation=True,
                )
                child = await conn.fetchrow(
                    """
                    SELECT runtime_generation, status
                      FROM threads
                     WHERE id = $1::uuid
                       AND kind = 'subagent'
                       AND parent_job_id = $2::uuid
                     FOR UPDATE
                    """,
                    child_uuid,
                    parent_uuid,
                )
                if (
                    child is None
                    or child["runtime_generation"] != generation_uuid
                    or child["status"] == "ended"
                ):
                    return None
                row = await conn.fetchrow(self._THREAD_MESSAGE_UPSERT_SQL, *insert_args)
                await conn.execute(
                    self._THREAD_ACTIVITY_BUMP_SQL,
                    str(child_uuid),
                    turn_number,
                )
        return {"id": str(row["id"]), "seq": row["seq"]}

    async def save_subagent_thread_messages(
        self,
        thread_id: str,
        *,
        parent_job_id: str,
        parent_authority: ParentExecutionAuthority,
        runtime_generation: str,
        messages: List[Dict[str, Any]],
    ) -> bool:
        """Atomically persist a reminted fork seed before child execution.

        Every message is serialized by ``DbSubagentLedger`` before this call.
        Parent authority and child generation are locked once, then all rows
        upsert and the activity bump commit together.  ``False`` means the
        child is missing/ended/stale; authority refusal and DB outage raise.
        """

        try:
            parent_uuid = uuid.UUID(str(parent_job_id))
            child_uuid = uuid.UUID(str(thread_id))
            generation_uuid = uuid.UUID(str(runtime_generation))
        except (ValueError, TypeError, AttributeError):
            return False

        args: List[Tuple[Any, ...]] = []
        max_turn = 0
        for message in messages:
            turn_number = message.get("turn_number")
            max_turn = max(max_turn, int(turn_number or 0))
            args.append(
                (
                    _coerce_row_id(message.get("id")),
                    str(child_uuid),
                    message["role"],
                    message.get("content"),
                    json.dumps(message.get("tool_calls"))
                    if message.get("tool_calls") is not None
                    else None,
                    turn_number,
                    json.dumps(message.get("metrics"))
                    if message.get("metrics") is not None
                    else None,
                    message.get("tool_call_id"),
                    message.get("thinking"),
                    json.dumps(message.get("reasoning"))
                    if message.get("reasoning") is not None
                    else None,
                    json.dumps(message.get("tool_results"))
                    if message.get("tool_results") is not None
                    else None,
                    message.get("provider"),
                    json.dumps(message.get("provider_raw"))
                    if message.get("provider_raw") is not None
                    else None,
                    json.dumps(message.get("additional_kwargs"))
                    if message.get("additional_kwargs") is not None
                    else None,
                    json.dumps(message.get("response_metadata"))
                    if message.get("response_metadata") is not None
                    else None,
                )
            )

        async with self.acquire() as conn:
            async with conn.transaction():
                await require_parent_execution_authority(
                    conn,
                    parent_authority,
                    parent_job_id=parent_uuid,
                    mutation=True,
                )
                child = await conn.fetchrow(
                    """
                    SELECT runtime_generation, status
                      FROM threads
                     WHERE id = $1::uuid
                       AND kind = 'subagent'
                       AND parent_job_id = $2::uuid
                     FOR UPDATE
                    """,
                    child_uuid,
                    parent_uuid,
                )
                if (
                    child is None
                    or child["runtime_generation"] != generation_uuid
                    or child["status"] == "ended"
                ):
                    return False
                if args:
                    await conn.executemany(self._THREAD_MESSAGE_UPSERT_BATCH_SQL, args)
                    await conn.execute(
                        self._THREAD_ACTIVITY_BUMP_SQL,
                        str(child_uuid),
                        max_turn,
                    )
        return True

    async def list_live_subagent_threads(
        self,
        parent_job_id: str,
        *,
        parent_authority: ParentExecutionAuthority,
    ) -> List[Dict[str, Any]]:
        """Generation-bearing queued/running children recoverable by a parent."""
        try:
            parent_uuid = uuid.UUID(str(parent_job_id))
        except (ValueError, TypeError, AttributeError):
            return []
        async with self.acquire() as conn:
            async with conn.transaction():
                await require_parent_execution_authority(
                    conn,
                    parent_authority,
                    parent_job_id=parent_uuid,
                    mutation=False,
                )
                rows = await conn.fetch(
                    f"""
                SELECT {self._SUBAGENT_THREAD_COLUMNS}
                  FROM threads
                 WHERE kind = 'subagent'
                   AND parent_job_id = $1::uuid
                   AND status IN ('created', 'active')
                   AND subagent_status IN ('queued', 'running')
                 ORDER BY created_at, id
                 FOR SHARE
                """,
                    str(parent_uuid),
                )
        return [dict(row) for row in rows]

    async def get_subagent_thread(
        self,
        parent_job_id: str,
        thread_id: str,
        *,
        parent_authority: ParentExecutionAuthority,
    ) -> Optional[Dict[str, Any]]:
        """Read one child under its parent, including its generation token."""
        try:
            parent_uuid = uuid.UUID(str(parent_job_id))
            child_uuid = uuid.UUID(str(thread_id))
        except (ValueError, TypeError, AttributeError):
            return None
        async with self.acquire() as conn:
            async with conn.transaction():
                await require_parent_execution_authority(
                    conn,
                    parent_authority,
                    parent_job_id=parent_uuid,
                    mutation=False,
                )
                row = await conn.fetchrow(
                    f"""
                SELECT {self._SUBAGENT_THREAD_COLUMNS}
                  FROM threads
                 WHERE id = $1::uuid
                   AND kind = 'subagent'
                   AND parent_job_id = $2::uuid
                 FOR SHARE
                """,
                    str(child_uuid),
                    str(parent_uuid),
                )
        return dict(row) if row else None

    async def get_subagent_thread_by_call(
        self,
        parent_job_id: str,
        parent_tool_call_id: str,
        *,
        parent_authority: ParentExecutionAuthority,
    ) -> Optional[Dict[str, Any]]:
        """The child that answered one ``delegate_agent`` call of a job — the
        rotation-surviving idempotency lookup (newest row when the same call
        was re-run after a hard kill left an earlier one ``running``)."""
        call_id = str(parent_tool_call_id or "").strip()
        if not parent_job_id or not call_id:
            return None
        try:
            # A non-uuid parent (a test host, a bare-metal job id) is "no
            # row", never an asyncpg DataError out of the uuid bind.
            parent_uuid = uuid.UUID(str(parent_job_id))
        except (ValueError, TypeError, AttributeError):
            return None
        async with self.acquire() as conn:
            async with conn.transaction():
                await require_parent_execution_authority(
                    conn,
                    parent_authority,
                    parent_job_id=parent_uuid,
                    mutation=False,
                )
                row = await conn.fetchrow(
                    f"""
                SELECT {self._SUBAGENT_THREAD_COLUMNS}
                  FROM threads
                 WHERE kind = 'subagent'
                   AND parent_job_id = $1::uuid
                   AND parent_tool_call_id = $2
                 ORDER BY created_at DESC, id DESC
                 LIMIT 1
                 FOR SHARE
                """,
                    str(parent_uuid),
                    call_id,
                )
        return self._row_to_dict(row)

    async def get_subagent_thread_by_handle(
        self,
        parent_job_id: str,
        handle: str,
        *,
        parent_authority: ParentExecutionAuthority,
    ) -> Optional[Dict[str, Any]]:
        """Resolve one addressable worker child; duplicate handles fail closed."""

        label = str(handle or "").strip()
        try:
            parent_uuid = uuid.UUID(str(parent_job_id))
        except (ValueError, TypeError, AttributeError):
            return None
        if not label:
            return None
        async with self.acquire() as conn:
            async with conn.transaction():
                await require_parent_execution_authority(
                    conn,
                    parent_authority,
                    parent_job_id=parent_uuid,
                    mutation=False,
                )
                rows = await conn.fetch(
                    f"""
                    SELECT {self._SUBAGENT_THREAD_COLUMNS}
                      FROM threads
                     WHERE kind = 'subagent'
                       AND parent_job_id = $1::uuid
                       AND subagent_handle = $2
                     ORDER BY created_at DESC, id DESC
                     LIMIT 2
                     FOR SHARE
                    """,
                    str(parent_uuid),
                    label,
                )
        if len(rows) > 1:
            raise RuntimeError("subagent handle is ambiguous for this parent")
        return self._row_to_dict(rows[0]) if rows else None

    async def session_parent_authority_current(
        self, parent_authority: SessionParentAuthority
    ) -> bool:
        """Probe one immutable session authority immediately before an effect."""

        async with self.acquire() as conn:
            async with conn.transaction():
                await require_session_parent_authority(
                    conn,
                    parent_authority,
                    parent_thread_id=parent_authority.parent_thread_id,
                )
        return True

    async def update_session_subagent_thread(
        self,
        thread_id: str,
        *,
        parent_thread_id: str,
        parent_authority: SessionParentAuthority,
        runtime_generation: str,
        status: Optional[str] = None,
        subagent_status: Optional[str] = None,
        outcome: Optional[str] = None,
        turns: Optional[int] = None,
        tokens: Optional[int] = None,
        report_path: Optional[str] = None,
        error: Optional[str] = None,
        ended: bool = False,
    ) -> bool:
        """Update a foreground session child under parent + generation fences.

        Background ``queued -> running`` is a local lifecycle update, but its
        terminal state must use the orchestrator's atomic terminal and input-
        delivery transaction.  The SQL therefore permits non-terminal updates
        for either shape and permits ``ended=True`` only for foreground rows.
        """

        try:
            parent_uuid = uuid.UUID(str(parent_thread_id))
            child_uuid = uuid.UUID(str(thread_id))
            generation_uuid = uuid.UUID(str(runtime_generation))
        except (ValueError, TypeError, AttributeError):
            return False
        async with self.acquire() as conn:
            async with conn.transaction():
                await require_session_parent_authority(
                    conn,
                    parent_authority,
                    parent_thread_id=parent_uuid,
                )
                result = await conn.execute(
                    """
                    UPDATE threads
                       SET status           = COALESCE($2::text, status),
                           subagent_status  = COALESCE($3::text, subagent_status),
                           subagent_outcome = COALESCE($4::text, subagent_outcome),
                           total_turns      = COALESCE($5::integer, total_turns),
                           total_tokens     = COALESCE($6::integer, total_tokens),
                           report_path      = COALESCE($7::text, report_path),
                           subagent_error   = COALESCE($8::text, subagent_error),
                           ended_at         = CASE
                               WHEN $9::boolean
                               THEN COALESCE(ended_at, CURRENT_TIMESTAMP)
                               ELSE ended_at
                           END,
                           last_activity    = CURRENT_TIMESTAMP
                     WHERE id = $1::uuid
                       AND kind = 'subagent'
                       AND parent_job_id IS NULL
                       AND parent_thread_id = $11::uuid
                       AND runtime_generation = $10::uuid
                       AND status <> 'ended'
                       AND (
                             NOT $9::boolean
                             OR COALESCE(
                                  metadata->'subagent'->>'run_in_background',
                                  'false'
                                ) = 'false'
                           )
                    """,
                    str(child_uuid),
                    status,
                    subagent_status,
                    outcome,
                    turns,
                    tokens,
                    report_path,
                    error,
                    bool(ended),
                    str(generation_uuid),
                    str(parent_uuid),
                )
                if result == "UPDATE 0" and ended:
                    existing = await conn.fetchrow(
                        """
                        SELECT status, subagent_status, subagent_outcome,
                               total_turns, total_tokens, report_path,
                               subagent_error,
                               COALESCE(
                                   metadata->'subagent'->>'run_in_background',
                                   'false'
                               ) AS run_in_background
                          FROM threads
                         WHERE id = $1::uuid
                           AND kind = 'subagent'
                           AND parent_job_id IS NULL
                           AND parent_thread_id = $2::uuid
                           AND runtime_generation = $3::uuid
                         FOR UPDATE
                        """,
                        child_uuid,
                        parent_uuid,
                        generation_uuid,
                    )
                    expected = (
                        ("status", status),
                        ("subagent_status", subagent_status),
                        ("subagent_outcome", outcome),
                        ("total_turns", turns),
                        ("total_tokens", tokens),
                        ("report_path", report_path),
                        ("subagent_error", error),
                    )
                    if (
                        existing is not None
                        and existing.get("status") == "ended"
                        and existing.get("run_in_background") == "false"
                        and all(
                            supplied is None or existing.get(column) == supplied
                            for column, supplied in expected
                        )
                    ):
                        result = "UPDATE 1"
        return result == "UPDATE 1"

    async def save_session_subagent_thread_message(
        self,
        thread_id: str,
        *,
        parent_thread_id: str,
        parent_authority: SessionParentAuthority,
        runtime_generation: str,
        role: str,
        content: Optional[str] = None,
        tool_calls: Optional[Any] = None,
        turn_number: Optional[int] = None,
        metrics: Optional[dict] = None,
        tool_call_id: Optional[str] = None,
        thinking: Optional[str] = None,
        reasoning: Optional[Any] = None,
        tool_results: Optional[Any] = None,
        provider: Optional[str] = None,
        provider_raw: Optional[Any] = None,
        additional_kwargs: Optional[dict] = None,
        response_metadata: Optional[dict] = None,
        id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Persist one session-child transcript row under exact authority."""

        try:
            parent_uuid = uuid.UUID(str(parent_thread_id))
            child_uuid = uuid.UUID(str(thread_id))
            generation_uuid = uuid.UUID(str(runtime_generation))
        except (ValueError, TypeError, AttributeError):
            return None
        insert_args = (
            _coerce_row_id(id),
            str(child_uuid),
            role,
            content,
            json.dumps(tool_calls) if tool_calls is not None else None,
            turn_number,
            json.dumps(metrics) if metrics is not None else None,
            tool_call_id,
            thinking,
            json.dumps(reasoning) if reasoning is not None else None,
            json.dumps(tool_results) if tool_results is not None else None,
            provider,
            json.dumps(provider_raw) if provider_raw is not None else None,
            json.dumps(additional_kwargs) if additional_kwargs is not None else None,
            json.dumps(response_metadata) if response_metadata is not None else None,
        )
        async with self.acquire() as conn:
            async with conn.transaction():
                await require_session_parent_authority(
                    conn,
                    parent_authority,
                    parent_thread_id=parent_uuid,
                )
                child = await conn.fetchrow(
                    """
                    SELECT runtime_generation, status
                      FROM threads
                     WHERE id = $1::uuid
                       AND kind = 'subagent'
                       AND parent_job_id IS NULL
                       AND parent_thread_id = $2::uuid
                     FOR UPDATE
                    """,
                    child_uuid,
                    parent_uuid,
                )
                if (
                    child is None
                    or str(child["runtime_generation"]) != str(generation_uuid)
                    or child["status"] == "ended"
                ):
                    return None
                row = await conn.fetchrow(self._THREAD_MESSAGE_UPSERT_SQL, *insert_args)
                await conn.execute(
                    self._THREAD_ACTIVITY_BUMP_SQL,
                    str(child_uuid),
                    turn_number,
                )
        return {"id": str(row["id"]), "seq": row["seq"]}

    async def save_session_subagent_thread_messages(
        self,
        thread_id: str,
        *,
        parent_thread_id: str,
        parent_authority: SessionParentAuthority,
        runtime_generation: str,
        messages: List[Dict[str, Any]],
    ) -> bool:
        """Atomically persist a reminted session-child fork seed."""

        try:
            parent_uuid = uuid.UUID(str(parent_thread_id))
            child_uuid = uuid.UUID(str(thread_id))
            generation_uuid = uuid.UUID(str(runtime_generation))
        except (ValueError, TypeError, AttributeError):
            return False
        args: List[Tuple[Any, ...]] = []
        max_turn = 0
        for message in messages:
            turn_number = message.get("turn_number")
            max_turn = max(max_turn, int(turn_number or 0))
            args.append(
                (
                    _coerce_row_id(message.get("id")),
                    str(child_uuid),
                    message["role"],
                    message.get("content"),
                    json.dumps(message.get("tool_calls"))
                    if message.get("tool_calls") is not None
                    else None,
                    turn_number,
                    json.dumps(message.get("metrics"))
                    if message.get("metrics") is not None
                    else None,
                    message.get("tool_call_id"),
                    message.get("thinking"),
                    json.dumps(message.get("reasoning"))
                    if message.get("reasoning") is not None
                    else None,
                    json.dumps(message.get("tool_results"))
                    if message.get("tool_results") is not None
                    else None,
                    message.get("provider"),
                    json.dumps(message.get("provider_raw"))
                    if message.get("provider_raw") is not None
                    else None,
                    json.dumps(message.get("additional_kwargs"))
                    if message.get("additional_kwargs") is not None
                    else None,
                    json.dumps(message.get("response_metadata"))
                    if message.get("response_metadata") is not None
                    else None,
                )
            )
        async with self.acquire() as conn:
            async with conn.transaction():
                await require_session_parent_authority(
                    conn,
                    parent_authority,
                    parent_thread_id=parent_uuid,
                )
                child = await conn.fetchrow(
                    """
                    SELECT runtime_generation, status
                      FROM threads
                     WHERE id = $1::uuid
                       AND kind = 'subagent'
                       AND parent_job_id IS NULL
                       AND parent_thread_id = $2::uuid
                     FOR UPDATE
                    """,
                    child_uuid,
                    parent_uuid,
                )
                if (
                    child is None
                    or str(child["runtime_generation"]) != str(generation_uuid)
                    or child["status"] == "ended"
                ):
                    return False
                if args:
                    await conn.executemany(self._THREAD_MESSAGE_UPSERT_BATCH_SQL, args)
                    await conn.execute(
                        self._THREAD_ACTIVITY_BUMP_SQL,
                        str(child_uuid),
                        max_turn,
                    )
        return True

    async def get_session_subagent_thread_by_call(
        self,
        parent_thread_id: str,
        parent_tool_call_id: str,
        *,
        parent_authority: SessionParentAuthority,
    ) -> Optional[Dict[str, Any]]:
        """Newest child for one session delegation call, under exact authority."""

        call_id = str(parent_tool_call_id or "").strip()
        if not call_id:
            return None
        try:
            parent_uuid = uuid.UUID(str(parent_thread_id))
        except (ValueError, TypeError, AttributeError):
            return None
        async with self.acquire() as conn:
            async with conn.transaction():
                await require_session_parent_authority(
                    conn,
                    parent_authority,
                    parent_thread_id=parent_uuid,
                )
                row = await conn.fetchrow(
                    f"""
                    SELECT {self._SUBAGENT_THREAD_COLUMNS}
                      FROM threads
                     WHERE kind = 'subagent'
                       AND parent_job_id IS NULL
                       AND parent_thread_id = $1::uuid
                       AND parent_tool_call_id = $2
                     ORDER BY created_at DESC, id DESC
                     LIMIT 1
                     FOR SHARE
                    """,
                    str(parent_uuid),
                    call_id,
                )
        return self._row_to_dict(row)

    async def get_session_subagent_thread_by_handle(
        self,
        parent_thread_id: str,
        handle: str,
        *,
        parent_authority: SessionParentAuthority,
    ) -> Optional[Dict[str, Any]]:
        """Resolve one session child by handle under exact parent authority."""

        label = str(handle or "").strip()
        try:
            parent_uuid = uuid.UUID(str(parent_thread_id))
        except (ValueError, TypeError, AttributeError):
            return None
        if not label:
            return None
        async with self.acquire() as conn:
            async with conn.transaction():
                await require_session_parent_authority(
                    conn,
                    parent_authority,
                    parent_thread_id=parent_uuid,
                )
                rows = await conn.fetch(
                    f"""
                    SELECT {self._SUBAGENT_THREAD_COLUMNS}
                      FROM threads
                     WHERE kind = 'subagent'
                       AND parent_job_id IS NULL
                       AND parent_thread_id = $1::uuid
                       AND subagent_handle = $2
                     ORDER BY created_at DESC, id DESC
                     LIMIT 2
                     FOR SHARE
                    """,
                    str(parent_uuid),
                    label,
                )
        if len(rows) > 1:
            raise RuntimeError("session subagent handle is ambiguous for this parent")
        return self._row_to_dict(rows[0]) if rows else None

    async def get_thread_messages_history(
        self,
        thread_id: str,
        limit: Optional[int] = 200,
        offset: int = 0,
        since_turn: Optional[int] = None,
        seq_gt: Optional[int] = None,
        newest_first: bool = False,
        include_provider_raw: bool = False,
        order_by_seq: bool = False,
    ) -> List[Dict[str, Any]]:
        """Load thread message history. Ordered by turn_number, created_at ASC.

        Pass ``limit=None`` to load the entire conversation. Session resume
        uses this: the LLM working context is bounded afterwards by
        ContextManager.ensure_within_limits (token-driven compaction), not by
        truncating stored history. A fixed message cap can slice a parallel
        tool-call batch and orphan a function call, which the Responses API
        rejects with a 400.

        Pass ``since_turn=N`` to load only rows with ``turn_number > N``. The
        resume-from-checkpoint path uses this to skip the pre-checkpoint
        history that the summary row already covers (see
        ``get_latest_compaction_checkpoint``).

        Pass ``seq_gt=S`` to load only rows with ``seq > S``, ordered by ``seq``
        (exact insertion order). This is the message-granular resume cursor: a
        compaction records ``boundary_seq`` = the seq of the last message its
        summary covers, and resume loads ``summary + (seq > boundary_seq)`` — the
        agent's real live tail — instead of whole post-boundary turns. Preferred
        over ``since_turn`` when the checkpoint carries a ``boundary_seq``.

        Pass ``newest_first=True`` (with a ``limit``) for the resume backstop:
        the rows are selected ``seq DESC LIMIT N`` (the **newest** N, not the
        oldest) and returned reversed to chronological order. This bounds the
        restore so one pathological tail (thousands of messages) can't OOM even
        when there's no usable summary/boundary; it is a floor, not the
        mechanism. The caller logs when ``len(result) == limit`` (trimmed).
        """
        # HF-7 thread-read diet: the resume consumers read only
        # role/content/tool_calls/tool_call_id (_db_rows_to_lc_messages) and
        # turn_number (the turn_count restore in _restore_session_messages). The
        # other 10 columns — including the large JSONB reasoning/tool_results/
        # provider_raw/response_metadata/additional_kwargs — were fetched on
        # every resume and never read (the rebuilt AIMessage doesn't carry them).
        # Select only what resume consumes. The seq / turn_number / created_at
        # ORDER BYs below don't require the column in the projection.
        provider_projection = ", message.provider_raw" if include_provider_raw else ""
        query = f"""
            SELECT message.id, message.role, message.content,
                   message.tool_calls, message.tool_call_id,
                   message.turn_number{provider_projection}
            FROM thread_messages AS message
            WHERE message.thread_id = $1
              AND message.role NOT IN ('summary', 'error')
              AND message.rewound_at IS NULL
              AND NOT EXISTS (
                    SELECT 1 FROM thread_input_deliveries AS delivery
                     WHERE delivery.message_id = message.id
                       AND delivery.state IN (
                           'persisted', 'owned', 'queued', 'deferred', 'cancelled'
                       )
                  )
        """
        params: List[Any] = [thread_id]
        if seq_gt is not None:
            params.append(seq_gt)
            query += f"\n              AND seq > ${len(params)}"
        if since_turn is not None:
            params.append(since_turn)
            query += f"\n              AND turn_number > ${len(params)}"
        # newest_first: take the NEWEST `limit` rows (seq DESC), reversed to
        # chronological below — the resume floor. Else order by seq (the seq
        # cursor) or turn/created_at (legacy since_turn / full-load callers).
        if newest_first:
            query += "\n            ORDER BY seq DESC"
        elif seq_gt is not None or order_by_seq:
            query += "\n            ORDER BY seq ASC"
        else:
            query += "\n            ORDER BY turn_number ASC, created_at ASC"
        if limit is not None:
            params.append(limit)
            query += f"\n            LIMIT ${len(params)}"
        params.append(offset)
        query += f"\n            OFFSET ${len(params)}"

        rows = await self.fetch(query, *params)

        def _j(v):
            return json.loads(v) if isinstance(v, (str, bytes)) else v

        result = []
        for row in rows:
            result.append(
                {
                    "id": str(row["id"]),
                    "role": row["role"],
                    "content": row["content"],
                    "tool_calls": _j(row["tool_calls"]) if row["tool_calls"] else None,
                    "tool_call_id": row["tool_call_id"],
                    "turn_number": row["turn_number"],
                    **(
                        {"provider_raw": _j(row["provider_raw"])}
                        if include_provider_raw
                        else {}
                    ),
                }
            )
        if newest_first:
            # Selected newest-first to apply the LIMIT to the tail; hand back
            # chronological so callers (resume) get oldest→newest as usual.
            result.reverse()
        return result

    async def get_latest_compaction_checkpoint(
        self, thread_id: str
    ) -> Optional[Dict[str, Any]]:
        """Return the latest compaction checkpoint for this thread, if any.

        Reads the newest ``role='summary'`` row (which the main history query
        excludes). Used by the resume path: if a checkpoint exists, restore
        ``[summary] + history(since_turn=boundary_turn)`` instead of the full
        log — avoids re-loading hundreds of messages and re-summarizing from
        scratch.

        Rolling compactions merge prior summaries into a single new row, so the
        newest row always carries the cumulative summary. Returns ``None`` when
        no compaction has been persisted yet (back-compat: fall back to full
        load). ``boundary_turn`` may be ``None`` on rows written before the
        checkpoint feature shipped — caller must also fall back in that case.
        """
        query = """
            SELECT content, metrics, turn_number
            FROM thread_messages
            WHERE thread_id = $1
              AND role = 'summary'
              AND rewound_at IS NULL
            ORDER BY turn_number DESC NULLS LAST, created_at DESC
            LIMIT 1
        """
        row = await self.fetchrow(query, thread_id)
        if row is None:
            return None

        metrics_raw = row["metrics"]
        metrics = (
            json.loads(metrics_raw)
            if isinstance(metrics_raw, (str, bytes))
            else metrics_raw
        ) or {}
        return {
            "summary": row["content"] or "",
            "boundary_turn": metrics.get("boundary_turn"),
            "boundary_seq": metrics.get("boundary_seq"),
            "turn_number": row["turn_number"],
        }

    async def get_seq_for_message_id(
        self, thread_id: str, msg_id: str
    ) -> Optional[int]:
        """Return the persisted ``seq`` of a message by its in-memory id.

        Resolves the summarized/kept boundary message
        (``ContextManager._last_compaction_boundary_id``) into a ``boundary_seq``
        for the summary row. The in-memory id is coerced the same way it was on
        write (``_coerce_row_id``), so a provider/minted id resolves to its row.
        Returns ``None`` when the message isn't persisted (a transient injection,
        or a resume-time fresh-id message) — the caller then leaves
        ``boundary_seq`` unset and falls back to ``boundary_turn``.
        """
        row = await self.fetchrow(
            "SELECT seq FROM thread_messages "
            "WHERE thread_id = $1 AND id = $2 AND rewound_at IS NULL",
            thread_id,
            _coerce_row_id(msg_id),
        )
        return row["seq"] if row else None

    async def get_live_message(
        self, thread_id: str, msg_id: str
    ) -> Optional[Dict[str, Any]]:
        """Resolve a message id to its live row (seq/role/content).

        The rewind target lookup: returns ``None`` for unknown ids AND for
        rows already tombstoned by an earlier rewind — a rewound-away message
        is not a valid rewind target.
        """
        row = await self.fetchrow(
            "SELECT seq, role, content FROM thread_messages "
            "WHERE thread_id = $1 AND id = $2 AND rewound_at IS NULL",
            thread_id,
            _coerce_row_id(msg_id),
        )
        if row is None:
            return None
        return {"seq": row["seq"], "role": row["role"], "content": row["content"]}

    async def apply_rewind(
        self,
        thread_id: str,
        from_seq: int,
        mode: str,
        actor: Optional[str] = None,
        abandoned_sha: Optional[str] = None,
        restored_to_sha: Optional[str] = None,
        restore_commit_sha: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Tombstone the tail at ``seq >= from_seq`` and ledger the rewind.

        One transaction: the sweep (skipped for mode='code' — files-only
        rewinds leave the transcript untouched), the ``thread_rewinds``
        ledger insert, the surviving-turn readback the caller uses to reset
        ``turn_count``, and a clamp of the durable memory-extraction cursor to
        that surviving turn. The clamp updates an existing cursor only: a
        thread that has never extracted memory must retain the implicit zero
        baseline. Idempotent re-run sweeps 0 rows (the ``rewound_at IS NULL``
        guard) but does append a second ledger row — callers serialize
        (session loop / advisory lock).
        """
        if mode not in ("both", "conversation", "code"):
            raise ValueError(f"invalid rewind mode: {mode}")
        swept = 0
        lease = _active_run_queue_lease_for_thread(thread_id)
        async with self.acquire() as conn:
            async with conn.transaction():
                # Serialize the rewind with the stateless turn's final fenced
                # transcript/effect transaction. Whichever owns the thread
                # row first wins cleanly: a committed obligation blocks the
                # rewind; a committed rewind makes the boundary update fail
                # and rolls the producer back. Pinned sessions have no queue
                # lease but retain the same thread-row serialization.
                thread_exists = await conn.fetchval(
                    "SELECT 1 FROM threads WHERE id = $1::uuid FOR UPDATE",
                    thread_id,
                )
                if thread_exists is None:
                    raise ValueError("session thread no longer exists")
                if lease is not None:
                    await _require_run_queue_fence(conn, lease)
                if mode in ("both", "conversation"):
                    unfinished = await conn.fetch(
                        self._SESSION_MEMORY_REWIND_GUARD_SQL,
                        thread_id,
                        from_seq,
                    )
                    if unfinished:
                        raise RuntimeError("rewind waits for final-memory extraction")
                    swept = await conn.fetchval(
                        """
                        WITH swept AS (
                            UPDATE thread_messages
                            SET rewound_at = now()
                            WHERE thread_id = $1
                              AND seq >= $2
                              AND rewound_at IS NULL
                            RETURNING 1
                        )
                        SELECT COUNT(*) FROM swept
                        """,
                        thread_id,
                        from_seq,
                    )
                row = await conn.fetchrow(
                    """
                    INSERT INTO thread_rewinds
                        (thread_id, from_seq, mode, actor, swept_count,
                         abandoned_sha, restored_to_sha, restore_commit_sha)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    RETURNING id
                    """,
                    thread_id,
                    from_seq,
                    mode,
                    actor,
                    swept or 0,
                    abandoned_sha,
                    restored_to_sha,
                    restore_commit_sha,
                )
                surviving_turn = await conn.fetchval(
                    """
                    SELECT COALESCE(MAX(turn_number), 0)
                    FROM thread_messages
                    WHERE thread_id = $1
                      AND rewound_at IS NULL
                      AND role NOT IN ('summary', 'error')
                    """,
                    thread_id,
                )
                if mode in ("both", "conversation"):
                    # Rewind and cursor movement are one commit. Without this,
                    # a cursor from the abandoned future (for example 10 after
                    # rewinding to turn 5) suppresses extraction on the rebuilt
                    # timeline until it catches up with that dead history.
                    # Do not INSERT an absent row: absence is the pre-first-
                    # extraction baseline and must continue to behave as zero.
                    await conn.execute(
                        """
                        UPDATE thread_session_runtime_state
                        SET memory_extraction_turn = LEAST(
                                memory_extraction_turn, $2
                            ),
                            updated_at = now()
                        WHERE thread_id = $1
                        """,
                        thread_id,
                        int(surviving_turn or 0),
                    )
        return {
            "rewind_id": str(row["id"]),
            "swept": int(swept or 0),
            "surviving_turn": int(surviving_turn or 0),
        }

    async def resweep_rewind(self, thread_id: str, from_seq: int) -> int:
        """Narrow, idempotent mop-up sweep for stragglers past a rewind.

        `_handle_rewind`'s hard-interrupt wait (up to 60s) can overlap a
        turn-completion INSERT already in flight: a message at/after
        `from_seq` can land *after* `apply_rewind`'s sweep already ran,
        leaving a live, un-tombstoned stray racing the just-truncated
        in-memory view. Same shape and same `rewound_at IS NULL` guard as
        `apply_rewind`'s sweep, so a normal run with no stragglers touches 0
        rows. Deliberately not a second `apply_rewind` call — this must not
        append a second `thread_rewinds` ledger row for what is mop-up of
        the *same* rewind, not a new one.
        """
        async with self.acquire() as conn:
            swept = await conn.fetchval(
                """
                WITH swept AS (
                    UPDATE thread_messages
                    SET rewound_at = now()
                    WHERE thread_id = $1
                      AND seq >= $2
                      AND rewound_at IS NULL
                    RETURNING 1
                )
                SELECT COUNT(*) FROM swept
                """,
                thread_id,
                from_seq,
            )
        return int(swept or 0)

    async def record_turn_commit(self, thread_id: str, commit_sha: str) -> None:
        """Map the workspace commit that just landed to the transcript position.

        seq = MAX(seq) over the thread's rows at commit time (0 before the
        first message — the pre-conversation workspace state, a valid restore
        target for a rewind to the very first message). Two commits at the
        same transcript position (e.g. a compaction checkpoint right after a
        turn commit) collapse to the later SHA — the newest workspace state
        for that position is the correct restore target.
        """
        sql = """
            INSERT INTO thread_turn_commits (thread_id, seq, commit_sha)
            SELECT $1,
                   COALESCE(MAX(seq), 0),
                   $2
            FROM thread_messages
            WHERE thread_id = $1
            ON CONFLICT (thread_id, seq) DO UPDATE
                SET commit_sha = EXCLUDED.commit_sha,
                    created_at = now()
        """
        lease = _active_run_queue_lease_for_thread(thread_id)
        async with self.acquire() as conn:
            if lease is None:
                await conn.execute(sql, thread_id, commit_sha)
                return
            async with conn.transaction():
                # First statement: hold the exact queue row across the mapping
                # upsert so a zombie cannot publish its Git SHA after a steal.
                await _require_run_queue_fence(conn, lease)
                await conn.execute(sql, thread_id, commit_sha)

    async def seed_workspace_baseline_commit(
        self, thread_id: str, commit_sha: str
    ) -> None:
        """Create the immutable pre-first-turn workspace ledger row.

        A first completed turn needs two ledger points for files-only undo:
        its new workspace commit and the workspace HEAD from before that turn.
        Reattaches keep the original baseline (``DO NOTHING``), while the
        active stateless claimant must prove the exact thread lease before the
        insert. ``thread_turn_commits`` has no parent foreign key, so no
        ``threads`` lock is needed and the queue fence remains first SQL.
        """

        sql = """
            INSERT INTO thread_turn_commits (thread_id, seq, commit_sha)
            VALUES ($1, 0, $2)
            ON CONFLICT (thread_id, seq) DO NOTHING
        """
        lease = _active_run_queue_lease_for_thread(thread_id)
        async with self.acquire() as conn:
            if lease is None:
                await conn.execute(sql, thread_id, commit_sha)
                return
            async with conn.transaction():
                await _require_run_queue_fence(conn, lease)
                await conn.execute(sql, thread_id, commit_sha)

    async def resolve_restore_commit(
        self, thread_id: str, before_seq: int
    ) -> Optional[str]:
        """Workspace SHA for 'state before the message at before_seq'.

        The newest mapped commit strictly below the target: the workspace as
        it stood after every turn that survives the rewind. ``None`` = no
        coverage (thread predates the feature) — code restore unavailable.
        """
        return await self.fetchval(
            """
            SELECT commit_sha FROM thread_turn_commits
            WHERE thread_id = $1 AND seq < $2
            ORDER BY seq DESC
            LIMIT 1
            """,
            thread_id,
            before_seq,
        )

    async def list_workspace_turn_commits(self, thread_id: str) -> List[str]:
        """Return the thread's workspace ledger from newest to oldest.

        Undo chooses the previous *distinct Git tree*, not merely the previous
        row: attach reconciliation, read-only turns, and earlier undo effects
        may all map the same file state at different transcript positions.
        Git owns tree equivalence; this fenced method supplies its complete,
        ordered durable candidate chain without trying to infer it in SQL.
        """

        sql = """
            SELECT commit_sha
            FROM thread_turn_commits
            WHERE thread_id = $1
            ORDER BY seq DESC
        """
        lease = _active_run_queue_lease_for_thread(thread_id)
        if lease is None:
            rows = await self.fetch(sql, thread_id)
        else:
            async with self.acquire() as conn:
                async with conn.transaction():
                    # The chain is input to an external workspace mutation.
                    # Hold the exact queue share lock through the complete read;
                    # the Git marker + later fenced mapping make the wider
                    # cross-system operation crash recoverable.
                    await _require_run_queue_fence(conn, lease)
                    rows = await conn.fetch(sql, thread_id)
        return [str(row["commit_sha"]) for row in rows]

    # ------------------------------------------------------------------
    # Durable persistent-session state (migration 0133)
    # ------------------------------------------------------------------

    async def list_session_tasks(self, thread_id: str) -> List[Dict[str, Any]]:
        """Load the authoritative task checklist for one session thread."""

        sql = """
            SELECT task_number, description, status, priority, notes,
                   created_at, completed_at
            FROM thread_session_tasks
            WHERE thread_id = $1
            ORDER BY task_number
        """
        lease = _active_run_queue_lease_for_thread(thread_id)
        if lease is None:
            rows = await self.fetch(sql, thread_id)
        else:
            async with self.acquire() as conn:
                async with conn.transaction():
                    await _require_run_queue_fence(conn, lease)
                    rows = await conn.fetch(sql, thread_id)
        return [dict(row) for row in rows]

    async def create_session_task(
        self,
        thread_id: str,
        description: str,
        priority: str,
    ) -> Dict[str, Any]:
        """Allocate and insert one task under the thread/lease fence."""

        lease = _active_run_queue_lease_for_thread(thread_id)
        async with self.acquire() as conn:
            async with conn.transaction():
                exists = await conn.fetchval(
                    "SELECT 1 FROM threads WHERE id = $1 FOR UPDATE",
                    thread_id,
                )
                if exists is None:
                    raise ValueError("session thread no longer exists")
                if lease is not None:
                    # Global admission order is threads -> run_queue.  Keep the
                    # numbering lock first, then prove the exact lease before
                    # the INSERT; a rejected fence rolls the transaction back.
                    await _require_run_queue_fence(conn, lease)
                row = await conn.fetchrow(
                    """
                    INSERT INTO thread_session_tasks
                        (thread_id, task_number, description, priority)
                    SELECT $1,
                           COALESCE(MAX(task_number), 0) + 1,
                           $2,
                           $3
                    FROM thread_session_tasks
                    WHERE thread_id = $1
                    RETURNING task_number, description, status, priority,
                              notes, created_at, completed_at
                    """,
                    thread_id,
                    description,
                    priority,
                )
        return dict(row)

    async def start_session_task(
        self, thread_id: str, task_number: int
    ) -> Optional[Dict[str, Any]]:
        """Move one pending task to ``in_progress`` under the owner fence."""

        lease = _active_run_queue_lease_for_thread(thread_id)
        async with self.acquire() as conn:
            async with conn.transaction():
                if lease is not None:
                    await _require_run_queue_fence(conn, lease)
                row = await conn.fetchrow(
                    """
                    UPDATE thread_session_tasks
                    SET status = 'in_progress', updated_at = now()
                    WHERE thread_id = $1 AND task_number = $2
                      AND status = 'pending'
                    RETURNING task_number, description, status, priority,
                              notes, created_at, completed_at
                    """,
                    thread_id,
                    task_number,
                )
        return dict(row) if row is not None else None

    async def complete_session_task(
        self,
        thread_id: str,
        task_number: int,
        notes: str = "",
    ) -> Optional[Dict[str, Any]]:
        """Complete one task durably before its tool result is returned."""

        lease = _active_run_queue_lease_for_thread(thread_id)
        async with self.acquire() as conn:
            async with conn.transaction():
                if lease is not None:
                    await _require_run_queue_fence(conn, lease)
                row = await conn.fetchrow(
                    """
                    UPDATE thread_session_tasks
                    SET status = 'completed',
                        completed_at = now(),
                        notes = CASE WHEN $3 = '' THEN notes ELSE $3 END,
                        updated_at = now()
                    WHERE thread_id = $1 AND task_number = $2
                      AND status <> 'completed'
                    RETURNING task_number, description, status, priority,
                              notes, created_at, completed_at
                    """,
                    thread_id,
                    task_number,
                    notes,
                )
        return dict(row) if row is not None else None

    async def claim_memory_extraction_interval(
        self,
        thread_id: str,
        *,
        turn_count: int,
        interval: int,
    ) -> bool:
        """Atomically claim one elapsed interval for memory extraction.

        The cursor advances before the auxiliary call, matching the historical
        in-process writer.  A successor observing the cursor cannot repeat the
        same extraction after a claim handoff or process crash.
        """

        lease = _active_run_queue_lease_for_thread(thread_id)
        if interval <= 0 or turn_count < interval:
            return False
        async with self.acquire() as conn:
            async with conn.transaction():
                if lease is not None:
                    # The INSERT/UPSERT may take a parent FK lock implicitly.
                    # Take it explicitly before run_queue to keep the global
                    # threads -> queue ordering, then fence before the write.
                    exists = await conn.fetchval(
                        "SELECT 1 FROM threads WHERE id = $1 FOR KEY SHARE",
                        thread_id,
                    )
                    if exists is None:
                        raise ValueError("session thread no longer exists")
                    await _require_run_queue_fence(conn, lease)
                row = await conn.fetchrow(
                    """
                    INSERT INTO thread_session_runtime_state
                        (thread_id, memory_extraction_turn)
                    VALUES ($1, $2)
                    ON CONFLICT (thread_id) DO UPDATE
                    SET memory_extraction_turn = EXCLUDED.memory_extraction_turn,
                        updated_at = now()
                    WHERE thread_session_runtime_state.memory_extraction_turn
                          <= EXCLUDED.memory_extraction_turn - $3
                    RETURNING memory_extraction_turn
                    """,
                    thread_id,
                    turn_count,
                    interval,
                )
        return row is not None

    async def list_thread_cloud_anchors(
        self, thread_id: str
    ) -> Dict[str, Dict[str, Any]]:
        """Load durable cloud provenance keyed by logical workspace path."""

        sql = """
            SELECT workspace_path, anchor
            FROM thread_cloud_citation_anchors
            WHERE thread_id = $1
        """
        lease = _active_run_queue_lease_for_thread(thread_id)
        if lease is None:
            rows = await self.fetch(sql, thread_id)
        else:
            async with self.acquire() as conn:
                async with conn.transaction():
                    await _require_run_queue_fence(conn, lease)
                    rows = await conn.fetch(sql, thread_id)
        anchors: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            value = row["anchor"]
            if isinstance(value, str):
                value = json.loads(value)
            if isinstance(value, dict):
                anchors[str(row["workspace_path"])] = value
        return anchors

    async def upsert_thread_cloud_anchor(
        self,
        thread_id: str,
        workspace_path: str,
        anchor: Dict[str, Any],
    ) -> None:
        """Persist one cloud anchor under the current stateless lease fence."""

        lease = _active_run_queue_lease_for_thread(thread_id)
        async with self.acquire() as conn:
            async with conn.transaction():
                if lease is not None:
                    # See claim_memory_extraction_interval: establish parent
                    # authority before queue fencing so the FK cannot invert
                    # admission's threads -> run_queue lock order.
                    exists = await conn.fetchval(
                        "SELECT 1 FROM threads WHERE id = $1 FOR KEY SHARE",
                        thread_id,
                    )
                    if exists is None:
                        raise ValueError("session thread no longer exists")
                    await _require_run_queue_fence(conn, lease)
                await conn.execute(
                    """
                    INSERT INTO thread_cloud_citation_anchors
                        (thread_id, workspace_path, anchor)
                    VALUES ($1, $2, $3::jsonb)
                    ON CONFLICT (thread_id, workspace_path) DO UPDATE
                    SET anchor = EXCLUDED.anchor, updated_at = now()
                    """,
                    thread_id,
                    workspace_path,
                    json.dumps(anchor, sort_keys=True, separators=(",", ":")),
                )

    # Shared by the single-row upsert (RETURNING id, seq) and the turn-complete
    # reconcile batch (same statement minus RETURNING — executemany discards
    # results). Keep the column set in lockstep with the orchestrator's writer.
    _THREAD_MESSAGE_UPSERT_SQL = """
        INSERT INTO thread_messages
            (id, thread_id, role, content, tool_calls, turn_number,
             metrics, tool_call_id, thinking,
             reasoning, tool_results, provider, provider_raw,
             additional_kwargs, response_metadata)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
                $14, $15)
        ON CONFLICT (id) DO UPDATE SET
            content           = EXCLUDED.content,
            tool_calls        = EXCLUDED.tool_calls,
            turn_number       = EXCLUDED.turn_number,
            metrics           = EXCLUDED.metrics,
            tool_call_id      = EXCLUDED.tool_call_id,
            thinking          = EXCLUDED.thinking,
            reasoning         = EXCLUDED.reasoning,
            tool_results      = EXCLUDED.tool_results,
            provider          = EXCLUDED.provider,
            provider_raw      = EXCLUDED.provider_raw,
            additional_kwargs = EXCLUDED.additional_kwargs,
            response_metadata = EXCLUDED.response_metadata
        RETURNING id, seq
    """
    _THREAD_MESSAGE_UPSERT_BATCH_SQL = _THREAD_MESSAGE_UPSERT_SQL.replace(
        "RETURNING id, seq", ""
    )
    # Same columns, insert-if-absent: a batch row flagged ``insert_if_absent``
    # (a lossy compaction view the reconcile still owes when the incremental
    # write was lost) fills a missing row but never rewrites an existing one's
    # content. Only the tool link follows: a mid-turn cross-family model switch
    # remaps tool-call ids in memory on the AI message and its result alike,
    # the AI row upserts the new id, and a result left on the old one would
    # split the stored pair (restore then prunes both halves as orphans).
    _THREAD_MESSAGE_INSERT_IF_ABSENT_BATCH_SQL = (
        _THREAD_MESSAGE_UPSERT_SQL.partition("ON CONFLICT (id)")[0]
        + "ON CONFLICT (id) DO UPDATE SET tool_call_id = EXCLUDED.tool_call_id"
    )
    _THREAD_ACTIVITY_BUMP_SQL = """
        UPDATE threads
        SET last_activity = CURRENT_TIMESTAMP,
            total_turns   = GREATEST(total_turns, COALESCE($2, 0))
        WHERE id = $1
    """
    _THREAD_MESSAGE_PARENT_LOCK_SQL = """
        SELECT 1 FROM threads
        WHERE id = $1::uuid
        FOR KEY SHARE
    """
    _SESSION_MEMORY_PARENT_LOCK_SQL = """
        SELECT project_id, metadata, execution_lane
        FROM threads
        WHERE id = $1::uuid
        FOR UPDATE
    """
    _SESSION_MEMORY_PROJECT_SCOPE_SQL = """
        SELECT 1
        FROM thread_mounts
        WHERE thread_id = $1::uuid
          AND source_ref = $2::uuid
          AND mount_kind IN ('project', 'project_default')
        FOR KEY SHARE
    """
    _SESSION_TURN_EXECUTION_SQL = """
        UPDATE thread_messages
        SET turn_execution_id = COALESCE(
                turn_execution_id,
                uuid_generate_v4()
            )
        WHERE thread_id = $1::uuid
          AND id = $2::uuid
          AND turn_number = $3
          AND rewound_at IS NULL
        RETURNING turn_execution_id, seq
    """
    _SESSION_TURN_END_SEQ_SQL = """
        SELECT max(seq)
        FROM thread_messages
        WHERE thread_id = $1::uuid
          AND id = ANY($2::uuid[])
          AND rewound_at IS NULL
    """
    _SESSION_MEMORY_EFFECT_INSERT_SQL = """
        WITH inserted AS (
            INSERT INTO completion_effects
                (producer_kind, producer_id, scope_id, effect_name,
                 effect_group, detail)
            VALUES
                ('session_turn', $2::uuid, $1::uuid,
                 'final_memory_extraction', 'memory_extraction',
                 jsonb_build_object(
                     'input_message_id', $3::uuid,
                     'turn_number', $4::integer,
                     'memory_scope_kind', $5::text,
                     'memory_scope_id', $6::uuid,
                     'boundary_seq', $7::bigint,
                     'end_seq', $8::bigint
                 ))
            ON CONFLICT (producer_kind, producer_id, effect_name) DO NOTHING
            RETURNING producer_id
        )
        SELECT producer_id FROM inserted
        UNION ALL
        SELECT producer_id
        FROM completion_effects
        WHERE producer_kind = 'session_turn'
          AND producer_id = $2::uuid
          AND scope_id = $1::uuid
          AND effect_name = 'final_memory_extraction'
          AND effect_group = 'memory_extraction'
          AND detail = jsonb_build_object(
              'input_message_id', $3::uuid,
              'turn_number', $4::integer,
              'memory_scope_kind', $5::text,
              'memory_scope_id', $6::uuid,
              'boundary_seq', $7::bigint,
              'end_seq', $8::bigint
          )
        LIMIT 1
    """
    _SESSION_MEMORY_REWIND_GUARD_SQL = """
        SELECT effect.producer_id
        FROM completion_effects AS effect
        JOIN thread_messages AS boundary
          ON boundary.thread_id = effect.scope_id
         AND boundary.turn_execution_id = effect.producer_id
        WHERE effect.producer_kind = 'session_turn'
          AND effect.effect_name = 'final_memory_extraction'
          AND effect.scope_id = $1::uuid
          AND effect.state = 'pending'
          AND boundary.seq >= $2::bigint
        ORDER BY boundary.seq, effect.producer_id
        FOR UPDATE OF effect
    """

    async def save_thread_message(
        self,
        thread_id: str,
        role: str,
        content: Optional[str] = None,
        tool_calls: Optional[Any] = None,
        turn_number: Optional[int] = None,
        metrics: Optional[dict] = None,
        tool_call_id: Optional[str] = None,
        thinking: Optional[str] = None,
        reasoning: Optional[Any] = None,
        tool_results: Optional[Any] = None,
        provider: Optional[str] = None,
        provider_raw: Optional[Any] = None,
        additional_kwargs: Optional[dict] = None,
        response_metadata: Optional[dict] = None,
        id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Persist a ``thread_messages`` row directly (no orchestrator hop).

        The persistent agent already holds this asyncpg pool for resume reads and
        config writes, so message writes go straight here instead of through
        ``POST /api/agents/threads/{id}/messages`` (a pure pass-through). Two
        properties this direct path adds, both load-bearing for the resume fix in
        knowledge-base/knowledge/issues/persistent_session_midturn_message_loss.md:

        - **Caller-supplied ``id`` + idempotent upsert.** The agent mints a stable
          id per message (``persistent_graph._ensure_msg_id`` /
          ``_sanitize_ai_response``) and passes it here; ``_coerce_row_id`` maps
          it to a valid UUID for the PK column (provider/minted ids aren't UUIDs).
          ``ON CONFLICT (id) DO UPDATE`` means a message written incrementally and
          then re-saved by the turn-complete reconciliation pass collapses onto
          one row — no duplicates. ``id=None`` mints a fresh UUID (single-shot
          rows like the user message and the summary, which are never re-saved).
        - **``seq`` returned in the same round-trip.** ``RETURNING id, seq`` hands
          back the row's monotonic insertion order, so a compaction can record
          ``boundary_seq`` without a follow-up read.

        Mirrors the orchestrator's column set and the ``threads`` activity bump.
        ``tool_call_id`` is set only on role='tool' rows; ``thinking`` only on
        role='ai' rows with reasoning. The component columns (reasoning,
        tool_results, provider, provider_raw, additional_kwargs,
        response_metadata) are nullable (migration 0019). The ``seq`` column and
        its ``ON CONFLICT`` target arrived in migration 0023.
        """
        msg_id = _coerce_row_id(id)
        insert_args = (
            msg_id,
            thread_id,
            role,
            content,
            json.dumps(tool_calls) if tool_calls is not None else None,
            turn_number,
            json.dumps(metrics) if metrics is not None else None,
            tool_call_id,
            thinking,
            json.dumps(reasoning) if reasoning is not None else None,
            json.dumps(tool_results) if tool_results is not None else None,
            provider,
            json.dumps(provider_raw) if provider_raw is not None else None,
            json.dumps(additional_kwargs) if additional_kwargs is not None else None,
            json.dumps(response_metadata) if response_metadata is not None else None,
        )

        async def _write(conn):
            row = await conn.fetchrow(self._THREAD_MESSAGE_UPSERT_SQL, *insert_args)
            # Bump thread activity + turn count (mirrors the orchestrator path so
            # going direct doesn't regress last_activity / total_turns tracking).
            await conn.execute(
                self._THREAD_ACTIVITY_BUMP_SQL,
                thread_id,
                turn_number,
            )
            return row

        lease = _active_run_queue_lease_for_thread(thread_id)
        async with self.acquire() as conn:
            if lease is None:
                # Pinned lane: today's exact behavior (autocommit statements).
                row = await _write(conn)
            else:
                # Public End, admission, and reaper retirement lock the thread
                # before its queue row.  Establish the message FK's parent-row
                # authority explicitly in that same order before fencing the
                # exact claim; otherwise an implicit FK lock after run_queue
                # can deadlock with terminal retirement's threads -> queue
                # transaction.  Both the upsert and activity update remain in
                # this transaction, so a stale fence rolls everything back.
                async with conn.transaction():
                    thread_exists = await conn.fetchval(
                        self._THREAD_MESSAGE_PARENT_LOCK_SQL,
                        thread_id,
                    )
                    if thread_exists is None:
                        raise ValueError("session thread no longer exists")
                    await _require_run_queue_fence(conn, lease)
                    row = await _write(conn)
        return {"id": str(row["id"]), "seq": row["seq"]}

    async def persist_pinned_input_delivery(
        self,
        *,
        thread_id: str,
        delivery_id: str,
        role: str,
        content: str,
        source: str,
        turn_number: Optional[int],
        agent_id: str,
        pod_uid: str,
        runtime_generation: str,
        session_runtime_generation: str,
        runtime_attach_token: str,
    ) -> Dict[str, Any]:
        """Persist and claim one pinned input in a parent-first transaction."""

        from shared.persistent_input_delivery import persist_input_delivery

        async with self.acquire() as conn:
            async with conn.transaction():
                return await persist_input_delivery(
                    conn,
                    thread_id=thread_id,
                    delivery_id=delivery_id,
                    role=role,
                    content=content,
                    source=source,
                    turn_number=turn_number,
                    agent_id=agent_id,
                    pod_uid=pod_uid,
                    runtime_generation=runtime_generation,
                    session_runtime_generation=session_runtime_generation,
                    runtime_attach_token=runtime_attach_token,
                )

    async def claim_pending_pinned_input_deliveries(
        self,
        *,
        thread_id: str,
        agent_id: str,
        pod_uid: str,
        runtime_generation: str,
        session_runtime_generation: str,
        runtime_attach_token: str,
    ) -> List[Dict[str, Any]]:
        """Reclaim persisted but unadmitted input after attach/restart."""

        from shared.persistent_input_delivery import claim_pending_input_deliveries

        async with self.acquire() as conn:
            async with conn.transaction():
                return await claim_pending_input_deliveries(
                    conn,
                    thread_id=thread_id,
                    agent_id=agent_id,
                    pod_uid=pod_uid,
                    runtime_generation=runtime_generation,
                    session_runtime_generation=session_runtime_generation,
                    runtime_attach_token=runtime_attach_token,
                )

    async def mark_pinned_input_delivery_queued(
        self,
        *,
        thread_id: str,
        delivery_id: str,
        agent_id: str,
        pod_uid: str,
        runtime_generation: str,
        session_runtime_generation: str,
        runtime_attach_token: str,
        claim_generation: int,
    ) -> bool:
        from shared.persistent_input_delivery import (
            lock_runtime_authority,
            mark_input_delivery_queued,
        )

        async with self.acquire() as conn:
            async with conn.transaction():
                await lock_runtime_authority(
                    conn,
                    thread_id=thread_id,
                    agent_id=agent_id,
                    pod_uid=pod_uid,
                    session_runtime_generation=session_runtime_generation,
                    runtime_attach_token=runtime_attach_token,
                )
                return await mark_input_delivery_queued(
                    conn,
                    delivery_id=delivery_id,
                    agent_id=agent_id,
                    pod_uid=pod_uid,
                    runtime_generation=runtime_generation,
                    session_runtime_generation=session_runtime_generation,
                    runtime_attach_token=runtime_attach_token,
                    claim_generation=claim_generation,
                )

    async def transition_pinned_input_delivery(
        self,
        *,
        thread_id: str,
        delivery_id: str,
        agent_id: str,
        pod_uid: str,
        runtime_generation: str,
        session_runtime_generation: str,
        runtime_attach_token: str,
        claim_generation: int,
        transition: str,
        turn_number: Optional[int] = None,
        reason: Optional[str] = None,
    ) -> bool:
        from shared.persistent_input_delivery import (
            lock_runtime_authority,
            transition_input_delivery,
        )

        async with self.acquire() as conn:
            async with conn.transaction():
                await lock_runtime_authority(
                    conn,
                    thread_id=thread_id,
                    agent_id=agent_id,
                    pod_uid=pod_uid,
                    session_runtime_generation=session_runtime_generation,
                    runtime_attach_token=runtime_attach_token,
                )
                return await transition_input_delivery(
                    conn,
                    delivery_id=delivery_id,
                    agent_id=agent_id,
                    pod_uid=pod_uid,
                    runtime_generation=runtime_generation,
                    session_runtime_generation=session_runtime_generation,
                    runtime_attach_token=runtime_attach_token,
                    claim_generation=claim_generation,
                    transition=transition,
                    turn_number=turn_number,
                    reason=reason,
                )

    async def verify_pinned_runtime_effect_authority(
        self,
        *,
        thread_id: str,
        agent_id: str,
        pod_uid: str,
        session_runtime_generation: str,
        runtime_attach_token: str,
    ) -> bool:
        """Prove the exact pinned runtime is still allowed external effects.

        The shared lock also requires ``runtime_retirement_token IS NULL``.
        Provider and tool boundaries call this immediately before external I/O
        so owner-authorized retirement cannot wait for the lifecycle watchdog's
        next poll to close admission.
        """

        from shared.persistent_input_delivery import (
            InputDeliveryAuthorityLost,
            lock_runtime_authority,
        )

        try:
            async with self.acquire() as conn:
                async with conn.transaction():
                    await lock_runtime_authority(
                        conn,
                        thread_id=thread_id,
                        agent_id=agent_id,
                        pod_uid=pod_uid,
                        session_runtime_generation=session_runtime_generation,
                        runtime_attach_token=runtime_attach_token,
                    )
            return True
        except InputDeliveryAuthorityLost:
            return False

    async def get_pinned_input_delivery(
        self, delivery_id: str
    ) -> Optional[Dict[str, Any]]:
        from shared.persistent_input_delivery import get_input_delivery

        async with self.acquire() as conn:
            return await get_input_delivery(conn, delivery_id)

    async def claim_stateless_input_delivery(
        self,
        *,
        thread_id: str,
        delivery_id: str,
        lease_token: int,
        executor_id: str,
        pod_uid: str,
    ) -> Optional[Dict[str, Any]]:
        """Bind one event input to this exact stateless queue claimant."""

        from shared.persistent_input_delivery import (
            claim_stateless_input_delivery,
        )

        async with self.acquire() as conn:
            async with conn.transaction():
                return await claim_stateless_input_delivery(
                    conn,
                    thread_id=thread_id,
                    delivery_id=delivery_id,
                    lease_token=lease_token,
                    executor_id=executor_id,
                    pod_uid=pod_uid,
                )

    async def transition_stateless_input_delivery(
        self,
        *,
        thread_id: str,
        delivery_id: str,
        lease_token: int,
        executor_id: str,
        pod_uid: str,
        claim_generation: int,
        transition: str,
        turn_number: Optional[int] = None,
        reason: Optional[str] = None,
    ) -> bool:
        """Transition an event under its exact stateless queue lease."""

        from shared.persistent_input_delivery import (
            transition_stateless_input_delivery,
        )

        async with self.acquire() as conn:
            async with conn.transaction():
                return await transition_stateless_input_delivery(
                    conn,
                    thread_id=thread_id,
                    delivery_id=delivery_id,
                    lease_token=lease_token,
                    executor_id=executor_id,
                    pod_uid=pod_uid,
                    claim_generation=claim_generation,
                    transition=transition,
                    turn_number=turn_number,
                    reason=reason,
                )

    async def save_thread_messages(
        self,
        thread_id: str,
        messages: List[Dict[str, Any]],
        *,
        turn_input_message_id: Optional[str] = None,
        turn_number: Optional[int] = None,
        memory_scope_kind: Optional[str] = None,
        memory_scope_id: Optional[str] = None,
    ) -> Optional[str]:
        """Batch-upsert a turn's messages: one pipelined ``executemany`` + a
        single ``threads`` bump.

        Replaces the turn-complete reconcile's row-by-row ``save_thread_message``
        loop (~2 round-trips per message) with 2 round-trips for the whole turn.
        Each dict carries the same fields as :meth:`save_thread_message`'s kwargs;
        a stable ``id`` makes the upsert land on the incremental row via
        ``ON CONFLICT (id)``, and ``seq`` is preserved (assigned once on first
        insert). No ``RETURNING`` — the reconcile never reads ``seq`` back, and
        ``executemany`` discards results anyway. A dict flagged
        ``insert_if_absent`` (a lossy compaction view of its row) only
        inserts: it fills a row whose incremental write was lost and never
        rewrites the content of one that landed (its ``tool_call_id`` alone
        follows, so a remapped pair stays paired).

        The upsert runs inside a transaction so the whole turn reconciles
        atomically. This batches ONLY the reconcile; the incremental mid-turn
        persists still go through :meth:`save_thread_message` one at a time (the
        crash-durability path).

        A stateless turn-complete caller also supplies the exact accepted input
        message, turn number, and immutable memory scope. After the claim fence
        succeeds, this method validates that scope against the locked thread,
        mints (or reuses) a ``turn_execution_id`` on the boundary row, and
        inserts the stable final-memory obligation in the same transaction as
        the final transcript. A stale claimant therefore commits neither, and
        an idempotent reconcile returns the same producer identity. A later
        project/mount edit cannot redirect the captured destination. Pinned
        callers have no lease context and preserve the transcript-only path.

        An empty message list remains a no-op except for the stateless producer
        path: even a turn that emitted no AI/tool row must durably mint its
        final-memory obligation.
        """
        if not messages and not turn_input_message_id:
            return None

        lease = _active_run_queue_lease_for_thread(thread_id)
        should_mint_effect = lease is not None and bool(turn_input_message_id)
        if not messages and not should_mint_effect:
            # In particular, preserve the pinned lane's historical empty-batch
            # no-op even when its caller provides turn-boundary metadata.
            return None
        if should_mint_effect and turn_number is None:
            raise ValueError(
                "turn_number is required when minting a session-turn effect"
            )
        if should_mint_effect:
            if memory_scope_kind not in {"thread", "project"}:
                raise ValueError(
                    "memory_scope_kind must be thread or project for a "
                    "session-turn effect"
                )
            try:
                destination_id = uuid.UUID(str(memory_scope_id))
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    "memory_scope_id must be a UUID for a session-turn effect"
                ) from exc
            if memory_scope_kind == "thread" and str(destination_id) != str(
                uuid.UUID(str(thread_id))
            ):
                raise ValueError("thread memory scope must equal the thread id")
        else:
            destination_id = None

        def _dj(value: Any) -> Optional[str]:
            return json.dumps(value) if value is not None else None

        args: List[Tuple] = []
        insert_if_absent: List[bool] = []
        max_turn = 0
        for m in messages:
            row_turn_number = m.get("turn_number")
            max_turn = max(max_turn, row_turn_number or 0)
            insert_if_absent.append(m.get("insert_if_absent") is True)
            args.append(
                (
                    _coerce_row_id(m.get("id")),
                    thread_id,
                    m["role"],
                    m.get("content"),
                    _dj(m.get("tool_calls")),
                    row_turn_number,
                    _dj(m.get("metrics")),
                    m.get("tool_call_id"),
                    m.get("thinking"),
                    _dj(m.get("reasoning")),
                    _dj(m.get("tool_results")),
                    m.get("provider"),
                    _dj(m.get("provider_raw")),
                    _dj(m.get("additional_kwargs")),
                    _dj(m.get("response_metadata")),
                )
            )

        turn_execution_id = None
        async with self.acquire() as conn:
            async with conn.transaction():
                # Match public End's threads -> run_queue order before any
                # batch FK/upsert or activity mutation. Pinned lane (no lease
                # context) keeps today's exact transaction shape.
                memory_parent = None
                if lease is not None:
                    # The authoritative final reconcile captures its memory
                    # tenancy while the thread row is locked, before the exact
                    # queue fence. A later project/mount edit can refresh
                    # credentials but cannot redirect an old turn's facts.
                    memory_parent = await conn.fetchrow(
                        self._SESSION_MEMORY_PARENT_LOCK_SQL,
                        thread_id,
                    )
                    if memory_parent is None:
                        raise ValueError("session thread no longer exists")
                    await _require_run_queue_fence(conn, lease)
                if should_mint_effect:
                    if str(memory_parent["execution_lane"] or "") != "stateless":
                        raise ValueError(
                            "session-turn effects require the stateless lane"
                        )
                    if memory_scope_kind == "project":
                        metadata = memory_parent["metadata"] or {}
                        if isinstance(metadata, str):
                            try:
                                metadata = json.loads(metadata)
                            except (TypeError, ValueError):
                                metadata = {}
                        legacy_ids = (
                            metadata.get("project_ids", [])
                            if isinstance(metadata, dict)
                            else []
                        )
                        direct_match = memory_parent["project_id"] is not None and str(
                            memory_parent["project_id"]
                        ) == str(destination_id)
                        legacy_match = str(destination_id) in {
                            str(value) for value in legacy_ids
                        }
                        mount_match = await conn.fetchval(
                            self._SESSION_MEMORY_PROJECT_SCOPE_SQL,
                            thread_id,
                            destination_id,
                        )
                        if not (direct_match or legacy_match or mount_match):
                            raise ValueError(
                                "captured memory project is not attached to "
                                "the exact thread"
                            )
                # Same upsert as save_thread_message, minus RETURNING. Each
                # execution's ON CONFLICT is independent (executemany runs N
                # separate commands), so distinct-id rows never collide.
                # Insert-if-absent rows run as their own statement per
                # consecutive run, in row order, so a row either kind has to
                # mint still lands at its position in seq.
                for only_if_absent, run in groupby(
                    zip(insert_if_absent, args), key=lambda pair: pair[0]
                ):
                    await conn.executemany(
                        self._THREAD_MESSAGE_INSERT_IF_ABSENT_BATCH_SQL
                        if only_if_absent
                        else self._THREAD_MESSAGE_UPSERT_BATCH_SQL,
                        [row for _, row in run],
                    )
                # One activity/turn bump for the whole batch (was per-message).
                # The stateless producer path also bumps an output-less turn.
                activity_turn = max_turn
                if should_mint_effect:
                    activity_turn = max(activity_turn, turn_number or 0)
                await conn.execute(
                    self._THREAD_ACTIVITY_BUMP_SQL,
                    thread_id,
                    activity_turn,
                )
                if should_mint_effect:
                    input_row_id = _coerce_row_id(turn_input_message_id)
                    boundary = await conn.fetchrow(
                        self._SESSION_TURN_EXECUTION_SQL,
                        thread_id,
                        input_row_id,
                        turn_number,
                    )
                    if boundary is None:
                        raise ValueError(
                            "exact live turn-boundary message was not found"
                        )
                    turn_execution_id = boundary["turn_execution_id"]
                    boundary_seq = int(boundary["seq"])
                    reconciled_ids = [input_row_id]
                    reconciled_ids.extend(row[0] for row in args)
                    end_seq = await conn.fetchval(
                        self._SESSION_TURN_END_SEQ_SQL,
                        thread_id,
                        reconciled_ids,
                    )
                    if end_seq is None or int(end_seq) < boundary_seq:
                        raise ValueError(
                            "exact reconciled turn transcript has no valid end"
                        )
                    effect_producer_id = await conn.fetchval(
                        self._SESSION_MEMORY_EFFECT_INSERT_SQL,
                        thread_id,
                        turn_execution_id,
                        input_row_id,
                        turn_number,
                        memory_scope_kind,
                        destination_id,
                        boundary_seq,
                        int(end_seq),
                    )
                    if effect_producer_id is None:
                        raise ValueError(
                            "existing session-turn effect has conflicting identity"
                        )
        return str(turn_execution_id) if turn_execution_id is not None else None

    # =========================================================================
    # SYNC WRAPPERS (for scripts and other sync contexts)
    # =========================================================================

    # Class-level event loop for sync wrappers (shared across all instances)
    _sync_loop = None

    @classmethod
    def _get_sync_loop(cls):
        """Get or create a persistent event loop for sync operations.

        Returns the same event loop across all calls, allowing asyncpg
        connection pools to persist between sync wrapper calls.
        """
        import asyncio

        if cls._sync_loop is None or cls._sync_loop.is_closed():
            cls._sync_loop = asyncio.new_event_loop()
        return cls._sync_loop

    @classmethod
    def _run_async(cls, coro):
        """Helper to run async coroutines in sync context.

        Uses a persistent event loop to execute coroutines, allowing
        asyncpg connection pools to persist between calls.

        Args:
            coro: Async coroutine to execute

        Returns:
            Result of the coroutine
        """
        import asyncio

        try:
            # Check if we're already in an async context
            loop = asyncio.get_running_loop()
            if loop.is_running():
                raise RuntimeError(
                    "Cannot use sync wrapper inside async context. "
                    "Use async methods directly instead."
                )
        except RuntimeError:
            pass  # No running loop, that's fine

        loop = cls._get_sync_loop()
        return loop.run_until_complete(coro)

    def connect_sync(self) -> None:
        """Synchronous wrapper for connect().

        Establishes async connection pool in sync context.
        """
        self._run_async(self.connect())

    def close_sync(self) -> None:
        """Synchronous wrapper for close().

        Closes connection pool in sync context.
        """
        self._run_async(self.close())

    def execute_sync(self, query: str, *args) -> str:
        """Synchronous wrapper for execute().

        Args:
            query: SQL query with $1, $2, etc. placeholders
            *args: Query parameters

        Returns:
            Command status string
        """
        return self._run_async(self.execute(query, *args))

    def fetch_sync(self, query: str, *args) -> List[Dict[str, Any]]:
        """Synchronous wrapper for fetch().

        Args:
            query: SQL query with $1, $2, etc. placeholders
            *args: Query parameters

        Returns:
            List of row dictionaries
        """
        rows = self._run_async(self.fetch(query, *args))
        return [self._row_to_dict(row) for row in rows]

    def fetchrow_sync(self, query: str, *args) -> Optional[Dict[str, Any]]:
        """Synchronous wrapper for fetchrow().

        Args:
            query: SQL query with $1, $2, etc. placeholders
            *args: Query parameters

        Returns:
            Single row dictionary or None
        """
        row = self._run_async(self.fetchrow(query, *args))
        return self._row_to_dict(row)

    def fetchval_sync(self, query: str, *args) -> Any:
        """Synchronous wrapper for fetchval().

        Args:
            query: SQL query with $1, $2, etc. placeholders
            *args: Query parameters

        Returns:
            Single value from first column of first row
        """
        return self._run_async(self.fetchval(query, *args))


class JobsNamespace:
    """Namespace for job-related operations.

    Provides CRUD operations for the jobs table.
    """

    def __init__(self, db: PostgresDB):
        self.db = db

    async def create(
        self,
        description: str,
        document_path: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> uuid.UUID:
        """Refuses. Agents do not create jobs; the orchestrator does.

        This used to be a raw ``INSERT INTO jobs`` — the only one in the tree
        outside tests — and it bypassed every invariant
        ``orchestrator.database.postgres.create_job()`` maintains: the
        ``origin`` stamp, execution-lane inheritance from the parent, the
        authority columns, datasource linking and the policy-revision
        snapshot. A row written here would be silently classified as
        human-submitted work.

        It had no callers, but ``src/agent/agent.py`` already holds this exact
        object and calls sibling methods on it, so it sat one line away from
        being used. Kept as an explicit refusal rather than deleted, so that
        line fails loudly at the call instead of quietly writing a
        half-initialised job.

        Raises:
            NotImplementedError: always.
        """
        raise NotImplementedError(
            "Agents must not insert jobs directly — POST /api/jobs through the "
            "orchestrator, which stamps origin and the authority columns."
        )

    async def get(self, job_id: uuid.UUID) -> Optional[Dict[str, Any]]:
        """Get job by ID.

        Args:
            job_id: Job UUID

        Returns:
            Job details as dictionary or None if not found
        """
        row = await self.db.fetchrow("SELECT * FROM jobs WHERE id = $1", job_id)
        return self.db._row_to_dict(row)

    async def merge_context(self, job_id: uuid.UUID, updates: Dict[str, Any]) -> bool:
        """Atomically merge caller-safe top-level keys into job context."""
        safe_updates = dict(updates or {})
        for key in (
            "pull_request",
            "required_deliverables",
            "deliverable_contract_provenance",
            "prior_deliverable_contract",
            "required_pr_repositories",
        ):
            safe_updates.pop(key, None)
        result = await self.db.execute(
            """
            UPDATE jobs
            SET context = COALESCE(context, '{}'::jsonb) || $1::jsonb,
                updated_at = NOW()
            WHERE id = $2
            """,
            json.dumps(safe_updates),
            job_id,
        )
        return result == "UPDATE 1"

    async def record_pull_request(
        self,
        job_id: uuid.UUID,
        datasource_id: uuid.UUID,
        pull_request: Dict[str, Any],
        *,
        source_revision: str,
    ) -> bool:
        """Persist PR authority only for an exact writable job attachment.

        The caller cannot select authority through the record itself: the
        linked datasource row is loaded first and its server-owned repository
        identity must match. ``source_revision`` is derived by ``repo_open_pr``
        only after the checked-out branch and its pushed remote ref agree.
        This is the sole agent-side PR authority writer.
        """

        from urllib.parse import urlparse

        from shared.runtime.services.forge import (
            ForgeError,
            forge_web_url_matches_connector,
            parse_owner_repo,
        )
        from shared.deliverable_contract import normalize_repository_identity

        async with self.db.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    SELECT datasource.connection_url,
                           datasource.config,
                           datasource.read_only,
                           datasource.policy_revision,
                           contract.pr_repositories,
                           contract.pr_bindings,
                           job.status AS job_status,
                           COALESCE((
                               SELECT bool_or(link.read_only)
                                 FROM project_datasources AS link
                                WHERE link.project_id = job.project_id
                                  AND link.datasource_id = datasource.id
                           ), FALSE) AS project_read_only
                      FROM job_datasources AS attachment
                      JOIN jobs AS job ON job.id = attachment.job_id
                      JOIN datasources AS datasource
                        ON datasource.id = attachment.datasource_id
                      LEFT JOIN job_deliverable_contracts AS contract
                        ON contract.job_id = job.id
                     WHERE attachment.job_id = $1
                       AND attachment.datasource_id = $2
                       AND datasource.type = 'repository'
                     FOR UPDATE OF job
                     FOR SHARE OF attachment, datasource
                    """,
                    job_id,
                    datasource_id,
                )
                if (
                    row is None
                    or row["job_status"] != "processing"
                    or row["read_only"]
                    or row["project_read_only"]
                ):
                    return False
                try:
                    owner, repository = parse_owner_repo(
                        str(row["connection_url"] or "")
                    )
                except ForgeError:
                    return False
                expected = normalize_repository_identity(f"{owner}/{repository}")
                supplied = normalize_repository_identity(pull_request.get("repo"))
                if expected is None or supplied != expected:
                    return False

                config = row["config"]
                if isinstance(config, str):
                    try:
                        config = json.loads(config)
                    except (TypeError, ValueError, json.JSONDecodeError):
                        config = {}
                config = config if isinstance(config, dict) else {}
                expected_forge = str(config.get("forge") or "").strip().lower()
                supplied_forge = str(pull_request.get("forge") or "").strip().lower()
                number = pull_request.get("number")
                url = pull_request.get("url")
                head = pull_request.get("head")
                base = pull_request.get("base")
                normalized_revision = str(source_revision or "").strip().lower()
                parsed_url = urlparse(url) if isinstance(url, str) else None
                connection_url = str(row["connection_url"] or "")
                if (
                    not expected_forge
                    or supplied_forge != expected_forge
                    or isinstance(number, bool)
                    or not isinstance(number, int)
                    or number < 1
                    or parsed_url is None
                    or parsed_url.scheme not in {"http", "https"}
                    or parsed_url.hostname is None
                    or parsed_url.username is not None
                    or parsed_url.password is not None
                    or not forge_web_url_matches_connector(
                        url,
                        connection_url,
                        expected_forge,
                    )
                    or not isinstance(head, str)
                    or not head.strip()
                    or len(head) > 500
                    or not isinstance(base, str)
                    or not base.strip()
                    or len(base) > 500
                    or len(url) > 2_000
                    or len(normalized_revision) not in {40, 64}
                    or any(
                        char not in "0123456789abcdef" for char in normalized_revision
                    )
                ):
                    return False

                pr_repositories = list(row["pr_repositories"] or [])
                bindings = row["pr_bindings"]
                if isinstance(bindings, str):
                    try:
                        bindings = json.loads(bindings)
                    except (TypeError, ValueError, json.JSONDecodeError):
                        bindings = []
                bindings = bindings if isinstance(bindings, list) else []
                if pr_repositories:
                    binding = bindings[0] if len(bindings) == 1 else None
                    if (
                        len(pr_repositories) != 1
                        or pr_repositories[0] != expected
                        or not isinstance(binding, dict)
                        or str(binding.get("repository") or "") != expected
                        or str(binding.get("datasource_id") or "") != str(datasource_id)
                        or str(binding.get("forge") or "") != expected_forge
                        or int(binding.get("policy_revision") or -1)
                        != int(row["policy_revision"])
                    ):
                        return False

                # Persist only the fixed, credential-free record produced by
                # repo_open_pr. Extra caller/result fields never enter durable
                # context, and the server-derived connector supplies both
                # repository and forge authority.
                record = {
                    "forge": expected_forge,
                    "repo": expected,
                    "number": number,
                    "url": url,
                    "head": head.strip(),
                    "base": base.strip(),
                }
                semantic = (
                    datasource_id,
                    expected,
                    expected_forge,
                    number,
                    url,
                    head.strip(),
                    base.strip(),
                    normalized_revision,
                    int(row["policy_revision"]),
                )
                existing = await conn.fetchrow(
                    """
                    SELECT datasource_id, repository, forge, number, url,
                           head, base, source_revision, policy_revision
                      FROM job_pull_request_authorities
                     WHERE job_id = $1
                     FOR UPDATE
                    """,
                    job_id,
                )
                existing_semantic = (
                    tuple(existing.values()) if existing is not None else None
                )
                if existing_semantic != semantic:
                    await conn.execute(
                        """
                        INSERT INTO job_pull_request_authorities (
                            job_id, datasource_id, repository, forge, number,
                            url, head, base, source_revision, policy_revision
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                        ON CONFLICT (job_id) DO UPDATE
                           SET record_id = gen_random_uuid(),
                               record_generation =
                                   job_pull_request_authorities.record_generation + 1,
                               datasource_id = EXCLUDED.datasource_id,
                               repository = EXCLUDED.repository,
                               forge = EXCLUDED.forge,
                               number = EXCLUDED.number,
                               url = EXCLUDED.url,
                               head = EXCLUDED.head,
                               base = EXCLUDED.base,
                               source_revision = EXCLUDED.source_revision,
                               policy_revision = EXCLUDED.policy_revision,
                               updated_at = now(),
                               verified_at = NULL,
                               verified_record_id = NULL,
                               verified_generation = NULL,
                               verified_state = NULL,
                               verified_head = NULL,
                               verified_base = NULL,
                               verified_head_revision = NULL
                        """,
                        job_id,
                        *semantic,
                    )

                result = await conn.execute(
                    """
                    UPDATE jobs
                       SET context = COALESCE(context, '{}'::jsonb)
                                     || jsonb_build_object(
                                         'pull_request', $1::jsonb
                                     ),
                           updated_at = now()
                     WHERE id = $2
                       AND EXISTS (
                           SELECT 1
                             FROM job_datasources
                            WHERE job_id = $2 AND datasource_id = $3
                       )
                    """,
                    json.dumps(record),
                    job_id,
                    datasource_id,
                )
        return result == "UPDATE 1"

    async def update_status(
        self,
        job_id: uuid.UUID,
        status: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> None:
        """Update job status fields.

        Args:
            job_id: Job UUID
            status: Main job status (optional)
            error_message: Error message if failed (optional)
        """
        updates = []
        values = []
        idx = 1

        if status is not None:
            updates.append(f"status = ${idx}")
            values.append(status)
            idx += 1

        if error_message is not None:
            updates.append(f"error_message = ${idx}")
            values.append(error_message)
            idx += 1

        if not updates:
            return

        updates.append("updated_at = NOW()")
        values.append(job_id)

        query = f"""
            UPDATE jobs
            SET {", ".join(updates)}
            WHERE id = ${idx}
        """

        await self.db.execute(query, *values)
        logger.debug(f"Updated job {job_id} status")

    async def get_pending(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get pending jobs.

        Args:
            limit: Maximum number of jobs to return

        Returns:
            List of job dictionaries
        """
        rows = await self.db.fetch(
            """
            SELECT * FROM jobs
            WHERE status = 'pending'
            ORDER BY created_at ASC
            LIMIT $1
            """,
            limit,
        )
        return [self.db._row_to_dict(row) for row in rows]

    async def list(
        self, status: Optional[str] = None, limit: int = 100, offset: int = 0
    ) -> List[Dict[str, Any]]:
        """List jobs with optional filtering.

        Args:
            status: Filter by status (optional)
            limit: Maximum number of jobs to return
            offset: Number of jobs to skip

        Returns:
            List of job dictionaries
        """
        if status:
            rows = await self.db.fetch(
                """
                SELECT * FROM jobs
                WHERE status = $1
                ORDER BY created_at DESC
                LIMIT $2 OFFSET $3
                """,
                status,
                limit,
                offset,
            )
        else:
            rows = await self.db.fetch(
                """
                SELECT * FROM jobs
                ORDER BY created_at DESC
                LIMIT $1 OFFSET $2
                """,
                limit,
                offset,
            )
        return [self.db._row_to_dict(row) for row in rows]

    async def store_resolved_config(
        self,
        job_id: uuid.UUID,
        resolved_config: Dict[str, Any],
    ) -> bool:
        """Store the fully resolved config snapshot for a job.

        Only writes if resolved_config is currently NULL (first run only).

        Args:
            job_id: Job UUID
            resolved_config: Fully resolved config dict (agent config + prompts + instructions)

        Returns:
            True if the config was stored, False if already set
        """
        result = await self.db.execute(
            "UPDATE jobs SET resolved_config = $1::jsonb WHERE id = $2 AND resolved_config IS NULL",
            json.dumps(resolved_config),
            job_id,
        )
        stored = result == "UPDATE 1"
        if stored:
            logger.debug(f"Stored resolved config for job {job_id}")
        return stored

    async def get_resolved_config(
        self,
        job_id: uuid.UUID,
    ) -> Optional[Dict[str, Any]]:
        """Get the resolved config snapshot for a job.

        Args:
            job_id: Job UUID

        Returns:
            Resolved config dict or None if not set
        """
        row = await self.db.fetchrow(
            "SELECT resolved_config FROM jobs WHERE id = $1",
            job_id,
        )
        if row and row["resolved_config"]:
            rc = row["resolved_config"]
            return rc if isinstance(rc, dict) else json.loads(rc)
        return None

    # Sync wrappers for scripts
    def create_sync(
        self,
        description: str,
        document_path: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> uuid.UUID:
        """Synchronous wrapper for create()."""
        return PostgresDB._run_async(self.create(description, document_path, context))

    def get_sync(self, job_id: uuid.UUID) -> Optional[Dict[str, Any]]:
        """Synchronous wrapper for get()."""
        return PostgresDB._run_async(self.get(job_id))

    def update_status_sync(
        self,
        job_id: uuid.UUID,
        status: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> None:
        """Synchronous wrapper for update_status()."""
        return PostgresDB._run_async(self.update_status(job_id, status, error_message))

    def get_pending_sync(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Synchronous wrapper for get_pending()."""
        return PostgresDB._run_async(self.get_pending(limit))

    def list_sync(
        self, status: Optional[str] = None, limit: int = 100, offset: int = 0
    ) -> List[Dict[str, Any]]:
        """Synchronous wrapper for list()."""
        return PostgresDB._run_async(self.list(status, limit, offset))


class ConfigOverridesNamespace:
    """Config-override reads for the agent's resolution path."""

    def __init__(self, db: PostgresDB):
        self.db = db

    async def list_overrides_for_family(self, family: str) -> List[Dict[str, Any]]:
        """Return override rows for <family> plus global (NULL-family) rows.

        Read once per job at first run; the result is loaded into the loader's
        process map and frozen into resolved_config.
        """
        rows = await self.db.fetch(
            """
            SELECT family, kind, name, content, content_format, value_json
            FROM config_overrides
            WHERE family = $1 OR family IS NULL
            """,
            family,
        )
        return [self.db._row_to_dict(row) for row in rows]


__all__ = ["PostgresDB"]
