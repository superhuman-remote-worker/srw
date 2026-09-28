"""SQL migration runner.

Apply versioned SQL files from ``src/orchestrator/database/migrations/{app,audit,vector}/``
in lexicographic order, tracked in a ``schema_migrations`` table on each DB.

Applied files are immutable. The one reviewed historical exception is vector
0025's published pgvector-preload variant: migration_recovery pins its exact
filename, canonical on-disk checksum, and alternate successful-ledger checksum.
Acceptance preserves the complete successful ledger row and never replays its
SQL. Dirty rows and all unreviewed checksum drift still fail closed.

Two reviewed app files were applied under earlier names and published under
new ones (migration_recovery.RENAMED_APPLIED_MIGRATIONS). Their exact historical
rows are retained and not treated as missing files; the canonical files apply
like any other pending file. Unknown names, other checksums, failed rows and a
missing or different canonical file still fail closed.

Design rationale and operational runbook live in ``knowledge-base/knowledge/db_migration.md``.
"""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
import hashlib
import logging
import os
import re
import time
from pathlib import Path

import asyncpg

from orchestrator.database.migration_recovery import (
    APPLIED_CHECKSUM_COMPATIBILITIES,
    NOTX_RECOVERIES,
    RENAMED_APPLIED_MIGRATIONS,
    ConcurrentIndexRecovery,
)
from shared.db_url import build_postgres_url

LOCK_ID = 0x5352575F4D4947  # "SRW_MIG" packed into int64.
# A distinct lock is load-bearing for CREATE INDEX CONCURRENTLY. If a second
# runner opens the transactional pass and then waits on the same session lock,
# PostgreSQL's concurrent build can wait for that transaction's old snapshot
# while the transaction waits for the build: a real advisory/virtual-xid
# deadlock. The normal migration lock still serializes transactional passes;
# this one serializes each non-transactional operation through its ledger write.
NOTX_LOCK_ID = 0x5352575F4E5458  # "SRW_NOTX" packed into int64.
NOTX_SUFFIX = ".notx.sql"
MAINTENANCE_GATES_ENV = "MIGRATION_MAINTENANCE_GATES"
_MAINTENANCE_GATE_RE = re.compile(
    r"^--[ \t]*maintenance-gate:[ \t]*([a-z0-9][a-z0-9-]*)[ \t]*$",
    re.MULTILINE,
)
_MAINTENANCE_GATE_DECL_RE = re.compile(
    r"^[ \t]*--[ \t]*maintenance-gate:[^\r\n]*$",
    re.MULTILINE | re.IGNORECASE,
)
_STANDARD_STRINGS_SETTING_RE = re.compile(
    r"\bstandard_conforming_strings\b",
    re.IGNORECASE,
)

log = logging.getLogger(__name__)

DDL = """
CREATE TABLE IF NOT EXISTS public.schema_migrations (
    filename       TEXT         PRIMARY KEY,
    checksum       TEXT         NOT NULL,
    applied_at     TIMESTAMPTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP,
    applied_by     TEXT         NOT NULL DEFAULT current_user,
    execution_ms   INTEGER      NOT NULL,
    success        BOOLEAN      NOT NULL DEFAULT TRUE,
    error          TEXT
);
CREATE INDEX IF NOT EXISTS schema_migrations_dirty_idx
    ON public.schema_migrations(filename) WHERE success = FALSE;
"""

# Columns the runner needs on schema_migrations. ``CREATE TABLE IF NOT
# EXISTS`` no-ops if a table by the same name already exists with a
# different shape, so we explicitly verify after creation. Pre-cutover
# vector_schema.sql shipped a hand-rolled ``schema_migrations(version,
# description)`` table; this check turns that collision into a clear
# error instead of an UndefinedColumnError on the first INSERT.
REQUIRED_COLUMNS = frozenset(
    {
        "filename",
        "checksum",
        "applied_at",
        "applied_by",
        "execution_ms",
        "success",
        "error",
    }
)


class DryRunRollback(Exception):
    """Raised at end of dry-run to force the outer transaction to roll back."""


def _checksum(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def _is_notx(path: Path) -> bool:
    # ``.notx.sql`` is two dots; ``Path.suffixes`` returns ['.notx', '.sql'].
    return path.name.endswith(NOTX_SUFFIX)


def _maintenance_gate(sql: str) -> str | None:
    """Return the one operator acknowledgement required by a migration.

    Most migrations must remain rolling-compatible and have no gate. A rare
    drained-window migration declares an exact, reviewable header such as::

        -- maintenance-gate: pinned-runtime-authority-v1

    The runner checks the gate while holding the migration advisory lock. A
    from-zero migration chain is inherently free of old application writers;
    an existing chain requires the named acknowledgement in
    ``MIGRATION_MAINTENANCE_GATES``. Multiple declarations are rejected rather
    than silently weakening an operator contract.
    """

    declarations = _MAINTENANCE_GATE_DECL_RE.findall(sql)
    if not declarations:
        return None
    if len(declarations) != 1:
        raise RuntimeError("migration declares more than one maintenance gate")
    match = _MAINTENANCE_GATE_RE.fullmatch(declarations[0])
    if match is None:
        raise RuntimeError("migration declares a malformed maintenance gate")
    return match.group(1)


def _acknowledged_maintenance_gates() -> frozenset[str]:
    return frozenset(
        item.strip()
        for item in os.getenv(MAINTENANCE_GATES_ENV, "").split(",")
        if item.strip()
    )


def _postgres_identifier_continuation(char: str) -> bool:
    """Match PostgreSQL's unquoted identifier continuation characters.

    PostgreSQL's scanner accepts every high-bit byte in an identifier, not
    only Unicode characters Python classifies as alphanumeric. Lexer boundary
    decisions must therefore treat any decoded non-ASCII code point as an
    identifier continuation too.
    """

    return bool(
        char
        and (
            char in {"_", "$"}
            or (char.isascii() and char.isalnum())
            or ord(char) >= 128
        )
    )


def _top_level_sql_statements(sql: str) -> list[tuple[int, int, int]]:
    """Return ``(span start, code start, span end)`` for SQL statements.

    Migration files contain PL/pgSQL bodies, comments, and quoted strings, so
    looking for ``BEGIN;``/``COMMIT;`` with a line regex is not safe. This
    deliberately small lexer finds only semicolons in the outer SQL stream;
    dollar-quoted function bodies and quoted/commented text remain opaque.
    """

    statements: list[tuple[int, int, int]] = []
    statement_start = 0
    length = len(sql)
    index = 0
    state = "normal"
    block_depth = 0
    dollar_delimiter = ""
    single_quote_backslash_escapes = False

    while index < length:
        char = sql[index]
        following = sql[index + 1] if index + 1 < length else ""

        if state == "line_comment":
            if char in "\r\n":
                state = "normal"
            index += 1
            continue
        if state == "block_comment":
            if char == "/" and following == "*":
                block_depth += 1
                index += 2
                continue
            if char == "*" and following == "/":
                block_depth -= 1
                index += 2
                if block_depth == 0:
                    state = "normal"
                continue
            index += 1
            continue
        if state == "single_quote":
            if single_quote_backslash_escapes and char == "\\":
                index += 2
                continue
            if char == "'":
                if following == "'":
                    index += 2
                    continue
                state = "normal"
            index += 1
            continue
        if state == "double_quote":
            if char == '"':
                if following == '"':
                    index += 2
                    continue
                state = "normal"
            index += 1
            continue
        if state == "dollar_quote":
            if sql.startswith(dollar_delimiter, index):
                index += len(dollar_delimiter)
                state = "normal"
            else:
                index += 1
            continue

        if char == "-" and following == "-":
            state = "line_comment"
            index += 2
            continue
        if char == "/" and following == "*":
            state = "block_comment"
            block_depth = 1
            index += 2
            continue
        if char == "'":
            # With PostgreSQL's default ``standard_conforming_strings=on``, a
            # backslash is ordinary text in ``'...'`` and must not hide a
            # following quote/semicolon from this lexer. Only an explicit
            # E-string gives backslash its escape meaning. The prefix must be
            # a standalone token rather than the tail of an identifier.
            single_quote_backslash_escapes = bool(
                index > 0
                and sql[index - 1] in {"e", "E"}
                and (index < 2 or not _postgres_identifier_continuation(sql[index - 2]))
            )
            state = "single_quote"
            index += 1
            continue
        if char == '"':
            state = "double_quote"
            index += 1
            continue
        if char == "$":
            delimiter = re.match(r"\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$", sql[index:])
            # PostgreSQL requires whitespace/punctuation before a dollar-
            # quoted string; otherwise ``prefix$tag$`` is one unquoted
            # identifier. Treating the identifier tail as a delimiter could
            # hide a later transaction-control statement from this lexer.
            if delimiter is not None and (
                index == 0 or not _postgres_identifier_continuation(sql[index - 1])
            ):
                dollar_delimiter = delimiter.group(0)
                state = "dollar_quote"
                index += len(dollar_delimiter)
                continue
        if char == ";":
            statement_end = index + 1
            code_start = _leading_sql_code_offset(sql, statement_start, statement_end)
            if code_start < statement_end:
                statements.append((statement_start, code_start, statement_end))
            statement_start = statement_end
        index += 1

    code_start = _leading_sql_code_offset(sql, statement_start, length)
    if code_start < length:
        statements.append((statement_start, code_start, length))
    return statements


def _leading_sql_code_offset(sql: str, start: int, end: int) -> int:
    """Skip whitespace and outer SQL comments inside one statement span."""

    index = start
    while index < end:
        if sql[index].isspace():
            index += 1
            continue
        if sql.startswith("--", index):
            carriage_return = sql.find("\r", index + 2, end)
            line_feed = sql.find("\n", index + 2, end)
            newline_candidates = tuple(
                position for position in (carriage_return, line_feed) if position >= 0
            )
            if not newline_candidates:
                return end
            newline = min(newline_candidates)
            index = newline + 1
            if sql[newline] == "\r" and index < end and sql[index] == "\n":
                index += 1
            continue
        if sql.startswith("/*", index):
            depth = 1
            index += 2
            while index < end and depth:
                if sql.startswith("/*", index):
                    depth += 1
                    index += 2
                elif sql.startswith("*/", index):
                    depth -= 1
                    index += 2
                else:
                    index += 1
            if depth:
                raise RuntimeError("migration contains an unterminated SQL comment")
            continue
        break
    return index


def _leading_sql_keywords(sql: str, *, limit: int = 2) -> tuple[str, ...]:
    """Return leading unquoted keywords with SQL comments as separators."""

    keywords: list[str] = []
    index = 0
    length = len(sql)
    while len(keywords) < limit:
        index = _leading_sql_code_offset(sql, index, length)
        if index >= length or not (sql[index].isascii() and sql[index].isalpha()):
            break
        start = index
        index += 1
        while index < length and _postgres_identifier_continuation(sql[index]):
            index += 1
        token = sql[start:index]
        if not token.isascii():
            break
        keywords.append(token.upper())
    return tuple(keywords)


def _is_transaction_control_statement(sql: str) -> bool:
    keywords = _leading_sql_keywords(sql)
    if not keywords:
        return False
    if keywords[0] in {"BEGIN", "COMMIT", "END", "ROLLBACK", "ABORT"}:
        return True
    return keywords[:2] in {("START", "TRANSACTION"), ("PREPARE", "TRANSACTION")}


def _runner_owned_transaction_sql(sql: str) -> str:
    """Remove a reviewed file wrapper so the runner owns the only transaction.

    Historical migrations include a top-level ``BEGIN; ... COMMIT;`` wrapper.
    Sending those bytes inside ``asyncpg.Connection.transaction()`` lets the
    file's COMMIT prematurely commit both DDL and the xact advisory lock before
    the runner records its ledger row. Strip only an exact whole-file wrapper;
    reject every other outer transaction-control shape before executing SQL.
    """

    if _STANDARD_STRINGS_SETTING_RE.search(sql):
        raise RuntimeError(
            "migration may not change the runner-owned "
            "standard_conforming_strings setting"
        )

    statements = _top_level_sql_statements(sql)
    controls: list[tuple[int, int, int, str]] = []
    for start, code_start, end in statements:
        code = sql[code_start:end].strip()
        if _leading_sql_keywords(code)[:2] == ("RESET", "ALL"):
            raise RuntimeError(
                "migration may not reset all runner-owned session settings"
            )
        if _is_transaction_control_statement(code):
            controls.append((start, code_start, end, code))
    if not controls:
        return sql

    first = statements[0]
    last = statements[-1]
    if (
        len(controls) != 2
        or controls[0][:3] != first
        or controls[1][:3] != last
        or controls[0][3].upper() != "BEGIN;"
        or controls[1][3].upper() != "COMMIT;"
    ):
        raise RuntimeError(
            "transactional migration contains transaction control outside "
            "an exact whole-file BEGIN;/COMMIT; wrapper"
        )

    _, begin_code_start, begin_end, _ = controls[0]
    commit_start, _, commit_end, _ = controls[1]
    return sql[:begin_code_start] + sql[begin_end:commit_start] + sql[commit_end:]


async def _acquire_notx_lock(conn: asyncpg.Connection) -> None:
    """Acquire the session lock without blocking a concurrent-index build.

    A blocking ``pg_advisory_lock`` call itself holds a virtual transaction ID.
    ``CREATE INDEX CONCURRENTLY`` may wait for that transaction while it waits
    for the advisory lock, producing a deadlock. Short try-lock statements end
    before the build needs to advance to its next phase.
    """
    while not await conn.fetchval(
        "SELECT pg_catalog.pg_try_advisory_lock($1)", NOTX_LOCK_ID
    ):
        await asyncio.sleep(0.05)


async def _force_runner_session_settings(conn: asyncpg.Connection) -> None:
    """Install the parser/schema settings assumed by the migration runner."""

    await conn.execute(
        "SET SESSION standard_conforming_strings = on; "
        # Leaving pg_catalog implicit makes PostgreSQL resolve catalog
        # functions before public while retaining public as the target schema
        # for historical unqualified migration DDL.
        "SET SESSION search_path = public"
    )
    if await conn.fetchval("SHOW standard_conforming_strings") != "on":
        raise RuntimeError("database refused standard_conforming_strings=on")
    if await conn.fetchval("SELECT pg_catalog.current_schema()") != "public":
        raise RuntimeError("database has no usable public migration schema")


async def _load_installed_pgvector(conn: asyncpg.Connection) -> None:
    """Register pgvector's settings in a cold upgrade connection.

    Installing the extension persists its SQL objects, but its shared library
    and USERSET GUC definitions load per backend. CREATE FUNCTION ... SET
    rejects an unknown hnsw.iterative_scan for non-superusers, even though the
    real setting is user-settable. Calling the vector input function loads the
    library without privileged LOAD or changing any stored migration bytes.
    Fresh databases without the extension continue through their bootstrap.
    """

    schema = await conn.fetchval(
        "SELECT pg_catalog.quote_ident(n.nspname) "
        "FROM pg_catalog.pg_extension e "
        "JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace "
        "WHERE e.extname = 'vector'"
    )
    if schema is not None:
        await conn.execute(f"SELECT '[0]'::{schema}.vector")


@asynccontextmanager
async def _transactional_runner_connection(
    pool: asyncpg.Pool,
) -> AsyncIterator[asyncpg.Connection]:
    """Acquire one connection plus the bootstrap/transaction session lock.

    ``CREATE TABLE IF NOT EXISTS`` is not race-free when two sessions create
    the same relation concurrently: PostgreSQL can still raise a duplicate
    catalog-key violation. A session lock must therefore begin before the
    ledger exists. The inner transaction also takes the xact-scoped form so
    the schema changes and success row retain their database-owned atomic
    boundary; this outer belt only spans bootstrap and fresh-chain detection.
    Waiters use try-lock polling and return the connection between attempts.
    That keeps a fleet of starting replicas from occupying every pool slot
    while the lock owner needs to record a migration failure.
    """

    conn: asyncpg.Connection | None = None
    while conn is None:
        candidate = await pool.acquire()
        try:
            acquired = await candidate.fetchval(
                "SELECT pg_catalog.pg_try_advisory_lock($1)", LOCK_ID
            )
        except BaseException:
            await pool.release(candidate)
            raise
        if acquired is True:
            conn = candidate
            break
        await pool.release(candidate)
        await asyncio.sleep(0.05)

    try:
        await _force_runner_session_settings(conn)
        yield conn
    finally:
        try:
            unlocked = await conn.fetchval(
                "SELECT pg_catalog.pg_advisory_unlock($1)", LOCK_ID
            )
            if unlocked is not True:
                raise RuntimeError("migration runner lost its session advisory lock")
        finally:
            await pool.release(conn)


def discover(migrations_dir: Path) -> list[Path]:
    """List migrations in apply order. Reject duplicate versions early.

    Normal migrations use ``NNNN_``.  A lowercase suffix (``NNNNa_``) is an
    exceptional interstitial version used only to repair an immutable later
    migration for databases that have not reached it yet.  The interstitial
    itself must also be safe when discovered by databases already past it.
    """
    if not migrations_dir.is_dir():
        raise RuntimeError(f"migrations dir not found: {migrations_dir}")
    files = sorted(
        (
            *migrations_dir.glob("[0-9][0-9][0-9][0-9]_*.sql"),
            *migrations_dir.glob("[0-9][0-9][0-9][0-9][a-z]_*.sql"),
        )
    )
    seen: dict[str, Path] = {}
    for path in files:
        prefix = path.name.split("_", 1)[0]
        if prefix in seen:
            raise RuntimeError(
                f"duplicate migration prefix {prefix!r}: "
                f"{seen[prefix].name} vs {path.name}"
            )
        seen[prefix] = path
    return files


def _renamed_applied_history(
    stray: set[str],
    applied: dict[str, str],
    files: list[Path],
) -> set[str]:
    """Return the historical rows an exact reviewed rename accounts for.

    A row qualifies only when its filename is registered, its successful
    checksum is the reviewed one, and the canonical file is on disk with those
    same bytes. The row is retained unchanged; nothing is marked applied on its
    behalf, so the canonical file still applies under its own name. Any other
    registered row is refused outright rather than reported as merely missing.
    """

    on_disk = {path.name: path for path in files}
    retained: set[str] = set()
    for filename in sorted(stray):
        renamed = RENAMED_APPLIED_MIGRATIONS.get(filename)
        if renamed is None:
            continue
        if applied[filename] != renamed.checksum:
            raise RuntimeError(
                f"checksum changed: {filename} (a renamed migration's historical "
                "row must be the exact reviewed artifact)"
            )
        canonical = on_disk.get(renamed.canonical_filename)
        if canonical is None or _checksum(canonical.read_text()) != renamed.checksum:
            raise RuntimeError(
                f"renamed migration {filename} requires "
                f"{renamed.canonical_filename} on disk with its reviewed checksum"
            )
        log.warning(
            "retained historical ledger row %s; its canonical file %s is %s",
            filename,
            renamed.canonical_filename,
            "applied" if renamed.canonical_filename in applied else "pending",
        )
        retained.add(filename)
    return retained


async def _record_failure_on_connection(
    conn: asyncpg.Connection,
    filename: str,
    checksum: str,
    ms: int,
    error: str,
) -> None:
    """Record a failure after rollback or outside a transaction."""
    await conn.execute(
        "INSERT INTO public.schema_migrations(filename, checksum, "
        "execution_ms, success, error) VALUES($1,$2,$3,FALSE,$4) "
        "ON CONFLICT (filename) DO UPDATE SET "
        "checksum = EXCLUDED.checksum, "
        "applied_at = CURRENT_TIMESTAMP, "
        "applied_by = current_user, "
        "execution_ms = EXCLUDED.execution_ms, "
        "success = FALSE, "
        "error = EXCLUDED.error",
        filename,
        checksum,
        ms,
        error[:8000],
    )


async def _require_migration_ledger_shape(conn: asyncpg.Connection) -> None:
    """Reject a same-name legacy ledger before any migration query uses it."""

    existing_columns = {
        row["column_name"]
        for row in await conn.fetch(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'schema_migrations' "
            "AND table_schema = 'public'"
        )
    }
    missing = REQUIRED_COLUMNS - existing_columns
    if missing:
        raise RuntimeError(
            f"schema_migrations table exists but is missing columns "
            f"{sorted(missing)}; this is most likely a legacy table "
            f"from before the migration runner. Drop it manually "
            f"(DROP TABLE public.schema_migrations) and let the runner "
            f"recreate it. See knowledge-base/knowledge/db_migration.md "
            f"§Operational runbook."
        )


async def _record_success_on_connection(
    conn: asyncpg.Connection,
    filename: str,
    checksum: str,
    ms: int,
) -> None:
    """Record a non-transactional success, replacing its dirty row if any."""
    await conn.execute(
        "INSERT INTO public.schema_migrations"
        "(filename, checksum, execution_ms) VALUES($1,$2,$3) "
        "ON CONFLICT (filename) DO UPDATE SET "
        "checksum = EXCLUDED.checksum, "
        "applied_at = CURRENT_TIMESTAMP, "
        "applied_by = current_user, "
        "execution_ms = EXCLUDED.execution_ms, "
        "success = TRUE, "
        "error = NULL",
        filename,
        checksum,
        ms,
    )


async def _concurrent_index_state(
    conn: asyncpg.Connection,
    recovery: ConcurrentIndexRecovery,
) -> asyncpg.Record | None:
    """Return the visible same-name index state and its canonical shape."""
    return await conn.fetchrow(
        """
        SELECT
            i.indisunique,
            i.indisvalid,
            i.indisready,
            i.indislive,
            i.indnkeyatts,
            i.indnatts,
            am.amname AS access_method,
            i.indrelid = pg_catalog.to_regclass($2) AS expected_table,
            ARRAY(
                SELECT pg_catalog.pg_get_indexdef(i.indexrelid, position, TRUE)
                FROM pg_catalog.generate_series(1, i.indnkeyatts) AS position
                ORDER BY position
            ) AS key_definitions,
            pg_catalog.pg_get_expr(i.indpred, i.indrelid) AS predicate
        FROM pg_catalog.pg_index AS i
        JOIN pg_catalog.pg_class AS index_class ON index_class.oid = i.indexrelid
        JOIN pg_catalog.pg_am AS am ON am.oid = index_class.relam
        WHERE i.indexrelid = pg_catalog.to_regclass($1)
        """,
        recovery.index_name,
        recovery.table_name,
    )


def _is_expected_concurrent_index(
    state: asyncpg.Record | None,
    recovery: ConcurrentIndexRecovery,
) -> bool:
    """Require both usable catalog flags and the reviewed exact index shape."""
    if state is None:
        return False
    return (
        state["indisunique"]
        and state["indisvalid"]
        and state["indisready"]
        and state["indislive"]
        and state["indnkeyatts"] == len(recovery.key_definitions)
        and state["indnatts"] == len(recovery.key_definitions)
        and state["access_method"] == recovery.access_method
        and state["expected_table"]
        and tuple(state["key_definitions"]) == recovery.key_definitions
        and state["predicate"] == recovery.predicate
    )


async def _require_applied_recovery_dependency(
    conn: asyncpg.Connection,
    migrations_dir: Path,
    filename: str,
) -> Path:
    """Resolve one reviewed replay file and prove its ledger checksum."""
    path = migrations_dir / filename
    if not path.is_file():
        raise RuntimeError(
            f"recovery dependency {filename!r} is missing for {migrations_dir}"
        )
    row = await conn.fetchrow(
        "SELECT checksum, success FROM public.schema_migrations WHERE filename = $1",
        filename,
    )
    checksum = _checksum(path.read_text())
    if row is None or not row["success"]:
        raise RuntimeError(
            f"recovery dependency {filename!r} is not successfully applied"
        )
    if row["checksum"] != checksum:
        raise RuntimeError(
            f"checksum changed: {filename} (recovery dependencies are immutable)"
        )
    return path


async def _run_concurrent_index_recovery(
    conn: asyncpg.Connection,
    path: Path,
    recovery: ConcurrentIndexRecovery,
) -> None:
    """Converge one explicitly reviewed concurrent-index migration."""
    cleanup_path = await _require_applied_recovery_dependency(
        conn,
        path.parent,
        recovery.cleanup_filename,
    )
    replay_path = await _require_applied_recovery_dependency(
        conn,
        path.parent,
        recovery.replay_filename,
    )

    state = await _concurrent_index_state(conn, recovery)
    if state is not None and not _is_expected_concurrent_index(state, recovery):
        log.warning(
            "recovering unusable or unexpected %s before %s",
            recovery.index_name,
            path.name,
        )
        await conn.execute(cleanup_path.read_text())
        if await _concurrent_index_state(conn, recovery) is not None:
            raise RuntimeError(
                f"{cleanup_path.name} did not remove {recovery.index_name}"
            )

    # Re-run the reviewed, replay-safe dedupe immediately before the build.
    # This closes the ordinary retry gap where new duplicates appeared after
    # 0130 was ledgered but before a prior concurrent build completed.
    await conn.execute(replay_path.read_text())

    state = await _concurrent_index_state(conn, recovery)
    if state is None:
        # Execute the immutable migration bytes, not reconstructed DDL.
        await conn.execute(path.read_text())
        state = await _concurrent_index_state(conn, recovery)

    if not _is_expected_concurrent_index(state, recovery):
        raise RuntimeError(
            f"{path.name} did not produce a unique, valid, ready, live "
            f"{recovery.index_name} with the reviewed shape"
        )


async def run_migrations(
    pool: asyncpg.Pool,
    migrations_dir: Path,
    *,
    dry_run: bool = False,
) -> None:
    """Apply pending migrations; refuse to run if anything looks wrong.

    Raises:
        RuntimeError: dirty row, checksum drift, missing applied file, or
            duplicate prefixes on disk.
        Exception: re-raised from the failing migration after recording the
            failure on the dirty row.
    """
    files = discover(migrations_dir)
    if not files:
        log.warning("no migrations in %s", migrations_dir)
        return

    transactional = [p for p in files if not _is_notx(p)]
    non_transactional = [p for p in files if _is_notx(p)]
    for path in non_transactional:
        if _maintenance_gate(path.read_text()) is not None:
            raise RuntimeError(
                f"maintenance-gated migration {path.name!r} must be "
                "transactional so its history check and schema change share "
                "the migration advisory-lock transaction"
            )

    async with _transactional_runner_connection(pool) as conn:
        ledger_preexisting = bool(
            await conn.fetchval(
                "SELECT pg_catalog.to_regclass('public.schema_migrations') IS NOT NULL"
            )
        )
        if not dry_run:
            # Ordinary failures need the ledger to survive the migration
            # transaction so their dirty row can be recorded after rollback.
            await conn.execute(DDL)
            await _require_migration_ledger_shape(conn)

        # Transactional pass — wrapped under the advisory lock + outer txn.
        transaction_failure: tuple[str, str, int, str] | None = None
        try:
            async with conn.transaction():
                await conn.execute(
                    "SELECT pg_catalog.pg_advisory_xact_lock($1)", LOCK_ID
                )
                if dry_run:
                    # Dry-run bootstrap and index repair are part of the same
                    # rollback boundary as migration DDL and tentative rows.
                    # No cleanup-by-drop is needed (or safe) afterward.
                    await conn.execute(DDL)
                    await _require_migration_ledger_shape(conn)

                dirty_rows = await conn.fetch(
                    "SELECT filename, checksum, error "
                    "FROM public.schema_migrations "
                    "WHERE success = FALSE ORDER BY filename"
                )
                unrecoverable_dirty = [
                    row for row in dirty_rows if row["filename"] not in NOTX_RECOVERIES
                ]
                if unrecoverable_dirty:
                    dirty = unrecoverable_dirty[0]
                    raise RuntimeError(
                        f"dirty migration {dirty['filename']!r}: "
                        f"{dirty['error']!s}; manual repair required "
                        f"(see knowledge-base/knowledge/db_migration.md §Operational runbook)"
                    )
                for dirty in dirty_rows:
                    path = next(
                        (item for item in files if item.name == dirty["filename"]),
                        None,
                    )
                    if path is None:
                        raise RuntimeError(
                            f"dirty recoverable migration {dirty['filename']!r} "
                            "is missing on disk"
                        )
                    if dirty["checksum"] != _checksum(path.read_text()):
                        raise RuntimeError(
                            f"checksum changed: {path.name} "
                            "(dirty recoverable migrations are immutable)"
                        )
                    log.warning(
                        "dirty migration %s has an explicit automatic "
                        "recovery contract; retrying",
                        dirty["filename"],
                    )

                applied = {
                    r["filename"]: r["checksum"]
                    for r in await conn.fetch(
                        "SELECT filename, checksum "
                        "FROM public.schema_migrations "
                        "WHERE success = TRUE ORDER BY filename"
                    )
                }
                existing_application_relations = await conn.fetchval(
                    """
                    SELECT EXISTS (
                        SELECT 1
                          FROM pg_catalog.pg_class relation
                         JOIN pg_catalog.pg_namespace namespace
                            ON namespace.oid = relation.relnamespace
                         WHERE namespace.nspname <> 'information_schema'
                           AND namespace.nspname !~ '^pg_'
                           AND relation.oid IS DISTINCT FROM
                               pg_catalog.to_regclass('public.schema_migrations')
                           AND relation.relkind IN ('r','p','v','m','S','f')
                    )
                    """
                )

                for path in files:
                    if path.name in applied:
                        disk_checksum = _checksum(path.read_text())
                        if applied[path.name] == disk_checksum:
                            continue
                        compatibility = APPLIED_CHECKSUM_COMPATIBILITIES.get(path.name)
                        if (
                            compatibility is None
                            or disk_checksum != compatibility.canonical_checksum
                            or applied[path.name] != compatibility.historical_checksum
                        ):
                            raise RuntimeError(
                                f"checksum changed: {path.name} "
                                f"(applied migrations are immutable; "
                                f"write a superseding migration instead)"
                            )
                        log.warning(
                            "accepted reviewed historical checksum for %s; "
                            "successful ledger row retained unchanged",
                            path.name,
                        )

                stray = set(applied) - {p.name for p in files}
                stray -= _renamed_applied_history(stray, applied, files)
                if stray:
                    raise RuntimeError(f"applied but missing on disk: {sorted(stray)}")

                pending_tx = [p for p in transactional if p.name not in applied]

                if not pending_tx and not [
                    p for p in non_transactional if p.name not in applied
                ]:
                    log.info(
                        "schema up to date (%d applied) in %s",
                        len(applied),
                        migrations_dir.name,
                    )
                    if dry_run:
                        raise DryRunRollback()
                    return

                if pending_tx:
                    await _load_installed_pgvector(conn)
                    log.info(
                        "applying %d transactional migration(s) in %s",
                        len(pending_tx),
                        migrations_dir.name,
                    )
                # A missing/empty ledger alone is not a fresh-install proof:
                # a restored schema_current or legacy database may have lost
                # its migration history.  Inspect the application schema under
                # the same migration lock and bypass a maintenance gate only
                # when both history and user relations are genuinely absent.
                fresh_chain = bool(
                    not ledger_preexisting
                    and not applied
                    and not existing_application_relations
                )
                acknowledged_gates = _acknowledged_maintenance_gates()
                for path in pending_tx:
                    sql = path.read_text()
                    execution_sql = _runner_owned_transaction_sql(sql)
                    maintenance_gate = _maintenance_gate(sql)
                    if (
                        maintenance_gate is not None
                        and not fresh_chain
                        and maintenance_gate not in acknowledged_gates
                    ):
                        raise RuntimeError(
                            f"maintenance-gated migration {path.name!r} requires "
                            f"{MAINTENANCE_GATES_ENV} to include "
                            f"{maintenance_gate!r}; drain every old mutating "
                            "application/agent writer and use the migration's "
                            "documented no-overlap deployment procedure before "
                            "acknowledging it"
                        )
                    log.info("→ %s", path.name)
                    t0 = time.monotonic()
                    try:
                        # A previous migration may have changed session-level
                        # settings. Reassert the lexer/schema contract at the
                        # final boundary immediately before every file.
                        await _force_runner_session_settings(conn)
                        await conn.execute(execution_sql)
                    except Exception as exc:
                        ms = int((time.monotonic() - t0) * 1000)
                        transaction_failure = (
                            path.name,
                            _checksum(sql),
                            ms,
                            str(exc),
                        )
                        raise
                    ms = int((time.monotonic() - t0) * 1000)
                    await conn.execute(
                        "INSERT INTO public.schema_migrations"
                        "(filename, checksum, execution_ms) "
                        "VALUES($1,$2,$3)",
                        path.name,
                        _checksum(sql),
                        ms,
                    )
                    log.info("✓ %s (%d ms)", path.name, ms)

                if dry_run:
                    raise DryRunRollback()
        except DryRunRollback:
            log.info("dry-run: transactional pass rolled back")
            return
        except Exception:
            # A dry run is observational even when the migration body fails.
            # Its enclosing transaction rolls back bootstrap DDL, migration
            # DDL, and tentative rows; never replace that with a dirty row.
            if not dry_run and transaction_failure is not None:
                await _record_failure_on_connection(conn, *transaction_failure)
            raise

    # Non-transactional pass — run outside any txn, one connection per migration
    # so the runner survives the kinds of operations that demand it (CREATE
    # INDEX CONCURRENTLY, ALTER SYSTEM, etc.). A session advisory lock spans
    # the physical operation, catalog verification (when registered), and its
    # ledger write. Skipped on dry-run.
    if dry_run:
        if non_transactional:
            log.info(
                "dry-run: skipping %d non-transactional migration(s) "
                "(cannot be rolled back)",
                len(non_transactional),
            )
        return

    for path in non_transactional:
        async with pool.acquire() as conn:
            await _force_runner_session_settings(conn)
            await _acquire_notx_lock(conn)
            try:
                # A second runner may have completed this migration while this
                # connection waited for the session lock, so re-read the row.
                applied_row = await conn.fetchrow(
                    "SELECT checksum FROM public.schema_migrations "
                    "WHERE filename = $1 AND success = TRUE",
                    path.name,
                )
                sql = path.read_text()
                checksum = _checksum(sql)
                if applied_row:
                    if applied_row["checksum"] != checksum:
                        raise RuntimeError(
                            f"checksum changed: {path.name} "
                            "(applied migrations are immutable; write a "
                            "superseding migration instead)"
                        )
                    continue

                recovery = NOTX_RECOVERIES.get(path.name)
                log.info("→ %s (non-transactional)", path.name)
                t0 = time.monotonic()
                try:
                    if recovery is None:
                        await conn.execute(sql)
                    else:
                        await _run_concurrent_index_recovery(conn, path, recovery)
                except Exception as exc:
                    ms = int((time.monotonic() - t0) * 1000)
                    await _record_failure_on_connection(
                        conn,
                        path.name,
                        checksum,
                        ms,
                        str(exc),
                    )
                    raise
                ms = int((time.monotonic() - t0) * 1000)
                await _record_success_on_connection(
                    conn,
                    path.name,
                    checksum,
                    ms,
                )
                log.info("✓ %s (%d ms)", path.name, ms)
            finally:
                await conn.execute(
                    "SELECT pg_catalog.pg_advisory_unlock($1)", NOTX_LOCK_ID
                )


async def _main() -> int:
    """Entrypoint for ``python -m orchestrator.database.migrate``.

    Used by CI for the dry-run gate and for ad-hoc local runs against a
    DATABASE_URL. The orchestrator itself drives the runner via lifespan.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dir",
        default=str(Path(__file__).parent / "migrations" / "app"),
        help="Migrations directory (default: ./migrations/app)",
    )
    parser.add_argument(
        "--database-url",
        default=build_postgres_url("POSTGRES", fallback_env="DATABASE_URL"),
        help=(
            "Postgres URL (defaults to a DSN built from "
            "POSTGRES_USER/PASSWORD/HOST/PORT/DB, falling back to $DATABASE_URL)"
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Apply transactional migrations then roll back",
    )
    args = parser.parse_args()

    if not args.database_url:
        parser.error(
            "--database-url is required (or set POSTGRES_USER/PASSWORD plus "
            "POSTGRES_HOST/PORT/DB, or $DATABASE_URL)"
        )

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )

    pool = await asyncpg.create_pool(args.database_url, min_size=1, max_size=2)
    try:
        await run_migrations(pool, Path(args.dir), dry_run=args.dry_run)
    finally:
        await pool.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
