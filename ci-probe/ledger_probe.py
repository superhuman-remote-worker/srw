"""pytest plugin: find what holds up a session child's create in CI.

Active only for tests/test_session_delegate_current_path_real_postgres.py. It
times every step of the create path, watches the event loop for stalls (and
samples the main thread's stack while one lasts), and when a create has been
pending for a while it dumps every asyncio task's stack plus pg_stat_activity
and pg_locks from a separate connection.

Output goes to stderr and to $LEDGER_PROBE_DIR/<worker>.log.
"""

from __future__ import annotations

import asyncio
import functools
import os
from pathlib import Path
import sys
import threading
import time
import traceback

import pytest

TARGET = "tests/test_session_delegate_current_path_real_postgres.py"
_T0 = time.monotonic()
_WORKER = os.environ.get("PYTEST_XDIST_WORKER", "main")
_OUT = Path(os.environ.get("LEDGER_PROBE_DIR", "ledger-probe")) / f"{_WORKER}.log"
_LOCK = threading.Lock()


def log(message: str) -> None:
    line = (
        f"[probe {time.strftime('%H:%M:%S')} +{time.monotonic() - _T0:9.3f}s "
        f"{_WORKER}] {message}"
    )
    with _LOCK:
        print(line, file=sys.stderr, flush=True)
        try:
            _OUT.parent.mkdir(parents=True, exist_ok=True)
            with _OUT.open("a") as handle:
                handle.write(line + "\n")
        except OSError:
            pass


class _State:
    def __init__(self):
        self.loop: asyncio.AbstractEventLoop | None = None
        self.dsn: str | None = None
        self.pending: dict[int, dict] = {}
        self.next_id = 0
        self.stop = threading.Event()
        self.thread: threading.Thread | None = None
        self.main_ident = threading.main_thread().ident


STATE = _State()
_PATCHES: list[tuple[object, str, object]] = []


def _short_stack(frame, limit=30) -> str:
    entries = traceback.extract_stack(frame, limit=limit)
    return "\n".join(
        f"      {e.filename.split('site-packages/')[-1].split('/src/')[-1]}:{e.lineno} {e.name}: {(e.line or '').strip()[:120]}"
        for e in entries
    )


def _patch(owner, name, wrapper_factory):
    original = getattr(owner, name)
    _PATCHES.append((owner, name, original))
    setattr(owner, name, wrapper_factory(original))


def _timed(label, *, track=False, describe=None):
    def factory(original):
        @functools.wraps(original)
        async def wrapper(*args, **kwargs):
            if STATE.loop is None:
                STATE.loop = asyncio.get_running_loop()
            detail = describe(*args, **kwargs) if describe else ""
            started = time.monotonic()
            token = None
            if track:
                STATE.next_id += 1
                token = STATE.next_id
                STATE.pending[token] = {
                    "label": label,
                    "started": started,
                    "task": asyncio.current_task(),
                    "dumps": 0,
                }
            log(f"> {label} {detail}")
            outcome = "ok"
            try:
                return await original(*args, **kwargs)
            except BaseException as exc:
                outcome = f"{type(exc).__name__}: {exc}"[:200]
                raise
            finally:
                if token is not None:
                    STATE.pending.pop(token, None)
                log(
                    f"< {label} {detail} {(time.monotonic() - started) * 1000:.1f} ms {outcome}"
                )

        return wrapper

    return factory


async def _pg_snapshot(dsn: str) -> list[str]:
    import asyncpg

    conn = await asyncpg.connect(dsn, timeout=3)
    try:
        lines = []
        for row in await conn.fetch(
            """
            SELECT pid, state, wait_event_type, wait_event, backend_type,
                   round(extract(epoch FROM now() - xact_start)::numeric, 3) AS xact_s,
                   round(extract(epoch FROM now() - query_start)::numeric, 3) AS query_s,
                   pg_blocking_pids(pid) AS blocked_by,
                   left(regexp_replace(query, '\\s+', ' ', 'g'), 220) AS query
              FROM pg_stat_activity
             WHERE pid <> pg_backend_pid()
            """
        ):
            lines.append(f"    activity {dict(row)}")
        for row in await conn.fetch(
            """
            SELECT pid, locktype, relation::regclass::text AS rel, mode, granted,
                   page, tuple, transactionid::text AS xid
              FROM pg_locks
             WHERE pid <> pg_backend_pid()
               AND (NOT granted OR locktype IN ('tuple', 'transactionid')
                    OR mode IN ('RowShareLock', 'RowExclusiveLock', 'ExclusiveLock',
                                'AccessExclusiveLock', 'ShareLock'))
             ORDER BY pid, granted
            """
        ):
            lines.append(f"    lock {dict(row)}")
        return lines
    finally:
        await conn.close()


def _dump(entry: dict, pending_s: float) -> None:
    log(f"!!! {entry['label']} pending {pending_s:.2f}s — dump {entry['dumps']}")
    frame = sys._current_frames().get(STATE.main_ident)
    if frame is not None:
        log("  main thread stack:\n" + _short_stack(frame))
    loop = STATE.loop
    if loop is not None:
        try:
            tasks = list(asyncio.all_tasks(loop))
        except RuntimeError as exc:
            tasks = []
            log(f"  all_tasks failed: {exc}")
        log(f"  {len(tasks)} tasks")
        for task in tasks:
            try:
                frames = task.get_stack(limit=25)
            except Exception as exc:  # noqa: BLE001
                log(f"  task {task.get_name()} stack failed: {exc}")
                continue
            marker = " <== tracked" if task is entry.get("task") else ""
            rendered = "\n".join(
                f"      {f.f_code.co_filename.split('site-packages/')[-1].split('/src/')[-1]}:{f.f_lineno} {f.f_code.co_name}"
                for f in frames
            )
            log(f"  task {task.get_name()} {task._state}{marker}:\n{rendered}")
    if STATE.dsn:
        try:
            lines = asyncio.run(_pg_snapshot(STATE.dsn))
            log("  postgres:\n" + "\n".join(lines))
        except Exception as exc:  # noqa: BLE001
            log(f"  postgres snapshot failed: {type(exc).__name__}: {exc}")


def _watchdog() -> None:
    lag_started = None
    last_stack = None
    while not STATE.stop.is_set():
        loop = STATE.loop
        if loop is not None and not loop.is_closed():
            done = threading.Event()
            sent = time.monotonic()
            try:
                loop.call_soon_threadsafe(done.set)
            except RuntimeError:
                STATE.loop = None
                continue
            if not done.wait(0.2):
                if lag_started is None:
                    lag_started = sent
                frame = sys._current_frames().get(STATE.main_ident)
                stack = _short_stack(frame, limit=18) if frame is not None else "?"
                if stack != last_stack:
                    log(
                        f"loop blocked {time.monotonic() - lag_started:.2f}s so far, main thread:\n{stack}"
                    )
                    last_stack = stack
                done.wait(5)
            elif lag_started is not None:
                log(f"loop unblocked after {time.monotonic() - lag_started:.2f}s")
                lag_started = None
                last_stack = None
        now = time.monotonic()
        for entry in list(STATE.pending.values()):
            pending_s = now - entry["started"]
            if (entry["dumps"] == 0 and pending_s > 1.0) or (
                entry["dumps"] == 1 and pending_s > 3.5
            ):
                _dump(entry, pending_s)
                entry["dumps"] += 1
        STATE.stop.wait(0.05)


def _install() -> None:
    import asyncpg.connect_utils as connect_utils
    import asyncpg.connection as asyncpg_connection
    import asyncpg.pool as asyncpg_pool
    import httpx

    import agent.api.orchestrator_client as orchestrator_client
    import agent.subagents.runtime as runtime
    import agent.subagents.session_persistence as session_persistence
    import orchestrator.database.postgres as postgres

    def describe_db(self, *args, **kwargs):
        STATE.dsn = getattr(self, "_connection_string", None) or STATE.dsn
        return ""

    def describe_asgi(self, request, *args, **kwargs):
        return f"{request.method} {request.url.path}"

    _patch(runtime, "build_child", _timed("build_child"))
    _patch(
        session_persistence.SessionSubagentLedger,
        "open",
        _timed("ledger.open", track=True),
    )
    _patch(
        session_persistence.SessionSubagentLedger,
        "_authority",
        _timed("ledger._authority"),
    )
    _patch(
        orchestrator_client.OrchestratorClient,
        "create_session_subagent_thread",
        _timed("client.create_session_subagent_thread"),
    )
    _patch(
        httpx.ASGITransport,
        "handle_async_request",
        _timed("asgi", describe=describe_asgi),
    )
    _patch(
        postgres.PostgresDB,
        "create_session_subagent_thread",
        _timed("db.create_session_subagent_thread", describe=describe_db),
    )
    _patch(
        postgres,
        "require_session_parent_authority",
        _timed("require_session_parent_authority"),
    )
    _patch(connect_utils, "_connect_addr", _timed("asyncpg new connection"))
    _patch(
        asyncpg_connection.Connection,
        "_introspect_types",
        _timed("asyncpg introspect types"),
    )

    original_aenter = asyncpg_pool.PoolAcquireContext.__aenter__

    async def timed_aenter(self):
        started = time.monotonic()
        pool = self.pool
        try:
            return await original_aenter(self)
        finally:
            waited = time.monotonic() - started
            if waited > 0.02:
                log(
                    f"pool acquire waited {waited * 1000:.1f} ms "
                    f"(max={pool._maxsize} queue_free={pool._queue.qsize()})"
                )

    _PATCHES.append((asyncpg_pool.PoolAcquireContext, "__aenter__", original_aenter))
    asyncpg_pool.PoolAcquireContext.__aenter__ = timed_aenter

    STATE.stop.clear()
    STATE.thread = threading.Thread(target=_watchdog, name="ledger-probe", daemon=True)
    STATE.thread.start()


def _uninstall() -> None:
    STATE.stop.set()
    if STATE.thread is not None:
        STATE.thread.join(timeout=10)
    while _PATCHES:
        owner, name, original = _PATCHES.pop()
        setattr(owner, name, original)
    STATE.loop = None
    STATE.pending.clear()


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_protocol(item, nextitem):
    if not item.nodeid.startswith(TARGET):
        yield
        return
    log(f"=== start {item.nodeid}")
    _install()
    try:
        yield
    finally:
        _uninstall()
        log(f"=== end {item.nodeid}")


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    if item.nodeid.startswith(TARGET):
        report = outcome.get_result()
        log(f"--- {item.nodeid} {call.when} {report.outcome} {call.duration:.3f}s")
