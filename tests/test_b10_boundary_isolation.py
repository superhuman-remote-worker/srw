"""B10's modules and its assigned caller stand on their own.

R1.B10 moved the session surface (detail, state, controls, tool groups),
history and citations, the SSE/input/queue/interrupt transport, permission
decisions and magic links, and the attention/permission-reminder/decision-wake
bodies out of ``orchestrator.main``. It also closed the one production caller
the ledger assigned it: ``services/session_wake.py``'s lazy application lookup
of the usage ledger for the Officer daily-ceiling brake.

These cases hold that closure. A regression to a process-wide lookup — a router
reading module state instead of its own application's, a second application's
store or ledger answering for the first, or the drain falling back to the
application module — fails here rather than in production.
"""

from __future__ import annotations

import ast
import asyncio
import contextlib
import dataclasses
import inspect
import pathlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from orchestrator.routers import (
    thread_history,
    thread_permissions,
    thread_session,
    thread_transport,
)
from orchestrator.schemas import thread_session as thread_session_schemas
from orchestrator.schemas import thread_transport as thread_transport_schemas
from orchestrator.services import (
    magic_link_pages,
    pinned_forwarding,
    session_attention,
    session_tool_view,
    session_wake,
    stateless_input_admission,
    thread_event_stream,
    thread_permissions as thread_permission_operations,
    thread_projection,
    thread_turn_locks,
)

from ._mounted_router import mount_router

ROOT = pathlib.Path(__file__).resolve().parents[1]
MAIN = ROOT / "src" / "orchestrator" / "main.py"
THREAD_ID = "11111111-2222-4333-8444-555555555555"

B10_MODULES = [
    thread_history,
    thread_permissions,
    thread_session,
    thread_transport,
    thread_session_schemas,
    thread_transport_schemas,
    magic_link_pages,
    pinned_forwarding,
    session_attention,
    session_tool_view,
    session_wake,
    stateless_input_admission,
    thread_event_stream,
    thread_permission_operations,
    thread_projection,
    thread_turn_locks,
]


def _imports_of(module) -> list[str]:
    tree = ast.parse(pathlib.Path(module.__file__).read_text())
    names: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            base = node.module or ""
            names.append(base)
            names.extend(f"{base}.{alias.name}" for alias in node.names)
    return names


@pytest.mark.parametrize("module", B10_MODULES, ids=lambda m: m.__name__)
def test_b10_module_never_imports_the_application_module(module) -> None:
    offending = [
        name
        for name in _imports_of(module)
        if name == "orchestrator.main"
        or name.startswith("orchestrator.main.")
        or name == "orchestrator.main"
    ]
    assert offending == []
    source = pathlib.Path(module.__file__).read_text()
    assert 'import_module("orchestrator.main")' not in source
    assert 'sys.modules["orchestrator.main"]' not in source


# --------------------------------------------------------------------------- #
# Routers resolve collaborators only from their own application
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ("resolver", "attribute"),
    [
        (
            thread_session.get_thread_session_dependencies,
            "thread_session_dependencies_factory",
        ),
        (
            thread_history.get_thread_history_dependencies,
            "thread_history_dependencies_factory",
        ),
        (
            thread_transport.get_thread_transport_dependencies,
            "thread_transport_dependencies_factory",
        ),
        (
            thread_permissions.get_thread_permission_dependencies,
            "thread_permission_dependencies_factory",
        ),
    ],
    ids=lambda value: value if isinstance(value, str) else "",
)
def test_router_resolver_reads_only_the_requesting_application(
    resolver, attribute
) -> None:
    helper = ast.parse(inspect.getsource(resolver)).body[0]
    assert isinstance(helper, ast.FunctionDef)
    assert [argument.arg for argument in helper.args.args] == ["request"]
    assert not [
        node
        for node in ast.walk(helper)
        if isinstance(node, (ast.Import, ast.ImportFrom))
    ]
    assert ast.unparse(helper.body[-1]) == f"return request.app.state.{attribute}()"


def _owner_gate():
    async def gate(request, store, thread_id):
        del request
        store.gated.append(thread_id)
        return {"id": "owner-1", "is_admin": False}, {
            "id": thread_id,
            "user_id": "owner-1",
            "execution_lane": "stateless",
            "metadata": {},
        }

    return gate


class _RenameStore:
    def __init__(self) -> None:
        self.gated: list[str] = []
        self.renamed: list[tuple[str, str]] = []

    async def update_thread_title(self, thread_id: str, title: str) -> None:
        self.renamed.append((thread_id, title))


def _session_app(store):
    return mount_router(
        thread_session.router,
        factories={
            "thread_session_dependencies_factory": (
                lambda: thread_session.ThreadSessionDependencies(
                    store=store,
                    require_thread_owner=_owner_gate(),
                    require_approved_user=AsyncMock(),
                    resolve_cloud_session_url=lambda *_: None,
                    resolve_session_config=AsyncMock(),
                    enforce_session_create_grants=AsyncMock(),
                    tool_view=SimpleNamespace(),
                )
            )
        },
    )


def test_session_routes_use_the_store_of_their_own_application() -> None:
    first, second = _RenameStore(), _RenameStore()
    one = TestClient(_session_app(first))
    two = TestClient(_session_app(second))

    assert (
        one.patch(
            f"/api/persistent/threads/{THREAD_ID}", json={"title": "A"}
        ).status_code
        == 200
    )
    assert (
        two.patch(
            f"/api/persistent/threads/{THREAD_ID}", json={"title": "B"}
        ).status_code
        == 200
    )

    assert first.renamed == [(THREAD_ID, "A")] and first.gated == [THREAD_ID]
    assert second.renamed == [(THREAD_ID, "B")] and second.gated == [THREAD_ID]


class _QueueStore:
    def __init__(self) -> None:
        self.gated: list[str] = []
        self.acquired = 0

    @contextlib.asynccontextmanager
    async def acquire(self):
        self.acquired += 1
        yield SimpleNamespace(fetchrow=AsyncMock(return_value=None))


def _transport_app(store, turn_locks):
    return mount_router(
        thread_transport.router,
        factories={
            "thread_transport_dependencies_factory": (
                lambda: thread_transport.ThreadTransportDependencies(
                    store=store,
                    require_thread_owner=_owner_gate(),
                    require_approved_user=AsyncMock(),
                    forwarding=SimpleNamespace(),
                    stateless_input=SimpleNamespace(),
                    turn_locks=turn_locks,
                )
            )
        },
    )


def test_transport_routes_use_the_store_of_their_own_application() -> None:
    first, second = _QueueStore(), _QueueStore()
    one = TestClient(_transport_app(first, thread_turn_locks.ThreadTurnLocks()))
    TestClient(_transport_app(second, thread_turn_locks.ThreadTurnLocks()))

    response = one.get(f"/api/persistent/threads/{THREAD_ID}/queue")

    assert response.status_code == 200
    assert response.json()["queue"]["state"] == "none"
    assert (first.gated, first.acquired) == ([THREAD_ID], 1)
    assert (second.gated, second.acquired) == ([], 0)


def test_turn_locks_belong_to_one_application() -> None:
    first = thread_turn_locks.ThreadTurnLocks()
    second = thread_turn_locks.ThreadTurnLocks()

    lock = first.ensure(THREAD_ID, 3)

    assert first.ensure(THREAD_ID, 3) is lock
    assert second.ensure(THREAD_ID, 3) is not lock
    assert (THREAD_ID, 3) in first.locks and (THREAD_ID, 3) in second.locks
    assert "_thread_turn_locks" not in vars(thread_transport)


def test_the_application_owns_one_turn_lock_registry() -> None:
    import orchestrator.main as main

    first = main._thread_transport_dependencies().turn_locks
    second = main._thread_transport_dependencies().turn_locks
    assert first is second is main._thread_turn_locks
    assert isinstance(first, thread_turn_locks.ThreadTurnLocks)


# --------------------------------------------------------------------------- #
# The Officer daily-ceiling metering is bound per store by composition
# --------------------------------------------------------------------------- #


@pytest.fixture
def _clean_metering():
    before = dict(session_wake._METERING_BY_STORE)
    yield
    session_wake._METERING_BY_STORE.clear()
    session_wake._METERING_BY_STORE.update(before)


def _ceilinged_thread() -> dict:
    return {
        "id": THREAD_ID,
        "metadata": {
            "config_override": {"officer": {"enabled": True, "daily_token_ceiling": 10}}
        },
    }


def _ledger(tokens: int):
    return SimpleNamespace(
        is_available=True,
        query_usage=AsyncMock(
            return_value={"by_category": [{"unit": "prompt-token", "quantity": tokens}]}
        ),
    )


@pytest.mark.asyncio
@pytest.mark.usefixtures("_clean_metering")
async def test_two_stores_reach_two_ledgers() -> None:
    store_a, store_b = SimpleNamespace(name="a"), SimpleNamespace(name="b")
    over, under = _ledger(10), _ledger(9)
    session_wake.bind_officer_wake_metering(store_a, lambda: over)
    session_wake.bind_officer_wake_metering(store_b, lambda: under)

    deferred_a = await session_wake._officer_ceiling_deferral(
        store_a, _ceilinged_thread()
    )
    deferred_b = await session_wake._officer_ceiling_deferral(
        store_b, _ceilinged_thread()
    )

    assert deferred_a is not None
    assert deferred_b is None
    over.query_usage.assert_awaited_once()
    under.query_usage.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.usefixtures("_clean_metering")
async def test_an_unbound_store_fails_open_and_reaches_no_other_ledger() -> None:
    bound = _ledger(10)
    session_wake.bind_officer_wake_metering(SimpleNamespace(), lambda: bound)

    assert (
        await session_wake._officer_ceiling_deferral(
            SimpleNamespace(), _ceilinged_thread()
        )
        is None
    )
    bound.query_usage.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.usefixtures("_clean_metering")
async def test_the_provider_is_read_per_check_so_a_later_ledger_is_seen() -> None:
    store = SimpleNamespace()
    current: dict = {"ledger": None}
    session_wake.bind_officer_wake_metering(store, lambda: current["ledger"])

    assert (
        await session_wake._officer_ceiling_deferral(store, _ceilinged_thread()) is None
    )
    current["ledger"] = _ledger(10)
    assert (
        await session_wake._officer_ceiling_deferral(store, _ceilinged_thread())
        is not None
    )


@pytest.mark.asyncio
@pytest.mark.usefixtures("_clean_metering")
async def test_unbinding_forgets_only_that_store() -> None:
    kept, dropped = SimpleNamespace(), SimpleNamespace()
    session_wake.bind_officer_wake_metering(kept, lambda: _ledger(10))
    session_wake.bind_officer_wake_metering(dropped, lambda: _ledger(10))

    session_wake.unbind_officer_wake_metering(dropped)

    assert await session_wake._officer_ceiling_deferral(kept, _ceilinged_thread())
    assert (
        await session_wake._officer_ceiling_deferral(dropped, _ceilinged_thread())
        is None
    )


def _lifespan() -> ast.AsyncFunctionDef:
    """The application's startup body (R1.B11 moved it from ``lifespan``
    into ``_start_application``; ``lifespan`` now only sequences it)."""
    tree = ast.parse(MAIN.read_text())
    return next(
        node
        for node in tree.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "_start_application"
    )


def test_the_composition_step_is_a_plain_call_and_lifespan_stays_a_context_manager() -> (
    None
):
    """Adding the bind before ``lifespan`` must not steal its decorator.

    The first frozen candidate did exactly that: ``@asynccontextmanager`` ended
    up on ``_bind_officer_wake_metering``, leaving ``lifespan`` a bare async
    generator (the boot tests caught it). Pin both halves structurally.
    """
    tree = ast.parse(MAIN.read_text())
    nodes = {
        node.name: node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    bind = nodes["_bind_officer_wake_metering"]
    assert isinstance(bind, ast.FunctionDef) and bind.decorator_list == []
    lifespan = nodes["lifespan"]
    assert [ast.unparse(d) for d in lifespan.decorator_list] == ["asynccontextmanager"]

    import contextlib

    import orchestrator.main as main

    assert isinstance(main.lifespan(object()), contextlib.AbstractAsyncContextManager)


def test_lifespan_binds_the_ledger_after_building_it_and_before_the_sweeper() -> None:
    body = ast.unparse(_lifespan())
    built = body.index("usage_ledger = UsageLedger(")
    bound = body.index("_bind_officer_wake_metering()")
    sweeper = body.index("session_wake_sweeper_loop(postgres_db, _shutdown_event)")
    assert built < bound < sweeper
    assert body.count("_bind_officer_wake_metering()") == 1


def test_the_composition_step_binds_the_application_store_to_its_ledger(
    monkeypatch, _clean_metering
) -> None:
    import orchestrator.main as main

    store = SimpleNamespace()
    ledger = SimpleNamespace(is_available=True)
    monkeypatch.setattr(main, "postgres_db", store)
    monkeypatch.setattr(main, "usage_ledger", None)
    main._bind_officer_wake_metering()

    assert session_wake._bound_usage_ledger(store) is None
    monkeypatch.setattr(main, "usage_ledger", ledger)
    assert session_wake._bound_usage_ledger(store) is ledger


# --------------------------------------------------------------------------- #
# Attention bodies are composed by the application; B11 keeps the tasks
# --------------------------------------------------------------------------- #


def test_attention_dependencies_read_the_late_recycler_through_a_provider() -> None:
    fields = {
        field.name
        for field in dataclasses.fields(session_attention.SessionAttentionDependencies)
    }
    assert {"persistent_thread_recycler", "thread_retirement_operations"} <= fields
    wake = inspect.getsource(session_attention.wake_after_permission_decision)
    assert "dependencies.persistent_thread_recycler()" in wake
    sweeper = inspect.getsource(session_attention.attention_sleep_sweeper)
    assert "dependencies.thread_retirement_operations()" in sweeper


def test_lifespan_keeps_leader_gated_tasks_over_the_moved_bodies() -> None:
    body = ast.unparse(_lifespan())
    for name in ("thread_permission_notify_sweeper", "attention_sleep_sweeper"):
        call = f"session_attention_operations.{name}"
        assert call in body
        # R1.B11: lifespan starts leader-only loops through its task set.
        at = body.index(call)
        gated = body.rindex("tasks.start_leader_gated(", 0, at)
        plain = body.rfind("tasks.start(", 0, at)
        assert gated > plain


@pytest.mark.asyncio
async def test_application_attention_dependencies_see_the_recycler_bound_later(
    monkeypatch,
) -> None:
    import orchestrator.main as main

    monkeypatch.setattr(main, "_persistent_thread_recycler", None)
    dependencies = main._session_attention_dependencies()
    assert dependencies.persistent_thread_recycler() is None
    recycler = object()
    monkeypatch.setattr(main, "_persistent_thread_recycler", recycler)
    assert dependencies.persistent_thread_recycler() is recycler
    await asyncio.sleep(0)


# --------------------------------------------------------------------------- #
# Main keeps composition only
# --------------------------------------------------------------------------- #

MOVED_FROM_MAIN = {
    "_redact_thread_metadata",
    "get_thread",
    "get_thread_session_state",
    "ThreadControlRequest",
    "submit_thread_control",
    "_session_tool_grants",
    "get_thread_tool_groups",
    "ToolGroupPreviewRequest",
    "preview_tool_groups",
    "update_thread",
    "get_thread_citations",
    "_stamp_tool_categories",
    "get_thread_messages_history",
    "_thread_turn_inflight",
    "_ensure_thread_turn_lock",
    "_schedule_turn_lock_cleanup",
    "_resolve_thread_for_forwarding",
    "_require_forwardable_pinned_binding",
    "_binding_runtime_authority",
    "_revalidate_pinned_forwarding_binding",
    "_forward_to_agent",
    "_no_cursor_replay_start",
    "THREAD_EVENTS_EPOCH_RECHECK_S",
    "THREAD_CLIENT_PRESENCE_RENEW_S",
    "THREAD_CLIENT_PRESENCE_TTL_S",
    "thread_event_stream",
    "ThreadInputRequest",
    "_load_thread_for_owner",
    "thread_input",
    "thread_queue_state",
    "thread_queue_retry",
    "ThreadInterruptRequest",
    "thread_interrupt",
    "ThreadApproveRequest",
    "thread_approve",
    "_decide_permission_request",
    "_MAGIC_EXTEND_CAP",
    "_magic_link_confirmation_page",
    "_magic_link_result_page",
    "magic_link_get",
    "magic_link_post",
    "magic_link_extend",
    "_phase5_wake_stateless_if_suspended",
    "_phase5_wake_if_suspended",
    "thread_permission_notify_sweeper",
    "_ATTENTION_SLEEP_INTERVAL_S",
    "_ATTENTION_SLEEP_MINUTES",
    "attention_sleep_sweeper",
}


def test_main_no_longer_defines_the_moved_operations() -> None:
    tree = ast.parse(MAIN.read_text())
    defined = set()
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            defined.add(node.name)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            defined.add(node.target.id)
        elif isinstance(node, ast.Assign):
            defined.update(t.id for t in node.targets if isinstance(t, ast.Name))
    assert defined & MOVED_FROM_MAIN == set()


def test_the_one_retained_wrapper_has_its_named_operator_consumer() -> None:
    harness = (
        ROOT / "src" / "orchestrator" / "operator_cli" / "stateless_wake_acceptance.py"
    ).read_text()
    assert "self.main._thread_input_stateless(" in harness
    tree = ast.parse(MAIN.read_text())
    wrapper = next(
        node
        for node in tree.body
        if isinstance(node, ast.AsyncFunctionDef)
        and node.name == "_thread_input_stateless"
    )
    assert "stateless_wake_acceptance" in (ast.get_docstring(wrapper) or "")
    assert "admit_stateless_input" in ast.unparse(wrapper)
