"""R1.B12 T1: the application's request-spawned task registries at shutdown.

``_stop_application`` stops the lifecycle's background tasks, drains the
dispatch state and the KB reindex registry, then closes clients and stores.
Every other application-owned registry keeps strong references to tasks that a
request spawned — cloud engage/stage, stateless workspace reconciles, project
repairs, attach-abort successors and late session-folder provisioning — and
nothing cancels or awaits them, so they are cut off at event-loop teardown,
after the pools they use have closed.

The unit tests pin each registry's ``drain()``: an in-flight task is cancelled
and awaited (its own cancellation path has run when ``drain`` returns), the
caller's own task is never cancelled, a second drain is a no-op, and the
registry is left empty and usable. The lifecycle tests reuse the R1.B11
harness: real in-flight tasks sit in every registry while the lifespan shuts
down, and each must observe its cancellation before the first client closes.
"""

from __future__ import annotations

import asyncio
import contextlib
from types import SimpleNamespace
from typing import Any

import pytest

from orchestrator.services import (
    application_tasks,
    cloud_task_registry,
    project_provisioning,
    session_attach_recovery,
    stateless_workspace_scheduler,
    thread_resume,
)
from tests.test_b11_lifespan_characterization import (
    SHUTDOWN_CLOSURE,
    _closure,
    _Recorder,
    _run_lifespan,
    main,
)

_XFAIL = pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason=(
        "R1.B12 T1: the application's request-spawned task registries have no "
        "shutdown drain; their tasks outlive the stores"
    ),
)

THREAD = "11111111-1111-4111-8111-111111111111"
GENERATION = "22222222-2222-4222-8222-222222222222"
ATTACH_TOKEN = "33333333-3333-4333-8333-333333333333"
AGENT = "44444444-4444-4444-8444-444444444444"
SUCCESSOR = "55555555-5555-4555-8555-555555555555"

# Captured at import: inside the lifespan harness ``asyncio.create_task`` is a
# recorder that hands back stub tasks.
_REAL_CREATE_TASK = asyncio.create_task


def _drain_of(owner: Any, name: str = "drain"):
    drain = getattr(owner, name, None)
    assert drain is not None, f"{owner!r} offers no {name}()"
    return drain


class _InFlight:
    """A request-spawned body that blocks until cancelled and notes its cleanup."""

    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.cleaned_up: list[str] = []

    async def body(self, label: str) -> None:
        self.started.set()
        try:
            await asyncio.Event().wait()
        finally:
            self.cleaned_up.append(label)


async def _settle() -> None:
    for _ in range(3):
        await asyncio.sleep(0)


# --------------------------------------------------------------------------- #
# The generic helper for the two plain dicts main owns
# --------------------------------------------------------------------------- #


@_XFAIL
@pytest.mark.asyncio
async def test_drain_task_mapping_cancels_and_awaits_every_in_flight_task():
    drain_task_mapping = _drain_of(application_tasks, "drain_task_mapping")
    work = _InFlight()

    first = asyncio.create_task(work.body("first"))
    second = asyncio.create_task(work.body("second"))
    tasks: dict[Any, asyncio.Task[None]] = {("a", 1): first, "b": second}
    await _settle()

    await drain_task_mapping(tasks)

    assert first.cancelled() and second.cancelled()
    # Awaited, not just signalled: each task's own cancellation path ran.
    assert sorted(work.cleaned_up) == ["first", "second"]
    assert tasks == {}
    # Idempotent, and the mapping stays usable.
    await drain_task_mapping(tasks)
    third = asyncio.create_task(work.body("third"))
    tasks["c"] = third
    await drain_task_mapping(tasks)
    assert third.cancelled() and tasks == {}


@_XFAIL
@pytest.mark.asyncio
async def test_drain_task_mapping_never_cancels_the_calling_task():
    drain_task_mapping = _drain_of(application_tasks, "drain_task_mapping")
    work = _InFlight()
    other = asyncio.create_task(work.body("other"))
    tasks: dict[str, asyncio.Task[Any]] = {"other": other}

    async def drains_its_own_registry() -> str:
        tasks["self"] = asyncio.current_task()  # type: ignore[assignment]
        await drain_task_mapping(tasks)
        return "survived"

    caller = asyncio.create_task(drains_its_own_registry())
    assert await asyncio.wait_for(caller, timeout=2) == "survived"
    assert other.cancelled()
    # The caller's own slot is left for its own owner to clear.
    assert set(tasks) == {"self"}


# --------------------------------------------------------------------------- #
# Per-registry drains
# --------------------------------------------------------------------------- #


@_XFAIL
@pytest.mark.asyncio
async def test_cloud_task_registry_drain_cancels_engage_and_stage_tasks():
    registry = cloud_task_registry.CloudTaskRegistry()
    drain = _drain_of(registry)
    work = _InFlight()

    engage = asyncio.create_task(work.body("engage"))
    registry.protected_engage_register((THREAD, GENERATION), engage)
    registry.stage_start("stage-key", lambda: work.body("stage"))
    stage = registry.cloud_stage_tasks["stage-key"]
    await _settle()
    # A stage scheduled but not yet run when shutdown begins: its coroutine's
    # own ``finally`` never executes, so only the drain can free the slot.
    registry.stage_start("late-stage-key", lambda: work.body("late-stage"))
    late_stage = registry.cloud_stage_tasks["late-stage-key"]

    await drain()

    assert engage.cancelled() and stage.cancelled() and late_stage.cancelled()
    assert sorted(work.cleaned_up) == ["engage", "stage"]
    assert registry.protected_engage_tasks == {}
    assert registry.cloud_stage_tasks == {}
    await drain()
    # Usable afterwards: the same key can be staged again.
    registry.stage_start("stage-key", lambda: work.body("again"))
    assert registry.stage_has("stage-key")
    await drain()
    assert registry.cloud_stage_tasks == {}


@_XFAIL
@pytest.mark.asyncio
async def test_stateless_workspace_ensure_registry_drain_cancels_the_reconcile():
    registry = stateless_workspace_scheduler.StatelessWorkspaceEnsureRegistry()
    drain = _drain_of(registry)
    work = _InFlight()

    async def ensure_session_workspace(thread_id: str, **_kwargs: Any) -> None:
        await work.body(thread_id)

    task = stateless_workspace_scheduler.schedule_stateless_workspace_ensure(
        THREAD,
        dependencies=stateless_workspace_scheduler.StatelessWorkspaceScheduleDependencies(
            store=object(),
            provisioner=object(),
            suspension=object(),
            registry=registry,
            ensure_session_workspace=ensure_session_workspace,
        ),
    )
    await work.started.wait()

    await drain()

    assert task.cancelled()
    assert work.cleaned_up == [THREAD]
    assert registry.in_flight() == {}
    await drain()


@_XFAIL
@pytest.mark.asyncio
async def test_project_repair_state_drain_cancels_background_repairs():
    state = project_provisioning.ProjectRepairState()
    drain = _drain_of(state)
    work = _InFlight()

    scheduled = project_provisioning.fire_background_repair(
        "project:1", work.body("repair"), dependencies=SimpleNamespace(repair=state)
    )
    assert scheduled
    (task,) = tuple(state.bg_repair_tasks)
    await work.started.wait()

    await drain()

    assert task.cancelled()
    assert work.cleaned_up == ["repair"]
    assert state.bg_repair_tasks == set()
    await drain()


@_XFAIL
@pytest.mark.asyncio
async def test_late_cloud_setup_tasks_are_drained_by_the_mapping_helper():
    drain_task_mapping = _drain_of(application_tasks, "drain_task_mapping")
    late: dict[str, asyncio.Task[None]] = {}
    work = _InFlight()
    task = asyncio.create_task(work.body("late-cloud"))
    thread_resume.register_late_cloud_setup(
        THREAD, task, dependencies=SimpleNamespace(late_cloud_setup_tasks=late)
    )
    await work.started.wait()

    await drain_task_mapping(late)

    assert task.cancelled()
    assert work.cleaned_up == ["late-cloud"]
    assert late == {}


class _OutcomeStore:
    """The append-only abort-outcome table, and every statement run against it."""

    def __init__(self) -> None:
        self.outcomes = [
            {
                "thread_id": THREAD,
                "retired_runtime_generation": GENERATION,
                "retired_attach_token": ATTACH_TOKEN,
                "retired_agent_id": AGENT,
                "successor_generation": SUCCESSOR,
                "quiescence_protocol": "tmux_kill_v1",
                "workspace_generation": None,
                "workspace_runtime_incarnation": None,
            }
        ]
        self.statements: list[str] = []

    @contextlib.asynccontextmanager
    async def acquire(self):
        yield self

    async def fetchrow(self, sql: str, *args: Any):
        self.statements.append(sql)
        for row in self.outcomes:
            if (
                row["thread_id"],
                row["retired_runtime_generation"],
                row["retired_attach_token"],
                row["retired_agent_id"],
            ) == args:
                return dict(row)
        return None

    async def execute(self, sql: str, *_args: Any):  # pragma: no cover - guard
        self.statements.append(sql)
        return "UPDATE 0"


def _recovery_dependencies(store, successor_tasks, reconcile):
    return session_attach_recovery.SessionAttachRecoveryDependencies(
        store=store,
        container_provisioner=object(),
        docker_provisioner=object(),
        workspace_suspension_service=object(),
        ensure_session_workspace=None,  # type: ignore[arg-type]
        thread_project_ids=None,  # type: ignore[arg-type]
        reconcile_attach_abort_successor=reconcile,
        provision_or_assign=None,  # type: ignore[arg-type]
        successor_tasks=successor_tasks,
    )


def _schedule_successor(dependencies):
    return session_attach_recovery.schedule_attach_abort_successor(
        THREAD,
        retired_runtime_generation=GENERATION,
        retired_attach_token=ATTACH_TOKEN,
        retired_agent_id=AGENT,
        dependencies=dependencies,
    )


@_XFAIL
@pytest.mark.asyncio
async def test_attach_abort_successor_drain_keeps_the_durable_outcome():
    drain_task_mapping = _drain_of(application_tasks, "drain_task_mapping")
    store = _OutcomeStore()
    durable_before = [dict(row) for row in store.outcomes]
    successor_tasks: dict[Any, asyncio.Task[None]] = {}
    work = _InFlight()

    async def reconcile_blocks(candidate) -> bool:
        await work.body(candidate["successor_generation"])
        return True

    task = _schedule_successor(
        _recovery_dependencies(store, successor_tasks, reconcile_blocks)
    )
    await work.started.wait()

    await drain_task_mapping(successor_tasks)

    assert task.cancelled()
    assert work.cleaned_up == [SUCCESSOR]
    assert successor_tasks == {}
    # The drain neither wrote nor cleared the outcome: the only statement is
    # the read that found it, and the append-only row is unchanged.
    assert len(store.statements) == 1
    assert store.statements[0].lstrip().upper().startswith("SELECT")
    assert store.outcomes == durable_before

    # What the next process's durable scan does with it: schedule the same
    # exact retired edge again and reconcile the same successor.
    reconciled: list[str] = []

    async def reconcile_now(candidate) -> bool:
        reconciled.append(candidate["successor_generation"])
        return True

    next_process_tasks: dict[Any, asyncio.Task[None]] = {}
    await _schedule_successor(
        _recovery_dependencies(store, next_process_tasks, reconcile_now)
    )
    assert reconciled == [SUCCESSOR]


# --------------------------------------------------------------------------- #
# Lifecycle: every registry is drained before the first client closes
# --------------------------------------------------------------------------- #

_FIRST_CLOSE = "nats_bridge.disconnect"
_STORES_AND_CLIENTS = [
    event for event in SHUTDOWN_CLOSURE if event != "kb_datasource_tasks.drain"
]


class _RegistryTasks:
    """Real in-flight tasks, one per registry, each noting its cancellation."""

    LABELS = (
        "attach_abort_successor",
        "stateless_workspace_ensure",
        "late_cloud_setup",
        "protected_engage",
        "cloud_stage",
        "project_repair",
    )

    def __init__(self, log: list[str]) -> None:
        self._log = log
        self.tasks: dict[str, asyncio.Task[None]] = {}
        self._keys = {
            "attach_abort_successor": (THREAD, GENERATION, ATTACH_TOKEN, AGENT),
            "stateless_workspace_ensure": THREAD,
            "late_cloud_setup": THREAD,
            "protected_engage": (THREAD, GENERATION),
            "cloud_stage": f"b12-stage-{THREAD}",
        }

    async def _body(self, label: str) -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self._log.append(f"cancelled:{label}")
            raise

    async def place(self) -> None:
        # The harness replaces ``asyncio.create_task`` with a recorder; these
        # must be real tasks, so they are created on the loop directly.
        loop = asyncio.get_running_loop()
        for label in self.LABELS:
            self.tasks[label] = loop.create_task(self._body(label))
        keys = self._keys
        main._attach_abort_successor_tasks[keys["attach_abort_successor"]] = self.tasks[
            "attach_abort_successor"
        ]
        main._stateless_workspace_ensure_registry.register(
            keys["stateless_workspace_ensure"],
            self.tasks["stateless_workspace_ensure"],
        )
        main._late_cloud_setup_tasks[keys["late_cloud_setup"]] = self.tasks[
            "late_cloud_setup"
        ]
        main.cloud_task_registry.protected_engage_register(
            keys["protected_engage"], self.tasks["protected_engage"]
        )
        main.cloud_task_registry.cloud_stage_tasks[keys["cloud_stage"]] = self.tasks[
            "cloud_stage"
        ]
        main._project_repair_state.bg_repair_tasks.add(self.tasks["project_repair"])
        await _settle()

    async def cleanup(self) -> None:
        pending = [task for task in self.tasks.values() if not task.done()]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        keys = self._keys
        main._attach_abort_successor_tasks.pop(keys["attach_abort_successor"], None)
        main._stateless_workspace_ensure_registry.discard(
            keys["stateless_workspace_ensure"]
        )
        main._late_cloud_setup_tasks.pop(keys["late_cloud_setup"], None)
        main.cloud_task_registry.protected_engage_tasks.pop(
            keys["protected_engage"], None
        )
        main.cloud_task_registry.cloud_stage_tasks.pop(keys["cloud_stage"], None)
        main._project_repair_state.bg_repair_tasks.discard(
            self.tasks.get("project_repair")
        )


@_XFAIL
@pytest.mark.asyncio
async def test_shutdown_cancels_every_registry_task_before_any_client_closes(
    monkeypatch,
):
    recorder = _Recorder()
    placed = _RegistryTasks(recorder.events)

    async def _inside(_world):
        await placed.place()

    try:
        await _run_lifespan(monkeypatch, recorder, inside=_inside)
        events = recorder.events
        first_close = events.index(_FIRST_CLOSE)
        for label in _RegistryTasks.LABELS:
            assert f"cancelled:{label}" in events, f"{label} was never cancelled"
            assert events.index(f"cancelled:{label}") < first_close, label
        assert all(task.done() for task in placed.tasks.values())
        assert main._attach_abort_successor_tasks == {}
        assert main._stateless_workspace_ensure_registry.in_flight() == {}
        assert main._late_cloud_setup_tasks == {}
        assert main.cloud_task_registry.protected_engage_tasks == {}
        assert main.cloud_task_registry.cloud_stage_tasks == {}
        assert main._project_repair_state.bg_repair_tasks == set()
        assert _closure(recorder) == SHUTDOWN_CLOSURE
    finally:
        await placed.cleanup()


@_XFAIL
@pytest.mark.asyncio
async def test_attach_abort_successor_is_stopped_and_its_outcome_survives_shutdown(
    monkeypatch,
):
    recorder = _Recorder()
    outcome_store = _OutcomeStore()
    durable_before = [dict(row) for row in outcome_store.outcomes]
    reconciling = asyncio.Event()
    stopped: list[str] = []
    scheduled: list[asyncio.Task[None]] = []

    async def reconcile_blocks(_candidate) -> bool:
        reconciling.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            recorder.events.append("cancelled:attach_abort_successor")
            stopped.append("successor")
            raise
        return True  # pragma: no cover

    async def _inside(world):
        world.store.acquire = outcome_store.acquire
        monkeypatch.setattr(main, "_reconcile_attach_abort_successor", reconcile_blocks)
        # The real scheduler, with the real ``asyncio.create_task``: the
        # harness's recorder would replace the task with a stub. The harness
        # restores the real function when it exits either way.
        recording = asyncio.create_task
        asyncio.create_task = _REAL_CREATE_TASK
        try:
            scheduled.append(
                main._schedule_attach_abort_successor(
                    THREAD,
                    retired_runtime_generation=GENERATION,
                    retired_attach_token=ATTACH_TOKEN,
                    retired_agent_id=AGENT,
                )
            )
        finally:
            asyncio.create_task = recording
        await asyncio.wait_for(reconciling.wait(), timeout=2)

    try:
        await _run_lifespan(monkeypatch, recorder, inside=_inside)
        events = recorder.events
        assert "cancelled:attach_abort_successor" in events
        assert events.index("cancelled:attach_abort_successor") < events.index(
            "postgres_db.disconnect"
        )
        assert scheduled and scheduled[0].cancelled()
        # Only the read that found the outcome ran; the row the next process's
        # durable scan reconciles is exactly as it was.
        assert len(outcome_store.statements) == 1
        assert outcome_store.statements[0].lstrip().upper().startswith("SELECT")
        assert outcome_store.outcomes == durable_before
    finally:
        for task in scheduled:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        main._attach_abort_successor_tasks.pop(
            (THREAD, GENERATION, ATTACH_TOKEN, AGENT), None
        )


@_XFAIL
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failing",
    ["kb_datasource_tasks", "cloud_task_registry", "_project_repair_state"],
)
async def test_a_failing_drain_does_not_abort_the_rest_of_shutdown(
    monkeypatch, failing
):
    recorder = _Recorder()
    placed = _RegistryTasks(recorder.events)

    async def _explode() -> None:
        recorder.events.append(f"drain-failed:{failing}")
        raise RuntimeError(f"{failing} drain exploded")

    async def _inside(_world):
        await placed.place()
        # Applied inside the lifespan: the harness installs its own
        # ``kb_datasource_tasks.drain`` wrapper when it starts.
        monkeypatch.setattr(getattr(main, failing), "drain", _explode, raising=False)

    failure: BaseException | None = None
    try:
        try:
            await _run_lifespan(monkeypatch, recorder, inside=_inside)
        except RuntimeError as exc:
            failure = exc
        events = recorder.events
        owned_by_failing = {
            "cloud_task_registry": {"protected_engage", "cloud_stage"},
            "_project_repair_state": {"project_repair"},
        }.get(failing, set())
        for label in _RegistryTasks.LABELS:
            if label in owned_by_failing:
                continue
            assert f"cancelled:{label}" in events, f"{label} was never cancelled"
            assert events.index(f"cancelled:{label}") < events.index(_FIRST_CLOSE)
        # Every client and store still closed, in order...
        assert [e for e in events if e in _STORES_AND_CLIENTS] == _STORES_AND_CLIENTS
        # ...and only then does the drain's failure surface.
        assert isinstance(failure, RuntimeError)
        assert str(failure) == f"{failing} drain exploded"
    finally:
        await placed.cleanup()
