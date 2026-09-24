"""Job Bench spec, queue, ownership, and restart-safe sweeper tests."""

from __future__ import annotations

import asyncio
import copy
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException
from starlette.requests import Request

from orchestrator.routers import bench as bench_router
from orchestrator.services.bench import (
    BenchStore,
    build_bench_job_payload,
    build_submission_queue,
    freeze_spec,
    sweep_run,
    sweep_tick,
)
from orchestrator.application import jobs as jobs_composition
from orchestrator.security import access as access_module
from orchestrator.services import job_mutation_controls as job_mutation_controls_module
from orchestrator.services import session_tool_policy as session_tool_policy_module
import functools

RUN_ID = "11111111-1111-1111-1111-111111111111"
USER_ID = "22222222-2222-2222-2222-222222222222"


def _request(dependencies: bench_router.BenchDependencies | None = None) -> Request:
    if dependencies is None:
        dependencies = bench_router.BenchDependencies(
            store=BenchStore(MagicMock()), create_job=AsyncMock()
        )
    return Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/api/bench/runs",
            "headers": [],
            "app": SimpleNamespace(
                state=SimpleNamespace(bench_dependencies_factory=lambda: dependencies)
            ),
        }
    )


def _spec(*, max_in_flight: int = 2, replicates: int = 1) -> dict:
    return freeze_spec(
        {
            "tasks": [
                {
                    "id": "t1",
                    "family": "small",
                    "description": "first task",
                    "required_deliverables": ["./output/a.md"],
                    "config_name": "defaults",
                    "config_override": {
                        "llm": {"temperature": 0.2, "model": "task-model"},
                        "nested": {"task": True, "winner": "task"},
                    },
                },
                {
                    "id": "t2",
                    "description": "second task",
                    "required_deliverables": ["output/b.md"],
                    "config_name": "developer",
                },
                {
                    "id": "t3",
                    "description": "third task",
                    "required_deliverables": [],
                },
            ],
            "replicates": replicates,
            "max_in_flight": max_in_flight,
            "arms": [
                {
                    "name": "baseline",
                    "model": "pinned-model",
                    "config_override": {
                        "nested": {"arm": True, "winner": "arm"},
                    },
                }
            ],
        }
    )


def _run(*, state: list[dict] | None = None, **spec_overrides) -> dict:
    return {
        "id": RUN_ID,
        "name": "test",
        "status": "running",
        "created_by": USER_ID,
        "spec": _spec(**spec_overrides),
        "state": copy.deepcopy(state or []),
    }


class FakeStore:
    """In-memory implementation of the BenchStore methods a sweep touches."""

    def __init__(self, run: dict, tagged_jobs: list[dict] | None = None):
        self.run = copy.deepcopy(run)
        self.tagged_jobs = copy.deepcopy(tagged_jobs or [])
        self.saves: list[dict] = []
        self._sweep_lock = asyncio.Lock()

    async def list_tagged_jobs(self, _run_id: str) -> list[dict]:
        return copy.deepcopy(self.tagged_jobs)

    async def list_running_runs(self) -> list[dict]:
        return [copy.deepcopy(self.run)] if self.run["status"] == "running" else []

    @asynccontextmanager
    async def try_sweep_lock(self):
        """Emulate pg_try_advisory_lock: non-blocking claim, loser gets False."""
        if self._sweep_lock.locked():
            yield False
            return
        await self._sweep_lock.acquire()
        try:
            yield True
        finally:
            self._sweep_lock.release()

    async def save_run(
        self,
        _run_id: str,
        state: list[dict],
        *,
        status: str | None = None,
        require_status: str | None = None,
    ) -> dict | None:
        if require_status and self.run["status"] != require_status:
            return None
        self.run["state"] = copy.deepcopy(state)
        if status:
            self.run["status"] = status
        self.saves.append(copy.deepcopy(self.run))
        return copy.deepcopy(self.run)


def _tagged(entry: dict, status: str) -> dict:
    return {
        "id": entry["job_id"],
        "status": status,
        "created_at": datetime(2026, 8, 4, tzinfo=timezone.utc),
        "context": {
            "bench": {
                "run_id": RUN_ID,
                "task": entry["task"],
                "arm": entry["arm"],
                "replicate": entry["replicate"],
            }
        },
    }


@pytest.mark.parametrize("layer", ["tasks", "arms"])
def test_freeze_spec_refuses_transport_keys(layer):
    # Bench arms/tasks reach db.create_job through admit_job, never the
    # POST /api/jobs adapter — the transport fence runs at this write boundary.
    entry = (
        {"id": "t", "description": "d"}
        if layer == "tasks"
        else {
            "name": "a",
            "model": "m",
        }
    )
    entry["config_override"] = {"llm": {"model": "m", "base_url": "https://evil/v1"}}
    source = {"tasks": [], "arms": [], layer: [entry]}
    if layer == "arms":
        source["tasks"] = [{"id": "t", "description": "d"}]
    with pytest.raises(HTTPException) as exc:
        freeze_spec(source)
    assert exc.value.status_code == 422


def test_freeze_spec_is_complete_normalized_and_detached():
    source = {
        "tasks": [
            {
                "id": "task",
                "description": "Pinned description",
                "required_deliverables": ["./output/a.md", "output/a.md"],
                "config_name": "defaults",
                "config_override": {"extra": {"enabled": True}},
            }
        ],
        "replicates": 3,
        "max_in_flight": 2,
        "arms": [
            {
                "name": "candidate",
                "model": "model-pin",
                "config_override": {"guardrails": {"max": 4}},
            }
        ],
        "project_id": "33333333-3333-3333-3333-333333333333",
    }

    frozen = freeze_spec(source)
    source["tasks"][0]["description"] = "mutated later"
    source["tasks"][0]["config_override"]["extra"]["enabled"] = False
    source["arms"][0]["config_override"]["guardrails"]["max"] = 99

    assert frozen == {
        "version": 1,
        "tasks": [
            {
                "id": "task",
                "description": "Pinned description",
                "required_deliverables": ["output/a.md"],
                "config_name": "worker_base",
                "config_override": {"extra": {"enabled": True}},
                "priority": 5,
            }
        ],
        "replicates": 3,
        "max_in_flight": 2,
        "arms": [
            {
                "name": "candidate",
                "config_override": {"guardrails": {"max": 4}},
                "model": "model-pin",
            }
        ],
        "project_id": "33333333-3333-3333-3333-333333333333",
    }


def test_job_payload_is_owned_tagged_and_deep_merged_with_pins_last():
    run = _run()
    task = run["spec"]["tasks"][0]
    arm = run["spec"]["arms"][0]

    payload = build_bench_job_payload(run, task, arm, 2)

    assert payload["user_id"] == USER_ID
    assert payload["context"] == {
        "bench": {
            "run_id": RUN_ID,
            "task": "t1",
            "arm": "baseline",
            "replicate": 2,
        }
    }
    assert payload["required_deliverables"] == ["output/a.md"]
    assert payload["datasource_ids"] == []
    assert payload["config_name"] == "worker_base"
    assert payload["config_override"] == {
        "llm": {"temperature": 0.2, "model": "pinned-model"},
        "nested": {
            "task": True,
            "arm": True,
            "winner": "arm",
        },
        "autonomy": "full",
    }


def test_lane_is_frozen_per_arm_and_reaches_the_job_payload():
    """A lane A/B is only expressible if the arm can carry the lane.

    ``execution_lane`` is a top-level ``JobCreate`` field, not config, so it
    cannot ride inside ``config_override``; without this passthrough the
    stateless-vs-pinned comparison the S3 rollout gate asks for could not be
    written at all. Freezing it alongside the other arm fields is what keeps
    replicates of one arm from drifting across planes mid-run.
    """

    spec = freeze_spec(
        {
            "tasks": [{"id": "t1", "description": "task"}],
            "replicates": 1,
            "max_in_flight": 1,
            "arms": [
                {"name": "pinned-arm", "model": "m", "execution_lane": "pinned"},
                {
                    "name": "stateless-arm",
                    "model": "m",
                    "execution_lane": "stateless",
                },
            ],
        }
    )
    assert [arm.get("execution_lane") for arm in spec["arms"]] == [
        "pinned",
        "stateless",
    ]

    run = {"id": RUN_ID, "created_by": USER_ID, "spec": spec}
    task = spec["tasks"][0]
    lanes = [
        build_bench_job_payload(run, task, arm, 1)["execution_lane"]
        for arm in spec["arms"]
    ]
    assert lanes == ["pinned", "stateless"]


def test_omitted_lane_stays_none_so_existing_specs_are_unchanged():
    """Historical single-lane specs must keep their exact behaviour.

    ``None`` is not the same as ``"pinned"`` to ``JobCreate``: omitted means
    "root stays pinned, child inherits its authoritative parent", so asserting
    "pinned" here would silently re-parent child semantics for every bench run
    that predates lane arms.
    """

    run = _run()
    arm = run["spec"]["arms"][0]
    assert "execution_lane" not in arm
    assert (
        build_bench_job_payload(run, run["spec"]["tasks"][0], arm, 1)["execution_lane"]
        is None
    )


def test_arm_project_is_frozen_and_overrides_the_run_project():
    """Paired treatments need separate memory pools without separate runs."""

    run_project = "33333333-3333-3333-3333-333333333333"
    legacy_project = "44444444-4444-4444-4444-444444444444"
    skills_project = "55555555-5555-5555-5555-555555555555"
    spec = freeze_spec(
        {
            "tasks": [{"id": "t1", "description": "task"}],
            "replicates": 1,
            "max_in_flight": 1,
            "project_id": run_project,
            "arms": [
                {
                    "name": "legacy",
                    "model": "m",
                    "project_id": legacy_project,
                },
                {
                    "name": "skills",
                    "model": "m",
                    "project_id": skills_project,
                },
                {"name": "inherited", "model": "m"},
            ],
        }
    )
    assert [arm.get("project_id") for arm in spec["arms"]] == [
        legacy_project,
        skills_project,
        None,
    ]

    run = {"id": RUN_ID, "created_by": USER_ID, "spec": spec}
    task = spec["tasks"][0]
    assert [
        build_bench_job_payload(run, task, arm, 1)["project_id"] for arm in spec["arms"]
    ] == [legacy_project, skills_project, run_project]


@pytest.mark.asyncio
async def test_create_run_validates_and_persists_each_arm_project(monkeypatch):
    legacy_project = "44444444-4444-4444-4444-444444444444"
    skills_project = "55555555-5555-5555-5555-555555555555"
    caller = {"id": USER_ID, "is_admin": False, "scopes": []}
    require_member = AsyncMock()
    create = AsyncMock(return_value={"id": RUN_ID, "status": "running"})
    monkeypatch.setattr(
        bench_router, "require_approved_user", AsyncMock(return_value=caller)
    )
    monkeypatch.setattr(bench_router, "require_project_member", require_member)
    monkeypatch.setattr(bench_router, "create_bench_run", create)
    database = MagicMock()
    dependencies = bench_router.BenchDependencies(
        store=BenchStore(database),
        create_job=AsyncMock(),
        validate_tool_overrides=lambda value: value,
    )

    body = bench_router.BenchRunCreate(
        name="paired",
        tasks=[{"id": "t1", "description": "task"}],
        replicates=1,
        max_in_flight=1,
        arms=[
            {"name": "legacy", "model": "m", "project_id": legacy_project},
            {"name": "skills", "model": "m", "project_id": skills_project},
        ],
    )
    await bench_router.create_run(_request(dependencies), body)

    assert [call.args[2] for call in require_member.await_args_list] == [
        legacy_project,
        skills_project,
    ]
    frozen_input = create.await_args.kwargs["spec"]
    assert [arm["project_id"] for arm in frozen_input["arms"]] == [
        legacy_project,
        skills_project,
    ]


@pytest.mark.asyncio
async def test_create_run_rejects_arm_outside_token_project_scope(monkeypatch):
    allowed_project = "33333333-3333-3333-3333-333333333333"
    other_project = "44444444-4444-4444-4444-444444444444"
    caller = {
        "id": USER_ID,
        "is_admin": False,
        "scopes": [f"project:{allowed_project}"],
    }
    monkeypatch.setattr(
        bench_router, "require_approved_user", AsyncMock(return_value=caller)
    )
    require_member = AsyncMock()
    monkeypatch.setattr(bench_router, "require_project_member", require_member)
    database = MagicMock()
    dependencies = bench_router.BenchDependencies(
        store=BenchStore(database),
        create_job=AsyncMock(),
        validate_tool_overrides=lambda value: value,
    )

    body = bench_router.BenchRunCreate(
        name="paired",
        tasks=[{"id": "t1", "description": "task"}],
        replicates=1,
        max_in_flight=1,
        arms=[{"name": "legacy", "model": "m", "project_id": other_project}],
    )
    with pytest.raises(HTTPException, match="outside the token's project scope") as exc:
        await bench_router.create_run(_request(dependencies), body)

    assert exc.value.status_code == 403
    assert require_member.await_count == 1
    assert require_member.await_args.args[1:] == (
        database,
        allowed_project,
    )
    assert require_member.await_args.kwargs == {
        "min_role": "editor",
        "allow_archived": False,
    }


@pytest.mark.asyncio
async def test_run_reads_use_the_request_application_store(monkeypatch):
    caller = {"id": USER_ID, "is_admin": False, "scopes": []}
    database = MagicMock()
    store = MagicMock(db=database)
    store.list_runs = AsyncMock(return_value=[{"id": RUN_ID}])
    dependencies = bench_router.BenchDependencies(store=store, create_job=AsyncMock())
    authorize = AsyncMock(return_value=caller)
    monkeypatch.setattr(bench_router, "require_approved_user", authorize)

    result = await bench_router.list_runs(_request(dependencies), limit=7)

    assert result == [{"id": RUN_ID}]
    authorize.assert_awaited_once_with(authorize.await_args.args[0], database)
    store.list_runs.assert_awaited_once_with(created_by=USER_ID, limit=7)


@pytest.mark.asyncio
async def test_report_uses_bound_reporting_collaborators(monkeypatch):
    caller = {"id": USER_ID, "is_admin": False, "scopes": []}
    run = {"id": RUN_ID, "created_by": USER_ID}
    database = MagicMock()
    store = MagicMock(db=database)
    store.get_run = AsyncMock(return_value=run)
    audit_reader = MagicMock(is_available=True)
    forge = MagicMock()
    resolve_job_repo = AsyncMock(return_value=("repo", "branch"))
    dependencies = bench_router.BenchDependencies(
        store=store,
        create_job=AsyncMock(),
        audit_reader=audit_reader,
        forge=forge,
        resolve_job_repo=resolve_job_repo,
    )
    monkeypatch.setattr(
        bench_router, "require_approved_user", AsyncMock(return_value=caller)
    )
    report = AsyncMock(return_value={"run_id": RUN_ID})
    monkeypatch.setattr(bench_router, "compute_bench_report", report)

    result = await bench_router.get_report(_request(dependencies), RUN_ID)

    assert result == {"run_id": RUN_ID}
    report.assert_awaited_once_with(
        database,
        run,
        audit_reader=audit_reader,
        gitea_client=forge,
        resolve_job_repo=resolve_job_repo,
    )


@pytest.mark.asyncio
async def test_cancel_uses_authorized_application_operation_without_http_reentry(
    monkeypatch,
):
    caller = {"id": USER_ID, "is_admin": False, "scopes": []}
    run = {"id": RUN_ID, "created_by": USER_ID}
    store = MagicMock(db=MagicMock())
    store.get_run = AsyncMock(return_value=run)
    cancel_job = AsyncMock(return_value={"status": "cancelled"})
    dependencies = bench_router.BenchDependencies(
        store=store, create_job=AsyncMock(), cancel_job=cancel_job
    )
    monkeypatch.setattr(
        bench_router, "require_approved_user", AsyncMock(return_value=caller)
    )

    async def cancel_run(_store, visible_run, *, cancel_job_fn):
        assert _store is store
        assert visible_run is run
        assert await cancel_job_fn("job-1") == {"status": "cancelled"}
        return {"status": "cancelled"}

    monkeypatch.setattr(bench_router, "cancel_bench_run", cancel_run)

    result = await bench_router.cancel_run(_request(dependencies), RUN_ID)

    assert result == {"status": "cancelled"}
    cancel_job.assert_awaited_once_with("job-1", caller)


@pytest.mark.asyncio
async def test_application_cancel_adapter_revalidates_member_without_request(
    monkeypatch,
):
    import orchestrator.main as main

    caller = {"id": USER_ID, "is_admin": False, "scopes": []}
    job = {"id": "job-1", "user_id": USER_ID, "execution_lane": "pinned"}
    database = MagicMock()
    database.get_job = AsyncMock(return_value=job)
    authorize = AsyncMock(return_value=True)
    cancel = AsyncMock(return_value={"status": "cancelled"})
    monkeypatch.setattr(main.app.state.resources, "postgres_db", database)
    monkeypatch.setattr(access_module, "user_can_access_job", authorize)
    monkeypatch.setattr(
        job_mutation_controls_module.JobControlOperations,
        "cancel",
        cancel,
    )

    assert await jobs_composition.cancel_bench_job(
        main.app.state.resources, "job-1", caller
    ) == {"status": "cancelled"}

    authorize.assert_awaited_once_with(caller, database, "job-1")
    cancel.assert_awaited_once_with("job-1", job=job)


def _bound_to_application(operation, function, resources) -> bool:
    """``operation`` is ``function`` partially applied to exactly ``resources``."""

    return (
        isinstance(operation, functools.partial)
        and operation.func is function
        and len(operation.args) == 1
        and operation.args[0] is resources
        and not operation.keywords
    )


def test_application_factory_binds_complete_bench_dependencies():
    import orchestrator.main as main

    dependencies = jobs_composition.bench_dependencies(main.app.state.resources)

    assert dependencies.store.db is main.app.state.resources.postgres_db
    assert _bound_to_application(
        dependencies.create_job,
        jobs_composition.create_bench_job,
        main.app.state.resources,
    )
    assert (
        dependencies.validate_tool_overrides
        is session_tool_policy_module.with_validated_tool_overrides
    )
    assert dependencies.audit_reader is main.app.state.resources.audit_reader
    assert dependencies.forge is main.app.state.resources.gitea_client
    assert dependencies.resolve_job_repo is not None
    assert _bound_to_application(
        dependencies.cancel_job,
        jobs_composition.cancel_bench_job,
        main.app.state.resources,
    )


def test_seeded_queue_is_stable_and_skips_ledger_entries():
    run = _run(replicates=2)
    first = build_submission_queue(run)
    assert build_submission_queue(run) == first
    assert len(first) == 6

    task, arm, replicate = first[0]
    run["state"] = [
        {
            "task": task["id"],
            "arm": arm["name"],
            "replicate": replicate,
            "job_id": "already-created",
            "final_status": "completed",
        }
    ]
    remaining = build_submission_queue(run)
    assert len(remaining) == 5
    assert (task, arm, replicate) not in remaining


@pytest.mark.asyncio
async def test_sweeper_tops_up_only_to_max_in_flight():
    store = FakeStore(_run(max_in_flight=2))
    create_job = AsyncMock(
        side_effect=[
            {"id": "job-1", "status": "created"},
            {"id": "job-2", "status": "created"},
        ]
    )

    created = await sweep_run(store, store.run, create_job)

    assert created == 2
    assert create_job.await_count == 2
    assert len(store.run["state"]) == 2
    assert all(entry["final_status"] is None for entry in store.run["state"])
    assert store.run["status"] == "running"


@pytest.mark.asyncio
async def test_sweeper_refreshes_terminal_status_before_top_up():
    state = [
        {
            "task": "t1",
            "arm": "baseline",
            "replicate": 1,
            "job_id": "job-done",
            "final_status": None,
        },
        {
            "task": "t2",
            "arm": "baseline",
            "replicate": 1,
            "job_id": "job-live",
            "final_status": None,
        },
    ]
    run = _run(state=state, max_in_flight=2)
    store = FakeStore(
        run,
        [_tagged(state[0], "failed"), _tagged(state[1], "processing")],
    )
    create_job = AsyncMock(return_value={"id": "job-new", "status": "created"})

    assert await sweep_run(store, store.run, create_job) == 1
    assert store.run["state"][0]["final_status"] == "failed"
    assert store.run["state"][1]["final_status"] is None
    assert store.run["state"][2]["job_id"] == "job-new"


@pytest.mark.asyncio
async def test_restart_resumes_from_persisted_ledger_without_duplicate():
    first_store = FakeStore(_run(max_in_flight=1))
    first_create = AsyncMock(
        return_value={"id": "job-before-restart", "status": "created"}
    )
    assert await sweep_run(first_store, first_store.run, first_create) == 1
    first_entry = first_store.run["state"][0]

    # Simulated process restart: a fresh store instance sees only the durable
    # run row and authoritative tagged job status.
    restarted = FakeStore(
        first_store.run,
        [_tagged(first_entry, "completed")],
    )
    second_create = AsyncMock(
        return_value={"id": "job-after-restart", "status": "created"}
    )

    assert await sweep_run(restarted, restarted.run, second_create) == 1
    assert [entry["job_id"] for entry in restarted.run["state"]] == [
        "job-before-restart",
        "job-after-restart",
    ]
    assert restarted.run["state"][0]["final_status"] == "completed"


@pytest.mark.asyncio
async def test_restart_reconstructs_job_created_before_ledger_commit():
    run = _run(max_in_flight=1)
    crash_window_job = {
        "id": "job-in-crash-window",
        "status": "processing",
        "created_at": datetime(2026, 8, 4, tzinfo=timezone.utc),
        "context": {
            "bench": {
                "run_id": RUN_ID,
                "task": "t2",
                "arm": "baseline",
                "replicate": 1,
            }
        },
    }
    store = FakeStore(run, [crash_window_job])
    create_job = AsyncMock()

    assert await sweep_run(store, store.run, create_job) == 0
    create_job.assert_not_awaited()
    assert store.run["state"][0]["job_id"] == "job-in-crash-window"


@pytest.mark.asyncio
async def test_concurrent_ticks_submit_once_across_replicas():
    """Two replicas' sweepers fired 2-5 ms apart and double-submitted the same
    (task, arm, replicate). The advisory claim makes the loser a no-op.
    knowledge-base/knowledge/issues/bench_sweeper_multi_replica_race.md
    """
    store = FakeStore(_run(max_in_flight=2))
    first_submit = asyncio.Event()
    release = asyncio.Event()
    created_ids = iter(["job-1", "job-2"])

    async def create_job(_run, _task, _arm, _replicate):
        first_submit.set()
        await release.wait()  # hold replica A inside the locked tick
        return {"id": next(created_ids), "status": "created"}

    replica_a = asyncio.create_task(sweep_tick(store, create_job))
    await first_submit.wait()  # A now holds the claim, mid-submission

    assert await sweep_tick(store, create_job) == 0, "replica B swept under A's lock"

    release.set()
    assert await replica_a == 2
    assert [entry["job_id"] for entry in store.run["state"]] == ["job-1", "job-2"]
    pairs = [(e["task"], e["arm"], e["replicate"]) for e in store.run["state"]]
    assert len(pairs) == len(set(pairs)), "duplicate (task, arm, replicate) pair"


@pytest.mark.asyncio
async def test_sweep_lock_is_released_between_ticks():
    store = FakeStore(_run(max_in_flight=1))
    create_job = AsyncMock(side_effect=[{"id": "job-1", "status": "created"}])
    assert await sweep_tick(store, create_job) == 1

    store.tagged_jobs = [_tagged(store.run["state"][0], "completed")]
    create_job.side_effect = [{"id": "job-2", "status": "created"}]
    assert await sweep_tick(store, create_job) == 1  # lock was not leaked


@pytest.mark.asyncio
async def test_sweep_lock_released_when_a_run_sweep_raises():
    store = FakeStore(_run(max_in_flight=1))
    boom = AsyncMock(side_effect=RuntimeError("job service down"))
    assert await sweep_tick(store, boom) == 0  # per-run isolation, not a raise

    create_job = AsyncMock(return_value={"id": "job-1", "status": "created"})
    assert await sweep_tick(store, create_job) == 1  # next tick still claims


@pytest.mark.asyncio
async def test_tick_without_lock_support_still_sweeps():
    """Narrow stores without try_sweep_lock (older fakes) must keep working."""

    class NarrowStore(FakeStore):
        try_sweep_lock = None  # a narrow fake that predates the lock method

    store = NarrowStore(_run(max_in_flight=1))
    create_job = AsyncMock(return_value={"id": "job-1", "status": "created"})
    assert await sweep_tick(store, create_job) == 1


@pytest.mark.asyncio
async def test_bench_store_claim_is_session_scoped_and_released():
    """Claim and release must ride ONE held connection — PostgresDB.fetchval
    grabs a fresh pooled connection per call, which would unlock a different
    session and leak the lock for the winner's connection lifetime."""
    conn = AsyncMock()
    conn.fetchval.side_effect = [True, True]
    acquires = 0

    @asynccontextmanager
    async def acquire():
        nonlocal acquires
        acquires += 1
        yield conn

    db = MagicMock()
    db.acquire = acquire

    async with BenchStore(db).try_sweep_lock() as claimed:
        assert claimed is True

    assert acquires == 1, "claim and release must share one session"
    claim_call, release_call = conn.fetchval.await_args_list
    assert "pg_try_advisory_lock" in claim_call.args[0]
    assert "pg_advisory_xact_lock" not in claim_call.args[0]  # tick has no txn
    assert "pg_advisory_unlock" in release_call.args[0]
    assert claim_call.args[1] == release_call.args[1] == "bench_sweep"


@pytest.mark.asyncio
async def test_bench_store_loser_issues_no_unlock():
    conn = AsyncMock()
    conn.fetchval.return_value = False

    @asynccontextmanager
    async def acquire():
        yield conn

    db = MagicMock()
    db.acquire = acquire

    async with BenchStore(db).try_sweep_lock() as claimed:
        assert claimed is False
    assert conn.fetchval.await_count == 1  # never unlock a lock we don't hold
