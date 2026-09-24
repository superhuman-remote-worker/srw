"""Execution configuration is transactional, immutable and adapter-specific."""

from tests import _b09_control_seams as control_seams

import asyncio
from contextlib import asynccontextmanager
from copy import deepcopy
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

from fastapi import HTTPException
import pytest

from orchestrator.database.postgres import PostgresDB
from orchestrator.services import manifest_execution_snapshot as snapshots
from orchestrator.services.manifest_store import ManifestStore
from shared.manifests import validate_documents
from orchestrator.application import controls as controls_composition
from orchestrator.services import config_resolver as config_resolver_module
from orchestrator.services import deployment_gates as deployment_gates_module
from orchestrator.services import grant_enforcement as grant_enforcement_module
from orchestrator.services import (
    job_datasource_selection as job_datasource_selection_module,
)
from orchestrator.services import (
    job_dispatch_credentials as job_dispatch_credentials_module,
)
from orchestrator.services import job_start_bundle as job_start_bundle_module
from orchestrator.services import (
    job_workspace_authority as job_workspace_authority_module,
)
from orchestrator.services import job_workspace_runtime as job_workspace_runtime_module
from orchestrator.services import (
    managed_repository_authority as managed_repository_authority_module,
)
from orchestrator.services import runtime_actor as runtime_actor_module
from orchestrator.services import workspace_tier_policy as workspace_tier_policy_module
import httpx


USER = "11111111-1111-4111-8111-111111111111"
WORK = "22222222-2222-4222-8222-222222222222"


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["Job", "Session"])
@pytest.mark.parametrize(
    "change",
    [
        {"image": "example.invalid/other:1"},
        {"command": ["custom-launch"]},
        {"args": []},
        {"env": {}},
        {"resources": {}},
        {"probes": {}},
        {"config": None},
        {"config": {"unknown_private_option": True}},
    ],
)
async def test_existing_callers_reject_unhonored_canonical_srw_launch(kind, change):
    """A compatibility request cannot silently replace the selected image/config."""
    runtime = {"image": "installed:1", "adapter": "srw/v1", **change}
    # No defaults, grants or resolver collaborators are available: refusal must
    # happen before any private resolution or insertion starts.
    db = SimpleNamespace(manifest_runtime_image="installed:1")
    with pytest.raises(HTTPException) as denied:
        await snapshots.prepare_srw_snapshot(
            db,
            work_kind=kind,
            work_id=WORK,
            owner_id=USER,
            project_ids=[],
            config_name="worker_base" if kind == "Job" else "session_base",
            expert_id=None,
            expert_row={
                "harness_adapter": "srw/v1",
                "manifest": {"spec": {"runtime": runtime}},
            },
            config_override={},
            description="Launch parity check",
            datasource_ids=[],
            policy_revisions={},
        )
    assert denied.value.status_code == 422


class Connection:
    def __init__(self):
        self.execution = None
        self.revisions = []
        self.in_transaction = False
        self.jobs = []
        self.catalog_locks = 0

    @asynccontextmanager
    async def transaction(self):
        before = deepcopy((self.execution, self.revisions, self.jobs))
        self.in_transaction = True
        try:
            yield
        except BaseException:
            self.execution, self.revisions, self.jobs = before
            raise
        finally:
            self.in_transaction = False

    async def fetchrow(self, query, *args):
        if "INSERT INTO srw_execution_specs(" in query:
            assert self.in_transaction
            if self.execution:
                return None
            self.execution = dict(
                id=uuid4(),
                resource_id=args[0],
                resource_version=args[1],
                work_kind=args[2],
                work_id=args[3],
                owner_id=args[4],
                project_ids=args[5],
                document=json.loads(args[6]),
                resolved=json.loads(args[7]),
                revision=args[8],
                dependencies=json.loads(args[9]),
                harness_adapter=args[10],
                generation=1,
            )
            return deepcopy(self.execution)
        if "UPDATE srw_execution_specs SET" in query:
            assert self.in_transaction
            self.execution.update(
                document=json.loads(args[1]),
                resolved=json.loads(args[2]),
                dependencies=json.loads(args[3]),
                revision=args[4],
                project_ids=args[5],
                generation=self.execution["generation"] + 1,
            )
            return deepcopy(self.execution)
        if "FROM srw_execution_specs" in query:
            return deepcopy(self.execution)
        if "INSERT INTO jobs" in query:
            assert self.in_transaction
            row = {"id": args[-1], "status": args[5]}
            self.jobs.append(row)
            return row
        if "INSERT INTO threads" in query:
            assert self.in_transaction
            row = {"id": uuid4(), "metadata": json.loads(args[6])}
            self.jobs.append(row)
            return row
        raise AssertionError(query)

    async def execute(self, query, *args):
        if "pg_advisory_xact_lock" in query and "srw-resource-catalog" in query:
            assert self.in_transaction
            self.catalog_locks += 1
            return "SELECT 1"
        if "INSERT INTO srw_execution_spec_revisions" in query:
            assert self.in_transaction
            self.revisions.append(deepcopy(args))
            return "INSERT 0 1"
        raise AssertionError(query)


def database(connection):
    db = PostgresDB.__new__(PostgresDB)

    class Pool:
        @asynccontextmanager
        async def acquire(self):
            yield connection

    db._pool = Pool()
    return db


def native_document(private):
    return {
        "apiVersion": "srw/v1alpha1",
        "kind": "Job",
        "metadata": {
            "name": "ordinary-image",
            "scope": {"kind": "Account", "name": USER},
        },
        "spec": {
            "execution": {
                "expert": {
                    "inline": {
                        "runtime": {
                            "image": "example.invalid/plain@sha256:abc",
                            "config": private,
                        }
                    }
                },
                "workspace": None,
                "connectors": {},
            }
        },
    }


def prepared(private, *, adapter="generic"):
    document = native_document(private)
    return dict(
        document=document,
        resolved=deepcopy(document),
        dependencies=[],
        revision="sha256:one",
        harness_adapter=adapter,
    )


@pytest.mark.asyncio
async def test_native_capture_uses_active_connection_and_never_srw_parser(monkeypatch):
    connection = Connection()
    db = database(connection)
    render = AsyncMock(side_effect=AssertionError("generic config entered SRW"))
    monkeypatch.setattr(snapshots, "prepare_srw_snapshot", render)
    config = {
        "unrecognized_tool": None,
        "password": "literal-opaque-test-setting",
        "nested": [None, {}],
    }
    async with connection.transaction():
        result = await snapshots.capture_execution(
            db,
            connection,
            work_kind="Job",
            work_id=WORK,
            owner_id=USER,
            project_ids=[],
            execution_manifest=prepared(config),
        )
    render.assert_not_awaited()
    assert connection.catalog_locks == 1
    assert (
        result["resolved"]["spec"]["execution"]["expert"]["inline"]["runtime"]["config"]
        == config
    )
    assert len(connection.revisions) == 1


@pytest.mark.asyncio
async def test_job_snapshot_conflict_rolls_back_job_insert(monkeypatch):
    connection = Connection()
    db = database(connection)
    capture = AsyncMock(side_effect=HTTPException(409, "snapshot conflict"))
    monkeypatch.setattr(snapshots, "capture_execution", capture)
    with pytest.raises(HTTPException):
        await db.create_job(
            description="Task", job_id=WORK, execution_manifest=prepared({})
        )
    assert connection.jobs == []
    assert connection.catalog_locks == 1
    args, kwargs = capture.await_args
    assert args == (db, connection)
    assert kwargs["work_id"] == WORK
    assert kwargs["execution_manifest"]["harness_adapter"] == "generic"


@pytest.mark.asyncio
async def test_thread_snapshot_captures_complete_initial_metadata_in_insert_transaction(
    monkeypatch,
):
    connection = Connection()
    db = database(connection)

    async def capture(*args, **kwargs):
        assert args == (db, connection)
        assert connection.in_transaction
        assert kwargs["expert_id"] == WORK
        assert kwargs["config_override"]["llm"] == {"model": "chosen"}
        assert (
            kwargs["config_override"]["interactive"]["permission_mode"] == "supervised"
        )
        return {}

    monkeypatch.setattr(snapshots, "capture_execution", capture)
    thread_id = await db.create_thread(
        initial_metadata={
            "expert_id": WORK,
            "config_override": {"llm": {"model": "chosen"}},
        }
    )
    assert str(connection.jobs[0]["id"]) == thread_id
    assert connection.jobs[0]["metadata"]["expert_id"] == WORK
    assert connection.catalog_locks == 1


@pytest.mark.asyncio
async def test_existing_job_spec_cannot_be_replaced_or_reowned():
    connection = Connection()
    store = ManifestStore(database(connection))
    kwargs = dict(
        work_kind="Job",
        work_id=WORK,
        owner_id=USER,
        project_ids=[],
        conn=connection,
        **prepared({"x": None}),
    )
    async with connection.transaction():
        first = await store.freeze_execution(**kwargs)
        assert await store.freeze_execution(**kwargs) == first
        with pytest.raises(HTTPException):
            await store.freeze_execution(**{**kwargs, "owner_id": None})
        with pytest.raises(HTTPException):
            await store.freeze_execution(**{**kwargs, "resolved": native_document({})})
        with pytest.raises(HTTPException):
            await store.freeze_execution(
                **{**kwargs, "dependencies": [{"uid": WORK, "revision": "later"}]}
            )
    assert len(connection.revisions) == 1


@pytest.mark.asyncio
async def test_session_updates_preserve_previous_generation_and_reject_stale_writer():
    connection = Connection()
    store = ManifestStore(database(connection))
    async with connection.transaction():
        initial = await store.freeze_execution(
            work_kind="Session",
            work_id=WORK,
            owner_id=USER,
            project_ids=[],
            conn=connection,
            **prepared({"model": "first"}, adapter="srw/v1"),
        )
        updated = await store.update_session_execution(
            work_id=WORK,
            owner_id=USER,
            project_ids=[],
            conn=connection,
            expected_generation=1,
            **prepared({"model": "second"}, adapter="srw/v1"),
        )
        assert updated["generation"] == 2
        assert initial["generation"] == 1
        assert json.loads(connection.revisions[0][3]) == initial["resolved"]
        with pytest.raises(HTTPException):
            await store.update_session_execution(
                work_id=WORK,
                owner_id=USER,
                project_ids=[],
                conn=connection,
                expected_generation=1,
                **prepared({"model": "third"}, adapter="srw/v1"),
            )
    assert len(connection.revisions) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("expert_config_name", ["worker_base", "developer"])
async def test_rendered_reference_snapshot_validates_and_preserves_effective_settings(
    expert_config_name,
):
    db = SimpleNamespace(
        get_user_settings=AsyncMock(return_value={"default_model": "gpt-4o"}),
        resolve_default_for_capability=AsyncMock(return_value=None),
        get_system_setting=AsyncMock(return_value={"value": {"enabled": True}}),
        get_user=AsyncMock(return_value={"id": USER, "is_admin": True}),
    )
    result = await snapshots.prepare_srw_snapshot(
        db,
        work_kind="Job",
        work_id=WORK,
        owner_id=USER,
        project_ids=[],
        config_name="worker_base",
        expert_id=None,
        description="Run a check",
        config_override={"workspace": {"backend": "virtual"}},
        datasource_ids=[],
        policy_revisions={},
        expert_row={
            "name": "native-inline",
            "harness_config_name": expert_config_name,
            "expert_type": "worker",
            "config": {},
            "prompts": {"persona": "exact native persona"},
        },
    )
    validate_documents([result["document"]])
    blob, policy = snapshots.srw_snapshot_config(result)
    assert (
        result["resolved"]["spec"]["execution"]["expert"]["inline"]["runtime"][
            "config"
        ]["config_name"]
        == expert_config_name
    )
    assert blob["agent"]["llm"]["model"] == "gpt-4o"
    assert policy["workspace"]["backend"] == "virtual"
    assert "remote" not in blob["agent"]["workspace"]
    assert blob["prompts"]["persona"] == "exact native persona"


@pytest.mark.asyncio
async def test_generic_expert_cannot_fall_through_empty_compatibility_config():
    db = SimpleNamespace(
        get_expert_by_id=AsyncMock(return_value={"harness_adapter": None, "config": {}})
    )
    with pytest.raises(HTTPException, match="native manifest Job admission"):
        await snapshots.prepare_srw_snapshot(
            db,
            work_kind="Job",
            work_id=WORK,
            owner_id=USER,
            project_ids=[],
            config_name="worker_base",
            expert_id=WORK,
            description="Task",
            config_override=None,
            datasource_ids=[],
            policy_revisions={},
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("private_field", ["config", "harness_config_layers"])
async def test_native_private_roster_references_use_runner_visibility(private_field):
    db = SimpleNamespace(
        get_user_settings=AsyncMock(return_value={"default_model": "gpt-4o"}),
        resolve_default_for_capability=AsyncMock(return_value=None),
        get_system_setting=AsyncMock(return_value={"value": {"enabled": False}}),
        get_user=AsyncMock(return_value={"id": USER, "is_admin": False}),
        get_expert_visible_by_id=AsyncMock(return_value=None),
        get_expert_by_id=AsyncMock(
            side_effect=AssertionError("native reference bypassed runner visibility")
        ),
    )
    private = {"subagents": {"roster": {"hidden": {"$ref": WORK}}}}
    expert = {"expert_type": "worker", "config": {}, "prompts": {}}
    expert[private_field] = (
        [private] if private_field == "harness_config_layers" else private
    )
    result = await snapshots.prepare_srw_snapshot(
        db,
        work_kind="Job",
        work_id=WORK,
        owner_id=USER,
        project_ids=[],
        config_name="worker_base",
        expert_id=None,
        expert_row=expert,
        description="Task",
        config_override={"workspace": {"backend": "none"}},
        datasource_ids=[],
        policy_revisions={},
    )
    blob, _ = snapshots.srw_snapshot_config(result)
    db.get_expert_visible_by_id.assert_awaited_once_with(
        WORK, user_id=USER, project_ids=[], is_admin=False
    )
    db.get_expert_by_id.assert_not_awaited()
    assert "hidden" not in blob["agent"]["subagents"]["roster"]


@pytest.mark.asyncio
@pytest.mark.parametrize("private_field", ["config", "harness_config_layers"])
async def test_native_child_private_transport_is_rejected_before_resolution(
    monkeypatch,
    private_field,
):
    from orchestrator.services import config_resolver, session_config_resolution

    child = {"harness_adapter": "srw/v1", "config": {}, "prompts": {}}
    authority = {"workspace": {"remote": {"host": "unauthorized"}}}
    child[private_field] = (
        [authority] if private_field == "harness_config_layers" else authority
    )
    monkeypatch.setattr(
        session_config_resolution,
        "prefetch_roster_refs",
        AsyncMock(return_value={WORK: child}),
    )
    monkeypatch.setattr(
        config_resolver,
        "resolve_config",
        lambda **_: pytest.fail("Unvalidated child reached the resolver"),
    )
    db = SimpleNamespace(
        get_user_settings=AsyncMock(return_value={}),
        resolve_default_for_capability=AsyncMock(return_value=None),
    )
    with pytest.raises(HTTPException, match="runtime authority"):
        await snapshots.prepare_srw_snapshot(
            db,
            work_kind="Job",
            work_id=WORK,
            owner_id=USER,
            project_ids=[],
            config_name="worker_base",
            expert_id=None,
            expert_row={"config": {}, "prompts": {}, "expert_type": "worker"},
            description="Task",
            config_override=None,
            datasource_ids=[],
            policy_revisions={},
        )


@pytest.mark.parametrize(
    "fragment",
    [
        {"workspace": {"remote": {"host": "caller-supplied"}}},
        {"workspace": {"mounts": [{"source": "caller-supplied"}]}},
        {"connections": {"postgres": {"host": "caller-supplied"}}},
        {"env_keys": {"INTERNAL_KEY": "caller-supplied"}},
        {"runtime_actor": {"user_id": USER}},
        {"tools": {"canvas": ["run_command"]}},
        {"subagents": {"roster": {"child": {"tools": {"canvas": ["run_command"]}}}}},
    ],
)
def test_srw_private_authority_and_tool_smuggling_are_rejected(fragment):
    before = deepcopy(fragment)
    with pytest.raises(HTTPException):
        snapshots.validate_srw_authored_fragment(fragment)
    assert fragment == before


@pytest.mark.asyncio
async def test_saved_native_expert_uses_runner_visibility_too():
    db = SimpleNamespace(
        get_user_settings=AsyncMock(return_value={"default_model": "gpt-4o"}),
        resolve_default_for_capability=AsyncMock(return_value=None),
        get_system_setting=AsyncMock(return_value={"value": {"enabled": False}}),
        get_user=AsyncMock(return_value={"id": USER, "is_admin": False}),
        get_expert_visible_by_id=AsyncMock(return_value=None),
        get_expert_by_id=AsyncMock(
            return_value={
                "harness_adapter": "srw/v1",
                "config": {"subagents": {"roster": {"hidden": {"$ref": USER}}}},
                "prompts": {},
                "expert_type": "worker",
            }
        ),
    )
    await snapshots.prepare_srw_snapshot(
        db,
        work_kind="Job",
        work_id=WORK,
        owner_id=USER,
        project_ids=[],
        config_name="worker_base",
        expert_id=WORK,
        description="Task",
        config_override={"workspace": {"backend": "none"}},
        datasource_ids=[],
        policy_revisions={},
    )
    db.get_expert_by_id.assert_awaited_once_with(WORK)
    db.get_expert_visible_by_id.assert_awaited_once_with(
        USER, user_id=USER, project_ids=[], is_admin=False
    )


@pytest.mark.asyncio
async def test_project_snapshot_uses_active_composition_without_live_expert_or_link(
    monkeypatch,
):
    from orchestrator.services import manifest_projects

    dependency = {
        "uid": USER,
        "revision": "active-project-revision",
        "resourceVersion": 3,
    }
    lookup = AsyncMock(
        return_value={
            "project_composed": True,
            "project_dependency": dependency,
            "harness_adapter": "srw/v1",
            "expert_type": "session",
            "config": {"llm": {"model": "gpt-4o"}},
            "harness_config_layers": [{"llm": {"temperature": 0.23}}],
            "prompts": {"persona": "active generation persona"},
            "manifest_uid": WORK,
            "manifest_revision": "frozen-source-revision",
        }
    )
    monkeypatch.setattr(manifest_projects, "project_expert_for_execution", lookup)
    db = SimpleNamespace(
        get_user_settings=AsyncMock(return_value={"default_model": "gpt-4o"}),
        resolve_default_for_capability=AsyncMock(return_value=None),
        get_system_setting=AsyncMock(return_value={"value": {"enabled": True}}),
        get_user=AsyncMock(return_value={"id": USER, "is_admin": True}),
        get_expert_by_id=AsyncMock(
            side_effect=AssertionError("live Expert used instead of composition")
        ),
        get_project_expert_link=AsyncMock(
            side_effect=AssertionError("live Project link mixed with composition")
        ),
    )
    result = await snapshots.prepare_srw_snapshot(
        db,
        work_kind="Session",
        work_id=WORK,
        owner_id=USER,
        project_ids=[USER],
        config_name="session_base",
        expert_id=WORK,
        description="Session",
        config_override={"workspace": {"backend": "none"}},
        datasource_ids=[],
        policy_revisions={},
    )
    lookup.assert_awaited_once_with(
        db, USER, expert_id=WORK, role="session", config_name="session_base"
    )
    blob, _ = snapshots.srw_snapshot_config(result)
    assert blob["prompts"]["persona"] == "active generation persona"
    assert blob["agent"]["llm"]["temperature"] == 0.23
    assert dependency in result["dependencies"]
    assert {"uid": WORK, "revision": "frozen-source-revision"} in result["dependencies"]


def test_runtime_bindings_do_not_replace_frozen_model_or_mutate_snapshot():
    blob = {
        "agent": {
            "llm": {"model": "admitted"},
            "workspace": {"backend": "sandbox"},
            "tools": {"workspace": ["read_file"]},
        },
        "prompts": {"persona": "admitted prompt"},
    }
    policy = deepcopy(blob["agent"])
    original = deepcopy(blob)
    delivered, checked = snapshots.apply_srw_delivery_bindings(
        blob,
        policy,
        {
            "llm": {"model": "changed-after-admission"},
            "workspace": {"remote": {"host": "authorized-workspace"}},
        },
    )
    assert delivered["agent"]["llm"]["model"] == "admitted"
    assert checked["workspace"]["remote"]["host"] == "authorized-workspace"
    assert blob == original


@pytest.mark.parametrize("workspace", ["vm", "denied-sandbox"])
def test_workspace_sudo_policy_survives_frozen_srw_delivery(workspace):
    from orchestrator.services.job_workspace_runtime import (
        apply_sticky_sudo_denial,
        inject_vm_workspace_config,
    )

    blob = {
        "agent": {
            "llm": {"model": "admitted"},
            "workspace": {"backend": "sandbox"},
            "shell": {"sudo_action": "freeze", "max_tabs": 4},
        }
    }
    policy = deepcopy(blob["agent"])
    original = deepcopy((blob, policy))
    override = {
        "llm": {"model": "changed-after-admission"},
        "shell": {"max_tabs": 99},
    }
    if workspace == "vm":
        override = inject_vm_workspace_config(
            override, {"status": "ready", "ssh_host": "authorized-vm"}
        )
        expected_action = "allow"
    else:
        override["workspace"] = {"backend": "sandbox"}
        override = apply_sticky_sudo_denial(
            {
                "context": {
                    "sudo_denial": {
                        "denied": True,
                        "decided_by": "operator",
                        "reason": "Use a local virtual environment",
                    }
                }
            },
            override,
        )
        expected_action = "block"

    delivered, checked = snapshots.apply_srw_delivery_bindings(blob, policy, override)
    for settings in (delivered["agent"], checked):
        assert settings["shell"]["sudo_action"] == expected_action
        assert settings["shell"]["max_tabs"] == 4
        assert settings["llm"]["model"] == "admitted"
        if workspace == "denied-sandbox":
            assert (
                settings["shell"]["sudo_block_message"]
                == override["shell"]["sudo_block_message"]
            )
    assert (blob, policy) == original


@pytest.mark.asyncio
async def test_connection_binding_is_task_local():
    outer, other = Connection(), Connection()
    db = database(other)
    async with db.using_connection(outer):
        async with db.acquire() as current:
            assert current is outer

        async def child():
            async with db.acquire() as current:
                assert current is other

        await asyncio.create_task(child())
    async with db.acquire() as current:
        assert current is other


@pytest.mark.asyncio
async def test_datasource_lock_reentry_is_limited_to_owning_task():
    db = database(Connection())
    acquisitions = []

    @asynccontextmanager
    async def acquire_lock(key, **kwargs):
        acquisitions.append((key, asyncio.current_task()))
        yield True

    db._dedicated_session_advisory_lock = acquire_lock
    async with db.thread_datasource_lock(WORK):
        async with db.thread_datasource_lock(WORK):
            assert len(acquisitions) == 1

        async def child():
            async with db.thread_datasource_lock(WORK):
                assert len(acquisitions) == 2

        await asyncio.create_task(child())
    assert acquisitions[0][1] is not acquisitions[1][1]


@pytest.mark.asyncio
async def test_configuration_transaction_locks_delivery_before_connection_and_row():
    connection = Connection()
    db = database(connection)
    events = []

    @asynccontextmanager
    async def delivery_lock(_thread_id):
        assert not connection.in_transaction
        events.append("delivery")
        yield

    async def execute(query, *args):
        assert connection.in_transaction
        assert "pg_advisory_xact_lock" in query
        events.append("config")

    db.thread_datasource_lock = delivery_lock
    connection.execute = execute
    async with db.thread_configuration_transaction(WORK) as current:
        assert current is connection
        events.append("row")
    assert events == ["delivery", "config", "row"]


@pytest.mark.asyncio
async def test_session_update_rolls_back_settings_when_revision_capture_fails(
    monkeypatch,
):
    import orchestrator.main as main
    from orchestrator.services import thread_config_update

    thread = {"id": WORK, "metadata": {}}

    class SessionConnection(Connection):
        async def fetchrow(self, query, *args):
            if "SELECT * FROM threads" in query:
                return thread
            return await super().fetchrow(query, *args)

        async def execute(self, query, *args):
            if "pg_advisory_xact_lock" in query:
                return "SELECT 1"
            return await super().execute(query, *args)

    connection = SessionConnection()
    db = database(connection)

    @asynccontextmanager
    async def delivery_lock(_thread_id):
        yield

    async def change_settings(*_args, **_kwargs):
        assert connection.in_transaction
        connection.jobs.append({"settings": "accepted", "datasources": [WORK]})
        return {}, [WORK]

    db.thread_datasource_lock = delivery_lock
    db.refresh_session_execution = AsyncMock(
        side_effect=HTTPException(409, "capture failed")
    )
    monkeypatch.setattr(main.app.state.resources, "postgres_db", db)
    monkeypatch.setattr(
        thread_config_update, "apply_thread_config_update_locked", change_settings
    )
    with pytest.raises(HTTPException, match="capture failed"):
        await control_seams.apply_thread_config_update(
            WORK, thread, {}, [WORK], request=None, actor=None
        )
    assert connection.jobs == []
    db.refresh_session_execution.assert_awaited_once_with(
        WORK, conn=connection, config_override={}
    )


def test_historical_import_uses_paused_blob_without_current_expert_resolution(
    monkeypatch,
):
    from orchestrator.operator_cli.manifest_execution_migration import (
        historical_snapshot,
    )
    from orchestrator.services import config_resolver

    monkeypatch.setattr(
        config_resolver,
        "resolve_config",
        lambda **_: pytest.fail("historical import re-resolved current files"),
    )
    blob = {
        "agent": {
            "llm": {"model": "historical-model", "temperature": 0.17},
            "workspace": {"backend": "sandbox", "remote": {"host": "retired-host"}},
        },
        "prompts": {"persona": "historical prompt"},
        "instructions": "historical instructions",
    }
    row = {
        "id": WORK,
        "user_id": USER,
        "status": "paused",
        "resolved_config": blob,
        "config_name": "removed-profile",
        "context": {},
    }
    result = historical_snapshot(
        row, work_kind="Job", image="installed-reference-image"
    )
    resumed, _ = snapshots.srw_snapshot_config(result)
    assert resumed["agent"]["llm"] == blob["agent"]["llm"]
    assert resumed["prompts"] == blob["prompts"]
    assert resumed["instructions"] == "historical instructions"
    assert "remote" not in resumed["agent"]["workspace"]
    assert row["status"] == "paused"
    assert row["resolved_config"] is blob


@pytest.mark.parametrize("kind", ["Job", "Session"])
def test_unfrozen_history_is_reported_without_fabricating_parity(kind):
    from orchestrator.operator_cli.manifest_execution_migration import (
        historical_snapshot,
    )

    row = {"id": WORK, "user_id": USER, "metadata": {}, "context": {}}
    assert historical_snapshot(row, work_kind=kind, image="installed") is None


@pytest.mark.asyncio
async def test_session_delivery_reads_snapshot_without_live_config_lookup(monkeypatch):
    from orchestrator.services import session_config_resolution as sessions

    frozen = snapshots.rendered_srw_snapshot(
        {
            "agent": {"llm": {"model": "frozen"}, "workspace": {"backend": "virtual"}},
            "prompts": {"persona": "frozen prompt"},
        },
        {"llm": {"model": "frozen"}, "workspace": {"backend": "virtual"}},
        work_kind="Session",
        work_id=WORK,
        owner_id=USER,
        project_ids=[],
        config_name="removed-config",
        description="Session",
        datasource_ids=[],
        policy_revisions={},
        image="installed",
        dependencies=[],
    )
    frozen.update(id=uuid4(), generation=2)
    db = SimpleNamespace(fetchrow=AsyncMock(return_value=frozen))
    check = AsyncMock()
    deps = SimpleNamespace(
        store=db,
        enforce_dispatch_grants=check,
        thread_project_ids=AsyncMock(return_value=[]),
        thread_has_knowledge_scope=AsyncMock(return_value=False),
        inject_thread_dispatch_credentials=AsyncMock(side_effect=lambda co, **_: co),
    )
    monkeypatch.setattr(
        sessions, "acknowledged_grant_strip", AsyncMock(return_value=None)
    )
    monkeypatch.setattr(
        sessions, "resolve_config", lambda **_: pytest.fail("live resolver used")
    )
    delivered = await sessions.resolve_session_config(
        {"id": WORK, "user_id": USER, "permission_mode": "autonomous"},
        {},
        dependencies=deps,
    )
    assert delivered["agent"]["llm"]["model"] == "frozen"
    assert delivered["execution_snapshot"]["generation"] == 2
    assert delivered["agent"]["interactive"]["permission_mode"] == "autonomous"
    check.assert_awaited_once()
    assert check.await_args.args[0]["interactive"]["permission_mode"] == "autonomous"


@pytest.mark.asyncio
@pytest.mark.parametrize("recipient_supports_snapshot", [True, False])
async def test_job_resume_uses_frozen_config_and_requires_capable_recipient(
    monkeypatch, recipient_supports_snapshot
):
    import orchestrator.main as main

    frozen = snapshots.rendered_srw_snapshot(
        {
            "agent": {
                "llm": {"model": "admitted-model"},
                "workspace": {"backend": "none"},
            },
            "prompts": {"persona": "admitted prompt"},
        },
        {"llm": {"model": "admitted-model"}, "workspace": {"backend": "none"}},
        work_kind="Job",
        work_id=WORK,
        owner_id=USER,
        project_ids=[],
        config_name="removed-expert-profile",
        description="Task",
        datasource_ids=[],
        policy_revisions={},
        image="installed",
        dependencies=[],
    )
    frozen.update(id=uuid4(), generation=1)
    job = {
        "id": WORK,
        "user_id": USER,
        "status": "paused",
        "config_name": "changed-profile",
        "config_override": {
            "llm": {"model": "changed-model"},
            "workspace": {"backend": "none"},
        },
        "context": {},
    }
    agent = {"id": USER, "pod_ip": "127.0.0.1", "pod_port": 8001}
    db = SimpleNamespace(
        fetchrow=AsyncMock(return_value=frozen),
        managed_repository_authorities_are_current=AsyncMock(return_value=True),
        update_job_status=AsyncMock(),
        heartbeat=AsyncMock(),
        get_expert_by_id=AsyncMock(side_effect=AssertionError("live Expert read")),
    )
    sent = []

    class Client:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def get(self, url):
            return SimpleNamespace(
                status_code=200,
                json=lambda: {
                    "capabilities": {
                        "resolved_config_resume": recipient_supports_snapshot
                    }
                },
            )

        async def post(self, url, *, json):
            sent.append(deepcopy(json))
            return SimpleNamespace(status_code=200)

    decision = SimpleNamespace(
        ready=True,
        effective_backend="none",
        safe_projection=lambda: {"effective_backend": "none"},
    )
    check = AsyncMock()
    monkeypatch.setattr(main.app.state.resources, "postgres_db", db)
    monkeypatch.setattr(
        main.app.state.resources.settings, "completion_commands_enabled", False
    )
    monkeypatch.setattr(
        job_workspace_authority_module,
        "prepare_job_workspace_runtime",
        AsyncMock(return_value=("proceed", job, None)),
    )
    monkeypatch.setattr(
        job_workspace_authority_module,
        "attest_pinned_k8s_job_workspace",
        AsyncMock(return_value=(job, None)),
    )
    monkeypatch.setattr(
        job_datasource_selection_module,
        "resolve_authorized_job_datasources",
        AsyncMock(return_value=[]),
    )
    monkeypatch.setattr(
        job_start_bundle_module, "job_project_repositories", AsyncMock(return_value=[])
    )
    monkeypatch.setattr(
        managed_repository_authority_module,
        "authorize_job_repository_transport",
        AsyncMock(return_value=(None, [], [])),
    )
    monkeypatch.setattr(
        job_workspace_runtime_module,
        "inject_matching_workspace_config",
        lambda _job, co, **_: (co, decision),
    )
    monkeypatch.setattr(
        workspace_tier_policy_module, "inject_lite_workspace_config", lambda co, **_: co
    )
    monkeypatch.setattr(deployment_gates_module, "is_experts_db_enabled", lambda: False)
    monkeypatch.setattr(
        grant_enforcement_module, "user_experts_enabled", AsyncMock(return_value=True)
    )
    monkeypatch.setattr(grant_enforcement_module, "enforce_dispatch_grants", check)
    monkeypatch.setattr(
        job_dispatch_credentials_module,
        "inject_dispatch_credentials",
        AsyncMock(side_effect=lambda _job, co, **_: co),
    )
    monkeypatch.setattr(
        config_resolver_module,
        "resolve_config",
        lambda **_: pytest.fail("resume used live SRW resolver"),
    )
    monkeypatch.setattr(
        runtime_actor_module,
        "mint_worker_runtime_actor",
        AsyncMock(return_value=SimpleNamespace(to_payload=lambda: {})),
    )
    monkeypatch.setattr(
        job_workspace_authority_module,
        "pinned_k8s_job_workspace_authority_is_current",
        AsyncMock(return_value=True),
    )
    monkeypatch.setattr(
        controls_composition,
        "prepare_pinned_job_mutation_target",
        AsyncMock(
            return_value=SimpleNamespace(
                agent=agent, recipient=SimpleNamespace(model_dump=lambda **_: {})
            )
        ),
    )
    monkeypatch.setattr(httpx, "AsyncClient", Client)

    assert (
        await control_seams.resume_job_on_agent(job, agent)
        is recipient_supports_snapshot
    )
    if recipient_supports_snapshot:
        assert sent[0]["resolved_config"]["agent"]["llm"]["model"] == "admitted-model"
        assert sent[0]["resolved_config"]["prompts"]["persona"] == "admitted prompt"
        assert "config_override" not in sent[0]
        check.assert_awaited_once()
    else:
        assert sent == []
        db.update_job_status.assert_not_awaited()
    db.get_expert_by_id.assert_not_awaited()


@pytest.mark.asyncio
async def test_resume_preflight_checks_frozen_policy_without_live_expert(monkeypatch):
    monkeypatch.setattr(
        "orchestrator.services.vm_workspace_recovery_store.VMWorkspaceRecoveryStore.unresolved_participation",
        AsyncMock(return_value=None),
    )
    import orchestrator.main as main

    frozen = snapshots.rendered_srw_snapshot(
        {
            "agent": {
                "llm": {"model": "admitted-model"},
                "workspace": {"backend": "none"},
            },
            "prompts": {},
        },
        {"llm": {"model": "admitted-model"}, "workspace": {"backend": "none"}},
        work_kind="Job",
        work_id=WORK,
        owner_id=USER,
        project_ids=[],
        config_name="removed-expert-profile",
        description="Task",
        datasource_ids=[],
        policy_revisions={},
        image="installed",
        dependencies=[],
    )
    job = {
        "id": WORK,
        "user_id": USER,
        "execution_harness_adapter": "srw/v1",
        "config_name": "removed-profile",
        "context": {},
    }
    db = SimpleNamespace(fetchrow=AsyncMock(return_value=frozen))
    check = AsyncMock(
        side_effect=grant_enforcement_module.GrantDenied(["revoked model grant"])
    )
    monkeypatch.setattr(main.app.state.resources, "postgres_db", db)
    monkeypatch.setattr(
        main.app.state.resources.completion_control_boundary, "guard", AsyncMock()
    )
    monkeypatch.setattr(
        grant_enforcement_module, "user_experts_enabled", AsyncMock(return_value=True)
    )
    monkeypatch.setattr(grant_enforcement_module, "enforce_dispatch_grants", check)
    monkeypatch.setattr(
        config_resolver_module,
        "resolve_config",
        lambda **_: pytest.fail("resume preflight read live config"),
    )
    with pytest.raises(HTTPException) as denied:
        await control_seams.resume_job_internal(WORK, user={"id": USER}, job=job)
    assert denied.value.status_code == 403
    assert check.await_args.args[0]["llm"]["model"] == "admitted-model"


@pytest.mark.asyncio
async def test_migration_preview_is_read_only_and_apply_preserves_lifecycle():
    from orchestrator.operator_cli.manifest_execution_migration import migrate_batch

    row = {
        "id": WORK,
        "user_id": USER,
        "project_id": None,
        "status": "paused",
        "context": {},
        "resolved_config": {
            "agent": {
                "llm": {"model": "private-model"},
                "workspace": {"backend": "none"},
            },
            "prompts": {"persona": "private-prompt"},
        },
    }

    class ImportConnection(Connection):
        async def fetchrow(self, query, *args):
            if "SELECT * FROM jobs" in query:
                return deepcopy(row)
            return await super().fetchrow(query, *args)

        async def fetchval(self, query, *args):
            assert "FROM srw_execution_specs" in query
            return self.execution is not None

    connection = ImportConnection()
    db = database(connection)
    db.fetch = AsyncMock(return_value=[{"id": WORK}])
    preview = await migrate_batch(db, apply=False, kind="Job", limit=1)
    assert preview["counts"] == {"ready": 1}
    assert preview["nextCursor"] == f"Job:{WORK}"
    assert connection.execution is None
    applied = await migrate_batch(db, apply=True, kind="Job", limit=1)
    assert applied["counts"] == {"imported": 1}
    assert row["status"] == "paused"
    assert len(connection.revisions) == 1
    encoded = json.dumps(applied)
    assert "private-model" not in encoded and "private-prompt" not in encoded
    await migrate_batch(db, apply=True, kind="Job", limit=1)
    assert len(connection.revisions) == 1
