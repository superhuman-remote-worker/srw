"""Native manifests use the production Job funnel and all migration fences."""

from copy import deepcopy
from dataclasses import replace
import base64
import json
from pathlib import Path
import re
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import asyncpg
from fastapi import HTTPException
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

from tests import _b09_control_seams as control_seams

from orchestrator.database.postgres import PostgresDB
from orchestrator.services.generic_harness_runtime import GenericPodObservation
from orchestrator.services.manifest_execution import ManifestExecutionService
from orchestrator.services.manifest_execution_snapshot import read_execution
from orchestrator.services.manifest_resources import ManifestResourceService
from orchestrator.application import preparation as preparation_composition
from orchestrator.services import config_resolver as config_resolver_module
from orchestrator.services import container_provisioner as container_provisioner_module
from orchestrator.services import (
    job_dispatch_credentials as job_dispatch_credentials_module,
)
from orchestrator.services import job_start_bundle as job_start_bundle_module
from orchestrator.services import runtime_actor as runtime_actor_module
from orchestrator.services import vm_provisioner as vm_provisioner_module


@pytest.fixture(scope="module")
def postgres_url():
    with PostgresContainer("pgvector/pgvector:pg15") as container:
        yield re.sub(
            r"^postgresql\+\w+://", "postgresql://", container.get_connection_url()
        )


@pytest_asyncio.fixture
async def database(postgres_url):
    name = "native_full_" + uuid4().hex
    admin = await asyncpg.connect(postgres_url)
    await admin.execute(f'CREATE DATABASE "{name}"')
    await admin.close()
    db = PostgresDB(postgres_url.rsplit("/", 1)[0] + "/" + name)
    await db.connect()
    snapshot = (
        Path(__file__).resolve().parents[1]
        / "src/orchestrator/database/schema_current.sql"
    )
    await db.execute(snapshot.read_text())
    db.manifest_runtime_image = "test.invalid/srw:installed"
    yield db
    await db.disconnect()


@pytest_asyncio.fixture
async def actor(database):
    row = await database.fetchrow(
        "INSERT INTO users(display_name,is_approved,is_admin) "
        "VALUES('Native integration owner',TRUE,TRUE) RETURNING *"
    )
    return dict(row)


class ProcessRuntime:
    """The sole external boundary: Kubernetes observation and process cleanup."""

    def __init__(self):
        self.pods = {}
        self.launches = []
        self.cleaned = []

    async def observe(self, identity, *, expected_pod_uid=None):
        return self.pods.get(
            identity.pod_name, GenericPodObservation(identity.pod_name, None, "Absent")
        )

    async def launch(self, plan):
        observation = GenericPodObservation(
            plan.identity.pod_name,
            str(uuid4()),
            "Running",
            image_id="test.invalid/plain@sha256:" + "a" * 64,
        )
        self.launches.append(plan)
        self.pods[plan.identity.pod_name] = observation
        return observation

    def exit(self, code=0):
        name = self.launches[-1].identity.pod_name
        self.pods[name] = replace(
            self.pods[name],
            phase="Succeeded" if code == 0 else "Failed",
            process_exit_code=code,
            containers_terminal=True,
        )

    async def cancel(self, identity, *, expected_pod_uid):
        self.exit(143)
        return self.pods[identity.pod_name]

    async def cleanup(self, identity, *, expected_pod_uid):
        assert self.pods[identity.pod_name].pod_uid == expected_pod_uid
        self.cleaned.append(identity.pod_name)
        del self.pods[identity.pod_name]
        return True


def assignment(*, adapter="generic", mode="ProcessExit"):
    runtime = {
        "image": "test.invalid/plain:v1",
        "config": {
            "tools": ["arbitrary_read_file"],
            "a": None,
            "nested": {"password": "private-image-setting", "items": [None, {}]},
        },
    }
    if adapter == "srw/v1":
        runtime = {
            "image": "test.invalid/srw:installed",
            "adapter": adapter,
            "config": {
                "config_name": "worker_base",
                "config": {"llm": {"model": "admitted-model"}},
                "prompts": {"persona": "The admitted persona stays fixed."},
            },
        }
    return {
        "apiVersion": "srw/v1alpha1",
        "kind": "Job",
        "metadata": {"name": "full-schema-job"},
        "spec": {
            "task": {"text": "Execute the assignment", "data": {"ticket": 17}},
            "execution": {
                "expert": {"inline": {"runtime": runtime}},
                "workspace": None,
            },
            "completion": {"mode": mode},
            "retry": {"maxAttempts": 1},
            "timeoutSeconds": 60,
        },
    }


async def admit(database, actor, document, *, harness_egress="[]"):
    runtime = ProcessRuntime()
    execution = ManifestExecutionService(
        database,
        runtime=runtime,
        namespace="test",
        srw_image=database.manifest_runtime_image,
        native_hosting_enabled=True,
        harness_egress=harness_egress,
    )
    resources = ManifestResourceService(database, admit_job=execution.admit)
    result = await resources.apply(json.dumps(document), actor, format="json")
    work_id = next(iter(result["executions"].values()))
    snapshot = await read_execution(database, "Job", work_id)
    return execution, runtime, resources, result, work_id, snapshot


@pytest.mark.asyncio
async def test_session_partial_patch_freezes_delta_and_rejects_stale_runtime_and_owner(
    database,
    actor,
    monkeypatch,
):
    import orchestrator.main as main
    from orchestrator.services.manifest_execution_snapshot import srw_snapshot_config
    from shared.runtime.core.session_config_patch import RESOLVED_PATCH_MARKER

    thread_id = await database.create_thread(
        user_id=str(actor["id"]),
        authority_user_id=str(actor["id"]),
        initial_metadata={
            "config_override": {
                "llm": {"model": "gpt-4o", "temperature": 0.2, "top_p": 0.73},
                "workspace": {"backend": "none"},
            }
        },
    )
    before = await read_execution(database, "Session", thread_id)
    original, _ = srw_snapshot_config(before)

    async def changed_source(*args, **kwargs):
        pytest.fail("A partial PATCH reloaded a changed configuration source")

    monkeypatch.setattr(database, "get_expert_by_id", changed_source)
    monkeypatch.setattr(database, "get_user_settings", changed_source)
    monkeypatch.setattr(
        database, "manifest_skills_provider", changed_source, raising=False
    )
    monkeypatch.setattr(main.app.state.resources, "postgres_db", database)
    thread = await database.get_thread(thread_id)
    delivery, _ = await control_seams.apply_thread_config_update(
        thread_id,
        thread,
        {"llm": {"temperature": 0.7}},
        None,
        request=None,
        actor=None,
        managed_runtime=True,
        snapshot_patch_protocol=1,
        snapshot_generation=1,
    )
    current = await read_execution(database, "Session", thread_id)
    resolved, _ = srw_snapshot_config(current)
    assert current["generation"] == 2
    assert delivery[RESOLVED_PATCH_MARKER]["generation"] == 2
    assert resolved["agent"]["llm"] == {**original["agent"]["llm"], "temperature": 0.7}
    assert resolved["prompts"] == original["prompts"]
    assert resolved["instructions"] == original["instructions"]
    assert (
        await database.fetchval(
            "SELECT count(*) FROM srw_execution_spec_revisions WHERE execution_id=$1",
            current["id"],
        )
        == 2
    )
    stored = await database.get_thread(thread_id)
    for replacement in [
        {"expert_id": str(uuid4())},
        {"prompts": {"persona": "replacement"}},
        {"subagents": {"roster": {}}},
        {"extra": {"instruction_files": []}},
    ]:
        with pytest.raises(HTTPException) as denied:
            await control_seams.apply_thread_config_update(
                thread_id,
                stored,
                replacement,
                None,
                request=None,
                actor=actor,
            )
        assert denied.value.status_code == 409
        assert "new session" in denied.value.detail
    assert (await read_execution(database, "Session", thread_id))["generation"] == 2
    for protocol, generation in [(None, None), (1, 1)]:
        with pytest.raises(HTTPException) as denied:
            await control_seams.apply_thread_config_update(
                thread_id,
                stored,
                {"llm": {"temperature": 0.9}},
                None,
                request=None,
                actor=None,
                managed_runtime=True,
                snapshot_patch_protocol=protocol,
                snapshot_generation=generation,
            )
        assert denied.value.status_code == 409
    assert (await database.get_thread(thread_id))["metadata"] == stored["metadata"]

    await database.execute(
        "UPDATE users SET is_approved=FALSE WHERE id=$1", actor["id"]
    )
    with pytest.raises(HTTPException) as denied:
        await control_seams.apply_thread_config_update(
            thread_id,
            stored,
            {"llm": {"temperature": 0.9}},
            None,
            request=None,
            actor=actor,
        )
    assert denied.value.status_code == 403
    assert (await database.get_thread(thread_id))["metadata"] == stored["metadata"]
    assert (await read_execution(database, "Session", thread_id))["generation"] == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("exit_code,terminal_status", [(0, "completed"), (1, "failed")])
async def test_generic_admission_process_observation_and_reapply(
    database, actor, exit_code, terminal_status, monkeypatch
):
    async def wrong_adapter(*args, **kwargs):
        pytest.fail("Generic private configuration entered the SRW resolver")

    monkeypatch.setattr(
        "orchestrator.services.manifest_execution_snapshot.prepare_srw_snapshot",
        wrong_adapter,
    )
    document = assignment()
    execution, runtime, resources, result, work_id, snapshot = await admit(
        database, actor, document
    )
    job = await database.get_job(work_id)
    assert job["status"] == "created"
    assert job["execution_harness_adapter"] == "generic"
    assert job["execution_lane"] == "pinned"
    assert job["assigned_agent_id"] is None
    context = (
        json.loads(job["context"])
        if isinstance(job["context"], str)
        else job["context"]
    )
    assert context["_workspace_contract"]["assigned_backend"] == "none"
    assert context["manifest_task_data"] == {"ticket": 17}
    assert (
        snapshot["resolved"]["spec"]["execution"]["expert"]["inline"]["runtime"][
            "config"
        ]
        == document["spec"]["execution"]["expert"]["inline"]["runtime"]["config"]
    )
    assert (
        await database.fetchval(
            "SELECT count(*) FROM srw_execution_spec_revisions WHERE execution_id=$1",
            snapshot["id"],
        )
        == 1
    )

    # Both accidental legacy claims and unfenced raw status writes must fail.
    assert await database.claim_job_for_agent(work_id, str(uuid4())) is False
    with pytest.raises(asyncpg.CheckViolationError):
        await database.update_job_status(work_id, status="processing")
    await execution.reconcile_one(str(snapshot["id"]))
    assert (await database.get_job(work_id))["status"] == "processing"
    assert runtime.launches[0].pod["spec"]["automountServiceAccountToken"] is False
    runtime.exit(exit_code)
    await execution.reconcile_one(str(snapshot["id"]))
    assert (await database.get_job(work_id))["status"] == terminal_status
    assert len(runtime.cleaned) == 1

    repeated = await resources.apply(json.dumps(document), actor, format="json")
    assert repeated["executions"] == result["executions"]
    await execution.reconcile()
    assert len(runtime.launches) == 1
    assert await database.fetchval("SELECT count(*) FROM jobs") == 1
    assert await database.fetchval("SELECT count(*) FROM job_completion_commands") == 0


@pytest.mark.asyncio
async def test_invalid_operator_egress_rejects_generic_before_admission_effects(
    database, actor
):
    with pytest.raises(HTTPException) as caught:
        await admit(
            database,
            actor,
            assignment(),
            harness_egress='[{"to":[{"ipBlock":{"cidr":"not-cidr"}}]}]',
        )
    assert caught.value.status_code == 503
    assert caught.value.detail["code"] == "HostingConfigurationInvalid"
    for table in (
        "jobs",
        "srw_resources",
        "srw_execution_specs",
        "srw_execution_attempts",
    ):
        assert await database.fetchval(f"SELECT count(*) FROM {table}") == 0


@pytest.mark.asyncio
async def test_retry_uses_current_operator_egress_while_image_settings_remain_opaque(
    database, actor
):
    first_policy = [
        {"to": [{"ipBlock": {"cidr": "192.0.2.1/32"}}], "ports": [{"port": 443}]}
    ]
    next_policy = [
        {
            "to": [
                {
                    "namespaceSelector": {"matchLabels": {"name": "models"}},
                    "podSelector": {"matchLabels": {"app": "gateway"}},
                }
            ],
            "ports": [{"port": 8443}],
        }
    ]
    document = assignment()
    document["spec"]["retry"]["maxAttempts"] = 2
    authored_runtime = document["spec"]["execution"]["expert"]["inline"]["runtime"]
    authored_runtime["config"]["harnessEgress"] = [{}]
    authored_runtime["env"] = {"MANIFEST_HARNESS_EGRESS": "[{}]"}
    execution, runtime, _, _, work_id, snapshot = await admit(
        database, actor, document, harness_egress=json.dumps(first_policy)
    )
    await execution.reconcile_one(str(snapshot["id"]))
    assert runtime.launches[0].network_policy["spec"]["egress"] == first_policy
    runtime.exit(23)
    await execution.reconcile_one(str(snapshot["id"]))
    next_service = ManifestExecutionService(
        database,
        runtime=runtime,
        namespace="test",
        native_hosting_enabled=True,
        harness_egress=next_policy,
    )
    for _ in range(3):
        if len(runtime.launches) == 2:
            break
        await next_service.reconcile_one(str(snapshot["id"]))
    assert len(runtime.launches) == 2
    assert runtime.launches[0].network_policy["spec"]["egress"] == first_policy
    assert runtime.launches[1].network_policy["spec"]["egress"] == next_policy
    assert (
        snapshot["resolved"]["spec"]["execution"]["expert"]["inline"]["runtime"][
            "config"
        ]
        == authored_runtime["config"]
    )
    runtime.exit()
    await next_service.reconcile_one(str(snapshot["id"]))
    assert (await database.get_job(work_id))["status"] == "completed"


@pytest.mark.asyncio
async def test_file_connector_admission_reaches_the_generic_delivery_plan(
    database, actor
):
    document = assignment()
    document["spec"]["execution"]["connectors"] = {
        "fixture": {
            "inline": {
                "driver": "srw.files/v1",
                "config": {"files": {"/run/srw/bindings/fixture": "fixture content"}},
            }
        }
    }
    execution, runtime, _, _, work_id, snapshot = await admit(database, actor, document)
    await execution.reconcile_one(str(snapshot["id"]))
    launch = runtime.launches[0]
    mounts = launch.pod["spec"]["containers"][0]["volumeMounts"]
    files = {
        mount["mountPath"]: base64.b64decode(
            launch.delivery_secret["data"][mount["subPath"]]
        )
        for mount in mounts
    }
    assert files["/run/srw/bindings/fixture"] == b"fixture content"
    assert (
        json.loads(files["/run/srw/config.json"])
        == document["spec"]["execution"]["expert"]["inline"]["runtime"]["config"]
    )
    assert json.loads(files["/run/srw/task.json"]) == document["spec"]["task"]
    assert json.loads(files["/run/srw/bindings.json"])["connectors"]["fixture"][
        "bindings"
    ] == ["/run/srw/bindings/fixture"]
    runtime.exit()
    await execution.reconcile_one(str(snapshot["id"]))
    assert (await database.get_job(work_id))["status"] == "completed"


@pytest.mark.asyncio
async def test_stale_admission_owner_rolls_back_job_resource_and_snapshot(
    database, actor
):
    await database.execute(
        "UPDATE users SET is_approved=FALSE WHERE id=$1", actor["id"]
    )
    # The initial HTTP identity is stale; the common Job insertion lock checks
    # the current owner within the same manifest-application transaction.
    with pytest.raises(HTTPException) as caught:
        await admit(database, actor, assignment())
    assert caught.value.status_code == 403
    assert await database.fetchval("SELECT count(*) FROM jobs") == 0
    assert await database.fetchval("SELECT count(*) FROM srw_resources") == 0
    assert await database.fetchval("SELECT count(*) FROM srw_execution_specs") == 0


@pytest.mark.asyncio
async def test_admin_submission_executes_as_the_scoped_account_owner(database, actor):
    owner_id = await database.fetchval(
        "INSERT INTO users(display_name,is_approved,is_admin) "
        "VALUES('Scoped execution owner',TRUE,FALSE) RETURNING id"
    )
    document = assignment()
    document["metadata"]["scope"] = {"kind": "Account", "name": str(owner_id)}
    execution, runtime, _, _, work_id, snapshot = await admit(database, actor, document)
    assert (await database.get_job(work_id))["user_id"] == owner_id
    assert snapshot["owner_id"] == owner_id
    assert snapshot["resolved"]["metadata"]["scope"]["name"] == str(owner_id)
    await execution.reconcile_one(str(snapshot["id"]))
    assert len(runtime.launches) == 1
    runtime.exit()
    await execution.reconcile_one(str(snapshot["id"]))
    assert (await database.get_job(work_id))["status"] == "completed"


@pytest.mark.asyncio
async def test_native_srw_admission_delivers_frozen_configuration_from_database(
    database, actor, monkeypatch
):
    import orchestrator.main as main

    document = assignment(adapter="srw/v1", mode="Reported")
    _, runtime, _, _, work_id, snapshot = await admit(
        database, actor, document, harness_egress="invalid-installed-json"
    )
    private = snapshot["resolved"]["spec"]["execution"]["expert"]["inline"]["runtime"][
        "config"
    ]
    assert private["format"] == "srw/resolved-config-v1"
    assert private["resolved"]["agent"]["llm"]["model"] == "admitted-model"
    assert (
        private["resolved"]["prompts"]["persona"] == "The admitted persona stays fixed."
    )

    # Historical compatibility columns must never become a second source of
    # configuration after admission. Delivery reads the immutable specification.
    await database.execute(
        "UPDATE jobs SET config_override=$2::jsonb,resolved_config=$3::jsonb WHERE id=$1",
        UUID(work_id),
        json.dumps(
            {
                "llm": {"model": "later-legacy-override"},
                "workspace": {"backend": "none"},
            }
        ),
        json.dumps({"agent": {"llm": {"model": "later-legacy-blob"}}}),
    )
    job = await database.get_job(work_id)
    assert job["execution_harness_adapter"] == "srw/v1"

    async def provider_transport(_job, config, **_):
        value = deepcopy(config)

        def visit(fragment):
            if not isinstance(fragment, dict):
                return
            if fragment.get("model"):
                fragment["provider"] = "openai"
                fragment["base_url"] = "http://model.test.invalid/v1"
            for item in list(fragment.values()):
                visit(item)

        visit(value)
        return value

    monkeypatch.setattr(main.app.state.resources, "postgres_db", database)
    monkeypatch.setattr(
        vm_provisioner_module, "vm_provisioner", SimpleNamespace(mode="kubevirt")
    )
    monkeypatch.setattr(
        container_provisioner_module,
        "container_provisioner",
        SimpleNamespace(is_available=False, in_cluster=False),
    )
    monkeypatch.setattr(
        job_dispatch_credentials_module,
        "inject_dispatch_credentials",
        provider_transport,
    )
    monkeypatch.setattr(
        runtime_actor_module,
        "mint_worker_runtime_actor",
        AsyncMock(return_value=SimpleNamespace(to_payload=lambda: {})),
    )
    monkeypatch.setattr(
        config_resolver_module,
        "resolve_config",
        lambda **_: pytest.fail("Live SRW resolver used after admission"),
    )
    delivered = await job_start_bundle_module.build_job_start_request(
        job,
        dependencies=preparation_composition.job_start_bundle_dependencies(
            main.app.state.resources
        ),
    )
    assert delivered is not None
    assert delivered.config_override is None
    assert delivered.resolved_config["agent"]["llm"]["model"] == "admitted-model"
    assert (
        delivered.resolved_config["prompts"]["persona"]
        == "The admitted persona stays fixed."
    )
    assert delivered.context["manifest_task_data"] == {"ticket": 17}
    assert runtime.launches == []
    assert (await read_execution(database, "Job", work_id))["resolved"] == snapshot[
        "resolved"
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("driver", ["srw.env/v1", "srw.files/v1", "custom/v1"])
async def test_srw_admission_rejects_undeliverable_connector_before_writes(
    database, actor, driver
):
    document = assignment(adapter="srw/v1", mode="Reported")
    document["spec"]["execution"]["connectors"] = {
        "source": {"inline": {"driver": driver, "config": {"literal": None}}}
    }
    with pytest.raises(HTTPException) as error:
        await admit(database, actor, document)
    assert error.value.status_code == 422
    assert "Connector driver" in str(error.value.detail)
    assert await database.fetchval("SELECT count(*) FROM jobs") == 0
    assert await database.fetchval("SELECT count(*) FROM srw_execution_specs") == 0
    assert (
        await database.fetchval("SELECT count(*) FROM srw_resources WHERE kind='Job'")
        == 0
    )
