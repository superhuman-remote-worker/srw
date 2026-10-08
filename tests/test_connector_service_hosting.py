"""Service-pod hosting without a database (connector drivers D5 item 3).

The Kubernetes effects of ``ServicePodRuntime`` against a fake API, the
composition and the loop. The reconciler's passes run against PostgreSQL in
tests/test_connector_service_hosting_real_postgres.py.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest

from orchestrator.application import background_tasks
from orchestrator.application import connectors as connectors_composition
from orchestrator.application.settings import DeploymentSettings
from orchestrator.services import connector_service_hosting as hosting
from orchestrator.services.connector_service_launch import ServicePodIdentity

IDENTITY = ServicePodIdentity(
    identity_id="11111111-2222-4333-8444-555555555555",
    connector_id="66666666-7777-4888-8999-aaaaaaaaaaaa",
    driver="srw.echo-service/v1",
    digest="sha256:" + "ab" * 32,
    generation="hmac-sha256:" + "cd" * 32,
)
POD = IDENTITY.pod_name


class ApiError(Exception):
    def __init__(self, status: int, body: str = "") -> None:
        super().__init__(f"HTTP {status}")
        self.status = status
        self.body = body


class FakeApi:
    """The namespaced calls the runtime makes, as one recorder."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []
        self.objects: dict[tuple[str, str], dict] = {}
        self.fail: dict[str, Exception] = {}

    def __getattr__(self, name: str):
        def call(**kwargs: Any):
            kwargs.pop("_request_timeout", None)
            self.calls.append((name, kwargs))
            if name in self.fail:
                raise self.fail[name]
            verb, _, kind = name.partition("_namespaced_")
            if verb == "create":
                body = kwargs["body"]
                key = (kind, body["metadata"]["name"])
                if key in self.objects:
                    raise ApiError(409)
                stored = {
                    **body,
                    "metadata": {**body["metadata"], "uid": f"uid-{len(self.objects)}"},
                }
                self.objects[key] = stored
                return stored
            if verb == "read":
                key = (kind, kwargs["name"])
                if key not in self.objects:
                    raise ApiError(404)
                return self.objects[key]
            if verb == "delete":
                key = (kind, kwargs["name"])
                if key not in self.objects:
                    raise ApiError(404)
                del self.objects[key]
                return {}
            if verb == "patch":
                return {}
            if verb == "list":
                selector = dict(
                    item.split("=", 1) if "=" in item else (item, None)
                    for item in kwargs["label_selector"].split(",")
                )
                items = []
                for (object_kind, _), body in self.objects.items():
                    if object_kind != kind:
                        continue
                    labels = body["metadata"].get("labels", {})
                    if all(
                        key in labels and (value is None or labels[key] == value)
                        for key, value in selector.items()
                    ):
                        items.append(body)
                return {"items": items}
            raise AssertionError(name)

        return call


def _plan(kinds=("network_policy", "secret", "service", "pod")):
    metadata = {"name": POD, "labels": dict(IDENTITY.labels)}
    bodies = {kind: {"kind": kind, "metadata": dict(metadata)} for kind in kinds}
    return SimpleNamespace(
        identity=IDENTITY,
        network_policy=bodies["network_policy"],
        secret=bodies["secret"],
        service=bodies["service"],
        pod=bodies["pod"],
    )


@pytest.fixture
def api():
    return FakeApi()


@pytest.fixture
def runtime(api):
    return hosting.ServicePodRuntime(api, api, namespace="srw-connectors")


@pytest.mark.asyncio
async def test_launch_creates_the_policy_first_and_the_pod_owns_its_objects(
    api, runtime
):
    uid = await runtime.launch(_plan())
    created = [name for name, _ in api.calls if name.startswith("create_")]
    assert created == [
        "create_namespaced_network_policy",
        "create_namespaced_secret",
        "create_namespaced_service",
        "create_namespaced_pod",
    ]
    assert uid == "uid-3"
    patches = [kwargs for name, kwargs in api.calls if name.startswith("patch_")]
    assert {p["name"] for p in patches} == {POD}
    assert patches[0]["body"]["metadata"]["ownerReferences"] == [
        {"apiVersion": "v1", "kind": "Pod", "name": POD, "uid": "uid-3"}
    ]
    assert all(kwargs["namespace"] == "srw-connectors" for _, kwargs in api.calls)


@pytest.mark.asyncio
async def test_a_retried_launch_reads_the_existing_pod(api, runtime):
    await runtime.launch(_plan())
    assert await runtime.launch(_plan()) == "uid-3"


@pytest.mark.asyncio
async def test_a_quota_refusal_is_a_capacity_error_not_unconfirmed(api, runtime):
    api.fail["create_namespaced_pod"] = ApiError(
        403, 'pods "x" is forbidden: exceeded quota: srw-connector-service-pods'
    )
    with pytest.raises(hosting.ServiceCapacityError):
        await runtime.launch(_plan())
    api.fail["create_namespaced_pod"] = ApiError(500)
    with pytest.raises(hosting.ServiceRuntimeError):
        await runtime.launch(_plan())


@pytest.mark.asyncio
async def test_observe_reads_readiness_of_the_driver_container(api, runtime):
    assert (await runtime.observe(IDENTITY)).absent
    api.objects[("pod", POD)] = {
        "metadata": {"name": POD, "uid": "u", "labels": dict(IDENTITY.labels)},
        "status": {
            "phase": "Running",
            "containerStatuses": [{"name": "driver", "ready": True, "state": {}}],
        },
    }
    state = await runtime.observe(IDENTITY)
    assert (state.phase, state.uid, state.ready) == ("Running", "u", True)
    api.objects[("pod", POD)]["metadata"]["labels"] = {}
    assert (await runtime.observe(IDENTITY)).phase == "Replaced"


@pytest.mark.asyncio
async def test_remove_deletes_every_object_and_the_binding_policies(api, runtime):
    await runtime.launch(_plan())
    binding = {
        "kind": "network_policy",
        "metadata": {
            "name": f"{POD}-t000000000001",
            "labels": {**IDENTITY.labels, "srw.io/binding-owner-kind": "thread"},
        },
    }
    api.objects[("network_policy", binding["metadata"]["name"])] = binding
    assert await runtime.remove(IDENTITY) is True
    assert api.objects == {}
    # Removing again is a no-op: every object is already gone.
    assert await runtime.remove(IDENTITY) is True


@pytest.mark.asyncio
async def test_binding_policies_are_synced_to_the_desired_set(api, runtime):
    def body(name):
        return {
            "kind": "network_policy",
            "metadata": {
                "name": name,
                "labels": {**IDENTITY.labels, "srw.io/binding-owner-kind": "job"},
            },
        }

    await runtime.sync_binding_policies(IDENTITY, {"a": body("a"), "b": body("b")})
    assert {name for kind, name in api.objects} == {"a", "b"}
    await runtime.sync_binding_policies(IDENTITY, {"b": body("b"), "c": body("c")})
    assert {name for kind, name in api.objects} == {"b", "c"}


@pytest.mark.asyncio
async def test_managed_objects_lists_every_kind_by_its_manager_label(api, runtime):
    await runtime.launch(_plan())
    found = await runtime.managed_objects()
    assert sorted(name for _, name, _ in found) == [POD] * 4
    assert {identity for _, _, identity in found} == {IDENTITY.identity_id}
    selectors = {
        kwargs["label_selector"]
        for name, kwargs in api.calls
        if name.startswith("list_")
    }
    assert selectors == {"srw/managed-by=connector-service-hosting"}


# =============================================================================
# Composition and the loop
# =============================================================================


def _settings(**over: Any) -> DeploymentSettings:
    values: dict[str, Any] = dict(
        auto_assign_enabled=False,
        stateless_session_enabled=False,
        stateless_worker_enabled=False,
        stateless_worker_default_enabled=False,
        completion_commands_enabled=False,
        completion_status_reorder_enabled=False,
        persistent_agent_reconciliation_enabled=False,
        officer_runtime_verification_enabled=False,
        officer_auto_pull_release_enabled=False,
        completion_finalizer_inline_delay_seconds=0.0,
        connector_service_pods_enabled=True,
        connector_service_namespace="srw-connectors",
        connector_service_release_namespace="srw",
        connector_driver_shim_image="srw-registry:5000/srw-driver-shim@sha256:"
        + "a" * 64,
        connector_service_exchange_host="srw-orchestrator.srw.svc",
        connector_lease_exchange_port=8088,
        connector_service_orchestrator_labels={
            "app.kubernetes.io/component": "orchestrator"
        },
        connector_service_max_installation=3,
        connector_service_idle_seconds=60.0,
        connector_service_resources={"limits": {"memory": "128Mi"}},
    )
    values.update(over)
    return DeploymentSettings(**values)


def test_hosting_settings_come_from_the_deployment():
    resources = SimpleNamespace(settings=_settings())
    settings = connectors_composition.service_hosting_settings(resources)
    assert settings.namespace == "srw-connectors"
    assert settings.max_installation == 3
    assert settings.idle_seconds == 60.0
    policy = settings.launch_policy("10.43.0.20")
    assert policy.exchange_address == "10.43.0.20"
    assert policy.memory_limit == "128Mi"
    assert policy.cpu_limit == "500m"


@pytest.mark.parametrize(
    "over",
    [
        {"connector_service_pods_enabled": False},
        {"connector_driver_shim_image": ""},
        {"connector_lease_exchange_port": None},
        {"connector_service_orchestrator_labels": {}},
    ],
)
def test_hosting_is_off_when_disabled_or_incomplete(over):
    resources = SimpleNamespace(settings=_settings(**over))
    assert connectors_composition.service_hosting_settings(resources) is None


def test_the_reconciler_is_registered_leader_gated_and_shut_down_in_order():
    order = background_tasks.BACKGROUND_TASK_SHUTDOWN_ORDER
    assert "connector_service_reconciler" in order
    assert order.index("connector_lease_exchange") < order.index(
        "connector_service_reconciler"
    )
    source = __import__("inspect").getsource(background_tasks.start_background_tasks)
    assert (
        'tasks.start_leader_gated(\n            "connector_service_reconciler"'
        in source
    )


def test_the_egress_route_needs_access_to_the_connector_and_shows_its_pods():
    import contextlib
    from datetime import datetime, timezone
    from unittest.mock import MagicMock

    from fastapi import HTTPException
    from fastapi.testclient import TestClient

    from orchestrator.routers.datasources import DatasourcesDependencies, router
    from orchestrator.services.connector_drivers import builtin_connector_drivers
    from orchestrator.services.connector_drivers.matrix import HostingStatus
    from orchestrator.services.datasources import DatasourceDependencies
    from tests._mounted_router import mount_router

    connector = IDENTITY.connector_id
    resolved = datetime(2026, 10, 8, tzinfo=timezone.utc)
    rows = [
        {
            "id": IDENTITY.identity_id,
            "image_digest": IDENTITY.digest,
            "ready_at": resolved,
            "egress": '{"hosts": [{"host": "one.one.one.one", "addresses": ["1.1.1.1"]}], "dns": "none"}',
            "egress_resolved_at": resolved,
            "created_at": resolved,
            "revoked_at": None,
            "revoke_reason": None,
            "launch_error": None,
        }
    ]

    class Conn:
        async def fetch(self, query, *args):
            assert str(args[0]) == connector
            return rows

    class Store:
        @contextlib.asynccontextmanager
        async def acquire(self):
            yield Conn()

    checked: list[str] = []

    async def access(_request, _store, datasource_id):
        checked.append(datasource_id)
        if datasource_id != connector:
            raise HTTPException(status_code=404)
        return {"id": "u"}, {"id": connector, "type": "echo_service"}

    async def unreachable(*_args, **_kwargs):
        raise AssertionError("not this gate")

    store = Store()
    deps = DatasourcesDependencies(
        store=store,
        operations=DatasourceDependencies(
            store=store,
            vector_db=MagicMock(),
            knowledge_index=MagicMock(),
            mcp_datasources_enabled=lambda: False,
            mcp_stdio_enabled=lambda: False,
            validate_mcp_datasource=lambda _url, _creds: None,
            connector_drivers=builtin_connector_drivers(echo_service_image="r/echo:1"),
        ),
        require_approved_user=unreachable,
        require_project_member=unreachable,
        require_project_owner=unreachable,
        require_datasource_access=access,
        require_datasource_owner=unreachable,
        require_job_access=unreachable,
        service_hosting=HostingStatus(enabled=True),
    )
    client = TestClient(
        mount_router(
            router, factories={"datasources_dependencies_factory": lambda: deps}
        )
    )
    answer = client.get(f"/api/datasources/{connector}/egress")
    assert answer.status_code == 200
    body = answer.json()
    assert body["driver"] == "srw.echo-service/v1"
    assert body["pods"][0]["enforced"]["hosts"][0]["addresses"] == ["1.1.1.1"]
    assert body["pods"][0]["ready"] is True
    assert body["installation"]["reason"] == "start_up_wait_unverified"
    assert (
        client.get(f"/api/datasources/{IDENTITY.identity_id}/egress").status_code == 404
    )
    assert checked == [connector, IDENTITY.identity_id]


@pytest.mark.asyncio
async def test_the_loop_runs_passes_until_shutdown_and_survives_errors():
    shutdown = asyncio.Event()
    passes: list[str] = []

    class Reconciler:
        async def reconcile_once(self):
            passes.append("pass")
            if len(passes) == 1:
                raise RuntimeError("transient")
            if len(passes) == 3:
                shutdown.set()
            return hosting.ReconcileReport()

    builds = iter([None, Reconciler(), Reconciler(), Reconciler(), Reconciler()])
    await asyncio.wait_for(
        hosting.connector_service_reconciler(
            shutdown, build=lambda: next(builds), interval_seconds=0.01
        ),
        timeout=10,
    )
    assert passes == ["pass", "pass", "pass"]
