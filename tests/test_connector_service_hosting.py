"""Service-pod hosting without a database (connector drivers D5 item 3).

The Kubernetes effects of ``ServicePodRuntime`` against a fake API, the
composition and the loop. The reconciler's passes run against PostgreSQL in
tests/test_connector_service_hosting_real_postgres.py.
"""

from __future__ import annotations

import asyncio
import json
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
async def test_an_admission_refusal_records_the_api_servers_reason(api, runtime):
    """A Pod Security (or other 4xx) refusal says why, so the pod's
    launch_error shows it; a server error says only its status."""
    message = (
        'pods "srw-drv-x" is forbidden: violates PodSecurity "baseline:v1.31": '
        "host namespaces (hostNetwork=true)"
    )
    api.fail["create_namespaced_pod"] = ApiError(
        403, json.dumps({"kind": "Status", "message": message, "code": 403})
    )
    with pytest.raises(hosting.ServiceRuntimeError) as raised:
        await runtime.launch(_plan())
    assert "(HTTP 403): " in str(raised.value)
    assert 'violates PodSecurity "baseline:v1.31"' in str(raised.value)
    api.fail["create_namespaced_pod"] = ApiError(500, "internal detail")
    with pytest.raises(hosting.ServiceRuntimeError) as raised:
        await runtime.launch(_plan())
    assert str(raised.value).endswith("(HTTP 500)")


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
async def test_observe_reads_a_terminal_phase_and_when_the_pod_turned_unready(
    api, runtime
):
    pod = {
        "metadata": {"name": POD, "uid": "u", "labels": dict(IDENTITY.labels)},
        "status": {
            "phase": "Failed",
            "reason": "Evicted",
            "containerStatuses": [{"name": "driver", "ready": False, "state": {}}],
        },
    }
    api.objects[("pod", POD)] = pod
    state = await runtime.observe(IDENTITY)
    assert state.lost and state.reason == "Evicted"
    pod["status"] = {
        "phase": "Pending",
        "conditions": [
            {
                "type": "Ready",
                "status": "False",
                "lastTransitionTime": "2026-10-08T10:00:00Z",
            }
        ],
        "initContainerStatuses": [
            {
                "name": "canary-wait",
                "state": {"waiting": {"reason": "CrashLoopBackOff"}},
            }
        ],
        "containerStatuses": [{"name": "driver", "ready": False, "state": {}}],
    }
    state = await runtime.observe(IDENTITY)
    assert not state.lost and not state.ready
    assert state.reason == "CrashLoopBackOff"
    assert state.unready_since.isoformat() == "2026-10-08T10:00:00+00:00"
    pod["status"]["phase"] = "Succeeded"
    assert (await runtime.observe(IDENTITY)).lost


@pytest.mark.asyncio
async def test_the_runtime_reads_the_kubernetes_clients_own_models():
    """The API client returns models, not dicts: their attributes are named
    by each model's attribute_map (cluster_ip for clusterIP), which a
    camel-to-snake rule gets wrong. A dict fake hid that on k3d."""
    from datetime import datetime, timezone

    from kubernetes import client as k8s

    service = k8s.V1Service(spec=k8s.V1ServiceSpec(cluster_ip="10.43.0.20"))
    since = datetime(2026, 10, 8, 10, 0, tzinfo=timezone.utc)
    pod = k8s.V1Pod(
        metadata=k8s.V1ObjectMeta(name=POD, uid="u", labels=dict(IDENTITY.labels)),
        status=k8s.V1PodStatus(
            phase="Pending",
            conditions=[
                k8s.V1PodCondition(
                    type="Ready", status="False", last_transition_time=since
                )
            ],
            init_container_statuses=[
                k8s.V1ContainerStatus(
                    name="canary-wait",
                    image="shim",
                    image_id="",
                    ready=False,
                    restart_count=1,
                    state=k8s.V1ContainerState(
                        waiting=k8s.V1ContainerStateWaiting(reason="CrashLoopBackOff")
                    ),
                    last_state=k8s.V1ContainerState(
                        terminated=k8s.V1ContainerStateTerminated(
                            exit_code=1, message="srw-driver-shim: x\nthe verdict\n"
                        )
                    ),
                )
            ],
            container_statuses=[
                k8s.V1ContainerStatus(
                    name="driver",
                    image="echo",
                    image_id="",
                    ready=False,
                    restart_count=0,
                )
            ],
        ),
    )

    class Core:
        def read_namespaced_service(self, name, namespace, **_kwargs):
            assert (name, namespace) == ("srw-orchestrator", "srw")
            return service

        def read_namespaced_pod(self, name, namespace, **_kwargs):
            return pod

    runtime = hosting.ServicePodRuntime(Core(), object(), namespace="srw-connectors")
    assert await runtime.service_cluster_ip("srw-orchestrator", "srw") == "10.43.0.20"
    state = await runtime.observe(IDENTITY)
    assert (state.phase, state.uid, state.ready) == ("Pending", "u", False)
    assert state.reason == "CrashLoopBackOff"
    assert state.message == "canary-wait: the verdict"
    assert state.unready_since == since
    pod.status.container_statuses[0].ready = True
    pod.status.conditions[0].status = "True"
    state = await runtime.observe(IDENTITY)
    assert state.ready and state.unready_since is None


@pytest.mark.asyncio
async def test_the_exchange_cluster_ip_comes_from_the_api(api, runtime):
    """No DNS: the Service object says where the exchange is."""
    api.objects[("service", "srw-orchestrator")] = {
        "metadata": {"name": "srw-orchestrator"},
        "spec": {"clusterIP": "10.43.0.20"},
    }
    assert await runtime.service_cluster_ip("srw-orchestrator", "srw") == "10.43.0.20"
    name, kwargs = api.calls[-1]
    assert name == "read_namespaced_service"
    assert kwargs == {"name": "srw-orchestrator", "namespace": "srw"}
    api.objects[("service", "srw-orchestrator")]["spec"]["clusterIP"] = "None"
    with pytest.raises(hosting.ServiceRuntimeError, match="no ClusterIP"):
        await runtime.service_cluster_ip("srw-orchestrator", "srw")
    with pytest.raises(hosting.ServiceRuntimeError):
        await runtime.service_cluster_ip("absent", "srw")


@pytest.mark.asyncio
async def test_observe_reads_why_an_init_container_last_failed(api, runtime):
    """The canary wait's verdict (its last log line, the termination
    message with FallbackToLogsOnError) is what the pod is stopped with."""
    verdict = (
        "srw-driver-shim: canary-wait: no 3 rounds in a row with the allowed "
        "targets answering and the canaries refused in 2m0s: the default deny "
        "is not enforced"
    )
    api.objects[("pod", POD)] = {
        "metadata": {"name": POD, "uid": "u", "labels": dict(IDENTITY.labels)},
        "status": {
            "phase": "Pending",
            "initContainerStatuses": [
                {
                    "name": "canary-wait",
                    "state": {"waiting": {"reason": "CrashLoopBackOff"}},
                    "lastState": {
                        "terminated": {
                            "exitCode": 1,
                            "message": "srw-driver-shim: canary 10.43.0.20:8085 is "
                            "still reachable\n" + verdict + "\n",
                        }
                    },
                },
                {"name": "install-shim", "state": {"waiting": {}}},
            ],
            "containerStatuses": [{"name": "driver", "ready": False, "state": {}}],
        },
    }
    state = await runtime.observe(IDENTITY)
    assert state.reason == "CrashLoopBackOff"
    assert state.message == "canary-wait: " + verdict
    # A clean exit, or none yet, is no failure.
    api.objects[("pod", POD)]["status"]["initContainerStatuses"] = [
        {"name": "canary-wait", "state": {"terminated": {"exitCode": 0}}}
    ]
    assert (await runtime.observe(IDENTITY)).message is None
    # No message (an OOM kill): the exit code says something.
    api.objects[("pod", POD)]["status"]["initContainerStatuses"] = [
        {"name": "install-shim", "state": {"terminated": {"exitCode": 137}}}
    ]
    assert (await runtime.observe(IDENTITY)).message == "install-shim: exit 137"


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


def _endpoint(identity: str = IDENTITY.identity_id) -> dict:
    from orchestrator.services.connector_service_launch import endpoint_service

    return endpoint_service(
        connector_id=IDENTITY.connector_id,
        digest=IDENTITY.digest,
        identity_id=identity,
        port=8080,
        namespace="srw-connectors",
    )


@pytest.mark.asyncio
async def test_an_endpoint_is_created_then_pointed_at_another_pod(api, runtime):
    other = "22222222-3333-4444-8555-666666666666"
    assert await runtime.sync_endpoint(_endpoint()) is True
    name = _endpoint()["metadata"]["name"]
    assert ("service", name) in api.objects
    # The same pod again: nothing to do.
    assert await runtime.sync_endpoint(_endpoint()) is False
    assert not [call for call, _ in api.calls if call.startswith("patch_")]
    # Another pod: only the selector is patched (no delete, no gap).
    assert await runtime.sync_endpoint(_endpoint(other)) is True
    (patch,) = [kwargs for call, kwargs in api.calls if call.startswith("patch_")]
    assert patch["name"] == name
    assert patch["body"] == {"spec": {"selector": {"srw.io/driver-identity": other}}}
    assert not [call for call, _ in api.calls if call.startswith("delete_")]


@pytest.mark.asyncio
async def test_an_endpoints_target_is_read_from_its_selector(api, runtime):
    name = _endpoint()["metadata"]["name"]
    assert await runtime.endpoint_target(name) is None
    await runtime.sync_endpoint(_endpoint())
    assert await runtime.endpoint_target(name) == IDENTITY.identity_id
    api.fail["read_namespaced_service"] = ApiError(500)
    with pytest.raises(hosting.ServiceRuntimeError):
        await runtime.endpoint_target(name)


@pytest.mark.asyncio
async def test_endpoints_are_listed_apart_from_any_pods_objects(api, runtime):
    await runtime.launch(_plan())
    await runtime.sync_endpoint(_endpoint())
    name = _endpoint()["metadata"]["name"]
    assert await runtime.endpoint_services() == [name]
    # The identity sweep never sees it: it belongs to the connector.
    assert name not in {found for _, found, _ in await runtime.managed_objects()}
    # Removing the pod's objects leaves it too.
    await runtime.remove(IDENTITY)
    assert ("service", name) in api.objects
    await runtime.delete_endpoint(name)
    assert ("service", name) not in api.objects
    await runtime.delete_endpoint(name)  # already gone: fine


@pytest.mark.asyncio
async def test_a_pod_is_ready_only_when_every_container_is(api, runtime):
    """A managed MCP pod's front serves /readyz from a real MCP probe of
    the server beside it: the pod is not ready before both are."""
    api.objects[("pod", POD)] = {
        "metadata": {"name": POD, "uid": "u", "labels": dict(IDENTITY.labels)},
        "status": {
            "phase": "Running",
            "containerStatuses": [
                {"name": "driver", "ready": True, "state": {}},
                {"name": "front", "ready": False, "state": {}},
            ],
        },
    }
    assert (await runtime.observe(IDENTITY)).ready is False
    api.objects[("pod", POD)]["status"]["containerStatuses"][1]["ready"] = True
    assert (await runtime.observe(IDENTITY)).ready is True


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
        connector_lease_canary_port=8089,
        connector_service_orchestrator_labels={
            "app.kubernetes.io/component": "orchestrator"
        },
        connector_service_max_installation=3,
        connector_service_idle_seconds=60.0,
        connector_service_resources={"limits": {"memory": "128Mi"}},
        connector_service_refused_cidrs=("10.0.50.0/24",),
        connector_service_pod_ip="10.42.0.9",
        connector_service_node_ip="10.0.50.11",
    )
    values.update(over)
    return DeploymentSettings(**values)


class TestEgressWithdrawn:
    @staticmethod
    def _spec():
        from shared.connectors.builtin import ECHO_SERVICE_SPEC

        return ECHO_SERVICE_SPEC

    @staticmethod
    def _recorded(host="one.one.one.one", ports=(443,), private=False):
        return {
            "hosts": [
                {
                    "host": host,
                    "addresses": ["1.1.1.1"],
                    "ports": list(ports),
                    "protocol": "tcp",
                    "literal": False,
                    "many_addresses": False,
                }
            ],
            "dns": "none",
            "dns_reason": None,
            "private_allowed": private,
            "resolved_at": "2026-10-08T00:00:00+00:00",
        }

    def _check(self, recorded, config, *, private_allowed=False):
        from orchestrator.services.connector_service_hosting import egress_withdrawn

        return egress_withdrawn(
            self._spec(),
            None if config is None else {"config": config},
            recorded,
            private_allowed=private_allowed,
        )

    def test_unchanged_egress_holds(self):
        config = {"host": "one.one.one.one", "port": 443, "message": "v2"}
        assert self._check(self._recorded(), config) is None
        assert self._check(json.dumps(self._recorded()), config) is None
        # Gaining private addresses is not a withdrawal (that pod drains).
        assert self._check(self._recorded(), config, private_allowed=True) is None

    def test_a_lost_private_tier_is_withdrawn(self):
        config = {"host": "one.one.one.one", "port": 443}
        found = self._check(self._recorded(private=True), config)
        assert found is not None and "private" in found
        assert (
            self._check(self._recorded(private=True), config, private_allowed=True)
            is None
        )

    @pytest.mark.parametrize(
        "config",
        [
            {"host": "dns.google", "port": 443},
            {"host": "one.one.one.one", "port": 853},
            {"port": 443},
        ],
    )
    def test_a_changed_destination_is_withdrawn(self, config):
        assert self._check(self._recorded(), config) is not None

    def test_a_gone_connector_is_withdrawn_and_no_record_compares_nothing(self):
        assert self._check(self._recorded(), None) == "its connector is gone"
        assert self._check(None, {"host": "x.example", "port": 1}) is None

    def test_a_pinned_address_the_policy_refuses_now_is_withdrawn(self):
        from orchestrator.services.connector_egress import EgressPolicy
        from orchestrator.services.connector_service_hosting import egress_withdrawn

        config = {"config": {"host": "one.one.one.one", "port": 443}}

        def check(*refused: str, private: bool = False):
            policy = EgressPolicy.build(
                ("10.42.0.0/16", "10.43.0.0/16"),
                allow_private=private,
                refused_cidrs=refused,
            )
            return egress_withdrawn(
                self._spec(),
                config,
                self._recorded(),
                private_allowed=private,
                policy=policy,
            )

        assert check() is None
        assert check("10.0.50.0/24") is None
        found = check("10.0.50.0/24", "1.1.1.0/24")
        assert found is not None and "1.1.1.1" in found and "refuses" in found

    def test_an_ipv6_address_pinned_on_a_cluster_now_ipv4_only_is_withdrawn(self):
        from orchestrator.services.connector_egress import EgressPolicy
        from orchestrator.services.connector_service_hosting import egress_withdrawn

        recorded = self._recorded()
        recorded["hosts"][0]["addresses"] = ["1.1.1.1", "2606:4700:4700::1111"]
        config = {"config": {"host": "one.one.one.one", "port": 443}}

        def check(ipv6: bool):
            policy = EgressPolicy.build(
                ("10.42.0.0/16", "10.43.0.0/16"), allow_private=False, ipv6=ipv6
            )
            return egress_withdrawn(
                self._spec(), config, recorded, private_allowed=False, policy=policy
            )

        assert check(True) is None
        found = check(False)
        assert found is not None and "IPv6" in found


def test_the_endpoint_ranks_the_current_generation_first():
    from datetime import datetime, timedelta, timezone

    from orchestrator.services.connector_service_hosting import _serving

    t0 = datetime(2026, 10, 8, tzinfo=timezone.utc)

    def pod(name, generation, minutes, *, ready=True, replaced=False):
        return {
            "id": name,
            "credential_generation": generation,
            "created_at": t0 + timedelta(minutes=minutes),
            "ready_at": t0 if ready else None,
            "replaced_at": t0 if replaced else None,
        }

    old_a = pod("a", "A", 0)
    new_b = pod("b", "B", 5)
    assert _serving([old_a, new_b])["id"] == "b"
    assert _serving([old_a, new_b], "A")["id"] == "a"
    assert _serving([old_a, new_b], "B")["id"] == "b"
    # A current pod being replaced still beats a superseded generation; its
    # ready replacement beats it.
    replaced_a = pod("a", "A", 0, replaced=True)
    assert _serving([replaced_a, new_b], "A")["id"] == "a"
    assert _serving([replaced_a, new_b, pod("a2", "A", 6)], "A")["id"] == "a2"
    # No ready pod of the current generation: the newest ready one serves
    # until it is.
    assert _serving([pod("a", "A", 9, ready=False), new_b], "A")["id"] == "b"
    assert _serving([pod("a", "A", 9, ready=False)], "A")["id"] == "a"


def test_a_lease_drivers_pod_is_never_given_an_access_level():
    from orchestrator.services.connector_service_hosting import credential_generation
    from orchestrator.services.connector_service_launch import pod_config
    from shared.connectors.builtin import ECHO_SERVICE_SPEC

    assert ECHO_SERVICE_SPEC.credential_delivery == "lease"
    config = {"host": "one.one.one.one", "port": 443}
    assert pod_config(ECHO_SERVICE_SPEC, {**config, "access": "ReadOnly"}) == config
    assert pod_config(ECHO_SERVICE_SPEC, None) == {}

    def generation(access):
        return credential_generation(
            ECHO_SERVICE_SPEC,
            {"config": {**config, "access": access}},
            private_allowed=False,
        )

    assert generation("ReadOnly") == generation("ReadWrite")


def test_the_pod_key_index_is_built_without_if_not_exists():
    """An invalid shell of a failed concurrent build must not count as
    applied (0391 follows 0203's runbook)."""
    from pathlib import Path

    sql = (
        Path(__file__).resolve().parents[1]
        / "src/orchestrator/database/migrations/app"
        / "0391_connector_service_pod_key_v2_idx.notx.sql"
    ).read_text()
    statement = "\n".join(
        line for line in sql.splitlines() if not line.startswith("--")
    ).strip()
    assert statement.startswith(
        "CREATE UNIQUE INDEX CONCURRENTLY uq_connector_driver_identities_serving_key"
    )
    assert "IF NOT EXISTS" not in statement
    assert "indisvalid" in sql


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
    assert settings.canary_port == 8089 and policy.canary_port == 8089
    assert settings.refused_cidrs == ("10.0.50.0/24",)
    assert settings.pod_ip == "10.42.0.9"
    assert settings.node_ip == "10.0.50.11"
    assert settings.cluster_problem("10.43.0.20") is None
    assert "10.43.0.20" not in (settings.cluster_problem("10.96.0.10") or "")
    assert "Service address 10.96.0.10" in settings.cluster_problem("10.96.0.10")
    assert settings.reresolve_seconds == 300.0
    assert settings.repin_drain_seconds == 30.0


def test_re_pinning_comes_from_the_deployment(monkeypatch):
    monkeypatch.setenv("CONNECTOR_SERVICE_RERESOLVE_SECONDS", "120")
    monkeypatch.setenv("CONNECTOR_SERVICE_REPIN_DRAIN_SECONDS", "5")
    settings = DeploymentSettings.from_environment()
    assert settings.connector_service_reresolve_seconds == 120.0
    assert settings.connector_service_repin_drain_seconds == 5.0
    monkeypatch.setenv("CONNECTOR_SERVICE_RERESOLVE_SECONDS", "0")
    assert (
        DeploymentSettings.from_environment().connector_service_reresolve_seconds == 0
    )
    hosting_settings = connectors_composition.service_hosting_settings(
        SimpleNamespace(
            settings=_settings(
                connector_service_reresolve_seconds=45.0,
                connector_service_repin_drain_seconds=7.0,
            )
        )
    )
    assert hosting_settings.reresolve_seconds == 45.0
    assert hosting_settings.repin_drain_seconds == 7.0


@pytest.mark.parametrize(
    ("node_ip", "private", "refused", "fragment"),
    [
        ("10.0.50.11", True, ("10.0.50.0/24",), None),
        ("172.18.0.2", True, ("10.0.50.0/24",), "node address 172.18.0.2"),
        ("", True, ("10.0.50.0/24",), "node address is unknown"),
        ("not-an-ip", True, (), "is not an address"),
        # No private tier: a private node is refused to every pod anyway...
        ("172.18.0.2", False, (), None),
        # ...but a public one must be listed.
        ("203.0.113.7", False, (), "node address 203.0.113.7"),
        ("203.0.113.7", False, ("203.0.113.0/24",), None),
        # The cluster ranges and link-local count as refused too.
        ("10.42.0.1", True, (), None),
    ],
)
def test_the_node_must_be_refused_to_every_driver_pod(
    node_ip, private, refused, fragment
):
    import dataclasses

    resources = SimpleNamespace(settings=_settings())
    settings = dataclasses.replace(
        connectors_composition.service_hosting_settings(resources),
        node_ip=node_ip,
        private_tiers=frozenset({"home-allowed"}) if private else frozenset(),
        refused_cidrs=refused,
    )
    found = settings.cluster_problem("10.43.0.20")
    if fragment is None:
        assert found is None
    else:
        assert found is not None and fragment in found


@pytest.mark.parametrize(
    "over",
    [
        {"connector_service_pods_enabled": False},
        {"connector_driver_shim_image": ""},
        {"connector_lease_exchange_port": None},
        # No canary listener: a start-up wait could prove nothing.
        {"connector_lease_canary_port": None},
        {"connector_service_orchestrator_labels": {}},
    ],
)
def test_hosting_is_off_when_disabled_or_incomplete(over):
    resources = SimpleNamespace(settings=_settings(**over))
    assert connectors_composition.service_hosting_settings(resources) is None


def test_hosting_off_runs_the_revoke_loop_on_every_replica():
    """Hosting off (or not configured): a loop on every replica (not
    leader-gated) revokes live service-pod identities, shut down in order."""
    import inspect

    order = background_tasks.BACKGROUND_TASK_SHUTDOWN_ORDER
    assert "connector_service_identity_revoker" in order
    source = inspect.getsource(background_tasks.start_background_tasks)
    assert 'tasks.start(\n            "connector_service_identity_revoker"' in source
    assert (
        'start_leader_gated(\n            "connector_service_identity_revoker"'
        not in (source)
    )


@pytest.mark.asyncio
async def test_the_revoke_loop_revokes_at_once_then_on_its_interval(monkeypatch):
    calls: list[object] = []

    async def revoke(store):
        calls.append(store)
        if len(calls) == 2:
            raise RuntimeError("database away")  # logged; the loop goes on
        return []

    monkeypatch.setattr(hosting, "revoke_unhosted_identities", revoke)
    shutdown = asyncio.Event()
    store = object()
    task = asyncio.create_task(
        hosting.connector_service_identity_revoker(
            shutdown, store=store, interval_seconds=0.02
        )
    )
    for _ in range(100):
        if len(calls) >= 3:
            break
        await asyncio.sleep(0.01)
    shutdown.set()
    await asyncio.wait_for(task, timeout=2)
    assert len(calls) >= 3 and all(item is store for item in calls)


def test_the_reconciler_builder_creates_the_networking_api_once(monkeypatch):
    from orchestrator.services import agent_provisioner, container_provisioner

    monkeypatch.setattr(
        agent_provisioner.agent_provisioner, "_k8s_available", True, raising=False
    )
    core = SimpleNamespace(api_client=None)
    monkeypatch.setattr(
        container_provisioner.container_provisioner, "_core_api", core, raising=False
    )
    resources = SimpleNamespace(
        settings=_settings(), postgres_db=None, connector_drivers=None
    )
    settings = connectors_composition.service_hosting_settings(resources)
    build = connectors_composition.connector_service_reconciler_builder(
        resources, settings
    )
    first, second = build(), build()
    assert first is not second
    assert first.runtime.networking_api is second.runtime.networking_api
    assert first.runtime.core_api is core
    monkeypatch.setattr(
        agent_provisioner.agent_provisioner, "_k8s_available", False, raising=False
    )
    assert build() is None


def test_driver_images_resolve_at_public_addresses_unless_listed():
    from orchestrator.services.connector_drivers import builtin_connector_drivers

    resources = SimpleNamespace(
        settings=_settings(
            connector_driver_registry_insecure_hosts=frozenset({"srw-registry:5000"}),
            connector_driver_registry_private_hosts=frozenset({"srw-registry:5000"}),
            connector_service_cluster_cidrs=("10.96.0.0/12",),
        ),
        connector_drivers=builtin_connector_drivers(
            echo_service_image="srw-registry:5000/srw-driver-echo:dev"
        ),
        postgres_db=None,
    )
    resolver = connectors_composition.service_image_settings(resources).resolver
    assert resolver.checks_addresses is True
    assert resolver.private_hosts == {"srw-registry:5000"}
    assert [str(n) for n in resolver.refused_networks] == ["10.96.0.0/12"]


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
async def test_preparing_a_delivery_without_service_drivers_touches_nothing():
    from orchestrator.services import connector_credential_leases as leases

    class Untouchable:
        def acquire(self):
            raise AssertionError("no service entry: no database work")

    await leases.prepare_lease_delivery(
        Untouchable(),
        [{"type": "lease_probe", "datasource_id": IDENTITY.connector_id}, "junk"],
        owner=leases.LeaseOwner.thread(IDENTITY.identity_id),
    )


@pytest.mark.parametrize(
    ("module", "function", "delivery"),
    [
        (
            "job_start_bundle",
            "build_job_start_request",
            "deliver_connector_leases_with(",
        ),
        (
            "job_control_delivery",
            "resume_job_on_agent",
            "deliver_connector_leases_with(",
        ),
        ("unit_claim_bundle", "_assemble_claim_bundle", "_deliver_claim_leases("),
    ],
)
def test_every_dispatch_prepares_service_images_before_its_transaction(
    module, function, delivery
):
    """The registry lookup happens before the delivery's transaction opens:
    each delivery is preceded by a prepare, and in the claim, by a prepare
    before its ``conn.transaction()``."""
    import importlib
    import inspect

    source = inspect.getsource(
        getattr(importlib.import_module(f"orchestrator.services.{module}"), function)
    )
    prepares = [
        i for i in range(len(source)) if source.startswith("prepare_lease_delivery(", i)
    ]
    deliveries = [i for i in range(len(source)) if source.startswith(delivery, i)]
    assert prepares and len(prepares) == len(deliveries)
    for prepare, deliver in zip(prepares, deliveries):
        assert prepare < deliver
        if module == "unit_claim_bundle":
            assert prepare < source.index("conn.transaction()", prepare) < deliver


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
