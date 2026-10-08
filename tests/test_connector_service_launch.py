"""The service-plane launch builder (connector drivers D5 item 2).

One service pod is a Secret, a NetworkPolicy, a Service and a Pod, all named
after its ``sdi_`` identity. The whole manifests are pinned here.
"""

from __future__ import annotations

import base64
import json
from dataclasses import replace
from datetime import datetime, timezone

import pytest

from orchestrator.services.connector_egress import EgressPins, PinnedHost
from orchestrator.services.connector_service_launch import (
    ServiceLaunchError,
    ServiceLaunchPolicy,
    ServicePodIdentity,
    binding_ingress_policy,
    build_service_launch,
    service_resources,
)
from shared.connectors.contract import (
    AccessLevel,
    CredentialSlot,
    DriverSpec,
    EgressRule,
    ServiceSpec,
    validate_spec,
)

DIGEST = "sha256:" + "ab" * 32
IDENTITY = "11111111-2222-4333-8444-555555555555"
CONNECTOR = "66666666-7777-4888-8999-aaaaaaaaaaaa"
GENERATION = "hmac-sha256:" + "cd" * 32
POD = "srw-drv-11111111222243338444555555555555"
TOKEN = "sdi_" + "A" * 49
NOW = datetime(2026, 10, 8, 12, 0, tzinfo=timezone.utc)
ORCHESTRATOR_LABELS = {
    "app.kubernetes.io/name": "superhuman-remote-worker",
    "app.kubernetes.io/instance": "srw",
    "app.kubernetes.io/component": "orchestrator",
}

SPEC = DriverSpec(
    name="srw.echo-service/v1",
    title="Echo",
    plane="service",
    delivery_forms=("lease_token",),
    config_schema={"type": "object"},
    credential_slots=(
        CredentialSlot("secret", "secret_string", {"type": "object"}, required=True),
    ),
    access_levels=(AccessLevel("ReadWrite", 1, "the lease exchange"),),
    supported_backends=frozenset({"sandbox"}),
    workspace_requirements="none",
    egress=(EgressRule("${config.host}", ("${config.port}",)),),
    credential_delivery="lease",
    service=ServiceSpec(
        port=8080,
        callers=("harness", "workspace"),
        resources={"limits": {"memory": "128Mi"}},
    ),
)


def _identity(**over) -> ServicePodIdentity:
    values = {
        "identity_id": IDENTITY,
        "connector_id": CONNECTOR,
        "driver": SPEC.name,
        "digest": DIGEST,
        "generation": GENERATION,
    }
    values.update(over)
    return ServicePodIdentity(**values)


POLICY = ServiceLaunchPolicy(
    namespace="srw-connectors",
    release_namespace="srw",
    shim_image="ghcr.io/superhuman-remote-worker/srw-driver-shim@sha256:" + "ef" * 32,
    exchange_host="srw-orchestrator.srw.svc",
    exchange_address="10.43.0.20",
    exchange_port=8088,
    orchestrator_labels=ORCHESTRATOR_LABELS,
)

PINS = EgressPins(
    hosts=(
        PinnedHost(
            host="one.one.one.one",
            addresses=("1.0.0.1", "1.1.1.1"),
            ports=(443,),
        ),
    ),
    resolved_at=NOW,
)


def _plan(**over):
    values = {
        "spec": SPEC,
        "image": f"srw-registry:5000/srw-driver-echo@{DIGEST}",
        "entrypoint": ["/srw-driver-echo"],
        "cmd": ["--listen", ":8080"],
        "config": {"host": "one.one.one.one", "port": 443},
        "credentials": {"secret": "upstream-secret"},
        "identity_token": TOKEN,
        "pins": PINS,
        "policy": POLICY,
    }
    values.update(over)
    return build_service_launch(_identity(), **values)


LABELS = {
    "srw/managed-by": "connector-service-hosting",
    "srw.io/plane": "service",
    "srw.io/connector-id": CONNECTOR,
    "srw.io/driver-identity": IDENTITY,
    "srw.io/driver": "srw.echo-service_v1",
    "srw.io/image-digest": "abababababab",
    "srw.io/credential-generation": "cdcdcdcdcdcd",
}
METADATA = {"name": POD, "namespace": "srw-connectors", "labels": LABELS}
RELEASE = {"matchLabels": {"kubernetes.io/metadata.name": "srw"}}
ORCHESTRATOR_PEER = {
    "namespaceSelector": RELEASE,
    "podSelector": {"matchLabels": ORCHESTRATOR_LABELS},
}
SHIM_SECURITY = {
    "allowPrivilegeEscalation": False,
    "privileged": False,
    "capabilities": {"drop": ["ALL"]},
    "readOnlyRootFilesystem": True,
    "runAsNonRoot": True,
    "runAsUser": 65532,
    "runAsGroup": 65532,
}
SHIM_RESOURCES = {
    "requests": {"cpu": "10m", "memory": "16Mi", "ephemeral-storage": "16Mi"},
    "limits": {"cpu": "100m", "memory": "32Mi", "ephemeral-storage": "32Mi"},
}


def test_the_spec_with_callers_and_a_port_is_valid():
    assert validate_spec(SPEC) == []
    for service in (
        ServiceSpec(port=0),
        ServiceSpec(callers=()),
        ServiceSpec(callers=("browser",)),  # type: ignore[arg-type]
    ):
        assert validate_spec(replace(SPEC, service=service))


def test_the_identity_names_and_labels_every_object():
    identity = _identity()
    assert identity.pod_name == POD
    assert len(identity.pod_name) <= 63
    assert identity.labels == LABELS
    assert all(len(value) <= 63 for value in identity.labels.values())
    # No SRW agent or chart label: those open internal services.
    assert not {"app", "app.kubernetes.io/name", "srw.io/component"} & set(LABELS)


def test_the_pod_manifest():
    plan = _plan()
    assert plan.pod == {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": METADATA,
        "spec": {
            "restartPolicy": "Always",
            "serviceAccountName": "srw-connector-driver",
            "automountServiceAccountToken": False,
            "enableServiceLinks": False,
            "hostNetwork": False,
            "hostPID": False,
            "hostIPC": False,
            "shareProcessNamespace": False,
            "securityContext": {"seccompProfile": {"type": "RuntimeDefault"}},
            "terminationGracePeriodSeconds": 30,
            "hostAliases": [
                {"ip": "1.0.0.1", "hostnames": ["one.one.one.one"]},
                {"ip": "1.1.1.1", "hostnames": ["one.one.one.one"]},
                {"ip": "10.43.0.20", "hostnames": ["srw-orchestrator.srw.svc"]},
            ],
            "initContainers": [
                {
                    "name": "canary-wait",
                    "image": POLICY.shim_image,
                    "imagePullPolicy": "IfNotPresent",
                    "command": ["/srw-driver-shim"],
                    "args": [
                        "canary-wait",
                        "--deny",
                        "10.43.0.20:8085",
                        "--allow",
                        "10.43.0.20:8088",
                        "--expect",
                        "1.0.0.1:443",
                        "--consecutive",
                        "3",
                        "--timeout",
                        "120s",
                    ],
                    "resources": SHIM_RESOURCES,
                    "securityContext": SHIM_SECURITY,
                    "terminationMessagePolicy": "FallbackToLogsOnError",
                },
                {
                    "name": "install-shim",
                    "image": POLICY.shim_image,
                    "imagePullPolicy": "IfNotPresent",
                    "command": ["/srw-driver-shim"],
                    "args": ["install", "/srw/bin"],
                    "resources": SHIM_RESOURCES,
                    "securityContext": SHIM_SECURITY,
                    "terminationMessagePolicy": "FallbackToLogsOnError",
                    "volumeMounts": [{"name": "srw-bin", "mountPath": "/srw/bin"}],
                },
            ],
            "containers": [
                {
                    "name": "driver",
                    "image": f"srw-registry:5000/srw-driver-echo@{DIGEST}",
                    "imagePullPolicy": "IfNotPresent",
                    "command": ["/srw/bin/srw-driver-shim", "serve", "--"],
                    "args": ["/srw-driver-echo", "--listen", ":8080"],
                    "ports": [
                        {"name": "srw-driver", "containerPort": 8080, "protocol": "TCP"}
                    ],
                    "env": [
                        {"name": "SRW_REQUEST_FILE", "value": "/run/srw/request.json"},
                        {
                            "name": "SRW_DRIVER_IDENTITY_FILE",
                            "value": "/run/srw/identity",
                        },
                        {
                            "name": "SRW_EXCHANGE_URL",
                            "value": "http://srw-orchestrator.srw.svc:8088",
                        },
                        {"name": "SRW_DRIVER_PORT", "value": "8080"},
                    ],
                    "resources": {
                        "requests": {
                            "cpu": "50m",
                            "memory": "64Mi",
                            "ephemeral-storage": "64Mi",
                        },
                        "limits": {
                            "cpu": "500m",
                            "memory": "128Mi",
                            "ephemeral-storage": "1Gi",
                        },
                    },
                    "readinessProbe": {
                        "tcpSocket": {"port": "srw-driver"},
                        "periodSeconds": 5,
                        "failureThreshold": 3,
                    },
                    "securityContext": {
                        "allowPrivilegeEscalation": False,
                        "privileged": False,
                        "capabilities": {"drop": ["ALL"]},
                    },
                    "volumeMounts": [
                        {"name": "srw-bin", "mountPath": "/srw/bin", "readOnly": True},
                        {
                            "name": "delivery",
                            "mountPath": "/run/srw/request.json",
                            "subPath": "request.json",
                            "readOnly": True,
                        },
                        {
                            "name": "delivery",
                            "mountPath": "/run/srw/identity",
                            "subPath": "identity",
                            "readOnly": True,
                        },
                    ],
                }
            ],
            "volumes": [
                {"name": "srw-bin", "emptyDir": {"sizeLimit": "32Mi"}},
                {
                    "name": "delivery",
                    "secret": {"secretName": POD, "defaultMode": 0o444},
                },
            ],
            "dnsPolicy": "None",
            "dnsConfig": {"nameservers": ["127.0.0.1"]},
        },
    }


def test_the_secret_manifest_holds_the_request_and_the_identity():
    plan = _plan()
    assert plan.secret["metadata"] == METADATA
    assert plan.secret["immutable"] is True
    assert plan.secret["type"] == "Opaque"
    assert set(plan.secret["data"]) == {"request.json", "identity"}
    assert base64.b64decode(plan.secret["data"]["identity"]).decode() == TOKEN
    request = json.loads(base64.b64decode(plan.secret["data"]["request.json"]))
    assert request == {
        "protocol_version": "1.0",
        "plane": "service",
        "driver": "srw.echo-service/v1",
        "connector": {
            "id": CONNECTOR,
            "config": {"host": "one.one.one.one", "port": 443},
        },
        # A lease driver exchanges each binding's lease: no credential here.
        "credentials": {},
        "service": {"port": 8080, "port_name": "srw-driver"},
        "exchange": {
            "url": "http://srw-orchestrator.srw.svc:8088",
            "identity_file": "/run/srw/identity",
        },
        "egress": PINS.record(),
    }
    # Only the pod's own Secret holds the token: no env literal, no label.
    assert TOKEN not in json.dumps(plan.pod)
    assert TOKEN not in json.dumps(plan.service)
    assert TOKEN not in json.dumps(plan.network_policy)


def test_a_driver_holding_its_credential_gets_it_in_its_request():
    plan = _plan(
        spec=replace(SPEC, credential_delivery="inline", delivery_forms=("mcp_client",))
    )
    request = json.loads(base64.b64decode(plan.secret["data"]["request.json"]))
    assert request["credentials"] == {"secret": "upstream-secret"}


def test_the_network_policy_manifest():
    plan = _plan()
    assert plan.network_policy == {
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": METADATA,
        "spec": {
            "podSelector": {"matchLabels": {"srw.io/driver-identity": IDENTITY}},
            "policyTypes": ["Ingress", "Egress"],
            "ingress": [
                {
                    "from": [
                        {
                            "namespaceSelector": RELEASE,
                            "podSelector": {
                                "matchExpressions": [
                                    {
                                        "key": "app",
                                        "operator": "In",
                                        "values": [
                                            "srw-agent",
                                            "srw-persistent-agent",
                                            "srw-agent-stateless",
                                        ],
                                    }
                                ]
                            },
                        },
                        ORCHESTRATOR_PEER,
                    ],
                    "ports": [{"protocol": "TCP", "port": "srw-driver"}],
                }
            ],
            "egress": [
                {
                    "to": [ORCHESTRATOR_PEER],
                    "ports": [{"protocol": "TCP", "port": 8088}],
                },
                {
                    "to": [
                        {"ipBlock": {"cidr": "1.0.0.1/32"}},
                        {"ipBlock": {"cidr": "1.1.1.1/32"}},
                    ],
                    "ports": [{"protocol": "TCP", "port": 443}],
                },
            ],
        },
    }


def test_a_workspace_only_driver_admits_no_agent_pod():
    plan = _plan(
        spec=replace(SPEC, service=replace(SPEC.service, callers=("workspace",)))
    )
    assert plan.network_policy["spec"]["ingress"] == []


def test_the_service_manifest():
    assert _plan().service == {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": METADATA,
        "spec": {
            "type": "ClusterIP",
            "selector": {"srw.io/driver-identity": IDENTITY},
            "ports": [
                {
                    "name": "srw-driver",
                    "protocol": "TCP",
                    "port": 8080,
                    "targetPort": "srw-driver",
                }
            ],
        },
    }


def test_a_declared_dns_need_keeps_the_cluster_resolver():
    plan = _plan(pins=replace(PINS, dns=True, dns_reason="SRV records"))
    assert "dnsPolicy" not in plan.pod["spec"]
    rules = plan.network_policy["spec"]["egress"]
    assert rules[-1]["ports"] == [
        {"protocol": "UDP", "port": 53},
        {"protocol": "TCP", "port": 53},
    ]


def test_a_dollar_in_the_image_command_is_never_expanded():
    """Kubernetes expands $(VAR) in args; an image's own command is literal."""
    plan = build_service_launch(
        _identity(),
        spec=SPEC,
        image=f"ghcr.io/org/echo@{DIGEST}",
        entrypoint=("/bin/echo",),
        cmd=("$(SRW_DRIVER_PORT)", "cost: $5"),
        config={"host": "one.one.one.one", "port": 443},
        credentials=None,
        identity_token=TOKEN,
        pins=PINS,
        policy=POLICY,
    )
    driver = plan.pod["spec"]["containers"][0]
    assert driver["args"] == ["/bin/echo", "$$(SRW_DRIVER_PORT)", "cost: $$5"]
    assert "runtimeClassName" not in plan.pod["spec"]


def test_the_vm_peer_matches_the_launcher_labels_the_chart_stamps():
    """The VM peer selects exactly the labels the VM controller's VMI
    template puts on every virt-launcher pod."""
    import pathlib

    root = pathlib.Path(__file__).resolve().parents[1]
    configmap = (root / "helm/templates/vm-controller/configmap.yaml").read_text()
    for line in (
        "srw.io/component: agent-workspace",
        'srw.io/owner-kind: "${OWNER_KIND}"',
        'srw.io/owner-id: "${OWNER_ID}"',
    ):
        assert line in configmap
    owner = "77777777-8888-4999-8aaa-bbbbbbbbbbbb"
    peer = binding_ingress_policy(
        _identity(), kind="job", owner_id=owner, policy=POLICY
    )["spec"]["ingress"][0]["from"][1]
    assert peer["podSelector"]["matchLabels"] == {
        "srw.io/component": "agent-workspace",
        "srw.io/owner-kind": "job",
        "srw.io/owner-id": owner,
    }


def test_the_binding_ingress_policy_admits_one_workspace():
    owner = "77777777-8888-4999-8aaa-bbbbbbbbbbbb"
    policy = binding_ingress_policy(
        _identity(), kind="thread", owner_id=owner, policy=POLICY
    )
    assert policy == {
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": {
            "name": f"{POD}-t777777778888",
            "namespace": "srw-connectors",
            "labels": {
                **LABELS,
                "srw.io/binding-owner-kind": "thread",
                "srw.io/binding-owner": owner,
            },
        },
        "spec": {
            "podSelector": {"matchLabels": {"srw.io/driver-identity": IDENTITY}},
            "policyTypes": ["Ingress"],
            "ingress": [
                {
                    "from": [
                        {
                            "namespaceSelector": RELEASE,
                            "podSelector": {
                                "matchLabels": {
                                    "srw.io/component": "agent-workspace",
                                    "srw/thread-id": owner,
                                }
                            },
                        },
                        # A same-cluster VM workspace's virt-launcher pod.
                        {
                            "namespaceSelector": RELEASE,
                            "podSelector": {
                                "matchLabels": {
                                    "srw.io/component": "agent-workspace",
                                    "srw.io/owner-kind": "thread",
                                    "srw.io/owner-id": owner,
                                }
                            },
                        },
                    ],
                    "ports": [{"protocol": "TCP", "port": "srw-driver"}],
                }
            ],
        },
    }
    job = binding_ingress_policy(_identity(), kind="job", owner_id=owner, policy=POLICY)
    assert job["metadata"]["name"].endswith("-j777777778888")
    assert job["spec"]["ingress"][0]["from"][0]["podSelector"]["matchLabels"] == {
        "srw.io/component": "agent-workspace",
        "srw/job-id": owner,
    }
    assert job["spec"]["ingress"][0]["from"][1]["podSelector"]["matchLabels"] == {
        "srw.io/component": "agent-workspace",
        "srw.io/owner-kind": "job",
        "srw.io/owner-id": owner,
    }
    with pytest.raises(ValueError):
        binding_ingress_policy(_identity(), kind="pod", owner_id=owner, policy=POLICY)


class TestResources:
    def test_defaults_fill_and_requests_never_exceed_limits(self):
        assert service_resources({}, POLICY) == {
            "requests": {"cpu": "50m", "memory": "64Mi", "ephemeral-storage": "64Mi"},
            "limits": {"cpu": "500m", "memory": "256Mi", "ephemeral-storage": "1Gi"},
        }
        lowered = service_resources({"limits": {"cpu": "20m"}}, POLICY)
        assert lowered["requests"]["cpu"] == "20m"

    def test_the_ceiling_refuses(self):
        with pytest.raises(ServiceLaunchError, match="ceiling"):
            service_resources({"limits": {"memory": "8Gi"}}, POLICY)
        with pytest.raises(ServiceLaunchError, match="unsupported"):
            service_resources({"limits": {"nvidia.com/gpu": "1"}}, POLICY)
        with pytest.raises(ServiceLaunchError, match="invalid"):
            service_resources({"limits": {"cpu": "lots"}}, POLICY)


class TestRefusals:
    def test_only_a_service_driver_is_hosted(self):
        with pytest.raises(ServiceLaunchError, match="not a service-plane driver"):
            _plan(spec=replace(SPEC, plane="bind_time", service=None))

    def test_the_image_is_launched_by_its_digest(self):
        for image in (
            "srw-registry:5000/srw-driver-echo:latest",
            "srw-registry:5000/srw-driver-echo@sha256:" + "00" * 32,
        ):
            with pytest.raises(ServiceLaunchError, match="digest"):
                _plan(image=image)

    def test_an_image_without_a_program_is_refused(self):
        with pytest.raises(ServiceLaunchError, match="entrypoint"):
            _plan(entrypoint=[], cmd=[])

    def test_an_oversized_request_is_refused(self):
        with pytest.raises(ServiceLaunchError, match="512 KiB"):
            _plan(config={"blob": "x" * (600 * 1024)})

    def test_another_drivers_identity_is_refused(self):
        with pytest.raises(ServiceLaunchError, match="another driver"):
            build_service_launch(
                _identity(driver="srw.other/v1"),
                spec=SPEC,
                image=f"r/x@{DIGEST}",
                entrypoint=["/x"],
                cmd=[],
                config={},
                credentials=None,
                identity_token=TOKEN,
                pins=PINS,
                policy=POLICY,
            )

    def test_the_policy_refuses_the_release_namespace(self):
        with pytest.raises(ValueError, match="release namespace"):
            replace(POLICY, namespace="srw")
        with pytest.raises(ValueError, match="labels"):
            replace(POLICY, orchestrator_labels={})

    def test_a_malformed_identity_is_refused(self):
        with pytest.raises(ValueError):
            _identity(digest="latest")
        with pytest.raises(ValueError):
            _identity(generation="")
