"""The bind-time launch builder (connector drivers D6).

One operation pod is a Secret, a NetworkPolicy and a Pod, all named after its
``connector_driver_operations`` row. The pod is hardened like every driver
pod, never restarts, carries a deadline, receives no ingress and reaches only
the result route and its pinned hosts.
"""

from __future__ import annotations

import base64
import json
from datetime import datetime, timezone

import pytest

from orchestrator.services.connector_bind_time_launch import (
    MANAGER,
    RESULT_PATH,
    BindTimePod,
    build_bind_time_launch,
)
from orchestrator.services.connector_egress import EgressPins, PinnedHost
from orchestrator.services.connector_service_launch import (
    ServiceLaunchError,
    ServiceLaunchPolicy,
)
from shared.connectors.envelope import DriverRequest

DIGEST = "sha256:" + "ab" * 32
OPERATION = "11111111-2222-4333-8444-555555555555"
CONNECTOR = "66666666-7777-4888-8999-aaaaaaaaaaaa"
POD = "srw-bnd-11111111222243338444555555555555"
TOKEN = "sdi_" + "A" * 49
NOW = datetime(2026, 10, 8, 12, 0, tzinfo=timezone.utc)
ORCHESTRATOR_LABELS = {"app.kubernetes.io/component": "orchestrator"}
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
    hosts=(PinnedHost(host="api.example", addresses=("1.1.1.1",), ports=(443,)),),
    resolved_at=NOW,
)
REQUEST = DriverRequest(
    operation="bind",
    config={"variable": "EXAMPLE_TOKEN"},
    access="ReadWrite",
    credentials={"token": "upstream-secret"},
    binding_id="b-1",
)


def _pod(**over) -> BindTimePod:
    values = {
        "operation_id": OPERATION,
        "operation": "bind",
        "driver": "example.env/v1",
        "digest": DIGEST,
        "connector_id": CONNECTOR,
    }
    values.update(over)
    return BindTimePod(**values)


def _plan(**over):
    values = {
        "request": REQUEST.to_json(),
        "image": f"srw-registry:5000/example@{DIGEST}",
        "entrypoint": ["python3", "/driver.py"],
        "cmd": [],
        "identity_token": TOKEN,
        "pins": PINS,
        "policy": POLICY,
        "deadline_seconds": 120,
    }
    values.update(over)
    return build_bind_time_launch(_pod(), **values)


class TestTheOperationPod:
    def test_names_and_labels_follow_the_operation_row(self):
        plan = _plan()
        for obj in (plan.pod, plan.secret, plan.network_policy):
            assert obj["metadata"]["name"] == POD
            assert obj["metadata"]["namespace"] == "srw-connectors"
            labels = obj["metadata"]["labels"]
            # Never the service hosting's manager: its sweep would delete it.
            assert labels["srw/managed-by"] == MANAGER == "connector-bind-time"
            assert labels["srw.io/plane"] == "bind_time"
            assert labels["srw.io/driver-operation"] == OPERATION
            assert labels["srw.io/connector-id"] == CONNECTOR
            assert labels["srw.io/operation"] == "bind"

    def test_it_runs_once_unprivileged_with_a_deadline(self):
        spec = _plan().pod["spec"]
        assert spec["restartPolicy"] == "Never"
        assert spec["activeDeadlineSeconds"] == 120
        assert spec["automountServiceAccountToken"] is False
        assert spec["serviceAccountName"] == "srw-connector-driver"
        assert spec["enableServiceLinks"] is False
        assert not spec["hostNetwork"] and not spec["hostPID"] and not spec["hostIPC"]
        assert spec["securityContext"] == {"seccompProfile": {"type": "RuntimeDefault"}}
        (driver,) = spec["containers"]
        assert driver["securityContext"] == {
            "allowPrivilegeEscalation": False,
            "privileged": False,
            "capabilities": {"drop": ["ALL"]},
        }
        assert "ports" not in driver
        for container in spec["initContainers"]:
            assert container["securityContext"]["runAsNonRoot"] is True
            assert container["securityContext"]["capabilities"] == {"drop": ["ALL"]}

    def test_the_shim_runs_the_image_by_digest_and_posts_its_result(self):
        spec = _plan().pod["spec"]
        assert [c["name"] for c in spec["initContainers"]] == [
            "canary-wait",
            "install-shim",
        ]
        (driver,) = spec["containers"]
        assert driver["image"] == f"srw-registry:5000/example@{DIGEST}"
        assert driver["command"] == ["/srw/bin/srw-driver-shim", "run", "--"]
        assert driver["args"] == ["python3", "/driver.py"]
        env = {item["name"]: item["value"] for item in driver["env"]}
        assert env == {
            "SRW_REQUEST_FILE": "/run/srw/request.json",
            "SRW_DRIVER_IDENTITY_FILE": "/run/srw/identity",
            "SRW_RESULT_URL": f"http://srw-orchestrator.srw.svc:8088{RESULT_PATH}",
        }
        # No envFrom: never the app Secret, only the pod's own.
        assert "envFrom" not in driver
        assert {"ip": "10.43.0.20", "hostnames": ["srw-orchestrator.srw.svc"]} in spec[
            "hostAliases"
        ]
        assert {"ip": "1.1.1.1", "hostnames": ["api.example"]} in spec["hostAliases"]
        assert spec["dnsPolicy"] == "None"

    def test_the_secret_holds_only_the_request_and_the_identity(self):
        secret = _plan().secret
        assert secret["immutable"] is True
        assert set(secret["data"]) == {"request.json", "identity"}
        request = json.loads(base64.b64decode(secret["data"]["request.json"]))
        assert request["operation"] == "bind"
        assert request["credentials"] == {"token": "upstream-secret"}
        assert base64.b64decode(secret["data"]["identity"]).decode() == TOKEN

    def test_no_ingress_and_egress_to_the_result_port_and_pinned_hosts_only(self):
        policy = _plan().network_policy["spec"]
        assert policy["podSelector"] == {
            "matchLabels": {"srw.io/driver-operation": OPERATION}
        }
        assert policy["policyTypes"] == ["Ingress", "Egress"]
        assert policy["ingress"] == []
        exchange, upstream = policy["egress"]
        assert exchange["ports"] == [{"protocol": "TCP", "port": 8088}]
        assert upstream == {
            "to": [{"ipBlock": {"cidr": "1.1.1.1/32"}}],
            "ports": [{"protocol": "TCP", "port": 443}],
        }

    def test_a_spec_operation_reaches_the_result_port_only(self):
        plan = build_bind_time_launch(
            _pod(operation="spec", connector_id=None),
            request=DriverRequest(operation="spec").to_json(),
            image=f"srw-registry:5000/example@{DIGEST}",
            entrypoint=["/driver"],
            cmd=[],
            identity_token=TOKEN,
            pins=EgressPins(hosts=(), resolved_at=NOW),
            policy=POLICY,
            deadline_seconds=60,
        )
        assert len(plan.network_policy["spec"]["egress"]) == 1
        assert "srw.io/connector-id" not in plan.pod["metadata"]["labels"]


class TestRefusals:
    def test_an_image_must_be_pinned_by_its_digest(self):
        with pytest.raises(ServiceLaunchError, match="digest"):
            _plan(image="srw-registry:5000/example:latest")

    def test_an_image_without_a_program_is_refused(self):
        with pytest.raises(ServiceLaunchError, match="entrypoint"):
            _plan(entrypoint=[], cmd=[])

    def test_an_unknown_operation_is_refused(self):
        with pytest.raises(ValueError, match="operation"):
            _pod(operation="discover")

    def test_a_request_over_512_kib_is_refused(self):
        big = DriverRequest(
            operation="bind", config={"x": "y" * 600_000}, binding_id="b"
        )
        with pytest.raises(ServiceLaunchError, match="512 KiB"):
            _plan(request=big.to_json())
