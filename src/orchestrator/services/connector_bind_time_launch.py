"""Bind-time driver pods: the launch builder (connector drivers D6).

One short-lived pod per driver operation (``spec``, ``check``, ``bind``,
``revoke``, ``gc``) of a registered image. It runs once and exits; it is
never restarted and never reached from outside. Three objects, named after
the operation's ``connector_driver_operations`` row:

* an immutable **Secret** with ``/run/srw/request.json`` (the operation's
  request: the connector's config, access and credentials, the binding id and
  stored ``driver_state``; nothing for ``spec``) and ``/run/srw/identity``
  (the pod's ``sdi_`` token); 512 KiB at most, owned by the pod once it
  exists so Kubernetes garbage collection removes it with the pod;
* a **NetworkPolicy** selecting the pod: no ingress at all; egress to the
  lease exchange's port (where the result route is) and to the hosts the
  driver declares, pinned per pod as service pods are; DNS only when
  declared. A ``spec`` operation gets the exchange port only;
* the **Pod**: ``restartPolicy: Never`` with ``activeDeadlineSeconds`` (so the
  namespace's Terminating quota counts it apart from service pods), the image
  pinned by digest, the same hardening as every driver pod (no ServiceAccount
  token, a ServiceAccount with no bindings, no service links, no host
  namespaces, seccomp ``RuntimeDefault``, every capability dropped, no
  privilege escalation, the image's own user, Pod Security ``baseline`` on the
  namespace) and the same two SRW init containers: the canary wait and the
  shim install. The shim runs the driver (``srw-driver-shim run``): it reads
  the driver's typed JSON lines from stdout and posts them with the exit code
  to the result route, authenticated by the pod's identity. The pod never
  touches a workspace: what a bind returns is data SRW delivers.

A custom image never runs privileged here, trusted or not: the bind-time
plane has no privileged mode, and the namespace refuses one.

Labels name the operation and its manager (``connector-bind-time``), never
the service hosting's, whose sweep deletes what it does not know.

Design: knowledge-base/knowledge/features/connector_drivers.md, "Three
planes" (bind-time), "The driver namespace baseline".
"""

from __future__ import annotations

import base64
import re
from collections.abc import Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

from orchestrator.services.connector_egress import EgressPins
from orchestrator.services.connector_service_launch import (
    IDENTITY_PATH,
    REQUEST_PATH,
    SHIM_DIR,
    SHIM_PATH,
    ServiceLaunchError,
    ServiceLaunchPolicy,
    _canary_args,
    _json_bytes,
    _orchestrator_peer,
    _shim_security,
    _SHIM_RESOURCES,
    label_value,
    service_resources,
)

MANAGER = "connector-bind-time"
#: The result route on the lease exchange's dedicated port.
RESULT_PATH = "/v1/drivers/result"
OPERATIONS: tuple[str, ...] = ("spec", "check", "bind", "revoke", "gc")
_MAX_DELIVERY_BYTES = 512 * 1024
_DIGEST_REFERENCE = re.compile(r"[^\s@]+@sha256:[0-9a-f]{64}\Z")
#: A bind-time pod has nothing to finish once it is told to stop.
TERMINATION_GRACE_SECONDS = 5


@dataclass(frozen=True)
class BindTimePod:
    """One operation's pod: its ``connector_driver_operations`` row."""

    operation_id: str
    operation: str
    driver: str
    digest: str
    connector_id: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "operation_id", str(UUID(str(self.operation_id))))
        if self.operation not in OPERATIONS:
            raise ValueError(f"unknown driver operation {self.operation!r}")
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", self.digest):
            raise ValueError("a driver pod runs one sha256 image digest")
        if self.connector_id is not None:
            object.__setattr__(self, "connector_id", str(UUID(str(self.connector_id))))

    @property
    def pod_name(self) -> str:
        return f"srw-bnd-{UUID(self.operation_id).hex}"

    @property
    def selector(self) -> dict[str, str]:
        return {"srw.io/driver-operation": self.operation_id}

    @property
    def labels(self) -> dict[str, str]:
        labels = {
            "srw/managed-by": MANAGER,
            "srw.io/plane": "bind_time",
            "srw.io/driver-operation": self.operation_id,
            "srw.io/operation": self.operation,
            "srw.io/driver": label_value(self.driver),
            "srw.io/image-digest": self.digest.removeprefix("sha256:")[:12],
        }
        if self.connector_id is not None:
            labels["srw.io/connector-id"] = self.connector_id
        return labels


@dataclass(frozen=True)
class BindTimeLaunchPlan:
    pod_identity: BindTimePod
    namespace: str
    pod: dict = field(repr=False)
    network_policy: dict = field(repr=False)
    secret: dict = field(repr=False)


def result_url(policy: ServiceLaunchPolicy) -> str:
    return f"http://{policy.exchange_host}:{policy.exchange_port}{RESULT_PATH}"


def build_bind_time_launch(
    pod_identity: BindTimePod,
    *,
    request: Mapping[str, Any],
    image: str,
    entrypoint: Sequence[str],
    cmd: Sequence[str],
    identity_token: str,
    pins: EgressPins,
    policy: ServiceLaunchPolicy,
    deadline_seconds: int,
) -> BindTimeLaunchPlan:
    """The Secret, NetworkPolicy and Pod of one bind-time operation."""
    if not _DIGEST_REFERENCE.fullmatch(image) or not image.endswith(
        pod_identity.digest
    ):
        raise ServiceLaunchError("a driver pod launches its image by its digest")
    if not policy.shim_image or any(ch.isspace() for ch in policy.shim_image):
        raise ServiceLaunchError("no driver shim image is configured")
    program = [*entrypoint, *cmd]
    if not program:
        raise ServiceLaunchError(
            "the driver image declares no entrypoint or command for the shim to run"
        )
    if deadline_seconds < 1:
        raise ServiceLaunchError("a bind-time pod needs a deadline")
    name = pod_identity.pod_name
    metadata = {
        "name": name,
        "namespace": policy.namespace,
        "labels": pod_identity.labels,
    }
    delivery = {
        "request.json": _json_bytes(dict(request)),
        "identity": identity_token.encode("ascii"),
    }
    if sum(len(value) for value in delivery.values()) > _MAX_DELIVERY_BYTES:
        raise ServiceLaunchError("the driver pod's request exceeds 512 KiB")
    secret = {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": deepcopy(metadata),
        "type": "Opaque",
        "immutable": True,
        "data": {
            key: base64.b64encode(value).decode("ascii")
            for key, value in delivery.items()
        },
    }
    network_policy = {
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": deepcopy(metadata),
        "spec": {
            "podSelector": {"matchLabels": pod_identity.selector},
            "policyTypes": ["Ingress", "Egress"],
            # Nothing reaches a bind-time pod.
            "ingress": [],
            "egress": [
                {
                    "to": [_orchestrator_peer(policy)],
                    "ports": [{"protocol": "TCP", "port": policy.exchange_port}],
                },
                *pins.network_policy_egress(),
            ],
        },
    }
    canary = {
        "name": "canary-wait",
        "image": policy.shim_image,
        "imagePullPolicy": policy.shim_pull_policy,
        "command": ["/srw-driver-shim"],
        "args": _canary_args(pins, policy),
        "resources": deepcopy(_SHIM_RESOURCES),
        "securityContext": _shim_security(),
        "terminationMessagePolicy": "FallbackToLogsOnError",
    }
    install = {
        "name": "install-shim",
        "image": policy.shim_image,
        "imagePullPolicy": policy.shim_pull_policy,
        "command": ["/srw-driver-shim"],
        "args": ["install", SHIM_DIR],
        "resources": deepcopy(_SHIM_RESOURCES),
        "securityContext": _shim_security(),
        "terminationMessagePolicy": "FallbackToLogsOnError",
        "volumeMounts": [{"name": "srw-bin", "mountPath": SHIM_DIR}],
    }
    driver = {
        "name": "driver",
        "image": image,
        "imagePullPolicy": "IfNotPresent",
        "command": [SHIM_PATH, "run", "--"],
        # Kubernetes expands $(VAR) in args; $$ is a literal $.
        "args": [part.replace("$", "$$") for part in program],
        "env": [
            {"name": "SRW_REQUEST_FILE", "value": REQUEST_PATH},
            {"name": "SRW_DRIVER_IDENTITY_FILE", "value": IDENTITY_PATH},
            {"name": "SRW_RESULT_URL", "value": result_url(policy)},
        ],
        "resources": service_resources({}, policy),
        "securityContext": {
            "allowPrivilegeEscalation": False,
            "privileged": False,
            "capabilities": {"drop": ["ALL"]},
        },
        "terminationMessagePolicy": "FallbackToLogsOnError",
        "volumeMounts": [
            {"name": "srw-bin", "mountPath": SHIM_DIR, "readOnly": True},
            {
                "name": "delivery",
                "mountPath": REQUEST_PATH,
                "subPath": "request.json",
                "readOnly": True,
            },
            {
                "name": "delivery",
                "mountPath": IDENTITY_PATH,
                "subPath": "identity",
                "readOnly": True,
            },
        ],
    }
    host_aliases = pins.host_aliases()
    host_aliases.append(
        {"ip": policy.exchange_address, "hostnames": [policy.exchange_host]}
    )
    pod_spec: dict[str, Any] = {
        "restartPolicy": "Never",
        "activeDeadlineSeconds": int(deadline_seconds),
        "serviceAccountName": policy.service_account,
        "automountServiceAccountToken": False,
        "enableServiceLinks": False,
        "hostNetwork": False,
        "hostPID": False,
        "hostIPC": False,
        "shareProcessNamespace": False,
        "securityContext": {"seccompProfile": {"type": "RuntimeDefault"}},
        "terminationGracePeriodSeconds": TERMINATION_GRACE_SECONDS,
        "hostAliases": host_aliases,
        "initContainers": [canary, install],
        "containers": [driver],
        "volumes": [
            {"name": "srw-bin", "emptyDir": {"sizeLimit": "32Mi"}},
            {"name": "delivery", "secret": {"secretName": name, "defaultMode": 0o444}},
        ],
    }
    if not pins.dns:
        pod_spec["dnsPolicy"] = "None"
        pod_spec["dnsConfig"] = {"nameservers": ["127.0.0.1"]}
    pod = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": deepcopy(metadata),
        "spec": pod_spec,
    }
    return BindTimeLaunchPlan(
        pod_identity=pod_identity,
        namespace=policy.namespace,
        pod=pod,
        network_policy=network_policy,
        secret=secret,
    )


__all__ = [
    "MANAGER",
    "OPERATIONS",
    "RESULT_PATH",
    "BindTimeLaunchPlan",
    "BindTimePod",
    "build_bind_time_launch",
    "result_url",
]
