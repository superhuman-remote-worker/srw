"""Service-plane driver pods: the launch builder (connector drivers D5).

Adapted from generic hosting (``generic_harness_runtime.build_generic_launch``)
for long-lived pods shared by every binding of one connector, image digest and
credential generation. One launch is five objects, all named after the pod's
``sdi_`` identity row (so a restarted pod is a new name, never a reused one):

* an immutable **Secret** with ``/run/srw/request.json`` (the connector's
  config, and its credentials only for a driver that holds them in the pod)
  and ``/run/srw/identity`` (the pod's ``sdi_`` token); 512 KiB at most;
* a **NetworkPolicy** selecting the pod: ingress on ``srw-driver`` from agent
  pods and the orchestrator for a harness-facing driver (a workspace-facing
  one gets one policy per binding, :func:`binding_ingress_policy`); egress to
  the lease exchange port and the pinned hosts, DNS only when declared;
* a **Service** (ClusterIP) with the one named port ``srw-driver``;
* the **Pod**: ``restartPolicy: Always``, the driver image pinned by digest,
  no ServiceAccount token, a ServiceAccount with no bindings, no service
  links, no host namespaces, seccomp ``RuntimeDefault``, every capability
  dropped and no privilege escalation (the image's own user, root included:
  the namespace enforces Pod Security ``baseline``), the pinned hosts in
  ``hostAliases`` and, without declared DNS, a resolver that answers nothing.

Two SRW init containers run first, from the static shim image: the
**canary wait**, which blocks until the namespace default deny and this pod's
policy are enforced (a canary it must not reach stays refused, the exchange
it must reach answers), and the **shim install**, which copies the shim into
an ``emptyDir``. The shim becomes the driver container's command, with the
image's own entrypoint and command as its arguments.

A **managed MCP** driver's pod (D5a, a spec with an ``mcp`` block) differs:
the server image runs as itself, with the block's arguments and environment
and nothing of SRW's (no identity, no request file, no credential), and
SRW's **front** beside it holds the identity and the request file, serves
the ``srw-driver`` port and is ready only when a real MCP probe of the
server answers. Only the canary wait runs before them.

Labels deliberately omit SRW's agent and chart labels, which grant access to
internal services under existing NetworkPolicies.

Design: knowledge-base/knowledge/features/connector_drivers.md, "Three
planes", "Reachability", "The driver namespace baseline".
"""

from __future__ import annotations

import base64
import json
import re
from collections.abc import Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

from orchestrator.services.connector_egress import EgressPins
from shared.connectors.contract import (
    PROTOCOL_VERSION,
    SERVICE_PORT_NAME,
    DriverSpec,
)
from shared.connectors.mcp import ManagedMcp, TemplateError, managed_mcp

MANAGER = "connector-service-hosting"
REQUEST_PATH = "/run/srw/request.json"
IDENTITY_PATH = "/run/srw/identity"
SHIM_DIR = "/srw/bin"
SHIM_PATH = f"{SHIM_DIR}/srw-driver-shim"
#: The canary a driver pod must never reach: a listener of the exchange's
#: own server (``connectors.servicePods.canaryPort``), so it opens and closes
#: with the exchange. Never the API port, which stops listening before the
#: exchange does while the orchestrator shuts down.
CANARY_PORT = 8089
#: Agent pods, by the ``app`` label their provisioners set.
AGENT_APPS: tuple[str, ...] = (
    "srw-agent",
    "srw-persistent-agent",
    "srw-agent-stateless",
)
#: The unprivileged user SRW's own init containers run as.
SHIM_USER = 65532
_MAX_DELIVERY_BYTES = 512 * 1024
_NAMESPACE = re.compile(r"[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?\Z")
_DIGEST_REFERENCE = re.compile(r"[^\s@]+@sha256:[0-9a-f]{64}\Z")
_LABEL_VALUE = re.compile(r"[^A-Za-z0-9_.-]")


class ServiceLaunchError(ValueError):
    """A launch that SRW will not build; safe to show the operator."""


def label_value(text: str) -> str:
    """``text`` as a Kubernetes label value (63 characters, no ``/``)."""
    return _LABEL_VALUE.sub("_", text)[:63].strip("_.-")


@dataclass(frozen=True)
class ServicePodIdentity:
    """One service pod: its ``sdi_`` identity row and its key."""

    identity_id: str
    connector_id: str
    driver: str
    digest: str
    generation: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "identity_id", str(UUID(str(self.identity_id))))
        object.__setattr__(self, "connector_id", str(UUID(str(self.connector_id))))
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", self.digest):
            raise ValueError("a service pod runs one sha256 image digest")
        if not self.generation:
            raise ValueError("a service pod has a credential generation")

    @property
    def pod_name(self) -> str:
        return f"srw-drv-{UUID(self.identity_id).hex}"

    @property
    def selector(self) -> dict[str, str]:
        return {"srw.io/driver-identity": self.identity_id}

    @property
    def labels(self) -> dict[str, str]:
        return {
            "srw/managed-by": MANAGER,
            "srw.io/plane": "service",
            "srw.io/connector-id": self.connector_id,
            "srw.io/driver-identity": self.identity_id,
            "srw.io/driver": label_value(self.driver),
            "srw.io/image-digest": self.digest.removeprefix("sha256:")[:12],
            "srw.io/credential-generation": label_value(
                self.generation.rsplit(":", 1)[-1]
            )[:12],
        }


@dataclass(frozen=True)
class ServiceLaunchPolicy:
    """Operator-owned hosting settings, separate from the driver's spec."""

    namespace: str
    release_namespace: str
    shim_image: str
    exchange_host: str
    exchange_address: str
    exchange_port: int
    orchestrator_labels: Mapping[str, str]
    service_account: str = "srw-connector-driver"
    shim_pull_policy: str = "IfNotPresent"
    agent_apps: tuple[str, ...] = AGENT_APPS
    canary_port: int = CANARY_PORT
    canary_consecutive: int = 3
    canary_timeout_seconds: int = 120
    cpu_request: str = "50m"
    cpu_limit: str = "500m"
    memory_request: str = "64Mi"
    memory_limit: str = "256Mi"
    ephemeral_storage_request: str = "64Mi"
    ephemeral_storage_limit: str = "1Gi"
    max_cpu: str = "2"
    max_memory: str = "2Gi"
    termination_grace_seconds: int = 30
    #: SRW's managed MCP front (D5a), pinned by digest: the pod's only
    #: exposed port in front of an MCP server image.
    front_image: str = ""
    front_pull_policy: str = "IfNotPresent"

    def __post_init__(self) -> None:
        for name in (self.namespace, self.release_namespace):
            if not _NAMESPACE.fullmatch(name or ""):
                raise ValueError(f"invalid namespace {name!r}")
        if self.namespace == self.release_namespace:
            raise ValueError("driver pods never run in the release namespace")
        if not self.orchestrator_labels:
            raise ValueError("the orchestrator's pod labels are required")
        if not 1 <= self.canary_port <= 65535 or self.canary_port in (
            self.exchange_port,
            8085,
        ):
            raise ValueError("the canary port is its own port of the exchange's server")


@dataclass(frozen=True)
class ServiceLaunchPlan:
    identity: ServicePodIdentity
    namespace: str
    pod: dict = field(repr=False)
    service: dict = field(repr=False)
    network_policy: dict = field(repr=False)
    secret: dict = field(repr=False)


def _quantity(value: Any):
    from kubernetes.utils.quantity import parse_quantity

    try:
        return parse_quantity(str(value))
    except (ValueError, TypeError) as exc:
        raise ServiceLaunchError(f"invalid resource quantity {value!r}") from exc


def service_resources(
    requested: Mapping[str, Any], policy: ServiceLaunchPolicy
) -> dict[str, dict[str, str]]:
    """The driver container's resources: the spec's, else the installation's
    defaults, never past its ceilings; a request is raised to no more than its
    limit."""
    requests = {
        "cpu": policy.cpu_request,
        "memory": policy.memory_request,
        **{k: str(v) for k, v in (requested.get("requests") or {}).items()},
    }
    limits = {
        "cpu": policy.cpu_limit,
        "memory": policy.memory_limit,
        **{k: str(v) for k, v in (requested.get("limits") or {}).items()},
    }
    unknown = (set(requests) | set(limits)) - {"cpu", "memory"}
    if unknown:
        raise ServiceLaunchError(f"unsupported driver resources {sorted(unknown)}")
    for name, ceiling in (("cpu", policy.max_cpu), ("memory", policy.max_memory)):
        if _quantity(limits[name]) > _quantity(ceiling):
            raise ServiceLaunchError(
                f"the driver's {name} limit exceeds the installation's ceiling"
            )
        if _quantity(requests[name]) > _quantity(limits[name]):
            requests[name] = limits[name]
    requests["ephemeral-storage"] = policy.ephemeral_storage_request
    limits["ephemeral-storage"] = policy.ephemeral_storage_limit
    return {"requests": requests, "limits": limits}


def service_request(
    identity: ServicePodIdentity,
    *,
    spec: DriverSpec,
    config: Mapping[str, Any],
    credentials: Mapping[str, Any] | None,
    pins: EgressPins,
    policy: ServiceLaunchPolicy,
) -> dict[str, Any]:
    """``/run/srw/request.json`` of a service pod.

    A driver that delivers by lease gets no credential here: it exchanges
    each binding's lease token with its identity. Only a driver that holds
    its upstream credential in the pod (an image configured by environment,
    D5a) gets the connector's credentials.
    """
    held = spec.credential_delivery != "lease"
    request = {
        "protocol_version": PROTOCOL_VERSION,
        "plane": "service",
        "driver": spec.name,
        "connector": {"id": identity.connector_id, "config": dict(config)},
        "credentials": dict(credentials or {}) if held else {},
        "service": {"port": spec.service.port, "port_name": SERVICE_PORT_NAME},
        "exchange": {
            "url": f"http://{policy.exchange_host}:{policy.exchange_port}",
            "identity_file": IDENTITY_PATH,
        },
        "egress": pins.record(),
    }
    mcp = managed_mcp(spec)
    if mcp is not None:
        # The front's own block: what it forwards to, the tool classes per
        # access level and how it hands the server the credential.
        request["mcp"] = mcp.front_config()
    return request


def _json_bytes(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")


def _release_peer(policy: ServiceLaunchPolicy, pod_selector: dict) -> dict:
    return {
        "namespaceSelector": {
            "matchLabels": {"kubernetes.io/metadata.name": policy.release_namespace}
        },
        "podSelector": pod_selector,
    }


def _orchestrator_peer(policy: ServiceLaunchPolicy) -> dict:
    return _release_peer(policy, {"matchLabels": dict(policy.orchestrator_labels)})


def _driver_port() -> list[dict[str, Any]]:
    return [{"protocol": "TCP", "port": SERVICE_PORT_NAME}]


def _canary_args(pins: EgressPins, policy: ServiceLaunchPolicy) -> list[str]:
    args = [
        "canary-wait",
        "--deny",
        f"{policy.exchange_address}:{policy.canary_port}",
        "--allow",
        f"{policy.exchange_address}:{policy.exchange_port}",
    ]
    # The upstream hosts are expected, never required: an upstream that is
    # down must not keep the pod from starting (its policy is proven by the
    # exchange answering after the canary was refused).
    for pinned in pins.hosts:
        if pinned.protocol == "tcp" and not pinned.literal and pinned.ports:
            args += ["--expect", f"{pinned.addresses[0]}:{pinned.ports[0]}"]
    args += [
        "--consecutive",
        str(policy.canary_consecutive),
        "--timeout",
        f"{policy.canary_timeout_seconds}s",
    ]
    return args


def _shim_security() -> dict[str, Any]:
    return {
        "allowPrivilegeEscalation": False,
        "privileged": False,
        "capabilities": {"drop": ["ALL"]},
        "readOnlyRootFilesystem": True,
        "runAsNonRoot": True,
        "runAsUser": SHIM_USER,
        "runAsGroup": SHIM_USER,
    }


_SHIM_RESOURCES = {
    "requests": {"cpu": "10m", "memory": "16Mi", "ephemeral-storage": "16Mi"},
    "limits": {"cpu": "100m", "memory": "32Mi", "ephemeral-storage": "32Mi"},
}


def build_service_launch(
    identity: ServicePodIdentity,
    *,
    spec: DriverSpec,
    image: str,
    entrypoint: Sequence[str],
    cmd: Sequence[str],
    config: Mapping[str, Any],
    credentials: Mapping[str, Any] | None,
    identity_token: str,
    pins: EgressPins,
    policy: ServiceLaunchPolicy,
) -> ServiceLaunchPlan:
    """The Secret, NetworkPolicy, Service and Pod of one service pod."""
    if spec.plane != "service" or spec.service is None:
        raise ServiceLaunchError(f"{spec.name} is not a service-plane driver")
    if spec.name != identity.driver:
        raise ServiceLaunchError("the identity belongs to another driver")
    if not _DIGEST_REFERENCE.fullmatch(image) or not image.endswith(identity.digest):
        raise ServiceLaunchError("a service pod launches its image by its digest")
    if not policy.shim_image or any(ch.isspace() for ch in policy.shim_image):
        raise ServiceLaunchError("no driver shim image is configured")
    mcp = managed_mcp(spec)
    if mcp is not None and not _DIGEST_REFERENCE.fullmatch(policy.front_image or ""):
        raise ServiceLaunchError("no managed MCP front image is pinned by digest")
    program = [*entrypoint, *cmd]
    if mcp is not None and mcp.command:
        program = list(mcp.command)
    if not program:
        raise ServiceLaunchError(
            "the driver image declares no entrypoint or command for the shim to run"
        )
    name = identity.pod_name
    namespace = policy.namespace
    labels = identity.labels
    metadata = {"name": name, "namespace": namespace, "labels": labels}
    request = service_request(
        identity,
        spec=spec,
        config=config,
        credentials=credentials,
        pins=pins,
        policy=policy,
    )
    delivery = {
        "request.json": _json_bytes(request),
        "identity": identity_token.encode("ascii"),
    }
    if sum(len(value) for value in delivery.values()) > _MAX_DELIVERY_BYTES:
        raise ServiceLaunchError("the service pod's request exceeds 512 KiB")
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

    ingress: list[dict[str, Any]] = []
    if "harness" in spec.service.callers:
        # Agent pods are pooled: each call's lease authorizes it.
        ingress.append(
            {
                "from": [
                    _release_peer(
                        policy,
                        {
                            "matchExpressions": [
                                {
                                    "key": "app",
                                    "operator": "In",
                                    "values": list(policy.agent_apps),
                                }
                            ]
                        },
                    ),
                    _orchestrator_peer(policy),
                ],
                "ports": _driver_port(),
            }
        )
    egress = [
        {
            "to": [_orchestrator_peer(policy)],
            "ports": [{"protocol": "TCP", "port": policy.exchange_port}],
        },
        *pins.network_policy_egress(),
    ]
    network_policy = {
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": deepcopy(metadata),
        "spec": {
            "podSelector": {"matchLabels": identity.selector},
            "policyTypes": ["Ingress", "Egress"],
            "ingress": ingress,
            "egress": egress,
        },
    }
    service = {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": deepcopy(metadata),
        "spec": {
            "type": "ClusterIP",
            "selector": identity.selector,
            "ports": [
                {
                    "name": SERVICE_PORT_NAME,
                    "protocol": "TCP",
                    "port": spec.service.port,
                    "targetPort": SERVICE_PORT_NAME,
                }
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
        # Its last log line (why the wait gave up) becomes the termination
        # message the reconciler records with the pod.
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
        "command": [SHIM_PATH, "serve", "--"],
        # Kubernetes expands $(VAR) in args; $$ is a literal $.
        "args": [part.replace("$", "$$") for part in program],
        "ports": [
            {
                "name": SERVICE_PORT_NAME,
                "containerPort": spec.service.port,
                "protocol": "TCP",
            }
        ],
        "env": [
            {"name": "SRW_REQUEST_FILE", "value": REQUEST_PATH},
            {"name": "SRW_DRIVER_IDENTITY_FILE", "value": IDENTITY_PATH},
            {"name": "SRW_EXCHANGE_URL", "value": request["exchange"]["url"]},
            {"name": "SRW_DRIVER_PORT", "value": str(spec.service.port)},
        ],
        "resources": service_resources(spec.service.resources, policy),
        "readinessProbe": {
            "tcpSocket": {"port": SERVICE_PORT_NAME},
            "periodSeconds": 5,
            "failureThreshold": 3,
        },
        "securityContext": {
            "allowPrivilegeEscalation": False,
            "privileged": False,
            "capabilities": {"drop": ["ALL"]},
        },
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
    init_containers = [canary, install]
    containers = [driver]
    volumes: list[dict[str, Any]] = [
        {"name": "srw-bin", "emptyDir": {"sizeLimit": "32Mi"}},
        {
            "name": "delivery",
            "secret": {"secretName": name, "defaultMode": 0o444},
        },
    ]
    if mcp is not None:
        # A managed MCP server: the image runs as itself (no shim, no
        # identity, no request file, no credential) beside SRW's front,
        # which holds the identity and is the pod's only named port.
        try:
            server = _mcp_server_container(
                image=image,
                program=program,
                mcp=mcp,
                config=config,
                resources=service_resources(spec.service.resources, policy),
            )
        except TemplateError as exc:
            raise ServiceLaunchError(str(exc)) from None
        init_containers = [canary]
        containers = [
            server,
            _mcp_front_container(
                policy=policy,
                port=spec.service.port,
                env=driver["env"],
                mounts=driver["volumeMounts"][1:],
            ),
        ]
        volumes = [
            volumes[1],
            {"name": "tmp", "emptyDir": {"sizeLimit": "256Mi"}},
        ]
    host_aliases = pins.host_aliases()
    host_aliases.append(
        {"ip": policy.exchange_address, "hostnames": [policy.exchange_host]}
    )
    pod_spec: dict[str, Any] = {
        "restartPolicy": "Always",
        "serviceAccountName": policy.service_account,
        "automountServiceAccountToken": False,
        "enableServiceLinks": False,
        "hostNetwork": False,
        "hostPID": False,
        "hostIPC": False,
        "shareProcessNamespace": False,
        "securityContext": {"seccompProfile": {"type": "RuntimeDefault"}},
        "terminationGracePeriodSeconds": policy.termination_grace_seconds,
        "hostAliases": host_aliases,
        "initContainers": init_containers,
        "containers": containers,
        "volumes": volumes,
    }
    if not pins.dns:
        # No DNS egress: a lookup fails at once instead of timing out against
        # a resolver the policy blocks. The pinned names are in /etc/hosts.
        pod_spec["dnsPolicy"] = "None"
        pod_spec["dnsConfig"] = {"nameservers": ["127.0.0.1"]}
    pod = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": deepcopy(metadata),
        "spec": pod_spec,
    }
    return ServiceLaunchPlan(
        identity=identity,
        namespace=namespace,
        pod=pod,
        service=service,
        network_policy=network_policy,
        secret=secret,
    )


# The front buffers each answer (at most 4 MiB, 96 MiB in all) to filter
# and scrub it: its Go heap is held under GOMEMLIMIT, below the limit.
_FRONT_RESOURCES = {
    "requests": {"cpu": "10m", "memory": "32Mi", "ephemeral-storage": "16Mi"},
    "limits": {"cpu": "200m", "memory": "256Mi", "ephemeral-storage": "32Mi"},
}
_FRONT_GOMEMLIMIT = "200MiB"


def _literal(value: str) -> str:
    """A value Kubernetes never expands ($(VAR) in args and env; $$ is $)."""
    return value.replace("$", "$$")


def _mcp_server_container(
    *,
    image: str,
    program: Sequence[str],
    mcp: ManagedMcp,
    config: Mapping[str, Any],
    resources: dict[str, Any],
) -> dict[str, Any]:
    """The MCP server image as it is: its own program, the block's arguments
    and environment from the connector's config, nothing of SRW's."""
    return {
        "name": "driver",
        "image": image,
        "imagePullPolicy": "IfNotPresent",
        "command": [_literal(part) for part in program],
        "args": [_literal(part) for part in mcp.server_args(config)],
        "env": [
            {"name": key, "value": _literal(value)}
            for key, value in sorted(mcp.server_env(config).items())
        ],
        "resources": resources,
        # The image writes nowhere but /tmp, a small emptyDir: a tool that
        # writes files (a download to a caller-chosen path) cannot leave
        # them in the shared pod's image for another binding.
        "securityContext": {
            "allowPrivilegeEscalation": False,
            "privileged": False,
            "readOnlyRootFilesystem": True,
            "capabilities": {"drop": ["ALL"]},
        },
        "volumeMounts": [{"name": "tmp", "mountPath": "/tmp"}],
    }


def _mcp_front_container(
    *,
    policy: ServiceLaunchPolicy,
    port: int,
    env: list[dict[str, str]],
    mounts: list[dict[str, Any]],
) -> dict[str, Any]:
    """SRW's front: the request file, the identity, the named port, and
    readiness from a real MCP probe of the server."""
    return {
        "name": "front",
        "image": policy.front_image,
        "imagePullPolicy": policy.front_pull_policy,
        "command": ["/srw-mcp-front"],
        "args": ["serve"],
        "ports": [
            {"name": SERVICE_PORT_NAME, "containerPort": port, "protocol": "TCP"}
        ],
        "env": [*deepcopy(env), {"name": "GOMEMLIMIT", "value": _FRONT_GOMEMLIMIT}],
        "resources": deepcopy(_FRONT_RESOURCES),
        "readinessProbe": {
            "httpGet": {"path": "/readyz", "port": SERVICE_PORT_NAME},
            "periodSeconds": 5,
            "timeoutSeconds": 4,
            "failureThreshold": 3,
        },
        "livenessProbe": {
            "httpGet": {"path": "/livez", "port": SERVICE_PORT_NAME},
            "periodSeconds": 20,
            "failureThreshold": 3,
        },
        "securityContext": _shim_security(),
        "volumeMounts": deepcopy(mounts),
    }


def endpoint_service_name(connector_id: str, digest: str) -> str:
    """The endpoint Service of one connector's pods at one image digest."""
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", digest or ""):
        raise ValueError("an endpoint names one sha256 image digest")
    return f"srw-ep-{UUID(str(connector_id)).hex}-{digest.removeprefix('sha256:')[:12]}"


def endpoint_url(
    *, namespace: str, connector_id: str, digest: str, port: int, path: str = ""
) -> str:
    """Where a binding's caller reaches its connector's pods, by name."""
    if not _NAMESPACE.fullmatch(namespace or ""):
        raise ValueError(f"invalid namespace {namespace!r}")
    name = endpoint_service_name(connector_id, digest)
    return f"http://{name}.{namespace}.svc.cluster.local:{int(port)}{path}"


def endpoint_service(
    *,
    connector_id: str,
    digest: str,
    identity_id: str,
    port: int,
    namespace: str,
) -> dict[str, Any]:
    """The endpoint Service of a connector's pods at one digest.

    Bindings carry its name, never a pod's: it outlives every pod of the
    connector and digest, and its selector names the one pod that serves
    now (the newest ready one). A pod replaced after a re-pin, a lost pod or
    a new credential generation moves it, so a caller's address stays the
    same while the pod behind it changes. It carries no driver identity
    label: the sweep keeps it while a live pod of its key exists.
    """
    labels = {
        "srw/managed-by": MANAGER,
        "srw.io/plane": "service",
        "srw.io/endpoint": "true",
        "srw.io/connector-id": str(UUID(str(connector_id))),
        "srw.io/image-digest": digest.removeprefix("sha256:")[:12],
    }
    return {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": endpoint_service_name(connector_id, digest),
            "namespace": namespace,
            "labels": labels,
        },
        "spec": {
            "type": "ClusterIP",
            "selector": {"srw.io/driver-identity": str(UUID(str(identity_id)))},
            "ports": [
                {
                    "name": SERVICE_PORT_NAME,
                    "protocol": "TCP",
                    "port": int(port),
                    "targetPort": SERVICE_PORT_NAME,
                }
            ],
        },
    }


def binding_policy_name(identity: ServicePodIdentity, kind: str, owner_id: str) -> str:
    return f"{identity.pod_name}-{kind[0]}{UUID(str(owner_id)).hex[:12]}"


def binding_ingress_policy(
    identity: ServicePodIdentity,
    *,
    kind: str,
    owner_id: str,
    policy: ServiceLaunchPolicy,
) -> dict[str, Any]:
    """Ingress for one binding of a workspace-facing driver: only the bound
    workspace on ``srw-driver``.

    Two peers in the release namespace, both ``srw.io/component:
    agent-workspace``: a container workspace pod carries ``srw/job-id`` or
    ``srw/thread-id``; a same-cluster KubeVirt VM workspace's virt-launcher
    pod carries the VMI template's ``srw.io/owner-kind`` and
    ``srw.io/owner-id`` instead (``vmController`` VM template). A VM in a
    remote VM cluster is not in this cluster and cannot reach the pod.

    A network rule, not a lease check: it bounds where a leaked lease token
    can be used.
    """
    if kind not in ("job", "thread"):
        raise ValueError(f"unknown binding owner kind {kind!r}")
    owner = str(UUID(str(owner_id)))
    label = "srw/job-id" if kind == "job" else "srw/thread-id"
    return {
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": {
            "name": binding_policy_name(identity, kind, owner),
            "namespace": policy.namespace,
            "labels": {
                **identity.labels,
                "srw.io/binding-owner-kind": kind,
                "srw.io/binding-owner": owner,
            },
        },
        "spec": {
            "podSelector": {"matchLabels": identity.selector},
            "policyTypes": ["Ingress"],
            "ingress": [
                {
                    "from": [
                        _release_peer(
                            policy,
                            {
                                "matchLabels": {
                                    "srw.io/component": "agent-workspace",
                                    label: owner,
                                }
                            },
                        ),
                        _release_peer(
                            policy,
                            {
                                "matchLabels": {
                                    "srw.io/component": "agent-workspace",
                                    "srw.io/owner-kind": kind,
                                    "srw.io/owner-id": owner,
                                }
                            },
                        ),
                    ],
                    "ports": _driver_port(),
                }
            ],
        },
    }


__all__ = [
    "AGENT_APPS",
    "CANARY_PORT",
    "IDENTITY_PATH",
    "MANAGER",
    "REQUEST_PATH",
    "SHIM_PATH",
    "ServiceLaunchError",
    "ServiceLaunchPlan",
    "ServiceLaunchPolicy",
    "ServicePodIdentity",
    "binding_ingress_policy",
    "binding_policy_name",
    "build_service_launch",
    "endpoint_service",
    "endpoint_service_name",
    "endpoint_url",
    "label_value",
    "service_request",
    "service_resources",
]
