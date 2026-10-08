"""Service-plane driver hosting: the reconciler and idle stop (connector drivers D5).

One shared service pod per connector, image digest and credential generation
(decision 7), started when bindings need it and stopped when idle. A binding
is a live credential lease of a service driver's connector: it carries the
image digest it was bound to (``connector_service_images``), so a moved tag
moves only new bindings. The pod is one ``connector_driver_identities`` row:
its ``sdi_`` identity, minted before the pod exists and mounted only into it,
plus its lifecycle (0361).

Each leader-gated pass:

0. refuses to host at all unless this orchestrator's pod address and the
   lease exchange's Service address lie inside ``clusterCidrs``: on a cluster
   whose real ranges differ, the cluster ranges every pod policy refuses
   would be the wrong ones. Every live pod is stopped and none starts;
1. reads the live bindings of installed service drivers and the live pods;
2. starts a pod for every (connector, digest) with a binding and no live pod
   for the connector's current credential generation: under an advisory lock
   it counts the installation's pods against ``maxInstallation`` (a clear
   capacity refusal; the namespace quota is the backstop), mints the
   identity, then pins the pod's egress, resolves the image's entrypoint and
   creates the NetworkPolicy, Secret, Service and Pod
   (``connector_service_launch``). A launch SRW refuses (an egress host it may
   not open, an image without a program) revokes the identity at once and
   backs the key off;
3. records readiness; a pod not ready within the start timeout, or gone, is
   stopped (and replaced by the next pass while bindings remain);
4. stops a pod at once when what its policy opens is no longer allowed:
   the connector's projects lost private addresses, or its declared egress
   (hosts, ports) changed; it does not drain;
5. marks a pod idle when its last binding ends, or when a newer generation of
   its connector supersedes it for a digest or credential change (it drains
   its bindings for the idle time), and stops it after ``idleSeconds``: the
   identity is revoked first, so the exchange refuses it at once, then the
   objects are deleted and their absence recorded;
6. re-resolves a serving pod's pinned hosts every ``reresolveSeconds``
   (D5a): a shared pod with live bindings never idles, so it would never
   pick up an upstream that moved. When an address set changed, a
   replacement starts for the same key with the new policy and
   hostAliases (the old pod is marked ``replaced_at`` and keeps serving),
   and the old pod stops ``repinDrainSeconds`` after the replacement is
   ready;
7. keeps one ingress policy per binding for a workspace-facing driver,
   admitting only that binding's workspace;
8. keeps one endpoint Service per connector and digest with a live pod,
   pointing at the pod that serves now (the newest ready one): callers
   reach the connector by that stable name while pods are replaced;
9. deletes every object it manages that no live row names (a connector
   delete cascades its identities; this removes their pods).

Design: knowledge-base/knowledge/features/connector_drivers.md, "Three
planes" (service) and "The driver namespace baseline".
"""

from __future__ import annotations

import asyncio
import ipaddress
import json
import logging
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from orchestrator.services.connector_driver_identities import (
    mint_driver_identity,
    revoke_driver_identity,
)
from orchestrator.services.connector_egress import (
    EgressPins,
    EgressPolicy,
    EgressRefused,
    expand_rule,
    pin_egress,
    private_addresses_allowed,
    system_resolver,
)
from orchestrator.services.connector_service_images import (
    ServiceImageUnavailable,
    ensure_image,
)
from orchestrator.services.connector_service_launch import (
    MANAGER,
    ServiceLaunchError,
    ServiceLaunchPolicy,
    ServicePodIdentity,
    binding_ingress_policy,
    build_service_launch,
    endpoint_service,
)
from orchestrator.services.pinned_k8s_effect import (
    run_bounded_k8s_call,
    run_bounded_k8s_mutation,
)
from shared.connectors.contract import DriverSpec

logger = logging.getLogger(__name__)

#: Why a pod stopped (``connector_driver_identities.revoke_reason``).
IDLE = "idle"
LAUNCH_REFUSED = "launch_refused"
LAUNCH_FAILED = "launch_failed"
CAPACITY = "capacity"
START_TIMEOUT = "start_timeout"
POD_LOST = "pod_lost"
NOT_READY = "not_ready"
HOSTING_REFUSED = "hosting_refused"
HOSTING_DISABLED = "hosting_disabled"
#: Passes in a row the cluster check must fail before every pod stops.
CLUSTER_STRIKES = 2
EGRESS_WITHDRAWN = "egress_withdrawn"
#: A pinned host resolved to other addresses: a replacement serves now.
EGRESS_REPINNED = "egress_repinned"
#: Stops that back the key off before the next start.
_BACKOFF_REASONS = (LAUNCH_REFUSED, LAUNCH_FAILED, START_TIMEOUT, CAPACITY)
_CAPACITY_LOCK = "srw-connector-service-capacity"


class ServiceCapacityError(RuntimeError):
    """The installation (or the namespace quota) has no room for a pod."""


class ServiceRuntimeError(RuntimeError):
    """A Kubernetes effect that did not complete; safe to log."""


def _field(obj: Any, name: str, default: Any = None) -> Any:
    """A field of a Kubernetes object, as a dict or a client model.

    ``name`` is the API's JSON name. A client model names its attributes in
    its ``attribute_map`` (``cluster_ip`` for ``clusterIP``: no mechanical
    camel-to-snake rule gets acronyms right).
    """
    if obj is None:
        return default
    if isinstance(obj, Mapping):
        return obj.get(name, default)
    for attribute, key in (getattr(type(obj), "attribute_map", None) or {}).items():
        if key == name:
            return getattr(obj, attribute, default)
    snake = "".join(f"_{ch.lower()}" if ch.isupper() else ch for ch in name)
    return getattr(obj, snake, default)


@dataclass(frozen=True)
class PodState:
    """A driver pod as the API reports it.

    ``unready_since`` is when its Ready condition last turned false (None
    while ready, or when the API reports no condition). ``message`` is why
    an init container last failed: the last line of its termination message
    (the canary wait's verdict), recorded with the pod when it is stopped.
    """

    phase: str
    uid: str | None = None
    ready: bool = False
    reason: str | None = None
    unready_since: datetime | None = None
    message: str | None = None

    @property
    def absent(self) -> bool:
        return self.phase == "Absent"

    @property
    def lost(self) -> bool:
        """Gone, replaced, or terminal: a ``restartPolicy: Always`` pod that
        failed (evicted) or succeeded never runs again."""
        return self.phase in ("Absent", "Replaced", "Failed", "Succeeded")


def _timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str) and value:
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def _status(exc: BaseException) -> int | None:
    return getattr(exc, "status", None)


def _init_failure(status: Any, limit: int = 300) -> str | None:
    """The last line an init container printed before it last failed."""
    for item in _field(status, "initContainerStatuses") or []:
        for key in ("state", "lastState"):
            terminated = _field(_field(item, key), "terminated")
            if terminated is None or _field(terminated, "exitCode") in (0, None):
                continue
            lines = [
                line.strip()
                for line in str(_field(terminated, "message") or "").splitlines()
                if line.strip()
            ]
            text = lines[-1] if lines else f"exit {_field(terminated, 'exitCode')}"
            text = f"{_field(item, 'name')}: {text}"
            return text if len(text) <= limit else text[: limit - 3] + "..."
    return None


def _quota_refusal(exc: BaseException) -> bool:
    return _status(exc) == 403 and "exceeded quota" in str(
        getattr(exc, "body", "") or exc
    )


def _api_reason(exc: BaseException, limit: int = 400) -> str | None:
    """The API server's own message for a refusal (Pod Security admission,
    an invalid object), or ``None``. It describes SRW's object, not a
    secret, so it is recorded with the pod."""
    body = getattr(exc, "body", None)
    if isinstance(body, bytes):
        body = body.decode("utf-8", "replace")
    if not isinstance(body, str) or not body:
        return None
    try:
        status = json.loads(body)
    except ValueError:
        return None
    message = status.get("message") if isinstance(status, dict) else None
    if not isinstance(message, str) or not message:
        return None
    message = " ".join(message.split())
    return message if len(message) <= limit else message[: limit - 3] + "..."


class ServicePodRuntime:
    """Bounded Kubernetes effects in the connector driver namespace."""

    def __init__(self, core_api: Any, networking_api: Any, *, namespace: str):
        self.core_api = core_api
        self.networking_api = networking_api
        self.namespace = namespace

    async def _create(self, create: Callable[..., Any], body: dict) -> Any:
        try:
            return await run_bounded_k8s_mutation(
                create, namespace=self.namespace, body=body
            )
        except Exception as exc:
            if _status(exc) == 409:
                return None  # a retried launch: names are unique per identity
            if _quota_refusal(exc):
                raise ServiceCapacityError(
                    "the connector driver namespace's quota is exhausted"
                ) from None
            reason = _api_reason(exc) if _status(exc) in (400, 403, 422) else None
            raise ServiceRuntimeError(
                f"creating {body['kind']} {body['metadata']['name']} failed "
                f"(HTTP {_status(exc)})" + (f": {reason}" if reason else "")
            ) from None

    async def launch(self, plan: Any) -> str | None:
        """Create the policy first (the selector exists before the pod), then
        the Secret and Service, then the Pod; returns the pod's UID."""
        await self._create(
            self.networking_api.create_namespaced_network_policy, plan.network_policy
        )
        await self._create(self.core_api.create_namespaced_secret, plan.secret)
        await self._create(self.core_api.create_namespaced_service, plan.service)
        pod = await self._create(self.core_api.create_namespaced_pod, plan.pod)
        if pod is None:
            pod = await run_bounded_k8s_call(
                self.core_api.read_namespaced_pod,
                name=plan.identity.pod_name,
                namespace=self.namespace,
            )
        uid = _field(_field(pod, "metadata"), "uid")
        if uid:
            # The pod owns its Secret and Service, so Kubernetes garbage
            # collection removes leftovers. Best effort: the reconciler's
            # sweep is the backstop.
            owner = {
                "metadata": {
                    "ownerReferences": [
                        {
                            "apiVersion": "v1",
                            "kind": "Pod",
                            "name": plan.identity.pod_name,
                            "uid": uid,
                        }
                    ]
                }
            }
            for patch in (
                self.core_api.patch_namespaced_secret,
                self.core_api.patch_namespaced_service,
            ):
                try:
                    await run_bounded_k8s_mutation(
                        patch,
                        name=plan.identity.pod_name,
                        namespace=self.namespace,
                        body=owner,
                    )
                except Exception:
                    logger.debug("owner reference patch failed", exc_info=True)
        return str(uid) if uid else None

    async def service_cluster_ip(self, name: str, namespace: str) -> str:
        """A Service's ClusterIP as the API reports it (no DNS involved)."""
        try:
            service = await run_bounded_k8s_call(
                self.core_api.read_namespaced_service, name=name, namespace=namespace
            )
        except Exception:
            raise ServiceRuntimeError(
                f"reading the Service {namespace}/{name} failed"
            ) from None
        address = _field(_field(service, "spec"), "clusterIP")
        if not address or address == "None":
            raise ServiceRuntimeError(
                f"the Service {namespace}/{name} has no ClusterIP"
            )
        return str(address)

    async def observe(self, identity: ServicePodIdentity) -> PodState:
        try:
            pod = await run_bounded_k8s_call(
                self.core_api.read_namespaced_pod,
                name=identity.pod_name,
                namespace=self.namespace,
            )
        except Exception as exc:
            if _status(exc) == 404:
                return PodState("Absent")
            raise ServiceRuntimeError("reading a driver pod failed") from None
        metadata, status = _field(pod, "metadata"), _field(pod, "status")
        labels = _field(metadata, "labels") or {}
        if labels.get("srw.io/driver-identity") != identity.identity_id:
            return PodState("Replaced", uid=_field(metadata, "uid"))
        statuses = list(_field(status, "containerStatuses") or [])
        driver = next(
            (item for item in statuses if _field(item, "name") == "driver"),
            None,
        )
        waiting = _field(_field(driver, "state"), "waiting")
        # An init container stuck (Init:CrashLoopBackOff after a restart) is
        # the likelier reason a pod that was ready is not any more.
        init_waiting = next(
            (
                _field(_field(item, "state"), "waiting")
                for item in _field(status, "initContainerStatuses") or []
                if _field(_field(item, "state"), "waiting") is not None
            ),
            None,
        )
        condition = next(
            (
                item
                for item in _field(status, "conditions") or []
                if _field(item, "type") == "Ready"
            ),
            None,
        )
        # Every container: a managed MCP pod's front (whose /readyz is a real
        # MCP probe of the server beside it) as well as the driver.
        ready = bool(_field(driver, "ready", False)) and all(
            bool(_field(item, "ready", False)) for item in statuses
        )
        return PodState(
            phase=_field(status, "phase") or "Unknown",
            uid=_field(metadata, "uid"),
            ready=ready,
            reason=_field(init_waiting, "reason")
            or _field(waiting, "reason")
            or _field(status, "reason"),
            unready_since=(
                None
                if ready or _field(condition, "status") == "True"
                else _timestamp(_field(condition, "lastTransitionTime"))
            ),
            message=_init_failure(status),
        )

    async def _delete(self, delete: Callable[..., Any], name: str) -> None:
        try:
            await run_bounded_k8s_mutation(delete, name=name, namespace=self.namespace)
        except Exception as exc:
            if _status(exc) != 404:
                raise ServiceRuntimeError(f"deleting {name} failed") from None

    async def _binding_policies(self, identity_id: str) -> list[str]:
        listed = await run_bounded_k8s_call(
            self.networking_api.list_namespaced_network_policy,
            namespace=self.namespace,
            label_selector=(
                f"srw.io/driver-identity={identity_id},srw.io/binding-owner-kind"
            ),
        )
        return [
            _field(_field(item, "metadata"), "name")
            for item in _field(listed, "items") or []
        ]

    async def remove(self, identity: ServicePodIdentity) -> bool:
        """Delete every object of the pod; ``True`` once the pod is gone."""
        name = identity.pod_name
        await self._delete(self.core_api.delete_namespaced_pod, name)
        await self._delete(self.core_api.delete_namespaced_service, name)
        await self._delete(self.core_api.delete_namespaced_secret, name)
        await self._delete(self.networking_api.delete_namespaced_network_policy, name)
        for policy in await self._binding_policies(identity.identity_id):
            await self._delete(
                self.networking_api.delete_namespaced_network_policy, policy
            )
        return (await self.observe(identity)).absent

    async def sync_binding_policies(
        self, identity: ServicePodIdentity, desired: Mapping[str, dict]
    ) -> None:
        """Create the binding policies ``desired`` names; delete the rest."""
        existing = set(await self._binding_policies(identity.identity_id))
        for name, body in desired.items():
            if name not in existing:
                await self._create(
                    self.networking_api.create_namespaced_network_policy, body
                )
        for name in existing - set(desired):
            await self._delete(
                self.networking_api.delete_namespaced_network_policy, name
            )

    async def sync_endpoint(self, body: dict) -> bool:
        """Create a connector's endpoint Service, or point it at the pod its
        selector names now; ``True`` when anything changed."""
        name = body["metadata"]["name"]
        try:
            current = await run_bounded_k8s_call(
                self.core_api.read_namespaced_service,
                name=name,
                namespace=self.namespace,
            )
        except Exception as exc:
            if _status(exc) != 404:
                raise ServiceRuntimeError(f"reading {name} failed") from None
            await self._create(self.core_api.create_namespaced_service, body)
            return True
        selector = dict(_field(_field(current, "spec"), "selector") or {})
        if selector == body["spec"]["selector"]:
            return False
        try:
            await run_bounded_k8s_mutation(
                self.core_api.patch_namespaced_service,
                name=name,
                namespace=self.namespace,
                body={"spec": {"selector": body["spec"]["selector"]}},
            )
        except Exception:
            raise ServiceRuntimeError(f"pointing {name} at its pod failed") from None
        return True

    async def endpoint_services(self) -> list[str]:
        """The names of every endpoint Service this hosting created."""
        listed = await run_bounded_k8s_call(
            self.core_api.list_namespaced_service,
            namespace=self.namespace,
            label_selector=f"srw/managed-by={MANAGER},srw.io/endpoint=true",
        )
        return [
            _field(_field(item, "metadata"), "name")
            for item in _field(listed, "items") or []
        ]

    async def delete_endpoint(self, name: str) -> None:
        await self._delete(self.core_api.delete_namespaced_service, name)

    async def managed_objects(self) -> list[tuple[Callable[..., Any], str, str]]:
        """Every object this hosting created for one pod: ``(delete, name,
        identity)``. Endpoint Services belong to a connector and digest, not
        to a pod; :meth:`endpoint_services` lists them."""
        selector = f"srw/managed-by={MANAGER}"
        found: list[tuple[Callable[..., Any], str, str]] = []
        for list_call, delete in (
            (
                self.core_api.list_namespaced_pod,
                self.core_api.delete_namespaced_pod,
            ),
            (
                self.core_api.list_namespaced_service,
                self.core_api.delete_namespaced_service,
            ),
            (
                self.core_api.list_namespaced_secret,
                self.core_api.delete_namespaced_secret,
            ),
            (
                self.networking_api.list_namespaced_network_policy,
                self.networking_api.delete_namespaced_network_policy,
            ),
        ):
            listed = await run_bounded_k8s_call(
                list_call, namespace=self.namespace, label_selector=selector
            )
            for item in _field(listed, "items") or []:
                metadata = _field(item, "metadata")
                labels = _field(metadata, "labels") or {}
                if labels.get("srw.io/endpoint") == "true":
                    continue
                found.append(
                    (
                        delete,
                        _field(metadata, "name"),
                        labels.get("srw.io/driver-identity", ""),
                    )
                )
        return found

    async def delete_object(self, delete: Callable[..., Any], name: str) -> None:
        await self._delete(delete, name)


@dataclass(frozen=True)
class ServiceHostingSettings:
    """The installation's service-pod hosting (``connectors.servicePods``)."""

    namespace: str
    release_namespace: str
    shim_image: str
    exchange_host: str
    exchange_port: int
    orchestrator_labels: Mapping[str, str]
    #: The exchange server's canary listener: the start-up wait's deny target.
    canary_port: int = 8089
    max_installation: int = 10
    idle_seconds: float = 600.0
    start_timeout_seconds: float = 180.0
    launch_backoff_seconds: float = 300.0
    cluster_cidrs: tuple[str, ...] = ("10.42.0.0/16", "10.43.0.0/16")
    private_tiers: frozenset[str] = frozenset({"home-allowed"})
    ipv6: bool = False
    resources: Mapping[str, Any] = field(default_factory=dict)
    #: Refused even where private addresses are allowed (nodes, load
    #: balancers): ``servicePods.refusedCidrs``.
    refused_cidrs: tuple[str, ...] = ()
    #: This orchestrator's pod address (the downward API's status.podIP).
    pod_ip: str = ""
    #: The address of the node it runs on (status.hostIP).
    node_ip: str = ""
    #: Seconds between re-resolutions of a ready pod's pinned egress hosts
    #: (``servicePods.reresolveSeconds``); 0 never re-resolves.
    reresolve_seconds: float = 300.0
    #: Seconds a pod replaced after a re-pin keeps running once its
    #: replacement serves (``servicePods.repinDrainSeconds``).
    repin_drain_seconds: float = 30.0

    def cluster_problem(self, exchange_address: str) -> str | None:
        """Why this cluster's ranges are not the configured ``clusterCidrs``.

        Driver policies refuse ``clusterCidrs`` as egress; on a cluster whose
        real pod and service ranges differ they would refuse the wrong ones.
        The orchestrator's own pod address and the exchange's Service address
        are one known pod and one known Service address: both must lie inside.
        """
        cidrs = []
        for cidr in self.cluster_cidrs:
            try:
                cidrs.append(ipaddress.ip_network(cidr, strict=False))
            except ValueError:
                continue
        named = ", ".join(self.cluster_cidrs) or "none"
        for what, raw in (
            ("this orchestrator's pod address", self.pod_ip),
            ("the lease exchange's Service address", exchange_address),
        ):
            if not raw:
                return f"{what} is unknown"
            try:
                address = ipaddress.ip_address(raw)
            except ValueError:
                return f"{what} {raw!r} is not an address"
            if not any(
                cidr.version == address.version and address in cidr for cidr in cidrs
            ):
                return (
                    f"{what} {address} is outside connectors.servicePods."
                    f"clusterCidrs ({named}); set them to the cluster's real pod "
                    "and service ranges"
                )
        return self._node_problem()

    def _node_problem(self) -> str | None:
        """Why a driver pod could reach this cluster's nodes (kubelet,
        kube-apiserver, etcd), or ``None``.

        The node this orchestrator runs on is one known node address: every
        connector's egress policy must refuse it. Where private tiers exist
        that takes ``refusedCidrs`` covering it (the default is one
        installation's node and load-balancer ranges, not every cluster's);
        without, a private node address is refused anyway and a public one
        must be listed.
        """
        from orchestrator.services.connector_egress import EgressPolicy, refusal

        if not self.node_ip:
            return "this orchestrator's node address is unknown"
        try:
            address = ipaddress.ip_address(self.node_ip)
        except ValueError:
            return (
                f"this orchestrator's node address {self.node_ip!r} is not an address"
            )
        try:
            widest = EgressPolicy.build(
                self.cluster_cidrs,
                allow_private=bool(self.private_tiers),
                ipv6=self.ipv6,
                refused_cidrs=self.refused_cidrs,
            )
        except ValueError as exc:
            return f"the egress ranges are not networks ({exc})"
        if refusal(address, widest) is None:
            return (
                f"this orchestrator's node address {address} is not refused to "
                "driver pods; add the cluster's node (and load balancer) ranges "
                "to connectors.servicePods.refusedCidrs"
            )
        return None

    def launch_policy(self, exchange_address: str) -> ServiceLaunchPolicy:
        resources = self.resources or {}
        requests = resources.get("requests") or {}
        limits = resources.get("limits") or {}
        ceilings = resources.get("max") or {}
        overrides = {
            key: str(value)
            for key, value in (
                ("cpu_request", requests.get("cpu")),
                ("memory_request", requests.get("memory")),
                ("ephemeral_storage_request", requests.get("ephemeralStorage")),
                ("cpu_limit", limits.get("cpu")),
                ("memory_limit", limits.get("memory")),
                ("ephemeral_storage_limit", limits.get("ephemeralStorage")),
                ("max_cpu", ceilings.get("cpu")),
                ("max_memory", ceilings.get("memory")),
            )
            if value is not None
        }
        return ServiceLaunchPolicy(
            namespace=self.namespace,
            release_namespace=self.release_namespace,
            shim_image=self.shim_image,
            exchange_host=self.exchange_host,
            exchange_address=exchange_address,
            exchange_port=self.exchange_port,
            orchestrator_labels=dict(self.orchestrator_labels),
            canary_port=self.canary_port,
            **overrides,
        )


@dataclass
class ReconcileReport:
    started: list[str] = field(default_factory=list)
    ready: list[str] = field(default_factory=list)
    stopped: list[tuple[str, str]] = field(default_factory=list)
    removed: list[str] = field(default_factory=list)
    refused: list[tuple[str, str]] = field(default_factory=list)
    capacity: int = 0
    swept: int = 0

    def __bool__(self) -> bool:
        return bool(
            self.started
            or self.ready
            or self.stopped
            or self.removed
            or self.refused
            or self.capacity
            or self.swept
        )


def credential_generation(
    spec: DriverSpec, connector: Mapping[str, Any], *, private_allowed: bool
) -> str:
    """A keyed fingerprint of what a pod's immutable Secret and policy hold.

    A driver holding its credential in the pod includes it; a lease driver
    does not (its pod exchanges each binding's lease). The egress tier is in
    it too, so a project tier change starts a pod with the new policy.
    """
    from orchestrator.security.crypto import credential_fingerprint

    held = spec.credential_delivery != "lease"
    canonical = json.dumps(
        {
            "driver": spec.name,
            "config": connector.get("config") or {},
            "credentials": (connector.get("credentials") or {}) if held else None,
            "private_allowed": private_allowed,
        },
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return credential_fingerprint(canonical)


def egress_withdrawn(
    spec: DriverSpec,
    connector: Mapping[str, Any] | None,
    recorded: Any,
    *,
    private_allowed: bool,
) -> str | None:
    """Why a running pod's pinned egress opens more than is allowed now.

    A pod's policy is fixed when it starts. It may not keep private addresses
    its connector's projects lost, nor destinations its connector's config no
    longer declares: such a pod stops at once instead of draining. ``None``
    when its egress still holds (or nothing was pinned to compare).
    """
    if connector is None:
        return "its connector is gone"
    if isinstance(recorded, str):
        recorded = json.loads(recorded)
    if not isinstance(recorded, Mapping):
        return None
    if recorded.get("private_allowed") and not private_allowed:
        return "its connector's projects no longer allow private addresses"
    config = connector.get("config") or {}
    try:
        declared = sorted(
            (host, tuple(ports), rule.protocol)
            for rule in spec.egress
            for host, ports in (expand_rule(rule, config),)
        )
    except EgressRefused as exc:
        return f"its declared egress no longer holds ({exc})"
    pinned = sorted(
        (str(item.get("host")), tuple(item.get("ports") or ()), item.get("protocol"))
        for item in recorded.get("hosts") or ()
    )
    if declared != pinned:
        return "its declared egress changed"
    return None


def _pod_key(row: Mapping[str, Any]) -> tuple[str, str, str]:
    return (
        str(row["connector_id"]),
        str(row["image_digest"]),
        str(row["credential_generation"]),
    )


def _address_sets(recorded: Any) -> dict[tuple, tuple[bool, frozenset[str]]]:
    """A recorded egress as ``{(host, ports, protocol): (literal, addresses)}``."""
    if not isinstance(recorded, Mapping):
        return {}
    return {
        (
            str(item.get("host")),
            tuple(item.get("ports") or ()),
            str(item.get("protocol") or "tcp"),
        ): (
            bool(item.get("literal")),
            frozenset(str(address) for address in item.get("addresses") or ()),
        )
        for item in recorded.get("hosts") or ()
        if isinstance(item, Mapping)
    }


def _serving(rows: list[Mapping[str, Any]]) -> Mapping[str, Any]:
    """The pod a connector's endpoint at one digest points at: the newest
    ready pod not being replaced, else the newest ready one, else the newest
    (which gets traffic once it is ready). Newer pods supersede older ones:
    a new credential generation, a re-pin's replacement, a lost pod's."""

    def newest(candidates: list[Mapping[str, Any]]) -> Mapping[str, Any] | None:
        return max(candidates, key=lambda row: row["created_at"], default=None)

    ready = [row for row in rows if row["ready_at"] is not None]
    return (
        newest([row for row in ready if row["replaced_at"] is None])
        or newest(ready)
        or newest(rows)
    )


def _identity(row: Mapping[str, Any]) -> ServicePodIdentity:
    return ServicePodIdentity(
        identity_id=str(row["id"]),
        connector_id=str(row["connector_id"]),
        driver=str(row["driver"]),
        digest=str(row["image_digest"]),
        generation=str(row["credential_generation"]),
    )


_LIVE_BINDINGS = """
SELECT connector_id, driver, image_digest, job_id, thread_id
  FROM connector_credential_leases
 WHERE revoked_at IS NULL AND expires_at > now()
   AND image_digest IS NOT NULL AND driver = ANY($1::text[])
"""
_POD_ROWS = """
SELECT id, connector_id, driver, image_digest, credential_generation,
       image_reference, pod_namespace, pod_name, pod_uid, created_at, ready_at,
       last_bound_at, idle_since, revoked_at, revoke_reason, removed_at, egress,
       egress_resolved_at, replaced_at
  FROM connector_driver_identities
 WHERE credential_generation IS NOT NULL AND removed_at IS NULL
"""


@dataclass
class _Binding:
    connector_id: str
    driver: str
    digest: str
    owners: set[tuple[str, str]] = field(default_factory=set)


class ServiceHostingReconciler:
    """One leader's service-pod reconciliation over the store and the API."""

    def __init__(
        self,
        *,
        store: Any,
        runtime: ServicePodRuntime,
        drivers: Any,
        settings: ServiceHostingSettings,
        resolver: Callable[[str, bool], Any] = system_resolver,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        strikes: dict[str, int] | None = None,
    ) -> None:
        self.store = store
        self.runtime = runtime
        self.drivers = drivers
        self.settings = settings
        self.resolver = resolver
        self.clock = clock
        #: Passes in a row the cluster check failed; shared across the
        #: per-pass reconcilers of one loop.
        self.strikes = strikes if strikes is not None else {}

    def _service_specs(self) -> dict[str, DriverSpec]:
        return {
            driver.spec.name: driver.spec
            for driver in self.drivers.drivers()
            if driver.spec.plane == "service" and driver.spec.service is not None
        }

    async def _exchange_address(self) -> str:
        """The exchange Service's ClusterIP, read from the API: a relative
        name through the resolver's search path could answer otherwise."""
        name, _, rest = self.settings.exchange_host.partition(".")
        namespace = rest.split(".", 1)[0] or self.settings.release_namespace
        try:
            address = await self.runtime.service_cluster_ip(name, namespace)
        except ServiceRuntimeError as exc:
            raise ServiceLaunchError(str(exc)) from None
        if ":" in address:
            raise ServiceLaunchError(
                f"the lease exchange Service {namespace}/{name} has no IPv4 ClusterIP"
            )
        return address

    async def _bindings(self, conn: Any, names: Iterable[str]) -> dict:
        grouped: dict[tuple[str, str], _Binding] = {}
        for row in await conn.fetch(_LIVE_BINDINGS, sorted(names)):
            key = (str(row["connector_id"]), str(row["image_digest"]))
            binding = grouped.setdefault(
                key, _Binding(key[0], str(row["driver"]), key[1])
            )
            if row["job_id"] is not None:
                binding.owners.add(("job", str(row["job_id"])))
            if row["thread_id"] is not None:
                binding.owners.add(("thread", str(row["thread_id"])))
        return grouped

    async def _revoke(
        self, row: Mapping[str, Any], reason: str, *, error: str | None = None
    ) -> None:
        async with self.store.acquire() as conn:
            async with conn.transaction():
                await revoke_driver_identity(
                    conn, identity_id=str(row["id"]), reason=reason
                )
                await conn.execute(
                    "UPDATE connector_driver_identities "
                    "SET launch_error = COALESCE($2, launch_error), idle_since = NULL "
                    "WHERE id = $1",
                    UUID(str(row["id"])),
                    error,
                )

    async def _mark_removed(self, identity_id: str) -> None:
        async with self.store.acquire() as conn:
            await conn.execute(
                "UPDATE connector_driver_identities SET removed_at = now() "
                "WHERE id = $1 AND revoked_at IS NOT NULL",
                UUID(identity_id),
            )

    async def _stop(
        self,
        row: Mapping[str, Any],
        reason: str,
        report: ReconcileReport,
        *,
        error: str | None = None,
    ) -> None:
        """Revoke first (the exchange refuses the pod at once), then delete."""
        await self._revoke(row, reason, error=error)
        report.stopped.append((str(row["id"]), reason))
        await self._remove(row, report)

    async def _remove(self, row: Mapping[str, Any], report: ReconcileReport) -> None:
        try:
            gone = await self.runtime.remove(_identity(row))
        except ServiceRuntimeError as exc:
            logger.warning(
                "Driver pod %s: removal incomplete (%s)", row["pod_name"], exc
            )
            return
        if gone:
            await self._mark_removed(str(row["id"]))
            report.removed.append(str(row["id"]))

    async def _backed_off(
        self, conn: Any, connector_id: str, digest: str, generation: str
    ) -> bool:
        return bool(
            await conn.fetchval(
                """
                SELECT 1 FROM connector_driver_identities
                 WHERE connector_id = $1 AND image_digest = $2
                   AND credential_generation = $3
                   AND revoke_reason = ANY($4::text[])
                   AND revoked_at > now() - make_interval(secs => $5::float8)
                 LIMIT 1
                """,
                UUID(connector_id),
                digest,
                generation,
                list(_BACKOFF_REASONS),
                self.settings.launch_backoff_seconds,
            )
        )

    async def _claim(
        self,
        spec: DriverSpec,
        binding: _Binding,
        generation: str,
        reference: str,
        *,
        replaces: str | None = None,
    ) -> tuple[Any, Any] | None:
        """Mint the identity of a new pod under the capacity lock.

        ``None`` when a live pod already holds the key; raises
        :class:`ServiceCapacityError` at the installation cap. With
        ``replaces``, that live pod is marked replaced in the same
        transaction, so the key is free for its replacement while it keeps
        serving (``None`` if it was stopped or replaced meanwhile).
        """
        async with self.store.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                    _CAPACITY_LOCK,
                )
                if replaces is not None:
                    marked = await conn.fetchval(
                        "UPDATE connector_driver_identities SET replaced_at = now() "
                        "WHERE id = $1 AND revoked_at IS NULL AND replaced_at IS NULL "
                        "RETURNING id",
                        UUID(replaces),
                    )
                    if marked is None:
                        return None
                held = await conn.fetchval(
                    "SELECT 1 FROM connector_driver_identities "
                    "WHERE connector_id = $1 AND image_digest = $2 "
                    "AND credential_generation = $3 AND revoked_at IS NULL "
                    "AND replaced_at IS NULL",
                    UUID(binding.connector_id),
                    binding.digest,
                    generation,
                )
                if held:
                    return None
                live = await conn.fetchval(
                    "SELECT count(*) FROM connector_driver_identities "
                    "WHERE credential_generation IS NOT NULL AND removed_at IS NULL"
                )
                if int(live) >= self.settings.max_installation:
                    raise ServiceCapacityError(
                        f"the installation runs its cap of "
                        f"{self.settings.max_installation} service pods"
                    )
                minted = await mint_driver_identity(
                    conn,
                    connector_id=binding.connector_id,
                    driver=spec.name,
                    image_digest=binding.digest,
                    pod_namespace=self.settings.namespace,
                )
                row = await conn.fetchrow(
                    """
                    UPDATE connector_driver_identities
                       SET pod_name = 'srw-drv-' || replace(id::text, '-', ''),
                           credential_generation = $2,
                           image_reference = $3
                     WHERE id = $1
                    RETURNING id, connector_id, driver, image_digest,
                              credential_generation, image_reference,
                              pod_namespace, pod_name, pod_uid, created_at,
                              ready_at, last_bound_at, idle_since, revoked_at,
                              revoke_reason, removed_at
                    """,
                    UUID(minted.id),
                    generation,
                    reference,
                )
        return row, minted

    async def _start(
        self,
        spec: DriverSpec,
        binding: _Binding,
        connector: Mapping[str, Any],
        *,
        generation: str,
        private_allowed: bool,
        exchange_address: str,
        report: ReconcileReport,
        pins: EgressPins | None = None,
        replaces: str | None = None,
    ) -> None:
        """Start a pod for a key; with ``replaces``, a replacement for that
        live pod, pinned with the ``pins`` its re-resolution found."""
        from orchestrator.services.connector_service_images import (
            image_reference_for,
        )

        reference = image_reference_for(spec.name)
        if not reference:
            report.refused.append((binding.connector_id, "no image is configured"))
            return
        try:
            claimed = await self._claim(
                spec, binding, generation, reference, replaces=replaces
            )
        except ServiceCapacityError as exc:
            report.capacity += 1
            logger.warning(
                "Driver pod for connector %s not started: %s", binding.connector_id, exc
            )
            return
        if claimed is None:
            return
        row, minted = claimed
        identity = _identity(row)
        try:
            config = connector.get("config") or {}
            if pins is None:
                pins = await self._pin(spec, config, private_allowed=private_allowed)
            async with self.store.acquire() as conn:
                image = await ensure_image(
                    conn, driver=spec.name, reference=reference, digest=binding.digest
                )
            plan = build_service_launch(
                identity,
                spec=spec,
                # The repository the digest was resolved from, which may be an
                # earlier reference's when the driver's image moved since.
                image=f"{_repository(image.reference)}@{binding.digest}",
                entrypoint=image.entrypoint,
                cmd=image.cmd,
                config=config,
                credentials=connector.get("credentials"),
                identity_token=minted.token,
                pins=pins,
                policy=self.settings.launch_policy(exchange_address),
            )
        except (EgressRefused, ServiceLaunchError, ServiceImageUnavailable) as exc:
            # SRW will not build this pod: say why, back the key off.
            await self._revoke(row, LAUNCH_REFUSED, error=str(exc))
            await self._mark_removed(str(row["id"]))
            report.refused.append((binding.connector_id, str(exc)))
            logger.warning(
                "Driver pod for connector %s refused: %s", binding.connector_id, exc
            )
            return
        except Exception as exc:
            # Nothing was created yet; never leave a live identity without a
            # pod. The key backs off like a refusal.
            logger.exception(
                "Driver pod for connector %s could not be built", binding.connector_id
            )
            await self._revoke(row, LAUNCH_FAILED, error=type(exc).__name__)
            await self._mark_removed(str(row["id"]))
            report.refused.append((binding.connector_id, type(exc).__name__))
            return
        async with self.store.acquire() as conn:
            await conn.execute(
                "UPDATE connector_driver_identities "
                "SET egress = $2::jsonb, egress_resolved_at = $3 WHERE id = $1",
                UUID(identity.identity_id),
                json.dumps(pins.record()),
                pins.resolved_at,
            )
        try:
            uid = await self.runtime.launch(plan)
        except ServiceCapacityError as exc:
            await self._revoke(row, CAPACITY, error=str(exc))
            report.capacity += 1
            await self._remove(row, report)
            return
        except ServiceRuntimeError as exc:
            await self._revoke(row, LAUNCH_FAILED, error=str(exc))
            report.refused.append((binding.connector_id, str(exc)))
            await self._remove(row, report)
            return
        async with self.store.acquire() as conn:
            await conn.execute(
                "UPDATE connector_driver_identities SET pod_uid = $2, "
                "last_bound_at = now() WHERE id = $1",
                UUID(identity.identity_id),
                uid,
            )
        report.started.append(identity.identity_id)
        logger.info(
            "Driver pod %s started for connector %s (%s at %s)%s",
            identity.pod_name,
            identity.connector_id,
            spec.name,
            binding.digest,
            f", replacing {replaces} after a re-pin" if replaces else "",
        )

    async def _pin(
        self, spec: DriverSpec, config: Mapping[str, Any], *, private_allowed: bool
    ) -> EgressPins:
        """Resolve and check the spec's egress for one connector's config."""
        policy = EgressPolicy.build(
            self.settings.cluster_cidrs,
            allow_private=private_allowed,
            ipv6=self.settings.ipv6,
            refused_cidrs=self.settings.refused_cidrs,
        )
        return await pin_egress(
            spec.egress,
            config,
            policy=policy,
            needs_dns=spec.needs_dns,
            resolver=self.resolver,
            now=self.clock,
        )

    async def _observe(self, row: Mapping[str, Any], report: ReconcileReport) -> bool:
        """Record readiness; stop a lost or never-ready pod. Whether it lives."""
        identity = _identity(row)
        state = await self.runtime.observe(identity)
        if state.lost:
            # Evicted (Failed), gone or replaced: the next pass starts a new
            # pod while bindings need one.
            await self._stop(row, POD_LOST, report)
            return False
        timeout = self.settings.start_timeout_seconds
        if state.ready:
            if row["ready_at"] is None:
                async with self.store.acquire() as conn:
                    await conn.execute(
                        "UPDATE connector_driver_identities SET ready_at = now() "
                        "WHERE id = $1 AND ready_at IS NULL",
                        UUID(identity.identity_id),
                    )
                report.ready.append(identity.identity_id)
            return True
        if row["ready_at"] is None:
            if (self.clock() - row["created_at"]).total_seconds() > timeout:
                logger.warning(
                    "Driver pod %s not ready after %.0fs (%s); stopping it",
                    identity.pod_name,
                    timeout,
                    state.message or state.reason or state.phase,
                )
                await self._stop(row, START_TIMEOUT, report, error=state.message)
                return False
            return True
        # It was ready and is not any more (a node restart, Init:CrashLoop):
        # the start timeout again, from when it turned unready.
        since = state.unready_since
        if since is not None and (self.clock() - since).total_seconds() > timeout:
            logger.warning(
                "Driver pod %s unready for %.0fs (%s); replacing it",
                identity.pod_name,
                timeout,
                state.message or state.reason or state.phase,
            )
            await self._stop(row, NOT_READY, report, error=state.message)
            return False
        return True

    async def _settle_idle(
        self, row: Mapping[str, Any], *, bound: bool, report: ReconcileReport
    ) -> bool:
        """Track a pod's idleness; stop it after the idle timeout."""
        identity_id = UUID(str(row["id"]))
        async with self.store.acquire() as conn:
            if bound:
                await conn.execute(
                    "UPDATE connector_driver_identities "
                    "SET last_bound_at = now(), idle_since = NULL WHERE id = $1",
                    identity_id,
                )
                return True
            idle_since = row["idle_since"]
            if idle_since is None:
                await conn.execute(
                    "UPDATE connector_driver_identities SET idle_since = now() "
                    "WHERE id = $1 AND idle_since IS NULL",
                    identity_id,
                )
                return True
        if (self.clock() - idle_since).total_seconds() >= self.settings.idle_seconds:
            await self._stop(row, IDLE, report)
            return False
        return True

    async def reconcile_once(self) -> ReconcileReport:
        report = ReconcileReport()
        specs = self._service_specs()
        async with self.store.acquire() as conn:
            bindings = await self._bindings(conn, specs) if specs else {}
            rows = list(await conn.fetch(_POD_ROWS))
        # Stopped pods whose objects are not confirmed gone yet.
        for row in rows:
            if row["revoked_at"] is not None:
                await self._remove(row, report)
        live = [row for row in rows if row["revoked_at"] is None]

        exchange: str | None = None
        try:
            exchange = await self._exchange_address()
        except ServiceLaunchError as exc:
            # A resolver blip: nothing starts this pass, nothing is stopped.
            logger.warning("Driver pods not started this pass: %s", exc)
        else:
            problem = self.settings.cluster_problem(exchange)
            if problem is None:
                self.strikes.pop("cluster", None)
            else:
                strikes = self.strikes["cluster"] = self.strikes.get("cluster", 0) + 1
                report.refused.append(("installation", problem))
                if strikes < CLUSTER_STRIKES:
                    # One bad pass may be a moment's misreading: start
                    # nothing, stop nothing yet.
                    logger.warning(
                        "Service-pod hosting check failed (%s); no driver pod "
                        "starts, and every one stops if it fails again",
                        problem,
                    )
                    return report
                logger.error(
                    "Service-pod hosting refused: %s. Every driver pod is "
                    "stopped and none starts.",
                    problem,
                )
                for row in live:
                    await self._stop(row, HOSTING_REFUSED, report)
                await self._sync_endpoints(specs)
                report.swept = await self._sweep()
                return report

        # Each connector's config and tier, read once per pass.
        connectors: dict[str, tuple[Mapping[str, Any] | None, bool]] = {}

        async def connector_state(
            connector_id: str,
        ) -> tuple[Mapping[str, Any] | None, bool]:
            if connector_id not in connectors:
                connector = await self.store.get_datasource(connector_id)
                async with self.store.acquire() as conn:
                    private = await private_addresses_allowed(
                        conn, connector_id, private_tiers=self.settings.private_tiers
                    )
                connectors[connector_id] = (connector, private)
            return connectors[connector_id]

        # The connector's current generation for each bound (connector, digest).
        current: dict[tuple[str, str], tuple[str, bool, Mapping[str, Any]]] = {}
        for key, binding in bindings.items():
            spec = specs.get(binding.driver)
            connector, private = await connector_state(binding.connector_id)
            if spec is None or connector is None:
                continue
            current[key] = (
                credential_generation(spec, connector, private_allowed=private),
                private,
                connector,
            )

        survivors: list[Mapping[str, Any]] = []
        active_ids: set[str] = set()
        for row in live:
            spec = specs.get(str(row["driver"]))
            if spec is None:
                await self._stop(row, IDLE, report)  # the driver was uninstalled
                continue
            connector, private = await connector_state(str(row["connector_id"]))
            withdrawn = egress_withdrawn(
                spec, connector, row["egress"], private_allowed=private
            )
            if withdrawn is not None:
                logger.warning(
                    "Driver pod %s stopped at once: %s", row["pod_name"], withdrawn
                )
                await self._stop(row, EGRESS_WITHDRAWN, report)
                continue
            if not await self._observe(row, report):
                continue
            key = (str(row["connector_id"]), str(row["image_digest"]))
            active = key in current and current[key][0] == row["credential_generation"]
            if await self._settle_idle(row, bound=active, report=report):
                survivors.append(row)
                if active:
                    active_ids.add(str(row["id"]))

        # A pod being replaced holds no key: if its replacement failed, the
        # next start (after its back-off) replaces it again.
        held = {
            (
                str(r["connector_id"]),
                str(r["image_digest"]),
                str(r["credential_generation"]),
            )
            for r in survivors
            if r["replaced_at"] is None
        }
        for key, (generation, private, connector) in current.items():
            if exchange is None or (key[0], key[1], generation) in held:
                continue
            binding = bindings[key]
            async with self.store.acquire() as conn:
                if await self._backed_off(conn, key[0], key[1], generation):
                    continue
            await self._start(
                specs[binding.driver],
                binding,
                connector,
                generation=generation,
                private_allowed=private,
                exchange_address=exchange,
                report=report,
            )

        if exchange is not None:
            await self._repin(
                survivors,
                active_ids,
                specs,
                connector_state=connector_state,
                exchange_address=exchange,
                report=report,
            )
        await self._sync_binding_policies(bindings, specs)
        await self._sync_endpoints(specs)
        report.swept = await self._sweep()
        return report

    def _repin_due(self, row: Mapping[str, Any]) -> bool:
        interval = self.settings.reresolve_seconds
        resolved = row["egress_resolved_at"]
        if interval <= 0 or resolved is None:
            return False
        return (self.clock() - resolved).total_seconds() >= interval

    async def _repin(
        self,
        survivors: list[Mapping[str, Any]],
        active_ids: set[str],
        specs: Mapping[str, DriverSpec],
        *,
        connector_state: Callable[[str], Any],
        exchange_address: str,
        report: ReconcileReport,
    ) -> None:
        """Re-resolve the pinned hosts of the pods that serve bindings, and
        replace a pod whose addresses changed.

        A shared pod with live bindings never goes idle, so it never rolls
        on its own: an upstream that moved would stay unreachable. Every
        ``reresolveSeconds`` its hosts are resolved again; when an address
        set changed, a replacement starts for the same key with the new
        policy and hostAliases, the endpoint Service moves to it once it is
        ready, and the old pod stops ``repinDrainSeconds`` later. The old
        pod serves throughout, so a caller loses at most the requests in
        flight at the switch (and a stateful MCP session, which its client
        starts again). A host that no longer resolves, or resolves to an
        address SRW refuses, keeps the addresses already pinned.
        """
        replacements: dict[tuple[str, str, str], Mapping[str, Any]] = {}
        for row in survivors:
            if row["replaced_at"] is None and row["ready_at"] is not None:
                key = _pod_key(row)
                newest = replacements.get(key)
                if newest is None or row["created_at"] > newest["created_at"]:
                    replacements[key] = row
        for row in survivors:
            if row["replaced_at"] is not None:
                replacement = replacements.get(_pod_key(row))
                if replacement is not None and (
                    (self.clock() - replacement["ready_at"]).total_seconds()
                    >= self.settings.repin_drain_seconds
                ):
                    await self._stop(row, EGRESS_REPINNED, report)
                continue
            if (
                str(row["id"]) not in active_ids
                or row["ready_at"] is None
                or not self._repin_due(row)
            ):
                continue
            spec = specs[str(row["driver"])]
            connector, private = await connector_state(str(row["connector_id"]))
            if connector is None:
                continue
            pins = await self._re_resolve(row, spec, connector, private=private)
            if pins is None:
                continue
            logger.warning(
                "Driver pod %s: a pinned host resolves to other addresses now; "
                "starting its replacement",
                row["pod_name"],
            )
            await self._start(
                spec,
                _Binding(
                    str(row["connector_id"]),
                    str(row["driver"]),
                    str(row["image_digest"]),
                ),
                connector,
                generation=str(row["credential_generation"]),
                private_allowed=private,
                exchange_address=exchange_address,
                report=report,
                pins=pins,
                replaces=str(row["id"]),
            )

    async def _re_resolve(
        self,
        row: Mapping[str, Any],
        spec: DriverSpec,
        connector: Mapping[str, Any],
        *,
        private: bool,
    ) -> EgressPins | None:
        """The pod's pins resolved again when an address set changed;
        otherwise ``None``, with the resolution time recorded."""
        recorded = row["egress"]
        if isinstance(recorded, str):
            recorded = json.loads(recorded)
        pinned = _address_sets(recorded)
        pins: EgressPins | None = None
        if any(not literal for literal, _ in pinned.values()):
            try:
                pins = await self._pin(
                    spec, connector.get("config") or {}, private_allowed=private
                )
            except EgressRefused as exc:
                logger.warning(
                    "Driver pod %s keeps its pinned egress: re-resolving it was "
                    "refused (%s)",
                    row["pod_name"],
                    exc,
                )
        if pins is not None and _address_sets(pins.record()) != pinned:
            return pins
        async with self.store.acquire() as conn:
            await conn.execute(
                "UPDATE connector_driver_identities SET egress_resolved_at = $2 "
                "WHERE id = $1 AND egress IS NOT NULL",
                UUID(str(row["id"])),
                self.clock(),
            )
        return None

    async def _sync_endpoints(self, specs: Mapping[str, DriverSpec]) -> None:
        """One endpoint Service per connector and digest with a live pod,
        pointing at the pod that serves now; the rest are deleted."""
        async with self.store.acquire() as conn:
            rows = await conn.fetch(_POD_ROWS + " AND revoked_at IS NULL")
        groups: dict[tuple[str, str], list[Mapping[str, Any]]] = {}
        for row in rows:
            if str(row["driver"]) in specs:
                key = (str(row["connector_id"]), str(row["image_digest"]))
                groups.setdefault(key, []).append(row)
        desired: dict[str, dict] = {}
        for (connector_id, digest), group in groups.items():
            serving = _serving(group)
            spec = specs[str(serving["driver"])]
            body = endpoint_service(
                connector_id=connector_id,
                digest=digest,
                identity_id=str(serving["id"]),
                port=spec.service.port,
                namespace=self.settings.namespace,
            )
            desired[body["metadata"]["name"]] = body
        try:
            existing = set(await self.runtime.endpoint_services())
        except Exception as exc:
            logger.warning("Endpoint Services not listed (%s)", exc)
            existing = set()
        for name, body in desired.items():
            try:
                await self.runtime.sync_endpoint(body)
            except (ServiceRuntimeError, ServiceCapacityError) as exc:
                logger.warning("Endpoint Service %s not synced (%s)", name, exc)
        for name in sorted(existing - set(desired)):
            try:
                await self.runtime.delete_endpoint(name)
            except ServiceRuntimeError as exc:
                logger.warning("Endpoint Service %s not deleted (%s)", name, exc)

    async def _sync_binding_policies(
        self,
        bindings: Mapping[tuple[str, str], _Binding],
        specs: Mapping[str, DriverSpec],
    ) -> None:
        """One ingress policy per binding of a workspace-facing driver's pod.

        A superseded pod keeps admitting its connector's bindings while it
        drains: SRW does not know which pod a binding's workspace still uses.
        """
        async with self.store.acquire() as conn:
            rows = await conn.fetch(_POD_ROWS + " AND revoked_at IS NULL")
        policy = self.settings.launch_policy("0.0.0.0")
        for row in rows:
            spec = specs.get(str(row["driver"]))
            if spec is None or "workspace" not in spec.service.callers:
                continue
            identity = _identity(row)
            binding = bindings.get((identity.connector_id, identity.digest))
            desired: dict[str, dict] = {}
            for kind, owner in sorted(binding.owners if binding else ()):
                body = binding_ingress_policy(
                    identity, kind=kind, owner_id=owner, policy=policy
                )
                desired[body["metadata"]["name"]] = body
            try:
                await self.runtime.sync_binding_policies(identity, desired)
            except (ServiceRuntimeError, ServiceCapacityError) as exc:
                logger.warning(
                    "Driver pod %s: binding policies not synced (%s)",
                    identity.pod_name,
                    exc,
                )

    async def _sweep(self) -> int:
        """Delete managed objects no unremoved row names."""
        async with self.store.acquire() as conn:
            known = {
                str(row["id"])
                for row in await conn.fetch(
                    "SELECT id FROM connector_driver_identities "
                    "WHERE credential_generation IS NOT NULL AND removed_at IS NULL"
                )
            }
        swept = 0
        for delete, name, identity in await self.runtime.managed_objects():
            if identity in known:
                continue
            try:
                await self.runtime.delete_object(delete, name)
                swept += 1
            except ServiceRuntimeError as exc:
                logger.warning("Orphaned driver object %s: %s", name, exc)
        return swept


def _repository(reference: str) -> str:
    """The repository of an image reference (no tag, no digest)."""
    from shared.connectors.images import ImageReference

    return ImageReference.parse(reference).name


async def connector_service_reconciler(
    shutdown_event: asyncio.Event,
    *,
    build: Callable[[], ServiceHostingReconciler | None],
    interval_seconds: float,
) -> None:
    """Leader-gated loop: reconcile service pods every ``interval_seconds``.

    ``build`` returns ``None`` while Kubernetes is unavailable. A failed pass
    is logged and the next one runs on time; every step re-reads durable
    state, so a pass a leadership change cancels is run again by the next
    leader.
    """
    logger.info("Connector service reconciler started (every %.0fs)", interval_seconds)
    while not shutdown_event.is_set():
        started = time.monotonic()
        try:
            reconciler = build()
            if reconciler is not None:
                report = await reconciler.reconcile_once()
                if report:
                    logger.info(
                        "connector service pods: started=%d ready=%d stopped=%s "
                        "removed=%d refused=%d capacity=%d swept=%d",
                        len(report.started),
                        len(report.ready),
                        report.stopped,
                        len(report.removed),
                        len(report.refused),
                        report.capacity,
                        report.swept,
                    )
        except Exception as exc:
            logger.warning("connector service reconcile error (non-fatal): %s", exc)
        wait = max(1.0, interval_seconds - (time.monotonic() - started))
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=wait)
            break
        except asyncio.TimeoutError:
            pass
    logger.info("Connector service reconciler stopped")


async def revoke_unhosted_identities(store: Any) -> list[str]:
    """Hosting is off: revoke every live service-pod identity.

    No reconciler runs to stop these pods, so the lease exchange must refuse
    them now. Database only: their objects stay in the driver namespace
    (which the chart keeps, ``helm.sh/resource-policy: keep``) until hosting
    is turned back on, when the reconciler's first pass deletes the objects
    of every revoked row, or until an operator deletes the namespace.
    """
    async with store.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id FROM connector_driver_identities "
            "WHERE credential_generation IS NOT NULL AND revoked_at IS NULL"
        )
        revoked: list[str] = []
        for row in rows:
            async with conn.transaction():
                revoked += await revoke_driver_identity(
                    conn, identity_id=str(row["id"]), reason=HOSTING_DISABLED
                )
    if revoked:
        logger.warning(
            "Service-pod hosting is off: revoked %d driver pod identities; their "
            "pods stay in the driver namespace until hosting is on again or the "
            "namespace is deleted",
            len(revoked),
        )
    return revoked


async def connector_service_identity_revoker(
    shutdown_event: asyncio.Event,
    *,
    store: Any,
    interval_seconds: float = 60.0,
) -> None:
    """Hosting is off in this process: revoke live service-pod identities
    now and every ``interval_seconds``.

    On every replica, not leader-gated (idempotent). During a rollout that
    turns hosting off, an older replica that still hosts may start pods with
    fresh identities; this keeps revoking them, and the exchange of a replica
    with hosting off refuses them meanwhile.
    """
    while not shutdown_event.is_set():
        try:
            await revoke_unhosted_identities(store)
        except Exception as exc:
            logger.warning("Revoking unhosted driver pod identities failed: %s", exc)
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=interval_seconds)
            break
        except asyncio.TimeoutError:
            pass


async def connector_egress_view(
    store: Any, connector_id: str, *, spec: DriverSpec | None
) -> dict[str, Any]:
    """What one connector's driver pods enforce: their pinned egress."""
    async with store.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, image_digest, ready_at, egress, egress_resolved_at,
                   created_at, revoked_at, revoke_reason, launch_error
              FROM connector_driver_identities
             WHERE connector_id = $1 AND credential_generation IS NOT NULL
               AND (revoked_at IS NULL OR revoked_at > now() - interval '1 day')
             ORDER BY created_at DESC
             LIMIT 10
            """,
            UUID(str(connector_id)),
        )
    pods = []
    for row in rows:
        egress = row["egress"]
        if isinstance(egress, str):
            egress = json.loads(egress)
        pods.append(
            {
                "identity_id": str(row["id"]),
                "image_digest": row["image_digest"],
                "live": row["revoked_at"] is None,
                "ready": row["ready_at"] is not None,
                "stopped_reason": row["revoke_reason"],
                "launch_error": row["launch_error"],
                "enforced": egress,
                "resolved_at": (
                    row["egress_resolved_at"].isoformat()
                    if row["egress_resolved_at"]
                    else None
                ),
            }
        )
    return {
        "connector_id": str(connector_id),
        "driver": spec.name if spec else None,
        "plane": spec.plane if spec else None,
        "declared": {
            "rules": [
                {
                    "host": rule.host,
                    "ports": list(rule.ports),
                    "protocol": rule.protocol,
                }
                for rule in (spec.egress if spec else ())
            ],
            "needs_dns": spec.needs_dns if spec else None,
        },
        "pods": pods,
    }


__all__ = [
    "CAPACITY",
    "EGRESS_REPINNED",
    "EGRESS_WITHDRAWN",
    "HOSTING_DISABLED",
    "HOSTING_REFUSED",
    "IDLE",
    "LAUNCH_FAILED",
    "LAUNCH_REFUSED",
    "NOT_READY",
    "POD_LOST",
    "START_TIMEOUT",
    "PodState",
    "ReconcileReport",
    "ServiceCapacityError",
    "ServiceHostingReconciler",
    "ServiceHostingSettings",
    "ServicePodRuntime",
    "ServiceRuntimeError",
    "connector_egress_view",
    "connector_service_identity_revoker",
    "connector_service_reconciler",
    "credential_generation",
    "egress_withdrawn",
    "revoke_unhosted_identities",
]
