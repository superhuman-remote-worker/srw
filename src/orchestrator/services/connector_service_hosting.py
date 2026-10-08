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
   the connector's projects lost private addresses, its declared egress
   (hosts, ports) changed, or an address it pinned is one the installation
   refuses now (a range added to ``refusedCidrs``); it does not drain;
5. marks a pod idle when its last binding ends, or when a newer generation of
   its connector supersedes it for a digest or credential change (it drains
   its bindings for the idle time), and stops it after ``idleSeconds``: the
   identity is revoked first, so the exchange refuses it at once, then the
   objects are deleted and their absence recorded. A lease driver's
   generation leaves ``access`` out: its pod applies each lease's access,
   so an access change starts no pod;
6. re-resolves a serving pod's pinned hosts every ``reresolveSeconds``
   (D5a): a shared pod with live bindings never idles, so it would never
   pick up an upstream that moved. When the same changed answer comes
   twice in a row (a pool answering a rotating subset, overlapping the
   pinned addresses or not, does not), a replacement starts for the same key with the new policy and
   hostAliases (the old pod is marked ``replaced_at`` and keeps serving),
   and the old pod stops ``repinDrainSeconds`` after the replacement is
   ready and its endpoint Service names it. A replacement that does not
   start waits another interval;
7. keeps one ingress policy per binding for a workspace-facing driver,
   admitting only that binding's workspace;
8. keeps one endpoint Service per connector and digest with a live pod,
   pointing at the pod that serves now (the newest ready one of the
   connector's current generation, else the newest ready one): callers
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
from orchestrator.services.connector_drivers.base import SupportsServiceConnector
from orchestrator.services.connector_egress import (
    EgressPins,
    EgressPolicy,
    EgressRefused,
    expand_rule,
    pin_egress,
    private_addresses_allowed,
    refusal,
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
    endpoint_service_name,
    pod_config,
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
#: The driver reported at start that its upstream is unreachable or does not
#: verify (exit code :data:`UPSTREAM_EXIT_CODE` and a termination message):
#: the git swap driver (C3).
UPSTREAM_UNREACHABLE = "upstream_unreachable"
UPSTREAM_EXIT_CODE = 78
#: Stops that back the key off before the next start.
_BACKOFF_REASONS = (
    LAUNCH_REFUSED,
    LAUNCH_FAILED,
    START_TIMEOUT,
    CAPACITY,
    UPSTREAM_UNREACHABLE,
)
#: A pod stopped to make room for a new one at the installation's cap: the
#: longest-idle pod no binding uses (C3 re-review S7).
IDLE_EVICTED = "idle_evicted"
#: A pod of an earlier credential generation (its connector's config or
#: tier changed) stopped once its successor serves: ``repinDrainSeconds``
#: after the successor turned ready and the endpoint Service names it, or at
#: once when the installation's cap leaves its successor no other room.
SUPERSEDED = "superseded"
#: The reconciler's LISTEN connection: a liveness check this often, and the
#: wait before opening it again after it was lost (doubling up to the most).
LISTEN_CHECK_SECONDS = 5.0
LISTEN_RETRY_SECONDS = 1.0
LISTEN_RETRY_MAX_SECONDS = 30.0
#: The running reconciler loop's wake-up (one per process): a delivery that
#: issues a new service binding asks for a pass now (C3 review S1).
_WAKE: dict[str, asyncio.Event] = {}
#: The channel a delivery NOTIFYs when it issues a new service binding: sent
#: when its transaction commits, never on a rollback, and heard by the
#: replica whose loop LISTENs (the leader).
RECONCILE_CHANNEL = "srw_connector_service_reconcile"
#: How long a woken pass waits first (a few notifications in a burst make
#: one pass).
WAKE_SETTLE_SECONDS = 0.5


def request_reconcile() -> None:
    """Ask this process's reconciler loop for a pass now (no-op where the
    loop does not run: another replica leads, or hosting is off)."""
    event = _WAKE.get("event")
    if event is not None:
        event.set()


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
    #: The driver container's report that its upstream is unreachable or
    #: untrusted (it exited with :data:`UPSTREAM_EXIT_CODE`), else ``None``.
    upstream: str | None = None

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


def _upstream_failure(driver_status: Any, limit: int = 300) -> str | None:
    """The driver container's own report that it cannot use its upstream:
    it exited with :data:`UPSTREAM_EXIT_CODE` (now or last time), and its
    termination message says why. Any other exit is the start timeout's."""
    from orchestrator.services.connector_git_swap_delivery import clean_detail

    for key in ("state", "lastState"):
        terminated = _field(_field(driver_status, key), "terminated")
        if terminated is None or _field(terminated, "exitCode") != UPSTREAM_EXIT_CODE:
            continue
        lines = [
            line.strip()
            for line in str(_field(terminated, "message") or "").splitlines()
            if line.strip()
        ]
        # The driver's own words, but the upstream may shape parts of them:
        # no control characters, capped (it is stored and logged, never
        # delivered: deliveries show a fixed reason).
        text = clean_detail(lines[-1] if lines else "", limit)
        return text or "the driver cannot use its upstream"
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
            upstream=_upstream_failure(driver),
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

    async def endpoint_target(self, name: str) -> str | None:
        """The pod identity an endpoint Service's selector names now, or
        ``None`` when the Service does not exist."""
        try:
            current = await run_bounded_k8s_call(
                self.core_api.read_namespaced_service,
                name=name,
                namespace=self.namespace,
            )
        except Exception as exc:
            if _status(exc) == 404:
                return None
            raise ServiceRuntimeError(f"reading {name} failed") from None
        selector = _field(_field(current, "spec"), "selector") or {}
        target = dict(selector).get("srw.io/driver-identity")
        return str(target) if target else None

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
    #: SRW's managed MCP front image, pinned by digest (D5a); a managed MCP
    #: driver's pod is refused without it.
    front_image: str = ""
    #: SRW's connector driver certificate authority (C3); a TLS driver's pod
    #: is refused without it.
    driver_ca: Any = None

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
            front_image=self.front_image,
            driver_ca=self.driver_ca,
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
    does not (its pod exchanges each binding's lease), nor its ``access``
    (each lease carries its own, see :func:`pod_config`). The egress tier is
    in it too, so a project tier change starts a pod with the new policy.
    """
    from orchestrator.security.crypto import credential_fingerprint

    held = spec.credential_delivery != "lease"
    canonical = json.dumps(
        {
            "driver": spec.name,
            "config": pod_config(spec, connector.get("config")),
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
    policy: EgressPolicy | None = None,
) -> str | None:
    """Why a running pod's pinned egress opens more than is allowed now.

    A pod's policy is fixed when it starts. It may not keep private addresses
    its connector's projects lost, nor destinations its connector's config no
    longer declares, nor (given the installation's current ``policy``) an
    address that policy refuses now, such as one in a range added to
    ``refusedCidrs``: such a pod stops at once instead of draining. ``None``
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
    if policy is not None:
        for item in recorded.get("hosts") or ():
            for address in item.get("addresses") or ():
                try:
                    network = ipaddress.ip_network(str(address), strict=False)
                except ValueError:
                    return f"its pinned address {address!r} is not an address"
                if network.version == 6 and not policy.ipv6:
                    return (
                        f"its pinned address {address} for {item.get('host')} is "
                        "IPv6, and this cluster pins IPv4 only now"
                    )
                reason = refusal(network, policy)
                if reason:
                    return (
                        f"its pinned address {address} for {item.get('host')} "
                        f"{reason} now"
                    )
    return None


def _row_value(row: Any, name: str) -> Any:
    """A column a row may lack (a fake row in a test, an older query)."""
    try:
        return row[name]
    except (KeyError, IndexError):
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


def _serving(
    rows: list[Mapping[str, Any]], generation: str | None = None
) -> Mapping[str, Any]:
    """The pod a connector's endpoint at one digest points at.

    The newest ready pod of the connector's current ``generation`` not being
    replaced, else the newest ready one of it; failing those, the newest
    ready pod of any generation not being replaced, the newest ready one,
    and last the newest (which gets traffic once it is ready). A pod of the
    current generation wins over a newer superseded one: after a config
    change and back, the older pod is the current one again. Within a
    generation newer pods supersede older ones (a re-pin's replacement, a
    lost pod's).
    """

    def newest(candidates: list[Mapping[str, Any]]) -> Mapping[str, Any] | None:
        return max(candidates, key=lambda row: row["created_at"], default=None)

    ready = [row for row in rows if row["ready_at"] is not None]
    current = [
        row
        for row in ready
        if generation is not None and str(row["credential_generation"]) == generation
    ]
    return (
        newest([row for row in current if row["replaced_at"] is None])
        or newest(current)
        or newest([row for row in ready if row["replaced_at"] is None])
        or newest(ready)
        or newest(rows)
    )


def _row_key(row: Mapping[str, Any]) -> tuple[str, str]:
    return (str(row["connector_id"]), str(row["image_digest"]))


def ready_successors(
    live: Iterable[Mapping[str, Any]], generations: Mapping[tuple[str, str], str]
) -> dict[tuple[str, str], Mapping[str, Any]]:
    """For each bound connector and digest (``generations``: its current
    credential generation), the ready pod of that generation the endpoint
    Service names; no entry while none of that generation is ready."""
    groups: dict[tuple[str, str], list[Mapping[str, Any]]] = {}
    for row in live:
        key = _row_key(row)
        generation = generations.get(key)
        if (
            generation is not None
            and row["ready_at"] is not None
            and str(row["credential_generation"]) == generation
        ):
            groups.setdefault(key, []).append(row)
    return {key: _serving(rows, generations[key]) for key, rows in groups.items()}


def drain_successor(
    row: Mapping[str, Any],
    generations: Mapping[tuple[str, str], str],
    successors: Mapping[tuple[str, str], Mapping[str, Any]],
) -> Mapping[str, Any] | None:
    """The pod a superseded ``row`` drains to: a bound key's pod of an
    earlier generation drains only once its current generation's pod is
    ready (``successors``). Until then it keeps serving, whatever the drain
    time: its successor may still be starting, or failing (an upstream CA
    that does not verify)."""
    key = _row_key(row)
    current = generations.get(key)
    if current is None or current == str(row["credential_generation"]):
        return None
    return successors.get(key)


def evictable_pods(
    survivors: Iterable[Mapping[str, Any]],
    active_ids: set[str],
    bindings: Mapping[tuple[str, str], Any],
) -> list[Mapping[str, Any]]:
    """The pods a new pod may take the place of at the installation's cap,
    longest idle first: idle, and no live binding of their connector and
    digest (a superseded pod drains its key's bindings: it gives way only to
    its own successor, :func:`superseded_pods`)."""
    return sorted(
        (
            row
            for row in survivors
            if str(row["id"]) not in active_ids
            and row["idle_since"] is not None
            and _row_key(row) not in bindings
        ),
        key=lambda row: row["idle_since"],
    )


def superseded_pods(
    survivors: Iterable[Mapping[str, Any]],
    generations: Mapping[tuple[str, str], str],
) -> dict[tuple[str, str], list[Mapping[str, Any]]]:
    """Each bound key's live pods of an earlier generation, oldest first:
    what that key's successor may take the place of at the cap."""
    found: dict[tuple[str, str], list[Mapping[str, Any]]] = {}
    for row in survivors:
        key = _row_key(row)
        current = generations.get(key)
        if current is not None and current != str(row["credential_generation"]):
            found.setdefault(key, []).append(row)
    for rows in found.values():
        rows.sort(key=lambda row: row["created_at"])
    return found


def _identity(row: Mapping[str, Any]) -> ServicePodIdentity:
    return ServicePodIdentity(
        identity_id=str(row["id"]),
        connector_id=str(row["connector_id"]),
        driver=str(row["driver"]),
        digest=str(row["image_digest"]),
        generation=str(row["credential_generation"]),
    )


_LIVE_BINDINGS = """
SELECT connector_id, driver, image_digest, job_id, thread_id, issued_at
  FROM connector_credential_leases
 WHERE revoked_at IS NULL AND expires_at > now()
   AND image_digest IS NOT NULL AND driver = ANY($1::text[])
"""
#: Whether a pod's key has a live binding now (an eviction candidate is read
#: again, under the capacity lock, just before it is stopped).
_KEY_BOUND = """
SELECT EXISTS (
  SELECT 1 FROM connector_credential_leases
   WHERE revoked_at IS NULL AND expires_at > now()
     AND connector_id = $1 AND image_digest = $2
)
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
    #: When each owner's lease was issued (the earliest of its leases).
    issued: dict[tuple[str, str], datetime] = field(default_factory=dict)

    def bound_by(self, kind: str, owner: str, issued_at: Any) -> None:
        self.owners.add((kind, owner))
        if isinstance(issued_at, datetime):
            known = self.issued.get((kind, owner))
            if known is None or issued_at < known:
                self.issued[(kind, owner)] = issued_at


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
        strikes: dict[str, Any] | None = None,
    ) -> None:
        self.store = store
        self.runtime = runtime
        self.drivers = drivers
        self.settings = settings
        self.resolver = resolver
        self.clock = clock
        #: What one pass remembers for the next, shared across the per-pass
        #: reconcilers of one loop: ``"cluster"``, the passes in a row the
        #: cluster check failed; ``"repin:<identity>"``, the changed answer
        #: a pod's last re-resolution saw. A new leader starts afresh, which
        #: only delays a re-pin by an interval.
        self.strikes = strikes if strikes is not None else {}
        #: This pass's pods no binding uses, longest idle first: what a new
        #: pod may take the place of at the installation's cap.
        self._evictable: list[Mapping[str, Any]] = []
        #: This pass's pods of an earlier generation, by connector and
        #: digest: what that key's successor may take the place of.
        self._superseded: dict[tuple[str, str], list[Mapping[str, Any]]] = {}
        #: Stopped pods still terminating: each frees a slot soon, so a start
        #: at the cap waits for it instead of stopping another pod.
        self._freeing = 0

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
            issued_at = _row_value(row, "issued_at")
            if row["job_id"] is not None:
                binding.bound_by("job", str(row["job_id"]), issued_at)
            if row["thread_id"] is not None:
                binding.bound_by("thread", str(row["thread_id"]), issued_at)
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

    async def _make_room(self, binding: _Binding, report: ReconcileReport) -> bool:
        """At the installation's cap: stop one pod for a new pod of
        ``binding``'s key. Whether one stopped.

        A pod stopped earlier and still terminating frees its slot soon:
        this start waits for it rather than stopping another (counting a
        terminating pod as gone only once its objects are, every pass would
        evict one more). Else the key's own pod of an earlier generation
        gives way to its successor (its bindings move to the successor with
        the endpoint Service), else the longest-idle pod no binding uses.
        """
        if self._freeing > 0:
            self._freeing -= 1
            logger.info(
                "At the installation's cap: driver pod for connector %s waits "
                "for a stopped pod's slot",
                binding.connector_id,
            )
            return False
        key = (binding.connector_id, binding.digest)
        for victim in self._superseded.pop(key, []):
            if await self._evict(victim, SUPERSEDED, report, bound_ok=True):
                return True
        while self._evictable:
            victim = self._evictable.pop(0)
            if await self._evict(victim, IDLE_EVICTED, report, bound_ok=False):
                return True
        return False

    async def _evict(
        self,
        row: Mapping[str, Any],
        reason: str,
        report: ReconcileReport,
        *,
        bound_ok: bool,
    ) -> bool:
        """Stop ``row`` to make room, under the capacity lock: the pass read
        it unbound, but a delivery may have bound its key since, so the
        revoke happens only while no live lease binds its connector and
        digest (``bound_ok``: a superseded pod giving way to its successor,
        whose key is bound by definition). Whether it stopped."""
        async with self.store.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                    _CAPACITY_LOCK,
                )
                if not bound_ok and await conn.fetchval(
                    _KEY_BOUND,
                    UUID(str(row["connector_id"])),
                    str(row["image_digest"]),
                ):
                    # Bound since the pass read it (for a stdio managed MCP
                    # server, D5b, the pod the binding's process starts in):
                    # spared, the next candidate makes room.
                    return False
                revoked = await revoke_driver_identity(
                    conn, identity_id=str(row["id"]), reason=reason
                )
                if not revoked:
                    return False  # stopped meanwhile
                await conn.execute(
                    "UPDATE connector_driver_identities SET idle_since = NULL "
                    "WHERE id = $1",
                    UUID(str(row["id"])),
                )
        logger.warning(
            "At the installation's cap: stopped driver pod %s (%s) to make room",
            row["pod_name"],
            reason,
        )
        report.stopped.append((str(row["id"]), reason))
        await self._remove(row, report)
        return True

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
            if replaces is not None or not await self._make_room(binding, report):
                report.capacity += 1
                logger.warning(
                    "Driver pod for connector %s not started: %s",
                    binding.connector_id,
                    exc,
                )
                return
            try:
                claimed = await self._claim(
                    spec, binding, generation, reference, replaces=replaces
                )
            except ServiceCapacityError:
                # The stopped pod's row counts until its objects are gone:
                # a later pass starts this one (and stops no other meanwhile).
                report.capacity += 1
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

    def _policy(self, private_allowed: bool) -> EgressPolicy:
        """What a pod's egress may reach on this installation now."""
        return EgressPolicy.build(
            self.settings.cluster_cidrs,
            allow_private=private_allowed,
            ipv6=self.settings.ipv6,
            refused_cidrs=self.settings.refused_cidrs,
        )

    async def _pin(
        self, spec: DriverSpec, config: Mapping[str, Any], *, private_allowed: bool
    ) -> EgressPins:
        """Resolve and check the spec's egress for one connector's config."""
        return await pin_egress(
            spec.egress,
            config,
            policy=self._policy(private_allowed),
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
        if not state.ready and state.upstream is not None:
            # The driver said at start that it cannot reach or trust its
            # upstream: stop now, back the key off, and let deliveries fall
            # back instead of waiting out the start timeout.
            logger.warning(
                "Driver pod %s cannot use its upstream (%s); stopping it",
                identity.pod_name,
                state.upstream,
            )
            await self._stop(row, UPSTREAM_UNREACHABLE, report, error=state.upstream)
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
        self,
        row: Mapping[str, Any],
        *,
        bound: bool,
        report: ReconcileReport,
        successor: Mapping[str, Any] | None = None,
    ) -> bool:
        """Track a pod's idleness; stop it after the idle timeout.

        A pod with a ``successor`` (the ready pod of its connector's newer
        generation: a config change, an upstream CA, a tier) drains for
        ``repinDrainSeconds`` from the successor's readiness, as a re-pin's
        old pod does, and stops only once the endpoint Service is seen to
        name another pod; whatever its idle time.
        """
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
        if successor is not None:
            ready_at = successor["ready_at"]
            if (
                ready_at is None
                or (self.clock() - ready_at).total_seconds()
                < self.settings.repin_drain_seconds
            ):
                return True
            target = await self._endpoint_target(row)
            if target is None or target == str(row["id"]):
                logger.warning(
                    "Driver pod %s keeps serving: its endpoint Service does not "
                    "name its successor yet",
                    row["pod_name"],
                )
                return True
            await self._stop(row, SUPERSEDED, report)
            return False
        if idle_since is None:
            return True
        if (self.clock() - idle_since).total_seconds() >= self._idle_seconds(row):
            await self._stop(row, IDLE, report)
            return False
        return True

    def _idle_seconds(self, row: Mapping[str, Any]) -> float:
        """How long a pod of this driver stays without bindings: the
        installation's idle time, or the driver's own when longer (the git
        swap driver: a first clone otherwise waits for a cold pod again after
        every idle stretch)."""
        spec = self._service_specs().get(str(row["driver"]))
        own = spec.service.idle_seconds if spec is not None else None
        return max(float(self.settings.idle_seconds), float(own or 0))

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
        self._freeing = sum(
            1
            for row in rows
            if row["revoked_at"] is not None and str(row["id"]) not in report.removed
        )
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
        connectors: dict[tuple[str, str], tuple[Mapping[str, Any] | None, bool]] = {}

        async def connector_state(
            connector_id: str, driver: str
        ) -> tuple[Mapping[str, Any] | None, bool]:
            key = (connector_id, driver)
            if key not in connectors:
                connector = await self.store.get_datasource(connector_id)
                serving = self.drivers.get(driver)
                if connector is not None and isinstance(
                    serving, SupportsServiceConnector
                ):
                    # A variant serving some rows of a stored type (the git
                    # swap) builds its pods from what it derives from the row.
                    connector = serving.service_connector(connector)
                async with self.store.acquire() as conn:
                    private = await private_addresses_allowed(
                        conn, connector_id, private_tiers=self.settings.private_tiers
                    )
                connectors[key] = (connector, private)
            return connectors[key]

        # The connector's current generation for each bound (connector, digest).
        current: dict[tuple[str, str], tuple[str, bool, Mapping[str, Any]]] = {}
        for key, binding in bindings.items():
            spec = specs.get(binding.driver)
            connector, private = await connector_state(
                binding.connector_id, binding.driver
            )
            if spec is None or connector is None:
                continue
            current[key] = (
                credential_generation(spec, connector, private_allowed=private),
                private,
                connector,
            )

        # The ready pod of each key's current generation: a superseded pod of
        # such a key drains from its readiness, not for the idle time.
        generations = {key: generation for key, (generation, _, _) in current.items()}
        successors = ready_successors(live, generations)
        survivors: list[Mapping[str, Any]] = []
        active_ids: set[str] = set()
        for row in live:
            spec = specs.get(str(row["driver"]))
            if spec is None:
                await self._stop(row, IDLE, report)  # the driver was uninstalled
                continue
            connector, private = await connector_state(
                str(row["connector_id"]), str(row["driver"])
            )
            withdrawn = egress_withdrawn(
                spec,
                connector,
                row["egress"],
                private_allowed=private,
                policy=self._policy(private),
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
            if await self._settle_idle(
                row,
                bound=active,
                report=report,
                successor=drain_successor(row, generations, successors),
            ):
                survivors.append(row)
                if active:
                    active_ids.add(str(row["id"]))
        # At the installation's cap, a new pod takes the place of the
        # longest-idle pod no binding uses (S7): one user's idle pods may
        # not keep everyone else's repositories on the fallback; a key's
        # successor takes its superseded pod's.
        self._evictable = evictable_pods(survivors, active_ids, bindings)
        self._superseded = superseded_pods(survivors, generations)

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
        await self._sync_binding_policies(bindings, specs, generations)
        await self._sync_endpoints(specs, generations)
        report.swept = await self._sweep()
        return report

    def _repin_due(self, row: Mapping[str, Any]) -> bool:
        interval = self.settings.reresolve_seconds
        resolved = row["egress_resolved_at"]
        if interval <= 0 or resolved is None:
            return False
        return (self.clock() - resolved).total_seconds() >= interval

    async def _endpoint_target(self, row: Mapping[str, Any]) -> str | None:
        """The identity the endpoint Service of a pod's connector and digest
        names now; ``None`` when it is missing or cannot be read."""
        name = endpoint_service_name(str(row["connector_id"]), str(row["image_digest"]))
        try:
            return await self.runtime.endpoint_target(name)
        except Exception as exc:
            logger.warning("Endpoint Service %s not read (%s)", name, exc)
            return None

    async def _repin(
        self,
        survivors: list[Mapping[str, Any]],
        active_ids: set[str],
        specs: Mapping[str, DriverSpec],
        *,
        connector_state: Callable[[str, str], Any],
        exchange_address: str,
        report: ReconcileReport,
    ) -> None:
        """Re-resolve the pinned hosts of the pods that serve bindings, and
        replace a pod whose upstream moved.

        A shared pod with live bindings never goes idle, so it never rolls
        on its own: an upstream that moved would stay unreachable. Every
        ``reresolveSeconds`` its hosts are resolved again. A replacement
        starts for the same key with the new policy and hostAliases when the
        same changed answer comes twice in a row: a pool answering a
        rotating subset (sharing a pinned address or not) replaces nothing,
        so DNS round-robin does not roll the pod every interval, and an
        upstream that really moved is followed one interval later.
        The endpoint Service moves to the replacement once it is ready, and
        the old pod stops ``repinDrainSeconds`` later, once the Service is
        seen to name another pod (if moving it failed, the old pod keeps
        serving). The old pod serves throughout, so a caller loses at most
        the requests in flight at the switch (and a stateful MCP session,
        which its client starts again). A host that no longer resolves, or
        resolves to an address SRW refuses, keeps the addresses already
        pinned; a replacement that could not start waits another interval.
        """
        live = {str(row["id"]) for row in survivors if row["replaced_at"] is None}
        for key in [k for k in self.strikes if k.startswith("repin:")]:
            if key.removeprefix("repin:") not in live:
                self.strikes.pop(key, None)
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
                if replacement is None or (
                    (self.clock() - replacement["ready_at"]).total_seconds()
                    < self.settings.repin_drain_seconds
                ):
                    continue
                target = await self._endpoint_target(row)
                if target is None or target == str(row["id"]):
                    logger.warning(
                        "Driver pod %s keeps serving: its endpoint Service does "
                        "not name its replacement yet",
                        row["pod_name"],
                    )
                    continue
                await self._stop(row, EGRESS_REPINNED, report)
                continue
            if (
                str(row["id"]) not in active_ids
                or row["ready_at"] is None
                or not self._repin_due(row)
            ):
                continue
            spec = specs[str(row["driver"])]
            connector, private = await connector_state(
                str(row["connector_id"]), str(row["driver"])
            )
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
        """The pod's pins resolved again when its upstream moved; otherwise
        ``None``.

        Moved: the answer differs from what was pinned and equals the one
        the last re-resolution saw. The resolution time is recorded either way, so a
        replacement that does not start is tried again an interval later,
        not every pass.
        """
        recorded = row["egress"]
        if isinstance(recorded, str):
            recorded = json.loads(recorded)
        pinned = _address_sets(recorded)
        sighting = f"repin:{row['id']}"
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
        moved: EgressPins | None = None
        if pins is not None:
            fresh = _address_sets(pins.record())
            if fresh == pinned:
                self.strikes.pop(sighting, None)
            elif self.strikes.get(sighting) == fresh:
                # Kept until the pod is replaced (then pruned): a start that
                # fails is tried again an interval later.
                moved = pins
            else:
                # A pool may answer a rotating subset, even one sharing no
                # pinned address, while the pinned ones still serve: wait
                # for the same answer twice before rolling a pod that
                # serves.
                self.strikes[sighting] = fresh
                logger.info(
                    "Driver pod %s: a pinned host answers other addresses; "
                    "replacing the pod if the next resolution agrees",
                    row["pod_name"],
                )
        async with self.store.acquire() as conn:
            await conn.execute(
                "UPDATE connector_driver_identities SET egress_resolved_at = $2 "
                "WHERE id = $1 AND egress IS NOT NULL",
                UUID(str(row["id"])),
                self.clock(),
            )
        return moved

    async def _sync_endpoints(
        self,
        specs: Mapping[str, DriverSpec],
        generations: Mapping[tuple[str, str], str] | None = None,
    ) -> None:
        """One endpoint Service per connector and digest with a live pod,
        pointing at the pod that serves now (``generations``: each bound
        connector and digest's current credential generation); the rest are
        deleted."""
        generations = generations or {}
        async with self.store.acquire() as conn:
            rows = await conn.fetch(_POD_ROWS + " AND revoked_at IS NULL")
        groups: dict[tuple[str, str], list[Mapping[str, Any]]] = {}
        for row in rows:
            if str(row["driver"]) in specs:
                key = (str(row["connector_id"]), str(row["image_digest"]))
                groups.setdefault(key, []).append(row)
        desired: dict[str, dict] = {}
        for (connector_id, digest), group in groups.items():
            serving = _serving(group, generations.get((connector_id, digest)))
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
        generations: Mapping[tuple[str, str], str] | None = None,
    ) -> None:
        """One ingress policy per binding of a workspace-facing driver's pod.

        A superseded pod (its connector has a newer generation: ``generations``)
        keeps admitting the bindings it served before it was superseded while
        it drains (SRW does not know which pod such a workspace still uses),
        and admits no binding issued after.
        """
        async with self.store.acquire() as conn:
            rows = await conn.fetch(_POD_ROWS + " AND revoked_at IS NULL")
        policy = self.settings.launch_policy("0.0.0.0")
        for row in rows:
            spec = specs.get(str(row["driver"]))
            if spec is None or "workspace" not in spec.service.callers:
                continue
            identity = _identity(row)
            key = (identity.connector_id, identity.digest)
            binding = bindings.get(key)
            owners = sorted(binding.owners if binding else ())
            current = (generations or {}).get(key)
            since = _row_value(row, "idle_since")
            if (
                binding is not None
                and current is not None
                and current != identity.generation
                and isinstance(since, datetime)
            ):
                # Superseded: only what it served before the change.
                owners = [
                    owner
                    for owner in owners
                    if (issued := binding.issued.get(owner)) is not None
                    and issued <= since
                ]
            desired: dict[str, dict] = {}
            for kind, owner in owners:
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
    store: Any = None,
) -> None:
    """Leader-gated loop: reconcile service pods every ``interval_seconds``,
    and soon after a delivery that issued a new binding commits: it NOTIFYs
    :data:`RECONCILE_CHANNEL`, which this loop LISTENs on with one of
    ``store``'s connections (and :func:`request_reconcile` wakes it within
    this process).

    ``build`` returns ``None`` while Kubernetes is unavailable. A failed pass
    is logged and the next one runs on time; every step re-reads durable
    state, so a pass a leadership change cancels is run again by the next
    leader.
    """
    logger.info("Connector service reconciler started (every %.0fs)", interval_seconds)
    wake = asyncio.Event()
    _WAKE["event"] = wake
    listener = (
        asyncio.create_task(_listen(store, wake, shutdown_event))
        if store is not None
        else None
    )
    try:
        await _reconcile_loop(shutdown_event, build, interval_seconds, wake)
    finally:
        if listener is not None:
            listener.cancel()
            try:
                await listener
            except (asyncio.CancelledError, Exception):
                pass
        if _WAKE.get("event") is wake:
            _WAKE.pop("event", None)
    logger.info("Connector service reconciler stopped")


async def _until(events: Iterable[asyncio.Event], timeout: float) -> None:
    """Wait until one of ``events`` is set, or ``timeout`` seconds."""
    waiters = [asyncio.ensure_future(event.wait()) for event in events]
    try:
        await asyncio.wait(
            waiters, timeout=timeout, return_when=asyncio.FIRST_COMPLETED
        )
    finally:
        for waiter in waiters:
            waiter.cancel()


async def _listen(
    store: Any, wake: asyncio.Event, shutdown_event: asyncio.Event
) -> None:
    """Hold one LISTEN connection for the loop's life.

    A connection the server ends (a restart, a failover,
    ``pg_terminate_backend``) is seen at once by its termination listener,
    or within :data:`LISTEN_CHECK_SECONDS` by a ``SELECT 1``. It goes back
    to the pool (which replaces a closed one) and a new one LISTENs after
    :data:`LISTEN_RETRY_SECONDS`, doubling while opening fails. The
    interval pass stays the floor: it covers what the gap missed.
    """

    def heard(_connection: Any, _pid: int, _channel: str, _payload: str) -> None:
        wake.set()

    backoff = LISTEN_RETRY_SECONDS
    while not shutdown_event.is_set():
        lost = asyncio.Event()

        def gone(_connection: Any) -> None:
            lost.set()

        try:
            async with store.acquire() as conn:
                await asyncio.wait_for(
                    conn.add_listener(RECONCILE_CHANNEL, heard), timeout=10
                )
                watch = getattr(conn, "add_termination_listener", None)
                if callable(watch):
                    watch(gone)
                backoff = LISTEN_RETRY_SECONDS
                try:
                    while not shutdown_event.is_set() and not lost.is_set():
                        await _until((shutdown_event, lost), LISTEN_CHECK_SECONDS)
                        if shutdown_event.is_set() or lost.is_set():
                            break
                        await asyncio.wait_for(conn.fetchval("SELECT 1"), timeout=10)
                finally:
                    try:
                        await asyncio.wait_for(
                            conn.remove_listener(RECONCILE_CHANNEL, heard), timeout=5
                        )
                    except Exception:
                        pass  # a lost connection has no LISTEN to end
                    unwatch = getattr(conn, "remove_termination_listener", None)
                    if callable(watch) and callable(unwatch):
                        try:
                            unwatch(gone)
                        except Exception:
                            pass
            if lost.is_set():
                logger.warning(
                    "Connector service reconciler: the LISTEN connection was "
                    "lost; opening another"
                )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.warning(
                "Connector service reconciler: LISTEN connection failed; passes "
                "run on the interval until it is open again",
                exc_info=True,
            )
        if shutdown_event.is_set():
            break
        await _until((shutdown_event,), backoff)
        backoff = min(backoff * 2, LISTEN_RETRY_MAX_SECONDS)


async def _reconcile_loop(
    shutdown_event: asyncio.Event,
    build: Callable[[], ServiceHostingReconciler | None],
    interval_seconds: float,
    wake: asyncio.Event,
) -> None:
    while not shutdown_event.is_set():
        started = time.monotonic()
        wake.clear()
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
        stop = asyncio.ensure_future(shutdown_event.wait())
        woken = asyncio.ensure_future(wake.wait())
        try:
            done, _ = await asyncio.wait(
                {stop, woken}, timeout=wait, return_when=asyncio.FIRST_COMPLETED
            )
        finally:
            for waiter in (stop, woken):
                waiter.cancel()
        if stop in done or shutdown_event.is_set():
            break
        if woken in done:
            # A delivery asked (after its commit): one pass for a burst.
            try:
                await asyncio.wait_for(
                    shutdown_event.wait(), timeout=WAKE_SETTLE_SECONDS
                )
                break
            except asyncio.TimeoutError:
                pass


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
