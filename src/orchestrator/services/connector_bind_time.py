"""Bind-time image drivers: one pod per operation, returning data only (D6).

A connector of a registered bind-time image (``connector_drivers.registered``)
is bound once per workspace-owning execution (a Job, or a session thread: the
lease owner, ``connector_credential_leases.LeaseOwner``). The bind runs the
driver's ``bind`` in a short-lived pod (``connector_bind_time_launch``) whose
shim posts the driver's typed JSON lines to the result route on the lease
exchange's port. What the driver returns is data: a binding descriptor of
``env_file`` and ``credential_file`` entries, checked
(``shared.connectors.registration.image_binding_problems``), stored encrypted
on the binding row and delivered by SRW's own materializers in the agent,
keyed by delivery form, as an environment or credential-file connector's.
No driver image gets a shell in a workspace.

**When.** :func:`prepare_bind_time_bindings` runs before a delivery opens its
transaction (``prepare_lease_delivery``) and waits for a new bind at most
``wait_seconds``; the bind goes on in the background past that.
:func:`deliver_bind_time_entries` runs inside the delivery's transaction
(``deliver_connector_leases``): a bound binding fills its entry's
credentials; a pending one refuses the delivery for a retry
(:class:`BindTimePending`); a failed one refuses it with the driver's reason
(:class:`BindTimeRefused`), which the connector shows. A delivery path that
prepares nothing starts the bind there and is refused until it finishes.
Re-delivery (every claim, attach and pod recycle) reuses the binding: one
pod per execution and connector, not per turn.

**Versions.** Each bind resolves the registration's reference to a digest
(``connector_service_images.resolve_driver_image``: a digest pins, a tag
follows, an unreachable registry reuses the last digest). A digest new to
the connector is checked against its previous binding's image (else the
registration's spec): a moved tag whose spec no longer validates the stored
config, drops a credential slot or changes the protocol major is refused
with "the image behind this tag changed its contract" and audited, and no
pod runs. The binding records ``{reference, digest, resolved_at, spec_hash,
protocol_version}``: the per-execution record of the bind (the immutable
execution snapshot is written at admission, before any bind).

**Revocation.** The leader's :func:`bind_time_reconciler` marks a bound
binding for revocation once its execution is terminal or gone or its
connector was deleted, runs the driver's ``revoke`` with the stored
``driver_state`` (best effort: an ``unsupported`` or any non-transient error
retires it with the reason recorded), fails binds a restart orphaned, and
removes every pod and object an operation left behind.

**Capacity.** Live operation pods are counted under an advisory lock against
``connectors.servicePods.quota.bindTimePods`` for a clear message; the
namespace's Terminating pod quota is the backstop, and its refusal is a
capacity error, never retried as "creation unconfirmed".

Design: knowledge-base/knowledge/features/connector_drivers.md, "Three
planes", "Driver versions", "The driver namespace baseline" and slice D6.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any
from uuid import UUID, uuid4

from orchestrator.services.connector_credential_leases import (
    LeaseDeliveryError,
    LeaseOwner,
    record_lease_event,
)
from orchestrator.services.connector_egress import (
    EgressPins,
    EgressPolicy,
    EgressRefused,
    pin_egress,
    private_addresses_allowed,
    system_resolver,
)
from orchestrator.services.connector_service_hosting import (
    PodState,
    ServiceCapacityError,
    ServiceHostingSettings,
    ServicePodRuntime,
    ServiceRuntimeError,
    _field,
    _init_failure,
)
from orchestrator.services.connector_bind_time_launch import (
    MANAGER,
    BindTimeLaunchPlan,
    BindTimePod,
    build_bind_time_launch,
)
from orchestrator.services.connector_service_images import (
    BoundImage,
    ServiceImageRefused,
    ServiceImageUnavailable,
    ensure_image,
    moved_image_problems,
    resolve_driver_image,
)
from orchestrator.services.connector_service_launch import ServiceLaunchError
from orchestrator.services.pinned_k8s_effect import (
    run_bounded_k8s_call,
    run_bounded_k8s_mutation,
)
from shared.connectors.builtin import IMAGE_DRIVER_SPEC, spec_for_row
from shared.connectors.contract import PROTOCOL_VERSION, DriverSpec, effective_access
from shared.connectors.envelope import (
    DriverError,
    DriverOutcome,
    DriverRequest,
    ExecutionRef,
    api_check_result,
    read_output,
)
from shared.connectors.images import ImageReference, refusal_message
from shared.connectors.leases import (
    DRIVER_IDENTITY_PREFIX,
    last_four,
    mint_token,
    token_digest,
    token_shape_valid,
)
from shared.connectors.registration import (
    custom_driver_problems,
    image_binding_problems,
    spec_from_json,
    wire_credentials,
)

logger = logging.getLogger(__name__)

#: How long a failed bind is answered from its row before a delivery tries
#: again (a driver error, a refused moved tag, a registry outage).
BIND_RETRY_SECONDS = 30.0
#: Seconds between looks at an operation's row and pod while it runs.
POLL_SECONDS = 0.5
OBSERVE_SECONDS = 3.0
#: A pod that ended without posting gets this long for a late post.
LATE_RESULT_SECONDS = 5.0
#: The most a posted outcome may hold (the driver's output cap plus framing).
MAX_RESULT_BYTES = 1024 * 1024 + 64 * 1024
_CAPACITY_LOCK = "srw-connector-bind-time-capacity"
#: Revoke attempts per pass, and how long a transient failure waits.
REVOKES_PER_PASS = 10
#: Retention of finished rows: what the connector page shows.
RETENTION_DAYS = 30


class BindTimeError(LeaseDeliveryError):
    """A registered driver's binding cannot be delivered (yet)."""


class BindTimePending(BindTimeError):
    """The bind is still running; the delivery is refused for a retry."""


class BindTimeRefused(BindTimeError):
    """The bind failed: a refused image, a driver error or SRW's refusal."""


class BindTimeUnavailable(RuntimeError):
    """This installation runs no driver pods."""


class BindTimeCapacity(RuntimeError):
    """No room for another bind-time pod."""


@dataclass(frozen=True)
class BindTimeSettings:
    """How bind-time pods run (``connectors.servicePods`` and
    ``connectors.customDrivers`` in the chart)."""

    hosting: ServiceHostingSettings
    max_pods: int = 10
    deadline_seconds: float = 120.0
    wait_seconds: float = 60.0


# =============================================================================
# Kubernetes effects
# =============================================================================


class BindTimePodRuntime(ServicePodRuntime):
    """Bounded effects for bind-time pods in the driver namespace."""

    async def launch_operation(self, plan: BindTimeLaunchPlan) -> str | None:
        """Policy first (the selector exists before the pod), then the Secret,
        then the Pod; the Secret is owned by the Pod once it exists."""
        await self._create(
            self.networking_api.create_namespaced_network_policy, plan.network_policy
        )
        await self._create(self.core_api.create_namespaced_secret, plan.secret)
        pod = await self._create(self.core_api.create_namespaced_pod, plan.pod)
        name = plan.pod_identity.pod_name
        if pod is None:
            pod = await run_bounded_k8s_call(
                self.core_api.read_namespaced_pod, name=name, namespace=self.namespace
            )
        uid = _field(_field(pod, "metadata"), "uid")
        if uid:
            try:
                await run_bounded_k8s_mutation(
                    self.core_api.patch_namespaced_secret,
                    name=name,
                    namespace=self.namespace,
                    body={
                        "metadata": {
                            "ownerReferences": [
                                {
                                    "apiVersion": "v1",
                                    "kind": "Pod",
                                    "name": name,
                                    "uid": uid,
                                }
                            ]
                        }
                    },
                )
            except Exception:
                logger.debug("owner reference patch failed", exc_info=True)
        return str(uid) if uid else None

    async def observe_operation(self, name: str) -> PodState:
        try:
            pod = await run_bounded_k8s_call(
                self.core_api.read_namespaced_pod, name=name, namespace=self.namespace
            )
        except Exception as exc:
            if getattr(exc, "status", None) == 404:
                return PodState("Absent")
            raise ServiceRuntimeError("reading a driver pod failed") from None
        status = _field(pod, "status")
        driver = next(
            (
                item
                for item in _field(status, "containerStatuses") or []
                if _field(item, "name") == "driver"
            ),
            None,
        )
        terminated = _field(_field(driver, "state"), "terminated")
        message = _init_failure(status)
        if message is None and terminated is not None:
            message = f"driver: exit {_field(terminated, 'exitCode')}" + (
                f" ({_field(terminated, 'reason')})"
                if _field(terminated, "reason")
                else ""
            )
        return PodState(
            phase=_field(status, "phase") or "Unknown",
            uid=_field(_field(pod, "metadata"), "uid"),
            reason=_field(status, "reason"),
            message=message,
        )

    async def remove_operation(self, name: str) -> bool:
        """Delete the pod, its Secret and its policy; ``True`` once gone."""
        await self._delete(self.core_api.delete_namespaced_pod, name)
        await self._delete(self.core_api.delete_namespaced_secret, name)
        await self._delete(self.networking_api.delete_namespaced_network_policy, name)
        return (await self.observe_operation(name)).absent

    async def operation_objects(self) -> list[tuple[Callable[..., Any], str, str]]:
        """Every object of a bind-time pod: ``(delete, name, operation id)``."""
        selector = f"srw/managed-by={MANAGER}"
        found: list[tuple[Callable[..., Any], str, str]] = []
        for list_call, delete in (
            (self.core_api.list_namespaced_pod, self.core_api.delete_namespaced_pod),
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
                found.append(
                    (
                        delete,
                        _field(metadata, "name"),
                        labels.get("srw.io/driver-operation", ""),
                    )
                )
        return found


# =============================================================================
# Running one operation
# =============================================================================


def _encrypt(value: Any) -> str:
    from orchestrator.security.crypto import encrypt

    return encrypt(json.dumps(value, separators=(",", ":")))


def _decrypt(ciphertext: str | None) -> Any:
    from orchestrator.security.crypto import DecryptionError, decrypt

    if not ciphertext:
        return None
    try:
        return json.loads(decrypt(ciphertext))
    except (DecryptionError, RuntimeError, ValueError, TypeError):
        return None


def _repository(reference: str) -> str:
    return ImageReference.parse(reference).name


def _outcome_from_post(posted: Any, operation: str) -> DriverOutcome:
    """The outcome a shim posted, interpreted as SRW reads driver output."""
    if not isinstance(posted, Mapping):
        return DriverOutcome(
            error=DriverError("system", "The connector driver posted no outcome")
        )
    if posted.get("protocol_error"):
        return DriverOutcome(
            error=DriverError(
                "system",
                "The connector driver broke the protocol",
                str(posted["protocol_error"])[:500],
            )
        )
    lines = posted.get("lines") or []
    stdout = "\n".join(json.dumps(line, separators=(",", ":")) for line in lines)
    return read_output(stdout, int(posted.get("exit_code", 1)), operation=operation)


@dataclass
class DriverOperations:
    """Runs one driver operation in its own pod and waits for its outcome."""

    store: Any
    settings: BindTimeSettings
    runtime: Callable[[], BindTimePodRuntime | None]
    resolver: Callable[[str, bool], Awaitable[Sequence[str]]] = system_resolver
    clock: Callable[[], datetime] = field(default=lambda: datetime.now(timezone.utc))

    async def _exchange_address(self, runtime: BindTimePodRuntime) -> str:
        hosting = self.settings.hosting
        name, _, rest = hosting.exchange_host.partition(".")
        namespace = rest.split(".", 1)[0] or hosting.release_namespace
        try:
            address = await runtime.service_cluster_ip(name, namespace)
        except ServiceRuntimeError as exc:
            raise ServiceLaunchError(str(exc)) from None
        if ":" in address:
            raise ServiceLaunchError(
                f"the lease exchange Service {namespace}/{name} has no IPv4 ClusterIP"
            )
        return address

    async def pins(
        self,
        spec: DriverSpec | None,
        config: Mapping[str, Any],
        *,
        private_allowed: bool,
    ) -> EgressPins:
        """The pod's egress: the spec's declared hosts for ``config``, pinned;
        none at all without a spec (a ``spec`` operation)."""
        if spec is None:
            return EgressPins(hosts=(), resolved_at=self.clock())
        hosting = self.settings.hosting
        return await pin_egress(
            spec.egress,
            config,
            policy=EgressPolicy.build(
                hosting.cluster_cidrs,
                allow_private=private_allowed,
                ipv6=hosting.ipv6,
                refused_cidrs=hosting.refused_cidrs,
            ),
            needs_dns=spec.needs_dns,
            resolver=self.resolver,
            now=self.clock,
        )

    async def _claim(
        self,
        pod: BindTimePod,
        *,
        image: BoundImage | Any,
        registration_id: str | None,
        binding_id: str | None,
    ) -> str:
        """Record the operation and mint its identity under the capacity
        lock; returns the identity token (shown once, stored hashed)."""
        async with self.store.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                    _CAPACITY_LOCK,
                )
                live = await conn.fetchval(
                    "SELECT count(*) FROM connector_driver_operations "
                    "WHERE removed_at IS NULL"
                )
                if int(live) >= self.settings.max_pods:
                    raise BindTimeCapacity(
                        f"the installation runs its cap of {self.settings.max_pods} "
                        "bind-time driver pods; try again shortly"
                    )
                token = mint_token(DRIVER_IDENTITY_PREFIX)
                await conn.execute(
                    """
                    INSERT INTO connector_driver_operations
                        (id, token_hash, token_last_four, operation,
                         registration_id, connector_id, binding_id,
                         image_reference, image_digest, pod_namespace, pod_name,
                         deadline_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                            now() + make_interval(secs => $12::float8))
                    """,
                    UUID(pod.operation_id),
                    token_digest(token),
                    last_four(token),
                    pod.operation,
                    UUID(registration_id) if registration_id else None,
                    UUID(pod.connector_id) if pod.connector_id else None,
                    UUID(binding_id) if binding_id else None,
                    image.reference,
                    image.digest,
                    self.settings.hosting.namespace,
                    pod.pod_name,
                    float(self.settings.deadline_seconds),
                )
        return token

    async def _finish(self, operation_id: str, *, error: str) -> None:
        """Close a running operation with SRW's own reason."""
        async with self.store.acquire() as conn:
            await conn.execute(
                """
                UPDATE connector_driver_operations
                   SET status = 'failed', error = $2, finished_at = now()
                 WHERE id = $1 AND status = 'running'
                """,
                UUID(operation_id),
                error[:500],
            )

    async def _remove(self, runtime: BindTimePodRuntime, pod: BindTimePod) -> None:
        try:
            gone = await runtime.remove_operation(pod.pod_name)
        except ServiceRuntimeError as exc:
            logger.warning("Driver pod %s: removal incomplete (%s)", pod.pod_name, exc)
            return
        if gone:
            async with self.store.acquire() as conn:
                await conn.execute(
                    "UPDATE connector_driver_operations SET removed_at = now() "
                    "WHERE id = $1",
                    UUID(pod.operation_id),
                )

    async def _wait(self, runtime: BindTimePodRuntime, pod: BindTimePod) -> Any:
        """The operation's row once it is no longer running."""
        started = time.monotonic()
        deadline = started + float(self.settings.deadline_seconds) + 10.0
        last_observed = started
        ended_at: float | None = None
        ended_message = ""
        while True:
            async with self.store.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT status, exit_code, outcome_ciphertext, error "
                    "FROM connector_driver_operations WHERE id = $1",
                    UUID(pod.operation_id),
                )
            if row is None or row["status"] != "running":
                return row
            now = time.monotonic()
            if now >= deadline:
                await self._finish(
                    pod.operation_id, error="the driver pod did not finish in time"
                )
                continue
            if ended_at is not None and now - ended_at >= LATE_RESULT_SECONDS:
                await self._finish(pod.operation_id, error=ended_message)
                continue
            if ended_at is None and now - last_observed >= OBSERVE_SECONDS:
                last_observed = now
                try:
                    state = await runtime.observe_operation(pod.pod_name)
                except ServiceRuntimeError:
                    state = None
                if state is not None and state.phase in (
                    "Absent",
                    "Failed",
                    "Succeeded",
                ):
                    ended_at = now
                    ended_message = (
                        "the driver pod ended without posting a result"
                        + (f" ({state.message})" if state.message else "")
                        + (f": {state.reason}" if state.reason else "")
                    )
            await asyncio.sleep(POLL_SECONDS)

    async def run(
        self,
        *,
        operation: str,
        driver: str,
        image: BoundImage | Any,
        request: DriverRequest,
        spec: DriverSpec | None,
        config: Mapping[str, Any],
        connector_id: str | None = None,
        registration_id: str | None = None,
        binding_id: str | None = None,
        private_allowed: bool = False,
    ) -> DriverOutcome:
        """Run ``operation`` in its own pod; the outcome, or SRW's error.

        Raises :class:`BindTimeUnavailable` without hosting and
        :class:`BindTimeCapacity` at the cap; every other failure is a
        ``system`` error in the outcome.
        """
        runtime = self.runtime()
        if runtime is None:
            raise BindTimeUnavailable("this installation runs no driver pods")
        try:
            exchange = await self._exchange_address(runtime)
            pins = await self.pins(spec, config, private_allowed=private_allowed)
        except (ServiceLaunchError, EgressRefused) as exc:
            return DriverOutcome(error=DriverError("system", str(exc)))
        pod = BindTimePod(
            operation_id=str(uuid4()),
            operation=operation,
            driver=driver,
            digest=image.digest,
            connector_id=connector_id,
        )
        token = await self._claim(
            pod, image=image, registration_id=registration_id, binding_id=binding_id
        )
        try:
            plan = build_bind_time_launch(
                pod,
                request=request.to_json(),
                image=f"{_repository(image.reference)}@{image.digest}",
                entrypoint=tuple(image.entrypoint),
                cmd=tuple(image.cmd),
                identity_token=token,
                pins=pins,
                policy=self.settings.hosting.launch_policy(exchange),
                deadline_seconds=int(self.settings.deadline_seconds),
            )
        except ServiceLaunchError as exc:
            await self._finish(pod.operation_id, error=str(exc))
            await self._mark_removed(pod)
            return DriverOutcome(error=DriverError("system", str(exc)))
        try:
            await runtime.launch_operation(plan)
        except ServiceCapacityError as exc:
            await self._finish(pod.operation_id, error=f"capacity: {exc}")
            await self._remove(runtime, pod)
            raise BindTimeCapacity(str(exc)) from None
        except ServiceRuntimeError as exc:
            await self._finish(pod.operation_id, error=str(exc))
            await self._remove(runtime, pod)
            return DriverOutcome(error=DriverError("system", str(exc)))
        try:
            row = await self._wait(runtime, pod)
        finally:
            await self._remove(runtime, pod)
        if row is None:
            return DriverOutcome(error=DriverError("system", "the operation vanished"))
        if row["status"] == "failed":
            return DriverOutcome(
                error=DriverError(
                    "system", "The connector driver did not answer", row["error"]
                )
            )
        return _outcome_from_post(_decrypt(row["outcome_ciphertext"]), operation)

    async def _mark_removed(self, pod: BindTimePod) -> None:
        async with self.store.acquire() as conn:
            await conn.execute(
                "UPDATE connector_driver_operations SET removed_at = now() "
                "WHERE id = $1",
                UUID(pod.operation_id),
            )


# =============================================================================
# The result route (on the lease exchange's port)
# =============================================================================


async def record_operation_result(
    store: Any, *, identity_token: str, posted: Mapping[str, Any]
) -> tuple[int, dict[str, Any]]:
    """Store what one operation pod's shim posted; ``(status, body)``.

    The pod's identity authenticates it and names the operation (the request
    never does). Each identity posts once, while its operation runs and
    before its deadline; the outcome is stored encrypted (a bind's result
    holds what reaches the workspace).
    """
    if not token_shape_valid(identity_token, DRIVER_IDENTITY_PREFIX):
        return 401, {"error": "unknown_driver_identity"}
    lines = posted.get("lines")
    exit_code = posted.get("exit_code")
    if (
        not isinstance(lines, list)
        or not all(isinstance(line, Mapping) for line in lines)
        or isinstance(exit_code, bool)
        or not isinstance(exit_code, int)
        or not isinstance(posted.get("operation"), str)
    ):
        return 422, {"error": "invalid_outcome"}
    outcome = {
        "exit_code": exit_code,
        "lines": list(lines),
        "protocol_error": str(posted.get("protocol_error") or "")[:500] or None,
    }
    async with store.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE connector_driver_operations
               SET status = 'finished', exit_code = $3, outcome_ciphertext = $4,
                   finished_at = now()
             WHERE token_hash = $1 AND status = 'running' AND deadline_at > now()
               AND operation = $2
            RETURNING id
            """,
            token_digest(identity_token),
            posted["operation"],
            exit_code,
            _encrypt(outcome),
        )
        if row is None:
            known = await conn.fetchval(
                "SELECT status FROM connector_driver_operations WHERE token_hash = $1",
                token_digest(identity_token),
            )
    if row is None:
        if known is None:
            return 401, {"error": "unknown_driver_identity"}
        return 409, {"error": "operation_closed"}
    return 200, {"status": "recorded"}


# =============================================================================
# Bindings
# =============================================================================


def registered_entry(entry: Any) -> bool:
    """Whether a payload entry is a registered image driver's connector."""
    return isinstance(entry, Mapping) and spec_for_row(entry) is IMAGE_DRIVER_SPEC


@dataclass
class BindTimeRuntime:
    """This application's bind-time hosting: the store, the operations
    runner and the trust policy (``configure_bind_time``)."""

    store: Any
    operations: DriverOperations
    privileged: Callable[[str], bool] = lambda _reference: False
    inflight: dict[tuple[str, str, str], asyncio.Task] = field(default_factory=dict)


_state: dict[str, BindTimeRuntime | None] = {"runtime": None}


def configure_bind_time(runtime: BindTimeRuntime | None) -> None:
    """Install (or, with ``None``, remove) this process's bind-time hosting."""
    _state["runtime"] = runtime


def bind_time_runtime() -> BindTimeRuntime | None:
    return _state["runtime"]


_LATEST = """
SELECT id, status, image_reference, image_digest, delivery_ciphertext,
       error_class, error_message, failed_at, created_at
  FROM connector_bind_time_bindings
 WHERE owner_kind = $1 AND owner_id = $2 AND connector_id = $3
   AND status IN ('pending', 'bound', 'failed')
 ORDER BY (status <> 'failed') DESC, created_at DESC
 LIMIT 1
"""


async def _latest(conn: Any, owner: LeaseOwner, connector_id: str) -> Any:
    return await conn.fetchrow(
        _LATEST, owner.kind, UUID(owner.id), UUID(str(connector_id))
    )


def _recent_failure(row: Any) -> bool:
    if row is None or row["status"] != "failed" or row["failed_at"] is None:
        return False
    failed = row["failed_at"]
    if failed.tzinfo is None:
        failed = failed.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - failed).total_seconds() < BIND_RETRY_SECONDS


async def _execution(conn: Any, owner: LeaseOwner) -> ExecutionRef:
    table = "jobs" if owner.kind == "job" else "threads"
    project_id = await conn.fetchval(
        f"SELECT project_id FROM {table} WHERE id = $1", UUID(owner.id)
    )
    return ExecutionRef(
        kind="job" if owner.kind == "job" else "session",
        id=owner.id,
        project_id=str(project_id) if project_id else None,
    )


async def _fail(
    store: Any,
    binding_id: str,
    *,
    error_class: str,
    message: str,
    image: BoundImage | None = None,
) -> None:
    async with store.acquire() as conn:
        await conn.execute(
            """
            UPDATE connector_bind_time_bindings
               SET status = 'failed', failed_at = now(), error_class = $2,
                   error_message = $3,
                   image_reference = COALESCE($4, image_reference),
                   image_digest = COALESCE($5, image_digest),
                   resolved_at = COALESCE($6, resolved_at),
                   spec_hash = COALESCE($7, spec_hash),
                   protocol_version = COALESCE($8, protocol_version)
             WHERE id = $1 AND status = 'pending'
            """,
            UUID(binding_id),
            error_class,
            message[:1000],
            image.reference if image else None,
            image.digest if image else None,
            image.resolved_at if image else None,
            image.spec_hash if image else None,
            image.protocol_version if image else None,
        )


async def _previous_bound_digest(conn: Any, connector_id: str, binding_id: str) -> Any:
    return await conn.fetchval(
        """
        SELECT image_digest FROM connector_bind_time_bindings
         WHERE connector_id = $1 AND id <> $2 AND image_digest IS NOT NULL
           AND status IN ('bound', 'revoking', 'revoked')
         ORDER BY bound_at DESC NULLS LAST, created_at DESC LIMIT 1
        """,
        UUID(str(connector_id)),
        UUID(binding_id),
    )


def _binding_problems(descriptor: Any, spec: DriverSpec) -> list[str]:
    """The registration's rules, plus the environment and file-variable
    rules every connector's delivery keeps."""
    from shared.credential_connectors import (
        credential_file_env_problem,
        normalize_credential_env,
    )

    problems = image_binding_problems(descriptor, spec)
    if problems:
        return problems
    env = {
        entry["value"]["name"]: entry["value"]["value"]
        for entry in descriptor["entries"]
        if entry["form"] == "env_file"
    }
    try:
        normalize_credential_env(env)
    except ValueError as exc:
        problems.append(str(exc))
    for entry in descriptor["entries"]:
        name = (
            entry["value"].get("env_var")
            if entry["form"] == "credential_file"
            else None
        )
        if name:
            why = credential_file_env_problem(name)
            if why:
                problems.append(why)
    try:
        wire_credentials(descriptor)
    except ValueError as exc:
        problems.append(str(exc))
    return problems


async def _bind(
    runtime: BindTimeRuntime,
    owner: LeaseOwner,
    connector_id: str,
    *,
    project_read_only: bool,
) -> None:
    """Run one binding of ``owner`` for ``connector_id`` to its end."""
    from orchestrator.services.connector_driver_registrations import (
        registration_for_connector,
    )
    from orchestrator.services.connector_secrets import read_connector_credentials

    store = runtime.store
    row = await store.get_datasource(str(connector_id))
    if row is None:
        return
    registration = await registration_for_connector(store, connector_id)
    async with store.acquire() as conn:
        binding_id = await conn.fetchval(
            """
            INSERT INTO connector_bind_time_bindings
                (owner_kind, owner_id, connector_id, registration_id, driver)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (owner_kind, owner_id, connector_id)
                WHERE status IN ('pending', 'bound') DO NOTHING
            RETURNING id
            """,
            owner.kind,
            UUID(owner.id),
            UUID(str(connector_id)),
            UUID(registration.id) if registration else None,
            registration.name if registration else IMAGE_DRIVER_SPEC.name,
        )
    if binding_id is None:
        return  # another delivery binds it
    binding_id = str(binding_id)
    if registration is None:
        await _fail(
            store,
            binding_id,
            error_class="config",
            message="This connector's driver registration is gone; it cannot bind",
        )
        return
    try:
        image = await resolve_driver_image(
            store, driver=registration.name, reference=registration.image_reference
        )
    except (ServiceImageRefused, ServiceImageUnavailable) as exc:
        await _fail(store, binding_id, error_class="transient", message=str(exc))
        return
    spec = registration.spec
    async with store.acquire() as conn:
        previous = await _previous_bound_digest(conn, connector_id, binding_id)
        problems = await moved_image_problems(
            conn,
            spec=spec,
            connector_id=connector_id,
            image=image,
            previous_digest=previous or registration.image_digest,
        )
        if not problems and image.spec is not None:
            # The new image's own spec drives this bind; it must still be one
            # SRW registers.
            try:
                spec = spec_from_json(image.spec)
            except ValueError as exc:
                problems = [f"its spec label is malformed ({exc})"]
            else:
                problems = custom_driver_problems(
                    spec, privileged=runtime.privileged(registration.image_reference)
                )
        if problems:
            await record_lease_event(
                conn,
                event_type="connector_driver_image_refused",
                resource_type="connector",
                resource_id=str(UUID(str(connector_id))),
                detail=(
                    f"driver={registration.name} reference={image.reference} "
                    f"digest={image.digest} owner={owner.kind}:{owner.id} "
                    f"problems={'; '.join(problems)}"
                ),
            )
        execution = await _execution(conn, owner)
        hosting = runtime.operations.settings.hosting
        private_allowed = await private_addresses_allowed(
            conn, str(connector_id), private_tiers=hosting.private_tiers
        )
    if problems:
        await _fail(
            store,
            binding_id,
            error_class="config",
            message=refusal_message(registration.image_reference, problems),
            image=image,
        )
        return
    await read_connector_credentials(
        [row], authorized=[str(row["id"])], dependencies=SimpleNamespace(store=store)
    )
    credentials = (
        row.get("credentials") if isinstance(row.get("credentials"), dict) else {}
    )
    access = effective_access(
        {"project_read_only": project_read_only, "config": row.get("config")}, spec
    )
    request = DriverRequest(
        operation="bind",
        config=dict(row.get("config") or {}),
        access=access,
        credentials=credentials,
        binding_id=binding_id,
        execution=execution,
    )
    try:
        outcome = await runtime.operations.run(
            operation="bind",
            driver=spec.name,
            image=image,
            request=request,
            spec=spec,
            config=row.get("config") or {},
            connector_id=str(connector_id),
            registration_id=registration.id,
            binding_id=binding_id,
            private_allowed=private_allowed,
        )
    except BindTimeCapacity as exc:
        await _fail(
            store, binding_id, error_class="transient", message=str(exc), image=image
        )
        return
    except BindTimeUnavailable as exc:
        await _fail(
            store, binding_id, error_class="system", message=str(exc), image=image
        )
        return
    if outcome.error is not None:
        if outcome.error.detail:
            logger.warning(
                "Driver %s bind for connector %s failed: %s (%s)",
                spec.name,
                connector_id,
                outcome.error.message,
                outcome.error.detail,
            )
        await _fail(
            store,
            binding_id,
            error_class=outcome.error.error_class,
            message=outcome.error.message,
            image=image,
        )
        return
    descriptor = (outcome.result or {}).get("binding")
    problems = _binding_problems(descriptor, spec)
    if problems:
        await _fail(
            store,
            binding_id,
            error_class="system",
            message="The driver returned a binding SRW will not deliver: "
            + "; ".join(problems[:5]),
            image=image,
        )
        return
    if outcome.updates:
        logger.warning(
            "Driver %s returned %d update line(s); this release does not apply them",
            spec.name,
            len(outcome.updates),
        )
    async with store.acquire() as conn:
        await conn.execute(
            """
            UPDATE connector_bind_time_bindings
               SET status = 'bound', bound_at = now(), delivery_ciphertext = $2,
                   driver_state_ciphertext = $3, image_reference = $4,
                   image_digest = $5, resolved_at = $6, spec_hash = $7,
                   protocol_version = $8, access = $9, driver = $10
             WHERE id = $1 AND status = 'pending'
            """,
            UUID(binding_id),
            _encrypt(wire_credentials(descriptor)),
            _encrypt(outcome.driver_state)
            if outcome.driver_state is not None
            else None,
            image.reference,
            image.digest,
            image.resolved_at,
            # An image without a label runs the registration's spec.
            image.spec_hash if image.spec is not None else registration.spec_hash,
            image.protocol_version
            if image.spec is not None
            else registration.protocol_version,
            access,
            spec.name,
        )
    logger.info(
        "Driver %s bound connector %s for %s %s at %s",
        spec.name,
        connector_id,
        owner.kind,
        owner.id,
        image.digest,
    )


async def ensure_binding(
    runtime: BindTimeRuntime,
    owner: LeaseOwner,
    connector_id: str,
    *,
    project_read_only: bool = False,
    wait: float | None = None,
) -> None:
    """Start the binding of ``owner`` for ``connector_id`` unless one is live
    or failed moments ago, and wait for it at most ``wait`` seconds (the bind
    goes on past that). Never raises: the delivery reads the outcome."""
    key = (owner.kind, owner.id, str(connector_id))
    task = runtime.inflight.get(key)
    if task is None or task.done():
        async with runtime.store.acquire() as conn:
            row = await _latest(conn, owner, connector_id)
        if row is not None and (row["status"] == "bound" or _recent_failure(row)):
            return
        if row is None or row["status"] == "failed":
            task = asyncio.create_task(
                _bind(
                    runtime,
                    owner,
                    str(connector_id),
                    project_read_only=project_read_only,
                ),
                name=f"connector-bind-{connector_id}",
            )
            runtime.inflight[key] = task

            def _done(done: asyncio.Task, key: tuple[str, str, str] = key) -> None:
                if runtime.inflight.get(key) is done:
                    runtime.inflight.pop(key, None)
                if not done.cancelled() and done.exception() is not None:
                    logger.error(
                        "A connector bind failed unexpectedly",
                        exc_info=done.exception(),
                    )

            task.add_done_callback(_done)
        else:
            task = None  # pending elsewhere: poll its row
    limit = runtime.operations.settings.wait_seconds if wait is None else wait
    if task is not None:
        try:
            await asyncio.wait_for(asyncio.shield(task), timeout=limit)
        except (TimeoutError, asyncio.TimeoutError):
            pass
        except Exception:
            pass
        return
    end = time.monotonic() + limit
    while time.monotonic() < end:
        async with runtime.store.acquire() as conn:
            row = await _latest(conn, owner, connector_id)
        if row is None or row["status"] != "pending":
            return
        await asyncio.sleep(POLL_SECONDS)


#: Binds a delivery started without waiting, kept until they finish.
_background: set[asyncio.Task] = set()


async def prepare_bind_time_bindings(
    entries: Sequence[Any] | None, *, owner: LeaseOwner
) -> None:
    """Bind every registered driver's connector in ``entries`` before the
    delivery opens its transaction, waiting at most ``wait_seconds`` for a
    new bind. Never raises."""
    runtime = bind_time_runtime()
    if runtime is None:
        return
    wanted = [entry for entry in entries or () if registered_entry(entry)]
    if not wanted:
        return
    await asyncio.gather(
        *(
            ensure_binding(
                runtime,
                owner,
                str(entry.get("datasource_id") or ""),
                project_read_only=bool(entry.get("project_read_only")),
            )
            for entry in wanted
            if _uuid(entry.get("datasource_id"))
        ),
        return_exceptions=True,
    )


def _uuid(value: Any) -> bool:
    try:
        UUID(str(value))
    except ValueError:
        return False
    return True


async def deliver_bind_time_entries(
    conn: Any, entries: Sequence[Any] | None, *, owner: LeaseOwner
) -> int:
    """Fill every registered driver's entry with its bound delivery, in place.

    Runs on the delivery's connection, in its transaction. Raises
    :class:`BindTimePending` while a bind runs (one is started here when
    none is) and :class:`BindTimeRefused` with the reason when it failed;
    either refuses the delivery, as a lease that cannot be issued does.
    Returns how many entries were filled.
    """
    filled = 0
    wanted = [entry for entry in entries or () if registered_entry(entry)]
    wanted.sort(key=lambda entry: str(entry.get("datasource_id") or "").lower())
    for entry in wanted:
        entry["credentials"] = {}
        connector_id = str(entry.get("datasource_id") or "")
        if not _uuid(connector_id):
            raise BindTimeRefused("A registered driver's entry names no connector")
        row = await _latest(conn, owner, connector_id)
        if row is not None and row["status"] == "bound":
            delivery = _decrypt(row["delivery_ciphertext"])
            if not isinstance(delivery, dict):
                raise BindTimeRefused(
                    "The connector's bound delivery is unreadable; it binds again "
                    "at the next delivery"
                )
            entry["credentials"] = delivery
            filled += 1
            continue
        if row is not None and row["status"] == "failed":
            if _recent_failure(row):
                raise BindTimeRefused(str(row["error_message"] or "The bind failed"))
        runtime = bind_time_runtime()
        if runtime is None:
            raise BindTimeRefused(
                "This installation runs no driver pods; a registered driver's "
                "connector cannot bind (connectors.servicePods.enabled)"
            )
        # Started on the store's own connections, never this transaction's.
        started = asyncio.create_task(
            ensure_binding(
                runtime,
                owner,
                connector_id,
                project_read_only=bool(entry.get("project_read_only")),
                wait=0,
            )
        )
        _background.add(started)
        started.add_done_callback(_background.discard)
        raise BindTimePending("The connector's driver is still binding; retry shortly")
    return filled


# =============================================================================
# Test connection and the spec operation
# =============================================================================


async def run_check(
    registration: Any, row: Mapping[str, Any], credentials: dict
) -> dict:
    """Test connection of a registered driver's connector: its ``check`` in
    a pod, on the digest its reference resolves to now."""
    runtime = bind_time_runtime()
    if runtime is None:
        return api_check_result(
            DriverOutcome(
                error=DriverError(
                    "unsupported",
                    "This installation runs no driver pods, so a registered "
                    "driver cannot be tested",
                )
            )
        )
    try:
        image = await resolve_driver_image(
            runtime.store,
            driver=registration.name,
            reference=registration.image_reference,
        )
    except (ServiceImageRefused, ServiceImageUnavailable) as exc:
        return {"status": "error", "message": str(exc), "error_class": "transient"}
    config = dict(row.get("config") or {})
    async with runtime.store.acquire() as conn:
        private_allowed = await private_addresses_allowed(
            conn,
            str(row["id"]),
            private_tiers=runtime.operations.settings.hosting.private_tiers,
        )
    try:
        outcome = await runtime.operations.run(
            operation="check",
            driver=registration.name,
            image=image,
            request=DriverRequest(
                operation="check",
                config=config,
                access=effective_access(row, registration.spec),
                credentials=credentials,
            ),
            spec=registration.spec,
            config=config,
            connector_id=str(row["id"]),
            registration_id=registration.id,
            private_allowed=private_allowed,
        )
    except (BindTimeCapacity, BindTimeUnavailable) as exc:
        return {"status": "error", "message": str(exc), "error_class": "transient"}
    return api_check_result(outcome)


async def run_spec_operation(reference: str, resolved: Any) -> dict[str, Any]:
    """The spec an unlabelled image answers, run in a pod with no secret
    and no egress but the result route (registration's fallback)."""
    from fastapi import HTTPException

    runtime = bind_time_runtime()
    if runtime is None:
        raise HTTPException(
            status_code=422,
            detail=(
                "The image has no io.srw.driver.spec label, and this installation "
                "runs no driver pod to ask it (connectors.servicePods.enabled)"
            ),
        )
    image = BoundImage(
        driver="unregistered",
        reference=reference,
        digest=resolved.digest,
        resolved_at=None,
        spec=None,
        spec_hash=None,
        protocol_version=PROTOCOL_VERSION,
        entrypoint=tuple(resolved.entrypoint),
        cmd=tuple(resolved.cmd),
    )
    try:
        outcome = await runtime.operations.run(
            operation="spec",
            driver="unregistered",
            image=image,
            request=DriverRequest(operation="spec"),
            spec=None,
            config={},
        )
    except (BindTimeCapacity, BindTimeUnavailable) as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from None
    if outcome.error is not None or not isinstance(outcome.result, dict):
        message = outcome.error.message if outcome.error else "no spec"
        raise HTTPException(
            status_code=422,
            detail=f"The image has no spec label and its spec operation failed: {message}",
        )
    return outcome.result


# =============================================================================
# The leader's reconciler: revoke, fail orphans, sweep
# =============================================================================

_MARK_REVOKE = """
UPDATE connector_bind_time_bindings AS b
   SET status = 'revoking', revoke_requested_at = now(), revoke_reason = $1
 WHERE b.status = 'bound' AND ({where})
RETURNING b.id
"""
_OWNER_ENDED = """(
    (b.owner_kind = 'job' AND NOT EXISTS (
        SELECT 1 FROM jobs AS j WHERE j.id = b.owner_id
           AND j.status::text NOT IN ('completed', 'failed', 'cancelled')))
    OR (b.owner_kind = 'thread' AND NOT EXISTS (
        SELECT 1 FROM threads AS t WHERE t.id = b.owner_id
           AND t.status::text <> 'ended'
           AND NOT (t.runtime_retirement_token IS NOT NULL
                    AND t.runtime_retirement_authorized_at IS NOT NULL)))
)"""
_CONNECTOR_GONE = (
    "NOT EXISTS (SELECT 1 FROM datasources AS d WHERE d.id = b.connector_id)"
)


@dataclass
class BindTimeReport:
    revoked: list[str] = field(default_factory=list)
    marked: int = 0
    orphaned: int = 0
    swept: int = 0

    def __bool__(self) -> bool:
        return bool(self.revoked or self.marked or self.orphaned or self.swept)


async def _revoke_one(runtime: BindTimeRuntime, row: Mapping[str, Any]) -> str | None:
    """Run one binding's ``revoke``; ``None`` when it is retired, else why it
    waits for the next pass (a transient failure)."""
    from orchestrator.services.connector_driver_registrations import (
        registration_by_id,
    )
    from orchestrator.services.connector_secrets import read_connector_credentials

    store = runtime.store
    binding_id = str(row["id"])
    connector = await store.get_datasource(str(row["connector_id"]))
    # The binding's own registration: the connector (and with it its
    # assignment) may be gone already.
    registration = await registration_by_id(store, row["registration_id"])
    if registration is None or not row["image_digest"]:
        return await _retire(store, binding_id, "no driver registration to revoke with")
    async with store.acquire() as conn:
        try:
            image = await ensure_image(
                conn,
                driver=registration.name,
                reference=row["image_reference"] or registration.image_reference,
                digest=row["image_digest"],
            )
        except ServiceImageUnavailable as exc:
            return str(exc)
    config: dict[str, Any] = {}
    credentials: dict[str, Any] = {}
    if connector is not None:
        await read_connector_credentials(
            [connector],
            authorized=[str(connector["id"])],
            dependencies=SimpleNamespace(store=store),
        )
        config = dict(connector.get("config") or {})
        found = connector.get("credentials")
        credentials = found if isinstance(found, dict) else {}
    state = _decrypt(row["driver_state_ciphertext"])
    try:
        outcome = await runtime.operations.run(
            operation="revoke",
            driver=registration.name,
            image=image,
            request=DriverRequest(
                operation="revoke",
                config=config,
                access=row["access"],
                credentials=credentials,
                binding_id=binding_id,
                driver_state=state if isinstance(state, str) else None,
            ),
            spec=registration.spec,
            config=config,
            connector_id=str(row["connector_id"]),
            registration_id=registration.id,
            binding_id=binding_id,
        )
    except BindTimeCapacity as exc:
        return str(exc)
    except BindTimeUnavailable as exc:
        return await _retire(store, binding_id, str(exc))
    if outcome.error is not None and outcome.error.retryable:
        return outcome.error.message
    return await _retire(
        store, binding_id, outcome.error.message if outcome.error else None
    )


async def _retire(store: Any, binding_id: str, error: str | None) -> None:
    async with store.acquire() as conn:
        await conn.execute(
            """
            UPDATE connector_bind_time_bindings
               SET status = 'revoked', revoked_at = now(), revoke_error = $2,
                   delivery_ciphertext = NULL, driver_state_ciphertext = NULL
             WHERE id = $1 AND status = 'revoking'
            """,
            UUID(binding_id),
            error[:500] if error else None,
        )
    return None


async def reconcile_bind_time_once(
    runtime: BindTimeRuntime, pod_runtime: BindTimePodRuntime | None
) -> BindTimeReport:
    """One leader pass: mark, revoke, fail orphans, sweep, prune."""
    report = BindTimeReport()
    store = runtime.store
    settings = runtime.operations.settings
    async with store.acquire() as conn:
        for reason, where in (
            ("execution_ended", _OWNER_ENDED),
            ("connector_deleted", _CONNECTOR_GONE),
        ):
            rows = await conn.fetch(_MARK_REVOKE.format(where=where), reason)
            report.marked += len(rows)
        # No bind outlives its pod's deadline by this much: one still pending
        # was orphaned by a restart.
        stale = settings.deadline_seconds + settings.wait_seconds + 60.0
        orphaned = await conn.fetch(
            """
            UPDATE connector_bind_time_bindings
               SET status = 'failed', failed_at = now(), error_class = 'transient',
                   error_message = 'The bind did not finish; it runs again at the '
                                   'next delivery'
             WHERE status = 'pending'
               AND created_at < now() - make_interval(secs => $1::float8)
            RETURNING id
            """,
            stale,
        )
        report.orphaned = len(orphaned)
        revoking = await conn.fetch(
            """
            SELECT id, connector_id, registration_id, image_reference,
                   image_digest, access, driver_state_ciphertext
              FROM connector_bind_time_bindings
             WHERE status = 'revoking'
             ORDER BY revoke_requested_at LIMIT $1
            """,
            REVOKES_PER_PASS,
        )
    if pod_runtime is not None:
        for row in revoking:
            waiting = await _revoke_one(runtime, row)
            if waiting is None:
                report.revoked.append(str(row["id"]))
            else:
                async with store.acquire() as conn:
                    await conn.execute(
                        "UPDATE connector_bind_time_bindings SET revoke_error = $2 "
                        "WHERE id = $1",
                        row["id"],
                        waiting[:500],
                    )
        report.swept = await _sweep(store, pod_runtime)
    async with store.acquire() as conn:
        await conn.execute(
            """
            DELETE FROM connector_bind_time_bindings
             WHERE status IN ('revoked', 'failed')
               AND created_at < now() - make_interval(days => $1)
            """,
            RETENTION_DAYS,
        )
        await conn.execute(
            """
            DELETE FROM connector_driver_operations
             WHERE removed_at IS NOT NULL
               AND created_at < now() - make_interval(days => $1)
            """,
            RETENTION_DAYS,
        )
    return report


async def _sweep(store: Any, pod_runtime: BindTimePodRuntime) -> int:
    """Close operations past their deadline and delete every object no
    running operation names (a restart's leftovers)."""
    async with store.acquire() as conn:
        await conn.execute(
            """
            UPDATE connector_driver_operations
               SET status = 'failed', error = 'the driver pod did not finish in time',
                   finished_at = now()
             WHERE status = 'running' AND deadline_at < now() - interval '30 seconds'
            """
        )
        running = {
            str(row["id"])
            for row in await conn.fetch(
                "SELECT id FROM connector_driver_operations WHERE status = 'running'"
            )
        }
    swept = 0
    try:
        objects = await pod_runtime.operation_objects()
    except Exception as exc:
        logger.warning("Listing bind-time driver objects failed: %s", exc)
        return 0
    present = {operation_id for _delete, _name, operation_id in objects}
    for delete, name, operation_id in objects:
        if operation_id in running:
            continue
        try:
            await pod_runtime.delete_object(delete, name)
            swept += 1
        except ServiceRuntimeError as exc:
            logger.warning("Deleting %s failed: %s", name, exc)
    # A finished operation with no object left counts against the cap no more
    # (what was deleted above is recorded at the next pass).
    async with store.acquire() as conn:
        await conn.execute(
            """
            UPDATE connector_driver_operations SET removed_at = now()
             WHERE status <> 'running' AND removed_at IS NULL
               AND NOT (id::text = ANY($1::text[]))
            """,
            sorted(present),
        )
    return swept


async def bind_time_reconciler(
    shutdown_event: asyncio.Event,
    *,
    pod_runtime: Callable[[], BindTimePodRuntime | None],
    interval_seconds: float = 15.0,
) -> None:
    """Leader-gated loop over :func:`reconcile_bind_time_once`. Best effort:
    a failed pass is logged and the next one runs on time."""
    logger.info(
        "Bind-time driver reconciler started (interval=%.0fs)", interval_seconds
    )
    while not shutdown_event.is_set():
        runtime = bind_time_runtime()
        if runtime is not None:
            try:
                report = await reconcile_bind_time_once(runtime, pod_runtime())
                if report:
                    logger.info(
                        "bind-time drivers: marked=%d revoked=%d orphaned=%d swept=%d",
                        report.marked,
                        len(report.revoked),
                        report.orphaned,
                        report.swept,
                    )
            except Exception as exc:
                logger.warning("bind-time driver pass error (non-fatal): %s", exc)
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=interval_seconds)
            break
        except asyncio.TimeoutError:
            pass
    logger.info("Bind-time driver reconciler stopped")


__all__ = [
    "BIND_RETRY_SECONDS",
    "MAX_RESULT_BYTES",
    "BindTimeCapacity",
    "BindTimeError",
    "BindTimePending",
    "BindTimePodRuntime",
    "BindTimeRefused",
    "BindTimeRuntime",
    "BindTimeSettings",
    "BindTimeUnavailable",
    "DriverOperations",
    "bind_time_reconciler",
    "bind_time_runtime",
    "configure_bind_time",
    "deliver_bind_time_entries",
    "ensure_binding",
    "prepare_bind_time_bindings",
    "reconcile_bind_time_once",
    "record_operation_result",
    "registered_entry",
    "run_check",
    "run_spec_operation",
]
