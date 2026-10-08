"""Bind-time image drivers: one pod per operation, returning data only (D6).

A connector of a registered bind-time image (``connector_drivers.registered``)
is bound once per workspace-owning execution (a Job, or a session thread: the
lease owner, ``connector_credential_leases.LeaseOwner``). The bind runs the
driver's ``bind`` in a short-lived pod (``connector_bind_time_launch``) whose
shim posts the driver's typed JSON lines to the result route on the lease
exchange's port. What the driver returns is data: a binding descriptor of
``env_file`` and ``credential_file`` entries, checked
(``shared.connectors.registration.image_binding_problems``: only the
variable names the spec declares, none on the best-effort list of known
tool hooks, files only in ``~/.srw-files/``, ``~/.netrc`` or ``~/.pgpass``),
stored encrypted on the binding row and delivered by SRW's own materializers
in the agent, keyed by delivery form, as an environment or credential-file
connector's. No driver image gets a shell in a workspace; what it returns is
data the workspace's programs then read, so the image's author is the trust
boundary, as a workspace image's is. A binding that sets a variable another
connector of the execution sets is never delivered (a session skips it with
a notice, a job is refused). A driver's own error text is sanitized and
shown only to the connector's owner and administrators.

**When.** A bind starts as early as SRW knows the execution needs it: the
leader's pass starts binds for live executions that select a registered
connector (a job when it is created, a session when it is created or the
connector is selected live, which also starts it at once), the job
dispatcher's preflight starts a job's binds before it claims the job and
holds the job until they finished (:func:`job_bind_gate`), and a session's
attach starts and waits for its binds before it reserves an agent
(:func:`prepare_thread_bindings`, at most ``wait_seconds``, under the
agent's 30 s request). :func:`deliver_bind_time_entries` runs inside the
delivery's transaction: a bound binding fills its entry's credentials. A
job's delivery is refused while a bind runs (:class:`BindTimePending`) or
after it failed for good (:class:`BindTimeRefused`); a session's never is:
the connector is skipped, with a notice in the workspace README (the
entry's ``cli_hint``) and on the connector, and arrives at a later
delivery. Re-delivery (every claim, attach and pod recycle) reuses the
binding: one pod per execution and connector, not per turn.

**Failures.** A ``transient`` failure (SRW's capacity, a registry outage, a
pod that did not finish) is retried with backoff, at most
:data:`MAX_BIND_ATTEMPTS` times. Any other class is final for that
execution and connector until the connector, its registration or the
binding's access changes: a job then fails with the driver's reason (the
dispatcher's preflight, as for other dispatch-time refusals), a session
skips the connector with the notice.

**Versions.** Each bind resolves the registration's reference to a digest
(``connector_service_images.resolve_driver_image``: a digest pins, a tag
follows, an unreachable registry reuses the last digest and the binding
says it is stale). A digest new to the connector is compared with the spec
it last bound with (else its registration's,
``registration.moved_spec_problems``): no label, another name or plane, a
protocol major, a disappeared, newly required or changed slot, new
variable names, forms or egress, or a stored config the new schema refuses
is refused with "the image behind this tag changed its contract" and
audited, and no pod runs. Test (:func:`run_check`) applies the same check.
The binding records ``{reference, digest, resolved_at, spec_hash,
protocol_version}`` and the spec it ran with.

**Revocation.** A binding is revoked when its execution ends or is gone,
its connector is deleted, detached from a live session, changed (config or
credentials), or its access changed, its registration is disabled, or its
execution no longer selects the connector or may no longer use it
(``access_lost``: the delivery's own authorization, checked by the leader,
paused jobs included). A
revoke requested while the bind runs takes effect when it ends: a bind that
minted something always ends in ``revoking``, never dropped (a descriptor
SRW refuses, a bind the reconciler gave up on, a runner that died: the
leader recovers its posted outcome). The leader's
:func:`bind_time_reconciler` runs the driver's ``revoke`` from the binding
alone: its own image digest and spec, and the connector's config and
credentials as they were at bind (kept encrypted on the binding until it is
revoked), with the stored ``driver_state``. A transient failure is retried
with backoff, at most :data:`MAX_REVOKE_ATTEMPTS` times; anything else
retires it with the reason recorded, and a revoke SRW gives up on is logged
and audited (``connector_driver_revoke_abandoned``, outliving the
connector). The pass also fails binds a restart orphaned (within
:data:`MAX_BIND_ATTEMPTS`) and removes every pod and object an operation
left behind; a row whose work fails is logged and backs off, never stopping
the pass.

**Capacity.** Live operation pods are counted under an advisory lock against
``connectors.servicePods.quota.bindTimePods`` for a clear message (and a
user's spec pods against :attr:`BindTimeSettings.max_spec_pods_per_user`);
the namespace's Terminating pod quota is the backstop, and its refusal is a
capacity error, never retried as "creation unconfirmed".

Design: knowledge-base/knowledge/features/connector_drivers.md, "Three
planes", "Driver versions", "The driver namespace baseline" and slice D6.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, Literal
from uuid import UUID, uuid4

from orchestrator.services.connector_credential_leases import (
    LeaseDeliveryError,
    LeaseOwner,
    job_lease_owner,
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
    config_errors,
    ensure_image,
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
from shared.connectors.images import ImageReference, refusal_message, spec_hash
from shared.connectors.leases import (
    DRIVER_IDENTITY_PREFIX,
    last_four,
    mint_token,
    token_digest,
    token_shape_valid,
)
from shared.connectors.registration import (
    custom_driver_problems,
    declared_env_names,
    image_binding_problems,
    moved_spec_problems,
    schema_problems,
    spec_from_json,
    wire_credentials,
)

logger = logging.getLogger(__name__)

#: A transient bind failure waits ``BASE * 2**(attempt - 1)`` seconds (at
#: most ``MAX``) before the next attempt, and gives up after
#: :data:`MAX_BIND_ATTEMPTS`.
BIND_RETRY_BASE_SECONDS = 30.0
BIND_RETRY_MAX_SECONDS = 900.0
MAX_BIND_ATTEMPTS = 6
#: The same for a revoke that failed transiently.
REVOKE_RETRY_BASE_SECONDS = 30.0
REVOKE_RETRY_MAX_SECONDS = 3600.0
MAX_REVOKE_ATTEMPTS = 12
#: Seconds between looks at an operation's row and pod while it runs.
POLL_SECONDS = 0.5
OBSERVE_SECONDS = 3.0
#: A pod that ended without posting gets this long for a late post.
LATE_RESULT_SECONDS = 5.0
#: An outcome no runner read this long after it was posted was orphaned.
UNREAD_OUTCOME_SECONDS = 120.0
#: The most a posted outcome may hold (the driver's output cap plus framing).
MAX_RESULT_BYTES = 1024 * 1024 + 64 * 1024
_CAPACITY_LOCK = "srw-connector-bind-time-capacity"
#: Work per leader pass.
REVOKES_PER_PASS = 10
STARTS_PER_PASS = 10
#: Executions whose selection and authorization a pass checks again.
ACCESS_CHECKS_PER_PASS = 20
#: The most of a driver's own message SRW keeps and shows.
MAX_DRIVER_MESSAGE = 500
#: What a reader who may not read a driver's own text sees instead.
DRIVER_MESSAGE_WITHHELD = (
    "its driver reported an error (the connector's owner sees the driver's "
    "message on the connector)"
)
#: Retention of finished rows: what the connector page shows.
RETENTION_DAYS = 30
#: Waiting reasons that mean the image never runs.
_PULL_FAILURES = frozenset(
    {"ErrImagePull", "ImagePullBackOff", "InvalidImageName", "ErrImageNeverPull"}
)


class BindTimeError(LeaseDeliveryError):
    """A registered driver's binding cannot be delivered (yet)."""


class BindTimePending(BindTimeError):
    """The bind is still running (or waits to retry); the delivery is
    refused for a retry."""


class BindTimeRefused(BindTimeError):
    """The bind failed for good: a refused image, a driver error or SRW's
    refusal."""


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
    wait_seconds: float = 20.0
    max_spec_pods_per_user: int = 2


# =============================================================================
# Kubernetes effects
# =============================================================================


def _pull_failure(status: Any) -> str | None:
    """Why the pod's image cannot be pulled, if that is why it waits."""
    for key in ("initContainerStatuses", "containerStatuses"):
        for item in _field(status, key) or []:
            waiting = _field(_field(item, "state"), "waiting")
            reason = _field(waiting, "reason")
            if reason in _PULL_FAILURES:
                return f"the driver image could not be pulled ({reason})"
    return None


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
        pulling = _pull_failure(status)
        if pulling is not None:
            # It would wait for its deadline: say why now.
            return PodState(
                phase="Failed",
                uid=_field(_field(pod, "metadata"), "uid"),
                message=pulling,
            )
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


def _json(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


def _repository(reference: str) -> str:
    return ImageReference.parse(reference).name


#: Control and invisible formatting characters a driver's text loses.
_CONTROL = re.compile(
    r"[\x00-\x1f\x7f-\x9f\u200b-\u200f\u2028-\u202e\u2060-\u206f\ufeff]"
)


def driver_text(value: Any) -> str:
    """A driver's own message as SRW stores and shows it: no control or
    invisible formatting characters, whitespace collapsed, at most
    :data:`MAX_DRIVER_MESSAGE` characters."""
    text = " ".join(_CONTROL.sub(" ", str(value or "")).split())
    if len(text) > MAX_DRIVER_MESSAGE:
        text = text[: MAX_DRIVER_MESSAGE - 1] + "\u2026"
    return text or "The connector driver reported an error without a message"


@dataclass(frozen=True, slots=True)
class DriverAuthoredError(DriverError):
    """An error line the driver itself wrote: its message is the driver's
    text (shown only to the connector's owner and administrators), never
    SRW's."""


def _outcome_from_post(posted: Any, operation: str) -> DriverOutcome:
    """The outcome a shim posted, interpreted as SRW reads driver output. An
    error the driver wrote itself comes back as a
    :class:`DriverAuthoredError` with its message sanitized."""
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
    outcome = read_output(stdout, int(posted.get("exit_code", 1)), operation=operation)
    error = outcome.error
    written = [
        line.get("error")
        for line in lines
        if isinstance(line, Mapping) and line.get("type") == "error"
    ]
    if (
        error is not None
        and len(written) == 1
        and isinstance(written[0], Mapping)
        and written[0].get("message") == error.message
    ):
        outcome = replace(
            outcome,
            error=DriverAuthoredError(
                error.error_class,
                driver_text(error.message),
                error.detail,
                error.field,
                error.retry_after_s,
            ),
        )
    return outcome


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
        requested_by: str | None,
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
                if pod.operation == "spec" and requested_by:
                    mine = await conn.fetchval(
                        "SELECT count(*) FROM connector_driver_operations "
                        "WHERE removed_at IS NULL AND operation = 'spec' "
                        "AND requested_by = $1",
                        UUID(requested_by),
                    )
                    if int(mine) >= self.settings.max_spec_pods_per_user:
                        raise BindTimeCapacity(
                            f"you have {int(mine)} driver spec pods running; "
                            "try again when they finish"
                        )
                token = mint_token(DRIVER_IDENTITY_PREFIX)
                await conn.execute(
                    """
                    INSERT INTO connector_driver_operations
                        (id, token_hash, token_last_four, operation,
                         registration_id, connector_id, binding_id, requested_by,
                         image_reference, image_digest, pod_namespace, pod_name,
                         deadline_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
                            now() + make_interval(secs => $13::float8))
                    """,
                    UUID(pod.operation_id),
                    token_digest(token),
                    last_four(token),
                    pod.operation,
                    UUID(registration_id) if registration_id else None,
                    UUID(pod.connector_id) if pod.connector_id else None,
                    UUID(binding_id) if binding_id else None,
                    UUID(requested_by) if requested_by else None,
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
                        state.message
                        if state.message and "could not be pulled" in state.message
                        else "the driver pod ended without posting a result"
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
        requested_by: str | None = None,
    ) -> DriverOutcome:
        """Run ``operation`` in its own pod; the outcome, or SRW's error.

        Raises :class:`BindTimeUnavailable` without hosting and
        :class:`BindTimeCapacity` at the cap; every other failure is an
        error in the outcome: ``transient`` when the pod could not run or
        answer, ``system`` when SRW cannot launch it at all. The posted
        outcome is cleared from its row once read, except a bind's: that is
        cleared when the binding settles (:func:`_settle`), so a runner that
        dies in between leaves the leader the driver's ``driver_state``.
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
            pod,
            image=image,
            registration_id=registration_id,
            binding_id=binding_id,
            requested_by=requested_by,
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
            return DriverOutcome(
                error=DriverError("transient", f"The driver pod did not start: {exc}")
            )
        try:
            row = await self._wait(runtime, pod)
        finally:
            await self._remove(runtime, pod)
        if row is None:
            return DriverOutcome(
                error=DriverError("transient", "The driver operation vanished")
            )
        if row["status"] == "failed":
            return DriverOutcome(
                error=DriverError(
                    "transient", f"The connector driver did not answer: {row['error']}"
                )
            )
        outcome = _outcome_from_post(_decrypt(row["outcome_ciphertext"]), operation)
        if operation != "bind":
            await self._consumed(pod.operation_id)
        return outcome

    async def _consumed(self, operation_id: str) -> None:
        """The outcome was read: it stays at rest no longer."""
        async with self.store.acquire() as conn:
            await conn.execute(
                "UPDATE connector_driver_operations SET outcome_ciphertext = NULL "
                "WHERE id = $1",
                UUID(operation_id),
            )

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

#: The keys a shim posts (drivers/shim/run.go); ``protocol_error`` only
#: when the driver broke the protocol.
RESULT_KEYS = frozenset({"protocol_version", "operation", "exit_code", "lines"})
RESULT_OPTIONAL_KEYS = frozenset({"protocol_error"})


async def operation_identity(store: Any, identity_token: str) -> tuple[int, str] | None:
    """Whether ``identity_token`` may post now, before its body is read:
    ``None`` when it names a running operation within its deadline, else
    ``(status, error)``."""
    if not token_shape_valid(identity_token, DRIVER_IDENTITY_PREFIX):
        return 401, "unknown_driver_identity"
    async with store.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT status, deadline_at > now() AS open "
            "FROM connector_driver_operations WHERE token_hash = $1",
            token_digest(identity_token),
        )
    if row is None:
        return 401, "unknown_driver_identity"
    if row["status"] != "running" or not row["open"]:
        return 409, "operation_closed"
    return None


def _no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    seen: dict[str, Any] = {}
    for key, value in pairs:
        if key in seen:
            raise ValueError(f"duplicate key {key!r}")
        seen[key] = value
    return seen


def parse_result_body(raw: bytes) -> dict[str, Any] | None:
    """A shim's post read strictly: JSON with no duplicate key anywhere, the
    shim's keys exactly, typed. ``None`` when it is anything else."""
    try:
        posted = json.loads(raw.decode("utf-8"), object_pairs_hook=_no_duplicates)
    except (UnicodeDecodeError, ValueError, RecursionError):
        return None
    if not isinstance(posted, dict):
        return None
    keys = set(posted)
    if not RESULT_KEYS <= keys or keys - RESULT_KEYS - RESULT_OPTIONAL_KEYS:
        return None
    lines = posted["lines"]
    exit_code = posted["exit_code"]
    error = posted.get("protocol_error")
    if (
        not isinstance(posted["protocol_version"], str)
        or len(posted["protocol_version"]) > 16
        or posted["operation"] not in ("spec", "check", "bind", "revoke", "gc")
        or isinstance(exit_code, bool)
        or not isinstance(exit_code, int)
        or not isinstance(lines, list)
        or len(lines) > 20000
        or not all(isinstance(line, dict) for line in lines)
        or (error is not None and not isinstance(error, str))
    ):
        return None
    return posted


async def record_operation_result(
    store: Any, *, identity_token: str, posted: Mapping[str, Any]
) -> tuple[int, dict[str, Any]]:
    """Store what one operation pod's shim posted; ``(status, body)``.

    The pod's identity authenticates it and names the operation (the request
    never does). Each identity posts once, while its operation runs and
    before its deadline; the outcome is stored encrypted (a bind's result
    holds what reaches the workspace) until its runner reads it.
    """
    if not token_shape_valid(identity_token, DRIVER_IDENTITY_PREFIX):
        return 401, {"error": "unknown_driver_identity"}
    outcome = {
        "exit_code": posted["exit_code"],
        "lines": list(posted["lines"]),
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
            posted["exit_code"],
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
SELECT id, status, attempt, read_only, image_reference, image_digest,
       delivery_ciphertext, error_class, error_message, error_source, retry_at,
       created_at
  FROM connector_bind_time_bindings
 WHERE owner_kind = $1 AND owner_id = $2 AND connector_id = $3
 ORDER BY created_at DESC
 LIMIT 1
"""

Decision = Literal["start", "pending", "bound", "waiting", "failed"]


async def _latest(conn: Any, owner: LeaseOwner, connector_id: str) -> Any:
    return await conn.fetchrow(
        _LATEST, owner.kind, UUID(owner.id), UUID(str(connector_id))
    )


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _aware(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def decide(row: Any) -> Decision:
    """What a delivery does with the newest binding of an execution and a
    connector: ``start`` a bind, wait for one ``pending``, deliver a
    ``bound`` one, wait for a transient failure's retry (``waiting``), or
    stop at a final one (``failed``)."""
    if row is None:
        return "start"
    if row["status"] == "pending":
        return "pending"
    if row["status"] == "bound":
        return "bound"
    if row["error_message"]:
        if row["retry_at"] is None:
            return "failed"
        if _aware(row["retry_at"]) > _now():
            return "waiting"
    # Revoked for a reason other than a failure (a detach, a changed
    # connector, an ended execution), or a retry that is due.
    return "start"


def _next_attempt(row: Any) -> int:
    if row is not None and row["error_message"] and row["error_class"] == "transient":
        return int(row["attempt"] or 0) + 1
    return 1


def _backoff(attempt: int, base: float, ceiling: float) -> float:
    return min(base * (2 ** max(0, attempt - 1)), ceiling)


async def _execution(conn: Any, owner: LeaseOwner) -> ExecutionRef:
    from orchestrator.services.workspace_tier_policy import (
        backend_from_override,
        thread_workspace_backend,
    )

    if owner.kind == "job":
        row = await conn.fetchrow(
            "SELECT project_id, config_override FROM jobs WHERE id = $1",
            UUID(owner.id),
        )
        backend = backend_from_override(row["config_override"]) if row else None
    else:
        row = await conn.fetchrow(
            "SELECT project_id, metadata FROM threads WHERE id = $1", UUID(owner.id)
        )
        backend = thread_workspace_backend(dict(row)) if row else None
    project_id = row["project_id"] if row else None
    return ExecutionRef(
        kind="job" if owner.kind == "job" else "session",
        id=owner.id,
        project_id=str(project_id) if project_id else None,
        workspace_backend=backend,
    )


async def _fail(
    store: Any,
    binding_id: str,
    *,
    error_class: str,
    message: str,
    attempt: int,
    source: str | None = None,
) -> None:
    """End a bind that minted nothing: a transient failure waits for a
    retry (until :data:`MAX_BIND_ATTEMPTS`), any other is final. A revoke
    asked meanwhile retires it instead. ``source`` is ``driver`` when
    ``message`` is the driver's own text. A posted outcome stays for the
    leader (:func:`_recover_unread`): a result line in it is revoked."""
    retry_at: datetime | None = None
    if error_class == "transient":
        if attempt < MAX_BIND_ATTEMPTS:
            retry_at = _now() + timedelta(
                seconds=_backoff(
                    attempt, BIND_RETRY_BASE_SECONDS, BIND_RETRY_MAX_SECONDS
                )
            )
        else:
            message = f"{message} (gave up after {attempt} attempts)"
    async with store.acquire() as conn:
        await conn.execute(
            """
            UPDATE connector_bind_time_bindings
               SET status = CASE WHEN revoke_requested_at IS NULL
                                 THEN 'failed' ELSE 'revoked' END,
                   revoked_at = CASE WHEN revoke_requested_at IS NULL
                                     THEN NULL ELSE now() END,
                   failed_at = now(), error_class = $2, error_message = $3,
                   retry_at = $4, inputs_ciphertext = NULL, error_source = $5
             WHERE id = $1 AND status = 'pending'
            """,
            UUID(binding_id),
            error_class,
            message[:1000],
            retry_at,
            source,
        )


async def _settle(
    store: Any,
    binding_id: str,
    *,
    delivery: Mapping[str, Any] | None,
    driver_state: str | None,
    refusal: str | None = None,
) -> str:
    """End a bind whose driver answered with a binding: ``bound``, or
    ``revoking`` when SRW refused the binding, a revoke was asked while it
    ran, or the reconciler gave up on it meanwhile. Whatever it minted is
    never dropped: its ``driver_state`` stays for the revoke. The bind's
    posted outcome is cleared in the same transaction, never before."""
    state = _encrypt(driver_state) if driver_state is not None else None
    async with store.acquire() as conn, conn.transaction():
        await conn.execute(
            "UPDATE connector_driver_operations SET outcome_ciphertext = NULL "
            "WHERE binding_id = $1 AND operation = 'bind'",
            UUID(binding_id),
        )
        if refusal is None and delivery is not None:
            bound = await conn.fetchval(
                """
                UPDATE connector_bind_time_bindings
                   SET status = 'bound', bound_at = now(), delivery_ciphertext = $2,
                       driver_state_ciphertext = $3
                 WHERE id = $1 AND status = 'pending' AND revoke_requested_at IS NULL
                RETURNING id
                """,
                UUID(binding_id),
                _encrypt(dict(delivery)),
                state,
            )
            if bound is not None:
                return "bound"
        await conn.execute(
            """
            UPDATE connector_bind_time_bindings
               SET status = 'revoking',
                   revoke_requested_at = COALESCE(revoke_requested_at, now()),
                   revoke_reason = COALESCE(
                       revoke_reason,
                       CASE WHEN $3::text IS NULL THEN 'bind_orphaned'
                            ELSE 'binding_refused' END),
                   driver_state_ciphertext = $2,
                   error_class = CASE WHEN $3::text IS NULL THEN error_class
                                      ELSE 'system' END,
                   error_message = COALESCE($3, error_message),
                   failed_at = CASE WHEN $3::text IS NULL THEN failed_at
                                    ELSE now() END,
                   retry_at = CASE WHEN $3::text IS NULL THEN retry_at ELSE NULL END
             WHERE id = $1 AND status IN ('pending', 'failed')
            """,
            UUID(binding_id),
            state,
            refusal[:1000] if refusal else None,
        )
    return "revoking"


async def _previous_bind(conn: Any, connector_id: str) -> Any:
    """The connector's newest bind that delivered: its digest and spec."""
    return await conn.fetchrow(
        """
        SELECT image_digest, spec FROM connector_bind_time_bindings
         WHERE connector_id = $1 AND bound_at IS NOT NULL
           AND image_digest IS NOT NULL AND spec IS NOT NULL
         ORDER BY bound_at DESC LIMIT 1
        """,
        UUID(str(connector_id)),
    )


@dataclass
class CheckedImage:
    """The image a bind or Test runs and the spec it runs with."""

    image: BoundImage
    spec_json: Mapping[str, Any]
    spec: DriverSpec | None
    env_names: tuple[str, ...]
    problems: list[str]


async def check_image(
    runtime: BindTimeRuntime, registration: Any, connector_id: str
) -> CheckedImage:
    """Resolve ``registration``'s reference and check the image against the
    spec the connector last bound with (its registration's before any
    bind). Raises ``ServiceImageRefused``/``ServiceImageUnavailable`` as
    the resolution does; a refused contract is in ``problems`` (audited)."""
    image = await resolve_driver_image(
        runtime.store, driver=registration.name, reference=registration.image_reference
    )
    async with runtime.store.acquire() as conn:
        previous = await _previous_bind(conn, connector_id)
        if previous is not None:
            previous_digest = str(previous["image_digest"])
            previous_spec = _json(previous["spec"]) or {}
        else:
            previous_digest = registration.image_digest
            previous_spec = registration.spec_json
        moved = image.digest != previous_digest
        problems: list[str] = []
        if moved:
            problems = moved_spec_problems(previous_spec, image.spec)
            spec_json: Mapping[str, Any] = image.spec or previous_spec
        else:
            spec_json = previous_spec
        spec: DriverSpec | None = None
        env_names: tuple[str, ...] = ()
        try:
            spec = spec_from_json(spec_json)
            env_names = declared_env_names(spec_json)
        except ValueError as exc:
            if not problems:
                problems = [f"its spec is malformed ({exc})"]
        if spec is not None:
            # Every reason at once: the contract, the rules, the stored config.
            problems += custom_driver_problems(
                spec,
                privileged=runtime.privileged(registration.image_reference),
                env_names=env_names,
            )
            safe = not schema_problems(spec.config_schema, "config_schema")
            if moved and safe:
                config = await conn.fetchval(
                    "SELECT config FROM datasources WHERE id = $1",
                    UUID(str(connector_id)),
                )
                problems += [
                    f"the stored config no longer validates: {error}"
                    for error in config_errors(spec.config_schema, _json(config) or {})
                ]
        problems = list(dict.fromkeys(problems))
        if problems:
            await record_lease_event(
                conn,
                event_type="connector_driver_image_refused",
                resource_type="connector",
                resource_id=str(UUID(str(connector_id))),
                detail=(
                    f"driver={registration.name} reference={image.reference} "
                    f"digest={image.digest} problems={'; '.join(problems)}"
                ),
            )
    return CheckedImage(
        image=image,
        spec_json=spec_json,
        spec=spec,
        env_names=env_names,
        problems=problems,
    )


async def _bind(
    runtime: BindTimeRuntime,
    owner: LeaseOwner,
    connector_id: str,
    *,
    read_only: bool,
    attempt: int,
) -> None:
    """Run one binding of ``owner`` for ``connector_id`` to its end. An
    unexpected error ends the binding ``failed`` (``system``) with its
    traceback logged: a job never waits on a binding nobody runs."""
    from orchestrator.services.connector_driver_registrations import (
        registration_for_connector,
    )

    store = runtime.store
    row = await store.get_datasource(str(connector_id))
    if row is None:
        return
    registration = await registration_for_connector(store, connector_id)
    async with store.acquire() as conn:
        binding_id = await conn.fetchval(
            """
            INSERT INTO connector_bind_time_bindings
                (owner_kind, owner_id, connector_id, registration_id, driver,
                 attempt, read_only)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (owner_kind, owner_id, connector_id)
                WHERE status IN ('pending', 'bound') DO NOTHING
            RETURNING id
            """,
            owner.kind,
            UUID(owner.id),
            UUID(str(connector_id)),
            UUID(registration.id) if registration else None,
            registration.name if registration else IMAGE_DRIVER_SPEC.name,
            attempt,
            read_only,
        )
    if binding_id is None:
        return  # another delivery binds it
    binding_id = str(binding_id)
    try:
        await _run_bind(
            runtime,
            owner,
            str(connector_id),
            row=row,
            registration=registration,
            binding_id=binding_id,
            read_only=read_only,
            attempt=attempt,
        )
    except Exception:
        logger.exception(
            "The bind of connector %s for %s %s failed unexpectedly",
            connector_id,
            owner.kind,
            owner.id,
        )
        await _fail(
            store,
            binding_id,
            error_class="system",
            message="SRW could not finish the bind (an internal error; the "
            "orchestrator log has the details)",
            attempt=attempt,
        )


async def _run_bind(
    runtime: BindTimeRuntime,
    owner: LeaseOwner,
    connector_id: str,
    *,
    row: Mapping[str, Any],
    registration: Any,
    binding_id: str,
    read_only: bool,
    attempt: int,
) -> None:
    """:func:`_bind`'s work once its binding row exists."""
    from orchestrator.services.connector_secrets import read_connector_credentials

    store = runtime.store
    row = dict(row)

    async def fail(error_class: str, message: str, source: str | None = None) -> None:
        await _fail(
            store,
            binding_id,
            error_class=error_class,
            message=message,
            attempt=attempt,
            source=source,
        )

    if registration is None:
        await fail(
            "config", "This connector's driver registration is gone; it cannot bind"
        )
        return
    if registration.disabled:
        await fail(
            "config",
            f"The driver registration {registration.name} is disabled; it binds "
            "nothing new",
        )
        return
    try:
        checked = await check_image(runtime, registration, str(connector_id))
    except ServiceImageRefused as exc:
        await fail("config", str(exc))
        return
    except ServiceImageUnavailable as exc:
        await fail("transient", str(exc))
        return
    if checked.problems or checked.spec is None:
        await fail(
            "config", refusal_message(registration.image_reference, checked.problems)
        )
        return
    spec, image = checked.spec, checked.image
    await read_connector_credentials(
        [row], authorized=[str(row["id"])], dependencies=SimpleNamespace(store=store)
    )
    credentials = (
        row.get("credentials") if isinstance(row.get("credentials"), dict) else {}
    )
    config = dict(row.get("config") or {})
    access = effective_access({"project_read_only": read_only, "config": config}, spec)
    hosting = runtime.operations.settings.hosting
    async with store.acquire() as conn:
        execution = await _execution(conn, owner)
        private_allowed = await private_addresses_allowed(
            conn, str(connector_id), private_tiers=hosting.private_tiers
        )
        # Everything the revoke needs, before anything can be minted: the
        # image at its digest, the spec it runs with, the inputs.
        await conn.execute(
            """
            UPDATE connector_bind_time_bindings
               SET image_reference = $2, image_digest = $3, image_stale = $4,
                   resolved_at = $5, spec = $6::jsonb, spec_hash = $7,
                   protocol_version = $8, access = $9, inputs_ciphertext = $10,
                   driver = $11
             WHERE id = $1 AND status = 'pending'
            """,
            UUID(binding_id),
            image.reference,
            image.digest,
            bool(image.stale),
            image.resolved_at,
            json.dumps(dict(checked.spec_json)),
            spec_hash(checked.spec_json),
            spec.protocol_version,
            access,
            _encrypt({"config": config, "credentials": credentials}),
            spec.name,
        )
    request = DriverRequest(
        operation="bind",
        config=config,
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
            config=config,
            connector_id=str(connector_id),
            registration_id=registration.id,
            binding_id=binding_id,
            private_allowed=private_allowed,
        )
    except BindTimeCapacity as exc:
        await fail("transient", str(exc))
        return
    except BindTimeUnavailable as exc:
        # The installation may run driver pods again (a rollout, a config
        # change): retried like any outage, within MAX_BIND_ATTEMPTS.
        await fail("transient", str(exc))
        return
    if outcome.error is not None:
        if outcome.error.detail:
            logger.warning(
                "Driver %s bind for connector %s failed: %s (%s)",
                spec.name,
                connector_id,
                outcome.error.message,
                driver_text(outcome.error.detail),
            )
        authored = isinstance(outcome.error, DriverAuthoredError)
        await fail(
            outcome.error.error_class,
            driver_text(outcome.error.message) if authored else outcome.error.message,
            "driver" if authored else None,
        )
        return
    if outcome.updates:
        logger.warning(
            "Driver %s returned %d update line(s); this release does not apply them",
            spec.name,
            len(outcome.updates),
        )
    descriptor = (outcome.result or {}).get("binding")
    problems = image_binding_problems(descriptor, spec, env_names=checked.env_names)
    if problems:
        # It minted something SRW will not deliver: revoke it.
        message = "The driver returned a binding SRW will not deliver: " + "; ".join(
            problems[:5]
        )
        await _settle(
            store,
            binding_id,
            delivery=None,
            driver_state=outcome.driver_state,
            refusal=message,
        )
        return
    settled = await _settle(
        store,
        binding_id,
        delivery=wire_credentials(descriptor),
        driver_state=outcome.driver_state,
    )
    logger.info(
        "Driver %s %s connector %s for %s %s at %s",
        spec.name,
        "bound" if settled == "bound" else "bound (and is revoking)",
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
    read_only: bool = False,
    wait: float | None = None,
) -> Decision:
    """Start the binding of ``owner`` for ``connector_id`` unless one is live,
    waits for a retry or failed for good, and wait for it at most ``wait``
    seconds (the bind goes on past that). Never raises; returns the
    decision for the newest binding once the wait is over."""
    key = (owner.kind, owner.id, str(connector_id))
    task = runtime.inflight.get(key)
    if task is None or task.done():
        async with runtime.store.acquire() as conn:
            row = await _latest(conn, owner, connector_id)
        decision = decide(row)
        if decision in ("bound", "waiting", "failed"):
            return decision
        if decision == "start":
            task = asyncio.create_task(
                _bind(
                    runtime,
                    owner,
                    str(connector_id),
                    read_only=read_only,
                    attempt=_next_attempt(row),
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
    if task is not None and limit > 0:
        try:
            await asyncio.wait_for(asyncio.shield(task), timeout=limit)
        except (TimeoutError, asyncio.TimeoutError):
            pass
        except Exception:
            pass
    elif task is None:
        end = time.monotonic() + limit
        while time.monotonic() < end:
            async with runtime.store.acquire() as conn:
                row = await _latest(conn, owner, connector_id)
            if decide(row) != "pending":
                break
            await asyncio.sleep(POLL_SECONDS)
    async with runtime.store.acquire() as conn:
        return decide(await _latest(conn, owner, connector_id))


#: Binds a delivery started without waiting, kept until they finish.
_background: set[asyncio.Task] = set()


def _start_in_background(
    runtime: BindTimeRuntime, owner: LeaseOwner, connector_id: str, read_only: bool
) -> None:
    started = asyncio.create_task(
        ensure_binding(runtime, owner, connector_id, read_only=read_only, wait=0)
    )
    _background.add(started)
    started.add_done_callback(_background.discard)


def _uuid(value: Any) -> bool:
    try:
        UUID(str(value))
    except ValueError:
        return False
    return True


async def prepare_bind_time_bindings(
    entries: Sequence[Any] | None, *, owner: LeaseOwner, wait: float | None = None
) -> None:
    """Bind every registered driver's connector in ``entries`` before the
    delivery opens its transaction, waiting at most ``wait`` seconds
    (``wait_seconds`` when ``None``) for a new bind. Never raises."""
    runtime = bind_time_runtime()
    if runtime is None:
        return
    wanted = [
        entry
        for entry in entries or ()
        if registered_entry(entry) and _uuid(entry.get("datasource_id"))
    ]
    if not wanted:
        return
    await asyncio.gather(
        *(
            ensure_binding(
                runtime,
                owner,
                str(entry.get("datasource_id")),
                read_only=bool(entry.get("project_read_only")),
                wait=wait,
            )
            for entry in wanted
        ),
        return_exceptions=True,
    )


_JOB_TARGETS = """
SELECT a.connector_id, COALESCE(pd.read_only, false) AS read_only, d.name
  FROM job_datasources jd
  JOIN jobs j ON j.id = jd.job_id
  JOIN datasources d ON d.id = jd.datasource_id
  JOIN connector_driver_assignments a ON a.connector_id = jd.datasource_id
  LEFT JOIN project_datasources pd
    ON pd.datasource_id = a.connector_id AND pd.project_id = j.project_id
 WHERE jd.job_id = $1
"""
_THREAD_TARGETS = """
SELECT a.connector_id, COALESCE(pd.read_only, false) AS read_only, NULL AS name
  FROM threads t
  JOIN connector_driver_assignments a
    ON COALESCE(t.metadata->'datasource_ids', '[]'::jsonb) ? a.connector_id::text
  LEFT JOIN project_datasources pd
    ON pd.datasource_id = a.connector_id AND pd.project_id = t.project_id
 WHERE t.id = $1
"""


async def _targets(conn: Any, kind: str, execution_id: str) -> list[Any]:
    """The registered connectors an execution selected (``connector_id``,
    ``read_only``: whether its project links one read-only, a multi-project
    session's other links checked again at delivery; ``name``)."""
    return await conn.fetch(
        _JOB_TARGETS if kind == "job" else _THREAD_TARGETS, UUID(execution_id)
    )


async def prepare_thread_bindings(
    store: Any,
    thread_id: str,
    *,
    wait: float | None = None,
    only: Sequence[str] | None = None,
) -> None:
    """Start a session's binds and wait for them at most ``wait`` seconds
    (``wait_seconds`` when ``None``), before its attach reserves an agent or
    answers the agent's poll (under the agent's 30 s request); ``only``
    narrows them to those connectors. Never raises."""
    runtime = bind_time_runtime()
    if runtime is None or not _uuid(thread_id):
        return
    try:
        async with runtime.store.acquire() as conn:
            targets = await _targets(conn, "thread", str(thread_id))
        if only is not None:
            wanted = _canonical_ids(only)
            targets = [
                target
                for target in targets
                if str(target["connector_id"]).lower() in wanted
            ]
        if targets:
            await asyncio.gather(
                *(
                    ensure_binding(
                        runtime,
                        LeaseOwner.thread(str(thread_id)),
                        str(target["connector_id"]),
                        read_only=bool(target["read_only"]),
                        wait=wait,
                    )
                    for target in targets
                ),
                return_exceptions=True,
            )
    except Exception as exc:
        logger.warning("Preparing session %s binds failed: %s", thread_id, exc)


def start_thread_bindings(
    thread_id: str, connector_ids: Sequence[str] | None = None
) -> None:
    """Start a session's binds in the background (a live selection), without
    waiting: of ``connector_ids`` only (the ones a live update added) when
    given; a connector of no registered driver starts nothing. No-op
    without driver pods."""
    if bind_time_runtime() is None:
        return
    started = asyncio.create_task(
        prepare_thread_bindings(None, thread_id, wait=0, only=connector_ids)
    )
    _background.add(started)
    started.add_done_callback(_background.discard)


_READS_DRIVER_TEXT = """
SELECT COALESCE(d.created_by = u.id OR u.is_admin, false)
  FROM datasources d
  LEFT JOIN users u ON u.id = (
      CASE WHEN $2 = 'job' THEN (SELECT user_id FROM jobs WHERE id = $3)
           ELSE (SELECT user_id FROM threads WHERE id = $3) END)
 WHERE d.id = $1
"""


async def _reads_driver_text(conn: Any, owner: LeaseOwner, connector_id: str) -> bool:
    """Whether the execution's owner may read the connector's driver's own
    messages: the connector's owner, or an administrator. Anyone else (a
    project member using a shared connector, and that execution's agent)
    sees :data:`DRIVER_MESSAGE_WITHHELD`."""
    return bool(
        await conn.fetchval(
            _READS_DRIVER_TEXT, UUID(str(connector_id)), owner.kind, UUID(owner.id)
        )
    )


async def _shown_error(
    conn: Any, owner: LeaseOwner, connector_id: str, row: Mapping[str, Any]
) -> str:
    """A binding's error as the execution's owner may read it."""
    message = str(row["error_message"] or "")
    if row["error_source"] == "driver" and not await _reads_driver_text(
        conn, owner, connector_id
    ):
        return DRIVER_MESSAGE_WITHHELD
    return message


GateAction = Literal["dispatch", "wait", "fail"]


async def job_bind_gate(job: Mapping[str, Any]) -> tuple[GateAction, str | None]:
    """The dispatcher's preflight for a job's binds, before it claims the
    job: ``dispatch`` once every registered connector is bound, ``wait``
    while a bind runs or waits to retry (it is started here, without
    waiting: the dispatch loop never blocks), ``fail`` with the driver's
    reason once one failed for good. Without driver pods it dispatches,
    and the delivery refuses."""
    runtime = bind_time_runtime()
    if runtime is None:
        return "dispatch", None
    owner = job_lease_owner(job)
    async with runtime.store.acquire() as conn:
        targets = await _targets(conn, "job", str(job["id"]))
        rows = [
            (target, await _latest(conn, owner, str(target["connector_id"])))
            for target in targets
        ]
        failed = next(
            ((target, row) for target, row in rows if decide(row) == "failed"), None
        )
        if failed is not None:
            target, row = failed
            shown = await _shown_error(conn, owner, str(target["connector_id"]), row)
            return "fail", f"Connector {target['name']}: {shown}"
    action: GateAction = "dispatch"
    for target, row in rows:
        decision = decide(row)
        if decision != "bound":
            action = "wait"
        if decision == "start":
            await ensure_binding(
                runtime,
                owner,
                str(target["connector_id"]),
                read_only=bool(target["read_only"]),
                wait=0,
            )
    return action, None


def _notice(decision: Decision, message: str) -> str:
    """Why a session's registered connector is not in its workspace
    (``message``: the binding's error as the session's owner reads it)."""
    if decision == "failed":
        return f"Not delivered: {message}"
    if decision == "waiting":
        return (
            f"Not delivered yet: {message} (SRW tries again; it arrives at a "
            "later attach or connector change)"
        )
    return (
        "Not delivered yet: its driver is still binding; it arrives at a later "
        "attach or connector change"
    )


async def deliver_bind_time_entries(
    conn: Any, entries: Sequence[Any] | None, *, owner: LeaseOwner
) -> int:
    """Fill every registered driver's entry with its bound delivery, in place.

    Runs on the delivery's connection, in its transaction. A bound binding
    of the entry's access fills it (one of another access is revoked and
    bound again). Otherwise a bind is started where none runs, and a job's
    delivery raises :class:`BindTimePending` (one runs or waits to retry) or
    :class:`BindTimeRefused` (it failed for good); a session's skips the
    entry with a notice in its ``cli_hint`` (the workspace README's line).
    A binding that sets a variable another connector of the execution sets
    (an environment connector's, a file's ``env_var``, or an earlier
    registered driver's, by connector id) is never delivered: a session
    skips it with a notice, a job's delivery is refused with the reason, so
    the agent never receives two values for one name. Returns how many
    entries were filled.
    """
    filled = 0
    runtime = bind_time_runtime()
    wanted = [entry for entry in entries or () if registered_entry(entry)]
    wanted.sort(key=lambda entry: str(entry.get("datasource_id") or "").lower())
    taken: dict[str, str] = {}
    for entry in entries or ():
        if isinstance(entry, Mapping) and not registered_entry(entry):
            for name in _entry_env_names(entry.get("credentials")):
                taken.setdefault(name, _connector_label(entry))
    for entry in wanted:
        entry["credentials"] = {}
        connector_id = str(entry.get("datasource_id") or "")
        if not _uuid(connector_id):
            raise BindTimeRefused("A registered driver's entry names no connector")
        read_only = bool(entry.get("project_read_only"))
        row = await _latest(conn, owner, connector_id)
        decision = decide(row)
        if decision == "bound":
            delivery = _decrypt(row["delivery_ciphertext"])
            stale = None
            if bool(row["read_only"]) != read_only:
                stale = "access_changed"  # the project link's access changed
            elif not isinstance(delivery, dict):
                stale = "delivery_unreadable"
            if stale is None:
                names = _entry_env_names(delivery)
                clash = sorted(name for name in names if name in taken)
                if clash:
                    message = (
                        f"it sets {', '.join(clash)}, which connector "
                        f"{taken[clash[0]]} sets too"
                    )
                    if owner.kind == "thread":
                        entry["cli_hint"] = f"Not delivered: {message}"
                        continue
                    raise BindTimeRefused(
                        f"Connector {_connector_label(entry)}: {message}"
                    )
                entry["credentials"] = delivery
                for name in names:
                    taken[name] = _connector_label(entry)
                filled += 1
                continue
            if runtime is not None:
                # On the store's own connection: this transaction may roll
                # back, and the revoke must hold for the next bind to start.
                async with runtime.store.acquire() as own:
                    await _request_revoke(own, "id = $1", [row["id"]], reason=stale)
            decision = "start"
        if runtime is None:
            message = (
                "This installation runs no driver pods; a registered driver's "
                "connector cannot bind (connectors.servicePods.enabled)"
            )
            if owner.kind == "thread":
                entry["cli_hint"] = f"Not delivered: {message}"
                continue
            raise BindTimeRefused(message)
        if decision == "start":
            # Started on the store's own connections, never this transaction's.
            _start_in_background(runtime, owner, connector_id, read_only)
        shown = (
            await _shown_error(conn, owner, connector_id, row)
            if row is not None and row["error_message"]
            else ""
        )
        if owner.kind == "thread":
            entry["cli_hint"] = _notice(decision, shown)
            continue
        if decision == "failed":
            raise BindTimeRefused(shown)
        raise BindTimePending("The connector's driver is still binding; retry shortly")
    return filled


def _connector_label(entry: Mapping[str, Any]) -> str:
    return str(entry.get("name") or entry.get("datasource_id") or "another")


def _entry_env_names(credentials: Any) -> set[str]:
    """The variables an entry's credentials set: its ``env_vars`` and every
    file's ``env_var``."""
    if not isinstance(credentials, Mapping):
        return set()
    names: set[str] = set()
    variables = credentials.get("env_vars")
    if isinstance(variables, Mapping):
        names.update(str(name) for name in variables)
    for item in credentials.get("files") or ():
        if isinstance(item, Mapping) and item.get("env_var"):
            names.add(str(item["env_var"]))
    return names


# =============================================================================
# Revocation requests
# =============================================================================


async def _request_revoke(
    conn: Any, where: str, args: Sequence[Any], *, reason: str
) -> int:
    """Revoke the live bindings ``where`` selects (``$1``.. are ``args``):
    a bound one is revoked by the next pass, a pending one when its bind
    ends. Returns how many."""
    reason_at = len(args) + 1
    bound = await conn.fetch(
        f"""
        UPDATE connector_bind_time_bindings
           SET status = 'revoking', revoke_requested_at = now(),
               revoke_reason = ${reason_at}
         WHERE status = 'bound' AND ({where})
        RETURNING id
        """,
        *args,
        reason,
    )
    pending = await conn.fetch(
        f"""
        UPDATE connector_bind_time_bindings
           SET revoke_requested_at = now(), revoke_reason = ${reason_at}
         WHERE status = 'pending' AND revoke_requested_at IS NULL AND ({where})
        RETURNING id
        """,
        *args,
        reason,
    )
    return len(bound) + len(pending)


async def revoke_owner_bindings(
    conn: Any, *, owner: LeaseOwner, connector_ids: Sequence[str], reason: str
) -> int:
    """Revoke an execution's bindings of ``connector_ids`` (a live detach)."""
    ids = [UUID(str(value)) for value in connector_ids if _uuid(value)]
    if not ids:
        return 0
    return await _request_revoke(
        conn,
        "owner_kind = $1 AND owner_id = $2 AND connector_id = ANY($3::uuid[])",
        [owner.kind, UUID(owner.id), ids],
        reason=reason,
    )


async def connector_changed(conn: Any, connector_id: str) -> int:
    """A connector's config or credentials changed: revoke every binding of
    it, and give a failed one a fresh try at the next delivery."""
    if not _uuid(connector_id):
        return 0
    uid = UUID(str(connector_id))
    await conn.execute(
        """
        UPDATE connector_bind_time_bindings
           SET retry_at = now(), attempt = 0
         WHERE connector_id = $1 AND error_message IS NOT NULL
           AND (retry_at IS NULL OR retry_at > now())
        """,
        uid,
    )
    return await _request_revoke(
        conn, "connector_id = $1", [uid], reason="connector_updated"
    )


async def registration_disabled(conn: Any, registration_id: str) -> int:
    """Revoke every binding of a registration that was disabled."""
    return await _request_revoke(
        conn,
        "registration_id = $1",
        [UUID(str(registration_id))],
        reason="registration_disabled",
    )


async def registration_enabled(conn: Any, registration_id: str) -> None:
    """A registration enabled again: its connectors' failed binds get a
    fresh try at the next delivery."""
    await conn.execute(
        """
        UPDATE connector_bind_time_bindings
           SET retry_at = now(), attempt = 0
         WHERE registration_id = $1 AND error_message IS NOT NULL
           AND (retry_at IS NULL OR retry_at > now())
        """,
        UUID(str(registration_id)),
    )


# =============================================================================
# Test connection and the spec operation
# =============================================================================


async def run_check(
    registration: Any, row: Mapping[str, Any], credentials: dict
) -> dict:
    """Test connection of a registered driver's connector: its ``check`` in
    a pod, on the digest its reference resolves to now, after the same
    image check a bind makes (a refused image starts no pod)."""
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
    if registration.disabled:
        return {
            "status": "error",
            "message": f"The driver registration {registration.name} is disabled",
            "error_class": "config",
        }
    try:
        checked = await check_image(runtime, registration, str(row["id"]))
    except ServiceImageRefused as exc:
        return {"status": "error", "message": str(exc), "error_class": "config"}
    except ServiceImageUnavailable as exc:
        return {"status": "error", "message": str(exc), "error_class": "transient"}
    if checked.problems or checked.spec is None:
        return {
            "status": "error",
            "message": refusal_message(registration.image_reference, checked.problems),
            "error_class": "config",
        }
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
            driver=checked.spec.name,
            image=checked.image,
            request=DriverRequest(
                operation="check",
                config=config,
                access=effective_access(row, checked.spec),
                credentials=credentials,
            ),
            spec=checked.spec,
            config=config,
            connector_id=str(row["id"]),
            registration_id=registration.id,
            private_allowed=private_allowed,
        )
    except (BindTimeCapacity, BindTimeUnavailable) as exc:
        return {"status": "error", "message": str(exc), "error_class": "transient"}
    return api_check_result(outcome)


async def run_spec_operation(
    reference: str, resolved: Any, *, requested_by: str | None = None
) -> dict[str, Any]:
    """The spec an unlabelled image answers, run in a pod with no secret
    and no egress but the result route (registration's fallback). A user
    runs at most ``max_spec_pods_per_user`` at once."""
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
            requested_by=requested_by,
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
# The leader's reconciler: start, revoke, recover, fail orphans, sweep
# =============================================================================

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
#: Live executions that select a registered connector with no binding to
#: deliver yet (a job on its parent's workspace binds as the parent, at its
#: dispatch).
_STARTS = """
SELECT 'job' AS kind, j.id AS owner_id, a.connector_id,
       COALESCE(pd.read_only, false) AS read_only
  FROM jobs j
  JOIN job_datasources jd ON jd.job_id = j.id
  JOIN connector_driver_assignments a ON a.connector_id = jd.datasource_id
  LEFT JOIN project_datasources pd
    ON pd.datasource_id = a.connector_id AND pd.project_id = j.project_id
 WHERE j.status::text NOT IN ('completed', 'failed', 'cancelled')
   AND j.parent_job_id IS NULL
   AND NOT EXISTS (SELECT 1 FROM connector_bind_time_bindings b
                    WHERE b.owner_kind = 'job' AND b.owner_id = j.id
                      AND b.connector_id = a.connector_id)
UNION ALL
SELECT 'thread', t.id, a.connector_id, COALESCE(pd.read_only, false)
  FROM threads t
  JOIN connector_driver_assignments a
    ON COALESCE(t.metadata->'datasource_ids', '[]'::jsonb) ? a.connector_id::text
  LEFT JOIN project_datasources pd
    ON pd.datasource_id = a.connector_id AND pd.project_id = t.project_id
 WHERE t.status::text <> 'ended'
   AND NOT (t.runtime_retirement_token IS NOT NULL
            AND t.runtime_retirement_authorized_at IS NOT NULL)
   AND NOT EXISTS (SELECT 1 FROM connector_bind_time_bindings b
                    WHERE b.owner_kind = 'thread' AND b.owner_id = t.id
                      AND b.connector_id = a.connector_id
                      AND (b.status IN ('pending', 'bound')
                           OR (b.error_message IS NOT NULL
                               AND (b.retry_at IS NULL OR b.retry_at > now()))))
 LIMIT $1
"""


def _by_owner(rows: Sequence[Mapping[str, Any]]) -> dict[LeaseOwner, list[Any]]:
    grouped: dict[LeaseOwner, list[Any]] = {}
    for row in rows:
        owner = LeaseOwner(str(row["kind"]), str(row["owner_id"]))
        grouped.setdefault(owner, []).append(row)
    return grouped


def _canonical_ids(values: Any) -> set[str]:
    out: set[str] = set()
    for value in values or ():
        try:
            out.add(str(UUID(str(value))))
        except ValueError:
            continue
    return out


_JOB_SELECTION = """
SELECT jd.datasource_id
  FROM job_datasources jd
  JOIN jobs j ON j.id = jd.job_id
 WHERE j.id = $1
    OR (j.parent_job_id = $1
        AND j.status::text NOT IN ('completed', 'failed', 'cancelled'))
"""


async def _lost_connectors(
    store: Any, owner: LeaseOwner, connector_ids: Sequence[str]
) -> list[str]:
    """Which of ``connector_ids`` the execution no longer selects or may no
    longer use, by the delivery's own authorization
    (``datasource_policy.classify_datasource_selection`` with the
    arguments a job's or a session's delivery passes). A job selects what
    it or a live child job on its workspace selects; a session what its
    ``metadata.datasource_ids`` holds. A vanished, unapproved or
    no-longer-member owner loses every one. A gone execution loses none
    here: its end revokes them."""
    from orchestrator.services.datasource_policy import (
        DatasourceUnavailableError,
        classify_datasource_selection,
    )
    from orchestrator.services.thread_mount_rows import durable_project_ids
    from orchestrator.services.workspace_tier_policy import backend_from_override

    wanted = sorted(_canonical_ids(connector_ids))
    if not wanted:
        return []
    kwargs: dict[str, Any] = {}
    async with store.acquire() as conn:
        if owner.kind == "job":
            execution = await conn.fetchrow(
                "SELECT user_id, project_id, config_override FROM jobs WHERE id = $1",
                UUID(owner.id),
            )
            if execution is None:
                return []
            selected = _canonical_ids(
                row["datasource_id"]
                for row in await conn.fetch(_JOB_SELECTION, UUID(owner.id))
            )
            projects = [str(execution["project_id"])] if execution["project_id"] else []
            backend = backend_from_override(_json(execution["config_override"]) or {})
            kwargs["legacy_job_id"] = owner.id
        else:
            execution = await conn.fetchrow(
                "SELECT id, user_id, project_id, metadata FROM threads WHERE id = $1",
                UUID(owner.id),
            )
            if execution is None:
                return []
            metadata = _json(execution["metadata"]) or {}
            selected = _canonical_ids(
                metadata.get("datasource_ids") if isinstance(metadata, dict) else ()
            )
            projects = None
            backend = None
    if owner.kind != "job":
        thread = {**dict(execution), "metadata": metadata}
        projects = durable_project_ids(
            thread, legacy_mounts=await store.list_thread_mounts(owner.id)
        )
    lost = [value for value in wanted if value not in selected]
    still = [value for value in wanted if value in selected]
    if not still:
        return lost
    actor_id = str(execution["user_id"]) if execution["user_id"] else None
    actor = await store.get_user(actor_id) if actor_id else None
    try:
        verdicts, _revisions = await classify_datasource_selection(
            store,
            actor,
            actor_id,
            still,
            list(projects or []),
            backend,
            allow_admin_explicit_override=True,
            trusted_system_inheritance=actor_id is None,
            **kwargs,
        )
    except DatasourceUnavailableError:
        return lost + still
    return lost + [verdict.datasource_id for verdict in verdicts if verdict.denied]


async def _access_lost(runtime: BindTimeRuntime) -> int:
    """Revoke (``access_lost``) the live bindings whose execution no longer
    selects their connector or may no longer use it (:func:`_lost_connectors`):
    a job, paused ones included, or a session whose selection, project
    membership or connector link changed by a path other than a live
    detach. At most :data:`ACCESS_CHECKS_PER_PASS` executions a pass, the
    least recently checked first."""
    store = runtime.store
    async with store.acquire() as conn:
        owners = await conn.fetch(
            """
            SELECT owner_kind AS kind, owner_id,
                   array_agg(DISTINCT connector_id) AS connector_ids
              FROM connector_bind_time_bindings
             WHERE status IN ('pending', 'bound') AND revoke_requested_at IS NULL
             GROUP BY owner_kind, owner_id
             ORDER BY bool_or(access_checked_at IS NULL) DESC,
                      min(access_checked_at)
             LIMIT $1
            """,
            ACCESS_CHECKS_PER_PASS,
        )
    revoked = 0
    for row in owners:
        owner = LeaseOwner(str(row["kind"]), str(row["owner_id"]))
        try:
            lost = await _lost_connectors(
                store, owner, [str(value) for value in row["connector_ids"]]
            )
            async with store.acquire() as conn, conn.transaction():
                if lost:
                    revoked += await revoke_owner_bindings(
                        conn, owner=owner, connector_ids=lost, reason="access_lost"
                    )
                await conn.execute(
                    """
                    UPDATE connector_bind_time_bindings
                       SET access_checked_at = now()
                     WHERE owner_kind = $1 AND owner_id = $2
                       AND status IN ('pending', 'bound')
                    """,
                    owner.kind,
                    UUID(owner.id),
                )
        except Exception:
            logger.warning(
                "Checking %s %s's bindings failed", owner.kind, owner.id, exc_info=True
            )
    return revoked


@dataclass
class BindTimeReport:
    revoked: list[str] = field(default_factory=list)
    marked: int = 0
    access_lost: int = 0
    orphaned: int = 0
    recovered: int = 0
    started: int = 0
    swept: int = 0

    def __bool__(self) -> bool:
        return bool(
            self.revoked
            or self.marked
            or self.access_lost
            or self.orphaned
            or self.recovered
            or self.started
            or self.swept
        )


async def _revoke_one(runtime: BindTimeRuntime, row: Mapping[str, Any]) -> str | None:
    """Run one binding's ``revoke`` from the binding alone (its image at its
    digest, the spec it bound with, the connector's config and credentials
    as they were at bind, its ``driver_state``): its connector, registration
    or execution may all be gone. ``None`` when it is retired, else why it
    waits for a later pass (a transient failure)."""
    store = runtime.store
    binding_id = str(row["id"])
    spec_json = _json(row["spec"])
    if not row["image_digest"] or not isinstance(spec_json, Mapping):
        # The bind never got as far as an image: it minted nothing.
        return await _retire(store, row, None)
    try:
        spec = spec_from_json(spec_json)
    except ValueError as exc:
        return await _retire(store, row, f"its spec is unreadable ({exc})")
    async with store.acquire() as conn:
        try:
            image = await ensure_image(
                conn,
                driver=str(row["driver"]),
                reference=str(row["image_reference"]),
                digest=str(row["image_digest"]),
            )
        except ServiceImageUnavailable as exc:
            return str(exc)
    inputs = _decrypt(row["inputs_ciphertext"]) or {}
    config = inputs.get("config") if isinstance(inputs.get("config"), dict) else {}
    credentials = (
        inputs.get("credentials") if isinstance(inputs.get("credentials"), dict) else {}
    )
    state = _decrypt(row["driver_state_ciphertext"])
    try:
        outcome = await runtime.operations.run(
            operation="revoke",
            driver=spec.name,
            image=image,
            request=DriverRequest(
                operation="revoke",
                config=config,
                access=row["access"],
                credentials=credentials,
                binding_id=binding_id,
                driver_state=state if isinstance(state, str) else None,
            ),
            spec=spec,
            config=config,
            connector_id=str(row["connector_id"]),
            registration_id=(
                str(row["registration_id"]) if row["registration_id"] else None
            ),
            binding_id=binding_id,
        )
    except BindTimeCapacity as exc:
        return str(exc)
    except BindTimeUnavailable as exc:
        return await _retire(store, row, str(exc))
    authored = isinstance(outcome.error, DriverAuthoredError)
    if outcome.error is not None and outcome.error.retryable:
        if authored:
            return _DriverText(driver_text(outcome.error.message))
        return outcome.error.message
    return await _retire(
        store,
        row,
        (
            None
            if outcome.error is None
            else driver_text(outcome.error.message)
            if authored
            else outcome.error.message
        ),
        source="driver" if authored else None,
    )


async def _retire(
    store: Any,
    row: Mapping[str, Any],
    error: str | None,
    *,
    source: str | None = None,
) -> None:
    """End a revoke: ``error`` is ``None`` when the driver revoked, else
    why SRW gave up. A revoke SRW gives up on is logged (WARNING) and
    audited as ``connector_driver_revoke_abandoned``, in ``security_events``,
    which outlives the connector and the binding: whatever the driver minted
    may still be live upstream."""
    binding_id = str(row["id"])
    async with store.acquire() as conn:
        retired = await conn.fetchval(
            """
            UPDATE connector_bind_time_bindings
               SET status = 'revoked', revoked_at = now(), revoke_error = $2,
                   revoke_error_source = $3,
                   delivery_ciphertext = NULL, driver_state_ciphertext = NULL,
                   inputs_ciphertext = NULL, revoke_next_at = NULL
             WHERE id = $1 AND status = 'revoking'
            RETURNING id
            """,
            UUID(binding_id),
            error[:500] if error else None,
            source if error else None,
        )
        if retired is not None and error is not None:
            logger.warning(
                "Gave up revoking binding %s of connector %s (registration %s): %s",
                binding_id,
                row.get("connector_id"),
                row.get("registration_id"),
                error,
            )
            await record_lease_event(
                conn,
                event_type="connector_driver_revoke_abandoned",
                resource_type="connector",
                resource_id=str(row.get("connector_id") or "") or None,
                detail=(
                    f"binding={binding_id} connector={row.get('connector_id')} "
                    f"registration={row.get('registration_id')} "
                    f"driver={row.get('driver')} digest={row.get('image_digest')} "
                    f"reason={error[:500]}"
                ),
            )
    return None


class _DriverText(str):
    """A revoke's reason that is the driver's own text."""


async def _revoke_later(store: Any, row: Mapping[str, Any], why: str) -> None:
    """A transient revoke failure: back off, and give up at the bound."""
    attempts = int(row["revoke_attempts"] or 0) + 1
    source = "driver" if isinstance(why, _DriverText) else None
    if attempts >= MAX_REVOKE_ATTEMPTS:
        await _retire(
            store, row, f"gave up after {attempts} attempts: {why}", source=source
        )
        return
    async with store.acquire() as conn:
        await conn.execute(
            """
            UPDATE connector_bind_time_bindings
               SET revoke_error = $2, revoke_attempts = $3,
                   revoke_next_at = now() + make_interval(secs => $4::float8),
                   revoke_error_source = $5
             WHERE id = $1 AND status = 'revoking'
            """,
            row["id"],
            why[:500],
            attempts,
            _backoff(attempts, REVOKE_RETRY_BASE_SECONDS, REVOKE_RETRY_MAX_SECONDS),
            source,
        )


async def _recover_unread(runtime: BindTimeRuntime) -> int:
    """Bind outcomes no runner read (it died): whatever the driver minted
    is moved to revoking with its ``driver_state``, never dropped."""
    store = runtime.store
    async with store.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, binding_id, operation, outcome_ciphertext
              FROM connector_driver_operations
             WHERE outcome_ciphertext IS NOT NULL
               AND finished_at < now() - make_interval(secs => $1::float8)
             ORDER BY finished_at LIMIT 20
            """,
            UNREAD_OUTCOME_SECONDS,
        )
    recovered = 0
    for row in rows:
        try:
            recovered += await _recover_one(store, row)
        except Exception:
            logger.warning(
                "Recovering driver operation %s failed", row["id"], exc_info=True
            )
    return recovered


async def _recover_one(store: Any, row: Mapping[str, Any]) -> int:
    """One unread outcome: a bind's result line moves its binding to
    revoking with the ``driver_state`` (a binding already revoking for
    another reason gets the state it lacks); the outcome is cleared."""
    recovered = 0
    posted = _decrypt(row["outcome_ciphertext"])
    lines = posted.get("lines") if isinstance(posted, dict) else None
    # A result line means the driver may have minted something, whatever SRW
    # would have made of its binding; an error line minted nothing.
    result = next(
        (
            line
            for line in lines or ()
            if isinstance(line, dict) and line.get("type") == "result"
        ),
        None,
    )
    if row["operation"] == "bind" and row["binding_id"] is not None and result:
        async with store.acquire() as conn:
            status = await conn.fetchval(
                "SELECT status FROM connector_bind_time_bindings WHERE id = $1",
                row["binding_id"],
            )
        state = result.get("driver_state")
        state = state if isinstance(state, str) else None
        if status in ("pending", "failed"):
            await _settle(
                store, str(row["binding_id"]), delivery=None, driver_state=state
            )
            recovered += 1
        elif status == "revoking" and state is not None:
            async with store.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE connector_bind_time_bindings
                       SET driver_state_ciphertext = $2
                     WHERE id = $1 AND status = 'revoking'
                       AND driver_state_ciphertext IS NULL
                    """,
                    row["binding_id"],
                    _encrypt(state),
                )
            recovered += 1
    async with store.acquire() as conn:
        await conn.execute(
            "UPDATE connector_driver_operations SET outcome_ciphertext = NULL "
            "WHERE id = $1",
            row["id"],
        )
    return recovered


async def reconcile_bind_time_once(
    runtime: BindTimeRuntime, pod_runtime: BindTimePodRuntime | None
) -> BindTimeReport:
    """One leader pass: mark (ended executions, deleted connectors), fail
    orphans, recover, revoke what an execution may no longer use, revoke,
    start, sweep, prune. Each row's work is its own: a failure is logged and
    that row backs off, the rest of the pass runs."""
    report = BindTimeReport()
    store = runtime.store
    settings = runtime.operations.settings
    async with store.acquire() as conn:
        for reason, where in (
            ("execution_ended", _OWNER_ENDED),
            ("connector_deleted", _CONNECTOR_GONE),
        ):
            report.marked += await _request_revoke(
                conn,
                f"id IN (SELECT b.id FROM connector_bind_time_bindings b "
                f"WHERE b.status IN ('bound', 'pending') AND {where})",
                [],
                reason=reason,
            )
    # Before the revokes are read, so this pass revokes them too.
    report.access_lost = await _access_lost(runtime)
    async with store.acquire() as conn:
        # No bind outlives its pod's deadline by this much: one still pending
        # was orphaned by a restart. Its retry is due at once, until
        # MAX_BIND_ATTEMPTS (then it failed for good, and a job waiting on
        # it fails); a revoke asked meanwhile revokes it.
        stale = settings.deadline_seconds + settings.wait_seconds + 60.0
        orphaned = await conn.fetch(
            """
            UPDATE connector_bind_time_bindings
               SET status = CASE WHEN revoke_requested_at IS NULL
                                 THEN 'failed' ELSE 'revoking' END,
                   failed_at = now(), error_class = 'transient',
                   error_source = NULL,
                   error_message = CASE
                       WHEN attempt >= $2
                       THEN 'The bind did not finish (gave up after '
                            || attempt || ' attempts)'
                       ELSE 'The bind did not finish; it runs again at the '
                            'next delivery' END,
                   retry_at = CASE WHEN attempt >= $2 THEN NULL ELSE now() END
             WHERE status = 'pending'
               AND created_at < now() - make_interval(secs => $1::float8)
            RETURNING id
            """,
            stale,
            MAX_BIND_ATTEMPTS,
        )
        report.orphaned = len(orphaned)
        revoking = await conn.fetch(
            """
            SELECT id, connector_id, registration_id, driver, image_reference,
                   image_digest, spec, access, inputs_ciphertext,
                   driver_state_ciphertext, revoke_attempts
              FROM connector_bind_time_bindings
             WHERE status = 'revoking'
               AND (revoke_next_at IS NULL OR revoke_next_at <= now())
             ORDER BY revoke_next_at NULLS FIRST, revoke_requested_at LIMIT $1
            """,
            REVOKES_PER_PASS,
        )
        starts = await conn.fetch(_STARTS, STARTS_PER_PASS)
    report.recovered = await _recover_unread(runtime)
    if pod_runtime is not None:
        for row in revoking:
            try:
                waiting = await _revoke_one(runtime, row)
            except Exception as exc:
                logger.warning(
                    "Revoking binding %s failed unexpectedly", row["id"], exc_info=True
                )
                waiting = f"SRW could not run the revoke ({type(exc).__name__})"
            try:
                if waiting is None:
                    report.revoked.append(str(row["id"]))
                else:
                    await _revoke_later(store, row, waiting)
            except Exception:
                logger.warning(
                    "Backing off binding %s failed", row["id"], exc_info=True
                )
        for owner, rows in _by_owner(starts).items():
            try:
                lost = set(
                    await _lost_connectors(
                        store, owner, [str(row["connector_id"]) for row in rows]
                    )
                )
            except Exception:
                logger.warning(
                    "Checking %s %s's connectors failed",
                    owner.kind,
                    owner.id,
                    exc_info=True,
                )
                continue
            for row in rows:
                if str(row["connector_id"]) in lost:
                    continue  # it may not use it: nothing to mint for it
                try:
                    await ensure_binding(
                        runtime,
                        owner,
                        str(row["connector_id"]),
                        read_only=bool(row["read_only"]),
                        wait=0,
                    )
                    report.started += 1
                except Exception:
                    logger.warning(
                        "Starting the bind of connector %s for %s %s failed",
                        row["connector_id"],
                        owner.kind,
                        owner.id,
                        exc_info=True,
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
             WHERE removed_at IS NOT NULL AND outcome_ciphertext IS NULL
               AND created_at < now() - make_interval(days => $1)
            """,
            RETENTION_DAYS,
        )
    return report


async def _sweep(store: Any, pod_runtime: BindTimePodRuntime) -> int:
    """Close operations past their deadline and delete every object no
    running operation names (a restart's leftovers).

    The objects are listed before the running operations are read: an
    operation's row is written before its objects exist, so an object
    created after the listing is never seen here, and one listed is seen
    with its running row."""
    try:
        objects = await pod_runtime.operation_objects()
    except Exception as exc:
        logger.warning("Listing bind-time driver objects failed: %s", exc)
        return 0
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
    present = {operation_id for _delete, _name, operation_id in objects}
    for delete, name, operation_id in objects:
        if operation_id in running:
            continue
        try:
            await pod_runtime.delete_object(delete, name)
            swept += 1
        except Exception as exc:
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
                        "bind-time drivers: marked=%d access_lost=%d revoked=%d "
                        "orphaned=%d recovered=%d started=%d swept=%d",
                        report.marked,
                        report.access_lost,
                        len(report.revoked),
                        report.orphaned,
                        report.recovered,
                        report.started,
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
    "MAX_BIND_ATTEMPTS",
    "MAX_RESULT_BYTES",
    "MAX_REVOKE_ATTEMPTS",
    "ACCESS_CHECKS_PER_PASS",
    "DRIVER_MESSAGE_WITHHELD",
    "MAX_DRIVER_MESSAGE",
    "BindTimeCapacity",
    "BindTimeError",
    "BindTimePending",
    "BindTimePodRuntime",
    "BindTimeRefused",
    "BindTimeRuntime",
    "BindTimeSettings",
    "BindTimeUnavailable",
    "CheckedImage",
    "DriverAuthoredError",
    "DriverOperations",
    "bind_time_reconciler",
    "bind_time_runtime",
    "check_image",
    "configure_bind_time",
    "connector_changed",
    "decide",
    "deliver_bind_time_entries",
    "driver_text",
    "ensure_binding",
    "job_bind_gate",
    "operation_identity",
    "parse_result_body",
    "prepare_bind_time_bindings",
    "prepare_thread_bindings",
    "reconcile_bind_time_once",
    "record_operation_result",
    "registered_entry",
    "registration_disabled",
    "registration_enabled",
    "revoke_owner_bindings",
    "run_check",
    "start_thread_bindings",
    "run_spec_operation",
]
