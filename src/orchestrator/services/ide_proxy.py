"""IDE Proxy Service — authoritative code-server reverse-proxy resolution.

The orchestrator proxies HTTP and WebSocket traffic from the browser to
code-server running inside workspace pods. Kubernetes coordinates are never
authority by themselves: Pod IPs can be reused immediately after deletion.
Every K8s request therefore reloads the owner projection and freshly attests
the exact Pod UID, labels, component, IP, and readiness in the control plane.
VM browser relay remains unavailable until it can use a guest-bound tunnel;
the launcher Pod is not a code-server endpoint. The short cache remains only
for the explicit single-host Docker development contract.
"""

import copy
from dataclasses import dataclass, field
import hashlib
import hmac
import ipaddress
import json
import logging
import time
from typing import Any, Literal, Optional
from uuid import UUID

from orchestrator.services.blocking_effect import joined_blocking_call
from orchestrator.services.ide_credentials import (
    IDE_CREDENTIAL_ENV,
    ide_credential,
    ide_credential_root,
)
from orchestrator.services.stateless_workspace_gate import (
    stateless_session_workspace_check,
)
from orchestrator.services.workspace_lifecycle import WorkspaceOwner
from shared.session_retirement import stateless_stop_markers

logger = logging.getLogger(__name__)

_K8S_READ_REQUEST_TIMEOUT = (5.0, 10.0)


class IdeProxyUnavailable(RuntimeError):
    """Typed refusal for an unsupported or unavailable IDE transport."""

    def __init__(self, code: str, detail: str) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail


# Both IDE transports in ``orchestrator/main.py`` — the buffered HTTP helper
# and the WebSocket proxy — carry the per-workspace code-server credential and
# refuse a remote runtime that has none. The advertisement must agree with
# them: an "Open IDE" button that 503s is worse than an honest absence, and on
# the job path ``start_session`` would otherwise pay for a snapshot restore
# nobody can open.
#
# Two questions, deliberately separate. Whether this *deployment* can bind a
# credential at all is a pure environment check (``browser_ide_refusal``, no
# I/O, safe to call anywhere). Whether *this workspace* enforces one can only
# be answered by attesting the runtime, so it lives in the async
# entity-scoped call below and is what the status endpoints actually use.
BROWSER_IDE_REFUSAL_CODE = "ide_credential_key_unconfigured"
BROWSER_IDE_REFUSAL_MESSAGE = (
    "The browser IDE is unavailable: this deployment has no IDE credential "
    "key, so no workspace can be bound to one. Use the Git view to browse the "
    "workspace."
)

BROWSER_IDE_UNBOUND_CODE = "ide_remote_transport_unavailable"
BROWSER_IDE_UNBOUND_MESSAGE = (
    "The browser IDE is unavailable for this workspace: it predates the "
    "per-workspace credential and will become available when the workspace is "
    "recreated. Use the Git view to browse it meanwhile."
)


def browser_ide_refusal() -> tuple[str, str] | None:
    """Return ``(code, message)`` when no workspace here could serve an IDE."""

    if ide_credential_root() is None:
        return BROWSER_IDE_REFUSAL_CODE, BROWSER_IDE_REFUSAL_MESSAGE
    return None


def contained_ide_status() -> dict[str, Any] | None:
    """Return the refusal payload while no backend can serve code-server.

    Every consumer resolves the refusal through this module, so a test (or a
    future gate) that lifts ``browser_ide_refusal`` lifts it everywhere at
    once instead of leaving one caller advertising what another refuses.
    """

    refusal = browser_ide_refusal()
    if refusal is None:
        return None
    return _contained_payload(*refusal)


def _contained_payload(code: str, message: str) -> dict[str, Any]:
    """The shape every withheld advertisement collapses to."""

    return {
        "status": "unavailable",
        "code_server_url": None,
        "code": code,
        "error": message,
    }


def contain_ide_status(status: dict[str, Any]) -> dict[str, Any]:
    """Downgrade a status payload that advertises an unreachable code-server.

    Fields other than the URL are preserved — ``gitea_url`` above all: the Git
    view reads the same workspace and is the honest fallback, so it must
    survive the downgrade.
    """

    contained = contained_ide_status()
    if contained is None or not status.get("code_server_url"):
        return status
    return {**status, **contained}


async def contain_ide_status_for(entity_id: str, status: dict[str, Any]) -> dict:
    """Advertise a ``code_server_url`` only if this workspace would serve it.

    Resolution costs one attestation, which is affordable here: the cockpit
    polls IDE status while a workspace provisions and stops once it reads
    active, and otherwise only asks when the user reaches for the button.
    Paying it buys an honest answer during the window where some Pods enforce
    a credential and older ones do not.
    """

    contained = contain_ide_status(status)
    if contained is not status or not status.get("code_server_url"):
        return contained
    try:
        target = await ide_proxy_service.resolve_target(entity_id)
    except IdeProxyUnavailable as exc:
        return {**status, **_contained_payload(exc.code, exc.detail)}
    except Exception:
        logger.debug("IDE advertisement resolution failed", exc_info=True)
        target = None
    if target is None or (target.credential is None and target.backend != "docker"):
        return {
            **status,
            **_contained_payload(BROWSER_IDE_UNBOUND_CODE, BROWSER_IDE_UNBOUND_MESSAGE),
        }
    return status


@dataclass(frozen=True, slots=True)
class IdeProxyTarget:
    """One exact, server-attested code-server network target.

    ``host`` is deliberately not authority.  ``identity`` contains the
    immutable control-plane/runtime tuple that was proven for this owner.  A
    caller must revalidate the complete value after opening its upstream TCP
    connection and before sending any browser-controlled bytes.
    """

    entity_id: str
    owner_kind: Literal["job", "thread"]
    backend: Literal["k8s", "vm", "docker"]
    scope: Literal["ide", "workspace_container", "vm"]
    host: str
    port: int
    identity: tuple[str, ...]
    # The credential this runtime's code-server will accept, when one can be
    # derived AND the Pod is observed to enforce it. It is the recipient
    # binding: a connection that reaches a foreign runtime is refused at that
    # runtime, so routing no longer has to be perfect for the boundary to
    # hold. ``repr=False`` keeps it out of log lines and tracebacks; it still
    # participates in equality, so a target whose credential changed fails
    # revalidation like any other drift.
    credential: str | None = field(default=None, repr=False)

    @property
    def authority(self) -> str:
        """Return an HTTP authority with correct IPv6 bracket handling."""

        try:
            parsed = ipaddress.ip_address(self.host)
        except ValueError:
            return f"{self.host}:{self.port}"
        rendered = f"[{parsed}]" if parsed.version == 6 else str(parsed)
        return f"{rendered}:{self.port}"


class IdeProxyService:
    """Resolves job/thread IDs to exact IDE reverse-proxy targets."""

    def __init__(self) -> None:
        self._db: Any = None
        self._container_provisioner: Any = None
        self._vm_provisioner: Any = None
        self._pod_ip_cache: dict[str, tuple[str, float, str]] = {}
        self._cache_ttl: float = 30.0  # seconds

    def connect(
        self,
        db: Any,
        container_provisioner: Any = None,
        vm_provisioner: Any = None,
    ) -> None:
        # Dependency rebuilds can switch database/provisioner authorities.
        # Even the explicit local-Docker compatibility cache must not survive
        # that boundary.
        self._pod_ip_cache.clear()
        self._db = db
        self._container_provisioner = container_provisioner
        self._vm_provisioner = vm_provisioner
        logger.info("IDE proxy service initialized")

    async def resolve_pod_ip(self, entity_id: str) -> Optional[str]:
        """Compatibility projection of :meth:`resolve_target`.

        Only exact local-Docker targets may be projected to a coordinate. New
        network callers must retain and revalidate the complete target; in
        particular, exporting a Kubernetes IP would recreate the U1/IP-reuse
        bug even though this service had just observed the correct Pod UID.
        """

        target = await self.resolve_target(entity_id)
        return (
            target.host if target is not None and target.backend == "docker" else None
        )

    async def resolve_target(self, entity_id: str) -> IdeProxyTarget | None:
        """Resolve and freshly attest an exact code-server runtime.

        Jobs are checked before threads, matching the authorization resolver.
        K8s coordinates are never cached and VM relay is contained. The Docker
        exception is an explicit single-host development trust contract and
        requires either an immutable container ID or the server-owned attested
        workspace lease.

        For each entity, checks (in order):
          1. ide_session.pod_ip  (restored IDE sessions)
          2. workspace_container.pod_ip  (live workspace containers)
          3. vm.ssh_host / vm.pod_ip  (live VMs)

        Returns:
            Exact target, or ``None`` when authority cannot be proven.
        """
        if not self._db:
            return None
        entity = await self._load_entity(entity_id)
        if entity is None:
            self.evict(entity_id)
            return None

        owner_kind, row, ctx = entity
        backend, scope, runtime, coordinate = self._classify_target(ctx)
        if backend == "fenced" or backend is None:
            self.evict(entity_id)
            return None
        if str(row.get("id") or "") != entity_id or not self._owner_is_live(
            owner_kind, row
        ):
            self.evict(entity_id)
            return None
        if not self._owner_runtime_is_admitted(owner_kind, row, ctx):
            self.evict(entity_id)
            return None
        if backend == "k8s":
            # A raw IP cache cannot distinguish deleted A from foreign B after
            # immediate address reuse. Never consult or populate it here.
            self.evict(entity_id)
            if scope is None or runtime is None:
                return None
            return await self._attest_k8s_target(
                entity_id,
                owner_kind=owner_kind,
                scope=scope,
                runtime=runtime,
            )

        if backend == "vm":
            self.evict(entity_id)
            provisioner = self._vm_provisioner
            if provisioner is None or not callable(
                getattr(provisioner, "attest_workspace_runtime", None)
            ):
                raise IdeProxyUnavailable(
                    "vm_ide_transport_unavailable", "VM IDE authority is unavailable",
                )
            try:
                proof = await provisioner.attest_workspace_runtime(
                    entity_id, entity_type=owner_kind,
                )
            except Exception as exc:
                raise IdeProxyUnavailable(
                    "vm_ide_runtime_unproven", "VM IDE runtime is not ready",
                ) from exc
            if not all(getattr(proof, name, None) for name in (
                "vm_uid", "vmi_uid", "launcher_pod_uid", "rootdisk_pvc_uid",
                "ssh_host_key_fingerprint", "host", "port",
            )):
                raise IdeProxyUnavailable(
                    "vm_ide_transport_unavailable", "VM IDE runtime is not ready",
                )
            return IdeProxyTarget(
                entity_id=entity_id, owner_kind=owner_kind, backend="vm",
                scope="vm", host=proof.host, port=proof.port,
                identity=(
                    proof.workspace_generation, proof.vm_uid, proof.vmi_uid,
                    proof.launcher_pod_uid, proof.rootdisk_pvc_uid,
                    proof.ssh_host_key_fingerprint,
                ),
            )

        if coordinate is None:
            self.evict(entity_id)
            return None
        docker_identity = self._docker_identity(scope=scope, runtime=runtime)
        if docker_identity is None:
            self.evict(entity_id)
            return None
        host, port = self._split_coordinate(coordinate)
        if host is None or port is None:
            self.evict(entity_id)
            return None
        cached = self._pod_ip_cache.get(entity_id)
        if (
            isinstance(cached, tuple)
            and len(cached) == 3
            and cached[0] == coordinate
            and cached[2] == backend
            and time.monotonic() < cached[1]
        ):
            pass
        else:
            self._pod_ip_cache[entity_id] = (
                coordinate,
                time.monotonic() + self._cache_ttl,
                backend,
            )
        return IdeProxyTarget(
            entity_id=entity_id,
            owner_kind=owner_kind,
            backend="docker",
            scope=scope,
            host=host,
            port=port,
            identity=(
                *docker_identity,
                *self._owner_lifecycle_projection(owner_kind, row),
                self._runtime_digest(runtime),
            ),
        )

    async def revalidate_target(self, target: IdeProxyTarget) -> bool:
        """Prove that ``target`` is still the exact current runtime."""

        try:
            current = await self.resolve_target(target.entity_id)
        except Exception:
            return False
        return current == target

    async def _load_entity(self, entity_id: str) -> Optional[tuple[str, dict, dict]]:
        """Load the job/thread row and its decoded runtime state."""

        job = await self._db.get_job(entity_id)
        if job:
            row, raw = job, job.get("context") or {}
            owner_kind = "job"
        else:
            thread = await self._db.get_thread(entity_id)
            if not thread:
                return None
            row, raw = thread, thread.get("metadata") or {}
            owner_kind = "thread"
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                return None
        if not isinstance(raw, dict):
            return None
        return owner_kind, row, raw

    async def _load_context(self, entity_id: str) -> Optional[dict]:
        """Compatibility helper for callers documenting job-first lookup."""

        entity = await self._load_entity(entity_id)
        return entity[2] if entity is not None else None

    @staticmethod
    def _classify_target(
        ctx: dict,
    ) -> tuple[str | None, str | None, dict | None, str | None]:
        """Classify a target without treating a coordinate as K8s authority."""

        ide_ctx = ctx.get("ide_session", {})
        if isinstance(ide_ctx, dict) and ide_ctx:
            restore_type = str(ide_ctx.get("restore_type") or "")
            ide_ip = ide_ctx.get("pod_ip")
            k8s_ide = (
                restore_type == "k8s_container"
                or ide_ctx.get("_runtime_incarnation") is not None
                or (
                    isinstance(ide_ip, str)
                    and bool(ide_ip)
                    and restore_type not in {"vm", "container"}
                )
            )
            if k8s_ide:
                if ide_ctx.get("status") not in {"active", "idle"}:
                    return "fenced", None, None, None
                return "k8s", "ide", ide_ctx, None
            if (
                ide_ctx.get("status") in {"active", "idle"}
                and isinstance(ide_ip, str)
                and ide_ip
            ):
                backend = "vm" if restore_type == "vm" else "docker"
                return backend, "ide", ide_ctx, ide_ip
            if (
                restore_type == "container"
                and ide_ctx.get("status") in {"active", "idle"}
                and ide_ctx.get("host_port") is not None
            ):
                return "docker", "ide", ide_ctx, f"127.0.0.1:{ide_ctx['host_port']}"

        ws_ctx = ctx.get("workspace_container", {})
        if isinstance(ws_ctx, dict) and ws_ctx:
            provisioner = str(ws_ctx.get("provisioner") or "")
            workspace_ip = ws_ctx.get("pod_ip")
            k8s_workspace = (
                provisioner == "k8s"
                or ws_ctx.get("_runtime_incarnation") is not None
                or (
                    isinstance(workspace_ip, str)
                    and bool(workspace_ip)
                    and provisioner != "docker"
                )
            )
            if k8s_workspace:
                if ws_ctx.get("status") != "ready":
                    return "fenced", None, None, None
                return "k8s", "workspace_container", ws_ctx, None
            if (
                ws_ctx.get("status") == "ready"
                and isinstance(workspace_ip, str)
                and workspace_ip
            ):
                return "docker", "workspace_container", ws_ctx, workspace_ip
            if ws_ctx.get("status") == "ready" and ws_ctx.get("ide_host"):
                ide_port = ws_ctx.get("ide_port", 8080)
                return (
                    "docker",
                    "workspace_container",
                    ws_ctx,
                    f"{ws_ctx['ide_host']}:{ide_port}",
                )

        vm_ctx = ctx.get("vm", {})
        if isinstance(vm_ctx, dict) and vm_ctx.get("status") == "ready":
            vm_host = vm_ctx.get("pod_ip") or vm_ctx.get("ssh_host")
            if isinstance(vm_host, str) and vm_host:
                return "vm", "vm", vm_ctx, vm_host

        return None, None, None, None

    @staticmethod
    def _owner_is_live(owner_kind: str, row: dict) -> bool:
        status = str(row.get("status") or "")
        if owner_kind == "job":
            return status in {
                "created",
                "pending",
                "processing",
                "pending_review",
                "paused",
                "reviewing",
                "waiting",
                "waiting_for_reply",
            }
        return status in {"created", "active", "idle", "awaiting_user"} or (
            status == "suspended" and row.get("execution_lane") == "pinned"
        )

    @staticmethod
    def _owner_lifecycle_projection(owner_kind: str, row: dict) -> tuple[str, ...]:
        """Fields whose change revokes an already-resolved browser target."""

        return tuple(
            str(row.get(field) or "")
            for field in (
                "id",
                "project_id",
                "user_id",
                "status",
                "execution_lane",
                "agent_id",
                "assigned_agent_id",
                "incarnation",
            )
        ) + (owner_kind,)

    @staticmethod
    def _runtime_digest(runtime: dict) -> str:
        """Opaque exact token for the server-owned runtime projection."""

        canonical = json.dumps(
            runtime,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
        return hashlib.sha256(canonical).hexdigest()

    @staticmethod
    def _owner_runtime_is_admitted(
        owner_kind: str,
        row: dict,
        ctx: dict,
    ) -> bool:
        if owner_kind != "thread" or str(row.get("execution_lane") or "") != (
            "stateless"
        ):
            return True
        try:
            stopped = bool(stateless_stop_markers(ctx))
            declared_backend, refusal = stateless_session_workspace_check(row)
        except RuntimeError:
            return False
        return not stopped and declared_backend == "sandbox" and refusal is None

    async def _attest_k8s_target(
        self,
        entity_id: str,
        *,
        owner_kind: str,
        scope: str,
        runtime: dict,
    ) -> IdeProxyTarget | None:
        """Freshly attest one exact K8s Pod before proxy network use."""

        initial_runtime = copy.deepcopy(runtime)
        entity = await self._load_entity(entity_id)
        if entity is None:
            return None
        initial_owner_kind, initial_row, initial_ctx = entity
        if (
            initial_owner_kind != owner_kind
            or not self._owner_is_live(owner_kind, initial_row)
            or not self._owner_runtime_is_admitted(owner_kind, initial_row, initial_ctx)
        ):
            return None
        initial_backend, initial_scope, current_runtime, _ = self._classify_target(
            initial_ctx
        )
        if (
            initial_backend != "k8s"
            or initial_scope != scope
            or current_runtime != initial_runtime
        ):
            return None
        lifecycle_projection = self._owner_lifecycle_projection(owner_kind, initial_row)

        provisioner = self._container_provisioner
        core_api = getattr(provisioner, "_core_api", None)
        namespace = getattr(provisioner, "_namespace", None)
        read_pod = getattr(core_api, "read_namespaced_pod", None)
        if (
            getattr(provisioner, "is_available", False) is not True
            or not isinstance(namespace, str)
            or not namespace
            or not callable(read_pod)
        ):
            return None

        try:
            expected_uid = str(UUID(str(runtime.get("_runtime_incarnation") or "")))
        except (TypeError, ValueError, AttributeError):
            return None
        expected_ip = runtime.get("pod_ip")
        if not isinstance(expected_ip, str) or not expected_ip:
            return None

        owner = (
            WorkspaceOwner.job(entity_id)
            if owner_kind == "job"
            else WorkspaceOwner.session(entity_id)
        )
        expected_name = f"ide-{entity_id[:12]}" if scope == "ide" else owner.pod_name
        expected_component = "ide-session" if scope == "ide" else owner.component_label
        stored_name = runtime.get("pod_name")
        stored_namespace = runtime.get("namespace")
        if stored_name not in {None, expected_name} or stored_namespace not in {
            None,
            namespace,
        }:
            return None
        try:
            pod = await joined_blocking_call(
                read_pod,
                name=expected_name,
                namespace=namespace,
                _request_timeout=_K8S_READ_REQUEST_TIMEOUT,
            )
            metadata = getattr(pod, "metadata", None)
            status = getattr(pod, "status", None)
            labels = getattr(metadata, "labels", None)
            observed_uid = str(UUID(str(getattr(metadata, "uid", "") or "")))
            observed_ip = str(getattr(status, "pod_ip", "") or "")
            container_statuses = getattr(status, "container_statuses", None)
        except Exception:
            return None

        opposite_owner_label = "srw/thread-id" if owner_kind == "job" else "srw/job-id"
        if (
            str(getattr(metadata, "name", "") or "") != expected_name
            or str(getattr(metadata, "namespace", "") or "") != namespace
            or observed_uid != expected_uid
            or getattr(metadata, "deletion_timestamp", None) is not None
            or not isinstance(labels, dict)
            or labels.get(owner.label_key) != entity_id
            or opposite_owner_label in labels
            or labels.get("app") != "srw-workspace"
            or labels.get("srw/component") != expected_component
            or labels.get("srw.io/component") != "agent-workspace"
            or getattr(status, "phase", None) != "Running"
            or observed_ip != expected_ip
            or not isinstance(container_statuses, (list, tuple))
            or not container_statuses
            or any(
                getattr(container, "ready", None) is not True
                for container in container_statuses
            )
        ):
            return None

        # Kubernetes observation awaited external authority. Re-read the
        # durable owner afterward and require the exact lifecycle and runtime
        # projection observed before that await. End, resume/replacement, tier
        # changes, and same-IP reuse therefore fail before a target leaves this
        # service.
        confirmed = await self._load_entity(entity_id)
        if confirmed is None:
            return None
        confirmed_owner_kind, confirmed_row, confirmed_ctx = confirmed
        confirmed_backend, confirmed_scope, confirmed_runtime, _ = (
            self._classify_target(confirmed_ctx)
        )
        if (
            confirmed_owner_kind != owner_kind
            or not self._owner_is_live(owner_kind, confirmed_row)
            or not self._owner_runtime_is_admitted(
                owner_kind, confirmed_row, confirmed_ctx
            )
            or self._owner_lifecycle_projection(owner_kind, confirmed_row)
            != lifecycle_projection
            or confirmed_backend != "k8s"
            or confirmed_scope != scope
            or confirmed_runtime != initial_runtime
        ):
            return None

        return IdeProxyTarget(
            entity_id=entity_id,
            owner_kind=owner_kind,
            backend="k8s",
            scope=scope,
            host=expected_ip,
            port=38080,
            identity=(
                namespace,
                expected_name,
                expected_uid,
                observed_ip,
                *lifecycle_projection,
                self._runtime_digest(initial_runtime),
            ),
            credential=self._enforced_credential(pod, owner, namespace, expected_name),
        )

    @staticmethod
    def _enforced_credential(
        pod: Any,
        owner: WorkspaceOwner,
        namespace: str,
        pod_name: str,
    ) -> str | None:
        """Return the credential this Pod *actually enforces*, else ``None``.

        Deriving a credential proves only what this orchestrator would send.
        A Pod created before the credential existed runs ``auth: none`` and
        would serve that request to anyone who reached it — so read the value
        back off the attested Pod spec and require an exact match. The
        transition therefore needs no flag day: an old Pod simply has no
        credential, stays contained, and becomes reachable when it is
        recreated on an image that enforces one.

        Derived from the shared ``WorkspaceOwner`` rather than from this
        service's own vocabulary: ``WorkspaceOwner.session()`` reports kind
        "session" while the proxy calls the same thing a "thread", and the two
        must not derive different values.
        """

        expected = ide_credential(
            namespace=namespace,
            owner_kind=owner.kind,
            owner_id=owner.id,
            pod_name=pod_name,
        )
        if not expected:
            return None
        spec = getattr(pod, "spec", None)
        containers = getattr(spec, "containers", None)
        if not isinstance(containers, (list, tuple)):
            return None
        for container in containers:
            for entry in getattr(container, "env", None) or ():
                if str(getattr(entry, "name", "") or "") != IDE_CREDENTIAL_ENV:
                    continue
                observed = getattr(entry, "value", None)
                if isinstance(observed, str) and hmac.compare_digest(
                    observed, expected
                ):
                    return expected
                # A container carrying a *different* value is not this owner's
                # runtime as this server understands it. Refuse rather than
                # send a credential it will reject anyway.
                return None
        return None

    @staticmethod
    def _docker_identity(
        *, scope: str | None, runtime: dict | None
    ) -> tuple[str, ...] | None:
        """Return the explicit local-development Docker trust identity."""

        if runtime is None:
            return None
        if scope == "ide":
            container_id = runtime.get("container_id")
            if (
                isinstance(container_id, str)
                and len(container_id) == 64
                and all(character in "0123456789abcdef" for character in container_id)
            ):
                return ("local-container", container_id)
            return None
        if scope == "workspace_container":
            lease_id = runtime.get("_docker_workspace_lease_id")
            generation = runtime.get("_canvas_workspace_generation")
            if (
                runtime.get("_docker_workspace_attested") is True
                and runtime.get("_docker_workspace_trust_mode") == "attested"
                and isinstance(lease_id, str)
                and lease_id
            ):
                return ("docker-workspace-lease", lease_id, str(generation or ""))
        return None

    @staticmethod
    def _split_coordinate(coordinate: str) -> tuple[str | None, int | None]:
        """Parse a trusted Docker development coordinate."""

        raw = coordinate.strip()
        host = raw
        port = 38080
        if raw.startswith("[") and "]:" in raw:
            host, raw_port = raw[1:].rsplit("]:", 1)
        elif raw.count(":") == 1:
            host, raw_port = raw.rsplit(":", 1)
        else:
            raw_port = "38080"
        try:
            port = int(raw_port)
        except (TypeError, ValueError):
            return None, None
        if not host or not 1 <= port <= 65535:
            return None, None
        return host, port

    def evict(self, entity_id: str) -> None:
        """Remove a job or thread from the cache (call on upstream connection failure)."""
        self._pod_ip_cache.pop(entity_id, None)


# Module-level singleton
ide_proxy_service = IdeProxyService()
