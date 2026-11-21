"""Pinned, generation-bound SSH transport for Dynamic Canvas.

Canvas file materialization and live-app direct channels share one authenticated
SSH transport pool.  The pool never creates a local TCP listener: callers open
request-scoped ``direct-tcpip`` channels to the fixed workspace loopback host.

Two kinds of workspace resolve here. A container is paired with a
provisioner-minted ``_workspace_binding``. A same-cluster VM has no Canvas
binding; its identity is the controller-authenticated tuple in
``metadata.vm`` (the same fields the IDE transport and the upload lease
prove), and every VM operation re-proves it with the VM provisioner's signed
``attest_workspace_runtime`` before and after it touches the guest.
"""

from __future__ import annotations

import asyncio
import json
import logging
import secrets
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Literal
from uuid import UUID, uuid5

try:
    import asyncssh
except ImportError:  # pragma: no cover - deployment dependency guard
    asyncssh = None  # type: ignore[assignment]

from orchestrator.services.workspace_binding import CANVAS_WORKSPACE_GENERATION_KEY
from shared.workspace_contract import vm_mode_from_env
from shared.workspace_recovery import WorkspaceRecoveryCode

logger = logging.getLogger(__name__)

CANVAS_LOOPBACK_HOST = "127.0.0.1"
DEFAULT_SSH_IDLE_TIMEOUT_SECONDS = 60.0
DEFAULT_SSH_CLOSE_TIMEOUT_SECONDS = 5.0
# A truthy, explicitly empty KnownHostsResult. Falsy values can make AsyncSSH
# consult the process user's default known_hosts file, which is not Canvas
# authority for a workspace generation.
EMPTY_KNOWN_HOSTS = ((), (), (), (), (), (), ())
# One controller round trip; an attestation that takes longer than this is an
# inconclusive answer, never a positive one.
VM_ATTESTATION_TIMEOUT_SECONDS = 15.0
# Fixed namespace for the VM Canvas generation (see vm_canvas_generation).
VM_CANVAS_GENERATION_NAMESPACE = UUID("5a0f4c3e-9d1b-5e7a-8c42-6b1d0e9f3a27")

GenerationResolver = Callable[[], Awaitable[dict[str, Any]]]
VMAttestationVerdict = Literal["current", "changed", "unknown"]


async def _bounded_wait_closed(resource: Any) -> None:
    wait_closed = getattr(resource, "wait_closed", None)
    if not callable(wait_closed):
        return
    try:
        async with asyncio.timeout(DEFAULT_SSH_CLOSE_TIMEOUT_SECONDS):
            await wait_closed()
    except TimeoutError:
        logger.warning("Canvas SSH resource close timed out")
    except Exception:
        logger.debug("Canvas SSH resource close failed", exc_info=True)


class CanvasSSHError(Exception):
    """Typed workspace-identity/transport failure for Canvas adapters."""

    def __init__(self, status_code: int, code: str, message: str):
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message


class CanvasDirectChannelUnavailable(Exception):
    """The pinned SSH transport is healthy but the loopback port is closed."""


@dataclass(frozen=True, slots=True)
class PinnedSSHCommandResult:
    """Bounded output from one command on an authenticated SSH transport."""

    exit_status: int
    stdout: bytes
    stderr: bytes


class _CommandOutputTooLarge(Exception):
    """Internal sentinel used to stop a noisy remote process."""


def workspace_metadata(thread: dict[str, Any]) -> dict[str, Any]:
    """Return one thread's parsed metadata without mutating the source row."""

    value = thread.get("metadata") or {}
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            value = {}
    return value if isinstance(value, dict) else {}


def bound_workspace_generation(thread: dict[str, Any]) -> UUID:
    """Resolve the authoritative Canvas workspace generation."""

    binding = workspace_metadata(thread).get("_workspace_binding") or {}
    try:
        return UUID(str(binding.get("generation")))
    except (TypeError, ValueError) as exc:
        raise CanvasSSHError(
            409,
            "workspace_unavailable",
            "The thread has no bound workspace generation",
        ) from exc


@dataclass(frozen=True, slots=True)
class RemoteWorkspaceTarget:
    """One provisioner-attested SSH endpoint for a workspace generation.

    ``vm_runtime`` is set only for a VM: the exact controller-authenticated
    runtime the endpoint belongs to, as ``(provision_generation, vm_uid,
    launcher_pod_uid, rootdisk_pvc_uid)``. It takes part in equality, so a
    launcher replacement behind the same address and key is a different
    target, but not in the pool key: the SSH transport is pinned by address
    and host key alone.
    """

    thread_id: str
    generation: UUID
    host: str
    port: int
    fingerprint: str
    vm_runtime: tuple[str, str, str, str] | None = None

    @property
    def pool_key(self) -> tuple[str, str, str, int, str]:
        return (
            self.thread_id,
            str(self.generation),
            self.host,
            self.port,
            self.fingerprint,
        )


def remote_target_is_vm_backed(thread: dict[str, Any]) -> bool:
    """Whether :func:`resolve_remote_workspace_target` resolves the VM endpoint.

    The endpoint-selection rule below prefers ``metadata.vm`` whenever its
    status is ``ready``, *regardless* of ``workspace_container``. That is one
    definition, exported so a caller which must refuse a VM-backed target can
    ask the resolver's own question instead of re-deriving it from tier
    signals that disagree.

    They do disagree, and it is not hypothetical.
    ``workspace_suspension._thread_is_vm_tier`` reads the declared backend
    (``config_override.workspace.backend``) whenever a container status is
    also present, and upgrade-to-VM provisions ``metadata.vm`` WITHOUT
    rewriting that backend — so a thread with both contexts ``ready`` and a
    stale ``sandbox``/``virtual`` backend reads as pod-tier there while this
    resolver hands back the VM's host and port.

    Takes the thread, not pre-parsed metadata, so it parses the row exactly
    the way the resolver does: a caller passing its own separately-parsed
    metadata is how the two would drift apart again.
    """
    vm_context = workspace_metadata(thread).get("vm") or {}
    return isinstance(vm_context, dict) and vm_context.get("status") == "ready"


def resolve_remote_workspace_target(
    thread: dict[str, Any], expected_generation: UUID
) -> RemoteWorkspaceTarget:
    """Resolve an active endpoint paired to an attested remote binding."""

    metadata = workspace_metadata(thread)
    binding = metadata.get("_workspace_binding") or {}
    if not isinstance(binding, dict) or binding.get("kind") != "remote":
        raise CanvasSSHError(
            409,
            "workspace_unavailable",
            "The workspace has no provisioner-attested remote binding",
        )
    if bound_workspace_generation(thread) != expected_generation:
        raise CanvasSSHError(
            409,
            "workspace_generation_changed",
            "The selected workspace generation is no longer current",
        )
    backing_id = binding.get("backing_id")
    if not isinstance(backing_id, str) or not backing_id:
        raise CanvasSSHError(
            409,
            "workspace_unavailable",
            "The workspace binding has no backing identity",
        )
    fingerprint = binding.get("ssh_host_key_fingerprint")
    if (
        not isinstance(fingerprint, str)
        or not fingerprint.startswith("SHA256:")
        or len(fingerprint) > 128
        or any(char.isspace() for char in fingerprint)
    ):
        raise CanvasSSHError(
            409,
            "workspace_unavailable",
            "The workspace has no provisioner-pinned SSH identity",
        )

    # One definition of "the VM wins", shared with callers that must refuse a
    # VM-backed target — see remote_target_is_vm_backed above.
    vm_context = metadata.get("vm") or {}
    workspace_context = metadata.get("workspace_container") or {}
    context = vm_context if remote_target_is_vm_backed(thread) else workspace_context
    if not isinstance(context, dict) or context.get("status") != "ready":
        raise CanvasSSHError(409, "workspace_unavailable", "Workspace is not active")
    if context.get(CANVAS_WORKSPACE_GENERATION_KEY) != str(expected_generation):
        raise CanvasSSHError(
            409,
            "workspace_generation_changed",
            "The active workspace endpoint does not match the selected generation",
        )
    host = context.get("ssh_host") or context.get("host") or context.get("pod_ip")
    if (
        not isinstance(host, str)
        or not host
        or any(ord(char) < 33 or ord(char) == 127 for char in host)
    ):
        raise CanvasSSHError(409, "workspace_unavailable", "Workspace is not active")
    try:
        port = int(context.get("ssh_port") or context.get("port") or 22)
    except (TypeError, ValueError) as exc:
        raise CanvasSSHError(
            409, "workspace_unavailable", "Workspace endpoint port is invalid"
        ) from exc
    if not 1 <= port <= 65535:
        raise CanvasSSHError(
            409, "workspace_unavailable", "Workspace endpoint port is invalid"
        )
    thread_id = str(thread.get("id") or "")
    if not thread_id:
        raise CanvasSSHError(404, "workspace_unavailable", "Thread is unavailable")
    return RemoteWorkspaceTarget(
        thread_id=thread_id,
        generation=expected_generation,
        host=host,
        port=port,
        fingerprint=fingerprint,
    )


def require_same_remote_workspace(
    thread: dict[str, Any], expected_target: RemoteWorkspaceTarget
) -> None:
    """Fail when binding, endpoint, generation, or pinned identity changed.

    A VM target is re-resolved from the VM's own identity, a container target
    from its binding, so a VM that takes over from a container (or the
    reverse) can never satisfy the other kind's check.
    """

    if expected_target.vm_runtime is not None:
        current = resolve_vm_workspace_target(thread, expected_target.generation)
    else:
        current = resolve_remote_workspace_target(thread, expected_target.generation)
    if current != expected_target:
        raise CanvasSSHError(
            409,
            "workspace_generation_changed",
            "The active workspace endpoint changed during the Canvas operation",
        )


# ---------------------------------------------------------------------------
# Same-cluster VM workspaces
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class VMCanvasIdentity:
    """The controller-authenticated VM tuple stored in ``metadata.vm``."""

    provision_generation: str
    vm_uid: str
    launcher_pod_uid: str
    rootdisk_pvc_uid: str
    host: str
    port: int
    fingerprint: str


def _canonical_uuid(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = UUID(value)
    except (TypeError, ValueError, AttributeError):
        return None
    canonical = str(parsed)
    return canonical if canonical == value.lower() else None


def _plain_token(value: Any, *, limit: int) -> str | None:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > limit
        or any(char.isspace() or ord(char) < 33 or ord(char) == 127 for char in value)
    ):
        return None
    return value


def _pinned_fingerprint(value: Any) -> str | None:
    if (
        not isinstance(value, str)
        or not value.startswith("SHA256:")
        or len(value) > 128
        or any(char.isspace() for char in value)
    ):
        return None
    return value


def _same_uid(left: Any, right: Any) -> bool:
    """Compare two Kubernetes UIDs, tolerating only UUID letter case."""

    if not isinstance(left, str) or not isinstance(right, str) or not left:
        return False
    canonical_left = _canonical_uuid(left)
    canonical_right = _canonical_uuid(right)
    if canonical_left is not None and canonical_right is not None:
        return canonical_left == canonical_right
    return left == right


def vm_canvas_identity(vm_context: Any) -> VMCanvasIdentity | None:
    """Return a ready VM's complete authenticated identity, or ``None``.

    The fields and their checks are the ones the VM remote-operation lease
    and the IDE transport require (``identity_authenticated`` for this exact
    provision generation, provisioner-probed readiness, VM UID, launcher Pod
    UID, rootdisk PVC UID, SSH endpoint and admitted host-key pin). A VM whose
    suspension has closed
    remote I/O has none. This is a durable-row read only; callers that touch
    the guest must also hold a fresh controller proof
    (:func:`attest_vm_workspace_target`).
    """

    if not isinstance(vm_context, dict) or vm_context.get("status") != "ready":
        return None
    if vm_context.get("_suspend_remote_io_closed"):
        return None
    if vm_context.get("identity_authenticated") is not True:
        return None
    # On the same cluster the workspace contract treats a VM as an effective
    # runtime only when the provisioner's own probe proved it ready
    # (shared.workspace_contract._runtime_is_authoritative); a VM ready by a
    # daemon registration alone is "vm_runtime_unattested" there, and so here.
    if vm_context.get("ssh_ready_source") != "provisioner_probe":
        return None
    generation = _canonical_uuid(vm_context.get("provision_generation"))
    if (
        generation is None
        or _canonical_uuid(vm_context.get("identity_provision_generation"))
        != generation
    ):
        return None
    launcher_pod_uid = _canonical_uuid(vm_context.get("active_pod_uid"))
    vm_uid = _plain_token(vm_context.get("vm_uid"), limit=256)
    rootdisk_pvc_uid = _plain_token(vm_context.get("rootdisk_pvc_uid"), limit=256)
    host = _plain_token(vm_context.get("ssh_host"), limit=512)
    fingerprint = _pinned_fingerprint(vm_context.get("ssh_host_key_fingerprint"))
    raw_port = vm_context.get("ssh_port")
    if (
        isinstance(raw_port, bool)
        or not isinstance(raw_port, (int, str))
        or not str(raw_port).isdigit()
        or not 1 <= int(raw_port) <= 65535
    ):
        return None
    if (
        launcher_pod_uid is None
        or vm_uid is None
        or rootdisk_pvc_uid is None
        or host is None
        or fingerprint is None
    ):
        return None
    return VMCanvasIdentity(
        provision_generation=generation,
        vm_uid=vm_uid,
        launcher_pod_uid=launcher_pod_uid,
        rootdisk_pvc_uid=rootdisk_pvc_uid,
        host=host,
        port=int(raw_port),
        fingerprint=fingerprint,
    )


def vm_canvas_generation(thread_id: str, rootdisk_pvc_uid: str) -> UUID:
    """The Canvas workspace generation of one VM disk.

    A container's Canvas generation is its binding's, which stays the same
    while the pod comes back on the same volume with the same host key. A VM
    gets a new provision generation, launcher Pod and host key every time it
    is created, including a restore onto a kept rootdisk, so none of those can
    be the generation without making every presentation stale after a
    suspend. The rootdisk PVC is what carries the files across, so the
    generation is derived from it: the same disk is the same file namespace,
    a new disk is a new generation. Transport authority never rests on this
    value; it is pinned to the live endpoint's host key and re-proved by the
    controller on every operation.
    """

    return uuid5(VM_CANVAS_GENERATION_NAMESPACE, f"{thread_id}\x00{rootdisk_pvc_uid}")


def _thread_vm_identity(thread: dict[str, Any]) -> tuple[str, VMCanvasIdentity]:
    identity = vm_canvas_identity(workspace_metadata(thread).get("vm"))
    if identity is None:
        raise CanvasSSHError(
            409,
            "workspace_unavailable",
            "The VM workspace has no controller-authenticated identity",
        )
    thread_id = str(thread.get("id") or "")
    if not thread_id:
        raise CanvasSSHError(404, "workspace_unavailable", "Thread is unavailable")
    return thread_id, identity


def canvas_workspace_generation(thread: dict[str, Any]) -> UUID:
    """The Canvas generation of the workspace the Canvas resolver selects.

    A ready VM wins, exactly as in :func:`resolve_remote_workspace_target`;
    otherwise this is the bound container/virtual generation, unchanged.
    """

    if remote_target_is_vm_backed(thread):
        thread_id, identity = _thread_vm_identity(thread)
        return vm_canvas_generation(thread_id, identity.rootdisk_pvc_uid)
    return bound_workspace_generation(thread)


def resolve_vm_workspace_target(
    thread: dict[str, Any], expected_generation: UUID
) -> RemoteWorkspaceTarget:
    """Resolve the ready VM's endpoint from its authenticated identity."""

    thread_id, identity = _thread_vm_identity(thread)
    generation = vm_canvas_generation(thread_id, identity.rootdisk_pvc_uid)
    if generation != expected_generation:
        raise CanvasSSHError(
            409,
            "workspace_generation_changed",
            "The selected workspace generation is no longer current",
        )
    return RemoteWorkspaceTarget(
        thread_id=thread_id,
        generation=generation,
        host=identity.host,
        port=identity.port,
        fingerprint=identity.fingerprint,
        vm_runtime=(
            identity.provision_generation,
            identity.vm_uid,
            identity.launcher_pod_uid,
            identity.rootdisk_pvc_uid,
        ),
    )


def resolve_canvas_workspace_target(
    thread: dict[str, Any], expected_generation: UUID
) -> RemoteWorkspaceTarget:
    """Canvas's resolver: a ready VM by its identity, else the bound container.

    :func:`resolve_remote_workspace_target` is deliberately left as it is:
    SSH access and first-use callers rely on it never resolving a VM.
    """

    if remote_target_is_vm_backed(thread):
        return resolve_vm_workspace_target(thread, expected_generation)
    return resolve_remote_workspace_target(thread, expected_generation)


def vm_canvas_route_available(
    host: str, *, can_reach: Callable[[str], bool] | None = None
) -> bool:
    """Whether the orchestrator has a route to a VM guest: same-cluster only.

    A VM on the tailnet (``VM_MODE=external``) is never reachable from the
    orchestrator pod, so Canvas answers it immediately instead of letting an
    SSH connect time out. ``can_reach`` lets a caller pass its own imported
    ``orchestrator_can_reach`` (and the tests that patch it).
    """

    if vm_mode_from_env() != "same-cluster":
        # DEFERRED, not forgotten: Canvas and the shared browser for a VM
        # reached over the tailnet need an orchestrator route to the guest
        # (a tailnet sidecar or a relay through the VM daemon). Until that
        # exists the answer is workspace_unroutable, at once.
        return False
    if can_reach is None:
        from orchestrator.services import ssh_helpers

        can_reach = ssh_helpers.orchestrator_can_reach
    try:
        return bool(can_reach(host))
    except Exception:
        return False


def vm_runtime_proof_matches(target: RemoteWorkspaceTarget, proof: Any) -> bool:
    """Whether a controller attestation proves exactly ``target``'s runtime."""

    runtime = target.vm_runtime
    if runtime is None or proof is None:
        return False
    generation, vm_uid, launcher_pod_uid, rootdisk_pvc_uid = runtime
    try:
        return bool(
            _canonical_uuid(str(proof.workspace_generation)) == generation
            and _same_uid(proof.vm_uid, vm_uid)
            and _same_uid(proof.launcher_pod_uid, launcher_pod_uid)
            and _same_uid(proof.runtime_incarnation, launcher_pod_uid)
            and _same_uid(proof.rootdisk_pvc_uid, rootdisk_pvc_uid)
            and proof.host == target.host
            and int(proof.port) == target.port
            and proof.ssh_host_key_fingerprint == target.fingerprint
            and vm_canvas_generation(target.thread_id, rootdisk_pvc_uid)
            == target.generation
        )
    except (AttributeError, TypeError, ValueError):
        return False


_DEFINITIVE_RECOVERY_CODES = frozenset(
    {
        WorkspaceRecoveryCode.RUNTIME_NOT_READY.value,
        WorkspaceRecoveryCode.REPLACEMENT_OBSERVED.value,
    }
)


def _current_vm_provisioner() -> Any:
    """Read the VM provisioner singleton from its owner module at call time."""

    from orchestrator.services import vm_provisioner as vm_provisioner_module

    return vm_provisioner_module.vm_provisioner


async def attest_vm_workspace_target(
    target: RemoteWorkspaceTarget,
) -> VMAttestationVerdict:
    """Freshly prove that a VM target is still the exact live runtime.

    The proof is the VM provisioner's ``attest_workspace_runtime``: a signed
    controller read of the VM, VMI and launcher Pod, paired with a re-read of
    the durable row. It is the same proof the IDE transport and the upload
    lease rely on. ``changed`` means the controller positively saw another
    runtime (or none); ``unknown`` means no conclusive answer, which a caller
    must never treat as ``current``.
    """

    if target.vm_runtime is None:
        raise ValueError("only a VM target has a controller attestation")
    attest = getattr(_current_vm_provisioner(), "attest_workspace_runtime", None)
    if not callable(attest):
        return "unknown"
    try:
        async with asyncio.timeout(VM_ATTESTATION_TIMEOUT_SECONDS):
            proof = await attest(target.thread_id, entity_type="thread")
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        code = getattr(exc, "recovery_code", None)
        if str(getattr(code, "value", code)) in _DEFINITIVE_RECOVERY_CODES:
            return "changed"
        logger.debug(
            "Canvas VM attestation was inconclusive for thread %s",
            target.thread_id,
            exc_info=True,
        )
        return "unknown"
    if getattr(proof, "rootdisk_pvc_uid", None) is None:
        # The controller did not observe the disk this time. That proves
        # nothing either way (the IDE transport calls it "unproven").
        return "unknown"
    return "current" if vm_runtime_proof_matches(target, proof) else "changed"


if asyncssh is not None:

    class PinnedSSHClient(asyncssh.SSHClient):
        """AsyncSSH verifier pinned to provisioner-attested key material."""

        def __init__(self, expected_fingerprint: str):
            self._expected_fingerprint = expected_fingerprint

        def validate_host_public_key(self, host, addr, port, key):  # noqa: ANN001
            del host, addr, port
            actual = key.get_fingerprint("sha256")
            return secrets.compare_digest(actual, self._expected_fingerprint)

else:

    class PinnedSSHClient:  # pragma: no cover - import-only fallback
        def __init__(self, expected_fingerprint: str):
            self._expected_fingerprint = expected_fingerprint


@dataclass(slots=True)
class _PooledSSHTransport:
    target: RemoteWorkspaceTarget
    connection: Any
    last_used: float
    leases: int = 0
    broken: bool = False
    closed: bool = False
    idle_handle: asyncio.TimerHandle | None = None


@dataclass(slots=True)
class _EntrySerial:
    lock: asyncio.Lock
    users: int = 0


class PinnedSSHTransportPool:
    """Concurrent transport pool keyed by attested workspace generation."""

    def __init__(
        self,
        *,
        max_entries: int = 32,
        idle_timeout: float = DEFAULT_SSH_IDLE_TIMEOUT_SECONDS,
    ) -> None:
        self._max_entries = max(1, max_entries)
        self._idle_timeout = max(0.0, idle_timeout)
        self._entries: dict[tuple[str, str, str, int, str], _PooledSSHTransport] = {}
        self._entry_serials: dict[tuple[str, str, str, int, str], _EntrySerial] = {}
        self._lock = asyncio.Lock()

    @staticmethod
    def _is_connection_closed(connection: Any) -> bool:
        checker = getattr(connection, "is_closed", None)
        return bool(checker()) if callable(checker) else False

    async def _close(self, entry: _PooledSSHTransport) -> None:
        if entry.closed:
            return
        entry.closed = True
        if entry.idle_handle is not None:
            entry.idle_handle.cancel()
            entry.idle_handle = None
        entry.connection.close()
        await _bounded_wait_closed(entry.connection)

    def _cancel_idle(self, entry: _PooledSSHTransport) -> None:
        if entry.idle_handle is not None:
            entry.idle_handle.cancel()
            entry.idle_handle = None

    def _schedule_idle(
        self,
        key: tuple[str, str, str, int, str],
        entry: _PooledSSHTransport,
    ) -> None:
        self._cancel_idle(entry)
        loop = asyncio.get_running_loop()
        entry.idle_handle = loop.call_later(
            self._idle_timeout,
            lambda: asyncio.create_task(self._expire_idle(key, entry)),
        )

    async def _expire_idle(
        self,
        key: tuple[str, str, str, int, str],
        entry: _PooledSSHTransport,
    ) -> None:
        close = False
        async with self._lock:
            if self._entries.get(key) is not entry or entry.leases != 0:
                return
            elapsed = asyncio.get_running_loop().time() - entry.last_used
            if elapsed < self._idle_timeout:
                self._schedule_idle(key, entry)
                return
            self._entries.pop(key, None)
            entry.idle_handle = None
            close = True
        if close:
            await self._close(entry)

    async def _connect(
        self, target: RemoteWorkspaceTarget, key_path: str
    ) -> _PooledSSHTransport:
        if asyncssh is None:
            raise CanvasSSHError(
                503, "workspace_unavailable", "Pinned SSH transport is unavailable"
            )
        connection = None
        try:
            async with asyncio.timeout(20):
                connection = await asyncssh.connect(
                    target.host,
                    port=target.port,
                    username="agent-host",
                    client_keys=[key_path],
                    known_hosts=EMPTY_KNOWN_HOSTS,
                    client_factory=lambda: PinnedSSHClient(target.fingerprint),
                    server_host_key_algs=["ssh-ed25519"],
                    connect_timeout=10,
                    login_timeout=15,
                )
        except asyncio.CancelledError:
            if connection is not None:
                connection.close()
                await _bounded_wait_closed(connection)
            raise
        except Exception as exc:
            if connection is not None:
                connection.close()
                await _bounded_wait_closed(connection)
            raise CanvasSSHError(
                503,
                "workspace_unavailable",
                "Pinned SSH connection could not be established",
            ) from exc
        return _PooledSSHTransport(
            target=target,
            connection=connection,
            leases=1,
            last_used=asyncio.get_running_loop().time(),
        )

    @asynccontextmanager
    async def _serialize_entry(self, key: tuple[str, str, str, int, str]):
        """Single-flight connection setup for one attested workspace target."""

        async with self._lock:
            serial = self._entry_serials.get(key)
            if serial is None:
                serial = _EntrySerial(lock=asyncio.Lock())
                self._entry_serials[key] = serial
            serial.users += 1
        try:
            async with serial.lock:
                yield
        finally:
            async with self._lock:
                serial.users = max(0, serial.users - 1)
                if serial.users == 0 and not serial.lock.locked():
                    self._entry_serials.pop(key, None)

    async def _entry(
        self, target: RemoteWorkspaceTarget, key_path: str
    ) -> _PooledSSHTransport:
        key = target.pool_key
        async with self._serialize_entry(key):
            stale: list[_PooledSSHTransport] = []
            async with self._lock:
                incumbent = self._entries.get(key)
                if (
                    incumbent is not None
                    and not incumbent.broken
                    and not incumbent.closed
                    and not self._is_connection_closed(incumbent.connection)
                ):
                    self._cancel_idle(incumbent)
                    incumbent.leases += 1
                    incumbent.last_used = asyncio.get_running_loop().time()
                    return incumbent
                if incumbent is not None:
                    self._entries.pop(key, None)
                    # A retired entry with active channels owns itself until
                    # its last lease exits. Closing it here would terminate an
                    # unrelated concurrent SFTP or app operation.
                    if incumbent.leases == 0:
                        stale.append(incumbent)

            # Close a dead incumbent before attempting a replacement. This
            # also prevents a failed reconnect from orphaning the old entry.
            for old in stale:
                await self._close(old)
            stale.clear()

            candidate: _PooledSSHTransport | None = None
            try:
                candidate = await self._connect(target, key_path)
                async with self._lock:
                    # Setup for this exact target is serialized, so no other
                    # checkout can have installed an incumbent while connect
                    # was in flight.
                    self._entries[key] = candidate

                    while len(self._entries) > self._max_entries:
                        idle = [
                            item
                            for item in self._entries.items()
                            if item[1].leases == 0 and item[1] is not candidate
                        ]
                        if not idle:
                            break
                        oldest_key, oldest = min(
                            idle, key=lambda item: item[1].last_used
                        )
                        self._entries.pop(oldest_key, None)
                        stale.append(oldest)

                for old in stale:
                    await self._close(old)
                result = candidate
                candidate = None
                return result
            finally:
                # Cancellation after a successful handshake but before pool
                # insertion must not leak the candidate transport.
                if candidate is not None:
                    async with self._lock:
                        if self._entries.get(key) is candidate:
                            self._entries.pop(key, None)
                    await self._close(candidate)

    async def _release(self, entry: _PooledSSHTransport) -> None:
        close = False
        key = entry.target.pool_key
        async with self._lock:
            entry.leases = max(0, entry.leases - 1)
            entry.last_used = asyncio.get_running_loop().time()
            entry.broken = entry.broken or self._is_connection_closed(entry.connection)
            is_current = self._entries.get(key) is entry
            if entry.broken and is_current:
                self._entries.pop(key, None)
                is_current = False
            if entry.leases == 0 and not is_current:
                close = True
            elif entry.leases == 0 and is_current:
                self._schedule_idle(key, entry)
        if close:
            await self._close(entry)

    @asynccontextmanager
    async def checkout(self, *, target: RemoteWorkspaceTarget, key_path: str):
        """Lease one authenticated transport; concurrent channels are allowed."""

        entry = await self._entry(target, key_path)
        try:
            yield entry.connection
        finally:
            await self._release(entry)

    async def invalidate(self, target: RemoteWorkspaceTarget) -> None:
        """Retire a transport without closing unrelated active leases."""

        close = False
        async with self._lock:
            entry = self._entries.pop(target.pool_key, None)
            if entry is None:
                return
            entry.broken = True
            close = entry.leases == 0
        if close:
            await self._close(entry)

    @staticmethod
    async def _terminate_process(process: Any) -> None:
        """Terminate one remote channel and bound its close acknowledgement."""

        terminate = getattr(process, "terminate", None)
        if callable(terminate):
            with suppress(Exception):
                terminate()
        await _bounded_wait_closed(process)

    @staticmethod
    async def _read_command_stream(
        reader: Any,
        *,
        chunks: list[bytes],
        remaining: list[int],
    ) -> None:
        while True:
            chunk = await reader.read(8192)
            if not chunk:
                return
            if isinstance(chunk, str):
                chunk = chunk.encode()
            elif not isinstance(chunk, bytes):
                chunk = bytes(chunk)
            remaining[0] -= len(chunk)
            if remaining[0] < 0:
                raise _CommandOutputTooLarge
            chunks.append(chunk)

    @staticmethod
    async def _wait_process(process: Any) -> None:
        wait_closed = getattr(process, "wait_closed", None)
        if callable(wait_closed):
            await wait_closed()
            return
        wait = getattr(process, "wait", None)
        if callable(wait):
            await wait()

    async def run_command(
        self,
        *,
        target: RemoteWorkspaceTarget,
        command: str,
        key_path: str,
        generation_resolver: GenerationResolver,
        timeout: float,
        max_output_bytes: int,
    ) -> PinnedSSHCommandResult:
        """Run one bounded command on the pinned, generation-bound transport."""

        if not command or not isinstance(command, str):
            raise ValueError("command must be a non-empty string")
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        if max_output_bytes <= 0:
            raise ValueError("max_output_bytes must be positive")

        async with self.checkout(target=target, key_path=key_path) as connection:
            require_same_remote_workspace(await generation_resolver(), target)
            process = None
            pumps: list[asyncio.Task[None]] = []
            stdout_chunks: list[bytes] = []
            stderr_chunks: list[bytes] = []
            remaining = [max_output_bytes]
            try:
                async with asyncio.timeout(timeout):
                    process = await connection.create_process(command, encoding=None)
                    pumps = [
                        asyncio.create_task(
                            self._read_command_stream(
                                process.stdout,
                                chunks=stdout_chunks,
                                remaining=remaining,
                            )
                        ),
                        asyncio.create_task(
                            self._read_command_stream(
                                process.stderr,
                                chunks=stderr_chunks,
                                remaining=remaining,
                            )
                        ),
                    ]
                    await asyncio.gather(*pumps)
                    await self._wait_process(process)
            except TimeoutError as exc:
                if process is not None:
                    await self._terminate_process(process)
                raise CanvasSSHError(
                    504,
                    "workspace_command_timeout",
                    "Workspace command timed out",
                ) from exc
            except asyncio.CancelledError:
                if process is not None:
                    cleanup = asyncio.create_task(self._terminate_process(process))
                    with suppress(asyncio.CancelledError):
                        await asyncio.shield(cleanup)
                raise
            except _CommandOutputTooLarge as exc:
                if process is not None:
                    await self._terminate_process(process)
                raise CanvasSSHError(
                    502,
                    "workspace_command_output_too_large",
                    "Workspace command output exceeded the allowed size",
                ) from exc
            except CanvasSSHError:
                raise
            except Exception as exc:
                if process is not None:
                    await self._terminate_process(process)
                if self._is_connection_closed(connection):
                    await self.invalidate(target)
                raise CanvasSSHError(
                    503,
                    "workspace_unavailable",
                    "Workspace SSH command could not be completed",
                ) from exc
            finally:
                for pump in pumps:
                    if not pump.done():
                        pump.cancel()
                if pumps:
                    await asyncio.gather(*pumps, return_exceptions=True)

            require_same_remote_workspace(await generation_resolver(), target)
            exit_status = getattr(process, "exit_status", None)
            if isinstance(exit_status, bool) or not isinstance(exit_status, int):
                raise CanvasSSHError(
                    503,
                    "workspace_unavailable",
                    "Workspace command did not return an exit status",
                )
            return PinnedSSHCommandResult(
                exit_status=exit_status,
                stdout=b"".join(stdout_chunks),
                stderr=b"".join(stderr_chunks),
            )

    @asynccontextmanager
    async def open_loopback_connection(
        self,
        *,
        target: RemoteWorkspaceTarget,
        destination_port: int,
        key_path: str,
        generation_resolver: GenerationResolver,
    ):
        """Open one direct channel to fixed workspace loopback, never a listener."""

        async with self.checkout(target=target, key_path=key_path) as connection:
            require_same_remote_workspace(await generation_resolver(), target)
            try:
                reader, writer = await connection.open_connection(
                    CANVAS_LOOPBACK_HOST, destination_port
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # Only an explicit SSH "connect failed" response proves that
                # the authenticated workspace loopback listener is closed.
                if asyncssh is not None and isinstance(exc, asyncssh.ChannelOpenError):
                    if exc.code == asyncssh.OPEN_CONNECT_FAILED:
                        raise CanvasDirectChannelUnavailable(
                            "Workspace loopback port is not accepting connections"
                        ) from exc
                    # Policy denial/resource shortage is scoped to this
                    # channel request and does not prove the shared transport
                    # is unhealthy.
                else:
                    # A non-channel exception is a transport failure. Retire
                    # the pool entry, deferring close until unrelated active
                    # leases finish.
                    await self.invalidate(target)
                raise CanvasSSHError(
                    503,
                    "workspace_unavailable",
                    "Workspace SSH direct channel could not be opened",
                ) from exc
            try:
                require_same_remote_workspace(await generation_resolver(), target)
                yield reader, writer
            finally:
                writer.close()
                await _bounded_wait_closed(writer)

    async def evict_thread(self, thread_id: str) -> None:
        """Close every pooled transport/channel belonging to one thread."""

        async with self._lock:
            doomed = [
                entry
                for key, entry in list(self._entries.items())
                if key[0] == thread_id
            ]
            for entry in doomed:
                self._entries.pop(entry.target.pool_key, None)
                entry.broken = True
        for entry in doomed:
            await self._close(entry)

    async def evict_generation(self, thread_id: str, generation: UUID) -> None:
        """Close transports for one retired thread workspace generation."""

        expected = str(generation)
        async with self._lock:
            doomed = [
                entry
                for key, entry in list(self._entries.items())
                if key[0] == thread_id and key[1] == expected
            ]
            for entry in doomed:
                self._entries.pop(entry.target.pool_key, None)
                entry.broken = True
        for entry in doomed:
            await self._close(entry)

    async def close_all(self) -> None:
        async with self._lock:
            doomed = list(self._entries.values())
            self._entries.clear()
            for entry in doomed:
                entry.broken = True
        for entry in doomed:
            await self._close(entry)


class PinnedSFTPPool:
    """Compatibility adapter running SFTP over the shared SSH transport pool."""

    def __init__(
        self,
        *,
        max_entries: int = 32,
        transport_pool: PinnedSSHTransportPool | None = None,
    ) -> None:
        self._transport_pool = transport_pool or PinnedSSHTransportPool(
            max_entries=max_entries
        )

    @asynccontextmanager
    async def checkout(
        self,
        *,
        thread_id: str,
        generation: UUID,
        host: str,
        port: int,
        fingerprint: str,
        key_path: str,
    ):
        target = RemoteWorkspaceTarget(
            thread_id=thread_id,
            generation=generation,
            host=host,
            port=port,
            fingerprint=fingerprint,
        )
        async with self._transport_pool.checkout(
            target=target, key_path=key_path
        ) as connection:
            try:
                sftp = await connection.start_sftp_client()
            except BaseException:
                # A failed subsystem startup can leave channel bookkeeping in
                # an unknown state. Retire this transport, but defer its close
                # until every unrelated concurrent lease has exited.
                await self._transport_pool.invalidate(target)
                raise
            try:
                yield sftp
            finally:
                with suppress(Exception):
                    sftp.exit()
                await _bounded_wait_closed(sftp)

    async def evict_thread(self, thread_id: str) -> None:
        await self._transport_pool.evict_thread(thread_id)

    async def evict_generation(self, thread_id: str, generation: UUID) -> None:
        await self._transport_pool.evict_generation(thread_id, generation)


PINNED_SSH_TRANSPORT_POOL = PinnedSSHTransportPool()


__all__ = [
    "CANVAS_LOOPBACK_HOST",
    "CanvasDirectChannelUnavailable",
    "CanvasSSHError",
    "EMPTY_KNOWN_HOSTS",
    "GenerationResolver",
    "PINNED_SSH_TRANSPORT_POOL",
    "PinnedSFTPPool",
    "PinnedSSHCommandResult",
    "PinnedSSHClient",
    "PinnedSSHTransportPool",
    "RemoteWorkspaceTarget",
    "VMAttestationVerdict",
    "VMCanvasIdentity",
    "VM_ATTESTATION_TIMEOUT_SECONDS",
    "asyncssh",
    "attest_vm_workspace_target",
    "bound_workspace_generation",
    "canvas_workspace_generation",
    "remote_target_is_vm_backed",
    "require_same_remote_workspace",
    "resolve_canvas_workspace_target",
    "resolve_remote_workspace_target",
    "resolve_vm_workspace_target",
    "vm_canvas_generation",
    "vm_canvas_identity",
    "vm_canvas_route_available",
    "vm_runtime_proof_matches",
    "workspace_metadata",
]
