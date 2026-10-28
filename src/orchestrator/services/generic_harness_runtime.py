"""Kubernetes hosting for an image with no required SRW application protocol.

The caller owns admission, immutable execution/attempt records, credential
authorization, deadlines, outcome policy and workspace fencing. This module
only translates an admitted snapshot and materialized bindings into isolated
Kubernetes objects and reports observations of their exact identities. It does
not load the reference harness or infer application success from readiness.
"""

from __future__ import annotations

import base64
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass, field
from decimal import Decimal
import json
from pathlib import PurePosixPath
import re
from typing import Protocol
from uuid import UUID

from orchestrator.services.pinned_k8s_effect import (
    run_bounded_k8s_call,
    run_bounded_k8s_mutation,
)


GENERIC_FINALIZER = "srw.io/generic-execution-protection"
_MANAGER = "generic-harness-runtime"
_RESERVED_ENV = frozenset({"SRW_CONFIG_FILE", "SRW_TASK_FILE", "SRW_BINDINGS_FILE"})
_ENV_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_NAMESPACE = re.compile(r"[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?\Z")
_QUANTITY = re.compile(r"([1-9][0-9]*)(Mi|Gi|Ti)\Z")
_MAX_DELIVERY_BYTES = 512 * 1024
_MISSING = object()


class GenericRuntimeError(RuntimeError):
    """A hosting failure that is safe to expose without Kubernetes response data."""


class RuntimeObjectIdentity(Protocol):
    @property
    def pod_name(self) -> str: ...

    @property
    def labels(self) -> dict[str, str]: ...


@dataclass(frozen=True)
class GenericAttemptIdentity:
    execution_id: str
    attempt: int

    def __post_init__(self):
        object.__setattr__(self, "execution_id", str(UUID(str(self.execution_id))))
        if (
            isinstance(self.attempt, bool)
            or not isinstance(self.attempt, int)
            or not 1 <= self.attempt <= 2**63 - 1
        ):
            raise ValueError("Attempt must be a positive database integer.")

    @property
    def pod_name(self) -> str:
        return f"srw-run-{UUID(self.execution_id).hex}-a{self.attempt}"

    @property
    def labels(self) -> dict[str, str]:
        # Deliberately omit the trusted SRW agent/chart labels, which grant
        # access to internal services under existing NetworkPolicies.
        return {
            "srw/managed-by": _MANAGER,
            "srw.io/execution-id": self.execution_id,
            "srw.io/execution-attempt": str(self.attempt),
        }


@dataclass(frozen=True)
class GenericBoundFile:
    """An explicitly authorized file delivered only to the harness container."""

    path: str
    content: str | bytes = field(repr=False)
    mode: int = 0o444


@dataclass(frozen=True)
class GenericBindings:
    """Trusted output of credential/connector/workspace materialization.

    ``secret_env`` supplies exactly the declared runtime.env SecretRefs;
    ``environment`` adds explicit connector bindings and may not override them.
    ``descriptor`` advertises authorized SSH workspace/connector endpoints, if
    requested, through SRW_BINDINGS_FILE. The descriptor does not mount workspace
    storage into the harness. ``egress`` contains operator-approved Kubernetes
    NetworkPolicy egress rules, never an unvalidated authored configuration.

    Values are deliberately excluded from repr and never copied into pod env
    literals, annotations, diagnostics, or a shared cluster Secret.
    """

    secret_env: Mapping[str, str] = field(default_factory=dict, repr=False)
    environment: Mapping[str, str] = field(default_factory=dict, repr=False)
    files: tuple[GenericBoundFile, ...] = field(default_factory=tuple, repr=False)
    descriptor: dict | None = field(default=None, repr=False)
    egress: tuple[dict, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class GenericRuntimePolicy:
    """Operator-owned limits and isolation options, separate from image config."""

    cpu_request: float = 0.25
    cpu_limit: float = 2
    memory_request: str = "256Mi"
    memory_limit: str = "2Gi"
    ephemeral_storage_request: str = "64Mi"
    ephemeral_storage_limit: str = "2Gi"
    termination_grace_seconds: int = 30
    runtime_class_name: str | None = None
    run_as_user: int | None = None
    run_as_group: int | None = None
    read_only_root_filesystem: bool = False
    max_cpu: float = 8
    max_memory: str = "16Gi"


@dataclass(frozen=True)
class GenericLaunchPlan:
    identity: RuntimeObjectIdentity
    namespace: str
    pod: dict = field(repr=False)
    delivery_secret: dict | None = field(repr=False)
    network_policy: dict = field(repr=False)


@dataclass(frozen=True)
class GenericPodObservation:
    pod_name: str
    pod_uid: str | None
    phase: str
    process_exit_code: int | None = None
    readiness: bool | None = None
    image_id: str | None = None
    reason: str | None = None
    containers_terminal: bool = False
    deletion_requested: bool = False
    pod_ip: str | None = None

    @property
    def pod_absent(self) -> bool:
        """API absence alone is not proof that remote workspace processes stopped."""
        return self.phase == "Absent"


def _cpu(value) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        raise ValueError("CPU must be a positive finite number.")
    number = Decimal(str(value))
    if not number.is_finite() or number <= 0:
        raise ValueError("CPU must be a positive finite number.")
    return number


def _bytes(value: str) -> int:
    match = _QUANTITY.fullmatch(value) if isinstance(value, str) else None
    if not match:
        raise ValueError("Memory/storage must use a positive Mi, Gi or Ti quantity.")
    return int(match[1]) * {"Mi": 2**20, "Gi": 2**30, "Ti": 2**40}[match[2]]


def _resources(runtime: dict, policy: GenericRuntimePolicy) -> dict:
    authored = runtime.get("resources", {})
    requests = {
        "cpu": policy.cpu_request,
        "memory": policy.memory_request,
        **authored.get("requests", {}),
    }
    limits = {
        "cpu": policy.cpu_limit,
        "memory": policy.memory_limit,
        **authored.get("limits", {}),
    }
    if _cpu(limits["cpu"]) > _cpu(policy.max_cpu) or _bytes(limits["memory"]) > _bytes(
        policy.max_memory
    ):
        raise ValueError("Runtime resources exceed the installation's hosting ceiling.")
    for name, parse in (("cpu", _cpu), ("memory", _bytes)):
        if parse(requests[name]) > parse(limits[name]):
            if name in authored.get("requests", {}):
                raise ValueError("Runtime resource requests exceed effective limits.")
            requests[name] = limits[name]
    requests["cpu"] = format(_cpu(requests["cpu"]), "f")
    limits["cpu"] = format(_cpu(limits["cpu"]), "f")
    if _bytes(policy.ephemeral_storage_request) > _bytes(
        policy.ephemeral_storage_limit
    ):
        raise ValueError("Ephemeral storage request exceeds its limit.")
    requests["ephemeral-storage"] = policy.ephemeral_storage_request
    limits["ephemeral-storage"] = policy.ephemeral_storage_limit
    return {"requests": requests, "limits": limits}


def pinned_image(image_id: str | None, authored_image: str) -> str:
    """Reuse an observed pullable manifest digest, never a mutable retry tag."""
    if re.fullmatch(r"[^\s@]+@sha256:[0-9a-f]{64}", authored_image):
        return authored_image
    candidate = (image_id or "").removeprefix("docker-pullable://")
    if re.fullmatch(r"[^\s@]+@sha256:[0-9a-f]{64}", candidate):
        return candidate
    raise GenericRuntimeError(
        "The runtime did not expose a pullable image digest; automatic image replacement is fenced."
    )


def _environment_name(name: str):
    if not isinstance(name, str) or not _ENV_NAME.fullmatch(name):
        raise ValueError("Invalid environment binding name.")
    if name in _RESERVED_ENV:
        raise ValueError("Environment bindings cannot replace SRW delivery variables.")


def _json_bytes(value) -> bytes:
    return json.dumps(
        value, ensure_ascii=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")


def build_generic_launch(
    identity: GenericAttemptIdentity,
    resolved_job: dict,
    *,
    namespace: str,
    bindings: GenericBindings | None = None,
    policy: GenericRuntimePolicy | None = None,
    remaining_timeout_seconds: int | None = None,
) -> GenericLaunchPlan:
    """Build from a validated, resolved and authorized Job *spec*.

    No image normalizer or tool catalogue is consulted. A supplied Job deadline
    must be converted to remaining seconds by the admission/reconciliation
    caller, so queueing and previous attempts never reset its timeout.
    """
    if not _NAMESPACE.fullmatch(namespace):
        raise ValueError("Invalid runtime namespace.")
    bindings = bindings or GenericBindings()
    policy = policy or GenericRuntimePolicy()
    selection = resolved_job["execution"]["expert"]
    if set(selection) != {"inline"}:
        raise ValueError("Generic execution requires a fully resolved expert.")
    runtime = deepcopy(selection["inline"]["runtime"])
    if runtime.get("adapter") is not None:
        raise ValueError("Reference-harness adapters require their dedicated runtime.")
    if any(key in runtime for key in ("command", "args", "probes")):
        # The schema has no launch fields. Reaching here means validation was
        # skipped; never translate an authored launch into a container spec.
        raise ValueError(
            "Images start with their own entrypoint; command, args and probes are not launch fields."
        )
    if "timeoutSeconds" in resolved_job and remaining_timeout_seconds is None:
        raise ValueError("The caller must supply the remaining whole-job deadline.")
    if remaining_timeout_seconds is not None and (
        isinstance(remaining_timeout_seconds, bool)
        or not isinstance(remaining_timeout_seconds, int)
        or remaining_timeout_seconds < 1
    ):
        raise ValueError("The whole-job deadline has already expired or is invalid.")

    name = identity.pod_name
    metadata = {"name": name, "namespace": namespace, "labels": identity.labels}
    env = []
    secret_data: dict[str, str] = {}
    secret_items = []
    mounts = []
    total_bytes = 0
    declared_env = runtime.get("env", {})
    expected_secrets = {
        key for key, value in declared_env.items() if isinstance(value, dict)
    }
    if set(bindings.secret_env) != expected_secrets:
        raise ValueError(
            "Every declared secret environment binding must be authorized."
        )
    if set(declared_env).intersection(bindings.environment):
        raise ValueError("Connector environment bindings collide with runtime.env.")
    if len(declared_env) + len(bindings.environment) > 512 or len(bindings.files) > 128:
        raise ValueError("Runtime delivery exceeds the supported binding count.")

    def data_key(content: bytes) -> str:
        nonlocal total_bytes
        total_bytes += len(content)
        if total_bytes > _MAX_DELIVERY_BYTES:
            raise ValueError("Runtime delivery exceeds the 512 KiB payload limit.")
        key = f"value-{len(secret_data)}"
        secret_data[key] = base64.b64encode(content).decode("ascii")
        return key

    def secret_environment(key: str, value: str):
        _environment_name(key)
        if not isinstance(value, str) or "\x00" in value:
            raise ValueError("Environment binding values must be strings without NUL.")
        env.append(
            {
                "name": key,
                "valueFrom": {
                    "secretKeyRef": {"name": name, "key": data_key(value.encode())}
                },
            }
        )

    paths: set[str] = set()

    def mounted_file(path: str, content: bytes, mode: int = 0o444):
        parsed = PurePosixPath(path)
        if (
            not path.startswith("/")
            or path.startswith("//")
            or "\x00" in path
            or str(parsed) != path
            or ".." in parsed.parts
            or path == "/"
            or any(
                path == prefix or path.startswith(prefix + "/")
                for prefix in ("/proc", "/sys", "/dev")
            )
            or any(
                path == other
                or path.startswith(other + "/")
                or other.startswith(path + "/")
                for other in paths
            )
        ):
            raise ValueError(
                "File binding paths must be distinct, normalized absolute files."
            )
        if (
            isinstance(mode, bool)
            or not isinstance(mode, int)
            or mode < 0
            or mode & ~0o777
        ):
            raise ValueError(
                "File binding mode must contain only ordinary permission bits."
            )
        paths.add(path)
        key = data_key(content)
        secret_items.append({"key": key, "path": key, "mode": mode})
        mounts.append(
            {"name": "delivery", "mountPath": path, "subPath": key, "readOnly": True}
        )

    for key, value in declared_env.items():
        _environment_name(key)
        if isinstance(value, str):
            if "\x00" in value:
                raise ValueError("Environment values cannot contain NUL.")
            env.append({"name": key, "value": value})
        elif isinstance(value, dict) and set(value) == {"secretRef"}:
            secret_environment(key, bindings.secret_env[key])
        else:
            raise ValueError("Runtime environment must contain strings or SecretRefs.")
    for key, value in bindings.environment.items():
        secret_environment(key, value)
    for source, variable, path in (
        (runtime, "SRW_CONFIG_FILE", "/run/srw/config.json"),
        (resolved_job, "SRW_TASK_FILE", "/run/srw/task.json"),
    ):
        key = "config" if variable == "SRW_CONFIG_FILE" else "task"
        if key in source:
            mounted_file(path, _json_bytes(source[key]))
            env.append({"name": variable, "value": path})
    if bindings.descriptor is not None:
        mounted_file("/run/srw/bindings.json", _json_bytes(bindings.descriptor))
        env.append({"name": "SRW_BINDINGS_FILE", "value": "/run/srw/bindings.json"})
    for item in bindings.files:
        # Only the connector-files subtree is available within platform
        # delivery. Keep the directory and config/task/descriptor paths reserved
        # even when an optional delivery file is absent from this execution.
        if (
            item.path == "/run/srw"
            or item.path.startswith("/run/srw/")
            or "/run/srw".startswith(item.path + "/")
        ) and not item.path.startswith("/run/srw/bindings/"):
            raise ValueError("File bindings cannot replace the SRW delivery directory.")
        content = (
            item.content.encode() if isinstance(item.content, str) else item.content
        )
        if not isinstance(content, bytes):
            raise ValueError("File binding content must be bytes or text.")
        mounted_file(item.path, content, item.mode)

    container = {
        "name": "harness",
        "image": runtime["image"],
        "imagePullPolicy": runtime.get("pullPolicy", "IfNotPresent"),
        "resources": _resources(runtime, policy),
        "securityContext": {
            "allowPrivilegeEscalation": False,
            "privileged": False,
            "capabilities": {"drop": ["ALL"]},
            "readOnlyRootFilesystem": policy.read_only_root_filesystem,
        },
    }
    # The image's own ENTRYPOINT and CMD always run: no command, no args, no
    # probes and no shell wrapper. Readiness observed later belongs to probes
    # the workspace runtime declares on the pods it builds itself.
    if env:
        container["env"] = env
    if mounts:
        container["volumeMounts"] = mounts
    pod_spec = {
        "containers": [container],
        "restartPolicy": "Never",
        "automountServiceAccountToken": False,
        "enableServiceLinks": False,
        "hostNetwork": False,
        "hostPID": False,
        "hostIPC": False,
        "shareProcessNamespace": False,
        "securityContext": {"seccompProfile": {"type": "RuntimeDefault"}},
        "terminationGracePeriodSeconds": policy.termination_grace_seconds,
    }
    if remaining_timeout_seconds is not None:
        pod_spec["activeDeadlineSeconds"] = remaining_timeout_seconds
    if policy.runtime_class_name is not None:
        pod_spec["runtimeClassName"] = policy.runtime_class_name
    if policy.run_as_user is not None:
        pod_spec["securityContext"]["runAsUser"] = policy.run_as_user
        pod_spec["securityContext"]["runAsNonRoot"] = policy.run_as_user != 0
    if policy.run_as_group is not None:
        pod_spec["securityContext"]["runAsGroup"] = policy.run_as_group
    if mounts:
        pod_spec["volumes"] = [
            {"name": "delivery", "secret": {"secretName": name, "items": secret_items}}
        ]
    secret = (
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": deepcopy(metadata),
            "type": "Opaque",
            "immutable": True,
            "data": secret_data,
        }
        if secret_data
        else None
    )
    return GenericLaunchPlan(
        identity=identity,
        namespace=namespace,
        pod={
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {**deepcopy(metadata), "finalizers": [GENERIC_FINALIZER]},
            "spec": pod_spec,
        },
        delivery_secret=secret,
        network_policy={
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": deepcopy(metadata),
            "spec": {
                "podSelector": {"matchLabels": identity.labels},
                "policyTypes": ["Ingress", "Egress"],
                "ingress": [],
                "egress": deepcopy(list(bindings.egress)),
            },
        },
    )


def _field(obj, name: str, default=None):
    if isinstance(obj, Mapping):
        return obj.get(name, default)
    snake = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    snake = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", snake).lower()
    return getattr(obj, snake, default)


def _matches(actual, expected) -> bool:
    """Compare desired fields while permitting Kubernetes defaulted fields."""
    if isinstance(expected, dict):
        for key, value in expected.items():
            found = _field(actual, key, _MISSING)
            if (
                value is False
                and key
                in {
                    "hostNetwork",
                    "hostPID",
                    "hostIPC",
                    "shareProcessNamespace",
                    "privileged",
                    "readOnlyRootFilesystem",
                }
                and (found is _MISSING or found is None)
            ):
                # These are non-pointer fields omitted by Kubernetes' JSON
                # serializer when false. Token/service-env defaults are true
                # and must never receive this exception.
                continue
            if (
                key in {"cpu", "memory", "storage", "ephemeral-storage"}
                and isinstance(found, str)
                and isinstance(value, str)
            ):
                from kubernetes.utils.quantity import parse_quantity

                try:
                    if parse_quantity(found) == parse_quantity(value):
                        continue
                except (ValueError, TypeError):
                    pass
            if not _matches(found, value):
                return False
        return True
    if isinstance(expected, list):
        if not expected and (actual is None or actual is _MISSING):
            return True
        return (
            isinstance(actual, (list, tuple))
            and len(actual) == len(expected)
            and all(_matches(found, wanted) for found, wanted in zip(actual, expected))
        )
    return actual == expected


def _owned(obj, identity: RuntimeObjectIdentity) -> bool:
    metadata = _field(obj, "metadata")
    return _field(metadata, "name") == identity.pod_name and _matches(
        _field(metadata, "labels"), identity.labels
    )


def _failed_initialization_is_terminal(spec, status) -> bool:
    """A failed non-restarting init sequence never starts later containers.

    Waiting/missing regular-container status is safe only with this positive
    init failure evidence. A terminal Pod phase or deletion timestamp alone
    must not be used to infer process absence after node loss.
    """
    if _field(spec, "restartPolicy") != "Never" or _field(status, "phase") != "Failed":
        return False
    initializers = _field(spec, "initContainers", []) or []
    by_name = {
        _field(item, "name"): item
        for item in _field(status, "initContainerStatuses", []) or []
    }
    failed = False
    for initializer in initializers:
        if _field(initializer, "restartPolicy") == "Always":
            return False
        member = by_name.get(_field(initializer, "name"))
        terminated = _field(_field(member, "state"), "terminated")
        if not failed:
            if terminated is None:
                return False
            code = _field(terminated, "exitCode")
            if code is None:
                return False
            failed = code != 0
        elif member is not None and (
            _field(member, "containerID")
            or _field(_field(member, "state"), "running") is not None
            or _field(_field(member, "lastState"), "terminated") is not None
        ):
            return False
    if not failed or _field(spec, "ephemeralContainers", []):
        return False
    for member in _field(status, "containerStatuses", []) or []:
        if (
            _field(member, "containerID")
            or _field(_field(member, "state"), "running") is not None
            or _field(_field(member, "state"), "terminated") is not None
            or _field(_field(member, "lastState"), "terminated") is not None
        ):
            return False
    return True


def _observation(
    pod,
    identity: RuntimeObjectIdentity,
    expected_uid: str | None,
    *,
    container_name: str = "harness",
):
    uid = _field(_field(pod, "metadata"), "uid")
    if not _owned(pod, identity) or not uid or (expected_uid and uid != expected_uid):
        return GenericPodObservation(identity.pod_name, uid, "Replaced")
    spec, status = _field(pod, "spec"), _field(pod, "status")
    statuses = _field(status, "containerStatuses", []) or []
    harness = next(
        (item for item in statuses if _field(item, "name") == container_name), None
    )
    containers = _field(spec, "containers", []) or []
    declared = next(
        (item for item in containers if _field(item, "name") == container_name), None
    )
    state = _field(harness, "state")
    terminated, waiting = _field(state, "terminated"), _field(state, "waiting")
    all_terminal = bool(containers)
    for spec_key, status_key in (
        ("containers", "containerStatuses"),
        ("initContainers", "initContainerStatuses"),
        ("ephemeralContainers", "ephemeralContainerStatuses"),
    ):
        members = _field(status, status_key, []) or []
        expected_names = {
            _field(item, "name") for item in _field(spec, spec_key, []) or []
        }
        observed_names = {_field(item, "name") for item in members}
        all_terminal = (
            all_terminal
            and expected_names.issubset(observed_names)
            and all(
                _field(_field(item, "state"), "terminated") is not None
                for item in members
            )
        )
    all_terminal = all_terminal or _failed_initialization_is_terminal(spec, status)
    return GenericPodObservation(
        pod_name=identity.pod_name,
        pod_uid=str(uid),
        phase=_field(status, "phase") or "Unknown",
        process_exit_code=_field(terminated, "exitCode"),
        readiness=(
            bool(_field(harness, "ready", False))
            if _field(declared, "readinessProbe") is not None
            else None
        ),
        image_id=_field(harness, "imageID"),
        reason=_field(terminated, "reason")
        or _field(waiting, "reason")
        or _field(status, "reason"),
        containers_terminal=bool(all_terminal),
        deletion_requested=_field(_field(pod, "metadata"), "deletionTimestamp")
        is not None,
        pod_ip=_field(status, "podIP"),
    )


class GenericHarnessRuntime:
    """Bounded Kubernetes effects; no database or application outcome writes.

    The execution reconciler must durably reserve the attempt and serialize
    launch/cancel before calling these methods. It must never call launch again
    after that attempt has an observed pod UID or terminal state. A finalizer
    retains terminal pods until their outcomes are durably recorded and cleanup
    is explicitly requested. Generic labels exclude the existing SRW pod reaper.
    """

    def __init__(
        self,
        core_api,
        networking_api,
        *,
        namespace: str,
        container_name: str = "harness",
    ):
        if not _NAMESPACE.fullmatch(namespace):
            raise ValueError("Invalid runtime namespace.")
        self.core_api = core_api
        self.networking_api = networking_api
        self.namespace = namespace
        self.container_name = container_name

    async def _read_pod(self, identity):
        try:
            return await run_bounded_k8s_call(
                self.core_api.read_namespaced_pod,
                name=identity.pod_name,
                namespace=self.namespace,
            )
        except Exception as exc:
            if getattr(exc, "status", None) == 404:
                return None
            raise GenericRuntimeError(
                "Unable to observe the generic runtime pod."
            ) from None

    async def _create(self, create, read, body, identity):
        try:
            return await run_bounded_k8s_mutation(
                create, namespace=self.namespace, body=body
            )
        except Exception as exc:
            if getattr(exc, "status", None) != 409:
                # Transport failures may have committed a create. Do not erase
                # the durable intent or its bindings; reconciliation reads it.
                raise GenericRuntimeError(
                    "Generic runtime object creation is unconfirmed."
                ) from None
        try:
            existing = await run_bounded_k8s_call(
                read, namespace=self.namespace, name=identity.pod_name
            )
        except Exception:
            raise GenericRuntimeError(
                "Unable to inspect an existing generic runtime object."
            ) from None
        if not _owned(existing, identity) or not _matches(existing, body):
            raise GenericRuntimeError(
                "Existing runtime object does not match the reserved attempt."
            )
        return existing

    async def launch(self, plan: GenericLaunchPlan) -> GenericPodObservation:
        if plan.namespace != self.namespace:
            raise ValueError("The launch plan belongs to another namespace.")
        # Create the isolation selector before creating any matching pod. CNI
        # enforcement must be verified for the installed cluster implementation.
        await self._create(
            self.networking_api.create_namespaced_network_policy,
            self.networking_api.read_namespaced_network_policy,
            plan.network_policy,
            plan.identity,
        )
        if plan.delivery_secret is not None:
            await self._create(
                self.core_api.create_namespaced_secret,
                self.core_api.read_namespaced_secret,
                plan.delivery_secret,
                plan.identity,
            )
        pod = await self._create(
            self.core_api.create_namespaced_pod,
            self.core_api.read_namespaced_pod,
            plan.pod,
            plan.identity,
        )
        return _observation(
            pod, plan.identity, None, container_name=self.container_name
        )

    async def observe(
        self, identity: RuntimeObjectIdentity, *, expected_pod_uid: str | None
    ) -> GenericPodObservation:
        pod = await self._read_pod(identity)
        if pod is None:
            return GenericPodObservation(identity.pod_name, None, "Absent")
        return _observation(
            pod, identity, expected_pod_uid, container_name=self.container_name
        )

    async def cancel(
        self, identity: RuntimeObjectIdentity, *, expected_pod_uid: str
    ) -> GenericPodObservation:
        if not expected_pod_uid:
            raise ValueError("Cancellation requires the observed pod UID.")
        observed = await self.observe(identity, expected_pod_uid=expected_pod_uid)
        if observed.pod_absent or observed.phase == "Replaced":
            return observed
        try:
            await run_bounded_k8s_mutation(
                self.core_api.delete_namespaced_pod,
                namespace=self.namespace,
                name=identity.pod_name,
                body={"preconditions": {"uid": expected_pod_uid}},
            )
        except Exception as exc:
            if getattr(exc, "status", None) not in {404, 409}:
                raise GenericRuntimeError(
                    "Generic runtime cancellation is unconfirmed."
                ) from None
        return await self.observe(identity, expected_pod_uid=expected_pod_uid)

    async def cleanup(
        self, identity: RuntimeObjectIdentity, *, expected_pod_uid: str
    ) -> bool:
        """Remove only a settled, terminal attempt and its delivery objects.

        Returning True means the API reports the pod and auxiliary objects
        absent. The caller separately fences any SSH workspace processes and
        revokes credentials before reusing its workspace for another attempt.
        """
        if not expected_pod_uid:
            raise ValueError("Cleanup requires the observed pod UID.")
        pod = await self._read_pod(identity)
        if pod is not None:
            observed = _observation(
                pod, identity, expected_pod_uid, container_name=self.container_name
            )
            if observed.phase == "Replaced" or not observed.containers_terminal:
                return False
            metadata = _field(pod, "metadata")
            finalizers = _field(metadata, "finalizers", []) or []
            if GENERIC_FINALIZER in finalizers:
                resource_version = _field(metadata, "resourceVersion")
                if not resource_version:
                    return False
                try:
                    await run_bounded_k8s_mutation(
                        self.core_api.patch_namespaced_pod,
                        namespace=self.namespace,
                        name=identity.pod_name,
                        body=[
                            {
                                "op": "test",
                                "path": "/metadata/uid",
                                "value": expected_pod_uid,
                            },
                            {
                                "op": "test",
                                "path": "/metadata/resourceVersion",
                                "value": resource_version,
                            },
                            {
                                "op": "test",
                                "path": "/metadata/finalizers",
                                "value": finalizers,
                            },
                            {
                                "op": "replace",
                                "path": "/metadata/finalizers",
                                "value": [
                                    item
                                    for item in finalizers
                                    if item != GENERIC_FINALIZER
                                ],
                            },
                        ],
                    )
                except Exception as exc:
                    if getattr(exc, "status", None) != 404:
                        return False
            observed = await self.cancel(identity, expected_pod_uid=expected_pod_uid)
            if not observed.pod_absent:
                return False
        for read, delete in (
            (
                self.core_api.read_namespaced_secret,
                self.core_api.delete_namespaced_secret,
            ),
            (
                self.networking_api.read_namespaced_network_policy,
                self.networking_api.delete_namespaced_network_policy,
            ),
        ):
            try:
                obj = await run_bounded_k8s_call(
                    read, namespace=self.namespace, name=identity.pod_name
                )
                uid = _field(_field(obj, "metadata"), "uid")
                if not uid or not _owned(obj, identity):
                    return False
                await run_bounded_k8s_mutation(
                    delete,
                    namespace=self.namespace,
                    name=identity.pod_name,
                    body={"preconditions": {"uid": uid}},
                )
                # Kubernetes deletion can leave an object with another
                # controller's finalizer. A successful DELETE is not absence.
                await run_bounded_k8s_call(
                    read, namespace=self.namespace, name=identity.pod_name
                )
                return False
            except Exception as exc:
                if getattr(exc, "status", None) != 404:
                    return False
        return True
