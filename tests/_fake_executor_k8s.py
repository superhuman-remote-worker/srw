"""In-memory apiserver + kubelet for pooled stateless executor Pods.

Only the behaviour the claimant-loss retention boundary depends on is modelled:
UID-preconditioned graceful delete, JSON Patch ``test``/``replace`` over
``/metadata/{uid,resourceVersion,finalizers}`` with resourceVersion bumps, and
the kubelet's two terminal edges — containers stop, then the object is removed
unless a finalizer still retains it (the 0.66 s window measured on k3d for a
finalizer-free Pod collapses to "immediately" here).
"""

from __future__ import annotations

import copy
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any

EXECUTOR_NAMESPACE = "srw"
POOL_NAME = "superhuman-remote-worker"
POOL_INSTANCE = "srw"


class ApiError(Exception):
    def __init__(self, status: int) -> None:
        super().__init__(f"HTTP {status}")
        self.status = status


def container_status(
    name: str,
    state: str,
    *,
    exit_code: int = 0,
    reason: str | None = None,
    restart_count: int = 0,
) -> SimpleNamespace:
    running = waiting = terminated = None
    container_id: str | None = f"containerd://{name}"
    if state == "running":
        running = SimpleNamespace(started_at=None)
    elif state == "terminated":
        terminated = SimpleNamespace(exit_code=exit_code, reason=reason or "Completed")
    elif state == "waiting":
        waiting = SimpleNamespace(reason=reason or "PodInitializing")
        container_id = None
    else:  # pragma: no cover - fixture misuse
        raise ValueError(state)
    return SimpleNamespace(
        name=name,
        state=SimpleNamespace(running=running, waiting=waiting, terminated=terminated),
        last_state=SimpleNamespace(running=None, waiting=None, terminated=None),
        container_id=container_id,
        restart_count=restart_count,
        started=state == "running",
        ready=state == "running",
    )


def executor_pod(
    *,
    name: str = "srw-agent-stateless-68d8c97ff6-w97cz",
    uid: str = "072ccef4-0000-4000-8000-000000000001",
    finalizers: list[str] | None = None,
    phase: str = "Running",
    node_name: str | None = "node-a",
    deleting: bool = False,
    init: tuple[tuple[str, str, int], ...] = (
        ("wait-for-orchestrator", "terminated", 0),
    ),
    containers: tuple[tuple[str, str], ...] = (("agent", "running"),),
    labels: dict[str, str] | None = None,
    resource_version: str = "100",
) -> SimpleNamespace:
    pod_labels = {
        "app.kubernetes.io/name": POOL_NAME,
        "app.kubernetes.io/instance": POOL_INSTANCE,
        "app.kubernetes.io/component": "agent-stateless",
        "srw/class": "agent-stateless",
        "app": "srw-agent-stateless",
    }
    pod_labels.update(labels or {})
    return SimpleNamespace(
        metadata=SimpleNamespace(
            name=name,
            namespace=EXECUTOR_NAMESPACE,
            uid=uid,
            resource_version=resource_version,
            finalizers=list(finalizers) if finalizers is not None else None,
            deletion_timestamp=(
                datetime.now(timezone.utc) - timedelta(seconds=1) if deleting else None
            ),
            labels=pod_labels,
            annotations={},
        ),
        spec=SimpleNamespace(
            node_name=node_name,
            init_containers=[SimpleNamespace(name=item[0]) for item in init],
            containers=[SimpleNamespace(name=item[0]) for item in containers],
            ephemeral_containers=None,
        ),
        status=SimpleNamespace(
            phase=phase,
            init_container_statuses=[
                container_status(item_name, state, exit_code=exit_code)
                for item_name, state, exit_code in init
            ],
            container_statuses=[
                container_status(item_name, state) for item_name, state in containers
            ],
            ephemeral_container_statuses=None,
        ),
    )


class FakeExecutorCoreApi:
    """The subset of ``CoreV1Api`` the orchestrator uses on executor Pods."""

    def __init__(self, *pods: SimpleNamespace) -> None:
        self.pods: dict[str, SimpleNamespace] = {pod.metadata.name: pod for pod in pods}
        self.patches: list[tuple[str, list[dict[str, Any]]]] = []
        self.deletes: list[tuple[str, dict[str, Any] | None, int | None]] = []
        self.removed: list[str] = []
        # Every Node is healthy unless listed here.
        self.missing_nodes: set[str] = set()
        self.out_of_service_nodes: set[str] = set()
        self.node_read_error: int | None = None
        self.pod_read_error: int | None = None
        self.node_reads: list[str] = []

    # -- apiserver -------------------------------------------------------
    def _get(self, name: str) -> SimpleNamespace:
        pod = self.pods.get(name)
        if pod is None:
            raise ApiError(404)
        return pod

    @staticmethod
    def _bump(pod: SimpleNamespace) -> None:
        pod.metadata.resource_version = str(int(pod.metadata.resource_version) + 1)

    def _maybe_remove(self, pod: SimpleNamespace) -> None:
        if pod.metadata.deletion_timestamp is not None and not pod.metadata.finalizers:
            self.pods.pop(pod.metadata.name, None)
            self.removed.append(pod.metadata.name)

    def read_namespaced_pod(self, name: str, namespace: str, **_: Any):
        assert namespace == EXECUTOR_NAMESPACE
        if self.pod_read_error is not None:
            raise ApiError(self.pod_read_error)
        return copy.deepcopy(self._get(name))

    def read_node(self, name: str, **_: Any):
        self.node_reads.append(name)
        if self.node_read_error is not None:
            raise ApiError(self.node_read_error)
        if name in self.missing_nodes:
            raise ApiError(404)
        taints = []
        if name in self.out_of_service_nodes:
            taints.append(
                SimpleNamespace(
                    key="node.kubernetes.io/out-of-service",
                    value="nodeshutdown",
                    effect="NoExecute",
                )
            )
        return SimpleNamespace(
            metadata=SimpleNamespace(name=name), spec=SimpleNamespace(taints=taints)
        )

    def list_namespaced_pod(self, namespace: str, label_selector: str = "", **_: Any):
        assert namespace == EXECUTOR_NAMESPACE
        wanted = dict(
            item.split("=", 1) for item in str(label_selector).split(",") if item
        )
        return SimpleNamespace(
            items=[
                copy.deepcopy(pod)
                for pod in self.pods.values()
                if all(pod.metadata.labels.get(k) == v for k, v in wanted.items())
            ]
        )

    def delete_namespaced_pod(
        self,
        name: str,
        namespace: str,
        body: dict[str, Any] | None = None,
        grace_period_seconds: int | None = None,
        **_: Any,
    ) -> None:
        pod = self._get(name)
        precondition = ((body or {}).get("preconditions") or {}).get("uid")
        if precondition and precondition != pod.metadata.uid:
            raise ApiError(409)
        self.deletes.append((name, body, grace_period_seconds))
        if pod.metadata.deletion_timestamp is None:
            pod.metadata.deletion_timestamp = datetime.now(timezone.utc) + timedelta(
                seconds=int(grace_period_seconds or 30)
            )
            self._bump(pod)
        if not pod.spec.node_name:
            # An unscheduled Pod has no kubelet to wait for: the apiserver
            # removes it at once unless a finalizer retains it.
            self._maybe_remove(pod)

    def patch_namespaced_pod(
        self, name: str, namespace: str, body: Any, **_: Any
    ) -> None:
        pod = self._get(name)
        self.patches.append((name, copy.deepcopy(body)))
        assert isinstance(body, list), "executor finalizer edits are JSON Patch"
        current = {
            "/metadata/uid": pod.metadata.uid,
            "/metadata/resourceVersion": pod.metadata.resource_version,
            "/metadata/finalizers": list(pod.metadata.finalizers or []),
        }
        for op in body:
            if op["op"] == "test" and current.get(op["path"]) != op["value"]:
                raise ApiError(422)
        for op in body:
            if op["op"] in {"replace", "add"}:
                assert op["path"] == "/metadata/finalizers", op
                pod.metadata.finalizers = list(op["value"])
        self._bump(pod)
        self._maybe_remove(pod)

    # -- kubelet ---------------------------------------------------------
    def kubelet_terminates(self, name: str) -> None:
        """Stop every container of a deleting Pod, then try to remove it."""

        pod = self._get(name)
        assert pod.metadata.deletion_timestamp is not None
        for status in [
            *pod.status.init_container_statuses,
            *pod.status.container_statuses,
        ]:
            if status.state.terminated is None:
                status.state = SimpleNamespace(
                    running=None,
                    waiting=None,
                    terminated=SimpleNamespace(exit_code=143, reason="Error"),
                )
                status.started = False
                status.ready = False
        pod.status.phase = "Failed"
        self._bump(pod)
        self._maybe_remove(pod)
