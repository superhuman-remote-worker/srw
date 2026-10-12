"""Cancellation and exact-finalizer proofs for pinned Kubernetes effects."""

import asyncio
import threading
from types import SimpleNamespace

import pytest

from orchestrator.services.pinned_k8s_effect import (
    K8S_MUTATION_REQUEST_TIMEOUT,
    PINNED_AUTHORITY_FINALIZER,
    finalizer_install_patch,
    finalizer_release_patch,
    legacy_pinned_namespace_candidates,
    protect_legacy_pinned_agent_authority,
    release_planned_pinned_pod_authority,
    run_bounded_k8s_mutation,
)


@pytest.mark.asyncio
async def test_cancelled_mutation_is_joined_before_cancellation_propagates():
    started = threading.Event()
    release = threading.Event()
    observed_timeout = None

    def mutation(*, _request_timeout=None):
        nonlocal observed_timeout
        observed_timeout = _request_timeout
        started.set()
        assert release.wait(2)
        return "committed"

    task = asyncio.create_task(run_bounded_k8s_mutation(mutation))
    assert await asyncio.to_thread(started.wait, 1)
    task.cancel()
    await asyncio.sleep(0.02)
    assert not task.done()

    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert observed_timeout == K8S_MUTATION_REQUEST_TIMEOUT


@pytest.mark.asyncio
async def test_cancelled_adoption_read_is_bounded_and_joined():
    started = threading.Event()
    release = threading.Event()
    observed_timeout = None

    class CoreApi:
        def read_namespaced_pod(
            self, *, name, namespace, _request_timeout=None
        ) -> SimpleNamespace:
            del name, namespace
            nonlocal observed_timeout
            observed_timeout = _request_timeout
            started.set()
            assert release.wait(2)
            return SimpleNamespace(
                metadata=SimpleNamespace(
                    uid="pod-u1",
                    resource_version="1",
                    labels={"authority": "exact"},
                    finalizers=[],
                    deletion_timestamp=None,
                )
            )

    task = asyncio.create_task(
        protect_legacy_pinned_agent_authority(
            CoreApi(),
            namespaces=("agents-old",),
            pod_name="legacy-pod",
            expected_pod_uid="pod-u1",
            pod_labels={"authority": "exact"},
        )
    )
    assert await asyncio.to_thread(started.wait, 1)
    task.cancel()
    await asyncio.sleep(0.02)
    assert not task.done()

    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert observed_timeout == K8S_MUTATION_REQUEST_TIMEOUT


def test_finalizer_patch_is_uid_version_exact_and_preserves_foreign_owners():
    finalizers = ["storage.example/protection", PINNED_AUTHORITY_FINALIZER]

    patch = finalizer_release_patch(
        uid="pod-u1", resource_version="41", finalizers=finalizers
    )

    assert patch == [
        {"op": "test", "path": "/metadata/uid", "value": "pod-u1"},
        {
            "op": "test",
            "path": "/metadata/resourceVersion",
            "value": "41",
        },
        {"op": "test", "path": "/metadata/finalizers", "value": finalizers},
        {
            "op": "replace",
            "path": "/metadata/finalizers",
            "value": ["storage.example/protection"],
        },
    ]


def test_finalizer_patch_is_idempotent_when_authority_already_released():
    assert (
        finalizer_release_patch(
            uid="pod-u1",
            resource_version="42",
            finalizers=["storage.example/protection"],
        )
        is None
    )


def test_finalizer_install_patch_handles_missing_list_and_fences_uid_version():
    assert finalizer_install_patch(
        uid="legacy-pod", resource_version="17", finalizers=[]
    ) == [
        {"op": "test", "path": "/metadata/uid", "value": "legacy-pod"},
        {
            "op": "test",
            "path": "/metadata/resourceVersion",
            "value": "17",
        },
        {
            "op": "add",
            "path": "/metadata/finalizers",
            "value": [PINNED_AUTHORITY_FINALIZER],
        },
    ]


def test_legacy_namespace_search_is_explicit_bounded_and_deduplicated(monkeypatch):
    monkeypatch.setenv(
        "PINNED_LEGACY_AGENT_NAMESPACES", "agents-old,agents-current,agents-old"
    )
    assert legacy_pinned_namespace_candidates("agents-current") == (
        "agents-current",
        "agents-old",
    )


@pytest.mark.asyncio
async def test_legacy_adoption_refuses_ambiguous_namespace_before_any_patch():
    patch_calls: list[str] = []

    class CoreApi:
        def read_namespaced_pod(
            self, *, name, namespace, _request_timeout=None
        ) -> SimpleNamespace:
            del name, _request_timeout
            return SimpleNamespace(
                metadata=SimpleNamespace(
                    uid=f"pod-{namespace}",
                    resource_version="1",
                    labels={"authority": "exact"},
                    finalizers=[],
                    deletion_timestamp=None,
                )
            )

        def patch_namespaced_pod(self, *, namespace, **_kwargs):
            patch_calls.append(namespace)

    evidence = await protect_legacy_pinned_agent_authority(
        CoreApi(),
        namespaces=("agents-current", "agents-old"),
        pod_name="legacy-pod",
        expected_pod_uid=None,
        pod_labels={"authority": "exact"},
    )

    assert evidence is None
    assert patch_calls == []


@pytest.mark.asyncio
async def test_warm_release_removes_finalizer_from_exact_terminal_deleting_pod():
    class NotFound(Exception):
        status = 404

    class CoreApi:
        def __init__(self) -> None:
            self.pod = SimpleNamespace(
                metadata=SimpleNamespace(
                    uid="pod-u1",
                    resource_version="41",
                    labels={"authority": "exact"},
                    finalizers=[PINNED_AUTHORITY_FINALIZER],
                    annotations=None,
                    deletion_timestamp="2026-09-13T22:09:01Z",
                ),
                status=SimpleNamespace(
                    container_statuses=[
                        SimpleNamespace(
                            state=SimpleNamespace(terminated=SimpleNamespace())
                        )
                    ]
                ),
            )

        def read_namespaced_pod(self, *, name, namespace, _request_timeout=None):
            del name, namespace, _request_timeout
            if self.pod is None:
                raise NotFound()
            return self.pod

        def patch_namespaced_pod(self, *, name, namespace, body, _request_timeout=None):
            del name, namespace, _request_timeout
            assert body[-1] == {
                "op": "replace",
                "path": "/metadata/finalizers",
                "value": [],
            }
            self.pod = None

    api = CoreApi()
    assert await release_planned_pinned_pod_authority(
        api,
        namespace="agents-a",
        pod_name="warm-pod",
        expected_pod_uid="pod-u1",
        expected_labels={"authority": "exact"},
    ) == {"outcome": "exact_absent_v1", "agent_present": False}


@pytest.mark.asyncio
async def test_terminal_warm_delete_cannot_delete_same_name_successor():
    class Conflict(Exception):
        status = 409

    class CoreApi:
        def __init__(self) -> None:
            self.pod = SimpleNamespace(
                metadata=SimpleNamespace(
                    uid="old-uid",
                    resource_version="41",
                    labels={"authority": "exact"},
                    finalizers=[PINNED_AUTHORITY_FINALIZER],
                    annotations=None,
                    deletion_timestamp=None,
                ),
                status=SimpleNamespace(
                    phase="Succeeded",
                    container_statuses=[
                        SimpleNamespace(
                            state=SimpleNamespace(terminated=SimpleNamespace())
                        )
                    ],
                ),
            )

        def read_namespaced_pod(self, *, name, namespace, _request_timeout=None):
            del name, namespace, _request_timeout
            return self.pod

        def delete_namespaced_pod(
            self, *, name, namespace, body, _request_timeout=None
        ):
            del name, namespace, _request_timeout
            self.pod.metadata.uid = "successor-uid"
            self.pod.status.phase = "Running"
            if body.get("preconditions", {}).get("uid") != self.pod.metadata.uid:
                raise Conflict()
            self.pod.metadata.deletion_timestamp = "now"

        def patch_namespaced_pod(self, **_kwargs):
            raise AssertionError("successor finalizer cannot be patched")

    api = CoreApi()
    assert (
        await release_planned_pinned_pod_authority(
            api,
            namespace="agents-a",
            pod_name="warm-pod",
            expected_pod_uid="old-uid",
            expected_labels={"authority": "exact"},
            terminal_only=True,
        )
        is None
    )
    assert api.pod.metadata.uid == "successor-uid"
    assert api.pod.metadata.deletion_timestamp is None
    assert api.pod.metadata.finalizers == [PINNED_AUTHORITY_FINALIZER]
