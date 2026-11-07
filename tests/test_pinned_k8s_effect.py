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
    release_pvc_finalizer_exact,
    retire_terminal_claimant_pod,
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


class _Status(Exception):
    def __init__(self, status):
        super().__init__(f"Kubernetes status {status}")
        self.status = status


class _PvcApi:
    """One PVC whose JSON Patch tests behave like the API server's."""

    PVC_PROTECTION = "kubernetes.io/pvc-protection"

    def __init__(self, *, before_patch=None, refusal=409):
        self.claim = SimpleNamespace(
            metadata=SimpleNamespace(
                uid="pvc-u1",
                resource_version="7",
                finalizers=[PINNED_AUTHORITY_FINALIZER, self.PVC_PROTECTION],
            )
        )
        self.before_patch = before_patch
        self.refusal = refusal
        self.patches = 0

    def read_namespaced_persistent_volume_claim(
        self, *, name, namespace, _request_timeout=None
    ):
        del name, namespace, _request_timeout
        if self.claim is None:
            raise _Status(404)
        return self.claim

    def patch_namespaced_persistent_volume_claim(
        self, *, name, namespace, body, _request_timeout=None
    ):
        del name, namespace, _request_timeout
        self.patches += 1
        if self.before_patch is not None:
            self.before_patch(self)
        if self.claim is None:
            raise _Status(404)
        tests = {op["path"]: op["value"] for op in body if op["op"] == "test"}
        metadata = self.claim.metadata
        if (
            tests["/metadata/uid"] != metadata.uid
            or tests["/metadata/resourceVersion"] != metadata.resource_version
            or tests["/metadata/finalizers"] != metadata.finalizers
        ):
            raise _Status(self.refusal)
        metadata.finalizers = list(body[-1]["value"])
        metadata.resource_version = str(int(metadata.resource_version) + 1)


async def _release(api, **kwargs):
    return await release_pvc_finalizer_exact(
        api,
        pvc_name="pvc-agent-s-1",
        expected_pvc_uid=kwargs.pop("expected_pvc_uid", "pvc-u1"),
        namespace="agents-a",
        **kwargs,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("refusal", [409, 422])
async def test_pvc_finalizer_release_rereads_after_the_protection_controller_races(
    refusal,
):
    """Kubernetes drops pvc-protection right after a delete (dev 2026-11-05)."""

    def protection_controller_runs_once(api):
        api.before_patch = None
        api.claim.metadata.finalizers = [PINNED_AUTHORITY_FINALIZER]
        api.claim.metadata.resource_version = "8"

    api = _PvcApi(before_patch=protection_controller_runs_once, refusal=refusal)
    assert await _release(api)
    assert api.patches == 2
    assert api.claim.metadata.finalizers == []


@pytest.mark.asyncio
async def test_pvc_finalizer_release_gives_up_after_bounded_stale_refusals():
    def always_changes(api):
        api.claim.metadata.resource_version = str(
            int(api.claim.metadata.resource_version) + 1
        )

    api = _PvcApi(before_patch=always_changes)
    assert not await _release(api, attempts=3)
    assert api.patches == 3
    assert PINNED_AUTHORITY_FINALIZER in api.claim.metadata.finalizers


@pytest.mark.asyncio
async def test_pvc_finalizer_release_does_not_retry_other_refusals():
    def forbidden(_api):
        raise _Status(403)

    api = _PvcApi(before_patch=forbidden)
    assert not await _release(api)
    assert api.patches == 1


@pytest.mark.asyncio
async def test_pvc_finalizer_release_never_touches_a_same_name_replacement():
    def replaced(api):
        api.before_patch = None
        api.claim = SimpleNamespace(
            metadata=SimpleNamespace(
                uid="pvc-u2",
                resource_version="1",
                finalizers=[PINNED_AUTHORITY_FINALIZER],
            )
        )

    api = _PvcApi(before_patch=replaced)
    assert not await _release(api)
    assert api.patches == 1
    assert api.claim.metadata.finalizers == [PINNED_AUTHORITY_FINALIZER]


@pytest.mark.asyncio
async def test_pvc_finalizer_release_counts_a_vanished_claim_as_released():
    def collected(api):
        api.claim = None

    api = _PvcApi(before_patch=collected)
    assert await _release(api)
    api = _PvcApi()
    api.claim = None
    assert await _release(api)


@pytest.mark.asyncio
async def test_pvc_finalizer_release_is_idempotent_once_released():
    api = _PvcApi()
    api.claim.metadata.finalizers = [_PvcApi.PVC_PROTECTION]
    assert await _release(api)
    assert api.patches == 0


class _TerminalPodApi:
    def __init__(self, *, volumes, restart_policy="Never"):
        self.pod = SimpleNamespace(
            metadata=SimpleNamespace(
                uid="pod-u1",
                resource_version="3",
                labels={"srw.io/provision-attempt": "a1"},
                finalizers=[PINNED_AUTHORITY_FINALIZER],
                deletion_timestamp="2026-11-05T19:00:00Z",
            ),
            spec=SimpleNamespace(
                containers=[SimpleNamespace(name="agent")],
                init_containers=[],
                ephemeral_containers=[],
                volumes=volumes,
                restart_policy=restart_policy,
            ),
            status=SimpleNamespace(
                container_statuses=[
                    SimpleNamespace(
                        name="agent",
                        state=SimpleNamespace(terminated=SimpleNamespace()),
                    )
                ]
            ),
        )
        self.released = False

    def read_namespaced_pod(self, *, name, namespace, _request_timeout=None):
        del name, namespace, _request_timeout
        if self.released:
            raise _Status(404)
        return self.pod

    def patch_namespaced_pod(self, *, name, namespace, body, _request_timeout=None):
        del name, namespace, _request_timeout
        assert body[-1]["value"] == []
        self.released = True


def _pvc(name):
    return SimpleNamespace(persistent_volume_claim=SimpleNamespace(claim_name=name))


_SCRATCH = SimpleNamespace(persistent_volume_claim=None)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("pvc_name", "volumes", "restart_policy", "released"),
    [
        # A claim-bearing pre-setup abort mounts only its own claim.
        ("pvc-own", [_pvc("pvc-own"), _SCRATCH], "Never", True),
        ("pvc-own", [_pvc("pvc-own"), _pvc("pvc-other")], "Never", False),
        ("pvc-own", [_pvc("pvc-own")], "Always", False),
        ("pvc-own", [_pvc("pvc-other")], "Never", False),
        # The claim-less case keeps refusing every PVC.
        (None, [_SCRATCH], "Never", True),
        (None, [_pvc("pvc-own")], "Never", False),
        (None, [_SCRATCH], "OnFailure", False),
    ],
)
async def test_nonrestartable_retirement_allows_only_the_pods_own_claim(
    pvc_name, volumes, restart_policy, released
):
    api = _TerminalPodApi(volumes=volumes, restart_policy=restart_policy)
    await retire_terminal_claimant_pod(
        api,
        pod_name="srw-agent-s-1",
        pod_uid="pod-u1",
        namespace="agents-a",
        expected_labels={"srw.io/provision-attempt": "a1"},
        pvc_name=pvc_name,
        require_nonrestartable=True,
    )
    assert api.released is released
