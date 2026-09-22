"""Bounded Kubernetes effects for pinned runtime authority.

``asyncio.to_thread`` cancellation does not stop the synchronous Kubernetes
client.  A caller that proceeds to retirement while that worker is still able
to commit a CREATE can manufacture false absence, and an unbounded evidence
read can indefinitely wedge that decision.  This helper gives every
authority-sensitive call a transport bound and joins the worker even when the
coroutine is cancelled.  Process restart remains covered by the durable 0185
intent/fence ledger.
"""

from __future__ import annotations

import asyncio
import os
import re
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any, TypeVar

PINNED_AUTHORITY_FINALIZER = "srw.io/pinned-authority-protection"
PINNED_AUTHORITY_PROTECTION_PROTOCOL = "finalizer_v1"
PINNED_WARM_PROTECTION_FENCE_ANNOTATION = "srw.io/warm-protection-fence"

K8S_MUTATION_CONNECT_TIMEOUT_SECONDS = max(
    1.0, float(os.getenv("PINNED_K8S_CONNECT_TIMEOUT_SECONDS", "5"))
)
K8S_MUTATION_READ_TIMEOUT_SECONDS = max(
    1.0, float(os.getenv("PINNED_K8S_MUTATION_TIMEOUT_SECONDS", "60"))
)
K8S_MUTATION_REQUEST_TIMEOUT = (
    K8S_MUTATION_CONNECT_TIMEOUT_SECONDS,
    K8S_MUTATION_READ_TIMEOUT_SECONDS,
)

_T = TypeVar("_T")


async def run_bounded_k8s_call(
    function: Callable[..., _T],
    /,
    *args: Any,
    request_timeout: Any | None = None,
    **kwargs: Any,
) -> _T:
    """Run one sync Kubernetes call and join it before propagating cancel.

    ``request_timeout`` is the public spelling of the generated client's
    ``_request_timeout``. Call sites must not name ``_``-prefixed kwargs --
    a client generation that rejects one raises ``ApiTypeError`` only in a
    deployed orchestrator, never against the fakes unit tests inject, so
    tests/test_k8s_patch_kwargs_contract.py bans them at the call site. This
    wrapper is the one place that spelling is allowed to exist.
    """

    if request_timeout is not None:
        kwargs["_request_timeout"] = request_timeout
    kwargs.setdefault("_request_timeout", K8S_MUTATION_REQUEST_TIMEOUT)
    worker = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
    cancelled = False
    while not worker.done():
        try:
            await asyncio.shield(worker)
        except asyncio.CancelledError:
            cancelled = True
    try:
        result = worker.result()
    except BaseException:
        # The mutation's result is more informative than a cancellation that
        # arrived while it was in flight; the durable reconciler sees the same
        # exception/unknown outcome as an uncancelled caller.
        raise
    if cancelled:
        raise asyncio.CancelledError
    return result


async def run_bounded_k8s_mutation(
    function: Callable[..., _T], /, *args: Any, **kwargs: Any
) -> _T:
    """Semantic mutation wrapper around the bounded, joined call runner."""

    return await run_bounded_k8s_call(function, *args, **kwargs)


def with_authority_finalizer(metadata: dict[str, Any]) -> dict[str, Any]:
    """Return manifest metadata carrying the protection finalizer from birth."""

    protected = dict(metadata)
    finalizers = [str(value) for value in protected.get("finalizers") or []]
    if PINNED_AUTHORITY_FINALIZER not in finalizers:
        finalizers.append(PINNED_AUTHORITY_FINALIZER)
    protected["finalizers"] = finalizers
    return protected


def finalizer_release_patch(
    *,
    uid: str,
    resource_version: str,
    finalizers: list[str],
    finalizer: str = PINNED_AUTHORITY_FINALIZER,
) -> list[dict[str, Any]] | None:
    """Build an exact JSON Patch that removes only one SRW finalizer."""

    if finalizer not in finalizers:
        return None
    retained = [value for value in finalizers if value != finalizer]
    return [
        {"op": "test", "path": "/metadata/uid", "value": uid},
        {
            "op": "test",
            "path": "/metadata/resourceVersion",
            "value": resource_version,
        },
        {"op": "test", "path": "/metadata/finalizers", "value": finalizers},
        {"op": "replace", "path": "/metadata/finalizers", "value": retained},
    ]


def pod_containers_are_terminal(pod: Any) -> bool:
    """Require terminated regular, init and ephemeral containers in one read.

    When the API supplies a spec, every declared container must have a terminal
    status. Missing status is not evidence that a container cannot run.
    """

    status = getattr(pod, "status", None)
    spec = getattr(pod, "spec", None)
    for spec_field, status_field in (
        ("containers", "container_statuses"),
        ("init_containers", "init_container_statuses"),
        ("ephemeral_containers", "ephemeral_container_statuses"),
    ):
        statuses = getattr(status, status_field, None) or []
        if spec_field == "containers" and not statuses:
            return False
        if not all(
            getattr(getattr(item, "state", None), "terminated", None) is not None
            for item in statuses
        ):
            return False
        declared = {str(item.name) for item in getattr(spec, spec_field, None) or []}
        observed = {str(getattr(item, "name", "")) for item in statuses}
        if not declared.issubset(observed):
            return False
    return True


async def retire_terminal_claimant_pod(
    core_api: Any,
    *,
    pod_name: str,
    pod_uid: str,
    namespace: str,
    expected_labels: dict[str, str],
    pvc_name: str,
    known_successor_uids: frozenset[str] = frozenset(),
) -> bool:
    """Retire one historically authorized, terminal PVC claimant.

    This is not process-zero recovery for an active actor. The caller must
    possess immutable settlement/handoff proof and current retirement authority.
    A UID/RV patch binds all finalizer preconditions to the same fresh Pod read.
    """

    async def read():
        return await run_bounded_k8s_call(
            core_api.read_namespaced_pod, name=pod_name, namespace=namespace
        )

    def exact(pod):
        metadata = getattr(pod, "metadata", None)
        labels = dict(getattr(metadata, "labels", None) or {})
        volumes = getattr(getattr(pod, "spec", None), "volumes", None) or []
        return (
            str(getattr(metadata, "uid", "")) == pod_uid
            and all(labels.get(key) == value for key, value in expected_labels.items())
            and any(
                getattr(
                    getattr(volume, "persistent_volume_claim", None), "claim_name", None
                )
                == pvc_name
                for volume in volumes
            )
            and pod_containers_are_terminal(pod)
        )

    try:
        pod = await read()
        observed_uid = str(getattr(getattr(pod, "metadata", None), "uid", ""))
        if observed_uid != pod_uid:
            # Same-name recycling may already have installed a durably
            # published successor. Never mutate it or accept an unknown UID.
            return observed_uid in known_successor_uids
        if not exact(pod):
            return False
        if not getattr(pod.metadata, "deletion_timestamp", None):
            await run_bounded_k8s_mutation(
                core_api.delete_namespaced_pod,
                name=pod_name,
                namespace=namespace,
                body={"preconditions": {"uid": pod_uid}},
            )
            pod = await read()
        if not exact(pod) or not getattr(pod.metadata, "deletion_timestamp", None):
            return False
        resource_version = str(getattr(pod.metadata, "resource_version", "") or "")
        if not resource_version:
            return False
        patch = finalizer_release_patch(
            uid=pod_uid,
            resource_version=resource_version,
            finalizers=list(getattr(pod.metadata, "finalizers", None) or []),
        )
        if patch is not None:
            await run_bounded_k8s_mutation(
                core_api.patch_namespaced_pod,
                name=pod_name,
                namespace=namespace,
                body=patch,
            )
        await read()
        return False  # Kubernetes has not yet confirmed physical removal.
    except Exception as exc:
        return getattr(exc, "status", None) == 404


def legacy_pinned_namespace_candidates(current_namespace: str) -> tuple[str, ...]:
    """Return the bounded, server-owned search space for 0185 adoption.

    A namespace move cannot be inferred from a deterministic object name.  The
    active namespace is always searched and operators may explicitly retain old
    namespaces in ``PINNED_LEGACY_AGENT_NAMESPACES`` during rollout.
    """

    candidates = [str(current_namespace or "").strip()]
    candidates.extend(
        value.strip()
        for value in os.getenv("PINNED_LEGACY_AGENT_NAMESPACES", "").split(",")
        if value.strip()
    )
    unique = tuple(dict.fromkeys(value for value in candidates if value))
    if len(unique) > 16:
        raise ValueError("PINNED_LEGACY_AGENT_NAMESPACES is limited to 16 entries")
    if any(
        len(value) > 63
        or re.fullmatch(r"[a-z0-9](?:[-a-z0-9]*[a-z0-9])?", value) is None
        for value in unique
    ):
        raise ValueError("PINNED_LEGACY_AGENT_NAMESPACES contains an invalid name")
    return unique


def finalizer_install_patch(
    *, uid: str, resource_version: str, finalizers: list[str]
) -> list[dict[str, Any]] | None:
    """Build a resourceVersion-fenced patch that installs SRW's finalizer."""

    if PINNED_AUTHORITY_FINALIZER in finalizers:
        return None
    protected = [*finalizers, PINNED_AUTHORITY_FINALIZER]
    patch: list[dict[str, Any]] = [
        {"op": "test", "path": "/metadata/uid", "value": uid},
        {
            "op": "test",
            "path": "/metadata/resourceVersion",
            "value": resource_version,
        },
    ]
    if finalizers:
        patch.append(
            {"op": "test", "path": "/metadata/finalizers", "value": finalizers}
        )
    # RFC 6902 ``add`` replaces an existing object member and also works when
    # Kubernetes omitted metadata.finalizers entirely.
    patch.append({"op": "add", "path": "/metadata/finalizers", "value": protected})
    return patch


def warm_protection_fence_value(*, protection_id: str, effect_token: str) -> str:
    """Return the exact server-owned value persisted with an abort receipt."""

    return f"{protection_id}:{effect_token}"


def _exact_object_metadata(
    value: Any,
    *,
    expected_uid: str | None,
    expected_labels: dict[str, str],
    allow_terminal_deleting: bool = False,
) -> dict[str, Any] | None:
    metadata = getattr(value, "metadata", None)
    uid = str(getattr(metadata, "uid", "") or "")
    resource_version = str(getattr(metadata, "resource_version", "") or "")
    labels = dict(getattr(metadata, "labels", None) or {})
    if not (
        uid
        and resource_version
        and (
            getattr(metadata, "deletion_timestamp", None) is None
            or (allow_terminal_deleting and pod_containers_are_terminal(value))
        )
        and (not expected_uid or uid == str(expected_uid))
        and all(
            labels.get(key) == expected for key, expected in expected_labels.items()
        )
        and labels.get("srw.io/provision-fence") != "true"
        and labels.get("srw.io/workspace-claim-fence") != "true"
    ):
        return None
    return {
        "uid": uid,
        "resource_version": resource_version,
        "finalizers": [
            str(item) for item in getattr(metadata, "finalizers", None) or []
        ],
        "annotations": dict(getattr(metadata, "annotations", None) or {}),
        "annotations_present": getattr(metadata, "annotations", None) is not None,
    }


async def _read_exact_object(
    read_function: Callable[..., Any],
    *,
    name: str,
    namespace: str,
    expected_uid: str | None,
    expected_labels: dict[str, str],
    allow_terminal_deleting: bool = False,
) -> tuple[Any, dict[str, Any]] | None:
    try:
        value = await run_bounded_k8s_call(
            read_function,
            name=name,
            namespace=namespace,
        )
    except Exception as exc:
        if getattr(exc, "status", None) == 404:
            return None
        raise
    evidence = _exact_object_metadata(
        value,
        expected_uid=expected_uid,
        expected_labels=expected_labels,
        allow_terminal_deleting=allow_terminal_deleting,
    )
    return (value, evidence) if evidence is not None else None


async def _protect_exact_object(
    read_function: Callable[..., Any],
    patch_function: Callable[..., Any],
    *,
    name: str,
    namespace: str,
    expected_uid: str,
    expected_labels: dict[str, str],
) -> dict[str, Any] | None:
    observed = await _read_exact_object(
        read_function,
        name=name,
        namespace=namespace,
        expected_uid=expected_uid,
        expected_labels=expected_labels,
    )
    if observed is None:
        return None
    _, evidence = observed
    patch = finalizer_install_patch(
        uid=evidence["uid"],
        resource_version=evidence["resource_version"],
        finalizers=evidence["finalizers"],
    )
    mutation_error: BaseException | None = None
    if patch is not None:
        try:
            await run_bounded_k8s_mutation(
                patch_function,
                name=name,
                namespace=namespace,
                body=patch,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # A transport failure can follow an accepted patch.  Re-read the
            # immutable UID before deciding whether the mutation failed.
            mutation_error = exc
    confirmed = await _read_exact_object(
        read_function,
        name=name,
        namespace=namespace,
        expected_uid=expected_uid,
        expected_labels=expected_labels,
    )
    if confirmed is not None:
        _, confirmed_evidence = confirmed
        if PINNED_AUTHORITY_FINALIZER in confirmed_evidence["finalizers"]:
            return confirmed_evidence
    if mutation_error is not None:
        raise mutation_error
    return None


async def discover_exact_pinned_pod_authority(
    core_api: Any,
    *,
    namespaces: tuple[str, ...],
    pod_name: str,
    expected_pod_uid: str,
    expected_labels: dict[str, str],
) -> dict[str, Any] | None:
    """Discover one unique exact live Pod without mutating it.

    Warm binding persists the returned namespace/UID/resourceVersion tuple
    before the corresponding finalizer patch.  Completing the whole bounded
    namespace scan first also prevents an ambiguous duplicate from acquiring
    an otherwise-unowned finalizer.
    """

    matches: list[tuple[str, dict[str, Any]]] = []
    for namespace in namespaces:
        observed = await _read_exact_object(
            core_api.read_namespaced_pod,
            name=pod_name,
            namespace=namespace,
            expected_uid=expected_pod_uid,
            expected_labels=expected_labels,
        )
        if observed is not None:
            _, evidence = observed
            matches.append((namespace, evidence))
    if len(matches) != 1:
        return None
    namespace, evidence = matches[0]
    return {
        "namespace": namespace,
        "pod_uid": evidence["uid"],
        "pod_resource_version": evidence["resource_version"],
        "finalizer_present": PINNED_AUTHORITY_FINALIZER in evidence["finalizers"],
    }


async def protect_planned_pinned_pod_authority(
    core_api: Any,
    *,
    namespace: str,
    pod_name: str,
    expected_pod_uid: str,
    expected_discovered_resource_version: str,
    protection_id: str,
    effect_token: str,
    expected_labels: dict[str, str],
    allow_current_resource_version: bool = False,
) -> dict[str, Any] | None:
    """Apply only the exact effect granted by a durable ``protecting`` row.

    Attach effects never chase a newer resourceVersion.  Consequently an
    expired-effect reconciler can win an annotation patch at the same version
    and make every delayed finalizer patch fail with 409.  Legacy reciprocal
    bindings already own their Pod and may safely rebase to its current exact
    UID/resourceVersion during rollout adoption.
    """

    if not all(
        str(value or "").strip()
        for value in (
            namespace,
            pod_name,
            expected_pod_uid,
            expected_discovered_resource_version,
            protection_id,
            effect_token,
        )
    ):
        return None
    observed = await _read_exact_object(
        core_api.read_namespaced_pod,
        name=pod_name,
        namespace=namespace,
        expected_uid=expected_pod_uid,
        expected_labels=expected_labels,
    )
    if observed is None:
        return None
    _, evidence = observed
    if PINNED_AUTHORITY_FINALIZER not in evidence["finalizers"]:
        if not allow_current_resource_version and evidence["resource_version"] != str(
            expected_discovered_resource_version
        ):
            return None
        patch = finalizer_install_patch(
            uid=evidence["uid"],
            resource_version=(
                evidence["resource_version"]
                if allow_current_resource_version
                else str(expected_discovered_resource_version)
            ),
            finalizers=evidence["finalizers"],
        )
        mutation_error: BaseException | None = None
        try:
            if patch is not None:
                await run_bounded_k8s_mutation(
                    core_api.patch_namespaced_pod,
                    name=pod_name,
                    namespace=namespace,
                    body=patch,
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            mutation_error = exc
        confirmed = await _read_exact_object(
            core_api.read_namespaced_pod,
            name=pod_name,
            namespace=namespace,
            expected_uid=expected_pod_uid,
            expected_labels=expected_labels,
        )
        if confirmed is None:
            if (
                mutation_error is not None
                and getattr(mutation_error, "status", None) != 409
            ):
                raise mutation_error
            return None
        _, evidence = confirmed
        if PINNED_AUTHORITY_FINALIZER not in evidence["finalizers"]:
            if (
                mutation_error is not None
                and getattr(mutation_error, "status", None) != 409
            ):
                raise mutation_error
            return None
    return {
        "namespace": namespace,
        "pod_uid": evidence["uid"],
        "pod_resource_version": evidence["resource_version"],
        "protection_finalizer": PINNED_AUTHORITY_FINALIZER,
        "evidence_protocol": "exact_live_finalizer_v1",
        "observed_at": datetime.now(timezone.utc),
    }


async def fence_unmodified_planned_pod_authority(
    core_api: Any,
    *,
    namespace: str,
    pod_name: str,
    expected_pod_uid: str,
    protection_id: str,
    effect_token: str,
    expected_labels: dict[str, str],
) -> dict[str, Any]:
    """Linearize an expired attach effect against its delayed finalizer patch.

    The successful annotation is re-read and returned as durable DB evidence.
    Both mutations test the same UID/resourceVersion, so either the finalizer
    wins and is adopted or this fence wins and the old patch can never commit.
    """

    fence_value = warm_protection_fence_value(
        protection_id=protection_id,
        effect_token=effect_token,
    )
    observed = await _read_exact_object(
        core_api.read_namespaced_pod,
        name=pod_name,
        namespace=namespace,
        expected_uid=expected_pod_uid,
        expected_labels=expected_labels,
    )
    if observed is None:
        classification = await observe_planned_pinned_pod_authority(
            core_api,
            namespace=namespace,
            pod_name=pod_name,
            expected_pod_uid=expected_pod_uid,
            expected_labels=expected_labels,
        )
        state = str(classification.get("state") or "")
        if state in {"exact_absent", "replacement"}:
            return {"state": state}
        return {"state": "unknown"}
    _, evidence = observed
    if PINNED_AUTHORITY_FINALIZER in evidence["finalizers"]:
        return {
            "state": "finalizer_won",
            "pod_resource_version": evidence["resource_version"],
        }
    annotations = evidence["annotations"]
    if annotations.get(PINNED_WARM_PROTECTION_FENCE_ANNOTATION) == fence_value:
        return {
            "state": "fence_won",
            "abort_fence_protocol": "exact_rv_annotation_fence_v1",
            "abort_fence_resource_version": evidence["resource_version"],
            "abort_fence_value": fence_value,
        }

    patch: list[dict[str, Any]] = [
        {"op": "test", "path": "/metadata/uid", "value": evidence["uid"]},
        {
            "op": "test",
            "path": "/metadata/resourceVersion",
            "value": evidence["resource_version"],
        },
    ]
    pointer = "/metadata/annotations/srw.io~1warm-protection-fence"
    old_value = annotations.get(PINNED_WARM_PROTECTION_FENCE_ANNOTATION)
    if not evidence["annotations_present"]:
        patch.append(
            {
                "op": "add",
                "path": "/metadata/annotations",
                "value": {PINNED_WARM_PROTECTION_FENCE_ANNOTATION: fence_value},
            }
        )
    elif old_value is None:
        patch.append({"op": "add", "path": pointer, "value": fence_value})
    else:
        patch.extend(
            (
                {"op": "test", "path": pointer, "value": old_value},
                {"op": "replace", "path": pointer, "value": fence_value},
            )
        )
    mutation_error: BaseException | None = None
    try:
        await run_bounded_k8s_mutation(
            core_api.patch_namespaced_pod,
            name=pod_name,
            namespace=namespace,
            body=patch,
        )
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        mutation_error = exc

    confirmed = await _read_exact_object(
        core_api.read_namespaced_pod,
        name=pod_name,
        namespace=namespace,
        expected_uid=expected_pod_uid,
        expected_labels=expected_labels,
    )
    if confirmed is None:
        classification = await observe_planned_pinned_pod_authority(
            core_api,
            namespace=namespace,
            pod_name=pod_name,
            expected_pod_uid=expected_pod_uid,
            expected_labels=expected_labels,
        )
        state = str(classification.get("state") or "")
        if state in {"exact_absent", "replacement"}:
            return {"state": state}
        if (
            mutation_error is not None
            and getattr(mutation_error, "status", None) != 409
        ):
            raise mutation_error
        return {"state": "unknown"}
    _, confirmed_evidence = confirmed
    if PINNED_AUTHORITY_FINALIZER in confirmed_evidence["finalizers"]:
        return {
            "state": "finalizer_won",
            "pod_resource_version": confirmed_evidence["resource_version"],
        }
    if (
        confirmed_evidence["annotations"].get(PINNED_WARM_PROTECTION_FENCE_ANNOTATION)
        == fence_value
    ):
        return {
            "state": "fence_won",
            "abort_fence_protocol": "exact_rv_annotation_fence_v1",
            "abort_fence_resource_version": confirmed_evidence["resource_version"],
            "abort_fence_value": fence_value,
        }
    if mutation_error is not None and getattr(mutation_error, "status", None) != 409:
        raise mutation_error
    return {"state": "unknown"}


async def observe_planned_pinned_pod_authority(
    core_api: Any,
    *,
    namespace: str,
    pod_name: str,
    expected_pod_uid: str,
    expected_labels: dict[str, str],
) -> dict[str, Any]:
    """Classify one exact planned Pod and whether SRW still protects it."""

    try:
        value = await run_bounded_k8s_call(
            core_api.read_namespaced_pod,
            name=pod_name,
            namespace=namespace,
        )
    except Exception as exc:
        if getattr(exc, "status", None) == 404:
            return {"state": "exact_absent", "finalizer_present": False}
        return {"state": "unknown", "finalizer_present": None}
    metadata = getattr(value, "metadata", None)
    uid = str(getattr(metadata, "uid", "") or "")
    labels = dict(getattr(metadata, "labels", None) or {})
    if uid != str(expected_pod_uid):
        return {"state": "replacement", "finalizer_present": False}
    if not all(labels.get(key) == value for key, value in expected_labels.items()):
        return {"state": "unknown", "finalizer_present": None}
    resource_version = str(getattr(metadata, "resource_version", "") or "")
    if not resource_version:
        return {"state": "unknown", "finalizer_present": None}
    finalizers = [str(item) for item in getattr(metadata, "finalizers", None) or []]
    annotations = dict(getattr(metadata, "annotations", None) or {})
    if getattr(metadata, "deletion_timestamp", None) is not None:
        statuses = (
            getattr(getattr(value, "status", None), "container_statuses", None) or []
        )
        terminal = bool(statuses) and all(
            getattr(getattr(status, "state", None), "terminated", None) is not None
            for status in statuses
        )
        return {
            "state": "exact_terminal" if terminal else "exact_terminating",
            "finalizer_present": PINNED_AUTHORITY_FINALIZER in finalizers,
            "pod_resource_version": resource_version,
            "annotations": annotations,
        }
    return {
        "state": "exact_live",
        "finalizer_present": PINNED_AUTHORITY_FINALIZER in finalizers,
        "pod_resource_version": resource_version,
        "annotations": annotations,
    }


async def release_planned_pinned_pod_authority(
    core_api: Any,
    *,
    namespace: str,
    pod_name: str,
    expected_pod_uid: str,
    expected_labels: dict[str, str],
) -> dict[str, Any] | None:
    """Remove SRW's finalizer from one exact released warm-binding Pod.

    Route publication may turn the once-live warm Pod into a terminal deleting
    object before the durable release row is reconciled.  Terminal container
    evidence plus the captured UID/labels is sufficient to remove only our
    finalizer; physical absence is still required before that path settles.
    """

    observed = await observe_planned_pinned_pod_authority(
        core_api,
        namespace=namespace,
        pod_name=pod_name,
        expected_pod_uid=expected_pod_uid,
        expected_labels=expected_labels,
    )
    state = str(observed.get("state") or "")
    if state == "exact_absent":
        return {"outcome": "exact_absent_v1", "agent_present": False}
    if state == "replacement":
        return {"outcome": "exact_replacement_v1", "agent_present": False}
    if state not in {"exact_live", "exact_terminal"}:
        return None
    if state == "exact_live" and not observed.get("finalizer_present"):
        return {"outcome": "exact_live_unprotected_v1", "agent_present": True}
    if not observed.get("finalizer_present"):
        # A terminal deleting object without our finalizer is on its way to
        # physical absence. Do not return its dead actor to the warm pool.
        return None

    exact = await _read_exact_object(
        core_api.read_namespaced_pod,
        name=pod_name,
        namespace=namespace,
        expected_uid=expected_pod_uid,
        expected_labels=expected_labels,
        allow_terminal_deleting=state == "exact_terminal",
    )
    if exact is None:
        return None
    _, evidence = exact
    patch = finalizer_release_patch(
        uid=evidence["uid"],
        resource_version=evidence["resource_version"],
        finalizers=evidence["finalizers"],
    )
    mutation_error: BaseException | None = None
    if patch is not None:
        try:
            await run_bounded_k8s_mutation(
                core_api.patch_namespaced_pod,
                name=pod_name,
                namespace=namespace,
                body=patch,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            mutation_error = exc
    confirmed = await observe_planned_pinned_pod_authority(
        core_api,
        namespace=namespace,
        pod_name=pod_name,
        expected_pod_uid=expected_pod_uid,
        expected_labels=expected_labels,
    )
    confirmed_state = str(confirmed.get("state") or "")
    if confirmed_state == "exact_absent":
        return {"outcome": "exact_absent_v1", "agent_present": False}
    if confirmed_state == "replacement":
        return {"outcome": "exact_replacement_v1", "agent_present": False}
    if confirmed_state == "exact_live" and not confirmed.get("finalizer_present"):
        return {"outcome": "exact_live_unprotected_v1", "agent_present": True}
    if mutation_error is not None:
        raise mutation_error
    return None


async def protect_legacy_pinned_agent_authority(
    core_api: Any,
    *,
    namespaces: tuple[str, ...],
    pod_name: str,
    expected_pod_uid: str | None,
    pod_labels: dict[str, str],
    pvc_name: str | None = None,
    expected_pvc_uid: str | None = None,
    pvc_labels: dict[str, str] | None = None,
) -> dict[str, Any] | None:
    """Protect one unique exact legacy Pod/PVC tuple and return its evidence.

    Discovery is completed across the whole bounded namespace set before any
    mutation.  This prevents an ambiguous duplicate from gaining a finalizer
    that no database authority can subsequently own.
    """

    matches: list[tuple[str, dict[str, Any]]] = []
    for namespace in namespaces:
        observed = await _read_exact_object(
            core_api.read_namespaced_pod,
            name=pod_name,
            namespace=namespace,
            expected_uid=expected_pod_uid,
            expected_labels=pod_labels,
        )
        if observed is not None:
            _, evidence = observed
            matches.append((namespace, evidence))
    if len(matches) != 1:
        return None
    namespace, pod_discovery = matches[0]

    pvc_discovery: dict[str, Any] | None = None
    if pvc_name is not None:
        if not pvc_labels:
            return None
        observed_pvc = await _read_exact_object(
            core_api.read_namespaced_persistent_volume_claim,
            name=pvc_name,
            namespace=namespace,
            expected_uid=expected_pvc_uid,
            expected_labels=pvc_labels,
        )
        if observed_pvc is None:
            return None
        _, pvc_discovery = observed_pvc

    # Protect the process-bearing object first. A crash between the two
    # mutations therefore leaves the exact Pod observable for autonomous
    # retry; it can never leave only a retained PVC while an unprotected node
    # process disappears behind a misleading Pod 404.
    protected_pod = await _protect_exact_object(
        core_api.read_namespaced_pod,
        core_api.patch_namespaced_pod,
        name=pod_name,
        namespace=namespace,
        expected_uid=pod_discovery["uid"],
        expected_labels=pod_labels,
    )
    if protected_pod is None:
        return None
    protected_pvc: dict[str, Any] | None = None
    if pvc_name is not None and pvc_discovery is not None:
        protected_pvc = await _protect_exact_object(
            core_api.read_namespaced_persistent_volume_claim,
            core_api.patch_namespaced_persistent_volume_claim,
            name=pvc_name,
            namespace=namespace,
            expected_uid=pvc_discovery["uid"],
            expected_labels=pvc_labels or {},
        )
        if protected_pvc is None:
            return None

    return {
        "namespace": namespace,
        "pod_uid": protected_pod["uid"],
        "pod_resource_version": protected_pod["resource_version"],
        "pvc_uid": protected_pvc["uid"] if protected_pvc else None,
        "pvc_resource_version": (
            protected_pvc["resource_version"] if protected_pvc else None
        ),
        "protection_finalizer": PINNED_AUTHORITY_FINALIZER,
        "evidence_protocol": "exact_live_finalizer_v1",
        "observed_at": datetime.now(timezone.utc),
    }
