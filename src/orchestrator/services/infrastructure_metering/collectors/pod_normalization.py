"""Compatibility exports for the shared Kubernetes resource normalizer."""

from shared.kubernetes_pod_requests import (
    ALLOWED_POD_LABELS,
    POD_REQUESTS_ALGORITHM,
    NormalizedPod,
    PodDiagnostic,
    PodEffectiveRequest,
    PodLifecycle,
    PodLifecycleState,
    PodNormalizationError,
    PodOwnerReference,
    classify_pod_lifecycle,
    normalize_pod,
)

__all__ = [
    "ALLOWED_POD_LABELS",
    "POD_REQUESTS_ALGORITHM",
    "NormalizedPod",
    "PodDiagnostic",
    "PodEffectiveRequest",
    "PodLifecycle",
    "PodLifecycleState",
    "PodNormalizationError",
    "PodOwnerReference",
    "classify_pod_lifecycle",
    "normalize_pod",
]
