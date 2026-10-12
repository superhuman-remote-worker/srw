"""Compatibility exports for the shared Kubernetes resource normalizer."""

from shared.kubernetes_quantities import (
    DECIMAL_PRECISION,
    SIGNED_BIGINT_MAX,
    NormalizedQuantity,
    ParsedKubernetesQuantity,
    QuantityNormalizationError,
    normalize_byte_quantity,
    normalize_cpu_millicores,
    parse_kubernetes_quantity,
)

__all__ = [
    "DECIMAL_PRECISION",
    "SIGNED_BIGINT_MAX",
    "NormalizedQuantity",
    "ParsedKubernetesQuantity",
    "QuantityNormalizationError",
    "normalize_byte_quantity",
    "normalize_cpu_millicores",
    "parse_kubernetes_quantity",
]
