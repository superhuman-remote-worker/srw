"""Exact Kubernetes quantity normalization for infrastructure metering.

Kubernetes API quantities are decimal strings with SI or binary-SI suffixes.
The metering pipeline stores scheduler capacity as integer millicores or bytes;
keeping that boundary integral avoids binary-float drift during later time
integration.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import (
    Decimal,
    DecimalException,
    ROUND_CEILING,
    ROUND_HALF_EVEN,
    localcontext,
)
from typing import Any, Literal

from kubernetes.utils.quantity import parse_quantity


DECIMAL_PRECISION = 50
SIGNED_BIGINT_MAX = 2**63 - 1
_MAX_QUANTITY_TEXT_LENGTH = 128


class QuantityNormalizationError(ValueError):
    """A stable, request-field-safe quantity validation failure."""

    def __init__(
        self,
        code: str,
        *,
        resource: str,
        original_value: str | None,
    ) -> None:
        super().__init__(code)
        self.code = code
        self.resource = resource
        self.original_value = original_value


@dataclass(frozen=True, slots=True)
class ParsedKubernetesQuantity:
    """One parsed quantity before conversion to a storage unit."""

    original: str
    value: Decimal


@dataclass(frozen=True, slots=True)
class NormalizedQuantity:
    """One exact quantity normalized to an integer metering unit."""

    original: str
    decimal_value: Decimal
    normalized_value: int
    normalized_unit: Literal["millicore", "byte"]

    def to_dict(self) -> dict[str, str | int]:
        return {
            "original": self.original,
            "decimal_value": str(self.decimal_value),
            "normalized_value": self.normalized_value,
            "normalized_unit": self.normalized_unit,
        }


def _original_text(value: Any) -> str | None:
    if isinstance(value, bool) or isinstance(value, float):
        return None
    if isinstance(value, (str, int, Decimal)):
        text = str(value)
        if len(text) <= _MAX_QUANTITY_TEXT_LENGTH:
            return text
    return None


def parse_kubernetes_quantity(
    value: Any,
    *,
    resource: str,
) -> ParsedKubernetesQuantity:
    """Parse a Kubernetes quantity with a finite, non-negative contract.

    Floats are rejected even though the upstream Python helper accepts them:
    converting a binary float to ``Decimal`` would make metering precision
    depend on an already-rounded representation. Kubernetes JSON uses strings
    for ``Quantity`` fields, while integers and ``Decimal`` remain convenient
    for fixtures and dynamic-client adapters.
    """

    original = _original_text(value)
    if original is None:
        raise QuantityNormalizationError(
            "invalid-quantity-type",
            resource=resource,
            original_value=None,
        )
    if not original or original != original.strip():
        raise QuantityNormalizationError(
            "invalid-quantity-format",
            resource=resource,
            original_value=original,
        )

    try:
        with localcontext() as context:
            context.prec = DECIMAL_PRECISION
            context.rounding = ROUND_HALF_EVEN
            parsed = parse_quantity(value)
            parsed = +Decimal(parsed)
    except (DecimalException, TypeError, ValueError, OverflowError) as exc:
        raise QuantityNormalizationError(
            "invalid-quantity-format",
            resource=resource,
            original_value=original,
        ) from exc

    if not parsed.is_finite():
        raise QuantityNormalizationError(
            "non-finite-quantity",
            resource=resource,
            original_value=original,
        )
    if parsed < 0:
        raise QuantityNormalizationError(
            "negative-quantity",
            resource=resource,
            original_value=original,
        )
    return ParsedKubernetesQuantity(original=original, value=parsed)


def _normalize(
    value: Any,
    *,
    resource: str,
    scale: int,
    unit: Literal["millicore", "byte"],
) -> NormalizedQuantity:
    parsed = parse_kubernetes_quantity(value, resource=resource)
    try:
        with localcontext() as context:
            context.prec = DECIMAL_PRECISION
            context.rounding = ROUND_HALF_EVEN
            scaled = parsed.value * Decimal(scale)
            normalized = int(scaled.to_integral_value(rounding=ROUND_CEILING))
    except (DecimalException, OverflowError, ValueError) as exc:
        raise QuantityNormalizationError(
            "quantity-overflow",
            resource=resource,
            original_value=parsed.original,
        ) from exc

    if normalized > SIGNED_BIGINT_MAX:
        raise QuantityNormalizationError(
            "quantity-overflow",
            resource=resource,
            original_value=parsed.original,
        )
    return NormalizedQuantity(
        original=parsed.original,
        decimal_value=parsed.value,
        normalized_value=normalized,
        normalized_unit=unit,
    )


def normalize_cpu_millicores(value: Any) -> NormalizedQuantity:
    """Normalize a CPU quantity upward to integer millicores."""

    return _normalize(value, resource="cpu", scale=1000, unit="millicore")


def normalize_byte_quantity(
    value: Any,
    *,
    resource: str = "memory",
) -> NormalizedQuantity:
    """Normalize a memory or storage quantity upward to integer bytes."""

    return _normalize(value, resource=resource, scale=1, unit="byte")


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
