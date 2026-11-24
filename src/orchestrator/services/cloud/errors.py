"""Error taxonomy for main-cloud backends.

Single exception class ``CloudBackendError`` + a ``CloudBackendErrorKind`` enum
discriminator. Callers pattern-match on ``.kind`` rather than walking a class
hierarchy — this is the Stripe pattern (see §4.7 of the design doc).

Phase 1 NOTE: the existing NextcloudBackend graceful-degradation paths log and
return ``None`` rather than raising, so these types are not thrown from the
Phase 1 adapter yet. They exist so callers and future adapters can start
wiring up error handling; Phase 1.5 / Phase 2 tighten the contract.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any, Optional


class CloudBackendErrorKind(StrEnum):
    NOT_FOUND = "not_found"
    ALREADY_EXISTS = "already_exists"
    PERMISSION_DENIED = "permission_denied"
    AUTHENTICATION_FAILED = "authentication_failed"
    QUOTA_EXCEEDED = "quota_exceeded"
    THROTTLED = "throttled"
    TIMEOUT = "timeout"
    UNAVAILABLE = "unavailable"
    INVALID_REQUEST = "invalid_request"
    NOT_SUPPORTED = "not_supported"
    UNKNOWN = "unknown"


class CloudBackendError(Exception):
    """Single exception raised by every main-cloud backend method."""

    def __init__(
        self,
        kind: CloudBackendErrorKind,
        message: str,
        *,
        backend: str,
        vendor_code: Optional[str] = None,
        vendor_message: Optional[str] = None,
        status_code: Optional[int] = None,
        request_id: Optional[str] = None,
        raw: Optional[dict[str, Any]] = None,
        retryable: bool = False,
    ) -> None:
        self.kind = kind
        self.backend = backend
        self.vendor_code = vendor_code
        self.vendor_message = vendor_message
        self.status_code = status_code
        self.request_id = request_id
        self.raw = raw
        self.retryable = retryable
        super().__init__(f"[{backend}:{kind.value}] {message}")


class FeatureNotAvailable(CloudBackendError):
    """Raised when a caller invokes a capability a backend does not support."""

    def __init__(self, feature: str, *, backend: str) -> None:
        super().__init__(
            CloudBackendErrorKind.NOT_SUPPORTED,
            f"backend {backend!r} does not support {feature!r}",
            backend=backend,
        )


class RetainedInstallationNotAttested(FeatureNotAvailable):
    """A retained main-cloud installation did not prove itself again.

    ``reason`` is the admin Main cloud page's word for why:
    ``unreachable`` (it did not initialize: no answer, or no Group Folders),
    ``no_installation_identity`` (it answered but named no installation) or
    ``other`` (it named a different installation). A ``FeatureNotAvailable``,
    so every caller that refuses the effect on one still does.
    """

    def __init__(self, reason: str, *, backend: str) -> None:
        super().__init__("retained main-cloud installation proof", backend=backend)
        self.reason = reason
