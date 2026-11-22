"""The certificate check before an admin's endpoint Test, model-row Test or Discover.

For an ``https://`` endpoint, :func:`preflight` reads the certificate the
server presents (:func:`shared.tls_pinning.inspect_tls`: a handshake, no
request) before the HTTP probe. When the certificate fails normal verification
on an endpoint without a pin, or does not match the endpoint's pin, the probe
is skipped: no request, and so no API key, reaches that server. With a pin the
probe then runs through it (:func:`probe_pin`).

:func:`tls_block` is the ``tls`` object both Test responses carry, with the
keys of the spec's Test JSON (vault ``endpoint-certificate-pinning-design``,
"API"). :func:`endpoint_trust_refusal` says why an endpoint can't be pinned:
the Trust API answers 400 with the same sentence, and Test offers no Trust.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlsplit

from orchestrator.services.provider_catalog import (
    _endpoint_is_subscription_proxy,
    _service_adapter,
)
from shared.tls_pinning import (
    CertificateChangedError,
    TlsInspection,
    TlsPin,
    inspect_tls,
    pinned_ssl_context,
)

HTTP_REFUSAL = "Pinning applies to https:// endpoints."
SUBSCRIPTION_PROXY_REFUSAL = "The subscription proxy can't be pinned."
ADAPTER_REFUSAL = (
    "{label} is served by the {names} adapter, which doesn't use a pinned certificate."
)
LIVE_VOICE_REFUSAL = (
    "Live voice does not support pinned certificates yet; give the endpoint a "
    "certificate that verifies."
)
DISCOVER_UNTRUSTED = (
    "The endpoint's certificate is not trusted. Test the endpoint to review it."
)
UNREADABLE_PIN = (
    "The pinned certificate stored for this endpoint can't be read ({error}). "
    "Trust the server's certificate again or remove the pin."
)

#: Stands in for a stored pin that can't be read. It matches no certificate,
#: and a pinned context built from it raises: a broken pin fails closed and is
#: never mistaken for "no pin".
UNUSABLE_PIN = TlsPin(b"")


def inspects(endpoint: Mapping[str, Any]) -> bool:
    """Whether Test reads this endpoint's certificate: an ``https://`` URL."""
    return urlsplit(str(endpoint.get("base_url") or "")).scheme == "https"


def _read_pin(endpoint: Mapping[str, Any]) -> tuple[TlsPin | None, str | None]:
    """The stored pin and, when it can't be used, why (with UNUSABLE_PIN)."""
    pem = endpoint.get("tls_pin_cert")
    sha256 = endpoint.get("tls_pin_sha256")
    if not pem and not sha256:
        return None, None
    try:
        if not pem:
            raise ValueError("no certificate stored with the fingerprint")
        pin = TlsPin.from_pem(pem)
        if sha256 and sha256 != pin.sha256:
            raise ValueError(f"the stored fingerprint is {sha256}")
        pinned_ssl_context(pin)  # OpenSSL must accept it as a trust anchor
    except Exception as exc:
        return UNUSABLE_PIN, UNREADABLE_PIN.format(error=exc)
    return pin, None


def stored_pin(endpoint: Mapping[str, Any]) -> TlsPin | None:
    """The endpoint's pin from ``tls_pin_cert``; ``None`` when it has none.

    A stored pin that can't be read gives :data:`UNUSABLE_PIN`, never ``None``.
    """
    return _read_pin(endpoint)[0]


def probe_pin(endpoint: Mapping[str, Any]) -> TlsPin | None:
    """The pin the HTTP probe uses: the stored one, for an ``https://`` URL only."""
    return stored_pin(endpoint) if inspects(endpoint) else None


def _as_datetime(value: Any) -> datetime | None:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value)
        except ValueError:
            return None
    if not isinstance(value, datetime):
        return None
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def _iso(value: Any) -> str | None:
    moment = _as_datetime(value)
    return moment.isoformat() if moment else None


def pin_public(endpoint: Mapping[str, Any]) -> dict[str, Any] | None:
    """The stored pin as the API shows it; ``None`` when there is none."""
    if not endpoint.get("tls_pin_sha256") and not endpoint.get("tls_pin_cert"):
        return None
    not_after = _as_datetime(endpoint.get("tls_pin_not_after"))
    return {
        "sha256": endpoint.get("tls_pin_sha256"),
        "subject": endpoint.get("tls_pin_subject"),
        "issuer": endpoint.get("tls_pin_issuer"),
        "not_after": not_after.isoformat() if not_after else None,
        "expired": bool(not_after and not_after < datetime.now(timezone.utc)),
        "trusted_at": _iso(endpoint.get("tls_pinned_at")),
    }


def _unpinned_adapter(row: Mapping[str, Any]) -> str | None:
    """The search/fetch/TTS adapter serving a row, which ignores a pin.

    Image generation names an adapter too, but its client honours the pin.
    """
    capabilities = row.get("capabilities")
    if isinstance(capabilities, (list, tuple)) and "image_generation" in capabilities:
        return None
    return _service_adapter(row)


def endpoint_trust_refusal(
    endpoint: Mapping[str, Any], rows: list[Mapping[str, Any]]
) -> str | None:
    """Why this endpoint's certificate can't be pinned, or ``None`` when it can.

    ``rows`` are the endpoint's catalog rows. The sentence is the Trust API's
    400 detail, verbatim.
    """
    if _endpoint_is_subscription_proxy(endpoint):
        return SUBSCRIPTION_PROXY_REFUSAL
    if not inspects(endpoint):
        return HTTP_REFUSAL
    adapters = [_unpinned_adapter(row) for row in rows]
    if adapters and all(adapters):
        names = ", ".join(sorted({a for a in adapters if a}))
        return ADAPTER_REFUSAL.format(label=endpoint.get("label"), names=names)
    for row in rows:
        capabilities = row.get("capabilities")
        if isinstance(capabilities, (list, tuple)) and "live_voice" in capabilities:
            return LIVE_VOICE_REFUSAL
    return None


def _host_port(base_url: str) -> tuple[str, int]:
    parts = urlsplit(base_url)
    return parts.hostname or "", parts.port or 443


def tls_block(
    inspection: TlsInspection, endpoint: Mapping[str, Any], *, trustable: bool
) -> dict[str, Any] | None:
    """The Test response's ``tls`` object; ``None`` when there is nothing to show.

    Nothing to show: no pin and a certificate that verifies normally. With a
    pin, ``verified`` means the presented certificate is the pinned one; a
    different one is ``certificate_changed`` even when it verifies normally
    (owner decision 1). ``can_trust`` drives the Trust button.
    """
    pin, problem = _read_pin(endpoint)
    if pin is None and inspection.verified:
        return None
    certificate = inspection.certificate
    reason, message = inspection.reason, inspection.message
    pin_matches: bool | None = None
    if pin is not None:
        reason = message = None
        if certificate is not None:
            pin_matches = problem is None and certificate.sha256 == pin.sha256
        if problem is not None:
            reason, message = "other", problem
        elif pin_matches is False:
            host, port = _host_port(str(endpoint["base_url"]))
            reason = "certificate_changed"
            message = str(
                CertificateChangedError(host, port, pin.sha256, certificate.sha256)
            )
    return {
        "verified": pin_matches is True if pin is not None else inspection.verified,
        "reason": reason,
        "message": message,
        "certificate": certificate.to_public() if certificate else None,
        "pin": pin_public(endpoint),
        "pin_matches": pin_matches,
        "publicly_valid": inspection.verified,
        "can_trust": (
            trustable
            and certificate is not None
            and not inspection.verified
            and pin_matches is not True
        ),
    }


async def preflight(
    endpoint: Mapping[str, Any], *, trustable: bool
) -> tuple[dict[str, Any] | None, bool]:
    """Inspect an ``https://`` endpoint's certificate before any request.

    Returns the ``tls`` block and whether the HTTP probe may run: not when the
    certificate fails normal verification without a pin, fails the pin, or the
    stored pin can't be read. When no certificate could be read at all (refused,
    timeout, not TLS) the probe runs, so Test keeps today's error text; normal
    verification or the pinned transport still guards that connection.
    """
    if not inspects(endpoint):
        return None, True
    pin, problem = _read_pin(endpoint)
    try:
        inspection = await inspect_tls(
            str(endpoint["base_url"]), pin=None if problem else pin
        )
    except ValueError:  # an https:// URL without a host: the probe reports it
        return None, problem is None
    block = tls_block(inspection, endpoint, trustable=trustable)
    if problem is not None:
        return block, False
    if inspection.certificate is None:
        return block, True
    if pin is not None:
        return block, block is not None and block["pin_matches"] is True
    return block, inspection.verified


def models_url(endpoint: Mapping[str, Any]) -> str:
    """The URL the model-list probe would call."""
    return str(endpoint["base_url"]).rstrip("/") + "/models"


def blocked_error(tls: Mapping[str, Any]) -> str:
    """Test's ``error`` when the certificate stopped the probe."""
    if tls.get("pin") is not None and tls.get("message"):
        return str(tls["message"])
    message = tls.get("message")
    if message:
        return f"The endpoint's certificate is not trusted: {message}"
    return "The endpoint's certificate is not trusted."


def blocked_test_response(
    endpoint: Mapping[str, Any], tls: Mapping[str, Any]
) -> dict[str, Any]:
    """Both Tests' answer when the certificate stopped the probe."""
    return {
        "ok": False,
        "status": None,
        "error": blocked_error(tls),
        "probe_url": models_url(endpoint),
        "tls": dict(tls),
    }


def blocked_discover_error(tls: Mapping[str, Any]) -> str:
    """Discover's ``error`` when the certificate stopped the probe."""
    if tls.get("pin") is not None and tls.get("message"):
        return str(tls["message"])
    return DISCOVER_UNTRUSTED
