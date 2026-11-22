"""Per-endpoint certificate pinning for outbound TLS.

An operator's on-premises model endpoint often serves a certificate that no CA
list can validate: a version-1 certificate without a subject alternative name,
signed by a private CA, or self-signed. Trusting its CA does not help (the
hostname check still fails) and switching verification off lets any machine on
the network impersonate the endpoint and read the API key. Pinning trusts
exactly one certificate instead: the one the admin reviewed and approved.

Mechanism: :func:`pinned_ssl_context` builds an ``ssl.SSLContext`` whose only
trust anchor is the pinned certificate, with partial-chain validation so a
leaf is accepted as its own anchor, no hostname check (the certificate itself
is the identity) and no validity-period check (an expired pinned certificate
keeps working until the operator replaces it). The check runs inside the
handshake, before any request byte, so the API key never reaches an impostor.

Two rules from the spike (vault spec ``endpoint-certificate-pinning-design``):

* Build a fresh context per client; never share one with ``requests``, which
  loads its own CA bundle into the context it is handed.
* The context starts from ``ssl.SSLContext(PROTOCOL_TLS_CLIENT)``, never from
  ``ssl.create_default_context()``: the latter reads ``SSL_CERT_FILE`` and, on
  recent Pythons, sets ``VERIFY_X509_STRICT``.

Shared by the orchestrator, the agent and the provisioners; the only
third-party import is httpx (``cryptography`` is imported lazily, and only by
:func:`inspect_tls`). A pinned client gets an explicit transport, so
httpx ignores environment proxies and the client's ``limits`` for it.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import binascii
import hashlib
import os
import re
import socket
import ssl
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Optional
from urllib.parse import urlsplit

import httpx

#: Env var suffix: the full name is ``{PREFIX}_TLS_PINNED_CERT``.
TLS_PINNED_CERT_SUFFIX = "TLS_PINNED_CERT"

#: The ``{PREFIX}_TLS_PINNED_CERT`` value dispatch writes when the endpoint's
#: stored pin can't be read: :meth:`TlsPin.from_env` refuses it, so a base URL
#: that survived in a stored override never goes out without its pin.
UNREADABLE_PIN = "unreadable"

#: OpenSSL's ``X509_V_FLAG_NO_CHECK_TIME``; Python has no name for it.
NO_CHECK_TIME = 0x200000

_PEM_RE = re.compile(
    r"-----BEGIN CERTIFICATE-----\s*(.*?)\s*-----END CERTIFICATE-----",
    re.DOTALL,
)


def fingerprint_sha256(der: bytes) -> str:
    """Return the SHA-256 of ``der`` as ``AB:CD:...`` (the openssl format)."""
    digest = hashlib.sha256(der).hexdigest().upper()
    return ":".join(digest[i : i + 2] for i in range(0, len(digest), 2))


@dataclass(frozen=True)
class TlsPin:
    """One pinned certificate, held as its DER encoding."""

    der: bytes

    @property
    def sha256(self) -> str:
        return fingerprint_sha256(self.der)

    @property
    def pem(self) -> str:
        return ssl.DER_cert_to_PEM_cert(self.der)

    @property
    def wire(self) -> str:
        """Base64 of the DER on one line: the form that travels in env vars."""
        return base64.b64encode(self.der).decode("ascii")

    @classmethod
    def from_pem(cls, pem: str) -> "TlsPin":
        blocks = _PEM_RE.findall(pem)
        if len(blocks) != 1:
            raise ValueError(
                f"expected exactly one PEM certificate, found {len(blocks)}"
            )
        try:
            der = base64.b64decode("".join(blocks[0].split()), validate=True)
        except (binascii.Error, ValueError) as exc:
            raise ValueError(f"invalid PEM certificate: {exc}") from exc
        return cls._checked(der)

    @classmethod
    def from_wire(cls, value: str) -> "TlsPin":
        try:
            der = base64.b64decode(value.strip(), validate=True)
        except (binascii.Error, ValueError) as exc:
            raise ValueError(f"invalid base64 certificate: {exc}") from exc
        return cls._checked(der)

    @classmethod
    def from_env(
        cls, prefix: str, env: Optional[Mapping[str, str]] = None
    ) -> "Optional[TlsPin]":
        """Read ``{prefix}_TLS_PINNED_CERT``; empty or unset means no pin.

        A value that is set but unparsable, or the :data:`UNREADABLE_PIN`
        marker, raises ``ValueError`` naming the variable: a broken pin must
        never silently become "no pin".
        """
        name = f"{prefix}_{TLS_PINNED_CERT_SUFFIX}"
        value = (os.environ if env is None else env).get(name, "")
        if not value.strip():
            return None
        if value.strip() == UNREADABLE_PIN:
            raise ValueError(
                f"{name}: the pinned certificate for this endpoint can't be "
                "read; an admin must trust it again or remove the pin in "
                "Admin → Models."
            )
        try:
            return cls.from_wire(value)
        except ValueError as exc:
            raise ValueError(
                f"{name} is not a valid pinned certificate: {exc}"
            ) from exc

    @classmethod
    def _checked(cls, der: bytes) -> "TlsPin":
        # A certificate is a DER SEQUENCE (0x30); anything else is not one.
        if not der or der[0] != 0x30:
            raise ValueError("not a DER-encoded certificate")
        return cls(der)


def pinned_ssl_context(pin: TlsPin) -> ssl.SSLContext:
    """Return a new client context that trusts exactly ``pin`` and nothing else."""
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_REQUIRED
    ctx.load_verify_locations(cadata=pin.pem)
    ctx.verify_flags |= ssl.VERIFY_X509_PARTIAL_CHAIN | NO_CHECK_TIME
    return ctx


def pin_applies(base_url: Optional[str]) -> bool:
    """Whether a pin can apply to ``base_url``: an ``https://`` URL only."""
    return urlsplit(str(base_url or "")).scheme == "https"


def read_stored_pin(pem: Optional[str], sha256: Optional[str]) -> Optional[TlsPin]:
    """The pin an endpoint row stores (``tls_pin_cert``, ``tls_pin_sha256``).

    ``None`` when the row stores no pin. A stored pin that can't be used
    raises ``ValueError``: a fingerprint without its certificate, a PEM that
    doesn't parse, a certificate that isn't the stored fingerprint's, or one
    OpenSSL can't load as a trust anchor. A pin that can't be read is never
    "no pin". The Test probe and dispatch both read pins through this.
    """
    if not pem and not sha256:
        return None
    if not pem:
        raise ValueError("no certificate stored with the fingerprint")
    pin = TlsPin.from_pem(pem)
    if sha256 and sha256 != pin.sha256:
        raise ValueError(f"the stored fingerprint is {sha256}")
    try:
        pinned_ssl_context(pin)  # OpenSSL must accept it as a trust anchor
    except ssl.SSLError as exc:
        raise ValueError(str(exc)) from exc
    return pin


#: How long the lookup of the certificate a server presents may take.
PRESENTED_LOOKUP_TIMEOUT = 5.0


class CertificateChangedError(httpx.ConnectError):
    """The server did not present the pinned certificate; no request was sent."""

    def __init__(
        self,
        host: str,
        port: int,
        pinned_sha256: str,
        presented_sha256: Optional[str],
        *,
        request: Optional[httpx.Request] = None,
    ) -> None:
        self.host = host
        self.port = port
        self.pinned_sha256 = pinned_sha256
        self.presented_sha256 = presented_sha256
        presented = presented_sha256 if presented_sha256 else "a different certificate"
        super().__init__(
            f"TLS certificate changed for {host}:{port}: pinned SHA-256 "
            f"{pinned_sha256}, server presents {presented}. "
            "An admin must re-trust it in Admin \u2192 Models.",
            request=request,
        )

    def __reduce__(self):
        return (
            _rebuild_changed,
            (self.host, self.port, self.pinned_sha256, self.presented_sha256),
        )


def _rebuild_changed(host, port, pinned, presented):
    return CertificateChangedError(host, port, pinned, presented)


class PinnedCertificateRejectedError(httpx.ConnectError):
    """The server presents the pinned certificate but OpenSSL still refuses it."""

    def __init__(
        self,
        host: str,
        port: int,
        sha256: str,
        reason: str,
        *,
        request: Optional[httpx.Request] = None,
    ) -> None:
        self.host = host
        self.port = port
        self.sha256 = sha256
        self.reason = reason
        super().__init__(
            f"TLS handshake with {host}:{port} failed although the server "
            f"presents the pinned certificate (SHA-256 {sha256}): {reason}. "
            "SRW cannot use this certificate; the server needs a different one.",
            request=request,
        )

    def __reduce__(self):
        return (
            _rebuild_rejected,
            (self.host, self.port, self.sha256, self.reason),
        )


def _rebuild_rejected(host, port, sha256, reason):
    return PinnedCertificateRejectedError(host, port, sha256, reason)


def _lookup_context() -> ssl.SSLContext:
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


def presented_certificate_sha256(
    host: str, port: int, *, timeout: float = PRESENTED_LOOKUP_TIMEOUT
) -> Optional[str]:
    """Fingerprint of the certificate ``host:port`` presents, or ``None``.

    Handshake only, nothing is sent over the connection. Any failure gives
    ``None``: this is diagnostics for an error message, never a trust decision.
    """
    try:
        with socket.create_connection((host, port), timeout=timeout) as raw:
            with _lookup_context().wrap_socket(raw, server_hostname=host) as tls:
                der = tls.getpeercert(binary_form=True)
    except Exception:
        return None
    return fingerprint_sha256(der) if der else None


async def apresented_certificate_sha256(
    host: str, port: int, *, timeout: float = PRESENTED_LOOKUP_TIMEOUT
) -> Optional[str]:
    """Async twin of :func:`presented_certificate_sha256`."""
    writer = None
    try:
        _, writer = await asyncio.wait_for(
            asyncio.open_connection(
                host, port, ssl=_lookup_context(), server_hostname=host
            ),
            timeout,
        )
        ssl_object = writer.get_extra_info("ssl_object")
        der = ssl_object.getpeercert(binary_form=True) if ssl_object else None
    except Exception:
        return None
    finally:
        if writer is not None:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()
    return fingerprint_sha256(der) if der else None


def _verification_error(exc: BaseException) -> Optional[ssl.SSLCertVerificationError]:
    seen: set[int] = set()
    cur: Optional[BaseException] = exc
    while cur is not None and id(cur) not in seen:
        if isinstance(cur, ssl.SSLCertVerificationError):
            return cur
        seen.add(id(cur))
        cur = cur.__cause__ or cur.__context__
    return None


def _pin_failure(
    verification: ssl.SSLCertVerificationError,
    request: httpx.Request,
    pin: TlsPin,
    host: str,
    port: int,
    presented: Optional[str],
) -> httpx.ConnectError:
    if presented == pin.sha256:
        reason = getattr(verification, "verify_message", None) or str(verification)
        return PinnedCertificateRejectedError(
            host, port, pin.sha256, reason, request=request
        )
    return CertificateChangedError(host, port, pin.sha256, presented, request=request)


class PinnedTransport(httpx.HTTPTransport):
    """Sync transport trusting only ``pin``; a pin failure names the change."""

    def __init__(self, pin: TlsPin, **kwargs: Any) -> None:
        self._pin = pin
        super().__init__(verify=pinned_ssl_context(pin), **kwargs)

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        try:
            return super().handle_request(request)
        except httpx.ConnectError as exc:
            verification = _verification_error(exc)
            if verification is None:
                raise
            host, port = request.url.host, request.url.port or 443
            presented = presented_certificate_sha256(host, port)
            raise _pin_failure(
                verification, request, self._pin, host, port, presented
            ) from exc


class AsyncPinnedTransport(httpx.AsyncHTTPTransport):
    """Async transport trusting only ``pin``; a pin failure names the change."""

    def __init__(self, pin: TlsPin, **kwargs: Any) -> None:
        self._pin = pin
        super().__init__(verify=pinned_ssl_context(pin), **kwargs)

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        try:
            return await super().handle_async_request(request)
        except httpx.ConnectError as exc:
            verification = _verification_error(exc)
            if verification is None:
                raise
            host, port = request.url.host, request.url.port or 443
            presented = await apresented_certificate_sha256(host, port)
            raise _pin_failure(
                verification, request, self._pin, host, port, presented
            ) from exc


def httpx_client_kwargs(pin: Optional[TlsPin], *, asynchronous: bool) -> dict[str, Any]:
    """Keyword arguments for an httpx client: ``{}`` without a pin."""
    if pin is None:
        return {}
    transport = AsyncPinnedTransport(pin) if asynchronous else PinnedTransport(pin)
    return {"transport": transport}


# ---- Read-only inspection (endpoint Test and the Trust API) -------------------

#: How long one inspection or pinned handshake may take.
INSPECT_TIMEOUT = 10.0

#: OpenSSL ``X509_V_ERR_*`` code -> the reason the admin UI shows.
REASON_BY_VERIFY_CODE = {
    18: "self_signed",
    19: "self_signed",
    2: "unknown_issuer",
    20: "unknown_issuer",
    21: "unknown_issuer",
    62: "address_mismatch",
    64: "address_mismatch",
    10: "expired",
    9: "not_yet_valid",
}


@dataclass(frozen=True)
class CertificateDetails:
    """What an admin needs to decide whether to trust a certificate."""

    sha256: str
    subject: str
    issuer: str
    not_before: datetime
    not_after: datetime
    expired: bool
    san: list[str]
    version: int
    pem: str

    def to_public(self) -> dict:
        """JSON-ready form: ISO dates, no PEM."""
        return {
            "sha256": self.sha256,
            "subject": self.subject,
            "issuer": self.issuer,
            "not_before": self.not_before.isoformat(),
            "not_after": self.not_after.isoformat(),
            "expired": self.expired,
            "san": list(self.san),
            "version": self.version,
        }


@dataclass(frozen=True)
class TlsInspection:
    """Result of :func:`inspect_tls`.

    ``verified`` means the certificate passes the verification SRW's httpx
    clients apply; ``reason``/``message`` say why not. ``error`` is set (and
    ``certificate`` is ``None``) when no certificate could be read at all.
    """

    verified: bool
    reason: Optional[str]
    message: Optional[str]
    certificate: Optional[CertificateDetails]
    pin_matches: Optional[bool]
    error: Optional[str]


def _https_target(base_url: str) -> tuple[str, int]:
    parts = urlsplit(base_url)
    if parts.scheme != "https":
        raise ValueError("inspect_tls needs an https:// URL")
    host = parts.hostname
    if not host:
        raise ValueError("inspect_tls needs an https:// URL with a host")
    return host, parts.port or 443


async def _handshake_der(
    host: str, port: int, ctx: ssl.SSLContext, timeout: float
) -> Optional[bytes]:
    """Complete a handshake, return the peer certificate (DER), send nothing."""
    writer = None
    try:
        _, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port, ssl=ctx, server_hostname=host),
            timeout,
        )
        ssl_object = writer.get_extra_info("ssl_object")
        return ssl_object.getpeercert(binary_form=True) if ssl_object else None
    finally:
        if writer is not None:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()


_NAME_SHORT = {
    "commonName": "CN",
    "organizationName": "O",
    "organizationalUnitName": "OU",
    "countryName": "C",
    "stateOrProvinceName": "ST",
    "localityName": "L",
    "emailAddress": "emailAddress",
    "domainComponent": "DC",
    "serialNumber": "serialNumber",
}


def _format_name(name: Any) -> str:
    parts = []
    for attr in name:
        long = getattr(attr.oid, "_name", "")
        parts.append(
            f"{_NAME_SHORT.get(long) or long or attr.oid.dotted_string}={attr.value}"
        )
    return ", ".join(parts)


def _certificate_details(der: bytes) -> CertificateDetails:
    from cryptography import x509  # lazy: only inspection needs it

    cert = x509.load_der_x509_certificate(der)
    san: list[str] = []
    try:
        ext = cert.extensions.get_extension_for_class(x509.SubjectAlternativeName)
    except x509.ExtensionNotFound:
        pass
    else:
        for entry in ext.value:
            if isinstance(entry, x509.IPAddress):
                san.append(f"IP:{entry.value}")
            elif isinstance(entry, x509.DNSName):
                san.append(f"DNS:{entry.value}")
            else:
                san.append(f"{type(entry).__name__}:{entry.value}")
    not_after = cert.not_valid_after_utc
    return CertificateDetails(
        sha256=fingerprint_sha256(der),
        subject=_format_name(cert.subject),
        issuer=_format_name(cert.issuer),
        not_before=cert.not_valid_before_utc,
        not_after=not_after,
        expired=not_after < datetime.now(timezone.utc),
        san=san,
        version=cert.version.value + 1,
        pem=ssl.DER_cert_to_PEM_cert(der),
    )


async def inspect_tls(
    base_url: str,
    *,
    pin: Optional[TlsPin] = None,
    timeout: float = INSPECT_TIMEOUT,
) -> TlsInspection:
    """Read the certificate ``base_url`` presents; sends no application data.

    The first handshake verifies exactly as SRW's httpx clients do. If that
    fails on the certificate, a second handshake without verification reads it,
    so the admin can see what they would be trusting.
    """
    host, port = _https_target(base_url)
    reason: Optional[str] = None
    message: Optional[str] = None
    try:
        der = await _handshake_der(host, port, httpx.create_ssl_context(), timeout)
    except Exception as exc:
        verification = _verification_error(exc)
        if verification is None:
            return TlsInspection(False, None, None, None, None, _error_text(exc))
        code = getattr(verification, "verify_code", None)
        reason = REASON_BY_VERIFY_CODE.get(code, "other")
        message = getattr(verification, "verify_message", None) or str(verification)
        try:
            der = await _handshake_der(host, port, _lookup_context(), timeout)
        except Exception as exc2:
            return TlsInspection(False, None, None, None, None, _error_text(exc2))
    if not der:
        return TlsInspection(False, None, None, None, None, "no certificate presented")
    try:
        details = _certificate_details(der)
    except Exception as exc:
        return TlsInspection(
            False, None, None, None, None, f"unreadable certificate: {exc}"
        )
    return TlsInspection(
        verified=reason is None,
        reason=reason,
        message=message,
        certificate=details,
        pin_matches=None if pin is None else details.sha256 == pin.sha256,
        error=None,
    )


def _error_text(exc: BaseException) -> str:
    if isinstance(exc, (asyncio.TimeoutError, TimeoutError)):
        return "timed out"
    return str(exc) or type(exc).__name__


async def pinned_handshake_error(
    base_url: str, pin: TlsPin, *, timeout: float = INSPECT_TIMEOUT
) -> Optional[str]:
    """One handshake under ``pin``: ``None`` if SRW can use it, else why not.

    Lets the Trust API refuse a certificate OpenSSL would reject (a weak key,
    say) before storing it. Sends no application data.
    """
    host, port = _https_target(base_url)
    try:
        await _handshake_der(host, port, pinned_ssl_context(pin), timeout)
    except Exception as exc:
        verification = _verification_error(exc)
        if verification is not None:
            return getattr(verification, "verify_message", None) or str(verification)
        return _error_text(exc)
    return None
