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

Pure stdlib, so the orchestrator, the agent and the provisioners share it.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import os
import re
import socket
import ssl
from dataclasses import dataclass
from typing import Any, Mapping, Optional

import httpx

#: Env var suffix: the full name is ``{PREFIX}_TLS_PINNED_CERT``.
TLS_PINNED_CERT_SUFFIX = "TLS_PINNED_CERT"

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

        A value that is set but unparsable raises ``ValueError`` naming the
        variable: a broken pin must never silently become "no pin".
        """
        name = f"{prefix}_{TLS_PINNED_CERT_SUFFIX}"
        value = (os.environ if env is None else env).get(name, "")
        if not value.strip():
            return None
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
    except (OSError, ssl.SSLError, ValueError):
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
    except (OSError, ssl.SSLError, ValueError, asyncio.TimeoutError):
        return None
    finally:
        if writer is not None:
            writer.close()
    return fingerprint_sha256(der) if der else None


def _is_verification_failure(exc: BaseException) -> bool:
    seen: set[int] = set()
    cur: Optional[BaseException] = exc
    while cur is not None and id(cur) not in seen:
        if isinstance(cur, ssl.SSLCertVerificationError):
            return True
        seen.add(id(cur))
        cur = cur.__cause__ or cur.__context__
    return False


def _changed(
    exc: httpx.ConnectError, request: httpx.Request, pin: TlsPin, presented
) -> CertificateChangedError:
    return CertificateChangedError(
        request.url.host,
        request.url.port or 443,
        pin.sha256,
        presented,
        request=request,
    )


class PinnedTransport(httpx.HTTPTransport):
    """Sync transport trusting only ``pin``; a pin failure names the change."""

    def __init__(self, pin: TlsPin, **kwargs: Any) -> None:
        self._pin = pin
        super().__init__(verify=pinned_ssl_context(pin), **kwargs)

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        try:
            return super().handle_request(request)
        except httpx.ConnectError as exc:
            if not _is_verification_failure(exc):
                raise
            presented = presented_certificate_sha256(
                request.url.host, request.url.port or 443
            )
            raise _changed(exc, request, self._pin, presented) from exc


class AsyncPinnedTransport(httpx.AsyncHTTPTransport):
    """Async transport trusting only ``pin``; a pin failure names the change."""

    def __init__(self, pin: TlsPin, **kwargs: Any) -> None:
        self._pin = pin
        super().__init__(verify=pinned_ssl_context(pin), **kwargs)

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        try:
            return await super().handle_async_request(request)
        except httpx.ConnectError as exc:
            if not _is_verification_failure(exc):
                raise
            presented = await apresented_certificate_sha256(
                request.url.host, request.url.port or 443
            )
            raise _changed(exc, request, self._pin, presented) from exc


def httpx_client_kwargs(pin: Optional[TlsPin], *, asynchronous: bool) -> dict[str, Any]:
    """Keyword arguments for an httpx client: ``{}`` without a pin."""
    if pin is None:
        return {}
    transport = AsyncPinnedTransport(pin) if asynchronous else PinnedTransport(pin)
    return {"transport": transport}
