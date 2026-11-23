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

Shared by the orchestrator, the agent and the provisioners; the third-party
imports are httpx and, when installed, ``httpx2``, which the newer SDKs run on
(``cryptography`` is imported lazily, and only by :func:`inspect_tls`). A
pinned client gets an explicit transport, so httpx ignores environment proxies
and the client's ``limits`` for it. Which helper builds which client:
:func:`httpx_client_kwargs` for a plain httpx client, :func:`pinned_transport_for`
for an SDK's own client class (openai, anthropic), which also carries the
limits and keepalive that SDK gives its own transport.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import binascii
import hashlib
import importlib
import os
import re
import socket
import ssl
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Optional
from urllib.parse import urlsplit

import httpx

try:  # anthropic >= 1.0 and openai's Default*HttpxClient >= 3.0 are built on it
    import httpx2
except ImportError:  # no SDK on httpx2 in this environment
    httpx2 = None

#: Env var suffix: the full name is ``{PREFIX}_TLS_PINNED_CERT``.
TLS_PINNED_CERT_SUFFIX = "TLS_PINNED_CERT"

#: The ``{PREFIX}_TLS_PINNED_CERT`` value dispatch writes when the endpoint's
#: stored pin can't be read: :meth:`TlsPin.from_env` refuses it, so a base URL
#: that survived in a stored override never goes out without its pin.
UNREADABLE_PIN = "unreadable"

_UNREADABLE_MESSAGE = (
    "the pinned certificate for this endpoint can't be read; an admin must "
    "trust it again or remove the pin in Admin \u2192 Models."
)

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
        """Parse the one-line base64 DER; the :data:`UNREADABLE_PIN` marker raises."""
        if value.strip() == UNREADABLE_PIN:
            raise ValueError(_UNREADABLE_MESSAGE)
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
            raise ValueError(f"{name}: {_UNREADABLE_MESSAGE}")
        try:
            return cls.from_wire(value)
        except ValueError as exc:
            raise ValueError(
                f"{name} is not a valid pinned certificate: {exc}"
            ) from exc

    @classmethod
    def _checked(cls, der: bytes) -> "TlsPin":
        """The pin for ``der``, once OpenSSL has loaded it as a trust anchor.

        The one "is this a usable certificate" check: every parse
        (``from_pem``, ``from_wire``, ``from_env``) runs it, so a reader that
        catches ``ValueError`` never holds a pin that fails later, when a
        client is built from it.
        """
        # A certificate is a DER SEQUENCE (0x30); anything else is not one.
        if not der or der[0] != 0x30:
            raise ValueError("not a DER-encoded certificate")
        pin = cls(der)
        try:
            pinned_ssl_context(pin)
        except ssl.SSLError as exc:
            raise ValueError(
                f"OpenSSL can't use it as a pinned certificate: {exc}"
            ) from exc
        return pin


def pinned_ssl_context(pin: TlsPin) -> ssl.SSLContext:
    """Return a new client context that trusts exactly ``pin`` and nothing else."""
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
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
    doesn't parse or that OpenSSL can't load as a trust anchor
    (:meth:`TlsPin.from_pem`), or a certificate that isn't the stored
    fingerprint's. A pin that can't be read is never "no pin". The Test probe
    and dispatch both read pins through this.
    """
    if not pem and not sha256:
        return None
    if not pem:
        raise ValueError("no certificate stored with the fingerprint")
    pin = TlsPin.from_pem(pem)
    if sha256 and sha256 != pin.sha256:
        raise ValueError(f"the stored fingerprint is {sha256}")
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
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
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


class _PinnedSync:
    """The pin-failure mapping, mixed into an httpx or httpx2 ``HTTPTransport``.

    ``_connect_error`` is that package's ``ConnectError``. A pin failure raises
    :class:`CertificateChangedError` or :class:`PinnedCertificateRejectedError`
    (httpx errors) from either package's transport: callers look for one class.
    """

    _connect_error: type[Exception] = httpx.ConnectError

    def __init__(self, pin: TlsPin, **kwargs: Any) -> None:
        self._pin = pin
        super().__init__(verify=pinned_ssl_context(pin), **kwargs)  # type: ignore[call-arg]

    def handle_request(self, request: Any) -> Any:
        try:
            return super().handle_request(request)  # type: ignore[misc]
        except self._connect_error as exc:
            verification = _verification_error(exc)
            if verification is None:
                raise
            host, port = request.url.host, request.url.port or 443
            presented = presented_certificate_sha256(host, port)
            raise _pin_failure(
                verification, request, self._pin, host, port, presented
            ) from exc


class _PinnedAsync:
    """The async twin of :class:`_PinnedSync`, for an ``AsyncHTTPTransport``."""

    _connect_error: type[Exception] = httpx.ConnectError

    def __init__(self, pin: TlsPin, **kwargs: Any) -> None:
        self._pin = pin
        super().__init__(verify=pinned_ssl_context(pin), **kwargs)  # type: ignore[call-arg]

    async def handle_async_request(self, request: Any) -> Any:
        try:
            return await super().handle_async_request(request)  # type: ignore[misc]
        except self._connect_error as exc:
            verification = _verification_error(exc)
            if verification is None:
                raise
            host, port = request.url.host, request.url.port or 443
            presented = await apresented_certificate_sha256(host, port)
            raise _pin_failure(
                verification, request, self._pin, host, port, presented
            ) from exc


class PinnedTransport(_PinnedSync, httpx.HTTPTransport):
    """Sync transport trusting only ``pin``; a pin failure names the change."""


class AsyncPinnedTransport(_PinnedAsync, httpx.AsyncHTTPTransport):
    """Async transport trusting only ``pin``; a pin failure names the change."""


if httpx2 is not None:

    class Httpx2PinnedTransport(_PinnedSync, httpx2.HTTPTransport):
        """:class:`PinnedTransport` for an httpx2 client."""

        _connect_error = httpx2.ConnectError

    class AsyncHttpx2PinnedTransport(_PinnedAsync, httpx2.AsyncHTTPTransport):
        """:class:`AsyncPinnedTransport` for an httpx2 client."""

        _connect_error = httpx2.ConnectError


def httpx_client_kwargs(pin: Optional[TlsPin], *, asynchronous: bool) -> dict[str, Any]:
    """Keyword arguments for a plain ``httpx`` client: ``{}`` without a pin.

    Only for a client built from the ``httpx`` package itself:
    ``httpx.Client(**httpx_client_kwargs(pin, asynchronous=False))`` for SRW's
    own httpx calls, or langchain-openai's ``http_client`` /
    ``http_async_client`` (openai 3.x still accepts a legacy httpx client).

    Not for an SDK's default client class. ``openai.DefaultHttpxClient`` /
    ``DefaultAsyncHttpxClient`` from openai 3.0, and every anthropic client
    from 1.0, are built on ``httpx2``: an httpx transport breaks inside the
    request (openai) or is refused (anthropic). For an SDK client, use that
    SDK's class with :func:`pinned_transport_for`, which picks the package::

        cls = openai.DefaultAsyncHttpxClient  # or anthropic.DefaultAsyncHttpxClient
        http_client = cls(transport=pinned_transport_for(cls, pin)) if pin else None
        client = openai.AsyncOpenAI(base_url=..., api_key=..., http_client=http_client)
    """
    if pin is None:
        return {}
    transport = AsyncPinnedTransport(pin) if asynchronous else PinnedTransport(pin)
    return {"transport": transport}


def pinned_transport_class(client_cls: type) -> type:
    """The pinned transport class for a client of class ``client_cls``.

    The anthropic SDK from 1.0 (and openai's ``Default*HttpxClient`` from 3.0)
    is built on ``httpx2``, a fork with httpx's API that refuses httpx objects;
    older releases are built on httpx. The class follows the client's package
    and its sync or async flavour. ``TypeError`` for any other class, so a
    caller can check support when it is built rather than on its first call.
    """
    if issubclass(client_cls, httpx.AsyncClient):
        return AsyncPinnedTransport
    if issubclass(client_cls, httpx.Client):
        return PinnedTransport
    if httpx2 is not None:
        if issubclass(client_cls, httpx2.AsyncClient):
            return AsyncHttpx2PinnedTransport
        if issubclass(client_cls, httpx2.Client):
            return Httpx2PinnedTransport
    raise TypeError(f"{client_cls!r} is not an httpx or httpx2 client class")


#: The SDKs whose default client class configures the transport it builds.
_SDK_PACKAGES = ("openai", "anthropic")


def _keepalive_socket_options() -> list[tuple[int, int, Any]]:
    """TCP keepalive as anthropic's ``DefaultHttpxClient`` sets it (Linux).

    Idle 60 s, then a probe every 60 s, 5 probes. The SDK builds these inline,
    with no constant to import; ``tests/test_create_llm_tls_pin.py`` compares a
    pinned and an unpinned client's pools, so a change in the SDK shows.
    """
    options: list[tuple[int, int, Any]] = [
        (socket.SOL_SOCKET, socket.SO_KEEPALIVE, True)
    ]
    for name, value in (
        ("TCP_KEEPINTVL", 60),
        ("TCP_KEEPCNT", 5),
        ("TCP_KEEPIDLE", 60),
    ):
        option = getattr(socket, name, None)
        if option is not None:
            options.append((socket.IPPROTO_TCP, option, value))
    return options


def _sdk_transport_defaults(client_cls: type) -> dict[str, Any]:
    """What an SDK's default client gives the transport it builds itself.

    ``openai`` and ``anthropic`` ``Default*HttpxClient`` set the connection
    limits (``{sdk}._constants.DEFAULT_CONNECTION_LIMITS``) and, for anthropic,
    TCP keepalive on their own transport, never on one passed in. A pinned
    transport gets the same, so it differs from the SDK's own only in trust.
    Empty for a plain httpx class.
    """
    sdk = next(
        (
            root
            for cls in client_cls.__mro__
            if (root := cls.__module__.split(".")[0]) in _SDK_PACKAGES
        ),
        None,
    )
    if sdk is None:
        return {}
    defaults: dict[str, Any] = {}
    try:
        constants = importlib.import_module(f"{sdk}._constants")
        defaults["limits"] = constants.DEFAULT_CONNECTION_LIMITS
    except (ImportError, AttributeError):
        pass  # an SDK release without the constant: the transport's own limits
    if sdk == "anthropic":
        defaults["socket_options"] = _keepalive_socket_options()
    return defaults


def pinned_transport_for(client_cls: type, pin: TlsPin, **kwargs: Any) -> Any:
    """A transport trusting only ``pin``, for a client of class ``client_cls``.

    The class comes from :func:`pinned_transport_class`. For an SDK's default
    client class it carries the settings that SDK gives its own transport
    (:func:`_sdk_transport_defaults`). ``kwargs`` go to the transport and win
    (e.g. ``proxy``); ``verify`` is always the pinned context. Pass the SDK's
    own class and build that class with the result:
    ``cls(transport=pinned_transport_for(cls, pin))`` (see
    :func:`httpx_client_kwargs` for the full recipe).
    """
    transport_cls = pinned_transport_class(client_cls)
    return transport_cls(pin, **{**_sdk_transport_defaults(client_cls), **kwargs})


def pinned_sdk_http_client(
    client_cls: type, pin: Optional[TlsPin], base_url: Optional[str]
) -> Any:
    """The ``http_client`` for an SDK client calling ``base_url``: ``None`` unpinned.

    ``cls = openai.DefaultAsyncHttpxClient`` (or the sync class), built with
    :func:`pinned_transport_for`: the recipe in :func:`httpx_client_kwargs`. A
    pin only means something for an ``https://`` URL, so a pin with any other
    URL (``http://``, or none: the SDK's public default) raises ``ValueError``
    instead of being ignored.
    """
    if pin is None:
        return None
    if not pin_applies(base_url):
        raise ValueError(
            "a pinned certificate needs an https:// base URL, but this client "
            f"would call {base_url or 'its public default endpoint'}"
        )
    return client_cls(transport=pinned_transport_for(client_cls, pin))


def pin_failure_in(exc: BaseException) -> Optional[httpx.ConnectError]:
    """The pin failure behind ``exc`` (an SDK wraps it), if there is one."""
    seen: set[int] = set()
    cur: Optional[BaseException] = exc
    while cur is not None and id(cur) not in seen:
        if isinstance(cur, (CertificateChangedError, PinnedCertificateRejectedError)):
            return cur
        seen.add(id(cur))
        cur = cur.__cause__ or cur.__context__
    return None


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
