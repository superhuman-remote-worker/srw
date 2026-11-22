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

import base64
import binascii
import hashlib
import os
import re
import ssl
from dataclasses import dataclass
from typing import Mapping, Optional

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
