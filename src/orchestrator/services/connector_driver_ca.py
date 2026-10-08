"""SRW's connector driver certificate authority (connector drivers C3).

A workspace reaches a TLS service driver (the git swap driver) by its
endpoint Service's name and trusts SRW's authority for that driver's URL
only (``http.<driver-url>.sslCAInfo``), never globally. The authority is
created at install: the chart generates it once and keeps it
(``connectors.drivers.ca``), or an operator names a Secret of their own;
either holds ``tls.crt`` and ``tls.key`` and is mounted into the
orchestrator only. When the reconciler launches a TLS driver pod, it signs a
certificate for that pod's Service names here; the certificate and its key
travel in the pod's immutable Secret, which goes with the pod.

The authority's certificate (public) is what a binding hands its workspace.
Its key never leaves the orchestrator.
"""

from __future__ import annotations

import datetime as dt
import logging
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, ed25519, ed448
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

logger = logging.getLogger(__name__)

#: A driver pod's certificate lives at most this long (and never past the
#: authority's own); pods are replaced far sooner by idling, re-pins and
#: credential generations.
LEAF_DAYS = 365
#: A certificate is valid from a little before its pod starts: node clocks
#: drift.
_BACKDATE = dt.timedelta(minutes=5)
CERT_FILE = "tls.crt"
KEY_FILE = "tls.key"


class DriverCaError(ValueError):
    """The configured authority cannot sign driver certificates."""


class DriverCertificateAuthority:
    """The authority's certificate and key; signs driver pod certificates."""

    def __init__(self, certificate: x509.Certificate, key: Any) -> None:
        try:
            constraints = certificate.extensions.get_extension_for_class(
                x509.BasicConstraints
            ).value
        except x509.ExtensionNotFound:
            raise DriverCaError("the driver CA certificate is no CA") from None
        if not constraints.ca:
            raise DriverCaError("the driver CA certificate is no CA")
        public = key.public_key().public_bytes(
            serialization.Encoding.DER,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        expected = certificate.public_key().public_bytes(
            serialization.Encoding.DER,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        if public != expected:
            raise DriverCaError("the driver CA key does not match its certificate")
        self._certificate = certificate
        self._key = key

    @classmethod
    def from_pem(cls, certificate: bytes, key: bytes) -> DriverCertificateAuthority:
        try:
            parsed = x509.load_pem_x509_certificate(certificate)
            private = serialization.load_pem_private_key(key, password=None)
        except (ValueError, TypeError) as exc:
            raise DriverCaError(f"the driver CA does not load ({exc})") from None
        return cls(parsed, private)

    @classmethod
    def load(cls, directory: str | Path) -> DriverCertificateAuthority:
        """The authority mounted at ``directory`` (``tls.crt``, ``tls.key``)."""
        base = Path(directory)
        try:
            certificate = (base / CERT_FILE).read_bytes()
            key = (base / KEY_FILE).read_bytes()
        except OSError as exc:
            raise DriverCaError(
                f"the driver CA is not readable at {base} ({exc.strerror})"
            ) from None
        return cls.from_pem(certificate, key)

    @property
    def certificate_pem(self) -> str:
        """The authority's certificate: what a workspace trusts per URL."""
        return self._certificate.public_bytes(serialization.Encoding.PEM).decode(
            "ascii"
        )

    @property
    def not_after(self) -> dt.datetime:
        return self._certificate.not_valid_after_utc

    def issue(
        self,
        dns_names: Sequence[str],
        *,
        now: dt.datetime | None = None,
        days: int = LEAF_DAYS,
    ) -> tuple[str, str]:
        """A server certificate for ``dns_names`` and its new key, as PEM."""
        names = [str(name).lower() for name in dns_names if str(name)]
        if not names:
            raise DriverCaError("a driver certificate names at least one host")
        now = now or dt.datetime.now(dt.timezone.utc)
        not_after = min(now + dt.timedelta(days=days), self.not_after)
        if not_after <= now:
            raise DriverCaError(
                "the driver CA has expired; renew connectors.drivers.ca"
            )
        key = ec.generate_private_key(ec.SECP256R1())
        # Clients match the SANs; the subject only names the certificate (a
        # common name holds at most 64 characters, an FQDN may be longer).
        common = min(names, key=len)
        subject = x509.Name(
            [x509.NameAttribute(NameOID.COMMON_NAME, common)]
            if len(common) <= 64
            else []
        )
        builder = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(self._certificate.subject)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - _BACKDATE)
            .not_valid_after(not_after)
            .add_extension(
                x509.SubjectAlternativeName([x509.DNSName(name) for name in names]),
                critical=False,
            )
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), True)
            .add_extension(
                x509.KeyUsage(
                    digital_signature=True,
                    content_commitment=False,
                    key_encipherment=False,
                    data_encipherment=False,
                    key_agreement=False,
                    key_cert_sign=False,
                    crl_sign=False,
                    encipher_only=False,
                    decipher_only=False,
                ),
                critical=True,
            )
            .add_extension(
                x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]),
                critical=False,
            )
            .add_extension(
                x509.SubjectKeyIdentifier.from_public_key(key.public_key()),
                critical=False,
            )
            .add_extension(self._authority_key_identifier(), critical=False)
        )
        algorithm = (
            None
            if isinstance(self._key, (ed25519.Ed25519PrivateKey, ed448.Ed448PrivateKey))
            else hashes.SHA256()
        )
        certificate = builder.sign(self._key, algorithm)
        return (
            certificate.public_bytes(serialization.Encoding.PEM).decode("ascii"),
            key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            ).decode("ascii"),
        )

    def _authority_key_identifier(self) -> x509.AuthorityKeyIdentifier:
        # The authority's own identifier when it has one: verifiers (GnuTLS
        # among them, which git uses on Ubuntu) pick the issuer by it.
        try:
            ski = self._certificate.extensions.get_extension_for_class(
                x509.SubjectKeyIdentifier
            ).value
        except x509.ExtensionNotFound:
            return x509.AuthorityKeyIdentifier.from_issuer_public_key(
                self._key.public_key()
            )
        return x509.AuthorityKeyIdentifier.from_issuer_subject_key_identifier(ski)


#: The process's authority, set when the application is built (as
#: ``connector_service_images.configure_service_images`` is).
_state: dict[str, DriverCertificateAuthority | None] = {"ca": None}


def configure_driver_ca(ca: DriverCertificateAuthority | None) -> None:
    _state["ca"] = ca


def driver_ca() -> DriverCertificateAuthority | None:
    return _state["ca"]


def load_driver_ca(directory: str) -> DriverCertificateAuthority | None:
    """The authority mounted at ``directory``, or ``None`` (logged) when it is
    unset or unusable: TLS drivers are then not installed."""
    if not directory:
        return None
    try:
        return DriverCertificateAuthority.load(directory)
    except DriverCaError as exc:
        logger.error(
            "Connector driver CA at %s is unusable (%s); TLS service drivers "
            "(the git swap driver) are not installed",
            directory,
            exc,
        )
        return None


__all__ = [
    "CERT_FILE",
    "KEY_FILE",
    "LEAF_DAYS",
    "DriverCaError",
    "DriverCertificateAuthority",
    "configure_driver_ca",
    "driver_ca",
    "load_driver_ca",
]
