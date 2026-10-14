"""Operator-supplied CA certificates for outbound TLS.

An on-premises service the platform calls — typically a model API behind a
private or self-signed CA — must be reachable over HTTPS without switching
verification off and without falling back to plain HTTP. The chart value
``trust.extraCaCertificates`` (or ``trust.existingConfigMap``) puts the
operator's PEM bundle into a ConfigMap; the orchestrator, every agent pod and
every workspace pod mount it at :data:`TRUST_CA_FILE` and receive
:data:`EXTRA_CA_FILE_ENV` pointing there.

Each Python process calls :func:`install_extra_ca_trust` before it builds any
HTTP client. ``SSL_CERT_FILE`` *replaces* a client's CA list (httpx 0.28 loads
it instead of certifi), so the hook writes one bundle holding the public roots
the clients used before plus the operator's certificates, and points
``SSL_CERT_FILE`` and ``REQUESTS_CA_BUNDLE`` at it. httpx (the OpenAI and
Anthropic SDKs, LangChain), requests, aiohttp and ``ssl.create_default_context``
then all verify against it. Nothing here disables verification.

Pure stdlib (``certifi`` is optional), so the orchestrator, the agent and the
provisioners share one definition of the mount and the environment.
"""

from __future__ import annotations

import logging
import os
import ssl
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import MutableMapping, Optional

logger = logging.getLogger(__name__)

#: Points at the operator's PEM file inside a pod.
EXTRA_CA_FILE_ENV = "SRW_EXTRA_CA_FILE"
#: Orchestrator env naming the ConfigMap (and key) the provisioners mount.
TRUST_CA_CONFIGMAP_ENV = "TRUST_CA_CONFIGMAP"
TRUST_CA_CONFIGMAP_KEY_ENV = "TRUST_CA_CONFIGMAP_KEY"

TRUST_CA_VOLUME = "srw-trust-ca"
TRUST_CA_MOUNT_DIR = "/etc/srw/trust"
TRUST_CA_FILE = f"{TRUST_CA_MOUNT_DIR}/ca.crt"

#: The variables a combined bundle is published under. Both replace the
#: client's default CA list, which is why the bundle carries the public roots.
BUNDLE_ENV_NAMES = ("SSL_CERT_FILE", "REQUESTS_CA_BUNDLE")
BUNDLE_FILE_NAME = "srw-ca-bundle.pem"

_PEM_BEGIN = "-----BEGIN CERTIFICATE-----"


@dataclass(frozen=True)
class TrustCaSource:
    """The ConfigMap a provisioner mounts into the pods it builds."""

    configmap: str
    key: str = "ca.crt"

    @classmethod
    def from_env(
        cls, environ: Optional[MutableMapping[str, str]] = None
    ) -> Optional["TrustCaSource"]:
        """The source the chart configured on the orchestrator, if any."""
        env = os.environ if environ is None else environ
        name = (env.get(TRUST_CA_CONFIGMAP_ENV) or "").strip()
        if not name:
            return None
        key = (env.get(TRUST_CA_CONFIGMAP_KEY_ENV) or "").strip() or "ca.crt"
        return cls(configmap=name, key=key)

    def volume(self) -> dict:
        return {
            "name": TRUST_CA_VOLUME,
            "configMap": {
                "name": self.configmap,
                "defaultMode": 0o444,
                "items": [{"key": self.key, "path": Path(TRUST_CA_FILE).name}],
            },
        }

    @staticmethod
    def volume_mount() -> dict:
        return {
            "name": TRUST_CA_VOLUME,
            "mountPath": TRUST_CA_MOUNT_DIR,
            "readOnly": True,
        }

    @staticmethod
    def env() -> dict:
        return {"name": EXTRA_CA_FILE_ENV, "value": TRUST_CA_FILE}


def _default_roots() -> str:
    """The PEM roots the process's clients trust when nothing is configured."""
    try:
        import certifi

        return Path(certifi.where()).read_text(encoding="utf-8")
    except Exception:  # pragma: no cover - certifi ships with httpx/requests
        cafile = ssl.get_default_verify_paths().cafile
        return Path(cafile).read_text(encoding="utf-8") if cafile else ""


def install_extra_ca_trust(
    environ: Optional[MutableMapping[str, str]] = None,
    *,
    bundle_dir: Optional[str] = None,
) -> Optional[str]:
    """Trust the operator's CA in this process; return the bundle path.

    No-op (``None``) unless :data:`EXTRA_CA_FILE_ENV` names a file. The base
    is the bundle ``SSL_CERT_FILE`` already names (an operator's own choice),
    else certifi's roots. An unreadable or malformed file is logged and
    ignored: the public endpoints keep working, and the private one fails its
    TLS handshake with the certificate error, never silently unverified.
    """
    env = os.environ if environ is None else environ
    extra_path = (env.get(EXTRA_CA_FILE_ENV) or "").strip()
    if not extra_path:
        return None
    try:
        extra = Path(extra_path).read_text(encoding="utf-8")
        if _PEM_BEGIN not in extra:
            raise ValueError("no PEM certificate in the file")
        # Parse before publishing: a broken bundle would break every client.
        ssl.create_default_context(cadata=extra)
    except (OSError, ValueError, ssl.SSLError) as exc:
        logger.error(
            "Extra CA trust NOT installed: %s=%s cannot be used (%s: %s). "
            "Fix trust.extraCaCertificates in the chart values.",
            EXTRA_CA_FILE_ENV,
            extra_path,
            type(exc).__name__,
            exc,
        )
        return None

    directory = Path(bundle_dir or tempfile.gettempdir())
    target = directory / BUNDLE_FILE_NAME
    base_file = (env.get("SSL_CERT_FILE") or "").strip()
    if base_file and Path(base_file).resolve() == target.resolve():
        # Already installed in this process, or inherited from the parent
        # process that installed it: the bundle carries the CA already.
        return str(target)
    try:
        base = (
            Path(base_file).read_text(encoding="utf-8")
            if base_file
            else _default_roots()
        )
    except OSError as exc:
        logger.error(
            "Extra CA trust NOT installed: SSL_CERT_FILE=%s cannot be read (%s)",
            base_file,
            exc,
        )
        return None

    bundle = base.rstrip("\n") + "\n" + extra.strip() + "\n"
    fd, tmp = tempfile.mkstemp(dir=directory, prefix=".srw-ca-", suffix=".pem")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(bundle)
        os.chmod(tmp, 0o644)
        os.replace(tmp, target)
    except OSError as exc:
        Path(tmp).unlink(missing_ok=True)
        logger.error("Extra CA trust NOT installed: cannot write %s (%s)", target, exc)
        return None

    for name in BUNDLE_ENV_NAMES:
        env[name] = str(target)
    # Node appends this file to its own roots, so it takes the operator's
    # certificates alone.
    env.setdefault("NODE_EXTRA_CA_CERTS", extra_path)
    logger.info(
        "Extra CA trust installed: %d operator certificate(s) from %s added to "
        "the public roots in %s",
        extra.count(_PEM_BEGIN),
        extra_path,
        target,
    )
    return str(target)


__all__ = [
    "BUNDLE_ENV_NAMES",
    "EXTRA_CA_FILE_ENV",
    "TRUST_CA_CONFIGMAP_ENV",
    "TRUST_CA_CONFIGMAP_KEY_ENV",
    "TRUST_CA_FILE",
    "TRUST_CA_MOUNT_DIR",
    "TRUST_CA_VOLUME",
    "TrustCaSource",
    "install_extra_ca_trust",
]
