"""What the provider-minted drivers' HTTP calls share (connector drivers C5).

SRW mints short-lived credentials at a provider from its own process: a
Kubernetes API server's TokenRequest (``token_request``) and GitHub's App
installation tokens (``github_app``). Both speak JSON over HTTPS, never
follow a redirect (a redirect would carry the minting credential
elsewhere), ignore the process's proxy environment and answer every
failure as a :class:`ProviderError`: a message for the connector's owner
(never a credential, never the provider's raw body beyond a short, cleaned
message) and whether trying again later can help.

Tests replace the client factory (:func:`configure_provider_http`) with one
over an ``httpx.MockTransport``.
"""

from __future__ import annotations

import ssl
import unicodedata
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import httpx

#: One provider call's deadline.
DEFAULT_TIMEOUT_SECONDS = 10.0
#: The longest provider message kept.
MAX_MESSAGE = 200

ClientFactory = Callable[..., httpx.AsyncClient]


class ProviderError(Exception):
    """A provider refused or did not answer. ``transient``: trying again
    later can help (a timeout, a 5xx, a rate limit)."""

    def __init__(self, message: str, *, transient: bool) -> None:
        super().__init__(message)
        self.transient = transient


@dataclass(frozen=True)
class MintedToken:
    """A credential a provider minted. ``token`` is secret; ``handle`` is
    what revoking it needs besides the token (a bound Secret's uid)."""

    token: str
    expires_at: datetime
    handle: str | None = None

    def __repr__(self) -> str:  # never the token
        return f"MintedToken(expires_at={self.expires_at!r})"


def _default_client(*, verify: Any = True, timeout: float = DEFAULT_TIMEOUT_SECONDS):
    return httpx.AsyncClient(
        verify=verify, timeout=timeout, trust_env=False, follow_redirects=False
    )


_state: dict[str, ClientFactory] = {"factory": _default_client}


def configure_provider_http(factory: ClientFactory | None) -> None:
    """Install the client factory (``None``: the real one)."""
    _state["factory"] = factory or _default_client


def provider_client(
    *, verify: Any = True, timeout: float = DEFAULT_TIMEOUT_SECONDS
) -> httpx.AsyncClient:
    return _state["factory"](verify=verify, timeout=timeout)


def tls_context(ca_pem: str | None) -> ssl.SSLContext:
    """Verify against ``ca_pem`` only when given, else the public roots;
    ``ProviderError`` (final) for certificates that do not load."""
    if not ca_pem:
        return ssl.create_default_context()
    try:
        return ssl.create_default_context(cadata=ca_pem)
    except (ssl.SSLError, ValueError):
        raise ProviderError(
            "the configured CA certificates are not usable", transient=False
        ) from None


def clean(text: Any, limit: int = MAX_MESSAGE) -> str:
    """A provider's message, safe to store and show: no control characters,
    collapsed whitespace, capped."""
    cleaned = "".join(
        " " if unicodedata.category(ch).startswith("C") else ch
        for ch in str(text or "")
    )
    cleaned = " ".join(cleaned.split())
    return cleaned if len(cleaned) <= limit else cleaned[: limit - 3] + "..."


def message_of(response: httpx.Response) -> str:
    """The ``message`` of a JSON error answer, cleaned (empty otherwise)."""
    try:
        body = response.json()
    except ValueError:
        return ""
    return clean(body.get("message")) if isinstance(body, dict) else ""


def transport_error(exc: Exception, who: str) -> ProviderError:
    """A request that got no answer: an untrusted certificate is final,
    anything else may pass."""
    text = str(exc)
    if "CERTIFICATE_VERIFY_FAILED" in text or isinstance(
        getattr(exc, "__cause__", None), ssl.SSLCertVerificationError
    ):
        return ProviderError(
            f"{who}'s certificate does not verify against the configured CA",
            transient=False,
        )
    if isinstance(exc, httpx.TimeoutException):
        return ProviderError(f"{who} did not answer in time", transient=True)
    return ProviderError(f"{who} could not be reached", transient=True)


def status_transient(status: int) -> bool:
    return status == 429 or status >= 500


def parse_time(value: Any) -> datetime:
    """An RFC 3339 timestamp, as an aware datetime."""
    if not isinstance(value, str) or not value:
        raise ValueError("no timestamp")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


__all__ = [
    "ClientFactory",
    "DEFAULT_TIMEOUT_SECONDS",
    "MintedToken",
    "ProviderError",
    "clean",
    "configure_provider_http",
    "message_of",
    "parse_time",
    "provider_client",
    "status_transient",
    "tls_context",
    "transport_error",
]
