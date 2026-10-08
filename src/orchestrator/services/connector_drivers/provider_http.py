"""What the provider-minted drivers' HTTP calls share (connector drivers C5).

SRW mints short-lived credentials at a provider from its own process: a
Kubernetes API server's TokenRequest (``token_request``) and GitHub's App
installation tokens (``github_app``). The provider's address comes from a
connector, so every call is held to the same rules:

* **One deadline per call.** Address resolution, the connection, the
  request and the whole answer run under one ``asyncio.timeout``
  (:data:`DEFAULT_DEADLINE_SECONDS`): a host that trickles its answer a byte
  at a time cannot hold a delivery, a claim or the leader's revoke sweep.
* **A capped answer.** The body is streamed and refused past
  :data:`MAX_BODY_BYTES`.
* **A pinned, checked address.** The host is resolved once and every answer
  must be one the connector's projects may reach
  (``connector_egress.refusal``: never loopback, link-local, cloud metadata
  or the cluster's ranges; private addresses only on a project tier that
  allows them). The request dials the checked address, with the name kept
  for the Host header and TLS (SNI and the certificate check), so a second
  lookup cannot send it elsewhere; nothing is kept alive between calls.
  Hosts the operator names in ``connectors.providerMinting.privateHosts``
  (``host`` or ``host:port``, e.g. ``kubernetes.default.svc``) may be
  private or in the cluster's ranges too, never link-local or metadata.
* **No redirect, no proxy** from the process environment.
* **Fixed reasons.** A :class:`ProviderError`'s text is one of a fixed set
  (an HTTP status SRW names, never the provider's own words, an address or
  an exception's text), so the error a connector's owner reads is no oracle
  for what lies behind an address; the raw detail goes to the server log.

Tests replace the client factory and the network (resolver) with
:func:`configure_provider_http` and :func:`configure_provider_network`.
"""

from __future__ import annotations

import asyncio
import ipaddress
import json
import logging
import ssl
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import httpx

from orchestrator.services.connector_egress import (
    DEFAULT_CLUSTER_CIDRS,
    DEFAULT_PRIVATE_TIERS,
    EgressPolicy,
    Resolver,
    refusal,
    system_resolver,
)

logger = logging.getLogger(__name__)

#: One provider call's deadline: resolution, connection, request and answer.
DEFAULT_DEADLINE_SECONDS = 15.0
#: The most of an answer SRW reads (a token or an error is a few hundred
#: bytes).
MAX_BODY_BYTES = 64 * 1024

ClientFactory = Callable[..., httpx.AsyncClient]

#: What a refused or failed call says, by reason (``{who}`` names the
#: provider): the only texts a connector's owner sees.
_TEXTS: dict[str, str] = {
    "timeout": "{who} did not answer in time",
    "unreachable": "{who} could not be reached",
    "does_not_resolve": "{who}'s host does not resolve",
    "address_refused": (
        "{who}'s address is not one this connector's projects may reach "
        "(an operator may allow a private host in "
        "connectors.providerMinting.privateHosts)"
    ),
    "certificate": "{who}'s certificate does not verify against the configured CA",
    "ca_unusable": "the configured CA certificates are not usable",
    "too_large": "{who}'s answer is larger than SRW reads",
    "malformed": "{who}'s answer is not what SRW expected",
}


class ProviderError(Exception):
    """A provider refused or did not answer. ``transient``: trying again
    later can help (a timeout, a 5xx, a rate limit). The text is fixed;
    ``reason`` names it for code and tests."""

    def __init__(self, message: str, *, transient: bool, reason: str = "") -> None:
        super().__init__(message)
        self.transient = transient
        self.reason = reason


def failure(reason: str, who: str, *, transient: bool) -> ProviderError:
    return ProviderError(
        _TEXTS[reason].format(who=who), transient=transient, reason=reason
    )


@dataclass(frozen=True)
class MintedToken:
    """A credential a provider minted. ``token`` is secret; ``handle`` is
    what revoking it needs besides the token (a bound Secret's uid)."""

    token: str
    expires_at: datetime
    handle: str | None = None

    def __repr__(self) -> str:  # never the token
        return f"MintedToken(expires_at={self.expires_at!r})"


@dataclass(frozen=True)
class ProviderNetwork:
    """Where provider calls may go on this installation: the cluster's
    ranges and the refused ones (``connectors.servicePods``), the tiers whose
    projects may reach private addresses, and the hosts the operator trusts
    (``connectors.providerMinting.privateHosts``)."""

    cluster_cidrs: tuple[str, ...] = DEFAULT_CLUSTER_CIDRS
    refused_cidrs: tuple[str, ...] = ()
    private_tiers: frozenset[str] = DEFAULT_PRIVATE_TIERS
    ipv6: bool = False
    private_hosts: frozenset[str] = frozenset()
    resolver: Resolver = field(default=system_resolver, repr=False)

    def listed(self, url: httpx.URL) -> bool:
        """Whether the operator listed this host (``host``, or ``host:port``
        with the port the call uses)."""
        host = (url.host or "").lower()
        port = url.port or (443 if url.scheme == "https" else 80)
        return host in self.private_hosts or f"{host}:{port}" in self.private_hosts

    def policy(self, *, listed: bool, allow_private: bool) -> EgressPolicy:
        if listed:
            # The operator's word: private and cluster addresses, never
            # loopback, link-local or metadata (refusal's fixed list).
            return EgressPolicy.build((), allow_private=True, ipv6=self.ipv6)
        return EgressPolicy.build(
            self.cluster_cidrs,
            allow_private=allow_private,
            ipv6=self.ipv6,
            refused_cidrs=self.refused_cidrs,
        )


def _default_client(
    *, verify: Any = True, timeout: float = DEFAULT_DEADLINE_SECONDS
) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        verify=verify,
        timeout=timeout,
        trust_env=False,
        follow_redirects=False,
        # Each call dials a checked address with a name for TLS: never reuse
        # one name's connection for another.
        limits=httpx.Limits(max_keepalive_connections=0),
    )


_state: dict[str, Any] = {"factory": _default_client, "network": ProviderNetwork()}


def configure_provider_http(factory: ClientFactory | None) -> None:
    """Install the client factory (``None``: the real one)."""
    _state["factory"] = factory or _default_client


def configure_provider_network(network: ProviderNetwork | None) -> None:
    """Install the installation's network rules (``None``: the defaults)."""
    _state["network"] = network or ProviderNetwork()


def provider_network() -> ProviderNetwork:
    return _state["network"]


def tls_context(ca_pem: str | None) -> ssl.SSLContext:
    """Verify against ``ca_pem`` only when given, else the public roots;
    ``ProviderError`` (final) for certificates that do not load."""
    if not ca_pem:
        return ssl.create_default_context()
    try:
        return ssl.create_default_context(cadata=ca_pem)
    except (ssl.SSLError, ValueError):
        raise failure("ca_unusable", "", transient=False) from None


def clean(text: Any, limit: int = 200) -> str:
    """Text safe to log: no control characters, collapsed, capped."""
    cleaned = "".join(" " if ord(ch) < 32 or ord(ch) == 127 else ch for ch in str(text))
    cleaned = " ".join(cleaned.split())
    return cleaned if len(cleaned) <= limit else cleaned[: limit - 3] + "..."


@dataclass(frozen=True)
class ProviderAnswer:
    """A provider's status and (capped) body."""

    status: int
    body: bytes = field(repr=False)

    def json(self) -> Any:
        """The body as JSON; ``ValueError`` when it is not."""
        return json.loads(self.body.decode("utf-8"))

    def message(self) -> str:
        """The answer's ``message``, cleaned, for the server log only."""
        try:
            body = self.json()
        except (ValueError, UnicodeDecodeError):
            return ""
        return clean(body.get("message")) if isinstance(body, dict) else ""


def status_transient(status: int) -> bool:
    return status == 429 or status >= 500


def parse_time(value: Any) -> datetime:
    """An RFC 3339 timestamp, as an aware datetime."""
    if not isinstance(value, str) or not value:
        raise ValueError("no timestamp")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


async def _targets(
    url: httpx.URL, *, who: str, allow_private: bool, sni_hostname: str | None
) -> list[tuple[httpx.URL, dict[str, str], dict[str, Any]]]:
    """The checked addresses to dial in turn, with the headers and the TLS
    name each needs."""
    network = provider_network()
    host = url.host
    netloc = url.netloc.decode("ascii")
    policy = network.policy(listed=network.listed(url), allow_private=allow_private)
    try:
        addresses: Iterable[Any] = [ipaddress.ip_address(host)]
        literal = True
    except ValueError:
        literal = False
        try:
            answers = await network.resolver(host, network.ipv6)
        except (OSError, UnicodeError) as exc:
            logger.info("Provider host %s does not resolve: %s", netloc, clean(exc))
            raise failure("does_not_resolve", who, transient=True) from None
        addresses = sorted(
            {ipaddress.ip_address(answer.split("%")[0]) for answer in answers},
            key=lambda a: (a.version, int(a)),
        )
        if not network.ipv6:
            addresses = [a for a in addresses if a.version == 4]
    addresses = list(addresses)
    if not addresses:
        raise failure("does_not_resolve", who, transient=True)
    for address in addresses:
        reason = refusal(address, policy)
        if reason is not None:
            logger.warning(
                "Provider call to %s refused: %s %s", netloc, address, reason
            )
            raise failure("address_refused", who, transient=False)
    extensions: dict[str, Any] = {"sni_hostname": sni_hostname} if sni_hostname else {}
    if literal:
        return [(url, {"Connection": "close"}, extensions)]
    return [
        (
            url.copy_with(host=str(address)),
            {"Host": netloc, "Connection": "close"},
            {"sni_hostname": sni_hostname or host},
        )
        for address in addresses
    ]


async def _exchange(
    client: httpx.AsyncClient,
    method: str,
    url: httpx.URL,
    *,
    headers: dict[str, str],
    json_body: Any,
    extensions: dict[str, Any],
    who: str,
) -> ProviderAnswer:
    async with client.stream(
        method, url, headers=headers, json=json_body, extensions=extensions
    ) as response:
        body = bytearray()
        async for chunk in response.aiter_bytes():
            body.extend(chunk)
            if len(body) > MAX_BODY_BYTES:
                logger.warning(
                    "Provider answer from %s exceeds %d bytes",
                    headers.get("Host") or url.host,
                    MAX_BODY_BYTES,
                )
                raise failure("too_large", who, transient=False)
        return ProviderAnswer(response.status_code, bytes(body))


def transport_error(exc: Exception, who: str) -> ProviderError:
    """A request that got no answer: an untrusted certificate is final,
    anything else may pass. The exception's text is logged, never shown."""
    text = str(exc)
    logger.info("Provider call to %s failed: %s", who, clean(text))
    if "CERTIFICATE_VERIFY_FAILED" in text or isinstance(
        getattr(exc, "__cause__", None), ssl.SSLCertVerificationError
    ):
        return failure("certificate", who, transient=False)
    if isinstance(exc, httpx.TimeoutException):
        return failure("timeout", who, transient=True)
    return failure("unreachable", who, transient=True)


async def provider_request(
    method: str,
    url: str,
    *,
    who: str,
    headers: dict[str, str],
    json_body: Any = None,
    ca_pem: str | None = None,
    allow_private: bool = False,
    sni_hostname: str | None = None,
    deadline: float | None = None,
) -> ProviderAnswer:
    """One provider call under one deadline (``None``:
    :data:`DEFAULT_DEADLINE_SECONDS`), to a checked, pinned address, reading
    at most :data:`MAX_BODY_BYTES`; ``ProviderError`` otherwise."""
    if deadline is None:
        deadline = DEFAULT_DEADLINE_SECONDS
    verify = tls_context(ca_pem)
    target = httpx.URL(url)
    try:
        async with asyncio.timeout(deadline):
            candidates = await _targets(
                target, who=who, allow_private=allow_private, sni_hostname=sni_hostname
            )
            async with _state["factory"](verify=verify, timeout=deadline) as client:
                for index, (dial, pinned, extensions) in enumerate(candidates):
                    try:
                        return await _exchange(
                            client,
                            method,
                            dial,
                            headers={**headers, **pinned},
                            json_body=json_body,
                            extensions=extensions,
                            who=who,
                        )
                    except (httpx.ConnectError, httpx.ConnectTimeout) as exc:
                        if index == len(candidates) - 1:
                            raise
                        logger.info("Provider address did not connect: %s", clean(exc))
                raise failure("unreachable", who, transient=True)
    except TimeoutError:
        logger.info("Provider call to %s exceeded its %.0fs deadline", who, deadline)
        raise failure("timeout", who, transient=True) from None
    except httpx.HTTPError as exc:
        raise transport_error(exc, who) from None


__all__ = [
    "ClientFactory",
    "DEFAULT_DEADLINE_SECONDS",
    "MAX_BODY_BYTES",
    "MintedToken",
    "ProviderAnswer",
    "ProviderError",
    "ProviderNetwork",
    "clean",
    "configure_provider_http",
    "configure_provider_network",
    "failure",
    "parse_time",
    "provider_network",
    "provider_request",
    "status_transient",
    "tls_context",
    "transport_error",
]
