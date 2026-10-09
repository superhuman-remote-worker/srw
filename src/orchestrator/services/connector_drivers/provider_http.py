"""What the provider-minted drivers' HTTP calls share (connector drivers C5).

SRW mints short-lived credentials at a provider from its own process: a
Kubernetes API server's TokenRequest (``token_request``) and GitHub's App
installation tokens (``github_app``). The provider's address comes from a
connector, so every call is held to the same rules:

* **One deadline per call.** Address resolution, the connection, the
  request and the whole answer run under one ``asyncio.timeout``
  (:data:`DEFAULT_DEADLINE_SECONDS`): a host that trickles its answer a byte
  at a time cannot hold a delivery, a claim or the leader's revoke sweep.
* **A capped answer.** SRW asks for no content coding
  (``Accept-Encoding: identity``), refuses an answer that names one anyway
  (a compressed body is never decoded: a few kilobytes of zstd or gzip can
  expand to gigabytes), and reads the raw body as it streams, refusing it
  past :data:`MAX_BODY_BYTES`.
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
* **Its own resolver threads.** A lookup runs on a small pool of its own
  (:data:`RESOLVER_THREADS`); a lookup the system resolver never answers
  holds one of those threads, never one of the process's shared executor,
  and with every thread busy a call fails at once.

A connector's Test reaches user-chosen hosts from this process too, and is
held to the same rules: a repository's forge API goes through
:func:`provider_request`, and its SSH endpoint dials an address
:func:`checked_addresses` passed (``workspace_ssh_connector``), on the tier
:func:`tier_allows_private` reads.

Tests replace the client factory and the network (resolver) with
:func:`configure_provider_http` and :func:`configure_provider_network`.
"""

from __future__ import annotations

import asyncio
import ipaddress
import json
import logging
import socket
import ssl
import threading
from collections.abc import AsyncIterator, Callable, Iterable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import httpx

from orchestrator.services.connector_egress import (
    DEFAULT_CLUSTER_CIDRS,
    DEFAULT_PRIVATE_TIERS,
    EgressPolicy,
    IPAddress,
    Resolver,
    private_addresses_allowed,
    refusal,
)

logger = logging.getLogger(__name__)

#: One provider call's deadline: resolution, connection, request and answer.
DEFAULT_DEADLINE_SECONDS = 15.0
#: The most of an answer SRW reads (a token or an error is a few hundred
#: bytes), on the wire: no content coding is ever decoded.
MAX_BODY_BYTES = 64 * 1024
#: Threads the provider calls' name lookups run on (and lookups at once).
RESOLVER_THREADS = 4

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
    ``reason`` names it for code and tests. ``reached``: the request may have
    reached the provider (``False`` only when it surely did not: the name
    did not resolve, the address was refused, the connection or its TLS
    handshake failed)."""

    def __init__(
        self, message: str, *, transient: bool, reason: str = "", reached: bool = True
    ) -> None:
        super().__init__(message)
        self.transient = transient
        self.reason = reason
        self.reached = reached


class UnrevokedToken(ProviderError):
    """A credential the provider minted but SRW refused (it grants more
    than was asked) and could not revoke at once: ``minted`` is kept, so its
    record is revoked again until it expires, never dropped."""

    def __init__(self, message: str, *, minted: MintedToken) -> None:
        super().__init__(message, transient=False, reason="overbroad")
        self.minted = minted


def failure(
    reason: str, who: str, *, transient: bool, reached: bool = True
) -> ProviderError:
    return ProviderError(
        _TEXTS[reason].format(who=who),
        transient=transient,
        reason=reason,
        reached=reached,
    )


_RESOLVER_POOL = ThreadPoolExecutor(
    max_workers=RESOLVER_THREADS, thread_name_prefix="srw-provider-resolve"
)
_RESOLVER_SLOTS = threading.BoundedSemaphore(RESOLVER_THREADS)


def _lookup(host: str, family: int) -> list[str]:
    try:
        infos = socket.getaddrinfo(host, None, family, socket.SOCK_STREAM)
    finally:
        _RESOLVER_SLOTS.release()
    return [str(info[4][0]) for info in infos]


async def provider_resolver(host: str, ipv6: bool) -> Sequence[str]:
    """A and (on dual-stack) AAAA answers, on the provider calls' own
    threads; ``OSError`` at once when all of them are taken (a hung system
    resolver), never a queue behind them."""
    if not _RESOLVER_SLOTS.acquire(blocking=False):
        raise OSError("every provider resolver thread is busy")
    family = socket.AF_UNSPEC if ipv6 else socket.AF_INET
    loop = asyncio.get_running_loop()
    try:
        future = loop.run_in_executor(_RESOLVER_POOL, _lookup, host, family)
    except BaseException:
        _RESOLVER_SLOTS.release()
        raise
    return await future


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
    resolver: Resolver = field(default=provider_resolver, repr=False)

    def listed(self, url: httpx.URL) -> bool:
        """Whether the operator listed this host (``host``, or ``host:port``
        with the port the call uses)."""
        port = url.port or (443 if url.scheme == "https" else 80)
        return self.lists(url.host or "", port)

    def lists(self, host: str, port: int) -> bool:
        """Whether the operator listed ``host`` (alone, or with ``port``)."""
        host = host.lower()
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


async def tier_allows_private(store: Any, connector_id: Any) -> bool:
    """Whether a connector's projects may reach private addresses, read on
    ``store`` (their network tier, as a driver pod's egress and a mint
    decide). ``False`` without a store or an answer: a Test that cannot
    read the tier reaches public addresses only."""
    if store is None or not connector_id:
        return False
    try:
        async with store.acquire() as conn:
            return bool(
                await private_addresses_allowed(
                    conn,
                    str(connector_id),
                    private_tiers=provider_network().private_tiers,
                )
            )
    except Exception:
        logger.warning(
            "Connector %s's network tier could not be read; private addresses "
            "are refused",
            clean(connector_id, 64),
            exc_info=True,
        )
        return False


def tls_context(ca_pem: str | None) -> ssl.SSLContext:
    """Verify against ``ca_pem`` only when given, else the public roots;
    ``ProviderError`` (final) for certificates that do not load."""
    if not ca_pem:
        return ssl.create_default_context()
    try:
        return ssl.create_default_context(cadata=ca_pem)
    except (ssl.SSLError, ValueError):
        raise failure("ca_unusable", "", transient=False, reached=False) from None


def clean(text: Any, limit: int = 200) -> str:
    """Text safe to log: no control characters, collapsed, capped."""
    cleaned = "".join(" " if ord(ch) < 32 or ord(ch) == 127 else ch for ch in str(text))
    cleaned = " ".join(cleaned.split())
    return cleaned if len(cleaned) <= limit else cleaned[: limit - 3] + "..."


@dataclass(frozen=True)
class ProviderAnswer:
    """A provider's status, (capped) body and headers (by lower-case name;
    a repeated header's values joined by commas)."""

    status: int
    body: bytes = field(repr=False)
    headers: Mapping[str, str] = field(default_factory=dict, repr=False)

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


def status_class(status: int) -> str:
    """How an answer SRW did not expect is named to a connector's owner:
    the status class, never the provider's words."""
    if 300 <= status < 400:
        return "a redirect, which SRW does not follow"
    if 400 <= status < 500:
        return "an HTTP 4xx answer"
    return f"an unexpected HTTP {status} answer"


def parse_time(value: Any) -> datetime:
    """An RFC 3339 timestamp, as an aware datetime."""
    if not isinstance(value, str) or not value:
        raise ValueError("no timestamp")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


async def checked_addresses(
    host: str, port: int, *, who: str, allow_private: bool
) -> tuple[list[IPAddress], bool]:
    """``host`` resolved once, every answer checked: ``(addresses, literal)``,
    the addresses to dial in turn (``literal``: ``host`` was one). A
    :class:`ProviderError` (``does_not_resolve`` or ``address_refused``)
    otherwise, before anything connects."""
    network = provider_network()
    label = clean(f"{host}:{port}", 300)
    policy = network.policy(
        listed=network.lists(host, port), allow_private=allow_private
    )
    try:
        addresses: Iterable[Any] = [ipaddress.ip_address(host)]
        literal = True
    except ValueError:
        literal = False
        try:
            answers = await network.resolver(host, network.ipv6)
        except (OSError, UnicodeError) as exc:
            logger.info("Provider host %s does not resolve: %s", label, clean(exc))
            raise failure(
                "does_not_resolve", who, transient=True, reached=False
            ) from None
        addresses = sorted(
            {ipaddress.ip_address(answer.split("%")[0]) for answer in answers},
            key=lambda a: (a.version, int(a)),
        )
        if not network.ipv6:
            addresses = [a for a in addresses if a.version == 4]
    addresses = list(addresses)
    if not addresses:
        raise failure("does_not_resolve", who, transient=True, reached=False)
    for address in addresses:
        reason = refusal(address, policy)
        if reason is not None:
            logger.warning("Provider call to %s refused: %s %s", label, address, reason)
            raise failure("address_refused", who, transient=False, reached=False)
    return addresses, literal


async def _targets(
    url: httpx.URL, *, who: str, allow_private: bool, sni_hostname: str | None
) -> list[tuple[httpx.URL, dict[str, str], dict[str, Any]]]:
    """The checked addresses to dial in turn, with the headers and the TLS
    name each needs."""
    host = url.host
    netloc = url.netloc.decode("ascii")
    port = url.port or (443 if url.scheme == "https" else 80)
    addresses, literal = await checked_addresses(
        host, port, who=who, allow_private=allow_private
    )
    extensions: dict[str, Any] = {"sni_hostname": sni_hostname} if sni_hostname else {}
    # No content coding: SRW never decodes one (see the module docstring).
    plain = {"Connection": "close", "Accept-Encoding": "identity"}
    if literal:
        return [(url, plain, extensions)]
    return [
        (
            url.copy_with(host=str(address)),
            {"Host": netloc, **plain},
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
        coding = response.headers.get("content-encoding", "").strip().lower()
        if coding not in ("", "identity"):
            # Asked for identity, answered with a coding: never decoded.
            logger.warning(
                "Provider answer from %s has Content-Encoding %s",
                headers.get("Host") or url.host,
                clean(coding, 40),
            )
            raise failure("malformed", who, transient=False)
        body = bytearray()
        # The raw bytes on the wire (chunked framing removed, no content
        # decoding): the cap holds for what SRW holds in memory.
        async for chunk in _raw(response):
            body.extend(chunk)
            if len(body) > MAX_BODY_BYTES:
                logger.warning(
                    "Provider answer from %s exceeds %d bytes",
                    headers.get("Host") or url.host,
                    MAX_BODY_BYTES,
                )
                raise failure("too_large", who, transient=False)
        return ProviderAnswer(
            response.status_code,
            bytes(body),
            {name.lower(): value for name, value in response.headers.items()},
        )


async def _raw(response: httpx.Response) -> AsyncIterator[bytes]:
    """The answer's raw bytes as they stream. A transport that read the
    whole answer itself (an in-memory one, in tests) has nothing left to
    stream: its bytes are the identity-coded body already checked."""
    if response.is_stream_consumed:
        yield response.content
        return
    async for chunk in response.aiter_raw():
        yield chunk


def transport_error(exc: Exception, who: str) -> ProviderError:
    """A request that got no answer: an untrusted certificate is final,
    anything else may pass. The exception's text is logged, never shown."""
    text = str(exc)
    logger.info("Provider call to %s failed: %s", who, clean(text))
    if "CERTIFICATE_VERIFY_FAILED" in text or isinstance(
        getattr(exc, "__cause__", None), ssl.SSLCertVerificationError
    ):
        # The TLS handshake failed: nothing was sent.
        return failure("certificate", who, transient=False, reached=False)
    connected = not isinstance(exc, (httpx.ConnectError, httpx.ConnectTimeout))
    if isinstance(exc, httpx.TimeoutException):
        return failure("timeout", who, transient=True, reached=connected)
    return failure("unreachable", who, transient=True, reached=connected)


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
    resolving = True
    try:
        async with asyncio.timeout(deadline):
            candidates = await _targets(
                target, who=who, allow_private=allow_private, sni_hostname=sni_hostname
            )
            resolving = False
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
        raise failure("timeout", who, transient=True, reached=not resolving) from None
    except httpx.HTTPError as exc:
        raise transport_error(exc, who) from None


__all__ = [
    "ClientFactory",
    "DEFAULT_DEADLINE_SECONDS",
    "MAX_BODY_BYTES",
    "RESOLVER_THREADS",
    "MintedToken",
    "ProviderAnswer",
    "ProviderError",
    "ProviderNetwork",
    "UnrevokedToken",
    "checked_addresses",
    "clean",
    "configure_provider_http",
    "configure_provider_network",
    "failure",
    "parse_time",
    "provider_network",
    "provider_request",
    "provider_resolver",
    "status_class",
    "status_transient",
    "tier_allows_private",
    "tls_context",
    "transport_error",
]
