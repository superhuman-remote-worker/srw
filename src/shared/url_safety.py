"""Address and URL rules for requests to hosts SRW does not choose.

One classifier, two policies:

- A server-side fetch (the OCI registry resolver in :mod:`shared.oci_registry`,
  the agent's citation-engine page fetch) dials public addresses only.
  :func:`check_public_http_url` takes http(s) URLs only, resolves the host
  (every A and AAAA answer) and refuses the URL when any answer, or the
  literal address the URL names, is loopback, private, shared (CGNAT),
  link-local, multicast, reserved, unspecified, or an IPv4-mapped,
  IPv4-compatible or translated (NAT64, 6to4) form of one.
- The agent's browser navigates from inside the job's own workspace, where
  loopback and private addresses are the workspace's own services. It
  refuses only the never-dialled ranges (link-local and cloud metadata,
  multicast, reserved) by literal address:
  ``address_refusal(literal_ip(host), private_allowed=True)``.

:func:`literal_ip` reads every spelling of an IPv4 address that browsers and
``getaddrinfo`` accept (``2852039166``, ``0xa9fea9fe``, ``0251.0376.0251.0376``,
``127.1``, percent-encoded or full-width digits), and :func:`url_host` reads
the host the way requests and browsers do (a backslash ends the authority),
so a URL cannot name a refused address in a form the check does not
recognise. A caller checks the URL its HTTP client will send: the citation
engine checks the URL ``requests`` prepared, not the one the model wrote.

Standard library only.
"""

from __future__ import annotations

import ipaddress
import re
import socket
from collections.abc import Callable, Iterable
from urllib.parse import SplitResult, unquote, urlsplit

_Address = ipaddress.IPv4Address | ipaddress.IPv6Address
_Network = ipaddress.IPv4Network | ipaddress.IPv6Network
#: Never dialled, whatever a host is listed as.
_NEVER: tuple[_Network, ...] = tuple(
    ipaddress.ip_network(cidr)
    for cidr in (
        "0.0.0.0/8",
        "169.254.0.0/16",  # link-local, cloud metadata
        "168.63.129.16/32",  # Azure wireserver
        "224.0.0.0/4",
        "240.0.0.0/4",
        "::/128",
        "fe80::/10",
        "fd00:ec2::254/128",  # AWS metadata over IPv6
        "ff00::/8",
    )
)
#: Public by the registry of special addresses, yet able to carry a private
#: IPv4 address: only for listed hosts, and never one whose embedded IPv4
#: address is never dialled.
_NAT64 = (ipaddress.ip_network("64:ff9b::/96"), ipaddress.ip_network("64:ff9b:1::/48"))
_6TO4 = ipaddress.ip_network("2002::/16")
_TRANSLATED: tuple[_Network, ...] = (*_NAT64, _6TO4)
#: Deprecated IPv4-compatible IPv6 (``::a.b.c.d``), never dialled (``::``
#: and ``::1`` excepted, which are not IPv4 addresses).
_IPV4_COMPATIBLE = ipaddress.ip_network("::/96")
#: RFC 6598 shared address space (carrier-grade NAT, the tailnet range).
_SHARED_ADDRESS_SPACE = ipaddress.ip_network("100.64.0.0/10")
#: The characters an IPv4 address can be spelled with (decimal, octal, hex).
_IPV4_SPELLING = re.compile(r"[0-9a-fx.]+")


class UnsafeURLError(ValueError):
    """A URL refused by scheme or address. The message says why, for the model."""


def _embedded_ipv4(address: _Address) -> ipaddress.IPv4Address | None:
    """The IPv4 address a NAT64 or 6to4 address carries, or ``None``."""
    if address.version != 6:
        return None
    value = int(address)
    if any(address in network for network in _NAT64):
        return ipaddress.IPv4Address(value & 0xFFFFFFFF)
    if address in _6TO4:
        return ipaddress.IPv4Address((value >> 80) & 0xFFFFFFFF)
    return None


def address_refusal(
    address: _Address,
    *,
    private_allowed: bool = False,
    refused: Iterable[_Network] = (),
) -> str | None:
    """Why a request may not dial ``address``, or ``None``.

    Link-local, metadata, multicast and unspecified addresses never.
    Loopback, private, shared (CGNAT), translated and every other non-global
    address, and the ``refused`` networks (the cluster's ranges), only when
    ``private_allowed`` (a host the operator listed, or the browser inside
    its own workspace).
    """
    mapped = getattr(address, "ipv4_mapped", None)
    if mapped is not None:
        address = mapped
    if address.version == 6 and address in _IPV4_COMPATIBLE and int(address) > 1:
        return "is an IPv4-compatible address"
    embedded = _embedded_ipv4(address)
    for candidate in (address, embedded):
        if candidate is None:
            continue
        for network in _NEVER:
            if network.version == candidate.version and candidate in network:
                return "is link-local, metadata, multicast or unspecified"
    if private_allowed:
        return None
    for network in refused:
        if network.version == address.version and address in network:
            return "is inside the cluster's ranges"
    for network in _TRANSLATED:
        if network.version == address.version and address in network:
            return "is a translated address"
    if not address.is_global:
        return "is not a public address"
    return None


def public_address_refusal(address: _Address) -> str | None:
    """Why a server-side fetch may not dial ``address``, or ``None``.

    :func:`address_refusal` without exceptions, plus an explicit check for
    the shared (CGNAT) range: ``is_global`` already excludes it, but
    ``is_private`` does not, so the rule does not rest on one property.
    """
    reason = address_refusal(address)
    if reason is not None:
        return reason
    unmapped = getattr(address, "ipv4_mapped", None) or address
    if unmapped.version == 4 and unmapped in _SHARED_ADDRESS_SPACE:
        return "is a shared (CGNAT) address"
    return None


def url_host(url: str) -> SplitResult:
    """``url`` split the way HTTP clients and browsers find its host.

    :func:`urllib.parse.urlsplit` reads ``http://10.0.0.1\\@public.example/``
    as host ``public.example``; urllib3 (under requests) and browsers end the
    authority at the backslash and dial ``10.0.0.1``. For http(s) a
    backslash is a path separator (WHATWG), so it is read as one here.

    Raises:
        ValueError: the URL does not parse (a bad port or IPv6 literal).
    """
    parsed = urlsplit(url.replace("\\", "/"))
    parsed.port  # noqa: B018 -- raises ValueError for a bad port
    return parsed


def literal_ip(host: str) -> _Address | None:
    """The address ``host`` spells, or ``None`` when it is a name.

    ``host`` is a URL's host as :func:`urllib.parse.urlsplit` returns it (an
    IPv6 literal without brackets; a zone is ignored). It is percent-decoded
    and IDNA-mapped first, as browsers do (``%31%30.0.0.1``, full-width
    digits and dots), and IPv4 is then read the way ``inet_aton``,
    ``getaddrinfo`` and browsers read it: dotted, decimal, octal, hex and
    short forms, with one trailing dot.
    """
    candidate = unquote(host)
    if ":" in candidate:
        try:
            return ipaddress.IPv6Address(candidate.split("%", 1)[0])
        except ValueError:
            return None
    if not candidate.isascii():
        try:
            candidate = candidate.encode("idna").decode("ascii")
        except UnicodeError:
            return None
    candidate = candidate.lower()
    if candidate.endswith("."):
        candidate = candidate[:-1]
    # inet_aton stops at whitespace ("127.0.0.1 x" parses), so only a
    # string made entirely of address characters is offered to it.
    if not candidate or not _IPV4_SPELLING.fullmatch(candidate):
        return None
    try:
        return ipaddress.IPv4Address(socket.inet_aton(candidate))
    except OSError:
        return None


Resolver = Callable[[str, int], Iterable[str]]


def system_resolver(host: str, port: int) -> list[str]:
    """Every A and AAAA answer of the system resolver for ``host``."""
    infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    return [str(info[4][0]) for info in infos]


def check_public_http_url(url: str, *, resolver: Resolver = system_resolver) -> str:
    """Return ``url`` when a server-side fetch may request it.

    It must be http or https, and its host, a literal address or every
    address the name resolves to, must be public
    (:func:`public_address_refusal`).

    Raises:
        UnsafeURLError: the URL is refused; the message names the host and
            why. A name that does not resolve raises the resolver's
            ``OSError`` instead: that is a failed fetch, not a refusal.

    The check resolves the name itself. A client that resolves it again
    when it connects leaves a DNS-rebinding window (a name that answers
    public here and private there); a caller closes it by dialling the
    checked address with the name kept for Host and TLS, as
    :mod:`shared.oci_registry` does.
    """
    try:
        parsed = url_host(url)
    except ValueError as exc:
        raise UnsafeURLError(f"refused: {url!r} is not a valid URL") from exc
    port = parsed.port
    scheme = parsed.scheme.lower()
    if scheme not in ("http", "https"):
        shown = f"{scheme}:// URLs are" if scheme else f"{url!r} has no scheme, it is"
        raise UnsafeURLError(f"refused: {shown} not fetched (http and https only)")
    host = parsed.hostname
    if not host:
        raise UnsafeURLError(f"refused: {url!r} has no host")
    if any(char.isspace() or not char.isprintable() for char in host):
        raise UnsafeURLError(f"refused: {host!r} is not a valid host name")

    literal = literal_ip(host)
    if literal is not None:
        reason = public_address_refusal(literal)
        if reason is not None:
            raise UnsafeURLError(f"refused: {host} {reason}")
        return url

    default_port = 443 if scheme == "https" else 80
    answers = list(resolver(host, port or default_port))
    if not answers:
        raise OSError(f"{host} does not resolve")
    for answer in answers:
        try:
            address = ipaddress.ip_address(answer.split("%", 1)[0])
        except ValueError:
            raise UnsafeURLError(
                f"refused: {host} resolves to an unrecognised address"
            ) from None
        reason = public_address_refusal(address)
        if reason is not None:
            raise UnsafeURLError(
                f"refused: {host} resolves to an address that {reason}"
            )
    return url


__all__ = [
    "Resolver",
    "UnsafeURLError",
    "address_refusal",
    "check_public_http_url",
    "literal_ip",
    "public_address_refusal",
    "system_resolver",
    "url_host",
]
