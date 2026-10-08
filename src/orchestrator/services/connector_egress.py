"""Driver egress pinned per pod (connector drivers D5, "Reachability").

NetworkPolicy has no hostnames and k3s's kube-router has no FQDN rules, so a
driver pod's declared egress is pinned when the pod is created:

1. each declared host (``spec.egress``; ``${config.<key>}`` from the
   connector's config) is resolved once, A records (AAAA too on dual-stack
   clusters);
2. every address is checked: the cluster's pod and service ranges, loopback,
   link-local (cloud metadata included), multicast and reserved ranges are
   always refused; private and home ranges (RFC 1918, CGNAT, IPv6 ULA) only
   when the connector's projects' network tier allows them;
3. the same answer goes into the pod's NetworkPolicy ``ipBlock``s and its
   ``hostAliases``, so the pod never resolves the name itself (no rebinding,
   no disagreeing resolver) and TLS still checks the name it dials;
4. DNS stays off unless the driver declares ``needs_dns``; then only the
   cluster resolver pods on port 53, and names are unrestricted.

The pins and their resolution time are recorded with the pod; the capability
matrix and the connector's egress view show them ("enforced"). Removing an
address from a policy does not close an established connection: re-pinning
is housekeeping, not revocation.

Design: knowledge-base/knowledge/features/connector_drivers.md, "Reachability"
and lane 8 §1.
"""

from __future__ import annotations

import asyncio
import ipaddress
import re
import socket
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from shared.connectors.contract import EgressRule

IPNetwork = ipaddress.IPv4Network | ipaddress.IPv6Network
IPAddress = ipaddress.IPv4Address | ipaddress.IPv6Address

#: k3s's default pod and service ranges (``connectors.servicePods.clusterCidrs``).
DEFAULT_CLUSTER_CIDRS: tuple[str, ...] = ("10.42.0.0/16", "10.43.0.0/16")
#: The network tiers whose projects may reach private and home addresses.
DEFAULT_PRIVATE_TIERS: frozenset[str] = frozenset({"home-allowed"})
#: A host pinned to more addresses than this is shown as "may break".
MANY_ADDRESSES = 16
#: The cluster resolver pods a driver that declares ``needs_dns`` may reach.
DNS_PEER = {
    "namespaceSelector": {
        "matchLabels": {"kubernetes.io/metadata.name": "kube-system"}
    },
    "podSelector": {"matchLabels": {"k8s-app": "kube-dns"}},
}

_CONFIG_REFERENCE = re.compile(r"\$\{config\.([a-z][a-z0-9_]*)\}\Z")
_HOSTNAME = re.compile(
    r"(?=.{1,253}\Z)[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?"
    r"(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)*\Z"
)
_ALWAYS_REFUSED: tuple[tuple[IPNetwork, str], ...] = tuple(
    (ipaddress.ip_network(cidr), reason)
    for cidr, reason in (
        ("0.0.0.0/8", "is an unspecified address"),
        ("127.0.0.0/8", "is loopback"),
        ("169.254.0.0/16", "is link-local (cloud metadata)"),
        ("224.0.0.0/4", "is multicast"),
        ("240.0.0.0/4", "is reserved"),
        ("255.255.255.255/32", "is broadcast"),
        ("::/128", "is an unspecified address"),
        ("::1/128", "is loopback"),
        ("fe80::/10", "is link-local"),
        ("ff00::/8", "is multicast"),
    )
)
_PRIVATE: tuple[IPNetwork, ...] = tuple(
    ipaddress.ip_network(cidr)
    for cidr in (
        "10.0.0.0/8",
        "172.16.0.0/12",
        "192.168.0.0/16",
        "100.64.0.0/10",
        "fc00::/7",
    )
)


class EgressRefused(ValueError):
    """A declared destination SRW will not open for this pod."""


@dataclass(frozen=True)
class EgressPolicy:
    """What a pod's egress may reach on this installation."""

    cluster_cidrs: tuple[IPNetwork, ...] = tuple(
        ipaddress.ip_network(c) for c in DEFAULT_CLUSTER_CIDRS
    )
    allow_private: bool = False
    ipv6: bool = False

    @classmethod
    def build(
        cls, cluster_cidrs: Iterable[str], *, allow_private: bool, ipv6: bool = False
    ) -> EgressPolicy:
        return cls(
            cluster_cidrs=tuple(ipaddress.ip_network(c) for c in cluster_cidrs),
            allow_private=allow_private,
            ipv6=ipv6,
        )


@dataclass(frozen=True)
class PinnedHost:
    """One declared destination as enforced: its addresses and ports."""

    host: str
    addresses: tuple[str, ...]
    ports: tuple[int, ...]
    protocol: str = "tcp"
    #: ``True`` when the host was an address or a CIDR (no hostAlias).
    literal: bool = False

    @property
    def many_addresses(self) -> bool:
        return len(self.addresses) > MANY_ADDRESSES

    def cidrs(self) -> list[str]:
        out = []
        for address in self.addresses:
            network = ipaddress.ip_network(address, strict=False)
            out.append(str(network))
        return out

    def record(self) -> dict[str, Any]:
        return {
            "host": self.host,
            "addresses": list(self.addresses),
            "ports": list(self.ports),
            "protocol": self.protocol,
            "literal": self.literal,
            "many_addresses": self.many_addresses,
        }


@dataclass(frozen=True)
class EgressPins:
    """A pod's resolved egress, written into its policy and hostAliases."""

    hosts: tuple[PinnedHost, ...]
    resolved_at: datetime
    dns: bool = False
    dns_reason: str | None = None
    private_allowed: bool = False

    def network_policy_egress(self) -> list[dict[str, Any]]:
        """``egress`` rules: one per pinned host, plus DNS when declared."""
        rules: list[dict[str, Any]] = []
        for pinned in self.hosts:
            rules.append(
                {
                    "to": [{"ipBlock": {"cidr": cidr}} for cidr in pinned.cidrs()],
                    "ports": [
                        {"protocol": pinned.protocol.upper(), "port": port}
                        for port in pinned.ports
                    ],
                }
            )
        if self.dns:
            rules.append(
                {
                    "to": [dict(DNS_PEER)],
                    "ports": [
                        {"protocol": "UDP", "port": 53},
                        {"protocol": "TCP", "port": 53},
                    ],
                }
            )
        return rules

    def host_aliases(self) -> list[dict[str, Any]]:
        """``hostAliases``: every pinned name at its resolved addresses."""
        by_address: dict[str, list[str]] = {}
        for pinned in self.hosts:
            if pinned.literal:
                continue
            for address in pinned.addresses:
                names = by_address.setdefault(address, [])
                if pinned.host not in names:
                    names.append(pinned.host)
        return [
            {"ip": address, "hostnames": names}
            for address, names in sorted(by_address.items())
        ]

    def record(self) -> dict[str, Any]:
        return {
            "hosts": [pinned.record() for pinned in self.hosts],
            "dns": "cluster_resolver" if self.dns else "none",
            "dns_reason": self.dns_reason,
            "private_allowed": self.private_allowed,
            "resolved_at": self.resolved_at.isoformat(),
        }


def _config_value(config: Mapping[str, Any], text: str) -> Any:
    match = _CONFIG_REFERENCE.fullmatch(text)
    if match is None:
        return text
    key = match.group(1)
    if key not in config:
        raise EgressRefused(f"the egress rule names config.{key}, which is not set")
    return config[key]


def expand_rule(
    rule: EgressRule, config: Mapping[str, Any]
) -> tuple[str, tuple[int, ...]]:
    """The host and ports of one declared rule for one connector's config."""
    host = _config_value(config, rule.host)
    if not isinstance(host, str) or not host:
        raise EgressRefused(f"the egress host {rule.host} is not a host name")
    host = host.strip().lower().rstrip(".")
    ports: list[int] = []
    for raw in rule.ports:
        value = _config_value(config, raw) if isinstance(raw, str) else raw
        if isinstance(value, str) and value.isdigit():
            value = int(value)
        if isinstance(value, bool) or not isinstance(value, int):
            raise EgressRefused(f"the egress port {raw} is not a port number")
        if not 1 <= value <= 65535:
            raise EgressRefused(f"the egress port {value} is out of range")
        if value not in ports:
            ports.append(value)
    return host, tuple(ports)


def refusal(address: IPAddress | IPNetwork, policy: EgressPolicy) -> str | None:
    """Why ``address`` (or a whole network) may not be pinned, or ``None``."""
    network = (
        address
        if isinstance(address, (ipaddress.IPv4Network, ipaddress.IPv6Network))
        else ipaddress.ip_network(address)
    )
    mapped = getattr(network.network_address, "ipv4_mapped", None)
    if mapped is not None:
        network = ipaddress.ip_network(f"{mapped}/{max(0, network.prefixlen - 96)}")
    for cluster in policy.cluster_cidrs:
        if cluster.version == network.version and network.overlaps(cluster):
            return "is inside the cluster's pod or service range"
    for refused, reason in _ALWAYS_REFUSED:
        if refused.version == network.version and network.overlaps(refused):
            return reason
    if not policy.allow_private:
        for private in _PRIVATE:
            if private.version == network.version and network.overlaps(private):
                return "is private, and the project's network tier allows no private addresses"
    return None


Resolver = Callable[[str, bool], Awaitable[Sequence[str]]]


async def system_resolver(host: str, ipv6: bool) -> Sequence[str]:
    """A and (on dual-stack) AAAA answers of the orchestrator's resolver."""
    loop = asyncio.get_running_loop()
    family = socket.AF_UNSPEC if ipv6 else socket.AF_INET
    infos = await loop.getaddrinfo(host, None, family=family, type=socket.SOCK_STREAM)
    return [str(info[4][0]) for info in infos]


async def pin_egress(
    rules: Sequence[EgressRule],
    config: Mapping[str, Any],
    *,
    policy: EgressPolicy,
    needs_dns: str | None = None,
    resolver: Resolver = system_resolver,
    now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
) -> EgressPins:
    """Resolve and check every declared destination once; raise
    :class:`EgressRefused` naming the first one SRW will not open."""
    pinned: list[PinnedHost] = []
    for rule in rules:
        host, ports = expand_rule(rule, config)
        literal = True
        try:
            network = ipaddress.ip_network(host, strict=True)
        except ValueError:
            network = None
        if network is not None:
            reason = refusal(network, policy)
            if reason:
                raise EgressRefused(f"{host} {reason}")
            addresses = (
                str(network)
                if network.num_addresses > 1
                else str(network.network_address),
            )
        else:
            if not _HOSTNAME.fullmatch(host):
                raise EgressRefused(f"{host!r} is not a host name, address or CIDR")
            literal = False
            try:
                answers = await resolver(host, policy.ipv6)
            except (OSError, UnicodeError) as exc:
                raise EgressRefused(f"{host} does not resolve ({exc})") from exc
            parsed = sorted(
                {ipaddress.ip_address(answer.split("%")[0]) for answer in answers},
                key=lambda a: (a.version, int(a)),
            )
            if not policy.ipv6:
                parsed = [address for address in parsed if address.version == 4]
            if not parsed:
                raise EgressRefused(f"{host} does not resolve to an address")
            for address in parsed:
                reason = refusal(address, policy)
                if reason:
                    raise EgressRefused(f"{host} resolves to {address}, which {reason}")
            addresses = tuple(str(address) for address in parsed)
        pinned.append(
            PinnedHost(
                host=host,
                addresses=addresses,
                ports=ports,
                protocol=rule.protocol,
                literal=literal,
            )
        )
    return EgressPins(
        hosts=tuple(pinned),
        resolved_at=now(),
        dns=needs_dns is not None,
        dns_reason=needs_dns,
        private_allowed=policy.allow_private,
    )


async def private_addresses_allowed(
    conn: Any, connector_id: str, *, private_tiers: Iterable[str]
) -> bool:
    """Whether the project tier lets this connector's pod reach private and
    home addresses.

    A shared pod serves every execution the connector is attached to, so the
    strictest project decides: private addresses only when the connector
    belongs to at least one project (its own or a link), every one of them is
    on a tier that allows them, and the connector is not public.
    """
    row = await conn.fetchrow(
        """
        WITH scope AS (
            SELECT ds.project_id FROM datasources AS ds
             WHERE ds.id = $1 AND ds.project_id IS NOT NULL
            UNION
            SELECT link.project_id FROM project_datasources AS link
             WHERE link.datasource_id = $1
        )
        SELECT (SELECT is_global FROM datasources WHERE id = $1) AS is_global,
               count(*) AS projects,
               count(*) FILTER (WHERE p.network_tier = ANY($2::text[])) AS allowed
          FROM scope JOIN projects AS p ON p.id = scope.project_id
        """,
        UUID(str(connector_id)),
        sorted(set(private_tiers)),
    )
    if row is None or row["is_global"]:
        return False
    return int(row["projects"]) > 0 and int(row["allowed"]) == int(row["projects"])


__all__ = [
    "DEFAULT_CLUSTER_CIDRS",
    "DEFAULT_PRIVATE_TIERS",
    "DNS_PEER",
    "MANY_ADDRESSES",
    "EgressPins",
    "EgressPolicy",
    "EgressRefused",
    "PinnedHost",
    "expand_rule",
    "pin_egress",
    "private_addresses_allowed",
    "refusal",
    "system_resolver",
]
