"""OCI registry resolution: an image reference to a manifest digest and config.

Shared by VM workspace preparation (an operator-approved registry list) and
connector driver hosting (any registry; connector drivers D5, "Driver
versions"). It reads only what a registry serves anonymously or through its
own bearer-token challenge: manifests by tag or digest, and the image config
blob that carries the entrypoint, the command and the labels. No pull
credentials, so private registries are unsupported here.

Every response is size-capped and its digest is verified against what the
reference or the manifest names, so a registry (or anything between it and
SRW) cannot substitute content. The token challenge may only send SRW to an
allowed token host; a blob redirect is followed once, over HTTPS, without the
registry token.

Without a registry allow-list (driver images) every request is checked by
address: the host is resolved once, every answer must be a public address
(loopback, private, cluster and other non-global ranges only for the hosts
named in ``private_hosts``; link-local and cloud metadata never), and the
request dials the checked address with the name kept for the Host header and
TLS, so a second lookup cannot send it elsewhere. Token realms and blob
redirect targets go through the same check. Errors are generic: a caller
never learns an internal status or address (they are logged).
"""

from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import json
import logging
import re
import socket
from collections.abc import Awaitable, Callable, Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit

import httpx

from shared.workspace_preparation import ARCHITECTURE, image_reference

logger = logging.getLogger(__name__)

ACCEPT = ", ".join(
    (
        "application/vnd.oci.image.index.v1+json",
        "application/vnd.oci.image.manifest.v1+json",
        "application/vnd.docker.distribution.manifest.list.v2+json",
        "application/vnd.docker.distribution.manifest.v2+json",
    )
)
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
MAX_RESPONSE_BYTES = 1024 * 1024
#: Docker Hub answers its registry's challenge from another host.
DEFAULT_TOKEN_HOSTS = frozenset({"auth.docker.io"})

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


class RegistryResolutionError(ValueError):
    pass


def address_refusal(
    address: _Address,
    *,
    private_allowed: bool = False,
    refused: Iterable[_Network] = (),
) -> str | None:
    """Why a registry request may not dial ``address``, or ``None``.

    Link-local, metadata, multicast and unspecified addresses never.
    Loopback, private, shared (CGNAT), translated and every other non-global
    address, and the ``refused`` networks (the cluster's ranges), only when
    the host is listed as private.
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


HostResolver = Callable[[str], Awaitable[Sequence[str]]]


async def system_host_resolver(host: str) -> Sequence[str]:
    """Every A and AAAA answer of the system resolver."""
    loop = asyncio.get_running_loop()
    infos = await loop.getaddrinfo(
        host, None, family=socket.AF_UNSPEC, type=socket.SOCK_STREAM
    )
    return [str(info[4][0]) for info in infos]


@dataclass(frozen=True)
class ResolvedImage:
    """One image as the registry serves it for linux/amd64.

    ``reference`` is ``host/repository@digest``. ``entrypoint`` and ``cmd``
    are the image's own (empty when it declares none); ``labels`` its
    config labels.
    """

    reference: str
    digest: str
    entrypoint: tuple[str, ...] = ()
    cmd: tuple[str, ...] = ()
    labels: dict[str, str] = field(default_factory=dict)


def _string_list(value: Any, what: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
        raise RegistryResolutionError(f"Registry image config has an invalid {what}.")
    return tuple(value)


def image_config(content: bytes) -> dict[str, Any]:
    """The runtime part of an OCI image config blob."""
    try:
        document = json.loads(content)
    except (ValueError, TypeError) as exc:
        raise RegistryResolutionError(
            "Registry returned an invalid image config."
        ) from exc
    if not isinstance(document, dict):
        raise RegistryResolutionError("Registry returned an invalid image config.")
    config = document.get("config")
    if config is None:
        config = {}
    if not isinstance(config, dict):
        raise RegistryResolutionError("Registry returned an invalid image config.")
    labels = config.get("Labels")
    if labels is None:
        labels = {}
    if not isinstance(labels, dict) or not all(
        isinstance(k, str) and isinstance(v, str) for k, v in labels.items()
    ):
        raise RegistryResolutionError("Registry image config has invalid labels.")
    return {
        "entrypoint": _string_list(config.get("Entrypoint"), "entrypoint"),
        "cmd": _string_list(config.get("Cmd"), "command"),
        "labels": dict(labels),
    }


class RegistryResolver:
    """Resolves references against registries over HTTPS.

    ``hosts`` is an allow-list of registry hosts; ``None`` allows any host
    (driver images: there is no registry allow-list) and checks every
    request's address instead: public addresses only, except for the
    ``private_hosts`` (``host[:port]``), which may also be private, loopback
    or inside the ``refused_networks`` (the cluster's ranges).
    ``insecure_hosts`` are reached over plain HTTP and must be named
    explicitly. A bearer-token challenge may point at ``token_hosts``; with
    ``same_host_tokens`` it may also point at the registry's own host.
    """

    def __init__(
        self,
        *,
        hosts=None,
        insecure_hosts=(),
        token_hosts=(),
        transport=None,
        same_host_tokens: bool = False,
        refusal: str = "Image registry is not enabled.",
        timeout: float = 20,
        private_hosts=(),
        refused_networks=(),
        host_resolver: HostResolver | None = None,
    ):
        self.hosts = None if hosts is None else frozenset(hosts)
        self.insecure_hosts = frozenset(insecure_hosts)
        self.token_hosts = frozenset(token_hosts)
        self.transport = transport
        self.same_host_tokens = same_host_tokens
        self.refusal = refusal
        self.timeout = float(timeout)
        self.private_hosts = frozenset(private_hosts)
        self.refused_networks = tuple(
            ipaddress.ip_network(cidr, strict=False) for cidr in refused_networks
        )
        self.host_resolver = host_resolver or system_host_resolver
        #: An allow-list is the operator's word for every host on it.
        self.checks_addresses = self.hosts is None
        if self.hosts is not None and not self.insecure_hosts <= self.hosts:
            raise ValueError("Insecure registry hosts must be explicitly allowed.")

    async def _dial(self, url: str) -> list[tuple[str, dict[str, str], dict[str, Any]]]:
        """The URLs to request in turn, with extra headers and extensions.

        With address checks, the host is resolved once and every answer must
        be allowed; the request dials them in order (the next when one does
        not connect), keeping the name for the Host header and for TLS (SNI
        and certificate check). No connection is kept alive: a pool keyed by
        address would reuse one name's TLS session for another.
        """
        if not self.checks_addresses:
            return [(url, {}, {})]
        parsed = httpx.URL(url)
        host = parsed.host
        netloc = parsed.netloc.decode("ascii")
        listed = netloc in self.private_hosts or host in self.private_hosts
        try:
            answers = [ipaddress.ip_address(host)]
            literal = True
        except ValueError:
            literal = False
            try:
                answers = [
                    ipaddress.ip_address(answer.split("%")[0])
                    for answer in await self.host_resolver(host)
                ]
            except (OSError, UnicodeError, ValueError) as exc:
                logger.info("Registry host %s does not resolve: %s", netloc, exc)
                raise RegistryResolutionError(
                    "Registry host does not resolve."
                ) from None
        if not answers:
            raise RegistryResolutionError("Registry host does not resolve.")
        for address in answers:
            reason = address_refusal(
                address, private_allowed=listed, refused=self.refused_networks
            )
            if reason is not None:
                logger.warning(
                    "Registry request to %s refused: %s %s", netloc, address, reason
                )
                raise RegistryResolutionError("Registry address is not allowed.")
        if literal:
            return [(url, {"Connection": "close"}, {})]
        extensions: dict[str, Any] = {}
        if parsed.scheme == "https":
            extensions["sni_hostname"] = host
        return [
            (
                str(parsed.copy_with(host=str(address))),
                {"Host": netloc, "Connection": "close"},
                extensions,
            )
            for address in answers
        ]

    def permitted(self, image):
        host, _, _ = image_reference(image)
        if self.hosts is not None and host not in self.hosts:
            raise RegistryResolutionError(self.refusal)

    async def _response(self, client, url, *, headers=None, params=None):
        try:
            async with asyncio.timeout(self.timeout):
                candidates = await self._dial(url)
                for index, (dial, pinned, extensions) in enumerate(candidates):
                    try:
                        return await self._fetch(
                            client,
                            dial,
                            headers={**(headers or {}), **pinned},
                            params=params,
                            extensions=extensions,
                        )
                    except (httpx.ConnectError, httpx.ConnectTimeout) as exc:
                        if index == len(candidates) - 1:
                            raise
                        logger.info("Registry address did not connect (%s)", exc)
                raise RegistryResolutionError("Registry host does not resolve.")
        except TimeoutError:
            raise RegistryResolutionError(
                "Registry response deadline exceeded."
            ) from None

    @staticmethod
    async def _fetch(client, url, *, headers, params, extensions):
        async with client.stream(
            "GET", url, headers=headers, params=params, extensions=extensions
        ) as response:
            content = bytearray()
            async for block in response.aiter_bytes():
                content.extend(block)
                if len(content) > MAX_RESPONSE_BYTES:
                    raise RegistryResolutionError(
                        "Registry response exceeds its size limit."
                    )
            return response.status_code, response.headers, bytes(content)

    def _client(self) -> httpx.AsyncClient:
        options: dict[str, Any] = {}
        if self.checks_addresses:
            # Requests dial checked addresses with a name for TLS: never reuse
            # one name's connection for another name at the same address.
            options["limits"] = httpx.Limits(
                max_connections=10, max_keepalive_connections=0
            )
        return httpx.AsyncClient(
            timeout=self.timeout,
            follow_redirects=False,
            trust_env=False,
            transport=self.transport,
            **options,
        )

    async def _authorize(self, client, response_headers, *, endpoint, repository):
        challenge = response_headers.get("WWW-Authenticate", "")
        if not challenge.lower().startswith("bearer "):
            raise RegistryResolutionError(
                "Registry requires unsupported preparation authentication."
            )
        fields = dict(re.findall(r'(\w+)="([^"\r\n]*)"', challenge[7:]))
        realm = fields.get("realm", "")
        parsed = urlsplit(realm)
        allowed = parsed.netloc in self.token_hosts or (
            self.same_host_tokens and parsed.netloc == endpoint
        )
        if (
            parsed.scheme != "https"
            or not allowed
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
        ):
            raise RegistryResolutionError("Registry token endpoint is not enabled.")
        code, _, token_body = await self._response(
            client,
            realm,
            params={
                "service": fields.get("service", endpoint),
                "scope": f"repository:{repository}:pull",
            },
        )
        if code != 200:
            logger.info("Registry token request failed: HTTP %s", code)
            raise RegistryResolutionError("Registry token request failed.")
        try:
            token_data = json.loads(token_body)
        except (ValueError, TypeError):
            token_data = None
        if not isinstance(token_data, dict):
            raise RegistryResolutionError("Registry returned an invalid token.")
        token = token_data.get("token") or token_data.get("access_token")
        if (
            not isinstance(token, str)
            or not 1 <= len(token) <= 16384
            or "\n" in token
            or "\r" in token
        ):
            raise RegistryResolutionError("Registry returned an invalid token.")
        return "Bearer " + token

    async def _platform_manifest(self, client, image):
        """``(host, repository, digest, manifest, base_url, headers)``."""
        self.permitted(image)
        host, repository, reference = image_reference(image)
        endpoint = "registry-1.docker.io" if host == "docker.io" else host
        scheme = "http" if host in self.insecure_hosts else "https"
        base = f"{scheme}://{endpoint}/v2/{repository}/"
        headers = {"Accept": ACCEPT}
        code, response_headers, content = await self._response(
            client, base + "manifests/" + reference, headers=headers
        )
        if code == 401:
            headers["Authorization"] = await self._authorize(
                client, response_headers, endpoint=endpoint, repository=repository
            )
            code, response_headers, content = await self._response(
                client, base + "manifests/" + reference, headers=headers
            )
        digest, manifest = self._manifest(code, response_headers, content, reference)
        if "manifests" in manifest:
            if not isinstance(manifest["manifests"], list) or any(
                not isinstance(m, dict) or not isinstance(m.get("platform", {}), dict)
                for m in manifest["manifests"]
            ):
                raise RegistryResolutionError(
                    "Registry returned an invalid image index."
                )
            candidates = [
                m
                for m in manifest["manifests"]
                if m.get("platform", {}).get("os") == "linux"
                and m.get("platform", {}).get("architecture") == ARCHITECTURE
                and not m.get("platform", {}).get("variant")
            ]
            if len(candidates) != 1:
                raise RegistryResolutionError(
                    "Base image has no unambiguous linux/amd64 manifest."
                )
            reference = candidates[0].get("digest", "")
            if not _DIGEST.fullmatch(reference):
                raise RegistryResolutionError("Invalid platform image digest.")
            code, response_headers, content = await self._response(
                client, base + "manifests/" + reference, headers=headers
            )
            digest, manifest = self._manifest(
                code, response_headers, content, reference
            )
        if manifest.get("schemaVersion") != 2 or not isinstance(
            manifest.get("layers"), list
        ):
            raise RegistryResolutionError(
                "Registry did not return an OCI image manifest."
            )
        return host, repository, digest, manifest, base, headers

    async def resolve(self, image):
        """``host/repository@digest`` of the image's linux/amd64 manifest."""
        async with self._client() as client:
            host, repository, digest, _, _, _ = await self._platform_manifest(
                client, image
            )
            return f"{host}/{repository}@{digest}"

    async def resolve_image(self, image) -> ResolvedImage:
        """The manifest digest plus the image config's runtime fields."""
        async with self._client() as client:
            (
                host,
                repository,
                digest,
                manifest,
                base,
                headers,
            ) = await self._platform_manifest(client, image)
            descriptor = manifest.get("config")
            config_digest = (
                descriptor.get("digest") if isinstance(descriptor, dict) else None
            )
            if not isinstance(config_digest, str) or not _DIGEST.fullmatch(
                config_digest
            ):
                raise RegistryResolutionError("Registry manifest names no config.")
            content = await self._blob(client, base + "blobs/" + config_digest, headers)
            if "sha256:" + hashlib.sha256(content).hexdigest() != config_digest:
                raise RegistryResolutionError(
                    "Registry config digest verification failed."
                )
            config = image_config(content)
            return ResolvedImage(
                reference=f"{host}/{repository}@{digest}", digest=digest, **config
            )

    async def _blob(self, client, url, headers) -> bytes:
        code, response_headers, content = await self._response(
            client, url, headers=headers
        )
        if code in (301, 302, 303, 307, 308):
            # Registries hand blobs to a storage host. Follow once, over
            # HTTPS, without the registry token.
            location = response_headers.get("Location", "")
            target = urlsplit(location)
            if target.scheme != "https" or target.username or target.password:
                raise RegistryResolutionError("Registry blob redirect is not allowed.")
            code, _, content = await self._response(client, location)
        if code != 200:
            logger.info("Image config retrieval failed: HTTP %s", code)
            raise RegistryResolutionError("Image config retrieval failed.")
        return content

    @staticmethod
    def _manifest(code, headers, content, reference):
        if code != 200:
            logger.info("Image manifest resolution failed: HTTP %s", code)
            raise RegistryResolutionError("Image manifest resolution failed.")
        digest = "sha256:" + hashlib.sha256(content).hexdigest()
        if headers.get("Docker-Content-Digest", digest) != digest or (
            reference.startswith("sha256:") and reference != digest
        ):
            raise RegistryResolutionError(
                "Registry manifest digest verification failed."
            )
        try:
            manifest = json.loads(content)
        except (ValueError, TypeError) as exc:
            raise RegistryResolutionError(
                "Registry returned an invalid manifest."
            ) from exc
        if not isinstance(manifest, dict):
            raise RegistryResolutionError("Registry returned an invalid manifest.")
        return digest, manifest


__all__ = [
    "ACCEPT",
    "DEFAULT_TOKEN_HOSTS",
    "MAX_RESPONSE_BYTES",
    "HostResolver",
    "RegistryResolutionError",
    "RegistryResolver",
    "ResolvedImage",
    "address_refusal",
    "image_config",
    "system_host_resolver",
]
