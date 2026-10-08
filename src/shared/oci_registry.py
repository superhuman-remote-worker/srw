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
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit

import httpx

from shared.workspace_preparation import ARCHITECTURE, image_reference

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


class RegistryResolutionError(ValueError):
    pass


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
    user: str | None = None


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
    user = config.get("User")
    return {
        "entrypoint": _string_list(config.get("Entrypoint"), "entrypoint"),
        "cmd": _string_list(config.get("Cmd"), "command"),
        "labels": dict(labels),
        "user": user if isinstance(user, str) and user else None,
    }


class RegistryResolver:
    """Resolves references against registries over HTTPS.

    ``hosts`` is an allow-list of registry hosts; ``None`` allows any host
    (driver images: there is no registry allow-list). ``insecure_hosts`` are
    reached over plain HTTP and must be named explicitly. A bearer-token
    challenge may point at ``token_hosts``; with ``same_host_tokens`` it may
    also point at the registry's own host.
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
    ):
        self.hosts = None if hosts is None else frozenset(hosts)
        self.insecure_hosts = frozenset(insecure_hosts)
        self.token_hosts = frozenset(token_hosts)
        self.transport = transport
        self.same_host_tokens = same_host_tokens
        self.refusal = refusal
        self.timeout = float(timeout)
        if self.hosts is not None and not self.insecure_hosts <= self.hosts:
            raise ValueError("Insecure registry hosts must be explicitly allowed.")

    def permitted(self, image):
        host, _, _ = image_reference(image)
        if self.hosts is not None and host not in self.hosts:
            raise RegistryResolutionError(self.refusal)

    async def _response(self, client, url, *, headers=None, params=None):
        try:
            async with asyncio.timeout(self.timeout):
                async with client.stream(
                    "GET", url, headers=headers, params=params
                ) as response:
                    content = bytearray()
                    async for block in response.aiter_bytes():
                        content.extend(block)
                        if len(content) > MAX_RESPONSE_BYTES:
                            raise RegistryResolutionError(
                                "Registry response exceeds its size limit."
                            )
                    return response.status_code, response.headers, bytes(content)
        except TimeoutError:
            raise RegistryResolutionError(
                "Registry response deadline exceeded."
            ) from None

    def _client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            timeout=self.timeout,
            follow_redirects=False,
            trust_env=False,
            transport=self.transport,
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
            raise RegistryResolutionError(
                f"Image config retrieval failed (HTTP {code})."
            )
        return content

    @staticmethod
    def _manifest(code, headers, content, reference):
        if code != 200:
            raise RegistryResolutionError(
                f"Image manifest resolution failed (HTTP {code})."
            )
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
    "RegistryResolutionError",
    "RegistryResolver",
    "ResolvedImage",
    "image_config",
]
