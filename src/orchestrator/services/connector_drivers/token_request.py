"""Kubernetes TokenRequest calls for kubeconfig connectors (connector drivers C5).

The rules (what a minting kubeconfig may hold, the minimal RBAC, the bound
Secret and the TokenRequest body) are ``shared.connectors.token_request``.
Here are the three calls SRW makes with the minting credential, each over
HTTPS verified against the kubeconfig's CA, never through a proxy:

* ``POST /api/v1/namespaces/<ns>/secrets``: the Secret a token is bound to,
  named after the credential row (``srw-mint-<32 hex>``), with no data;
* ``POST /api/v1/namespaces/<ns>/serviceaccounts/<sa>/token``: the token,
  bound to that Secret by name and uid. A refusal deletes the Secret again;
* ``DELETE /api/v1/namespaces/<ns>/secrets/<name>``, with the Secret's uid
  as a precondition when it is known: revokes the token. A Secret already
  gone (404) or replaced by another of the same name (409) counts as
  revoked.

It also reads a minting kubeconfig's YAML and writes the delivered one.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from typing import Any
from urllib.parse import quote

import httpx
import yaml

from orchestrator.services.connector_drivers.provider_http import (
    MintedToken,
    ProviderError,
    message_of,
    parse_time,
    provider_client,
    status_transient,
    tls_context,
    transport_error,
)
from shared.connectors.token_request import (
    MintingKubeconfig,
    TokenRequestConfigError,
    TokenRequestOptions,
    bound_secret,
    delivered_kubeconfig,
    minting_kubeconfig,
    token_request,
)

_WHO = "the cluster's API server"


def parse_minting_kubeconfig(text: Any) -> MintingKubeconfig:
    """A stored minting kubeconfig, read and checked
    (``shared.connectors.token_request.minting_kubeconfig``)."""
    if not isinstance(text, str) or not text.strip():
        raise TokenRequestConfigError("the minting kubeconfig is empty")
    try:
        doc = yaml.safe_load(text)
    except yaml.YAMLError:
        raise TokenRequestConfigError("the minting kubeconfig is not YAML") from None
    return minting_kubeconfig(doc)


def delivered_kubeconfig_text(**fields: Any) -> str:
    """The workspace's kubeconfig as YAML (``delivered_kubeconfig``)."""
    return yaml.safe_dump(delivered_kubeconfig(**fields), sort_keys=False)


def _headers(minting: MintingKubeconfig) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {minting.token}",
        "Accept": "application/json",
    }


def _extensions(minting: MintingKubeconfig) -> dict[str, Any]:
    return {"sni_hostname": minting.tls_server_name} if minting.tls_server_name else {}


def _path(*segments: str) -> str:
    return "/" + "/".join(quote(segment, safe="") for segment in segments)


def _refusal(response: httpx.Response, action: str) -> ProviderError:
    status = response.status_code
    message = message_of(response)
    suffix = f": {message}" if message else ""
    if status == 401:
        text = f"{_WHO} refused the minting credential (HTTP 401)"
    elif status == 403:
        text = (
            f"the minting credential may not {action} (HTTP 403); grant it the "
            "role SRW documents for TokenRequest minting"
        )
    elif status == 404:
        text = (
            f"{action}: the namespace or the ServiceAccount does not exist (HTTP 404)"
        )
    else:
        text = f"{_WHO} refused to {action} (HTTP {status})"
    return ProviderError(text + suffix, transient=status_transient(status))


async def mint_token(
    minting: MintingKubeconfig,
    options: TokenRequestOptions,
    *,
    secret: str,
    credential_id: Any,
    annotations: Mapping[str, str],
) -> MintedToken:
    """Create the bound Secret, then mint a token bound to it.

    The answer's ``handle`` is the Secret's uid. A token request that fails
    after the Secret exists deletes it again (best effort; the caller's
    record names it, so a sweep deletes it otherwise).
    """
    namespace = options.namespace
    async with provider_client(verify=tls_context(minting.ca_pem)) as client:
        try:
            created = await client.post(
                minting.server + _path("api", "v1", "namespaces", namespace, "secrets"),
                json=bound_secret(
                    name=secret, credential_id=credential_id, annotations=annotations
                ),
                headers=_headers(minting),
                extensions=_extensions(minting),
            )
        except httpx.HTTPError as exc:
            raise transport_error(exc, _WHO) from None
        if created.status_code != 201:
            raise _refusal(created, "create the bound Secret")
        try:
            uid = str(created.json()["metadata"]["uid"])
        except (ValueError, KeyError, TypeError):
            raise ProviderError(
                f"{_WHO} answered a Secret without a uid", transient=True
            ) from None
        try:
            answer = await client.post(
                minting.server
                + _path(
                    "api",
                    "v1",
                    "namespaces",
                    namespace,
                    "serviceaccounts",
                    options.service_account,
                    "token",
                ),
                json=token_request(options, secret=secret, secret_uid=uid),
                headers=_headers(minting),
                extensions=_extensions(minting),
            )
            if answer.status_code != 201:
                raise _refusal(answer, "mint a token (TokenRequest)")
            try:
                status = answer.json()["status"]
                token = str(status["token"])
                expires_at = parse_time(status["expirationTimestamp"])
            except (ValueError, KeyError, TypeError):
                raise ProviderError(
                    f"{_WHO} answered a TokenRequest without a token", transient=True
                ) from None
            if not token:
                raise ProviderError(f"{_WHO} answered an empty token", transient=True)
        except BaseException as exc:
            # The Secret exists and no token is recorded: delete it now.
            try:
                await asyncio.shield(_delete(client, minting, namespace, secret, uid))
            except Exception:
                pass
            if isinstance(exc, httpx.HTTPError):
                raise transport_error(exc, _WHO) from None
            raise
    return MintedToken(token=token, expires_at=expires_at, handle=uid)


async def _delete(
    client: httpx.AsyncClient,
    minting: MintingKubeconfig,
    namespace: str,
    name: str,
    uid: str | None,
) -> None:
    body: dict[str, Any] = {"kind": "DeleteOptions", "apiVersion": "v1"}
    if uid:
        body["preconditions"] = {"uid": uid}
    try:
        response = await client.request(
            "DELETE",
            minting.server
            + _path("api", "v1", "namespaces", namespace, "secrets", name),
            json=body,
            headers=_headers(minting),
            extensions=_extensions(minting),
        )
    except httpx.HTTPError as exc:
        raise transport_error(exc, _WHO) from None
    # 404: already gone; 409: the uid precondition failed, so ours is gone
    # and another object took the name.
    if response.status_code in (200, 202, 404, 409):
        return
    raise _refusal(response, "delete the bound Secret")


async def delete_bound_secret(
    minting: MintingKubeconfig, *, namespace: str, name: str, uid: str | None
) -> None:
    """Revoke a minted token: delete the Secret it is bound to."""
    async with provider_client(verify=tls_context(minting.ca_pem)) as client:
        await _delete(client, minting, namespace, name, uid)


__all__ = [
    "delete_bound_secret",
    "delivered_kubeconfig_text",
    "mint_token",
    "parse_minting_kubeconfig",
]
