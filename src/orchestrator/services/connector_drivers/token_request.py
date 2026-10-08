"""Kubernetes TokenRequest calls for kubeconfig connectors (connector drivers C5).

The rules (what a minting kubeconfig may hold, the minimal RBAC, the bound
Secret and the TokenRequest body) are ``shared.connectors.token_request``.
Here are the three calls SRW makes with the minting credential, each a
``provider_http`` call (one deadline, a capped answer, a checked and pinned
address, HTTPS verified against the kubeconfig's CA, never a proxy):

* ``POST /api/v1/namespaces/<ns>/secrets``: the Secret a token is bound to,
  named after the credential row (``srw-mint-<32 hex>``), with no data;
* ``POST /api/v1/namespaces/<ns>/serviceaccounts/<sa>/token``: the token,
  bound to that Secret by name and uid. A refusal deletes the Secret again,
  under a short deadline of its own; whatever that leaves, the record of the
  mint names it and the sweep deletes it;
* ``DELETE /api/v1/namespaces/<ns>/secrets/<name>``, with the Secret's uid
  as a precondition when it is known: revokes the token. A Secret already
  gone (404) or replaced by another of the same name (409) counts as
  revoked.

It also reads a minting kubeconfig's YAML and writes the delivered one.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any
from urllib.parse import quote

import yaml

from orchestrator.services.connector_drivers.provider_http import (
    MintedToken,
    ProviderAnswer,
    ProviderError,
    failure,
    parse_time,
    provider_request,
    status_transient,
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

logger = logging.getLogger(__name__)

_WHO = "the cluster's API server"
#: The deadline of the clean-up after a refused TokenRequest.
CLEANUP_DEADLINE_SECONDS = 5.0


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


def _path(*segments: str) -> str:
    return "/" + "/".join(quote(segment, safe="") for segment in segments)


def _refusal(answer: ProviderAnswer, action: str) -> ProviderError:
    """A fixed text per status class; the API server's message is logged."""
    status = answer.status
    logger.info(
        "Kubernetes API answered HTTP %d to %s: %s", status, action, answer.message()
    )
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
    elif status == 429:
        text = f"{_WHO} asked SRW to slow down (HTTP 429)"
    elif status >= 500:
        text = f"{_WHO} failed to {action} (a server error)"
    else:
        text = f"{_WHO} refused to {action} (an HTTP 4xx answer)"
    return ProviderError(text, transient=status_transient(status), reason="refused")


async def _call(
    minting: MintingKubeconfig,
    method: str,
    path: str,
    *,
    allow_private: bool,
    json_body: Any = None,
    deadline: float | None = None,
) -> ProviderAnswer:
    return await provider_request(
        method,
        minting.server + path,
        who=_WHO,
        headers=_headers(minting),
        json_body=json_body,
        ca_pem=minting.ca_pem,
        allow_private=allow_private,
        sni_hostname=minting.tls_server_name,
        deadline=deadline,
    )


async def mint_token(
    minting: MintingKubeconfig,
    options: TokenRequestOptions,
    *,
    secret: str,
    credential_id: Any,
    annotations: Mapping[str, str],
    allow_private: bool = False,
) -> MintedToken:
    """Create the bound Secret, then mint a token bound to it.

    The answer's ``handle`` is the Secret's uid. A token request that fails
    after the Secret exists deletes it again, best effort and bounded (the
    caller's record names it, so a sweep deletes it otherwise).
    """
    namespace = options.namespace
    created = await _call(
        minting,
        "POST",
        _path("api", "v1", "namespaces", namespace, "secrets"),
        allow_private=allow_private,
        json_body=bound_secret(
            name=secret, credential_id=credential_id, annotations=annotations
        ),
    )
    if created.status != 201:
        raise _refusal(created, "create the bound Secret")
    try:
        uid = str(created.json()["metadata"]["uid"])
    except (ValueError, KeyError, TypeError, UnicodeDecodeError):
        raise failure("malformed", _WHO, transient=True) from None
    try:
        answer = await _call(
            minting,
            "POST",
            _path(
                "api",
                "v1",
                "namespaces",
                namespace,
                "serviceaccounts",
                options.service_account,
                "token",
            ),
            allow_private=allow_private,
            json_body=token_request(options, secret=secret, secret_uid=uid),
        )
        if answer.status != 201:
            raise _refusal(answer, "mint a token (TokenRequest)")
        try:
            status = answer.json()["status"]
            token = str(status["token"])
            expires_at = parse_time(status["expirationTimestamp"])
        except (ValueError, KeyError, TypeError, UnicodeDecodeError):
            raise failure("malformed", _WHO, transient=True) from None
        if not token:
            raise failure("malformed", _WHO, transient=True)
    except ProviderError:
        # The Secret exists and no token is recorded: delete it now, under
        # a deadline of its own. A cancel skips this; the record remains.
        try:
            await _delete(
                minting,
                namespace,
                secret,
                uid,
                allow_private=allow_private,
                deadline=CLEANUP_DEADLINE_SECONDS,
            )
        except ProviderError:
            logger.info("The bound Secret %s is left for the sweep", secret)
        raise
    return MintedToken(token=token, expires_at=expires_at, handle=uid)


async def _delete(
    minting: MintingKubeconfig,
    namespace: str,
    name: str,
    uid: str | None,
    *,
    allow_private: bool,
    deadline: float | None = None,
) -> None:
    body: dict[str, Any] = {"kind": "DeleteOptions", "apiVersion": "v1"}
    if uid:
        body["preconditions"] = {"uid": uid}
    answer = await _call(
        minting,
        "DELETE",
        _path("api", "v1", "namespaces", namespace, "secrets", name),
        allow_private=allow_private,
        json_body=body,
        deadline=deadline,
    )
    # 404: already gone; 409: the uid precondition failed, so ours is gone
    # and another object took the name.
    if answer.status in (200, 202, 404, 409):
        return
    raise _refusal(answer, "delete the bound Secret")


async def delete_bound_secret(
    minting: MintingKubeconfig,
    *,
    namespace: str,
    name: str,
    uid: str | None,
    allow_private: bool = False,
) -> None:
    """Revoke a minted token: delete the Secret it is bound to."""
    await _delete(minting, namespace, name, uid, allow_private=allow_private)


__all__ = [
    "CLEANUP_DEADLINE_SECONDS",
    "delete_bound_secret",
    "delivered_kubeconfig_text",
    "mint_token",
    "parse_minting_kubeconfig",
]
