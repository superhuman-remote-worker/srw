"""GitHub App calls for repository connectors (connector drivers C5).

The rules (the config, the API base, the permissions per access level) are
``shared.connectors.github_app``. Here are SRW's calls, each over HTTPS,
never through a proxy, never following a redirect:

* the App JWT: RS256 over ``{iat, exp, iss}`` with the App's private key,
  backdated a minute and under ten minutes long;
* ``POST <api>/app/installations/<id>/access_tokens`` with the JWT: an
  installation token for the connector's one repository and the access
  level's ``contents`` permission. An answer that grants more than was asked
  is revoked at once and refused;
* ``DELETE <api>/installation/token`` with the installation token itself:
  revokes it (a token GitHub no longer accepts, 401, counts as revoked);
* for Test, ``GET <api>/repos/<owner>/<repository>`` with a read token.
"""

from __future__ import annotations

import time
from typing import Any

import httpx

from orchestrator.services.connector_drivers.provider_http import (
    MintedToken,
    ProviderError,
    message_of,
    parse_time,
    provider_client,
    status_transient,
    transport_error,
)
from shared.connectors.github_app import (
    API_VERSION,
    GitHubAppOptions,
    access_token_request,
    installation_permissions,
    jwt_claims,
)

_WHO = "GitHub"
#: A PEM private key's size cap (a 4096-bit RSA key is about 3.3 kB).
MAX_PRIVATE_KEY_BYTES = 16 * 1024


def normalize_private_key(value: Any) -> str:
    """The App's private key, checked: an unencrypted RSA key in PEM.

    Raises ``ValueError`` with a message for the connector's owner (never
    the key).
    """
    from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
    from cryptography.hazmat.primitives.serialization import load_pem_private_key

    if not isinstance(value, str) or not value.strip():
        raise ValueError("a GitHub App connector needs the App's private key")
    text = value.strip().replace("\r\n", "\n") + "\n"
    if len(text.encode("utf-8")) > MAX_PRIVATE_KEY_BYTES:
        raise ValueError("the GitHub App private key is too large")
    try:
        key = load_pem_private_key(text.encode("utf-8"), password=None)
    except (ValueError, TypeError):
        raise ValueError(
            "the GitHub App private key is not an unencrypted PEM private key "
            "(download it from the App's settings)"
        ) from None
    if not isinstance(key, RSAPrivateKey):
        raise ValueError("the GitHub App private key is not an RSA key")
    return text


def app_jwt(app_id: str, private_key: str, *, now: int | None = None) -> str:
    """The App's JWT, RS256."""
    import jwt

    claims = jwt_claims(app_id, int(time.time() if now is None else now))
    return jwt.encode(claims, private_key, algorithm="RS256")


def _headers(bearer: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {bearer}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": API_VERSION,
        "User-Agent": "srw-connector-github-app",
    }


def _refusal(response: httpx.Response, action: str) -> ProviderError:
    status = response.status_code
    message = message_of(response)
    suffix = f": {message}" if message else ""
    if status == 401:
        text = (
            f"{_WHO} refused the App's JWT (HTTP 401); check the App id and its "
            "private key"
        )
    elif status == 404:
        text = f"{_WHO} knows no such App installation (HTTP 404)"
    elif status == 422:
        text = (
            f"{_WHO} refused to {action} (HTTP 422); the installation must "
            "cover the repository and grant the contents permission"
        )
    else:
        text = f"{_WHO} refused to {action} (HTTP {status})"
    return ProviderError(text + suffix, transient=status_transient(status))


async def mint_installation_token(
    options: GitHubAppOptions,
    private_key: str,
    access: str,
    *,
    now: int | None = None,
) -> MintedToken:
    """An installation token for the connector's repository at ``access``."""
    try:
        bearer = app_jwt(options.app_id, private_key, now=now)
    except Exception:
        raise ProviderError(
            "the GitHub App private key cannot sign a JWT", transient=False
        ) from None
    url = (
        f"{options.api_base}/app/installations/{options.installation_id}/access_tokens"
    )
    async with provider_client() as client:
        try:
            response = await client.post(
                url,
                json=access_token_request(options.repository, access),
                headers=_headers(bearer),
            )
        except httpx.HTTPError as exc:
            raise transport_error(exc, _WHO) from None
        if response.status_code != 201:
            raise _refusal(response, "mint an installation token")
        try:
            body = response.json()
            token = str(body["token"])
            expires_at = parse_time(body["expires_at"])
        except (ValueError, KeyError, TypeError):
            raise ProviderError(
                f"{_WHO} answered an installation token SRW cannot read",
                transient=True,
            ) from None
        wanted = installation_permissions(access)
        if not token or not granted_as_asked(body.get("permissions"), wanted):
            # More (or other) than the access level: never deliver it.
            await _revoke(client, options.api_base, token)
            raise ProviderError(
                f"{_WHO} granted other permissions than SRW asked for "
                f"({sorted(wanted.items())}); the token was revoked",
                transient=False,
            )
    return MintedToken(token=token, expires_at=expires_at)


def granted_as_asked(granted: Any, wanted: dict[str, str]) -> bool:
    """Whether a token's granted permissions are the ones asked for, plus
    the ``metadata: read`` GitHub adds to every App token."""
    if not isinstance(granted, dict):
        return False
    allowed = {**wanted, "metadata": "read"}
    return all(granted.get(name) == level for name, level in wanted.items()) and all(
        allowed.get(name) == level for name, level in granted.items()
    )


async def _revoke(client: httpx.AsyncClient, api_base: str, token: str) -> None:
    if not token:
        return
    try:
        response = await client.request(
            "DELETE", f"{api_base}/installation/token", headers=_headers(token)
        )
    except httpx.HTTPError as exc:
        raise transport_error(exc, _WHO) from None
    # 401: GitHub no longer accepts the token (revoked or expired).
    if response.status_code in (204, 401):
        return
    raise _refusal(response, "revoke the installation token")


async def revoke_installation_token(api_base: str, token: str) -> None:
    """``DELETE /installation/token`` with the token itself."""
    async with provider_client() as client:
        await _revoke(client, api_base, token)


async def repository_facts(options: GitHubAppOptions, token: str) -> dict[str, Any]:
    """What a read token sees of the repository (Test)."""
    url = f"{options.api_base}/repos/{options.owner}/{options.repository}"
    async with provider_client() as client:
        try:
            response = await client.get(url, headers=_headers(token))
        except httpx.HTTPError as exc:
            raise transport_error(exc, _WHO) from None
    if response.status_code != 200:
        raise _refusal(response, "show the repository")
    try:
        body = response.json()
    except ValueError:
        body = {}
    return {
        "repository": f"{options.owner}/{options.repository}",
        "default_branch": body.get("default_branch")
        if isinstance(body, dict)
        else None,
        "private": body.get("private") if isinstance(body, dict) else None,
    }


__all__ = [
    "app_jwt",
    "mint_installation_token",
    "normalize_private_key",
    "repository_facts",
    "revoke_installation_token",
]
