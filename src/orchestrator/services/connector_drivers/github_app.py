"""GitHub App calls for repository connectors (connector drivers C5).

The rules (the config, the API base, the permissions per access level) are
``shared.connectors.github_app``. Here are SRW's calls, each a
``provider_http`` call (one deadline, a capped answer, a checked and pinned
address, verified against the connector's ``upstream_ca`` when it names one,
never a redirect or a proxy):

* the App JWT: RS256 over ``{iat, exp, iss}`` with the App's private key,
  backdated a minute and under ten minutes long;
* ``POST <api>/app/installations/<id>/access_tokens`` with the JWT: an
  installation token for the connector's one repository and the access
  level's ``contents`` permission. An answer that grants more than was asked,
  or covers another repository, is revoked at once and refused;
* ``DELETE <api>/installation/token`` with the installation token itself:
  revokes it (a token GitHub no longer accepts, 401, counts as revoked);
* for Test, ``GET <api>/repos/<owner>/<repository>`` with a read token.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from orchestrator.services.connector_drivers.provider_http import (
    MintedToken,
    ProviderAnswer,
    ProviderError,
    UnrevokedToken,
    failure,
    parse_time,
    provider_request,
    status_class,
    status_transient,
)
from shared.connectors.github_app import (
    API_VERSION,
    GitHubAppOptions,
    access_token_request,
    installation_permissions,
    jwt_claims,
)

logger = logging.getLogger(__name__)

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


def _refusal(answer: ProviderAnswer, action: str) -> ProviderError:
    """A fixed text per status class; GitHub's message is logged."""
    status = answer.status
    logger.info("GitHub answered HTTP %d to %s: %s", status, action, answer.message())
    if status == 401:
        text = (
            f"{_WHO} refused the App's JWT (HTTP 401); check the App id and its "
            "private key"
        )
    elif status == 403:
        text = f"{_WHO} refused to {action} (HTTP 403)"
    elif status == 404:
        text = f"{_WHO} knows no such App installation or repository (HTTP 404)"
    elif status == 422:
        text = (
            f"{_WHO} refused to {action} (HTTP 422); the installation must "
            "cover the repository and grant the contents permission"
        )
    elif status == 429:
        text = f"{_WHO} asked SRW to slow down (HTTP 429)"
    elif status >= 500:
        text = f"{_WHO} failed to {action} (a server error)"
    else:
        text = f"{_WHO} refused to {action} ({status_class(status)})"
    return ProviderError(text, transient=status_transient(status), reason="refused")


async def _call(
    method: str,
    url: str,
    bearer: str,
    *,
    ca_pem: str | None,
    allow_private: bool,
    json_body: Any = None,
) -> ProviderAnswer:
    return await provider_request(
        method,
        url,
        who=_WHO,
        headers=_headers(bearer),
        json_body=json_body,
        ca_pem=ca_pem,
        allow_private=allow_private,
    )


def granted_as_asked(granted: Any, wanted: dict[str, str]) -> bool:
    """Whether a token's granted permissions are the ones asked for, plus
    the ``metadata: read`` GitHub adds to every App token."""
    if not isinstance(granted, dict):
        return False
    allowed = {**wanted, "metadata": "read"}
    return all(granted.get(name) == level for name, level in wanted.items()) and all(
        allowed.get(name) == level for name, level in granted.items()
    )


def covers_only(repositories: Any, owner: str, name: str) -> bool:
    """Whether GitHub's answer names exactly the connector's repository (by
    ``full_name``, or ``owner.login`` and ``name``, case-insensitively)."""
    if not isinstance(repositories, list) or len(repositories) != 1:
        return False
    found = repositories[0]
    if not isinstance(found, dict):
        return False
    wanted = f"{owner}/{name}".lower()
    full = found.get("full_name")
    if isinstance(full, str):
        return full.lower() == wanted
    login = (
        (found.get("owner") or {}).get("login")
        if isinstance(found.get("owner"), dict)
        else None
    )
    return (
        isinstance(login, str)
        and isinstance(found.get("name"), str)
        and f"{login}/{found['name']}".lower() == wanted
    )


async def mint_installation_token(
    options: GitHubAppOptions,
    private_key: str,
    access: str,
    *,
    ca_pem: str | None = None,
    allow_private: bool = False,
    now: int | None = None,
) -> MintedToken:
    """An installation token for the connector's repository at ``access``."""
    try:
        bearer = app_jwt(options.app_id, private_key, now=now)
    except Exception:
        raise ProviderError(
            "the GitHub App private key cannot sign a JWT", transient=False
        ) from None
    answer = await _call(
        "POST",
        f"{options.api_base}/app/installations/{options.installation_id}/access_tokens",
        bearer,
        ca_pem=ca_pem,
        allow_private=allow_private,
        json_body=access_token_request(options.repository, access),
    )
    if answer.status != 201:
        raise _refusal(answer, "mint an installation token")
    try:
        body = answer.json()
        token = str(body["token"])
        expires_at = parse_time(body["expires_at"])
    except (ValueError, KeyError, TypeError, UnicodeDecodeError, AttributeError):
        raise failure("malformed", _WHO, transient=True) from None
    wanted = installation_permissions(access)
    if not token:
        raise failure("malformed", _WHO, transient=True)
    if not granted_as_asked(body.get("permissions"), wanted) or not covers_only(
        body.get("repositories"), options.owner, options.repository
    ):
        # More, other permissions or other repositories than asked: never
        # deliver it. One SRW could not revoke at once is handed back, so
        # its record keeps revoking it.
        refused = (
            f"{_WHO} granted other permissions or repositories than SRW asked "
            f"for ({sorted(wanted.items())} on "
            f"{options.owner}/{options.repository})"
        )
        try:
            await revoke_installation_token(
                options.api_base, token, ca_pem=ca_pem, allow_private=allow_private
            )
        except ProviderError:
            logger.warning("An over-broad installation token could not be revoked")
            raise UnrevokedToken(
                refused + "; SRW keeps revoking the token",
                minted=MintedToken(token=token, expires_at=expires_at),
            ) from None
        raise ProviderError(
            refused + "; the token was revoked", transient=False, reason="overbroad"
        )
    return MintedToken(token=token, expires_at=expires_at)


async def revoke_installation_token(
    api_base: str,
    token: str,
    *,
    ca_pem: str | None = None,
    allow_private: bool = False,
) -> None:
    """``DELETE /installation/token`` with the token itself."""
    if not token:
        return
    answer = await _call(
        "DELETE",
        f"{api_base}/installation/token",
        token,
        ca_pem=ca_pem,
        allow_private=allow_private,
    )
    # 401: GitHub no longer accepts the token (revoked or expired).
    if answer.status in (204, 401):
        return
    raise _refusal(answer, "revoke the installation token")


async def repository_facts(
    options: GitHubAppOptions,
    token: str,
    *,
    ca_pem: str | None = None,
    allow_private: bool = False,
) -> dict[str, Any]:
    """What a read token sees of the repository (Test)."""
    answer = await _call(
        "GET",
        f"{options.api_base}/repos/{options.owner}/{options.repository}",
        token,
        ca_pem=ca_pem,
        allow_private=allow_private,
    )
    if answer.status != 200:
        raise _refusal(answer, "show the repository")
    try:
        body = answer.json()
    except (ValueError, UnicodeDecodeError):
        body = {}
    body = body if isinstance(body, dict) else {}
    return {
        "repository": f"{options.owner}/{options.repository}",
        "default_branch": body.get("default_branch"),
        "private": body.get("private"),
    }


__all__ = [
    "app_jwt",
    "covers_only",
    "granted_as_asked",
    "mint_installation_token",
    "normalize_private_key",
    "repository_facts",
    "revoke_installation_token",
]
