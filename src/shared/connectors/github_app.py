"""GitHub App installation tokens for repository connectors (connector drivers C5).

A repository connector may authenticate as a GitHub App installation instead
of with a token: its credentials name ``auth_method: github_app`` and hold
the App's private key (a connector secret), and its config names the App
and the installation (``github_app: {app_id, installation_id, api_base}``).
SRW keeps the key. For each workspace-owning execution it signs an App JWT
and mints an installation token (``POST /app/installations/{id}/
access_tokens``) that expires an hour later, limited to the connector's one
repository and to the connector's access level: ``contents: read`` for
ReadOnly, ``contents: write`` for ReadWrite, nothing more. When the
execution ends SRW revokes it (``DELETE /installation/token``).

Where the git swap driver serves the repository (C3), the token never
reaches the workspace: the lease exchange hands it to the driver as the
upstream credential, re-minted before it expires. Without the swap driver,
the installation's C3 fallback applies, visibly: ``token-in-url`` delivers
the one-hour, repository-scoped token in the clone URL (never the key),
``refuse`` delivers nothing. Either way git presents the token with the
username GitHub documents for installation tokens (:data:`TOKEN_USERNAME`);
static forge tokens keep the one SRW always used (``oauth2``).

The App's key signs requests to the repository's own API host only
(:func:`api_host_for`), and the token GitHub answers must cover exactly the
connector's repository.

GitHub.com is served at ``https://api.github.com``; a GitHub Enterprise
Server at ``https://<host>/api/v3`` unless ``api_base`` names another base;
a GHE.com data-residency host ``<sub>.ghe.com`` at ``https://api.<sub>.ghe.com``.

Only the ``contents`` permission is minted, as the design asks: the
pull-request tools need ``pull_requests: write`` and so do not work on a
GitHub App connector.

Design: knowledge-base/knowledge/features/connector_drivers.md, "Three ways
to give an agent ephemeral authority", "The git swap driver"; slice C5.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

AUTH_METHOD = "github_app"
CONFIG_KEY = "github_app"
GITHUB_COM_API = "https://api.github.com"
#: The username GitHub documents for an installation token over HTTPS git.
TOKEN_USERNAME = "x-access-token"
#: GitHub refuses an App JWT that lives longer than ten minutes; SRW backdates
#: ``iat`` a minute for clock drift and keeps the total under that.
JWT_BACKDATE_SECONDS = 60
JWT_LIFETIME_SECONDS = 540
#: The permissions each access level is minted with, nothing more.
PERMISSIONS: dict[str, dict[str, str]] = {
    "ReadOnly": {"contents": "read"},
    "ReadWrite": {"contents": "write"},
}
#: GitHub's API version header.
API_VERSION = "2022-11-28"

_ID = re.compile(r"[1-9][0-9]{0,19}\Z")
_SEGMENT = re.compile(r"[A-Za-z0-9._-]{1,100}\Z")


class GitHubAppConfigError(ValueError):
    """A GitHub App connector SRW refuses; the message says why."""


@dataclass(frozen=True)
class GitHubAppOptions:
    """What a GitHub App connector names, besides its key."""

    app_id: str
    installation_id: str
    api_base: str
    owner: str
    repository: str

    def as_config(self, *, configured_api_base: str | None) -> dict[str, Any]:
        config: dict[str, Any] = {
            "app_id": self.app_id,
            "installation_id": self.installation_id,
        }
        if configured_api_base:
            config["api_base"] = configured_api_base
        return config


def uses_github_app(credentials: Any) -> bool:
    """Whether stored (or bound) credentials name a GitHub App."""
    return (
        isinstance(credentials, Mapping)
        and str(credentials.get("auth_method") or "").strip().lower() == AUTH_METHOD
    )


def repository_of(url: Any) -> tuple[str, str]:
    """``(owner, repository)`` of an HTTPS GitHub repository URL."""
    if not isinstance(url, str):
        raise GitHubAppConfigError("the connector has no repository URL")
    parts = urlsplit(url.strip())
    if parts.scheme.lower() != "https" or not parts.hostname:
        raise GitHubAppConfigError(
            "a GitHub App connector needs an https repository URL"
        )
    if "@" in parts.netloc or parts.query or parts.fragment:
        raise GitHubAppConfigError(
            "a GitHub App connector's URL carries no credentials, query or fragment"
        )
    segments = [s for s in parts.path.strip("/").split("/") if s]
    if len(segments) != 2:
        raise GitHubAppConfigError(
            "a GitHub App connector's URL names one repository: "
            "https://<host>/<owner>/<repository>"
        )
    owner, name = segments[0], segments[1].removesuffix(".git")
    if not _SEGMENT.fullmatch(owner) or not _SEGMENT.fullmatch(name):
        raise GitHubAppConfigError("the repository URL's owner or name is invalid")
    return owner, name


def default_api_base(url: Any) -> str:
    """The REST API base a repository's host answers on."""
    host = (urlsplit(str(url or "")).hostname or "").lower()
    if host in ("github.com", "www.github.com"):
        return GITHUB_COM_API
    if host.endswith(".ghe.com"):
        return f"https://api.{host}"
    port = urlsplit(str(url or "")).port
    netloc = f"{host}:{port}" if port and port != 443 else host
    return f"https://{netloc}/api/v3"


def _api_base(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise GitHubAppConfigError("github_app.api_base must be an https URL")
    parts = urlsplit(value.strip())
    try:
        parts.port
    except ValueError:
        raise GitHubAppConfigError("github_app.api_base has an invalid port") from None
    if (
        parts.scheme.lower() != "https"
        or not parts.hostname
        or "@" in parts.netloc
        or parts.query
        or parts.fragment
    ):
        raise GitHubAppConfigError(
            "github_app.api_base must be an https URL without credentials, "
            "query or fragment"
        )
    return f"https://{parts.netloc.lower()}{parts.path.rstrip('/')}"


def api_host_for(url: Any) -> str:
    """The one host a repository's App calls may go to: api.github.com for
    github.com, api.<sub>.ghe.com for <sub>.ghe.com, else the repository's
    own host (a GitHub Enterprise Server answers its API there)."""
    host = (urlsplit(str(url or "")).hostname or "").lower()
    if host in ("github.com", "www.github.com"):
        return "api.github.com"
    if host.endswith(".ghe.com"):
        return f"api.{host}"
    return host


def _identifier(value: Any, field: str) -> str:
    text = str(value).strip() if isinstance(value, (str, int)) else ""
    if isinstance(value, bool) or not _ID.fullmatch(text):
        raise GitHubAppConfigError(f"github_app.{field} must be a positive number")
    return text


def parse_github_app(config: Any, url: Any) -> GitHubAppOptions:
    """A GitHub App connector's ``github_app`` config, checked against its
    repository URL."""
    value = config.get(CONFIG_KEY) if isinstance(config, Mapping) else None
    if not isinstance(value, Mapping):
        raise GitHubAppConfigError(
            "a GitHub App connector's config needs github_app: {app_id, "
            "installation_id}"
        )
    unknown = sorted(set(value) - {"app_id", "installation_id", "api_base"})
    if unknown:
        raise GitHubAppConfigError(
            f"github_app has fields SRW does not know: {', '.join(unknown)}"
        )
    owner, name = repository_of(url)
    configured = value.get("api_base")
    api_base = _api_base(configured) if configured else default_api_base(url)
    if (urlsplit(api_base).hostname or "").lower() != api_host_for(url):
        # The App's key signs requests to this host: it must be the
        # repository's own API, never another host an edit points it at.
        raise GitHubAppConfigError(
            f"github_app.api_base must be on {api_host_for(url)}, the "
            "repository's own API host"
        )
    return GitHubAppOptions(
        app_id=_identifier(value.get("app_id"), "app_id"),
        installation_id=_identifier(value.get("installation_id"), "installation_id"),
        api_base=api_base,
        owner=owner,
        repository=name,
    )


def installation_permissions(access: str) -> dict[str, str]:
    """The permissions a token for ``access`` is minted with (the lowest for
    a level SRW does not know: fail closed)."""
    return dict(PERMISSIONS.get(access, PERMISSIONS["ReadOnly"]))


def jwt_claims(app_id: str, now: int) -> dict[str, Any]:
    """The App JWT's claims: backdated a minute, under ten minutes long."""
    return {
        "iat": now - JWT_BACKDATE_SECONDS,
        "exp": now - JWT_BACKDATE_SECONDS + JWT_LIFETIME_SECONDS,
        "iss": app_id,
    }


def access_token_request(repository: str, access: str) -> dict[str, Any]:
    """The access-token request body: one repository, the level's
    permissions."""
    return {
        "repositories": [repository],
        "permissions": installation_permissions(access),
    }


__all__ = [
    "API_VERSION",
    "AUTH_METHOD",
    "CONFIG_KEY",
    "GITHUB_COM_API",
    "GitHubAppConfigError",
    "GitHubAppOptions",
    "PERMISSIONS",
    "TOKEN_USERNAME",
    "access_token_request",
    "api_host_for",
    "default_api_base",
    "installation_permissions",
    "jwt_claims",
    "parse_github_app",
    "repository_of",
    "uses_github_app",
]
