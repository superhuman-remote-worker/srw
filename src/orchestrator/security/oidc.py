"""Keycloak OIDC token validation.

Validates access tokens from Keycloak using JWKS (RS256).
JWKS keys are cached and refreshed automatically by PyJWT's PyJWKClient.

A valid signature and issuer only prove that *some* client of the realm was
issued the token. Every client in the realm (Gitea, Nextcloud, OpenCloud's
service account and its user-impersonation tokens, admin-cli, an ID token
handed to any of them) gets tokens with the same signature and issuer, so an
access token is also bound to this API before it is accepted (the token
binding finding of the 2026-08-27 security audit):

- it must be an access token: Keycloak's ``typ`` claim is ``Bearer``, never
  ``ID`` or ``Refresh``;
- and either its ``azp`` (the client it was issued to) is an allowed client,
  or its ``aud`` names an allowed audience.

Keycloak's public-client access tokens carry no ``aud`` of their own (or only
``account``), so ``azp`` is the binding the bundled realm relies on. The
allowed clients are the orchestrator's own BFF client (``KEYCLOAK_CLIENT_ID``,
always allowed: the cookie path validates tokens the orchestrator itself
obtained through it) plus ``OIDC_ALLOWED_CLIENTS``; the allowed audiences are
``OIDC_ALLOWED_AUDIENCES``. The chart sets both (``auth.oidc``); an unset
variable falls back to the bundled realm's names. ``account`` (on nearly every
token Keycloak issues) and ``*`` are never accepted as an audience.

ID tokens and back-channel logout tokens are checked the OIDC way instead:
their ``aud`` must contain the BFF client id they were issued to.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

import jwt

logger = logging.getLogger(__name__)

# OIDC back-channel logout — `events` claim must contain this exact key.
# Defined by the OpenID Connect Back-Channel Logout spec § 2.4.
BACKCHANNEL_LOGOUT_EVENT = "http://schemas.openid.net/event/backchannel-logout"

#: Comma-separated client ids whose access tokens are accepted (``azp``).
OIDC_ALLOWED_CLIENTS_ENV = "OIDC_ALLOWED_CLIENTS"
#: Comma-separated audiences whose access tokens are accepted (``aud``).
OIDC_ALLOWED_AUDIENCES_ENV = "OIDC_ALLOWED_AUDIENCES"
#: The bundled realm's public SPA client (``keycloak.cockpitClientId``).
DEFAULT_ALLOWED_CLIENTS: tuple[str, ...] = ("cockpit",)
#: The bundled realm's bearer-only client for this API.
DEFAULT_ALLOWED_AUDIENCES: tuple[str, ...] = ("orchestrator-api",)
#: Audiences that would accept every client. Keycloak's audience-resolve
#: mapper puts ``account`` on almost every token it issues.
UNBINDABLE_AUDIENCES = frozenset({"account", "*"})

#: Keycloak's ``typ`` claim for an access token and for an ID token.
ACCESS_TOKEN_TYPE = "bearer"
ID_TOKEN_TYPE = "id"


def parse_name_list(raw: str | None, default: Iterable[str]) -> frozenset[str]:
    """A comma-separated list of names; unset is ``default``.

    Set but empty is the empty set, never the default: an operator who
    clears the list gets nothing from it.
    """
    if raw is None:
        return frozenset(default)
    return frozenset(item.strip() for item in raw.split(",") if item.strip())


def token_audiences(claims: dict[str, Any]) -> frozenset[str]:
    """The ``aud`` claim as a set: a string, a list of strings, or nothing."""
    aud = claims.get("aud")
    if isinstance(aud, str):
        return frozenset({aud})
    if isinstance(aud, list | tuple):
        return frozenset(item for item in aud if isinstance(item, str))
    return frozenset()


@dataclass(frozen=True)
class TokenBinding:
    """Which access tokens this API accepts, beyond signature and issuer."""

    allowed_clients: frozenset[str]
    allowed_audiences: frozenset[str]

    @classmethod
    def build(cls, clients: Iterable[str], audiences: Iterable[str]) -> TokenBinding:
        """A binding with blank names dropped and unbindable audiences
        refused (with a warning): they would accept every client's token."""
        unbindable = sorted(set(audiences) & UNBINDABLE_AUDIENCES)
        if unbindable:
            logger.warning(
                "OIDC audience(s) %s ignored: every client's token carries "
                "them, so they cannot bind a token to this API",
                unbindable,
            )
        return cls(
            allowed_clients=frozenset(c for c in clients if c),
            allowed_audiences=frozenset(
                a for a in audiences if a and a not in UNBINDABLE_AUDIENCES
            ),
        )

    def rejection(self, claims: dict[str, Any]) -> str | None:
        """``None`` when the claims are an access token bound to this API,
        else the reason they are not (client ids only, never the subject)."""
        typ = claims.get("typ")
        if not isinstance(typ, str) or typ.lower() != ACCESS_TOKEN_TYPE:
            return f"not an access token (typ={typ!r})"
        azp = claims.get("azp")
        if isinstance(azp, str) and azp in self.allowed_clients:
            return None
        audiences = token_audiences(claims)
        if audiences & self.allowed_audiences:
            return None
        return (
            f"issued to client {azp!r} with audience {sorted(audiences)}; "
            f"allowed clients are {sorted(self.allowed_clients)}, "
            f"allowed audiences {sorted(self.allowed_audiences)}"
        )


class OIDCValidator:
    """Validates Keycloak access tokens via JWKS."""

    def __init__(
        self,
        *,
        client_id: str | None = None,
        allowed_clients: Iterable[str] | None = None,
        allowed_audiences: Iterable[str] | None = None,
    ) -> None:
        self.keycloak_url = os.getenv("KEYCLOAK_URL", "http://localhost:8180")
        self.realm = os.getenv("KEYCLOAK_REALM", "srw")
        # Public-facing URL for token issuer validation — KC_HOSTNAME sets this
        # in tokens. Falls back to KEYCLOAK_URL for local dev where internal = external.
        self._issuer_url = os.getenv("KEYCLOAK_ISSUER_URL", self.keycloak_url)
        self._jwks_client: jwt.PyJWKClient | None = None
        # The BFF's own client (kc_client.py reads the same variable): the
        # audience of its ID and logout tokens, and always an allowed client.
        self.client_id = (
            client_id
            if client_id is not None
            else os.getenv("KEYCLOAK_CLIENT_ID", "cockpit")
        ).strip()
        clients = set(
            allowed_clients
            if allowed_clients is not None
            else parse_name_list(
                os.getenv(OIDC_ALLOWED_CLIENTS_ENV), DEFAULT_ALLOWED_CLIENTS
            )
        )
        clients.add(self.client_id)
        self.binding = TokenBinding.build(
            clients,
            allowed_audiences
            if allowed_audiences is not None
            else parse_name_list(
                os.getenv(OIDC_ALLOWED_AUDIENCES_ENV), DEFAULT_ALLOWED_AUDIENCES
            ),
        )
        logger.info(
            "OIDC access tokens accepted from clients %s or audiences %s",
            sorted(self.binding.allowed_clients),
            sorted(self.binding.allowed_audiences),
        )

    @property
    def issuer(self) -> str:
        return f"{self._issuer_url}/realms/{self.realm}"

    @property
    def is_configured(self) -> bool:
        return bool(self.keycloak_url and self.realm)

    @property
    def jwks_client(self) -> jwt.PyJWKClient:
        if self._jwks_client is None:
            # Fetch JWKS via internal URL (backchannel, fast)
            jwks_uri = (
                f"{self.keycloak_url}/realms/{self.realm}/protocol/openid-connect/certs"
            )
            self._jwks_client = jwt.PyJWKClient(jwks_uri, cache_keys=True)
        return self._jwks_client

    def validate_token(self, token: str) -> dict[str, Any] | None:
        """Validate a Bearer access token and return decoded claims.

        Returns None if validation fails (expired, invalid signature, wrong
        issuer) or the token is not an access token bound to this API (see
        the module docstring). Claims include: sub, email, preferred_username,
        realm_access, etc.
        """
        try:
            signing_key = self.jwks_client.get_signing_key_from_jwt(token)
            claims = jwt.decode(
                token,
                signing_key.key,
                algorithms=["RS256"],
                issuer=self.issuer,
                options={
                    "require": ["exp"],
                    "verify_exp": True,
                    # The audience is checked with azp by TokenBinding below:
                    # Keycloak access tokens often carry no aud for this API.
                    "verify_aud": False,
                },
            )
        except jwt.ExpiredSignatureError:
            logger.debug("OIDC token expired")
            return None
        except jwt.InvalidTokenError as e:
            logger.debug("OIDC token validation failed: %s", e)
            return None
        reason = self.binding.rejection(claims)
        if reason is not None:
            logger.warning("OIDC token rejected: %s", reason)
            return None
        return claims

    def _audience(self) -> str | None:
        # None makes PyJWT refuse any token that carries an aud at all.
        return self.client_id or None

    def decode_id_token(self, id_token: str) -> dict[str, Any] | None:
        """Verify and decode an id_token from the code-exchange response.

        Same JWKS path as access tokens — KC signs both with the realm key.
        Per OIDC Core §3.1.3.7 its ``aud`` must contain the BFF client id it
        was issued to, and it must not be another kind of token.
        Returns None on any failure.
        """
        try:
            signing_key = self.jwks_client.get_signing_key_from_jwt(id_token)
            claims = jwt.decode(
                id_token,
                signing_key.key,
                algorithms=["RS256"],
                issuer=self.issuer,
                audience=self._audience(),
                options={"require": ["exp"], "verify_exp": True},
            )
        except jwt.ExpiredSignatureError:
            logger.debug("OIDC id_token expired")
            return None
        except jwt.InvalidTokenError as e:
            logger.debug("OIDC id_token validation failed: %s", e)
            return None
        typ = claims.get("typ")
        if typ is not None and str(typ).lower() != ID_TOKEN_TYPE:
            logger.warning("OIDC id_token rejected: not an ID token (typ=%r)", typ)
            return None
        return claims

    def verify_logout_token(self, logout_token: str) -> dict[str, Any] | None:
        """Verify a back-channel logout token per OIDC BCL §2.6.

        Keycloak POSTs this token to /auth/backchannel-logout when a user
        logs out at the IdP. We MUST verify signature + issuer + audience
        (the BFF client the logout is addressed to) + that the ``events``
        claim contains the backchannel-logout key + that a subject
        identifier (sub or sid) is present + that ``nonce`` is absent
        (spec § 2.4).

        Returns the claims dict on success, None on any failure (treat as
        "ignore this request" — we don't want to leak which sids exist).
        """
        try:
            signing_key = self.jwks_client.get_signing_key_from_jwt(logout_token)
            claims = jwt.decode(
                logout_token,
                signing_key.key,
                algorithms=["RS256"],
                issuer=self.issuer,
                audience=self._audience(),
                options={"verify_exp": True},
            )
        except jwt.InvalidTokenError as e:
            logger.warning("Back-channel logout token validation failed: %s", e)
            return None

        # Spec-required structural checks beyond what PyJWT enforces:
        events = claims.get("events") or {}
        if not isinstance(events, dict) or BACKCHANNEL_LOGOUT_EVENT not in events:
            logger.warning("Back-channel logout token missing required event claim")
            return None
        if "nonce" in claims:
            logger.warning("Back-channel logout token must not carry nonce")
            return None
        if not claims.get("sub") and not claims.get("sid"):
            logger.warning("Back-channel logout token has neither sub nor sid")
            return None
        return claims


# Singleton instance
oidc_validator = OIDCValidator()
