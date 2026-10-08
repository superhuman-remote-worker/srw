"""Pydantic settings models for main-cloud backends.

Defines a discriminated union over the per-backend settings classes so that
a single ``load_main_cloud_config()`` call can validate deploy-time env vars
against the right schema for whichever backend is selected.

Phase 1.5 lands the ``NextcloudSettings`` / ``OpenCloudSettings`` /
``MS365Settings`` classes and the loader; only ``NextcloudSettings`` is
actually consumed by ``NextcloudBackend`` today — the other two exist so
Phase 2 (OpenCloud adapter) and Phase 5 (MS365 adapter) can start from a
populated settings class instead of re-inventing env-var plumbing.

See §4.2 of ``knowledge-base/knowledge/features/main_cloud_abstraction.md``.
"""

from __future__ import annotations

import logging
import os
from typing import Annotated, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, SecretStr, model_validator

from orchestrator.services.cloud.backend_instance_authority import (
    MainCloudBackendInstanceAuthority,
)


class NextcloudSettings(BaseModel):
    """Env-var-loaded settings for ``NextcloudBackend``.

    Legacy ``NEXTCLOUD_*`` env vars map onto these fields (see
    ``load_main_cloud_config`` for the alias table).
    """

    model_config = ConfigDict(extra="forbid")

    backend_id: Literal["nextcloud"] = "nextcloud"
    base_url: HttpUrl
    public_url: HttpUrl
    admin_user: str
    admin_password: SecretStr
    agent_user: str
    agent_password: SecretStr
    oidc_client_secret: Optional[SecretStr] = None
    protected_effect_url: Optional[HttpUrl] = None
    protected_effect_config_sha256: Optional[str] = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    protected_effect_hmac_key: Optional[SecretStr] = None

    @model_validator(mode="after")
    def _protected_effect_lane_is_complete(self) -> NextcloudSettings:
        coordinates = (
            self.protected_effect_url,
            self.protected_effect_config_sha256,
            self.protected_effect_hmac_key,
        )
        if any(value is not None for value in coordinates) and not all(
            value is not None for value in coordinates
        ):
            raise ValueError(
                "Nextcloud protected-effect URL, config digest, and HMAC key "
                "must be configured together"
            )
        if (
            self.protected_effect_hmac_key is not None
            and len(self.protected_effect_hmac_key.get_secret_value().encode("utf-8"))
            < 32
        ):
            raise ValueError(
                "Nextcloud protected-effect HMAC key must contain at least 32 bytes"
            )
        return self


class OpenCloudSettings(BaseModel):
    """Env-var-loaded settings for the (upcoming) OpenCloud adapter."""

    model_config = ConfigDict(extra="forbid")

    backend_id: Literal["opencloud"] = "opencloud"
    base_url: HttpUrl
    public_url: HttpUrl
    keycloak_issuer: HttpUrl
    keycloak_client_id: str
    keycloak_client_secret: SecretStr
    # Must match OpenCloud's built-in role assignment claim values. The
    # proxy driver's role_mapping is not configurable via env vars, so
    # the Keycloak group name has to line up with OpenCloud's defaults
    # (`opencloudAdmin`, `opencloudSpaceadmin`, `opencloudUser`,
    # `opencloudGuest`). Override only if you've mounted a custom
    # proxy.yaml with a different role_mapping.
    admin_role_claim_value: str = "opencloudAdmin"
    default_quota_bytes: Optional[int] = None
    # Local-dev only: rclone mounts pass --no-check-certificate so the tus
    # upload hop (ocdav redirects PATCHes to the PUBLIC data-gateway URL,
    # which presents the mkcert edge cert on local k3d) doesn't fail TLS
    # verification. All other mount traffic uses the internal plain-HTTP
    # service URL and is unaffected. Never enable on a real deployment.
    mount_insecure_tls: bool = False


class MS365Settings(BaseModel):
    """Env-var-loaded settings for the (future) Microsoft 365 adapter."""

    model_config = ConfigDict(extra="forbid")

    backend_id: Literal["ms365"] = "ms365"
    tenant_id: str
    client_id: str
    client_secret: SecretStr
    site_id: Optional[str] = None


MainCloudConfig = Annotated[
    Union[NextcloudSettings, OpenCloudSettings, MS365Settings],
    Field(discriminator="backend_id"),
]


class _MainCloudConfigHolder(BaseModel):
    """Internal wrapper so Pydantic resolves the discriminated union."""

    model_config = ConfigDict(extra="forbid")

    settings: MainCloudConfig


def _detect_legacy_nextcloud_mode() -> bool:
    """Return True if the env looks like a pre-abstraction Nextcloud deploy.

    Heuristic: no ``MAIN_CLOUD_BACKEND`` set AND at least one ``NEXTCLOUD_*``
    var is set (beyond the port override). Phase 3 uses this to keep
    existing Nextcloud deployments on Nextcloud after the greenfield
    default flipped to OpenCloud — operators upgrading in place do not
    have to add ``MAIN_CLOUD_BACKEND=nextcloud`` to their ``.env``
    unless they explicitly want to migrate.

    ``NEXTCLOUD_PORT`` alone does not count as "legacy" since a
    deployment may publish the port even while OpenCloud is primary.
    """
    if os.getenv("MAIN_CLOUD_BACKEND"):
        return False
    for key in os.environ:
        if not key.startswith("NEXTCLOUD_"):
            continue
        if key == "NEXTCLOUD_PORT":
            continue
        return True
    return False


def _pick(*names: str, default: Optional[str] = None) -> Optional[str]:
    """First non-empty env var from ``names``, else ``default``."""
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return default


def load_main_cloud_config(
    *,
    backend_override: Optional[str] = None,
) -> MainCloudConfig:
    """Load and validate the deploy-time main-cloud config.

    The configuration is the deployment's environment, which the Helm chart
    renders (``configmap.yaml``: ``MAIN_CLOUD_BACKEND`` and friends). There
    is no live overlay: the admin connection form and its API are gone
    (main_cloud_as_connectors.md, "Configuration: Helm only").

    Resolution order for the backend id:

    1. ``backend_override`` parameter (used by ``MainCloudRouter`` to
       instantiate a cached legacy backend for non-destructive switching).
    2. ``MAIN_CLOUD_BACKEND`` env var.
    3. ``_detect_legacy_nextcloud_mode()`` heuristic.
    4. Default → ``opencloud`` (Phase 3 greenfield).

    Each field is resolved as env var > hardcoded default.

    Raises
    ------
    pydantic.ValidationError:
        If required env vars are missing or invalid.
    ValueError:
        If ``backend_id`` is unknown.
    """

    backend_id = backend_override or os.getenv("MAIN_CLOUD_BACKEND")
    if not backend_id:
        # Phase 3: the greenfield default is OpenCloud. Deployments that
        # already have NEXTCLOUD_* env vars set (i.e. in-place upgrades
        # from Phase 1/2) keep running on Nextcloud via the legacy
        # heuristic, so nobody gets silently migrated by the upgrade.
        backend_id = "nextcloud" if _detect_legacy_nextcloud_mode() else "opencloud"

    if backend_id == "nextcloud":
        base_url = _pick(
            "MAIN_CLOUD_URL", "NEXTCLOUD_URL", default="http://localhost:8800"
        )
        public_url = _pick(
            "MAIN_CLOUD_PUBLIC_URL", "NEXTCLOUD_PUBLIC_URL", default=base_url
        )
        raw = {
            "settings": {
                "backend_id": "nextcloud",
                "base_url": base_url,
                "public_url": public_url,
                "admin_user": _pick(
                    "MAIN_CLOUD_ADMIN_USER", "NEXTCLOUD_ADMIN_USER", default="admin"
                ),
                "admin_password": _pick(
                    "MAIN_CLOUD_ADMIN_PASSWORD",
                    "NEXTCLOUD_ADMIN_PASSWORD",
                )
                or "admin",
                "agent_user": _pick(
                    "MAIN_CLOUD_AGENT_USER",
                    "NEXTCLOUD_AGENT_USER",
                    default="agent-service",
                ),
                "agent_password": _pick(
                    "MAIN_CLOUD_AGENT_PASSWORD",
                    "NEXTCLOUD_AGENT_PASSWORD",
                )
                or "agent-service-dev",
                "oidc_client_secret": _pick("NEXTCLOUD_OIDC_CLIENT_SECRET"),
                "protected_effect_url": _pick("NEXTCLOUD_PROTECTED_EFFECT_URL"),
                "protected_effect_config_sha256": _pick(
                    "NEXTCLOUD_PROTECTED_EFFECT_CONFIG_SHA256"
                ),
                "protected_effect_hmac_key": _pick(
                    "NEXTCLOUD_PROTECTED_EFFECT_HMAC_KEY",
                ),
            }
        }
    elif backend_id == "opencloud":
        # Defaults target a locally reachable OpenCloud (host-published
        # ports) so a fresh .env "just works" for host-side development.
        # Real deployments override every value via Helm secrets / Vault —
        # same convention as the Nextcloud branch above. The client_secret
        # default matches the placeholder the Keycloak realm ships with.
        oc_base_url = _pick(
            "MAIN_CLOUD_URL", "OPENCLOUD_URL", default="http://localhost:9200"
        )
        oc_public_url = _pick(
            "MAIN_CLOUD_PUBLIC_URL", "OPENCLOUD_PUBLIC_URL", default=oc_base_url
        )
        raw = {
            "settings": {
                "backend_id": "opencloud",
                "base_url": oc_base_url,
                "public_url": oc_public_url,
                "keycloak_issuer": _pick(
                    "OPENCLOUD_KEYCLOAK_ISSUER",
                    default="http://localhost:8180/realms/srw",
                ),
                "keycloak_client_id": os.getenv(
                    "OPENCLOUD_KEYCLOAK_CLIENT_ID",
                    "opencloud-orchestrator",
                ),
                "keycloak_client_secret": _pick("OPENCLOUD_KEYCLOAK_CLIENT_SECRET")
                or "opencloud-orchestrator-local-secret",
                "admin_role_claim_value": os.getenv(
                    "OPENCLOUD_ADMIN_ROLE_CLAIM_VALUE", "opencloudAdmin"
                ),
                "default_quota_bytes": _parse_int(
                    os.getenv("OPENCLOUD_DEFAULT_QUOTA_BYTES")
                ),
                "mount_insecure_tls": os.getenv("OPENCLOUD_MOUNT_INSECURE_TLS", "")
                .strip()
                .lower()
                in ("1", "true", "yes"),
            }
        }
    elif backend_id == "ms365":
        raw = {
            "settings": {
                "backend_id": "ms365",
                "tenant_id": os.getenv("MS365_TENANT_ID"),
                "client_id": os.getenv("MS365_CLIENT_ID"),
                "client_secret": _pick("MS365_CLIENT_SECRET"),
                "site_id": os.getenv("MS365_SITE_ID"),
            }
        }
    else:
        raise ValueError(
            f"unknown main cloud backend: {backend_id!r} "
            "(known: nextcloud, opencloud, ms365)"
        )

    holder = _MainCloudConfigHolder.model_validate(raw)
    return holder.settings


def _parse_int(value: Optional[str]) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except ValueError:
        return None


# Required (non-optional) secret fields per backend → the ordered env-var
# fallbacks ``load_main_cloud_config`` reads for each. Keep in lockstep with
# the secret ``_pick(...)`` calls in the loader above. ``oidc_client_secret`` is
# intentionally absent — it is ``Optional`` on ``NextcloudSettings``.
_REQUIRED_SECRET_ENVS: dict[str, dict[str, tuple[str, ...]]] = {
    "nextcloud": {
        "admin_password": ("MAIN_CLOUD_ADMIN_PASSWORD", "NEXTCLOUD_ADMIN_PASSWORD"),
        "agent_password": ("MAIN_CLOUD_AGENT_PASSWORD", "NEXTCLOUD_AGENT_PASSWORD"),
    },
    "opencloud": {
        "keycloak_client_secret": ("OPENCLOUD_KEYCLOAK_CLIENT_SECRET",),
    },
    "ms365": {
        "client_secret": ("MS365_CLIENT_SECRET",),
    },
}


def missing_secret_envs(backend_id: str) -> list[dict]:
    """Report which *required* secret env vars are unset for a backend config.

    Mirrors the secret resolution of ``load_main_cloud_config`` but only
    inspects *presence* — it never reads a secret's value and never falls back
    to the built-in dev defaults the loader uses (``admin`` /
    ``agent-service-dev`` / ``opencloud-orchestrator-local-secret``).

    The loader keeps those dev defaults on purpose so a bare ``.env`` or a test
    run "just works". This helper lets startup warn about a backend whose real
    secrets are not wired, instead of silently connecting with dev credentials
    and failing at the first cloud call. See
    ``knowledge-base/knowledge/issues/main_cloud.md`` Issue 5.

    Returns one ``{"field", "env_var", "checked"}`` entry per missing secret; an
    empty list means every required secret resolves to a non-empty value.
    """
    required = _REQUIRED_SECRET_ENVS.get(backend_id, {})
    if not required:
        return []

    missing: list[dict] = []
    for field, env_fallbacks in required.items():
        if not _pick(*env_fallbacks):
            missing.append(
                {
                    "field": field,
                    "env_var": env_fallbacks[-1],
                    "checked": list(env_fallbacks),
                }
            )
    return missing


def warn_main_cloud_missing_secret_config(
    missing: list[dict], *, logger: logging.Logger
) -> None:
    """Warn about absent cloud credentials without logging any configuration value.

    ``missing`` can contain an operator-provided ``credentials_ref`` identity.
    It is only used as a presence signal here; the warning keeps static cloud,
    environment, and schema context for operators without copying that identity
    or the active backend identifier into a log record.
    """
    if not missing:
        return
    logger.warning(
        "The active main cloud backend has required secret environment "
        "configuration unset and is running on built-in DEV credentials; it "
        "will fail at the first cloud call. Check the configured backend's "
        "required credential fields and set them via Helm/Vault."
    )


def main_cloud_routing_snapshot(settings: MainCloudConfig) -> dict:
    """Return the complete non-secret routing snapshot for DB adoption."""

    if isinstance(settings, NextcloudSettings):
        return {
            "version": 1,
            "backend_id": "nextcloud",
            "base_url": str(settings.base_url),
            "public_url": str(settings.public_url),
            "admin_user": settings.admin_user,
            "agent_user": settings.agent_user,
            "protected_effect_url": (
                str(settings.protected_effect_url).rstrip("/")
                if settings.protected_effect_url is not None
                else None
            ),
            "protected_effect_config_sha256": (settings.protected_effect_config_sha256),
        }
    if isinstance(settings, OpenCloudSettings):
        return {
            "version": 1,
            "backend_id": "opencloud",
            "base_url": str(settings.base_url),
            "public_url": str(settings.public_url),
            "keycloak_issuer": str(settings.keycloak_issuer),
            "keycloak_client_id": settings.keycloak_client_id,
            "admin_role_claim_value": settings.admin_role_claim_value,
            "default_quota_bytes": settings.default_quota_bytes,
            "mount_insecure_tls": settings.mount_insecure_tls,
        }
    raise ValueError("MS365 does not have a main-cloud instance contract")


def main_cloud_secret_references(backend_id: str) -> dict[str, str]:
    """Resolve the env *names* that supplied the active backend's secrets.

    Secret values are never returned. Required fields fail closed when their
    source is only a built-in development default: such a value cannot be
    reconstructed safely by another replica or by historical cleanup.
    """

    required = _REQUIRED_SECRET_ENVS.get(backend_id)
    if required is None:
        raise ValueError(f"unknown main cloud backend: {backend_id!r}")
    optional: dict[str, tuple[str, ...]] = (
        {
            "oidc_client_secret": ("NEXTCLOUD_OIDC_CLIENT_SECRET",),
            "protected_effect_hmac_key": ("NEXTCLOUD_PROTECTED_EFFECT_HMAC_KEY",),
        }
        if backend_id == "nextcloud"
        else {}
    )

    refs: dict[str, str] = {}
    for field, fallbacks in {**required, **optional}.items():
        env_name = next((name for name in fallbacks if os.getenv(name)), None)
        if env_name is None:
            if field in required:
                raise ValueError(
                    f"required secret env is unset for {backend_id}.{field}"
                )
            continue
        refs[field] = f"env:{env_name}"
    return refs


def load_main_cloud_config_from_instance(
    authority: MainCloudBackendInstanceAuthority,
) -> MainCloudConfig:
    """Rebuild one historical adapter only from its retained snapshot.

    There is deliberately no fallback to the active overlay or legacy env
    aliases. Every required secret must resolve through the exact reference
    recorded on this backend instance.
    """

    if not isinstance(authority, MainCloudBackendInstanceAuthority):
        raise ValueError("main-cloud backend instance authority is missing")
    routing = authority.routing
    refs = authority.secret_refs

    def _secret(field: str, *, required: bool = True) -> str | None:
        reference = refs.get(field)
        if reference is None:
            if required:
                raise ValueError(f"main-cloud secret reference {field!r} is missing")
            return None
        value = os.getenv(reference.removeprefix("env:"))
        if not value:
            raise ValueError(f"main-cloud secret reference {field!r} is unresolved")
        return value

    if authority.backend_id == "nextcloud":
        return NextcloudSettings(
            backend_id="nextcloud",
            base_url=routing["base_url"],
            public_url=routing["public_url"],
            admin_user=routing["admin_user"],
            admin_password=_secret("admin_password"),
            agent_user=routing["agent_user"],
            agent_password=_secret("agent_password"),
            oidc_client_secret=_secret("oidc_client_secret", required=False),
            protected_effect_url=routing["protected_effect_url"],
            protected_effect_config_sha256=routing["protected_effect_config_sha256"],
            protected_effect_hmac_key=_secret(
                "protected_effect_hmac_key",
                required=False,
            ),
        )
    if authority.backend_id == "opencloud":
        return OpenCloudSettings(
            backend_id="opencloud",
            base_url=routing["base_url"],
            public_url=routing["public_url"],
            keycloak_issuer=routing["keycloak_issuer"],
            keycloak_client_id=routing["keycloak_client_id"],
            keycloak_client_secret=_secret("keycloak_client_secret"),
            admin_role_claim_value=routing["admin_role_claim_value"],
            default_quota_bytes=routing["default_quota_bytes"],
            mount_insecure_tls=routing["mount_insecure_tls"],
        )
    raise ValueError(f"unsupported main-cloud backend {authority.backend_id!r}")
