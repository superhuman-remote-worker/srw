"""Per-user catalog and credential resolution for capability-scoped calls."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from shared.runtime.core.model_registry import (
    UnknownModelError,
    resolve_model as _resolve_model,
)


@dataclass(frozen=True, slots=True)
class CapabilityCredentials:
    """One resolved catalog row plus the transport needed to call it."""

    model: str
    base_url: str | None = None
    api_key: str | None = None
    provider: str | None = None
    params: dict[str, Any] = field(default_factory=dict)
    catalog_id: str | None = None
    # The row's ``served_model_id``: the name the provider is sent when it
    # differs from the catalog id in ``model``.
    served_model: str | None = None

    @property
    def wire_model(self) -> str:
        """The model name to send to the provider."""
        return self.served_model or self.model


async def resolve_capability_credentials(
    *,
    capability: str,
    user_settings: dict[str, Any],
    user_id: str | None,
    resolved_keys: dict[str, str],
    postgres_db,
    setting_key: str | None = None,
) -> CapabilityCredentials | None:
    """Resolve a capability selection to its parsed catalog row and transport.

    Selection is user setting, then the system capability resolver. Catalog
    rows are fetched through ``resolve_catalog_model`` so JSONB ``params_json``
    is normalized by ``postgres._row_to_model`` before provider selection.
    ``setting_key`` lets a second slot reuse this exact chain without inventing
    another credential mechanism.
    """

    user_key = setting_key or f"default_{capability}_model"
    model_id = user_settings.get(user_key)
    if not model_id:
        default_kind = user_key.removeprefix("default_").removesuffix("_model")
        model_id = await postgres_db.resolve_default_for_capability(default_kind)
    if not model_id:
        return None

    row = await postgres_db.resolve_catalog_model(model_id, capability=capability)
    if row is not None:
        params_value = row.get("params_json")
        params = dict(params_value) if isinstance(params_value, dict) else {}
        configured_provider = params.get("provider")
        provider = (
            configured_provider.strip()
            if isinstance(configured_provider, str) and configured_provider.strip()
            else None
        )
        provider_ref = row.get("provider_ref")
        if provider is None and row.get("provider_kind") == "system":
            provider = str(provider_ref) if provider_ref else None

        base_url = row.get("endpoint_base_url")
        api_key = row.get("api_key")
        # A system catalog transport may be overridden by a per-user/project
        # provider key. Endpoint rows remain authoritative for their own key.
        if row.get("provider_kind") == "system" and provider_ref:
            api_key = resolved_keys.get(str(provider_ref), api_key)

        return CapabilityCredentials(
            model=str(model_id),
            base_url=str(base_url) if base_url else None,
            api_key=str(api_key) if api_key else None,
            provider=provider,
            params=params,
            catalog_id=str(row["catalog_id"]) if row.get("catalog_id") else None,
            served_model=row.get("served_model_id") or None,
        )

    # Compatibility for a model resolved outside the DB catalog. The default
    # resolver is catalog-backed, but explicit legacy user settings can survive
    # an upgrade and should retain their previous transport behavior.
    try:
        meta = await _resolve_model(
            model_id,
            user_id=user_id,
            capability=capability,
        )
    except UnknownModelError:
        meta = None

    base_url = None
    api_key = None
    provider = None
    if meta is not None:
        provider = meta.api_key_ref or meta.provider
        if meta.endpoint_id:
            endpoint_row = await postgres_db.get_user_llm_endpoint(meta.endpoint_id)
            if endpoint_row:
                base_url = endpoint_row.get("base_url")
                api_key = endpoint_row.get("api_key")
        elif meta.api_key_ref:
            api_key = resolved_keys.get(meta.api_key_ref)

    return CapabilityCredentials(
        model=str(model_id),
        base_url=str(base_url) if base_url else None,
        api_key=str(api_key) if api_key else None,
        provider=provider,
        served_model=meta.served_model_id if meta is not None else None,
    )
