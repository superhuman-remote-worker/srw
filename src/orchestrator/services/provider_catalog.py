"""System provider transport, discovery and default-model catalogue operations.

HTTP authorization belongs to the router. Existing stores and discovery services
retain persistence, credentials, provider I/O and candidate import ownership.
"""

from __future__ import annotations

import asyncio
from collections.abc import Collection, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol, TYPE_CHECKING

from fastapi import HTTPException

from shared.helm_provenance import (
    AUTO_PIN_BREADCRUMB,
    RECONCILE_MANIFEST_KEY,
    SOURCE_UI,
    annotate,
    model_identity,
)

from orchestrator.schemas.provider_catalog import (
    AdminDefaultModelSet,
    ApiKeySet,
    LlmEndpointCreate,
    LlmEndpointUpdate,
    SubscriptionModelImport,
    VALID_DEFAULT_MODEL_KINDS,
    VALID_SYSTEM_API_KEY_PROVIDERS,
)
from shared.subscription_routing import is_subscription_endpoint
from orchestrator.services.readiness import try_auto_pin_required_defaults

if TYPE_CHECKING:
    from orchestrator.services.llm_endpoint_probe import ProbeResult
    from orchestrator.services.subscription_discovery import (
        DiscoveryResult,
        ImportOutcome,
        ModelCandidate,
    )


class ProviderCatalogStore(Protocol):
    async def list_system_api_keys(self) -> list[dict[str, Any]]: ...
    async def get_system_api_key(self, provider: str) -> str | None: ...
    async def upsert_system_api_key(
        self,
        provider: str,
        api_key: str,
        key_prefix: str,
        label: str | None = None,
        seeded_from: str | None = None,
        *,
        source: str | None = None,
        helm_value_hash: str | None = None,
    ) -> dict[str, Any]: ...
    async def delete_system_api_key(self, provider: str) -> bool: ...
    async def get_system_api_key_discovery_cache(
        self, provider: str
    ) -> dict[str, Any] | None: ...
    async def set_system_api_key_discovery_cache(
        self, provider: str, payload: dict[str, Any] | None
    ) -> bool: ...
    async def get_system_setting(self, key: str) -> dict[str, Any] | None: ...
    async def list_system_llm_endpoints(self) -> list[dict[str, Any]]: ...
    async def get_system_llm_endpoint(
        self, endpoint_id: str
    ) -> dict[str, Any] | None: ...
    async def create_system_llm_endpoint(
        self,
        label: str,
        base_url: str,
        api_key: str | None,
        key_prefix: str | None,
        transport_kind: str | None = None,
    ) -> dict[str, Any]: ...
    async def update_system_llm_endpoint(
        self,
        endpoint_id: str,
        label: str | None = None,
        base_url: str | None = None,
        api_key: str | None = None,
        key_prefix: str | None = None,
        clear_api_key: bool = False,
        transport_kind: str | None = None,
    ) -> dict[str, Any] | None: ...
    async def delete_system_llm_endpoint(self, endpoint_id: str) -> bool: ...
    async def get_default_llm_model(self, kind: str) -> str | None: ...
    async def set_default_llm_model(
        self, kind: str, model: str | None, *, updated_by: str | None = None
    ) -> None: ...
    async def list_models(
        self,
        *,
        capabilities: list[str] | None = None,
        provider_kind: str | None = None,
        provider_ref: str | None = None,
        enabled_only: bool = False,
    ) -> list[dict[str, Any]]: ...
    async def create_model(
        self,
        *,
        provider_kind: str,
        provider_ref: str,
        model_id: str,
        display_label: str,
        capabilities: list[str] | None = None,
        family: str,
        context_window: int | None = None,
        params_json: dict[str, Any] | None = None,
        enabled: bool = True,
        seeded_from: str | None = None,
        on_conflict_do_nothing: bool = False,
    ) -> dict[str, Any] | None: ...
    async def list_default_pin_capabilities(self) -> list[str]: ...
    async def list_models_by_capability_alphabetical(
        self, capability: str
    ) -> list[dict[str, Any]]: ...
    async def pin_default_llm_model_if_unset(
        self, kind: str, model: str, *, updated_by: str, source: str
    ) -> bool: ...


class EndpointProbe(Protocol):
    async def __call__(self, *, base_url: str, api_key: str | None) -> ProbeResult: ...


class ProviderDiscovery(Protocol):
    @property
    def DISCOVERABLE_PROVIDERS(self) -> Collection[str]: ...
    async def discover_models(
        self, provider: str, api_key: str
    ) -> list[dict[str, Any]]: ...
    def build_cache_payload(
        self, provider: str, candidates: list[dict[str, Any]]
    ) -> dict[str, Any]: ...
    def is_discovery_cache_fresh(self, cache_at: datetime | None) -> bool: ...


class SubscriptionDiscovery(Protocol):
    async def discover_subscription_models(
        self, *, base_url: str, api_key: str | None, catalog_rows: list[dict[str, Any]]
    ) -> DiscoveryResult: ...
    async def import_candidates(
        self,
        *,
        db: ProviderCatalogStore,
        endpoint_id: str,
        candidates: list[ModelCandidate],
        requested_ids: list[str] | None,
        include_review: bool,
    ) -> ImportOutcome: ...


def _validate_llm_endpoint_url(base_url: str, allow_insecure: bool) -> str:
    """Basic URL sanity check. Raises HTTPException(400) on malformed input.

    Rejects non-http(s) schemes (file://, javascript:), empty hosts, and
    http:// URLs unless the caller explicitly opts in via allow_insecure.
    """
    from urllib.parse import urlparse

    try:
        parsed = urlparse(base_url.strip())
    except Exception:
        raise HTTPException(status_code=400, detail="Malformed base_url")

    if parsed.scheme not in ("http", "https"):
        raise HTTPException(
            status_code=400,
            detail=f"base_url scheme must be http or https, got {parsed.scheme!r}",
        )
    if not parsed.netloc:
        raise HTTPException(status_code=400, detail="base_url must include a host")
    if parsed.scheme == "http" and not allow_insecure:
        raise HTTPException(
            status_code=400,
            detail=(
                "base_url uses http:// — set allow_insecure=true to override "
                "(not recommended outside local development)."
            ),
        )
    return parsed.geturl()


def _serialize_endpoint(
    row: dict[str, Any], *, manifest: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Shape an endpoint row for the API response (key_prefix only, no full key).

    The ``models`` key is kept on the response shape for Cockpit
    compatibility but is always empty after the catalog flip — model
    offerings live in the admin-curated ``models`` table now. ``manifest``
    (the ``helm.reconcile`` row) drives the Helm-managed annotation.
    """
    out = {
        "id": str(row["id"]),
        "label": row["label"],
        "base_url": row["base_url"],
        "key_prefix": row.get("key_prefix"),
        # Stable routing marker. The cockpit branches on this, never on the
        # label — renaming an endpoint must not change how it is treated.
        "transport_kind": row.get("transport_kind"),
        "created_at": row["created_at"].isoformat() if row.get("created_at") else None,
        "updated_at": row["updated_at"].isoformat() if row.get("updated_at") else None,
        "models": [],
        "source": row.get("source"),
    }
    return annotate(
        out, manifest=manifest, section="systemEndpoints", identity=row["label"]
    )


def _serialize_system_api_key(
    row: dict[str, Any], *, manifest: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Shape a system_api_keys row for API responses (prefix only)."""
    out = {
        "id": str(row["id"]),
        "provider": row["provider"],
        "key_prefix": row.get("key_prefix"),
        "label": row.get("label"),
        "seeded_from": row.get("seeded_from"),
        "created_at": row["created_at"].isoformat() if row.get("created_at") else None,
        "updated_at": row["updated_at"].isoformat() if row.get("updated_at") else None,
        "source": row.get("source"),
    }
    return annotate(
        out, manifest=manifest, section="systemApiKeys", identity=row["provider"]
    )


def _manifest_value(row: Any) -> dict[str, Any] | None:
    """The ``helm.reconcile`` manifest as a dict, or None when never written."""
    if not isinstance(row, dict):
        return None
    value = row.get("value")
    return value if isinstance(value, dict) else None


def _endpoint_is_subscription_proxy(endpoint: Mapping[str, Any]) -> bool:
    """Whether an endpoint row is the shared subscription proxy."""
    return is_subscription_endpoint(
        transport_kind=endpoint.get("transport_kind"),
        label=endpoint.get("label"),
        base_url=endpoint.get("base_url"),
    )


@dataclass(frozen=True)
class ProviderCatalogService:
    store: ProviderCatalogStore
    discovery: ProviderDiscovery
    probe: EndpointProbe
    subscriptions: SubscriptionDiscovery

    async def _reconcile_manifest(self) -> dict[str, Any] | None:
        return _manifest_value(
            await self.store.get_system_setting(RECONCILE_MANIFEST_KEY)
        )

    async def list_provider_keys(self) -> list[dict[str, Any]]:
        """List system-scoped provider API keys (prefix only, no full keys)."""
        manifest = await self._reconcile_manifest()
        rows = await self.store.list_system_api_keys()
        return [_serialize_system_api_key(r, manifest=manifest) for r in rows]

    async def set_provider_key(self, provider: str, body: ApiKeySet) -> dict[str, Any]:
        """Set or rotate the system-level API key for a provider.

        On success, schedules a non-blocking discovery probe for providers
        we know how to enumerate (see ``discovery_service.DISCOVERABLE_PROVIDERS``).
        The discovery cache is cleared inline before the probe fires so the
        cockpit never shows stale candidates from a previous key. When the
        ``admin.discovery_enabled`` flag is set to ``false``, no probe runs.
        """
        if provider not in VALID_SYSTEM_API_KEY_PROVIDERS:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Invalid provider '{provider}'. Valid: "
                    f"{sorted(VALID_SYSTEM_API_KEY_PROVIDERS)}"
                ),
            )
        row = await self.store.upsert_system_api_key(
            provider=provider,
            api_key=body.api_key,
            key_prefix=body.api_key[:8],
            label=body.label,
            source=SOURCE_UI,
        )
        await self._maybe_schedule_discovery(provider, body.api_key)
        return _serialize_system_api_key(row, manifest=await self._reconcile_manifest())

    async def delete_provider_key(self, provider: str) -> dict[str, str]:
        """Remove the system-level key for a provider."""
        deleted = await self.store.delete_system_api_key(provider)
        if not deleted:
            raise HTTPException(
                status_code=404, detail=f"No system key for provider '{provider}'"
            )
        return {"status": "deleted"}

    async def get_provider_discovery(self, provider: str) -> dict[str, Any]:
        """Return the cached discovery payload for a provider key.

        Powers the post-save confirmation dialog on Admin → Providers. The
        response is ``{ready, fresh, payload, cached_at}``:

        - ``ready=False`` when no probe has completed yet (e.g. the async
          probe scheduled by the PUT side-effect is still running).
        - ``fresh`` reflects the 24h TTL — the cockpit can prompt for an
          explicit rediscover when stale.
        - ``payload`` is the cockpit-ready candidate list shaped by
          :func:`discovery_service.build_cache_payload`.
        """
        if provider not in VALID_SYSTEM_API_KEY_PROVIDERS:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Invalid provider '{provider}'. Valid: "
                    f"{sorted(VALID_SYSTEM_API_KEY_PROVIDERS)}"
                ),
            )
        cached = await self.store.get_system_api_key_discovery_cache(provider)
        if cached is None:
            return {"ready": False, "fresh": False, "payload": None, "cached_at": None}
        cached_at_dt = (
            datetime.fromisoformat(cached["cached_at"])
            if cached.get("cached_at")
            else None
        )
        return {
            "ready": True,
            "fresh": self.discovery.is_discovery_cache_fresh(cached_at_dt),
            "payload": cached.get("payload"),
            "cached_at": cached.get("cached_at"),
        }

    async def rediscover_provider_models(self, provider: str) -> dict[str, Any]:
        """Force-refresh the discovery cache for a provider key.

        Useful when the provider released new models since the cache was
        populated, or when the admin wants to retry after a transient probe
        failure. Returns the freshly-cached payload (synchronous probe — the
        button blocks until results come back, like an explicit "test" click).
        """
        if provider not in VALID_SYSTEM_API_KEY_PROVIDERS:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Invalid provider '{provider}'. Valid: "
                    f"{sorted(VALID_SYSTEM_API_KEY_PROVIDERS)}"
                ),
            )
        if provider not in self.discovery.DISCOVERABLE_PROVIDERS:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Provider '{provider}' has no discovery source. "
                    "Add models manually via Admin → Models."
                ),
            )
        if not await self._discovery_enabled():
            raise HTTPException(
                status_code=409,
                detail=(
                    "Auto-discovery is disabled "
                    "(admin.discovery_enabled = false in system_settings)."
                ),
            )
        api_key = await self.store.get_system_api_key(provider)
        if not api_key:
            raise HTTPException(
                status_code=404, detail=f"No system key for provider '{provider}'"
            )
        candidates = await self.discovery.discover_models(provider, api_key)
        payload = self.discovery.build_cache_payload(provider, candidates)
        await self.store.set_system_api_key_discovery_cache(provider, payload)
        return {
            "ready": True,
            "fresh": True,
            "payload": payload,
            "cached_at": payload["fetched_at"],
        }

    async def _discovery_enabled(self) -> bool:
        """Return whether admin auto-discovery is enabled (system setting,
        default True). Operators flip this off when they want catalog growth
        to be a deliberate manual action."""
        row = await self.store.get_system_setting("admin.discovery_enabled")
        if row is None:
            return True
        value = row.get("value")
        if isinstance(value, dict):
            return bool(value.get("enabled", True))
        return bool(value)

    async def _maybe_schedule_discovery(self, provider: str, api_key: str) -> None:
        """Clear the cache for a key and fire an async probe if applicable.

        The probe runs as a fire-and-forget task so the PUT route returns as
        quickly as today; the cockpit polls ``GET .../discovery`` to render
        the confirmation dialog when results land. Errors are swallowed by
        ``discover_models`` so a failed probe never blocks key-save.
        """
        if provider not in self.discovery.DISCOVERABLE_PROVIDERS:
            return
        if not await self._discovery_enabled():
            return
        await self.store.set_system_api_key_discovery_cache(provider, None)

        async def _probe() -> None:
            candidates = await self.discovery.discover_models(provider, api_key)
            payload = self.discovery.build_cache_payload(provider, candidates)
            await self.store.set_system_api_key_discovery_cache(provider, payload)

        asyncio.create_task(_probe())

    async def list_provider_endpoints(self) -> list[dict[str, Any]]:
        """List system-scoped LLM endpoints with their models."""
        manifest = await self._reconcile_manifest()
        rows = await self.store.list_system_llm_endpoints()
        return [_serialize_endpoint(r, manifest=manifest) for r in rows]

    async def create_provider_endpoint(self, body: LlmEndpointCreate) -> dict[str, Any]:
        """Create a new system-scoped LLM endpoint (visible to every user)."""
        base_url = _validate_llm_endpoint_url(body.base_url, body.allow_insecure)
        key_prefix = body.api_key[:8] if body.api_key else None
        try:
            row = await self.store.create_system_llm_endpoint(
                label=body.label,
                base_url=base_url,
                api_key=body.api_key,
                key_prefix=key_prefix,
                source=SOURCE_UI,
            )
        except Exception as e:
            if "uq_llm_endpoint_label_system" in str(e):
                raise HTTPException(
                    status_code=409,
                    detail=f"A system endpoint labeled {body.label!r} already exists.",
                )
            raise
        row["models"] = []
        return _serialize_endpoint(row, manifest=await self._reconcile_manifest())

    async def update_provider_endpoint(
        self, endpoint_id: str, body: LlmEndpointUpdate
    ) -> dict[str, Any]:
        base_url = None
        if body.base_url is not None:
            base_url = _validate_llm_endpoint_url(body.base_url, body.allow_insecure)
        key_prefix = body.api_key[:8] if body.api_key else None

        row = await self.store.update_system_llm_endpoint(
            endpoint_id=endpoint_id,
            label=body.label,
            base_url=base_url,
            api_key=body.api_key,
            key_prefix=key_prefix,
            clear_api_key=body.clear_api_key and body.api_key is None,
            source=SOURCE_UI,
        )
        if row is None:
            raise HTTPException(status_code=404, detail="System endpoint not found")
        row["models"] = []
        return _serialize_endpoint(row, manifest=await self._reconcile_manifest())

    async def delete_provider_endpoint(self, endpoint_id: str) -> dict[str, str]:
        deleted = await self.store.delete_system_llm_endpoint(endpoint_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="System endpoint not found")
        return {"status": "deleted"}

    async def test_provider_endpoint(self, endpoint_id: str) -> dict[str, Any]:
        """Probe a system endpoint by calling ``GET {base_url}/models`` server-side."""
        endpoint = await self.store.get_system_llm_endpoint(endpoint_id)
        if endpoint is None:
            raise HTTPException(status_code=404, detail="System endpoint not found")

        result = await self.probe(
            base_url=endpoint["base_url"],
            api_key=endpoint.get("api_key"),
        )
        return {
            "ok": result.ok,
            "status": result.status,
            "error": result.error,
            "probe_url": result.probe_url,
        }

    async def _subscription_discovery(
        self, endpoint: Mapping[str, Any]
    ) -> DiscoveryResult:
        """Run enriched discovery for a subscription-proxy endpoint."""
        catalog_rows = await self.store.list_models(
            provider_kind="endpoint", provider_ref=str(endpoint["id"])
        )
        return await self.subscriptions.discover_subscription_models(
            base_url=endpoint["base_url"],
            api_key=endpoint.get("api_key"),
            catalog_rows=catalog_rows,
        )

    async def discover_provider_endpoint_models(
        self, endpoint_id: str
    ) -> dict[str, Any]:
        """Return the model list served by ``GET {base_url}/models`` (admin).

        Discovery is read-only — admins author catalog rows via Admin → Models
        using the endpoint as the transport reference.

        For the subscription proxy the same probe is enriched server-side with
        per-credential attribution and the proxy's static model definitions, so the
        response can say *which connected account* serves each model, which are
        already registered, and which are advertised but unsupported (image/video
        generation) or need review. ``subscription: false`` marks the plain
        endpoint shape, which is unchanged.
        """
        endpoint = await self.store.get_system_llm_endpoint(endpoint_id)
        if endpoint is None:
            raise HTTPException(status_code=404, detail="System endpoint not found")

        if _endpoint_is_subscription_proxy(endpoint):
            result = await self._subscription_discovery(endpoint)
            return {"subscription": True, "status": None, **result.to_public()}

        result = await self.probe(
            base_url=endpoint["base_url"],
            api_key=endpoint.get("api_key"),
        )

        return {
            "subscription": False,
            "ok": result.ok,
            "status": result.status,
            "error": result.error,
            "probe_url": result.probe_url,
            "models": result.models,
        }

    async def import_provider_endpoint_models(
        self, endpoint_id: str, body: SubscriptionModelImport
    ) -> dict[str, Any]:
        """Register discovered subscription models as catalog rows (admin).

        ``model_ids`` selects specific candidates; omitting it means "add all
        supported models". Idempotent in both directions: an already-registered
        model is skipped rather than re-inserted or rewritten, which is what
        preserves admin labels, capability choices, limits and enabled state
        across a rediscovery. Unsupported modalities are refused even when
        explicitly named.
        """
        endpoint = await self.store.get_system_llm_endpoint(endpoint_id)
        if endpoint is None:
            raise HTTPException(status_code=404, detail="System endpoint not found")
        if not _endpoint_is_subscription_proxy(endpoint):
            raise HTTPException(
                status_code=400,
                detail="Bulk import is only available for the subscription proxy.",
            )

        result = await self._subscription_discovery(endpoint)
        if not result.ok:
            # A failed discovery is not an authoritative empty inventory — refuse
            # rather than "import zero models" and report success.
            raise HTTPException(
                status_code=502,
                detail=result.error or "Could not read the subscription inventory.",
            )
        outcome = await self.subscriptions.import_candidates(
            db=self.store,
            endpoint_id=str(endpoint["id"]),
            candidates=result.candidates,
            requested_ids=body.model_ids,
            include_review=body.include_needs_review,
        )
        if outcome.created:
            await try_auto_pin_required_defaults(self.store)
        return outcome.to_public()

    async def list_provider_defaults(self) -> dict[str, str | None]:
        """Return the currently-configured default model IDs for each workload kind."""
        return {
            kind: await self.store.get_default_llm_model(kind)
            for kind in sorted(VALID_DEFAULT_MODEL_KINDS)
        }

    async def set_provider_default(
        self, kind: str, body: AdminDefaultModelSet, *, admin: dict[str, Any]
    ) -> dict[str, str | None]:
        """Set or clear (empty string) the default model for a workload kind."""
        if kind not in VALID_DEFAULT_MODEL_KINDS:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Invalid kind '{kind}'. Valid: {sorted(VALID_DEFAULT_MODEL_KINDS)}"
                ),
            )
        await self.store.set_default_llm_model(
            kind, body.model or None, updated_by=str(admin.get("id")), source=SOURCE_UI
        )
        return {"kind": kind, "model": await self.store.get_default_llm_model(kind)}

    async def helm_managed_overview(self) -> dict[str, Any]:
        """Everything Admin → Models needs to badge Helm-managed rows.

        Returns the ``helm.reconcile`` manifest the seed Job last wrote plus
        per-row provenance for keys, endpoints, catalog rows and default pins
        (``source``, ``managed_by_helm``, ``helm_drift``). Rows are the same
        shapes the list endpoints return, so the cockpit can merge by id.
        """
        manifest = await self._reconcile_manifest()
        keys = [
            _serialize_system_api_key(r, manifest=manifest)
            for r in await self.store.list_system_api_keys()
        ]
        endpoint_rows = await self.store.list_system_llm_endpoints()
        endpoints = [_serialize_endpoint(r, manifest=manifest) for r in endpoint_rows]
        label_by_id = {str(r["id"]): r["label"] for r in endpoint_rows}
        models = []
        for row in await self.store.list_models():
            anchor = (
                label_by_id.get(str(row["provider_ref"]), str(row["provider_ref"]))
                if row.get("provider_kind") == "endpoint"
                else str(row["provider_ref"])
            )
            models.append(
                annotate(
                    {
                        "id": str(row["id"]),
                        "provider_kind": row.get("provider_kind"),
                        "provider_ref": str(row.get("provider_ref")),
                        "model_id": row.get("model_id"),
                        "source": row.get("source"),
                    },
                    manifest=manifest,
                    section="models",
                    identity=model_identity(
                        row.get("provider_kind") or "system", anchor, row["model_id"]
                    ),
                )
            )
        defaults: dict[str, Any] = {}
        for kind in sorted(VALID_DEFAULT_MODEL_KINDS):
            setting = await self.store.get_system_setting(f"llm.default_{kind}_model")
            model = None
            if setting and isinstance(setting.get("value"), dict):
                model = setting["value"].get("model") or None
            defaults[kind] = annotate(
                {"model": model, "source": (setting or {}).get("source")},
                manifest=manifest,
                section="defaults",
                identity=kind,
            )
            # The system chose this pin (readiness auto-pin), not an admin
            # or the chart; Admin → Defaults labels it so.
            defaults[kind]["auto_pinned"] = bool(model) and (
                (setting or {}).get("updated_by") == AUTO_PIN_BREADCRUMB
            )
        return {
            "manifest": manifest
            or {
                "systemApiKeys": [],
                "systemEndpoints": [],
                "models": [],
                "defaults": [],
            },
            "applied_at": (manifest or {}).get("applied_at"),
            "keys": keys,
            "endpoints": endpoints,
            "models": models,
            "defaults": defaults,
        }
