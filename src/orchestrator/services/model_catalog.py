"""Curated model offerings and family metadata without application startup.

The catalogue is read fresh. The legacy reload operation remains a no-op, and
configuration/probe collaborators are supplied by the owning application.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TYPE_CHECKING

from fastapi import HTTPException

from orchestrator.schemas.model_catalog import (
    CatalogModelCreate,
    CatalogModelUpdate,
    VALID_CATALOG_CAPABILITIES,
    VALID_CATALOG_PROVIDER_KINDS,
)
from shared.helm_provenance import (
    RECONCILE_MANIFEST_KEY,
    SOURCE_UI,
    annotate,
    model_identity,
)

from orchestrator.schemas.provider_catalog import (
    VALID_DEFAULT_MODEL_KINDS,
    VALID_SYSTEM_API_KEY_PROVIDERS,
)
from orchestrator.services.provider_catalog import EndpointProbe
from orchestrator.services.readiness import try_auto_pin_required_defaults
from typing import Protocol

if TYPE_CHECKING:
    from orchestrator.services.family_matcher import FamilyDetection


class ModelCatalogStore(Protocol):
    async def get_system_setting(self, key: str) -> dict[str, Any] | None: ...
    async def get_system_api_key(self, provider: str) -> str | None: ...
    async def get_system_llm_endpoint(
        self, endpoint_id: str
    ) -> dict[str, Any] | None: ...
    async def list_system_llm_endpoints(self) -> list[dict[str, Any]]: ...
    async def get_default_llm_model(self, kind: str) -> str | None: ...
    async def list_models(
        self,
        *,
        capabilities: list[str] | None = None,
        provider_kind: str | None = None,
        provider_ref: str | None = None,
        enabled_only: bool = False,
    ) -> list[dict[str, Any]]: ...
    async def get_model(self, model_id: str) -> dict[str, Any] | None: ...
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
        reasoning_level: str | None = None,
        params_json: dict[str, Any] | None = None,
        enabled: bool = True,
        notes: str | None = None,
    ) -> dict[str, Any] | None: ...
    async def update_model(
        self, model_id: str, **fields: Any
    ) -> dict[str, Any] | None: ...
    async def delete_model(self, model_id: str) -> bool: ...
    async def list_default_pin_capabilities(self) -> list[str]: ...
    async def list_models_by_capability_alphabetical(
        self, capability: str
    ) -> list[dict[str, Any]]: ...
    async def pin_default_llm_model_if_unset(
        self, kind: str, model: str, *, updated_by: str, source: str
    ) -> bool: ...


def _catalog_routing(row: Mapping[str, Any]):
    """Routing metadata of a catalog row (empty when it carries none)."""
    from shared.subscription_routing import routing_from_params

    return routing_from_params(row.get("params_json"))


def _normalize_catalog_model_id(
    provider_kind: str, provider_ref: str, model_id: str
) -> str:
    """Prepend the ``openrouter/`` routing prefix for system-anchored
    OpenRouter rows.

    OpenRouter routing in the agent keys off the ``openrouter/`` model-ID
    prefix: ``_create_openrouter_llm`` strips it back to the gateway slug and
    targets ``openrouter.ai``. A system-anchored OpenRouter row whose ID lacks
    the prefix routes to the OpenAI factory default (``api.openai.com``) and
    rejects the ``sk-or-v1`` key. Mirrors the seed convention
    (``db_backed_model_catalog.md``) and ``discovery.py``'s auto-prepend.
    No-op for endpoint rows (routed by their inline base_url) and any
    non-OpenRouter provider.
    """
    if (
        provider_kind == "system"
        and provider_ref == "openrouter"
        and not model_id.lower().startswith("openrouter/")
    ):
        return f"openrouter/{model_id}"
    return model_id


@dataclass(frozen=True)
class ModelCatalogService:
    store: ModelCatalogStore
    probe: EndpointProbe
    get_config_dir: Callable[[], Path]
    load_settings_matrix: Callable[[Path], dict[str, Any]]
    settings_for_family: Callable[[str, str], Any]
    family_detector: Callable[[str], FamilyDetection]
    reasoning_capability: Callable[[str], dict[str, Any]]

    async def _provenance_context(
        self,
    ) -> tuple[dict[str, Any] | None, dict[str, str]]:
        """The ``helm.reconcile`` manifest and endpoint id → label map that
        turn a catalog row into its manifest identity."""
        setting = await self.store.get_system_setting(RECONCILE_MANIFEST_KEY)
        manifest = setting.get("value") if isinstance(setting, dict) else None
        if not isinstance(manifest, dict):
            manifest = None
        labels = {
            str(r["id"]): r["label"]
            for r in await self.store.list_system_llm_endpoints()
        }
        return manifest, labels

    def _serialize_catalog_model(
        self,
        row: dict[str, Any],
        *,
        manifest: dict[str, Any] | None = None,
        endpoint_labels: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Shape a ``models`` row for API responses.

        Single-column source of truth: only the ``capabilities`` array is on
        the wire. Cockpit clients have been migrated to the array form.
        ``manifest`` / ``endpoint_labels`` (see ``_provenance_context``)
        drive the Helm-managed annotation; without them a row is reported as
        unmanaged.
        """

        explicit_window = row.get("context_window")
        provider_ref = str(row["provider_ref"])
        anchor = (
            (endpoint_labels or {}).get(provider_ref, provider_ref)
            if row.get("provider_kind") == "endpoint"
            else provider_ref
        )
        identity = model_identity(
            row.get("provider_kind") or "system", anchor, row["model_id"]
        )
        return annotate(
            self._catalog_row_shape(row, explicit_window),
            manifest=manifest,
            section="models",
            identity=identity,
        )

    def _catalog_row_shape(
        self, row: dict[str, Any], explicit_window: Any
    ) -> dict[str, Any]:
        return {
            "id": str(row["id"]),
            "provider_kind": row["provider_kind"],
            "provider_ref": row["provider_ref"],
            "model_id": row["model_id"],
            "display_label": row["display_label"],
            "capabilities": list(row.get("capabilities") or []),
            "family": row["family"],
            "context_window": explicit_window,
            # Effective window for the Admin → Models "Context" column: the explicit
            # per-model cap when set, else the family default from the config matrix
            # (bundled_settings_for_family merges default ⊕ family; unknown → 128000).
            "resolved_context_window": explicit_window
            or self.settings_for_family(row["family"], "model_max_context_tokens"),
            "context_window_source": "explicit"
            if explicit_window
            else "family_default",
            "reasoning_level": row.get("reasoning_level"),
            "params_json": row.get("params_json"),
            # Routing metadata lifted out of params_json so the Admin UI can show
            # (and an operator can reason about) the client protocol and the
            # subscription source without parsing the JSON blob.
            "client_protocol": _catalog_routing(row).client_protocol,
            "subscription_sources": list(_catalog_routing(row).subscription_sources),
            "routing_needs_review": _catalog_routing(row).needs_review,
            "enabled": row.get("enabled", True),
            "seeded_from": row.get("seeded_from"),
            "source": row.get("source"),
            "notes": row.get("notes"),
            "created_at": row["created_at"].isoformat()
            if row.get("created_at")
            else None,
            "updated_at": row["updated_at"].isoformat()
            if row.get("updated_at")
            else None,
        }

    async def _validate_catalog_provider_ref(
        self, provider_kind: str, provider_ref: str
    ) -> None:
        """Reject catalog inserts/updates pointing at a transport that doesn't
        exist. Keeps the catalog from referencing stale rows after the admin
        deletes a provider key or system endpoint.
        """
        if provider_kind == "system":
            if provider_ref not in VALID_SYSTEM_API_KEY_PROVIDERS:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"Invalid system provider '{provider_ref}'. Valid: "
                        f"{sorted(VALID_SYSTEM_API_KEY_PROVIDERS)}"
                    ),
                )
            existing = await self.store.get_system_api_key(provider_ref)
            if not existing:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"No system_api_keys row for provider '{provider_ref}'. "
                        "Configure via Admin → Providers first."
                    ),
                )
        elif provider_kind == "endpoint":
            endpoint = await self.store.get_system_llm_endpoint(provider_ref)
            if endpoint is None:
                raise HTTPException(
                    status_code=400,
                    detail=f"No system endpoint with id '{provider_ref}'.",
                )
        else:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Invalid provider_kind '{provider_kind}'. Valid: "
                    f"{list(VALID_CATALOG_PROVIDER_KINDS)}"
                ),
            )

    async def list_catalog_models(
        self,
        capability: str | None = None,
        provider_kind: str | None = None,
        provider_ref: str | None = None,
        enabled_only: bool = False,
    ) -> list[dict[str, Any]]:
        """List catalog rows with optional filters.

        The ``capability`` query param narrows by membership — a row matches
        iff its ``capabilities[]`` contains the requested value. Returns full
        row shape.
        """
        if capability is not None and capability not in VALID_CATALOG_CAPABILITIES:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Invalid capability '{capability}'. "
                    f"Valid: {list(VALID_CATALOG_CAPABILITIES)}"
                ),
            )
        capability_filter = [capability] if capability else None
        rows = await self.store.list_models(
            capabilities=capability_filter,
            provider_kind=provider_kind,
            provider_ref=provider_ref,
            enabled_only=enabled_only,
        )
        manifest, labels = await self._provenance_context()
        return [
            self._serialize_catalog_model(r, manifest=manifest, endpoint_labels=labels)
            for r in rows
        ]

    async def create_catalog_model(self, body: CatalogModelCreate) -> dict[str, Any]:
        """Insert a new catalog row.

        Validates that ``provider_ref`` resolves to an existing transport before
        insert. Returns the created row; raises 409 on
        ``(provider_kind, provider_ref, model_id, capability)`` collision.
        """
        await self._validate_catalog_provider_ref(body.provider_kind, body.provider_ref)
        model_id = _normalize_catalog_model_id(
            body.provider_kind, body.provider_ref, body.model_id
        )
        try:
            row = await self.store.create_model(
                provider_kind=body.provider_kind,
                provider_ref=body.provider_ref,
                model_id=model_id,
                display_label=body.display_label,
                capabilities=body.capabilities,
                family=body.family,
                context_window=body.context_window,
                reasoning_level=body.reasoning_level,
                params_json=body.params_json,
                enabled=body.enabled,
                notes=body.notes,
                source=SOURCE_UI,
            )
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            if "uq_model_provider" in str(e):
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"Catalog row for ({body.provider_kind}/{body.provider_ref}, "
                        f"{model_id}) already exists."
                    ),
                )
            raise
        if row is None:
            raise HTTPException(
                status_code=500, detail="Catalog insert returned no row."
            )
        # The first model for a required capability becomes its default, so
        # a fresh install is ready without a separate Defaults step.
        await try_auto_pin_required_defaults(self.store)
        manifest, labels = await self._provenance_context()
        return self._serialize_catalog_model(
            row, manifest=manifest, endpoint_labels=labels
        )

    async def update_catalog_model(
        self, catalog_id: str, body: CatalogModelUpdate
    ) -> dict[str, Any]:
        """Patch a catalog row. Only fields present in the body are written.

        Pass ``null`` to clear an optional column. The validator re-checks the
        transport when ``provider_kind`` or ``provider_ref`` changes.
        """
        fields = body.model_dump(exclude_unset=True)
        existing = None
        if "provider_kind" in fields or "provider_ref" in fields:
            existing = await self.store.get_model(catalog_id)
            if existing is None:
                raise HTTPException(status_code=404, detail="Catalog row not found")
            new_kind = fields.get("provider_kind", existing["provider_kind"])
            new_ref = fields.get("provider_ref", existing["provider_ref"])
            await self._validate_catalog_provider_ref(new_kind, new_ref)
        # Apply the same openrouter/ prefix normalization as create when the
        # model_id is being (re)written. The effective provider_kind/ref may come
        # from this patch or fall back to the existing row.
        if "model_id" in fields:
            if existing is None:
                existing = await self.store.get_model(catalog_id)
                if existing is None:
                    raise HTTPException(status_code=404, detail="Catalog row not found")
            fields["model_id"] = _normalize_catalog_model_id(
                fields.get("provider_kind", existing["provider_kind"]),
                fields.get("provider_ref", existing["provider_ref"]),
                fields["model_id"],
            )
        # An admin write is recorded as such; a Helm-reconciled row then shows
        # as overridden until the next `helm upgrade` re-applies it.
        fields["source"] = SOURCE_UI
        try:
            row = await self.store.update_model(catalog_id, **fields)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            if "uq_model_provider" in str(e):
                raise HTTPException(
                    status_code=409,
                    detail="Update would collide with an existing catalog row.",
                )
            raise
        if row is None:
            raise HTTPException(status_code=404, detail="Catalog row not found")
        # Enabling a row or adding a capability can give a required
        # capability its first model.
        await try_auto_pin_required_defaults(self.store)
        manifest, labels = await self._provenance_context()
        return self._serialize_catalog_model(
            row, manifest=manifest, endpoint_labels=labels
        )

    async def delete_catalog_model(self, catalog_id: str) -> dict[str, Any]:
        """Hard-delete a catalog row. Returns a warning when the row's model_id
        is currently referenced by a ``default_llm_models`` pointer (the pin
        becomes a dangling reference; the resolver's first-enabled-alphabetical
        fallback handles it gracefully).
        """
        existing = await self.store.get_model(catalog_id)
        if existing is None:
            raise HTTPException(status_code=404, detail="Catalog row not found")

        referencing_kinds: list[str] = []
        for kind in sorted(VALID_DEFAULT_MODEL_KINDS):
            pin = await self.store.get_default_llm_model(kind)
            if pin == existing["model_id"]:
                referencing_kinds.append(kind)

        deleted = await self.store.delete_model(catalog_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="Catalog row not found")
        return {
            "status": "deleted",
            "id": catalog_id,
            "warning": (
                f"Default-model pin(s) referenced this row: {referencing_kinds}. "
                "Resolver falls back to first-enabled-alphabetical until repinned."
                if referencing_kinds
                else None
            ),
        }

    async def test_catalog_model(self, catalog_id: str) -> dict[str, Any]:
        """Probe a catalog row's transport.

        For ``provider_kind='endpoint'``, calls ``GET {endpoint.base_url}/models``
        via the existing endpoint-probe helper. For ``provider_kind='system'``,
        confirms the ``system_api_keys`` row exists and returns ``ok=True``
        without round-tripping the provider — vendor-specific health probes
        are out of scope for v1.
        """
        row = await self.store.get_model(catalog_id)
        if row is None:
            raise HTTPException(status_code=404, detail="Catalog row not found")

        if row["provider_kind"] == "endpoint":
            endpoint = await self.store.get_system_llm_endpoint(row["provider_ref"])
            if endpoint is None:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"Catalog row references missing endpoint "
                        f"'{row['provider_ref']}' — repoint or delete."
                    ),
                )
            result = await self.probe(
                base_url=endpoint["base_url"], api_key=endpoint.get("api_key")
            )
            return {
                "ok": result.ok,
                "status": result.status,
                "error": result.error,
                "probe_url": result.probe_url,
            }

        key = await self.store.get_system_api_key(row["provider_ref"])
        if not key:
            return {
                "ok": False,
                "status": None,
                "error": (
                    f"No system_api_keys row for provider '{row['provider_ref']}'."
                ),
                "probe_url": None,
            }
        return {"ok": True, "status": 200, "error": None, "probe_url": None}

    async def list_families(self) -> dict[str, Any]:
        """Return the family keys defined in ``model_config_matrix.yaml`` plus each
        family's default context window.

        Powers the family dropdown on the *Admin → Models* form (so adding a family
        in the YAML doesn't require a frontend rebuild) and the context-window
        field's "family default" placeholder.
        """

        matrix = self.load_settings_matrix(self.get_config_dir())
        families = sorted(k for k in matrix.keys() if isinstance(k, str))
        defaults = {
            fam: self.settings_for_family(fam, "model_max_context_tokens")
            for fam in families
        }
        return {"families": families, "defaults": defaults}

    async def detect_family(self, model_id: str) -> dict[str, str]:
        """Suggest a family for ``model_id`` via the regex matcher.

        Pre-fills the family dropdown on the *Admin → Models* add form and the
        discovery confirmation dialog so admins don't have to memorize the
        mapping. ``source`` is ``"matched"`` for a regex hit and ``"fallback"``
        when no rule matched (the result is ``default`` — works, but quality is
        on the model). Admin can override before saving either way.
        """
        if not model_id or not model_id.strip():
            raise HTTPException(status_code=400, detail="model_id is required")
        detection = self.family_detector(model_id.strip())
        return {
            "family": detection.family,
            "source": detection.source,
        }

    async def list_available_models(
        self, project_id: str | None = None
    ) -> dict[str, Any]:
        """List all models from the admin-curated catalog.

        Returns catalog rows grouped by provider/capability:

        - ``groups`` (chat-capability rows, grouped by provider)
        - ``auxiliary_models`` / ``vision_models`` / ``embedding_models`` /
          ``whisper_models`` / ``tts_models`` / ``search_models`` /
          ``fetch_models`` / ``rerank_models`` (one helper list per
          capability)

        Every row carries ``configured: true`` because the catalog only
        contains rows whose transport (system_api_keys row or system endpoint)
        is admin-managed. The legacy strategic+tactical preset bundle was
        removed in chunk 7 of the models_yaml_removal work — the job-create
        UX picks strategic and tactical models individually now.

        Query params:
            project_id: kept for backward compatibility — no longer affects the
                response shape now that the catalog is the source of truth.
        """
        _ = project_id  # accepted but unused post-flip

        # Catalog rows joined to transport (only enabled rows surface).
        catalog_rows = await self.store.list_models(enabled_only=True)
        system_endpoints = await self.store.list_system_llm_endpoints()
        endpoint_label_by_id: dict[str, str] = {
            str(e["id"]): e["label"] for e in system_endpoints
        }

        # Build (provider_kind, provider_ref) → group payload.
        groups_by_key: dict[tuple[str, str], dict[str, Any]] = {}
        auxiliary: list[dict[str, Any]] = []
        vision: list[dict[str, Any]] = []
        embedding: list[dict[str, Any]] = []
        whisper: list[dict[str, Any]] = []
        tts: list[dict[str, Any]] = []
        search: list[dict[str, Any]] = []
        fetch: list[dict[str, Any]] = []
        rerank: list[dict[str, Any]] = []

        configured_providers: set[str] = set()

        for row in catalog_rows:
            kind = row["provider_kind"]
            ref = row["provider_ref"]
            # Fan-out: under the array model one row contributes to every
            # capability bucket it claims. A multimodal chat row registered as
            # ['chat','auxiliary','vision'] surfaces in the chat groups AND
            # auxiliary_models AND vision_models simultaneously — which is
            # exactly the operator intent (one physical model serves all three).
            capabilities_set = set(row.get("capabilities") or [])
            helper_entry = {
                "id": row["model_id"],
                "label": row["display_label"],
                "configured": True,
            }
            if "auxiliary" in capabilities_set:
                auxiliary.append(helper_entry)
            if "vision" in capabilities_set:
                vision.append(helper_entry)
            if "embedding" in capabilities_set:
                embedding.append(helper_entry)
            if "whisper" in capabilities_set:
                whisper.append(helper_entry)
            if "tts" in capabilities_set:
                tts.append(helper_entry)
            if "search" in capabilities_set:
                search.append(helper_entry)
            if "fetch" in capabilities_set:
                fetch.append(helper_entry)
            if "rerank" in capabilities_set:
                rerank.append(helper_entry)
            # Chat-only path: register the row in its provider group. Embedding-/
            # whisper-/tts-only rows skip this path so the chat dropdowns don't
            # show non-chat models.
            if "chat" not in capabilities_set:
                continue
            key = (kind, ref)
            group = groups_by_key.get(key)
            if group is None:
                if kind == "system":
                    group_name = ref.title() if ref.islower() else ref
                    provider_tag = ref
                    configured_providers.add(ref)
                    group = {
                        "group": group_name,
                        "provider": provider_tag,
                        "configured": True,
                        "models": [],
                    }
                else:  # endpoint
                    label = endpoint_label_by_id.get(ref, f"endpoint:{ref[:8]}")
                    group = {
                        "group": f"System: {label}",
                        "provider": "system",
                        "endpoint_id": ref,
                        "configured": True,
                        "models": [],
                    }
                groups_by_key[key] = group
            group["models"].append(row["model_id"])

        groups = list(groups_by_key.values())

        # Per-model reasoning capability (family-derived) so the Cockpit reasoning
        # control is driven by the catalog instead of hardcoded client logic. The
        # single source of truth is config/model_config_matrix.yaml's `reasoning`
        # block per family. See knowledge-base/knowledge/features/family_centered_reasoning.md.

        reasoning_by_model: dict[str, dict[str, Any]] = {}
        for group in groups:
            for mid in group["models"]:
                if mid in reasoning_by_model:
                    continue
                cap = self.reasoning_capability(mid)
                reasoning_by_model[mid] = {
                    "method": cap.get("method", "none"),
                    "default": cap.get("default"),
                    "options": list(cap.get("options") or []),
                }

        return {
            "groups": groups,
            "auxiliary_models": auxiliary,
            "vision_models": vision,
            "whisper_models": whisper,
            "tts_models": tts,
            "search_models": search,
            "fetch_models": fetch,
            "embedding_models": embedding,
            "rerank_models": rerank,
            "configured_providers": sorted(configured_providers),
            "reasoning_by_model": reasoning_by_model,
        }

    async def reload_model_catalog(self) -> dict[str, str]:
        """No-op kept for backward compat with cockpit clients that still POST.

        Catalog rows live in the DB and ``/api/models`` queries them fresh on
        every call — there is no cache to invalidate. The YAML fallback
        registry that this endpoint used to bounce was deleted in chunk 6;
        the legacy YAML projection cache it then bounced was deleted in
        chunk 7.
        """
        return {"status": "reloaded"}
