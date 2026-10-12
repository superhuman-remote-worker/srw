"""Tests for the Admin → Models API + accessor surface (Phase C).

Covers:
- Every new ``/api/admin/providers/models/*`` route (and the families endpoint)
  is registered on the FastAPI app.
- Pydantic bodies (``CatalogModelCreate``, ``CatalogModelUpdate``) accept the
  expected shapes and reject invalid enums / missing required fields.
- ``params_json={"temperature": 0}`` and ``context_window=0`` round-trip as
  themselves through the DB accessors.
- ``resolve_catalog_model`` JOINs to the right transport (system vs endpoint)
  and prefers the system row when both are present.
- ``list_models_by_capability_alphabetical`` filters on enabled and sorts.
"""

from __future__ import annotations

import json
import os
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest

os.environ.setdefault("VECTOR_DB_URL", "postgresql://test@localhost/test")

from orchestrator.schemas.model_catalog import (  # noqa: E402
    VALID_CATALOG_PROVIDER_KINDS,
    VALID_CATALOG_CAPABILITIES,
    CatalogModelCreate,
    CatalogModelUpdate,
)
from orchestrator.services.model_catalog import (  # noqa: E402
    ModelCatalogService,
    _normalize_catalog_model_id,
)
from orchestrator.database.postgres import PostgresDB  # noqa: E402


# ---------------------------------------------------------------------------
# Route registration
# ---------------------------------------------------------------------------


CATALOG_ROUTES = {
    ("GET", "/api/admin/providers/models"),
    ("POST", "/api/admin/providers/models"),
    ("PATCH", "/api/admin/providers/models/{catalog_id}"),
    ("DELETE", "/api/admin/providers/models/{catalog_id}"),
    ("POST", "/api/admin/providers/models/{catalog_id}/test"),
    ("GET", "/api/admin/families"),
    ("GET", "/api/admin/families/detect"),
}


def _registered_routes() -> set[tuple[str, str]]:
    from orchestrator.main import app
    from tests._route_inventory import mounted_routes

    return mounted_routes(app)


class TestCatalogRoutesRegistered:
    def test_every_catalog_admin_route_is_wired(self):
        registered = _registered_routes()
        missing = [r for r in CATALOG_ROUTES if r not in registered]
        assert not missing, f"missing catalog admin routes: {missing}"


class TestCatalogConstants:
    def test_capability_enum_locked(self):
        assert VALID_CATALOG_CAPABILITIES == (
            "chat",
            "auxiliary",
            "embedding",
            "vision",
            "whisper",
            "tts",
            "search",
            "fetch",
            "rerank",
        )

    def test_provider_kinds_locked(self):
        assert VALID_CATALOG_PROVIDER_KINDS == ("system", "endpoint")


# ---------------------------------------------------------------------------
# Pydantic bodies
# ---------------------------------------------------------------------------


class TestCatalogModelCreate:
    def _ok_payload(self, **overrides):
        base = {
            "provider_kind": "system",
            "provider_ref": "anthropic",
            "model_id": "claude-opus-4-7",
            "display_label": "Claude Opus 4.7",
            "capabilities": ["chat", "auxiliary"],
            "family": "claude-opus",
        }
        base.update(overrides)
        return base

    def test_minimal_payload_accepted(self):
        body = CatalogModelCreate(**self._ok_payload())
        assert body.provider_kind == "system"
        assert body.enabled is True  # default
        assert body.capabilities == ["chat", "auxiliary"]

    def test_endpoint_kind_accepted(self):
        body = CatalogModelCreate(
            **self._ok_payload(
                provider_kind="endpoint",
                provider_ref="11111111-1111-1111-1111-111111111111",
            )
        )
        assert body.provider_kind == "endpoint"

    def test_invalid_capability_rejected(self):
        with pytest.raises(Exception):
            CatalogModelCreate(**self._ok_payload(capabilities=["banana"]))

    def test_empty_capabilities_array_rejected(self):
        """min_length=1 on the Pydantic field — every row must claim at
        least one role."""
        with pytest.raises(Exception):
            CatalogModelCreate(**self._ok_payload(capabilities=[]))

    def test_legacy_capability_singular_field_rejected(self):
        """The chunk 7 cleanup dropped the legacy `capability` field. Older
        clients posting the singular form get a Pydantic validation error
        pointing at the new `capabilities` array."""
        payload = self._ok_payload()
        payload.pop("capabilities")
        payload["capability"] = "chat"
        with pytest.raises(Exception):
            CatalogModelCreate(**payload)

    def test_invalid_provider_kind_rejected(self):
        with pytest.raises(Exception):
            CatalogModelCreate(**self._ok_payload(provider_kind="user"))

    def test_required_fields_enforced(self):
        for missing_field in (
            "provider_kind",
            "provider_ref",
            "model_id",
            "display_label",
            "capabilities",
            "family",
        ):
            payload = self._ok_payload()
            payload.pop(missing_field)
            with pytest.raises(Exception):
                CatalogModelCreate(**payload)  # type: ignore[arg-type]

    def test_explicit_zero_context_window_preserved(self):
        body = CatalogModelCreate(**self._ok_payload(context_window=0))
        # Must not be coerced to None — explicit 0 is the override signal.
        assert body.context_window == 0

    def test_explicit_zero_temperature_preserved(self):
        body = CatalogModelCreate(**self._ok_payload(params_json={"temperature": 0}))
        assert body.params_json == {"temperature": 0}


class TestCatalogModelUpdate:
    def test_empty_update_accepted(self):
        # No fields set — caller can patch nothing without error.
        body = CatalogModelUpdate()
        # All Optional fields default to None.
        assert body.enabled is None

    def test_partial_update_only_sets_passed_fields(self):
        body = CatalogModelUpdate(enabled=False)
        as_dict = body.model_dump(exclude_unset=True)
        assert as_dict == {"enabled": False}

    def test_explicit_zero_round_trip(self):
        body = CatalogModelUpdate(context_window=0, params_json={"temperature": 0})
        as_dict = body.model_dump(exclude_unset=True)
        assert as_dict == {"context_window": 0, "params_json": {"temperature": 0}}


# ---------------------------------------------------------------------------
# DB accessors (mirrors test_admin_providers_db.py style)
# ---------------------------------------------------------------------------


def _make_db(mock_conn):
    db = PostgresDB.__new__(PostgresDB)
    db._pool = MagicMock()
    db._connection_string = "test"
    db._queries = {}

    @asynccontextmanager
    async def _acquire():
        yield mock_conn

    db.acquire = _acquire
    return db


def _conn():
    return AsyncMock()


def _row(**overrides):
    base = {
        "id": UUID("11111111-1111-1111-1111-111111111111"),
        "provider_kind": "system",
        "provider_ref": "anthropic",
        "model_id": "claude-opus-4-7",
        "display_label": "Claude Opus 4.7",
        "capability": "chat",
        "capabilities": ["chat", "auxiliary"],
        "family": "claude-opus",
        "context_window": None,
        "reasoning_level": None,
        "params_json": None,
        "enabled": True,
        "seeded_from": None,
        "notes": None,
        "created_at": None,
        "updated_at": None,
    }
    base.update(overrides)
    return base


class TestCreateModelJsonbHandling:
    """Null vs explicit zero must be distinguished.

    Positional argument indices on the INSERT bind layout (post chunk 7):
    $1 provider_kind, $2 provider_ref, $3 model_id, $4 display_label,
    $5 capabilities, $6 family, $7 context_window, $8 reasoning_level,
    $9 params_json, $10 enabled, $11 seeded_from, $12 notes. ``args[0]``
    is the SQL string; positional binds start at ``args[1]``.
    """

    @pytest.mark.asyncio
    async def test_null_params_writes_sql_null(self):
        conn = _conn()
        conn.fetchrow = AsyncMock(return_value=_row())
        db = _make_db(conn)

        await db.create_model(
            provider_kind="system",
            provider_ref="anthropic",
            model_id="claude-opus-4-7",
            display_label="Claude Opus 4.7",
            capabilities=["chat", "auxiliary"],
            family="claude-opus",
            params_json=None,
        )
        params_arg = conn.fetchrow.await_args.args[9]
        assert params_arg is None

    @pytest.mark.asyncio
    async def test_explicit_zero_temperature_serialized_as_json(self):
        conn = _conn()
        conn.fetchrow = AsyncMock(return_value=_row())
        db = _make_db(conn)

        await db.create_model(
            provider_kind="system",
            provider_ref="anthropic",
            model_id="claude-opus-4-7",
            display_label="Claude Opus 4.7",
            capabilities=["chat", "auxiliary"],
            family="claude-opus",
            params_json={"temperature": 0},
        )
        params_arg = conn.fetchrow.await_args.args[9]
        assert params_arg is not None
        # round-trip via JSON to confirm the zero survived
        assert json.loads(params_arg) == {"temperature": 0}

    @pytest.mark.asyncio
    async def test_chat_capability_expands_to_chat_and_auxiliary(self):
        """Passing capability='chat' (legacy kwarg) lands capabilities=
        ['chat','auxiliary'] in the array column. Reflects the operator-intent
        invariant: chat-capable LLMs always work for the auxiliary observer/
        curator workload. The kwarg is kept on the accessor for legacy
        callers (init.py migration); the API layer enforces the array form."""
        conn = _conn()
        conn.fetchrow = AsyncMock(return_value=_row())
        db = _make_db(conn)

        await db.create_model(
            provider_kind="system",
            provider_ref="anthropic",
            model_id="claude-opus-4-7",
            display_label="Claude Opus 4.7",
            capability="chat",
            family="claude-opus",
        )
        # $5 = capabilities (only column now — capability dropped in chunk 7).
        capabilities_arg = conn.fetchrow.await_args.args[5]
        assert capabilities_arg == ["chat", "auxiliary"]

    @pytest.mark.asyncio
    async def test_explicit_capabilities_array_passed_through(self):
        """When the caller provides capabilities= directly the canonicalizer
        respects the explicit list — it does NOT auto-expand chat-only into
        chat+auxiliary. Operators get exact control over the row's roles."""
        conn = _conn()
        conn.fetchrow = AsyncMock(return_value=_row())
        db = _make_db(conn)

        await db.create_model(
            provider_kind="system",
            provider_ref="anthropic",
            model_id="claude-opus-4-7",
            display_label="Claude Opus 4.7",
            capabilities=["chat"],
            family="claude-opus",
        )
        capabilities_arg = conn.fetchrow.await_args.args[5]
        assert capabilities_arg == ["chat"]

    @pytest.mark.asyncio
    async def test_singleton_non_chat_capability_stays_singleton(self):
        """capability='embedding' lands as ['embedding'] — only the chat
        spelling auto-expands."""
        conn = _conn()
        conn.fetchrow = AsyncMock(return_value=_row())
        db = _make_db(conn)

        await db.create_model(
            provider_kind="system",
            provider_ref="openai",
            model_id="text-embedding-3-large",
            display_label="OpenAI Embedding",
            capability="embedding",
            family="openai-embedding",
        )
        capabilities_arg = conn.fetchrow.await_args.args[5]
        assert capabilities_arg == ["embedding"]

    @pytest.mark.asyncio
    async def test_unknown_capability_rejected(self):
        """Element-level enum guard at the accessor — defense in depth in
        case the Pydantic Literal is bypassed (e.g. internal callers)."""
        db = _make_db(_conn())
        with pytest.raises(ValueError, match="unknown capability"):
            await db.create_model(
                provider_kind="system",
                provider_ref="anthropic",
                model_id="claude-opus-4-7",
                display_label="Claude Opus 4.7",
                capabilities=["banana"],
                family="claude-opus",
            )

    @pytest.mark.asyncio
    async def test_neither_capability_nor_capabilities_rejected(self):
        """At least one of the two spellings must be set."""
        db = _make_db(_conn())
        with pytest.raises(ValueError):
            await db.create_model(
                provider_kind="system",
                provider_ref="anthropic",
                model_id="claude-opus-4-7",
                display_label="Claude Opus 4.7",
                family="claude-opus",
            )

    @pytest.mark.asyncio
    async def test_row_to_model_decodes_jsonb_string(self):
        # asyncpg returns JSONB as a string when the codec isn't registered;
        # _row_to_model must json.loads it transparently.
        row = _row(params_json='{"temperature": 0}')
        result = PostgresDB._row_to_model(row)
        assert result["params_json"] == {"temperature": 0}

    @pytest.mark.asyncio
    async def test_row_to_model_passes_through_dict(self):
        row = _row(params_json={"top_p": 0.9})
        result = PostgresDB._row_to_model(row)
        assert result["params_json"] == {"top_p": 0.9}


@pytest.fixture
def model_catalog_service():
    from orchestrator.services.family_matcher import detect_family
    from shared.runtime.core import loader

    store = MagicMock()
    # Provenance lookups are awaited on every list/create/update response.
    store.get_system_setting = AsyncMock(return_value=None)
    store.list_system_llm_endpoints = AsyncMock(return_value=[])
    return ModelCatalogService(
        store=store,
        probe=AsyncMock(),
        get_config_dir=lambda: loader.get_project_root() / "config",
        load_settings_matrix=lambda path: {},
        settings_for_family=loader.bundled_settings_for_family,
        family_detector=detect_family,
        reasoning_capability=loader.reasoning_capability,
    )


class TestSerializeCatalogModelContextWindow:
    """`_serialize_catalog_model` surfaces the resolved context window + source
    for the Admin → Models "Context" column: the explicit per-model cap when
    set, else the family default from the model config matrix."""

    def test_explicit_context_window_is_reported_as_explicit(
        self, model_catalog_service
    ):
        out = model_catalog_service._serialize_catalog_model(
            _row(context_window=262144)
        )
        assert out["context_window"] == 262144
        assert out["resolved_context_window"] == 262144
        assert out["context_window_source"] == "explicit"

    def test_unset_falls_back_to_family_default(self, model_catalog_service):
        # claude-opus family default is 1,000,000 (config/model_config_matrix.yaml).
        out = model_catalog_service._serialize_catalog_model(
            _row(context_window=None, family="claude-opus")
        )
        assert out["context_window"] is None
        assert out["resolved_context_window"] == 1_000_000
        assert out["context_window_source"] == "family_default"

    def test_unknown_family_falls_back_to_default_128k(self, model_catalog_service):
        # Unknown family → the `default` block's model_max_context_tokens (128000).
        out = model_catalog_service._serialize_catalog_model(
            _row(context_window=None, family="totally-unknown-family")
        )
        assert out["resolved_context_window"] == 128000
        assert out["context_window_source"] == "family_default"


class TestListModelsFilters:
    @pytest.mark.asyncio
    async def test_no_filters_emits_bare_select(self):
        conn = _conn()
        conn.fetch = AsyncMock(return_value=[])
        db = _make_db(conn)

        await db.list_models()
        sql = conn.fetch.await_args.args[0]
        assert "WHERE" not in sql

    @pytest.mark.asyncio
    async def test_capabilities_singleton_filter(self):
        """Single-element array filter for asking 'rows tagged for X'."""
        conn = _conn()
        conn.fetch = AsyncMock(return_value=[])
        db = _make_db(conn)

        await db.list_models(capabilities=["auxiliary"])
        sql = conn.fetch.await_args.args[0]
        args = conn.fetch.await_args.args[1:]
        assert "capabilities && $1::TEXT[]" in sql
        assert args == (["auxiliary"],)

    @pytest.mark.asyncio
    async def test_capabilities_array_filter_uses_overlap(self):
        """Multi-capability filter narrows by overlap — a row matches if
        any of the requested capabilities is present in the array. Used
        when the cockpit asks 'show me everything tagged for chat OR
        auxiliary'."""
        conn = _conn()
        conn.fetch = AsyncMock(return_value=[])
        db = _make_db(conn)

        await db.list_models(capabilities=["chat", "vision"])
        sql = conn.fetch.await_args.args[0]
        args = conn.fetch.await_args.args[1:]
        assert "capabilities && $1::TEXT[]" in sql
        assert args == (["chat", "vision"],)

    @pytest.mark.asyncio
    async def test_enabled_only_adds_clause(self):
        conn = _conn()
        conn.fetch = AsyncMock(return_value=[])
        db = _make_db(conn)

        await db.list_models(enabled_only=True)
        sql = conn.fetch.await_args.args[0]
        assert "enabled = TRUE" in sql


class TestReadinessAccessors:
    """The capability-completeness gate (chunk 5) reads two helpers
    on PostgresDB. These tests pin the SQL shape and the empty-case
    behavior so a future query rewrite doesn't silently break the gate."""

    @pytest.mark.asyncio
    async def test_count_returns_zero_for_unseen_capabilities(self):
        """A capability with no rows must report 0 (not be dropped) so
        callers can ask 'is `embedding` ready?' without a presence check."""
        conn = _conn()
        conn.fetch = AsyncMock(return_value=[{"cap": "chat", "n": 3}])
        db = _make_db(conn)

        counts = await db.count_enabled_models_by_capability()
        assert counts["chat"] == 3
        assert counts["embedding"] == 0
        assert counts["auxiliary"] == 0
        # Whisper/tts joined the enum in v1.1; they must also report 0.
        assert counts["whisper"] == 0
        assert counts["tts"] == 0

    @pytest.mark.asyncio
    async def test_count_fan_out_for_multi_capability_rows(self):
        """The unnest-based count must increment EVERY capability in a
        row's capabilities[] array. A chat row seeded as ['chat','auxiliary']
        contributes +1 to both buckets — exactly the user-reported fix:
        pinning a chat row as the auxiliary default no longer leaves the
        readiness gate red on a missing auxiliary count."""
        conn = _conn()
        conn.fetch = AsyncMock(
            return_value=[
                {"cap": "chat", "n": 2},
                {"cap": "auxiliary", "n": 2},
                {"cap": "vision", "n": 1},
                {"cap": "embedding", "n": 1},
            ]
        )
        db = _make_db(conn)

        counts = await db.count_enabled_models_by_capability()
        assert counts == {
            "chat": 2,
            "auxiliary": 2,
            "vision": 1,
            "embedding": 1,
            "whisper": 0,
            "tts": 0,
            "search": 0,
            "fetch": 0,
            "rerank": 0,
        }

    @pytest.mark.asyncio
    async def test_count_filters_to_enabled_rows(self):
        conn = _conn()
        conn.fetch = AsyncMock(return_value=[])
        db = _make_db(conn)

        await db.count_enabled_models_by_capability()
        sql = conn.fetch.await_args.args[0]
        assert "enabled = TRUE" in sql
        # unnest-driven fan-out: GROUP BY the unnested element, not the
        # singular column.
        assert "unnest(capabilities)" in sql
        assert "GROUP BY cap" in sql

    @pytest.mark.asyncio
    async def test_pinned_capabilities_strips_prefix_and_suffix(self):
        """The setting key pattern is ``llm.default_<cap>_model``; the
        accessor must extract ``<cap>`` for each non-empty value."""
        conn = _conn()
        conn.fetch = AsyncMock(
            return_value=[
                {
                    "key": "llm.default_chat_model",
                    "value": {"model": "claude-opus-4-7"},
                },
                {
                    "key": "llm.default_embedding_model",
                    "value": {"model": "text-embedding-3-large"},
                },
                # Empty model — must NOT be reported as pinned.
                {"key": "llm.default_auxiliary_model", "value": {"model": ""}},
            ]
        )
        db = _make_db(conn)

        pinned = await db.list_default_pin_capabilities()
        assert sorted(pinned) == ["chat", "embedding"]

    @pytest.mark.asyncio
    async def test_pinned_capabilities_skips_unrelated_setting_keys(self):
        """The LIKE filters guard against picking up unrelated
        `llm.*` system_settings rows that happen to match one half of
        the pattern."""
        conn = _conn()
        conn.fetch = AsyncMock(return_value=[])
        db = _make_db(conn)

        await db.list_default_pin_capabilities()
        sql = conn.fetch.await_args.args[0]
        args = conn.fetch.await_args.args[1:]
        assert "system_settings" in sql
        # Both LIKE bounds present so 'llm.default_foo_extra' doesn't slip in.
        assert args[0] == "llm.default_%"
        assert args[1] == "%_model"


class TestResolveCatalogModelTransportJoin:
    """resolve_catalog_model JOINs to system_api_keys or llm_endpoints
    depending on provider_kind, decrypts the api_key inline, and prefers the
    system row when both are present (ORDER BY provider_kind='system' DESC).
    """

    @pytest.mark.asyncio
    async def test_system_row_returns_decrypted_system_key(self):
        from orchestrator.security.crypto import encrypt

        conn = _conn()
        conn.fetchrow = AsyncMock(
            return_value=_row(
                system_api_key=encrypt("sk-ant-real"),
                endpoint_id=None,
                endpoint_label=None,
                endpoint_base_url=None,
                endpoint_api_key=None,
                catalog_id=UUID("11111111-1111-1111-1111-111111111111"),
            )
        )
        db = _make_db(conn)
        result = await db.resolve_catalog_model("claude-opus-4-7")
        assert result is not None
        assert result["api_key"] == "sk-ant-real"
        assert "system_api_key" not in result
        assert "endpoint_api_key" not in result

    @pytest.mark.asyncio
    async def test_endpoint_row_returns_decrypted_endpoint_key(self):
        from orchestrator.security.crypto import encrypt

        conn = _conn()
        conn.fetchrow = AsyncMock(
            return_value=_row(
                provider_kind="endpoint",
                provider_ref="11111111-1111-1111-1111-111111111111",
                system_api_key=None,
                endpoint_id=UUID("11111111-1111-1111-1111-111111111111"),
                endpoint_label="vLLM",
                endpoint_base_url="http://vllm.svc/v1",
                endpoint_api_key=encrypt("ep-secret"),
                catalog_id=UUID("22222222-2222-2222-2222-222222222222"),
            )
        )
        db = _make_db(conn)
        result = await db.resolve_catalog_model("RedHatAI/gemma")
        assert result is not None
        assert result["api_key"] == "ep-secret"
        assert result["endpoint_base_url"] == "http://vllm.svc/v1"

    @pytest.mark.asyncio
    async def test_no_match_returns_none(self):
        conn = _conn()
        conn.fetchrow = AsyncMock(return_value=None)
        db = _make_db(conn)
        assert await db.resolve_catalog_model("nope") is None

    @pytest.mark.asyncio
    async def test_query_orders_system_first(self):
        conn = _conn()
        conn.fetchrow = AsyncMock(return_value=None)
        db = _make_db(conn)
        await db.resolve_catalog_model("x")
        sql = conn.fetchrow.await_args.args[0]
        assert "ORDER BY (m.provider_kind = 'system') DESC" in sql


class TestListByCapabilityAlphabetical:
    @pytest.mark.asyncio
    async def test_filters_enabled_and_orders_by_label(self):
        conn = _conn()
        conn.fetch = AsyncMock(return_value=[])
        db = _make_db(conn)
        await db.list_models_by_capability_alphabetical("auxiliary")
        sql = conn.fetch.await_args.args[0]
        # Array membership replaces the literal capability= filter.
        assert "$1 = ANY(capabilities)" in sql
        assert "enabled = TRUE" in sql
        assert "ORDER BY display_label ASC" in sql
        assert conn.fetch.await_args.args[1:] == ("auxiliary",)


class TestPinDefaultIfUnset:
    """The auto-pin's conditional write: one statement, so a pin set between
    the auto-pin's read and this write is never overwritten."""

    @pytest.mark.asyncio
    async def test_writes_only_where_no_model_is_pinned(self):
        conn = _conn()
        conn.fetchrow = AsyncMock(return_value={"key": "llm.default_rerank_model"})
        db = _make_db(conn)

        pinned = await db.pin_default_llm_model_if_unset(
            "rerank", "qwen3-reranker-8b", updated_by="auto:x", source="default"
        )

        assert pinned is True
        sql = " ".join(conn.fetchrow.await_args.args[0].split())
        assert "ON CONFLICT (key) DO UPDATE" in sql
        # The update only fires over a row that names no model; a real pin
        # (object or legacy bare-string form) makes it a no-op.
        assert "WHERE COALESCE( NULLIF(system_settings.value->>'model', '')" in sql
        assert "jsonb_typeof(system_settings.value) = 'string'" in sql
        assert conn.fetchrow.await_args.args[1:] == (
            "llm.default_rerank_model",
            json.dumps({"model": "qwen3-reranker-8b"}),
            "auto:x",
            "default",
        )

    @pytest.mark.asyncio
    async def test_reports_false_when_a_pin_already_exists(self):
        conn = _conn()
        conn.fetchrow = AsyncMock(return_value=None)
        db = _make_db(conn)
        assert (
            await db.pin_default_llm_model_if_unset(
                "chat", "m", updated_by="auto:x", source="default"
            )
            is False
        )


class TestCatalogWritesAutoPin:
    """Admin → Models writes run the readiness auto-pin, so the first model
    added for a required capability becomes its default."""

    @staticmethod
    def _arm(store, *, pins=(), fail=False):
        store.get_system_api_key = AsyncMock(return_value="sk-test")
        store.create_model = AsyncMock(return_value=_row())
        store.update_model = AsyncMock(return_value=_row())
        if fail:
            store.list_default_pin_capabilities = AsyncMock(
                side_effect=RuntimeError("db down")
            )
        else:
            store.list_default_pin_capabilities = AsyncMock(return_value=list(pins))
        store.list_models_by_capability_alphabetical = AsyncMock(
            side_effect=lambda cap: (
                [{"model_id": "claude-opus-4-7"}]
                if cap in ("chat", "auxiliary")
                else []
            )
        )
        store.pin_default_llm_model_if_unset = AsyncMock(return_value=True)

    @staticmethod
    def _pinned_kinds(store):
        return [c.args[0] for c in store.pin_default_llm_model_if_unset.await_args_list]

    @pytest.mark.asyncio
    async def test_create_pins_the_first_model_for_its_capabilities(
        self, model_catalog_service
    ):
        store = model_catalog_service.store
        self._arm(store)
        await model_catalog_service.create_catalog_model(
            CatalogModelCreate(
                provider_kind="system",
                provider_ref="anthropic",
                model_id="claude-opus-4-7",
                display_label="Claude Opus 4.7",
                capabilities=["chat", "auxiliary"],
                family="claude-opus",
            )
        )
        assert self._pinned_kinds(store) == ["chat", "auxiliary"]

    @pytest.mark.asyncio
    async def test_update_runs_it_too(self, model_catalog_service):
        # Enabling a row can give a required capability its first model.
        store = model_catalog_service.store
        self._arm(store, pins=["chat"])
        await model_catalog_service.update_catalog_model(
            "11111111-1111-1111-1111-111111111111",
            CatalogModelUpdate(enabled=True),
        )
        assert self._pinned_kinds(store) == ["auxiliary"]

    @pytest.mark.asyncio
    async def test_a_failed_auto_pin_does_not_fail_the_write(
        self, model_catalog_service
    ):
        store = model_catalog_service.store
        self._arm(store, fail=True)
        out = await model_catalog_service.create_catalog_model(
            CatalogModelCreate(
                provider_kind="system",
                provider_ref="anthropic",
                model_id="claude-opus-4-7",
                display_label="Claude Opus 4.7",
                capabilities=["chat"],
                family="claude-opus",
            )
        )
        assert out["model_id"] == "claude-opus-4-7"
        store.pin_default_llm_model_if_unset.assert_not_awaited()


class TestNormalizeCatalogModelId:
    """Write-time normalization of the ``openrouter/`` routing prefix.

    A system-anchored OpenRouter catalog row must store its model_id WITH the
    ``openrouter/`` prefix so the agent routes through _create_openrouter_llm
    (which strips the prefix back to the gateway slug). Without it the row
    routes to the OpenAI factory default (api.openai.com) and rejects the
    sk-or-v1 key. Mirrors discovery.py's auto-prepend + the seed convention.
    """

    def test_prepends_prefix_for_system_openrouter_row(self):
        assert (
            _normalize_catalog_model_id("system", "openrouter", "minimax/minimax-m3")
            == "openrouter/minimax/minimax-m3"
        )

    def test_idempotent_when_prefix_already_present(self):
        assert (
            _normalize_catalog_model_id(
                "system", "openrouter", "openrouter/minimax/minimax-m3"
            )
            == "openrouter/minimax/minimax-m3"
        )

    def test_idempotent_is_case_insensitive(self):
        assert (
            _normalize_catalog_model_id(
                "system", "openrouter", "OpenRouter/minimax/minimax-m3"
            )
            == "OpenRouter/minimax/minimax-m3"
        )

    def test_other_system_providers_untouched(self):
        # openai/anthropic/etc. resolve correctly without a routing prefix.
        assert _normalize_catalog_model_id("system", "openai", "gpt-5.5") == "gpt-5.5"
        assert (
            _normalize_catalog_model_id("system", "anthropic", "claude-opus-4-7")
            == "claude-opus-4-7"
        )

    def test_endpoint_rows_untouched(self):
        # Endpoint rows route via the endpoint's inline base_url; prefixing
        # would be sent verbatim to the gateway and 404.
        assert (
            _normalize_catalog_model_id(
                "endpoint", "some-endpoint-uuid", "minimax/minimax-m3"
            )
            == "minimax/minimax-m3"
        )
