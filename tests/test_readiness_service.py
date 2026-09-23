"""Tests for ``orchestrator.services.readiness.compute_readiness``.

Pins the readiness signal that powers:
- ``GET /api/system/readiness`` (cockpit onboarding gate).
- The 503 hard-fail in ``POST /api/jobs`` and ``POST /api/persistent/threads``
  when the LLM stack isn't ready.

The tests use a small ``_FakeDb`` instead of mocking PostgresDB — the
readiness service only needs five accessors and the fake makes the test
intent obvious without aspaths through the encrypted-secrets layer.
"""

from __future__ import annotations

import os
from typing import Any

import pytest

os.environ.setdefault("VECTOR_DB_URL", "postgresql://test@localhost/test")

from orchestrator.services import readiness  # noqa: E402


def test_readiness_route_registered() -> None:
    """The cockpit onboarding gate calls /api/system/readiness on first
    paint. If the route disappears, the UI degrades silently."""
    # Local import — defers main.app construction until pytest has set up
    # the env var sys.path tweaks above.
    from orchestrator.main import app

    paths: set[tuple[str, str]] = set()
    for route in app.routes:
        methods = getattr(route, "methods", None) or set()
        path = getattr(route, "path", "")
        for m in methods:
            paths.add((m, path))
    assert ("GET", "/api/system/readiness") in paths


class _FakeDb:
    """Minimal stand-in for PostgresDB exposing the five accessors
    ``compute_readiness`` calls. Defaults represent the empty DB state
    (no providers, no models, no pins, no settings)."""

    def __init__(
        self,
        *,
        api_keys: list[dict[str, Any]] | None = None,
        endpoints: list[dict[str, Any]] | None = None,
        capability_counts: dict[str, int] | None = None,
        pinned_capabilities: list[str] | None = None,
        fallback_setting: dict[str, Any] | None = None,
        expert_defaults: list[dict[str, Any]] | None = None,
        endpoint_models: list[dict[str, Any]] | None = None,
    ) -> None:
        self._api_keys = api_keys or []
        self._endpoints = endpoints or []
        self._counts = capability_counts or {}
        self._pinned = pinned_capabilities or []
        self._fallback_setting = fallback_setting
        self._endpoint_models = endpoint_models or []
        self._expert_defaults = (
            expert_defaults
            if expert_defaults is not None
            else [{"expert_type": "worker"}, {"expert_type": "session"}]
        )

    async def list_system_api_keys(self) -> list[dict[str, Any]]:
        return list(self._api_keys)

    async def list_system_llm_endpoints(self) -> list[dict[str, Any]]:
        return list(self._endpoints)

    async def list_models(self, *, provider_kind: str) -> list[dict[str, Any]]:
        return [m for m in self._endpoint_models if m["provider_kind"] == provider_kind]

    async def count_enabled_models_by_capability(self) -> dict[str, int]:
        return dict(self._counts)

    async def list_default_pin_capabilities(self) -> list[str]:
        return list(self._pinned)

    async def get_system_setting(self, key: str) -> dict[str, Any] | None:
        if key == "llm.fallback_optional_capabilities_to_chat":
            return self._fallback_setting
        return None

    async def list_application_expert_defaults(self) -> list[dict[str, Any]]:
        return list(self._expert_defaults)


# ---------------------------------------------------------------------------
# compute_readiness
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_empty_state_is_not_ready() -> None:
    """No providers, no rows, no pins → not ready, every required cap missing."""
    result = await readiness.compute_readiness(_FakeDb())
    assert result["ready"] is False
    assert result["missing_providers"] == ["any"]
    assert set(result["missing_capabilities"]) == {
        "chat",
        "embedding",
        "auxiliary",
        "rerank",
    }
    # No pins required when there are no rows to pin against — nothing in
    # missing_defaults.
    assert result["missing_defaults"] == []


@pytest.mark.asyncio
async def test_provider_only_still_misses_models() -> None:
    """A configured provider with no catalog rows is the v1 failure mode."""
    db = _FakeDb(api_keys=[{"provider": "anthropic"}])
    result = await readiness.compute_readiness(db)
    assert result["ready"] is False
    assert result["missing_providers"] == []
    assert set(result["missing_capabilities"]) == {
        "chat",
        "embedding",
        "auxiliary",
        "rerank",
    }


@pytest.mark.asyncio
async def test_models_present_but_no_default_pins() -> None:
    """Catalog rows for every required cap, but no admin-pinned defaults
    → ready=False, missing_defaults lists the required caps."""
    db = _FakeDb(
        api_keys=[{"provider": "openai"}],
        capability_counts={"chat": 2, "embedding": 1, "auxiliary": 1, "rerank": 1},
        pinned_capabilities=[],
    )
    result = await readiness.compute_readiness(db)
    assert result["ready"] is False
    assert result["missing_capabilities"] == []
    assert set(result["missing_defaults"]) == {
        "chat",
        "embedding",
        "auxiliary",
        "rerank",
    }


@pytest.mark.asyncio
async def test_partially_pinned_defaults() -> None:
    """Pinning only some of the required capabilities keeps the gate red."""
    db = _FakeDb(
        api_keys=[{"provider": "openai"}],
        capability_counts={"chat": 1, "embedding": 1, "auxiliary": 1, "rerank": 1},
        pinned_capabilities=["chat", "embedding", "rerank"],
    )
    result = await readiness.compute_readiness(db)
    assert result["ready"] is False
    assert result["missing_defaults"] == ["auxiliary"]


@pytest.mark.asyncio
async def test_fully_ready_state() -> None:
    """All four required caps have rows and pinned defaults → ready."""
    db = _FakeDb(
        api_keys=[{"provider": "openai"}],
        capability_counts={"chat": 2, "embedding": 1, "auxiliary": 1, "rerank": 1},
        pinned_capabilities=["chat", "embedding", "auxiliary", "rerank"],
    )
    result = await readiness.compute_readiness(db)
    assert result["ready"] is True
    assert result["missing_providers"] == []
    assert result["missing_capabilities"] == []
    assert result["missing_defaults"] == []
    assert result["missing_expert_defaults"] == []


@pytest.mark.asyncio
async def test_missing_application_expert_pointer_blocks_readiness() -> None:
    db = _FakeDb(
        api_keys=[{"provider": "openai"}],
        capability_counts={"chat": 1, "embedding": 1, "auxiliary": 1, "rerank": 1},
        pinned_capabilities=["chat", "embedding", "auxiliary", "rerank"],
        expert_defaults=[{"expert_type": "worker"}],
    )
    result = await readiness.compute_readiness(db)
    assert result["ready"] is False
    assert result["missing_expert_defaults"] == ["session"]


@pytest.mark.asyncio
async def test_chat_row_with_auxiliary_in_array_satisfies_auxiliary_requirement() -> (
    None
):
    """One physical chat row registered as ['chat','auxiliary'] contributes
    to BOTH count buckets via the unnest-driven fan-out in
    PostgresDB.count_enabled_models_by_capability. The readiness gate sees
    auxiliary count > 0 and stops flagging it as missing — the exact
    user-reported bug from the model_capabilities_array work.
    """
    db = _FakeDb(
        api_keys=[{"provider": "openai"}],
        # Counts are what the fan-out would emit for ONE chat row with
        # capabilities=['chat','auxiliary'] plus one embedding row.
        capability_counts={"chat": 1, "auxiliary": 1, "embedding": 1, "rerank": 1},
        # User pinned the same physical chat row for both slots.
        pinned_capabilities=["chat", "auxiliary", "embedding", "rerank"],
    )
    result = await readiness.compute_readiness(db)
    assert result["ready"] is True
    assert result["missing_capabilities"] == []
    assert result["missing_defaults"] == []


@pytest.mark.asyncio
async def test_endpoint_alone_satisfies_provider_check() -> None:
    """A system endpoint with no API key still counts as a configured provider."""
    db = _FakeDb(
        endpoints=[{"id": "ep-1", "label": "vllm"}],
        capability_counts={"chat": 1, "embedding": 1, "auxiliary": 1, "rerank": 1},
        pinned_capabilities=["chat", "embedding", "auxiliary", "rerank"],
    )
    result = await readiness.compute_readiness(db)
    assert result["ready"] is True
    assert result["missing_providers"] == []


@pytest.mark.asyncio
async def test_optional_vision_falls_back_to_chat_by_default() -> None:
    """With the default flag set, missing vision → ``use_chat`` fallback;
    audio caps disable (``None``)."""
    db = _FakeDb(
        api_keys=[{"provider": "openai"}],
        capability_counts={"chat": 1, "embedding": 1, "auxiliary": 1, "rerank": 1},
        pinned_capabilities=["chat", "embedding", "auxiliary", "rerank"],
    )
    result = await readiness.compute_readiness(db)
    fallbacks = result["optional_capability_fallbacks"]
    assert fallbacks == {"vision": "use_chat", "whisper": None, "tts": None}


@pytest.mark.asyncio
async def test_optional_capability_present_no_fallback_needed() -> None:
    """Capability with rows reports None — natively available, no fallback."""
    db = _FakeDb(
        api_keys=[{"provider": "openai"}],
        capability_counts={
            "chat": 1,
            "embedding": 1,
            "auxiliary": 1,
            "rerank": 1,
            "vision": 1,
            "whisper": 1,
        },
        pinned_capabilities=["chat", "embedding", "auxiliary", "rerank"],
    )
    result = await readiness.compute_readiness(db)
    assert result["optional_capability_fallbacks"]["vision"] is None
    assert result["optional_capability_fallbacks"]["whisper"] is None


@pytest.mark.asyncio
async def test_fallback_flag_false_disables_vision_chat_bridge() -> None:
    """When the operator opts into strict separation, missing vision no
    longer reports use_chat — it just disables."""
    db = _FakeDb(
        api_keys=[{"provider": "openai"}],
        capability_counts={"chat": 1, "embedding": 1, "auxiliary": 1, "rerank": 1},
        pinned_capabilities=["chat", "embedding", "auxiliary", "rerank"],
        fallback_setting={"value": {"enabled": False}},
    )
    result = await readiness.compute_readiness(db)
    assert result["optional_capability_fallbacks"]["vision"] is None


# ---------------------------------------------------------------------------
# gate_error_detail
# ---------------------------------------------------------------------------


def test_gate_error_detail_carries_missing_lists() -> None:
    """Error body must surface the same `missing_*` fields the cockpit
    reads from /api/system/readiness so deep links work from either source."""
    payload = {
        "ready": False,
        "missing_providers": [],
        "missing_capabilities": ["embedding"],
        "missing_defaults": ["chat"],
        "missing_expert_defaults": ["session"],
        "optional_capability_fallbacks": {},
    }
    detail = readiness.gate_error_detail(payload)
    assert detail["error"] == "system_not_ready"
    assert detail["missing_capabilities"] == ["embedding"]
    assert detail["missing_defaults"] == ["chat"]
    assert detail["missing_expert_defaults"] == ["session"]
    assert "embedding" in detail["message"]
    assert "chat" in detail["message"]
    assert "session" in detail["message"]


def test_gate_error_detail_message_for_no_providers() -> None:
    payload = {
        "ready": False,
        "missing_providers": ["any"],
        "missing_capabilities": ["chat", "embedding", "auxiliary"],
        "missing_defaults": [],
    }
    detail = readiness.gate_error_detail(payload)
    assert "Configure at least one provider" in detail["message"]


@pytest.mark.asyncio
@pytest.mark.parametrize("capabilities", [["search"], ["search", "fetch"], ["fetch"]])
async def test_research_endpoint_does_not_complete_model_provider_setup(capabilities):
    db = _FakeDb(
        endpoints=[{"id": "research-endpoint", "label": "Renamed search service"}],
        endpoint_models=[
            {
                "provider_kind": "endpoint",
                "provider_ref": "research-endpoint",
                "capabilities": capabilities,
                "enabled": True,
            }
        ],
    )
    result = await readiness.compute_readiness(db)
    assert result["missing_providers"] == ["any"]
    assert result["ready"] is False


@pytest.mark.asyncio
async def test_mixed_research_and_chat_endpoint_counts_as_model_provider():
    db = _FakeDb(
        endpoints=[{"id": "mixed-endpoint"}],
        endpoint_models=[
            {
                "provider_kind": "endpoint",
                "provider_ref": "mixed-endpoint",
                "capabilities": ["search"],
            },
            {
                "provider_kind": "endpoint",
                "provider_ref": "mixed-endpoint",
                "capabilities": ["chat", "auxiliary"],
            },
        ],
    )
    result = await readiness.compute_readiness(db)
    assert result["missing_providers"] == []


# ---------------------------------------------------------------------------
# auto_pin_required_defaults
# ---------------------------------------------------------------------------


class _AutoPinDb:
    """The three accessors the auto-pin calls. ``rows`` maps a capability to
    its enabled model ids in resolver order (first by display label);
    ``lose_race`` names kinds whose conditional write finds a pin already."""

    def __init__(
        self,
        *,
        rows: dict[str, list[str]],
        pins: dict[str, str] | None = None,
        lose_race: set[str] | None = None,
    ) -> None:
        self._rows = rows
        self.pins = dict(pins or {})
        self._lose_race = lose_race or set()
        self.listed: list[str] = []
        self.writes: list[tuple[str, str, str, str]] = []

    async def list_default_pin_capabilities(self) -> list[str]:
        return [kind for kind, model in self.pins.items() if model]

    async def list_models_by_capability_alphabetical(
        self, capability: str
    ) -> list[dict[str, Any]]:
        self.listed.append(capability)
        return [{"model_id": m} for m in self._rows.get(capability, [])]

    async def pin_default_llm_model_if_unset(
        self, kind: str, model: str, *, updated_by: str, source: str
    ) -> bool:
        self.writes.append((kind, model, updated_by, source))
        if kind in self._lose_race:
            return False
        self.pins[kind] = model
        return True


@pytest.mark.asyncio
async def test_auto_pin_pins_each_unpinned_required_capability() -> None:
    """A fresh install that added its models is ready without a Defaults
    step: every required capability gets the row dispatch already falls
    back to. Optional capabilities are not pinned."""
    from shared.helm_provenance import AUTO_PIN_BREADCRUMB, SOURCE_DEFAULT

    db = _AutoPinDb(
        rows={
            "chat": ["a-chat", "b-chat"],
            "auxiliary": ["a-chat"],
            "embedding": ["emb"],
            "rerank": ["rr"],
            "vision": ["vis"],
        }
    )
    pinned = await readiness.auto_pin_required_defaults(db)

    assert pinned == [
        ("chat", "a-chat"),
        ("embedding", "emb"),
        ("auxiliary", "a-chat"),
        ("rerank", "rr"),
    ]
    assert "vision" not in db.pins
    assert {(w[2], w[3]) for w in db.writes} == {(AUTO_PIN_BREADCRUMB, SOURCE_DEFAULT)}


@pytest.mark.asyncio
async def test_auto_pin_skips_pinned_and_empty_capabilities() -> None:
    db = _AutoPinDb(
        rows={"chat": ["a-chat"], "auxiliary": ["a-chat"], "rerank": []},
        pins={"chat": "admin-pick"},
    )
    pinned = await readiness.auto_pin_required_defaults(db)

    assert pinned == [("auxiliary", "a-chat")]
    assert db.pins["chat"] == "admin-pick"
    assert "chat" not in db.listed


@pytest.mark.asyncio
async def test_auto_pin_is_one_read_once_everything_is_pinned() -> None:
    """Runs after every catalog write, so the steady state must stay cheap."""
    db = _AutoPinDb(
        rows={"chat": ["a-chat"]},
        pins={cap: "x" for cap in readiness.REQUIRED_CAPABILITIES},
    )
    assert await readiness.auto_pin_required_defaults(db) == []
    assert db.listed == []
    assert db.writes == []


@pytest.mark.asyncio
async def test_auto_pin_reports_only_writes_that_landed() -> None:
    """An admin pin landing between the read and the write wins."""
    db = _AutoPinDb(rows={"chat": ["a-chat"], "embedding": ["emb"]}, lose_race={"chat"})
    assert await readiness.auto_pin_required_defaults(db) == [("embedding", "emb")]


@pytest.mark.asyncio
async def test_auto_pinned_install_passes_the_gate() -> None:
    """End to end over the gate's own inputs: rows for all four required
    capabilities and no pins is not ready; after the auto-pin it is."""
    rows = {
        "chat": ["a-chat"],
        "auxiliary": ["a-chat"],
        "embedding": ["emb"],
        "rerank": ["rr"],
    }
    pin_db = _AutoPinDb(rows=rows)
    counts = {cap: len(models) for cap, models in rows.items()}

    before = await readiness.compute_readiness(
        _FakeDb(api_keys=[{"provider": "openai"}], capability_counts=counts)
    )
    await readiness.auto_pin_required_defaults(pin_db)
    after = await readiness.compute_readiness(
        _FakeDb(
            api_keys=[{"provider": "openai"}],
            capability_counts=counts,
            pinned_capabilities=list(pin_db.pins),
        )
    )

    assert before["missing_defaults"] == ["chat", "embedding", "auxiliary", "rerank"]
    assert after["ready"] is True


@pytest.mark.asyncio
async def test_try_auto_pin_never_raises(caplog: pytest.LogCaptureFixture) -> None:
    """Catalog writes call it inline; a failure must not fail the write."""

    class _Broken:
        async def list_default_pin_capabilities(self) -> list[str]:
            raise RuntimeError("db down")

    with caplog.at_level("WARNING", logger="orchestrator.services.readiness"):
        assert await readiness.try_auto_pin_required_defaults(_Broken()) == []
    assert "auto-pinning required default models failed" in caplog.text
