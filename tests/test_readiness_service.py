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
