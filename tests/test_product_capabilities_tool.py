"""Focused tests for the M2c persistent-session capability tool."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock
from uuid import UUID

import httpx
import pytest

from orchestrator.services.product_capabilities import (
    ProductCapabilityService,
    ResolutionRequest,
)
from orchestrator.services.shared_browser_canvas import BrowserCapabilityResponse
from shared.runtime.core.product_capabilities import (
    AgentAction,
    ComponentProvenance,
    ProductComponent,
    ProvenanceStatus,
    SCHEMA_VERSION,
    SchemaCompatibility,
    SessionState,
    UserState,
)
from agent.tools.context import SessionRuntimeFacts, ToolContext
from shared.connectors.builtin import spec_for_type
from agent.tools.email.tools import create_email_tools
from agent.tools.product_capabilities import (
    CapabilityToolErrorCode,
    CapabilityToolRequest,
    CapabilityToolStatus,
    MAX_SERVER_RESPONSE_BYTES,
    MAX_TOOL_CAPABILITY_IDS,
    MAX_TOOL_OUTPUT_BYTES,
    PRODUCT_CAPABILITIES_TOOL_ENABLED_ENV,
    PRODUCT_CAPABILITIES_TOOL_NAME,
    ProductCapabilitiesToolOutput,
    create_product_capability_tools,
    product_capabilities_tool_enabled,
)
from agent.tools.registry import TOOL_REGISTRY, filter_tools_by_backend, load_tools

_USER_ID = "11111111-1111-1111-1111-111111111111"
_THREAD_ID = "22222222-2222-2222-2222-222222222222"
_PROJECT_ID = "33333333-3333-3333-3333-333333333333"
_NOW = datetime(2026, 7, 27, 14, 0, tzinfo=timezone.utc)
_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(autouse=True)
def _enable_capability_tool(monkeypatch):
    monkeypatch.setenv(PRODUCT_CAPABILITIES_TOOL_ENABLED_ENV, "true")


async def _grants_allowed(_db: Any, **_kwargs: Any) -> dict[str, Any]:
    return {
        "browser": True,
        "datasource_tools": True,
        "delegation": True,
        "permission_mode": "auto_accept",
    }


async def _server_payload(
    *capability_ids: str,
    topic: str | None = None,
    grants: dict[str, Any] | None = None,
) -> bytes:
    async def resolve_grants(_db: Any, **_kwargs: Any) -> dict[str, Any]:
        return grants or await _grants_allowed(_db)

    service = ProductCapabilityService(
        object(),
        grants_resolver=resolve_grants,
        browser_resolver=lambda _thread: BrowserCapabilityResponse(
            feature_enabled=True,
            can_open_browser=True,
            workspace_ready=True,
            reason=None,
        ),
        environment={
            "CANVAS_SHARED_BROWSER_ENABLED": "true",
            "PROTECTED_CLOUD_MODE_ENABLED": "true",
        },
        clock=lambda: _NOW,
    )
    request = ResolutionRequest(
        user_id=_USER_ID,
        is_admin=False,
        thread_id=UUID(_THREAD_ID),
        primary_project_id=UUID(_PROJECT_ID),
        topic=topic,
        capability_ids=capability_ids,
        limit=20,
    )
    response = await service.resolve(
        request,
        admitted_thread={
            "id": _THREAD_ID,
            "user_id": _USER_ID,
            "project_id": _PROJECT_ID,
            "metadata": {"datasource_ids": ["must-not-leak"]},
        },
    )
    return response.model_dump_json().encode()


def _facts(
    *,
    backend_id: str | None = "sandbox",
    supports_shell: bool = True,
    supports_files: bool = True,
    supports_canvas: bool = True,
    supports_live_apps: bool = True,
    supports_browser: bool = True,
    datasource_types: tuple[str, ...] = (),
    email_tier: str | None = None,
    email_failed: bool = False,
    email_direct_send: bool = False,
    knowledge_binding: bool = False,
    knowledge_store: bool = False,
    memory: bool = False,
    cloud: bool = False,
    protected: bool = False,
    tools: tuple[str, ...] = (),
    component_provenance: tuple[tuple[ProductComponent, ComponentProvenance], ...] = (),
) -> SessionRuntimeFacts:
    if "email" in datasource_types and email_tier is None:
        email_tier = "read"
    # What the session derives from the attached connectors' specs.
    specs = [spec_for_type(item) for item in datasource_types]
    return SessionRuntimeFacts(
        observed_at=_NOW,
        backend_id=backend_id,  # type: ignore[arg-type]
        backend_supports_shell=supports_shell,
        backend_supports_file_tools=supports_files,
        backend_supports_canvas_presentation=supports_canvas,
        backend_supports_canvas_live_apps=supports_live_apps,
        backend_supports_shared_browser=supports_browser,
        attached_datasource_types=datasource_types,
        attached_tool_categories=tuple(
            spec.tool_category for spec in specs if spec and spec.tool_category
        ),
        knowledge_index_attached=any(
            spec is not None and "knowledge_index" in spec.delivery_forms
            for spec in specs
        ),
        email_access_tier=email_tier,  # type: ignore[arg-type]
        email_connection_failed=email_failed,
        email_direct_send_enabled=email_direct_send,
        knowledge_binding_available=knowledge_binding,
        knowledge_store_available=knowledge_store,
        memory_available=memory,
        cloud_mount_active=cloud,
        protected_cloud_active=protected,
        loaded_tool_names=(
            PRODUCT_CAPABILITIES_TOOL_NAME,
            "read_product_guide",
            *tools,
        ),
        runtime_component_provenance=component_provenance,
    )


def _context(
    facts: SessionRuntimeFacts | None,
    *,
    config: dict[str, Any] | None = None,
) -> ToolContext:
    return ToolContext(
        _thread_id=_THREAD_ID,
        user_id=_USER_ID,
        config=config or {},
        session_runtime_facts=facts,
    )


async def _invoke(
    payload: bytes,
    *,
    facts: SessionRuntimeFacts | None,
    args: dict[str, Any] | None = None,
    context: ToolContext | None = None,
) -> tuple[str, ProductCapabilitiesToolOutput]:
    async def fetcher(
        _context: ToolContext,
        _request: CapabilityToolRequest,
    ) -> bytes:
        return payload

    tool = create_product_capability_tools(
        context or _context(facts),
        fetcher=fetcher,
    )[0]
    raw = await tool.ainvoke(args or {})
    return raw, ProductCapabilitiesToolOutput.model_validate_json(raw)


def _only(output: ProductCapabilitiesToolOutput):
    assert output.response is not None
    assert len(output.response.capabilities) == 1
    return output.response.capabilities[0]


def test_tool_is_registered_under_product_help_and_default_off(monkeypatch):
    metadata = TOOL_REGISTRY[PRODUCT_CAPABILITIES_TOOL_NAME]
    assert metadata["category"] == "product_help"
    assert metadata["phases"] == ["strategic", "tactical"]

    monkeypatch.delenv(PRODUCT_CAPABILITIES_TOOL_ENABLED_ENV, raising=False)
    context = _context(None)
    assert product_capabilities_tool_enabled() is False
    assert create_product_capability_tools(context) == []
    assert load_tools([PRODUCT_CAPABILITIES_TOOL_NAME], context) == []


def test_tool_gate_parsing_is_closed_and_operator_owned():
    assert product_capabilities_tool_enabled({}) is False
    assert product_capabilities_tool_enabled(
        {PRODUCT_CAPABILITIES_TOOL_ENABLED_ENV: "YES"}
    )
    assert not product_capabilities_tool_enabled(
        {PRODUCT_CAPABILITIES_TOOL_ENABLED_ENV: "unexpected"}
    )


def test_tool_survives_none_backend_filter_and_guide_break_glass(monkeypatch):
    monkeypatch.setenv("APP_GUIDE_BREAK_GLASS_DISABLED", "true")
    none_backend = type(
        "NoneBackend",
        (),
        {
            "supports_shell": False,
            "supports_file_tools": False,
            "supports_canvas_presentation": False,
        },
    )()
    assert filter_tools_by_backend(
        [PRODUCT_CAPABILITIES_TOOL_NAME],
        none_backend,
    ) == [PRODUCT_CAPABILITIES_TOOL_NAME]
    assert [
        item.name
        for item in load_tools([PRODUCT_CAPABILITIES_TOOL_NAME], _context(None))
    ] == [PRODUCT_CAPABILITIES_TOOL_NAME]


def test_model_schema_cannot_supply_identity_scope_or_resolver():
    tool = create_product_capability_tools(_context(None))[0]
    assert set(tool.args) == {"topic", "capability_ids"}
    for forbidden in (
        "user_id",
        "thread_id",
        "project_id",
        "repository",
        "component",
        "resolver",
        "source_revision",
    ):
        assert forbidden not in tool.args


@pytest.mark.asyncio
async def test_unknown_guide_topic_fails_closed_before_fetch():
    fetched = False

    async def fetcher(
        _context: ToolContext,
        _request: CapabilityToolRequest,
    ) -> bytes:
        nonlocal fetched
        fetched = True
        return b"{}"

    capability_tool = create_product_capability_tools(
        _context(_facts()),
        fetcher=fetcher,
    )[0]
    raw = await capability_tool.ainvoke({"topic": "datasources-email"})
    output = ProductCapabilitiesToolOutput.model_validate_json(raw)

    assert output.status is CapabilityToolStatus.UNAVAILABLE
    assert output.error_code is CapabilityToolErrorCode.INVALID_REQUEST
    assert fetched is False


@pytest.mark.asyncio
async def test_fetcher_receives_bound_context_and_canonical_exact_filters():
    payload = await _server_payload(
        "datasources.email",
        "datasources.email.send",
        topic="email",
    )
    context = _context(
        _facts(
            datasource_types=("email",),
            email_tier="draft",
            tools=("email_draft", "email_read"),
        )
    )
    observed: dict[str, Any] = {}

    async def fetcher(
        received_context: ToolContext,
        request: CapabilityToolRequest,
    ) -> bytes:
        observed["context"] = received_context
        observed["request"] = request
        return payload

    capability_tool = create_product_capability_tools(
        context,
        fetcher=fetcher,
    )[0]
    raw = await capability_tool.ainvoke(
        {
            "topic": "email",
            "capability_ids": [
                "datasources.email.send",
                "datasources.email",
                "datasources.email.send",
            ],
        }
    )
    output = ProductCapabilitiesToolOutput.model_validate_json(raw)

    assert output.status is CapabilityToolStatus.READY
    assert observed["context"] is context
    assert observed["request"].capability_ids == (
        "datasources.email",
        "datasources.email.send",
    )
    assert observed["request"].model_dump() == {
        "topic": "email",
        "capability_ids": (
            "datasources.email",
            "datasources.email.send",
        ),
    }


@pytest.mark.asyncio
async def test_default_fetch_binds_internal_identity_thread_and_timeout(
    monkeypatch,
):
    import agent.tools.product_capabilities as capability_module

    payload = await _server_payload("datasources.email")
    observed: dict[str, Any] = {}

    class FakeResponse:
        status_code = 200
        headers: dict[str, str] = {}

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def aiter_bytes(self):
            yield payload

    class FakeClient:
        def __init__(self, **kwargs):
            observed["client"] = kwargs

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        def stream(self, method, url, *, params):
            observed["request"] = {
                "method": method,
                "url": url,
                "params": params,
            }
            return FakeResponse()

    monkeypatch.setenv("MCP_INTERNAL_KEY", "internal-test-key")
    monkeypatch.setenv("ORCHESTRATOR_URL", "http://orchestrator.test:8085/")
    monkeypatch.setattr(capability_module.httpx, "AsyncClient", FakeClient)
    request = CapabilityToolRequest(
        topic="email",
        capability_ids=("datasources.email",),
    )

    result = await capability_module._fetch_server_payload(
        _context(_facts()),
        request,
    )

    assert result == payload
    assert observed["client"]["headers"] == {
        "Accept": "application/json",
        "X-Internal-Key": "internal-test-key",
        "X-MCP-User-Id": _USER_ID,
    }
    assert observed["client"]["timeout"].read == 5.0
    assert observed["client"]["follow_redirects"] is False
    assert observed["request"]["url"] == (
        "http://orchestrator.test:8085/api/users/me/product-capabilities"
    )
    assert observed["request"]["params"] == [
        ("thread_id", _THREAD_ID),
        ("limit", "20"),
        ("topic", "email"),
        ("capability_id", "datasources.email"),
    ]
    assert all(
        key not in {"user_id", "project_id", "repository", "resolver"}
        for key, _value in observed["request"]["params"]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("facts", "expected_state", "expected_action"),
    [
        (
            _facts(),
            SessionState.NEEDS_ATTACHMENT,
            AgentAction.CAN_GUIDE,
        ),
        (
            _facts(
                datasource_types=("email",),
                email_tier="read",
                email_failed=True,
            ),
            SessionState.DEGRADED,
            AgentAction.CAN_GUIDE,
        ),
        (
            _facts(
                datasource_types=("email",),
                email_tier="read",
                tools=("email_read",),
            ),
            SessionState.READY,
            AgentAction.CAN_EXECUTE,
        ),
    ],
)
async def test_email_attachment_connection_and_read_actionability(
    facts: SessionRuntimeFacts,
    expected_state: SessionState,
    expected_action: AgentAction,
):
    _raw, output = await _invoke(
        await _server_payload("datasources.email"),
        facts=facts,
        args={"capability_ids": ["datasources.email"]},
    )
    capability = _only(output)

    assert capability.session.state is expected_state
    assert capability.session.source_component is ProductComponent.AGENT
    assert capability.agent_action is expected_action
    if "email" in facts.attached_datasource_types:
        assert capability.session.qualifiers[0].key == "email.access_tier"
        assert capability.session.qualifiers[0].value == facts.email_access_tier
    else:
        assert capability.session.qualifiers == ()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("tier", "tools", "expected_action"),
    [
        ("read", ("email_read",), AgentAction.CAN_GUIDE),
        ("draft", ("email_draft",), AgentAction.CAN_PROPOSE),
        ("send", ("email_draft",), AgentAction.CAN_PROPOSE),
        ("send", ("email_send",), AgentAction.CAN_EXECUTE),
    ],
)
async def test_email_send_requires_exact_live_tier_and_tool(
    tier: str,
    tools: tuple[str, ...],
    expected_action: AgentAction,
):
    _raw, output = await _invoke(
        await _server_payload("datasources.email.send"),
        facts=_facts(
            datasource_types=("email",),
            email_tier=tier,
            email_direct_send=(expected_action is AgentAction.CAN_EXECUTE),
            tools=tools,
        ),
        args={"capability_ids": ["datasources.email.send"]},
    )
    capability = _only(output)

    assert capability.agent_action is expected_action
    assert capability.session.state is (
        SessionState.NOT_READY if tier == "read" else SessionState.READY
    )


@pytest.mark.asyncio
async def test_denied_server_grant_cannot_be_widened_by_loaded_email_tool():
    grants = await _grants_allowed(object())
    grants["datasource_tools"] = False
    _raw, output = await _invoke(
        await _server_payload("datasources.email", grants=grants),
        facts=_facts(
            datasource_types=("email",),
            email_tier="read",
            tools=("email_read",),
        ),
        args={"capability_ids": ["datasources.email"]},
    )
    capability = _only(output)

    assert capability.user.state is UserState.DENIED
    assert capability.session.state is SessionState.READY
    assert capability.agent_action is AgentAction.CAN_GUIDE


@pytest.mark.asyncio
async def test_send_tool_without_current_unattended_send_gate_can_only_guide():
    _raw, output = await _invoke(
        await _server_payload("datasources.email.send"),
        facts=_facts(
            datasource_types=("email",),
            email_tier="send",
            email_direct_send=False,
            tools=("email_send",),
        ),
        args={"capability_ids": ["datasources.email.send"]},
    )

    assert _only(output).session.state is SessionState.READY
    assert _only(output).agent_action is AgentAction.CAN_GUIDE


@pytest.mark.asyncio
async def test_ready_snapshot_cannot_authorize_send_after_live_detach():
    connection = SimpleNamespace(
        access="send",
        unattended_send=True,
        open_smtp=MagicMock(),
    )
    facts = _facts(
        datasource_types=("email",),
        email_tier="send",
        email_direct_send=True,
        tools=("email_send",),
    )
    context = ToolContext(
        _thread_id=_THREAD_ID,
        user_id=_USER_ID,
        config={},
        datasources={"email": connection},
        session_runtime_facts=facts,
    )
    _raw, output = await _invoke(
        await _server_payload("datasources.email.send"),
        facts=None,
        args={"capability_ids": ["datasources.email.send"]},
        context=context,
    )
    assert _only(output).agent_action is AgentAction.CAN_EXECUTE

    bound_send = next(
        tool for tool in create_email_tools(context) if tool.name == "email_send"
    )
    context.datasources.clear()
    result = bound_send.invoke(
        {
            "subject": "M2 check",
            "body": "Synthetic test",
            "to": ["person@example.test"],
        }
    )

    assert "Error" in result and "binding changed" in result
    connection.open_smtp.assert_not_called()


@pytest.mark.asyncio
async def test_okf_knowledge_and_protected_cloud_use_redacted_live_facts():
    okf_raw, okf = await _invoke(
        await _server_payload("datasources.okf"),
        facts=_facts(
            datasource_types=("kb",),
            knowledge_binding=True,
            knowledge_store=True,
            tools=("kb_search",),
        ),
        args={"capability_ids": ["datasources.okf"]},
    )
    cloud_raw, cloud = await _invoke(
        await _server_payload("sessions.protected-cloud"),
        facts=_facts(
            cloud=True,
            protected=True,
            tools=("write_file",),
        ),
        args={"capability_ids": ["sessions.protected-cloud"]},
    )

    assert _only(okf).session.state is SessionState.READY
    assert _only(okf).agent_action is AgentAction.CAN_EXECUTE
    assert _only(cloud).session.state is SessionState.READY
    assert _only(cloud).agent_action is AgentAction.CAN_EXECUTE
    for private_value in ("datasource_id", "project_id", "mount_path"):
        assert private_value not in okf_raw
        assert private_value not in cloud_raw


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("backend_id", "supports_shell"),
    [
        ("none", False),
        ("virtual", False),
        ("sandbox", True),
        ("vm", True),
    ],
)
async def test_workspace_result_uses_active_public_backend_facts(
    backend_id: str,
    supports_shell: bool,
):
    tools = ("request_workspace_upgrade",) if not supports_shell else ()
    _raw, output = await _invoke(
        await _server_payload("workspaces.select"),
        facts=_facts(
            backend_id=backend_id,
            supports_shell=supports_shell,
            supports_files=backend_id != "none",
            tools=tools,
        ),
        args={"capability_ids": ["workspaces.select"]},
    )
    capability = _only(output)
    qualifiers = {item.key: item.value for item in capability.session.qualifiers}

    assert capability.session.state is SessionState.READY
    assert qualifiers == {
        "workspace.backend": backend_id,
        "workspace.supports_shell": supports_shell,
    }
    assert capability.agent_action is (
        AgentAction.CAN_PROPOSE if tools else AgentAction.CAN_GUIDE
    )


@pytest.mark.asyncio
async def test_missing_runtime_facts_preserves_server_unknown_and_marks_partial():
    _raw, output = await _invoke(
        await _server_payload("datasources.email"),
        facts=None,
        args={"capability_ids": ["datasources.email"]},
    )
    capability = _only(output)

    assert output.status is CapabilityToolStatus.PARTIAL
    assert output.error_code is CapabilityToolErrorCode.RUNTIME_FACTS_UNAVAILABLE
    assert capability.session.state is SessionState.UNKNOWN
    assert capability.agent_action is AgentAction.UNKNOWN


@pytest.mark.asyncio
async def test_invalid_request_is_bounded_and_does_not_call_fetcher():
    called = False

    async def fetcher(_context, _request):
        nonlocal called
        called = True
        return b"{}"

    capability_tool = create_product_capability_tools(
        _context(_facts()),
        fetcher=fetcher,
    )[0]
    raw = await capability_tool.ainvoke(
        {
            "capability_ids": [
                f"feature.item-{index}" for index in range(MAX_TOOL_CAPABILITY_IDS + 1)
            ]
        }
    )
    output = ProductCapabilitiesToolOutput.model_validate_json(raw)

    assert output.status is CapabilityToolStatus.UNAVAILABLE
    assert output.error_code is CapabilityToolErrorCode.INVALID_REQUEST
    assert called is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("payload", "expected_error"),
    [
        (b"{not json", CapabilityToolErrorCode.INVALID_RESPONSE),
        (
            b"x" * (MAX_SERVER_RESPONSE_BYTES + 1),
            CapabilityToolErrorCode.RESPONSE_TOO_LARGE,
        ),
    ],
)
async def test_malformed_and_oversized_server_results_fail_soft(
    payload: bytes,
    expected_error: CapabilityToolErrorCode,
):
    raw, output = await _invoke(payload, facts=_facts())

    assert output.status is CapabilityToolStatus.UNAVAILABLE
    assert output.error_code is expected_error
    assert len(raw.encode()) < MAX_TOOL_OUTPUT_BYTES


@pytest.mark.asyncio
async def test_timeout_and_transport_failures_expose_no_exception_text():
    private = "https://private.internal/user@example.test"

    async def timeout(_context, _request):
        raise httpx.ReadTimeout(private)

    capability_tool = create_product_capability_tools(
        _context(_facts()),
        fetcher=timeout,
    )[0]
    raw = await capability_tool.ainvoke({})
    output = ProductCapabilitiesToolOutput.model_validate_json(raw)

    assert output.error_code is CapabilityToolErrorCode.ENDPOINT_TIMEOUT
    assert private not in raw


@pytest.mark.asyncio
async def test_unsupported_major_is_rejected_without_forwarding_server_fields():
    raw_payload = json.loads(await _server_payload("datasources.email"))
    raw_payload["schema_version"] = "2.0"
    raw_payload["future_private_field"] = "do-not-forward"

    raw, output = await _invoke(
        json.dumps(raw_payload).encode(),
        facts=_facts(),
    )

    assert output.status is CapabilityToolStatus.UNAVAILABLE
    assert output.error_code is CapabilityToolErrorCode.UNSUPPORTED_SCHEMA
    assert output.schema_compatibility is SchemaCompatibility.UNSUPPORTED_MAJOR
    assert "do-not-forward" not in raw


@pytest.mark.asyncio
async def test_same_major_projects_only_known_fields_before_model_exposure():
    raw_payload = json.loads(await _server_payload("datasources.email"))
    raw_payload["schema_version"] = "1.7"
    raw_payload["future_top"] = "top-secret-marker"
    raw_payload["capabilities"][0]["future_capability_field"] = "cap-secret-marker"
    raw_payload["capabilities"][0]["session"]["future_session_field"] = (
        "session-secret-marker"
    )

    raw, output = await _invoke(
        json.dumps(raw_payload).encode(),
        facts=_facts(
            datasource_types=("email",),
            email_tier="read",
            tools=("email_read",),
        ),
    )

    assert output.status is CapabilityToolStatus.READY
    assert output.schema_compatibility is SchemaCompatibility.SAME_MAJOR
    assert output.response is not None
    assert output.response.schema_version == SCHEMA_VERSION
    for marker in (
        "top-secret-marker",
        "cap-secret-marker",
        "session-secret-marker",
        "future_top",
        "future_capability_field",
        "future_session_field",
    ):
        assert marker not in raw


@pytest.mark.asyncio
async def test_new_agent_projects_schema_1_0_orchestrator_without_m2d_minor_fields():
    raw_payload = json.loads(await _server_payload("datasources.email"))
    raw_payload["schema_version"] = "1.0"
    for component in raw_payload["product"]["components"].values():
        component.pop("release_version", None)
        component.pop("documentation_url", None)

    _raw, output = await _invoke(
        json.dumps(raw_payload).encode(),
        facts=_facts(),
    )

    assert output.status is CapabilityToolStatus.READY
    assert output.schema_compatibility is SchemaCompatibility.SAME_MAJOR
    assert output.response is not None
    assert output.response.schema_version == SCHEMA_VERSION


@pytest.mark.asyncio
async def test_live_component_provenance_replaces_server_view_and_detects_mixed_build():
    raw_payload = json.loads(await _server_payload("datasources.email"))
    raw_payload["product"] = {
        "name": "Superhuman Remote Worker",
        "release_version": "v1.2.3",
        "mixed_build": None,
        "components": {
            "orchestrator": {
                "source_revision": "a" * 40,
                "source_url": None,
                "artifact_digest": None,
                "content_digest": None,
                "release_version": "v1.2.3",
                "documentation_url": None,
                "provenance_status": "declared",
            }
        },
    }
    agent = ComponentProvenance(
        source_revision="b" * 40,
        provenance_status=ProvenanceStatus.DECLARED,
    )
    guide = ComponentProvenance(
        source_revision="b" * 40,
        content_digest=f"sha256:{'c' * 64}",
        provenance_status=ProvenanceStatus.DECLARED,
    )

    _raw, output = await _invoke(
        json.dumps(raw_payload).encode(),
        facts=_facts(
            component_provenance=(
                (ProductComponent.AGENT, agent),
                (ProductComponent.GUIDE, guide),
            )
        ),
    )

    assert output.response is not None
    assert output.response.product.mixed_build is True
    assert output.response.product.components[ProductComponent.AGENT] == agent
    assert output.response.product.components[ProductComponent.GUIDE] == guide


@pytest.mark.asyncio
async def test_same_major_unknown_capability_is_dropped_and_marked_partial():
    raw_payload = json.loads(await _server_payload("datasources.email"))
    raw_payload["schema_version"] = "1.1"
    raw_payload["capabilities"][0]["id"] = "future.capability"
    raw_payload["capabilities"][0]["help_topic_id"] = "future"

    raw, output = await _invoke(
        json.dumps(raw_payload).encode(),
        facts=_facts(),
    )

    assert output.status is CapabilityToolStatus.PARTIAL
    assert output.error_code is CapabilityToolErrorCode.PROJECTION_INCOMPLETE
    assert output.response is not None
    assert output.response.capabilities == ()
    assert output.response.truncated is True
    assert "future.capability" not in raw


@pytest.mark.asyncio
async def test_server_cannot_claim_agent_component_or_actionability():
    raw_payload = json.loads(await _server_payload("datasources.email"))
    raw_payload["capabilities"][0]["session"]["source_component"] = "agent"

    _raw, output = await _invoke(
        json.dumps(raw_payload).encode(),
        facts=_facts(),
    )

    assert output.status is CapabilityToolStatus.UNAVAILABLE
    assert output.error_code is CapabilityToolErrorCode.INVALID_RESPONSE


@pytest.mark.asyncio
async def test_output_is_deterministic_bounded_and_tail_truncated(monkeypatch):
    import agent.tools.product_capabilities as capability_module

    monkeypatch.setattr(capability_module, "MAX_TOOL_OUTPUT_BYTES", 8_000)
    payload = await _server_payload()
    facts = _facts(
        datasource_types=("email", "kb"),
        email_tier="send",
        knowledge_binding=True,
        knowledge_store=True,
        memory=True,
        cloud=True,
        protected=True,
        tools=(
            "browser_navigate",
            "create_job",
            "email_read",
            "email_send",
            "kb_search",
            "request_workspace_upgrade",
            "set_canvas",
            "write_file",
        ),
    )
    first_raw, first = await _invoke(payload, facts=facts)
    second_raw, second = await _invoke(payload, facts=facts)

    assert first_raw == second_raw
    assert len(first_raw.encode()) <= 8_000
    assert first.status is CapabilityToolStatus.PARTIAL
    assert first.error_code is CapabilityToolErrorCode.OUTPUT_TRUNCATED
    assert first.response is not None
    assert first.response.truncated is True
    assert first.response.capabilities == second.response.capabilities


@pytest.mark.asyncio
async def test_raw_context_secrets_are_never_read_or_serialized():
    private_values = (
        "mailbox@example.test",
        "private-datasource-id",
        "INBOX/Confidential",
        "imap.private.internal",
        "/workspace/cloud/private",
    )
    context = _context(
        _facts(
            datasource_types=("email",),
            email_tier="read",
            tools=("email_read",),
        ),
        config={
            "raw_attach_payload": {
                "account": private_values[0],
                "id": private_values[1],
                "folder": private_values[2],
                "host": private_values[3],
                "path": private_values[4],
            }
        },
    )
    raw, output = await _invoke(
        await _server_payload("datasources.email"),
        facts=context.session_runtime_facts,
        context=context,
    )

    assert output.status is CapabilityToolStatus.READY
    for value in private_values:
        assert value not in raw


def test_runtime_facts_are_immutable_canonical_and_reject_private_shape_drift():
    facts = _facts(
        datasource_types=("kb", "email", "email"),
        email_tier="draft",
        tools=("email_read", "email_read"),
    )
    assert facts.attached_datasource_types == ("email", "kb")
    assert facts.loaded_tool_names == tuple(sorted(set(facts.loaded_tool_names)))

    with pytest.raises((AttributeError, TypeError)):
        facts.email_access_tier = "send"  # type: ignore[misc]
    with pytest.raises(ValueError, match="unknown backend"):
        replace(facts, backend_id="remote")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="unknown email tier"):
        replace(facts, email_access_tier="admin")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="invalid component provenance"):
        replace(
            facts,
            runtime_component_provenance=(
                (
                    ProductComponent.ORCHESTRATOR,
                    ComponentProvenance(provenance_status=ProvenanceStatus.UNAVAILABLE),
                ),
            ),
        )
    with pytest.raises(ValueError, match="invalid component provenance"):
        replace(
            facts,
            runtime_component_provenance=(
                (
                    ProductComponent.AGENT,
                    ComponentProvenance(
                        source_revision="a" * 40,
                        artifact_digest=f"sha256:{'b' * 64}",
                        provenance_status=ProvenanceStatus.VERIFIED,
                    ),
                ),
            ),
        )
    with pytest.raises(ValueError, match="attached email"):
        SessionRuntimeFacts(
            observed_at=_NOW,
            backend_id="sandbox",
            backend_supports_shell=True,
            backend_supports_file_tools=True,
            backend_supports_canvas_presentation=True,
            backend_supports_canvas_live_apps=True,
            backend_supports_shared_browser=True,
            attached_datasource_types=("email",),
            attached_tool_categories=("email",),
        )


def test_operator_canary_gate_is_wired_default_off_for_agent_deployments():
    values = (_ROOT / "helm" / "values.yaml").read_text(encoding="utf-8")
    configmap = (_ROOT / "helm" / "templates" / "configmap.yaml").read_text(
        encoding="utf-8"
    )
    env_example = (_ROOT / ".env.example").read_text(encoding="utf-8")

    assert 'productCapabilitiesToolEnabled: "false"' in values
    assert "PRODUCT_CAPABILITIES_TOOL_ENABLED:" in configmap
    assert ".Values.agent.productCapabilitiesToolEnabled" in configmap
    assert "PRODUCT_CAPABILITIES_TOOL_ENABLED=false" in env_example
