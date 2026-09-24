"""Control-plane validation and dispatch contracts for MCP datasources."""

import sys
import textwrap
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest
from fastapi import HTTPException

import orchestrator.main
from orchestrator.services.deployment_gates import (
    mcp_datasources_enabled as _mcp_datasources_enabled,
)
from orchestrator.services.deployment_gates import (
    mcp_stdio_enabled as _mcp_stdio_enabled,
)
from orchestrator.routers.datasources import (
    create_datasource,
    test_datasource as probe_datasource_endpoint,
    update_datasource,
)
from orchestrator.schemas.datasources import DatasourceCreate, DatasourceUpdate
from orchestrator.application import preparation as preparation_composition
from orchestrator.services import (
    agent_datasource_payload as agent_datasource_payload_module,
)
from orchestrator.services import datasource_config as datasource_config_module


def _route_deps(*, store=None, gates=None):
    """Compose the connector router's collaborators as main's
    ``_datasources_dependencies`` factory does.

    The three pieces this file used to patch on ``orchestrator.main`` — the
    store and the two auth gates — are injected here instead, because the
    router reads them off this dataclass rather than off a module global.
    ``mcp_datasources_enabled`` / ``validate_mcp_datasource`` stay main's own
    functions and are still called per request, so the env-var feature gates
    behave exactly as they did.
    """
    from orchestrator.routers.datasources import DatasourcesDependencies
    from orchestrator.services.datasources import DatasourceDependencies
    from orchestrator.services.kb_task_registry import KbDatasourceTaskRegistry
    from orchestrator.services.knowledge_index import KnowledgeIndexDependencies

    db = MagicMock() if store is None else store
    operations = DatasourceDependencies(
        store=db,
        vector_db=MagicMock(),
        knowledge_index=KnowledgeIndexDependencies(
            store=db,
            vector_db=MagicMock(),
            gitea_client=MagicMock(),
            logger=MagicMock(),
            tasks=KbDatasourceTaskRegistry(),
            inject_system_kb_embedding_profile=AsyncMock(return_value=None),
        ),
        mcp_datasources_enabled=_mcp_datasources_enabled,
        validate_mcp_datasource=datasource_config_module.validate_mcp_datasource,
    )
    return DatasourcesDependencies(store=db, operations=operations, **(gates or {}))


ECHO_SERVER = textwrap.dedent(
    """
    from mcp.server.fastmcp import FastMCP

    mcp = FastMCP("echo")

    @mcp.tool()
    def echo(text: str) -> str:
        \"\"\"Echo the input back.\"\"\"
        return f"echo: {text}"

    mcp.run(transport="stdio")
    """
)


def test_mcp_feature_flags_default_off_and_accept_truthy(monkeypatch):
    monkeypatch.delenv("MCP_DATASOURCES_ENABLED", raising=False)
    monkeypatch.delenv("MCP_STDIO_ENABLED", raising=False)
    assert _mcp_datasources_enabled() is False
    assert _mcp_stdio_enabled() is False

    monkeypatch.setenv("MCP_DATASOURCES_ENABLED", "yes")
    monkeypatch.setenv("MCP_STDIO_ENABLED", "1")
    assert _mcp_datasources_enabled() is True
    assert _mcp_stdio_enabled() is True


class TestMcpShapeValidation:
    def test_remote_requires_http_url(self, monkeypatch):
        monkeypatch.setenv("MCP_STDIO_ENABLED", "true")
        with pytest.raises(HTTPException, match="connection_url"):
            datasource_config_module.validate_mcp_datasource(
                None, {"transport": "http"}
            )
        with pytest.raises(HTTPException, match="HTTP"):
            datasource_config_module.validate_mcp_datasource(
                "file:///tmp/socket", {"transport": "http"}
            )

    def test_stdio_gate_and_command(self, monkeypatch):
        monkeypatch.delenv("MCP_STDIO_ENABLED", raising=False)
        with pytest.raises(HTTPException, match="disabled"):
            datasource_config_module.validate_mcp_datasource(
                None,
                {"transport": "stdio", "command": "npx"},
            )

        monkeypatch.setenv("MCP_STDIO_ENABLED", "true")
        with pytest.raises(HTTPException, match="command"):
            datasource_config_module.validate_mcp_datasource(
                None, {"transport": "stdio"}
            )

    def test_rejects_unknown_transport(self):
        with pytest.raises(HTTPException, match="transport"):
            datasource_config_module.validate_mcp_datasource(
                "https://example.test/mcp",
                {"transport": "pigeon"},
            )

    def test_validates_auth_without_echoing_secret(self):
        secret = "DO_NOT_ECHO_THIS_TOKEN"
        with pytest.raises(HTTPException) as exc:
            datasource_config_module.validate_mcp_datasource(
                "https://example.test/mcp",
                {
                    "transport": "http",
                    "auth": {"type": "headers", "headers": {"X-Key": secret + "\n"}},
                },
            )
        assert secret not in str(exc.value.detail)


@pytest.mark.asyncio
async def test_mcp_create_rejected_before_auth_when_gate_off(monkeypatch):
    monkeypatch.delenv("MCP_DATASOURCES_ENABLED", raising=False)
    body = DatasourceCreate(
        name="GitHub",
        type="mcp",
        connection_url="https://example.test/mcp",
        credentials={"transport": "http"},
    )

    approve = AsyncMock(side_effect=AssertionError("auth ran before feature gate"))
    with pytest.raises(HTTPException) as exc:
        await create_datasource(
            body,
            object(),
            dependencies=_route_deps(gates={"require_approved_user": approve}),
        )

    assert exc.value.status_code == 403
    approve.assert_not_awaited()


@pytest.mark.asyncio
async def test_remote_mcp_create_passes_credentials_to_encrypted_db_path(monkeypatch):
    monkeypatch.setenv("MCP_DATASOURCES_ENABLED", "true")
    datasource_id = UUID("11111111-2222-3333-4444-555555555555")
    credentials = {
        "transport": "http",
        "auth": {"type": "bearer", "token": "secret"},
    }
    db = MagicMock()
    db.create_datasource = AsyncMock(
        return_value={
            "id": datasource_id,
            "name": "GitHub",
            "type": "mcp",
            "connection_url": "https://example.test/mcp",
            "credentials": credentials,
        }
    )

    result = await create_datasource(
        DatasourceCreate(
            name="GitHub",
            type="mcp",
            connection_url="https://example.test/mcp",
            credentials=credentials,
        ),
        object(),
        dependencies=_route_deps(
            store=db,
            gates={
                "require_approved_user": AsyncMock(return_value={"id": UUID(int=1)})
            },
        ),
    )

    assert "credentials" not in result
    assert db.create_datasource.await_args.kwargs["credentials"] == credentials


@pytest.mark.asyncio
async def test_stdio_create_normalizes_connection_url_to_none(monkeypatch):
    monkeypatch.setenv("MCP_DATASOURCES_ENABLED", "true")
    monkeypatch.setenv("MCP_STDIO_ENABLED", "true")
    db = MagicMock()
    db.create_datasource = AsyncMock(
        return_value={"id": UUID(int=2), "name": "Local", "type": "mcp"}
    )

    await create_datasource(
        DatasourceCreate(
            name="Local",
            type="mcp",
            connection_url="https://stale.example/mcp",
            credentials={
                "transport": "stdio",
                "command": "npx",
                "args": ["-y", "@modelcontextprotocol/server-everything"],
                "env": {},
            },
        ),
        object(),
        dependencies=_route_deps(
            store=db,
            gates={
                "require_approved_user": AsyncMock(return_value={"id": UUID(int=1)})
            },
        ),
    )

    assert db.create_datasource.await_args.kwargs["connection_url"] is None


@pytest.mark.asyncio
async def test_update_validates_merged_shape_and_can_clear_url(monkeypatch):
    monkeypatch.setenv("MCP_DATASOURCES_ENABLED", "true")
    monkeypatch.setenv("MCP_STDIO_ENABLED", "true")
    datasource_id = "11111111-2222-3333-4444-555555555555"
    existing = {
        "id": datasource_id,
        "name": "Server",
        "type": "mcp",
        "connection_url": "https://example.test/mcp",
        "credentials": {"transport": "http"},
    }
    db = MagicMock()
    db.update_datasource = AsyncMock(return_value=True)
    db.list_datasource_projects = AsyncMock(return_value=[])
    db.get_datasource = AsyncMock(return_value=existing)

    result = await update_datasource(
        object(),
        datasource_id,
        DatasourceUpdate(
            connection_url=None,
            credentials={
                "transport": "stdio",
                "command": "npx",
                "args": [],
                "env": {},
            },
        ),
        dependencies=_route_deps(
            store=db,
            gates={"require_datasource_owner": AsyncMock(return_value=({}, existing))},
        ),
    )

    assert result["id"] == datasource_id
    kwargs = db.update_datasource.await_args.kwargs
    assert kwargs["connection_url"] is None
    assert kwargs["connection_url_set"] is True


def test_payload_forwards_mcp_credentials(monkeypatch):
    monkeypatch.setenv("MCP_DATASOURCES_ENABLED", "true")
    monkeypatch.setenv("MCP_STDIO_ENABLED", "true")
    credentials = {
        "transport": "stdio",
        "command": "npx",
        "args": [],
        "env": {"K": "v"},
    }
    payload = agent_datasource_payload_module.build_datasources_payload(
        [
            {
                "id": "x",
                "type": "mcp",
                "name": "Local",
                "connection_url": None,
                "credentials": credentials,
                "project_read_only": False,
            }
        ],
        dependencies=preparation_composition.datasource_payload_dependencies(
            orchestrator.main.app.state.resources
        ),
    )
    assert payload[0]["credentials"] == credentials


def test_runtime_gates_strip_existing_mcp_rows(monkeypatch):
    datasource = {
        "id": "x",
        "type": "mcp",
        "name": "Local",
        "connection_url": None,
        "credentials": {"transport": "stdio", "command": "npx"},
        "project_read_only": False,
    }

    monkeypatch.delenv("MCP_DATASOURCES_ENABLED", raising=False)
    assert (
        agent_datasource_payload_module.build_datasources_payload(
            [datasource],
            dependencies=preparation_composition.datasource_payload_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
        is None
    )
    assert (
        agent_datasource_payload_module.build_datasource_tool_override(
            [datasource],
            None,
            dependencies=preparation_composition.datasource_payload_dependencies(
                orchestrator.main.app.state.resources
            ),
        )["tools"]["mcp"]
        == []
    )

    monkeypatch.setenv("MCP_DATASOURCES_ENABLED", "true")
    monkeypatch.delenv("MCP_STDIO_ENABLED", raising=False)
    assert (
        agent_datasource_payload_module.build_datasources_payload(
            [datasource],
            dependencies=preparation_composition.datasource_payload_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
        is None
    )

    monkeypatch.setenv("MCP_STDIO_ENABLED", "true")
    assert (
        agent_datasource_payload_module.build_datasources_payload(
            [datasource],
            dependencies=preparation_composition.datasource_payload_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
        is not None
    )
    assert agent_datasource_payload_module.build_datasource_tool_override(
        [datasource],
        None,
        dependencies=preparation_composition.datasource_payload_dependencies(
            orchestrator.main.app.state.resources
        ),
    )["tools"]["mcp"] == ["*"]


class TestMcpConnectionTest:
    @pytest.mark.asyncio
    async def test_stdio_echo_server_lists_tools(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MCP_DATASOURCES_ENABLED", "true")
        monkeypatch.setenv("MCP_STDIO_ENABLED", "true")
        script = tmp_path / "echo_server.py"
        script.write_text(ECHO_SERVER)
        datasource_id = "11111111-2222-3333-4444-555555555555"
        datasource = {
            "id": datasource_id,
            "type": "mcp",
            "connection_url": None,
            "credentials": {
                "transport": "stdio",
                "command": sys.executable,
                "args": [str(script)],
                "env": {},
            },
        }

        result = await probe_datasource_endpoint(
            object(),
            datasource_id,
            dependencies=_route_deps(
                gates={
                    "require_datasource_owner": AsyncMock(return_value=({}, datasource))
                }
            ),
        )

        assert result["status"] == "ok"
        assert "echo" in result["message"]

    @pytest.mark.asyncio
    async def test_unreachable_remote_returns_error_not_exception(self, monkeypatch):
        monkeypatch.setenv("MCP_DATASOURCES_ENABLED", "true")
        datasource_id = "11111111-2222-3333-4444-555555555555"
        datasource = {
            "id": datasource_id,
            "type": "mcp",
            "connection_url": "http://127.0.0.1:9/mcp",
            "credentials": {"transport": "http"},
        }

        result = await probe_datasource_endpoint(
            object(),
            datasource_id,
            dependencies=_route_deps(
                gates={
                    "require_datasource_owner": AsyncMock(return_value=({}, datasource))
                }
            ),
        )

        assert result["status"] == "error"
        assert "MCP" in result["message"]

    @pytest.mark.asyncio
    async def test_stdio_missing_runtime_reports_untested(self, monkeypatch):
        monkeypatch.setenv("MCP_DATASOURCES_ENABLED", "true")
        monkeypatch.setenv("MCP_STDIO_ENABLED", "true")
        datasource_id = "11111111-2222-3333-4444-555555555555"
        datasource = {
            "id": datasource_id,
            "type": "mcp",
            "connection_url": None,
            "credentials": {
                "transport": "stdio",
                "command": "definitely-not-a-binary",
                "args": [],
            },
        }

        result = await probe_datasource_endpoint(
            object(),
            datasource_id,
            dependencies=_route_deps(
                gates={
                    "require_datasource_owner": AsyncMock(return_value=({}, datasource))
                }
            ),
        )

        assert result["status"] == "ok"
        assert "untested" in result["message"].lower()
