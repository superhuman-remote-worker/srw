"""Control-plane validation and dispatch contracts for MCP datasources.

A stdio server is retired (connector drivers D5b): it is refused on create
and edit, never delivered and never probed; a stored stdio row can still be
read, deleted, or moved to a remote transport.
"""

import asyncio
import socket
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
from shared.connectors.builtin import MCP_STDIO_RETIRED


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
    from orchestrator.services.connector_drivers import builtin_connector_drivers
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
        connector_drivers=builtin_connector_drivers(),
    )
    return DatasourcesDependencies(store=db, operations=operations, **(gates or {}))


ECHO_SERVER = textwrap.dedent(
    """
    import sys
    from mcp.server.fastmcp import FastMCP

    mcp = FastMCP("echo", host="127.0.0.1", port=int(sys.argv[1]), log_level="ERROR")

    @mcp.tool()
    def echo(text: str) -> str:
        \"\"\"Echo the input back.\"\"\"
        return f"echo: {text}"

    mcp.run(transport="streamable-http")
    """
)

#: A stored stdio server, as rows written before D5b hold it.
_STDIO_ROW = {
    "id": "x",
    "type": "mcp",
    "name": "Local",
    "connection_url": None,
    "credentials": {"transport": "stdio", "command": "npx", "env": {"K": "v"}},
    "project_read_only": False,
}


def test_mcp_feature_flag_defaults_off_and_accepts_truthy(monkeypatch):
    monkeypatch.delenv("MCP_DATASOURCES_ENABLED", raising=False)
    assert _mcp_datasources_enabled() is False

    monkeypatch.setenv("MCP_DATASOURCES_ENABLED", "yes")
    assert _mcp_datasources_enabled() is True


class TestMcpShapeValidation:
    def test_remote_requires_http_url(self):
        with pytest.raises(HTTPException, match="connection_url"):
            datasource_config_module.validate_mcp_datasource(
                None, {"transport": "http"}
            )
        with pytest.raises(HTTPException, match="HTTP"):
            datasource_config_module.validate_mcp_datasource(
                "file:///tmp/socket", {"transport": "http"}
            )

    @pytest.mark.parametrize("flag", [None, "true"])
    def test_stdio_is_refused_whatever_the_old_flag_says(self, monkeypatch, flag):
        if flag is None:
            monkeypatch.delenv("MCP_STDIO_ENABLED", raising=False)
        else:
            monkeypatch.setenv("MCP_STDIO_ENABLED", flag)
        for credentials in (
            {"transport": "stdio"},
            {"transport": "stdio", "command": "npx", "args": [], "env": {}},
        ):
            with pytest.raises(HTTPException) as refused:
                datasource_config_module.validate_mcp_datasource(None, credentials)
            assert refused.value.status_code == 400
            assert refused.value.detail == MCP_STDIO_RETIRED

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
async def test_stdio_create_is_refused_and_writes_nothing(monkeypatch):
    monkeypatch.setenv("MCP_DATASOURCES_ENABLED", "true")
    monkeypatch.setenv("MCP_STDIO_ENABLED", "true")
    db = MagicMock()
    db.create_datasource = AsyncMock(
        return_value={"id": UUID(int=2), "name": "Local", "type": "mcp"}
    )

    with pytest.raises(HTTPException) as refused:
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

    assert refused.value.status_code == 400
    assert refused.value.detail == MCP_STDIO_RETIRED
    db.create_datasource.assert_not_awaited()


def _update_deps(existing):
    db = MagicMock()
    db.update_datasource = AsyncMock(return_value=True)
    db.list_datasource_projects = AsyncMock(return_value=[])
    db.get_datasource = AsyncMock(return_value=existing)
    return db, _route_deps(
        store=db,
        gates={"require_datasource_owner": AsyncMock(return_value=({}, existing))},
    )


@pytest.mark.asyncio
async def test_update_to_stdio_is_refused(monkeypatch):
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
    db, dependencies = _update_deps(existing)

    with pytest.raises(HTTPException) as refused:
        await update_datasource(
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
            dependencies=dependencies,
        )

    assert refused.value.detail == MCP_STDIO_RETIRED
    db.update_datasource.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_stored_stdio_row_moves_to_a_remote_url(monkeypatch):
    monkeypatch.setenv("MCP_DATASOURCES_ENABLED", "true")
    datasource_id = "11111111-2222-3333-4444-555555555555"
    existing = {**_STDIO_ROW, "id": datasource_id}
    db, dependencies = _update_deps(existing)

    result = await update_datasource(
        object(),
        datasource_id,
        DatasourceUpdate(
            connection_url="https://example.test/mcp",
            credentials={"transport": "http"},
        ),
        dependencies=dependencies,
    )

    assert result["id"] == datasource_id
    kwargs = db.update_datasource.await_args.kwargs
    assert kwargs["connection_url"] == "https://example.test/mcp"
    assert kwargs["credentials"] == {"transport": "http"}


def test_payload_forwards_remote_mcp_credentials(monkeypatch):
    monkeypatch.setenv("MCP_DATASOURCES_ENABLED", "true")
    credentials = {
        "transport": "http",
        "auth": {"type": "bearer", "token": "secret"},
    }
    payload = agent_datasource_payload_module.build_datasources_payload(
        [
            {
                "id": "x",
                "type": "mcp",
                "name": "Remote",
                "connection_url": "https://example.test/mcp",
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
    datasource = dict(_STDIO_ROW)

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

    # A stored stdio server stays out with the gate on, whatever the
    # retired MCP_STDIO_ENABLED says.
    monkeypatch.setenv("MCP_DATASOURCES_ENABLED", "true")
    monkeypatch.setenv("MCP_STDIO_ENABLED", "true")
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


async def _wait_for_port(port: int) -> None:
    for _ in range(100):
        try:
            _reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.close()
            await writer.wait_closed()
            return
        except OSError:
            await asyncio.sleep(0.05)
    raise AssertionError(f"test MCP server did not listen on port {port}")


class TestMcpConnectionTest:
    @pytest.mark.asyncio
    async def test_remote_echo_server_lists_tools(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MCP_DATASOURCES_ENABLED", "true")
        script = tmp_path / "echo_server.py"
        script.write_text(ECHO_SERVER)
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        server = await asyncio.create_subprocess_exec(
            sys.executable,
            str(script),
            str(port),
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        datasource_id = "11111111-2222-3333-4444-555555555555"
        datasource = {
            "id": datasource_id,
            "type": "mcp",
            "connection_url": f"http://127.0.0.1:{port}/mcp",
            "credentials": {"transport": "http"},
        }

        try:
            await _wait_for_port(port)
            result = await probe_datasource_endpoint(
                object(),
                datasource_id,
                dependencies=_route_deps(
                    gates={
                        "require_datasource_owner": AsyncMock(
                            return_value=({}, datasource)
                        )
                    }
                ),
            )
        finally:
            server.terminate()
            await server.wait()

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
    async def test_a_stored_stdio_server_is_never_run(self, monkeypatch):
        monkeypatch.setenv("MCP_DATASOURCES_ENABLED", "true")
        monkeypatch.setenv("MCP_STDIO_ENABLED", "true")
        datasource_id = "11111111-2222-3333-4444-555555555555"
        datasource = {
            "id": datasource_id,
            "type": "mcp",
            "connection_url": None,
            "credentials": {
                "transport": "stdio",
                "command": sys.executable,
                "args": ["-c", "raise SystemExit('ran')"],
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

        assert result == {"status": "unsupported", "message": MCP_STDIO_RETIRED}
