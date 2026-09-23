"""Tests for the MCP server tools, async client, and formatters.

Covers:
- AsyncCockpitClient persistent thread methods (HTTP calls, params, error handling)
- MCP server tool functions (delegation to client, formatting, error wrapping)
- Persistent thread formatter functions (output structure, edge cases)
- MCP auth fallback (X-MCP-User-Id + X-Internal-Key header auth)
"""

import os
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import httpx
import pytest

# Skip auth setup in server module
os.environ.setdefault("MCP_TRANSPORT", "stdio")

# The application package and the MCP SDK have independent import names.
from mcp_server import server as _mcp_server_mod  # noqa: E402


def test_ambiguous_mutation_is_not_labeled_as_confirmed_failure():
    error = _mcp_server_mod.MutationOutcomeUnknown("POST", "/api/jobs")

    rendered = _mcp_server_mod._format_action_error("create", "N/A", error)

    assert "unknown outcome" in rendered
    assert "failed" not in rendered.lower()


# ============================================================================
# Helpers
# ============================================================================


def _make_thread(
    thread_id=None,
    title="Test Session",
    status="active",
    config_name="defaults",
    permission_mode="supervised",
    project_id=None,
    agent_id=None,
    total_turns=5,
    total_tokens=1200,
    metadata=None,
):
    """Create a realistic thread dict."""
    return {
        "id": thread_id or str(uuid4()),
        "title": title,
        "status": status,
        "config_name": config_name,
        "permission_mode": permission_mode,
        "project_id": project_id,
        "agent_id": agent_id,
        "created_at": "2026-03-15T10:00:00Z",
        "last_activity": "2026-03-15T12:30:00Z",
        "ended_at": None,
        "total_turns": total_turns,
        "total_tokens": total_tokens,
        "metadata": metadata or {},
    }


def _make_message(role="assistant", content="Hello!", turn_number=1):
    """Create a realistic thread message dict."""
    return {
        "id": str(uuid4()),
        "role": role,
        "content": content,
        "tool_calls": None,
        "turn_number": turn_number,
        "metrics": None,
        "created_at": "2026-03-15T10:05:00Z",
    }


def _mock_response(json_data, status_code=200):
    """Create a mock httpx Response."""
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.raise_for_status = MagicMock()
    if status_code >= 400:
        resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            f"HTTP {status_code}",
            request=MagicMock(),
            response=resp,
        )
    return resp


# ============================================================================
# Formatter Tests
# ============================================================================


class TestFormatPersistentThreads:
    """Tests for format_persistent_threads."""

    def setup_method(self):
        from shared.orch_surface.formatters import format_persistent_threads

        self.fmt = format_persistent_threads

    def test_empty_list(self):
        assert self.fmt([]) == "No persistent threads found."

    def test_single_thread(self):
        threads = [_make_thread(title="My Session", status="active")]
        result = self.fmt(threads)
        assert "1 persistent thread(s)" in result
        assert "My Session" in result
        assert "[active]" in result
        assert "defaults" in result

    def test_multiple_threads(self):
        threads = [
            _make_thread(status="active"),
            _make_thread(status="idle"),
            _make_thread(status="ended"),
        ]
        result = self.fmt(threads)
        assert "3 persistent thread(s)" in result
        assert "[active]" in result
        assert "[idle]" in result
        assert "[ended]" in result

    def test_shows_turns_and_tokens(self):
        threads = [_make_thread(total_turns=42, total_tokens=9999)]
        result = self.fmt(threads)
        assert "Turns: 42" in result
        assert "Tokens: 9999" in result


class TestFormatPersistentThreadDetail:
    """Tests for format_persistent_thread_detail."""

    def setup_method(self):
        from shared.orch_surface.formatters import format_persistent_thread_detail

        self.fmt = format_persistent_thread_detail

    def test_basic_fields(self):
        thread = _make_thread(title="Dev Session", status="active")
        result = self.fmt(thread)
        assert "Dev Session" in result
        assert "active" in result
        assert "defaults" in result
        assert "supervised" in result

    def test_shows_project_and_agent(self):
        thread = _make_thread(project_id="proj-123", agent_id="agent-456")
        result = self.fmt(thread)
        assert "proj-123" in result
        assert "agent-456" in result

    def test_shows_ended_at(self):
        thread = _make_thread(status="ended")
        thread["ended_at"] = "2026-03-15T14:00:00Z"
        result = self.fmt(thread)
        assert "Ended: 2026-03-15T14:00:00Z" in result

    def test_shows_project_ids(self):
        # Phase 1 of cloud_collaboration_model.md §9: project_ids now lives
        # at the top level of the thread response (derived from
        # thread_mounts by the orchestrator), not under metadata.
        thread = _make_thread()
        thread["project_ids"] = ["p1", "p2", "p3"]
        result = self.fmt(thread)
        assert "p1" in result
        assert "p2" in result

    def test_shows_config_override(self):
        thread = _make_thread(metadata={"config_override": {"llm": {"model": "gpt-4"}}})
        result = self.fmt(thread)
        assert "Config override" in result
        assert "gpt-4" in result

    def test_truncates_long_config_override(self):
        long_config = {"key_" + str(i): "v" * 50 for i in range(20)}
        thread = _make_thread(metadata={"config_override": long_config})
        result = self.fmt(thread)
        assert "..." in result

    def test_shows_workspace_status(self):
        thread = _make_thread(
            metadata={"workspace_container": {"status": "ready", "pod_ip": "10.0.0.5"}}
        )
        result = self.fmt(thread)
        assert "Workspace: ready" in result
        assert "10.0.0.5" in result

    def test_metadata_as_string(self):
        """Handles metadata stored as JSON string (asyncpg edge case)."""
        import json

        thread = _make_thread()
        # config_override still flows through metadata after the Phase 1
        # cloud-collaboration refactor (only project_ids moved out), so it's
        # the right field to exercise the JSON-string parsing path with.
        thread["metadata"] = json.dumps(
            {"config_override": {"llm": {"model": "gpt-4"}}}
        )
        result = self.fmt(thread)
        assert "gpt-4" in result

    def test_metadata_none(self):
        thread = _make_thread()
        thread["metadata"] = None
        result = self.fmt(thread)
        assert "Thread:" in result


class TestFormatCreatedThread:
    """Tests for format_created_thread."""

    def setup_method(self):
        from shared.orch_surface.formatters import format_created_thread

        self.fmt = format_created_thread

    def test_shows_thread_id(self):
        result = self.fmt(
            {"thread_id": "abc-123", "status": "created"},
            config_name="persistent_defaults",
            title="My Session",
        )
        assert "abc-123" in result
        assert "My Session" in result
        assert "persistent_defaults" in result
        assert "created" in result

    def test_includes_usage_hint(self):
        result = self.fmt(
            {"thread_id": "tid-1", "status": "created"}, "defaults", "Test"
        )
        assert "get_persistent_thread" in result


class TestFormatPersistentThreadMessages:
    """Tests for format_persistent_thread_messages."""

    def setup_method(self):
        from shared.orch_surface.formatters import format_persistent_thread_messages

        self.fmt = format_persistent_thread_messages

    def test_empty_messages(self):
        data = {"thread_id": "tid-1", "messages": [], "total": 0}
        result = self.fmt(data)
        assert "No messages found" in result

    def test_single_message(self):
        data = {
            "thread_id": "tid-1",
            "messages": [_make_message(role="user", content="Hello")],
            "total": 1,
        }
        result = self.fmt(data)
        assert "1 of 1" in result
        assert "user" in result
        assert "Hello" in result

    def test_truncates_long_content(self):
        long_content = "x" * 600
        data = {
            "thread_id": "tid-1",
            "messages": [_make_message(content=long_content)],
            "total": 1,
        }
        result = self.fmt(data)
        assert "..." in result

    def test_full_content_not_truncated(self):
        long_content = "x" * 600
        data = {
            "thread_id": "tid-1",
            "messages": [_make_message(content=long_content)],
            "total": 1,
        }
        result = self.fmt(data, full_content=True)
        assert long_content in result
        assert "..." not in result

    def test_shows_tool_calls(self):
        msg = _make_message()
        msg["tool_calls"] = [
            {"name": "read_file", "arguments": {}},
            {"name": "write_file", "arguments": {}},
        ]
        data = {"thread_id": "tid-1", "messages": [msg], "total": 1}
        result = self.fmt(data)
        assert "read_file" in result
        assert "write_file" in result

    def test_pagination_hint(self):
        """Shows offset hint when more messages are available."""
        msgs = [_make_message(turn_number=i) for i in range(3)]
        data = {"thread_id": "tid-1", "messages": msgs, "total": 10}
        result = self.fmt(data)
        assert "offset=3" in result

    def test_no_pagination_hint_when_all_shown(self):
        msgs = [_make_message(turn_number=i) for i in range(3)]
        data = {"thread_id": "tid-1", "messages": msgs, "total": 3}
        result = self.fmt(data)
        assert "offset" not in result


class TestFormatPersistentThreadIde:
    """Tests for format_persistent_thread_ide."""

    def setup_method(self):
        from shared.orch_surface.formatters import format_persistent_thread_ide

        self.fmt = format_persistent_thread_ide

    def test_active_with_url(self):
        data = {
            "status": "active",
            "code_server_url": "http://proxy/ide/tid/proxy/",
            "source": "live_workspace",
            "gitea_url": "http://gitea/srw/thread-abc",
        }
        result = self.fmt(data)
        assert "active" in result
        assert "http://proxy/ide/tid/proxy/" in result
        assert "live_workspace" in result
        assert "http://gitea/srw/thread-abc" in result

    def test_unavailable(self):
        data = {"status": "unavailable", "code_server_url": None}
        result = self.fmt(data)
        assert "unavailable" in result
        assert "N/A" in result


class TestFormatThreadActionResult:
    """Tests for format_thread_action_result."""

    def setup_method(self):
        from shared.orch_surface.formatters import format_thread_action_result

        self.fmt = format_thread_action_result

    def test_basic_result(self):
        result = self.fmt("end_thread", "tid-1", {"status": "ended"})
        assert "end_thread" in result
        assert "tid-1" in result
        assert "ended" in result

    def test_extra_fields(self):
        result = self.fmt(
            "resume_thread", "tid-1", {"status": "created", "thread_id": "tid-1"}
        )
        assert "Thread: tid-1" in result
        # thread_id key is skipped (deduped with the Thread line)
        assert result.count("tid-1") == 1


# ============================================================================
# AsyncCockpitClient Tests
# ============================================================================


class TestAsyncCockpitClientPersistentThreads:
    """Tests for AsyncCockpitClient persistent thread methods."""

    @pytest.fixture
    def client(self):
        from shared.orch_surface.client import AsyncCockpitClient

        c = AsyncCockpitClient(base_url="http://localhost:8085")
        return c

    @pytest.mark.asyncio
    async def test_create_persistent_thread_default_params(self, client):
        """POST /api/persistent/threads with default body."""
        resp = _mock_response({"thread_id": "tid-1", "status": "created"})

        with patch.object(client._client, "post", AsyncMock(return_value=resp)) as mock:
            result = await client.create_persistent_thread()

            mock.assert_called_once()
            url = mock.call_args[0][0]
            body = mock.call_args[1]["json"]
            assert url == "/api/persistent/threads"
            assert body["config_name"] == "session_base"
            assert body["title"] == "Untitled Session"
            assert body["permission_mode"] == "supervised"
            # Optional fields should NOT be in body
            assert "project_id" not in body
            assert "datasource_ids" not in body
            assert body["use_datasource_defaults"] is True
            assert "model" not in body
            assert result == {"thread_id": "tid-1", "status": "created"}

    @pytest.mark.asyncio
    async def test_create_persistent_thread_all_params(self, client):
        """POST with all optional params included."""
        resp = _mock_response({"thread_id": "tid-2", "status": "created"})

        with patch.object(client._client, "post", AsyncMock(return_value=resp)) as mock:
            await client.create_persistent_thread(
                config_name="persistent_defaults",
                title="Research Session",
                permission_mode="autonomous",
                project_id="proj-1",
                project_ids=["proj-1", "proj-2"],
                datasource_ids=[],
                model="openai/gpt-4",
                temperature=0.7,
            )

            body = mock.call_args[1]["json"]
            assert body["config_name"] == "persistent_defaults"
            assert body["title"] == "Research Session"
            assert body["permission_mode"] == "autonomous"
            assert body["project_id"] == "proj-1"
            assert body["project_ids"] == ["proj-1", "proj-2"]
            assert body["datasource_ids"] == []
            assert "use_datasource_defaults" not in body
            assert body["model"] == "openai/gpt-4"
            assert body["temperature"] == 0.7

    @pytest.mark.asyncio
    async def test_list_persistent_threads_no_filters(self, client):
        """GET /api/persistent/threads without filters."""
        resp = _mock_response({"threads": [_make_thread()]})

        with patch.object(client._client, "get", AsyncMock(return_value=resp)) as mock:
            result = await client.list_persistent_threads()

            mock.assert_called_once()
            url = mock.call_args[0][0]
            params = mock.call_args[1].get("params", {})
            assert url == "/api/persistent/threads"
            assert params == {}
            assert len(result["threads"]) == 1

    @pytest.mark.asyncio
    async def test_list_persistent_threads_with_filters(self, client):
        """GET with project_id and status filters."""
        resp = _mock_response({"threads": []})

        with patch.object(client._client, "get", AsyncMock(return_value=resp)) as mock:
            await client.list_persistent_threads(project_id="proj-1", status="active")

            params = mock.call_args[1]["params"]
            assert params["project_id"] == "proj-1"
            assert params["status"] == "active"

    @pytest.mark.asyncio
    async def test_get_persistent_thread(self, client):
        """GET /api/persistent/threads/{id}."""
        thread = _make_thread(thread_id="tid-1")
        resp = _mock_response(thread)

        with patch.object(client._client, "get", AsyncMock(return_value=resp)) as mock:
            result = await client.get_persistent_thread("tid-1")

            url = mock.call_args[0][0]
            assert url == "/api/persistent/threads/tid-1"
            assert result["id"] == "tid-1"

    @pytest.mark.asyncio
    async def test_end_persistent_thread_soft(self, client):
        """DELETE /api/persistent/threads/{id}?permanent=false."""
        resp = _mock_response({"status": "ended"})

        with patch.object(
            client._client, "delete", AsyncMock(return_value=resp)
        ) as mock:
            result = await client.end_persistent_thread("tid-1")

            url = mock.call_args[0][0]
            params = mock.call_args[1]["params"]
            assert url == "/api/persistent/threads/tid-1"
            assert params["permanent"] is False
            assert result["status"] == "ended"

    @pytest.mark.asyncio
    async def test_end_persistent_thread_permanent(self, client):
        """DELETE with permanent=True."""
        resp = _mock_response({"status": "deleted"})

        with patch.object(
            client._client, "delete", AsyncMock(return_value=resp)
        ) as mock:
            result = await client.end_persistent_thread("tid-1", permanent=True)

            params = mock.call_args[1]["params"]
            assert params["permanent"] is True
            assert result["status"] == "deleted"

    @pytest.mark.asyncio
    async def test_resume_persistent_thread(self, client):
        """POST /api/persistent/threads/{id}/resume."""
        resp = _mock_response({"status": "created", "thread_id": "tid-1"})

        with patch.object(client._client, "post", AsyncMock(return_value=resp)) as mock:
            result = await client.resume_persistent_thread("tid-1")

            url = mock.call_args[0][0]
            assert url == "/api/persistent/threads/tid-1/resume"
            assert result["status"] == "created"

    @pytest.mark.asyncio
    async def test_get_persistent_thread_messages(self, client):
        """GET /api/persistent/threads/{id}/messages with pagination."""
        msgs_data = {
            "messages": [_make_message()],
            "total": 1,
            "thread_id": "tid-1",
        }
        resp = _mock_response(msgs_data)

        with patch.object(client._client, "get", AsyncMock(return_value=resp)) as mock:
            result = await client.get_persistent_thread_messages(
                "tid-1", limit=50, offset=10
            )

            url = mock.call_args[0][0]
            params = mock.call_args[1]["params"]
            assert url == "/api/persistent/threads/tid-1/messages"
            assert params["limit"] == 50
            assert params["offset"] == 10
            assert result["total"] == 1

    @pytest.mark.asyncio
    async def test_get_persistent_thread_ide(self, client):
        """GET /api/persistent/threads/{id}/ide."""
        ide_data = {
            "status": "active",
            "code_server_url": "http://proxy/ide/tid-1/proxy/",
            "source": "live_workspace",
            "gitea_url": None,
        }
        resp = _mock_response(ide_data)

        with patch.object(client._client, "get", AsyncMock(return_value=resp)) as mock:
            result = await client.get_persistent_thread_ide("tid-1")

            url = mock.call_args[0][0]
            assert url == "/api/persistent/threads/tid-1/ide"
            assert result["status"] == "active"

    @pytest.mark.asyncio
    async def test_http_error_propagates(self, client):
        """Client raises on non-2xx responses."""
        resp = _mock_response({}, status_code=404)

        with patch.object(client._client, "get", AsyncMock(return_value=resp)):
            with pytest.raises(httpx.HTTPStatusError):
                await client.get_persistent_thread("nonexistent")


# ============================================================================
# MCP Server Tool Tests
# ============================================================================


@pytest.mark.asyncio
async def test_list_datasources_tool_reports_full_ids_revisions_and_cursor():
    connector_id = "11111111-2222-4333-8444-555555555555"
    mock_client = AsyncMock()
    mock_client.list_datasources.return_value = {
        "items": [
            {
                "id": connector_id,
                "name": "Application database",
                "type": "postgresql",
                "scope_mode": "projects",
                "project_ids": ["aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"],
                "policy_revision": 23,
            }
        ],
        "next_cursor": "opaque-next-page",
    }

    with patch.object(_mcp_server_mod, "_get_client", return_value=mock_client):
        result = await _mcp_server_mod.list_datasources(
            ds_type="postgresql",
            q="application",
            scope_mode="projects",
            ownership="mine",
            limit=10,
            cursor="opaque-current-page",
        )

    mock_client.list_datasources.assert_awaited_once_with(
        ds_type="postgresql",
        q="application",
        project_id=None,
        scope_mode="projects",
        auto_attach=None,
        visibility=None,
        ownership="mine",
        availability=None,
        limit=10,
        cursor="opaque-current-page",
    )
    assert connector_id in result
    assert "Policy revision: 23" in result
    assert "Next cursor: opaque-next-page" in result


@pytest.mark.asyncio
async def test_get_datasource_tool_reports_exact_management_state():
    connector_id = "11111111-2222-4333-8444-555555555555"
    mock_client = AsyncMock()
    mock_client.get_datasource.return_value = {
        "id": connector_id,
        "name": "Application database",
        "type": "postgresql",
        "scope_mode": "all",
        "project_ids": [],
        "policy_revision": 24,
    }

    with patch.object(_mcp_server_mod, "_get_client", return_value=mock_client):
        result = await _mcp_server_mod.get_datasource(connector_id)

    mock_client.get_datasource.assert_awaited_once_with(connector_id)
    assert f"ID: {connector_id}" in result
    assert "Policy revision: 24" in result


@pytest.mark.asyncio
async def test_update_datasource_tool_reports_new_policy_revision():
    mock_client = AsyncMock()
    mock_client.update_datasource.return_value = {
        "id": "connector-1",
        "scope_mode": "projects",
        "project_ids": ["project-a"],
        "auto_attach": True,
        "policy_revision": 8,
    }

    with patch.object(_mcp_server_mod, "_get_client", return_value=mock_client):
        result = await _mcp_server_mod.update_datasource(
            "connector-1",
            scope_mode="projects",
            project_ids=["project-a"],
            auto_attach=True,
            policy_revision=7,
        )

    mock_client.update_datasource.assert_awaited_once_with(
        datasource_id="connector-1",
        name=None,
        description=None,
        connection_url=None,
        credentials=None,
        cli_hint=None,
        default_branch=None,
        config=None,
        is_global=None,
        read_only=None,
        scope_mode="projects",
        project_ids=["project-a"],
        auto_attach=True,
        policy_revision=7,
    )
    assert "Policy revision: 8" in result
    assert "Availability scope: projects" in result
    assert "Projects: project-a" in result


@pytest.mark.asyncio
async def test_reindex_summary_reports_skipped_duplicates():
    """Final review, Important 2b. The reindexer counts notes it declined to
    index because another path already holds the id; the summary omitted the
    number, so a partially-indexed vault read as a clean run — the notes are
    simply absent from search with nothing saying why."""
    mock_client = AsyncMock()
    mock_client.reindex_knowledge.return_value = {
        "status": "ok",
        "indexed_commit": "abcdef1234567890",
        "full": False,
        "upserted": 12,
        "deleted": 1,
        "skipped": 3,
        "skipped_duplicates": 2,
        "errors": 0,
    }

    with patch.object(_mcp_server_mod, "_get_client", return_value=mock_client):
        result = await _mcp_server_mod.reindex_knowledge("project-1")

    assert "skipped: 3" in result
    assert "skipped_duplicates: 2" in result
    assert "errors: 0" in result


@pytest.mark.asyncio
async def test_reindex_summary_defaults_skipped_duplicates_for_older_payloads():
    mock_client = AsyncMock()
    mock_client.reindex_knowledge.return_value = {"status": "ok"}

    with patch.object(_mcp_server_mod, "_get_client", return_value=mock_client):
        result = await _mcp_server_mod.reindex_knowledge("project-1")

    assert "skipped_duplicates: 0" in result


@pytest.mark.asyncio
async def test_update_project_tool_names_the_stored_secrets_it_dropped():
    """A read-modify-write through get_project + update_project that changes
    an endpoint keeps none of the redacted secrets; the tool must say so."""
    mock_client = AsyncMock()
    mock_client.update_project.return_value = {
        "status": "updated",
        "dropped_hidden_keys": ["llm.api_key", "workspace.remote"],
    }

    with patch.object(_mcp_server_mod, "_get_client", return_value=mock_client):
        result = await _mcp_server_mod.update_project(
            "project-1", default_config_override={"llm": {"base_url": "https://x"}}
        )

    assert "llm.api_key" in result
    assert "workspace.remote" in result

    mock_client.update_project.return_value = {"status": "updated"}
    with patch.object(_mcp_server_mod, "_get_client", return_value=mock_client):
        result = await _mcp_server_mod.update_project("project-1", name="n")

    assert result == "Project project-1 updated (updated)."


class TestMcpPersistentThreadTools:
    """Tests for MCP server persistent thread tool functions.

    Each tool is tested in isolation by mocking _get_client() to return
    a mock AsyncCockpitClient.
    """

    @pytest.fixture(autouse=True)
    def mock_client(self):
        """Provide a mock client via _get_client()."""
        self._mock = AsyncMock()
        with patch.object(_mcp_server_mod, "_get_client", return_value=self._mock):
            yield self._mock

    @pytest.mark.asyncio
    async def test_create_persistent_thread_success(self, mock_client):
        create_persistent_thread = _mcp_server_mod.create_persistent_thread

        mock_client.create_persistent_thread.return_value = {
            "thread_id": "tid-1",
            "status": "created",
        }

        result = await create_persistent_thread(title="Test", config_name="defaults")

        mock_client.create_persistent_thread.assert_awaited_once_with(
            config_name="defaults",
            title="Test",
            permission_mode="supervised",
            project_id=None,
            project_ids=None,
            datasource_ids=None,
            model=None,
            temperature=None,
        )
        assert "tid-1" in result
        assert "created" in result

    @pytest.mark.asyncio
    async def test_create_persistent_thread_forwards_explicit_empty_datasources(
        self, mock_client
    ):
        create_persistent_thread = _mcp_server_mod.create_persistent_thread
        mock_client.create_persistent_thread.return_value = {
            "thread_id": "tid-2",
            "status": "created",
        }

        await create_persistent_thread(datasource_ids=[])

        assert (
            mock_client.create_persistent_thread.await_args.kwargs["datasource_ids"]
            == []
        )

    @pytest.mark.asyncio
    async def test_create_persistent_thread_error(self, mock_client):
        create_persistent_thread = _mcp_server_mod.create_persistent_thread

        mock_client.create_persistent_thread.side_effect = httpx.HTTPStatusError(
            "500", request=MagicMock(), response=MagicMock(status_code=500)
        )

        result = await create_persistent_thread()

        assert "failed" in result.lower()

    @pytest.mark.asyncio
    async def test_list_persistent_threads(self, mock_client):
        list_persistent_threads = _mcp_server_mod.list_persistent_threads

        threads = [_make_thread(status="active"), _make_thread(status="idle")]
        mock_client.list_persistent_threads.return_value = {"threads": threads}

        result = await list_persistent_threads(status="active")

        mock_client.list_persistent_threads.assert_awaited_once_with(
            project_id=None, status="active"
        )
        assert "2 persistent thread(s)" in result

    @pytest.mark.asyncio
    async def test_list_persistent_threads_empty(self, mock_client):
        list_persistent_threads = _mcp_server_mod.list_persistent_threads

        mock_client.list_persistent_threads.return_value = {"threads": []}

        result = await list_persistent_threads()

        assert "No persistent threads found" in result

    @pytest.mark.asyncio
    async def test_get_persistent_thread(self, mock_client):
        get_persistent_thread = _mcp_server_mod.get_persistent_thread

        thread = _make_thread(thread_id="tid-1", title="My Session")
        mock_client.get_persistent_thread.return_value = thread

        result = await get_persistent_thread("tid-1")

        mock_client.get_persistent_thread.assert_awaited_once_with("tid-1")
        assert "My Session" in result
        assert "tid-1" in result

    @pytest.mark.asyncio
    async def test_end_persistent_thread_soft(self, mock_client):
        end_persistent_thread = _mcp_server_mod.end_persistent_thread

        mock_client.end_persistent_thread.return_value = {"status": "ended"}

        result = await end_persistent_thread("tid-1")

        mock_client.end_persistent_thread.assert_awaited_once_with(
            "tid-1", permanent=False
        )
        assert "ended" in result
        assert "end_thread" in result

    @pytest.mark.asyncio
    async def test_end_persistent_thread_permanent(self, mock_client):
        end_persistent_thread = _mcp_server_mod.end_persistent_thread

        mock_client.end_persistent_thread.return_value = {"status": "deleted"}

        result = await end_persistent_thread("tid-1", permanent=True)

        mock_client.end_persistent_thread.assert_awaited_once_with(
            "tid-1", permanent=True
        )
        assert "deleted" in result
        assert "delete_thread" in result

    @pytest.mark.asyncio
    async def test_end_persistent_thread_error(self, mock_client):
        end_persistent_thread = _mcp_server_mod.end_persistent_thread

        mock_client.end_persistent_thread.side_effect = Exception("DB down")

        result = await end_persistent_thread("tid-1")

        assert "failed" in result.lower()
        assert "tid-1" in result

    @pytest.mark.asyncio
    async def test_resume_persistent_thread_success(self, mock_client):
        resume_persistent_thread = _mcp_server_mod.resume_persistent_thread

        mock_client.resume_persistent_thread.return_value = {
            "status": "created",
            "thread_id": "tid-1",
        }

        result = await resume_persistent_thread("tid-1")

        mock_client.resume_persistent_thread.assert_awaited_once_with(
            "tid-1", acknowledge=None
        )
        assert "created" in result
        assert "resume_thread" in result

    @pytest.mark.asyncio
    async def test_resume_persistent_thread_forwards_acknowledge(self, mock_client):
        resume_persistent_thread = _mcp_server_mod.resume_persistent_thread

        mock_client.resume_persistent_thread.return_value = {
            "status": "created",
            "thread_id": "tid-1",
        }

        result = await resume_persistent_thread("tid-1", acknowledge=["connector:abc"])

        mock_client.resume_persistent_thread.assert_awaited_once_with(
            "tid-1", acknowledge=["connector:abc"]
        )
        assert "created" in result

    @pytest.mark.asyncio
    async def test_resume_persistent_thread_error(self, mock_client):
        resume_persistent_thread = _mcp_server_mod.resume_persistent_thread

        mock_client.resume_persistent_thread.side_effect = Exception("409 conflict")

        result = await resume_persistent_thread("tid-1")

        assert "failed" in result.lower()

    @pytest.mark.asyncio
    async def test_get_persistent_thread_messages(self, mock_client):
        get_thread_messages = _mcp_server_mod.get_persistent_thread_messages

        msgs = [_make_message(role="user", content="Hi", turn_number=1)]
        mock_client.get_persistent_thread_messages.return_value = {
            "messages": msgs,
            "total": 1,
            "thread_id": "tid-1",
        }

        result = await get_thread_messages("tid-1", limit=25, offset=0)

        mock_client.get_persistent_thread_messages.assert_awaited_once_with(
            "tid-1", limit=25, offset=0
        )
        assert "user" in result
        assert "Hi" in result

    @pytest.mark.asyncio
    async def test_get_persistent_thread_messages_clamps_limit(self, mock_client):
        get_thread_messages = _mcp_server_mod.get_persistent_thread_messages

        mock_client.get_persistent_thread_messages.return_value = {
            "messages": [],
            "total": 0,
            "thread_id": "tid-1",
        }

        await get_thread_messages("tid-1", limit=9999)

        # Should clamp to 500
        call_kwargs = mock_client.get_persistent_thread_messages.call_args
        assert call_kwargs[1]["limit"] == 500

    @pytest.mark.asyncio
    async def test_get_persistent_thread_messages_clamps_limit_low(self, mock_client):
        get_thread_messages = _mcp_server_mod.get_persistent_thread_messages

        mock_client.get_persistent_thread_messages.return_value = {
            "messages": [],
            "total": 0,
            "thread_id": "tid-1",
        }

        await get_thread_messages("tid-1", limit=-5)

        call_kwargs = mock_client.get_persistent_thread_messages.call_args
        assert call_kwargs[1]["limit"] == 1

    @pytest.mark.asyncio
    async def test_get_persistent_thread_ide(self, mock_client):
        get_thread_ide = _mcp_server_mod.get_persistent_thread_ide

        mock_client.get_persistent_thread_ide.return_value = {
            "status": "active",
            "code_server_url": "http://proxy/ide/tid-1/proxy/",
            "source": "live_vm",
            "gitea_url": "http://gitea/srw/thread-abc",
        }

        result = await get_thread_ide("tid-1")

        mock_client.get_persistent_thread_ide.assert_awaited_once_with("tid-1")
        assert "active" in result
        assert "http://proxy/ide/tid-1/proxy/" in result
        assert "live_vm" in result


# ============================================================================
# Auth Fallback Tests
# ============================================================================


def _mock_request_with_headers(header_map: dict[str, str]) -> MagicMock:
    """Create a mock Request whose .headers.get() returns from header_map.

    ``.cookies`` is a real empty dict — Starlette's Request exposes cookies
    as a dict, and the BFF cookie path in ``get_current_user`` does
    ``request.cookies.get('srw_session')``. Leaving it as a bare MagicMock
    would return a truthy MagicMock and falsely trigger the cookie branch.
    """
    mock_request = MagicMock()
    mock_headers = MagicMock()
    mock_headers.get = lambda key, default="": header_map.get(key, default)
    mock_request.headers = mock_headers
    mock_request.cookies = {}
    return mock_request


class TestMcpAuthFallback:
    """Tests for the MCP internal header auth fallback in get_current_user."""

    @pytest.mark.asyncio
    async def test_accepts_mcp_headers(self):
        """Valid X-MCP-User-Id + X-Internal-Key returns the user."""
        from orchestrator.security.auth import get_current_user

        user_id = str(uuid4())
        # Approval now flows from the user row (app-side admission); the MCP
        # header path no longer force-sets is_approved, so the row must carry it.
        user_record = {
            "id": user_id,
            "display_name": "MCP User",
            "email": "u@t.com",
            "is_approved": True,
        }

        mock_request = _mock_request_with_headers(
            {
                "Authorization": "",
                "X-MCP-User-Id": user_id,
                "X-Internal-Key": "test-secret",
            }
        )

        mock_db = AsyncMock()
        mock_db.get_user = AsyncMock(return_value=dict(user_record))

        with patch.dict(os.environ, {"MCP_INTERNAL_KEY": "test-secret"}):
            result = await get_current_user(mock_request, mock_db)

        assert result["id"] == user_id
        assert result["is_approved"] is True
        mock_db.get_user.assert_awaited_once_with(user_id)

    @pytest.mark.asyncio
    async def test_rejects_wrong_internal_key(self):
        """Wrong X-Internal-Key raises 401."""
        from fastapi import HTTPException

        from orchestrator.security.auth import get_current_user

        mock_request = _mock_request_with_headers(
            {
                "Authorization": "",
                "X-MCP-User-Id": "some-user",
                "X-Internal-Key": "wrong-key",
            }
        )

        mock_db = AsyncMock()

        with patch.dict(os.environ, {"MCP_INTERNAL_KEY": "correct-key"}):
            with pytest.raises(HTTPException) as exc_info:
                await get_current_user(mock_request, mock_db)
            assert exc_info.value.status_code == 401

    @pytest.mark.asyncio
    async def test_rejects_missing_headers(self):
        """No Bearer and no MCP headers raises 401."""
        from fastapi import HTTPException

        from orchestrator.security.auth import get_current_user

        mock_request = _mock_request_with_headers({})

        mock_db = AsyncMock()

        with patch.dict(os.environ, {"MCP_INTERNAL_KEY": "secret"}):
            with pytest.raises(HTTPException) as exc_info:
                await get_current_user(mock_request, mock_db)
            assert exc_info.value.status_code == 401

    @pytest.mark.asyncio
    async def test_rejects_user_not_found(self):
        """Valid headers but user not in DB raises 401."""
        from fastapi import HTTPException

        from orchestrator.security.auth import get_current_user

        mock_request = _mock_request_with_headers(
            {
                "Authorization": "",
                "X-MCP-User-Id": "missing-user",
                "X-Internal-Key": "test-secret",
            }
        )

        mock_db = AsyncMock()
        mock_db.get_user = AsyncMock(return_value=None)

        with patch.dict(os.environ, {"MCP_INTERNAL_KEY": "test-secret"}):
            with pytest.raises(HTTPException) as exc_info:
                await get_current_user(mock_request, mock_db)
            assert exc_info.value.status_code == 401


class TestAsyncCockpitClientAuditChatBulk:
    """The MCP bulk readers now page the lean ``/audit`` + ``/chat`` endpoints.

    The dedicated ``/audit/bulk`` + ``/chat/bulk`` routes were removed (they
    materialized whole-job histories incl. heavy per-row metadata and OOM'd the
    orchestrator). These tests pin the new request shapes so a regression can't
    silently re-point them at the dead routes or drop the lean projection.
    """

    @pytest.fixture
    def client(self):
        from shared.orch_surface.client import AsyncCockpitClient

        return AsyncCockpitClient(base_url="http://localhost:8085")

    @pytest.mark.asyncio
    async def test_get_audit_bulk_uses_lean_audit_endpoint(self, client):
        """audit bulk -> GET /audit?lean=true with offset/limit + filter."""
        resp = _mock_response(
            {"entries": [], "total": 0, "offset": 0, "limit": 200, "hasMore": False}
        )
        with patch.object(client._client, "get", AsyncMock(return_value=resp)) as mock:
            await client.get_audit_bulk(job_id="job-1", offset=20, limit=100)

            url = mock.call_args[0][0]
            params = mock.call_args[1]["params"]
            assert url == "/api/jobs/job-1/audit"
            assert params["lean"] == "true"
            assert params["offset"] == 20
            assert params["limit"] == 100
            assert params["filter"] == "all"

    @pytest.mark.asyncio
    async def test_get_audit_bulk_passes_filter(self, client):
        """The filter category must reach the endpoint (it was dropped before)."""
        resp = _mock_response({"entries": [], "total": 0, "hasMore": False})
        with patch.object(client._client, "get", AsyncMock(return_value=resp)) as mock:
            await client.get_audit_bulk(job_id="job-1", filter_category="errors")
            assert mock.call_args[1]["params"]["filter"] == "errors"

    @pytest.mark.asyncio
    async def test_get_audit_bulk_caps_limit_at_200(self, client):
        """The lean endpoint rejects limit>200, so the client must clamp."""
        resp = _mock_response({"entries": [], "total": 0, "hasMore": False})
        with patch.object(client._client, "get", AsyncMock(return_value=resp)) as mock:
            await client.get_audit_bulk(job_id="job-1", limit=5000)
            assert mock.call_args[1]["params"]["limit"] == 200

    @pytest.mark.asyncio
    async def test_get_chat_bulk_uses_chat_endpoint(self, client):
        """chat bulk -> GET /chat with offset/limit (no /chat/bulk)."""
        resp = _mock_response(
            {"entries": [], "total": 0, "offset": 0, "limit": 200, "hasMore": False}
        )
        with patch.object(client._client, "get", AsyncMock(return_value=resp)) as mock:
            await client.get_chat_bulk(job_id="job-1", offset=10, limit=50)

            url = mock.call_args[0][0]
            params = mock.call_args[1]["params"]
            assert url == "/api/jobs/job-1/chat"
            assert params["offset"] == 10
            assert params["limit"] == 50

    @pytest.mark.asyncio
    async def test_get_chat_bulk_caps_limit_at_200(self, client):
        resp = _mock_response({"entries": [], "total": 0, "hasMore": False})
        with patch.object(client._client, "get", AsyncMock(return_value=resp)) as mock:
            await client.get_chat_bulk(job_id="job-1", limit=5000)
            assert mock.call_args[1]["params"]["limit"] == 200
