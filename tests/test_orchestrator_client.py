"""Tests for the orchestrator client module."""

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from agent.api.orchestrator_client import (
    OrchestratorClient,
    SessionGrantDenied,
    ThreadConfigUpdateDenied,
    SubagentPersistenceError,
    VerdictRecordingError,
    create_orchestrator_client_from_env,
    get_agent_ip,
    get_hostname,
)
from shared.subagent_parent_authority import (
    ParentExecutionAuthority,
    ParentExecutionAuthorityRefused,
)


RUNTIME_GENERATION = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
RUNTIME_ATTACH_TOKEN = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
RUNTIME_RETIREMENT_TOKEN = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
PROCESS_GENERATION = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"


class TestGetAgentIp:
    """Tests for get_agent_ip function."""

    def test_get_agent_ip_from_env(self):
        """Test that AGENT_POD_IP env var is used when set."""
        with patch.dict(os.environ, {"AGENT_POD_IP": "192.168.1.100"}):
            ip = get_agent_ip()
            assert ip == "192.168.1.100"

    def test_get_agent_ip_auto_detect(self):
        """Test IP auto-detection when env var not set."""
        with patch.dict(os.environ, {}, clear=True):
            # Remove AGENT_POD_IP if it exists
            os.environ.pop("AGENT_POD_IP", None)
            ip = get_agent_ip()
            # Should return some valid IP (localhost or detected)
            assert ip is not None
            assert len(ip) > 0


class TestGetHostname:
    """Tests for get_hostname function."""

    def test_get_hostname_from_env(self):
        """Test that AGENT_HOSTNAME env var is used when set."""
        with patch.dict(os.environ, {"AGENT_HOSTNAME": "test-pod-1"}):
            hostname = get_hostname()
            assert hostname == "test-pod-1"

    def test_get_hostname_auto_detect(self):
        """Test hostname auto-detection when env var not set."""
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("AGENT_HOSTNAME", None)
            hostname = get_hostname()
            # Should return some valid hostname
            assert hostname is not None
            assert len(hostname) > 0


class TestOrchestratorClient:
    """Tests for OrchestratorClient class."""

    @pytest.fixture
    def client(self):
        """Create a test client instance."""
        return OrchestratorClient(
            orchestrator_url="http://localhost:8085",
            pod_ip="10.0.0.5",
            pod_port=8001,
            hostname="test-agent",
            config_name="creator",
            pid=12345,
        )

    @pytest.mark.asyncio
    async def test_connect(self, client):
        """Test client connection."""
        await client.connect()
        assert client._client is not None
        await client.close()

    @pytest.mark.asyncio
    async def test_close(self, client):
        """Test client close."""
        await client.connect()
        await client.close()
        assert client._client is None

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("outcome", "expected"),
        [
            ("released", True),
            ("already_detached", True),
            ("retirement_acknowledged", True),
            ("unsafe", False),
            ("unchanged", False),
        ],
    )
    async def test_release_thread_agent_requires_confirmed_outcome(
        self, client, outcome, expected
    ):
        response = MagicMock(status_code=200)
        response.json.return_value = {"status": outcome}
        client.agent_id = "agent-a"
        client.session_runtime_generation = RUNTIME_GENERATION
        client.session_runtime_attach_token = RUNTIME_ATTACH_TOKEN
        client._client = MagicMock()
        client._client.post = AsyncMock(return_value=response)

        assert (
            await client.release_thread_agent(
                "thread-a",
                agent_pod_uid="pod-uid-a",
                local_runtime_quiesced=True,
                local_quiescence_protocol="agent_runtime_zero_v1",
            )
            is expected
        )
        assert client._client.post.await_args.kwargs["json"] == {
            "agent_id": "agent-a",
            "session_runtime_generation": RUNTIME_GENERATION,
            "session_runtime_attach_token": RUNTIME_ATTACH_TOKEN,
            "agent_pod_uid": "pod-uid-a",
            "local_runtime_quiesced": True,
            "local_quiescence_protocol": "agent_runtime_zero_v1",
        }

    @pytest.mark.asyncio
    async def test_release_thread_agent_rejects_malformed_success_body(self, client):
        response = MagicMock(status_code=200)
        response.json.return_value = {"status": True}
        client.agent_id = "agent-a"
        client.session_runtime_generation = RUNTIME_GENERATION
        client.session_runtime_attach_token = RUNTIME_ATTACH_TOKEN
        client._client = MagicMock()
        client._client.post = AsyncMock(return_value=response)

        assert (
            await client.release_thread_agent(
                "thread-a",
                agent_pod_uid="pod-uid-a",
                local_runtime_quiesced=True,
                local_quiescence_protocol="agent_runtime_zero_v1",
            )
            is False
        )

    @pytest.mark.asyncio
    async def test_release_thread_agent_requires_quiescence_before_http(self, client):
        client.agent_id = "agent-a"
        client.session_runtime_generation = RUNTIME_GENERATION
        client.session_runtime_attach_token = RUNTIME_ATTACH_TOKEN
        client._client = MagicMock()
        client._client.post = AsyncMock()

        assert await client.release_thread_agent("thread-a") is False
        client._client.post.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_release_thread_agent_accepts_exact_pre_setup_proof(self, client):
        response = MagicMock(status_code=200)
        response.json.return_value = {"status": "already_detached"}
        client.agent_id = "agent-a"
        client.session_runtime_generation = RUNTIME_GENERATION
        client.session_runtime_attach_token = RUNTIME_ATTACH_TOKEN
        client._client = MagicMock()
        client._client.post = AsyncMock(return_value=response)

        assert await client.release_thread_agent(
            "thread-a",
            agent_pod_uid="pod-uid-a",
            local_runtime_quiesced=True,
            local_quiescence_protocol="agent_attach_not_started_v1",
            workspace_generation="cccccccc-cccc-4ccc-8ccc-cccccccccccc",
            workspace_runtime_incarnation=("dddddddd-dddd-4ddd-8ddd-dddddddddddd"),
        )
        assert client._client.post.await_args.kwargs["json"] == {
            "agent_id": "agent-a",
            "session_runtime_generation": RUNTIME_GENERATION,
            "session_runtime_attach_token": RUNTIME_ATTACH_TOKEN,
            "agent_pod_uid": "pod-uid-a",
            "local_runtime_quiesced": True,
            "local_quiescence_protocol": "agent_attach_not_started_v1",
            "workspace_generation": "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
            "workspace_runtime_incarnation": ("dddddddd-dddd-4ddd-8ddd-dddddddddddd"),
        }

    @pytest.mark.asyncio
    async def test_connect_includes_user_id_header_when_user_id_set(self):
        """When the agent is operating on behalf of a known user (e.g. a
        persistent-thread session), the orchestrator client must forward
        X-MCP-User-Id alongside X-Internal-Key so the orchestrator's
        _get_user_from_mcp_headers path can resolve the user. Without
        this header the call is rejected as anonymous with a 401."""
        with patch.dict(os.environ, {"MCP_INTERNAL_KEY": "test-internal-key"}):
            client = OrchestratorClient(
                orchestrator_url="http://localhost:8085",
                pod_ip="10.0.0.5",
                pod_port=8001,
                hostname="test-agent",
                config_name="creator",
                pid=12345,
                user_id="user-abc",
            )
            await client.connect()
            try:
                assert (
                    client._client.headers.get("X-Internal-Key") == "test-internal-key"
                )
                assert client._client.headers.get("X-MCP-User-Id") == "user-abc"
            finally:
                await client.close()

    @pytest.mark.asyncio
    async def test_connect_omits_user_id_header_when_not_set(self):
        """Worker-mode clients (no user context) must continue to send
        only X-Internal-Key. Adding an empty X-MCP-User-Id would still
        fail the orchestrator's "if mcp_user_id and …" guard but it
        could mask real bugs in tracing — keep the header absent."""
        with patch.dict(os.environ, {"MCP_INTERNAL_KEY": "test-internal-key"}):
            client = OrchestratorClient(
                orchestrator_url="http://localhost:8085",
                pod_ip="10.0.0.5",
                pod_port=8001,
                hostname="test-agent",
                config_name="creator",
                pid=12345,
            )
            await client.connect()
            try:
                assert (
                    client._client.headers.get("X-Internal-Key") == "test-internal-key"
                )
                assert "X-MCP-User-Id" not in client._client.headers
            finally:
                await client.close()

    @pytest.mark.asyncio
    async def test_register_success(self, client):
        """Test successful registration."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "agent_id": "agent-123",
            "heartbeat_interval_seconds": 60,
            "dispatch_process_generation": "process-123",
        }

        with patch.object(client, "_client", AsyncMock()) as mock_client:
            mock_client.post = AsyncMock(return_value=mock_response)

            result = await client.register()

            assert result is True
            assert client.agent_id == "agent-123"
            assert client.dispatch_process_generation == "process-123"
            assert client.heartbeat_interval == 60
            mock_client.post.assert_called_once()

    @pytest.mark.asyncio
    async def test_thread_registration_echoes_and_adopts_exact_runtime_generation(
        self, client
    ):
        response = MagicMock(status_code=200)
        response.json.return_value = {
            "agent_id": "agent-123",
            "heartbeat_interval_seconds": 60,
            "pinned_runtime_generation_contract": 1,
            "session_runtime_generation": RUNTIME_GENERATION,
            "session_runtime_attach_token": RUNTIME_ATTACH_TOKEN,
        }
        client._client = MagicMock()
        client._client.post = AsyncMock(return_value=response)

        with patch.dict(
            os.environ,
            {"SESSION_RUNTIME_GENERATION": RUNTIME_GENERATION},
        ):
            assert await client.register(agent_mode="persistent", thread_id="thread-a")

        body = client._client.post.await_args.kwargs["json"]
        assert body["session_runtime_generation"] == RUNTIME_GENERATION
        assert client.session_runtime_generation == RUNTIME_GENERATION
        assert client.session_runtime_attach_token == RUNTIME_ATTACH_TOKEN
        assert client.pinned_runtime_generation_contract is True

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "generation",
        [None, "not-a-uuid", "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"],
    )
    async def test_advertised_thread_registration_rejects_missing_malformed_or_mismatch(
        self, client, generation
    ):
        response = MagicMock(status_code=200)
        response.json.return_value = {
            "agent_id": "agent-123",
            "heartbeat_interval_seconds": 60,
            "pinned_runtime_generation_contract": 1,
            "session_runtime_generation": generation,
            "session_runtime_attach_token": RUNTIME_ATTACH_TOKEN,
        }
        client._client = MagicMock()
        client._client.post = AsyncMock(return_value=response)

        with patch.dict(
            os.environ,
            {"SESSION_RUNTIME_GENERATION": RUNTIME_GENERATION},
        ):
            assert not await client.register(
                agent_mode="persistent", thread_id="thread-a"
            )
        assert client.agent_id is None

    @pytest.mark.asyncio
    @pytest.mark.parametrize("attach_token", [None, "not-a-uuid", True, 1])
    async def test_advertised_thread_registration_rejects_missing_or_malformed_token(
        self, client, attach_token
    ):
        response = MagicMock(status_code=200)
        response.json.return_value = {
            "agent_id": "agent-123",
            "heartbeat_interval_seconds": 60,
            "pinned_runtime_generation_contract": 1,
            "session_runtime_generation": RUNTIME_GENERATION,
            "session_runtime_attach_token": attach_token,
        }
        client._client = MagicMock()
        client._client.post = AsyncMock(return_value=response)

        with patch.dict(
            os.environ,
            {"SESSION_RUNTIME_GENERATION": RUNTIME_GENERATION},
        ):
            assert not await client.register(
                agent_mode="persistent", thread_id="thread-a"
            )
        assert client.agent_id is None

    @pytest.mark.asyncio
    async def test_register_failure(self, client):
        """Test registration failure is handled gracefully."""
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"

        with patch.object(client, "_client", AsyncMock()) as mock_client:
            mock_client.post = AsyncMock(return_value=mock_response)

            result = await client.register()

            assert result is False
            assert client.agent_id is None

    @pytest.mark.asyncio
    async def test_register_connection_error(self, client):
        """Test registration handles connection errors gracefully."""
        import httpx

        with patch.object(client, "_client", AsyncMock()) as mock_client:
            mock_client.post = AsyncMock(
                side_effect=httpx.RequestError("Connection refused")
            )

            result = await client.register()

            assert result is False

    @pytest.mark.asyncio
    async def test_heartbeat_success(self, client):
        """Test successful heartbeat returns the response body.

        Phase 1c: heartbeat returns the JSON response (carries
        ``intents``) so callers can react to drain/upgrade hints; the
        old ``bool`` API is gone. Truthy semantics still work for code
        that only cares about success.
        """
        client.agent_id = "agent-123"

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json = MagicMock(return_value={"status": "ok", "intents": {}})

        with patch.object(client, "_client", AsyncMock()) as mock_client:
            mock_client.post = AsyncMock(return_value=mock_response)

            result = await client.heartbeat(
                status="ready",
                job_id=None,
                metrics={"memory_mb": 512, "cpu_percent": 25.5},
            )

            assert result == {"status": "ok", "intents": {}}
            mock_client.post.assert_called_once()

    @pytest.mark.asyncio
    async def test_heartbeat_payload_includes_graph_progress_metric(self, client):
        """Graph-progress heartbeat metrics should be forwarded untouched."""
        client.agent_id = "agent-123"

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json = MagicMock(return_value={"status": "ok", "intents": {}})

        with patch.object(client, "_client", AsyncMock()) as mock_client:
            mock_client.post = AsyncMock(return_value=mock_response)

            result = await client.heartbeat(
                status="working",
                job_id="job-7",
                metrics={"graph_progress": 9, "memory_mb": 512},
            )

            assert result == {"status": "ok", "intents": {}}
            mock_client.post.assert_called_once()
            payload = mock_client.post.call_args.kwargs["json"]
            assert payload["status"] == "working"
            assert payload["current_job_id"] == "job-7"
            assert payload["metrics"]["graph_progress"] == 9

    @pytest.mark.asyncio
    async def test_pinned_heartbeat_echoes_runtime_identity(self, client):
        client.agent_id = "agent-123"
        client.session_runtime_generation = RUNTIME_GENERATION
        client.session_runtime_attach_token = RUNTIME_ATTACH_TOKEN
        response = MagicMock(status_code=200)
        response.json.return_value = {"status": "ok"}
        client._client = MagicMock()
        client._client.post = AsyncMock(return_value=response)

        await client.heartbeat(status="session")

        assert client._client.post.await_args.kwargs["json"] == {
            "status": "session",
            "session_runtime_generation": RUNTIME_GENERATION,
            "session_runtime_attach_token": RUNTIME_ATTACH_TOKEN,
        }

    @pytest.mark.asyncio
    async def test_pinned_status_echoes_exact_runtime_identity(self, client):
        response = MagicMock(status_code=200)
        client._client = MagicMock()
        client._client.put = AsyncMock(return_value=response)
        client.pinned_runtime_generation_contract = True
        client.dispatch_process_generation = PROCESS_GENERATION
        client.session_runtime_generation = RUNTIME_GENERATION
        client.session_runtime_attach_token = RUNTIME_ATTACH_TOKEN

        assert await client.update_thread_status(
            "thread-a",
            "active",
            pinned_agent_id="agent-123",
        )

        assert client._client.put.await_args.kwargs["json"] == {
            "status": "active",
            "agent_id": "agent-123",
            "process_generation": PROCESS_GENERATION,
            "session_runtime_generation": RUNTIME_GENERATION,
            "session_runtime_attach_token": RUNTIME_ATTACH_TOKEN,
        }

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", ["ending", "ended"])
    @pytest.mark.parametrize("disposition", ["ended", "suspended"])
    async def test_pinned_terminal_status_echoes_exact_retirement_disposition(
        self, client, status, disposition
    ):
        response = MagicMock(status_code=200)
        client._client = MagicMock()
        client._client.put = AsyncMock(return_value=response)
        client.pinned_runtime_generation_contract = True
        client.dispatch_process_generation = PROCESS_GENERATION
        client.session_runtime_generation = RUNTIME_GENERATION
        client.session_runtime_attach_token = RUNTIME_ATTACH_TOKEN

        assert await client.update_thread_status(
            "thread-a",
            status,
            pinned_agent_id="agent-123",
            retirement_disposition=disposition,
        )

        assert client._client.put.await_args.kwargs["json"] == {
            "status": status,
            "retirement_disposition": disposition,
            "agent_id": "agent-123",
            "process_generation": PROCESS_GENERATION,
            "session_runtime_generation": RUNTIME_GENERATION,
            "session_runtime_attach_token": RUNTIME_ATTACH_TOKEN,
        }

    @pytest.mark.asyncio
    async def test_exact_retirement_outcome_requires_matching_append_only_receipt(
        self, client
    ):
        response = MagicMock(status_code=200)
        response.json.return_value = {
            "status": "settled_or_superseded",
            "retirement_disposition": "ended",
            "retirement_permanent": False,
            "outcome": "settled",
            "settled_at": "2026-08-26T12:00:00+00:00",
        }
        client._client = MagicMock()
        client._client.get = AsyncMock(return_value=response)

        outcome = await client.get_thread_retirement_outcome(
            "thread-a",
            pinned_agent_id="agent-123",
            session_runtime_generation=RUNTIME_GENERATION,
            session_runtime_attach_token=RUNTIME_ATTACH_TOKEN,
            session_runtime_retirement_token=RUNTIME_RETIREMENT_TOKEN,
            retirement_disposition="ended",
            retirement_permanent=False,
        )

        assert outcome == response.json.return_value
        client._client.get.assert_awaited_once_with(
            "http://localhost:8085/api/agents/threads/thread-a/retirement-outcome",
            headers={
                "X-Agent-ID": "agent-123",
                "X-Session-Runtime-Generation": RUNTIME_GENERATION,
                "X-Session-Runtime-Attach-Token": RUNTIME_ATTACH_TOKEN,
                "X-Session-Runtime-Retirement-Token": RUNTIME_RETIREMENT_TOKEN,
                "X-Retirement-Disposition": "ended",
                "X-Retirement-Permanent": "false",
            },
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "payload",
        [
            {"status": "settled_or_superseded"},
            {
                "status": "settled_or_superseded",
                "retirement_disposition": "suspended",
                "retirement_permanent": False,
                "outcome": "settled",
                "settled_at": "2026-08-26T12:00:00+00:00",
            },
            {
                "status": "settled_or_superseded",
                "retirement_disposition": "ended",
                "retirement_permanent": False,
                "outcome": "unknown",
                "settled_at": "2026-08-26T12:00:00+00:00",
            },
        ],
    )
    async def test_retirement_outcome_fails_closed_on_malformed_or_wrong_receipt(
        self, client, payload
    ):
        response = MagicMock(status_code=200)
        response.json.return_value = payload
        client._client = MagicMock()
        client._client.get = AsyncMock(return_value=response)

        assert (
            await client.get_thread_retirement_outcome(
                "thread-a",
                pinned_agent_id="agent-123",
                session_runtime_generation=RUNTIME_GENERATION,
                session_runtime_attach_token=RUNTIME_ATTACH_TOKEN,
                session_runtime_retirement_token=RUNTIME_RETIREMENT_TOKEN,
                retirement_disposition="ended",
                retirement_permanent=False,
            )
            is None
        )

    @pytest.mark.asyncio
    async def test_generic_200_does_not_prove_exact_local_quiescence(self, client):
        response = MagicMock(status_code=200)
        response.json.return_value = {"status": "ending"}
        client._client = MagicMock()
        client._client.put = AsyncMock(return_value=response)
        client.pinned_runtime_generation_contract = True
        client.dispatch_process_generation = PROCESS_GENERATION

        assert not await client.update_thread_status(
            "thread-a",
            "ended",
            pinned_agent_id="agent-123",
            session_runtime_generation=RUNTIME_GENERATION,
            session_runtime_attach_token=RUNTIME_ATTACH_TOKEN,
            retirement_disposition="ended",
            retirement_permanent=False,
            session_runtime_retirement_token=RUNTIME_RETIREMENT_TOKEN,
            local_runtime_quiesced=True,
            local_quiescence_protocol="workspace_process_zero_v1",
            workspace_generation=RUNTIME_GENERATION,
            workspace_runtime_incarnation=RUNTIME_ATTACH_TOKEN,
        )

    @pytest.mark.asyncio
    async def test_exact_permanent_exit_handoff_releases_retiring_agent(self, client):
        response = MagicMock(status_code=200)
        response.json.return_value = {
            "status": "ending",
            "retirement_disposition": "ended",
            "retirement_permanent": True,
            "retiring_agent_exit_authorized": True,
            "session_runtime_retirement_token": RUNTIME_RETIREMENT_TOKEN,
        }
        client._client = MagicMock()
        client._client.put = AsyncMock(return_value=response)
        client.pinned_runtime_generation_contract = True
        client.dispatch_process_generation = PROCESS_GENERATION

        assert await client.update_thread_status(
            "thread-a",
            "ended",
            pinned_agent_id="agent-123",
            session_runtime_generation=RUNTIME_GENERATION,
            session_runtime_attach_token=RUNTIME_ATTACH_TOKEN,
            retirement_disposition="ended",
            retirement_permanent=True,
            session_runtime_retirement_token=RUNTIME_RETIREMENT_TOKEN,
            local_runtime_quiesced=True,
            local_quiescence_protocol="workspace_process_zero_v1",
            workspace_generation=RUNTIME_GENERATION,
            workspace_runtime_incarnation=RUNTIME_ATTACH_TOKEN,
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("retiring_agent_exit_authorized", False),
            ("session_runtime_retirement_token", RUNTIME_ATTACH_TOKEN),
            ("retirement_permanent", False),
        ],
    )
    async def test_permanent_exit_handoff_requires_exact_typed_echo(
        self, client, field, value
    ):
        payload = {
            "status": "ending",
            "retirement_disposition": "ended",
            "retirement_permanent": True,
            "retiring_agent_exit_authorized": True,
            "session_runtime_retirement_token": RUNTIME_RETIREMENT_TOKEN,
        }
        payload[field] = value
        response = MagicMock(status_code=200)
        response.json.return_value = payload
        client._client = MagicMock()
        client._client.put = AsyncMock(return_value=response)
        client.pinned_runtime_generation_contract = True
        client.dispatch_process_generation = PROCESS_GENERATION

        assert not await client.update_thread_status(
            "thread-a",
            "ended",
            pinned_agent_id="agent-123",
            session_runtime_generation=RUNTIME_GENERATION,
            session_runtime_attach_token=RUNTIME_ATTACH_TOKEN,
            retirement_disposition="ended",
            retirement_permanent=True,
            session_runtime_retirement_token=RUNTIME_RETIREMENT_TOKEN,
            local_runtime_quiesced=True,
            local_quiescence_protocol="workspace_process_zero_v1",
            workspace_generation=RUNTIME_GENERATION,
            workspace_runtime_incarnation=RUNTIME_ATTACH_TOKEN,
        )

    @pytest.mark.asyncio
    async def test_suspend_echoes_exact_runtime_identity_headers(self, client):
        response = MagicMock(status_code=200)
        response.json.return_value = {"suspended": True}
        client.agent_id = "agent-123"
        client.session_runtime_generation = RUNTIME_GENERATION
        client.session_runtime_attach_token = RUNTIME_ATTACH_TOKEN
        client._client = MagicMock()
        client._client.post = AsyncMock(return_value=response)

        assert await client.suspend_thread("thread-a")

        client._client.post.assert_awaited_once_with(
            "http://localhost:8085/api/agents/threads/thread-a/suspend",
            timeout=300.0,
            headers={
                "X-Agent-ID": "agent-123",
                "X-Session-Runtime-Generation": RUNTIME_GENERATION,
                "X-Session-Runtime-Attach-Token": RUNTIME_ATTACH_TOKEN,
            },
        )

    @pytest.mark.asyncio
    async def test_heartbeat_returns_intents(self, client):
        """When orchestrator surfaces drain intent, the agent receives it."""
        client.agent_id = "agent-123"

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json = MagicMock(
            return_value={
                "status": "ok",
                "intents": {"should_drain": True, "drain_reason": "stale_image"},
            }
        )

        with patch.object(client, "_client", AsyncMock()) as mock_client:
            mock_client.post = AsyncMock(return_value=mock_response)

            result = await client.heartbeat(status="ready")

            assert result["intents"]["should_drain"] is True
            assert result["intents"]["drain_reason"] == "stale_image"

    @pytest.mark.asyncio
    async def test_heartbeat_without_agent_id(self, client):
        """Heartbeat returns None when agent_id not set."""
        client.agent_id = None

        result = await client.heartbeat(status="ready")

        assert result is None

    @pytest.mark.asyncio
    async def test_deregister_success(self, client):
        """Test successful deregistration."""
        client.agent_id = "agent-123"
        client.dispatch_process_generation = "process-123"

        mock_response = MagicMock()
        mock_response.status_code = 200

        with patch.object(client, "_client", AsyncMock()) as mock_client:
            mock_client.delete = AsyncMock(return_value=mock_response)

            result = await client.deregister()

            assert result is True
            assert client.agent_id is None
            assert client.dispatch_process_generation is None

    @pytest.mark.asyncio
    async def test_deregister_without_agent_id(self, client):
        """Test deregister fails when agent_id not set."""
        client.agent_id = None

        result = await client.deregister()

        assert result is False

    @pytest.mark.asyncio
    async def test_stop_heartbeat(self, client):
        """Test stop_heartbeat sets the event."""
        assert not client._stop_heartbeat.is_set()
        client.stop_heartbeat()
        assert client._stop_heartbeat.is_set()


class TestUpdateThreadConfig:
    """update_thread_config: enriched-dict return, typed 4xx denial, and
    None-on-transient semantics (live_session_settings.md P0.3)."""

    @pytest.fixture
    def client(self):
        return OrchestratorClient(
            orchestrator_url="http://localhost:8085",
            pod_ip="10.0.0.5",
            pod_port=8001,
            hostname="test-agent",
            config_name="creator",
            pid=12345,
        )

    def _response(self, status_code, json_body=None, text=""):
        r = MagicMock()
        r.status_code = status_code
        r.text = text
        if json_body is not None:
            r.json = MagicMock(return_value=json_body)
        else:
            r.json = MagicMock(side_effect=ValueError("no body"))
        return r

    @pytest.mark.asyncio
    async def test_success_returns_enriched_override(self, client):
        enriched = {"llm": {"model": "m", "base_url": "http://x", "api_key": "k"}}
        with patch.object(client, "_client", AsyncMock()) as mock_client:
            mock_client.patch = AsyncMock(
                return_value=self._response(
                    200, {"status": "updated", "config_override": enriched}
                )
            )
            result = await client.update_thread_config(
                "thread-1", {"llm": {"model": "m"}}
            )
        assert result == enriched

    @pytest.mark.asyncio
    async def test_4xx_raises_typed_denial_with_detail(self, client):
        """A 422 grant denial must surface its detail — not collapse to None
        (the old behavior let a denied model swap fall back to a local apply)."""
        with patch.object(client, "_client", AsyncMock()) as mock_client:
            mock_client.patch = AsyncMock(
                return_value=self._response(
                    422, {"detail": "permission_mode 'autonomous' exceeds grants"}
                )
            )
            with pytest.raises(ThreadConfigUpdateDenied) as exc_info:
                await client.update_thread_config(
                    "thread-1", {"interactive": {"permission_mode": "autonomous"}}
                )
        assert exc_info.value.status_code == 422
        assert "exceeds grants" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_4xx_without_json_body_uses_text(self, client):
        with patch.object(client, "_client", AsyncMock()) as mock_client:
            mock_client.patch = AsyncMock(
                return_value=self._response(404, None, text="Thread not found")
            )
            with pytest.raises(ThreadConfigUpdateDenied) as exc_info:
                await client.update_thread_config("thread-x", {"llm": {}})
        assert exc_info.value.status_code == 404
        assert "Thread not found" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_5xx_returns_none_transient(self, client):
        with patch.object(client, "_client", AsyncMock()) as mock_client:
            mock_client.patch = AsyncMock(
                return_value=self._response(500, {"detail": "boom"})
            )
            result = await client.update_thread_config("thread-1", {"llm": {}})
        assert result is None

    @pytest.mark.asyncio
    async def test_network_failure_returns_none(self, client):
        with patch.object(client, "_client", AsyncMock()) as mock_client:
            mock_client.patch = AsyncMock(side_effect=ConnectionError("down"))
            result = await client.update_thread_config("thread-1", {"llm": {}})
        assert result is None

    @pytest.mark.asyncio
    async def test_datasource_ids_ride_the_patch_payload(self, client):
        """Slice B: the desired FULL selection travels as a sibling of
        config_override — including an EMPTY list (detach all), which must
        not be dropped as falsy."""
        with patch.object(client, "_client", AsyncMock()) as mock_client:
            mock_client.patch = AsyncMock(
                return_value=self._response(
                    200, {"status": "updated", "config_override": {}}
                )
            )
            await client.update_thread_config(
                "thread-1", {}, datasource_ids=["ds-a", "ds-b"], snapshot_generation=3
            )
            assert mock_client.patch.call_args.kwargs["json"] == {
                "config_override": {},
                "datasource_ids": ["ds-a", "ds-b"],
                "snapshot_patch_protocol": 1,
                "snapshot_generation": 3,
            }

            await client.update_thread_config("thread-1", {}, datasource_ids=[])
            assert mock_client.patch.call_args.kwargs["json"] == {
                "config_override": {},
                "datasource_ids": [],
                "snapshot_patch_protocol": 1,
                "snapshot_generation": None,
            }

    @pytest.mark.asyncio
    async def test_omitted_datasource_ids_stay_off_the_wire(self, client):
        """None = no datasource change: the key must be absent, or the
        orchestrator would misread every config edit as a full detach."""
        with patch.object(client, "_client", AsyncMock()) as mock_client:
            mock_client.patch = AsyncMock(
                return_value=self._response(
                    200, {"status": "updated", "config_override": {}}
                )
            )
            await client.update_thread_config("thread-1", {"llm": {}})
        assert "datasource_ids" not in mock_client.patch.call_args.kwargs["json"]


class TestRecordVerificationRound:
    """record_verification_round: journal-before-observe durability for the
    critic verdict tools (knowledge-base/knowledge/superpowers/plans/2026-07-27-verification-fail-
    closed.md, Task 5). Unlike the rest of this client, failure must be LOUD —
    every downstream loss path treats a missing verdict as approval, so this
    method raises VerdictRecordingError instead of returning None/False."""

    @pytest.fixture
    def client(self):
        return OrchestratorClient(
            orchestrator_url="http://localhost:8085",
            pod_ip="10.0.0.5",
            pod_port=8001,
            hostname="test-agent",
            config_name="critic",
            pid=12345,
        )

    def _response(self, status_code, json_body=None, text=""):
        r = MagicMock()
        r.status_code = status_code
        r.text = text
        if json_body is not None:
            r.json = MagicMock(return_value=json_body)
        else:
            r.json = MagicMock(side_effect=ValueError("no body"))
        return r

    @pytest.mark.asyncio
    async def test_success_returns_server_response(self, client):
        server_body = {
            "verdict": "returned",
            "round": 2,
            "assigned": [{"id": "F2", "severity": "high"}],
            "open_findings": [{"id": "F1"}, {"id": "F2"}],
        }
        with patch.object(client, "_client", AsyncMock()) as mock_client:
            mock_client.post = AsyncMock(return_value=self._response(200, server_body))
            result = await client.record_verification_round(
                target_job_id="target-1",
                critic_job_id="critic-1",
                asserted_verdict="approved",
                opened=[{"claim": "x", "severity": "high"}],
                dispositions=[{"id": "F1", "disposition": "STILL_OPEN"}],
                head_commit="abc123",
            )
        assert result == server_body

    @pytest.mark.asyncio
    async def test_posts_to_the_target_job_verification_rounds_url(self, client):
        with patch.object(client, "_client", AsyncMock()) as mock_client:
            mock_client.post = AsyncMock(
                return_value=self._response(
                    200,
                    {
                        "verdict": "approved",
                        "round": 1,
                        "assigned": [],
                        "open_findings": [],
                    },
                )
            )
            await client.record_verification_round(
                target_job_id="target-1",
                critic_job_id="critic-1",
                asserted_verdict="approved",
                opened=[],
                dispositions=[],
            )
        call = mock_client.post.call_args
        assert call.args[0] == (
            "http://localhost:8085/api/jobs/target-1/verification/rounds"
        )
        assert call.kwargs["json"] == {
            "critic_job_id": "critic-1",
            "asserted_verdict": "approved",
            "opened": [],
            "dispositions": [],
            "head_commit": None,
            "content_tree": None,
        }

    @pytest.mark.asyncio
    async def test_409_with_errors_list_raises_with_errors_verbatim(self, client):
        """The errors list is model-facing — it must survive into the
        exception message so the model can correct itself."""
        with patch.object(client, "_client", AsyncMock()) as mock_client:
            mock_client.post = AsyncMock(
                return_value=self._response(
                    409,
                    {
                        "detail": {
                            "errors": [
                                "F1: no disposition supplied.",
                                "Cannot return a job with no findings.",
                            ]
                        }
                    },
                )
            )
            with pytest.raises(VerdictRecordingError) as exc_info:
                await client.record_verification_round(
                    target_job_id="target-1",
                    critic_job_id="critic-1",
                    asserted_verdict="returned",
                    opened=[],
                    dispositions=[],
                )
        assert "F1: no disposition supplied." in str(exc_info.value)
        assert "Cannot return a job with no findings." in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_409_with_non_dict_detail_falls_back_to_str(self, client):
        with patch.object(client, "_client", AsyncMock()) as mock_client:
            mock_client.post = AsyncMock(
                return_value=self._response(409, {"detail": "malformed request"})
            )
            with pytest.raises(VerdictRecordingError) as exc_info:
                await client.record_verification_round(
                    target_job_id="target-1",
                    critic_job_id="critic-1",
                    asserted_verdict="approved",
                    opened=[],
                    dispositions=[],
                )
        assert "malformed request" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_other_http_error_raises_with_status_and_body(self, client):
        with patch.object(client, "_client", AsyncMock()) as mock_client:
            mock_client.post = AsyncMock(
                return_value=self._response(500, text="internal error")
            )
            with pytest.raises(VerdictRecordingError) as exc_info:
                await client.record_verification_round(
                    target_job_id="target-1",
                    critic_job_id="critic-1",
                    asserted_verdict="approved",
                    opened=[],
                    dispositions=[],
                )
        assert "500" in str(exc_info.value)
        assert "internal error" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_network_failure_raises_not_none(self, client):
        """Unlike the rest of this client's best-effort methods, a network
        failure here must not collapse to None — that would let the caller
        treat an unrecorded verdict as recorded."""
        import httpx

        with patch.object(client, "_client", AsyncMock()) as mock_client:
            mock_client.post = AsyncMock(
                side_effect=httpx.ConnectError("connection refused")
            )
            with pytest.raises(VerdictRecordingError) as exc_info:
                await client.record_verification_round(
                    target_job_id="target-1",
                    critic_job_id="critic-1",
                    asserted_verdict="approved",
                    opened=[],
                    dispositions=[],
                )
        assert "network error" in str(exc_info.value).lower()


class TestCreateOrchestratorClientFromEnv:
    """Tests for create_orchestrator_client_from_env function."""

    def test_returns_client_with_default_url(self):
        """Test returns client with default URL when ORCHESTRATOR_URL not set."""
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("ORCHESTRATOR_URL", None)
            client = create_orchestrator_client_from_env("creator")
            assert client is not None
            assert client.orchestrator_url == "http://localhost:8085"

    def test_creates_client_with_url(self):
        """Test creates client when ORCHESTRATOR_URL is set."""
        with patch.dict(
            os.environ,
            {
                "ORCHESTRATOR_URL": "http://localhost:8085",
                "AGENT_POD_IP": "10.0.0.5",
                "AGENT_POD_PORT": "8001",
                "AGENT_HOSTNAME": "test-agent",
            },
        ):
            client = create_orchestrator_client_from_env("creator")

            assert client is not None
            assert client.orchestrator_url == "http://localhost:8085"
            assert client.pod_ip == "10.0.0.5"
            assert client.pod_port == 8001
            assert client.hostname == "test-agent"
            assert client.config_name == "creator"

    def test_uses_defaults_for_optional_vars(self):
        """Test uses defaults when optional env vars not set."""
        with patch.dict(os.environ, {"ORCHESTRATOR_URL": "http://localhost:8085"}):
            os.environ.pop("AGENT_POD_IP", None)
            os.environ.pop("AGENT_POD_PORT", None)
            os.environ.pop("AGENT_HOSTNAME", None)

            client = create_orchestrator_client_from_env("validator")

            assert client is not None
            assert client.pod_port == 8001  # Default
            assert client.pod_ip is not None  # Auto-detected
            assert client.hostname is not None  # Auto-detected


class TestTriggerSubjobMerge:
    """Tests for trigger_subjob_merge method."""

    @pytest.fixture
    def client(self):
        """Create a test client instance."""
        return OrchestratorClient(
            orchestrator_url="http://localhost:8085",
            pod_ip="10.0.0.5",
            pod_port=8001,
            hostname="test-agent",
            config_name="creator",
            pid=12345,
        )

    @pytest.mark.asyncio
    async def test_trigger_subjob_merge_success(self, client):
        """Test successful subjob merge trigger."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"status": "merged", "pr_number": 42}

        with patch.object(client, "_client", AsyncMock()) as mock_client:
            mock_client.post = AsyncMock(return_value=mock_response)

            result = await client.trigger_subjob_merge("subjob-uuid")

            assert result is True
            mock_client.post.assert_called_once_with(
                "http://localhost:8085/api/jobs/subjob-uuid/subjob-merge"
            )

    @pytest.mark.asyncio
    async def test_trigger_subjob_merge_failure(self, client):
        """Test merge trigger returns False on non-200 response."""
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"

        with patch.object(client, "_client", AsyncMock()) as mock_client:
            mock_client.post = AsyncMock(return_value=mock_response)

            result = await client.trigger_subjob_merge("subjob-uuid")

            assert result is False

    @pytest.mark.asyncio
    async def test_trigger_subjob_merge_connection_error(self, client):
        """Test merge trigger handles connection errors."""
        import httpx

        with patch.object(client, "_client", AsyncMock()) as mock_client:
            mock_client.post = AsyncMock(
                side_effect=httpx.RequestError("Connection refused")
            )

            result = await client.trigger_subjob_merge("subjob-uuid")

            assert result is False

    @pytest.mark.asyncio
    async def test_trigger_subjob_merge_connects_if_needed(self, client):
        """Test that trigger_subjob_merge auto-connects when _client is None."""
        client._client = None

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"status": "merged"}

        with patch.object(client, "connect", AsyncMock()) as mock_connect:
            # After connect, _client should be set
            async def set_client():
                mock_http = AsyncMock()
                mock_http.post = AsyncMock(return_value=mock_response)
                client._client = mock_http

            mock_connect.side_effect = set_client

            result = await client.trigger_subjob_merge("subjob-uuid")

            assert result is True
            mock_connect.assert_awaited_once()


# ---------------------------------------------------------------------------
# Section 4: New persistent-thread methods
# ---------------------------------------------------------------------------


class TestCreateThread:
    """Tests for create_thread method."""

    @pytest.fixture
    def client(self):
        return OrchestratorClient(
            orchestrator_url="http://localhost:8085",
            pod_ip="10.0.0.5",
            pod_port=8001,
            hostname="test-agent",
            config_name="persistent_defaults",
            pid=12345,
        )

    @pytest.mark.asyncio
    async def test_posts_correct_url_and_payload(self, client):
        """POSTs to /api/agents/threads with config_name, permission_mode, title."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"thread_id": "tid-1"}

        with patch.object(client, "_client", AsyncMock()) as mock_http:
            mock_http.post = AsyncMock(return_value=mock_response)
            await client.create_thread(
                "persistent_defaults", "supervised", "My Session"
            )

            mock_http.post.assert_called_once_with(
                "http://localhost:8085/api/agents/threads",
                json={
                    "config_name": "persistent_defaults",
                    "permission_mode": "supervised",
                    "title": "My Session",
                },
            )

    @pytest.mark.asyncio
    async def test_returns_thread_id_on_200(self, client):
        """Returns thread_id string from response JSON on 200."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"thread_id": "abc-123"}

        with patch.object(client, "_client", AsyncMock()) as mock_http:
            mock_http.post = AsyncMock(return_value=mock_response)
            result = await client.create_thread()
            assert result == "abc-123"

    @pytest.mark.asyncio
    async def test_returns_none_on_non_200(self, client):
        """Returns None on non-200 status."""
        mock_response = MagicMock()
        mock_response.status_code = 422
        mock_response.text = "Validation error"

        with patch.object(client, "_client", AsyncMock()) as mock_http:
            mock_http.post = AsyncMock(return_value=mock_response)
            result = await client.create_thread()
            assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_on_exception(self, client):
        """Returns None on request exception."""
        with patch.object(client, "_client", AsyncMock()) as mock_http:
            mock_http.post = AsyncMock(side_effect=Exception("network down"))
            result = await client.create_thread()
            assert result is None

    @pytest.mark.asyncio
    async def test_auto_connects_if_client_none(self, client):
        """Calls connect() if _client not initialized."""
        client._client = None
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"thread_id": "tid-2"}

        with patch.object(client, "connect", AsyncMock()) as mock_connect:

            async def set_client():
                mock_http = AsyncMock()
                mock_http.post = AsyncMock(return_value=mock_response)
                client._client = mock_http

            mock_connect.side_effect = set_client
            result = await client.create_thread()
            assert result == "tid-2"
            mock_connect.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_uses_default_args(self, client):
        """Default args use the canonical session framework base."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"thread_id": "tid-3"}

        with patch.object(client, "_client", AsyncMock()) as mock_http:
            mock_http.post = AsyncMock(return_value=mock_response)
            await client.create_thread()

            call_payload = mock_http.post.call_args[1]["json"]
            assert call_payload["config_name"] == "session_base"
            assert call_payload["permission_mode"] == "supervised"
            assert call_payload["title"] == "Local Session"


class TestSubagentGenerationClient:
    CHILD = "11111111-1111-4111-8111-111111111111"
    JOB = "22222222-2222-4222-8222-222222222222"
    DELIVERY = "33333333-3333-4333-8333-333333333333"
    AGENT = "44444444-4444-4444-8444-444444444444"

    @property
    def authority(self):
        return ParentExecutionAuthority(
            execution_lane="pinned",
            parent_job_id=self.JOB,
            agent_id=self.AGENT,
            pod_uid="pod-test",
            dispatch_process_generation="process-test",
        )

    @pytest.fixture
    def client(self):
        return OrchestratorClient(
            orchestrator_url="http://localhost:8085",
            pod_ip="10.0.0.5",
            pod_port=8001,
            hostname="test-agent",
            config_name="creator",
            pid=12345,
        )

    @pytest.mark.asyncio
    async def test_create_returns_only_a_complete_generation_lease(self, client):
        response = MagicMock(status_code=200)
        response.json.return_value = {
            "thread_id": self.CHILD,
            "runtime_generation": RUNTIME_GENERATION,
        }
        client._client = MagicMock(post=AsyncMock(return_value=response))

        result = await client.create_subagent_thread(
            self.JOB,
            parent_authority=self.authority,
            subagent_id=self.CHILD,
            handle="explorer-7f3a",
            subagent_type="explorer",
            initial_status="queued",
        )
        assert result == {
            "thread_id": self.CHILD,
            "runtime_generation": RUNTIME_GENERATION,
        }
        payload = client._client.post.await_args.kwargs["json"]
        assert payload["initial_status"] == "queued"

        response.json.return_value = {"thread_id": self.CHILD}
        assert (
            await client.create_subagent_thread(
                self.JOB,
                parent_authority=self.authority,
                handle="explorer-7f3a",
                subagent_type="explorer",
            )
            is None
        )

        response.json.return_value = {
            "thread_id": "99999999-9999-4999-8999-999999999999",
            "runtime_generation": RUNTIME_GENERATION,
        }
        assert (
            await client.create_subagent_thread(
                self.JOB,
                parent_authority=self.authority,
                subagent_id=self.CHILD,
                handle="explorer-7f3a",
                subagent_type="explorer",
            )
            is None
        )

    @pytest.mark.asyncio
    async def test_background_create_refusal_is_not_downgraded_to_no_row(self, client):
        stale = MagicMock(status_code=409)
        stale.json.return_value = {
            "detail": {
                "code": ParentExecutionAuthorityRefused.code,
                "reason": "pinned_process_not_current",
            }
        }
        client._client = MagicMock(post=AsyncMock(return_value=stale))

        with pytest.raises(ParentExecutionAuthorityRefused):
            await client.create_subagent_thread(
                self.JOB,
                parent_authority=self.authority,
                handle="explorer-7f3a",
                subagent_type="explorer",
                run_in_background=True,
                initial_status="queued",
            )

        malformed = MagicMock(status_code=200)
        malformed.json.return_value = {"thread_id": self.CHILD}
        client._client.post.return_value = malformed
        with pytest.raises(SubagentPersistenceError):
            await client.create_subagent_thread(
                self.JOB,
                parent_authority=self.authority,
                handle="explorer-7f3a",
                subagent_type="explorer",
                run_in_background=True,
                initial_status="queued",
            )

        mismatched = MagicMock(status_code=200)
        mismatched.json.return_value = {
            "thread_id": "99999999-9999-4999-8999-999999999999",
            "runtime_generation": RUNTIME_GENERATION,
        }
        client._client.post.return_value = mismatched
        with pytest.raises(SubagentPersistenceError):
            await client.create_subagent_thread(
                self.JOB,
                parent_authority=self.authority,
                subagent_id=self.CHILD,
                handle="explorer-7f3a",
                subagent_type="explorer",
                run_in_background=True,
                initial_status="queued",
            )

    @pytest.mark.asyncio
    async def test_terminal_and_reopen_preserve_conflict_receipts(self, client):
        applied = MagicMock(status_code=200)
        applied.json.return_value = {
            "result": "applied",
            "runtime_generation": RUNTIME_GENERATION,
        }
        stale = MagicMock(status_code=409)
        stale.json.return_value = {
            "detail": {
                "result": "stale",
                "runtime_generation": RUNTIME_GENERATION,
            }
        }
        client._client = MagicMock(post=AsyncMock(side_effect=[applied, stale]))

        result = await client.terminalize_subagent_thread(
            self.JOB,
            self.CHILD,
            parent_authority=self.authority,
            runtime_generation=RUNTIME_GENERATION,
            delivery_id=self.DELIVERY,
            message="child report",
            timestamp="2026-09-01T01:02:03+00:00",
            subagent_status="completed",
        )
        assert result["result"] == "applied"
        terminal_call = client._client.post.await_args_list[0]
        assert terminal_call.args[0].endswith(f"/{self.CHILD}/terminal")
        assert terminal_call.kwargs["json"]["delivery_id"] == self.DELIVERY

        result = await client.reopen_subagent_thread(
            self.JOB,
            self.CHILD,
            parent_authority=self.authority,
            runtime_generation=RUNTIME_GENERATION,
        )
        assert result == {
            "result": "stale",
            "runtime_generation": RUNTIME_GENERATION,
        }

    @pytest.mark.asyncio
    async def test_live_list_and_exact_lookup_use_internal_child_paths(self, client):
        live = MagicMock(status_code=200)
        live.json.return_value = {
            "subagents": [
                {"thread_id": self.CHILD, "runtime_generation": RUNTIME_GENERATION}
            ]
        }
        exact = MagicMock(status_code=200)
        exact.json.return_value = {
            "thread_id": self.CHILD,
            "runtime_generation": RUNTIME_GENERATION,
        }
        client._client = MagicMock(post=AsyncMock(side_effect=[live, exact]))

        assert (
            await client.list_live_subagent_threads(
                self.JOB, parent_authority=self.authority
            )
        )[0]["runtime_generation"] == RUNTIME_GENERATION
        assert (
            await client.get_subagent_thread(
                self.JOB, self.CHILD, parent_authority=self.authority
            )
        )["thread_id"] == self.CHILD
        assert (
            client._client.post.await_args_list[0].args[0].endswith("/subagents/live")
        )
        assert (
            client._client.post.await_args_list[1]
            .args[0]
            .endswith(f"/subagents/{self.CHILD}")
        )

    @pytest.mark.asyncio
    async def test_recovery_distinguishes_stale_authority_and_outage_from_empty(
        self, client
    ):
        refused = MagicMock(status_code=409)
        refused.json.return_value = {
            "detail": {
                "code": "parent_execution_authority_refused",
                "reason": "pinned_process_not_current",
            }
        }
        client._client = MagicMock(post=AsyncMock(return_value=refused))
        with pytest.raises(ParentExecutionAuthorityRefused):
            await client.list_live_subagent_threads(
                self.JOB, parent_authority=self.authority
            )

        client._client.post.side_effect = RuntimeError("database unavailable")
        with pytest.raises(SubagentPersistenceError):
            await client.get_subagent_thread(
                self.JOB, self.CHILD, parent_authority=self.authority
            )


class TestSaveThreadMessage:
    """Tests for save_thread_message method."""

    @pytest.fixture
    def client(self):
        return OrchestratorClient(
            orchestrator_url="http://localhost:8085",
            pod_ip="10.0.0.5",
            pod_port=8001,
            hostname="test-agent",
            config_name="persistent_defaults",
            pid=12345,
        )

    @pytest.mark.asyncio
    async def test_posts_correct_url_and_payload(self, client):
        """POSTs to /api/agents/threads/{thread_id}/messages with correct payload."""
        mock_response = MagicMock()
        mock_response.status_code = 200

        with patch.object(client, "_client", AsyncMock()) as mock_http:
            mock_http.post = AsyncMock(return_value=mock_response)
            await client.save_thread_message(
                "tid-1", "assistant", "hello", [{"name": "t"}], 3
            )

            mock_http.post.assert_called_once_with(
                "http://localhost:8085/api/agents/threads/tid-1/messages",
                json={
                    "role": "assistant",
                    "content": "hello",
                    "tool_calls": [{"name": "t"}],
                    "turn_number": 3,
                    "metrics": None,
                    "tool_call_id": None,
                    "thinking": None,
                    "reasoning": None,
                    "tool_results": None,
                    "provider": None,
                    "provider_raw": None,
                    "additional_kwargs": None,
                    "response_metadata": None,
                },
            )

    @pytest.mark.asyncio
    async def test_returns_true_on_200(self, client):
        """Returns True on 200."""
        mock_response = MagicMock()
        mock_response.status_code = 200

        with patch.object(client, "_client", AsyncMock()) as mock_http:
            mock_http.post = AsyncMock(return_value=mock_response)
            result = await client.save_thread_message("tid-1", "user", "hi")
            assert result is True

    @pytest.mark.asyncio
    async def test_returns_false_on_non_200(self, client):
        """Returns False on non-200 status."""
        mock_response = MagicMock()
        mock_response.status_code = 500

        with patch.object(client, "_client", AsyncMock()) as mock_http:
            mock_http.post = AsyncMock(return_value=mock_response)
            result = await client.save_thread_message("tid-1", "user", "hi")
            assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_when_client_none(self, client):
        """Returns False when _client is None — does NOT auto-connect."""
        client._client = None
        result = await client.save_thread_message("tid-1", "user", "hi")
        assert result is False

    @pytest.mark.asyncio
    async def test_does_not_auto_connect(self, client):
        """Does NOT call connect() — fire-and-forget safe means no lazy init."""
        client._client = None
        with patch.object(client, "connect", AsyncMock()) as mock_connect:
            await client.save_thread_message("tid-1", "user", "hi")
            mock_connect.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_returns_false_on_exception(self, client):
        """Exception returns False — fire-and-forget safe."""
        with patch.object(client, "_client", AsyncMock()) as mock_http:
            mock_http.post = AsyncMock(side_effect=RuntimeError("boom"))
            result = await client.save_thread_message("tid-1", "user", "hi")
            assert result is False

    @pytest.mark.asyncio
    async def test_optional_fields_sent_as_none(self, client):
        """Optional content/tool_calls/turn_number default to None in payload."""
        mock_response = MagicMock()
        mock_response.status_code = 200

        with patch.object(client, "_client", AsyncMock()) as mock_http:
            mock_http.post = AsyncMock(return_value=mock_response)
            await client.save_thread_message("tid-1", "assistant")

            call_payload = mock_http.post.call_args[1]["json"]
            assert call_payload["content"] is None
            assert call_payload["tool_calls"] is None
            assert call_payload["turn_number"] is None


class TestRequestThreadVmUpgrade:
    """Tests for request_thread_vm_upgrade method."""

    @pytest.fixture
    def client(self):
        return OrchestratorClient(
            orchestrator_url="http://localhost:8085",
            pod_ip="10.0.0.5",
            pod_port=8001,
            hostname="test-agent",
            config_name="persistent_defaults",
            pid=12345,
        )

    @pytest.mark.asyncio
    async def test_posts_correct_url_and_payload(self, client):
        """POSTs to /api/agents/threads/{thread_id}/upgrade-to-vm with cpu/memory."""
        mock_response = MagicMock()
        mock_response.status_code = 200

        with patch.object(client, "_client", AsyncMock()) as mock_http:
            mock_http.post = AsyncMock(return_value=mock_response)
            await client.request_thread_vm_upgrade("tid-1", cpu_cores=4, memory="8Gi")

            mock_http.post.assert_called_once_with(
                "http://localhost:8085/api/agents/threads/tid-1/upgrade-to-vm",
                json={"cpu_cores": 4, "memory": "8Gi"},
            )

    @pytest.mark.asyncio
    async def test_returns_true_on_200(self, client):
        """Returns True on 200."""
        mock_response = MagicMock()
        mock_response.status_code = 200

        with patch.object(client, "_client", AsyncMock()) as mock_http:
            mock_http.post = AsyncMock(return_value=mock_response)
            result = await client.request_thread_vm_upgrade("tid-1")
            assert result is True

    @pytest.mark.asyncio
    async def test_returns_false_on_non_200(self, client):
        """Returns False on non-200 status."""
        mock_response = MagicMock()
        mock_response.status_code = 503
        mock_response.text = "Service Unavailable"

        with patch.object(client, "_client", AsyncMock()) as mock_http:
            mock_http.post = AsyncMock(return_value=mock_response)
            result = await client.request_thread_vm_upgrade("tid-1")
            assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_on_exception(self, client):
        """Returns False on request exception."""
        with patch.object(client, "_client", AsyncMock()) as mock_http:
            mock_http.post = AsyncMock(side_effect=ConnectionError("refused"))
            result = await client.request_thread_vm_upgrade("tid-1")
            assert result is False

    @pytest.mark.asyncio
    async def test_auto_connects_if_client_none(self, client):
        """Calls connect() if _client not initialized."""
        client._client = None
        mock_response = MagicMock()
        mock_response.status_code = 200

        with patch.object(client, "connect", AsyncMock()) as mock_connect:

            async def set_client():
                mock_http = AsyncMock()
                mock_http.post = AsyncMock(return_value=mock_response)
                client._client = mock_http

            mock_connect.side_effect = set_client
            result = await client.request_thread_vm_upgrade("tid-1")
            assert result is True
            mock_connect.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_default_cpu_and_memory(self, client):
        """Default: cpu_cores=8, memory='16Gi'."""
        mock_response = MagicMock()
        mock_response.status_code = 200

        with patch.object(client, "_client", AsyncMock()) as mock_http:
            mock_http.post = AsyncMock(return_value=mock_response)
            await client.request_thread_vm_upgrade("tid-1")

            call_payload = mock_http.post.call_args[1]["json"]
            assert call_payload["cpu_cores"] == 8
            assert call_payload["memory"] == "16Gi"


class TestGetThreadWorkspace:
    """Tests for get_thread_workspace method."""

    @pytest.fixture
    def client(self):
        return OrchestratorClient(
            orchestrator_url="http://localhost:8085",
            pod_ip="10.0.0.5",
            pod_port=8001,
            hostname="test-agent",
            config_name="persistent_defaults",
            pid=12345,
        )

    @pytest.mark.asyncio
    async def test_gets_correct_url(self, client):
        """GETs /api/agents/threads/{thread_id}/workspace."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"status": "running"}

        with patch.object(client, "_client", AsyncMock()) as mock_http:
            mock_http.get = AsyncMock(return_value=mock_response)
            await client.get_thread_workspace("tid-1")

            # Every poll says this agent attaches to sidecar cloud folders
            # (D7), so the orchestrator never takes it for an older one.
            mock_http.get.assert_called_once_with(
                "http://localhost:8085/api/agents/threads/tid-1/workspace",
                headers={"X-SRW-Cloud-Mount-Delivery": "sidecar"},
            )

    @pytest.mark.asyncio
    async def test_sends_exact_agent_identity_on_workspace_credential_reads(
        self, client
    ):
        mock_response = MagicMock(status_code=200)
        mock_response.json.return_value = {"status": "creating"}
        client.agent_id = "00000000-0000-0000-0000-0000000000a1"
        client.session_runtime_generation = RUNTIME_GENERATION
        client.session_runtime_attach_token = RUNTIME_ATTACH_TOKEN

        with patch.object(client, "_client", AsyncMock()) as mock_http:
            mock_http.get = AsyncMock(return_value=mock_response)
            await client.get_thread_workspace("tid-1")

        mock_http.get.assert_awaited_once_with(
            "http://localhost:8085/api/agents/threads/tid-1/workspace",
            headers={
                "X-SRW-Cloud-Mount-Delivery": "sidecar",
                "X-Agent-ID": client.agent_id,
                "X-Session-Runtime-Generation": RUNTIME_GENERATION,
                "X-Session-Runtime-Attach-Token": RUNTIME_ATTACH_TOKEN,
            },
        )

    @pytest.mark.asyncio
    async def test_reports_sidecar_cloud_mount_state(self, client):
        mock_response = MagicMock(status_code=200)
        with patch.object(client, "_client", AsyncMock()) as mock_http:
            mock_http.post = AsyncMock(return_value=mock_response)
            assert await client.report_cloud_mount_status(
                "tid-1",
                fingerprint="f" * 64,
                pod_uid="pod-1",
                mounts=[{"name": "project", "state": "mounted"}],
            )
            mock_http.post.side_effect = RuntimeError("down")
            assert not await client.report_cloud_mount_status(
                "tid-1", fingerprint="f" * 64, pod_uid="pod-1", mounts=[]
            )
        url, kwargs = mock_http.post.await_args_list[0]
        assert url == (
            "http://localhost:8085/api/agents/threads/tid-1/cloud-mount-status",
        )
        assert kwargs["json"] == {
            "fingerprint": "f" * 64,
            "pod_uid": "pod-1",
            "mounts": [{"name": "project", "state": "mounted"}],
        }

    @pytest.mark.asyncio
    async def test_returns_parsed_json_on_200(self, client):
        """Returns parsed JSON dict on 200."""
        workspace_data = {
            "status": "running",
            "pod_ip": "10.0.0.99",
            "pod_name": "ws-pod-1",
            "namespace": "agent-vms",
        }
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = workspace_data

        with patch.object(client, "_client", AsyncMock()) as mock_http:
            mock_http.get = AsyncMock(return_value=mock_response)
            result = await client.get_thread_workspace("tid-1")
            assert result == workspace_data

    @pytest.mark.asyncio
    async def test_returns_none_on_non_200(self, client):
        """Returns None on non-200 status."""
        mock_response = MagicMock()
        mock_response.status_code = 404

        with patch.object(client, "_client", AsyncMock()) as mock_http:
            mock_http.get = AsyncMock(return_value=mock_response)
            result = await client.get_thread_workspace("tid-1")
            assert result is None

    @pytest.mark.asyncio
    async def test_raises_session_grant_denied_on_403_when_flagged(self, client):
        """raise_on_denied=True: a 403 (grant denial) raises SessionGrantDenied
        carrying the violation, so the attach path surfaces the real reason
        instead of misreporting 'no workspace provisioned' (the 5m40s bug).
        docs: session_permission_mode_grant_denied_ready_timeout.md
        """
        mock_response = MagicMock()
        mock_response.status_code = 403
        mock_response.json.return_value = {
            "detail": "config exceeds your capability grants: "
            "permission_mode: 'autonomous' exceeds the ceiling"
        }
        with patch.object(client, "_client", AsyncMock()) as mock_http:
            mock_http.get = AsyncMock(return_value=mock_response)
            with pytest.raises(SessionGrantDenied) as ei:
                await client.get_thread_workspace("tid-1", raise_on_denied=True)
        assert "permission_mode" in str(ei.value)
        assert "autonomous" in str(ei.value)

    @pytest.mark.asyncio
    async def test_returns_none_on_403_by_default(self, client):
        """Without the flag a 403 stays None — the upgrade/VM pollers that share
        get_thread_workspace must not start raising."""
        mock_response = MagicMock()
        mock_response.status_code = 403
        mock_response.json.return_value = {"detail": "denied"}
        with patch.object(client, "_client", AsyncMock()) as mock_http:
            mock_http.get = AsyncMock(return_value=mock_response)
            result = await client.get_thread_workspace("tid-1")
            assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_on_exception(self, client):
        """Returns None on request exception."""
        with patch.object(client, "_client", AsyncMock()) as mock_http:
            mock_http.get = AsyncMock(side_effect=Exception("timeout"))
            result = await client.get_thread_workspace("tid-1")
            assert result is None

    @pytest.mark.asyncio
    async def test_auto_connects_if_client_none(self, client):
        """Calls connect() if _client not initialized."""
        client._client = None
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"status": "pending"}

        with patch.object(client, "connect", AsyncMock()) as mock_connect:

            async def set_client():
                mock_http = AsyncMock()
                mock_http.get = AsyncMock(return_value=mock_response)
                client._client = mock_http

            mock_connect.side_effect = set_client
            result = await client.get_thread_workspace("tid-1")
            assert result == {"status": "pending"}
            mock_connect.assert_awaited_once()


class TestGetThreadLifecycle:
    @pytest.fixture
    def client(self):
        client = OrchestratorClient(
            orchestrator_url="http://localhost:8085",
            pod_ip="10.0.0.5",
            pod_port=8001,
            hostname="test-agent",
            config_name="persistent_defaults",
            pid=12345,
        )
        client.agent_id = "00000000-0000-4000-8000-0000000000a1"
        client.session_runtime_generation = RUNTIME_GENERATION
        client.session_runtime_attach_token = RUNTIME_ATTACH_TOKEN
        return client

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status_code", [403, 404, 409])
    async def test_exact_authority_refusal_terminates_stale_runtime(
        self, client, status_code
    ):
        response = MagicMock(status_code=status_code)
        with patch.object(client, "_client", AsyncMock()) as mock_http:
            mock_http.get = AsyncMock(return_value=response)

            result = await client.get_thread_lifecycle("tid-1")

        assert result == {"status": "runtime_moved", "authority_refused": True}
        mock_http.get.assert_awaited_once_with(
            "http://localhost:8085/api/agents/threads/tid-1/lifecycle",
            headers={
                "X-Agent-ID": client.agent_id,
                "X-Session-Runtime-Generation": RUNTIME_GENERATION,
                "X-Session-Runtime-Attach-Token": RUNTIME_ATTACH_TOKEN,
            },
        )

    @pytest.mark.asyncio
    async def test_transient_server_failure_remains_retryable(self, client):
        response = MagicMock(status_code=503)
        with patch.object(client, "_client", AsyncMock()) as mock_http:
            mock_http.get = AsyncMock(return_value=response)

            assert await client.get_thread_lifecycle("tid-1") is None
