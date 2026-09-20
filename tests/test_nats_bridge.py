"""Tests for the NATS bridge module (orchestrator/services/nats_bridge.py).

Tests cover:
1. NatsBridge initialization and graceful degradation
2. connect() / disconnect() lifecycle management
3. request_vm_create() — message format, payload, timeout handling
4. request_vm_delete() — message format
5. query_vm_status() — request/reply pattern, timeout
7. _on_vm_lifecycle_status() — parsing status messages, context updates
8. _on_daemon_register() — daemon registration with SSH readiness gating
9. _on_daemon_heartbeat() — heartbeat processing, IDE session tracking
11. NATS connection error/disconnect/reconnect callbacks
12. Edge cases: malformed JSON, missing fields, reconnection, publish errors
"""

import asyncio
import json
from unittest.mock import ANY, AsyncMock, MagicMock, patch

import pytest

from orchestrator.services.vm_lifecycle_auth import sign_payload

PROVISION_GENERATION = "00000000-0000-4000-8000-000000000001"


# =============================================================================
# Helpers
# =============================================================================


def make_msg(data: dict) -> MagicMock:
    """Create a mock NATS message with JSON-encoded data."""
    msg = MagicMock()
    msg.data = json.dumps(data).encode()
    msg.subject = "test.subject"
    return msg


def make_msg_raw(raw_bytes: bytes) -> MagicMock:
    """Create a mock NATS message with raw bytes (for malformed JSON tests)."""
    msg = MagicMock()
    msg.data = raw_bytes
    msg.subject = "test.subject"
    return msg


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def mock_nats_module():
    """Patch the nats module globally so NatsBridge thinks nats-py is installed."""
    mock_nats = MagicMock()
    mock_nats.connect = AsyncMock()
    with patch.dict("sys.modules", {"nats": mock_nats, "nats.aio.client": MagicMock()}):
        with patch("orchestrator.services.nats_bridge.NATS_AVAILABLE", True):
            with patch("orchestrator.services.nats_bridge.nats", mock_nats):
                yield mock_nats


@pytest.fixture
def mock_nc():
    """Create a mock NATS client with common methods."""
    nc = AsyncMock()
    nc.subscribe = AsyncMock()
    nc.publish = AsyncMock()
    nc.request = AsyncMock()
    nc.drain = AsyncMock()
    return nc


@pytest.fixture
def mock_db():
    """Create a mock database instance."""
    db = AsyncMock()
    db.merge_vm_context = AsyncMock()
    db.merge_thread_vm_context = AsyncMock()
    db.merge_vm_context_if_current = AsyncMock(return_value=True)
    db.merge_thread_vm_context_if_current = AsyncMock(return_value=True)
    db.merge_vm_context_if_provision_generation = AsyncMock(return_value=True)
    db.merge_thread_vm_context_if_provision_generation = AsyncMock(return_value=True)
    db.get_vm_provision_generation = AsyncMock(return_value=PROVISION_GENERATION)
    db.merge_ide_session_context = AsyncMock()
    db.merge_thread_ide_session_context = AsyncMock()
    db.bind_thread_workspace_backing = AsyncMock()
    # VM callbacks resolve thread-vs-job by DB lookup (never process-local
    # state), so these must be explicit: a bare AsyncMock returns a truthy mock
    # and would silently make every entity look like a thread. Default is job;
    # thread tests set get_thread.return_value themselves.
    db.get_thread = AsyncMock(return_value=None)
    db.get_job = AsyncMock(return_value={"id": "job", "user_id": "u1"})
    return db


@pytest.fixture
def leader():
    """Mark this replica as the elected leader for register/probe tests."""
    from orchestrator.services.leader_election import is_leader

    is_leader.set()
    try:
        yield
    finally:
        is_leader.clear()


@pytest.fixture
def bridge(mock_nats_module, mock_nc, monkeypatch):
    """Create a NatsBridge instance with a connected mock NATS client.

    Sets ORCHESTRATOR_ID=srw-test so all vm.lifecycle.* subjects are scoped
    (matching how the bridge runs in production). Tests for the unset-id
    refusal path use the dedicated `unscoped_bridge` fixture below.
    """
    monkeypatch.setenv("ORCHESTRATOR_ID", "srw-test")
    from orchestrator.services.nats_bridge import NatsBridge

    b = NatsBridge(url="nats://test:4222")
    b._nc = mock_nc
    b._available = True
    return b


@pytest.fixture
def bridge_with_db(bridge, mock_db, monkeypatch):
    """Create a NatsBridge with both mock client and mock database."""
    bridge._db = mock_db
    monkeypatch.setenv("VM_MODE", "external")
    return bridge


@pytest.fixture
def disconnected_bridge(mock_nats_module, monkeypatch):
    """Create a NatsBridge that is not connected (available=False)."""
    monkeypatch.setenv("ORCHESTRATOR_ID", "srw-test")
    from orchestrator.services.nats_bridge import NatsBridge

    b = NatsBridge(url="nats://test:4222")
    b._available = False
    return b


@pytest.fixture
def unscoped_bridge(mock_nats_module, mock_nc, monkeypatch):
    """A connected NatsBridge with ORCHESTRATOR_ID explicitly unset.

    Exercises the failsafe path: publish/subscribe to vm.lifecycle.* MUST
    refuse rather than fall back to flat subjects (which would cross-talk
    on a shared NATS hub).
    """
    monkeypatch.delenv("ORCHESTRATOR_ID", raising=False)
    from orchestrator.services.nats_bridge import NatsBridge

    b = NatsBridge(url="nats://test:4222")
    b._nc = mock_nc
    b._available = True
    return b


# =============================================================================
# Test: Initialization
# =============================================================================


class TestNatsBridgeInit:
    """Tests for NatsBridge.__init__ and is_available property."""

    def test_init_with_url(self, mock_nats_module):
        from orchestrator.services.nats_bridge import NatsBridge

        bridge = NatsBridge(url="nats://myhost:4222")
        assert bridge._url == "nats://myhost:4222"
        assert bridge._nc is None
        assert bridge._available is False

    def test_init_with_env_var(self, mock_nats_module):
        from orchestrator.services.nats_bridge import NatsBridge

        with patch.dict("os.environ", {"NATS_URL": "nats://env-host:4222"}):
            bridge = NatsBridge()
            assert bridge._url == "nats://env-host:4222"

    def test_init_url_overrides_env(self, mock_nats_module):
        from orchestrator.services.nats_bridge import NatsBridge

        with patch.dict("os.environ", {"NATS_URL": "nats://env-host:4222"}):
            bridge = NatsBridge(url="nats://explicit:4222")
            assert bridge._url == "nats://explicit:4222"

    def test_init_no_url_no_env(self, mock_nats_module):
        from orchestrator.services.nats_bridge import NatsBridge

        with patch.dict("os.environ", {}, clear=True):
            bridge = NatsBridge(url=None)
            assert bridge._url is None

    def test_is_available_false_initially(self, mock_nats_module):
        from orchestrator.services.nats_bridge import NatsBridge

        bridge = NatsBridge(url="nats://test:4222")
        assert bridge.is_available is False

    def test_is_available_true_when_connected(self, bridge):
        assert bridge.is_available is True

    def test_init_without_nats_package(self):
        """When nats-py is not installed, bridge initializes but stays unavailable."""
        with patch("orchestrator.services.nats_bridge.NATS_AVAILABLE", False):
            from orchestrator.services.nats_bridge import NatsBridge

            bridge = NatsBridge(url="nats://test:4222")
            assert bridge._available is False


# =============================================================================
# Test: connect()
# =============================================================================


class TestConnect:
    """Tests for NatsBridge.connect()."""

    @pytest.mark.asyncio
    async def test_connect_success(self, mock_nats_module, mock_db, monkeypatch):
        from orchestrator.services.nats_bridge import NatsBridge

        mock_nc = AsyncMock()
        mock_nc.subscribe = AsyncMock()
        mock_nats_module.connect = AsyncMock(return_value=mock_nc)

        monkeypatch.setenv("ORCHESTRATOR_ID", "srw-test")
        bridge = NatsBridge(url="nats://test:4222")

        await bridge.connect(db=mock_db)

        assert bridge._available is True
        assert bridge._nc is mock_nc
        assert bridge._db is mock_db

        # Only the authenticated controller lifecycle feed and the existing
        # session-event feed remain. Guest registration/heartbeat/sudo are
        # never exposed on the unauthenticated broker.
        assert mock_nc.subscribe.call_count == 2

        # All broadcast subjects are per-orchestrator scoped.
        subjects = [call.args[0] for call in mock_nc.subscribe.call_args_list]
        assert "vm.lifecycle.status.srw-test" in subjects
        assert "session.events.srw-test.>" in subjects
        assert not any(subject.startswith("agent.vm.") for subject in subjects)
        assert not any(subject.startswith("sudo.request.") for subject in subjects)
        # Regression: legacy flat subjects must not appear
        for flat in (
            "vm.lifecycle.status",
            "agent.vm.*.register",
            "agent.vm.*.heartbeat",
            "sudo.request.>",
            "session.events.>",
        ):
            assert flat not in subjects

    @pytest.mark.asyncio
    async def test_connect_stores_on_vm_ready_callback(self, mock_nats_module, mock_db):
        from orchestrator.services.nats_bridge import NatsBridge

        mock_nc = AsyncMock()
        mock_nc.subscribe = AsyncMock()
        mock_nats_module.connect = AsyncMock(return_value=mock_nc)

        bridge = NatsBridge(url="nats://test:4222")
        callback = MagicMock()

        mock_sudo_module = MagicMock()
        with patch.dict(
            "sys.modules", {"orchestrator.services.sudo_gate": mock_sudo_module}
        ):
            await bridge.connect(db=mock_db, on_vm_ready=callback)

        assert bridge._on_vm_ready is callback

    @pytest.mark.asyncio
    async def test_connect_already_connected_noop(self, bridge, mock_db):
        """If already connected, connect() is a no-op."""
        original_nc = bridge._nc
        await bridge.connect(db=mock_db)
        assert bridge._nc is original_nc

    @pytest.mark.asyncio
    async def test_connect_no_url_stays_unavailable(self, mock_nats_module, mock_db):
        from orchestrator.services.nats_bridge import NatsBridge

        with patch.dict("os.environ", {}, clear=True):
            bridge = NatsBridge(url=None)
            await bridge.connect(db=mock_db)
            assert bridge._available is False
            assert bridge._nc is None

    @pytest.mark.asyncio
    async def test_connect_nats_unavailable(self, mock_db):
        """When nats-py is not installed, connect() is a no-op."""
        with patch("orchestrator.services.nats_bridge.NATS_AVAILABLE", False):
            from orchestrator.services.nats_bridge import NatsBridge

            bridge = NatsBridge(url="nats://test:4222")
            await bridge.connect(db=mock_db)
            assert bridge._available is False

    @pytest.mark.asyncio
    async def test_connect_failure_sets_unavailable(self, mock_nats_module, mock_db):
        from orchestrator.services.nats_bridge import NatsBridge

        mock_nats_module.connect = AsyncMock(
            side_effect=ConnectionRefusedError("refused")
        )
        bridge = NatsBridge(url="nats://test:4222")

        await bridge.connect(db=mock_db)

        assert bridge._available is False
        assert bridge._nc is None

    @pytest.mark.asyncio
    async def test_connect_passes_nats_options(self, mock_nats_module, mock_db):
        from orchestrator.services.nats_bridge import NatsBridge

        mock_nc = AsyncMock()
        mock_nc.subscribe = AsyncMock()
        mock_nats_module.connect = AsyncMock(return_value=mock_nc)

        bridge = NatsBridge(url="nats://test:4222")

        mock_sudo_module = MagicMock()
        with patch.dict(
            "sys.modules", {"orchestrator.services.sudo_gate": mock_sudo_module}
        ):
            await bridge.connect(db=mock_db)

        # Verify nats.connect was called with expected parameters
        call_kwargs = mock_nats_module.connect.call_args
        assert call_kwargs.args[0] == "nats://test:4222"
        assert call_kwargs.kwargs["max_reconnect_attempts"] == -1
        assert call_kwargs.kwargs["reconnect_time_wait"] == 2


# =============================================================================
# Test: disconnect()
# =============================================================================


class TestDisconnect:
    """Tests for NatsBridge.disconnect()."""

    @pytest.mark.asyncio
    async def test_disconnect_drains_and_cleans_up(self, bridge, mock_nc):
        await bridge.disconnect()

        mock_nc.drain.assert_awaited_once()
        assert bridge._nc is None
        assert bridge._available is False

    @pytest.mark.asyncio
    async def test_disconnect_when_not_connected(self, disconnected_bridge):
        """Disconnect when not connected is a no-op."""
        disconnected_bridge._nc = None
        await disconnected_bridge.disconnect()
        # No exception should be raised
        assert disconnected_bridge._nc is None

    @pytest.mark.asyncio
    async def test_disconnect_drain_error_handled(self, bridge, mock_nc):
        """Drain errors are caught and logged, not raised."""
        mock_nc.drain = AsyncMock(side_effect=RuntimeError("drain error"))

        await bridge.disconnect()

        # Should still clean up despite drain error
        assert bridge._nc is None
        assert bridge._available is False

    @pytest.mark.asyncio
    async def test_disconnect_idempotent(self, bridge, mock_nc):
        """Calling disconnect twice is safe."""
        await bridge.disconnect()
        await bridge.disconnect()  # Should not raise
        assert bridge._nc is None


# =============================================================================
# Test: request_vm_create()
# =============================================================================


class TestRequestVmCreate:
    """Tests for NatsBridge.request_vm_create()."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("entity_type", ["job", "thread"])
    async def test_create_signs_requested_disk_size(
        self, bridge_with_db, mock_nc, entity_type
    ):
        from orchestrator.services.vm_lifecycle_auth import verify_payload
        from shared.workspace_initialization import initialization_request

        secret = b"development-vm-test-lifecycle-secret-32-bytes"
        bridge_with_db._lifecycle_hmac_secret = secret
        initialization = initialization_request([{"command": ["true"]}])
        await bridge_with_db.request_vm_create(
            job_id="test-job",
            entity_type=entity_type,
            vm_image="registry.example/dev-vm:v1",
            disk_size="120Gi",
            initialization=initialization,
            provision_generation=PROVISION_GENERATION,
        )
        payload = json.loads(mock_nc.publish.call_args.args[1])
        assert payload["disk_size"] == "120Gi"
        assert payload["initialization"] == initialization
        assert verify_payload(
            payload, direction="request", operation="create", secret=secret
        )
        payload["initialization"]["steps"][0]["command"] = ["false"]
        assert not verify_payload(
            payload, direction="request", operation="create", secret=secret
        )

    @pytest.mark.asyncio
    async def test_create_publishes_correct_payload(
        self, bridge_with_db, mock_nc, mock_db
    ):
        result = await bridge_with_db.request_vm_create(
            job_id="test-job-123",
            agent_config="scholar",
            cpu_cores=4,
            memory="8Gi",
            description="Test job description",
        )

        assert result is True
        mock_nc.publish.assert_awaited_once()

        call_args = mock_nc.publish.call_args
        assert call_args.args[0] == "vm.lifecycle.create.srw-test"

        payload = json.loads(call_args.args[1].decode())
        assert payload["job_id"] == "test-job-123"
        assert payload["entity_type"] == "job"
        assert payload["agent_config"] == "scholar"
        assert payload["cpu_cores"] == 4
        assert payload["memory"] == "8Gi"
        assert payload["nats_url"] == "nats://test:4222"
        assert payload["description"] == "Test job description"
        assert payload["orchestrator_id"] == "srw-test"
        assert "vm_image" not in payload  # Not provided, should be absent

    @pytest.mark.asyncio
    async def test_create_includes_vm_image_when_provided(
        self, bridge_with_db, mock_nc
    ):
        await bridge_with_db.request_vm_create(
            job_id="test-job-123",
            vm_image="registry.example.com/vm-base:latest",
        )

        payload = json.loads(mock_nc.publish.call_args.args[1].decode())
        assert payload["vm_image"] == "registry.example.com/vm-base:latest"

    @pytest.mark.asyncio
    async def test_create_carries_thread_identity_to_controller(
        self, bridge_with_db, mock_nc
    ):
        await bridge_with_db.request_vm_create(
            job_id="thread-123",
            entity_type="thread",
        )

        payload = json.loads(mock_nc.publish.call_args.args[1].decode())
        assert payload["job_id"] == "thread-123"
        assert payload["entity_type"] == "thread"

    @pytest.mark.asyncio
    async def test_create_excludes_vm_image_when_none(self, bridge_with_db, mock_nc):
        await bridge_with_db.request_vm_create(
            job_id="test-job-123",
            vm_image=None,
        )

        payload = json.loads(mock_nc.publish.call_args.args[1].decode())
        assert "vm_image" not in payload

    @pytest.mark.asyncio
    async def test_create_uses_default_params(self, bridge_with_db, mock_nc):
        await bridge_with_db.request_vm_create(job_id="test-job-123")

        payload = json.loads(mock_nc.publish.call_args.args[1].decode())
        assert payload["agent_config"] == "worker_base"
        assert payload["cpu_cores"] == 8
        assert payload["memory"] == "16Gi"
        assert payload["description"] == ""

    @pytest.mark.asyncio
    async def test_create_updates_vm_context(self, bridge_with_db, mock_db):
        await bridge_with_db.request_vm_create(job_id="test-job-123")

        mock_db.merge_vm_context.assert_awaited_once_with(
            "test-job-123", {"status": "provisioning"}
        )

    @pytest.mark.asyncio
    async def test_golden_poll_skips_provisioning_status(
        self, bridge_with_db, mock_nc, mock_db
    ):
        """set_provisioning=False (dispatcher golden poll) must not overwrite
        the context status — it must stay ``waiting_golden`` between polls so
        the decision logic keeps polling instead of treating the job as a
        booting VM."""
        result = await bridge_with_db.request_vm_create(
            job_id="test-job-123", set_provisioning=False
        )

        assert result is True
        mock_nc.publish.assert_awaited_once()  # the create IS still published
        mock_db.merge_vm_context.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_create_returns_false_when_unavailable(self, disconnected_bridge):
        result = await disconnected_bridge.request_vm_create(job_id="test-job-123")
        assert result is False

    @pytest.mark.asyncio
    async def test_create_returns_false_on_publish_error(self, bridge_with_db, mock_nc):
        mock_nc.publish = AsyncMock(side_effect=RuntimeError("publish failed"))

        result = await bridge_with_db.request_vm_create(job_id="test-job-123")
        assert result is False

    @pytest.mark.asyncio
    async def test_create_empty_string_vm_image_excluded(self, bridge_with_db, mock_nc):
        """An empty string vm_image is falsy and should not be included."""
        await bridge_with_db.request_vm_create(job_id="test-job-123", vm_image="")

        payload = json.loads(mock_nc.publish.call_args.args[1].decode())
        assert "vm_image" not in payload


# =============================================================================
# Test: request_vm_delete()
# =============================================================================


class TestRequestVmDelete:
    """Tests for NatsBridge.request_vm_delete()."""

    @pytest.mark.asyncio
    async def test_parent_cleanup_proof_is_covered_by_delete_signature(
        self, bridge_with_db, mock_nc
    ):
        from orchestrator.services.vm_lifecycle_auth import verify_payload

        secret = b"parent-cleanup-transport-test-secret"
        bridge_with_db._lifecycle_hmac_secret = secret
        proof = {"admission_id": "parent-admission", "intent_digest": "sha256:exact"}
        assert await bridge_with_db.request_vm_delete(
            "test-job-456",
            provision_generation="00000000-0000-4000-8000-000000000001",
            parent_cleanup=proof,
        )
        payload = json.loads(mock_nc.publish.call_args.args[1].decode())
        assert payload["parent_cleanup"] == proof
        assert verify_payload(
            payload, direction="request", operation="delete", secret=secret
        )
        payload["parent_cleanup"]["admission_id"] = "different-parent"
        assert not verify_payload(
            payload, direction="request", operation="delete", secret=secret
        )

    @pytest.mark.asyncio
    async def test_delete_publishes_correct_payload(
        self, bridge_with_db, mock_nc, mock_db
    ):
        result = await bridge_with_db.request_vm_delete(job_id="test-job-456")

        assert result is True
        mock_nc.publish.assert_awaited_once()

        call_args = mock_nc.publish.call_args
        assert call_args.args[0] == "vm.lifecycle.delete.srw-test"

        payload = json.loads(call_args.args[1].decode())
        assert payload == {
            "job_id": "test-job-456",
            "orchestrator_id": "srw-test",
            # Terminal by default; False keeps the persistent rootdisk.
            "purge_disk": True,
        }

    @pytest.mark.asyncio
    async def test_delete_can_request_a_kept_disk(self, bridge_with_db, mock_nc):
        await bridge_with_db.request_vm_delete("test-job-456", purge_disk=False)
        payload = json.loads(mock_nc.publish.call_args.args[1].decode())
        assert payload["purge_disk"] is False

    @pytest.mark.asyncio
    async def test_delete_updates_vm_context(self, bridge_with_db, mock_db):
        await bridge_with_db.request_vm_delete(job_id="test-job-456")

        mock_db.merge_vm_context.assert_awaited_once()
        job_id, ctx = mock_db.merge_vm_context.await_args.args
        assert job_id == "test-job-456"
        assert ctx["status"] == "deleting"
        # The dispatcher needs this anchor to tell an in-flight teardown from a
        # stuck one; without it a dropped NATS message strands the job forever
        # (dispatch_guards.vm_provisioning_decision).
        assert isinstance(ctx["deleting_started_at"], float)

    @pytest.mark.asyncio
    async def test_delete_returns_false_when_unavailable(self, disconnected_bridge):
        result = await disconnected_bridge.request_vm_delete(job_id="test-job-456")
        assert result is False

    @pytest.mark.asyncio
    async def test_delete_returns_false_on_publish_error(self, bridge_with_db, mock_nc):
        mock_nc.publish = AsyncMock(side_effect=RuntimeError("publish failed"))

        result = await bridge_with_db.request_vm_delete(job_id="test-job-456")
        assert result is False


# =============================================================================
# Test: query_vm_status()
# =============================================================================


class TestQueryVmStatus:
    """Tests for NatsBridge.query_vm_status()."""

    @pytest.mark.asyncio
    async def test_query_returns_status_dict(self, bridge, mock_nc):
        response_data = {
            "job_id": "test-job-789",
            "status": "running",
            "vm_name": "vm-test-job-789",
            "namespace": "agent-vms",
        }
        response_msg = MagicMock()
        response_msg.data = json.dumps(response_data).encode()
        mock_nc.request = AsyncMock(return_value=response_msg)

        result = await bridge.query_vm_status("test-job-789")

        assert result == response_data
        mock_nc.request.assert_awaited_once()

        call_args = mock_nc.request.call_args
        assert call_args.args[0] == "vm.lifecycle.get.srw-test"
        payload = json.loads(call_args.args[1].decode())
        assert payload == {"job_id": "test-job-789", "orchestrator_id": "srw-test"}
        assert call_args.kwargs["timeout"] == 5.0

    @pytest.mark.asyncio
    async def test_query_custom_timeout(self, bridge, mock_nc):
        response_msg = MagicMock()
        response_msg.data = json.dumps({"status": "ok"}).encode()
        mock_nc.request = AsyncMock(return_value=response_msg)

        await bridge.query_vm_status("test-job", timeout=10.0)

        assert mock_nc.request.call_args.kwargs["timeout"] == 10.0

    @pytest.mark.asyncio
    async def test_query_returns_none_on_timeout(self, bridge, mock_nc):
        mock_nc.request = AsyncMock(side_effect=TimeoutError("request timed out"))

        result = await bridge.query_vm_status("test-job-789")
        assert result is None

    @pytest.mark.asyncio
    async def test_query_returns_none_when_unavailable(self, disconnected_bridge):
        result = await disconnected_bridge.query_vm_status("test-job-789")
        assert result is None

    @pytest.mark.asyncio
    async def test_query_returns_none_on_generic_error(self, bridge, mock_nc):
        mock_nc.request = AsyncMock(side_effect=RuntimeError("connection lost"))

        result = await bridge.query_vm_status("test-job-789")
        assert result is None

    @pytest.mark.asyncio
    async def test_query_rejects_jetstream_ack(self, bridge, mock_nc):
        # If a JetStream stream's subjects ever cover vm.lifecycle.get.*, the
        # request inbox receives the stream's publish-ack instead of the
        # controller reply. That ack must never be surfaced as VM status.
        # See knowledge-base/knowledge/issues/vm_live_status_query_shadowed_by_jetstream_stream.md
        ack = MagicMock()
        ack.data = json.dumps({"stream": "VM_EVENTS", "seq": 42}).encode()
        mock_nc.request = AsyncMock(return_value=ack)

        result = await bridge.query_vm_status("test-job-789")
        assert result is None

    @pytest.mark.asyncio
    async def test_query_allows_reply_with_seq_field(self, bridge, mock_nc):
        # A genuine controller reply carrying job_id is NOT an ack even if it
        # happens to include stream/seq-like keys — the guard keys on the
        # absence of job_id.
        response_data = {
            "job_id": "test-job-789",
            "vm_name": "vm-test-job-789",
            "stream": "ignored",
            "seq": 7,
        }
        response_msg = MagicMock()
        response_msg.data = json.dumps(response_data).encode()
        mock_nc.request = AsyncMock(return_value=response_msg)

        result = await bridge.query_vm_status("test-job-789")
        assert result == response_data


# =============================================================================
# Test: _on_vm_lifecycle_status()
# =============================================================================


class TestOnVmLifecycleStatus:
    """Tests for NatsBridge._on_vm_lifecycle_status()."""

    @pytest.mark.asyncio
    async def test_same_cluster_ignores_nats_lifecycle_evidence(
        self, bridge_with_db, mock_db, monkeypatch
    ):
        monkeypatch.setenv("VM_MODE", "same-cluster")

        await bridge_with_db._on_vm_lifecycle_status(
            make_msg({"job_id": "job-001", "status": "ready"})
        )

        mock_db.merge_vm_context.assert_not_awaited()
        mock_db.merge_vm_context_if_provision_generation.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_basic_status_update(self, bridge_with_db, mock_db):
        msg = make_msg(
            {
                "job_id": "job-001",
                "status": "running",
                "vm_name": "vm-job-001",
                "namespace": "agent-vms",
            }
        )

        await bridge_with_db._on_vm_lifecycle_status(msg)

        mock_db.merge_vm_context.assert_awaited_once_with(
            "job-001",
            {
                "status": "running",
                "vm_name": "vm-job-001",
                "namespace": "agent-vms",
            },
        )

    @pytest.mark.asyncio
    async def test_created_status_persists_admitted_vm_uid(
        self, bridge_with_db, mock_db
    ):
        secret = b"nats-bridge-lifecycle-test-secret-at-least-32-bytes"
        bridge_with_db._lifecycle_hmac_secret = secret
        generation = "00000000-0000-4000-8000-000000000001"
        msg = make_msg(
            sign_payload(
                {
                    "job_id": "job-vm-uid",
                    "status": "created",
                    "vm_name": "agent-vm-job-vm-uid",
                    "vm_uid": "admitted-vm-uid-123",
                    "rootdisk_pvc_uid": "admitted-root-pvc-uid-456",
                    "namespace": "agent-vms",
                    "provision_generation": generation,
                },
                direction="response",
                operation="create",
                secret=secret,
            )
        )

        await bridge_with_db._on_vm_lifecycle_status(msg)

        updates = mock_db.merge_vm_context_if_provision_generation.await_args.args[2]
        assert updates["vm_uid"] == "admitted-vm-uid-123"
        assert updates["rootdisk_pvc_uid"] == "admitted-root-pvc-uid-456"
        assert updates["identity_authenticated"] is True
        mock_db.merge_vm_context.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_stale_authenticated_status_never_falls_back_to_unguarded_merge(
        self, bridge_with_db, mock_db
    ):
        secret = b"nats-bridge-lifecycle-test-secret-at-least-32-bytes"
        bridge_with_db._lifecycle_hmac_secret = secret
        mock_db.merge_vm_context_if_provision_generation.return_value = False
        payload = sign_payload(
            {
                "job_id": "job-stale",
                "status": "failed",
                "error": "old failure",
                "provision_generation": "00000000-0000-4000-8000-000000000001",
            },
            direction="response",
            operation="create",
            secret=secret,
        )

        await bridge_with_db._on_vm_lifecycle_status(make_msg(payload))

        mock_db.merge_vm_context_if_provision_generation.assert_awaited_once()
        mock_db.merge_vm_context.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_authenticated_status_without_generation_is_dropped(
        self, bridge_with_db, mock_db
    ):
        secret = b"nats-bridge-lifecycle-test-secret-at-least-32-bytes"
        bridge_with_db._lifecycle_hmac_secret = secret
        payload = sign_payload(
            {"job_id": "job-no-generation", "status": "failed"},
            direction="response",
            operation="create",
            secret=secret,
        )

        await bridge_with_db._on_vm_lifecycle_status(make_msg(payload))

        mock_db.merge_vm_context_if_provision_generation.assert_not_awaited()
        mock_db.merge_vm_context.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_old_or_invalid_status_does_not_overwrite_vm_uid(
        self, bridge_with_db, mock_db
    ):
        await bridge_with_db._on_vm_lifecycle_status(
            make_msg(
                {
                    "job_id": "job-old-controller",
                    "status": "created",
                    "vm_name": "agent-vm-job-old-controller",
                    "namespace": "agent-vms",
                }
            )
        )
        old_updates = mock_db.merge_vm_context.await_args.args[1]
        assert "vm_uid" not in old_updates
        assert "rootdisk_pvc_uid" not in old_updates

        mock_db.merge_vm_context.reset_mock()
        await bridge_with_db._on_vm_lifecycle_status(
            make_msg(
                {
                    "job_id": "job-invalid-uid",
                    "status": "created",
                    "vm_name": "agent-vm-job-invalid-uid",
                    "vm_uid": "invalid uid",
                    "rootdisk_pvc_uid": "invalid uid",
                    "namespace": "agent-vms",
                }
            )
        )
        invalid_updates = mock_db.merge_vm_context.await_args.args[1]
        assert "vm_uid" not in invalid_updates
        assert "rootdisk_pvc_uid" not in invalid_updates

    @pytest.mark.asyncio
    async def test_status_with_error(self, bridge_with_db, mock_db):
        msg = make_msg(
            {
                "job_id": "job-002",
                "status": "failed",
                "vm_name": "vm-job-002",
                "namespace": "agent-vms",
                "error": "Insufficient resources",
            }
        )

        await bridge_with_db._on_vm_lifecycle_status(msg)

        call_args = mock_db.merge_vm_context.call_args
        updates = call_args.args[1]
        assert updates["error"] == "Insufficient resources"
        assert updates["status"] == "failed"

    @pytest.mark.asyncio
    async def test_status_with_pod_ip(self, bridge_with_db, mock_db):
        msg = make_msg(
            {
                "job_id": "job-003",
                "status": "running",
                "vm_name": "vm-job-003",
                "namespace": "agent-vms",
                "pod_ip": "10.0.0.42",
            }
        )

        await bridge_with_db._on_vm_lifecycle_status(msg)

        updates = mock_db.merge_vm_context.call_args.args[1]
        assert updates["pod_ip"] == "10.0.0.42"

    @pytest.mark.asyncio
    async def test_waiting_golden_passes_import_telemetry(
        self, bridge_with_db, mock_db
    ):
        """waiting_golden statuses carry golden DV name + import progress —
        the dispatcher's poll logging and park error message read them from
        context.vm."""
        msg = make_msg(
            {
                "job_id": "job-golden",
                "status": "waiting_golden",
                "golden": "agent-vm-golden-9ca967a4ca08",
                "golden_phase": "ImportInProgress",
                "golden_progress": "68.19%",
            }
        )

        await bridge_with_db._on_vm_lifecycle_status(msg)

        updates = mock_db.merge_vm_context.call_args.args[1]
        assert updates["status"] == "waiting_golden"
        assert updates["golden"] == "agent-vm-golden-9ca967a4ca08"
        assert updates["golden_phase"] == "ImportInProgress"
        assert updates["golden_progress"] == "68.19%"

    @pytest.mark.asyncio
    async def test_non_golden_status_has_no_golden_keys(self, bridge_with_db, mock_db):
        msg = make_msg(
            {
                "job_id": "job-005",
                "status": "created",
                "vm_name": "vm-job-005",
                "namespace": "agent-vms",
            }
        )

        await bridge_with_db._on_vm_lifecycle_status(msg)

        updates = mock_db.merge_vm_context.call_args.args[1]
        assert "golden" not in updates
        assert "golden_progress" not in updates

    @pytest.mark.asyncio
    async def test_status_with_ssh_nodeport(self, bridge_with_db, mock_db):
        msg = make_msg(
            {
                "job_id": "job-004",
                "status": "running",
                "vm_name": "vm-job-004",
                "namespace": "agent-vms",
                "ssh_nodeport": 30022,
            }
        )

        await bridge_with_db._on_vm_lifecycle_status(msg)

        updates = mock_db.merge_vm_context.call_args.args[1]
        assert updates["ssh_nodeport"] == 30022

    @pytest.mark.asyncio
    async def test_status_without_optional_fields(self, bridge_with_db, mock_db):
        """Optional fields (error, pod_ip, ssh_nodeport) should not appear if absent."""
        msg = make_msg(
            {
                "job_id": "job-005",
                "status": "creating",
                "vm_name": "vm-job-005",
                "namespace": "agent-vms",
            }
        )

        await bridge_with_db._on_vm_lifecycle_status(msg)

        updates = mock_db.merge_vm_context.call_args.args[1]
        assert "error" not in updates
        assert "pod_ip" not in updates
        assert "ssh_nodeport" not in updates

    @pytest.mark.asyncio
    async def test_status_missing_job_id_ignored(self, bridge_with_db, mock_db):
        msg = make_msg({"status": "running", "vm_name": "vm-orphan"})

        await bridge_with_db._on_vm_lifecycle_status(msg)

        mock_db.merge_vm_context.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_status_malformed_json(self, bridge_with_db, mock_db):
        """Malformed JSON should be handled gracefully, not raise."""
        msg = make_msg_raw(b"not-valid-json{{{")

        await bridge_with_db._on_vm_lifecycle_status(msg)

        mock_db.merge_vm_context.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_status_empty_payload(self, bridge_with_db, mock_db):
        msg = make_msg({})

        await bridge_with_db._on_vm_lifecycle_status(msg)

        mock_db.merge_vm_context.assert_not_awaited()


# =============================================================================
# Test: _on_daemon_register()
# =============================================================================


class TestOnDaemonRegister:
    """Tests for NatsBridge._on_daemon_register().

    Readiness evidence is daemon-supplied (``ssh_ready``) or, for legacy
    golden images that omit the field, inferred from a tailnet ``ssh_host``.
    There is NO orchestrator-side SSH probe — the orchestrator has no route
    to the tailnet, so a probe gate can never pass (see knowledge-base/knowledge/issues/
    vm_ssh_readiness_probe_unroutable_from_orchestrator.md).
    """

    @pytest.mark.asyncio
    async def test_register_is_ignored_outside_external_mode(
        self, bridge_with_db, mock_db, monkeypatch
    ):
        monkeypatch.setenv("VM_MODE", "same-cluster")
        await bridge_with_db._on_daemon_register(
            make_msg({"job_id": "job-reg", "ip": "100.64.1.5", "ssh_ready": True})
        )
        mock_db.get_vm_provision_generation.assert_not_awaited()
        mock_db.merge_vm_context_if_provision_generation.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_thread_vm_guest_identity_does_not_enable_canvas(
        self, bridge_with_db, mock_db, leader
    ):
        """Guest NATS self-report is not provisioner-attested; fail closed."""
        thread_id = "thread-reg-canvas-closed"
        mock_db.get_thread = AsyncMock(return_value={"id": thread_id, "user_id": "u1"})
        bridge_with_db._seed_vm_ide_config = AsyncMock()
        await bridge_with_db._on_daemon_register(
            make_msg(
                {
                    "job_id": thread_id,
                    "hostname": "vm-thread",
                    "ip": "100.64.1.55",
                    "pid": 44,
                    "ssh_ready": True,
                    "ssh_host_key_fingerprint": "SHA256:self-reported",
                    "workspace_backing_id": "self-reported-boot",
                }
            )
        )

        updates = (
            mock_db.merge_thread_vm_context_if_provision_generation.call_args.args[2]
        )
        assert updates["status"] == "ready"
        assert updates["_canvas_workspace_generation"] is None
        mock_db.bind_thread_workspace_backing.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_ssh_ready_true_promotes_to_ready(
        self, bridge_with_db, mock_db, leader
    ):
        callback = MagicMock()
        bridge_with_db._on_vm_ready = callback
        bridge_with_db._seed_vm_ide_config = AsyncMock()
        msg = make_msg(
            {
                "job_id": "job-reg-001",
                "hostname": "vm-agent-001",
                "ip": "100.64.1.5",
                "pid": 12345,
                "ssh_ready": True,
            }
        )

        await bridge_with_db._on_daemon_register(msg)
        await asyncio.sleep(0)  # let the seed task run

        mock_db.merge_vm_context_if_provision_generation.assert_awaited_once()
        assert (
            mock_db.merge_vm_context_if_provision_generation.call_args.args[0]
            == "job-reg-001"
        )
        updates = mock_db.merge_vm_context_if_provision_generation.call_args.args[2]
        assert updates["status"] == "ready"
        assert updates["ssh_host"] == "100.64.1.5"
        assert updates["ssh_port"] == 22
        assert updates["hostname"] == "vm-agent-001"
        assert updates["daemon_pid"] == 12345
        assert updates["recovering"] is False
        assert updates["registered_at"]
        assert updates["ssh_registration_id"]
        assert updates["ssh_ready_source"] == "daemon"
        assert updates["ssh_verified_at"]
        assert updates["ssh_probe_error"] is None
        callback.assert_called_once()
        bridge_with_db._seed_vm_ide_config.assert_called_once_with(
            ANY,
            "100.64.1.5",
            22,
        )
        seeded_identity = bridge_with_db._seed_vm_ide_config.call_args.args[0]
        assert seeded_identity.entity_type == "job"
        assert seeded_identity.entity_id == "job-reg-001"
        assert seeded_identity.provision_generation == PROVISION_GENERATION

    @pytest.mark.asyncio
    async def test_current_generation_seed_is_pinned_and_post_revalidated(
        self, bridge_with_db, mock_db, monkeypatch
    ):
        from orchestrator.security.vm_guest import VmGuestIdentity
        from orchestrator.services.container_provisioner import (
            WorkspaceRuntimeAttestation,
        )

        fingerprint = "SHA256:" + ("A" * 43)
        attestation = WorkspaceRuntimeAttestation(
            backing_id="k8s-vmi:00000000-0000-4000-8000-000000000010",
            workspace_generation=PROVISION_GENERATION,
            runtime_incarnation="00000000-0000-4000-8000-000000000010",
            ssh_host_key_fingerprint=fingerprint,
            host="192.0.2.44",
            pod_ip="10.42.0.44",
            port=22,
        )
        provisioner = MagicMock()
        provisioner.query_status = AsyncMock(return_value={"ready": True})
        provisioner.attest_workspace_runtime = AsyncMock(return_value=attestation)
        effects = []

        async def seed(*_args, **kwargs):
            assert kwargs["expected_host_key_fingerprint"] == fingerprint
            target = await kwargs["mutation_authority"]()
            if target is not None:
                effects.append(target)
            return target is not None

        monkeypatch.setattr(
            "orchestrator.services.vm_provisioner.vm_provisioner", provisioner
        )
        monkeypatch.setattr(
            "orchestrator.services.ide_settings.seed_ide_config_for_user", seed
        )
        monkeypatch.setattr(
            "orchestrator.services.snapshot_service.snapshot_service",
            MagicMock(is_available=False),
        )
        identity = VmGuestIdentity("job", "job-current", PROVISION_GENERATION)
        mock_db.get_job.return_value = {
            "id": "job-current",
            "user_id": "user-1",
            "context": {
                "vm": {
                    "provision_generation": PROVISION_GENERATION,
                    "identity_provision_generation": PROVISION_GENERATION,
                    "identity_authenticated": True,
                    "vm_uid": "vm-uid-1",
                    "ssh_host": "192.0.2.44",
                    "ssh_port": 22,
                    "ssh_host_key_fingerprint": fingerprint,
                    "ssh_registration_id": "registration-1",
                }
            },
        }
        bridge_with_db.query_vm_status = AsyncMock(
            return_value={
                "_identity_authenticated": True,
                "ready": True,
                "provision_generation": PROVISION_GENERATION,
                "vm_uid": "vm-uid-1",
                "active_pod_uid": ("00000000-0000-4000-8000-000000000010"),
            }
        )

        assert await bridge_with_db._seed_vm_ide_config(identity, "192.0.2.44", 22)
        assert effects == [("192.0.2.44", 22, fingerprint)]
        assert provisioner.attest_workspace_runtime.await_count == 3

    @pytest.mark.asyncio
    async def test_replaced_vm_reusing_endpoint_receives_zero_seed_bytes(
        self, bridge_with_db, mock_db, monkeypatch
    ):
        from orchestrator.security.vm_guest import VmGuestIdentity
        from orchestrator.services.container_provisioner import (
            WorkspaceRuntimeAttestation,
        )

        fingerprint_a = "SHA256:" + ("A" * 43)
        fingerprint_b = "SHA256:" + ("B" * 43)

        def attestation(uid, fingerprint):
            return WorkspaceRuntimeAttestation(
                backing_id=f"k8s-vmi:{uid}",
                workspace_generation=PROVISION_GENERATION,
                runtime_incarnation=uid,
                ssh_host_key_fingerprint=fingerprint,
                host="192.0.2.44",
                pod_ip="10.42.0.44",
                port=22,
            )

        first = attestation("00000000-0000-4000-8000-000000000010", fingerprint_a)
        successor = attestation("00000000-0000-4000-8000-000000000011", fingerprint_b)
        provisioner = MagicMock()
        provisioner.query_status = AsyncMock(return_value={"ready": True})
        provisioner.attest_workspace_runtime = AsyncMock(
            side_effect=[first, successor, successor]
        )
        effects = []

        async def seed(*_args, **kwargs):
            target = await kwargs["mutation_authority"]()
            if target is not None:
                effects.append(target)
            return target is not None

        monkeypatch.setattr(
            "orchestrator.services.vm_provisioner.vm_provisioner", provisioner
        )
        monkeypatch.setattr(
            "orchestrator.services.ide_settings.seed_ide_config_for_user", seed
        )
        monkeypatch.setattr(
            "orchestrator.services.snapshot_service.snapshot_service",
            MagicMock(is_available=False),
        )
        identity = VmGuestIdentity("job", "job-replaced", PROVISION_GENERATION)
        mock_db.get_job.return_value = {
            "id": "job-replaced",
            "user_id": "user-1",
            "context": {
                "vm": {
                    "provision_generation": PROVISION_GENERATION,
                    "identity_provision_generation": PROVISION_GENERATION,
                    "identity_authenticated": True,
                    "vm_uid": "vm-uid-1",
                    "ssh_host": "192.0.2.44",
                    "ssh_port": 22,
                    "ssh_host_key_fingerprint": fingerprint_a,
                    "ssh_registration_id": "registration-1",
                }
            },
        }
        bridge_with_db.query_vm_status = AsyncMock(
            return_value={
                "_identity_authenticated": True,
                "ready": True,
                "provision_generation": PROVISION_GENERATION,
                "vm_uid": "vm-uid-1",
                "active_pod_uid": ("00000000-0000-4000-8000-000000000010"),
            }
        )

        assert not await bridge_with_db._seed_vm_ide_config(identity, "192.0.2.44", 22)
        assert effects == []

    @pytest.mark.asyncio
    async def test_ssh_ready_false_holds_ssh_pending(
        self, bridge_with_db, mock_db, leader
    ):
        callback = MagicMock()
        bridge_with_db._on_vm_ready = callback
        bridge_with_db._seed_vm_ide_config = AsyncMock()
        msg = make_msg(
            {
                "job_id": "job-reg-002",
                "hostname": "vm-agent-002",
                "ip": "100.64.1.6",
                "pid": 2,
                "ssh_ready": False,
            }
        )

        await bridge_with_db._on_daemon_register(msg)
        await asyncio.sleep(0)

        updates = mock_db.merge_vm_context_if_provision_generation.call_args.args[2]
        assert updates["status"] == "ssh_pending"
        assert updates["ssh_verified_at"] is None
        assert updates["ssh_ready_source"] is None
        assert "re-register" in updates["ssh_probe_error"]
        callback.assert_not_called()
        bridge_with_db._seed_vm_ide_config.assert_not_called()

    @pytest.mark.asyncio
    async def test_legacy_register_with_tailnet_ip_promotes(
        self, bridge_with_db, mock_db, leader
    ):
        """Golden images without the ssh_ready report: tailnet IP ⇒ ready."""
        callback = MagicMock()
        bridge_with_db._on_vm_ready = callback
        bridge_with_db._seed_vm_ide_config = AsyncMock()
        msg = make_msg(
            {
                "job_id": "job-reg-003",
                "hostname": "vm-agent-003",
                "ip": "100.64.2.10",
                "pid": 3,
            }
        )

        await bridge_with_db._on_daemon_register(msg)
        await asyncio.sleep(0)

        updates = mock_db.merge_vm_context_if_provision_generation.call_args.args[2]
        assert updates["status"] == "ready"
        assert updates["ssh_ready_source"] == "legacy_tailnet_ip"
        assert updates["ssh_verified_at"]
        callback.assert_called_once()

    @pytest.mark.asyncio
    async def test_legacy_register_with_nat_fallback_ip_holds_pending(
        self, bridge_with_db, mock_db, leader
    ):
        """The daemon registers with the QEMU NAT IP when tailscale takes
        >60s; that address is unroutable — hold until the tailnet
        re-register."""
        callback = MagicMock()
        bridge_with_db._on_vm_ready = callback
        msg = make_msg(
            {
                "job_id": "job-reg-004",
                "hostname": "vm-agent-004",
                "ip": "10.0.2.15",
                "pid": 4,
            }
        )

        await bridge_with_db._on_daemon_register(msg)

        updates = mock_db.merge_vm_context_if_provision_generation.call_args.args[2]
        assert updates["status"] == "ssh_pending"
        assert "not a tailnet IP" in updates["ssh_probe_error"]
        callback.assert_not_called()

    @pytest.mark.asyncio
    async def test_reregister_readiness_flip_promotes(
        self, bridge_with_db, mock_db, leader
    ):
        """ssh_ready False → True across two registers ends dispatchable."""
        callback = MagicMock()
        bridge_with_db._on_vm_ready = callback
        bridge_with_db._seed_vm_ide_config = AsyncMock()
        payload = {
            "job_id": "job-reg-005",
            "hostname": "vm-agent-005",
            "ip": "100.64.3.1",
            "pid": 5,
        }

        await bridge_with_db._on_daemon_register(
            make_msg({**payload, "ssh_ready": False})
        )
        callback.assert_not_called()

        await bridge_with_db._on_daemon_register(
            make_msg({**payload, "ssh_ready": True})
        )
        await asyncio.sleep(0)

        assert mock_db.merge_vm_context_if_provision_generation.await_count == 2
        updates = mock_db.merge_vm_context_if_provision_generation.call_args.args[2]
        assert updates["status"] == "ready"
        callback.assert_called_once()

    @pytest.mark.asyncio
    async def test_register_falls_back_to_hostname(
        self, bridge_with_db, mock_db, leader
    ):
        """When ip is missing, hostname is used as ssh_host (a hostname is
        not tailnet evidence, so a legacy register stays pending)."""
        msg = make_msg(
            {
                "job_id": "job-reg-006",
                "hostname": "vm-agent-006.local",
                "pid": 99,
            }
        )

        await bridge_with_db._on_daemon_register(msg)

        updates = mock_db.merge_vm_context_if_provision_generation.call_args.args[2]
        assert updates["ssh_host"] == "vm-agent-006.local"
        assert updates["ssh_port"] == 22
        assert updates["status"] == "ssh_pending"

    @pytest.mark.asyncio
    async def test_register_ip_preferred_over_hostname(
        self, bridge_with_db, mock_db, leader
    ):
        """When both ip and hostname are present, ip is preferred for ssh_host."""
        msg = make_msg(
            {
                "job_id": "job-reg-007",
                "hostname": "vm-agent-007",
                "ip": "100.64.2.10",
                "pid": 50,
            }
        )

        await bridge_with_db._on_daemon_register(msg)

        updates = mock_db.merge_vm_context_if_provision_generation.call_args.args[2]
        assert updates["ssh_host"] == "100.64.2.10"
        assert updates["hostname"] == "vm-agent-007"

    @pytest.mark.asyncio
    async def test_promote_callback_error_handled(
        self, bridge_with_db, mock_db, leader
    ):
        """Callback errors should not prevent registration from completing."""
        callback = MagicMock(side_effect=RuntimeError("callback boom"))
        bridge_with_db._on_vm_ready = callback
        bridge_with_db._seed_vm_ide_config = AsyncMock()
        msg = make_msg(
            {
                "job_id": "job-reg-008",
                "hostname": "vm-agent-008",
                "ip": "100.64.4.1",
                "pid": 8,
                "ssh_ready": True,
            }
        )

        await bridge_with_db._on_daemon_register(msg)
        await asyncio.sleep(0)

        mock_db.merge_vm_context_if_provision_generation.assert_awaited_once()
        callback.assert_called_once()

    @pytest.mark.asyncio
    async def test_register_no_callback_set(self, bridge_with_db, mock_db, leader):
        """Promotion works fine when no on_vm_ready callback is set."""
        bridge_with_db._on_vm_ready = None
        bridge_with_db._seed_vm_ide_config = AsyncMock()

        msg = make_msg(
            {
                "job_id": "job-reg-009",
                "hostname": "vm-agent-009",
                "ip": "100.64.5.1",
                "pid": 9,
                "ssh_ready": True,
            }
        )

        await bridge_with_db._on_daemon_register(msg)
        await asyncio.sleep(0)

        mock_db.merge_vm_context_if_provision_generation.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_register_missing_job_id_ignored(self, bridge_with_db, mock_db):
        msg = make_msg({"hostname": "orphan-vm", "ip": "100.64.6.1"})

        await bridge_with_db._on_daemon_register(msg)

        mock_db.merge_vm_context_if_provision_generation.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_register_malformed_json(self, bridge_with_db, mock_db):
        msg = make_msg_raw(b"{{not json")

        await bridge_with_db._on_daemon_register(msg)

        mock_db.merge_vm_context_if_provision_generation.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_register_clears_recovering_flag(
        self, bridge_with_db, mock_db, leader
    ):
        """Registration should set recovering=False to clear any recovery guard."""
        bridge_with_db._seed_vm_ide_config = AsyncMock()
        msg = make_msg(
            {
                "job_id": "job-reg-010",
                "hostname": "vm-agent-010",
                "ip": "100.64.7.1",
                "pid": 7,
            }
        )

        await bridge_with_db._on_daemon_register(msg)
        await asyncio.sleep(0)

        updates = mock_db.merge_vm_context_if_provision_generation.call_args.args[2]
        assert updates["recovering"] is False

    @pytest.mark.asyncio
    async def test_register_neither_ip_nor_hostname(
        self, bridge_with_db, mock_db, leader
    ):
        """Missing SSH host is recorded as non-dispatchable."""
        callback = MagicMock()
        bridge_with_db._on_vm_ready = callback
        msg = make_msg(
            {
                "job_id": "job-reg-011",
                "pid": 8,
            }
        )

        await bridge_with_db._on_daemon_register(msg)

        updates = mock_db.merge_vm_context_if_provision_generation.call_args.args[2]
        assert updates["ssh_host"] is None
        assert updates["status"] == "ssh_unreachable"
        assert "SSH host" in updates["ssh_probe_error"]
        callback.assert_not_called()

    @pytest.mark.asyncio
    async def test_ssh_ready_true_without_tailnet_ip_trusted(
        self, bridge_with_db, mock_db, leader
    ):
        """An explicit daemon ssh_ready=True is trusted over IP inspection
        (the daemon's own check already requires a tailnet IP)."""
        bridge_with_db._seed_vm_ide_config = AsyncMock()
        msg = make_msg(
            {
                "job_id": "job-reg-012",
                "hostname": "vm-agent-012",
                "ip": "100.64.9.9",
                "pid": 12,
                "ssh_ready": True,
            }
        )

        await bridge_with_db._on_daemon_register(msg)
        await asyncio.sleep(0)

        updates = mock_db.merge_vm_context_if_provision_generation.call_args.args[2]
        assert updates["status"] == "ready"
        assert updates["ssh_ready_source"] == "daemon"

    @pytest.mark.asyncio
    async def test_register_ignored_on_follower(self, bridge_with_db, mock_db):
        from orchestrator.services.leader_election import is_leader

        is_leader.clear()
        callback = MagicMock()
        bridge_with_db._on_vm_ready = callback
        msg = make_msg(
            {
                "job_id": "job-reg-follower",
                "hostname": "vm-agent-follower",
                "ip": "100.64.9.1",
                "pid": 9,
                "ssh_ready": True,
            }
        )

        await bridge_with_db._on_daemon_register(msg)

        mock_db.merge_vm_context_if_provision_generation.assert_not_awaited()
        callback.assert_not_called()


# =============================================================================
# Test: thread-vs-job routing is durable, not process-local (Defect 4)
# knowledge-base/knowledge/issues/session_vm_backend_never_attaches.md
# =============================================================================


class TestVmEntityRoutingIsDurable:
    """Under HA (2 replicas), the replica that PUBLISHED vm.lifecycle.create is
    not necessarily the leader that handles the daemon's register. Routing must
    therefore come from the database, never from state left in the publishing
    process — otherwise the leader treats a thread UUID as a job and writes
    ssh_host into a jobs row that does not exist, stranding the thread at
    status='created' forever.

    Every test here uses a bridge that never published anything, which is
    exactly the non-publishing replica's situation.
    """

    @pytest.mark.asyncio
    async def test_register_routes_to_thread_on_non_publishing_replica(
        self, bridge_with_db, mock_db, leader
    ):
        tid = "77b3d3e6-6dfc-422e-a8ec-a5848cb8febc"
        mock_db.get_thread = AsyncMock(return_value={"id": tid, "user_id": "u1"})
        bridge_with_db._seed_vm_ide_config = AsyncMock()

        await bridge_with_db._on_daemon_register(
            make_msg(
                {
                    "job_id": tid,
                    "hostname": "vm-thread",
                    "ip": "100.64.1.55",
                    "pid": 44,
                    "ssh_ready": True,
                }
            )
        )

        mock_db.merge_thread_vm_context_if_provision_generation.assert_awaited()
        mock_db.merge_vm_context_if_provision_generation.assert_not_awaited()
        assert (
            mock_db.merge_thread_vm_context_if_provision_generation.call_args.args[2][
                "status"
            ]
            == "ready"
        )

    @pytest.mark.asyncio
    async def test_register_routes_to_job_when_id_is_not_a_thread(
        self, bridge_with_db, mock_db, leader
    ):
        bridge_with_db._seed_vm_ide_config = AsyncMock()

        await bridge_with_db._on_daemon_register(
            make_msg(
                {
                    "job_id": "job-durable-001",
                    "hostname": "vm-job",
                    "ip": "100.64.1.7",
                    "pid": 7,
                    "ssh_ready": True,
                }
            )
        )

        mock_db.merge_vm_context_if_provision_generation.assert_awaited()
        mock_db.merge_thread_vm_context_if_provision_generation.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_register_writes_nothing_for_unknown_entity(
        self, bridge_with_db, mock_db, leader
    ):
        """Neither a thread nor a job: writing to either table would be a guess.
        Fail safe — the wrong-table write is the bug being fixed."""
        mock_db.get_thread = AsyncMock(return_value=None)
        mock_db.get_job = AsyncMock(return_value=None)
        bridge_with_db._seed_vm_ide_config = AsyncMock()

        await bridge_with_db._on_daemon_register(
            make_msg(
                {
                    "job_id": "ghost-entity",
                    "hostname": "vm-ghost",
                    "ip": "100.64.1.9",
                    "pid": 9,
                    "ssh_ready": True,
                }
            )
        )

        mock_db.merge_thread_vm_context_if_provision_generation.assert_not_awaited()
        mock_db.merge_vm_context_if_provision_generation.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_status_routes_to_thread_on_non_publishing_replica(
        self, bridge_with_db, mock_db
    ):
        tid = "77b3d3e6-6dfc-422e-a8ec-a5848cb8febc"
        mock_db.get_thread = AsyncMock(return_value={"id": tid, "user_id": "u1"})

        await bridge_with_db._on_vm_lifecycle_status(
            make_msg({"job_id": tid, "status": "created", "vm_name": "agent-vm-x"})
        )

        mock_db.merge_thread_vm_context.assert_awaited()
        mock_db.merge_vm_context.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_heartbeat_routes_to_thread_on_non_publishing_replica(
        self, bridge_with_db, mock_db, leader
    ):
        tid = "77b3d3e6-6dfc-422e-a8ec-a5848cb8febc"
        mock_db.get_thread = AsyncMock(return_value={"id": tid, "user_id": "u1"})

        await bridge_with_db._on_daemon_heartbeat(
            make_msg(
                {
                    "job_id": tid,
                    "agent_running": True,
                    "code_server_connections": 1,
                }
            )
        )

        mock_db.merge_thread_vm_context_if_provision_generation.assert_awaited()
        mock_db.merge_vm_context_if_provision_generation.assert_not_awaited()
        mock_db.merge_thread_ide_session_context.assert_awaited_once()
        mock_db.merge_ide_session_context.assert_not_awaited()


# =============================================================================
# Test: _on_daemon_heartbeat()
# =============================================================================


@pytest.mark.usefixtures("leader")
class TestOnDaemonHeartbeat:
    """Tests for NatsBridge._on_daemon_heartbeat()."""

    @pytest.mark.asyncio
    async def test_heartbeat_updates_last_heartbeat(self, bridge_with_db, mock_db):
        msg = make_msg(
            {
                "job_id": "job-hb-001",
                "agent_pid": 5555,
                "agent_running": True,
                "cpu_percent": 45.2,
                "memory_percent": 62.1,
                "disk_percent": 30.0,
            }
        )

        await bridge_with_db._on_daemon_heartbeat(msg)

        mock_db.merge_vm_context_if_provision_generation.assert_awaited_once()
        call_args = mock_db.merge_vm_context_if_provision_generation.call_args
        assert call_args.args[0] == "job-hb-001"
        updates = call_args.args[2]
        assert "last_heartbeat" in updates
        # Should be an ISO format timestamp
        assert "T" in updates["last_heartbeat"]

    @pytest.mark.asyncio
    async def test_heartbeat_with_code_server_connections(
        self, bridge_with_db, mock_db
    ):
        """Heartbeat with code_server_connections triggers IDE session update."""
        msg = make_msg(
            {
                "job_id": "job-hb-002",
                "agent_pid": 6666,
                "agent_running": True,
                "code_server_connections": 2,
            }
        )

        await bridge_with_db._on_daemon_heartbeat(msg)

        # Should update both VM context and IDE session
        mock_db.merge_vm_context_if_provision_generation.assert_awaited_once()
        mock_db.merge_ide_session_context.assert_awaited_once()

        ide_call = mock_db.merge_ide_session_context.call_args
        assert ide_call.args[0] == "job-hb-002"
        ide_updates = ide_call.args[1]
        assert ide_updates["code_server_connections"] == 2
        assert ide_updates["status"] == "active"
        assert "last_activity" in ide_updates

    @pytest.mark.asyncio
    async def test_heartbeat_zero_code_server_connections(
        self, bridge_with_db, mock_db
    ):
        """Zero connections marks IDE session as idle."""
        msg = make_msg(
            {
                "job_id": "job-hb-003",
                "agent_pid": 7777,
                "agent_running": True,
                "code_server_connections": 0,
            }
        )

        await bridge_with_db._on_daemon_heartbeat(msg)

        ide_call = mock_db.merge_ide_session_context.call_args
        ide_updates = ide_call.args[1]
        assert ide_updates["code_server_connections"] == 0
        assert ide_updates["status"] == "idle"
        # Zero connections should NOT set last_activity
        assert "last_activity" not in ide_updates

    @pytest.mark.asyncio
    async def test_heartbeat_no_code_server_field(self, bridge_with_db, mock_db):
        """When code_server_connections is absent, IDE session is not updated."""
        msg = make_msg(
            {
                "job_id": "job-hb-004",
                "agent_pid": 8888,
                "agent_running": True,
            }
        )

        await bridge_with_db._on_daemon_heartbeat(msg)

        mock_db.merge_vm_context_if_provision_generation.assert_awaited_once()
        mock_db.merge_ide_session_context.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_heartbeat_ide_session_error_non_fatal(self, bridge_with_db, mock_db):
        """IDE session update errors should not break heartbeat processing."""
        mock_db.merge_ide_session_context = AsyncMock(
            side_effect=RuntimeError("db error")
        )

        msg = make_msg(
            {
                "job_id": "job-hb-005",
                "agent_pid": 9999,
                "agent_running": True,
                "code_server_connections": 1,
            }
        )

        # Should not raise
        await bridge_with_db._on_daemon_heartbeat(msg)

        # VM context should still be updated
        mock_db.merge_vm_context_if_provision_generation.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_heartbeat_missing_job_id(self, bridge_with_db, mock_db):
        msg = make_msg(
            {
                "agent_pid": 1111,
                "agent_running": True,
            }
        )

        await bridge_with_db._on_daemon_heartbeat(msg)

        mock_db.merge_vm_context_if_provision_generation.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_heartbeat_malformed_json(self, bridge_with_db, mock_db):
        msg = make_msg_raw(b"corrupted-heartbeat")

        await bridge_with_db._on_daemon_heartbeat(msg)

        mock_db.merge_vm_context_if_provision_generation.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_heartbeat_no_db_skips_ide_update(self, bridge, mock_nc):
        """When _db is None, IDE session update is skipped (code_server_connections path)."""
        bridge._db = None

        msg = make_msg(
            {
                "job_id": "job-hb-006",
                "agent_pid": 2222,
                "agent_running": True,
                "code_server_connections": 3,
            }
        )

        # Should not raise despite _db being None
        await bridge._on_daemon_heartbeat(msg)


# =============================================================================
# Test: _set_vm_context()
# =============================================================================


class TestSetVmContext:
    """Tests for NatsBridge._set_vm_context() helper."""

    @pytest.mark.asyncio
    async def test_set_vm_context_calls_db(self, bridge_with_db, mock_db):
        await bridge_with_db._set_vm_context("job-ctx-001", {"status": "ready"})

        mock_db.merge_vm_context.assert_awaited_once_with(
            "job-ctx-001", {"status": "ready"}
        )

    @pytest.mark.asyncio
    async def test_set_vm_context_no_db(self, bridge):
        """When _db is None, _set_vm_context is a no-op."""
        bridge._db = None
        # Should not raise
        await bridge._set_vm_context("job-ctx-002", {"status": "ready"})

    @pytest.mark.asyncio
    async def test_set_vm_context_db_error_handled(self, bridge_with_db, mock_db):
        """Database errors are caught and logged, not raised."""
        mock_db.merge_vm_context = AsyncMock(side_effect=RuntimeError("db down"))

        # Should not raise
        await bridge_with_db._set_vm_context("job-ctx-003", {"status": "ready"})


# =============================================================================
# Test: NATS connection callbacks
# =============================================================================


class TestConnectionCallbacks:
    """Tests for NATS connection event callbacks."""

    @pytest.mark.asyncio
    async def test_on_error(self, bridge):
        """_on_error logs but does not raise."""
        await bridge._on_error(RuntimeError("test error"))

    @pytest.mark.asyncio
    async def test_on_disconnect(self, bridge):
        """_on_disconnect logs but does not raise."""
        await bridge._on_disconnect()

    @pytest.mark.asyncio
    async def test_on_reconnect(self, bridge):
        """_on_reconnect logs but does not raise."""
        await bridge._on_reconnect()


# =============================================================================
# Test: Graceful degradation (NATS unavailable)
# =============================================================================


class TestGracefulDegradation:
    """Tests that all operations gracefully degrade when NATS is unavailable."""

    @pytest.mark.asyncio
    async def test_create_returns_false(self, disconnected_bridge):
        result = await disconnected_bridge.request_vm_create(job_id="job-1")
        assert result is False

    @pytest.mark.asyncio
    async def test_delete_returns_false(self, disconnected_bridge):
        result = await disconnected_bridge.request_vm_delete(job_id="job-1")
        assert result is False

    @pytest.mark.asyncio
    async def test_query_returns_none(self, disconnected_bridge):
        result = await disconnected_bridge.query_vm_status("job-1")
        assert result is None

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_all_actions_return_falsy(self, disconnected_bridge):
        """Comprehensive check: every public method returns a falsy value."""
        assert await disconnected_bridge.request_vm_create(job_id="j") is False
        assert await disconnected_bridge.request_vm_delete(job_id="j") is False
        assert await disconnected_bridge.query_vm_status("j") is None


# =============================================================================
# Test: Module-level singleton
# =============================================================================


class TestModuleSingleton:
    """Tests for the module-level nats_bridge singleton."""

    def test_singleton_exists(self):
        from orchestrator.services.nats_bridge import nats_bridge

        assert nats_bridge is not None
        # Should be a NatsBridge instance
        assert hasattr(nats_bridge, "is_available")
        assert hasattr(nats_bridge, "connect")
        assert hasattr(nats_bridge, "disconnect")


# =============================================================================
# Test: Edge cases and robustness
# =============================================================================


class TestEdgeCases:
    """Edge cases and robustness tests."""

    @pytest.mark.asyncio
    async def test_lifecycle_status_with_all_optional_fields(
        self, bridge_with_db, mock_db
    ):
        """Status message with every optional field populated."""
        msg = make_msg(
            {
                "job_id": "job-edge-001",
                "status": "running",
                "vm_name": "vm-job-edge-001",
                "namespace": "agent-vms",
                "error": "some warning",
                "pod_ip": "10.0.0.100",
                "ssh_nodeport": 31022,
            }
        )

        await bridge_with_db._on_vm_lifecycle_status(msg)

        updates = mock_db.merge_vm_context.call_args.args[1]
        assert updates["status"] == "running"
        assert updates["vm_name"] == "vm-job-edge-001"
        assert updates["namespace"] == "agent-vms"
        assert updates["error"] == "some warning"
        assert updates["pod_ip"] == "10.0.0.100"
        assert updates["ssh_nodeport"] == 31022

    @pytest.mark.asyncio
    async def test_register_with_unicode_hostname(
        self, bridge_with_db, mock_db, leader
    ):
        """Hostnames with unusual characters should be handled."""
        bridge_with_db._start_vm_ssh_probe = MagicMock()
        msg = make_msg(
            {
                "job_id": "job-edge-002",
                "hostname": "vm-agent-\u00e4\u00f6\u00fc",
                "ip": "100.64.10.1",
                "pid": 42,
            }
        )

        await bridge_with_db._on_daemon_register(msg)

        updates = mock_db.merge_vm_context_if_provision_generation.call_args.args[2]
        assert updates["hostname"] == "vm-agent-\u00e4\u00f6\u00fc"

    @pytest.mark.asyncio
    async def test_create_with_special_chars_in_description(
        self, bridge_with_db, mock_nc
    ):
        """Description with JSON-special characters must be encoded properly."""
        desc = 'Process "file.pdf" with keys: {a: 1, b: 2}\nnewline'
        await bridge_with_db.request_vm_create(
            job_id="job-edge-003",
            description=desc,
        )

        payload = json.loads(mock_nc.publish.call_args.args[1].decode())
        assert payload["description"] == desc

    @pytest.mark.asyncio
    async def test_heartbeat_with_none_code_server_connections(
        self, bridge_with_db, mock_db, leader
    ):
        """Explicit null for code_server_connections should not update IDE session.

        The code checks `if cs_connections is not None`, so None should skip.
        """
        msg = make_msg(
            {
                "job_id": "job-edge-004",
                "agent_pid": 100,
                "agent_running": True,
                "code_server_connections": None,
            }
        )

        await bridge_with_db._on_daemon_heartbeat(msg)

        mock_db.merge_vm_context_if_provision_generation.assert_awaited_once()
        mock_db.merge_ide_session_context.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_db_merge_vm_context_error_in_lifecycle_status(
        self, bridge_with_db, mock_db
    ):
        """DB errors in lifecycle status handler should be caught."""
        mock_db.merge_vm_context = AsyncMock(side_effect=RuntimeError("db gone"))

        msg = make_msg(
            {
                "job_id": "job-edge-005",
                "status": "running",
                "vm_name": "vm-edge",
                "namespace": "ns",
            }
        )

        # Should not raise — the handler has its own try/except
        await bridge_with_db._on_vm_lifecycle_status(msg)

    @pytest.mark.asyncio
    async def test_db_merge_vm_context_error_in_register(
        self, bridge_with_db, mock_db, leader
    ):
        """DB errors in daemon register handler should be caught."""
        mock_db.merge_vm_context_if_provision_generation = AsyncMock(
            side_effect=RuntimeError("db gone")
        )
        bridge_with_db._start_vm_ssh_probe = MagicMock()

        msg = make_msg(
            {
                "job_id": "job-edge-006",
                "hostname": "vm-edge",
                "ip": "100.64.99.1",
                "pid": 1,
            }
        )

        # The outer try/except in _on_daemon_register catches this
        await bridge_with_db._on_daemon_register(msg)

    @pytest.mark.asyncio
    async def test_large_payload_in_status(self, bridge_with_db, mock_db):
        """Handle a status message with extra unknown fields gracefully."""
        msg = make_msg(
            {
                "job_id": "job-edge-007",
                "status": "running",
                "vm_name": "vm-edge",
                "namespace": "ns",
                "extra_field_1": "ignored",
                "extra_field_2": {"nested": True},
                "extra_field_3": [1, 2, 3],
            }
        )

        await bridge_with_db._on_vm_lifecycle_status(msg)

        # Only known fields should be in the update
        updates = mock_db.merge_vm_context.call_args.args[1]
        assert "extra_field_1" not in updates
        assert "extra_field_2" not in updates
        assert "extra_field_3" not in updates

    @pytest.mark.asyncio
    async def test_concurrent_create_and_delete(self, bridge_with_db, mock_nc, mock_db):
        """Create and delete for different jobs should both succeed."""
        import asyncio

        result_create, result_delete = await asyncio.gather(
            bridge_with_db.request_vm_create(job_id="job-A"),
            bridge_with_db.request_vm_delete(job_id="job-B"),
        )

        assert result_create is True
        assert result_delete is True
        assert mock_nc.publish.await_count == 2

    @pytest.mark.asyncio
    async def test_empty_string_job_id_in_register(self, bridge_with_db, mock_db):
        """Empty string job_id is falsy, should be treated as missing."""
        msg = make_msg(
            {
                "job_id": "",
                "hostname": "vm-agent",
                "ip": "100.64.1.1",
            }
        )

        await bridge_with_db._on_daemon_register(msg)

        # Empty string is falsy in Python, so `if not job_id: return`
        mock_db.merge_vm_context.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_heartbeat_negative_code_server_connections(
        self, bridge_with_db, mock_db, leader
    ):
        """Negative connection count (edge case) should still be processed.

        The code does `if cs_connections > 0` so negative should go to idle.
        """
        msg = make_msg(
            {
                "job_id": "job-edge-008",
                "agent_pid": 200,
                "agent_running": True,
                "code_server_connections": -1,
            }
        )

        await bridge_with_db._on_daemon_heartbeat(msg)

        ide_call = mock_db.merge_ide_session_context.call_args
        ide_updates = ide_call.args[1]
        assert ide_updates["status"] == "idle"
        assert ide_updates["code_server_connections"] == -1


# =============================================================================
# Test: per-orchestrator subject scoping (vm.lifecycle.*)
# =============================================================================


class TestSubjectScoping:
    """ORCHESTRATOR_ID env var scopes vm.lifecycle.* subjects so multiple
    orchestrators can safely share a NATS hub.

    Failsafe behavior: if NATS_URL is set but ORCHESTRATOR_ID is unset, the
    bridge refuses to publish/subscribe rather than fall back to flat
    subjects (which would re-introduce cross-talk).
    """

    def test_init_reads_orchestrator_id(self, mock_nats_module, monkeypatch):
        from orchestrator.services.nats_bridge import NatsBridge

        monkeypatch.setenv("ORCHESTRATOR_ID", "srw-dev")
        b = NatsBridge(url="nats://test:4222")
        assert b._orchestrator_id == "srw-dev"

    def test_init_orchestrator_id_blank_when_unset(self, mock_nats_module, monkeypatch):
        from orchestrator.services.nats_bridge import NatsBridge

        monkeypatch.delenv("ORCHESTRATOR_ID", raising=False)
        b = NatsBridge(url="nats://test:4222")
        assert b._orchestrator_id == ""

    def test_init_orchestrator_id_stripped(self, mock_nats_module, monkeypatch):
        from orchestrator.services.nats_bridge import NatsBridge

        monkeypatch.setenv("ORCHESTRATOR_ID", "  srw-dev  \n")
        b = NatsBridge(url="nats://test:4222")
        assert b._orchestrator_id == "srw-dev"

    def test_subj_returns_scoped_subject(self, bridge):
        assert bridge._subj("vm.lifecycle.create") == "vm.lifecycle.create.srw-test"
        assert bridge._subj("vm.lifecycle.status") == "vm.lifecycle.status.srw-test"

    def test_subj_returns_none_when_id_unset(self, unscoped_bridge):
        assert unscoped_bridge._subj("vm.lifecycle.create") is None

    @pytest.mark.asyncio
    async def test_create_refused_when_id_unset(self, unscoped_bridge, mock_nc):
        result = await unscoped_bridge.request_vm_create(job_id="job-x")
        assert result is False
        mock_nc.publish.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_delete_refused_when_id_unset(self, unscoped_bridge, mock_nc):
        result = await unscoped_bridge.request_vm_delete(job_id="job-x")
        assert result is False
        mock_nc.publish.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_query_refused_when_id_unset(self, unscoped_bridge, mock_nc):
        result = await unscoped_bridge.query_vm_status("job-x")
        assert result is None
        mock_nc.request.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_connect_skips_all_subscribes_when_id_unset(
        self, mock_nats_module, mock_db, monkeypatch
    ):
        """When ORCHESTRATOR_ID is unset, ALL broadcast subscribes are
        skipped — the bridge refuses to fall back to flat subjects which
        would cross-talk on a shared NATS hub."""
        from orchestrator.services.nats_bridge import NatsBridge

        mock_nc = AsyncMock()
        mock_nc.subscribe = AsyncMock()
        mock_nats_module.connect = AsyncMock(return_value=mock_nc)

        monkeypatch.delenv("ORCHESTRATOR_ID", raising=False)
        b = NatsBridge(url="nats://test:4222")

        mock_sudo_module = MagicMock()
        with patch.dict(
            "sys.modules", {"orchestrator.services.sudo_gate": mock_sudo_module}
        ):
            await b.connect(db=mock_db)

        # No subscribes at all — neither scoped nor flat.
        mock_nc.subscribe.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_create_does_not_publish_to_flat_subject(self, bridge, mock_nc):
        await bridge.request_vm_create(job_id="job-regression")
        subject = mock_nc.publish.call_args.args[0]
        assert subject == "vm.lifecycle.create.srw-test"
        assert subject != "vm.lifecycle.create"

    @pytest.mark.asyncio
    async def test_delete_does_not_publish_to_flat_subject(self, bridge, mock_nc):
        await bridge.request_vm_delete(job_id="job-regression")
        subject = mock_nc.publish.call_args.args[0]
        assert subject != "vm.lifecycle.delete"

    @pytest.mark.asyncio
    async def test_query_does_not_request_flat_subject(self, bridge, mock_nc):
        mock_nc.request = AsyncMock(
            return_value=MagicMock(data=json.dumps({"status": "x"}).encode())
        )
        await bridge.query_vm_status("job-regression")
        subject = mock_nc.request.call_args.args[0]
        assert subject != "vm.lifecycle.get"

    def test_compose_dev_path_silent_when_both_unset(
        self, mock_nats_module, monkeypatch
    ):
        """Compose dev with no NATS_URL and no ORCHESTRATOR_ID is a silent
        no-op, same as before this change (graceful degradation preserved)."""
        from orchestrator.services.nats_bridge import NatsBridge

        monkeypatch.delenv("NATS_URL", raising=False)
        monkeypatch.delenv("ORCHESTRATOR_ID", raising=False)
        b = NatsBridge()
        assert b._url is None
        assert b._orchestrator_id == ""
        assert b.is_available is False


# =============================================================================
# request_vm_list() — orphan-sweep inventory over NATS
# =============================================================================


class TestRequestVmList:
    """Tests for NatsBridge.request_vm_list()."""

    @staticmethod
    def _reply(data: dict) -> MagicMock:
        msg = MagicMock()
        msg.data = json.dumps(data).encode()
        return msg

    @pytest.mark.asyncio
    async def test_returns_vm_list(self, bridge, mock_nc):
        vms = [{"vm_name": "agent-vm-j1", "entity_id": "j1"}]
        mock_nc.request = AsyncMock(return_value=self._reply({"vms": vms}))

        assert await bridge.request_vm_list() == vms
        call_args = mock_nc.request.call_args
        assert call_args.args[0] == "vm.lifecycle.list.srw-test"
        assert json.loads(call_args.args[1].decode()) == {"orchestrator_id": "srw-test"}

    @pytest.mark.asyncio
    async def test_timeout_returns_none(self, bridge, mock_nc):
        # Includes the old-controller case: no subscriber on the list subject.
        mock_nc.request = AsyncMock(side_effect=TimeoutError("no responders"))
        assert await bridge.request_vm_list() is None

    @pytest.mark.asyncio
    async def test_unavailable_returns_none(self, disconnected_bridge):
        assert await disconnected_bridge.request_vm_list() is None

    @pytest.mark.asyncio
    async def test_jetstream_ack_returns_none(self, bridge, mock_nc):
        # A stream shadowing the request subject answers with its publish-ack.
        mock_nc.request = AsyncMock(
            return_value=self._reply({"stream": "vm-events", "seq": 42})
        )
        assert await bridge.request_vm_list() is None

    @pytest.mark.asyncio
    async def test_list_failed_reply_returns_none(self, bridge, mock_nc):
        mock_nc.request = AsyncMock(
            return_value=self._reply({"status": "list_failed", "error": "boom"})
        )
        assert await bridge.request_vm_list() is None


class TestRootdiskDispositionIsRecorded:
    """The controller's answer, not the orchestrator's intent.

    A controller without VM_PERSISTENT_ROOTDISK cascade-deletes the disk
    whatever purge_disk said, so context.vm.rootdisk must reflect what came
    back — the kept-disk GC sweep keys off it.
    knowledge-base/knowledge/features/vm_persistent_rootdisk.md D2/D4.
    """

    @pytest.mark.asyncio
    async def test_kept_disk_is_persisted(self, bridge_with_db, mock_db):
        mock_db.get_thread = AsyncMock(return_value=None)
        mock_db.get_job = AsyncMock(return_value={"id": "j1"})

        await bridge_with_db._on_vm_lifecycle_status(
            make_msg({"job_id": "j1", "status": "deleted", "rootdisk": "kept"})
        )

        ctx = mock_db.merge_vm_context.await_args.args[1]
        assert ctx["rootdisk"] == "kept"

    @pytest.mark.asyncio
    async def test_purged_disk_overwrites_an_optimistic_keep(
        self, bridge_with_db, mock_db
    ):
        mock_db.get_thread = AsyncMock(return_value=None)
        mock_db.get_job = AsyncMock(return_value={"id": "j1"})

        await bridge_with_db._on_vm_lifecycle_status(
            make_msg({"job_id": "j1", "status": "deleted", "rootdisk": "purged"})
        )

        ctx = mock_db.merge_vm_context.await_args.args[1]
        assert ctx["rootdisk"] == "purged"

    @pytest.mark.asyncio
    async def test_old_controller_silence_is_reported(
        self, bridge_with_db, mock_db, caplog
    ):
        mock_db.get_thread = AsyncMock(return_value=None)
        mock_db.get_job = AsyncMock(return_value={"id": "j1"})

        with caplog.at_level("WARNING"):
            await bridge_with_db._on_vm_lifecycle_status(
                make_msg({"job_id": "j1", "status": "deleted"})
            )

        assert "no rootdisk disposition" in caplog.text
        ctx = mock_db.merge_vm_context.await_args.args[1]
        assert "rootdisk" not in ctx

    @pytest.mark.asyncio
    async def test_non_delete_status_is_untouched(
        self, bridge_with_db, mock_db, caplog
    ):
        """created/failed statuses say nothing about disks — no key, no noise."""
        mock_db.get_thread = AsyncMock(return_value=None)
        mock_db.get_job = AsyncMock(return_value={"id": "j1"})

        with caplog.at_level("WARNING"):
            await bridge_with_db._on_vm_lifecycle_status(
                make_msg({"job_id": "j1", "status": "created"})
            )

        assert "rootdisk" not in caplog.text
        ctx = mock_db.merge_vm_context.await_args.args[1]
        assert "rootdisk" not in ctx
