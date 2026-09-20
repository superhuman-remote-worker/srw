"""Tests for the VM Provisioner module (orchestrator/services/vm_provisioner.py).

Tests cover explicit VM mode selection and the external/HTTP lifecycle paths.
"""

import asyncio
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

PROVISION_GENERATION = "00000000-0000-4000-8000-000000000001"
LAUNCHER_POD_UID = "00000000-0000-4000-8000-000000000002"
TEST_HOST_KEY_FINGERPRINT = "SHA256:" + ("A" * 43)


def _ready_vm_context(**overrides):
    context = {
        "provision_generation": PROVISION_GENERATION,
        "identity_provision_generation": PROVISION_GENERATION,
        "identity_authenticated": True,
        "vm_uid": "captured-vm-uid",
        "rootdisk_pvc_uid": "captured-root-uid",
        "ssh_host": "100.64.1.9",
        "ssh_port": 22,
        "ssh_host_key_fingerprint": TEST_HOST_KEY_FINGERPRINT,
    }
    context.update(overrides)
    return context


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def mock_nats_bridge():
    """Create a mock nats_bridge with controllable is_available."""
    bridge = MagicMock()
    bridge.is_available = False
    bridge.lifecycle_identity_authenticated = True
    bridge.request_vm_create = AsyncMock(return_value=True)
    bridge.request_vm_delete = AsyncMock(return_value=True)
    bridge.query_vm_status = AsyncMock(
        return_value={"job_id": "test", "status": "running"}
    )
    return bridge


@pytest.fixture
def mock_db():
    """Create a mock PostgresDB instance."""
    db = MagicMock()
    db.merge_vm_context = AsyncMock()
    db.merge_thread_vm_context = AsyncMock()
    db.begin_pinned_thread_vm_provisioning = AsyncMock(return_value=True)
    db.merge_vm_context_if_provision_generation = AsyncMock(return_value=True)
    db.merge_thread_vm_context_if_provision_generation = AsyncMock(return_value=True)
    db.managed_repository_workspace_process_zero_is_current = AsyncMock(
        return_value=True
    )
    db.claim_managed_repository_workspace_retirement = AsyncMock(return_value=True)
    db.record_managed_repository_workspace_process_zero = AsyncMock(return_value=True)
    db.get_job = AsyncMock(return_value={"context": {"vm": _ready_vm_context()}})
    db.get_thread = AsyncMock(return_value={"metadata": {"vm": _ready_vm_context()}})
    return db


@pytest.fixture
def provisioner_with_nats(mock_nats_bridge, mock_db):
    """Create a VMProvisioner configured for NATS mode."""
    with (
        patch.dict(os.environ, {"VM_MODE": "external"}),
        patch("orchestrator.services.vm_provisioner.nats_bridge", mock_nats_bridge),
    ):
        mock_nats_bridge.is_available = True
        from orchestrator.services.vm_provisioner import VMProvisioner

        prov = VMProvisioner()
        prov._db = mock_db

        async def _query_exact_vm(*_args, **_kwargs):
            delete_call = mock_nats_bridge.request_vm_delete.await_args
            deleted = (
                delete_call is not None
                and mock_nats_bridge.request_vm_delete.return_value is not False
            )
            purge_disk = bool(
                delete_call is None or delete_call.kwargs.get("purge_disk", True)
            )
            return {
                "_identity_authenticated": True,
                "status": "not_found" if deleted else "running",
                "provision_generation": PROVISION_GENERATION,
                "vm_uid": None if deleted else "captured-vm-uid",
                "rootdisk_pvc_uid": (
                    None if deleted and purge_disk else "captured-root-uid"
                ),
                "rootdisk_identity_known": True,
                "credential_runtime_started": False,
            }

        mock_nats_bridge.query_vm_status.side_effect = _query_exact_vm
        yield prov


@pytest.fixture
def provisioner_disabled():
    """Create a VMProvisioner with no backend available."""
    with (
        patch.dict(os.environ, {"VM_MODE": "off"}),
        patch("orchestrator.services.vm_provisioner.nats_bridge") as nb,
    ):
        nb.is_available = False
        from orchestrator.services.vm_provisioner import VMProvisioner

        prov = VMProvisioner()
        prov._db = None
        yield prov


@pytest.fixture
def provisioner_with_db(mock_db):
    with patch.dict(os.environ, {"VM_MODE": "off"}):
        from orchestrator.services.vm_provisioner import VMProvisioner

        prov = VMProvisioner()
        prov._db = mock_db
        yield prov


# =============================================================================
# Test: explicit mode selection
# =============================================================================
class TestBackendSelection:
    @pytest.mark.parametrize("mode", ["off", "same-cluster", "external"])
    def test_mode_uses_vm_mode_only(self, mode, mock_nats_bridge):
        mock_nats_bridge.is_available = True
        with (
            patch.dict(
                os.environ,
                {"VM_MODE": mode, "VM_CONTROLLER_URL": "http://controller"},
            ),
            patch("orchestrator.services.vm_provisioner.nats_bridge", mock_nats_bridge),
        ):
            from orchestrator.services.vm_provisioner import VMProvisioner

            provisioner = VMProvisioner()
            assert provisioner.mode == mode
            assert provisioner.is_available is (mode == "same-cluster")
            if mode == "external":
                assert "authenticated guest-management transport" in str(
                    provisioner.unavailable_reason
                )

    def test_unset_mode_is_off_and_warns_once(self, caplog):
        import orchestrator.services.vm_provisioner as vm_module

        vm_module._warned_unset_vm_mode = False
        with patch.dict(os.environ, {}, clear=True):
            provisioner = vm_module.VMProvisioner()
            assert provisioner.mode == "off"
            assert provisioner.mode == "off"
        assert caplog.text.count("VM_MODE is unset") == 1

    def test_invalid_mode_is_off_and_warns_once(self, caplog):
        import orchestrator.services.vm_provisioner as vm_module

        vm_module._warned_invalid_vm_mode = False
        with patch.dict(os.environ, {"VM_MODE": "bogus"}, clear=True):
            provisioner = vm_module.VMProvisioner()
            assert provisioner.mode == "off"
            assert provisioner.mode == "off"
        assert caplog.text.count("Invalid VM_MODE") == 1

    def test_same_cluster_requires_controller_url(self):
        with patch.dict(os.environ, {"VM_MODE": "same-cluster"}, clear=True):
            from orchestrator.services.vm_provisioner import VMProvisioner

            assert VMProvisioner().is_available is False

    def test_external_is_contained_even_when_nats_is_connected(self, mock_nats_bridge):
        mock_nats_bridge.is_available = True
        with (
            patch.dict(os.environ, {"VM_MODE": "external"}, clear=True),
            patch("orchestrator.services.vm_provisioner.nats_bridge", mock_nats_bridge),
        ):
            from orchestrator.services.vm_provisioner import VMProvisioner

            assert VMProvisioner().is_available is False

    @pytest.mark.asyncio
    async def test_external_missing_lifecycle_secret_refuses_list_and_delete(
        self, mock_nats_bridge, mock_db
    ):
        mock_nats_bridge.is_available = True
        mock_nats_bridge.lifecycle_identity_authenticated = False
        with (
            patch.dict(os.environ, {"VM_MODE": "external"}, clear=True),
            patch("orchestrator.services.vm_provisioner.nats_bridge", mock_nats_bridge),
        ):
            from orchestrator.services.vm_provisioner import VMProvisioner

            provisioner = VMProvisioner()
            provisioner._db = mock_db

            assert provisioner.lifecycle_available is False
            assert await provisioner.list_vms() is None
            assert await provisioner.delete_vm("job-unsigned") is False

        mock_nats_bridge.query_vm_status.assert_not_awaited()
        mock_nats_bridge.request_vm_delete.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_same_cluster_does_not_use_nats(self, mock_nats_bridge):
        mock_nats_bridge.is_available = True
        response = MagicMock()
        response.raise_for_status = MagicMock()
        response.json.return_value = {"status": "created"}
        with (
            patch.dict(
                os.environ,
                {"VM_MODE": "same-cluster", "VM_CONTROLLER_URL": "http://controller"},
            ),
            patch("orchestrator.services.vm_provisioner.nats_bridge", mock_nats_bridge),
        ):
            from orchestrator.services.vm_provisioner import VMProvisioner

            provisioner = VMProvisioner()
            provisioner._http_client = MagicMock()
            provisioner._http_client.post = AsyncMock(return_value=response)
            assert await provisioner.create_vm("job-http")
        mock_nats_bridge.request_vm_create.assert_not_awaited()


def test_connect_logs_mode(mock_db, caplog):
    with (
        patch.dict(os.environ, {"VM_MODE": "off"}),
        caplog.at_level("INFO", logger="orchestrator.services.vm_provisioner"),
    ):
        from orchestrator.services.vm_provisioner import VMProvisioner

        provisioner = VMProvisioner()
        provisioner.connect(mock_db)
    assert provisioner._db is mock_db
    assert "VM_MODE=off" in caplog.text


# =============================================================================
# Test: exact same-cluster runtime attestation
# =============================================================================


class TestVmWorkspaceRuntimeAttestation:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "observation,code",
        [
            ({"ready": False}, "workspace_runtime_not_ready"),
            (
                {"active_pod_uid": "00000000-0000-4000-8000-000000000099"},
                "workspace_replacement_observed",
            ),
            ({"vm_uid": "replacement-vm-uid"}, "workspace_replacement_observed"),
            (
                {"provision_generation": "00000000-0000-4000-8000-000000000099"},
                "workspace_replacement_observed",
            ),
            ({"ready": None}, None),
            ({"active_pod_uid": None}, None),
            ({"provision_generation": None}, None),
            ({"vm_uid": None}, None),
        ],
    )
    async def test_only_explicit_runtime_observations_carry_recovery_codes(
        self, mock_db, observation, code
    ):
        from orchestrator.services.container_provisioner import (
            WorkspaceRuntimeAuthorityError,
        )

        with patch.dict(os.environ, {"VM_MODE": "same-cluster"}):
            provisioner = self._provisioner(
                mock_db, self._context(), self._status(**observation)
            )
            with pytest.raises(WorkspaceRuntimeAuthorityError) as caught:
                await provisioner.attest_workspace_runtime("job-1")
        assert getattr(caught.value, "recovery_code", None) == code

    @staticmethod
    def _context(**overrides):
        context = {
            "status": "ready",
            "provision_generation": PROVISION_GENERATION,
            "identity_authenticated": True,
            "identity_provision_generation": PROVISION_GENERATION,
            "vm_uid": "admitted-vm-uid",
            "active_pod_uid": LAUNCHER_POD_UID,
            "ssh_host_key_fingerprint": TEST_HOST_KEY_FINGERPRINT,
            "ssh_ready_source": "provisioner_probe",
            "ssh_registration_id": "registration-1",
            "ssh_host": "10.42.1.23",
            "ssh_port": 22,
        }
        context.update(overrides)
        return context

    @staticmethod
    def _status(**overrides):
        status = {
            "_identity_authenticated": True,
            "provision_generation": PROVISION_GENERATION,
            "vm_uid": "admitted-vm-uid",
            "active_pod_uid": LAUNCHER_POD_UID,
            "pod_ip": "10.42.1.23",
            "ready": True,
            "phase": "Running",
        }
        status.update(overrides)
        return status

    @staticmethod
    def _provisioner(mock_db, context, status):
        from orchestrator.services.vm_provisioner import VMProvisioner

        provisioner = VMProvisioner()
        provisioner._controller_url = "http://vm-controller:8080"
        provisioner._http_client = MagicMock()
        provisioner._lifecycle_hmac_secret = b"attestation-secret-at-least-32-bytes"
        provisioner._db = mock_db
        mock_db.get_job.return_value = {"context": {"vm": context}}
        provisioner._query_http = AsyncMock(return_value=status)
        return provisioner

    @pytest.mark.asyncio
    async def test_matching_live_incarnation_returns_endpoint_pair_and_pin(
        self, mock_db
    ):
        with patch.dict(os.environ, {"VM_MODE": "same-cluster"}):
            provisioner = self._provisioner(
                mock_db,
                self._context(),
                self._status(),
            )
            attested = await provisioner.attest_workspace_runtime("job-1")

        assert attested.backing_id == f"k8s-vmi:{LAUNCHER_POD_UID}"
        assert attested.workspace_generation == PROVISION_GENERATION
        assert attested.runtime_incarnation == LAUNCHER_POD_UID
        assert attested.ssh_host_key_fingerprint == TEST_HOST_KEY_FINGERPRINT
        assert attested.host == "10.42.1.23"
        assert attested.pod_ip == "10.42.1.23"
        assert attested.port == 22
        provisioner._query_http.assert_awaited_once_with(
            "job-1",
            provision_generation=PROVISION_GENERATION,
        )
        assert mock_db.get_job.await_count == 2

    @pytest.mark.asyncio
    async def test_thread_runtime_uses_thread_owner_and_exact_second_reread(
        self, mock_db
    ):
        context = self._context()
        mock_db.get_thread = AsyncMock(
            side_effect=[
                {"metadata": {"vm": dict(context)}},
                {"metadata": {"vm": dict(context)}},
            ]
        )
        with patch.dict(os.environ, {"VM_MODE": "same-cluster"}):
            provisioner = self._provisioner(mock_db, context, self._status())
            attested = await provisioner.attest_workspace_runtime(
                "thread-1", entity_type="thread"
            )

        assert attested.runtime_incarnation == LAUNCHER_POD_UID
        assert attested.ssh_host_key_fingerprint == TEST_HOST_KEY_FINGERPRINT
        assert mock_db.get_thread.await_count == 2

    @pytest.mark.asyncio
    async def test_successor_after_controller_probe_is_refused_before_return(
        self, mock_db
    ):
        from orchestrator.services.container_provisioner import (
            WorkspaceRuntimeAuthorityError,
        )

        current = self._context()
        successor = self._context(
            active_pod_uid="00000000-0000-4000-8000-000000000099",
            ssh_host="10.42.1.23",
        )
        mock_db.get_job = AsyncMock(
            side_effect=[
                {"context": {"vm": current}},
                {"context": {"vm": successor}},
            ]
        )
        with patch.dict(os.environ, {"VM_MODE": "same-cluster"}):
            provisioner = self._provisioner(mock_db, current, self._status())
            mock_db.get_job.side_effect = [
                {"context": {"vm": current}},
                {"context": {"vm": successor}},
            ]
            with pytest.raises(
                WorkspaceRuntimeAuthorityError, match="authority changed"
            ):
                await provisioner.attest_workspace_runtime("job-1")

    @pytest.mark.asyncio
    async def test_external_runtime_uses_generation_bound_daemon_endpoint(
        self, mock_db
    ):
        context = self._context(
            ssh_host="192.0.2.44",
            ssh_port=2202,
        )
        mock_db.get_job = AsyncMock(
            side_effect=[
                {"context": {"vm": dict(context)}},
                {"context": {"vm": dict(context)}},
            ]
        )
        with (
            patch.dict(os.environ, {"VM_MODE": "external"}),
            patch("orchestrator.services.vm_provisioner.nats_bridge") as bridge,
        ):
            bridge.is_available = True
            bridge.lifecycle_identity_authenticated = True
            bridge.query_vm_status = AsyncMock(return_value=self._status())
            from orchestrator.services.vm_provisioner import VMProvisioner

            provisioner = VMProvisioner()
            provisioner._db = mock_db
            provisioner._lifecycle_hmac_secret = (
                b"external-attestation-secret-at-least-32-bytes"
            )
            attested = await provisioner.attest_workspace_runtime("job-1")

        assert attested.host == "192.0.2.44"
        assert attested.port == 2202
        assert attested.pod_ip == "10.42.1.23"
        assert attested.runtime_incarnation == LAUNCHER_POD_UID

    @pytest.mark.asyncio
    async def test_launcher_pod_uid_mismatch_is_refused(self, mock_db):
        from orchestrator.services.container_provisioner import (
            WorkspaceRuntimeAuthorityError,
        )

        with patch.dict(os.environ, {"VM_MODE": "same-cluster"}):
            provisioner = self._provisioner(
                mock_db,
                self._context(),
                self._status(active_pod_uid="00000000-0000-4000-8000-000000000003"),
            )
            with pytest.raises(WorkspaceRuntimeAuthorityError, match="Pod UID changed"):
                await provisioner.attest_workspace_runtime("job-1")

    @pytest.mark.asyncio
    async def test_provision_generation_mismatch_is_refused(self, mock_db):
        from orchestrator.services.container_provisioner import (
            WorkspaceRuntimeAuthorityError,
        )

        with patch.dict(os.environ, {"VM_MODE": "same-cluster"}):
            provisioner = self._provisioner(
                mock_db,
                self._context(),
                self._status(
                    provision_generation=("00000000-0000-4000-8000-000000000004")
                ),
            )
            with pytest.raises(
                WorkspaceRuntimeAuthorityError, match="generation changed"
            ):
                await provisioner.attest_workspace_runtime("job-1")

    @pytest.mark.asyncio
    async def test_unauthenticated_context_is_refused(self, mock_db):
        from orchestrator.services.container_provisioner import (
            WorkspaceRuntimeAuthorityError,
        )

        with patch.dict(os.environ, {"VM_MODE": "same-cluster"}):
            provisioner = self._provisioner(
                mock_db,
                self._context(identity_authenticated=False),
                self._status(),
            )
            with pytest.raises(WorkspaceRuntimeAuthorityError, match="unauthenticated"):
                await provisioner.attest_workspace_runtime("job-1")
        provisioner._query_http.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "fingerprint",
        [None, "SHA256:not-a-canonical-fingerprint"],
        ids=["missing", "malformed"],
    )
    async def test_missing_or_malformed_pin_is_refused(self, mock_db, fingerprint):
        from orchestrator.services.container_provisioner import (
            WorkspaceRuntimeAuthorityError,
        )

        with patch.dict(os.environ, {"VM_MODE": "same-cluster"}):
            provisioner = self._provisioner(
                mock_db,
                self._context(ssh_host_key_fingerprint=fingerprint),
                self._status(),
            )
            with pytest.raises(
                WorkspaceRuntimeAuthorityError, match="fingerprint is malformed"
            ):
                await provisioner.attest_workspace_runtime("job-1")
        provisioner._query_http.assert_not_awaited()


# =============================================================================
# Test: create_vm()
# =============================================================================


class TestCreateVm:
    """Tests for create_vm() across both backends."""

    @pytest.mark.asyncio
    async def test_create_vm_external_mode_is_contained_before_state_or_nats(
        self, provisioner_with_nats, mock_nats_bridge
    ):
        """Broker reachability is not authenticated guest-management authority."""
        result = await provisioner_with_nats.create_vm(
            job_id="job-001",
            agent_config="developer",
            vm_image="my-image:v1",
            cpu_cores=4,
            memory="8Gi",
            description="Build feature",
        )
        assert result is False
        assert provisioner_with_nats.is_available is False
        assert provisioner_with_nats.lifecycle_available is True
        mock_nats_bridge.request_vm_create.assert_not_awaited()
        provisioner_with_nats._db.merge_vm_context.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_http_create_carries_thread_owner_kind(self, provisioner_disabled):
        response = MagicMock()
        response.raise_for_status = MagicMock()
        response.json.return_value = {
            "status": "created",
            "vm_name": "agent-vm-thread-1",
            "vm_uid": "http-admitted-vm-uid",
            "rootdisk_pvc_uid": "http-root-pvc-uid",
            "namespace": "agent-vms",
            "pod_ip": "10.42.0.18",
            "ready": True,
            "phase": "Running",
            "active_pod_uid": "launcher-uid-1",
        }
        provisioner_disabled._set_thread_vm_context = AsyncMock()
        provisioner_disabled._db = MagicMock()
        provisioner_disabled._db.get_workspace_network_tier = AsyncMock(
            return_value="home-allowed"
        )
        provisioner_disabled._http_client = MagicMock()
        provisioner_disabled._http_client.post = AsyncMock(return_value=response)

        result = await provisioner_disabled._create_http(
            job_id="thread-1",
            agent_config="worker_base",
            vm_image=None,
            cpu_cores=8,
            memory="16Gi",
            description="",
            entity_type="thread",
        )
        assert result["pod_ip"] == "10.42.0.18"
        assert result["ready"] is True
        assert result["phase"] == "Running"
        assert result["active_pod_uid"] == "launcher-uid-1"

        payload = provisioner_disabled._http_client.post.await_args.kwargs["json"]
        assert payload["job_id"] == "thread-1"
        assert payload["entity_type"] == "thread"
        assert payload["network_tier"] == "home-allowed"
        # An unsigned legacy controller response remains operational telemetry,
        # but its reusable names/UID strings are not authoritative identity.
        created_updates = provisioner_disabled._set_thread_vm_context.await_args_list[
            -1
        ].args[1]
        assert created_updates["status"] == "created"
        assert "vm_uid" not in created_updates
        assert "rootdisk_pvc_uid" not in created_updates
        assert "pod_ip" not in created_updates
        assert "active_pod_uid" not in created_updates

    @pytest.mark.asyncio
    async def test_http_create_forwards_waiting_capacity(self, provisioner_disabled):
        response = MagicMock()
        response.raise_for_status = MagicMock()
        response.json.return_value = {
            "status": "waiting_capacity",
            "running_vms": 4,
            "max_concurrent_vms": 4,
        }
        provisioner_disabled._set_vm_context = AsyncMock()
        provisioner_disabled._http_client = MagicMock()
        provisioner_disabled._http_client.post = AsyncMock(return_value=response)

        result = await provisioner_disabled._create_http(
            job_id="job-capacity",
            agent_config="worker_base",
            vm_image=None,
            cpu_cores=8,
            memory="16Gi",
            description="",
        )

        assert result["status"] == "waiting_capacity"
        updates = provisioner_disabled._set_vm_context.await_args_list[-1].args[1]
        assert updates["running_vms"] == 4
        assert updates["max_concurrent_vms"] == 4

    @pytest.mark.asyncio
    async def test_authenticated_create_durably_merges_controller_host_key_pin(
        self, provisioner_disabled, mock_db
    ):
        from orchestrator.services.vm_lifecycle_auth import AUTH_FIELD, sign_payload

        secret = b"http-create-host-key-test-secret-at-least-32-bytes"
        provisioner_disabled._db = mock_db
        provisioner_disabled._lifecycle_hmac_secret = secret
        mock_db.get_workspace_network_tier = AsyncMock(return_value="internet-only")

        async def _post(_path, *, json):
            request_auth = json[AUTH_FIELD]
            response = MagicMock(status_code=200)
            response.raise_for_status = MagicMock()
            response.json.return_value = sign_payload(
                {
                    "job_id": "job-host-key",
                    "status": "created",
                    "vm_name": "agent-vm-job-host-key",
                    "vm_uid": "admitted-vm-uid",
                    "namespace": "agent-vms",
                    "provision_generation": PROVISION_GENERATION,
                    "ssh_host_key_fingerprint": TEST_HOST_KEY_FINGERPRINT,
                },
                direction="response",
                operation="create",
                secret=secret,
                correlation_id=request_auth["request_id"],
            )
            return response

        client = MagicMock()
        client.post = AsyncMock(side_effect=_post)
        provisioner_disabled._http_client = client

        result = await provisioner_disabled._create_http(
            job_id="job-host-key",
            agent_config="worker_base",
            vm_image=None,
            cpu_cores=8,
            memory="16Gi",
            description="",
            provision_generation=PROVISION_GENERATION,
        )

        assert result["ssh_host_key_fingerprint"] == TEST_HOST_KEY_FINGERPRINT
        updates = mock_db.merge_vm_context_if_provision_generation.await_args_list[
            -1
        ].args[2]
        assert updates["vm_uid"] == "admitted-vm-uid"
        assert updates["ssh_host_key_fingerprint"] == TEST_HOST_KEY_FINGERPRINT
        assert updates["identity_authenticated"] is True

    @pytest.mark.asyncio
    async def test_create_vm_returns_false_when_disabled(self, provisioner_disabled):
        """create_vm() returns False when no backend is available."""
        result = await provisioner_disabled.create_vm(job_id="job-005")
        assert result is False

    @pytest.mark.asyncio
    async def test_create_vm_nats_failure(
        self, provisioner_with_nats, mock_nats_bridge
    ):
        """External creation remains closed independently of broker outcome."""
        mock_nats_bridge.request_vm_create = AsyncMock(return_value=False)
        result = await provisioner_with_nats.create_vm(job_id="job-006")
        assert result is False
        mock_nats_bridge.request_vm_create.assert_not_awaited()


# =============================================================================
# Test: fresh-provision reset (C — reap counter / stale endpoint hygiene)
# =============================================================================


class TestFreshProvisionReset:
    """Every (re)provision must reset stale VM lifecycle/probe state."""

    def test_fresh_provision_ctx_shape(self):
        from orchestrator.services.vm_provisioner import VMProvisioner

        ctx = VMProvisioner._fresh_provision_ctx()
        assert ctx["snapshot_attempts"] == 0
        assert ctx["ssh_host"] is None
        assert ctx["ssh_port"] is None
        assert ctx["ssh_registration_id"] is None
        assert ctx["registered_at"] is None
        assert ctx["ssh_verified_at"] is None
        assert ctx["ssh_probe_attempts"] == 0
        assert ctx["ssh_probe_error"] is None
        assert ctx["ssh_probe_failed_at"] is None
        assert ctx["ssh_host_key_fingerprint"] is None
        assert ctx["vm_uid"] is None
        assert ctx["rootdisk_pvc_uid"] is None
        assert ctx["golden_wait_started_at"] is None
        assert ctx["capacity_wait_started_at"] is None
        # A stale teardown anchor would make the next incarnation read as
        # instantly-stuck in 'deleting' and be recycled on sight.
        assert ctx["deleting_started_at"] is None
        assert ctx["retirement_cleanup_pending"] is False
        assert ctx["retirement_attempts"] == 0
        assert ctx["retirement_last_result"] is None
        assert ctx["retirement_retry_after"] is None
        assert isinstance(ctx["provisioned_at"], float)

    @pytest.mark.asyncio
    async def test_create_vm_same_cluster_resets_before_dispatch(
        self, provisioner_with_nats, mock_nats_bridge, mock_db
    ):
        with patch.dict(os.environ, {"VM_MODE": "same-cluster"}):
            provisioner_with_nats._controller_url = "http://controller"
            provisioner_with_nats._create_http = AsyncMock(return_value=True)
            await provisioner_with_nats.create_vm(job_id="reset-http")
        # First context write is the reset — before the controller writes
        # 'provisioning' — so it can't clobber the live provisioning status.
        first = mock_db.merge_vm_context.await_args_list[0]
        assert first[0][0] == "reset-http"
        ctx = first[0][1]
        assert ctx["snapshot_attempts"] == 0
        assert ctx["ssh_host"] is None
        assert "provisioned_at" in ctx

    @pytest.mark.asyncio
    async def test_create_thread_vm_resets(self, mock_nats_bridge, mock_db):
        with patch.dict(os.environ, {"VM_MODE": "same-cluster"}):
            from orchestrator.services.vm_provisioner import VMProvisioner

            prov = VMProvisioner()
            prov._db = mock_db
            prov._controller_url = "http://controller"
            prov._create_http = AsyncMock(return_value=True)
            await prov.create_thread_vm(
                thread_id="reset-thread",
                expected_runtime_generation=PROVISION_GENERATION,
                expected_vm_context=None,
            )
        first = mock_db.begin_pinned_thread_vm_provisioning.await_args
        assert first.args[0] == "reset-thread"
        ctx = first.kwargs["provision_context"]
        assert ctx["snapshot_attempts"] == 0
        assert ctx["ssh_host"] is None
        assert ctx["_runtime_incarnation"] is None
        assert ctx["status"] == "provisioning"
        assert prov._create_http.await_args.kwargs["entity_type"] == "thread"
        assert prov._create_http.await_args.kwargs["set_provisioning"] is False

    @pytest.mark.asyncio
    async def test_create_thread_vm_refuses_dispatch_when_authority_cas_loses(
        self, mock_nats_bridge, mock_db
    ):
        mock_db.begin_pinned_thread_vm_provisioning = AsyncMock(return_value=False)
        with patch.dict(os.environ, {"VM_MODE": "same-cluster"}):
            from orchestrator.services.vm_provisioner import VMProvisioner

            prov = VMProvisioner()
            prov._db = mock_db
            prov._controller_url = "http://controller"
            prov._create_http = AsyncMock(return_value=True)
            assert not await prov.create_thread_vm(
                thread_id="stale-thread",
                expected_runtime_generation=PROVISION_GENERATION,
                expected_vm_context=None,
            )
        prov._create_http.assert_not_awaited()


# =============================================================================
# Test: golden-poll re-issue (fresh=False)
# (knowledge-history/done/golden_image_cold_import_fails_inflight_vm_jobs.md)
# =============================================================================


class TestGoldenPollCreate:
    """create_vm(fresh=False) — the dispatcher's waiting_golden poll.

    A poll must NOT reset the provision context (golden_wait_started_at
    anchors the golden budget across polls) and must not flip the context
    status to 'provisioning' (it must stay waiting_golden so the decision
    logic keeps polling). Only provisioned_at rolls forward, so the boot
    budget starts ≈ when the golden completes and the VM is actually built.
    """

    @pytest.mark.asyncio
    async def test_poll_rolls_only_provisioned_at(
        self, provisioner_with_nats, mock_nats_bridge, mock_db
    ):
        with patch.dict(os.environ, {"VM_MODE": "same-cluster"}):
            provisioner_with_nats._controller_url = "http://controller"
            provisioner_with_nats._create_http = AsyncMock(return_value=True)
            await provisioner_with_nats.create_vm(job_id="poll-1", fresh=False)
        first = mock_db.merge_vm_context.await_args_list[0]
        assert first[0][0] == "poll-1"
        ctx = first[0][1]
        assert set(ctx.keys()) == {"provisioned_at"}

    @pytest.mark.asyncio
    async def test_poll_passes_set_provisioning_false(
        self, provisioner_with_nats, mock_nats_bridge
    ):
        with patch.dict(os.environ, {"VM_MODE": "same-cluster"}):
            provisioner_with_nats._controller_url = "http://controller"
            provisioner_with_nats._create_http = AsyncMock(return_value=True)
            await provisioner_with_nats.create_vm(job_id="poll-2", fresh=False)
        kwargs = provisioner_with_nats._create_http.await_args.kwargs
        assert kwargs["set_provisioning"] is False

    @pytest.mark.asyncio
    async def test_fresh_default_resets_and_sets_provisioning(
        self, provisioner_with_nats, mock_nats_bridge, mock_db
    ):
        with patch.dict(os.environ, {"VM_MODE": "same-cluster"}):
            provisioner_with_nats._controller_url = "http://controller"
            provisioner_with_nats._create_http = AsyncMock(return_value=True)
            await provisioner_with_nats.create_vm(job_id="fresh-1")
        ctx = mock_db.merge_vm_context.await_args_list[0][0][1]
        assert ctx["snapshot_attempts"] == 0
        assert ctx["golden_wait_started_at"] is None
        kwargs = provisioner_with_nats._create_http.await_args.kwargs
        assert kwargs["set_provisioning"] is True


# =============================================================================
# Test: delete_vm()
# =============================================================================


class TestDeleteVm:
    """Tests for delete_vm() across both backends."""

    @pytest.mark.asyncio
    async def test_delete_vm_nats_backend(
        self, provisioner_with_nats, mock_nats_bridge
    ):
        """Contained external creation does not strand exact-generation cleanup."""
        assert provisioner_with_nats.is_available is False
        assert provisioner_with_nats.lifecycle_available is True
        result = await provisioner_with_nats.delete_vm(job_id="job-del-001")
        assert result is True
        mock_nats_bridge.request_vm_delete.assert_awaited_once_with(
            "job-del-001",
            purge_disk=True,
            provision_generation=PROVISION_GENERATION,
            entity_type="job",
            expected_vm_uid="captured-vm-uid",
            expected_rootdisk_pvc_uid="captured-root-uid",
        )

    @pytest.mark.asyncio
    async def test_delete_vm_returns_false_when_disabled(self, provisioner_disabled):
        """delete_vm() returns False when no backend is available."""
        result = await provisioner_disabled.delete_vm(job_id="job-del-none")
        assert result is False

    @pytest.mark.asyncio
    async def test_delete_vm_nats_failure(
        self, provisioner_with_nats, mock_nats_bridge
    ):
        """delete_vm() returns False when NATS publish fails."""
        mock_nats_bridge.request_vm_delete = AsyncMock(return_value=False)
        result = await provisioner_with_nats.delete_vm(job_id="job-del-fail")
        assert result is False


# =============================================================================
# Test: query_status()
# =============================================================================


class TestQueryStatus:
    """Tests for query_status() across both backends."""

    @pytest.mark.asyncio
    async def test_query_status_nats_backend(
        self, provisioner_with_nats, mock_nats_bridge
    ):
        """query_status() delegates to nats_bridge when NATS is available."""
        expected = {"job_id": "status-001", "status": "running", "ready": True}
        mock_nats_bridge.query_vm_status = AsyncMock(return_value=expected)

        result = await provisioner_with_nats.query_status(
            job_id="status-001", timeout=3.0
        )
        assert result == expected
        mock_nats_bridge.query_vm_status.assert_awaited_once_with(
            "status-001", 3.0, provision_generation=PROVISION_GENERATION
        )

    @pytest.mark.asyncio
    async def test_nats_status_persists_late_rootdisk_identity(
        self, provisioner_with_nats, mock_nats_bridge, mock_db
    ):
        mock_nats_bridge.query_vm_status = AsyncMock(
            return_value={
                "job_id": "late-nats",
                "vm_name": "agent-vm-late-nats",
                "namespace": "agent-vms",
                "vm_uid": "late-nats-vm-uid",
                "rootdisk_pvc_uid": "late-nats-root-pvc-uid",
                "provision_generation": PROVISION_GENERATION,
                "_identity_authenticated": True,
            }
        )

        result = await provisioner_with_nats.query_status("late-nats")

        assert result is not None
        assert "_identity_authenticated" not in result
        updates = mock_db.merge_vm_context_if_provision_generation.await_args.args[2]
        assert updates["vm_uid"] == "late-nats-vm-uid"
        assert updates["rootdisk_pvc_uid"] == "late-nats-root-pvc-uid"
        assert updates["identity_authenticated"] is True

    @pytest.mark.asyncio
    async def test_http_status_persists_late_thread_rootdisk_identity(
        self, provisioner_disabled, mock_db
    ):
        from orchestrator.services.vm_lifecycle_auth import sign_payload

        secret = b"http-lifecycle-status-test-secret-at-least-32-bytes"
        provisioner_disabled._db = mock_db
        provisioner_disabled._controller_url = "http://vm-controller:8080"
        provisioner_disabled._lifecycle_hmac_secret = secret
        client = MagicMock()

        async def _get(_path, *, params, timeout):
            response = MagicMock(status_code=200)
            response.raise_for_status = MagicMock()
            response.json.return_value = sign_payload(
                {
                    "job_id": "late-http-thread",
                    "vm_name": "agent-vm-late-http-thread",
                    "namespace": "agent-vms",
                    "vm_uid": "late-http-vm-uid",
                    "rootdisk_pvc_uid": "late-http-root-pvc-uid",
                    "provision_generation": PROVISION_GENERATION,
                    "pod_ip": "10.42.0.19",
                    "ready": True,
                    "phase": "Running",
                    "active_pod_uid": "launcher-uid-2",
                },
                direction="response",
                operation="status",
                secret=secret,
                correlation_id=params["lifecycle_auth_request_id"],
            )
            return response

        client.get = AsyncMock(side_effect=_get)
        provisioner_disabled._http_client = client

        with patch.dict(os.environ, {"VM_MODE": "same-cluster"}):
            result = await provisioner_disabled.query_status(
                "late-http-thread", entity_type="thread"
            )

        assert result is not None
        assert result["pod_ip"] == "10.42.0.19"
        assert result["ready"] is True
        assert result["phase"] == "Running"
        assert result["active_pod_uid"] == "launcher-uid-2"
        updates = (
            mock_db.merge_thread_vm_context_if_provision_generation.await_args.args[2]
        )
        assert updates["vm_uid"] == "late-http-vm-uid"
        assert updates["rootdisk_pvc_uid"] == "late-http-root-pvc-uid"

    @pytest.mark.asyncio
    async def test_query_status_returns_none_when_disabled(self, provisioner_disabled):
        """query_status() returns None when no backend is available."""
        result = await provisioner_disabled.query_status(job_id="no-backend")
        assert result is None

    @pytest.mark.asyncio
    async def test_query_status_nats_timeout(
        self, provisioner_with_nats, mock_nats_bridge
    ):
        """query_status() returns None when NATS request times out."""
        mock_nats_bridge.query_vm_status = AsyncMock(return_value=None)
        result = await provisioner_with_nats.query_status(
            job_id="timeout-test", timeout=1.0
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_query_status_default_timeout(
        self, provisioner_with_nats, mock_nats_bridge
    ):
        """query_status() uses default 5.0s timeout when not specified."""
        await provisioner_with_nats.query_status(job_id="default-timeout")
        mock_nats_bridge.query_vm_status.assert_awaited_once_with(
            "default-timeout", 5.0, provision_generation=PROVISION_GENERATION
        )


# =============================================================================
# Test: _set_vm_context()
# =============================================================================


class TestSetVmContext:
    """Tests for _set_vm_context() helper."""

    @pytest.mark.asyncio
    async def test_set_vm_context_calls_db(self, provisioner_with_db, mock_db):
        """_set_vm_context() delegates to db.merge_vm_context."""
        updates = {"status": "created", "vm_name": "agent-vm-test"}
        await provisioner_with_db._set_vm_context("job-ctx-001", updates)
        mock_db.merge_vm_context.assert_awaited_once_with("job-ctx-001", updates)

    @pytest.mark.asyncio
    async def test_set_vm_context_no_db(self):
        """_set_vm_context() is a no-op when db is not set."""
        with patch("orchestrator.services.vm_provisioner.nats_bridge") as nb:
            nb.is_available = False
            from orchestrator.services.vm_provisioner import VMProvisioner

            prov = VMProvisioner()
            prov._db = None
            # Should not raise
            await prov._set_vm_context("job-no-db", {"status": "test"})

    @pytest.mark.asyncio
    async def test_set_vm_context_handles_db_error(self, provisioner_with_db, mock_db):
        """_set_vm_context() catches and logs db exceptions."""
        mock_db.merge_vm_context = AsyncMock(
            side_effect=Exception("DB connection lost")
        )
        # Should not raise
        await provisioner_with_db._set_vm_context("job-db-err", {"status": "test"})
        mock_db.merge_vm_context.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_set_vm_context_passes_arbitrary_updates(
        self, provisioner_with_db, mock_db
    ):
        """_set_vm_context() passes the exact updates dict to the DB."""
        updates = {
            "status": "ready",
            "ssh_host": "10.0.0.5",
            "ssh_port": 22,
            "custom_key": "custom_value",
        }
        await provisioner_with_db._set_vm_context("job-arb", updates)
        mock_db.merge_vm_context.assert_awaited_once_with("job-arb", updates)


# =============================================================================
# Test: Environment Variable Configuration
# =============================================================================


class TestEnvironmentConfiguration:
    """Tests for environment variable handling during __init__."""

    def test_default_namespace(self):
        """Default VM namespace is 'agent-vms'."""
        env = dict(os.environ)
        env.pop("VM_NAMESPACE", None)
        with patch.dict(os.environ, env, clear=True):
            from orchestrator.services.vm_provisioner import VMProvisioner

            prov = VMProvisioner()
            assert prov._vm_namespace == "agent-vms"

    def test_custom_namespace(self):
        """VM_NAMESPACE env var overrides the default."""
        with patch.dict(os.environ, {"VM_NAMESPACE": "custom-ns"}):
            from orchestrator.services.vm_provisioner import VMProvisioner

            prov = VMProvisioner()
            assert prov._vm_namespace == "custom-ns"

    def test_default_vm_image(self):
        """Default VM image is the ghcr.io image."""
        env = dict(os.environ)
        env.pop("DEFAULT_VM_IMAGE", None)
        with patch.dict(os.environ, env, clear=True):
            from orchestrator.services.vm_provisioner import VMProvisioner

            prov = VMProvisioner()
            assert "ghcr.io" in prov._default_vm_image
            assert "agent-vm-base" in prov._default_vm_image

    def test_custom_vm_image(self):
        """DEFAULT_VM_IMAGE env var overrides the default."""
        with patch.dict(
            os.environ, {"DEFAULT_VM_IMAGE": "my-registry.io/custom-vm:v5"}
        ):
            from orchestrator.services.vm_provisioner import VMProvisioner

            prov = VMProvisioner()
            assert prov._default_vm_image == "my-registry.io/custom-vm:v5"


# =============================================================================
# Test: Module-Level Singleton
# =============================================================================


class TestSingleton:
    """Tests for the module-level vm_provisioner singleton."""

    def test_singleton_exists(self):
        """Module exports a vm_provisioner singleton."""
        from orchestrator.services.vm_provisioner import vm_provisioner

        assert vm_provisioner is not None

    def test_singleton_is_vm_provisioner(self):
        """Singleton is an instance of VMProvisioner."""
        from orchestrator.services.vm_provisioner import VMProvisioner, vm_provisioner

        assert isinstance(vm_provisioner, VMProvisioner)


# =============================================================================
# Test: Direct Backend K8s API Parameters
# =============================================================================


# =============================================================================
# Test: Concurrent Operations
# =============================================================================


class TestConcurrentOperations:
    """Tests for concurrent VM operations."""

    @pytest.mark.asyncio
    async def test_multiple_creates_in_parallel(self, mock_nats_bridge, mock_db):
        """Concurrent callers cannot bypass external-mode containment."""
        with (
            patch.dict(os.environ, {"VM_MODE": "external"}),
            patch("orchestrator.services.vm_provisioner.nats_bridge", mock_nats_bridge),
        ):
            mock_nats_bridge.is_available = True
            from orchestrator.services.vm_provisioner import VMProvisioner

            prov = VMProvisioner()
            prov._db = mock_db

            results = await asyncio.gather(
                prov.create_vm(job_id="par-001"),
                prov.create_vm(job_id="par-002"),
                prov.create_vm(job_id="par-003"),
            )

        assert results == [False, False, False]
        mock_nats_bridge.request_vm_create.assert_not_awaited()
        mock_db.merge_vm_context.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_create_and_query_in_parallel(self, mock_nats_bridge, mock_db):
        """create_vm() and query_status() can run concurrently."""
        with (
            patch.dict(os.environ, {"VM_MODE": "external"}),
            patch("orchestrator.services.vm_provisioner.nats_bridge", mock_nats_bridge),
        ):
            mock_nats_bridge.is_available = True
            from orchestrator.services.vm_provisioner import VMProvisioner

            prov = VMProvisioner()
            prov._db = mock_db

            create_result, status_result = await asyncio.gather(
                prov.create_vm(job_id="mix-001"),
                prov.query_status(job_id="mix-002"),
            )

        assert create_result is False
        assert status_result is not None
        mock_nats_bridge.request_vm_create.assert_not_awaited()


# =============================================================================
# Test: asyncio.to_thread Usage in Direct Backend
# =============================================================================


# =============================================================================
# Test: list_vms() — orphan-sweep inventory across backends
# =============================================================================


class TestListVms:
    @pytest.mark.asyncio
    async def test_list_nats_backend(self, provisioner_with_nats, mock_nats_bridge):
        expected = [{"vm_name": "agent-vm-j1", "entity_id": "j1"}]
        mock_nats_bridge.request_vm_list = AsyncMock(return_value=expected)
        assert await provisioner_with_nats.list_vms() == expected

    @pytest.mark.asyncio
    async def test_list_http_backend(self, provisioner_disabled):
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {
            "vms": [{"vm_name": "agent-vm-j2", "entity_id": "j2"}]
        }
        provisioner_disabled._controller_url = "http://vm-controller:8080"
        provisioner_disabled._http_client = MagicMock()
        provisioner_disabled._http_client.get = AsyncMock(return_value=resp)
        with (
            patch.dict(os.environ, {"VM_MODE": "same-cluster"}),
            patch("orchestrator.services.vm_provisioner.nats_bridge") as nb,
        ):
            nb.is_available = False
            vms = await provisioner_disabled.list_vms()
        assert vms == [{"vm_name": "agent-vm-j2", "entity_id": "j2"}]

    @pytest.mark.asyncio
    async def test_list_http_404_means_unknown_not_empty(self, provisioner_disabled):
        # An old controller without the list op must yield None (unknown),
        # never [] — the sweep would otherwise treat it as "no VMs".
        resp = MagicMock()
        resp.status_code = 404
        provisioner_disabled._controller_url = "http://vm-controller:8080"
        provisioner_disabled._http_client = MagicMock()
        provisioner_disabled._http_client.get = AsyncMock(return_value=resp)
        with (
            patch.dict(os.environ, {"VM_MODE": "same-cluster"}),
            patch("orchestrator.services.vm_provisioner.nats_bridge") as nb,
        ):
            nb.is_available = False
            assert await provisioner_disabled.list_vms() is None

    @pytest.mark.asyncio
    async def test_list_no_backend_returns_none(self, provisioner_disabled):
        with patch("orchestrator.services.vm_provisioner.nats_bridge") as nb:
            nb.is_available = False
            assert await provisioner_disabled.list_vms() is None


# =============================================================================
# release_vm / release_thread_vm — snapshot outcome must be reported truthfully
# knowledge-base/knowledge/issues/vm_workspace_snapshot_unreachable_from_orchestrator.md
# =============================================================================


def _snapshot_service(*, captured: bool):
    """Snapshot service whose capture SUCCEEDS or is SKIPPED.

    capture_vm_snapshot returns False for an unroutable tailnet target — it does
    not raise — so a try/except around it cannot see the failure.
    """
    svc = MagicMock()
    svc.is_available = True
    svc.capture_vm_snapshot = AsyncMock(return_value=captured)
    return svc


def _make_process_retirement_settle(db):
    async def settle(*_args, **_kwargs):
        db.managed_repository_workspace_process_zero_is_current.return_value = True
        return True

    db.record_managed_repository_workspace_process_zero.side_effect = settle


class TestReleaseReportsSnapshotOutcome:
    """A VM workspace lives on the tailnet, which the orchestrator cannot reach,
    so capture_vm_snapshot returns False for every VM. Release deletes the VM
    regardless (by design, non-fatal) — but it must not claim it captured a
    snapshot it did not, or the logs actively mislead whoever investigates the
    missing workspace state."""

    @pytest.mark.asyncio
    async def test_thread_release_does_not_claim_capture_when_skipped(
        self, provisioner_with_nats, mock_nats_bridge, caplog
    ):
        provisioner_with_nats._snapshot_service = _snapshot_service(captured=False)
        provisioner_with_nats._db.managed_repository_workspace_process_zero_is_current.return_value = False
        _make_process_retirement_settle(provisioner_with_nats._db)
        provisioner_with_nats._db.get_thread = AsyncMock(
            return_value={
                "metadata": {
                    "vm": _ready_vm_context(ssh_host="100.64.1.6", ssh_port=22)
                }
            }
        )

        with caplog.at_level("INFO"):
            ok = await provisioner_with_nats.release_thread_vm("tid-1")

        assert ok is True  # deletion still proceeds — non-fatal by design
        text = caplog.text
        assert "snapshot captured" not in text.lower(), (
            "must not report a capture that returned False"
        )
        assert "tid-1" in text

    @pytest.mark.asyncio
    async def test_thread_release_still_reports_a_real_capture(
        self, provisioner_with_nats, mock_nats_bridge, caplog
    ):
        provisioner_with_nats._snapshot_service = _snapshot_service(captured=True)
        provisioner_with_nats._db.managed_repository_workspace_process_zero_is_current.return_value = False
        _make_process_retirement_settle(provisioner_with_nats._db)
        provisioner_with_nats._db.get_thread = AsyncMock(
            return_value={
                "metadata": {"vm": _ready_vm_context(ssh_host="10.0.0.9", ssh_port=22)}
            }
        )

        with caplog.at_level("INFO"):
            await provisioner_with_nats.release_thread_vm("tid-2")

        assert "snapshot captured" in caplog.text.lower()
        capture = provisioner_with_nats._snapshot_service.capture_vm_snapshot
        assert (
            capture.await_args.kwargs["expected_host_key_fingerprint"]
            == TEST_HOST_KEY_FINGERPRINT
        )
        assert capture.await_args.kwargs["ssh_host"] == "10.0.0.9"

    @pytest.mark.asyncio
    async def test_job_release_does_not_claim_capture_when_skipped(
        self, provisioner_with_nats, mock_nats_bridge, caplog
    ):
        """release_vm carries the identical pattern for jobs."""
        provisioner_with_nats._snapshot_service = _snapshot_service(captured=False)
        provisioner_with_nats._db.managed_repository_workspace_process_zero_is_current.return_value = False
        _make_process_retirement_settle(provisioner_with_nats._db)
        provisioner_with_nats._db.get_job = AsyncMock(
            return_value={"context": {"vm": _ready_vm_context()}}
        )

        with caplog.at_level("INFO"):
            ok = await provisioner_with_nats.release_vm(
                "job-1", ssh_host="100.64.1.9", ssh_port=22
            )

        assert ok is True
        assert "snapshot captured" not in caplog.text.lower()

    @pytest.mark.asyncio
    async def test_replaced_vm_reusing_endpoint_receives_zero_snapshot_bytes(
        self, provisioner_with_nats
    ):
        provisioner_with_nats._snapshot_service = _snapshot_service(captured=True)
        provisioner_with_nats._db.managed_repository_workspace_process_zero_is_current.return_value = False
        provisioner_with_nats.revalidate_vm_teardown_identity = AsyncMock(
            return_value="superseded"
        )

        assert not await provisioner_with_nats.release_vm(
            "job-reused-ip", ssh_host="100.64.1.9", ssh_port=22
        )
        provisioner_with_nats._snapshot_service.capture_vm_snapshot.assert_not_awaited()


class TestPurgeDiskIntent:
    """``purge_disk`` tells the controller whether a delete is terminal.

    Default True everywhere, so a call site that says nothing keeps today's
    semantics; False means "a recreate is coming, keep the rootdisk".
    knowledge-base/knowledge/features/vm_persistent_rootdisk.md D2.
    """

    @pytest.mark.asyncio
    async def test_job_delete_defaults_to_purge(
        self, provisioner_with_nats, mock_nats_bridge
    ):
        await provisioner_with_nats.delete_vm("job-1")
        mock_nats_bridge.request_vm_delete.assert_awaited_once_with(
            "job-1",
            purge_disk=True,
            provision_generation=PROVISION_GENERATION,
            entity_type="job",
            expected_vm_uid="captured-vm-uid",
            expected_rootdisk_pvc_uid="captured-root-uid",
        )

    @pytest.mark.asyncio
    async def test_job_delete_can_keep_the_disk(
        self, provisioner_with_nats, mock_nats_bridge
    ):
        await provisioner_with_nats.delete_vm("job-1", purge_disk=False)
        mock_nats_bridge.request_vm_delete.assert_awaited_once_with(
            "job-1",
            purge_disk=False,
            provision_generation=PROVISION_GENERATION,
            entity_type="job",
            expected_vm_uid="captured-vm-uid",
            expected_rootdisk_pvc_uid="captured-root-uid",
        )

    @pytest.mark.asyncio
    async def test_thread_delete_can_keep_the_disk(
        self, provisioner_with_nats, mock_nats_bridge
    ):
        await provisioner_with_nats.delete_thread_vm("tid-1", purge_disk=False)
        mock_nats_bridge.request_vm_delete.assert_awaited_once_with(
            "tid-1",
            purge_disk=False,
            provision_generation=PROVISION_GENERATION,
            entity_type="thread",
            expected_vm_uid="captured-vm-uid",
            expected_rootdisk_pvc_uid="captured-root-uid",
        )

    @pytest.mark.asyncio
    async def test_release_purges(self, provisioner_with_nats, mock_nats_bridge):
        """Release is terminal for the entity — the disk goes with it."""
        provisioner_with_nats._snapshot_service = None
        await provisioner_with_nats.release_vm("job-1")
        mock_nats_bridge.request_vm_delete.assert_awaited_once_with(
            "job-1",
            purge_disk=True,
            provision_generation=PROVISION_GENERATION,
            entity_type="job",
            expected_vm_uid="captured-vm-uid",
            expected_rootdisk_pvc_uid="captured-root-uid",
        )

    @pytest.mark.asyncio
    async def test_thread_release_purges(self, provisioner_with_nats, mock_nats_bridge):
        provisioner_with_nats._snapshot_service = None
        await provisioner_with_nats.release_thread_vm("tid-1")
        mock_nats_bridge.request_vm_delete.assert_awaited_once_with(
            "tid-1",
            purge_disk=True,
            provision_generation=PROVISION_GENERATION,
            entity_type="thread",
            expected_vm_uid="captured-vm-uid",
            expected_rootdisk_pvc_uid="captured-root-uid",
        )


class TestCapturedVmTeardown:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("entity_type", ["job", "thread"])
    @pytest.mark.parametrize("retirement_succeeds", [True, False])
    async def test_late_boot_retires_through_controller_endpoint_without_promotion(
        self,
        provisioner_with_db,
        mock_db,
        monkeypatch,
        entity_type,
        retirement_succeeds,
    ):
        from orchestrator.services.vm_provisioner import VMTeardownResult

        monkeypatch.setenv("VM_MODE", "same-cluster")
        provisioner_with_db._controller_url = "http://controller"
        context = _ready_vm_context(
            status="retiring_process_zero", ssh_host=None, ssh_port=None
        )
        mock_db.get_job.return_value = {"context": {"vm": context}}
        mock_db.get_thread.return_value = {"metadata": {"vm": context}}
        mock_db.managed_repository_workspace_process_zero_is_current.return_value = (
            False
        )
        provisioner_with_db._query_http = AsyncMock(
            return_value={
                "_identity_authenticated": True,
                "provision_generation": PROVISION_GENERATION,
                "vm_uid": "captured-vm-uid",
                "rootdisk_pvc_uid": "captured-root-uid",
                "rootdisk_identity_known": True,
                "credential_runtime_started": True,
                "ready": True,
                "pod_ip": "10.42.3.220",
                "active_pod_uid": LAUNCHER_POD_UID,
            }
        )
        provisioner_with_db.delete_vm_captured = AsyncMock(
            return_value=VMTeardownResult("completed", True)
        )
        identity = await provisioner_with_db.capture_vm_teardown_identity(
            "job-1", entity_type=entity_type
        )
        with patch(
            "orchestrator.services.vm_provisioner.retire_managed_repository_processes",
            new=AsyncMock(return_value=retirement_succeeds),
        ) as retire:
            result = await provisioner_with_db.release_vm_captured(
                "job-1",
                identity,
                entity_type=entity_type,
                purge_disk=False,
                capture_snapshot=False,
            )

        assert result == (
            VMTeardownResult("completed", True)
            if retirement_succeeds
            else VMTeardownResult("process_zero_unproven", False)
        )
        retire.assert_awaited_once_with(
            host="10.42.3.220",
            port=22,
            host_key_fingerprint=TEST_HOST_KEY_FINGERPRINT,
            operation="VM managed repository process retirement",
        )
        if retirement_succeeds:
            mock_db.record_managed_repository_workspace_process_zero.assert_awaited_once()
            assert (
                provisioner_with_db.delete_vm_captured.await_args.kwargs["purge_disk"]
                is False
            )
        else:
            mock_db.record_managed_repository_workspace_process_zero.assert_not_awaited()
            provisioner_with_db.delete_vm_captured.assert_not_awaited()
        mock_db.merge_vm_context_if_provision_generation.assert_not_awaited()
        mock_db.merge_thread_vm_context_if_provision_generation.assert_not_awaited()
        assert context["status"] == "retiring_process_zero"
        assert context["ssh_host"] is None

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "changed",
        [
            {"_identity_authenticated": False},
            {"vm_uid": "replacement-vm"},
            {"rootdisk_pvc_uid": "replacement-disk"},
            {"rootdisk_identity_known": False},
            {"provision_generation": "00000000-0000-4000-8000-000000000099"},
            {"ready": False},
            {"active_pod_uid": None},
            {"pod_ip": "guest.example"},
            {"pod_ip": "127.0.0.1"},
            {"pod_ip": "169.254.169.254"},
            {"pod_ip": "0.0.0.0"},
            {"pod_ip": "224.0.0.1"},
            {"pod_ip": "10.42.3.220 "},
            {"missing_stored_pin": True},
        ],
    )
    async def test_late_boot_endpoint_requires_exact_authenticated_runtime(
        self, provisioner_with_db, mock_db, monkeypatch, changed
    ):
        monkeypatch.setenv("VM_MODE", "same-cluster")
        provisioner_with_db._controller_url = "http://controller"
        mock_db.get_job.return_value = {
            "context": {
                "vm": _ready_vm_context(
                    status="retiring_process_zero", ssh_host=None, ssh_port=None
                )
            }
        }
        mock_db.managed_repository_workspace_process_zero_is_current.return_value = (
            False
        )
        if changed.get("missing_stored_pin"):
            mock_db.get_job.return_value["context"]["vm"][
                "ssh_host_key_fingerprint"
            ] = None
        provisioner_with_db._query_http = AsyncMock(
            return_value={
                "_identity_authenticated": True,
                "provision_generation": PROVISION_GENERATION,
                "vm_uid": "captured-vm-uid",
                "rootdisk_pvc_uid": "captured-root-uid",
                "rootdisk_identity_known": True,
                "credential_runtime_started": True,
                "ready": True,
                "pod_ip": "10.42.3.220",
                "active_pod_uid": LAUNCHER_POD_UID,
                **changed,
            }
        )
        identity = await provisioner_with_db.capture_vm_teardown_identity("job-1")
        provisioner_with_db.delete_vm_captured = AsyncMock()
        with patch(
            "orchestrator.services.vm_provisioner.retire_managed_repository_processes",
            new=AsyncMock(return_value=True),
        ) as retire:
            result = await provisioner_with_db.release_vm_captured(
                "job-1", identity, purge_disk=False, capture_snapshot=False
            )
        assert not result.deleted
        retire.assert_not_awaited()
        mock_db.record_managed_repository_workspace_process_zero.assert_not_awaited()
        provisioner_with_db.delete_vm_captured.assert_not_awaited()

    @staticmethod
    def _identity():
        from orchestrator.services.vm_provisioner import VMTeardownIdentity

        return VMTeardownIdentity(
            provision_generation=PROVISION_GENERATION,
            vm_uid="captured-vm-uid",
            rootdisk_pvc_uid="captured-root-uid",
        )

    @staticmethod
    def _probe(
        disposition: str,
        *,
        vm_uid=None,
        root_uid=None,
        known=True,
        credential_runtime_started=None,
    ):
        from orchestrator.services.vm_provisioner import (
            VMTeardownIdentity,
            _VMTeardownProbe,
        )

        return _VMTeardownProbe(
            disposition,
            VMTeardownIdentity(
                PROVISION_GENERATION,
                vm_uid,
                root_uid,
                credential_runtime_started=credential_runtime_started,
            ),
            rootdisk_identity_known=known,
        )

    @pytest.mark.asyncio
    async def test_capture_probes_authenticated_late_rootdisk_uid(
        self, provisioner_with_db, mock_db
    ):
        mock_db.get_job.return_value = {
            "context": {
                "vm": {
                    "provision_generation": PROVISION_GENERATION,
                    "identity_provision_generation": PROVISION_GENERATION,
                    "identity_authenticated": True,
                    "vm_uid": "captured-vm-uid",
                }
            }
        }
        provisioner_with_db._probe_vm_teardown_identity = AsyncMock(
            return_value=self._probe(
                "present",
                vm_uid="captured-vm-uid",
                root_uid="late-root-uid",
                known=True,
            )
        )

        identity = await provisioner_with_db.capture_vm_teardown_identity("job-1")

        assert identity.vm_uid == "captured-vm-uid"
        assert identity.rootdisk_pvc_uid == "late-root-uid"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("parent_cleanup", [None, {"admission_id": "exact-parent"}])
    async def test_thread_capture_and_delete_use_exact_thread_generation(
        self, provisioner_with_db, mock_db, parent_cleanup
    ):
        mock_db.get_thread.return_value = {
            "metadata": {
                "vm": {
                    "provision_generation": PROVISION_GENERATION,
                    "identity_provision_generation": PROVISION_GENERATION,
                    "identity_authenticated": True,
                    "vm_uid": "captured-vm-uid",
                    "rootdisk_pvc_uid": "captured-root-uid",
                }
            }
        }
        identity = await provisioner_with_db.capture_vm_teardown_identity(
            "thread-1", entity_type="thread"
        )
        present = self._probe(
            "present",
            vm_uid="captured-vm-uid",
            root_uid="captured-root-uid",
            credential_runtime_started=False,
        )
        absent = self._probe("absent", vm_uid=None, root_uid=None, known=True)
        provisioner_with_db._probe_vm_teardown_identity = AsyncMock(
            side_effect=[present, absent]
        )
        provisioner_with_db._delete_vm_with_identity = AsyncMock(return_value=True)

        outcome = await provisioner_with_db.delete_vm_captured(
            "thread-1", identity, entity_type="thread", parent_cleanup=parent_cleanup
        )

        assert outcome.disposition == "completed"
        provisioner_with_db._delete_vm_with_identity.assert_awaited_once_with(
            "thread-1",
            purge_disk=True,
            provision_generation=PROVISION_GENERATION,
            expected_vm_uid="captured-vm-uid",
            expected_rootdisk_pvc_uid="captured-root-uid",
            entity_type="thread",
            **(
                {"parent_cleanup": parent_cleanup} if parent_cleanup is not None else {}
            ),
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("credential_runtime_started", [False, True])
    @pytest.mark.parametrize("parent_cleanup", [None, {"admission_id": "exact-parent"}])
    async def test_release_records_process_zero_before_captured_delete(
        self,
        provisioner_with_db,
        mock_db,
        credential_runtime_started,
        parent_cleanup,
    ):
        from orchestrator.services.vm_provisioner import (
            VMTeardownIdentity,
            VMTeardownResult,
        )

        identity = VMTeardownIdentity(
            provision_generation=PROVISION_GENERATION,
            vm_uid="captured-vm-uid",
            rootdisk_pvc_uid="captured-root-uid",
            ssh_host="vm.internal",
            ssh_port=22,
            ssh_host_key_fingerprint=TEST_HOST_KEY_FINGERPRINT,
        )
        present = self._probe(
            "present",
            vm_uid="captured-vm-uid",
            root_uid="captured-root-uid",
            credential_runtime_started=credential_runtime_started,
        )
        provisioner_with_db._probe_vm_teardown_identity = AsyncMock(
            side_effect=[present, present]
        )
        mock_db.managed_repository_workspace_process_zero_is_current.return_value = (
            False
        )
        provisioner_with_db.delete_vm_captured = AsyncMock(
            return_value=VMTeardownResult("completed", True)
        )

        with patch(
            "orchestrator.services.vm_provisioner.retire_managed_repository_processes",
            new=AsyncMock(return_value=True),
        ) as retire:
            outcome = await provisioner_with_db.release_vm_captured(
                "job-1",
                identity,
                capture_snapshot=False,
                parent_cleanup=parent_cleanup,
            )

        assert (outcome.disposition, outcome.deleted) == ("completed", True)
        mock_db.claim_managed_repository_workspace_retirement.assert_awaited_once_with(
            "job-1",
            owner_kind="job",
            scope="vm",
            provisioner="vm",
            runtime_incarnation=PROVISION_GENERATION,
        )
        mock_db.record_managed_repository_workspace_process_zero.assert_awaited_once_with(
            "job-1",
            owner_kind="job",
            scope="vm",
            provisioner="vm",
            runtime_incarnation=PROVISION_GENERATION,
        )
        if credential_runtime_started:
            retire.assert_awaited_once_with(
                host="vm.internal",
                port=22,
                host_key_fingerprint=TEST_HOST_KEY_FINGERPRINT,
                operation="VM managed repository process retirement",
            )
        else:
            retire.assert_not_awaited()
        provisioner_with_db.delete_vm_captured.assert_awaited_once_with(
            "job-1",
            identity,
            purge_disk=True,
            entity_type="job",
            **(
                {"parent_cleanup": parent_cleanup} if parent_cleanup is not None else {}
            ),
        )

    @pytest.mark.asyncio
    async def test_started_vm_without_retirement_endpoint_fails_closed(
        self, provisioner_with_db, mock_db
    ):
        identity = self._identity()
        present = self._probe(
            "present",
            vm_uid="captured-vm-uid",
            root_uid="captured-root-uid",
            credential_runtime_started=True,
        )
        provisioner_with_db._probe_vm_teardown_identity = AsyncMock(
            return_value=present
        )
        mock_db.managed_repository_workspace_process_zero_is_current.return_value = (
            False
        )
        provisioner_with_db.delete_vm_captured = AsyncMock()

        outcome = await provisioner_with_db.release_vm_captured(
            "job-1", identity, capture_snapshot=False
        )

        assert (outcome.disposition, outcome.deleted) == (
            "process_zero_unproven",
            False,
        )
        mock_db.record_managed_repository_workspace_process_zero.assert_not_awaited()
        provisioner_with_db.delete_vm_captured.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_delete_acceptance_waits_for_exact_vm_and_rootdisk_absence(
        self, provisioner_with_db
    ):
        present = self._probe(
            "present",
            vm_uid="captured-vm-uid",
            root_uid="captured-root-uid",
            credential_runtime_started=False,
        )
        provisioner_with_db._probe_vm_teardown_identity = AsyncMock(
            side_effect=[present, present]
        )
        provisioner_with_db._delete_vm_with_identity = AsyncMock(return_value=True)

        outcome = await provisioner_with_db.delete_vm_captured(
            "job-1", self._identity()
        )

        assert outcome.disposition == "retry_pending"
        assert outcome.deleted is False

    @pytest.mark.asyncio
    async def test_response_loss_converges_with_stale_db_context_after_exact_absence(
        self, provisioner_with_db
    ):
        absent = self._probe("absent", vm_uid=None, root_uid=None, known=True)
        provisioner_with_db._probe_vm_teardown_identity = AsyncMock(return_value=absent)
        provisioner_with_db._delete_vm_with_identity = AsyncMock()

        outcome = await provisioner_with_db.delete_vm_captured(
            "job-1", self._identity()
        )

        assert outcome.disposition == "completed"
        assert outcome.deleted is True
        provisioner_with_db._delete_vm_with_identity.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("vm_uid", "root_uid"),
        [
            ("replacement-vm-uid", "captured-root-uid"),
            ("captured-vm-uid", "replacement-root-uid"),
        ],
    )
    async def test_proven_replacement_identity_supersedes_without_delete(
        self, provisioner_with_db, vm_uid, root_uid
    ):
        provisioner_with_db._probe_vm_teardown_identity = AsyncMock(
            return_value=self._probe(
                "present", vm_uid=vm_uid, root_uid=root_uid, known=True
            )
        )
        provisioner_with_db._delete_vm_with_identity = AsyncMock()

        outcome = await provisioner_with_db.delete_vm_captured(
            "job-1", self._identity()
        )

        assert outcome.disposition == "identity_superseded"
        provisioner_with_db._delete_vm_with_identity.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_transport_false_with_unchanged_identity_retries(
        self, provisioner_with_db
    ):
        present = self._probe(
            "present",
            vm_uid="captured-vm-uid",
            root_uid="captured-root-uid",
            credential_runtime_started=False,
        )
        provisioner_with_db._probe_vm_teardown_identity = AsyncMock(
            side_effect=[present, present]
        )
        provisioner_with_db._delete_vm_with_identity = AsyncMock(return_value=False)

        outcome = await provisioner_with_db.delete_vm_captured(
            "job-1", self._identity()
        )

        assert outcome.disposition == "retry_pending"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("root_known", [False, True])
    async def test_active_vm_without_captured_rootdisk_uid_never_purges(
        self, provisioner_with_db, root_known
    ):
        identity = self._identity()
        identity = type(identity)(identity.provision_generation, identity.vm_uid, None)
        provisioner_with_db._probe_vm_teardown_identity = AsyncMock(
            return_value=self._probe(
                "present",
                vm_uid="captured-vm-uid",
                root_uid=None,
                known=root_known,
            )
        )
        provisioner_with_db._delete_vm_with_identity = AsyncMock()

        outcome = await provisioner_with_db.delete_vm_captured("job-1", identity)

        assert outcome.disposition == "identity_unknown"
        provisioner_with_db._delete_vm_with_identity.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_orphan_purge_requires_captured_rootdisk_uid(
        self, provisioner_with_db
    ):
        identity = self._identity()
        identity = type(identity)(identity.provision_generation, identity.vm_uid, None)
        provisioner_with_db._delete_vm_with_identity = AsyncMock()

        outcome = await provisioner_with_db.delete_orphan_vm_captured(
            "job-1", identity, purge_disk=True
        )

        assert outcome.disposition == "identity_unknown"
        provisioner_with_db._delete_vm_with_identity.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_orphan_delete_acceptance_waits_for_exact_absence(
        self, provisioner_with_db
    ):
        present = self._probe(
            "present",
            vm_uid="captured-vm-uid",
            root_uid="captured-root-uid",
            credential_runtime_started=False,
        )
        provisioner_with_db._probe_vm_teardown_identity = AsyncMock(
            side_effect=[present, present]
        )
        provisioner_with_db._delete_vm_with_identity = AsyncMock(return_value=True)

        outcome = await provisioner_with_db.delete_orphan_vm_captured(
            "job-1", self._identity(), purge_disk=True
        )

        assert (outcome.disposition, outcome.deleted) == ("retry_pending", False)

    @pytest.mark.asyncio
    async def test_orphan_response_loss_converges_on_exact_absence(
        self, provisioner_with_db
    ):
        present = self._probe(
            "present",
            vm_uid="captured-vm-uid",
            root_uid="captured-root-uid",
            credential_runtime_started=False,
        )
        absent = self._probe("absent", vm_uid=None, root_uid=None, known=True)
        provisioner_with_db._probe_vm_teardown_identity = AsyncMock(
            side_effect=[present, absent]
        )
        provisioner_with_db._delete_vm_with_identity = AsyncMock(return_value=False)

        outcome = await provisioner_with_db.delete_orphan_vm_captured(
            "job-1", self._identity(), purge_disk=True
        )

        assert (outcome.disposition, outcome.deleted) == ("completed", True)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("runtime_started", [True, None])
    async def test_orphan_with_possible_credential_runtime_is_preserved(
        self, provisioner_with_db, runtime_started
    ):
        provisioner_with_db._probe_vm_teardown_identity = AsyncMock(
            return_value=self._probe(
                "present",
                vm_uid="captured-vm-uid",
                root_uid="captured-root-uid",
                credential_runtime_started=runtime_started,
            )
        )
        provisioner_with_db._delete_vm_with_identity = AsyncMock()

        outcome = await provisioner_with_db.delete_orphan_vm_captured(
            "job-1", self._identity(), purge_disk=True
        )

        assert (outcome.disposition, outcome.deleted) == (
            "process_zero_unproven",
            False,
        )
        provisioner_with_db._delete_vm_with_identity.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_orphan_replacement_supersedes_without_delete(
        self, provisioner_with_db
    ):
        provisioner_with_db._probe_vm_teardown_identity = AsyncMock(
            return_value=self._probe(
                "present",
                vm_uid="replacement-vm-uid",
                root_uid="captured-root-uid",
            )
        )
        provisioner_with_db._delete_vm_with_identity = AsyncMock()

        outcome = await provisioner_with_db.delete_orphan_vm_captured(
            "job-1", self._identity(), purge_disk=True
        )

        assert (outcome.disposition, outcome.deleted) == (
            "identity_superseded",
            False,
        )
        provisioner_with_db._delete_vm_with_identity.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_http_delete_sends_purge_disk_query_param(
        self, provisioner_with_nats
    ):
        client = AsyncMock()
        response = MagicMock(status_code=200)
        response.json.return_value = {"status": "deleted"}
        response.raise_for_status = MagicMock()
        client.delete = AsyncMock(return_value=response)
        provisioner_with_nats._http_client = client

        await provisioner_with_nats._delete_http("job-1", purge_disk=False)

        assert client.delete.await_args.args[0] == "/vms/job-1"
        assert client.delete.await_args.kwargs["params"] == {"purge_disk": "false"}

    @pytest.mark.asyncio
    async def test_http_delete_omits_the_param_when_purging(
        self, provisioner_with_nats
    ):
        """Keeps the request byte-identical against an un-upgraded controller."""
        client = AsyncMock()
        response = MagicMock(status_code=200)
        response.json.return_value = {"status": "deleted"}
        response.raise_for_status = MagicMock()
        client.delete = AsyncMock(return_value=response)
        provisioner_with_nats._http_client = client

        await provisioner_with_nats._delete_http("job-1")

        assert client.delete.await_args.args[0] == "/vms/job-1"
        assert client.delete.await_args.kwargs["params"] == {}


class TestCreateVmDiskSize:
    """``disk_size`` rides the HTTP create payload only when a job asked for it."""

    @staticmethod
    def _http_provisioner(provisioner_disabled):
        response = MagicMock()
        response.raise_for_status = MagicMock()
        response.json.return_value = {"status": "created", "vm_name": "agent-vm-j"}
        provisioner_disabled._set_vm_context = AsyncMock()
        provisioner_disabled._db = MagicMock()
        provisioner_disabled._db.get_workspace_network_tier = AsyncMock(
            return_value="internet-only"
        )
        provisioner_disabled._http_client = MagicMock()
        provisioner_disabled._http_client.post = AsyncMock(return_value=response)
        return provisioner_disabled

    @pytest.mark.asyncio
    async def test_http_create_forwards_disk_size(self, provisioner_disabled):
        from shared.workspace_initialization import initialization_request

        prov = self._http_provisioner(provisioner_disabled)
        initialization = initialization_request([{"command": ["true"]}])
        await prov._create_http(
            job_id="job-disk",
            agent_config="developer",
            vm_image=None,
            cpu_cores=12,
            memory="24Gi",
            description="",
            disk_size="120Gi",
            initialization=initialization,
        )
        payload = prov._http_client.post.await_args.kwargs["json"]
        assert payload["disk_size"] == "120Gi"
        assert payload["initialization"] == initialization

    @pytest.mark.asyncio
    async def test_http_create_omits_disk_size_when_unset(self, provisioner_disabled):
        prov = self._http_provisioner(provisioner_disabled)
        await prov._create_http(
            job_id="job-nodisk",
            agent_config="developer",
            vm_image=None,
            cpu_cores=8,
            memory="16Gi",
            description="",
        )
        payload = prov._http_client.post.await_args.kwargs["json"]
        assert "disk_size" not in payload
