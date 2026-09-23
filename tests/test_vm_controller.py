"""Tests for the VM Controller — KubeVirt VM Lifecycle Manager.

Tests cover:
1. load_template() — template file loading, missing file handling
2. render_template() — variable substitution (JOB_ID, CPU_CORES, MEMORY, etc.)
3. init_k8s() — Kubernetes client initialization
4. connect_nats() — NATS connection and subscription setup
5. handle_create() — VM creation flow including Headscale key, template rendering, K8s API
6. handle_delete() — VM deletion including Headscale node cleanup
7. handle_status_query() — status query with request/reply
8. _publish_status() — status message format
9. run() and request_shutdown() — lifecycle management
10. Error handling — K8s API errors, NATS errors, Headscale failures, invalid requests
11. Edge cases — duplicate create requests, deleting non-existent VM, malformed messages
"""

import asyncio
import hashlib
import json
import sys
import types
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import yaml
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    PublicFormat,
    load_ssh_private_key,
)

from vm_controller.lifecycle_auth import sign_payload

# Repository assets are separate from the installed application packages.
project_root = Path(__file__).parent.parent

# --- kubernetes (only needed at import-time for type hints / constants) ------
_mock_k8s = types.ModuleType("kubernetes")
_mock_k8s_client = types.ModuleType("kubernetes.client")
_mock_k8s_config = types.ModuleType("kubernetes.config")
_mock_k8s_exc = types.ModuleType("kubernetes.client.exceptions")

LIFECYCLE_SECRET = b"controller-test-lifecycle-secret-at-least-32-bytes"
PROVISION_GENERATION = "00000000-0000-4000-8000-000000000001"
TEST_HOST_KEY_FINGERPRINT = "SHA256:" + ("A" * 43)
EXISTING_HOST_KEY_FINGERPRINT = "SHA256:" + ("B" * 43)


def test_controller_dockerfile_packages_lifecycle_auth_module() -> None:
    dockerfile = (project_root / "docker/Dockerfile.vm-controller").read_text(
        encoding="utf-8"
    )

    assert "COPY src/vm_controller/ ./src/vm_controller/" in dockerfile


class _FakeApiException(Exception):
    """Stand-in for kubernetes.client.exceptions.ApiException."""

    def __init__(self, status=500, body=""):
        self.status = status
        self.body = body
        super().__init__(f"{status}: {body}")


_mock_k8s_exc.ApiException = _FakeApiException  # type: ignore[attr-defined]
_mock_k8s_client.exceptions = _mock_k8s_exc  # type: ignore[attr-defined]
_mock_k8s_client.CustomObjectsApi = MagicMock  # type: ignore[attr-defined]
_mock_k8s_client.CoreV1Api = MagicMock  # type: ignore[attr-defined]
_mock_k8s_client.CoordinationV1Api = MagicMock  # type: ignore[attr-defined]
_mock_k8s.client = _mock_k8s_client  # type: ignore[attr-defined]
_mock_k8s.config = _mock_k8s_config  # type: ignore[attr-defined]
_mock_k8s_config.load_incluster_config = MagicMock()  # type: ignore[attr-defined]

from kubernetes.client import ApiClient as KubernetesApiClient  # noqa: E402

_K8S_STUB_MODULES = {
    "kubernetes": _mock_k8s,
    "kubernetes.client": _mock_k8s_client,
    "kubernetes.config": _mock_k8s_config,
    "kubernetes.client.exceptions": _mock_k8s_exc,
}
# Whatever was there before us — the real client is a declared orchestrator
# dependency, so on a full-suite run this is usually the genuine package.
_REAL_K8S_MODULES = {name: sys.modules.get(name) for name in _K8S_STUB_MODULES}


def _install_k8s_stubs() -> None:
    sys.modules.update(_K8S_STUB_MODULES)


def _restore_k8s_modules() -> None:
    for name, real in _REAL_K8S_MODULES.items():
        if real is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = real


_install_k8s_stubs()

# --- nats -------------------------------------------------------------------
_mock_nats = types.ModuleType("nats")
_mock_nats.connect = AsyncMock()  # type: ignore[attr-defined]
sys.modules.setdefault("nats", _mock_nats)

# ---------------------------------------------------------------------------
# NOW import the controller — the mocked modules make this succeed
# ---------------------------------------------------------------------------
from vm_controller.controller import (  # noqa: E402
    CDI_PLURAL,
    KUBEVIRT_GROUP,
    KUBEVIRT_PLURAL,
    KUBEVIRT_VERSION,
    LIFECYCLE_NONCE_GC_PAGE_LIMIT,
    VM_NAMESPACE,
    VMController,
    _generate_ssh_host_key,
    _openssh_sha256_fingerprint,
)

_restore_k8s_modules()


class TestSameClusterContracts:
    def test_generated_host_key_fingerprint_round_trip(self):
        material = _generate_ssh_host_key()

        private_key = load_ssh_private_key(material.private_key.encode("ascii"), None)
        derived_public = (
            private_key.public_key()
            .public_bytes(
                Encoding.OpenSSH,
                PublicFormat.OpenSSH,
            )
            .decode("ascii")
        )

        assert material.public_key == derived_public
        assert material.fingerprint == _openssh_sha256_fingerprint(derived_public)
        assert material.fingerprint.startswith("SHA256:")

    def test_secret_backed_render_injects_host_key_only_into_user_data(self):
        ctrl = _make_controller(headscale_available=False)
        ctrl.cloud_init_text = "#cloud-config\nruncmd:\n  - systemctl restart ssh\n"
        ctrl.template_text = """\
apiVersion: kubevirt.io/v1
kind: VirtualMachine
metadata:
  name: agent-vm-${JOB_ID}
spec:
  dataVolumeTemplates: []
  template:
    spec:
      domain: {}
      volumes:
        - name: cloud-init
          cloudInitNoCloud:
            secretRef:
              name: agent-vm-${JOB_ID}-cloudinit
"""

        manifest = ctrl.render_template(SAMPLE_JOB_CONFIG)
        user_data = manifest.pop("_srwCloudInitUserData")
        fingerprint = manifest.pop("_srwSSHHostKeyFingerprint")
        cloud_config = yaml.safe_load(user_data)

        assert cloud_config["ssh_deletekeys"] is True
        assert cloud_config["ssh_genkeytypes"] == []
        assert cloud_config["ssh_keys"]["ed25519_private"].startswith(
            "-----BEGIN OPENSSH PRIVATE KEY-----"
        )
        public_key = cloud_config["ssh_keys"]["ed25519_public"]
        assert fingerprint == _openssh_sha256_fingerprint(public_key)
        cloud_init = manifest["spec"]["template"]["spec"]["volumes"][0][
            "cloudInitNoCloud"
        ]
        assert cloud_init == {
            "secretRef": {"name": f"agent-vm-{SAMPLE_JOB_CONFIG['job_id']}-cloudinit"}
        }
        assert "PRIVATE KEY" not in yaml.safe_dump(manifest)

    def test_render_injects_placement_after_yaml_parse(self):
        ctrl = _make_controller(headscale_available=False)
        with (
            patch(
                "vm_controller.controller.VM_NODE_SELECTOR",
                {"srw.io/vm-node": "true"},
            ),
            patch(
                "vm_controller.controller.VM_TOLERATIONS",
                [{"key": "srw.io/vm-node", "operator": "Exists"}],
            ),
        ):
            manifest = ctrl.render_template(SAMPLE_JOB_CONFIG)

        vmi_spec = manifest["spec"]["template"]["spec"]
        assert vmi_spec["nodeSelector"] == {"srw.io/vm-node": "true"}
        assert vmi_spec["tolerations"] == [
            {"key": "srw.io/vm-node", "operator": "Exists"}
        ]

    def test_payload_network_tier_overrides_env_default(self):
        """The per-project tier sent by the orchestrator beats the chart default."""
        ctrl = _make_controller(headscale_available=False)
        ctrl.cloud_init_text = "#cloud-config\ntier: ${NETWORK_TIER}\n"
        config = {**SAMPLE_JOB_CONFIG, "network_tier": "home-allowed"}
        with patch("vm_controller.controller.VM_DEFAULT_NETWORK_TIER", "internet-only"):
            manifest = ctrl.render_template(config)
        rendered = yaml.safe_load(manifest.pop("_srwCloudInitUserData"))
        assert rendered["tier"] == "home-allowed"

    def test_env_default_network_tier_applies_when_payload_omits_it(self):
        ctrl = _make_controller(headscale_available=False)
        ctrl.cloud_init_text = "#cloud-config\ntier: ${NETWORK_TIER}\n"
        config = {k: v for k, v in SAMPLE_JOB_CONFIG.items() if k != "network_tier"}
        with patch("vm_controller.controller.VM_DEFAULT_NETWORK_TIER", "internet-only"):
            manifest = ctrl.render_template(config)
        rendered = yaml.safe_load(manifest.pop("_srwCloudInitUserData"))
        assert rendered["tier"] == "internet-only"

    def test_payload_network_tier_is_validated_when_env_default_is_empty(self):
        ctrl = _make_controller(headscale_available=False)
        config = {**SAMPLE_JOB_CONFIG, "network_tier": "NOT_VALID"}
        with patch("vm_controller.controller.VM_DEFAULT_NETWORK_TIER", ""):
            with pytest.raises(ValueError, match="network_tier"):
                ctrl.render_template(config)

    def test_cloud_init_receives_guest_token_url_and_tier(self):
        ctrl = _make_controller(headscale_available=False)
        ctrl.cloud_init_text = (
            "#cloud-config\ntoken: ${VM_AUTH_TOKEN}\n"
            "url: ${ORCHESTRATOR_URL}\ntier: ${NETWORK_TIER}\n"
        )
        config = {
            **SAMPLE_JOB_CONFIG,
            "provision_generation": PROVISION_GENERATION,
            "orchestrator_url": "http://payload-orchestrator:8085",
            "network_tier": "home-allowed",
        }
        with (
            patch("vm_controller.controller.LIFECYCLE_HMAC_SECRET", LIFECYCLE_SECRET),
            patch("vm_controller.controller.ORCHESTRATOR_URL", ""),
            patch("vm_controller.controller.VM_DEFAULT_NETWORK_TIER", ""),
        ):
            manifest = ctrl.render_template(config)

        rendered = yaml.safe_load(manifest.pop("_srwCloudInitUserData"))
        assert rendered["url"] == "http://payload-orchestrator:8085"
        assert rendered["tier"] == "home-allowed"
        assert len(rendered["token"]) == 64

    @pytest.mark.asyncio
    async def test_capacity_gate_reports_live_vm_count(self):
        ctrl = _make_controller(headscale_available=False)
        ctrl.k8s_client.list_namespaced_custom_object.return_value = {
            "items": [
                {"metadata": {"name": "agent-vm-one"}},
                {"metadata": {"name": "not-managed"}},
                {
                    "metadata": {
                        "name": "agent-vm-deleting",
                        "deletionTimestamp": "now",
                    }
                },
            ]
        }
        with patch("vm_controller.controller.VM_MAX_CONCURRENT", 1):
            result = await ctrl._capacity_wait("agent-vm-two")

        assert result == {
            "status": "waiting_capacity",
            "running_vms": 1,
            "max_concurrent_vms": 1,
        }

    @pytest.mark.asyncio
    async def test_concurrent_creates_cannot_oversubscribe_capacity(self, monkeypatch):
        ctrl = _make_controller(headscale_available=False)
        live_names: list[str] = []
        list_call = ctrl.k8s_client.list_namespaced_custom_object
        create_call = ctrl.k8s_client.create_namespaced_custom_object

        async def _interleaving_to_thread(func, /, *args, **kwargs):
            if func is list_call:
                return {"items": [{"metadata": {"name": name}} for name in live_names]}
            if func is create_call:
                # Give the competing task a chance to count before admission.
                # The controller-wide lock must prevent it from doing so.
                await asyncio.sleep(0)
                admitted = func(*args, **kwargs)
                live_names.append(kwargs["body"]["metadata"]["name"])
                return admitted
            return func(*args, **kwargs)

        monkeypatch.setattr(asyncio, "to_thread", _interleaving_to_thread)
        first = {**SAMPLE_JOB_CONFIG, "job_id": "capacity-one"}
        second = {**SAMPLE_JOB_CONFIG, "job_id": "capacity-two"}
        with patch("vm_controller.controller.VM_MAX_CONCURRENT", 1):
            results = await asyncio.gather(
                ctrl._do_create(first), ctrl._do_create(second)
            )

        assert sorted(result["status"] for result in results) == [
            "created",
            "waiting_capacity",
        ]
        assert create_call.call_count == 1

    @pytest.mark.asyncio
    async def test_cloud_init_secret_create_and_delete_calls_core_api(self):
        ctrl = _make_controller(headscale_available=False)
        await ctrl._ensure_cloud_init_secret(
            job_id=SAMPLE_JOB_CONFIG["job_id"],
            owner_kind="job",
            generation=PROVISION_GENERATION,
            user_data="#cloud-config\n",
            host_key_fingerprint=TEST_HOST_KEY_FINGERPRINT,
        )
        body = ctrl.core_api.create_namespaced_secret.call_args.kwargs["body"]
        assert body["stringData"] == {"userdata": "#cloud-config\n"}
        assert (
            body["metadata"]["labels"]["srw.io/owner-id"] == SAMPLE_JOB_CONFIG["job_id"]
        )

        await ctrl._delete_cloud_init_secret(SAMPLE_JOB_CONFIG["job_id"])
        ctrl.core_api.delete_namespaced_secret.assert_called_once_with(
            name=f"agent-vm-{SAMPLE_JOB_CONFIG['job_id']}-cloudinit",
            namespace=VM_NAMESPACE,
        )

    @pytest.mark.asyncio
    async def test_cloud_init_secret_409_owner_or_generation_mismatch_raises(self):
        ctrl = _make_controller(headscale_available=False)
        ctrl.core_api.create_namespaced_secret.side_effect = _FakeApiException(
            status=409, body="already exists"
        )
        ctrl.core_api.read_namespaced_secret.return_value = types.SimpleNamespace(
            metadata=types.SimpleNamespace(
                labels={
                    "srw.io/owner-kind": "thread",
                    "srw.io/owner-id": SAMPLE_JOB_CONFIG["job_id"],
                },
                annotations={
                    "srw.io/provision-generation": (
                        "00000000-0000-4000-8000-000000000099"
                    )
                },
            )
        )

        with pytest.raises(RuntimeError, match="another VM generation"):
            await ctrl._ensure_cloud_init_secret(
                job_id=SAMPLE_JOB_CONFIG["job_id"],
                owner_kind="job",
                generation=PROVISION_GENERATION,
                user_data="#cloud-config\n",
                host_key_fingerprint=TEST_HOST_KEY_FINGERPRINT,
            )

    @pytest.mark.asyncio
    async def test_cloud_init_secret_retry_returns_existing_generation_pin(self):
        ctrl = _make_controller(headscale_available=False)
        ctrl.core_api.create_namespaced_secret.side_effect = _FakeApiException(
            status=409, body="already exists"
        )
        ctrl.core_api.read_namespaced_secret.return_value = types.SimpleNamespace(
            metadata=types.SimpleNamespace(
                labels={
                    "srw.io/owner-kind": "job",
                    "srw.io/owner-id": SAMPLE_JOB_CONFIG["job_id"],
                },
                annotations={
                    "srw.io/provision-generation": PROVISION_GENERATION,
                    "srw.io/ssh-host-key-fingerprint": (EXISTING_HOST_KEY_FINGERPRINT),
                },
            )
        )

        created, fingerprint = await ctrl._ensure_cloud_init_secret(
            job_id=SAMPLE_JOB_CONFIG["job_id"],
            owner_kind="job",
            generation=PROVISION_GENERATION,
            user_data="#cloud-config\n",
            host_key_fingerprint=TEST_HOST_KEY_FINGERPRINT,
        )

        assert created is False
        assert fingerprint == EXISTING_HOST_KEY_FINGERPRINT

    @pytest.mark.asyncio
    async def test_cloud_init_secret_is_created_before_vm_and_owned_after_admission(
        self,
    ):
        ctrl = _make_controller(headscale_available=False)
        ctrl.cloud_init_text = (
            "#cloud-config\nssh_authorized_key: ${SSH_AUTHORIZED_KEY}\n"
            "vm_auth_token: ${VM_AUTH_TOKEN}\n"
        )
        events: list[str] = []
        original_admit = ctrl.k8s_client.create_namespaced_custom_object.side_effect

        def _create_secret(**_kwargs):
            events.append("secret-create")

        def _create_vm(**kwargs):
            events.append("vm-create")
            return original_admit(**kwargs)

        def _patch_secret(**_kwargs):
            events.append("secret-owner-patch")

        ctrl.core_api.create_namespaced_secret.side_effect = _create_secret
        ctrl.k8s_client.create_namespaced_custom_object.side_effect = _create_vm
        ctrl.core_api.patch_namespaced_secret.side_effect = _patch_secret
        config = {
            **SAMPLE_JOB_CONFIG,
            "provision_generation": PROVISION_GENERATION,
        }

        with (
            patch("vm_controller.controller.VM_MAX_CONCURRENT", 0),
            patch("vm_controller.controller.LIFECYCLE_HMAC_SECRET", LIFECYCLE_SECRET),
            patch.dict(
                "vm_controller.controller.os.environ",
                {"SSH_AUTHORIZED_KEY": "ssh-ed25519 AAAAtest"},
            ),
        ):
            result = await ctrl._do_create(config)

        assert result["status"] == "created"
        assert result["ssh_host_key_fingerprint"].startswith("SHA256:")
        assert events == ["secret-create", "vm-create", "secret-owner-patch"]
        vm_body = ctrl.k8s_client.create_namespaced_custom_object.call_args.kwargs[
            "body"
        ]
        assert "_srwCloudInitUserData" not in vm_body
        assert "_srwSSHHostKeyFingerprint" not in vm_body
        assert "PRIVATE KEY" not in yaml.safe_dump(vm_body)
        assert "PRIVATE KEY" not in yaml.safe_dump(result)
        secret_body = ctrl.core_api.create_namespaced_secret.call_args.kwargs["body"]
        secret_cloud_config = yaml.safe_load(secret_body["stringData"]["userdata"])
        assert secret_cloud_config["ssh_keys"]["ed25519_private"].startswith(
            "-----BEGIN OPENSSH PRIVATE KEY-----"
        )
        assert (
            secret_body["metadata"]["annotations"]["srw.io/ssh-host-key-fingerprint"]
            == result["ssh_host_key_fingerprint"]
        )
        owner = ctrl.core_api.patch_namespaced_secret.call_args.kwargs["body"][
            "metadata"
        ]["ownerReferences"][0]
        assert owner == {
            "apiVersion": "kubevirt.io/v1",
            "kind": "VirtualMachine",
            "name": f"agent-vm-{SAMPLE_JOB_CONFIG['job_id']}",
            "uid": "admitted-vm-uid-001",
            "controller": True,
            "blockOwnerDeletion": False,
        }

    @pytest.mark.asyncio
    async def test_create_rejects_empty_rendered_ssh_authorized_key(self):
        ctrl = _make_controller(headscale_available=False)
        ctrl.cloud_init_text = "#cloud-config\nkey: ${SSH_AUTHORIZED_KEY}\n"
        with (
            patch("vm_controller.controller.VM_MAX_CONCURRENT", 0),
            patch.dict(
                "vm_controller.controller.os.environ",
                {"SSH_AUTHORIZED_KEY": ""},
            ),
            pytest.raises(ValueError, match="SSH_AUTHORIZED_KEY must be non-empty"),
        ):
            await ctrl._do_create(SAMPLE_JOB_CONFIG)

        ctrl.core_api.create_namespaced_secret.assert_not_called()
        ctrl.k8s_client.create_namespaced_custom_object.assert_not_called()

    @pytest.mark.asyncio
    async def test_exhausted_vm_create_deletes_new_cloud_init_secret(self):
        ctrl = _make_controller(headscale_available=False)
        ctrl.cloud_init_text = (
            "#cloud-config\nssh_authorized_key: ${SSH_AUTHORIZED_KEY}\n"
            "vm_auth_token: ${VM_AUTH_TOKEN}\n"
        )
        ctrl.k8s_client.create_namespaced_custom_object.side_effect = _FakeApiException(
            status=409, body="VirtualMachine is being deleted"
        )
        config = {
            **SAMPLE_JOB_CONFIG,
            "provision_generation": PROVISION_GENERATION,
        }

        with (
            patch("vm_controller.controller.VM_MAX_CONCURRENT", 0),
            patch("vm_controller.controller.LIFECYCLE_HMAC_SECRET", LIFECYCLE_SECRET),
            patch.dict(
                "vm_controller.controller.os.environ",
                {"SSH_AUTHORIZED_KEY": "ssh-ed25519 AAAAtest"},
            ),
            patch("asyncio.sleep", new_callable=AsyncMock),
            pytest.raises(_FakeApiException),
        ):
            await ctrl._do_create(config)

        ctrl.core_api.delete_namespaced_secret.assert_called_once_with(
            name=f"agent-vm-{SAMPLE_JOB_CONFIG['job_id']}-cloudinit",
            namespace=VM_NAMESPACE,
        )

    @pytest.mark.asyncio
    async def test_vm_409_other_provision_generation_raises(self):
        ctrl = _make_controller(headscale_available=False)
        ctrl.k8s_client.create_namespaced_custom_object.side_effect = _FakeApiException(
            status=409, body="already exists"
        )
        ctrl.k8s_client.get_namespaced_custom_object.return_value = {
            "metadata": {
                "name": f"agent-vm-{SAMPLE_JOB_CONFIG['job_id']}",
                "uid": "different-generation-vm",
                "annotations": {
                    "srw.io/provision-generation": (
                        "00000000-0000-4000-8000-000000000099"
                    )
                },
            }
        }
        config = {
            **SAMPLE_JOB_CONFIG,
            "provision_generation": PROVISION_GENERATION,
        }

        with (
            patch("vm_controller.controller.VM_MAX_CONCURRENT", 0),
            patch("vm_controller.controller.LIFECYCLE_HMAC_SECRET", LIFECYCLE_SECRET),
            pytest.raises(RuntimeError, match="another provision generation"),
        ):
            await ctrl._do_create(config)

    @pytest.mark.asyncio
    async def test_status_returns_vmi_pod_ip_and_active_pod_uid(self):
        ctrl = _make_controller(headscale_available=False)
        job_id = SAMPLE_JOB_CONFIG["job_id"]

        def _get(**kwargs):
            if kwargs["plural"] == "virtualmachineinstances":
                return {
                    "status": {
                        "interfaces": [{"ipAddress": "10.42.1.23"}],
                        "activePods": {"launcher-pod-uid": "node-a"},
                    }
                }
            return {
                "metadata": {"name": f"agent-vm-{job_id}"},
                "status": {
                    "conditions": [{"type": "Ready", "status": "True"}],
                    "printableStatus": "Running",
                },
            }

        ctrl.k8s_client.get_namespaced_custom_object.side_effect = _get
        result = await ctrl._do_status(job_id)

        assert result["ready"] is True
        assert result["pod_ip"] == "10.42.1.23"
        assert result["active_pod_uid"] == "launcher-pod-uid"


@pytest.fixture(autouse=True, scope="module")
def _kubernetes_stubs_live_for_this_module():
    """Keep the stubs installed only while THIS module's tests run.

    The controller imports ``kubernetes`` lazily inside its methods (``init_k8s``
    and every ``except ApiException`` site), so the stubs must be live during the
    tests — but leaving them in ``sys.modules`` for the whole session shadowed the
    real client for everyone else, and the stub is missing attributes the real one
    has (``load_kube_config``, ``CoreV1Api``). That broke
    ``tests/test_infrastructure_metering_collector_runtime.py`` on full-suite runs.
    """

    _install_k8s_stubs()
    try:
        yield
    finally:
        _restore_k8s_modules()


# =============================================================================
# Sample VM template used across tests
# =============================================================================

SAMPLE_TEMPLATE = """\
apiVersion: kubevirt.io/v1
kind: VirtualMachine
metadata:
  name: agent-vm-${JOB_ID}
  labels:
    job-id: ${JOB_ID}
spec:
  dataVolumeTemplates:
    - metadata:
        name: agent-vm-${JOB_ID}-rootdisk
      spec:
        storage:
          accessModes:
            - ReadWriteOnce
          storageClassName: ${VM_STORAGE_CLASS}
          resources:
            requests:
              storage: ${VM_DISK_SIZE}
        source:
          registry:
            url: docker://${VM_IMAGE}
  template:
    spec:
      domain:
        cpu:
          cores: ${CPU_CORES}
        memory:
          guest: ${MEMORY}
      volumes:
        - name: rootdisk
          dataVolume:
            name: agent-vm-${JOB_ID}-rootdisk
        - name: cloud-init
          cloudInitNoCloud:
            userData: |
              NATS_URL=${NATS_URL}
              JOB_ID=${JOB_ID}
              ORCHESTRATOR_ID=${ORCHESTRATOR_ID}
              AGENT_CONFIG=${AGENT_CONFIG}
              DESCRIPTION=${DESCRIPTION}
              TAILSCALE_AUTH_KEY=${TAILSCALE_AUTH_KEY}
              HEADSCALE_URL=${HEADSCALE_URL}
"""

SAMPLE_JOB_CONFIG = {
    "job_id": "aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb",
    "agent_config": "developer",
    "vm_image": "ghcr.io/example/agent-vm:latest",
    "cpu_cores": 4,
    "memory": "8Gi",
    "nats_url": "nats://orchestrator-nats:4222",
    "description": "Build the feature module",
}

# =============================================================================
# Helpers
# =============================================================================


def make_nats_msg(data: dict, reply: str | None = None) -> MagicMock:
    """Create a mock NATS message with JSON-encoded data."""
    msg = MagicMock()
    msg.data = json.dumps(data).encode()
    msg.reply = reply
    return msg


def make_nats_msg_raw(raw_bytes: bytes, reply: str | None = None) -> MagicMock:
    """Create a mock NATS message with raw bytes (for malformed message tests)."""
    msg = MagicMock()
    msg.data = raw_bytes
    msg.reply = reply
    return msg


# =============================================================================
# Fixtures
# =============================================================================


def _make_headscale_mock(available: bool = True):
    hs = MagicMock()
    hs.is_available = available
    hs.is_ready = available
    hs.last_error = None
    hs.init = AsyncMock()
    hs.close = AsyncMock()
    if available:
        hs.create_auth_key = AsyncMock(return_value="hskey-preauth-abc123")
        hs.delete_node = AsyncMock(return_value=True)
    else:
        hs.create_auth_key = AsyncMock(return_value=None)
        hs.delete_node = AsyncMock(return_value=False)
    return hs


def _make_controller(headscale_available: bool = True) -> VMController:
    """Build a VMController with fully mocked I/O dependencies."""
    ctrl = VMController.__new__(VMController)
    ctrl.headscale = _make_headscale_mock(headscale_available)
    ctrl.template_text = SAMPLE_TEMPLATE
    ctrl._shutdown = asyncio.Event()

    # Mock NATS client
    ctrl.nc = AsyncMock()
    ctrl.nc.is_connected = True
    ctrl.nc.publish = AsyncMock()
    ctrl.nc.subscribe = AsyncMock()
    ctrl.nc.drain = AsyncMock()

    # Mock K8s client
    ctrl.k8s_client = MagicMock()
    ctrl.core_api = MagicMock()
    ctrl.coordination_api = MagicMock()

    def carrier_signature(**kwargs):
        with patch("vm_controller.controller.LIFECYCLE_HMAC_SECRET", LIFECYCLE_SECRET):
            return VMController._workspace_cleanup_carrier_signature(**kwargs)

    ctrl._workspace_cleanup_carrier_signature = carrier_signature
    ctrl.coordination_api.create_namespaced_lease.return_value = {}
    ctrl.coordination_api.list_namespaced_lease.return_value = {"items": []}

    async def acquire_cleanup_reservation(**kwargs):
        outcomes = {
            "controller_rootdisk_delete": "deleted",
            "controller_failed_dv_recreate": "recreated",
            "controller_rootdisk_adopt": "adopted",
        }
        default_carrier = ctrl._parse_workspace_cleanup_carrier(
            _cleanup_carrier_lease()
        )
        default_carrier.update(
            {
                "owner_kind": kwargs["owner_kind"],
                "owner_id": kwargs["owner_id"],
                "source": kwargs["source"],
                "outcome": outcomes[kwargs["source"]],
                "name": f"agent-vm-{kwargs['owner_id']}-rootdisk",
                "old_dv_uid": kwargs["dv_uid"],
                "old_pvc_uid": kwargs["pvc_uid"],
                "provision_generation": kwargs["provision_generation"],
            }
        )
        return {
            "allowed": True,
            "admission_id": "00000000-0000-4000-8000-000000000901",
            "completed_outcome": None,
            "carrier": default_carrier,
        }

    ctrl._acquire_workspace_cleanup_reservation = AsyncMock(
        side_effect=acquire_cleanup_reservation
    )
    ctrl._complete_workspace_cleanup_reservation = AsyncMock()
    ctrl._resume_workspace_cleanup_reservation = AsyncMock(
        return_value={"allowed": True, "completed_outcome": None}
    )
    ctrl._refresh_workspace_cleanup_carrier = AsyncMock(side_effect=lambda value: value)
    ctrl.coordination_api.replace_namespaced_lease.side_effect = (
        lambda **kwargs: kwargs["body"]
    )

    def _read_pvc(**kwargs):
        name = kwargs["name"]
        owner_id = name[len("agent-vm-") : -len("-rootdisk")]
        owner_kind = "thread" if owner_id.startswith("thread-") else "job"
        return types.SimpleNamespace(
            metadata=types.SimpleNamespace(
                name=name,
                uid=f"root-pvc-uid-{owner_id}",
                labels={
                    "srw.io/owner-kind": owner_kind,
                    "srw.io/owner-id": owner_id,
                },
                owner_references=[
                    types.SimpleNamespace(
                        kind="DataVolume",
                        uid="rootdisk-dv-uid",
                        controller=True,
                    )
                ],
            )
        )

    ctrl.core_api.read_namespaced_persistent_volume_claim.side_effect = _read_pvc

    def _admit_object(**kwargs):
        body = kwargs.get("body") or {}
        metadata = dict(body.get("metadata") or {})
        if kwargs.get("plural") == KUBEVIRT_PLURAL:
            metadata["uid"] = "admitted-vm-uid-001"
        return {**body, "metadata": metadata}

    ctrl.k8s_client.create_namespaced_custom_object.side_effect = _admit_object
    ctrl.k8s_client.get_namespaced_custom_object.return_value = {
        "metadata": {
            "name": f"agent-vm-{SAMPLE_JOB_CONFIG['job_id']}",
            "uid": "existing-vm-uid-002",
        }
    }

    ctrl.http_runner = None

    return ctrl


def _cleanup_carrier_lease(
    *,
    admission_id: str = "00000000-0000-4000-8000-000000000901",
    request_id: str = "00000000-0000-4000-8000-000000000902",
    owner_id: str = SAMPLE_JOB_CONFIG["job_id"],
    source: str = "controller_rootdisk_delete",
    outcome: str = "deleted",
    generation: str = PROVISION_GENERATION,
    old_dv_uid: str = "old-dv-uid",
    old_pvc_uid: str = "00000000-0000-4000-8000-000000000903",
    successor_dv_uid: str | None = None,
    successor_pvc_uid: str | None = None,
) -> dict:
    annotations = {
        "srw.io/cleanup-admission-id": admission_id,
        "srw.io/cleanup-request-id": request_id,
        "srw.io/cleanup-intent-digest": "sha256:exact-controller-cleanup-intent",
        "srw.io/cleanup-owner-kind": "job",
        "srw.io/cleanup-owner-id": owner_id,
        "srw.io/cleanup-source": source,
        "srw.io/cleanup-outcome": outcome,
        "srw.io/cleanup-rootdisk-name": f"agent-vm-{owner_id}-rootdisk",
        "srw.io/cleanup-old-dv-uid": old_dv_uid,
        "srw.io/cleanup-old-pvc-uid": old_pvc_uid,
        "srw.io/cleanup-generation": generation,
        "srw.io/cleanup-nonce": "00000000-0000-4000-8000-000000000904",
    }
    if successor_dv_uid is not None:
        annotations["srw.io/cleanup-successor-dv-uid"] = successor_dv_uid
    if successor_pvc_uid is not None:
        annotations["srw.io/cleanup-successor-pvc-uid"] = successor_pvc_uid
    from vm_controller.controller import _CLEANUP_ANNOTATIONS

    with patch("vm_controller.controller.LIFECYCLE_HMAC_SECRET", LIFECYCLE_SECRET):
        annotations["srw.io/cleanup-carrier-signature"] = (
            VMController._workspace_cleanup_carrier_signature(
                name=f"srw-cleanup-{admission_id.replace('-', '')}",
                uid="cleanup-carrier-uid",
                values={
                    key: annotations.get(value, "")
                    for key, value in _CLEANUP_ANNOTATIONS.items()
                },
            )
        )
    return {
        "metadata": {
            "name": f"srw-cleanup-{admission_id.replace('-', '')}",
            "namespace": VM_NAMESPACE,
            "uid": "cleanup-carrier-uid",
            "resourceVersion": "7",
            "labels": {"srw.io/vm-workspace-cleanup-carrier": "true"},
            "annotations": annotations,
        }
    }


@pytest.fixture(autouse=True)
def _scoped_orchestrator_id():
    """Patch the module-level ORCHESTRATOR_ID for every test in this file.

    The controller reads ORCHESTRATOR_ID at module import time and uses it
    to scope vm.lifecycle.* subjects. Real deployments set the env var via
    Helm; tests use this fixture so existing assertions on subject names
    keep working with a stable "test-oid" suffix. The dedicated
    TestOrchestratorIdRequired class below opts out by patching to "".
    """
    with patch("vm_controller.controller.ORCHESTRATOR_ID", "test-oid"):
        yield


@pytest.mark.parametrize(
    "field",
    [
        "srw.io/cleanup-old-dv-uid",
        "srw.io/cleanup-generation",
        "srw.io/cleanup-successor-dv-uid",
    ],
)
def test_cleanup_carrier_rejects_tampered_durable_identity(controller, field):
    lease = _cleanup_carrier_lease(
        successor_dv_uid="successor-dv", successor_pvc_uid="successor-pvc"
    )
    lease["metadata"]["annotations"][field] = "tampered"
    with pytest.raises(RuntimeError, match="authentication"):
        controller._parse_workspace_cleanup_carrier(lease)


def test_cleanup_carrier_rejects_copied_lease_uid(controller):
    lease = _cleanup_carrier_lease()
    lease["metadata"]["uid"] = "copied-lease"
    with pytest.raises(RuntimeError, match="authentication"):
        controller._parse_workspace_cleanup_carrier(lease)


def test_cleanup_carrier_survives_elapsed_time_but_fails_after_key_rotation(controller):
    lease = _cleanup_carrier_lease()
    del controller._workspace_cleanup_carrier_signature
    with (
        patch("vm_controller.controller.LIFECYCLE_HMAC_SECRET", LIFECYCLE_SECRET),
        patch("time.time", return_value=9999999999),
    ):
        assert (
            controller._parse_workspace_cleanup_carrier(lease)["carrier_sealed"] is True
        )
    with patch(
        "vm_controller.controller.LIFECYCLE_HMAC_SECRET",
        b"rotated-controller-secret-at-least-32-bytes",
    ):
        with pytest.raises(RuntimeError, match="authentication"):
            controller._parse_workspace_cleanup_carrier(lease)


@pytest.fixture(autouse=True)
def _run_sync_kubernetes_mocks_inline(monkeypatch):
    """Keep MagicMock K8s calls deterministic while production uses to_thread."""

    async def _inline(func, /, *args, **kwargs):
        return func(*args, **kwargs)

    monkeypatch.setattr(asyncio, "to_thread", _inline)


@pytest.fixture
def controller():
    return _make_controller(headscale_available=True)


@pytest.fixture
def controller_no_headscale():
    return _make_controller(headscale_available=False)


# =============================================================================
# Tests: load_template()
# =============================================================================


class TestLoadTemplate:
    """Tests for VM template file loading."""

    def test_load_template_success(self, tmp_path):
        """Loading a valid template file stores its content."""
        template_path = tmp_path / "vm-template.yaml"
        template_path.write_text(SAMPLE_TEMPLATE)

        ctrl = _make_controller()
        with patch("vm_controller.controller.VM_TEMPLATE_PATH", str(template_path)):
            ctrl.load_template()

        assert ctrl.template_text == SAMPLE_TEMPLATE
        assert "${JOB_ID}" in ctrl.template_text

    def test_load_template_missing_file(self, tmp_path):
        """Loading a non-existent template calls sys.exit(1)."""
        ctrl = _make_controller()
        with patch(
            "vm_controller.controller.VM_TEMPLATE_PATH",
            str(tmp_path / "nonexistent.yaml"),
        ):
            with pytest.raises(SystemExit) as exc_info:
                ctrl.load_template()
            assert exc_info.value.code == 1

    def test_load_template_empty_file(self, tmp_path):
        """Loading an empty template file stores an empty string."""
        template_path = tmp_path / "vm-template.yaml"
        template_path.write_text("")

        ctrl = _make_controller()
        with patch("vm_controller.controller.VM_TEMPLATE_PATH", str(template_path)):
            ctrl.load_template()

        assert ctrl.template_text == ""

    def test_load_template_preserves_multiline(self, tmp_path):
        """Template loading preserves multi-line YAML content."""
        content = "line1: value1\nline2:\n  nested: true\n  list:\n    - item1\n"
        template_path = tmp_path / "vm-template.yaml"
        template_path.write_text(content)

        ctrl = _make_controller()
        with patch("vm_controller.controller.VM_TEMPLATE_PATH", str(template_path)):
            ctrl.load_template()

        assert ctrl.template_text == content


# =============================================================================
# Tests: render_template()
# =============================================================================


class TestRenderTemplate:
    """Tests for VM template variable substitution."""

    def test_render_all_placeholders(self, controller):
        """All placeholders are substituted with job config values."""
        result = controller.render_template(SAMPLE_JOB_CONFIG, "ts-key-123")

        assert result["metadata"]["name"] == f"agent-vm-{SAMPLE_JOB_CONFIG['job_id']}"
        assert result["metadata"]["labels"]["job-id"] == SAMPLE_JOB_CONFIG["job_id"]
        assert result["metadata"]["labels"]["srw.io/owner-kind"] == "job"
        assert (
            result["metadata"]["labels"]["srw.io/owner-id"]
            == SAMPLE_JOB_CONFIG["job_id"]
        )
        assert result["spec"]["template"]["metadata"]["labels"] == {
            "srw.io/owner-kind": "job",
            "srw.io/owner-id": SAMPLE_JOB_CONFIG["job_id"],
        }
        assert result["spec"]["dataVolumeTemplates"][0]["metadata"]["labels"] == {
            "srw.io/owner-kind": "job",
            "srw.io/owner-id": SAMPLE_JOB_CONFIG["job_id"],
        }

        spec = result["spec"]["template"]["spec"]
        assert spec["domain"]["cpu"]["cores"] == 4
        assert spec["domain"]["memory"]["guest"] == "8Gi"
        assert (
            result["spec"]["dataVolumeTemplates"][0]["spec"]["source"]["registry"][
                "url"
            ]
            == f"docker://{SAMPLE_JOB_CONFIG['vm_image']}"
        )

        user_data = spec["volumes"][1]["cloudInitNoCloud"]["userData"]
        assert SAMPLE_JOB_CONFIG["job_id"] in user_data
        assert "developer" in user_data
        assert "Build the feature module" in user_data
        assert "ts-key-123" in user_data

    def test_render_uses_defaults_for_missing_keys(self, controller):
        """Missing optional keys fall back to module-level defaults."""
        minimal_config = {"job_id": "minimal-job-id"}

        with (
            patch("vm_controller.controller.DEFAULT_VM_IMAGE", "default-image:v1"),
            patch("vm_controller.controller.DEFAULT_CPU", 2),
            patch("vm_controller.controller.DEFAULT_MEMORY", "4Gi"),
        ):
            result = controller.render_template(minimal_config)

        assert result["metadata"]["name"] == "agent-vm-minimal-job-id"
        spec = result["spec"]["template"]["spec"]
        assert spec["domain"]["cpu"]["cores"] == 2
        assert spec["domain"]["memory"]["guest"] == "4Gi"
        assert (
            result["spec"]["dataVolumeTemplates"][0]["spec"]["source"]["registry"][
                "url"
            ]
            == "docker://default-image:v1"
        )

    def test_render_default_agent_config(self, controller):
        """agent_config defaults to 'worker_base' when not specified."""
        config = {"job_id": "test-id"}
        result = controller.render_template(config)

        user_data = result["spec"]["template"]["spec"]["volumes"][1][
            "cloudInitNoCloud"
        ]["userData"]
        assert "AGENT_CONFIG=worker_base" in user_data

    def test_render_empty_tailscale_key(self, controller):
        """Empty tailscale auth key results in empty placeholder."""
        config = {"job_id": "test-id"}
        result = controller.render_template(config, tailscale_auth_key="")

        user_data = result["spec"]["template"]["spec"]["volumes"][1][
            "cloudInitNoCloud"
        ]["userData"]
        assert "TAILSCALE_AUTH_KEY=\n" in user_data

    def test_render_injects_orchestrator_id(self, controller):
        """ORCHESTRATOR_ID is substituted into cloud-init so the in-VM
        management-daemon publishes to per-orchestrator scoped subjects."""
        config = {"job_id": "test-id"}
        result = controller.render_template(config, tailscale_auth_key="")

        user_data = result["spec"]["template"]["spec"]["volumes"][1][
            "cloudInitNoCloud"
        ]["userData"]
        assert "ORCHESTRATOR_ID=test-oid" in user_data

    def test_render_injects_headscale_url_from_env(self, controller):
        """HEADSCALE_URL is read from environment and injected."""
        config = {"job_id": "test-id"}

        with patch.dict("os.environ", {"HEADSCALE_URL": "https://hs.example.com"}):
            result = controller.render_template(config, "key-abc")

        user_data = result["spec"]["template"]["spec"]["volumes"][1][
            "cloudInitNoCloud"
        ]["userData"]
        assert "HEADSCALE_URL=https://hs.example.com" in user_data

    def test_render_uses_local_nats_url(self, controller):
        """NATS_URL is always the local leaf node, not the one from job config."""
        config = {
            "job_id": "test-id",
            "nats_url": "nats://remote-orchestrator:4222",
        }

        with patch("vm_controller.controller.NATS_URL", "nats://local-leaf:4222"):
            result = controller.render_template(config)

        user_data = result["spec"]["template"]["spec"]["volumes"][1][
            "cloudInitNoCloud"
        ]["userData"]
        assert "NATS_URL=nats://local-leaf:4222" in user_data
        assert "nats://remote-orchestrator:4222" not in user_data

    def test_render_empty_description(self, controller):
        """Empty description is substituted as empty string."""
        config = {"job_id": "test-id"}
        result = controller.render_template(config)

        user_data = result["spec"]["template"]["spec"]["volumes"][1][
            "cloudInitNoCloud"
        ]["userData"]
        assert "DESCRIPTION=\n" in user_data

    def test_render_description_with_special_characters(self, controller):
        """Description with special characters is substituted verbatim."""
        config = {
            "job_id": "test-id",
            "description": "Parse $HOME & /tmp files; echo 'hello'",
        }
        result = controller.render_template(config)

        user_data = result["spec"]["template"]["spec"]["volumes"][1][
            "cloudInitNoCloud"
        ]["userData"]
        assert "Parse $HOME & /tmp files; echo 'hello'" in user_data

    def test_render_returns_valid_yaml_dict(self, controller):
        """render_template returns a dict (parsed YAML), not a string."""
        result = controller.render_template(SAMPLE_JOB_CONFIG)
        assert isinstance(result, dict)
        assert "apiVersion" in result
        assert result["kind"] == "VirtualMachine"

    def test_render_cpu_cores_as_integer(self, controller):
        """CPU cores are rendered as an integer, not a string."""
        config = {"job_id": "test-id", "cpu_cores": 8}
        result = controller.render_template(config)

        cores = result["spec"]["template"]["spec"]["domain"]["cpu"]["cores"]
        assert isinstance(cores, int)
        assert cores == 8

    def test_render_stamps_thread_owner_on_vm_vmi_and_data_volume(self, controller):
        config = {**SAMPLE_JOB_CONFIG, "job_id": "thread-123", "entity_type": "thread"}

        result = controller.render_template(config)

        for labels in (
            result["metadata"]["labels"],
            result["spec"]["template"]["metadata"]["labels"],
            result["spec"]["dataVolumeTemplates"][0]["metadata"]["labels"],
        ):
            assert labels["srw.io/owner-kind"] == "thread"
            assert labels["srw.io/owner-id"] == "thread-123"

    def test_render_rejects_unknown_owner_kind(self, controller):
        config = {**SAMPLE_JOB_CONFIG, "entity_type": "customer"}

        with pytest.raises(ValueError, match="entity_type"):
            controller.render_template(config)


# =============================================================================
# Tests: init_k8s()
# =============================================================================


class TestInitK8s:
    """Tests for Kubernetes client initialization."""

    def test_init_k8s_loads_in_cluster_config(self):
        """init_k8s calls load_incluster_config."""
        ctrl = _make_controller()
        ctrl.k8s_client = None

        mock_config = MagicMock()
        mock_client = MagicMock()
        mock_api = MagicMock()
        mock_client.CustomObjectsApi.return_value = mock_api

        with patch.dict(
            "sys.modules",
            {
                "kubernetes": MagicMock(client=mock_client, config=mock_config),
                "kubernetes.client": mock_client,
                "kubernetes.config": mock_config,
            },
        ):
            ctrl.init_k8s()

        mock_config.load_incluster_config.assert_called_once()

    def test_init_k8s_sets_custom_objects_api(self):
        """After init_k8s, k8s_client is a CustomObjectsApi instance."""
        ctrl = _make_controller()
        ctrl.k8s_client = None

        mock_api = MagicMock()
        mock_client = MagicMock()
        mock_client.CustomObjectsApi.return_value = mock_api

        with patch.dict(
            "sys.modules",
            {
                "kubernetes": MagicMock(client=mock_client, config=MagicMock()),
                "kubernetes.client": mock_client,
                "kubernetes.config": MagicMock(),
            },
        ):
            ctrl.init_k8s()

        assert ctrl.k8s_client is mock_api

    def test_init_k8s_sets_core_api_for_pvc_identity(self):
        ctrl = _make_controller()
        ctrl.core_api = None

        mock_core_api = MagicMock()
        mock_client = MagicMock()
        mock_client.CoreV1Api.return_value = mock_core_api

        with patch.dict(
            "sys.modules",
            {
                "kubernetes": MagicMock(client=mock_client, config=MagicMock()),
                "kubernetes.client": mock_client,
                "kubernetes.config": MagicMock(),
            },
        ):
            ctrl.init_k8s()

        assert ctrl.core_api is mock_core_api

    def test_init_k8s_sets_coordination_api_for_durable_replay_claims(self):
        ctrl = _make_controller()
        ctrl.coordination_api = None

        mock_coordination_api = MagicMock()
        mock_client = MagicMock()
        mock_client.CoordinationV1Api.return_value = mock_coordination_api

        with patch.dict(
            "sys.modules",
            {
                "kubernetes": MagicMock(client=mock_client, config=MagicMock()),
                "kubernetes.client": mock_client,
                "kubernetes.config": MagicMock(),
            },
        ):
            ctrl.init_k8s()

        assert ctrl.coordination_api is mock_coordination_api


# =============================================================================
# Tests: connect_nats()
# =============================================================================


class TestConnectNats:
    """Tests for NATS connection setup."""

    @pytest.mark.asyncio
    async def test_connect_nats_success(self):
        """connect_nats establishes a NATS connection."""
        ctrl = _make_controller()
        ctrl.nc = None

        mock_nc = AsyncMock()
        mock_nats_mod = MagicMock()
        mock_nats_mod.connect = AsyncMock(return_value=mock_nc)

        with patch.dict("sys.modules", {"nats": mock_nats_mod}):
            await ctrl.connect_nats()

        assert ctrl.nc is mock_nc
        mock_nats_mod.connect.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_connect_nats_uses_configured_url(self):
        """connect_nats passes the module-level NATS_URL."""
        ctrl = _make_controller()
        ctrl.nc = None

        mock_nats_mod = MagicMock()
        mock_nats_mod.connect = AsyncMock(return_value=AsyncMock())

        with (
            patch("vm_controller.controller.NATS_URL", "nats://custom:4222"),
            patch.dict("sys.modules", {"nats": mock_nats_mod}),
        ):
            await ctrl.connect_nats()

        first_positional = mock_nats_mod.connect.call_args[0][0]
        assert first_positional == "nats://custom:4222"

    @pytest.mark.asyncio
    async def test_connect_nats_infinite_reconnect(self):
        """connect_nats configures infinite reconnect attempts (-1)."""
        ctrl = _make_controller()
        ctrl.nc = None

        mock_nats_mod = MagicMock()
        mock_nats_mod.connect = AsyncMock(return_value=AsyncMock())

        with patch.dict("sys.modules", {"nats": mock_nats_mod}):
            await ctrl.connect_nats()

        kw = mock_nats_mod.connect.call_args[1]
        assert kw["max_reconnect_attempts"] == -1

    @pytest.mark.asyncio
    async def test_connect_nats_error_propagates(self):
        """Connection failure propagates the exception."""
        ctrl = _make_controller()
        ctrl.nc = None

        mock_nats_mod = MagicMock()
        mock_nats_mod.connect = AsyncMock(side_effect=ConnectionError("refused"))

        with patch.dict("sys.modules", {"nats": mock_nats_mod}):
            with pytest.raises(ConnectionError, match="refused"):
                await ctrl.connect_nats()


# =============================================================================
# Tests: handle_create()
# =============================================================================


class TestHandleCreate:
    """Tests for VM creation handler."""

    @pytest.mark.asyncio
    async def test_create_vm_success(self, controller):
        """Successful VM creation calls K8s API and publishes 'created' status."""
        msg = make_nats_msg(SAMPLE_JOB_CONFIG)
        await controller.handle_create(msg)

        controller.k8s_client.create_namespaced_custom_object.assert_called_once()
        kw = controller.k8s_client.create_namespaced_custom_object.call_args[1]
        assert kw["group"] == "kubevirt.io"
        assert kw["version"] == "v1"
        assert kw["plural"] == "virtualmachines"

        # Verify status published
        controller.nc.publish.assert_awaited()
        subject, raw = controller.nc.publish.call_args[0]
        assert subject == "vm.lifecycle.status.test-oid"
        payload = json.loads(raw.decode())
        assert payload["status"] == "created"
        assert payload["job_id"] == SAMPLE_JOB_CONFIG["job_id"]
        assert payload["vm_uid"] == "admitted-vm-uid-001"
        assert payload["rootdisk_pvc_uid"] == (
            f"root-pvc-uid-{SAMPLE_JOB_CONFIG['job_id']}"
        )

    @pytest.mark.asyncio
    async def test_create_omits_unattested_rootdisk_pvc_uid(self, controller):
        controller.core_api.read_namespaced_persistent_volume_claim.return_value = (
            types.SimpleNamespace(
                metadata=types.SimpleNamespace(
                    name="agent-vm-spoofed-rootdisk",
                    uid="spoofed-pvc-uid",
                    labels={
                        "srw.io/owner-kind": "job",
                        "srw.io/owner-id": SAMPLE_JOB_CONFIG["job_id"],
                    },
                )
            )
        )
        controller.core_api.list_namespaced_persistent_volume_claim.side_effect = None
        controller.core_api.list_namespaced_persistent_volume_claim.return_value = types.SimpleNamespace(
            items=[
                controller.core_api.read_namespaced_persistent_volume_claim.return_value
            ]
        )
        controller.core_api.read_namespaced_persistent_volume_claim.side_effect = None

        with patch("vm_controller.controller.VM_ROOTDISK_PVC_UID_ATTEMPTS", 1):
            result = await controller._do_create(SAMPLE_JOB_CONFIG)

        assert result["status"] == "created"
        assert "rootdisk_pvc_uid" not in result

    @pytest.mark.asyncio
    async def test_create_vm_generates_headscale_key(self, controller):
        """VM creation generates a Headscale pre-auth key when available."""
        msg = make_nats_msg(SAMPLE_JOB_CONFIG)
        await controller.handle_create(msg)

        controller.headscale.create_auth_key.assert_awaited_once_with(
            SAMPLE_JOB_CONFIG["job_id"]
        )

    @pytest.mark.asyncio
    async def test_create_vm_without_headscale(self, controller_no_headscale):
        """VM creation proceeds without Headscale when unavailable."""
        msg = make_nats_msg(SAMPLE_JOB_CONFIG)
        await controller_no_headscale.handle_create(msg)

        controller_no_headscale.headscale.create_auth_key.assert_not_awaited()
        controller_no_headscale.k8s_client.create_namespaced_custom_object.assert_called_once()

        payload = json.loads(
            controller_no_headscale.nc.publish.call_args[0][1].decode()
        )
        assert payload["status"] == "created"

    @pytest.mark.asyncio
    async def test_create_vm_defers_when_headscale_key_unavailable(self, controller):
        """No key → defer the create instead of building an unreachable VM.

        Regression for the 2026-07-17/07-25 outage: a VM built without a
        tailnet pre-auth key boots and heartbeats but can never be reached
        over SSH, so it silently burned the whole 3 × 10 min provisioning
        budget before failing the job. See knowledge-base/knowledge/issues/
        vm_controller_headscale_latch_kills_provisioning.md.
        """
        controller.headscale.create_auth_key = AsyncMock(return_value=None)
        controller.headscale.last_error = "ConnectError: connection refused"
        msg = make_nats_msg(SAMPLE_JOB_CONFIG)
        await controller.handle_create(msg)

        controller.k8s_client.create_namespaced_custom_object.assert_not_called()
        payload = json.loads(controller.nc.publish.call_args[0][1].decode())
        assert payload["status"] == "waiting_headscale"
        assert payload["job_id"] == SAMPLE_JOB_CONFIG["job_id"]
        assert "connection refused" in payload["headscale_error"]

    @pytest.mark.asyncio
    async def test_create_vm_defers_without_last_error(self, controller):
        """Deferral still carries a usable reason when last_error is unset."""
        controller.headscale.create_auth_key = AsyncMock(return_value=None)
        controller.headscale.last_error = None
        msg = make_nats_msg(SAMPLE_JOB_CONFIG)
        await controller.handle_create(msg)

        controller.k8s_client.create_namespaced_custom_object.assert_not_called()
        payload = json.loads(controller.nc.publish.call_args[0][1].decode())
        assert payload["status"] == "waiting_headscale"
        assert payload["headscale_error"]

    @pytest.mark.asyncio
    async def test_create_vm_renders_manifest_correctly(self, controller):
        """Created VM manifest contains the correct job_id in its name."""
        msg = make_nats_msg(SAMPLE_JOB_CONFIG)
        await controller.handle_create(msg)

        kw = controller.k8s_client.create_namespaced_custom_object.call_args[1]
        assert (
            kw["body"]["metadata"]["name"] == f"agent-vm-{SAMPLE_JOB_CONFIG['job_id']}"
        )

    @pytest.mark.asyncio
    async def test_create_vm_uses_correct_namespace(self, controller):
        """VM is created in the configured namespace."""
        msg = make_nats_msg(SAMPLE_JOB_CONFIG)
        with patch("vm_controller.controller.VM_NAMESPACE", "test-namespace"):
            await controller.handle_create(msg)

        kw = controller.k8s_client.create_namespaced_custom_object.call_args[1]
        assert kw["namespace"] == "test-namespace"

    @pytest.mark.asyncio
    async def test_create_vm_k8s_api_error(self, controller):
        """K8s API error during creation publishes 'failed' status."""
        controller.k8s_client.create_namespaced_custom_object.side_effect = (
            RuntimeError("Internal Server Error")
        )

        msg = make_nats_msg(SAMPLE_JOB_CONFIG)
        await controller.handle_create(msg)

        payload = json.loads(controller.nc.publish.call_args[0][1].decode())
        assert payload["status"] == "failed"
        assert payload["job_id"] == SAMPLE_JOB_CONFIG["job_id"]
        assert "error" in payload

    @pytest.mark.asyncio
    async def test_create_vm_conflict_retry_succeeds(self, controller):
        """409 Conflict with 'is being deleted' triggers retry, second call succeeds."""
        conflict = _FakeApiException(
            status=409, body="vm agent-vm-test is being deleted"
        )

        controller.k8s_client.create_namespaced_custom_object.side_effect = [
            conflict,
            MagicMock(),  # success on second attempt
        ]

        msg = make_nats_msg(SAMPLE_JOB_CONFIG)
        with patch("asyncio.sleep", new_callable=AsyncMock):
            await controller.handle_create(msg)

        assert controller.k8s_client.create_namespaced_custom_object.call_count == 2
        payload = json.loads(controller.nc.publish.call_args[0][1].decode())
        assert payload["status"] == "created"

    @pytest.mark.asyncio
    async def test_create_vm_conflict_exhausted_retries(self, controller):
        """409 Conflict persisting after all retries publishes 'failed'."""

        def make_conflict():
            return _FakeApiException(
                status=409, body="vm agent-vm-test is being deleted"
            )

        # 13 calls = initial + 12 retries
        controller.k8s_client.create_namespaced_custom_object.side_effect = [
            make_conflict() for _ in range(14)
        ]

        msg = make_nats_msg(SAMPLE_JOB_CONFIG)
        with patch("asyncio.sleep", new_callable=AsyncMock):
            await controller.handle_create(msg)

        payload = json.loads(controller.nc.publish.call_args[0][1].decode())
        assert payload["status"] == "failed"

    @pytest.mark.asyncio
    async def test_create_vm_409_already_exists_is_idempotent_success(self, controller):
        """Plain 409 AlreadyExists is idempotent success, not a failure.

        The VM name is agent-vm-<job_id>, so an existing live VM IS this
        job's VM (a duplicate/racing create lost to one that succeeded).
        Propagating the 409 as 'failed' parked two healthy loop jobs — see
        knowledge-history/done/golden_image_cold_import_fails_inflight_vm_jobs.md §B.
        """
        conflict = _FakeApiException(status=409, body="already exists")

        controller.k8s_client.create_namespaced_custom_object.side_effect = conflict

        msg = make_nats_msg(SAMPLE_JOB_CONFIG)
        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            await controller.handle_create(msg)

        # No sleep = no retry loop; single create call
        mock_sleep.assert_not_awaited()
        assert controller.k8s_client.create_namespaced_custom_object.call_count == 1
        payload = json.loads(controller.nc.publish.call_args[0][1].decode())
        assert payload["status"] == "created"
        assert payload["vm_name"] == f"agent-vm-{SAMPLE_JOB_CONFIG['job_id']}"
        assert payload["vm_uid"] == "existing-vm-uid-002"
        controller.k8s_client.get_namespaced_custom_object.assert_called_once_with(
            group=KUBEVIRT_GROUP,
            version=KUBEVIRT_VERSION,
            namespace=VM_NAMESPACE,
            plural=KUBEVIRT_PLURAL,
            name=f"agent-vm-{SAMPLE_JOB_CONFIG['job_id']}",
        )

    @pytest.mark.asyncio
    async def test_create_vm_without_admitted_uid_fails_closed(self, controller):
        controller.k8s_client.create_namespaced_custom_object.side_effect = None
        controller.k8s_client.create_namespaced_custom_object.return_value = {}
        controller.k8s_client.get_namespaced_custom_object.return_value = {
            "metadata": {"name": f"agent-vm-{SAMPLE_JOB_CONFIG['job_id']}"}
        }

        await controller.handle_create(make_nats_msg(SAMPLE_JOB_CONFIG))

        payload = json.loads(controller.nc.publish.call_args[0][1].decode())
        assert payload["status"] == "failed"
        assert "metadata.uid" in payload["error"]

    @pytest.mark.asyncio
    async def test_create_vm_malformed_json(self, controller):
        """Malformed JSON in NATS message publishes 'failed' status."""
        msg = make_nats_msg_raw(b"not valid json")
        await controller.handle_create(msg)

        payload = json.loads(controller.nc.publish.call_args[0][1].decode())
        assert payload["status"] == "failed"
        assert payload["job_id"] == "unknown"

    @pytest.mark.asyncio
    async def test_create_vm_missing_job_id_key_fails(self, controller):
        """Missing job_id key in payload causes KeyError during render, publishes failure."""
        msg = make_nats_msg({"agent_config": "developer"})
        await controller.handle_create(msg)

        # render_template requires job_config["job_id"] — missing key fails
        controller.k8s_client.create_namespaced_custom_object.assert_not_called()
        payload = json.loads(controller.nc.publish.call_args[0][1].decode())
        assert payload["status"] == "failed"
        assert payload["job_id"] == "unknown"

    @pytest.mark.asyncio
    async def test_create_vm_status_includes_vm_name(self, controller):
        """Published status includes the vm_name derived from the manifest."""
        msg = make_nats_msg(SAMPLE_JOB_CONFIG)
        await controller.handle_create(msg)

        payload = json.loads(controller.nc.publish.call_args[0][1].decode())
        assert payload["vm_name"] == f"agent-vm-{SAMPLE_JOB_CONFIG['job_id']}"

    @pytest.mark.asyncio
    async def test_create_vm_status_includes_namespace(self, controller):
        """Published status includes the VM namespace."""
        msg = make_nats_msg(SAMPLE_JOB_CONFIG)
        with patch("vm_controller.controller.VM_NAMESPACE", "custom-ns"):
            await controller.handle_create(msg)

        payload = json.loads(controller.nc.publish.call_args[0][1].decode())
        assert payload["namespace"] == "custom-ns"

    @pytest.mark.asyncio
    async def test_create_vm_minimal_config(self, controller):
        """VM creation works with only job_id in payload."""
        msg = make_nats_msg({"job_id": "only-job-id"})
        await controller.handle_create(msg)

        controller.k8s_client.create_namespaced_custom_object.assert_called_once()
        payload = json.loads(controller.nc.publish.call_args[0][1].decode())
        assert payload["status"] == "created"
        assert payload["job_id"] == "only-job-id"

    @pytest.mark.asyncio
    async def test_create_vm_with_extra_fields_in_payload(self, controller):
        """Create handler ignores extra fields in the payload."""
        config = dict(SAMPLE_JOB_CONFIG)
        config["extra_field"] = "should be ignored"
        config["another_extra"] = 42

        msg = make_nats_msg(config)
        await controller.handle_create(msg)

        controller.k8s_client.create_namespaced_custom_object.assert_called_once()
        payload = json.loads(controller.nc.publish.call_args[0][1].decode())
        assert payload["status"] == "created"


# =============================================================================
# Tests: handle_delete()
# =============================================================================


class TestHandleDelete:
    """Tests for VM deletion handler."""

    @pytest.mark.asyncio
    async def test_delete_vm_success(self, controller):
        """Successful VM deletion calls K8s API and publishes 'deleted' status."""
        job_id = "delete-me-1234"
        msg = make_nats_msg({"job_id": job_id})
        await controller.handle_delete(msg)

        # A terminal delete now touches two resources (VM, then the rootdisk
        # DataVolume) — pick out the VM call.
        kw = _calls_for(
            controller.k8s_client.delete_namespaced_custom_object, KUBEVIRT_PLURAL
        )[0].kwargs
        assert kw["name"] == f"agent-vm-{job_id}"
        assert kw["group"] == "kubevirt.io"
        assert kw["version"] == "v1"
        assert kw["plural"] == "virtualmachines"

        payload = json.loads(controller.nc.publish.call_args[0][1].decode())
        assert payload["status"] == "deleted"
        assert payload["job_id"] == job_id
        assert payload["vm_name"] == f"agent-vm-{job_id}"

    @pytest.mark.asyncio
    async def test_delete_without_disk_authority_keeps_headscale_node(self, controller):
        """A retained disk keeps the matching Headscale identity."""
        job_id = "delete-me-5678"
        msg = make_nats_msg({"job_id": job_id})
        await controller.handle_delete(msg)

        controller.headscale.delete_node.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_delete_vm_without_headscale(self, controller_no_headscale):
        """VM deletion skips Headscale cleanup when unavailable."""
        job_id = "delete-no-hs"
        msg = make_nats_msg({"job_id": job_id})
        await controller_no_headscale.handle_delete(msg)

        controller_no_headscale.headscale.delete_node.assert_not_awaited()
        assert (
            len(
                _calls_for(
                    controller_no_headscale.k8s_client.delete_namespaced_custom_object,
                    KUBEVIRT_PLURAL,
                )
            )
            == 1
        )

    @pytest.mark.asyncio
    async def test_delete_vm_already_gone_404(self, controller):
        """Deleting a non-existent VM (404) is treated as success."""
        controller.k8s_client.delete_namespaced_custom_object.side_effect = (
            _FakeApiException(status=404, body="Not Found")
        )

        job_id = "already-gone"
        msg = make_nats_msg({"job_id": job_id})
        await controller.handle_delete(msg)

        payload = json.loads(controller.nc.publish.call_args[0][1].decode())
        assert payload["status"] == "deleted"
        assert payload["job_id"] == job_id

    @pytest.mark.asyncio
    async def test_delete_vm_k8s_api_error_non_404(self, controller):
        """K8s API error (non-404) during deletion publishes 'delete_failed'."""
        controller.k8s_client.delete_namespaced_custom_object.side_effect = (
            _FakeApiException(status=503, body="Service Unavailable")
        )

        job_id = "fail-delete"
        msg = make_nats_msg({"job_id": job_id})
        await controller.handle_delete(msg)

        payload = json.loads(controller.nc.publish.call_args[0][1].decode())
        assert payload["status"] == "delete_failed"
        assert payload["job_id"] == job_id
        assert "error" in payload

    @pytest.mark.asyncio
    async def test_delete_vm_generic_exception(self, controller):
        """Generic exception during deletion publishes 'delete_failed'."""
        controller.k8s_client.delete_namespaced_custom_object.side_effect = (
            RuntimeError("unexpected failure")
        )

        msg = make_nats_msg({"job_id": "err-test"})
        await controller.handle_delete(msg)

        payload = json.loads(controller.nc.publish.call_args[0][1].decode())
        assert payload["status"] == "delete_failed"

    @pytest.mark.asyncio
    async def test_delete_vm_malformed_json(self, controller):
        """Malformed JSON in delete message publishes 'delete_failed'."""
        msg = make_nats_msg_raw(b"{{invalid json")
        await controller.handle_delete(msg)

        payload = json.loads(controller.nc.publish.call_args[0][1].decode())
        assert payload["status"] == "delete_failed"
        assert payload["job_id"] == "unknown"

    @pytest.mark.asyncio
    async def test_delete_vm_missing_job_id_key(self, controller):
        """Missing job_id key in delete payload publishes failure."""
        msg = make_nats_msg({"vm_name": "agent-vm-xyz"})
        await controller.handle_delete(msg)

        payload = json.loads(controller.nc.publish.call_args[0][1].decode())
        assert payload["status"] == "delete_failed"

    @pytest.mark.asyncio
    async def test_delete_vm_uses_correct_namespace(self, controller):
        """VM deletion uses the configured namespace."""
        msg = make_nats_msg({"job_id": "ns-test"})
        with patch("vm_controller.controller.VM_NAMESPACE", "my-namespace"):
            await controller.handle_delete(msg)

        kw = controller.k8s_client.delete_namespaced_custom_object.call_args[1]
        assert kw["namespace"] == "my-namespace"

    @pytest.mark.asyncio
    async def test_delete_vm_headscale_failure_does_not_block(self, controller):
        """Headscale node deletion failure does not prevent VM deletion success."""
        controller.headscale.delete_node = AsyncMock(return_value=False)

        msg = make_nats_msg({"job_id": "hs-fail-ok"})
        await controller.handle_delete(msg)

        assert _calls_for(
            controller.k8s_client.delete_namespaced_custom_object, KUBEVIRT_PLURAL
        )
        payload = json.loads(controller.nc.publish.call_args[0][1].decode())
        assert payload["status"] == "deleted"

    @pytest.mark.asyncio
    async def test_delete_constructs_vm_name_from_job_id(self, controller):
        """Delete handler constructs vm_name as 'agent-vm-{job_id}'."""
        job_id = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
        msg = make_nats_msg({"job_id": job_id})
        await controller.handle_delete(msg)

        kw = _calls_for(
            controller.k8s_client.delete_namespaced_custom_object, KUBEVIRT_PLURAL
        )[0].kwargs
        assert kw["name"] == f"agent-vm-{job_id}"


# =============================================================================
# Tests: handle_status_query()
# =============================================================================


class TestHandleStatusQuery:
    """Tests for VM status query handler."""

    @pytest.mark.asyncio
    async def test_status_query_ready_vm(self, controller):
        """Status query for a ready VM returns ready=True and correct phase."""
        job_id = "status-test"
        controller.k8s_client.get_namespaced_custom_object.return_value = {
            "metadata": {"name": f"agent-vm-{job_id}"},
            "status": {
                "conditions": [
                    {"type": "Ready", "status": "True"},
                    {"type": "Initialized", "status": "True"},
                ],
                "printableStatus": "Running",
                "created": True,
            },
        }

        msg = make_nats_msg({"job_id": job_id}, reply="reply.inbox.123")
        await controller.handle_status_query(msg)

        subject, raw = controller.nc.publish.call_args[0]
        assert subject == "reply.inbox.123"
        payload = json.loads(raw.decode())
        assert payload["ready"] is True
        assert payload["phase"] == "Running"
        assert payload["created"] is True
        assert payload["job_id"] == job_id
        assert payload["vm_name"] == f"agent-vm-{job_id}"

    @pytest.mark.asyncio
    async def test_status_query_not_ready_vm(self, controller):
        """Status query for a non-ready VM returns ready=False."""
        job_id = "not-ready"
        controller.k8s_client.get_namespaced_custom_object.return_value = {
            "metadata": {"name": f"agent-vm-{job_id}"},
            "status": {
                "conditions": [{"type": "Ready", "status": "False"}],
                "printableStatus": "Provisioning",
                "created": False,
            },
        }

        msg = make_nats_msg({"job_id": job_id}, reply="reply.inbox.456")
        await controller.handle_status_query(msg)

        payload = json.loads(controller.nc.publish.call_args[0][1].decode())
        assert payload["ready"] is False
        assert payload["phase"] == "Provisioning"
        assert payload["created"] is False

    @pytest.mark.asyncio
    async def test_status_query_no_conditions(self, controller):
        """Status query for a VM with no conditions returns ready=False."""
        job_id = "no-conditions"
        controller.k8s_client.get_namespaced_custom_object.return_value = {
            "metadata": {"name": f"agent-vm-{job_id}"},
            "status": {"printableStatus": "Starting"},
        }

        msg = make_nats_msg({"job_id": job_id}, reply="reply.inbox.789")
        await controller.handle_status_query(msg)

        payload = json.loads(controller.nc.publish.call_args[0][1].decode())
        assert payload["ready"] is False
        assert payload["phase"] == "Starting"

    @pytest.mark.asyncio
    async def test_status_query_empty_status(self, controller):
        """Status query for a VM with no status block returns defaults."""
        job_id = "empty-status"
        controller.k8s_client.get_namespaced_custom_object.return_value = {
            "metadata": {"name": f"agent-vm-{job_id}"},
        }

        msg = make_nats_msg({"job_id": job_id}, reply="reply.inbox.000")
        await controller.handle_status_query(msg)

        payload = json.loads(controller.nc.publish.call_args[0][1].decode())
        assert payload["ready"] is False
        assert payload["phase"] == "Unknown"
        assert payload["created"] is False

    @pytest.mark.asyncio
    async def test_status_query_no_reply_publishes_to_status_subject(self, controller):
        """When msg.reply is None, status is published to vm.lifecycle.status.{oid}."""
        job_id = "no-reply"
        controller.k8s_client.get_namespaced_custom_object.return_value = {
            "metadata": {"name": f"agent-vm-{job_id}"},
            "status": {
                "conditions": [],
                "printableStatus": "Running",
                "created": True,
            },
        }

        msg = make_nats_msg({"job_id": job_id}, reply=None)
        await controller.handle_status_query(msg)

        subject = controller.nc.publish.call_args[0][0]
        assert subject == "vm.lifecycle.status.test-oid"

    @pytest.mark.asyncio
    async def test_status_query_vm_not_found_error(self, controller):
        """Status query for a non-existent VM returns error via reply."""
        controller.k8s_client.get_namespaced_custom_object.side_effect = Exception(
            "Not Found"
        )

        msg = make_nats_msg({"job_id": "missing"}, reply="reply.inbox.err")
        await controller.handle_status_query(msg)

        payload = json.loads(controller.nc.publish.call_args[0][1].decode())
        assert payload["status"] == "query_failed"
        assert payload["job_id"] == "missing"
        assert "error" in payload

    @pytest.mark.asyncio
    async def test_status_query_error_without_reply(self, controller):
        """Error without reply subject publishes to vm.lifecycle.status.{oid}."""
        controller.k8s_client.get_namespaced_custom_object.side_effect = Exception(
            "Server Error"
        )

        msg = make_nats_msg({"job_id": "error-no-reply"}, reply=None)
        await controller.handle_status_query(msg)

        subject = controller.nc.publish.call_args[0][0]
        assert subject == "vm.lifecycle.status.test-oid"
        payload = json.loads(controller.nc.publish.call_args[0][1].decode())
        assert payload["status"] == "query_failed"

    @pytest.mark.asyncio
    async def test_status_query_error_with_reply(self, controller):
        """Error with reply subject sends error to the reply address."""
        controller.k8s_client.get_namespaced_custom_object.side_effect = Exception(
            "Timeout"
        )

        msg = make_nats_msg({"job_id": "error-reply"}, reply="reply.inbox.err2")
        await controller.handle_status_query(msg)

        subject = controller.nc.publish.call_args[0][0]
        assert subject == "reply.inbox.err2"
        payload = json.loads(controller.nc.publish.call_args[0][1].decode())
        assert payload["status"] == "query_failed"

    @pytest.mark.asyncio
    async def test_status_query_malformed_json(self, controller):
        """Malformed JSON in status query returns error."""
        msg = make_nats_msg_raw(b"not json", reply="reply.inbox.bad")
        await controller.handle_status_query(msg)

        payload = json.loads(controller.nc.publish.call_args[0][1].decode())
        assert payload["status"] == "query_failed"

    @pytest.mark.asyncio
    async def test_status_query_multiple_conditions_ready_false(self, controller):
        """Ready is True only when type=Ready and status=True is present."""
        job_id = "multi-cond"
        controller.k8s_client.get_namespaced_custom_object.return_value = {
            "metadata": {"name": f"agent-vm-{job_id}"},
            "status": {
                "conditions": [
                    {"type": "LiveMigratable", "status": "True"},
                    {"type": "Ready", "status": "False"},
                    {"type": "AgentConnected", "status": "True"},
                ],
                "printableStatus": "Starting",
            },
        }

        msg = make_nats_msg({"job_id": job_id}, reply="reply.inbox.multi")
        await controller.handle_status_query(msg)

        payload = json.loads(controller.nc.publish.call_args[0][1].decode())
        assert payload["ready"] is False

    @pytest.mark.asyncio
    async def test_status_query_uses_correct_k8s_coordinates(self, controller):
        """Status query uses KubeVirt group/version/plural."""
        job_id = "coords-test"
        controller.k8s_client.get_namespaced_custom_object.return_value = {
            "status": {"conditions": [], "printableStatus": "Running"},
        }

        msg = make_nats_msg({"job_id": job_id}, reply="reply.test")
        await controller.handle_status_query(msg)

        calls = controller.k8s_client.get_namespaced_custom_object.call_args_list
        coordinates = {
            (call.kwargs["group"], call.kwargs["version"], call.kwargs["plural"])
            for call in calls
        }
        assert coordinates == {
            ("kubevirt.io", "v1", "virtualmachines"),
            ("kubevirt.io", "v1", "virtualmachineinstances"),
        }
        assert {call.kwargs["name"] for call in calls} == {f"agent-vm-{job_id}"}

    @pytest.mark.asyncio
    async def test_status_query_with_extra_fields(self, controller):
        """Status query ignores extra fields in the payload."""
        controller.k8s_client.get_namespaced_custom_object.return_value = {
            "status": {"conditions": [], "printableStatus": "Running"},
        }

        msg = make_nats_msg(
            {"job_id": "extra-fields", "extra": "ignored"},
            reply="reply.test",
        )
        await controller.handle_status_query(msg)

        payload = json.loads(controller.nc.publish.call_args[0][1].decode())
        assert payload["job_id"] == "extra-fields"


# =============================================================================
# Tests: handle_list() / http_list() — orphan-sweep inventory
# =============================================================================


class TestHandleList:
    """vm.lifecycle.list request/reply — inventory for the VM orphan sweep."""

    @staticmethod
    def _wire_vms(controller, items):
        controller.k8s_client.list_namespaced_custom_object.return_value = {
            "items": items
        }

    @staticmethod
    def _vm_item(name: str, created: str = "2026-07-09T10:00:00Z", phase="Running"):
        return {
            "metadata": {"name": name, "creationTimestamp": created},
            "status": {"printableStatus": phase},
        }

    @pytest.mark.asyncio
    async def test_lists_agent_vms_only(self, controller):
        """Golden DataVolume names and foreign objects are filtered out."""
        self._wire_vms(
            controller,
            [
                self._vm_item("agent-vm-job-uuid-1"),
                self._vm_item("agent-vm-golden-abc123"),  # golden — excluded
                self._vm_item("some-other-vm"),  # foreign — excluded
            ],
        )
        msg = make_nats_msg({"orchestrator_id": "test"}, reply="reply.inbox.list")
        await controller.handle_list(msg)

        subject, raw = controller.nc.publish.call_args[0]
        assert subject == "reply.inbox.list"
        payload = json.loads(raw.decode())
        assert len(payload["vms"]) == 1
        vm = payload["vms"][0]
        assert vm["vm_name"] == "agent-vm-job-uuid-1"
        assert vm["entity_id"] == "job-uuid-1"
        assert vm["created_at"] == "2026-07-09T10:00:00Z"
        assert vm["phase"] == "Running"

    @pytest.mark.asyncio
    async def test_empty_cluster_replies_empty_list(self, controller):
        self._wire_vms(controller, [])
        msg = make_nats_msg({}, reply="reply.inbox.empty")
        await controller.handle_list(msg)
        payload = json.loads(controller.nc.publish.call_args[0][1].decode())
        assert payload == {"vms": []}

    @pytest.mark.asyncio
    async def test_k8s_error_replies_list_failed(self, controller):
        controller.k8s_client.list_namespaced_custom_object.side_effect = RuntimeError(
            "api down"
        )
        msg = make_nats_msg({}, reply="reply.inbox.err")
        await controller.handle_list(msg)
        payload = json.loads(controller.nc.publish.call_args[0][1].decode())
        assert payload["status"] == "list_failed"
        assert "vms" not in payload

    @pytest.mark.asyncio
    async def test_no_reply_subject_publishes_nothing(self, controller):
        """A list is only meaningful request/reply — no status fallback."""
        self._wire_vms(controller, [self._vm_item("agent-vm-x")])
        msg = make_nats_msg({}, reply=None)
        await controller.handle_list(msg)
        controller.nc.publish.assert_not_called()


class TestHttpList:
    """GET /vms — same inventory over the HTTP transport."""

    @pytest.mark.asyncio
    async def test_http_list_returns_vms(self, controller):
        controller.k8s_client.list_namespaced_custom_object.return_value = {
            "items": [
                {
                    "metadata": {
                        "name": "agent-vm-j1",
                        "creationTimestamp": "2026-07-09T09:00:00Z",
                    },
                    "status": {"printableStatus": "Running"},
                }
            ]
        }
        resp = await controller.http_list(MagicMock())
        assert resp.status == 200
        payload = json.loads(resp.body.decode())
        assert payload["vms"][0]["entity_id"] == "j1"

    @pytest.mark.asyncio
    async def test_http_list_k8s_error_500s(self, controller):
        controller.k8s_client.list_namespaced_custom_object.side_effect = RuntimeError(
            "api down"
        )
        resp = await controller.http_list(MagicMock())
        assert resp.status == 500
        assert json.loads(resp.body.decode())["status"] == "list_failed"


# =============================================================================
# Tests: _publish_status()
# =============================================================================


class TestPublishStatus:
    """Tests for status publishing."""

    @pytest.mark.asyncio
    async def test_publish_status_success(self, controller):
        """_publish_status publishes JSON to vm.lifecycle.status.{oid}."""
        payload = {"job_id": "test", "status": "created", "vm_name": "agent-vm-test"}
        await controller._publish_status("test", payload)

        subject, raw = controller.nc.publish.call_args[0]
        assert subject == "vm.lifecycle.status.test-oid"
        assert json.loads(raw.decode()) == payload

    @pytest.mark.asyncio
    async def test_publish_status_nats_error_does_not_raise(self, controller):
        """_publish_status logs error but does not propagate NATS failure."""
        controller.nc.publish.side_effect = Exception("NATS disconnected")
        # Should not raise
        await controller._publish_status("test", {"status": "test"})

    @pytest.mark.asyncio
    async def test_publish_status_encodes_utf8(self, controller):
        """_publish_status encodes payload as UTF-8 JSON bytes."""
        payload = {"job_id": "test", "description": "Umlauts: \u00e4\u00f6\u00fc"}
        await controller._publish_status("test", payload)

        raw_bytes = controller.nc.publish.call_args[0][1]
        assert isinstance(raw_bytes, bytes)
        decoded = json.loads(raw_bytes.decode("utf-8"))
        assert "\u00e4\u00f6\u00fc" in decoded["description"]

    @pytest.mark.asyncio
    async def test_publish_status_subject_is_always_lifecycle_status(self, controller):
        """_publish_status always uses the vm.lifecycle.status.{oid} subject."""
        for payload in [
            {"status": "created"},
            {"status": "deleted"},
            {"status": "failed"},
        ]:
            controller.nc.publish.reset_mock()
            await controller._publish_status("any-job", payload)
            assert (
                controller.nc.publish.call_args[0][0] == "vm.lifecycle.status.test-oid"
            )


# =============================================================================
# Tests: run() and request_shutdown()
# =============================================================================


class TestRunAndShutdown:
    """Tests for controller lifecycle management."""

    @pytest.mark.asyncio
    async def test_run_subscribes_to_all_subjects(self, controller):
        """run() subscribes to create, delete, and get subjects."""
        controller._shutdown.set()

        with (
            patch.object(controller, "load_template"),
            patch.object(controller, "init_k8s"),
            patch.object(controller, "connect_nats", new_callable=AsyncMock),
        ):
            await controller.run()

        subjects = [call[0][0] for call in controller.nc.subscribe.call_args_list]
        assert "vm.lifecycle.create.test-oid" in subjects
        assert "vm.lifecycle.delete.test-oid" in subjects
        assert "vm.lifecycle.get.test-oid" in subjects
        # Regression: flat subjects must NOT be subscribed
        assert "vm.lifecycle.create" not in subjects
        assert "vm.lifecycle.delete" not in subjects
        assert "vm.lifecycle.get" not in subjects

    @pytest.mark.asyncio
    async def test_run_calls_init_sequence_in_order(self, controller):
        """run() calls load_template, init_k8s, headscale.init, connect_nats."""
        controller._shutdown.set()
        call_order = []

        def track(name):
            def fn(*_a, **_kw):
                call_order.append(name)

            return fn

        async def track_async(name):
            async def fn(*_a, **_kw):
                call_order.append(name)

            return fn

        with (
            patch.object(
                controller, "load_template", side_effect=track("load_template")
            ),
            patch.object(controller, "init_k8s", side_effect=track("init_k8s")),
            patch.object(controller, "connect_nats", side_effect=track("connect_nats")),
        ):
            controller.headscale.init = AsyncMock(side_effect=track("headscale_init"))
            await controller.run()

        assert call_order == [
            "load_template",
            "init_k8s",
            "headscale_init",
            "connect_nats",
        ]

    @pytest.mark.asyncio
    async def test_run_drains_nats_on_shutdown(self, controller):
        """run() drains NATS connection during shutdown."""
        controller._shutdown.set()

        with (
            patch.object(controller, "load_template"),
            patch.object(controller, "init_k8s"),
            patch.object(controller, "connect_nats", new_callable=AsyncMock),
        ):
            await controller.run()

        controller.nc.drain.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_run_closes_headscale_on_shutdown(self, controller):
        """run() closes the Headscale client during shutdown."""
        controller._shutdown.set()

        with (
            patch.object(controller, "load_template"),
            patch.object(controller, "init_k8s"),
            patch.object(controller, "connect_nats", new_callable=AsyncMock),
        ):
            await controller.run()

        controller.headscale.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_run_skips_drain_when_disconnected(self, controller):
        """run() skips NATS drain when already disconnected."""
        controller._shutdown.set()
        controller.nc.is_connected = False

        with (
            patch.object(controller, "load_template"),
            patch.object(controller, "init_k8s"),
            patch.object(controller, "connect_nats", new_callable=AsyncMock),
        ):
            await controller.run()

        controller.nc.drain.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_run_skips_drain_when_nc_is_none(self):
        """run() handles nc=None during shutdown gracefully."""
        ctrl = _make_controller()
        ctrl.nc = None
        ctrl._shutdown.set()

        mock_nc = AsyncMock()
        mock_nc.is_connected = True
        mock_nc.subscribe = AsyncMock()
        mock_nc.drain = AsyncMock()

        async def fake_connect():
            ctrl.nc = mock_nc

        with (
            patch.object(ctrl, "load_template"),
            patch.object(ctrl, "init_k8s"),
            patch.object(ctrl, "connect_nats", side_effect=fake_connect),
        ):
            await ctrl.run()

        mock_nc.drain.assert_awaited_once()

    def test_request_shutdown_sets_event(self, controller):
        """request_shutdown() sets the internal shutdown event."""
        assert not controller._shutdown.is_set()
        controller.request_shutdown()
        assert controller._shutdown.is_set()

    @pytest.mark.asyncio
    async def test_request_shutdown_unblocks_run(self, controller):
        """request_shutdown() unblocks the run() wait loop."""
        with (
            patch.object(controller, "load_template"),
            patch.object(controller, "init_k8s"),
            patch.object(controller, "connect_nats", new_callable=AsyncMock),
        ):

            async def delayed_shutdown():
                await asyncio.sleep(0.05)
                controller.request_shutdown()

            task = asyncio.create_task(delayed_shutdown())
            await controller.run()
            await task

        assert controller._shutdown.is_set()

    @pytest.mark.asyncio
    async def test_run_subscribe_callbacks_are_handlers(self, controller):
        """run() wires the correct handler to each subscription."""
        controller._shutdown.set()

        with (
            patch.object(controller, "load_template"),
            patch.object(controller, "init_k8s"),
            patch.object(controller, "connect_nats", new_callable=AsyncMock),
        ):
            await controller.run()

        # Build a map of subject -> callback name
        cb_map = {}
        for call in controller.nc.subscribe.call_args_list:
            subject = call[0][0]
            cb = call[1].get("cb") or (call[0][1] if len(call[0]) > 1 else None)
            cb_map[subject] = (
                getattr(cb, "__name__", None) or getattr(cb, "__func__", cb).__name__
            )

        assert cb_map["vm.lifecycle.create.test-oid"] == "handle_create"
        assert cb_map["vm.lifecycle.delete.test-oid"] == "handle_delete"
        assert cb_map["vm.lifecycle.get.test-oid"] == "handle_status_query"


# =============================================================================
# Tests: ORCHESTRATOR_ID gating
# =============================================================================


class TestOrchestratorIdRequired:
    """run() refuses to subscribe to flat vm.lifecycle.* subjects.

    Without per-orchestrator scoping the controller would race other
    controllers sharing the NATS hub and provision duplicate VMs for every
    create request. The startup-time sys.exit(1) is the failsafe.
    """

    @pytest.mark.asyncio
    async def test_run_exits_when_orchestrator_id_unset(self, controller):
        controller._shutdown.set()
        with (
            patch("vm_controller.controller.ORCHESTRATOR_ID", ""),
            patch.object(controller, "load_template"),
            patch.object(controller, "init_k8s"),
            patch.object(controller, "connect_nats", new_callable=AsyncMock),
        ):
            with pytest.raises(SystemExit) as exc:
                await controller.run()
            assert exc.value.code == 1

    @pytest.mark.asyncio
    async def test_run_does_not_subscribe_when_orchestrator_id_unset(self, controller):
        controller._shutdown.set()
        with (
            patch("vm_controller.controller.ORCHESTRATOR_ID", ""),
            patch.object(controller, "load_template"),
            patch.object(controller, "init_k8s"),
            patch.object(controller, "connect_nats", new_callable=AsyncMock),
        ):
            with pytest.raises(SystemExit):
                await controller.run()

        controller.nc.subscribe.assert_not_awaited()


# =============================================================================
# Tests: main() entry point
# =============================================================================


class TestMain:
    """Tests for the main() entry point and signal handling."""

    def test_main_registers_signal_handlers(self):
        """main() registers SIGTERM and SIGINT handlers."""
        import signal as signal_module
        from vm_controller.controller import main

        registered = {}
        with (
            patch("vm_controller.controller.VMController") as mock_cls,
            patch(
                "vm_controller.controller.signal.signal",
                side_effect=lambda s, h: registered.update({s: h}),
            ),
            patch("vm_controller.controller.asyncio.run"),
        ):
            mock_cls.return_value = MagicMock()
            main()

        assert signal_module.SIGTERM in registered
        assert signal_module.SIGINT in registered

    def test_main_calls_asyncio_run(self):
        """main() calls asyncio.run with controller.run()."""
        from vm_controller.controller import main

        mock_ctrl = MagicMock()
        mock_coro = MagicMock()
        mock_ctrl.run.return_value = mock_coro

        with (
            patch("vm_controller.controller.VMController", return_value=mock_ctrl),
            patch("vm_controller.controller.signal.signal"),
            patch("vm_controller.controller.asyncio.run") as mock_arun,
        ):
            main()

        mock_arun.assert_called_once_with(mock_coro)

    def test_signal_handler_calls_request_shutdown(self):
        """Signal handler invokes request_shutdown on the controller."""
        import signal as signal_module
        from vm_controller.controller import main

        handlers = {}
        with (
            patch("vm_controller.controller.VMController") as mock_cls,
            patch(
                "vm_controller.controller.signal.signal",
                side_effect=lambda s, h: handlers.update({s: h}),
            ),
            patch("vm_controller.controller.asyncio.run"),
        ):
            mock_ctrl = MagicMock()
            mock_cls.return_value = mock_ctrl
            main()

        # Invoke the captured SIGTERM handler
        handlers[signal_module.SIGTERM](signal_module.SIGTERM, None)
        mock_ctrl.request_shutdown.assert_called_once()


# =============================================================================
# Tests: VMController.__init__()
# =============================================================================


class TestVMControllerInit:
    """Tests for VMController initialization."""

    def test_init_defaults(self):
        """VMController starts with None clients and empty template."""
        ctrl = VMController()
        assert ctrl.nc is None
        assert ctrl.k8s_client is None
        assert ctrl.template_text == ""
        assert not ctrl._shutdown.is_set()

    def test_init_creates_headscale_client(self):
        """VMController creates a HeadscaleClient instance on init."""
        ctrl = VMController()
        # headscale is set (even if it is a MagicMock from our stub)
        assert ctrl.headscale is not None

    def test_init_shutdown_event_not_set(self):
        """The internal _shutdown event starts unset."""
        ctrl = VMController()
        assert not ctrl._shutdown.is_set()


# =============================================================================
# Tests: Edge cases and integration scenarios
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases and unusual scenarios."""

    @pytest.mark.asyncio
    async def test_create_then_delete_same_job(self, controller):
        """Creating and then deleting a VM for the same job works correctly."""
        job_id = "create-delete-test"
        await controller.handle_create(
            make_nats_msg(
                {
                    "job_id": job_id,
                    "agent_config": "developer",
                }
            )
        )
        await controller.handle_delete(make_nats_msg({"job_id": job_id}))

        controller.k8s_client.create_namespaced_custom_object.assert_called_once()
        # VM object once; the second delete call is the rootdisk purge.
        assert (
            len(
                _calls_for(
                    controller.k8s_client.delete_namespaced_custom_object,
                    KUBEVIRT_PLURAL,
                )
            )
            == 1
        )

        # Last status should be "deleted"
        last_payload = json.loads(controller.nc.publish.call_args[0][1].decode())
        assert last_payload["status"] == "deleted"

    @pytest.mark.asyncio
    async def test_concurrent_create_requests(self, controller):
        """Multiple concurrent create requests each produce their own VM."""
        jobs = [
            {"job_id": f"concurrent-{i}", "agent_config": "developer"} for i in range(3)
        ]
        msgs = [make_nats_msg(job) for job in jobs]

        await asyncio.gather(*[controller.handle_create(m) for m in msgs])

        assert controller.k8s_client.create_namespaced_custom_object.call_count == 3

    @pytest.mark.asyncio
    async def test_publish_failure_does_not_crash_create(self, controller):
        """NATS publish failure during create does not crash the handler."""
        controller.nc.publish.side_effect = Exception("NATS down")

        msg = make_nats_msg(SAMPLE_JOB_CONFIG)
        # Should not raise
        await controller.handle_create(msg)
        controller.k8s_client.create_namespaced_custom_object.assert_called_once()

    @pytest.mark.asyncio
    async def test_publish_failure_does_not_crash_delete(self, controller):
        """NATS publish failure during delete does not crash the handler."""
        controller.nc.publish.side_effect = Exception("NATS down")

        msg = make_nats_msg({"job_id": "test-pub-fail"})
        # Should not raise
        await controller.handle_delete(msg)

    @pytest.mark.asyncio
    async def test_empty_bytes_message_create(self, controller):
        """Empty bytes message on create is handled gracefully."""
        msg = make_nats_msg_raw(b"")
        await controller.handle_create(msg)

        payload = json.loads(controller.nc.publish.call_args[0][1].decode())
        assert payload["status"] == "failed"

    @pytest.mark.asyncio
    async def test_empty_bytes_message_delete(self, controller):
        """Empty bytes message on delete is handled gracefully."""
        msg = make_nats_msg_raw(b"")
        await controller.handle_delete(msg)

        payload = json.loads(controller.nc.publish.call_args[0][1].decode())
        assert payload["status"] == "delete_failed"

    @pytest.mark.asyncio
    async def test_double_shutdown_is_idempotent(self, controller):
        """Calling request_shutdown() twice does not cause issues."""
        controller.request_shutdown()
        controller.request_shutdown()
        assert controller._shutdown.is_set()

    @pytest.mark.asyncio
    async def test_handle_create_double_call_same_job(self, controller):
        """Two create requests for the same job ID each call K8s API."""
        msg1 = make_nats_msg({"job_id": "dup-job"})
        msg2 = make_nats_msg({"job_id": "dup-job"})

        await controller.handle_create(msg1)
        await controller.handle_create(msg2)

        assert controller.k8s_client.create_namespaced_custom_object.call_count == 2


# =============================================================================
# Tests: Module-level constants
# =============================================================================


class TestModuleConstants:
    """Tests for module-level configuration constants."""

    def test_kubevirt_api_coordinates(self):
        """KubeVirt API coordinates are correctly defined."""
        assert KUBEVIRT_GROUP == "kubevirt.io"
        assert KUBEVIRT_VERSION == "v1"
        assert KUBEVIRT_PLURAL == "virtualmachines"

    def test_default_config_values(self):
        """Default configuration values are sensible."""
        from vm_controller.controller import DEFAULT_CPU, DEFAULT_MEMORY, VM_NAMESPACE

        assert isinstance(DEFAULT_CPU, int)
        assert DEFAULT_CPU > 0
        assert "Gi" in DEFAULT_MEMORY or "Mi" in DEFAULT_MEMORY
        assert VM_NAMESPACE  # Not empty

    def test_nats_url_has_nats_scheme(self):
        """NATS_URL has the nats:// scheme."""
        from vm_controller.controller import NATS_URL

        assert "nats://" in NATS_URL


# =============================================================================
# Tests: golden-image cloning
# (knowledge-base/knowledge/features/vm_golden_image_boot_acceleration.md)
# =============================================================================

from vm_controller.controller import _golden_name  # noqa: E402


class TestGoldenName:
    """Deterministic, content-keyed golden PVC names."""

    def test_deterministic_and_prefixed(self):
        n = _golden_name("ghcr.io/x/agent-vm-base:sha-abc")
        assert n == _golden_name("ghcr.io/x/agent-vm-base:sha-abc")
        assert n.startswith("agent-vm-golden-")
        assert len(n) == len("agent-vm-golden-") + 12

    def test_differs_per_image_digest(self):
        assert _golden_name("img:sha-a") != _golden_name("img:sha-b")


class TestApplyCloneSource:
    """Rendered VM manifest → rootdisk clones the golden PVC instead of import."""

    def test_swaps_registry_for_pvc_clone(self, controller):
        manifest = controller.render_template(SAMPLE_JOB_CONFIG, "")
        controller._apply_clone_source(manifest, "agent-vm-golden-deadbeef")
        dv = manifest["spec"]["dataVolumeTemplates"][0]["spec"]
        assert dv["source"] == {"pvc": {"name": "agent-vm-golden-deadbeef"}}
        assert "registry" not in dv["source"]
        # same namespace → no namespace key (avoids cross-ns clone RBAC)
        assert "namespace" not in dv["source"]["pvc"]
        # clone target must match the golden's Filesystem volumeMode
        assert dv["storage"]["volumeMode"] == "Filesystem"


class TestEnsureGolden:
    """The golden ensure state machine, against the CDI DataVolume resource."""

    @staticmethod
    def _get(c):
        return c.k8s_client.get_namespaced_custom_object

    @staticmethod
    def _create(c):
        return c.k8s_client.create_namespaced_custom_object

    @pytest.mark.asyncio
    async def test_succeeded_fast_path_no_create(self, controller):
        self._get(controller).return_value = {"status": {"phase": "Succeeded"}}
        name = await controller._ensure_golden("img:sha-a")
        assert name == _golden_name("img:sha-a")
        self._create(controller).assert_not_called()

    @pytest.mark.asyncio
    async def test_absent_creates_then_waits_succeeded(self, controller):
        self._get(controller).side_effect = [
            _FakeApiException(status=404),
            {"status": {"phase": "Succeeded"}},
        ]
        with patch("asyncio.sleep", new_callable=AsyncMock):
            name = await controller._ensure_golden("img:sha-a")
        assert name == _golden_name("img:sha-a")
        create = self._create(controller)
        create.assert_called_once()
        kwargs = create.call_args.kwargs
        assert kwargs["group"] == "cdi.kubevirt.io"
        assert kwargs["version"] == "v1beta1"
        assert kwargs["plural"] == "datavolumes"
        # golden manifest: explicit spec.pvc + Filesystem + bind-immediate + keep-handle
        body = kwargs["body"]
        # spec.storage, never spec.pvc: only the storage form is inflated by
        # CDI's filesystemOverhead, and a literal 20Gi filesystem cannot hold a
        # 20 GiB image on a real CSI (DataVolume too small to contain image).
        assert "pvc" not in body["spec"]
        assert body["spec"]["storage"]["volumeMode"] == "Filesystem"
        assert body["spec"]["storage"]["accessModes"] == ["ReadWriteOnce"]
        assert body["spec"]["storage"]["resources"]["requests"]["storage"]
        ann = body["metadata"]["annotations"]
        assert ann["cdi.kubevirt.io/storage.bind.immediate.requested"] == "true"
        assert ann["cdi.kubevirt.io/storage.deleteAfterCompletion"] == "false"

    @pytest.mark.asyncio
    async def test_failed_golden_is_recreated(self, controller):
        failed = {
            "metadata": {"uid": "golden-uid", "resourceVersion": "7"},
            "status": {"phase": "Failed"},
        }
        self._get(controller).side_effect = [
            failed,
            failed,
            {"status": {"phase": "Succeeded"}},
        ]
        controller.k8s_client.list_namespaced_custom_object.return_value = {"items": []}
        with patch("asyncio.sleep", new_callable=AsyncMock):
            name = await controller._ensure_golden("img:sha-a")
        assert name == _golden_name("img:sha-a")
        controller.k8s_client.delete_namespaced_custom_object.assert_called_once()
        self._create(controller).assert_called_once()

    @pytest.mark.asyncio
    async def test_importing_waits_without_creating(self, controller):
        self._get(controller).side_effect = [
            {"status": {"phase": "ImportInProgress"}},
            {"status": {"phase": "Succeeded"}},
        ]
        with patch("asyncio.sleep", new_callable=AsyncMock):
            name = await controller._ensure_golden("img:sha-a")
        assert name == _golden_name("img:sha-a")
        self._create(controller).assert_not_called()

    @pytest.mark.asyncio
    async def test_create_409_is_the_lock_then_waits(self, controller):
        self._get(controller).side_effect = [
            _FakeApiException(status=404),
            {"status": {"phase": "Succeeded"}},
        ]
        self._create(controller).side_effect = _FakeApiException(status=409)
        with patch("asyncio.sleep", new_callable=AsyncMock):
            name = await controller._ensure_golden("img:sha-a")
        assert name == _golden_name("img:sha-a")

    @pytest.mark.asyncio
    async def test_create_failure_returns_none_for_fallback(self, controller):
        self._get(controller).side_effect = [_FakeApiException(status=404)]
        self._create(controller).side_effect = _FakeApiException(status=500)
        name = await controller._ensure_golden("img:sha-a")
        assert name is None


class TestGoldenStateNowait:
    """Non-blocking golden check for the create path.

    Unlike _ensure_golden (kept for pre-warm), this must NEVER sleep waiting
    for CDI: a create handler blocked for a cold import (~30 min) outlives
    every orchestrator budget and races later creates into 409 collisions —
    see knowledge-history/done/golden_image_cold_import_fails_inflight_vm_jobs.md.
    """

    @staticmethod
    def _get(c):
        return c.k8s_client.get_namespaced_custom_object

    @staticmethod
    def _create(c):
        return c.k8s_client.create_namespaced_custom_object

    @pytest.mark.asyncio
    async def test_succeeded_returns_name_no_waiting(self, controller):
        self._get(controller).return_value = {"status": {"phase": "Succeeded"}}
        name, waiting = await controller._golden_state_nowait("img:sha-a")
        assert name == _golden_name("img:sha-a")
        assert waiting is None
        self._create(controller).assert_not_called()

    @pytest.mark.asyncio
    async def test_importing_returns_waiting_without_sleeping(self, controller):
        self._get(controller).return_value = {
            "status": {"phase": "ImportInProgress", "progress": "68.19%"}
        }
        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            name, waiting = await controller._golden_state_nowait("img:sha-a")
        mock_sleep.assert_not_awaited()
        assert name is None
        assert waiting == {
            "golden": _golden_name("img:sha-a"),
            "golden_phase": "ImportInProgress",
            "golden_progress": "68.19%",
        }
        self._create(controller).assert_not_called()

    @pytest.mark.asyncio
    async def test_absent_creates_dv_and_returns_waiting(self, controller):
        self._get(controller).side_effect = _FakeApiException(status=404)
        name, waiting = await controller._golden_state_nowait("img:sha-a")
        assert name is None
        assert waiting["golden"] == _golden_name("img:sha-a")
        assert waiting["golden_phase"] == "Pending"
        create = self._create(controller)
        create.assert_called_once()
        assert create.call_args.kwargs["group"] == "cdi.kubevirt.io"

    @pytest.mark.asyncio
    async def test_absent_create_409_racer_still_waits(self, controller):
        self._get(controller).side_effect = _FakeApiException(status=404)
        self._create(controller).side_effect = _FakeApiException(status=409)
        name, waiting = await controller._golden_state_nowait("img:sha-a")
        assert name is None
        assert waiting is not None

    @pytest.mark.asyncio
    async def test_absent_create_error_falls_back_to_registry(self, controller):
        self._get(controller).side_effect = _FakeApiException(status=404)
        self._create(controller).side_effect = _FakeApiException(status=500)
        name, waiting = await controller._golden_state_nowait("img:sha-a")
        assert name is None
        assert waiting is None

    @pytest.mark.asyncio
    async def test_failed_golden_recreated_then_waits(self, controller):
        self._get(controller).return_value = {
            "metadata": {"uid": "golden-uid", "resourceVersion": "7"},
            "status": {"phase": "Failed"},
        }
        controller.k8s_client.list_namespaced_custom_object.return_value = {"items": []}
        name, waiting = await controller._golden_state_nowait("img:sha-a")
        assert name is None
        assert waiting is not None
        controller.k8s_client.delete_namespaced_custom_object.assert_called_once()
        self._create(controller).assert_called_once()


class TestDoCreateWaitingGolden:
    """_do_create defers (no VM, no Headscale key) while the golden imports."""

    @pytest.mark.asyncio
    async def test_importing_golden_defers_vm_create(self, controller):
        controller.k8s_client.get_namespaced_custom_object.return_value = {
            "status": {"phase": "ImportInProgress", "progress": "42.0%"}
        }
        msg = make_nats_msg(SAMPLE_JOB_CONFIG)
        with patch("vm_controller.controller.VM_GOLDEN_IMAGE_ENABLED", True):
            await controller.handle_create(msg)

        # No VM object, no Headscale key minted per poll
        controller.k8s_client.create_namespaced_custom_object.assert_not_called()
        controller.headscale.create_auth_key.assert_not_awaited()

        payload = json.loads(controller.nc.publish.call_args[0][1].decode())
        assert payload["status"] == "waiting_golden"
        assert payload["job_id"] == SAMPLE_JOB_CONFIG["job_id"]
        assert payload["golden_progress"] == "42.0%"
        assert payload["golden"].startswith("agent-vm-golden-")

    @pytest.mark.asyncio
    async def test_succeeded_golden_creates_vm_with_clone_source(self, controller):
        controller.k8s_client.get_namespaced_custom_object.return_value = {
            "status": {"phase": "Succeeded"}
        }
        msg = make_nats_msg(SAMPLE_JOB_CONFIG)
        with (
            patch("vm_controller.controller.VM_GOLDEN_IMAGE_ENABLED", True),
            patch("vm_controller.controller.VM_GOLDEN_GC_ENABLED", False),
        ):
            await controller.handle_create(msg)

        create = controller.k8s_client.create_namespaced_custom_object
        create.assert_called_once()
        body = create.call_args.kwargs["body"]
        dv = body["spec"]["dataVolumeTemplates"][0]["spec"]
        assert "pvc" in dv["source"]  # clone, not registry import

        payload = json.loads(controller.nc.publish.call_args[0][1].decode())
        assert payload["status"] == "created"

    @pytest.mark.asyncio
    async def test_golden_infra_error_falls_back_to_registry_create(self, controller):
        controller.k8s_client.get_namespaced_custom_object.side_effect = (
            _FakeApiException(status=404)
        )
        # golden DV create rejected (CDI infra down) → registry fallback;
        # VM create (2nd create call) succeeds.
        controller.k8s_client.create_namespaced_custom_object.side_effect = [
            _FakeApiException(status=500),
            {
                "metadata": {
                    "name": f"agent-vm-{SAMPLE_JOB_CONFIG['job_id']}",
                    "uid": "registry-fallback-vm-uid",
                }
            },
        ]
        msg = make_nats_msg(SAMPLE_JOB_CONFIG)
        with patch("vm_controller.controller.VM_GOLDEN_IMAGE_ENABLED", True):
            await controller.handle_create(msg)

        payload = json.loads(controller.nc.publish.call_args[0][1].decode())
        assert payload["status"] == "created"


class TestGcGoldens:
    """Keep the newest N; never GC the current, in-use, or too-young goldens."""

    @pytest.mark.asyncio
    async def test_keeps_newest_skips_current_and_in_use(self, controller):
        imgs = {k: f"img:sha-{k}" for k in ("new", "b", "c", "old")}
        ts = {
            "new": "2026-01-04T00:00:00Z",
            "b": "2026-01-03T00:00:00Z",
            "c": "2026-01-02T00:00:00Z",
            "old": "2026-01-01T00:00:00Z",
        }
        goldens = {
            "items": [
                {
                    "metadata": {
                        "name": _golden_name(imgs[k]),
                        "creationTimestamp": ts[k],
                        "labels": {"srw.io/golden-image": "x"},
                    }
                }
                for k in ("new", "b", "c", "old")
            ]
        }
        vms = {
            "items": [
                {
                    "spec": {
                        "dataVolumeTemplates": [
                            {
                                "spec": {
                                    "source": {"pvc": {"name": _golden_name(imgs["b"])}}
                                }
                            }
                        ]
                    }
                }
            ]
        }
        controller.k8s_client.list_namespaced_custom_object.side_effect = [
            goldens,
            vms,
            {"items": []},
        ]
        controller.k8s_client.get_namespaced_custom_object.return_value = {
            "metadata": {"uid": "golden-uid", "resourceVersion": "7"}
        }
        with (
            patch("vm_controller.controller.VM_GOLDEN_KEEP", 1),
            patch("vm_controller.controller.VM_GOLDEN_GC_MIN_AGE_MINUTES", 0),
        ):
            await controller._gc_goldens(imgs["c"])  # current image = c
        deletes = controller.k8s_client.delete_namespaced_custom_object
        # keep newest 1 (new); b is in-use, c is current → only old is GC'd.
        deleted = [call.kwargs["name"] for call in deletes.call_args_list]
        assert deleted == [_golden_name(imgs["old"])]

    @pytest.mark.asyncio
    async def test_noop_when_at_or_below_keep(self, controller):
        goldens = {
            "items": [
                {
                    "metadata": {
                        "name": "g1",
                        "creationTimestamp": "2026-01-01T00:00:00Z",
                    }
                }
            ]
        }
        controller.k8s_client.list_namespaced_custom_object.side_effect = [goldens]
        with patch("vm_controller.controller.VM_GOLDEN_KEEP", 3):
            await controller._gc_goldens("img:sha-a")
        controller.k8s_client.delete_namespaced_custom_object.assert_not_called()


class TestDoCreateGoldenIntegration:
    """_do_create wires the clone in when enabled; byte-identical when off."""

    @pytest.mark.asyncio
    async def test_enabled_applies_clone_source(self, controller):
        controller._golden_state_nowait = AsyncMock(
            return_value=("agent-vm-golden-abc123def456", None)
        )
        with (
            patch("vm_controller.controller.VM_GOLDEN_IMAGE_ENABLED", True),
            patch("vm_controller.controller.VM_GOLDEN_GC_ENABLED", False),
        ):
            await controller._do_create(SAMPLE_JOB_CONFIG)
        body = controller.k8s_client.create_namespaced_custom_object.call_args.kwargs[
            "body"
        ]
        src = body["spec"]["dataVolumeTemplates"][0]["spec"]["source"]
        assert src == {"pvc": {"name": "agent-vm-golden-abc123def456"}}

    @pytest.mark.asyncio
    async def test_disabled_keeps_registry_source(self, controller):
        with patch("vm_controller.controller.VM_GOLDEN_IMAGE_ENABLED", False):
            await controller._do_create(SAMPLE_JOB_CONFIG)
        body = controller.k8s_client.create_namespaced_custom_object.call_args.kwargs[
            "body"
        ]
        src = body["spec"]["dataVolumeTemplates"][0]["spec"]["source"]
        assert "registry" in src
        assert "pvc" not in src

    @pytest.mark.asyncio
    async def test_golden_failure_falls_back_to_registry(self, controller):
        controller._golden_state_nowait = AsyncMock(return_value=(None, None))
        with patch("vm_controller.controller.VM_GOLDEN_IMAGE_ENABLED", True):
            await controller._do_create(SAMPLE_JOB_CONFIG)
        body = controller.k8s_client.create_namespaced_custom_object.call_args.kwargs[
            "body"
        ]
        src = body["spec"]["dataVolumeTemplates"][0]["spec"]["source"]
        assert "registry" in src
        assert "pvc" not in src


# =============================================================================
# Tests: persistent rootdisk — Phase 0
# (knowledge-base/knowledge/features/vm_persistent_rootdisk.md D1 + D2's controller half)
# =============================================================================

from vm_controller.controller import _rootdisk_name  # noqa: E402


def _calls_for(mock, plural: str) -> list:
    """Filter a k8s CustomObjectsApi mock's calls down to one resource kind.

    ``k8s_client`` is one MagicMock serving both VirtualMachines and
    DataVolumes, so every assertion has to say which it means.
    """
    return [c for c in mock.call_args_list if c.kwargs.get("plural") == plural]


def _dv_create_body(controller) -> dict:
    calls = _calls_for(
        controller.k8s_client.create_namespaced_custom_object, CDI_PLURAL
    )
    assert calls, "no DataVolume was created"
    return calls[-1].kwargs["body"]


def _vm_create_body(controller) -> dict:
    calls = _calls_for(
        controller.k8s_client.create_namespaced_custom_object, KUBEVIRT_PLURAL
    )
    assert calls, "no VirtualMachine was created"
    return calls[-1].kwargs["body"]


def _dv_phase(phase: str | None):
    """side_effect for get_namespaced_custom_object: one DV in ``phase``.

    ``None`` means 404 (absent).
    """

    def _get(**kwargs):
        if kwargs.get("plural") == KUBEVIRT_PLURAL:
            return {
                "metadata": {
                    "name": kwargs.get("name"),
                    "uid": "persistent-rootdisk-vm-uid",
                }
            }
        if phase is None:
            raise _FakeApiException(status=404)
        name = kwargs.get("name")
        owner_id = name[len("agent-vm-") : -len("-rootdisk")]
        return {
            "metadata": {
                "name": name,
                "uid": "rootdisk-dv-uid",
                "labels": {
                    "srw.io/owner-kind": (
                        "thread" if owner_id.startswith("thread-") else "job"
                    ),
                    "srw.io/owner-id": owner_id,
                },
            },
            "status": {"phase": phase},
        }

    return _get


class TestRootdiskName:
    """The standalone DV keeps the exact name the VM template already uses,
    so ``volumes[].dataVolume.name`` needs no change."""

    def test_matches_template_name(self):
        assert _rootdisk_name("abc-123") == "agent-vm-abc-123-rootdisk"

    def test_is_entity_agnostic(self):
        # The controller never learns whether an id is a job or a thread; VM
        # names are agent-vm-<id> for both, so rootdisks are too.
        assert _rootdisk_name("thread-uuid") == "agent-vm-thread-uuid-rootdisk"


class TestPersistentRootdiskDisabled:
    """Flag OFF → today's rendering, byte-identical. No DV, no extra calls."""

    @pytest.mark.asyncio
    async def test_manifest_keeps_data_volume_templates(self, controller):
        with patch("vm_controller.controller.VM_PERSISTENT_ROOTDISK", False):
            await controller._do_create(SAMPLE_JOB_CONFIG)
        body = _vm_create_body(controller)
        assert "dataVolumeTemplates" in body["spec"]
        assert not _calls_for(
            controller.k8s_client.create_namespaced_custom_object, CDI_PLURAL
        )


class TestPersistentRootdiskEnabled:
    """Flag ON → the rootdisk becomes a standalone DataVolume the VM does not
    own, so it survives VM deletion and is reattached by name on recreate."""

    @pytest.mark.asyncio
    async def test_data_volume_templates_popped_volumes_untouched(self, controller):
        controller.k8s_client.get_namespaced_custom_object.side_effect = _dv_phase(None)
        with patch("vm_controller.controller.VM_PERSISTENT_ROOTDISK", True):
            await controller._do_create(SAMPLE_JOB_CONFIG)

        body = _vm_create_body(controller)
        assert "dataVolumeTemplates" not in body["spec"]
        # The by-name reference is the whole trick — it must be untouched.
        volumes = body["spec"]["template"]["spec"]["volumes"]
        rootvol = next(v for v in volumes if v["name"] == "rootdisk")
        assert rootvol["dataVolume"]["name"] == _rootdisk_name(
            SAMPLE_JOB_CONFIG["job_id"]
        )

    @pytest.mark.asyncio
    async def test_standalone_dv_created_with_the_template_spec(self, controller):
        controller.k8s_client.get_namespaced_custom_object.side_effect = _dv_phase(None)
        with patch("vm_controller.controller.VM_PERSISTENT_ROOTDISK", True):
            await controller._do_create(SAMPLE_JOB_CONFIG)

        dv = _dv_create_body(controller)
        assert dv["kind"] == "DataVolume"
        assert dv["metadata"]["name"] == _rootdisk_name(SAMPLE_JOB_CONFIG["job_id"])
        assert dv["metadata"]["namespace"] == VM_NAMESPACE
        # Labels drive the GC sweep and the orphan backstop.
        assert dv["metadata"]["labels"]["srw.io/rootdisk"] == "true"
        assert dv["metadata"]["labels"]["job-id"] == SAMPLE_JOB_CONFIG["job_id"]
        assert dv["metadata"]["labels"]["srw.io/owner-kind"] == "job"
        assert (
            dv["metadata"]["labels"]["srw.io/owner-id"] == SAMPLE_JOB_CONFIG["job_id"]
        )
        # Spec is the template's own — same size, storage class, source.
        assert dv["spec"]["storage"]["storageClassName"]
        assert "registry" in dv["spec"]["source"]
        # No bind.immediate: the clone target must stay WaitForFirstConsumer so
        # it binds on the VM's node (same rule as the golden work).
        annotations = dv["metadata"].get("annotations", {})
        assert "cdi.kubevirt.io/storage.bind.immediate.requested" not in annotations

    @pytest.mark.asyncio
    async def test_thread_rootdisk_carries_thread_owner_identity(self, controller):
        controller.k8s_client.get_namespaced_custom_object.side_effect = _dv_phase(None)
        config = {**SAMPLE_JOB_CONFIG, "job_id": "thread-123", "entity_type": "thread"}
        with patch("vm_controller.controller.VM_PERSISTENT_ROOTDISK", True):
            await controller._do_create(config)

        labels = _dv_create_body(controller)["metadata"]["labels"]
        assert labels["srw.io/owner-kind"] == "thread"
        assert labels["srw.io/owner-id"] == "thread-123"

    @pytest.mark.asyncio
    async def test_golden_clone_source_carries_into_the_standalone_dv(self, controller):
        """Ordering guard: the clone mutation must be applied BEFORE the pop,
        or a golden-enabled create would silently import from the registry."""
        controller._golden_state_nowait = AsyncMock(
            return_value=("agent-vm-golden-abc123def456", None)
        )
        controller.k8s_client.get_namespaced_custom_object.side_effect = _dv_phase(None)
        with (
            patch("vm_controller.controller.VM_GOLDEN_IMAGE_ENABLED", True),
            patch("vm_controller.controller.VM_GOLDEN_GC_ENABLED", False),
            patch("vm_controller.controller.VM_PERSISTENT_ROOTDISK", True),
        ):
            await controller._do_create(SAMPLE_JOB_CONFIG)

        dv = _dv_create_body(controller)
        # namespace is added on the way out — see TestRootdiskCloneSourceNamespace.
        assert dv["spec"]["source"]["pvc"]["name"] == "agent-vm-golden-abc123def456"
        assert dv["spec"]["storage"]["volumeMode"] == "Filesystem"

    @pytest.mark.asyncio
    async def test_succeeded_rootdisk_is_reattached_without_a_clone(self, controller):
        """The recovery path: disk already exists → skip creation entirely."""
        controller.k8s_client.get_namespaced_custom_object.side_effect = _dv_phase(
            "Succeeded"
        )
        with patch("vm_controller.controller.VM_PERSISTENT_ROOTDISK", True):
            await controller._do_create(SAMPLE_JOB_CONFIG)

        assert not _calls_for(
            controller.k8s_client.create_namespaced_custom_object, CDI_PLURAL
        )
        # ...and the VM still gets built, pointing at the existing disk.
        body = _vm_create_body(controller)
        assert "dataVolumeTemplates" not in body["spec"]

    @pytest.mark.asyncio
    async def test_succeeded_rootdisk_adoption_refuses_active_recovery_pin(
        self, controller
    ):
        controller.k8s_client.get_namespaced_custom_object.side_effect = _dv_phase(
            "Succeeded"
        )
        controller._active_recovery_pins = AsyncMock(
            return_value=({"pvc_uid": (f"root-pvc-uid-{SAMPLE_JOB_CONFIG['job_id']}")},)
        )

        with (
            patch("vm_controller.controller.VM_PERSISTENT_ROOTDISK", True),
            pytest.raises(RuntimeError, match="pinned"),
        ):
            await controller._do_create(SAMPLE_JOB_CONFIG)

        assert not _calls_for(
            controller.k8s_client.create_namespaced_custom_object, KUBEVIRT_PLURAL
        )

    @pytest.mark.asyncio
    async def test_succeeded_rootdisk_rechecks_pin_immediately_before_vm_create(
        self, controller
    ):
        pvc_uid = f"root-pvc-uid-{SAMPLE_JOB_CONFIG['job_id']}"
        controller.k8s_client.get_namespaced_custom_object.side_effect = _dv_phase(
            "Succeeded"
        )
        controller._active_recovery_pins = AsyncMock(
            side_effect=[(), ({"pvc_uid": pvc_uid},)]
        )

        with (
            patch("vm_controller.controller.VM_PERSISTENT_ROOTDISK", True),
            pytest.raises(RuntimeError, match="pinned"),
        ):
            await controller._do_create(SAMPLE_JOB_CONFIG)

        controller._acquire_workspace_cleanup_reservation.assert_awaited_once()
        controller._complete_workspace_cleanup_reservation.assert_not_awaited()
        assert not _calls_for(
            controller.k8s_client.create_namespaced_custom_object, KUBEVIRT_PLURAL
        )

    @pytest.mark.asyncio
    async def test_completed_adoption_replay_only_accepts_existing_vm(self, controller):
        controller.k8s_client.get_namespaced_custom_object.side_effect = _dv_phase(
            "Succeeded"
        )
        controller._acquire_workspace_cleanup_reservation.side_effect = None
        controller._acquire_workspace_cleanup_reservation.return_value = {
            "allowed": True,
            "admission_id": "00000000-0000-4000-8000-000000000932",
            "completed_outcome": "adopted",
        }

        with patch("vm_controller.controller.VM_PERSISTENT_ROOTDISK", True):
            result = await controller._do_create(SAMPLE_JOB_CONFIG)

        assert result["status"] == "created"
        assert not _calls_for(
            controller.k8s_client.create_namespaced_custom_object, KUBEVIRT_PLURAL
        )
        controller._complete_workspace_cleanup_reservation.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_failed_rootdisk_is_deleted_and_recreated(self, controller):
        owner_id = SAMPLE_JOB_CONFIG["job_id"]
        name = _rootdisk_name(owner_id)
        recreated = False

        def get_object(**kwargs):
            if kwargs.get("plural") == KUBEVIRT_PLURAL:
                return {
                    "metadata": {
                        "name": f"agent-vm-{owner_id}",
                        "uid": "persistent-rootdisk-vm-uid",
                    }
                }
            return {
                "metadata": {
                    "name": name,
                    "uid": "replacement-dv-uid" if recreated else "rootdisk-dv-uid",
                    "labels": {
                        "srw.io/owner-kind": "job",
                        "srw.io/owner-id": owner_id,
                    },
                },
                "status": {"phase": "Pending" if recreated else "Failed"},
            }

        def create_object(**kwargs):
            nonlocal recreated
            if kwargs.get("plural") == CDI_PLURAL:
                recreated = True
            body = kwargs.get("body") or {}
            metadata = dict(body.get("metadata") or {})
            metadata["uid"] = (
                "replacement-dv-uid"
                if kwargs.get("plural") == CDI_PLURAL
                else "admitted-vm-uid-001"
            )
            return {**body, "metadata": metadata}

        def read_pvc(**_kwargs):
            dv_uid = "replacement-dv-uid" if recreated else "rootdisk-dv-uid"
            pvc_uid = (
                "replacement-root-pvc-uid" if recreated else f"root-pvc-uid-{owner_id}"
            )
            return types.SimpleNamespace(
                metadata=types.SimpleNamespace(
                    name=name,
                    uid=pvc_uid,
                    labels={
                        "srw.io/owner-kind": "job",
                        "srw.io/owner-id": owner_id,
                    },
                    owner_references=[
                        types.SimpleNamespace(
                            kind="DataVolume", uid=dv_uid, controller=True
                        )
                    ],
                )
            )

        controller.k8s_client.get_namespaced_custom_object.side_effect = get_object
        controller.k8s_client.create_namespaced_custom_object.side_effect = (
            create_object
        )
        controller.core_api.read_namespaced_persistent_volume_claim.side_effect = (
            read_pvc
        )
        controller.core_api.list_namespaced_persistent_volume_claim.side_effect = (
            lambda **_kwargs: types.SimpleNamespace(items=[read_pvc()])
        )
        with patch("vm_controller.controller.VM_PERSISTENT_ROOTDISK", True):
            await controller._do_create(SAMPLE_JOB_CONFIG)

        deletes = _calls_for(
            controller.k8s_client.delete_namespaced_custom_object, CDI_PLURAL
        )
        assert [c.kwargs["name"] for c in deletes] == [
            _rootdisk_name(SAMPLE_JOB_CONFIG["job_id"])
        ]
        assert _calls_for(
            controller.k8s_client.create_namespaced_custom_object, CDI_PLURAL
        )
        controller._acquire_workspace_cleanup_reservation.assert_awaited_once()
        controller._complete_workspace_cleanup_reservation.assert_awaited_once()
        assert (
            controller._complete_workspace_cleanup_reservation.await_args.kwargs[
                "outcome"
            ]
            == "recreated"
        )

    @pytest.mark.asyncio
    async def test_in_progress_rootdisk_is_adopted(self, controller):
        """A racing create is already building it; KubeVirt gates VMI start on
        DV readiness, so adopting is safe and a second create would 409."""
        controller.k8s_client.get_namespaced_custom_object.side_effect = _dv_phase(
            "CloneScheduled"
        )
        with patch("vm_controller.controller.VM_PERSISTENT_ROOTDISK", True):
            await controller._do_create(SAMPLE_JOB_CONFIG)

        assert not _calls_for(
            controller.k8s_client.create_namespaced_custom_object, CDI_PLURAL
        )
        assert not _calls_for(
            controller.k8s_client.delete_namespaced_custom_object, CDI_PLURAL
        )
        assert _vm_create_body(controller)

    @pytest.mark.asyncio
    async def test_recreated_rootdisk_resumes_carried_db_reservation_after_restart(
        self, controller
    ):
        admission_id = "00000000-0000-4000-8000-000000000931"
        pvc_uid = f"root-pvc-uid-{SAMPLE_JOB_CONFIG['job_id']}"
        carrier_lease = _cleanup_carrier_lease(
            admission_id=admission_id,
            source="controller_failed_dv_recreate",
            outcome="recreated",
            generation="legacy",
            old_dv_uid="old-failed-dv-uid",
            old_pvc_uid="old-failed-pvc-uid",
            successor_dv_uid="rootdisk-dv-uid",
            successor_pvc_uid=pvc_uid,
        )

        def list_leases(**kwargs):
            if kwargs.get("label_selector") == (
                "srw.io/vm-workspace-cleanup-carrier=true"
            ):
                return {"items": [carrier_lease]}
            return {"items": []}

        controller.coordination_api.list_namespaced_lease.side_effect = list_leases

        def get_object(**kwargs):
            if kwargs.get("plural") == KUBEVIRT_PLURAL:
                return {
                    "metadata": {
                        "name": f"agent-vm-{SAMPLE_JOB_CONFIG['job_id']}",
                        "uid": "admitted-vm-uid-001",
                    }
                }
            value = _dv_phase("Pending")(**kwargs)
            value["metadata"]["annotations"] = {
                "srw.io/cleanup-admission-id": admission_id,
                "srw.io/cleanup-carrier-uid": "cleanup-carrier-uid",
                "srw.io/cleanup-nonce": ("00000000-0000-4000-8000-000000000904"),
                "srw.io/provision-generation": "legacy",
            }
            return value

        controller.k8s_client.get_namespaced_custom_object.side_effect = get_object

        with patch("vm_controller.controller.VM_PERSISTENT_ROOTDISK", True):
            await controller._do_create(SAMPLE_JOB_CONFIG)

        assert controller._resume_workspace_cleanup_reservation.await_count == 2
        controller._acquire_workspace_cleanup_reservation.assert_not_awaited()
        controller._complete_workspace_cleanup_reservation.assert_awaited_once()
        assert (
            controller._complete_workspace_cleanup_reservation.await_args.kwargs[
                "outcome"
            ]
            == "recreated"
        )

    @pytest.mark.asyncio
    async def test_dv_create_409_is_adopted(self, controller):
        controller.k8s_client.get_namespaced_custom_object.side_effect = _dv_phase(None)

        def _create(**kwargs):
            if kwargs.get("plural") == CDI_PLURAL:
                raise _FakeApiException(status=409, body="already exists")
            return MagicMock()

        controller.k8s_client.create_namespaced_custom_object.side_effect = _create
        with patch("vm_controller.controller.VM_PERSISTENT_ROOTDISK", True):
            result = await controller._do_create(SAMPLE_JOB_CONFIG)

        assert result["status"] == "created"

    @pytest.mark.asyncio
    async def test_dv_create_failure_fails_the_create_loudly(self, controller):
        """No silent fallback to the templated disk — that would quietly
        reintroduce the cascade-delete this feature exists to remove."""
        controller.k8s_client.get_namespaced_custom_object.side_effect = _dv_phase(None)

        def _create(**kwargs):
            if kwargs.get("plural") == CDI_PLURAL:
                raise _FakeApiException(status=500, body="quota exceeded")
            return MagicMock()

        controller.k8s_client.create_namespaced_custom_object.side_effect = _create
        with patch("vm_controller.controller.VM_PERSISTENT_ROOTDISK", True):
            with pytest.raises(_FakeApiException):
                await controller._do_create(SAMPLE_JOB_CONFIG)

        assert not _calls_for(
            controller.k8s_client.create_namespaced_custom_object, KUBEVIRT_PLURAL
        )

    @pytest.mark.asyncio
    async def test_template_without_data_volume_templates_refuses(self, controller):
        controller.template_text = SAMPLE_TEMPLATE.replace(
            "  dataVolumeTemplates:", "  x-dataVolumeTemplates:"
        )
        with patch("vm_controller.controller.VM_PERSISTENT_ROOTDISK", True):
            with pytest.raises(RuntimeError, match="no dataVolumeTemplates"):
                await controller._do_create(SAMPLE_JOB_CONFIG)


class TestDeletePurgeIntent:
    """``purge_disk`` decides whether a delete is terminal (disk + Headscale
    node go) or a recreate is expected (both are kept — D2/D3)."""

    @pytest.mark.asyncio
    async def test_default_without_captured_uid_keeps_disk_and_headscale(
        self, controller
    ):
        """A legacy purge intent cannot substitute a reusable name for identity."""
        result = await controller._do_delete("job-1")

        deletes = _calls_for(
            controller.k8s_client.delete_namespaced_custom_object, CDI_PLURAL
        )
        assert deletes == []
        controller.headscale.delete_node.assert_not_awaited()
        assert result["rootdisk"] == "kept"

    @pytest.mark.asyncio
    async def test_keep_leaves_disk_and_headscale_node(self, controller):
        """The recovery case. The node must stay: the reused disk still holds
        /var/lib/tailscale state for it, so deleting it would leave the
        recovered VM reconnecting as a dead node."""
        result = await controller._do_delete("job-1", purge_disk=False)

        assert not _calls_for(
            controller.k8s_client.delete_namespaced_custom_object, CDI_PLURAL
        )
        controller.headscale.delete_node.assert_not_awaited()
        assert result["rootdisk"] == "kept"
        # The VM object itself still goes.
        assert _calls_for(
            controller.k8s_client.delete_namespaced_custom_object, KUBEVIRT_PLURAL
        )

    @pytest.mark.asyncio
    async def test_purge_failure_is_non_fatal(self, controller):
        def _delete(**kwargs):
            if kwargs.get("plural") == CDI_PLURAL:
                raise _FakeApiException(status=500, body="boom")
            return MagicMock()

        controller.k8s_client.delete_namespaced_custom_object.side_effect = _delete
        result = await controller._do_delete("job-1")
        assert result["status"] == "deleted"

    @pytest.mark.asyncio
    async def test_handle_delete_passes_purge_intent_through(self, controller):
        msg = make_nats_msg({"job_id": "job-1", "purge_disk": False})
        await controller.handle_delete(msg)

        assert not _calls_for(
            controller.k8s_client.delete_namespaced_custom_object, CDI_PLURAL
        )
        payload = json.loads(controller.nc.publish.call_args[0][1].decode())
        assert payload["rootdisk"] == "kept"

    @pytest.mark.asyncio
    async def test_handle_delete_default_purge_without_uid_is_kept(self, controller):
        await controller.handle_delete(make_nats_msg({"job_id": "job-1"}))
        payload = json.loads(controller.nc.publish.call_args[0][1].decode())
        assert payload["rootdisk"] == "kept"

    @pytest.mark.asyncio
    async def test_http_delete_reads_purge_disk_query_param(self, controller):
        request = MagicMock()
        request.match_info = {"job_id": "job-1"}
        request.query = {"purge_disk": "false"}

        with patch.object(controller, "_do_delete", AsyncMock()) as do_delete:
            do_delete.return_value = {"job_id": "job-1", "status": "deleted"}
            await controller.http_delete(request)

        do_delete.assert_awaited_once_with(
            "job-1", purge_disk=False, provision_generation=None
        )

    @pytest.mark.asyncio
    async def test_http_delete_defaults_to_purge(self, controller):
        request = MagicMock()
        request.match_info = {"job_id": "job-1"}
        request.query = {}

        with patch.object(controller, "_do_delete", AsyncMock()) as do_delete:
            do_delete.return_value = {"job_id": "job-1", "status": "deleted"}
            await controller.http_delete(request)

        do_delete.assert_awaited_once_with(
            "job-1", purge_disk=True, provision_generation=None
        )


class TestWorkspaceCleanupCarriers:
    @pytest.mark.asyncio
    async def test_parent_admission_has_distinct_durable_child_request(
        self, controller
    ):
        controller._acquire_workspace_cleanup_reservation = (
            VMController._acquire_workspace_cleanup_reservation.__get__(controller)
        )
        controller._ensure_workspace_cleanup_carrier = AsyncMock(return_value={})
        requests = []

        async def acquire(path, payload, *, operation):
            requests.append(payload)
            identity = {
                key: payload[key]
                for key in (
                    "source",
                    "owner_kind",
                    "owner_id",
                    "pvc_uid",
                    "dv_uid",
                    "provision_generation",
                )
            }
            digest = hashlib.sha256(
                json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest()
            return {
                "allowed": True,
                "admission_id": payload["request_id"],
                "request_id": payload["request_id"],
                "intent_digest": "sha256:" + digest,
                "completed_outcome": None,
            }

        controller._workspace_cleanup_authority_request = acquire
        for parent in ("parent-one", "parent-two"):
            await controller._acquire_workspace_cleanup_reservation(
                source="controller_rootdisk_delete",
                owner_kind="job",
                owner_id=SAMPLE_JOB_CONFIG["job_id"],
                pvc_uid="old-pvc",
                dv_uid="old-dv",
                provision_generation=PROVISION_GENERATION,
                parent_cleanup={"admission_id": parent},
                parent_provision_generation=PROVISION_GENERATION,
                expected_vm_uid="captured-vm",
            )
        assert requests[0]["request_id"] != requests[1]["request_id"]
        assert requests[0]["parent_cleanup"] == {"admission_id": "parent-one"}
        assert requests[0]["parent_provision_generation"] == PROVISION_GENERATION
        assert requests[0]["expected_vm_uid"] == "captured-vm"
        assert controller._ensure_workspace_cleanup_carrier.await_count == 2

    @pytest.mark.asyncio
    async def test_restart_authenticates_creation_carrier_before_uid_seal(
        self, controller
    ):
        reservation = {
            "admission_id": "00000000-0000-4000-8000-000000000941",
            "request_id": "00000000-0000-4000-8000-000000000942",
            "intent_digest": "sha256:original-intent",
        }
        stored = {}

        def create(**kwargs):
            lease = kwargs["body"]
            lease["metadata"].update(uid="created-uid", resourceVersion="1")
            import copy

            stored["lease"] = copy.deepcopy(lease)
            return copy.deepcopy(lease)

        controller.coordination_api.create_namespaced_lease.side_effect = create
        controller.coordination_api.replace_namespaced_lease.side_effect = RuntimeError(
            "process stopped"
        )
        with pytest.raises(RuntimeError, match="process stopped"):
            await controller._ensure_workspace_cleanup_carrier(
                reservation,
                source="controller_rootdisk_delete",
                owner_kind="job",
                owner_id=SAMPLE_JOB_CONFIG["job_id"],
                pvc_uid="old-pvc",
                dv_uid="old-dv",
                provision_generation=PROVISION_GENERATION,
            )
        controller.coordination_api.list_namespaced_lease.return_value = {
            "items": [stored["lease"]]
        }
        controller.coordination_api.read_namespaced_lease.side_effect = (
            lambda **kwargs: stored["lease"]
        )

        def replace(**kwargs):
            stored["lease"] = kwargs["body"]
            return stored["lease"]

        controller.coordination_api.replace_namespaced_lease.side_effect = replace
        controller._refresh_workspace_cleanup_carrier = (
            VMController._refresh_workspace_cleanup_carrier.__get__(controller)
        )
        (carrier,) = await controller._list_workspace_cleanup_carriers()
        sealed = await controller._refresh_workspace_cleanup_carrier(carrier)
        assert sealed["carrier_uid"] == "created-uid"
        assert sealed["carrier_sealed"] is True
        assert (
            "srw.io/cleanup-carrier-signature"
            in stored["lease"]["metadata"]["annotations"]
        )
        # The restarted delete reaches completion only after sealing the UID
        # and revalidating the database admission; no disk is recreated here.
        controller._get_dv = AsyncMock(return_value=None)
        controller.core_api.read_namespaced_persistent_volume_claim.side_effect = (
            _FakeApiException(status=404)
        )
        assert await controller._reconcile_workspace_cleanup_carrier(sealed) is True
        controller._resume_workspace_cleanup_reservation.assert_awaited_once()
        controller._complete_workspace_cleanup_reservation.assert_awaited_once()
        controller.k8s_client.create_namespaced_custom_object.assert_not_called()

    @pytest.mark.asyncio
    async def test_acquire_refuses_server_digest_for_another_cleanup_intent(
        self, controller
    ):
        controller._acquire_workspace_cleanup_reservation = (
            VMController._acquire_workspace_cleanup_reservation.__get__(controller)
        )
        controller._workspace_cleanup_authority_request = AsyncMock(
            return_value={
                "allowed": True,
                "admission_id": "00000000-0000-4000-8000-000000000940",
                "request_id": "00000000-0000-4000-8000-000000000999",
                "intent_digest": "sha256:another-intent",
                "completed_outcome": None,
            }
        )

        with pytest.raises(RuntimeError, match="identity"):
            await controller._acquire_workspace_cleanup_reservation(
                source="controller_rootdisk_delete",
                owner_kind="job",
                owner_id=SAMPLE_JOB_CONFIG["job_id"],
                pvc_uid="00000000-0000-4000-8000-000000000942",
                dv_uid="old-dv-uid",
                provision_generation=PROVISION_GENERATION,
            )

        controller.coordination_api.create_namespaced_lease.assert_not_called()

    @pytest.mark.asyncio
    async def test_acquire_publishes_exact_carrier_before_destructive_return(
        self, controller
    ):
        trace: list[str] = []
        admission_id = "00000000-0000-4000-8000-000000000941"

        async def authority(_path, payload, *, operation):
            trace.append("database")
            identity = {
                key: payload[key]
                for key in (
                    "source",
                    "owner_kind",
                    "owner_id",
                    "pvc_uid",
                    "dv_uid",
                    "provision_generation",
                )
            }
            digest = (
                "sha256:"
                + hashlib.sha256(
                    json.dumps(
                        identity,
                        sort_keys=True,
                        separators=(",", ":"),
                        ensure_ascii=True,
                    ).encode("utf-8")
                ).hexdigest()
            )
            return {
                "allowed": True,
                "admission_id": admission_id,
                "request_id": payload["request_id"],
                "intent_digest": digest,
                "completed_outcome": None,
            }

        def create_carrier(**kwargs):
            trace.append("carrier")
            body = kwargs["body"]
            return {
                **body,
                "metadata": {
                    **body["metadata"],
                    "uid": "cleanup-carrier-uid",
                    "resourceVersion": "1",
                },
            }

        controller._acquire_workspace_cleanup_reservation = (
            VMController._acquire_workspace_cleanup_reservation.__get__(controller)
        )
        controller._workspace_cleanup_authority_request = AsyncMock(
            side_effect=authority
        )
        controller.coordination_api.create_namespaced_lease.side_effect = create_carrier

        result = await controller._acquire_workspace_cleanup_reservation(
            source="controller_rootdisk_delete",
            owner_kind="job",
            owner_id=SAMPLE_JOB_CONFIG["job_id"],
            pvc_uid="00000000-0000-4000-8000-000000000942",
            dv_uid="old-dv-uid",
            provision_generation=PROVISION_GENERATION,
        )

        assert trace == ["database", "carrier"]
        assert result["carrier"]["admission_id"] == admission_id
        body = controller.coordination_api.create_namespaced_lease.call_args.kwargs[
            "body"
        ]
        annotations = body["metadata"]["annotations"]
        assert annotations["srw.io/cleanup-old-dv-uid"] == "old-dv-uid"
        assert annotations["srw.io/cleanup-generation"] == PROVISION_GENERATION
        assert annotations["srw.io/cleanup-intent-digest"].startswith("sha256:")
        import copy

        sealed_lease = copy.deepcopy(
            controller.coordination_api.replace_namespaced_lease.call_args.kwargs[
                "body"
            ]
        )
        sealed_annotations = sealed_lease["metadata"]["annotations"]
        assert "srw.io/cleanup-creation-signature" not in sealed_annotations
        sealed_annotations.pop("srw.io/cleanup-carrier-signature")
        sealed_lease["metadata"]["uid"] = "copied-lease-uid"
        with pytest.raises(RuntimeError, match="authentication"):
            controller._parse_workspace_cleanup_carrier(sealed_lease)

    @pytest.mark.asyncio
    async def test_captured_delete_retry_settles_exact_absence_from_carrier(
        self, controller
    ):
        carrier_lease = _cleanup_carrier_lease()
        controller.coordination_api.list_namespaced_lease.return_value = {
            "items": [carrier_lease]
        }
        controller._get_dv = AsyncMock(return_value=None)
        controller.core_api.read_namespaced_persistent_volume_claim.side_effect = (
            _FakeApiException(status=404)
        )

        async def complete(carrier, *, outcome):
            assert outcome == "deleted"
            await controller._delete_workspace_cleanup_carrier(carrier)

        controller._complete_workspace_cleanup_reservation.side_effect = complete

        await controller._delete_captured_rootdisk(
            f"agent-vm-{SAMPLE_JOB_CONFIG['job_id']}-rootdisk",
            owner_kind="job",
            owner_id=SAMPLE_JOB_CONFIG["job_id"],
            expected_pvc_uid=("00000000-0000-4000-8000-000000000903"),
        )

        controller._complete_workspace_cleanup_reservation.assert_awaited_once()
        assert (
            controller._complete_workspace_cleanup_reservation.await_args.kwargs[
                "outcome"
            ]
            == "deleted"
        )
        controller.coordination_api.delete_namespaced_lease.assert_called_once()

    @pytest.mark.asyncio
    async def test_captured_delete_retains_carrier_while_old_uid_is_observable(
        self, controller
    ):
        owner_id = SAMPLE_JOB_CONFIG["job_id"]
        name = f"agent-vm-{owner_id}-rootdisk"
        pvc_uid = "00000000-0000-4000-8000-000000000943"
        dv = {
            "metadata": {
                "name": name,
                "uid": "old-dv-uid",
                "labels": {
                    "srw.io/owner-kind": "job",
                    "srw.io/owner-id": owner_id,
                },
            }
        }
        pvc = types.SimpleNamespace(
            metadata=types.SimpleNamespace(
                name=name,
                uid=pvc_uid,
                labels={
                    "srw.io/owner-kind": "job",
                    "srw.io/owner-id": owner_id,
                },
                owner_references=[
                    types.SimpleNamespace(
                        kind="DataVolume", uid="old-dv-uid", controller=True
                    )
                ],
            )
        )
        controller._get_dv = AsyncMock(return_value=dv)
        controller.core_api.list_namespaced_persistent_volume_claim.side_effect = None
        controller.core_api.list_namespaced_persistent_volume_claim.return_value = (
            types.SimpleNamespace(items=[pvc])
        )
        controller.core_api.read_namespaced_persistent_volume_claim.side_effect = None
        controller.core_api.read_namespaced_persistent_volume_claim.return_value = pvc

        with pytest.raises(RuntimeError, match="still reconciling"):
            await controller._delete_captured_rootdisk(
                name,
                owner_kind="job",
                owner_id=owner_id,
                expected_pvc_uid=pvc_uid,
            )

        controller._complete_workspace_cleanup_reservation.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_gc_settles_lost_delete_completion_when_dv_is_no_longer_listed(
        self, controller
    ):
        carrier_lease = _cleanup_carrier_lease()

        def list_leases(**kwargs):
            if kwargs.get("label_selector") == (
                "srw.io/vm-workspace-cleanup-carrier=true"
            ):
                return {"items": [carrier_lease]}
            return {"items": []}

        controller.coordination_api.list_namespaced_lease.side_effect = list_leases
        controller.k8s_client.list_namespaced_custom_object.return_value = {"items": []}
        controller._get_dv = AsyncMock(return_value=None)
        controller.core_api.read_namespaced_persistent_volume_claim.side_effect = (
            _FakeApiException(status=404)
        )

        async def complete(carrier, *, outcome):
            assert outcome == "deleted"
            await controller._delete_workspace_cleanup_carrier(carrier)

        controller._complete_workspace_cleanup_reservation.side_effect = complete

        await controller._gc_rootdisks()

        controller._complete_workspace_cleanup_reservation.assert_awaited_once()
        controller.coordination_api.delete_namespaced_lease.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_settles_interrupted_delete_before_replacement_with_gc_disabled(
        self, controller
    ):
        owner_id = SAMPLE_JOB_CONFIG["job_id"]
        carrier_lease = _cleanup_carrier_lease()
        controller.coordination_api.list_namespaced_lease.return_value = {
            "items": [carrier_lease]
        }
        controller._get_dv = AsyncMock(return_value=None)
        controller.core_api.read_namespaced_persistent_volume_claim.side_effect = (
            _FakeApiException(status=404)
        )
        trace = []

        async def complete(carrier, *, outcome):
            assert outcome == "deleted"
            trace.append("complete")

        def create(**kwargs):
            trace.append("create")
            return kwargs["body"]

        controller._complete_workspace_cleanup_reservation.side_effect = complete
        controller.k8s_client.create_namespaced_custom_object.side_effect = create
        with patch("vm_controller.controller.VM_ROOTDISK_GC_ENABLED", False):
            await controller._ensure_rootdisk(
                controller.render_template(SAMPLE_JOB_CONFIG),
                owner_id,
                provision_generation=PROVISION_GENERATION,
            )
        assert trace == ["complete", "create"]

    @pytest.mark.asyncio
    async def test_failed_recreate_resumes_from_carrier_when_old_dv_is_absent(
        self, controller
    ):
        owner_id = SAMPLE_JOB_CONFIG["job_id"]
        name = f"agent-vm-{owner_id}-rootdisk"
        carrier_lease = _cleanup_carrier_lease(
            source="controller_failed_dv_recreate",
            outcome="recreated",
        )
        successor_dv = {
            "metadata": {
                "name": name,
                "uid": "successor-dv-uid",
                "labels": {
                    "srw.io/owner-kind": "job",
                    "srw.io/owner-id": owner_id,
                },
                "annotations": {
                    "srw.io/cleanup-carrier-uid": "cleanup-carrier-uid",
                    "srw.io/cleanup-nonce": ("00000000-0000-4000-8000-000000000904"),
                    "srw.io/provision-generation": PROVISION_GENERATION,
                },
            },
            "status": {"phase": "Pending"},
        }
        controller.coordination_api.list_namespaced_lease.return_value = {
            "items": [carrier_lease]
        }
        controller._get_dv = AsyncMock(side_effect=[None, None, None, successor_dv])
        successor_pvc = types.SimpleNamespace(
            metadata=types.SimpleNamespace(
                name=name,
                uid="successor-pvc-uid",
                labels={
                    "srw.io/owner-kind": "job",
                    "srw.io/owner-id": owner_id,
                },
                owner_references=[
                    types.SimpleNamespace(
                        kind="DataVolume", uid="successor-dv-uid", controller=True
                    )
                ],
            )
        )
        controller.core_api.list_namespaced_persistent_volume_claim.return_value = (
            types.SimpleNamespace(items=[successor_pvc])
        )
        controller.core_api.read_namespaced_persistent_volume_claim.side_effect = [
            _FakeApiException(status=404),
            _FakeApiException(status=404),
            successor_pvc,
            successor_pvc,
        ]
        manifest = controller.render_template(SAMPLE_JOB_CONFIG)

        await controller._ensure_rootdisk(
            manifest,
            owner_id,
            owner_kind="job",
            provision_generation=PROVISION_GENERATION,
        )

        controller._resume_workspace_cleanup_reservation.assert_awaited_once()
        create = _calls_for(
            controller.k8s_client.create_namespaced_custom_object, CDI_PLURAL
        )[0]
        annotations = create.kwargs["body"]["metadata"]["annotations"]
        assert annotations["srw.io/cleanup-carrier-uid"] == "cleanup-carrier-uid"
        assert annotations["srw.io/cleanup-nonce"] == (
            "00000000-0000-4000-8000-000000000904"
        )
        replacement = manifest["_srwRootdiskReservation"]
        assert replacement["dv_uid"] == "successor-dv-uid"
        assert replacement["pvc_uid"] == "successor-pvc-uid"
        controller.coordination_api.replace_namespaced_lease.assert_called_once()

    @pytest.mark.asyncio
    async def test_failed_recreate_waits_for_partial_old_pvc_deletion(self, controller):
        owner_id = SAMPLE_JOB_CONFIG["job_id"]
        name = f"agent-vm-{owner_id}-rootdisk"
        old_pvc_uid = "00000000-0000-4000-8000-000000000903"
        carrier_lease = _cleanup_carrier_lease(
            source="controller_failed_dv_recreate",
            outcome="recreated",
            old_pvc_uid=old_pvc_uid,
        )
        controller.coordination_api.list_namespaced_lease.return_value = {
            "items": [carrier_lease]
        }
        controller._get_dv = AsyncMock(return_value=None)
        old_pvc = types.SimpleNamespace(
            metadata=types.SimpleNamespace(
                name=name,
                uid=old_pvc_uid,
                labels={
                    "srw.io/owner-kind": "job",
                    "srw.io/owner-id": owner_id,
                },
                owner_references=[
                    types.SimpleNamespace(
                        kind="DataVolume", uid="old-dv-uid", controller=True
                    )
                ],
            )
        )
        controller.core_api.read_namespaced_persistent_volume_claim.side_effect = None
        controller.core_api.read_namespaced_persistent_volume_claim.return_value = (
            old_pvc
        )
        manifest = controller.render_template(SAMPLE_JOB_CONFIG)

        with pytest.raises(RuntimeError, match="still deleting"):
            await controller._ensure_rootdisk(
                manifest,
                owner_id,
                owner_kind="job",
                provision_generation=PROVISION_GENERATION,
            )

        assert not _calls_for(
            controller.k8s_client.create_namespaced_custom_object, CDI_PLURAL
        )

    @pytest.mark.asyncio
    async def test_completed_recreate_carrier_only_replays_bound_existing_vm(
        self, controller
    ):
        owner_id = SAMPLE_JOB_CONFIG["job_id"]
        pvc_uid = f"root-pvc-uid-{owner_id}"
        carrier_lease = _cleanup_carrier_lease(
            source="controller_failed_dv_recreate",
            outcome="recreated",
            generation="legacy",
            old_dv_uid="old-failed-dv",
            old_pvc_uid="old-failed-pvc",
            successor_dv_uid="rootdisk-dv-uid",
            successor_pvc_uid=pvc_uid,
        )

        def list_leases(**kwargs):
            if kwargs.get("label_selector") == (
                "srw.io/vm-workspace-cleanup-carrier=true"
            ):
                return {"items": [carrier_lease]}
            return {"items": []}

        def get_object(**kwargs):
            if kwargs.get("plural") == KUBEVIRT_PLURAL:
                return {
                    "metadata": {
                        "name": f"agent-vm-{owner_id}",
                        "uid": "already-admitted-vm",
                    }
                }
            value = _dv_phase("Pending")(**kwargs)
            value["metadata"]["annotations"] = {
                "srw.io/cleanup-admission-id": ("00000000-0000-4000-8000-000000000901"),
                "srw.io/cleanup-carrier-uid": "cleanup-carrier-uid",
                "srw.io/cleanup-nonce": ("00000000-0000-4000-8000-000000000904"),
                "srw.io/provision-generation": "legacy",
            }
            return value

        controller.coordination_api.list_namespaced_lease.side_effect = list_leases
        controller.k8s_client.get_namespaced_custom_object.side_effect = get_object
        controller._resume_workspace_cleanup_reservation.return_value = {
            "allowed": False,
            "completed_outcome": "recreated",
        }

        with patch("vm_controller.controller.VM_PERSISTENT_ROOTDISK", True):
            result = await controller._do_create(SAMPLE_JOB_CONFIG)

        assert result["status"] == "created"
        assert not _calls_for(
            controller.k8s_client.create_namespaced_custom_object, KUBEVIRT_PLURAL
        )
        controller._complete_workspace_cleanup_reservation.assert_not_awaited()
        controller.coordination_api.delete_namespaced_lease.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("carrier_generation", "successor_dv_uid", "message"),
        [
            ("00000000-0000-4000-8000-000000000099", None, "generation"),
            (PROVISION_GENERATION, "different-successor-dv", "successor"),
        ],
    )
    async def test_failed_recreate_refuses_stale_carrier_generation_or_identity(
        self, controller, carrier_generation, successor_dv_uid, message
    ):
        owner_id = SAMPLE_JOB_CONFIG["job_id"]
        carrier_lease = _cleanup_carrier_lease(
            source="controller_failed_dv_recreate",
            outcome="recreated",
            generation=carrier_generation,
            successor_dv_uid=successor_dv_uid,
            successor_pvc_uid=("different-successor-pvc" if successor_dv_uid else None),
        )
        controller.coordination_api.list_namespaced_lease.return_value = {
            "items": [carrier_lease]
        }
        controller._get_dv = AsyncMock(return_value=None)
        manifest = controller.render_template(SAMPLE_JOB_CONFIG)

        with pytest.raises(RuntimeError, match=message):
            await controller._ensure_rootdisk(
                manifest,
                owner_id,
                owner_kind="job",
                provision_generation=PROVISION_GENERATION,
            )

        assert not _calls_for(
            controller.k8s_client.create_namespaced_custom_object, CDI_PLURAL
        )

    @pytest.mark.asyncio
    async def test_delete_carrier_uid_drift_never_deletes_by_name(self, controller):
        carrier_lease = _cleanup_carrier_lease(old_dv_uid="expected-old-dv")
        controller.coordination_api.list_namespaced_lease.return_value = {
            "items": [carrier_lease]
        }
        controller._get_dv = AsyncMock(
            return_value={
                "metadata": {
                    "name": (f"agent-vm-{SAMPLE_JOB_CONFIG['job_id']}-rootdisk"),
                    "uid": "replacement-dv",
                }
            }
        )

        await controller._reconcile_workspace_cleanup_carriers()

        assert not _calls_for(
            controller.k8s_client.delete_namespaced_custom_object, CDI_PLURAL
        )
        controller._complete_workspace_cleanup_reservation.assert_not_awaited()
        controller.coordination_api.delete_namespaced_lease.assert_not_called()


class TestGcRootdisks:
    """Layer 3 of the rootdisk GC — the orphan net for disks whose entity row
    the orchestrator no longer knows (a dev DB reset, a deleted row). Off by
    default: it cannot consult the DB, so it cannot tell a leaked disk from a
    long-suspended session's workspace."""

    def _dv(self, name: str, age_h: float):
        from datetime import datetime, timedelta, timezone

        ts = datetime.now(timezone.utc) - timedelta(hours=age_h)
        return {
            "metadata": {
                "name": name,
                "uid": f"dv-uid-{name}",
                "labels": {
                    "srw.io/rootdisk": "true",
                    "srw.io/owner-kind": "job",
                    "srw.io/owner-id": name[len("agent-vm-") : -len("-rootdisk")],
                },
                "creationTimestamp": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
        }

    def _wire(self, controller, dvs, vm_names):
        def _list(**kwargs):
            if kwargs.get("plural") == CDI_PLURAL:
                return {"items": dvs}
            return {"items": [{"metadata": {"name": n}} for n in vm_names]}

        controller.k8s_client.list_namespaced_custom_object.side_effect = _list

    @pytest.mark.asyncio
    async def test_old_orphan_is_deleted(self, controller):
        self._wire(controller, [self._dv("agent-vm-j1-rootdisk", 100)], [])
        with patch("vm_controller.controller.VM_ROOTDISK_ORPHAN_HOURS", 72):
            await controller._gc_rootdisks()

        deletes = _calls_for(
            controller.k8s_client.delete_namespaced_custom_object, CDI_PLURAL
        )
        assert [c.kwargs["name"] for c in deletes] == ["agent-vm-j1-rootdisk"]

    @pytest.mark.asyncio
    async def test_disk_with_a_live_vm_is_spared(self, controller):
        """A recovery in flight: the disk is old, but its VM is back."""
        self._wire(controller, [self._dv("agent-vm-j1-rootdisk", 100)], ["agent-vm-j1"])
        with patch("vm_controller.controller.VM_ROOTDISK_ORPHAN_HOURS", 72):
            await controller._gc_rootdisks()

        assert not _calls_for(
            controller.k8s_client.delete_namespaced_custom_object, CDI_PLURAL
        )

    @pytest.mark.asyncio
    async def test_young_orphan_is_spared(self, controller):
        """A kept disk is SUPPOSED to outlive its VM during a recovery."""
        self._wire(controller, [self._dv("agent-vm-j1-rootdisk", 1)], [])
        with patch("vm_controller.controller.VM_ROOTDISK_ORPHAN_HOURS", 72):
            await controller._gc_rootdisks()

        assert not _calls_for(
            controller.k8s_client.delete_namespaced_custom_object, CDI_PLURAL
        )

    @pytest.mark.asyncio
    async def test_vm_list_failure_deletes_nothing(self, controller):
        """Without the VM list every disk looks orphaned — bail, don't guess."""

        def _list(**kwargs):
            if kwargs.get("plural") == CDI_PLURAL:
                return {"items": [self._dv("agent-vm-j1-rootdisk", 100)]}
            raise _FakeApiException(status=500)

        controller.k8s_client.list_namespaced_custom_object.side_effect = _list
        with patch("vm_controller.controller.VM_ROOTDISK_ORPHAN_HOURS", 72):
            await controller._gc_rootdisks()

        assert not _calls_for(
            controller.k8s_client.delete_namespaced_custom_object, CDI_PLURAL
        )

    @pytest.mark.asyncio
    async def test_exact_recovery_pin_prevents_orphan_gc(self, controller):
        self._wire(controller, [self._dv("agent-vm-j1-rootdisk", 100)], [])
        controller.coordination_api.list_namespaced_lease.return_value = {
            "items": [
                {
                    "metadata": {
                        "uid": "pin-uid",
                        "resourceVersion": "1",
                        "labels": {
                            "srw.io/vm-workspace-recovery-pin": "true",
                            "srw.io/recovery-id": (
                                "00000000-0000-4000-8000-000000000741"
                            ),
                            "srw.io/recovery-pvc-uid": "root-pvc-uid-j1",
                            "srw.io/recovery-generation": PROVISION_GENERATION,
                        },
                    }
                }
            ]
        }
        with patch("vm_controller.controller.VM_ROOTDISK_ORPHAN_HOURS", 72):
            await controller._gc_rootdisks()

        assert not _calls_for(
            controller.k8s_client.delete_namespaced_custom_object, CDI_PLURAL
        )

    @pytest.mark.asyncio
    async def test_orphan_gc_holds_database_cleanup_reservation_through_delete(
        self, controller
    ):
        self._wire(controller, [self._dv("agent-vm-j1-rootdisk", 100)], [])
        read_pvc = (
            controller.core_api.read_namespaced_persistent_volume_claim.side_effect
        )
        admitted_pvc = read_pvc(name="agent-vm-j1-rootdisk", namespace=VM_NAMESPACE)
        controller.core_api.read_namespaced_persistent_volume_claim.side_effect = [
            admitted_pvc,
            _FakeApiException(status=404),
            _FakeApiException(status=404),
        ]
        controller._get_dv = AsyncMock(return_value=None)
        with patch("vm_controller.controller.VM_ROOTDISK_ORPHAN_HOURS", 72):
            await controller._gc_rootdisks()

        controller._acquire_workspace_cleanup_reservation.assert_awaited_once()
        controller._complete_workspace_cleanup_reservation.assert_awaited_once()
        assert (
            controller._complete_workspace_cleanup_reservation.await_args.kwargs[
                "outcome"
            ]
            == "deleted"
        )

    @pytest.mark.asyncio
    async def test_pin_authority_failure_aborts_orphan_gc(self, controller):
        self._wire(controller, [self._dv("agent-vm-j1-rootdisk", 100)], [])
        controller.coordination_api.list_namespaced_lease.side_effect = RuntimeError(
            "API down"
        )
        with (
            patch("vm_controller.controller.VM_ROOTDISK_ORPHAN_HOURS", 72),
            pytest.raises(RuntimeError, match="API down"),
        ):
            await controller._gc_rootdisks()

        assert not _calls_for(
            controller.k8s_client.delete_namespaced_custom_object, CDI_PLURAL
        )

    @pytest.mark.asyncio
    async def test_unknown_pvc_identity_refuses_orphan_gc(self, controller):
        self._wire(controller, [self._dv("agent-vm-j1-rootdisk", 100)], [])
        controller.core_api.read_namespaced_persistent_volume_claim.side_effect = (
            RuntimeError("apiserver unavailable")
        )

        with patch("vm_controller.controller.VM_ROOTDISK_ORPHAN_HOURS", 72):
            await controller._gc_rootdisks()

        assert not _calls_for(
            controller.k8s_client.delete_namespaced_custom_object, CDI_PLURAL
        )

    @pytest.mark.asyncio
    async def test_pin_activation_cannot_ack_after_gc_crosses_delete_boundary(
        self, controller
    ):
        owner_id = SAMPLE_JOB_CONFIG["job_id"]
        pvc_uid = "00000000-0000-4000-8000-000000000742"
        dv = self._dv(f"agent-vm-{owner_id}-rootdisk", 100)
        self._wire(controller, [dv], [])
        entered = asyncio.Event()
        continue_delete = asyncio.Event()
        deleted = False

        async def probe(*_args, **_kwargs):
            entered.set()
            await continue_delete.wait()
            return True, pvc_uid

        async def pvc_by_uid(*_args, **_kwargs):
            if deleted:
                return True, None
            return True, types.SimpleNamespace(
                metadata=types.SimpleNamespace(
                    name=f"agent-vm-{owner_id}-rootdisk",
                    uid=pvc_uid,
                    labels={
                        "srw.io/owner-kind": "job",
                        "srw.io/owner-id": owner_id,
                    },
                    owner_references=[
                        types.SimpleNamespace(
                            kind="DataVolume",
                            uid=dv["metadata"]["uid"],
                            controller=True,
                        )
                    ],
                )
            )

        async def delete(*_args, **_kwargs):
            nonlocal deleted
            deleted = True

        controller._rootdisk_pvc_probe = probe
        controller._rootdisk_pvc_by_uid = pvc_by_uid
        controller._get_dv = AsyncMock(
            side_effect=lambda _name: None if deleted else dv
        )
        controller._delete_dv = AsyncMock(side_effect=delete)
        controller.coordination_api.read_namespaced_lease.side_effect = (
            _FakeApiException(status=404)
        )
        controller.coordination_api.create_namespaced_lease.return_value = (
            types.SimpleNamespace(
                metadata=types.SimpleNamespace(uid="pin-uid", resource_version="1")
            )
        )
        command = {
            "recovery_id": "00000000-0000-4000-8000-000000000741",
            "pvc_uid": pvc_uid,
            "provision_generation": PROVISION_GENERATION,
            "state": "active",
            "owner_kind": "job",
            "owner_id": owner_id,
            "namespace": VM_NAMESPACE,
        }

        with patch("vm_controller.controller.VM_ROOTDISK_ORPHAN_HOURS", 72):
            gc_task = asyncio.create_task(controller._gc_rootdisks())
            await entered.wait()
            pin_task = asyncio.create_task(
                controller._do_reconcile_workspace_recovery_pin(command)
            )
            await asyncio.sleep(0)
            crossed = pin_task.done()
            continue_delete.set()
            await gc_task

        assert crossed is False
        with pytest.raises(RuntimeError, match="PVC identity is unavailable"):
            await pin_task

    @pytest.mark.asyncio
    async def test_create_does_not_run_it_when_disabled(self, controller):
        controller._gc_rootdisks_safe = AsyncMock()
        controller.k8s_client.get_namespaced_custom_object.side_effect = _dv_phase(None)
        with (
            patch("vm_controller.controller.VM_PERSISTENT_ROOTDISK", True),
            patch("vm_controller.controller.VM_ROOTDISK_GC_ENABLED", False),
        ):
            await controller._do_create(SAMPLE_JOB_CONFIG)

        controller._gc_rootdisks_safe.assert_not_called()


class TestRootdiskCloneSourceNamespace:
    """A templated DataVolume may omit spec.source.pvc.namespace — CDI defaults
    it from the owning VM. A STANDALONE one may not: the CDI webhook rejects it
    with 422 'spec.source.pvc.namespace: Required value'.

    Live-gate finding 2026-07-29 (job a43bfb73): every VM create failed the
    moment the flag was flipped. Unit tests could not have caught it — the k8s
    mock accepts any body — so this test pins the shape the API demands.
    """

    @pytest.mark.asyncio
    async def test_clone_source_carries_an_explicit_namespace(self, controller):
        controller._golden_state_nowait = AsyncMock(
            return_value=("agent-vm-golden-abc123def456", None)
        )
        controller.k8s_client.get_namespaced_custom_object.side_effect = _dv_phase(None)
        with (
            patch("vm_controller.controller.VM_GOLDEN_IMAGE_ENABLED", True),
            patch("vm_controller.controller.VM_GOLDEN_GC_ENABLED", False),
            patch("vm_controller.controller.VM_PERSISTENT_ROOTDISK", True),
        ):
            await controller._do_create(SAMPLE_JOB_CONFIG)

        dv = _dv_create_body(controller)
        assert dv["spec"]["source"]["pvc"] == {
            "name": "agent-vm-golden-abc123def456",
            # Same namespace as the target, so no cross-namespace clone RBAC is
            # involved — it just has to be stated.
            "namespace": VM_NAMESPACE,
        }

    @pytest.mark.asyncio
    async def test_an_explicit_namespace_is_left_alone(self, controller):
        controller.k8s_client.get_namespaced_custom_object.side_effect = _dv_phase(None)
        controller.template_text = SAMPLE_TEMPLATE.replace(
            "        source:\n          registry:\n            url: docker://${VM_IMAGE}",
            "        source:\n          pvc:\n            name: some-golden\n"
            "            namespace: other-ns",
        )
        with patch("vm_controller.controller.VM_PERSISTENT_ROOTDISK", True):
            await controller._do_create(SAMPLE_JOB_CONFIG)

        dv = _dv_create_body(controller)
        assert dv["spec"]["source"]["pvc"]["namespace"] == "other-ns"

    @pytest.mark.asyncio
    async def test_registry_source_is_untouched(self, controller):
        """No golden → registry import, which has no namespace concept."""
        controller.k8s_client.get_namespaced_custom_object.side_effect = _dv_phase(None)
        with patch("vm_controller.controller.VM_PERSISTENT_ROOTDISK", True):
            await controller._do_create(SAMPLE_JOB_CONFIG)

        dv = _dv_create_body(controller)
        assert "registry" in dv["spec"]["source"]
        assert "pvc" not in dv["spec"]["source"]


class TestLifecycleAuthenticationReplayGuard:
    @pytest.mark.asyncio
    async def test_duplicate_mutating_request_is_rejected(self, controller):
        controller._do_create = AsyncMock(
            return_value={
                "job_id": SAMPLE_JOB_CONFIG["job_id"],
                "status": "created",
                "provision_generation": PROVISION_GENERATION,
            }
        )
        controller.nc = AsyncMock()
        payload = sign_payload(
            {
                **SAMPLE_JOB_CONFIG,
                "provision_generation": PROVISION_GENERATION,
            },
            direction="request",
            operation="create",
            secret=LIFECYCLE_SECRET,
        )
        msg = MagicMock(data=json.dumps(payload).encode())

        with patch("vm_controller.controller.LIFECYCLE_HMAC_SECRET", LIFECYCLE_SECRET):
            await controller.handle_create(msg)
            await controller.handle_create(msg)

        controller._do_create.assert_awaited_once()
        controller.nc.publish.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_old_create_replay_after_delete_is_rejected(self, controller):
        controller._do_create = AsyncMock(
            return_value={
                "job_id": SAMPLE_JOB_CONFIG["job_id"],
                "status": "created",
                "provision_generation": PROVISION_GENERATION,
            }
        )
        controller._do_delete = AsyncMock(
            return_value={
                "job_id": SAMPLE_JOB_CONFIG["job_id"],
                "status": "deleted",
                "provision_generation": PROVISION_GENERATION,
            }
        )
        controller.nc = AsyncMock()
        create_payload = sign_payload(
            {
                **SAMPLE_JOB_CONFIG,
                "provision_generation": PROVISION_GENERATION,
            },
            direction="request",
            operation="create",
            secret=LIFECYCLE_SECRET,
        )
        delete_payload = sign_payload(
            {
                "job_id": SAMPLE_JOB_CONFIG["job_id"],
                "purge_disk": True,
                "provision_generation": PROVISION_GENERATION,
            },
            direction="request",
            operation="delete",
            secret=LIFECYCLE_SECRET,
        )

        with patch("vm_controller.controller.LIFECYCLE_HMAC_SECRET", LIFECYCLE_SECRET):
            await controller.handle_create(
                MagicMock(data=json.dumps(create_payload).encode())
            )
            await controller.handle_delete(
                MagicMock(data=json.dumps(delete_payload).encode())
            )
            await controller.handle_create(
                MagicMock(data=json.dumps(create_payload).encode())
            )

        controller._do_create.assert_awaited_once()
        controller._do_delete.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_replay_is_rejected_after_controller_restart(self):
        claimed: set[str] = set()

        def _claim(*, body, **_kwargs):
            name = body["metadata"]["name"]
            if name in claimed:
                raise _FakeApiException(status=409, body="already claimed")
            claimed.add(name)
            return body

        first = _make_controller()
        restarted = _make_controller()
        durable_api = MagicMock()
        durable_api.create_namespaced_lease.side_effect = _claim
        first.coordination_api = durable_api
        restarted.coordination_api = durable_api
        first._do_create = AsyncMock(
            return_value={"job_id": "restart-replay", "status": "created"}
        )
        restarted._do_create = AsyncMock(
            return_value={"job_id": "restart-replay", "status": "created"}
        )
        payload = sign_payload(
            {
                **SAMPLE_JOB_CONFIG,
                "job_id": "restart-replay",
                "provision_generation": PROVISION_GENERATION,
            },
            direction="request",
            operation="create",
            secret=LIFECYCLE_SECRET,
        )
        message = MagicMock(data=json.dumps(payload).encode())

        with patch("vm_controller.controller.LIFECYCLE_HMAC_SECRET", LIFECYCLE_SECRET):
            await first.handle_create(message)
            await restarted.handle_create(message)

        first._do_create.assert_awaited_once()
        restarted._do_create.assert_not_awaited()
        assert durable_api.create_namespaced_lease.call_count == 2

    @pytest.mark.asyncio
    async def test_nonce_store_rbac_failure_rejects_mutation(self, controller):
        controller.coordination_api.create_namespaced_lease.side_effect = (
            _FakeApiException(status=403, body="forbidden")
        )
        controller._do_delete = AsyncMock()
        payload = sign_payload(
            {
                "job_id": "forbidden-delete",
                "purge_disk": True,
                "provision_generation": PROVISION_GENERATION,
            },
            direction="request",
            operation="delete",
            secret=LIFECYCLE_SECRET,
        )

        with patch("vm_controller.controller.LIFECYCLE_HMAC_SECRET", LIFECYCLE_SECRET):
            await controller.handle_delete(MagicMock(data=json.dumps(payload).encode()))

        controller._do_delete.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_expired_nonce_leases_are_garbage_collected(self, controller):
        from datetime import datetime, timedelta, timezone

        old = datetime.now(timezone.utc) - timedelta(hours=1)
        fresh = datetime.now(timezone.utc)
        controller.coordination_api.list_namespaced_lease.return_value = {
            "items": [
                {
                    "metadata": {
                        "name": "srw-vm-lifecycle-old",
                        "creationTimestamp": old.isoformat(),
                    }
                },
                {
                    "metadata": {
                        "name": "srw-vm-lifecycle-fresh",
                        "creationTimestamp": fresh.isoformat(),
                    }
                },
            ]
        }

        assert await controller._gc_expired_lifecycle_nonces(now=fresh)

        assert (
            controller.coordination_api.list_namespaced_lease.call_args.kwargs["limit"]
            == LIFECYCLE_NONCE_GC_PAGE_LIMIT
        )
        controller.coordination_api.delete_namespaced_lease.assert_called_once()
        assert (
            controller.coordination_api.delete_namespaced_lease.call_args.kwargs["name"]
            == "srw-vm-lifecycle-old"
        )

    @pytest.mark.asyncio
    async def test_nonce_gc_bounds_list_page_and_deletions(self, controller):
        from datetime import datetime, timedelta, timezone

        now = datetime.now(timezone.utc)
        expired = now - timedelta(hours=1)
        controller.coordination_api.list_namespaced_lease.return_value = {
            "items": [
                {
                    "metadata": {
                        "name": f"srw-vm-lifecycle-expired-{index}",
                        "creationTimestamp": expired.isoformat(),
                    }
                }
                for index in range(10)
            ]
        }

        with (
            patch("vm_controller.controller.LIFECYCLE_NONCE_GC_PAGE_LIMIT", 3),
            patch("vm_controller.controller.LIFECYCLE_NONCE_GC_DELETE_LIMIT", 2),
        ):
            assert await controller._gc_expired_lifecycle_nonces(now=now)

        assert (
            controller.coordination_api.list_namespaced_lease.call_args.kwargs["limit"]
            == 3
        )
        assert controller.coordination_api.delete_namespaced_lease.call_count == 2

    @pytest.mark.asyncio
    async def test_nonce_gc_rotates_through_bounded_pages(self, controller):
        from datetime import datetime, timezone

        controller.coordination_api.list_namespaced_lease.side_effect = [
            {"metadata": {"continue": "next-page"}, "items": []},
            {"metadata": {}, "items": []},
        ]

        assert await controller._gc_expired_lifecycle_nonces(
            now=datetime.now(timezone.utc)
        )
        assert controller._lifecycle_nonce_gc_continue == "next-page"
        assert await controller._gc_expired_lifecycle_nonces(
            now=datetime.now(timezone.utc)
        )
        assert controller._lifecycle_nonce_gc_continue is None

        calls = controller.coordination_api.list_namespaced_lease.call_args_list
        assert "_continue" not in calls[0].kwargs
        assert calls[1].kwargs["_continue"] == "next-page"

    @pytest.mark.asyncio
    async def test_nonce_gc_resets_expired_continue_token(self, controller):
        from datetime import datetime, timezone

        controller._lifecycle_nonce_gc_continue = "expired-page"
        controller.coordination_api.list_namespaced_lease.side_effect = (
            _FakeApiException(status=410, body="continue token expired")
        )

        assert await controller._gc_expired_lifecycle_nonces(
            now=datetime.now(timezone.utc)
        )
        assert controller._lifecycle_nonce_gc_continue is None

    @pytest.mark.asyncio
    async def test_post_claim_failure_requires_fresh_signed_request(self, controller):
        claimed: set[str] = set()

        def _claim(*, body, **_kwargs):
            name = body["metadata"]["name"]
            if name in claimed:
                raise _FakeApiException(status=409, body="already claimed")
            claimed.add(name)
            return body

        controller.coordination_api.create_namespaced_lease.side_effect = _claim
        controller.coordination_api.list_namespaced_lease.side_effect = [
            _FakeApiException(status=500, body="temporary list failure"),
            {"metadata": {}, "items": []},
        ]
        unsigned = {
            **SAMPLE_JOB_CONFIG,
            "provision_generation": PROVISION_GENERATION,
        }
        first = sign_payload(
            unsigned,
            direction="request",
            operation="create",
            secret=LIFECYCLE_SECRET,
        )
        fresh = sign_payload(
            unsigned,
            direction="request",
            operation="create",
            secret=LIFECYCLE_SECRET,
        )
        assert (
            first["_lifecycle_auth"]["request_id"]
            != fresh["_lifecycle_auth"]["request_id"]
        )

        with (
            patch("vm_controller.controller.LIFECYCLE_HMAC_SECRET", LIFECYCLE_SECRET),
            patch("vm_controller.controller.LIFECYCLE_NONCE_GC_INTERVAL", 1),
        ):
            assert not await controller._verify_lifecycle_request(
                first, "create", mutating=True
            )
            assert not await controller._verify_lifecycle_request(
                first, "create", mutating=True
            )
            assert await controller._verify_lifecycle_request(
                fresh, "create", mutating=True
            )


class TestLifecycleIdentityGeneration:
    @pytest.mark.asyncio
    async def test_status_recovers_rootdisk_uid_that_missed_create_wait(
        self, controller
    ):
        config = {
            **SAMPLE_JOB_CONFIG,
            "provision_generation": PROVISION_GENERATION,
        }
        read_pvc = (
            controller.core_api.read_namespaced_persistent_volume_claim.side_effect
        )
        controller.core_api.read_namespaced_persistent_volume_claim.side_effect = (
            _FakeApiException(status=404, body="not admitted yet")
        )

        with (
            patch("vm_controller.controller.LIFECYCLE_HMAC_SECRET", LIFECYCLE_SECRET),
            patch("vm_controller.controller.VM_ROOTDISK_PVC_UID_ATTEMPTS", 1),
        ):
            created = await controller._do_create(config)

        assert "rootdisk_pvc_uid" not in created
        controller.core_api.read_namespaced_persistent_volume_claim.side_effect = (
            read_pvc
        )
        controller.k8s_client.get_namespaced_custom_object.return_value = {
            "metadata": {
                "name": f"agent-vm-{SAMPLE_JOB_CONFIG['job_id']}",
                "uid": "admitted-vm-uid-001",
                "annotations": {
                    "srw.io/provision-generation": PROVISION_GENERATION,
                },
                "labels": {
                    "srw.io/owner-kind": "job",
                    "srw.io/owner-id": SAMPLE_JOB_CONFIG["job_id"],
                },
            },
            "status": {"printableStatus": "Running"},
        }

        status = await controller._do_status(SAMPLE_JOB_CONFIG["job_id"])

        assert status["vm_uid"] == "admitted-vm-uid-001"
        assert status["provision_generation"] == PROVISION_GENERATION
        assert status["rootdisk_pvc_uid"] == (
            f"root-pvc-uid-{SAMPLE_JOB_CONFIG['job_id']}"
        )
        assert "rootdisk_identity_known" not in status

    @pytest.mark.asyncio
    async def test_exact_absence_status_opt_in_adds_rootdisk_identity_evidence(
        self, controller
    ):
        job_id = SAMPLE_JOB_CONFIG["job_id"]
        controller.k8s_client.get_namespaced_custom_object.return_value = {
            "metadata": {
                "name": f"agent-vm-{job_id}",
                "uid": "admitted-vm-uid-001",
                "annotations": {
                    "srw.io/provision-generation": PROVISION_GENERATION,
                },
            },
            "status": {"printableStatus": "Running"},
        }

        status = await controller._do_status(
            job_id,
            PROVISION_GENERATION,
            exact_absence=True,
        )

        assert status["rootdisk_identity_known"] is True
        assert status["rootdisk_pvc_uid"] == f"root-pvc-uid-{job_id}"

    @pytest.mark.asyncio
    async def test_idle_stop_probe_requires_vm_vmi_and_launcher_absence(
        self, controller
    ):
        """A missing VM alone cannot return its compute budget while a VMI remains."""
        job_id = SAMPLE_JOB_CONFIG["job_id"]
        controller.k8s_client.get_namespaced_custom_object.side_effect = _FakeApiException(404)
        controller._rootdisk_pvc_probe = AsyncMock(
            return_value=(True, f"root-pvc-uid-{job_id}")
        )
        controller.core_api.list_namespaced_pod.return_value = MagicMock(items=[])
        status = await controller._do_status(
            job_id, PROVISION_GENERATION, exact_absence=True
        )
        assert status["status"] == "not_found"
        assert status["runtime_absence_known"] is True
        assert status["vmi_absent"] is True
        assert status["launcher_absent"] is True

        controller.k8s_client.get_namespaced_custom_object.side_effect = [
            _FakeApiException(404),
            {"metadata": {"uid": "same-generation-vmi"}},
        ]
        status = await controller._do_status(
            job_id, PROVISION_GENERATION, exact_absence=True
        )
        assert status["runtime_absence_known"] is False

    @pytest.mark.asyncio
    async def test_authenticated_delete_uses_admitted_uid_precondition(
        self, controller
    ):
        job_id = "generation-delete"
        controller.k8s_client.get_namespaced_custom_object.return_value = {
            "metadata": {
                "name": f"agent-vm-{job_id}",
                "uid": "generation-delete-vm-uid",
                "annotations": {
                    "srw.io/provision-generation": PROVISION_GENERATION,
                },
            }
        }

        with patch("vm_controller.controller.LIFECYCLE_HMAC_SECRET", LIFECYCLE_SECRET):
            result = await controller._do_delete(
                job_id, provision_generation=PROVISION_GENERATION
            )

        vm_delete = _calls_for(
            controller.k8s_client.delete_namespaced_custom_object,
            KUBEVIRT_PLURAL,
        )[0]
        assert vm_delete.kwargs["body"]["preconditions"] == {
            "uid": "generation-delete-vm-uid"
        }
        assert result["provision_generation"] == PROVISION_GENERATION
        assert result["generation_evidence"] == "admitted-vm-metadata"

    @pytest.mark.asyncio
    async def test_authenticated_delete_rejects_generation_mismatch(self, controller):
        job_id = "generation-reused"
        controller.k8s_client.get_namespaced_custom_object.return_value = {
            "metadata": {
                "name": f"agent-vm-{job_id}",
                "uid": "replacement-vm-uid",
                "annotations": {
                    "srw.io/provision-generation": (
                        "00000000-0000-4000-8000-000000000099"
                    ),
                },
            }
        }

        with (
            patch("vm_controller.controller.LIFECYCLE_HMAC_SECRET", LIFECYCLE_SECRET),
            pytest.raises(RuntimeError, match="another provision generation"),
        ):
            await controller._do_delete(
                job_id, provision_generation=PROVISION_GENERATION
            )

        controller.k8s_client.delete_namespaced_custom_object.assert_not_called()


class TestWorkspaceRecoveryControllerEvidence:
    VM_UID = "00000000-0000-4000-8000-000000000701"
    OLD_VMI_UID = "00000000-0000-4000-8000-000000000702"
    OLD_POD_UID = "00000000-0000-4000-8000-000000000703"
    PVC_UID = "00000000-0000-4000-8000-000000000704"
    NODE_UID = "00000000-0000-4000-8000-000000000705"

    def identity(self):
        return {
            "owner_kind": "job",
            "owner_id": SAMPLE_JOB_CONFIG["job_id"],
            "provision_generation": PROVISION_GENERATION,
            "cluster_name": "test",
            "namespace": VM_NAMESPACE,
            "vm_uid": self.VM_UID,
            "prior_vmi_uid": self.OLD_VMI_UID,
            "prior_launcher_uid": self.OLD_POD_UID,
            "root_pvc_uid": self.PVC_UID,
        }

    def wire(self, controller, *, terminal=False, replacement=False, migration=False):
        current_vmi = (
            "00000000-0000-4000-8000-000000000712" if replacement else self.OLD_VMI_UID
        )
        current_pod = (
            "00000000-0000-4000-8000-000000000713" if replacement else self.OLD_POD_UID
        )
        vm = {
            "metadata": {
                "name": f"agent-vm-{SAMPLE_JOB_CONFIG['job_id']}",
                "uid": self.VM_UID,
                "labels": {
                    "srw.io/owner-kind": "job",
                    "srw.io/owner-id": SAMPLE_JOB_CONFIG["job_id"],
                },
                "annotations": {"srw.io/provision-generation": PROVISION_GENERATION},
            },
            "status": {
                "conditions": [{"type": "Ready", "status": "True"}],
            },
            "spec": {
                "template": {
                    "spec": {
                        "volumes": [
                            {
                                "name": "rootdisk",
                                "dataVolume": {
                                    "name": f"agent-vm-{SAMPLE_JOB_CONFIG['job_id']}-rootdisk"
                                },
                            }
                        ]
                    }
                }
            },
        }
        rootdisk_name = f"agent-vm-{SAMPLE_JOB_CONFIG['job_id']}-rootdisk"
        dv_uid = "00000000-0000-4000-8000-000000000706"
        dv = {
            "metadata": {
                "name": rootdisk_name,
                "uid": dv_uid,
                "labels": {
                    "srw.io/owner-kind": "job",
                    "srw.io/owner-id": SAMPLE_JOB_CONFIG["job_id"],
                },
            }
        }
        vmi = {
            "metadata": {
                "name": f"agent-vm-{SAMPLE_JOB_CONFIG['job_id']}",
                "uid": current_vmi,
                "ownerReferences": [
                    {
                        "kind": "VirtualMachine",
                        "uid": self.VM_UID,
                        "controller": True,
                    }
                ],
            },
            "spec": {
                "volumes": [{"name": "rootdisk", "dataVolume": {"name": rootdisk_name}}]
            },
            "status": {
                "phase": "Running" if not terminal else "Failed",
                "interfaces": [
                    {
                        "ipAddress": "10.42.0.90",
                        "mac": "02:00:00:00:07:01",
                    }
                ],
                **({"migrationState": {"migrationUid": "moving"}} if migration else {}),
            },
        }

        def get_object(**kwargs):
            if kwargs["plural"] == KUBEVIRT_PLURAL:
                return vm
            if kwargs["plural"] == CDI_PLURAL:
                return dv
            return vmi

        controller.k8s_client.get_namespaced_custom_object.side_effect = get_object
        controller.core_api.list_namespaced_persistent_volume_claim.return_value = (
            types.SimpleNamespace(
                items=[
                    types.SimpleNamespace(
                        metadata=types.SimpleNamespace(
                            name=rootdisk_name,
                            uid=self.PVC_UID,
                            deletion_timestamp=None,
                            labels={
                                "srw.io/owner-kind": "job",
                                "srw.io/owner-id": SAMPLE_JOB_CONFIG["job_id"],
                            },
                            owner_references=[
                                types.SimpleNamespace(
                                    kind="DataVolume", uid=dv_uid, controller=True
                                )
                            ],
                        )
                    )
                ]
            )
        )

        def terminated(container_id):
            return {
                "terminated": {
                    "containerID": container_id,
                    "finishedAt": "2026-09-16T12:00:00Z",
                    "reason": "Completed",
                }
            }

        pod = {
            "metadata": {
                "uid": current_pod,
                "ownerReferences": [
                    {
                        "kind": "VirtualMachineInstance",
                        "uid": current_vmi,
                        "controller": True,
                    }
                ],
            },
            "spec": {
                "nodeName": "node8",
                "restartPolicy": "Never",
                "containers": [
                    {"name": "compute", "volumeMounts": [{"name": "rootdisk"}]},
                    {"name": "guest-console-log"},
                ],
                "volumes": [
                    {
                        "name": "rootdisk",
                        "persistentVolumeClaim": {"claimName": rootdisk_name},
                    }
                ],
            },
            "status": {
                "phase": "Succeeded" if terminal else "Running",
                "podIP": "10.42.0.90",
                "containerStatuses": [
                    {
                        "name": "compute",
                        "containerID": "containerd://compute-old",
                        "restartCount": 0,
                        "state": (
                            terminated("containerd://compute-old")
                            if terminal
                            else {"running": {}}
                        ),
                    },
                    {
                        "name": "guest-console-log",
                        "containerID": "containerd://console-old",
                        "restartCount": 0,
                        "state": (
                            terminated("containerd://console-old")
                            if terminal
                            else {"running": {}}
                        ),
                    },
                ],
            },
        }
        controller.core_api.list_namespaced_pod.return_value = types.SimpleNamespace(
            items=[pod]
        )
        controller.core_api.read_node.return_value = types.SimpleNamespace(
            metadata=types.SimpleNamespace(uid=self.NODE_UID)
        )
        return pod

    @pytest.mark.asyncio
    async def test_exact_current_terminated_states_mint_stop_evidence(self, controller):
        self.wire(controller, terminal=True)

        observed = await controller._do_observe_workspace_recovery(self.identity())

        assert observed["prior_runtime"] == "stopped"
        evidence = observed["stop_evidence"]
        assert evidence["vmi_uid"] == self.OLD_VMI_UID
        assert evidence["launcher_uid"] == self.OLD_POD_UID
        assert [item["name"] for item in evidence["containers"]] == [
            "compute",
            "guest-console-log",
        ]
        controller.k8s_client.create_namespaced_custom_object.assert_not_called()
        controller.k8s_client.delete_namespaced_custom_object.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("unsafe", ["last_state", "restarted"])
    async def test_last_state_or_restarted_container_is_not_stop_proof(
        self, controller, unsafe
    ):
        pod = self.wire(controller, terminal=True)
        status = pod["status"]["containerStatuses"][0]
        if unsafe == "last_state":
            status["lastState"] = status.pop("state")
        else:
            status["restartCount"] = 1

        observed = await controller._do_observe_workspace_recovery(self.identity())

        assert observed["prior_runtime"] != "stopped"
        assert observed["stop_evidence"] == "unknown"

    @pytest.mark.asyncio
    async def test_deserialized_empty_last_state_preserves_stop_proof(self, controller):
        pod = self.wire(controller, terminal=True)
        empty_last_state = KubernetesApiClient()._ApiClient__deserialize(  # noqa: SLF001
            {}, "V1ContainerState"
        )
        assert empty_last_state
        assert all(
            getattr(empty_last_state, field) is None
            for field in ("running", "waiting", "terminated")
        )
        pod["status"]["containerStatuses"][0]["lastState"] = empty_last_state

        observed = await controller._do_observe_workspace_recovery(self.identity())

        assert observed["prior_runtime"] == "stopped"
        assert observed["stop_evidence"] != "unknown"

    @pytest.mark.asyncio
    async def test_deserialized_previous_termination_is_not_stop_proof(
        self, controller
    ):
        pod = self.wire(controller, terminal=True)
        previous_state = KubernetesApiClient()._ApiClient__deserialize(  # noqa: SLF001
            {
                "terminated": {
                    "containerID": "containerd://previous-incarnation",
                    "exitCode": 0,
                    "finishedAt": "2026-09-16T11:00:00Z",
                    "reason": "Completed",
                    "startedAt": "2026-09-16T10:00:00Z",
                }
            },
            "V1ContainerState",
        )
        assert previous_state.terminated is not None
        pod["status"]["containerStatuses"][0]["lastState"] = previous_state

        observed = await controller._do_observe_workspace_recovery(self.identity())

        assert observed["prior_runtime"] != "stopped"
        assert observed["stop_evidence"] == "unknown"

    @pytest.mark.asyncio
    async def test_malformed_last_state_is_not_stop_proof(self, controller):
        pod = self.wire(controller, terminal=True)
        pod["status"]["containerStatuses"][0]["lastState"] = {"unknownState": {}}

        observed = await controller._do_observe_workspace_recovery(self.identity())

        assert observed["prior_runtime"] != "stopped"
        assert observed["stop_evidence"] == "unknown"

    @pytest.mark.asyncio
    async def test_missing_restart_policy_is_not_stop_proof(self, controller):
        pod = self.wire(controller, terminal=True)
        pod["spec"].pop("restartPolicy")

        observed = await controller._do_observe_workspace_recovery(self.identity())

        assert observed["prior_runtime"] != "stopped"
        assert observed["stop_evidence"] == "unknown"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("termination_identity", ["missing", "mismatched"])
    async def test_ambiguous_current_termination_identity_is_not_stop_proof(
        self, controller, termination_identity
    ):
        pod = self.wire(controller, terminal=True)
        terminated = pod["status"]["containerStatuses"][0]["state"]["terminated"]
        if termination_identity == "missing":
            terminated.pop("containerID")
        else:
            terminated["containerID"] = "containerd://different-incarnation"

        observed = await controller._do_observe_workspace_recovery(self.identity())

        assert observed["prior_runtime"] != "stopped"
        assert observed["stop_evidence"] == "unknown"

    @pytest.mark.asyncio
    async def test_missing_old_launcher_is_not_stop_proof(self, controller):
        self.wire(controller, replacement=True)

        observed = await controller._do_observe_workspace_recovery(self.identity())

        assert observed["prior_runtime"] == "unknown"
        assert observed["stop_evidence"] == "unknown"
        assert observed["successor"]["launcher_uid"] != self.OLD_POD_UID

    @pytest.mark.asyncio
    async def test_migration_or_multiple_launchers_is_ambiguous(self, controller):
        self.wire(controller, migration=True)

        observed = await controller._do_observe_workspace_recovery(self.identity())

        assert observed["ambiguous"] is True
        assert observed["migration_ambiguous"] is True

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "unsafe",
        ["missing_compute", "unknown_reason", "undeclared", "duplicate"],
    )
    async def test_incomplete_or_unknown_current_container_evidence_is_rejected(
        self, controller, unsafe
    ):
        pod = self.wire(controller, terminal=True)
        statuses = pod["status"]["containerStatuses"]
        if unsafe == "missing_compute":
            statuses.pop(0)
        elif unsafe == "unknown_reason":
            statuses[0]["state"]["terminated"]["reason"] = "ContainerStatusUnknown"
        elif unsafe == "undeclared":
            statuses.append(
                {
                    "name": "unexpected-sidecar",
                    "containerID": "containerd://unexpected",
                    "restartCount": 0,
                    "state": {"terminated": {"finishedAt": "2026-09-16T12:00:00Z"}},
                }
            )
        else:
            statuses.append(dict(statuses[0]))

        observed = await controller._do_observe_workspace_recovery(self.identity())

        assert observed["prior_runtime"] != "stopped"
        assert observed["stop_evidence"] == "unknown"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "broken_link",
        ["vm_volume", "vmi_volume", "launcher_volume", "pvc_owner", "vmi_owner"],
    )
    async def test_unrelated_disk_or_non_controller_owner_cannot_mint_evidence(
        self, controller, broken_link
    ):
        pod = self.wire(controller, terminal=True)
        vm = controller.k8s_client.get_namespaced_custom_object(plural=KUBEVIRT_PLURAL)
        vmi = controller.k8s_client.get_namespaced_custom_object(plural="other")
        pvc = controller.core_api.list_namespaced_persistent_volume_claim.return_value.items[
            0
        ]
        if broken_link == "vm_volume":
            vm["spec"]["template"]["spec"]["volumes"][0]["dataVolume"]["name"] = (
                "same-owner-unrelated"
            )
        elif broken_link == "vmi_volume":
            vmi["spec"]["volumes"][0]["dataVolume"]["name"] = "same-owner-unrelated"
        elif broken_link == "launcher_volume":
            pod["spec"]["volumes"][0]["persistentVolumeClaim"]["claimName"] = (
                "same-owner-unrelated"
            )
        elif broken_link == "pvc_owner":
            pvc.metadata.owner_references[0].controller = None
        else:
            vmi["metadata"]["ownerReferences"][0].pop("controller")

        observed = await controller._do_observe_workspace_recovery(self.identity())

        assert observed["prior_runtime"] != "stopped"
        assert observed["stop_evidence"] == "unknown"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "unsafe", [None, "running", "restarted", "last_state", "missing", "grace_zero"]
    )
    async def test_native_console_sidecar_requires_real_current_termination_even_when_retained(
        self, controller, unsafe
    ):
        pod = self.wire(controller, terminal=True)
        sidecar = pod["spec"]["containers"].pop()
        sidecar["restartPolicy"] = "Always"
        pod["spec"]["initContainers"] = [sidecar]
        status = pod["status"]["containerStatuses"].pop()
        pod["status"]["initContainerStatuses"] = [status]
        # Retention is only fixture metadata; it must never create stop proof.
        pod["metadata"]["finalizers"] = ["srw.io/vm-recovery-gate-stop-evidence"]
        if unsafe == "running":
            status["state"] = {"running": {}}
        elif unsafe == "restarted":
            status["restartCount"] = 1
        elif unsafe == "last_state":
            status["lastState"] = status["state"].copy()
        elif unsafe == "missing":
            pod["status"]["initContainerStatuses"] = []
        elif unsafe == "grace_zero":
            pod["metadata"]["deletionGracePeriodSeconds"] = 0
        observed = await controller._do_observe_workspace_recovery(self.identity())
        if unsafe is None:
            assert observed["prior_runtime"] == "stopped"
            assert {
                entry["kind"] for entry in observed["stop_evidence"]["containers"]
            } == {"init", "regular"}
        else:
            assert observed["prior_runtime"] != "stopped"
            assert observed["stop_evidence"] == "unknown"


class TestWorkspaceRecoveryControllerPins:
    @pytest.mark.asyncio
    async def test_pin_scan_ignores_cleanup_carriers(self, controller):
        carrier = _cleanup_carrier_lease()
        pin = {
            "metadata": {
                "uid": "pin-uid",
                "resourceVersion": "12",
                "labels": {
                    "srw.io/vm-workspace-recovery-pin": "true",
                    "srw.io/recovery-id": ("00000000-0000-4000-8000-000000000951"),
                    "srw.io/recovery-pvc-uid": ("00000000-0000-4000-8000-000000000952"),
                    "srw.io/recovery-generation": PROVISION_GENERATION,
                },
            }
        }
        controller.coordination_api.list_namespaced_lease.return_value = {
            "items": [carrier, pin]
        }

        pins = await controller._active_recovery_pins()

        assert len(pins) == 1
        assert pins[0]["pin_uid"] == "pin-uid"

    @pytest.mark.asyncio
    async def test_recovery_pin_prevents_failed_datavolume_recreation(self, controller):
        controller.k8s_client.get_namespaced_custom_object.side_effect = _dv_phase(
            "Failed"
        )
        controller._active_recovery_pins = AsyncMock(
            return_value=({"pvc_uid": (f"root-pvc-uid-{SAMPLE_JOB_CONFIG['job_id']}")},)
        )

        with (
            patch("vm_controller.controller.VM_PERSISTENT_ROOTDISK", True),
            pytest.raises(RuntimeError, match="pinned"),
        ):
            await controller._do_create(SAMPLE_JOB_CONFIG)

        assert not _calls_for(
            controller.k8s_client.delete_namespaced_custom_object, CDI_PLURAL
        )
        assert not _calls_for(
            controller.k8s_client.create_namespaced_custom_object, CDI_PLURAL
        )

    @pytest.mark.asyncio
    async def test_unknown_pvc_identity_refuses_failed_datavolume_recreation(
        self, controller
    ):
        controller.k8s_client.get_namespaced_custom_object.side_effect = _dv_phase(
            "Failed"
        )
        controller.core_api.read_namespaced_persistent_volume_claim.side_effect = (
            RuntimeError("apiserver unavailable")
        )

        with (
            patch("vm_controller.controller.VM_PERSISTENT_ROOTDISK", True),
            pytest.raises(RuntimeError, match="identity is unknown"),
        ):
            await controller._do_create(SAMPLE_JOB_CONFIG)

        assert not _calls_for(
            controller.k8s_client.delete_namespaced_custom_object, CDI_PLURAL
        )

    @pytest.mark.asyncio
    async def test_pin_activation_cannot_ack_after_failed_dv_delete_boundary(
        self, controller
    ):
        owner_id = SAMPLE_JOB_CONFIG["job_id"]
        pvc_uid = "00000000-0000-4000-8000-000000000752"
        entered = asyncio.Event()
        continue_delete = asyncio.Event()
        deleted = False

        def get_object(**kwargs):
            if kwargs.get("plural") == KUBEVIRT_PLURAL:
                return {
                    "metadata": {
                        "name": f"agent-vm-{owner_id}",
                        "uid": "vm-uid-after-dv-recreate",
                    }
                }
            if deleted:
                raise _FakeApiException(status=404)
            return {
                "metadata": {
                    "name": f"agent-vm-{owner_id}-rootdisk",
                    "uid": "failed-dv-uid",
                    "labels": {
                        "srw.io/owner-kind": "job",
                        "srw.io/owner-id": owner_id,
                    },
                },
                "status": {"phase": "Failed"},
            }

        async def probe(*_args, **_kwargs):
            entered.set()
            await continue_delete.wait()
            return True, pvc_uid

        async def pvc_by_uid(*_args, **_kwargs):
            if deleted:
                return True, None
            return True, types.SimpleNamespace(
                metadata=types.SimpleNamespace(
                    name=f"agent-vm-{owner_id}-rootdisk",
                    uid=pvc_uid,
                    labels={
                        "srw.io/owner-kind": "job",
                        "srw.io/owner-id": owner_id,
                    },
                    owner_references=[
                        types.SimpleNamespace(
                            kind="DataVolume", uid="failed-dv-uid", controller=True
                        )
                    ],
                )
            )

        async def delete(*_args, **_kwargs):
            nonlocal deleted
            deleted = True

        controller.k8s_client.get_namespaced_custom_object.side_effect = get_object
        controller._rootdisk_pvc_probe = probe
        controller._rootdisk_pvc_by_uid = pvc_by_uid
        controller.core_api.read_namespaced_persistent_volume_claim.side_effect = (
            lambda **_kwargs: types.SimpleNamespace(
                metadata=types.SimpleNamespace(
                    name=f"agent-vm-{owner_id}-rootdisk",
                    uid=pvc_uid,
                    labels={
                        "srw.io/owner-kind": "job",
                        "srw.io/owner-id": owner_id,
                    },
                    owner_references=[
                        types.SimpleNamespace(
                            kind="DataVolume", uid="failed-dv-uid", controller=True
                        )
                    ],
                )
            )
        )
        controller._delete_dv = AsyncMock(side_effect=delete)
        controller.coordination_api.read_namespaced_lease.side_effect = (
            _FakeApiException(status=404)
        )
        controller.coordination_api.create_namespaced_lease.return_value = (
            types.SimpleNamespace(
                metadata=types.SimpleNamespace(uid="pin-uid", resource_version="1")
            )
        )
        command = {
            "recovery_id": "00000000-0000-4000-8000-000000000751",
            "pvc_uid": pvc_uid,
            "provision_generation": PROVISION_GENERATION,
            "state": "active",
            "owner_kind": "job",
            "owner_id": owner_id,
            "namespace": VM_NAMESPACE,
        }

        with patch("vm_controller.controller.VM_PERSISTENT_ROOTDISK", True):
            create_task = asyncio.create_task(controller._do_create(SAMPLE_JOB_CONFIG))
            await entered.wait()
            pin_task = asyncio.create_task(
                controller._do_reconcile_workspace_recovery_pin(command)
            )
            await asyncio.sleep(0)
            crossed = pin_task.done()
            continue_delete.set()
            with pytest.raises(RuntimeError, match="DataVolume identity"):
                await create_task

        assert crossed is False
        with pytest.raises(RuntimeError, match="PVC identity is unavailable"):
            await pin_task

    @pytest.mark.asyncio
    async def test_pin_create_replay_and_exact_release_survive_restart(
        self, controller
    ):
        recovery_id = "00000000-0000-4000-8000-000000000721"
        pvc_uid = "00000000-0000-4000-8000-000000000722"
        lease = types.SimpleNamespace(
            metadata=types.SimpleNamespace(
                uid="pin-uid-1",
                resource_version="7",
                labels={
                    "srw.io/vm-workspace-recovery-pin": "true",
                    "srw.io/recovery-id": recovery_id,
                    "srw.io/recovery-pvc-uid": pvc_uid,
                    "srw.io/recovery-generation": PROVISION_GENERATION,
                    "srw.io/owner-kind": "job",
                    "srw.io/owner-id": SAMPLE_JOB_CONFIG["job_id"],
                },
            )
        )
        controller.core_api.list_namespaced_persistent_volume_claim.return_value = (
            types.SimpleNamespace(
                items=[
                    types.SimpleNamespace(
                        metadata=types.SimpleNamespace(
                            name=f"agent-vm-{SAMPLE_JOB_CONFIG['job_id']}-rootdisk",
                            uid=pvc_uid,
                            deletion_timestamp=None,
                            labels={
                                "srw.io/owner-kind": "job",
                                "srw.io/owner-id": SAMPLE_JOB_CONFIG["job_id"],
                            },
                            owner_references=[
                                types.SimpleNamespace(
                                    kind="DataVolume",
                                    uid="pin-dv-uid",
                                    controller=True,
                                )
                            ],
                        )
                    )
                ]
            )
        )
        controller.core_api.list_namespaced_persistent_volume_claim.side_effect = None
        controller.k8s_client.get_namespaced_custom_object.return_value = {
            "metadata": {
                "name": f"agent-vm-{SAMPLE_JOB_CONFIG['job_id']}-rootdisk",
                "uid": "pin-dv-uid",
                "labels": {
                    "srw.io/owner-kind": "job",
                    "srw.io/owner-id": SAMPLE_JOB_CONFIG["job_id"],
                },
            }
        }
        controller.coordination_api.read_namespaced_lease.side_effect = [
            _FakeApiException(status=404),
            lease,
            lease,
        ]
        controller.coordination_api.create_namespaced_lease.return_value = lease
        command = {
            "recovery_id": recovery_id,
            "pvc_uid": pvc_uid,
            "provision_generation": PROVISION_GENERATION,
            "state": "active",
            "owner_kind": "job",
            "owner_id": SAMPLE_JOB_CONFIG["job_id"],
            "namespace": VM_NAMESPACE,
        }

        first = await controller._do_reconcile_workspace_recovery_pin(command)
        replay = await controller._do_reconcile_workspace_recovery_pin(command)
        released = await controller._do_reconcile_workspace_recovery_pin(
            {**command, "state": "released", "pin_uid": "pin-uid-1"}
        )

        assert first == replay
        assert released["state"] == "released"
        controller.coordination_api.create_namespaced_lease.assert_called_once()
        controller.coordination_api.delete_namespaced_lease.assert_called_once()

    @pytest.mark.asyncio
    async def test_stale_pin_release_is_refused(self, controller):
        recovery_id = "00000000-0000-4000-8000-000000000731"
        pvc_uid = "00000000-0000-4000-8000-000000000732"
        controller.coordination_api.read_namespaced_lease.return_value = (
            types.SimpleNamespace(
                metadata=types.SimpleNamespace(
                    uid="current-pin",
                    resource_version="9",
                    labels={
                        "srw.io/vm-workspace-recovery-pin": "true",
                        "srw.io/recovery-id": recovery_id,
                        "srw.io/recovery-pvc-uid": pvc_uid,
                        "srw.io/recovery-generation": PROVISION_GENERATION,
                        "srw.io/owner-kind": "job",
                        "srw.io/owner-id": SAMPLE_JOB_CONFIG["job_id"],
                    },
                )
            )
        )

        with pytest.raises(RuntimeError, match="stale"):
            await controller._do_reconcile_workspace_recovery_pin(
                {
                    "recovery_id": recovery_id,
                    "pvc_uid": pvc_uid,
                    "provision_generation": PROVISION_GENERATION,
                    "state": "released",
                    "owner_kind": "job",
                    "owner_id": SAMPLE_JOB_CONFIG["job_id"],
                    "namespace": VM_NAMESPACE,
                    "pin_uid": "stale-pin",
                }
            )
        controller.coordination_api.delete_namespaced_lease.assert_not_called()


class TestLifecycleIdentityGenerationContinuation:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("dv_owner_kind", "pvc_owner_uid", "message"),
        [
            ("thread", "captured-dv-uid", "DataVolume identity"),
            ("job", "different-dv-uid", "PVC ownership"),
        ],
    )
    async def test_captured_rootdisk_delete_requires_exact_owner_chain(
        self, controller, dv_owner_kind, pvc_owner_uid, message
    ):
        owner_id = SAMPLE_JOB_CONFIG["job_id"]
        name = _rootdisk_name(owner_id)
        pvc_uid = "00000000-0000-4000-8000-000000000911"
        controller._get_dv = AsyncMock(
            return_value={
                "metadata": {
                    "name": name,
                    "uid": "captured-dv-uid",
                    "labels": {
                        "srw.io/owner-kind": dv_owner_kind,
                        "srw.io/owner-id": owner_id,
                    },
                }
            }
        )
        controller.core_api.read_namespaced_persistent_volume_claim.side_effect = None
        controller.core_api.read_namespaced_persistent_volume_claim.return_value = (
            types.SimpleNamespace(
                metadata=types.SimpleNamespace(
                    name=name,
                    uid=pvc_uid,
                    labels={
                        "srw.io/owner-kind": "job",
                        "srw.io/owner-id": owner_id,
                    },
                    owner_references=[
                        types.SimpleNamespace(
                            kind="DataVolume", uid=pvc_owner_uid, controller=True
                        )
                    ],
                )
            )
        )
        controller.core_api.list_namespaced_persistent_volume_claim.side_effect = None
        controller.core_api.list_namespaced_persistent_volume_claim.return_value = types.SimpleNamespace(
            items=[
                controller.core_api.read_namespaced_persistent_volume_claim.return_value
            ]
        )

        with pytest.raises(RuntimeError, match=message):
            await controller._delete_captured_rootdisk(
                name,
                owner_kind="job",
                owner_id=owner_id,
                expected_pvc_uid=pvc_uid,
            )

        controller._acquire_workspace_cleanup_reservation.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_captured_rootdisk_delete_fails_closed_when_db_authority_is_down(
        self, controller
    ):
        owner_id = SAMPLE_JOB_CONFIG["job_id"]
        name = _rootdisk_name(owner_id)
        pvc_uid = "00000000-0000-4000-8000-000000000912"
        controller._get_dv = AsyncMock(
            return_value={
                "metadata": {
                    "name": name,
                    "uid": "captured-dv-uid",
                    "labels": {
                        "srw.io/owner-kind": "job",
                        "srw.io/owner-id": owner_id,
                    },
                }
            }
        )
        controller.core_api.read_namespaced_persistent_volume_claim.side_effect = None
        controller.core_api.read_namespaced_persistent_volume_claim.return_value = (
            types.SimpleNamespace(
                metadata=types.SimpleNamespace(
                    name=name,
                    uid=pvc_uid,
                    labels={
                        "srw.io/owner-kind": "job",
                        "srw.io/owner-id": owner_id,
                    },
                    owner_references=[
                        types.SimpleNamespace(
                            kind="DataVolume",
                            uid="captured-dv-uid",
                            controller=True,
                        )
                    ],
                )
            )
        )
        controller.core_api.list_namespaced_persistent_volume_claim.side_effect = None
        controller.core_api.list_namespaced_persistent_volume_claim.return_value = types.SimpleNamespace(
            items=[
                controller.core_api.read_namespaced_persistent_volume_claim.return_value
            ]
        )
        controller._acquire_workspace_cleanup_reservation.side_effect = RuntimeError(
            "workspace cleanup authority is unavailable"
        )

        with pytest.raises(RuntimeError, match="authority is unavailable"):
            await controller._delete_captured_rootdisk(
                name,
                owner_kind="job",
                owner_id=owner_id,
                expected_pvc_uid=pvc_uid,
            )

        assert not _calls_for(
            controller.k8s_client.delete_namespaced_custom_object, CDI_PLURAL
        )

    @pytest.mark.asyncio
    async def test_ordinary_purge_without_captured_pvc_uid_leaves_disk_intact(
        self, controller
    ):
        job_id = "purge-without-pvc-authority"
        controller.k8s_client.get_namespaced_custom_object.return_value = {
            "metadata": {"name": f"agent-vm-{job_id}", "uid": "vm-uid"}
        }

        result = await controller._do_delete(job_id, purge_disk=True)

        assert result["rootdisk"] == "kept"
        assert not _calls_for(
            controller.k8s_client.delete_namespaced_custom_object, CDI_PLURAL
        )

    @pytest.mark.asyncio
    async def test_captured_delete_response_loss_converges_when_vm_and_disk_absent(
        self, controller
    ):
        def _missing(**_kwargs):
            raise _FakeApiException(status=404, body="gone")

        controller.k8s_client.get_namespaced_custom_object.side_effect = _missing
        controller.core_api.read_namespaced_persistent_volume_claim.side_effect = (
            _FakeApiException(status=404, body="gone")
        )

        with patch("vm_controller.controller.LIFECYCLE_HMAC_SECRET", LIFECYCLE_SECRET):
            result = await controller._do_delete(
                "response-lost",
                provision_generation=PROVISION_GENERATION,
                expected_vm_uid="old-vm-uid",
                expected_rootdisk_pvc_uid="old-root-uid",
            )

        assert result["status"] == "deleted"
        assert result["generation_evidence"] == "request-echo-vm-absent"
        controller.k8s_client.delete_namespaced_custom_object.assert_not_called()

    @pytest.mark.asyncio
    async def test_absent_vm_with_replacement_rootdisk_uid_is_superseded(
        self, controller
    ):
        def _get(**kwargs):
            if kwargs.get("plural") == KUBEVIRT_PLURAL:
                raise _FakeApiException(status=404, body="gone")
            return None

        controller.k8s_client.get_namespaced_custom_object.side_effect = _get
        controller.core_api.read_namespaced_persistent_volume_claim.return_value = (
            types.SimpleNamespace(
                metadata=types.SimpleNamespace(
                    name=_rootdisk_name("replacement-disk"),
                    uid="replacement-root-uid",
                    labels={
                        "srw.io/owner-kind": "job",
                        "srw.io/owner-id": "replacement-disk",
                    },
                )
            )
        )
        controller.core_api.read_namespaced_persistent_volume_claim.side_effect = None

        with (
            patch("vm_controller.controller.LIFECYCLE_HMAC_SECRET", LIFECYCLE_SECRET),
            pytest.raises(RuntimeError, match="superseded rootdisk PVC UID"),
        ):
            await controller._do_delete(
                "replacement-disk",
                provision_generation=PROVISION_GENERATION,
                expected_vm_uid="old-vm-uid",
                expected_rootdisk_pvc_uid="old-root-uid",
            )

        controller.k8s_client.delete_namespaced_custom_object.assert_not_called()

    @pytest.mark.asyncio
    async def test_uid_precondition_conflict_never_purges_replacement_disk(
        self, controller
    ):
        job_id = "uid-race"
        controller.k8s_client.get_namespaced_custom_object.return_value = {
            "metadata": {
                "name": f"agent-vm-{job_id}",
                "uid": "old-vm-uid",
                "annotations": {
                    "srw.io/provision-generation": PROVISION_GENERATION,
                },
            }
        }
        controller.k8s_client.delete_namespaced_custom_object.side_effect = (
            _FakeApiException(status=409, body="UID precondition failed")
        )

        with (
            patch("vm_controller.controller.LIFECYCLE_HMAC_SECRET", LIFECYCLE_SECRET),
            pytest.raises(_FakeApiException),
        ):
            await controller._do_delete(
                job_id, provision_generation=PROVISION_GENERATION
            )

        assert not _calls_for(
            controller.k8s_client.delete_namespaced_custom_object, CDI_PLURAL
        )

    @pytest.mark.asyncio
    async def test_create_and_delete_for_same_entity_are_serialized(self, controller):
        create_entered = asyncio.Event()
        release_create = asyncio.Event()
        delete_entered = asyncio.Event()

        async def _create(_config):
            create_entered.set()
            await release_create.wait()
            return {"job_id": "locked", "status": "created"}

        async def _delete(_job_id, **_kwargs):
            delete_entered.set()
            return {"job_id": "locked", "status": "deleted"}

        controller._do_create_serialized = AsyncMock(side_effect=_create)
        controller._do_delete_serialized = AsyncMock(side_effect=_delete)
        create_task = asyncio.create_task(controller._do_create({"job_id": "locked"}))
        await create_entered.wait()
        delete_task = asyncio.create_task(controller._do_delete("locked"))
        await asyncio.sleep(0)
        assert not delete_entered.is_set()

        release_create.set()
        await asyncio.gather(create_task, delete_task)
        assert delete_entered.is_set()


class TestRenderDiskSize:
    """Per-job rootdisk size — ``job_config["disk_size"]`` overrides the
    controller-wide ``VM_DISK_SIZE`` and lands in the DataVolume template."""

    @staticmethod
    def _dv_storage(result):
        return result["spec"]["dataVolumeTemplates"][0]["spec"]["storage"]["resources"][
            "requests"
        ]["storage"]

    def test_render_disk_size_default_when_absent(self, controller):
        with patch("vm_controller.controller.VM_DISK_SIZE", "20Gi"):
            result = controller.render_template({"job_id": "test-id"})
        assert self._dv_storage(result) == "20Gi"

    def test_render_disk_size_from_job_config(self, controller):
        with patch("vm_controller.controller.VM_DISK_SIZE", "20Gi"):
            result = controller.render_template(
                {"job_id": "test-id", "disk_size": "120Gi"}
            )
        assert self._dv_storage(result) == "120Gi"

    def test_render_disk_size_never_below_controller_default(self, controller):
        """A clone target smaller than the golden source fails in CDI, and the
        default is the golden floor by construction — so never shrink."""
        with patch("vm_controller.controller.VM_DISK_SIZE", "20Gi"):
            result = controller.render_template(
                {"job_id": "test-id", "disk_size": "5Gi"}
            )
        assert self._dv_storage(result) == "20Gi"

    def test_render_disk_size_invalid_falls_back(self, controller):
        with patch("vm_controller.controller.VM_DISK_SIZE", "20Gi"):
            result = controller.render_template(
                {"job_id": "test-id", "disk_size": "lots; rm -rf /"}
            )
        assert self._dv_storage(result) == "20Gi"
