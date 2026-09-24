"""Tests for the IDE proxy service and URL generation."""

import asyncio
from contextlib import asynccontextmanager
import gzip
import json
import os
import threading
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from fastapi import HTTPException
from fastapi.responses import RedirectResponse


@asynccontextmanager
async def _raw_http_server(response_parts: list[bytes]):
    """Serve one deliberately low-level response for proxy transport tests."""

    async def handle(reader, writer):
        try:
            await reader.readuntil(b"\r\n\r\n")
            for part in response_parts:
                writer.write(part)
                await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    try:
        yield server.sockets[0].getsockname()[1]
    finally:
        server.close()
        await server.wait_closed()


def _ide_dependencies(db, proxy, **gates):
    """Bind the IDE router's collaborators the way the application does.

    ``postgres_db``, ``ide_proxy_service`` and the auth gates used to be
    ``main`` globals these tests patched; they are ``IdeDependencies`` fields
    now, so an override is passed in rather than patched onto a module.
    """
    from orchestrator.routers.ide import IdeDependencies

    return IdeDependencies(
        store=db,
        ide_sessions=MagicMock(),
        ide_proxy=proxy,
        **gates,
    )


# =============================================================================
# IdeProxyService — pod IP resolution and caching
# =============================================================================


class TestIdeProxyServiceInit:
    """Tests for IdeProxyService initialization."""

    def test_default_state(self):
        from orchestrator.services.ide_proxy import IdeProxyService

        service = IdeProxyService()
        assert service._db is None
        assert service._container_provisioner is None
        assert service._pod_ip_cache == {}
        assert service._cache_ttl == 30.0

    def test_connect_stores_authoritative_dependencies(self):
        from orchestrator.services.ide_proxy import IdeProxyService

        service = IdeProxyService()
        mock_db = MagicMock()
        mock_provisioner = MagicMock()
        service.connect(mock_db, mock_provisioner)
        assert service._db is mock_db
        assert service._container_provisioner is mock_provisioner

    def test_connect_rebuild_clears_coordinate_cache(self):
        from orchestrator.services.ide_proxy import IdeProxyService

        service = IdeProxyService()
        service._pod_ip_cache["job-a"] = (
            "127.0.0.1:38080",
            time.monotonic() + 60,
            "docker",
        )

        service.connect(MagicMock(), MagicMock())

        assert service._pod_ip_cache == {}


class TestResolvePodIp:
    """Tests for IdeProxyService.resolve_pod_ip()."""

    _CONTAINER_ID = "a" * 64

    @classmethod
    def _ide_context(cls, ip: str, *, status: str = "active") -> dict:
        return {
            "ide_session": {
                "status": status,
                "restore_type": "container",
                "pod_ip": ip,
                "container_id": cls._CONTAINER_ID,
            }
        }

    @pytest.fixture
    def service(self):
        from orchestrator.services.ide_proxy import IdeProxyService

        svc = IdeProxyService()
        svc._db = MagicMock()
        svc._db.get_job = AsyncMock(return_value=None)
        svc._db.get_thread = AsyncMock(return_value=None)
        return svc

    @pytest.mark.asyncio
    async def test_cache_hit(self, service):
        """Explicit Docker cache is used only after current DB classification."""
        service._db.get_job = AsyncMock(
            return_value={
                "id": "job-1",
                "status": "processing",
                "context": self._ide_context("10.0.0.1"),
            }
        )
        service._pod_ip_cache["job-1"] = (
            "10.0.0.1",
            time.monotonic() + 60,
            "docker",
        )

        result = await service.resolve_pod_ip("job-1")

        assert result == "10.0.0.1"
        service._db.get_job.assert_awaited_once_with("job-1")

    @pytest.mark.asyncio
    async def test_cache_expired(self, service):
        """Expired cache entry triggers DB lookup."""
        service._pod_ip_cache["job-1"] = (
            "10.0.0.1",
            time.monotonic() - 1,
            "docker",
        )
        service._db.get_job = AsyncMock(
            return_value={
                "id": "job-1",
                "status": "processing",
                "context": self._ide_context("10.0.0.2"),
            }
        )

        result = await service.resolve_pod_ip("job-1")

        assert result == "10.0.0.2"
        service._db.get_job.assert_called_once_with("job-1")

    @pytest.mark.asyncio
    async def test_cache_miss_ide_session(self, service):
        """Resolves pod_ip from ide_session context."""
        service._db.get_job = AsyncMock(
            return_value={
                "id": "job-1",
                "status": "processing",
                "context": self._ide_context("10.0.0.5"),
            }
        )

        result = await service.resolve_pod_ip("job-1")

        assert result == "10.0.0.5"
        # Verify it's cached now
        assert "job-1" in service._pod_ip_cache
        assert service._pod_ip_cache["job-1"][0] == "10.0.0.5"

    @pytest.mark.asyncio
    async def test_cache_miss_workspace_container(self, service):
        """Resolves pod_ip from workspace_container context."""
        service._db.get_job = AsyncMock(
            return_value={
                "id": "job-1",
                "status": "processing",
                "context": {
                    "workspace_container": {
                        "status": "ready",
                        "provisioner": "docker",
                        "pod_ip": "10.0.1.3",
                        "_docker_workspace_attested": True,
                        "_docker_workspace_trust_mode": "attested",
                        "_docker_workspace_lease_id": "lease-a",
                    }
                },
            }
        )

        result = await service.resolve_pod_ip("job-1")
        assert result == "10.0.1.3"

    @pytest.mark.asyncio
    async def test_cache_miss_vm(self, service):
        """VM relay never mistakes the lifecycle-controller Pod for a guest."""
        from orchestrator.services.ide_proxy import IdeProxyUnavailable

        service._vm_provisioner = SimpleNamespace(
            attest_workspace_runtime=AsyncMock(
                return_value=SimpleNamespace(
                    backing_id="k8s-vmi:launcher-a",
                    workspace_generation="generation-a",
                    runtime_incarnation="launcher-a",
                    ssh_host_key_fingerprint="SHA256:pin",
                    host="10.0.2.1",
                    pod_ip="10.0.2.1",
                )
            )
        )
        service._db.get_job = AsyncMock(
            return_value={
                "id": "job-1",
                "status": "processing",
                "context": {
                    "vm": {
                        "status": "ready",
                        "ssh_host": "100.64.0.1",
                        "pod_ip": "10.0.2.1",
                    }
                },
            }
        )

        with pytest.raises(IdeProxyUnavailable) as exc:
            await service.resolve_pod_ip("job-1")

        assert exc.value.code == "vm_ide_transport_unavailable"
        service._vm_provisioner.attest_workspace_runtime.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_cache_miss_vm_fallback_ssh_host(self, service):
        """A raw VM coordinate without signed authority fails closed."""
        from orchestrator.services.ide_proxy import IdeProxyUnavailable

        service._db.get_job = AsyncMock(
            return_value={
                "id": "job-1",
                "status": "processing",
                "context": {"vm": {"status": "ready", "pod_ip": "10.0.2.1"}},
            }
        )

        with pytest.raises(IdeProxyUnavailable) as exc:
            await service.resolve_pod_ip("job-1")
        assert exc.value.code == "vm_ide_transport_unavailable"

    @pytest.mark.asyncio
    async def test_priority_order(self, service):
        """ide_session.pod_ip takes priority over workspace_container and vm."""
        service._db.get_job = AsyncMock(
            return_value={
                "id": "job-1",
                "status": "processing",
                "context": {
                    "ide_session": {
                        "status": "active",
                        "restore_type": "container",
                        "pod_ip": "10.0.0.1",
                        "container_id": self._CONTAINER_ID,
                    },
                    "workspace_container": {
                        "status": "ready",
                        "provisioner": "docker",
                        "pod_ip": "10.0.0.2",
                    },
                    "vm": {"status": "ready", "ssh_host": "10.0.0.3"},
                },
            }
        )

        result = await service.resolve_pod_ip("job-1")
        assert result == "10.0.0.1"

    @pytest.mark.asyncio
    async def test_job_not_found(self, service):
        """Returns None when job doesn't exist."""
        service._db.get_job = AsyncMock(return_value=None)
        service._db.get_thread = AsyncMock(return_value=None)

        result = await service.resolve_pod_ip("nonexistent")
        assert result is None

    @pytest.mark.asyncio
    async def test_no_active_session(self, service):
        """Returns None when no IDE-related context is active."""
        service._db.get_job = AsyncMock(
            return_value={
                "status": "processing",
                "context": {
                    "ide_session": {"status": "expired"},
                    "vm": {"status": "deleted"},
                },
            }
        )

        result = await service.resolve_pod_ip("job-1")
        assert result is None
        assert "job-1" not in service._pod_ip_cache

    @pytest.mark.asyncio
    async def test_no_db(self, service):
        """Returns None when DB is not connected."""
        service._db = None
        result = await service.resolve_pod_ip("job-1")
        assert result is None

    @pytest.mark.asyncio
    async def test_context_as_json_string(self, service):
        """Handles context stored as JSON string (not dict)."""
        service._db.get_job = AsyncMock(
            return_value={
                "id": "job-1",
                "status": "processing",
                "context": json.dumps(
                    {
                        "ide_session": {
                            "status": "active",
                            "restore_type": "container",
                            "pod_ip": "10.0.0.7",
                            "container_id": self._CONTAINER_ID,
                        }
                    }
                ),
            }
        )

        result = await service.resolve_pod_ip("job-1")
        assert result == "10.0.0.7"

    @pytest.mark.asyncio
    async def test_context_empty(self, service):
        """Returns None when context is empty."""
        service._db.get_job = AsyncMock(return_value={"context": {}})

        result = await service.resolve_pod_ip("job-1")
        assert result is None

    @pytest.mark.asyncio
    async def test_context_none(self, service):
        """Returns None when context is None."""
        service._db.get_job = AsyncMock(return_value={"context": None})

        result = await service.resolve_pod_ip("job-1")
        assert result is None

    @pytest.mark.asyncio
    async def test_idle_session_also_resolves(self, service):
        """'idle' session status is also valid for resolution."""
        service._db.get_job = AsyncMock(
            return_value={
                "id": "job-1",
                "status": "processing",
                "context": self._ide_context("10.0.0.8", status="idle"),
            }
        )

        result = await service.resolve_pod_ip("job-1")
        assert result == "10.0.0.8"


class TestKubernetesIdeProxyAuthority:
    """Kubernetes coordinates require one fresh exact-Pod attestation."""

    _JOB_ID = "11111111-1111-4111-8111-111111111111"
    _THREAD_ID = "22222222-2222-4222-8222-222222222222"
    _RUNTIME_A = "33333333-3333-4333-8333-333333333333"
    _RUNTIME_B = "44444444-4444-4444-8444-444444444444"
    _POD_IP = "10.42.2.19"

    @classmethod
    def _runtime(cls, *, scope, entity_id, runtime_uid=None):
        if scope == "ide":
            return {
                "status": "active",
                "restore_type": "k8s_container",
                "pod_ip": cls._POD_IP,
                "pod_name": f"ide-{entity_id[:12]}",
                "namespace": "agent-workspaces",
                "_runtime_incarnation": runtime_uid or cls._RUNTIME_A,
            }
        prefix = "workspace" if entity_id == cls._JOB_ID else "ws-thread"
        return {
            "status": "ready",
            "provisioner": "k8s",
            "pod_ip": cls._POD_IP,
            "pod_name": f"{prefix}-{entity_id[:12]}",
            "namespace": "agent-workspaces",
            "_runtime_incarnation": runtime_uid or cls._RUNTIME_A,
        }

    @classmethod
    def _pod(
        cls,
        *,
        owner_kind,
        entity_id,
        scope,
        runtime_uid=None,
        owner_id=None,
    ):
        if scope == "ide":
            name = f"ide-{entity_id[:12]}"
            component = "ide-session"
        elif owner_kind == "job":
            name = f"workspace-{entity_id[:12]}"
            component = "workspace"
        else:
            name = f"ws-thread-{entity_id[:12]}"
            component = "thread-workspace"
        owner_label = "srw/job-id" if owner_kind == "job" else "srw/thread-id"
        return SimpleNamespace(
            metadata=SimpleNamespace(
                name=name,
                namespace="agent-workspaces",
                uid=runtime_uid or cls._RUNTIME_A,
                deletion_timestamp=None,
                labels={
                    owner_label: owner_id or entity_id,
                    "app": "srw-workspace",
                    "srw/component": component,
                    "srw.io/component": "agent-workspace",
                },
            ),
            status=SimpleNamespace(
                phase="Running",
                pod_ip=cls._POD_IP,
                container_statuses=[SimpleNamespace(ready=True)],
            ),
        )

    @staticmethod
    def _service(*, job=None, thread=None, pod=None):
        from orchestrator.services.ide_proxy import IdeProxyService

        core_api = SimpleNamespace(read_namespaced_pod=MagicMock(return_value=pod))
        provisioner = SimpleNamespace(
            is_available=True,
            _namespace="agent-workspaces",
            _core_api=core_api,
        )
        db = MagicMock()
        db.get_job = AsyncMock(return_value=job)
        db.get_thread = AsyncMock(return_value=thread)
        service = IdeProxyService()
        service.connect(db, provisioner)
        return service, core_api

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("owner_kind", "scope"),
        [
            ("job", "ide"),
            ("job", "workspace_container"),
            ("thread", "workspace_container"),
        ],
    )
    async def test_exact_owner_uid_ip_and_readiness_are_freshly_attested(
        self, owner_kind, scope
    ):
        entity_id = self._JOB_ID if owner_kind == "job" else self._THREAD_ID
        runtime = self._runtime(scope=scope, entity_id=entity_id)
        row = {
            "id": entity_id,
            "status": "processing" if owner_kind == "job" else "active",
            "execution_lane": "pinned",
            "context" if owner_kind == "job" else "metadata": {
                "ide_session" if scope == "ide" else scope: runtime
            },
        }
        service, core_api = self._service(
            job=row if owner_kind == "job" else None,
            thread=row if owner_kind == "thread" else None,
            pod=self._pod(
                owner_kind=owner_kind,
                entity_id=entity_id,
                scope=scope,
            ),
        )

        result = await service.resolve_target(entity_id)

        assert result is not None
        assert result.backend == "k8s"
        assert result.host == self._POD_IP
        assert entity_id not in service._pod_ip_cache
        expected_name = (
            f"ide-{entity_id[:12]}"
            if scope == "ide"
            else (
                f"workspace-{entity_id[:12]}"
                if owner_kind == "job"
                else f"ws-thread-{entity_id[:12]}"
            )
        )
        core_api.read_namespaced_pod.assert_called_once_with(
            name=expected_name,
            namespace="agent-workspaces",
            _request_timeout=(5.0, 10.0),
        )

    @pytest.mark.asyncio
    async def test_k8s_attestation_cancellation_joins_started_api_read(self):
        entity_id = self._JOB_ID
        scope = "workspace_container"
        pod = self._pod(owner_kind="job", entity_id=entity_id, scope=scope)
        entered = threading.Event()
        released = threading.Event()

        def blocking_read(*, name, namespace, _request_timeout):
            assert name == f"workspace-{entity_id[:12]}"
            assert namespace == "agent-workspaces"
            assert _request_timeout == (5.0, 10.0)
            entered.set()
            released.wait(timeout=2)
            return pod

        service, core_api = self._service(
            job={
                "id": entity_id,
                "status": "processing",
                "context": {scope: self._runtime(scope=scope, entity_id=entity_id)},
            },
            pod=pod,
        )
        core_api.read_namespaced_pod.side_effect = blocking_read

        task = asyncio.create_task(service.resolve_target(entity_id))
        assert await asyncio.to_thread(entered.wait, 1)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()

        released.set()
        with pytest.raises(asyncio.CancelledError):
            await task

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("owner_kind", "scope"),
        [
            ("job", "ide"),
            ("job", "workspace_container"),
            ("thread", "workspace_container"),
        ],
    )
    async def test_cached_ip_cannot_reach_foreign_successor_with_same_ip(
        self, owner_kind, scope
    ):
        """Deleted A followed by foreign B at A's IP fails before proxy I/O."""

        entity_id = self._JOB_ID if owner_kind == "job" else self._THREAD_ID
        runtime = self._runtime(scope=scope, entity_id=entity_id)
        row = {
            "id": entity_id,
            "status": "processing" if owner_kind == "job" else "active",
            "execution_lane": "pinned",
            "context" if owner_kind == "job" else "metadata": {
                "ide_session" if scope == "ide" else scope: runtime
            },
        }
        service, core_api = self._service(
            job=row if owner_kind == "job" else None,
            thread=row if owner_kind == "thread" else None,
            pod=self._pod(
                owner_kind=owner_kind,
                entity_id=entity_id,
                scope=scope,
                runtime_uid=self._RUNTIME_B,
                owner_id="foreign-owner",
            ),
        )
        # Model the vulnerable pre-fix cache hit, including the same raw IP.
        service._pod_ip_cache[entity_id] = (  # type: ignore[assignment]
            self._POD_IP,
            time.monotonic() + 60,
        )

        result = await service.resolve_pod_ip(entity_id)

        assert result is None
        assert entity_id not in service._pod_ip_cache
        core_api.read_namespaced_pod.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mutation",
        [
            "uid",
            "ip",
            "owner",
            "component",
            "namespace",
            "terminating",
            "phase",
            "readiness",
        ],
    )
    async def test_each_k8s_authority_dimension_fails_closed(self, mutation):
        entity_id = self._JOB_ID
        scope = "workspace_container"
        runtime = self._runtime(scope=scope, entity_id=entity_id)
        pod = self._pod(owner_kind="job", entity_id=entity_id, scope=scope)
        if mutation == "uid":
            pod.metadata.uid = self._RUNTIME_B
        elif mutation == "ip":
            pod.status.pod_ip = "10.42.2.20"
        elif mutation == "owner":
            pod.metadata.labels["srw/job-id"] = "foreign-owner"
        elif mutation == "component":
            pod.metadata.labels["srw/component"] = "ide-session"
        elif mutation == "namespace":
            pod.metadata.namespace = "foreign-namespace"
        elif mutation == "terminating":
            pod.metadata.deletion_timestamp = object()
        elif mutation == "phase":
            pod.status.phase = "Pending"
        else:
            pod.status.container_statuses[0].ready = False
        service, _ = self._service(
            job={
                "id": entity_id,
                "status": "processing",
                "context": {scope: runtime},
            },
            pod=pod,
        )

        assert await service.resolve_pod_ip(entity_id) is None

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("owner_kind", "owner_status"),
        [
            ("job", "completed"),
            ("job", "failed"),
            ("job", "cancelled"),
            ("job", "unexpected"),
            ("thread", "ended"),
            ("thread", "suspended"),
        ],
    )
    async def test_terminal_owner_refuses_otherwise_ready_exact_pod(
        self, owner_kind, owner_status
    ):
        entity_id = self._JOB_ID if owner_kind == "job" else self._THREAD_ID
        scope = "workspace_container"
        row = {
            "id": entity_id,
            "status": owner_status,
            "context" if owner_kind == "job" else "metadata": {
                scope: self._runtime(scope=scope, entity_id=entity_id)
            },
        }
        service, core_api = self._service(
            job=row if owner_kind == "job" else None,
            thread=row if owner_kind == "thread" else None,
            pod=self._pod(
                owner_kind=owner_kind,
                entity_id=entity_id,
                scope=scope,
            ),
        )

        assert await service.resolve_pod_ip(entity_id) is None
        core_api.read_namespaced_pod.assert_not_called()

    @pytest.mark.asyncio
    async def test_loaded_row_must_identify_the_requested_k8s_owner(self):
        scope = "workspace_container"
        service, core_api = self._service(
            job={
                "id": "different-job",
                "status": "processing",
                "context": {scope: self._runtime(scope=scope, entity_id=self._JOB_ID)},
            },
            pod=self._pod(
                owner_kind="job",
                entity_id=self._JOB_ID,
                scope=scope,
            ),
        )

        assert await service.resolve_pod_ip(self._JOB_ID) is None
        core_api.read_namespaced_pod.assert_not_called()

    @pytest.mark.asyncio
    async def test_transitioning_ide_does_not_fall_back_to_ready_workspace(self):
        row = {
            "id": self._JOB_ID,
            "status": "processing",
            "context": {
                "ide_session": {
                    **self._runtime(scope="ide", entity_id=self._JOB_ID),
                    "status": "creating",
                },
                "workspace_container": self._runtime(
                    scope="workspace_container", entity_id=self._JOB_ID
                ),
            },
        }
        service, core_api = self._service(
            job=row,
            pod=self._pod(
                owner_kind="job",
                entity_id=self._JOB_ID,
                scope="workspace_container",
            ),
        )

        assert await service.resolve_pod_ip(self._JOB_ID) is None
        core_api.read_namespaced_pod.assert_not_called()


class TestVmIdeProxyAuthority:
    """A guest-bound target never downgrades to a launcher HTTP address."""

    _JOB_ID = "77777777-7777-4777-8777-777777777777"
    _POD_IP = "10.42.4.17"

    @staticmethod
    def _attestation(*, generation: str, launcher: str):
        return SimpleNamespace(
            backing_id=f"k8s-vmi:{launcher}",
            workspace_generation=generation,
            runtime_incarnation=launcher,
            ssh_host_key_fingerprint="SHA256:controller-attested",
            host=TestVmIdeProxyAuthority._POD_IP,
            pod_ip=TestVmIdeProxyAuthority._POD_IP,
        )

    @classmethod
    def _job(cls):
        return {
            "id": cls._JOB_ID,
            "status": "processing",
            "context": {
                "vm": {
                    "status": "ready",
                    "pod_ip": cls._POD_IP,
                    "ssh_host": cls._POD_IP,
                }
            },
        }

    @pytest.mark.asyncio
    async def test_launcher_same_ip_is_never_treated_as_code_server(self):
        from orchestrator.services.ide_proxy import (
            IdeProxyService,
            IdeProxyUnavailable,
        )

        db = SimpleNamespace(
            get_job=AsyncMock(return_value=self._job()),
            get_thread=AsyncMock(return_value=None),
        )
        vm = SimpleNamespace(
            attest_workspace_runtime=AsyncMock(
                side_effect=[
                    self._attestation(generation="generation-a", launcher="launcher-a"),
                    self._attestation(generation="generation-b", launcher="launcher-b"),
                ]
            )
        )
        service = IdeProxyService()
        service.connect(db, vm_provisioner=vm)

        with pytest.raises(IdeProxyUnavailable) as exc:
            await service.resolve_target(self._JOB_ID)

        assert exc.value.code == "vm_ide_transport_unavailable"
        vm.attest_workspace_runtime.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_vm_attestation_outage_fails_closed_without_coordinate_cache(self):
        from orchestrator.services.ide_proxy import (
            IdeProxyService,
            IdeProxyUnavailable,
        )

        db = SimpleNamespace(
            get_job=AsyncMock(return_value=self._job()),
            get_thread=AsyncMock(return_value=None),
        )
        vm = SimpleNamespace(
            attest_workspace_runtime=AsyncMock(side_effect=RuntimeError("offline"))
        )
        service = IdeProxyService()
        service.connect(db, vm_provisioner=vm)
        service._pod_ip_cache[self._JOB_ID] = (
            self._POD_IP,
            time.monotonic() + 60,
            "vm",
        )

        with pytest.raises(IdeProxyUnavailable) as exc:
            await service.resolve_target(self._JOB_ID)
        assert exc.value.code == "vm_ide_runtime_unproven"
        vm.attest_workspace_runtime.assert_awaited_once()
        assert self._JOB_ID not in service._pod_ip_cache

    @pytest.mark.asyncio
    async def test_full_attestation_resolves_ssh_endpoint_not_launcher_http(self):
        from orchestrator.services.ide_proxy import IdeProxyService

        proof = SimpleNamespace(
            workspace_generation=str(__import__("uuid").uuid4()),
            vm_uid=str(__import__("uuid").uuid4()),
            vmi_uid=str(__import__("uuid").uuid4()),
            launcher_pod_uid=str(__import__("uuid").uuid4()),
            rootdisk_pvc_uid=str(__import__("uuid").uuid4()),
            ssh_host_key_fingerprint="SHA256:" + "a" * 43,
            host=self._POD_IP, port=22,
        )
        db = SimpleNamespace(
            get_job=AsyncMock(return_value=self._job()),
            get_thread=AsyncMock(return_value=None),
        )
        vm = SimpleNamespace(attest_workspace_runtime=AsyncMock(return_value=proof))
        service = IdeProxyService()
        service.connect(db, vm_provisioner=vm)
        target = await service.resolve_target(self._JOB_ID)
        assert target.backend == "vm"
        assert target.host == self._POD_IP and target.port == 22
        assert target.identity[1:5] == (
            proof.vm_uid, proof.vmi_uid, proof.launcher_pod_uid,
            proof.rootdisk_pvc_uid,
        )
        assert await service.resolve_pod_ip(self._JOB_ID) is None

    @pytest.mark.asyncio
    async def test_http_route_returns_typed_vm_transport_refusal(self):
        from orchestrator.routers.ide import ide_proxy_http
        from orchestrator.services.ide_proxy import IdeProxyUnavailable

        request = MagicMock()
        request.headers = {"accept": "text/html"}
        request.method = "GET"
        request.url.query = ""
        request.client = None
        proxy = SimpleNamespace(
            resolve_target=AsyncMock(
                side_effect=IdeProxyUnavailable(
                    "vm_ide_transport_unavailable",
                    "VM IDE transport requires an exact guest tunnel",
                )
            ),
            evict=MagicMock(),
        )
        db = SimpleNamespace(get_thread=AsyncMock(return_value=None))
        with pytest.raises(HTTPException) as exc:
            await ide_proxy_http(
                request,
                self._JOB_ID,
                "workspace",
                dependencies=_ide_dependencies(
                    db,
                    proxy,
                    require_approved_user=AsyncMock(return_value={"id": "user-a"}),
                    user_can_access_ide_entity=AsyncMock(return_value=True),
                ),
            )

        assert exc.value.status_code == 503
        assert exc.value.detail["code"] == "vm_ide_transport_unavailable"


class TestEvict:
    """Tests for cache eviction."""

    def test_evict_existing_entry(self):
        from orchestrator.services.ide_proxy import IdeProxyService

        service = IdeProxyService()
        service._pod_ip_cache["job-1"] = ("10.0.0.1", time.monotonic() + 60)

        service.evict("job-1")

        assert "job-1" not in service._pod_ip_cache

    def test_evict_nonexistent_entry(self):
        """Evicting a non-cached job_id is a no-op."""
        from orchestrator.services.ide_proxy import IdeProxyService

        service = IdeProxyService()
        service.evict("nonexistent")  # should not raise


# =============================================================================
# _build_code_server_url — URL generation helper
# =============================================================================


class TestBuildCodeServerUrl:
    """Tests for the proxy URL builder in ide_session.py."""

    def test_default_base_url(self):
        """Uses localhost:8085 when IDE_PROXY_BASE_URL is not set."""
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("IDE_PROXY_BASE_URL", None)

            from orchestrator.services.ide_session import _build_code_server_url

            url = _build_code_server_url("abc-123-def")

            assert url == (
                "http://localhost:8085/api/ide/abc-123-def/proxy/"
                "?folder=/home/agent-host/workspace"
            )

    def test_custom_base_url(self):
        """Uses IDE_PROXY_BASE_URL when set."""
        with patch.dict(
            os.environ,
            {"IDE_PROXY_BASE_URL": "https://api.example.com"},
        ):
            from orchestrator.services.ide_session import _build_code_server_url

            url = _build_code_server_url("job-uuid-1234")

            assert url.startswith(
                "https://api.example.com/api/ide/job-uuid-1234/proxy/"
            )
            assert "folder=/home/agent-host/workspace" in url

    def test_custom_folder(self):
        """Supports custom folder path."""
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("IDE_PROXY_BASE_URL", None)

            from orchestrator.services.ide_session import _build_code_server_url

            url = _build_code_server_url("job-1", folder="/opt/workspace")

            assert "folder=/opt/workspace" in url

    def test_url_contains_job_id(self):
        """Job ID is embedded in the URL path for routing."""
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("IDE_PROXY_BASE_URL", None)

            from orchestrator.services.ide_session import _build_code_server_url

            url = _build_code_server_url("unique-job-id-999")

            assert "/api/ide/unique-job-id-999/proxy/" in url


# =============================================================================
# ContainerProvisioner IDE pod methods
# =============================================================================


class TestCreateIdePod:
    """Tests for ContainerProvisioner.create_ide_pod()."""

    @pytest.mark.asyncio
    async def test_not_available(self):
        """Returns None when K8s is not available."""
        from orchestrator.services.container_provisioner import ContainerProvisioner

        provisioner = ContainerProvisioner()
        result = await provisioner.create_ide_pod("job-1")
        assert result is None

    def test_ide_pod_name_format(self):
        """IDE pod uses 'ide-' prefix with truncated job_id."""
        from orchestrator.services.container_provisioner import ContainerProvisioner
        from orchestrator.services.workspace_lifecycle import WorkspaceOwner

        provisioner = ContainerProvisioner()
        manifest = provisioner._build_pod_manifest(
            pod_name="ide-abc123456789",
            owner=WorkspaceOwner.job("abc123456789-full-uuid"),
            image="test:latest",
            cpu="250m",
            memory="512Mi",
            cpu_limit="1000m",
            memory_limit="2Gi",
        )

        assert manifest["metadata"]["name"] == "ide-abc123456789"

    def test_ide_pod_label_override(self):
        """create_ide_pod overrides the component label to 'ide-session'."""
        from orchestrator.services.container_provisioner import ContainerProvisioner
        from orchestrator.services.workspace_lifecycle import WorkspaceOwner

        provisioner = ContainerProvisioner()
        manifest = provisioner._build_pod_manifest(
            pod_name="ide-test123",
            owner=WorkspaceOwner.job("test123-full-uuid"),
            image="test:latest",
            cpu="250m",
            memory="512Mi",
            cpu_limit="1000m",
            memory_limit="2Gi",
        )

        # Default label is 'workspace', create_ide_pod overrides to 'ide-session'
        assert manifest["metadata"]["labels"]["srw/component"] == "workspace"

        # Simulate what create_ide_pod does
        manifest["metadata"]["labels"]["srw/component"] = "ide-session"
        assert manifest["metadata"]["labels"]["srw/component"] == "ide-session"


class TestDeleteIdePod:
    """Tests for ContainerProvisioner.delete_ide_pod()."""

    @pytest.mark.asyncio
    async def test_not_available(self):
        """Returns False when K8s is not available."""
        from orchestrator.services.container_provisioner import ContainerProvisioner

        provisioner = ContainerProvisioner()
        result = await provisioner.delete_ide_pod("job-1")
        assert result is False

    @pytest.mark.asyncio
    async def test_legacy_delete_refuses_without_cleanup_intent_or_process_zero(self):
        """A name and live Pod UID are not terminal deletion authority."""
        from orchestrator.services.container_provisioner import ContainerProvisioner
        from orchestrator.services.workspace_lifecycle import WorkspaceOwner

        provisioner = ContainerProvisioner()
        provisioner._k8s_available = True
        provisioner._namespace = "test-ns"
        mock_api = MagicMock()
        provisioner._core_api = mock_api
        owner = WorkspaceOwner.job("abc123456789")
        runtime_uid = "11111111-1111-4111-8111-111111111111"
        mock_api.read_namespaced_pod.return_value = SimpleNamespace(
            metadata=SimpleNamespace(
                name="ide-abc123456789",
                namespace="test-ns",
                uid=runtime_uid,
                deletion_timestamp=None,
                labels={
                    "app": "srw-workspace",
                    "srw/component": "ide-session",
                    "srw.io/component": "agent-workspace",
                    "srw/job-id": owner.id,
                },
            )
        )
        provisioner._delete_seed_configmap = AsyncMock(return_value=True)

        # Mock asyncio.to_thread to call the function synchronously
        async def fake_to_thread(fn, *args, **kwargs):
            return fn(*args, **kwargs)

        with patch(
            "orchestrator.services.container_provisioner.asyncio.to_thread",
            side_effect=fake_to_thread,
        ):
            result = await provisioner.delete_ide_pod("abc123456789")

        assert result is False
        mock_api.delete_namespaced_pod.assert_not_called()
        provisioner._delete_seed_configmap.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_legacy_delete_404_refuses_without_durable_runtime_receipt(self):
        """API absence alone cannot prove process-zero or seed ownership."""
        from orchestrator.services.container_provisioner import ContainerProvisioner

        provisioner = ContainerProvisioner()
        provisioner._k8s_available = True
        provisioner._namespace = "test-ns"
        mock_api = MagicMock()
        provisioner._core_api = mock_api

        exc = Exception("Not Found")
        exc.status = 404
        mock_api.read_namespaced_pod.side_effect = exc
        provisioner._delete_seed_configmap = AsyncMock(return_value=True)

        async def fake_to_thread(fn, *args, **kwargs):
            return fn(*args, **kwargs)

        with patch(
            "orchestrator.services.container_provisioner.asyncio.to_thread",
            side_effect=fake_to_thread,
        ):
            result = await provisioner.delete_ide_pod("job-1")

        assert result is False
        mock_api.delete_namespaced_pod.assert_not_called()
        provisioner._delete_seed_configmap.assert_not_awaited()


# =============================================================================
# Proxy endpoint header filtering
# =============================================================================


class TestProxyHeaderPolicy:
    """The auth-none boundary uses positive request/response allowlists."""

    def test_allowlists_are_minimal_and_have_no_identity_fields(self):
        from orchestrator.services import ide_proxy_gateway

        request = ide_proxy_gateway._IDE_PROXY_REQUEST_ALLOW_HEADERS
        response = ide_proxy_gateway._IDE_PROXY_RESPONSE_ALLOW_HEADERS
        assert {"accept", "range", "if-none-match"} <= request
        assert {"content-type", "etag"} <= response
        assert "accept-encoding" not in request
        assert "content-length" not in response
        for header in request | response:
            assert header == header.lower()
            assert not header.startswith(("x-", "cf-", "proxy-"))
            assert header not in {
                "authorization",
                "cookie",
                "set-cookie",
                "www-authenticate",
            }


class TestIdeProxySecurityBoundary:
    """Browser credentials never cross into the auth-none upstream."""

    @pytest.mark.asyncio
    async def test_http_strips_request_and_response_credentials(self):
        from orchestrator.routers import ide as ide_routes

        target = SimpleNamespace(
            backend="docker",
            host="10.42.0.7",
            authority="10.42.0.7:38080",
        )
        proxy = SimpleNamespace(
            resolve_target=AsyncMock(return_value=target),
            evict=MagicMock(),
        )
        db = SimpleNamespace(get_thread=AsyncMock(return_value=None))
        upstream = httpx.Response(
            200,
            headers={
                "content-type": "text/plain",
                "set-cookie": "upstream=secret",
                "www-authenticate": "Basic realm=secret",
                "authentication-info": "nextnonce=secret",
                "x-remote-user": "upstream-admin",
                "cf-access-jwt-assertion": "upstream-jwt",
                "x-code-server-secret": "upstream-secret",
                "location": "http://foreign.invalid/steal",
            },
            content=b"ok",
        )
        request_upstream = AsyncMock(return_value=upstream)
        request = MagicMock()
        request.headers = {
            "Cookie": "srw_session=browser-secret",
            "Authorization": "Bearer browser-secret",
            "Proxy-Authorization": "Basic browser-secret",
            "X-Api-Key": "browser-secret",
            "X-Forwarded-User": "forged-admin",
            "X-Remote-User": "forged-admin",
            "CF-Access-Jwt-Assertion": "browser-jwt",
            "Accept": "text/plain",
            "X-Code-Server-Safe": "safe",
        }
        request.method = "GET"
        request.url.query = (
            "folder=%2Fworkspace&access_token=browser-secret&"
            "reconnectionToken=editor-state"
        )
        request.client = SimpleNamespace(host="192.0.2.10")

        with (
            patch.object(ide_routes, "_request_exact_ide_http", request_upstream),
            patch(
                "orchestrator.services.ssh_helpers.orchestrator_can_reach",
                return_value=True,
            ),
        ):
            response = await ide_routes.ide_proxy_http(
                request,
                "job-a",
                "workspace",
                dependencies=_ide_dependencies(
                    db,
                    proxy,
                    require_approved_user=AsyncMock(return_value={"id": "user-a"}),
                    user_can_access_ide_entity=AsyncMock(return_value=True),
                ),
            )

        forwarded = {
            key.lower(): value
            for key, value in request_upstream.await_args.kwargs["headers"].items()
        }
        assert "cookie" not in forwarded
        assert "authorization" not in forwarded
        assert "proxy-authorization" not in forwarded
        assert "x-api-key" not in forwarded
        assert "x-forwarded-user" not in forwarded
        assert "x-remote-user" not in forwarded
        assert "cf-access-jwt-assertion" not in forwarded
        assert "x-code-server-safe" not in forwarded
        assert forwarded["accept"] == "text/plain"
        assert forwarded["accept-encoding"] == "identity"
        assert forwarded["x-forwarded-for"] == "192.0.2.10"
        assert forwarded["x-forwarded-proto"] == "https"
        assert forwarded["host"] == "10.42.0.7:38080"
        assert request_upstream.await_args.kwargs["url"] == (
            "http://10.42.0.7:38080/workspace?"
            "folder=%2Fworkspace&reconnectionToken=editor-state"
        )
        assert "set-cookie" not in response.headers
        assert "www-authenticate" not in response.headers
        assert "authentication-info" not in response.headers
        assert "x-remote-user" not in response.headers
        assert "cf-access-jwt-assertion" not in response.headers
        assert "x-code-server-secret" not in response.headers
        assert "location" not in response.headers
        assert response.headers["content-type"] == "text/plain"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("method", ["POST", "PUT", "PATCH", "DELETE"])
    async def test_mutating_http_is_contained_before_body_or_target_use(self, method):
        from orchestrator.routers import ide as ide_routes

        consumed = False

        async def body():
            nonlocal consumed
            consumed = True
            yield b"must-not-cross"

        request = MagicMock()
        request.headers = {"Cookie": "browser-secret"}
        request.method = method
        request.stream = MagicMock(side_effect=body)
        proxy = SimpleNamespace(resolve_target=AsyncMock(), evict=MagicMock())
        with (
            patch.object(
                ide_routes, "_request_exact_ide_http", AsyncMock()
            ) as upstream,
            pytest.raises(HTTPException) as exc,
        ):
            await ide_routes.ide_proxy_http(
                request,
                "job-a",
                "effect",
                dependencies=_ide_dependencies(
                    # No store: reaching the lifecycle fence at all is a failure.
                    None,
                    proxy,
                    require_approved_user=AsyncMock(return_value={"id": "user-a"}),
                    user_can_access_ide_entity=AsyncMock(return_value=True),
                ),
            )

        assert exc.value.status_code == 503
        assert exc.value.detail["code"] == ("ide_mutation_operation_lease_unavailable")
        assert consumed is False
        request.stream.assert_not_called()
        proxy.resolve_target.assert_not_awaited()
        upstream.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_transport_helper_cannot_bypass_mutation_containment(self):
        from orchestrator.services.ide_proxy import (
            IdeProxyUnavailable,
            ide_proxy_service,
        )
        from orchestrator.services import ide_proxy_gateway

        constructor = MagicMock()
        with (
            patch.object(ide_proxy_gateway.httpx, "AsyncClient", constructor),
            pytest.raises(IdeProxyUnavailable) as exc,
        ):
            await ide_proxy_gateway._request_exact_ide_http(
                target=SimpleNamespace(backend="docker"),
                method="POST",
                url="http://10.42.0.7:38080/effect",
                headers={},
                content=b"must-not-cross",
                ide_proxy=ide_proxy_service,
            )

        assert exc.value.code == "ide_mutation_operation_lease_unavailable"
        constructor.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("backend", ["k8s", "vm"])
    async def test_remote_target_is_contained_before_connect(self, backend):
        from orchestrator.services.ide_proxy import (
            IdeProxyUnavailable,
            ide_proxy_service,
        )
        from orchestrator.services import ide_proxy_gateway

        constructor = MagicMock()
        with (
            patch.object(ide_proxy_gateway.httpx, "AsyncClient", constructor),
            pytest.raises(IdeProxyUnavailable) as exc,
        ):
            await ide_proxy_gateway._request_exact_ide_http(
                target=SimpleNamespace(backend=backend),
                method="GET",
                url="http://10.42.0.7:38080/data",
                headers={},
                content=None,
                ide_proxy=ide_proxy_service,
            )

        assert exc.value.code == "ide_remote_transport_unavailable"
        constructor.assert_not_called()

    @pytest.mark.asyncio
    async def test_credential_bound_remote_target_is_allowed_and_binds_upstream(self):
        """A K8s target may proceed once the destination can refuse a stranger.

        The credential is the recipient binding: code-server compares it to its
        own injected value, so a connection that reached a foreign runtime is
        rejected there. Assert both halves — the refusal is lifted, and the
        request actually carries the credential.
        """
        from orchestrator.services import ide_proxy_gateway

        sent: list[dict] = []

        class Client:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return False

            @asynccontextmanager
            async def stream(self, **kwargs):
                sent.append(kwargs)

                class _Streamed:
                    status_code = 200
                    headers = httpx.Headers({"content-type": "text/html"})

                    async def aiter_raw(self):
                        yield b"ok"

                yield _Streamed()

        proxy = SimpleNamespace(revalidate_target=AsyncMock(return_value=True))
        browser_headers = {"accept": "text/html"}
        with patch.object(
            ide_proxy_gateway.httpx, "AsyncClient", MagicMock(return_value=Client())
        ):
            response = await ide_proxy_gateway._request_exact_ide_http(
                target=SimpleNamespace(backend="k8s", credential="c0ffee"),
                method="GET",
                url="http://10.42.0.7:38080/data",
                headers=browser_headers,
                content=None,
                ide_proxy=proxy,
            )

        assert response.status_code == 200
        assert sent[0]["headers"]["cookie"] == "code-server-session=c0ffee"
        # The caller's dict is per-request and must not accumulate a runtime's
        # credential for the next one.
        assert "cookie" not in browser_headers

    @pytest.mark.asyncio
    async def test_remote_target_without_a_credential_is_still_refused(self):
        """Re-scoping the guard must not become "K8s is fine now"."""
        from orchestrator.services.ide_proxy import (
            IdeProxyUnavailable,
            ide_proxy_service,
        )
        from orchestrator.services import ide_proxy_gateway

        constructor = MagicMock()
        with (
            patch.object(ide_proxy_gateway.httpx, "AsyncClient", constructor),
            pytest.raises(IdeProxyUnavailable) as exc,
        ):
            await ide_proxy_gateway._request_exact_ide_http(
                target=SimpleNamespace(backend="k8s", credential=None),
                method="GET",
                url="http://10.42.0.7:38080/data",
                headers={},
                content=None,
                ide_proxy=ide_proxy_service,
            )

        assert exc.value.code == "ide_remote_transport_unavailable"
        constructor.assert_not_called()

    @pytest.mark.asyncio
    async def test_http_connection_trace_refuses_authority_loss_before_send(self):
        from orchestrator.services import ide_proxy_gateway

        events: list[str] = []

        class Client:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return False

            @asynccontextmanager
            async def stream(self, **kwargs):
                events.append("connected")
                await kwargs["extensions"]["trace"](
                    "connection.connect_tcp.complete", {}
                )
                events.append("request-sent")
                yield httpx.Response(200, content=b"unsafe")

        proxy = SimpleNamespace(revalidate_target=AsyncMock(return_value=False))
        constructor = MagicMock(return_value=Client())
        with (
            patch.object(ide_proxy_gateway.httpx, "AsyncClient", constructor),
            pytest.raises(ide_proxy_gateway._IdeProxyAuthorityLost),
        ):
            await ide_proxy_gateway._request_exact_ide_http(
                target=SimpleNamespace(backend="docker"),
                method="GET",
                url="http://10.42.0.7:38080/data",
                headers={},
                content=None,
                ide_proxy=proxy,
            )

        assert events == ["connected"]
        assert constructor.call_args.kwargs["trust_env"] is False

    @pytest.mark.asyncio
    async def test_real_tcp_connect_sends_zero_bytes_after_failed_attestation(self):
        """Prove the httpcore trace boundary against a real TCP listener."""

        from orchestrator.services import ide_proxy_gateway

        connected = asyncio.Event()
        received: list[bytes] = []

        async def handle(reader, writer):
            connected.set()
            received.append(await reader.read(4096))
            writer.close()
            await writer.wait_closed()

        server = await asyncio.start_server(handle, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        proxy = SimpleNamespace(revalidate_target=AsyncMock(return_value=False))
        try:
            with pytest.raises(ide_proxy_gateway._IdeProxyAuthorityLost):
                await ide_proxy_gateway._request_exact_ide_http(
                    target=SimpleNamespace(backend="docker"),
                    method="GET",
                    url=f"http://127.0.0.1:{port}/data",
                    headers={},
                    content=None,
                    ide_proxy=proxy,
                )
            await asyncio.wait_for(connected.wait(), timeout=1)
            for _ in range(50):
                if received:
                    break
                await asyncio.sleep(0.01)
            assert received == [b""]
        finally:
            server.close()
            await server.wait_closed()

    @pytest.mark.asyncio
    async def test_http_response_is_suppressed_if_authority_changes_after_effect(self):
        from orchestrator.services import ide_proxy_gateway

        class Client:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return False

            @asynccontextmanager
            async def stream(self, **kwargs):
                await kwargs["extensions"]["trace"](
                    "connection.connect_tcp.complete", {}
                )

                class Upstream:
                    status_code = 200
                    headers = httpx.Headers()

                    async def aiter_raw(self):
                        yield b"old-runtime"

                yield Upstream()

        proxy = SimpleNamespace(revalidate_target=AsyncMock(side_effect=[True, False]))
        with (
            patch.object(ide_proxy_gateway.httpx, "AsyncClient", return_value=Client()),
            pytest.raises(ide_proxy_gateway._IdeProxyAuthorityLost),
        ):
            await ide_proxy_gateway._request_exact_ide_http(
                target=SimpleNamespace(backend="docker"),
                method="GET",
                url="http://10.42.0.7:38080/data",
                headers={},
                content=None,
                ide_proxy=proxy,
            )

        assert proxy.revalidate_target.await_count == 2

    @pytest.mark.asyncio
    async def test_compressed_response_remains_raw_with_consistent_headers(self):
        from orchestrator.services import ide_proxy_gateway

        decoded = b"code-server asset" * 128
        encoded = gzip.compress(decoded)
        response = [
            (
                b"HTTP/1.1 200 OK\r\n"
                b"Content-Type: application/javascript\r\n"
                b"Content-Encoding: gzip\r\n"
                + f"Content-Length: {len(encoded)}\r\n".encode()
                + b"Connection: close\r\n\r\n"
            ),
            encoded,
        ]
        proxy = SimpleNamespace(revalidate_target=AsyncMock(return_value=True))
        async with _raw_http_server(response) as port:
            result = await ide_proxy_gateway._request_exact_ide_http(
                target=SimpleNamespace(backend="docker"),
                method="GET",
                url=f"http://127.0.0.1:{port}/asset.js",
                headers={"accept-encoding": "identity"},
                content=None,
                max_response_body_bytes=len(encoded),
                ide_proxy=proxy,
            )

        assert result.body == encoded
        headers = dict(result.headers)
        assert headers["content-encoding"] == "gzip"
        assert int(headers["content-length"]) == len(result.body)
        assert proxy.revalidate_target.await_count == 2

    @pytest.mark.asyncio
    async def test_chunked_response_over_limit_is_rejected_before_return(self):
        from orchestrator.services.ide_proxy import IdeProxyUnavailable
        from orchestrator.services import ide_proxy_gateway

        limit = 64
        first = b"a" * limit
        second = b"b"
        response = [
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n"
            b"Connection: close\r\n\r\n",
            f"{len(first):x}\r\n".encode() + first + b"\r\n",
            f"{len(second):x}\r\n".encode() + second + b"\r\n",
            b"0\r\n\r\n",
        ]
        proxy = SimpleNamespace(revalidate_target=AsyncMock(return_value=True))
        async with _raw_http_server(response) as port:
            with pytest.raises(IdeProxyUnavailable) as exc:
                await ide_proxy_gateway._request_exact_ide_http(
                    target=SimpleNamespace(backend="docker"),
                    method="GET",
                    url=f"http://127.0.0.1:{port}/large",
                    headers={"accept-encoding": "identity"},
                    content=None,
                    max_response_body_bytes=limit,
                    ide_proxy=proxy,
                )

        assert exc.value.code == "ide_response_too_large"

    @pytest.mark.asyncio
    async def test_chunked_response_at_exact_limit_is_accepted(self):
        from orchestrator.services import ide_proxy_gateway

        limit = 64
        body = b"z" * limit
        response = [
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n"
            b"Connection: close\r\n\r\n",
            f"{len(body):x}\r\n".encode() + body + b"\r\n",
            b"0\r\n\r\n",
        ]
        proxy = SimpleNamespace(revalidate_target=AsyncMock(return_value=True))
        async with _raw_http_server(response) as port:
            result = await ide_proxy_gateway._request_exact_ide_http(
                target=SimpleNamespace(backend="docker"),
                method="GET",
                url=f"http://127.0.0.1:{port}/boundary",
                headers={"accept-encoding": "identity"},
                content=None,
                max_response_body_bytes=limit,
                ide_proxy=proxy,
            )

        assert result.body == body
        assert len(result.body) == limit


# =============================================================================
# ide_proxy_http — graceful auth for browser navigations (BFF idle-session UX)
# =============================================================================


class TestIsBrowserNavigation:
    """Unit tests for the _is_browser_navigation request classifier."""

    @staticmethod
    def _req(headers: dict[str, str]):
        req = MagicMock()
        req.headers = headers
        return req

    def test_sec_fetch_mode_navigate(self):
        from orchestrator.services.ide_proxy_gateway import (
            _is_browser_navigation,
        )

        assert _is_browser_navigation(self._req({"sec-fetch-mode": "navigate"})) is True

    def test_sec_fetch_mode_cors_is_not_navigation(self):
        from orchestrator.services.ide_proxy_gateway import (
            _is_browser_navigation,
        )

        # code-server's own asset/XHR sub-requests — must NOT be treated as navs.
        req = self._req({"sec-fetch-mode": "cors", "accept": "application/json"})
        assert _is_browser_navigation(req) is False

    def test_accept_html_fallback(self):
        from orchestrator.services.ide_proxy_gateway import (
            _is_browser_navigation,
        )

        # Older browsers without Sec-Fetch-Mode: fall back to Accept: text/html.
        req = self._req({"accept": "text/html,application/xhtml+xml"})
        assert _is_browser_navigation(req) is True

    def test_no_signal_is_not_navigation(self):
        from orchestrator.services.ide_proxy_gateway import (
            _is_browser_navigation,
        )

        assert _is_browser_navigation(self._req({})) is False


class TestIdeProxyHttpAuthRedirect:
    """ide_proxy_http turns a no-session 401 on a top-level browser navigation
    into a 302 to the cockpit login, while leaving XHR/sub-resource 401s and all
    403s (pending approval / IDE access denied) as raw errors — never looping an
    authenticated-but-unauthorized user through login."""

    @staticmethod
    def _req(headers: dict[str, str]):
        req = MagicMock()
        req.headers = headers
        req.cookies = {}
        return req

    @pytest.mark.asyncio
    async def test_navigation_401_redirects_to_login(self):
        from orchestrator.routers.ide import ide_proxy_http

        resp = await ide_proxy_http(
            self._req({"sec-fetch-mode": "navigate"}),
            "thread-123",
            "",
            dependencies=_ide_dependencies(
                None,
                MagicMock(),
                require_approved_user=AsyncMock(
                    side_effect=HTTPException(
                        status_code=401, detail="Not authenticated"
                    )
                ),
            ),
        )

        assert isinstance(resp, RedirectResponse)
        assert resp.status_code == 302
        assert resp.headers["location"] == "/auth/login?return_to=/"

    @pytest.mark.asyncio
    async def test_xhr_401_is_not_redirected(self):
        from orchestrator.routers.ide import ide_proxy_http

        with pytest.raises(HTTPException) as exc_info:
            await ide_proxy_http(
                self._req({"sec-fetch-mode": "cors", "accept": "application/json"}),
                "thread-123",
                "",
                dependencies=_ide_dependencies(
                    None,
                    MagicMock(),
                    require_approved_user=AsyncMock(
                        side_effect=HTTPException(
                            status_code=401, detail="Not authenticated"
                        )
                    ),
                ),
            )

        assert exc_info.value.status_code == 401

    @pytest.mark.asyncio
    async def test_navigation_403_is_not_redirected(self):
        from orchestrator.routers.ide import ide_proxy_http

        with pytest.raises(HTTPException) as exc_info:
            await ide_proxy_http(
                self._req({"sec-fetch-mode": "navigate"}),
                "thread-123",
                "",
                dependencies=_ide_dependencies(
                    None,
                    MagicMock(),
                    require_approved_user=AsyncMock(
                        side_effect=HTTPException(
                            status_code=403, detail="Account pending approval."
                        )
                    ),
                ),
            )

        assert exc_info.value.status_code == 403


# =============================================================================
# Browser IDE advertisement — one refusal, every consumer
# =============================================================================


class TestBrowserIdeAdvertisement:
    """The status advertisement must agree with what the proxy will serve."""

    ACTIVE = {
        "status": "active",
        "code_server_url": "http://proxy/api/ide/thread-a/proxy/",
        "source": "live_workspace",
        "gitea_url": "https://git.example/srw/thread-a",
    }

    def test_deployment_without_a_root_key_advertises_nothing(self, monkeypatch):
        """No key means no workspace here can bind one — say so once, cheaply."""
        from orchestrator.services.ide_proxy import contain_ide_status

        monkeypatch.delenv("IDE_CREDENTIAL_KEY", raising=False)

        result = contain_ide_status(dict(self.ACTIVE))

        assert result["status"] == "unavailable"
        assert result["code_server_url"] is None
        assert result["code"] == "ide_credential_key_unconfigured"
        assert result["error"]
        # The Git view reads the same workspace and still works; dropping it
        # would take away the only remaining way in.
        assert result["gitea_url"] == "https://git.example/srw/thread-a"
        assert result["source"] == "live_workspace"

    def test_payload_without_a_url_is_returned_untouched(self, monkeypatch):
        from orchestrator.services.ide_proxy import contain_ide_status

        monkeypatch.delenv("IDE_CREDENTIAL_KEY", raising=False)
        payload = {"status": "restoring", "code_server_url": None, "gitea_url": None}

        assert contain_ide_status(payload) is payload

    def test_configured_deployment_defers_to_the_per_workspace_check(self, monkeypatch):
        """The cheap gate must not answer a question only the runtime can."""
        import orchestrator.services.ide_proxy as ide_proxy

        monkeypatch.setenv("IDE_CREDENTIAL_KEY", "k")
        payload = dict(self.ACTIVE)

        assert ide_proxy.contain_ide_status(payload) is payload
        assert ide_proxy.contained_ide_status() is None

    @pytest.mark.asyncio
    async def test_bound_workspace_keeps_its_url(self, monkeypatch):
        import orchestrator.services.ide_proxy as ide_proxy

        monkeypatch.setenv("IDE_CREDENTIAL_KEY", "k")
        target = SimpleNamespace(backend="k8s", credential="c0ffee")
        payload = dict(self.ACTIVE)
        with patch.object(
            ide_proxy.ide_proxy_service,
            "resolve_target",
            AsyncMock(return_value=target),
        ):
            result = await ide_proxy.contain_ide_status_for("thread-a", payload)

        assert result is payload

    @pytest.mark.asyncio
    async def test_workspace_predating_the_credential_is_not_advertised(
        self, monkeypatch
    ):
        """The button must not appear for a Pod the proxy will refuse."""
        import orchestrator.services.ide_proxy as ide_proxy

        monkeypatch.setenv("IDE_CREDENTIAL_KEY", "k")
        target = SimpleNamespace(backend="k8s", credential=None)
        with patch.object(
            ide_proxy.ide_proxy_service,
            "resolve_target",
            AsyncMock(return_value=target),
        ):
            result = await ide_proxy.contain_ide_status_for(
                "thread-a", dict(self.ACTIVE)
            )

        assert result["status"] == "unavailable"
        assert result["code"] == "ide_remote_transport_unavailable"
        assert result["gitea_url"] == "https://git.example/srw/thread-a"

    @pytest.mark.asyncio
    async def test_typed_backend_refusal_is_passed_through(self, monkeypatch):
        import orchestrator.services.ide_proxy as ide_proxy

        monkeypatch.setenv("IDE_CREDENTIAL_KEY", "k")
        with patch.object(
            ide_proxy.ide_proxy_service,
            "resolve_target",
            AsyncMock(
                side_effect=ide_proxy.IdeProxyUnavailable(
                    "vm_ide_transport_unavailable", "VM IDE needs an exact tunnel"
                )
            ),
        ):
            result = await ide_proxy.contain_ide_status_for(
                "thread-a", dict(self.ACTIVE)
            )

        assert result["code"] == "vm_ide_transport_unavailable"


class TestIdeWebSocketTransport:
    """The stream half — code-server cannot render a workbench without it."""

    def _ws(self):
        ws = MagicMock()
        ws.url.query = ""
        ws.accept = AsyncMock()
        ws.close = AsyncMock()
        ws.receive = AsyncMock(return_value={"type": "websocket.disconnect"})
        ws.send_text = AsyncMock()
        ws.send_bytes = AsyncMock()
        return ws

    @asynccontextmanager
    async def _upstream(self):
        class _Upstream:
            async def send(self, _message):
                return None

            def __aiter__(self):
                async def empty():
                    if False:
                        yield None

                return empty()

        yield _Upstream()

    @pytest.mark.asyncio
    async def test_credential_bound_stream_carries_the_cookie(self):
        """The handshake is what a foreign runtime refuses — bind it there."""
        from orchestrator.routers import ide as ide_routes

        target = SimpleNamespace(
            backend="k8s",
            credential="c0ffee",
            host="10.42.0.7",
            port=38080,
            authority="10.42.0.7:38080",
        )
        proxy = MagicMock()
        proxy.resolve_target = AsyncMock(return_value=target)
        ws = self._ws()
        ws.url.query = "token=leak-me&reconnectionToken=keep-me"
        calls = []

        def connect(url, **kwargs):
            calls.append((url, kwargs))
            return self._upstream()

        with (
            patch.object(
                ide_routes,
                "_require_stateless_ide_lifecycle",
                AsyncMock(return_value=None),
            ),
            patch(
                "orchestrator.services.ssh_helpers.orchestrator_can_reach",
                return_value=True,
            ),
            patch("websockets.connect", side_effect=connect),
        ):
            await ide_routes.ide_proxy_ws(
                ws,
                "thread-a",
                "stable/connection",
                dependencies=_ide_dependencies(
                    MagicMock(),
                    proxy,
                    resolve_ws_user=AsyncMock(
                        return_value={"id": "u", "is_approved": True}
                    ),
                    user_can_access_ide_entity=AsyncMock(return_value=True),
                ),
            )

        ws.accept.assert_awaited_once()
        url, kwargs = calls[0]
        assert kwargs["additional_headers"]["Cookie"] == "code-server-session=c0ffee"
        # Browser-auth material is stripped from the upgrade URL exactly as it
        # is on the HTTP path; code-server's own state fields survive.
        assert "token=leak-me" not in url
        assert "reconnectionToken=keep-me" in url

    @pytest.mark.asyncio
    async def test_vm_refusal_closes_instead_of_raising(self):
        from orchestrator.routers import ide as ide_routes
        from orchestrator.services.ide_proxy import IdeProxyUnavailable

        proxy = MagicMock()
        proxy.resolve_target = AsyncMock(
            side_effect=IdeProxyUnavailable(
                "vm_ide_transport_unavailable", "VM IDE needs an exact tunnel"
            )
        )
        ws = self._ws()
        with patch.object(
            ide_routes,
            "_require_stateless_ide_lifecycle",
            AsyncMock(return_value=None),
        ):
            await ide_routes.ide_proxy_ws(
                ws,
                "thread-a",
                "",
                dependencies=_ide_dependencies(
                    MagicMock(),
                    proxy,
                    resolve_ws_user=AsyncMock(
                        return_value={"id": "u", "is_approved": True}
                    ),
                    user_can_access_ide_entity=AsyncMock(return_value=True),
                ),
            )

        ws.accept.assert_not_awaited()
        ws.close.assert_awaited_once_with(
            code=4503, reason="vm_ide_transport_unavailable"
        )


class TestIdeWebSocketLifecycleSupervisor:
    """A stream outlives the proof that opened it; the bound is this check."""

    TARGET = SimpleNamespace(
        backend="k8s", credential="c0ffee", host="10.42.0.7", port=38080
    )

    async def _current(self, gateway, *, resolve):
        with patch.object(
            gateway, "_require_stateless_ide_lifecycle", AsyncMock(return_value=None)
        ):
            return await gateway._ide_ws_runtime_is_current(
                "thread-a",
                self.TARGET,
                store=MagicMock(),
                ide_proxy=MagicMock(resolve_target=resolve),
            )

    @pytest.mark.asyncio
    async def test_same_runtime_keeps_the_stream(self):
        from orchestrator.services import ide_proxy_gateway

        assert await self._current(
            ide_proxy_gateway, resolve=AsyncMock(return_value=self.TARGET)
        )

    @pytest.mark.asyncio
    async def test_status_churn_alone_does_not_drop_a_working_ide(self):
        """A session flipping active/awaiting_user must not close the socket.

        This is why the check is not ``revalidate_target``: that compares the
        owner lifecycle projection too, which moves during ordinary use.
        """
        from orchestrator.services import ide_proxy_gateway

        churned = SimpleNamespace(
            backend="k8s",
            credential="c0ffee",
            host="10.42.0.7",
            port=38080,
            identity=("something", "else"),
        )

        assert await self._current(
            ide_proxy_gateway, resolve=AsyncMock(return_value=churned)
        )

    @pytest.mark.asyncio
    async def test_replacement_runtime_closes_the_stream(self):
        from orchestrator.services import ide_proxy_gateway

        successor = SimpleNamespace(
            backend="k8s", credential="decafbad", host="10.42.0.9", port=38080
        )

        assert not await self._current(
            ide_proxy_gateway, resolve=AsyncMock(return_value=successor)
        )

    @pytest.mark.asyncio
    async def test_ended_owner_closes_the_stream(self):
        from orchestrator.services import ide_proxy_gateway

        assert not await self._current(
            ide_proxy_gateway, resolve=AsyncMock(return_value=None)
        )

    @pytest.mark.asyncio
    async def test_inconclusive_check_closes_the_stream(self):
        """Never hold a stream open because the check itself failed."""
        from orchestrator.services import ide_proxy_gateway

        assert not await self._current(
            ide_proxy_gateway, resolve=AsyncMock(side_effect=RuntimeError("api down"))
        )
