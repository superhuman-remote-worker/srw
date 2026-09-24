"""Wire contracts for the extracted thread IDE-status and upload routes.

R1.B04 lane W. The lane split (pinned vs stateless) and the refusal ordering
inside each branch are what these pin, together with the exact ``detail``
strings the cockpit's composer distinguishes on.
"""

import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import httpx
import pytest
from fastapi import FastAPI, HTTPException

from orchestrator.routers import thread_files as thread_routes
from orchestrator.routers.thread_files import (
    ThreadFilesDependencies,
    get_thread_files_dependencies,
    router,
)
from orchestrator.services import thread_files as operations
from orchestrator.services.thread_uploads import ThreadUploadError, UploadedFile

THREAD = "66666666-6666-4666-8666-666666666666"
USER = {"id": "u-1", "is_approved": True}
UPLOADS = f"/api/persistent/threads/{THREAD}/uploads"


@pytest.fixture
def wire(monkeypatch):
    events: list[str] = []

    async def require_thread_owner(request, store, thread_id):
        events.append("owner")
        if request.headers.get("x-test-user") != USER["id"]:
            raise HTTPException(status_code=401, detail="Authentication required")
        return USER, dict(state.thread)

    def thread_workspace_backend(thread):
        events.append("backend")
        return state.backend

    def require_stateless_workspace(thread):
        events.append("stateless_backend")
        if state.stateless_refusal is not None:
            raise state.stateless_refusal
        return state.stateless_backend

    state = SimpleNamespace(
        thread={"id": THREAD, "execution_lane": "pinned", "metadata": {}},
        fresh=None,
        backend="sandbox",
        stateless_backend="virtual",
        stateless_refusal=None,
        lock_owner=True,
        lock_error=None,
        builds=0,
    )

    @asynccontextmanager
    async def ensure_lock(thread_id, *, wait=False, **_kwargs):
        events.append("lock")
        if state.lock_error is not None:
            raise state.lock_error
        yield state.lock_owner

    # A pinned VM thread's IDE status reads its VM idle state and any open
    # idle operation through PostgresDB.acquire(). No idle operation exists
    # here, so every raw read answers empty.
    sql = SimpleNamespace(
        fetch=AsyncMock(return_value=[]),
        fetchrow=AsyncMock(return_value=None),
    )

    @asynccontextmanager
    async def acquire():
        yield sql

    store = SimpleNamespace(
        get_thread=AsyncMock(
            side_effect=lambda _id: (None if state.fresh is None else dict(state.fresh))
        ),
        stateless_session_workspace_ensure_lock=ensure_lock,
        acquire=acquire,
    )
    holder = SimpleNamespace(
        store=store,
        container_provisioner=SimpleNamespace(
            workspace_pod_authority=AsyncMock(return_value="exact_live")
        ),
        vm_provisioner=SimpleNamespace(name="vm"),
    )

    def factory():
        state.builds += 1
        return ThreadFilesDependencies(
            store=holder.store,
            container_provisioner=holder.container_provisioner,
            vm_provisioner=holder.vm_provisioner,
            thread_workspace_backend=thread_workspace_backend,
            require_stateless_workspace=require_stateless_workspace,
            require_thread_owner=require_thread_owner,
        )

    state.fresh = dict(state.thread)
    app = FastAPI()
    app.state.thread_files_dependencies_factory = factory
    app.include_router(router)
    return SimpleNamespace(app=app, state=state, holder=holder, sql=sql, events=events)


_DEFAULT = object()


async def call(wire, method, path, *, headers=_DEFAULT, **kwargs):
    if headers is _DEFAULT:
        headers = {"x-test-user": USER["id"]}
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=wire.app), base_url="http://files.test"
    ) as client:
        return await client.request(method, path, headers=headers, **kwargs)


@pytest.fixture
def uploads(monkeypatch):
    """Stub the whole ``thread_uploads`` surface the routes lazily import."""

    seen = SimpleNamespace(
        destination=SimpleNamespace(host="10.42.0.7", port=2222),
        is_k8s=False,
        upload_calls=[],
        delete_calls=[],
        removed="report.pdf",
        results=[
            UploadedFile(
                name="a.txt", size=3, mime_type="text/plain", path="uploads/a.txt"
            )
        ],
        resolve_error=None,
    )

    def resolve(thread):
        if seen.resolve_error is not None:
            raise seen.resolve_error
        return seen.destination

    def make_upload(kind):
        async def run(thread, payloads, **kwargs):
            seen.upload_calls.append((kind, payloads, kwargs))
            return seen.results

        return run

    def make_delete(kind):
        async def run(thread, path, **kwargs):
            seen.delete_calls.append((kind, path, kwargs))
            return seen.removed

        return run

    module = "orchestrator.services.thread_uploads."
    monkeypatch.setattr(module + "resolve_thread_upload_destination", resolve)
    monkeypatch.setattr(
        module + "is_kubernetes_thread_upload_destination", lambda _t: seen.is_k8s
    )
    for kind, name in [
        ("plain", "upload_files_to_thread_workspace"),
        ("k8s", "upload_files_to_attested_k8s_workspace"),
        ("vm", "upload_files_to_attested_vm_workspace"),
        ("stateless", "upload_files_to_attested_stateless_workspace"),
    ]:
        monkeypatch.setattr(module + name, make_upload(kind))
    for kind, name in [
        ("plain", "delete_file_from_thread_workspace"),
        ("k8s", "delete_file_from_attested_k8s_workspace"),
        ("vm", "delete_file_from_attested_vm_workspace"),
        ("stateless", "delete_file_from_attested_stateless_workspace"),
    ]:
        monkeypatch.setattr(module + name, make_delete(kind))
    return seen


# =============================================================================
# Thread IDE status
# =============================================================================


class TestThreadIdeStatus:
    PATH = f"/api/persistent/threads/{THREAD}/ide"

    @pytest.fixture(autouse=True)
    def _passthrough_containment(self, monkeypatch):
        async def contain(_thread_id, payload):
            return payload

        monkeypatch.setattr(thread_routes, "contain_ide_status_for", contain)

    @pytest.mark.asyncio
    async def test_the_owner_gate_runs_first(self, wire):
        response = await call(wire, "GET", self.PATH, headers={})

        assert response.status_code == 401
        assert wire.events == ["owner"]

    @pytest.mark.asyncio
    async def test_a_vm_workspace_wins_over_a_container(self, wire, monkeypatch):
        """A pinned VM thread is answered by the lease-gated VM branch: a
        status read without ``lease_id`` offers no code-server URL (the owner
        must POST for a bounded access lease first) and never falls back to
        the container's live URL."""
        monkeypatch.setenv("IDE_PROXY_BASE_URL", "https://srw.test")
        wire.state.thread = dict(
            wire.state.thread,
            metadata={
                "vm": {"status": "ready", "ssh_host": "10.0.0.3"},
                "workspace_container": {"status": "ready", "pod_ip": "10.42.0.7"},
            },
        )

        response = await call(wire, "GET", self.PATH)

        assert response.status_code == 200
        assert response.json() == {
            "workspace_lifecycle": None,
            "status": "ready",
            "code_server_url": None,
            "gitea_url": None,
        }

    @pytest.mark.asyncio
    async def test_a_ready_container_reports_live_workspace(self, wire, monkeypatch):
        monkeypatch.setenv("IDE_PROXY_BASE_URL", "https://srw.test")
        wire.state.thread = dict(
            wire.state.thread,
            metadata={
                "workspace_container": {"status": "ready", "pod_ip": "10.42.0.7"}
            },
        )

        response = await call(wire, "GET", self.PATH)

        assert response.json()["source"] == "live_workspace"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "vm,status",
        [
            ({"status": "ready"}, "ready"),
            ({"status": "failed", "ssh_host": "10.0.0.3"}, "unavailable"),
        ],
    )
    async def test_a_vm_status_read_never_uses_the_metadata_address(
        self, wire, vm, status
    ):
        """The guest endpoint is attested fresh when a lease starts the IDE,
        so a recorded address neither makes a VM usable nor its absence
        unusable: only the VM's own readiness decides, and no lease-less read
        carries a URL."""
        wire.state.thread = dict(wire.state.thread, metadata={"vm": vm})

        response = await call(wire, "GET", self.PATH)

        assert response.json() == {
            "workspace_lifecycle": None,
            "status": status,
            "code_server_url": None,
            "gitea_url": None,
        }

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", ["pending", "provisioning"])
    async def test_a_pinned_vm_being_provisioned_reports_restoring_without_url(
        self, wire, status
    ):
        vm = {"status": status, "ssh_host": "10.0.0.3"}
        wire.state.thread = dict(
            wire.state.thread,
            metadata={"vm": vm},
        )
        wire.sql.fetch.return_value = [
            {
                "id": UUID(THREAD),
                "status": "created",
                "execution_lane": "pinned",
                "metadata": {"vm": vm},
                "workspace_idle_episode": None,
                "workspace_idle_revision": 0,
                "idle_phase": None,
                "idle_episode_id": None,
                "idle_reason": None,
                "idle_retry_after": None,
            }
        ]

        response = await call(wire, "GET", self.PATH)

        assert response.status_code == 200
        assert response.json() == {
            "workspace_lifecycle": None,
            "status": "restoring",
            "code_server_url": None,
            "gitea_url": None,
        }

    @pytest.mark.asyncio
    async def test_an_open_idle_operation_keeps_restoring_precedence(self, wire):
        wire.state.thread = dict(
            wire.state.thread,
            metadata={"vm": {"status": "failed"}},
        )
        wire.sql.fetchrow.return_value = {"id": UUID(THREAD)}

        response = await call(wire, "GET", self.PATH)

        assert response.json() == {
            "workspace_lifecycle": None,
            "status": "restoring",
            "code_server_url": None,
            "gitea_url": None,
        }

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", ["provisioning", "pending"])
    async def test_a_provisioning_workspace_reports_restoring(self, wire, status):
        wire.state.thread = dict(
            wire.state.thread,
            metadata={"workspace_container": {"status": status}},
        )

        response = await call(wire, "GET", self.PATH)

        assert response.json() == {
            "status": "restoring",
            "code_server_url": None,
            "gitea_url": None,
        }

    @pytest.mark.asyncio
    async def test_a_json_string_metadata_row_is_parsed(self, wire, monkeypatch):
        monkeypatch.setenv("GITEA_URL", "https://git.example/")
        monkeypatch.setenv("GITEA_ADMIN_USER", "srw")
        wire.state.thread = dict(
            wire.state.thread,
            metadata=json.dumps({"workspace_container": {"repo_name": "thread-repo"}}),
        )

        response = await call(wire, "GET", self.PATH)

        assert response.json()["gitea_url"] == "https://git.example/srw/thread-repo"

    @pytest.mark.asyncio
    async def test_corrupt_metadata_degrades_to_unavailable(self, wire):
        wire.state.thread = dict(wire.state.thread, metadata="{not json")

        response = await call(wire, "GET", self.PATH)

        assert response.json() == {
            "status": "unavailable",
            "code_server_url": None,
            "gitea_url": None,
        }

    @pytest.mark.asyncio
    async def test_an_unvalidatable_repo_name_is_never_echoed_into_a_link(
        self, wire, monkeypatch
    ):
        monkeypatch.setenv("GITEA_URL", "https://git.example")
        wire.state.thread = dict(
            wire.state.thread,
            metadata={"workspace_container": {"repo_name": "../../etc/passwd"}},
        )

        response = await call(wire, "GET", self.PATH)

        assert response.json()["gitea_url"] is None


# =============================================================================
# Uploads
# =============================================================================


class TestUploadFilesToThread:
    @pytest.mark.asyncio
    async def test_the_owner_gate_runs_before_the_file_count_check(self, wire, uploads):
        response = await call(
            wire,
            "POST",
            UPLOADS,
            headers={},
            files=[("files", ("a.txt", b"abc", "text/plain"))],
        )

        assert response.status_code == 401
        assert wire.events == ["owner"]

    @pytest.mark.asyncio
    async def test_too_many_files_is_a_400(self, wire, uploads):
        files = [("files", (f"f{i}.txt", b"x", "text/plain")) for i in range(21)]

        response = await call(wire, "POST", UPLOADS, files=files)

        assert response.status_code == 400
        assert response.json()["detail"] == "Maximum 20 files per request"

    @pytest.mark.asyncio
    async def test_a_missing_thread_row_is_a_404(self, wire, uploads):
        wire.state.fresh = None

        response = await call(
            wire, "POST", UPLOADS, files=[("files", ("a.txt", b"abc", "text/plain"))]
        )

        assert response.status_code == 404
        assert response.json()["detail"] == "Thread not found"

    @pytest.mark.asyncio
    async def test_the_plain_pinned_path_returns_the_upload_envelope(
        self, wire, uploads
    ):
        response = await call(
            wire, "POST", UPLOADS, files=[("files", ("a.txt", b"abc", "text/plain"))]
        )

        assert response.status_code == 200
        assert response.json() == {
            "thread_id": THREAD,
            "files": [
                {
                    "name": "a.txt",
                    "size": 3,
                    "mime_type": "text/plain",
                    "path": "uploads/a.txt",
                }
            ],
        }
        kind, payloads, _kwargs = uploads.upload_calls[0]
        assert kind == "plain"
        assert payloads == [("a.txt", b"abc", "text/plain")]

    @pytest.mark.asyncio
    async def test_the_vm_path_passes_the_injected_provisioner_and_backend_reader(
        self, wire, uploads, monkeypatch
    ):
        wire.state.backend = "vm"
        captured = {}

        class _Lease:
            identity = SimpleNamespace(
                workspace_generation="gen",
                launcher_pod_uid="uid",
                ssh_host_key_fingerprint="SHA256:x",
            )

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_a):
                return False

            async def revalidate(self):
                return object()

        async def prepare(thread_id, thread, destination, **kwargs):
            captured.update(kwargs, thread_id=thread_id)
            return _Lease()

        monkeypatch.setattr(
            thread_routes, "_prepare_pinned_vm_thread_operation", prepare
        )

        response = await call(
            wire, "POST", UPLOADS, files=[("files", ("a.txt", b"abc", "text/plain"))]
        )

        assert response.status_code == 200
        assert captured["operation_kind"] == "thread_upload"
        assert captured["store"] is wire.holder.store
        assert captured["vm_provisioner"] is wire.holder.vm_provisioner
        assert captured["thread_workspace_backend"]({}) == "vm"
        assert uploads.upload_calls[0][0] == "vm"

    @pytest.mark.asyncio
    async def test_the_k8s_path_passes_the_injected_store_and_provisioner(
        self, wire, uploads, monkeypatch
    ):
        uploads.is_k8s = True
        captured = {}

        async def prepare(thread_id, thread, destination, **kwargs):
            captured.update(kwargs)
            return "gen", "inc", "SHA256:f", AsyncMock(return_value="exact_live")

        monkeypatch.setattr(thread_routes, "_prepare_pinned_k8s_thread_upload", prepare)

        response = await call(
            wire, "POST", UPLOADS, files=[("files", ("a.txt", b"abc", "text/plain"))]
        )

        assert response.status_code == 200
        assert captured["store"] is wire.holder.store
        assert captured["container_provisioner"] is wire.holder.container_provisioner
        kind, _payloads, kwargs = uploads.upload_calls[0]
        assert kind == "k8s"
        assert kwargs["expected_workspace_generation"] == "gen"
        assert kwargs["expected_runtime_incarnation"] == "inc"
        assert kwargs["expected_host_key_fingerprint"] == "SHA256:f"

    @pytest.mark.asyncio
    async def test_a_thread_upload_error_keeps_its_status_and_detail(
        self, wire, uploads
    ):
        uploads.resolve_error = ThreadUploadError(409, "Workspace is not ready")

        response = await call(
            wire, "POST", UPLOADS, files=[("files", ("a.txt", b"abc", "text/plain"))]
        )

        assert response.status_code == 409
        assert response.json()["detail"] == "Workspace is not ready"


class TestStatelessUploadLane:
    @pytest.fixture(autouse=True)
    def _stateless(self, wire):
        wire.state.thread = dict(
            wire.state.thread, execution_lane="stateless", status="active"
        )
        wire.state.fresh = dict(wire.state.thread)
        return wire

    @pytest.mark.asyncio
    async def test_an_unowned_lock_is_a_503(self, wire, uploads):
        wire.state.lock_owner = False

        response = await call(
            wire, "POST", UPLOADS, files=[("files", ("a.txt", b"abc", "text/plain"))]
        )

        assert response.status_code == 503
        assert (
            response.json()["detail"]
            == "Stateless workspace lifecycle lock unavailable"
        )

    @pytest.mark.asyncio
    async def test_a_lock_timeout_is_a_503(self, wire, uploads):
        wire.state.lock_error = TimeoutError()

        response = await call(
            wire, "POST", UPLOADS, files=[("files", ("a.txt", b"abc", "text/plain"))]
        )

        assert response.status_code == 503
        assert (
            response.json()["detail"] == "Stateless workspace lifecycle lock timed out"
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", ["ended", "failed", "retired"])
    async def test_a_non_live_session_refuses_uploads(self, wire, uploads, status):
        wire.state.fresh = dict(wire.state.fresh, status=status)

        response = await call(
            wire, "POST", UPLOADS, files=[("files", ("a.txt", b"abc", "text/plain"))]
        )

        assert response.status_code == 409
        assert response.json()["detail"] == "Stateless session is not accepting uploads"

    @pytest.mark.asyncio
    async def test_a_stop_marker_refuses_uploads(self, wire, uploads, monkeypatch):
        monkeypatch.setattr(
            thread_routes, "stateless_stop_markers", lambda _m: {"stopped": True}
        )

        response = await call(
            wire, "POST", UPLOADS, files=[("files", ("a.txt", b"abc", "text/plain"))]
        )

        assert response.status_code == 409

    @pytest.mark.asyncio
    async def test_unreadable_markers_fail_closed(self, wire, uploads, monkeypatch):
        monkeypatch.setattr(
            thread_routes,
            "stateless_stop_markers",
            MagicMock(side_effect=RuntimeError("bad json")),
        )

        response = await call(
            wire, "POST", UPLOADS, files=[("files", ("a.txt", b"abc", "text/plain"))]
        )

        assert response.status_code == 409

    @pytest.mark.asyncio
    async def test_the_body_is_materialized_inside_the_lock(self, wire, uploads):
        response = await call(
            wire, "POST", UPLOADS, files=[("files", ("a.txt", b"abc", "text/plain"))]
        )

        assert response.status_code == 200
        assert wire.events.index("lock") < wire.events.index("stateless_backend")
        assert uploads.upload_calls[0][0] == "plain"

    @pytest.mark.asyncio
    async def test_a_sandbox_backend_uses_the_attested_writer_and_the_injected_probe(
        self, wire, uploads
    ):
        wire.state.stateless_backend = "sandbox"
        wire.state.fresh = dict(
            wire.state.fresh,
            metadata={
                "workspace_container": {"_runtime_incarnation": "inc"},
                "_workspace_binding": {
                    "generation": "gen",
                    "ssh_host_key_fingerprint": "SHA256:f",
                },
            },
        )

        response = await call(
            wire, "POST", UPLOADS, files=[("files", ("a.txt", b"abc", "text/plain"))]
        )

        assert response.status_code == 200
        kind, _payloads, kwargs = uploads.upload_calls[0]
        assert kind == "stateless"
        assert kwargs["expected_workspace_generation"] == "gen"
        assert kwargs["expected_runtime_incarnation"] == "inc"
        assert kwargs["expected_host_key_fingerprint"] == "SHA256:f"
        assert await kwargs["authority_probe"]() == "exact_live"
        assert (
            wire.holder.container_provisioner.workspace_pod_authority.await_args.kwargs
            == {"expected_runtime_incarnation": "inc"}
        )

    @pytest.mark.asyncio
    async def test_a_workspace_refusal_propagates(self, wire, uploads):
        wire.state.stateless_refusal = HTTPException(
            status_code=409, detail="Session workspace is unavailable"
        )

        response = await call(
            wire, "POST", UPLOADS, files=[("files", ("a.txt", b"abc", "text/plain"))]
        )

        assert response.status_code == 409
        assert response.json()["detail"] == "Session workspace is unavailable"


# =============================================================================
# Upload deletes
# =============================================================================


class TestDeleteThreadUpload:
    PATH = f"{UPLOADS}/report.pdf"

    @pytest.mark.asyncio
    async def test_the_owner_gate_runs_first(self, wire, uploads):
        response = await call(wire, "DELETE", self.PATH, headers={})

        assert response.status_code == 401
        assert wire.events == ["owner"]

    @pytest.mark.asyncio
    async def test_a_removed_file_echoes_the_normalized_path(self, wire, uploads):
        uploads.removed = "bundle/a.txt"

        response = await call(wire, "DELETE", f"{UPLOADS}/bundle/sub/../a.txt")

        assert response.status_code == 200
        assert response.json() == {
            "thread_id": THREAD,
            "path": "uploads/bundle/a.txt",
            "deleted": True,
        }

    @pytest.mark.asyncio
    async def test_no_such_upload_is_a_404(self, wire, uploads):
        uploads.removed = None

        response = await call(wire, "DELETE", self.PATH)

        assert response.status_code == 404
        assert response.json()["detail"] == "Upload not found"

    @pytest.mark.asyncio
    async def test_the_vm_path_claims_a_delete_operation(
        self, wire, uploads, monkeypatch
    ):
        wire.state.backend = "vm"
        captured = {}

        class _Lease:
            identity = SimpleNamespace(
                workspace_generation="gen",
                launcher_pod_uid="uid",
                ssh_host_key_fingerprint="SHA256:x",
            )

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_a):
                return False

            async def revalidate(self):
                return object()

        async def prepare(thread_id, thread, destination, **kwargs):
            captured.update(kwargs)
            return _Lease()

        monkeypatch.setattr(
            thread_routes, "_prepare_pinned_vm_thread_operation", prepare
        )

        response = await call(wire, "DELETE", self.PATH)

        assert response.status_code == 200
        assert captured["operation_kind"] == "thread_delete"
        assert uploads.delete_calls[0][0] == "vm"

    @pytest.mark.asyncio
    async def test_a_stateless_refusal_uses_the_delete_wording(self, wire, uploads):
        wire.state.thread = dict(
            wire.state.thread, execution_lane="stateless", status="ended"
        )
        wire.state.fresh = dict(wire.state.thread)

        response = await call(wire, "DELETE", self.PATH)

        assert response.status_code == 409
        assert (
            response.json()["detail"]
            == "Stateless session is not accepting upload deletes"
        )

    @pytest.mark.asyncio
    async def test_a_thread_upload_error_keeps_its_status_and_detail(
        self, wire, uploads
    ):
        uploads.resolve_error = ThreadUploadError(400, "Path escapes uploads/")

        response = await call(wire, "DELETE", self.PATH)

        assert response.status_code == 400
        assert response.json()["detail"] == "Path escapes uploads/"


# =============================================================================
# Service helpers
# =============================================================================


class TestRequiredThreadUploadUuid:
    def test_a_valid_uuid_is_normalized(self):
        assert (
            operations._required_thread_upload_uuid(
                "66666666-6666-4666-8666-666666666666", label="generation"
            )
            == "66666666-6666-4666-8666-666666666666"
        )

    @pytest.mark.parametrize("value", [None, "", "nope", 7, object()])
    def test_an_unparseable_value_is_a_409(self, value):
        with pytest.raises(ThreadUploadError) as exc:
            operations._required_thread_upload_uuid(value, label="generation")

        assert exc.value.status_code == 409
        assert exc.value.detail == "Pinned workspace generation is unavailable"

    def test_the_nil_uuid_is_refused(self):
        with pytest.raises(ThreadUploadError) as exc:
            operations._required_thread_upload_uuid(
                "00000000-0000-0000-0000-000000000000", label="generation"
            )

        assert exc.value.status_code == 409


class TestPreparePinnedVmThreadOperation:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "lane,backend", [("stateless", "vm"), ("pinned", "sandbox")]
    )
    async def test_a_non_pinned_vm_thread_is_refused(self, lane, backend):
        with pytest.raises(ThreadUploadError) as exc:
            await operations._prepare_pinned_vm_thread_operation(
                THREAD,
                {"execution_lane": lane},
                object(),
                operation_kind="thread_upload",
                store=SimpleNamespace(),
                vm_provisioner=SimpleNamespace(),
                thread_workspace_backend=lambda _t: backend,
            )

        assert exc.value.status_code == 409
        assert exc.value.detail == "Pinned VM workspace authority is unavailable"

    @pytest.mark.asyncio
    async def test_a_moved_endpoint_settles_the_claim_as_replaced(self, monkeypatch):
        from orchestrator.services.thread_uploads import _SshTarget

        destination = _SshTarget(
            host="10.0.0.9",
            port=2222,
            username="agent-host",
            key_path="/k",
            workspace_path="/w",
            host_key_fingerprint="SHA256:" + "a" * 43,
        )
        lease = SimpleNamespace(
            identity=SimpleNamespace(ssh_host="10.0.0.1", ssh_port=2222),
            receipt={"id": "r-1", "claim_token": 4},
            claimant="c-1",
        )
        settle = AsyncMock()
        monkeypatch.setattr(
            "orchestrator.services.vm_remote_operation.claim_vm_remote_operation",
            AsyncMock(return_value=lease),
        )

        with pytest.raises(ThreadUploadError) as exc:
            await operations._prepare_pinned_vm_thread_operation(
                THREAD,
                {"execution_lane": "pinned"},
                destination,
                operation_kind="thread_upload",
                store=SimpleNamespace(settle_vm_remote_operation=settle),
                vm_provisioner=SimpleNamespace(),
                thread_workspace_backend=lambda _t: "vm",
            )

        assert exc.value.status_code == 409
        assert exc.value.detail == "VM endpoint changed before operation"
        assert settle.await_args.args == ("r-1",)
        assert settle.await_args.kwargs == {
            "claim_token": 4,
            "claimant": "c-1",
            "result_kind": "replaced",
        }

    @pytest.mark.asyncio
    async def test_an_unverifiable_runtime_is_a_503(self, monkeypatch):
        from orchestrator.services.vm_remote_operation import (
            VMRemoteOperationUnavailable,
        )
        from orchestrator.services.thread_uploads import _SshTarget

        monkeypatch.setattr(
            "orchestrator.services.vm_remote_operation.claim_vm_remote_operation",
            AsyncMock(side_effect=VMRemoteOperationUnavailable("nope")),
        )

        with pytest.raises(ThreadUploadError) as exc:
            await operations._prepare_pinned_vm_thread_operation(
                THREAD,
                {"execution_lane": "pinned"},
                _SshTarget(
                    host="h",
                    port=1,
                    username="u",
                    key_path="/k",
                    workspace_path="/w",
                    host_key_fingerprint="SHA256:" + "a" * 43,
                ),
                operation_kind="thread_upload",
                store=SimpleNamespace(),
                vm_provisioner=SimpleNamespace(),
                thread_workspace_backend=lambda _t: "vm",
            )

        assert exc.value.status_code == 503
        assert (
            exc.value.detail == "VM workspace runtime authority could not be verified"
        )


class TestPreparePinnedK8sThreadUpload:
    def _thread(self, **overrides):
        thread = {
            "id": THREAD,
            "execution_lane": "pinned",
            "status": "active",
            "runtime_retirement_token": None,
            "agent_id": "agent-1",
            "runtime_generation": "11111111-1111-4111-8111-111111111111",
            "metadata": {
                "workspace_container": {
                    "status": "ready",
                    "provisioner": "k8s",
                    "pod_ip": "10.42.0.7",
                    "_canvas_workspace_generation": (
                        "22222222-2222-4222-8222-222222222222"
                    ),
                    "_runtime_incarnation": "33333333-3333-4333-8333-333333333333",
                },
                "_workspace_binding": {
                    "kind": "remote",
                    "backing_id": "k8s-pod:abc",
                    "generation": "22222222-2222-4222-8222-222222222222",
                    "ssh_host_key_fingerprint": "SHA256:fingerprint",
                },
            },
        }
        thread.update(overrides)
        return thread

    @pytest.mark.asyncio
    async def test_a_thread_that_fails_the_snapshot_is_refused(self, monkeypatch):
        monkeypatch.setattr(
            "orchestrator.services.thread_uploads."
            "is_kubernetes_thread_upload_destination",
            lambda _t: False,
        )

        with pytest.raises(ThreadUploadError) as exc:
            await operations._prepare_pinned_k8s_thread_upload(
                THREAD,
                self._thread(),
                object(),
                store=SimpleNamespace(),
                container_provisioner=SimpleNamespace(),
            )

        assert exc.value.status_code == 409
        assert (
            exc.value.detail == "Pinned Kubernetes workspace authority is unavailable"
        )

    @pytest.mark.asyncio
    async def test_a_missing_session_binding_is_refused(self, monkeypatch):
        from orchestrator.services.thread_uploads import _SshTarget

        destination = _SshTarget(
            host="10.42.0.7",
            port=2222,
            username="agent-host",
            key_path="/k",
            workspace_path="/w",
            host_key_fingerprint="SHA256:" + "a" * 43,
        )
        monkeypatch.setattr(
            "orchestrator.services.thread_uploads."
            "is_kubernetes_thread_upload_destination",
            lambda _t: True,
        )
        monkeypatch.setattr(
            "orchestrator.services.thread_uploads.resolve_thread_upload_destination",
            lambda _t: destination,
        )
        store = SimpleNamespace(get_pinned_session_binding=AsyncMock(return_value=None))

        with pytest.raises(ThreadUploadError) as exc:
            await operations._prepare_pinned_k8s_thread_upload(
                THREAD,
                self._thread(),
                destination,
                store=store,
                container_provisioner=SimpleNamespace(),
            )

        assert exc.value.status_code == 409
        assert exc.value.detail == "Pinned session agent authority is unavailable"

    @pytest.mark.asyncio
    async def test_the_probe_uses_the_injected_store_and_provisioner(self, monkeypatch):
        from orchestrator.services.thread_uploads import _SshTarget

        destination = _SshTarget(
            host="10.42.0.7",
            port=2222,
            username="agent-host",
            key_path="/k",
            workspace_path="/w",
            host_key_fingerprint="SHA256:" + "a" * 43,
        )
        thread = self._thread()
        monkeypatch.setattr(
            "orchestrator.services.thread_uploads."
            "is_kubernetes_thread_upload_destination",
            lambda _t: True,
        )
        monkeypatch.setattr(
            "orchestrator.services.thread_uploads.resolve_thread_upload_destination",
            lambda _t: destination,
        )
        binding = SimpleNamespace(
            agent_id="agent-1", agent_status="ready", target_key="tk-1"
        )
        store = SimpleNamespace(
            get_pinned_session_binding=AsyncMock(return_value=binding),
            get_thread=AsyncMock(return_value=thread),
        )
        provisioner = SimpleNamespace(
            attest_workspace_runtime=AsyncMock(
                return_value=SimpleNamespace(
                    runtime_incarnation="33333333-3333-4333-8333-333333333333",
                    workspace_generation="22222222-2222-4222-8222-222222222222",
                    backing_id="k8s-pod:abc",
                    ssh_host_key_fingerprint="SHA256:fingerprint",
                    pod_ip="10.42.0.7",
                    host="10.42.0.7",
                    port=2222,
                )
            )
        )

        (
            generation,
            incarnation,
            fingerprint,
            probe,
        ) = await operations._prepare_pinned_k8s_thread_upload(
            THREAD,
            thread,
            destination,
            store=store,
            container_provisioner=provisioner,
        )

        assert generation == "22222222-2222-4222-8222-222222222222"
        assert incarnation == "33333333-3333-4333-8333-333333333333"
        assert fingerprint == "SHA256:fingerprint"
        assert await probe() == "exact_live"

        store.get_thread = AsyncMock(side_effect=RuntimeError("db down"))
        assert await probe() == "unknown"

        store.get_thread = AsyncMock(return_value=self._thread(status="ended"))
        assert await probe() == "replacement"


# =============================================================================
# Dependency resolution
# =============================================================================


class TestDependencyResolution:
    @pytest.mark.asyncio
    async def test_each_request_rebuilds_and_observes_a_rebound_singleton(
        self, wire, uploads
    ):
        await call(wire, "GET", f"/api/persistent/threads/{THREAD}/ide")
        first = wire.state.builds

        await call(wire, "GET", f"/api/persistent/threads/{THREAD}/ide")

        assert wire.state.builds > first

    def test_the_provider_reads_the_per_app_factory(self, wire):
        first = get_thread_files_dependencies(SimpleNamespace(app=wire.app))
        second = get_thread_files_dependencies(SimpleNamespace(app=wire.app))

        assert first is not second
        assert first.store is second.store
