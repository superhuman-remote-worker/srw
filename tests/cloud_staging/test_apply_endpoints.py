"""Apply/reject endpoints — protected cloud mode Slice C (Task 10).

The engine itself (``apply_staged_diff``/``reject_staged_diff``) is
exercised in ``test_apply.py``; here it's patched out so these tests focus
on endpoint-level concerns: epoch parsed from the request body and threaded
through to the engine call, ``StagedApplyError`` -> ``HTTPException``
passthrough, the partial-write 502 mapping, and owner-auth propagation.
Also covers ``thread_cloud_diff._reset_thread_overlay`` directly (the
agent-URL resolution helper), since it has no other dedicated test.

Follows the ExitStack pattern in
tests/cloud_staging/test_thread_cloud_diff_endpoints.py: ``import main``
(conftest puts orchestrator/ on sys.path), patch its module globals, build
the route dependencies from main's own factory *inside* those patches, and
call the endpoint coroutines directly.

R1.B04 moved both routes to ``routers.thread_cloud_diff`` and their bodies
(plus ``_reset_thread_overlay`` and the two overlay-authority helpers) to
``services.thread_cloud_diff``. Collaborators now arrive through
``ThreadCloudDiffRouteDependencies``, so the patches below target either a
global main's factory reads live or the service module that owns the name —
patching a moved name on ``main`` would not intercept.
"""

from contextlib import ExitStack, contextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

import orchestrator.main
from orchestrator.routers import thread_cloud_diff as diff_routes
from orchestrator.services import thread_cloud_diff as diff_ops

# NOTE: import via the bare ``services.`` root, NOT ``orchestrator.services.``.
# orchestrator/ is on sys.path (conftest.py) so ``services.cloud_staging.apply``
# and ``orchestrator.services.cloud_staging.apply`` are two DIFFERENT module
# objects with two different ``StagedApplyError`` classes. main.py's inline
# imports use the bare form (``from services.cloud_staging.apply import
# ...``), so tests raising ``StagedApplyError`` for its ``except`` clause to
# catch must use the identical import path.
from orchestrator.services.cloud_staging.apply import StagedApplyError
from orchestrator.application import workspace as workspace_composition
from orchestrator.services import snapshot_service as snapshot_service_module

THREAD_ID = "11111111-1111-4111-8111-111111111111"
AGENT_ID = "22222222-2222-4222-8222-222222222222"
RUNTIME_GENERATION = "33333333-3333-4333-8333-333333333333"
ATTACH_TOKEN = "44444444-4444-4444-8444-444444444444"
WORKSPACE_GENERATION = "55555555-5555-4555-8555-555555555555"
WORKSPACE_RUNTIME = "66666666-6666-4666-8666-666666666666"


# --------------------------------------------------------------------------- #
# Fixtures / helpers
# --------------------------------------------------------------------------- #


def _make_user() -> dict:
    return {"id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", "email": "user@example.test"}


def _make_thread(*, protected: bool = True, agent_id: str | None = AGENT_ID) -> dict:
    metadata = {
        "protected_cloud": protected,
        "workspace_container": {
            "status": "ready",
            "_canvas_workspace_generation": WORKSPACE_GENERATION,
            "_runtime_incarnation": WORKSPACE_RUNTIME,
        },
        "_workspace_binding": {
            "kind": "remote",
            "generation": WORKSPACE_GENERATION,
        },
    }
    return {
        "id": THREAD_ID,
        "user_id": _make_user()["id"],
        "metadata": metadata,
        "agent_id": agent_id,
        "execution_lane": "pinned",
        "runtime_generation": RUNTIME_GENERATION,
        "runtime_attach_token": ATTACH_TOKEN,
        "runtime_retirement_token": None,
    }


def _staged_summary(*, runtime_generation: str = RUNTIME_GENERATION) -> dict:
    return {
        "producer": {
            "agent_id": AGENT_ID,
            "runtime_generation": runtime_generation,
            "runtime_attach_token": ATTACH_TOKEN,
            "workspace_generation": WORKSPACE_GENERATION,
            "workspace_runtime_incarnation": WORKSPACE_RUNTIME,
        }
    }


@contextmanager
def _patch_endpoint(
    *,
    user: dict,
    thread: dict,
    require_thread_owner_result=None,
    reset_overlay_result: bool = True,
):
    """Patch every global the two endpoints touch; yield db + dependencies.

    The owner gate is a *declared* dependency now (the dataclass defaults it to
    the real ``security.access.require_thread_owner``, which main's factory
    leaves alone), so it is injected with ``dataclasses.replace`` rather than
    patched onto a module. Everything else is still a main global, resolved
    live by ``main._thread_cloud_diff_dependencies()`` — which is why that
    factory has to run inside this stack.
    """
    stack = ExitStack()
    gate = (
        require_thread_owner_result
        if require_thread_owner_result is not None
        else AsyncMock(return_value=(user, thread))
    )
    db = MagicMock()
    db.get_ro_mount_by_thread = AsyncMock(
        return_value={"staged_epoch": 5, "staged_summary": _staged_summary()}
    )
    stack.enter_context(patch("orchestrator.main.app.state.resources.postgres_db", db))
    stack.enter_context(
        patch("orchestrator.services.snapshot_service.snapshot_service", MagicMock())
    )
    stack.enter_context(
        patch("orchestrator.main.app.state.resources.main_cloud_router", MagicMock())
    )
    stack.enter_context(
        patch(
            "orchestrator.services.deployment_gates.is_protected_cloud_mode_enabled",
            lambda: True,
        )
    )
    reset_overlay = AsyncMock(return_value=reset_overlay_result)
    # ``_reset_thread_overlay`` moved with the endpoint bodies; apply/reject
    # resolve it in the service module's own namespace, so that is where it
    # has to be patched.
    stack.enter_context(patch.object(diff_ops, "_reset_thread_overlay", reset_overlay))
    with stack:
        dependencies = replace(
            workspace_composition.thread_cloud_diff_dependencies(
                orchestrator.main.app.state.resources
            ),
            require_thread_owner=gate,
        )
        # Proof the patches intercept: the factory reads these globals live,
        # and the protected-mode flag is off by default (env-driven), so a
        # missed patch would 404 every protected case below.
        assert dependencies.operations.store is db
        assert dependencies.operations.is_protected_cloud_mode_enabled() is True
        assert dependencies.require_thread_owner is gate
        yield SimpleNamespace(
            db=db,
            dependencies=dependencies,
            gate=gate,
            reset_overlay=reset_overlay,
        )


# --------------------------------------------------------------------------- #
# POST /api/agents/threads/{thread_id}/cloud-diff/apply
# --------------------------------------------------------------------------- #


class TestApplyEndpoint:
    @pytest.mark.asyncio
    async def test_apply_passes_epoch_and_call_args(self, fake_request):
        user = _make_user()
        thread = _make_thread()
        engine_mock = AsyncMock(
            return_value={
                "applied": 1,
                "deleted": 0,
                "errors": [],
                "epoch": 6,
                "overlay_reset": True,
            }
        )
        with (
            _patch_endpoint(user=user, thread=thread) as wired,
            patch(
                "orchestrator.services.cloud_staging.apply.apply_staged_diff",
                engine_mock,
            ),
        ):
            result = await diff_routes.apply_thread_cloud_diff(
                fake_request,
                THREAD_ID,
                {"epoch": 5},
                dependencies=wired.dependencies,
            )

            assert result == {
                "thread_id": THREAD_ID,
                "applied": 1,
                "deleted": 0,
                "errors": [],
                "epoch": 6,
                "overlay_reset": True,
            }
            engine_mock.assert_awaited_once()
            _, kwargs = engine_mock.call_args
            assert kwargs["thread_id"] == THREAD_ID
            assert kwargs["epoch"] == 5
            assert (
                kwargs["postgres_db"]
                is orchestrator.main.app.state.resources.postgres_db
            )
            assert (
                kwargs["main_cloud_router"]
                is orchestrator.main.app.state.resources.main_cloud_router
            )
            assert (
                kwargs["snapshot_service"] is snapshot_service_module.snapshot_service
            )
            assert callable(kwargs["reset_agent_overlay"])

            # Verify the closure is actually wired to _reset_thread_overlay,
            # not just "a callable".
            out = await kwargs["reset_agent_overlay"]()
            assert out is True
            wired.reset_overlay.assert_awaited_once_with(
                THREAD_ID,
                diff_ops._capture_thread_overlay_reset_authority(
                    thread, _staged_summary()
                ),
                dependencies=wired.dependencies.operations,
            )

    @pytest.mark.asyncio
    async def test_apply_missing_epoch_defaults_to_sentinel(self, fake_request):
        """No ``epoch`` in the body -> -1, which will never match a real
        ``staged_epoch`` and always epoch-stales at the engine layer."""
        user = _make_user()
        thread = _make_thread()
        engine_mock = AsyncMock(
            side_effect=StagedApplyError(
                409, {"code": "epoch_stale", "staged_epoch": 3}
            )
        )
        with (
            _patch_endpoint(user=user, thread=thread) as wired,
            patch(
                "orchestrator.services.cloud_staging.apply.apply_staged_diff",
                engine_mock,
            ),
        ):
            with pytest.raises(HTTPException):
                await diff_routes.apply_thread_cloud_diff(
                    fake_request,
                    THREAD_ID,
                    {},
                    dependencies=wired.dependencies,
                )
            _, kwargs = engine_mock.call_args
            assert kwargs["epoch"] == -1

    @pytest.mark.asyncio
    async def test_apply_malformed_epoch_422(self, fake_request):
        """Non-coercible epoch in the body -> 422 invalid_epoch, engine never
        called (was an unhandled ValueError -> 500 before the polish pass)."""
        user = _make_user()
        thread = _make_thread()
        engine_mock = AsyncMock()
        with (
            _patch_endpoint(user=user, thread=thread) as wired,
            patch(
                "orchestrator.services.cloud_staging.apply.apply_staged_diff",
                engine_mock,
            ),
        ):
            with pytest.raises(HTTPException) as ei:
                await diff_routes.apply_thread_cloud_diff(
                    fake_request,
                    THREAD_ID,
                    {"epoch": "not-a-number"},
                    dependencies=wired.dependencies,
                )
            engine_mock.assert_not_awaited()
        assert ei.value.status_code == 422
        assert ei.value.detail == {"code": "invalid_epoch"}

        # reject mirrors the same guard (None -> TypeError branch).
        reject_mock = AsyncMock()
        with (
            _patch_endpoint(user=user, thread=thread) as wired2,
            patch(
                "orchestrator.services.cloud_staging.apply.reject_staged_diff",
                reject_mock,
            ),
        ):
            with pytest.raises(HTTPException) as ei2:
                await diff_routes.reject_thread_cloud_diff(
                    fake_request,
                    THREAD_ID,
                    {"epoch": None},
                    dependencies=wired2.dependencies,
                )
            reject_mock.assert_not_awaited()
        assert ei2.value.status_code == 422
        assert ei2.value.detail == {"code": "invalid_epoch"}

    @pytest.mark.asyncio
    async def test_apply_staged_error_maps_to_http(self, fake_request):
        user = _make_user()
        thread = _make_thread()
        engine_mock = AsyncMock(
            side_effect=StagedApplyError(
                409, {"code": "external_modifications_detected", "diverged": []}
            )
        )
        with (
            _patch_endpoint(user=user, thread=thread) as wired,
            patch(
                "orchestrator.services.cloud_staging.apply.apply_staged_diff",
                engine_mock,
            ),
        ):
            with pytest.raises(HTTPException) as ei:
                await diff_routes.apply_thread_cloud_diff(
                    fake_request,
                    THREAD_ID,
                    {"epoch": 5},
                    dependencies=wired.dependencies,
                )
        assert ei.value.status_code == 409
        assert ei.value.detail == {
            "code": "external_modifications_detected",
            "diverged": [],
        }

    @pytest.mark.asyncio
    async def test_apply_staging_missing_410_passthrough(self, fake_request):
        user = _make_user()
        thread = _make_thread()
        engine_mock = AsyncMock(
            side_effect=StagedApplyError(410, {"code": "staging_missing"})
        )
        with (
            _patch_endpoint(user=user, thread=thread) as wired,
            patch(
                "orchestrator.services.cloud_staging.apply.apply_staged_diff",
                engine_mock,
            ),
        ):
            with pytest.raises(HTTPException) as ei:
                await diff_routes.apply_thread_cloud_diff(
                    fake_request,
                    THREAD_ID,
                    {"epoch": 5},
                    dependencies=wired.dependencies,
                )
        assert ei.value.status_code == 410
        assert ei.value.detail == {"code": "staging_missing"}

    @pytest.mark.asyncio
    async def test_apply_partial_failure_maps_to_502(self, fake_request):
        user = _make_user()
        thread = _make_thread()
        engine_mock = AsyncMock(
            return_value={
                "applied": 1,
                "deleted": 0,
                "errors": ["bad.txt: boom"],
            }
        )
        with (
            _patch_endpoint(user=user, thread=thread) as wired,
            patch(
                "orchestrator.services.cloud_staging.apply.apply_staged_diff",
                engine_mock,
            ),
        ):
            with pytest.raises(HTTPException) as ei:
                await diff_routes.apply_thread_cloud_diff(
                    fake_request,
                    THREAD_ID,
                    {"epoch": 5},
                    dependencies=wired.dependencies,
                )
        assert ei.value.status_code == 502
        assert ei.value.detail == {
            "code": "partial_write_failure",
            "applied": 1,
            "deleted": 0,
            "errors": ["bad.txt: boom"],
        }

    @pytest.mark.asyncio
    async def test_apply_404_when_not_protected(self, fake_request):
        user = _make_user()
        thread = _make_thread(protected=False)
        with (
            _patch_endpoint(user=user, thread=thread) as wired,
            pytest.raises(HTTPException) as ei,
        ):
            await diff_routes.apply_thread_cloud_diff(
                fake_request,
                THREAD_ID,
                {"epoch": 5},
                dependencies=wired.dependencies,
            )
        assert ei.value.status_code == 404

    @pytest.mark.asyncio
    async def test_apply_owner_gate_denied(self, fake_request):
        denied = AsyncMock(
            side_effect=HTTPException(status_code=403, detail="Not your thread")
        )
        with (
            _patch_endpoint(
                user=_make_user(),
                thread=_make_thread(),
                require_thread_owner_result=denied,
            ) as wired,
            pytest.raises(HTTPException) as ei,
        ):
            await diff_routes.apply_thread_cloud_diff(
                fake_request,
                THREAD_ID,
                {"epoch": 5},
                dependencies=wired.dependencies,
            )
        assert ei.value.status_code == 403


# --------------------------------------------------------------------------- #
# POST /api/agents/threads/{thread_id}/cloud-diff/reject
# --------------------------------------------------------------------------- #


class TestRejectEndpoint:
    @pytest.mark.asyncio
    async def test_reject_passes_epoch_and_call_args(self, fake_request):
        user = _make_user()
        thread = _make_thread()
        engine_mock = AsyncMock(
            return_value={"rejected": True, "epoch": 6, "overlay_reset": True}
        )
        with (
            _patch_endpoint(user=user, thread=thread) as wired,
            patch(
                "orchestrator.services.cloud_staging.apply.reject_staged_diff",
                engine_mock,
            ),
        ):
            result = await diff_routes.reject_thread_cloud_diff(
                fake_request,
                THREAD_ID,
                {"epoch": 5},
                dependencies=wired.dependencies,
            )

            assert result == {
                "thread_id": THREAD_ID,
                "rejected": True,
                "epoch": 6,
                "overlay_reset": True,
            }
            engine_mock.assert_awaited_once()
            _, kwargs = engine_mock.call_args
            assert kwargs["thread_id"] == THREAD_ID
            assert kwargs["epoch"] == 5
            assert (
                kwargs["postgres_db"]
                is orchestrator.main.app.state.resources.postgres_db
            )
            assert (
                kwargs["snapshot_service"] is snapshot_service_module.snapshot_service
            )
            assert "main_cloud_router" not in kwargs
            assert callable(kwargs["reset_agent_overlay"])

    @pytest.mark.asyncio
    async def test_reject_staged_error_maps_to_http(self, fake_request):
        user = _make_user()
        thread = _make_thread()
        engine_mock = AsyncMock(
            side_effect=StagedApplyError(
                409, {"code": "epoch_stale", "staged_epoch": 9}
            )
        )
        with (
            _patch_endpoint(user=user, thread=thread) as wired,
            patch(
                "orchestrator.services.cloud_staging.apply.reject_staged_diff",
                engine_mock,
            ),
        ):
            with pytest.raises(HTTPException) as ei:
                await diff_routes.reject_thread_cloud_diff(
                    fake_request,
                    THREAD_ID,
                    {"epoch": 5},
                    dependencies=wired.dependencies,
                )
        assert ei.value.status_code == 409
        assert ei.value.detail == {"code": "epoch_stale", "staged_epoch": 9}

    @pytest.mark.asyncio
    async def test_reject_404_when_not_protected(self, fake_request):
        user = _make_user()
        thread = _make_thread(protected=False)
        with (
            _patch_endpoint(user=user, thread=thread) as wired,
            pytest.raises(HTTPException) as ei,
        ):
            await diff_routes.reject_thread_cloud_diff(
                fake_request,
                THREAD_ID,
                {"epoch": 5},
                dependencies=wired.dependencies,
            )
        assert ei.value.status_code == 404

    @pytest.mark.asyncio
    async def test_reject_owner_gate_denied(self, fake_request):
        denied = AsyncMock(
            side_effect=HTTPException(status_code=403, detail="Not your thread")
        )
        with (
            _patch_endpoint(
                user=_make_user(),
                thread=_make_thread(),
                require_thread_owner_result=denied,
            ) as wired,
            pytest.raises(HTTPException) as ei,
        ):
            await diff_routes.reject_thread_cloud_diff(
                fake_request,
                THREAD_ID,
                {"epoch": 5},
                dependencies=wired.dependencies,
            )
        assert ei.value.status_code == 403


# --------------------------------------------------------------------------- #
# main._reset_thread_overlay — agent-URL resolution + fail-soft behavior
# --------------------------------------------------------------------------- #


class _FakeResponse:
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code


class _FakeAsyncClient:
    def __init__(self, *, response=None, exc=None, **_kwargs) -> None:
        self._response = response
        self._exc = exc

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_a):
        return False

    async def post(self, url, *, headers=None, json=None):
        if self._exc is not None:
            raise self._exc
        self.url = url
        self.headers = headers
        self.json = json
        return self._response


class TestResetThreadOverlay:
    @staticmethod
    def _deps() -> diff_ops.ThreadCloudDiffDependencies:
        """Operations dependencies from main's factory.

        Call this *inside* the ``postgres_db`` patch: the factory reads that
        global live, and ``_reset_thread_overlay`` reaches the store only
        through ``dependencies.store``.
        """
        return workspace_composition.thread_cloud_diff_dependencies(
            orchestrator.main.app.state.resources
        ).operations

    @staticmethod
    def _authority() -> dict[str, str]:
        authority = diff_ops._capture_thread_overlay_reset_authority(
            _make_thread(), _staged_summary()
        )
        assert authority is not None
        return authority

    @staticmethod
    def _db(*, thread=None, agent=None, reciprocal=True):
        db = MagicMock()
        db.get_thread = AsyncMock(
            side_effect=[thread or _make_thread(), thread or _make_thread()]
        )
        db.get_agent = AsyncMock(
            return_value=agent
            or {
                "id": AGENT_ID,
                "thread_id": THREAD_ID,
                "pod_ip": "10.0.0.9",
                "pod_port": 8001,
            }
        )
        db.pinned_thread_agent_is_reciprocal = AsyncMock(return_value=reciprocal)
        return db

    @pytest.mark.asyncio
    async def test_reset_overlay_true_on_200(self):
        db = self._db()
        client = _FakeAsyncClient(response=_FakeResponse(200))
        with (
            patch("orchestrator.main.app.state.resources.postgres_db", db),
            patch(
                "orchestrator.services.thread_cloud_diff.httpx.AsyncClient",
                return_value=client,
            ),
        ):
            deps = self._deps()
            assert deps.store is db  # the patch intercepts
            out = await diff_ops._reset_thread_overlay(
                THREAD_ID, self._authority(), dependencies=deps
            )
        assert out is True
        assert client.headers == {
            "X-Agent-ID": AGENT_ID,
            "X-Session-Runtime-Generation": RUNTIME_GENERATION,
            "X-Session-Runtime-Attach-Token": ATTACH_TOKEN,
        }
        assert client.json == {
            "thread_id": THREAD_ID,
            "workspace_generation": WORKSPACE_GENERATION,
            "workspace_runtime_incarnation": WORKSPACE_RUNTIME,
        }

    @pytest.mark.asyncio
    async def test_reset_overlay_false_on_404(self):
        """404 (pod alive, overlay unavailable — Task 9's contract) is not
        fatal; it's still just a normal False."""
        db = self._db()
        with (
            patch("orchestrator.main.app.state.resources.postgres_db", db),
            patch(
                "orchestrator.services.thread_cloud_diff.httpx.AsyncClient",
                return_value=_FakeAsyncClient(response=_FakeResponse(404)),
            ),
        ):
            deps = self._deps()
            assert deps.store is db  # the patch intercepts
            out = await diff_ops._reset_thread_overlay(
                THREAD_ID, self._authority(), dependencies=deps
            )
        assert out is False

    @pytest.mark.asyncio
    async def test_reset_overlay_false_on_exception(self):
        """Dead/unreachable pod -> exception -> False, never raises."""
        db = self._db()
        with (
            patch("orchestrator.main.app.state.resources.postgres_db", db),
            patch(
                "orchestrator.services.thread_cloud_diff.httpx.AsyncClient",
                return_value=_FakeAsyncClient(exc=ConnectionError("dead pod")),
            ),
        ):
            deps = self._deps()
            assert deps.store is db  # the patch intercepts
            out = await diff_ops._reset_thread_overlay(
                THREAD_ID, self._authority(), dependencies=deps
            )
        assert out is False

    @pytest.mark.asyncio
    async def test_reset_overlay_false_when_no_agent_bound(self):
        out = await diff_ops._reset_thread_overlay(
            THREAD_ID, None, dependencies=self._deps()
        )
        assert out is False

    @pytest.mark.asyncio
    async def test_reset_overlay_false_when_agent_has_no_pod_ip(self):
        db = self._db(agent={"id": AGENT_ID, "thread_id": THREAD_ID, "pod_ip": None})
        with patch("orchestrator.main.app.state.resources.postgres_db", db):
            deps = self._deps()
            assert deps.store is db  # the patch intercepts
            out = await diff_ops._reset_thread_overlay(
                THREAD_ID, self._authority(), dependencies=deps
            )
        assert out is False

    @pytest.mark.asyncio
    async def test_reset_overlay_refuses_successor_before_http(self):
        successor = _make_thread()
        successor["runtime_generation"] = "77777777-7777-4777-8777-777777777777"
        db = self._db(thread=successor)
        client = _FakeAsyncClient(response=_FakeResponse(200))
        with (
            patch("orchestrator.main.app.state.resources.postgres_db", db),
            patch(
                "orchestrator.services.thread_cloud_diff.httpx.AsyncClient",
                return_value=client,
            ) as http_client,
        ):
            deps = self._deps()
            assert deps.store is db  # the patch intercepts
            out = await diff_ops._reset_thread_overlay(
                THREAD_ID, self._authority(), dependencies=deps
            )
        assert out is False
        http_client.assert_not_called()

    def test_capture_refuses_review_from_predecessor_generation(self):
        assert (
            diff_ops._capture_thread_overlay_reset_authority(
                _make_thread(),
                _staged_summary(
                    runtime_generation="77777777-7777-4777-8777-777777777777"
                ),
            )
            is None
        )

    @pytest.mark.asyncio
    async def test_reset_overlay_drops_workspace_replacement_at_final_read(self):
        successor = _make_thread()
        successor["metadata"] = dict(successor["metadata"])
        successor["metadata"]["workspace_container"] = dict(
            successor["metadata"]["workspace_container"]
        )
        successor["metadata"]["workspace_container"]["_runtime_incarnation"] = (
            "77777777-7777-4777-8777-777777777777"
        )
        db = self._db()
        db.get_thread = AsyncMock(side_effect=[_make_thread(), successor])
        client = _FakeAsyncClient(response=_FakeResponse(200))
        with (
            patch("orchestrator.main.app.state.resources.postgres_db", db),
            patch(
                "orchestrator.services.thread_cloud_diff.httpx.AsyncClient",
                return_value=client,
            ) as http_client,
        ):
            deps = self._deps()
            assert deps.store is db  # the patch intercepts
            out = await diff_ops._reset_thread_overlay(
                THREAD_ID, self._authority(), dependencies=deps
            )
        assert out is False
        http_client.assert_not_called()
        db.pinned_thread_agent_is_reciprocal.assert_not_awaited()
