from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# R1.B06: this operation moved to services/run_queue_admin.
from orchestrator.services import run_queue_admin  # noqa: E402
from fastapi import HTTPException
from orchestrator.application import access as access_composition
from orchestrator.application import sessions as sessions_composition


def _valid_sandbox_thread(**metadata_overrides):
    generation = "11111111-1111-4111-8111-111111111111"
    runtime = "22222222-2222-4222-8222-222222222222"
    metadata = {
        "config_override": {"workspace": {"backend": "sandbox"}},
        "workspace_container": {
            "status": "ready",
            "provisioner": "k8s",
            "pod_ip": "10.42.0.25",
            "port": 30022,
            "pod_name": "ws-thread-thread-a",
            "namespace": "agent-workspaces",
            "_canvas_workspace_generation": generation,
            "_runtime_incarnation": runtime,
        },
        "_workspace_binding": {
            "generation": generation,
            "kind": "remote",
            "backing_id": "k8s-pod:agent-workspaces:pod-uid",
            "ssh_host_key_fingerprint": "SHA256:trusted",
        },
        **metadata_overrides,
    }
    return {
        "id": "thread-a",
        "execution_lane": "stateless",
        "status": "active",
        "metadata": metadata,
    }


def _ide_dependencies(db, proxy, **gates):
    """Bind the IDE router's collaborators the way the application does.

    ``store`` and ``ide_proxy`` used to be ``main`` globals these tests
    patched; they are dataclass fields now, and the auth gates are fields too
    rather than module attributes, so an override has to be passed in.
    """
    from orchestrator.routers.ide import IdeDependencies

    return IdeDependencies(
        store=db,
        ide_sessions=MagicMock(),
        ide_proxy=proxy,
        **gates,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["suspended", "ended"])
async def test_stale_ready_stateless_ide_refuses_nonactive_lifecycle(status):
    from orchestrator.services.ide_proxy_gateway import (
        _require_stateless_ide_lifecycle,
    )

    thread = {
        "id": "thread-a",
        "execution_lane": "stateless",
        "status": status,
        "metadata": {"workspace_container": {"status": "ready"}},
    }
    db = MagicMock()
    db.get_thread = AsyncMock(return_value=thread)
    proxy = MagicMock()
    with pytest.raises(HTTPException) as exc:
        await _require_stateless_ide_lifecycle("thread-a", store=db, ide_proxy=proxy)

    assert exc.value.status_code == 409
    proxy.evict.assert_called_once_with("thread-a")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "marker_key",
    [
        "_stateless_workspace_retirement_pending",
        "_stateless_claim_retirement",
        "_stateless_claim_loss_hold",
        "_stateless_claim_losses",
    ],
)
@pytest.mark.parametrize("value", [None, False, 0, "", [], {}])
async def test_present_falsey_stop_marker_refuses_ide(marker_key, value):
    from orchestrator.services.ide_proxy_gateway import (
        _require_stateless_ide_lifecycle,
    )

    thread = {
        "id": "thread-a",
        "execution_lane": "stateless",
        "status": "active",
        "metadata": {marker_key: value},
    }
    db = MagicMock()
    db.get_thread = AsyncMock(return_value=thread)
    with pytest.raises(HTTPException) as exc:
        await _require_stateless_ide_lifecycle(
            "thread-a", store=db, ide_proxy=MagicMock()
        )

    assert exc.value.status_code == 409


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "config_override",
    [
        {"workspace": {"backend": "sandbox"}, "officer": []},
        {
            "workspace": {"backend": "sandbox"},
            "officer": {"enabled": "yes"},
        },
        {
            "workspace": {"backend": "sandbox"},
            "officer": {"conference": True},
        },
        {"workspace": {"backend": "docker"}},
        {"workspace": {"backend": "virtual"}},
    ],
)
async def test_stateless_ide_refuses_unsupported_session_class_or_workspace(
    config_override,
):
    from orchestrator.services.ide_proxy_gateway import (
        _require_stateless_ide_lifecycle,
    )

    thread = _valid_sandbox_thread(config_override=config_override)
    db = MagicMock()
    db.get_thread = AsyncMock(return_value=thread)
    proxy = MagicMock()
    with pytest.raises(HTTPException) as exc:
        await _require_stateless_ide_lifecycle("thread-a", store=db, ide_proxy=proxy)

    assert exc.value.status_code == 409
    proxy.evict.assert_called_once_with("thread-a")


@pytest.mark.asyncio
async def test_valid_stateless_sandbox_ide_is_admitted_and_pinned_is_unchanged():
    from orchestrator.services.ide_proxy_gateway import (
        _require_stateless_ide_lifecycle,
    )

    db = MagicMock()
    db.get_thread = AsyncMock(
        side_effect=[
            _valid_sandbox_thread(),
            {
                "id": "thread-pinned",
                "execution_lane": "pinned",
                "status": "suspended",
                "metadata": {"config_override": "malformed"},
            },
        ]
    )
    proxy = MagicMock()
    await _require_stateless_ide_lifecycle("thread-a", store=db, ide_proxy=proxy)
    await _require_stateless_ide_lifecycle("thread-pinned", store=db, ide_proxy=proxy)

    proxy.evict.assert_called_once_with("thread-a")


@pytest.mark.asyncio
async def test_http_virtual_stateless_ide_refuses_before_proxy_resolution():
    from orchestrator.routers.ide import ide_proxy_http

    thread = _valid_sandbox_thread(
        config_override={"workspace": {"backend": "virtual"}}
    )
    db = MagicMock()
    db.get_thread = AsyncMock(return_value=thread)
    proxy = MagicMock()
    proxy.resolve_pod_ip = AsyncMock()
    request = MagicMock()
    request.headers = {}
    request.method = "GET"
    with pytest.raises(HTTPException) as exc:
        await ide_proxy_http(
            request,
            "thread-a",
            "",
            dependencies=_ide_dependencies(
                db,
                proxy,
                require_approved_user=AsyncMock(return_value={"id": "u"}),
                user_can_access_ide_entity=AsyncMock(return_value=True),
            ),
        )

    assert exc.value.status_code == 409
    proxy.resolve_pod_ip.assert_not_awaited()


@pytest.mark.asyncio
async def test_ws_malformed_stateless_class_refuses_before_proxy_resolution():
    from orchestrator.routers.ide import ide_proxy_ws

    thread = _valid_sandbox_thread(
        config_override={
            "workspace": {"backend": "sandbox"},
            "officer": {"enabled": "yes"},
        }
    )
    db = MagicMock()
    db.get_thread = AsyncMock(return_value=thread)
    proxy = MagicMock()
    proxy.resolve_pod_ip = AsyncMock()
    ws = MagicMock()
    ws.close = AsyncMock()
    await ide_proxy_ws(
        ws,
        "thread-a",
        "",
        dependencies=_ide_dependencies(
            db,
            proxy,
            resolve_ws_user=AsyncMock(return_value={"id": "u", "is_approved": True}),
            user_can_access_ide_entity=AsyncMock(return_value=True),
        ),
    )

    ws.close.assert_awaited_once_with(
        code=4409,
        reason="Workspace lifecycle fenced",
    )
    proxy.resolve_pod_ip.assert_not_awaited()


@pytest.mark.asyncio
async def test_http_stateless_remote_is_contained_before_network_connect():
    from orchestrator.routers.ide import ide_proxy_http

    db = MagicMock()
    db.get_thread = AsyncMock(return_value=_valid_sandbox_thread())
    proxy = MagicMock()
    target = MagicMock()
    target.backend = "k8s"
    target.host = "10.0.0.2"
    target.authority = "10.0.0.2:38080"
    # Explicit, not left to the mock: an unset attribute auto-vivifies to a
    # truthy MagicMock, which would look like a bound credential and let this
    # very containment pass by accident.
    target.credential = None
    proxy.resolve_target = AsyncMock(return_value=target)
    request = MagicMock()
    request.headers = {}
    request.method = "GET"
    request.url.query = ""
    request.client = None
    with (
        patch(
            "orchestrator.services.ssh_helpers.orchestrator_can_reach",
            return_value=True,
        ),
        patch("httpx.AsyncClient") as client,
        pytest.raises(HTTPException) as exc,
    ):
        await ide_proxy_http(
            request,
            "thread-a",
            "workspace",
            dependencies=_ide_dependencies(
                db,
                proxy,
                require_approved_user=AsyncMock(return_value={"id": "u"}),
                user_can_access_ide_entity=AsyncMock(return_value=True),
            ),
        )

    assert exc.value.status_code == 503
    assert exc.value.detail["code"] == "ide_remote_transport_unavailable"
    proxy.resolve_target.assert_awaited_once_with("thread-a")
    client.assert_not_called()


@pytest.mark.asyncio
async def test_ws_stateless_remote_without_a_credential_is_refused():
    """A remote stream is opened only once its runtime can refuse a stranger.

    Was an unconditional refusal while code-server ran `auth: none`. The rule
    is now the same as the HTTP transport's: no credential, no stream — and
    still decided before `accept()` and before any upstream handshake, so a
    browser never gets an open socket it cannot use.
    """
    from orchestrator.routers.ide import ide_proxy_ws

    db = MagicMock()
    db.get_thread = AsyncMock(return_value=_valid_sandbox_thread())
    proxy = MagicMock()
    target = MagicMock()
    target.backend = "k8s"
    target.host = "10.0.0.2"
    target.authority = "10.0.0.2:38080"
    # Explicit: an unset attribute auto-vivifies truthy and would read as a
    # bound credential.
    target.credential = None
    proxy.resolve_target = AsyncMock(return_value=target)
    ws = MagicMock()
    ws.url.query = ""
    ws.accept = AsyncMock()
    ws.close = AsyncMock()
    ws.receive = AsyncMock(return_value={"type": "websocket.disconnect"})
    ws.send_text = AsyncMock()
    ws.send_bytes = AsyncMock()
    connected_urls = []

    class _Upstream:
        async def send(self, _message):
            return None

        def __aiter__(self):
            async def empty():
                if False:
                    yield None

            return empty()

    class _Connection:
        async def __aenter__(self):
            return _Upstream()

        async def __aexit__(self, *_args):
            return False

    def connect(url, **_kwargs):
        connected_urls.append(url)
        return _Connection()

    with (
        patch(
            "orchestrator.services.ssh_helpers.orchestrator_can_reach",
            return_value=True,
        ),
        patch("websockets.connect", side_effect=connect),
    ):
        await ide_proxy_ws(
            ws,
            "thread-a",
            "workspace",
            dependencies=_ide_dependencies(
                db,
                proxy,
                resolve_ws_user=AsyncMock(
                    return_value={"id": "u", "is_approved": True}
                ),
                user_can_access_ide_entity=AsyncMock(return_value=True),
            ),
        )

    assert connected_urls == []
    ws.accept.assert_not_awaited()
    ws.close.assert_awaited_once_with(
        code=4503,
        reason="ide_remote_transport_unavailable",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "marker_key",
    [
        "_stateless_workspace_retirement_pending",
        "_stateless_claim_retirement",
        "_stateless_claim_loss_hold",
        "_stateless_claim_losses",
    ],
)
@pytest.mark.parametrize("value", [None, False, 0, "", [], {}])
async def test_present_falsey_stop_marker_refuses_admin_unpark(marker_key, value):
    from orchestrator import main

    conn = MagicMock()
    conn.fetchrow = AsyncMock(
        return_value={
            "execution_lane": "stateless",
            "metadata": {marker_key: value},
        }
    )

    @asynccontextmanager
    async def _acquire():
        yield conn

    db = MagicMock()
    db.acquire = _acquire
    with (
        patch.object(main.app.state.resources, "postgres_db", db),
        patch.object(access_composition, "require_admin", AsyncMock()),
        pytest.raises(HTTPException) as exc,
    ):
        await run_queue_admin.unpark_run_queue_unit(
            "11111111-1111-4111-8111-111111111111",
            dependencies=sessions_composition.run_queue_admin_dependencies(
                main.app.state.resources
            ),
        )

    assert exc.value.status_code == 409
