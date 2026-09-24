"""Tables use app-owned dependencies; main retains its compatibility bindings."""

import asyncio
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from fastapi import FastAPI, HTTPException, Request

from orchestrator.routers.tables import (
    TablesDependencies,
    get_tables_dependencies,
    router,
)
from orchestrator.security import auth
from orchestrator.application import access as access_composition
from orchestrator.security import access as access_module
from orchestrator.security import auth as auth_module


def make_app(db):
    app = FastAPI()
    app.state.tables_dependencies = TablesDependencies(db=db)
    app.include_router(router)
    return app


@pytest.mark.asyncio
@pytest.mark.parametrize("second_is_admin", [True, False])
async def test_two_apps_isolate_reads_identity_and_denial_audit(
    monkeypatch, user_admin, user_a, second_is_admin
):
    databases = [
        SimpleNamespace(
            get_tables=AsyncMock(
                return_value=[{"name": f"app-{index}", "rowCount": 1}]
            ),
            record_security_event=AsyncMock(),
        )
        for index in range(2)
    ]
    apps = [make_app(db) for db in databases]
    callers = [user_admin, {**user_a, "is_admin": second_is_admin}]
    entered = [asyncio.Event(), asyncio.Event()]

    async def resolve_user(request, db):
        index = next(index for index, app in enumerate(apps) if request.app is app)
        assert db is databases[index]
        entered[index].set()
        await entered[1 - index].wait()
        return callers[index]

    monkeypatch.setattr(auth, "get_current_user", resolve_user)
    async with (
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=apps[0]), base_url="http://first.test"
        ) as first,
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=apps[1]), base_url="http://second.test"
        ) as second,
    ):
        responses = await asyncio.wait_for(
            asyncio.gather(first.get("/api/tables"), second.get("/api/tables")),
            timeout=2,
        )
    assert responses[0].status_code == 200
    assert responses[0].json() == [{"name": "app-0", "rowCount": 1}]
    databases[0].get_tables.assert_awaited_once_with()
    databases[0].record_security_event.assert_not_awaited()
    if second_is_admin:
        assert responses[1].status_code == 200
        assert responses[1].json() == [{"name": "app-1", "rowCount": 1}]
        databases[1].get_tables.assert_awaited_once_with()
        databases[1].record_security_event.assert_not_awaited()
    else:
        assert responses[1].status_code == 403
        assert responses[1].json() == {"detail": "Admin access required"}
        databases[1].get_tables.assert_not_awaited()
        databases[1].record_security_event.assert_awaited_once()
        event = databases[1].record_security_event.await_args.kwargs
        assert event["event_type"] == "admin_denied"
        assert event["user_id"] == str(user_a["id"])
        assert event["path"] == "/api/tables"


@pytest.mark.asyncio
async def test_dependency_override_is_local_to_one_app(monkeypatch, user_admin):
    original = SimpleNamespace(get_tables=AsyncMock(return_value=[]))
    replacement = SimpleNamespace(
        get_tables=AsyncMock(return_value=[{"name": "override", "rowCount": 2}])
    )
    first_app, second_app = make_app(original), make_app(original)
    first_app.dependency_overrides[get_tables_dependencies] = (
        lambda: TablesDependencies(db=replacement)
    )
    identity = AsyncMock(return_value=user_admin)
    monkeypatch.setattr(auth, "get_current_user", identity)

    async with (
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=first_app), base_url="http://first.test"
        ) as first,
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=second_app), base_url="http://second.test"
        ) as second,
    ):
        overridden = await first.get("/api/tables")
        unchanged = await second.get("/api/tables")

    assert overridden.status_code == unchanged.status_code == 200
    assert overridden.json() == [{"name": "override", "rowCount": 2}]
    assert unchanged.json() == []
    replacement.get_tables.assert_awaited_once_with()
    original.get_tables.assert_awaited_once_with()
    assert [call.args[1] for call in identity.await_args_list] == [
        replacement,
        original,
    ]
    assert first_app.state.tables_dependencies.db is original
    assert second_app.state.tables_dependencies.db is original


@pytest.mark.asyncio
async def test_main_admin_adapter_preserves_call_time_auth_and_audit_bindings(
    monkeypatch, user_a
):
    from orchestrator import main

    db = object()
    resolver = AsyncMock(return_value=user_a)
    audit = AsyncMock()
    request = Request(
        {"type": "http", "method": "GET", "path": "/api/tables", "headers": []}
    )
    monkeypatch.setattr(main.app.state.resources, "postgres_db", db)
    monkeypatch.setattr(auth_module, "require_approved_user", resolver)
    monkeypatch.setattr(access_module, "log_security_event", audit)
    with pytest.raises(HTTPException) as caught:
        await access_composition.require_admin(main.app.state.resources, request)
    assert caught.value.status_code == 403
    assert caught.value.detail == "Admin access required"
    resolver.assert_awaited_once_with(request, db)
    audit.assert_awaited_once_with(
        db,
        event_type="admin_denied",
        user=user_a,
        resource_type="admin_endpoint",
        resource_id="/api/tables",
        detail="Admin access required",
        request=request,
    )


def test_importing_tables_does_not_import_application_startup():
    result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            "import sys; import orchestrator.routers.tables; "
            "assert 'orchestrator.main' not in sys.modules",
        ],
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert result.returncode == 0, result.stderr
